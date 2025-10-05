import os
import json
import sys
import signal
import shutil
from datetime import datetime, timezone
import psycopg2
import re
from dotenv import load_dotenv
from langchain.text_splitter import RecursiveCharacterTextSplitter

# This finds the project root by going up four directories and adds it to the path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.append(project_root)

# --- Project-specific Imports ---
try:
    # Assuming the renames will be applied
    from app.services.google_drive_service import get_drive_service, _download_file, get_drive_id, get_all_files_in_folder
    from app.services.document_analyzer import (
        analyze_document_with_openai_langchain_structured,
        parse_docx_text, 
        parse_pdf_text,
        docx_has_images,
        pdf_has_images
    )
    from app.database.pg_vector import PGVectorManager
    from app.database.tables import DatabaseSetup
    from langchain.docstore.document import Document
    from app.services.aws import AWS
except ImportError as e:
    print(f"Error: Failed to import a required module. Ensure all components exist and names are correct.")
    print(f"Details: {e}")
    sys.exit(1)

# --- Configuration ---
load_dotenv()
SHARED_DRIVE_NAMES = [
    "Company Shared Drive",
    "Marketing",
    "Sales "
]
BASE_PATH = os.path.dirname(os.path.abspath(__file__))

# Maximum file size in Megabytes for processing.
MAX_FILE_SIZE_MB = 50
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024

# Timeout for the LLM analysis of a single document, in seconds.
LLM_ANALYSIS_TIMEOUT = 240  # 4 minutes

# Temporary directories for processing
DOWNLOAD_PATH = os.path.join(BASE_PATH, "temp_downloaded_files")
TEXT_ONLY_PATH = os.path.join(DOWNLOAD_PATH, "textual_documents")
WITH_IMAGES_PATH = os.path.join(DOWNLOAD_PATH, "files_with_images")

# Final destination for extracted images
IMAGE_OUTPUT_DIR = os.path.join(project_root, "extracted_document_images")
FILE_MAPPING_PATH = os.path.join(DOWNLOAD_PATH, "file_id_to_local_path.json")
VECTORSTORE_COLLECTION_NAME = os.getenv("VECTORSTORE_COLLECTION_NAME", "google_drive_data")

class TimeoutException(Exception):
    """Custom exception for timeouts."""
    pass

def timeout_handler(signum, frame):
    """Raises a TimeoutException when the signal is received."""
    raise TimeoutException

# Set the signal handler for the alarm signal
signal.signal(signal.SIGALRM, timeout_handler)

def get_processed_files_history():
    """Fetches a dictionary of all processed files and their last processed timestamp."""
    db_setup = DatabaseSetup()
    history = {}
    try:
        with db_setup.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT file_id, processed_at FROM file_metadata WHERE processed_at IS NOT NULL")
                records = cur.fetchall()
                
                print(f"DEBUG: Found {len(records)} processed files in database")
                
                for record in records:
                    file_id = record[0]
                    processed_time = record[1]
                    
                    # Ensure the datetime from the DB is offset-aware (we store everything in UTC)
                    if processed_time.tzinfo is None:
                        processed_time = processed_time.replace(tzinfo=timezone.utc)
                    
                    history[file_id] = processed_time
                    print(f"  - {file_id}: {processed_time}")
                    
        print(f"✅ Returning {len(history)} processed files in history.")
        return history
    except psycopg2.Error as e:
        print(f"❌ Database error while fetching processed file history: {e}")
        return None

def get_files_to_process(drive_service, drive_id):
    """
    Identifies files that are new or have been updated since their last processing time.
    """
    print("\n[Step 1: Identifying new or updated files in Google Drive...]")
    processed_history = get_processed_files_history()
    if processed_history is None:
        return None # Indicates a DB error

    all_drive_files = get_all_files_in_folder(drive_service, drive_id, is_shared_drive=True, shared_drive_id=drive_id)
    if not all_drive_files:
        print("No files found in the Google Drive folder.")
        return []

    files_to_process = []
    for file_info in all_drive_files:
        file_id = file_info['id']
        file_name = file_info['name']

        # --- File Size Check ---
        # The 'size' key may be missing for native Google Workspace files (Docs, Sheets).
        # We only apply the size check for files where size is reported by the API (e.g., uploaded PDFs, DOCX).
        print(f'file_info keys = {file_info.keys()}')
        if 'size' in file_info:
            print(f"  - File '{file_name}' has a size of {file_info['size']} bytes.")
        if 'size' in file_info and int(file_info['size']) > MAX_FILE_SIZE_BYTES:
            size_in_mb = int(file_info['size']) / (1024 * 1024)
            print(f"  - Skipping file '{file_name}' due to size: {size_in_mb:.2f} MB (Limit: {MAX_FILE_SIZE_MB} MB).")
            continue

        modified_time_str = file_info['modifiedTime']
        # Convert Google's string timestamp to a timezone-aware datetime object
        modified_time = datetime.fromisoformat(modified_time_str.replace('Z', '+00:00'))

        if file_id not in processed_history:
            print(f"  - Found new file: '{file_info['name']}'")
            files_to_process.append(file_info)
        elif modified_time > processed_history[file_id]:
            print(f"  - Found updated file: '{file_info['name']}' (Updated: {modified_time})")
            files_to_process.append(file_info)

    print(f"✅ Found {len(files_to_process)} new or updated files to process.")
    return files_to_process

def delete_vectors_for_file(file_id: str):
    """Deletes all vector embeddings and associated S3 images for a specific file_id."""
    print(f"  - Deleting old vectors and S3 images for updated file_id: {file_id}")
    db_setup = DatabaseSetup()
    
    # Initialize counters for reporting
    deleted_images = 0
    failed_images = 0
    deleted_vectors = 0
    
    try:
        with db_setup.connect() as conn:
            with conn.cursor() as cur:
                # LangChain uses a single embedding table, not per-collection tables
                embedding_table = "langchain_pg_embedding"
                collection_table = "langchain_pg_collection"
                
                # First, get the collection UUID for the collection name
                cur.execute(
                    f"SELECT uuid FROM {collection_table} WHERE name = %s",
                    (VECTORSTORE_COLLECTION_NAME,)
                )
                collection_result = cur.fetchone()
                
                if not collection_result:
                    print(f"    ⚠️ Collection '{VECTORSTORE_COLLECTION_NAME}' not found in database")
                    return
                
                collection_uuid = collection_result[0]
                
                # Get existing image paths before deletion
                select_query = f"""
                    SELECT cmetadata FROM {embedding_table}
                    WHERE collection_id = %s 
                    AND cmetadata->>'file_id' = %s
                    AND cmetadata->'image_data' IS NOT NULL
                    AND jsonb_array_length(cmetadata->'image_data') > 0;
                """
                cur.execute(select_query, (collection_uuid, file_id))
                results = cur.fetchall()
                
                # Delete S3 images if any exist
                if results:
                    from app.services.aws import AWS
                    aws_client = AWS()
                    
                    for result in results:
                        metadata = result[0]
                        image_data = metadata.get('image_data', [])
                        
                        if isinstance(image_data, list):
                            for img_info in image_data:
                                if isinstance(img_info, dict) and 'image_path' in img_info:
                                    s3_url = img_info['image_path']
                                    bucket_name, s3_key = aws_client.extract_s3_key_from_url(s3_url)
                                    
                                    if bucket_name and s3_key:
                                        try:
                                            aws_client.delete_file_from_s3(bucket_name, s3_key)
                                            deleted_images += 1
                                        except Exception as e:
                                            failed_images += 1
                                            print(f"      - ⚠️ Failed to delete S3 image {s3_key}: {e}")
                                    else:
                                        failed_images += 1
                                        print(f"      - ⚠️ Could not parse S3 URL: {s3_url}")
                
                # Delete vector embeddings using collection_id and file_id
                delete_query = f"""
                    DELETE FROM {embedding_table}
                    WHERE collection_id = %s AND cmetadata->>'file_id' = %s;
                """
                cur.execute(delete_query, (collection_uuid, file_id))
                deleted_vectors = cur.rowcount
                
        # Report results
        if deleted_images > 0 or failed_images > 0:
            print(f"    - S3 cleanup: {deleted_images} images deleted, {failed_images} failed")
        
        print(f"    ✅ Successfully deleted {deleted_vectors} vector embeddings for {file_id}")
        
    except Exception as e:
        print(f"    ❌ Could not delete vectors/images for {file_id}. Reason: {e}")


def get_expected_local_filepath(file_name: str, mime_type: str, download_dir: str) -> str:
    """Determines the final local file path, including extension for exported Google Docs."""
    # We only want to convert Google Docs to PDF. Other G-Suite files are ignored.
    export_mimetypes = {
        'application/vnd.google-apps.document': '.pdf'
    }
    if mime_type in export_mimetypes:
        extension = export_mimetypes[mime_type]
        base_name, ext = os.path.splitext(file_name)
        return os.path.join(download_dir, f"{file_name}{extension}" if ext.lower() != extension else file_name)
    else:
        return os.path.join(download_dir, file_name)

def download_and_sort_files(drive_service, files_to_process):
    """
    DEPRECATED: This function is no longer used in the sequential processing model.
    Downloads files, sorts them into folders, and creates a file ID to path mapping.
    """
    print("\n[Step 2: Downloading and sorting files...]")
    os.makedirs(TEXT_ONLY_PATH, exist_ok=True)
    os.makedirs(WITH_IMAGES_PATH, exist_ok=True)
    
    file_id_to_local_path = {}
    db_setup = DatabaseSetup()

    with db_setup.connect() as conn:
        with conn.cursor() as cur:
            for file_info in files_to_process:
                file_id, file_name, mime_type = file_info['id'], file_info['name'], file_info['mimeType']
                
                # --- Sanitize filename to be filesystem-friendly ---
                # Replace slashes and other problematic characters to prevent path errors.
                sanitized_file_name = file_name.replace("/", "_").replace("\\", "_")

                # Metadata logging
                cur.execute("""
                    INSERT INTO file_metadata (file_id, file_name, platform, last_modified)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (file_id) DO UPDATE SET
                        file_name = EXCLUDED.file_name,
                        last_modified = EXCLUDED.last_modified,
                        processed_at = NULL;
                """, (
                    file_id, sanitized_file_name, 'Google Drive', # Use sanitized name for consistency
                    datetime.fromisoformat(file_info['modifiedTime'].replace('Z', '+00:00'))
                ))

                try:
                    print(f"  - Downloading '{file_name}' (sanitized to '{sanitized_file_name}')...")
                    _download_file(drive_service, file_id, sanitized_file_name, mime_type, DOWNLOAD_PATH)
                    
                    # This is the original, potentially non-unique path
                    original_temp_path = get_expected_local_filepath(sanitized_file_name, mime_type, DOWNLOAD_PATH)

                    # Check if the file was actually downloaded before proceeding
                    if not os.path.exists(original_temp_path):
                        print(f"    - ⚠️ Skipping file '{sanitized_file_name}' as it was not found locally after download attempt (check permissions).")
                        continue

                    # --- Unique Filename Generation to handle multiple files with the same name ---
                    # We use the unique file_id to prevent local filename collisions.
                    base_name = os.path.basename(original_temp_path)
                    unique_filename = f"{file_id}--{base_name}" # Using "--" as a clear separator
                    temp_local_path = os.path.join(os.path.dirname(original_temp_path), unique_filename)
                    
                    # Rename the file to its unique name immediately after download
                    shutil.move(original_temp_path, temp_local_path)

                    # Sort based on content using the new, unique path
                    if temp_local_path.lower().endswith('.pdf') and pdf_has_images(temp_local_path) or \
                       temp_local_path.lower().endswith('.docx') and docx_has_images(temp_local_path):
                        final_path = os.path.join(WITH_IMAGES_PATH, os.path.basename(temp_local_path))
                    else:
                        final_path = os.path.join(TEXT_ONLY_PATH, os.path.basename(temp_local_path))
                    
                    shutil.move(temp_local_path, final_path)
                    file_id_to_local_path[file_id] = final_path
                except Exception as e:
                    print(f"    ❌ An unexpected error occurred while processing {sanitized_file_name}: {e}")
            conn.commit()

    with open(FILE_MAPPING_PATH, 'w') as f:
        json.dump(file_id_to_local_path, f, indent=2)
    
    print(f"✅ Download and sort complete. Mapping saved to {FILE_MAPPING_PATH}")
    return file_id_to_local_path

def update_processed_timestamp(file_id: str):
    """Updates the 'processed_at' timestamp for a specific file_id in the database."""
    db_setup = DatabaseSetup()
    try:
        with db_setup.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE file_metadata SET processed_at = %s WHERE file_id = %s;",
                    (datetime.now(timezone.utc), file_id)
                )
    except psycopg2.Error as e:
        print(f"    ❌ DB error updating timestamp for {file_id}: {e}")

def detect_deleted_files(drive_service) -> list:
    """
    Detects files that exist in our database but no longer exist in Google Drive.
    Returns a list of file_ids that should be deleted from our system.
    """
    print("\n[Detecting deleted files from Google Drive...]")
    
    # Get all file IDs currently in Google Drive
    current_drive_files = set()
    for drive_name in SHARED_DRIVE_NAMES:
        print(f"  - Scanning drive: '{drive_name}'")
        drive_id = get_drive_id(drive_service, drive_name)
        if drive_id:
            all_files = get_all_files_in_folder(drive_service, drive_id, is_shared_drive=True, shared_drive_id=drive_id)
            for file_info in all_files:
                current_drive_files.add(file_info['id'])
        else:
            print(f"    ⚠️ Could not find drive ID for '{drive_name}'")
    
    print(f"  - Found {len(current_drive_files)} files currently in Google Drive")
    
    # Get all file IDs in our database
    db_setup = DatabaseSetup()
    db_file_ids = set()
    try:
        with db_setup.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT file_id FROM file_metadata")
                for record in cur.fetchall():
                    db_file_ids.add(record[0])
    except Exception as e:
        print(f"    ❌ Error fetching database file IDs: {e}")
        return []
    
    print(f"  - Found {len(db_file_ids)} files in database")
    
    # Find files in DB but not in Drive (deleted files)
    deleted_files = db_file_ids - current_drive_files
    
    print(f"  - Found {len(deleted_files)} files deleted from Google Drive")
    if deleted_files:
        print("  - Deleted file IDs:")
        for file_id in sorted(deleted_files):
            print(f"    • {file_id}")
    
    return list(deleted_files)

def delete_file_completely(file_id: str):
    """
    Completely removes a file from our system: vectors, S3 images, and metadata.
    """
    print(f"  - Completely deleting file_id: {file_id}")
    
    # Get file name for logging before deletion
    db_setup = DatabaseSetup()
    file_name = "Unknown"
    try:
        with db_setup.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT file_name FROM file_metadata WHERE file_id = %s", (file_id,))
                result = cur.fetchone()
                if result:
                    file_name = result[0]
    except Exception as e:
        print(f"    ⚠️ Could not fetch file name: {e}")
    
    print(f"    - File name: '{file_name}'")
    
    # First delete vectors and S3 images (reuse existing logic)
    delete_vectors_for_file(file_id)
    
    # Then delete file metadata
    try:
        with db_setup.connect() as conn:
            with conn.cursor() as cur:
                # Delete metadata
                cur.execute("DELETE FROM file_metadata WHERE file_id = %s", (file_id,))
                deleted_count = cur.rowcount
                conn.commit()
                
                if deleted_count > 0:
                    print(f"    ✅ Deleted metadata for '{file_name}'")
                else:
                    print(f"    ⚠️ No metadata found for file_id: {file_id}")
                    
    except Exception as e:
        print(f"    ❌ Error deleting metadata for {file_id}: {e}")

def process_and_embed_file(drive_service, file_info, vector_manager, db_setup):
    """
    Processes a single file: downloads, analyzes, embeds, and cleans up.
    """
    file_id, original_file_name, mime_type = file_info['id'], file_info['name'], file_info['mimeType']
    
    # Sanitize filename to be filesystem-friendly
    sanitized_file_name = original_file_name.replace("/", "_").replace("\\", "_")

    # --- File Type Gatekeeper ---
    # This logic is now more robust. It checks the mime_type directly from Google Drive
    # in addition to the final filename, ensuring we process PDFs even if they lack the extension in their name.
    expected_final_path = get_expected_local_filepath(sanitized_file_name, mime_type, DOWNLOAD_PATH)
    is_supported_type = (
        expected_final_path.lower().endswith('.pdf') or
        expected_final_path.lower().endswith('.docx') or
        mime_type == 'application/pdf'
    )

    if not is_supported_type:
        file_type_for_log = os.path.splitext(original_file_name)[1] or mime_type
        print(f"\n--- Skipping '{original_file_name}' (Type: {file_type_for_log}) because it is not a supported file type. ---")
        return

    temp_local_path = None
    print(f"\n--- Processing '{original_file_name}' (ID: {file_id}) ---")

    try:
        # 1. Log metadata update
        with db_setup.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO file_metadata (file_id, file_name, platform, last_modified)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (file_id) DO UPDATE SET
                        file_name = EXCLUDED.file_name,
                        last_modified = EXCLUDED.last_modified,
                        processed_at = NULL;
                """, (
                    file_id, sanitized_file_name, 'Google Drive',
                    datetime.fromisoformat(file_info['modifiedTime'].replace('Z', '+00:00'))
                ))
                conn.commit()

        # 2. Download the single file
        print(f"  - Downloading...")
        _download_file(drive_service, file_id, sanitized_file_name, mime_type, DOWNLOAD_PATH)
        temp_local_path = get_expected_local_filepath(sanitized_file_name, mime_type, DOWNLOAD_PATH)

        if not os.path.exists(temp_local_path):
            print(f"    - ⚠️ Download failed for '{sanitized_file_name}'. Skipping.")
            return

        # 3. Determine if the document has images and process accordingly
        # This logic is now corrected to send any file with images for full analysis.
        has_images = (expected_final_path.lower().endswith('.pdf') and pdf_has_images(temp_local_path)) or \
                     (expected_final_path.lower().endswith('.docx') and docx_has_images(temp_local_path))

        if has_images:
            # Logic from former process_image_documents
            print(f"  - Analyzing document with images...")
            
            # --- Start Timeout Block ---
            try:
                signal.alarm(LLM_ANALYSIS_TIMEOUT) # Set the alarm
                analysis_result = analyze_document_with_openai_langchain_structured(
                    file_path=temp_local_path, file_id=file_id,
                    user_initial_prompt="Summarize this document and provide a detailed description of each image it contains.",
                    image_output_dir=IMAGE_OUTPUT_DIR
                )
                signal.alarm(0) # Disable the alarm
            except TimeoutException:
                print(f"    - ⚠️ TIMEOUT: AI analysis for '{original_file_name}' took too long and was skipped.")
                # We need to return here to ensure cleanup happens but processing stops.
                return
            finally:
                signal.alarm(0) # Ensure alarm is disabled in any case
            # --- End Timeout Block ---

            if "error" in analysis_result:
                raise RuntimeError(f"AI analysis failed: {analysis_result['error']}")
            
            document_summary = analysis_result.get("document_summary") or ""
            complete_text = analysis_result.get("complete_document_text", "")
            
            # --- Create formatted page content with both summary and full text ---
            formatted_page_content = f"""## Document Summary:
{document_summary}

## Complete Document:
{complete_text}"""
            
            print(f'formatted_page_content: {formatted_page_content}')
            doc = Document(
                page_content=formatted_page_content,
                metadata={
                    "file_id": file_id, "file_name": sanitized_file_name,
                    "image_data": analysis_result.get("image_descriptions", []),
                    "summary_source": True, "processed_at": datetime.now(timezone.utc).isoformat()
                }
            )
            vector_manager.insert_documents_sync(VECTORSTORE_COLLECTION_NAME, [doc])
            print(f"    ✅ Successfully analyzed and embedded.")

        else:
            # Logic from former process_text_documents
            print(f"  - Processing text-only document...")
            raw_text = parse_pdf_text(temp_local_path) if temp_local_path.lower().endswith('.pdf') else parse_docx_text(temp_local_path)
            
            # If a document has no text AND we already know it has no images, then we can safely skip it.
            if not raw_text or not raw_text.strip():
                print(f"    - ⚠️ No text content extracted and no images found in {sanitized_file_name}. Skipping.")
                return

            cleaned_text = re.sub(r'\s+', ' ', raw_text).strip()
            text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=200, length_function=len)
            chunks = text_splitter.split_text(cleaned_text)
            
            docs_to_embed = [
                Document(
                    page_content=chunk,
                    metadata={
                        "file_id": file_id, "file_name": sanitized_file_name, "chunk_number": i + 1,
                        "total_chunks": len(chunks), "summary_source": False,
                        "processed_at": datetime.now(timezone.utc).isoformat()
                    }
                ) for i, chunk in enumerate(chunks)
            ]

            if docs_to_embed:
                vector_manager.insert_documents_sync(VECTORSTORE_COLLECTION_NAME, docs_to_embed)
                print(f"    ✅ Successfully chunked and embedded {len(docs_to_embed)} chunks.")

        # 4. Mark as processed in DB
        update_processed_timestamp(file_id)

    except Exception as e:
        print(f"    ❌ An unexpected error occurred while processing {original_file_name}: {e}")
    finally:
        # 5. Clean up the downloaded file regardless of success or failure
        if temp_local_path and os.path.exists(temp_local_path):
            os.remove(temp_local_path)
            print(f"  - Cleaned up temporary file: {os.path.basename(temp_local_path)}")

def process_text_documents(path_to_file_id_map, vector_manager):
    """DEPRECATED: This function is no longer used in the sequential processing model."""
    print("\n[Step 3a: Processing text-only documents...]")
    if not os.path.exists(TEXT_ONLY_PATH): return

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
        length_function=len
    )

    for file_name in os.listdir(TEXT_ONLY_PATH):
        full_path = os.path.join(TEXT_ONLY_PATH, file_name)
        file_id = path_to_file_id_map.get(full_path)
        if not file_id: continue

        print(f"  - Chunking and embedding '{file_name}'...")
        try:
            raw_text = parse_pdf_text(full_path) if file_name.lower().endswith('.pdf') else parse_docx_text(full_path)
            if not raw_text:
                print(f"    - ⚠️ No text content extracted from {file_name}. Skipping.")
                continue

            # --- Simple & Safe Text Cleaning ---
            # Replace multiple whitespace characters (spaces, tabs, newlines) with a single space.
            # This is a safe way to clean up text without losing semantic structure.
            cleaned_text = re.sub(r'\s+', ' ', raw_text).strip()

            chunks = text_splitter.split_text(cleaned_text)
            print(f"    - Split into {len(chunks)} chunks.")

            docs_to_embed = []
            for i, chunk in enumerate(chunks):
                doc = Document(
                    page_content=chunk,
                    metadata={
                        "file_id": file_id,
                        "file_name": file_name,
                        "chunk_number": i + 1,
                        "total_chunks": len(chunks),
                        "summary_source": False,
                        "processed_at": datetime.now(timezone.utc).isoformat()
                    }
                )
                docs_to_embed.append(doc)

            if docs_to_embed:
                vector_manager.insert_documents_sync(VECTORSTORE_COLLECTION_NAME, docs_to_embed)
                update_processed_timestamp(file_id)
                print(f"    ✅ Successfully embedded {len(docs_to_embed)} chunks.")
        except Exception as e:
            print(f"    ❌ Failed to process and embed {file_name}: {e}")

def process_image_documents(path_to_file_id_map, vector_manager):
    """DEPRECATED: This function is no longer used in the sequential processing model."""
    print("\n[Step 3b: Processing documents with images...]")
    if not os.path.exists(WITH_IMAGES_PATH): return

    os.makedirs(IMAGE_OUTPUT_DIR, exist_ok=True)

    for file_name in os.listdir(WITH_IMAGES_PATH):
        full_path = os.path.join(WITH_IMAGES_PATH, file_name)
        file_id = path_to_file_id_map.get(full_path)
        if not file_id: continue

        print(f"  - Analyzing and embedding '{file_name}'...")
        try:
            analysis_result = analyze_document_with_openai_langchain_structured(
                file_path=full_path, file_id=file_id,
                user_initial_prompt="Summarize this document and provide a detailed description of each image it contains.",
                image_output_dir=IMAGE_OUTPUT_DIR
            )
            if "error" in analysis_result:
                print(f"    ❌ AI analysis failed: {analysis_result['error']}")
                continue
            
            # Gracefully handle cases where the AI returns a null summary
            document_summary = analysis_result.get("document_summary") or ""
            
            doc = Document(
                page_content=document_summary,
                metadata={
                    "file_id": file_id, "file_name": file_name,
                    "image_data": analysis_result.get("image_descriptions", []),
                    "summary_source": True, "processed_at": datetime.now(timezone.utc).isoformat()
                }
            )
            vector_manager.insert_documents_sync(VECTORSTORE_COLLECTION_NAME, [doc])
            update_processed_timestamp(file_id)
            print(f"    ✅ Successfully analyzed and embedded.")
        except Exception as e:
            print(f"    ❌ Failed to process {file_name}: {e}")

def cleanup():
    """Removes temporary directories used during processing."""
    print("\n[Step 4: Cleaning up temporary files...]")
    if os.path.exists(DOWNLOAD_PATH):
        shutil.rmtree(DOWNLOAD_PATH)
        print(f"  - Removed temporary directory: {DOWNLOAD_PATH}")
    print("✅ Cleanup complete.")

def main():
    """Main function to execute the full workflow."""
    print("--- Starting Intelligent Google Drive Processing Pipeline ---")
    
    drive_service = get_drive_service()
    if not drive_service:
        return

    # Create temporary directories
    os.makedirs(TEXT_ONLY_PATH, exist_ok=True)
    os.makedirs(WITH_IMAGES_PATH, exist_ok=True)
    os.makedirs(IMAGE_OUTPUT_DIR, exist_ok=True)

    # --- Phase 0: Clean up deleted files ---
    deleted_files = detect_deleted_files(drive_service)
    if deleted_files:
        print(f"\n[Phase 0: Cleaning up {len(deleted_files)} deleted files...]")
        for i, file_id in enumerate(deleted_files, 1):
            print(f"\n[{i}/{len(deleted_files)}] Deleting file_id: {file_id}")
            try:
                delete_file_completely(file_id)
            except Exception as e:
                print(f"    ❌ Failed to delete {file_id}: {e}")
        print("✅ Deleted files cleanup complete.")
    else:
        print("\n[Phase 0: No deleted files found - all database files exist in Google Drive]")
    # --- Phase 1: Discover all files to be processed ---
    all_files_to_process = []
    for drive_name in SHARED_DRIVE_NAMES:
        print(f"\n---> Discovering files in Drive: '{drive_name}' <---")
        drive_id = get_drive_id(drive_service, drive_name)
        if not drive_id:
            print(f"      - ‼️  Could not find Drive ID for '{drive_name}'. Skipping.")
            continue

        files_for_this_drive = get_files_to_process(drive_service, drive_id)
        if files_for_this_drive is None:
            print("Pipeline stopped due to a database error during file discovery.")
            return
        
        if files_for_this_drive:
            print(f"      - Found {len(files_for_this_drive)} new/updated files.")
            all_files_to_process.extend(files_for_this_drive)
        else:
            print("      - No new or updated files found in this drive.")

    if not all_files_to_process:
        print("\nNo new or updated files to process across all drives. Pipeline finished.")
        cleanup()
        return

    # --- Phase 2: Process each file sequentially ---
    vector_manager = PGVectorManager()
    db_setup = DatabaseSetup()
    processed_history = get_processed_files_history()
    
    # DEBUG: Print processed history
    print(f"\n=== DEBUG: Found {len(processed_history)} files in processed history ===")
    for file_id, timestamp in processed_history.items():
        print(f"  - {file_id}: {timestamp}")
    print("=" * 60)

    try:
        for file_info in all_files_to_process:
            current_file_id = file_info['id']
            current_file_name = file_info['name']
            
            print(f"\n--- Processing '{current_file_name}' (ID: {current_file_id}) ---")
            
            # DEBUG: Explicit check
            if current_file_id in processed_history:
                print(f"  - ✅ File found in processed history, will delete old data")
                delete_vectors_for_file(current_file_id)
            else:
                print(f"  - ❌ File NOT found in processed history, treating as new")
                
                # Additional debug: Check if file exists in database at all
                db_setup_debug = DatabaseSetup()
                try:
                    with db_setup_debug.connect() as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                "SELECT file_id, file_name, processed_at FROM file_metadata WHERE file_id = %s",
                                (current_file_id,)
                            )
                            result = cur.fetchone()
                            if result:
                                print(f"    - File exists in DB: {result[1]}, processed_at: {result[2]}")
                            else:
                                print(f"    - File does NOT exist in database")
                except Exception as e:
                    print(f"    - Error checking database: {e}")
            
            # Process the file
            process_and_embed_file(drive_service, file_info, vector_manager, db_setup)
    finally:
        vector_manager.close_sync()

    # --- Phase 3: Final Cleanup ---
    cleanup()
    
    print("\n--- Intelligent Pipeline Finished ---")


if __name__ == "__main__":
    main()