import os
import json
import sys
import shutil
from datetime import datetime, timezone
from collections import Counter
import psycopg2
from dotenv import load_dotenv

# This finds the project root by going up four directories and adds it to the path
# drive_processor.py -> google_drive -> data_ingestion -> app -> project root
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.append(project_root)

# --- Project-specific Imports ---
# These are imported after setting the system path
try:
    from app.services.google_drive_service import get_drive_service, _download_file
    from app.database.tables import DatabaseSetup
    from app.personal_work.summaries_using_langchain import docx_has_images, pdf_has_images
except ImportError as e:
    print(f"Error: Failed to import a required module. Make sure the script is in the correct directory.")
    print(f"Details: {e}")
    sys.exit(1)

# --- Configuration ---
load_dotenv()
SHARED_DRIVE_NAME = "Company Shared Drive"
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_PATH = os.path.join(BASE_PATH, "downloaded_files")
IMAGE_EXTRACTION_PATH = os.path.join(BASE_PATH, "temp_extracted_images")
FILE_MAPPING_PATH = os.path.join(BASE_PATH, "file_id_to_local_path.json")

# --- New Paths for Sorted Documents ---
TEXT_ONLY_PATH = os.path.join(BASE_PATH, "textual_documents")
WITH_IMAGES_PATH = os.path.join(BASE_PATH, "files_with_images")

# --- Helper Functions ---

def get_drive_id(service, drive_name: str):
    """Finds the ID of a Google Shared Drive by its name."""
    try:
        page_token = None
        while True:
            response = service.drives().list(
                q=f"name='{drive_name}'",
                fields="nextPageToken, drives(id, name)",
                pageToken=page_token
            ).execute()
            drives = response.get('drives', [])
            if drives:
                return drives[0]['id']
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break
        
        print(f"Error: Shared Drive '{drive_name}' not found.")
        return None
    except Exception as e:
        print(f"An error occurred while searching for the Shared Drive: {e}")
        return None

def get_all_files_in_folder(service, folder_id: str, is_shared_drive: bool = False, shared_drive_id: str = None):
    """Recursively gets all file details within a given folder ID."""
    all_files = []
    query = f"'{folder_id}' in parents and trashed=false"
    page_token = None
    
    try:
        while True:
            list_args = {
                'q': query,
                'spaces': "drive",
                'fields': "nextPageToken, files(id, name, mimeType, modifiedTime)",
                'pageToken': page_token
            }
            if is_shared_drive:
                list_args['corpora'] = 'drive'
                list_args['driveId'] = shared_drive_id
                list_args['includeItemsFromAllDrives'] = True
                list_args['supportsAllDrives'] = True

            response = service.files().list(**list_args).execute()
            
            for item in response.get('files', []):
                if item.get('mimeType') == 'application/vnd.google-apps.folder':
                    # It's a folder, recurse into it, passing the original shared_drive_id
                    all_files.extend(get_all_files_in_folder(service, item.get('id'), is_shared_drive, shared_drive_id))
                else:
                    # It's a file, add it to our list
                    all_files.append(item)
            
            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break
        return all_files
    except Exception as e:
        print(f"An error occurred during recursive file search: {e}")
        return []

def get_expected_local_filepath(file_name: str, mime_type: str, download_dir: str) -> str:
    """Determines the final local file path, including extension for exported Google Docs."""
    export_mimetypes = {
        'application/vnd.google-apps.document': '.pdf',
        'application/vnd.google-apps.spreadsheet': '.xlsx',
        'application/vnd.google-apps.presentation': '.pdf',
    }
    
    # Google Docs files have no extension in their name, so we add one.
    # Other files (like an uploaded PDF) already have an extension.
    if mime_type in export_mimetypes:
        extension = export_mimetypes[mime_type]
        # Ensure we don't add a duplicate extension if the name already has one
        base_name, ext = os.path.splitext(file_name)
        if ext.lower() != extension:
             return os.path.join(download_dir, f"{file_name}{extension}")
        else:
            return os.path.join(download_dir, file_name)
    else:
        return os.path.join(download_dir, file_name)

# --- Main Logic ---

def main():
    """Main function to execute the full workflow."""
    print("--- Starting Google Drive File Processing Script ---")

    # 1. Initialization
    print("\n[Step 1: Initializing...]")
    os.makedirs(DOWNLOAD_PATH, exist_ok=True)
    os.makedirs(IMAGE_EXTRACTION_PATH, exist_ok=True)
    # Create the destination folders for sorted files
    os.makedirs(TEXT_ONLY_PATH, exist_ok=True)
    os.makedirs(WITH_IMAGES_PATH, exist_ok=True)
    
    drive_service = get_drive_service()
    if not drive_service:
        print("Could not connect to Google Drive. Exiting.")
        return

    db_setup = DatabaseSetup()
    try:
        conn = db_setup.connect()
        cur = conn.cursor()
        print("✅ Database connection successful.")
    except psycopg2.OperationalError as e:
        print(f"❌ Could not connect to the database: {e}")
        return

    # 2. Download files, record metadata, and create mapping
    print(f"\n[Step 2: Downloading files from '{SHARED_DRIVE_NAME}' and recording metadata...]")
    drive_id = get_drive_id(drive_service, SHARED_DRIVE_NAME)
    if not drive_id:
        conn.close()
        return

    files_to_process = get_all_files_in_folder(drive_service, drive_id, is_shared_drive=True, shared_drive_id=drive_id)
    if not files_to_process:
        print("No files found in the folder. Exiting.")
        conn.close()
        return

    file_id_to_local_path = {}
    print(f"Found {len(files_to_process)} files. Starting download and database logging...")

    DOWNLOAD_LIMIT = 20
    download_count = 0

    for file_info in files_to_process:
        file_id = file_info['id']
        file_name = file_info['name']
        mime_type = file_info['mimeType']
        
        # Define allowed mime types for PDF, DOCX, and Google Docs
        allowed_mime_types = [
            'application/pdf',  # Standard PDF files
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document',  # Microsoft Word (.docx)
            'application/vnd.google-apps.document'  # Google Docs (will be exported as PDF)
        ]

        # Skip files that are not of the allowed types
        if mime_type not in allowed_mime_types:
            print(f"  - Skipping '{file_name}' (type: {mime_type}). Not a PDF or DOCX.")
            continue
        
        # Stop if the download limit has been reached
        if download_count >= DOWNLOAD_LIMIT:
            print(f"\nReached test limit of {DOWNLOAD_LIMIT} files. Stopping download phase.")
            break
        
        print(f"  - Processing '{file_name}'...")
        
        # Insert metadata into the database
        try:
            cur.execute("""
                INSERT INTO file_metadata (file_id, file_name, platform, last_modified, processed_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (file_id) DO UPDATE SET
                    file_name = EXCLUDED.file_name,
                    last_modified = EXCLUDED.last_modified,
                    processed_at = EXCLUDED.processed_at;
            """, (
                file_id,
                file_name,
                'Google Drive',
                datetime.fromisoformat(file_info['modifiedTime'].replace('Z', '+00:00')),
                None
            ))
            
            # Download the file
            _download_file(drive_service, file_id, file_name, mime_type, DOWNLOAD_PATH)
            
            # Record the mapping
            local_path = get_expected_local_filepath(file_name, mime_type, DOWNLOAD_PATH)
            file_id_to_local_path[file_id] = local_path
            
            # Increment the counter for each downloaded file
            download_count += 1
            
        except Exception as e:
            print(f"    ❌ Failed to process file {file_name}: {e}")
            conn.rollback() # Rollback the transaction for this file
        else:
            conn.commit() # Commit the transaction for this file

    cur.close()
    conn.close()

    # 3. Identify and quantify file types
    print("\n[Step 3: Analyzing downloaded file types...]")
    try:
        downloaded_files = os.listdir(DOWNLOAD_PATH)
        if not downloaded_files:
            print("No files were downloaded.")
        else:
            extensions = [os.path.splitext(f)[1].lower() or ".no_extension" for f in downloaded_files]
            ext_counts = Counter(extensions)
            print("File type quantities:")
            for ext, count in ext_counts.items():
                print(f"  - {ext}: {count} file(s)")
    except Exception as e:
        print(f"❌ Could not analyze file types: {e}")

    # 4. Classify and move files based on content
    print("\n[Step 4: Classifying and moving documents based on content...]")
    text_only_files = []
    text_with_images_files = []
    unsupported_files = []

    # Get a fresh list of files from the download directory
    downloaded_files = os.listdir(DOWNLOAD_PATH)
    downloaded_files.sort() 

    for file_name in downloaded_files:
        source_path = os.path.join(DOWNLOAD_PATH, file_name)
        
        try:
            if file_name.lower().endswith(".pdf"):
                # Use the lightweight checker
                if pdf_has_images(source_path):
                    text_with_images_files.append(file_name)
                    shutil.move(source_path, os.path.join(WITH_IMAGES_PATH, file_name))
                else:
                    text_only_files.append(file_name)
                    shutil.move(source_path, os.path.join(TEXT_ONLY_PATH, file_name))
            elif file_name.lower().endswith(".docx"):
                # Use the lightweight checker
                if docx_has_images(source_path):
                    text_with_images_files.append(file_name)
                    shutil.move(source_path, os.path.join(WITH_IMAGES_PATH, file_name))
                else:
                    text_only_files.append(file_name)
                    shutil.move(source_path, os.path.join(TEXT_ONLY_PATH, file_name))
            else:
                unsupported_files.append(file_name)
                # Unsupported files will remain in the 'downloaded_files' folder.
        except Exception as e:
            print(f"    ❌ Could not process or move file {file_name}: {e}")
            unsupported_files.append(file_name)

    print("\n--- Classification and Move Results ---")
    print(f"\n📄 Text-Only Files (moved to '{os.path.basename(TEXT_ONLY_PATH)}'):")
    if text_only_files:
        for f in text_only_files: print(f"  - {f}")
    else:
        print("  (None found)")

    print(f"\n🖼️ Files with Text and Images (moved to '{os.path.basename(WITH_IMAGES_PATH)}'):")
    if text_with_images_files:
        for f in text_with_images_files: print(f"  - {f}")
    else:
        print("  (None found)")
    
    if unsupported_files:
        print(f"\n❔ Unsupported or failed files (left in '{os.path.basename(DOWNLOAD_PATH)}'):")
        for f in unsupported_files: print(f"  - {f}")


    # Step 5: Update file paths and save the final mapping
    print("\n[Step 5: Updating file paths in mapping for final locations...]")
    for file_id, old_path in list(file_id_to_local_path.items()):
        file_name = os.path.basename(old_path)
        if file_name in text_only_files:
            new_path = os.path.join(TEXT_ONLY_PATH, file_name)
            file_id_to_local_path[file_id] = new_path
        elif file_name in text_with_images_files:
            new_path = os.path.join(WITH_IMAGES_PATH, file_name)
            file_id_to_local_path[file_id] = new_path
        # Note: If a file was not moved (e.g., unsupported), its original path in 'downloaded_files' is correct.
    print("✅ Path mapping updated.")

    with open(FILE_MAPPING_PATH, 'w') as f:
        json.dump(file_id_to_local_path, f, indent=2)
    print(f"✅ Final mapping saved to '{FILE_MAPPING_PATH}'.")

    # Cleanup temporary image extraction directory
    shutil.rmtree(IMAGE_EXTRACTION_PATH)
    print(f"\n✅ Cleaned up temporary directory: {IMAGE_EXTRACTION_PATH}")

    # Cleanup the download directory, as all relevant files have been moved
    if os.path.exists(DOWNLOAD_PATH):
        shutil.rmtree(DOWNLOAD_PATH)
        print(f"✅ Cleaned up download directory: {DOWNLOAD_PATH}")

    print("\n--- Script Finished ---")


if __name__ == "__main__":
    main()