import os
import json
import sys
from datetime import datetime, timezone
import psycopg2
import re
from dotenv import load_dotenv

# --- Setup System Path ---
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.append(project_root)

# --- Project-specific Imports ---
try:
    from app.database.pg_vector import PGVectorManager
    from app.database.tables import DatabaseSetup
    from langchain.docstore.document import Document
    from langchain.text_splitter import RecursiveCharacterTextSplitter
    # We will use simple text extractors, assuming they exist in a utils file
    from app.services.document_analyzer import parse_docx_text, parse_pdf_text

except ImportError as e:
    print(f"Error: Failed to import a required module. Ensure all components exist.")
    print(f"Details: {e}")
    sys.exit(1)

# --- Configuration ---
load_dotenv()
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
TEXTUAL_FILES_PATH = os.path.join(BASE_PATH, "textual_documents")
FILE_MAPPING_PATH = os.path.join(BASE_PATH, "file_id_to_local_path.json")
VECTORSTORE_COLLECTION_NAME = os.getenv("VECTORSTORE_COLLECTION_NAME", "google_drive_data")

# --- Helper Functions ---

def get_reverse_mapping(mapping_path: str) -> dict:
    """Loads the JSON map and reverses it from {file_id: path} to {path: file_id}."""
    try:
        with open(mapping_path, 'r') as f:
            file_id_to_path = json.load(f)
        return {v: k for k, v in file_id_to_path.items()}
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error reading or parsing mapping file at '{mapping_path}': {e}")
        return {}

def update_processed_timestamp(file_id: str):
    """Updates the 'processed_at' timestamp for a specific file_id in the database."""
    db_setup = DatabaseSetup()
    conn = None
    try:
        conn = db_setup.connect()
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE file_metadata SET processed_at = %s WHERE file_id = %s;",
                (datetime.now(timezone.utc), file_id)
            )
            conn.commit()
            print(f"    ✅ Marked file_id '{file_id}' as processed.")
    except psycopg2.Error as e:
        print(f"    ❌ DB error updating timestamp for {file_id}: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def main():
    """Main function to process and embed textual documents."""
    print("--- Starting Textual Document Embedding Script ---")

    # 1. Initialization
    print("\n[Step 1: Initializing...]")
    path_to_file_id_map = get_reverse_mapping(FILE_MAPPING_PATH)
    if not path_to_file_id_map:
        return

    vector_manager = PGVectorManager()
    
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
        length_function=len
    )
    
    try:
        files_to_process = [f for f in os.listdir(TEXTUAL_FILES_PATH) if f.lower().endswith(('.pdf', '.docx'))]
        if not files_to_process:
            print("No textual documents found to process. Exiting.")
            return
        print(f"Found {len(files_to_process)} textual documents to process.")

        # 2. Process each file
        for file_name in files_to_process:
            full_path = os.path.join(TEXTUAL_FILES_PATH, file_name)
            print(f"\n--- Processing: {file_name} ---")

            file_id = path_to_file_id_map.get(full_path)
            if not file_id:
                print(f"  - ⚠️ Warning: Could not find file_id for '{full_path}'. Skipping.")
                continue
            print(f"  - Found File ID: {file_id}")

            # Extract text content
            print(f"  - Processing and chunking '{file_name}'...")
            try:
                raw_text = parse_pdf_text(full_path) if file_name.lower().endswith('.pdf') else parse_docx_text(full_path)
                if not raw_text:
                    print(f"    - ⚠️ No text content extracted. Skipping.")
                    continue

                # --- Simple & Safe Text Cleaning ---
                # Replace multiple whitespace characters with a single space.
                cleaned_text = re.sub(r'\s+', ' ', raw_text).strip()
                
                chunks = text_splitter.split_text(cleaned_text)
                print(f"    - Split into {len(chunks)} chunks.")

            except Exception as e:
                print(f"  - ❌ Error extracting or chunking text from '{file_name}': {e}")
                continue

            # Create LangChain Documents for each chunk
            docs_to_embed = []
            for i, chunk in enumerate(chunks):
            doc = Document(
                    page_content=chunk,
                metadata={
                    "file_id": file_id,
                    "file_name": file_name,
                        "chunk_number": i + 1,
                        "total_chunks": len(chunks),
                    "summary_source": False, # Flag this as raw text
                    "processed_at": datetime.now(timezone.utc).isoformat()
                }
            )
                docs_to_embed.append(doc)

            # Ingest into PGVector
            if docs_to_embed:
            try:
                    print(f"  - Ingesting {len(docs_to_embed)} chunks into collection '{VECTORSTORE_COLLECTION_NAME}'...")
                vector_manager.insert_documents_sync(
                    collection_name=VECTORSTORE_COLLECTION_NAME,
                        documents=docs_to_embed
                )
                print("  - ✅ Ingestion successful.")
                update_processed_timestamp(file_id)

            except Exception as e:
                print(f"  - ❌ Error during vector ingestion for '{file_name}': {e}")

    finally:
        vector_manager.close_sync()
        print("\n--- Script Finished ---")

if __name__ == "__main__":
    main() 