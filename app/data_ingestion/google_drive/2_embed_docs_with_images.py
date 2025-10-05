import os
import json
import sys
# import asyncio # No longer needed
from datetime import datetime, timezone
import psycopg2

from dotenv import load_dotenv

# This finds the project root by going up four directories and adds it to the path
# drive_processor.py -> google_drive -> data_ingestion -> app -> project root
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.append(project_root)

# --- Project-specific Imports ---
try:
    from app.services.document_analyzer import analyze_document_with_openai_langchain_structured
    from app.database.pg_vector import PGVectorManager
    from app.database.tables import DatabaseSetup
    from langchain.docstore.document import Document
except ImportError as e:
    print(f"Error: Failed to import a required module. Ensure all components exist.")
    print(f"Details: {e}")
    sys.exit(1)

# --- Configuration ---
load_dotenv()
BASE_PATH = os.path.dirname(os.path.abspath(__file__))
FILES_WITH_IMAGES_PATH = os.path.join(BASE_PATH, "files_with_images")
FILE_MAPPING_PATH = os.path.join(BASE_PATH, "file_id_to_local_path.json")

# This is where the analysis function will save the actual image files.
# It should be a persistent location if you want to reference the images later.
IMAGE_OUTPUT_DIR = os.path.join(project_root, "extracted_document_images") 

VECTORSTORE_COLLECTION_NAME = os.getenv("VECTORSTORE_COLLECTION_NAME", "google_drive_data")

# --- Main Logic ---

def get_reverse_mapping(mapping_path: str) -> dict:
    """Loads the JSON map and reverses it from {file_id: path} to {path: file_id}."""
    try:
        with open(mapping_path, 'r') as f:
            file_id_to_path = json.load(f)
        
        # Invert the dictionary. Note: This assumes all local paths are unique.
        path_to_file_id = {v: k for k, v in file_id_to_path.items()}
        return path_to_file_id
    except FileNotFoundError:
        print(f"Error: Mapping file not found at '{mapping_path}'. Cannot proceed.")
        return {}
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{mapping_path}'.")
        return {}

def update_processed_timestamp(file_id: str):
    """Updates the 'processed_at' timestamp for a specific file_id in the database."""
    db_setup = DatabaseSetup()
    conn = None
    try:
        conn = db_setup.connect()
        with conn.cursor() as cur:
            update_query = """
                UPDATE file_metadata
                SET processed_at = %s
                WHERE file_id = %s;
            """
            cur.execute(update_query, (datetime.now(timezone.utc), file_id))
            conn.commit()
            print(f"    ✅ Marked file_id '{file_id}' as processed in the database.")
    except psycopg2.Error as e:
        print(f"    ❌ Database error while updating timestamp for {file_id}: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def main():
    """
    Main synchronous function to execute the processing and embedding workflow.
    """
    print("--- Starting Document Processing and Embedding Script ---")
    
    # 1. Initialization
    print("\n[Step 1: Initializing and loading mappings...]")
    os.makedirs(IMAGE_OUTPUT_DIR, exist_ok=True)
    
    path_to_file_id_map = get_reverse_mapping(FILE_MAPPING_PATH)
    if not path_to_file_id_map:
        return
        
    vector_manager = PGVectorManager()

    files_to_process = [f for f in os.listdir(FILES_WITH_IMAGES_PATH) if f.lower().endswith(('.pdf', '.docx'))]
    if not files_to_process:
        print("No files found in the 'files_with_images' folder. Exiting.")
        return
        
    print(f"Found {len(files_to_process)} files to process.")

    # 2. Loop through files and process them
    for file_name in files_to_process:
        full_path = os.path.join(FILES_WITH_IMAGES_PATH, file_name)
        print(f"\n--- Processing: {file_name} ---")

        # Step A: Get File ID
        file_id = path_to_file_id_map.get(full_path)
        if not file_id:
            print(f"  - ⚠️ Warning: Could not find file_id for '{full_path}'. Skipping.")
            continue
        print(f"  - Found File ID: {file_id}")
        
        # Step B: Analyze with LLM
        print(f"  - Sending to AI for analysis...")
        user_prompt = "Summarize this document and provide a detailed description of each image it contains."
        analysis_result = analyze_document_with_openai_langchain_structured(
            file_path=full_path,
            file_id=file_id,
            user_initial_prompt=user_prompt,
            image_output_dir=IMAGE_OUTPUT_DIR
        )

        if "error" in analysis_result:
            print(f"  - ❌ AI analysis failed for '{file_name}': {analysis_result['error']}. Skipping.")
            continue
        
        print("  - ✅ AI analysis successful.")
        
        # Step C: Prepare Data for Vector Store
        document_summary = analysis_result.get("document_summary", "No summary provided.")
        image_descriptions = analysis_result.get("image_descriptions", [])
        
        # Create a single LangChain Document for the summary
        doc_to_embed = Document(
            page_content=document_summary,
            metadata={
                "file_id": file_id,
                "file_name": file_name,
                "image_data": image_descriptions,
                "summary_source": True, # Flag to identify this as a summary embedding
                "processed_at": datetime.now(timezone.utc).isoformat()
            }
        )
        
        # Step D: Ingest into PGVector
        try:
            print(f"  - Creating embedding and ingesting into collection '{VECTORSTORE_COLLECTION_NAME}'...")
            vector_manager.insert_documents_sync(
                collection_name=VECTORSTORE_COLLECTION_NAME,
                documents=[doc_to_embed], # Must be a list
            )
            print("  - ✅ Ingestion successful.")
            
            # Step E: Mark as Processed (only after successful ingestion)
            update_processed_timestamp(file_id)

        except Exception as e:
            print(f"  - ❌ An error occurred during vector ingestion for '{file_name}': {e}")

    # Final cleanup
    vector_manager.close_sync()
    print("\n--- Script Finished ---")

if __name__ == "__main__":
    main()