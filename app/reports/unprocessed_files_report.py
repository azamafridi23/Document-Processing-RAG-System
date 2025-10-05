import os
import sys
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv

# --- Setup System Path ---
# This finds the project root by going up three directories and adds it to the path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.append(project_root)

# --- Project-specific Imports ---
try:
    from app.services.google_drive_service import get_drive_service, get_drive_id, get_all_files_in_folder
    from app.database.tables import DatabaseSetup
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
# File size limit in Megabytes.
MAX_FILE_SIZE_MB = 50
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024

SUPPORTED_MIMETYPES = {
    'application/pdf',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document', # .docx
    'application/vnd.google-apps.document' # Google Doc
}

def get_ingested_file_ids() -> set:
    """
    Fetches the set of file IDs for all files that have a 'processed_at' timestamp.
    These are files that have been successfully ingested.
    """
    db_setup = DatabaseSetup()
    ingested_ids = set()
    try:
        with db_setup.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT file_id FROM file_metadata WHERE processed_at IS NOT NULL")
                for record in cur.fetchall():
                    ingested_ids.add(record[0])
        print(f"Found {len(ingested_ids)} successfully ingested files in the database.")
        return ingested_ids
    except psycopg2.Error as e:
        print(f"❌ Database error while fetching ingested file history: {e}")
        return None


def get_large_unprocessed_files_from_drive(drive_service, ingested_file_ids: set) -> list:
    """
    Scans Google Drives for large files that have not been ingested.
    """
    print(f"\n[Step 1: Searching for files > {MAX_FILE_SIZE_MB}MB that are not yet ingested...]")
    large_unprocessed_files = []

    for drive_name in SHARED_DRIVE_NAMES:
        print(f"\n---> Scanning Drive: '{drive_name}' <---")
        drive_id = get_drive_id(drive_service, drive_name)
        if not drive_id:
            print(f"      - ‼️  Could not find Drive ID for '{drive_name}'. Skipping.")
            continue

        all_drive_files = get_all_files_in_folder(drive_service, drive_id, is_shared_drive=True, shared_drive_id=drive_id)
        if not all_drive_files:
            print(f"      - No files found in '{drive_name}'.")
            continue
        
        for file_info in all_drive_files:
            file_id = file_info['id']
            file_name = file_info['name']
            mime_type = file_info['mimeType']
            
            # Check if file is supported type
            if mime_type not in SUPPORTED_MIMETYPES:
                continue

            # Check if file has been ingested
            if file_id in ingested_file_ids:
                continue

            # Check size (key may not exist for G-Suite native files until they are exported)
            if 'size' in file_info and int(file_info['size']) > MAX_FILE_SIZE_BYTES:
                size_in_mb = int(file_info['size']) / (1024 * 1024)
                print(f"  - Found large, un-ingested file: '{file_name}' ({size_in_mb:.2f} MB)")
                large_unprocessed_files.append({
                    "file_id": file_id,
                    "file_name": file_name,
                    "reason": f"Large file ({size_in_mb:.2f} MB) not ingested."
                })
    
    print(f"✅ Found {len(large_unprocessed_files)} large, un-ingested files.")
    return large_unprocessed_files


def get_all_unprocessed_files_from_db() -> list:
    """
    Fetches all records from the file_metadata table where 'processed_at' is NULL.
    """
    print("\n[Step 2: Searching for all files marked as 'unprocessed' in the database...]")
    unprocessed_files_db = []
    db_setup = DatabaseSetup()
    try:
        with db_setup.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT file_id, file_name FROM file_metadata WHERE processed_at IS NULL")
                records = cur.fetchall()
                for record in records:
                    unprocessed_files_db.append({
                        "file_id": record[0],
                        "file_name": record[1],
                        "reason": "File exists in DB but 'processed_at' is NULL."
                    })
        print(f"✅ Found {len(unprocessed_files_db)} files marked as unprocessed in the database.")
        return unprocessed_files_db
    except psycopg2.Error as e:
        print(f"❌ Database error while fetching unprocessed files: {e}")
        return []

def main():
    """Main function to execute the report generation."""
    print("--- Starting Unprocessed Files Report ---")
    
    drive_service = get_drive_service()
    if not drive_service:
        print("Failed to initialize Google Drive service. Exiting.")
        return

    # Get a list of files we know have been ingested successfully.
    ingested_file_ids = get_ingested_file_ids()
    if ingested_file_ids is None:
        print("Pipeline stopped due to a database error.")
        return

    # Find large files in Drive that are not on our ingested list.
    large_files = get_large_unprocessed_files_from_drive(drive_service, ingested_file_ids)

    # Find all files in our DB that are explicitly marked as unprocessed.
    db_unprocessed = get_all_unprocessed_files_from_db()

    # --- Merge the final list ---
    final_unprocessed_list = {}
    for item in large_files + db_unprocessed:
        # Use a dictionary to automatically handle duplicates by file_id
        if item['file_id'] not in final_unprocessed_list:
            final_unprocessed_list[item['file_id']] = item
    
    print("\n\n--- FINAL REPORT: Unprocessed Files ---")
    if not final_unprocessed_list:
        print("✅ All supported files are processed.")
    else:
        file_list = sorted(final_unprocessed_list.values(), key=lambda x: x['file_name'])
        print(f"Found a total of {len(file_list)} unprocessed files:\n")
        for i, item in enumerate(file_list, 1):
            print(f"{i}. File Name: {item['file_name']}")
            print(f"   File ID: {item['file_id']}")
            print(f"   Reason: {item['reason']}\n")

    print("--- Report Finished ---")


if __name__ == "__main__":
    main()