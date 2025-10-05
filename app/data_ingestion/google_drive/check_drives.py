import os
import sys

# This ensures that the script can import the drive service
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
if project_root not in sys.path:
    sys.path.append(project_root)

try:
    from app.services.google_drive_service import get_drive_service
    from googleapiclient.errors import HttpError
except ImportError as e:
    print(f"Error: Failed to import a required module.")
    print(f"Details: {e}")
    sys.exit(1)

def list_all_visible_shared_drives():
    """
    Connects to the Google Drive API and lists all Shared Drives
    that the authenticated user has access to.
    """
    print("--- Attempting to list all visible Shared Drives... ---")
    
    service = get_drive_service()
    if not service:
        print("Could not authenticate with Google Drive service.")
        return

    try:
        page_token = None
        all_drives = []
        while True:
            # Call the drives().list method without any query to get all drives
            response = service.drives().list(
                fields="nextPageToken, drives(id, name)",
                pageToken=page_token,
                pageSize=100 # Get up to 100 at a time
            ).execute()
            
            drives = response.get('drives', [])
            all_drives.extend(drives)
            
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break
        
        if not all_drives:
            print("\n❌ No Shared Drives were found for the authenticated user.")
            print("   This is likely a permissions issue. Please check which account authorized this application.")
        else:
            print(f"\n✅ Successfully found {len(all_drives)} Shared Drive(s):")
            for drive in all_drives:
                print(f"  - Name: '{drive.get('name')}'  (ID: {drive.get('id')})")

    except HttpError as error:
        print(f"\nAn API error occurred: {error}")
        print("Please ensure the Google Drive API is enabled in your Google Cloud project.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

if __name__ == "__main__":
    list_all_visible_shared_drives() 