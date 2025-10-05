import os.path
import io
from googleapiclient.http import MediaIoBaseDownload

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Import the unified scopes from the Gmail manager
from app.services.gmail_manager import SCOPES


def get_drive_service():
    """
    Authenticates with the Google Drive API and returns a service object.
    Handles token creation, storage, and refresh using a shared token.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                "credentials.json", SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    try:
        service = build("drive", "v3", credentials=creds)
        return service
    except HttpError as error:
        print(f"An error occurred while building the service: {error}")
        return None


def get_user_and_folder_info(service):
    """
    Gets the current user's name, email, and lists all available folders.

    Args:
        service: Authorized Google Drive API service instance.
    """
    try:
        # 1. Get User Information using the 'about' resource
        about = service.about().get(fields="user(displayName, emailAddress)").execute()
        user_info = about.get('user', {})
        print("\n--- User Information ---")
        print(f"Name: {user_info.get('displayName')}")
        print(f"Email: {user_info.get('emailAddress')}")

        # 2. List all folders
        print("\n--- Available Folders ---")
        folders = []
        page_token = None
        # Query for items of the folder mimeType that are not in the trash
        query = "mimeType='application/vnd.google-apps.folder' and trashed=false"
        while True:
            response = service.files().list(
                q=query,
                spaces="drive",
                fields="nextPageToken, files(id, name)",
                pageToken=page_token
            ).execute()
            folders.extend(response.get("files", []))
            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break
        
        if folders:
            for folder in folders:
                print(f"- {folder.get('name')} (ID: {folder.get('id')})")
        else:
            print("No folders found.")

    except HttpError as error:
        print(f"An error occurred: {error}")


def get_files_from_folder_by_name(service, folder_name):
    """
    Finds a folder by name and recursively returns a list of dictionaries for all files inside.
    Each dictionary contains the name, id, and modified time of a file.

    Args:
        service: Authorized Google Drive API service instance.
        folder_name: The name of the top-level folder to search for.

    Returns:
        A flat list of dictionaries for every file found in the folder and its sub-folders.
        Returns an empty list if the folder is not found or is empty.
    """
    try:
        query = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and trashed=false"
        response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        folders = response.get('files', [])
        
        if not folders:
            print(f"\nFolder '{folder_name}' not found.")
            return []
        
        top_folder = folders[0]
        print(f"\nFound top-level folder '{top_folder.get('name')}' with ID: {top_folder.get('id')}")
        
        all_files = []
        _recursive_get_files(service, top_folder.get('id'), all_files)
        return all_files

    except HttpError as error:
        print(f"An error occurred finding the folder: {error}")
        return []

def _recursive_get_files(service, folder_id, all_files):
    """A helper function to recursively get all file details."""
    query = f"'{folder_id}' in parents and trashed=false"
    page_token = None
    
    try:
        while True:
            response = service.files().list(
                q=query,
                spaces="drive",
                fields="nextPageToken, files(id, name, mimeType, modifiedTime)",
                pageToken=page_token
            ).execute()
            
            for item in response.get('files', []):
                if item.get('mimeType') == 'application/vnd.google-apps.folder':
                    # It's a folder, recurse into it
                    _recursive_get_files(service, item.get('id'), all_files)
                else:
                    # It's a file, add it to our list
                    all_files.append({
                        "name": item.get("name"), 
                        "id": item.get("id"),
                        "modifiedTime": item.get("modifiedTime")
                    })
            
            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break
    except HttpError as error:
        print(f"An error occurred during recursive file search: {error}")


def download_files_from_folder(service, folder_name):
    """
    Finds a folder by name and recursively downloads all files from it and its sub-folders.

    Args:
        service: Authorized Google Drive API service instance.
        folder_name: The name of the top-level folder to download files from.
    """
    try:
        query = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and trashed=false"
        response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        folders = response.get('files', [])
        
        if not folders:
            print(f"\nFolder '{folder_name}' not found.")
            return
        
        top_folder = folders[0]
        print(f"\nFound top-level folder '{top_folder.get('name')}' with ID: {top_folder.get('id')}")
        
        # Start the recursive download process
        _recursive_download(service, top_folder.get('id'), "app/data_ingestion/google_drive/google_drive_files")

    except HttpError as error:
        print(f"An error occurred finding the folder: {error}")

def _recursive_download(service, folder_id, local_base_path):
    """
    A helper function to recursively download files, mirroring the folder structure.

    Args:
        service: Authorized Google Drive API service instance.
        folder_id: The ID of the Google Drive folder to process.
        local_base_path: The local path to save files into.
    """
    os.makedirs(local_base_path, exist_ok=True)
    
    query = f"'{folder_id}' in parents and trashed=false"
    page_token = None

    try:
        while True:
            response = service.files().list(
                q=query,
                spaces="drive",
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token
            ).execute()
            
            for item in response.get('files', []):
                item_name = item.get('name')
                item_id = item.get('id')
                mime_type = item.get('mimeType')
                local_item_path = os.path.join(local_base_path, item_name)

                if mime_type == 'application/vnd.google-apps.folder':
                    print(f"Entering sub-folder: '{item_name}'...")
                    _recursive_download(service, item_id, local_item_path)
                else:
                    print(f"Downloading file: '{item_name}' to '{local_base_path}/'")
                    _download_file(service, item_id, item_name, mime_type, local_base_path)
            
            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break
    except HttpError as error:
        print(f"An error occurred during recursive download: {error}")

def _download_file(service, file_id, file_name, mime_type, local_path):
    """A helper function to handle the actual file download/export logic."""
    export_mimetypes = {
        'application/vnd.google-apps.document': {'mimeType': 'application/pdf', 'extension': '.pdf'},
        'application/vnd.google-apps.spreadsheet': {'mimeType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 'extension': '.xlsx'},
        'application/vnd.google-apps.presentation': {'mimeType': 'application/pdf', 'extension': '.pdf'},
    }

    try:
        if mime_type in export_mimetypes:
            export_info = export_mimetypes[mime_type]
            # Add supportsAllDrives for shared drive compatibility
            request = service.files().export_media(
                fileId=file_id, 
                mimeType=export_info['mimeType']
            )
            # This parameter needs to be added to the underlying request URI
            request.uri += "&supportsAllDrives=true"
            file_path = os.path.join(local_path, f"{file_name}{export_info['extension']}")
        else:
            # Add supportsAllDrives for shared drive compatibility
            request = service.files().get_media(
                fileId=file_id, 
                supportsAllDrives=True
            )
            file_path = os.path.join(local_path, file_name)

        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                print(f"  -> Progress: {int(status.progress() * 100)}%")

        with open(file_path, 'wb') as f:
            f.write(fh.getbuffer())
        print(f"  -> Successfully saved '{os.path.basename(file_path)}'")
    
    except HttpError as error:
        # If a 403 error occurs, it might be due to a lack of permissions on the shared drive.
        if error.resp.status == 403:
            print(f"  -> ERROR: Permission denied for file '{file_name}' (ID: {file_id}).")
            print("      Please ensure the service account or user has access to the 'Company Shared Drive'.")
        else:
            print(f"  -> An error occurred downloading '{file_name}': {error}")


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
    # If it's the top-level of a shared drive, list all files and folders at the root.
    # The initial folder_id is the drive_id itself.
    if is_shared_drive and folder_id == shared_drive_id:
        query = f"'{folder_id}' in parents and trashed=false"
    else:
        query = f"'{folder_id}' in parents and trashed=false"

    page_token = None
    
    try:
        while True:
            list_args = {
                'q': query,
                'spaces': "drive",
                'fields': "nextPageToken, files(id, name, mimeType, modifiedTime, size)",
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
                    all_files.extend(get_all_files_in_folder(service, item.get('id'), is_shared_drive, shared_drive_id))
                else:
                    all_files.append(item)
            
            page_token = response.get("nextPageToken", None)
            if page_token is None:
                break
        return all_files
    except Exception as e:
        print(f"An error occurred during recursive file search: {e}")
        return []


if __name__ == "__main__":
    drive_service = get_drive_service()
    if drive_service:
        # Call the new function to display user and folder info first.
        get_user_and_folder_info(drive_service)
        
        print("\n" + "="*40)
        
        # FOLDER_NAME = "Test Folder" 
        
        # file_list = get_files_from_folder_by_name(drive_service, FOLDER_NAME)

        # if file_list:
        #     print(f"\nFound a total of {len(file_list)} files in '{FOLDER_NAME}' and its sub-folders:")
        #     for item in file_list:
        #         print(f"- Name: {item['name']}, Modified: {item['modifiedTime']}")
        # else:
        #     print(f"No files found inside folder '{FOLDER_NAME}'.")
        
        # print("\n" + "="*40)
        # download_files_from_folder(drive_service, FOLDER_NAME)
