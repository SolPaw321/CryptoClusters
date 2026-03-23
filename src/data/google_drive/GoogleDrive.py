import os
import pickle
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

from src.data.google_drive.secrets.google_drive import *
from src.data.google_drive.secrets.PATHS import *


class GoogleDriveUpload:
    def __init__(self, local_folder_path, google_drive_folder_id=None):
        self.SCOPES = SCOPES
        self.service = self.__authenticate()

        self.google_drive_folder_id = google_drive_folder_id or SINK_FOLDER_ID
        self.local_folder_path = local_folder_path


    def __authenticate(self):
        creds = None

        TOKEN = os.path.join(GOOGLE_DRIVE,'token.pickle')
        CLIENT_SECRETS = os.path.join(GOOGLE_DRIVE, 'client_secrets.json')

        if os.path.exists(TOKEN):
            with open(TOKEN, 'rb') as token:
                creds = pickle.load(token)
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS, self.SCOPES)
                creds = flow.run_local_server(port=0)
            with open(TOKEN, 'wb') as token:
                pickle.dump(creds, token)
        return build('drive', 'v3', credentials=creds)

    def __find_folder(self, folder_name, parent_folder_id):
        query = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and '{parent_folder_id}' in parents and trashed = false"
        results = self.service.files().list(q=query, fields="files(id, name)").execute()
        folders = results.get('files', [])
        if folders:
            return folders[0]['id']
        else:
            return None

    def __create_or_get_folder(self, folder_name, parent_folder_id):
        folder_id = self.__find_folder(folder_name, parent_folder_id)
        if folder_id:
            print(f"Folder '{folder_name}' is existing.")
            return folder_id
        else:
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_folder_id]
            }
            folder = self.service.files().create(body=file_metadata, fields='id').execute()
            print(f"New folder created '{folder_name}' on Google Drive.")
            return folder.get('id')

    def __find_file(self, file_name, parent_folder_id):
        query = f"name='{file_name}' and '{parent_folder_id}' in parents and trashed = false"
        results = self.service.files().list(q=query, fields="files(id, name)").execute()
        files = results.get('files', [])
        if files:
            return files[0]['id']
        else:
            return None

    def __upload_or_update_file(self, file_path, parent_folder_id, *, rewrite=True):
        file_name = os.path.basename(file_path)
        file_id = self.__find_file(file_name, parent_folder_id)

        file_metadata = {
            'name': file_name,
            'parents': [parent_folder_id]
        }
        media = MediaFileUpload(file_path, resumable=True)

        if file_id and rewrite:
            print(f"File '{file_name}' is existing. Updating file.")
            self.service.files().update(fileId=file_id, media_body=media).execute()
        elif file_id and not rewrite:
            print(f"File '{file_name}' is existing. NOT updating file.")
        else:
            print(f"File '{file_name}' is not existing. Uploading file.")
            self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()

    def upload_folder(self, *, rewrite=True):
        for root, dirs, files in os.walk(self.local_folder_path):
            rel_path = os.path.relpath(root, self.local_folder_path)
            rel_parts = rel_path.split(os.sep) if rel_path != '.' else []
            current_parent = self.google_drive_folder_id

            for part in rel_parts:
                current_parent = self.__create_or_get_folder(part, current_parent)
            for file_name in files:
                file_path = os.path.join(root, file_name)
                self.__upload_or_update_file(file_path, current_parent, rewrite=rewrite)



class GoogleDriveDownload:
    def __init__(self, local_folder_path, google_drive_folder_id=None):
        self.SCOPES = SCOPES
        self.local_folder_path = local_folder_path
        self.google_drive_folder_id = google_drive_folder_id or CRYPTO_CLUSTERS_FOLDER_ID

        self.service = self.__authenticate()

    def __authenticate(self):
        creds = None

        TOKEN = os.path.join(GOOGLE_DRIVE, "token.pickle")
        CLIENT_SECRETS = os.path.join(GOOGLE_DRIVE, "client_secrets.json")

        if os.path.exists(TOKEN):
            with open(TOKEN, "rb") as token:
                creds = pickle.load(token)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    CLIENT_SECRETS,
                    self.SCOPES
                )
                creds = flow.run_local_server(port=0)

            with open(TOKEN, "wb") as token:
                pickle.dump(creds, token)

        return build("drive", "v3", credentials=creds)

    def __list_folder_items(self, folder_id):
        query = f"'{folder_id}' in parents and trashed = false"
        results = self.service.files().list(
            q=query,
            fields="files(id, name, mimeType)",
            pageSize=1000
        ).execute()
        return results.get("files", [])

    def __download_file(self, file_id, file_name, local_parent_path, *, rewrite=True):
        os.makedirs(local_parent_path, exist_ok=True)
        local_file_path = os.path.join(local_parent_path, file_name)

        if os.path.exists(local_file_path):
            if not rewrite:
                print(f"File already exists, skipping: {local_file_path}")
                return
            else:
                print(f"File already exists, rewriting: {local_file_path}")

        request = self.service.files().get_media(fileId=file_id)

        with open(local_file_path, "wb") as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False

            while not done:
                status, done = downloader.next_chunk()
                if status:
                    print(f"Downloading '{file_name}': {int(status.progress() * 100)}%")

        print(f"Downloaded file: {local_file_path}")

    def __download_folder_recursive(self, folder_id, local_current_path, *, rewrite=True):
        os.makedirs(local_current_path, exist_ok=True)

        items = self.__list_folder_items(folder_id)

        for item in items:
            item_id = item["id"]
            item_name = item["name"]
            mime_type = item["mimeType"]

            if mime_type == "application/vnd.google-apps.folder":
                subfolder_path = os.path.join(local_current_path, item_name)
                print(f"Entering folder: {item_name}")
                self.__download_folder_recursive(item_id, subfolder_path)
            else:
                self.__download_file(item_id, item_name, local_current_path, rewrite=rewrite)

    def download_folder(self, *, rewrite=True):
        self.__download_folder_recursive(
            self.google_drive_folder_id,
            self.local_folder_path,
            rewrite=rewrite
        )