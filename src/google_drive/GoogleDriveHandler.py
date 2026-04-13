from __future__ import annotations

import os
import pickle
from typing import Optional
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.auth.credentials import TokenState
from google.auth.exceptions import RefreshError
from pathlib import Path

from .folders_ids import GdPaths, SCOPES


class GoogleDriveHandler:
    NAME = "GoogleDrive"
    """
    Google Drive Handler.

    Tool for uploading and downloading files from/to Google Drive.
    """
    def __init__(self):
        #self.SCOPES = SCOPES

        __GOOGLE_DRIVE = Path(__file__).resolve().parent
        __GOOGLE_DRIVE_SECRETS = __GOOGLE_DRIVE / "secrets"
        __GOOGLE_DRIVE.mkdir(parents=True, exist_ok=True)
        __GOOGLE_DRIVE_SECRETS.mkdir(parents=True, exist_ok=True)
        self.TOKEN = os.path.join(__GOOGLE_DRIVE_SECRETS, "token.pickle")
        self.CLIENT_SECRETS = os.path.join(__GOOGLE_DRIVE_SECRETS, "client_secrets.json")

        self.creds = None
        self.service = None
        self.__authenticate()

    def __authenticate(self) -> None:
        """
        Authenticate user and build Google Drive service.

        Load credentials from local token file if it exists.
        If credentials are missing or invalid:
          - refresh them when possible
          - otherwise run OAuth flow again

        After successful authentication:
          - save credentials to token file
          - assign credentials to self.creds
          - build Google Drive API service and assign it to self.service
        """
        creds = None
        if os.path.exists(self.TOKEN):
            with open(self.TOKEN, "rb") as token:
                creds = pickle.load(token)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.CLIENT_SECRETS,
                    SCOPES
                )
                creds = flow.run_local_server(port=0)

            with open(self.TOKEN, "wb") as token:
                pickle.dump(creds, token)

        self.creds = creds
        self.service = build("drive", "v3", credentials=creds)

    def __ensure_credentials(self) -> None:
        """
        Ensure that current credentials are available and usable.

        Behavior:
          - if credentials are missing, authenticate again
          - if access token is invalid and refresh token exists, refresh credentials
          - if credentials cannot be refreshed, authenticate again

        Also save refreshed credentials back to local token file.
        """
        if self.creds is None:
            self.__authenticate()
            return

        if self.creds.token_state == TokenState.INVALID:
            if self.creds.refresh_token:
                self.creds.refresh(Request())
                with open(self.TOKEN, "wb") as token:
                    pickle.dump(self.creds, token)
            else:
                self.__authenticate()

    def __execute(self, request_builder):
        """
        Execute Google API request safely.

        Before execution:
          - ensure that credentials are valid

        If refresh token has expired or has been revoked:
          - remove local token file
          - authenticate again
          - rebuild service
          - retry request once

        :param request_builder:
            callable returning a fresh Google API request object
            Example:
                lambda: self.service.files().list(
                    q=query,
                    fields="files(id, name)"
                )

        :return:
            result of request.execute()

        :raises RefreshError:
            when credentials refresh fails for a reason other than invalid_grant
        :raises Exception:
            any exception raised by request.execute() that is not handled here
        """
        self.__ensure_credentials()

        try:
            return request_builder().execute()

        except (RefreshError, ConnectionResetError) as e:
            if "invalid_grant" in str(e):
                print(f"[{self.NAME}] Refresh token expired or revoked. Reauthenticating...")

                if os.path.exists(self.TOKEN):
                    os.remove(self.TOKEN)

                self.creds = None
                self.service = None
                self.__authenticate()

                return request_builder().execute()
            raise

    def __find_folder(self, folder_name: str, parent_folder_id: str) -> Optional[str]:
        """
        Find a folder on Google Drive.

        :param folder_name: folder name
        :param parent_folder_id: parent folder id
        :return: found folder id (bytes) or None
        """
        query = (
            "mimeType='application/vnd.google-apps.folder' "
            f"and name='{folder_name}' "
            f"and '{parent_folder_id}' in parents "
            "and trashed = false"
        )

        results = self.__execute(
            lambda: self.service.files().list(
                q=query,
                fields="files(id, name)"
            )
        )
        folders = results.get("files", [])
        return folders[0]["id"] if folders else None

    def __create_or_get_folder(self, folder_name:str, parent_folder_id: str) -> Optional[str]:
        """
        If given folder exists, then return its id.
        If not, create a new folder and return its id.

        :param folder_name: folder name
        :param parent_folder_id: parent folder id
        :return: created or got folder id (bytes), or None
        """
        folder_id = self.__find_folder(folder_name, parent_folder_id)

        if folder_id:
            print(f"[{self.NAME}] Folder '{folder_name}' is existing.")
            return folder_id

        file_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id]
        }
        folder = self.__execute(
            lambda: self.service.files().create(
                body=file_metadata,
                fields="id"
            )
        )
        print(f"[{self.NAME}] New folder created '{folder_name}' on Google Drive.")
        return folder.get("id")

    def __find_file(self, file_name: str, parent_folder_id: str) -> Optional[str]:
        """
        Find a file on Google Drive.

        :param file_name: file name to be found
        :param parent_folder_id: parent folder id
        :return: found file id (bytes) or None
        """
        query = (
            f"name='{file_name}' "
            f"and '{parent_folder_id}' in parents "
            "and trashed = false"
        )

        results = self.__execute(
            lambda: self.service.files().list(
                q=query,
                fields="files(id, name)"
            )
        )
        files = results.get("files", [])
        return files[0]["id"] if files else None

    def __upload_or_update_file(self, file_path, parent_folder_id: str, *, rewrite: bool = True) -> None:
        """
        Upload or update a file.

        If file exists and rewrite=True, then update file.
        If file exists and rewrite=File, then skip upload on this file.
        Otherwise, upload a new file.

        :param file_path: local file path
        :param parent_folder_id: parent folder id
        :param rewrite: true or false
        """
        file_name = os.path.basename(file_path)
        file_id = self.__find_file(file_name, parent_folder_id)

        file_metadata = {
            "name": file_name,
            "parents": [parent_folder_id]
        }

        if file_id and rewrite:
            print(f"[{self.NAME}] File '{file_name}' is existing. Updating file.")
            self.__execute(
                lambda: self.service.files().update(
                    fileId=file_id,
                    media_body=MediaFileUpload(file_path, resumable=True)
                )
            )
        elif file_id and not rewrite:
            print(f"[{self.NAME}] File '{file_name}' is existing. NOT updating file.")
        else:
            print(f"[{self.NAME}] File '{file_name}' is not existing. Uploading file.")
            self.__execute(
                lambda: self.service.files().create(
                    body=file_metadata,
                    media_body=MediaFileUpload(file_path, resumable=True),
                    fields="id"
                )
            )

    def __list_folder_items(self, folder_id: str):
        """
        Returns list of files in given folder.

        :param folder_id: folder id
        """
        query = f"'{folder_id}' in parents and trashed = false"

        results = self.__execute(
            lambda: self.service.files().list(
                q=query,
                fields="files(id, name, mimeType)",
                pageSize=1000
            )
        )

        return results.get("files", [])

    def __download_file(self, file_id: str, file_name: str, local_parent_path: str, *, rewrite:bool=True) -> None:
        """
        Download a file from Google Drive.

        If local file exists and rewrite=True, then rewrite local file.
        If local file exists and rewrite=False, then skip downloading.

        :param file_id: file id
        :param file_name: file name
        :param local_parent_path: local file path
        :param rewrite: true or false
        """
        os.makedirs(local_parent_path, exist_ok=True)
        local_file_path = os.path.join(local_parent_path, file_name)

        if os.path.exists(local_file_path):
            if not rewrite:
                print(f"[{self.NAME}] File already exists, skipping: {local_file_path}")
                return
            else:
                print(f"[{self.NAME}] File already exists, rewriting: {local_file_path}")

        request = self.service.files().get_media(fileId=file_id)

        with open(local_file_path, "wb") as f:
            downloader = MediaIoBaseDownload(f, request)
            done = False

            while not done:
                status, done = downloader.next_chunk()
                if status:
                    print(f"[{self.NAME}] Downloading '{file_name}': {int(status.progress() * 100)}%")

        print(f"[{self.NAME}] Downloaded file: {local_file_path}")

    def __download_folder_recursive(self, folder_id: str, local_current_path: str | Path, *, rewrite:bool=True) -> None:
        """
        Download all files in given folder recursively.

        :param folder_id: folder id
        :param local_current_path: local folder path
        :param rewrite: true or false
        """
        os.makedirs(local_current_path, exist_ok=True)

        items = self.__list_folder_items(folder_id)

        for item in items:
            item_id = item["id"]
            item_name = item["name"]
            mime_type = item["mimeType"]

            if mime_type == "application/vnd.google-apps.folder":
                subfolder_path = os.path.join(local_current_path, item_name)
                print(f"[{self.NAME}] Entering folder: {item_name}")
                self.__download_folder_recursive(
                    item_id,
                    subfolder_path,
                    rewrite=rewrite
                )
            else:
                self.__download_file(
                    item_id,
                    item_name,
                    local_current_path,
                    rewrite=rewrite
                )

    def upload_folder(self, *, google_drive_folder_id:str=None, local_folder_path:str|Path=None, rewrite:bool=True) -> None:
        """
        API for uploading a file form local to Google Drive.

        :param google_drive_folder_id: folder id to upload
        :param local_folder_path: uploading local folder
        :param rewrite: true or false
        """
        parent_folder_id = google_drive_folder_id or GdPaths.SINK

        for root, dirs, files in os.walk(local_folder_path):
            dirs[:] = [d for d in dirs if "temp" not in d.lower()]

            rel_path = os.path.relpath(root, local_folder_path)
            rel_parts = rel_path.split(os.sep) if rel_path != "." else []
            current_parent = parent_folder_id

            for part in rel_parts:
                current_parent = self.__create_or_get_folder(part, current_parent)
            for file_name in files:
                file_path = os.path.join(root, file_name)
                self.__upload_or_update_file(
                    file_path,
                    current_parent,
                    rewrite=rewrite
                )

    def upload_file(self, local_file_path:str, *, google_drive_folder_id:str=None, rewrite:bool=True) -> None:
        """
        Upload a single file from local to Google Drive.

        :param local_file_path: local file path
        :param google_drive_folder_id: folder id to upload
        :param rewrite: true or false
        """
        parent_folder_id = google_drive_folder_id or GdPaths.SINK

        if not os.path.isfile(local_file_path):
            raise FileNotFoundError(
                f"[{self.NAME}] Local file {local_file_path} not existing."
            )

        self.__upload_or_update_file(
            local_file_path,
            parent_folder_id,
            rewrite=rewrite
        )

    def download_folder(self, *, google_drive_folder_id:str=None, local_folder_path:str=None, rewrite:bool=True) -> None:
        """
        Download a folder from Google Drive to local.

        :param google_drive_folder_id: folder id
        :param local_folder_path: local folder path
        :param rewrite: true or false
        """
        folder_id = google_drive_folder_id or GdPaths.SINK
        self.__download_folder_recursive(
            folder_id,
            local_folder_path,
            rewrite=rewrite
        )

    def download_file(self, file_name:str, *, local_folder_path:str=None, google_drive_folder_id:str=None, rewrite:bool=True) -> None:
        """
        Download a single file from Google Drive to local.

        :param file_name: file name
        :param local_folder_path: local file path
        :param google_drive_folder_id: folder id
        :param rewrite: true or false
        """
        parent_folder_id = google_drive_folder_id or GdPaths.CRYPTO_CLUSTERS
        local_folder_path = local_folder_path or "."

        file_id = self.__find_file(file_name, parent_folder_id)

        if file_id is None:
            raise FileNotFoundError(
                f"[{self.NAME}] File '{file_name}' not found in Google Drive folder '{parent_folder_id}'."
            )

        self.__download_file(
            file_id,
            file_name,
            local_folder_path,
            rewrite=rewrite
        )