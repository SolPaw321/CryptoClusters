from src.data.google_drive.GoogleDrive import GoogleDriveUpload, GoogleDriveDownload
from src.paths import ADDRESSES_STORE
from src.data.google_drive.secrets.google_drive import *

if __name__ == "__main__":
    uploader = GoogleDriveUpload(local_folder_path=ADDRESSES_STORE)
    uploader.upload_folder()

    downloader = GoogleDriveDownload(local_folder_path="down", google_drive_folder_id=SINK_FOLDER_ID)
    downloader.download_folder()