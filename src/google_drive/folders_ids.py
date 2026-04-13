from typing import  ClassVar


SCOPES = ['https://www.googleapis.com/auth/drive']


class GoogleDriveFolderIDs:

    _instance: ClassVar["GoogleDriveFolderIDs | None"] = None
    _initialized: ClassVar[bool] = False

    def __init__(self) -> None:
        if self.__class__._initialized:
            return

        # root
        self.CRYPTO_CLUSTERS = '11jBUISmCDUP3nrGm1zKW2AryCnPwWGZv'
        # root/...
        self.ADDRESSES = '1A38QjfXBWWQsOK8wKxCq3learcqLEh1-'
        # root/...
        self.SINK = '1odWNnT9Xq0GpdqQ6zvYn5AooaMqcGoT0'

        self.__class__._initialized = True

    def __new__(cls) -> "GoogleDriveFolderIDs":
        """
        Create or return the single instance of the class.

        :return: The singleton instance of the class.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

GdPaths = GoogleDriveFolderIDs()