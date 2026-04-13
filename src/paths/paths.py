from pathlib import Path
from typing import ClassVar


class Paths:
    """
    Singleton class responsible for storing and initializing project paths.

    This class provides a single shared instance that exposes commonly used
    directories in the project, such as the project root, source directory,
    and data-related folders.

    During the first initialization, all paths stored in ``__ALL_PATHS`` are
    created automatically if they do not already exist.

    Attributes:
        PROJECT_ROOT (Path): Absolute path to the project root directory.
        SRC (Path): Path to the main source code directory.
        DATA (Path): Path to the top-level data directory.
        ADDRESSES (Path): Path to the addresses data directory.
        ADDRESSES_TEMP (Path): Path to the temporary addresses directory.
    """
    _instance: ClassVar["Paths | None"] = None
    _initialized: ClassVar[bool] = False

    def __init__(self) -> None:
        """
        Initialize all project paths only once.
        """
        if self.__class__._initialized:
            return

        self.PROJECT_ROOT = Path(__file__).resolve().parents[2]
        self.SRC = self.PROJECT_ROOT / "src"

        self.DATA = self.PROJECT_ROOT / "data"
        self.ADDRESSES = self.DATA / "addresses"
        self.ADDRESSES_TEMP = self.ADDRESSES / "temp"

        self.__ALL_PATHS = (
            self.PROJECT_ROOT,
            self.SRC,
            self.DATA,
            self.ADDRESSES,
            self.ADDRESSES_TEMP,
        )

        self._ensure_directories()
        self.__class__._initialized = True

    def __new__(cls) -> "Paths":
        """
        Create or return the single instance of the class.

        :return: The singleton instance of the class.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def _ensure_directories(self) -> None:
        """
        Create all directories stored in :attr:`__ALL_PATHS`.
        """
        for path in self.__ALL_PATHS:
            path.mkdir(parents=True, exist_ok=True)
