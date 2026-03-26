from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC = PROJECT_ROOT / "src"

DATA = PROJECT_ROOT / "data"
DATA.mkdir(parents=True, exist_ok=True)
ADDRESSES = DATA / "addresses"
ADDRESSES.mkdir(parents=True, exist_ok=True)
ADDRESSES_TEMP = ADDRESSES / "temp"
ADDRESSES_TEMP.mkdir(parents=True, exist_ok=True)
