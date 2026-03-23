from pathlib import Path

__current_folder = Path(__file__).resolve().parent

CRYPTO_CLUSTERS = __current_folder.parent.parent

SRC = CRYPTO_CLUSTERS / "src"

DATA = SRC / "data"
ADDRESSES = DATA / "addresses"
ADDRESSES_STORE =  ADDRESSES / "store"