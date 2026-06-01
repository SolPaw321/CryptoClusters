"""
File Cache and Backup module.

Handles reading and writing to local CSV and TXT files for state persistence
and duplicate prevention.
"""
import csv
import pandas as pd
from pathlib import Path
from typing import Set, List, Dict, Any

from src.paths import PATHS

OUTPUT_CSV = PATHS.ADDRESSES / "only_subaccounts.csv"
SCANNED_CACHE_FILE = PATHS.ADDRESSES_TEMP / "scanned_masters.txt"


def load_scanned_masters() -> Set[str]:
    """
    Load already processed master addresses from cache file.

    Reads the local text file containing addresses that have already passed
    through the Radar phase to prevent redundant API calls.

    :return: set of scanned master addresses
    """
    if not SCANNED_CACHE_FILE.exists():
        return set()
    with open(SCANNED_CACHE_FILE, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def load_existing_subaccounts() -> Set[str]:
    """
    Load already saved subaccounts from the CSV backup.

    Reads the local CSV file to populate the set of known subaccounts,
    ensuring they are not processed or saved multiple times.

    :return: set of existing subaccount addresses
    """
    if not OUTPUT_CSV.exists():
        return set()
    try:
        df = pd.read_csv(OUTPUT_CSV)
        if 'subaccount_address' in df.columns:
            return set(df['subaccount_address'].dropna().tolist())
    except Exception as e:
        print(f"[-] Error loading existing subaccounts: {e}")
    return set()

def append_to_csv(rows: List[Dict[str, Any]]) -> None:
    """
    Backup discovered subaccount relations to a CSV file.

    Appends new rows to the output CSV. If the file does not exist,
    it automatically writes the header first.

    :param rows: list of dictionaries representing subaccount data
    """
    if not rows:
        return
    file_exists = OUTPUT_CSV.exists()
    fieldnames = ["master_address", "subaccount_address", "discovered_at", "subaccount_name"]
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)


def append_to_cache(addresses: List[str]) -> None:
    """
    Backup processed master addresses to a TXT file.

    Appends scanned addresses to the local cache, marking them as completed
    for future script executions.

    :param addresses: list of master addresses
    """
    if not addresses:
        return
    with open(SCANNED_CACHE_FILE, "a", encoding="utf-8") as f:
        for address in addresses:
            f.write(f"{address}\n")