import pandas as pd
import requests
import time
import csv
from pathlib import Path
from typing import List, Dict, Any, Set

from src.paths import PATHS

# --- PATH CONFIGURATION ---
INPUT_CSV = PATHS.ADDRESSES / "unique_wallets.csv"
OUTPUT_CSV = PATHS.ADDRESSES / "only_subaccounts.csv"
SCANNED_CACHE_FILE = PATHS.ADDRESSES_TEMP / "scanned_masters.txt"

API_URL = "https://api.hyperliquid.xyz/info"
BATCH_SIZE = 50 

WEIGHT_PER_SEC = 1200 / 60.0

WEIGHT_USER_ROLE = 60
WEIGHT_SUBACCOUNTS = 20

SLEEP_USER_ROLE = (WEIGHT_USER_ROLE / WEIGHT_PER_SEC) + 0.1
SLEEP_SUBACCOUNTS = (WEIGHT_SUBACCOUNTS / WEIGHT_PER_SEC) + 0.1


def fetch_master_address(address: str) -> str:
    """
    Checks the role of the address. If it's a subaccount or agent, returns the master account.
    Otherwise, returns the original address. (Cost: 60 weight)
    """
    payload = {"type": "userRole", "user": address}
    try:
        response = requests.post(API_URL, json=payload, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        role = data.get("role")
        if role == "subAccount":
            return data.get("data", {}).get("master", address)
        elif role == "agent":
            return data.get("data", {}).get("user", address)
        return address
    except Exception as e:
        print(f"[-] Error fetching role for {address}: {e}")
        return address

def fetch_subaccounts(master_address: str) -> List[Dict[str, Any]]:
    """
    Fetch subaccounts from Hyperliquid API for a given master address. (Cost: 20 weight)
    """
    payload = {"type": "subAccounts", "user": master_address}
    try:
        response = requests.post(API_URL, json=payload, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if isinstance(data, list):
            return data
        return []
    except Exception as e:
        print(f"[-] Error fetching subaccounts for {master_address}: {e}")
        return []

def load_scanned_masters() -> Set[str]:
    """Load the list of already processed master addresses from the cache file."""
    if not SCANNED_CACHE_FILE.exists():
        return set()
    with open(SCANNED_CACHE_FILE, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}

def append_to_csv(file_path: Path, rows: List[Dict[str, Any]]) -> None:
    """Append new subaccounts to the CSV file."""
    if not rows:
        return
    file_exists = file_path.exists()
    fieldnames = ["master_address", "subaccount_address", "discovered_at", "subaccount_name"]
    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

def append_to_cache(file_path: Path, addresses: List[str]) -> None:
    """Append processed master addresses to the cache file."""
    with open(file_path, "a", encoding="utf-8") as f:
        for address in addresses:
            f.write(f"{address}\n")

def main() -> None:
    if not INPUT_CSV.exists():
        print(f"Error: Input file {INPUT_CSV} does not exist!")
        return

    print(f"[*] Loading addresses from {INPUT_CSV}...")
    try:
        df_input = pd.read_csv(INPUT_CSV)
        if 'address' not in df_input.columns:
            print("Error: Input file must contain an 'address' column.")
            return
        all_addresses = df_input['address'].dropna().unique().tolist()
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    # Load history to resume where we left off
    scanned_history = load_scanned_masters()
    pending_addresses = [addr for addr in all_addresses if addr not in scanned_history]

    total_pending = len(pending_addresses)
    
    print(f"[*] Total unique addresses in input: {len(all_addresses)}")
    print(f"[*] Already completely scanned: {len(scanned_history)}")
    print(f"[*] Remaining to scan: {total_pending}")

    if total_pending == 0:
        print("\n[*] All addresses have already been processed. Nothing to do!")
        return

    print(f"[*] Starting fetch loop (Batch size: {BATCH_SIZE})...")
    print(f"[*] Rate Limit Strategy: Sleeping ~{SLEEP_USER_ROLE:.1f}s after UserRole, ~{SLEEP_SUBACCOUNTS:.1f}s after SubAccounts.")
    
    subaccounts_batch = []
    scanned_batch = []
    new_subaccounts_count = 0

    for i, address in enumerate(pending_addresses, 1):
        if i % 10 == 0 or i == total_pending:
            print(f"    Progress: {i}/{total_pending} ({(i/total_pending)*100:.1f}%)")

        master = fetch_master_address(address)
        time.sleep(SLEEP_USER_ROLE) 

        if master in scanned_history or master in scanned_batch:
            scanned_batch.append(address) # Mark original address as handled
            continue

        subs = fetch_subaccounts(master)
        
        time.sleep(SLEEP_SUBACCOUNTS) 
        
        for sub in subs:
            sub_address = sub.get("subAccountUser")
            sub_name = sub.get("name", "Unnamed")
            
            if sub_address:
                subaccounts_batch.append({
                    "master_address": master,
                    "subaccount_address": sub_address,
                    "discovered_at": pd.Timestamp.now(tz='UTC').isoformat(),
                    "subaccount_name": sub_name
                })
        
        scanned_batch.append(address)
        if master != address:
            scanned_batch.append(master)
        
        if len(scanned_batch) >= BATCH_SIZE or i == total_pending:
            append_to_csv(OUTPUT_CSV, subaccounts_batch)
            append_to_cache(SCANNED_CACHE_FILE, scanned_batch)
            
            scanned_history.update(scanned_batch) 
            
            new_subaccounts_count += len(subaccounts_batch)
            
            subaccounts_batch.clear()
            scanned_batch.clear()

    print(f"\n[+] Finished successfully! Discovered {new_subaccounts_count} subaccounts in this run.")

if __name__ == "__main__":
    main()