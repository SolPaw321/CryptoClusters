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
BATCH_SIZE = 10 

# --- HYPERLIQUID API RATE LIMITS ---
WEIGHT_PER_SEC = 1200 / 60.0
WEIGHT_USER_ROLE = 60
WEIGHT_SUBACCOUNTS = 20

SLEEP_USER_ROLE = (WEIGHT_USER_ROLE / WEIGHT_PER_SEC) + 0.1    
SLEEP_SUBACCOUNTS = (WEIGHT_SUBACCOUNTS / WEIGHT_PER_SEC) + 0.1 

def safe_api_request(payload: dict) -> Any:
    attempt = 0
    while True:
        try:
            response = requests.post(API_URL, json=payload, timeout=15)
            if response.status_code == 429:
                print("\n[!] Rate limit hit. Pausing...")
                time.sleep(30)
                continue
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            attempt += 1
            sleep_time = min(60, 2 ** attempt)
            print(f"\n[-] Connection error with API: {e}")
            print(f"[-] API not available. Retrying in {sleep_time}s (Attempt #{attempt})")
            time.sleep(sleep_time)

def fetch_master_address(address: str) -> str:
    """Checks the role of the address via userRole endpoint."""
    payload = {"type": "userRole", "user": address}
    data = safe_api_request(payload)
    role = data.get("role")
    if role == "subAccount":
        return data.get("data", {}).get("master", address)
    return address

def fetch_subaccounts(master_address: str) -> List[Dict[str, Any]]:
    """Fetch subaccounts list for a given master address."""
    payload = {"type": "subAccounts", "user": master_address}
    data = safe_api_request(payload)
    return data if isinstance(data, list) else []

def load_scanned_masters() -> Set[str]:
    """Load the list of already processed master addresses from the cache file."""
    if not SCANNED_CACHE_FILE.exists():
        return set()
    with open(SCANNED_CACHE_FILE, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}

def load_existing_subaccounts() -> Set[str]:
    """Load already saved subaccounts from the main output file."""
    if not OUTPUT_CSV.exists():
        return set()
    try:
        df = pd.read_csv(OUTPUT_CSV)
        if 'subaccount_address' in df.columns:
            return set(df['subaccount_address'].dropna().tolist())
    except Exception as e:
        print(f"[-] Error loading existing subaccounts: {e}")
    return set()

def append_to_csv(file_path: Path, rows: List[Dict[str, Any]]) -> None:
    """Append new data to the target CSV."""
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
    print(f"[*] --- CONFIGURATION ---")
    print(f"[*] Input:  {INPUT_CSV}")
    print(f"[*] Output: {OUTPUT_CSV}")
    print(f"[*] Cache:  {SCANNED_CACHE_FILE}")
    print(f"[*] ---------------------------")

    if not INPUT_CSV.exists():
        print(f"Error: Input file {INPUT_CSV} does not exist!")
        return

    try:
        df_input = pd.read_csv(INPUT_CSV)
        if 'address' not in df_input.columns:
            print("Error: Input file must contain an 'address' column.")
            return
        all_addresses = df_input['address'].dropna().unique().tolist()
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    scanned_history = load_scanned_masters()
    existing_subaccounts = load_existing_subaccounts()
    
    pending_addresses = [
        addr for addr in all_addresses 
        if addr not in scanned_history and addr not in existing_subaccounts
    ]

    if not pending_addresses:
        print("\n[*] Everything is already up to date. No new addresses to scan.")
        return

    print(f"[*] Starting loop. Pending: {len(pending_addresses)}")
    print(f"[*] ---------------------------\n")
    
    accounts_batch = []
    scanned_batch = []
    new_count = 0

    for i, address in enumerate(pending_addresses, 1):
        subs = fetch_subaccounts(address)
        time.sleep(SLEEP_SUBACCOUNTS) 

        if subs:
            master = address
            new_in_this_round = 0
            
            for sub in subs:
                sub_addr = sub.get("subAccountUser")
                if sub_addr and sub_addr not in existing_subaccounts:
                    accounts_batch.append({
                        "master_address": master,
                        "subaccount_address": sub_addr,
                        "discovered_at": pd.Timestamp.now(tz='UTC').isoformat(),
                        "subaccount_name": sub.get("name", "Unnamed")
                    })
                    existing_subaccounts.add(sub_addr)
                    new_in_this_round += 1
            
            scanned_batch.append(address)
            
            print(f"[{i}/{len(pending_addresses)}] [MASTER] {master} -> Found {new_in_this_round} new subaccounts.")

        else:
            real_master = fetch_master_address(address)
            time.sleep(SLEEP_USER_ROLE)
            scanned_batch.append(address)

            if real_master != address:
                if real_master not in scanned_history and real_master not in scanned_batch:
                    master_subs = fetch_subaccounts(real_master)
                    time.sleep(SLEEP_SUBACCOUNTS)
                    
                    new_in_this_round = 0
                    
                    for sub in master_subs:
                        sub_addr = sub.get("subAccountUser")
                        if sub_addr and sub_addr not in existing_subaccounts:
                            accounts_batch.append({
                                "master_address": real_master,
                                "subaccount_address": sub_addr,
                                "discovered_at": pd.Timestamp.now(tz='UTC').isoformat(),
                                "subaccount_name": sub.get("name", "Unnamed")
                            })
                            existing_subaccounts.add(sub_addr)
                            new_in_this_round += 1
                    
                    scanned_batch.append(real_master)
                    
                    print(f"[{i}/{len(pending_addresses)}] [SUBACCOUNT] Resolved {address[:8]}... -> Discovered New Master: {real_master} -> Found {new_in_this_round} new subaccounts.")
                else:
                    print(f"[{i}/{len(pending_addresses)}] [SUBACCOUNT] Resolved {address[:8]}... -> Master {real_master} already scanned.")
            else:
                print(f"[{i}/{len(pending_addresses)}] [LONELY MASTER] {address} -> Found 0 subaccounts.")
        
        if len(scanned_batch) >= BATCH_SIZE or i == len(pending_addresses):
            append_to_csv(OUTPUT_CSV, accounts_batch)
            append_to_cache(SCANNED_CACHE_FILE, scanned_batch)
            
            scanned_history.update(scanned_batch) 
            new_count += len(accounts_batch)
            
            accounts_batch.clear()
            scanned_batch.clear()

    print(f"\n[+] Done. Added a total of {new_count} new entries to {OUTPUT_CSV.name}")

if __name__ == "__main__":
    main()