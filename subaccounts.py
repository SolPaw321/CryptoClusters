import os
import pandas as pd
import requests
import time
import csv
import psycopg
from pathlib import Path
from typing import List, Dict, Any, Set
from dotenv import load_dotenv

from src.paths import PATHS

# --- PATH CONFIGURATION ---
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

# --- DB CONFIGURATION ---
load_dotenv(dotenv_path=PATHS.ENV)

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DB_PORT", 5432))
}

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

def fetch_pending_batch(conn, batch_size: int) -> List[str]:
    """Fetch batch addresses and change their status to PROCESSING."""
    query = """
        UPDATE wallet_processing_queue
        SET status = 'PROCESSING'
        WHERE wallet_address IN (
            SELECT wallet_address
            FROM wallet_processing_queue
            WHERE status = 'PENDING'
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        RETURNING wallet_address;
    """
    with conn.cursor() as cur:
        cur.execute(query, (batch_size,))
        addresses = [row[0] for row in cur.fetchall()]
    
    conn.commit()
    return addresses

def mark_batch_as_done(conn, addresses: List[str]) -> None:
    """Changes processed adresses status to DONE."""
    if not addresses:
        return
    query = """
        UPDATE wallet_processing_queue
        SET status = 'DONE'
        WHERE wallet_address = %s;
    """
    with conn.cursor() as cur:
        cur.executemany(query, [(addr,) for addr in addresses])

def revert_batch_to_pending(conn, addresses: List[str]) -> None:
    """Reverts status of unprocessed adresses from PROCESSING to PENDING."""
    if not addresses:
        return
    query = """
        UPDATE wallet_processing_queue
        SET status = 'PENDING'
        WHERE wallet_address = %s;
    """
    with conn.cursor() as cur:
        cur.executemany(query, [(addr,) for addr in addresses])

def main() -> None:
    print(f"[*] --- CONFIGURATION ---")
    print(f"[*] Output CSV Backup: {OUTPUT_CSV}")
    print(f"[*] Cache TXT Backup:  {SCANNED_CACHE_FILE}")
    print(f"[*] Connecting to DB:  '{DB_CONFIG['dbname']}'...")
    print(f"[*] ---------------------------\n")

    scanned_history = load_scanned_masters()
    existing_subaccounts = load_existing_subaccounts()

    with psycopg.connect(**DB_CONFIG) as conn:
        print("[*] Connected to DB. Starting worker...\n")
        new_count = 0

        try:
            while True:
                batch_addresses = fetch_pending_batch(conn, BATCH_SIZE)
                
                if not batch_addresses:
                    print("\n[*] No PENDING adresses in database.")
                    break
                
                print(f"\n[!] Reserved {len(batch_addresses)} adresses. Status changed to PROCESSING.")
                
                accounts_batch = []   
                scanned_batch = []    
                db_masters_batch = []    
                db_subs_batch = []       

                for i, address in enumerate(batch_addresses, 1):
                    subs = fetch_subaccounts(address)
                    time.sleep(SLEEP_SUBACCOUNTS) 

                    if subs:
                        master = address
                        db_masters_batch.append((master,))
                        new_in_this_round = 0
                        
                        for sub in subs:
                            sub_addr = sub.get("subAccountUser")
                            if sub_addr and sub_addr not in existing_subaccounts:
                                sub_name = sub.get("name", "Unnamed")
                                
                                accounts_batch.append({
                                    "master_address": master,
                                    "subaccount_address": sub_addr,
                                    "discovered_at": pd.Timestamp.now(tz='UTC').isoformat(),
                                    "subaccount_name": sub_name
                                })
                                db_subs_batch.append((sub_addr, master, sub_name))
                                existing_subaccounts.add(sub_addr)
                                new_in_this_round += 1
                        
                        scanned_batch.append(address)
                        print(f"[{i}/{len(batch_addresses)}] [MASTER] {master} -> Found {new_in_this_round} new subaccounts.")

                    else:
                        real_master = fetch_master_address(address)
                        time.sleep(SLEEP_USER_ROLE)
                        
                        scanned_batch.append(address)
                        db_masters_batch.append((address,)) 

                        if real_master != address:
                            if real_master not in scanned_history and real_master not in scanned_batch:
                                master_subs = fetch_subaccounts(real_master)
                                time.sleep(SLEEP_SUBACCOUNTS)
                                
                                db_masters_batch.append((real_master,))
                                new_in_this_round = 0
                                
                                for sub in master_subs:
                                    sub_addr = sub.get("subAccountUser")
                                    if sub_addr and sub_addr not in existing_subaccounts:
                                        sub_name = sub.get("name", "Unnamed")
                                        
                                        accounts_batch.append({
                                            "master_address": real_master,
                                            "subaccount_address": sub_addr,
                                            "discovered_at": pd.Timestamp.now(tz='UTC').isoformat(),
                                            "subaccount_name": sub_name
                                        })
                                        db_subs_batch.append((sub_addr, real_master, sub_name))
                                        existing_subaccounts.add(sub_addr)
                                        new_in_this_round += 1
                                
                                scanned_batch.append(real_master)
                                print(f"[{i}/{len(batch_addresses)}] [SUBACCOUNT] Resolved {address[:8]}... -> Discovered New Master: {real_master} -> Found {new_in_this_round} new subaccounts.")
                        else:
                            print(f"[{i}/{len(batch_addresses)}] [LONELY MASTER] {address} -> Found 0 subaccounts.")

                append_to_csv(OUTPUT_CSV, accounts_batch)
                append_to_cache(SCANNED_CACHE_FILE, scanned_batch)
                scanned_history.update(scanned_batch)
                
                with conn.cursor() as cur:
                    if db_masters_batch:
                        cur.executemany("""
                            INSERT INTO wallets (address)
                            VALUES (%s)
                            ON CONFLICT (address) DO NOTHING;
                        """, db_masters_batch)
                    
                    if db_subs_batch:
                        cur.executemany("""
                            INSERT INTO wallets (address, master_address, subaccount_name)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (address) DO UPDATE SET
                                master_address = EXCLUDED.master_address,
                                subaccount_name = EXCLUDED.subaccount_name;
                        """, db_subs_batch)

                mark_batch_as_done(conn, batch_addresses)
                
                conn.commit()
                new_count += len(accounts_batch)
                print(f"[+] Successfully flushed data from {len(batch_addresses)} addresses to files and DB.")

        except KeyboardInterrupt:
            print("\n[!] Program interrupted manually.")
            print("[!] Performing shutdown and DB rollback...")
            
            processed_addresses = scanned_batch 
            unprocessed_addresses = [addr for addr in batch_addresses if addr not in scanned_batch]
            
            append_to_csv(OUTPUT_CSV, accounts_batch)
            append_to_cache(SCANNED_CACHE_FILE, scanned_batch)
            scanned_history.update(scanned_batch)
            
            with conn.cursor() as cur:
                if db_masters_batch:
                    cur.executemany("INSERT INTO wallets (address) VALUES (%s) ON CONFLICT DO NOTHING;", db_masters_batch)
                if db_subs_batch:
                    cur.executemany("INSERT INTO wallets (address, master_address, subaccount_name) VALUES (%s, %s, %s) ON CONFLICT (address) DO UPDATE SET master_address = EXCLUDED.master_address, subaccount_name = EXCLUDED.subaccount_name;", db_subs_batch)

            mark_batch_as_done(conn, processed_addresses)
            revert_batch_to_pending(conn, unprocessed_addresses)
            
            conn.commit()
            
            new_count += len(accounts_batch)
            print(f"[+] Saved {len(processed_addresses)} processed addresses. Reverted {len(unprocessed_addresses)} addresses back to PENDING.")

        print(f"\n[+] Session finished. A total of {new_count} new subaccounts were added in this session.")

if __name__ == "__main__":
    main()