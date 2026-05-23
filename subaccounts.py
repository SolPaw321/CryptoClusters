import os
import pandas as pd
import csv
from pathlib import Path
from typing import List, Dict, Any, Set

from src.paths import PATHS
from src.database.db_client import HyperliquidClient
from src.database.hl_endpoints import HyperliquidEndpoints

# --- PATH CONFIGURATION ---
OUTPUT_CSV = PATHS.ADDRESSES / "only_subaccounts.csv"
SCANNED_CACHE_FILE = PATHS.ADDRESSES_TEMP / "scanned_masters.txt"
BATCH_SIZE = 10 

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
    """
    Fetch batch addresses, change their status to PROCESSING, 
    and set started_at timestamp.
    """
    query = """
        UPDATE wallet_processing_queue
        SET status = 'PROCESSING',
            started_at = NOW(),
            completed_at = NULL
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
    """
    Changes processed addresses status to DONE 
    and updates completed_at with the completion timestamp.
    """
    if not addresses:
        return
    query = """
        UPDATE wallet_processing_queue
        SET status = 'DONE',
            completed_at = NOW()
        WHERE wallet_address = %s;
    """
    with conn.cursor() as cur:
        cur.executemany(query, [(addr,) for addr in addresses])

def revert_batch_to_pending(conn, addresses: List[str]) -> None:
    """
    Reverts status of unprocessed addresses from PROCESSING to PENDING 
    and clears timestamps for clean retry.
    """
    if not addresses:
        return
    query = """
        UPDATE wallet_processing_queue
        SET status = 'PENDING',
            started_at = NULL,
            completed_at = NULL
        WHERE wallet_address = %s;
    """
    with conn.cursor() as cur:
        cur.executemany(query, [(addr,) for addr in addresses])

def main() -> None:
    print(f"[*] --- CONFIGURATION ---")
    print(f"[*] Output CSV Backup: {OUTPUT_CSV}")
    print(f"[*] Cache TXT Backup:  {SCANNED_CACHE_FILE}")
    print(f"[*] ---------------------------\n")

    scanned_history = load_scanned_masters()
    existing_subaccounts = load_existing_subaccounts()

    api_client = HyperliquidClient()
    endpoints = HyperliquidEndpoints(api_client)

    with api_client.get_db_connection() as conn:
        print("[*] Connected to DB. Starting worker...\n")
        new_count = 0

        try:
            while True:
                batch_addresses = fetch_pending_batch(conn, BATCH_SIZE)
                
                if not batch_addresses:
                    print("\n[*] No PENDING addresses in database.")
                    break
                
                print(f"\n[!] Reserved {len(batch_addresses)} addresses. Status changed to PROCESSING.")
                
                accounts_batch = []   
                scanned_batch = []    
                db_masters_batch = []    
                db_subs_batch = []       

                for i, address in enumerate(batch_addresses, 1):
                    if address in existing_subaccounts or address in scanned_history:
                        print(f"[{i}/{len(batch_addresses)}] [SKIPPED] {address[:8]}... -> Known address found in cache. Skipping request.")
                        scanned_batch.append(address)
                        continue

                    subs = endpoints.fetch_subaccounts(address)

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
                        real_master = endpoints.fetch_master_address(address)
                        
                        scanned_batch.append(address)
                        db_masters_batch.append((address,)) 

                        if real_master != address:
                            if real_master not in scanned_history and real_master not in scanned_batch:
                                master_subs = endpoints.fetch_subaccounts(real_master)
                                
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

                # Flush logic remains identical
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