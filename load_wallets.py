import os
import csv
from pathlib import Path

from src.paths import PATHS
from src.database.db_client import HyperliquidClient

def load_unique_wallets(conn, filepath: Path) -> None:
    """Loads unique wallets from CSV into the main wallets table."""
    with open(filepath, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        data = [(row['address'], row['source']) for row in reader]

    query = """
        INSERT INTO wallets (address, source)
        VALUES (%s, %s)
        ON CONFLICT (address) DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.executemany(query, data)
    conn.commit()
    print(f"[*] Loaded file: {filepath.name}")

def load_subaccounts(conn, filepath: Path) -> None:
    """Loads subaccounts and their master relationships from CSV."""
    with open(filepath, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        masters = []
        subs = []
        for row in reader:
            masters.append((row['master_address'],))
            subs.append((
                row['subaccount_address'], 
                row['master_address'], 
                row['subaccount_name']
            ))

    master_query = """
        INSERT INTO wallets (address)
        VALUES (%s)
        ON CONFLICT (address) DO NOTHING;
    """

    sub_query = """
        INSERT INTO wallets (address, master_address, subaccount_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (address) DO UPDATE SET
            master_address = EXCLUDED.master_address,
            subaccount_name = EXCLUDED.subaccount_name;
    """

    with conn.cursor() as cur:
        cur.executemany(master_query, masters)
        cur.executemany(sub_query, subs)
    conn.commit()
    print(f"[*] Loaded file: {filepath.name} (Subaccounts: {len(subs)})")

def populate_queue(conn) -> None:
    """Populates the processing queue with all distinct addresses from the wallets table."""
    query = """
        INSERT INTO wallet_processing_queue (wallet_address)
        SELECT address FROM wallets
        ON CONFLICT (wallet_address) DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()
    print("[*] Inserted new addresses from the main table into the processing queue.")

def mark_scanned_masters_as_done(conn, filepath: Path) -> None:
    """Updates status to 'DONE' for addresses already present in the scanned masters text file."""
    if not os.path.exists(filepath):
        print(f"[-] File {filepath} does not exist. Skipping status update.")
        return

    with open(filepath, mode='r', encoding='utf-8') as f:
        scanned_addresses = [(line.strip(),) for line in f if line.strip()]

    if not scanned_addresses:
        print("[-] The scanned_masters.txt file is empty.")
        return

    query = """
        UPDATE wallet_processing_queue
        SET status = 'DONE',
            completed_at = NOW()
        WHERE wallet_address = %s;
    """
    
    with conn.cursor() as cur:
        cur.executemany(query, scanned_addresses)
    conn.commit()
    print(f"[+] Updated status to 'DONE' for {len(scanned_addresses)} scanned masters.")

def mark_known_subaccounts_as_done(conn) -> None:
    """
    Updates status to 'DONE' for all wallets that are assigned as subaccounts 
    in the main table.
    """
    query = """
        UPDATE wallet_processing_queue
        SET status = 'DONE',
            completed_at = NOW()
        WHERE wallet_address IN (
            SELECT address 
            FROM wallets 
            WHERE master_address IS NOT NULL
        ) AND status != 'DONE';
    """
    with conn.cursor() as cur:
        cur.execute(query)
        updated_rows = cur.rowcount 
    conn.commit()
    print(f"[+] Marked {updated_rows} known subaccounts as 'DONE'.")

def main() -> None:
    hl_client = HyperliquidClient()

    WALLETS_CSV = PATHS.ADDRESSES / "unique_wallets.csv"
    SUBACCOUNTS_CSV = PATHS.ADDRESSES / "only_subaccounts.csv"
    SCANNED_TXT = PATHS.ADDRESSES_TEMP / "scanned_masters.txt" 

    with hl_client.get_db_connection() as conn:
        load_unique_wallets(conn, WALLETS_CSV)
        load_subaccounts(conn, SUBACCOUNTS_CSV)

        populate_queue(conn)

        mark_scanned_masters_as_done(conn, SCANNED_TXT)
        mark_known_subaccounts_as_done(conn)

if __name__ == "__main__":
    main()