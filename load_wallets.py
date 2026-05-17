import os
import csv
import psycopg

from dotenv import load_dotenv
from src.paths import PATHS

load_dotenv(dotenv_path=PATHS.ENV)

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DB_PORT", 5432))
}

def load_unique_wallets(conn, filepath):
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

def load_subaccounts(conn, filepath):
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

def populate_queue(conn):
    query = """
        INSERT INTO wallet_processing_queue (wallet_address)
        SELECT address FROM wallets
        ON CONFLICT (wallet_address) DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(query)
    conn.commit()

def mark_scanned_masters_as_done(conn, filepath):
    """Zmienia status na 'DONE' dla adresów obecnych w pliku tekstowym."""
    if not os.path.exists(filepath):
        print(f"Plik {filepath} nie istnieje. Pomijam aktualizację statusów.")
        return

    with open(filepath, mode='r', encoding='utf-8') as f:
        scanned_addresses = [(line.strip(),) for line in f if line.strip()]

    if not scanned_addresses:
        print("Plik scanned_masters.txt jest pusty.")
        return

    query = """
        UPDATE wallet_processing_queue
        SET status = 'DONE'
        WHERE wallet_address = %s;
    """
    
    with conn.cursor() as cur:
        cur.executemany(query, scanned_addresses)
    conn.commit()
    print(f"Zaktualizowano status na 'DONE' dla {len(scanned_addresses)} adresów.")

def main():
    with psycopg.connect(**DB_CONFIG) as conn:
        
        WALLETS_CSV = PATHS.ADDRESSES / "unique_wallets.csv"
        SUBACCOUNTS_CSV = PATHS.ADDRESSES / "only_subaccounts.csv"
        SCANNED_TXT = PATHS.ADDRESSES_TEMP / "scanned_masters.txt" 

        load_unique_wallets(conn, WALLETS_CSV)
        load_subaccounts(conn, SUBACCOUNTS_CSV)
        populate_queue(conn)
        mark_scanned_masters_as_done(conn, SCANNED_TXT)

if __name__ == "__main__":
    main()