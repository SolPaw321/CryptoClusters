import pandas as pd
from typing import List, Dict, Set, Tuple

from src.paths import PATHS
from src.database.hl_endpoints import HyperliquidEndpoints
import src.database.repository as db
import src.database.caching as cache

OUTPUT_CSV = PATHS.ADDRESSES / "only_subaccounts.csv"
SCANNED_CACHE_FILE = PATHS.ADDRESSES_TEMP / "scanned_masters.txt"
BATCH_SIZE = 10

class SubaccountsDiscovery:
    """
    Discovery Phase Handler.

    Encapsulates the state and logic for resolving wallet hierarchies,
    processing batches, and safely flushing data to persistent storage.
    """
    def __init__(self, conn, endpoints: HyperliquidEndpoints, existing_subaccounts: Set[str], scanned_history: Set[str]):
        self.conn = conn
        self.endpoints = endpoints
        self.existing_subaccounts = existing_subaccounts
        self.scanned_history = scanned_history

        # Internal state for the current batch
        self.accounts_batch: List[Dict] = []
        self.scanned_batch: List[str] = []
        self.db_masters_batch: List[Tuple] = []
        self.db_subs_batch: List[Tuple] = []

    def _extract_new_subaccounts(self, master_addr: str, subs: List[Dict]) -> int:
        """
        Extract newly discovered subaccounts from API response.

        :param master_addr: master wallet address
        :param subs: list of subaccount dictionaries returned by the API
        :return: Number of newly added subaccounts
        """
        new_in_this_round = 0
        for sub in subs:
            sub_addr = sub.get("subAccountUser")
            if sub_addr and sub_addr not in self.existing_subaccounts:
                sub_name = sub.get("name", "Unnamed")
                
                self.accounts_batch.append({
                    "master_address": master_addr,
                    "subaccount_address": sub_addr,
                    "discovered_at": pd.Timestamp.now(tz='UTC').isoformat(),
                    "subaccount_name": sub_name
                })
                self.db_subs_batch.append((sub_addr, master_addr, sub_name))
                self.existing_subaccounts.add(sub_addr)
                new_in_this_round += 1
                
        return new_in_this_round

    def _process_single_wallet(self, index: int, total: int, address: str) -> None:
        """
        Process discovery logic for a single wallet address with exact logging.

        :param index: current index in the batch
        :param total: total size of the batch
        :param address: the target wallet address to evaluate
        """
        if address in self.existing_subaccounts or address in self.scanned_history:
            print(f"[{index}/{total}] [SKIPPED] {address[:8]}... -> Known address found in cache. Skipping request.")
            self.scanned_batch.append(address)
            return

        subs = self.endpoints.fetch_subaccounts(address)

        if subs:
            self.db_masters_batch.append((address,))
            added = self._extract_new_subaccounts(address, subs)
            self.scanned_batch.append(address)
            print(f"[{index}/{total}] [MASTER] {address} -> Found {added} new subaccounts.")
            return

        real_master = self.endpoints.fetch_master_address(address)
        
        self.scanned_batch.append(address)
        self.db_masters_batch.append((address,))

        if real_master != address:
            if real_master not in self.scanned_history and real_master not in self.scanned_batch:
                master_subs = self.endpoints.fetch_subaccounts(real_master)
                self.db_masters_batch.append((real_master,))
                added = self._extract_new_subaccounts(real_master, master_subs)
                self.scanned_batch.append(real_master)
                print(f"[{index}/{total}] [SUBACCOUNT] Resolved {address[:8]}... -> Discovered New Master: {real_master} -> Found {added} new subaccounts.")
        else:
            print(f"[{index}/{total}] [LONELY MASTER] {address} -> Found 0 subaccounts.")


    def _flush_batch_to_storage(self, batch_addresses: List[str]) -> None:
        """
        Flush discovered batch data to local files and the database.

        :param batch_addresses: list of original pending addresses to mark as DONE
        """
        cache.append_to_csv(self.accounts_batch)
        cache.append_to_cache(self.scanned_batch)
        self.scanned_history.update(self.scanned_batch)
        
        with self.conn.cursor() as cur:
            if self.db_masters_batch:
                cur.executemany("INSERT INTO wallets (address) VALUES (%s) ON CONFLICT (address) DO NOTHING;", self.db_masters_batch)
                # Initialize discovered masters in the sync queue for future processing phases
                cur.executemany("INSERT INTO wallet_data_sync_queue (wallet_address, status) VALUES (%s, 'IDLE') ON CONFLICT DO NOTHING;", self.db_masters_batch)
            
            if self.db_subs_batch:
                cur.executemany("""
                    INSERT INTO wallets (address, master_address, subaccount_name)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (address) DO UPDATE SET
                        master_address = EXCLUDED.master_address,
                        subaccount_name = EXCLUDED.subaccount_name;
                """, self.db_subs_batch)

        db.mark_batch_as_done(self.conn, batch_addresses)
        self.conn.commit()


    def execute(self) -> bool:
        """
        Execute the wallet discovery and resolution phase.

        :return: True if a batch was processed, False if the pending queue is empty
        """
        batch_addresses = db.fetch_pending_batch(self.conn, BATCH_SIZE)
        
        if not batch_addresses:
            return False
        
        total_in_batch = len(batch_addresses)
        print(f"\n[!] [DISCOVERY] Reserved {total_in_batch} addresses. Status changed to PROCESSING.")

        try:
            for i, address in enumerate(batch_addresses, 1):
                self._process_single_wallet(i, total_in_batch, address)

            self._flush_batch_to_storage(batch_addresses)
            print(f"[+] [DISCOVERY] Flushed {total_in_batch} addresses. Total {len(self.accounts_batch)} new subaccounts.")
            return True

        except KeyboardInterrupt:
            print("\n[!] Program interrupted manually.")
            print("[!] Performing shutdown and DB rollback...")
            
            unprocessed_addresses = [addr for addr in batch_addresses if addr not in self.scanned_batch]
            
            self._flush_batch_to_storage(self.scanned_batch)
            db.revert_batch_to_pending(self.conn, unprocessed_addresses)
            self.conn.commit()
            
            print(f"[+] Saved {len(self.scanned_batch)} processed addresses. Reverted {len(unprocessed_addresses)} addresses back to PENDING.")
            raise

        except Exception as e:
            print(f"\n[-] Unexpected error in Discovery phase: {e}")
            self.conn.rollback()
            db.revert_batch_to_pending(self.conn, batch_addresses)
            self.conn.commit()
            raise