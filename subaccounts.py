import time

from src.database.db_client import HyperliquidClient
from src.database.hl_endpoints import HyperliquidEndpoints
import src.database.caching as cache
from src.database.subaccounts_discovery import SubaccountsDiscovery

POLLING_INTERVAL_SECONDS = 60

def main() -> None:
    """
    Main execution loop orchestrating the data pipeline.
    """
    print(f"[*] --- CONFIGURATION ---")
    print(f"[*] Starting Orchestrator")
    print(f"[*] ---------------------------\n")

    scanned_history = cache.load_scanned_masters()
    existing_subaccounts = cache.load_existing_subaccounts()

    api_client = HyperliquidClient()
    endpoints = HyperliquidEndpoints(api_client)

    with api_client.get_db_connection() as conn:
        print("[*] Connected to PostgreSQL. Starting worker...\n")

        discovery_phase = SubaccountsDiscovery(conn, endpoints, existing_subaccounts, scanned_history)

        try:
            while True:
                # Search for new wallets and resolve master/subaccount hierarchies.
                if discovery_phase.execute():
                    continue 

                # TODO: Implement Extraction Phase here (e.g., extraction_phase.execute())
                # if extraction_phase.execute():
                #     continue

                # If all queues are empty, rest the worker to avoid spamming the DB.
                print(f"\n[*] All queues are empty. Waiting {POLLING_INTERVAL_SECONDS} seconds...")
                time.sleep(POLLING_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            print("\n[+] Graceful shutdown complete.")
        except Exception as e:
            print(f"\n[!] Pipeline terminated due to critical error: {e}")

if __name__ == "__main__":
    main()