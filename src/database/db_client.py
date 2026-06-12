import os
import time
import requests
import psycopg
from typing import Any
from dotenv import load_dotenv

from src.paths import PATHS

class HyperliquidClient:
    API_URL = "https://api.hyperliquid.xyz/info"
    MAX_WEIGHT_PER_MINUTE = 1200
    WEIGHT_PER_SEC = MAX_WEIGHT_PER_MINUTE / 60.0

    BASE_WEIGHTS = {
        "clearinghouseState": 2,
        "subAccounts": 20,
        "userRole": 60,            
        "userFillsByTime": 20,     
        "userFunding": 20,
        "portfolio": 20        
    }

    def __init__(self):
        load_dotenv(dotenv_path=PATHS.ENV)
        self.db_config = {
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST", "127.0.0.1"),
            "port": int(os.getenv("DB_PORT", 5432))
        }

    def get_db_connection(self):
        """
        Returns a PostgreSQL database connection.
        """
        return psycopg.connect(**self.db_config)

    def _calculate_dynamic_weight(self, payload: dict, response_data: Any) -> int:
        """
        Calculates the actual request cost based on the response size.
        """
        req_type = payload.get("type", "unknown")
        base_weight = self.BASE_WEIGHTS.get(req_type, 20)
        
        # Add penalty weight for fetched history size (1 extra weight per 20 items)
        if req_type in ["userFillsByTime", "userFunding"] and isinstance(response_data, list):
            extra_weight = len(response_data) // 20
            return base_weight + extra_weight
            
        return base_weight

    def safe_api_request(self, payload: dict) -> Any:
        """
        Sends the payload to the API and automatically pauses the script 
        based on the consumed API weight.
        """
        attempt = 0
        while True:
            try:
                response = requests.post(self.API_URL, json=payload, timeout=15)
                
                if response.status_code == 429:
                    print("\n[!] Rate limit hit. Pausing for 30 seconds...", flush=True)
                    time.sleep(30)
                    continue
                    
                response.raise_for_status()
                data = response.json()
                
                actual_weight = self._calculate_dynamic_weight(payload, data)
                sleep_time = (actual_weight / self.WEIGHT_PER_SEC) + 0.1
                time.sleep(sleep_time)
                
                return data
                
            except requests.exceptions.RequestException as e:
                attempt += 1
                sleep_time = min(60, 2 ** attempt)
                print(f"\n[-] API Connection error: {e}", flush=True)
                print(f"[-] Retrying in {sleep_time}s (Attempt #{attempt})", flush=True)
                time.sleep(sleep_time)