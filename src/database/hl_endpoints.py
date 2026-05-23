from typing import List, Dict, Any
from src.database.db_client import HyperliquidClient

class HyperliquidEndpoints:
    def __init__(self, client: HyperliquidClient):
        self.client = client

    def fetch_master_address(self, address: str) -> str:
        """
        (Base Weight: 60) 
        Verifies the role of the given wallet. If it's a subaccount, 
        returns the master address. Otherwise, returns itself.
        """
        payload = {"type": "userRole", "user": address}
        data = self.client.safe_api_request(payload)
        
        if data.get("role") == "subAccount":
            return data.get("data", {}).get("master", address)
        return address

    def fetch_subaccounts(self, master_address: str) -> List[Dict[str, Any]]:
        """
        (Base Weight: 20) 
        Fetches the complete list of subaccounts assigned to a master address.
        """
        payload = {"type": "subAccounts", "user": master_address}
        data = self.client.safe_api_request(payload)
        return data if isinstance(data, list) else []

    def fetch_clearinghouse_state(self, wallet_address: str) -> Dict[str, Any]:
        """
        (Base Weight: 2) 
        Fetches the current portfolio state (account value, margin, positions).
        """
        payload = {"type": "clearinghouseState", "user": wallet_address}
        return self.client.safe_api_request(payload)

    def fetch_user_fills(self, wallet_address: str, start_time: int = 0) -> List[Dict[str, Any]]:
        """
        (Base Weight: 20 + Dynamic Penalty) 
        Fetches transaction history (fills).
        """
        payload = {
            "type": "userFillsByTime", 
            "user": wallet_address, 
            "startTime": start_time
        }
        data = self.client.safe_api_request(payload)
        return data if isinstance(data, list) else []

    def fetch_user_funding(self, wallet_address: str, start_time: int = 0) -> List[Dict[str, Any]]:
        """
        (Base Weight: 20 + Dynamic Penalty) 
        Fetches historical funding fee payments.
        """
        payload = {
            "type": "userFunding", 
            "user": wallet_address, 
            "startTime": start_time
        }
        data = self.client.safe_api_request(payload)
        return data if isinstance(data, list) else []