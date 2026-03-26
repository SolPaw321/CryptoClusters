from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional, Iterable
import csv
import json
import threading
import time
import websocket


@dataclass
class TransactionRecord:
    address: str
    source: str
    discovered: str = ""
    coin: Optional[str] = None
    side: Optional[str] = None
    timestamp: Optional[int] = None
    size: Optional[float] = None

    def __post_init__(self) -> None:
        if not self.discovered:
            self.discovered = datetime.now(timezone.utc).isoformat()


class TransactionSink:
    COL_ORDER = ["address", "source", "discovered", "coin", "side", "timestamp", "size"]

    def __init__(self, csv_path: str) -> None:
        self.csv_path = csv_path
        self._seen_addresses: set[str] = set()
        self._collected_wallets: list = list()
        self._lock = threading.Lock()
        self._ensure_csv_exists()

    def _ensure_csv_exists(self) -> None:
        """
        Ensure that csv file exists.
        """
        try:
            with open(self.csv_path, "x", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.COL_ORDER)
                writer.writeheader()
        except FileExistsError:
            pass

    def add(self, record: TransactionRecord) -> bool:
        """
        Add unique wallet address to memory.

        :param record: a transaction record
        :return: True if unique address, False otherwise.
        """
        if record.address in self._seen_addresses:
            return False

        self._seen_addresses.add(record.address)
        self._collected_wallets.append(asdict(record))
        return True

    def save_to_csv(self):
        """
        Save collected wallet addresses to csv file and free memory.
        """
        with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.COL_ORDER)
            writer.writerows(self._collected_wallets)

        self._collected_wallets = list()
        self._seen_addresses = set()

    def n_wallets_founded(self) -> int:
        """
        Number of unique wallets founded.
        :return: number of unique wallets
        """
        return len(self._seen_addresses)

class BaseSource(ABC):
    """
    Abstract class for wallets addresses finders.
    """
    def __init__(self, name: str, sink: TransactionSink) -> None:
        self.name = name
        self.sink = sink

    @abstractmethod
    def start(self) -> None:
        pass

    @abstractmethod
    def stop(self) -> None:
        pass


class TradesSource(BaseSource):
    """
    Search transactions in "real time" on given markets (coins) for collecting unique wallet addresses.
    """
    WS_URL = "wss://api.hyperliquid.xyz/ws"

    def __init__(
        self,
        sink: TransactionSink,
        markets: Iterable[str],
        reconnect_delay: float = 5.0,
    ) -> None:
        super().__init__(name="trades", sink=sink)
        self.markets = list(markets)
        self.reconnect_delay = reconnect_delay

        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self.start_time = time.time()
        self._n_trades = 0

        self.max_running_time = 60 # 1 minute
        self.max_wallets_found = 1000

    def start(self, *, max_running_time=None, max_wallets_found=None) -> None:
        """
        On start connection.

        Run a connection on a separate thread.

        :param max_running_time: maximum running time in seconds
        :param max_wallets_found: maximum number of unique wallets addresses to found
        """
        if self._running:
            return
        self._running = True

        if max_running_time is not None and type(max_running_time)==int:
            self.max_running_time = max_running_time
        if max_wallets_found is not None and type(max_wallets_found)==int:
            self.max_wallets_found = max_wallets_found

        try:
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()

            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            print(f"[{self.name}] KeyboardInterrupt. Stoping...")
            self.stop()

    def stop(self) -> None:
        """
        Close websocket connection and join threads when meet the stop condition.

        Stops when:
          - maximum time has occurred or
          - maximum number of unique wallets founded
        """
        self._running = False
        n_wallets = self.sink.n_wallets_founded()
        n_trades = self._n_trades

        if self._ws is not None:
            self._ws.close()

        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=2)

        self.sink.save_to_csv()

        print(f"[{self.name}] Intercepted transactions: {n_trades}")
        print(f"[{self.name}] Unique wallets found: {n_wallets}")
        print(f"[{self.name}] Searching efficiency: {n_wallets/(2*n_trades)*100} %")

    def _run(self) -> None:
        """
        Run websocket connection.
        """
        while self._running:
            self._ws = websocket.WebSocketApp(
                self.WS_URL,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )

            self._ws.run_forever()

            if self._running:
                time.sleep(self.reconnect_delay)

    def _stop_condition(self):
        """
        Checks the stop condition.

        Stops when:
          - maximum time has occurred or
          - maximum number of unique wallets founded
        """
        time_running = time.time() - self.start_time
        unique_wallets_found = self.sink.n_wallets_founded()

        if time_running >= self.max_running_time:
            print(f"[{self.name}] Maximum running time achieved.")
            self.stop()
        elif unique_wallets_found >= self.max_wallets_found:
            print(f"[{self.name}] Maximum number of wallets founded.")
            self.stop()

    def _on_open(self, ws: websocket.WebSocketApp) -> None:
        """
        Open websocket connection and subscribe given markets (coins).

        :param ws: WebSocketApp
        """
        print(f"[{self.name}] Connected.")

        for market in self.markets:
            msg = {
                "method": "subscribe",
                "subscription": {
                    "type": "trades",
                    "coin": market,
                },
            }
            ws.send(json.dumps(msg))
            print(f"[{self.name}] Subscription for trade on: {market}")

    def _on_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        """
        Sending a message to WebSocketApp and handling a payload.

        :param ws: WebSocketApp
        :param message: websocket message
        """
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            return

        channel = payload.get("channel")
        if channel != "trades":
            return

        data = payload.get("data", [])
        if not isinstance(data, list):
            return

        for trade in data:
            self._handle_trade(trade)

        self._stop_condition()

    def _handle_trade(self, trade: dict) -> None:
        """
        Handle a trade message.

        Create TransactionRecords for buyer and seller, then add to unique wallet addresses to TransactionSink.

        :param trade: transaction information (eq. wallet address, coin, time, position size)
        """
        users = trade.get("users")
        if not isinstance(users, list) or len(users) != 2:
            return

        buyer, seller = users[0], users[1]
        market = trade.get("coin")
        timestamp_ms = trade.get("time")
        size = self._to_float(trade.get("sz"))

        buyer_record = TransactionRecord(
            address=buyer,
            source=self.name,
            coin=market,
            side="buyer",
            timestamp=timestamp_ms,
            size=size,
        )

        seller_record = TransactionRecord(
            address=seller,
            source=self.name,
            coin=market,
            side="seller",
            timestamp=timestamp_ms,
            size=size,
        )

        is_new_buyer = self.sink.add(buyer_record)
        is_new_seller = self.sink.add(seller_record)

        if is_new_buyer:
            print(f"[{self.name}] | NEW BUYER  | {buyer} | {market} | {size}")
        if is_new_seller:
            print(f"[{self.name}] | NEW SELLER | {seller} | {market} | {size}")

        self._n_trades += 1

    def _on_error(self, ws: websocket.WebSocketApp, error: object) -> None:
        """
        Handle errors.

        :param ws: WebSocketApp
        :param error: an error occurred
        """
        print(f"[{self.name}] Error: {error}")

    def _on_close(self, ws: websocket.WebSocketApp, close_status_code: object, close_msg: object) -> None:
        """
        On close.

        :param ws: WebSocketApp
        :param close_status_code: close status code
        :param close_msg: close message
        """
        print(f"[{self.name}] Disconnected. code={close_status_code} msg={close_msg}")

    @staticmethod
    def _to_float(value) -> Optional[float]:
        """
        Convert given value to float.

        Return None if the value when TypeError or ValueError occurred.
        Also return None when the value is None.

        :param value: given value
        :return: float | None
        """
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None
