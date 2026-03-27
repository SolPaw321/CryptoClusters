from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional, Iterable, TypeVar, Type
import csv
import json
import threading
import time
import websocket

T = TypeVar("T")

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
            max_retries: int = 5,
            base_delay: float = 3.0,
            ping_interval: float = 20.0,
            ping_timeout: float = 10.0,
            max_delay: float = 30.0,
            max_running_time: float = 60.0, # seconds
            max_wallets_found: int = 1000) -> None:
        super().__init__(name="trades", sink=sink)
        self.markets = list(markets)

        self.max_retries = self.__validate_inputs(max_retries, int, 5)
        self.base_delay = self.__validate_inputs(base_delay, float, 3.0)
        self.ping_interval = self.__validate_inputs(ping_interval, float, 20.0)
        self.ping_timeout = self.__validate_inputs(ping_timeout, float, 10.0)
        self.max_delay = self.__validate_inputs(max_delay, float, 30.0)

        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

        self._running = False
        self._finalized = False
        self._stop_reason: Optional[str] = None
        self.current_retry = 0

        self.start_time = 0
        self._n_trades = 0

        self.max_running_time = self.__validate_inputs(max_running_time, float, 60.0)
        self.max_wallets_found = self.__validate_inputs(max_wallets_found, int, 1000)

    @staticmethod
    def __validate_inputs(input_value, expected_type: Type[T], default_value: T, minimum_value: T = 0) -> T:
        """
        Validate input value.

        :param input_value: input value
        :param expected_type: expected type of input value
        :param default_value: default value
        :return: input_value if type(input_value)==expected_type, default_value otherwise
        """
        if type(input_value) == expected_type and input_value > minimum_value:
            return input_value
        return default_value

    def start(self) -> None:
        """
        Start websocket connection in a separate thread.
        This method blocks until the worker thread finishes.
        """
        if self._running:
            return

        self._running = True
        self._finalized = False
        self._stop_reason = None
        self.current_retry = 0
        self.start_time = time.time()
        self._n_trades = 0
        self._stop_event.clear()

        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

        try:
            while self._thread.is_alive():
                self._thread.join(timeout=0.5)
        except KeyboardInterrupt:
            self._request_stop("KeyboardInterrupt")
        finally:
            self._join_thread(timeout=5.0)
            self._finalize()

    def stop(self, stop_msg: Optional[str] = None) -> None:
        """
        Public stop method.
        Can be called manually from outside the class.

        :param stop_msg: stopping message
        """
        self._request_stop(stop_msg)
        self._join_thread(timeout=5.0)
        self._finalize()

    def _request_stop(self, reason: Optional[str] = None) -> None:
        """
        Internal stop request.
        Safe to call multiple times and safe from websocket callbacks.

        :param reason: stopping reason
        """
        if reason and self._stop_reason is None:
            self._stop_reason = reason

        self._running = False
        self._stop_event.set()

        ws = self._ws
        self._ws = None

        if ws is not None:
            try:
                print(f"[{self.name}] Closing WebSocketApp...")
                ws.close()
                print(f"[{self.name}] WebSocketApp closed.")
            except Exception as e:
                print(f"[{self.name}] Error while closing WebSocketApp: {e}")

    def _join_thread(self, timeout: float = 5.0) -> None:
        """
        Join worker thread if possible.

        :param timeout: thread joining timeout
        """
        if self._thread is None:
            return

        if threading.current_thread() is self._thread:
            return

        if self._thread.is_alive():
            print(f"[{self.name}] Joining thread...")
            self._thread.join(timeout=timeout)

            if self._thread.is_alive():
                print(f"[{self.name}] Thread is still alive after {timeout} seconds.")
            else:
                print(f"[{self.name}] Thread joined.")

    def _finalize(self) -> None:
        """
        Final summary and CSV save.
        Executed only once.
        """
        if self._finalized:
            return

        self._finalized = True

        n_wallets = self.sink.n_wallets_founded()
        n_trades = self._n_trades

        self.sink.save_to_csv()

        if self._stop_reason:
            print(f"[{self.name}] Stop reason: {self._stop_reason}")

        print(f"[{self.name}] Intercepted transactions: {n_trades}")
        print(f"[{self.name}] Unique wallets found: {n_wallets}")

        if n_trades > 0:
            efficiency = n_wallets / (2 * n_trades) * 100
            print(f"[{self.name}] Searching efficiency: {efficiency:.2f} %")
        else:
            print(f"[{self.name}] Searching efficiency: 0.00 %")

    def _run(self) -> None:
        """
        Run websocket connection with reconnect attempts.
        """
        while not self._stop_event.is_set():
            self._ws = websocket.WebSocketApp(
                self.WS_URL,
                on_open=self._on_open,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
            )

            try:
                self._ws.run_forever(
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_timeout,
                )
            except Exception as e:
                print(f"[{self.name}] run_forever exception: {e}")
            finally:
                self._ws = None

            if self._stop_event.is_set():
                break

            self.current_retry += 1

            if self.current_retry > self.max_retries:
                self._request_stop(
                    f"Maximum reconnect attempts exceeded ({self.max_retries})."
                )
                break

            delay = min(self.base_delay * (2 ** (self.current_retry - 1)), self.max_delay)

            print(
                f"[{self.name}] Reconnecting attempt "
                f"{self.current_retry}/{self.max_retries} in {delay} s..."
            )

            if self._stop_event.wait(delay):
                break

    def _stop_condition(self) -> None:
        """
        Checks the stop condition.

        Stops when:
          - maximum time has occurred or
          - maximum number of unique wallets was found
        """
        time_running = time.time() - self.start_time
        unique_wallets_found = self.sink.n_wallets_founded()

        if time_running >= self.max_running_time:
            self._request_stop(
                f"Maximum running time achieved. "
                f"Running time: {time_running:.2f} s, "
                f"Max running time: {self.max_running_time:.2f} s."
            )
        elif unique_wallets_found >= self.max_wallets_found:
            self._request_stop(
                f"Maximum number of wallets found. "
                f"Unique wallets found: {unique_wallets_found}, "
                f"Max unique wallets: {self.max_wallets_found}."
            )

    def _on_open(self, ws: websocket.WebSocketApp) -> None:
        """
        Open websocket connection and subscribe given markets.

        :param ws: WebSocketApp
        """
        if self._stop_event.is_set():
            return

        print(f"[{self.name}] Connected.")
        self.current_retry = 0

        for market in self.markets:
            msg = {
                "method": "subscribe",
                "subscription": {
                    "type": "trades",
                    "coin": market,
                },
            }

            try:
                ws.send(json.dumps(msg))
                print(f"[{self.name}] Subscription for trade on: {market}")
            except Exception as e:
                print(f"[{self.name}] Subscription error for {market}: {e}")

    def _on_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        """
        Handle websocket message.

        :param ws: WebSocketApp
        :param message: message
        """
        if self._stop_event.is_set():
            return

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
