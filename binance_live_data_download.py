import time
import logging
import threading
from binance.lib.utils import config_logging
from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
from binance.spot import Spot
import json
import csv
import os
from datetime import datetime
from typing import List, Dict
import queue
import re

#STORAGE_PATH = "/mnt/d/data"
STORAGE_PATH = "/home/ziyu"
CACHE_SIZE = 1048576  # Adjust this value based on your memory constraints

class DiskWriter(threading.Thread):
    def __init__(self, queue):
        super().__init__()
        self.queue = queue
        self.file_handles = {}
        self.start()

    def run(self):
        current_date = None
        while True:
            symbol, msg_type, data = self.queue.get()
            if msg_type is None:  # Stop signal
                break

            receive_time = datetime.now()
            data['receive_time'] = receive_time.strftime("%Y-%m-%d %H:%M:%S.%f")
            epoch_time_ms = int(receive_time.timestamp() * 1000)
            data['receive_epoch_ms'] = epoch_time_ms

            date = receive_time.strftime("%Y-%m-%d")
            if date != current_date:
                # Close file handles for the previous date
                self.close_file_handles()
                current_date = date

            file_name = f"{msg_type}_{symbol}_{date}.csv"
            full_path = os.path.join(STORAGE_PATH, file_name)

            try:
                if full_path not in self.file_handles:
                    self.file_handles[full_path] = self.open_file_handle(full_path, data.keys())

                file_handle, writer = self.file_handles[full_path]
                writer.writerow(data)

            except OSError as e:
                logging.error(f"Error writing to file {full_path}: {e}")

            self.queue.task_done()

    def join(self, timeout=None):
        self.queue.put((None, None, None))
        super().join(timeout)
        self.close_file_handles()

    def open_file_handle(self, full_path, fieldnames):
        file_handle = open(full_path, 'a', newline='')
        writer = csv.DictWriter(file_handle, fieldnames=fieldnames)
        if os.path.getsize(full_path) == 0:
            writer.writeheader()
        return file_handle, writer

    def close_file_handles(self):
        for file_handle, _ in self.file_handles.values():
            try:
                file_handle.close()
            except OSError as e:
                logging.error(f"Error closing file: {e}")
        self.file_handles.clear()

# 47 symbols at most for snapshot within 120 seconds
class RestApiThread(threading.Thread):
    def __init__(self, client, subscriptions, queue):
        super().__init__()
        self.client = client
        self.subscriptions = subscriptions.get('snapshot', [])  # Get the 'snapshot' list directly
        self.queue = queue
        self.daemon = True  # Make the thread a daemon thread

    def run(self):
        while True:
            start_time = time.time()
            for config in self.subscriptions:  # Iterate over the 'snapshot' list directly
                symbol = config.get('symbol').upper()
                level = config.get('level', 1000)
                snapshot_resp = self.client.depth(symbol, limit=level)
                if 'code' in snapshot_resp:
                    logging.error(f"Error in snapshot response for {symbol}: {snapshot_resp}")
                else:
                    self.queue.put((symbol, 'snapshot', snapshot_resp))
                elapsed_time = time.time() - start_time
                wait_time = max(0, 3 - (elapsed_time % 3))
                time.sleep(wait_time)

            cycle_duration = time.time() - start_time
            if cycle_duration < 120:
                time.sleep(120 - cycle_duration)

def extract_components(stream_value):
    # 1. (\w+): Matches the symbol part
    # 2. @(\w+): Matches the msg_type part immediately following '@'
    # 3. (\d+ms)?: Optionally matches a sequence of digits followed by 'ms' (for frequency), making this part optional
    # The frequency part (\d+ms)? is now clearly defined to match one or more digits followed by 'ms'
    pattern = re.compile(r'^(\w+)@(\w+)(?:@(\d+ms))?$')

    match = pattern.search(stream_value)
    if match:
        symbol, msg_type, freq = match.groups()
        return {
            'symbol': symbol,
            'msg_type': msg_type,
            'freq': freq if freq is not None else 'N/A'  # Using 'None' check for clarity
        }
    return None

class MessageProcessor:
    def __init__(self, data_queue):
        self.msg_count = 0
        self.data_queue = data_queue

    def handle_message(self, _, message):
        self.msg_count += 1  # Increment message count for every message received
        try:
            data = json.loads(message)
            logging.debug(f'Processing message: {data}')
            if "result" in data and data["id"]:
                logging.info(f'Subscriptions: {data["id"]}')
            elif "data" in data:
                components = extract_components(data['stream'])
                sym = components['symbol']
                msg_type = components['msg_type']
                data_cache.put((sym, msg_type, data["data"]))
            if self.msg_count % 10000 == 0:
                logging.info(f'received {self.msg_count} messages')
        except (ValueError, KeyError):
            logging.error(f"Invalid message format: {message}")

class ReconnectingWebsocketStreamClient(SpotWebsocketStreamClient):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.should_reconnect = True
        self.reconnect_event = threading.Event()

    def on_close(self, _):
        if self.should_reconnect:
            logging.warning("WebSocket connection closed. Reconnecting...")
            self.reconnect_event.set()

    def reconnect(self):
        self.stop()
        self.reconnect_event.wait()
        self.reconnect_event.clear()
        self.start()

def batch_subscribe(client: ReconnectingWebsocketStreamClient, subscriptions: Dict[str, List[Dict]], batch_size=5, delay=2):
    """
    Subscribe to streams in batches, waiting for a delay between each batch.

    :param client: The WebSocket client instance.
    :param subscriptions: The subscriptions to make, organized by message type.
    :param batch_size: The number of subscriptions to send in each batch.
    :param delay: The delay between batches, in seconds.
    """
    # Flatten all subscription requests into a single list with message type included
    all_requests = []
    for msg_type, symbol_configs in subscriptions.items():
        for config in symbol_configs:
            all_requests.append((msg_type, config))
    
    # Batch the requests and send each batch with a delay
    for i in range(0, len(all_requests), batch_size):
        batch = all_requests[i:i + batch_size]
        for msg_type, config in batch:
            symbol = config.get('symbol')
            speed = config.get('speed', 1000)
            level = config.get('level', 20)
            # Modify this part to send the subscription based on msg_type
            if msg_type == "diff_book_depth":
                client.diff_book_depth(symbol=symbol, speed=speed)
            elif msg_type == "book_ticker":
                client.book_ticker(symbol=symbol)
            elif msg_type == "depth":
                client.partial_book_depth(symbol=symbol, level=20, speed=100)
            # Add more message types here as needed
        # Wait before sending the next batch
        if i + batch_size < len(all_requests):  # Prevent sleeping after the last batch
            time.sleep(delay)

def main():
    config_logging(logging, logging.INFO, log_file='downloader.log')
    global data_cache
    data_cache = queue.Queue(maxsize=CACHE_SIZE)
    disk_writer = DiskWriter(data_cache)

    message_processor = MessageProcessor(data_cache)
    spot_ws_client = ReconnectingWebsocketStreamClient(
        on_message=message_processor.handle_message,
        stream_url="wss://stream.binance.com:443",
        is_combined=True
    )

    subscriptions = {
        "depth": [
            {"symbol": "ethusdt", "level": 20},
            {"symbol": "injusdt", "level": 20},
        ],
        "book_ticker": [
            {"symbol": "injusdt"},
            {"symbol": "ethusdt"},
        ],
        "diff_book_depth": [
            {"symbol": "ethusdt", "speed": 100},
            {"symbol": "injusdt", "speed": 100},
        ],
        "snapshot": [
            {"symbol": "ethusdt", "level": 1000},
            {"symbol": "injusdt", "level": 1000},
        ],
    }

    batch_subscribe(spot_ws_client, subscriptions, batch_size=5, delay=2)

    spot_rest_client = Spot()
    rest_api_thread = RestApiThread(spot_rest_client, subscriptions, data_cache)
    rest_api_thread.start()

    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            spot_ws_client.should_reconnect = False
            break

    logging.debug("Closing WebSocket connection")
    disk_writer.join()
    spot_ws_client.stop()


if __name__ == "__main__":
    main()