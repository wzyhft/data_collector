import time
import logging
import threading
from binance.lib.utils import config_logging
from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient
import json
import csv
import os
from datetime import datetime
from typing import List, Dict
import queue

STORAGE_PATH = "/mnt/d/data"
CACHE_SIZE = 1048576  # Adjust this value based on your memory constraints

class DiskWriter(threading.Thread):
    def __init__(self, queue):
        super().__init__()
        self.queue = queue
        self.file_handles = {}
        self.start()

    def run(self):
        while True:
            msg_type, data = self.queue.get()
            if msg_type is None:  # Stop signal
                break

            receive_time = datetime.now()
            data['receive_time'] = receive_time.strftime("%Y-%m-%d %H:%M:%S.%f")
            epoch_time_ms = int(receive_time.timestamp() * 1000)
            data['epoch_time_ms'] = epoch_time_ms

            symbol = data['s']
            date = receive_time.strftime("%Y-%m-%d")
            file_name = f"{msg_type}_{symbol}_{date}.csv"
            full_path = os.path.join(STORAGE_PATH, file_name)

            file_handle = self.file_handles.get(full_path)
            if file_handle is None:
                file_handle = open(full_path, 'a', newline='')
                writer = csv.DictWriter(file_handle, fieldnames=data.keys())
                if os.path.getsize(full_path) == 0:
                    writer.writeheader()
                self.file_handles[full_path] = (file_handle, writer)
            else:
                _, writer = file_handle

            writer.writerow(data)

            self.queue.task_done()

    def join(self, timeout=None):
        self.queue.put((None, None))
        super().join(timeout)

        for file_handle, _ in self.file_handles.values():
            file_handle.close()

def message_handler(_, message):
    data_cache.put((message['stream'], message['data']))

def persist_data(msg_type: str, json_data):
    data = json.loads(json_data)

    receive_time = datetime.now()
    data['receive_time'] = receive_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    epoch_time_ms = int(receive_time.timestamp() * 1000)
    data['epoch_time_ms'] = epoch_time_ms

    symbol = data['s']
    date = receive_time.strftime("%Y-%m-%d")
    file_name = f"{msg_type}_{symbol}_{date}.csv"
    full_path = os.path.join(STORAGE_PATH, file_name)

    with open(full_path, 'a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=data.keys())
        if file.tell() == 0:
            writer.writeheader()
        writer.writerow(data)

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

def subscribe_streams(client: ReconnectingWebsocketStreamClient, subscriptions: Dict[str, List[Dict]]):
    for msg_type, symbol_configs in subscriptions.items():
        for config in symbol_configs:
            symbol = config.get('symbol')
            speed = config.get('speed', 1000)
            if msg_type == "diff_book_depth":
                client.diff_book_depth(symbol=symbol, speed=speed)
            elif msg_type == "book_ticker":
                client.book_ticker(symbol=symbol)
            elif msg_type == "depth":
                client.partial_book_depth(symbol=symbol, level=20, speed=100)
            # Add more message types here

def main():
    config_logging(logging, logging.INFO)
    global data_cache
    data_cache = queue.Queue(maxsize=CACHE_SIZE)
    disk_writer = DiskWriter(data_cache)
    client = ReconnectingWebsocketStreamClient(on_message=message_handler)

    subscriptions = {
        "depth": [
            {"symbol": "ethusdt", "speed": 1000},
            {"symbol": "injusdt", "speed": 1000},
        ],
        "book_ticker": [
            {"symbol": "injusdt"},
            {"symbol": "ethusdt"},
        ],
    }

    subscribe_streams(client, subscriptions)

    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            client.should_reconnect = False
            break

    logging.debug("Closing WebSocket connection")
    disk_writer.join()
    client.stop()


if __name__ == "__main__":
    main()