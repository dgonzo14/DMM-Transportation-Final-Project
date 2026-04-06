import os
import json
import time
import logging
import requests
from datetime import datetime
from abc import ABC, abstractmethod

import threading
from concurrent.futures import ThreadPoolExecutor

# --- Configuration ---
TFL_API_BASE_URL = "https://api.tfl.gov.uk"
TFL_APP_KEY = os.getenv("TFL_APP_KEY")  # REQUIRED for Crowding
MODES = ["tube"]
POLL_INTERVAL = 30
CROWDING_INTERVAL = 300  # Crowding updates slower; 5 mins (300s) is safer for rate limits
ENABLE_CROWDING = True  # Set to False to disable the high-volume requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class DataSink(ABC):
    @abstractmethod
    def write(self, data: any, metadata: dict): pass


class LocalFileSystemSink(DataSink):
    def __init__(self, base_path):
        self.base_path = base_path

    def write(self, data, metadata):
        type_dir = os.path.join(self.base_path, metadata['data_type'])
        os.makedirs(type_dir, exist_ok=True)
        timestamp = metadata['timestamp'].strftime("%Y%m%d_%H%M%S")

        # Unique naming for crowding to prevent overwrites if multiple stations are saved
        suffix = f"_{metadata.get('naptan')}" if 'naptan' in metadata else f"_{metadata['mode']}"
        filename = f"tfl{suffix}_{timestamp}.json"

        with open(os.path.join(type_dir, filename), 'w') as f:
            json.dump(data, f)


class TfLProducer:
    def __init__(self, sink: DataSink):
        self.sink = sink
        self.headers = {"Cache-Control": "no-cache"}
        if TFL_APP_KEY: self.headers["app_key"] = TFL_APP_KEY

        self.last_crowding_time = 0
        self.station_ids = []
        if ENABLE_CROWDING:
            self.station_ids = self._discover_stations()

        self.max_workers = 10  # Number of concurrent threads
        self.rate_limit_semaphore = threading.Semaphore(self.max_workers)

    def _get(self, endpoint):
        try:
            response = requests.get(f"{TFL_API_BASE_URL}{endpoint}", headers=self.headers, timeout=10)
            if response.status_code == 429:
                logging.warning("Rate limit hit! Sleeping...")
                time.sleep(5)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Error fetching {endpoint}: {e}")
            return None

    def _discover_stations(self):
        """One-time fetch of all NaPTAN IDs for the target modes."""
        logging.info("Discovering station NaPTAN IDs...")
        stations = []
        for mode in MODES:
            data = self._get(f"/StopPoint/Mode/{mode}")

            # THE FIX: TfL returns an object with a 'stopPoints' key which is the list
            if data and "stopPoints" in data:
                station_list = data["stopPoints"]
                stations.extend([
                    s['id'] for s in station_list
                    if s.get('stopType') == "NaptanMetroStation"
                ])
            else:
                logging.warning(f"Unexpected data format for mode {mode}")

        logging.info(f"Discovered {len(stations)} stations.")
        return list(set(stations))

    def fetch_standard_data(self):
        now = datetime.now()
        for mode in MODES:
            # 1. Arrivals & 2. Status
            for dtype, path in [("arrivals", f"/Mode/{mode}/Arrivals"),
                                ("status", f"/Line/Mode/{mode}/Status?detail=true")]:
                data = self._get(path)
                if data:
                    self.sink.write(data, {"mode": mode, "data_type": dtype, "timestamp": now})
                    logging.info(f"Fetched Arrivals/Status data for {mode} at time {now.strftime('%Y-%m-%d %H:%M:%S')}.")

    def _fetch_single_station_crowding(self, naptan, timestamp):
        """Worker function for a single station."""
        with self.rate_limit_semaphore:
            data = self._get(f"/crowding/{naptan}/Live")
            if data:
                self.sink.write(data, {
                    "naptan": naptan,
                    "data_type": "crowding",
                    "timestamp": timestamp
                })
            # Keep a small buffer so we don't overwhelm the API burst limit
            time.sleep(0.1)

    def fetch_crowding_data(self):
        """Queries station IDs concurrently using a thread pool."""
        now = datetime.now()
        logging.info(f"Starting concurrent crowding poll for {len(self.station_ids)} stations...")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Map the worker function across all discovered NaPTANs
            executor.map(lambda n: self._fetch_single_station_crowding(n, now), self.station_ids)

        logging.info("Finished crowding poll.")

    def run(self):
        while True:
            loop_start = time.time()

            # Standard Data (Arrivals/Status)
            self.fetch_standard_data()

            # Optional Crowding Data (on a separate schedule)
            if ENABLE_CROWDING and (time.time() - self.last_crowding_time > CROWDING_INTERVAL):
                self.fetch_crowding_data()
                self.last_crowding_time = time.time()

            elapsed = time.time() - loop_start
            time.sleep(max(0, POLL_INTERVAL - elapsed))


if __name__ == "__main__":
    storage = LocalFileSystemSink("./landing_zone/tfl")
    producer = TfLProducer(sink=storage)
    producer.run()