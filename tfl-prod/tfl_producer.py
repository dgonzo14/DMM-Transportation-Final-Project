import os
import json
import time
import logging
from datetime import datetime
from abc import ABC, abstractmethod

import requests
import boto3

import threading
from concurrent.futures import ThreadPoolExecutor

from pathlib import Path
from dotenv import load_dotenv

# --- Configuration ---

# configured for .env to sit in the root directory

script_dir = Path(__file__).resolve().parent
env_path = script_dir.parent / '.env'
load_dotenv(dotenv_path=env_path, override=True)

TFL_API_BASE_URL = "https://api.tfl.gov.uk"
TFL_APP_KEY = os.getenv("TFL_APP_KEY", None)  # REQUIRED for Crowding

MODES = ["tube"]
POLL_INTERVAL = 30
CROWDING_INTERVAL = 300  # Crowding updates slower; 5 mins (300s) is safer for rate limits
ENABLE_CROWDING = True  # Set to False to disable the high-volume requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if not TFL_APP_KEY:
    logging.warning("TFL_APP_KEY not set. Crowding data will not be fetched.")
    ENABLE_CROWDING = False
    # exit(1) # might be good when we go to prod but lowk annoying rn


class DataSink(ABC):
    @abstractmethod
    def write(self, data: any, metadata: dict): pass


class LocalFileSystemSink(DataSink):
    def __init__(self, base_path):
        self.base_path = base_path

    def write(self, data, metadata):
        now = metadata['timestamp']
        data_type = metadata['data_type']
        suffix = metadata.get('naptan') or metadata['mode']
        dir_path = os.path.join(
            self.base_path,
            f"bronze/tfl/{data_type}",
            f"year={now.year}", f"month={now.month:02d}",
            f"day={now.day:02d}", f"hour={now.hour:02d}"
        )
        os.makedirs(dir_path, exist_ok=True)
        filename = f"{now.strftime('%Y%m%dT%H%M%SZ')}_{data_type}_{suffix}.json"
        with open(os.path.join(dir_path, filename), 'w') as f:
            json.dump(data, f)

class R2Sink(DataSink):
    def __init__(self, bucket: str, account_id: str, access_key: str, secret_key: str):
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="auto",
        )

    def write(self, data, metadata):
        now = metadata['timestamp']
        data_type = metadata['data_type']
        suffix = metadata.get('naptan') or metadata['mode']
        key = (
            f"bronze/tfl/{data_type}/"
            f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}/"
            f"{now.strftime('%Y%m%dT%H%M%SZ')}_{data_type}_{suffix}.json"
        )
        self.client.put_object(Bucket=self.bucket, Key=key, Body=json.dumps(data))


class TfLProducer:
    def __init__(self, sink: DataSink):
        self.sink = sink
        self.headers = {"Cache-Control": "no-cache"}
        if TFL_APP_KEY: self.headers["app_key"] = TFL_APP_KEY

        self.last_crowding_time = 0
        self.last_status_time = 0
        self.status_interval = 300  # 5 minutes for Line Status & Lifts

        self.station_ids = None
        if ENABLE_CROWDING:
            self.station_ids = self._discover_stations()

        self.max_workers = 10
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
            if data and "stopPoints" in data:
                station_list = data["stopPoints"]
                stations.extend([
                    s['id'] for s in station_list
                    if s.get('stopType') == "NaptanMetroStation"
                ])
        logging.info(f"Discovered {len(stations)} stations.")
        return list(set(stations))

    def fetch_arrival_data(self):
        """High-frequency polling (30s refresh cycle)."""
        now = datetime.now()
        for mode in MODES:
            # Polling mode-level arrivals is most efficient for bulk data [1]
            data = self._get(f"/Mode/{mode}/Arrivals")
            if data:
                self.sink.write(data, {"mode": mode, "data_type": "arrivals", "timestamp": now})
                logging.info(f"Fetched Arrivals for {mode} at {now.isoformat()}.")

    def fetch_system_status(self):
        """Lower-frequency polling (5m refresh cycle) for operational metrics."""
        now = datetime.now()
        for mode in MODES:
            # 1. Line Status (General health) [2]
            status_data = self._get(f"/Line/Mode/{mode}/Status?detail=true")
            if status_data:
                self.sink.write(status_data, {"mode": mode, "data_type": "status", "timestamp": now})

            # 2. Lift/Escalator Disruptions (Competency/Accessibility metric)
            lift_data = self._get("/Disruptions/Lifts/v2")
            if lift_data:
                self.sink.write(lift_data, {"mode": mode, "data_type": "lift_disruptions", "timestamp": now})

        logging.info(f"Fetched Status & Lift data at {now.isoformat()}.")

    def _fetch_single_station_crowding(self, naptan, timestamp):
        with self.rate_limit_semaphore:
            data = self._get(f"/crowding/{naptan}/Live")
            if data:
                self.sink.write(data, {
                    "naptan": naptan,
                    "data_type": "crowding",
                    "timestamp": timestamp
                })
            time.sleep(0.1)

    def fetch_crowding_data(self):
        now = datetime.now()
        logging.info(f"Starting crowding poll for {len(self.station_ids)} stations...")
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            executor.map(lambda n: self._fetch_single_station_crowding(n, now), self.station_ids)

    def run(self):
        while True:
            loop_start = time.time()

            # 1. High-frequency: Arrivals (Poll every 30s) [3, 1]
            self.fetch_arrival_data()

            # 2. Medium-frequency: Status & Accessibility (Poll every 5m)
            if (time.time() - self.last_status_time > self.status_interval):
                self.fetch_system_status()
                self.last_status_time = time.time()

            # 3. Scheduled: Crowding Data
            if ENABLE_CROWDING and (time.time() - self.last_crowding_time > CROWDING_INTERVAL):
                self.fetch_crowding_data()
                self.last_crowding_time = time.time()

            elapsed = time.time() - loop_start
            time.sleep(max(0, POLL_INTERVAL - elapsed))


if __name__ == "__main__":
    storage = LocalFileSystemSink("./landing_zone/tfl")
    # storage = R2Sink(
    #     bucket=os.getenv("R2_BUCKET"),
    #     account_id=os.getenv("R2_ACCOUNT_ID"),
    #     access_key=os.getenv("R2_ACCESS_KEY"),
    #     secret_key=os.getenv("R2_SECRET_KEY"),
    # )
    producer = TfLProducer(sink=storage)
    producer.run()