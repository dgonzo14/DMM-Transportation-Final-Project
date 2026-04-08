import os
import json
import time
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

# --- Configuration ---

script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
env_path = project_root / ".env"
load_dotenv(dotenv_path=env_path, override=True)

POLL_INTERVAL = int(os.getenv("MTA_POLL_INTERVAL", "30"))
MTA_SUBWAY_FEEDS_FILE = os.getenv("MTA_SUBWAY_FEEDS_FILE", "feeds.json")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class DataSink(ABC):
    @abstractmethod
    def write(self, data: Any, metadata: dict) -> None:
        pass


class LocalFileSystemSink(DataSink):
    def __init__(self, base_path: str):
        self.base_path = base_path

    def write(self, data: Any, metadata: dict) -> None:
        data_type = metadata["data_type"]
        feed_name = metadata["feed_name"]
        timestamp = metadata["timestamp"].strftime("%Y%m%d_%H%M%S")

        out_dir = os.path.join(self.base_path, data_type, feed_name)
        os.makedirs(out_dir, exist_ok=True)

        filename = f"{feed_name}_{data_type}_{timestamp}.json"
        filepath = os.path.join(out_dir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)


class MTAProducer:
    def __init__(self, sink: DataSink):
        self.sink = sink
        self.headers = {"Cache-Control": "no-cache"}
        self.feeds = self._load_feeds()

    def _load_feeds(self) -> List[Dict[str, str]]:
        feeds_path = Path(MTA_SUBWAY_FEEDS_FILE)

        if not feeds_path.is_absolute():
            feeds_path = project_root / feeds_path

        if not feeds_path.exists():
            raise FileNotFoundError(
                f"MTA feeds file not found: {feeds_path}. "
                "Set MTA_SUBWAY_FEEDS_FILE in .env to a valid JSON file."
            )

        try:
            with open(feeds_path, "r", encoding="utf-8") as f:
                feeds = json.load(f)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Feeds file contains invalid JSON: {exc}") from exc
        except OSError as exc:
            raise ValueError(f"Could not read feeds file {feeds_path}: {exc}") from exc

        if not isinstance(feeds, list) or not feeds:
            raise ValueError("Feeds file must contain a non-empty JSON list")

        for i, feed in enumerate(feeds):
            if not isinstance(feed, dict):
                raise ValueError(f"Feed entry at index {i} must be an object")
            if "feed_name" not in feed or "url" not in feed:
                raise ValueError(f"Feed entry at index {i} must contain 'feed_name' and 'url'")

        logging.info("Loaded %s MTA feeds from %s", len(feeds), feeds_path)
        return feeds

    def _get_bytes(self, url: str) -> Optional[bytes]:
        try:
            response = requests.get(url, headers=self.headers, timeout=20)
            response.raise_for_status()
            return response.content
        except Exception as e:
            logging.error("Error fetching %s: %s", url, e)
            return None

    def _parse_feed(self, raw_bytes: bytes) -> gtfs_realtime_pb2.FeedMessage:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(raw_bytes)
        return feed

    def _feed_to_dict(self, feed: gtfs_realtime_pb2.FeedMessage) -> dict:
        return MessageToDict(feed, preserving_proto_field_name=True)

    def _split_entities(self, feed: gtfs_realtime_pb2.FeedMessage):
        trip_updates = []
        vehicle_positions = []
        alerts = []

        for entity in feed.entity:
            entity_dict = MessageToDict(entity, preserving_proto_field_name=True)
            if entity.HasField("trip_update"):
                trip_updates.append(entity_dict)
            elif entity.HasField("vehicle"):
                vehicle_positions.append(entity_dict)
            elif entity.HasField("alert"):
                alerts.append(entity_dict)

        return trip_updates, vehicle_positions, alerts

    def fetch_all(self) -> None:
        now = datetime.now()

        for feed_cfg in self.feeds:
            feed_name = feed_cfg["feed_name"]
            url = feed_cfg["url"]

            raw_bytes = self._get_bytes(url)
            if not raw_bytes:
                continue

            try:
                feed = self._parse_feed(raw_bytes)
                feed_dict = self._feed_to_dict(feed)
                trip_updates, vehicle_positions, alerts = self._split_entities(feed)

                self.sink.write(
                    feed_dict,
                    {
                        "feed_name": feed_name,
                        "data_type": "full_feed",
                        "timestamp": now,
                    },
                )

                self.sink.write(
                    trip_updates,
                    {
                        "feed_name": feed_name,
                        "data_type": "trip_updates",
                        "timestamp": now,
                    },
                )

                self.sink.write(
                    vehicle_positions,
                    {
                        "feed_name": feed_name,
                        "data_type": "vehicle_positions",
                        "timestamp": now,
                    },
                )

                self.sink.write(
                    alerts,
                    {
                        "feed_name": feed_name,
                        "data_type": "alerts",
                        "timestamp": now,
                    },
                )

                logging.info(
                    "Fetched %s: entities=%s trip_updates=%s vehicle_positions=%s alerts=%s",
                    feed_name,
                    len(feed.entity),
                    len(trip_updates),
                    len(vehicle_positions),
                    len(alerts),
                )

            except Exception as e:
                logging.error("Error parsing feed %s: %s", feed_name, e)

    def run(self) -> None:
        while True:
            loop_start = time.time()
            self.fetch_all()
            elapsed = time.time() - loop_start
            time.sleep(max(0, POLL_INTERVAL - elapsed))


if __name__ == "__main__":
    storage = LocalFileSystemSink("./landing_zone/mta")
    producer = MTAProducer(sink=storage)
    producer.run()