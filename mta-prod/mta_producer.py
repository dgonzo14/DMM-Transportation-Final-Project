import os
import json
import time
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import boto3
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
MTA_SUBWAY_FEEDS_FILE = os.getenv("MTA_SUBWAY_FEEDS_FILE") or os.getenv("MTA_SUBWAY_FEEDS_JSON", "feeds.json")
MTA_SINK_BACKEND = os.getenv("MTA_SINK_BACKEND", "local").strip().lower()
MTA_LOCAL_BASE_PATH = os.getenv("MTA_LOCAL_BASE_PATH", "./landing_zone")
MTA_BRONZE_PREFIX = os.getenv("MTA_BRONZE_PREFIX", "bronze/mta").strip("/")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def build_bronze_object_key(metadata: dict) -> str:
    feed_name = metadata["feed_name"]
    data_type = metadata["data_type"]
    timestamp = metadata["timestamp"]
    timestamp_token = timestamp.strftime("%Y%m%dT%H%M%SZ")
    return (
        f"{MTA_BRONZE_PREFIX}/{data_type}/feed_name={feed_name}/"
        f"year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}/hour={timestamp.hour:02d}/"
        f"{timestamp_token}_{feed_name}_{data_type}.json"
    )


class DataSink(ABC):
    @abstractmethod
    def write(self, data: Any, metadata: dict) -> None:
        pass


class LocalFileSystemSink(DataSink):
    def __init__(self, base_path: str):
        self.base_path = base_path

    def write(self, data: Any, metadata: dict) -> None:
        relative_path = build_bronze_object_key(metadata)
        filepath = os.path.join(self.base_path, *relative_path.split("/"))
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)


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

    def write(self, data: Any, metadata: dict) -> None:
        key = build_bronze_object_key(metadata)
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )


def build_sink_from_env() -> DataSink:
    if MTA_SINK_BACKEND == "local":
        return LocalFileSystemSink(MTA_LOCAL_BASE_PATH)

    if MTA_SINK_BACKEND == "r2":
        required_env = {
            "R2_BUCKET": os.getenv("R2_BUCKET"),
            "R2_ACCOUNT_ID": os.getenv("R2_ACCOUNT_ID"),
            "R2_ACCESS_KEY": os.getenv("R2_ACCESS_KEY"),
            "R2_SECRET_KEY": os.getenv("R2_SECRET_KEY"),
        }
        missing = [key for key, value in required_env.items() if not value]
        if missing:
            raise ValueError(f"Missing R2 config in .env: {missing}")

        return R2Sink(
            bucket=required_env["R2_BUCKET"],
            account_id=required_env["R2_ACCOUNT_ID"],
            access_key=required_env["R2_ACCESS_KEY"],
            secret_key=required_env["R2_SECRET_KEY"],
        )

    raise ValueError("MTA_SINK_BACKEND must be either 'local' or 'r2'")


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
        now = utc_now()

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
    storage = build_sink_from_env()
    producer = MTAProducer(sink=storage)
    producer.run()
