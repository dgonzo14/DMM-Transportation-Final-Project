from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import boto3

from mta_prod.config import Settings


@dataclass(frozen=True)
class BronzeObjectRef:
    object_key: str
    last_modified: Optional[datetime]


class BronzeSink(ABC):
    @abstractmethod
    def write_json(self, object_key: str, payload: Dict[str, Any]) -> None:
        raise NotImplementedError


class LocalBronzeSink(BronzeSink):
    def __init__(self, base_path: Path):
        self.base_path = base_path

    def write_json(self, object_key: str, payload: Dict[str, Any]) -> None:
        output_path = self.base_path / Path(object_key)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False)


class R2BronzeSink(BronzeSink):
    def __init__(self, settings: Settings):
        missing = [
            key for key, value in {
                "R2_BUCKET": settings.r2_bucket,
                "R2_ACCOUNT_ID": settings.r2_account_id,
                "R2_ACCESS_KEY": settings.r2_access_key,
                "R2_SECRET_KEY": settings.r2_secret_key,
            }.items() if not value
        ]
        if missing:
            raise ValueError(f"Missing R2 configuration for bronze storage: {missing}")

        self.bucket = settings.r2_bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=settings.r2_endpoint_url,
            aws_access_key_id=settings.r2_access_key,
            aws_secret_access_key=settings.r2_secret_key,
            region_name="auto",
        )

    def write_json(self, object_key: str, payload: Dict[str, Any]) -> None:
        self.client.put_object(
            Bucket=self.bucket,
            Key=object_key,
            Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
        )


def build_bronze_sink(settings: Settings) -> BronzeSink:
    if settings.mta_sink_backend == "local":
        return LocalBronzeSink(settings.mta_local_base_path)
    return R2BronzeSink(settings)


def build_bronze_object_key(
    settings: Settings,
    data_type: str,
    ingested_at: datetime,
    feed_name: str,
    ingestion_id: str,
) -> str:
    timestamp = ingested_at.astimezone(timezone.utc)
    safe_feed_name = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in feed_name)
    return (
        f"{settings.mta_bronze_prefix}/{data_type}/"
        f"year={timestamp.year}/month={timestamp.month:02d}/day={timestamp.day:02d}/hour={timestamp.hour:02d}/"
        f"{timestamp.strftime('%Y%m%dT%H%M%SZ')}_{safe_feed_name}_{ingestion_id}.json"
    )


def _date_prefixes(settings: Settings, data_type: str, start_date: Optional[date], end_date: Optional[date]) -> List[str]:
    if not start_date and not end_date:
        return [f"{settings.mta_bronze_prefix}/{data_type}/"]

    if start_date and not end_date:
        end_date = start_date
    if end_date and not start_date:
        start_date = end_date
    if start_date is None or end_date is None:
        return [f"{settings.mta_bronze_prefix}/{data_type}/"]

    prefixes: List[str] = []
    current = start_date
    while current <= end_date:
        prefixes.append(
            f"{settings.mta_bronze_prefix}/{data_type}/"
            f"year={current.year}/month={current.month:02d}/day={current.day:02d}/"
        )
        current += timedelta(days=1)
    return prefixes


def list_bronze_objects(
    settings: Settings,
    data_type: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
) -> List[BronzeObjectRef]:
    prefixes = _date_prefixes(settings, data_type, start_date, end_date)
    objects: List[BronzeObjectRef] = []

    if settings.mta_sink_backend == "local":
        base_path = settings.mta_local_base_path
        for prefix in prefixes:
            prefix_path = base_path / Path(prefix)
            if not prefix_path.exists():
                continue
            for path in prefix_path.rglob("*.json"):
                object_key = path.relative_to(base_path).as_posix()
                last_modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
                objects.append(BronzeObjectRef(object_key=object_key, last_modified=last_modified))
        objects.sort(key=lambda item: item.object_key)
        return objects

    sink = R2BronzeSink(settings)
    paginator = sink.client.get_paginator("list_objects_v2")
    for prefix in prefixes:
        for page in paginator.paginate(Bucket=settings.r2_bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith("/") or not key.endswith(".json"):
                    continue
                objects.append(BronzeObjectRef(object_key=key, last_modified=obj.get("LastModified")))
    objects.sort(key=lambda item: item.object_key)
    return objects


def read_bronze_envelope(settings: Settings, object_key: str) -> Dict[str, Any]:
    if settings.mta_sink_backend == "local":
        path = settings.mta_local_base_path / Path(object_key)
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    sink = R2BronzeSink(settings)
    response = sink.client.get_object(Bucket=settings.r2_bucket, Key=object_key)
    payload = response["Body"].read().decode("utf-8")
    return json.loads(payload)
