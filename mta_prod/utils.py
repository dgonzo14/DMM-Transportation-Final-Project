from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

import pandas as pd


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def stable_hash(*parts: Any) -> str:
    payload = "|".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def parse_epoch_timestamp(value: Any) -> Optional[datetime]:
    if value in (None, "", 0, "0"):
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except Exception:
        return None


def parse_iso_timestamp(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    try:
        ts = pd.to_datetime(value, utc=True)
        if pd.isna(ts):
            return None
        return ts.to_pydatetime()
    except Exception:
        return None


def normalize_service_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:]}"
    return value


def first_translation_text(block: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(block, dict):
        return None
    translations = block.get("translation", [])
    if not translations:
        return None
    text = translations[0].get("text")
    return str(text) if text else None


def extract_direction(
    stop_id: Optional[str] = None,
    nyct_trip_descriptor: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    if isinstance(nyct_trip_descriptor, dict):
        direction = nyct_trip_descriptor.get("direction")
        if direction not in (None, ""):
            return str(direction)
    if stop_id:
        if stop_id.endswith("N"):
            return "N"
        if stop_id.endswith("S"):
            return "S"
    return None


def extract_train_id(nyct_trip_descriptor: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(nyct_trip_descriptor, dict):
        return None
    train_id = nyct_trip_descriptor.get("train_id")
    return str(train_id) if train_id not in (None, "") else None


def extract_route_ids_from_entity(entity: Dict[str, Any]) -> Iterable[str]:
    route_ids = set()

    trip_update = entity.get("trip_update", {})
    trip = trip_update.get("trip", {})
    route_id = trip.get("route_id")
    if route_id:
        route_ids.add(str(route_id))

    vehicle = entity.get("vehicle", {})
    vehicle_trip = vehicle.get("trip", {})
    route_id = vehicle_trip.get("route_id")
    if route_id:
        route_ids.add(str(route_id))

    alert = entity.get("alert", {})
    for informed_entity in alert.get("informed_entity", []):
        route_id = informed_entity.get("route_id")
        if route_id:
            route_ids.add(str(route_id))

    return sorted(route_ids)
