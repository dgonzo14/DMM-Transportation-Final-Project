from __future__ import annotations

from typing import Any, Dict, Iterable, List

import pandas as pd

from mta_spark_next.utils import (
    extract_direction,
    extract_train_id,
    json_dumps,
    normalize_service_date,
    parse_epoch_timestamp,
    stable_hash,
)


def _vehicle_entities(envelope: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    data_type = envelope["metadata"]["data_type"]
    payload = envelope["payload"]
    if data_type == "full_feed":
        for entity in payload.get("entity", []):
            if "vehicle" in entity:
                yield entity
    elif data_type == "vehicle_positions":
        for entity in payload:
            yield entity


def build_vehicle_position_frame(envelope: Dict[str, Any], dag_run_id: str) -> pd.DataFrame:
    metadata = envelope["metadata"]
    rows: List[Dict[str, Any]] = []

    for entity in _vehicle_entities(envelope):
        vehicle = entity.get("vehicle", {})
        trip = vehicle.get("trip", {})
        nyct_trip_descriptor = trip.get("nyct_trip_descriptor")
        stop_id = vehicle.get("stop_id")
        route_id = trip.get("route_id")
        trip_id = trip.get("trip_id")
        start_date = normalize_service_date(trip.get("start_date"))
        start_time = trip.get("start_time")
        train_id = extract_train_id(nyct_trip_descriptor)
        direction = extract_direction(stop_id=stop_id, nyct_trip_descriptor=nyct_trip_descriptor)

        rows.append({
            "VEHICLE_OBSERVATION_KEY": stable_hash(metadata["object_key"], entity.get("id"), vehicle.get("vehicle", {}).get("id")),
            "TRIP_STABLE_KEY": stable_hash(route_id, trip_id, start_date, start_time, train_id, direction),
            "OBJECT_KEY": metadata["object_key"],
            "INGESTION_ID": metadata["ingestion_id"],
            "DAG_RUN_ID": dag_run_id,
            "FEED_NAME": metadata["feed_name"],
            "SOURCE_URL": metadata["source_url"],
            "INGESTED_AT": metadata["ingested_at"],
            "FEED_TIMESTAMP": metadata["feed_timestamp"],
            "ENTITY_ID": entity.get("id"),
            "VEHICLE_ID": vehicle.get("vehicle", {}).get("id"),
            "TRIP_ID": trip_id,
            "ROUTE_ID": route_id,
            "START_DATE": start_date,
            "START_TIME": start_time,
            "TRAIN_ID": train_id,
            "DIRECTION": direction,
            "STOP_ID": stop_id,
            "CURRENT_STOP_SEQUENCE": vehicle.get("current_stop_sequence"),
            "CURRENT_STATUS": vehicle.get("current_status"),
            "VEHICLE_TIMESTAMP": parse_epoch_timestamp(vehicle.get("timestamp")),
            "LATITUDE": vehicle.get("position", {}).get("latitude"),
            "LONGITUDE": vehicle.get("position", {}).get("longitude"),
            "BEARING": vehicle.get("position", {}).get("bearing"),
            "RAW_VEHICLE_JSON": json_dumps(entity),
        })

    return pd.DataFrame(rows)
