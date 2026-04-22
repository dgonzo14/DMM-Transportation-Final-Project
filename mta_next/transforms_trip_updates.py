from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd

from mta_next.utils import (
    extract_direction,
    extract_train_id,
    json_dumps,
    normalize_service_date,
    parse_epoch_timestamp,
    stable_hash,
)


def _trip_update_entities(envelope: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    data_type = envelope["metadata"]["data_type"]
    payload = envelope["payload"]
    if data_type == "full_feed":
        for entity in payload.get("entity", []):
            if "trip_update" in entity:
                yield entity
    elif data_type == "trip_updates":
        for entity in payload:
            yield entity


def build_trip_update_frames(envelope: Dict[str, Any], dag_run_id: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    metadata = envelope["metadata"]
    trip_rows: List[Dict[str, Any]] = []
    stop_rows: List[Dict[str, Any]] = []

    for entity in _trip_update_entities(envelope):
        trip_update = entity.get("trip_update", {})
        trip = trip_update.get("trip", {})
        nyct_trip_descriptor = trip.get("nyct_trip_descriptor")

        route_id = trip.get("route_id")
        trip_id = trip.get("trip_id")
        start_date = normalize_service_date(trip.get("start_date"))
        start_time = trip.get("start_time")
        train_id = extract_train_id(nyct_trip_descriptor)
        direction = extract_direction(nyct_trip_descriptor=nyct_trip_descriptor)
        trip_stable_key = stable_hash(route_id, trip_id, start_date, start_time, train_id, direction)

        trip_rows.append({
            "TRIP_OBSERVATION_KEY": stable_hash(metadata["object_key"], entity.get("id"), trip_id),
            "TRIP_STABLE_KEY": trip_stable_key,
            "OBJECT_KEY": metadata["object_key"],
            "INGESTION_ID": metadata["ingestion_id"],
            "DAG_RUN_ID": dag_run_id,
            "FEED_NAME": metadata["feed_name"],
            "SOURCE_URL": metadata["source_url"],
            "INGESTED_AT": metadata["ingested_at"],
            "FEED_TIMESTAMP": metadata["feed_timestamp"],
            "ENTITY_ID": entity.get("id"),
            "TRIP_ID": trip_id,
            "ROUTE_ID": route_id,
            "START_DATE": start_date,
            "START_TIME": start_time,
            "TRAIN_ID": train_id,
            "DIRECTION": direction,
            "IS_ASSIGNED": trip.get("is_assigned"),
            "SCHEDULE_RELATIONSHIP": trip.get("schedule_relationship"),
            "TRIP_UPDATE_TIMESTAMP": parse_epoch_timestamp(trip_update.get("timestamp")),
            "STOP_TIME_UPDATE_COUNT": len(trip_update.get("stop_time_update", [])),
            "NYCT_TRIP_DESCRIPTOR_JSON": json_dumps(nyct_trip_descriptor) if nyct_trip_descriptor else None,
            "RAW_TRIP_UPDATE_JSON": json_dumps(entity),
        })

        for index, stop_time_update in enumerate(trip_update.get("stop_time_update", [])):
            stop_id = stop_time_update.get("stop_id")
            stop_direction = extract_direction(stop_id=stop_id, nyct_trip_descriptor=nyct_trip_descriptor)
            trip_stop_stable_key = stable_hash(
                route_id,
                trip_id,
                start_date,
                start_time,
                train_id,
                stop_direction,
                stop_id,
            )
            stop_rows.append({
                "STOP_TIME_OBSERVATION_KEY": stable_hash(metadata["object_key"], entity.get("id"), index, stop_id),
                "TRIP_STOP_STABLE_KEY": trip_stop_stable_key,
                "TRIP_STABLE_KEY": trip_stable_key,
                "OBJECT_KEY": metadata["object_key"],
                "INGESTION_ID": metadata["ingestion_id"],
                "DAG_RUN_ID": dag_run_id,
                "FEED_NAME": metadata["feed_name"],
                "SOURCE_URL": metadata["source_url"],
                "INGESTED_AT": metadata["ingested_at"],
                "FEED_TIMESTAMP": metadata["feed_timestamp"],
                "ENTITY_ID": entity.get("id"),
                "TRIP_ID": trip_id,
                "ROUTE_ID": route_id,
                "START_DATE": start_date,
                "START_TIME": start_time,
                "TRAIN_ID": train_id,
                "DIRECTION": stop_direction,
                "STOP_SEQUENCE_INDEX": index,
                "STOP_ID": stop_id,
                "ARRIVAL_TIME": parse_epoch_timestamp(stop_time_update.get("arrival", {}).get("time")),
                "DEPARTURE_TIME": parse_epoch_timestamp(stop_time_update.get("departure", {}).get("time")),
                "ARRIVAL_DELAY_SECONDS": stop_time_update.get("arrival", {}).get("delay"),
                "DEPARTURE_DELAY_SECONDS": stop_time_update.get("departure", {}).get("delay"),
                "SCHEDULE_RELATIONSHIP": stop_time_update.get("schedule_relationship"),
                "NYCT_STOP_TIME_UPDATE_JSON": json_dumps(stop_time_update.get("nyct_stop_time_update"))
                if stop_time_update.get("nyct_stop_time_update")
                else None,
                "RAW_STOP_TIME_UPDATE_JSON": json_dumps(stop_time_update),
            })

    return pd.DataFrame(trip_rows), pd.DataFrame(stop_rows)
