from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Iterable, Iterator, List

import pandas as pd
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from mta_spark_next.mta_schemas import SILVER_HISTORY_TABLE, SILVER_TABLES
from mta_spark_next.transforms_alerts import build_alert_frame
from mta_spark_next.transforms_trip_updates import build_trip_update_frames
from mta_spark_next.transforms_vehicle_positions import build_vehicle_position_frame
from mta_spark_next.utils import json_dumps, parse_iso_timestamp, stable_hash


RECORD_SCHEMA = StructType(
    [
        StructField("table_name", StringType(), False),
        StructField("object_key", StringType(), True),
        StructField("row_json", StringType(), False),
    ]
)


def _string_field(name: str) -> StructField:
    return StructField(name, StringType(), True)


def _number_field(name: str) -> StructField:
    return StructField(name, DoubleType(), True)


def _boolean_field(name: str) -> StructField:
    return StructField(name, BooleanType(), True)


def _timestamp_field(name: str) -> StructField:
    return StructField(name, StringType(), True)


TABLE_JSON_SCHEMAS: Dict[str, StructType] = {
    "MTA_FEED_SNAPSHOTS_SILVER": StructType(
        [
            _string_field("FEED_SNAPSHOT_KEY"),
            _string_field("OBJECT_KEY"),
            _string_field("INGESTION_ID"),
            _string_field("DAG_RUN_ID"),
            _string_field("FEED_NAME"),
            _string_field("SOURCE_URL"),
            _string_field("DATA_TYPE"),
            _timestamp_field("INGESTED_AT"),
            _timestamp_field("FEED_TIMESTAMP"),
            _number_field("ENTITY_COUNT"),
            _number_field("TRIP_UPDATE_COUNT"),
            _number_field("VEHICLE_POSITION_COUNT"),
            _number_field("ALERT_COUNT"),
            _string_field("ROUTE_IDS_JSON"),
            _string_field("FEED_HEADER_JSON"),
            _string_field("NYCT_FEED_HEADER_JSON"),
            _string_field("RAW_PAYLOAD_JSON"),
        ]
    ),
    "MTA_TRIP_UPDATES_SILVER": StructType(
        [
            _string_field("TRIP_OBSERVATION_KEY"),
            _string_field("TRIP_STABLE_KEY"),
            _string_field("OBJECT_KEY"),
            _string_field("INGESTION_ID"),
            _string_field("DAG_RUN_ID"),
            _string_field("FEED_NAME"),
            _string_field("SOURCE_URL"),
            _timestamp_field("INGESTED_AT"),
            _timestamp_field("FEED_TIMESTAMP"),
            _string_field("ENTITY_ID"),
            _string_field("TRIP_ID"),
            _string_field("ROUTE_ID"),
            _string_field("START_DATE"),
            _string_field("START_TIME"),
            _string_field("TRAIN_ID"),
            _string_field("DIRECTION"),
            _boolean_field("IS_ASSIGNED"),
            _string_field("SCHEDULE_RELATIONSHIP"),
            _timestamp_field("TRIP_UPDATE_TIMESTAMP"),
            _number_field("STOP_TIME_UPDATE_COUNT"),
            _string_field("NYCT_TRIP_DESCRIPTOR_JSON"),
            _string_field("RAW_TRIP_UPDATE_JSON"),
        ]
    ),
    "MTA_STOP_TIME_UPDATES_SILVER": StructType(
        [
            _string_field("STOP_TIME_OBSERVATION_KEY"),
            _string_field("TRIP_STOP_STABLE_KEY"),
            _string_field("TRIP_STABLE_KEY"),
            _string_field("OBJECT_KEY"),
            _string_field("INGESTION_ID"),
            _string_field("DAG_RUN_ID"),
            _string_field("FEED_NAME"),
            _string_field("SOURCE_URL"),
            _timestamp_field("INGESTED_AT"),
            _timestamp_field("FEED_TIMESTAMP"),
            _string_field("ENTITY_ID"),
            _string_field("TRIP_ID"),
            _string_field("ROUTE_ID"),
            _string_field("START_DATE"),
            _string_field("START_TIME"),
            _string_field("TRAIN_ID"),
            _string_field("DIRECTION"),
            _number_field("STOP_SEQUENCE_INDEX"),
            _string_field("STOP_ID"),
            _timestamp_field("ARRIVAL_TIME"),
            _timestamp_field("DEPARTURE_TIME"),
            _number_field("ARRIVAL_DELAY_SECONDS"),
            _number_field("DEPARTURE_DELAY_SECONDS"),
            _string_field("SCHEDULE_RELATIONSHIP"),
            _string_field("NYCT_STOP_TIME_UPDATE_JSON"),
            _string_field("RAW_STOP_TIME_UPDATE_JSON"),
        ]
    ),
    "MTA_VEHICLE_POSITIONS_SILVER": StructType(
        [
            _string_field("VEHICLE_OBSERVATION_KEY"),
            _string_field("TRIP_STABLE_KEY"),
            _string_field("OBJECT_KEY"),
            _string_field("INGESTION_ID"),
            _string_field("DAG_RUN_ID"),
            _string_field("FEED_NAME"),
            _string_field("SOURCE_URL"),
            _timestamp_field("INGESTED_AT"),
            _timestamp_field("FEED_TIMESTAMP"),
            _string_field("ENTITY_ID"),
            _string_field("VEHICLE_ID"),
            _string_field("TRIP_ID"),
            _string_field("ROUTE_ID"),
            _string_field("START_DATE"),
            _string_field("START_TIME"),
            _string_field("TRAIN_ID"),
            _string_field("DIRECTION"),
            _string_field("STOP_ID"),
            _number_field("CURRENT_STOP_SEQUENCE"),
            _string_field("CURRENT_STATUS"),
            _timestamp_field("VEHICLE_TIMESTAMP"),
            _number_field("LATITUDE"),
            _number_field("LONGITUDE"),
            _number_field("BEARING"),
            _string_field("RAW_VEHICLE_JSON"),
        ]
    ),
    "MTA_ALERTS_SILVER": StructType(
        [
            _string_field("ALERT_OBSERVATION_KEY"),
            _string_field("OBJECT_KEY"),
            _string_field("INGESTION_ID"),
            _string_field("DAG_RUN_ID"),
            _string_field("FEED_NAME"),
            _string_field("SOURCE_URL"),
            _timestamp_field("INGESTED_AT"),
            _timestamp_field("FEED_TIMESTAMP"),
            _string_field("ENTITY_ID"),
            _string_field("ROUTE_ID"),
            _string_field("STOP_ID"),
            _string_field("ALERT_CAUSE"),
            _string_field("ALERT_EFFECT"),
            _timestamp_field("ACTIVE_PERIOD_START"),
            _timestamp_field("ACTIVE_PERIOD_END"),
            _number_field("INFORMED_ENTITY_COUNT"),
            _string_field("HEADER_TEXT"),
            _string_field("DESCRIPTION_TEXT"),
            _string_field("INFORMED_ENTITIES_JSON"),
            _string_field("RAW_ALERT_JSON"),
        ]
    ),
    SILVER_HISTORY_TABLE: StructType(
        [
            _string_field("OBJECT_KEY"),
            _string_field("INGESTION_ID"),
            _string_field("DAG_RUN_ID"),
            _string_field("DATA_TYPE"),
            _string_field("FEED_NAME"),
            _string_field("SOURCE_URL"),
            _timestamp_field("INGESTED_AT"),
            _string_field("STATUS"),
            _number_field("ROW_COUNT"),
            _string_field("TABLE_COUNTS_JSON"),
            _string_field("ERROR_MESSAGE"),
        ]
    ),
}


TIMESTAMP_COLUMNS: Dict[str, List[str]] = {
    "MTA_FEED_SNAPSHOTS_SILVER": ["INGESTED_AT", "FEED_TIMESTAMP"],
    "MTA_TRIP_UPDATES_SILVER": ["INGESTED_AT", "FEED_TIMESTAMP", "TRIP_UPDATE_TIMESTAMP"],
    "MTA_STOP_TIME_UPDATES_SILVER": [
        "INGESTED_AT",
        "FEED_TIMESTAMP",
        "ARRIVAL_TIME",
        "DEPARTURE_TIME",
    ],
    "MTA_VEHICLE_POSITIONS_SILVER": ["INGESTED_AT", "FEED_TIMESTAMP", "VEHICLE_TIMESTAMP"],
    "MTA_ALERTS_SILVER": [
        "INGESTED_AT",
        "FEED_TIMESTAMP",
        "ACTIVE_PERIOD_START",
        "ACTIVE_PERIOD_END",
    ],
    SILVER_HISTORY_TABLE: ["INGESTED_AT"],
}


LONG_COLUMNS: Dict[str, List[str]] = {
    "MTA_FEED_SNAPSHOTS_SILVER": [
        "ENTITY_COUNT",
        "TRIP_UPDATE_COUNT",
        "VEHICLE_POSITION_COUNT",
        "ALERT_COUNT",
    ],
    "MTA_TRIP_UPDATES_SILVER": ["STOP_TIME_UPDATE_COUNT"],
    "MTA_STOP_TIME_UPDATES_SILVER": [
        "STOP_SEQUENCE_INDEX",
        "ARRIVAL_DELAY_SECONDS",
        "DEPARTURE_DELAY_SECONDS",
    ],
    "MTA_VEHICLE_POSITIONS_SILVER": ["CURRENT_STOP_SEQUENCE"],
    "MTA_ALERTS_SILVER": ["INFORMED_ENTITY_COUNT"],
    SILVER_HISTORY_TABLE: ["ROW_COUNT"],
}


DOUBLE_COLUMNS: Dict[str, List[str]] = {
    "MTA_VEHICLE_POSITIONS_SILVER": ["LATITUDE", "LONGITUDE", "BEARING"],
}


TABLE_WRITE_ORDER = [*SILVER_TABLES, SILVER_HISTORY_TABLE]


def _normalize_envelope(envelope: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(envelope)
    metadata = dict(normalized.get("metadata", {}))
    metadata["ingested_at"] = parse_iso_timestamp(metadata.get("ingested_at"))
    metadata["feed_timestamp"] = parse_iso_timestamp(metadata.get("feed_timestamp"))
    normalized["metadata"] = metadata
    return normalized


def _build_feed_snapshot_frame(envelope: Dict[str, Any], dag_run_id: str) -> pd.DataFrame:
    metadata = envelope["metadata"]
    payload = envelope["payload"]
    data_type = metadata["data_type"]

    entity_count = 0
    trip_update_count = 0
    vehicle_position_count = 0
    alert_count = 0

    if data_type == "full_feed":
        entity_count = len(payload.get("entity", []))
        trip_update_count = sum(1 for entity in payload.get("entity", []) if "trip_update" in entity)
        vehicle_position_count = sum(1 for entity in payload.get("entity", []) if "vehicle" in entity)
        alert_count = sum(1 for entity in payload.get("entity", []) if "alert" in entity)
        feed_header = payload.get("header")
    else:
        entity_count = len(payload)
        feed_header = None
        if data_type == "trip_updates":
            trip_update_count = len(payload)
        elif data_type == "vehicle_positions":
            vehicle_position_count = len(payload)
        elif data_type == "alerts":
            alert_count = len(payload)

    row = {
        "FEED_SNAPSHOT_KEY": stable_hash(metadata["object_key"], data_type),
        "OBJECT_KEY": metadata["object_key"],
        "INGESTION_ID": metadata["ingestion_id"],
        "DAG_RUN_ID": dag_run_id,
        "FEED_NAME": metadata["feed_name"],
        "SOURCE_URL": metadata["source_url"],
        "DATA_TYPE": data_type,
        "INGESTED_AT": metadata["ingested_at"],
        "FEED_TIMESTAMP": metadata["feed_timestamp"],
        "ENTITY_COUNT": entity_count,
        "TRIP_UPDATE_COUNT": trip_update_count,
        "VEHICLE_POSITION_COUNT": vehicle_position_count,
        "ALERT_COUNT": alert_count,
        "ROUTE_IDS_JSON": json_dumps(metadata.get("route_ids", [])),
        "FEED_HEADER_JSON": json_dumps(feed_header) if feed_header else None,
        "NYCT_FEED_HEADER_JSON": json_dumps(feed_header.get("nyct_feed_header"))
        if isinstance(feed_header, dict) and feed_header.get("nyct_feed_header")
        else None,
        "RAW_PAYLOAD_JSON": json_dumps(payload),
    }
    return pd.DataFrame([row])


def _clean_value(value: Any) -> Any:
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    return value


def _record_json(row: Dict[str, Any]) -> str:
    cleaned = {key: _clean_value(value) for key, value in row.items()}
    return json.dumps(cleaned, ensure_ascii=False, sort_keys=True)


def _frame_records(table_name: str, object_key: str, frame: pd.DataFrame) -> Iterator[tuple[str, str, str]]:
    for row in frame.to_dict("records"):
        yield (table_name, object_key, _record_json(row))


def _success_history_row(
    metadata: Dict[str, Any],
    dag_run_id: str,
    table_counts: Dict[str, int],
) -> Dict[str, Any]:
    return {
        "OBJECT_KEY": metadata["object_key"],
        "INGESTION_ID": metadata["ingestion_id"],
        "DAG_RUN_ID": dag_run_id,
        "DATA_TYPE": metadata["data_type"],
        "FEED_NAME": metadata["feed_name"],
        "SOURCE_URL": metadata["source_url"],
        "INGESTED_AT": metadata["ingested_at"],
        "STATUS": "SUCCESS",
        "ROW_COUNT": sum(table_counts.values()),
        "TABLE_COUNTS_JSON": json_dumps(table_counts),
        "ERROR_MESSAGE": None,
    }


def _failed_history_row(raw_json: str, dag_run_id: str, exc: Exception) -> Dict[str, Any]:
    metadata: Dict[str, Any] = {}
    try:
        parsed = json.loads(raw_json)
        if isinstance(parsed, dict) and isinstance(parsed.get("metadata"), dict):
            metadata = parsed["metadata"]
    except Exception:
        metadata = {}

    return {
        "OBJECT_KEY": metadata.get("object_key"),
        "INGESTION_ID": metadata.get("ingestion_id"),
        "DAG_RUN_ID": dag_run_id,
        "DATA_TYPE": metadata.get("data_type"),
        "FEED_NAME": metadata.get("feed_name"),
        "SOURCE_URL": metadata.get("source_url"),
        "INGESTED_AT": parse_iso_timestamp(metadata.get("ingested_at")),
        "STATUS": "FAILED",
        "ROW_COUNT": 0,
        "TABLE_COUNTS_JSON": json_dumps({}),
        "ERROR_MESSAGE": str(exc),
    }


def build_silver_records(raw_json: str, dag_run_id: str) -> Iterator[tuple[str, str | None, str]]:
    try:
        envelope = _normalize_envelope(json.loads(raw_json))
        metadata = envelope["metadata"]
        object_key = metadata["object_key"]

        snapshot_df = _build_feed_snapshot_frame(envelope, dag_run_id=dag_run_id)
        trip_df, stop_df = build_trip_update_frames(envelope, dag_run_id=dag_run_id)
        vehicle_df = build_vehicle_position_frame(envelope, dag_run_id=dag_run_id)
        alert_df = build_alert_frame(envelope, dag_run_id=dag_run_id)

        frames = {
            "MTA_FEED_SNAPSHOTS_SILVER": snapshot_df,
            "MTA_TRIP_UPDATES_SILVER": trip_df,
            "MTA_STOP_TIME_UPDATES_SILVER": stop_df,
            "MTA_VEHICLE_POSITIONS_SILVER": vehicle_df,
            "MTA_ALERTS_SILVER": alert_df,
        }
        table_counts = {table_name: int(len(frame)) for table_name, frame in frames.items()}

        for table_name in SILVER_TABLES:
            yield from _frame_records(table_name, object_key, frames[table_name])

        history = _success_history_row(metadata, dag_run_id=dag_run_id, table_counts=table_counts)
        yield (SILVER_HISTORY_TABLE, object_key, _record_json(history))
    except Exception as exc:
        history = _failed_history_row(raw_json, dag_run_id=dag_run_id, exc=exc)
        yield (SILVER_HISTORY_TABLE, history.get("OBJECT_KEY"), _record_json(history))


def partition_silver_records(rows: Iterable[Any], dag_run_id: str) -> Iterator[tuple[str, str | None, str]]:
    for row in rows:
        yield from build_silver_records(row.value, dag_run_id=dag_run_id)
