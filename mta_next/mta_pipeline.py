from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from mta_next.bronze_io import BronzeObjectRef, list_bronze_objects, read_bronze_envelope
from mta_next.config import get_settings
from mta_next.schemas import SILVER_HISTORY_TABLE, SILVER_TABLES
from mta_next.snowflake_io import (
    append_history_rows,
    delete_rows_by_values,
    ensure_tables_exist,
    fetch_max_ingested_at,
    fetch_success_object_keys,
    get_snowflake_connection,
    write_dataframe,
)
from mta_next.transforms_alerts import build_alert_frame
from mta_next.transforms_trip_updates import build_trip_update_frames
from mta_next.transforms_vehicle_positions import build_vehicle_position_frame
from mta_next.utils import json_dumps, parse_iso_timestamp, stable_hash, utc_now


LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


BRONZE_TYPES = ["full_feed", "trip_updates", "vehicle_positions", "alerts"]


def _parse_date_arg(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


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
        "NYCT_FEED_HEADER_JSON": json_dumps(feed_header.get("nyct_feed_header")) if isinstance(feed_header, dict) and feed_header.get("nyct_feed_header") else None,
        "RAW_PAYLOAD_JSON": json_dumps(payload),
    }
    return pd.DataFrame([row])


def _auto_start_date(conn, bronze_type: str) -> Optional[date]:
    settings = get_settings()
    max_ingested_at = fetch_max_ingested_at(conn, settings, SILVER_HISTORY_TABLE, data_type=bronze_type)
    if max_ingested_at is None:
        return None
    return (max_ingested_at.date() - timedelta(days=1))


def _select_pending_objects(
    conn,
    bronze_type: str,
    start_date: Optional[date],
    end_date: Optional[date],
    force: bool,
    max_objects: Optional[int],
) -> List[BronzeObjectRef]:
    settings = get_settings()
    objects = list_bronze_objects(settings, bronze_type, start_date=start_date, end_date=end_date)

    if not force:
        processed_keys = set(
            fetch_success_object_keys(
                conn,
                settings,
                SILVER_HISTORY_TABLE,
                data_type=bronze_type,
                start_date=start_date.isoformat() if start_date else None,
                end_date=end_date.isoformat() if end_date else None,
            )
        )
        objects = [obj for obj in objects if obj.object_key not in processed_keys]

    if max_objects:
        objects = objects[:max_objects]

    return objects


def _write_silver_frames(conn, object_key: str, snapshot_df: pd.DataFrame, trip_df: pd.DataFrame, stop_df: pd.DataFrame, vehicle_df: pd.DataFrame, alert_df: pd.DataFrame) -> Dict[str, int]:
    settings = get_settings()
    for table_name in SILVER_TABLES:
        delete_rows_by_values(conn, settings, table_name, "OBJECT_KEY", [object_key])

    write_dataframe(conn, settings, snapshot_df, "MTA_FEED_SNAPSHOTS_SILVER")
    write_dataframe(conn, settings, trip_df, "MTA_TRIP_UPDATES_SILVER")
    write_dataframe(conn, settings, stop_df, "MTA_STOP_TIME_UPDATES_SILVER")
    write_dataframe(conn, settings, vehicle_df, "MTA_VEHICLE_POSITIONS_SILVER")
    write_dataframe(conn, settings, alert_df, "MTA_ALERTS_SILVER")

    return {
        "MTA_FEED_SNAPSHOTS_SILVER": int(len(snapshot_df)),
        "MTA_TRIP_UPDATES_SILVER": int(len(trip_df)),
        "MTA_STOP_TIME_UPDATES_SILVER": int(len(stop_df)),
        "MTA_VEHICLE_POSITIONS_SILVER": int(len(vehicle_df)),
        "MTA_ALERTS_SILVER": int(len(alert_df)),
    }


def _process_bronze_object(conn, object_key: str, dag_run_id: str) -> Dict[str, Any]:
    settings = get_settings()
    envelope = _normalize_envelope(read_bronze_envelope(settings, object_key))
    metadata = envelope["metadata"]

    snapshot_df = _build_feed_snapshot_frame(envelope, dag_run_id=dag_run_id)
    trip_df, stop_df = build_trip_update_frames(envelope, dag_run_id=dag_run_id)
    vehicle_df = build_vehicle_position_frame(envelope, dag_run_id=dag_run_id)
    alert_df = build_alert_frame(envelope, dag_run_id=dag_run_id)

    table_counts = _write_silver_frames(
        conn,
        object_key=object_key,
        snapshot_df=snapshot_df,
        trip_df=trip_df,
        stop_df=stop_df,
        vehicle_df=vehicle_df,
        alert_df=alert_df,
    )
    row_count = sum(table_counts.values())

    return {
        "OBJECT_KEY": metadata["object_key"],
        "INGESTION_ID": metadata["ingestion_id"],
        "DAG_RUN_ID": dag_run_id,
        "DATA_TYPE": metadata["data_type"],
        "FEED_NAME": metadata["feed_name"],
        "SOURCE_URL": metadata["source_url"],
        "INGESTED_AT": metadata["ingested_at"],
        "STATUS": "SUCCESS",
        "ROW_COUNT": row_count,
        "TABLE_COUNTS_JSON": json_dumps(table_counts),
        "ERROR_MESSAGE": None,
    }


def run_mta_silver_pipeline_once(
    dag_run_id: Optional[str] = None,
    bronze_type: str = "full_feed",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    force: bool = False,
    max_objects: Optional[int] = None,
) -> Dict[str, Any]:
    if bronze_type not in BRONZE_TYPES:
        raise ValueError(f"Unsupported bronze type: {bronze_type}")

    dag_run_id = dag_run_id or f"silver__{utc_now().isoformat()}"
    parsed_start = _parse_date_arg(start_date)
    parsed_end = _parse_date_arg(end_date)

    settings = get_settings()
    conn = get_snowflake_connection(settings)
    try:
        ensure_tables_exist(conn, [SILVER_HISTORY_TABLE, *SILVER_TABLES])

        if not parsed_start and not parsed_end and not force:
            parsed_start = _auto_start_date(conn, bronze_type)
            parsed_end = utc_now().date() if parsed_start else None

        pending_objects = _select_pending_objects(
            conn,
            bronze_type=bronze_type,
            start_date=parsed_start,
            end_date=parsed_end,
            force=force,
            max_objects=max_objects,
        )

        history_rows: List[Dict[str, Any]] = []
        for object_ref in pending_objects:
            try:
                LOGGER.info("Loading bronze object into silver: %s", object_ref.object_key)
                history_rows.append(_process_bronze_object(conn, object_ref.object_key, dag_run_id))
            except Exception as exc:
                LOGGER.exception("Failed silver load for bronze object %s", object_ref.object_key)
                history_rows.append({
                    "OBJECT_KEY": object_ref.object_key,
                    "INGESTION_ID": None,
                    "DAG_RUN_ID": dag_run_id,
                    "DATA_TYPE": bronze_type,
                    "FEED_NAME": None,
                    "SOURCE_URL": None,
                    "INGESTED_AT": object_ref.last_modified,
                    "STATUS": "FAILED",
                    "ROW_COUNT": 0,
                    "TABLE_COUNTS_JSON": json_dumps({}),
                    "ERROR_MESSAGE": str(exc),
                })

        append_history_rows(conn, settings, SILVER_HISTORY_TABLE, history_rows)

        summary = {
            "dag_run_id": dag_run_id,
            "bronze_type": bronze_type,
            "objects_considered": len(pending_objects),
            "objects_succeeded": sum(1 for row in history_rows if row["STATUS"] == "SUCCESS"),
            "objects_failed": sum(1 for row in history_rows if row["STATUS"] == "FAILED"),
        }
        LOGGER.info("MTA silver pipeline summary: %s", summary)
        return summary
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Incrementally load MTA bronze snapshots into silver tables.")
    parser.add_argument("--dag-run-id", help="Optional Airflow DAG run id.")
    parser.add_argument("--bronze-type", choices=BRONZE_TYPES, default="full_feed")
    parser.add_argument("--start-date", help="Inclusive start date filter in YYYY-MM-DD.")
    parser.add_argument("--end-date", help="Inclusive end date filter in YYYY-MM-DD.")
    parser.add_argument("--force", action="store_true", help="Reprocess objects even if silver history already shows success.")
    parser.add_argument("--max-objects", type=int, help="Optional cap for one-shot runs.")
    args = parser.parse_args()

    run_mta_silver_pipeline_once(
        dag_run_id=args.dag_run_id,
        bronze_type=args.bronze_type,
        start_date=args.start_date,
        end_date=args.end_date,
        force=args.force,
        max_objects=args.max_objects,
    )


if __name__ == "__main__":
    main()
