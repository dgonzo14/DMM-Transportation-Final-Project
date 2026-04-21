import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import boto3
import pandas as pd
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

from mta_producer import DataSink, MTAProducer
from mta_producer_to_snowflake import (
    ensure_tables_exist,
    first_route_id_from_alert,
    get_snowflake_connection,
    json_str,
    safe_float,
    safe_int,
    ts_to_dt,
)

# ----------------------------
# Configuration
# ----------------------------

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
env_path = os.path.join(project_root, ".env")
load_dotenv(dotenv_path=env_path, override=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
LOGGER = logging.getLogger(__name__)

SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
MTA_BRONZE_PREFIX = os.getenv("MTA_BRONZE_PREFIX", "bronze/mta").strip("/")
R2_BUCKET = os.getenv("R2_BUCKET")
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY")

RAW_TABLES = [
    "RAW_MTA_PRODUCER_FEED_RUNS",
    "RAW_MTA_PRODUCER_FULL_FEEDS",
    "RAW_MTA_PRODUCER_TRIP_UPDATES",
    "RAW_MTA_PRODUCER_VEHICLE_POSITIONS",
    "RAW_MTA_PRODUCER_ALERTS",
]

CREATE_BRONZE_LOADS_SQL = """
CREATE TABLE IF NOT EXISTS RAW_MTA_BRONZE_FULL_FEED_LOADS (
    OBJECT_KEY STRING,
    INGESTION_ID STRING,
    DAG_RUN_ID STRING,
    FEED_NAME STRING,
    REQUESTED_AT TIMESTAMP_TZ,
    OBJECT_ETAG STRING,
    OBJECT_LAST_MODIFIED TIMESTAMP_TZ,
    SUCCESS BOOLEAN,
    ERROR_MESSAGE STRING,
    INSERTED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);
"""


class NoOpSink(DataSink):
    def write(self, data: Any, metadata: dict) -> None:
        pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_r2_client():
    missing = [
        key for key, value in {
            "R2_BUCKET": R2_BUCKET,
            "R2_ACCOUNT_ID": R2_ACCOUNT_ID,
            "R2_ACCESS_KEY": R2_ACCESS_KEY,
            "R2_SECRET_KEY": R2_SECRET_KEY,
        }.items() if not value
    ]
    if missing:
        raise ValueError(f"Missing R2 config in .env: {missing}")

    return boto3.client(
        "s3",
        endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="auto",
    )


def ensure_loader_table_exists(conn) -> None:
    cur = conn.cursor()
    try:
        cur.execute(CREATE_BRONZE_LOADS_SQL)
    finally:
        cur.close()


def fetch_loaded_object_keys(conn) -> Set[str]:
    cur = conn.cursor()
    try:
        cur.execute(
            f"""
            SELECT DISTINCT OBJECT_KEY
            FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RAW_MTA_BRONZE_FULL_FEED_LOADS
            WHERE SUCCESS = TRUE
            """
        )
        return {row[0] for row in cur.fetchall()}
    finally:
        cur.close()


def build_source_url_lookup() -> Dict[str, str]:
    producer = MTAProducer(sink=NoOpSink())
    return {feed["feed_name"]: feed["url"] for feed in producer.feeds}


def make_ingestion_id(object_key: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, object_key))


def parse_full_feed_key(object_key: str) -> Tuple[str, datetime]:
    prefix = f"{MTA_BRONZE_PREFIX}/full_feed/"
    if not object_key.startswith(prefix):
        raise ValueError(f"Object key is outside the MTA full-feed prefix: {object_key}")

    relative = object_key[len(prefix):]
    parts = relative.split("/")
    if len(parts) != 6:
        raise ValueError(f"Unexpected full-feed object layout: {object_key}")

    feed_name = parts[0].split("=", 1)[1]
    timestamp_token = parts[-1].split("_", 1)[0]
    requested_at = datetime.strptime(timestamp_token, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    return feed_name, requested_at


def list_new_full_feed_objects(r2_client, loaded_keys: Set[str]) -> List[Dict[str, Any]]:
    prefix = f"{MTA_BRONZE_PREFIX}/full_feed/"
    paginator = r2_client.get_paginator("list_objects_v2")
    objects: List[Dict[str, Any]] = []

    for page in paginator.paginate(Bucket=R2_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            object_key = obj["Key"]
            if object_key.endswith("/") or object_key in loaded_keys:
                continue
            objects.append(obj)

    objects.sort(key=lambda obj: obj["Key"])
    return objects


def split_entities(feed_dict: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    trip_updates = []
    vehicle_positions = []
    alerts = []

    for entity_dict in feed_dict.get("entity", []):
        if "trip_update" in entity_dict:
            trip_updates.append(entity_dict)
        elif "vehicle" in entity_dict:
            vehicle_positions.append(entity_dict)
        elif "alert" in entity_dict:
            alerts.append(entity_dict)

    return trip_updates, vehicle_positions, alerts


def delete_existing_rows(conn, ingestion_id: str) -> None:
    cur = conn.cursor()
    try:
        for table_name in RAW_TABLES:
            cur.execute(
                f"""
                DELETE FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name}
                WHERE INGESTION_ID = '{ingestion_id}'
                """
            )
    finally:
        cur.close()


def build_snapshot_rows(
    object_key: str,
    feed_dict: Dict[str, Any],
    source_url_lookup: Dict[str, str],
    dag_run_id: str,
) -> Dict[str, List[Dict[str, Any]]]:
    feed_name, requested_at = parse_full_feed_key(object_key)
    ingestion_id = make_ingestion_id(object_key)
    source_url = source_url_lookup.get(feed_name)

    entities = feed_dict.get("entity", [])
    trip_updates, vehicle_positions, alerts = split_entities(feed_dict)

    feed_timestamp = ts_to_dt(feed_dict.get("header", {}).get("timestamp"))
    entity_count = len(entities)
    trip_update_count = len(trip_updates)
    vehicle_position_count = len(vehicle_positions)
    alert_count = len(alerts)

    run_rows = [{
        "INGESTION_ID": ingestion_id,
        "DAG_RUN_ID": dag_run_id,
        "FEED_NAME": feed_name,
        "SOURCE_URL": source_url,
        "REQUESTED_AT": requested_at,
        "FEED_TIMESTAMP": feed_timestamp,
        "ENTITY_COUNT": entity_count,
        "TRIP_UPDATE_COUNT": trip_update_count,
        "VEHICLE_POSITION_COUNT": vehicle_position_count,
        "ALERT_COUNT": alert_count,
        "SUCCESS": True,
        "ERROR_MESSAGE": None,
    }]

    full_feed_rows = [{
        "INGESTION_ID": ingestion_id,
        "DAG_RUN_ID": dag_run_id,
        "FEED_NAME": feed_name,
        "SOURCE_URL": source_url,
        "REQUESTED_AT": requested_at,
        "FEED_TIMESTAMP": feed_timestamp,
        "ENTITY_COUNT": entity_count,
        "TRIP_UPDATE_COUNT": trip_update_count,
        "VEHICLE_POSITION_COUNT": vehicle_position_count,
        "ALERT_COUNT": alert_count,
        "RAW_FEED_JSON": json_str(feed_dict),
    }]

    trip_update_rows = []
    for entity_dict in trip_updates:
        trip_update = entity_dict.get("trip_update", {})
        trip = trip_update.get("trip", {})
        stop_time_updates = trip_update.get("stop_time_update", [])

        trip_update_rows.append({
            "INGESTION_ID": ingestion_id,
            "DAG_RUN_ID": dag_run_id,
            "FEED_NAME": feed_name,
            "SOURCE_URL": source_url,
            "REQUESTED_AT": requested_at,
            "FEED_TIMESTAMP": feed_timestamp,
            "ENTITY_ID": entity_dict.get("id"),
            "TRIP_ID": trip.get("trip_id"),
            "ROUTE_ID": trip.get("route_id"),
            "START_DATE": trip.get("start_date"),
            "STOP_UPDATE_COUNT": len(stop_time_updates),
            "RAW_TRIP_UPDATE_JSON": json_str(entity_dict),
        })

    vehicle_rows = []
    for entity_dict in vehicle_positions:
        vehicle_wrapper = entity_dict.get("vehicle", {})
        trip = vehicle_wrapper.get("trip", {})
        vehicle = vehicle_wrapper.get("vehicle", {})
        position = vehicle_wrapper.get("position", {})

        vehicle_rows.append({
            "INGESTION_ID": ingestion_id,
            "DAG_RUN_ID": dag_run_id,
            "FEED_NAME": feed_name,
            "SOURCE_URL": source_url,
            "REQUESTED_AT": requested_at,
            "FEED_TIMESTAMP": feed_timestamp,
            "ENTITY_ID": entity_dict.get("id"),
            "VEHICLE_ID": vehicle.get("id"),
            "TRIP_ID": trip.get("trip_id"),
            "ROUTE_ID": trip.get("route_id"),
            "START_DATE": trip.get("start_date"),
            "STOP_ID": vehicle_wrapper.get("stop_id"),
            "CURRENT_STOP_SEQUENCE": safe_int(vehicle_wrapper.get("current_stop_sequence")),
            "CURRENT_STATUS": vehicle_wrapper.get("current_status"),
            "LATITUDE": safe_float(position.get("latitude")),
            "LONGITUDE": safe_float(position.get("longitude")),
            "BEARING": safe_float(position.get("bearing")),
            "RAW_VEHICLE_JSON": json_str(entity_dict),
        })

    alert_rows = []
    for entity_dict in alerts:
        alert_dict = entity_dict.get("alert", {})

        alert_rows.append({
            "INGESTION_ID": ingestion_id,
            "DAG_RUN_ID": dag_run_id,
            "FEED_NAME": feed_name,
            "SOURCE_URL": source_url,
            "REQUESTED_AT": requested_at,
            "FEED_TIMESTAMP": feed_timestamp,
            "ENTITY_ID": entity_dict.get("id"),
            "FIRST_ROUTE_ID": first_route_id_from_alert(alert_dict),
            "INFORMED_ENTITY_COUNT": len(alert_dict.get("informed_entity", [])),
            "ACTIVE_PERIOD_COUNT": len(alert_dict.get("active_period", [])),
            "RAW_ALERT_JSON": json_str(entity_dict),
        })

    return {
        "run_rows": run_rows,
        "full_feed_rows": full_feed_rows,
        "trip_update_rows": trip_update_rows,
        "vehicle_rows": vehicle_rows,
        "alert_rows": alert_rows,
    }


def load_dataframe(conn, df: pd.DataFrame, table_name: str) -> None:
    if df.empty:
        return

    write_pandas(
        conn,
        df,
        table_name,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        quote_identifiers=False,
        use_logical_type=True,
    )


def run_r2_bronze_load(dag_run_id: Optional[str] = None) -> Dict[str, int]:
    dag_run_id = dag_run_id or f"r2__{utc_now().isoformat()}"
    r2_client = get_r2_client()
    source_url_lookup = build_source_url_lookup()

    conn = get_snowflake_connection()
    try:
        ensure_tables_exist(conn)
        ensure_loader_table_exists(conn)

        loaded_keys = fetch_loaded_object_keys(conn)
        pending_objects = list_new_full_feed_objects(r2_client, loaded_keys)
        LOGGER.info("Found %s new MTA bronze full-feed objects to load", len(pending_objects))

        run_rows: List[Dict[str, Any]] = []
        full_feed_rows: List[Dict[str, Any]] = []
        trip_update_rows: List[Dict[str, Any]] = []
        vehicle_rows: List[Dict[str, Any]] = []
        alert_rows: List[Dict[str, Any]] = []
        bronze_load_rows: List[Dict[str, Any]] = []

        for obj in pending_objects:
            object_key = obj["Key"]
            object_last_modified = obj.get("LastModified")
            object_etag = (obj.get("ETag") or "").strip('"')
            ingestion_id = make_ingestion_id(object_key)

            feed_name = None
            requested_at = object_last_modified
            source_url = None

            try:
                feed_name, requested_at = parse_full_feed_key(object_key)
                source_url = source_url_lookup.get(feed_name)
                delete_existing_rows(conn, ingestion_id)

                response = r2_client.get_object(Bucket=R2_BUCKET, Key=object_key)
                payload = response["Body"].read().decode("utf-8")
                feed_dict = json.loads(payload)

                if not isinstance(feed_dict, dict):
                    raise ValueError(f"Expected JSON object for full feed, got {type(feed_dict).__name__}")

                snapshot_rows = build_snapshot_rows(
                    object_key=object_key,
                    feed_dict=feed_dict,
                    source_url_lookup=source_url_lookup,
                    dag_run_id=dag_run_id,
                )

                run_rows.extend(snapshot_rows["run_rows"])
                full_feed_rows.extend(snapshot_rows["full_feed_rows"])
                trip_update_rows.extend(snapshot_rows["trip_update_rows"])
                vehicle_rows.extend(snapshot_rows["vehicle_rows"])
                alert_rows.extend(snapshot_rows["alert_rows"])

                bronze_load_rows.append({
                    "OBJECT_KEY": object_key,
                    "INGESTION_ID": ingestion_id,
                    "DAG_RUN_ID": dag_run_id,
                    "FEED_NAME": feed_name,
                    "REQUESTED_AT": requested_at,
                    "OBJECT_ETAG": object_etag,
                    "OBJECT_LAST_MODIFIED": object_last_modified,
                    "SUCCESS": True,
                    "ERROR_MESSAGE": None,
                })

            except Exception as exc:
                LOGGER.exception("Failed to load bronze object %s", object_key)

                run_rows.append({
                    "INGESTION_ID": ingestion_id,
                    "DAG_RUN_ID": dag_run_id,
                    "FEED_NAME": feed_name,
                    "SOURCE_URL": source_url,
                    "REQUESTED_AT": requested_at,
                    "FEED_TIMESTAMP": None,
                    "ENTITY_COUNT": 0,
                    "TRIP_UPDATE_COUNT": 0,
                    "VEHICLE_POSITION_COUNT": 0,
                    "ALERT_COUNT": 0,
                    "SUCCESS": False,
                    "ERROR_MESSAGE": str(exc),
                })

                bronze_load_rows.append({
                    "OBJECT_KEY": object_key,
                    "INGESTION_ID": ingestion_id,
                    "DAG_RUN_ID": dag_run_id,
                    "FEED_NAME": feed_name,
                    "REQUESTED_AT": requested_at,
                    "OBJECT_ETAG": object_etag,
                    "OBJECT_LAST_MODIFIED": object_last_modified,
                    "SUCCESS": False,
                    "ERROR_MESSAGE": str(exc),
                })

        load_dataframe(conn, pd.DataFrame(run_rows), "RAW_MTA_PRODUCER_FEED_RUNS")
        load_dataframe(conn, pd.DataFrame(full_feed_rows), "RAW_MTA_PRODUCER_FULL_FEEDS")
        load_dataframe(conn, pd.DataFrame(trip_update_rows), "RAW_MTA_PRODUCER_TRIP_UPDATES")
        load_dataframe(conn, pd.DataFrame(vehicle_rows), "RAW_MTA_PRODUCER_VEHICLE_POSITIONS")
        load_dataframe(conn, pd.DataFrame(alert_rows), "RAW_MTA_PRODUCER_ALERTS")
        load_dataframe(conn, pd.DataFrame(bronze_load_rows), "RAW_MTA_BRONZE_FULL_FEED_LOADS")

        summary = {
            "bronze_objects_seen": int(len(pending_objects)),
            "feed_runs_loaded": int(len(run_rows)),
            "full_feeds_loaded": int(len(full_feed_rows)),
            "trip_updates_loaded": int(len(trip_update_rows)),
            "vehicle_positions_loaded": int(len(vehicle_rows)),
            "alerts_loaded": int(len(alert_rows)),
            "successful_bronze_objects": int(sum(1 for row in bronze_load_rows if row["SUCCESS"])),
            "failed_bronze_objects": int(sum(1 for row in bronze_load_rows if not row["SUCCESS"])),
        }
        LOGGER.info("R2 bronze load summary: %s", summary)
        return summary

    finally:
        conn.close()


if __name__ == "__main__":
    print(run_r2_bronze_load())
