import json
import logging
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

from mta_producer import MTAProducer, DataSink

# ----------------------------
# Configuration
# ----------------------------

script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
env_path = project_root / ".env"
load_dotenv(dotenv_path=env_path, override=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
LOGGER = logging.getLogger(__name__)

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_PRIVATE_KEY_FILE = os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE")
SNOWFLAKE_PRIVATE_KEY_FILE_PWD = os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PWD")

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS RAW_MTA_PRODUCER_FEED_RUNS (
    INGESTION_ID STRING,
    DAG_RUN_ID STRING,
    FEED_NAME STRING,
    SOURCE_URL STRING,
    REQUESTED_AT TIMESTAMP_TZ,
    FEED_TIMESTAMP TIMESTAMP_TZ,
    ENTITY_COUNT NUMBER,
    TRIP_UPDATE_COUNT NUMBER,
    VEHICLE_POSITION_COUNT NUMBER,
    ALERT_COUNT NUMBER,
    SUCCESS BOOLEAN,
    ERROR_MESSAGE STRING,
    INSERTED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_MTA_PRODUCER_FULL_FEEDS (
    INGESTION_ID STRING,
    DAG_RUN_ID STRING,
    FEED_NAME STRING,
    SOURCE_URL STRING,
    REQUESTED_AT TIMESTAMP_TZ,
    FEED_TIMESTAMP TIMESTAMP_TZ,
    ENTITY_COUNT NUMBER,
    TRIP_UPDATE_COUNT NUMBER,
    VEHICLE_POSITION_COUNT NUMBER,
    ALERT_COUNT NUMBER,
    RAW_FEED_JSON STRING,
    INSERTED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_MTA_PRODUCER_TRIP_UPDATES (
    INGESTION_ID STRING,
    DAG_RUN_ID STRING,
    FEED_NAME STRING,
    SOURCE_URL STRING,
    REQUESTED_AT TIMESTAMP_TZ,
    FEED_TIMESTAMP TIMESTAMP_TZ,
    ENTITY_ID STRING,
    TRIP_ID STRING,
    ROUTE_ID STRING,
    START_DATE STRING,
    STOP_UPDATE_COUNT NUMBER,
    RAW_TRIP_UPDATE_JSON STRING,
    INSERTED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_MTA_PRODUCER_VEHICLE_POSITIONS (
    INGESTION_ID STRING,
    DAG_RUN_ID STRING,
    FEED_NAME STRING,
    SOURCE_URL STRING,
    REQUESTED_AT TIMESTAMP_TZ,
    FEED_TIMESTAMP TIMESTAMP_TZ,
    ENTITY_ID STRING,
    VEHICLE_ID STRING,
    TRIP_ID STRING,
    ROUTE_ID STRING,
    START_DATE STRING,
    STOP_ID STRING,
    CURRENT_STOP_SEQUENCE NUMBER,
    CURRENT_STATUS STRING,
    LATITUDE FLOAT,
    LONGITUDE FLOAT,
    BEARING FLOAT,
    RAW_VEHICLE_JSON STRING,
    INSERTED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_MTA_PRODUCER_ALERTS (
    INGESTION_ID STRING,
    DAG_RUN_ID STRING,
    FEED_NAME STRING,
    SOURCE_URL STRING,
    REQUESTED_AT TIMESTAMP_TZ,
    FEED_TIMESTAMP TIMESTAMP_TZ,
    ENTITY_ID STRING,
    FIRST_ROUTE_ID STRING,
    INFORMED_ENTITY_COUNT NUMBER,
    ACTIVE_PERIOD_COUNT NUMBER,
    RAW_ALERT_JSON STRING,
    INSERTED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);
"""


class NoOpSink(DataSink):
    def write(self, data: Any, metadata: dict) -> None:
        pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ts_to_dt(value: Optional[Any]) -> Optional[datetime]:
    if value in (None, "", 0, "0"):
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except Exception:
        return None


def safe_int(value: Optional[Any]) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def safe_float(value: Optional[Any]) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def json_str(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)


def get_snowflake_connection():
    missing = [
        key for key, value in {
            "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
            "SNOWFLAKE_USER": SNOWFLAKE_USER,
            "SNOWFLAKE_WAREHOUSE": SNOWFLAKE_WAREHOUSE,
            "SNOWFLAKE_DATABASE": SNOWFLAKE_DATABASE,
            "SNOWFLAKE_SCHEMA": SNOWFLAKE_SCHEMA,
            "SNOWFLAKE_PRIVATE_KEY_FILE": SNOWFLAKE_PRIVATE_KEY_FILE,
        }.items() if not value
    ]
    if missing:
        raise ValueError(f"Missing Snowflake config in .env: {missing}")

    conn_kwargs = {
        "account": SNOWFLAKE_ACCOUNT,
        "user": SNOWFLAKE_USER,
        "warehouse": SNOWFLAKE_WAREHOUSE,
        "database": SNOWFLAKE_DATABASE,
        "schema": SNOWFLAKE_SCHEMA,
        "authenticator": "SNOWFLAKE_JWT",
        "private_key_file": SNOWFLAKE_PRIVATE_KEY_FILE,
    }

    if SNOWFLAKE_ROLE:
        conn_kwargs["role"] = SNOWFLAKE_ROLE
    if SNOWFLAKE_PRIVATE_KEY_FILE_PWD:
        conn_kwargs["private_key_file_pwd"] = SNOWFLAKE_PRIVATE_KEY_FILE_PWD

    return snowflake.connector.connect(**conn_kwargs)


def ensure_tables_exist(conn) -> None:
    cur = conn.cursor()
    try:
        statements = [stmt.strip() for stmt in CREATE_TABLES_SQL.split(";") if stmt.strip()]
        for stmt in statements:
            cur.execute(stmt)
    finally:
        cur.close()


def first_route_id_from_alert(alert_dict: Dict[str, Any]) -> Optional[str]:
    informed_entities = alert_dict.get("informed_entity", [])
    for ie in informed_entities:
        route_id = ie.get("route_id")
        if route_id:
            return route_id
    return None


def build_dataframes_from_producer(dag_run_id: Optional[str] = None) -> Tuple[pd.DataFrame, ...]:
    dag_run_id = dag_run_id or f"manual__{utc_now().isoformat()}"
    producer = MTAProducer(sink=NoOpSink())

    run_rows: List[Dict[str, Any]] = []
    full_feed_rows: List[Dict[str, Any]] = []
    trip_update_rows: List[Dict[str, Any]] = []
    vehicle_rows: List[Dict[str, Any]] = []
    alert_rows: List[Dict[str, Any]] = []

    for feed_cfg in producer.feeds:
        ingestion_id = str(uuid.uuid4())
        feed_name = feed_cfg["feed_name"]
        source_url = feed_cfg["url"]
        requested_at = utc_now()

        try:
            LOGGER.info("Fetching feed %s", feed_name)
            raw_bytes = producer._get_bytes(source_url)
            if not raw_bytes:
                raise ValueError("No bytes returned from feed request")

            feed = producer._parse_feed(raw_bytes)
            feed_dict = producer._feed_to_dict(feed)
            trip_updates, vehicle_positions, alerts = producer._split_entities(feed)

            feed_timestamp = ts_to_dt(feed_dict.get("header", {}).get("timestamp"))
            entity_count = len(feed.entity)
            trip_update_count = len(trip_updates)
            vehicle_position_count = len(vehicle_positions)
            alert_count = len(alerts)

            run_rows.append({
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
            })

            full_feed_rows.append({
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
            })

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

        except Exception as exc:
            LOGGER.exception("Failed feed %s", feed_name)
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

    return (
        pd.DataFrame(run_rows),
        pd.DataFrame(full_feed_rows),
        pd.DataFrame(trip_update_rows),
        pd.DataFrame(vehicle_rows),
        pd.DataFrame(alert_rows),
    )


def load_to_snowflake(
    run_df: pd.DataFrame,
    full_feed_df: pd.DataFrame,
    trip_update_df: pd.DataFrame,
    vehicle_df: pd.DataFrame,
    alert_df: pd.DataFrame,
) -> None:
    conn = get_snowflake_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(f"USE WAREHOUSE {SNOWFLAKE_WAREHOUSE}")
            cur.execute(f"USE DATABASE {SNOWFLAKE_DATABASE}")
            cur.execute(f"USE SCHEMA {SNOWFLAKE_SCHEMA}")
        finally:
            cur.close()

        ensure_tables_exist(conn)

        if not run_df.empty:
            write_pandas(
                conn,
                run_df,
                "RAW_MTA_PRODUCER_FEED_RUNS",
                database=SNOWFLAKE_DATABASE,
                schema=SNOWFLAKE_SCHEMA,
                quote_identifiers=False,
                use_logical_type=True,
            )

        if not full_feed_df.empty:
            write_pandas(
                conn,
                full_feed_df,
                "RAW_MTA_PRODUCER_FULL_FEEDS",
                database=SNOWFLAKE_DATABASE,
                schema=SNOWFLAKE_SCHEMA,
                quote_identifiers=False,
                use_logical_type=True,
            )

        if not trip_update_df.empty:
            write_pandas(
                conn,
                trip_update_df,
                "RAW_MTA_PRODUCER_TRIP_UPDATES",
                database=SNOWFLAKE_DATABASE,
                schema=SNOWFLAKE_SCHEMA,
                quote_identifiers=False,
                use_logical_type=True,
            )

        if not vehicle_df.empty:
            write_pandas(
                conn,
                vehicle_df,
                "RAW_MTA_PRODUCER_VEHICLE_POSITIONS",
                database=SNOWFLAKE_DATABASE,
                schema=SNOWFLAKE_SCHEMA,
                quote_identifiers=False,
                use_logical_type=True,
            )

        if not alert_df.empty:
            write_pandas(
                conn,
                alert_df,
                "RAW_MTA_PRODUCER_ALERTS",
                database=SNOWFLAKE_DATABASE,
                schema=SNOWFLAKE_SCHEMA,
                quote_identifiers=False,
                use_logical_type=True,
            )

    finally:
        conn.close()


def run_snapshot_upload(dag_run_id: Optional[str] = None) -> Dict[str, int]:
    run_df, full_feed_df, trip_update_df, vehicle_df, alert_df = build_dataframes_from_producer(
        dag_run_id=dag_run_id
    )
    load_to_snowflake(run_df, full_feed_df, trip_update_df, vehicle_df, alert_df)

    summary = {
        "feed_runs_loaded": int(len(run_df)),
        "full_feeds_loaded": int(len(full_feed_df)),
        "trip_updates_loaded": int(len(trip_update_df)),
        "vehicle_positions_loaded": int(len(vehicle_df)),
        "alerts_loaded": int(len(alert_df)),
    }
    LOGGER.info("Snapshot upload summary: %s", summary)
    return summary


if __name__ == "__main__":
    print(run_snapshot_upload())