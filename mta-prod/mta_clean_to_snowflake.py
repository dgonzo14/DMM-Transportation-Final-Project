import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

# ----------------------------
# Config
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
# SNOWFLAKE_PRIVATE_KEY_FILE_PWD = os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE_PWD")

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS CLEAN_MTA_TRIP_UPDATES (
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
    START_TIME STRING,
    STOP_UPDATE_COUNT NUMBER,
    FIRST_STOP_ID STRING,
    LAST_STOP_ID STRING,
    FIRST_ARRIVAL_TIME TIMESTAMP_TZ,
    LAST_ARRIVAL_TIME TIMESTAMP_TZ,
    RAW_TRIP_UPDATE_JSON STRING,
    INSERTED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS CLEAN_MTA_STOP_TIME_UPDATES (
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
    START_TIME STRING,
    STOP_SEQUENCE_INDEX NUMBER,
    STOP_ID STRING,
    DIRECTION STRING,
    ARRIVAL_TIME TIMESTAMP_TZ,
    DEPARTURE_TIME TIMESTAMP_TZ,
    SECONDS_UNTIL_ARRIVAL NUMBER,
    SECONDS_UNTIL_DEPARTURE NUMBER,
    RAW_STOP_TIME_JSON STRING,
    INSERTED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS CLEAN_MTA_VEHICLE_POSITIONS (
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
    START_TIME STRING,
    STOP_ID STRING,
    DIRECTION STRING,
    CURRENT_STOP_SEQUENCE NUMBER,
    CURRENT_STATUS STRING,
    VEHICLE_TIMESTAMP TIMESTAMP_TZ,
    LATITUDE FLOAT,
    LONGITUDE FLOAT,
    BEARING FLOAT,
    RAW_VEHICLE_JSON STRING,
    INSERTED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS CLEAN_MTA_ALERTS (
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

CREATE TABLE IF NOT EXISTS CLEAN_MTA_ROUTE_SNAPSHOT_SUMMARY (
    REQUESTED_AT TIMESTAMP_TZ,
    FEED_NAME STRING,
    ROUTE_ID STRING,
    TRIP_UPDATE_COUNT NUMBER,
    VEHICLE_POSITION_COUNT NUMBER,
    ALERT_COUNT NUMBER,
    INSERTED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);
"""


def get_snowflake_connection():
    if not SNOWFLAKE_PRIVATE_KEY_FILE:
        raise ValueError("SNOWFLAKE_PRIVATE_KEY_FILE is missing from .env")

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
    # if SNOWFLAKE_PRIVATE_KEY_FILE_PWD:
    #     conn_kwargs["private_key_file_pwd"] = SNOWFLAKE_PRIVATE_KEY_FILE_PWD

    return snowflake.connector.connect(**conn_kwargs)


def ensure_tables_exist(conn) -> None:
    cur = conn.cursor()
    try:
        statements = [stmt.strip() for stmt in CREATE_TABLES_SQL.split(";") if stmt.strip()]
        for stmt in statements:
            cur.execute(stmt)
    finally:
        cur.close()


def fetch_df(conn, sql: str) -> pd.DataFrame:
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetch_pandas_all()
    finally:
        cur.close()


def to_utc_datetime(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            return value.tz_localize("UTC").to_pydatetime()
        return value.to_pydatetime()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    try:
        ts = pd.to_datetime(value, utc=True)
        return ts.to_pydatetime()
    except Exception:
        return None


def parse_epoch_ts(value: Any) -> Optional[datetime]:
    if value in (None, "", 0, "0"):
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except Exception:
        return None


def safe_int(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def normalize_service_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    if len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:]}"
    return value


def extract_direction(stop_id: Optional[str]) -> Optional[str]:
    if not stop_id:
        return None
    if stop_id.endswith("N"):
        return "N"
    if stop_id.endswith("S"):
        return "S"
    return None


def seconds_until(target_dt: Optional[datetime], base_dt: Optional[datetime]) -> Optional[int]:
    if not target_dt or not base_dt:
        return None
    return int((target_dt - base_dt).total_seconds())


def first_arrival(stop_updates: List[Dict[str, Any]]) -> Optional[datetime]:
    if not stop_updates:
        return None
    return parse_epoch_ts(stop_updates[0].get("arrival", {}).get("time"))


def last_arrival(stop_updates: List[Dict[str, Any]]) -> Optional[datetime]:
    if not stop_updates:
        return None
    return parse_epoch_ts(stop_updates[-1].get("arrival", {}).get("time"))


def first_route_id_from_alert(alert_dict: Dict[str, Any]) -> Optional[str]:
    informed_entities = alert_dict.get("informed_entity", [])
    for item in informed_entities:
        route_id = item.get("route_id")
        if route_id:
            return route_id
    return None


def build_clean_trip_and_stop_dfs(raw_trip_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    trip_rows = []
    stop_rows = []

    for row in raw_trip_df.itertuples(index=False):
        raw = json.loads(row.RAW_TRIP_UPDATE_JSON)
        trip_update = raw.get("trip_update", {})
        trip = trip_update.get("trip", {})
        stop_updates = trip_update.get("stop_time_update", [])

        requested_at = to_utc_datetime(row.REQUESTED_AT)
        feed_timestamp = to_utc_datetime(row.FEED_TIMESTAMP)

        trip_rows.append({
            "INGESTION_ID": row.INGESTION_ID,
            "DAG_RUN_ID": row.DAG_RUN_ID,
            "FEED_NAME": row.FEED_NAME,
            "SOURCE_URL": row.SOURCE_URL,
            "REQUESTED_AT": requested_at,
            "FEED_TIMESTAMP": feed_timestamp,
            "ENTITY_ID": raw.get("id"),
            "TRIP_ID": trip.get("trip_id"),
            "ROUTE_ID": trip.get("route_id"),
            "START_DATE": normalize_service_date(trip.get("start_date")),
            "START_TIME": trip.get("start_time"),
            "STOP_UPDATE_COUNT": len(stop_updates),
            "FIRST_STOP_ID": stop_updates[0].get("stop_id") if stop_updates else None,
            "LAST_STOP_ID": stop_updates[-1].get("stop_id") if stop_updates else None,
            "FIRST_ARRIVAL_TIME": first_arrival(stop_updates),
            "LAST_ARRIVAL_TIME": last_arrival(stop_updates),
            "RAW_TRIP_UPDATE_JSON": row.RAW_TRIP_UPDATE_JSON,
        })

        for idx, stu in enumerate(stop_updates):
            arrival_time = parse_epoch_ts(stu.get("arrival", {}).get("time"))
            departure_time = parse_epoch_ts(stu.get("departure", {}).get("time"))
            stop_id = stu.get("stop_id")

            stop_rows.append({
                "INGESTION_ID": row.INGESTION_ID,
                "DAG_RUN_ID": row.DAG_RUN_ID,
                "FEED_NAME": row.FEED_NAME,
                "SOURCE_URL": row.SOURCE_URL,
                "REQUESTED_AT": requested_at,
                "FEED_TIMESTAMP": feed_timestamp,
                "ENTITY_ID": raw.get("id"),
                "TRIP_ID": trip.get("trip_id"),
                "ROUTE_ID": trip.get("route_id"),
                "START_DATE": normalize_service_date(trip.get("start_date")),
                "START_TIME": trip.get("start_time"),
                "STOP_SEQUENCE_INDEX": idx,
                "STOP_ID": stop_id,
                "DIRECTION": extract_direction(stop_id),
                "ARRIVAL_TIME": arrival_time,
                "DEPARTURE_TIME": departure_time,
                "SECONDS_UNTIL_ARRIVAL": seconds_until(arrival_time, requested_at),
                "SECONDS_UNTIL_DEPARTURE": seconds_until(departure_time, requested_at),
                "RAW_STOP_TIME_JSON": json.dumps(stu, ensure_ascii=False),
            })

    trip_df = pd.DataFrame(trip_rows)
    stop_df = pd.DataFrame(stop_rows)

    if not trip_df.empty:
        trip_df = trip_df.drop_duplicates(subset=["INGESTION_ID", "ENTITY_ID"])
    if not stop_df.empty:
        stop_df = stop_df.drop_duplicates(subset=["INGESTION_ID", "ENTITY_ID", "STOP_SEQUENCE_INDEX", "STOP_ID"])

    return trip_df, stop_df


def build_clean_vehicle_df(raw_vehicle_df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for row in raw_vehicle_df.itertuples(index=False):
        raw = json.loads(row.RAW_VEHICLE_JSON)
        vehicle_payload = raw.get("vehicle", {})
        trip = vehicle_payload.get("trip", {})
        vehicle = vehicle_payload.get("vehicle", {})
        position = vehicle_payload.get("position", {})

        stop_id = vehicle_payload.get("stop_id")
        requested_at = to_utc_datetime(row.REQUESTED_AT)
        feed_timestamp = to_utc_datetime(row.FEED_TIMESTAMP)

        rows.append({
            "INGESTION_ID": row.INGESTION_ID,
            "DAG_RUN_ID": row.DAG_RUN_ID,
            "FEED_NAME": row.FEED_NAME,
            "SOURCE_URL": row.SOURCE_URL,
            "REQUESTED_AT": requested_at,
            "FEED_TIMESTAMP": feed_timestamp,
            "ENTITY_ID": raw.get("id"),
            "VEHICLE_ID": vehicle.get("id"),
            "TRIP_ID": trip.get("trip_id"),
            "ROUTE_ID": trip.get("route_id"),
            "START_DATE": normalize_service_date(trip.get("start_date")),
            "START_TIME": trip.get("start_time"),
            "STOP_ID": stop_id,
            "DIRECTION": extract_direction(stop_id),
            "CURRENT_STOP_SEQUENCE": safe_int(vehicle_payload.get("current_stop_sequence")),
            "CURRENT_STATUS": vehicle_payload.get("current_status"),
            "VEHICLE_TIMESTAMP": parse_epoch_ts(vehicle_payload.get("timestamp")),
            "LATITUDE": safe_float(position.get("latitude")),
            "LONGITUDE": safe_float(position.get("longitude")),
            "BEARING": safe_float(position.get("bearing")),
            "RAW_VEHICLE_JSON": row.RAW_VEHICLE_JSON,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.drop_duplicates(subset=["INGESTION_ID", "ENTITY_ID"])
    return df


def build_clean_alert_df(raw_alert_df: pd.DataFrame) -> pd.DataFrame:
    rows = []

    for row in raw_alert_df.itertuples(index=False):
        raw = json.loads(row.RAW_ALERT_JSON)
        alert_dict = raw.get("alert", {})

        rows.append({
            "INGESTION_ID": row.INGESTION_ID,
            "DAG_RUN_ID": row.DAG_RUN_ID,
            "FEED_NAME": row.FEED_NAME,
            "SOURCE_URL": row.SOURCE_URL,
            "REQUESTED_AT": to_utc_datetime(row.REQUESTED_AT),
            "FEED_TIMESTAMP": to_utc_datetime(row.FEED_TIMESTAMP),
            "ENTITY_ID": raw.get("id"),
            "FIRST_ROUTE_ID": first_route_id_from_alert(alert_dict),
            "INFORMED_ENTITY_COUNT": len(alert_dict.get("informed_entity", [])),
            "ACTIVE_PERIOD_COUNT": len(alert_dict.get("active_period", [])),
            "RAW_ALERT_JSON": row.RAW_ALERT_JSON,
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.drop_duplicates(subset=["INGESTION_ID", "ENTITY_ID"])
    return df


def build_route_snapshot_summary(
    trip_df: pd.DataFrame,
    vehicle_df: pd.DataFrame,
    alert_df: pd.DataFrame
) -> pd.DataFrame:
    trip_summary = pd.DataFrame(columns=["REQUESTED_AT", "FEED_NAME", "ROUTE_ID", "TRIP_UPDATE_COUNT"])
    vehicle_summary = pd.DataFrame(columns=["REQUESTED_AT", "FEED_NAME", "ROUTE_ID", "VEHICLE_POSITION_COUNT"])
    alert_summary = pd.DataFrame(columns=["REQUESTED_AT", "FEED_NAME", "ROUTE_ID", "ALERT_COUNT"])

    if not trip_df.empty:
        trip_summary = (
            trip_df.groupby(["REQUESTED_AT", "FEED_NAME", "ROUTE_ID"], dropna=False)
            .size()
            .reset_index(name="TRIP_UPDATE_COUNT")
        )

    if not vehicle_df.empty:
        vehicle_summary = (
            vehicle_df.groupby(["REQUESTED_AT", "FEED_NAME", "ROUTE_ID"], dropna=False)
            .size()
            .reset_index(name="VEHICLE_POSITION_COUNT")
        )

    if not alert_df.empty:
        alert_tmp = alert_df[alert_df["FIRST_ROUTE_ID"].notna()].copy()
        alert_summary = (
            alert_tmp.groupby(["REQUESTED_AT", "FEED_NAME", "FIRST_ROUTE_ID"], dropna=False)
            .size()
            .reset_index(name="ALERT_COUNT")
            .rename(columns={"FIRST_ROUTE_ID": "ROUTE_ID"})
        )

    # force consistent merge dtypes
    for df in [trip_summary, vehicle_summary, alert_summary]:
        if "ROUTE_ID" in df.columns:
            df["ROUTE_ID"] = df["ROUTE_ID"].astype("string")

    summary = trip_summary.merge(
        vehicle_summary,
        on=["REQUESTED_AT", "FEED_NAME", "ROUTE_ID"],
        how="outer"
    ).merge(
        alert_summary,
        on=["REQUESTED_AT", "FEED_NAME", "ROUTE_ID"],
        how="outer"
    )

    if summary.empty:
        return summary

    summary["TRIP_UPDATE_COUNT"] = summary["TRIP_UPDATE_COUNT"].fillna(0).astype(int)
    summary["VEHICLE_POSITION_COUNT"] = summary["VEHICLE_POSITION_COUNT"].fillna(0).astype(int)
    summary["ALERT_COUNT"] = summary["ALERT_COUNT"].fillna(0).astype(int)

    return summary


def load_clean_tables(
    conn,
    trip_df: pd.DataFrame,
    stop_df: pd.DataFrame,
    vehicle_df: pd.DataFrame,
    alert_df: pd.DataFrame,
    route_summary_df: pd.DataFrame,
) -> None:
    ensure_tables_exist(conn)

    if not trip_df.empty:
        write_pandas(
            conn,
            trip_df,
            "CLEAN_MTA_TRIP_UPDATES",
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            quote_identifiers=False,
            use_logical_type=True,
        )

    if not stop_df.empty:
        write_pandas(
            conn,
            stop_df,
            "CLEAN_MTA_STOP_TIME_UPDATES",
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            quote_identifiers=False,
            use_logical_type=True,
        )

    if not vehicle_df.empty:
        write_pandas(
            conn,
            vehicle_df,
            "CLEAN_MTA_VEHICLE_POSITIONS",
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            quote_identifiers=False,
            use_logical_type=True,
        )

    if not alert_df.empty:
        write_pandas(
            conn,
            alert_df,
            "CLEAN_MTA_ALERTS",
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            quote_identifiers=False,
            use_logical_type=True,
        )

    if not route_summary_df.empty:
        write_pandas(
            conn,
            route_summary_df,
            "CLEAN_MTA_ROUTE_SNAPSHOT_SUMMARY",
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            quote_identifiers=False,
            use_logical_type=True,
        )


def run_cleaning() -> Dict[str, int]:
    conn = get_snowflake_connection()
    try:
        raw_trip_df = fetch_df(
            conn,
            f"""
            SELECT *
            FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RAW_MTA_PRODUCER_TRIP_UPDATES
            """
        )

        raw_vehicle_df = fetch_df(
            conn,
            f"""
            SELECT *
            FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RAW_MTA_PRODUCER_VEHICLE_POSITIONS
            """
        )

        raw_alert_df = fetch_df(
            conn,
            f"""
            SELECT *
            FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RAW_MTA_PRODUCER_ALERTS
            """
        )

        trip_df, stop_df = build_clean_trip_and_stop_dfs(raw_trip_df)
        vehicle_df = build_clean_vehicle_df(raw_vehicle_df)
        alert_df = build_clean_alert_df(raw_alert_df)
        route_summary_df = build_route_snapshot_summary(trip_df, vehicle_df, alert_df)

        load_clean_tables(conn, trip_df, stop_df, vehicle_df, alert_df, route_summary_df)

        summary = {
            "clean_trip_updates_loaded": int(len(trip_df)),
            "clean_stop_time_updates_loaded": int(len(stop_df)),
            "clean_vehicle_positions_loaded": int(len(vehicle_df)),
            "clean_alerts_loaded": int(len(alert_df)),
            "route_snapshot_summary_loaded": int(len(route_summary_df)),
        }
        LOGGER.info("Cleaning summary: %s", summary)
        return summary

    finally:
        conn.close()


if __name__ == "__main__":
    print(run_cleaning())