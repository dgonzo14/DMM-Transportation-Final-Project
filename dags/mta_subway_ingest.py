import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import snowflake.connector
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2
from snowflake.connector.pandas_tools import write_pandas
import os
from dotenv import load_dotenv

load_dotenv(os.path.expanduser("~/mta_project/.env"))

LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS RAW_MTA_SUBWAY_FEED_RUNS (
    INGESTION_ID STRING,
    DAG_RUN_ID STRING,
    FEED_NAME STRING,
    SOURCE_URL STRING,
    REQUESTED_AT TIMESTAMP_TZ,
    FEED_TIMESTAMP TIMESTAMP_TZ,
    ENTITY_COUNT NUMBER,
    HTTP_STATUS NUMBER,
    SUCCESS BOOLEAN,
    ERROR_MESSAGE STRING,
    INSERTED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_MTA_SUBWAY_ENTITIES (
    INGESTION_ID STRING,
    DAG_RUN_ID STRING,
    FEED_NAME STRING,
    SOURCE_URL STRING,
    REQUESTED_AT TIMESTAMP_TZ,
    FEED_TIMESTAMP TIMESTAMP_TZ,
    ENTITY_ID STRING,
    ENTITY_TYPE STRING,
    ROUTE_ID STRING,
    TRIP_ID STRING,
    START_DATE STRING,
    VEHICLE_ID STRING,
    STOP_ID STRING,
    CURRENT_STOP_SEQUENCE NUMBER,
    CURRENT_STATUS STRING,
    RAW_ENTITY_JSON STRING,
    INSERTED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW_MTA_SUBWAY_STOP_TIME_UPDATES (
    INGESTION_ID STRING,
    DAG_RUN_ID STRING,
    FEED_NAME STRING,
    SOURCE_URL STRING,
    REQUESTED_AT TIMESTAMP_TZ,
    FEED_TIMESTAMP TIMESTAMP_TZ,
    ENTITY_ID STRING,
    TRIP_ID STRING,
    ROUTE_ID STRING,
    STOP_SEQUENCE_INDEX NUMBER,
    STOP_ID STRING,
    ARRIVAL_TIME TIMESTAMP_TZ,
    DEPARTURE_TIME TIMESTAMP_TZ,
    SCHEDULE_RELATIONSHIP STRING,
    RAW_STOP_TIME_JSON STRING,
    INSERTED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);
"""


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ts_to_dt(ts: Optional[int]) -> Optional[datetime]:
    if not ts:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def load_feed_config() -> List[Dict[str, str]]:
    raw = os.environ["MTA_SUBWAY_FEEDS_JSON"]
    feeds = json.loads(raw)

    if not isinstance(feeds, list) or not feeds:
        raise ValueError("MTA_SUBWAY_FEEDS_JSON must be a non-empty JSON list")

    required_keys = {"feed_name", "url"}
    for feed in feeds:
        missing = required_keys - set(feed.keys())
        if missing:
            raise ValueError(f"Feed config missing keys: {missing}")

    return feeds


# def get_snowflake_connection():
#     return snowflake.connector.connect(
#         account=os.environ["SNOWFLAKE_ACCOUNT"],
#         user=os.environ["SNOWFLAKE_USER"],
#         password=os.environ["SNOWFLAKE_PASSWORD"],
#         warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
#         database=os.environ["SNOWFLAKE_DATABASE"],
#         schema=os.environ["SNOWFLAKE_SCHEMA"],
#         role=os.environ.get("SNOWFLAKE_ROLE"),
#     )

def get_snowflake_connection():
    return snowflake.connector.connect(
        account="sfedu02-unb02139",
        user="GORILLA",
        warehouse="GORILLA_WH",
        database="GORILLA_DB",
        schema="GORILLA_SCHEMA",
        private_key_file="/home/compute/d.gonzalez/.snowflake_keys/rsa_key.p8",
    )


def ensure_tables_exist(conn) -> None:
    cur = conn.cursor()
    try:
        statements = [stmt.strip() for stmt in CREATE_TABLES_SQL.split(";") if stmt.strip()]
        for stmt in statements:
            cur.execute(stmt)
    finally:
        cur.close()


def fetch_feed_bytes(url: str) -> requests.Response:
    response = requests.get(url, timeout=40)
    response.raise_for_status()
    return response


def safe_json_string(message_obj) -> str:
    return json.dumps(
        MessageToDict(message_obj, preserving_proto_field_name=True),
        ensure_ascii=False
    )


def first_alert_route_id(entity_alert) -> Optional[str]:
    if not entity_alert.informed_entity:
        return None
    for ie in entity_alert.informed_entity:
        if getattr(ie, "route_id", None):
            return ie.route_id
    return None


def parse_entity_row(
    entity,
    feed_name: str,
    source_url: str,
    ingestion_id: str,
    dag_run_id: str,
    requested_at: datetime,
    feed_timestamp: Optional[datetime],
) -> Dict[str, Any]:
    entity_type = None
    route_id = None
    trip_id = None
    start_date = None
    vehicle_id = None
    stop_id = None
    current_stop_sequence = None
    current_status = None

    if entity.HasField("trip_update"):
        entity_type = "trip_update"
        tu = entity.trip_update
        route_id = tu.trip.route_id if tu.trip.HasField("route_id") else None
        trip_id = tu.trip.trip_id if tu.trip.HasField("trip_id") else None
        start_date = tu.trip.start_date if tu.trip.HasField("start_date") else None

    elif entity.HasField("vehicle"):
        entity_type = "vehicle"
        v = entity.vehicle
        route_id = v.trip.route_id if v.trip.HasField("route_id") else None
        trip_id = v.trip.trip_id if v.trip.HasField("trip_id") else None
        start_date = v.trip.start_date if v.trip.HasField("start_date") else None
        vehicle_id = v.vehicle.id if v.vehicle.HasField("id") else None
        stop_id = v.stop_id if v.HasField("stop_id") else None
        current_stop_sequence = v.current_stop_sequence if v.HasField("current_stop_sequence") else None
        if v.HasField("current_status"):
            current_status = gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(v.current_status)

    elif entity.HasField("alert"):
        entity_type = "alert"
        route_id = first_alert_route_id(entity.alert)

    else:
        entity_type = "unknown"

    return {
        "INGESTION_ID": ingestion_id,
        "DAG_RUN_ID": dag_run_id,
        "FEED_NAME": feed_name,
        "SOURCE_URL": source_url,
        "REQUESTED_AT": requested_at,
        "FEED_TIMESTAMP": feed_timestamp,
        "ENTITY_ID": entity.id,
        "ENTITY_TYPE": entity_type,
        "ROUTE_ID": route_id,
        "TRIP_ID": trip_id,
        "START_DATE": start_date,
        "VEHICLE_ID": vehicle_id,
        "STOP_ID": stop_id,
        "CURRENT_STOP_SEQUENCE": current_stop_sequence,
        "CURRENT_STATUS": current_status,
        "RAW_ENTITY_JSON": safe_json_string(entity),
    }


def parse_stop_time_rows(
    entity,
    feed_name: str,
    source_url: str,
    ingestion_id: str,
    dag_run_id: str,
    requested_at: datetime,
    feed_timestamp: Optional[datetime],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    if not entity.HasField("trip_update"):
        return rows

    tu = entity.trip_update
    trip_id = tu.trip.trip_id if tu.trip.HasField("trip_id") else None
    route_id = tu.trip.route_id if tu.trip.HasField("route_id") else None

    for idx, stu in enumerate(tu.stop_time_update):
        arrival_time = None
        departure_time = None

        if stu.HasField("arrival") and stu.arrival.HasField("time"):
            arrival_time = ts_to_dt(stu.arrival.time)

        if stu.HasField("departure") and stu.departure.HasField("time"):
            departure_time = ts_to_dt(stu.departure.time)

        relationship = None
        if stu.HasField("schedule_relationship"):
            relationship = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.Name(
                stu.schedule_relationship
            )

        rows.append({
            "INGESTION_ID": ingestion_id,
            "DAG_RUN_ID": dag_run_id,
            "FEED_NAME": feed_name,
            "SOURCE_URL": source_url,
            "REQUESTED_AT": requested_at,
            "FEED_TIMESTAMP": feed_timestamp,
            "ENTITY_ID": entity.id,
            "TRIP_ID": trip_id,
            "ROUTE_ID": route_id,
            "STOP_SEQUENCE_INDEX": idx,
            "STOP_ID": stu.stop_id if stu.HasField("stop_id") else None,
            "ARRIVAL_TIME": arrival_time,
            "DEPARTURE_TIME": departure_time,
            "SCHEDULE_RELATIONSHIP": relationship,
            "RAW_STOP_TIME_JSON": safe_json_string(stu),
        })

    return rows


def build_dataframes(dag_run_id: Optional[str] = None):
    dag_run_id = dag_run_id or f"manual__{utc_now().isoformat()}"
    feeds = load_feed_config()

    run_rows: List[Dict[str, Any]] = []
    entity_rows: List[Dict[str, Any]] = []
    stop_rows: List[Dict[str, Any]] = []

    for feed_cfg in feeds:
        ingestion_id = str(uuid.uuid4())
        feed_name = feed_cfg["feed_name"]
        url = feed_cfg["url"]
        requested_at = utc_now()

        try:
            LOGGER.info("Fetching feed %s", feed_name)
            resp = fetch_feed_bytes(url)

            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(resp.content)

            feed_timestamp = ts_to_dt(feed.header.timestamp if feed.header.timestamp else None)

            for entity in feed.entity:
                entity_rows.append(
                    parse_entity_row(
                        entity=entity,
                        feed_name=feed_name,
                        source_url=url,
                        ingestion_id=ingestion_id,
                        dag_run_id=dag_run_id,
                        requested_at=requested_at,
                        feed_timestamp=feed_timestamp,
                    )
                )
                stop_rows.extend(
                    parse_stop_time_rows(
                        entity=entity,
                        feed_name=feed_name,
                        source_url=url,
                        ingestion_id=ingestion_id,
                        dag_run_id=dag_run_id,
                        requested_at=requested_at,
                        feed_timestamp=feed_timestamp,
                    )
                )

            run_rows.append({
                "INGESTION_ID": ingestion_id,
                "DAG_RUN_ID": dag_run_id,
                "FEED_NAME": feed_name,
                "SOURCE_URL": url,
                "REQUESTED_AT": requested_at,
                "FEED_TIMESTAMP": feed_timestamp,
                "ENTITY_COUNT": len(feed.entity),
                "HTTP_STATUS": resp.status_code,
                "SUCCESS": True,
                "ERROR_MESSAGE": None,
            })

        except Exception as exc:
            LOGGER.exception("Failed feed %s", feed_name)
            run_rows.append({
                "INGESTION_ID": ingestion_id,
                "DAG_RUN_ID": dag_run_id,
                "FEED_NAME": feed_name,
                "SOURCE_URL": url,
                "REQUESTED_AT": requested_at,
                "FEED_TIMESTAMP": None,
                "ENTITY_COUNT": 0,
                "HTTP_STATUS": None,
                "SUCCESS": False,
                "ERROR_MESSAGE": str(exc),
            })

    return (
        pd.DataFrame(run_rows),
        pd.DataFrame(entity_rows),
        pd.DataFrame(stop_rows),
    )


def load_to_snowflake(run_df: pd.DataFrame, entity_df: pd.DataFrame, stop_df: pd.DataFrame):
    conn = get_snowflake_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute("USE WAREHOUSE GORILLA_WH")
            cur.execute("USE DATABASE GORILLA_DB")
            cur.execute("USE SCHEMA GORILLA_SCHEMA")
        finally:
            cur.close()

        ensure_tables_exist(conn)

        if not run_df.empty:
            write_pandas(
                conn,
                run_df,
                "RAW_MTA_SUBWAY_FEED_RUNS",
                database="GORILLA_DB",
                schema="GORILLA_SCHEMA",
                quote_identifiers=False,
            )

        if not entity_df.empty:
            write_pandas(
                conn,
                entity_df,
                "RAW_MTA_SUBWAY_ENTITIES",
                database="GORILLA_DB",
                schema="GORILLA_SCHEMA",
                quote_identifiers=False,
            )

        if not stop_df.empty:
            write_pandas(
                conn,
                stop_df,
                "RAW_MTA_SUBWAY_STOP_TIME_UPDATES",
                database="GORILLA_DB",
                schema="GORILLA_SCHEMA",
                quote_identifiers=False,
            )

    finally:
        conn.close()


def run_ingestion(dag_run_id: Optional[str] = None) -> Dict[str, Any]:
    run_df, entity_df, stop_df = build_dataframes(dag_run_id=dag_run_id)
    load_to_snowflake(run_df, entity_df, stop_df)

    summary = {
        "feed_runs_loaded": int(len(run_df)),
        "entities_loaded": int(len(entity_df)),
        "stop_updates_loaded": int(len(stop_df)),
    }
    LOGGER.info("Ingestion summary: %s", summary)
    return summary


if __name__ == "__main__":
    print(run_ingestion())