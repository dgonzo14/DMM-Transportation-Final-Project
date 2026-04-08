import json
import logging
import os
from datetime import datetime
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

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


def fetch_one(conn, sql: str):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetchone()
    finally:
        cur.close()


def fetch_all(conn, sql: str):
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetchall()
    finally:
        cur.close()


def run_quality_summary():
    conn = get_snowflake_connection()
    try:
        summary = {}

        summary["raw_counts"] = {
            "producer_feed_runs": fetch_one(conn, f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RAW_MTA_PRODUCER_FEED_RUNS")[0],
            "producer_full_feeds": fetch_one(conn, f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RAW_MTA_PRODUCER_FULL_FEEDS")[0],
            "producer_trip_updates": fetch_one(conn, f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RAW_MTA_PRODUCER_TRIP_UPDATES")[0],
            "producer_vehicle_positions": fetch_one(conn, f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RAW_MTA_PRODUCER_VEHICLE_POSITIONS")[0],
            "producer_alerts": fetch_one(conn, f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RAW_MTA_PRODUCER_ALERTS")[0],
        }

        summary["clean_counts"] = {
            "clean_trip_updates": fetch_one(conn, f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.CLEAN_MTA_TRIP_UPDATES")[0],
            "clean_stop_time_updates": fetch_one(conn, f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.CLEAN_MTA_STOP_TIME_UPDATES")[0],
            "clean_vehicle_positions": fetch_one(conn, f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.CLEAN_MTA_VEHICLE_POSITIONS")[0],
            "clean_alerts": fetch_one(conn, f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.CLEAN_MTA_ALERTS")[0],
            "route_snapshot_summary": fetch_one(conn, f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.CLEAN_MTA_ROUTE_SNAPSHOT_SUMMARY")[0],
        }

        freshness = fetch_one(
            conn,
            f"""
            SELECT MIN(REQUESTED_AT), MAX(REQUESTED_AT)
            FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.CLEAN_MTA_TRIP_UPDATES
            """
        )
        summary["trip_update_time_window"] = {
            "min_requested_at": str(freshness[0]),
            "max_requested_at": str(freshness[1]),
        }

        summary["distinct_routes"] = {
            "trip_updates": fetch_one(conn, f"SELECT COUNT(DISTINCT ROUTE_ID) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.CLEAN_MTA_TRIP_UPDATES")[0],
            "vehicle_positions": fetch_one(conn, f"SELECT COUNT(DISTINCT ROUTE_ID) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.CLEAN_MTA_VEHICLE_POSITIONS")[0],
            "alerts": fetch_one(conn, f"SELECT COUNT(DISTINCT FIRST_ROUTE_ID) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.CLEAN_MTA_ALERTS")[0],
        }

        summary["null_checks"] = {
            "trip_updates_missing_route_id": fetch_one(conn, f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.CLEAN_MTA_TRIP_UPDATES WHERE ROUTE_ID IS NULL")[0],
            "stop_updates_missing_stop_id": fetch_one(conn, f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.CLEAN_MTA_STOP_TIME_UPDATES WHERE STOP_ID IS NULL")[0],
            "vehicle_positions_missing_stop_id": fetch_one(conn, f"SELECT COUNT(*) FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.CLEAN_MTA_VEHICLE_POSITIONS WHERE STOP_ID IS NULL")[0],
        }

        summary["duplicate_checks"] = {
            "trip_update_duplicates": fetch_one(
                conn,
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT INGESTION_ID, ENTITY_ID, COUNT(*) AS c
                    FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.CLEAN_MTA_TRIP_UPDATES
                    GROUP BY 1, 2
                    HAVING COUNT(*) > 1
                )
                """
            )[0],
            "vehicle_position_duplicates": fetch_one(
                conn,
                f"""
                SELECT COUNT(*)
                FROM (
                    SELECT INGESTION_ID, ENTITY_ID, COUNT(*) AS c
                    FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.CLEAN_MTA_VEHICLE_POSITIONS
                    GROUP BY 1, 2
                    HAVING COUNT(*) > 1
                )
                """
            )[0],
        }

        top_routes = fetch_all(
            conn,
            f"""
            SELECT ROUTE_ID, COUNT(*) AS n
            FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.CLEAN_MTA_TRIP_UPDATES
            GROUP BY 1
            ORDER BY n DESC
            LIMIT 10
            """
        )
        summary["top_routes_by_trip_updates"] = [{"route_id": r[0], "count": int(r[1])} for r in top_routes]

        output_dir = project_root / "outputs"
        output_dir.mkdir(parents=True, exist_ok=True)
        out_file = output_dir / f"mta_quality_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)

        print(json.dumps(summary, indent=2))
        LOGGER.info("Wrote quality summary to %s", out_file)

    finally:
        conn.close()


if __name__ == "__main__":
    run_quality_summary()