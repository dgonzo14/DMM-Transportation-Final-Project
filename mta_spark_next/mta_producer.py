from __future__ import annotations

import argparse
import base64
import logging
import uuid
from typing import Any, Dict, Iterable, List, Optional

import requests
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

from mta_spark_next.bronze_io import BronzeSink, build_bronze_object_key, build_bronze_sink
from mta_spark_next.config import Settings, get_settings
from mta_spark_next.mta_schemas import BRONZE_HISTORY_TABLE
from mta_spark_next.snowflake_io import append_history_rows, ensure_tables_exist, get_snowflake_connection
from mta_spark_next.utils import extract_route_ids_from_entity, json_dumps, parse_epoch_timestamp, utc_now


LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def _parse_feed_bytes(raw_bytes: bytes) -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw_bytes)
    return feed


def _feed_to_dict(feed: gtfs_realtime_pb2.FeedMessage) -> Dict[str, Any]:
    return MessageToDict(feed, preserving_proto_field_name=True)


def _split_entities(feed_dict: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    trip_updates: List[Dict[str, Any]] = []
    vehicle_positions: List[Dict[str, Any]] = []
    alerts: List[Dict[str, Any]] = []

    for entity in feed_dict.get("entity", []):
        if "trip_update" in entity:
            trip_updates.append(entity)
        elif "vehicle" in entity:
            vehicle_positions.append(entity)
        elif "alert" in entity:
            alerts.append(entity)

    return {
        "trip_updates": trip_updates,
        "vehicle_positions": vehicle_positions,
        "alerts": alerts,
    }


def _collect_route_ids(feed_dict: Dict[str, Any]) -> List[str]:
    route_ids = set()
    for entity in feed_dict.get("entity", []):
        route_ids.update(extract_route_ids_from_entity(entity))
    return sorted(route_ids)


def _build_metadata(
    ingestion_id: str,
    dag_run_id: str,
    feed_name: str,
    data_type: str,
    source_url: str,
    object_key: str,
    ingested_at,
    feed_timestamp,
    route_ids: Iterable[str],
) -> Dict[str, Any]:
    return {
        "ingestion_id": ingestion_id,
        "dag_run_id": dag_run_id,
        "feed_name": feed_name,
        "data_type": data_type,
        "source_url": source_url,
        "object_key": object_key,
        "ingested_at": ingested_at.isoformat(),
        "feed_timestamp": feed_timestamp.isoformat() if feed_timestamp else None,
        "route_ids": list(route_ids),
    }


def _build_envelope(metadata: Dict[str, Any], payload: Any, raw_bytes: Optional[bytes] = None) -> Dict[str, Any]:
    envelope: Dict[str, Any] = {
        "metadata": metadata,
        "payload": payload,
    }
    if raw_bytes is not None:
        envelope["raw_protobuf_base64"] = base64.b64encode(raw_bytes).decode("utf-8")
    return envelope


def _write_feed_objects(
    settings: Settings,
    sink: BronzeSink,
    dag_run_id: str,
    feed_name: str,
    source_url: str,
    raw_bytes: bytes,
    feed_dict: Dict[str, Any],
) -> List[Dict[str, Any]]:
    ingested_at = utc_now()
    ingestion_id = str(uuid.uuid4())
    route_ids = _collect_route_ids(feed_dict)
    split_entities = _split_entities(feed_dict)
    feed_timestamp = parse_epoch_timestamp(feed_dict.get("header", {}).get("timestamp"))

    payloads = {
        "full_feed": (feed_dict, len(feed_dict.get("entity", [])), raw_bytes),
        "trip_updates": (split_entities["trip_updates"], len(split_entities["trip_updates"]), None),
        "vehicle_positions": (split_entities["vehicle_positions"], len(split_entities["vehicle_positions"]), None),
        "alerts": (split_entities["alerts"], len(split_entities["alerts"]), None),
    }

    history_rows: List[Dict[str, Any]] = []
    for data_type, (payload, row_count, maybe_raw_bytes) in payloads.items():
        object_key = build_bronze_object_key(
            settings=settings,
            data_type=data_type,
            ingested_at=ingested_at,
            feed_name=feed_name,
            ingestion_id=ingestion_id,
        )
        metadata = _build_metadata(
            ingestion_id=ingestion_id,
            dag_run_id=dag_run_id,
            feed_name=feed_name,
            data_type=data_type,
            source_url=source_url,
            object_key=object_key,
            ingested_at=ingested_at,
            feed_timestamp=feed_timestamp,
            route_ids=route_ids,
        )
        sink.write_json(object_key, _build_envelope(metadata, payload, raw_bytes=maybe_raw_bytes))
        history_rows.append({
            "OBJECT_KEY": object_key,
            "INGESTION_ID": ingestion_id,
            "DAG_RUN_ID": dag_run_id,
            "DATA_TYPE": data_type,
            "FEED_NAME": feed_name,
            "SOURCE_URL": source_url,
            "INGESTED_AT": ingested_at,
            "STATUS": "SUCCESS",
            "ROW_COUNT": row_count,
            "TABLE_COUNTS_JSON": json_dumps({
                "entity_count": len(feed_dict.get("entity", [])),
                "trip_update_count": len(split_entities["trip_updates"]),
                "vehicle_position_count": len(split_entities["vehicle_positions"]),
                "alert_count": len(split_entities["alerts"]),
            }),
            "ERROR_MESSAGE": None,
        })

    return history_rows


def _persist_bronze_history(settings: Settings, history_rows: List[Dict[str, Any]]) -> None:
    if not settings.snowflake_enabled or not history_rows:
        return

    try:
        conn = get_snowflake_connection(settings)
    except Exception as exc:
        LOGGER.warning("Skipping bronze history persistence because Snowflake is unavailable: %s", exc)
        return

    try:
        ensure_tables_exist(conn, [BRONZE_HISTORY_TABLE])
        append_history_rows(conn, settings, BRONZE_HISTORY_TABLE, history_rows)
    finally:
        conn.close()


def run_mta_bronze_fetch_once(
    dag_run_id: Optional[str] = None,
    feed_names: Optional[List[str]] = None,
) -> Dict[str, Any]:
    settings = get_settings()
    sink = build_bronze_sink(settings)
    dag_run_id = dag_run_id or f"manual__{utc_now().isoformat()}"
    requested_feed_names = set(feed_names or [])

    written_history_rows: List[Dict[str, Any]] = []
    failed_feeds: List[str] = []
    for feed in settings.load_feeds():
        if requested_feed_names and feed["feed_name"] not in requested_feed_names:
            continue

        try:
            LOGGER.info("Fetching MTA GTFS-Realtime feed %s", feed["feed_name"])
            response = requests.get(feed["url"], headers={"Cache-Control": "no-cache"}, timeout=20)
            response.raise_for_status()

            feed_message = _parse_feed_bytes(response.content)
            feed_dict = _feed_to_dict(feed_message)
            history_rows = _write_feed_objects(
                settings=settings,
                sink=sink,
                dag_run_id=dag_run_id,
                feed_name=feed["feed_name"],
                source_url=feed["url"],
                raw_bytes=response.content,
                feed_dict=feed_dict,
            )
            written_history_rows.extend(history_rows)
        except Exception as exc:
            LOGGER.exception("Failed MTA bronze fetch for feed %s", feed["feed_name"])
            failed_feeds.append(feed["feed_name"])
            written_history_rows.append({
                "OBJECT_KEY": None,
                "INGESTION_ID": None,
                "DAG_RUN_ID": dag_run_id,
                "DATA_TYPE": "full_feed",
                "FEED_NAME": feed["feed_name"],
                "SOURCE_URL": feed["url"],
                "INGESTED_AT": utc_now(),
                "STATUS": "FAILED",
                "ROW_COUNT": 0,
                "TABLE_COUNTS_JSON": json_dumps({}),
                "ERROR_MESSAGE": str(exc),
            })

    _persist_bronze_history(settings, written_history_rows)

    summary = {
        "dag_run_id": dag_run_id,
        "objects_written": len(written_history_rows),
        "feeds_processed": len({row["FEED_NAME"] for row in written_history_rows}),
        "feeds_failed": failed_feeds,
        "data_types_written": sorted({row["DATA_TYPE"] for row in written_history_rows}),
    }
    LOGGER.info("MTA bronze fetch summary: %s", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch MTA GTFS-Realtime feeds once and write bronze snapshots.")
    parser.add_argument("--dag-run-id", help="Optional Airflow run id for lineage.")
    parser.add_argument(
        "--feed-name",
        action="append",
        dest="feed_names",
        help="Optional feed_name filter. Repeat for multiple feeds.",
    )
    args = parser.parse_args()
    run_mta_bronze_fetch_once(dag_run_id=args.dag_run_id, feed_names=args.feed_names)


if __name__ == "__main__":
    main()
