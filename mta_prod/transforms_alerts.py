from __future__ import annotations

from typing import Any, Dict, Iterable, List

import pandas as pd

from mta_prod.utils import first_translation_text, json_dumps, parse_epoch_timestamp, stable_hash


def _alert_entities(envelope: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    data_type = envelope["metadata"]["data_type"]
    payload = envelope["payload"]
    if data_type == "full_feed":
        for entity in payload.get("entity", []):
            if "alert" in entity:
                yield entity
    elif data_type == "alerts":
        for entity in payload:
            yield entity


def build_alert_frame(envelope: Dict[str, Any], dag_run_id: str) -> pd.DataFrame:
    metadata = envelope["metadata"]
    rows: List[Dict[str, Any]] = []

    for entity in _alert_entities(envelope):
        alert = entity.get("alert", {})
        informed_entities = alert.get("informed_entity", [])
        first_informed = informed_entities[0] if informed_entities else {}
        active_period = alert.get("active_period", [])
        first_active = active_period[0] if active_period else {}

        rows.append({
            "ALERT_OBSERVATION_KEY": stable_hash(metadata["object_key"], entity.get("id")),
            "OBJECT_KEY": metadata["object_key"],
            "INGESTION_ID": metadata["ingestion_id"],
            "DAG_RUN_ID": dag_run_id,
            "FEED_NAME": metadata["feed_name"],
            "SOURCE_URL": metadata["source_url"],
            "INGESTED_AT": metadata["ingested_at"],
            "FEED_TIMESTAMP": metadata["feed_timestamp"],
            "ENTITY_ID": entity.get("id"),
            "ROUTE_ID": first_informed.get("route_id"),
            "STOP_ID": first_informed.get("stop_id"),
            "ALERT_CAUSE": alert.get("cause"),
            "ALERT_EFFECT": alert.get("effect"),
            "ACTIVE_PERIOD_START": parse_epoch_timestamp(first_active.get("start")),
            "ACTIVE_PERIOD_END": parse_epoch_timestamp(first_active.get("end")),
            "INFORMED_ENTITY_COUNT": len(informed_entities),
            "HEADER_TEXT": first_translation_text(alert.get("header_text")),
            "DESCRIPTION_TEXT": first_translation_text(alert.get("description_text")),
            "INFORMED_ENTITIES_JSON": json_dumps(informed_entities),
            "RAW_ALERT_JSON": json_dumps(entity),
        })

    return pd.DataFrame(rows)
