from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from mta_next.config import get_settings
from mta_next.schemas import GOLD_HISTORY_TABLE, GOLD_TABLES, SILVER_HISTORY_TABLE
from mta_next.snowflake_io import (
    append_history_rows,
    delete_rows_by_values,
    ensure_tables_exist,
    fetch_dataframe,
    fetch_success_object_keys,
    get_snowflake_connection,
    sql_in_list,
    sql_quote,
    table_fqn,
    write_dataframe,
)
from mta_next.utils import json_dumps, stable_hash, utc_now


LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def _parse_date_arg(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    datetime.strptime(value, "%Y-%m-%d")
    return value


def _fetch_pending_gold_history_rows(
    conn,
    start_date: Optional[str],
    end_date: Optional[str],
    force: bool,
    max_objects: Optional[int],
) -> pd.DataFrame:
    settings = get_settings()
    filters = ["STATUS = 'SUCCESS'"]
    if start_date:
        filters.append(f"CAST(INGESTED_AT AS DATE) >= {sql_quote(start_date)}")
    if end_date:
        filters.append(f"CAST(INGESTED_AT AS DATE) <= {sql_quote(end_date)}")

    silver_df = fetch_dataframe(
        conn,
        f"""
        SELECT OBJECT_KEY, INGESTION_ID, DATA_TYPE, FEED_NAME, SOURCE_URL, INGESTED_AT
        FROM {table_fqn(settings, SILVER_HISTORY_TABLE)}
        WHERE {' AND '.join(filters)}
        ORDER BY INGESTED_AT
        """,
    )
    if silver_df.empty:
        return silver_df

    silver_df = silver_df.drop_duplicates(subset=["OBJECT_KEY"], keep="last").copy()

    if not force:
        processed_gold = set(
            fetch_success_object_keys(
                conn,
                settings,
                GOLD_HISTORY_TABLE,
                start_date=start_date,
                end_date=end_date,
            )
        )
        silver_df = silver_df[~silver_df["OBJECT_KEY"].isin(processed_gold)].copy()

    if max_objects:
        silver_df = silver_df.head(max_objects)

    return silver_df


def _fetch_rows_by_object_keys(conn, table_name: str, object_keys: List[str]) -> pd.DataFrame:
    settings = get_settings()
    if not object_keys:
        return pd.DataFrame()
    return fetch_dataframe(
        conn,
        f"""
        SELECT *
        FROM {table_fqn(settings, table_name)}
        WHERE OBJECT_KEY IN ({sql_in_list(object_keys)})
        """,
    )


def _fetch_rows_by_stable_keys(conn, stable_keys: List[str]) -> pd.DataFrame:
    settings = get_settings()
    if not stable_keys:
        return pd.DataFrame()
    return fetch_dataframe(
        conn,
        f"""
        SELECT *
        FROM {table_fqn(settings, 'MTA_STOP_TIME_UPDATES_SILVER')}
        WHERE TRIP_STOP_STABLE_KEY IN ({sql_in_list(stable_keys)})
        """,
    )


def _normalize_timestamp_column(df: pd.DataFrame, column_name: str) -> None:
    if column_name in df.columns:
        df[column_name] = pd.to_datetime(df[column_name], utc=True, errors="coerce")


def _first_non_null(series: pd.Series):
    for value in series:
        if pd.notna(value):
            return value
    return None


def _compute_arrival_inference(stop_updates_df: pd.DataFrame) -> pd.DataFrame:
    if stop_updates_df.empty:
        return pd.DataFrame()

    for column_name in ["INGESTED_AT", "ARRIVAL_TIME", "DEPARTURE_TIME"]:
        _normalize_timestamp_column(stop_updates_df, column_name)

    stop_updates_df = stop_updates_df.sort_values(["TRIP_STOP_STABLE_KEY", "INGESTED_AT", "STOP_SEQUENCE_INDEX"])

    rows: List[Dict[str, Any]] = []
    for stable_key, group in stop_updates_df.groupby("TRIP_STOP_STABLE_KEY", dropna=False):
        group = group.sort_values(["INGESTED_AT", "STOP_SEQUENCE_INDEX"])
        first_row = group.iloc[0]
        last_row = group.iloc[-1]

        arrival_epochs = [value.timestamp() for value in group["ARRIVAL_TIME"].dropna()]
        variance = float(pd.Series(arrival_epochs).var(ddof=0)) if len(arrival_epochs) > 1 else 0.0

        first_arrival = _first_non_null(group["ARRIVAL_TIME"])
        last_arrival = _first_non_null(group["ARRIVAL_TIME"][::-1])
        first_departure = _first_non_null(group["DEPARTURE_TIME"])
        last_departure = _first_non_null(group["DEPARTURE_TIME"][::-1])

        if pd.notna(first_arrival) and pd.notna(last_arrival):
            prediction_drift_seconds = float((last_arrival - first_arrival).total_seconds())
        else:
            prediction_drift_seconds = None

        observed_span_seconds = None
        if pd.notna(first_row["INGESTED_AT"]) and pd.notna(last_row["INGESTED_AT"]):
            observed_span_seconds = float((last_row["INGESTED_AT"] - first_row["INGESTED_AT"]).total_seconds())

        inferred_completion_proxy_ts = last_departure or last_arrival or last_row["INGESTED_AT"]

        rows.append({
            "ARRIVAL_INFERENCE_KEY": stable_key,
            "ROUTE_ID": first_row.get("ROUTE_ID"),
            "TRIP_ID": first_row.get("TRIP_ID"),
            "START_DATE": first_row.get("START_DATE"),
            "START_TIME": first_row.get("START_TIME"),
            "TRAIN_ID": first_row.get("TRAIN_ID"),
            "DIRECTION": first_row.get("DIRECTION"),
            "STOP_ID": first_row.get("STOP_ID"),
            "FIRST_SEEN_INGESTED_AT": first_row.get("INGESTED_AT"),
            "LAST_SEEN_INGESTED_AT": last_row.get("INGESTED_AT"),
            "FIRST_PREDICTED_ARRIVAL_TS": first_arrival,
            "LAST_PREDICTED_ARRIVAL_TS": last_arrival,
            "FIRST_PREDICTED_DEPARTURE_TS": first_departure,
            "LAST_PREDICTED_DEPARTURE_TS": last_departure,
            "POLL_COUNT": int(len(group)),
            "PREDICTION_DRIFT_SECONDS": prediction_drift_seconds,
            "PREDICTION_VARIANCE_SECONDS": variance,
            "OBSERVED_SPAN_SECONDS": observed_span_seconds,
            "INFERRED_COMPLETION_PROXY_TS": inferred_completion_proxy_ts,
            "LAST_SOURCE_OBJECT_KEY": last_row.get("OBJECT_KEY"),
        })

    return pd.DataFrame(rows)


def _fetch_arrival_gold_scope(conn, arrival_df: pd.DataFrame) -> pd.DataFrame:
    settings = get_settings()
    if arrival_df.empty:
        return pd.DataFrame()

    working_df = arrival_df.copy()
    working_df["OBSERVED_TS"] = working_df["LAST_PREDICTED_ARRIVAL_TS"].fillna(working_df["LAST_PREDICTED_DEPARTURE_TS"])
    working_df = working_df.dropna(subset=["OBSERVED_TS", "ROUTE_ID", "STOP_ID"])
    if working_df.empty:
        return pd.DataFrame()

    route_ids = sorted({str(value) for value in working_df["ROUTE_ID"].dropna().tolist()})
    stop_ids = sorted({str(value) for value in working_df["STOP_ID"].dropna().tolist()})
    min_ts = pd.to_datetime(working_df["OBSERVED_TS"].min(), utc=True).to_pydatetime()
    max_ts = pd.to_datetime(working_df["OBSERVED_TS"].max(), utc=True).to_pydatetime() + timedelta(minutes=15)

    return fetch_dataframe(
        conn,
        f"""
        SELECT *
        FROM {table_fqn(settings, 'MTA_ARRIVAL_INFERENCE_GOLD')}
        WHERE ROUTE_ID IN ({sql_in_list(route_ids)})
          AND STOP_ID IN ({sql_in_list(stop_ids)})
          AND COALESCE(LAST_PREDICTED_ARRIVAL_TS, LAST_PREDICTED_DEPARTURE_TS)
              BETWEEN TO_TIMESTAMP_TZ({sql_quote(min_ts.isoformat())})
              AND TO_TIMESTAMP_TZ({sql_quote(max_ts.isoformat())})
        """,
    )


def _compute_route_headways(arrival_gold_df: pd.DataFrame) -> pd.DataFrame:
    if arrival_gold_df.empty:
        return pd.DataFrame()

    for column_name in ["LAST_PREDICTED_ARRIVAL_TS", "LAST_PREDICTED_DEPARTURE_TS"]:
        _normalize_timestamp_column(arrival_gold_df, column_name)

    arrival_gold_df = arrival_gold_df.copy()
    arrival_gold_df["OBSERVED_TS"] = arrival_gold_df["LAST_PREDICTED_ARRIVAL_TS"].fillna(arrival_gold_df["LAST_PREDICTED_DEPARTURE_TS"])
    arrival_gold_df = arrival_gold_df.dropna(subset=["OBSERVED_TS", "ROUTE_ID", "STOP_ID"])
    if arrival_gold_df.empty:
        return pd.DataFrame()

    arrival_gold_df["WINDOW_START"] = arrival_gold_df["OBSERVED_TS"].dt.floor("15min")
    arrival_gold_df["WINDOW_END"] = arrival_gold_df["WINDOW_START"] + pd.Timedelta(minutes=15)

    rows: List[Dict[str, Any]] = []
    for keys, group in arrival_gold_df.groupby(["ROUTE_ID", "DIRECTION", "STOP_ID", "WINDOW_START", "WINDOW_END"], dropna=False):
        route_id, direction, stop_id, window_start, window_end = keys
        group = group.sort_values("OBSERVED_TS")
        diffs = group["OBSERVED_TS"].diff().dropna().dt.total_seconds()

        rows.append({
            "HEADWAY_WINDOW_KEY": stable_hash(route_id, direction, stop_id, window_start.isoformat() if pd.notna(window_start) else None),
            "ROUTE_ID": route_id,
            "DIRECTION": direction,
            "STOP_ID": stop_id,
            "WINDOW_START": window_start,
            "WINDOW_END": window_end,
            "TRIP_COUNT": int(len(group)),
            "AVG_HEADWAY_SECONDS": float(diffs.mean()) if not diffs.empty else None,
            "MIN_HEADWAY_SECONDS": float(diffs.min()) if not diffs.empty else None,
            "MAX_HEADWAY_SECONDS": float(diffs.max()) if not diffs.empty else None,
        })

    return pd.DataFrame(rows)


def _compute_route_snapshot_summary(
    snapshot_df: pd.DataFrame,
    trip_df: pd.DataFrame,
    stop_df: pd.DataFrame,
    vehicle_df: pd.DataFrame,
    alert_df: pd.DataFrame,
) -> pd.DataFrame:
    if snapshot_df.empty:
        return pd.DataFrame()

    def _counts(frame: pd.DataFrame, count_name: str) -> pd.DataFrame:
        if frame.empty:
            return pd.DataFrame(columns=["OBJECT_KEY", "FEED_NAME", "ROUTE_ID", count_name])
        tmp = frame.copy()
        tmp["ROUTE_ID"] = tmp["ROUTE_ID"].fillna("UNKNOWN")
        return tmp.groupby(["OBJECT_KEY", "FEED_NAME", "ROUTE_ID"], dropna=False).size().reset_index(name=count_name)

    trip_counts = _counts(trip_df, "TRIP_UPDATE_COUNT")
    stop_counts = _counts(stop_df, "STOP_TIME_UPDATE_COUNT")
    vehicle_counts = _counts(vehicle_df, "VEHICLE_POSITION_COUNT")
    alert_counts = _counts(alert_df, "ALERT_COUNT")

    merged = trip_counts.merge(stop_counts, on=["OBJECT_KEY", "FEED_NAME", "ROUTE_ID"], how="outer")
    merged = merged.merge(vehicle_counts, on=["OBJECT_KEY", "FEED_NAME", "ROUTE_ID"], how="outer")
    merged = merged.merge(alert_counts, on=["OBJECT_KEY", "FEED_NAME", "ROUTE_ID"], how="outer")
    if merged.empty:
        return merged

    for column_name in ["TRIP_UPDATE_COUNT", "STOP_TIME_UPDATE_COUNT", "VEHICLE_POSITION_COUNT", "ALERT_COUNT"]:
        merged[column_name] = merged[column_name].fillna(0).astype(int)

    snapshot_df = snapshot_df[["OBJECT_KEY", "INGESTION_ID", "FEED_NAME", "INGESTED_AT", "FEED_TIMESTAMP"]].drop_duplicates()
    for column_name in ["INGESTED_AT", "FEED_TIMESTAMP"]:
        _normalize_timestamp_column(snapshot_df, column_name)
    snapshot_df["SNAPSHOT_TS"] = snapshot_df["FEED_TIMESTAMP"].fillna(snapshot_df["INGESTED_AT"])

    merged = merged.merge(snapshot_df[["OBJECT_KEY", "INGESTION_ID", "FEED_NAME", "SNAPSHOT_TS"]], on=["OBJECT_KEY", "FEED_NAME"], how="left")
    merged["ROUTE_SNAPSHOT_KEY"] = merged.apply(
        lambda row: stable_hash(row["OBJECT_KEY"], row["ROUTE_ID"]),
        axis=1,
    )
    return merged[
        [
            "ROUTE_SNAPSHOT_KEY",
            "OBJECT_KEY",
            "INGESTION_ID",
            "FEED_NAME",
            "ROUTE_ID",
            "SNAPSHOT_TS",
            "TRIP_UPDATE_COUNT",
            "STOP_TIME_UPDATE_COUNT",
            "VEHICLE_POSITION_COUNT",
            "ALERT_COUNT",
        ]
    ]


def _fetch_alert_scope(conn, new_alert_df: pd.DataFrame) -> pd.DataFrame:
    settings = get_settings()
    if new_alert_df.empty:
        return pd.DataFrame()

    working_df = new_alert_df.copy()
    for column_name in ["FEED_TIMESTAMP", "INGESTED_AT"]:
        _normalize_timestamp_column(working_df, column_name)
    working_df["OBSERVED_TS"] = working_df["FEED_TIMESTAMP"].fillna(working_df["INGESTED_AT"])
    working_df["ROUTE_ID"] = working_df["ROUTE_ID"].fillna("UNKNOWN")
    working_df = working_df.dropna(subset=["OBSERVED_TS"])
    if working_df.empty:
        return pd.DataFrame()

    route_ids = sorted({str(value) for value in working_df["ROUTE_ID"].dropna().tolist()})
    min_ts = pd.to_datetime(working_df["OBSERVED_TS"].min(), utc=True).to_pydatetime().replace(minute=0, second=0, microsecond=0)
    max_ts = pd.to_datetime(working_df["OBSERVED_TS"].max(), utc=True).to_pydatetime().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)

    return fetch_dataframe(
        conn,
        f"""
        SELECT *
        FROM {table_fqn(settings, 'MTA_ALERTS_SILVER')}
        WHERE COALESCE(ROUTE_ID, 'UNKNOWN') IN ({sql_in_list(route_ids)})
          AND COALESCE(FEED_TIMESTAMP, INGESTED_AT)
              BETWEEN TO_TIMESTAMP_TZ({sql_quote(min_ts.isoformat())})
              AND TO_TIMESTAMP_TZ({sql_quote(max_ts.isoformat())})
        """,
    )


def _compute_alert_activity(alert_df: pd.DataFrame) -> pd.DataFrame:
    if alert_df.empty:
        return pd.DataFrame()

    for column_name in ["FEED_TIMESTAMP", "INGESTED_AT", "ACTIVE_PERIOD_END"]:
        _normalize_timestamp_column(alert_df, column_name)

    alert_df = alert_df.copy()
    alert_df["ROUTE_ID"] = alert_df["ROUTE_ID"].fillna("UNKNOWN")
    alert_df["OBSERVED_TS"] = alert_df["FEED_TIMESTAMP"].fillna(alert_df["INGESTED_AT"])
    alert_df = alert_df.dropna(subset=["OBSERVED_TS"])
    if alert_df.empty:
        return pd.DataFrame()

    alert_df["WINDOW_START"] = alert_df["OBSERVED_TS"].dt.floor("1h")
    alert_df["WINDOW_END"] = alert_df["WINDOW_START"] + pd.Timedelta(hours=1)
    alert_df["ACTIVE_ALERT_FLAG"] = alert_df.apply(
        lambda row: pd.isna(row["ACTIVE_PERIOD_END"]) or row["ACTIVE_PERIOD_END"] >= row["WINDOW_START"],
        axis=1,
    )

    rows: List[Dict[str, Any]] = []
    for keys, group in alert_df.groupby(["ROUTE_ID", "WINDOW_START", "WINDOW_END"], dropna=False):
        route_id, window_start, window_end = keys
        rows.append({
            "ALERT_ACTIVITY_KEY": stable_hash(route_id, window_start.isoformat() if pd.notna(window_start) else None),
            "ROUTE_ID": route_id,
            "WINDOW_START": window_start,
            "WINDOW_END": window_end,
            "ALERT_SNAPSHOT_COUNT": int(len(group)),
            "DISTINCT_ALERT_ENTITY_COUNT": int(group["ENTITY_ID"].nunique(dropna=True)),
            "MAX_INFORMED_ENTITY_COUNT": int(group["INFORMED_ENTITY_COUNT"].fillna(0).max()) if not group.empty else 0,
            "ACTIVE_ALERT_COUNT": int(group["ACTIVE_ALERT_FLAG"].sum()),
        })

    return pd.DataFrame(rows)


def _upsert_gold_frames(
    conn,
    arrival_df: pd.DataFrame,
    headway_df: pd.DataFrame,
    route_snapshot_df: pd.DataFrame,
    alert_activity_df: pd.DataFrame,
) -> Dict[str, int]:
    settings = get_settings()

    if not arrival_df.empty:
        delete_rows_by_values(conn, settings, "MTA_ARRIVAL_INFERENCE_GOLD", "ARRIVAL_INFERENCE_KEY", arrival_df["ARRIVAL_INFERENCE_KEY"].tolist())
        write_dataframe(conn, settings, arrival_df, "MTA_ARRIVAL_INFERENCE_GOLD")

    if not headway_df.empty:
        delete_rows_by_values(conn, settings, "MTA_ROUTE_HEADWAYS_GOLD", "HEADWAY_WINDOW_KEY", headway_df["HEADWAY_WINDOW_KEY"].tolist())
        write_dataframe(conn, settings, headway_df, "MTA_ROUTE_HEADWAYS_GOLD")

    if not route_snapshot_df.empty:
        delete_rows_by_values(conn, settings, "MTA_ROUTE_SNAPSHOT_SUMMARY_GOLD", "ROUTE_SNAPSHOT_KEY", route_snapshot_df["ROUTE_SNAPSHOT_KEY"].tolist())
        write_dataframe(conn, settings, route_snapshot_df, "MTA_ROUTE_SNAPSHOT_SUMMARY_GOLD")

    if not alert_activity_df.empty:
        delete_rows_by_values(conn, settings, "MTA_ALERT_ACTIVITY_GOLD", "ALERT_ACTIVITY_KEY", alert_activity_df["ALERT_ACTIVITY_KEY"].tolist())
        write_dataframe(conn, settings, alert_activity_df, "MTA_ALERT_ACTIVITY_GOLD")

    return {
        "MTA_ARRIVAL_INFERENCE_GOLD": int(len(arrival_df)),
        "MTA_ROUTE_HEADWAYS_GOLD": int(len(headway_df)),
        "MTA_ROUTE_SNAPSHOT_SUMMARY_GOLD": int(len(route_snapshot_df)),
        "MTA_ALERT_ACTIVITY_GOLD": int(len(alert_activity_df)),
    }


def run_mta_gold_inference_once(
    dag_run_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    force: bool = False,
    max_objects: Optional[int] = None,
) -> Dict[str, Any]:
    dag_run_id = dag_run_id or f"gold__{utc_now().isoformat()}"
    start_date = _parse_date_arg(start_date)
    end_date = _parse_date_arg(end_date)

    settings = get_settings()
    conn = get_snowflake_connection(settings)
    try:
        ensure_tables_exist(conn, [GOLD_HISTORY_TABLE, *GOLD_TABLES])
        pending_df = _fetch_pending_gold_history_rows(conn, start_date, end_date, force, max_objects)
        if pending_df.empty:
            summary = {
                "dag_run_id": dag_run_id,
                "objects_considered": 0,
                "objects_succeeded": 0,
                "objects_failed": 0,
            }
            LOGGER.info("No pending MTA gold objects. Summary: %s", summary)
            return summary

        object_keys = [str(value) for value in pending_df["OBJECT_KEY"].tolist()]

        try:
            snapshot_df = _fetch_rows_by_object_keys(conn, "MTA_FEED_SNAPSHOTS_SILVER", object_keys)
            trip_df = _fetch_rows_by_object_keys(conn, "MTA_TRIP_UPDATES_SILVER", object_keys)
            stop_df_new = _fetch_rows_by_object_keys(conn, "MTA_STOP_TIME_UPDATES_SILVER", object_keys)
            vehicle_df = _fetch_rows_by_object_keys(conn, "MTA_VEHICLE_POSITIONS_SILVER", object_keys)
            alert_df_new = _fetch_rows_by_object_keys(conn, "MTA_ALERTS_SILVER", object_keys)

            impacted_stable_keys = sorted({str(value) for value in stop_df_new.get("TRIP_STOP_STABLE_KEY", pd.Series(dtype=str)).dropna().tolist()})
            stop_df_scope = _fetch_rows_by_stable_keys(conn, impacted_stable_keys)
            arrival_df = _compute_arrival_inference(stop_df_scope)
            if not arrival_df.empty:
                _upsert_gold_frames(conn, arrival_df, pd.DataFrame(), pd.DataFrame(), pd.DataFrame())
            arrival_scope_df = _fetch_arrival_gold_scope(conn, arrival_df)
            headway_df = _compute_route_headways(arrival_scope_df)

            route_snapshot_df = _compute_route_snapshot_summary(snapshot_df, trip_df, stop_df_new, vehicle_df, alert_df_new)
            alert_scope_df = _fetch_alert_scope(conn, alert_df_new)
            alert_activity_df = _compute_alert_activity(alert_scope_df)

            table_counts = _upsert_gold_frames(conn, pd.DataFrame(), headway_df, route_snapshot_df, alert_activity_df)
            table_counts["MTA_ARRIVAL_INFERENCE_GOLD"] = int(len(arrival_df))

            history_rows = []
            total_row_count = sum(table_counts.values())
            for row in pending_df.itertuples(index=False):
                history_rows.append({
                    "OBJECT_KEY": row.OBJECT_KEY,
                    "INGESTION_ID": row.INGESTION_ID,
                    "DAG_RUN_ID": dag_run_id,
                    "DATA_TYPE": row.DATA_TYPE,
                    "FEED_NAME": row.FEED_NAME,
                    "SOURCE_URL": row.SOURCE_URL,
                    "INGESTED_AT": row.INGESTED_AT,
                    "STATUS": "SUCCESS",
                    "ROW_COUNT": total_row_count,
                    "TABLE_COUNTS_JSON": json_dumps(table_counts),
                    "ERROR_MESSAGE": None,
                })

            append_history_rows(conn, settings, GOLD_HISTORY_TABLE, history_rows)
            summary = {
                "dag_run_id": dag_run_id,
                "objects_considered": len(pending_df),
                "objects_succeeded": len(history_rows),
                "objects_failed": 0,
                "table_counts": table_counts,
            }
            LOGGER.info("MTA gold inference summary: %s", summary)
            return summary
        except Exception as exc:
            LOGGER.exception("MTA gold inference failed for dag_run_id=%s", dag_run_id)
            failed_rows = []
            for row in pending_df.itertuples(index=False):
                failed_rows.append({
                    "OBJECT_KEY": row.OBJECT_KEY,
                    "INGESTION_ID": row.INGESTION_ID,
                    "DAG_RUN_ID": dag_run_id,
                    "DATA_TYPE": row.DATA_TYPE,
                    "FEED_NAME": row.FEED_NAME,
                    "SOURCE_URL": row.SOURCE_URL,
                    "INGESTED_AT": row.INGESTED_AT,
                    "STATUS": "FAILED",
                    "ROW_COUNT": 0,
                    "TABLE_COUNTS_JSON": json_dumps({}),
                    "ERROR_MESSAGE": str(exc),
                })
            append_history_rows(conn, settings, GOLD_HISTORY_TABLE, failed_rows)
            raise
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute MTA gold analytics incrementally from silver tables.")
    parser.add_argument("--dag-run-id", help="Optional Airflow DAG run id.")
    parser.add_argument("--start-date", help="Inclusive start date filter in YYYY-MM-DD.")
    parser.add_argument("--end-date", help="Inclusive end date filter in YYYY-MM-DD.")
    parser.add_argument("--force", action="store_true", help="Recompute gold outputs even if history shows success.")
    parser.add_argument("--max-objects", type=int, help="Optional cap for one-shot runs.")
    args = parser.parse_args()

    run_mta_gold_inference_once(
        dag_run_id=args.dag_run_id,
        start_date=args.start_date,
        end_date=args.end_date,
        force=args.force,
        max_objects=args.max_objects,
    )


if __name__ == "__main__":
    main()
