#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TfL Backfill Pipeline

Processes historical bronze data per data type independently.
Produces 15-minute and hourly aggregations for each.

Usage:
    # Backfill a single data type
    python tfl_backfill.py --bucket my-bucket --type arrivals

    # Backfill a specific date range
    python tfl_backfill.py --bucket my-bucket --type arrivals --start 2026-04-01 --end 2026-04-20

    # Backfill all types
    python tfl_backfill.py --bucket my-bucket --type all
"""

import argparse
from datetime import datetime, timedelta, UTC

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from tfl_pipeline import (
    create_spark,
    arrivals_schema, crowding_schema, status_schema, lift_disruptions_schema,
    transform_arrivals, transform_crowding, transform_status, transform_lift_disruptions,
    SNOWFLAKE_OPTIONS,
)
from tfl_arrival_inference import _add_trip_key


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def _date_paths(bucket: str, data_type: str, start: datetime, end: datetime) -> list[str]:
    """
    Generates explicit day-level paths for the given date range.
    Avoids scanning the entire bucket when you only want a window of days.
    """
    paths = []
    cur = start
    while cur <= end:
        paths.append(
            f"s3a://{bucket}/bronze/tfl/{data_type}/"
            f"year={cur.year}/month={cur.month:02d}/day={cur.day:02d}/"
        )
        cur += timedelta(days=1)
    return paths


def _load(spark: SparkSession, paths: list[str], schema, data_type: str):
    return (
        spark.read
        .format("json")
        .schema(schema)
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.json")
        .load(paths)
    )


def _write_snowflake(df, table: str, mode: str = "append"):
    for field in df.schema.fields:
        if "TimestampNTZ" in str(field.dataType):
            df = df.withColumn(field.name, F.col(field.name).cast(TimestampType()))
    (
        df.write
        .format("net.snowflake.spark.snowflake")
        .options(**SNOWFLAKE_OPTIONS)
        .option("dbtable", table)
        .mode(mode)
        .save()
    )


# ---------------------------------------------------------------------------
# Arrivals backfill
# Two outputs:
#   1. TFL_ARRIVAL_METRICS_GOLD     — one row per inferred trip
#   2. TFL_ARRIVALS_15MIN_GOLD      — 15-min windowed service frequency
#   3. TFL_ARRIVALS_HOURLY_GOLD     — hourly windowed service frequency
# ---------------------------------------------------------------------------

def backfill_arrivals(spark: SparkSession, paths: list[str]):
    raw = _load(spark, paths, arrivals_schema(), "arrivals")
    df = _add_trip_key(transform_arrivals(raw))

    # --- Trip-level metrics (same logic as streaming inference but batch) ---
    w_ordered = Window.partitionBy("trip_key").orderBy("poll_ts")
    w_all     = Window.partitionBy("trip_key")

    trip_metrics = (
        df
        .withColumn("rn",                       F.row_number().over(w_ordered))
        .withColumn("initial_expected_arrival", F.first("expected_arrival_ts").over(w_all))
        .withColumn("actual_arrival_proxy",     F.last("expected_arrival_ts").over(w_all))
        .withColumn("arrival_timestamp",        F.last("poll_ts").over(w_all))
        .withColumn("ea_epoch",                 F.col("expected_arrival_ts").cast("double"))
        .withColumn("prediction_error_seconds",
            F.last("ea_epoch").over(w_all) - F.first("ea_epoch").over(w_all))
        .withColumn("prediction_stability_var",
            F.avg(F.col("ea_epoch") * F.col("ea_epoch")).over(w_all)
            - F.pow(F.avg("ea_epoch").over(w_all), 2))
        .withColumn("poll_count", F.count("*").over(w_all))
        .where(F.col("rn") == 1)
        .select(
            "trip_key",
            "initial_expected_arrival",
            "actual_arrival_proxy",
            "arrival_timestamp",
            "prediction_error_seconds",
            "prediction_stability_var",
            "poll_count",
        )
    )

    _write_snowflake(trip_metrics, "TFL_ARRIVAL_METRICS_GOLD")
    print(f"[arrivals] trip metrics written: {trip_metrics.count()} rows")

    # --- 15-minute service frequency ---
    agg_15min = (
        df
        .groupBy(
            F.window("expected_arrival_ts", "15 minutes").alias("window"),
            "line_id",
            "station_id",
        )
        .agg(
            F.countDistinct("vehicle_id").alias("train_count"),
            F.avg("time_to_station").alias("avg_time_to_station_s"),
            F.stddev("time_to_station").alias("stddev_time_to_station_s"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "line_id",
            "station_id",
            "train_count",
            "avg_time_to_station_s",
            "stddev_time_to_station_s",
        )
    )

    _write_snowflake(agg_15min, "TFL_ARRIVALS_15MIN_GOLD")
    print(f"[arrivals] 15-min windows written: {agg_15min.count()} rows")

    # --- Hourly service frequency ---
    agg_hourly = (
        df
        .groupBy(
            F.window("expected_arrival_ts", "1 hour").alias("window"),
            "line_id",
            "station_id",
        )
        .agg(
            F.countDistinct("vehicle_id").alias("train_count"),
            F.avg("time_to_station").alias("avg_time_to_station_s"),
            F.stddev("time_to_station").alias("stddev_time_to_station_s"),
            F.avg("prediction_error_seconds")
                .alias("avg_prediction_error_s"),  # requires trip_metrics join — see note
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "line_id",
            "station_id",
            "train_count",
            "avg_time_to_station_s",
            "stddev_time_to_station_s",
        )
    )

    _write_snowflake(agg_hourly, "TFL_ARRIVALS_HOURLY_GOLD")
    print(f"[arrivals] hourly windows written: {agg_hourly.count()} rows")


# ---------------------------------------------------------------------------
# Crowding backfill
#   Crowding files are per-naptan snapshots, so windows show baseline % over time.
#   Outputs:
#     TFL_CROWDING_15MIN_GOLD
#     TFL_CROWDING_HOURLY_GOLD
# ---------------------------------------------------------------------------

def backfill_crowding(spark: SparkSession, paths: list[str]):
    # Crowding files embed the naptan in the filename but not in the JSON.
    # We recover it from the file path using input_file_name().
    raw = _load(spark, paths, crowding_schema(), "crowding")
    df = (
        transform_crowding(raw)
        .withColumn("_file", F.input_file_name())
        .withColumn(
            "station_id",
            # Filename format: ...Z_crowding_<NAPTAN>.json
            F.regexp_extract(F.col("_file"), r"_crowding_([^.]+)\.json$", 1),
        )
        .drop("_file")
        .where(F.col("data_available") == True)
    )

    for window_size, table in [("15 minutes", "TFL_CROWDING_15MIN_GOLD"),
                                ("1 hour",     "TFL_CROWDING_HOURLY_GOLD")]:
        agg = (
            df
            .groupBy(
                F.window("crowding_ts", window_size).alias("window"),
                "station_id",
            )
            .agg(
                F.avg("pct_of_baseline").alias("avg_pct_of_baseline"),
                F.max("pct_of_baseline").alias("peak_pct_of_baseline"),
                F.count("*").alias("sample_count"),
            )
            .select(
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "station_id",
                "avg_pct_of_baseline",
                "peak_pct_of_baseline",
                "sample_count",
            )
        )
        _write_snowflake(agg, table)
        print(f"[crowding] {window_size} windows written: {agg.count()} rows")


# ---------------------------------------------------------------------------
# Status backfill
#   Line status snapshots. Windows show how long each severity persisted.
#   Outputs:
#     TFL_STATUS_15MIN_GOLD
#     TFL_STATUS_HOURLY_GOLD
# ---------------------------------------------------------------------------

def backfill_status(spark: SparkSession, paths: list[str]):
    raw = _load(spark, paths, status_schema(), "status")
    df = transform_status(raw)

    for window_size, table in [("15 minutes", "TFL_STATUS_15MIN_GOLD"),
                                ("1 hour",     "TFL_STATUS_HOURLY_GOLD")]:
        agg = (
            df
            .groupBy(
                F.window("modified_ts", window_size).alias("window"),
                "line_id",
                "severity",
                "severity_description",
            )
            .agg(
                F.count("*").alias("snapshot_count"),
                # How many distinct disruption reasons appeared in this window
                F.countDistinct("disruption_reason").alias("distinct_disruptions"),
            )
            .select(
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                "line_id",
                "severity",
                "severity_description",
                "snapshot_count",
                "distinct_disruptions",
            )
        )
        _write_snowflake(agg, table)
        print(f"[status] {window_size} windows written: {agg.count()} rows")


# ---------------------------------------------------------------------------
# Lift disruptions backfill
#   These are point-in-time snapshots with no natural timestamp in the JSON.
#   We derive the timestamp from the filename, same trick as crowding.
#   Outputs:
#     TFL_LIFT_DISRUPTIONS_HOURLY_GOLD  (15-min is too granular for this data)
# ---------------------------------------------------------------------------

def backfill_lift_disruptions(spark: SparkSession, paths: list[str]):
    raw = _load(spark, paths, lift_disruptions_schema(), "lift_disruptions")
    df = (
        transform_lift_disruptions(raw)
        .withColumn("_file", F.input_file_name())
        .withColumn(
            "snapshot_ts",
            # Filename format: 20260420T152728Z_lift_disruptions_tube.json
            F.to_timestamp(
                F.regexp_extract(F.col("_file"), r"(\d{8}T\d{6}Z)", 1),
                "yyyyMMdd'T'HHmmss'Z'",
            ),
        )
        .drop("_file")
    )

    agg = (
        df
        .groupBy(
            F.window("snapshot_ts", "1 hour").alias("window"),
            "station_id",
        )
        .agg(
            F.collect_set("lift_id").alias("disrupted_lifts"),
            F.count("lift_id").alias("disruption_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "station_id",
            "disrupted_lifts",
            "disruption_count",
        )
    )
    _write_snowflake(agg, "TFL_LIFT_DISRUPTIONS_HOURLY_GOLD")
    print(f"[lift_disruptions] hourly windows written: {agg.count()} rows")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

BACKFILL_REGISTRY = {
    "arrivals":          backfill_arrivals,
    "crowding":          backfill_crowding,
    "status":            backfill_status,
    "lift_disruptions":  backfill_lift_disruptions,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument(
        "--type", required=True,
        choices=[*BACKFILL_REGISTRY.keys(), "all"],
        help="Data type to backfill, or 'all' to run every type sequentially",
    )
    parser.add_argument(
        "--start",
        help="Start date inclusive (YYYY-MM-DD). Omit to scan full bucket.",
    )
    parser.add_argument(
        "--end",
        help="End date inclusive (YYYY-MM-DD). Defaults to today if --start given.",
    )
    args = parser.parse_args()

    types = list(BACKFILL_REGISTRY.keys()) if args.type == "all" else [args.type]

    spark = create_spark("tfl-backfill")

    for data_type in types:
        print(f"\n{'='*60}")
        print(f"Backfilling: {data_type}")
        print(f"{'='*60}")

        if args.start:
            start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=UTC)
            if args.end:
                end = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=UTC)
            else:
                end = datetime.now(UTC)
            paths = _date_paths(args.bucket, data_type, start, end)
            print(f"Date range: {args.start} → {end.strftime('%Y-%m-%d')} ({len(paths)} days)")
        else:
            paths = [f"s3a://{args.bucket}/bronze/tfl/{data_type}/"]
            print("Date range: full bucket scan")

        BACKFILL_REGISTRY[data_type](spark, paths)

    spark.stop()


if __name__ == "__main__":
    main()