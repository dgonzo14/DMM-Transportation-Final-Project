#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tfl_gold.py
Gold-layer transform for TfL arrival performance metrics.

Mirrors GOLD_TFL_ARRIVAL_PERFORMANCE:
  - Per prediction_id: tracks first/last expectedArrival across all polls
  - prediction_error_seconds = final_expected_ts - initial_expected_ts
  - prediction_jitter = stddev(seconds_to_arrival) across all polls
  - service_period bucketing: AM Peak / PM Peak / Off-Peak
  - is_on_time: |error| <= 30s
  - Deduplication: one row per prediction_id, latest last_poll_ts wins
  - Filters: minutes_tracked < 90, |error| < 600s
  - agency column hardcoded as 'TfL' for MTA join-readiness
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

from tfl_schemas import read_bronze, arrivals_schema
from tfl_common import day_paths, write_snowflake


def build_arrival_performance(df: DataFrame) -> DataFrame:
    """
    Accepts a raw bronze arrivals DataFrame (or a pre-filtered subset).
    Returns one row per prediction_id with full lifecycle metrics.
    """
    # Flat projection — same columns as SILVER_TFL_ARRIVALS
    flat = (
        df.select(
            F.col("id").alias("prediction_id"),
            F.col("vehicleId").alias("vehicle_id"),
            F.col("naptanId").alias("station_id"),
            F.col("stationName").alias("station_name"),
            F.col("lineId").alias("line_id"),
            F.col("timeToStation").alias("seconds_to_arrival"),
            F.to_timestamp("expectedArrival").alias("expected_arrival_ts"),
            F.to_timestamp("timing.read").alias("data_captured_at"),
        )
        .where(F.col("prediction_id").isNotNull())
    )

    # Window over the full lifecycle of each prediction
    # ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING mirrors the
    # SQL LAST_VALUE fix in the original GOLD view
    w_full = (
        Window.partitionBy("prediction_id")
        .orderBy("data_captured_at")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    w_part = Window.partitionBy("prediction_id")

    enriched = (
        flat
        .withColumn("initial_expected_ts",
            F.first("expected_arrival_ts").over(w_full))
        .withColumn("final_expected_ts",
            F.last("expected_arrival_ts").over(w_full))
        .withColumn("first_poll_ts",      F.min("data_captured_at").over(w_part))
        .withColumn("last_poll_ts",       F.max("data_captured_at").over(w_part))
        .withColumn("prediction_jitter",  F.stddev("seconds_to_arrival").over(w_part))
        .withColumn("prediction_error_seconds",
            F.col("final_expected_ts").cast("long")
            - F.col("initial_expected_ts").cast("long"))
        .withColumn("minutes_tracked",
            (F.col("last_poll_ts").cast("long")
             - F.col("first_poll_ts").cast("long")) / 60)
        .withColumn("service_period",
            F.when(F.hour("initial_expected_ts").between(7,  9),  "AM Peak")
             .when(F.hour("initial_expected_ts").between(16, 18), "PM Peak")
             .otherwise("Off-Peak"))
        .withColumn("is_on_time",
            F.when(F.abs("prediction_error_seconds") <= 30, 1).otherwise(0))
        .withColumn("agency", F.lit("TfL"))
        # QUALIFY ROW_NUMBER() equivalent — keep latest last_poll_ts per prediction
        .withColumn("_rn",
            F.row_number().over(
                Window.partitionBy("prediction_id")
                .orderBy(F.col("last_poll_ts").desc())
            ))
        .where(F.col("_rn") == 1)
        # WHERE clause from original SQL
        .where(
            (
                (F.col("minutes_tracked") > 0) |
                (F.col("prediction_error_seconds") < 30)
            ) &
            (F.col("minutes_tracked") < 90) &
            (F.abs("prediction_error_seconds") < 600)
        )
        .select(
            "agency",
            "prediction_id",
            "station_id",
            "station_name",
            "vehicle_id",
            "line_id",
            "initial_expected_ts",
            F.col("final_expected_ts").alias("actual_arrival_ts"),
            "prediction_error_seconds",
            "minutes_tracked",
            "prediction_jitter",
            "first_poll_ts",
            "last_poll_ts",
            "service_period",
            "is_on_time",
        )
    )

    return enriched


def run_gold(spark: SparkSession, hours_back: int = 48):
    """
    Reads the last N hours of bronze arrivals directly and writes gold.
    Calculates the window based on Missouri (US/Central) time.
    """
    from datetime import datetime, timedelta
    import pytz # Standard library for timezone handling

    # Define Missouri Timezone
    mo_tz = pytz.timezone('US/Central')
    
    # Get current time in Missouri
    now_mo = datetime.now(mo_tz)
    
    # Subtract the hours from Missouri time
    start_mo = now_mo - timedelta(hours=hours_back)
    
    # day_paths still needs to know the range to scan R2/Bronze
    paths = day_paths("arrivals", start_mo, now_mo)

    print(f"[gold] Missouri Window: {start_mo.strftime('%Y-%m-%d %H:%M %Z')} → {now_mo.strftime('%Y-%m-%d %H:%M %Z')}")
    print(f"[gold] processing {hours_back} hours of data...")

    raw  = read_bronze(spark, "arrivals", paths)
    gold = build_arrival_performance(raw)

    count = gold.count()
    write_snowflake(gold, "TFL_ARRIVAL_PERFORMANCE_GOLD", mode="append")
    print(f"[gold] TFL_ARRIVAL_PERFORMANCE_GOLD: {count} rows")

if __name__ == "__main__":
    import argparse
    from tfl_common import create_spark

    parser = argparse.ArgumentParser()
    # If you run --hours-back 2, it's 2 hours before Missouri's current clock
    parser.add_argument("--hours-back", type=int, default=12)
    args = parser.parse_args()

    spark = create_spark("tfl-gold")
    try:
        run_gold(spark, hours_back=args.hours_back)
    finally:
        spark.stop()