#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tfl_silver.py
Silver-layer transforms for all four TfL data types.
Each function takes a raw bronze DataFrame and returns a flat, typed DataFrame
ready to write to Snowflake silver tables.

Mirrors:
    SILVER_TFL_ARRIVALS
    SILVER_TFL_LINE_STATUS
    SILVER_TFL_LIFT_DISRUPTIONS
    SILVER_TFL_CROWDING
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_arrivals(df: DataFrame) -> DataFrame:
    """
    Mirrors SILVER_TFL_ARRIVALS.
    One row per arrival prediction poll.
    """
    return (
        df.select(
            F.col("id").alias("prediction_id"),
            F.col("vehicleId").alias("vehicle_id"),
            F.col("naptanId").alias("station_id"),
            F.col("stationName").alias("station_name"),
            F.col("lineId").alias("line_id"),
            F.col("platformName").alias("platform"),
            F.col("direction"),
            F.col("timeToStation").alias("seconds_to_arrival"),
            F.to_timestamp("expectedArrival").alias("expected_arrival_ts"),
            F.to_timestamp("timing.read").alias("data_captured_at"),
            F.input_file_name().alias("file_name"),
        )
        .where(F.col("prediction_id").isNotNull())
    )


def transform_line_status(df: DataFrame) -> DataFrame:
    """
    Mirrors SILVER_TFL_LINE_STATUS.
    Explodes lineStatuses array — one row per line per severity entry.
    """
    return (
        df.select(
            F.col("id").alias("line_id"),
            F.col("name").alias("line_name"),
            F.to_timestamp("modified").alias("record_timestamp"),
            F.explode("lineStatuses").alias("s"),
        )
        .select(
            "line_id",
            "line_name",
            "record_timestamp",
            F.col("s.statusSeverity").alias("severity_code"),
            F.col("s.statusSeverityDescription").alias("status_description"),
            F.col("s.reason").alias("disruption_reason"),
        )
    )


def transform_lift_disruptions(df: DataFrame) -> DataFrame:
    """
    Mirrors SILVER_TFL_LIFT_DISRUPTIONS.
    Explodes disruptedLiftUniqueIds — one row per lift.
    Derives record_timestamp from filename since JSON has no timestamp field.
    """
    return (
        df
        .withColumn("_file", F.input_file_name())
        .withColumn(
            "record_timestamp",
            F.to_timestamp(
                F.regexp_extract(F.col("_file"), r"(\d{8}T\d{6}Z)", 1),
                "yyyyMMdd'T'HHmmss'Z'",
            ),
        )
        .select(
            F.col("stationUniqueId").alias("station_id"),
            F.explode("disruptedLiftUniqueIds").alias("lift_id"),
            F.col("message").alias("full_message"),
            F.col("record_timestamp"),
        )
    )


def transform_crowding(df: DataFrame) -> DataFrame:
    """
    Mirrors SILVER_TFL_CROWDING.
    Crowding JSON files are single objects with no naptan_id field —
    the naptan is extracted from the filename.
    Rows where dataAvailable=False are dropped.
    """
    return (
        df
        .withColumn("_file", F.input_file_name())
        .withColumn(
            "naptan_id",
            F.regexp_extract(F.col("_file"), r"crowding_([A-Z0-9]+)\.json$", 1),
        )
        .select(
            "naptan_id",
            F.col("dataAvailable").alias("is_live"),
            F.col("percentageOfBaseline").alias("crowding_percentage"),
            F.to_timestamp("timeUtc").alias("observation_time_utc"),
            F.col("_file").alias("file_name"),
        )
        .where(F.col("is_live") == True)
    )


# ---------------------------------------------------------------------------
# Registry — used by both the silver runner and the streaming runner
# ---------------------------------------------------------------------------

SILVER_TRANSFORMS = {
    "arrivals":         (transform_arrivals,         "TFL_ARRIVALS_SILVER"),
    "status":           (transform_line_status,       "TFL_STATUS_SILVER"),
    "lift_disruptions": (transform_lift_disruptions,  "TFL_LIFT_DISRUPTIONS_SILVER"),
    "crowding":         (transform_crowding,          "TFL_CROWDING_SILVER"),
}