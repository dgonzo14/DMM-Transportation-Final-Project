#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TfL Transit Analytics: R2 to Snowflake Pipeline
"""

import argparse
import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, BooleanType, ArrayType,
)

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

load_dotenv()


def get_private_key_string(path: str) -> str:
    with open(path, "rb") as f:
        p_key = serialization.load_pem_private_key(
            f.read(),
            password=None,
            backend=default_backend()
        )
    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    pkb_str = pkb.decode("utf-8")
    pkb_str = pkb_str.replace("-----BEGIN PRIVATE KEY-----", "").replace("-----END PRIVATE KEY-----", "").replace("\n",
                                                                                                                  "")
    return pkb_str


SNOWFLAKE_OPTIONS = {
    "sfURL": os.environ["SNOWFLAKE_URL"],
    "sfUser": os.environ["SNOWFLAKE_USER"],
    "sfDatabase": os.environ["SNOWFLAKE_DATABASE"],
    "sfSchema": os.environ["SNOWFLAKE_SCHEMA"],
    "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "pem_private_key": get_private_key_string(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]),
}

R2_ENDPOINT = os.environ["R2_ENDPOINT"]
R2_ACCESS_KEY = os.environ["R2_ACCESS_KEY"]
R2_SECRET_KEY = os.environ["R2_SECRET_KEY"]


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def create_spark(app_name="tfl-backfill"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.jars.packages",
            ",".join([
                # Use the clean version string
                "net.snowflake:spark-snowflake_2.13:3.1.8",
                "net.snowflake:snowflake-jdbc:3.20.0",

                # AWS / S3A Support
                "org.apache.hadoop:hadoop-aws:3.4.1",
                "com.amazonaws:aws-java-sdk-bundle:1.12.767",
            ]),
        )
        .config("spark.hadoop.fs.s3a.endpoint", R2_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", R2_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", R2_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

def arrivals_schema():
    timing = StructType([
        StructField("countdownServerAdjustment", StringType(), True),
        StructField("source", StringType(), True),
        StructField("insert", StringType(), True),
        StructField("read", StringType(), True),
        StructField("sent", StringType(), True),
        StructField("received", StringType(), True),
    ])
    return StructType([
        StructField("id", StringType(), True),
        StructField("operationType", IntegerType(), True),
        StructField("vehicleId", StringType(), True),
        StructField("naptanId", StringType(), True),
        StructField("stationName", StringType(), True),
        StructField("lineId", StringType(), True),
        StructField("lineName", StringType(), True),
        StructField("platformName", StringType(), True),
        StructField("direction", StringType(), True),
        StructField("destinationNaptanId", StringType(), True),
        StructField("destinationName", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("timeToStation", IntegerType(), True),
        StructField("currentLocation", StringType(), True),
        StructField("towards", StringType(), True),
        StructField("expectedArrival", StringType(), True),
        StructField("timeToLive", StringType(), True),
        StructField("modeName", StringType(), True),
        StructField("timing", timing, True),
    ])


def crowding_schema():
    return StructType([
        StructField("dataAvailable", BooleanType(), True),
        StructField("percentageOfBaseline", DoubleType(), True),
        StructField("timeUtc", StringType(), True),
        StructField("timeLocal", StringType(), True),
    ])


def status_schema():
    line_status = StructType([
        StructField("id", IntegerType(), True),
        StructField("lineId", StringType(), True),
        StructField("statusSeverity", IntegerType(), True),
        StructField("statusSeverityDescription", StringType(), True),
        StructField("reason", StringType(), True),
        StructField("created", StringType(), True),
    ])
    return StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("modeName", StringType(), True),
        StructField("created", StringType(), True),
        StructField("modified", StringType(), True),
        StructField("lineStatuses", ArrayType(line_status), True),
    ])


def lift_disruptions_schema():
    return StructType([
        StructField("stationUniqueId", StringType(), True),
        StructField("disruptedLiftUniqueIds", ArrayType(StringType()), True),
        StructField("message", StringType(), True),
    ])


# ---------------------------------------------------------------------------
# Transforms — each produces a flat, typed DataFrame
# ---------------------------------------------------------------------------

def transform_arrivals(df):
    return df.select(
        F.col("naptanId").alias("station_id"),
        F.col("stationName").alias("station_name"),
        F.col("lineId").alias("line_id"),
        F.col("lineName").alias("line_name"),
        F.col("vehicleId").alias("vehicle_id"),
        F.col("direction"),
        F.col("platformName").alias("platform_name"),
        F.col("destinationNaptanId").alias("destination_naptan_id"),
        F.col("destinationName").alias("destination_name"),
        F.col("towards"),
        F.col("currentLocation").alias("current_location"),
        F.col("timeToStation").alias("time_to_station"),
        F.col("modeName").alias("mode_name"),
        F.to_timestamp("expectedArrival").alias("expected_arrival_ts"),
        F.to_timestamp("timestamp").alias("poll_ts"),
        F.to_timestamp("timing.sent").alias("api_sent_ts"),
        F.to_timestamp("timing.read").alias("api_read_ts"),
    )


def transform_crowding(df):
    # crowding files are single objects (not arrays), one per naptan per poll
    return df.select(
        F.col("dataAvailable").alias("data_available"),
        F.col("percentageOfBaseline").alias("pct_of_baseline"),
        F.to_timestamp("timeUtc").alias("crowding_ts"),
    )


def transform_status(df):
    # Explode the lineStatuses array so each severity gets its own row
    return (
        df.select(
            F.col("id").alias("line_id"),
            F.col("name").alias("line_name"),
            F.col("modeName").alias("mode_name"),
            F.to_timestamp("modified").alias("modified_ts"),
            F.explode("lineStatuses").alias("status"),
        )
        .select(
            "line_id",
            "line_name",
            "mode_name",
            "modified_ts",
            F.col("status.statusSeverity").alias("severity"),
            F.col("status.statusSeverityDescription").alias("severity_description"),
            F.col("status.reason").alias("disruption_reason"),
        )
    )


def transform_lift_disruptions(df):
    return df.select(
        F.col("stationUniqueId").alias("station_id"),
        F.explode("disruptedLiftUniqueIds").alias("lift_id"),
        F.col("message"),
    )


# ---------------------------------------------------------------------------
# Generic stream reader
# ---------------------------------------------------------------------------

def read_stream(spark, bucket, data_type, schema):
    return (
        spark.readStream
        .format("json")
        .schema(schema)
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.json")
        .option("maxFilesPerTrigger", 10)
        .load(f"s3a://{bucket}/bronze/tfl/{data_type}/")
    )


# ---------------------------------------------------------------------------
# Snowflake sink
# ---------------------------------------------------------------------------

def snowflake_foreach_batch(table):
    def _write(batch_df, batch_id):
        if batch_df.count() > 0:
            (
                batch_df.write
                .format("snowflake")
                .options(**SNOWFLAKE_OPTIONS)
                .option("dbtable", table)
                .mode("append")
                .save()
            )

    return _write


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def build_pipeline(spark, bucket, ckpt_base):
    streams = {
        "arrivals": (
            transform_arrivals(read_stream(spark, bucket, "arrivals", arrivals_schema())),
            "TFL_ARRIVALS_SILVER",
        ),
        "crowding": (
            transform_crowding(read_stream(spark, bucket, "crowding", crowding_schema())),
            "TFL_CROWDING_SILVER",
        ),
        "status": (
            transform_status(read_stream(spark, bucket, "status", status_schema())),
            "TFL_STATUS_SILVER",
        ),
        "lift_disruptions": (
            transform_lift_disruptions(read_stream(spark, bucket, "lift_disruptions", lift_disruptions_schema())),
            "TFL_LIFT_DISRUPTIONS_SILVER",
        ),
    }

    # --- Arrivals aggregation (15-min service frequency per line/station) ---
    arrivals_df, _ = streams["arrivals"]
    arrivals_agg = (
        arrivals_df
        .withWatermark("expected_arrival_ts", "30 minutes")
        .groupBy(
            F.window("expected_arrival_ts", "15 minutes"),
            F.col("line_id"),
            F.col("station_id"),
        )
        .agg(
            F.count("vehicle_id").alias("train_count"),
            F.avg("time_to_station").alias("avg_time_to_station"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "line_id",
            "station_id",
            "train_count",
            "avg_time_to_station",
        )
    )

    queries = []

    # Arrivals aggregation -> Snowflake + console
    queries.append(
        arrivals_agg.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", "false")
        .start()
    )
    queries.append(
        arrivals_agg.writeStream
        .foreachBatch(snowflake_foreach_batch("TFL_ARRIVALS_AGG_SILVER"))
        .option("checkpointLocation", f"{ckpt_base}/arrivals_agg")
        .outputMode("update")
        .start()
    )

    # Remaining streams -> Snowflake (append, no aggregation needed)
    for name, (df, table) in streams.items():
        if name == "arrivals":
            # raw arrivals rows
            queries.append(
                df.writeStream
                .foreachBatch(snowflake_foreach_batch(table))
                .option("checkpointLocation", f"{ckpt_base}/{name}")
                .start()
            )
        else:
            queries.append(
                df.writeStream
                .foreachBatch(snowflake_foreach_batch(table))
                .option("checkpointLocation", f"{ckpt_base}/{name}")
                .start()
            )

    return queries


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True, help="R2 bucket name (e.g. my-bucket)")
    parser.add_argument("--ckpt", required=True, help="Base R2 path for checkpoints (e.g. my-bucket/checkpoints)")
    args = parser.parse_args()

    spark = create_spark()
    queries = build_pipeline(spark, args.bucket, args.ckpt)

    try:
        for q in queries:
            q.awaitTermination()
    except KeyboardInterrupt:
        for q in queries:
            q.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
