#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tfl_common.py
Shared config, Spark session factory, Snowflake writer, and path utilities.
All other pipeline modules import from here.

Required environment variables:
    R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET
    SNOWFLAKE_URL, SNOWFLAKE_USER, SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_PRIVATE_KEY_PATH
    CKPT_BASE   e.g. s3a://tfl-datasync/checkpoints/silver
"""

import os
import sys
from datetime import datetime, timedelta, UTC

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

try:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    def get_private_key_string(key_path: str, password: str = None) -> str:
        with open(key_path, "rb") as f:
            p_key = serialization.load_pem_private_key(
                f.read(),
                password=password.encode() if password else None,
                backend=default_backend(),
            )
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        s = pkb.decode("utf-8")
        s = s.replace("-----BEGIN PRIVATE KEY-----", "")
        s = s.replace("-----END PRIVATE KEY-----", "")
        return s.replace("\n", "")

except ImportError:
    def get_private_key_string(key_path, password=None):
        raise RuntimeError("cryptography package not installed")


# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

def _require_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        print(f"ERROR: required environment variable {name!r} is not set")
        sys.exit(1)
    return val


try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

R2_ENDPOINT   = _require_env("R2_ENDPOINT")
R2_ACCESS_KEY = _require_env("R2_ACCESS_KEY")
R2_SECRET_KEY = _require_env("R2_SECRET_KEY")
R2_BUCKET     = _require_env("R2_BUCKET")
CKPT_BASE     = os.environ.get("CKPT_BASE", f"s3a://{R2_BUCKET}/checkpoints/silver")

SNOWFLAKE_OPTIONS = {
    "sfURL":           _require_env("SNOWFLAKE_URL"),
    "sfUser":          _require_env("SNOWFLAKE_USER"),
    "sfDatabase":      _require_env("SNOWFLAKE_DATABASE"),
    "sfSchema":        _require_env("SNOWFLAKE_SCHEMA"),
    "sfWarehouse":     _require_env("SNOWFLAKE_WAREHOUSE"),
    "pem_private_key": get_private_key_string(_require_env("SNOWFLAKE_PRIVATE_KEY_PATH")),
}

PACKAGES = ",".join([
    "net.snowflake:spark-snowflake_2.13:2.12.0-spark_3.4",
    "net.snowflake:snowflake-jdbc:3.13.30",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

def create_spark(app_name: str = "tfl-pipeline") -> SparkSession:
    cores = os.environ.get("SPARK_WORKER_CORES", "*")
    return (
        SparkSession.builder
        .appName(app_name)
        .master(f"local[{cores}]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.hadoop.fs.s3a.endpoint", R2_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", R2_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", R2_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
        .config("spark.hadoop.fs.s3a.change.detection.version.required", "false")
        .config("spark.hadoop.fs.s3a.connection.ttl", "300000")
        .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "200000")
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400000")
        .config("spark.hadoop.fs.s3a.retry.interval", "500")
        .config("spark.hadoop.fs.s3a.retry.throttle.interval", "100")
        .config("spark.hadoop.fs.s3a.assumed.role.session.duration", "1800")
        .config("spark.snowflake.pushdown", "off")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# Snowflake writer
# ---------------------------------------------------------------------------

def write_snowflake(df, table: str, mode: str = "append"):
    """Writes a DataFrame to Snowflake, casting TimestampNTZ columns first."""
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
    print(f"  → wrote to {table}")


def make_foreachbatch_sink(table: str):
    """Returns a foreachBatch handler that writes each micro-batch to Snowflake."""
    def _write(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        for field in batch_df.schema.fields:
            if "TimestampNTZ" in str(field.dataType):
                batch_df = batch_df.withColumn(
                    field.name, F.col(field.name).cast(TimestampType())
                )
        (
            batch_df.write
            .format("net.snowflake.spark.snowflake")
            .options(**SNOWFLAKE_OPTIONS)
            .option("dbtable", table)
            .mode("append")
            .save()
        )
        print(f"  [batch {batch_id}] → {table}: {batch_df.count()} rows")
    return _write


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def hour_paths(data_type: str, start: datetime, end: datetime) -> list[str]:
    paths, cur = [], start.replace(minute=0, second=0, microsecond=0)
    while cur <= end:
        paths.append(
            f"s3a://{R2_BUCKET}/bronze/tfl/{data_type}/"
            f"year={cur.year}/month={cur.month:02d}/"
            f"day={cur.day:02d}/hour={cur.hour:02d}/"
        )
        cur += timedelta(hours=1)
    return paths


def day_paths(data_type: str, start: datetime, end: datetime) -> list[str]:
    paths, cur = [], start.replace(hour=0, minute=0, second=0, microsecond=0)
    while cur <= end:
        paths.append(
            f"s3a://{R2_BUCKET}/bronze/tfl/{data_type}/"
            f"year={cur.year}/month={cur.month:02d}/day={cur.day:02d}/"
        )
        cur += timedelta(days=1)
    return paths


def last_n_hours(data_type: str, n: int = 24) -> list[str]:
    now = datetime.now(UTC)
    return hour_paths(data_type, now - timedelta(hours=n), now)


def last_n_days(data_type: str, n: int = 7) -> list[str]:
    now = datetime.now(UTC)
    return day_paths(data_type, now - timedelta(days=n), now)