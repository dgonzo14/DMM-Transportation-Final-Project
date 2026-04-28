#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tfl_silver_runner.py
Streaming silver runner — called every 10 minutes by Airflow.

Uses availableNow trigger: processes all bronze files that have landed
since the last checkpoint, writes them to Snowflake silver tables, then exits.
The checkpoint guarantees no file is processed twice and no file is skipped.

Usage:
    # Normal run (Airflow) — checkpoint tracks where it left off
    python tfl_silver_runner.py

    # First run after a manual backfill — only load paths from this date onwards
    # instead of scanning the full bucket root
    python tfl_silver_runner.py --since 2026-04-28

    # Override checkpoint location
    python tfl_silver_runner.py --ckpt-base s3a://tfl-datasync/checkpoints/silver
"""

import argparse
from datetime import datetime, timedelta, UTC

from tfl_common import create_spark, make_foreachbatch_sink, CKPT_BASE, R2_BUCKET, day_paths
from tfl_schemas import stream_bronze
from tfl_silver import SILVER_TRANSFORMS


def _base_path(data_type: str) -> str:
    return f"s3a://{R2_BUCKET}/bronze/tfl/{data_type}/"


def _since_paths(data_type: str, since: datetime) -> list[str]:
    """
    Returns explicit day-level paths from since to today + 1.
    Used instead of the full bucket root when --since is specified,
    so the stream only sees files in those partitions.
    """
    return day_paths(data_type, since, datetime.now(UTC) + timedelta(days=1))


def run(spark, ckpt_base: str, since: datetime = None):
    queries = []

    for data_type, (transform_fn, table) in SILVER_TRANSFORMS.items():
        if since:
            # Streaming reader only accepts a single path string.
            # Point it at the year/month level of the since date so it only
            # sees partitions from that month onwards, ignoring older months.
            # For cross-month ranges this uses the data type root (safe fallback).
            now = datetime.now(UTC)
            if since.year == now.year and since.month == now.month:
                load_path = (
                    f"s3a://{R2_BUCKET}/bronze/tfl/{data_type}/"
                    f"year={since.year}/month={since.month:02d}/"
                )
            else:
                load_path = _base_path(data_type)
            since_str = f" (scanning from {load_path})"
        else:
            load_path = _base_path(data_type)
            since_str = ""

        raw = stream_bronze(spark, data_type, load_path)

        def _foreachbatch(batch_df, batch_id,
                          _transform=transform_fn, _table=table):
            if batch_df.isEmpty():
                return
            transformed = _transform(batch_df)
            make_foreachbatch_sink(_table)(transformed, batch_id)

        q = (
            raw.writeStream
            .foreachBatch(_foreachbatch)
            .option("checkpointLocation", f"{ckpt_base}/{data_type}")
            .trigger(availableNow=True)
            .start()
        )
        queries.append((data_type, q))
        print(f"[silver] started {data_type} -> {table}{since_str}")

    for data_type, q in queries:
        q.awaitTermination()
        print(f"[silver] finished {data_type}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ckpt-base", default=CKPT_BASE,
        help="Override checkpoint base path",
    )
    parser.add_argument(
        "--since",
        help=(
            "Only process files in partitions on or after this date (YYYY-MM-DD). "
            "Use on the first Airflow run after a manual backfill to skip "
            "historical partitions entirely. "
            "Once the checkpoint is established this flag is no longer needed."
        ),
    )
    args = parser.parse_args()

    since = None
    if args.since:
        since = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=UTC)
        print(f"[silver] --since {args.since}: loading only partitions from this date forwards")

    spark = create_spark("tfl-silver")
    try:
        run(spark, args.ckpt_base, since=since)
    finally:
        spark.stop()