#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tfl_silver_runner.py
Streaming silver runner — called every 10 minutes by Airflow.

Uses availableNow trigger: processes all bronze files that have landed
since the last checkpoint, writes them to Snowflake silver tables, then exits.
The checkpoint guarantees no file is processed twice and no file is skipped.
"""

import argparse

from tfl_common import create_spark, make_foreachbatch_sink, CKPT_BASE, R2_BUCKET
from tfl_schemas import stream_bronze
from tfl_silver import SILVER_TRANSFORMS


def run(spark, ckpt_base: str):
    queries = []

    for data_type, (transform_fn, table) in SILVER_TRANSFORMS.items():
        base_path = f"s3a://{R2_BUCKET}/bronze/tfl/{data_type}/"

        raw = stream_bronze(spark, data_type, base_path)

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
        print(f"[silver] started {data_type} → {table}")

    for data_type, q in queries:
        q.awaitTermination()
        print(f"[silver] finished {data_type}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ckpt-base", default=CKPT_BASE,
                        help="Override checkpoint base path")
    args = parser.parse_args()

    spark = create_spark("tfl-silver")
    try:
        run(spark, args.ckpt_base)
    finally:
        spark.stop()