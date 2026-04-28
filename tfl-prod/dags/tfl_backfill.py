#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tfl_backfill.py
Ad-hoc historical backfill for all four data types.

Reads bronze files for a given date range and writes to silver
and (for arrivals) optionally gold tables.

Usage:
    # Backfill all types sequentially
    python tfl_backfill.py --type all --start 2026-04-01 --end 2026-04-27

    # Single type
    python tfl_backfill.py --type arrivals --start 2026-04-20

    # Arrivals silver + gold in one pass
    python tfl_backfill.py --type arrivals --start 2026-04-20 --gold

    # All types in parallel (recommended with 8 cores)
    python tfl_backfill.py --type all --start 2026-04-20 --gold --parallel
"""

import argparse
import multiprocessing as mp
from datetime import datetime, UTC

from tfl_common import create_spark, write_snowflake, day_paths
from tfl_schemas import read_bronze
from tfl_silver import SILVER_TRANSFORMS
from tfl_gold import build_arrival_performance


# Arrivals is much larger than the other three combined so it gets more cores.
# Total = 8, matching the machine. Adjust if running on different hardware.
CORE_ALLOCATION = {
    "arrivals":         "2",
    "crowding":         "4",
    "status":           "1",
    "lift_disruptions": "1",
}


def backfill_type(spark, data_type: str, start: datetime, end: datetime,
                  include_gold: bool = False):
    paths = day_paths(data_type, start, end)
    print(f"\n[backfill] {data_type}: {len(paths)} day-paths "
          f"({start.date()} -> {end.date()})")

    if not paths:
        print("  no paths generated -- check your date range")
        return

    transform_fn, table = SILVER_TRANSFORMS[data_type]
    raw    = read_bronze(spark, data_type, paths)
    silver = transform_fn(raw)

    write_snowflake(silver, table)
    print(f"  {table}: {silver.count()} rows")

    if include_gold and data_type == "arrivals":
        print("[backfill] building gold from same paths...")
        gold = build_arrival_performance(raw)
        write_snowflake(gold, "TFL_ARRIVAL_PERFORMANCE_GOLD")
        print(f"  TFL_ARRIVAL_PERFORMANCE_GOLD: {gold.count()} rows")


def _backfill_worker(data_type: str, start: datetime, end: datetime,
                     include_gold: bool):
    """
    Runs in a subprocess -- each worker gets its own SparkSession.
    Uses spawn (not fork) to avoid Py4J/JVM conflicts between contexts.
    Core count is capped per worker based on CORE_ALLOCATION.
    """
    import os
    os.environ["SPARK_WORKER_CORES"] = CORE_ALLOCATION.get(data_type, "2")
    spark = create_spark(f"tfl-backfill-{data_type}")
    try:
        backfill_type(spark, data_type, start, end, include_gold=include_gold)
    finally:
        spark.stop()


def run_parallel(types: list, start: datetime, end: datetime,
                 include_gold: bool):
    # spawn avoids JVM/Py4J conflicts between forked Spark contexts
    ctx = mp.get_context("spawn")
    procs = []

    for data_type in types:
        p = ctx.Process(
            target=_backfill_worker,
            args=(data_type, start, end, include_gold),
            name=f"backfill-{data_type}",
        )
        p.start()
        procs.append((data_type, p))
        cores = CORE_ALLOCATION.get(data_type, "2")
        print(f"  started {data_type} (pid {p.pid}, {cores} cores)")

    failed = []
    for data_type, p in procs:
        p.join()
        if p.exitcode != 0:
            failed.append(data_type)
            print(f"  FAIL {data_type} (exit code {p.exitcode})")
        else:
            print(f"  DONE {data_type}")

    if failed:
        print(f"\n[backfill] {len(failed)} type(s) failed: {failed}")
        raise SystemExit(1)

    print("\n[backfill] all types complete")


def run_sequential(types: list, start: datetime, end: datetime,
                   include_gold: bool):
    spark = create_spark("tfl-backfill")
    try:
        for data_type in types:
            backfill_type(spark, data_type, start, end, include_gold=include_gold)
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", required=True,
                        choices=[*SILVER_TRANSFORMS.keys(), "all"])
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end",   help="YYYY-MM-DD (default: today)")
    parser.add_argument("--gold",  action="store_true",
                        help="Also build gold tables (arrivals only)")
    parser.add_argument("--parallel", action="store_true",
                        help="Run all types in parallel subprocesses "
                             "(recommended with 8 cores, ignored for single type)")
    args = parser.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=UTC)
    end   = (datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=UTC)
             if args.end else datetime.now(UTC))
    types = list(SILVER_TRANSFORMS.keys()) if args.type == "all" else [args.type]

    if args.parallel and len(types) > 1:
        print(f"[backfill] parallel mode -- {len(types)} types across 8 cores")
        run_parallel(types, start, end, include_gold=args.gold)
    else:
        if args.parallel and len(types) == 1:
            print("[backfill] only one type specified, running sequentially")
        run_sequential(types, start, end, include_gold=args.gold)


if __name__ == "__main__":
    main()