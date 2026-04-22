#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TfL Arrival Inference: Stateful trip tracking via flatMapGroupsWithState.

Infers actual arrival times from polling disappearance and computes:
  - prediction_error  : last_expected_arrival - initial_expected_arrival
  - prediction_stability : variance of expectedArrival across all polls
"""

import math
from datetime import datetime, timezone
from typing import Iterator, Tuple

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType, LongType,
)

from tfl_pipeline import arrivals_schema, read_stream, snowflake_foreach_batch, SNOWFLAKE_OPTIONS


# ---------------------------------------------------------------------------
# State schema (serialised as a Row — Spark requires a registered encoder)
# We use Python dataclass-style via a plain tuple and annotate manually.
#
# State fields:
#   trip_key                  str
#   initial_expected_arrival  epoch seconds (float stored as double)
#   last_expected_arrival     epoch seconds
#   last_poll_ts              epoch seconds
#   expected_arrivals_sum     running sum  (for variance: E[X])
#   expected_arrivals_sq_sum  running sum of squares (for variance: E[X²])
#   poll_count                int
# ---------------------------------------------------------------------------

OUTPUT_SCHEMA = StructType([
    StructField("trip_key",                  StringType(),    False),
    StructField("initial_expected_arrival",  TimestampType(), True),
    StructField("actual_arrival_proxy",      TimestampType(), True),
    StructField("arrival_timestamp",         TimestampType(), True),
    StructField("prediction_error_seconds",  DoubleType(),    True),
    StructField("prediction_stability_var",  DoubleType(),    True),
    StructField("poll_count",                LongType(),      True),
])

# How long (seconds) without a poll sighting before a trip is declared arrived.
ARRIVAL_TIMEOUT_S = 120


# ---------------------------------------------------------------------------
# Trip key construction
# ---------------------------------------------------------------------------

def _add_trip_key(df):
    """
    vehicleId == "000" → composite key from line/destination/direction/station.
    Otherwise          → vehicleId + naptanId.
    """
    return df.withColumn(
        "trip_key",
        F.when(
            F.col("vehicle_id") == "000",
            F.concat_ws(
                "|",
                F.col("line_id"),
                F.col("destination_name"),
                F.col("direction"),
                F.col("station_id"),
            ),
        ).otherwise(
            F.concat_ws("|", F.col("vehicle_id"), F.col("station_id")),
        ),
    )


# ---------------------------------------------------------------------------
# State update function
# ---------------------------------------------------------------------------

def _update_trip_state(
    trip_key: str,
    rows: Iterator,
    state: GroupState,
) -> Iterator[Tuple]:
    """
    flatMapGroupsWithState handler.

    State tuple layout (index constants below for readability):
      0  initial_expected_arrival  (float, epoch s)
      1  last_expected_arrival     (float, epoch s)
      2  last_poll_ts              (float, epoch s)
      3  ea_sum                    (float)
      4  ea_sq_sum                 (float)
      5  poll_count                (int)
    """
    _INIT_EA = 0
    _LAST_EA = 1
    _LAST_POLL = 2
    _EA_SUM = 3
    _EA_SQ_SUM = 4
    _COUNT = 5

    def _to_epoch(ts) -> float:
        if ts is None:
            return 0.0
        if isinstance(ts, datetime):
            return ts.timestamp()
        # already a float/int
        return float(ts)

    def _emit(s) -> Tuple:
        init_ea = datetime.fromtimestamp(s[_INIT_EA], tz=timezone.utc)
        last_ea = datetime.fromtimestamp(s[_LAST_EA], tz=timezone.utc)
        last_poll = datetime.fromtimestamp(s[_LAST_POLL], tz=timezone.utc)

        error_s = s[_LAST_EA] - s[_INIT_EA]

        # Variance via Welford-equivalent: E[X²] - E[X]²
        n = s[_COUNT]
        if n > 1:
            mean = s[_EA_SUM] / n
            variance = max(0.0, (s[_EA_SQ_SUM] / n) - (mean ** 2))
        else:
            variance = 0.0

        return (trip_key, init_ea, last_ea, last_poll, error_s, variance, n)

    # --- Timeout: trip disappeared, emit and clear state ---
    if state.hasTimedOut:
        if state.exists:
            s = state.get
            yield _emit(s)
        state.remove()
        return

    # --- Process incoming rows for this micro-batch ---
    new_state = list(state.get) if state.exists else None

    for row in rows:
        ea_epoch = _to_epoch(row.expected_arrival_ts)
        poll_epoch = _to_epoch(row.poll_ts)

        if new_state is None:
            new_state = [ea_epoch, ea_epoch, poll_epoch, ea_epoch, ea_epoch ** 2, 1]
        else:
            new_state[_LAST_EA] = ea_epoch
            new_state[_LAST_POLL] = max(new_state[_LAST_POLL], poll_epoch)
            new_state[_EA_SUM] += ea_epoch
            new_state[_EA_SQ_SUM] += ea_epoch ** 2
            new_state[_COUNT] += 1

    if new_state is not None:
        state.update(tuple(new_state))
        # Reset the timeout window on every new observation.
        # Spark will fire hasTimedOut after ARRIVAL_TIMEOUT_S of event-time inactivity.
        state.setTimeoutDuration(ARRIVAL_TIMEOUT_S * 1000)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def build_inference_pipeline(spark: SparkSession, bucket: str, ckpt_base: str):
    from tfl_pipeline import transform_arrivals

    raw = read_stream(spark, bucket, "arrivals", arrivals_schema())
    arrivals = transform_arrivals(raw)
    keyed = _add_trip_key(arrivals)

    # flatMapGroupsWithState requires event-time watermark when using
    # EventTimeTimeout so Spark knows how far to advance the clock.
    watermarked = keyed.withWatermark("poll_ts", "5 minutes")

    # State tuple encoder: Spark needs to know the types.
    # We use a simple tuple of (float×5, int) — registered via applyInPandasWithState
    # for Python, but flatMapGroupsWithState works with typed Datasets in Scala.
    # In PySpark ≥3.4 we use the Python-native API:
    inferred = (
        watermarked
        .groupBy("trip_key")
        .flatMapGroupsWithState(
            outputMode="append",
            timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
            outputStructType=OUTPUT_SCHEMA,
            stateStructType=StructType([
                StructField("initial_expected_arrival", DoubleType(), False),
                StructField("last_expected_arrival",    DoubleType(), False),
                StructField("last_poll_ts",             DoubleType(), False),
                StructField("ea_sum",                   DoubleType(), False),
                StructField("ea_sq_sum",                DoubleType(), False),
                StructField("poll_count",               LongType(),   False),
            ]),
            func=_update_trip_state,
        )
    )

    # Console sink for local debugging
    console_query = (
        inferred.writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    # Snowflake sink
    sf_query = (
        inferred.writeStream
        .foreachBatch(snowflake_foreach_batch("TFL_ARRIVAL_METRICS_GOLD"))
        .option("checkpointLocation", f"{ckpt_base}/arrival_inference")
        .outputMode("append")
        .start()
    )

    return [console_query, sf_query]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from tfl_pipeline import create_spark

    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--ckpt",   required=True)
    args = parser.parse_args()

    spark = create_spark("tfl-arrival-inference")
    queries = build_inference_pipeline(spark, args.bucket, args.ckpt)

    try:
        for q in queries:
            q.awaitTermination()
    except KeyboardInterrupt:
        for q in queries:
            q.stop()
    finally:
        spark.stop()
