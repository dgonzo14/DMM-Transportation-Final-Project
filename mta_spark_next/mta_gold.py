from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from mta_spark_next.mta_common import (
    create_spark,
    delete_rows_by_values,
    ensure_compare_database_and_tables,
    get_spark_mta_settings,
    read_snowflake_query,
    table_fqn,
    write_snowflake,
)
from mta_spark_next.mta_schemas import GOLD_HISTORY_TABLE, GOLD_TABLES, SILVER_HISTORY_TABLE
from mta_spark_next.snowflake_io import sql_in_list, sql_quote
from mta_spark_next.utils import json_dumps, stable_hash, utc_now


LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


ARRIVAL_SCHEMA = StructType(
    [
        StructField("ARRIVAL_INFERENCE_KEY", StringType(), True),
        StructField("ROUTE_ID", StringType(), True),
        StructField("TRIP_ID", StringType(), True),
        StructField("START_DATE", StringType(), True),
        StructField("START_TIME", StringType(), True),
        StructField("TRAIN_ID", StringType(), True),
        StructField("DIRECTION", StringType(), True),
        StructField("STOP_ID", StringType(), True),
        StructField("FIRST_SEEN_INGESTED_AT", TimestampType(), True),
        StructField("LAST_SEEN_INGESTED_AT", TimestampType(), True),
        StructField("FIRST_PREDICTED_ARRIVAL_TS", TimestampType(), True),
        StructField("LAST_PREDICTED_ARRIVAL_TS", TimestampType(), True),
        StructField("FIRST_PREDICTED_DEPARTURE_TS", TimestampType(), True),
        StructField("LAST_PREDICTED_DEPARTURE_TS", TimestampType(), True),
        StructField("POLL_COUNT", LongType(), True),
        StructField("PREDICTION_DRIFT_SECONDS", DoubleType(), True),
        StructField("PREDICTION_VARIANCE_SECONDS", DoubleType(), True),
        StructField("OBSERVED_SPAN_SECONDS", DoubleType(), True),
        StructField("INFERRED_COMPLETION_PROXY_TS", TimestampType(), True),
        StructField("LAST_SOURCE_OBJECT_KEY", StringType(), True),
    ]
)

HEADWAY_SCHEMA = StructType(
    [
        StructField("HEADWAY_WINDOW_KEY", StringType(), True),
        StructField("ROUTE_ID", StringType(), True),
        StructField("DIRECTION", StringType(), True),
        StructField("STOP_ID", StringType(), True),
        StructField("WINDOW_START", TimestampType(), True),
        StructField("WINDOW_END", TimestampType(), True),
        StructField("TRIP_COUNT", LongType(), True),
        StructField("AVG_HEADWAY_SECONDS", DoubleType(), True),
        StructField("MIN_HEADWAY_SECONDS", DoubleType(), True),
        StructField("MAX_HEADWAY_SECONDS", DoubleType(), True),
    ]
)

ROUTE_SNAPSHOT_SCHEMA = StructType(
    [
        StructField("ROUTE_SNAPSHOT_KEY", StringType(), True),
        StructField("OBJECT_KEY", StringType(), True),
        StructField("INGESTION_ID", StringType(), True),
        StructField("FEED_NAME", StringType(), True),
        StructField("ROUTE_ID", StringType(), True),
        StructField("SNAPSHOT_TS", TimestampType(), True),
        StructField("TRIP_UPDATE_COUNT", LongType(), True),
        StructField("STOP_TIME_UPDATE_COUNT", LongType(), True),
        StructField("VEHICLE_POSITION_COUNT", LongType(), True),
        StructField("ALERT_COUNT", LongType(), True),
    ]
)

ALERT_ACTIVITY_SCHEMA = StructType(
    [
        StructField("ALERT_ACTIVITY_KEY", StringType(), True),
        StructField("ROUTE_ID", StringType(), True),
        StructField("WINDOW_START", TimestampType(), True),
        StructField("WINDOW_END", TimestampType(), True),
        StructField("ALERT_SNAPSHOT_COUNT", LongType(), True),
        StructField("DISTINCT_ALERT_ENTITY_COUNT", LongType(), True),
        StructField("MAX_INFORMED_ENTITY_COUNT", LongType(), True),
        StructField("ACTIVE_ALERT_COUNT", LongType(), True),
    ]
)

GOLD_HISTORY_SCHEMA = StructType(
    [
        StructField("OBJECT_KEY", StringType(), True),
        StructField("INGESTION_ID", StringType(), True),
        StructField("DAG_RUN_ID", StringType(), True),
        StructField("DATA_TYPE", StringType(), True),
        StructField("FEED_NAME", StringType(), True),
        StructField("SOURCE_URL", StringType(), True),
        StructField("INGESTED_AT", TimestampType(), True),
        StructField("STATUS", StringType(), True),
        StructField("ROW_COUNT", LongType(), True),
        StructField("TABLE_COUNTS_JSON", StringType(), True),
        StructField("ERROR_MESSAGE", StringType(), True),
    ]
)


def _parse_date_arg(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    datetime.strptime(value, "%Y-%m-%d")
    return value


def _is_empty(df: DataFrame) -> bool:
    return df.rdd.isEmpty()


def _empty_df(spark: SparkSession, schema: StructType) -> DataFrame:
    return spark.createDataFrame([], schema)


def _timestamp_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    return str(value)


@F.udf(returnType=StringType())
def _stable_hash2(part1: Any, part2: Any) -> str:
    return stable_hash(part1, part2)


@F.udf(returnType=StringType())
def _stable_hash4(part1: Any, part2: Any, part3: Any, part4: Any) -> str:
    return stable_hash(part1, part2, part3, part4)


@F.udf(returnType=StringType())
def _timestamp_iso_udf(value: Any) -> Optional[str]:
    return _timestamp_iso(value)


def _pending_gold_query(settings, start_date: Optional[str], end_date: Optional[str], force: bool, max_objects: Optional[int]) -> str:
    filters = ["STATUS = 'SUCCESS'"]
    gold_filters = ["g.STATUS = 'SUCCESS'", "g.OBJECT_KEY = s.OBJECT_KEY"]
    if start_date:
        filters.append(f"CAST(INGESTED_AT AS DATE) >= {sql_quote(start_date)}")
        gold_filters.append(f"CAST(g.INGESTED_AT AS DATE) >= {sql_quote(start_date)}")
    if end_date:
        filters.append(f"CAST(INGESTED_AT AS DATE) <= {sql_quote(end_date)}")
        gold_filters.append(f"CAST(g.INGESTED_AT AS DATE) <= {sql_quote(end_date)}")

    processed_filter = ""
    if not force:
        processed_filter = f"""
          AND NOT EXISTS (
              SELECT 1
              FROM {table_fqn(settings, GOLD_HISTORY_TABLE)} g
              WHERE {' AND '.join(gold_filters)}
          )
        """

    limit_clause = f"LIMIT {max_objects}" if max_objects else ""
    return f"""
        WITH silver_rows AS (
            SELECT
                OBJECT_KEY,
                INGESTION_ID,
                DATA_TYPE,
                FEED_NAME,
                SOURCE_URL,
                INGESTED_AT,
                ROW_NUMBER() OVER (
                    PARTITION BY OBJECT_KEY
                    ORDER BY INGESTED_AT DESC
                ) AS RN
            FROM {table_fqn(settings, SILVER_HISTORY_TABLE)}
            WHERE {' AND '.join(filters)}
        )
        SELECT OBJECT_KEY, INGESTION_ID, DATA_TYPE, FEED_NAME, SOURCE_URL, INGESTED_AT
        FROM silver_rows s
        WHERE RN = 1
        {processed_filter}
        ORDER BY INGESTED_AT
        {limit_clause}
    """


def _fetch_pending_gold_history_rows(
    spark: SparkSession,
    settings,
    start_date: Optional[str],
    end_date: Optional[str],
    force: bool,
    max_objects: Optional[int],
) -> DataFrame:
    return read_snowflake_query(
        spark,
        settings,
        _pending_gold_query(settings, start_date, end_date, force, max_objects),
    )


def _read_rows_by_object_keys(spark: SparkSession, settings, table_name: str, object_keys: Sequence[str]) -> DataFrame:
    if not object_keys:
        return read_snowflake_query(spark, settings, f"SELECT * FROM {table_fqn(settings, table_name)} WHERE 1 = 0")
    return read_snowflake_query(
        spark,
        settings,
        f"""
        SELECT *
        FROM {table_fqn(settings, table_name)}
        WHERE OBJECT_KEY IN ({sql_in_list(object_keys)})
        """,
    )


def _read_rows_by_stable_keys(spark: SparkSession, settings, stable_keys: Sequence[str]) -> DataFrame:
    if not stable_keys:
        return read_snowflake_query(
            spark,
            settings,
            f"SELECT * FROM {table_fqn(settings, 'MTA_STOP_TIME_UPDATES_SILVER')} WHERE 1 = 0",
        )
    return read_snowflake_query(
        spark,
        settings,
        f"""
        SELECT *
        FROM {table_fqn(settings, 'MTA_STOP_TIME_UPDATES_SILVER')}
        WHERE TRIP_STOP_STABLE_KEY IN ({sql_in_list(stable_keys)})
        """,
    )


def _collect_distinct_strings(df: DataFrame, column_name: str) -> List[str]:
    return [
        str(row[column_name])
        for row in df.select(column_name).where(F.col(column_name).isNotNull()).distinct().collect()
    ]


def _collect_delete_keys(df: DataFrame, column_name: str) -> List[str]:
    return _collect_distinct_strings(df, column_name)


def _compute_arrival_inference(spark: SparkSession, stop_updates_df: DataFrame) -> DataFrame:
    if _is_empty(stop_updates_df):
        return _empty_df(spark, ARRIVAL_SCHEMA)

    ordered = (
        Window.partitionBy("TRIP_STOP_STABLE_KEY")
        .orderBy(F.col("INGESTED_AT").asc_nulls_last(), F.col("STOP_SEQUENCE_INDEX").asc_nulls_last())
    )
    ordered_full = ordered.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    partition = Window.partitionBy("TRIP_STOP_STABLE_KEY")

    enriched = (
        stop_updates_df.withColumn("_rn", F.row_number().over(ordered))
        .withColumn("_first_ingested_at", F.first("INGESTED_AT", ignorenulls=False).over(ordered_full))
        .withColumn("_last_ingested_at", F.last("INGESTED_AT", ignorenulls=False).over(ordered_full))
        .withColumn("_first_arrival", F.first("ARRIVAL_TIME", ignorenulls=True).over(ordered_full))
        .withColumn("_last_arrival", F.last("ARRIVAL_TIME", ignorenulls=True).over(ordered_full))
        .withColumn("_first_departure", F.first("DEPARTURE_TIME", ignorenulls=True).over(ordered_full))
        .withColumn("_last_departure", F.last("DEPARTURE_TIME", ignorenulls=True).over(ordered_full))
        .withColumn("_last_object_key", F.last("OBJECT_KEY", ignorenulls=False).over(ordered_full))
        .withColumn("_last_route_id", F.last("ROUTE_ID", ignorenulls=False).over(ordered_full))
        .withColumn("_arrival_epoch", F.unix_timestamp("ARRIVAL_TIME").cast(DoubleType()))
        .withColumn("_prediction_variance", F.coalesce(F.var_pop("_arrival_epoch").over(partition), F.lit(0.0)))
        .withColumn("_poll_count", F.count(F.lit(1)).over(partition))
    )

    return (
        enriched.where(F.col("_rn") == 1)
        .select(
            F.col("TRIP_STOP_STABLE_KEY").alias("ARRIVAL_INFERENCE_KEY"),
            F.col("ROUTE_ID"),
            F.col("TRIP_ID"),
            F.col("START_DATE"),
            F.col("START_TIME"),
            F.col("TRAIN_ID"),
            F.col("DIRECTION"),
            F.col("STOP_ID"),
            F.col("_first_ingested_at").alias("FIRST_SEEN_INGESTED_AT"),
            F.col("_last_ingested_at").alias("LAST_SEEN_INGESTED_AT"),
            F.col("_first_arrival").alias("FIRST_PREDICTED_ARRIVAL_TS"),
            F.col("_last_arrival").alias("LAST_PREDICTED_ARRIVAL_TS"),
            F.col("_first_departure").alias("FIRST_PREDICTED_DEPARTURE_TS"),
            F.col("_last_departure").alias("LAST_PREDICTED_DEPARTURE_TS"),
            F.col("_poll_count").cast(LongType()).alias("POLL_COUNT"),
            F.when(
                F.col("_first_arrival").isNotNull() & F.col("_last_arrival").isNotNull(),
                (F.unix_timestamp("_last_arrival") - F.unix_timestamp("_first_arrival")).cast(DoubleType()),
            ).alias("PREDICTION_DRIFT_SECONDS"),
            F.col("_prediction_variance").cast(DoubleType()).alias("PREDICTION_VARIANCE_SECONDS"),
            F.when(
                F.col("_first_ingested_at").isNotNull() & F.col("_last_ingested_at").isNotNull(),
                (F.unix_timestamp("_last_ingested_at") - F.unix_timestamp("_first_ingested_at")).cast(DoubleType()),
            ).alias("OBSERVED_SPAN_SECONDS"),
            F.coalesce(F.col("_last_departure"), F.col("_last_arrival"), F.col("_last_ingested_at")).alias(
                "INFERRED_COMPLETION_PROXY_TS"
            ),
            F.col("_last_object_key").alias("LAST_SOURCE_OBJECT_KEY"),
        )
    )


def _fetch_arrival_gold_scope(spark: SparkSession, settings, arrival_df: DataFrame) -> DataFrame:
    if _is_empty(arrival_df):
        return _empty_df(spark, ARRIVAL_SCHEMA)

    working_df = (
        arrival_df.withColumn(
            "_observed_ts",
            F.coalesce(F.col("LAST_PREDICTED_ARRIVAL_TS"), F.col("LAST_PREDICTED_DEPARTURE_TS")),
        )
        .where(F.col("_observed_ts").isNotNull())
        .where(F.col("ROUTE_ID").isNotNull())
        .where(F.col("STOP_ID").isNotNull())
        .cache()
    )
    try:
        if _is_empty(working_df):
            return _empty_df(spark, ARRIVAL_SCHEMA)

        route_ids = _collect_distinct_strings(working_df, "ROUTE_ID")
        stop_ids = _collect_distinct_strings(working_df, "STOP_ID")
        bounds = working_df.agg(F.min("_observed_ts").alias("MIN_TS"), F.max("_observed_ts").alias("MAX_TS")).first()
        min_ts = _timestamp_iso(bounds["MIN_TS"])
        max_ts = _timestamp_iso(bounds["MAX_TS"] + timedelta(minutes=15))

        return read_snowflake_query(
            spark,
            settings,
            f"""
            SELECT *
            FROM {table_fqn(settings, 'MTA_ARRIVAL_INFERENCE_GOLD')}
            WHERE ROUTE_ID IN ({sql_in_list(route_ids)})
              AND STOP_ID IN ({sql_in_list(stop_ids)})
              AND COALESCE(LAST_PREDICTED_ARRIVAL_TS, LAST_PREDICTED_DEPARTURE_TS)
                  BETWEEN TO_TIMESTAMP_TZ({sql_quote(min_ts)})
                  AND TO_TIMESTAMP_TZ({sql_quote(max_ts)})
            """,
        )
    finally:
        working_df.unpersist()


def _compute_route_headways(spark: SparkSession, arrival_gold_df: DataFrame) -> DataFrame:
    if _is_empty(arrival_gold_df):
        return _empty_df(spark, HEADWAY_SCHEMA)

    working = (
        arrival_gold_df.withColumn(
            "_observed_ts",
            F.coalesce(F.col("LAST_PREDICTED_ARRIVAL_TS"), F.col("LAST_PREDICTED_DEPARTURE_TS")),
        )
        .where(F.col("_observed_ts").isNotNull())
        .where(F.col("ROUTE_ID").isNotNull())
        .where(F.col("STOP_ID").isNotNull())
    )
    if _is_empty(working):
        return _empty_df(spark, HEADWAY_SCHEMA)

    working = (
        working.withColumn(
            "WINDOW_START",
            F.from_unixtime(F.floor(F.unix_timestamp("_observed_ts") / F.lit(900)) * F.lit(900)).cast(TimestampType()),
        )
        .withColumn("WINDOW_END", F.expr("WINDOW_START + INTERVAL 15 MINUTES"))
    )
    group_cols = ["ROUTE_ID", "DIRECTION", "STOP_ID", "WINDOW_START", "WINDOW_END"]
    ordered = Window.partitionBy(*group_cols).orderBy(F.col("_observed_ts").asc_nulls_last())
    with_diffs = working.withColumn(
        "_headway_seconds",
        (F.unix_timestamp("_observed_ts") - F.unix_timestamp(F.lag("_observed_ts").over(ordered))).cast(DoubleType()),
    )

    return (
        with_diffs.groupBy(*group_cols)
        .agg(
            F.count(F.lit(1)).cast(LongType()).alias("TRIP_COUNT"),
            F.avg("_headway_seconds").cast(DoubleType()).alias("AVG_HEADWAY_SECONDS"),
            F.min("_headway_seconds").cast(DoubleType()).alias("MIN_HEADWAY_SECONDS"),
            F.max("_headway_seconds").cast(DoubleType()).alias("MAX_HEADWAY_SECONDS"),
        )
        .withColumn("_window_start_iso", _timestamp_iso_udf("WINDOW_START"))
        .withColumn("HEADWAY_WINDOW_KEY", _stable_hash4("ROUTE_ID", "DIRECTION", "STOP_ID", "_window_start_iso"))
        .select(
            "HEADWAY_WINDOW_KEY",
            "ROUTE_ID",
            "DIRECTION",
            "STOP_ID",
            "WINDOW_START",
            "WINDOW_END",
            "TRIP_COUNT",
            "AVG_HEADWAY_SECONDS",
            "MIN_HEADWAY_SECONDS",
            "MAX_HEADWAY_SECONDS",
        )
    )


def _empty_counts(spark: SparkSession, count_name: str) -> DataFrame:
    return spark.createDataFrame(
        [],
        StructType(
            [
                StructField("OBJECT_KEY", StringType(), True),
                StructField("FEED_NAME", StringType(), True),
                StructField("ROUTE_ID", StringType(), True),
                StructField(count_name, LongType(), True),
            ]
        ),
    )


def _counts_by_route(spark: SparkSession, frame: DataFrame, count_name: str) -> DataFrame:
    if _is_empty(frame):
        return _empty_counts(spark, count_name)
    return (
        frame.withColumn("ROUTE_ID", F.coalesce(F.col("ROUTE_ID"), F.lit("UNKNOWN")))
        .groupBy("OBJECT_KEY", "FEED_NAME", "ROUTE_ID")
        .agg(F.count(F.lit(1)).cast(LongType()).alias(count_name))
    )


def _compute_route_snapshot_summary(
    spark: SparkSession,
    snapshot_df: DataFrame,
    trip_df: DataFrame,
    stop_df: DataFrame,
    vehicle_df: DataFrame,
    alert_df: DataFrame,
) -> DataFrame:
    if _is_empty(snapshot_df):
        return _empty_df(spark, ROUTE_SNAPSHOT_SCHEMA)

    trip_counts = _counts_by_route(spark, trip_df, "TRIP_UPDATE_COUNT")
    stop_counts = _counts_by_route(spark, stop_df, "STOP_TIME_UPDATE_COUNT")
    vehicle_counts = _counts_by_route(spark, vehicle_df, "VEHICLE_POSITION_COUNT")
    alert_counts = _counts_by_route(spark, alert_df, "ALERT_COUNT")

    merged = (
        trip_counts.join(stop_counts, ["OBJECT_KEY", "FEED_NAME", "ROUTE_ID"], "full_outer")
        .join(vehicle_counts, ["OBJECT_KEY", "FEED_NAME", "ROUTE_ID"], "full_outer")
        .join(alert_counts, ["OBJECT_KEY", "FEED_NAME", "ROUTE_ID"], "full_outer")
    )
    if _is_empty(merged):
        return _empty_df(spark, ROUTE_SNAPSHOT_SCHEMA)

    for column_name in ["TRIP_UPDATE_COUNT", "STOP_TIME_UPDATE_COUNT", "VEHICLE_POSITION_COUNT", "ALERT_COUNT"]:
        merged = merged.withColumn(column_name, F.coalesce(F.col(column_name), F.lit(0)).cast(LongType()))

    snapshots = (
        snapshot_df.select("OBJECT_KEY", "INGESTION_ID", "FEED_NAME", "INGESTED_AT", "FEED_TIMESTAMP")
        .dropDuplicates()
        .withColumn("SNAPSHOT_TS", F.coalesce(F.col("FEED_TIMESTAMP"), F.col("INGESTED_AT")))
        .select("OBJECT_KEY", "INGESTION_ID", "FEED_NAME", "SNAPSHOT_TS")
    )

    return (
        merged.join(snapshots, ["OBJECT_KEY", "FEED_NAME"], "left")
        .withColumn("ROUTE_SNAPSHOT_KEY", _stable_hash2("OBJECT_KEY", "ROUTE_ID"))
        .select(
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
        )
    )


def _fetch_alert_scope(spark: SparkSession, settings, new_alert_df: DataFrame) -> DataFrame:
    if _is_empty(new_alert_df):
        return read_snowflake_query(
            spark,
            settings,
            f"SELECT * FROM {table_fqn(settings, 'MTA_ALERTS_SILVER')} WHERE 1 = 0",
        )

    working_df = (
        new_alert_df.withColumn("ROUTE_ID", F.coalesce(F.col("ROUTE_ID"), F.lit("UNKNOWN")))
        .withColumn("_observed_ts", F.coalesce(F.col("FEED_TIMESTAMP"), F.col("INGESTED_AT")))
        .where(F.col("_observed_ts").isNotNull())
        .withColumn("_window_start", F.date_trunc("hour", F.col("_observed_ts")))
        .cache()
    )
    try:
        if _is_empty(working_df):
            return read_snowflake_query(
                spark,
                settings,
                f"SELECT * FROM {table_fqn(settings, 'MTA_ALERTS_SILVER')} WHERE 1 = 0",
            )

        route_ids = _collect_distinct_strings(working_df, "ROUTE_ID")
        bounds = working_df.agg(F.min("_window_start").alias("MIN_TS"), F.max("_window_start").alias("MAX_TS")).first()
        min_ts = _timestamp_iso(bounds["MIN_TS"])
        max_ts = _timestamp_iso(bounds["MAX_TS"] + timedelta(hours=1))

        return read_snowflake_query(
            spark,
            settings,
            f"""
            SELECT *
            FROM {table_fqn(settings, 'MTA_ALERTS_SILVER')}
            WHERE COALESCE(ROUTE_ID, 'UNKNOWN') IN ({sql_in_list(route_ids)})
              AND COALESCE(FEED_TIMESTAMP, INGESTED_AT)
                  BETWEEN TO_TIMESTAMP_TZ({sql_quote(min_ts)})
                  AND TO_TIMESTAMP_TZ({sql_quote(max_ts)})
            """,
        )
    finally:
        working_df.unpersist()


def _compute_alert_activity(spark: SparkSession, alert_df: DataFrame) -> DataFrame:
    if _is_empty(alert_df):
        return _empty_df(spark, ALERT_ACTIVITY_SCHEMA)

    working = (
        alert_df.withColumn("ROUTE_ID", F.coalesce(F.col("ROUTE_ID"), F.lit("UNKNOWN")))
        .withColumn("_observed_ts", F.coalesce(F.col("FEED_TIMESTAMP"), F.col("INGESTED_AT")))
        .where(F.col("_observed_ts").isNotNull())
    )
    if _is_empty(working):
        return _empty_df(spark, ALERT_ACTIVITY_SCHEMA)

    working = (
        working.withColumn("WINDOW_START", F.date_trunc("hour", F.col("_observed_ts")))
        .withColumn("WINDOW_END", F.expr("WINDOW_START + INTERVAL 1 HOUR"))
        .withColumn(
            "_active_alert_flag",
            F.when(F.col("ACTIVE_PERIOD_END").isNull() | (F.col("ACTIVE_PERIOD_END") >= F.col("WINDOW_START")), 1).otherwise(0),
        )
    )

    return (
        working.groupBy("ROUTE_ID", "WINDOW_START", "WINDOW_END")
        .agg(
            F.count(F.lit(1)).cast(LongType()).alias("ALERT_SNAPSHOT_COUNT"),
            F.countDistinct("ENTITY_ID").cast(LongType()).alias("DISTINCT_ALERT_ENTITY_COUNT"),
            F.max(F.coalesce(F.col("INFORMED_ENTITY_COUNT"), F.lit(0))).cast(LongType()).alias("MAX_INFORMED_ENTITY_COUNT"),
            F.sum("_active_alert_flag").cast(LongType()).alias("ACTIVE_ALERT_COUNT"),
        )
        .withColumn("_window_start_iso", _timestamp_iso_udf("WINDOW_START"))
        .withColumn("ALERT_ACTIVITY_KEY", _stable_hash2("ROUTE_ID", "_window_start_iso"))
        .select(
            "ALERT_ACTIVITY_KEY",
            "ROUTE_ID",
            "WINDOW_START",
            "WINDOW_END",
            "ALERT_SNAPSHOT_COUNT",
            "DISTINCT_ALERT_ENTITY_COUNT",
            "MAX_INFORMED_ENTITY_COUNT",
            "ACTIVE_ALERT_COUNT",
        )
    )


def _upsert_gold_frame(settings, df: DataFrame, table_name: str, key_column: str) -> int:
    cached = df.cache()
    try:
        row_count = cached.count()
        if row_count == 0:
            return 0
        delete_rows_by_values(settings, table_name, key_column, _collect_delete_keys(cached, key_column))
        write_snowflake(cached, table_name, settings)
        return int(row_count)
    finally:
        cached.unpersist()


def _write_history(spark: SparkSession, settings, history_df: DataFrame) -> int:
    cached = history_df.cache()
    try:
        row_count = cached.count()
        if row_count:
            write_snowflake(cached, GOLD_HISTORY_TABLE, settings)
        return int(row_count)
    finally:
        cached.unpersist()


def _success_history_df(pending_df: DataFrame, dag_run_id: str, table_counts: Dict[str, int]) -> DataFrame:
    total_row_count = int(sum(table_counts.values()))
    table_counts_json = json_dumps(table_counts)
    return pending_df.select(
        "OBJECT_KEY",
        "INGESTION_ID",
        F.lit(dag_run_id).alias("DAG_RUN_ID"),
        "DATA_TYPE",
        "FEED_NAME",
        "SOURCE_URL",
        "INGESTED_AT",
        F.lit("SUCCESS").alias("STATUS"),
        F.lit(total_row_count).cast(LongType()).alias("ROW_COUNT"),
        F.lit(table_counts_json).alias("TABLE_COUNTS_JSON"),
        F.lit(None).cast(StringType()).alias("ERROR_MESSAGE"),
    )


def _failure_history_df(pending_df: DataFrame, dag_run_id: str, error_message: str) -> DataFrame:
    return pending_df.select(
        "OBJECT_KEY",
        "INGESTION_ID",
        F.lit(dag_run_id).alias("DAG_RUN_ID"),
        "DATA_TYPE",
        "FEED_NAME",
        "SOURCE_URL",
        "INGESTED_AT",
        F.lit("FAILED").alias("STATUS"),
        F.lit(0).cast(LongType()).alias("ROW_COUNT"),
        F.lit(json_dumps({})).alias("TABLE_COUNTS_JSON"),
        F.lit(error_message).alias("ERROR_MESSAGE"),
    )


def _sorted_object_keys(pending_rows: Iterable[Any]) -> List[str]:
    return [str(row.OBJECT_KEY) for row in pending_rows if row.OBJECT_KEY]


def run_mta_gold_inference_once(
    dag_run_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    force: bool = False,
    max_objects: Optional[int] = None,
    target_database: Optional[str] = None,
) -> Dict[str, Any]:
    dag_run_id = dag_run_id or f"spark_gold__{utc_now().isoformat()}"
    start_date = _parse_date_arg(start_date)
    end_date = _parse_date_arg(end_date)

    settings = get_spark_mta_settings(target_database=target_database)
    ensure_compare_database_and_tables(settings, [GOLD_HISTORY_TABLE, *GOLD_TABLES])

    spark = create_spark(settings, app_name="mta-spark-gold")
    pending_df: Optional[DataFrame] = None
    try:
        pending_df = _fetch_pending_gold_history_rows(spark, settings, start_date, end_date, force, max_objects).cache()
        if _is_empty(pending_df):
            summary = {
                "dag_run_id": dag_run_id,
                "objects_considered": 0,
                "objects_succeeded": 0,
                "objects_failed": 0,
            }
            LOGGER.info("No pending MTA gold objects. Summary: %s", summary)
            return summary

        pending_rows = pending_df.collect()
        object_keys = _sorted_object_keys(pending_rows)

        try:
            snapshot_df = _read_rows_by_object_keys(spark, settings, "MTA_FEED_SNAPSHOTS_SILVER", object_keys).cache()
            trip_df = _read_rows_by_object_keys(spark, settings, "MTA_TRIP_UPDATES_SILVER", object_keys).cache()
            stop_df_new = _read_rows_by_object_keys(spark, settings, "MTA_STOP_TIME_UPDATES_SILVER", object_keys).cache()
            vehicle_df = _read_rows_by_object_keys(spark, settings, "MTA_VEHICLE_POSITIONS_SILVER", object_keys).cache()
            alert_df_new = _read_rows_by_object_keys(spark, settings, "MTA_ALERTS_SILVER", object_keys).cache()
            try:
                impacted_stable_keys = _collect_distinct_strings(stop_df_new, "TRIP_STOP_STABLE_KEY")
                stop_df_scope = _read_rows_by_stable_keys(spark, settings, impacted_stable_keys).cache()
                try:
                    arrival_df = _compute_arrival_inference(spark, stop_df_scope)
                    arrival_count = _upsert_gold_frame(
                        settings,
                        arrival_df,
                        "MTA_ARRIVAL_INFERENCE_GOLD",
                        "ARRIVAL_INFERENCE_KEY",
                    )

                    arrival_scope_df = _fetch_arrival_gold_scope(spark, settings, arrival_df).cache()
                    try:
                        headway_df = _compute_route_headways(spark, arrival_scope_df)
                        route_snapshot_df = _compute_route_snapshot_summary(
                            spark,
                            snapshot_df,
                            trip_df,
                            stop_df_new,
                            vehicle_df,
                            alert_df_new,
                        )
                        alert_scope_df = _fetch_alert_scope(spark, settings, alert_df_new).cache()
                        try:
                            alert_activity_df = _compute_alert_activity(spark, alert_scope_df)

                            table_counts = {
                                "MTA_ARRIVAL_INFERENCE_GOLD": arrival_count,
                                "MTA_ROUTE_HEADWAYS_GOLD": _upsert_gold_frame(
                                    settings,
                                    headway_df,
                                    "MTA_ROUTE_HEADWAYS_GOLD",
                                    "HEADWAY_WINDOW_KEY",
                                ),
                                "MTA_ROUTE_SNAPSHOT_SUMMARY_GOLD": _upsert_gold_frame(
                                    settings,
                                    route_snapshot_df,
                                    "MTA_ROUTE_SNAPSHOT_SUMMARY_GOLD",
                                    "ROUTE_SNAPSHOT_KEY",
                                ),
                                "MTA_ALERT_ACTIVITY_GOLD": _upsert_gold_frame(
                                    settings,
                                    alert_activity_df,
                                    "MTA_ALERT_ACTIVITY_GOLD",
                                    "ALERT_ACTIVITY_KEY",
                                ),
                            }
                        finally:
                            alert_scope_df.unpersist()
                    finally:
                        arrival_scope_df.unpersist()
                finally:
                    stop_df_scope.unpersist()
            finally:
                for df in [snapshot_df, trip_df, stop_df_new, vehicle_df, alert_df_new]:
                    df.unpersist()

            history_count = _write_history(spark, settings, _success_history_df(pending_df, dag_run_id, table_counts))
            summary = {
                "dag_run_id": dag_run_id,
                "objects_considered": len(pending_rows),
                "objects_succeeded": history_count,
                "objects_failed": 0,
                "table_counts": table_counts,
            }
            LOGGER.info("MTA Spark gold inference summary: %s", summary)
            return summary
        except Exception as exc:
            LOGGER.exception("MTA Spark gold inference failed for dag_run_id=%s", dag_run_id)
            _write_history(spark, settings, _failure_history_df(pending_df, dag_run_id, str(exc)))
            raise
    finally:
        if pending_df is not None:
            pending_df.unpersist()
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Compute MTA gold analytics incrementally with Spark.")
    parser.add_argument("--dag-run-id", help="Optional Airflow DAG run id.")
    parser.add_argument("--start-date", help="Inclusive start date filter in YYYY-MM-DD.")
    parser.add_argument("--end-date", help="Inclusive end date filter in YYYY-MM-DD.")
    parser.add_argument("--force", action="store_true", help="Recompute gold outputs even if history shows success.")
    parser.add_argument("--max-objects", type=int, help="Optional cap for one-shot runs.")
    parser.add_argument("--target-database", help="Override MTA_SPARK_SNOWFLAKE_DATABASE for this run.")
    args = parser.parse_args()

    run_mta_gold_inference_once(
        dag_run_id=args.dag_run_id,
        start_date=args.start_date,
        end_date=args.end_date,
        force=args.force,
        max_objects=args.max_objects,
        target_database=args.target_database,
    )


if __name__ == "__main__":
    main()
