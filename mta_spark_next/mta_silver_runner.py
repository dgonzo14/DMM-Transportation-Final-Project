from __future__ import annotations

import argparse
from datetime import datetime
from typing import Any, Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from mta_spark_next.mta_schemas import SILVER_HISTORY_TABLE, SILVER_TABLES
from mta_spark_next.utils import utc_now
from mta_spark_next.mta_common import (
    bronze_base_path,
    bronze_day_paths,
    create_spark,
    delete_rows_by_object_keys,
    ensure_compare_database_and_tables,
    get_spark_mta_settings,
    read_bronze_text_batch,
    read_bronze_text_stream,
    write_snowflake,
)
from mta_spark_next.mta_silver import (
    LONG_COLUMNS,
    RECORD_SCHEMA,
    TABLE_JSON_SCHEMAS,
    TIMESTAMP_COLUMNS,
    partition_silver_records,
)


BRONZE_TYPES = ["full_feed", "trip_updates", "vehicle_positions", "alerts"]


def _parse_date_arg(value: Optional[str]):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def _authoritative_bronze_type(bronze_type: str) -> str:
    if bronze_type == "all":
        return "full_feed"
    if bronze_type not in BRONZE_TYPES:
        raise ValueError(f"Unsupported bronze type: {bronze_type}")
    return bronze_type


def _materialize_table(parsed_df: DataFrame, table_name: str) -> DataFrame:
    df = (
        parsed_df.where(F.col("table_name") == table_name)
        .select(F.from_json(F.col("row_json"), TABLE_JSON_SCHEMAS[table_name]).alias("row"))
        .select("row.*")
    )
    for column_name in TIMESTAMP_COLUMNS.get(table_name, []):
        df = df.withColumn(column_name, F.to_timestamp(F.col(column_name)))
    for column_name in LONG_COLUMNS.get(table_name, []):
        df = df.withColumn(column_name, F.col(column_name).cast(LongType()))
    return df


def _records_for_batch(batch_df: DataFrame, dag_run_id: str) -> DataFrame:
    spark = batch_df.sparkSession
    records_rdd = batch_df.select("value").rdd.mapPartitions(
        lambda rows: partition_silver_records(rows, dag_run_id=dag_run_id)
    )
    return spark.createDataFrame(records_rdd, schema=RECORD_SCHEMA)


def _write_batch(batch_df: DataFrame, batch_id: int, settings, dag_run_id: str) -> None:
    if batch_df.rdd.isEmpty():
        print(f"[mta-silver] batch {batch_id}: no files")
        return

    parsed_df = _records_for_batch(batch_df, dag_run_id=dag_run_id).cache()
    history_df = None
    try:
        if parsed_df.rdd.isEmpty():
            print(f"[mta-silver] batch {batch_id}: no records")
            return

        history_df = _materialize_table(parsed_df, SILVER_HISTORY_TABLE).cache()
        success_keys = [
            row.OBJECT_KEY
            for row in history_df.where(F.col("STATUS") == "SUCCESS")
            .select("OBJECT_KEY")
            .distinct()
            .collect()
            if row.OBJECT_KEY
        ]
        delete_rows_by_object_keys(settings, SILVER_TABLES, success_keys)

        for table_name in SILVER_TABLES:
            out_df = _materialize_table(parsed_df, table_name)
            row_count = out_df.count()
            if row_count == 0:
                continue
            write_snowflake(out_df, table_name, settings)
            print(f"[mta-silver] batch {batch_id}: wrote {row_count} rows to {table_name}")

        history_count = history_df.count()
        if history_count:
            write_snowflake(history_df, SILVER_HISTORY_TABLE, settings)
            print(f"[mta-silver] batch {batch_id}: wrote {history_count} rows to {SILVER_HISTORY_TABLE}")
    finally:
        if history_df is not None:
            history_df.unpersist()
        parsed_df.unpersist()


def run_available_now(
    spark: SparkSession,
    settings,
    bronze_type: str,
    dag_run_id: str,
    checkpoint_base: str,
    max_files_per_trigger: int,
) -> Dict[str, Any]:
    source_path = bronze_base_path(settings, bronze_type)
    print(f"[mta-silver] source: {source_path}")
    print(f"[mta-silver] target database: {settings.target_database}.{settings.target_schema}")
    print(f"[mta-silver] checkpoint: {checkpoint_base}/{bronze_type}")

    raw = read_bronze_text_stream(
        spark=spark,
        base_path=source_path,
        max_files_per_trigger=max_files_per_trigger,
    )
    query = (
        raw.writeStream.foreachBatch(
            lambda batch_df, batch_id: _write_batch(
                batch_df=batch_df,
                batch_id=batch_id,
                settings=settings,
                dag_run_id=dag_run_id,
            )
        )
        .option("checkpointLocation", f"{checkpoint_base.rstrip('/')}/{bronze_type}")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    return {"dag_run_id": dag_run_id, "bronze_type": bronze_type, "mode": "availableNow"}


def run_batch_dates(
    spark: SparkSession,
    settings,
    bronze_type: str,
    dag_run_id: str,
    start_date: str,
    end_date: Optional[str],
) -> Dict[str, Any]:
    start = _parse_date_arg(start_date)
    end = _parse_date_arg(end_date) if end_date else start
    paths = bronze_day_paths(settings, bronze_type, start, end)
    print(f"[mta-silver] batch paths: {paths}")
    raw = read_bronze_text_batch(spark=spark, paths=paths)
    _write_batch(raw, batch_id=0, settings=settings, dag_run_id=dag_run_id)
    return {
        "dag_run_id": dag_run_id,
        "bronze_type": bronze_type,
        "mode": "batch",
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
    }


def run_mta_spark_silver(
    dag_run_id: Optional[str] = None,
    bronze_type: str = "full_feed",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    checkpoint_base: Optional[str] = None,
    max_files_per_trigger: int = 50,
    target_database: Optional[str] = None,
) -> Dict[str, Any]:
    bronze_type = _authoritative_bronze_type(bronze_type)
    dag_run_id = dag_run_id or f"spark_silver__{utc_now().isoformat()}"
    settings = get_spark_mta_settings(target_database=target_database)
    checkpoint_base = checkpoint_base or settings.checkpoint_base

    ensure_compare_database_and_tables(settings)
    spark = create_spark(settings, app_name="mta-spark-silver")
    try:
        if start_date:
            return run_batch_dates(
                spark=spark,
                settings=settings,
                bronze_type=bronze_type,
                dag_run_id=dag_run_id,
                start_date=start_date,
                end_date=end_date,
            )
        return run_available_now(
            spark=spark,
            settings=settings,
            bronze_type=bronze_type,
            dag_run_id=dag_run_id,
            checkpoint_base=checkpoint_base,
            max_files_per_trigger=max_files_per_trigger,
        )
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Spark MTA bronze -> silver runner that writes the existing MTA tables to a compare database."
    )
    parser.add_argument("--dag-run-id", help="Optional Airflow DAG run id.")
    parser.add_argument("--bronze-type", choices=[*BRONZE_TYPES, "all"], default="full_feed")
    parser.add_argument("--start-date", help="Batch mode inclusive start date in YYYY-MM-DD.")
    parser.add_argument("--end-date", help="Batch mode inclusive end date in YYYY-MM-DD.")
    parser.add_argument("--checkpoint-base", help="Override Spark checkpoint base path.")
    parser.add_argument("--max-files-per-trigger", type=int, default=50)
    parser.add_argument("--target-database", help="Override MTA_SPARK_SNOWFLAKE_DATABASE for this run.")
    args = parser.parse_args()

    summary = run_mta_spark_silver(
        dag_run_id=args.dag_run_id,
        bronze_type=args.bronze_type,
        start_date=args.start_date,
        end_date=args.end_date,
        checkpoint_base=args.checkpoint_base,
        max_files_per_trigger=args.max_files_per_trigger,
        target_database=args.target_database,
    )
    print(f"[mta-silver] summary: {summary}")


if __name__ == "__main__":
    main()
