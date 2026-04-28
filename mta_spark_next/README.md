# MTA Spark Next

This folder is a Spark-based implementation of the MTA pipeline. It intentionally
keeps the existing `mta_next` package unchanged, while copying the pipeline
dependencies here so LinuxLab can run from one folder.

The Spark silver runner writes the same table names and schemas as the current
MTA silver pipeline:

- `MTA_FEED_SNAPSHOTS_SILVER`
- `MTA_TRIP_UPDATES_SILVER`
- `MTA_STOP_TIME_UPDATES_SILVER`
- `MTA_VEHICLE_POSITIONS_SILVER`
- `MTA_ALERTS_SILVER`
- `MTA_SILVER_LOAD_HISTORY`

The Spark gold runner writes the same gold tables and history table:

- `MTA_ARRIVAL_INFERENCE_GOLD`
- `MTA_ROUTE_HEADWAYS_GOLD`
- `MTA_ROUTE_SNAPSHOT_SUMMARY_GOLD`
- `MTA_ALERT_ACTIVITY_GOLD`
- `MTA_GOLD_LOAD_HISTORY`

By default it targets a compare database named:

```bash
${SNOWFLAKE_DATABASE}_SPARK_COMPARE
```

Override that with:

```bash
export MTA_SPARK_SNOWFLAKE_DATABASE=YOUR_COMPARE_DATABASE
```

The copied `mta_spark_next` producer, Spark silver runner, and Spark gold
inference all use that compare database target by default.

The core Spark pipeline files follow the same naming pattern as the TfL DAG
implementation:

- `mta_common.py`
- `mta_schemas.py`
- `mta_silver.py`
- `mta_silver_runner.py`
- `mta_gold.py`
- `dags/mta_dag.py`

## LinuxLab Task Swap

Use the copied bronze producer from this package:

```bash
python -m mta_spark_next.mta_producer --dag-run-id "{{ dag_run.run_id }}"
```

Then replace the old silver task with:

```bash
python -m mta_spark_next.mta_silver_runner \
  --dag-run-id "{{ dag_run.run_id }}" \
  --bronze-type full_feed
```

For the gold task, use the Spark gold inference module:

```bash
python -m mta_spark_next.mta_gold --dag-run-id "{{ dag_run.run_id }}"
```

If you want every Snowflake write in the test DAG to land in the compare
database, create the target schemas first:

```bash
python -m mta_spark_next.prepare_database
```

## Backfill / Comparison Runs

Process a date range without using streaming checkpoints:

```bash
python -m mta_spark_next.mta_silver_runner \
  --bronze-type full_feed \
  --start-date 2026-04-21 \
  --end-date 2026-04-21
```

Then run Spark gold over the same date range:

```bash
python -m mta_spark_next.mta_gold \
  --start-date 2026-04-21 \
  --end-date 2026-04-21
```

The `all` bronze type is treated as `full_feed`, matching the current
authoritative MTA path and avoiding duplicate rows from the split bronze files.
