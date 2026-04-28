# DMM Transportation Final Project

This repository contains the production data pipelines for comparing real-time public transit operations across MTA subway feeds and Transport for London (TfL) Tube feeds. The project follows a bronze, silver, and gold data architecture:

1. Bronze producers collect raw real-time transit data and write immutable partitioned objects to Cloudflare R2.
2. Silver processing cleans, flattens, and normalizes the raw data into Snowflake tables.
3. Gold processing creates analysis-ready metrics for arrival prediction behavior, route headways, route summaries, alert activity, and TfL arrival performance.

Spark is used as a processing component in the silver and gold stages. Snowflake is the production warehouse, and LinuxLab/Airflow is the deployment target for scheduled execution.

## Repository Layout

| Path | Purpose |
| --- | --- |
| `mta_prod/` | Production MTA pipeline: bronze producer, Spark silver runner, Spark gold analytics, schemas, Snowflake helpers, and Airflow DAG. |
| `tfl-prod/` | Production TfL pipeline: TfL API producer, Spark silver/gold jobs, Streamlit app, SQL, and Airflow DAGs. |
| `feeds.json` | MTA GTFS-Realtime subway feed definitions used by the MTA producer. |
| `.env.example` | Template for local/LinuxLab environment variables. |
| `requirements.txt` | Shared Python dependencies for producers, Spark jobs, Snowflake writes, and R2 access. |

## Environment Setup

Use a virtual environment on LinuxLab or locally:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Fill in `.env` with the required credentials and runtime settings:

```bash
MTA_SUBWAY_FEEDS_FILE=feeds.json
MTA_SINK_BACKEND=r2
MTA_BRONZE_PREFIX=bronze/mta

R2_BUCKET=...
R2_ACCOUNT_ID=...
R2_ACCESS_KEY=...
R2_SECRET_KEY=...

SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_WAREHOUSE=...
SNOWFLAKE_DATABASE=...
SNOWFLAKE_SCHEMA=...
SNOWFLAKE_PRIVATE_KEY_FILE=...
```

For TfL Spark jobs, also provide the aliases expected by `tfl-prod/dags/tfl_common.py`:

```bash
R2_ENDPOINT=https://<R2_ACCOUNT_ID>.r2.cloudflarestorage.com
SNOWFLAKE_URL=<SNOWFLAKE_ACCOUNT>.snowflakecomputing.com
SNOWFLAKE_PRIVATE_KEY_PATH=/home/your_user/.snowflake_keys/rsa_key.p8
CKPT_BASE=s3a://<R2_BUCKET>/checkpoints/tfl/silver
```

If the Snowflake private key is encrypted, set:

```bash
SNOWFLAKE_PRIVATE_KEY_FILE_PWD=...
```

## MTA Production Pipeline

The MTA production pipeline lives in `mta_prod/`. It reads official MTA GTFS-Realtime protobuf subway feeds, writes full-feed bronze snapshots to R2, flattens them into silver Snowflake tables, and computes gold metrics from repeated observations.

Create or verify the production Snowflake tables:

```bash
python -m mta_prod.prepare_database
```

Run the full one-shot production flow:

```bash
RUN_ID="manual_mta_$(date -u +%Y%m%dT%H%M%SZ)"

python -m mta_prod.mta_producer --dag-run-id "$RUN_ID"

python -m mta_prod.mta_silver_runner \
  --dag-run-id "$RUN_ID" \
  --bronze-type full_feed

python -m mta_prod.mta_gold --dag-run-id "$RUN_ID"
```

If the bronze producer is already running as its own task, run only silver and gold:

```bash
RUN_ID="manual_mta_silver_gold_$(date -u +%Y%m%dT%H%M%SZ)"

python -m mta_prod.mta_silver_runner \
  --dag-run-id "$RUN_ID" \
  --bronze-type full_feed

python -m mta_prod.mta_gold --dag-run-id "$RUN_ID"
```

To replay a specific date partition:

```bash
python -m mta_prod.mta_silver_runner \
  --bronze-type full_feed \
  --start-date 2026-04-28 \
  --end-date 2026-04-28

python -m mta_prod.mta_gold \
  --start-date 2026-04-28 \
  --end-date 2026-04-28
```

The MTA Airflow DAG is `mta_prod/dags/mta_dag.py`. It runs every five minutes and executes bronze, silver, and gold in order.

## MTA Snowflake Outputs

Silver tables:

- `MTA_FEED_SNAPSHOTS_SILVER`
- `MTA_TRIP_UPDATES_SILVER`
- `MTA_STOP_TIME_UPDATES_SILVER`
- `MTA_VEHICLE_POSITIONS_SILVER`
- `MTA_ALERTS_SILVER`
- `MTA_SILVER_LOAD_HISTORY`

Gold tables:

- `MTA_ARRIVAL_INFERENCE_GOLD`
- `MTA_ROUTE_HEADWAYS_GOLD`
- `MTA_ROUTE_SNAPSHOT_SUMMARY_GOLD`
- `MTA_ALERT_ACTIVITY_GOLD`
- `MTA_GOLD_LOAD_HISTORY`

MTA silver loads are idempotent by source `OBJECT_KEY`: reprocessing a bronze snapshot deletes and replaces rows produced from that snapshot. Gold loads use history and stable keys to update only the impacted analytical outputs.

## TfL Production Pipeline

The TfL pipeline lives in `tfl-prod/`. The producer polls the TfL Unified API for arrivals, line status, lift disruptions, and crowding data, then writes partitioned bronze JSON objects to R2.

Run the TfL producer:

```bash
python tfl-prod/tfl_producer.py
```

The producer is a long-running polling process. Stop it with `Ctrl+C` when running manually.

Run TfL silver processing from the DAG directory so local imports resolve:

```bash
cd tfl-prod/dags
python tfl_silver_runner.py
```

Run TfL gold arrival performance:

```bash
cd tfl-prod/dags
python tfl_gold.py --days-back 2
```

The TfL Airflow DAGs are in `tfl-prod/dags/tfl_dag.py`:

- `tfl_silver`: runs every 10 minutes
- `tfl_gold`: runs hourly

## TfL Snowflake Outputs

Silver tables:

- `TFL_ARRIVALS_SILVER`
- `TFL_STATUS_SILVER`
- `TFL_LIFT_DISRUPTIONS_SILVER`
- `TFL_CROWDING_SILVER`

Gold table:

- `TFL_ARRIVAL_PERFORMANCE_GOLD`

The TfL silver layer parses endpoint-specific JSON into relational tables. The gold layer compares repeated arrival predictions to measure prediction error, jitter, tracking duration, and peak-period behavior.

## Storage and Partitioning

Bronze files are stored in R2 with source, data type, and time partitions.

MTA example:

```text
bronze/mta/full_feed/year=YYYY/month=MM/day=DD/hour=HH/<timestamp>_full_feed_<feed>.json
```

TfL example:

```text
bronze/tfl/arrivals/year=YYYY/month=MM/day=DD/hour=HH/<timestamp>_arrivals_tube.json
```

This partitioning allows LinuxLab and Airflow runs to process specific time windows without scanning the full object store. It also supports controlled replay of a day or hour when debugging or backfilling.

## Weather Production Pipeline

The weather pipeline lives in `weather-prod/`. It provides environmental context for transit performance analysis by ingesting real-time data from the Open-Meteo API for NYC and London.

### Automation & Security
To ensure a continuous 24/7 ingestion cycle, the pipeline utilizes **RSA-2048 Key-Pair Authentication**. This enables the `weather_producer.py` to run autonomously on remote servers without requiring manual browser-based MFA.

### Resiliency & De-duplication
- **Dual-Destination Sink:** The producer stages data to a `LocalFileSystemSink` (backup) before pushing to the Snowflake `RAW` schema, preventing data loss during network interruptions.
- **Declarative De-duplication:** The Silver layer utilizes a window-function-based view (`WEATHER_HOURLY_FINAL`) to reconcile overlaps between historical backfills and live streaming data using `ROW_NUMBER()` over city and timestamp partitions.

### Run the Weather Producer
```bash
python weather-prod/weather_producer.py


