# DMM-Transportation-Final-Project

Python Version:
- maintained by pyenv
- Python version is 3.10.18 for reproducibility

## Running the MTA Bronze -> Silver -> Gold pipeline on LinuxLab

The refactored MTA pipeline lives in `mta_next/`. The original `mta-prod/` folder is still present for reference and debug utilities, but the new production architecture is:

1. `python -m mta_next.mta_producer`
   Fetch each NYCT GTFS-Realtime feed once and write immutable bronze snapshots to R2.
2. `python -m mta_next.mta_pipeline --bronze-type full_feed`
   Read new bronze objects incrementally and load typed silver tables into Snowflake.
3. `python -m mta_next.mta_arrival_inference`
   Recompute only the impacted gold analytics from the updated silver rows.

For LinuxLab, use a repo-local virtualenv and a Linux path for the Snowflake key:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
chmod 600 /home/your_user/.snowflake_keys/rsa_key.p8
cp .env.example .env
```

Set these required values in `.env`:

- `MTA_SUBWAY_FEEDS_FILE`
- `MTA_SINK_BACKEND=r2`
- `R2_BUCKET`, `R2_ACCOUNT_ID`, `R2_ACCESS_KEY`, `R2_SECRET_KEY`
- `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_PRIVATE_KEY_FILE`

Then run the three one-shot stages manually:

```bash
python -m mta_next.mta_producer
python -m mta_next.mta_pipeline --bronze-type full_feed
python -m mta_next.mta_arrival_inference
```

Historical replay is handled by:

```bash
python -m mta_next.mta_backfill --start-date 2026-04-01 --end-date 2026-04-20 --layers all
```

The Airflow DAG for the 5-minute LinuxLab schedule is in [airflow_dags/mta_pipeline_every_5_min.py](/c:/Users/dgonz/datamanipscale/DMM-Transportation-Final-Project/airflow_dags/mta_pipeline_every_5_min.py:1).

## Architecture Note

The new MTA architecture mirrors the TfL layering by splitting the system into separate producer, pipeline, inference, and backfill entrypoints. The producer is one-shot and Airflow-safe, the pipeline is incremental and idempotent, the inference layer only recomputes gold outputs for impacted keys, and the backfill entrypoint replays historical bronze partitions without turning the scheduled job into a full-history reload.

It differs from the TfL implementation because MTA uses GTFS-Realtime protobuf full-dataset feeds rather than TfL REST JSON endpoints. The MTA silver loader therefore treats bronze objects as full feed snapshots that may contain trip updates, vehicle positions, and alerts in the same payload. It flattens those snapshots into typed silver tables while preserving MTA-specific identifiers such as `route_id`, `trip_id`, `train_id`, `direction`, `stop_id`, `feed_timestamp`, and ingestion lineage fields like `object_key` and `ingestion_id`.

Idempotency is enforced at each stage with Snowflake history tables:

- `MTA_BRONZE_LOAD_HISTORY`
- `MTA_SILVER_LOAD_HISTORY`
- `MTA_GOLD_LOAD_HISTORY`

Silver loads delete and replace rows by `OBJECT_KEY`, while gold loads delete and replace only the impacted surrogate keys such as trip-stop inference keys, headway window keys, route snapshot keys, and alert activity keys. Arrival inference is adapted to MTA by aggregating repeated `stop_time_update` observations for the same stable trip-stop key over time, then computing drift, variance, observation span, and a disappearance-based completion proxy from those repeated snapshot observations.
