from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="nyc_mta_subway_to_snowflake",
    start_date=datetime(2026, 4, 1),
    schedule="* * * * *",   # every minute
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "mta-nyc-team",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["mta", "nyc", "gtfs-rt", "snowflake"],
) as dag:

    @task
    def ingest():
        from dags.mta_subway_ingest import run_ingestion
        return run_ingestion()

    ingest()