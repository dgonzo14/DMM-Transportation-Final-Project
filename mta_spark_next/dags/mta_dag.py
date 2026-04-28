from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


REPO_ROOT = Path(__file__).resolve().parents[2]


default_args = {
    "owner": "mta-project",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="mta_spark_pipeline_every_5_min",
    description="MTA bronze -> Spark silver -> Spark gold pipeline writing to compare Snowflake database",
    default_args=default_args,
    schedule="*/5 * * * *",
    start_date=datetime(2026, 4, 21),
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["mta", "spark", "silver", "gold"],
) as dag:
    common_prefix = f"""
set -euo pipefail
cd "{REPO_ROOT}"
source .venv/bin/activate
export PYTHONUNBUFFERED=1
"""

    fetch_bronze_once = BashOperator(
        task_id="fetch_bronze_once",
        bash_command=common_prefix
        + '\npython -m mta_spark_next.prepare_database\npython -m mta_spark_next.mta_producer --dag-run-id "{{ dag_run.run_id }}"',
    )

    load_bronze_to_spark_silver = BashOperator(
        task_id="load_bronze_to_spark_silver",
        bash_command=common_prefix
        + '\npython -m mta_spark_next.mta_silver_runner --dag-run-id "{{ dag_run.run_id }}" --bronze-type full_feed',
    )

    update_gold_metrics = BashOperator(
        task_id="update_gold_metrics",
        bash_command=common_prefix
        + '\npython -m mta_spark_next.mta_gold --dag-run-id "{{ dag_run.run_id }}"',
    )

    fetch_bronze_once >> load_bronze_to_spark_silver >> update_gold_metrics
