from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


REPO_ROOT = Path(__file__).resolve().parents[1]


default_args = {
    "owner": "mta-project",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="mta_pipeline_every_5_min",
    description="One-shot MTA bronze -> silver -> gold pipeline for LinuxLab Airflow",
    default_args=default_args,
    schedule="*/5 * * * *",
    start_date=datetime(2026, 4, 21),
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["mta", "bronze", "silver", "gold"],
) as dag:
    common_prefix = f"""
set -euo pipefail
cd "{REPO_ROOT}"
source .venv/bin/activate
export PYTHONUNBUFFERED=1
"""

    fetch_bronze_once = BashOperator(
        task_id="fetch_bronze_once",
        bash_command=common_prefix + '\npython -m mta_next.mta_producer --dag-run-id "{{ dag_run.run_id }}"',
    )

    load_bronze_to_silver = BashOperator(
        task_id="load_bronze_to_silver",
        bash_command=common_prefix + '\npython -m mta_next.mta_pipeline --dag-run-id "{{ dag_run.run_id }}" --bronze-type full_feed',
    )

    update_gold_metrics = BashOperator(
        task_id="update_gold_metrics",
        bash_command=common_prefix + '\npython -m mta_next.mta_arrival_inference --dag-run-id "{{ dag_run.run_id }}"',
    )

    fetch_bronze_once >> load_bronze_to_silver >> update_gold_metrics
