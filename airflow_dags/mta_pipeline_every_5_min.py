from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from mta_next.mta_arrival_inference import run_mta_gold_inference_once
from mta_next.mta_pipeline import run_mta_silver_pipeline_once
from mta_next.mta_producer import run_mta_bronze_fetch_once


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
    fetch_bronze_once = PythonOperator(
        task_id="fetch_bronze_once",
        python_callable=run_mta_bronze_fetch_once,
        op_kwargs={"dag_run_id": "{{ dag_run.run_id }}"},
    )

    load_bronze_to_silver = PythonOperator(
        task_id="load_bronze_to_silver",
        python_callable=run_mta_silver_pipeline_once,
        op_kwargs={
            "dag_run_id": "{{ dag_run.run_id }}",
            "bronze_type": "full_feed",
        },
    )

    update_gold_metrics = PythonOperator(
        task_id="update_gold_metrics",
        python_callable=run_mta_gold_inference_once,
        op_kwargs={"dag_run_id": "{{ dag_run.run_id }}"},
    )

    fetch_bronze_once >> load_bronze_to_silver >> update_gold_metrics
