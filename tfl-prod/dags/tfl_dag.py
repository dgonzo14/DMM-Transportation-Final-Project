"""
tfl_dag.py — Airflow DAGs for the TfL Spark pipeline.

silver_dag : every 10 minutes — tfl_silver_runner.py (availableNow streaming)
gold_dag   : every hour       — tfl_gold.py (batch, last 2 days of arrivals)

Store all credentials as Airflow Variables (Admin → Variables):
    R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY, R2_BUCKET, CKPT_BASE
    SNOWFLAKE_URL, SNOWFLAKE_USER, SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_PRIVATE_KEY_PATH
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

TOOLS = "/home/compute/a.garvens/airflow25/tools"

ENV = {
    "R2_ENDPOINT":                "{{ var.value.R2_ENDPOINT }}",
    "R2_ACCESS_KEY":              "{{ var.value.R2_ACCESS_KEY }}",
    "R2_SECRET_KEY":              "{{ var.value.R2_SECRET_KEY }}",
    "R2_BUCKET":                  "{{ var.value.R2_BUCKET }}",
    "CKPT_BASE":                  "{{ var.value.CKPT_BASE }}",
    "SNOWFLAKE_URL":              "{{ var.value.SNOWFLAKE_URL }}",
    "SNOWFLAKE_USER":             "{{ var.value.SNOWFLAKE_USER }}",
    "SNOWFLAKE_DATABASE":         "{{ var.value.SNOWFLAKE_DATABASE }}",
    "SNOWFLAKE_SCHEMA":           "{{ var.value.SNOWFLAKE_SCHEMA }}",
    "SNOWFLAKE_WAREHOUSE":        "{{ var.value.SNOWFLAKE_WAREHOUSE }}",
    "SNOWFLAKE_PRIVATE_KEY_PATH": "{{ var.value.SNOWFLAKE_PRIVATE_KEY_PATH }}",
}

DEFAULT_ARGS = {
    "owner":            "a.garvens",
    "retries":          1,
    "email_on_failure": False,
}

with DAG(
    dag_id="tfl_silver",
    description="TfL bronze -> silver, every 10 minutes",
    schedule_interval="*/10 * * * *",
    start_date=datetime(2026, 4, 27),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tfl", "silver"],
) as silver_dag:

    BashOperator(
        task_id="silver_run",
        bash_command=f"python {TOOLS}/tfl_silver_runner.py",
        env=ENV,
    )


with DAG(
    dag_id="tfl_gold",
    description="TfL bronze -> gold arrival performance, hourly",
    schedule_interval="0 * * * *",
    start_date=datetime(2026, 4, 27),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["tfl", "gold"],
) as gold_dag:

    BashOperator(
        task_id="gold_arrival_performance",
        bash_command=f"python {TOOLS}/tfl_gold.py --days-back 2",
        env=ENV,
    )