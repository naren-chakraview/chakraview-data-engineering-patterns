"""
dbt + DuckDB Airflow DAG

Runs dbt deps -> dbt run -> dbt test on a daily schedule.
Uses BashOperator — no Spark cluster required.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = Path("/opt/airflow/dbt")

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="elt_dbt_duckdb",
    default_args=default_args,
    description="Daily dbt run against DuckDB",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["elt", "dbt", "duckdb"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --target dev",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --target dev",
    )

    dbt_deps >> dbt_run >> dbt_test
