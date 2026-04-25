"""
Airflow reference DAG — demonstrates common operator patterns.

Shows: BashOperator, PythonOperator, SparkSubmitOperator, cross-task dependencies,
retry config, SLA alerts, and task groups.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup


def check_data_quality(**context) -> None:
    """Run basic data quality checks and fail the task if they don't pass."""
    ds = context["ds"]
    print(f"Running quality checks for partition: {ds}")
    # Replace with actual quality check logic
    row_count = 1000   # placeholder
    if row_count == 0:
        raise ValueError(f"No rows found for {ds} — aborting pipeline")
    print(f"Quality check passed: {row_count} rows")


def notify_success(**context) -> None:
    ds = context["ds"]
    print(f"Pipeline succeeded for {ds}. Notifying stakeholders.")
    # Replace with Slack/PagerDuty/email notification


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="data_pipeline",
    default_args=default_args,
    description="Reference ETL pipeline: extract -> transform -> validate -> load -> notify",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["reference", "orchestration"],
) as dag:

    with TaskGroup("extract") as extract_group:
        extract_orders = BashOperator(
            task_id="extract_orders",
            bash_command="echo 'Extracting orders for {{ ds }}'",
        )
        extract_customers = BashOperator(
            task_id="extract_customers",
            bash_command="echo 'Extracting customers for {{ ds }}'",
        )

    transform = SparkSubmitOperator(
        task_id="transform",
        application="/opt/spark-apps/transform-assembly-0.1.0.jar",
        name="transform-{{ ds }}",
        conf={"spark.driver.memory": "2g"},
    )

    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=check_data_quality,
    )

    load = BashOperator(
        task_id="load_to_lakehouse",
        bash_command="echo 'Loading to lakehouse for {{ ds }}'",
    )

    notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        trigger_rule="all_success",
    )

    extract_group >> transform >> quality_check >> load >> notify
