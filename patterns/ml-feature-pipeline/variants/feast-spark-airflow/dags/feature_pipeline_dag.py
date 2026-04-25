"""
Feature Pipeline DAG — Feast + Spark + Airflow

Steps:
1. Compute offline features (Spark job)
2. feast apply (register feature definitions)
3. feast materialize-incremental (offline -> Redis)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "ml-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=2),
}

with DAG(
    dag_id="ml_feature_pipeline",
    default_args=default_args,
    description="Compute and materialise ML features daily",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ml", "feast", "features"],
) as dag:

    compute_features = SparkSubmitOperator(
        task_id="compute_offline_features",
        application="/opt/spark-apps/compute_features.py",
        name="feature-compute-{{ ds }}",
    )

    feast_apply = BashOperator(
        task_id="feast_apply",
        bash_command="cd /opt/feast && feast apply",
    )

    feast_materialize = BashOperator(
        task_id="feast_materialize",
        bash_command=(
            "cd /opt/feast && "
            "feast materialize-incremental "
            "$(date -u +%Y-%m-%dT%H:%M:%S)"
        ),
    )

    compute_features >> feast_apply >> feast_materialize
