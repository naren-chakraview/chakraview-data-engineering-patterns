"""
Batch Lakehouse DAG — Spark + Iceberg + Airflow

Runs the Spark job daily, retries twice on failure, alerts on SLA miss.
Requires the Spark app JAR to be present at the path set in Airflow Variable spark_app_jar.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=2),
}

with DAG(
    dag_id="batch_lakehouse_iceberg",
    default_args=default_args,
    description="Daily Spark batch job writing orders to Iceberg",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["batch", "lakehouse", "iceberg"],
) as dag:

    ingest = SparkSubmitOperator(
        task_id="ingest_orders_to_iceberg",
        application=Variable.get(
            "spark_app_jar",
            default_var="/opt/spark-apps/batch-lakehouse-spark-iceberg-assembly-0.1.0.jar",
        ),
        name="batch-lakehouse-iceberg-{{ ds }}",
        conf={
            "spark.sql.extensions":
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.lakehouse":
                "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.lakehouse.type": "rest",
            "spark.sql.catalog.lakehouse.uri":
                Variable.get("iceberg_rest_uri", default_var="http://iceberg-rest:8181"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
        },
        env_vars={
            "ICEBERG_REST_URI":      Variable.get("iceberg_rest_uri",      default_var="http://iceberg-rest:8181"),
            "LAKEHOUSE_WAREHOUSE":   Variable.get("lakehouse_warehouse",   default_var="s3a://chakra-lakehouse/"),
            "S3_ENDPOINT":           Variable.get("s3_endpoint",           default_var="http://minio:9000"),
            "AWS_ACCESS_KEY_ID":     Variable.get("aws_access_key_id",     default_var="minioadmin"),
            "AWS_SECRET_ACCESS_KEY": Variable.get("aws_secret_access_key", default_var="minioadmin"),
        },
    )
