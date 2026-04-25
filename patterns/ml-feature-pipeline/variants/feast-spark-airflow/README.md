# ML Feature Pipeline — Feast + Spark + Airflow

## When to use

Feast-managed feature store with scheduled Airflow orchestration for retry and SLA alerting.

## How to run locally

```bash
cp .env.example .env
docker compose up -d
# Airflow UI: http://localhost:8080 — trigger dag `ml_feature_pipeline`
```

## Architecture

```
Airflow DAG (daily schedule):
    1. SparkSubmitOperator → compute_features.py → Parquet on S3
    2. BashOperator → feast apply (register feature definitions)
    3. BashOperator → feast materialize-incremental → Redis
```
