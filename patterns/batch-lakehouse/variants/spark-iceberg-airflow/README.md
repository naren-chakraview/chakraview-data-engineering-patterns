# Batch Lakehouse — Spark + Iceberg + Airflow

## When to use
Scheduled retry, SLA alerting, and backfill are required. Team already has or wants Airflow.

## When NOT to use
No scheduling needed — use spark-iceberg instead (simpler, no Airflow overhead).

## How to run locally

```bash
cp .env.example .env
sbt assembly
docker compose up -d
```

Airflow UI: http://localhost:8080 (admin / admin on first run)
Trigger DAG: `batch_lakehouse_iceberg`
MinIO console: http://localhost:9001
