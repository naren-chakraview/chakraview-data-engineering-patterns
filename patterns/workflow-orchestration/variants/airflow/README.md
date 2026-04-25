# Workflow Orchestration — Airflow

## When to use

Large org, many data source integrations (600+ providers), existing Airflow investment.

## How to run locally

```bash
cp .env.example .env
docker compose up -d
# UI: http://localhost:8080  (admin / admin on first run)
# Enable and trigger dag: data_pipeline
```

## Key files

- `dags/data_pipeline_dag.py` — reference DAG with BashOperator, PythonOperator, SparkSubmitOperator, TaskGroup
