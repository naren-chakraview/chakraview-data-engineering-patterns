# ELT / Warehouse — dbt + DuckDB + Airflow

## When to use
Cost-sensitive, local/dev, or small-to-medium data. DuckDB runs in-process — no warehouse account needed.

## How to run

```bash
pip install dbt-duckdb
cp .env.example .env && source .env
dbt deps && dbt run && dbt test

# With Airflow scheduling:
docker compose up -d
# Airflow UI: http://localhost:8080 — trigger dag `elt_dbt_duckdb`
```
