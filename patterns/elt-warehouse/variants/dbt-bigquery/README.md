# ELT / Warehouse — dbt + BigQuery

## When to use
GCP-first org. BigQuery is your warehouse.

## How to run

```bash
pip install dbt-bigquery
gcloud auth application-default login   # or set GOOGLE_APPLICATION_CREDENTIALS
cp .env.example .env && source .env
dbt deps && dbt debug && dbt run && dbt test
```
