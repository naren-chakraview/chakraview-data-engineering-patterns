# Workflow Orchestration — Prefect

## When to use

Python-first team. Dynamic task generation. Modern UX. No desire for Airflow's scheduler ops.

## How to run locally

```bash
cp .env.example .env
docker compose up -d         # Prefect UI: http://localhost:4200
pip install -e .
export PREFECT_API_URL=http://localhost:4200/api

# Run flow directly:
python flows/data_pipeline_flow.py

# Or deploy with a schedule:
prefect deployment build flows/data_pipeline_flow.py:data_pipeline \
  --name daily --cron "0 6 * * *" --apply
prefect worker start --pool default-agent-pool
```
