# Workflow Orchestration — Dagster

## When to use

Asset-centric thinking. Need per-materialisation metadata and lineage graph out of the box.

## How to run locally

```bash
cp .env.example .env
pip install -e .
docker compose up -d        # UI: http://localhost:3000

# Or run locally without Docker:
dagster dev -m pipeline.definitions
```

## Key files

- `pipeline/assets.py` — software-defined assets with metadata and lineage
- `pipeline/definitions.py` — job, schedule, and Definitions wiring
