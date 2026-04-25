# Workflow Orchestration

Airflow, Prefect, or Dagster schedule and coordinate data pipeline tasks.
The glue between every other pattern.

## Variants

| Variant | Abstraction | Differentiator | Pull branch |
|---|---|---|---|
| `airflow` | DAG of operators | 600+ providers, industry standard | `pattern/workflow-orchestration/airflow` |
| `prefect` | Python-native flows | Modern ergonomics, dynamic tasks | `pattern/workflow-orchestration/prefect` |
| `dagster` | Software-defined assets | Asset lineage, strong observability | `pattern/workflow-orchestration/dagster` |

## Choosing between variants

- **airflow**: large org, many integrations, existing Airflow investment.
- **prefect**: Python-first team, need dynamic task generation, want modern UX without Airflow ops overhead.
- **dagster**: asset-centric thinking, need per-materialisation metadata and lineage graph out of the box.
