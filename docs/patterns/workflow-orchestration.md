# Workflow Orchestration

Airflow, Prefect, or Dagster schedule and coordinate the tasks that make up a data pipeline. Orchestration is the glue between every other pattern — a batch lakehouse job is just a Spark submit until Airflow gives it a schedule, retry logic, and an SLA alert.

## When to use

- Any batch or scheduled pipeline needs retry on failure, dependency management, or backfill
- Multiple pipeline steps must be coordinated (extract → transform → load → notify)
- Operational visibility (which runs succeeded, which failed, how long they took) is required
- SLA alerting on pipeline completion time is needed

## When NOT to use

- Your pipeline is a continuous streaming job (Flink/Spark Streaming) — streaming jobs are long-running processes, not scheduled tasks; use a process supervisor (Kubernetes Deployment, systemd) instead
- You have a single script with no dependencies — a cron job is sufficient

## Variants

| Variant | Stack | Key differentiator | Use when |
|---|---|---|---|
| [airflow](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/workflow-orchestration/variants/airflow) | Airflow 2.9 (LocalExecutor) | 600+ providers, mature ecosystem | Large org, many integrations, existing Airflow investment |
| [prefect](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/workflow-orchestration/variants/prefect) | Prefect 2.x | Python-native flows, dynamic tasks | Python-first team, modern UX, dynamic task generation |
| [dagster](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/workflow-orchestration/variants/dagster) | Dagster 1.x | Software-defined assets, lineage graph | Asset-centric thinking, strong observability requirements |

## Related patterns

- [Batch Lakehouse](batch-lakehouse.md) — the spark-iceberg-airflow variant integrates Airflow directly
- [ELT / Warehouse](elt-warehouse.md) — the dbt-duckdb-airflow variant integrates Airflow directly
- [ML Feature Pipeline](ml-feature-pipeline.md) — the feast-spark-airflow variant integrates Airflow directly
