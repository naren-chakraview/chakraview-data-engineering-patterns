# Batch Lakehouse

Scheduled Spark jobs read from source systems, apply transformations, and write to an open table format (Iceberg or Delta Lake) on object storage. The highest-throughput, lowest-complexity pattern for analytical data movement.

## When to use

- Latency requirement is > 15 minutes (hourly, daily, or on-demand runs are acceptable)
- Source data arrives in files (S3 drops, SFTP, database exports) or via scheduled database queries
- Full reprocessing from source is feasible (no need to replay a Kafka log)
- Team is Spark/Scala fluent; no Kafka or Flink expertise required
- Cost is a primary constraint — Spark runs only during the job, no always-on cluster

## When NOT to use

- You need < 5-minute data freshness — use Streaming Lakehouse
- Your source is a live database WAL — use CDC Pipeline
- Your team writes SQL but not Scala — use ELT / Warehouse

## Variants

| Variant | Stack | Key idiom | Use when |
|---|---|---|---|
| [spark-iceberg](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/batch-lakehouse/variants/spark-iceberg) | Spark 3.5 + Iceberg 1.5 + S3 | `DataFrameWriter` with Iceberg catalog | Multi-engine shop; need Trino/Presto reads |
| [spark-delta](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/batch-lakehouse/variants/spark-delta) | Spark 3.5 + Delta Lake 3.1 + S3 | `DeltaTable.forPath().merge()` | Databricks-first; want tightest Spark integration |
| [spark-iceberg-airflow](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/batch-lakehouse/variants/spark-iceberg-airflow) | Spark + Iceberg + Airflow 2.9 | `SparkSubmitOperator` in DAG | Need scheduled retry, alerting, and backfill |

## Related patterns

- [Streaming Lakehouse](streaming-lakehouse.md) — when latency < 5 minutes is required
- [CDC Pipeline](cdc-pipeline.md) — when source is a live database WAL
- [Workflow Orchestration](workflow-orchestration.md) — when you need a standalone orchestrator reference
