# Batch Lakehouse

Scheduled Spark jobs read from source, transform, and write to an open table format on object storage.

## Variants

| Variant | Table format | Orchestration | Pull branch |
|---|---|---|---|
| `spark-iceberg` | Iceberg 1.5 | None (run manually or via cron) | `pattern/batch-lakehouse/spark-iceberg` |
| `spark-delta` | Delta Lake 3.1 | None | `pattern/batch-lakehouse/spark-delta` |
| `spark-iceberg-airflow` | Iceberg 1.5 | Airflow 2.9 DAG | `pattern/batch-lakehouse/spark-iceberg-airflow` |
| `spark-iceberg-semantic` | Iceberg 1.5 + RDF (Jena) | None | `pattern/batch-lakehouse/spark-iceberg-semantic` |

## Choosing between variants

- **spark-iceberg**: default choice. Iceberg is read by Trino, Presto, Flink, DuckDB, and Athena without extra configuration.
- **spark-delta**: choose when the team lives in Databricks or needs `DeltaTable.merge()` API ergonomics.
- **spark-iceberg-airflow**: choose when you need scheduled retry, SLA alerting, and backfill out of the box.
- **spark-iceberg-semantic**: choose when you need entity deduplication across polyglot sources using RDF and SPARQL. See [Semantic Medallion pattern](../../docs/patterns/semantic-medallion.md).

## Prerequisites

- Scala 2.12, SBT 1.9+
- Java 11+
- Spark 3.5 installed locally (`spark-submit` on PATH), or use `spark-shell` for interactive exploration
- Docker + Docker Compose (for local MinIO / Iceberg REST / Airflow)
