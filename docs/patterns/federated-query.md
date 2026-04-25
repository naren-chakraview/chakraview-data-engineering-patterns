# Federated Query

Trino or Presto queries across multiple storage systems (Iceberg on S3, Hive, PostgreSQL, Kafka) without moving data. The right pattern when your data estate is polyglot and data movement is too expensive or too slow.

## When to use

- Data lives in multiple systems (lakehouse + operational database + data warehouse) and must be queried together
- Data movement (ETL) is not feasible due to volume, latency, or cost
- Query latency of seconds to minutes is acceptable (federated queries are slower than local queries)
- Team is SQL-fluent and does not need a processing engine for transformation

## When NOT to use

- All data is in one system — direct query is always faster than federation
- Sub-second query latency required — federated queries cross network boundaries; local queries do not
- Heavy aggregations on huge tables — federated query engines push down filters but not all aggregations; Spark batch will outperform

## Variants

| Variant | Stack | Use when |
|---|---|---|
| [trino-iceberg-s3](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/federated-query/variants/trino-iceberg-s3) | Trino 450 + Iceberg 1.5 + MinIO | Iceberg as primary format; Presto-compatible SQL |
| [presto-hive](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/federated-query/variants/presto-hive) | Presto 0.287 + Hive Metastore | Existing Hive metastore; legacy Hadoop estate |

## Related patterns

- [Batch Lakehouse](batch-lakehouse.md) — to build the Iceberg tables that Trino queries
- [ELT / Warehouse](elt-warehouse.md) — when all data is already in one warehouse
