# ADR-0006: Lakehouse Storage Layer — Open Table Formats Over Managed Warehouses

**Status**: Accepted
**Date**: 2026-04-25
**Deciders**: Portfolio architect

---

## Context

For patterns that require durable, queryable storage at scale (batch lakehouse, streaming lakehouse, lambda architecture), two broad approaches exist:

1. **Open table formats on object storage** — Iceberg or Delta Lake on S3/GCS/ADLS. The processing engine writes directly to object storage; a catalog (Hive Metastore, Glue, Iceberg REST) tracks table state.

2. **Managed cloud warehouses** — Snowflake, BigQuery, Redshift, Databricks SQL. The warehouse vendor manages both storage and compute; the engineer writes SQL or uses a warehouse-native API.

This repo must choose which approach to demonstrate as the primary storage layer for lakehouse patterns.

## Decision

Use **open table formats on object storage** (Iceberg and Delta Lake) as the primary storage layer for all lakehouse patterns. Managed warehouses are covered only in the ELT/warehouse pattern, where they are the appropriate system of record.

For local development, use **MinIO** as an S3-compatible object store, so that engineers can run the full stack on a laptop without cloud credentials.

## Rationale

### Why open formats

**Portability:** An Iceberg table on S3 is queryable by Spark, Flink, Trino, Presto, Dremio, DuckDB, and AWS Athena. No conversion is required to switch the query engine. This portability is the defining property of the lakehouse pattern — it is what distinguishes a lakehouse from a data warehouse with a data lake attached.

**Cost at scale:** Object storage (S3 Standard) costs $0.023/GB-month. Snowflake's managed storage costs $23/TB-month — 1000× more. For organisations with PB-scale data, this difference is material. Open formats on object storage allow the compute layer to be scaled independently of the storage layer.

**Engine choice:** With open formats, the organisation is not locked into a single query engine. A Spark batch pipeline and a Trino ad-hoc query can both read the same Iceberg table without copying data.

### Why not managed warehouses as the primary lakehouse layer

Managed warehouses are the correct choice when the team's operational bandwidth is limited and data volume is small-to-medium. But they cannot demonstrate the portability, cost, and engine-independence properties that define the lakehouse pattern. A boilerplate that says "write to Snowflake" is an ELT pattern, not a lakehouse pattern.

### MinIO for local dev

MinIO provides an S3-compatible API on localhost. This means the boilerplate uses the same S3A client configuration locally as in production — only the endpoint URL changes. Engineers can run the full stack locally without cloud credentials, which reduces the barrier to trying the boilerplate.

## Consequences

**Positive:**

- Engineers learn the full open-source lakehouse stack: Iceberg/Delta metadata layer, Parquet data files, S3-compatible object storage, and a catalog.
- The boilerplate is cloud-agnostic — switch the S3 endpoint and credentials to move from MinIO (local) to S3 (AWS), GCS (GCP), or ADLS (Azure).

**Negative:**

- Operating Iceberg/Delta on object storage requires more configuration than managed warehouses. Engineers must understand the catalog layer (REST catalog, Hive Metastore, or Glue) — this is non-trivial for teams new to the lakehouse pattern.
- MinIO in docker-compose consumes local disk and memory. Engineers with resource-constrained laptops may find the local stack slow.

## When this choice stops being correct

If managed warehouses (Snowflake, BigQuery) adopt native open table format support that allows external engines to read and write the same tables without copying data, the distinction between "managed warehouse" and "open lakehouse" narrows. Snowflake Iceberg Tables (public preview as of 2026) is a step in this direction — if it reaches general availability with full Flink and Trino write support, this ADR should be revisited.

## Alternatives considered

**Databricks Lakehouse:** Managed Databricks provides Delta Lake with a managed catalog (Unity Catalog). Rejected as the primary pattern because Databricks lock-in reduces portability. The `spark-delta` variants in this repo demonstrate Delta Lake without requiring Databricks.

**Apache Hudi on S3:** Considered as a third format variant. Rejected — see ADR-0003.
