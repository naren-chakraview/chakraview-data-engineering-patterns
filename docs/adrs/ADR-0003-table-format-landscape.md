# ADR-0003: Table Format Landscape — Iceberg vs Delta Lake vs Hudi

**Status**: Accepted
**Date**: 2026-04-25
**Deciders**: Portfolio architect

---

## Context

The lakehouse pattern requires an open table format that adds ACID transactions, schema evolution, and time travel on top of object storage (S3, GCS, ADLS). Three formats compete: Apache Iceberg, Delta Lake, and Apache Hudi. Each variant in this repo that writes a lakehouse table must choose one.

The formats share a core capability: they track table state in a metadata layer (a manifest or transaction log) above Parquet/ORC data files, so that a commit is atomic, readers see a consistent snapshot, and historical versions remain queryable.

Their differences are most visible at the edges: multi-engine interoperability, schema evolution breadth, upsert performance, and governance model.

## Decision

**Default to Apache Iceberg.** Use Delta Lake only in Databricks-primary stacks. Do not use Hudi unless the workload is dominated by record-level upserts at very high update rates.

## Rationale

### Iceberg

Iceberg's primary advantage is portability. An Iceberg table written by Spark is readable by Flink, Trino, Presto, Dremio, StarRocks, DuckDB, and AWS Athena — with no conversion or copy. This interoperability is built into the spec, not an afterthought.

Iceberg supports full schema evolution: add, drop, rename, and reorder columns without rewriting data files. Column IDs (not names) are used to track fields, so renaming a column does not break readers that pinned to the old name.

Iceberg's snapshot isolation model gives readers a consistent point-in-time view even as writers commit new data. Time travel is available to any engine that can read Iceberg metadata.

Weakness: Iceberg's REST catalog and Glue/Hive metastore integrations require configuration. The first-time setup is more involved than Delta (which self-bootstraps on any Spark cluster with the Delta JAR).

### Delta Lake

Delta Lake's strength is deep Spark integration. Delta was built by Databricks and ships as a first-class feature of the Databricks Runtime. Operations like `MERGE INTO`, `OPTIMIZE`, and `VACUUM` are simpler in Delta than in Iceberg when Spark is the only engine.

Delta's transaction log (a sequence of JSON files in `_delta_log/`) is readable by Spark natively. Flink can read Delta tables via the Delta Flink connector, but write support is limited compared to Iceberg.

Weakness: cross-engine portability is limited. Trino and Presto can read Delta tables, but the implementation lags Iceberg's. A Delta table is a first-class object in Databricks; it is a second-class object in every other query engine.

### Hudi

Hudi's primary strength is record-level upsert performance via copy-on-write (CoW) and merge-on-read (MoR) storage types. For CDC workloads where the majority of operations are UPDATEs to specific records (not bulk appends), Hudi's MoR tables minimise write amplification.

Weakness: Hudi's schema evolution support is more limited than Iceberg or Delta. Multi-engine support (Trino, Presto) is improving but lags. For append-heavy or read-heavy workloads, Hudi's upsert optimisation provides no benefit over Iceberg.

## Consequences

**Positive:**

- Iceberg variants are readable by any engine without conversion, supporting the federated-query pattern directly.
- Delta variants show the Databricks-native path, which is the dominant managed stack choice.
- Engineers reading this repo see both formats demonstrated side by side with explicit rationale for the choice.

**Negative:**

- Hudi is excluded from boilerplate variants. Engineers with Hudi-dominated CDC workloads will not find a starter here.
- Maintaining two format variants per applicable pattern adds duplication in the boilerplate code (e.g., `IcebergSink` and `DeltaSink` implement the same logical operation).

## When this choice stops being correct

If Delta Lake closes the cross-engine gap (full Trino/Presto write support, engine-agnostic catalog), the Iceberg/Delta split becomes less important and Iceberg's portability advantage weakens. As of 2026, the gap is material.

## Alternatives considered

**Hudi as a third variant for CDC patterns:** Rejected — the marginal benefit over Debezium+Flink+Iceberg is small, and adding a third format to each CDC variant would double the maintenance surface for diminishing returns.

**Parquet only (no table format):** Rejected — Parquet without a table format has no ACID semantics, no time travel, and no schema evolution. It is not a realistic production choice for any lakehouse workload.
