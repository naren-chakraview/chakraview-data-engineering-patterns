# ELT / Warehouse

dbt transforms data that is already in a cloud warehouse (Snowflake, BigQuery) or a local analytical database (DuckDB). No distributed compute required — the warehouse handles scale. The simplest pattern when your data is already where it needs to be.

## When to use

- Data is already loaded into a cloud warehouse or DuckDB
- Transformation logic is SQL (no Python UDFs, no distributed joins across systems)
- Latency requirement is minutes to hours (dbt runs on a schedule)
- Team writes SQL fluently; no Spark or Flink experience required
- You want built-in lineage, testing, and documentation (dbt provides all three)

## When NOT to use

- Data is not yet in the warehouse — load it first (use CDC Pipeline, Batch Lakehouse, or a managed ELT tool)
- Latency < 5 minutes — dbt is not a streaming tool
- Transformation logic requires Python, ML inference, or cross-system joins — use Spark or Federated Query

## Variants

| Variant | Stack | Use when |
|---|---|---|
| [dbt-snowflake](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/elt-warehouse/variants/dbt-snowflake) | dbt 1.8 + Snowflake | Snowflake is your warehouse |
| [dbt-bigquery](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/elt-warehouse/variants/dbt-bigquery) | dbt 1.8 + BigQuery | GCP-first org |
| [dbt-duckdb-airflow](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/elt-warehouse/variants/dbt-duckdb-airflow) | dbt 1.8 + DuckDB + Airflow 2.9 | Cost-sensitive, small-medium data, local or file-based |

## Related patterns

- [Batch Lakehouse](batch-lakehouse.md) — when data is not in a warehouse and must be processed at scale
- [CDC Pipeline](cdc-pipeline.md) — to load data into the warehouse from a live database
- [Workflow Orchestration](workflow-orchestration.md) — for standalone dbt orchestration references
