# ELT / Warehouse

dbt transforms data already in a warehouse. No distributed compute — the warehouse handles scale.

## Variants

| Variant | Warehouse | Pull branch |
|---|---|---|
| `dbt-snowflake` | Snowflake | `pattern/elt-warehouse/dbt-snowflake` |
| `dbt-bigquery` | BigQuery | `pattern/elt-warehouse/dbt-bigquery` |
| `dbt-duckdb-airflow` | DuckDB (file-based, local) | `pattern/elt-warehouse/dbt-duckdb-airflow` |

## Prerequisites

- Python 3.11, pip
- `dbt-snowflake` / `dbt-bigquery` / `dbt-duckdb` installed (see each variant's README)
- Cloud credentials (Snowflake/BigQuery) OR Docker (dbt-duckdb-airflow)
