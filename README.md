# Chakra Data Engineering Patterns

Idiomatic boilerplate starters for 9 data engineering patterns × 22 stack variants.
Pick a pattern, pick a stack, pull the branch — done.

📖 **[Full docs site](https://naren-chakraview.github.io/chakraview-data-engineering-patterns/)**

---

## Patterns at a glance

| Pattern | Variants |
|---|---|
| **Batch Lakehouse** | Spark+Iceberg · Spark+Delta · Spark+Iceberg+Airflow |
| **Streaming Lakehouse** | Flink+Iceberg · Spark Streaming+Delta |
| **ELT / Warehouse** | dbt+Snowflake · dbt+BigQuery · dbt+DuckDB+Airflow |
| **CDC Pipeline** | Debezium+Kafka+Flink · Debezium+Kafka+Spark |
| **Lambda Architecture** | Flink+Spark+Iceberg · Spark Streaming+Spark Batch+Delta |
| **Federated Query** | Trino+Iceberg+S3 · Presto+Hive |
| **ML Feature Pipeline** | Feast+Redis+Spark · Flink+Spark offline · Feast+Spark+Airflow |
| **Graph Processing** | Spark GraphX · Neo4j+Spark |
| **Workflow Orchestration** | Airflow · Prefect · Dagster |

---

## How to use

**Browse everything** — clone `main` and explore `patterns/`:

```bash
git clone https://github.com/naren-chakraview/chakraview-data-engineering-patterns
```

**Pull a single variant** — clone only that branch (variant files land at repo root):

```bash
git clone --branch pattern/batch-lakehouse/spark-iceberg \
  https://github.com/naren-chakraview/chakraview-data-engineering-patterns \
  my-lakehouse
cd my-lakehouse
cp .env.example .env   # fill in your values
docker compose up -d
```

---

## Language convention

| Ecosystem | Language |
|---|---|
| Spark | Scala 2.12 |
| Flink | Java 17 |
| dbt / SQL-only | SQL |
| ML, Orchestration | Python 3.11 |
| Config-heavy (CDC, Federated Query) | YAML/JSON + SQL |
