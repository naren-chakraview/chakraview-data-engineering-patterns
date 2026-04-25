# Batch Lakehouse — Spark + Delta Lake

## When to use
Databricks-first stack. Want `DeltaTable.merge()` ergonomics and tightest Spark integration.

## When NOT to use
Queries from Trino, Presto, or non-Spark engines — use spark-iceberg instead.

## Trade-offs vs spark-iceberg
- **Delta**: simpler setup (no REST catalog); best `MERGE` ergonomics on Spark; Databricks-native
- **Iceberg**: reads from any engine; more portable; requires REST catalog config

## How to run locally

```bash
cp .env.example .env
docker compose up -d
sbt assembly
spark-submit \
  --class io.chakraview.lakehouse.Main \
  --master local[*] \
  target/scala-2.12/batch-lakehouse-spark-delta-assembly-0.1.0.jar
```
