# Batch Lakehouse — Spark + Iceberg

## When to use
Multi-engine environment: Trino, Presto, Athena, or DuckDB must read the same tables Spark writes.
Portability matters more than deepest Spark integration.

## When NOT to use
Your team is Databricks-only and portability is not a concern — use spark-delta instead.

## Trade-offs vs spark-delta
- **Iceberg**: any engine reads/writes; broader schema evolution; REST catalog required
- **Delta**: simpler local setup (no REST catalog); `DeltaTable.merge()` is more ergonomic; best on Databricks

## How to run locally

```bash
cp .env.example .env
docker compose up -d          # starts MinIO + Iceberg REST catalog
sbt assembly                  # builds fat JAR
spark-submit \
  --class io.chakraview.lakehouse.Main \
  --master local[*] \
  target/scala-2.12/batch-lakehouse-spark-iceberg-assembly-0.1.0.jar
```

MinIO console: http://localhost:9001 (minioadmin / minioadmin)
Iceberg REST: http://localhost:8181
