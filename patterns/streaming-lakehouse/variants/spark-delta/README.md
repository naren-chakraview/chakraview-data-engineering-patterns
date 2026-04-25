# Streaming Lakehouse — Spark Structured Streaming + Delta Lake

## When to use
Team knows Spark. Latency > 30 seconds acceptable. Databricks-compatible Delta format preferred.

## When NOT to use
Sub-minute latency or exactly-once event-time semantics required — use flink-iceberg instead.

## How to run locally

```bash
cp .env.example .env
docker compose up -d
sbt assembly
spark-submit \
  --class io.chakraview.streaming.Main \
  --master local[*] \
  target/scala-2.12/streaming-lakehouse-spark-delta-assembly-0.1.0.jar
```
