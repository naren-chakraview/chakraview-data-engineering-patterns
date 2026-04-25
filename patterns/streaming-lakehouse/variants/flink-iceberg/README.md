# Streaming Lakehouse — Flink + Iceberg

## When to use
Sub-minute latency, exactly-once semantics, native event-time windowing required.

## When NOT to use
Team has no Flink experience and latency > 30s is acceptable — use spark-delta instead.

## How to run locally

```bash
cp .env.example .env
docker compose up -d
mvn package -DskipTests
flink run -m localhost:8082 \
  target/streaming-lakehouse-flink-iceberg-0.1.0.jar
```

Flink UI: http://localhost:8082
MinIO console: http://localhost:9001
Iceberg REST: http://localhost:8181
