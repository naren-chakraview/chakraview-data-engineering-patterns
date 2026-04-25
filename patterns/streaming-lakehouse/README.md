# Streaming Lakehouse

Continuous pipeline reads Kafka events, processes them, writes to a lakehouse with sub-minute latency.

## Variants

| Variant | Engine | Table format | Latency floor | Pull branch |
|---|---|---|---|---|
| `flink-iceberg` | Flink 1.19 (Java) | Iceberg 1.5 | ~5s (checkpoint) | `pattern/streaming-lakehouse/flink-iceberg` |
| `spark-delta` | Spark 3.5 Structured Streaming (Scala) | Delta Lake 3.1 | ~30–60s (trigger) | `pattern/streaming-lakehouse/spark-delta` |

## Choosing between variants

- **flink-iceberg**: exactly-once per-event, native event-time watermarking, sub-minute freshness. Higher operational complexity (Flink cluster + Kafka).
- **spark-delta**: micro-batch, latency floor ~30s, simpler if team already knows Spark SQL.

## Prerequisites

- Docker + Docker Compose (starts Kafka + Flink or Kafka + MinIO)
- Java 17 + Maven 3.9 (flink-iceberg) OR Scala 2.12 + SBT 1.9 (spark-delta)
- A running Kafka topic `events.orders` (docker-compose creates it automatically)
