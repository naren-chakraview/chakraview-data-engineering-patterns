# Streaming Lakehouse

A continuous pipeline reads events from a Kafka topic, processes them with Flink or Spark Structured Streaming, and writes to an open table format with sub-minute commit latency. Combines the freshness of streaming with the query semantics of a lakehouse.

## When to use

- Latency requirement is 1–5 minutes (or sub-minute with Flink)
- Source data is already in Kafka (events, CDC output, IoT telemetry)
- Reprocessing must replay the Kafka log from a given offset — not re-extract from source
- Exactly-once semantics are required (Flink variant) or micro-batch is acceptable (Spark variant)

## When NOT to use

- Latency > 15 minutes — use Batch Lakehouse (simpler, cheaper)
- Source is a live database and you haven't set up Kafka — use CDC Pipeline first, then this pattern downstream
- Team has no Kafka or stream processing experience — operational complexity is high

## Variants

| Variant | Stack | Key idiom | Latency floor | Use when |
|---|---|---|---|---|
| [flink-iceberg](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/streaming-lakehouse/variants/flink-iceberg) | Flink 1.19 + Iceberg 1.5 + Kafka | `FlinkSink` with two-phase commit on checkpoint | ~5s (checkpoint interval) | Exactly-once, event-time windowing, sub-minute |
| [spark-delta](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/streaming-lakehouse/variants/spark-delta) | Spark 3.5 + Delta 3.1 + Kafka | `readStream` + `foreachBatch` with Delta MERGE | ~30–60s (trigger interval) | Team knows Spark, latency > 30s acceptable |

## Related patterns

- [Batch Lakehouse](batch-lakehouse.md) — when latency > 15 minutes is fine
- [CDC Pipeline](cdc-pipeline.md) — when source is a database WAL, not a Kafka topic
- [Lambda Architecture](lambda-architecture.md) — when both streaming AND batch correctness are required
