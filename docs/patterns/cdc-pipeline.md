# CDC Pipeline

Debezium reads the database write-ahead log (WAL) and streams every committed insert, update, and delete as an event to a Kafka topic. A downstream processor (Flink or Spark) consumes the CDC events and materialises them into the lakehouse.

## When to use

- Source is a live relational database (PostgreSQL, MySQL, SQL Server, Oracle)
- Sub-second capture latency is required (WAL-based, not polling-based)
- You need a full change history, not just current state (every insert/update/delete)
- Zero impact on source database query performance is required (WAL reading does not add query load)

## When NOT to use

- Source database does not support WAL replication (use Airbyte with query-based CDC instead)
- Latency > 5 minutes acceptable and source supports direct Spark JDBC reads — use Batch Lakehouse (simpler)
- You only need current state, not change history — a nightly snapshot may be sufficient

## Variants

| Variant | Stack | Key idiom | Use when |
|---|---|---|---|
| [debezium-kafka-flink](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/cdc-pipeline/variants/debezium-kafka-flink) | Debezium + Kafka + Flink 1.19 | `KafkaSource` consuming CDC envelope → `KeyedProcessFunction` for upsert | Exactly-once, complex stream processing downstream |
| [debezium-kafka-spark](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/cdc-pipeline/variants/debezium-kafka-spark) | Debezium + Kafka + Spark 3.5 | `readStream` from Kafka → `foreachBatch` with Delta MERGE | Team knows Spark, latency > 30s acceptable |

## Related patterns

- [Streaming Lakehouse](streaming-lakehouse.md) — this pattern feeds into a streaming lakehouse
- [Lambda Architecture](lambda-architecture.md) — CDC as the streaming input to the Lambda pattern
