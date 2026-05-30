# CDC Pipeline

Debezium reads the database WAL and streams every committed change to Kafka. A downstream engine
(Flink or Spark) processes the CDC events and materialises them into a lakehouse.

## Variants

| Variant | Processing engine | Latency | Pull branch |
|---|---|---|---|
| `debezium-kafka-flink` | Flink 1.19 (Java) | ~5s | `pattern/cdc-pipeline/debezium-kafka-flink` |
| `debezium-kafka-spark` | Spark 3.5 Structured Streaming (Scala) | ~30–60s | `pattern/cdc-pipeline/debezium-kafka-spark` |
| `debezium-kafka-semantic` | Flink 1.19 + RDF (Jena) | ~5s | `pattern/cdc-pipeline/debezium-kafka-semantic` |

## Choosing between variants

- **debezium-kafka-flink**: default choice. True streaming with event-time semantics. Choose when sub-5s latency is required.
- **debezium-kafka-spark**: choose when your team uses Spark elsewhere and prefers Structured Streaming.
- **debezium-kafka-semantic**: choose when you need real-time entity resolution and RDF emission from CDC events. See [Semantic Medallion pattern](../../docs/patterns/semantic-medallion.md).

## Prerequisites

- Docker + Docker Compose (starts PostgreSQL, Kafka, Debezium Connect, and Flink or Spark)
- Java 17 + Maven 3.9 (debezium-kafka-flink) OR Scala 2.12 + SBT 1.9 (debezium-kafka-spark)
- PostgreSQL configured with `wal_level=logical` (docker-compose handles this automatically)
