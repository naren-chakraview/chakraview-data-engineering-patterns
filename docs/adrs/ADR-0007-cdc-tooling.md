# ADR-0007: CDC Tooling — Debezium Primary, Airbyte as Managed Alternative

**Status**: Accepted
**Date**: 2026-04-25
**Deciders**: Portfolio architect

---

## Context

Change Data Capture (CDC) reads database write-ahead logs (WAL) to produce a stream of row-level change events. This is the correct extraction mechanism when polling is too slow (WAL capture is sub-second, polling is minutes) or when the source database cannot tolerate the read load of a polling extractor.

Multiple tools perform CDC extraction. A choice must be made for the boilerplate.

## Decision

Use **Debezium** as the primary CDC tool, deployed as a Kafka Connect connector writing events to Kafka (or Redpanda) topics. Document **Airbyte** in the README as the managed alternative for teams that do not want to operate Kafka Connect.

## Rationale

### Why Debezium

Debezium is the reference implementation of Kafka Connect-based CDC. It supports PostgreSQL, MySQL, MongoDB, Oracle, SQL Server, and DB2 via dedicated connectors. Each connector reads the database WAL directly (PostgreSQL: logical replication, MySQL: binlog, MongoDB: change streams), producing structured JSON events with before/after row state.

Debezium's key properties:

- **Sub-second latency**: WAL events are produced as they are committed to the source database, not on a polling interval.
- **Exactly-once delivery**: Debezium tracks connector offsets in Kafka. After a restart, it resumes from the last committed offset without producing duplicate events (when combined with Flink's exactly-once checkpoint).
- **Schema evolution**: Debezium's schema registry integration (Confluent Schema Registry or Apicurio) tracks schema changes and propagates them to consumers without manual intervention.
- **Zero polling load on source**: The WAL is written by the database's own transaction mechanism; Debezium reads it as a log consumer, not as a query.

### Why Kafka (not direct-to-Flink)

Debezium can write directly to a Flink source without Kafka, but this removes the replay buffer. The cdc-pipeline boilerplate uses Kafka (or Redpanda) as the durable log between Debezium and the processing engine, which enables:

1. **Reprocessing**: a Flink job bug can be fixed and a new job started from the beginning of the Kafka topic (within the retention window) without re-reading the source database.
2. **Fan-out**: multiple consumers (Flink, Spark, an audit consumer) can read the same CDC topic independently.
3. **Back-pressure isolation**: Flink job slowdowns do not propagate back-pressure to the Debezium connector.

### Airbyte as managed alternative

Airbyte abstracts the connector deployment and configuration behind a UI and managed infrastructure. For teams that do not want to operate Kafka Connect and Zookeeper, Airbyte provides CDC connectors for the same databases as Debezium, with a simpler configuration surface.

Trade-off: Airbyte adds Airbyte Cloud cost or Airbyte self-hosted operational complexity. It also hides the Kafka topic structure, making fan-out and replay harder. Airbyte is the right choice when simplicity outweighs flexibility.

## Consequences

**Positive:**

- Engineers learn the full CDC stack: Debezium connector → Kafka topic → Flink/Spark consumer. This is the most widely deployed production CDC architecture.
- The Kafka intermediate layer enables reprocessing and fan-out, which are critical for production reliability.

**Negative:**

- The local dev stack for CDC is the heaviest in this repo: Postgres (or MySQL) + Zookeeper + Kafka (or Redpanda) + Kafka Connect + Debezium connector + Flink/Spark. Engineers with resource-constrained laptops may struggle to run the full stack.

## When this choice stops being correct

If Flink's Debezium source connector (via `flink-connector-debezium`) reaches stability for direct-to-Flink CDC without Kafka (retaining exactly-once and replay), the Kafka intermediate hop could be removed. As of 2026, the Kafka-as-buffer architecture is the production-proven choice.

## Alternatives considered

**AWS DMS (Database Migration Service):** Managed CDC for AWS customers. Rejected as the primary boilerplate because it requires AWS credentials and cannot be run locally without a real AWS account.

**Maxwell's Daemon:** MySQL-specific CDC tool. Narrower scope than Debezium (MySQL only). Rejected.

**Striim:** Commercial CDC platform. Not open-source. Rejected.
