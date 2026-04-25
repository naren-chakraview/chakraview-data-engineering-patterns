# ADR-0004: Stream Processing Engine — Flink vs Spark Structured Streaming

**Status**: Accepted
**Date**: 2026-04-25
**Deciders**: Portfolio architect

---

## Context

Two streaming engines dominate the open-source data engineering stack: Apache Flink and Apache Spark Structured Streaming. Both produce lakehouse output (Iceberg/Delta), both support exactly-once semantics, and both integrate with Kafka/Redpanda as the event source.

The streaming-lakehouse, CDC pipeline, and lambda architecture patterns each require a stream processing engine. A decision is needed: use one engine throughout, or show both where the trade-off is meaningful.

## Decision

Show **both engines** in patterns where the choice materially affects the operational model:

- **Flink** — for sub-minute latency, true event-time processing, and exactly-once CDC capture.
- **Spark Structured Streaming** — for teams with existing Spark investment, latency > 30s acceptable, unified batch+streaming codebase.

Use **Java** for all Flink variants (see ADR-0002). Use **Scala** for all Spark variants.

## Rationale

### Flink strengths

Flink processes each event as it arrives. Its execution model is a continuous dataflow graph — not a series of micro-batch queries. This gives Flink three advantages over Spark Structured Streaming:

1. **Latency**: Flink can achieve sub-second end-to-end latency. Spark's micro-batch interval is configurable (as low as 100ms) but adds inherent batch overhead.
2. **Event-time correctness**: Flink's watermark API is native and per-operator. Late data handling, windowed joins, and out-of-order event processing are first-class concepts. Spark's event-time support is a later addition and has sharper edge cases.
3. **State management**: Flink manages operator state in RocksDB, with configurable incremental checkpointing and exactly-once semantics via distributed snapshots (Chandy-Lamport algorithm). State is decoupled from the message bus.

### Spark Structured Streaming strengths

For teams with existing Spark investment, Structured Streaming offers the same DataFrame API as batch Spark. A single codebase can run as batch (scheduled) or streaming (continuous) by changing the trigger mode. This is the primary argument for Spark in streaming contexts: **operational simplicity for Spark-first teams**.

Structured Streaming's micro-batch model is also easier to reason about: each trigger reads a batch of records, applies the same transformations as a batch job, and commits an output. There is no stateful operator graph to tune or checkpoint.

### When Spark wins

- The team owns Spark infrastructure and does not want to operate a Flink cluster.
- Latency requirement is > 30 seconds (micro-batch intervals of 30s are operationally similar to short-interval batch).
- The pipeline logic is predominantly stateless (filter, project, aggregate without joins).

### When Flink wins

- Sub-minute latency is required.
- The pipeline has stateful joins across streams (e.g., enriching events with a broadcast lookup table that changes).
- Exactly-once is required for CDC source capture (Flink's Debezium source connector provides exactly-once natively).

## Consequences

**Positive:**

- Engineers see both engines side by side in the streaming-lakehouse and lambda-architecture patterns, with explicit rationale for the choice in each variant's README.
- The Flink variants demonstrate the higher operational bar required for sub-minute latency, setting accurate expectations.

**Negative:**

- Two engines require two language stacks (Java for Flink, Scala for Spark) in streaming patterns. An engineer who wants to compare a Flink and a Spark streaming variant must context-switch between Java and Scala.

## When this choice stops being correct

If Apache Flink adopts a SQL-first API that fully replaces the Java/Python DataStream API for production use cases, the language gap narrows and the comparison becomes easier. Flink SQL is improving rapidly; as of 2026, the DataStream API remains necessary for complex stateful pipelines.

## Alternatives considered

**Flink only:** Simpler repo, single ecosystem. Rejected because many production teams use Spark Streaming and the trade-off between the two engines is a genuine, non-obvious decision that deserves to be demonstrated side by side.

**Kafka Streams / ksqlDB:** Lower operational complexity for event-at-a-time processing without a separate cluster. Rejected because Kafka Streams cannot write to lakehouse formats directly (requires a Sink connector or Kafka Connect + custom sink), making it unsuitable as the primary engine for the lakehouse patterns in this repo.
