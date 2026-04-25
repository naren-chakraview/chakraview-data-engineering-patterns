# ADR-0010: ML Feature Pipeline — Online vs Offline Store Split

**Status**: Accepted
**Date**: 2026-04-25
**Deciders**: Portfolio architect

---

## Context

ML feature pipelines must serve features in two contexts with different latency and throughput requirements:

1. **Offline serving**: features used during model training. Latency of minutes is acceptable. Throughput is high — a training job may request features for millions of historical examples.

2. **Online serving**: features used during model inference. Latency must be milliseconds. Throughput depends on prediction QPS. Features must be pre-computed and stored in a low-latency store (Redis, DynamoDB, Cassandra).

A feature store is a system that manages both stores and provides a unified API for defining, computing, and retrieving features.

## Decision

Use the **online/offline split** as the primary architectural pattern for all ML feature pipeline variants. Use **Feast** as the feature store framework. Demonstrate:

1. `feast-redis-spark` — Feast + Redis (online) + Spark (offline compute). The standard production split.
2. `flink-spark-offline` — Flink for near-real-time online feature computation + Spark for batch offline compute. For sub-minute online feature freshness without a full Feast deployment.
3. `feast-spark-airflow` — Feast + Spark + Airflow for scheduled offline materialisation.

## Rationale

### Why online/offline split

The split reflects a real architectural constraint: no single store technology is optimal for both training (high throughput, minutes latency, columnar access) and serving (low latency, key-value access). Training reads features in bulk scans; serving reads features by entity key.

Collapsing the split (training and serving from the same store) either:
- Forces training to use a key-value store (Redis) for bulk scans — expensive and slow.
- Forces serving to use a columnar store (Parquet/Iceberg) for key-value lookups — too slow for inference latency.

The split is the correct model for production ML platforms. The boilerplate demonstrates it explicitly.

### Why Feast

Feast is the open-source feature store with the broadest adoption. It provides:

- A unified **feature definition API** (Python) — features are defined once and materialised to both online and offline stores.
- **Point-in-time correct joins** for training data generation — a critical correctness requirement that is easy to get wrong without a feature store (training-serving skew).
- Support for multiple online backends (Redis, DynamoDB, SQLite for local dev) and offline backends (Parquet, BigQuery, Snowflake).
- **Materialisation** — a Feast command or Spark job computes feature values for a time range and loads them into the online store for low-latency serving.

### Why include `flink-spark-offline`

The Flink variant demonstrates the pattern for teams that need sub-minute online feature freshness without a full Feast deployment. Feast's materialisation is batch (scheduled); Flink can update the online store continuously as new events arrive. This is the correct architecture for features that must be < 1 minute stale at serving time (e.g., recent transaction count for fraud detection).

### Feast limitations acknowledged in READMEs

- Feast does not manage model training or serving — it is a feature platform, not an MLOps platform.
- Feast's Python SDK is the primary interface. Scala/Java clients exist but are less mature.
- Feast's online store freshness depends on materialisation frequency for the Feast+Spark variants. Only the Flink variant achieves continuous updates.

## Consequences

**Positive:**

- Engineers see the full feature pipeline: feature definition → offline compute → online materialisation → serving.
- Point-in-time correct training joins are demonstrated, which is the most common source of training-serving skew.
- The Flink variant demonstrates the sub-minute freshness path for latency-sensitive online features.

**Negative:**

- Feast requires Redis for local online serving (docker-compose includes Redis). Redis adds memory pressure to the local dev stack.
- The Feast + Spark + Airflow variant is the most operationally complex in the repo (Feast registry + Redis + Spark + Airflow).

## When this choice stops being correct

If Feast is superseded by a more actively maintained open-source feature store (Tecton open-source, Hopsworks Community, or a new entrant), the Feast variants should be updated. As of 2026, Feast remains the leading open-source feature store by GitHub activity and production deployment.

## Alternatives considered

**Tecton:** Commercial feature store with the most mature production track record. Rejected because it requires a Tecton account and cannot be run locally without a commercial agreement.

**Hopsworks:** Open-source feature store with online/offline split. More feature-complete than Feast but heavier to deploy (requires Kubernetes). Rejected for the local dev boilerplate due to deployment complexity.

**No feature store (raw Spark + Redis):** Demonstrates the pattern without Feast abstraction. Rejected because the boilerplate should show the abstraction engineers use in production, not the raw implementation beneath it. The `flink-spark-offline` variant approximates this by omitting Feast and showing direct Flink-to-Redis writes.
