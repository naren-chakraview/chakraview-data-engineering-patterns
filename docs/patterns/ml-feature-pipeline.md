# ML Feature Pipeline

Computes and serves ML features with a clear split: an offline store (historical features on object storage) for model training, and an online store (Redis or key-value) for real-time model serving.

## When to use

- ML models require features at serving time with millisecond latency (online store)
- Training data must be consistent with serving features (point-in-time correctness)
- Feature logic must be shared between training and serving (no training/serving skew)
- Team owns ML model training and serving, not just data pipelines

## When NOT to use

- Models use only static features (no time-varying inputs) — batch inference from a lakehouse table is sufficient
- No real-time serving — offline batch inference does not need an online store
- ML platform is managed (Databricks Feature Store, Vertex AI Feature Store) — use the platform's native feature API

## Variants

| Variant | Stack | Use when |
|---|---|---|
| [feast-redis-spark](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/ml-feature-pipeline/variants/feast-redis-spark) | Feast 0.40 + Redis + Spark 3.5 | Want a feature store framework managing both stores |
| [flink-spark-offline](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/ml-feature-pipeline/variants/flink-spark-offline) | Flink 1.19 (online) + Spark 3.5 (offline) | Want to own the online/offline mechanics without a framework |
| [feast-spark-airflow](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/ml-feature-pipeline/variants/feast-spark-airflow) | Feast + Spark + Airflow 2.9 | Feast + scheduled materialisation with orchestration |

## Related patterns

- [Streaming Lakehouse](streaming-lakehouse.md) — the offline store feeds from a streaming lakehouse in production
- [Workflow Orchestration](workflow-orchestration.md) — for scheduling feature materialisation jobs
