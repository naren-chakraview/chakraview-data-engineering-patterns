# ML Feature Pipeline

Computes and serves ML features with a clear online (Redis, ms latency) / offline
(Parquet/Iceberg on S3, minute latency) split.

## Variants

| Variant | Framework | Online store | Offline store | Pull branch |
|---|---|---|---|---|
| `feast-redis-spark` | Feast 0.40 | Redis | Parquet on S3 | `pattern/ml-feature-pipeline/feast-redis-spark` |
| `flink-spark-offline` | None (hand-rolled) | Redis (via Flink) | Parquet on S3 (via Spark) | `pattern/ml-feature-pipeline/flink-spark-offline` |
| `feast-spark-airflow` | Feast 0.40 + Airflow | Redis | Parquet on S3 | `pattern/ml-feature-pipeline/feast-spark-airflow` |

## Choosing between variants

- **feast-redis-spark**: default. Feast manages point-in-time correct historical retrieval for training.
- **flink-spark-offline**: choose when you need sub-second online freshness (Flink writes directly to Redis) and want to own the mechanics without a framework.
- **feast-spark-airflow**: choose when materialisation needs Airflow scheduling, retry, and SLA alerting.
