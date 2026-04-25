# ML Feature Pipeline — Flink Online + Spark Offline

## When to use

Sub-second online feature freshness (Flink writes directly to Redis on each event).
Want to own the mechanics without a feature store framework.

## How to run locally

```bash
cp .env.example .env
docker compose up -d

# Online path (Flink — continuous):
cd online && mvn package -DskipTests
flink run -m localhost:8082 target/ml-feature-pipeline-flink-online-0.1.0.jar

# Offline path (Spark — scheduled):
cd offline && pip install pyspark
spark-submit compute_offline_features.py
```

## Architecture

```
Kafka events.orders
    ↓
Flink job (30s checkpoints, AT_LEAST_ONCE)
    ↓ writes HSET to Redis key: feature:order_stats:<order_id>
Redis — ms latency online serving

S3 raw orders (Parquet)
    ↓
Spark batch (scheduled)
    ↓ writes Parquet to S3 features/
Training code reads offline Parquet directly
```
