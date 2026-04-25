# Lambda Architecture — Flink (streaming) + Spark (batch) + Iceberg

## When to use

Stakeholders need both real-time approximate results AND nightly corrected results,
and they genuinely cannot agree on one.

## How to run locally

```bash
cp .env.example .env
docker compose up -d

# Start streaming path (Flink)
cd streaming && mvn package -DskipTests
flink run -m localhost:8082 target/lambda-streaming-flink-iceberg-0.1.0.jar

# Run batch path (Spark, scheduled via cron or Airflow)
cd ../batch && sbt assembly
spark-submit --class io.chakraview.lambda.BatchJob --master local[*] \
  target/scala-2.12/lambda-batch-spark-iceberg-assembly-0.1.0.jar
```

Query both paths in DuckDB:

```sql
-- Streaming results (fresh, approximate)
SELECT * FROM iceberg_scan('s3://chakra-lakehouse/...') WHERE path = 'streaming';

-- Batch results (accurate, up to 1h old)
SELECT * FROM iceberg_scan('s3://chakra-lakehouse/...') WHERE path = 'batch';
```

## Architecture

```
Kafka events.orders
    ↓
Flink streaming job (30s checkpoints) → Iceberg table (path="streaming")

S3 raw CSV
    ↓
Spark batch job (scheduled) → Iceberg table (path="batch")

Query time: SELECT ... WHERE path = 'streaming' | 'batch'
```
