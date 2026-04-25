# Lambda Architecture — Spark Streaming + Spark Batch + Delta Lake

## When to use

Single Spark team that needs both streaming and batch, with Delta as the unifying format.

## How to run locally

```bash
cp .env.example .env
docker compose up -d

# Streaming path (continuous):
cd streaming && sbt assembly
spark-submit --class io.chakraview.lambda.StreamingJob --master local[*] \
  target/scala-2.12/lambda-streaming-spark-delta-assembly-0.1.0.jar &

# Batch path (scheduled):
cd ../batch && sbt assembly
spark-submit --class io.chakraview.lambda.BatchJob --master local[*] \
  target/scala-2.12/lambda-batch-spark-delta-assembly-0.1.0.jar
```

## Architecture

```
Kafka events.orders
    ↓
Spark Streaming (60s trigger) → Delta table (path="streaming", append mode)

S3 raw CSV
    ↓
Spark Batch (scheduled) → Delta table (path="batch", replaceWhere overwrite)

Query: SELECT * FROM delta.`s3a://chakra-lakehouse/delta/orders/lambda` WHERE path = 'batch'
```
