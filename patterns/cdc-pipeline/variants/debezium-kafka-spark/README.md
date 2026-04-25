# CDC Pipeline — Debezium + Kafka + Spark

## When to use

CDC capture with Spark processing. Latency > 30s acceptable. Team already knows Spark.

## How to run locally

```bash
cp .env.example .env
docker compose up -d
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @config/debezium-connector.json
sbt assembly
spark-submit --class io.chakraview.cdc.Main --master local[*] \
  target/scala-2.12/cdc-pipeline-spark-assembly-0.1.0.jar
```

## Architecture

```
PostgreSQL (WAL / pgoutput)
    ↓
Debezium Connect (ExtractNewRecordState → flat JSON)
    ↓
Kafka topic: chakra.public.orders
    ↓
Spark Structured Streaming (60s trigger)
    └─ foreachBatch → Delta MERGE (upserts) + soft-delete (status="deleted")
    └─ sink: MinIO (s3a://)
```

## Key files

| File | Purpose |
|---|---|
| `config/debezium-connector.json` | Registers the PostgreSQL source connector |
| `config/init.sql` | Creates the `orders` table with WAL replication |
| `src/main/scala/io/chakraview/cdc/Main.scala` | Spark Structured Streaming job |
| `src/main/scala/io/chakraview/cdc/CdcSchema.scala` | Debezium flat JSON schema |
