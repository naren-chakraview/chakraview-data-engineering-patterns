# CDC Pipeline — Debezium + Kafka + Flink

## When to use

Sub-second capture from a live PostgreSQL database. Exactly-once processing downstream.

## How to run locally

```bash
cp .env.example .env
docker compose up -d
# Wait for all services healthy, then register the Debezium connector:
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @config/debezium-connector.json

mvn package -DskipTests
flink run -m localhost:8082 target/cdc-pipeline-flink-0.1.0.jar

# Insert a row to trigger a CDC event:
docker exec -it <postgres-container> psql -U chakra -c \
  "INSERT INTO orders VALUES ('ord-999','cust-z','pending',100,NOW(),NOW());"
```

## Architecture

```
PostgreSQL (WAL / pgoutput)
    ↓
Debezium Connect (ExtractNewRecordState → flat JSON)
    ↓
Kafka topic: chakra.public.orders
    ↓
Flink (30s checkpoints, EXACTLY_ONCE)
    ├─ upserts → Iceberg sink (see streaming-lakehouse/flink-iceberg)
    └─ deletes → Iceberg delete
```

## Key files

| File | Purpose |
|---|---|
| `config/debezium-connector.json` | Registers the PostgreSQL source connector |
| `config/init.sql` | Creates the `orders` table with WAL replication |
| `src/main/java/io/chakraview/cdc/Main.java` | Flink job entry point |
| `src/main/java/io/chakraview/cdc/CdcEventParser.java` | Parses Debezium flat JSON |
