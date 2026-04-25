# Federated Query — Trino + Iceberg + S3

## When to use

Multi-engine data estate. Iceberg tables written by Spark/Flink need ad-hoc SQL access.
Cross-system joins without data movement.

## When NOT to use

All data is in one system — query it directly. Sub-second latency — federated queries
cross network boundaries; they cannot match local query performance.

## How to run locally

```bash
cp .env.example .env
docker compose up -d
# Wait for Trino to be healthy, then query:
docker exec -it <trino-container> trino --catalog iceberg --schema orders
trino> SELECT * FROM processed LIMIT 10;

# Or connect with DBeaver/TablePlus to localhost:8080 (no auth for local dev)
```

Run example queries from `queries/orders_example.sql`.
