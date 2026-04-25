# Federated Query — Presto + Hive Metastore

## When to use

Existing Hive metastore or Hadoop estate. Legacy tables registered in Hive need SQL access.

## How to run locally

```bash
cp .env.example .env
docker compose up -d
docker exec -it <presto-container> presto --catalog hive --schema default
presto:default> SHOW TABLES;
```

## Architecture

```
MinIO (S3-compatible)
    ↑ stores data files (ORC/Parquet)
Hive Metastore (PostgreSQL-backed)
    ↑ table metadata / schema registry
Presto coordinator
    ↑ SQL federation
```

Run example queries from `queries/orders_example.sql`.
