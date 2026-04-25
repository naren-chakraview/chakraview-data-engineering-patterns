# Federated Query

Trino or Presto queries across multiple storage backends without moving data.
Right for polyglot data estates where ETL is too expensive or slow.

## Variants

| Variant | Engine | Primary catalog | Pull branch |
|---|---|---|---|
| `trino-iceberg-s3` | Trino 450 | Iceberg on MinIO/S3 | `pattern/federated-query/trino-iceberg-s3` |
| `presto-hive` | Presto 0.287 | Hive Metastore (PostgreSQL-backed) | `pattern/federated-query/presto-hive` |

## Choosing between variants

- **trino-iceberg-s3**: default. Trino is more actively developed; Iceberg is the portability choice.
- **presto-hive**: choose if you have an existing Hive Metastore or Hadoop estate to federate over.

## How federation works

Each catalog in `config/catalog/` maps a connector type to a data source. A single Trino/Presto
query can join across catalogs:

```sql
SELECT o.order_id, c.name
FROM iceberg.orders.processed o
JOIN postgresql.public.customers c ON o.customer_id = c.id
```
