-- List all Iceberg schemas
SHOW SCHEMAS FROM iceberg;

-- Query orders written by the batch-lakehouse boilerplate
SELECT
    order_id,
    customer_id,
    status,
    amount_usd,
    date_trunc('day', processed_at) AS order_date
FROM iceberg.orders.processed
WHERE processed_at >= CURRENT_DATE - INTERVAL '7' DAY
LIMIT 100;

-- Cross-catalog federation: Iceberg + built-in TPC-H
SELECT
    o.order_id,
    o.amount_usd,
    t.name AS nation_name
FROM iceberg.orders.processed o
JOIN tpch.sf1.nation t ON t.nationkey = 1
LIMIT 10;

-- Time travel: query as of a specific snapshot
SELECT COUNT(*) FROM iceberg.orders.processed
FOR VERSION AS OF 1234567890;
