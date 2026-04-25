-- List Hive databases
SHOW DATABASES;

-- Query orders table in Hive
SELECT
    order_id,
    customer_id,
    status,
    amount_cents / 100.0 AS amount_usd
FROM hive.default.orders
WHERE dt = '2026-01-01'
LIMIT 100;

-- Cross-database join
SELECT h.order_id, h.status
FROM hive.default.orders h
WHERE h.amount_cents > 5000
ORDER BY h.amount_cents DESC
LIMIT 20;
