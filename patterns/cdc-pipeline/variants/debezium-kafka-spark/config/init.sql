-- Create orders table for CDC demo
CREATE TABLE IF NOT EXISTS orders (
    order_id     VARCHAR(36)    PRIMARY KEY,
    customer_id  VARCHAR(36)    NOT NULL,
    status       VARCHAR(20)    NOT NULL DEFAULT 'pending',
    amount_cents INTEGER        NOT NULL,
    placed_at    TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- Seed some initial rows
INSERT INTO orders (order_id, customer_id, status, amount_cents)
VALUES
    ('ord-001', 'cust-a', 'pending',   4999),
    ('ord-002', 'cust-b', 'confirmed', 12999),
    ('ord-003', 'cust-a', 'shipped',   899)
ON CONFLICT DO NOTHING;
