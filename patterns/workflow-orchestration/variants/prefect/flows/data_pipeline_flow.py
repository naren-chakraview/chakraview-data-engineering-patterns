"""
Prefect reference flow — demonstrates flows, tasks, caching, retries, and subflows.
"""

from __future__ import annotations

from datetime import timedelta

from prefect import flow, task
from prefect.tasks import task_input_hash


@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
    retries=2,
    retry_delay_seconds=30,
)
def extract_orders(date: str) -> list[dict]:
    """Extract orders for a given date. Cached for 1h so retries are cheap."""
    print(f"Extracting orders for {date}")
    # Replace with actual extraction logic (JDBC, API, S3, etc.)
    return [
        {"order_id": "ord-001", "amount_cents": 4999,  "date": date},
        {"order_id": "ord-002", "amount_cents": 12999, "date": date},
    ]


@task(retries=1)
def transform_orders(raw: list[dict]) -> list[dict]:
    """Apply business rules and compute derived fields."""
    return [
        {
            **row,
            "amount_usd":    row["amount_cents"] / 100.0,
            "is_high_value": row["amount_cents"] > 10_000,
        }
        for row in raw
        if row.get("order_id")
    ]


@task
def load_to_lakehouse(data: list[dict], path: str) -> int:
    """Write transformed data to the lakehouse (Iceberg/Delta on S3)."""
    print(f"Loading {len(data)} rows to {path}")
    # Replace with actual write logic (PySpark, pyarrow, etc.)
    return len(data)


@task
def notify(count: int, date: str) -> None:
    print(f"Pipeline complete for {date}: {count} rows loaded")


@flow(name="data-pipeline", log_prints=True)
def data_pipeline(date: str = "2026-01-01") -> None:
    """Main pipeline flow. Run manually or schedule via Prefect deployment."""
    raw         = extract_orders(date)
    transformed = transform_orders(raw)
    count       = load_to_lakehouse(
        transformed,
        path=f"s3://chakra-lakehouse/delta/orders/{date}/",
    )
    notify(count, date)


if __name__ == "__main__":
    data_pipeline()
