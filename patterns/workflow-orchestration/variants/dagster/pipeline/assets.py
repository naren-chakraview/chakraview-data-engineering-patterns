"""
Dagster software-defined assets — reference pipeline.

Each asset represents a data artifact. Dagster tracks lineage between assets,
records materialisation metadata, and schedules re-materialisation on failure or schedule.
"""

from __future__ import annotations

from dagster import (
    AssetExecutionContext,
    MetadataValue,
    asset,
)


@asset(
    group_name="orders",
    description="Raw orders loaded from source system",
)
def raw_orders(context: AssetExecutionContext) -> list[dict]:
    data = [
        {"order_id": "ord-001", "amount_cents": 4999,  "status": "confirmed"},
        {"order_id": "ord-002", "amount_cents": 12999, "status": "shipped"},
        {"order_id": "ord-003", "amount_cents": 899,   "status": "pending"},
    ]
    context.add_output_metadata({
        "row_count": MetadataValue.int(len(data)),
        "preview":   MetadataValue.json(data[:2]),
    })
    return data


@asset(
    group_name="orders",
    description="Orders with USD amounts, quality checks applied",
)
def transformed_orders(
    context: AssetExecutionContext,
    raw_orders: list[dict],
) -> list[dict]:
    result = [
        {**row, "amount_usd": row["amount_cents"] / 100.0}
        for row in raw_orders
        if row.get("order_id") and row["amount_cents"] > 0
    ]
    context.add_output_metadata({
        "row_count":    MetadataValue.int(len(result)),
        "rows_dropped": MetadataValue.int(len(raw_orders) - len(result)),
    })
    return result


@asset(
    group_name="orders",
    description="Daily order summary aggregated for BI consumption",
)
def orders_summary(
    context: AssetExecutionContext,
    transformed_orders: list[dict],
) -> dict:
    total_revenue = sum(r["amount_usd"] for r in transformed_orders)
    summary = {
        "order_count":       len(transformed_orders),
        "total_revenue_usd": round(total_revenue, 2),
        "avg_order_value":   round(total_revenue / max(len(transformed_orders), 1), 2),
    }
    context.add_output_metadata({
        "summary":           MetadataValue.json(summary),
        "total_revenue_usd": MetadataValue.float(summary["total_revenue_usd"]),
    })
    return summary
