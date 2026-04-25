from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float32, Int64, String

order = Entity(
    name="order_id",
    description="Unique order identifier",
)

customer = Entity(
    name="customer_id",
    description="Unique customer identifier",
)

order_stats_source = FileSource(
    path="data/features/order_stats/",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

customer_stats_source = FileSource(
    path="data/features/customer_stats/",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

order_stats_fv = FeatureView(
    name="order_stats",
    entities=[order],
    ttl=timedelta(days=1),
    schema=[
        Field(name="amount_usd",  dtype=Float32),
        Field(name="item_count",  dtype=Int64),
    ],
    online=True,
    source=order_stats_source,
    tags={"team": "ml-platform"},
)

customer_stats_fv = FeatureView(
    name="customer_stats",
    entities=[customer],
    ttl=timedelta(days=7),
    schema=[
        Field(name="order_count_30d",     dtype=Int64),
        Field(name="total_spend_usd_30d", dtype=Float32),
        Field(name="avg_order_value_usd", dtype=Float32),
        Field(name="preferred_status",    dtype=String),
    ],
    online=True,
    source=customer_stats_source,
    tags={"team": "ml-platform"},
)
