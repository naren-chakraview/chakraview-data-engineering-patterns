"""
Spark job: compute features and write to offline store (Parquet on S3/local).

Run this on a schedule (daily or hourly) to refresh the offline feature store.
Feast materialise then pushes fresh values from offline -> online (Redis).
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("feast-feature-compute")
        .config("spark.hadoop.fs.s3a.endpoint",    os.getenv("S3_ENDPOINT", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key",  os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key",  os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def compute_order_stats(spark: SparkSession) -> None:
    orders = spark.read.parquet(os.getenv("ORDERS_PATH", "s3a://chakra-lakehouse/silver/orders/"))

    order_stats = (
        orders
        .withColumn("event_timestamp", F.col("placed_at").cast("timestamp"))
        .withColumn("created", F.lit(datetime.now(tz=timezone.utc).isoformat()))
        .select(
            "order_id",
            "event_timestamp",
            "created",
            (F.col("amount_cents") / 100.0).alias("amount_usd"),
            F.size("items").alias("item_count"),
        )
    )

    order_stats.write.mode("overwrite").parquet(
        os.getenv("ORDER_STATS_PATH", "data/features/order_stats/")
    )
    print(f"Computed order_stats: {order_stats.count()} rows")


def compute_customer_stats(spark: SparkSession) -> None:
    orders = spark.read.parquet(os.getenv("ORDERS_PATH", "s3a://chakra-lakehouse/silver/orders/"))

    window_30d = orders.filter(
        F.col("placed_at") >= F.date_sub(F.current_date(), 30)
    )

    customer_stats = (
        window_30d
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("order_count_30d"),
            (F.sum("amount_cents") / 100.0).alias("total_spend_usd_30d"),
            (F.avg("amount_cents") / 100.0).alias("avg_order_value_usd"),
            F.first("status").alias("preferred_status"),
        )
        .withColumn("event_timestamp", F.lit(datetime.now(tz=timezone.utc).isoformat()).cast("timestamp"))
        .withColumn("created", F.lit(datetime.now(tz=timezone.utc).isoformat()))
    )

    customer_stats.write.mode("overwrite").parquet(
        os.getenv("CUSTOMER_STATS_PATH", "data/features/customer_stats/")
    )
    print(f"Computed customer_stats: {customer_stats.count()} rows")


if __name__ == "__main__":
    spark = build_spark()
    compute_order_stats(spark)
    compute_customer_stats(spark)
    spark.stop()
