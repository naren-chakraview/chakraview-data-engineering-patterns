"""Spark offline feature computation — writes Parquet for training data retrieval."""

from __future__ import annotations

import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("offline-feature-compute")
        .config("spark.hadoop.fs.s3a.endpoint",   os.getenv("S3_ENDPOINT", "http://localhost:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


if __name__ == "__main__":
    spark = build_spark()
    orders = spark.read.parquet(os.getenv("ORDERS_PATH", "s3a://chakra-lakehouse/silver/orders/"))
    now = datetime.now(tz=timezone.utc).isoformat()

    features = (
        orders
        .withColumn("event_timestamp", F.col("placed_at").cast("timestamp"))
        .withColumn("created", F.lit(now))
        .select("order_id", "customer_id", "event_timestamp", "created",
                (F.col("amount_cents") / 100.0).alias("amount_usd"))
    )
    features.write.mode("overwrite").parquet(
        os.getenv("OFFLINE_FEATURES_PATH", "s3a://chakra-lakehouse/features/orders/")
    )
    print(f"Offline features: {features.count()} rows")
    spark.stop()
