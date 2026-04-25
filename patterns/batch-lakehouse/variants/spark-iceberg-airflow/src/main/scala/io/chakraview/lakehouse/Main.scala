package io.chakraview.lakehouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("batch-lakehouse-iceberg-airflow")
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.lakehouse.type", "rest")
      .config("spark.sql.catalog.lakehouse.uri",
        sys.env.getOrElse("ICEBERG_REST_URI", "http://localhost:8181"))
      .config("spark.sql.catalog.lakehouse.warehouse",
        sys.env.getOrElse("LAKEHOUSE_WAREHOUSE", "s3a://chakra-lakehouse/"))
      .config("spark.hadoop.fs.s3a.endpoint",
        sys.env.getOrElse("S3_ENDPOINT", "http://localhost:9000"))
      .config("spark.hadoop.fs.s3a.access.key",
        sys.env.getOrElse("AWS_ACCESS_KEY_ID", "minioadmin"))
      .config("spark.hadoop.fs.s3a.secret.key",
        sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", "minioadmin"))
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate()

    val source = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("s3a://chakra-lakehouse/raw/orders/")

    val transformed = source
      .withColumn("processed_at", current_timestamp())
      .withColumn("amount_usd", col("amount_cents").divide(100.0))
      .filter(col("order_id").isNotNull)

    transformed.writeTo("lakehouse.orders.processed")
      .option("write.format.default", "parquet")
      .option("write.parquet.compression-codec", "snappy")
      .createOrReplace()

    println(s"Wrote ${transformed.count()} rows to lakehouse.orders.processed")
    spark.stop()
  }
}
