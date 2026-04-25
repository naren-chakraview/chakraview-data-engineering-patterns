package io.chakraview.lakehouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  private val TablePath =
    sys.env.getOrElse("DELTA_TABLE_PATH", "s3a://chakra-lakehouse/delta/orders/processed")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("batch-lakehouse-delta")
      .config("spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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

    DeltaWriter.merge(spark, transformed, TablePath, "order_id")

    println(s"Merged ${transformed.count()} rows into $TablePath")
    spark.stop()
  }
}
