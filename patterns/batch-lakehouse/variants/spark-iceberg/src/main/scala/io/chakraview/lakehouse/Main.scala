package io.chakraview.lakehouse

import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessions.iceberg("batch-lakehouse-iceberg")
    import spark.implicits._

    val source = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("s3a://chakra-lakehouse/raw/orders/")

    val transformed = source
      .withColumn("processed_at", current_timestamp())
      .withColumn("amount_usd", col("amount_cents").divide(100.0))
      .filter(col("order_id").isNotNull)

    IcebergWriter.append(transformed, "lakehouse.orders.processed")

    println(s"Wrote ${transformed.count()} rows to lakehouse.orders.processed")
    spark.stop()
  }
}
