package io.chakraview.lambda

import org.apache.spark.sql.functions._

// Batch path: high-accuracy results written to same Iceberg table with path="batch"
object BatchJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessions.iceberg("lambda-batch-spark-iceberg")
    import spark.implicits._

    val source = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("s3a://chakra-lakehouse/raw/orders/")

    val transformed = source
      .withColumn("processed_at", current_timestamp())
      .withColumn("amount_usd",   col("amount_cents").divide(100.0))
      .withColumn("path",         lit("batch"))
      .filter(col("order_id").isNotNull)

    // Overwrite the batch partition; streaming partition is untouched
    spark.sql("DELETE FROM lakehouse.orders.events WHERE path = 'batch'")
    transformed.writeTo("lakehouse.orders.events")
      .option("write.format.default", "parquet")
      .append()

    println(s"Batch path updated: ${transformed.count()} rows")
    spark.stop()
  }
}
