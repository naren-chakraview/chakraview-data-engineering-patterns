package io.chakraview.streaming

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration._

object Main {
  private val TablePath =
    sys.env.getOrElse("DELTA_TABLE_PATH", "s3a://chakra-lakehouse/delta/orders/events")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("streaming-lakehouse-spark-delta")
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

    import spark.implicits._

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",
        sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
      .option("subscribe", "events.orders")
      .option("startingOffsets", "earliest")
      .load()

    val orders = raw.select(from_json(
        col("value").cast("string"),
        EventSchema.schema
      ).alias("data")).select("data.*")
      .withColumn("ingested_at", current_timestamp())

    val query = orders.writeStream
      .trigger(Trigger.ProcessingTime(60.seconds))
      .option("checkpointLocation",
        sys.env.getOrElse("CHECKPOINT_PATH", "s3a://chakra-lakehouse/checkpoints/orders-streaming/"))
      .foreachBatch { (batchDf: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], _: Long) =>
        if (!batchDf.isEmpty) {
          if (!DeltaTable.isDeltaTable(spark, TablePath)) {
            batchDf.write.format("delta").save(TablePath)
          } else {
            DeltaTable.forPath(spark, TablePath)
              .alias("target")
              .merge(batchDf.alias("source"), "target.order_id = source.order_id")
              .whenMatched().updateAll()
              .whenNotMatched().insertAll()
              .execute()
          }
        }
      }
      .start()

    query.awaitTermination()
  }
}
