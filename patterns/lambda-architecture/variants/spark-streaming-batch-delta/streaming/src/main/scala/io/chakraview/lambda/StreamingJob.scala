package io.chakraview.lambda

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration._

object StreamingJob {
  private val TablePath =
    sys.env.getOrElse("DELTA_TABLE_PATH", "s3a://chakra-lakehouse/delta/orders/lambda")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("lambda-streaming-spark-delta")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
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

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",
        sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
      .option("subscribe", "events.orders")
      .option("startingOffsets", "latest")
      .load()

    val orders = raw
      .select(from_json(col("value").cast("string"), EventSchema.schema).alias("d"))
      .select("d.*")
      .withColumn("path", lit("streaming"))

    orders.writeStream
      .trigger(Trigger.ProcessingTime(60.seconds))
      .option("checkpointLocation",
        sys.env.getOrElse("CHECKPOINT_PATH", "s3a://chakra-lakehouse/checkpoints/lambda-streaming/"))
      .foreachBatch { (batch: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], _: Long) =>
        if (!batch.isEmpty) {
          if (!DeltaTable.isDeltaTable(spark, TablePath))
            batch.write.format("delta").partitionBy("path").save(TablePath)
          else
            batch.write.format("delta").mode("append").save(TablePath)
        }
      }
      .start()
      .awaitTermination()
  }
}
