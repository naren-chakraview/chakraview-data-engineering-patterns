package io.chakraview.cdc

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration._

object Main {
  private val TablePath =
    sys.env.getOrElse("DELTA_TABLE_PATH", "s3a://chakra-lakehouse/delta/orders/cdc")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("cdc-pipeline-spark")
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

    // Debezium publishes flat JSON after ExtractNewRecordState unwrap transform
    val cdcSchema = CdcSchema.schema

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",
        sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
      .option("subscribe", "chakra.public.orders")
      .option("startingOffsets", "earliest")
      .load()

    val events = raw
      .select(from_json(col("value").cast("string"), cdcSchema).alias("d"))
      .select("d.*")

    val query = events.writeStream
      .trigger(Trigger.ProcessingTime(60.seconds))
      .option("checkpointLocation",
        sys.env.getOrElse("CHECKPOINT_PATH", "s3a://chakra-lakehouse/checkpoints/cdc-orders/"))
      .foreachBatch { (batch: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], _: Long) =>
        if (!batch.isEmpty) {
          val upserts = batch.filter(col("__deleted") =!= lit("true"))
          val deletes  = batch.filter(col("__deleted") === lit("true"))

          if (!upserts.isEmpty) {
            if (!DeltaTable.isDeltaTable(spark, TablePath))
              upserts.drop("__deleted").write.format("delta").save(TablePath)
            else
              DeltaTable.forPath(spark, TablePath)
                .alias("t")
                .merge(upserts.drop("__deleted").alias("s"), "t.order_id = s.order_id")
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute()
          }

          // Soft-delete: mark row status rather than physically removing it
          if (!deletes.isEmpty && DeltaTable.isDeltaTable(spark, TablePath)) {
            DeltaTable.forPath(spark, TablePath)
              .alias("t")
              .merge(deletes.alias("s"), "t.order_id = s.order_id")
              .whenMatched().update(Map("status" -> lit("deleted")))
              .execute()
          }
        }
      }
      .start()

    query.awaitTermination()
  }
}
