package io.chakraview.lakehouse

import org.apache.spark.sql.{DataFrame, SparkSession}

object IcebergWriter {

  def append(df: DataFrame, table: String): Unit =
    df.writeTo(table)
      .option("write.format.default", "parquet")
      .option("write.parquet.compression-codec", "snappy")
      .createOrReplace()

  def upsert(spark: SparkSession, df: DataFrame, table: String, idCol: String): Unit = {
    df.createOrReplaceTempView("__source")
    spark.sql(s"""
      MERGE INTO $table t
      USING __source s ON t.$idCol = s.$idCol
      WHEN MATCHED     THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """)
  }
}
