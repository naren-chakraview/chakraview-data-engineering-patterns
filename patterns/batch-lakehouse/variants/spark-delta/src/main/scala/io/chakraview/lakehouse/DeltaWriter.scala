package io.chakraview.lakehouse

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}

object DeltaWriter {

  def append(df: DataFrame, path: String): Unit =
    df.write
      .format("delta")
      .mode("append")
      .save(path)

  def merge(spark: SparkSession, df: DataFrame, path: String, idCol: String): Unit = {
    if (!DeltaTable.isDeltaTable(spark, path)) {
      df.write.format("delta").save(path)
      return
    }
    DeltaTable.forPath(spark, path)
      .alias("target")
      .merge(df.alias("source"), s"target.$idCol = source.$idCol")
      .whenMatched().updateAll()
      .whenNotMatched().insertAll()
      .execute()
  }
}
