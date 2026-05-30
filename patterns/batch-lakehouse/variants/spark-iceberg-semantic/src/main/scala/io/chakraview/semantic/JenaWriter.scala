package io.chakraview.semantic

import org.apache.spark.sql.{SaveMode, SparkSession}

class JenaWriter(config: RDFConfig) {

  private val icebergTable = config.icebergGoldTable
  private val tdb2Path = config.tdb2Path

  def writeRDF(
    triples: Seq[(String, String, String)],
    spark: SparkSession
  ): Unit = {
    writeToIceberg(triples, spark)
    writeToJenaTDB2(triples)
  }

  private def writeToIceberg(
    triples: Seq[(String, String, String)],
    spark: SparkSession
  ): Unit = {
    import spark.implicits._

    val df = spark.createDataFrame(triples).toDF("subject", "predicate", "object")

    df.write
      .mode(SaveMode.Append)
      .format("iceberg")
      .option("write-format", "parquet")
      .saveAsTable(icebergTable)

    println(s"Wrote ${triples.length} triples to Iceberg table: $icebergTable")
  }

  private def writeToJenaTDB2(triples: Seq[(String, String, String)]): Unit = {
    println(s"Placeholder: would write ${triples.length} triples to Jena TDB2 at: $tdb2Path")
    // In production: TDB2Factory.connectDataset(tdb2Path) + add statements
  }
}
