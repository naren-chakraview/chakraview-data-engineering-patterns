package io.chakraview.semantic

import org.apache.spark.sql.SparkSession
import scala.util.{Success, Failure}

object Main {

  def main(args: Array[String]): Unit = {
    val config = RDFConfig.fromEnv()

    RDFConfig.validate(config) match {
      case Failure(e) =>
        System.err.println(s"Invalid config: ${e.getMessage}")
        System.exit(1)
      case Success(_) => ()
    }

    val spark = SparkSession
      .builder()
      .appName("semantic-batch-lakehouse")
      .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.lakehouse.type", "rest")
      .config("spark.sql.catalog.lakehouse.uri", config.icebergRestURI)
      .getOrCreate()

    try {
      val transformer = new SemanticTransformer(config)
      val writer = new JenaWriter(config)

      for (tableName <- config.icebergSilverTables) {
        println(s"Processing table: $tableName")
        // In production: read from Iceberg and transform
      }

      println("Batch job completed")
    } finally {
      spark.stop()
    }
  }
}
