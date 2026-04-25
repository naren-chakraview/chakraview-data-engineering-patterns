package io.chakraview.graph

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  private def neo4jOptions: Map[String, String] = Map(
    "url"                            -> sys.env.getOrElse("NEO4J_URI",      "bolt://localhost:7687"),
    "authentication.type"            -> "basic",
    "authentication.basic.username"  -> sys.env.getOrElse("NEO4J_USER",     "neo4j"),
    "authentication.basic.password"  -> sys.env.getOrElse("NEO4J_PASSWORD", "password"),
  )

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("graph-processing-neo4j-spark")
      .getOrCreate()

    val opts = neo4jOptions

    // 1. Read orders from CSV (source system) and bulk-load into Neo4j
    val orders = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(sys.env.getOrElse("ORDERS_PATH", "data/sample-orders.csv"))

    orders.write
      .format("org.neo4j.spark.DataSource")
      .options(opts)
      .option("labels",    ":Order")
      .option("node.keys", "order_id")
      .mode("Overwrite")
      .save()
    println(s"Loaded ${orders.count()} Order nodes into Neo4j")

    // 2. Query graph via Cypher and pull results back as a Spark DataFrame
    val ordersWithCustomers = spark.read
      .format("org.neo4j.spark.DataSource")
      .options(opts)
      .option("query", GraphQueries.ordersWithCustomers)
      .load()

    ordersWithCustomers
      .groupBy("customer_id")
      .agg(count("order_id").alias("order_count"), sum("amount_cents").alias("total_cents"))
      .orderBy(desc("order_count"))
      .show(20)

    spark.stop()
  }
}
