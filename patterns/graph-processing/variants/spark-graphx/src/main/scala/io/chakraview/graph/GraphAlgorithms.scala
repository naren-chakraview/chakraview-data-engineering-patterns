package io.chakraview.graph

import org.apache.spark.graphx.Graph
import org.apache.spark.sql.{DataFrame, SparkSession}

object GraphAlgorithms {

  def pageRank(spark: SparkSession, graph: Graph[Long, Double], tolerance: Double = 0.001): DataFrame = {
    import spark.implicits._
    graph.pageRank(tolerance).vertices
      .toDF("vertex_id", "pagerank")
      .orderBy(org.apache.spark.sql.functions.desc("pagerank"))
  }

  def connectedComponents(spark: SparkSession, graph: Graph[Long, Double]): DataFrame = {
    import spark.implicits._
    graph.connectedComponents().vertices
      .toDF("vertex_id", "component_id")
  }

  def triangleCount(spark: SparkSession, graph: Graph[Long, Double]): DataFrame = {
    import spark.implicits._
    // triangleCount requires canonicalised (partition-aware) graph
    graph.partitionBy(org.apache.spark.graphx.PartitionStrategy.RandomVertexCut)
      .triangleCount().vertices
      .toDF("vertex_id", "triangle_count")
  }
}
