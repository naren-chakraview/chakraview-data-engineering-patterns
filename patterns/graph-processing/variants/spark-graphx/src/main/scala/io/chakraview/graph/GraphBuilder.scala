package io.chakraview.graph

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GraphBuilder {

  // Load graph from a CSV edge list: src_id,dst_id,weight
  def fromEdgeList(spark: SparkSession, path: String): Graph[Long, Double] = {
    val edges: RDD[Edge[Double]] = spark.sparkContext
      .textFile(path)
      .filter(line => !line.startsWith("#") && line.nonEmpty)
      .map { line =>
        val parts = line.split(",")
        Edge(parts(0).trim.toLong, parts(1).trim.toLong,
          if (parts.length > 2) parts(2).trim.toDouble else 1.0)
      }
    Graph.fromEdges(edges, defaultValue = 1L)
  }
}
