package io.chakraview.graph

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("graph-processing-graphx")
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

    val edgePath   = sys.env.getOrElse("EDGE_LIST_PATH",    "s3a://chakra-lakehouse/raw/graph-edges/")
    val outputPath = sys.env.getOrElse("GRAPH_OUTPUT_PATH", "s3a://chakra-lakehouse/delta/graph/")

    val graph = GraphBuilder.fromEdgeList(spark, edgePath)
    println(s"Graph: ${graph.vertices.count()} vertices, ${graph.edges.count()} edges")

    val pageRank = GraphAlgorithms.pageRank(spark, graph)
    pageRank.write.mode("overwrite").parquet(s"$outputPath/pagerank/")
    println(s"PageRank computed: ${pageRank.count()} vertices")

    val cc = GraphAlgorithms.connectedComponents(spark, graph)
    cc.write.mode("overwrite").parquet(s"$outputPath/connected-components/")
    println(s"Connected components: ${cc.select("component_id").distinct().count()} components")

    spark.stop()
  }
}
