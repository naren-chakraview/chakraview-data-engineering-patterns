package io.chakraview.lakehouse

import org.apache.spark.sql.SparkSession

object SparkSessions {
  def iceberg(appName: String): SparkSession =
    SparkSession.builder()
      .appName(appName)
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.lakehouse.type", "rest")
      .config("spark.sql.catalog.lakehouse.uri",
        sys.env.getOrElse("ICEBERG_REST_URI", "http://localhost:8181"))
      .config("spark.sql.catalog.lakehouse.warehouse",
        sys.env.getOrElse("LAKEHOUSE_WAREHOUSE", "s3a://chakra-lakehouse/"))
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
}
