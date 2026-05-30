name := "semantic-batch-lakehouse"
version := "1.0.0"
scalaVersion := "2.12.18"

val sparkVersion = "3.5.0"
val icebergVersion = "1.5.0"
val jenaVersion = "4.9.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.iceberg" %% "iceberg-spark-runtime-3.5" % icebergVersion,
  "org.apache.iceberg" % "iceberg-core" % icebergVersion,
  "org.apache.jena" % "jena-core" % jenaVersion,
  "org.apache.jena" % "jena-arq" % jenaVersion,
  "org.apache.jena" % "jena-tdb2" % jenaVersion,
  "org.scalatest" %% "scalatest" % "3.2.17" % "test",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "test"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
