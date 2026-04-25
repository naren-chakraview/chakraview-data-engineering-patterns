ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "io.chakraview"

val sparkVersion     = "3.5.1"
val neo4jConnVersion = "5.3.1"

lazy val root = (project in file("."))
  .settings(
    name := "graph-processing-neo4j-spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
      "org.neo4j.driver"  % "neo4j-connector-apache-spark_2.12" % neo4jConnVersion,
      "org.apache.hadoop"  % "hadoop-aws" % "3.3.6" % "provided",
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case "reference.conf"         => MergeStrategy.concat
      case _                        => MergeStrategy.first
    },
  )
