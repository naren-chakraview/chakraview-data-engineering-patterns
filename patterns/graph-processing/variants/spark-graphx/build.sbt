ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "io.chakraview"

val sparkVersion  = "3.5.1"
val hadoopVersion = "3.3.6"

lazy val root = (project in file("."))
  .settings(
    name := "graph-processing-spark-graphx",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"         % sparkVersion  % "provided",
      "org.apache.spark" %% "spark-sql"          % sparkVersion  % "provided",
      "org.apache.spark" %% "spark-graphx"       % sparkVersion  % "provided",
      "org.apache.hadoop"  % "hadoop-aws"         % hadoopVersion % "provided",
      "com.amazonaws"      % "aws-java-sdk-bundle" % "1.12.262"  % "provided",
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case "reference.conf"         => MergeStrategy.concat
      case _                        => MergeStrategy.first
    },
  )
