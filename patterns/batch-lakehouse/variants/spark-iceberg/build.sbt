ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "io.chakraview"

val sparkVersion   = "3.5.1"
val icebergVersion = "1.5.2"
val hadoopVersion  = "3.3.6"

lazy val root = (project in file("."))
  .settings(
    name := "batch-lakehouse-spark-iceberg",
    libraryDependencies ++= Seq(
      "org.apache.spark"  %% "spark-core"                     % sparkVersion   % "provided",
      "org.apache.spark"  %% "spark-sql"                      % sparkVersion   % "provided",
      "org.apache.iceberg"  % "iceberg-spark-runtime-3.5_2.12" % icebergVersion,
      "org.apache.hadoop"   % "hadoop-aws"                    % hadoopVersion  % "provided",
      "com.amazonaws"       % "aws-java-sdk-bundle"           % "1.12.262"     % "provided",
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case "reference.conf"         => MergeStrategy.concat
      case _                        => MergeStrategy.first
    },
  )
