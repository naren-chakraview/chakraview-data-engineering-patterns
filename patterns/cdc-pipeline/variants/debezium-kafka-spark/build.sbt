ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "io.chakraview"

val sparkVersion = "3.5.1"
val deltaVersion = "3.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "cdc-pipeline-spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"           % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"            % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
      "io.delta"         %% "delta-spark"          % deltaVersion,
      "org.apache.hadoop"  % "hadoop-aws"          % "3.3.6"      % "provided",
      "com.amazonaws"      % "aws-java-sdk-bundle" % "1.12.262"   % "provided",
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case "reference.conf"         => MergeStrategy.concat
      case _                        => MergeStrategy.first
    },
  )
