ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "io.chakraview"

lazy val root = (project in file("."))
  .settings(
    name := "semantic-federated-query-jena-sparql",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.15" % "test",
    ),
  )
