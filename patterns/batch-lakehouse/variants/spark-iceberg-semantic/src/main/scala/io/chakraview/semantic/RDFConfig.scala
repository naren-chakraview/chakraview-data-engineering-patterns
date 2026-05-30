package io.chakraview.semantic

import scala.util.Try

case class RDFConfig(
  baseIRI: String = "https://company.com",
  icebergSilverDatabase: String = "lakehouse",
  icebergSilverTables: List[String] = List("customers", "invoices", "orders"),
  icebergGoldTable: String = "lakehouse.gold_rdf",
  icebergRestURI: String = "http://localhost:8181",
  tdb2Path: String = "/tmp/tdb2",
  ontologyPath: String = "src/main/resources/ontology.ttl"
)

object RDFConfig {
  def fromEnv(): RDFConfig = {
    RDFConfig(
      baseIRI = sys.env.getOrElse("BASE_IRI", "https://company.com"),
      icebergRestURI = sys.env.getOrElse("ICEBERG_REST_URI", "http://localhost:8181"),
      tdb2Path = sys.env.getOrElse("TDB2_PATH", "/tmp/tdb2")
    )
  }

  def validate(config: RDFConfig): Try[Unit] = Try {
    require(config.baseIRI.nonEmpty, "baseIRI cannot be empty")
    require(config.icebergGoldTable.nonEmpty, "icebergGoldTable cannot be empty")
    require(config.tdb2Path.nonEmpty, "tdb2Path cannot be empty")
  }
}
