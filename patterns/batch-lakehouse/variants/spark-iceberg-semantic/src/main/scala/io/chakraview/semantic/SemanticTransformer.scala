package io.chakraview.semantic

import scala.collection.mutable

class SemanticTransformer(config: RDFConfig) {

  private val baseIRI = config.baseIRI

  def recordToRDFTriples(entityType: String, record: Map[String, String]): List[String] = {
    val iri = record.get("iri").getOrElse(
      throw new IllegalArgumentException(s"Record missing 'iri' field: $record")
    )

    val triples = mutable.ListBuffer[String]()
    triples += s"<$iri> a :${capitalize(entityType)} ."

    record.foreach { case (key, value) =>
      if (key != "iri" && !key.startsWith("_")) {
        val predicate = camelCase(key)
        val escapedValue = escapeRDFLiteral(value)
        triples += s"<$iri> :$predicate $escapedValue ."
      }
    }

    triples.toList
  }

  def createReference(fromIRI: String, predicate: String, toIRI: String): String = {
    s"<$fromIRI> :$predicate <$toIRI> ."
  }

  private def capitalize(s: String): String =
    if (s.nonEmpty) s.head.toUpper + s.tail else s

  private def camelCase(s: String): String = {
    s.split("_").zipWithIndex.map {
      case (part, 0) => part
      case (part, _) => capitalize(part)
    }.mkString
  }

  private def escapeRDFLiteral(value: String): String = {
    val escaped = value
      .replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
    s"\"$escaped\""
  }

  def validateTriple(triple: String): Boolean = {
    triple.contains("<") && triple.contains(">") && triple.contains(" ") && triple.endsWith(" .")
  }
}
