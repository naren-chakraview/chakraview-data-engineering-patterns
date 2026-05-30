import org.scalatest.funsuite.AnyFunSuite
import io.chakraview.semantic.SemanticTransformer

class SemanticTransformerSuite extends AnyFunSuite {

  test("recordToRDFTriples generates valid triples") {
    val config = io.chakraview.semantic.RDFConfig()
    val transformer = new SemanticTransformer(config)

    val record = Map(
      "iri" -> "https://company.com/customer#123",
      "name" -> "Acme",
      "status" -> "active"
    )

    val triples = transformer.recordToRDFTriples("customer", record)

    assert(triples.nonEmpty)
    assert(triples.exists(_.contains("Customer")))
    assert(triples.exists(_.contains("Acme")))
  }

  test("createReference creates valid predicate link") {
    val config = io.chakraview.semantic.RDFConfig()
    val transformer = new SemanticTransformer(config)

    val ref = transformer.createReference(
      "https://company.com/customer#c1",
      "hasInvoice",
      "https://company.com/invoice#i1"
    )

    assert(ref.contains("hasInvoice"))
    assert(ref.endsWith(" ."))
  }
}
