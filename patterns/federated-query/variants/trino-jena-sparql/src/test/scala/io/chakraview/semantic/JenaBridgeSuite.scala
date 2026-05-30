import org.scalatest.funsuite.AnyFunSuite
import io.chakraview.semantic.JenaBridge

class JenaBridgeSuite extends AnyFunSuite {

  test("joinResults merges SQL and SPARQL by IRI mapping") {
    val sqlResults = Seq(
      Map("customer_id" -> "c123", "name" -> "Acme Corp"),
      Map("customer_id" -> "c456", "name" -> "BetaCorp")
    )

    val sparqlResults = Seq(
      Map("customer" -> "https://company.com/customer#c123", "status" -> "Active"),
      Map("customer" -> "https://company.com/customer#c456", "status" -> "Inactive")
    )

    val bridge = new JenaBridge()
    val joined = bridge.joinResults(
      sqlResults,
      sparqlResults,
      sqlKey = "customer_id",
      sparqlKey = "customer",
      mapper = (id: String) => s"https://company.com/customer#$id"
    )

    assert(joined.length == 2)
    assert(joined.head("name") == "Acme Corp")
    assert(joined.head("status") == "Active")
  }

  test("validateSPARQLQuery checks syntax") {
    val bridge = new JenaBridge()

    assert(bridge.validateSPARQLQuery("SELECT ?x WHERE { ?x a :Type }"))
    assert(!bridge.validateSPARQLQuery("INVALID QUERY"))
  }
}
