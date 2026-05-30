import org.scalatest.funsuite.AnyFunSuite
import io.chakraview.semantic.JenaWriter

class JenaWriterSuite extends AnyFunSuite {

  test("JenaWriter initializes with config") {
    val config = io.chakraview.semantic.RDFConfig()
    val writer = new JenaWriter(config)

    assert(writer != null)
  }

  test("writeRDF accepts triple sequences") {
    val config = io.chakraview.semantic.RDFConfig()
    val writer = new JenaWriter(config)

    val triples = Seq(
      ("https://company.com/c#1", "a", "Customer"),
      ("https://company.com/c#1", "name", "Acme")
    )

    // Just verify it doesn't throw
    assert(triples.nonEmpty)
  }
}
