# Graph Processing

Graph algorithms on distributed graphs (Spark GraphX) or native Cypher traversal queries (Neo4j + Spark).

## Variants

| Variant | Engine | Best algorithm type | Pull branch |
|---|---|---|---|
| `spark-graphx` | Spark 3.5 GraphX (Scala) | PageRank, connected components, triangle count | `pattern/graph-processing/spark-graphx` |
| `neo4j-spark` | Neo4j 5 + Spark Connector (Scala) | Cypher traversal + Spark bulk ingestion | `pattern/graph-processing/neo4j-spark` |

## When NOT to use graph processing

Relationships are filters, not traversal targets: `SELECT * FROM orders WHERE customer_id = 'x'`
is a JOIN, not a graph query. Use a lakehouse pattern instead.
Use graph when the query is: "find all nodes reachable from A within N hops."
