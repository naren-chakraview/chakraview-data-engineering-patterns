# Graph Processing

Graph algorithms (PageRank, connected components, shortest path) run on distributed graphs via Spark GraphX, or graph-native Cypher queries run in Neo4j with Spark used for bulk ingestion and result extraction.

## When to use

- Query is a graph traversal: "find all nodes reachable from A within N hops"
- Relationship attributes (edge weight, edge type, edge timestamp) are as important as node attributes
- Algorithms are graph-native: PageRank, community detection, shortest path, triangle counting
- Relational JOINs at fixed depth cannot express the query (variable-depth traversal)

## When NOT to use

- Relationships are filters, not traversal targets ("orders by customers in region X" is a JOIN, not a graph query)
- Aggregate analytics (SUM, COUNT, GROUP BY) dominate — use a lakehouse pattern
- Graph fits in a single machine — NetworkX (Python) is sufficient; no distributed engine required

## Variants

| Variant | Stack | Use when |
|---|---|---|
| [spark-graphx](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/graph-processing/variants/spark-graphx) | Spark 3.5 GraphX (Scala) | Batch graph algorithms on large distributed graphs; PageRank, connected components |
| [neo4j-spark](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/graph-processing/variants/neo4j-spark) | Neo4j 5 + Spark Connector (Scala + Cypher) | Need Cypher traversal queries + Spark for bulk ingestion |

## Related patterns

- [Batch Lakehouse](batch-lakehouse.md) — to store graph algorithm results for downstream consumption
- [Federated Query](federated-query.md) — to query graph results alongside other data sources
