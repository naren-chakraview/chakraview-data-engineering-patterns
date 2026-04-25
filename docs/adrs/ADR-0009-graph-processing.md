# ADR-0009: Graph Processing — When Graph is the Right Model

**Status**: Accepted
**Date**: 2026-04-25
**Deciders**: Portfolio architect

---

## Context

Graph processing is included in this repo as a pattern. The inclusion requires justification: most data engineering workloads do not require graph algorithms, and graph is frequently over-applied to problems that are better solved with a standard lakehouse pattern and SQL joins.

A decision is also needed on which graph engine to show: Spark GraphX (bulk distributed graph algorithms), Neo4j (native graph database with Cypher traversal), or a combination.

## Decision

Include the graph-processing pattern with **two variants**:

1. **Spark GraphX** — for batch graph algorithms (PageRank, connected components, triangle count) on large distributed graphs.
2. **Neo4j + Spark connector** — for use cases requiring Cypher traversal queries alongside Spark-scale bulk ingestion.

Document clearly in both the pattern README and each variant README **when graph is not the right model**, so engineers reach for the correct pattern.

## Rationale

### When graph is the right model

Graph is the correct model when **relationships are the query target, not a filter**:

- "Find all accounts reachable within 3 hops from a flagged account" — graph traversal. The depth is variable; a SQL self-join at fixed depth is impractical.
- "Rank documents by link authority" — PageRank. The score of each node depends on the scores of its neighbours, which depend on their neighbours recursively.
- "Find all connected components in a user-device graph" — connected components. Equivalent SQL requires recursive CTEs with depth limits.

### When graph is NOT the right model

- "Get all orders for a customer" — a foreign key join. Use a lakehouse pattern and SQL.
- "Find users who placed more than 10 orders" — an aggregation. Use a lakehouse pattern and SQL.
- "Find users in the same city as a given user" — an equality filter. Use a lakehouse pattern and SQL.

The distinguishing test: if the query can be expressed as a fixed-depth join or a GROUP BY, it is not a graph problem.

### Spark GraphX vs Neo4j

**Spark GraphX** excels at bulk graph algorithms on large graphs (billions of edges). The graph is loaded from files (Parquet, CSV), algorithms run as distributed MapReduce-style iterations, and results are written back to files or a lakehouse table. GraphX is appropriate when:

- The graph is too large for Neo4j's memory model (Neo4j is optimised for graphs that fit in RAM + page cache).
- The workload is batch (nightly PageRank, weekly community detection).
- The team already operates Spark and does not want to introduce another system.

**Neo4j** excels at online graph traversal with variable depth. Cypher queries like `MATCH path = (a)-[*1..5]-(b)` are not expressible efficiently in SQL or GraphX. Neo4j is appropriate when:

- The workload requires real-time or near-real-time graph traversal (fraud detection, recommendation engines).
- The graph fits in Neo4j's managed memory (< 500GB is comfortable on modern hardware).
- Cypher is preferable to Spark DataStream operations for the engineering team.

## Consequences

**Positive:**

- Engineers see both approaches — distributed batch graph (GraphX) and native graph database (Neo4j) — with a clear decision boundary.
- The pattern README front-loads the "when NOT to use graph" guidance, reducing misapplication.

**Negative:**

- Neo4j requires a commercial licence for clustering and certain algorithms in the Graph Data Science (GDS) library. The boilerplate uses Neo4j Community Edition, which is sufficient for local development but not for production HA deployments.
- GraphX is a mature but relatively stagnant part of Spark. Its API has not changed significantly since Spark 2.x.

## When this choice stops being correct

If Apache Spark GraphX is deprecated in a future Spark release in favour of GraphFrames or a Flink graph library, the GraphX variant should be updated or replaced. As of Spark 3.5, GraphX is still included and supported.

## Alternatives considered

**Apache Giraph / Pregel:** MapReduce-based graph processing. Superseded by GraphX and effectively end-of-life. Rejected.

**GraphFrames:** A Spark graph library built on DataFrames rather than RDDs, with a more SQL-friendly API than GraphX. More actively developed than GraphX. Considered as a replacement for GraphX but excluded because it is not part of the Spark core distribution — it requires a separate JAR, and the boilerplate would need to explain the dependency relationship with GraphX. Included as a note in the GraphX variant README.

**Amazon Neptune / Azure Cosmos DB graph API:** Managed graph databases. Rejected because they require cloud credentials and cannot be run locally without significant cost.
