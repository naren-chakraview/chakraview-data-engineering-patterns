# Graph Processing — Neo4j + Spark

## When to use

Need Cypher traversal queries (variable-depth paths) AND Spark-scale bulk ingestion/extraction.

## How to run locally

```bash
cp .env.example .env
docker compose up -d       # Neo4j Browser: http://localhost:7474

sbt assembly
spark-submit --class io.chakraview.graph.Main --master local[*] \
  --packages org.neo4j.driver:neo4j-connector-apache-spark_2.12:5.3.1 \
  target/scala-2.12/graph-processing-neo4j-spark-assembly-0.1.0.jar
```

Explore the graph in Neo4j Browser at http://localhost:7474:

```cypher
MATCH (o:Order)-[:PLACED_BY]->(c:Customer) RETURN o, c LIMIT 25
```

## Key queries (see `GraphQueries.scala`)

| Query | Purpose |
|---|---|
| `ordersWithCustomers` | Join Order → Customer nodes |
| `highVolumeCustomers` | Find fraud ring seeds by order volume |
| `reachableWithin3Hops` | Variable-depth traversal (impossible in SQL) |
