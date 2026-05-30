# Semantic Medallion

Extends three foundational patterns (Batch Lakehouse, CDC Pipeline, and Federated Query) with semantic data modeling capabilities. Uses RDF triples and SPARQL to unify entities across polyglot sources, enabling entity deduplication, cross-domain reasoning, and multi-hop relationship queries without requiring a separate graph database.

## When to use

- **Entity unification across heterogeneous sources** — microservices, legacy systems, and cloud databases store the same real-world entity under different IDs and schemas. A customer in one system is a party in another, an account in a third. Semantic modeling makes these synonyms machine-discoverable.
- **Polyglot data estate with semantic reasoning** — your data lives in SQL, JSON, Parquet, and APIs. A traditional data warehouse can't reason about relationships between these without extensive ETL. Semantic medallion lets you model relationships as RDF triples and query across formats with SPARQL.
- **Compliance and audit trails** — you need to track how a business fact was derived, what sources fed it, and how confidence scores evolved. Semantic models capture provenance natively.

## When NOT to use

- **Single source of truth with clear schema** — if all data arrives in one format (Postgres + dbt or Snowflake), a traditional medallion adds no value. Use Batch Lakehouse or ELT / Warehouse.
- **No relationship queries at query time** — if you never need to ask "what entities are related to X?", RDF overhead is wasted.
- **Sub-second query latency required** — SPARQL on TDB2 is not optimized for millisecond response times. Use a purpose-built graph database.

## Architecture

Semantic medallion is a **medallion architecture extension**, not a replacement. It layers RDF and semantic reasoning on top of existing bronze/silver/gold medallion stages:

1. **Bronze layer** — raw data from all sources, stored as-is (S3 files, Kafka topics, database snapshots)
2. **Silver layer** — cleaned, deduplicated, with deterministic entity IRIs minted at CDC ingest (using SHA256 of key fields)
3. **Semantic layer** — RDF triples extracted from silver layer tables, stored in Apache Jena TDB2
4. **Gold layer** — queryable via SPARQL for cross-domain reasoning, with results materialized back to lakehouse tables for BI consumption

**Key innovation:** Entity identity is resolved at the silver layer (during CDC ingest), not at query time. This makes SPARQL queries fast and reproducible.

## Variants

Semantic medallion extends three existing patterns. Each adds semantic capability to its base pattern:

| Variant | Base pattern | Storage | SPARQL engine | When to use |
|---|---|---|---|---|
| [spark-iceberg-semantic](../patterns/batch-lakehouse/variants/spark-iceberg-semantic) | Batch Lakehouse | Iceberg on S3 + Jena TDB2 | Apache Jena TDB2 | Scheduled batch jobs; entity deduplication via deterministic IRI minting |
| [debezium-kafka-semantic](../patterns/cdc-pipeline/variants/debezium-kafka-semantic) | CDC Pipeline | Kafka + Jena TDB2 | Apache Jena TDB2 | Streaming CDC; real-time entity resolution at ingest; event-sourced semantic facts |
| [trino-jena-sparql](../patterns/federated-query/variants/trino-jena-sparql) | Federated Query | Trino + Jena TDB2 | Apache Jena TDB2 with Trino | Query across SQL + Iceberg + graph; semantic joins without moving data |

## Trade-offs

| Aspect | Benefit | Cost | Mitigation |
|---|---|---|---|
| **Semantic overhead** | Captures relationships and provenance; enables cross-domain queries | Schema design is harder; ontology requires care | Start with simple class/predicate model; use SHACL for validation |
| **RDF storage footprint** | Jena TDB2 is lightweight and embeddable | Triple storage is 3-4× the original fact size | Use schema.org or domain-specific ontologies; prune low-confidence edges |
| **SPARQL query complexity** | Powerful for graph traversal; multi-hop relationship queries are natural | Developers must learn SPARQL instead of SQL | Provide Trino SQL-to-SPARQL bridges or SPARQL query templates |
| **Entity resolution latency** | Deterministic hashing is fast (SHA256 on ingest) | Must choose entity resolution keys upfront | Audit entity key choices in code review; use ADR-0013 |
| **Operational complexity** | Integrated into existing medallion stages | Adds Jena TDB2 as new system to monitor | Run TDB2 in Docker; delegate to existing Spark cluster for management |

## Case study

See [Semantic Medallion Case Study](../../case-studies/semantic-medallion.md) for a step-by-step walkthrough of unifying customer entities across a payments system, an order management system, and a marketing platform.

## Related patterns

- [Batch Lakehouse](batch-lakehouse.md) — base pattern for spark-iceberg-semantic variant
- [CDC Pipeline](cdc-pipeline.md) — base pattern for debezium-kafka-semantic variant
- [Federated Query](federated-query.md) — base pattern for trino-jena-sparql variant
- [Graph Processing](graph-processing.md) — when you need a purpose-built graph database instead of semantic overlays
- [Knowledge Graphs](../landscape/semantic-medallion.md) — positioning and ontology primer

## Architecture decision records

- [ADR-0011: Semantic Medallion Approach](../adrs/ADR-0011-semantic-medallion-approach.md) — why extend existing patterns vs standalone implementation
- [ADR-0012: RDF Storage — Jena TDB2 Choice](../adrs/ADR-0012-rdf-storage-jena-choice.md) — evaluation of SPARQL engines and RDF databases
- [ADR-0013: Entity Resolution Strategy](../adrs/ADR-0013-entity-resolution-strategy.md) — deterministic IRI minting via SHA256 vs ML-based approaches
