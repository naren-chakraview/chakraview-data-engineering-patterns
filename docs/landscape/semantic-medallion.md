# Semantic Medallion in the Data Landscape

A semantic medallion is a **medallion architecture augmented with RDF triples and SPARQL** — it solves the problem of unifying entities and relationships across a polyglot data estate without requiring a separate graph database.

## The problem it solves

Traditional data warehouses and lakehouses assume a single schema or a set of related schemas under one management boundary. But modern data estates are fragmented:

- A customer might be stored as `customer_id` in the order system, `party_id` in a legacy billing system, and `account_uuid` in a SaaS platform.
- A product might be sourced from an internal catalog, a vendor management system, and a marketplace API — each with different identifiers and attributes.
- Relationships matter: "Which customers bought products from vendors who are also partners?" — a query that spans three systems and requires joining across ID schemes.

Traditional approaches to this problem are expensive:

- **Centralized master data management (MDM):** A dedicated system of record that ingests and reconciles data. Adds latency and operational burden.
- **Extensive ETL:** Custom mapping code for each source pair. Brittle and hard to scale.
- **Data warehouse with slowly-changing dimensions:** Works for some cases, but requires upfront schema design and struggles with complex relationships.

**Semantic modeling** offers a different approach: model entities and relationships as **RDF triples** (subject-predicate-object statements) and query them with **SPARQL**. This separates identity resolution (choosing which entity ID is canonical) from storage and querying.

## How semantic medallion differs from traditional medallion

| Aspect | Traditional medallion | Semantic medallion |
|---|---|---|
| **Entity identity** | Resolved at warehouse layer via foreign keys and surrogate keys | Resolved at silver layer via deterministic IRI minting (SHA256 of key fields) |
| **Relationships** | Captured in foreign keys and join logic | Captured as RDF triples (subject-predicate-object) |
| **Query language** | SQL | SQL (for Iceberg/Delta) + SPARQL (for RDF triples) |
| **Schema flexibility** | Rigid — schema must be defined before ingestion | Flexible — can add predicates and relationships without schema migration |
| **Cross-domain reasoning** | Requires explicit joins; limited to pre-defined relationships | Supports arbitrary traversal; multi-hop queries are natural |
| **Provenance tracking** | Not built in | Native — every triple can carry source and confidence metadata |

## RDF vs relational trade-offs

### RDF strengths

- **Schema-on-read:** Relationships can be added without schema migration. A new link between customer and loyalty program can be added as a triple without touching the customer table.
- **Multi-hop traversal:** "Find all products sold by partners of suppliers of vendors for customers in region X" is a single SPARQL query, not a tree of SQL joins.
- **Flexible identity:** An entity can have multiple IDs (IRIs). Entity deduplication is adding a `owl:sameAs` edge, not a schema change.
- **Provenance:** Triples can carry metadata (graph annotations, confidence scores, source timestamps). Queries can filter by confidence or source.

### RDF costs

- **Storage overhead:** RDF triples take 3–4× the space of relational facts (each triple is subject-predicate-object-metadata). Compression via dictionary encoding helps but doesn't eliminate the gap.
- **Query complexity:** SPARQL is less familiar than SQL. Developers must learn recursive query patterns (e.g., `OPTIONAL`, `FILTER`).
- **Performance at scale:** SPARQL engines are optimized for graph traversal, not for scanning large fact tables. Jena TDB2 is fast for 100M–1B triples; beyond that, specialized graph databases (Neo4j, GraphDB) are better.
- **Operational complexity:** Another system to monitor and tune. Jena requires tuning for memory, TDB2 index settings, and query optimization.

## When to choose semantic medallion

**Choose RDF if:**
- Entities have multiple IDs and you need to deduplicate them across sources
- Queries require multi-hop relationship traversal (3+ joins in SQL)
- Relationships are dynamic and change frequently (adding new relationship types shouldn't require schema changes)
- Provenance and confidence tracking are important
- Your team is comfortable with ontology modeling (OWL, schema.org, domain-specific vocabularies)

**Choose traditional medallion if:**
- All data arrives under one management boundary with clear schemas
- Queries are primarily fact lookups and aggregations (not graph traversal)
- You need sub-second latency (SPARQL on TDB2 is not optimized for this)
- Your team prefers SQL and dbt over SPARQL and ontology design

## Positioning vs related patterns

**vs Graph Processing (Spark GraphX, Neo4j):**
- Graph Processing is for algorithms on graph-structured data (PageRank, community detection, shortest path).
- Semantic medallion is for entity unification and relationship queries.
- Use Graph Processing when relationships are the **data**; use semantic medallion when relationships are **metadata** that helps you reason about the data.

**vs Knowledge Graphs:**
- A knowledge graph is a specialized RDF system built for reasoning over semantic data.
- Semantic medallion is a pattern for layering semantic reasoning onto an existing medallion architecture.
- A knowledge graph is often the *output* of a semantic medallion pipeline (export RDF to a knowledge graph engine for more advanced reasoning).

**vs Master Data Management (MDM):**
- MDM is a system of record that centralizes identity resolution.
- Semantic medallion is a pattern for distributed identity resolution (each system resolves IDs locally, then semantic layer unifies them).
- Semantic medallion is lighter-weight and faster to implement; MDM is more centralized and governance-heavy.

## Implementation checklist

1. **Choose your base pattern:** Batch Lakehouse, CDC Pipeline, or Federated Query?
2. **Design your ontology:** What entities (classes) exist? What relationships (predicates) matter? Use schema.org or a domain-specific vocabulary.
3. **Implement entity resolution:** Mint IRIs deterministically at the silver layer using SHA256 of entity key fields.
4. **Extract RDF:** Convert silver layer tables to RDF triples. Use RML (R2RML) or Spark code to automate this.
5. **Deploy Jena TDB2:** Run Apache Jena as a sidecar to your lakehouse. Back up TDB2 snapshot regularly.
6. **Write SPARQL queries:** Test multi-hop relationship queries. Compare SPARQL results to expected SQL joins.
7. **Materialize results:** Write SPARQL query results back to lakehouse tables for BI consumption.

## References

- [Apache Jena](https://jena.apache.org/) — RDF storage and SPARQL query engine
- [W3C SPARQL specification](https://www.w3.org/TR/sparql11-query/) — query language reference
- [schema.org](https://schema.org/) — standard vocabulary for common entities (Person, Organization, Product, etc.)
- [Semantic medallion case study](../../case-studies/semantic-medallion.md) — step-by-step example
