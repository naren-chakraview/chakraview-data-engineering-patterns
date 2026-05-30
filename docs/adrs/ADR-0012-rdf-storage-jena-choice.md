# ADR-0012: RDF Storage and SPARQL Engine — Apache Jena TDB2

**Status**: Accepted  
**Date**: 2026-05-30  
**Deciders**: Portfolio architect

---

## Context

Semantic medallion variants need to store and query RDF triples (subject-predicate-object facts). Several RDF storage and SPARQL query engines exist:

1. **Apache Jena TDB2**: Embedded RDF database; SPARQL query engine; open source; no external service required.
2. **Apache Jena Fuseki**: RDF server (uses TDB2 as backing store); exposes SPARQL via HTTP; good for scaling to multiple clients.
3. **GraphDB**: Commercial RDF database; advanced reasoning (OWL, SWRL); high query performance; not OSS.
4. **Neo4j**: Graph database; not native RDF, but can store RDF-like properties; strong community; cloud-native.
5. **RDF4J**: Open source; Java-based; similar feature set to Jena; lighter-weight than Fuseki.

We need to choose a storage engine that:
- Works with Apache Spark (for batch extraction of silver → RDF)
- Works with Kafka consumers (for real-time RDF emission from CDC)
- Is lightweight enough to embed in a lakehouse platform (no separate cluster)
- Is OSS (aligns with portfolio patterns' philosophy)
- Has mature SPARQL query support

## Decision

**Use Apache Jena TDB2 for initial implementation.** Deploy as:

- **For batch (Spark):** TDB2 as a local filesystem database, read/written by Spark jobs via Jena's Java API
- **For streaming (Kafka):** TDB2 snapshot + Kafka changelog topic (events represent RDF inserts/retracts)
- **For federation (Trino):** TDB2 snapshot exported as Parquet to Iceberg; Trino queries via Iceberg, not SPARQL (for now)

Provide a **migration path to Fuseki** (Jena HTTP server) for teams that later need:
- Horizontal scaling (multiple concurrent SPARQL clients)
- Separation of read and write workloads
- Query caching and optimization

## Rationale

**Why Jena TDB2?**

1. **Spark compatibility:** Jena's Java API integrates with Spark without external services. No separate RDF database to manage.
2. **Lightweight:** TDB2 is a single-process, embedded database. Ideal for a pattern repo starter kit.
3. **OSS:** Apache-licensed, no commercial restrictions.
4. **SPARQL maturity:** Full W3C SPARQL 1.1 support; reasonable query optimization.
5. **Embeddable:** Can be instantiated within a Spark job or a Kafka consumer; no external infrastructure required initially.

**Why not Fuseki initially?**

Fuseki adds operational complexity (HTTP server, endpoint management, authentication). For a starter kit pattern, TDB2's embedded model is simpler. We document a migration path to Fuseki in ADR consequences.

**Why not GraphDB or Neo4j?**

- GraphDB: Commercial; licensing cost; overkill for a starter pattern.
- Neo4j: Not native RDF; would require translation between RDF and Neo4j property graph model; doesn't align with semantic web standards.

**Why not skip RDF storage and use Spark DataFrames?**

We could represent RDF as Spark DataFrames (subject, predicate, object columns) and query via Spark SQL. This avoids a new system but loses SPARQL's graph navigation semantics. Multi-hop queries become complex SQL joins. SPARQL's `OPTIONAL`, `FILTER`, and recursive patterns are harder to express in Spark SQL.

## Consequences

**Positive:**

- No external RDF infrastructure to provision; TDB2 fits in a Docker container.
- SPARQL queries are standard (W3C); not vendor-locked to Spark or Iceberg.
- Spark jobs can read/write TDB2 directly via Jena API; no network latency.
- Migration to Fuseki is straightforward (same data format, just expose via HTTP).

**Negative:**

- TDB2 is single-process; horizontal scaling requires Fuseki (adds operational complexity).
- SPARQL query performance plateaus around 1B triples; beyond that, a specialized graph database is better.
- Jena TDB2 is less actively developed than Fuseki; security patches may lag.
- Pattern assumes teams are comfortable adding Java/Scala Jena dependencies to Spark projects.

## Migration path to Fuseki

**When to migrate from TDB2 to Fuseki:**
- Multiple concurrent SPARQL clients (> 5 simultaneous queries)
- RDF triple count > 500M (TDB2 performance degrades)
- Need HTTP-based access (Trino, BI tools, external services)
- Require authentication and role-based query access

**How to migrate:**

1. Export TDB2 backup to N-Triples or Turtle format
2. Create Fuseki server with same TDB2 backing store
3. Point Spark jobs and Kafka consumers to Fuseki HTTP endpoint instead of embedded TDB2
4. (Optional) add Fuseki caching and query optimization layers

The API remains SPARQL; only the transport (embedded Java → HTTP) changes.

## Alternatives considered

**Option A: RDF4J**

Similar feature set to Jena; slightly lighter-weight. Could work, but Jena has broader community adoption and better Spark integration examples.

**Option B: Postgres with RDF extensions**

Use PostgreSQL + PostGIS (geographic queries) or pg_sparql extension. Trades heavyweight database operations for RDF-native query patterns.

Rejected because:
- Requires running PostgreSQL; adds operational burden vs embedded TDB2.
- pg_sparql extension is less mature than Jena.
- SPARQL queries translated to SQL are harder to reason about than native SPARQL.

**Option C: Treat RDF as a Spark DataFrame**

Store (subject, predicate, object) as three columns in Iceberg; query via Spark SQL.

Rejected because:
- Multi-hop graph queries become nested self-joins; hard to express and maintain.
- Loses SPARQL's semantic query optimization.
- No standard way to represent RDF features (IRIs, blank nodes, literals with language tags).
