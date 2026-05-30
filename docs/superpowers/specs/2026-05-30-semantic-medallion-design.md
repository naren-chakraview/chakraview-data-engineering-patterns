# Semantic Medallion Architecture: Design Specification

**Date:** 2026-05-30  
**Status:** Design (ready for implementation planning)  
**Scope:** Three new pattern extensions + enterprise-modernization case study  

---

## Executive Summary

This spec defines how to integrate semantic medallion architecture (RDF/knowledge graphs) into the data-engineering-patterns repo. Rather than a standalone pattern, semantic medallion **extends three existing patterns** to enable data unification across polyglot data estates:

1. **Semantic Batch Lakehouse** — extends Batch Lakehouse; outputs RDF to Iceberg + Jena TDB2
2. **Semantic Federated Query** — extends Federated Query; adds SPARQL endpoint alongside Trino SQL
3. **Semantic CDC** — extends CDC Pipeline; mints stable IRIs for entities at ingest time

The case study integrates with **chakraview-enterprise-modernization** as a Phase 2 enhancement to Phase 1 dimensional modeling, showing how semantic unification solves polyglot data problems.

---

## Problem & Motivation

### Traditional Medallion Limitations

Current data engineering patterns (Batch Lakehouse, ELT/Warehouse, Federated Query) handle data transformation well but struggle with **entity unification across sources**:

- **Salesforce customer-123**, **Stripe account cus_xyz**, **Postgres order owner 123** — are they the same entity?
- Entity resolution logic scattered across queries, dashboards, and data pipelines
- New data source = schema negotiation overhead
- No self-describing data catalog; metadata lives separately

### Semantic Medallion Solution

Extend the Bronze-Silver-Gold medallion by embedding **knowledge graph structure** into the data itself:

| Layer | Traditional | Semantic |
|-------|-------------|----------|
| **Bronze** | Raw tables | Raw tables + source IRIs |
| **Silver** | Cleaned tables | Cleaned tables + stable IRIs (entity resolution done) |
| **Gold** | Dimensional tables | RDF triples (knowledge graph) |

**Key insight:** Mint a stable, globally unique identifier (IRI: Internationalized Resource Identifier) for each entity **once**, at the Silver layer. All downstream processes use that IRI. Relationships are embedded in the data itself via RDF triples, eliminating scattered JOINs and entity resolution logic.

---

## Architecture: Three Patterns + Data Flow

### Pattern 1: Semantic CDC (Bronze → Silver Bridge)

**What it does:** Captures source records, performs entity resolution, mints stable IRIs.

**Stack:**
- **Debezium 2.x** — captures changes from Salesforce, Stripe, Postgres
- **Kafka 3.x** — streams raw source records
- **IRI Minter Service (Python)** — subscribes to Kafka, performs entity resolution, emits `{source_record, assigned_iri}`
- **Spark Streaming** — persists enriched records back to Kafka for downstream consumption

**Data flow:**
```
Salesforce → Debezium → Kafka (salesforce.customers)
                          ↓
Stripe → Debezium ──→ Kafka (stripe.transactions)
                          ↓
Postgres → Debezium ──→ Kafka (postgres.orders)
                          ↓
                    IRI Minter Service
                    (entity resolution)
                          ↓
                    Enriched Kafka topics
                    {record + assigned_iri}
```

**Entity resolution strategy:**
- Deterministic rules: "match on email + domain for customers"
- Deduplication: "customer in Salesforce + Stripe with same email = one IRI"
- IRI format: `https://company.com/customer#{normalized_identifier}`

**Output:** Kafka topics with enriched records carrying IRIs, ready for Silver layer.

**Branch:** `pattern/cdc-pipeline/debezium-kafka-semantic`

---

### Pattern 2: Semantic Batch Lakehouse (Silver → Gold RDF)

**What it does:** Reads clean Silver tables, converts to RDF triples, writes to both Iceberg (Ntriples) and Jena TDB2.

**Stack:**
- **Spark 3.5 (Scala)** — batch processing
- **Iceberg 1.5** — storage for Silver input + Gold RDF output
- **Apache Jena 4.x** — RDF library for triple generation and validation
- **Jena TDB2** — dedicated RDF triple store (semantic store)

**Data flow:**
```
Silver (Iceberg tables)
  ├─ customers (with IRI column)
  ├─ invoices (with IRI column)
  └─ orders (with IRI column)
        ↓
  Semantic Batch Lakehouse
  (Spark + Jena)
        ↓
  RDF Triple Generation
  <https://company.com/customer#acme> a :Customer .
  <https://company.com/customer#acme> :hasName "Acme Corp" .
  <https://company.com/customer#acme> :hasInvoice <https://company.com/invoice#inv-001> .
        ↓
  Dual Output:
  1. Iceberg Gold (Ntriples format)
  2. Jena TDB2 (queryable RDF store)
```

**Key responsibilities:**
- Read Silver tables (fact + dimension tables, carrying IRIs)
- Map Silver schema to shared ontology (e.g., "customer" column → `:Customer` RDF class)
- Generate RDF triples using Jena
- Validate triple generation (no malformed IRIs, ontology compliance)
- Write to Iceberg (Ntriples: subject-predicate-object columns)
- Sync to Jena TDB2 for semantic queries

**Dual storage:**
- **Iceberg Ntriples:** Analytical queries via Trino SQL, schema-on-read, optimized for BI tools
- **Jena TDB2:** Semantic queries via SPARQL, ontology-aware, optimized for reasoning

**Branch:** `pattern/batch-lakehouse/spark-iceberg-semantic`

---

### Pattern 3: Semantic Federated Query (Multi-Source SPARQL)

**What it does:** Unifies querying across Iceberg tables and RDF store with a shared ontology.

**Stack:**
- **Trino 450** — distributed SQL engine
- **Iceberg connector** — queries Silver/Gold tables
- **Jena SPARQL endpoint** — queries RDF store
- **Custom bridge logic** — joins SQL + SPARQL results

**Data flow:**
```
Query Intent: "All active customers with unpaid invoices and support tickets"
  ↓
Option 1 (SQL): Trino queries Iceberg Silver/Gold tables
  SELECT c.name, COUNT(i.id) FROM customers c
  JOIN invoices i ON c.iri = i.customer_iri
  WHERE c.status = 'active' AND i.paid = false

Option 2 (SPARQL): SPARQL queries Jena TDB2
  SELECT ?customer ?invoice WHERE {
    ?customer a :Customer .
    ?customer :hasStatus :Active .
    ?customer :hasInvoice ?invoice .
    ?invoice :isPaid false .
  }

Option 3 (Hybrid): Trino + SPARQL bridge
  1. Trino identifies candidate customers
  2. SPARQL enriches with ontology-driven relationships
  3. Results merged
```

**Single source of truth:**
- One ontology defines "Customer", "Invoice", "Status" once
- Both SQL and SPARQL queries reference same ontology
- No schema drift between relational and RDF layers

**Branch:** `pattern/federated-query/trino-jena-sparql`

---

## Dual Storage: Iceberg + Jena TDB2

### Why Dual Storage?

| Storage | Best For | Query Language | Trade-offs |
|---------|----------|----------------|-----------|
| **Iceberg** | Analytical queries, BI tools, large-scale scans | SQL (Trino) | Schema-oriented, no implicit reasoning |
| **Jena TDB2** | Semantic queries, ontology reasoning, graph traversal | SPARQL | Smaller dataset size, specialized reasoning |

**Design choice:** Both coexist. Analytical teams use Iceberg+Trino (familiar SQL). Semantic teams use Jena+SPARQL (unified view, ontology-driven). Advanced users bridge both.

### Storage Details

**Iceberg (Gold layer):**
- Schema: `subject (string), predicate (string), object (string), source (string), ingested_at (timestamp)`
- Format: Parquet (columnar, optimized for analytical queries)
- Partitioning: By predicate type for query selectivity
- Retention: Full history (audit trail of RDF changes)

**Jena TDB2:**
- Triple store native to Jena
- Queryable via SPARQL endpoint (HTTP API)
- Indexes: subject, predicate, object (all directions)
- Reasoning: Optional inference via Jena rules or OWL reasoner

---

## Enterprise Modernization Integration: Phase 2

### Context

The **chakraview-enterprise-modernization** repo documents a strangler fig migration from Java EE monolith to cloud-native microservices. Phase 1 (current) implements traditional dimensional modeling (Kimball star schema).

### Phase 2: Semantic Unification Layer

**When:** After Phase 1 teams encounter polyglot data problem ("how do we unify customer data across Salesforce + Stripe + orders?")

**What:** Layer semantic medallion on top of Phase 1 tables without replacing Phase 1.

**Coexistence:**
- Phase 1: Dimensional model, Iceberg Silver/Gold tables, Trino SQL, BI dashboards (unchanged)
- Phase 2: RDF knowledge graph, Jena TDB2, SPARQL queries, entity unification (new)

**How to integrate:**
1. **Semantic CDC** wires up Salesforce, Stripe as new sources with IRI minting
2. **Semantic Batch Lakehouse** converts Phase 1 Silver/Gold tables to RDF
3. **Semantic Federated Query** adds SPARQL endpoint alongside Trino
4. **Ontology** defines "Customer" once, used by both phases

**Case study scenario:**
- E-commerce company with Shopify (storefront), Salesforce (support), Stripe (payments), custom Postgres (orders)
- Phase 1: Traditional medallion for operational analytics
- Phase 2: Semantic layer for churn analysis (correlate purchases + support tickets + payment failures)
- Business outcome: "give me customers with 3+ support tickets AND failed payments in last 30 days" — one SPARQL query, no manual joins

**Documentation location:** `chakraview-enterprise-modernization/docs/case-study/phase-2-semantic-unification/`

---

## Technology Choices & Rationale

### RDF Library: Apache Jena 4.x

**Selected:** Apache Jena + TDB2  
**Alternatives considered:** RDF4J, Virtuoso, GraphDB

**Rationale:**
- ✅ Open source (no vendor lock-in)
- ✅ Mature (20+ years, production deployments)
- ✅ Good SPARQL engine with reasoning support
- ✅ Self-hosted (no licensing, practitioners can run locally)
- ✅ Easy docker deployment
- ✅ Well-documented

**Trade-offs:**
- Smaller scale than enterprise RDF stores (GraphDB, Virtuoso) — acceptable for reference implementations
- Manual schema management (no UI like GraphDB) — acceptable, we document ontology as code

### IRI Minting: Entity Resolution

**Strategy:** Deterministic, rule-based entity resolution at CDC layer

**Implementation:**
- Rules defined in config: "match customers on email + domain"
- Hash-based IRI: `IRI = f(entity_type, normalized_match_key)`
- Deterministic: same customer always gets same IRI

**Example:**
```python
# Config
entity_rules = {
  "customer": {
    "key_fields": ["email", "domain"],
    "normalize": lambda e: f"{e.email}@{e.domain}".lower()
  }
}

# IRI generation
def mint_iri(entity_type, entity_dict, rules):
  rule = rules[entity_type]
  key = rule["normalize"](entity_dict)
  iri = f"https://company.com/{entity_type}#{hash(key)}"
  return iri
```

**Limitations acknowledged:**
- No real-time entity resolution updates (deterministic, batch-based)
- Requires manual rule configuration per domain
- Advanced: deduplication service can be added later (Spark SQL dedup, fuzzy matching)

---

## Repo Organization

```
chakraview-data-engineering-patterns/

patterns/
├── batch-lakehouse/
│   ├── README.md (updated with semantic variant link)
│   └── variants/
│       ├── spark-iceberg/
│       ├── spark-delta/
│       ├── spark-iceberg-airflow/
│       └── spark-iceberg-semantic/  ← NEW
│           ├── README.md
│           ├── docker-compose.yml
│           ├── .env.example
│           ├── build.sbt
│           ├── src/main/scala/
│           │   └── io/chakraview/semantic/
│           │       ├── SemanticTransformer.scala
│           │       ├── JenaWriter.scala
│           │       ├── RDFConfig.scala
│           │       └── Main.scala
│           └── tests/
│
├── federated-query/
│   ├── README.md (updated with semantic variant link)
│   └── variants/
│       ├── trino-iceberg-s3/
│       ├── presto-hive/
│       └── trino-jena-sparql/  ← NEW
│           ├── README.md
│           ├── docker-compose.yml
│           ├── .env.example
│           ├── config/
│           │   ├── catalog/
│           │   │   ├── iceberg.properties
│           │   │   ├── jena.properties  (new)
│           │   │   └── tpch.properties
│           │   ├── config.properties
│           │   └── jvm.config
│           ├── queries/
│           │   ├── examples/*.sparql
│           │   └── sql/*.sql
│           └── src/main/scala/
│               └── io/chakraview/semantic/
│                   └── JenaBridge.scala
│
├── cdc-pipeline/
│   ├── README.md (updated with semantic variant link)
│   └── variants/
│       ├── debezium-kafka-flink/
│       ├── debezium-kafka-spark/
│       └── debezium-kafka-semantic/  ← NEW
│           ├── README.md
│           ├── docker-compose.yml
│           ├── .env.example
│           ├── debezium-configs/
│           │   ├── salesforce-source.json
│           │   ├── stripe-source.json
│           │   └── postgres-source.json
│           └── src/main/python/
│               └── io/chakraview/semantic/
│                   ├── iri_minter/
│                   │   ├── __init__.py
│                   │   ├── service.py
│                   │   ├── config.yaml
│                   │   └── tests/
│                   └── entity_resolver/
│                       ├── __init__.py
│                       ├── resolver.py
│                       └── tests/

docs/
├── patterns/
│   ├── semantic-medallion.md  ← NEW: overview of all 3 patterns
│   └── ...
├── landscape/
│   └── semantic-medallion.md  ← NEW: where it fits in the landscape
├── adrs/
│   ├── ADR-0011-semantic-medallion-approach.md  ← NEW
│   ├── ADR-0012-rdf-storage-jena-choice.md  ← NEW
│   ├── ADR-0013-entity-resolution-strategy.md  ← NEW
│   └── ...

shared/
└── semantic/  ← NEW: shared code across patterns
    ├── ontologies/
    │   └── base-ecommerce.ttl  (example shared ontology)
    └── iri_minting/
        ├── __init__.py
        └── resolver.py  (shared IRI logic)
```

---

## Implementation: Three Focused Variants

### Semantic Batch Lakehouse (`spark-iceberg-semantic`)

**Goal:** Convert Iceberg Silver tables to RDF, output to Iceberg Gold + Jena TDB2

**Core files:**
- `SemanticTransformer.scala` — reads Silver tables, applies ontology mapping
- `JenaWriter.scala` — writes RDF to Iceberg + Jena
- `RDFConfig.scala` — ontology configuration, mapping rules
- `Main.scala` — entry point, orchestration
- `src/test/` — unit + integration tests

**Local dev:**
- `docker-compose.yml`: Spark, Iceberg REST, Jena TDB2, MinIO
- Example workflow: read sample Silver customer/invoice tables → convert to RDF → validate in Jena

**Key metrics:**
- ✅ RDF triple generation accuracy (validate IRIs, triples conform to ontology)
- ✅ Iceberg write performance (Ntriples schema, partitioning)
- ✅ Jena sync consistency (Iceberg ↔ TDB2 parity)

---

### Semantic Federated Query (`trino-jena-sparql`)

**Goal:** Query Iceberg + Jena via Trino SQL + SPARQL, with bridge logic for hybrid queries

**Core files:**
- `config/catalog/jena.properties` — Trino connector config for Jena endpoint
- `config/catalog/iceberg.properties` — Iceberg catalog (existing pattern)
- `queries/examples/*.sparql` — example SPARQL queries on Jena
- `queries/examples/*.sql` — example SQL queries on Iceberg
- `JenaBridge.scala` — logic to join Trino results + SPARQL results
- `src/test/` — unit + integration tests

**Local dev:**
- `docker-compose.yml`: Trino, Iceberg REST, Jena TDB2, MinIO, sample data
- Example workflow: run SQL query on Iceberg → run SPARQL on Jena → compare results

**Key metrics:**
- ✅ SPARQL query correctness (results match expected ontology inferences)
- ✅ Trino+Jena bridge logic (join semantics, null handling)
- ✅ Query latency (Trino SQL vs SPARQL vs hybrid)

---

### Semantic CDC (`debezium-kafka-semantic`)

**Goal:** Capture source records, mint IRIs, enrich with entity resolution

**Core files:**
- `iri_minter/service.py` — Kafka consumer, IRI minting logic
- `entity_resolver/resolver.py` — entity resolution rules (deterministic matching)
- `debezium-configs/*.json` — Debezium source connector configs
- `config.yaml` — entity resolution rules per source
- `src/test/` — unit + integration tests

**Local dev:**
- `docker-compose.yml`: Debezium, Kafka, Postgres (source), MinIO, IRI Minter service
- Example workflow: insert record in Postgres → Debezium captures → IRI Minter enriches → Kafka outputs enriched record

**Key metrics:**
- ✅ Entity resolution correctness (same entities get same IRI)
- ✅ IRI stability (deterministic, reproducible)
- ✅ Kafka throughput (latency from source change to enriched Kafka topic)

---

## Testing Strategy

### Unit Tests

**Semantic Batch Lakehouse:**
- RDF triple generation (correct subject-predicate-object triples)
- Ontology validation (IRIs well-formed, triples conform to ontology)
- Iceberg write (schema compliance, partitioning)

**Semantic Federated Query:**
- SPARQL query parsing and execution
- Trino+Jena bridge logic (SQL results + SPARQL results = expected combined result)
- Ontology inference (rules applied correctly)

**Semantic CDC:**
- IRI minting (deterministic, correct format)
- Entity resolution rules (matching logic correct)
- Kafka message generation (enriched records well-formed)

### Integration Tests

**End-to-end pipeline:**
1. Semantic CDC: insert records in Postgres → minted IRIs appear in Kafka
2. Semantic Batch Lakehouse: read enriched Kafka → write RDF to Iceberg + Jena
3. Semantic Federated Query: query Iceberg + Jena, results consistent

**All tests:** docker-compose, no external services, run locally in ~30 seconds

---

## Data Unification Value Proposition

### What Semantic Medallion Solves

| Problem | Traditional Solution | Semantic Solution |
|---------|----------------------|-------------------|
| Entity deduplication across sources | Separate ETL logic per source | IRI minting at CDC layer, once and done |
| Schema negotiation for new sources | Schema mapping in every query | Wire new source to ontology |
| "What is a Customer?" | Check multiple table definitions | Query ontology |
| Scattered entity resolution | JOINs in queries, dbt models, dashboards | IRIs in the data |
| Audit trail of relationships | SQL logs, lineage tools | RDF triples (who-where-when queryable) |

### Use Cases

1. **Customer 360:** "Give me all interactions for customer X across Salesforce + Stripe + orders" → one SPARQL query
2. **Risk Analysis:** "Find customers with payment failures + support escalations" → ontology-driven query, no manual joins
3. **Data Governance:** "Which source owns this customer?" → RDF triples show source lineage
4. **Catalog Self-Service:** "What entities are available?" → query ontology, no separate metadata system

---

## Success Criteria

✅ **Design approval:** All sections reviewed and approved  
✅ **Implementation planning:** Writing-plans skill creates task breakdown  
✅ **Pattern completeness:** All 3 patterns have boilerplate code + docker-compose + tests  
✅ **Case study design:** Enterprise-modernization Phase 2 scenario documented  
✅ **Documentation:** Patterns indexed, ADRs written, ontology examples provided  
✅ **Integration:** New patterns linked in landscape, decision matrix updated  

---

## Open Questions & Future Work

### Not in Scope (Design Phase)

- Advanced entity resolution (fuzzy matching, ML-based deduplication) — deterministic rules sufficient for MVP
- Ontology versioning/governance — single ontology per case study, extensible for future
- Inference engines (OWL reasoner) — Jena basic SPARQL sufficient, upgrade path documented
- High-cardinality RDF stores (billions of triples) — Jena TDB2 scales to tens of millions, Virtuoso/GraphDB path documented

### Post-Implementation

- Performance tuning (Iceberg partitioning strategy for RDF)
- Advanced SPARQL query optimization
- Real-time entity resolution (streaming dedup)
- Ontology learning from data

---

## References

**Related patterns:**
- Batch Lakehouse (existing)
- Federated Query (existing)
- CDC Pipeline (existing)
- Graph Processing (existing) — complements semantic queries

**Related docs:**
- chakraview-enterprise-modernization/docs/case-study/phase-2-semantic-unification/
- chakraview-zero-trust-blueprint/ — entity identity patterns

**Standards & frameworks:**
- RDF 1.1 (W3C) — data model
- SPARQL 1.1 (W3C) — query language
- OWL 2 (W3C) — ontology language
- Apache Jena 4.x documentation

---

## Approval

**Design reviewed and approved:** 2026-05-30  
**Ready for implementation planning:** Yes
