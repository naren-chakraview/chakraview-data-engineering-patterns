# Chakra Data Engineering Patterns

Idiomatic boilerplate starters for 9 data engineering patterns × 22 stack variants.
Pick a pattern, pick a stack, pull the branch — done.

📖 **[Full docs site](https://naren-chakraview.github.io/chakraview-data-engineering-patterns/)**

---

## Patterns at a glance

| Pattern | Variants |
|---|---|
| **Batch Lakehouse** | Spark+Iceberg · Spark+Delta · Spark+Iceberg+Airflow |
| **Streaming Lakehouse** | Flink+Iceberg · Spark Streaming+Delta |
| **ELT / Warehouse** | dbt+Snowflake · dbt+BigQuery · dbt+DuckDB+Airflow |
| **CDC Pipeline** | Debezium+Kafka+Flink · Debezium+Kafka+Spark |
| **Lambda Architecture** | Flink+Spark+Iceberg · Spark Streaming+Spark Batch+Delta |
| **Federated Query** | Trino+Iceberg+S3 · Presto+Hive |
| **ML Feature Pipeline** | Feast+Redis+Spark · Flink+Spark offline · Feast+Spark+Airflow |
| **Graph Processing** | Spark GraphX · Neo4j+Spark |
| **Workflow Orchestration** | Airflow · Prefect · Dagster |

---

## 🔗 Semantic Medallion: Entity Unification via RDF

**NEW:** Transform your data estate into a unified knowledge graph with **semantic medallion** — extends 3 core patterns with RDF/SPARQL for cross-source entity deduplication and ontology-driven analytics.

### Semantic Pattern Variants

| Pattern | Semantic Variant | What it does |
|---|---|---|
| **CDC Pipeline** | `debezium-kafka-semantic` | Real-time IRI minting + entity resolution at ingest |
| **Batch Lakehouse** | `spark-iceberg-semantic` | Converts Silver tables to RDF triples (Iceberg Gold + Jena TDB2) |
| **Federated Query** | `trino-jena-sparql` | Unifies SQL + SPARQL queries via IRI mapping + shared ontology |

### Use Cases

- **Entity Unification**: Single IRI for customers across Salesforce, Stripe, Postgres, etc.
- **Semantic Reasoning**: Ontology-driven queries (e.g., "at-risk customers with 3+ support tickets + failed payments")
- **Data Catalog**: RDF triples ARE self-describing data catalog
- **Knowledge Graph**: Full provenance and relationships embedded in data

### Quick Start

```bash
# Explore semantic medallion architecture
git clone https://github.com/naren-chakraview/chakraview-data-engineering-patterns
cd chakraview-data-engineering-patterns
cat docs/patterns/semantic-medallion.md          # Pattern overview
cat docs/adrs/ADR-0011-semantic-medallion-approach.md  # Design decisions
cat docs/landscape/semantic-medallion.md         # Positioning

# Deploy Phase 1-4 incrementally
git clone --branch pattern/cdc-pipeline/debezium-kafka-semantic \
  https://github.com/naren-chakraview/chakraview-data-engineering-patterns \
  my-semantic-cdc
```

📚 **Read More**: [Semantic Medallion Pattern](https://naren-chakraview.github.io/chakraview-data-engineering-patterns/patterns/semantic-medallion/) | [Architecture Decision Records](https://naren-chakraview.github.io/chakraview-data-engineering-patterns/adrs/#semantic-medallion)

---

## How to use

**Browse everything** — clone `main` and explore `patterns/`:

```bash
git clone https://github.com/naren-chakraview/chakraview-data-engineering-patterns
```

**Pull a single variant** — clone only that branch (variant files land at repo root):

```bash
git clone --branch pattern/batch-lakehouse/spark-iceberg \
  https://github.com/naren-chakraview/chakraview-data-engineering-patterns \
  my-lakehouse
cd my-lakehouse
cp .env.example .env   # fill in your values
docker compose up -d
```

---

## Language convention

| Ecosystem | Language |
|---|---|
| Spark | Scala 2.12 |
| Flink | Java 17 |
| dbt / SQL-only | SQL |
| ML, Orchestration | Python 3.11 |
| Config-heavy (CDC, Federated Query) | YAML/JSON + SQL |
