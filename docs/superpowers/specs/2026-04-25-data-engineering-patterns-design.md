# Design: chakraview-data-engineering-patterns

**Date**: 2026-04-25  
**Status**: Approved  
**Author**: Naren Chakraview

---

## Purpose

A reusable reference repository of boilerplate starters for common big data / data engineering patterns. Each pattern is shown in 2–3 idiomatic stack variants. The repo serves two audiences: (1) engineers who want a starting point for a real project and (2) readers who want a landscape-level understanding of where each pattern and stack fits.

---

## Repo Name

`chakraview-data-engineering-patterns`

---

## Top-Level Structure

```
chakraview-data-engineering-patterns/
├── patterns/
│   ├── batch-lakehouse/
│   ├── streaming-lakehouse/
│   ├── elt-warehouse/
│   ├── cdc-pipeline/
│   ├── lambda-architecture/
│   ├── federated-query/
│   ├── ml-feature-pipeline/
│   ├── graph-processing/
│   └── workflow-orchestration/
├── docs/
│   ├── index.md
│   ├── landscape/
│   │   └── index.md
│   ├── decision-matrix/
│   │   └── index.md
│   ├── patterns/
│   │   └── <one page per pattern>.md
│   ├── adrs/
│   │   ├── README.md
│   │   └── ADR-000N-*.md
│   └── assets/
│       └── logo.svg
├── tooling/
│   └── sync-pattern-branches.sh
├── mkdocs.yml
└── requirements-docs.txt
```

Each `patterns/<name>/` follows:

```
patterns/<name>/
├── README.md               ← when to use this pattern; variant comparison
└── variants/
    └── <stack>/            ← self-contained boilerplate
        ├── README.md
        ├── src/
        ├── config/
        ├── docker-compose.yml
        ├── .env.example
        └── build.sbt | pom.xml | pyproject.toml  (language-dependent)
```

---

## Pattern Catalog

| Pattern | Variant A | Variant B | Variant C |
|---|---|---|---|
| **batch-lakehouse** | Spark + Iceberg (Scala) | Spark + Delta Lake (Scala) | Spark + Iceberg + Airflow (Scala + Python DAG) |
| **streaming-lakehouse** | Flink + Iceberg (Java) | Spark Structured Streaming + Delta Lake (Scala) | — |
| **elt-warehouse** | dbt + Snowflake (SQL) | dbt + BigQuery (SQL) | dbt + DuckDB + Airflow (SQL + Python DAG) |
| **cdc-pipeline** | Debezium + Kafka + Flink (Java) | Debezium + Kafka + Spark (Scala) | — |
| **lambda-architecture** | Flink (streaming) + Spark (batch) + Iceberg | Spark Streaming + Spark Batch + Delta Lake | — |
| **federated-query** | Trino + Iceberg + S3 (config + SQL) | Presto + Hive metastore (config + SQL) | — |
| **ml-feature-pipeline** | Feast + Redis + Spark (Python) | Flink online + Spark offline (Python) | Feast + Spark + Airflow (Python DAG) |
| **graph-processing** | Spark GraphX (Scala) | Neo4j + Spark connector (Scala + Cypher) | — |
| **workflow-orchestration** | Airflow (Python DAGs) | Prefect (Python flows) | Dagster (Python assets) |

**Total: 9 patterns, 22 variants.**

### Language convention

| Ecosystem | Language |
|---|---|
| Spark | Scala |
| Flink | Java |
| dbt / SQL-only | SQL |
| ML, Orchestration | Python |
| Config-heavy (CDC, Federated Query) | YAML/JSON + SQL |

---

## Per-Variant Contents

Every variant is independently self-contained:

```
variants/<stack>/
├── README.md           ← when to use, when NOT to use, trade-offs vs siblings, how to run locally
├── src/                ← idiomatic boilerplate
├── config/             ← connector configs, cluster manifests, env templates
├── docker-compose.yml  ← local dev stack
├── .env.example        ← required env vars with explanatory comments
└── build.sbt | pom.xml | pyproject.toml  (absent for SQL-only variants)
```

**Boilerplate depth by language:**

- **Scala (Spark/GraphX)** — `Main.scala` entry point + 1–2 classes showing the pattern's key idiom (e.g., `IcebergSink.scala`, `DeltaMerge.scala`)
- **Java (Flink)** — `Main.java` + one `ProcessFunction` or `SinkFunction`
- **Python (ML/Orchestration)** — one DAG/flow/asset file + one feature definition or pipeline class
- **SQL (dbt)** — `models/`, `profiles.yml`, example models showing the pattern's idiom
- **Config-heavy (CDC, Federated)** — connector JSON, cluster YAML, example SQL views

Each `README.md` covers four sections: **When to use**, **When NOT to use**, **Trade-offs vs sibling variants**, **How to run locally**.

---

## Docs Site

Built with MkDocs Material (same theme as other chakraview repos). Deployed to GitHub Pages via CI.

### Landscape intro (`docs/landscape/index.md`)

Covers the full modern data processing landscape:
- Evolution from Hadoop/MapReduce → Spark → streaming-first → lakehouse → data mesh
- Where batch wins, where streaming is necessary, where ELT dominates
- Table/format layer: Iceberg vs Delta Lake vs Hudi — when each matters
- Orchestration landscape: Airflow vs Prefect vs Dagster vs Temporal
- Cloud-native vs open-source trade-offs
- How to read the decision matrix and pick a starting point

### Decision matrix (`docs/decision-matrix/index.md`)

Two tables:

1. **Pattern selection** — rows: use-case characteristics (latency requirement, reprocessing need, data volume, team skills, cost sensitivity); columns: patterns; cells: fit score + brief rationale
2. **Stack selection** — per pattern, rows: stack variants; columns: operational complexity, cloud portability, ecosystem maturity, cost profile

### Pattern pages (`docs/patterns/<name>.md`)

One page per pattern: fit notes, variant comparison, links to the boilerplate directories, cross-references to related patterns.

---

## ADR Set

All ADRs use the standard format: Status / Date / Context / Decision / Consequences / Alternatives Considered. Each ADR includes a **"when this choice stops being correct"** section.

| ADR | Title |
|---|---|
| ADR-0001 | Pattern matrix repo structure — directory + branch dual access |
| ADR-0002 | Idiomatic language per ecosystem |
| ADR-0003 | Table format landscape — Iceberg vs Delta Lake vs Hudi |
| ADR-0004 | Stream processing engine — Flink vs Spark Structured Streaming |
| ADR-0005 | Orchestration landscape — Airflow vs Prefect vs Dagster vs Temporal |
| ADR-0006 | Lakehouse storage layer — open table formats over managed warehouses |
| ADR-0007 | CDC tooling — Debezium primary, Airbyte as managed alternative |
| ADR-0008 | Local dev approach — Docker Compose per variant |
| ADR-0009 | Graph processing — when graph is the right model |
| ADR-0010 | ML feature pipeline — online vs offline store split |

---

## Branch Naming and Dual Access

**Main branch** (`main`) — full repo, all patterns and docs.

**Pattern branches** — one per variant, contains only that variant's files at the repo root:

```
pattern/batch-lakehouse/spark-iceberg
pattern/batch-lakehouse/spark-delta
pattern/batch-lakehouse/spark-iceberg-airflow
pattern/streaming-lakehouse/flink-iceberg
pattern/streaming-lakehouse/spark-delta
pattern/elt-warehouse/dbt-snowflake
pattern/elt-warehouse/dbt-bigquery
pattern/elt-warehouse/dbt-duckdb-airflow
pattern/cdc-pipeline/debezium-kafka-flink
pattern/cdc-pipeline/debezium-kafka-spark
pattern/lambda-architecture/flink-spark-iceberg
pattern/lambda-architecture/spark-streaming-batch-delta
pattern/federated-query/trino-iceberg-s3
pattern/federated-query/presto-hive
pattern/ml-feature-pipeline/feast-redis-spark
pattern/ml-feature-pipeline/flink-spark-offline
pattern/ml-feature-pipeline/feast-spark-airflow
pattern/graph-processing/spark-graphx
pattern/graph-processing/neo4j-spark
pattern/workflow-orchestration/airflow
pattern/workflow-orchestration/prefect
pattern/workflow-orchestration/dagster
```

**`tooling/sync-pattern-branches.sh`** — reads the `patterns/` tree, for each variant creates/resets the branch with only that variant's files at root, commits, and pushes. Run manually after any variant is added or updated in `main`.

---

## CI Workflows

| Workflow | Trigger | What it does |
|---|---|---|
| `deploy-docs.yml` | push to `main` | Builds MkDocs site, deploys to GitHub Pages |
| `adr-lint.yml` | push to `main` | Checks all ADRs have required sections |
| `validate-variants.yml` | push to `main` | Checks each variant has `README.md`, `docker-compose.yml`, `.env.example` |
