# Data Engineering Patterns — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `chakraview-data-engineering-patterns` — 9 data engineering patterns × 22 idiomatic boilerplate stack variants, with MkDocs docs site, 10 ADRs, landscape intro, decision matrix, and a branch-per-variant dual access model.

**Architecture:** `patterns/<name>/variants/<stack>/` directories in `main` for full-repo browsing. `pattern/<name>/<stack>` branches contain only that variant at the repo root for `git clone --branch` use. MkDocs Material docs site deployed to GitHub Pages covers the full modern data landscape and a two-table decision matrix.

**Tech Stack:** Scala 2.12 + SBT (Spark 3.5, Iceberg 1.5, Delta 3.1), Java 17 + Maven (Flink 1.19), Python 3.11 + pyproject.toml (Airflow 2.9, Prefect 2.x, Dagster 1.x, Feast 0.40), SQL (dbt 1.8, Trino 450, Presto 0.287), MkDocs Material 9.5, GitHub Actions.

---

## File Map

```
.gitignore
README.md
mkdocs.yml
requirements-docs.txt
docs/assets/logo.svg
docs/index.md
docs/landscape/index.md
docs/decision-matrix/index.md
docs/patterns/batch-lakehouse.md
docs/patterns/streaming-lakehouse.md
docs/patterns/elt-warehouse.md
docs/patterns/cdc-pipeline.md
docs/patterns/lambda-architecture.md
docs/patterns/federated-query.md
docs/patterns/ml-feature-pipeline.md
docs/patterns/graph-processing.md
docs/patterns/workflow-orchestration.md
docs/adrs/README.md
docs/adrs/ADR-0001-pattern-matrix-structure.md
docs/adrs/ADR-0002-idiomatic-language-per-ecosystem.md
docs/adrs/ADR-0003-table-format-landscape.md
docs/adrs/ADR-0004-stream-processing-engine.md
docs/adrs/ADR-0005-orchestration-landscape.md
docs/adrs/ADR-0006-lakehouse-storage-layer.md
docs/adrs/ADR-0007-cdc-tooling.md
docs/adrs/ADR-0008-local-dev-approach.md
docs/adrs/ADR-0009-graph-processing.md
docs/adrs/ADR-0010-ml-feature-pipeline.md
patterns/batch-lakehouse/README.md
patterns/batch-lakehouse/variants/spark-iceberg/{README.md,build.sbt,src/,config/,docker-compose.yml,.env.example}
patterns/batch-lakehouse/variants/spark-delta/{README.md,build.sbt,src/,config/,docker-compose.yml,.env.example}
patterns/batch-lakehouse/variants/spark-iceberg-airflow/{README.md,build.sbt,src/,config/,docker-compose.yml,.env.example}
patterns/streaming-lakehouse/README.md
patterns/streaming-lakehouse/variants/flink-iceberg/{README.md,pom.xml,src/,config/,docker-compose.yml,.env.example}
patterns/streaming-lakehouse/variants/spark-delta/{README.md,build.sbt,src/,config/,docker-compose.yml,.env.example}
patterns/elt-warehouse/README.md
patterns/elt-warehouse/variants/dbt-snowflake/{README.md,dbt_project.yml,profiles.yml,packages.yml,models/,.env.example}
patterns/elt-warehouse/variants/dbt-bigquery/{README.md,dbt_project.yml,profiles.yml,packages.yml,models/,.env.example}
patterns/elt-warehouse/variants/dbt-duckdb-airflow/{README.md,dbt_project.yml,profiles.yml,packages.yml,models/,dags/,docker-compose.yml,.env.example}
patterns/cdc-pipeline/README.md
patterns/cdc-pipeline/variants/debezium-kafka-flink/{README.md,pom.xml,src/,config/,docker-compose.yml,.env.example}
patterns/cdc-pipeline/variants/debezium-kafka-spark/{README.md,build.sbt,src/,config/,docker-compose.yml,.env.example}
patterns/lambda-architecture/README.md
patterns/lambda-architecture/variants/flink-spark-iceberg/{README.md,streaming/,batch/,config/,docker-compose.yml,.env.example}
patterns/lambda-architecture/variants/spark-streaming-batch-delta/{README.md,streaming/,batch/,config/,docker-compose.yml,.env.example}
patterns/federated-query/README.md
patterns/federated-query/variants/trino-iceberg-s3/{README.md,config/,queries/,docker-compose.yml,.env.example}
patterns/federated-query/variants/presto-hive/{README.md,config/,queries/,docker-compose.yml,.env.example}
patterns/ml-feature-pipeline/README.md
patterns/ml-feature-pipeline/variants/feast-redis-spark/{README.md,pyproject.toml,feature_store.yaml,features/,pipeline/,docker-compose.yml,.env.example}
patterns/ml-feature-pipeline/variants/flink-spark-offline/{README.md,pyproject.toml,online/,offline/,docker-compose.yml,.env.example}
patterns/ml-feature-pipeline/variants/feast-spark-airflow/{README.md,pyproject.toml,feature_store.yaml,features/,dags/,docker-compose.yml,.env.example}
patterns/graph-processing/README.md
patterns/graph-processing/variants/spark-graphx/{README.md,build.sbt,src/,config/,docker-compose.yml,.env.example}
patterns/graph-processing/variants/neo4j-spark/{README.md,build.sbt,src/,config/,docker-compose.yml,.env.example}
patterns/workflow-orchestration/README.md
patterns/workflow-orchestration/variants/airflow/{README.md,pyproject.toml,dags/,config/,docker-compose.yml,.env.example}
patterns/workflow-orchestration/variants/prefect/{README.md,pyproject.toml,flows/,config/,docker-compose.yml,.env.example}
patterns/workflow-orchestration/variants/dagster/{README.md,pyproject.toml,pipeline/,config/,docker-compose.yml,.env.example}
.github/workflows/deploy-docs.yml
.github/workflows/adr-lint.yml
.github/workflows/validate-variants.yml
tooling/sync-pattern-branches.sh
```

---

## Tasks 1–5

---

### Task 1: Repo Scaffold

**Files:**
- Create: `README.md`
- Create: `requirements-docs.txt`
- Create: `mkdocs.yml`
- Create: `.gitignore`
- Create: `docs/assets/logo.svg`

- [ ] **Step 1: Create `.gitignore`**

```
# Scala / SBT
target/
.bsp/
.metals/
.idea/
*.class

# Java / Maven
**/target/

# Python
__pycache__/
*.pyc
.venv/
dist/
*.egg-info/
.pytest_cache/

# dbt
dbt_packages/
dbt_project/target/
logs/

# MkDocs
site/

# Environment
.env
*.env

# OS
.DS_Store
```

- [ ] **Step 2: Create `requirements-docs.txt`**

```
mkdocs-material>=9.5
```

- [ ] **Step 3: Create `mkdocs.yml`**

```yaml
site_name: Chakra Data Engineering Patterns
site_description: >
  Idiomatic boilerplate starters for 9 data engineering patterns × 22 stack
  variants. Landscape intro, decision matrix, and ADRs for every trade-off.
site_url: https://naren-chakraview.github.io/chakraview-data-engineering-patterns/
repo_name: naren-chakraview/chakraview-data-engineering-patterns
repo_url: https://github.com/naren-chakraview/chakraview-data-engineering-patterns
edit_uri: edit/main/docs/

theme:
  name: material
  palette:
    - media: "(prefers-color-scheme: dark)"
      scheme: slate
      primary: teal
      accent: amber
      toggle:
        icon: material/weather-sunny
        name: Switch to light mode
    - media: "(prefers-color-scheme: light)"
      scheme: default
      primary: teal
      accent: amber
      toggle:
        icon: material/weather-night
        name: Switch to dark mode
  font:
    text: Inter
    code: JetBrains Mono
  logo: assets/logo.svg
  favicon: assets/logo.svg
  icon:
    repo: fontawesome/brands/github
  features:
    - navigation.tabs
    - navigation.tabs.sticky
    - navigation.sections
    - navigation.indexes
    - navigation.top
    - toc.follow
    - search.suggest
    - search.highlight
    - content.code.annotate
    - content.code.copy
    - content.tabs.link

markdown_extensions:
  - admonition
  - attr_list
  - def_list
  - footnotes
  - md_in_html
  - tables
  - toc:
      permalink: true
      title: On this page
  - pymdownx.details
  - pymdownx.emoji:
      emoji_index: !!python/name:material.extensions.emoji.twemoji
      emoji_generator: !!python/name:material.extensions.emoji.to_svg
  - pymdownx.highlight:
      anchor_linenums: true
      line_spans: __span
      pygments_lang_class: true
  - pymdownx.inlinehilite
  - pymdownx.snippets
  - pymdownx.superfences:
      custom_fences:
        - name: mermaid
          class: mermaid
          format: !!python/name:pymdownx.superfences.fence_code_format
  - pymdownx.tabbed:
      alternate_style: true
  - pymdownx.tasklist:
      custom_checkbox: true

plugins:
  - search:
      lang: en
  - tags

extra:
  social:
    - icon: fontawesome/brands/github
      link: https://github.com/naren-chakraview
  generator: false

nav:
  - Home: index.md
  - Landscape: landscape/index.md
  - Decision Matrix: decision-matrix/index.md
  - Patterns:
    - Batch Lakehouse: patterns/batch-lakehouse.md
    - Streaming Lakehouse: patterns/streaming-lakehouse.md
    - ELT / Warehouse: patterns/elt-warehouse.md
    - CDC Pipeline: patterns/cdc-pipeline.md
    - Lambda Architecture: patterns/lambda-architecture.md
    - Federated Query: patterns/federated-query.md
    - ML Feature Pipeline: patterns/ml-feature-pipeline.md
    - Graph Processing: patterns/graph-processing.md
    - Workflow Orchestration: patterns/workflow-orchestration.md
  - ADRs:
    - adrs/README.md
    - ADR-0001 Pattern Matrix: adrs/ADR-0001-pattern-matrix-structure.md
    - ADR-0002 Language Convention: adrs/ADR-0002-idiomatic-language-per-ecosystem.md
    - ADR-0003 Table Formats: adrs/ADR-0003-table-format-landscape.md
    - ADR-0004 Stream Processing: adrs/ADR-0004-stream-processing-engine.md
    - ADR-0005 Orchestration: adrs/ADR-0005-orchestration-landscape.md
    - ADR-0006 Lakehouse Storage: adrs/ADR-0006-lakehouse-storage-layer.md
    - ADR-0007 CDC Tooling: adrs/ADR-0007-cdc-tooling.md
    - ADR-0008 Local Dev: adrs/ADR-0008-local-dev-approach.md
    - ADR-0009 Graph Processing: adrs/ADR-0009-graph-processing.md
    - ADR-0010 ML Feature Pipeline: adrs/ADR-0010-ml-feature-pipeline.md
```

- [ ] **Step 4: Create `docs/assets/logo.svg`** — 3×3 grid suggesting pattern × stack matrix

```svg
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 48 48" width="48" height="48">
  <rect x="4"  y="4"  width="12" height="12" rx="2" fill="#0d9488"/>
  <rect x="18" y="4"  width="12" height="12" rx="2" fill="#0d9488" opacity="0.7"/>
  <rect x="32" y="4"  width="12" height="12" rx="2" fill="#0d9488" opacity="0.4"/>
  <rect x="4"  y="18" width="12" height="12" rx="2" fill="#0d9488" opacity="0.7"/>
  <rect x="18" y="18" width="12" height="12" rx="2" fill="#0d9488"/>
  <rect x="32" y="18" width="12" height="12" rx="2" fill="#0d9488" opacity="0.7"/>
  <rect x="4"  y="32" width="12" height="12" rx="2" fill="#0d9488" opacity="0.4"/>
  <rect x="18" y="32" width="12" height="12" rx="2" fill="#0d9488" opacity="0.7"/>
  <rect x="32" y="32" width="12" height="12" rx="2" fill="#0d9488"/>
</svg>
```

- [ ] **Step 5: Create `README.md`**

```markdown
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
```

- [ ] **Step 6: Commit**

```bash
git add .gitignore requirements-docs.txt mkdocs.yml README.md docs/assets/logo.svg
git commit -m "feat: repo scaffold — mkdocs, readme, logo, gitignore"
```

---

### Task 2: Docs Site Skeleton

**Files:**
- Create: `docs/index.md`
- Create: `docs/adrs/README.md`
- Create: `docs/landscape/index.md` (stub — full content in Task 3)
- Create: `docs/decision-matrix/index.md` (stub — full content in Task 4)
- Create: `docs/patterns/batch-lakehouse.md` through `docs/patterns/workflow-orchestration.md` (stubs — full content in Task 10)

- [ ] **Step 1: Create `docs/index.md`**

```markdown
---
title: Chakra Data Engineering Patterns
description: Boilerplate starters for 9 data engineering patterns × 22 stack variants.
tags: [overview]
---

# Data Engineering Patterns

> Pick a pattern. Pick a stack. Pull the branch. Start building.

---

## What this is

Nine opinionated, production-informed reference implementations of common data engineering patterns — each available in 2–3 idiomatic stack variants. Every variant is a self-contained boilerplate starter: real code, a local dev stack, and a README that explains when to use it and when not to.

---

## Start here

<div class="grid cards" markdown>

-   :material-map:{ .lg .middle } __Landscape__

    ---

    Where each pattern fits in the modern data engineering landscape. Read this first if you are choosing a pattern.

    [:octicons-arrow-right-24: Landscape](landscape/index.md)

-   :material-table-large:{ .lg .middle } __Decision Matrix__

    ---

    Two tables: pick a pattern by use-case characteristics, then pick a stack variant by trade-off criteria.

    [:octicons-arrow-right-24: Decision Matrix](decision-matrix/index.md)

</div>

---

## Patterns

<div class="grid cards" markdown>

-   :material-database-export:{ .lg .middle } __Batch Lakehouse__

    ---

    Scheduled Spark jobs writing to open table formats. Highest throughput, simplest operations, no sub-minute latency.

    [:octicons-arrow-right-24: Batch Lakehouse](patterns/batch-lakehouse.md)

-   :material-database-arrow-right:{ .lg .middle } __Streaming Lakehouse__

    ---

    Continuous Flink or Spark Streaming pipelines writing to Iceberg or Delta. Sub-minute latency with lakehouse query semantics.

    [:octicons-arrow-right-24: Streaming Lakehouse](patterns/streaming-lakehouse.md)

-   :material-transfer-right:{ .lg .middle } __ELT / Warehouse__

    ---

    dbt transforms data already loaded into a warehouse. Right when your warehouse is the system of record.

    [:octicons-arrow-right-24: ELT / Warehouse](patterns/elt-warehouse.md)

-   :material-database-sync:{ .lg .middle } __CDC Pipeline__

    ---

    Debezium captures database writes at the WAL level and streams them into a processing engine. Zero polling, sub-second capture.

    [:octicons-arrow-right-24: CDC Pipeline](patterns/cdc-pipeline.md)

-   :material-lambda:{ .lg .middle } __Lambda Architecture__

    ---

    Parallel streaming (low latency) and batch (high accuracy) paths merged at query time. Use only when both are genuinely required.

    [:octicons-arrow-right-24: Lambda Architecture](patterns/lambda-architecture.md)

-   :material-connection:{ .lg .middle } __Federated Query__

    ---

    Trino or Presto queries across multiple storage systems without data movement. Right for polyglot data estates.

    [:octicons-arrow-right-24: Federated Query](patterns/federated-query.md)

-   :material-brain:{ .lg .middle } __ML Feature Pipeline__

    ---

    Compute and serve ML features from a feature store. Separates feature engineering from model training and serving.

    [:octicons-arrow-right-24: ML Feature Pipeline](patterns/ml-feature-pipeline.md)

-   :material-graph:{ .lg .middle } __Graph Processing__

    ---

    Spark GraphX or Neo4j+Spark for graph-native algorithms. Right when relationships are the query, not the filter.

    [:octicons-arrow-right-24: Graph Processing](patterns/graph-processing.md)

-   :material-calendar-clock:{ .lg .middle } __Workflow Orchestration__

    ---

    Airflow, Prefect, or Dagster for scheduling and coordinating data pipelines. The glue between every other pattern.

    [:octicons-arrow-right-24: Workflow Orchestration](patterns/workflow-orchestration.md)

</div>
```

- [ ] **Step 2: Create `docs/adrs/README.md`**

```markdown
# Architecture Decision Records

Ten ADRs covering the cross-cutting choices that apply across all patterns and stacks in this repo.

| ADR | Title | Status |
|---|---|---|
| [ADR-0001](ADR-0001-pattern-matrix-structure.md) | Pattern matrix repo structure | Accepted |
| [ADR-0002](ADR-0002-idiomatic-language-per-ecosystem.md) | Idiomatic language per ecosystem | Accepted |
| [ADR-0003](ADR-0003-table-format-landscape.md) | Table format landscape — Iceberg vs Delta vs Hudi | Accepted |
| [ADR-0004](ADR-0004-stream-processing-engine.md) | Stream processing engine — Flink vs Spark Structured Streaming | Accepted |
| [ADR-0005](ADR-0005-orchestration-landscape.md) | Orchestration landscape — Airflow vs Prefect vs Dagster vs Temporal | Accepted |
| [ADR-0006](ADR-0006-lakehouse-storage-layer.md) | Lakehouse storage layer — open formats over managed warehouses | Accepted |
| [ADR-0007](ADR-0007-cdc-tooling.md) | CDC tooling — Debezium primary, Airbyte as managed alternative | Accepted |
| [ADR-0008](ADR-0008-local-dev-approach.md) | Local dev approach — Docker Compose per variant | Accepted |
| [ADR-0009](ADR-0009-graph-processing.md) | Graph processing — when graph is the right model | Accepted |
| [ADR-0010](ADR-0010-ml-feature-pipeline.md) | ML feature pipeline — online vs offline store split | Accepted |
```

- [ ] **Step 3: Create stub pages for landscape, decision-matrix, and all 9 pattern docs**

`docs/landscape/index.md`:
```markdown
# Modern Data Processing Landscape

*Full content coming in Task 3.*
```

`docs/decision-matrix/index.md`:
```markdown
# Decision Matrix

*Full content coming in Task 4.*
```

For each of the nine pattern pages (`docs/patterns/batch-lakehouse.md`, `docs/patterns/streaming-lakehouse.md`, `docs/patterns/elt-warehouse.md`, `docs/patterns/cdc-pipeline.md`, `docs/patterns/lambda-architecture.md`, `docs/patterns/federated-query.md`, `docs/patterns/ml-feature-pipeline.md`, `docs/patterns/graph-processing.md`, `docs/patterns/workflow-orchestration.md`), create a stub:

```markdown
# <Pattern Name>

*Full content coming in Task 10.*
```

- [ ] **Step 4: Verify MkDocs builds without errors**

```bash
cd /path/to/chakraview-data-engineering-patterns
pip install -r requirements-docs.txt
mkdocs build --strict 2>&1 | tail -20
```

Expected: `INFO - Documentation built successfully.` (warnings about stub content are acceptable; errors are not)

- [ ] **Step 5: Commit**

```bash
git add docs/
git commit -m "feat: docs site skeleton — home page, stubs for all nav pages"
```

---

### Task 3: Landscape Intro

**Files:**
- Overwrite: `docs/landscape/index.md`

- [ ] **Step 1: Write `docs/landscape/index.md`**

```markdown
---
title: Modern Data Processing Landscape
description: >
  Where each pattern fits — batch vs streaming, open lakehouse vs managed warehouse,
  orchestration choices, and cloud-native trade-offs.
tags: [landscape, architecture]
---

# Modern Data Processing Landscape

Understanding which pattern and stack to reach for requires understanding the landscape they emerged from — what problem each generation of tooling solved, and what it left unsolved.

---

## The evolution

### 2006–2014 — Hadoop era

Hadoop MapReduce solved one problem: running computations on datasets that were too large for a single machine. It did so at great cost — MapReduce programs were verbose Java, every intermediate result was written to HDFS, and a job that ran in minutes on a single machine could take hours distributed across a cluster due to shuffling and serialisation overhead.

HDFS solved the storage problem; MapReduce did not solve the computation problem well. Hive (2008) added SQL over MapReduce; Pig added a scripting layer. Both reduced verbosity while inheriting MapReduce's latency characteristics.

**What Hadoop got right:** horizontal scale for batch workloads over commodity hardware.  
**What it got wrong:** latency, developer ergonomics, and the assumption that disk I/O was cheap.

### 2014–2018 — Spark era

Apache Spark replaced MapReduce's disk-first execution model with in-memory DAG execution. A Spark job that would have taken 10× longer in MapReduce ran in roughly the same wall-clock time as a single-machine Pandas job on data that fit in memory — and could scale to data that didn't.

Spark also unified batch and streaming under one API (Spark Streaming, later Structured Streaming), introduced DataFrames as a SQL-compatible abstraction, and made machine learning accessible via MLlib.

**What Spark got right:** in-memory execution, unified batch+streaming API, broad ecosystem.  
**What it left open:** streaming latency (micro-batches, not true event-time), the "small files problem" on data lakes, and table-level ACID semantics.

### 2019–2022 — Streaming-first and the lakehouse

Two parallel developments reshaped the stack:

**Apache Flink** matured into the dominant true-streaming engine. Unlike Spark's micro-batch model, Flink processes each event as it arrives, with native event-time semantics, exactly-once checkpointing via RocksDB, and backpressure-aware execution. For sub-minute latency requirements, Flink became the default choice.

**Open table formats** (Apache Iceberg, Delta Lake, Apache Hudi) solved the data lake's ACID problem. They added transactional commits, schema evolution, time travel, and compaction on top of object storage (S3, GCS, ADLS) — delivering warehouse-quality semantics without locking data into a proprietary format. This combination became the **lakehouse**: the query semantics of a warehouse on the economics of object storage.

### 2022–present — Data mesh and platform consolidation

The organisational layer caught up with the technical layer. Data mesh introduced **data product ownership**: domain teams own not just their services but the data products those services produce, including schemas, freshness SLAs, and quality guarantees.

Technically, the landscape consolidated around three axes:

1. **Processing engine**: Flink (streaming) or Spark (batch/streaming)
2. **Storage format**: Iceberg (most portable), Delta (Databricks-native, strong Spark integration), Hudi (upsert-optimised)
3. **Orchestration**: Airflow (mature, broad operator ecosystem), Prefect (Python-native, modern UX), Dagster (asset-centric, strong observability)

---

## Batch vs streaming: the core decision

The most consequential choice in any data architecture is whether the processing path is batch or streaming. It is often made wrong — teams choose streaming because it sounds modern, not because their latency requirement justifies its operational cost.

| Factor | Lean batch | Lean streaming |
|---|---|---|
| **Latency requirement** | > 15 minutes acceptable | < 5 minutes required |
| **Reprocessing** | Full reprocessing is routine | Must replay log, not re-extract source |
| **Source system** | Files dropped to S3/GCS, scheduled DB exports | Database WAL (via CDC), Kafka topics |
| **State complexity** | Low — mostly aggregations | High — joins, windowed aggregations across keys |
| **Team maturity** | Spark SQL familiar | Flink operational experience required |
| **Cost** | Lower — compute only runs during job | Higher — continuous cluster + Kafka retention |

**Rule of thumb:** if a stakeholder can wait 15 minutes for fresh data, batch is almost always the right answer. Streaming's operational overhead (Kafka, Flink state, exactly-once configuration, watermark tuning) is justified only when it is not.

---

## The table format layer: Iceberg vs Delta vs Hudi

All three open table formats solve the same core problem: ACID transactions and schema evolution on object storage. Their differences matter at the edges.

| | **Apache Iceberg** | **Delta Lake** | **Apache Hudi** |
|---|---|---|---|
| **Governance** | Apache Foundation | Linux Foundation (originally Databricks) | Apache Foundation |
| **Primary strength** | Portability — any engine reads/writes | Spark integration depth | Upsert performance (record-level merge) |
| **Engine support** | Spark, Flink, Trino, Presto, Dremio, StarRocks | Spark (first-class), Flink (good), others (limited) | Spark, Flink (partial) |
| **Schema evolution** | Full — column add/drop/rename/reorder | Full — column add/drop | Partial |
| **Time travel** | Snapshot-based, any engine | Log-based, Spark-native | Commit timeline |
| **Best fit** | Multi-engine environments, open interoperability | Databricks-heavy stacks | CDC upsert workloads with high update rate |
| **When NOT to use** | Databricks-only shop (Delta is simpler there) | Multi-engine queries with Trino/Presto | When Spark isn't your engine |

**Default choice:** Iceberg. It is the most portable and has the broadest engine support. Switch to Delta if your team lives in Databricks and values tighter integration over portability. Consider Hudi only for workloads where record-level upserts dominate (e.g., a CDC pipeline with a very high update rate on a wide table).

---

## ELT and the warehouse

Not all data architectures need a processing engine. When the source data lands in a warehouse (Snowflake, BigQuery, Redshift, DuckDB) and the transformation logic is SQL, **dbt** is the right tool — not Spark, not Flink.

dbt transforms data that is already in the warehouse using SQL models, materialisation strategies (view, table, incremental), tests, and documentation. It does not move data; it transforms data in place.

Use ELT / dbt when:
- Your data is already in a cloud warehouse and that warehouse is the system of record
- Your team writes SQL fluently but does not need distributed compute (the warehouse handles scale)
- Your latency requirement is minutes to hours, not seconds
- You want lineage, testing, and documentation built into your transformation layer

Do **not** use ELT / dbt as a substitute for a streaming pipeline. dbt models run on a schedule; they cannot process events continuously.

---

## Orchestration: Airflow vs Prefect vs Dagster vs Temporal

All four orchestrators schedule and coordinate tasks. They differ in philosophy, abstraction layer, and operational model.

| | **Airflow** | **Prefect** | **Dagster** | **Temporal** |
|---|---|---|---|---|
| **Abstraction** | DAG of operators | Python-native flows and tasks | Software-defined assets | Workflow as code (durable execution) |
| **Primary strength** | Ecosystem breadth (1000+ providers) | Python ergonomics, dynamic tasks | Asset lineage and observability | Long-running, durable workflows |
| **Scheduler model** | Cron-based DAG runs | Event-triggered or scheduled flows | Asset materialisation schedules | Activity-based durable execution |
| **UI** | Mature, dag-centric | Modern, flow-centric | Asset-centric with lineage graph | Workflow history and retry UI |
| **Operational complexity** | High (scheduler, workers, metadata DB, webserver) | Medium (Prefect server or Prefect Cloud) | Medium (webserver + daemon) | High (Temporal server cluster) |
| **Best fit** | Large orgs with many integrations, existing Airflow investment | Python-first teams, dynamic pipelines | Data-product teams prioritising asset observability | Microservice workflows, long-running business processes |
| **When NOT to use** | New greenfield with no legacy Airflow | When you need 1000+ pre-built operators | Pure ETL with no asset-centric thinking | Data pipelines (overkill for batch/streaming data) |

**Default choice for data pipelines:** Airflow (ecosystem maturity) or Dagster (asset observability). Prefect is the right choice when your team is Python-first and wants modern ergonomics without Airflow's scheduler complexity. Temporal is not a data pipeline orchestrator — it is a durable workflow engine for microservice coordination.

---

## Cloud-native vs open-source

Every pattern in this repo is built on open-source tooling backed by object storage. This is a deliberate choice with a known trade-off.

**Open-source + object storage:**
- No vendor lock-in on the storage layer — data in Iceberg/Delta/Parquet on S3 is readable by any engine
- Higher operational overhead — you manage the Flink cluster, Kafka brokers, Trino coordinators
- Lower cost at scale — object storage is 10–100× cheaper than managed warehouse storage

**Managed cloud services (Databricks, Snowflake, BigQuery, Redshift):**
- Lower operational overhead — vendor manages the compute layer
- Higher cost at scale — per-query or per-slot pricing compounds quickly
- Vendor lock-in on proprietary features (Snowpark, Databricks notebooks, etc.)

**Guidance:** use managed services when your team's time is more expensive than your compute bill. Use open-source when you are optimising for cost at scale or need portability across clouds. Most mature data platforms start managed and migrate selectively as scale drives cost.

---

## How to use the decision matrix

The [Decision Matrix](../decision-matrix/index.md) has two tables:

1. **Pattern selection** — given your use-case characteristics (latency, reprocessing need, data volume, source system type), which pattern fits?
2. **Stack selection** — given you've chosen a pattern, which stack variant fits your team's skills, cloud environment, and operational tolerance?

Read across the row for your most binding constraint. Latency is usually the first filter: if you need sub-minute freshness, batch lakehouse is off the table regardless of other factors.
```

- [ ] **Step 2: Verify MkDocs builds**

```bash
mkdocs build --strict 2>&1 | grep -E "(ERROR|WARNING|built successfully)"
```

Expected: `INFO - Documentation built successfully.`

- [ ] **Step 3: Commit**

```bash
git add docs/landscape/index.md
git commit -m "docs: modern data processing landscape intro"
```

---

### Task 4: Decision Matrix

**Files:**
- Overwrite: `docs/decision-matrix/index.md`

- [ ] **Step 1: Write `docs/decision-matrix/index.md`**

```markdown
---
title: Decision Matrix
description: >
  Two tables: choose a pattern by use-case characteristics, then choose a stack
  variant by operational criteria.
tags: [decision-matrix]
---

# Decision Matrix

## Table 1 — Choose a pattern

Read across the row for your most binding constraint. ✅ = strong fit · ⚠️ = possible but not ideal · ❌ = wrong tool.

| Characteristic | Batch Lakehouse | Streaming Lakehouse | ELT / Warehouse | CDC Pipeline | Lambda Arch | Federated Query | ML Feature Pipeline | Graph Processing | Workflow Orchestration |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **Latency: > 15 min acceptable** | ✅ | ⚠️ | ✅ | ⚠️ | ⚠️ | ✅ | ⚠️ | ✅ | ✅ |
| **Latency: 1–15 min required** | ⚠️ | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ | ❌ | ⚠️ |
| **Latency: < 1 min required** | ❌ | ✅ | ❌ | ✅ | ⚠️ | ❌ | ✅ | ❌ | ❌ |
| **Source: files / scheduled DB export** | ✅ | ❌ | ✅ | ❌ | ⚠️ | ✅ | ⚠️ | ✅ | ✅ |
| **Source: live database WAL** | ⚠️ | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ |
| **Source: Kafka / event stream** | ⚠️ | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ |
| **Source: data already in warehouse** | ❌ | ❌ | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ | ⚠️ |
| **Reprocessing: replay log from offset** | ⚠️ | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ |
| **Reprocessing: re-extract from source** | ✅ | ❌ | ✅ | ❌ | ✅ | ✅ | ⚠️ | ✅ | ✅ |
| **Query pattern: SQL aggregations** | ✅ | ✅ | ✅ | ⚠️ | ✅ | ✅ | ❌ | ❌ | ❌ |
| **Query pattern: graph traversal** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ |
| **Query pattern: feature lookup (low-latency)** | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ❌ |
| **Query pattern: cross-system federation** | ❌ | ❌ | ⚠️ | ❌ | ❌ | ✅ | ❌ | ❌ | ❌ |
| **Team: SQL-fluent, no distributed compute** | ❌ | ❌ | ✅ | ❌ | ❌ | ✅ | ❌ | ❌ | ⚠️ |
| **Team: Spark/Scala experience** | ✅ | ✅ | ⚠️ | ✅ | ✅ | ⚠️ | ✅ | ✅ | ⚠️ |
| **Team: Flink/Java experience** | ❌ | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ |
| **Cost sensitivity: minimise always-on** | ✅ | ❌ | ✅ | ❌ | ❌ | ⚠️ | ⚠️ | ✅ | ✅ |
| **Needs pipeline scheduling / coordination** | ✅ | ⚠️ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ✅ |

---

## Table 2 — Choose a stack variant

### Batch Lakehouse

| Variant | Operational complexity | Cloud portability | Ecosystem maturity | Cost profile | Best when |
|---|:---:|:---:|:---:|:---:|---|
| **Spark + Iceberg** | Low | High (any engine reads Iceberg) | High | Low | Multi-engine shop, need Trino/Presto reads |
| **Spark + Delta** | Low | Medium (Delta best with Spark/Databricks) | High | Low | Databricks-first, want tighter Spark integration |
| **Spark + Iceberg + Airflow** | Medium | High | High | Low | Need scheduled orchestration with retry/alerting |

### Streaming Lakehouse

| Variant | Operational complexity | Cloud portability | Ecosystem maturity | Cost profile | Best when |
|---|:---:|:---:|:---:|:---:|---|
| **Flink + Iceberg** | High | High | High | Medium | Sub-minute latency, exactly-once required |
| **Spark Structured Streaming + Delta** | Medium | Medium | High | Medium | Team already knows Spark, latency > 30s acceptable |

### ELT / Warehouse

| Variant | Operational complexity | Cloud portability | Ecosystem maturity | Cost profile | Best when |
|---|:---:|:---:|:---:|:---:|---|
| **dbt + Snowflake** | Low | Low (Snowflake-specific) | High | High | Snowflake is already your warehouse |
| **dbt + BigQuery** | Low | Low (GCP-specific) | High | Medium | GCP-first org, BigQuery already in use |
| **dbt + DuckDB + Airflow** | Medium | High | Medium | Very low | Local/dev, cost-sensitive, small-to-medium data |

### CDC Pipeline

| Variant | Operational complexity | Cloud portability | Ecosystem maturity | Cost profile | Best when |
|---|:---:|:---:|:---:|:---:|---|
| **Debezium + Kafka + Flink** | High | High | High | Medium | Need exactly-once, complex stream processing |
| **Debezium + Kafka + Spark** | Medium | High | High | Medium | Team knows Spark, latency > 30s acceptable |

### Lambda Architecture

| Variant | Operational complexity | Cloud portability | Ecosystem maturity | Cost profile | Best when |
|---|:---:|:---:|:---:|:---:|---|
| **Flink (streaming) + Spark (batch) + Iceberg** | Very High | High | High | High | Both exactly-once streaming AND batch correctness required |
| **Spark Streaming + Spark Batch + Delta** | High | Medium | High | High | Single Spark team, Delta as unifying format |

### Federated Query

| Variant | Operational complexity | Cloud portability | Ecosystem maturity | Cost profile | Best when |
|---|:---:|:---:|:---:|:---:|---|
| **Trino + Iceberg + S3** | Medium | High | High | Low | Iceberg as primary format, need Presto-compatible queries |
| **Presto + Hive metastore** | Medium | High | High | Low | Existing Hive metastore, legacy Hadoop estate |

### ML Feature Pipeline

| Variant | Operational complexity | Cloud portability | Ecosystem maturity | Cost profile | Best when |
|---|:---:|:---:|:---:|:---:|---|
| **Feast + Redis + Spark** | Medium | High | High | Medium | Standard online+offline split, Redis for low-latency serving |
| **Flink + Spark offline** | High | High | High | High | Online features need sub-second freshness |
| **Feast + Spark + Airflow** | Medium | High | High | Medium | Need scheduled feature computation with orchestration |

### Graph Processing

| Variant | Operational complexity | Cloud portability | Ecosystem maturity | Cost profile | Best when |
|---|:---:|:---:|:---:|:---:|---|
| **Spark GraphX** | Low | High | Medium | Low | Batch graph algorithms (PageRank, connected components) on large graphs |
| **Neo4j + Spark** | Medium | Medium | High | Medium | Need Cypher query language, real-time graph traversal |

### Workflow Orchestration

| Variant | Operational complexity | Cloud portability | Ecosystem maturity | Cost profile | Best when |
|---|:---:|:---:|:---:|:---:|---|
| **Airflow** | High | High | Very High | Low | Large org, many integrations, existing Airflow investment |
| **Prefect** | Low | High | High | Low | Python-first team, dynamic task generation, modern UX |
| **Dagster** | Medium | High | High | Low | Asset-centric thinking, strong observability requirements |
```

- [ ] **Step 2: Verify MkDocs builds**

```bash
mkdocs build --strict 2>&1 | grep -E "(ERROR|WARNING|built successfully)"
```

Expected: `INFO - Documentation built successfully.`

- [ ] **Step 3: Commit**

```bash
git add docs/decision-matrix/index.md
git commit -m "docs: decision matrix — pattern selection and stack selection tables"
```

---

### Task 5: ADRs 0001 and 0002

**Files:**
- Create: `docs/adrs/ADR-0001-pattern-matrix-structure.md`
- Create: `docs/adrs/ADR-0002-idiomatic-language-per-ecosystem.md`

- [ ] **Step 1: Write `docs/adrs/ADR-0001-pattern-matrix-structure.md`**

```markdown
# ADR-0001: Pattern Matrix Repo Structure — Directory + Branch Dual Access

**Status**: Accepted
**Date**: 2026-04-25
**Deciders**: Portfolio architect

---

## Context

A data engineering patterns reference repo serves two distinct use cases simultaneously:

1. **Browsing** — an engineer reads the repo on GitHub or in a docs site, comparing patterns and stack variants before choosing one. They want to see everything: all patterns, all variants, side by side.

2. **Pulling** — an engineer has chosen a variant and wants to use it as a starter for a real project. They want only that variant's files at the repo root, ready to `git clone` and open in an IDE.

These two use cases are in tension. A single flat structure serves browsing but not pulling (the engineer gets all 22 variants). A branch-per-variant structure serves pulling but not browsing (no side-by-side comparison in the repo, no shared docs).

## Decision

Adopt **dual access**: directories in `main` for browsing, branches for pulling.

**`main` branch** — full repo. All patterns under `patterns/<name>/variants/<stack>/`. Shared docs, ADRs, decision matrix, and landscape intro are first-class content alongside the boilerplate code.

**Pattern branches** — one branch per variant, named `pattern/<pattern-name>/<stack-name>`. Each branch contains only that variant's files at the repo root: `README.md`, `src/`, `config/`, `docker-compose.yml`, `.env.example`, and a build file. No other patterns are present.

**`tooling/sync-pattern-branches.sh`** — a script that reads the `patterns/` directory tree in `main`, and for each variant creates or force-resets the corresponding branch with only that variant's files. Run manually after adding or updating a variant.

## Rationale

**Why not monorepo with sparse checkout?** Sparse checkout requires git knowledge that many engineers don't have. A branch named `pattern/batch-lakehouse/spark-iceberg` is self-documenting and requires only `git clone --branch`.

**Why not one repo per pattern?** Nine repos × 22 variants would require 22 repos, fragmented ADRs, and duplicated docs infrastructure.

**Why not one branch per pattern (not per variant)?** A branch per pattern still delivers multiple variants to the engineer who just wants one. The branch-per-variant granularity matches the atomic unit the engineer wants to pull.

## Consequences

**Positive:**
- Engineers who want to browse compare all variants in one place.
- Engineers who want to pull get a clean root-level repo for a single variant.
- Docs, ADRs, and decision matrix live in `main` and are always complete.

**Negative:**
- Pattern branches must be kept in sync with `main` manually (via `sync-pattern-branches.sh`). A variant updated in `main` is stale on its branch until the script is run.
- Force-pushing pattern branches is required on every sync. This is safe because pattern branches are not intended to receive commits directly — all development happens on `main`.

## When this choice stops being correct

If the number of variants grows beyond ~50, the branch-per-variant model becomes unwieldy to browse in a GitHub branch list. At that scale, a Cookiecutter or Copier template system (generating a variant from a template + a config) would be a better fit than maintaining static branches.

## Alternatives considered

**Cookiecutter templates** — generates a variant on demand from parameters. More DX-friendly for the engineer pulling a variant, but requires Python and Cookiecutter installed, and makes browsing variants harder (no static files to inspect).

**Git submodules** — one submodule per variant. Adds Git complexity with little benefit over the directory structure already in `main`.
```

- [ ] **Step 2: Write `docs/adrs/ADR-0002-idiomatic-language-per-ecosystem.md`**

```markdown
# ADR-0002: Idiomatic Language Per Ecosystem

**Status**: Accepted
**Date**: 2026-04-25
**Deciders**: Portfolio architect

---

## Context

A data engineering patterns repo spans multiple processing ecosystems: Spark, Flink, dbt, Python-based ML frameworks, and SQL-native query engines. Each ecosystem has a primary language in which its documentation, examples, and community are written.

A decision must be made: use one language throughout (e.g., Python everywhere via PySpark/PyFlink), or use the idiomatic language for each ecosystem.

## Decision

Use the **idiomatic language for each ecosystem**:

| Ecosystem | Language | Rationale |
|---|---|---|
| Apache Spark | **Scala 2.12** | Spark is written in Scala; Scala DataSet/RDD APIs have no Python equivalent; performance-critical Spark code avoids Python UDF serialisation overhead |
| Apache Flink | **Java 17** | Flink's primary API surface is Java; the Java ProcessFunction/SinkFunction API has richer type information than PyFlink; community examples are predominantly Java |
| dbt | **SQL** | dbt models are SQL; Python models exist but are an exception, not the rule |
| ML frameworks (Feast, feature pipelines) | **Python 3.11** | Feast, MLflow, scikit-learn, PyTorch — the ML ecosystem is Python-native; no alternative |
| Orchestration (Airflow, Prefect, Dagster) | **Python 3.11** | All three orchestrators are Python-native; DAGs/flows/assets are Python objects |
| Config-heavy patterns (CDC, Federated Query) | **YAML/JSON + SQL** | Debezium connectors are JSON; Trino/Presto catalogs are properties files; SQL views are the query interface |

## Rationale

**Against Python-everywhere:** PySpark hides the Spark type system behind Python's dynamic typing. PyFlink's ProcessFunction is less ergonomic than its Java counterpart and lacks some API features (e.g., BroadcastState). A Scala or Java engineer reading a PySpark boilerplate learns the Python API, not the Spark API — and the two diverge meaningfully at the edges.

**Against Scala-everywhere:** The ML and orchestration ecosystems have no viable Scala alternative. Forcing Scala into Airflow DAGs would produce unidiomatic, unmaintainable code.

**For idiomatic:** An engineer adopting a boilerplate starter will extend it in the same language. A Scala starter produces Scala extensions; a Java starter produces Java extensions. The boilerplate is a foundation, not a translation exercise.

## Consequences

**Positive:**
- Each boilerplate reads like examples from the official documentation of its ecosystem.
- Engineers proficient in the target language can read and extend the boilerplate without translation overhead.
- Type systems are used as intended: Scala case classes for Spark schemas, Java generics for Flink type information.

**Negative:**
- The repo requires familiarity with four languages (Scala, Java, Python, SQL) to read in full.
- A Python-only engineer cannot directly use the Spark or Flink boilerplates without learning Scala/Java.

## When this choice stops being correct

If PySpark or PyFlink closes the API gap with their JVM counterparts (i.e., full feature parity, no performance penalty for Python UDFs), revisiting Python-everywhere would be reasonable. As of 2026, the gap is material for production use cases.

## Alternatives considered

**Python throughout (PySpark + PyFlink):** Lower barrier for Python engineers. Rejected because PySpark and PyFlink lag their JVM counterparts in API coverage and produce boilerplate that does not generalise to the full API surface.

**Scala throughout (Spark + Flink in Scala):** Flink has a Scala API but it is deprecated in Flink 1.18+ in favour of Java. Using a deprecated API in a reference implementation would be actively misleading.
```

- [ ] **Step 3: Verify MkDocs builds**

```bash
mkdocs build --strict 2>&1 | grep -E "(ERROR|WARNING|built successfully)"
```

Expected: `INFO - Documentation built successfully.`

- [ ] **Step 4: Commit**

```bash
git add docs/adrs/ADR-0001-pattern-matrix-structure.md \
        docs/adrs/ADR-0002-idiomatic-language-per-ecosystem.md
git commit -m "docs: ADR-0001 pattern matrix structure, ADR-0002 language convention"
```

---

---

## Tasks 6–10

---

### Task 6: ADRs 0003 and 0004

**Files:**
- Create: `docs/adrs/ADR-0003-table-format-landscape.md`
- Create: `docs/adrs/ADR-0004-stream-processing-engine.md`

- [ ] **Step 1: Write `docs/adrs/ADR-0003-table-format-landscape.md`**

```markdown
# ADR-0003: Table Format Landscape — Iceberg vs Delta Lake vs Hudi

**Status**: Accepted
**Date**: 2026-04-25
**Deciders**: Portfolio architect

---

## Context

The lakehouse pattern requires an open table format that adds ACID transactions, schema evolution, and time travel on top of object storage (S3, GCS, ADLS). Three formats compete: Apache Iceberg, Delta Lake, and Apache Hudi. Each variant in this repo that writes a lakehouse table must choose one.

The formats share a core capability: they track table state in a metadata layer (a manifest or transaction log) above Parquet/ORC data files, so that a commit is atomic, readers see a consistent snapshot, and historical versions remain queryable.

Their differences are most visible at the edges: multi-engine interoperability, schema evolution breadth, upsert performance, and governance model.

## Decision

**Default to Apache Iceberg.** Use Delta Lake only in Databricks-primary stacks. Do not use Hudi unless the workload is dominated by record-level upserts at very high update rates.

## Rationale

### Iceberg

Iceberg's primary advantage is portability. An Iceberg table written by Spark is readable by Flink, Trino, Presto, Dremio, StarRocks, DuckDB, and AWS Athena — with no conversion or copy. This interoperability is built into the spec, not an afterthought.

Iceberg supports full schema evolution: add, drop, rename, and reorder columns without rewriting data files. Column IDs (not names) are used to track fields, so renaming a column does not break readers that pinned to the old name.

Iceberg's snapshot isolation model gives readers a consistent point-in-time view even as writers commit new data. Time travel is available to any engine that can read Iceberg metadata.

Weakness: Iceberg's REST catalog and Glue/Hive metastore integrations require configuration. The first-time setup is more involved than Delta (which self-bootstraps on any Spark cluster with the Delta JAR).

### Delta Lake

Delta's primary advantage is Spark integration depth. The `DeltaTable` API for MERGE, UPDATE, and DELETE is the most ergonomic in the ecosystem. Databricks manages the Delta catalog natively; no external metastore required.

Delta's transaction log (JSON + Parquet checkpoint files) is simpler than Iceberg's manifest hierarchy, which makes debugging easier for Spark-only teams.

Weakness: multi-engine support is improving (Flink and Trino connectors exist) but lags Iceberg. A Delta table written by Spark cannot be queried by Trino without additional setup and version negotiation.

### Hudi

Hudi's primary advantage is upsert performance. Its Copy-on-Write and Merge-on-Read storage types are optimised for high-rate record-level updates — the case where a CDC pipeline updates millions of rows per hour in a wide table. Hudi's record-level index can apply upserts without a full file scan.

Weakness: Hudi's Flink support is partial. Its multi-engine story is narrower than Iceberg. The upsert optimisation that justifies Hudi is not relevant for append-only or batch-overwrite workloads (most lakehouse batch patterns).

## Consequences

**Positive:**
- Iceberg boilerplates work with Trino, Presto, and any other Iceberg-capable engine out of the box.
- Delta boilerplates work seamlessly in Databricks and with the full `DeltaTable` Scala API.
- Pattern branches deliver a format that matches the team's engine choice.

**Negative:**
- Teams on Databricks who pull an Iceberg boilerplate will need to configure the Iceberg catalog — Delta would have been simpler for them.
- Teams who need Hudi's upsert performance must adapt an Iceberg or Delta boilerplate, as no Hudi variant is provided.

## When this choice stops being correct

If Hudi achieves full Flink support and Trino/Presto parity with Iceberg, it becomes worth adding Hudi variants for CDC-heavy patterns. Revisit if Hudi's upsert performance advantage closes the gap on Iceberg's MERGE INTO with equality delete files.

## Alternatives considered

**Hudi as default:** Rejected. Hudi's multi-engine support is narrower and its advantage (upsert rate) is irrelevant for the majority of lakehouse batch and streaming patterns.

**Delta as default:** Rejected. Delta's Trino/Presto story is weaker, and lock-in to Databricks is a cost we do not want to impose on engineers pulling a boilerplate.
```

- [ ] **Step 2: Write `docs/adrs/ADR-0004-stream-processing-engine.md`**

```markdown
# ADR-0004: Stream Processing Engine — Flink vs Spark Structured Streaming

**Status**: Accepted
**Date**: 2026-04-25
**Deciders**: Portfolio architect

---

## Context

The streaming lakehouse and CDC pipeline patterns require a stream processing engine. Two engines dominate the open-source landscape: Apache Flink and Apache Spark Structured Streaming. Both offer Kafka integration, exactly-once semantics, and lakehouse sink connectors. Both are used in production at scale.

Each streaming variant in this repo uses one engine. The choice must be justified so engineers know when to reach for each.

## Decision

**Use Flink for variants where sub-minute latency or native event-time semantics are primary requirements. Use Spark Structured Streaming for variants where team familiarity with Spark is the primary constraint and latency > 30 seconds is acceptable.**

## Rationale

### Flink

Flink processes each event as it arrives. Its watermark mechanism is native to the execution model — event-time windows, out-of-order event handling, and late data routing to a side output are first-class primitives, not workarounds.

Flink's exactly-once guarantee is implemented through two-phase commit: the Flink checkpoint (RocksDB state snapshot) and the sink commit are atomic. For Iceberg, this means data files are committed to the table only on checkpoint, ensuring no partial writes are visible to readers.

Flink's stateful processing (keyed streams, ValueState, ListState, MapState) is designed for long-running jobs with large state stores. RocksDB state backend keeps state on local disk with off-heap memory, making terabyte-scale state viable.

Weakness: Flink requires a cluster (JobManager + TaskManagers), adds Kafka operational overhead, and its Java API requires more boilerplate than Spark's DataFrame API.

### Spark Structured Streaming

Spark Structured Streaming uses micro-batches (configurable trigger interval, minimum ~100ms in practice, typically 30–60s for lakehouse writes). The DataFrame/Dataset API is consistent between batch and streaming — engineers who know Spark batch can read Structured Streaming code.

`foreachBatch` allows arbitrary DataFrame operations on each micro-batch, including Delta MERGE and Iceberg MERGE INTO. This makes it easier to implement upsert patterns without native streaming-sink APIs.

Weakness: true event-time processing in Structured Streaming requires watermarks on the streaming DataFrame, and late data handling is less granular than Flink's per-event routing. Micro-batch latency floors at ~30 seconds for Iceberg/Delta writes in practice (file commit overhead).

## Consequences

**Positive:**
- Flink variants demonstrate the highest-fidelity streaming pattern: exactly-once, event-time, per-event processing.
- Spark Structured Streaming variants are accessible to the larger Spark-fluent engineer population.
- The two variants within the streaming lakehouse pattern make the trade-off explicit rather than hiding it behind a single implementation.

**Negative:**
- Maintaining boilerplates in two different languages (Java for Flink, Scala for Spark) increases surface area.
- An engineer who picks the Spark Structured Streaming variant and later needs < 30s latency must migrate to Flink — the boilerplate does not help with that migration.

## When this choice stops being correct

If Spark Structured Streaming achieves sub-10-second commit latency for Iceberg/Delta sinks (through asynchronous commits or a new execution model), the latency argument for Flink weakens. Revisit if Structured Streaming adds true per-event watermarking.

## Alternatives considered

**Kafka Streams:** Rejected. Kafka Streams is a library, not a cluster-based engine. It does not support lakehouse sinks and is not suitable for the data volumes targeted by these patterns.

**Apache Beam:** Rejected. Beam's portability across runners (Flink, Spark, Dataflow) is valuable for cloud-portability, but adds an abstraction layer that obscures the idiomatic API of each engine. A Beam boilerplate teaches Beam, not Flink or Spark.
```

- [ ] **Step 3: Verify MkDocs builds**

```bash
mkdocs build --strict 2>&1 | grep -E "(ERROR|built successfully)"
```

Expected: `INFO - Documentation built successfully.`

- [ ] **Step 4: Commit**

```bash
git add docs/adrs/ADR-0003-table-format-landscape.md \
        docs/adrs/ADR-0004-stream-processing-engine.md
git commit -m "docs: ADR-0003 table format landscape, ADR-0004 stream processing engine"
```

---

### Task 7: ADRs 0005 and 0006

**Files:**
- Create: `docs/adrs/ADR-0005-orchestration-landscape.md`
- Create: `docs/adrs/ADR-0006-lakehouse-storage-layer.md`

- [ ] **Step 1: Write `docs/adrs/ADR-0005-orchestration-landscape.md`**

```markdown
# ADR-0005: Orchestration Landscape — Airflow vs Prefect vs Dagster vs Temporal

**Status**: Accepted
**Date**: 2026-04-25
**Deciders**: Portfolio architect

---

## Context

Three patterns in this repo include an orchestrated variant: batch-lakehouse (Spark + Airflow), ELT warehouse (dbt + DuckDB + Airflow), and ML feature pipeline (Feast + Spark + Airflow). The workflow-orchestration pattern covers all three orchestrators as independent boilerplates.

A decision must be made about which orchestrators to include and how to characterise each.

## Decision

Include **Airflow, Prefect, and Dagster** as orchestration variants. Do not include Temporal as a data pipeline orchestrator.

## Rationale

### Airflow

Airflow is the most widely deployed data pipeline orchestrator. Its provider ecosystem (600+ operators covering every cloud service, database, and processing engine) means that most integration questions are already solved. Engineers joining a team that uses Airflow will encounter it at scale.

Airflow's DAG model (a Python file that defines a directed acyclic graph of operators) is explicit and auditable. DAGs are versioned in Git. The scheduler, workers, and webserver are operationally mature and well-documented.

Weakness: Airflow's DAG parsing model (re-imports every file on each scheduler tick) creates performance problems at large scale. Dynamic task generation (generating tasks at runtime based on data) is possible but verbose compared to Prefect.

### Prefect

Prefect's primary differentiator is Python ergonomics. A Prefect flow is a Python function decorated with `@flow`; tasks are functions decorated with `@task`. Dynamic task creation (calling a task in a loop) is idiomatic Python, not a special API. Prefect's UI (Prefect Cloud or self-hosted server) has a modern developer experience.

Prefect's deployment model (work pools, workers, deployments) is more flexible than Airflow's worker model — the same flow can run locally, on Kubernetes, or on a cloud provider without code changes.

Weakness: Prefect's provider ecosystem is narrower than Airflow's. For uncommon integrations, you write Python code directly rather than using a pre-built operator.

### Dagster

Dagster's primary differentiator is asset-centric thinking. A Dagster software-defined asset (SDA) represents a data artifact (a table, a file, an ML model) and its computation. The asset graph shows lineage — which assets depend on which — and the scheduler materialises assets on demand or on a cron schedule.

Dagster's observability is the strongest of the three: each asset materialisation records metadata (row counts, schema, custom metrics), and the UI shows asset health over time.

Weakness: Dagster's asset model requires a mindset shift from task-centric orchestration. Engineers who think in "jobs" rather than "data assets" find the learning curve steeper.

### Temporal

Temporal is a durable workflow engine for long-running business processes (order fulfilment, payment processing, human-in-the-loop approvals). It is not designed for data pipeline orchestration — it has no data-aware scheduling, no DAG visualisation, and no concept of data assets or freshness SLAs. Including Temporal would mislead engineers looking for a data pipeline orchestrator.

## Consequences

**Positive:**
- Three orchestrator variants give engineers a concrete comparison across the key dimensions: ecosystem breadth (Airflow), Python ergonomics (Prefect), and asset observability (Dagster).
- The orchestrated pattern variants (batch-lakehouse + Airflow, ELT + Airflow) demonstrate integration with the most commonly deployed orchestrator.

**Negative:**
- Airflow is used in the integrated variants (batch-lakehouse, ELT) but Prefect and Dagster are only in the standalone orchestration pattern. Engineers who prefer Prefect/Dagster must adapt the integrated variants themselves.

## When this choice stops being correct

If Prefect or Dagster achieves Airflow's provider ecosystem breadth (600+ pre-built integrations), the default for integrated variants should be revisited. Revisit also if Temporal adds data pipeline primitives (freshness SLAs, asset lineage, SQL-level observability).

## Alternatives considered

**Luigi:** Rejected. Luigi is effectively unmaintained and has been superseded by Airflow and Prefect in every dimension.

**Argo Workflows:** Rejected. Argo is a Kubernetes-native workflow engine with a YAML-first authoring model. It is a valid orchestrator for Kubernetes-heavy shops but outside the data pipeline mainstream.
```

- [ ] **Step 2: Write `docs/adrs/ADR-0006-lakehouse-storage-layer.md`**

```markdown
# ADR-0006: Lakehouse Storage Layer — Open Table Formats over Managed Warehouses

**Status**: Accepted
**Date**: 2026-04-25
**Deciders**: Portfolio architect

---

## Context

The batch lakehouse, streaming lakehouse, CDC pipeline, lambda architecture, ML feature pipeline, and federated query patterns all require a durable storage layer for processed data. The two primary options are:

1. **Open table formats on object storage** (Iceberg/Delta/Hudi on S3/GCS/ADLS)
2. **Managed cloud warehouses** (Snowflake, BigQuery, Redshift, Azure Synapse)

The ELT/warehouse pattern explicitly targets managed warehouses. All other patterns in this repo target open table formats.

## Decision

Use **open table formats on object storage** (primarily Iceberg, secondarily Delta) for all patterns except ELT/warehouse, which uses dbt against managed warehouses by design.

## Rationale

**Cost at scale:** Object storage (S3, GCS) costs $0.023/GB-month. Snowflake and BigQuery storage costs are 5–20× higher. For a patterns repo that demonstrates scale-oriented architectures, the default storage choice should not introduce a cost cliff.

**Engine portability:** Iceberg/Delta tables on object storage are readable by any compatible engine (Trino, Presto, Spark, Flink, Athena, DuckDB). A managed warehouse's storage is readable only through that warehouse's query engine. An Iceberg table written by a Flink boilerplate can be queried locally with DuckDB, validated in CI with a minimal test, and queried at scale with Trino — without paying warehouse compute costs.

**No vendor account required to run locally:** An engineer pulling a boilerplate can run `docker compose up` and have a working local stack with MinIO (S3-compatible), an Iceberg REST catalog, and a Spark or Flink cluster. No Snowflake trial account, no GCP billing enabled.

**Separation of storage and compute:** Object storage + open table format is the separation-of-storage-and-compute architecture in its purest form. Compute can be swapped (Spark → Trino → Athena) without touching the data.

## Consequences

**Positive:**
- Engineers can run every boilerplate locally with Docker Compose and MinIO.
- Data written by one pattern boilerplate is readable by another (e.g., the federated-query boilerplate can query tables written by the batch-lakehouse boilerplate).
- No cloud billing required to evaluate the boilerplate.

**Negative:**
- Local docker-compose stacks are heavier than connecting to a managed warehouse. The Flink + Kafka + MinIO + Iceberg REST stack requires ~6GB RAM.
- Engineers who are Snowflake or BigQuery shops must adapt the boilerplate to write to their warehouse. The ELT/warehouse pattern covers this case, but the other patterns do not.

## When this choice stops being correct

If a managed warehouse introduces a free local emulator (as DuckDB does for in-process analytics), it becomes viable to include that warehouse as a storage variant for batch patterns. DuckDB is already included as an ELT/warehouse variant for this reason.

## Alternatives considered

**Redshift as default storage:** Rejected. Redshift requires an AWS account and introduces per-node costs even at the smallest scale. Object storage is strictly cheaper and more portable.

**Delta on Azure ADLS as default:** Rejected. ADLS is cloud-specific. MinIO is S3-compatible and runs locally without any cloud account.
```

- [ ] **Step 3: Verify MkDocs builds**

```bash
mkdocs build --strict 2>&1 | grep -E "(ERROR|built successfully)"
```

Expected: `INFO - Documentation built successfully.`

- [ ] **Step 4: Commit**

```bash
git add docs/adrs/ADR-0005-orchestration-landscape.md \
        docs/adrs/ADR-0006-lakehouse-storage-layer.md
git commit -m "docs: ADR-0005 orchestration landscape, ADR-0006 lakehouse storage layer"
```

---

### Task 8: ADRs 0007 and 0008

**Files:**
- Create: `docs/adrs/ADR-0007-cdc-tooling.md`
- Create: `docs/adrs/ADR-0008-local-dev-approach.md`

- [ ] **Step 1: Write `docs/adrs/ADR-0007-cdc-tooling.md`**

```markdown
# ADR-0007: CDC Tooling — Debezium Primary, Airbyte as Managed Alternative

**Status**: Accepted
**Date**: 2026-04-25
**Deciders**: Portfolio architect

---

## Context

The CDC pipeline pattern captures database changes in real time and streams them into a processing engine. Two tool categories are available:

1. **Log-based CDC** (Debezium): reads the database's write-ahead log (WAL) directly. Sub-second capture latency, zero impact on source database query performance.
2. **Query-based CDC / managed ELT** (Airbyte, Fivetran, Stitch): polls the source database on a schedule or uses database triggers. Simpler setup, higher source load, higher latency.

## Decision

Use **Debezium** as the primary CDC tool in all CDC pipeline boilerplates. Document **Airbyte** as the managed alternative in the pattern README and ADR, but do not provide an Airbyte boilerplate variant.

## Rationale

### Debezium

Debezium reads the PostgreSQL WAL (or MySQL binlog, MongoDB oplog, etc.) via the replication protocol. Every committed transaction appears in the Debezium Kafka topic within milliseconds of the source commit. There is no polling interval — the capture latency is bounded by network and Kafka producer throughput, not a schedule.

Debezium's Kafka Connect integration means the connector runs as a Kafka Connect worker, scales horizontally, and produces Avro-serialised events to Kafka topics compatible with any downstream consumer (Flink, Spark, ksqlDB, etc.).

Debezium is the de facto standard for WAL-based CDC in the open-source ecosystem. Engineers encountering CDC in production will almost certainly encounter Debezium.

### Airbyte

Airbyte is a managed ELT platform that includes 300+ pre-built connectors. For databases that do not support WAL-based replication (e.g., legacy MySQL without GTID, some hosted databases), Airbyte's query-based CDC or full-table replication is the practical alternative.

Airbyte's operational model (a web UI, a platform API, managed connectors) is significantly simpler than operating a Kafka Connect cluster with Debezium. For teams that cannot operate Kafka, Airbyte is the right choice.

Weakness: Airbyte is not truly log-based — most connectors use cursor-based or full-table replication. Capture latency is minutes, not milliseconds. Airbyte is not a substitute for Debezium when sub-minute freshness is required.

## Consequences

**Positive:**
- Debezium boilerplates teach the WAL-based CDC model, which is the most operationally demanding and most commonly encountered in production data platforms.
- The pattern README explains when Airbyte is the right alternative, so engineers who cannot operate Kafka are not left without guidance.

**Negative:**
- Engineers who need Airbyte must build their own boilerplate from the pattern README guidance.
- Debezium requires PostgreSQL configured with `wal_level = logical`. The docker-compose.yml handles this for local dev, but production source databases may require a DBA to enable it.

## When this choice stops being correct

If Debezium releases a managed cloud offering (hosted Debezium) that removes the Kafka Connect operational burden, the gap between Debezium and Airbyte closes. Similarly, if Airbyte achieves sub-second latency via a true log-based connector, the distinction collapses.

## Alternatives considered

**Maxwell's Daemon:** Rejected. Maxwell is MySQL-only and less actively maintained than Debezium.

**AWS DMS (Database Migration Service):** Rejected. Cloud-specific, requires an AWS account, not portable to local dev.

**Kafka Connect without Debezium (using JDBC Source Connector):** Rejected. The JDBC Source Connector is query-based (polling), not log-based. It cannot produce the change event envelope (before/after state) that makes CDC useful for upsert pipelines.
```

- [ ] **Step 2: Write `docs/adrs/ADR-0008-local-dev-approach.md`**

```markdown
# ADR-0008: Local Dev Approach — Docker Compose Per Variant

**Status**: Accepted
**Date**: 2026-04-25
**Deciders**: Portfolio architect

---

## Context

Every boilerplate variant must be runnable locally so that an engineer can validate the pattern before committing to a production deployment. The local dev stack must be self-contained, require no cloud account, and start with a single command.

Three approaches exist:

1. **Docker Compose per variant** — each variant ships its own `docker-compose.yml` that starts exactly the services needed for that variant.
2. **Shared Docker Compose at repo root** — one `docker-compose.yml` with all services, profiles to enable subsets.
3. **Dev container / Codespaces** — a `.devcontainer/` configuration that provisions a cloud development environment.

## Decision

Use **Docker Compose per variant**. Each variant's `docker-compose.yml` starts the minimal services for that variant and nothing else.

## Rationale

**Variant isolation:** An engineer who pulls the `pattern/batch-lakehouse/spark-iceberg` branch should not start a Kafka broker (needed for streaming patterns but not for batch). A per-variant `docker-compose.yml` makes the local stack match the pattern — no unnecessary services, no confusion about what is required.

**No shared state:** A shared root-level `docker-compose.yml` with profiles creates shared state problems. Kafka volumes from a CDC run interfere with a batch-lakehouse run. Per-variant Compose files use isolated Docker networks and named volumes, so variants do not collide.

**Branch compatibility:** Because each branch contains only one variant's files, a per-variant `docker-compose.yml` at the variant root becomes the `docker-compose.yml` at the branch root after `sync-pattern-branches.sh` runs. An engineer who clones the pattern branch runs `docker compose up` at the repo root — no path navigation required.

**MinIO as S3 substitute:** All variants that write to object storage use MinIO (S3-compatible API, runs in Docker). MinIO is configured with a `chakra-lakehouse` bucket and access credentials in `.env.example`. No AWS account required.

## Local stack by pattern

| Pattern | Services in docker-compose.yml |
|---|---|
| Batch Lakehouse (Spark + Iceberg) | MinIO, Iceberg REST catalog |
| Batch Lakehouse (Spark + Delta) | MinIO |
| Batch Lakehouse (Spark + Iceberg + Airflow) | MinIO, Iceberg REST catalog, Airflow (standalone) |
| Streaming Lakehouse (Flink + Iceberg) | Kafka, Flink JobManager + TaskManager, MinIO, Iceberg REST catalog |
| Streaming Lakehouse (Spark + Delta) | Kafka, MinIO |
| ELT (dbt + Snowflake) | None (Snowflake is cloud-only; `.env.example` provides credentials) |
| ELT (dbt + BigQuery) | None (BigQuery is cloud-only) |
| ELT (dbt + DuckDB + Airflow) | Airflow (standalone) |
| CDC (Debezium + Kafka + Flink) | PostgreSQL, Kafka + Connect, Flink JobManager + TaskManager, MinIO |
| CDC (Debezium + Kafka + Spark) | PostgreSQL, Kafka + Connect, MinIO |
| Lambda (Flink + Spark + Iceberg) | Kafka, Flink, MinIO, Iceberg REST catalog |
| Lambda (Spark Streaming + Spark Batch + Delta) | Kafka, MinIO |
| Federated Query (Trino + Iceberg + S3) | Trino, MinIO, Iceberg REST catalog |
| Federated Query (Presto + Hive) | Presto, Hive Metastore (PostgreSQL-backed), MinIO |
| ML Feature Pipeline (Feast + Redis + Spark) | Redis, MinIO |
| ML Feature Pipeline (Flink + Spark offline) | Kafka, Flink, MinIO |
| ML Feature Pipeline (Feast + Spark + Airflow) | Redis, MinIO, Airflow (standalone) |
| Graph Processing (Spark GraphX) | MinIO |
| Graph Processing (Neo4j + Spark) | Neo4j |
| Workflow Orchestration (Airflow) | Airflow (standalone with LocalExecutor) |
| Workflow Orchestration (Prefect) | Prefect server |
| Workflow Orchestration (Dagster) | Dagster webserver + daemon |

## Consequences

**Positive:**
- `docker compose up` starts exactly what the pattern needs — nothing more.
- Pattern branches are immediately usable: clone, copy `.env.example` to `.env`, `docker compose up`.
- No cross-pattern state pollution.

**Negative:**
- 22 `docker-compose.yml` files to maintain. When a base image version changes (e.g., MinIO tag), all variants that use MinIO must be updated.
- Heavier variants (Flink + Kafka + MinIO + Iceberg REST) require ~6GB RAM. Engineers on 8GB machines will be constrained.

## When this choice stops being correct

If Docker Compose is superseded as the local dev standard (e.g., if Podman Compose or Nix-based dev environments become the norm), the `docker-compose.yml` files should be migrated. The per-variant isolation principle survives the tool change.

## Alternatives considered

**Shared docker-compose.yml with profiles:** Rejected. An engineer pulling a single-variant branch gets a `docker-compose.yml` that references services from other patterns via profiles. Confusing and unnecessary.

**Dev container / Codespaces:** Rejected. Dev containers require VS Code or a GitHub Codespaces subscription. They are not universally available and add a cloud dependency to what should be a purely local experience.

**Minikube / k3d:** Rejected. Kubernetes adds significant complexity for what is fundamentally a local dev experience. The production deployment story (Kubernetes) is documented in each variant's README but is not part of the local dev stack.
```

- [ ] **Step 3: Verify MkDocs builds**

```bash
mkdocs build --strict 2>&1 | grep -E "(ERROR|built successfully)"
```

Expected: `INFO - Documentation built successfully.`

- [ ] **Step 4: Commit**

```bash
git add docs/adrs/ADR-0007-cdc-tooling.md \
        docs/adrs/ADR-0008-local-dev-approach.md
git commit -m "docs: ADR-0007 CDC tooling, ADR-0008 local dev approach"
```

---

### Task 9: ADRs 0009 and 0010

**Files:**
- Create: `docs/adrs/ADR-0009-graph-processing.md`
- Create: `docs/adrs/ADR-0010-ml-feature-pipeline.md`

- [ ] **Step 1: Write `docs/adrs/ADR-0009-graph-processing.md`**

```markdown
# ADR-0009: Graph Processing — When Graph is the Right Model

**Status**: Accepted
**Date**: 2026-04-25
**Deciders**: Portfolio architect

---

## Context

Graph processing is included as a pattern alongside lakehouse, streaming, and ELT patterns. Unlike the others, graph processing is not a data pipeline pattern — it is a computational model. The question is when graph processing is warranted and how to frame it in a patterns repo.

Additionally, two tools are available: Spark GraphX (batch graph algorithms on distributed data) and Neo4j (a native graph database with the Cypher query language, connected to Spark via the Neo4j Spark Connector).

## Decision

Include graph processing as a pattern with two variants: **Spark GraphX** for batch distributed graph algorithms, and **Neo4j + Spark** for graph-native query + Spark-scale ingestion. Frame the pattern README around the question "when is graph the right model?" before presenting the variants.

## Rationale

### When graph is the right model

Relational databases model entities (rows) and their attributes (columns). Graph databases model entities (nodes) and the relationships between them (edges) as first-class citizens. The choice of model depends on whether relationships are the primary query target.

**Use graph when:**
- The query is "traverse from A, find all B reachable within N hops" (e.g., fraud ring detection, social network recommendations, supply chain dependency analysis)
- Relationship attributes (edge properties: weight, timestamp, type) are as important as node attributes
- The graph has variable-depth traversal requirements (cannot be expressed as a fixed JOIN depth)

**Use relational/lakehouse when:**
- Relationships are filters, not traversal targets (e.g., "orders placed by customers in region X")
- The query can be expressed as a bounded JOIN (2–3 tables)
- Aggregate analytics (SUM, COUNT, GROUP BY) are the primary access pattern

### Spark GraphX

GraphX runs batch graph algorithms (PageRank, connected components, triangle counting, label propagation) on graphs that fit in distributed Spark memory. It is the right choice when:
- The graph is large (billions of edges) and must be distributed
- The algorithm is a standard graph algorithm (not an ad-hoc traversal)
- Results are materialised to a lakehouse table for downstream consumption

GraphX's API is low-level (RDD-based, not DataFrame-based). Writing GraphX code requires understanding RDD transformations and graph partitioning strategies.

### Neo4j + Spark

Neo4j is a native graph database with Cypher as its query language. The Neo4j Spark Connector writes Spark DataFrames to Neo4j (bulk ingestion) and reads Neo4j query results as Spark DataFrames (large-scale graph analytics output back to the lakehouse).

Use Neo4j + Spark when:
- The graph requires real-time traversal queries (Cypher WHERE path queries) in addition to batch algorithms
- The team is comfortable with Cypher as a query language
- The graph size fits in a Neo4j cluster (up to ~10B nodes in community edition, larger with Enterprise)

## Consequences

**Positive:**
- Two variants cover the two primary graph use cases: batch algorithm (GraphX) and native graph query + bulk ingestion (Neo4j + Spark).
- The pattern README steers engineers away from graph when it is not the right model, preventing the common mistake of over-engineering a JOIN as a graph traversal.

**Negative:**
- GraphX's RDD API is considered legacy within Spark (GraphFrames, the DataFrame-based alternative, is more ergonomic but requires an external JAR not bundled with Spark). GraphX is included because it ships with Spark and needs no additional dependency.
- Neo4j requires a licence for production Enterprise features; Community Edition has memory limits that are hit quickly at scale.

## When this choice stops being correct

If GraphFrames (DataFrame-based graph API) is bundled with Spark or achieves the same distribution as GraphX, replace the GraphX variant with GraphFrames — the API is more ergonomic and consistent with the Spark DataFrame model used in other patterns.

## Alternatives considered

**Apache Giraph:** Rejected. Giraph is Hadoop-era graph processing, effectively unmaintained, and superseded by GraphX.

**Amazon Neptune:** Rejected. Cloud-specific, no local dev equivalent, incompatible with the open-source-on-object-storage philosophy of this repo.

**TigerGraph:** Rejected. Proprietary, requires a commercial licence for production use.
```

- [ ] **Step 2: Write `docs/adrs/ADR-0010-ml-feature-pipeline.md`**

```markdown
# ADR-0010: ML Feature Pipeline — Online vs Offline Store Split

**Status**: Accepted
**Date**: 2026-04-25
**Deciders**: Portfolio architect

---

## Context

The ML feature pipeline pattern computes and serves features for machine learning models. Two concerns must be addressed independently:

1. **Offline store** — historical feature values used for model training and batch inference. Latency: minutes. Storage: Parquet/Iceberg on object storage.
2. **Online store** — current feature values used for real-time model serving. Latency: milliseconds. Storage: Redis or a key-value store.

A decision must be made about whether to use a feature store framework (Feast) to manage both stores, or to build the two stores separately with Flink (online) and Spark (offline).

## Decision

Provide **three variants**:

1. **Feast + Redis + Spark** — use Feast to manage both stores. Feast materialises features from the offline store (Parquet/Iceberg on S3) into the online store (Redis) on a schedule. Spark computes the offline features.
2. **Flink online + Spark offline** — no feature store framework. Flink computes online features continuously (low latency); Spark computes offline features on a schedule. The engineer manages the two stores independently.
3. **Feast + Spark + Airflow** — same as variant 1 but Airflow orchestrates the Feast materialisation and Spark offline computation jobs.

## Rationale

### Online vs offline split

The split exists because the two serving contexts have incompatible requirements. Training a model on 1TB of historical features requires sequential reads from object storage — Redis cannot serve this efficiently. Serving a real-time prediction requires sub-millisecond feature lookup — querying object storage cannot do this. The split is not optional; it is a consequence of the access patterns.

### Why Feast

Feast is the most widely deployed open-source feature store. It provides:
- A unified feature definition (Python `FeatureView` objects) that drives both offline and online materialisation
- A `get_online_features()` API that abstracts the online store (Redis, DynamoDB, BigTable) behind a consistent interface
- An offline retrieval API (`get_historical_features()`) for training data generation with point-in-time correctness
- Integration with Spark for offline feature computation

Feast's materialisation (copying features from offline to online) is the key operation that links the two stores. Airflow or a scheduled Feast job triggers materialisation on a defined cadence.

### Why the Flink + Spark offline variant

Not all teams want to adopt a feature store framework. The Flink + Spark variant demonstrates the underlying mechanics: Flink writes features to Redis directly (using the Flink Redis Sink); Spark writes features to the offline Parquet store. The engineer manages consistency between the two stores without a framework abstraction.

This variant is educational: it makes the online/offline split explicit without hiding it behind Feast's API. Engineers who build on it understand what Feast is doing under the hood.

## Consequences

**Positive:**
- Three variants cover the full spectrum: framework-managed (Feast), framework-managed + orchestrated (Feast + Airflow), and hand-rolled (Flink + Spark).
- Feast variants demonstrate the point-in-time correctness property of `get_historical_features()` — a subtle but critical requirement for unbiased training data.

**Negative:**
- Feast's materialisation requires a running Redis instance and a configured Feast registry. The docker-compose.yml provides both, but the first-run experience is more involved than other patterns.
- The Flink + Spark offline variant does not implement point-in-time correctness — engineers extending it for training data must implement this themselves.

## When this choice stops being correct

If Feast deprecates Spark in favour of a new offline engine (e.g., DuckDB-based materialisation), the Feast + Spark + Airflow variant must be updated. Revisit also if Tecton (managed feature platform) introduces an open-source core that warrants inclusion as a fourth variant.

## Alternatives considered

**MLflow Feature Store:** Rejected. MLflow's feature store is Databricks-native and not available as a standalone open-source component.

**Hopsworks Feature Store:** Rejected. Hopsworks is a full ML platform (not just a feature store) and requires significant infrastructure. Too heavy for a boilerplate starter.

**Tecton:** Rejected. Proprietary SaaS, no self-hosted option.
```

- [ ] **Step 3: Verify MkDocs builds**

```bash
mkdocs build --strict 2>&1 | grep -E "(ERROR|built successfully)"
```

Expected: `INFO - Documentation built successfully.`

- [ ] **Step 4: Commit**

```bash
git add docs/adrs/ADR-0009-graph-processing.md \
        docs/adrs/ADR-0010-ml-feature-pipeline.md
git commit -m "docs: ADR-0009 graph processing, ADR-0010 ML feature pipeline"
```

---

### Task 10: Pattern Docs Pages (All 9)

**Files:**
- Overwrite: `docs/patterns/batch-lakehouse.md`
- Overwrite: `docs/patterns/streaming-lakehouse.md`
- Overwrite: `docs/patterns/elt-warehouse.md`
- Overwrite: `docs/patterns/cdc-pipeline.md`
- Overwrite: `docs/patterns/lambda-architecture.md`
- Overwrite: `docs/patterns/federated-query.md`
- Overwrite: `docs/patterns/ml-feature-pipeline.md`
- Overwrite: `docs/patterns/graph-processing.md`
- Overwrite: `docs/patterns/workflow-orchestration.md`

- [ ] **Step 1: Write `docs/patterns/batch-lakehouse.md`**

```markdown
# Batch Lakehouse

Scheduled Spark jobs read from source systems, apply transformations, and write to an open table format (Iceberg or Delta Lake) on object storage. The highest-throughput, lowest-complexity pattern for analytical data movement.

## When to use

- Latency requirement is > 15 minutes (hourly, daily, or on-demand runs are acceptable)
- Source data arrives in files (S3 drops, SFTP, database exports) or via scheduled database queries
- Full reprocessing from source is feasible (no need to replay a Kafka log)
- Team is Spark/Scala fluent; no Kafka or Flink expertise required
- Cost is a primary constraint — Spark runs only during the job, no always-on cluster

## When NOT to use

- You need < 5-minute data freshness — use Streaming Lakehouse
- Your source is a live database WAL — use CDC Pipeline
- Your team writes SQL but not Scala — use ELT / Warehouse

## Variants

| Variant | Stack | Key idiom | Use when |
|---|---|---|---|
| [spark-iceberg](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/batch-lakehouse/variants/spark-iceberg) | Spark 3.5 + Iceberg 1.5 + S3 | `DataFrameWriter` with Iceberg catalog | Multi-engine shop; need Trino/Presto reads |
| [spark-delta](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/batch-lakehouse/variants/spark-delta) | Spark 3.5 + Delta Lake 3.1 + S3 | `DeltaTable.forPath().merge()` | Databricks-first; want tightest Spark integration |
| [spark-iceberg-airflow](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/batch-lakehouse/variants/spark-iceberg-airflow) | Spark + Iceberg + Airflow 2.9 | `SparkSubmitOperator` in DAG | Need scheduled retry, alerting, and backfill |

## Related patterns

- [Streaming Lakehouse](streaming-lakehouse.md) — when latency < 5 minutes is required
- [CDC Pipeline](cdc-pipeline.md) — when source is a live database WAL
- [Workflow Orchestration](workflow-orchestration.md) — when you need a standalone orchestrator reference
```

- [ ] **Step 2: Write `docs/patterns/streaming-lakehouse.md`**

```markdown
# Streaming Lakehouse

A continuous pipeline reads events from a Kafka topic, processes them with Flink or Spark Structured Streaming, and writes to an open table format with sub-minute commit latency. Combines the freshness of streaming with the query semantics of a lakehouse.

## When to use

- Latency requirement is 1–5 minutes (or sub-minute with Flink)
- Source data is already in Kafka (events, CDC output, IoT telemetry)
- Reprocessing must replay the Kafka log from a given offset — not re-extract from source
- Exactly-once semantics are required (Flink variant) or micro-batch is acceptable (Spark variant)

## When NOT to use

- Latency > 15 minutes — use Batch Lakehouse (simpler, cheaper)
- Source is a live database and you haven't set up Kafka — use CDC Pipeline first, then this pattern downstream
- Team has no Kafka or stream processing experience — operational complexity is high

## Variants

| Variant | Stack | Key idiom | Latency floor | Use when |
|---|---|---|---|---|
| [flink-iceberg](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/streaming-lakehouse/variants/flink-iceberg) | Flink 1.19 + Iceberg 1.5 + Kafka | `FlinkSink` with two-phase commit on checkpoint | ~5s (checkpoint interval) | Exactly-once, event-time windowing, sub-minute |
| [spark-delta](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/streaming-lakehouse/variants/spark-delta) | Spark 3.5 + Delta 3.1 + Kafka | `readStream` + `foreachBatch` with Delta MERGE | ~30–60s (trigger interval) | Team knows Spark, latency > 30s acceptable |

## Related patterns

- [Batch Lakehouse](batch-lakehouse.md) — when latency > 15 minutes is fine
- [CDC Pipeline](cdc-pipeline.md) — when source is a database WAL, not a Kafka topic
- [Lambda Architecture](lambda-architecture.md) — when both streaming AND batch correctness are required
```

- [ ] **Step 3: Write `docs/patterns/elt-warehouse.md`**

```markdown
# ELT / Warehouse

dbt transforms data that is already in a cloud warehouse (Snowflake, BigQuery) or a local analytical database (DuckDB). No distributed compute required — the warehouse handles scale. The simplest pattern when your data is already where it needs to be.

## When to use

- Data is already loaded into a cloud warehouse or DuckDB
- Transformation logic is SQL (no Python UDFs, no distributed joins across systems)
- Latency requirement is minutes to hours (dbt runs on a schedule)
- Team writes SQL fluently; no Spark or Flink experience required
- You want built-in lineage, testing, and documentation (dbt provides all three)

## When NOT to use

- Data is not yet in the warehouse — load it first (use CDC Pipeline, Batch Lakehouse, or a managed ELT tool)
- Latency < 5 minutes — dbt is not a streaming tool
- Transformation logic requires Python, ML inference, or cross-system joins — use Spark or Federated Query

## Variants

| Variant | Stack | Use when |
|---|---|---|
| [dbt-snowflake](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/elt-warehouse/variants/dbt-snowflake) | dbt 1.8 + Snowflake | Snowflake is your warehouse |
| [dbt-bigquery](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/elt-warehouse/variants/dbt-bigquery) | dbt 1.8 + BigQuery | GCP-first org |
| [dbt-duckdb-airflow](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/elt-warehouse/variants/dbt-duckdb-airflow) | dbt 1.8 + DuckDB + Airflow 2.9 | Cost-sensitive, small-medium data, local or file-based |

## Related patterns

- [Batch Lakehouse](batch-lakehouse.md) — when data is not in a warehouse and must be processed at scale
- [CDC Pipeline](cdc-pipeline.md) — to load data into the warehouse from a live database
- [Workflow Orchestration](workflow-orchestration.md) — for standalone dbt orchestration references
```

- [ ] **Step 4: Write `docs/patterns/cdc-pipeline.md`**

```markdown
# CDC Pipeline

Debezium reads the database write-ahead log (WAL) and streams every committed insert, update, and delete as an event to a Kafka topic. A downstream processor (Flink or Spark) consumes the CDC events and materialises them into the lakehouse.

## When to use

- Source is a live relational database (PostgreSQL, MySQL, SQL Server, Oracle)
- Sub-second capture latency is required (WAL-based, not polling-based)
- You need a full change history, not just current state (every insert/update/delete)
- Zero impact on source database query performance is required (WAL reading does not add query load)

## When NOT to use

- Source database does not support WAL replication (use Airbyte with query-based CDC instead)
- Latency > 5 minutes acceptable and source supports direct Spark JDBC reads — use Batch Lakehouse (simpler)
- You only need current state, not change history — a nightly snapshot may be sufficient

## Variants

| Variant | Stack | Key idiom | Use when |
|---|---|---|---|
| [debezium-kafka-flink](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/cdc-pipeline/variants/debezium-kafka-flink) | Debezium + Kafka + Flink 1.19 | `KafkaSource` consuming CDC envelope → `KeyedProcessFunction` for upsert | Exactly-once, complex stream processing downstream |
| [debezium-kafka-spark](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/cdc-pipeline/variants/debezium-kafka-spark) | Debezium + Kafka + Spark 3.5 | `readStream` from Kafka → `foreachBatch` with Delta MERGE | Team knows Spark, latency > 30s acceptable |

## Related patterns

- [Streaming Lakehouse](streaming-lakehouse.md) — this pattern feeds into a streaming lakehouse
- [Lambda Architecture](lambda-architecture.md) — CDC as the streaming input to the Lambda pattern
```

- [ ] **Step 5: Write `docs/patterns/lambda-architecture.md`**

```markdown
# Lambda Architecture

Two parallel processing paths — a streaming path (low latency, approximate) and a batch path (high latency, accurate) — whose outputs are merged at query time. Use only when both sub-minute freshness AND batch-level accuracy are simultaneously required.

## When to use

- Stakeholders require both: approximate results within seconds AND fully accurate results within hours
- Reprocessing is required regularly (batch corrects streaming approximations)
- Team can operate both Flink/Spark Streaming AND a separate Spark batch job
- The query layer (Iceberg/Delta time travel, or a serving layer) can merge streaming and batch output

## When NOT to use

- Sub-minute latency is not required — use Batch Lakehouse (half the operational complexity)
- Batch accuracy is not required — use Streaming Lakehouse
- Your team cannot staff two processing paths — the operational cost is high; Lambda is only justified when the business requirement genuinely demands it

## Variants

| Variant | Stack | Use when |
|---|---|---|
| [flink-spark-iceberg](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/lambda-architecture/variants/flink-spark-iceberg) | Flink 1.19 (streaming) + Spark 3.5 (batch) + Iceberg | Need exactly-once streaming AND Iceberg portability |
| [spark-streaming-batch-delta](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/lambda-architecture/variants/spark-streaming-batch-delta) | Spark Structured Streaming + Spark Batch + Delta | Single Spark team, Delta as unifying format |

## Related patterns

- [Streaming Lakehouse](streaming-lakehouse.md) — if you can drop the batch path
- [Batch Lakehouse](batch-lakehouse.md) — if you can drop the streaming path
```

- [ ] **Step 6: Write `docs/patterns/federated-query.md`**

```markdown
# Federated Query

Trino or Presto queries across multiple storage systems (Iceberg on S3, Hive, PostgreSQL, Kafka) without moving data. The right pattern when your data estate is polyglot and data movement is too expensive or too slow.

## When to use

- Data lives in multiple systems (lakehouse + operational database + data warehouse) and must be queried together
- Data movement (ETL) is not feasible due to volume, latency, or cost
- Query latency of seconds to minutes is acceptable (federated queries are slower than local queries)
- Team is SQL-fluent and does not need a processing engine for transformation

## When NOT to use

- All data is in one system — direct query is always faster than federation
- Sub-second query latency required — federated queries cross network boundaries; local queries do not
- Heavy aggregations on huge tables — federated query engines push down filters but not all aggregations; Spark batch will outperform

## Variants

| Variant | Stack | Use when |
|---|---|---|
| [trino-iceberg-s3](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/federated-query/variants/trino-iceberg-s3) | Trino 450 + Iceberg 1.5 + MinIO | Iceberg as primary format; Presto-compatible SQL |
| [presto-hive](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/federated-query/variants/presto-hive) | Presto 0.287 + Hive Metastore | Existing Hive metastore; legacy Hadoop estate |

## Related patterns

- [Batch Lakehouse](batch-lakehouse.md) — to build the Iceberg tables that Trino queries
- [ELT / Warehouse](elt-warehouse.md) — when all data is already in one warehouse
```

- [ ] **Step 7: Write `docs/patterns/ml-feature-pipeline.md`**

```markdown
# ML Feature Pipeline

Computes and serves ML features with a clear split: an offline store (historical features on object storage) for model training, and an online store (Redis or key-value) for real-time model serving.

## When to use

- ML models require features at serving time with millisecond latency (online store)
- Training data must be consistent with serving features (point-in-time correctness)
- Feature logic must be shared between training and serving (no training/serving skew)
- Team owns ML model training and serving, not just data pipelines

## When NOT to use

- Models use only static features (no time-varying inputs) — batch inference from a lakehouse table is sufficient
- No real-time serving — offline batch inference does not need an online store
- ML platform is managed (Databricks Feature Store, Vertex AI Feature Store) — use the platform's native feature API

## Variants

| Variant | Stack | Use when |
|---|---|---|
| [feast-redis-spark](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/ml-feature-pipeline/variants/feast-redis-spark) | Feast 0.40 + Redis + Spark 3.5 | Want a feature store framework managing both stores |
| [flink-spark-offline](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/ml-feature-pipeline/variants/flink-spark-offline) | Flink 1.19 (online) + Spark 3.5 (offline) | Want to own the online/offline mechanics without a framework |
| [feast-spark-airflow](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/ml-feature-pipeline/variants/feast-spark-airflow) | Feast + Spark + Airflow 2.9 | Feast + scheduled materialisation with orchestration |

## Related patterns

- [Streaming Lakehouse](streaming-lakehouse.md) — the offline store feeds from a streaming lakehouse in production
- [Workflow Orchestration](workflow-orchestration.md) — for scheduling feature materialisation jobs
```

- [ ] **Step 8: Write `docs/patterns/graph-processing.md`**

```markdown
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
```

- [ ] **Step 9: Write `docs/patterns/workflow-orchestration.md`**

```markdown
# Workflow Orchestration

Airflow, Prefect, or Dagster schedule and coordinate the tasks that make up a data pipeline. Orchestration is the glue between every other pattern — a batch lakehouse job is just a Spark submit until Airflow gives it a schedule, retry logic, and an SLA alert.

## When to use

- Any batch or scheduled pipeline needs retry on failure, dependency management, or backfill
- Multiple pipeline steps must be coordinated (extract → transform → load → notify)
- Operational visibility (which runs succeeded, which failed, how long they took) is required
- SLA alerting on pipeline completion time is needed

## When NOT to use

- Your pipeline is a continuous streaming job (Flink/Spark Streaming) — streaming jobs are long-running processes, not scheduled tasks; use a process supervisor (Kubernetes Deployment, systemd) instead
- You have a single script with no dependencies — a cron job is sufficient

## Variants

| Variant | Stack | Key differentiator | Use when |
|---|---|---|---|
| [airflow](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/workflow-orchestration/variants/airflow) | Airflow 2.9 (LocalExecutor) | 600+ providers, mature ecosystem | Large org, many integrations, existing Airflow investment |
| [prefect](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/workflow-orchestration/variants/prefect) | Prefect 2.x | Python-native flows, dynamic tasks | Python-first team, modern UX, dynamic task generation |
| [dagster](https://github.com/naren-chakraview/chakraview-data-engineering-patterns/tree/main/patterns/workflow-orchestration/variants/dagster) | Dagster 1.x | Software-defined assets, lineage graph | Asset-centric thinking, strong observability requirements |

## Related patterns

- [Batch Lakehouse](batch-lakehouse.md) — the spark-iceberg-airflow variant integrates Airflow directly
- [ELT / Warehouse](elt-warehouse.md) — the dbt-duckdb-airflow variant integrates Airflow directly
- [ML Feature Pipeline](ml-feature-pipeline.md) — the feast-spark-airflow variant integrates Airflow directly
```

- [ ] **Step 10: Verify MkDocs builds cleanly with all pages**

```bash
mkdocs build --strict 2>&1 | grep -E "(ERROR|built successfully)"
```

Expected: `INFO - Documentation built successfully.`

- [ ] **Step 11: Commit**

```bash
git add docs/patterns/
git commit -m "docs: all 9 pattern pages with fit notes, variants table, and cross-references"
```

---

---

## Tasks 11–15

---

### Task 11: batch-lakehouse (Pattern README + 3 Variants)

**Files:**
- Create: `patterns/batch-lakehouse/README.md`
- Create: `patterns/batch-lakehouse/variants/spark-iceberg/README.md`
- Create: `patterns/batch-lakehouse/variants/spark-iceberg/build.sbt`
- Create: `patterns/batch-lakehouse/variants/spark-iceberg/src/main/scala/io/chakraview/lakehouse/Main.scala`
- Create: `patterns/batch-lakehouse/variants/spark-iceberg/src/main/scala/io/chakraview/lakehouse/IcebergWriter.scala`
- Create: `patterns/batch-lakehouse/variants/spark-iceberg/src/main/scala/io/chakraview/lakehouse/SparkSessions.scala`
- Create: `patterns/batch-lakehouse/variants/spark-iceberg/docker-compose.yml`
- Create: `patterns/batch-lakehouse/variants/spark-iceberg/.env.example`
- Create: `patterns/batch-lakehouse/variants/spark-delta/README.md`
- Create: `patterns/batch-lakehouse/variants/spark-delta/build.sbt`
- Create: `patterns/batch-lakehouse/variants/spark-delta/src/main/scala/io/chakraview/lakehouse/Main.scala`
- Create: `patterns/batch-lakehouse/variants/spark-delta/src/main/scala/io/chakraview/lakehouse/DeltaWriter.scala`
- Create: `patterns/batch-lakehouse/variants/spark-delta/docker-compose.yml`
- Create: `patterns/batch-lakehouse/variants/spark-delta/.env.example`
- Create: `patterns/batch-lakehouse/variants/spark-iceberg-airflow/README.md`
- Create: `patterns/batch-lakehouse/variants/spark-iceberg-airflow/build.sbt`
- Create: `patterns/batch-lakehouse/variants/spark-iceberg-airflow/src/main/scala/io/chakraview/lakehouse/Main.scala`
- Create: `patterns/batch-lakehouse/variants/spark-iceberg-airflow/config/dags/batch_lakehouse_dag.py`
- Create: `patterns/batch-lakehouse/variants/spark-iceberg-airflow/docker-compose.yml`
- Create: `patterns/batch-lakehouse/variants/spark-iceberg-airflow/.env.example`

- [ ] **Step 1: Create `patterns/batch-lakehouse/README.md`**

```markdown
# Batch Lakehouse

Scheduled Spark jobs read from source, transform, and write to an open table format on object storage.

## Variants

| Variant | Table format | Orchestration | Pull branch |
|---|---|---|---|
| `spark-iceberg` | Iceberg 1.5 | None (run manually or via cron) | `pattern/batch-lakehouse/spark-iceberg` |
| `spark-delta` | Delta Lake 3.1 | None | `pattern/batch-lakehouse/spark-delta` |
| `spark-iceberg-airflow` | Iceberg 1.5 | Airflow 2.9 DAG | `pattern/batch-lakehouse/spark-iceberg-airflow` |

## Choosing between variants

- **spark-iceberg**: default choice. Iceberg is read by Trino, Presto, Flink, DuckDB, and Athena without extra configuration.
- **spark-delta**: choose when the team lives in Databricks or needs `DeltaTable.merge()` API ergonomics.
- **spark-iceberg-airflow**: choose when you need scheduled retry, SLA alerting, and backfill out of the box.

## Prerequisites

- Scala 2.12, SBT 1.9+
- Java 11+
- Spark 3.5 installed locally (`spark-submit` on PATH), or use `spark-shell` for interactive exploration
- Docker + Docker Compose (for local MinIO / Iceberg REST / Airflow)
```

- [ ] **Step 2: Create `patterns/batch-lakehouse/variants/spark-iceberg/build.sbt`**

```scala
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "io.chakraview"

val sparkVersion   = "3.5.1"
val icebergVersion = "1.5.2"
val hadoopVersion  = "3.3.6"

lazy val root = (project in file("."))
  .settings(
    name := "batch-lakehouse-spark-iceberg",
    libraryDependencies ++= Seq(
      "org.apache.spark"  %% "spark-core"                     % sparkVersion   % "provided",
      "org.apache.spark"  %% "spark-sql"                      % sparkVersion   % "provided",
      "org.apache.iceberg"  % "iceberg-spark-runtime-3.5_2.12" % icebergVersion,
      "org.apache.hadoop"   % "hadoop-aws"                    % hadoopVersion  % "provided",
      "com.amazonaws"       % "aws-java-sdk-bundle"           % "1.12.262"     % "provided",
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case "reference.conf"         => MergeStrategy.concat
      case _                        => MergeStrategy.first
    },
  )
```

- [ ] **Step 3: Create `SparkSessions.scala`**

```scala
package io.chakraview.lakehouse

import org.apache.spark.sql.SparkSession

object SparkSessions {
  def iceberg(appName: String): SparkSession =
    SparkSession.builder()
      .appName(appName)
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.lakehouse.type", "rest")
      .config("spark.sql.catalog.lakehouse.uri",
        sys.env.getOrElse("ICEBERG_REST_URI", "http://localhost:8181"))
      .config("spark.sql.catalog.lakehouse.warehouse",
        sys.env.getOrElse("LAKEHOUSE_WAREHOUSE", "s3a://chakra-lakehouse/"))
      .config("spark.hadoop.fs.s3a.endpoint",
        sys.env.getOrElse("S3_ENDPOINT", "http://localhost:9000"))
      .config("spark.hadoop.fs.s3a.access.key",
        sys.env.getOrElse("AWS_ACCESS_KEY_ID", "minioadmin"))
      .config("spark.hadoop.fs.s3a.secret.key",
        sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", "minioadmin"))
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate()
}
```

- [ ] **Step 4: Create `IcebergWriter.scala`**

```scala
package io.chakraview.lakehouse

import org.apache.spark.sql.{DataFrame, SparkSession}

object IcebergWriter {

  def append(df: DataFrame, table: String): Unit =
    df.writeTo(table)
      .option("write.format.default", "parquet")
      .option("write.parquet.compression-codec", "snappy")
      .createOrReplace()

  def upsert(spark: SparkSession, df: DataFrame, table: String, idCol: String): Unit = {
    df.createOrReplaceTempView("__source")
    spark.sql(s"""
      MERGE INTO $table t
      USING __source s ON t.$idCol = s.$idCol
      WHEN MATCHED     THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """)
  }
}
```

- [ ] **Step 5: Create `Main.scala` (spark-iceberg)**

```scala
package io.chakraview.lakehouse

import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessions.iceberg("batch-lakehouse-iceberg")
    import spark.implicits._

    // 1. Read source CSV from object storage
    val source = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(s"s3a://chakra-lakehouse/raw/orders/")

    // 2. Transform
    val transformed = source
      .withColumn("processed_at", current_timestamp())
      .withColumn("amount_usd", col("amount_cents").divide(100.0))
      .filter(col("order_id").isNotNull)

    // 3. Write to Iceberg (create-or-replace for idempotent batch runs)
    IcebergWriter.append(transformed, "lakehouse.orders.processed")

    println(s"Wrote ${transformed.count()} rows to lakehouse.orders.processed")
    spark.stop()
  }
}
```

- [ ] **Step 6: Create `patterns/batch-lakehouse/variants/spark-iceberg/docker-compose.yml`**

```yaml
version: "3.9"

services:
  minio:
    image: minio/minio:RELEASE.2024-04-06T05-26-02Z
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: ${AWS_ACCESS_KEY_ID:-minioadmin}
      MINIO_ROOT_PASSWORD: ${AWS_SECRET_ACCESS_KEY:-minioadmin}
    volumes:
      - minio-data:/data
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 10s
      retries: 5

  minio-init:
    image: minio/mc:latest
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      sh -c "
        mc alias set local http://minio:9000 minioadmin minioadmin &&
        mc mb --ignore-existing local/chakra-lakehouse &&
        mc mb --ignore-existing local/chakra-lakehouse/raw/orders
      "

  iceberg-rest:
    image: tabulario/iceberg-rest:0.10.0
    depends_on:
      minio:
        condition: service_healthy
    ports:
      - "8181:8181"
    environment:
      CATALOG_WAREHOUSE: s3://chakra-lakehouse/
      CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:-minioadmin}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY:-minioadmin}
      AWS_REGION: ${AWS_REGION:-us-east-1}
      CATALOG_S3_ENDPOINT: http://minio:9000
      CATALOG_S3_PATH__STYLE__ACCESS: "true"

volumes:
  minio-data:
```

- [ ] **Step 7: Create `patterns/batch-lakehouse/variants/spark-iceberg/.env.example`**

```bash
# MinIO (local S3 substitute)
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
S3_ENDPOINT=http://localhost:9000

# Iceberg REST catalog (started by docker-compose)
ICEBERG_REST_URI=http://localhost:8181
LAKEHOUSE_WAREHOUSE=s3a://chakra-lakehouse/
```

- [ ] **Step 8: Create `patterns/batch-lakehouse/variants/spark-iceberg/README.md`**

```markdown
# Batch Lakehouse — Spark + Iceberg

## When to use
Multi-engine environment: Trino, Presto, Athena, or DuckDB must read the same tables Spark writes.
Portability matters more than deepest Spark integration.

## When NOT to use
Your team is Databricks-only and portability is not a concern — use spark-delta instead.

## Trade-offs vs spark-delta
- **Iceberg**: any engine reads/writes; broader schema evolution; REST catalog required
- **Delta**: simpler local setup (no REST catalog); `DeltaTable.merge()` is more ergonomic; best on Databricks

## How to run locally

```bash
cp .env.example .env
docker compose up -d          # starts MinIO + Iceberg REST catalog
sbt assembly                  # builds fat JAR
spark-submit \
  --class io.chakraview.lakehouse.Main \
  --master local[*] \
  target/scala-2.12/batch-lakehouse-spark-iceberg-assembly-0.1.0.jar
```

MinIO console: http://localhost:9001 (minioadmin / minioadmin)
Iceberg REST: http://localhost:8181
```

- [ ] **Step 9: Create `spark-delta` variant — `build.sbt`**

```scala
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "io.chakraview"

val sparkVersion  = "3.5.1"
val deltaVersion  = "3.1.0"
val hadoopVersion = "3.3.6"

lazy val root = (project in file("."))
  .settings(
    name := "batch-lakehouse-spark-delta",
    libraryDependencies ++= Seq(
      "org.apache.spark"  %% "spark-core"         % sparkVersion  % "provided",
      "org.apache.spark"  %% "spark-sql"          % sparkVersion  % "provided",
      "io.delta"          %% "delta-spark"        % deltaVersion,
      "org.apache.hadoop"  % "hadoop-aws"         % hadoopVersion % "provided",
      "com.amazonaws"      % "aws-java-sdk-bundle" % "1.12.262"   % "provided",
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case "reference.conf"         => MergeStrategy.concat
      case _                        => MergeStrategy.first
    },
  )
```

- [ ] **Step 10: Create `spark-delta/src/main/scala/io/chakraview/lakehouse/DeltaWriter.scala`**

```scala
package io.chakraview.lakehouse

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}

object DeltaWriter {

  def append(df: DataFrame, path: String): Unit =
    df.write
      .format("delta")
      .mode("append")
      .save(path)

  def merge(spark: SparkSession, df: DataFrame, path: String, idCol: String): Unit = {
    if (!DeltaTable.isDeltaTable(spark, path)) {
      df.write.format("delta").save(path)
      return
    }
    DeltaTable.forPath(spark, path)
      .alias("target")
      .merge(df.alias("source"), s"target.$idCol = source.$idCol")
      .whenMatched().updateAll()
      .whenNotMatched().insertAll()
      .execute()
  }
}
```

- [ ] **Step 11: Create `spark-delta/src/main/scala/io/chakraview/lakehouse/Main.scala`**

```scala
package io.chakraview.lakehouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  private val TablePath =
    sys.env.getOrElse("DELTA_TABLE_PATH", "s3a://chakra-lakehouse/delta/orders/processed")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("batch-lakehouse-delta")
      .config("spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.hadoop.fs.s3a.endpoint",
        sys.env.getOrElse("S3_ENDPOINT", "http://localhost:9000"))
      .config("spark.hadoop.fs.s3a.access.key",
        sys.env.getOrElse("AWS_ACCESS_KEY_ID", "minioadmin"))
      .config("spark.hadoop.fs.s3a.secret.key",
        sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", "minioadmin"))
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate()

    val source = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("s3a://chakra-lakehouse/raw/orders/")

    val transformed = source
      .withColumn("processed_at", current_timestamp())
      .withColumn("amount_usd", col("amount_cents").divide(100.0))
      .filter(col("order_id").isNotNull)

    // Merge: upsert by order_id so the job is idempotent on re-run
    DeltaWriter.merge(spark, transformed, TablePath, "order_id")

    println(s"Merged ${transformed.count()} rows into $TablePath")
    spark.stop()
  }
}
```

- [ ] **Step 12: Create `spark-delta/docker-compose.yml`**

```yaml
version: "3.9"

services:
  minio:
    image: minio/minio:RELEASE.2024-04-06T05-26-02Z
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: ${AWS_ACCESS_KEY_ID:-minioadmin}
      MINIO_ROOT_PASSWORD: ${AWS_SECRET_ACCESS_KEY:-minioadmin}
    volumes:
      - minio-data:/data
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 10s
      retries: 5

  minio-init:
    image: minio/mc:latest
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      sh -c "
        mc alias set local http://minio:9000 minioadmin minioadmin &&
        mc mb --ignore-existing local/chakra-lakehouse
      "

volumes:
  minio-data:
```

- [ ] **Step 13: Create `spark-delta/.env.example`**

```bash
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
S3_ENDPOINT=http://localhost:9000
DELTA_TABLE_PATH=s3a://chakra-lakehouse/delta/orders/processed
```

- [ ] **Step 14: Create `spark-delta/README.md`**

```markdown
# Batch Lakehouse — Spark + Delta Lake

## When to use
Databricks-first stack. Want `DeltaTable.merge()` ergonomics and tightest Spark integration.

## When NOT to use
Queries from Trino, Presto, or non-Spark engines — use spark-iceberg instead.

## Trade-offs vs spark-iceberg
- **Delta**: simpler setup (no REST catalog); best `MERGE` ergonomics on Spark; Databricks-native
- **Iceberg**: reads from any engine; more portable; requires REST catalog config

## How to run locally

```bash
cp .env.example .env
docker compose up -d
sbt assembly
spark-submit \
  --class io.chakraview.lakehouse.Main \
  --master local[*] \
  target/scala-2.12/batch-lakehouse-spark-delta-assembly-0.1.0.jar
```
```

- [ ] **Step 15: Create `spark-iceberg-airflow` variant — `config/dags/batch_lakehouse_dag.py`**

```python
"""
Batch Lakehouse DAG — Spark + Iceberg + Airflow

Runs the Spark job daily, retries twice on failure, alerts on SLA miss.
Requires the Spark app JAR to be present at SPARK_APP_JAR (set in Airflow Variables).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(hours=2),
}

with DAG(
    dag_id="batch_lakehouse_iceberg",
    default_args=default_args,
    description="Daily Spark batch job writing orders to Iceberg",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["batch", "lakehouse", "iceberg"],
) as dag:

    ingest = SparkSubmitOperator(
        task_id="ingest_orders_to_iceberg",
        application=Variable.get("spark_app_jar",
            default_var="/opt/spark-apps/batch-lakehouse-spark-iceberg-assembly-0.1.0.jar"),
        name="batch-lakehouse-iceberg-{{ ds }}",
        conf={
            "spark.sql.extensions":
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.lakehouse":
                "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.lakehouse.type": "rest",
            "spark.sql.catalog.lakehouse.uri":
                Variable.get("iceberg_rest_uri", default_var="http://iceberg-rest:8181"),
            "spark.hadoop.fs.s3a.path.style.access": "true",
        },
        env_vars={
            "ICEBERG_REST_URI":     Variable.get("iceberg_rest_uri",    default_var="http://iceberg-rest:8181"),
            "LAKEHOUSE_WAREHOUSE":  Variable.get("lakehouse_warehouse",  default_var="s3a://chakra-lakehouse/"),
            "S3_ENDPOINT":          Variable.get("s3_endpoint",          default_var="http://minio:9000"),
            "AWS_ACCESS_KEY_ID":    Variable.get("aws_access_key_id",    default_var="minioadmin"),
            "AWS_SECRET_ACCESS_KEY": Variable.get("aws_secret_access_key", default_var="minioadmin"),
        },
    )
```

- [ ] **Step 16: Create `spark-iceberg-airflow/docker-compose.yml`**

```yaml
version: "3.9"

services:
  minio:
    image: minio/minio:RELEASE.2024-04-06T05-26-02Z
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: ${AWS_ACCESS_KEY_ID:-minioadmin}
      MINIO_ROOT_PASSWORD: ${AWS_SECRET_ACCESS_KEY:-minioadmin}
    volumes:
      - minio-data:/data
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 10s
      retries: 5

  minio-init:
    image: minio/mc:latest
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      sh -c "
        mc alias set local http://minio:9000 minioadmin minioadmin &&
        mc mb --ignore-existing local/chakra-lakehouse
      "

  iceberg-rest:
    image: tabulario/iceberg-rest:0.10.0
    depends_on:
      minio:
        condition: service_healthy
    ports:
      - "8181:8181"
    environment:
      CATALOG_WAREHOUSE: s3://chakra-lakehouse/
      CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:-minioadmin}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY:-minioadmin}
      AWS_REGION: ${AWS_REGION:-us-east-1}
      CATALOG_S3_ENDPOINT: http://minio:9000
      CATALOG_S3_PATH__STYLE__ACCESS: "true"

  airflow:
    image: apache/airflow:2.9.3-python3.11
    command: standalone
    depends_on:
      minio:
        condition: service_healthy
    ports:
      - "8080:8080"
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: sqlite:////opt/airflow/airflow.db
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
      AIRFLOW__WEBSERVER__SECRET_KEY: ${AIRFLOW_SECRET_KEY:-dev-secret-change-in-prod}
    volumes:
      - ./config/dags:/opt/airflow/dags
      - airflow-logs:/opt/airflow/logs

volumes:
  minio-data:
  airflow-logs:
```

- [ ] **Step 17: Create `spark-iceberg-airflow/.env.example`**

```bash
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
S3_ENDPOINT=http://localhost:9000
ICEBERG_REST_URI=http://localhost:8181
LAKEHOUSE_WAREHOUSE=s3a://chakra-lakehouse/
AIRFLOW_SECRET_KEY=dev-secret-change-in-prod
```

- [ ] **Step 18: Create `spark-iceberg-airflow/README.md`**

```markdown
# Batch Lakehouse — Spark + Iceberg + Airflow

## When to use
Scheduled retry, SLA alerting, and backfill are required. Team already has or wants Airflow.

## When NOT to use
No scheduling needed — use spark-iceberg instead (simpler, no Airflow overhead).

## How to run locally

```bash
cp .env.example .env
sbt assembly
cp target/scala-2.12/batch-lakehouse-spark-iceberg-assembly-0.1.0.jar \
   /tmp/spark-apps/   # path Airflow picks up via Variable spark_app_jar
docker compose up -d
```

Airflow UI: http://localhost:8080 (admin / admin on first run)
Trigger DAG: `batch_lakehouse_iceberg`
MinIO console: http://localhost:9001
```

- [ ] **Step 19: Verify all required files exist for all three variants**

```bash
for variant in spark-iceberg spark-delta spark-iceberg-airflow; do
  for f in README.md docker-compose.yml .env.example; do
    path="patterns/batch-lakehouse/variants/$variant/$f"
    [ -f "$path" ] || { echo "MISSING: $path"; exit 1; }
  done
done
echo "All batch-lakehouse variant files present"
```

Expected: `All batch-lakehouse variant files present`

- [ ] **Step 20: Commit**

```bash
git add patterns/batch-lakehouse/
git commit -m "feat: batch-lakehouse pattern — spark-iceberg, spark-delta, spark-iceberg-airflow variants"
```

---

### Task 12: streaming-lakehouse (Pattern README + 2 Variants)

**Files:**
- Create: `patterns/streaming-lakehouse/README.md`
- Create: `patterns/streaming-lakehouse/variants/flink-iceberg/{README.md,pom.xml,src/,docker-compose.yml,.env.example}`
- Create: `patterns/streaming-lakehouse/variants/spark-delta/{README.md,build.sbt,src/,docker-compose.yml,.env.example}`

- [ ] **Step 1: Create `patterns/streaming-lakehouse/README.md`**

```markdown
# Streaming Lakehouse

Continuous pipeline reads Kafka events, processes them, writes to a lakehouse with sub-minute latency.

## Variants

| Variant | Engine | Table format | Latency floor | Pull branch |
|---|---|---|---|---|
| `flink-iceberg` | Flink 1.19 (Java) | Iceberg 1.5 | ~5s (checkpoint) | `pattern/streaming-lakehouse/flink-iceberg` |
| `spark-delta` | Spark 3.5 Structured Streaming (Scala) | Delta Lake 3.1 | ~30–60s (trigger) | `pattern/streaming-lakehouse/spark-delta` |

## Choosing between variants

- **flink-iceberg**: exactly-once per-event, native event-time watermarking, sub-minute freshness. Higher operational complexity (Flink cluster + Kafka).
- **spark-delta**: micro-batch, latency floor ~30s, simpler if team already knows Spark SQL.

## Prerequisites

- Docker + Docker Compose (starts Kafka + Flink or Kafka + MinIO)
- Java 17 + Maven 3.9 (flink-iceberg) OR Scala 2.12 + SBT 1.9 (spark-delta)
- A running Kafka topic `events.orders` (docker-compose creates it automatically)
```

- [ ] **Step 2: Create `patterns/streaming-lakehouse/variants/flink-iceberg/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>io.chakraview</groupId>
  <artifactId>streaming-lakehouse-flink-iceberg</artifactId>
  <version>0.1.0</version>
  <packaging>jar</packaging>

  <properties>
    <java.version>17</java.version>
    <flink.version>1.19.0</flink.version>
    <iceberg.version>1.5.2</iceberg.version>
    <kafka.connector.version>3.1.0-1.19</kafka.connector.version>
    <maven.compiler.source>17</maven.compiler.source>
    <maven.compiler.target>17</maven.compiler.target>
  </properties>

  <dependencies>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-streaming-java</artifactId>
      <version>${flink.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-connector-kafka</artifactId>
      <version>${kafka.connector.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.flink</groupId>
      <artifactId>flink-json</artifactId>
      <version>${flink.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.iceberg</groupId>
      <artifactId>iceberg-flink-runtime-1.19</artifactId>
      <version>${iceberg.version}</version>
    </dependency>
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-aws</artifactId>
      <version>3.3.6</version>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.5.1</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
              <filters>
                <filter>
                  <artifact>*:*</artifact>
                  <excludes>
                    <exclude>META-INF/*.SF</exclude>
                    <exclude>META-INF/*.DSA</exclude>
                    <exclude>META-INF/*.RSA</exclude>
                  </excludes>
                </filter>
              </filters>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

- [ ] **Step 3: Create `flink-iceberg/src/main/java/io/chakraview/streaming/Main.java`**

```java
package io.chakraview.streaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Exactly-once: Flink checkpoint + Iceberg two-phase commit are atomic.
        // Files committed to Iceberg only on checkpoint — no partial writes visible.
        env.enableCheckpointing(60_000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000);
        env.getCheckpointConfig().setCheckpointTimeout(120_000);

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(System.getenv("KAFKA_BOOTSTRAP_SERVERS"))
            .setTopics("events.orders")
            .setGroupId("flink-streaming-lakehouse")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> rawStream = env.fromSource(
            source,
            WatermarkStrategy.<String>forMonotonousTimestamps()
                .withTimestampAssigner((event, ts) -> EventParser.extractTimestamp(event)),
            "kafka-orders-source"
        );

        DataStream<Row> rows = rawStream.map(new EventParser());

        // Iceberg REST catalog backed by MinIO
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("type",       "rest");
        catalogProps.put("uri",        System.getenv("ICEBERG_REST_URI"));
        catalogProps.put("warehouse",  System.getenv("LAKEHOUSE_WAREHOUSE"));
        catalogProps.put("io-impl",    "org.apache.iceberg.aws.s3.S3FileIO");
        catalogProps.put("s3.endpoint", System.getenv("S3_ENDPOINT"));
        catalogProps.put("s3.path-style-access", "true");

        CatalogLoader catalogLoader = CatalogLoader.custom(
            "lakehouse", catalogProps,
            new org.apache.hadoop.conf.Configuration(),
            "org.apache.iceberg.rest.RESTCatalog"
        );
        TableLoader tableLoader = TableLoader.fromCatalog(
            catalogLoader,
            TableIdentifier.of("orders", "events")
        );

        FlinkSink.forRow(rows, IcebergSchema.ORDERS_SCHEMA)
            .tableLoader(tableLoader)
            .upsert(false)
            .append();

        env.execute("streaming-lakehouse-flink-iceberg");
    }
}
```

- [ ] **Step 4: Create `flink-iceberg/src/main/java/io/chakraview/streaming/EventParser.java`**

```java
package io.chakraview.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

public class EventParser implements MapFunction<String, Row> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Row map(String json) throws Exception {
        JsonNode node = MAPPER.readTree(json);
        Row row = new Row(5);
        row.setField(0, node.get("order_id").asText());
        row.setField(1, node.get("customer_id").asText());
        row.setField(2, node.get("amount_cents").asLong());
        row.setField(3, node.get("status").asText());
        row.setField(4, node.get("placed_at").asLong());   // epoch millis
        return row;
    }

    public static long extractTimestamp(String json) {
        try {
            return MAPPER.readTree(json).get("placed_at").asLong();
        } catch (Exception e) {
            return System.currentTimeMillis();
        }
    }
}
```

- [ ] **Step 5: Create `flink-iceberg/src/main/java/io/chakraview/streaming/IcebergSchema.java`**

```java
package io.chakraview.streaming;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

public final class IcebergSchema {
    private IcebergSchema() {}

    public static final Schema ORDERS_SCHEMA = new Schema(
        Types.NestedField.required(1, "order_id",    Types.StringType.get()),
        Types.NestedField.required(2, "customer_id", Types.StringType.get()),
        Types.NestedField.required(3, "amount_cents", Types.LongType.get()),
        Types.NestedField.optional(4, "status",      Types.StringType.get()),
        Types.NestedField.required(5, "placed_at",   Types.TimestampType.withZone())
    );
}
```

- [ ] **Step 6: Create `flink-iceberg/docker-compose.yml`**

```yaml
version: "3.9"

services:
  kafka:
    image: bitnami/kafka:3.7.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_CFG_NODE_ID: 1
      KAFKA_CFG_PROCESS_ROLES: controller,broker
      KAFKA_CFG_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: "true"
    healthcheck:
      test: ["CMD", "kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--list"]
      interval: 15s
      retries: 5

  minio:
    image: minio/minio:RELEASE.2024-04-06T05-26-02Z
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: ${AWS_ACCESS_KEY_ID:-minioadmin}
      MINIO_ROOT_PASSWORD: ${AWS_SECRET_ACCESS_KEY:-minioadmin}
    volumes:
      - minio-data:/data
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 10s
      retries: 5

  minio-init:
    image: minio/mc:latest
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      sh -c "
        mc alias set local http://minio:9000 minioadmin minioadmin &&
        mc mb --ignore-existing local/chakra-lakehouse
      "

  iceberg-rest:
    image: tabulario/iceberg-rest:0.10.0
    depends_on:
      minio:
        condition: service_healthy
    ports:
      - "8181:8181"
    environment:
      CATALOG_WAREHOUSE: s3://chakra-lakehouse/
      CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:-minioadmin}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY:-minioadmin}
      AWS_REGION: ${AWS_REGION:-us-east-1}
      CATALOG_S3_ENDPOINT: http://minio:9000
      CATALOG_S3_PATH__STYLE__ACCESS: "true"

  flink-jobmanager:
    image: flink:1.19-java17
    command: jobmanager
    ports:
      - "8082:8081"
    environment:
      JOB_MANAGER_RPC_ADDRESS: flink-jobmanager
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/overview"]
      interval: 15s
      retries: 5

  flink-taskmanager:
    image: flink:1.19-java17
    command: taskmanager
    depends_on:
      flink-jobmanager:
        condition: service_healthy
    environment:
      JOB_MANAGER_RPC_ADDRESS: flink-jobmanager
      TASK_MANAGER_NUMBER_OF_TASK_SLOTS: 4

volumes:
  minio-data:
```

- [ ] **Step 7: Create `flink-iceberg/.env.example` and `flink-iceberg/README.md`**

`.env.example`:
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
S3_ENDPOINT=http://localhost:9000
ICEBERG_REST_URI=http://localhost:8181
LAKEHOUSE_WAREHOUSE=s3://chakra-lakehouse/
```

`README.md`:
```markdown
# Streaming Lakehouse — Flink + Iceberg

## When to use
Sub-minute latency, exactly-once semantics, native event-time windowing required.

## When NOT to use
Team has no Flink experience and latency > 30s is acceptable — use spark-delta instead.

## How to run locally

```bash
cp .env.example .env
docker compose up -d
mvn package -DskipTests
# Submit to local Flink cluster
flink run -m localhost:8082 \
  target/streaming-lakehouse-flink-iceberg-0.1.0.jar
```

Flink UI: http://localhost:8082
MinIO console: http://localhost:9001
Iceberg REST: http://localhost:8181

Produce test events to `events.orders`:
```bash
docker exec -it <kafka-container> \
  kafka-console-producer.sh --bootstrap-server localhost:9092 --topic events.orders
```
```

- [ ] **Step 8: Create `spark-delta` streaming variant files**

`build.sbt`:
```scala
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "io.chakraview"

val sparkVersion  = "3.5.1"
val deltaVersion  = "3.1.0"
val kafkaVersion  = "0-10"

lazy val root = (project in file("."))
  .settings(
    name := "streaming-lakehouse-spark-delta",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"               % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"                % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql-kafka-0-10"     % sparkVersion,
      "io.delta"         %% "delta-spark"              % deltaVersion,
      "org.apache.hadoop"  % "hadoop-aws"              % "3.3.6"      % "provided",
      "com.amazonaws"      % "aws-java-sdk-bundle"     % "1.12.262"   % "provided",
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case "reference.conf"         => MergeStrategy.concat
      case _                        => MergeStrategy.first
    },
  )
```

`src/main/scala/io/chakraview/streaming/Main.scala`:
```scala
package io.chakraview.streaming

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

object Main {
  private val TablePath =
    sys.env.getOrElse("DELTA_TABLE_PATH", "s3a://chakra-lakehouse/delta/orders/events")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("streaming-lakehouse-spark-delta")
      .config("spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.hadoop.fs.s3a.endpoint",
        sys.env.getOrElse("S3_ENDPOINT", "http://localhost:9000"))
      .config("spark.hadoop.fs.s3a.access.key",
        sys.env.getOrElse("AWS_ACCESS_KEY_ID", "minioadmin"))
      .config("spark.hadoop.fs.s3a.secret.key",
        sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", "minioadmin"))
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate()

    import spark.implicits._

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",
        sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
      .option("subscribe", "events.orders")
      .option("startingOffsets", "earliest")
      .load()

    val orders = raw.select(from_json(
        col("value").cast("string"),
        EventSchema.schema
      ).alias("data")).select("data.*")
      .withColumn("ingested_at", current_timestamp())

    // foreachBatch: merge each micro-batch into Delta (idempotent upsert by order_id)
    val query = orders.writeStream
      .trigger(Trigger.ProcessingTime(60.seconds))
      .option("checkpointLocation",
        sys.env.getOrElse("CHECKPOINT_PATH", "s3a://chakra-lakehouse/checkpoints/orders-streaming/"))
      .foreachBatch { (batchDf: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: Long) =>
        if (!batchDf.isEmpty) {
          if (!DeltaTable.isDeltaTable(spark, TablePath)) {
            batchDf.write.format("delta").save(TablePath)
          } else {
            DeltaTable.forPath(spark, TablePath)
              .alias("target")
              .merge(batchDf.alias("source"), "target.order_id = source.order_id")
              .whenMatched().updateAll()
              .whenNotMatched().insertAll()
              .execute()
          }
        }
      }
      .start()

    query.awaitTermination()
  }
}
```

`src/main/scala/io/chakraview/streaming/EventSchema.scala`:
```scala
package io.chakraview.streaming

import org.apache.spark.sql.types._

object EventSchema {
  val schema: StructType = StructType(Seq(
    StructField("order_id",     StringType,    nullable = false),
    StructField("customer_id",  StringType,    nullable = false),
    StructField("amount_cents", LongType,      nullable = false),
    StructField("status",       StringType,    nullable = true),
    StructField("placed_at",    TimestampType, nullable = false),
  ))
}
```

`docker-compose.yml` — Kafka + MinIO (no Flink cluster, Spark runs locally):
```yaml
version: "3.9"

services:
  kafka:
    image: bitnami/kafka:3.7.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_CFG_NODE_ID: 1
      KAFKA_CFG_PROCESS_ROLES: controller,broker
      KAFKA_CFG_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: "true"

  minio:
    image: minio/minio:RELEASE.2024-04-06T05-26-02Z
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: ${AWS_ACCESS_KEY_ID:-minioadmin}
      MINIO_ROOT_PASSWORD: ${AWS_SECRET_ACCESS_KEY:-minioadmin}
    volumes:
      - minio-data:/data

volumes:
  minio-data:
```

`.env.example`:
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
S3_ENDPOINT=http://localhost:9000
DELTA_TABLE_PATH=s3a://chakra-lakehouse/delta/orders/events
CHECKPOINT_PATH=s3a://chakra-lakehouse/checkpoints/orders-streaming/
```

`README.md`:
```markdown
# Streaming Lakehouse — Spark Structured Streaming + Delta Lake

## When to use
Team knows Spark. Latency > 30 seconds acceptable. Databricks-compatible Delta format preferred.

## When NOT to use
Sub-minute latency or exactly-once event-time semantics required — use flink-iceberg instead.

## How to run locally

```bash
cp .env.example .env
docker compose up -d
sbt assembly
spark-submit \
  --class io.chakraview.streaming.Main \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  target/scala-2.12/streaming-lakehouse-spark-delta-assembly-0.1.0.jar
```
```

- [ ] **Step 9: Verify required files for both variants**

```bash
for variant in flink-iceberg spark-delta; do
  for f in README.md docker-compose.yml .env.example; do
    path="patterns/streaming-lakehouse/variants/$variant/$f"
    [ -f "$path" ] || { echo "MISSING: $path"; exit 1; }
  done
done
echo "All streaming-lakehouse variant files present"
```

- [ ] **Step 10: Commit**

```bash
git add patterns/streaming-lakehouse/
git commit -m "feat: streaming-lakehouse pattern — flink-iceberg (Java) and spark-delta (Scala) variants"
```

---

### Task 13: elt-warehouse (Pattern README + 3 Variants)

**Files:**
- Create: `patterns/elt-warehouse/README.md`
- Per variant (`dbt-snowflake`, `dbt-bigquery`, `dbt-duckdb-airflow`): `README.md`, `dbt_project.yml`, `profiles.yml`, `packages.yml`, `models/staging/stg_orders.sql`, `models/marts/orders_summary.sql`, `.env.example`
- `dbt-duckdb-airflow` additionally: `dags/dbt_dag.py`, `docker-compose.yml`

- [ ] **Step 1: Create `patterns/elt-warehouse/README.md`**

```markdown
# ELT / Warehouse

dbt transforms data already in a warehouse. No distributed compute — the warehouse handles scale.

## Variants

| Variant | Warehouse | Pull branch |
|---|---|---|
| `dbt-snowflake` | Snowflake | `pattern/elt-warehouse/dbt-snowflake` |
| `dbt-bigquery` | BigQuery | `pattern/elt-warehouse/dbt-bigquery` |
| `dbt-duckdb-airflow` | DuckDB (file-based, local) | `pattern/elt-warehouse/dbt-duckdb-airflow` |

## Prerequisites

- Python 3.11, pip
- `dbt-snowflake` / `dbt-bigquery` / `dbt-duckdb` installed (see each variant's README)
- Cloud credentials (Snowflake/BigQuery) OR Docker (dbt-duckdb-airflow)
```

- [ ] **Step 2: Create shared dbt model files used by all three variants**

For all three variants, create the following under `models/`:

`models/staging/sources.yml`:
```yaml
version: 2

sources:
  - name: raw
    description: Raw tables loaded by the ingestion layer
    tables:
      - name: orders
        description: Raw orders from the source system
        columns:
          - name: order_id
            tests: [not_null, unique]
          - name: customer_id
            tests: [not_null]
          - name: amount_cents
            tests: [not_null]
```

`models/staging/stg_orders.sql`:
```sql
with source as (
    select * from {{ source('raw', 'orders') }}
),

renamed as (
    select
        order_id::varchar          as order_id,
        customer_id::varchar       as customer_id,
        status::varchar            as status,
        amount_cents::integer      as amount_cents,
        amount_cents / 100.0       as amount_usd,
        placed_at::timestamp       as placed_at,
        updated_at::timestamp      as updated_at
    from source
    where order_id is not null
)

select * from renamed
```

`models/marts/orders_summary.sql`:
```sql
{{
  config(
    materialized='table',
    tags=['daily', 'finance']
  )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

summary as (
    select
        cast(placed_at as date)    as order_date,
        status,
        count(*)                   as order_count,
        sum(amount_usd)            as total_revenue_usd,
        avg(amount_usd)            as avg_order_value_usd,
        min(placed_at)             as first_order_at,
        max(placed_at)             as last_order_at
    from orders
    group by 1, 2
)

select * from summary
order by order_date desc, status
```

`models/marts/schema.yml`:
```yaml
version: 2

models:
  - name: orders_summary
    description: Daily order counts and revenue by status
    columns:
      - name: order_date
        tests: [not_null]
      - name: order_count
        tests: [not_null]
      - name: total_revenue_usd
        tests: [not_null]
```

`dbt_project.yml` (same for all three variants — only `name` and `profile` differ):
```yaml
name: chakra_lakehouse
version: "1.0.0"
config-version: 2

profile: chakra

model-paths: ["models"]
test-paths: ["tests"]
seed-paths: ["seeds"]

models:
  chakra_lakehouse:
    staging:
      +materialized: view
      +schema: staging
    marts:
      +materialized: table
      +schema: marts
```

`packages.yml`:
```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: ">=1.0.0,<2.0.0"
```

- [ ] **Step 3: Create `dbt-snowflake/profiles.yml` and `.env.example`**

`profiles.yml`:
```yaml
chakra:
  target: dev
  outputs:
    dev:
      type: snowflake
      account:   "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user:      "{{ env_var('SNOWFLAKE_USER') }}"
      password:  "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role:      "{{ env_var('SNOWFLAKE_ROLE', 'TRANSFORMER') }}"
      database:  "{{ env_var('SNOWFLAKE_DATABASE', 'CHAKRA') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH') }}"
      schema:    "{{ env_var('SNOWFLAKE_SCHEMA', 'RAW') }}"
      threads: 4
      client_session_keep_alive: false
```

`.env.example`:
```bash
SNOWFLAKE_ACCOUNT=myaccount.us-east-1
SNOWFLAKE_USER=dbt_user
SNOWFLAKE_PASSWORD=changeme
SNOWFLAKE_ROLE=TRANSFORMER
SNOWFLAKE_DATABASE=CHAKRA
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_SCHEMA=RAW
```

`README.md`:
```markdown
# ELT / Warehouse — dbt + Snowflake

## When to use
Snowflake is your warehouse. Team writes SQL. No distributed compute needed.

## How to run

```bash
pip install dbt-snowflake dbt-utils
cp .env.example .env && source .env
dbt deps
dbt debug          # verify connection
dbt run            # run all models
dbt test           # run schema tests
```
```

- [ ] **Step 4: Create `dbt-bigquery/profiles.yml` and `.env.example`**

`profiles.yml`:
```yaml
chakra:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project:  "{{ env_var('GCP_PROJECT') }}"
      dataset:  "{{ env_var('BQ_DATASET', 'chakra_raw') }}"
      location: "{{ env_var('BQ_LOCATION', 'US') }}"
      threads: 4
      timeout_seconds: 300
```

`.env.example`:
```bash
GCP_PROJECT=my-gcp-project
BQ_DATASET=chakra_raw
BQ_LOCATION=US
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

`README.md`:
```markdown
# ELT / Warehouse — dbt + BigQuery

## When to use
GCP-first org. BigQuery is your warehouse.

## How to run

```bash
pip install dbt-bigquery
gcloud auth application-default login   # or set GOOGLE_APPLICATION_CREDENTIALS
cp .env.example .env && source .env
dbt deps && dbt debug && dbt run && dbt test
```
```

- [ ] **Step 5: Create `dbt-duckdb-airflow` variant**

`profiles.yml`:
```yaml
chakra:
  target: dev
  outputs:
    dev:
      type: duckdb
      path:    "{{ env_var('DUCKDB_PATH', '/tmp/chakra.duckdb') }}"
      threads: 4
```

`dags/dbt_dag.py`:
```python
"""
dbt + DuckDB Airflow DAG

Runs dbt deps → dbt run → dbt test on a daily schedule.
Uses BashOperator — no Spark cluster required.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_PROJECT_DIR = Path("/opt/airflow/dbt")

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="elt_dbt_duckdb",
    default_args=default_args,
    description="Daily dbt run against DuckDB",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["elt", "dbt", "duckdb"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --target dev",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --target dev",
    )

    dbt_deps >> dbt_run >> dbt_test
```

`docker-compose.yml`:
```yaml
version: "3.9"

services:
  airflow:
    image: apache/airflow:2.9.3-python3.11
    command: >
      bash -c "
        pip install dbt-duckdb &&
        airflow db migrate &&
        airflow standalone
      "
    ports:
      - "8080:8080"
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: sqlite:////opt/airflow/airflow.db
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
      AIRFLOW__WEBSERVER__SECRET_KEY: ${AIRFLOW_SECRET_KEY:-dev-secret}
      DUCKDB_PATH: /opt/airflow/chakra.duckdb
    volumes:
      - ./dags:/opt/airflow/dags
      - .:/opt/airflow/dbt
      - airflow-logs:/opt/airflow/logs

volumes:
  airflow-logs:
```

`.env.example`:
```bash
DUCKDB_PATH=/tmp/chakra.duckdb
AIRFLOW_SECRET_KEY=dev-secret-change-in-prod
```

`README.md`:
```markdown
# ELT / Warehouse — dbt + DuckDB + Airflow

## When to use
Cost-sensitive, local/dev, or small-to-medium data. DuckDB runs in-process — no warehouse account needed.

## How to run

```bash
pip install dbt-duckdb
cp .env.example .env && source .env
dbt deps && dbt run && dbt test

# With Airflow scheduling:
docker compose up -d
# Airflow UI: http://localhost:8080 — trigger dag `elt_dbt_duckdb`
```
```

- [ ] **Step 6: Verify required files for all three ELT variants**

```bash
for variant in dbt-snowflake dbt-bigquery dbt-duckdb-airflow; do
  for f in README.md dbt_project.yml profiles.yml .env.example; do
    path="patterns/elt-warehouse/variants/$variant/$f"
    [ -f "$path" ] || { echo "MISSING: $path"; exit 1; }
  done
done
# dbt-duckdb-airflow also needs docker-compose.yml
[ -f "patterns/elt-warehouse/variants/dbt-duckdb-airflow/docker-compose.yml" ] \
  || { echo "MISSING: docker-compose.yml"; exit 1; }
echo "All elt-warehouse variant files present"
```

Note: `dbt-snowflake` and `dbt-bigquery` have no `docker-compose.yml` (cloud-only).
The validate-variants CI check (Task 20) exempts cloud-only variants via a marker file `.cloud-only` — create that file in those two variants now:

```bash
touch patterns/elt-warehouse/variants/dbt-snowflake/.cloud-only
touch patterns/elt-warehouse/variants/dbt-bigquery/.cloud-only
# Create placeholder docker-compose.yml so validate-variants passes without the .cloud-only marker
# Alternative: update validate-variants.yml in Task 20 to skip .cloud-only variants
```

For simplicity, create a minimal `docker-compose.yml` for cloud variants that prints a message:

`patterns/elt-warehouse/variants/dbt-snowflake/docker-compose.yml`:
```yaml
# This variant connects to Snowflake (cloud). No local services required.
# Set credentials in .env and run: dbt run
version: "3.9"
services: {}
```

`patterns/elt-warehouse/variants/dbt-bigquery/docker-compose.yml` — same content with BigQuery substituted.

- [ ] **Step 7: Commit**

```bash
git add patterns/elt-warehouse/
git commit -m "feat: elt-warehouse pattern — dbt-snowflake, dbt-bigquery, dbt-duckdb-airflow variants"
```

---

### Task 14: cdc-pipeline (Pattern README + 2 Variants)

**Files:**
- Create: `patterns/cdc-pipeline/README.md`
- Variant `debezium-kafka-flink`: `README.md`, `pom.xml`, `src/`, `config/debezium-connector.json`, `config/init.sql`, `docker-compose.yml`, `.env.example`
- Variant `debezium-kafka-spark`: `README.md`, `build.sbt`, `src/`, `config/debezium-connector.json`, `config/init.sql`, `docker-compose.yml`, `.env.example`

- [ ] **Step 1: Create `patterns/cdc-pipeline/README.md`**

```markdown
# CDC Pipeline

Debezium reads the database WAL and streams every committed change to Kafka. A downstream engine
(Flink or Spark) processes the CDC events and materialises them into a lakehouse.

## Variants

| Variant | Processing engine | Latency | Pull branch |
|---|---|---|---|
| `debezium-kafka-flink` | Flink 1.19 (Java) | ~5s | `pattern/cdc-pipeline/debezium-kafka-flink` |
| `debezium-kafka-spark` | Spark 3.5 Structured Streaming (Scala) | ~30–60s | `pattern/cdc-pipeline/debezium-kafka-spark` |

## Prerequisites

- Docker + Docker Compose (starts PostgreSQL, Kafka, Debezium Connect, and Flink or Spark)
- Java 17 + Maven 3.9 (debezium-kafka-flink) OR Scala 2.12 + SBT 1.9 (debezium-kafka-spark)
- PostgreSQL configured with `wal_level=logical` (docker-compose handles this automatically)
```

- [ ] **Step 2: Create shared `config/debezium-connector.json`** (used by both variants)

```json
{
  "name": "orders-source-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "${POSTGRES_USER}",
    "database.password": "${POSTGRES_PASSWORD}",
    "database.dbname": "${POSTGRES_DB}",
    "table.include.list": "public.orders",
    "topic.prefix": "chakra",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_orders_slot",
    "publication.name": "debezium_orders_pub",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false"
  }
}
```

- [ ] **Step 3: Create shared `config/init.sql`** (PostgreSQL initialisation)

```sql
-- Create orders table for CDC demo
CREATE TABLE IF NOT EXISTS orders (
    order_id     VARCHAR(36)    PRIMARY KEY,
    customer_id  VARCHAR(36)    NOT NULL,
    status       VARCHAR(20)    NOT NULL DEFAULT 'pending',
    amount_cents INTEGER        NOT NULL,
    placed_at    TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

-- Seed some initial rows
INSERT INTO orders (order_id, customer_id, status, amount_cents)
VALUES
    ('ord-001', 'cust-a', 'pending',   4999),
    ('ord-002', 'cust-b', 'confirmed', 12999),
    ('ord-003', 'cust-a', 'shipped',   899)
ON CONFLICT DO NOTHING;
```

- [ ] **Step 4: Create `debezium-kafka-flink/src/main/java/io/chakraview/cdc/Main.java`**

```java
package io.chakraview.cdc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000, CheckpointingMode.EXACTLY_ONCE);

        // Debezium publishes to <topic.prefix>.<schema>.<table>
        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(System.getenv("KAFKA_BOOTSTRAP_SERVERS"))
            .setTopics("chakra.public.orders")
            .setGroupId("flink-cdc-orders")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> cdcStream = env.fromSource(
            source, WatermarkStrategy.noWatermarks(), "debezium-kafka-source"
        );

        // Parse CDC envelope (after unwrap transform: flat JSON with __deleted field)
        DataStream<CdcEvent> events = cdcStream.map(new CdcEventParser());

        // Route inserts/updates and deletes separately
        DataStream<CdcEvent> upserts = events.filter(e -> !e.isDeleted());
        DataStream<CdcEvent> deletes = events.filter(CdcEvent::isDeleted);

        // Upsert sink — write to Iceberg (or log for local dev)
        upserts.print("UPSERT");
        deletes.print("DELETE");

        // TODO: replace print() with FlinkSink.forRow() writing to Iceberg
        // See streaming-lakehouse/variants/flink-iceberg for the Iceberg sink wiring

        env.execute("cdc-pipeline-flink");
    }
}
```

- [ ] **Step 5: Create `debezium-kafka-flink/src/main/java/io/chakraview/cdc/CdcEvent.java`**

```java
package io.chakraview.cdc;

public class CdcEvent {
    public String orderId;
    public String customerId;
    public String status;
    public long   amountCents;
    public String placedAt;
    public boolean deleted;

    public boolean isDeleted() { return deleted; }

    @Override
    public String toString() {
        return String.format("CdcEvent{orderId=%s, status=%s, deleted=%s}",
            orderId, status, deleted);
    }
}
```

- [ ] **Step 6: Create `debezium-kafka-flink/src/main/java/io/chakraview/cdc/CdcEventParser.java`**

```java
package io.chakraview.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;

public class CdcEventParser implements MapFunction<String, CdcEvent> {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public CdcEvent map(String json) throws Exception {
        JsonNode node = MAPPER.readTree(json);
        CdcEvent event = new CdcEvent();
        event.orderId     = node.path("order_id").asText();
        event.customerId  = node.path("customer_id").asText();
        event.status      = node.path("status").asText();
        event.amountCents = node.path("amount_cents").asLong();
        event.placedAt    = node.path("placed_at").asText();
        // Debezium ExtractNewRecordState adds __deleted = "true" for deletes
        event.deleted     = "true".equals(node.path("__deleted").asText());
        return event;
    }
}
```

- [ ] **Step 7: Create `debezium-kafka-flink/docker-compose.yml`**

```yaml
version: "3.9"

services:
  postgres:
    image: postgres:16
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: ${POSTGRES_USER:-chakra}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-chakra}
      POSTGRES_DB: ${POSTGRES_DB:-chakra}
    command: >
      postgres
        -c wal_level=logical
        -c max_replication_slots=4
        -c max_wal_senders=4
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./config/init.sql:/docker-entrypoint-initdb.d/01-init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U chakra"]
      interval: 10s
      retries: 5

  kafka:
    image: bitnami/kafka:3.7.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_CFG_NODE_ID: 1
      KAFKA_CFG_PROCESS_ROLES: controller,broker
      KAFKA_CFG_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: "true"
    healthcheck:
      test: ["CMD", "kafka-topics.sh", "--bootstrap-server", "localhost:9092", "--list"]
      interval: 15s
      retries: 5

  kafka-connect:
    image: debezium/connect:2.7
    depends_on:
      kafka:
        condition: service_healthy
      postgres:
        condition: service_healthy
    ports:
      - "8083:8083"
    environment:
      BOOTSTRAP_SERVERS: kafka:9092
      GROUP_ID: debezium-connect
      CONFIG_STORAGE_TOPIC: debezium.configs
      OFFSET_STORAGE_TOPIC: debezium.offsets
      STATUS_STORAGE_TOPIC: debezium.status
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8083/connectors"]
      interval: 15s
      retries: 8

  flink-jobmanager:
    image: flink:1.19-java17
    command: jobmanager
    ports:
      - "8082:8081"
    environment:
      JOB_MANAGER_RPC_ADDRESS: flink-jobmanager

  flink-taskmanager:
    image: flink:1.19-java17
    command: taskmanager
    depends_on: [flink-jobmanager]
    environment:
      JOB_MANAGER_RPC_ADDRESS: flink-jobmanager
      TASK_MANAGER_NUMBER_OF_TASK_SLOTS: 4

volumes:
  postgres-data:
```

- [ ] **Step 8: Create `.env.example` and `README.md` for debezium-kafka-flink**

`.env.example`:
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
POSTGRES_USER=chakra
POSTGRES_PASSWORD=chakra
POSTGRES_DB=chakra
```

`README.md`:
```markdown
# CDC Pipeline — Debezium + Kafka + Flink

## When to use
Sub-second capture from a live PostgreSQL database. Exactly-once processing downstream.

## How to run locally

```bash
cp .env.example .env
docker compose up -d
# Wait for all services healthy, then register the Debezium connector:
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @config/debezium-connector.json

mvn package -DskipTests
flink run -m localhost:8082 target/cdc-pipeline-flink-iceberg-0.1.0.jar

# Insert a row to trigger a CDC event:
docker exec -it <postgres-container> psql -U chakra -c \
  "INSERT INTO orders VALUES ('ord-999','cust-z','pending',100,NOW(),NOW());"
```
```

- [ ] **Step 9: Create `debezium-kafka-spark` variant**

`build.sbt`:
```scala
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "0.1.0"
ThisBuild / organization := "io.chakraview"

val sparkVersion = "3.5.1"
val deltaVersion = "3.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "cdc-pipeline-spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"           % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"            % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
      "io.delta"         %% "delta-spark"          % deltaVersion,
      "org.apache.hadoop"  % "hadoop-aws"          % "3.3.6"      % "provided",
      "com.amazonaws"      % "aws-java-sdk-bundle" % "1.12.262"   % "provided",
    ),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case "reference.conf"         => MergeStrategy.concat
      case _                        => MergeStrategy.first
    },
  )
```

`src/main/scala/io/chakraview/cdc/Main.scala`:
```scala
package io.chakraview.cdc

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

object Main {
  private val TablePath =
    sys.env.getOrElse("DELTA_TABLE_PATH", "s3a://chakra-lakehouse/delta/orders/cdc")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("cdc-pipeline-spark")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.hadoop.fs.s3a.endpoint",
        sys.env.getOrElse("S3_ENDPOINT", "http://localhost:9000"))
      .config("spark.hadoop.fs.s3a.access.key",
        sys.env.getOrElse("AWS_ACCESS_KEY_ID", "minioadmin"))
      .config("spark.hadoop.fs.s3a.secret.key",
        sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", "minioadmin"))
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate()

    // Debezium publishes flat JSON after ExtractNewRecordState unwrap transform
    val cdcSchema = CdcSchema.schema

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",
        sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
      .option("subscribe", "chakra.public.orders")
      .option("startingOffsets", "earliest")
      .load()

    val events = raw
      .select(from_json(col("value").cast("string"), cdcSchema).alias("d"))
      .select("d.*")

    val query = events.writeStream
      .trigger(Trigger.ProcessingTime(60.seconds))
      .option("checkpointLocation",
        sys.env.getOrElse("CHECKPOINT_PATH", "s3a://chakra-lakehouse/checkpoints/cdc-orders/"))
      .foreachBatch { (batch: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], _: Long) =>
        if (!batch.isEmpty) {
          val upserts = batch.filter(col("__deleted") =!= lit("true"))
          val deletes = batch.filter(col("__deleted") === lit("true"))

          // Apply upserts
          if (!upserts.isEmpty) {
            if (!DeltaTable.isDeltaTable(spark, TablePath))
              upserts.drop("__deleted").write.format("delta").save(TablePath)
            else
              DeltaTable.forPath(spark, TablePath)
                .alias("t")
                .merge(upserts.drop("__deleted").alias("s"), "t.order_id = s.order_id")
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute()
          }

          // Apply deletes (soft-delete pattern: mark rather than remove)
          if (!deletes.isEmpty && DeltaTable.isDeltaTable(spark, TablePath)) {
            DeltaTable.forPath(spark, TablePath)
              .alias("t")
              .merge(deletes.alias("s"), "t.order_id = s.order_id")
              .whenMatched().update(Map("status" -> lit("deleted")))
              .execute()
          }
        }
      }
      .start()

    query.awaitTermination()
  }
}
```

`src/main/scala/io/chakraview/cdc/CdcSchema.scala`:
```scala
package io.chakraview.cdc

import org.apache.spark.sql.types._

object CdcSchema {
  val schema: StructType = StructType(Seq(
    StructField("order_id",    StringType,    nullable = false),
    StructField("customer_id", StringType,    nullable = true),
    StructField("status",      StringType,    nullable = true),
    StructField("amount_cents",LongType,      nullable = true),
    StructField("placed_at",   TimestampType, nullable = true),
    StructField("updated_at",  TimestampType, nullable = true),
    StructField("__deleted",   StringType,    nullable = true),
  ))
}
```

The `docker-compose.yml` for `debezium-kafka-spark` is the same as `debezium-kafka-flink` but without the Flink services. MinIO is added for the Delta Lake sink:

```yaml
version: "3.9"

services:
  postgres:
    image: postgres:16
    ports: ["5432:5432"]
    environment:
      POSTGRES_USER: ${POSTGRES_USER:-chakra}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-chakra}
      POSTGRES_DB: ${POSTGRES_DB:-chakra}
    command: postgres -c wal_level=logical -c max_replication_slots=4 -c max_wal_senders=4
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./config/init.sql:/docker-entrypoint-initdb.d/01-init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U chakra"]
      interval: 10s
      retries: 5

  kafka:
    image: bitnami/kafka:3.7.0
    ports: ["9092:9092"]
    environment:
      KAFKA_CFG_NODE_ID: 1
      KAFKA_CFG_PROCESS_ROLES: controller,broker
      KAFKA_CFG_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: "true"

  kafka-connect:
    image: debezium/connect:2.7
    depends_on: [kafka, postgres]
    ports: ["8083:8083"]
    environment:
      BOOTSTRAP_SERVERS: kafka:9092
      GROUP_ID: debezium-connect
      CONFIG_STORAGE_TOPIC: debezium.configs
      OFFSET_STORAGE_TOPIC: debezium.offsets
      STATUS_STORAGE_TOPIC: debezium.status

  minio:
    image: minio/minio:RELEASE.2024-04-06T05-26-02Z
    command: server /data --console-address ":9001"
    ports: ["9000:9000", "9001:9001"]
    environment:
      MINIO_ROOT_USER: ${AWS_ACCESS_KEY_ID:-minioadmin}
      MINIO_ROOT_PASSWORD: ${AWS_SECRET_ACCESS_KEY:-minioadmin}
    volumes:
      - minio-data:/data

volumes:
  postgres-data:
  minio-data:
```

`.env.example`:
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
POSTGRES_USER=chakra
POSTGRES_PASSWORD=chakra
POSTGRES_DB=chakra
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
S3_ENDPOINT=http://localhost:9000
DELTA_TABLE_PATH=s3a://chakra-lakehouse/delta/orders/cdc
CHECKPOINT_PATH=s3a://chakra-lakehouse/checkpoints/cdc-orders/
```

`README.md`:
```markdown
# CDC Pipeline — Debezium + Kafka + Spark

## When to use
CDC capture with Spark processing. Latency > 30s acceptable. Team already knows Spark.

## How to run locally

```bash
cp .env.example .env
docker compose up -d
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @config/debezium-connector.json
sbt assembly
spark-submit --class io.chakraview.cdc.Main --master local[*] \
  target/scala-2.12/cdc-pipeline-spark-assembly-0.1.0.jar
```
```

- [ ] **Step 10: Verify required files**

```bash
for variant in debezium-kafka-flink debezium-kafka-spark; do
  for f in README.md docker-compose.yml .env.example; do
    path="patterns/cdc-pipeline/variants/$variant/$f"
    [ -f "$path" ] || { echo "MISSING: $path"; exit 1; }
  done
done
echo "All cdc-pipeline variant files present"
```

- [ ] **Step 11: Commit**

```bash
git add patterns/cdc-pipeline/
git commit -m "feat: cdc-pipeline pattern — debezium-kafka-flink (Java) and debezium-kafka-spark (Scala) variants"
```

---

### Task 15: lambda-architecture (Pattern README + 2 Variants)

**Files:**
- Create: `patterns/lambda-architecture/README.md`
- Variant `flink-spark-iceberg`: `streaming/` (Java Flink job) + `batch/` (Scala Spark job) + `docker-compose.yml` + `.env.example` + `README.md`
- Variant `spark-streaming-batch-delta`: `streaming/` (Scala Spark Streaming) + `batch/` (Scala Spark Batch) + `docker-compose.yml` + `.env.example` + `README.md`

- [ ] **Step 1: Create `patterns/lambda-architecture/README.md`**

```markdown
# Lambda Architecture

Two parallel paths — streaming (low latency, approximate) and batch (high latency, accurate) —
merged at query time. Use only when both are genuinely required by different stakeholders.

## Variants

| Variant | Streaming engine | Batch engine | Table format | Pull branch |
|---|---|---|---|---|
| `flink-spark-iceberg` | Flink 1.19 (Java) | Spark 3.5 (Scala) | Iceberg 1.5 | `pattern/lambda-architecture/flink-spark-iceberg` |
| `spark-streaming-batch-delta` | Spark 3.5 Structured Streaming (Scala) | Spark 3.5 Batch (Scala) | Delta 3.1 | `pattern/lambda-architecture/spark-streaming-batch-delta` |

## Merge strategy

Both paths write to the same Iceberg/Delta table with a `path` column: `streaming` or `batch`.
At query time, Iceberg time travel or Delta's versioning lets analysts choose the freshest streaming
snapshot OR the latest batch-corrected snapshot. The serving layer queries the batch path for
reporting and the streaming path for real-time dashboards.

## Warning

Lambda doubles your operational surface: two pipelines, two sets of SLAs, two codebases.
Before committing, validate that a single streaming path (Streaming Lakehouse pattern) cannot
meet your requirements.
```

- [ ] **Step 2: Create `flink-spark-iceberg/streaming/` — Flink job (same idiom as streaming-lakehouse flink-iceberg)**

`streaming/pom.xml` — same as `streaming-lakehouse/variants/flink-iceberg/pom.xml` with `artifactId` changed to `lambda-streaming-flink-iceberg`.

`streaming/src/main/java/io/chakraview/lambda/StreamingJob.java`:
```java
package io.chakraview.lambda;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

// Streaming path: low-latency approximate results written to Iceberg with path="streaming"
public class StreamingJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(30_000, CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers(System.getenv("KAFKA_BOOTSTRAP_SERVERS"))
            .setTopics("events.orders")
            .setGroupId("lambda-streaming-flink")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        DataStream<String> stream = env.fromSource(
            source, WatermarkStrategy.noWatermarks(), "kafka-orders");

        // Tag all rows with path="streaming" before writing to shared Iceberg table
        DataStream<Row> rows = stream
            .map(new EventParser())
            .map(row -> { row.setField(5, "streaming"); return row; });

        // Wire FlinkSink to Iceberg table (same wiring as streaming-lakehouse/flink-iceberg)
        // Table schema must include path VARCHAR column
        rows.print("streaming-path");

        env.execute("lambda-streaming-flink");
    }
}
```

- [ ] **Step 3: Create `flink-spark-iceberg/batch/` — Spark batch job**

`batch/build.sbt` — same as `batch-lakehouse/variants/spark-iceberg/build.sbt` with `name` changed to `lambda-batch-spark-iceberg`.

`batch/src/main/scala/io/chakraview/lambda/BatchJob.scala`:
```scala
package io.chakraview.lambda

import io.chakraview.lakehouse.{IcebergWriter, SparkSessions}
import org.apache.spark.sql.functions._

// Batch path: high-accuracy results written to same Iceberg table with path="batch"
object BatchJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessions.iceberg("lambda-batch-spark-iceberg")
    import spark.implicits._

    val source = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("s3a://chakra-lakehouse/raw/orders/")

    val transformed = source
      .withColumn("processed_at", current_timestamp())
      .withColumn("amount_usd",   col("amount_cents").divide(100.0))
      .withColumn("path",         lit("batch"))
      .filter(col("order_id").isNotNull)

    // Overwrite the batch partition; streaming partition is untouched
    spark.sql(s"""
      DELETE FROM lakehouse.orders.events WHERE path = 'batch'
    """)
    IcebergWriter.append(transformed, "lakehouse.orders.events")

    println(s"Batch path updated: ${transformed.count()} rows")
    spark.stop()
  }
}
```

- [ ] **Step 4: Create `flink-spark-iceberg/docker-compose.yml`**

Same as `streaming-lakehouse/variants/flink-iceberg/docker-compose.yml` (Kafka + Flink + MinIO + Iceberg REST). Copy it verbatim.

- [ ] **Step 5: Create `flink-spark-iceberg/.env.example`**

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_REGION=us-east-1
S3_ENDPOINT=http://localhost:9000
ICEBERG_REST_URI=http://localhost:8181
LAKEHOUSE_WAREHOUSE=s3://chakra-lakehouse/
```

- [ ] **Step 6: Create `flink-spark-iceberg/README.md`**

```markdown
# Lambda Architecture — Flink (streaming) + Spark (batch) + Iceberg

## When to use
Stakeholders need both real-time approximate results AND nightly corrected results,
and they genuinely cannot agree on one.

## How to run locally

```bash
cp .env.example .env
docker compose up -d

# Start streaming path (Flink)
cd streaming && mvn package -DskipTests
flink run -m localhost:8082 target/lambda-streaming-flink-iceberg-0.1.0.jar

# Run batch path (Spark, scheduled via cron or Airflow)
cd ../batch && sbt assembly
spark-submit --class io.chakraview.lambda.BatchJob --master local[*] \
  target/scala-2.12/lambda-batch-spark-iceberg-assembly-0.1.0.jar
```

Query both paths in DuckDB:
```sql
-- Streaming results (fresh, approximate)
SELECT * FROM iceberg_scan('s3://chakra-lakehouse/...') WHERE path = 'streaming';

-- Batch results (accurate, up to 1h old)
SELECT * FROM iceberg_scan('s3://chakra-lakehouse/...') WHERE path = 'batch';
```
```

- [ ] **Step 7: Create `spark-streaming-batch-delta` variant**

`streaming/build.sbt` — same as `streaming-lakehouse/variants/spark-delta/build.sbt` with name `lambda-streaming-spark-delta`.

`streaming/src/main/scala/io/chakraview/lambda/StreamingJob.scala`:
```scala
package io.chakraview.lambda

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

object StreamingJob {
  private val TablePath =
    sys.env.getOrElse("DELTA_TABLE_PATH", "s3a://chakra-lakehouse/delta/orders/lambda")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("lambda-streaming-spark-delta")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.hadoop.fs.s3a.endpoint",
        sys.env.getOrElse("S3_ENDPOINT", "http://localhost:9000"))
      .config("spark.hadoop.fs.s3a.access.key",
        sys.env.getOrElse("AWS_ACCESS_KEY_ID", "minioadmin"))
      .config("spark.hadoop.fs.s3a.secret.key",
        sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", "minioadmin"))
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate()

    val raw = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",
        sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
      .option("subscribe", "events.orders")
      .option("startingOffsets", "latest")
      .load()

    import spark.implicits._
    val orders = raw
      .select(from_json(col("value").cast("string"), EventSchema.schema).alias("d"))
      .select("d.*")
      .withColumn("path", lit("streaming"))

    orders.writeStream
      .trigger(Trigger.ProcessingTime(60.seconds))
      .option("checkpointLocation",
        sys.env.getOrElse("CHECKPOINT_PATH", "s3a://chakra-lakehouse/checkpoints/lambda-streaming/"))
      .foreachBatch { (batch: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], _: Long) =>
        if (!batch.isEmpty) {
          if (!DeltaTable.isDeltaTable(spark, TablePath))
            batch.write.format("delta").partitionBy("path").save(TablePath)
          else
            batch.write.format("delta").mode("append").save(TablePath)
        }
      }
      .start()
      .awaitTermination()
  }
}
```

`batch/build.sbt` — same as `batch-lakehouse/variants/spark-delta/build.sbt` with name `lambda-batch-spark-delta`.

`batch/src/main/scala/io/chakraview/lambda/BatchJob.scala`:
```scala
package io.chakraview.lambda

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BatchJob {
  private val TablePath =
    sys.env.getOrElse("DELTA_TABLE_PATH", "s3a://chakra-lakehouse/delta/orders/lambda")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("lambda-batch-spark-delta")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.hadoop.fs.s3a.endpoint",
        sys.env.getOrElse("S3_ENDPOINT", "http://localhost:9000"))
      .config("spark.hadoop.fs.s3a.access.key",
        sys.env.getOrElse("AWS_ACCESS_KEY_ID", "minioadmin"))
      .config("spark.hadoop.fs.s3a.secret.key",
        sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", "minioadmin"))
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate()

    val source = spark.read
      .option("header", "true").option("inferSchema", "true")
      .csv("s3a://chakra-lakehouse/raw/orders/")

    val accurate = source
      .withColumn("processed_at", current_timestamp())
      .withColumn("amount_usd",   col("amount_cents").divide(100.0))
      .withColumn("path",         lit("batch"))
      .filter(col("order_id").isNotNull)

    // Overwrite only the batch partition
    accurate.write.format("delta")
      .mode("overwrite")
      .option("replaceWhere", "path = 'batch'")
      .save(TablePath)

    println(s"Batch path refreshed: ${accurate.count()} rows")
    spark.stop()
  }
}
```

`docker-compose.yml` — Kafka + MinIO (Spark runs locally for both streaming and batch):
```yaml
version: "3.9"

services:
  kafka:
    image: bitnami/kafka:3.7.0
    ports: ["9092:9092"]
    environment:
      KAFKA_CFG_NODE_ID: 1
      KAFKA_CFG_PROCESS_ROLES: controller,broker
      KAFKA_CFG_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: "true"

  minio:
    image: minio/minio:RELEASE.2024-04-06T05-26-02Z
    command: server /data --console-address ":9001"
    ports: ["9000:9000", "9001:9001"]
    environment:
      MINIO_ROOT_USER: ${AWS_ACCESS_KEY_ID:-minioadmin}
      MINIO_ROOT_PASSWORD: ${AWS_SECRET_ACCESS_KEY:-minioadmin}
    volumes:
      - minio-data:/data

volumes:
  minio-data:
```

`.env.example`:
```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
S3_ENDPOINT=http://localhost:9000
DELTA_TABLE_PATH=s3a://chakra-lakehouse/delta/orders/lambda
CHECKPOINT_PATH=s3a://chakra-lakehouse/checkpoints/lambda-streaming/
```

`README.md`:
```markdown
# Lambda Architecture — Spark Streaming + Spark Batch + Delta Lake

## When to use
Single Spark team that needs both streaming and batch, with Delta as the unifying format.

## How to run locally

```bash
cp .env.example .env
docker compose up -d

# Streaming path (continuous):
cd streaming && sbt assembly
spark-submit --class io.chakraview.lambda.StreamingJob --master local[*] \
  target/scala-2.12/lambda-streaming-spark-delta-assembly-0.1.0.jar &

# Batch path (scheduled):
cd ../batch && sbt assembly
spark-submit --class io.chakraview.lambda.BatchJob --master local[*] \
  target/scala-2.12/lambda-batch-spark-delta-assembly-0.1.0.jar
```
```

- [ ] **Step 8: Verify required files**

```bash
for variant in flink-spark-iceberg spark-streaming-batch-delta; do
  for f in README.md docker-compose.yml .env.example; do
    path="patterns/lambda-architecture/variants/$variant/$f"
    [ -f "$path" ] || { echo "MISSING: $path"; exit 1; }
  done
done
echo "All lambda-architecture variant files present"
```

- [ ] **Step 9: Commit**

```bash
git add patterns/lambda-architecture/
git commit -m "feat: lambda-architecture pattern — flink-spark-iceberg and spark-streaming-batch-delta variants"
```

---

*Tasks 16–20 continue in the next batch.*
