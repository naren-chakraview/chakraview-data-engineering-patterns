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

*Tasks 6–10 continue in the next batch.*
