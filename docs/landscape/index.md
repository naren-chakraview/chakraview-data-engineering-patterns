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
