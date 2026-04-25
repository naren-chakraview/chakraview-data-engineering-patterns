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
