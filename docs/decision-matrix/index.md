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
