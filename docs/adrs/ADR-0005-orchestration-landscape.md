# ADR-0005: Orchestration Landscape — Airflow vs Prefect vs Dagster vs Temporal

**Status**: Accepted
**Date**: 2026-04-25
**Deciders**: Portfolio architect

---

## Context

The workflow-orchestration pattern and several multi-step patterns (batch-lakehouse with Airflow, ELT/dbt with Airflow, ML feature pipeline with Airflow) require an orchestration layer. Four tools compete: Apache Airflow, Prefect, Dagster, and Temporal.

A decision is needed: which orchestrators to include as boilerplate variants, and what guidance to provide for choosing between them.

## Decision

Include **Airflow, Prefect, and Dagster** as workflow-orchestration variants. Do not include Temporal.

For multi-pattern integrations (batch-lakehouse + Airflow, dbt + Airflow, ML pipeline + Airflow), use Airflow exclusively — it is the lowest common denominator and has the broadest operator ecosystem.

## Rationale

### Why include all three (Airflow, Prefect, Dagster)

The orchestration choice has genuine, non-obvious trade-offs that are not resolvable without knowing team preferences and existing infrastructure. Showing all three side by side, with a comparison table and per-variant README, gives engineers the most direct way to assess the difference.

### Airflow

Airflow is the industry standard for data pipeline orchestration. The Apache Airflow provider ecosystem has 800+ operators covering every data source and sink in common use. Any team inheriting a production data platform likely already runs Airflow.

Weaknesses: Airflow's scheduler is stateful and complex to operate at scale. DAG definitions are written in Python but the DAG object model is an indirect representation of the dependency graph (tasks reference each other's IDs, not return values). Dynamic task generation (Airflow 2.x dynamic task mapping) is improved but more verbose than Prefect or Dagster equivalents.

### Prefect

Prefect's primary advantage is Python ergonomics. A Prefect flow is a Python function decorated with `@flow`; tasks are functions decorated with `@task`. Dependency is expressed by calling tasks from the flow — no DSL, no task ID references. Prefect handles retries, caching (via `cache_key_fn`), and concurrent execution natively.

Prefect Cloud provides managed infrastructure; `prefect server` provides a self-hosted option. Operational complexity is lower than Airflow for small-to-medium deployments.

Weaknesses: Prefect's provider ecosystem is smaller than Airflow's. Teams that need a pre-built operator for a specific data source (e.g., Salesforce, SAP) will find Airflow has more coverage.

### Dagster

Dagster's core abstraction is the **software-defined asset (SDA)**: a Python function that produces a data artifact, with optional metadata attached to each materialisation. Dagster tracks lineage between assets automatically and provides a per-materialisation audit trail out of the box.

This makes Dagster the strongest choice for data platform teams that think in terms of data products (see data mesh): each asset is a versioned data artifact with a known lineage, freshness SLA, and owner.

Weaknesses: Dagster's learning curve is steeper than Prefect's for engineers not already thinking in asset-centric terms. The `Definitions` object and code location model are non-obvious on first contact.

### Why not Temporal

Temporal is a durable workflow engine for microservice coordination, not a data pipeline orchestrator. It lacks native concepts for data-oriented scheduling (partitions, date ranges, backfill). Including Temporal would mislead engineers into considering it for batch/streaming data pipelines where Airflow/Prefect/Dagster are the correct tool.

## Consequences

**Positive:**

- Engineers see three orchestrators side by side, each demonstrating its idiomatic abstraction (DAG/operator, flow/task, asset).
- The Airflow variant is reusable as the integration layer for other patterns (batch-lakehouse, ELT, ML).

**Negative:**

- Three variants triple the maintenance surface for the workflow-orchestration pattern. If Prefect or Dagster change their API significantly, the boilerplate will need updating.

## When this choice stops being correct

If Dagster or Prefect acquires a provider ecosystem comparable to Airflow's, the argument for Airflow as the integration-layer default weakens. As of 2026, Airflow's provider coverage is the decisive differentiator for multi-source pipelines.

## Alternatives considered

**Airflow only:** Simpler. Rejected because Prefect and Dagster represent genuinely different orchestration philosophies that engineers should see demonstrated, not just described.

**Temporal as a fourth variant:** Rejected — see Rationale above. Temporal solves a different problem.
