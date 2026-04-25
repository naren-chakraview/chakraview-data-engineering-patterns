# ADR-0008: Local Dev Approach — Docker Compose Per Variant

**Status**: Accepted
**Date**: 2026-04-25
**Deciders**: Portfolio architect

---

## Context

Every boilerplate variant in this repo needs a local development environment that an engineer can bring up without cloud credentials, managed services, or a Kubernetes cluster. The approach must work on a standard developer laptop (macOS or Linux, 16GB RAM, Docker Desktop or Docker Engine installed).

Several approaches exist: Docker Compose, Minikube/K3s, devcontainers, Nix, and manual process management.

## Decision

Use **Docker Compose** as the local dev environment for every variant. Each variant ships a self-contained `docker-compose.yml` at its root.

## Rationale

### Why Docker Compose

**Lowest barrier:** Docker Compose requires only Docker Desktop (or Docker Engine + Compose plugin). It requires no Kubernetes knowledge, no cluster provisioning, and no cloud account. `docker compose up -d` is a universally understood command.

**Self-contained:** Each variant's `docker-compose.yml` is self-describing. It lists every service the variant needs, its image version, configuration, and health checks. An engineer reading the file understands the full local stack without reading any other documentation.

**Realistic local stacks:** Modern Docker Compose can run Kafka, Flink, Spark (standalone), MinIO, PostgreSQL, and Trino on a single machine with reasonable resource constraints. The local stacks in this repo are designed to fit within 8GB of RAM.

**No cloud credentials for the happy path:** MinIO replaces S3; local PostgreSQL replaces RDS; Kafka (or Redpanda) runs in a container. Engineers can run the full pipeline without any cloud account.

### Resource budgets

Each variant's `docker-compose.yml` is designed to stay within these bounds:

| Pattern type | Target RAM |
|---|---|
| Batch Lakehouse (Spark + MinIO) | 6GB |
| Streaming Lakehouse (Flink + MinIO) | 8GB |
| ELT (dbt + DuckDB) | 2GB |
| CDC (Kafka + Debezium + Flink/Spark) | 8GB |
| Federated Query (Trino + MinIO) | 6GB |
| ML Feature Pipeline (Redis + MinIO) | 4GB |
| Graph (Neo4j or MinIO only) | 4GB |
| Workflow Orchestration (Airflow/Prefect/Dagster) | 4GB |

### .env.example

Every variant ships a `.env.example` with default values for all required environment variables. The `docker-compose.yml` reads from `.env` (which the engineer creates by `cp .env.example .env`). This pattern makes credential injection explicit and prevents accidental credential commits.

## Consequences

**Positive:**

- Every engineer with Docker can run any variant in under 5 minutes with no configuration beyond `cp .env.example .env && docker compose up -d`.
- Health checks in each `docker-compose.yml` ensure services are ready before dependent services start, reducing "it didn't work the first time" friction.

**Negative:**

- Docker Compose is not a production deployment model. Engineers who want to deploy a variant to Kubernetes must translate the Compose file to Helm charts or Kustomize manifests. This is intentional — the boilerplate is a local starter, not a production config.
- Resource-constrained laptops (< 8GB RAM) will struggle with the CDC and streaming lakehouse stacks.

## When this choice stops being correct

If devcontainers become the universal local dev standard (VS Code Remote Containers + GitHub Codespaces), a `devcontainer.json` that wraps the Docker Compose file would be a natural addition. The Docker Compose approach is not incompatible with devcontainers — it would be an additive change.

## Alternatives considered

**Minikube / K3s:** Closer to production Kubernetes. Rejected because it requires Kubernetes knowledge, adds cluster provisioning complexity, and is materially harder to bring up than Docker Compose for an engineer evaluating a boilerplate.

**Nix:** Reproducible, hermetic environments. Rejected because Nix has a steep learning curve and is not universally installed. Docker is more universally available on developer machines.

**No local dev (cloud-only):** Rejected — the core value of a boilerplate starter is that an engineer can run it immediately without provisioning cloud infrastructure.
