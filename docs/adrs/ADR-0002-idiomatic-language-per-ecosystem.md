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
