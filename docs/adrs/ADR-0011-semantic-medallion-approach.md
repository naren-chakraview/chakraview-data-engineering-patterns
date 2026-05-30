# ADR-0011: Semantic Medallion — Extension vs Standalone Pattern

**Status**: Accepted  
**Date**: 2026-05-30  
**Deciders**: Portfolio architect

---

## Context

Entity unification across polyglot data sources is a common problem in modern data estates. A customer exists under multiple IDs in an order system, a loyalty system, and a marketing platform. Products are sourced from an internal catalog, a vendor system, and a marketplace API.

Two approaches to solving this:

1. **Standalone pattern:** Design a new "Semantic Medallion" pattern from scratch, with its own bronze/silver/gold stages, RDF storage layer, and SPARQL query engine.
2. **Extension approach:** Layer semantic reasoning (RDF + SPARQL) onto three existing patterns (Batch Lakehouse, CDC Pipeline, Federated Query) as optional variants. This allows engineers to add entity resolution to a pattern they're already using, without learning a completely new architecture.

## Decision

**Adopt the extension approach.** Implement semantic medallion as three optional variants:

- `spark-iceberg-semantic`: Extends Batch Lakehouse with entity deduplication via RDF
- `debezium-kafka-semantic`: Extends CDC Pipeline with real-time entity resolution at ingest
- `trino-jena-sparql`: Extends Federated Query with SPARQL cross-domain reasoning

Each variant inherits the base pattern's infrastructure (medallion stages, storage, orchestration) and adds an RDF/SPARQL layer on top.

## Rationale

**Why extensions and not a standalone pattern?**

1. **Code reuse:** Engineers who choose a semantic variant already understand the bronze/silver/gold medallion structure. They can reuse Spark jobs, CDC pipelines, and Trino configurations. A standalone pattern would require duplicating all three.

2. **Lower barrier to adoption:** An engineer choosing Batch Lakehouse for a project can later decide that entity resolution is needed and switch to `spark-iceberg-semantic`. They don't have to redesign their entire pipeline or rewrite CDC consumers. The transition is incremental.

3. **Documentation leverage:** We document medallion stages once; semantic variants reference the base pattern. Reduces duplication and maintenance burden.

4. **Evolutionary fit:** Entity resolution is often a *later* concern, not an upfront requirement. A team might start with Batch Lakehouse, then realize they need to deduplicate customer IDs from two sources. Variants let them evolve without rearchitecting.

5. **Stack clarity:** Three variants with clear base patterns (Batch + RDF, CDC + RDF, Federated + RDF) are easier to understand than one abstract "Semantic Medallion" pattern that blurs the underlying architecture.

## Consequences

**Positive:**

- Engineers reuse medallion patterns they already know; RDF is additive, not disruptive.
- Each variant is independently deployable; teams adopt semantic reasoning only where needed.
- Documentation is cleaner; we avoid a deep inheritance hierarchy of patterns.
- Lower learning curve; focus on entity IRI design and SPARQL queries, not a new medallion structure.

**Negative:**

- Maintain three variant codebases instead of one. However, they differ mainly in entity extraction and SPARQL translation; core medallion logic is shared.
- Some duplication of RDF/SPARQL infrastructure across variants. Mitigated by factoring shared code into a library (e.g., `semantic-core` module).
- Engineers unfamiliar with the base patterns must first understand medallion before understanding semantic extensions.

## When this choice stops being correct

If we later add 3+ more semantic-adjacent patterns (e.g., semantic graph processing, semantic feature stores), a standalone "semantic platform" might be justified. For now, the extension approach aligns with the pattern portfolio's design principle: each pattern stands alone, with variants for stack choices, not architectural layers.

## Alternatives considered

**Option A: Standalone Semantic Medallion pattern**

A new pattern with its own medallion stages, entity resolution, RDF storage, and SPARQL querying. Engineers choose it instead of choosing Batch Lakehouse or CDC Pipeline.

Rejected because:
- Requires relearning medallion structure; no code reuse from base patterns.
- Harder to integrate with existing Batch or CDC pipelines; engineers choose one or the other, not both.
- Documentation duplication; we'd describe medallion stages three times (once for each of Batch, CDC, Semantic).

**Option B: Semantic as a cross-cutting concern, part of every pattern**

Extend Batch Lakehouse, CDC Pipeline, and Federated Query by making RDF optional within each, controlled by a configuration flag.

Rejected because:
- Makes the base patterns more complex; adds conditional code paths.
- Encourages half-baked semantic implementations; engineers toggle it on without understanding entity resolution implications.
- Harder to test; more configuration permutations.

**Option C: Semantic-only repository**

Create a separate `chakraview-semantic-data-patterns` repo with semantic variants only. The main patterns repo stays pure medallion.

Rejected because:
- Fragments the pattern library; engineers must search two repos to find the right variant.
- Semantic variants reference base patterns in another repo; documentation links break if repos diverge.
- Organizational overhead; two repos = two release cycles, two docs sites, two CI/CD pipelines.
