# ADR-0013: Entity Resolution Strategy — Deterministic IRI Minting

**Status**: Accepted  
**Date**: 2026-05-30  
**Deciders**: Portfolio architect

---

## Context

Semantic medallion's core challenge is **entity identity**: given a customer record from an order system with ID `cust_12345` and another from a loyalty system with ID `loyalty_99876`, how do we know they're the same entity?

Two strategies for entity resolution:

1. **Deterministic hashing:** Mint IRIs (Internationalized Resource Identifiers, URIs for RDF entities) by hashing entity key fields. E.g., `customer:sha256:abc123def` where the hash is computed from (source_system, customer_id). Same entity always produces the same IRI.

2. **ML-based entity resolution:** Train a model on labeled entity pairs; use it to match incoming records probabilistically. Returns a confidence score for each match.

We need a strategy that:
- Is reproducible (running the same code twice produces identical results)
- Has no training data dependency
- Generates stable entity IRIs that persist across medallion stages
- Provides an audit trail (which fields were used for deduplication?)

## Decision

**Use deterministic IRI minting via SHA256 hashing at the CDC ingest layer.** Specifically:

1. **Identify entity resolution keys:** For each entity type (customer, product, vendor), choose the fields that uniquely identify an instance *within a source system*. E.g., for Customer: (source_system, customer_id).

2. **Mint IRIs:** At the silver layer (during CDC ingest), compute `IRI = "entity:" + entity_type + ":" + sha256(canonical_key_fields)`. Store this IRI in the silver table alongside the original entity ID.

3. **Link to semantic layer:** When extracting RDF, use this deterministic IRI as the RDF subject. All statements about this entity, regardless of source system, use the same IRI.

4. **Deduplicate via owl:sameAs:** If two source systems use different key structures (e.g., Customer has primary_key (source, customer_id) in one system but (source, email) in another), manually assert `owl:sameAs` triples linking the two IRIs. This is a small, auditable set of deduplication rules.

## Rationale

**Why deterministic hashing?**

1. **Reproducibility:** Same input → same IRI, always. No randomness or stateful decisions. Running the same Spark job twice produces identical silver tables.

2. **No training data:** ML-based approaches require labeled entity pairs; we may not have them upfront. Deterministic hashing works from day one.

3. **Fast:** SHA256 is a built-in operation in Spark/Python; no inference latency.

4. **Audit trail:** The hash function and key fields are explicit in code. An auditor can trace which fields drove an entity ID decision.

5. **Conflict-free:** Two entities from different sources with the same canonical keys will get the same IRI (collision), which is correct — they're the same entity. If keys are chosen poorly, we'll see duplicates that trigger investigation.

6. **Schema evolution friendly:** If the entity key evolves (e.g., adding a region field), we can create a new key version (`entity:v2:sha256:...`) and maintain both old and new IRIs via `owl:sameAs`.

**When is deterministic hashing insufficient?**

- **Fuzzy matching:** If entity keys aren't stable (e.g., customer name spelled differently in two systems), hashing will fail. Requires manual deduplication or ML augmentation.
- **Cross-domain entities:** A person in both order and HR systems might have different ID schemes; hashing each independently produces different IRIs. Requires manual `owl:sameAs` to link them.

## Consequences

**Positive:**

- Reproducible: auditable, idempotent, fast.
- Requires no external dependencies (no ML model, no training data).
- Scales well; hashing is O(1).
- Integrates naturally with CDC: mint IRIs at ingest time, before any downstream processing.
- Simple to implement: one Spark SQL function per entity type.

**Negative:**

- Requires upfront entity key design; poor key choices lead to wrong deduplication.
- Doesn't handle fuzzy matches (name variations, typos).
- Manual `owl:sameAs` assertions don't scale beyond ~1000 cross-domain links.
- If the canonical key fields are ambiguous or change over time, IRIs become unstable.

## Mitigation strategies

1. **Entity key review:** Document entity keys in code comments and ADR references. Have entity owners review for completeness and stability.

2. **Staged deduplication:** Start with conservative entity keys (primary keys only); layer on fuzzy matching via ML later if needed.

3. **Versioning:** Support entity key versioning (`entity:v1`, `entity:v2`). When keys evolve, create new versions and link with `owl:sameAs`.

4. **Monitoring:** Track IRI collision rates (multiple records with the same IRI). High collision rate suggests poor key design.

5. **Augmentation:** If deterministic hashing is insufficient, use ML entity resolution to generate `owl:sameAs` candidates with confidence scores. Deterministic hashing remains the baseline.

## Alternatives considered

**Option A: ML-based entity resolution**

Train a model on labeled entity pairs (e.g., "customer_12345 from OrderSys == loyalty_99876 from LoyaltySys"). Use it to predict matches for new records.

Rejected for initial pattern because:
- Requires labeled training data, which may not exist upfront.
- Adds inference latency to ingest pipeline.
- Confidence scores create ambiguity (how confident must a match be to deduplicate?).
- Harder to audit; machine learning decisions are opaque.

*Mitigation:* Provide a follow-up guide on augmenting deterministic hashing with ML scoring if needed.

**Option B: Manual master data management**

Operators manually maintain a mapping table (source_system_A_id → canonical_id, source_system_B_id → canonical_id).

Rejected because:
- Doesn't scale beyond small entity sets.
- Manual mapping is error-prone and hard to audit.
- Breaks reproducibility; operators can make inconsistent decisions.

**Option C: Federated identity (URNs for each source)**

Each source system assigns its own URN (e.g., `urn:order:cust_12345`, `urn:loyalty:loyalty_99876`). Query-time federation links them via SPARQL `OPTIONAL` clauses.

Rejected because:
- Defers deduplication to query time, increasing query complexity.
- No single canonical entity ID; every query must account for aliases.
- Harder to maintain referential integrity.

## Implementation checklist

- [ ] Define entity types (Customer, Product, Vendor, etc.)
- [ ] For each entity type, choose entity resolution keys (fields that uniquely identify within source)
- [ ] Document keys in ADR or code comment (rationale for why these fields were chosen)
- [ ] Implement IRI minting in silver layer (Spark SQL or Scala UDF)
- [ ] Add IRI columns to silver tables
- [ ] Test: run the same ingest twice, verify IRIs are identical
- [ ] Monitor: track collision rates; alert if high
- [ ] Plan cross-domain deduplication: document manual `owl:sameAs` assertions in a configuration file
