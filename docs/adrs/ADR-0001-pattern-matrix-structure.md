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
