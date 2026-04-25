#!/usr/bin/env bash
# sync-pattern-branches.sh
#
# For each variant in patterns/<pattern>/variants/<stack>/,
# creates or force-resets a branch named pattern/<pattern>/<stack>
# containing only that variant's files at the repo root.
#
# Run from the repo root after adding or updating any variant in main.
# Requires: git, a GitHub remote named 'origin', push access.
#
# Usage:
#   ./tooling/sync-pattern-branches.sh             # sync all variants
#   ./tooling/sync-pattern-branches.sh batch-lakehouse  # sync one pattern only

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
MAIN_BRANCH="main"
FILTER_PATTERN="${1:-}"   # optional: sync only this pattern name

cd "$REPO_ROOT"

# Ensure we start on main and it is clean
git checkout "$MAIN_BRANCH"
if [ -n "$(git status --porcelain)" ]; then
  echo "ERROR: working tree is dirty. Commit or stash changes before syncing branches."
  exit 1
fi

MAIN_SHA="$(git rev-parse --short HEAD)"
echo "Syncing pattern branches from main ($MAIN_SHA)..."

synced=0
skipped=0

for pattern_dir in patterns/*/variants/*/; do
  [[ -d "$pattern_dir" ]] || continue

  # Parse path: patterns/<pattern>/variants/<stack>/
  IFS='/' read -ra parts <<< "${pattern_dir%/}"
  pattern="${parts[1]}"
  stack="${parts[3]}"

  if [[ -n "$FILTER_PATTERN" && "$pattern" != "$FILTER_PATTERN" ]]; then
    ((skipped++))
    continue
  fi

  branch="pattern/${pattern}/${stack}"
  echo "→ $branch"

  # Stage variant files in a temp directory
  tmpdir="$(mktemp -d)"
  cp -r "$REPO_ROOT/$pattern_dir/." "$tmpdir/"

  # Create empty root commit for the branch
  EMPTY_TREE="$(git hash-object -t tree /dev/null)"
  EMPTY_COMMIT="$(git commit-tree "$EMPTY_TREE" -m "init: pattern branch root")"
  git branch -f "$branch" "$EMPTY_COMMIT"

  # Populate via worktree (never touches the main working tree)
  worktree_dir="$(mktemp -d)"
  git worktree add --quiet "$worktree_dir" "$branch"

  cp -r "$tmpdir/." "$worktree_dir/"

  (
    cd "$worktree_dir"
    git add --all
    git commit -m "chore: sync $branch from main ($MAIN_SHA)"
  )

  git push origin "$branch" --force
  git worktree remove "$worktree_dir" --force
  rm -rf "$tmpdir"

  ((synced++))
done

git checkout "$MAIN_BRANCH"
echo ""
echo "Done. Synced: $synced branches. Skipped: $skipped branches."
