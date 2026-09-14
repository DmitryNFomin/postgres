#!/usr/bin/env bash
# 01-build-all.sh
#
# Builds the two PostgreSQL trees the benchmark needs. There are only two
# real builds, even though the matrix later runs five configurations:
#
#   build "baseline" = branch bench-v8-baseline  (unmodified PostgreSQL
#                       plus the small benchmark helper extensions)
#   build "patched"  = branch bench-v8-patched   (the wait-event-tracing
#                       patch series, on top of the same commit, plus the
#                       same benchmark helper extensions)
#
# Configuration 1 ("master") runs the baseline build as-is. Configurations
# 2-5 ("hook-null", "module-off", "stats", "trace") all run the SAME
# patched build, and differ only in postgresql.conf -- that happens in
# 02-run-matrix.sh, not here. That is why this step takes ~12 minutes
# (two builds), not ~25 (five builds).
#
# This script only builds and installs. It never starts a server.
set -Eeuo pipefail
export LC_ALL=C

# ---------------------------------------------------------------------------
# EDIT THESE THREE VALUES before running the kit.
# ---------------------------------------------------------------------------
REPO_URL="https://github.com/DmitryNFomin/postgres.git"
COMMIT_BASELINE="765efece39ba3fb04fdf20b1dadcd9ecea76fbc9"
COMMIT_PATCHED="d7b4584a901241258604eef1f03dfd6b3f1fa926"
# ---------------------------------------------------------------------------

BRANCH_BASELINE="bench-v8-baseline"
BRANCH_PATCHED="bench-v8-patched"

die() { echo "01-build-all.sh: ERROR: $*" >&2; exit 1; }
log() { printf '%s\n' "[$(date -u +%H:%M:%S)] $*"; }

for placeholder in "$REPO_URL" "$COMMIT_BASELINE" "$COMMIT_PATCHED"; do
  [[ "$placeholder" != *"@@"* ]] ||
    die "a placeholder (@@...@@) at the top of this script was never filled in. Edit REPO_URL, COMMIT_BASELINE, and COMMIT_PATCHED before running."
done

for tool in git meson ninja python3 sha256sum make cc; do
  command -v "$tool" >/dev/null 2>&1 || die "required tool not found: $tool"
done

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WORK="$SCRIPT_DIR/work"
REPO="$WORK/src/repo"
LOG_DIR="$WORK/build-logs"
MANIFEST="$WORK/manifest.json"
JOBS=$(nproc 2>/dev/null || echo 4)

mkdir -p "$WORK/src" "$LOG_DIR"

log "Working directory: $WORK"
log "Using $JOBS parallel build job(s)"

# ---------------------------------------------------------------------------
# 1. Get the source once.
# ---------------------------------------------------------------------------
if [[ -d "$REPO/.git" ]]; then
  log "Fork already cloned at $REPO; fetching updates"
  git -C "$REPO" fetch --quiet origin
else
  log "Cloning $REPO_URL"
  git clone --quiet "$REPO_URL" "$REPO"
fi

resolve_commit() {
  local label=$1 commit=$2
  if ! git -C "$REPO" cat-file -e "${commit}^{commit}" 2>/dev/null; then
    log "commit $commit ($label) not present locally yet; fetching it directly"
    git -C "$REPO" fetch --quiet origin "$commit" ||
      die "could not fetch commit $commit for $label from $REPO_URL"
  fi
  git -C "$REPO" cat-file -e "${commit}^{commit}" 2>/dev/null ||
    die "commit $commit for $label is still not reachable after fetching"
}
resolve_commit baseline "$COMMIT_BASELINE"
resolve_commit patched "$COMMIT_PATCHED"

# ---------------------------------------------------------------------------
# 2. Build one tree (checkout via a git worktree, meson build, meson
#    install, then the two small benchmark-only extensions that are not
#    part of the normal install set).
# ---------------------------------------------------------------------------
# Performance-measurement build flags, identical for both trees:
#   --buildtype=release   meson's fully optimized build (like --O3, no
#                          debug info) -- this is release-equivalent
#                          optimization, not the project's normal
#                          development default (which is debugoptimized).
#   -Dcassert=false        assertions OFF. This is a timing run, not a
#                          correctness run; assertions add real overhead
#                          and would make every number meaningless.
MESON_FLAGS=(--buildtype=release -Dcassert=false)

FIXTURE_REL="src/test/modules/test_wait_primitive"

: >"$WORK/.build-records.jsonl"

build_one() {
  local name=$1 branch=$2 commit=$3
  local worktree="$WORK/src/$name"
  local builddir="$WORK/build/$name"
  local prefix="$WORK/install/$name"
  local log="$LOG_DIR/$name.log"

  log "== Building '$name' (branch $branch, commit $commit) =="
  : >"$log"

  # Idempotent re-run support: drop any earlier worktree registration for
  # this path (directory contents, and git's own bookkeeping about it)
  # before trying to add it fresh.
  git -C "$REPO" worktree remove --force "$worktree" >>"$log" 2>&1 || true
  rm -rf -- "$worktree" "$builddir" "$prefix"
  git -C "$REPO" worktree prune >>"$log" 2>&1 || true

  git -C "$REPO" worktree add --detach "$worktree" "$commit" >>"$log" 2>&1 ||
    die "'git worktree add' failed for $name; see $log"

  local actual_commit
  actual_commit=$(git -C "$worktree" rev-parse HEAD)
  [[ "$actual_commit" == "$commit" ]] ||
    die "$name checked out $actual_commit, expected $commit"

  log "  meson setup (this is the slow step, several minutes)"
  meson setup "$builddir" "$worktree" --prefix="$prefix" \
    "${MESON_FLAGS[@]}" >>"$log" 2>&1 ||
    die "meson setup failed for $name; see $log"

  log "  meson compile -j$JOBS"
  meson compile -C "$builddir" -j "$JOBS" >>"$log" 2>&1 ||
    die "build failed for $name; see $log"

  log "  meson install"
  meson install -C "$builddir" >>"$log" 2>&1 ||
    die "install failed for $name; see $log"

  # The benchmark's wait-primitive microbenchmark extension lives under
  # src/test/modules, which meson only stages for its own temporary test
  # installs, not for a normal --prefix install. Build and install it by
  # hand with PGXS, against the pg_config we just installed.
  local fixture="$worktree/$FIXTURE_REL"
  if [[ -d "$fixture" ]]; then
    log "  installing benchmark fixture: test_wait_primitive"
    make -C "$fixture" USE_PGXS=1 PG_CONFIG="$prefix/bin/pg_config" \
      -j "$JOBS" >>"$log" 2>&1 ||
      die "building test_wait_primitive failed for $name; see $log"
    make -C "$fixture" USE_PGXS=1 PG_CONFIG="$prefix/bin/pg_config" \
      install >>"$log" 2>&1 ||
      die "installing test_wait_primitive failed for $name; see $log"
  else
    die "$name is missing $FIXTURE_REL -- both bench-v8-* branches are supposed to carry this fixture. Checked: $fixture"
  fi

  [[ -x "$prefix/bin/postgres" ]] ||
    die "$name build did not produce $prefix/bin/postgres"

  local pg_sha module_sha=none module_path
  pg_sha=$(sha256sum "$prefix/bin/postgres" | awk '{print $1}')
  module_path=$(find "$prefix/lib" -maxdepth 2 -name 'pg_wait_event_tracing.*' -print -quit 2>/dev/null || true)
  if [[ -n "$module_path" && -f "$module_path" ]]; then
    module_sha=$(sha256sum "$module_path" | awk '{print $1}')
  fi

  # Hard assertion: the whole matrix silently measures nothing useful if
  # COMMIT_PATCHED and COMMIT_BASELINE ever point at the wrong branch (e.g.
  # both missing the series, or both carrying it). Catch that here, at
  # build time, rather than after a 2.5-3.5 hour run.
  case "$name" in
    patched)
      [[ "$module_sha" != none ]] ||
        die "'$name' (commit $commit) does NOT contain pg_wait_event_tracing, but it is supposed to be the wait-event-tracing series build. Likely cause: COMMIT_PATCHED is not actually on $branch, or that branch is missing the contrib module."
      ;;
    baseline)
      [[ "$module_sha" == none ]] ||
        die "'$name' (commit $commit) UNEXPECTEDLY contains pg_wait_event_tracing ($module_path), but it is supposed to be the unmodified baseline. Likely cause: COMMIT_BASELINE is not actually on $branch, or points at a commit that already has the series applied."
      ;;
  esac

  python3 - "$WORK/.build-records.jsonl" "$name" "$branch" "$commit" \
    "$prefix" "$pg_sha" "$module_sha" <<'PY'
import json
import sys
out, name, branch, commit, prefix, pg_sha, module_sha = sys.argv[1:]
with open(out, "a", encoding="utf-8") as f:
    json.dump({
        "name": name, "branch": branch, "commit": commit, "prefix": prefix,
        "postgres_sha256": pg_sha, "pg_wait_event_tracing_sha256": module_sha,
    }, f, sort_keys=True)
    f.write("\n")
PY

  log "== '$name' done: $prefix =="
}

build_one baseline "$BRANCH_BASELINE" "$COMMIT_BASELINE"
build_one patched "$BRANCH_PATCHED" "$COMMIT_PATCHED"

# ---------------------------------------------------------------------------
# 3. Record a manifest: what was built, with what tools, and what came out.
# ---------------------------------------------------------------------------
COMPILER_VERSION=$(cc --version 2>&1 | head -1)
MESON_VERSION=$(meson --version 2>&1)
CONFIGURE_LINE="meson setup <builddir> <srcdir> --prefix=<prefix> ${MESON_FLAGS[*]}"

python3 - "$MANIFEST" "$WORK/.build-records.jsonl" "$REPO_URL" \
  "$COMPILER_VERSION" "$MESON_VERSION" "$CONFIGURE_LINE" "$JOBS" <<'PY'
import datetime
import json
import platform
import sys

out, records_path, repo_url, cc_version, meson_version, configure_line, \
    jobs = sys.argv[1:]
with open(records_path, encoding="utf-8") as f:
    builds = [json.loads(line) for line in f if line.strip()]
manifest = {
    "schema_version": 1,
    "created_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "build_host": platform.node(),
    "repo_url": repo_url,
    "compiler_version": cc_version,
    "meson_version": meson_version,
    "configure_line": configure_line,
    "optimization": "release-equivalent (--buildtype=release), assertions OFF (-Dcassert=false)",
    "parallel_jobs": int(jobs),
    "builds": builds,
}
with open(out, "w", encoding="utf-8") as f:
    json.dump(manifest, f, indent=2, sort_keys=True)
    f.write("\n")
PY

log ""
log "Build manifest: $MANIFEST"
log "Installed prefixes:"
log "  baseline: $WORK/install/baseline"
log "  patched:  $WORK/install/patched"
log ""
log "Both trees built with: $CONFIGURE_LINE"
log "All builds succeeded. Next step: ./02-run-matrix.sh"
