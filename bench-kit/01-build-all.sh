#!/usr/bin/env bash
# Build two independent baselines plus the pinned v9 and v10 trees.
#
# Every build uses the same source, build, and install path.  The completed
# installation is copied to an equal-length runtime prefix before the next
# build starts.  This prevents source/prefix string lengths from becoming an
# accidental code-layout difference in a benchmark intended to measure only
# the patch.
set -Eeuo pipefail
export LC_ALL=C

REPO_URL="https://github.com/DmitryNFomin/postgres.git"
COMMIT_BASELINE="765efece39ba3fb04fdf20b1dadcd9ecea76fbc9"
COMMIT_V9="40bffed8a92291c27a5d1956a5cd18dd3609f397"
COMMIT_V10="c12783fbf86e8116526afe4566d58bf90c3478e0"
BRANCH_BASELINE="bench-v10-baseline"
BRANCH_V9="bench-v10-v9-reference"
BRANCH_V10="bench-v10-attachment-guard"

die() { echo "01-build-all.sh: ERROR: $*" >&2; exit 1; }
log() { printf '%s\n' "[$(date -u +%H:%M:%S)] $*"; }

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

for placeholder in "$REPO_URL" "$COMMIT_BASELINE" "$COMMIT_V9" "$COMMIT_V10"; do
  [[ "$placeholder" != *"@@"* ]] ||
    die "an unfilled @@...@@ placeholder remains in the pinned source values"
done

for tool in python3 sha256sum make cc ar ranlib find hostname tar; do
  command -v "$tool" >/dev/null 2>&1 || die "required tool not found: $tool"
done

HOST_CHECK="$SCRIPT_DIR/host-check.json"
SOURCE_DIR="$SCRIPT_DIR/source"
SOURCE_MANIFEST="$SOURCE_DIR/source-manifest.json"
SOURCE_ARCHIVE_BASELINE="$SOURCE_DIR/postgres-baseline.tar.gz"
SOURCE_ARCHIVE_V9="$SOURCE_DIR/postgres-v9.tar.gz"
SOURCE_ARCHIVE_V10="$SOURCE_DIR/postgres-v10.tar.gz"
WORK="$SCRIPT_DIR/work"
ACTIVE_SOURCE="$WORK/src/active"
ACTIVE_BUILD="$WORK/build/active"
ACTIVE_PREFIX="$WORK/prefix/active"
INSTALL_ROOT="$WORK/install"
LOG_DIR="$WORK/build-logs"
MANIFEST="$WORK/manifest.json"
RECORDS="$WORK/.build-records.jsonl"
JOBS=${BUILD_JOBS:-$(nproc 2>/dev/null || echo 4)}
FIXTURE_REL="src/test/modules/test_wait_primitive"
CONFIGURE_FLAGS=(
  --disable-rpath
  --without-icu
  --without-readline
  --without-zlib
)

hash_file() {
  sha256sum "$1" | awk '{print $1}'
}

[[ -f "$HOST_CHECK" ]] ||
  die "missing $HOST_CHECK -- run ./00-check-host.sh first"
python3 - "$HOST_CHECK" "$(hostname)" <<'PY' ||
import json
import sys

path, hostname = sys.argv[1:]
with open(path, encoding="utf-8") as stream:
    report = json.load(stream)
if report.get("warning_count") != 0:
    raise SystemExit("host check contains warnings")
if report.get("hostname") != hostname:
    raise SystemExit("host check was created on a different host")
PY
  die "host check is not a clean report for this host"

case " ${CC:-} " in
  *ccache*|*sccache*)
    die "CC must not use ccache or sccache; independent builds must compile"
    ;;
esac

mkdir -p "$WORK/src" "$WORK/build" "$WORK/prefix" "$INSTALL_ROOT" "$LOG_DIR"

log "Working directory: $WORK"
log "Using $JOBS parallel build job(s)"

for source_file in \
  "$SOURCE_MANIFEST" \
  "$SOURCE_ARCHIVE_BASELINE" \
  "$SOURCE_ARCHIVE_V9" \
  "$SOURCE_ARCHIVE_V10"; do
  [[ -f "$source_file" && ! -L "$source_file" ]] ||
    die "missing regular bundled-source file: $source_file"
done
log "Using bundled pinned source archives (no network required)"
SOURCE_METADATA_TEXT=$(
  python3 - "$SOURCE_MANIFEST" "$SOURCE_ARCHIVE_BASELINE" \
    "$SOURCE_ARCHIVE_V9" "$SOURCE_ARCHIVE_V10" "$REPO_URL" \
    "$COMMIT_BASELINE" "$COMMIT_V9" "$COMMIT_V10" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

(manifest_path, baseline_path, v9_path, v10_path, repo_url, baseline_commit,
 v9_commit, v10_commit) = sys.argv[1:]
manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
if manifest.get("schema_version") != 3:
    raise SystemExit("unsupported bundled-source manifest")
if manifest.get("benchmark_series") != "wet-v10":
    raise SystemExit("unexpected bundled-source benchmark series")
if manifest.get("repo_url") != repo_url:
    raise SystemExit("bundled-source repository URL mismatch")
if manifest.get("commits") != {
    "baseline": baseline_commit,
    "v9": v9_commit,
    "v10": v10_commit,
}:
    raise SystemExit("bundled-source commit mismatch")
if manifest.get("comparison") != {
    "name": "inline-attachment-needed-guard",
    "reference_commit": v9_commit,
    "treatment_commit": v10_commit,
    "treatment_parent_commit": v9_commit,
}:
    raise SystemExit("bundled-source comparison metadata mismatch")
if manifest.get("reference") != {
    "name": "null-hook-fast-path",
    "commit": v9_commit,
    "parent_commit": "d7b4584a901241258604eef1f03dfd6b3f1fa926",
    "branch_prediction_hint": "none",
}:
    raise SystemExit("bundled-source reference metadata mismatch")
archives = manifest.get("archives")
if not isinstance(archives, dict) or set(archives) != {
    "baseline", "v9", "v10"
}:
    raise SystemExit("bundled-source archive set mismatch")
for name, path_text in (
    ("baseline", baseline_path),
    ("v9", v9_path),
    ("v10", v10_path),
):
    digest = hashlib.sha256()
    with Path(path_text).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    if archives[name].get("filename") != Path(path_text).name:
        raise SystemExit(f"bundled {name} source filename mismatch")
    if digest.hexdigest() != archives[name].get("sha256"):
        raise SystemExit(f"bundled {name} source hash mismatch")
print(manifest["common_base"])
print(manifest["source_date_epoch"])
print(manifest["fixture_tree"])
PY
) || die "bundled source archives failed verification"
mapfile -t SOURCE_METADATA <<<"$SOURCE_METADATA_TEXT"
[[ ${#SOURCE_METADATA[@]} -eq 3 ]] ||
  die "bundled-source manifest returned incomplete metadata"
COMMON_BASE=${SOURCE_METADATA[0]}
REPRO_EPOCH=${SOURCE_METADATA[1]}
FIXTURE_TREE_BASELINE=${SOURCE_METADATA[2]}
FIXTURE_TREE_V9=${SOURCE_METADATA[2]}
FIXTURE_TREE_V10=${SOURCE_METADATA[2]}
cp "$SOURCE_MANIFEST" "$WORK/source-manifest.json"

[[ "$COMMON_BASE" == 0c5d6269614e107d1d2d669f82f63f7e232b30c9 ]] ||
  die "unexpected baseline/v9/v10 common base: $COMMON_BASE"
[[ "$REPRO_EPOCH" == 1789301160 ]] ||
  die "unexpected reproducibility epoch: $REPRO_EPOCH"
[[ "$FIXTURE_TREE_BASELINE" == "$FIXTURE_TREE_V9" &&
   "$FIXTURE_TREE_BASELINE" == "$FIXTURE_TREE_V10" ]] ||
  die "benchmark fixture trees differ across baseline, v9, and v10"

: >"$RECORDS"

build_one() {
  local name=$1 branch=$2 commit=$3 runtime_leaf=$4
  local runtime_prefix="$INSTALL_ROOT/$runtime_leaf"
  local log="$LOG_DIR/$name.log"
  local compiler_json="$LOG_DIR/$name-compilers.json"
  local install_tree_json="$LOG_DIR/$name-install-tree.json"

  log "== Building '$name' (branch $branch, commit $commit) =="
  : >"$log"

  rm -rf -- "$ACTIVE_SOURCE" "$ACTIVE_BUILD" "$ACTIVE_PREFIX" "$runtime_prefix"
  local archive=$SOURCE_ARCHIVE_BASELINE
  case "$name" in
    v9) archive=$SOURCE_ARCHIVE_V9 ;;
    v10) archive=$SOURCE_ARCHIVE_V10 ;;
  esac
  mkdir -p "$ACTIVE_SOURCE"
  tar -xzf "$archive" -C "$ACTIVE_SOURCE" >>"$log" 2>&1 ||
    die "extracting bundled source failed for $name; see $log"

  log "  configure (ICU, readline, and zlib disabled; not used by workloads)"
  mkdir -p "$ACTIVE_BUILD"
  (
    cd "$ACTIVE_BUILD"
    SOURCE_DATE_EPOCH="$REPRO_EPOCH" \
      CCACHE_DISABLE=1 SCCACHE_RECACHE=1 \
      CC=cc \
      "$ACTIVE_SOURCE/configure" --prefix="$ACTIVE_PREFIX" \
        "${CONFIGURE_FLAGS[@]}"
  ) >>"$log" 2>&1 ||
    die "configure failed for $name; see $log"

  python3 - "$compiler_json" "$(command -v cc)" <<'PY' ||
import hashlib
import json
import subprocess
import sys
from pathlib import Path

out, compiler_path = map(Path, sys.argv[1:])
version = subprocess.run(
    [str(compiler_path), "--version"],
    check=True,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
).stdout.splitlines()
data = {
    "c": {
        "command": "cc",
        "path": str(compiler_path),
        "sha256": hashlib.sha256(compiler_path.read_bytes()).hexdigest(),
        "version": version[0] if version else "unknown",
    }
}
out.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
PY
    die "could not record compiler metadata for $name"

  log "  make -j$JOBS"
  SOURCE_DATE_EPOCH="$REPRO_EPOCH" \
    CCACHE_DISABLE=1 SCCACHE_RECACHE=1 \
    make -C "$ACTIVE_BUILD" -j "$JOBS" >>"$log" 2>&1 ||
    die "build failed for $name; see $log"

  log "  make install"
  SOURCE_DATE_EPOCH="$REPRO_EPOCH" \
    CCACHE_DISABLE=1 SCCACHE_RECACHE=1 \
    make -C "$ACTIVE_BUILD" install >>"$log" 2>&1 ||
    die "install failed for $name; see $log"

  if [[ "$name" == v9 || "$name" == v10 ]]; then
    local tracing_extension="$ACTIVE_SOURCE/contrib/pg_wait_event_tracing"
    log "  installing $name tracing extension"
    SOURCE_DATE_EPOCH="$REPRO_EPOCH" \
      CCACHE_DISABLE=1 SCCACHE_RECACHE=1 \
      make -C "$tracing_extension" USE_PGXS=1 \
        PG_CONFIG="$ACTIVE_PREFIX/bin/pg_config" -j "$JOBS" \
        >>"$log" 2>&1 ||
      die "building pg_wait_event_tracing failed for $name; see $log"
    SOURCE_DATE_EPOCH="$REPRO_EPOCH" \
      CCACHE_DISABLE=1 SCCACHE_RECACHE=1 \
      make -C "$tracing_extension" USE_PGXS=1 \
        PG_CONFIG="$ACTIVE_PREFIX/bin/pg_config" install \
        >>"$log" 2>&1 ||
      die "installing pg_wait_event_tracing failed for $name; see $log"
  fi

  local fixture="$ACTIVE_SOURCE/$FIXTURE_REL"
  [[ -d "$fixture" ]] ||
    die "$name is missing the byte-identical benchmark fixture at $fixture"
  log "  installing benchmark fixture: test_wait_primitive"
  SOURCE_DATE_EPOCH="$REPRO_EPOCH" \
    CCACHE_DISABLE=1 SCCACHE_RECACHE=1 \
    make -C "$fixture" USE_PGXS=1 PG_CONFIG="$ACTIVE_PREFIX/bin/pg_config" \
      -j "$JOBS" >>"$log" 2>&1 ||
    die "building test_wait_primitive failed for $name; see $log"
  SOURCE_DATE_EPOCH="$REPRO_EPOCH" \
    CCACHE_DISABLE=1 SCCACHE_RECACHE=1 \
    make -C "$fixture" USE_PGXS=1 PG_CONFIG="$ACTIVE_PREFIX/bin/pg_config" \
      install >>"$log" 2>&1 ||
    die "installing test_wait_primitive failed for $name; see $log"

  # Static archives are build-only artifacts and can carry archive-member
  # timestamps on some toolchains.  No benchmark process or later build uses
  # them, so keep the four runtime installations limited to executable
  # artifacts whose complete trees must reproduce byte for byte.
  find "$ACTIVE_PREFIX" -type f -name '*.a' -delete

  [[ -x "$ACTIVE_PREFIX/bin/postgres" ]] ||
    die "$name did not produce an installed postgres executable"

  # Copy only after the build is complete.  The next build reuses these exact
  # active paths, while runtime paths have equal-length leaf names.
  cp -a "$ACTIVE_PREFIX" "$runtime_prefix"
  LD_LIBRARY_PATH="$runtime_prefix/lib:${LD_LIBRARY_PATH:-}" \
    "$runtime_prefix/bin/postgres" --version >>"$log" 2>&1 ||
    die "relocated $name installation is not executable"

  python3 - "$runtime_prefix" "$install_tree_json" <<'PY' ||
import hashlib
import json
import os
import sys
from pathlib import Path

root, out = map(Path, sys.argv[1:])
tree = {}
for path in sorted(root.rglob("*")):
    relative = path.relative_to(root).as_posix()
    if path.is_symlink():
        tree[relative] = {"type": "symlink", "target": os.readlink(path)}
    elif path.is_file():
        tree[relative] = {
            "type": "file",
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
    elif not path.is_dir():
        raise SystemExit(f"special installed path: {relative}")
out.write_text(json.dumps(tree, indent=2, sort_keys=True) + "\n")
PY
    die "could not record the installed file tree for $name"

  local module_path fixture_path
  local module_sha=none fixture_sha
  module_path=$(find "$runtime_prefix/lib" -maxdepth 2 \
    -name 'pg_wait_event_tracing.*' -type f -print -quit)
  fixture_path=$(find "$runtime_prefix/lib" -maxdepth 2 \
    -name 'test_wait_primitive.*' -type f -print -quit)
  [[ -n "$fixture_path" ]] ||
    die "$name did not install the test_wait_primitive shared library"
  fixture_sha=$(hash_file "$fixture_path")
  if [[ -n "$module_path" ]]; then
    module_sha=$(hash_file "$module_path")
  fi

  case "$name" in
    v9|v10)
      [[ "$module_sha" != none ]] ||
        die "$name commit does not install pg_wait_event_tracing"
      ;;
    baseline-a|baseline-b)
      [[ "$module_sha" == none ]] ||
        die "$name unexpectedly installs pg_wait_event_tracing"
      ;;
    *) die "unknown build name: $name" ;;
  esac

  python3 - "$RECORDS" "$name" "$branch" "$commit" "$runtime_prefix" \
    "$ACTIVE_PREFIX" "$compiler_json" "$install_tree_json" \
    "$FIXTURE_TREE_BASELINE" \
    "$(hash_file "$runtime_prefix/bin/postgres")" \
    "$(hash_file "$runtime_prefix/bin/pgbench")" \
    "$(hash_file "$runtime_prefix/bin/psql")" \
    "$(hash_file "$runtime_prefix/bin/initdb")" \
    "$(hash_file "$runtime_prefix/bin/pg_ctl")" \
    "$fixture_sha" "$module_sha" \
    "$(hash_file "$log")" "$(hash_file "$compiler_json")" \
    "$(hash_file "$install_tree_json")" <<'PY'
import json
import sys

(out, name, branch, commit, runtime_prefix, compile_prefix, compiler_path,
 install_tree_path, fixture_tree, postgres_sha, pgbench_sha, psql_sha,
 initdb_sha, pg_ctl_sha, fixture_sha, module_sha, build_log_sha,
 compiler_json_sha, install_tree_json_sha) = sys.argv[1:]
with open(compiler_path, encoding="utf-8") as f:
    compilers = json.load(f)
with open(install_tree_path, encoding="utf-8") as f:
    install_tree = json.load(f)
record = {
    "name": name,
    "branch": branch,
    "commit": commit,
    "runtime_prefix": runtime_prefix,
    "compile_prefix": compile_prefix,
    "compiler": compilers,
    "install_tree": install_tree,
    "fixture_tree": fixture_tree,
    "provenance_sha256": {
        "build_log": build_log_sha,
        "compiler_json": compiler_json_sha,
        "install_tree_json": install_tree_json_sha,
    },
    "sha256": {
        "postgres": postgres_sha,
        "pgbench": pgbench_sha,
        "psql": psql_sha,
        "initdb": initdb_sha,
        "pg_ctl": pg_ctl_sha,
        "test_wait_primitive": fixture_sha,
        "pg_wait_event_tracing": module_sha,
    },
}
with open(out, "a", encoding="utf-8") as f:
    json.dump(record, f, sort_keys=True)
    f.write("\n")
PY

  log "== '$name' done: $runtime_prefix =="
}

verify_baseline_reproducibility() {
  python3 - "$RECORDS" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    records = {
        item["name"]: item
        for item in (json.loads(line) for line in stream if line.strip())
    }
if set(records) != {"baseline-a", "baseline-b"}:
    raise SystemExit(
        f"unexpected early baseline records: {sorted(records)}"
    )
left = records["baseline-a"]
right = records["baseline-b"]
if left["fixture_tree"] != right["fixture_tree"]:
    raise SystemExit("independent baseline fixture trees differ")
if left["install_tree"] != right["install_tree"]:
    raise SystemExit("independent baseline installation trees differ")
for name, digest in left["sha256"].items():
    if digest != right["sha256"].get(name):
        raise SystemExit(
            f"independent baseline builds differ for {name}: "
            f"{digest} != {right['sha256'].get(name)}"
        )
PY
}

# Equal runtime prefix lengths make their path strings equally capable of
# affecting runtime layout. Compilation itself always uses ACTIVE_PREFIX.
for leaf in base-a base-b v9-ref v10opt; do
  [[ ${#leaf} -eq 6 ]] || die "runtime prefix leaf '$leaf' is not six bytes"
done

build_one baseline-a "$BRANCH_BASELINE" "$COMMIT_BASELINE" base-a
build_one baseline-b "$BRANCH_BASELINE" "$COMMIT_BASELINE" base-b
verify_baseline_reproducibility ||
  die "independent baseline builds differ; v9/v10 builds were not started"
log "Independent baseline reproducibility: PASS"
build_one v9 "$BRANCH_V9" "$COMMIT_V9" v9-ref
build_one v10 "$BRANCH_V10" "$COMMIT_V10" v10opt

SOURCE_MANIFEST_SHA=$(hash_file "$SOURCE_MANIFEST")
SOURCE_ARCHIVE_BASELINE_SHA=$(hash_file "$SOURCE_ARCHIVE_BASELINE")
SOURCE_ARCHIVE_V9_SHA=$(hash_file "$SOURCE_ARCHIVE_V9")
SOURCE_ARCHIVE_V10_SHA=$(hash_file "$SOURCE_ARCHIVE_V10")

python3 - "$MANIFEST" "$RECORDS" "$REPO_URL" "$COMMON_BASE" \
  "$REPRO_EPOCH" "$(make --version | head -n 1)" "$JOBS" \
  "${CC:-}" "${CFLAGS:-}" "${CPPFLAGS:-}" "${LDFLAGS:-}" \
  "${CONFIGURE_FLAGS[*]}" "$(hostname)" \
  "$SOURCE_MANIFEST_SHA" "$SOURCE_ARCHIVE_BASELINE_SHA" \
  "$SOURCE_ARCHIVE_V9_SHA" "$SOURCE_ARCHIVE_V10_SHA" <<'PY'
import datetime
import json
import sys

(out, records_path, repo_url, common_base, source_date_epoch, make_version,
 jobs, cc_env, cflags, cppflags, ldflags, configure_flags, build_host,
 source_manifest_sha256, source_baseline_sha256,
 source_v9_sha256, source_v10_sha256) = sys.argv[1:]
with open(records_path, encoding="utf-8") as f:
    builds = [json.loads(line) for line in f if line.strip()]

by_name = {item["name"]: item for item in builds}
if set(by_name) != {"baseline-a", "baseline-b", "v9", "v10"}:
    raise SystemExit(f"unexpected build records: {sorted(by_name)}")

for binary in ("postgres", "pgbench", "psql", "initdb", "pg_ctl",
               "test_wait_primitive"):
    left = by_name["baseline-a"]["sha256"][binary]
    right = by_name["baseline-b"]["sha256"][binary]
    if left != right:
        raise SystemExit(
            f"independent baseline builds differ for {binary}: "
            f"{left} != {right}")

fixture_hashes = {
    item["sha256"]["test_wait_primitive"] for item in builds
}
if len(fixture_hashes) != 1:
    raise SystemExit(
        "installed test_wait_primitive binaries differ across builds: "
        + ", ".join(sorted(fixture_hashes)))

manifest = {
    "schema_version": 6,
    "benchmark_series": "wet-v10",
    "treatment": "inline-attachment-needed-guard",
    "created_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "build_host": build_host,
    "repo_url": repo_url,
    "common_base": common_base,
    "source_date_epoch": int(source_date_epoch),
    "build_system": "configure-make",
    "make_version": make_version,
    "configure_flags": configure_flags.split(),
    "optimization": "configure default; assertions disabled",
    "parallel_jobs": int(jobs),
    "environment": {
        "CC": cc_env or None,
        "CFLAGS": cflags or None,
        "CPPFLAGS": cppflags or None,
        "LDFLAGS": ldflags or None,
    },
    "compiler_cache": "disabled",
    "bundled_source": {
        "manifest_sha256": source_manifest_sha256,
        "baseline_archive_sha256": source_baseline_sha256,
        "v9_archive_sha256": source_v9_sha256,
        "v10_archive_sha256": source_v10_sha256,
    },
    "path_control": {
        "compile_source": builds[0]["compile_prefix"].replace(
            "/prefix/active", "/src/active"),
        "compile_build": builds[0]["compile_prefix"].replace(
            "/prefix/active", "/build/active"),
        "compile_prefix": builds[0]["compile_prefix"],
        "runtime_prefix_leaf_bytes": 6,
    },
    "builds": builds,
}
with open(out, "w", encoding="utf-8") as f:
    json.dump(manifest, f, indent=2, sort_keys=True)
    f.write("\n")
PY

log ""
log "Build manifest: $MANIFEST"
log "Runtime prefixes:"
log "  baseline A: $INSTALL_ROOT/base-a"
log "  baseline B: $INSTALL_ROOT/base-b"
log "  v9 reference: $INSTALL_ROOT/v9-ref"
log "  v10 guard:   $INSTALL_ROOT/v10opt"
log ""
log "All four controlled-path builds succeeded."
log "Next step: ./02-run-matrix.sh"
