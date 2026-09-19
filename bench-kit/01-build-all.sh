#!/usr/bin/env bash
# Build two independent vanilla baselines plus the patched (v11 series) and
# control trees.
#
# Unlike v10 (v9 vs v10, both treatments), v11 compares vanilla PostgreSQL
# against one patched series in one matrix, plus a "control" build that
# applies patches-control/*.patch on top of the patched tree (making the
# timed wait-event site bodies identical to the ordinary ones -- see
# brief-v11-wpc-kit.md). Every build uses the same source, build, and
# install path; the completed installation is copied to an equal-length
# runtime prefix before the next build starts, so source/prefix string
# lengths cannot become an accidental code-layout confound.
set -Eeuo pipefail
export LC_ALL=C

die() { echo "01-build-all.sh: ERROR: $*" >&2; exit 1; }
log() { printf '%s\n' "[$(date -u +%H:%M:%S)] $*"; }

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib-python.sh
source "$SCRIPT_DIR/lib-python.sh"
require_python

# Single shared rule (build_manifest_rules.BUILD_WITH_MODULE) for which
# build gets the pg_wait_event_tracing module: every "does this build
# install the module" decision below is derived from it, not a per-line
# yes/no literal that could drift from the rule 02-run-matrix.sh's
# preflight, verify_reusable_builds.py, and analyze-results.py all enforce.
MODULE_BUILD=$("$PYTHON_BIN" -c \
  "from build_manifest_rules import BUILD_WITH_MODULE; print(BUILD_WITH_MODULE)")
has_module() { [[ "$1" == "$MODULE_BUILD" ]] && echo yes || echo no; }

# ---------------------------------------------------------------------------
# SELFTEST_FAKE_PREFIX: fake-binaries self-test mode (selftest-fakebin/,
# driven by selftest-fakebin/run-selftest.sh via self-test.py). Skips real
# compilation entirely and installs the stub pg_ctl/initdb/pgbench/psql/
# postgres/pg_test_timing binaries at every runtime prefix the rest of the
# kit expects, plus a manifest.json whose hashes genuinely match what got
# installed, so 02-run-matrix.sh's binary-vs-manifest check still means
# something. Never used for a real benchmark run.
# ---------------------------------------------------------------------------
if [[ -n "${SELFTEST_FAKE_PREFIX:-}" ]]; then
  [[ -d "$SELFTEST_FAKE_PREFIX" ]] ||
    die "SELFTEST_FAKE_PREFIX does not exist: $SELFTEST_FAKE_PREFIX"
  WORK="$SCRIPT_DIR/work"
  INSTALL_ROOT="$WORK/install"
  MANIFEST="$WORK/manifest.json"
  RECORDS="$WORK/.build-records.jsonl"
  hash_file() { sha256sum "$1" | awk '{print $1}'; }

  install_fake_prefix() {
    local runtime_leaf=$1 build_module=$2
    local prefix="$INSTALL_ROOT/$runtime_leaf"
    rm -rf -- "$prefix"
    mkdir -p "$prefix/bin" "$prefix/lib"
    local bin
    for bin in postgres pg_ctl initdb pgbench psql pg_test_timing; do
      cp "$SELFTEST_FAKE_PREFIX/$bin" "$prefix/bin/$bin"
      chmod +x "$prefix/bin/$bin"
    done
    echo "fake test_wait_primitive shared object (selftest-fakebin)" \
      >"$prefix/lib/test_wait_primitive.so"
    local module_sha=none
    if [[ "$build_module" == yes ]]; then
      echo "fake pg_wait_event_tracing shared object (selftest-fakebin)" \
        >"$prefix/lib/pg_wait_event_tracing.so"
      module_sha=$(hash_file "$prefix/lib/pg_wait_event_tracing.so")
    fi
    "$PYTHON_BIN" - "$RECORDS" "$name_for_leaf" "$runtime_leaf" "$prefix" \
      "$(hash_file "$prefix/bin/postgres")" \
      "$(hash_file "$prefix/bin/pgbench")" \
      "$(hash_file "$prefix/bin/psql")" \
      "$(hash_file "$prefix/bin/initdb")" \
      "$(hash_file "$prefix/bin/pg_ctl")" \
      "$(hash_file "$prefix/lib/test_wait_primitive.so")" \
      "$module_sha" <<'PY'
import json
import sys
(out, name, leaf, prefix, postgres_sha, pgbench_sha, psql_sha, initdb_sha,
 pg_ctl_sha, fixture_sha, module_sha) = sys.argv[1:]
record = {
    "name": name,
    "commit": "selftest-fake",
    "runtime_prefix": prefix,
    "compile_prefix": prefix,
    "compiler": {"c": {"command": "cc", "path": "selftest-fakebin/cc",
                        "sha256": "0" * 64, "version": "selftest-fakebin"}},
    "install_tree": {},
    "fixture_tree": "selftest-fake",
    "provenance_sha256": {"build_log": "0" * 64, "compiler_json": "0" * 64,
                           "install_tree_json": "0" * 64},
    "sha256": {
        "postgres": postgres_sha, "pgbench": pgbench_sha, "psql": psql_sha,
        "initdb": initdb_sha, "pg_ctl": pg_ctl_sha,
        "test_wait_primitive": fixture_sha, "pg_wait_event_tracing": module_sha,
    },
}
with open(out, "a", encoding="utf-8") as f:
    json.dump(record, f, sort_keys=True)
    f.write("\n")
PY
  }

  mkdir -p "$WORK" "$WORK/build-logs"
  echo '{"schema_version": 11, "selftest_fake_prefix": true}' \
    >"$WORK/source-manifest.json"
  : >"$RECORDS"
  for pair in "baseline-a=base-a=$(has_module baseline-a)" \
              "baseline-b=base-b=$(has_module baseline-b)" \
              "patched=patchd=$(has_module patched)" \
              "control=ctrlop=$(has_module control)"; do
    name_for_leaf=${pair%%=*}
    rest=${pair#*=}
    leaf=${rest%%=*}
    module=${rest#*=}
    install_fake_prefix "$leaf" "$module"
  done

  "$PYTHON_BIN" - "$MANIFEST" "$RECORDS" <<'PY'
import datetime
import json
import sys

out, records_path = sys.argv[1:]
with open(records_path, encoding="utf-8") as f:
    builds = [json.loads(line) for line in f if line.strip()]
manifest = {
    "schema_version": 11,
    "benchmark_series": "wet-v11",
    "created_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "build_host": "selftest-fakebin",
    "repo_url": "selftest-fakebin",
    "source_date_epoch": 0,
    "build_system": "selftest-fake (SELFTEST_FAKE_PREFIX, no compilation)",
    "optimization": "n/a",
    "parallel_jobs": 1,
    "environment": {},
    "compiler_cache": "disabled",
    "bundled_source": {},
    "path_control": {"runtime_prefix_leaf_bytes": 6},
    "builds": builds,
}
with open(out, "w", encoding="utf-8") as f:
    json.dump(manifest, f, indent=2, sort_keys=True)
    f.write("\n")
PY

  log "SELFTEST_FAKE_PREFIX mode: installed fake binaries at $INSTALL_ROOT/{base-a,base-b,patchd,ctrlop}"
  log "Build manifest (fake): $MANIFEST"
  exit 0
fi

"$PYTHON_BIN" "$SCRIPT_DIR/sources_conf.py" check "$SCRIPT_DIR/sources.conf" ||
  die "sources.conf is not filled in (see the brief's Launch note)"

MASTER_SHA=$("$PYTHON_BIN" "$SCRIPT_DIR/sources_conf.py" get "$SCRIPT_DIR/sources.conf" MASTER_SHA)
V11_SHA=$("$PYTHON_BIN" "$SCRIPT_DIR/sources_conf.py" get "$SCRIPT_DIR/sources.conf" V11_SHA)
CONTROL_SHA=$("$PYTHON_BIN" "$SCRIPT_DIR/sources_conf.py" get "$SCRIPT_DIR/sources.conf" CONTROL_SHA)
REPO_URL=$("$PYTHON_BIN" "$SCRIPT_DIR/sources_conf.py" get "$SCRIPT_DIR/sources.conf" POSTGRES_REPO_URL)

for tool in sha256sum make cc ar ranlib find hostname tar; do
  command -v "$tool" >/dev/null 2>&1 || die "required tool not found: $tool"
done

HOST_CHECK="$SCRIPT_DIR/host-check.json"
SOURCE_DIR="$SCRIPT_DIR/source"
SOURCE_MANIFEST="$SOURCE_DIR/source-manifest.json"
SOURCE_ARCHIVE_MASTER="$SOURCE_DIR/postgres-master.tar.gz"
SOURCE_ARCHIVE_PATCHED="$SOURCE_DIR/postgres-patched.tar.gz"
SOURCE_ARCHIVE_CONTROL="$SOURCE_DIR/postgres-control.tar.gz"
FIXTURE_DIR="$SOURCE_DIR/test_wait_primitive"
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
"$PYTHON_BIN" - "$HOST_CHECK" "$(hostname)" <<'PY' ||
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
  *ccache*)
    die "CC must not use ccache or sccache; independent builds must compile"
    ;;
esac

mkdir -p "$WORK/src" "$WORK/build" "$WORK/prefix" "$INSTALL_ROOT" "$LOG_DIR"

log "Working directory: $WORK"
log "Using $JOBS parallel build job(s)"

for source_file in \
  "$SOURCE_MANIFEST" "$SOURCE_ARCHIVE_MASTER" \
  "$SOURCE_ARCHIVE_PATCHED" "$SOURCE_ARCHIVE_CONTROL"; do
  [[ -f "$source_file" && ! -L "$source_file" ]] ||
    die "missing regular bundled-source file: $source_file"
done
[[ -d "$FIXTURE_DIR" && ! -L "$FIXTURE_DIR" ]] ||
  die "missing bundled test_wait_primitive fixture snapshot: $FIXTURE_DIR"
log "Using bundled pinned source archives (no network required)"

SOURCE_METADATA_TEXT=$(
  "$PYTHON_BIN" - "$SOURCE_MANIFEST" "$SOURCE_ARCHIVE_MASTER" \
    "$SOURCE_ARCHIVE_PATCHED" "$SOURCE_ARCHIVE_CONTROL" "$FIXTURE_DIR" \
    "$REPO_URL" "$MASTER_SHA" "$V11_SHA" "$CONTROL_SHA" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

(manifest_path, master_path, patched_path, control_path, fixture_dir,
 repo_url, master_commit, v11_commit, control_commit) = sys.argv[1:]
manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
if manifest.get("schema_version") != 11:
    raise SystemExit("unsupported bundled-source manifest schema")
if manifest.get("benchmark_series") != "wet-v11":
    raise SystemExit("unexpected bundled-source benchmark series")
if manifest.get("repo_url") != repo_url:
    raise SystemExit("bundled-source repository URL mismatch")
if manifest.get("commits") != {
    "master": master_commit,
    "patched": v11_commit,
    "control": control_commit,
}:
    raise SystemExit("bundled-source commit mismatch")

def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()

archives = manifest.get("archives")
if not isinstance(archives, dict) or set(archives) != {
    "master", "patched", "control"
}:
    raise SystemExit("bundled-source archive set mismatch")
for name, path_text in (
    ("master", master_path), ("patched", patched_path),
    ("control", control_path),
):
    if archives[name].get("filename") != Path(path_text).name:
        raise SystemExit(f"bundled {name} source filename mismatch")
    if digest(path_text) != archives[name].get("sha256"):
        raise SystemExit(f"bundled {name} source hash mismatch")

fixture = manifest.get("fixture")
if not isinstance(fixture, dict):
    raise SystemExit("bundled-source manifest lacks fixture provenance")
fixture_files = sorted(
    p for p in Path(fixture_dir).rglob("*") if p.is_file()
)
expected_files = fixture.get("files_sha256", {})
actual = {
    p.relative_to(Path(fixture_dir).parent).as_posix(): digest(p)
    for p in fixture_files
}
if actual != expected_files:
    raise SystemExit("bundled test_wait_primitive fixture hash mismatch")
print(manifest["source_date_epoch"])
print(fixture["tree"])
PY
) || die "bundled source archives failed verification"
mapfile -t SOURCE_METADATA <<<"$SOURCE_METADATA_TEXT"
[[ ${#SOURCE_METADATA[@]} -eq 2 ]] ||
  die "bundled-source manifest returned incomplete metadata"
REPRO_EPOCH=${SOURCE_METADATA[0]}
FIXTURE_TREE=${SOURCE_METADATA[1]}
cp "$SOURCE_MANIFEST" "$WORK/source-manifest.json"

: >"$RECORDS"

build_one() {
  local name=$1 commit=$2 runtime_leaf=$3 build_module=$4
  local runtime_prefix="$INSTALL_ROOT/$runtime_leaf"
  local log="$LOG_DIR/$name.log"
  local compiler_json="$LOG_DIR/$name-compilers.json"
  local install_tree_json="$LOG_DIR/$name-install-tree.json"

  log "== Building '$name' (commit $commit) =="
  : >"$log"

  rm -rf -- "$ACTIVE_SOURCE" "$ACTIVE_BUILD" "$ACTIVE_PREFIX" "$runtime_prefix"
  local archive=$SOURCE_ARCHIVE_MASTER
  case "$name" in
    patched) archive=$SOURCE_ARCHIVE_PATCHED ;;
    control) archive=$SOURCE_ARCHIVE_CONTROL ;;
  esac
  mkdir -p "$ACTIVE_SOURCE"
  tar -xzf "$archive" -C "$ACTIVE_SOURCE" >>"$log" 2>&1 ||
    die "extracting bundled source failed for $name; see $log"

  # The fixture is not part of real PostgreSQL history; it is overlaid from
  # the bundled snapshot (fixture-src/, packaged and hash-verified by
  # make-baremetal-package.sh; never read from git) onto every tree so all
  # four builds get byte-identical fixture code.
  rm -rf -- "${ACTIVE_SOURCE:?}/${FIXTURE_REL:?}"
  mkdir -p "$(dirname "$ACTIVE_SOURCE/$FIXTURE_REL")"
  cp -a "$FIXTURE_DIR" "$ACTIVE_SOURCE/$FIXTURE_REL"

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

  "$PYTHON_BIN" - "$compiler_json" "$(command -v cc)" <<'PY' ||
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

  if [[ "$build_module" == yes ]]; then
    local tracing_extension="$ACTIVE_SOURCE/contrib/pg_wait_event_tracing"
    [[ -d "$tracing_extension" ]] ||
      die "$name is missing contrib/pg_wait_event_tracing; is it really the patched/control commit?"
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

  find "$ACTIVE_PREFIX" -type f -name '*.a' -delete

  [[ -x "$ACTIVE_PREFIX/bin/postgres" ]] ||
    die "$name did not produce an installed postgres executable"

  cp -a "$ACTIVE_PREFIX" "$runtime_prefix"
  LD_LIBRARY_PATH="$runtime_prefix/lib:${LD_LIBRARY_PATH:-}" \
    "$runtime_prefix/bin/postgres" --version >>"$log" 2>&1 ||
    die "relocated $name installation is not executable"

  "$PYTHON_BIN" - "$runtime_prefix" "$install_tree_json" <<'PY' ||
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

  if [[ "$build_module" == yes ]]; then
    [[ "$module_sha" != none ]] ||
      die "$name commit does not install pg_wait_event_tracing"
  else
    [[ "$module_sha" == none ]] ||
      die "$name unexpectedly installs pg_wait_event_tracing"
  fi

  "$PYTHON_BIN" - "$RECORDS" "$name" "$commit" "$runtime_prefix" \
    "$ACTIVE_PREFIX" "$compiler_json" "$install_tree_json" "$FIXTURE_TREE" \
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

(out, name, commit, runtime_prefix, compile_prefix, compiler_path,
 install_tree_path, fixture_tree, postgres_sha, pgbench_sha, psql_sha,
 initdb_sha, pg_ctl_sha, fixture_sha, module_sha, build_log_sha,
 compiler_json_sha, install_tree_json_sha) = sys.argv[1:]
with open(compiler_path, encoding="utf-8") as f:
    compilers = json.load(f)
with open(install_tree_path, encoding="utf-8") as f:
    install_tree = json.load(f)
record = {
    "name": name,
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
  "$PYTHON_BIN" - "$RECORDS" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    records = {
        item["name"]: item
        for item in (json.loads(line) for line in stream if line.strip())
    }
if set(records) != {"baseline-a", "baseline-b"}:
    raise SystemExit(f"unexpected early baseline records: {sorted(records)}")
left, right = records["baseline-a"], records["baseline-b"]
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
# affecting runtime layout.
for leaf in base-a base-b patchd ctrlop; do
  [[ ${#leaf} -eq 6 ]] || die "runtime prefix leaf '$leaf' is not six bytes"
done

build_one baseline-a "$MASTER_SHA" base-a "$(has_module baseline-a)"
build_one baseline-b "$MASTER_SHA" base-b "$(has_module baseline-b)"
verify_baseline_reproducibility ||
  die "independent baseline builds differ; patched/control builds were not started"
log "Independent baseline reproducibility: PASS"
build_one patched "$V11_SHA" patchd "$(has_module patched)"
build_one control "$CONTROL_SHA" ctrlop "$(has_module control)"

SOURCE_MANIFEST_SHA=$(hash_file "$SOURCE_MANIFEST")
SOURCE_ARCHIVE_MASTER_SHA=$(hash_file "$SOURCE_ARCHIVE_MASTER")
SOURCE_ARCHIVE_PATCHED_SHA=$(hash_file "$SOURCE_ARCHIVE_PATCHED")
SOURCE_ARCHIVE_CONTROL_SHA=$(hash_file "$SOURCE_ARCHIVE_CONTROL")

"$PYTHON_BIN" - "$MANIFEST" "$RECORDS" "$REPO_URL" "$REPRO_EPOCH" \
  "$(make --version | head -n 1)" "$JOBS" \
  "${CC:-}" "${CFLAGS:-}" "${CPPFLAGS:-}" "${LDFLAGS:-}" \
  "${CONFIGURE_FLAGS[*]}" "$(hostname)" \
  "$SOURCE_MANIFEST_SHA" "$SOURCE_ARCHIVE_MASTER_SHA" \
  "$SOURCE_ARCHIVE_PATCHED_SHA" "$SOURCE_ARCHIVE_CONTROL_SHA" <<'PY'
import datetime
import json
import sys

(out, records_path, repo_url, source_date_epoch, make_version,
 jobs, cc_env, cflags, cppflags, ldflags, configure_flags, build_host,
 source_manifest_sha256, source_master_sha256,
 source_patched_sha256, source_control_sha256) = sys.argv[1:]
with open(records_path, encoding="utf-8") as f:
    builds = [json.loads(line) for line in f if line.strip()]

by_name = {item["name"]: item for item in builds}
if set(by_name) != {"baseline-a", "baseline-b", "patched", "control"}:
    raise SystemExit(f"unexpected build records: {sorted(by_name)}")

for binary in ("postgres", "pgbench", "psql", "initdb", "pg_ctl",
               "test_wait_primitive"):
    left = by_name["baseline-a"]["sha256"][binary]
    right = by_name["baseline-b"]["sha256"][binary]
    if left != right:
        raise SystemExit(
            f"independent baseline builds differ for {binary}: "
            f"{left} != {right}")

manifest = {
    "schema_version": 11,
    "benchmark_series": "wet-v11",
    "created_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "build_host": build_host,
    "repo_url": repo_url,
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
        "master_archive_sha256": source_master_sha256,
        "patched_archive_sha256": source_patched_sha256,
        "control_archive_sha256": source_control_sha256,
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
log "  patched:    $INSTALL_ROOT/patchd"
log "  control:    $INSTALL_ROOT/ctrlop"
log ""
log "All four controlled-path builds succeeded."
log "Next step: ./02-run-matrix.sh"
