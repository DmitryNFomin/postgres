#!/usr/bin/env bash
# Create the self-contained package that the executor copies to Linux.
set -Eeuo pipefail
export LC_ALL=C

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)
DIST_DIR=${1:-"$REPO_DIR/dist"}
PACKAGE_NAME=wet-v9-baremetal-r1
STAGE=$(mktemp -d)
ARCHIVE="$DIST_DIR/$PACKAGE_NAME.tar.gz"
SIDECAR="$ARCHIVE.sha256"
VERIFY=""
SUCCESS=0
OUTPUTS_OWNED=0

cleanup() {
  rm -rf -- "$STAGE"
  [[ -z "$VERIFY" ]] || rm -rf -- "$VERIFY"
  if [[ "$OUTPUTS_OWNED" -eq 1 && "$SUCCESS" -ne 1 ]]; then
    rm -f -- "$ARCHIVE" "$SIDECAR"
  fi
}
trap cleanup EXIT

for tool in cmp git tar sha256sum python3; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "missing required packaging tool: $tool" >&2
    exit 1
  }
done

[[ ! -e "$ARCHIVE" && ! -e "$SIDECAR" ]] || {
  echo "refusing to overwrite existing package: $ARCHIVE" >&2
  exit 1
}
OUTPUTS_OWNED=1
mkdir -p "$DIST_DIR" "$STAGE/$PACKAGE_NAME/source" \
  "$STAGE/$PACKAGE_NAME/workloads" "$STAGE/$PACKAGE_NAME/patches"

files=(
  00-check-host.sh
  01-build-all.sh
  02-run-matrix.sh
  03-collect.sh
  analyze-raw-archive.sh
  wait-for-idle.sh
  analyze-results.py
  benchmark_protocol.py
  w3_qualification.py
  self-test.py
  run-benchmark.sh
  README.md
)
workloads=(
  w3-short-lwlock.sql
  recording-proof.sql
  w3-qualification.sql
)
for name in "${files[@]}"; do
  [[ -f "$SCRIPT_DIR/$name" && ! -L "$SCRIPT_DIR/$name" ]] || {
    echo "missing regular package input: $name" >&2
    exit 1
  }
  cp "$SCRIPT_DIR/$name" "$STAGE/$PACKAGE_NAME/"
done
for name in "${workloads[@]}"; do
  [[ -f "$SCRIPT_DIR/workloads/$name" &&
     ! -L "$SCRIPT_DIR/workloads/$name" ]] || {
    echo "missing regular workload input: $name" >&2
    exit 1
  }
  cp "$SCRIPT_DIR/workloads/$name" "$STAGE/$PACKAGE_NAME/workloads/"
done
cp "$REPO_DIR/BAREMETAL-RUNBOOK-v9.md" "$STAGE/$PACKAGE_NAME/"

BASELINE_COMMIT=765efece39ba3fb04fdf20b1dadcd9ecea76fbc9
PATCHED_COMMIT=40bffed8a92291c27a5d1956a5cd18dd3609f397
PATCHED_PARENT=d7b4584a901241258604eef1f03dfd6b3f1fa926
COMMON_BASE=0c5d6269614e107d1d2d669f82f63f7e232b30c9
FIXTURE_TREE=93dde50fc966a3ab01f4218010ec65548370d6d6
SOURCE_EPOCH=1789301160
PATCH_ARTIFACT="$SCRIPT_DIR/patches/0006-optimize-null-wait-event-hook-path.patch"
REVIEW_PATCH="$REPO_DIR/patches-v9/0006-optimize-null-wait-event-hook-path.patch"
BASELINE_ARCHIVE="$STAGE/$PACKAGE_NAME/source/postgres-baseline.tar.gz"
PATCHED_ARCHIVE="$STAGE/$PACKAGE_NAME/source/postgres-patched.tar.gz"
SOURCE_MANIFEST="$STAGE/$PACKAGE_NAME/source/source-manifest.json"

[[ -f "$PATCH_ARTIFACT" && ! -L "$PATCH_ARTIFACT" ]]
[[ -f "$REVIEW_PATCH" && ! -L "$REVIEW_PATCH" ]]
[[ "$(git -C "$REPO_DIR" rev-parse "$PATCHED_COMMIT^")" == "$PATCHED_PARENT" ]]
cmp "$PATCH_ARTIFACT" "$REVIEW_PATCH"
cmp "$PATCH_ARTIFACT" <(
  git -C "$REPO_DIR" diff --binary "$PATCHED_PARENT" "$PATCHED_COMMIT"
)
cp "$PATCH_ARTIFACT" "$STAGE/$PACKAGE_NAME/patches/"
[[ "$(git -C "$REPO_DIR" merge-base "$BASELINE_COMMIT" "$PATCHED_COMMIT")" \
   == "$COMMON_BASE" ]]
[[ "$(git -C "$REPO_DIR" show -s --format=%ct "$COMMON_BASE")" \
   == "$SOURCE_EPOCH" ]]
[[ "$(git -C "$REPO_DIR" rev-parse \
  "$BASELINE_COMMIT:src/test/modules/test_wait_primitive")" \
   == "$FIXTURE_TREE" ]]
[[ "$(git -C "$REPO_DIR" rev-parse \
  "$PATCHED_COMMIT:src/test/modules/test_wait_primitive")" \
   == "$FIXTURE_TREE" ]]
git -C "$REPO_DIR" archive --format=tar.gz \
  -o "$BASELINE_ARCHIVE" "$BASELINE_COMMIT"
git -C "$REPO_DIR" archive --format=tar.gz \
  -o "$PATCHED_ARCHIVE" "$PATCHED_COMMIT"
python3 - "$SOURCE_MANIFEST" "$BASELINE_ARCHIVE" "$PATCHED_ARCHIVE" \
  "$BASELINE_COMMIT" "$PATCHED_COMMIT" "$COMMON_BASE" "$FIXTURE_TREE" \
  "$SOURCE_EPOCH" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

(out, baseline_path, patched_path, baseline_commit, patched_commit,
 common_base, fixture_tree, source_epoch) = sys.argv[1:]

def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

data = {
    "schema_version": 2,
    "benchmark_series": "wet-v9",
    "repo_url": "https://github.com/DmitryNFomin/postgres.git",
    "commits": {
        "baseline": baseline_commit,
        "patched": patched_commit,
    },
    "common_base": common_base,
    "source_date_epoch": int(source_epoch),
    "fixture_tree": fixture_tree,
    "treatment": {
        "name": "null-hook-fast-path",
        "parent_commit": "d7b4584a901241258604eef1f03dfd6b3f1fa926",
        "branch_prediction_hint": "none",
    },
    "archives": {
        "baseline": {
            "filename": Path(baseline_path).name,
            "sha256": digest(baseline_path),
        },
        "patched": {
            "filename": Path(patched_path).name,
            "sha256": digest(patched_path),
        },
    },
}
Path(out).write_text(
    json.dumps(data, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY

chmod 755 \
  "$STAGE/$PACKAGE_NAME/00-check-host.sh" \
  "$STAGE/$PACKAGE_NAME/01-build-all.sh" \
  "$STAGE/$PACKAGE_NAME/02-run-matrix.sh" \
  "$STAGE/$PACKAGE_NAME/03-collect.sh" \
  "$STAGE/$PACKAGE_NAME/analyze-raw-archive.sh" \
  "$STAGE/$PACKAGE_NAME/wait-for-idle.sh" \
  "$STAGE/$PACKAGE_NAME/analyze-results.py" \
  "$STAGE/$PACKAGE_NAME/w3_qualification.py" \
  "$STAGE/$PACKAGE_NAME/self-test.py" \
  "$STAGE/$PACKAGE_NAME/run-benchmark.sh"

(
  cd "$STAGE/$PACKAGE_NAME"
  find . -type f ! -name PACKAGE-MANIFEST.sha256 -print0 \
    | sort -z \
    | xargs -0 sha256sum >PACKAGE-MANIFEST.sha256
)

tar -czf "$ARCHIVE" -C "$STAGE" "$PACKAGE_NAME"
(
  cd "$DIST_DIR"
  sha256sum "$(basename "$ARCHIVE")" >"$(basename "$SIDECAR")"
)

VERIFY=$(mktemp -d)
python3 - "$ARCHIVE" <<'PY'
import subprocess
import sys
from pathlib import PurePosixPath

entries = subprocess.run(
    ["tar", "-tzf", sys.argv[1]],
    check=True,
    stdout=subprocess.PIPE,
    text=True,
).stdout.splitlines()
for entry in entries:
    path = PurePosixPath(entry)
    if path.is_absolute() or ".." in path.parts:
        raise SystemExit(f"unsafe package member: {entry}")
PY
tar -xzf "$ARCHIVE" -C "$VERIFY"
(
  cd "$VERIFY/$PACKAGE_NAME"
  sha256sum -c PACKAGE-MANIFEST.sha256 >/dev/null
  mkdir source-check-baseline source-check-patched
  tar -xzf source/postgres-baseline.tar.gz -C source-check-baseline
  tar -xzf source/postgres-patched.tar.gz -C source-check-patched
  diff -qr \
    source-check-baseline/src/test/modules/test_wait_primitive \
    source-check-patched/src/test/modules/test_wait_primitive >/dev/null
  test -x source-check-baseline/configure
  test -f source-check-patched/contrib/pg_wait_event_tracing/Makefile
  PYTHONDONTWRITEBYTECODE=1 ./self-test.py
)
(
  cd "$DIST_DIR"
  sha256sum -c "$(basename "$SIDECAR")"
)

SUCCESS=1
printf 'Package: %s\n' "$ARCHIVE"
printf 'Checksum: %s\n' "$SIDECAR"
printf 'Start command after extraction: ./%s/run-benchmark.sh\n' "$PACKAGE_NAME"
