#!/usr/bin/env bash
# Package one complete raw-evidence archive for local verification and analysis.
set -Eeuo pipefail
export LC_ALL=C

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WORK="$SCRIPT_DIR/work"
RESULTS="$SCRIPT_DIR/results"

die() { echo "03-collect.sh: ERROR: $*" >&2; exit 1; }
log() { printf '%s\n' "$*"; }

for tool in tar sha256sum python3 cp find; do
  command -v "$tool" >/dev/null 2>&1 || die "required tool not found: $tool"
done

required_results=(
  results.csv
  schedule.csv
  seed.txt
  protocol.json
  telemetry.jsonl
  progress.json
  aa-early.jsonl
  matrix-complete.json
)
for name in "${required_results[@]}"; do
  [[ -f "$RESULTS/$name" && ! -L "$RESULTS/$name" ]] ||
    die "missing required result file: $RESULTS/$name"
done
for directory in logs recording-proofs w3-qualification client-load; do
  [[ -d "$RESULTS/$directory" && ! -L "$RESULTS/$directory" ]] ||
    die "missing required evidence directory: $RESULTS/$directory"
  [[ -z "$(find "$RESULTS/$directory" \
    ! -type f ! -type d -print -quit)" ]] ||
    die "special or symlinked evidence is not allowed: $RESULTS/$directory"
done
[[ -f "$WORK/manifest.json" && ! -L "$WORK/manifest.json" ]] ||
  die "missing build manifest: $WORK/manifest.json"
[[ -f "$WORK/source-manifest.json" &&
   ! -L "$WORK/source-manifest.json" ]] ||
  die "missing source manifest: $WORK/source-manifest.json"
[[ -d "$WORK/build-logs" && ! -L "$WORK/build-logs" ]] ||
  die "missing build logs: $WORK/build-logs"
[[ -z "$(find "$WORK/build-logs" ! -type f ! -type d -print -quit)" ]] ||
  die "special or symlinked build logs are not allowed"
[[ -f "$SCRIPT_DIR/host-check.txt" && ! -L "$SCRIPT_DIR/host-check.txt" ]] ||
  die "host-check.txt is missing; run ./00-check-host.sh before collecting"
[[ -f "$SCRIPT_DIR/host-check.json" && ! -L "$SCRIPT_DIR/host-check.json" ]] ||
  die "host-check.json is missing; run ./00-check-host.sh before collecting"

HOSTNAME_TAG=$(hostname -s 2>/dev/null || hostname)
DATE_TAG=$(date -u +%Y%m%dT%H%M%SZ)
ARCHIVE="$SCRIPT_DIR/results-$HOSTNAME_TAG-$DATE_TAG.tar.gz"
[[ ! -e "$ARCHIVE" && ! -e "$ARCHIVE.sha256" ]] ||
  die "archive or sidecar already exists for today: $ARCHIVE"

STAGE=$(mktemp -d "$SCRIPT_DIR/.collect-XXXXXX")
EXTRACT=$(mktemp -d "$SCRIPT_DIR/.verify-XXXXXX")
SUCCESS=0
cleanup() {
  rm -rf -- "$STAGE" "$EXTRACT"
  if [[ "$SUCCESS" -ne 1 ]]; then
    rm -f -- "$ARCHIVE" "$ARCHIVE.sha256"
  fi
}
trap cleanup EXIT

mkdir -p "$STAGE/results" "$STAGE/build" \
  "$STAGE/kit/workloads" "$STAGE/kit/patches"

log "Staging raw results"
for name in "${required_results[@]}"; do
  cp "$RESULTS/$name" "$STAGE/results/"
done
for directory in logs recording-proofs w3-qualification client-load; do
  cp -a "$RESULTS/$directory" "$STAGE/results/"
done
cp "$WORK/manifest.json" "$WORK/source-manifest.json" "$STAGE/build/"
cp -a "$WORK/build-logs" "$STAGE/build/"
cp "$SCRIPT_DIR/host-check.txt" "$SCRIPT_DIR/host-check.json" "$STAGE/"

log "Staging exact harness and workloads"
for name in \
  00-check-host.sh \
  01-build-all.sh \
  02-run-matrix.sh \
  03-collect.sh \
  analyze-raw-archive.sh \
  wait-for-idle.sh \
  analyze-results.py \
  benchmark_protocol.py \
  w3_qualification.py \
  self-test.py \
  run-benchmark.sh \
  README.md; do
  [[ -f "$SCRIPT_DIR/$name" && ! -L "$SCRIPT_DIR/$name" ]] ||
    die "missing regular harness file: $name"
  cp "$SCRIPT_DIR/$name" "$STAGE/kit/"
done
for name in w3-short-lwlock.sql recording-proof.sql w3-qualification.sql; do
  [[ -f "$SCRIPT_DIR/workloads/$name" &&
     ! -L "$SCRIPT_DIR/workloads/$name" ]] ||
    die "missing workload file: $name"
  cp "$SCRIPT_DIR/workloads/$name" "$STAGE/kit/workloads/"
done
for name in 0006-optimize-null-wait-event-hook-path.patch; do
  [[ -f "$SCRIPT_DIR/patches/$name" &&
     ! -L "$SCRIPT_DIR/patches/$name" ]] ||
    die "missing optimization patch: $name"
  cp "$SCRIPT_DIR/patches/$name" "$STAGE/kit/patches/"
done
if [[ -f "$SCRIPT_DIR/BAREMETAL-RUNBOOK-v9.md" ]]; then
  cp "$SCRIPT_DIR/BAREMETAL-RUNBOOK-v9.md" "$STAGE/kit/"
elif [[ -f "$SCRIPT_DIR/../BAREMETAL-RUNBOOK-v9.md" ]]; then
  cp "$SCRIPT_DIR/../BAREMETAL-RUNBOOK-v9.md" "$STAGE/kit/"
fi

python3 - "$STAGE" "$STAGE/MANIFEST.sha256" <<'PY'
import hashlib
import sys
from pathlib import Path

root, out = Path(sys.argv[1]), Path(sys.argv[2])
paths = sorted(
    path for path in root.rglob("*")
    if path.is_file() and path != out
)
with out.open("x", encoding="ascii") as stream:
    for path in paths:
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        stream.write(f"{digest}  {path.relative_to(root).as_posix()}\n")
PY

log "Creating archive"
tar -czf "$ARCHIVE" -C "$STAGE" \
  MANIFEST.sha256 host-check.txt host-check.json results build kit
(
  cd "$SCRIPT_DIR"
  sha256sum "$(basename "$ARCHIVE")" >"$(basename "$ARCHIVE").sha256"
)

log "Clean-room verifying extracted raw archive"
python3 - "$ARCHIVE" <<'PY'
import subprocess
import sys
from pathlib import PurePosixPath

archive = sys.argv[1]
entries = subprocess.run(
    ["tar", "-tzf", archive],
    check=True,
    stdout=subprocess.PIPE,
    text=True,
).stdout.splitlines()
for entry in entries:
    path = PurePosixPath(entry)
    if path.is_absolute() or ".." in path.parts:
        raise SystemExit(f"unsafe archive member: {entry}")
PY
tar -xzf "$ARCHIVE" -C "$EXTRACT"
(
  cd "$EXTRACT"
  sha256sum -c MANIFEST.sha256 >/dev/null
)
SIZE_HUMAN=$(du -h "$ARCHIVE" | cut -f1)
ARCHIVE_SHA=$(awk '{print $1}' "$ARCHIVE.sha256")
SUCCESS=1
log ""
log "Archive: $ARCHIVE"
log "Size: $SIZE_HUMAN"
log "SHA-256: $ARCHIVE_SHA"
log "Raw archive integrity verification: PASS"
log ""
log "The archive records the host name, OS user, and working paths."
log "Run ./analyze-raw-archive.sh on the local machine after copying it back."
