#!/usr/bin/env bash
# Verify, run, analyze, and package the persistent-backend crossover.
set -Eeuo pipefail
export LC_ALL=C
unset \
  BASH_ENV CDPATH ENV LD_AUDIT LD_LIBRARY_PATH LD_PRELOAD \
  PGAPPNAME PGCLIENTENCODING PGCONNECT_TIMEOUT PGDATABASE PGHOST PGHOSTADDR \
  PGOPTIONS PGPASSFILE PGPASSWORD PGPORT PGSERVICE PGSERVICEFILE PGSSLMODE \
  PGUSER PYTHONHOME PYTHONPATH PYTHONSTARTUP
umask 077

SOURCE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
RUN_MODE=full
if [[ ${1:-} == --smoke ]]; then
  RUN_MODE=smoke
  shift
fi
KIT_DIR=${1:?usage: ./run.sh /path/to/wet-v11-baremetal-r1}
[[ $# -eq 1 ]] || {
  echo "usage: ./run.sh [--smoke] /path/to/wet-v11-baremetal-r1" >&2
  exit 1
}
KIT_DIR=$(cd "$KIT_DIR" && pwd -P)
RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
OUTPUT_NAME="w6c-persistent-crossover-$RUN_ID"
STAGE_PARENT=""
STAGE=""
WORKER_DIR=""
TOOLS_DIR=""
PROVENANCE_DIR=""
SOCKET_DIR=""
DRIVER_LOG=""
ARCHIVE=""
TEMP_ARCHIVE=""
TEMP_SIDECAR=""
MARKER_TEMP=""
WORKER_PID=""
SUCCESS=0
ARCHIVE_PUBLISHED=0
SIDECAR_PUBLISHED=0

log() {
  local message="[$(date -u +%H:%M:%S)] $*"
  printf '%s\n' "$message"
  [[ -z "$DRIVER_LOG" || ! -f "$DRIVER_LOG" ]] ||
    printf '%s\n' "$message" >>"$DRIVER_LOG"
}
die() {
  echo "run.sh: ERROR: $*" >&2
  exit 1
}
find_postgres_pids() {
  {
    pgrep -x postgres 2>/dev/null || true
    pgrep -x postmaster 2>/dev/null || true
  } | sort -nu
}
cleanup() {
  if [[ -n "$WORKER_PID" ]] && kill -0 "$WORKER_PID" 2>/dev/null; then
    kill "$WORKER_PID" 2>/dev/null || true
    wait "$WORKER_PID" 2>/dev/null || true
  fi
  WORKER_PID=""
  [[ -z "$TEMP_ARCHIVE" ]] || rm -f -- "$TEMP_ARCHIVE"
  [[ -z "$TEMP_SIDECAR" ]] || rm -f -- "$TEMP_SIDECAR"
  [[ -z "$MARKER_TEMP" ]] || rm -f -- "$MARKER_TEMP"
  if [[ "$SUCCESS" -ne 1 ]]; then
    [[ "$ARCHIVE_PUBLISHED" -eq 0 ]] || rm -f -- "$ARCHIVE"
    [[ "$SIDECAR_PUBLISHED" -eq 0 ]] ||
      rm -f -- "$ARCHIVE.sha256"
  fi
  if [[ "$SUCCESS" -ne 1 ]]; then
    if [[ -n "$STAGE" && -d "$STAGE" ]]; then
      log "Crossover stopped. Partial evidence remains at $STAGE"
    else
      echo "Crossover stopped before staging." >&2
    fi
  fi
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

[[ "$(id -u)" -ne 0 ]] || die "run the crossover as the original non-root executor"
for tool in \
  sha256sum tar pgrep awk grep stat sort find xargs cp chmod rm \
  readlink mktemp mv mkdir dirname date hostname; do
  command -v "$tool" >/dev/null 2>&1 || die "missing tool: $tool"
done
[[ -f "$SOURCE_DIR/CROSSOVER-MANIFEST.sha256" &&
   ! -L "$SOURCE_DIR/CROSSOVER-MANIFEST.sha256" ]] ||
  die "CROSSOVER-MANIFEST.sha256 is missing"
(
  cd "$SOURCE_DIR"
  sha256sum -c CROSSOVER-MANIFEST.sha256
) || die "crossover package integrity check failed"
[[ -f "$KIT_DIR/PACKAGE-MANIFEST.sha256" &&
   ! -L "$KIT_DIR/PACKAGE-MANIFEST.sha256" ]] ||
  die "kit package manifest is missing"
(
  cd "$KIT_DIR"
  sha256sum -c PACKAGE-MANIFEST.sha256 >/dev/null
) || die "kit package integrity check failed"
[[ -f "$KIT_DIR/work/manifest.json" && ! -L "$KIT_DIR/work/manifest.json" ]] ||
  die "kit build manifest is missing"
[[ "$(stat -c '%U' "$KIT_DIR/work/manifest.json")" == "$(id -un)" ]] ||
  die "kit builds belong to a different executor"
[[ -z "$(find_postgres_pids)" ]] ||
  die "a PostgreSQL server is already running"

PYTHON_BIN=""
for candidate in \
  "${PYTHON_BIN_OVERRIDE:-}" \
  python3 python3.14 python3.13 python3.12 python3.11 python3.10 python3.9; do
  [[ -n "$candidate" ]] || continue
  candidate=$(command -v "$candidate" 2>/dev/null) || continue
  candidate=$(readlink -f "$candidate") || continue
  if "$candidate" -I -c \
      'import sys; raise SystemExit(sys.version_info < (3, 9))' \
      >/dev/null 2>&1; then
    PYTHON_BIN=$candidate
    break
  fi
done
[[ -n "$PYTHON_BIN" ]] ||
  die "Python 3.9 or newer was not found (set PYTHON_BIN_OVERRIDE if needed)"
"$PYTHON_BIN" -I "$SOURCE_DIR/self-test.py" ||
  die "crossover self-test failed"

CROSSOVER_MANIFEST_SHA256=$(sha256sum \
  "$SOURCE_DIR/CROSSOVER-MANIFEST.sha256" | awk '{print $1}')
KIT_MANIFEST_SHA256=$(sha256sum \
  "$KIT_DIR/PACKAGE-MANIFEST.sha256" | awk '{print $1}')
SMOKE_MARKER="$SOURCE_DIR/.SMOKE-PASS"
SMOKE_ID="schema=2 crossover=$CROSSOVER_MANIFEST_SHA256 kit=$KIT_MANIFEST_SHA256 executor=$(id -un) hostname=$(hostname)"
if [[ "$RUN_MODE" == full ]]; then
  [[ -f "$SMOKE_MARKER" && ! -L "$SMOKE_MARKER" &&
     "$(<"$SMOKE_MARKER")" == "$SMOKE_ID" ]] ||
    die "run './run.sh --smoke $KIT_DIR' successfully before the full benchmark"
fi

STAGE_PARENT=$(mktemp -d /var/tmp/w6c-persistent-crossover.XXXXXX)
STAGE="$STAGE_PARENT/evidence"
ARCHIVE="$STAGE_PARENT/$OUTPUT_NAME.tar.gz"
WORKER_DIR="$STAGE/worker"
TOOLS_DIR="$STAGE/tools"
PROVENANCE_DIR="$STAGE/provenance"
SOCKET_DIR="$STAGE_PARENT/sock"
DRIVER_LOG="$STAGE/driver.log"
mkdir "$STAGE" "$WORKER_DIR" "$TOOLS_DIR" "$PROVENANCE_DIR"
: >"$DRIVER_LOG"

copied_files=0
while read -r digest relative; do
  [[ "$digest" =~ ^[0-9a-f]{64}$ && "$relative" == ./* ]] ||
    die "malformed crossover manifest entry"
  name=${relative#./}
  [[ "$name" =~ ^[A-Za-z0-9._-]+$ ]] ||
    die "unsafe crossover manifest path: $relative"
  [[ -f "$SOURCE_DIR/$name" && ! -L "$SOURCE_DIR/$name" ]] ||
    die "crossover input is not a regular file: $name"
  cp "$SOURCE_DIR/$name" "$TOOLS_DIR/"
  copied_files=$((copied_files + 1))
done <"$SOURCE_DIR/CROSSOVER-MANIFEST.sha256"
[[ "$copied_files" -gt 0 ]] || die "crossover manifest is empty"
cp "$SOURCE_DIR/CROSSOVER-MANIFEST.sha256" "$TOOLS_DIR/"
(
  cd "$TOOLS_DIR"
  sha256sum -c CROSSOVER-MANIFEST.sha256
) || die "crossover tool snapshot verification failed"
chmod 755 "$TOOLS_DIR"/*.py "$TOOLS_DIR"/*.sh
chmod 644 "$TOOLS_DIR"/*.md "$TOOLS_DIR"/*.sha256

for relative in \
  PACKAGE-MANIFEST.sha256 host-check.json host-check.txt work/manifest.json; do
  cp "$KIT_DIR/$relative" "$PROVENANCE_DIR/$(basename "$relative")"
done
cp "$TOOLS_DIR/CROSSOVER-MANIFEST.sha256" "$PROVENANCE_DIR/"
if [[ "$RUN_MODE" == full ]]; then
  cp "$SMOKE_MARKER" "$PROVENANCE_DIR/smoke-pass.txt"
fi
printf '%s\n' \
  "executor=$(id -un)" \
  "python=$PYTHON_BIN" \
  "hostname=$(hostname)" \
  >"$PROVENANCE_DIR/runtime.txt"
"$PYTHON_BIN" -I "$TOOLS_DIR/verify_runtime.py" \
  "$SOCKET_DIR" 55473 ||
  die "generated PostgreSQL socket path exceeds the platform limit"

log "Persistent-backend W6c crossover ($RUN_MODE)"
log "Executor: $(id -un)"
log "Kit: $KIT_DIR"
log "Staging: $STAGE"
if [[ "$RUN_MODE" == full ]]; then
  log "Waiting for the verified idle gate."
  "$KIT_DIR/wait-for-idle.sh" >>"$DRIVER_LOG" 2>&1
  [[ -z "$(find_postgres_pids)" ]] ||
    die "a PostgreSQL server started during the idle gate"
else
  log "Smoke mode skips the 60-second idle gate."
fi
"$PYTHON_BIN" -I "$TOOLS_DIR/host_state.py" \
  "$PROVENANCE_DIR/host-before.json" ||
  die "current host state differs from the crossover protocol"

if [[ "$RUN_MODE" == full ]]; then
  log "Beginning 16 independent persistent-backend sessions (patched installation only)."
else
  log "Beginning two short startup and capture-transition smoke sessions."
fi
PYTHONDONTWRITEBYTECODE=1 PYTHON_BIN="$PYTHON_BIN" \
  bash "$TOOLS_DIR/run-worker.sh" \
  "$KIT_DIR" "$WORKER_DIR" "$SOCKET_DIR" "$RUN_MODE" \
  >>"$DRIVER_LOG" 2>&1 &
WORKER_PID=$!
if ! wait "$WORKER_PID"; then
  WORKER_PID=""
  die "crossover worker failed; see $DRIVER_LOG"
fi
WORKER_PID=""
"$PYTHON_BIN" -I "$TOOLS_DIR/host_state.py" \
  "$PROVENANCE_DIR/host-after.json" ||
  die "host state changed during the crossover"
rm -rf -- "$WORKER_DIR/data" "$SOCKET_DIR"

if [[ "$RUN_MODE" == smoke ]]; then
  MARKER_TEMP=$(mktemp "$SOURCE_DIR/.SMOKE-PASS.XXXXXX")
  printf '%s\n' "$SMOKE_ID" >"$MARKER_TEMP"
  chmod 600 "$MARKER_TEMP"
  mv -f "$MARKER_TEMP" "$SMOKE_MARKER"
  MARKER_TEMP=""
  rm -rf -- "$STAGE_PARENT"
  STAGE=""
  STAGE_PARENT=""
  SUCCESS=1
  printf '[%s] CROSSOVER SMOKE TEST: PASS\n' "$(date -u +%H:%M:%S)"
  printf 'Now run: ./run.sh %s\n' "$KIT_DIR"
  exit 0
fi

"$PYTHON_BIN" -I "$TOOLS_DIR/analyze.py" "$STAGE" \
  >"$STAGE/analysis.stdout" ||
  die "evidence validation or analysis failed"
log "Evidence validation and crossover analysis completed."
printf '{"state":"complete","updated_utc":"%s"}\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$STAGE/status.json"

"$PYTHON_BIN" -I - "$STAGE" "$(id -u)" <<'PY'
import os
import stat
import sys
from pathlib import Path

root = Path(sys.argv[1])
expected_uid = int(sys.argv[2])
for path in root.rglob("*"):
    item = path.lstat()
    if item.st_uid != expected_uid:
        raise SystemExit("evidence owner differs: " + str(path))
    if stat.S_ISLNK(item.st_mode):
        raise SystemExit("symlink in evidence: " + str(path))
    if not (stat.S_ISREG(item.st_mode) or stat.S_ISDIR(item.st_mode)):
        raise SystemExit("special file in evidence: " + str(path))
PY
(
  cd "$STAGE"
  find . -type f ! -name EVIDENCE-MANIFEST.sha256 -print0 |
    sort -z |
    xargs -0 sha256sum >EVIDENCE-MANIFEST.sha256
  sha256sum -c EVIDENCE-MANIFEST.sha256 >/dev/null
)
TEMP_ARCHIVE=$(mktemp "$STAGE_PARENT/.$OUTPUT_NAME.XXXXXX.tar.gz")
TEMP_SIDECAR=$(mktemp "$STAGE_PARENT/.$OUTPUT_NAME.XXXXXX.sha256")
tar -czf "$TEMP_ARCHIVE" \
  -C "$(dirname "$STAGE")" "$(basename "$STAGE")"
ARCHIVE_SHA256=$(sha256sum "$TEMP_ARCHIVE" | awk '{print $1}')
printf '%s  %s\n' "$ARCHIVE_SHA256" "$(basename "$ARCHIVE")" \
  >"$TEMP_SIDECAR"
mv -Tn "$TEMP_ARCHIVE" "$ARCHIVE"
[[ ! -e "$TEMP_ARCHIVE" ]] ||
  die "archive destination appeared during publication"
TEMP_ARCHIVE=""
ARCHIVE_PUBLISHED=1
mv -Tn "$TEMP_SIDECAR" "$ARCHIVE.sha256"
[[ ! -e "$TEMP_SIDECAR" ]] ||
  die "checksum destination appeared during publication"
TEMP_SIDECAR=""
SIDECAR_PUBLISHED=1
rm -rf -- "$STAGE"
SUCCESS=1
printf '[%s] CROSSOVER COMPLETE\n' "$(date -u +%H:%M:%S)"
printf 'Archive: %s\nChecksum: %s\n' "$ARCHIVE" "$ARCHIVE.sha256"
printf 'Return both files without editing.\n'
