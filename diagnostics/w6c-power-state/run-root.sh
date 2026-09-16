#!/usr/bin/env bash
# Root-only telemetry wrapper for the non-root W6c diagnostic worker.
set -Eeuo pipefail
export LC_ALL=C
export PATH=/usr/sbin:/usr/bin:/sbin:/bin
unset \
  BASH_ENV CDPATH ENV LD_AUDIT LD_LIBRARY_PATH LD_PRELOAD \
  PYTHONHOME PYTHONPATH PYTHONSTARTUP
umask 077

SOURCE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
KIT_DIR=${1:?usage: run-root.sh /path/to/wet-v10-baremetal-r3}
KIT_DIR=$(cd "$KIT_DIR" && pwd -P)
EXPECTED_PACKAGE_MANIFEST_SHA256=1deb4eb2bf7698ebae432d3c4e684fb4b8c4ddfac1a5976b73f280668dc4c5f1
RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)
OUTPUT_NAME="w6c-power-diag-$RUN_ID"
STAGE_PARENT=""
STAGE=""
WORKER_DIR=""
TELEMETRY_DIR=""
TOOLS_DIR=""
PROVENANCE_DIR=""
DRIVER_LOG=""
ARCHIVE=""
TEMP_ARCHIVE=""
TEMP_SIDECAR=""
SAMPLER_PID=""
TURBOSTAT_PID=""
WORKER_PID=""
WORKER_CHILD_PID=""
SUCCESS=0

terminal_log() {
  local message="[$(date -u +%H:%M:%S)] $*"
  printf '%s\n' "$message"
  [[ -z "$STAGE" || ! -d "$STAGE" ]] ||
    printf '%s\n' "$message" >>"$DRIVER_LOG"
}
stop_telemetry() {
  local pid failed=0
  for pid in "$SAMPLER_PID" "$TURBOSTAT_PID"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
    fi
  done
  for pid in "$SAMPLER_PID" "$TURBOSTAT_PID"; do
    if [[ -n "$pid" ]] && ! wait "$pid" 2>/dev/null; then
      failed=1
    fi
  done
  SAMPLER_PID=""
  TURBOSTAT_PID=""
  return "$failed"
}
stop_worker() {
  if process_running "$WORKER_CHILD_PID"; then
    kill "$WORKER_CHILD_PID" 2>/dev/null || true
  fi
  if [[ -n "$WORKER_PID" ]]; then
    wait "$WORKER_PID" 2>/dev/null || true
  fi
  WORKER_PID=""
  WORKER_CHILD_PID=""
}
cleanup() {
  stop_worker
  stop_telemetry || true
  [[ -z "$TEMP_ARCHIVE" ]] || rm -f -- "$TEMP_ARCHIVE"
  [[ -z "$TEMP_SIDECAR" ]] || rm -f -- "$TEMP_SIDECAR"
  if [[ "$SUCCESS" -ne 1 ]]; then
    if [[ -n "$STAGE" && -d "$STAGE" ]]; then
      terminal_log "Diagnostic stopped. Partial evidence remains at $STAGE"
    else
      echo "Diagnostic stopped before staging." >&2
    fi
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
die() {
  echo "run-root.sh: ERROR: $*" >&2
  exit 1
}
require_root_owned_nonwritable() {
  local path=$1 permissions
  [[ "$(stat -c '%U' "$path")" == root ]] ||
    die "root does not own trusted path: $path"
  permissions=$(stat -c '%A' "$path")
  [[ ${permissions:5:1} != w && ${permissions:8:1} != w ]] ||
    die "trusted path is group/other writable: $path"
}
find_postgres_pids() {
  {
    pgrep -x postgres 2>/dev/null || true
    pgrep -x postmaster 2>/dev/null || true
  } | sort -nu
}
process_running() {
  local pid=$1 state
  [[ -n "$pid" ]] || return 1
  state=$(ps -o stat= -p "$pid" 2>/dev/null | awk '{print $1}')
  [[ -n "$state" && "$state" != Z* ]]
}

[[ "$(id -u)" -eq 0 ]] || die "run-root.sh must run as root"
for tool in \
  runuser turbostat taskset sha256sum tar pgrep awk grep stat getent \
  sort find xargs cp chown chmod rm readlink findmnt mktemp mv mkdir \
  dirname basename cat head sleep ps; do
  command -v "$tool" >/dev/null 2>&1 || die "missing tool: $tool"
done
INVOKED_SCRIPT=$(readlink -f "${BASH_SOURCE[0]}")
[[ "$INVOKED_SCRIPT" == "$SOURCE_DIR/run-root.sh" ]] ||
  die "could not resolve the invoked diagnostic script"
trusted_path=$INVOKED_SCRIPT
while :; do
  require_root_owned_nonwritable "$trusted_path"
  [[ "$trusted_path" == / ]] && break
  trusted_path=$(dirname "$trusted_path")
done
[[ -f "$SOURCE_DIR/DIAGNOSTIC-MANIFEST.sha256" ]] ||
  die "DIAGNOSTIC-MANIFEST.sha256 is missing"
require_root_owned_nonwritable \
  "$SOURCE_DIR/DIAGNOSTIC-MANIFEST.sha256"
(
  cd "$SOURCE_DIR"
  sha256sum -c DIAGNOSTIC-MANIFEST.sha256
) || die "diagnostic package integrity check failed"
[[ -f "$KIT_DIR/PACKAGE-MANIFEST.sha256" ]] ||
  die "r3 package manifest is missing"
[[ "$(sha256sum "$KIT_DIR/PACKAGE-MANIFEST.sha256" | awk '{print $1}')" \
   == "$EXPECTED_PACKAGE_MANIFEST_SHA256" ]] ||
  die "this is not the diagnosed r3 package"
(
  cd "$KIT_DIR"
  sha256sum -c PACKAGE-MANIFEST.sha256 >/dev/null
) || die "r3 package integrity check failed"
[[ -f "$KIT_DIR/work/manifest.json" ]] ||
  die "r3 build manifest is missing"

EXECUTOR=$(stat -c '%U' "$KIT_DIR/work/manifest.json")
[[ -n "$EXECUTOR" && "$EXECUTOR" != root ]] ||
  die "could not identify the original non-root executor"
EXECUTOR_HOME=$(getent passwd "$EXECUTOR" | awk -F: '{print $6}')
[[ -n "$EXECUTOR_HOME" ]] || die "could not find executor home"
PYTHON_LINK="$EXECUTOR_HOME/wet-python39-bin/python3"
[[ -x "$PYTHON_LINK" ]] ||
  die "expected Python 3.9 shim is missing: $PYTHON_LINK"
PYTHON_REAL=$(readlink -f "$PYTHON_LINK")
[[ -x "$PYTHON_REAL" && "$(stat -c '%U' "$PYTHON_REAL")" == root ]] ||
  die "Python interpreter is not a root-owned executable"
trusted_path=$PYTHON_REAL
while :; do
  require_root_owned_nonwritable "$trusted_path"
  [[ "$trusted_path" == / ]] && break
  trusted_path=$(dirname "$trusted_path")
done
"$PYTHON_REAL" -I -c \
  'import sys; raise SystemExit(sys.version_info < (3, 9))' ||
  die "Python 3.9 or newer is required"
POSTGRES_PIDS=$(find_postgres_pids)
[[ -z "$POSTGRES_PIDS" ]] || die "a PostgreSQL server is already running"
[[ "$(< /sys/devices/system/cpu/intel_pstate/no_turbo)" == 0 ]] ||
  die "turbo must remain enabled for this diagnostic"
grep -q -- '--Summary' < <(turbostat --help 2>&1) ||
  die "this turbostat lacks --Summary support"

STAGE_PARENT=$(mktemp -d /var/tmp/w6c-power-diag.XXXXXX)
chmod 711 "$STAGE_PARENT"
STAGE="$STAGE_PARENT/$OUTPUT_NAME"
ARCHIVE="$STAGE_PARENT/$OUTPUT_NAME.tar.gz"
WORKER_DIR="$STAGE/worker"
TELEMETRY_DIR="$STAGE/telemetry"
TOOLS_DIR="$STAGE/tools"
PROVENANCE_DIR="$STAGE/provenance"
DRIVER_LOG="$STAGE/driver.log"
mkdir "$STAGE" "$TOOLS_DIR" "$WORKER_DIR" \
  "$TELEMETRY_DIR" "$PROVENANCE_DIR"
chmod 711 "$STAGE"
chmod 755 "$TOOLS_DIR"
for name in \
  README.md analyze.py run-root.sh run-worker.sh run_turbostat.py \
  sample_system.py self-test.py DIAGNOSTIC-MANIFEST.sha256; do
  [[ -f "$SOURCE_DIR/$name" && ! -L "$SOURCE_DIR/$name" ]] ||
    die "diagnostic input is not a regular file: $name"
  require_root_owned_nonwritable "$SOURCE_DIR/$name"
  cp "$SOURCE_DIR/$name" "$TOOLS_DIR/"
done
(
  cd "$TOOLS_DIR"
  sha256sum -c DIAGNOSTIC-MANIFEST.sha256
) || die "root-owned diagnostic snapshot verification failed"
chown -R root:root "$TOOLS_DIR"
chmod -R go-w "$TOOLS_DIR"
chmod 755 "$TOOLS_DIR" "$TOOLS_DIR/run-worker.sh"
chmod 644 "$TOOLS_DIR"/*.py "$TOOLS_DIR"/*.md \
  "$TOOLS_DIR"/*.sha256 "$TOOLS_DIR"/*.sh
chmod 755 "$TOOLS_DIR/run-root.sh" "$TOOLS_DIR/run-worker.sh"

for relative in \
  PACKAGE-MANIFEST.sha256 host-check.json host-check.txt \
  work/manifest.json; do
  runuser -u "$EXECUTOR" -- \
    cat "$KIT_DIR/$relative" \
    >"$PROVENANCE_DIR/$(basename "$relative")" ||
    die "executor could not copy provenance file: $relative"
done
findmnt --json --target "$KIT_DIR" >"$PROVENANCE_DIR/findmnt.json"
cp "$TOOLS_DIR/DIAGNOSTIC-MANIFEST.sha256" "$PROVENANCE_DIR/"
printf '%s\n' "executor=$EXECUTOR" "python=$PYTHON_REAL" \
  >"$PROVENANCE_DIR/runtime.txt"

chown "$EXECUTOR" "$WORKER_DIR"
chmod 700 "$WORKER_DIR"
: >"$DRIVER_LOG"
cd /
terminal_log "W6c power-state diagnostic"
terminal_log "Executor: $EXECUTOR"
terminal_log "R3 kit: $KIT_DIR"
terminal_log "Root staging: $STAGE"
terminal_log "Turbo remains enabled; this diagnostic changes no host setting."

terminal_log "Waiting for the verified r3 idle gate."
runuser -u "$EXECUTOR" -- env -i \
  HOME="$EXECUTOR_HOME" USER="$EXECUTOR" LOGNAME="$EXECUTOR" \
  PATH=/usr/bin:/bin LC_ALL=C PYTHONDONTWRITEBYTECODE=1 \
  "$KIT_DIR/wait-for-idle.sh" >>"$DRIVER_LOG" 2>&1

POSTGRES_PIDS=$(find_postgres_pids)
[[ -z "$POSTGRES_PIDS" ]] ||
  die "a PostgreSQL server started during the idle gate"
"$PYTHON_REAL" -I "$TOOLS_DIR/sample_system.py" probe >/dev/null ||
  die "required /proc/sysfs telemetry is unavailable"
taskset -c 16-62:2 \
  "$PYTHON_REAL" -I "$TOOLS_DIR/sample_system.py" \
  record "$TELEMETRY_DIR/system.jsonl" --interval 1 &
SAMPLER_PID=$!
taskset -c 16-62:2 \
  "$PYTHON_REAL" -I "$TOOLS_DIR/run_turbostat.py" \
  "$TELEMETRY_DIR/turbostat.tsv" \
  "$TELEMETRY_DIR/turbostat.stderr" &
TURBOSTAT_PID=$!
sleep 3
process_running "$SAMPLER_PID" ||
  die "system telemetry sampler exited during startup"
process_running "$TURBOSTAT_PID" ||
  die "turbostat exited during startup"
grep -q 'Busy%' "$TELEMETRY_DIR/turbostat.tsv" &&
  grep -Eq 'Bzy_MHz|Avg_MHz' "$TELEMETRY_DIR/turbostat.tsv" &&
  grep -q 'PkgWatt' "$TELEMETRY_DIR/turbostat.tsv" ||
  die "turbostat lacks required frequency/power columns"

terminal_log "Telemetry started. Beginning 16 W6c cells."
runuser -u "$EXECUTOR" -- env -i \
  HOME="$EXECUTOR_HOME" USER="$EXECUTOR" LOGNAME="$EXECUTOR" \
  PATH=/usr/bin:/bin LC_ALL=C PYTHONDONTWRITEBYTECODE=1 \
  PYTHON_BIN="$PYTHON_REAL" \
  bash "$TOOLS_DIR/run-worker.sh" "$KIT_DIR" "$WORKER_DIR" \
  >>"$DRIVER_LOG" 2>&1 &
WORKER_PID=$!
for _ in {1..50}; do
  WORKER_CHILD_PID=$(pgrep -P "$WORKER_PID" | head -n 1 || true)
  [[ -z "$WORKER_CHILD_PID" ]] || break
  process_running "$WORKER_PID" || break
  sleep 0.1
done
[[ "$WORKER_CHILD_PID" =~ ^[0-9]+$ ]] || {
  stop_worker
  die "could not identify the non-root worker process"
}
[[ "$(stat -c '%U' "/proc/$WORKER_CHILD_PID")" == "$EXECUTOR" ]] || {
  stop_worker
  die "worker process owner differs from the executor"
}
while process_running "$WORKER_PID"; do
  if ! process_running "$SAMPLER_PID"; then
    stop_worker
    die "system telemetry sampler exited while the worker was running"
  fi
  if ! process_running "$TURBOSTAT_PID"; then
    stop_worker
    die "turbostat exited while the worker was running"
  fi
  sleep 2
done
if ! wait "$WORKER_PID"; then
  WORKER_PID=""
  die "diagnostic worker failed; see $DRIVER_LOG"
fi
WORKER_PID=""
WORKER_CHILD_PID=""

process_running "$SAMPLER_PID" ||
  die "system telemetry sampler exited before the worker completed"
process_running "$TURBOSTAT_PID" ||
  die "turbostat exited before the worker completed"
stop_telemetry ||
  die "a telemetry process failed while stopping"

RAW_WORKER_DIR="$STAGE/worker-raw"
mv "$WORKER_DIR" "$RAW_WORKER_DIR"
mkdir "$WORKER_DIR" "$WORKER_DIR/logs"
chmod 700 "$WORKER_DIR" "$WORKER_DIR/logs"
copy_worker_file() {
  local relative=$1
  runuser -u "$EXECUTOR" -- \
    cat "$RAW_WORKER_DIR/$relative" \
    >"$WORKER_DIR/$relative" ||
    die "could not snapshot worker output: $relative"
}
for relative in results.csv schedule.csv seed.txt events.jsonl; do
  copy_worker_file "$relative"
done
for run_index in {1..16}; do
  for relative in \
    "logs/server-$run_index.log.initdb" \
    "logs/server-setup-$run_index.log" \
    "logs/pgbench-init-$run_index.log" \
    "logs/server-$run_index.log" \
    "logs/pgbench-$run_index.log" \
    "logs/numa-before-$run_index.txt" \
    "logs/numa-during-$run_index.txt" \
    "logs/numa-after-$run_index.txt"; do
    copy_worker_file "$relative"
  done
done
rm -rf -- "$RAW_WORKER_DIR"
"$PYTHON_REAL" -I "$TOOLS_DIR/analyze.py" "$STAGE" \
  >"$STAGE/analysis.stdout"
terminal_log "Telemetry and evidence validation completed."
printf '{"state":"complete","updated_utc":"%s"}\n' \
  "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$STAGE/status.json"

"$PYTHON_REAL" -I - "$STAGE" <<'PY'
import os
import sys
from pathlib import Path

root = Path(sys.argv[1])
for path in root.rglob("*"):
    stat = path.lstat()
    if path.is_symlink():
        raise SystemExit("symlink in evidence: " + str(path))
    if stat.st_uid != 0 or stat.st_gid != 0:
        raise SystemExit("non-root-owned evidence: " + str(path))
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
mv -Tn "$TEMP_SIDECAR" "$ARCHIVE.sha256"
[[ ! -e "$TEMP_SIDECAR" ]] ||
  die "checksum destination appeared during publication"
TEMP_SIDECAR=""
chown "$EXECUTOR" "$ARCHIVE" "$ARCHIVE.sha256"
rm -rf -- "$STAGE"
SUCCESS=1
printf '[%s] DIAGNOSTIC COMPLETE\n' "$(date -u +%H:%M:%S)"
printf 'Archive: %s\nChecksum: %s\n' "$ARCHIVE" "$ARCHIVE.sha256"
printf 'Return both files without editing.\n'
