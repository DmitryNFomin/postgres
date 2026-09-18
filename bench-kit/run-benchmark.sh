#!/usr/bin/env bash
# One-command, fail-closed driver for the complete v11 bare-metal benchmark.
set -Eeuo pipefail
export LC_ALL=C

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
STATUS_FILE="$SCRIPT_DIR/status.json"
LOG_DIR="$SCRIPT_DIR/run-logs"

usage() {
  cat <<'EOF'
Usage:
  SERVER_CPUS=... PGBENCH_CPUS=... ./run-benchmark.sh
                              verify, preflight, build, plateau probe,
                              smoke, run, crossover, collect
  ./run-benchmark.sh --status show current status and recent log output
  ./run-benchmark.sh --preflight-only

Run the default command inside tmux or screen on the idle Linux host.
SERVER_CPUS and PGBENCH_CPUS must be set to two disjoint CPU lists (Linux
cpu-list syntax, e.g. "1-31") that share no physical core; see
BAREMETAL-RUNBOOK-v11.md. CPU affinity is not tied to one fixed topology.
EOF
}

show_status() {
  if [[ -f "$STATUS_FILE" ]]; then
    cat "$STATUS_FILE"
  else
    echo "No status file exists yet."
  fi
  for progress in \
    "$SCRIPT_DIR/results/progress.json" \
    "$SCRIPT_DIR/smoke-results/progress.json"; do
    if [[ -f "$progress" ]]; then
      echo ""
      echo "Matrix progress from $progress:"
      cat "$progress"
      break
    fi
  done
  latest=$(find "$LOG_DIR" -maxdepth 1 -name 'benchmark-*.log' \
    -type f -print 2>/dev/null | sort | tail -n 1 || true)
  if [[ -n "$latest" ]]; then
    echo ""
    echo "Recent output from $latest:"
    tail -n 25 "$latest"
  fi
}

case "${1:-}" in
  "")
    ;;
  --status)
    show_status
    exit 0
    ;;
  --preflight-only)
    PREFLIGHT_ONLY=1
    ;;
  -h|--help)
    usage
    exit 0
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac

LOCK_DIR="$SCRIPT_DIR/.benchmark.lock"
LOCK_OWNED=0
release_lock() {
  if [[ "$LOCK_OWNED" -eq 1 ]]; then
    rm -f -- "$LOCK_DIR/owner"
    rmdir "$LOCK_DIR" 2>/dev/null || true
  fi
}
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  echo "ERROR: another launcher may be active ($LOCK_DIR exists)." >&2
  [[ ! -f "$LOCK_DIR/owner" ]] || cat "$LOCK_DIR/owner" >&2
  exit 1
fi
LOCK_OWNED=1
printf 'pid=%s started_utc=%s\n' "$$" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  >"$LOCK_DIR/owner"
trap release_lock EXIT

mkdir -p "$LOG_DIR"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
MAIN_LOG="$LOG_DIR/benchmark-$RUN_ID.log"
exec > >(tee -a "$MAIN_LOG") 2>&1

START_EPOCH=$(date +%s)
PHASE_START_EPOCH=$START_EPOCH
CURRENT_PHASE=starting
ACTIVE_PID=""

write_status() {
  local state=$1 message=$2
  local now elapsed phase_elapsed
  now=$(date +%s)
  elapsed=$((now - START_EPOCH))
  phase_elapsed=$((now - PHASE_START_EPOCH))
  python3 - "$STATUS_FILE" "$state" "$CURRENT_PHASE" "$message" \
    "$elapsed" "$phase_elapsed" "$MAIN_LOG" "$$" <<'PY'
import datetime
import json
import os
import sys
from pathlib import Path

(path_text, state, phase, message, elapsed, phase_elapsed, log_path,
 pid) = sys.argv[1:]
path = Path(path_text)
temporary = path.with_name(path.name + ".tmp")
data = {
    "updated_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "state": state,
    "phase": phase,
    "message": message,
    "elapsed_seconds": int(elapsed),
    "phase_elapsed_seconds": int(phase_elapsed),
    "log": log_path,
    "launcher_pid": int(pid),
}
temporary.write_text(
    json.dumps(data, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
os.replace(temporary, path)
PY
}

diagnostics() {
  local recent_output
  recent_output=$(tail -n 40 "$MAIN_LOG" 2>/dev/null || true)
  echo ""
  echo "Failure diagnostics"
  echo "-------------------"
  echo "Phase: $CURRENT_PHASE"
  echo "Main log: $MAIN_LOG"
  if [[ -f "$SCRIPT_DIR/results/progress.json" ]]; then
    echo "Full-matrix progress:"
    cat "$SCRIPT_DIR/results/progress.json"
  elif [[ -f "$SCRIPT_DIR/smoke-results/progress.json" ]]; then
    echo "Smoke-matrix progress:"
    cat "$SCRIPT_DIR/smoke-results/progress.json"
  fi
  echo "Recent output:"
  printf '%s\n' "$recent_output"
}

on_error() {
  local rc=$?
  trap - ERR INT TERM
  if [[ -n "$ACTIVE_PID" ]] && kill -0 "$ACTIVE_PID" 2>/dev/null; then
    kill "$ACTIVE_PID" 2>/dev/null || true
    wait "$ACTIVE_PID" 2>/dev/null || true
  fi
  write_status failed "phase failed with exit code $rc" || true
  diagnostics
  exit "$rc"
}
on_signal() {
  local rc=$1 signal=$2
  trap - ERR INT TERM
  if [[ -n "$ACTIVE_PID" ]] && kill -0 "$ACTIVE_PID" 2>/dev/null; then
    kill "$ACTIVE_PID" 2>/dev/null || true
    wait "$ACTIVE_PID" 2>/dev/null || true
  fi
  write_status failed "interrupted by $signal" || true
  diagnostics
  exit "$rc"
}
fail() {
  local message=$1
  echo "ERROR: $message" >&2
  write_status failed "$message" || true
  diagnostics
  exit 1
}
trap on_error ERR
trap 'on_signal 130 INT' INT
trap 'on_signal 143 TERM' TERM

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }

run_phase() {
  local phase=$1 estimate=$2
  shift 2
  CURRENT_PHASE=$phase
  PHASE_START_EPOCH=$(date +%s)
  write_status running "started; expected $estimate"
  log "PHASE $phase started (expected $estimate)"
  (
    trap - ERR
    exec "$@"
  ) &
  ACTIVE_PID=$!
  local last_heartbeat=0
  while kill -0 "$ACTIVE_PID" 2>/dev/null; do
    sleep 5
    if kill -0 "$ACTIVE_PID" 2>/dev/null; then
      local elapsed=$(( $(date +%s) - PHASE_START_EPOCH ))
      if (( elapsed - last_heartbeat >= 30 )); then
        log "HEARTBEAT phase=$phase elapsed=$((elapsed / 60))m"
        last_heartbeat=$elapsed
        if [[ "$phase" != matrix && "$phase" != smoke ]]; then
          write_status running "heartbeat; expected $estimate"
        fi
      fi
    fi
  done
  wait "$ACTIVE_PID"
  ACTIVE_PID=""
  log "PHASE $phase completed in $(( ($(date +%s) - PHASE_START_EPOCH) / 60 ))m"
  write_status running "completed"
}

log "PostgreSQL wait-event-tracing v11 bare-metal benchmark"
log "Kit: $SCRIPT_DIR"
log "Log: $MAIN_LOG"
log "SSH automation is not used. This process runs only on the current host."

if [[ "$(id -u)" -eq 0 ]]; then
  fail "PostgreSQL build and initdb must not run as root."
fi

SOCKET_PROBE="$SCRIPT_DIR/results/sock/.s.PGSQL.55471"
if [[ ${#SOCKET_PROBE} -gt 100 ]]; then
  fail "kit path is too long for a PostgreSQL Unix socket; extract it directly under the executor account's home"
fi

SAVED_SERVER_CPUS=${SERVER_CPUS:-}
SAVED_PGBENCH_CPUS=${PGBENCH_CPUS:-}
for variable in \
  SERVER_CPUS PGBENCH_CPUS RUNS DURATION WARMUP_SECONDS \
  PGBENCH_SCALE W1_ITERATIONS QUIESCENCE_SECONDS BENCHMARK_MODE \
  PGBENCH_KIT_PORT RUN_SEED BUILD_JOBS \
  CC CXX CFLAGS CXXFLAGS CPPFLAGS LDFLAGS MAKEFLAGS \
  PYTHONPATH PYTHONHOME \
  LD_LIBRARY_PATH LIBRARY_PATH CPATH C_INCLUDE_PATH CPLUS_INCLUDE_PATH \
  PKG_CONFIG_PATH PGOPTIONS PGSERVICE PGSERVICEFILE; do
  unset "$variable"
done
# CPU affinity is configurable, not fixed (brief-v11-wpc-kit.md): the two
# masks above are the one pair of inherited variables this launcher
# deliberately restores after the broad unset loop.
SERVER_CPUS=$SAVED_SERVER_CPUS
PGBENCH_CPUS=$SAVED_PGBENCH_CPUS

# brief-v11-wpc-kit.md, "Launch note": refuse to run while any sources.conf
# placeholder remains.
python3 "$SCRIPT_DIR/sources_conf.py" check "$SCRIPT_DIR/sources.conf" ||
  fail "sources.conf still has placeholder commit hash(es); the coordinator must fill these in first"

if [[ -f "$SCRIPT_DIR/PACKAGE-MANIFEST.sha256" ]]; then
  CURRENT_PHASE=package-integrity
  PHASE_START_EPOCH=$(date +%s)
  write_status running "verifying package files"
  (
    cd "$SCRIPT_DIR"
    sha256sum -c PACKAGE-MANIFEST.sha256
  )
else
  log "Development checkout: PACKAGE-MANIFEST.sha256 is absent."
fi

[[ -n "$SERVER_CPUS" ]] || fail "SERVER_CPUS must be set (see BAREMETAL-RUNBOOK-v11.md)"
[[ -n "$PGBENCH_CPUS" ]] || fail "PGBENCH_CPUS must be set (see BAREMETAL-RUNBOOK-v11.md)"
export SERVER_CPUS PGBENCH_CPUS
log "Configured CPU affinity: PostgreSQL=$SERVER_CPUS pgbench=$PGBENCH_CPUS"

run_phase preflight "under 1 minute" "$SCRIPT_DIR/00-check-host.sh"
run_phase kit-self-test "under 1 minute" "$SCRIPT_DIR/self-test.py"
run_phase initial-idle-check "1 to 20 minutes" "$SCRIPT_DIR/wait-for-idle.sh"

if [[ "${PREFLIGHT_ONLY:-0}" -eq 1 ]]; then
  CURRENT_PHASE=preflight
  write_status complete "preflight passed; no build or benchmark was started"
  log "Preflight passed. Nothing else was run."
  exit 0
fi

[[ ! -e "$SCRIPT_DIR/results" ]] ||
  fail "results already exists; preserving it for inspection"
[[ ! -e "$SCRIPT_DIR/smoke-results" ]] ||
  fail "smoke-results already exists; preserving it for inspection"
if find "$SCRIPT_DIR" -maxdepth 1 \
  \( -name 'results-*.tar.gz' -o -name 'results-*.tar.gz.sha256' \) \
  -print -quit | grep -q .; then
  fail "a prior result archive exists; move it out of the kit before starting"
fi

run_phase build "20 to 40 minutes" "$SCRIPT_DIR/01-build-all.sh"
run_phase plateau-probe "10 to 20 minutes" "$SCRIPT_DIR/plateau-probe.sh"
run_phase smoke "about 10 to 25 minutes" \
  env BENCHMARK_MODE=smoke "$SCRIPT_DIR/02-run-matrix.sh"
run_phase smoke-verification "under 1 minute" \
  python3 "$SCRIPT_DIR/analyze-results.py" "$SCRIPT_DIR/smoke-results" \
  --output-json "$SCRIPT_DIR/smoke-results/analysis.json" \
  --output-markdown "$SCRIPT_DIR/smoke-results/analysis.md"

log "Smoke matrix passed all 35 configuration/workload cells."
run_phase cooldown "1 to 20 minutes" "$SCRIPT_DIR/wait-for-idle.sh"
run_phase final-host-check "under 1 minute" "$SCRIPT_DIR/00-check-host.sh"
log "Cooldown and final host check passed."
log "The full 560-cell matrix will now start."
run_phase matrix "6.5 to 8.5 hours" "$SCRIPT_DIR/02-run-matrix.sh"
run_phase collection "a few minutes" "$SCRIPT_DIR/03-collect.sh"

archive=$(find "$SCRIPT_DIR" -maxdepth 1 \
  -name 'results-*.tar.gz' -type f -print | sort | tail -n 1)
[[ -n "$archive" && -f "$archive.sha256" ]] ||
  fail "collection completed without a results archive"
(
  cd "$SCRIPT_DIR"
  sha256sum -c "$(basename "$archive").sha256"
)

log "Matrix stage complete. Starting the second-stage persistent-backend crossover."
run_phase crossover-smoke "1 to 2 minutes" \
  "$SCRIPT_DIR/crossover/run.sh" --smoke "$SCRIPT_DIR"
run_phase crossover "2.5 to 3.5 hours" \
  "$SCRIPT_DIR/crossover/run.sh" "$SCRIPT_DIR"

crossover_archive=$(find /var/tmp -maxdepth 2 \
  -name 'w6c-persistent-crossover-*.tar.gz' -type f -print 2>/dev/null |
  sort | tail -n 1 || true)

CURRENT_PHASE=complete
PHASE_START_EPOCH=$(date +%s)
write_status complete "raw archives ready; local analysis required: $archive"
log "RAW CAPTURE SUCCESS"
log "Matrix archive:    $archive"
log "Matrix checksum:   $archive.sha256"
if [[ -n "$crossover_archive" ]]; then
  log "Crossover archive: $crossover_archive"
  log "Crossover checksum: $crossover_archive.sha256"
fi
log "Return the matrix archive/checksum and the crossover archive/checksum, unedited, and analyze them on the local machine."
