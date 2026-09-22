#!/usr/bin/env bash
# 02-run-matrix.sh
#
# Runs the full v11 measurement matrix: 7 configurations x 5 workloads x 16
# repetitions = 560 runs. Each repetition is one complete randomized block
# per workload (see latin_square.py); every cell uses a fresh PostgreSQL
# data directory. This is a single vanilla-vs-v11 matrix; there is no v9/v10
# pair (see brief-v11-wpc-kit.md).
#
# The seven configurations:
#   1 master       vanilla build A, nothing loaded
#   2 master-aa    independent vanilla build B (noise/control arm)
#   3 control      patched + patches-control (timed sites compiled out),
#                  module absent
#   4 hook-null    patched build, module absent (hook call sites present
#                  but never attach)
#   5 module-off   patched build, module loaded, capture = off
#   6 stats        patched build, module loaded, capture = stats
#   7 trace        patched build, module loaded, capture = trace
#
# The five workloads:
#   W1   test_wait_primitive microbenchmark, all five functions, 1e8 iters
#   W3   short LWLock contention, 8 pgbench clients
#   W4   pgbench read-only (-S), 16 clients, 4 GB shared_buffers
#   W5   pgbench TPC-B (read-write, no -S), 16 clients, 4 GB shared_buffers
#   W6c  pgbench read-only (-S), 32 clients, 32 MB shared_buffers
#
# Expect this step to take most of the 8-10 hour run. It is unattended and
# safe to run under tmux/screen.
set -Eeuo pipefail
export LC_ALL=C

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WORK="$SCRIPT_DIR/work"

# shellcheck source=lib-python.sh
source "$SCRIPT_DIR/lib-python.sh"
if ! resolve_python; then
  echo "02-run-matrix.sh: ERROR: no Python 3.9+ interpreter found (tried \$PYTHON_BIN_OVERRIDE, python3.11, python3.9, python3); on Rocky Linux 8 install one with: sudo dnf install -y python3.11 (or: sudo dnf install -y python39)" >&2
  exit 1
fi

"$PYTHON_BIN" "$SCRIPT_DIR/sources_conf.py" check "$SCRIPT_DIR/sources.conf" ||
  { echo "02-run-matrix.sh: ERROR: sources.conf is not filled in" >&2; exit 1; }

MODE=${BENCHMARK_MODE:-full}
# RUNS/DURATION/WARMUP_SECONDS/PGBENCH_SCALE/W1_ITERATIONS/
# QUIESCENCE_SECONDS all come from benchmark_protocol.py's FULL_PROFILE/
# SMOKE_PROFILE -- the single source analyze-results.py's verify_protocol()
# checks protocol.json against -- rather than a second hardcoded copy here
# that could disagree with it. Those profiles are themselves compressed
# under SELFTEST_FAKE_PREFIX (benchmark_protocol.py), so this script never
# needs its own fake-mode branch for any of these six values.
case "$MODE" in
  full)
    RESULTS="$SCRIPT_DIR/results"
    PROFILE_NAME=FULL_PROFILE
    ;;
  smoke)
    RESULTS="$SCRIPT_DIR/smoke-results"
    PROFILE_NAME=SMOKE_PROFILE
    ;;
  *)
    echo "02-run-matrix.sh: ERROR: BENCHMARK_MODE must be full or smoke" >&2
    exit 1
    ;;
esac
read -r RUNS DURATION WARMUP_SECONDS PGBENCH_SCALE W1_ITERATIONS QUIESCENCE_SECONDS \
  < <("$PYTHON_BIN" -c "
from benchmark_protocol import $PROFILE_NAME as p
print(p['runs_per_cell'], p['duration_seconds'], p['warmup_seconds'],
      p['pgbench_scale'], p['w1_iterations'], p['quiescence_seconds'])
")
MANIFEST="$WORK/manifest.json"
HOST_CHECK="$SCRIPT_DIR/host-check.json"
AFFINITY_HELPER="$SCRIPT_DIR/cpu_affinity.py"
MATRIX_START_EPOCH=0
CELLS_COMPLETED=0

die() {
  echo "" >&2
  echo "02-run-matrix.sh: ERROR: $*" >&2
  if [[ -n "${CURRENT_CELL:-}" ]]; then
    echo "FAILED at cell: $CURRENT_CELL" >&2
  fi
  if declare -F write_progress >/dev/null 2>&1 &&
     [[ -n "${PROGRESS:-}" && -d "${RESULTS:-/nonexistent}" ]]; then
    write_progress failed "$*" >/dev/null 2>&1 || true
  fi
  stop_freq_sampler >/dev/null 2>&1 || true
  stop_client >/dev/null 2>&1 || true
  stop_server >/dev/null 2>&1 || true
  declare -F stop_cell_watchdog >/dev/null 2>&1 && stop_cell_watchdog
  exit 1
}
log() { printf '%s\n' "[$(date -u +%H:%M:%S)] $*"; }
REHEARSAL=0
[[ "${BENCHMARK_REHEARSAL:-0}" == 1 ]] && REHEARSAL=1
rehearsal_note() { log "*** REHEARSAL NOTE (not evidence, not blocking): $* ***"; }

# Per-cell wall-time budget: a real host runs a W1 cell (a single backend
# looping over test_wait_primitive, no pgbench, no warmup/duration window
# bounding it the way pgbench workloads have) in well under a minute, but
# under QEMU emulation the same 1e8-iteration loop can run for the better
# part of an hour per cell -- found when a real rehearsal ran one W1 cell
# for 53+ minutes before being killed by hand. This applies in BOTH modes
# (not just rehearsal): a real budget of 30 minutes is generous enough
# that no real cell has ever come close to it, so it costs nothing there,
# but it means a genuinely hung cell on real hardware also surfaces
# instead of silently consuming hours. Under BENCHMARK_REHEARSAL=1 the
# budget tightens to 10 minutes, matched to how fast a rehearsal is
# already expected to move. Applied around each run_cell() call in the
# matrix loop below, not inside run_cell() itself, so the timer covers
# exactly the wall time a single cell takes end to end (server start
# through server stop) with no risk of drifting from what the loop's own
# "done in Ns" timing already reports.
CELL_TIMEOUT_SECONDS=1800
(( REHEARSAL )) && CELL_TIMEOUT_SECONDS=600
MAIN_PID=$$
WATCHDOG_PID=""
start_cell_watchdog() {
  local label=$1
  (
    sleep "$CELL_TIMEOUT_SECONDS"
    echo "" >&2
    echo "02-run-matrix.sh: ERROR: cell exceeded its ${CELL_TIMEOUT_SECONDS}s wall-time budget: $label" >&2
    kill -TERM "$MAIN_PID" 2>/dev/null
  ) &
  WATCHDOG_PID=$!
  disown "$WATCHDOG_PID" 2>/dev/null || true
}
stop_cell_watchdog() {
  [[ -z "$WATCHDOG_PID" ]] || kill "$WATCHDOG_PID" 2>/dev/null || true
  wait "$WATCHDOG_PID" 2>/dev/null || true
  WATCHDOG_PID=""
}
handle_cell_timeout() {
  trap - TERM
  echo "" >&2
  echo "02-run-matrix.sh: ERROR: cell wall-time budget exceeded (${CELL_TIMEOUT_SECONDS}s)${CURRENT_CELL:+: $CURRENT_CELL}" >&2
  exit 124
}
trap handle_cell_timeout TERM

# ---------------------------------------------------------------------------
# Server/client lifecycle, defined early because die() calls these.
# ---------------------------------------------------------------------------
active_prefix=""
active_datadir=""
active_pgbench_pid=""
active_sampler_pid=""
CURRENT_CELL=""

run_from_prefix() {
  local prefix=$1
  shift
  env LD_LIBRARY_PATH="$prefix/lib:${LD_LIBRARY_PATH:-}" "$@"
}

run_pgbench_from_prefix() {
  local prefix=$1 appname=$2
  shift 2
  exec env \
    LD_LIBRARY_PATH="$prefix/lib:${LD_LIBRARY_PATH:-}" \
    PGAPPNAME="$appname" PGHOST="$SOCKET_DIR" PGPORT="$PORT" \
    PGUSER="$DBUSER" "$@"
}

assert_process_affinity() {
  local pid=$1 expected=$2 role=$3
  [[ -z "${SELFTEST_FAKE_PREFIX:-}" ]] || return 0
  "$PYTHON_BIN" "$AFFINITY_HELPER" verify-pid "$pid" "$expected" ||
    die "$role process does not have the required CPU affinity"
}

stop_client() {
  if [[ -n "$active_pgbench_pid" ]] &&
     kill -0 "$active_pgbench_pid" 2>/dev/null; then
    kill "$active_pgbench_pid" 2>/dev/null || true
    wait "$active_pgbench_pid" 2>/dev/null || true
  fi
  active_pgbench_pid=""
}

stop_freq_sampler() {
  if [[ -n "$active_sampler_pid" ]] &&
     kill -0 "$active_sampler_pid" 2>/dev/null; then
    kill "$active_sampler_pid" 2>/dev/null || true
    wait "$active_sampler_pid" 2>/dev/null || true
  fi
  active_sampler_pid=""
}

stop_server() {
  if [[ -n "$active_prefix" && -n "$active_datadir" ]]; then
    if ! run_from_prefix "$active_prefix" "$active_prefix/bin/pg_ctl" \
      -D "$active_datadir" -m fast -w stop; then
      return 1
    fi
  fi
  active_prefix=""
  active_datadir=""
}

stop_server_checked() {
  local context=$1
  stop_server ||
    die "normal server shutdown failed after $context; the data directory was left at $active_datadir"
}

on_error() {
  local rc=$?
  trap - ERR EXIT
  echo "" >&2
  echo "02-run-matrix.sh: an unexpected error occurred (exit $rc)." >&2
  if [[ -n "$CURRENT_CELL" ]]; then
    echo "FAILED at cell: $CURRENT_CELL" >&2
    echo "The data directory for this cell was left in place for inspection." >&2
  fi
  stop_freq_sampler
  stop_client
  stop_server >/dev/null 2>&1 || true
  stop_cell_watchdog
  exit "$rc"
}
trap on_error ERR
on_exit() {
  stop_freq_sampler
  stop_client
  stop_server >/dev/null 2>&1 || true
  stop_cell_watchdog
}
trap on_exit EXIT

[[ ! -e "$RESULTS" ]] ||
  die "results directory already exists: $RESULTS
A partial matrix cannot be resumed or merged -- every configuration has to
meet the same machine conditions for the comparison to mean anything. Move
$RESULTS aside (or remove it) if you want to start over, or run
./03-collect.sh first if it holds a finished run you want to keep."

# ---------------------------------------------------------------------------
# CPU affinity is configurable (not tied to one topology): SERVER_CPUS and
# PGBENCH_CPUS must already be set in the environment by the caller
# (run-benchmark.sh reads them from the operator, see the runbook), and are
# re-verified live here exactly as 00-check-host.sh verified them.
# ---------------------------------------------------------------------------
: "${SERVER_CPUS:?SERVER_CPUS must be set (see the runbook)}"
: "${PGBENCH_CPUS:?PGBENCH_CPUS must be set (see the runbook)}"
export SERVER_CPUS PGBENCH_CPUS

for tool in "$PYTHON_BIN" sha256sum awk ps taskset lscpu; do
  command -v "$tool" >/dev/null 2>&1 || die "required tool not found: $tool"
done
if [[ -z "${SELFTEST_FAKE_PREFIX:-}" ]]; then
  [[ -f "$AFFINITY_HELPER" && ! -L "$AFFINITY_HELPER" ]] ||
    die "missing regular CPU-affinity helper: $AFFINITY_HELPER"
  AFFINITY_PROOF_JSON=$("$PYTHON_BIN" "$AFFINITY_HELPER" collect) ||
    die "live CPU topology, or SERVER_CPUS/PGBENCH_CPUS, failed verification"
  [[ -f "$HOST_CHECK" ]] ||
    die "missing $HOST_CHECK -- run ./00-check-host.sh first"
  "$PYTHON_BIN" "$AFFINITY_HELPER" \
    verify-host-report "$HOST_CHECK" "$(hostname)" ||
    die "host check is not a clean report for this host"
else
  # analyze-results.py's verify_protocol() runs validate_affinity_proof()
  # against this unconditionally (fake mode included), so it has to be a
  # real proof shape, not a placeholder -- see cpu_affinity.fake_topology_
  # proof()'s docstring.
  AFFINITY_PROOF_JSON=$("$PYTHON_BIN" "$AFFINITY_HELPER" fake-collect) ||
    die "SERVER_CPUS/PGBENCH_CPUS failed fake-mode verification"
  # The protocol.json provenance step below hashes host-check.txt/.json
  # unconditionally; if this is invoked directly (not through
  # run-benchmark.sh, which always runs 00-check-host.sh first, including
  # in fake mode), provide minimal stand-ins if real ones are not there.
  [[ -f "$SCRIPT_DIR/host-check.txt" ]] ||
    echo "selftest-fakebin: 00-check-host.sh was not run" >"$SCRIPT_DIR/host-check.txt"
  if [[ ! -f "$SCRIPT_DIR/host-check.json" ]]; then
    "$PYTHON_BIN" - "$SCRIPT_DIR/host-check.json" "$AFFINITY_PROOF_JSON" <<'PY'
import json
import sys
out, affinity_json = sys.argv[1:]
data = {
    "warning_count": 0,
    "hostname": "selftest-fakebin",
    "co_resident_postgres_process_count": 0,
    "host_isolation": "dedicated",
    "cpu_affinity_protocol": json.loads(affinity_json),
}
with open(out, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, sort_keys=True)
    f.write("\n")
PY
  fi
fi
[[ -r "$MANIFEST" ]] ||
  die "missing $MANIFEST -- run ./01-build-all.sh first"
BUILD_MANIFEST_SHA256=$(sha256sum "$MANIFEST" | awk '{print $1}')

PREFIX_BASELINE_A="$WORK/install/base-a"
PREFIX_BASELINE_B="$WORK/install/base-b"
PREFIX_PATCHED="$WORK/install/patchd"
PREFIX_CONTROL="$WORK/install/ctrlop"
for p in "$PREFIX_BASELINE_A" "$PREFIX_BASELINE_B" "$PREFIX_PATCHED" "$PREFIX_CONTROL"; do
  [[ -x "$p/bin/postgres" ]] || die "missing build at $p -- run ./01-build-all.sh first"
done

[[ -r "$SCRIPT_DIR/build_manifest_rules.py" ]] ||
  die "missing Python helper: $SCRIPT_DIR/build_manifest_rules.py"
"$PYTHON_BIN" - "$SCRIPT_DIR" "$MANIFEST" \
  "baseline-a=$PREFIX_BASELINE_A" "baseline-b=$PREFIX_BASELINE_B" \
  "patched=$PREFIX_PATCHED" "control=$PREFIX_CONTROL" <<'PY' ||
import json
import sys
from pathlib import Path

script_dir, manifest_path = sys.argv[1:3]
sys.path.insert(0, script_dir)
from build_manifest_rules import ManifestMismatch, validate_installed_builds

manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
prefixes = dict(item.split("=", 1) for item in sys.argv[3:])
try:
    validate_installed_builds(manifest, prefixes)
except ManifestMismatch as exc:
    raise SystemExit(str(exc))
PY
  die "installed binaries do not match $MANIFEST; rebuild before running"

MODULE_NAME=$("$PYTHON_BIN" "$SCRIPT_DIR/sources_conf.py" get "$SCRIPT_DIR/sources.conf" MODULE_NAME)
HOOKS_INSTALLED_FUNCTION=$("$PYTHON_BIN" "$SCRIPT_DIR/sources_conf.py" get "$SCRIPT_DIR/sources.conf" HOOKS_INSTALLED_FUNCTION)
GUC_CAPTURE="$MODULE_NAME.capture"
GUC_MAX_TRANCHES="$MODULE_NAME.max_tranches"
GUC_TRACE_RING_SIZE="$MODULE_NAME.trace_ring_size"

PORT=${PGBENCH_KIT_PORT:-55471}
DBUSER=$(id -un)

(( RUNS > 0 )) || die "RUNS must be greater than zero"
MIN_DURATION=5
MIN_WARMUP_SECONDS=2
[[ -z "${SELFTEST_FAKE_PREFIX:-}" ]] || { MIN_DURATION=1; MIN_WARMUP_SECONDS=1; }
(( DURATION >= MIN_DURATION )) || die "DURATION must be at least $MIN_DURATION seconds"
(( WARMUP_SECONDS >= MIN_WARMUP_SECONDS )) || die "WARMUP_SECONDS must be at least $MIN_WARMUP_SECONDS seconds"
(( PGBENCH_SCALE > 0 )) || die "PGBENCH_SCALE must be greater than zero"
(( W1_ITERATIONS > 0 )) || die "W1_ITERATIONS must be greater than zero"
(( QUIESCENCE_SECONDS >= 0 )) || die "QUIESCENCE_SECONDS must not be negative"

mapfile -t CONFIGS < <("$PYTHON_BIN" -c \
  "from benchmark_protocol import CONFIGS; print('\n'.join(CONFIGS))")
mapfile -t WORKLOADS < <("$PYTHON_BIN" -c \
  "from benchmark_protocol import WORKLOADS; print('\n'.join(WORKLOADS))")
mapfile -t W1_FUNCTIONS < <("$PYTHON_BIN" -c \
  "from benchmark_protocol import W1_FUNCTIONS; print('\n'.join(W1_FUNCTIONS))")
# Single config table (benchmark_protocol.py) for which configurations must
# prove the module absent vs. loaded, and which capture mode each loaded
# config expects -- assert_mode_proof() below must consult these, not a
# private bash case statement that could drift from benchmark_protocol.py.
mapfile -t MODULE_ABSENT_CONFIGS < <("$PYTHON_BIN" -c \
  "from benchmark_protocol import MODULE_ABSENT_CONFIGS as c; print('\n'.join(c))")
mapfile -t MODULE_LOADED_CONFIGS < <("$PYTHON_BIN" -c \
  "from benchmark_protocol import MODULE_LOADED_CONFIGS as c; print('\n'.join(c))")
declare -A CAPTURE_FOR_CONFIG=()
while IFS=$'\t' read -r config_name capture_value; do
  [[ -n "$config_name" ]] || continue
  CAPTURE_FOR_CONFIG["$config_name"]="$capture_value"
done < <("$PYTHON_BIN" -c \
  "from benchmark_protocol import CAPTURE_FOR_CONFIG as c
for k, v in c.items():
    print(f'{k}\t{v}')")

in_list() {
  local needle=$1 item
  shift
  for item in "$@"; do [[ "$item" == "$needle" ]] && return 0; done
  return 1
}
# Same single-source reasoning for which repetition(s) trigger the early
# A/A gate: benchmark_protocol.EARLY_AA_GATE_REPETITIONS is itself
# compressed under SELFTEST_FAKE_PREFIX (to just repetition 1, since a
# fake-mode FULL_PROFILE only ever runs one repetition), so a hardcoded
# "6|10" bash case here would never fire in fake mode and a launcher-level
# fake run's aa-early.jsonl would end up missing the checkpoint
# analyze-results.py's verify_early_aa() requires for "full" mode.
mapfile -t EARLY_AA_GATE_REPETITIONS < <("$PYTHON_BIN" -c \
  "from benchmark_protocol import EARLY_AA_GATE_REPETITIONS as r; print('\n'.join(str(x) for x in r))")
CELLS_PER_REPETITION=$(( ${#CONFIGS[@]} * ${#WORKLOADS[@]} ))
TOTAL_CELLS=$(( ${#CONFIGS[@]} * ${#WORKLOADS[@]} * RUNS ))

SOCKET_DIR="$RESULTS/sock"
DATA_ROOT="$RESULTS/data"
LOG_DIR="$RESULTS/logs"
CSV="$RESULTS/results.csv"
SCHEDULE_CSV="$RESULTS/schedule.csv"
SEED_FILE="$RESULTS/seed.txt"
PROTOCOL="$RESULTS/protocol.json"
RECORDING_DIR="$RESULTS/recording-proofs"
W3_QUAL_DIR="$RESULTS/w3-qualification"
CLIENT_LOAD_DIR="$RESULTS/client-load"
W1_DETAIL_DIR="$RESULTS/w1-detail"
TIMING_DIR="$RESULTS/pg-test-timing"
WORKLOAD_DIR="$SCRIPT_DIR/workloads"
PROGRESS="$RESULTS/progress.json"
AA_EARLY="$RESULTS/aa-early.jsonl"
PLATEAU_PROBE_RESULT="$SCRIPT_DIR/plateau-probe-result.json"

mkdir -p "$RESULTS" "$SOCKET_DIR" "$DATA_ROOT" "$LOG_DIR" \
  "$RECORDING_DIR" "$W3_QUAL_DIR" "$CLIENT_LOAD_DIR" "$W1_DETAIL_DIR" \
  "$TIMING_DIR"
: >"$AA_EARLY"

for workload_file in w3-short-lwlock.sql recording-proof.sql w3-qualification.sql; do
  [[ -r "$WORKLOAD_DIR/$workload_file" ]] ||
    die "missing workload file: $WORKLOAD_DIR/$workload_file"
done
for helper in benchmark_protocol.py cpu_affinity.py w3_qualification.py \
              wilcoxon.py latin_square.py sources_conf.py \
              build_manifest_rules.py; do
  [[ -r "$SCRIPT_DIR/$helper" ]] ||
    die "missing Python helper: $SCRIPT_DIR/$helper"
done

SOCKET_PROBE="$SOCKET_DIR/.s.PGSQL.$PORT"
if [[ ${#SOCKET_PROBE} -gt 100 ]]; then
  die "the kit's path is too long for a unix socket ($SOCKET_PROBE is ${#SOCKET_PROBE} bytes). Move the whole v11 kit somewhere with a shorter path (e.g. directly under your home directory) and try again."
fi
[[ ! -e "$SOCKET_PROBE" ]] ||
  die "a socket already exists at $SOCKET_PROBE; is a server from a previous run still up?"

# ---------------------------------------------------------------------------
# Plateau probe decision (see plateau-probe.sh). Falls back to "unpinned"
# (no numactl/taskset wrapping of initdb/dataset-clone) if the probe has
# not been run -- that is only expected during --preflight-only/self-test
# style dry runs, never for a real full/smoke matrix, which run-benchmark.sh
# always precedes with the probe.
# ---------------------------------------------------------------------------
NUMA_WRAP=()
if [[ -f "$PLATEAU_PROBE_RESULT" ]]; then
  read -r NUMA_DECISION NUMA_NODE < <("$PYTHON_BIN" -c "
import json
data = json.load(open('$PLATEAU_PROBE_RESULT', encoding='utf-8'))
print(data['selected_variant'], data.get('server_numa_node', ''))
")
  if [[ "$NUMA_DECISION" == pinned ]]; then
    if command -v numactl >/dev/null 2>&1; then
      NUMA_WRAP=(numactl "--cpunodebind=$NUMA_NODE" "--membind=$NUMA_NODE")
    else
      NUMA_WRAP=(taskset -c "$SERVER_CPUS")
    fi
    log "Plateau probe selected: pinned (${NUMA_WRAP[*]})"
  else
    log "Plateau probe selected: unpinned"
  fi
else
  log "No plateau-probe-result.json found; proceeding unpinned. (run-benchmark.sh always runs plateau-probe.sh first for a real matrix)"
fi

# ---------------------------------------------------------------------------
# Per-configuration postgresql.conf additions and build mapping.
# ---------------------------------------------------------------------------
config_build_prefix() {
  case "$1" in
    master) echo "$PREFIX_BASELINE_A" ;;
    master-aa) echo "$PREFIX_BASELINE_B" ;;
    control) echo "$PREFIX_CONTROL" ;;
    hook-null|module-off|stats|trace) echo "$PREFIX_PATCHED" ;;
    *) die "unknown config: $1" ;;
  esac
}

config_build_name() {
  case "$1" in
    master) echo baseline-a ;;
    master-aa) echo baseline-b ;;
    control) echo control ;;
    hook-null|module-off|stats|trace) echo patched ;;
    *) die "unknown config: $1" ;;
  esac
}

append_config_lines() {
  local config=$1 conf_file=$2
  case "$config" in
    master|master-aa|control|hook-null)
      # No module in shared_preload_libraries for any of these: control and
      # hook-null are both "module absent" proof configurations (see the
      # brief's mode-proof table). control differs from hook-null only in
      # which *binary* runs (patches-control's identical-bodies patch).
      ;;
    module-off)
      {
        echo "shared_preload_libraries = '$MODULE_NAME'"
        echo "$GUC_CAPTURE = 'off'"
      } >>"$conf_file"
      ;;
    stats)
      {
        echo "shared_preload_libraries = '$MODULE_NAME'"
        echo "$GUC_CAPTURE = 'stats'"
        echo "$GUC_MAX_TRANCHES = 192"
        echo "$GUC_TRACE_RING_SIZE = '4MB'"
      } >>"$conf_file"
      ;;
    trace)
      {
        echo "shared_preload_libraries = '$MODULE_NAME'"
        echo "$GUC_CAPTURE = 'trace'"
        echo "$GUC_MAX_TRANCHES = 192"
        echo "$GUC_TRACE_RING_SIZE = '4MB'"
      } >>"$conf_file"
      ;;
    *)
      die "unknown config: $config"
      ;;
  esac
}

workload_shared_buffers() {
  case "$1" in
    W1) echo 128MB ;;
    W3) echo 16MB ;;
    W4) echo 4GB ;;
    W5) echo 4GB ;;
    W6c) echo 32MB ;;
    *) die "unknown workload: $1" ;;
  esac
}

init_cluster() {
  local prefix=$1 datadir=$2 shared_buffers=$3 logfile=$4
  local conf="$datadir/postgresql.conf"

  run_from_prefix "$prefix" "${NUMA_WRAP[@]}" "$prefix/bin/initdb" \
    -D "$datadir" --no-sync --no-locale -E UTF8 \
    --auth-local=trust --auth-host=trust --username="$DBUSER" \
    >>"$logfile.initdb" 2>&1 ||
    die "initdb failed; see $logfile.initdb"

  {
    echo "listen_addresses = ''"
    echo "unix_socket_directories = '$SOCKET_DIR'"
    echo "max_connections = 100"
    echo "shared_buffers = '$shared_buffers'"
    echo "autovacuum = off"
    echo "checkpoint_timeout = '1h'"
    echo "max_wal_size = '8GB'"
  } >>"$conf"
}

start_server() {
  local prefix=$1 datadir=$2 config=$3 logfile=$4
  local conf="$datadir/postgresql.conf"

  append_config_lines "$config" "$conf"

  active_prefix=$prefix
  active_datadir=$datadir
  run_from_prefix "$prefix" taskset -c "$SERVER_CPUS" \
    "$prefix/bin/pg_ctl" \
    -D "$datadir" -l "$logfile" \
    -o "-p $PORT -k $SOCKET_DIR" -w start >/dev/null ||
    die "server failed to start for $config; see $logfile"
  local postmaster_pid
  read -r postmaster_pid <"$datadir/postmaster.pid"
  [[ "$postmaster_pid" =~ ^[0-9]+$ ]] ||
    die "could not read the postmaster pid after starting $config"
  assert_process_affinity \
    "$postmaster_pid" "$SERVER_CPUS" "PostgreSQL ($config)"
  ACTIVE_POSTMASTER_PID=$postmaster_pid
}

psql_for() {
  local prefix=$1
  shift
  PGHOST="$SOCKET_DIR" PGPORT="$PORT" PGUSER="$DBUSER" \
    run_from_prefix "$prefix" "$prefix/bin/psql" \
    -X -v ON_ERROR_STOP=1 -d postgres "$@"
}

# ---------------------------------------------------------------------------
# Mode proofs (brief-v11-wpc-kit.md, "Configurations"): every cell must
# PROVE the feature is in the state its configuration claims before the
# timed window starts. A silent preload failure would otherwise make
# "stats"/"trace" rows read identical to "module-off" -- a false "wait
# tracing is free" result.
# ---------------------------------------------------------------------------
capture_should_be_active() {
  case "$1" in
    stats|trace) return 0 ;;
    *) return 1 ;;
  esac
}

capture_should_trace() {
  [[ "$1" == trace ]]
}

assert_mode_proof() {
  local prefix=$1 config=$2 preload capture_out expected hooks_installed

  preload=$(psql_for "$prefix" -qAtc "SHOW shared_preload_libraries" 2>&1) ||
    die "could not read shared_preload_libraries for $config: $preload"

  if in_list "$config" "${MODULE_ABSENT_CONFIGS[@]}"; then
    case "$preload" in
      *"$MODULE_NAME"*)
        die "$MODULE_NAME is unexpectedly present in shared_preload_libraries for $config ('$preload')"
        ;;
    esac
    if capture_out=$(psql_for "$prefix" -qAtc "SHOW $GUC_CAPTURE" 2>&1); then
      die "$GUC_CAPTURE is unexpectedly readable for $config (got '$capture_out')"
    fi
    local extension_count
    extension_count=$(psql_for "$prefix" -qAtc \
      "SELECT count(*) FROM pg_extension WHERE extname = '$MODULE_NAME'") ||
      die "could not inspect pg_extension for $config"
    [[ "$extension_count" == 0 ]] ||
      die "$MODULE_NAME SQL extension unexpectedly exists for $config"
  elif in_list "$config" "${MODULE_LOADED_CONFIGS[@]}"; then
    case "$preload" in
      *"$MODULE_NAME"*) ;;
      *)
        die "$MODULE_NAME is missing from shared_preload_libraries for $config (got '$preload')"
        ;;
    esac
    expected=${CAPTURE_FOR_CONFIG[$config]:-}
    [[ -n "$expected" ]] ||
      die "no expected capture mode for $config in benchmark_protocol.CAPTURE_FOR_CONFIG"
    capture_out=$(psql_for "$prefix" -qAtc "SHOW $GUC_CAPTURE" 2>&1) ||
      die "$GUC_CAPTURE is not readable for $config even though the module should be loaded"
    [[ "$capture_out" == "$expected" ]] ||
      die "$GUC_CAPTURE is '$capture_out' for $config, expected exactly '$expected'"
    psql_for "$prefix" -qAtc "CREATE EXTENSION IF NOT EXISTS $MODULE_NAME" >/dev/null ||
      die "CREATE EXTENSION $MODULE_NAME failed for $config"
    local extension_count
    extension_count=$(psql_for "$prefix" -qAtc \
      "SELECT count(*) FROM pg_extension WHERE extname = '$MODULE_NAME'") ||
      die "could not verify $MODULE_NAME SQL installation for $config"
    [[ "$extension_count" == 1 ]] ||
      die "$MODULE_NAME SQL extension is not installed for $config"
    if [[ "$config" == module-off ]]; then
      hooks_installed=$(psql_for "$prefix" -qAtc \
        "SELECT $HOOKS_INSTALLED_FUNCTION()") ||
        die "$HOOKS_INSTALLED_FUNCTION() failed for $config"
      [[ "$hooks_installed" == f ]] ||
        die "$HOOKS_INSTALLED_FUNCTION() returned '$hooks_installed' for module-off, expected false"
    fi
  else
    die "unknown config: $config (not in benchmark_protocol's MODULE_ABSENT_CONFIGS or MODULE_LOADED_CONFIGS)"
  fi
}

# ---------------------------------------------------------------------------
# Covariates (brief-v11-wpc-kit.md, "Covariates per cell").
# ---------------------------------------------------------------------------
start_freq_sampler() {
  local outfile=$1
  : >"$outfile"
  (
    while true; do
      "$PYTHON_BIN" - <<'PY'
import glob
vals = []
for path in glob.glob(
        "/sys/devices/system/cpu/cpu*/cpufreq/scaling_cur_freq"):
    try:
        vals.append(int(open(path, encoding="ascii").read().strip()))
    except (OSError, ValueError):
        pass
if vals:
    print(sum(vals) / len(vals))
PY
      sleep 5
    done
  ) >>"$outfile" 2>/dev/null &
  active_sampler_pid=$!
}

summarize_freq_sampler() {
  local infile=$1
  "$PYTHON_BIN" - "$infile" <<'PY'
import sys
values = []
try:
    with open(sys.argv[1], encoding="ascii") as stream:
        for line in stream:
            line = line.strip()
            if line:
                values.append(float(line))
except OSError:
    pass
if values:
    print(f"{sum(values) / len(values):.6g} {min(values):.6g} {max(values):.6g}")
else:
    print("  ")
PY
}

meminfo_cached_kb() {
  awk '/^Cached:/{print $2; exit}' /proc/meminfo 2>/dev/null || echo ""
}

numa_local_fraction() {
  local pid=$1
  local node_list=$2
  [[ -r "/proc/$pid/numa_maps" ]] || { echo ""; return; }
  "$PYTHON_BIN" - "/proc/$pid/numa_maps" "$node_list" <<'PY'
import re
import sys

path, node_list_text = sys.argv[1:]
local_nodes = {int(n) for n in node_list_text.split(",") if n != ""}
total = 0
local = 0
try:
    with open(path, encoding="ascii", errors="replace") as stream:
        text = stream.read()
except OSError:
    print("")
    raise SystemExit
for match in re.finditer(r"N(\d+)=(\d+)", text):
    node, pages = int(match.group(1)), int(match.group(2))
    total += pages
    if node in local_nodes:
        local += pages
if total == 0:
    print("")
else:
    print(f"{local / total:.6g}")
PY
}

# ---------------------------------------------------------------------------
# CSV output.
# ---------------------------------------------------------------------------
CSV_HEADER="run_index,seed,timestamp_utc,block,position,config,build,workload,repetition,shared_buffers,clients,duration_s,warmup_s,iterations,ns_per_iteration,tps,latency_avg_ms,measurement_samples,measurement_interval_s,pgbench_cpu_percent,pgbench_cpu_capacity_fraction,cpu_freq_khz_mean,cpu_freq_khz_min,cpu_freq_khz_max,numa_local_fraction,meminfo_cached_kb_before,meminfo_cached_kb_after,load_average_before,timing_clock_source,server_cpus,pgbench_cpus,server_log"
echo "$CSV_HEADER" >"$CSV"

csv_row() {
  local IFS=,
  printf '%s\n' "$*" >>"$CSV"
}

write_progress() {
  local state=$1 message=$2
  local now elapsed=0 eta=0
  now=$(date +%s)
  if (( MATRIX_START_EPOCH > 0 )); then
    elapsed=$((now - MATRIX_START_EPOCH))
    if (( CELLS_COMPLETED > 0 && CELLS_COMPLETED < TOTAL_CELLS )); then
      eta=$((elapsed * (TOTAL_CELLS - CELLS_COMPLETED) / CELLS_COMPLETED))
    fi
  fi
  "$PYTHON_BIN" - "$PROGRESS" "$state" "$message" "${CURRENT_CELL:-}" \
    "$CELLS_COMPLETED" "$TOTAL_CELLS" "$elapsed" "$eta" "$MODE" <<'PY'
import datetime
import json
import os
import sys
from pathlib import Path

(path_text, state, message, cell, completed, total, elapsed, eta,
 mode) = sys.argv[1:]
path = Path(path_text)
temporary = path.with_name(path.name + ".tmp")
data = {
    "updated_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "state": state,
    "message": message,
    "mode": mode,
    "current_cell": cell or None,
    "cells_completed": int(completed),
    "cells_total": int(total),
    "elapsed_seconds": int(elapsed),
    "eta_seconds": int(eta),
}
temporary.write_text(
    json.dumps(data, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
os.replace(temporary, path)
PY
}

# ---------------------------------------------------------------------------
# Early A/A gate (brief: "if the master vs master-aa paired 95% interval
# half-width exceeds 1.0% on W4, stop and report 'host unsuitable'").
# ---------------------------------------------------------------------------
check_early_aa() {
  local repetitions=$1
  "$PYTHON_BIN" - "$CSV" "$AA_EARLY" "$repetitions" <<'PY'
import csv
import datetime
import json
import statistics
import sys

csv_path, output_path, repetitions_text = sys.argv[1:]
repetitions = int(repetitions_text)
with open(csv_path, newline="", encoding="utf-8") as stream:
    rows = list(csv.DictReader(stream))
by_key = {
    (row["config"], row["workload"], int(row["repetition"])): row
    for row in rows
}
workload = "W4"
master = [
    float(by_key[("master", workload, repetition)]["tps"])
    for repetition in range(1, repetitions + 1)
]
aa = [
    float(by_key[("master-aa", workload, repetition)]["tps"])
    for repetition in range(1, repetitions + 1)
]
pairs = [
    (a - m) / ((a + m) / 2.0) * 100.0 for a, m in zip(aa, master)
]
t95_table = (
    None, 12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365,
    2.306, 2.262, 2.228, 2.201, 2.179, 2.160, 2.145, 2.131,
)
df = len(pairs) - 1
t_crit = t95_table[df] if 0 < df < len(t95_table) else 1.96
mean = statistics.mean(pairs)
half_width = (
    t_crit * statistics.stdev(pairs) / (len(pairs) ** 0.5)
    if len(pairs) > 1
    else 0.0
)
passed = half_width <= 1.0
record = {
    "checked_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "repetitions_complete": repetitions,
    "workload": workload,
    "mean_paired_percent": mean,
    "half_width_percent": half_width,
    "limit_percent": 1.0,
    "passed": passed,
}
with open(output_path, "a", encoding="utf-8") as stream:
    json.dump(record, stream, sort_keys=True)
    stream.write("\n")
if not passed:
    raise SystemExit(
        f"host unsuitable: master vs master-aa paired 95% half-width on "
        f"{workload} is {half_width:.3f}% after repetition {repetitions}, "
        "over the 1.0% limit"
    )
PY
}

# ---------------------------------------------------------------------------
# pgbench measurement: continuous WARMUP_SECONDS + DURATION run, only
# post-warmup progress samples count. Same method as v10, extended with a
# frequency-covariate sampler running for the whole window.
# ---------------------------------------------------------------------------
run_pgbench_measured() {
  local prefix=$1 clients=$2 threads=$3 seed=$4 config=$5 workload=$6
  local run_index=$7
  shift 7
  local -a extra_args=("$@")
  local logfile="$LOG_DIR/pgbench-$run_index.log"
  local appname="wet-v11-$run_index"
  local check_recording=no
  local expect_trace=false
  capture_should_be_active "$config" && check_recording=yes
  capture_should_trace "$config" && expect_trace=true
  local total=$((WARMUP_SECONDS + DURATION))
  run_pgbench_from_prefix "$prefix" "$appname" \
    taskset -c "$PGBENCH_CPUS" "$prefix/bin/pgbench" -n \
    -c "$clients" -j "$threads" -T "$total" -P 1 \
    --random-seed="$seed" "${extra_args[@]}" postgres \
    >"$logfile" 2>&1 &
  local pgbench_pid=$!
  active_pgbench_pid=$pgbench_pid

  local snapshot_delay=$((WARMUP_SECONDS / 3))
  (( snapshot_delay > 0 )) || snapshot_delay=1
  local proof_start=$SECONDS
  sleep "$snapshot_delay"
  if ! kill -0 "$pgbench_pid" 2>/dev/null; then
    wait "$pgbench_pid" || true
    active_pgbench_pid=""
    die "pgbench exited before the warmup proof point; see $logfile"
  fi
  local pgbench_comm pgbench_cpu
  if [[ -z "${SELFTEST_FAKE_PREFIX:-}" ]]; then
    pgbench_comm=$(ps -o comm= -p "$pgbench_pid" | awk '{print $1}')
    [[ "${pgbench_comm##*/}" == pgbench ]] ||
      die "background pid $pgbench_pid is '$pgbench_comm', not pgbench"
  fi
  assert_process_affinity "$pgbench_pid" "$PGBENCH_CPUS" "pgbench"
  pgbench_cpu=$(ps -o pcpu= -p "$pgbench_pid" | awk '{print $1}')
  [[ "$pgbench_cpu" =~ ^[0-9]+([.][0-9]+)?$ ]] ||
    die "could not read pgbench CPU utilization for pid $pgbench_pid"
  local pgbench_capacity
  pgbench_capacity=$("$PYTHON_BIN" - "$pgbench_cpu" "$threads" <<'PY'
import sys
value, threads = float(sys.argv[1]), int(sys.argv[2])
print(f"{value / (threads * 100.0):.12g}")
PY
  )
  "$PYTHON_BIN" - "$pgbench_capacity" <<'PY' ||
import sys

capacity = float(sys.argv[1])
if capacity >= 0.90:
    raise SystemExit(
        f"pgbench used {capacity:.1%} of its thread capacity; "
        "the client driver is saturated"
    )
PY
    die "pgbench client saturation invalidates $CURRENT_CELL; see $logfile"
  "$PYTHON_BIN" - "$CLIENT_LOAD_DIR/cell-$run_index.json" \
    "$pgbench_cpu" "$pgbench_capacity" "$threads" "$pgbench_pid" <<'PY'
import json
import sys
out, cpu, capacity, threads, pid = sys.argv[1:]
with open(out, "x", encoding="utf-8") as f:
    json.dump({
        "pid": int(pid),
        "pgbench_cpu_percent": float(cpu),
        "thread_count": int(threads),
        "thread_capacity_fraction": float(capacity),
    }, f, indent=2, sort_keys=True)
    f.write("\n")
PY
  PGBENCH_CPU_RESULT=$pgbench_cpu
  PGBENCH_CAPACITY_RESULT=$pgbench_capacity

  if [[ "$check_recording" == yes ]]; then
      local proof_file="$RECORDING_DIR/cell-$run_index.csv"
      local proof client_count clients_recording timing_calls trace_records
      proof=$(psql_for "$prefix" -qAt -F ',' \
        -v appname="$appname" -v expect_trace="$expect_trace" \
        ${SELFTEST_FAKE_PREFIX:+-v selftest_clients="$clients"} \
        -f "$WORKLOAD_DIR/recording-proof.sql") ||
        die "could not prove client recording during warmup; see $logfile"
      printf '%s\n' \
        "client_count,clients_recording,timing_calls,representative_trace_records" \
        "$proof" >"$proof_file"
      IFS=',' read -r client_count clients_recording timing_calls trace_records \
        <<<"$proof"
      for value in "$client_count" "$clients_recording" "$timing_calls" \
                   "$trace_records"; do
        [[ "$value" =~ ^[0-9]+$ ]] ||
          die "malformed mode proof '$proof' for $CURRENT_CELL"
      done
      (( client_count == clients )) ||
        die "mode proof saw $client_count pgbench clients, expected $clients"
      (( clients_recording == clients )) ||
        die "only $clients_recording/$clients pgbench clients recorded timing data (pg_stat_wait_event_timing)"
      (( timing_calls > 0 )) ||
        die "the pgbench client backends recorded zero timing calls"
      if [[ "$expect_trace" == true ]]; then
        (( trace_records > 0 )) ||
          die "trace capture has timing rows but pg_get_wait_event_trace() returned nothing for a pgbench PID"
      else
        (( trace_records == 0 )) ||
          die "non-trace mode proof unexpectedly returned trace records"
      fi

      if [[ "$workload" == W3 ]]; then
        local w3_raw="$W3_QUAL_DIR/cell-$run_index.tsv"
        local w3_summary="$W3_QUAL_DIR/cell-$run_index.json"
        local w3_elapsed
        psql_for "$prefix" -qAt -F $'\t' -v appname="$appname" \
          -f "$WORKLOAD_DIR/w3-qualification.sql" >"$w3_raw" ||
          die "could not capture W3 qualification evidence"
        w3_elapsed=$((SECONDS - proof_start))
        if (( w3_elapsed > 0 && w3_elapsed < WARMUP_SECONDS )); then
          : # normal case
        elif [[ "$REHEARSAL" -eq 1 ]]; then
          rehearsal_note "W3 qualification took ${w3_elapsed}s against a ${WARMUP_SECONDS}s warmup window for $CURRENT_CELL -- this VM/emulation may be slower than the intended target; measure real proof durations under $RECORDING_DIR before trusting a shorter warmup there"
          (( w3_elapsed > 0 )) || w3_elapsed=1
        else
          die "W3 qualification consumed the warmup window"
        fi
        if ! "$PYTHON_BIN" "$SCRIPT_DIR/w3_qualification.py" \
          "$w3_raw" "$w3_summary" "$w3_elapsed"; then
          if [[ "$REHEARSAL" -eq 1 ]]; then
            # REPORTED, not enforced, under BENCHMARK_REHEARSAL=1: real
            # mode is unchanged and still fail-closed above (w3_qualification.py
            # itself is never patched to be lenient; only this call site's
            # reaction to its exit status differs). A rehearsal VM/emulation
            # can genuinely fail to reach the contention rate this gate
            # requires without that saying anything about the patch under
            # test -- and deferred accounting itself (this v11 series) can
            # legitimately LOWER lwlock_calls_per_second on real hardware
            # too, since shorter critical-section hold times mean more
            # acquisitions succeed uncontended and so never wait at all;
            # see DECISION-deferred-accounting.md. $w3_summary was still
            # written (w3_qualification.py writes it before checking
            # "passed"), so every measured value is logged here against
            # its threshold rather than only the boolean verdict.
            rehearsal_note "W3 qualification failed for $CURRENT_CELL; reporting every measured value against its threshold below (see $w3_summary for the full record) instead of failing the rehearsal:"
            while IFS= read -r qual_line; do
              rehearsal_note "  $qual_line"
            done < <("$PYTHON_BIN" -c "
import json, sys
d = json.load(open(sys.argv[1]))
th = d['thresholds']
rows = [
    ('lwlock_calls_per_second', 'min_lwlock_calls_per_second', '>='),
    ('lwlock_fraction', 'min_lwlock_fraction', '>='),
    ('procarray_fraction_of_lwlock', 'min_procarray_fraction_of_lwlock', '>='),
    ('io_fraction', 'max_io_fraction', '<='),
    ('histogram_coverage', 'min_histogram_coverage', '>='),
    ('p50_us_upper', 'max_p50_us_upper', '<='),
    ('p95_us_upper', 'max_p95_us_upper', '<='),
]
for value_key, threshold_key, op in rows:
    value = d[value_key]
    threshold = th[threshold_key]
    ok = (value >= threshold) if op == '>=' else (value <= threshold)
    print(f'{value_key}: {value} {op} {threshold} [{\"OK\" if ok else \"MISS\"}]')
" "$w3_summary")
          else
            die "W3 did not qualify as short ProcArray LWLock contention; see $w3_summary"
          fi
        fi
      fi

    # Elapsed real seconds from pgbench's own launch (proof_start, before
    # even the snapshot_delay sleep) to here -- i.e. exactly the quantity
    # the guard below compares against WARMUP_SECONDS. Recorded per cell
    # (mode_proof_duration_s in $RECORDING_DIR) regardless of mode: useful
    # on a real host too, to see headroom against WARMUP_SECONDS before a
    # slower one ever gets close to it.
    local proof_elapsed=$((SECONDS - proof_start))
    echo "$proof_elapsed" >"$proof_file.duration_s"
    if (( proof_elapsed >= WARMUP_SECONDS )); then
      if [[ "$REHEARSAL" -eq 1 ]]; then
        # The guard itself is unchanged and still fail-closed outside
        # BENCHMARK_REHEARSAL=1 (a real host passing this means the
        # warmup budget has real headroom); a VM/emulated rehearsal can
        # legitimately run the same proof query slower without that
        # meaning anything about the patch, so this is a REHEARSAL NOTE
        # instead of a die() -- benchmark_protocol.py already widens
        # WARMUP_SECONDS under BENCHMARK_REHEARSAL=1 for exactly this;
        # reaching this branch means even that wider budget wasn't enough
        # on this host, which is itself useful to know before trusting a
        # real run's tighter real-mode budget.
        rehearsal_note "mode proof took ${proof_elapsed}s against a ${WARMUP_SECONDS}s warmup window for $CURRENT_CELL (config=$config workload=$workload) -- exceeds even the widened rehearsal budget; this VM/emulation is slower than the intended target for this configuration"
      else
        die "mode proof consumed the entire warmup window and could contaminate the measured samples"
      fi
    fi
  fi

  wait "$pgbench_pid" || {
    active_pgbench_pid=""
    die "pgbench failed; see $logfile"
  }
  active_pgbench_pid=""

  local parsed
  parsed=$("$PYTHON_BIN" - "$logfile" "$WARMUP_SECONDS" "$DURATION" <<'PY'
import re
import sys
path, warmup_s, duration_s = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
text = open(path, encoding="utf-8", errors="replace").read()
matches = re.findall(
    r"^progress: ([0-9.]+) s, ([0-9.]+) tps, "
    r"lat ([0-9.]+) ms stddev [0-9.]+, ([0-9]+) failed",
    text, re.M)
if not matches:
    raise SystemExit("no parseable progress lines in pgbench output")
final_failures = re.findall(
    r"^number of failed transactions: ([0-9]+) ", text, re.M
)
if len(final_failures) != 1:
    raise SystemExit("pgbench output lacks one final failed-transaction count")
if int(final_failures[0]) != 0:
    raise SystemExit(
        f"pgbench reported {final_failures[0]} failed transactions overall"
    )
progress = [
    (float(elapsed), float(tps), float(latency), int(failures))
    for elapsed, tps, latency, failures in matches
]
selected = []
previous = 0.0
for elapsed, tps, latency, failures in progress:
    interval = elapsed - previous
    if interval <= 0:
        raise SystemExit("non-increasing pgbench progress timestamps")
    if previous >= warmup_s:
        selected.append((interval, tps, latency, failures))
    previous = elapsed
if not selected:
    print("ERROR: no post-warmup progress lines in pgbench output",
          file=sys.stderr)
    raise SystemExit(1)
measured_interval = sum(item[0] for item in selected)
if measured_interval < duration_s - 2 or measured_interval > duration_s + 1:
    raise SystemExit(
        f"post-warmup progress covers {measured_interval:.3f}s, "
        f"expected approximately {duration_s}s")
failures = sum(item[3] for item in selected)
if failures:
    raise SystemExit(f"pgbench reported {failures} failed transactions")
transactions = sum(interval * tps for interval, tps, _, _ in selected)
if transactions <= 0:
    raise SystemExit("post-warmup progress reports no transactions")
tps = transactions / measured_interval
latency_ms = sum(
    interval * tps_value * latency
    for interval, tps_value, latency, _ in selected
) / transactions
print(f"tps={tps:.12g}")
print(f"latency_ms={latency_ms:.12g}")
print(f"samples={len(selected)}")
print(f"interval_s={measured_interval:.12g}")
PY
  ) || die "could not parse pgbench output; see $logfile"

  TPS_RESULT=$(printf '%s\n' "$parsed" | sed -n 's/^tps=//p')
  LATENCY_RESULT=$(printf '%s\n' "$parsed" | sed -n 's/^latency_ms=//p')
  SAMPLES_RESULT=$(printf '%s\n' "$parsed" | sed -n 's/^samples=//p')
  INTERVAL_RESULT=$(printf '%s\n' "$parsed" | sed -n 's/^interval_s=//p')
}

# ---------------------------------------------------------------------------
# One measured cell.
# ---------------------------------------------------------------------------
run_cell() {
  local run_index=$1 config=$2 workload=$3 repetition=$4 block=$5 position=$6
  CURRENT_CELL="run_index=$run_index config=$config workload=$workload repetition=$repetition"

  local prefix build shared_buffers datadir logfile
  prefix=$(config_build_prefix "$config")
  build=$(config_build_name "$config")
  shared_buffers=$(workload_shared_buffers "$workload")
  datadir="$DATA_ROOT/cell-$run_index"
  logfile="$LOG_DIR/server-$run_index.log"
  [[ ! -e "$datadir" ]] || die "data directory already exists: $datadir"

  local ts_start
  ts_start=$(date -u +%Y-%m-%dT%H:%M:%SZ)

  init_cluster "$PREFIX_BASELINE_A" "$datadir" "$shared_buffers" "$logfile"
  if [[ "$workload" == W4 || "$workload" == W5 || "$workload" == W6c ]]; then
    local setup_log="$LOG_DIR/server-setup-$run_index.log"
    start_server "$PREFIX_BASELINE_A" "$datadir" master "$setup_log"
    PGHOST="$SOCKET_DIR" PGPORT="$PORT" PGUSER="$DBUSER" \
      run_from_prefix "$PREFIX_BASELINE_A" \
      "${NUMA_WRAP[@]}" taskset -c "$PGBENCH_CPUS" \
      "$PREFIX_BASELINE_A/bin/pgbench" -i -q --unlogged-tables \
      -s "$PGBENCH_SCALE" postgres \
      >"$LOG_DIR/pgbench-init-$run_index.log" 2>&1 ||
      die "pgbench -i failed for $workload; see $LOG_DIR/pgbench-init-$run_index.log"
    psql_for "$PREFIX_BASELINE_A" -qAtc "CHECKPOINT" >/dev/null ||
      die "neutral setup checkpoint failed for $workload"
    stop_server_checked "neutral $workload dataset setup"
  fi

  start_server "$prefix" "$datadir" "$config" "$logfile"
  assert_mode_proof "$prefix" "$config"
  if [[ "$workload" == W1 || "$workload" == W3 ]]; then
    psql_for "$prefix" -qAtc \
      "CREATE EXTENSION test_wait_primitive" >/dev/null ||
      die "CREATE EXTENSION test_wait_primitive failed"
  fi

  local iterations="" ns_per_iteration="" tps="" latency_ms="" clients=""
  local samples="" measurement_interval=""
  local pgbench_cpu="" pgbench_capacity=""
  local duration_field=$DURATION warmup_field=$WARMUP_SECONDS
  local freq_stats="  " numa_fraction="" cached_before="" cached_after=""
  local load_before="" clock_source=""

  local check_recording=no
  capture_should_be_active "$config" && check_recording=yes
  local expect_trace=false
  capture_should_trace "$config" && expect_trace=true

  psql_for "$prefix" -qAtc "CHECKPOINT" >/dev/null ||
    die "pre-measurement checkpoint failed for $workload"
  if [[ "$workload" == W4 || "$workload" == W5 ]]; then
    psql_for "$prefix" -qAtc \
      "SELECT count(*) FROM pgbench_accounts" >/dev/null ||
      die "warm-cache scan of pgbench_accounts failed"
  fi
  (( QUIESCENCE_SECONDS == 0 )) || sleep "$QUIESCENCE_SECONDS"

  clock_source=$(psql_for "$prefix" -qAtc "SHOW timing_clock_source" 2>/dev/null || echo "")
  load_before=$(awk '{print $1}' /proc/loadavg 2>/dev/null || echo "")
  cached_before=$(meminfo_cached_kb)
  local freq_log="$LOG_DIR/freq-$run_index.log"
  start_freq_sampler "$freq_log"

  case "$workload" in
    W1)
      local warmup_iter=$((W1_ITERATIONS / 100))
      (( warmup_iter > 0 )) || warmup_iter=1
      for fn in "${W1_FUNCTIONS[@]}"; do
        psql_for "$prefix" -qAtc "SELECT $fn($warmup_iter)" >/dev/null ||
          die "W1 warmup call to $fn failed"
      done
      # All five functions are measured in one backend session so the
      # single ns_per_iteration figure for this cell is the average cost
      # per iteration across all five primitives (brief: "all five
      # functions"). Per-function detail is kept for diagnostics.
      local select_list="" total_iterations=0
      for fn in "${W1_FUNCTIONS[@]}"; do
        select_list+="${select_list:+, }$fn($W1_ITERATIONS) AS \"$fn\""
        total_iterations=$((total_iterations + W1_ITERATIONS))
      done
      local recorded_expr="0" trace_expr="0"
      if [[ "$check_recording" == yes ]]; then
        recorded_expr="(SELECT coalesce(sum(calls), 0) FROM pg_stat_get_wait_event_timing(pg_backend_pid()))"
        [[ "$expect_trace" != true ]] ||
          trace_expr="(SELECT count(*) FROM pg_backend_wait_event_trace WHERE wait_event_type <> 'Query')"
      fi
      local combined
      combined=$(psql_for "$prefix" -qAt -F ',' -c \
        "SELECT $select_list, $recorded_expr, $trace_expr") ||
        die "W1 measured call failed"
      IFS=',' read -r -a w1_values <<<"$combined"
      local n_fns=${#W1_FUNCTIONS[@]}
      local recorded=${w1_values[$n_fns]}
      local trace_records=${w1_values[$((n_fns + 1))]}
      "$PYTHON_BIN" - "$W1_DETAIL_DIR/cell-$run_index.json" \
        "${W1_FUNCTIONS[@]}" -- "${w1_values[@]:0:$n_fns}" <<'PY'
import json
import sys
args = sys.argv[1:]
out = args[0]
rest = args[1:]
sep = rest.index("--")
names, values = rest[:sep], rest[sep + 1:]
data = {name: float(value) for name, value in zip(names, values)}
with open(out, "x", encoding="utf-8") as f:
    json.dump(data, f, indent=2, sort_keys=True)
    f.write("\n")
PY
      ns_per_iteration=$("$PYTHON_BIN" - "${w1_values[@]:0:$n_fns}" <<'PY'
import sys
values = [float(v) for v in sys.argv[1:]]
print(f"{sum(values) / len(values):.12g}")
PY
)
      if [[ "$check_recording" == yes ]]; then
        if [[ ! "$recorded" =~ ^[0-9]+$ ]] || (( recorded == 0 )); then
          die "pg_stat_get_wait_event_timing recorded zero calls for this backend after the W1 measured call -- capture is not actually collecting"
        fi
        [[ "$trace_records" =~ ^[0-9]+$ ]] ||
          die "W1 returned a malformed trace proof: '$trace_records'"
        if [[ "$expect_trace" == true ]] && (( trace_records == 0 )); then
          die "W1 timing counters recorded, but the backend trace ring is empty"
        fi
        printf '%s\n' \
          "timing_calls,trace_records" "$recorded,$trace_records" \
          >"$RECORDING_DIR/cell-$run_index.csv"
      fi
      iterations=$total_iterations
      duration_field=""
      warmup_field=""
      ;;
    W3)
      clients=8
      run_pgbench_measured "$prefix" 8 8 "$repetition" "$config" \
        "$workload" "$run_index" \
        -f "$WORKLOAD_DIR/w3-short-lwlock.sql"
      tps=$TPS_RESULT; latency_ms=$LATENCY_RESULT; samples=$SAMPLES_RESULT
      measurement_interval=$INTERVAL_RESULT
      pgbench_cpu=$PGBENCH_CPU_RESULT; pgbench_capacity=$PGBENCH_CAPACITY_RESULT
      ;;
    W4)
      clients=16
      run_pgbench_measured "$prefix" 16 8 "$repetition" "$config" \
        "$workload" "$run_index" -S
      tps=$TPS_RESULT; latency_ms=$LATENCY_RESULT; samples=$SAMPLES_RESULT
      measurement_interval=$INTERVAL_RESULT
      pgbench_cpu=$PGBENCH_CPU_RESULT; pgbench_capacity=$PGBENCH_CAPACITY_RESULT
      ;;
    W5)
      clients=16
      # Default pgbench script (TPC-B-like, read-write): no -S.
      run_pgbench_measured "$prefix" 16 8 "$repetition" "$config" \
        "$workload" "$run_index"
      tps=$TPS_RESULT; latency_ms=$LATENCY_RESULT; samples=$SAMPLES_RESULT
      measurement_interval=$INTERVAL_RESULT
      pgbench_cpu=$PGBENCH_CPU_RESULT; pgbench_capacity=$PGBENCH_CAPACITY_RESULT
      ;;
    W6c)
      clients=32
      run_pgbench_measured "$prefix" 32 8 "$repetition" "$config" \
        "$workload" "$run_index" -S
      tps=$TPS_RESULT; latency_ms=$LATENCY_RESULT; samples=$SAMPLES_RESULT
      measurement_interval=$INTERVAL_RESULT
      pgbench_cpu=$PGBENCH_CPU_RESULT; pgbench_capacity=$PGBENCH_CAPACITY_RESULT
      ;;
    *)
      die "unknown workload: $workload"
      ;;
  esac

  stop_freq_sampler
  freq_stats=$(summarize_freq_sampler "$freq_log")
  cached_after=$(meminfo_cached_kb)
  # Sample at the end of the measured window (here, right after the
  # workload's measured pgbench/W1 run and before stop_server_checked
  # tears the cluster down) -- the fraction of the postmaster's resident
  # pages that sit on the NUMA node(s) SERVER_CPUS lives on. This is only
  # a meaningful signal because 00-check-host.sh/cpu_affinity.py now
  # refuse (no override) any SERVER_CPUS that spans more than one NUMA
  # node: with that guard, server_numa_nodes below is always exactly one
  # node. Before that guard existed, a cross-socket SERVER_CPUS (e.g. the
  # rehearsal incident's "1-31" on a 2-node, interleaved-numbering host)
  # made server_numa_nodes = every node on the machine, so "local" was
  # vacuously the whole host and this always read 1.0 regardless of where
  # memory actually landed -- see reports/wpf-report.md, Addendum 6.
  local numa_node_list
  numa_node_list=$("$PYTHON_BIN" "$AFFINITY_HELPER" collect 2>/dev/null |
    "$PYTHON_BIN" -c "import json,sys; print(','.join(str(n) for n in json.load(sys.stdin)['server_numa_nodes']))" 2>/dev/null || echo "")
  numa_fraction=$(numa_local_fraction "$ACTIVE_POSTMASTER_PID" "$numa_node_list")

  stop_server_checked "$CURRENT_CELL"
  rm -rf -- "$datadir"

  local freq_mean freq_min freq_max
  read -r freq_mean freq_min freq_max <<<"$freq_stats"

  csv_row "$run_index" "$SEED" "$ts_start" "$block" "$position" "$config" \
    "$build" "$workload" "$repetition" "$shared_buffers" "$clients" \
    "$duration_field" "$warmup_field" "$iterations" "$ns_per_iteration" \
    "$tps" "$latency_ms" "$samples" "$measurement_interval" \
    "$pgbench_cpu" "$pgbench_capacity" \
    "$freq_mean" "$freq_min" "$freq_max" "$numa_fraction" \
    "$cached_before" "$cached_after" "$load_before" "$clock_source" \
    "$SERVER_CPUS" "$PGBENCH_CPUS" "logs/server-$run_index.log"

  CURRENT_CELL=""
}

# ---------------------------------------------------------------------------
# pg_test_timing per installation, once, in preflight (brief: "one run of
# pg_test_timing per installation in preflight").
# ---------------------------------------------------------------------------
for pair in "baseline-a=$PREFIX_BASELINE_A" "baseline-b=$PREFIX_BASELINE_B" \
            "patched=$PREFIX_PATCHED" "control=$PREFIX_CONTROL"; do
  name=${pair%%=*}
  prefix=${pair#*=}
  run_from_prefix "$prefix" "$prefix/bin/pg_test_timing" -d 2 \
    >"$TIMING_DIR/$name.txt" 2>&1 ||
    die "pg_test_timing failed for $name"
done
log "pg_test_timing captured for all four installations: $TIMING_DIR"

# ---------------------------------------------------------------------------
# Schedule: one 7x7 Latin-square configuration order per workload per
# repetition (latin_square.py), workload-block order shuffled per
# repetition, exactly as v10 shuffled workload order.
# ---------------------------------------------------------------------------
SEED=${RUN_SEED:-$(od -An -N8 -tu8 /dev/urandom | tr -d ' \n')}
echo "$SEED" >"$SEED_FILE"
log "Random seed for this run's schedule: $SEED (saved to $SEED_FILE)"

"$PYTHON_BIN" - "$SCHEDULE_CSV" "$SEED" "$RUNS" "$MODE" "${#CONFIGS[@]}" \
  "${#WORKLOADS[@]}" "${CONFIGS[@]}" "${WORKLOADS[@]}" <<'PY'
import random
import sys

sys.path.insert(0, ".")
from latin_square import build_schedule, verify_schedule

args = sys.argv[1:]
path, seed, runs, mode = args[0], int(args[1]), int(args[2]), args[3]
nconfigs, nworkloads = int(args[4]), int(args[5])
rest = args[6:]
configs = rest[:nconfigs]
workloads = rest[nconfigs:nconfigs + nworkloads]

rng = random.Random(seed)
per_workload_schedule = {}
for workload in workloads:
    if mode == "full":
        schedule = build_schedule(rng, configs, runs)
        verify_schedule(schedule, configs, runs // nconfigs)
    else:
        schedule = []
        for _ in range(runs):
            order = list(configs)
            rng.shuffle(order)
            schedule.append(order)
    per_workload_schedule[workload] = schedule

cells = []
for repetition in range(1, runs + 1):
    workload_order = list(workloads)
    rng.shuffle(workload_order)
    for workload in workload_order:
        order = per_workload_schedule[workload][repetition - 1]
        block = f"{workload}-{repetition:02d}"
        for position, config in enumerate(order, 1):
            cells.append((config, workload, repetition, block, position))

with open(path, "x", encoding="utf-8") as f:
    f.write("run_index,config,workload,repetition,block,position\n")
    for i, (config, workload, repetition, block, position) in enumerate(cells, 1):
        f.write(f"{i},{config},{workload},{repetition},{block},{position}\n")
PY

"$PYTHON_BIN" - "$PROTOCOL" "$SEED" "$RUNS" "$DURATION" "$WARMUP_SECONDS" \
  "$PGBENCH_SCALE" "$W1_ITERATIONS" "$MODULE_NAME" "$GUC_CAPTURE" \
  "$GUC_MAX_TRANCHES" "$GUC_TRACE_RING_SIZE" \
  "$QUIESCENCE_SECONDS" "$TOTAL_CELLS" "$SCRIPT_DIR" \
  "$MODE" "$BUILD_MANIFEST_SHA256" "$SERVER_CPUS" "$PGBENCH_CPUS" \
  "$AFFINITY_PROOF_JSON" \
  "${CONFIGS[@]}" -- "${WORKLOADS[@]}" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

args = sys.argv[1:]
(out, seed, runs, duration, warmup, scale, w1_iterations, module,
 guc_capture, guc_max_tranches, guc_trace_ring_size, quiescence,
 total_cells, script_dir, mode, build_manifest_sha256, server_cpus,
 pgbench_cpus, affinity_proof_json) = args[:19]
rest = args[19:]
sep = rest.index("--")
configs, workloads = rest[:sep], rest[sep + 1:]

root = Path(script_dir)
sys.path.insert(0, script_dir)
from benchmark_protocol import (
    BOUND_KIT_FILES,
    EARLY_AA_GATE_MAX_HALF_WIDTH_PERCENT,
    EARLY_AA_GATE_REPETITIONS,
    EARLY_AA_GATE_WORKLOAD,
    W3_PROTOCOL,
)

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

data = {
    "schema_version": 11,
    "benchmark_series": "wet-v11",
    "mode": mode,
    "seed": int(seed),
    "runs_per_cell": int(runs),
    "expected_cells": int(total_cells),
    "duration_seconds": int(duration),
    "warmup_seconds": int(warmup),
    "quiescence_seconds": int(quiescence),
    "pgbench_scale": int(scale),
    "w1_iterations": int(w1_iterations),
    "module_name": module,
    "guc_capture": guc_capture,
    "guc_max_tranches": guc_max_tranches,
    "guc_trace_ring_size": guc_trace_ring_size,
    "server_cpus": server_cpus,
    "pgbench_cpus": pgbench_cpus,
    "cpu_affinity_protocol": json.loads(affinity_proof_json),
    "build_manifest_sha256": build_manifest_sha256,
    "host_check_sha256": {
        name: digest(root / name)
        for name in ("host-check.txt", "host-check.json")
    },
    "configs": configs,
    "workloads": workloads,
    "schedule": (
        "one 7x7 Latin-square configuration order per workload per "
        "repetition (every configuration occupies every position at "
        "least twice over 16 repetitions); workload-block order shuffled"
    ),
    "outlier_policy": "retain every successful cell; no post-hoc exclusions",
    "analysis": {
        "script": "analyze-results.py",
        "primary_reference": "master",
        "pairing_key": "workload + repetition",
        "estimators": ["paired Student t 95% interval",
                       "Wilcoxon signed-rank Hodges-Lehmann 95% interval"],
        "w1_equivalence_margin_ns": 2.0,
        "pgbench_equivalence_margin_percent": 2.0,
        "pgbench_metric": "log(tps)",
        "classification_rule": (
            "equivalent iff both intervals lie inside the margin; "
            "faster/slower iff both intervals exclude zero on the same "
            "side; otherwise unresolved"
        ),
        "early_aa_gate": {
            "check_after_repetitions": list(EARLY_AA_GATE_REPETITIONS),
            "workload": EARLY_AA_GATE_WORKLOAD,
            "max_half_width_percent": EARLY_AA_GATE_MAX_HALF_WIDTH_PERCENT,
        },
    },
    "max_pgbench_thread_capacity_fraction": 0.90,
    "w3_qualification": W3_PROTOCOL,
    "mode_proof": (
        "run once per cell before the timed window, from a proof session "
        "on the same server; failure aborts the run"
    ),
    "setup": {
        "initdb_build": "baseline-a for every cell",
        "dataset_initialization": (
            "baseline-a with module absent, followed by CHECKPOINT and "
            "confirmed normal shutdown"
        ),
        "pre_measurement": (
            "CREATE EXTENSION as required, CHECKPOINT, W4/W5 full account "
            "scan only, then fixed quiescence"
        ),
        "shutdown": "pg_ctl fast, wait, required success before data removal",
    },
    "bound_file_sha256": {
        name: digest(root / name) for name in BOUND_KIT_FILES
    },
}
with open(out, "x", encoding="utf-8") as f:
    json.dump(data, f, indent=2, sort_keys=True)
    f.write("\n")
PY

log "Schedule written: $SCHEDULE_CSV ($TOTAL_CELLS cells)"
log "Protocol recorded: $PROTOCOL"
log "Mode: $MODE"

mapfile -t SCHEDULE_LINES < <(tail -n +2 "$SCHEDULE_CSV")
[[ ${#SCHEDULE_LINES[@]} -eq "$TOTAL_CELLS" ]] ||
  die "schedule has ${#SCHEDULE_LINES[@]} rows, expected $TOTAL_CELLS"

START_EPOCH=$(date +%s)
MATRIX_START_EPOCH=$START_EPOCH
write_progress running "matrix initialized"
n=0
for line in "${SCHEDULE_LINES[@]}"; do
  IFS=',' read -r run_index config workload repetition block position <<<"$line"
  n=$((n + 1))
  cell_start=$(date +%s)
  CURRENT_CELL="run_index=$run_index config=$config workload=$workload repetition=$repetition"

  log "[$n/$TOTAL_CELLS] config=$config workload=$workload repetition=$repetition position=$position"
  write_progress running "starting cell $n/$TOTAL_CELLS"
  start_cell_watchdog "$CURRENT_CELL"
  run_cell "$run_index" "$config" "$workload" "$repetition" "$block" "$position"
  stop_cell_watchdog
  CELLS_COMPLETED=$n

  cell_elapsed=$(( $(date +%s) - cell_start ))
  total_elapsed=$(( $(date +%s) - START_EPOCH ))
  if (( n > 0 )); then
    remaining=$(( total_elapsed * (TOTAL_CELLS - n) / n ))
    log "  done in ${cell_elapsed}s | elapsed $((total_elapsed / 60))m | ETA $((remaining / 60))m"
  fi
  write_progress running "completed cell $n/$TOTAL_CELLS"

  if [[ "$MODE" == full && $((n % CELLS_PER_REPETITION)) -eq 0 ]]; then
    completed_repetition=$((n / CELLS_PER_REPETITION))
    if in_list "$completed_repetition" "${EARLY_AA_GATE_REPETITIONS[@]}"; then
      log "Running early A/A gate after repetition $completed_repetition"
      check_early_aa "$completed_repetition" ||
        die "early A/A gate failed; host unsuitable (raw data retained)"
    fi
  fi
done

write_progress complete "all matrix cells completed"
"$PYTHON_BIN" - "$RESULTS/matrix-complete.json" "$CSV" "$SCHEDULE_CSV" \
  "$PROTOCOL" "$PROGRESS" "$AA_EARLY" "$TOTAL_CELLS" <<'PY'
import csv
import datetime
import hashlib
import json
import sys
from pathlib import Path

(
    out, results_path, schedule_path, protocol_path,
    progress_path, aa_early_path, expected,
) = sys.argv[1:]
expected = int(expected)
with open(results_path, newline="", encoding="utf-8") as f:
    rows = list(csv.DictReader(f))
if len(rows) != expected:
    raise SystemExit(
        f"cannot mark matrix complete: {len(rows)} rows, expected {expected}")

def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

data = {
    "completed_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "rows": len(rows),
    "sha256": {
        "results.csv": digest(results_path),
        "schedule.csv": digest(schedule_path),
        "protocol.json": digest(protocol_path),
        "progress.json": digest(progress_path),
        "aa-early.jsonl": digest(aa_early_path),
    },
}
with open(out, "x", encoding="utf-8") as f:
    json.dump(data, f, indent=2, sort_keys=True)
    f.write("\n")
PY

log ""
log "Matrix complete: $TOTAL_CELLS/$TOTAL_CELLS cells."
log "Raw results:   $CSV"
log "Server logs:   $LOG_DIR"
log "Schedule:      $SCHEDULE_CSV"
log "Seed:          $SEED"
log ""
log "Next step: ./03-collect.sh"
