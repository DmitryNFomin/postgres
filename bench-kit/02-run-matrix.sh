#!/usr/bin/env bash
# 02-run-matrix.sh
#
# Runs the full measurement matrix: 6 configurations x 4 workloads x 12
# repetitions = 288 runs.  Each repetition is one complete randomized block
# containing every configuration/workload cell.  Every cell uses a fresh
# PostgreSQL data directory.
#
# The six configurations (see the README for the long version):
#   1  master       baseline build, nothing loaded
#   2  master-aa    independent rebuild of the baseline (noise/control arm)
#   3  hook-null    patched build, collector module NOT preloaded
#   4  module-off   patched build, collector loaded, capture switched off
#   5  stats        patched build, collector loaded, capture = 'stats'
#   6  trace        patched build, collector loaded, capture = 'trace'
#
# The four workloads:
#   W1   isolated wait-primitive microbenchmark, 10^8 iterations
#   W3   short LWLock contention, 8 pgbench clients
#   W4   pgbench read-only (-S), 16 clients, 4 GB shared_buffers
#   W6c  pgbench read-only (-S), 32 clients, 32 MB shared_buffers
#
# Expect this step to take roughly 4-5.5 hours. It is unattended and safe
# to run under tmux/screen -- just start it and check back later.
set -Eeuo pipefail
export LC_ALL=C

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WORK="$SCRIPT_DIR/work"
MODE=${BENCHMARK_MODE:-full}
case "$MODE" in
  full)
    RESULTS="$SCRIPT_DIR/results"
    RUNS=12
    DURATION=30
    WARMUP_SECONDS=10
    PGBENCH_SCALE=100
    W1_ITERATIONS=100000000
    QUIESCENCE_SECONDS=5
    ;;
  smoke)
    RESULTS="$SCRIPT_DIR/smoke-results"
    RUNS=1
    DURATION=6
    WARMUP_SECONDS=3
    PGBENCH_SCALE=1
    W1_ITERATIONS=100000
    QUIESCENCE_SECONDS=0
    ;;
  *)
    echo "02-run-matrix.sh: ERROR: BENCHMARK_MODE must be full or smoke" >&2
    exit 1
    ;;
esac
MANIFEST="$WORK/manifest.json"
HOST_CHECK="$SCRIPT_DIR/host-check.json"
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
  stop_client >/dev/null 2>&1 || true
  stop_server >/dev/null 2>&1 || true
  exit 1
}
log() { printf '%s\n' "[$(date -u +%H:%M:%S)] $*"; }

# ---------------------------------------------------------------------------
# Server lifecycle and error handling, defined early because die() (used by
# every check below) calls stop_server.  Exactly one server runs at a time;
# stop_server is idempotent and safe to call from an error handler.
# ---------------------------------------------------------------------------
active_prefix=""
active_datadir=""
active_pgbench_pid=""
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

stop_client() {
  if [[ -n "$active_pgbench_pid" ]] &&
     kill -0 "$active_pgbench_pid" 2>/dev/null; then
    kill "$active_pgbench_pid" 2>/dev/null || true
    wait "$active_pgbench_pid" 2>/dev/null || true
  fi
  active_pgbench_pid=""
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
  stop_client
  stop_server >/dev/null 2>&1 || true
  exit "$rc"
}
trap on_error ERR
on_exit() {
  stop_client
  stop_server >/dev/null 2>&1 || true
}
trap on_exit EXIT

# ---------------------------------------------------------------------------
# The one thing this script promises never to do: run on top of leftover
# results from a previous, possibly partial, matrix.
# ---------------------------------------------------------------------------
[[ ! -e "$RESULTS" ]] ||
  die "results directory already exists: $RESULTS
A partial matrix cannot be resumed or merged -- every configuration has to
meet the same machine conditions for the comparison to mean anything. Move
$RESULTS aside (or remove it) if you want to start over, or run
./03-collect.sh first if it holds a finished run you want to keep."

for tool in python3 sha256sum awk ps; do
  command -v "$tool" >/dev/null 2>&1 || die "required tool not found: $tool"
done
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
[[ -r "$MANIFEST" ]] ||
  die "missing $MANIFEST -- run ./01-build-all.sh first"
BUILD_MANIFEST_SHA256=$(sha256sum "$MANIFEST" | awk '{print $1}')

PREFIX_BASELINE_A="$WORK/install/base-a"
PREFIX_BASELINE_B="$WORK/install/base-b"
PREFIX_PATCHED="$WORK/install/patchd"
for p in "$PREFIX_BASELINE_A" "$PREFIX_BASELINE_B" "$PREFIX_PATCHED"; do
  [[ -x "$p/bin/postgres" ]] || die "missing build at $p -- run ./01-build-all.sh first"
done

if ! python3 - "$MANIFEST" \
  "baseline-a=$PREFIX_BASELINE_A" \
  "baseline-b=$PREFIX_BASELINE_B" \
  "patched=$PREFIX_PATCHED" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

manifest = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
prefixes = dict(item.split("=", 1) for item in sys.argv[2:])
if manifest.get("schema_version") != 5:
    raise SystemExit("unsupported build manifest schema")
if (
    manifest.get("benchmark_series") != "wet-v9"
    or manifest.get("treatment") != "null-hook-fast-path"
):
    raise SystemExit("unexpected build benchmark series or treatment")
builds = {item["name"]: item for item in manifest.get("builds", [])}
if set(builds) != set(prefixes):
    raise SystemExit("build manifest does not describe the required prefixes")
for name, prefix_text in prefixes.items():
    prefix = Path(prefix_text)
    expected = builds[name]["sha256"]
    for binary in ("postgres", "pgbench", "psql", "initdb", "pg_ctl"):
        path = prefix / "bin" / binary
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != expected[binary]:
            raise SystemExit(f"{name}/{binary} differs from build manifest")
    fixture = next(prefix.glob("lib/**/test_wait_primitive.*"), None)
    if fixture is None or hashlib.sha256(
            fixture.read_bytes()).hexdigest() != expected["test_wait_primitive"]:
        raise SystemExit(f"{name}/test_wait_primitive differs from manifest")
    modules = [
        path for path in prefix.glob("lib/**/pg_wait_event_tracing.*")
        if path.is_file()
    ]
    if name == "patched":
        if len(modules) != 1:
            raise SystemExit("patched prefix has no unique tracing module")
        actual = hashlib.sha256(modules[0].read_bytes()).hexdigest()
        if actual != expected["pg_wait_event_tracing"]:
            raise SystemExit("patched tracing module differs from manifest")
    elif modules:
        raise SystemExit(f"{name} unexpectedly contains the tracing module")
PY
then
  die "installed binaries do not match $MANIFEST; rebuild before running"
fi

# ---------------------------------------------------------------------------
# The extension's exact names, read from contrib/pg_wait_event_tracing's
# source, not guessed:
#   library / shared_preload_libraries entry : pg_wait_event_tracing
#   pg_wait_event_tracing.capture             enum: off | stats | trace
#   pg_wait_event_tracing.max_tranches        integer, PGC_POSTMASTER
#   pg_wait_event_tracing.trace_ring_size     integer (KB), PGC_POSTMASTER
# ---------------------------------------------------------------------------
MODULE_NAME="pg_wait_event_tracing"
GUC_CAPTURE="pg_wait_event_tracing.capture"
GUC_MAX_TRANCHES="pg_wait_event_tracing.max_tranches"
GUC_TRACE_RING_SIZE="pg_wait_event_tracing.trace_ring_size"

# ---------------------------------------------------------------------------
# The public full protocol is fixed, not tunable through inherited
# environment variables. The internal smoke profile exercises all
# configurations/workloads quickly before the full run.
# ---------------------------------------------------------------------------
PORT=${PGBENCH_KIT_PORT:-55471}
DBUSER=$(id -un)

(( RUNS > 0 )) || die "RUNS must be greater than zero"
(( DURATION >= 5 )) || die "DURATION must be at least 5 seconds"
(( WARMUP_SECONDS >= 2 )) || die "WARMUP_SECONDS must be at least 2 seconds"
(( PGBENCH_SCALE > 0 )) || die "PGBENCH_SCALE must be greater than zero"
(( W1_ITERATIONS > 0 )) || die "W1_ITERATIONS must be greater than zero"
(( QUIESCENCE_SECONDS >= 0 )) || die "QUIESCENCE_SECONDS must not be negative"

# CPU pinning is intentionally unsupported for this packaged protocol. The
# executor requirement is to leave both variables unset; the kit never
# derives or invents topology-specific ranges.
SERVER_CPUS=${SERVER_CPUS:-}
PGBENCH_CPUS=${PGBENCH_CPUS:-}
[[ -z "$SERVER_CPUS" && -z "$PGBENCH_CPUS" ]] ||
  die "SERVER_CPUS and PGBENCH_CPUS must remain unset for this protocol"

CONFIGS=(master master-aa hook-null module-off stats trace)
WORKLOADS=(W1 W3 W4 W6c)
TOTAL_CELLS=$(( ${#CONFIGS[@]} * ${#WORKLOADS[@]} * RUNS ))

SOCKET_DIR="$RESULTS/sock"
DATA_ROOT="$RESULTS/data"
LOG_DIR="$RESULTS/logs"
CSV="$RESULTS/results.csv"
SCHEDULE_CSV="$RESULTS/schedule.csv"
SEED_FILE="$RESULTS/seed.txt"
TELEMETRY="$RESULTS/telemetry.jsonl"
PROTOCOL="$RESULTS/protocol.json"
RECORDING_DIR="$RESULTS/recording-proofs"
W3_QUAL_DIR="$RESULTS/w3-qualification"
CLIENT_LOAD_DIR="$RESULTS/client-load"
WORKLOAD_DIR="$SCRIPT_DIR/workloads"
PROGRESS="$RESULTS/progress.json"
AA_EARLY="$RESULTS/aa-early.jsonl"

mkdir -p "$RESULTS" "$SOCKET_DIR" "$DATA_ROOT" "$LOG_DIR" \
  "$RECORDING_DIR" "$W3_QUAL_DIR" "$CLIENT_LOAD_DIR"
: >"$AA_EARLY"

for workload_file in w3-short-lwlock.sql recording-proof.sql w3-qualification.sql; do
  [[ -r "$WORKLOAD_DIR/$workload_file" ]] ||
    die "missing workload file: $WORKLOAD_DIR/$workload_file"
done
for helper in benchmark_protocol.py w3_qualification.py; do
  [[ -r "$SCRIPT_DIR/$helper" ]] ||
    die "missing Python helper: $SCRIPT_DIR/$helper"
done

# A unix socket path has a hard length limit (~100 bytes on Linux). Fail
# with a clear message now rather than a cryptic connection error later.
SOCKET_PROBE="$SOCKET_DIR/.s.PGSQL.$PORT"
if [[ ${#SOCKET_PROBE} -gt 100 ]]; then
  die "the kit's path is too long for a unix socket ($SOCKET_PROBE is ${#SOCKET_PROBE} bytes). Move the whole v9 kit somewhere with a shorter path (e.g. directly under your home directory) and try again."
fi
[[ ! -e "$SOCKET_PROBE" ]] ||
  die "a socket already exists at $SOCKET_PROBE; is a server from a previous run still up?"

# ---------------------------------------------------------------------------
# Per-configuration postgresql.conf additions.
# ---------------------------------------------------------------------------
config_build_prefix() {
  case "$1" in
    master) echo "$PREFIX_BASELINE_A" ;;
    master-aa) echo "$PREFIX_BASELINE_B" ;;
    hook-null|module-off|stats|trace) echo "$PREFIX_PATCHED" ;;
    *) die "unknown config: $1" ;;
  esac
}

config_build_name() {
  case "$1" in
    master) echo baseline-a ;;
    master-aa) echo baseline-b ;;
    hook-null|module-off|stats|trace) echo patched ;;
    *) die "unknown config: $1" ;;
  esac
}

append_config_lines() {
  local config=$1 conf_file=$2
  case "$config" in
    master|master-aa)
      # Unmodified baseline build. Nothing to add.
      ;;
    hook-null)
      # Patched build, but the collector module is deliberately left out of
      # shared_preload_libraries, so it never attaches. This measures the
      # cost of the core hook call sites alone.
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
        # Shared-memory sizing GUCs, set explicitly (at their module
        # defaults) so the configuration is self-documenting rather than
        # relying on silent defaults.
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
    W6c) echo 32MB ;;
    *) die "unknown workload: $1" ;;
  esac
}

init_cluster() {
  local prefix=$1 datadir=$2 shared_buffers=$3 logfile=$4
  local conf="$datadir/postgresql.conf"

  run_from_prefix "$prefix" "$prefix/bin/initdb" \
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
  local -a server_pin=()
  [[ -z "$SERVER_CPUS" ]] || server_pin=(taskset -c "$SERVER_CPUS")
  run_from_prefix "$prefix" "${server_pin[@]}" "$prefix/bin/pg_ctl" \
    -D "$datadir" -l "$logfile" \
    -o "-p $PORT -k $SOCKET_DIR" -w start >/dev/null ||
    die "server failed to start for $config; see $logfile"
}

psql_for() {
  local prefix=$1
  shift
  PGHOST="$SOCKET_DIR" PGPORT="$PORT" PGUSER="$DBUSER" \
    run_from_prefix "$prefix" "$prefix/bin/psql" \
    -X -v ON_ERROR_STOP=1 -d postgres "$@"
}

# ---------------------------------------------------------------------------
# Correction (post-review): every cell must PROVE the feature is in the
# state the configuration claims, rather than trusting that
# shared_preload_libraries and capture took effect. A silent preload
# failure would otherwise make "stats"/"trace" rows read identical to
# "module-off" -- a false "wait tracing is free" result.
# ---------------------------------------------------------------------------
capture_should_be_active() {
  case "$1" in
    stats|trace) return 0 ;;
    *) return 1 ;;
  esac
}

assert_capture_state() {
  local prefix=$1 config=$2 preload capture_out expected

  preload=$(psql_for "$prefix" -qAtc "SHOW shared_preload_libraries" 2>&1) ||
    die "could not read shared_preload_libraries for $config: $preload"

  case "$config" in
    master|master-aa|hook-null)
      case "$preload" in
        *"$MODULE_NAME"*)
          die "$MODULE_NAME is unexpectedly present in shared_preload_libraries for $config ('$preload') -- the module must be entirely absent from this configuration"
          ;;
      esac
      if capture_out=$(psql_for "$prefix" -qAtc "SHOW $GUC_CAPTURE" 2>&1); then
        die "$GUC_CAPTURE is unexpectedly readable for $config (got '$capture_out'); the module should not be loaded at all, so this GUC should not exist"
      fi
      ;;
    module-off|stats|trace)
      case "$preload" in
        *"$MODULE_NAME"*) ;;
        *)
          die "$MODULE_NAME is missing from shared_preload_libraries for $config (got '$preload') -- the collector never loaded"
          ;;
      esac
      case "$config" in
        module-off) expected=off ;;
        stats) expected=stats ;;
        trace) expected=trace ;;
      esac
      capture_out=$(psql_for "$prefix" -qAtc "SHOW $GUC_CAPTURE" 2>&1) ||
        die "$GUC_CAPTURE is not readable for $config even though the module should be loaded (got: $capture_out)"
      [[ "$capture_out" == "$expected" ]] ||
        die "$GUC_CAPTURE is '$capture_out' for $config, expected exactly '$expected'"
      ;;
    *)
      die "unknown config: $config"
      ;;
  esac
}

install_and_assert_sql_extension() {
  local prefix=$1 config=$2 present

  case "$config" in
    module-off|stats|trace)
      psql_for "$prefix" -qAtc "CREATE EXTENSION $MODULE_NAME" >/dev/null ||
        die "CREATE EXTENSION $MODULE_NAME failed for $config"
      present=$(psql_for "$prefix" -qAtc \
        "SELECT count(*) FROM pg_extension WHERE extname = '$MODULE_NAME'") ||
        die "could not verify $MODULE_NAME SQL installation for $config"
      [[ "$present" == 1 ]] ||
        die "$MODULE_NAME SQL extension is not installed for $config"
      ;;
    master|master-aa|hook-null)
      present=$(psql_for "$prefix" -qAtc \
        "SELECT count(*) FROM pg_extension WHERE extname = '$MODULE_NAME'") ||
        die "could not inspect pg_extension for $config"
      [[ "$present" == 0 ]] ||
        die "$MODULE_NAME SQL extension unexpectedly exists for $config"
      ;;
    *) die "unknown config: $config" ;;
  esac
}

cpu_freq_median() {
  python3 - <<'PY'
import glob
import statistics
vals = []
for path in glob.glob(
        "/sys/devices/system/cpu/cpu*/cpufreq/scaling_cur_freq"):
    try:
        vals.append(int(open(path, encoding="ascii").read().strip()))
    except (OSError, ValueError):
        pass
print(int(statistics.median(vals)) if vals else "")
PY
}

record_telemetry() {
  local phase=$1 run_index=$2 config=$3 workload=$4 repetition=$5 freq=$6
  python3 - "$TELEMETRY" "$phase" "$run_index" "$config" "$workload" \
    "$repetition" "$freq" <<'PY'
import datetime
import json
import sys
out, phase, run_index, config, workload, repetition, freq = sys.argv[1:]
row = {
    "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "phase": phase, "run_index": int(run_index), "config": config,
    "workload": workload, "repetition": int(repetition),
}
if freq:
    row["cpu_freq_khz_median"] = int(freq)
with open(out, "a", encoding="utf-8") as f:
    json.dump(row, f, sort_keys=True)
    f.write("\n")
PY
}

# ---------------------------------------------------------------------------
# CSV output.
# ---------------------------------------------------------------------------
CSV_HEADER="run_index,seed,timestamp_utc,config,build,workload,repetition,shared_buffers,clients,duration_s,warmup_s,iterations,ns_per_iteration,tps,latency_avg_ms,measurement_samples,measurement_interval_s,pgbench_cpu_percent,pgbench_cpu_capacity_fraction,cpu_freq_khz_median_before,cpu_freq_khz_median_after,server_cpus,pgbench_cpus,server_log"
echo "$CSV_HEADER" >"$CSV"

csv_row() {
  # All arguments are plain tokens, numbers, or relative paths -- none of
  # them can contain a comma, so joining on IFS is safe.
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
  python3 - "$PROGRESS" "$state" "$message" "${CURRENT_CELL:-}" \
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

check_early_aa() {
  local repetitions=$1
  python3 - "$CSV" "$AA_EARLY" "$repetitions" <<'PY'
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
workloads = ("W1", "W3", "W4", "W6c")
checks = {}
passed = True
for workload in workloads:
    metric = "ns_per_iteration" if workload == "W1" else "tps"
    master = [
        float(by_key[("master", workload, repetition)][metric])
        for repetition in range(1, repetitions + 1)
    ]
    aa = [
        float(by_key[("master-aa", workload, repetition)][metric])
        for repetition in range(1, repetitions + 1)
    ]
    pairs = [
        (left - right) / ((left + right) / 2.0) * 100.0
        for left, right in zip(aa, master)
    ]
    if repetitions == 1:
        item_passed = abs(pairs[0]) <= 10.0
        item = {
            "absolute_pair_percent": abs(pairs[0]),
            "limit_percent": 10.0,
        }
    else:
        master_cv = statistics.stdev(master) / statistics.mean(master) * 100
        aa_cv = statistics.stdev(aa) / statistics.mean(aa) * 100
        mean_bias = statistics.mean(pairs)
        item_passed = (
            master_cv <= 5.0
            and aa_cv <= 5.0
            and abs(mean_bias) <= 5.0
        )
        item = {
            "master_cv_percent": master_cv,
            "master_aa_cv_percent": aa_cv,
            "mean_paired_bias_percent": mean_bias,
            "max_cv_percent": 5.0,
            "max_absolute_mean_paired_bias_percent": 5.0,
        }
    item["passed"] = item_passed
    checks[workload] = item
    passed = passed and item_passed

record = {
    "checked_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "repetitions_complete": repetitions,
    "checks": checks,
    "passed": passed,
}
with open(output_path, "a", encoding="utf-8") as stream:
    json.dump(record, stream, sort_keys=True)
    stream.write("\n")
if not passed:
    failed = ", ".join(
        workload for workload, item in checks.items() if not item["passed"]
    )
    raise SystemExit(
        f"early baseline A/A noise gate failed after repetition "
        f"{repetitions}: {failed}"
    )
PY
}

# ---------------------------------------------------------------------------
# pgbench measurement: one continuous run of WARMUP_SECONDS + DURATION
# seconds with one progress line per second; only the progress samples
# taken after the warmup window count towards the reported TPS and
# latency. This is the same warmup-then-discard method the earlier harness
# used, just folded into a single pgbench invocation instead of two.
#
# check_recording (yes/no): for the "stats"/"trace" configurations only,
# take one concurrent snapshot of the actual pgbench client PIDs partway
# through the discarded warmup. Every client must have nonzero calls. This
# has to happen WHILE pgbench's client backends are
# still connected: the module frees a backend's own stats when that
# backend disconnects, so checking after pgbench has already exited would
# always read zero. The snapshot is deliberately taken during warmup, not
# near the end of the run: by then the post-warmup filter already discards
# that second of data, so the snapshot's one extra connection cannot
# perturb any of the samples that end up in the reported TPS/latency --
# unlike sampling near the end, which would touch one of the thirty
# measured one-second samples in every stats/trace cell only, an
# asymmetric contamination of exactly the comparison this kit exists to
# produce (biasing against, never for, stats/trace, but it has no
# business being there regardless).
# ---------------------------------------------------------------------------
run_pgbench_measured() {
  local prefix=$1 clients=$2 threads=$3 seed=$4 logfile=$5 check_recording=$6
  local appname=$7 workload=$8 run_index=$9
  shift 9
  local -a extra_args=("$@")
  local total=$((WARMUP_SECONDS + DURATION))
  local -a client_pin=()
  [[ -z "$PGBENCH_CPUS" ]] || client_pin=(taskset -c "$PGBENCH_CPUS")

  run_pgbench_from_prefix "$prefix" "$appname" \
    "${client_pin[@]}" "$prefix/bin/pgbench" -n \
    -c "$clients" -j "$threads" -T "$total" -P 1 \
    --random-seed="$seed" "${extra_args[@]}" postgres \
    >"$logfile" 2>&1 &
  local pgbench_pid=$!
  active_pgbench_pid=$pgbench_pid

  # Sample the client driver's aggregate CPU use for every configuration.
  # With pinning deliberately optional, a driver sitting near the capacity
  # of all its threads is a confound that must be visible in the evidence.
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
  pgbench_comm=$(ps -o comm= -p "$pgbench_pid" | awk '{print $1}')
  [[ "${pgbench_comm##*/}" == pgbench ]] ||
    die "background pid $pgbench_pid is '$pgbench_comm', not pgbench"
  pgbench_cpu=$(ps -o pcpu= -p "$pgbench_pid" | awk '{print $1}')
  [[ "$pgbench_cpu" =~ ^[0-9]+([.][0-9]+)?$ ]] ||
    die "could not read pgbench CPU utilization for pid $pgbench_pid"
  local pgbench_capacity
  pgbench_capacity=$(python3 - "$pgbench_cpu" "$threads" <<'PY'
import sys
value, threads = float(sys.argv[1]), int(sys.argv[2])
print(f"{value / (threads * 100.0):.12g}")
PY
  )
  python3 - "$pgbench_capacity" <<'PY' ||
import sys

capacity = float(sys.argv[1])
if capacity >= 0.90:
    raise SystemExit(
        f"pgbench used {capacity:.1%} of its thread capacity; "
        "the client driver is saturated"
    )
PY
    die "pgbench client saturation invalidates $CURRENT_CELL; see $logfile"
  python3 - "$CLIENT_LOAD_DIR/cell-$run_index.json" \
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
      local expect_trace=false
      [[ "$CURRENT_CELL" == *"config=trace"* ]] && expect_trace=true
      local proof_file="$RECORDING_DIR/cell-$run_index.csv"
      local proof client_count clients_recording timing_calls trace_records
      proof=$(psql_for "$prefix" -qAt -F ',' \
        -v appname="$appname" -v expect_trace="$expect_trace" \
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
          die "malformed recording proof '$proof' for $CURRENT_CELL"
      done
      (( client_count == clients )) ||
        die "recording proof saw $client_count pgbench clients, expected $clients"
      (( clients_recording == clients )) ||
        die "only $clients_recording/$clients pgbench clients recorded timing data"
      (( timing_calls > 0 )) ||
        die "the pgbench client backends recorded zero timing calls"
      if [[ "$expect_trace" == true ]]; then
        (( trace_records > 0 )) ||
          die "trace capture has timing rows but its representative client ring is empty"
      else
        (( trace_records == 0 )) ||
          die "non-trace recording proof unexpectedly returned trace records"
      fi

      if [[ "$workload" == W3 ]]; then
        local w3_raw="$W3_QUAL_DIR/cell-$run_index.tsv"
        local w3_summary="$W3_QUAL_DIR/cell-$run_index.json"
        local w3_elapsed
        psql_for "$prefix" -qAt -F $'\t' -v appname="$appname" \
          -f "$WORKLOAD_DIR/w3-qualification.sql" >"$w3_raw" ||
          die "could not capture W3 qualification evidence"
        w3_elapsed=$((SECONDS - proof_start))
        (( w3_elapsed > 0 && w3_elapsed < WARMUP_SECONDS )) ||
          die "W3 qualification consumed the warmup window"
        if ! python3 "$SCRIPT_DIR/w3_qualification.py" \
          "$w3_raw" "$w3_summary" "$w3_elapsed"; then
          die "W3 did not qualify as short ProcArray LWLock contention; see $w3_summary"
        fi
      fi

    if (( SECONDS - proof_start >= WARMUP_SECONDS )); then
      die "recording proof consumed the entire warmup window and could contaminate the measured samples"
    fi
  fi

  wait "$pgbench_pid" || {
    active_pgbench_pid=""
    die "pgbench failed; see $logfile"
  }
  active_pgbench_pid=""

  local parsed
  parsed=$(python3 - "$logfile" "$WARMUP_SECONDS" "$DURATION" <<'PY'
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
  local run_index=$1 config=$2 workload=$3 repetition=$4
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

  # initdb and pgbench dataset creation always run with the same baseline
  # binaries and with no collector loaded.  The treatment server starts only
  # after setup has shut down cleanly.
  init_cluster "$PREFIX_BASELINE_A" "$datadir" "$shared_buffers" "$logfile"
  if [[ "$workload" == W4 || "$workload" == W6c ]]; then
    local setup_log="$LOG_DIR/server-setup-$run_index.log"
    start_server "$PREFIX_BASELINE_A" "$datadir" master "$setup_log"
    PGHOST="$SOCKET_DIR" PGPORT="$PORT" PGUSER="$DBUSER" \
      run_from_prefix "$PREFIX_BASELINE_A" \
      "$PREFIX_BASELINE_A/bin/pgbench" -i -q --unlogged-tables \
      -s "$PGBENCH_SCALE" postgres \
      >"$LOG_DIR/pgbench-init-$run_index.log" 2>&1 ||
      die "pgbench -i failed for $workload; see $LOG_DIR/pgbench-init-$run_index.log"
    psql_for "$PREFIX_BASELINE_A" -qAtc "CHECKPOINT" >/dev/null ||
      die "neutral setup checkpoint failed for $workload"
    stop_server_checked "neutral $workload dataset setup"
  fi

  start_server "$prefix" "$datadir" "$config" "$logfile"
  assert_capture_state "$prefix" "$config"
  install_and_assert_sql_extension "$prefix" "$config"
  if [[ "$workload" == W1 || "$workload" == W3 ]]; then
    psql_for "$prefix" -qAtc \
      "CREATE EXTENSION test_wait_primitive" >/dev/null ||
      die "CREATE EXTENSION test_wait_primitive failed"
  fi

  local iterations="" ns_per_iteration="" tps="" latency_ms="" clients=""
  local samples="" measurement_interval=""
  local pgbench_cpu="" pgbench_capacity=""
  local duration_field=$DURATION warmup_field=$WARMUP_SECONDS
  local freq_before freq_after

  local check_recording=no
  capture_should_be_active "$config" && check_recording=yes

  # Deterministic preconditioning.  W4 is explicitly warm-cache; W6c is not
  # scanned because its constrained shared_buffers/eviction behavior is the
  # workload.  A checkpoint and fixed quiet period precede every measured
  # pgbench run.
  psql_for "$prefix" -qAtc "CHECKPOINT" >/dev/null ||
    die "pre-measurement checkpoint failed for $workload"
  if [[ "$workload" == W4 ]]; then
    psql_for "$prefix" -qAtc \
      "SELECT count(*) FROM pgbench_accounts" >/dev/null ||
      die "warm-cache scan of pgbench_accounts failed"
  fi
  (( QUIESCENCE_SECONDS == 0 )) || sleep "$QUIESCENCE_SECONDS"

  freq_before=$(cpu_freq_median)
  record_telemetry before "$run_index" "$config" "$workload" "$repetition" "$freq_before"

  case "$workload" in
    W1)
      local warmup_iter=$((W1_ITERATIONS / 100))
      (( warmup_iter > 0 )) || warmup_iter=1
      psql_for "$prefix" -qAtc \
        "SELECT test_wait_primitive_latch_set($warmup_iter)" >/dev/null ||
        die "W1 warmup call failed"
      if [[ "$check_recording" == yes ]]; then
        # Fold the "did it actually record anything" check into the same
        # connection/statement as the measured call: a fresh psql
        # invocation is a fresh backend, and this module frees a backend's
        # stats when that backend disconnects, so a separate check
        # afterwards would always read zero.
        local combined recorded trace_records=0
        local trace_expr="0"
        [[ "$config" != trace ]] ||
          trace_expr="(SELECT count(*) FROM pg_backend_wait_event_trace WHERE wait_event_type <> 'Query')"
        combined=$(psql_for "$prefix" -qAt -F ',' -c \
          "WITH m AS MATERIALIZED
             (SELECT test_wait_primitive_latch_set($W1_ITERATIONS) AS ns)
           SELECT m.ns,
                  (SELECT coalesce(sum(calls), 0)
                   FROM pg_stat_get_wait_event_timing(pg_backend_pid())),
                  $trace_expr
           FROM m") ||
          die "W1 measured call failed"
        IFS=',' read -r ns_per_iteration recorded trace_records <<<"$combined"
        if [[ ! "$recorded" =~ ^[0-9]+$ ]] || (( recorded == 0 )); then
          die "pg_stat_get_wait_event_timing recorded zero calls for this backend after the W1 measured call -- capture is not actually collecting"
        fi
        [[ "$trace_records" =~ ^[0-9]+$ ]] ||
          die "W1 returned a malformed trace proof: '$trace_records'"
        if [[ "$config" == trace ]] && (( trace_records == 0 )); then
          die "W1 timing counters recorded, but the backend trace ring is empty"
        fi
        printf '%s\n' \
          "timing_calls,trace_records" "$recorded,$trace_records" \
          >"$RECORDING_DIR/cell-$run_index.csv"
      else
        ns_per_iteration=$(psql_for "$prefix" -qAtc \
          "SELECT test_wait_primitive_latch_set($W1_ITERATIONS)") ||
          die "W1 measured call failed"
      fi
      iterations=$W1_ITERATIONS
      duration_field=""
      warmup_field=""
      ;;
    W3)
      clients=8
      run_pgbench_measured "$prefix" 8 8 "$repetition" \
        "$LOG_DIR/pgbench-$run_index.log" "$check_recording" \
        "wet-v9-$run_index" "$workload" "$run_index" \
        -f "$WORKLOAD_DIR/w3-short-lwlock.sql"
      tps=$TPS_RESULT
      latency_ms=$LATENCY_RESULT
      samples=$SAMPLES_RESULT
      measurement_interval=$INTERVAL_RESULT
      pgbench_cpu=$PGBENCH_CPU_RESULT
      pgbench_capacity=$PGBENCH_CAPACITY_RESULT
      ;;
    W4)
      clients=16
      run_pgbench_measured "$prefix" 16 8 "$repetition" \
        "$LOG_DIR/pgbench-$run_index.log" "$check_recording" \
        "wet-v9-$run_index" "$workload" "$run_index" -S
      tps=$TPS_RESULT
      latency_ms=$LATENCY_RESULT
      samples=$SAMPLES_RESULT
      measurement_interval=$INTERVAL_RESULT
      pgbench_cpu=$PGBENCH_CPU_RESULT
      pgbench_capacity=$PGBENCH_CAPACITY_RESULT
      ;;
    W6c)
      clients=32
      run_pgbench_measured "$prefix" 32 8 "$repetition" \
        "$LOG_DIR/pgbench-$run_index.log" "$check_recording" \
        "wet-v9-$run_index" "$workload" "$run_index" -S
      tps=$TPS_RESULT
      latency_ms=$LATENCY_RESULT
      samples=$SAMPLES_RESULT
      measurement_interval=$INTERVAL_RESULT
      pgbench_cpu=$PGBENCH_CPU_RESULT
      pgbench_capacity=$PGBENCH_CAPACITY_RESULT
      ;;
    *)
      die "unknown workload: $workload"
      ;;
  esac

  freq_after=$(cpu_freq_median)
  record_telemetry after "$run_index" "$config" "$workload" "$repetition" "$freq_after"

  stop_server_checked "$CURRENT_CELL"
  rm -rf -- "$datadir"

  csv_row "$run_index" "$SEED" "$ts_start" "$config" "$build" "$workload" \
    "$repetition" "$shared_buffers" "$clients" "$duration_field" "$warmup_field" \
    "$iterations" "$ns_per_iteration" "$tps" "$latency_ms" \
    "$samples" "$measurement_interval" "$pgbench_cpu" "$pgbench_capacity" \
    "$freq_before" "$freq_after" "$SERVER_CPUS" "$PGBENCH_CPUS" \
    "logs/server-$run_index.log"

  CURRENT_CELL=""
}

# ---------------------------------------------------------------------------
# Build one independently randomized complete configuration block for each
# workload/repetition pair.  Workload-block order and configuration order
# are both randomized.  Paired contrasts therefore compare nearby cells.
# ---------------------------------------------------------------------------
SEED=${RUN_SEED:-$(od -An -N8 -tu8 /dev/urandom | tr -d ' \n')}
echo "$SEED" >"$SEED_FILE"
log "Random seed for this run's schedule: $SEED (saved to $SEED_FILE)"

python3 - "$SCHEDULE_CSV" "$SEED" "$RUNS" "${#CONFIGS[@]}" "${#WORKLOADS[@]}" \
  "${CONFIGS[@]}" "${WORKLOADS[@]}" <<'PY'
import random
import sys

args = sys.argv[1:]
path, seed, runs, nconfigs, nworkloads = args[0], int(args[1]), int(args[2]), \
    int(args[3]), int(args[4])
rest = args[5:]
configs = rest[:nconfigs]
workloads = rest[nconfigs:nconfigs + nworkloads]

rng = random.Random(seed)
cells = []
for repetition in range(1, runs + 1):
    workload_order = list(workloads)
    rng.shuffle(workload_order)
    for workload in workload_order:
        config_order = list(configs)
        rng.shuffle(config_order)
        cells.extend(
            (config, workload, repetition) for config in config_order
        )

with open(path, "x", encoding="utf-8") as f:
    f.write("run_index,config,workload,repetition\n")
    for i, (config, workload, repetition) in enumerate(cells, 1):
        f.write(f"{i},{config},{workload},{repetition}\n")
PY

python3 - "$PROTOCOL" "$SEED" "$RUNS" "$DURATION" "$WARMUP_SECONDS" \
  "$PGBENCH_SCALE" "$W1_ITERATIONS" "$MODULE_NAME" "$GUC_CAPTURE" \
  "$GUC_MAX_TRANCHES" "$GUC_TRACE_RING_SIZE" "$SERVER_CPUS" "$PGBENCH_CPUS" \
  "$QUIESCENCE_SECONDS" "$TOTAL_CELLS" "$SCRIPT_DIR" \
  "$MODE" "$BUILD_MANIFEST_SHA256" \
  "${CONFIGS[@]}" -- "${WORKLOADS[@]}" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

args = sys.argv[1:]
(out, seed, runs, duration, warmup, scale, w1_iterations, module,
 guc_capture, guc_max_tranches, guc_trace_ring_size, server_cpus,
 pgbench_cpus, quiescence, total_cells, script_dir, mode,
 build_manifest_sha256) = args[:18]
rest = args[18:]
sep = rest.index("--")
configs, workloads = rest[:sep], rest[sep + 1:]

root = Path(script_dir)
sys.path.insert(0, script_dir)
from benchmark_protocol import BOUND_KIT_FILES, W3_PROTOCOL

bound_files = BOUND_KIT_FILES
def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

data = {
    "schema_version": 4,
    "benchmark_series": "wet-v9",
    "treatment": "null-hook-fast-path",
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
    "server_cpus": server_cpus or None,
    "pgbench_cpus": pgbench_cpus or None,
    "cpu_pinning": "enabled (taskset)" if (server_cpus or pgbench_cpus) else "unpinned (default)",
    "build_manifest_sha256": build_manifest_sha256,
    "host_check_sha256": {
        name: digest(root / name)
        for name in ("host-check.txt", "host-check.json")
    },
    "configs": configs,
    "workloads": workloads,
    "schedule": (
        "one complete independently shuffled configuration block for each "
        "workload x repetition; workload-block order also shuffled"
    ),
    "outlier_policy": "retain every successful cell; no post-hoc exclusions",
    "analysis": {
        "script": "analyze-results.py",
        "primary_reference": "master",
        "pairing_key": "workload + repetition",
        "interval": "two-sided 95% Student t interval over paired contrasts",
        "w1_contrast": "config minus master, nanoseconds per iteration",
        "w1_equivalence_margin_ns": 2.0,
        "pgbench_contrast": "(config / master - 1) * 100 percent",
        "pgbench_equivalence_margin_percent": 2.0,
        "equivalence_rule": (
            "criterion met only when the complete 95% interval lies "
            "inside the corresponding predeclared margin"
        ),
        "unresolved_rule": (
            "an interval crossing zero is reported as no statistically "
            "resolved difference, never as proof of equivalence"
        ),
        "aa_stability": {
            "arms": ["master", "master-aa"],
            "max_cv_percent_each_arm": 2.0,
            "max_absolute_paired_bias_percent": 2.0,
        },
        "early_aa_gate": {
            "check_after_repetitions": [1, 3, 6, 9],
            "max_absolute_single_pair_percent": 10.0,
            "max_cv_percent_each_arm": 5.0,
            "max_absolute_mean_paired_bias_percent": 5.0,
        },
    },
    "max_pgbench_thread_capacity_fraction": 0.90,
    "w3_qualification": W3_PROTOCOL,
    "recording_proof": (
        "every stats/trace pgbench client must expose nonzero timing data; "
        "trace must also expose a non-Query wait in a representative ring"
    ),
    "setup": {
        "initdb_build": "baseline-a for every cell",
        "dataset_initialization": (
            "baseline-a with collector absent, followed by CHECKPOINT and "
            "confirmed normal shutdown"
        ),
        "pre_measurement": (
            "CREATE EXTENSION as required, CHECKPOINT, W4 full account "
            "scan only, then fixed quiescence"
        ),
        "shutdown": "pg_ctl fast, wait, required success before data removal",
    },
    "bound_file_sha256": {
        name: digest(root / name) for name in bound_files
    },
}
with open(out, "x", encoding="utf-8") as f:
    json.dump(data, f, indent=2, sort_keys=True)
    f.write("\n")
PY

log "Schedule written: $SCHEDULE_CSV ($TOTAL_CELLS cells)"
log "Protocol recorded: $PROTOCOL"
log "Mode: $MODE"
if [[ "$MODE" == full ]]; then
  log "Starting the matrix. Expect roughly 4-5.5 hours."
else
  log "Starting the 24-cell smoke matrix."
fi
log ""

# ---------------------------------------------------------------------------
# Main loop.
# ---------------------------------------------------------------------------
mapfile -t SCHEDULE_LINES < <(tail -n +2 "$SCHEDULE_CSV")
[[ ${#SCHEDULE_LINES[@]} -eq "$TOTAL_CELLS" ]] ||
  die "schedule has ${#SCHEDULE_LINES[@]} rows, expected $TOTAL_CELLS"

START_EPOCH=$(date +%s)
MATRIX_START_EPOCH=$START_EPOCH
write_progress running "matrix initialized"
n=0
for line in "${SCHEDULE_LINES[@]}"; do
  IFS=',' read -r run_index config workload repetition <<<"$line"
  n=$((n + 1))
  cell_start=$(date +%s)
  CURRENT_CELL="run_index=$run_index config=$config workload=$workload repetition=$repetition"

  log "[$n/$TOTAL_CELLS] config=$config workload=$workload repetition=$repetition"
  write_progress running "starting cell $n/$TOTAL_CELLS"
  run_cell "$run_index" "$config" "$workload" "$repetition"
  CELLS_COMPLETED=$n

  cell_elapsed=$(( $(date +%s) - cell_start ))
  total_elapsed=$(( $(date +%s) - START_EPOCH ))
  if (( n > 0 )); then
    remaining=$(( total_elapsed * (TOTAL_CELLS - n) / n ))
    log "  done in ${cell_elapsed}s | elapsed $((total_elapsed / 60))m | ETA $((remaining / 60))m"
  fi
  write_progress running "completed cell $n/$TOTAL_CELLS"

  if [[ "$MODE" == full && $((n % 24)) -eq 0 ]]; then
    completed_repetition=$((n / 24))
    case "$completed_repetition" in
      1|3|6|9)
        log "Running early baseline A/A noise gate after repetition $completed_repetition"
        check_early_aa "$completed_repetition" ||
          die "early baseline A/A noise gate failed; the host is too noisy to continue"
        ;;
    esac
  fi
done

write_progress complete "all matrix cells completed"
python3 - "$RESULTS/matrix-complete.json" "$CSV" "$SCHEDULE_CSV" \
  "$TELEMETRY" "$PROTOCOL" "$PROGRESS" "$AA_EARLY" "$TOTAL_CELLS" <<'PY'
import csv
import datetime
import hashlib
import json
import sys
from pathlib import Path

(
    out,
    results_path,
    schedule_path,
    telemetry_path,
    protocol_path,
    progress_path,
    aa_early_path,
    expected,
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
        "telemetry.jsonl": digest(telemetry_path),
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
