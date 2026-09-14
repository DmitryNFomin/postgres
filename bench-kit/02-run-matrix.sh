#!/usr/bin/env bash
# 02-run-matrix.sh
#
# Runs the full measurement matrix: 5 configurations x 4 workloads x 12
# repetitions = 240 runs, in one randomized order (not grouped by
# configuration or workload), each on a completely fresh PostgreSQL data
# directory that is created just before the run and removed right after.
#
# The five configurations (see the README for the long version):
#   1  master       baseline build, nothing loaded
#   2  hook-null    patched build, collector module NOT preloaded
#   3  module-off   patched build, collector loaded, capture switched off
#   4  stats        patched build, collector loaded, capture = 'stats'
#   5  trace        patched build, collector loaded, capture = 'trace'
#
# The four workloads:
#   W1   isolated wait-primitive microbenchmark, 10^8 iterations
#   W3   short LWLock contention, 8 pgbench clients
#   W4   pgbench read-only (-S), 16 clients, 4 GB shared_buffers
#   W6c  pgbench read-only (-S), 32 clients, 32 MB shared_buffers
#
# Expect this step to take roughly 2.5-3.5 hours. It is unattended and safe
# to run under tmux/screen -- just start it and check back later.
set -Eeuo pipefail
export LC_ALL=C

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WORK="$SCRIPT_DIR/work"
RESULTS="$SCRIPT_DIR/results"
MANIFEST="$WORK/manifest.json"

die() {
  echo "" >&2
  echo "02-run-matrix.sh: ERROR: $*" >&2
  if [[ -n "${CURRENT_CELL:-}" ]]; then
    echo "FAILED at cell: $CURRENT_CELL" >&2
  fi
  stop_server
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
CURRENT_CELL=""

run_from_prefix() {
  local prefix=$1
  shift
  env LD_LIBRARY_PATH="$prefix/lib:${LD_LIBRARY_PATH:-}" "$@"
}

stop_server() {
  if [[ -n "$active_prefix" && -n "$active_datadir" ]]; then
    run_from_prefix "$active_prefix" "$active_prefix/bin/pg_ctl" \
      -D "$active_datadir" -m fast -w stop >/dev/null 2>&1 || true
  fi
  active_prefix=""
  active_datadir=""
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
  stop_server
  exit "$rc"
}
trap on_error ERR
trap stop_server EXIT

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

for tool in python3 sha256sum awk; do
  command -v "$tool" >/dev/null 2>&1 || die "required tool not found: $tool"
done
[[ -r "$MANIFEST" ]] ||
  die "missing $MANIFEST -- run ./01-build-all.sh first"

PREFIX_BASELINE="$WORK/install/baseline"
PREFIX_PATCHED="$WORK/install/patched"
for p in "$PREFIX_BASELINE" "$PREFIX_PATCHED"; do
  [[ -x "$p/bin/postgres" ]] || die "missing build at $p -- run ./01-build-all.sh first"
done

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
# Tunables. These match the earlier (v7) measurement harness so the method
# is not being reinvented: 12 repetitions, 10-second warmup discarded before
# a 30-second measured window, 10^8 iterations for the microbenchmark,
# pgbench scale 100.
# ---------------------------------------------------------------------------
RUNS=12
DURATION=${DURATION:-30}
WARMUP_SECONDS=${WARMUP_SECONDS:-10}
PGBENCH_SCALE=${PGBENCH_SCALE:-100}
W1_ITERATIONS=100000000
PORT=${PGBENCH_KIT_PORT:-55471}
DBUSER=$(id -un)

# Optional CPU pinning. Opt-in only: the supported default is to leave both
# unset and let the OS scheduler place everything. Only set these if you
# already know this machine's core topology and want to isolate the server
# from the pgbench client load -- this kit never derives ranges on its own.
# Format is whatever "taskset -c" accepts, e.g. SERVER_CPUS=0-7.
SERVER_CPUS=${SERVER_CPUS:-}
PGBENCH_CPUS=${PGBENCH_CPUS:-}
if [[ -n "$SERVER_CPUS" || -n "$PGBENCH_CPUS" ]]; then
  command -v taskset >/dev/null 2>&1 ||
    die "taskset is required when SERVER_CPUS or PGBENCH_CPUS is set"
fi
[[ -z "$SERVER_CPUS" ]] || taskset -c "$SERVER_CPUS" true ||
  die "SERVER_CPUS='$SERVER_CPUS' is not a usable CPU list on this machine"
[[ -z "$PGBENCH_CPUS" ]] || taskset -c "$PGBENCH_CPUS" true ||
  die "PGBENCH_CPUS='$PGBENCH_CPUS' is not a usable CPU list on this machine"

CONFIGS=(master hook-null module-off stats trace)
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

mkdir -p "$RESULTS" "$SOCKET_DIR" "$DATA_ROOT" "$LOG_DIR"

# A unix socket path has a hard length limit (~100 bytes on Linux). Fail
# with a clear message now rather than a cryptic connection error later.
SOCKET_PROBE="$SOCKET_DIR/.s.PGSQL.$PORT"
if [[ ${#SOCKET_PROBE} -gt 100 ]]; then
  die "the kit's path is too long for a unix socket ($SOCKET_PROBE is ${#SOCKET_PROBE} bytes). Move the whole bench-kit-v8 directory somewhere with a shorter path (e.g. directly under your home directory) and try again."
fi
[[ ! -e "$SOCKET_PROBE" ]] ||
  die "a socket already exists at $SOCKET_PROBE; is a server from a previous run still up?"

# ---------------------------------------------------------------------------
# Per-configuration postgresql.conf additions.
# ---------------------------------------------------------------------------
config_build_prefix() {
  case "$1" in
    master) echo "$PREFIX_BASELINE" ;;
    hook-null|module-off|stats|trace) echo "$PREFIX_PATCHED" ;;
    *) die "unknown config: $1" ;;
  esac
}

config_build_name() {
  case "$1" in
    master) echo baseline ;;
    hook-null|module-off|stats|trace) echo patched ;;
    *) die "unknown config: $1" ;;
  esac
}

append_config_lines() {
  local config=$1 conf_file=$2
  case "$config" in
    master)
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

start_server() {
  local prefix=$1 datadir=$2 shared_buffers=$3 config=$4 logfile=$5
  local conf="$datadir/postgresql.conf"

  run_from_prefix "$prefix" "$prefix/bin/initdb" \
    -D "$datadir" --no-sync --no-locale -E UTF8 \
    --auth-local=trust --auth-host=trust --username="$DBUSER" \
    >>"$logfile.initdb" 2>&1 ||
    die "initdb failed for $config; see $logfile.initdb"

  {
    echo "listen_addresses = ''"
    echo "unix_socket_directories = '$SOCKET_DIR'"
    echo "max_connections = 100"
    echo "shared_buffers = '$shared_buffers'"
    echo "autovacuum = off"
  } >>"$conf"
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
    master|hook-null)
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
CSV_HEADER="run_index,seed,timestamp_utc,config,build,workload,repetition,shared_buffers,clients,duration_s,warmup_s,iterations,ns_per_iteration,tps,latency_avg_ms,cpu_freq_khz_median_before,cpu_freq_khz_median_after,server_cpus,pgbench_cpus,server_log"
echo "$CSV_HEADER" >"$CSV"

csv_row() {
  # All arguments are plain tokens, numbers, or relative paths -- none of
  # them can contain a comma, so joining on IFS is safe.
  local IFS=,
  printf '%s\n' "$*" >>"$CSV"
}

# ---------------------------------------------------------------------------
# pgbench measurement: one continuous run of WARMUP_SECONDS + DURATION
# seconds with one progress line per second; only the progress samples
# taken after the warmup window count towards the reported TPS and
# latency. This is the same warmup-then-discard method the earlier harness
# used, just folded into a single pgbench invocation instead of two.
#
# check_recording (yes/no): for the "stats"/"trace" configurations only,
# take one concurrent snapshot of pg_stat_wait_event_timing partway through
# the (discarded) warmup window and die() if it shows zero calls
# cluster-wide. This has to happen WHILE pgbench's client backends are
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
  shift 6
  local -a extra_args=("$@")
  local total=$((WARMUP_SECONDS + DURATION))
  local -a client_pin=()
  [[ -z "$PGBENCH_CPUS" ]] || client_pin=(taskset -c "$PGBENCH_CPUS")

  PGHOST="$SOCKET_DIR" PGPORT="$PORT" PGUSER="$DBUSER" \
    run_from_prefix "$prefix" "${client_pin[@]}" "$prefix/bin/pgbench" -n \
    -c "$clients" -j "$threads" -T "$total" -P 1 \
    --random-seed="$seed" "${extra_args[@]}" postgres \
    >"$logfile" 2>&1 &
  local pgbench_pid=$!

  if [[ "$check_recording" == yes ]]; then
    # Roughly the middle of the warmup window (e.g. ~5s of a 10s default
    # warmup): late enough that pgbench's clients are fully connected and
    # generating waits, early enough that the sample lands entirely inside
    # data the post-warmup filter discards.
    local snapshot_delay=$((WARMUP_SECONDS / 2))
    sleep "$snapshot_delay"
    # pgbench cannot realistically have exited this early into the run
    # (it is still inside the warmup window), so this is a safety net,
    # not something the check depends on to be meaningful.
    if kill -0 "$pgbench_pid" 2>/dev/null; then
      local recorded
      recorded=$(psql_for "$prefix" -qAtc \
        "SELECT coalesce(sum(calls), 0) FROM pg_stat_wait_event_timing") ||
        die "could not read pg_stat_wait_event_timing during the warmup window; see $logfile"
      if [[ ! "$recorded" =~ ^[0-9]+$ ]] || (( recorded == 0 )); then
        die "pg_stat_wait_event_timing recorded zero calls cluster-wide during the warmup window -- capture is not actually collecting; see $logfile"
      fi
    else
      wait "$pgbench_pid" || true
      die "pgbench exited before the recording snapshot could be taken; see $logfile"
    fi
  fi

  wait "$pgbench_pid" || die "pgbench failed; see $logfile"

  local parsed
  parsed=$(python3 - "$logfile" "$WARMUP_SECONDS" "$clients" <<'PY'
import re
import statistics
import sys
path, warmup_s, clients = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
text = open(path, encoding="utf-8", errors="replace").read()
progress = [
    (float(elapsed), float(tps))
    for elapsed, tps in re.findall(
        r"^progress: ([0-9.]+) s, ([0-9.]+) tps", text, re.M)
    if float(elapsed) > warmup_s
]
if not progress:
    print("ERROR: no post-warmup progress lines in pgbench output",
          file=sys.stderr)
    raise SystemExit(1)
tps = statistics.fmean(value for _, value in progress)
latency_ms = 1000.0 * clients / tps if tps > 0 else float("nan")
print(f"tps={tps}")
print(f"latency_ms={latency_ms}")
PY
  ) || die "could not parse pgbench output; see $logfile"

  TPS_RESULT=$(printf '%s\n' "$parsed" | sed -n 's/^tps=//p')
  LATENCY_RESULT=$(printf '%s\n' "$parsed" | sed -n 's/^latency_ms=//p')
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

  start_server "$prefix" "$datadir" "$shared_buffers" "$config" "$logfile"
  assert_capture_state "$prefix" "$config"

  local iterations="" ns_per_iteration="" tps="" latency_ms="" clients=""
  local duration_field=$DURATION warmup_field=$WARMUP_SECONDS
  local freq_before freq_after
  freq_before=$(cpu_freq_median)
  record_telemetry before "$run_index" "$config" "$workload" "$repetition" "$freq_before"

  local check_recording=no
  capture_should_be_active "$config" && check_recording=yes

  case "$workload" in
    W1)
      psql_for "$prefix" -qAtc \
        "CREATE EXTENSION test_wait_primitive" >/dev/null ||
        die "CREATE EXTENSION test_wait_primitive failed"
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
        local combined recorded
        combined=$(psql_for "$prefix" -qAt -F ',' -c \
          "WITH m AS (SELECT test_wait_primitive_latch_set($W1_ITERATIONS) AS ns)
           SELECT m.ns, (SELECT coalesce(sum(calls), 0)
                         FROM pg_stat_get_wait_event_timing(pg_backend_pid()))
           FROM m") ||
          die "W1 measured call failed"
        ns_per_iteration=${combined%%,*}
        recorded=${combined##*,}
        if [[ ! "$recorded" =~ ^[0-9]+$ ]] || (( recorded == 0 )); then
          die "pg_stat_get_wait_event_timing recorded zero calls for this backend after the W1 measured call -- capture is not actually collecting"
        fi
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
      psql_for "$prefix" -qAtc \
        "CREATE EXTENSION test_wait_primitive" >/dev/null ||
        die "CREATE EXTENSION test_wait_primitive failed"
      local w3_sql="$RESULTS/w3-short-lwlock.sql"
      [[ -f "$w3_sql" ]] || echo "SELECT test_wait_primitive_lwlock_contention(100);" >"$w3_sql"
      clients=8
      run_pgbench_measured "$prefix" 8 8 "$repetition" \
        "$LOG_DIR/pgbench-$run_index.log" "$check_recording" -f "$w3_sql"
      tps=$TPS_RESULT
      latency_ms=$LATENCY_RESULT
      ;;
    W4)
      PGHOST="$SOCKET_DIR" PGPORT="$PORT" PGUSER="$DBUSER" \
        run_from_prefix "$prefix" "$prefix/bin/pgbench" -i -q \
        --unlogged-tables -s "$PGBENCH_SCALE" postgres \
        >"$LOG_DIR/pgbench-init-$run_index.log" 2>&1 ||
        die "pgbench -i failed for W4; see $LOG_DIR/pgbench-init-$run_index.log"
      # The scale-100 dataset fits comfortably in a 4 GB shared_buffers, so
      # warm every heap page before timing -- otherwise the first seconds
      # would be measuring cold-cache I/O, not the steady-state read cost
      # this workload is meant to represent.
      psql_for "$prefix" -qAtc "SELECT count(*) FROM pgbench_accounts" >/dev/null ||
        die "warm-up scan of pgbench_accounts failed"
      clients=16
      run_pgbench_measured "$prefix" 16 8 "$repetition" \
        "$LOG_DIR/pgbench-$run_index.log" "$check_recording" -S
      tps=$TPS_RESULT
      latency_ms=$LATENCY_RESULT
      ;;
    W6c)
      PGHOST="$SOCKET_DIR" PGPORT="$PORT" PGUSER="$DBUSER" \
        run_from_prefix "$prefix" "$prefix/bin/pgbench" -i -q \
        --unlogged-tables -s "$PGBENCH_SCALE" postgres \
        >"$LOG_DIR/pgbench-init-$run_index.log" 2>&1 ||
        die "pgbench -i failed for W6c; see $LOG_DIR/pgbench-init-$run_index.log"
      # Deliberately NOT pre-warmed: the scale-100 dataset does not fit in
      # a 32 MB shared_buffers, and the eviction pressure that results is
      # the point of this workload.
      clients=32
      run_pgbench_measured "$prefix" 32 8 "$repetition" \
        "$LOG_DIR/pgbench-$run_index.log" "$check_recording" -S
      tps=$TPS_RESULT
      latency_ms=$LATENCY_RESULT
      ;;
    *)
      die "unknown workload: $workload"
      ;;
  esac

  freq_after=$(cpu_freq_median)
  record_telemetry after "$run_index" "$config" "$workload" "$repetition" "$freq_after"

  stop_server
  rm -rf -- "$datadir"

  csv_row "$run_index" "$SEED" "$ts_start" "$config" "$build" "$workload" \
    "$repetition" "$shared_buffers" "$clients" "$duration_field" "$warmup_field" \
    "$iterations" "$ns_per_iteration" "$tps" "$latency_ms" \
    "$freq_before" "$freq_after" "$SERVER_CPUS" "$PGBENCH_CPUS" \
    "logs/server-$run_index.log"

  CURRENT_CELL=""
}

# ---------------------------------------------------------------------------
# Build the randomized schedule for the whole matrix (not grouped by
# configuration or by workload), with a seed that is printed and saved so
# the exact order can be reproduced.
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

cells = [
    (config, workload, repetition)
    for config in configs
    for workload in workloads
    for repetition in range(1, runs + 1)
]
random.Random(seed).shuffle(cells)

with open(path, "x", encoding="utf-8") as f:
    f.write("run_index,config,workload,repetition\n")
    for i, (config, workload, repetition) in enumerate(cells, 1):
        f.write(f"{i},{config},{workload},{repetition}\n")
PY

python3 - "$PROTOCOL" "$SEED" "$RUNS" "$DURATION" "$WARMUP_SECONDS" \
  "$PGBENCH_SCALE" "$W1_ITERATIONS" "$MODULE_NAME" "$GUC_CAPTURE" \
  "$GUC_MAX_TRANCHES" "$GUC_TRACE_RING_SIZE" "$SERVER_CPUS" "$PGBENCH_CPUS" \
  "${CONFIGS[@]}" -- "${WORKLOADS[@]}" <<'PY'
import json
import sys

args = sys.argv[1:]
(out, seed, runs, duration, warmup, scale, w1_iterations, module,
 guc_capture, guc_max_tranches, guc_trace_ring_size, server_cpus,
 pgbench_cpus) = args[:13]
rest = args[13:]
sep = rest.index("--")
configs, workloads = rest[:sep], rest[sep + 1:]

data = {
    "seed": int(seed),
    "runs_per_cell": int(runs),
    "duration_seconds": int(duration),
    "warmup_seconds": int(warmup),
    "pgbench_scale": int(scale),
    "w1_iterations": int(w1_iterations),
    "module_name": module,
    "guc_capture": guc_capture,
    "guc_max_tranches": guc_max_tranches,
    "guc_trace_ring_size": guc_trace_ring_size,
    "server_cpus": server_cpus or None,
    "pgbench_cpus": pgbench_cpus or None,
    "cpu_pinning": "enabled (taskset)" if (server_cpus or pgbench_cpus) else "unpinned (default)",
    "configs": configs,
    "workloads": workloads,
    "schedule": "one global random permutation of config x workload x repetition, not blocked by configuration or workload",
}
with open(out, "x", encoding="utf-8") as f:
    json.dump(data, f, indent=2, sort_keys=True)
    f.write("\n")
PY

log "Schedule written: $SCHEDULE_CSV ($TOTAL_CELLS cells)"
log "Protocol recorded: $PROTOCOL"
log "Starting the matrix. Expect roughly 2.5-3.5 hours."
log ""

# ---------------------------------------------------------------------------
# Main loop.
# ---------------------------------------------------------------------------
mapfile -t SCHEDULE_LINES < <(tail -n +2 "$SCHEDULE_CSV")
[[ ${#SCHEDULE_LINES[@]} -eq "$TOTAL_CELLS" ]] ||
  die "schedule has ${#SCHEDULE_LINES[@]} rows, expected $TOTAL_CELLS"

START_EPOCH=$(date +%s)
n=0
for line in "${SCHEDULE_LINES[@]}"; do
  IFS=',' read -r run_index config workload repetition <<<"$line"
  n=$((n + 1))
  cell_start=$(date +%s)

  log "[$n/$TOTAL_CELLS] config=$config workload=$workload repetition=$repetition"
  run_cell "$run_index" "$config" "$workload" "$repetition"

  cell_elapsed=$(( $(date +%s) - cell_start ))
  total_elapsed=$(( $(date +%s) - START_EPOCH ))
  if (( n > 0 )); then
    remaining=$(( total_elapsed * (TOTAL_CELLS - n) / n ))
    log "  done in ${cell_elapsed}s | elapsed $((total_elapsed / 60))m | ETA $((remaining / 60))m"
  fi
done

log ""
log "Matrix complete: $TOTAL_CELLS/$TOTAL_CELLS cells."
log "Raw results:   $CSV"
log "Server logs:   $LOG_DIR"
log "Schedule:      $SCHEDULE_CSV"
log "Seed:          $SEED"
log ""
log "Next step: ./03-collect.sh"
