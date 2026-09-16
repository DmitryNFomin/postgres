#!/usr/bin/env bash
# Run 16 baseline-only W6c cells as the original non-root executor.
set -Eeuo pipefail
export LC_ALL=C
umask 077

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
KIT_DIR=${1:?usage: run-worker.sh KIT_DIR OUTPUT_DIR}
OUT_DIR=${2:?usage: run-worker.sh KIT_DIR OUTPUT_DIR}
EXPECTED_BUILD_MANIFEST_SHA256=97ddbfdbf1c43e5b28cc5a25ab491cd98db0fe6839fb48e7984b28a877e86e4b
PREFIX_A="$KIT_DIR/work/install/base-a"
PREFIX_B="$KIT_DIR/work/install/base-b"
AFFINITY_HELPER="$KIT_DIR/cpu_affinity.py"
HOST_CHECK="$KIT_DIR/host-check.json"
PORT=55472
SOCKET_DIR="$OUT_DIR/sock"
DATA_ROOT="$OUT_DIR/data"
LOG_DIR="$OUT_DIR/logs"
RESULTS_CSV="$OUT_DIR/results.csv"
SCHEDULE="$OUT_DIR/schedule.csv"
EVENTS="$OUT_DIR/events.jsonl"
SERVER_CPUS=""
PGBENCH_CPUS=""
RUNS=8
WARMUP_SECONDS=10
DURATION=30
TOTAL_SECONDS=$((WARMUP_SECONDS + DURATION))
SCALE=100
active_prefix=""
active_datadir=""
active_pgbench_pid=""
POSTMASTER_PID=""
POSTMASTER_PGID=""
CURRENT_CELL=""
PYTHON_BIN=${PYTHON_BIN:-python3}
EXECUTOR_USER=""

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }
python_run() { "$PYTHON_BIN" -I "$@"; }
die() {
  echo "run-worker.sh: ERROR: $*" >&2
  [[ -z "$CURRENT_CELL" ]] || echo "cell: $CURRENT_CELL" >&2
  stop_client >/dev/null 2>&1 || true
  force_stop_server >/dev/null 2>&1 || true
  exit 1
}
on_exit() {
  stop_client >/dev/null 2>&1 || true
  force_stop_server >/dev/null 2>&1 || true
}
trap on_exit EXIT
trap 'die "unexpected failure at line $LINENO"' ERR

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
    PGUSER="$EXECUTOR_USER" "$@"
}

psql_for() {
  local prefix=$1
  shift
  PGHOST="$SOCKET_DIR" PGPORT="$PORT" PGUSER="$EXECUTOR_USER" \
    run_from_prefix "$prefix" "$prefix/bin/psql" \
    -X -v ON_ERROR_STOP=1 -d postgres "$@"
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
  POSTMASTER_PID=""
  POSTMASTER_PGID=""
}

postgres_group_is_owned() {
  local pgid=$1 comm found=0
  while read -r comm; do
    [[ -n "$comm" ]] || continue
    found=1
    case "$comm" in
      postgres|postmaster) ;;
      *) return 1 ;;
    esac
  done < <(ps -o comm= --pgrp "$pgid" 2>/dev/null)
  [[ "$found" -eq 1 ]]
}

process_group_running() {
  ps -o stat= --pgrp "$1" 2>/dev/null |
    awk '$1 !~ /^Z/ { found = 1 } END { exit !found }'
}

force_stop_server() {
  if [[ -z "$active_prefix" || -z "$active_datadir" ]]; then
    return 0
  fi
  if run_from_prefix "$active_prefix" "$active_prefix/bin/pg_ctl" \
      -D "$active_datadir" -m immediate -w stop; then
    active_prefix=""
    active_datadir=""
    POSTMASTER_PID=""
    POSTMASTER_PGID=""
    return 0
  fi
  if [[ "$POSTMASTER_PID" =~ ^[0-9]+$ &&
        "$POSTMASTER_PGID" == "$POSTMASTER_PID" &&
        "$POSTMASTER_PGID" =~ ^[0-9]+$ ]] &&
     postgres_group_is_owned "$POSTMASTER_PGID"; then
    kill -TERM -- "-$POSTMASTER_PGID" 2>/dev/null || true
    local attempt
    for ((attempt = 0; attempt < 30; attempt++)); do
      process_group_running "$POSTMASTER_PGID" || break
      sleep 1
    done
    if process_group_running "$POSTMASTER_PGID"; then
      kill -KILL -- "-$POSTMASTER_PGID" 2>/dev/null || true
      for ((attempt = 0; attempt < 5; attempt++)); do
        process_group_running "$POSTMASTER_PGID" || break
        sleep 1
      done
    fi
    process_group_running "$POSTMASTER_PGID" && return 1
  else
    return 1
  fi
  active_prefix=""
  active_datadir=""
  POSTMASTER_PID=""
  POSTMASTER_PGID=""
  return 0
}

stop_server_checked() {
  local context=$1
  stop_server || die "server shutdown failed after $context"
}

write_event() {
  local phase=$1 run_index=$2 config=$3 repetition=$4
  local timestamp_ns
  timestamp_ns=$(date +%s%N)
  [[ "$timestamp_ns" =~ ^[0-9]{19}$ ]] ||
    die "GNU date did not return nanosecond epoch time"
  printf \
    '{"timestamp_ns":%s,"phase":"%s","run_index":%s,"config":"%s","repetition":%s}\n' \
    "$timestamp_ns" "$phase" "$run_index" "$config" "$repetition" \
    >>"$EVENTS"
  EVENT_TIMESTAMP_NS=$timestamp_ns
}

capture_numa_maps() {
  local postmaster_pid=$1 phase=$2 run_index=$3
  local output="$LOG_DIR/numa-$phase-$run_index.txt"
  {
    echo "postmaster_pid=$postmaster_pid"
    for pid in "$postmaster_pid" $(pgrep -P "$postmaster_pid" || true); do
      [[ -r "/proc/$pid/numa_maps" ]] || continue
      echo ""
      echo "== pid $pid =="
      cat "/proc/$pid/numa_maps"
    done
  } >"$output"
}

init_cluster() {
  local datadir=$1 logfile=$2
  run_from_prefix "$PREFIX_A" "$PREFIX_A/bin/initdb" \
    -D "$datadir" --no-sync --no-locale -E UTF8 \
    --auth-local=trust --auth-host=trust --username="$EXECUTOR_USER" \
    >"$logfile.initdb" 2>&1 ||
    die "initdb failed; see $logfile.initdb"
  {
    echo "listen_addresses = ''"
    echo "unix_socket_directories = '$SOCKET_DIR'"
    echo "unix_socket_permissions = 0700"
    echo "max_connections = 100"
    echo "shared_buffers = '32MB'"
    echo "autovacuum = off"
    echo "checkpoint_timeout = '1h'"
    echo "max_wal_size = '8GB'"
  } >>"$datadir/postgresql.conf"
}

start_server() {
  local prefix=$1 datadir=$2 logfile=$3
  active_prefix=$prefix
  active_datadir=$datadir
  run_from_prefix "$prefix" taskset -c "$SERVER_CPUS" \
    "$prefix/bin/pg_ctl" \
    -D "$datadir" -l "$logfile" \
    -o "-p $PORT -k $SOCKET_DIR" -w start >/dev/null ||
    die "server failed to start; see $logfile"
  local postmaster_pid postmaster_pgid
  read -r postmaster_pid <"$datadir/postmaster.pid"
  [[ "$postmaster_pid" =~ ^[0-9]+$ ]] ||
    die "could not read postmaster pid"
  python_run "$AFFINITY_HELPER" \
    verify-pid "$postmaster_pid" "$SERVER_CPUS" ||
    die "postmaster CPU affinity differs"
  postmaster_pgid=$(ps -o pgid= -p "$postmaster_pid" | awk '{print $1}')
  [[ "$postmaster_pgid" == "$postmaster_pid" ]] ||
    die "could not establish the postmaster process-group identity"
  POSTMASTER_PID=$postmaster_pid
  POSTMASTER_PGID=$postmaster_pgid
}

verify_baseline_state() {
  local prefix=$1
  local preload
  preload=$(psql_for "$prefix" -qAtc "SHOW shared_preload_libraries")
  [[ -z "$preload" ]] ||
    die "shared_preload_libraries is unexpectedly '$preload'"
  if psql_for "$prefix" -qAtc \
      "SHOW pg_wait_event_tracing.capture" >/dev/null 2>&1; then
    die "wait-event tracing GUC unexpectedly exists"
  fi
}

parse_pgbench() {
  local logfile=$1
  python_run - "$logfile" "$WARMUP_SECONDS" "$DURATION" <<'PY'
import re
import statistics
import sys

path, warmup_s, duration_s = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
text = open(path, encoding="utf-8", errors="replace").read()
matches = re.findall(
    r"^progress: ([0-9.]+) s, ([0-9.]+) tps, "
    r"lat ([0-9.]+) ms stddev [0-9.]+, ([0-9]+) failed",
    text,
    re.M,
)
final_failures = re.findall(
    r"^number of failed transactions: ([0-9]+) ", text, re.M
)
if len(final_failures) != 1 or int(final_failures[0]) != 0:
    raise SystemExit("pgbench did not report exactly zero failures")
progress = [
    (float(elapsed), float(tps), float(latency), int(failures))
    for elapsed, tps, latency, failures in matches
]
selected = []
warmup = []
previous = 0.0
for elapsed, tps, latency, failures in progress:
    interval = elapsed - previous
    if interval <= 0:
        raise SystemExit("non-increasing pgbench timestamps")
    if previous >= warmup_s:
        selected.append((interval, tps, latency, failures))
    elif previous >= 1:
        warmup.append(tps)
    previous = elapsed
if not selected or any(item[3] for item in selected):
    raise SystemExit("missing samples or failed measured transactions")
measured_interval = sum(item[0] for item in selected)
if not duration_s - 2 <= measured_interval <= duration_s + 1:
    raise SystemExit("unexpected measured interval")
transactions = sum(interval * tps for interval, tps, _, _ in selected)
tps = transactions / measured_interval
latency = sum(
    interval * rate * latency
    for interval, rate, latency, _ in selected
) / transactions
rates = [item[1] for item in selected]
within_cv = statistics.stdev(rates) / statistics.mean(rates) * 100
warmup_shift = (
    statistics.mean(rates) / statistics.mean(warmup) - 1
) * 100
print("\t".join((
    "{:.12g}".format(tps),
    "{:.12g}".format(latency),
    str(len(selected)),
    "{:.12g}".format(measured_interval),
    "{:.12g}".format(within_cv),
    "{:.12g}".format(warmup_shift),
)))
PY
}

write_result() {
  local mode=$1
  shift
  python_run - "$mode" "$RESULTS_CSV" "$@" <<'PY'
import csv
import sys

mode, path = sys.argv[1:3]
fields = (
    "run_index", "timestamp_utc", "config", "repetition", "tps",
    "latency_ms", "measurement_samples", "measurement_interval_s",
    "within_cell_cv_percent", "warmup_to_measurement_percent",
    "pgbench_cpu_percent", "pgbench_capacity_fraction",
    "start_ns", "measurement_start_ns", "end_ns", "postmaster_pid",
    "server_cpus",
    "pgbench_cpus", "pgbench_log", "server_log",
)
if mode == "initialize":
    if len(sys.argv) != 3:
        raise SystemExit("unexpected result initialization arguments")
    with open(path, "x", newline="", encoding="utf-8") as stream:
        csv.DictWriter(stream, fieldnames=fields).writeheader()
elif mode == "append":
    values = sys.argv[3:]
    if len(values) != len(fields):
        raise SystemExit("result field count differs")
    with open(path, "a", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writerow(dict(zip(fields, values)))
else:
    raise SystemExit("unknown result mode")
PY
}

run_cell() {
  local run_index=$1 config=$2 repetition=$3
  CURRENT_CELL="run_index=$run_index config=$config repetition=$repetition"
  local prefix datadir logfile setup_log pgbench_log
  case "$config" in
    baseline-a) prefix=$PREFIX_A ;;
    baseline-b) prefix=$PREFIX_B ;;
    *) die "unknown configuration: $config" ;;
  esac
  datadir="$DATA_ROOT/cell-$run_index"
  logfile="$LOG_DIR/server-$run_index.log"
  setup_log="$LOG_DIR/server-setup-$run_index.log"
  pgbench_log="$LOG_DIR/pgbench-$run_index.log"
  [[ ! -e "$datadir" ]] || die "data directory already exists: $datadir"

  init_cluster "$datadir" "$logfile"
  start_server "$PREFIX_A" "$datadir" "$setup_log"
  PGHOST="$SOCKET_DIR" PGPORT="$PORT" PGUSER="$EXECUTOR_USER" \
    run_from_prefix "$PREFIX_A" taskset -c "$PGBENCH_CPUS" \
    "$PREFIX_A/bin/pgbench" -i -q --unlogged-tables \
    -s "$SCALE" postgres >"$LOG_DIR/pgbench-init-$run_index.log" 2>&1 ||
    die "pgbench initialization failed"
  psql_for "$PREFIX_A" -qAtc "CHECKPOINT" >/dev/null ||
    die "setup checkpoint failed"
  stop_server_checked "dataset setup"

  start_server "$prefix" "$datadir" "$logfile"
  verify_baseline_state "$prefix"
  psql_for "$prefix" -qAtc "CHECKPOINT" >/dev/null ||
    die "pre-measurement checkpoint failed"
  sleep 5
  capture_numa_maps "$POSTMASTER_PID" before "$run_index"

  local timestamp_utc start_ns measurement_start_ns end_ns
  local measured_postmaster_pid
  timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  write_event pgbench_start "$run_index" "$config" "$repetition"
  start_ns=$EVENT_TIMESTAMP_NS
  run_pgbench_from_prefix "$prefix" "w6-power-$run_index" \
    taskset -c "$PGBENCH_CPUS" \
    "$prefix/bin/pgbench" -n -c 32 -j 8 -T "$TOTAL_SECONDS" -P 1 \
    --random-seed="$repetition" -S postgres >"$pgbench_log" 2>&1 &
  active_pgbench_pid=$!
  sleep 5
  kill -0 "$active_pgbench_pid" 2>/dev/null ||
    die "pgbench exited during warmup"
  python_run "$AFFINITY_HELPER" \
    verify-pid "$active_pgbench_pid" "$PGBENCH_CPUS" ||
    die "pgbench CPU affinity differs"
  capture_numa_maps "$POSTMASTER_PID" during "$run_index"
  local pgbench_cpu
  pgbench_cpu=$(ps -o pcpu= -p "$active_pgbench_pid" | awk '{print $1}')
  [[ "$pgbench_cpu" =~ ^[0-9]+([.][0-9]+)?$ ]] ||
    die "could not read pgbench CPU utilization"
  local measurement_target_ns=$(
    python_run - "$start_ns" "$WARMUP_SECONDS" <<'PY'
import sys
print(int(sys.argv[1]) + int(sys.argv[2]) * 1_000_000_000)
PY
  )
  python_run - "$measurement_target_ns" <<'PY'
import sys
import time
delay = (int(sys.argv[1]) - time.time_ns()) / 1_000_000_000
if delay > 0:
    time.sleep(delay)
PY
  write_event measurement_start "$run_index" "$config" "$repetition"
  measurement_start_ns=$EVENT_TIMESTAMP_NS
  wait "$active_pgbench_pid" || {
    active_pgbench_pid=""
    die "pgbench failed; see $pgbench_log"
  }
  active_pgbench_pid=""
  write_event pgbench_end "$run_index" "$config" "$repetition"
  end_ns=$EVENT_TIMESTAMP_NS
  capture_numa_maps "$POSTMASTER_PID" after "$run_index"
  measured_postmaster_pid=$POSTMASTER_PID

  local parsed tps latency samples interval within_cv warmup_shift
  parsed=$(parse_pgbench "$pgbench_log") ||
    die "could not parse $pgbench_log"
  IFS=$'\t' read -r \
    tps latency samples interval within_cv warmup_shift <<<"$parsed"
  [[ -n "$tps" && -n "$latency" && -n "$samples" &&
     -n "$interval" && -n "$within_cv" && -n "$warmup_shift" ]] ||
    die "parsed pgbench metrics are incomplete"
  local capacity
  capacity=$(python_run - "$pgbench_cpu" <<'PY'
import sys
print("{:.12g}".format(float(sys.argv[1]) / 800.0))
PY
  )
  python_run - "$capacity" <<'PY' ||
import sys
if float(sys.argv[1]) >= 0.9:
    raise SystemExit("pgbench is saturated")
PY
    die "pgbench driver saturation invalidates the diagnostic"

  stop_server_checked "$CURRENT_CELL"
  rm -rf -- "$datadir"
  write_result append \
    "$run_index" "$timestamp_utc" "$config" "$repetition" "$tps" \
    "$latency" "$samples" "$interval" "$within_cv" "$warmup_shift" \
    "$pgbench_cpu" "$capacity" "$start_ns" "$measurement_start_ns" \
    "$end_ns" "$measured_postmaster_pid" "$SERVER_CPUS" "$PGBENCH_CPUS" \
    "logs/pgbench-$run_index.log" "logs/server-$run_index.log"
  log "completed $CURRENT_CELL tps=$tps within_cv=$within_cv%"
  CURRENT_CELL=""
}

[[ "$(id -u)" -ne 0 ]] || die "worker must not run as root"
for tool in "$PYTHON_BIN" taskset sha256sum awk ps pgrep id hostname; do
  command -v "$tool" >/dev/null 2>&1 || die "missing tool: $tool"
done
[[ -d "$OUT_DIR" && -w "$OUT_DIR" ]] ||
  die "output directory is missing or not writable"
EXECUTOR_USER=$(id -un)
[[ -x "$PREFIX_A/bin/postgres" && -x "$PREFIX_B/bin/postgres" ]] ||
  die "verified r3 baseline installs are missing"
[[ "$(sha256sum "$KIT_DIR/work/manifest.json" | awk '{print $1}')" \
   == "$EXPECTED_BUILD_MANIFEST_SHA256" ]] ||
  die "r3 build manifest differs from the diagnosed build"
python_run "$AFFINITY_HELPER" verify-live ||
  die "live topology differs from the r3 protocol"
affinity_constants=$(python_run "$AFFINITY_HELPER" constants-tsv) ||
  die "could not read canonical CPU masks"
IFS=$'\t' read -r SERVER_CPUS PGBENCH_CPUS _ \
  <<<"$affinity_constants"
[[ "$SERVER_CPUS" == 1-63:2 && "$PGBENCH_CPUS" == 0-14:2 ]] ||
  die "canonical CPU masks differ from the diagnostic protocol"
python_run "$AFFINITY_HELPER" \
  verify-host-report "$HOST_CHECK" "$(hostname)" ||
  die "host report is not valid for this host"
python_run - "$KIT_DIR/work/manifest.json" "$PREFIX_A" "$PREFIX_B" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

manifest_path, prefix_a, prefix_b = sys.argv[1:]
manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
builds = {item["name"]: item for item in manifest["builds"]}
for name, prefix_text in (
    ("baseline-a", prefix_a),
    ("baseline-b", prefix_b),
):
    prefix = Path(prefix_text)
    expected = builds[name]["install_tree"]
    actual_paths = {
        path.relative_to(prefix).as_posix()
        for path in prefix.rglob("*")
        if path.is_file() or path.is_symlink()
    }
    if actual_paths != set(expected):
        raise SystemExit("{} install-tree file set differs".format(name))
    for relative, item in expected.items():
        path = prefix / relative
        if item["type"] == "file":
            actual = hashlib.sha256(path.read_bytes()).hexdigest()
            if actual != item["sha256"]:
                raise SystemExit("{} {} differs".format(name, relative))
        elif item["type"] == "symlink":
            if not path.is_symlink() or path.readlink().as_posix() != item[
                "target"
            ]:
                raise SystemExit("{} {} symlink differs".format(
                    name, relative
                ))
        else:
            raise SystemExit("{} {} has unknown type".format(name, relative))
if builds["baseline-a"]["install_tree"] != builds["baseline-b"]["install_tree"]:
    raise SystemExit("baseline installation trees differ")
PY

mkdir -p "$SOCKET_DIR" "$DATA_ROOT" "$LOG_DIR"
chmod 700 "$SOCKET_DIR" "$DATA_ROOT" "$LOG_DIR"
: >"$EVENTS"
python_run - "$SCHEDULE" "$RUNS" >"$OUT_DIR/seed.txt" <<'PY'
import csv
import random
import secrets
import sys

path, repetitions = sys.argv[1], int(sys.argv[2])
seed = secrets.randbits(64)
randomizer = random.Random(seed)
rows = []
for repetition in range(1, repetitions + 1):
    configs = ["baseline-a", "baseline-b"]
    randomizer.shuffle(configs)
    rows.extend((config, repetition) for config in configs)
with open(path, "x", newline="", encoding="utf-8") as stream:
    writer = csv.writer(stream)
    writer.writerow(("run_index", "config", "repetition"))
    for index, (config, repetition) in enumerate(rows, 1):
        writer.writerow((index, config, repetition))
print(seed)
PY
write_result initialize

mapfile -t cells < <(tail -n +2 "$SCHEDULE")
[[ ${#cells[@]} -eq $((RUNS * 2)) ]] || die "schedule is incomplete"
for line in "${cells[@]}"; do
  IFS=, read -r run_index config repetition <<<"$line"
  run_cell "$run_index" "$config" "$repetition"
done
write_event worker_complete 0 complete 0
trap - EXIT ERR
on_exit
log "W6c diagnostic worker completed all $((RUNS * 2)) cells"
