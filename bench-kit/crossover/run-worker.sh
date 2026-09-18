#!/usr/bin/env bash
# Run the fixed persistent-backend W6c crossover as the original executor.
set -Eeuo pipefail
export LC_ALL=C
umask 077

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
KIT_DIR=${1:?usage: run-worker.sh KIT_DIR OUTPUT_DIR SOCKET_DIR [full|smoke]}
OUT_DIR=${2:?usage: run-worker.sh KIT_DIR OUTPUT_DIR SOCKET_DIR [full|smoke]}
SOCKET_DIR=${3:?usage: run-worker.sh KIT_DIR OUTPUT_DIR SOCKET_DIR [full|smoke]}
RUN_MODE=${4:-full}
[[ "$RUN_MODE" == full || "$RUN_MODE" == smoke ]] ||
  { echo "invalid worker mode: $RUN_MODE" >&2; exit 1; }
# brief-v11-wpc-kit.md: patched installation only, no v9/v10 pair. The kit's
# own build manifest (work/manifest.json) and package manifest are trusted
# by hash-binding them into protocol.json (see protocol.py), not by pinning
# to one hardcoded expected digest -- the real package's hashes depend on
# the commits the coordinator fills into sources.conf.
PREFIX_A="$KIT_DIR/work/install/base-a"
PREFIX_PATCHED="$KIT_DIR/work/install/patchd"
AFFINITY_HELPER="$KIT_DIR/cpu_affinity.py"
HOST_CHECK="$KIT_DIR/host-check.json"
PYTHON_BIN=${PYTHON_BIN:-python3}
PORT=55473
DATA_ROOT="$OUT_DIR/data"
LOG_DIR="$OUT_DIR/logs"
BACKEND_DIR="$OUT_DIR/backend-pids"
SCHEDULE="$OUT_DIR/schedule.csv"
RESULTS="$OUT_DIR/results.csv"
PROOFS="$OUT_DIR/mode-proofs.csv"
EVENTS="$OUT_DIR/events.jsonl"
PROTOCOL="$OUT_DIR/protocol.json"
SEED_FILE="$OUT_DIR/seed.txt"
SESSIONS=16
BLOCK_SECONDS=30
SETTLE_SECONDS=3
INITIAL_WARMUP_SECONDS=10
if [[ "$RUN_MODE" == smoke ]]; then
  BLOCK_SECONDS=1
  SETTLE_SECONDS=0
  INITIAL_WARMUP_SECONDS=2
fi
SERVER_CPUS=""
PGBENCH_CPUS=""
EXECUTOR_USER=""
active_prefix=""
active_datadir=""
active_pgbench_pid=""
POSTMASTER_PID=""
POSTMASTER_PGID=""
CURRENT_SESSION=""
CLOCK_TICKS_PER_SECOND=""

log() { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }
python_run() { "$PYTHON_BIN" -I "$@"; }
run_from_prefix() {
  local prefix=$1
  shift
  env LD_LIBRARY_PATH="$prefix/lib:${LD_LIBRARY_PATH:-}" "$@"
}
psql_for() {
  local prefix=$1
  shift
  PGHOST="$SOCKET_DIR" PGPORT="$PORT" PGUSER="$EXECUTOR_USER" \
    run_from_prefix "$prefix" "$prefix/bin/psql" \
    -X -v ON_ERROR_STOP=1 -d postgres "$@"
}
psql_control_for() {
  local prefix=$1
  shift
  PGHOST="$SOCKET_DIR" PGPORT="$PORT" PGUSER="$EXECUTOR_USER" \
    PGOPTIONS="-c pg_wait_event_tracing.capture=off" \
    run_from_prefix "$prefix" "$prefix/bin/psql" \
    -X -v ON_ERROR_STOP=1 -d postgres "$@"
}
run_pgbench_from_prefix() {
  local prefix=$1 appname=$2
  shift 2
  exec env \
    LD_LIBRARY_PATH="$prefix/lib:${LD_LIBRARY_PATH:-}" \
    PGAPPNAME="$appname" PGHOST="$SOCKET_DIR" PGPORT="$PORT" \
    PGUSER="$EXECUTOR_USER" "$@"
}
process_group_running() {
  ps -o stat= --pgrp "$1" 2>/dev/null |
    awk '$1 !~ /^Z/ { found = 1 } END { exit !found }'
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
        "$POSTMASTER_PGID" == "$POSTMASTER_PID" ]] &&
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
}
stop_server_checked() {
  local context=$1
  stop_server || die "server shutdown failed after $context"
}
on_exit() {
  stop_client >/dev/null 2>&1 || true
  force_stop_server >/dev/null 2>&1 || true
}
die() {
  echo "run-worker.sh: ERROR: $*" >&2
  [[ -z "$CURRENT_SESSION" ]] || echo "session: $CURRENT_SESSION" >&2
  on_exit
  exit 1
}
trap on_exit EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'die "unexpected failure at line $LINENO"' ERR

write_event() {
  local phase=$1 session_index=$2 block_index=$3 mode=$4
  local pgbench_cpu_ticks=${5:-}
  local pgbench_starttime_ticks=${6:-}
  local clock_ticks_per_second=${7:-}
  local timestamp_ns
  timestamp_ns=$(date +%s%N)
  [[ "$timestamp_ns" =~ ^[0-9]{19}$ ]] ||
    die "GNU date did not return nanosecond epoch time"
  if [[ -n "$pgbench_cpu_ticks" ]]; then
    printf \
      '{"timestamp_ns":%s,"phase":"%s","session_index":%s,"block_index":%s,"mode":"%s","pgbench_cpu_ticks":%s,"pgbench_starttime_ticks":%s,"clock_ticks_per_second":%s}\n' \
      "$timestamp_ns" "$phase" "$session_index" "$block_index" "$mode" \
      "$pgbench_cpu_ticks" "$pgbench_starttime_ticks" \
      "$clock_ticks_per_second" >>"$EVENTS"
  else
    printf \
      '{"timestamp_ns":%s,"phase":"%s","session_index":%s,"block_index":%s,"mode":"%s"}\n' \
      "$timestamp_ns" "$phase" "$session_index" "$block_index" "$mode" \
      >>"$EVENTS"
  fi
}
pgbench_cpu_snapshot() {
  local pid=$1 stat_line rest
  IFS= read -r stat_line <"/proc/$pid/stat" || return 1
  rest=${stat_line##*) }
  set -- $rest
  [[ $# -ge 20 && ${12} =~ ^[0-9]+$ && ${13} =~ ^[0-9]+$ &&
     ${20} =~ ^[0-9]+$ ]] || return 1
  printf '%s\t%s\t%s\n' \
    "$(( ${12} + ${13} ))" "${20}" "$CLOCK_TICKS_PER_SECOND"
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
  local prefix=$1 datadir=$2 logfile=$3 preload=$4
  if [[ "$preload" == yes ]]; then
    {
      echo "shared_preload_libraries = 'pg_wait_event_tracing'"
      echo "pg_wait_event_tracing.capture = 'stats'"
      echo "pg_wait_event_tracing.max_tranches = 192"
      echo "pg_wait_event_tracing.trace_ring_size = '4MB'"
    } >>"$datadir/postgresql.conf"
  fi
  active_prefix=$prefix
  active_datadir=$datadir
  run_from_prefix "$prefix" taskset -c "$SERVER_CPUS" \
    "$prefix/bin/pg_ctl" \
    -D "$datadir" -l "$logfile" \
    -o "-p $PORT -k $SOCKET_DIR" -w start >/dev/null ||
    die "server failed to start; see $logfile"
  read -r POSTMASTER_PID <"$datadir/postmaster.pid"
  [[ "$POSTMASTER_PID" =~ ^[0-9]+$ ]] ||
    die "could not read postmaster PID"
  [[ -n "${SELFTEST_FAKE_PREFIX:-}" ]] || python_run "$AFFINITY_HELPER" \
    verify-pid "$POSTMASTER_PID" "$SERVER_CPUS" ||
    die "postmaster CPU affinity differs"
  POSTMASTER_PGID=$(ps -o pgid= -p "$POSTMASTER_PID" | awk '{print $1}')
  [[ "$POSTMASTER_PGID" == "$POSTMASTER_PID" ]] ||
    die "could not establish postmaster process-group identity"
}

backend_snapshot() {
  local prefix=$1 appname=$2 output=$3
  psql_control_for "$prefix" -qAtc \
    "SELECT pid FROM pg_stat_activity
     WHERE application_name = '$appname'
       AND backend_type = 'client backend'
     ORDER BY pid" >"$output"
  [[ "$(wc -l <"$output" | tr -d ' ')" == 32 ]] ||
    return 1
  [[ "$(sort -u "$output" | wc -l | tr -d ' ')" == 32 ]] ||
    return 1
}
assert_same_backends() {
  local prefix=$1 appname=$2 expected=$3
  local temporary="$expected.current"
  backend_snapshot "$prefix" "$appname" "$temporary" ||
    die "pgbench does not expose exactly 32 client backends"
  cmp -s "$expected" "$temporary" ||
    die "pgbench backend identities changed during the session"
  rm -f -- "$temporary"
}
capture_snapshot() {
  local prefix=$1 appname=$2
  psql_control_for "$prefix" -qAt -F ',' -c \
    "WITH clients AS MATERIALIZED
       (SELECT pid FROM pg_stat_activity
        WHERE application_name = '$appname'
          AND backend_type = 'client backend'),
     timing AS MATERIALIZED
       (SELECT t.pid, sum(t.calls)::bigint AS calls
        FROM pg_stat_wait_event_timing AS t
        JOIN clients AS c USING (pid)
        GROUP BY t.pid),
     representative AS MATERIALIZED
       (SELECT t.procnumber
        FROM pg_stat_wait_event_timing AS t
        JOIN clients AS c USING (pid)
        ORDER BY t.pid LIMIT 1)
     SELECT
       (SELECT count(*) FROM clients),
       (SELECT count(*) FROM timing WHERE calls > 0),
       coalesce((SELECT sum(calls) FROM timing), 0),
       coalesce((SELECT count(*) FROM representative AS r,
                 LATERAL pg_get_wait_event_trace(r.procnumber)
                 WHERE wait_event_type <> 'Query'), 0)"
}
set_capture_mode() {
  local prefix=$1 mode=$2 appname=$3
  psql_control_for "$prefix" -qAtc \
    "ALTER SYSTEM SET pg_wait_event_tracing.capture = '$mode'" >/dev/null ||
    die "could not write capture mode $mode"
  [[ "$(psql_control_for "$prefix" -qAtc "SELECT pg_reload_conf()")" == t ]] ||
    die "configuration reload failed for capture mode $mode"
  local attempt snapshot client_count clients_recording calls trace
  for ((attempt = 0; attempt < 100; attempt++)); do
    snapshot=$(capture_snapshot "$prefix" "$appname") ||
      die "could not inspect capture state"
    IFS=, read -r client_count clients_recording calls trace <<<"$snapshot"
    if [[ "$client_count" == 32 ]]; then
      case "$mode" in
        off)
          [[ "$clients_recording" == 0 && "$calls" == 0 &&
             "$trace" == 0 ]] && break
          ;;
        stats)
          [[ "$clients_recording" == 32 && "$calls" =~ ^[1-9][0-9]*$ &&
             "$trace" == 0 ]] && break
          ;;
        trace)
          [[ "$clients_recording" == 32 && "$calls" =~ ^[1-9][0-9]*$ &&
             "$trace" =~ ^[1-9][0-9]*$ ]] && break
          ;;
      esac
    fi
    sleep 0.1
  done
  (( attempt < 100 )) ||
    die "capture mode $mode did not reach every pgbench backend"
}
set_server_default_off() {
  local prefix=$1 attempt shown
  psql_control_for "$prefix" -qAtc \
    "ALTER SYSTEM SET pg_wait_event_tracing.capture = 'off'" >/dev/null ||
    die "could not set initial capture-off mode"
  [[ "$(psql_control_for "$prefix" -qAtc "SELECT pg_reload_conf()")" == t ]] ||
    die "initial capture-off reload failed"
  for ((attempt = 0; attempt < 100; attempt++)); do
    shown=$(psql_control_for "$prefix" -qAtc \
      "SELECT setting FROM pg_file_settings
       WHERE name = 'pg_wait_event_tracing.capture' AND applied
       ORDER BY seqno DESC LIMIT 1") ||
      die "could not inspect initial capture mode"
    [[ "$shown" == off ]] && break
    sleep 0.1
  done
  (( attempt < 100 )) || die "initial capture-off mode did not take effect"
}
append_mode_proof() {
  local session_index=$1 block_index=$2 mode=$3 prefix=$4 appname=$5
  local backends=$6 backend_digest before after
  local client_count clients_recording calls_before calls_after trace_records
  assert_same_backends "$prefix" "$appname" "$backends"
  before=$(capture_snapshot "$prefix" "$appname")
  sleep 1
  assert_same_backends "$prefix" "$appname" "$backends"
  after=$(capture_snapshot "$prefix" "$appname")
  IFS=, read -r client_count clients_recording calls_before _ <<<"$before"
  IFS=, read -r _ _ calls_after trace_records <<<"$after"
  backend_digest=$(sha256sum "$backends" | awk '{print $1}')
  case "$mode" in
    off)
      [[ "$client_count" == 32 && "$clients_recording" == 0 &&
         "$calls_before" == 0 && "$calls_after" == 0 &&
         "$trace_records" == 0 ]] ||
        die "off-mode recording proof failed"
      ;;
    stats)
      [[ "$client_count" == 32 && "$clients_recording" == 32 &&
         "$calls_before" =~ ^[1-9][0-9]*$ &&
         "$calls_after" =~ ^[1-9][0-9]*$ &&
         "$calls_after" -gt "$calls_before" &&
         "$trace_records" == 0 ]] ||
        die "stats-mode recording proof failed"
      ;;
    trace)
      [[ "$client_count" == 32 && "$clients_recording" == 32 &&
         "$calls_before" =~ ^[1-9][0-9]*$ &&
         "$calls_after" =~ ^[1-9][0-9]*$ &&
         "$calls_after" -gt "$calls_before" &&
         "$trace_records" =~ ^[1-9][0-9]*$ ]] ||
        die "trace-mode recording proof failed"
      ;;
  esac
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$session_index" "$block_index" "$mode" "$client_count" \
    "$clients_recording" "$calls_before" "$calls_after" "$trace_records" \
    "$backend_digest" >>"$PROOFS"
}

run_session() {
  local session_index=$1 sequence=$2 pgbench_seed=$3
  CURRENT_SESSION="index=$session_index sequence=$sequence"
  local prefix=$PREFIX_PATCHED
  local datadir="$DATA_ROOT/session-$session_index"
  local server_log="$LOG_DIR/server-$session_index.log"
  local pgbench_log="$LOG_DIR/pgbench-$session_index.log"
  local aggregate_prefix="$LOG_DIR/aggregate-$session_index"
  local backends="$BACKEND_DIR/session-$session_index.txt"
  local appname="wet-crossover-$session_index"
  local measured_pgbench_pid=""
  local -a backend_pids
  [[ ! -e "$datadir" ]] || die "session data directory already exists"

  # Each session creates its own neutral dataset directly (no pairing, so
  # no sharing/cloning a fixture between two sessions is needed).
  local init_log="$LOG_DIR/init-$session_index.log"
  local setup_log="$LOG_DIR/server-setup-$session_index.log"
  init_cluster "$datadir" "$init_log"
  start_server "$PREFIX_A" "$datadir" "$setup_log" no
  PGHOST="$SOCKET_DIR" PGPORT="$PORT" PGUSER="$EXECUTOR_USER" \
    run_from_prefix "$PREFIX_A" taskset -c "$PGBENCH_CPUS" \
    "$PREFIX_A/bin/pgbench" -i -q -s 100 postgres \
    >"$LOG_DIR/pgbench-init-$session_index.log" 2>&1 ||
    die "pgbench initialization failed"
  psql_for "$PREFIX_A" -qAtc "CHECKPOINT" >/dev/null ||
    die "neutral dataset checkpoint failed"
  stop_server_checked "neutral dataset setup for session $session_index"

  start_server "$prefix" "$datadir" "$server_log" yes
  [[ "$(psql_control_for "$prefix" -qAtc "SHOW shared_preload_libraries")" \
      == pg_wait_event_tracing ]] ||
    die "tracing module did not preload"
  [[ "$(psql_control_for "$prefix" -qAtc \
      "SELECT setting FROM pg_file_settings
       WHERE name = 'pg_wait_event_tracing.capture' AND applied
       ORDER BY seqno DESC LIMIT 1")" == stats ]] ||
    die "initial capture mode is not stats"
  set_server_default_off "$prefix"
  psql_control_for "$prefix" -qAtc \
    "CREATE EXTENSION pg_wait_event_tracing" >/dev/null ||
    die "could not create tracing SQL extension"
  psql_control_for "$prefix" -qAtc "CHECKPOINT" >/dev/null ||
    die "pre-measurement checkpoint failed"
  sleep 5

  run_pgbench_from_prefix "$prefix" "$appname" \
    taskset -c "$PGBENCH_CPUS" "$prefix/bin/pgbench" -n \
    -c 32 -j 8 -T 3600 -P 1 --progress-timestamp \
    --random-seed="$pgbench_seed" --failures-detailed \
    -S -l --aggregate-interval=1 \
    --log-prefix="$aggregate_prefix" postgres \
    >"$pgbench_log" 2>&1 &
  active_pgbench_pid=$!
  measured_pgbench_pid=$active_pgbench_pid
  sleep "$INITIAL_WARMUP_SECONDS"
  kill -0 "$active_pgbench_pid" 2>/dev/null ||
    die "pgbench exited during initial warmup"
  [[ -n "${SELFTEST_FAKE_PREFIX:-}" ]] || python_run "$AFFINITY_HELPER" \
    verify-pid "$active_pgbench_pid" "$PGBENCH_CPUS" ||
    die "pgbench CPU affinity differs"
  backend_snapshot "$prefix" "$appname" "$backends" ||
    die "pgbench did not create exactly 32 persistent backends"
  mapfile -t backend_pids <"$backends"
  python_run "$SCRIPT_DIR/verify_affinity.py" \
    "$SERVER_CPUS" "${backend_pids[@]}" ||
    die "a pgbench backend CPU affinity differs"

  local -a modes
  if [[ "$sequence" == A ]]; then
    modes=(off stats stats off off trace trace off)
  elif [[ "$sequence" == B ]]; then
    modes=(off trace trace off off stats stats off)
  else
    die "unknown sequence $sequence"
  fi

  local block_index mode pgbench_cpu_snapshot_start
  local pgbench_cpu_snapshot_end start_ticks starttime_ticks clock_ticks
  local end_ticks end_starttime_ticks end_clock_ticks
  for block_index in {1..8}; do
    mode=${modes[$((block_index - 1))]}
    write_event transition_start "$session_index" "$block_index" "$mode"
    set_capture_mode "$prefix" "$mode" "$appname"
    append_mode_proof \
      "$session_index" "$block_index" "$mode" "$prefix" "$appname" \
      "$backends"
    sleep "$SETTLE_SECONDS"
    assert_same_backends "$prefix" "$appname" "$backends"
    pgbench_cpu_snapshot_start=$(pgbench_cpu_snapshot "$active_pgbench_pid") ||
      die "could not read pgbench CPU counters"
    IFS=$'\t' read -r start_ticks starttime_ticks clock_ticks \
      <<<"$pgbench_cpu_snapshot_start"
    write_event measurement_start \
      "$session_index" "$block_index" "$mode" \
      "$start_ticks" "$starttime_ticks" "$clock_ticks"
    sleep "$BLOCK_SECONDS"
    pgbench_cpu_snapshot_end=$(pgbench_cpu_snapshot "$active_pgbench_pid") ||
      die "could not read ending pgbench CPU counters"
    IFS=$'\t' read -r end_ticks end_starttime_ticks end_clock_ticks \
      <<<"$pgbench_cpu_snapshot_end"
    write_event measurement_end \
      "$session_index" "$block_index" "$mode" \
      "$end_ticks" "$end_starttime_ticks" "$end_clock_ticks"
    assert_same_backends "$prefix" "$appname" "$backends"
    log "session $session_index/$SESSIONS block $block_index/8 mode=$mode"
  done

  kill -ALRM "$active_pgbench_pid" 2>/dev/null ||
    die "could not stop pgbench with its duration signal"
  wait "$active_pgbench_pid" ||
    die "pgbench failed; see $pgbench_log"
  active_pgbench_pid=""
  grep -q '^number of failed transactions: 0 ' "$pgbench_log" ||
    die "pgbench did not report exactly zero failed transactions"

  if [[ "$RUN_MODE" == smoke ]]; then
    python_run - "$SCRIPT_DIR" "$aggregate_prefix" \
      "$measured_pgbench_pid" <<'PY'
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import extract_blocks

rows = extract_blocks.read_aggregate_logs(
    Path(sys.argv[2]),
    int(sys.argv[3]),
)
if not rows:
    raise RuntimeError("smoke aggregate logs contain no complete seconds")
PY
    stop_server_checked "$CURRENT_SESSION smoke"
    rm -rf -- "$datadir"
    CURRENT_SESSION=""
    log "completed smoke session sequence=$sequence"
    return
  fi

  local metadata
  metadata=$(python_run - "$session_index" \
    "$sequence" "$pgbench_seed" "$POSTMASTER_PID" \
    "$measured_pgbench_pid" "$SERVER_CPUS" "$PGBENCH_CPUS" \
    "logs/aggregate-$session_index" "logs/server-$session_index.log" <<'PY'
import json
import sys
keys = (
    "session_index", "sequence",
    "pgbench_seed", "postmaster_pid", "pgbench_pid", "server_cpus",
    "pgbench_cpus", "aggregate_log_prefix", "server_log",
)
print(json.dumps(dict(zip(keys, sys.argv[1:]))))
PY
  )
  python_run "$SCRIPT_DIR/extract_blocks.py" \
    "$RESULTS" "$EVENTS" "$aggregate_prefix" "$backends" "$metadata" ||
    die "could not extract session measurements"
  stop_server_checked "$CURRENT_SESSION"
  rm -rf -- "$datadir"
  CURRENT_SESSION=""
  log "completed persistent-backend session $session_index/$SESSIONS"
}

[[ "$(id -u)" -ne 0 ]] || die "worker must not run as root"
for tool in \
  "$PYTHON_BIN" taskset sha256sum awk ps pgrep id hostname sort wc tr \
  cmp date grep getconf; do
  command -v "$tool" >/dev/null 2>&1 || die "missing tool: $tool"
done
[[ -d "$OUT_DIR" && -w "$OUT_DIR" ]] ||
  die "output directory is missing or not writable"
python_run "$SCRIPT_DIR/verify_runtime.py" "$SOCKET_DIR" "$PORT" ||
  die "PostgreSQL socket path exceeds the platform limit"
EXECUTOR_USER=$(id -un)
CLOCK_TICKS_PER_SECOND=$(getconf CLK_TCK)
[[ "$CLOCK_TICKS_PER_SECOND" =~ ^[1-9][0-9]*$ ]] ||
  die "could not read CLK_TCK"
if [[ -z "${SELFTEST_FAKE_PREFIX:-}" ]]; then
  [[ -f "$KIT_DIR/PACKAGE-MANIFEST.sha256" ]] ||
    die "kit package manifest is missing"
  (
    cd "$KIT_DIR"
    sha256sum -c PACKAGE-MANIFEST.sha256 >/dev/null
  ) || die "kit package integrity check failed"
fi
[[ -f "$KIT_DIR/work/manifest.json" ]] || die "kit build manifest is missing"
for prefix in "$PREFIX_A" "$PREFIX_PATCHED"; do
  [[ -x "$prefix/bin/postgres" && -x "$prefix/bin/pgbench" ]] ||
    die "verified kit installation is missing: $prefix"
done
python_run "$SCRIPT_DIR/verify_installs.py" \
  "$KIT_DIR/work/manifest.json" "$PREFIX_PATCHED" ||
  die "reused kit installation tree differs from the build manifest"
# brief-v11-wpc-kit.md: CPU affinity is configurable, not one fixed
# topology. SERVER_CPUS/PGBENCH_CPUS are inherited from the environment
# (run-benchmark.sh -> run.sh -> here) and re-verified live exactly as the
# matrix stage verified them. Fake-binary self-tests skip live topology
# verification entirely (real host topology/os.sched_getaffinity are not
# meaningful, or even available, against fake binaries).
: "${SERVER_CPUS:?SERVER_CPUS must be set}"
: "${PGBENCH_CPUS:?PGBENCH_CPUS must be set}"
export SERVER_CPUS PGBENCH_CPUS
if [[ -z "${SELFTEST_FAKE_PREFIX:-}" ]]; then
  python_run "$AFFINITY_HELPER" verify-live ||
    die "live topology, or SERVER_CPUS/PGBENCH_CPUS, failed verification"
  python_run "$AFFINITY_HELPER" \
    verify-host-report "$HOST_CHECK" "$(hostname)" ||
    die "host report is not valid for this host"
fi

mkdir -p "$SOCKET_DIR" "$DATA_ROOT" "$LOG_DIR" "$BACKEND_DIR"
chmod 700 "$SOCKET_DIR" "$DATA_ROOT" "$LOG_DIR" "$BACKEND_DIR"
: >"$EVENTS"
printf '%s\n' \
  "session_index,block_index,mode,client_count,clients_recording,calls_before,calls_after,trace_records,backend_pid_sha256" \
  >"$PROOFS"
python_run "$SCRIPT_DIR/generate_schedule.py" "$SCHEDULE" >"$SEED_FILE"
python_run "$SCRIPT_DIR/protocol.py" \
  "$PROTOCOL" "$(<"$SEED_FILE")" \
  "$(sha256sum "$KIT_DIR/PACKAGE-MANIFEST.sha256" | awk '{print $1}')" \
  "$(sha256sum "$KIT_DIR/work/manifest.json" | awk '{print $1}')" \
  "$SCRIPT_DIR" "$SERVER_CPUS" "$PGBENCH_CPUS"

if [[ "$RUN_MODE" == smoke ]]; then
  run_session 1 A 1
  run_session 2 B 1
  [[ "$(wc -l <"$EVENTS" | tr -d ' ')" == 48 ]] ||
    die "smoke event count differs"
  [[ "$(wc -l <"$PROOFS" | tr -d ' ')" == 17 ]] ||
    die "smoke proof count differs"
  trap - EXIT ERR
  on_exit
  log "worker smoke test passed"
  exit 0
fi

mapfile -t cells < <(tail -n +2 "$SCHEDULE")
[[ ${#cells[@]} -eq "$SESSIONS" ]] || die "schedule is incomplete"
expected_session=1
for line in "${cells[@]}"; do
  [[ "$line" != *$'\r'* ]] || die "schedule contains a carriage return"
  IFS=, read -r session_index sequence pgbench_seed <<<"$line"
  [[ "$session_index" == "$expected_session" &&
     "$sequence" =~ ^[AB]$ &&
     "$pgbench_seed" == "$session_index" ]] ||
    die "schedule row is malformed: $line"
  run_session "$session_index" "$sequence" "$pgbench_seed"
  expected_session=$((expected_session + 1))
done
trap - EXIT ERR
on_exit
log "persistent-backend crossover completed all $SESSIONS sessions"
