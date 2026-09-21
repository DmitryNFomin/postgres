#!/usr/bin/env bash
# plateau-probe.sh
#
# Preflight diagnostic (brief-v11-wpc-kit.md, "Plateau probe in preflight"):
# eight vanilla-only W6c sessions, four with the dataset clone and initdb
# pinned to the server's NUMA node via `numactl --cpunodebind=N --membind=N`
# (or `taskset` to the server mask if numactl is absent), four unpinned,
# alternating. Reports both spreads. The matrix (02-run-matrix.sh) then
# uses whichever variant has the smaller spread, recorded in
# plateau-probe-result.json.
#
# Only the setup step (initdb + pgbench -i dataset clone) is pinned or not;
# the measured server always runs under the normal SERVER_CPUS taskset,
# exactly as every matrix cell does.
set -Eeuo pipefail
export LC_ALL=C

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WORK="$SCRIPT_DIR/work"
RESULT_FILE="$SCRIPT_DIR/plateau-probe-result.json"
AFFINITY_HELPER="$SCRIPT_DIR/cpu_affinity.py"

die() { echo "plateau-probe.sh: ERROR: $*" >&2; exit 1; }
log() { printf '%s\n' "[$(date -u +%H:%M:%S)] $*"; }

# shellcheck source=lib-python.sh
source "$SCRIPT_DIR/lib-python.sh"
require_python

if [[ -z "${SELFTEST_FAKE_PREFIX:-}" ]]; then
  "$PYTHON_BIN" "$SCRIPT_DIR/sources_conf.py" check "$SCRIPT_DIR/sources.conf" ||
    die "sources.conf is not filled in"
fi

: "${SERVER_CPUS:?SERVER_CPUS must be set}"
: "${PGBENCH_CPUS:?PGBENCH_CPUS must be set}"
export SERVER_CPUS PGBENCH_CPUS

for tool in taskset lscpu; do
  command -v "$tool" >/dev/null 2>&1 || die "required tool not found: $tool"
done

PREFIX_BASELINE_A="$WORK/install/base-a"
[[ -x "$PREFIX_BASELINE_A/bin/postgres" ]] ||
  die "missing baseline-a build at $PREFIX_BASELINE_A -- run ./01-build-all.sh first"

if [[ -n "${SELFTEST_FAKE_PREFIX:-}" ]]; then
  # Real host CPU topology is meaningless against fake binaries (and
  # cpu_affinity.py's os.sched_getaffinity() is not even available on
  # every self-test platform, e.g. macOS): skip live topology
  # verification and use a fixed fake NUMA node.
  SERVER_NUMA_NODE=0
else
  AFFINITY_JSON=$("$PYTHON_BIN" "$AFFINITY_HELPER" collect) ||
    die "SERVER_CPUS/PGBENCH_CPUS failed verification"
  SERVER_NUMA_NODE=$("$PYTHON_BIN" -c "
import json, sys
data = json.loads('''$AFFINITY_JSON''')
nodes = data['server_numa_nodes']
print(nodes[0] if nodes else '')
")
  [[ -n "$SERVER_NUMA_NODE" ]] ||
    die "could not determine the server's NUMA node from SERVER_CPUS"
fi

NUMACTL_AVAILABLE=0
command -v numactl >/dev/null 2>&1 && NUMACTL_AVAILABLE=1

PORT=${PGBENCH_KIT_PORT:-55471}
DBUSER=$(id -un)
SOCKET_DIR="$WORK/plateau-probe-sock"
DATA_ROOT="$WORK/plateau-probe-data"
LOG_DIR="$WORK/plateau-probe-logs"
rm -rf -- "$SOCKET_DIR" "$DATA_ROOT" "$LOG_DIR"
mkdir -p "$SOCKET_DIR" "$DATA_ROOT" "$LOG_DIR"

# Full-protocol W6c shape (brief: shared_buffers per workload as in v10;
# 16-repetition full matrix uses 30s window/10s warmup; the probe uses the
# same window so it is diagnostic of the actual run, not the smoke profile).
if [[ -n "${SELFTEST_FAKE_PREFIX:-}" ]]; then
  DURATION=2
  WARMUP_SECONDS=1
else
  DURATION=30
  WARMUP_SECONDS=10
fi
PGBENCH_SCALE=100
CLIENTS=32
THREADS=8
SHARED_BUFFERS=32MB

run_from_prefix() {
  env LD_LIBRARY_PATH="$PREFIX_BASELINE_A/lib:${LD_LIBRARY_PATH:-}" "$@"
}

run_session() {
  local index=$1 variant=$2
  local datadir="$DATA_ROOT/session-$index"
  local logfile="$LOG_DIR/server-$index.log"
  local -a wrap=()
  if [[ "$variant" == pinned ]]; then
    if [[ "$NUMACTL_AVAILABLE" -eq 1 ]]; then
      wrap=(numactl "--cpunodebind=$SERVER_NUMA_NODE" "--membind=$SERVER_NUMA_NODE")
    else
      wrap=(taskset -c "$SERVER_CPUS")
    fi
  fi

  run_from_prefix "${wrap[@]}" "$PREFIX_BASELINE_A/bin/initdb" \
    -D "$datadir" --no-sync --no-locale -E UTF8 \
    --auth-local=trust --auth-host=trust --username="$DBUSER" \
    >>"$logfile.initdb" 2>&1 ||
    die "initdb failed for probe session $index; see $logfile.initdb"
  {
    echo "listen_addresses = ''"
    echo "unix_socket_directories = '$SOCKET_DIR'"
    echo "max_connections = 100"
    echo "shared_buffers = '$SHARED_BUFFERS'"
    echo "autovacuum = off"
    echo "checkpoint_timeout = '1h'"
    echo "max_wal_size = '8GB'"
  } >>"$datadir/postgresql.conf"

  run_from_prefix taskset -c "$SERVER_CPUS" \
    "$PREFIX_BASELINE_A/bin/pg_ctl" -D "$datadir" -l "$logfile" \
    -o "-p $PORT -k $SOCKET_DIR" -w start >/dev/null ||
    die "server failed to start for probe session $index; see $logfile"

  PGHOST="$SOCKET_DIR" PGPORT="$PORT" PGUSER="$DBUSER" \
    run_from_prefix "${wrap[@]}" taskset -c "$PGBENCH_CPUS" \
    "$PREFIX_BASELINE_A/bin/pgbench" -i -q --unlogged-tables \
    -s "$PGBENCH_SCALE" postgres \
    >"$LOG_DIR/pgbench-init-$index.log" 2>&1 ||
    die "pgbench -i failed for probe session $index"
  PGHOST="$SOCKET_DIR" PGPORT="$PORT" PGUSER="$DBUSER" \
    run_from_prefix "$PREFIX_BASELINE_A/bin/psql" -X -v ON_ERROR_STOP=1 \
    -d postgres -qAtc "CHECKPOINT" >/dev/null ||
    die "checkpoint failed for probe session $index"

  local total=$((WARMUP_SECONDS + DURATION))
  local pgbench_log="$LOG_DIR/pgbench-$index.log"
  PGHOST="$SOCKET_DIR" PGPORT="$PORT" PGUSER="$DBUSER" \
    run_from_prefix taskset -c "$PGBENCH_CPUS" \
    "$PREFIX_BASELINE_A/bin/pgbench" -n -S -c "$CLIENTS" -j "$THREADS" \
    -T "$total" -P 1 --random-seed="$index" postgres \
    >"$pgbench_log" 2>&1 ||
    die "pgbench failed for probe session $index; see $pgbench_log"

  run_from_prefix "$PREFIX_BASELINE_A/bin/pg_ctl" -D "$datadir" -m fast -w stop \
    >>"$logfile" 2>&1 ||
    die "clean shutdown failed for probe session $index; see $logfile"
  rm -rf -- "$datadir"

  "$PYTHON_BIN" - "$pgbench_log" "$WARMUP_SECONDS" "$DURATION" <<'PY'
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
selected = []
previous = 0.0
for elapsed, tps, _latency, _failures in matches:
    elapsed = float(elapsed)
    interval = elapsed - previous
    if previous >= warmup_s and interval > 0:
        selected.append((interval, float(tps)))
    previous = elapsed
if not selected:
    raise SystemExit("no post-warmup progress lines")
measured_interval = sum(interval for interval, _ in selected)
transactions = sum(interval * tps for interval, tps in selected)
print(f"{transactions / measured_interval:.6g}")
PY
}

# benchmark_protocol.PLATEAU_PROBE_SESSIONS_PER_VARIANT (4 real, 1 under
# SELFTEST_FAKE_PREFIX or BENCHMARK_REHEARSAL=1) is the same count
# analyze-results.py's verify_plateau_probe() checks pinned_tps/
# unpinned_tps against -- single source, not a hardcoded 8/4 here that
# could drift from a compressed expectation there.
SESSIONS_PER_VARIANT=$("$PYTHON_BIN" -c \
  "from benchmark_protocol import PLATEAU_PROBE_SESSIONS_PER_VARIANT as n; print(n)")
TOTAL_SESSIONS=$((SESSIONS_PER_VARIANT * 2))
log "Server NUMA node: $SERVER_NUMA_NODE  numactl available: $NUMACTL_AVAILABLE"
log "Running $TOTAL_SESSIONS vanilla W6c probe sessions ($SESSIONS_PER_VARIANT pinned, $SESSIONS_PER_VARIANT unpinned, alternating)"

PINNED_TPS=()
UNPINNED_TPS=()
for ((i = 1; i <= TOTAL_SESSIONS; i++)); do
  if (( i % 2 == 1 )); then
    variant=pinned
  else
    variant=unpinned
  fi
  log "Session $i/$TOTAL_SESSIONS ($variant)"
  tps=$(run_session "$i" "$variant")
  log "  tps=$tps"
  if [[ "$variant" == pinned ]]; then
    PINNED_TPS+=("$tps")
  else
    UNPINNED_TPS+=("$tps")
  fi
done

"$PYTHON_BIN" - "$RESULT_FILE" "$SERVER_NUMA_NODE" "$NUMACTL_AVAILABLE" \
  "${#PINNED_TPS[@]}" "${PINNED_TPS[@]}" -- "${UNPINNED_TPS[@]}" <<'PY'
import json
import statistics
import sys

args = sys.argv[1:]
result_file, server_numa_node, numactl_available, _n_pinned = args[:4]
rest = args[4:]
sep = rest.index("--")
pinned = [float(v) for v in rest[:sep]]
unpinned = [float(v) for v in rest[sep + 1:]]

def spread_percent(values):
    if len(values) < 2:
        return 0.0
    mean = statistics.mean(values)
    return statistics.stdev(values) / mean * 100.0 if mean else float("inf")

pinned_spread = spread_percent(pinned)
unpinned_spread = spread_percent(unpinned)
selected = "pinned" if pinned_spread <= unpinned_spread else "unpinned"

data = {
    "server_numa_node": int(server_numa_node),
    "numactl_available": bool(int(numactl_available)),
    "pinned_tps": pinned,
    "unpinned_tps": unpinned,
    "pinned_spread_percent": pinned_spread,
    "unpinned_spread_percent": unpinned_spread,
    "selected_variant": selected,
}
with open(result_file, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, sort_keys=True)
    f.write("\n")
print(json.dumps(data, indent=2, sort_keys=True))
PY

log "Plateau probe result written to $RESULT_FILE"
