#!/usr/bin/env bash
# 00-check-host.sh
#
# Read-only sanity check of this machine before the wait-event-tracing
# benchmark kit runs on it. This script NEVER changes any setting. It only
# looks at things and tells you what it saw. If something looks wrong for a
# clean performance measurement, it prints a loud WARNING and exits nonzero
# before any build or multi-hour measurement can start.
#
# What "wait-event-tracing" is, in one line: a PostgreSQL patch that times
# how long the server spends waiting on locks, I/O, and similar events. This
# kit measures how much overhead that adds. You do not need to understand
# the patch to run this kit.
set -Eeuo pipefail
export LC_ALL=C

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib-python.sh
source "$SCRIPT_DIR/lib-python.sh"
OUT_TXT="$SCRIPT_DIR/host-check.txt"
OUT_JSON="$SCRIPT_DIR/host-check.json"
WARNINGS=0

log() { printf '%s\n' "$*" | tee -a "$OUT_TXT"; }
warn() {
  WARNINGS=$((WARNINGS + 1))
  { printf '\n*** WARNING: %s ***\n\n' "$*"; } | tee -a "$OUT_TXT" >&2
}
section() { log ""; log "== $* =="; }

: >"$OUT_TXT"
log "Host check for the wait-event-tracing benchmark kit"
log "Run at: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
log "Hostname: $(hostname)"
log ""
log "This is a read-only report. Nothing on this machine is changed."

if [[ "$(uname -s)" != Linux ]]; then
  warn "this benchmark requires Linux"
  log "Host check stopped before Linux-specific /proc and /sys checks."
  exit 1
fi

# ---------------------------------------------------------------------------
section "Required tools"
REQUIRED_TOOLS=(
  python3 perl bison flex make cc ar ranlib objdump sha256sum tar
  awk sed grep find ps pgrep df hostname systemd-detect-virt
  sort wc tr paste lscpu taskset
)
for tool in "${REQUIRED_TOOLS[@]}"; do
  if command -v "$tool" >/dev/null 2>&1; then
    log "$tool: $(command -v "$tool")"
  else
    warn "required tool is not installed: $tool"
  fi
done

# The kit needs Python 3.9+; Rocky Linux 8's stock python3 is 3.6. Try, in
# order, $PYTHON_BIN_OVERRIDE, python3.11, python3.9, then plain python3,
# and report whichever one qualifies (see lib-python.sh).
if resolve_python; then
  PYTHON_VERSION=$("$PYTHON_BIN" -c \
    'import sys; print(".".join(map(str, sys.version_info[:3])))')
  log "Selected Python interpreter: $PYTHON_BIN (version $PYTHON_VERSION)"
else
  PYTHON_VERSION="unknown"
  warn "no Python 3.9+ interpreter found (tried \$PYTHON_BIN_OVERRIDE, python3.11, python3.9, python3); on Rocky Linux 8 install one with: sudo dnf install -y python3.11 (or: sudo dnf install -y python39), or set PYTHON_BIN_OVERRIDE"
fi

CC_VERSION_LINE="unknown"
if command -v cc >/dev/null 2>&1; then
  CC_VERSION_LINE=$(cc --version 2>&1 | head -n 1)
  log "C compiler:              $CC_VERSION_LINE"
  # gcc 8.5 (Rocky Linux 8's default) is a supported, accepted compiler;
  # this only flags something clearly too old to build a modern
  # PostgreSQL tree, it does not raise the floor above gcc 8.
  if [[ "$CC_VERSION_LINE" =~ gcc.*\ ([0-9]+)\.[0-9]+ ]] &&
     [[ "${BASH_REMATCH[1]}" -lt 8 ]]; then
    warn "C compiler is gcc ${BASH_REMATCH[1]}.x, older than the gcc 8 floor"
  fi
fi
# ---------------------------------------------------------------------------
section "CPU"
CPU_MODEL="unknown"
SOCKETS=1
CORES_PER_SOCKET=1
THREADS_PER_CORE=1
if command -v lscpu >/dev/null 2>&1; then
  LSCPU_OUT=$(lscpu)
  CPU_MODEL=$(printf '%s\n' "$LSCPU_OUT" | awk -F: '/^Model name:/{sub(/^[ \t]+/,"",$2); print $2; exit}')
  s=$(printf '%s\n' "$LSCPU_OUT" | awk -F: '/^Socket\(s\):/{gsub(/[ \t]/,"",$2); print $2; exit}')
  c=$(printf '%s\n' "$LSCPU_OUT" | awk -F: '/^Core\(s\) per socket:/{gsub(/[ \t]/,"",$2); print $2; exit}')
  t=$(printf '%s\n' "$LSCPU_OUT" | awk -F: '/^Thread\(s\) per core:/{gsub(/[ \t]/,"",$2); print $2; exit}')
  [[ -n "$s" ]] && SOCKETS=$s
  [[ -n "$c" ]] && CORES_PER_SOCKET=$c
  [[ -n "$t" ]] && THREADS_PER_CORE=$t
  [[ -z "$CPU_MODEL" ]] && CPU_MODEL="unknown"
else
  warn "lscpu is not installed; CPU model/core counts could not be read precisely"
fi
PHYSICAL_CORES=$((SOCKETS * CORES_PER_SOCKET))
LOGICAL_CPUS=$((PHYSICAL_CORES * THREADS_PER_CORE))
[[ "$LOGICAL_CPUS" -ge 1 ]] || LOGICAL_CPUS=$(nproc 2>/dev/null || echo 1)
SMT_ACTIVE="unknown"
if [[ -r /sys/devices/system/cpu/smt/active ]]; then
  case "$(cat /sys/devices/system/cpu/smt/active)" in
    1) SMT_ACTIVE=on ;;
    0) SMT_ACTIVE=off ;;
    *) SMT_ACTIVE=unknown ;;
  esac
elif [[ "$THREADS_PER_CORE" -gt 1 ]]; then
  SMT_ACTIVE=on
elif [[ "$THREADS_PER_CORE" -eq 1 ]]; then
  SMT_ACTIVE=off
fi

log "CPU model:              $CPU_MODEL"
log "Sockets:                $SOCKETS"
log "Physical cores:         $PHYSICAL_CORES"
log "Threads per core:       $THREADS_PER_CORE"
log "Logical CPUs:           $LOGICAL_CPUS"
log "SMT (hyperthreading):   $SMT_ACTIVE"

if [[ "$PHYSICAL_CORES" -lt 8 ]]; then
  warn "fewer than 8 physical cores ($PHYSICAL_CORES) -- the runbook asks for 8+"
fi

# ---------------------------------------------------------------------------
# Configurable CPU affinity (brief-v11-wpc-kit.md: "no longer hard-coded to
# one topology"). SERVER_CPUS/PGBENCH_CPUS come from the environment (the
# operator sets them for this host's actual layout; see the runbook). This
# check only requires that both masks are non-empty, online, disjoint, and
# share no physical core -- it prints whatever topology it finds rather
# than asserting one fixed shape.
# ---------------------------------------------------------------------------
section "CPU affinity (configurable)"
AFFINITY_HELPER="$SCRIPT_DIR/cpu_affinity.py"
AFFINITY_TOPOLOGY_VERIFIED=0
AFFINITY_PROOF_JSON='{"verified": false}'
TOPOLOGY_OK=1

if command -v lscpu >/dev/null 2>&1; then
  log "Full topology (lscpu -e):"
  lscpu -e 2>&1 | tee -a "$OUT_TXT" || true
fi

if [[ ! -r "$AFFINITY_HELPER" ]]; then
  warn "required CPU-affinity helper is missing: $AFFINITY_HELPER"
  TOPOLOGY_OK=0
elif [[ -z "${SERVER_CPUS:-}" || -z "${PGBENCH_CPUS:-}" ]]; then
  warn "SERVER_CPUS and PGBENCH_CPUS must both be set in the environment before running this check (see the runbook); refusing to guess a topology"
  TOPOLOGY_OK=0
else
  log "Requested PostgreSQL taskset: $SERVER_CPUS"
  log "Requested pgbench taskset:    $PGBENCH_CPUS"
fi

NUMA_NODE_COUNT=0
for node_dir in /sys/devices/system/node/node*; do
  [[ -d "$node_dir" ]] || continue
  NUMA_NODE_COUNT=$((NUMA_NODE_COUNT + 1))
done
log "NUMA nodes present: $NUMA_NODE_COUNT"
if [[ "$NUMA_NODE_COUNT" -gt 1 ]] && ! command -v numactl >/dev/null 2>&1; then
  # brief-v11-wpc-kit.md: "If numactl is missing and the host has more than
  # one NUMA node, warn but continue" -- this is deliberately not a
  # fail-closed warn(): it must not block the run. plateau-probe.sh falls
  # back to taskset-only pinning (no membind) in this case.
  log ""
  log "*** NOTE (non-blocking): numactl is not installed and this host has more than one NUMA node. The plateau probe will fall back to taskset-only pinning (no membind). ***"
  log ""
fi

if [[ "$TOPOLOGY_OK" -eq 1 ]]; then
  if AFFINITY_PROOF_JSON=$("${PYTHON_BIN:-python3}" "$AFFINITY_HELPER" collect); then
    AFFINITY_TOPOLOGY_VERIFIED=1
    log "Affinity topology:      verified (masks disjoint, no shared physical core, taskset confirmed)"
  else
    AFFINITY_PROOF_JSON='{"verified": false}'
    warn "SERVER_CPUS/PGBENCH_CPUS failed verification (empty, overlapping, sharing a physical core, or not applicable with taskset) -- see the detail above"
    TOPOLOGY_OK=0
  fi
fi

if [[ "$AFFINITY_TOPOLOGY_VERIFIED" -ne 1 ]]; then
  log "Affinity topology:      NOT VERIFIED"
fi

# ---------------------------------------------------------------------------
section "CPU frequency governor and turbo/boost"
declare -a GOVERNORS=()
for f in /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor; do
  [[ -r "$f" ]] || continue
  GOVERNORS+=("$(cat "$f")")
done
if [[ ${#GOVERNORS[@]} -eq 0 ]]; then
  log "Per-core governor: not exposed by this kernel/CPU driver"
  warn "could not read any cpufreq governor; cannot confirm 'performance' mode"
else
  # Tabulate distinct governor values.
  declare -A COUNTS=()
  for g in "${GOVERNORS[@]}"; do COUNTS[$g]=$(( ${COUNTS[$g]:-0} + 1 )); done
  for g in "${!COUNTS[@]}"; do
    log "  $g: ${COUNTS[$g]} core(s)"
  done
  NON_PERF=0
  for g in "${!COUNTS[@]}"; do
    [[ "$g" == performance ]] || NON_PERF=$((NON_PERF + COUNTS[$g]))
  done
  if [[ "$NON_PERF" -gt 0 ]]; then
    warn "$NON_PERF core(s) are NOT in 'performance' governor -- this adds noise to short-lock and microbenchmark workloads. This script does not change it for you."
  fi
fi

TURBO_STATE="unknown"
if [[ -r /sys/devices/system/cpu/intel_pstate/no_turbo ]]; then
  case "$(cat /sys/devices/system/cpu/intel_pstate/no_turbo)" in
    0) TURBO_STATE="enabled (intel_pstate no_turbo=0)" ;;
    1) TURBO_STATE="DISABLED (intel_pstate no_turbo=1)" ;;
  esac
elif [[ -r /sys/devices/system/cpu/cpufreq/boost ]]; then
  case "$(cat /sys/devices/system/cpu/cpufreq/boost)" in
    1) TURBO_STATE="enabled (cpufreq boost=1)" ;;
    0) TURBO_STATE="DISABLED (cpufreq boost=0)" ;;
  esac
fi
log "Turbo/boost state:      $TURBO_STATE"
[[ "$TURBO_STATE" == unknown ]] && log "  (not exposed on this CPU/kernel; record it from the BIOS if you know it)"

# ---------------------------------------------------------------------------
section "Kernel"
log "$(uname -a)"

# ---------------------------------------------------------------------------
section "Memory"
MEM_TOTAL_KB=$(awk '/^MemTotal:/{print $2}' /proc/meminfo)
MEM_AVAIL_KB=$(awk '/^MemAvailable:/{print $2}' /proc/meminfo)
log "Total RAM:  $(( MEM_TOTAL_KB / 1024 )) MiB"
log "Free RAM:   $(( MEM_AVAIL_KB / 1024 )) MiB (MemAvailable)"
if [[ "$MEM_TOTAL_KB" -lt $((16 * 1024 * 1024)) ]]; then
  warn "less than 16 GB total RAM -- the runbook asks for 16+ GB"
fi

# ---------------------------------------------------------------------------
section "Swap"
SWAP_TOTAL_KB=$(awk '/^SwapTotal:/{print $2}' /proc/meminfo)
SWAP_FREE_KB=$(awk '/^SwapFree:/{print $2}' /proc/meminfo)
if [[ "$SWAP_TOTAL_KB" -gt 0 ]]; then
  SWAP_USED_KB=$((SWAP_TOTAL_KB - SWAP_FREE_KB))
  log "Swap: configured, $(( SWAP_TOTAL_KB / 1024 )) MiB total, $(( SWAP_USED_KB / 1024 )) MiB in use"
  if [[ "$SWAP_USED_KB" -gt 0 ]]; then
    warn "swap is actively in use ($(( SWAP_USED_KB / 1024 )) MiB) -- this can silently slow a run"
  fi
else
  log "Swap: off"
fi

# ---------------------------------------------------------------------------
section "Disk space (kit directory)"
DISK_LINE=$(df -Pk "$SCRIPT_DIR" | awk 'NR==2{print $4}')
FREE_DISK_MB=$((DISK_LINE / 1024))
log "Free disk under $SCRIPT_DIR: $FREE_DISK_MB MiB"
if [[ "$FREE_DISK_MB" -lt $((30 * 1024)) ]]; then
  warn "less than 30 GB free disk under the kit directory"
fi

# ---------------------------------------------------------------------------
section "Load average and other activity"
read -r LOAD1 LOAD5 LOAD15 REST < /proc/loadavg
log "Load average (1/5/15 min): $LOAD1 $LOAD5 $LOAD15"
LOAD_OVER=$(awk -v l="$LOAD1" 'BEGIN{print (l > 0.5) ? 1 : 0}')
if [[ "$LOAD_OVER" -eq 1 ]]; then
  warn "1-minute load average is $LOAD1, above the 0.5 threshold for an 'otherwise idle' host"
fi

log ""
log "Top processes by CPU (a busy host here means the run will be noisy):"
ps -eo pid,ppid,pcpu,pmem,comm --sort=-pcpu |
  sed -n '1,8p' |
  tee -a "$OUT_TXT"

if [[ -r /proc/pressure/cpu ]]; then
  log ""
  log "CPU pressure:"
  cat /proc/pressure/cpu | tee -a "$OUT_TXT"
fi

VIRTUALIZATION="unknown"
if command -v systemd-detect-virt >/dev/null 2>&1; then
  VIRTUALIZATION=$(systemd-detect-virt 2>/dev/null || true)
  [[ -n "$VIRTUALIZATION" ]] || VIRTUALIZATION=none
fi
log "Virtualization: $VIRTUALIZATION"
if [[ "$VIRTUALIZATION" == unknown ]]; then
  warn "virtualization could not be detected; dedicated bare metal is unproven"
elif [[ "$VIRTUALIZATION" != none ]]; then
  warn "virtualization was detected ($VIRTUALIZATION), but the runbook requires dedicated bare metal"
fi

POSTGRES_PIDS=$(
  {
    pgrep -x postgres 2>/dev/null || true
    pgrep -x postmaster 2>/dev/null || true
  } | sort -nu
)
if [[ -n "$POSTGRES_PIDS" ]]; then
  POSTGRES_PROCESS_COUNT=$(printf '%s\n' "$POSTGRES_PIDS" | wc -l | tr -d ' ')
  POSTGRES_PID_CSV=$(printf '%s\n' "$POSTGRES_PIDS" | paste -sd, -)
  log ""
  log "Co-resident PostgreSQL processes: $POSTGRES_PROCESS_COUNT"
  log "These are recorded but do not block an otherwise-idle host."
  if ! ps -ww -p "$POSTGRES_PID_CSV" \
      -o user=,pid=,ppid=,stat=,etimes=,pcpu=,pmem=,comm=,args= |
      tee -a "$OUT_TXT"; then
    log "Process inventory changed while it was being captured."
  fi
else
  POSTGRES_PROCESS_COUNT=0
  log "Co-resident PostgreSQL processes: none"
fi

# ---------------------------------------------------------------------------
section "Summary"
log "Warnings: $WARNINGS"
log "Report written to: $OUT_TXT"

# ---------------------------------------------------------------------------
# Machine-readable copy, best-effort. A Python interpreter is required
# later by the build and matrix scripts anyway (resolved above into
# PYTHON_BIN), so it is safe to use here too; fall back to plain python3
# if even that could not be resolved, so this report still gets written.
JSON_PYTHON=${PYTHON_BIN:-}
if [[ -z "$JSON_PYTHON" ]] && command -v python3 >/dev/null 2>&1; then
  JSON_PYTHON=python3
fi
if [[ -n "$JSON_PYTHON" ]]; then
  "$JSON_PYTHON" - "$OUT_JSON" "$CPU_MODEL" "$SOCKETS" "$CORES_PER_SOCKET" \
    "$THREADS_PER_CORE" "$PHYSICAL_CORES" "$LOGICAL_CPUS" "$SMT_ACTIVE" \
    "$TURBO_STATE" "$MEM_TOTAL_KB" "$MEM_AVAIL_KB" "$SWAP_TOTAL_KB" \
    "$FREE_DISK_MB" "$LOAD1" "$LOAD5" "$LOAD15" "$WARNINGS" \
    "$(hostname)" "$(uname -a)" "$VIRTUALIZATION" \
    "$POSTGRES_PROCESS_COUNT" "$AFFINITY_PROOF_JSON" \
    "$PYTHON_BIN" "$PYTHON_VERSION" "$CC_VERSION_LINE" <<'PY'
import datetime
import glob
import json
import sys

(out, cpu_model, sockets, cores_per_socket, threads_per_core,
 physical_cores, logical_cpus, smt_active, turbo_state, mem_total_kb,
 mem_avail_kb, swap_total_kb, free_disk_mb, load1, load5, load15,
 warnings, hostname, kernel, virtualization, postgres_process_count,
 affinity_proof_json, python_bin, python_version, cc_version) = sys.argv[1:]

governors = []
for path in sorted(glob.glob(
        "/sys/devices/system/cpu/cpu*/cpufreq/scaling_governor")):
    try:
        governors.append(open(path, encoding="ascii").read().strip())
    except OSError:
        pass

data = {
    "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "hostname": hostname,
    "kernel": kernel,
    "virtualization": virtualization,
    "cpu_model": cpu_model,
    "sockets": int(sockets),
    "cores_per_socket": int(cores_per_socket),
    "threads_per_core": int(threads_per_core),
    "physical_cores": int(physical_cores),
    "logical_cpus": int(logical_cpus),
    "smt_active": smt_active,
    "turbo_state": turbo_state,
    "governors": governors,
    "mem_total_kb": int(mem_total_kb),
    "mem_available_kb": int(mem_avail_kb),
    "swap_total_kb": int(swap_total_kb),
    "free_disk_mb_kit_dir": int(free_disk_mb),
    "load_average_1_5_15": [float(load1), float(load5), float(load15)],
    "warning_count": int(warnings),
    "co_resident_postgres_process_count": int(postgres_process_count),
    "host_isolation": (
        "co-resident"
        if int(postgres_process_count) > 0
        else "dedicated"
    ),
    "cpu_affinity_protocol": json.loads(affinity_proof_json),
    "python_bin": python_bin,
    "python_version": python_version,
    "cc_version": cc_version,
}
with open(out, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, sort_keys=True)
    f.write("\n")
PY
  log "Machine-readable copy: $OUT_JSON"
fi

if [[ "$WARNINGS" -gt 0 ]]; then
  log ""
  log "$WARNINGS warning(s) above. The benchmark is blocked before any build."
  log "Fix or explain every warning, then rerun this check."
  exit 1
fi

exit 0
