#!/usr/bin/env bash
# Wait until build/smoke load has cleared before the measured matrix.
set -Eeuo pipefail
export LC_ALL=C

MAX_WAIT_SECONDS=1200
STABLE_SECONDS=60
POLL_SECONDS=10
START=$(date +%s)
STABLE_SINCE=0

if [[ "${1:-}" == --self-test ]]; then
  low=$(awk -v current_load=0.25 \
    'BEGIN { print (current_load <= 0.5) ? 1 : 0 }')
  high=$(awk -v current_load=0.75 \
    'BEGIN { print (current_load <= 0.5) ? 1 : 0 }')
  [[ "$low" == 1 && "$high" == 0 ]] || {
    echo "wait-for-idle.sh: ERROR: awk comparison self-test failed" >&2
    exit 1
  }
  echo "Idle-gate awk self-test: PASS"
  exit 0
fi
[[ $# -eq 0 ]] || {
  echo "Usage: $0 [--self-test]" >&2
  exit 2
}

[[ "$(uname -s)" == Linux ]] || {
  echo "wait-for-idle.sh: ERROR: Linux is required" >&2
  exit 1
}

while true; do
  now=$(date +%s)
  elapsed=$((now - START))
  read -r load1 _ < /proc/loadavg
  postgres_count=$(
    {
      pgrep -x postgres 2>/dev/null || true
      pgrep -x postmaster 2>/dev/null || true
    } | sort -nu | wc -l | tr -d ' '
  )
  load_ok=$(awk -v current_load="$load1" \
    'BEGIN { print (current_load <= 0.5) ? 1 : 0 }')

  printf '[%s] cooldown elapsed=%ss load1=%s co_resident_postgres=%s\n' \
    "$(date -u +%H:%M:%S)" "$elapsed" "$load1" "$postgres_count"

  if [[ "$load_ok" -eq 1 ]]; then
    if (( STABLE_SINCE == 0 )); then
      STABLE_SINCE=$now
    fi
    if (( now - STABLE_SINCE >= STABLE_SECONDS )); then
      echo "Idle gate: PASS ($STABLE_SECONDS stable seconds)"
      exit 0
    fi
  else
    STABLE_SINCE=0
  fi

  if (( elapsed >= MAX_WAIT_SECONDS )); then
    echo "wait-for-idle.sh: ERROR: host did not become idle within ${MAX_WAIT_SECONDS}s" >&2
    exit 1
  fi
  sleep "$POLL_SECONDS"
done
