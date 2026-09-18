#!/usr/bin/env bash
# selftest-fakebin/run-selftest.sh
#
# Fake-binaries self-test: exercises the kit's REAL shell control flow
# (01-build-all.sh -> plateau-probe.sh -> 01b-disassemble.sh ->
# 02-run-matrix.sh smoke -> 03-collect.sh) against selftest-fakebin/'s
# fake pg_ctl/initdb/pgbench/psql/postgres/etc. instead of a real
# PostgreSQL server, on whatever machine runs it (including macOS: this
# is the launcher's --preflight-only self-test). Never starts a real
# server, never touches git.
#
# It also re-verifies the actual bug this self-test exists for: it
# temporarily reverts plateau-probe.sh's run_session() `pg_ctl ... stop`
# fix, confirms the run then fails with the same
# "ValueError: could not convert string to float" shape the original bug
# report showed, and restores the fix -- so this reproduces on every run,
# not just once at bring-up.
#
# Invoked by self-test.py; also runnable directly:
#   ./selftest-fakebin/run-selftest.sh
set -Eeuo pipefail
export LC_ALL=C

FAKEBIN_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
KIT_SRC=$(cd "$FAKEBIN_DIR/.." && pwd)

log() { printf '[selftest-fakebin] %s\n' "$*"; }
die() { echo "[selftest-fakebin] ERROR: $*" >&2; exit 1; }

# The kit's scripts need bash >= 4 (mapfile, etc.); macOS ships bash 3.2.
# Prefer a Homebrew bash if the system default is too old.
BASH_BIN=""
for candidate in "${SELFTEST_BASH:-}" bash /opt/homebrew/bin/bash /usr/local/bin/bash; do
  [[ -n "$candidate" ]] || continue
  command -v "$candidate" >/dev/null 2>&1 || continue
  major=$("$candidate" -c 'echo "${BASH_VERSINFO[0]}"' 2>/dev/null || echo 0)
  if [[ "$major" =~ ^[0-9]+$ ]] && (( major >= 4 )); then
    BASH_BIN=$(command -v "$candidate")
    break
  fi
done
[[ -n "$BASH_BIN" ]] ||
  die "need bash >= 4 (e.g. 'brew install bash') to run the fake-binaries self-test; found only: $(bash --version | head -n1)"
log "Using $($BASH_BIN --version | head -n1)"

# A short scratch path, unique per invocation (two self-tests can run
# concurrently -- e.g. this script directly and make-baremetal-package.sh's
# own clean-room self-test.py run): 02-run-matrix.sh/plateau-probe.sh
# refuse to run under a path long enough to overflow a unix-socket path
# (~100 bytes with the socket filename appended), which most temp
# directories (especially macOS's per-user $TMPDIR) are not short enough
# for, so this stays directly under /tmp rather than using mktemp -t/$TMPDIR.
if [[ -n "${SELFTEST_WORKROOT:-}" ]]; then
  WORKROOT="$SELFTEST_WORKROOT"
  rm -rf -- "$WORKROOT"
  mkdir -p "$WORKROOT"
else
  WORKROOT=$(mktemp -d /tmp/wet-v11-fbst.XXXXXX)
fi
trap 'rm -rf -- "$WORKROOT"' EXIT

KIT="$WORKROOT/kit"
mkdir -p "$KIT"
# Copy everything except generated/VCS state.
(cd "$KIT_SRC" && tar -cf - \
  --exclude=work --exclude=results --exclude=smoke-results \
  --exclude=disassembly --exclude='.DS_Store' --exclude='results-*.tar.gz*' .) \
  | (cd "$KIT" && tar -xf -)

FAKEBIN="$KIT/selftest-fakebin"
export PATH="$FAKEBIN:$PATH"
export SELFTEST_FAKE_PREFIX="$FAKEBIN"
export SERVER_CPUS=${SERVER_CPUS:-0-1}
export PGBENCH_CPUS=${PGBENCH_CPUS:-2-3}

run_step() {
  local label=$1
  shift
  log "$label"
  ( cd "$KIT" && "$BASH_BIN" "$@" ) ||
    die "$label failed"
}

log "Fake kit copy: $KIT"

run_step "01-build-all.sh (fake binaries, no compilation)" ./01-build-all.sh

# --- --reuse-builds must refuse a stale "control" install that still has
# --- the tracing module (what a build from before "build the control
# --- installation without the tracing module" would leave behind, or
# --- work/ copied over from such a build): verify_reusable_builds.py and
# --- 02-run-matrix.sh's own preflight both apply
# --- build_manifest_rules.validate_installed_builds(), so this exercises
# --- the one shared rule both now use. ------------------------------------
log "verify_reusable_builds.py: expect a clean pass against the fake build"
( cd "$KIT" && python3 verify_reusable_builds.py "$KIT" ) ||
  die "verify_reusable_builds.py rejected a fresh, correct fake build"
log "  PASS: verify_reusable_builds.py accepts the fresh build"

log "verify_reusable_builds.py: stale control install with a tracing module must be refused"
STALE_MODULE="$KIT/work/install/ctrlop/lib/pg_wait_event_tracing.so"
echo "stale module left over from a pre-fix build" >"$STALE_MODULE"
set +e
stale_output=$(cd "$KIT" && python3 verify_reusable_builds.py "$KIT" 2>&1)
stale_rc=$?
set -e
rm -f -- "$STALE_MODULE"
[[ "$stale_rc" -ne 0 ]] ||
  die "verify_reusable_builds.py accepted a control install with a stale tracing module"
printf '%s\n' "$stale_output" | grep -q "control unexpectedly contains the tracing module" ||
  die "verify_reusable_builds.py refused the stale build, but not for the expected reason; got: $stale_output"
log "  PASS: verify_reusable_builds.py refuses a stale control+module install (--reuse-builds would refuse to run)"

# --- the actual regression: plateau-probe.sh's run_session() must not
# --- leak `pg_ctl ... stop`'s stdout into the captured tps value. -------
log "plateau-probe.sh (fixed): expect a clean run"
( cd "$KIT" && "$BASH_BIN" ./plateau-probe.sh >"$WORKROOT/plateau-fixed.log" 2>&1 ) ||
  die "plateau-probe.sh failed even with the fix in place; see $WORKROOT/plateau-fixed.log"
grep -q '"selected_variant"' "$WORKROOT/plateau-fixed.log" ||
  die "plateau-probe.sh (fixed) did not produce a result; see $WORKROOT/plateau-fixed.log"
log "  PASS: plateau-probe.sh completed cleanly with the fix in place"

log "plateau-probe.sh (fix reverted): expect the ORIGINAL bug to reproduce"
PROBE="$KIT/plateau-probe.sh"
cp "$PROBE" "$WORKROOT/plateau-probe.sh.fixed"
python3 - "$PROBE" <<'PY'
import sys
path = sys.argv[1]
text = open(path, encoding="utf-8").read()
fixed = (
    '  run_from_prefix "$PREFIX_BASELINE_A/bin/pg_ctl" -D "$datadir" -m fast -w stop \\\n'
    '    >>"$logfile" 2>&1 ||\n'
    '    die "clean shutdown failed for probe session $index; see $logfile"'
)
reverted = (
    '  run_from_prefix "$PREFIX_BASELINE_A/bin/pg_ctl" -D "$datadir" -m fast -w stop ||\n'
    '    die "clean shutdown failed for probe session $index"'
)
if fixed not in text:
    raise SystemExit(
        "run-selftest.sh: the expected fixed pg_ctl-stop snippet was not "
        "found in plateau-probe.sh -- has it been refactored? update this "
        "self-test's revert-verification to match."
    )
open(path, "w", encoding="utf-8").write(text.replace(fixed, reverted))
PY
set +e
( cd "$KIT" && "$BASH_BIN" ./plateau-probe.sh >"$WORKROOT/plateau-reverted.log" 2>&1 )
reverted_rc=$?
set -e
cp "$WORKROOT/plateau-probe.sh.fixed" "$PROBE"
[[ "$reverted_rc" -ne 0 ]] ||
  die "reverting the pg_ctl-stop fix did not make plateau-probe.sh fail -- the self-test cannot detect this regression"
grep -q 'ValueError: could not convert string to float' "$WORKROOT/plateau-reverted.log" ||
  die "reverted plateau-probe.sh failed, but not with the original bug's ValueError shape; see $WORKROOT/plateau-reverted.log"
grep -q 'server stopped' "$WORKROOT/plateau-reverted.log" ||
  die "reverted plateau-probe.sh's failure did not show the polluted pg_ctl stop text"
log "  PASS: reverting the fix reproduces the exact reported ValueError"

run_step "01b-disassemble.sh (fake binaries)" ./01b-disassemble.sh

log "02-run-matrix.sh smoke matrix (fake binaries, 1 rep, DURATION/WARMUP compressed)"
( cd "$KIT" && env BENCHMARK_MODE=smoke "$BASH_BIN" ./02-run-matrix.sh \
    >"$WORKROOT/matrix-smoke.log" 2>&1 ) ||
  die "02-run-matrix.sh smoke matrix failed; see $WORKROOT/matrix-smoke.log"
grep -q "Matrix complete: 35/35 cells." "$WORKROOT/matrix-smoke.log" ||
  die "02-run-matrix.sh smoke matrix did not complete all 35 cells; see $WORKROOT/matrix-smoke.log"
log "  PASS: 35/35 smoke cells (7 configs x 5 workloads x 1 repetition)"

log "03-collect.sh (against the smoke matrix's results)"
( cd "$KIT" && rm -rf results && cp -a smoke-results results )
run_step "03-collect.sh" ./03-collect.sh

log "ALL FAKE-BINARY SELF-TEST STAGES PASSED"
