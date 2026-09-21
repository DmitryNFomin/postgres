#!/usr/bin/env bash
# selftest-fakebin/run-selftest.sh
#
# Fake-binaries self-test: drives the REAL run-benchmark.sh launcher --
# every phase, in the same order, with the same commands a real run uses
# (preflight, kit-self-test, initial idle check, build, disassembly,
# plateau probe, smoke, smoke-verification, cooldown, final host check,
# the full 560-cell matrix, collection, crossover smoke, crossover full)
# -- against selftest-fakebin/'s fake pg_ctl/initdb/pgbench/psql/postgres
# instead of a real PostgreSQL server, on whatever machine runs it
# (including macOS). Never starts a real PostgreSQL server, never touches
# git. This script does not re-implement the phase list itself, on
# purpose: it used to invoke 01-build-all.sh -> plateau-probe.sh ->
# 01b-disassemble.sh -> 02-run-matrix.sh smoke -> 03-collect.sh directly
# and never ran analyze-results.py on the smoke results at all, which is
# exactly how analyze-results.py's backwards module-presence rule (it
# required "control" to have pg_wait_event_tracing) survived until a real
# run hit it in smoke-verification -- see reports/wpf-report.md,
# Addendum 5. Only run-benchmark.sh itself can guarantee a phase is never
# silently skipped again.
#
# What actually makes this fast despite driving the unmodified launcher:
# selftest-fakebin/pgbench sleeps a small fraction of a real second per
# *simulated* second of -T (see that script) instead of shrinking any
# DURATION/WARMUP_SECONDS/RUNS/etc. protocol constant -- those stay
# exactly what a real run uses, because analyze-results.py's
# verify_protocol() checks protocol.json against the same fixed
# FULL_PROFILE/SMOKE_PROFILE dicts (benchmark_protocol.py) a real run must
# match.
#
# Three fake kit copies are prepared, all under one scratch WORKROOT:
#   kit-repro   -- analyze-results.py's fix temporarily reverted, so the
#                  real outage reproduces via the real launcher (dies at
#                  smoke-verification with exactly "control build lacks
#                  pg_wait_event_tracing"), then the fix is restored and
#                  this copy's own build is reused for the
#                  verify_reusable_builds.py and plateau-probe.sh
#                  regression checks below.
#   kit-full    -- the fix intact, run-benchmark.sh to completion:
#                  RAW CAPTURE SUCCESS, then analyze-raw-archive.sh on the
#                  produced matrix archive.
#   kit-reuse   -- a fresh extraction with ONLY kit-full's work/ copied
#                  in (BAREMETAL-RUNBOOK-v11.md, "Restarting after a
#                  failure"), then `--reuse-builds` from scratch: skips
#                  the build phase and runs the rest to completion.
#
# Invoked by self-test.py; also runnable directly:
#   ./selftest-fakebin/run-selftest.sh
set -Eeuo pipefail
export LC_ALL=C

FAKEBIN_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
KIT_SRC=$(cd "$FAKEBIN_DIR/.." && pwd)

log() { printf '[selftest-fakebin] %s\n' "$*"; }
die() { echo "[selftest-fakebin] ERROR: $*" >&2; exit 1; }

# This script's own direct Python calls (the module-rule revert/restore
# heredocs and the verify_reusable_builds.py checks below) used to run
# under plain `python3`, same as every other kit script before it adopted
# lib-python.sh's resolve_python(). That is fine on a dev machine where
# `python3` is already 3.9+, but Rocky Linux 8's stock `python3` is 3.6 --
# too old for verify_reusable_builds.py's `from __future__ import
# annotations`, which fails with "SyntaxError: future feature annotations
# is not defined" under 3.6. Resolve the same PYTHON_BIN every other kit
# script uses instead of assuming plain `python3` is new enough.
# shellcheck source=../lib-python.sh
source "$KIT_SRC/lib-python.sh"
require_python die

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
[[ -n "${SELFTEST_KEEP_WORKROOT:-}" ]] || trap 'rm -rf -- "$WORKROOT"' EXIT

# run-benchmark.sh's own sub-scripts are invoked by their own
# "#!/usr/bin/env bash" shebang (SCRIPT/child.sh), not through $BASH_BIN,
# so `env bash` has to resolve to a bash >= 4 too. Put a shim directory
# with a `bash` symlink to $BASH_BIN at the very front of PATH so it does,
# regardless of what plain `bash` on this machine's normal PATH is.
BASH_SHIM_DIR="$WORKROOT/bashshim"
mkdir -p "$BASH_SHIM_DIR"
ln -sf "$BASH_BIN" "$BASH_SHIM_DIR/bash"

export SERVER_CPUS=${SERVER_CPUS:-0-1}
export PGBENCH_CPUS=${PGBENCH_CPUS:-2-3}

run_step() {
  local label=$1
  shift
  log "$label"
  ( cd "$KIT" && "$BASH_BIN" "$@" ) ||
    die "$label failed"
}

# prepare_kit_copy DEST_NAME -- fresh copy of the kit source (excluding
# generated/VCS state) under $WORKROOT/DEST_NAME, with
# crossover/CROSSOVER-MANIFEST.sha256 bootstrapped (crossover/run.sh
# refuses to run without one next to it; a real package gets one from
# make-baremetal-package.sh, but a dev-checkout copy like this one never
# has one committed).
prepare_kit_copy() {
  local dest="$WORKROOT/$1"
  rm -rf -- "$dest"
  mkdir -p "$dest"
  (cd "$KIT_SRC" && tar -cf - \
    --exclude=work --exclude=results --exclude=smoke-results \
    --exclude=disassembly --exclude='.DS_Store' --exclude='results-*.tar.gz*' \
    --exclude=run-logs --exclude=status.json --exclude=.benchmark.lock \
    --exclude=host-check.txt --exclude=host-check.json \
    --exclude=plateau-probe-result.json \
    --exclude=__pycache__ --exclude='*.pyc' .) \
    | (cd "$dest" && tar -xf -)
  rm -f -- "$dest/crossover/CROSSOVER-MANIFEST.sha256"
  (
    cd "$dest/crossover"
    find . -type f ! -name CROSSOVER-MANIFEST.sha256 -print0 |
      sort -z |
      xargs -0 sha256sum >CROSSOVER-MANIFEST.sha256
  )
  printf '%s' "$dest"
}

# bootstrap_package_manifest DEST -- generates DEST/PACKAGE-MANIFEST.sha256
# over the kit copy's current contents, the same way make-baremetal-
# package.sh does for a real package. crossover/run.sh refuses to run
# without one next to the kit it is pointed at (its own integrity gate,
# same idea as CROSSOVER-MANIFEST.sha256 above), and so does
# run-benchmark.sh's own package-integrity phase when one is present.
# Call this ONLY once a kit copy's contents are final: kit-repro calls it
# a second time, right after its module-rule revert edits (analyze-
# results.py/self-test.py), so the manifest describes the reverted
# content run-benchmark.sh is about to see instead of KIT_SRC's original
# one -- needed whenever KIT_SRC already ships a PACKAGE-MANIFEST.sha256
# of its own (a real packaged copy, e.g. under make-baremetal-package.sh's
# clean-room self-test; a bare dev checkout has no such file to begin
# with, which is why this gap went unnoticed until packaging exercised
# it).
bootstrap_package_manifest() {
  local dest=$1
  rm -f -- "$dest/PACKAGE-MANIFEST.sha256"
  (
    cd "$dest"
    find . -type f ! -name PACKAGE-MANIFEST.sha256 -print0 |
      sort -z |
      xargs -0 sha256sum >PACKAGE-MANIFEST.sha256
  )
}

# launch_benchmark KIT_DIR LOGFILE [ARGS...] -- runs run-benchmark.sh
# (via the bash shim, in fake-binaries mode) inside KIT_DIR, capturing
# combined output to LOGFILE. Returns run-benchmark.sh's exit code without
# tripping this script's `set -e`.
launch_benchmark() {
  local dest=$1 logfile=$2
  shift 2
  local rc=0
  (
    cd "$dest" &&
    PATH="$BASH_SHIM_DIR:$dest/selftest-fakebin:$PATH" \
    SELFTEST_FAKE_PREFIX="$dest/selftest-fakebin" \
    SELFTEST_NESTED=1 \
    SERVER_CPUS="$SERVER_CPUS" PGBENCH_CPUS="$PGBENCH_CPUS" \
    ./run-benchmark.sh "$@"
  ) >"$logfile" 2>&1 || rc=$?
  return "$rc"
}

# ===========================================================================
# kit-repro: reproduce today's real outage through the REAL launcher, then
# restore the fix and reuse this copy's build for two more standing
# regression checks.
# ===========================================================================
KIT_REPRO=$(prepare_kit_copy kit-repro)
KIT="$KIT_REPRO"
log "Fake kit copy (repro): $KIT_REPRO"

ANALYZER="$KIT_REPRO/analyze-results.py"
SELFTEST_PY="$KIT_REPRO/self-test.py"
cp "$ANALYZER" "$WORKROOT/analyze-results.py.fixed"
cp "$SELFTEST_PY" "$WORKROOT/self-test.py.fixed"
log "Reverting analyze-results.py's module-rule fix (reproducing the real outage)"
# self-test.py's own synthetic-manifest builder (its "kit-self-test" phase,
# which run-benchmark.sh runs before smoke-verification) had the exact same
# backwards module-presence rule baked in as a private _module_sha() --
# which is WHY analyze-results.py's bug survived undetected: the two wrong
# rules agreed with each other, so self-test.py's own analyzer exercise
# never noticed. Revert both together, exactly like production was before
# the fix, so kit-self-test still passes and the REAL smoke-verification
# phase (against actually-built fake binaries, which correctly never put
# the module in "control") is the one that reproduces the outage.
"$PYTHON_BIN" - "$SELFTEST_PY" <<'PY'
import sys
path = sys.argv[1]
text = open(path, encoding="utf-8").read()
fixed = (
    "def _module_sha(config_build: str) -> str:\n"
    "    # Single shared rule (build_manifest_rules.BUILD_WITH_MODULE): the\n"
    "    # tracing module is present only in the \"patched\" build, \"none\"\n"
    "    # everywhere else -- this used to hand \"control\" a fake module hash\n"
    "    # too, which is the same backwards rule analyze-results.py's old\n"
    "    # private check had (reports/wpf-report.md, Addendum 5), just baked\n"
    "    # into this synthetic-manifest builder instead.\n"
    "    return \"8\" * 64 if config_build == BUILD_WITH_MODULE else \"none\"\n"
)
reverted = (
    'def _module_sha(config_build: str) -> str:\n'
    '    return "none" if config_build in ("baseline-a", "baseline-b") else (\n'
    '        "8" * 64 if config_build == "patched" else "9" * 64\n'
    '    )\n'
)
if fixed not in text:
    raise SystemExit(
        "run-selftest.sh: the expected fixed self-test.py _module_sha() "
        "was not found -- has it been refactored? update this self-test's "
        "revert-verification to match."
    )
open(path, "w", encoding="utf-8").write(text.replace(fixed, reverted))
PY
"$PYTHON_BIN" - "$ANALYZER" <<'PY'
import sys
path = sys.argv[1]
text = open(path, encoding="utf-8").read()
fixed = (
    "    # Single shared rule (build_manifest_rules.py): five binaries hashed in\n"
    "    # all four builds, fixture hashed in all four builds, tracing module\n"
    "    # present and hash-matched in \"patched\" only, absent (or \"none\")\n"
    "    # everywhere else. This used to be a private, and backwards, copy of\n"
    "    # that rule right here -- see build_manifest_rules.validate_manifest_\n"
    "    # hashes()'s docstring and reports/wpf-report.md, Addendum 5.\n"
    "    try:\n"
    "        build_manifest_rules.validate_manifest_hashes(builds)\n"
    "    except build_manifest_rules.ManifestMismatch as exc:\n"
    "        raise InvalidResults(str(exc)) from exc\n"
)
reverted = (
    '    for name in ("patched", "control"):\n'
    '        require(builds[name]["sha256"]["pg_wait_event_tracing"] != "none",\n'
    '                f"{name} build lacks pg_wait_event_tracing")\n'
    '    for name in ("baseline-a", "baseline-b"):\n'
    '        require(builds[name]["sha256"]["pg_wait_event_tracing"] == "none",\n'
    '                f"{name} unexpectedly contains pg_wait_event_tracing")\n'
)
if fixed not in text:
    raise SystemExit(
        "run-selftest.sh: the expected fixed module-rule snippet was not "
        "found in analyze-results.py -- has it been refactored? update "
        "this self-test's revert-verification to match."
    )
open(path, "w", encoding="utf-8").write(text.replace(fixed, reverted))
PY

# Regenerate the manifest to match the just-reverted content, AFTER the
# edits above, not before: when KIT_SRC is a bare dev checkout this file
# never existed in the first place, so this was previously skipped
# entirely on the assumption there was nothing to conflict with (see
# bootstrap_package_manifest's docstring). That assumption breaks when
# run-selftest.sh runs against a real packaged copy instead (e.g. from
# make-baremetal-package.sh's own clean-room self-test invocation): a
# real package always ships PACKAGE-MANIFEST.sha256, so prepare_kit_copy()
# above carried an unmodified one over from KIT_SRC, and it would still
# describe the PRE-revert file contents -- run-benchmark.sh's own
# package-integrity phase would then fail on self-test.py/analyze-
# results.py for the wrong reason (a stale hash) before ever reaching the
# real regression this block exists to reproduce (smoke-verification).
# Regenerating it here, once the revert is final, keeps the manifest
# honest either way.
bootstrap_package_manifest "$KIT_REPRO"

log "Launcher-level fake run (module rule reverted): expect it to fail at smoke-verification"
REPRO_LOG="$WORKROOT/repro.log"
repro_rc=0
launch_benchmark "$KIT_REPRO" "$REPRO_LOG" || repro_rc=$?
cp "$WORKROOT/analyze-results.py.fixed" "$ANALYZER"
cp "$WORKROOT/self-test.py.fixed" "$SELFTEST_PY"
[[ "$repro_rc" -ne 0 ]] ||
  die "the launcher-level fake run passed with the module-rule fix reverted -- it cannot detect this regression"
grep -q 'PHASE smoke-verification started' "$REPRO_LOG" ||
  die "the reverted-module-rule run failed before reaching smoke-verification; see $REPRO_LOG"
grep -q 'verification: FAIL: control build lacks pg_wait_event_tracing' "$REPRO_LOG" ||
  die "the reverted-module-rule run failed, but not with the expected 'control build lacks pg_wait_event_tracing'; see $REPRO_LOG"
grep -q '^Phase: smoke-verification$' "$REPRO_LOG" ||
  die "run-benchmark.sh did not report smoke-verification as the failed phase; see $REPRO_LOG"
log "  PASS: reproduced the real outage exactly (module rule reverted -> smoke-verification fails with 'control build lacks pg_wait_event_tracing'), then restored the fix"

# --- --reuse-builds must refuse a stale "control" install that still has
# --- the tracing module (what a build from before "build the control
# --- installation without the tracing module" would leave behind, or
# --- work/ copied over from such a build): verify_reusable_builds.py and
# --- 02-run-matrix.sh's own preflight both apply
# --- build_manifest_rules.validate_installed_builds(), so this exercises
# --- the one shared rule both now use. Reuses kit-repro's already-built
# --- work/ (the module-rule revert above never touched 01-build-all.sh or
# --- its output). ----------------------------------------------------------
log "verify_reusable_builds.py: expect a clean pass against the fake build"
( cd "$KIT_REPRO" && "$PYTHON_BIN" verify_reusable_builds.py "$KIT_REPRO" ) ||
  die "verify_reusable_builds.py rejected a fresh, correct fake build"
log "  PASS: verify_reusable_builds.py accepts the fresh build"

log "verify_reusable_builds.py: stale control install with a tracing module must be refused"
STALE_MODULE="$KIT_REPRO/work/install/ctrlop/lib/pg_wait_event_tracing.so"
echo "stale module left over from a pre-fix build" >"$STALE_MODULE"
set +e
stale_output=$(cd "$KIT_REPRO" && "$PYTHON_BIN" verify_reusable_builds.py "$KIT_REPRO" 2>&1)
stale_rc=$?
set -e
rm -f -- "$STALE_MODULE"
[[ "$stale_rc" -ne 0 ]] ||
  die "verify_reusable_builds.py accepted a control install with a stale tracing module"
printf '%s\n' "$stale_output" | grep -q "control unexpectedly contains the tracing module" ||
  die "verify_reusable_builds.py refused the stale build, but not for the expected reason; got: $stale_output"
log "  PASS: verify_reusable_builds.py refuses a stale control+module install (--reuse-builds would refuse to run)"

# --- co-resident PostgreSQL must be refused by 00-check-host.sh itself,
# --- not discovered hours later when crossover/run.sh's own "otherwise
# --- idle" check refuses to start (reports/wpf-report.md, Addendum 5).
# --- check_coresident_postgres() uses a real `pgrep -x postgres`/
# --- `postmaster`, regardless of SELFTEST_FAKE_PREFIX, so a stub process
# --- whose executable is literally named "postgres" exercises the exact
# --- same code path a real co-resident PostgreSQL server would. This
# --- must NOT be (and is not) confused with this self-test's OWN fake
# --- postgres: that is a "#!/usr/bin/env bash" script, whose comm the
# --- kernel reports as "bash" (the shebang interpreter), never
# --- "postgres" -- pgrep -x only ever matches an executable actually
# --- named postgres/postmaster. The stub is a tiny locally-compiled C
# --- binary (cc is already a required tool), not a renamed copy of a
# --- system binary like /bin/sleep: on modern macOS (Apple Silicon
# --- hardened runtime / arm64e), execing a copy of a signed system
# --- binary from a different path is silently killed at exec time --
# --- ps then shows nothing and pgrep never sees it, so that approach
# --- looked like "the check works" while actually never exercising it. -
log "00-check-host.sh: a co-resident postgres process must be refused"
STUB_POSTGRES_DIR="$WORKROOT/coresident-stub"
mkdir -p "$STUB_POSTGRES_DIR"
STUB_POSTGRES="$STUB_POSTGRES_DIR/postgres"
cat >"$STUB_POSTGRES_DIR/postgres.c" <<'C'
#include <unistd.h>
int main(void) { sleep(30); return 0; }
C
cc -o "$STUB_POSTGRES" "$STUB_POSTGRES_DIR/postgres.c" ||
  die "could not compile the co-resident-postgres stub"
"$STUB_POSTGRES" &
stub_pid=$!
sleep 0.3 # let pgrep actually see the new process
set +e
coresident_output=$(
  cd "$KIT_REPRO" &&
  SELFTEST_FAKE_PREFIX="$KIT_REPRO/selftest-fakebin" \
  SERVER_CPUS="$SERVER_CPUS" PGBENCH_CPUS="$PGBENCH_CPUS" \
  "$BASH_BIN" ./00-check-host.sh 2>&1
)
coresident_rc=$?
set -e
kill "$stub_pid" 2>/dev/null || true
wait "$stub_pid" 2>/dev/null || true
[[ "$coresident_rc" -ne 0 ]] ||
  die "00-check-host.sh accepted a host with a co-resident postgres process"
printf '%s\n' "$coresident_output" | grep -q "a PostgreSQL server is already running on this host" ||
  die "00-check-host.sh refused the co-resident postgres process, but not with the expected message; got: $coresident_output"
log "  PASS: 00-check-host.sh refuses a co-resident postgres process"

# --- the plateau-probe regression: plateau-probe.sh's run_session() must
# --- not leak `pg_ctl ... stop`'s stdout into the captured tps value.
# --- Reuses kit-repro's build (work/install/base-a). -----------------------
log "plateau-probe.sh (fixed): expect a clean run"
(
  cd "$KIT_REPRO" &&
  PATH="$BASH_SHIM_DIR:$KIT_REPRO/selftest-fakebin:$PATH" \
  SELFTEST_FAKE_PREFIX="$KIT_REPRO/selftest-fakebin" \
  "$BASH_BIN" ./plateau-probe.sh
) >"$WORKROOT/plateau-fixed.log" 2>&1 ||
  die "plateau-probe.sh failed even with the fix in place; see $WORKROOT/plateau-fixed.log"
grep -q '"selected_variant"' "$WORKROOT/plateau-fixed.log" ||
  die "plateau-probe.sh (fixed) did not produce a result; see $WORKROOT/plateau-fixed.log"
log "  PASS: plateau-probe.sh completed cleanly with the fix in place"

log "plateau-probe.sh (fix reverted): expect the ORIGINAL bug to reproduce"
PROBE="$KIT_REPRO/plateau-probe.sh"
cp "$PROBE" "$WORKROOT/plateau-probe.sh.fixed"
"$PYTHON_BIN" - "$PROBE" <<'PY'
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
(
  cd "$KIT_REPRO" &&
  PATH="$BASH_SHIM_DIR:$KIT_REPRO/selftest-fakebin:$PATH" \
  SELFTEST_FAKE_PREFIX="$KIT_REPRO/selftest-fakebin" \
  "$BASH_BIN" ./plateau-probe.sh
) >"$WORKROOT/plateau-reverted.log" 2>&1
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

rm -rf -- "$KIT_REPRO"

# ===========================================================================
# kit-full: the fix intact, drive run-benchmark.sh all the way through
# every phase.
# ===========================================================================
KIT_FULL=$(prepare_kit_copy kit-full)
bootstrap_package_manifest "$KIT_FULL"
KIT="$KIT_FULL"
log "Fake kit copy (full run): $KIT_FULL"
FULL_LOG="$WORKROOT/full.log"
full_rc=0
launch_benchmark "$KIT_FULL" "$FULL_LOG" || full_rc=$?
[[ "$full_rc" -eq 0 ]] ||
  die "the launcher-level fake run failed; see $FULL_LOG (tail: $(tail -n 20 "$FULL_LOG"))"
grep -q 'RAW CAPTURE SUCCESS' "$FULL_LOG" ||
  die "the launcher-level fake run did not report RAW CAPTURE SUCCESS; see $FULL_LOG"
for phase in preflight kit-self-test initial-idle-check build disassembly \
             plateau-probe smoke smoke-verification cooldown final-host-check \
             matrix collection crossover-smoke crossover; do
  grep -q "PHASE $phase started" "$FULL_LOG" ||
    die "the launcher-level fake run never started phase '$phase'; see $FULL_LOG"
  grep -q "PHASE $phase completed" "$FULL_LOG" ||
    die "the launcher-level fake run never completed phase '$phase'; see $FULL_LOG"
done
log "  PASS: every phase (preflight through crossover) ran, in order, and RAW CAPTURE SUCCESS was reported"

log "analyze-raw-archive.sh on the produced matrix archive"
MATRIX_ARCHIVE=$(find "$KIT_FULL" -maxdepth 1 -name 'results-*.tar.gz' -type f -print | sort | tail -n 1)
[[ -n "$MATRIX_ARCHIVE" ]] || die "no results-*.tar.gz archive was produced by the fake run"
(
  cd "$KIT_FULL" &&
  PATH="$BASH_SHIM_DIR:$KIT_FULL/selftest-fakebin:$PATH" \
  SELFTEST_FAKE_PREFIX="$KIT_FULL/selftest-fakebin" \
  "$BASH_BIN" ./analyze-raw-archive.sh "$MATRIX_ARCHIVE" "$WORKROOT/full-archive-analysis"
) >"$WORKROOT/analyze-raw-archive.log" 2>&1 ||
  die "analyze-raw-archive.sh failed against the fake matrix archive; see $WORKROOT/analyze-raw-archive.log"
log "  PASS: analyze-raw-archive.sh accepted the fake matrix archive"

# ===========================================================================
# kit-reuse: BAREMETAL-RUNBOOK-v11.md's "Restarting after a failure" restart
# sequence, verified exactly: a fresh extraction with ONLY work/ copied
# across from kit-full's completed build, then --reuse-builds from there.
# ===========================================================================
KIT_REUSE=$(prepare_kit_copy kit-reuse)
bootstrap_package_manifest "$KIT_REUSE"
KIT="$KIT_REUSE"
log "Fake kit copy (reuse-builds restart): $KIT_REUSE"
rm -rf -- "$KIT_REUSE/work"
cp -a "$KIT_FULL/work" "$KIT_REUSE/work"
REUSE_LOG="$WORKROOT/reuse.log"
reuse_rc=0
launch_benchmark "$KIT_REUSE" "$REUSE_LOG" --reuse-builds || reuse_rc=$?
[[ "$reuse_rc" -eq 0 ]] ||
  die "--reuse-builds fake run failed; see $REUSE_LOG (tail: $(tail -n 20 "$REUSE_LOG"))"
grep -q 'Existing builds verified byte-for-byte; skipping 01-build-all.sh.' "$REUSE_LOG" ||
  die "--reuse-builds did not report reusing the copied-over build; see $REUSE_LOG"
grep -q 'PHASE build started' "$REUSE_LOG" &&
  die "--reuse-builds still ran the build phase instead of skipping it; see $REUSE_LOG"
grep -q 'RAW CAPTURE SUCCESS' "$REUSE_LOG" ||
  die "--reuse-builds fake run did not report RAW CAPTURE SUCCESS; see $REUSE_LOG"
log "  PASS: --reuse-builds restart sequence (extract fresh, copy only work/, --reuse-builds) skips the build and runs to completion"

rm -rf -- "$KIT_FULL" "$KIT_REUSE"

log "ALL FAKE-BINARY SELF-TEST STAGES PASSED"
