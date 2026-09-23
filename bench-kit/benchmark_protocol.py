"""Canonical constants for the v11 bare-metal benchmark protocol.

See briefs/brief-v11-wpc-kit.md on the v11-notes branch.
Unlike the v10 kit this comparison is a single seven-configuration matrix
against vanilla PostgreSQL (there is no v9/v10 pair): master, master-aa,
control, hook-null, module-off, stats, trace. CPU affinity is no longer
pinned to one fixed two-socket topology; SERVER_CPUS/PGBENCH_CPUS are
read from the environment (see cpu_affinity.py) and verified at runtime
for whatever topology the host actually has.
"""

from __future__ import annotations

import os

from w3_qualification import THRESHOLDS


SCHEMA_VERSION = 11
BENCHMARK_SERIES = "wet-v11"

CONFIGS = (
    "master",
    "master-aa",
    "control",
    "hook-null",
    "module-off",
    "stats",
    "trace",
)

# Which build each configuration runs on. "control" is its own build
# (patched + patches-control/0001-*.patch); hook-null/module-off/stats/
# trace all run the same "patched" (v11 series) build with different
# shared_preload_libraries / capture settings.
BUILD_FOR_CONFIG = {
    "master": "baseline-a",
    "master-aa": "baseline-b",
    "control": "control",
    "hook-null": "patched",
    "module-off": "patched",
    "stats": "patched",
    "trace": "patched",
}
BUILD_NAMES = ("baseline-a", "baseline-b", "control", "patched")

WORKLOADS = ("W1", "W3", "W4", "W5", "W6c")
PGBENCH_WORKLOADS = ("W3", "W4", "W5", "W6c")

# Configurations whose mode proof requires the module absent entirely
# (empty shared_preload_libraries, no pg_wait_event_tracing extension).
MODULE_ABSENT_CONFIGS = ("master", "master-aa", "control", "hook-null")
# Configurations whose mode proof requires the module loaded.
MODULE_LOADED_CONFIGS = ("module-off", "stats", "trace")
ACTIVE_CONFIGS = ("stats", "trace")  # capture actually recording timing data
TRACE_CONFIGS = ("trace",)

CAPTURE_FOR_CONFIG = {
    "module-off": "off",
    "stats": "stats",
    "trace": "trace",
}

W1_FUNCTIONS = (
    "test_wait_primitive_latch_set",
    "test_wait_primitive_latch_timeout",
    "test_wait_primitive_file_read",
    "test_wait_primitive_usleep0",
    "test_wait_primitive_report_only",
)

# Every contrast the analysis reports, as (label, treatment, reference).
CONTRASTS_VS_MASTER = (
    ("control", "control", "master"),
    ("hook-null", "hook-null", "master"),
    ("module-off", "module-off", "master"),
    ("stats", "stats", "master"),
    ("trace", "trace", "master"),
)
AA_CONTRAST = ("master-aa", "master-aa", "master")
LAYOUT_CONTRAST = ("hook-null-minus-control", "hook-null", "control")
WITHIN_PATCHED_CONTRASTS = (
    ("module-off-minus-hook-null", "module-off", "hook-null"),
    ("stats-minus-module-off", "stats", "module-off"),
    ("trace-minus-module-off", "trace", "module-off"),
)
ALL_CONTRASTS = (
    (AA_CONTRAST,)
    + CONTRASTS_VS_MASTER
    + (LAYOUT_CONTRAST,)
    + WITHIN_PATCHED_CONTRASTS
)

RESULT_FIELDS = (
    "run_index",
    "seed",
    "timestamp_utc",
    "block",
    "position",
    "config",
    "build",
    "workload",
    "repetition",
    "shared_buffers",
    "clients",
    "duration_s",
    "warmup_s",
    "iterations",
    "ns_per_iteration",
    "tps",
    "latency_avg_ms",
    "measurement_samples",
    "measurement_interval_s",
    "pgbench_cpu_percent",
    "pgbench_cpu_capacity_fraction",
    "cpu_freq_khz_mean",
    "cpu_freq_khz_min",
    "cpu_freq_khz_max",
    "numa_local_fraction",
    "meminfo_cached_kb_before",
    "meminfo_cached_kb_after",
    "load_average_before",
    "timing_clock_source",
    "server_cpus",
    "pgbench_cpus",
    "server_log",
)

BOUND_KIT_FILES = (
    "00-check-host.sh",
    "01-build-all.sh",
    "01b-disassemble.sh",
    "02-run-matrix.sh",
    "03-collect.sh",
    "build_manifest_rules.py",
    "lib-python.sh",
    "plateau-probe.sh",
    "analyze-raw-archive.sh",
    "wait-for-idle.sh",
    "analyze-results.py",
    "benchmark_protocol.py",
    "cpu_affinity.py",
    "w3_qualification.py",
    "wilcoxon.py",
    "stats_common.py",
    "latin_square.py",
    "sources.conf",
    "sources_conf.py",
    "workloads/w3-short-lwlock.sql",
    "workloads/recording-proof.sql",
    "workloads/w3-qualification.sql",
)

RUNS = 16  # the real protocol's repetition count -- fixed regardless of
# SELFTEST_FAKE_PREFIX: self-test.py's own synthetic 560-cell (7x5x16)
# clean-room tree is unconditional and has nothing to do with fake
# binaries, so this constant must not react to that environment variable
# (see FULL_PROFILE below, which is the one that does).
_FAKE = bool(os.environ.get("SELFTEST_FAKE_PREFIX"))

# BENCHMARK_REHEARSAL=1: a REAL rehearsal of this launcher (real configure/
# make/gcc, real pg_ctl/pgbench, real timing) against real PostgreSQL
# builds, typically inside a VM, before trusting a from-scratch kit on real
# hardware -- see 00-check-host.sh for the companion VM-incapable-host-check
# relaxation. Unlike SELFTEST_FAKE_PREFIX, this must still measure real
# elapsed time and produce real pgbench/W1 numbers (they are just not
# evidence), so only the multiplicative knobs that turn a multi-hour run
# into a multi-hour run TIMES SIXTEEN are compressed here -- repetition
# count, the per-cell measurement window, and session counts -- while
# per-cell overhead (warmup, quiescence, pgbench data-set scale, W1
# iteration count) and the smoke profile (already short by design) stay at
# their real values, so the rehearsal still exercises real-shaped timing
# and data volume.
_REHEARSAL = os.environ.get("BENCHMARK_REHEARSAL") == "1"

# Fake-binaries self-test: a launcher-level preflight run must finish in
# well under a real run's hours, so every timed/repeated knob below is
# compressed for SELFTEST_FAKE_PREFIX -- this is the ONE place each of
# them is compressed; 02-run-matrix.sh, plateau-probe.sh, and the
# crossover all read the resulting values (or their own analogous
# profile, for the crossover) rather than hardcoding a fake-mode number
# themselves. Never used for a real run.
def _early_aa_gate_repetitions(fake: bool, rehearsal: bool) -> tuple:
    return (1,) if (fake or rehearsal) else (6, 10)


def early_aa_gate_repetitions(rehearsal: bool) -> tuple:
    """Verifier-facing counterpart of EARLY_AA_GATE_REPETITIONS -- see
    full_profile() above for why rehearsal is an explicit parameter but
    fake is still read ambiently."""
    return _early_aa_gate_repetitions(_FAKE, rehearsal)


EARLY_AA_GATE_REPETITIONS = _early_aa_gate_repetitions(_FAKE, _REHEARSAL)
EARLY_AA_GATE_WORKLOAD = "W4"
EARLY_AA_GATE_MAX_HALF_WIDTH_PERCENT = 1.0

def _full_profile(fake: bool, rehearsal: bool) -> dict:
    """The full-matrix protocol profile for a given (fake, rehearsal) mode
    pair. A single function so every consumer -- the ambient FULL_PROFILE
    below (used by producers: 02-run-matrix.sh, plateau-probe.sh,
    self-test.py's own launcher-driven fake run) and
    analyze-results.py's verify_protocol() (used by a *verifier*, which
    must judge an archive by what it says about itself, not by the
    verifying process's own environment -- see the rehearsal parameter's
    call site) -- agree on exactly what each mode compresses."""
    runs_per_cell = 1 if (fake or rehearsal) else RUNS
    return {
        "mode": "full",
        "runs_per_cell": runs_per_cell,
        "expected_cells": len(CONFIGS) * len(WORKLOADS) * runs_per_cell,
        "duration_seconds": 1 if fake else (5 if rehearsal else 30),
        # 30s (not the real 10s) under BENCHMARK_REHEARSAL=1: a real host
        # passing at 10s proved the mode-proof query (recording-proof.sql,
        # w3-qualification.sql) has real headroom there, but a VM/emulated
        # rehearsal can legitimately run that same query slower without that
        # meaning anything about the patch -- found when a real rehearsal
        # died at "mode proof consumed the entire warmup window" on the
        # "trace" config specifically (the heaviest capture path) under QEMU
        # emulation. 02-run-matrix.sh's own guard is unchanged and still
        # fail-closed outside rehearsal mode; if even this wider budget is
        # not enough, that guard downgrades to a REHEARSAL NOTE instead of
        # dying, and every measured proof duration is recorded per cell
        # under results/recording-proofs/ regardless of mode.
        "warmup_seconds": 3 if fake else (30 if rehearsal else 10),
        "quiescence_seconds": 0 if fake else 5,
        "pgbench_scale": 1 if fake else 100,
        # 1e6 (not the real 1e8) under BENCHMARK_REHEARSAL=1: W1 is a single
        # backend running a tight SELECT loop over test_wait_primitive with no
        # pgbench, no warmup/duration window at all, and no per-cell wall-time
        # floor the way pgbench workloads have -- so nothing else bounds its
        # run time. A real host executes 1e8 iterations in well under a
        # minute; under QEMU emulation, each iteration's underlying syscall
        # (latch/timeout/file-read/usleep) is slow enough that a real
        # rehearsal ran the W1 cell for master-aa alone for 53+ minutes at
        # 99% CPU before being killed -- seven W1 cells (one per config)
        # would have added roughly six hours. Real mode is unaffected;
        # fake mode already uses 1000.
        "w1_iterations": 1000 if fake else (1_000_000 if rehearsal else 100_000_000),
    }


def full_profile(rehearsal: bool) -> dict:
    """The full-matrix profile a *verifier* should expect from an archive,
    given whether the archive says (via its own recorded host-check.json
    benchmark_rehearsal flag, not this process's own BENCHMARK_REHEARSAL)
    it is a rehearsal or a real capture. The FAKE dimension is still read
    ambiently (_FAKE) rather than taking a second explicit parameter: a
    SELFTEST_FAKE_PREFIX fake-binaries archive is always verified
    in-process, on the same host, by the same self-test run that produced
    it (never copied elsewhere the way analyze-raw-archive.sh's rehearsal
    archives are), so the ambient environment is always correct for that
    dimension -- unlike BENCHMARK_REHEARSAL, which is not."""
    return _full_profile(_FAKE, rehearsal)


FULL_PROFILE = _full_profile(_FAKE, _REHEARSAL)

# plateau-probe.sh alternates pinned/unpinned sessions this many times
# each (8 total real sessions); analyze-results.py's verify_plateau_probe()
# checks the retained pinned_tps/unpinned_tps lists against this same
# count, so a fake-mode or rehearsal launcher run needs it small too (2
# total sessions).


def _plateau_probe_sessions_per_variant(fake: bool, rehearsal: bool) -> int:
    return 1 if (fake or rehearsal) else 4


def plateau_probe_sessions_per_variant(rehearsal: bool) -> int:
    """Verifier-facing counterpart of PLATEAU_PROBE_SESSIONS_PER_VARIANT --
    see full_profile() above for why rehearsal is an explicit parameter
    but fake is still read ambiently."""
    return _plateau_probe_sessions_per_variant(_FAKE, rehearsal)


PLATEAU_PROBE_SESSIONS_PER_VARIANT = _plateau_probe_sessions_per_variant(_FAKE, _REHEARSAL)

# crossover/protocol.py's SESSIONS (2 fake/rehearsal, 16 real) mirrors this
# same _FAKE/_REHEARSAL gate for the exact same reason; kept there (not
# here) because nothing outside crossover/ needs it and protocol.py already
# carries the other crossover-specific constants (MEASUREMENT_SECONDS etc).


def _smoke_profile(fake: bool, rehearsal: bool) -> dict:
    return {
        "mode": "smoke",
        "runs_per_cell": 1,
        "expected_cells": len(CONFIGS) * len(WORKLOADS),
        "duration_seconds": 1 if fake else 6,
        # Same rehearsal-mode widening as FULL_PROFILE above, same reason: the
        # real host's own smoke matrix already passed all 35 cells at 3s, but
        # a real rehearsal failed at smoke cell 12 (config=trace, W4) with
        # "mode proof consumed the entire warmup window" under QEMU emulation.
        "warmup_seconds": 3 if not rehearsal else 30,
        "quiescence_seconds": 0,
        "pgbench_scale": 1,
        "w1_iterations": 1000 if fake else 100_000,
    }


def smoke_profile(rehearsal: bool) -> dict:
    """Verifier-facing counterpart of SMOKE_PROFILE -- see full_profile()
    above for why rehearsal is an explicit parameter but fake is still
    read ambiently."""
    return _smoke_profile(_FAKE, rehearsal)


SMOKE_PROFILE = _smoke_profile(_FAKE, _REHEARSAL)

# Real-time tolerance for a pgbench cell's measured post-warmup progress
# interval against its configured duration_seconds -- the single source
# 02-run-matrix.sh's own inline verification and analyze-results.py's
# verify_rows() both use, so a fake-mode fix to one cannot silently
# disagree with the other (crossover/protocol.py's MEASUREMENT_NS_LOW/HIGH
# is the same idea for the persistent-backend crossover's block boundaries).
# Real and BENCHMARK_REHEARSAL=1 both measure a real pgbench -T run with
# real wall-clock progress lines, so both keep the historical -2s/+1s
# absolute window (already comfortably relative-generous for a rehearsal's
# shorter duration_seconds=5). SELFTEST_FAKE_PREFIX's duration_seconds=1 is
# a stub pgbench's "sleep 1" per simulated second plus real subprocess/
# parsing overhead around it, which can run noticeably long on a slow or
# busy laptop without meaning anything about the code under test -- same
# fragility as crossover/protocol.py's MEASUREMENT_NS_LOW/HIGH, fixed the
# same way: a generous multiplicative window instead of a tight absolute
# one.
def duration_interval_bounds(
    duration_seconds: float, fake: bool | None = None,
) -> tuple[float, float]:
    if fake is None:
        fake = _FAKE
    if fake:
        return duration_seconds * 0.5, duration_seconds * 5.0
    return duration_seconds - 2.0, duration_seconds + 1.0

SHARED_BUFFERS_FOR_WORKLOAD = {
    "W1": "128MB",
    "W3": "16MB",
    "W4": "4GB",
    "W5": "4GB",
    "W6c": "32MB",
}
CLIENTS_FOR_WORKLOAD = {"W3": 8, "W4": 16, "W5": 16, "W6c": 32}

W3_PROTOCOL = dict(THRESHOLDS)
W3_PROTOCOL["scope"] = "each active (stats/trace) W3 client population during warmup"

MODULE_GUCS = {
    "capture": "pg_wait_event_tracing.capture",
    "max_tranches": "pg_wait_event_tracing.max_tranches",
    "trace_ring_size": "pg_wait_event_tracing.trace_ring_size",
}
MAX_TRANCHES = 192
TRACE_RING_SIZE = "4MB"

EQUIVALENCE_MARGINS = {
    "W1": {"units": "ns/iteration", "value": 2.0},
    "pgbench": {"units": "log_tps", "value": None},  # filled by margin_log()
}


def pgbench_margin_log(percent: float = 2.0) -> float:
    """Convert the declared +/-2% pgbench margin to a symmetric bound on
    log(TPS) differences: |log(t) - log(r)| <= log(1 + percent/100)."""
    import math

    return math.log(1.0 + percent / 100.0)
