"""Canonical constants for the v11 bare-metal benchmark protocol.

See /Users/dmitryfomin/work/git/postgres_patch/v11/briefs/brief-v11-wpc-kit.md.
Unlike the v10 kit this comparison is a single seven-configuration matrix
against vanilla PostgreSQL (there is no v9/v10 pair): master, master-aa,
control, hook-null, module-off, stats, trace. CPU affinity is no longer
pinned to one fixed two-socket topology; SERVER_CPUS/PGBENCH_CPUS are
read from the environment (see cpu_affinity.py) and verified at runtime
for whatever topology the host actually has.
"""

from __future__ import annotations

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

RUNS = 16
EARLY_AA_GATE_REPETITIONS = (6, 10)
EARLY_AA_GATE_WORKLOAD = "W4"
EARLY_AA_GATE_MAX_HALF_WIDTH_PERCENT = 1.0

FULL_PROFILE = {
    "mode": "full",
    "runs_per_cell": RUNS,
    "expected_cells": len(CONFIGS) * len(WORKLOADS) * RUNS,
    "duration_seconds": 30,
    "warmup_seconds": 10,
    "quiescence_seconds": 5,
    "pgbench_scale": 100,
    "w1_iterations": 100_000_000,
}

SMOKE_PROFILE = {
    "mode": "smoke",
    "runs_per_cell": 1,
    "expected_cells": len(CONFIGS) * len(WORKLOADS),
    "duration_seconds": 6,
    "warmup_seconds": 3,
    "quiescence_seconds": 0,
    "pgbench_scale": 1,
    "w1_iterations": 100_000,
}

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
