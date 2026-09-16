"""Canonical constants for the v10 bare-metal benchmark protocol."""

from __future__ import annotations

from cpu_affinity import CPU_PINNING, PGBENCH_CPUS, SERVER_CPUS
from w3_qualification import THRESHOLDS


CONFIGS = (
    "master",
    "master-aa",
    "v9-hook-null",
    "v9-module-off",
    "v9-stats",
    "v9-trace",
    "v10-hook-null",
    "v10-module-off",
    "v10-stats",
    "v10-trace",
)
WORKLOADS = ("W1", "W3", "W4", "W6c")
PGBENCH_WORKLOADS = ("W3", "W4", "W6c")
ACTIVE_CONFIGS = (
    "v9-stats",
    "v9-trace",
    "v10-stats",
    "v10-trace",
)
TRACE_CONFIGS = ("v9-trace", "v10-trace")
V10_V9_PAIRS = tuple(
    (mode, f"v10-{mode}", f"v9-{mode}")
    for mode in ("hook-null", "module-off", "stats", "trace")
)
RESULT_FIELDS = (
    "run_index",
    "seed",
    "timestamp_utc",
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
    "cpu_freq_khz_median_before",
    "cpu_freq_khz_median_after",
    "server_cpus",
    "pgbench_cpus",
    "server_log",
)

BOUND_KIT_FILES = (
    "00-check-host.sh",
    "01-build-all.sh",
    "02-run-matrix.sh",
    "03-collect.sh",
    "analyze-raw-archive.sh",
    "wait-for-idle.sh",
    "analyze-results.py",
    "benchmark_protocol.py",
    "cpu_affinity.py",
    "w3_qualification.py",
    "patches/0006-optimize-null-wait-event-hook-path.patch",
    "patches/0007-inline-attachment-needed-guard.patch",
    "workloads/w3-short-lwlock.sql",
    "workloads/recording-proof.sql",
    "workloads/w3-qualification.sql",
)

FULL_PROFILE = {
    "mode": "full",
    "runs_per_cell": 12,
    "expected_cells": 480,
    "duration_seconds": 30,
    "warmup_seconds": 10,
    "quiescence_seconds": 5,
    "pgbench_scale": 100,
    "w1_iterations": 100_000_000,
}

SMOKE_PROFILE = {
    "mode": "smoke",
    "runs_per_cell": 1,
    "expected_cells": 40,
    "duration_seconds": 6,
    "warmup_seconds": 3,
    "quiescence_seconds": 0,
    "pgbench_scale": 1,
    "w1_iterations": 100_000,
}

W3_PROTOCOL = dict(THRESHOLDS)
W3_PROTOCOL["scope"] = (
    "each v9/v10 stats and trace W3 client population during warmup"
)
