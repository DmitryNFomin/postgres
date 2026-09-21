#!/usr/bin/env python3
"""Fixed protocol constants for the persistent-backend W6c crossover,
second stage (brief-v11-wpc-kit.md: "reuse the v10 persistent-backend
crossover unchanged in protocol but on the patched installation only
(there is no v9/v10 pair now)"). Every session runs the same "patched"
v11-series build; there is no build pairing, so the analysis unit is one
independent session (no matched-pair contrast)."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path


SEQUENCES = {
    "A": (
        ("off", "off_stats_before"),
        ("stats", "stats_first"),
        ("stats", "stats_second"),
        ("off", "off_stats_after"),
        ("off", "off_trace_before"),
        ("trace", "trace_first"),
        ("trace", "trace_second"),
        ("off", "off_trace_after"),
    ),
    "B": (
        ("off", "off_trace_before"),
        ("trace", "trace_first"),
        ("trace", "trace_second"),
        ("off", "off_trace_after"),
        ("off", "off_stats_before"),
        ("stats", "stats_first"),
        ("stats", "stats_second"),
        ("off", "off_stats_after"),
    ),
}

BLOCKS_PER_SESSION = 8
_FAKE = bool(os.environ.get("SELFTEST_FAKE_PREFIX"))
# BENCHMARK_REHEARSAL=1: a real rehearsal of run-benchmark.sh against real
# builds (see benchmark_protocol.py for the full rationale). Only SESSIONS
# is compressed here (16 -> 2, same value as the fake-binaries self-test,
# for the same "analyze.py requires an even A/B split" reason below); a
# rehearsal must still measure a real MEASUREMENT_SECONDS/SETTLE_SECONDS/
# INITIAL_WARMUP_SECONDS window with real pgbench, so those stay at their
# real values (unlike SELFTEST_FAKE_PREFIX, which has nothing real to wait
# out and compresses those too).
_REHEARSAL = os.environ.get("BENCHMARK_REHEARSAL") == "1"
if _FAKE:
    # Fake-binaries self-test: BLOCKS_PER_SESSION stays exactly 8
    # (analyze.py hard-requires it), but there is nothing real to wait out
    # per block, and a launcher-level preflight needs to finish in well
    # under a real run's hours, so SESSIONS drops to 2 (1 "A" + 1 "B" --
    # analyze.py requires an even split) and every real-time window is
    # compressed. run-worker.sh's own SESSIONS/BLOCK_SECONDS/
    # SETTLE_SECONDS/INITIAL_WARMUP_SECONDS mirror these same numbers by
    # convention (it is a bash script, so it cannot import this module) --
    # keep them in agreement if any of these change. 3, not 1 or 2, for
    # MEASUREMENT_SECONDS: extract_blocks.py's within-block CV needs at
    # least two full seconds of per-second samples after boundary
    # trimming (statistics.stdev() requires 2+ points), and a
    # non-second-aligned start/end can trim a full second off either edge.
    # Never used for a real run.
    SESSIONS = 2
    MEASUREMENT_SECONDS = 3
    SETTLE_SECONDS = 0
    INITIAL_WARMUP_SECONDS = 1
else:
    SESSIONS = 2 if _REHEARSAL else 16
    MEASUREMENT_SECONDS = 30
    SETTLE_SECONDS = 3
    INITIAL_WARMUP_SECONDS = 10
# Single source for the real-nanosecond block-measurement tolerance every
# consumer (analyze.py's validate_results(), extract_blocks.py's
# build_session_results(), self-test.py's synthetic evidence) must agree
# on -- each used to hardcode its own 29_500_000_000/31_500_000_000 pair,
# which is exactly the real (MEASUREMENT_SECONDS=30) case of this same
# -0.5s/+1.5s slack, so a fake-mode MEASUREMENT_SECONDS caused each
# independent copy to disagree with the others instead of just this one
# constant changing everywhere at once.
MEASUREMENT_NS_LOW = int((MEASUREMENT_SECONDS - 0.5) * 1_000_000_000)
MEASUREMENT_NS_HIGH = int((MEASUREMENT_SECONDS + 1.5) * 1_000_000_000)
CLIENTS = 32
THREADS = 8
SCALE = 100
MAX_PGBENCH_CAPACITY_FRACTION = 0.90
EQUIVALENCE_MARGIN_PERCENT = 2.0
PLACEBO_MARGIN_PERCENT = 1.0
BOUND_FILES = (
    "analyze.py",
    "extract_blocks.py",
    "generate_schedule.py",
    "host_state.py",
    "protocol.py",
    "run-worker.sh",
    "verify_affinity.py",
    "verify_installs.py",
    "verify_runtime.py",
)

SCHEDULE_FIELDS = (
    "session_index",
    "sequence",
    "pgbench_seed",
)

RESULT_FIELDS = (
    "session_index",
    "sequence",
    "pgbench_seed",
    "block_index",
    "mode",
    "role",
    "measurement_start_ns",
    "measurement_end_ns",
    "full_seconds",
    "transactions",
    "tps",
    "latency_ms",
    "within_block_cv_percent",
    "failed_transactions",
    "pgbench_cpu_percent",
    "pgbench_capacity_fraction",
    "postmaster_pid",
    "pgbench_pid",
    "backend_pid_sha256",
    "server_cpus",
    "pgbench_cpus",
    "aggregate_log_prefix",
    "server_log",
)

PROOF_FIELDS = (
    "session_index",
    "block_index",
    "mode",
    "client_count",
    "clients_recording",
    "calls_before",
    "calls_after",
    "trace_records",
    "backend_pid_sha256",
)


def t95(df: int) -> float:
    """Two-sided 95% Student t critical value."""
    values = (
        None,
        12.706,
        4.303,
        3.182,
        2.776,
        2.571,
        2.447,
        2.365,
        2.306,
        2.262,
        2.228,
        2.201,
        2.179,
        2.160,
        2.145,
        2.131,
        2.120,
        2.110,
        2.101,
        2.093,
        2.086,
        2.080,
        2.074,
        2.069,
        2.064,
        2.060,
        2.056,
        2.052,
        2.048,
        2.045,
        2.042,
    )
    if df <= 0:
        raise ValueError("degrees of freedom must be positive")
    return values[df] if df < len(values) else 1.96


def file_hashes(root: Path) -> dict[str, str]:
    return {
        name: hashlib.sha256((root / name).read_bytes()).hexdigest()
        for name in BOUND_FILES
    }


def protocol_document(
    schedule_seed: int,
    package_manifest_sha256: str,
    build_manifest_sha256: str,
    bound_file_sha256: dict[str, str],
    server_cpus: str,
    pgbench_cpus: str,
) -> dict:
    return {
        "schema_version": 2,
        "benchmark": "wet-v11-w6c-persistent-crossover-second-stage",
        "sessions": SESSIONS,
        "blocks_per_session": BLOCKS_PER_SESSION,
        "measurement_seconds": MEASUREMENT_SECONDS,
        "settle_seconds": SETTLE_SECONDS,
        "initial_warmup_seconds": INITIAL_WARMUP_SECONDS,
        "clients": CLIENTS,
        "threads": THREADS,
        "scale": SCALE,
        "server_cpus": server_cpus,
        "pgbench_cpus": pgbench_cpus,
        "schedule_seed": schedule_seed,
        "kit_package_manifest_sha256": package_manifest_sha256,
        "kit_build_manifest_sha256": build_manifest_sha256,
        "analysis_unit": "one persistent-backend server session (independent; no build pairing)",
        "one_second_samples_are_independent": False,
        "sequences": {
            name: [mode for mode, _ in sequence]
            for name, sequence in SEQUENCES.items()
        },
        "outlier_policy": "retain every successful block; no post-hoc exclusions",
        "equivalence_margin_percent": EQUIVALENCE_MARGIN_PERCENT,
        "placebo_margin_percent": PLACEBO_MARGIN_PERCENT,
        "bound_file_sha256": bound_file_sha256,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("schedule_seed", type=int)
    parser.add_argument("package_manifest_sha256")
    parser.add_argument("build_manifest_sha256")
    parser.add_argument("tools_dir", type=Path)
    parser.add_argument("server_cpus")
    parser.add_argument("pgbench_cpus")
    args = parser.parse_args()
    document = protocol_document(
        args.schedule_seed,
        args.package_manifest_sha256,
        args.build_manifest_sha256,
        file_hashes(args.tools_dir),
        args.server_cpus,
        args.pgbench_cpus,
    )
    args.output.write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
