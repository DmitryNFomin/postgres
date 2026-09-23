#!/usr/bin/env python3
"""Synthetic tests for the persistent-backend crossover, second stage
(patched installation only, no build pairing)."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import analyze
import extract_blocks
import generate_schedule
import host_state
import verify_runtime
from protocol import (
    BLOCKS_PER_SESSION,
    BOUND_FILES,
    CLIENTS,
    MEASUREMENT_NS_HIGH,
    MEASUREMENT_SECONDS,
    PROOF_FIELDS,
    RESULT_FIELDS,
    SCHEDULE_FIELDS,
    SEQUENCES,
    SESSIONS,
    THREADS,
    _FAKE,
    file_hashes,
    protocol_document,
)

# Derived from protocol.py's own MEASUREMENT_SECONDS (which is itself
# gated on SELFTEST_FAKE_PREFIX) instead of a hardcoded 30_000_000_000,
# so this synthetic-evidence self-test agrees with analyze.py's
# validate_results() regardless of which mode this process happens to
# inherit -- a literal 30s figure here made this self-test fail whenever
# it ran with SELFTEST_FAKE_PREFIX set in its environment (e.g. nested
# inside a fake-binaries self-test run), even though this synthetic check
# never launches any binary at all.
_MEASUREMENT_NS = MEASUREMENT_SECONDS * 1_000_000_000
# extract_blocks.py's build_session_results() computes pgbench CPU% as
# (delta pgbench_cpu_ticks / clock_ticks_per_second) / elapsed_seconds,
# and rejects it above MAX_PGBENCH_CAPACITY_FRACTION -- a fixed 1000->
# 15400 tick delta was calibrated for the real 30-second window (giving
# ~60% capacity); holding it fixed while MEASUREMENT_SECONDS shrinks
# under SELFTEST_FAKE_PREFIX makes that same delta land over a much
# shorter elapsed_seconds and look like the driver saturated. Scale it
# with MEASUREMENT_SECONDS so the synthetic capacity stays ~60% either way.
_MEASUREMENT_END_TICKS = 1000 + round(14400 * MEASUREMENT_SECONDS / 30)


HERE = Path(__file__).resolve().parent
SERVER_CPUS = "1-15"
PGBENCH_CPUS = "16-23"


def synthetic_host_state(load_average: object = None) -> dict:
    return {
        "schema_version": 2,
        "timestamp_utc": "2026-03-08T00:00:00+00:00",
        "hostname": "synthetic-host",
        "kernel": "synthetic-kernel",
        "load_average": (
            [0.0, 0.0, 0.0] if load_average is None else load_average
        ),
        "online_cpus": "0-23",
        "smt_active": "0",
        "intel_pstate_no_turbo": "1",
        "governors": ["performance"] * 24,
    }


def write_csv(path: Path, fields: tuple[str, ...], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_analysis_fixture(root: Path) -> None:
    worker = root / "worker"
    tools = root / "tools"
    provenance = root / "provenance"
    logs = worker / "logs"
    backends = worker / "backend-pids"
    for path in (worker, tools, provenance, logs, backends):
        path.mkdir(parents=True, exist_ok=True)

    schedule_path = worker / "schedule.csv"
    seed = generate_schedule.generate(schedule_path, SESSIONS, seed=123456)
    schedule = list(csv.DictReader(schedule_path.open(encoding="utf-8")))
    (worker / "seed.txt").write_text(str(seed) + "\n", encoding="ascii")
    for name in BOUND_FILES:
        shutil.copyfile(HERE / name, tools / name)
    (provenance / "PACKAGE-MANIFEST.sha256").write_text(
        "synthetic package manifest\n", encoding="utf-8",
    )
    (provenance / "manifest.json").write_text(
        '{"schema_version": 11, "benchmark_series": "wet-v11"}\n', encoding="utf-8",
    )
    host = synthetic_host_state()
    (provenance / "host-before.json").write_text(
        json.dumps(host, sort_keys=True) + "\n", encoding="utf-8",
    )
    host["timestamp_utc"] = "2026-03-08T01:00:00+00:00"
    (provenance / "host-after.json").write_text(
        json.dumps(host, sort_keys=True) + "\n", encoding="utf-8",
    )
    protocol = protocol_document(
        seed,
        analyze.sha256(provenance / "PACKAGE-MANIFEST.sha256"),
        analyze.sha256(provenance / "manifest.json"),
        file_hashes(tools),
        SERVER_CPUS,
        PGBENCH_CPUS,
    )
    (worker / "protocol.json").write_text(
        json.dumps(protocol, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )

    proof_rows = []
    events = []
    session_inputs = []
    clock = 1_800_000_000_000_000_000
    for scheduled in schedule:
        session_index = int(scheduled["session_index"])
        sequence = scheduled["sequence"]
        backend_path = backends / "session-{}.txt".format(session_index)
        backend_path.write_text(
            "".join("{}\n".format(10_000 + session_index * 100 + index)
                    for index in range(CLIENTS)),
            encoding="ascii",
        )
        backend_digest = hashlib.sha256(backend_path.read_bytes()).hexdigest()
        server_log = logs / "server-{}.log".format(session_index)
        server_log.write_text("synthetic server log\n", encoding="utf-8")
        aggregate_prefix = logs / "aggregate-{}".format(session_index)
        per_thread_counts = [dict() for _ in range(THREADS)]

        session_level = 560_000.0 * (1.0 + (session_index % 5 - 2) * 0.006)
        for block_index, (mode, role) in enumerate(SEQUENCES[sequence], 1):
            mode_factor = {"off": 1.0, "stats": 0.991, "trace": 0.984}[mode]
            tps = session_level * mode_factor
            transition = clock
            start = transition + 5_000_000_000
            end = start + _MEASUREMENT_NS
            clock = end + 1_000_000_000
            events.extend([
                {
                    "timestamp_ns": transition, "phase": "transition_start",
                    "session_index": session_index, "block_index": block_index,
                    "mode": mode,
                },
                {
                    "timestamp_ns": start, "phase": "measurement_start",
                    "session_index": session_index, "block_index": block_index,
                    "mode": mode, "pgbench_cpu_ticks": 1000,
                    "pgbench_starttime_ticks": 500, "clock_ticks_per_second": 100,
                },
                {
                    "timestamp_ns": end, "phase": "measurement_end",
                    "session_index": session_index, "block_index": block_index,
                    "mode": mode, "pgbench_cpu_ticks": _MEASUREMENT_END_TICKS,
                    "pgbench_starttime_ticks": 500, "clock_ticks_per_second": 100,
                },
            ])
            calls_before = 0 if mode == "off" else 1000
            calls_after = 0 if mode == "off" else 2000
            proof_rows.append({
                "session_index": session_index, "block_index": block_index,
                "mode": mode, "client_count": CLIENTS,
                "clients_recording": 0 if mode == "off" else CLIENTS,
                "calls_before": calls_before, "calls_after": calls_after,
                "trace_records": 100 if mode == "trace" else 0,
                "backend_pid_sha256": backend_digest,
            })
            total_per_second = round(tps)
            for second in range(start // 1_000_000_000, end // 1_000_000_000):
                for thread in range(THREADS):
                    per_thread_counts[thread][second] = (
                        total_per_second // THREADS
                        + (1 if thread < total_per_second % THREADS else 0)
                    )
        first_second = min(per_thread_counts[0]) - 1
        last_second = max(per_thread_counts[0]) + 1
        for thread, counts in enumerate(per_thread_counts):
            suffix = ".999" if thread == 0 else ".999.{}".format(thread)
            with Path(str(aggregate_prefix) + suffix).open("w", encoding="utf-8") as aggregate:
                for second in range(first_second, last_second + 1):
                    aggregate.write(aggregate_row(second, counts.get(second, 1000)))
        session_inputs.append((
            aggregate_prefix,
            backend_path,
            {
                **scheduled,
                "postmaster_pid": 20_000 + session_index,
                "pgbench_pid": 999,
                "server_cpus": SERVER_CPUS,
                "pgbench_cpus": PGBENCH_CPUS,
                "aggregate_log_prefix": "logs/aggregate-{}".format(session_index),
                "server_log": "logs/server-{}.log".format(session_index),
            },
        ))

    write_csv(worker / "mode-proofs.csv", PROOF_FIELDS, proof_rows)
    with (worker / "events.jsonl").open("w", encoding="utf-8") as stream:
        for event in events:
            stream.write(json.dumps(event, sort_keys=True) + "\n")
    for aggregate_prefix, backend_path, metadata in session_inputs:
        extract_blocks.append_session_results(
            worker / "results.csv", worker / "events.jsonl",
            aggregate_prefix, backend_path, metadata,
        )


def test_schedule() -> None:
    with tempfile.TemporaryDirectory() as temporary:
        path = Path(temporary) / "schedule.csv"
        seed = generate_schedule.generate(path, SESSIONS, seed=42)
        assert seed == 42
        assert b"\r" not in path.read_bytes()
        rows = list(csv.DictReader(path.open(encoding="utf-8")))
        assert tuple(rows[0]) == SCHEDULE_FIELDS
        analyze.validate_schedule(rows)


def test_host_state() -> None:
    state = synthetic_host_state((0.0, 0.0, 0.0))
    host_state.validate(state)
    state["load_average"] = [0.0, 0.0, 0.0]
    host_state.validate(state)

    # BENCHMARK_REHEARSAL=1 parity with 00-check-host.sh (rehearsal.py):
    # host_state.py must relax exactly the same "no cpufreq governor
    # exposed" case the preflight already relaxed earlier in the same
    # run, and must never relax a governor that IS readable but genuinely
    # not "performance". This reproduces, and guards against regressing,
    # the real rehearsal outage this covers: rocky-8 rehearsal run 4
    # passed 00-check-host.sh's preflight (VM exposes no cpufreq governor
    # at all, relaxed there) and the full 35-cell matrix, then died at
    # crossover-smoke with "CPU governors are not all performance"
    # because host_state.py had no rehearsal awareness at all.
    saved_rehearsal = os.environ.get("BENCHMARK_REHEARSAL")
    try:
        missing_governor_state = synthetic_host_state()
        missing_governor_state["governors"] = []

        os.environ.pop("BENCHMARK_REHEARSAL", None)
        try:
            host_state.validate(dict(missing_governor_state))
        except RuntimeError:
            pass
        else:
            raise AssertionError(
                "a missing cpufreq governor was accepted without "
                "BENCHMARK_REHEARSAL=1"
            )

        os.environ["BENCHMARK_REHEARSAL"] = "1"
        host_state.validate(dict(missing_governor_state))  # must not raise

        wrong_governor_state = synthetic_host_state()
        wrong_governor_state["governors"] = ["powersave"] * 24
        try:
            host_state.validate(dict(wrong_governor_state))
        except RuntimeError:
            pass
        else:
            raise AssertionError(
                "a readable non-performance governor was accepted under "
                "BENCHMARK_REHEARSAL=1 -- only a MISSING governor is ever "
                "relaxed"
            )
    finally:
        if saved_rehearsal is None:
            os.environ.pop("BENCHMARK_REHEARSAL", None)
        else:
            os.environ["BENCHMARK_REHEARSAL"] = saved_rehearsal


def test_runtime_paths() -> None:
    observed_failure = Path(
        "/var/tmp/w6c-persistent-crossover.HMbUIs/"
        "w6c-persistent-crossover-20260917T121525Z/worker/sock"
    )
    try:
        verify_runtime.validate_socket_path(observed_failure, 55473)
    except RuntimeError as error:
        assert "maximum is 107" in str(error)
    else:
        raise AssertionError("observed overlong socket path was accepted")

    corrected = Path("/var/tmp/w6c-persistent-crossover.HMbUIs/sock")
    verify_runtime.validate_socket_path(corrected, 55473)
    assert len(
        str(verify_runtime.postgres_socket_path(corrected, 55473)).encode()
    ) < verify_runtime.MAX_UNIX_SOCKET_PATH_BYTES

    socket_name = ".s.PGSQL.55473"
    boundary_length = (
        verify_runtime.MAX_UNIX_SOCKET_PATH_BYTES - len(socket_name.encode()) - 2
    )
    verify_runtime.validate_socket_path(Path("/" + "x" * boundary_length), 55473)
    try:
        verify_runtime.validate_socket_path(
            Path("/" + "x" * (boundary_length + 1)), 55473,
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError("108-byte socket path was accepted")


def same_line_local_dependencies(line: str) -> list[tuple[str, str]]:
    assignments = re.findall(r"(?:^|\s)([A-Za-z_][A-Za-z0-9_]*)=([^\s]+)", line)
    names = {name for name, _ in assignments}
    return [
        (name, dependency)
        for name, value in assignments
        for dependency in re.findall(r"\$(?:\{)?([A-Za-z_][A-Za-z0-9_]*)", value)
        if dependency in names
    ]


def test_shell_nounset_declarations() -> None:
    assert same_line_local_dependencies(
        'local expected="$1" temporary="$expected.current"'
    ) == [("temporary", "expected")]

    worker = (HERE / "run-worker.sh").read_text(encoding="utf-8")
    for line_number, line in enumerate(worker.splitlines(), 1):
        stripped = line.strip()
        if not stripped.startswith("local "):
            continue
        conflicts = same_line_local_dependencies(stripped)
        if conflicts:
            raise AssertionError(
                "run-worker.sh:{} same-line local dependency: {}".format(
                    line_number, conflicts,
                )
            )


def aggregate_row(second: int, transactions: int) -> str:
    values = (second, transactions, transactions * 50, transactions * 2500,
              40, 60, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    return " ".join(str(value) for value in values) + "\n"


def test_extraction() -> None:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        events_path = root / "events.jsonl"
        backend_path = root / "backends.txt"
        result_path = root / "results.csv"
        prefix = root / "aggregate"
        backend_path.write_text(
            "".join("{}\n".format(1000 + index) for index in range(CLIENTS)),
            encoding="ascii",
        )
        events = []
        base = 1_800_000_000_200_000_000
        for block_index, (mode, _) in enumerate(SEQUENCES["A"], 1):
            start = base + (block_index - 1) * 40_000_000_000
            events.extend([
                {
                    "timestamp_ns": start - 2_000_000_000, "phase": "transition_start",
                    "session_index": 1, "block_index": block_index, "mode": mode,
                },
                {
                    "timestamp_ns": start, "phase": "measurement_start",
                    "session_index": 1, "block_index": block_index, "mode": mode,
                    "pgbench_cpu_ticks": 1000, "pgbench_starttime_ticks": 500,
                    "clock_ticks_per_second": 100,
                },
                {
                    "timestamp_ns": start + _MEASUREMENT_NS, "phase": "measurement_end",
                    "session_index": 1, "block_index": block_index, "mode": mode,
                    "pgbench_cpu_ticks": _MEASUREMENT_END_TICKS, "pgbench_starttime_ticks": 500,
                    "clock_ticks_per_second": 100,
                },
            ])
        with events_path.open("w", encoding="utf-8") as stream:
            for event in events:
                stream.write(json.dumps(event) + "\n")
        first_second = base // 1_000_000_000 - 5
        for thread in range(THREADS):
            suffix = ".777" if thread == 0 else ".777.{}".format(thread)
            with Path(str(prefix) + suffix).open("w", encoding="utf-8") as log:
                for second in range(first_second, first_second + 330):
                    log.write(aggregate_row(second, 1000))
        metadata = {
            "session_index": 1,
            "sequence": "A",
            "pgbench_seed": 1,
            "pgbench_pid": 777,
            "postmaster_pid": 888,
            "server_cpus": SERVER_CPUS,
            "pgbench_cpus": PGBENCH_CPUS,
            "aggregate_log_prefix": "logs/aggregate-1",
            "server_log": "logs/server-1.log",
        }
        rows = extract_blocks.append_session_results(
            result_path, events_path, prefix, backend_path, metadata,
        )
        assert len(rows) == BLOCKS_PER_SESSION
        assert all(
            int(row["full_seconds"]) == MEASUREMENT_SECONDS - 1 for row in rows
        )
        assert all(float(row["tps"]) == THREADS * 1000.0 for row in rows)
        assert all(int(row["failed_transactions"]) == 0 for row in rows)


def _extract_with_block_duration(
    root: Path, slow_block_index: int, duration_ns: int,
) -> list[dict]:
    """Like test_extraction() above, except block `slow_block_index`
    measures for `duration_ns` real nanoseconds instead of the nominal
    _MEASUREMENT_NS -- simulating a block that ran long in real time (a
    slow/busy laptop under SELFTEST_FAKE_PREFIX, or an actually-broken
    block boundary) rather than fabricating a synthetic timestamp that
    disagrees with real elapsed time. Inter-block spacing is derived from
    each block's own duration (not a fixed constant) so this works for a
    deliberately huge duration_ns too, without one block's timestamps
    overlapping the next."""
    events_path = root / "events.jsonl"
    backend_path = root / "backends.txt"
    result_path = root / "results.csv"
    prefix = root / "aggregate"
    backend_path.write_text(
        "".join("{}\n".format(1000 + index) for index in range(CLIENTS)),
        encoding="ascii",
    )
    events = []
    seconds_needed: set[int] = set()
    cursor = 1_800_000_000_000_000_000
    for block_index, (mode, _) in enumerate(SEQUENCES["A"], 1):
        duration = duration_ns if block_index == slow_block_index else _MEASUREMENT_NS
        start = cursor
        end = start + duration
        events.extend([
            {
                "timestamp_ns": start - 2_000_000_000, "phase": "transition_start",
                "session_index": 1, "block_index": block_index, "mode": mode,
            },
            {
                "timestamp_ns": start, "phase": "measurement_start",
                "session_index": 1, "block_index": block_index, "mode": mode,
                "pgbench_cpu_ticks": 1000, "pgbench_starttime_ticks": 500,
                "clock_ticks_per_second": 100,
            },
            {
                "timestamp_ns": end, "phase": "measurement_end",
                "session_index": 1, "block_index": block_index, "mode": mode,
                "pgbench_cpu_ticks": _MEASUREMENT_END_TICKS, "pgbench_starttime_ticks": 500,
                "clock_ticks_per_second": 100,
            },
        ])
        first_second = (start + 999_999_999) // 1_000_000_000
        last_second = end // 1_000_000_000 - 1
        seconds_needed.update(range(first_second, last_second + 1))
        cursor = end + 20_000_000_000
    with events_path.open("w", encoding="utf-8") as stream:
        for event in events:
            stream.write(json.dumps(event) + "\n")
    for thread in range(THREADS):
        suffix = ".777" if thread == 0 else ".777.{}".format(thread)
        with Path(str(prefix) + suffix).open("w", encoding="utf-8") as log:
            for second in sorted(seconds_needed):
                log.write(aggregate_row(second, 1000))
    metadata = {
        "session_index": 1,
        "sequence": "A",
        "pgbench_seed": 1,
        "pgbench_pid": 777,
        "postmaster_pid": 888,
        "server_cpus": SERVER_CPUS,
        "pgbench_cpus": PGBENCH_CPUS,
        "aggregate_log_prefix": "logs/aggregate-1",
        "server_log": "logs/server-1.log",
    }
    return extract_blocks.append_session_results(
        result_path, events_path, prefix, backend_path, metadata,
    )


def test_extraction_slow_fake_block() -> None:
    """Regression test for the exact incident reported on a busy macOS
    laptop: a fake-mode crossover block's real elapsed time ran a few
    hundred milliseconds past the OLD (pre-fix) tolerance ceiling --
    "block 4 measurement duration differs" -- even though the block
    boundary itself was fine, just slow. protocol.py's
    MEASUREMENT_NS_LOW/HIGH and FULL_SECONDS_MIN/MAX are now mode-aware:
    generous under SELFTEST_FAKE_PREFIX, unchanged for real/
    BENCHMARK_REHEARSAL=1. This must hold in whatever mode this process
    actually inherits (crossover/run.sh always runs this self-test right
    before the crossover phase it is about to run, fake or real), so the
    "an actually-broken block boundary is still rejected" half runs
    unconditionally, and the "the exact fake-mode incident now passes"
    half is specific to SELFTEST_FAKE_PREFIX (its MEASUREMENT_SECONDS=3
    is what defines the incident's old ceiling)."""
    with tempfile.TemporaryDirectory() as temporary:
        try:
            _extract_with_block_duration(
                Path(temporary), 4, MEASUREMENT_NS_HIGH * 10,
            )
        except RuntimeError as error:
            if "measurement duration differs" not in str(error):
                raise AssertionError(
                    f"expected a measurement-duration failure, got: {error}"
                ) from error
        else:
            raise AssertionError(
                "an absurdly long (10x the ceiling) block boundary was "
                "accepted -- the fix must not disable this check"
            )

    if _FAKE:
        # The OLD formula -- still exactly what real/BENCHMARK_REHEARSAL=1
        # mode uses today (protocol.py) -- gave a fake-mode
        # (MEASUREMENT_SECONDS=3) ceiling of (3 + 1.5) * 1e9 =
        # 4_500_000_000ns. A block that ran 300ms past that old ceiling,
        # on an otherwise fine block boundary, must now be accepted.
        old_high = int((MEASUREMENT_SECONDS + 1.5) * 1_000_000_000)
        with tempfile.TemporaryDirectory() as temporary:
            rows = _extract_with_block_duration(
                Path(temporary), 4, old_high + 300_000_000,
            )
        assert len(rows) == BLOCKS_PER_SESSION
        assert int(rows[3]["block_index"]) == 4


def test_analysis() -> None:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        write_analysis_fixture(root)
        report = analyze.analyze(root)
        assert report["suitability"]["passed"] is True
        assert report["overhead"]["stats_overhead"]["estimate_percent"] < 0
        assert report["overhead"]["trace_overhead"]["estimate_percent"] < 0
        assert (root / "analysis.json").is_file()
        assert (root / "analysis.md").is_file()
        assert (root / "session-estimates.csv").is_file()

    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        write_analysis_fixture(root)
        proofs = root / "worker/mode-proofs.csv"
        text = proofs.read_text(encoding="utf-8")
        proofs.write_text(
            text.replace(
                ",off,{},0,0,0,0,".format(CLIENTS),
                ",off,{},1,1,1,0,".format(CLIENTS),
                1,
            ),
            encoding="utf-8",
        )
        try:
            analyze.analyze(root)
        except RuntimeError as error:
            assert "off-mode proof" in str(error)
        else:
            raise AssertionError("invalid off-mode proof was accepted")


def main() -> int:
    test_schedule()
    test_host_state()
    test_runtime_paths()
    test_shell_nounset_declarations()
    test_extraction()
    test_extraction_slow_fake_block()
    test_analysis()
    print("w6c-persistent-crossover self-test: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
