#!/usr/bin/env python3
"""Synthetic tests for the persistent-backend crossover."""

from __future__ import annotations

import csv
import hashlib
import json
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
    PAIRS,
    PROOF_FIELDS,
    RESULT_FIELDS,
    SCHEDULE_FIELDS,
    SEQUENCES,
    THREADS,
    file_hashes,
    protocol_document,
)


HERE = Path(__file__).resolve().parent


def synthetic_host_state(load_average: object = None) -> dict:
    return {
        "schema_version": 1,
        "timestamp_utc": "2026-03-08T00:00:00+00:00",
        "hostname": "synthetic-host",
        "kernel": "synthetic-kernel",
        "load_average": (
            [0.0, 0.0, 0.0] if load_average is None else load_average
        ),
        "online_cpus": "0-63",
        "node0_cpus": "0-62:2",
        "node1_cpus": "1-63:2",
        "smt_active": "0",
        "intel_pstate_no_turbo": 0,
        "governors": ["performance"] * 64,
    }


def write_csv(path: Path, fields: tuple[str, ...], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=fields,
            lineterminator="\n",
        )
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
    seed = generate_schedule.generate(schedule_path, PAIRS, seed=123456)
    schedule = list(csv.DictReader(schedule_path.open(encoding="utf-8")))
    (worker / "seed.txt").write_text(str(seed) + "\n", encoding="ascii")
    for name in BOUND_FILES:
        shutil.copyfile(HERE / name, tools / name)
    (provenance / "PACKAGE-MANIFEST.sha256").write_text(
        "synthetic package manifest\n",
        encoding="utf-8",
    )
    (provenance / "manifest.json").write_text(
        '{"schema_version": 6}\n',
        encoding="utf-8",
    )
    host = synthetic_host_state()
    (provenance / "host-before.json").write_text(
        json.dumps(host, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    host["timestamp_utc"] = "2026-03-08T01:00:00+00:00"
    (provenance / "host-after.json").write_text(
        json.dumps(host, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    protocol = protocol_document(
        seed,
        analyze.sha256(
            provenance / "PACKAGE-MANIFEST.sha256"
        ),
        analyze.sha256(
            provenance / "manifest.json"
        ),
        file_hashes(tools),
    )
    (worker / "protocol.json").write_text(
        json.dumps(protocol, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    proof_rows = []
    events = []
    session_inputs = []
    clock = 1_800_000_000_000_000_000
    for scheduled in schedule:
        session_index = int(scheduled["session_index"])
        pair_index = int(scheduled["pair_index"])
        build = scheduled["build"]
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

        session_level = 560_000.0 * (1.0 + (pair_index % 5 - 2) * 0.006)
        if build == "v10":
            session_level *= 1.002
        for block_index, (mode, role) in enumerate(
            SEQUENCES[sequence],
            1,
        ):
            mode_factor = {
                "off": 1.0,
                "stats": 0.990 if build == "v9" else 0.992,
                "trace": 0.982 if build == "v9" else 0.986,
            }[mode]
            tps = session_level * mode_factor
            transition = clock
            start = transition + 5_000_000_000
            end = start + 30_000_000_000
            clock = end + 1_000_000_000
            events.extend([
                {
                    "timestamp_ns": transition,
                    "phase": "transition_start",
                    "session_index": session_index,
                    "block_index": block_index,
                    "mode": mode,
                },
                {
                    "timestamp_ns": start,
                    "phase": "measurement_start",
                    "session_index": session_index,
                    "block_index": block_index,
                    "mode": mode,
                    "pgbench_cpu_ticks": 1000,
                    "pgbench_starttime_ticks": 500,
                    "clock_ticks_per_second": 100,
                },
                {
                    "timestamp_ns": end,
                    "phase": "measurement_end",
                    "session_index": session_index,
                    "block_index": block_index,
                    "mode": mode,
                    "pgbench_cpu_ticks": 15400,
                    "pgbench_starttime_ticks": 500,
                    "clock_ticks_per_second": 100,
                },
            ])
            calls_before = 0 if mode == "off" else 1000
            calls_after = 0 if mode == "off" else 2000
            proof_rows.append({
                "session_index": session_index,
                "block_index": block_index,
                "mode": mode,
                "client_count": CLIENTS,
                "clients_recording": 0 if mode == "off" else CLIENTS,
                "calls_before": calls_before,
                "calls_after": calls_after,
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
            with Path(str(aggregate_prefix) + suffix).open(
                "w", encoding="utf-8"
            ) as aggregate:
                for second in range(first_second, last_second + 1):
                    aggregate.write(
                        aggregate_row(second, counts.get(second, 1000))
                    )
        session_inputs.append((
            aggregate_prefix,
            backend_path,
            {
                **scheduled,
                "postmaster_pid": 20_000 + session_index,
                "pgbench_pid": 999,
                "server_cpus": "1-63:2",
                "pgbench_cpus": "0-14:2",
                "aggregate_log_prefix":
                    "logs/aggregate-{}".format(session_index),
                "server_log": "logs/server-{}.log".format(session_index),
            },
        ))

    write_csv(worker / "mode-proofs.csv", PROOF_FIELDS, proof_rows)
    with (worker / "events.jsonl").open("w", encoding="utf-8") as stream:
        for event in events:
            stream.write(json.dumps(event, sort_keys=True) + "\n")
    for aggregate_prefix, backend_path, metadata in session_inputs:
        extract_blocks.append_session_results(
            worker / "results.csv",
            worker / "events.jsonl",
            aggregate_prefix,
            backend_path,
            metadata,
        )


def test_schedule() -> None:
    with tempfile.TemporaryDirectory() as temporary:
        path = Path(temporary) / "schedule.csv"
        seed = generate_schedule.generate(path, PAIRS, seed=42)
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

    corrected = Path(
        "/var/tmp/w6c-persistent-crossover.HMbUIs/sock"
    )
    verify_runtime.validate_socket_path(corrected, 55473)
    assert len(
        str(verify_runtime.postgres_socket_path(corrected, 55473)).encode()
    ) < verify_runtime.MAX_UNIX_SOCKET_PATH_BYTES

    socket_name = ".s.PGSQL.55473"
    boundary_length = (
        verify_runtime.MAX_UNIX_SOCKET_PATH_BYTES
        - len(socket_name.encode())
        - 2
    )
    verify_runtime.validate_socket_path(
        Path("/" + "x" * boundary_length),
        55473,
    )
    try:
        verify_runtime.validate_socket_path(
            Path("/" + "x" * (boundary_length + 1)),
            55473,
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError("108-byte socket path was accepted")


def same_line_local_dependencies(line: str) -> list[tuple[str, str]]:
    assignments = re.findall(
        r"(?:^|\s)([A-Za-z_][A-Za-z0-9_]*)=([^\s]+)",
        line,
    )
    names = {name for name, _ in assignments}
    return [
        (name, dependency)
        for name, value in assignments
        for dependency in re.findall(
            r"\$(?:\{)?([A-Za-z_][A-Za-z0-9_]*)",
            value,
        )
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
                    line_number,
                    conflicts,
                )
            )


def aggregate_row(second: int, transactions: int) -> str:
    values = (
        second,
        transactions,
        transactions * 50,
        transactions * 2500,
        40,
        60,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
    )
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
                    "timestamp_ns": start - 2_000_000_000,
                    "phase": "transition_start",
                    "session_index": 1,
                    "block_index": block_index,
                    "mode": mode,
                },
                {
                    "timestamp_ns": start,
                    "phase": "measurement_start",
                    "session_index": 1,
                    "block_index": block_index,
                    "mode": mode,
                    "pgbench_cpu_ticks": 1000,
                    "pgbench_starttime_ticks": 500,
                    "clock_ticks_per_second": 100,
                },
                {
                    "timestamp_ns": start + 30_000_000_000,
                    "phase": "measurement_end",
                    "session_index": 1,
                    "block_index": block_index,
                    "mode": mode,
                    "pgbench_cpu_ticks": 15400,
                    "pgbench_starttime_ticks": 500,
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
            "pair_index": 1,
            "pair_position": 1,
            "build": "v9",
            "sequence": "A",
            "pgbench_seed": 1,
            "pgbench_pid": 777,
            "postmaster_pid": 888,
            "server_cpus": "1-63:2",
            "pgbench_cpus": "0-14:2",
            "aggregate_log_prefix": "logs/aggregate-1",
            "server_log": "logs/server-1.log",
        }
        rows = extract_blocks.append_session_results(
            result_path,
            events_path,
            prefix,
            backend_path,
            metadata,
        )
        assert len(rows) == BLOCKS_PER_SESSION
        assert all(int(row["full_seconds"]) == 29 for row in rows)
        assert all(
            float(row["tps"]) == THREADS * 1000.0 for row in rows
        )
        assert all(int(row["failed_transactions"]) == 0 for row in rows)


def test_analysis() -> None:
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        write_analysis_fixture(root)
        report = analyze.analyze(root)
        assert report["suitability"]["passed"] is True
        assert report["by_build"]["v9"]["stats_overhead"][
            "estimate_percent"
        ] < 0
        assert report["v10_vs_v9"]["stats_overhead_change"][
            "estimate_percent"
        ] > 0
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
    test_analysis()
    print("w6c-persistent-crossover self-test: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
