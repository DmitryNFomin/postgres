#!/usr/bin/env python3
"""Extract exact full-second block measurements from pgbench aggregate logs."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from protocol import (
    BLOCKS_PER_SESSION,
    CLIENTS,
    FULL_SECONDS_MAX,
    FULL_SECONDS_MIN,
    MAX_PGBENCH_CAPACITY_FRACTION,
    MEASUREMENT_NS_HIGH,
    MEASUREMENT_NS_LOW,
    RESULT_FIELDS,
    SEQUENCES,
    THREADS,
)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def read_events(path: Path, session_index: int) -> dict[int, dict[str, dict]]:
    blocks: dict[int, dict[str, dict]] = defaultdict(dict)
    for line in path.read_text(encoding="utf-8").splitlines():
        item = json.loads(line)
        if item.get("session_index") != session_index:
            continue
        block_index = item.get("block_index")
        phase = item.get("phase")
        if (
            type(block_index) is int
            and phase in {"measurement_start", "measurement_end"}
        ):
            require(
                phase not in blocks[block_index],
                "duplicate block event",
            )
            blocks[block_index][phase] = item
    require(
        set(blocks) == set(range(1, BLOCKS_PER_SESSION + 1))
        and all(
            set(events) == {"measurement_start", "measurement_end"}
            for events in blocks.values()
        ),
        "measurement events are incomplete",
    )
    return blocks


def read_aggregate_logs(prefix: Path, pgbench_pid: int) -> dict[int, dict]:
    paths = [Path("{}.{}".format(prefix, pgbench_pid))]
    paths.extend(
        Path("{}.{}.{}".format(prefix, pgbench_pid, thread))
        for thread in range(1, THREADS)
    )
    require(
        all(path.is_file() and not path.is_symlink() for path in paths),
        "pgbench aggregate logs are incomplete",
    )
    by_second: dict[int, dict] = defaultdict(
        lambda: {"transactions": 0, "latency_us": 0.0, "failures": 0}
    )
    for path in paths:
        seen = set()
        for line in path.read_text(
            encoding="utf-8", errors="strict"
        ).splitlines():
            fields = line.split()
            require(len(fields) == 16, "malformed aggregate log row")
            second = int(fields[0])
            require(second not in seen, "duplicate aggregate second")
            seen.add(second)
            values = [float(value) for value in fields[1:]]
            require(
                all(math.isfinite(value) for value in values),
                "non-finite aggregate value",
            )
            transactions = int(fields[1])
            failures = sum(int(fields[index]) for index in (13, 14, 15))
            require(
                transactions >= 0 and failures >= 0,
                "negative aggregate counter",
            )
            item = by_second[second]
            item["transactions"] += transactions
            item["latency_us"] += float(fields[2])
            item["failures"] += failures
            item.setdefault("threads", 0)
            item["threads"] += 1
        require(seen, "an aggregate log is empty")
    require(by_second, "aggregate logs are empty")
    return by_second


def build_session_results(
    events_path: Path,
    log_prefix: Path,
    backends_path: Path,
    metadata: dict,
) -> list[dict]:
    session_index = int(metadata["session_index"])
    sequence_name = metadata["sequence"]
    require(sequence_name in SEQUENCES, "unknown crossover sequence")
    blocks = read_events(events_path, session_index)
    by_second = read_aggregate_logs(
        log_prefix,
        int(metadata["pgbench_pid"]),
    )
    backend_bytes = backends_path.read_bytes()
    require(backend_bytes.endswith(b"\n"), "backend list lacks final newline")
    backend_pids = backend_bytes.splitlines()
    require(
        len(backend_pids) == CLIENTS
        and len(set(backend_pids)) == CLIENTS
        and all(pid.isdigit() for pid in backend_pids),
        "backend PID proof is invalid",
    )
    backend_digest = hashlib.sha256(backend_bytes).hexdigest()

    rows = []
    for block_index, (mode, role) in enumerate(
        SEQUENCES[sequence_name],
        1,
    ):
        start_event = blocks[block_index]["measurement_start"]
        end_event = blocks[block_index]["measurement_end"]
        start_ns = int(start_event["timestamp_ns"])
        end_ns = int(end_event["timestamp_ns"])
        require(
            MEASUREMENT_NS_LOW <= end_ns - start_ns <= MEASUREMENT_NS_HIGH,
            "block {} measurement duration differs".format(block_index),
        )
        first_second = (start_ns + 999_999_999) // 1_000_000_000
        last_second = end_ns // 1_000_000_000 - 1
        seconds = list(range(first_second, last_second + 1))
        require(
            FULL_SECONDS_MIN <= len(seconds) <= FULL_SECONDS_MAX
            and all(
                second in by_second
                and by_second[second].get("threads") == THREADS
                for second in seconds
            ),
            "block {} lacks full aggregate seconds".format(block_index),
        )
        per_second = [by_second[second]["transactions"] for second in seconds]
        transactions = sum(per_second)
        latency_us = sum(
            by_second[second]["latency_us"] for second in seconds
        )
        failures = sum(
            by_second[second]["failures"] for second in seconds
        )
        require(transactions > 0, "block has no transactions")
        tps = transactions / len(seconds)
        within_cv = (
            statistics.stdev(per_second)
            / statistics.mean(per_second)
            * 100.0
        )
        for field in (
            "pgbench_cpu_ticks",
            "pgbench_starttime_ticks",
            "clock_ticks_per_second",
        ):
            require(
                type(start_event.get(field)) is int
                and type(end_event.get(field)) is int,
                "measurement event lacks pgbench CPU counters",
            )
        require(
            start_event["pgbench_starttime_ticks"]
            == end_event["pgbench_starttime_ticks"]
            and start_event["clock_ticks_per_second"]
            == end_event["clock_ticks_per_second"]
            and end_event["pgbench_cpu_ticks"]
            >= start_event["pgbench_cpu_ticks"],
            "pgbench process identity or CPU counter changed",
        )
        elapsed_seconds = (end_ns - start_ns) / 1_000_000_000.0
        pgbench_cpu_percent = (
            (
                end_event["pgbench_cpu_ticks"]
                - start_event["pgbench_cpu_ticks"]
            )
            / start_event["clock_ticks_per_second"]
            / elapsed_seconds
            * 100.0
        )
        require(
            math.isfinite(pgbench_cpu_percent),
            "pgbench CPU measurement is non-finite",
        )
        capacity = pgbench_cpu_percent / (THREADS * 100.0)
        require(
            0 <= capacity < MAX_PGBENCH_CAPACITY_FRACTION,
            "pgbench driver saturation invalidates the session",
        )
        row = {
            "session_index": session_index,
            "sequence": sequence_name,
            "pgbench_seed": int(metadata["pgbench_seed"]),
            "block_index": block_index,
            "mode": mode,
            "role": role,
            "measurement_start_ns": start_ns,
            "measurement_end_ns": end_ns,
            "full_seconds": len(seconds),
            "transactions": transactions,
            "tps": "{:.12g}".format(tps),
            "latency_ms": "{:.12g}".format(
                latency_us / transactions / 1000.0
            ),
            "within_block_cv_percent": "{:.12g}".format(within_cv),
            "failed_transactions": failures,
            "pgbench_cpu_percent": "{:.12g}".format(pgbench_cpu_percent),
            "pgbench_capacity_fraction": "{:.12g}".format(capacity),
            "postmaster_pid": int(metadata["postmaster_pid"]),
            "pgbench_pid": int(metadata["pgbench_pid"]),
            "backend_pid_sha256": backend_digest,
            "server_cpus": metadata["server_cpus"],
            "pgbench_cpus": metadata["pgbench_cpus"],
            "aggregate_log_prefix": metadata["aggregate_log_prefix"],
            "server_log": metadata["server_log"],
        }
        rows.append(row)
    return rows


def append_session_results(
    output: Path,
    events_path: Path,
    log_prefix: Path,
    backends_path: Path,
    metadata: dict,
) -> list[dict]:
    rows = build_session_results(
        events_path,
        log_prefix,
        backends_path,
        metadata,
    )

    write_header = not output.exists()
    with output.open("a", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=RESULT_FIELDS,
            lineterminator="\n",
        )
        if write_header:
            writer.writeheader()
        writer.writerows(rows)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("events", type=Path)
    parser.add_argument("log_prefix", type=Path)
    parser.add_argument("backends", type=Path)
    parser.add_argument("metadata_json")
    args = parser.parse_args()
    metadata = json.loads(args.metadata_json)
    append_session_results(
        args.output,
        args.events,
        args.log_prefix,
        args.backends,
        metadata,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
