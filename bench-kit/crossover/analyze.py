#!/usr/bin/env python3
"""Validate and analyze the persistent-backend W6c crossover, second
stage: patched installation only, 16 independent sessions (no v9/v10
pairing -- brief-v11-wpc-kit.md)."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import extract_blocks
import host_state
from protocol import (
    BLOCKS_PER_SESSION,
    CLIENTS,
    EQUIVALENCE_MARGIN_PERCENT,
    FULL_SECONDS_MAX,
    FULL_SECONDS_MIN,
    MAX_PGBENCH_CAPACITY_FRACTION,
    MEASUREMENT_NS_HIGH as _MEASUREMENT_NS_HIGH,
    MEASUREMENT_NS_LOW as _MEASUREMENT_NS_LOW,
    PLACEBO_MARGIN_PERCENT,
    PROOF_FIELDS,
    RESULT_FIELDS,
    SCHEDULE_FIELDS,
    SEQUENCES,
    SESSIONS,
    file_hashes,
    protocol_document,
    t95,
)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def read_csv(path: Path, fields: tuple[str, ...]) -> list[dict[str, str]]:
    require(path.is_file() and not path.is_symlink(), "{} is missing".format(path))
    with path.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        require(
            tuple(reader.fieldnames or ()) == fields,
            "{} schema differs".format(path.name),
        )
        return list(reader)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def log_mean(values: list[float]) -> float:
    require(values and all(value > 0 for value in values), "TPS must be positive")
    return statistics.mean(math.log(value) for value in values)


def percent(log_value: float) -> float:
    return math.expm1(log_value) * 100.0


def interval(log_values: list[float], margin: float) -> dict:
    require(len(log_values) >= 2, "too few values for confidence interval")
    mean = statistics.mean(log_values)
    half = t95(len(log_values) - 1) * statistics.stdev(log_values)
    half /= math.sqrt(len(log_values))
    lower = percent(mean - half)
    upper = percent(mean + half)
    if lower >= -margin and upper <= margin:
        classification = "equivalent"
    elif lower > 0:
        classification = "faster"
    elif upper < 0:
        classification = "slower"
    else:
        classification = "unresolved"
    return {
        "n": len(log_values),
        "estimate_percent": percent(mean),
        "lower_95_percent": lower,
        "upper_95_percent": upper,
        "stdev_log": statistics.stdev(log_values),
        "margin_percent": margin,
        "classification": classification,
        "log_values": log_values,
    }


def validate_schedule(rows: list[dict[str, str]]) -> None:
    require(len(rows) == SESSIONS, "schedule is incomplete")
    require(
        [int(row["session_index"]) for row in rows] == list(range(1, SESSIONS + 1)),
        "schedule session indices are incomplete",
    )
    for row in rows:
        require(
            int(row["pgbench_seed"]) == int(row["session_index"]),
            "schedule seed does not equal session index",
        )
    counts = Counter(row["sequence"] for row in rows)
    require(
        counts == Counter({"A": SESSIONS // 2, "B": SESSIONS // 2}),
        "sequence allocation is not balanced",
    )


def validate_results(rows: list[dict[str, str]], schedule: list[dict[str, str]]) -> None:
    require(len(rows) == SESSIONS * BLOCKS_PER_SESSION, "results are incomplete")
    schedule_by_session = {int(row["session_index"]): row for row in schedule}
    by_session = defaultdict(list)
    for row in rows:
        session_index = int(row["session_index"])
        by_session[session_index].append(row)
        expected = schedule_by_session.get(session_index)
        require(expected is not None, "result references unknown session")
        for field in ("sequence", "pgbench_seed"):
            require(row[field] == expected[field],
                    "result differs from schedule in {}".format(field))
        for field in ("tps", "latency_ms", "within_block_cv_percent",
                      "pgbench_cpu_percent", "pgbench_capacity_fraction"):
            require(math.isfinite(float(row[field])),
                    "non-finite result field {}".format(field))
        require(
            FULL_SECONDS_MIN <= int(row["full_seconds"]) <= FULL_SECONDS_MAX
            and int(row["transactions"]) > 0
            and float(row["tps"]) > 0
            and float(row["latency_ms"]) >= 0
            and float(row["within_block_cv_percent"]) >= 0
            and int(row["failed_transactions"]) == 0
            and 0 <= float(row["pgbench_capacity_fraction"]) < MAX_PGBENCH_CAPACITY_FRACTION,
            "result values are invalid",
        )

    require(set(by_session) == set(range(1, SESSIONS + 1)), "sessions missing")
    server_cpus_set = {row["server_cpus"] for row in rows}
    pgbench_cpus_set = {row["pgbench_cpus"] for row in rows}
    require(len(server_cpus_set) == 1 and len(pgbench_cpus_set) == 1,
            "CPU affinity differs across sessions")
    for session_index, session_rows in by_session.items():
        session_rows.sort(key=lambda row: int(row["block_index"]))
        expected = SEQUENCES[session_rows[0]["sequence"]]
        require(
            [int(row["block_index"]) for row in session_rows]
            == list(range(1, BLOCKS_PER_SESSION + 1))
            and [(row["mode"], row["role"]) for row in session_rows] == list(expected)
            and len({row["postmaster_pid"] for row in session_rows}) == 1
            and len({row["backend_pid_sha256"] for row in session_rows}) == 1
            and len({row["aggregate_log_prefix"] for row in session_rows}) == 1
            and len({row["server_log"] for row in session_rows}) == 1,
            "session {} identity or block order differs".format(session_index),
        )
        previous_end = 0
        for row in session_rows:
            start = int(row["measurement_start_ns"])
            end = int(row["measurement_end_ns"])
            require(
                previous_end < start < end
                and _MEASUREMENT_NS_LOW <= end - start <= _MEASUREMENT_NS_HIGH,
                "session {} block timestamps are invalid".format(session_index),
            )
            previous_end = end


def validate_protocol(root: Path) -> None:
    path = root / "worker/protocol.json"
    require(path.is_file() and not path.is_symlink(), "protocol is missing")
    protocol = json.loads(path.read_text(encoding="utf-8"))
    seed_text = (root / "worker/seed.txt").read_text(encoding="ascii").strip()
    require(seed_text.isdecimal(), "schedule seed is malformed")
    expected = protocol_document(
        int(seed_text),
        sha256(root / "provenance/PACKAGE-MANIFEST.sha256"),
        sha256(root / "provenance/manifest.json"),
        file_hashes(root / "tools"),
        protocol.get("server_cpus", ""),
        protocol.get("pgbench_cpus", ""),
    )
    require(protocol == expected, "protocol or bound provenance differs")


def validate_host_state(root: Path) -> None:
    before_path = root / "provenance/host-before.json"
    after_path = root / "provenance/host-after.json"
    require(
        before_path.is_file() and not before_path.is_symlink()
        and after_path.is_file() and not after_path.is_symlink(),
        "host-state evidence is missing",
    )
    before = json.loads(before_path.read_text(encoding="utf-8"))
    after = json.loads(after_path.read_text(encoding="utf-8"))
    host_state.validate(before)
    host_state.validate(after)
    for field in ("hostname", "kernel", "online_cpus", "smt_active",
                  "intel_pstate_no_turbo", "governors"):
        require(before[field] == after[field], "host state changed: {}".format(field))


def validate_events(root: Path, rows: list[dict[str, str]]) -> None:
    path = root / "worker/events.jsonl"
    require(path.is_file() and not path.is_symlink(), "events are missing")
    events = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    require(len(events) == SESSIONS * BLOCKS_PER_SESSION * 3, "event count differs")
    expected_rows = {
        (int(row["session_index"]), int(row["block_index"])): row for row in rows
    }
    by_block = defaultdict(list)
    previous_timestamp = 0
    for event in events:
        timestamp = event.get("timestamp_ns")
        require(type(timestamp) is int and timestamp > previous_timestamp,
                "event timestamps are not strictly increasing")
        previous_timestamp = timestamp
        key = (event.get("session_index"), event.get("block_index"))
        require(key in expected_rows and event.get("mode") == expected_rows[key]["mode"],
                "event block or mode differs")
        by_block[key].append(event)
    for key, block_events in by_block.items():
        require(
            [event.get("phase") for event in block_events]
            == ["transition_start", "measurement_start", "measurement_end"],
            "event phases differ for block {}".format(key),
        )
        row = expected_rows[key]
        require(
            all(
                type(block_events[index].get(field)) is int
                for index in (1, 2)
                for field in ("pgbench_cpu_ticks", "pgbench_starttime_ticks",
                              "clock_ticks_per_second")
            )
            and block_events[1]["timestamp_ns"] == int(row["measurement_start_ns"])
            and block_events[2]["timestamp_ns"] == int(row["measurement_end_ns"]),
            "measurement events differ from result boundaries or CPU counters",
        )


def validate_proofs(root: Path, rows: list[dict[str, str]]) -> None:
    proofs = read_csv(root / "worker/mode-proofs.csv", PROOF_FIELDS)
    require(len(proofs) == SESSIONS * BLOCKS_PER_SESSION, "mode proofs are incomplete")
    expected = {(int(row["session_index"]), int(row["block_index"])): row for row in rows}
    require(len(expected) == SESSIONS * BLOCKS_PER_SESSION,
            "result block identities are duplicated")
    seen = set()
    for proof in proofs:
        key = (int(proof["session_index"]), int(proof["block_index"]))
        require(key not in seen, "duplicate mode proof")
        seen.add(key)
        row = expected.get(key)
        require(
            row is not None and proof["mode"] == row["mode"]
            and proof["backend_pid_sha256"] == row["backend_pid_sha256"]
            and int(proof["client_count"]) == CLIENTS,
            "mode proof identity differs",
        )
        clients = int(proof["clients_recording"])
        before = int(proof["calls_before"])
        after = int(proof["calls_after"])
        trace = int(proof["trace_records"])
        if proof["mode"] == "off":
            require((clients, before, after, trace) == (0, 0, 0, 0),
                    "off-mode proof shows recording")
        elif proof["mode"] == "stats":
            require(clients == CLIENTS and 0 < before < after and trace == 0,
                    "stats-mode proof is invalid")
        elif proof["mode"] == "trace":
            require(clients == CLIENTS and 0 < before < after and trace > 0,
                    "trace-mode proof is invalid")
        else:
            raise RuntimeError("unknown proof mode")
    require(seen == set(expected), "a block lacks a mode proof")


def validate_evidence_paths(root: Path, rows: list[dict[str, str]]) -> None:
    for session_index in range(1, SESSIONS + 1):
        session = [row for row in rows if int(row["session_index"]) == session_index]
        digest = session[0]["backend_pid_sha256"]
        backend_path = root / "worker/backend-pids" / "session-{}.txt".format(session_index)
        require(
            backend_path.is_file() and not backend_path.is_symlink()
            and sha256(backend_path) == digest,
            "backend PID evidence differs",
        )
        server_path = root / "worker" / session[0]["server_log"]
        require(server_path.is_file() and not server_path.is_symlink(),
                "server log is missing")
        prefix = root / "worker" / session[0]["aggregate_log_prefix"]
        metadata = {
            field: session[0][field]
            for field in ("session_index", "sequence", "pgbench_seed",
                          "postmaster_pid", "pgbench_pid", "server_cpus",
                          "pgbench_cpus", "aggregate_log_prefix", "server_log")
        }
        derived = extract_blocks.build_session_results(
            root / "worker/events.jsonl", prefix, backend_path, metadata,
        )
        ordered = sorted(session, key=lambda row: int(row["block_index"]))
        require(
            all(
                all(str(actual[field]) == expected[field] for field in RESULT_FIELDS)
                for actual, expected in zip(derived, ordered)
            ),
            "results do not reconcile with raw aggregate logs and events",
        )


def session_estimates(rows: list[dict[str, str]]) -> list[dict]:
    grouped = defaultdict(list)
    for row in rows:
        grouped[int(row["session_index"])].append(row)
    estimates = []
    for session_index in range(1, SESSIONS + 1):
        session = grouped[session_index]
        by_role = {row["role"]: float(row["tps"]) for row in session}
        stats_active = [by_role["stats_first"], by_role["stats_second"]]
        stats_off = [by_role["off_stats_before"], by_role["off_stats_after"]]
        trace_active = [by_role["trace_first"], by_role["trace_second"]]
        trace_off = [by_role["off_trace_before"], by_role["off_trace_after"]]
        off_values = stats_off + trace_off
        stats_overhead = log_mean(stats_active) - log_mean(stats_off)
        trace_overhead = log_mean(trace_active) - log_mean(trace_off)
        placebo_roles = (
            ("off_stats_after", "off_trace_before")
            if session[0]["sequence"] == "A"
            else ("off_trace_after", "off_stats_before")
        )
        item = {
            "session_index": session_index,
            "sequence": session[0]["sequence"],
            "stats_overhead_log": stats_overhead,
            "trace_overhead_log": trace_overhead,
            "trace_vs_stats_log": trace_overhead - stats_overhead,
            "placebo_log": (
                math.log(by_role[placebo_roles[1]]) - math.log(by_role[placebo_roles[0]])
            ),
            "stats_bracket_drift_log": (
                math.log(by_role["off_stats_after"]) - math.log(by_role["off_stats_before"])
            ),
            "trace_bracket_drift_log": (
                math.log(by_role["off_trace_after"]) - math.log(by_role["off_trace_before"])
            ),
            "off_level_log": log_mean(off_values),
            "stats_level_log": log_mean(stats_active),
            "trace_level_log": log_mean(trace_active),
            "max_within_block_cv_percent": max(
                float(row["within_block_cv_percent"]) for row in session
            ),
            "max_pgbench_capacity_fraction": max(
                float(row["pgbench_capacity_fraction"]) for row in session
            ),
        }
        estimates.append(item)
    return estimates


def analyze(root: Path) -> dict:
    validate_protocol(root)
    validate_host_state(root)
    schedule = read_csv(root / "worker/schedule.csv", SCHEDULE_FIELDS)
    rows = read_csv(root / "worker/results.csv", RESULT_FIELDS)
    validate_schedule(schedule)
    validate_results(rows, schedule)
    validate_events(root, rows)
    validate_proofs(root, rows)
    validate_evidence_paths(root, rows)
    estimates = session_estimates(rows)

    overhead = {
        "stats_overhead": interval(
            [item["stats_overhead_log"] for item in estimates], EQUIVALENCE_MARGIN_PERCENT,
        ),
        "trace_overhead": interval(
            [item["trace_overhead_log"] for item in estimates], EQUIVALENCE_MARGIN_PERCENT,
        ),
        "trace_vs_stats": interval(
            [item["trace_vs_stats_log"] for item in estimates], EQUIVALENCE_MARGIN_PERCENT,
        ),
        "off_off_placebo": interval(
            [item["placebo_log"] for item in estimates], PLACEBO_MARGIN_PERCENT,
        ),
        "stats_bracket_drift": interval(
            [item["stats_bracket_drift_log"] for item in estimates], PLACEBO_MARGIN_PERCENT,
        ),
        "trace_bracket_drift": interval(
            [item["trace_bracket_drift_log"] for item in estimates], PLACEBO_MARGIN_PERCENT,
        ),
    }

    noise_controls_passed = all(
        overhead[key]["classification"] == "equivalent"
        for key in ("off_off_placebo", "stats_bracket_drift", "trace_bracket_drift")
    )
    report = {
        "schema_version": 2,
        "design": "persistent-backend bracketed crossover, patched installation only",
        "sessions": SESSIONS,
        "blocks": len(rows),
        "suitability": {
            "passed": noise_controls_passed,
            "reason": (
                "off/off placebo and bracket-drift intervals are inside "
                "the predeclared margin"
                if noise_controls_passed
                else "a placebo or bracket-drift interval exceeds the "
                "predeclared margin"
            ),
        },
        "overhead": overhead,
        "maximum_within_block_cv_percent": max(
            item["max_within_block_cv_percent"] for item in estimates
        ),
        "maximum_pgbench_capacity_fraction": max(
            item["max_pgbench_capacity_fraction"] for item in estimates
        ),
    }

    with (root / "session-estimates.csv").open("x", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=tuple(estimates[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(estimates)
    (root / "analysis.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )

    lines = [
        "# Persistent-backend W6c crossover (second stage, patched only)",
        "",
        "Evidence validation: **PASS**",
        "",
        "Statistical suitability: **{}** ({})".format(
            "PASS" if noise_controls_passed else "FAIL",
            report["suitability"]["reason"],
        ),
        "",
        "## Tool overhead (patched installation, 16 sessions)",
        "",
        "| Contrast | Estimate | 95% CI | Classification |",
        "|---|---:|---:|---|",
    ]
    for label, key in (
        ("stats vs off", "stats_overhead"),
        ("trace vs off", "trace_overhead"),
        ("trace vs stats", "trace_vs_stats"),
        ("off/off placebo", "off_off_placebo"),
        ("stats bracket drift", "stats_bracket_drift"),
        ("trace bracket drift", "trace_bracket_drift"),
    ):
        item = overhead[key]
        lines.append(
            "| {} | {:+.3f}% | [{:+.3f}%, {:+.3f}%] | {} |".format(
                label, item["estimate_percent"], item["lower_95_percent"],
                item["upper_95_percent"], item["classification"],
            )
        )
    lines.extend([
        "",
        "The confidence intervals use independent sessions as the unit of "
        "analysis (no build pairing: every session runs the patched "
        "installation). One-second rows are never treated as independent "
        "replicates.",
        "",
    ])
    (root / "analysis.md").write_text("\n".join(lines), encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    analyze(args.root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
