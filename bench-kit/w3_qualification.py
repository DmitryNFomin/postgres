#!/usr/bin/env python3
"""Build and verify W3 short-LWLock qualification evidence."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path


THRESHOLDS = {
    "min_lwlock_calls_per_second": 1000.0,
    "min_lwlock_fraction": 0.95,
    "min_procarray_fraction_of_lwlock": 0.95,
    "max_io_fraction": 0.01,
    "min_histogram_coverage": 0.99,
    "max_p50_us_upper": 32.768,
    "max_p95_us_upper": 131.072,
}


class QualificationError(Exception):
    """Raised when raw W3 evidence is malformed or does not qualify."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise QualificationError(message)


def _finite(value: str, label: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise QualificationError(f"{label} is not numeric: {value!r}") from exc
    _require(math.isfinite(parsed), f"{label} is not finite")
    return parsed


def _percentile_upper(
    histogram: list[int],
    histogram_calls: int,
    fraction: float,
) -> float:
    rank = math.ceil(histogram_calls * fraction)
    cumulative = 0
    for bucket, count in enumerate(histogram):
        cumulative += count
        if cumulative >= rank:
            return 1.024 * (2**bucket)
    raise QualificationError("W3 histogram percentile could not be resolved")


def analyze_file(raw_path: Path, elapsed_seconds: float) -> dict:
    """Return the canonical qualification summary for one raw TSV snapshot."""
    _require(
        math.isfinite(elapsed_seconds) and elapsed_seconds > 0,
        "W3 snapshot duration must be finite and positive",
    )

    events = []
    seen_events: set[tuple[str, str]] = set()
    try:
        lines = raw_path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise QualificationError(f"cannot read W3 snapshot: {exc}") from exc

    for line in lines:
        fields = line.split("\t")
        _require(len(fields) == 6, f"invalid W3 qualification row: {line!r}")
        event_type, event, calls_text, total_text, max_text, histogram_text = (
            fields
        )
        _require(bool(event_type and event), "W3 event names must not be empty")
        key = (event_type, event)
        _require(key not in seen_events, f"duplicate W3 event row: {key!r}")
        seen_events.add(key)
        try:
            calls = int(calls_text)
            histogram = [int(value) for value in histogram_text.split(",")]
        except ValueError as exc:
            raise QualificationError(
                f"invalid integer in W3 qualification row: {line!r}"
            ) from exc
        item = {
            "wait_event_type": event_type,
            "wait_event": event,
            "calls": calls,
            "total_time_ms": _finite(total_text, "W3 total_time_ms"),
            "max_time_us": _finite(max_text, "W3 max_time_us"),
            "histogram": histogram,
        }
        _require(
            item["calls"] >= 0
            and item["total_time_ms"] >= 0
            and item["max_time_us"] >= 0
            and len(histogram) == 32
            and all(value >= 0 for value in histogram),
            f"invalid W3 qualification values: {item!r}",
        )
        events.append(item)

    _require(bool(events), "W3 qualification snapshot is empty")
    lwlocks = [
        item for item in events if item["wait_event_type"] == "LWLock"
    ]
    _require(bool(lwlocks), "W3 qualification snapshot has no LWLock waits")
    lwlock_calls = sum(item["calls"] for item in lwlocks)
    procarray_calls = sum(
        item["calls"]
        for item in lwlocks
        if item["wait_event"] == "ProcArray"
    )
    internal_calls = sum(
        item["calls"]
        for item in events
        if item["wait_event_type"] != "Client"
    )
    io_calls = sum(
        item["calls"]
        for item in events
        if item["wait_event_type"] == "IO"
    )
    histogram = [
        sum(item["histogram"][index] for item in lwlocks)
        for index in range(32)
    ]
    histogram_calls = sum(histogram)
    _require(
        min(lwlock_calls, histogram_calls, internal_calls) > 0,
        "W3 qualification contains empty aggregate counters",
    )

    summary = {
        "snapshot_seconds": elapsed_seconds,
        "lwlock_calls": lwlock_calls,
        "procarray_calls": procarray_calls,
        "lwlock_calls_per_second": lwlock_calls / elapsed_seconds,
        "lwlock_fraction": lwlock_calls / internal_calls,
        "procarray_fraction_of_lwlock": procarray_calls / lwlock_calls,
        "io_fraction": io_calls / internal_calls,
        "histogram_calls": histogram_calls,
        "histogram_coverage": min(lwlock_calls, histogram_calls)
        / max(lwlock_calls, histogram_calls),
        "mean_lwlock_wait_us": (
            sum(item["total_time_ms"] for item in lwlocks)
            * 1000
            / lwlock_calls
        ),
        "p50_us_upper": _percentile_upper(
            histogram, histogram_calls, 0.50
        ),
        "p95_us_upper": _percentile_upper(
            histogram, histogram_calls, 0.95
        ),
        "p99_us_upper": _percentile_upper(
            histogram, histogram_calls, 0.99
        ),
        "thresholds": dict(THRESHOLDS),
        "events": events,
    }
    checks = {
        "lwlock_calls_per_second": summary["lwlock_calls_per_second"]
        >= THRESHOLDS["min_lwlock_calls_per_second"],
        "lwlock_fraction": summary["lwlock_fraction"]
        >= THRESHOLDS["min_lwlock_fraction"],
        "procarray_fraction": summary["procarray_fraction_of_lwlock"]
        >= THRESHOLDS["min_procarray_fraction_of_lwlock"],
        "io_fraction": summary["io_fraction"]
        <= THRESHOLDS["max_io_fraction"],
        "histogram_coverage": summary["histogram_coverage"]
        >= THRESHOLDS["min_histogram_coverage"],
        "p50": summary["p50_us_upper"]
        <= THRESHOLDS["max_p50_us_upper"],
        "p95": summary["p95_us_upper"]
        <= THRESHOLDS["max_p95_us_upper"],
    }
    summary["checks"] = checks
    summary["passed"] = all(checks.values())
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("raw_tsv", type=Path)
    parser.add_argument("output_json", type=Path)
    parser.add_argument("elapsed_seconds", type=float)
    args = parser.parse_args()

    try:
        summary = analyze_file(args.raw_tsv, args.elapsed_seconds)
        with args.output_json.open("x", encoding="utf-8") as stream:
            json.dump(summary, stream, indent=2, sort_keys=True)
            stream.write("\n")
        if not summary["passed"]:
            failed = ", ".join(
                name
                for name, passed in summary["checks"].items()
                if not passed
            )
            raise QualificationError(f"W3 qualification failed: {failed}")
        return 0
    except (QualificationError, OSError) as exc:
        print(f"w3 qualification: FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
