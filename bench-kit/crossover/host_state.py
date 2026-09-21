#!/usr/bin/env python3
"""Capture and validate current host state around the crossover."""

from __future__ import annotations

import argparse
import datetime
import json
import os
import platform
import re
import socket
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import rehearsal  # noqa: E402


def parse_cpu_list(text: str) -> set[int]:
    cpus = set()
    for item in text.strip().split(","):
        range_part, separator, stride_part = item.partition(":")
        stride = int(stride_part) if separator else 1
        if "-" in range_part:
            start, end = map(int, range_part.split("-", 1))
            cpus.update(range(start, end + 1, stride))
        else:
            cpus.add(int(range_part))
    return cpus


def read(path: str) -> str:
    return Path(path).read_text(encoding="ascii").strip()


def _read_optional(path: str) -> str:
    try:
        return read(path)
    except OSError:
        return ""


def _fake_capture() -> dict:
    """SELFTEST_FAKE_PREFIX: this may be running on a dev machine with no
    /sys/devices/system/cpu (e.g. macOS) and no "performance" governor to
    report -- there is no real hardware state to capture against fake
    binaries anyway. Return a fixed, internally-consistent synthetic
    report instead; validate_host_state() (analyze.py) only requires the
    non-timestamp fields to be byte-identical before/after, which a fixed
    dict trivially satisfies. Never used for a real run."""
    return {
        "schema_version": 2,
        "timestamp_utc": datetime.datetime.now(
            datetime.timezone.utc
        ).isoformat(),
        "hostname": "selftest-fakebin",
        "kernel": "selftest-fakebin (no real kernel)",
        "load_average": [0.0, 0.0, 0.0],
        "online_cpus": "0-63",
        "smt_active": "",
        "intel_pstate_no_turbo": "",
        "governors": ["performance"],
    }


def capture() -> dict:
    if os.environ.get("SELFTEST_FAKE_PREFIX"):
        return _fake_capture()
    governor_paths = sorted(
        Path("/sys/devices/system/cpu").glob(
            "cpu[0-9]*/cpufreq/scaling_governor"
        ),
        key=lambda path: int(re.search(r"cpu([0-9]+)", str(path)).group(1)),
    )
    return {
        "schema_version": 2,
        "timestamp_utc": datetime.datetime.now(
            datetime.timezone.utc
        ).isoformat(),
        "hostname": socket.gethostname(),
        "kernel": platform.platform(),
        "load_average": list(os.getloadavg()),
        "online_cpus": read("/sys/devices/system/cpu/online"),
        "smt_active": _read_optional("/sys/devices/system/cpu/smt/active"),
        "intel_pstate_no_turbo": _read_optional(
            "/sys/devices/system/cpu/intel_pstate/no_turbo"
        ),
        "governors": [read(str(path)) for path in governor_paths],
    }


def validate(state: dict) -> None:
    """brief-v11-wpc-kit.md: CPU affinity is configurable, not pinned to
    one topology, so this only checks internal consistency of whatever the
    host actually reports -- not a fixed CPU count or NUMA layout. The
    caller (analyze.py) separately requires before == after."""
    if state.get("schema_version") != 2:
        raise RuntimeError("host-state schema differs")
    if not (
        isinstance(state.get("hostname"), str)
        and state["hostname"]
        and isinstance(state.get("kernel"), str)
        and state["kernel"]
        and isinstance(state.get("timestamp_utc"), str)
        and isinstance(state.get("load_average"), (list, tuple))
        and len(state["load_average"]) == 3
        and all(
            isinstance(value, (int, float))
            for value in state["load_average"]
        )
    ):
        raise RuntimeError("host-state metadata is malformed")
    try:
        timestamp = datetime.datetime.fromisoformat(state["timestamp_utc"])
    except ValueError as error:
        raise RuntimeError("host-state timestamp is malformed") from error
    if (
        timestamp.tzinfo is None
        or timestamp.utcoffset() != datetime.timedelta(0)
    ):
        raise RuntimeError("host-state timestamp is not UTC")
    if not parse_cpu_list(state.get("online_cpus", "")):
        raise RuntimeError("online CPU set is empty or malformed")
    governors = state.get("governors") or []
    if set(governors) != {"performance"}:
        if rehearsal.governor_check_relaxed(governors):
            sys.stderr.write(
                "\n*** REHEARSAL NOTE (not evidence, not blocking): no "
                "cpufreq governor is exposed on this host "
                "(BENCHMARK_REHEARSAL=1) -- 00-check-host.sh's preflight "
                "already relaxed the same check earlier in this run ***\n\n"
            )
        else:
            raise RuntimeError("CPU governors are not all performance")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    state = capture()
    args.output.write_text(
        json.dumps(state, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    validate(state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
