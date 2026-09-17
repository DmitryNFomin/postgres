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
from pathlib import Path


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


def capture() -> dict:
    governor_paths = sorted(
        Path("/sys/devices/system/cpu").glob(
            "cpu[0-9]*/cpufreq/scaling_governor"
        ),
        key=lambda path: int(re.search(r"cpu([0-9]+)", str(path)).group(1)),
    )
    return {
        "schema_version": 1,
        "timestamp_utc": datetime.datetime.now(
            datetime.timezone.utc
        ).isoformat(),
        "hostname": socket.gethostname(),
        "kernel": platform.platform(),
        "load_average": list(os.getloadavg()),
        "online_cpus": read("/sys/devices/system/cpu/online"),
        "node0_cpus": read("/sys/devices/system/node/node0/cpulist"),
        "node1_cpus": read("/sys/devices/system/node/node1/cpulist"),
        "smt_active": read("/sys/devices/system/cpu/smt/active"),
        "intel_pstate_no_turbo": int(
            read("/sys/devices/system/cpu/intel_pstate/no_turbo")
        ),
        "governors": [read(str(path)) for path in governor_paths],
    }


def validate(state: dict) -> None:
    expected_online = set(range(64))
    expected_node0 = set(range(0, 64, 2))
    expected_node1 = set(range(1, 64, 2))
    if state.get("schema_version") != 1:
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
    if parse_cpu_list(state["online_cpus"]) != expected_online:
        raise RuntimeError("online CPU set differs")
    if parse_cpu_list(state["node0_cpus"]) != expected_node0:
        raise RuntimeError("NUMA node 0 CPU set differs")
    if parse_cpu_list(state["node1_cpus"]) != expected_node1:
        raise RuntimeError("NUMA node 1 CPU set differs")
    if state["smt_active"] != "0":
        raise RuntimeError("SMT is active")
    if state["intel_pstate_no_turbo"] != 0:
        raise RuntimeError("turbo is disabled")
    if (
        len(state["governors"]) != 64
        or set(state["governors"]) != {"performance"}
    ):
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
