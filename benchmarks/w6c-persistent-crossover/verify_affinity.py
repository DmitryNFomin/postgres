#!/usr/bin/env python3
"""Verify one CPU affinity mask for a batch of process IDs."""

from __future__ import annotations

import os
import sys


def parse_cpu_list(text: str) -> set[int]:
    cpus = set()
    for item in text.split(","):
        range_part, separator, stride_part = item.partition(":")
        stride = int(stride_part) if separator else 1
        if "-" in range_part:
            start, end = map(int, range_part.split("-", 1))
            cpus.update(range(start, end + 1, stride))
        else:
            cpus.add(int(range_part))
    return cpus


def main() -> int:
    if len(sys.argv) < 3:
        raise SystemExit("usage: verify_affinity.py CPU_LIST PID...")
    expected = parse_cpu_list(sys.argv[1])
    for pid_text in sys.argv[2:]:
        pid = int(pid_text)
        actual = os.sched_getaffinity(pid)
        if actual != expected:
            raise RuntimeError(
                "PID {} affinity is {}, expected {}".format(
                    pid,
                    sorted(actual),
                    sorted(expected),
                )
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
