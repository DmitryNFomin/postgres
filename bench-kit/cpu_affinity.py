#!/usr/bin/env python3
"""Canonical CPU-affinity protocol and fail-closed topology checks."""

import json
import os
import re
import subprocess
import sys
from pathlib import Path


SERVER_CPUS = "1-63:2"
PGBENCH_CPUS = "0-14:2"
CPU_PINNING = (
    "fixed taskset: PostgreSQL on NUMA node 1; "
    "pgbench on eight CPUs from NUMA node 0"
)

EXPECTED_ONLINE_CPUS = list(range(64))
EXPECTED_NODE0_CPUS = list(range(0, 64, 2))
EXPECTED_NODE1_CPUS = list(range(1, 64, 2))
EXPECTED_PGBENCH_CPUS = list(range(0, 16, 2))


def parse_cpu_list(text):
    """Parse Linux CPU-list syntax, including range strides."""
    if not isinstance(text, str) or not text.strip():
        raise ValueError("CPU list must be a nonempty string")
    cpus = set()
    for item in text.strip().split(","):
        if not item:
            raise ValueError("CPU list contains an empty item")
        range_part, separator, stride_part = item.partition(":")
        if separator:
            if not stride_part or not stride_part.isdecimal():
                raise ValueError("CPU-list stride must be a positive integer")
            stride = int(stride_part)
            if stride <= 0:
                raise ValueError("CPU-list stride must be positive")
        else:
            stride = 1
        if "-" in range_part:
            parts = range_part.split("-")
            if len(parts) != 2 or not all(part.isdecimal() for part in parts):
                raise ValueError("CPU-list range is malformed")
            start, end = map(int, parts)
            if end < start:
                raise ValueError("CPU-list range is reversed")
            cpus.update(range(start, end + 1, stride))
        else:
            if separator or not range_part.isdecimal():
                raise ValueError("CPU-list item is malformed")
            cpus.add(int(range_part))
    return cpus


def _read_cpu_list(path):
    return parse_cpu_list(Path(path).read_text(encoding="ascii"))


def _assert_exact_integer(value, expected, label):
    if type(value) is not int or value != expected:
        raise ValueError("{} must be integer {}".format(label, expected))


def _assert_exact_cpu_ids(value, expected, label):
    if (
        not isinstance(value, list)
        or any(type(cpu) is not int for cpu in value)
        or value != expected
    ):
        raise ValueError("{} differs from the fixed protocol".format(label))


def validate_host_report(report, expected_hostname=None):
    """Validate retained host evidence with strict JSON types."""
    if not isinstance(report, dict):
        raise ValueError("host report is not an object")
    _assert_exact_integer(
        report.get("warning_count"),
        0,
        "host warning_count",
    )
    if (
        expected_hostname is not None
        and report.get("hostname") != expected_hostname
    ):
        raise ValueError("host check was created on a different host")

    affinity = report.get("cpu_affinity_protocol")
    if not isinstance(affinity, dict):
        raise ValueError("host report lacks CPU-affinity topology proof")
    if affinity.get("verified") is not True:
        raise ValueError("host CPU-affinity topology was not verified")
    if affinity.get("server_cpus") != SERVER_CPUS:
        raise ValueError("host PostgreSQL CPU mask differs")
    if affinity.get("pgbench_cpus") != PGBENCH_CPUS:
        raise ValueError("host pgbench CPU mask differs")
    for key, expected in (
        ("server_socket", 1),
        ("server_numa_node", 1),
        ("pgbench_socket", 0),
        ("pgbench_numa_node", 0),
    ):
        _assert_exact_integer(affinity.get(key), expected, "host " + key)
    _assert_exact_cpu_ids(
        affinity.get("online_cpu_ids"),
        EXPECTED_ONLINE_CPUS,
        "host online CPU set",
    )
    _assert_exact_cpu_ids(
        affinity.get("node0_cpu_ids"),
        EXPECTED_NODE0_CPUS,
        "host NUMA node 0 CPU set",
    )
    _assert_exact_cpu_ids(
        affinity.get("node1_cpu_ids"),
        EXPECTED_NODE1_CPUS,
        "host NUMA node 1 CPU set",
    )
    for key, expected in (
        ("sockets", 2),
        ("cores_per_socket", 32),
        ("threads_per_core", 1),
        ("physical_cores", 64),
        ("logical_cpus", 64),
    ):
        _assert_exact_integer(report.get(key), expected, "host " + key)
    if report.get("smt_active") != "off":
        raise ValueError("host SMT state differs from the fixed protocol")


def _verify_self_affinity(cpu_list):
    expected = parse_cpu_list(cpu_list)
    actual = os.sched_getaffinity(0)
    if actual != expected:
        raise ValueError(
            "effective affinity is {}, expected {}".format(
                sorted(actual),
                sorted(expected),
            )
        )


def _probe_taskset(cpu_list):
    process = subprocess.run(
        [
            "taskset",
            "-c",
            cpu_list,
            sys.executable,
            str(Path(__file__).resolve()),
            "verify-self",
            cpu_list,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if process.returncode != 0:
        detail = process.stderr.strip() or process.stdout.strip()
        raise ValueError(
            "taskset cannot apply exact affinity {}: {}".format(
                cpu_list,
                detail,
            )
        )


def collect_topology_proof():
    """Inspect live sysfs/lscpu topology and return bound JSON evidence."""
    expected_online = set(EXPECTED_ONLINE_CPUS)
    expected_node0 = set(EXPECTED_NODE0_CPUS)
    expected_node1 = set(EXPECTED_NODE1_CPUS)
    online = _read_cpu_list("/sys/devices/system/cpu/online")
    node0 = _read_cpu_list("/sys/devices/system/node/node0/cpulist")
    node1 = _read_cpu_list("/sys/devices/system/node/node1/cpulist")
    node_names = sorted(
        path.name
        for path in Path("/sys/devices/system/node").glob("node*")
        if path.is_dir() and re.fullmatch(r"node[0-9]+", path.name)
    )

    if online != expected_online:
        raise ValueError("online CPUs are not exactly 0-63")
    if node_names != ["node0", "node1"]:
        raise ValueError("expected exactly NUMA nodes 0 and 1")
    if node0 != expected_node0:
        raise ValueError("NUMA node 0 is not exactly the even CPUs 0-62")
    if node1 != expected_node1:
        raise ValueError("NUMA node 1 is not exactly the odd CPUs 1-63")
    if node0 & node1 or node0 | node1 != online:
        raise ValueError("NUMA CPU sets overlap or do not cover online CPUs")
    if parse_cpu_list(SERVER_CPUS) != expected_node1:
        raise ValueError("PostgreSQL mask is not exactly NUMA node 1")
    if parse_cpu_list(PGBENCH_CPUS) != set(EXPECTED_PGBENCH_CPUS):
        raise ValueError("pgbench mask is not exactly CPUs 0,2,...,14")

    output = subprocess.check_output(
        ["lscpu", "-p=CPU,CORE,SOCKET,NODE"],
        text=True,
    )
    rows = {}
    for line in output.splitlines():
        if not line or line.startswith("#"):
            continue
        fields = line.split(",")
        if len(fields) != 4:
            raise ValueError("unexpected lscpu topology output")
        cpu, core, socket, node = map(int, fields)
        if cpu in rows:
            raise ValueError("duplicate CPU in lscpu topology output")
        rows[cpu] = (core, socket, node)
    if set(rows) != expected_online:
        raise ValueError("lscpu does not describe exactly CPUs 0-63")
    for cpu in expected_node0:
        if rows[cpu][1:] != (0, 0):
            raise ValueError(
                "CPU {} is not on socket 0 / NUMA node 0".format(cpu)
            )
    for cpu in expected_node1:
        if rows[cpu][1:] != (1, 1):
            raise ValueError(
                "CPU {} is not on socket 1 / NUMA node 1".format(cpu)
            )
    for socket in (0, 1):
        core_ids = {
            (row_socket, core)
            for core, row_socket, _ in rows.values()
            if row_socket == socket
        }
        if len(core_ids) != 32:
            raise ValueError(
                "socket {} does not expose exactly 32 cores".format(socket)
            )
    if len({(socket, core) for core, socket, _ in rows.values()}) != 64:
        raise ValueError("lscpu topology does not prove SMT is off")
    smt_active = Path("/sys/devices/system/cpu/smt/active").read_text(
        encoding="ascii"
    ).strip()
    if smt_active != "0":
        raise ValueError("kernel reports SMT active")

    available = os.sched_getaffinity(0)
    required = expected_node1 | set(EXPECTED_PGBENCH_CPUS)
    if not required <= available:
        raise ValueError(
            "current cpuset does not permit every required CPU"
        )
    _probe_taskset(SERVER_CPUS)
    _probe_taskset(PGBENCH_CPUS)

    return {
        "verified": True,
        "server_cpus": SERVER_CPUS,
        "server_socket": 1,
        "server_numa_node": 1,
        "pgbench_cpus": PGBENCH_CPUS,
        "pgbench_socket": 0,
        "pgbench_numa_node": 0,
        "online_cpu_ids": EXPECTED_ONLINE_CPUS,
        "node0_cpu_ids": EXPECTED_NODE0_CPUS,
        "node1_cpu_ids": EXPECTED_NODE1_CPUS,
    }


def main():
    if len(sys.argv) < 2:
        raise SystemExit("usage: cpu_affinity.py COMMAND [ARG ...]")
    command = sys.argv[1]
    try:
        if command == "constants-tsv" and len(sys.argv) == 2:
            print("{}\t{}\t{}".format(
                SERVER_CPUS,
                PGBENCH_CPUS,
                CPU_PINNING,
            ))
        elif command == "collect" and len(sys.argv) == 2:
            print(json.dumps(collect_topology_proof(), sort_keys=True))
        elif command == "verify-live" and len(sys.argv) == 2:
            collect_topology_proof()
        elif command == "verify-self" and len(sys.argv) == 3:
            _verify_self_affinity(sys.argv[2])
        elif command == "verify-pid" and len(sys.argv) == 4:
            pid = int(sys.argv[2])
            expected = parse_cpu_list(sys.argv[3])
            actual = os.sched_getaffinity(pid)
            if actual != expected:
                raise ValueError(
                    "pid {} affinity is {}, expected {}".format(
                        pid,
                        sorted(actual),
                        sorted(expected),
                    )
                )
        elif command == "verify-host-report" and len(sys.argv) == 4:
            report = json.loads(
                Path(sys.argv[2]).read_text(encoding="utf-8")
            )
            validate_host_report(report, sys.argv[3])
        else:
            raise ValueError("invalid arguments for " + command)
    except (OSError, ValueError, subprocess.SubprocessError) as error:
        print("cpu_affinity.py: {}".format(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
