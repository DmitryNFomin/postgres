#!/usr/bin/env python3
"""CPU-affinity protocol and fail-closed topology checks, v11.

Unlike the v10 kit (topology-specific: exactly one verified two-socket,
64-core map), this kit is not tied to one machine's topology
(brief-v11-wpc-kit.md: "no longer hard-coded to one topology"). SERVER_CPUS
and PGBENCH_CPUS are read from the environment; this module verifies at
runtime that both masks are non-empty, that they do not share a single
physical core (same socket+core id), and that a process can actually be
pinned to each with taskset. The host check prints the discovered
topology and refuses to run if either check fails.
"""

import json
import os
import re
import subprocess
import sys
from pathlib import Path


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


def configured_cpu_lists():
    """Read SERVER_CPUS/PGBENCH_CPUS from the environment. Both are
    required; this kit has no built-in default topology to fall back to."""
    server_cpus = os.environ.get("SERVER_CPUS", "").strip()
    pgbench_cpus = os.environ.get("PGBENCH_CPUS", "").strip()
    if not server_cpus:
        raise ValueError("SERVER_CPUS is not set (or is empty)")
    if not pgbench_cpus:
        raise ValueError("PGBENCH_CPUS is not set (or is empty)")
    return server_cpus, pgbench_cpus


def _lscpu_topology():
    """Return {cpu: (core, socket, node)} for every online CPU."""
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
    return rows


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


def collect_topology_proof():
    """Inspect live sysfs/lscpu topology and return bound JSON evidence.

    Fails closed (raises ValueError) if:
      - SERVER_CPUS or PGBENCH_CPUS is unset/empty;
      - either mask references an offline CPU;
      - the two masks overlap or share a physical core (same socket+core
        id, e.g. two hyperthread siblings);
      - taskset cannot actually apply either mask.
    """
    server_cpus, pgbench_cpus = configured_cpu_lists()
    server_ids = parse_cpu_list(server_cpus)
    pgbench_ids = parse_cpu_list(pgbench_cpus)
    if not server_ids:
        raise ValueError("SERVER_CPUS resolves to an empty CPU set")
    if not pgbench_ids:
        raise ValueError("PGBENCH_CPUS resolves to an empty CPU set")

    topology = _lscpu_topology()
    online = set(topology)
    if not server_ids <= online:
        raise ValueError(
            "SERVER_CPUS references CPU(s) not online: "
            + ",".join(str(c) for c in sorted(server_ids - online))
        )
    if not pgbench_ids <= online:
        raise ValueError(
            "PGBENCH_CPUS references CPU(s) not online: "
            + ",".join(str(c) for c in sorted(pgbench_ids - online))
        )
    if server_ids & pgbench_ids:
        raise ValueError(
            "SERVER_CPUS and PGBENCH_CPUS overlap: "
            + ",".join(str(c) for c in sorted(server_ids & pgbench_ids))
        )

    server_cores = {(topology[c][1], topology[c][0]) for c in server_ids}
    pgbench_cores = {(topology[c][1], topology[c][0]) for c in pgbench_ids}
    shared_cores = server_cores & pgbench_cores
    if shared_cores:
        raise ValueError(
            "SERVER_CPUS and PGBENCH_CPUS share a physical core "
            "(socket,core)=" + ",".join(str(c) for c in sorted(shared_cores))
        )

    available = os.sched_getaffinity(0)
    if not (server_ids | pgbench_ids) <= available:
        raise ValueError(
            "current cpuset does not permit every configured CPU"
        )
    _probe_taskset(server_cpus)
    _probe_taskset(pgbench_cpus)

    node_names = sorted(
        path.name
        for path in Path("/sys/devices/system/node").glob("node*")
        if path.is_dir() and re.fullmatch(r"node[0-9]+", path.name)
    )

    return {
        "verified": True,
        "server_cpus": server_cpus,
        "pgbench_cpus": pgbench_cpus,
        "server_cpu_ids": sorted(server_ids),
        "pgbench_cpu_ids": sorted(pgbench_ids),
        "server_sockets": sorted({topology[c][1] for c in server_ids}),
        "pgbench_sockets": sorted({topology[c][1] for c in pgbench_ids}),
        "server_numa_nodes": sorted({topology[c][2] for c in server_ids}),
        "pgbench_numa_nodes": sorted({topology[c][2] for c in pgbench_ids}),
        "shared_physical_core": False,
        "online_cpu_ids": sorted(online),
        "numa_node_count": len(node_names),
    }


def fake_topology_proof():
    """Synthetic CPU-affinity proof for SELFTEST_FAKE_PREFIX (fake-binaries
    self-test) mode: there is no real topology to probe (this may be
    running on a non-Linux dev machine, or a Linux one whose CPU layout is
    irrelevant against fake binaries), so this is derived purely from
    SERVER_CPUS/PGBENCH_CPUS via parse_cpu_list() -- the same parser
    collect_topology_proof() uses for the real check -- instead of a
    placeholder object that validate_affinity_proof()/validate_host_report()
    would reject outright (they did: 00-check-host.sh and 02-run-matrix.sh
    used to each write their own minimal fake stand-in here, and neither
    satisfied the shape the real verifier requires, which nothing ever
    caught before analyze-results.py actually ran against a fake-binaries
    smoke result -- see reports/wpf-report.md, Addendum 5). Never used for
    a real run."""
    server_cpus, pgbench_cpus = configured_cpu_lists()
    server_ids = parse_cpu_list(server_cpus)
    pgbench_ids = parse_cpu_list(pgbench_cpus)
    if not server_ids:
        raise ValueError("SERVER_CPUS resolves to an empty CPU set")
    if not pgbench_ids:
        raise ValueError("PGBENCH_CPUS resolves to an empty CPU set")
    if server_ids & pgbench_ids:
        raise ValueError(
            "SERVER_CPUS and PGBENCH_CPUS overlap: "
            + ",".join(str(c) for c in sorted(server_ids & pgbench_ids))
        )
    return {
        "verified": True,
        "shared_physical_core": False,
        "server_cpus": server_cpus,
        "pgbench_cpus": pgbench_cpus,
        "server_cpu_ids": sorted(server_ids),
        "pgbench_cpu_ids": sorted(pgbench_ids),
        "server_numa_nodes": [0],
        "pgbench_numa_nodes": [0],
    }


def validate_affinity_proof(affinity):
    """Validate one cpu_affinity_protocol object's internal consistency.

    Does not pin the topology to any fixed machine: only checks that the
    proof says "verified", that the two masks are disjoint, share no
    physical core, and are both non-empty.
    """
    if not isinstance(affinity, dict):
        raise ValueError("missing CPU-affinity topology proof")
    if affinity.get("verified") is not True:
        raise ValueError("CPU-affinity topology was not verified")
    if affinity.get("shared_physical_core") is not False:
        raise ValueError(
            "CPU-affinity proof does not certify disjoint physical "
            "cores for PostgreSQL and pgbench"
        )
    for key in ("server_cpu_ids", "pgbench_cpu_ids"):
        ids = affinity.get(key)
        if not isinstance(ids, list) or not ids or any(
            type(cpu) is not int for cpu in ids
        ):
            raise ValueError(f"{key} must be a nonempty list of integers")
    if set(affinity["server_cpu_ids"]) & set(affinity["pgbench_cpu_ids"]):
        raise ValueError("server/pgbench CPU sets overlap")
    for key in ("server_cpus", "pgbench_cpus"):
        if not isinstance(affinity.get(key), str) or not affinity[key]:
            raise ValueError(f"{key} must be a nonempty string")


def validate_host_report(report, expected_hostname=None):
    """Validate retained host evidence with strict-enough JSON types."""
    if not isinstance(report, dict):
        raise ValueError("host report is not an object")
    if type(report.get("warning_count")) is not int or report["warning_count"] != 0:
        raise ValueError("host warning_count must be integer 0")
    if (
        expected_hostname is not None
        and report.get("hostname") != expected_hostname
    ):
        raise ValueError("host check was created on a different host")
    try:
        validate_affinity_proof(report.get("cpu_affinity_protocol"))
    except ValueError as error:
        raise ValueError(f"host {error}") from error


def main():
    if len(sys.argv) < 2:
        raise SystemExit("usage: cpu_affinity.py COMMAND [ARG ...]")
    command = sys.argv[1]
    try:
        if command == "collect" and len(sys.argv) == 2:
            print(json.dumps(collect_topology_proof(), sort_keys=True))
        elif command == "fake-collect" and len(sys.argv) == 2:
            print(json.dumps(fake_topology_proof(), sort_keys=True))
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
