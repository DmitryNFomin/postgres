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


def format_cpu_list(cpu_ids):
    """Compress a sorted collection of CPU ids into taskset syntax,
    including the stride form (e.g. "1-15:2") needed for a host whose CPU
    numbering interleaves NUMA nodes (odd/even split across two sockets).
    A run of three or more evenly-spaced ids becomes "start-end[:stride]";
    anything else falls back to individual ids joined by commas."""
    ids = sorted(set(cpu_ids))
    if not ids:
        return ""
    groups = []
    i = 0
    n = len(ids)
    while i < n:
        j = i
        if j + 1 < n:
            stride = ids[j + 1] - ids[j]
            k = j + 1
            while k + 1 < n and ids[k + 1] - ids[k] == stride:
                k += 1
            if k > j:
                if stride == 1:
                    groups.append("{}-{}".format(ids[j], ids[k]))
                else:
                    groups.append("{}-{}:{}".format(ids[j], ids[k], stride))
                i = k + 1
                continue
        groups.append(str(ids[j]))
        i = j + 1
    return ",".join(groups)


def _nodes_with_core_representatives(topology):
    """{node: [cpu, ...]} with exactly one representative CPU id per
    distinct physical (socket, core) -- the lowest-numbered CPU sharing
    that core -- so an SMT sibling is never double-counted as a second
    physical core."""
    nodes = {}
    seen_cores = set()
    for cpu in sorted(topology):
        core, socket, node = topology[cpu]
        key = (node, socket, core)
        if key in seen_cores:
            continue
        seen_cores.add(key)
        nodes.setdefault(node, []).append(cpu)
    for cpus in nodes.values():
        cpus.sort()
    return nodes


def describe_topology_lines(topology):
    """Human-readable topology summary: NUMA nodes, CPUs per node with the
    actual numbering (taskset syntax), and SMT state -- everything an
    executor needs to pick a same-node mask pair by hand."""
    nodes = _nodes_with_core_representatives(topology)
    lines = []
    for node in sorted(nodes):
        cpus_in_node = sorted(
            cpu for cpu, (_core, _socket, cpu_node) in topology.items()
            if cpu_node == node
        )
        lines.append(
            "NUMA node {}: {} physical core(s), CPUs {}".format(
                node, len(nodes[node]), format_cpu_list(cpus_in_node)
            )
        )
    total_cpus = len(topology)
    total_cores = sum(len(cpus) for cpus in nodes.values())
    if total_cores and total_cpus != total_cores:
        lines.append(
            "SMT: on ({} logical CPUs / {} physical cores)".format(
                total_cpus, total_cores
            )
        )
    else:
        lines.append("SMT: off ({} physical core(s))".format(total_cores))
    return lines


def recommend_single_node_masks(topology, per_role=8):
    """Recommend a same-node SERVER_CPUS/PGBENCH_CPUS pair: the first
    `per_role` physical cores of one NUMA node for the server, the next
    `per_role` physical cores of the SAME node for pgbench (the v7
    protocol). Prefers a node that does not contain CPU id 0 (commonly
    the busiest CPU for kernel/IRQ duties on a freshly booted host) when
    more than one node has enough cores; falls back to the lowest node id
    otherwise. Returns None if no single node has 2 * per_role physical
    cores online."""
    nodes = _nodes_with_core_representatives(topology)

    def sort_key(node):
        return (0 if 0 not in nodes[node] else 1, node)

    for node in sorted(nodes, key=sort_key):
        cores = nodes[node]
        if len(cores) >= 2 * per_role:
            return {
                "node": node,
                "server_cpus": format_cpu_list(cores[:per_role]),
                "pgbench_cpus": format_cpu_list(cores[per_role:2 * per_role]),
            }
    return None


def check_single_node_masks(server_ids, pgbench_ids, topology):
    """Refuse (ValueError, no override) if SERVER_CPUS or PGBENCH_CPUS
    spans more than one NUMA node. A cross-socket mask lets the scheduler
    and the memory allocator split work and pages across both nodes,
    adding run-to-run variance that has nothing to do with the code under
    test (see reports/wpf-report.md, "numa_local_fraction" investigation:
    a mask spanning every node on the host makes that metric vacuously
    1.0 no matter where memory actually lands). On refusal, prints the
    live topology and a concrete, same-node recommended pair."""
    server_nodes = {topology[c][2] for c in server_ids}
    pgbench_nodes = {topology[c][2] for c in pgbench_ids}
    if len(server_nodes) <= 1 and len(pgbench_nodes) <= 1:
        return
    lines = []
    if len(server_nodes) > 1:
        lines.append(
            "SERVER_CPUS spans more than one NUMA node: nodes {} (CPUs {})".format(
                ",".join(str(n) for n in sorted(server_nodes)),
                format_cpu_list(server_ids),
            )
        )
    if len(pgbench_nodes) > 1:
        lines.append(
            "PGBENCH_CPUS spans more than one NUMA node: nodes {} (CPUs {})".format(
                ",".join(str(n) for n in sorted(pgbench_nodes)),
                format_cpu_list(pgbench_ids),
            )
        )
    lines.append(
        "A cross-socket mask lets the scheduler and memory allocator split "
        "work and pages across both NUMA nodes, adding run-to-run variance "
        "that has nothing to do with the code under test -- SERVER_CPUS "
        "and PGBENCH_CPUS must each be confined to a single NUMA node."
    )
    lines.append("")
    lines.append("Topology (from lscpu -e):")
    lines.extend("  " + line for line in describe_topology_lines(topology))
    lines.append("")
    recommendation = recommend_single_node_masks(topology)
    if recommendation is not None:
        lines.append(
            "Recommended pair (v7 protocol: server gets 8 physical cores, "
            "pgbench gets the next 8 physical cores, both on NUMA node {}):"
            .format(recommendation["node"])
        )
        lines.append("  export SERVER_CPUS={}".format(recommendation["server_cpus"]))
        lines.append("  export PGBENCH_CPUS={}".format(recommendation["pgbench_cpus"]))
    else:
        lines.append(
            "No single NUMA node on this host has {} or more physical "
            "cores online; this protocol needs {} for the server and {} "
            "for pgbench on the SAME node, so this host cannot run it as "
            "configured.".format(2 * 8, 8, 8)
        )
    raise ValueError("\n".join(lines))


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

    check_single_node_masks(server_ids, pgbench_ids, topology)

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
    # Defense in depth: a proof recorded before this NUMA guard existed (or
    # one that was hand-edited) could still claim "verified" while its
    # SERVER_CPUS/PGBENCH_CPUS span more than one NUMA node -- reject it
    # here too, not only at collection time.
    for key in ("server_numa_nodes", "pgbench_numa_nodes"):
        nodes = affinity.get(key)
        if nodes is None:
            continue
        if not isinstance(nodes, list) or any(type(n) is not int for n in nodes):
            raise ValueError(f"{key} must be a list of integer NUMA node ids")
        if len(set(nodes)) > 1:
            raise ValueError(
                f"{key} spans more than one NUMA node: {sorted(set(nodes))} "
                "-- SERVER_CPUS/PGBENCH_CPUS must each be confined to a "
                "single NUMA node"
            )


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
