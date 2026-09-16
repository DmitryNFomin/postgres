#!/usr/bin/env python3
"""Record one-second host telemetry without external sysstat tools."""

import argparse
import json
import os
import signal
import time
from pathlib import Path


CPU_IDS = tuple(range(64))
CPUFREQ_ROOT = Path("/sys/devices/system/cpu")
RAPL_ROOT = Path("/sys/class/powercap")
BLOCK_ROOT = Path("/sys/block")
NODE_ROOT = Path("/sys/devices/system/node")
HWMON_ROOT = Path("/sys/class/hwmon")
INTEL_PSTATE_ROOT = Path("/sys/devices/system/cpu/intel_pstate")
CLOCK_TICKS_PER_SECOND = int(os.sysconf("SC_CLK_TCK"))
BLOCK_DEVICES = ()
RAPL_DOMAINS = ()
TEMPERATURE_SENSORS = ()
STOP = False


def read_text(path):
    try:
        return path.read_text(encoding="ascii").strip()
    except OSError:
        return None


def read_int(path):
    value = read_text(path)
    return int(value) if value is not None else None


def read_cpu_frequency():
    values = {}
    for cpu in CPU_IDS:
        value = read_int(
            CPUFREQ_ROOT
            / ("cpu{}".format(cpu))
            / "cpufreq"
            / "scaling_cur_freq"
        )
        if value is not None:
            values[str(cpu)] = value
    return values


def read_proc_stat():
    values = {}
    for line in Path("/proc/stat").read_text(encoding="ascii").splitlines():
        fields = line.split()
        if (
            len(fields) >= 5
            and fields[0].startswith("cpu")
            and fields[0][3:].isdigit()
        ):
            values[fields[0][3:]] = [int(value) for value in fields[1:]]
    return values


def read_per_cpu_interrupts(path):
    lines = path.read_text(encoding="ascii", errors="replace").splitlines()
    if not lines:
        return {}
    cpu_count = sum(
        token.startswith("CPU") for token in lines[0].split()
    )
    totals = [0] * cpu_count
    sources = {}
    for line in lines[1:]:
        label, separator, remainder = line.partition(":")
        if not separator:
            continue
        fields = remainder.split()
        numeric = fields[:cpu_count]
        if len(numeric) != cpu_count:
            continue
        try:
            values = [int(value) for value in numeric]
        except ValueError:
            continue
        totals = [left + right for left, right in zip(totals, values)]
        detail = " ".join(fields[cpu_count:])
        source = "{}:{}".format(label.strip(), detail)
        sources[source] = {
            str(cpu): value for cpu, value in enumerate(values)
        }
    return {
        "totals": {
            str(cpu): value for cpu, value in enumerate(totals)
        },
        "sources": sources,
    }


def read_named_counters(path):
    values = {}
    text = read_text(path)
    if text is None:
        return values
    for line in text.splitlines():
        fields = line.split()
        if len(fields) == 2:
            try:
                values[fields[0]] = int(fields[1])
            except ValueError:
                pass
    return values


def discover_block_devices():
    devices = []
    if not BLOCK_ROOT.is_dir():
        return devices
    for device in sorted(BLOCK_ROOT.iterdir()):
        if device.name.startswith(("loop", "ram")):
            continue
        if (device / "partition").exists():
            continue
        slaves = device / "slaves"
        if slaves.is_dir() and any(slaves.iterdir()):
            continue
        devices.append((device.name, device / "stat"))
    return devices


def read_block_stats():
    values = {}
    for name, stat_path in BLOCK_DEVICES:
        text = read_text(stat_path)
        if text:
            values[name] = [int(value) for value in text.split()]
    return values


def read_process_cpu():
    values = {}
    for stat_path in sorted(Path("/proc").glob("[0-9]*/stat")):
        try:
            text = stat_path.read_text(encoding="ascii")
            close = text.rfind(")")
            if close < 0:
                continue
            pid = stat_path.parent.name
            comm = text[text.find("(") + 1:close]
            fields = text[close + 2:].split()
            values[pid] = {
                "comm": comm,
                "user_ticks": int(fields[11]),
                "system_ticks": int(fields[12]),
                "starttime_ticks": int(fields[19]),
            }
        except (OSError, ValueError, IndexError):
            continue
    return values


def discover_rapl_domains():
    domains = []
    if not RAPL_ROOT.is_dir():
        return domains
    energy_paths = set()
    for domain in RAPL_ROOT.glob("intel-rapl*"):
        try:
            resolved = domain.resolve(strict=True)
        except OSError:
            continue
        if resolved.is_dir():
            energy_paths.update(
                path.resolve() for path in resolved.rglob("energy_uj")
            )
    for energy_path in sorted(energy_paths):
        domain = energy_path.parent
        key = domain.name
        domains.append((
            key,
            energy_path,
            read_text(domain / "name") or key,
            read_int(domain / "max_energy_range_uj"),
        ))
    return domains


def read_rapl():
    values = {}
    for key, energy_path, name, maximum in RAPL_DOMAINS:
        values[key] = {
            "name": name,
            "energy_uj": read_int(energy_path),
            "max_energy_range_uj": maximum,
        }
    return values


def discover_temperature_sensors():
    sensors = []
    if not HWMON_ROOT.is_dir():
        return sensors
    for sensor in sorted(HWMON_ROOT.glob("hwmon*/temp*_input")):
        base = sensor.parent
        stem = sensor.name[:-6]
        label = read_text(base / (stem + "label")) or sensor.name
        device = read_text(base / "name") or base.name
        key = "{}:{}:{}:{}".format(
            base.resolve(),
            device,
            label,
            sensor.name,
        )
        sensors.append((key, sensor))
    return sensors


def read_temperatures():
    values = {}
    for key, sensor in TEMPERATURE_SENSORS:
        value = read_int(sensor)
        if value is not None:
            values[key] = value
    return values


def read_intel_pstate():
    return {
        name: read_int(INTEL_PSTATE_ROOT / name)
        for name in (
            "no_turbo",
            "min_perf_pct",
            "max_perf_pct",
            "num_pstates",
        )
    }


def collect_sample():
    return {
        "schema_version": 1,
        "timestamp_ns": time.time_ns(),
        "monotonic_ns": time.monotonic_ns(),
        "cpu_freq_khz": read_cpu_frequency(),
        "proc_stat": read_proc_stat(),
        "interrupts": read_per_cpu_interrupts(Path("/proc/interrupts")),
        "softirqs": read_per_cpu_interrupts(Path("/proc/softirqs")),
        "block_stat": read_block_stats(),
        "process_cpu": read_process_cpu(),
        "clock_ticks_per_second": CLOCK_TICKS_PER_SECOND,
        "rapl": read_rapl(),
        "temperatures_millic": read_temperatures(),
        "node0_numastat": read_named_counters(NODE_ROOT / "node0/numastat"),
        "node1_numastat": read_named_counters(NODE_ROOT / "node1/numastat"),
        "intel_pstate": read_intel_pstate(),
        "loadavg": read_text(Path("/proc/loadavg")),
    }


def validate_probe(sample):
    expected = {str(cpu) for cpu in CPU_IDS}
    if set(sample["cpu_freq_khz"]) != expected:
        raise RuntimeError("scaling_cur_freq is unavailable for some CPUs")
    if set(sample["proc_stat"]) != expected:
        raise RuntimeError("/proc/stat does not expose exactly CPUs 0-63")
    if sample["intel_pstate"]["no_turbo"] not in (0, 1):
        raise RuntimeError("intel_pstate/no_turbo is unavailable")
    if len(sample["interrupts"]["totals"]) != 64:
        raise RuntimeError("/proc/interrupts lacks per-CPU counters")
    if len(sample["softirqs"]["totals"]) != 64:
        raise RuntimeError("/proc/softirqs lacks per-CPU counters")
    required_numa = {"numa_miss", "other_node"}
    for node in ("node0_numastat", "node1_numastat"):
        if not required_numa <= set(sample[node]):
            raise RuntimeError("{} lacks required counters".format(node))


def request_stop(_signum, _frame):
    global STOP
    STOP = True


def record(path, interval):
    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    deadline = time.monotonic()
    with path.open("x", encoding="utf-8") as stream:
        while not STOP:
            sample = collect_sample()
            validate_probe(sample)
            json.dump(sample, stream)
            stream.write("\n")
            stream.flush()
            deadline += interval
            time.sleep(max(0.0, deadline - time.monotonic()))


def main():
    global BLOCK_DEVICES, RAPL_DOMAINS, TEMPERATURE_SENSORS
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("probe")
    record_parser = subparsers.add_parser("record")
    record_parser.add_argument("output", type=Path)
    record_parser.add_argument("--interval", type=float, default=1.0)
    args = parser.parse_args()
    BLOCK_DEVICES = tuple(discover_block_devices())
    RAPL_DOMAINS = tuple(discover_rapl_domains())
    TEMPERATURE_SENSORS = tuple(discover_temperature_sensors())

    if args.command == "probe":
        sample = collect_sample()
        validate_probe(sample)
        print(json.dumps(sample, sort_keys=True))
    else:
        if args.interval <= 0:
            raise SystemExit("interval must be positive")
        record(args.output, args.interval)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
