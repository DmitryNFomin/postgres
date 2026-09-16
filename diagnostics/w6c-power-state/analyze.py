#!/usr/bin/env python3
"""Validate and summarize a completed W6c power-state diagnostic."""

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path


SERVER_CPUS = tuple(range(1, 64, 2))
PGBENCH_CPUS = tuple(range(0, 16, 2))
EXPECTED_CONFIGS = ("baseline-a", "baseline-b")
EXPECTED_REPETITIONS = 8
EXPECTED_ROWS = len(EXPECTED_CONFIGS) * EXPECTED_REPETITIONS
RESULT_FIELDS = (
    "run_index",
    "timestamp_utc",
    "config",
    "repetition",
    "tps",
    "latency_ms",
    "measurement_samples",
    "measurement_interval_s",
    "within_cell_cv_percent",
    "warmup_to_measurement_percent",
    "pgbench_cpu_percent",
    "pgbench_capacity_fraction",
    "start_ns",
    "measurement_start_ns",
    "end_ns",
    "postmaster_pid",
    "server_cpus",
    "pgbench_cpus",
    "pgbench_log",
    "server_log",
)
CPU_TOTAL_FIELDS = tuple(range(8))
CPU_BUSY_FIELDS = (0, 1, 2, 5, 6, 7)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def median(values):
    return statistics.median(values) if values else None


def correlation(left, right):
    if (
        len(left) < 8
        or len(left) != len(right)
        or statistics.stdev(left) == 0
        or statistics.stdev(right) == 0
    ):
        return None
    left_mean = statistics.mean(left)
    right_mean = statistics.mean(right)
    left_delta = [value - left_mean for value in left]
    right_delta = [value - right_mean for value in right]
    denominator = math.sqrt(
        sum(value * value for value in left_delta)
        * sum(value * value for value in right_delta)
    )
    if denominator == 0:
        return None
    return sum(
        left_value * right_value
        for left_value, right_value in zip(left_delta, right_delta)
    ) / denominator


def counter_delta(first, last, maximum=None):
    if last >= first:
        return last - first
    if maximum:
        return maximum - first + last
    raise RuntimeError("non-wrapping counter decreased")


def counter_rate(delta, elapsed):
    return delta / elapsed if elapsed > 0 else None


def validate_sample_coverage(samples, start_ns, end_ns, label):
    require(len(samples) >= 28, "{} has too few samples".format(label))
    timestamps = [int(sample["timestamp_ns"]) for sample in samples]
    require(
        timestamps == sorted(timestamps)
        and len(timestamps) == len(set(timestamps)),
        "{} timestamps are unordered or duplicated".format(label),
    )
    require(
        timestamps[0] <= start_ns + 1_500_000_000,
        "{} starts too late".format(label),
    )
    require(
        timestamps[-1] >= end_ns - 1_500_000_000,
        "{} ends too early".format(label),
    )
    gaps = [
        right - left for left, right in zip(timestamps, timestamps[1:])
    ]
    require(
        gaps and max(gaps) <= 1_750_000_000,
        "{} contains a telemetry gap".format(label),
    )


def cpu_utilization(first, last, cpus):
    busy = total = 0
    for cpu in cpus:
        start = first["proc_stat"][str(cpu)]
        end = last["proc_stat"][str(cpu)]
        require(
            len(start) >= 8 and len(end) >= 8,
            "short /proc/stat CPU row",
        )
        deltas = [
            counter_delta(start[index], end[index])
            for index in CPU_TOTAL_FIELDS
        ]
        total += sum(deltas)
        busy += sum(deltas[index] for index in CPU_BUSY_FIELDS)
    return busy / total if total else None


def summed_delta(first, last, field, cpus):
    return sum(
        counter_delta(
            first[field]["totals"][str(cpu)],
            last[field]["totals"][str(cpu)],
        )
        for cpu in cpus
    )


def dominant_source(first, last, field, cpus, elapsed):
    candidates = []
    first_sources = first[field]["sources"]
    last_sources = last[field]["sources"]
    for source in set(first_sources) & set(last_sources):
        delta = sum(
            counter_delta(
                first_sources[source][str(cpu)],
                last_sources[source][str(cpu)],
            )
            for cpu in cpus
        )
        candidates.append((delta, source))
    if not candidates:
        return None, None
    delta, source = max(candidates)
    return source, counter_rate(delta, elapsed)


def block_delta(first, last, index):
    total = 0
    for device, start in first["block_stat"].items():
        end = last["block_stat"].get(device)
        if end is not None and len(start) > index and len(end) > index:
            total += counter_delta(start[index], end[index])
    return total


def named_delta(first, last, field, name):
    start = first[field].get(name)
    end = last[field].get(name)
    if start is None or end is None:
        return None
    return counter_delta(start, end)


def process_cpu_seconds(samples, comms):
    first_sample = samples[0]["process_cpu"]
    by_identity = {}
    for sample in samples:
        for pid, item in sample["process_cpu"].items():
            if item["comm"] not in comms:
                continue
            identity = (pid, item["starttime_ticks"])
            if identity not in by_identity:
                baseline = 0
                first_item = first_sample.get(pid)
                if (
                    first_item is not None
                    and first_item["starttime_ticks"]
                    == item["starttime_ticks"]
                ):
                    baseline = (
                        first_item["user_ticks"]
                        + first_item["system_ticks"]
                    )
                by_identity[identity] = [baseline, 0]
            by_identity[identity][1] = max(
                by_identity[identity][1],
                item["user_ticks"] + item["system_ticks"],
            )
    ticks = sum(
        max(0, maximum - baseline)
        for baseline, maximum in by_identity.values()
    )
    clock_ticks = samples[0]["clock_ticks_per_second"]
    return ticks / clock_ticks if clock_ticks else None


def package_power(first, last, elapsed_seconds, package_name):
    for key, start in first["rapl"].items():
        end = last["rapl"].get(key)
        if (
            start.get("name") == package_name
            and end is not None
            and start.get("energy_uj") is not None
            and end.get("energy_uj") is not None
        ):
            delta = counter_delta(
                start["energy_uj"],
                end["energy_uj"],
                start.get("max_energy_range_uj"),
            )
            return delta / 1_000_000.0 / elapsed_seconds
    return None


def parse_turbostat(path):
    records = []
    header = None
    for line in path.read_text(
        encoding="utf-8", errors="replace"
    ).splitlines():
        timestamp_text, separator, payload = line.partition("\t")
        if not separator or not timestamp_text.isdecimal():
            continue
        fields = payload.split()
        if "Busy%" in fields and (
            "Bzy_MHz" in fields or "Avg_MHz" in fields
        ):
            header = fields
            continue
        if header is None or len(fields) != len(header):
            continue
        values = {}
        for name, value in zip(header, fields):
            try:
                values[name] = float(value)
            except ValueError:
                values[name] = None
        records.append({
            "timestamp_ns": int(timestamp_text),
            "values": values,
        })
    require(len(records) >= 100, "turbostat records are incomplete")
    columns = set().union(*(record["values"] for record in records))
    require(
        "Busy%" in columns
        and ("Bzy_MHz" in columns or "Avg_MHz" in columns)
        and "PkgWatt" in columns,
        "turbostat lacks required frequency/power columns",
    )
    return records


def turbostat_metrics(records, start_ns, end_ns, label):
    covered = [
        record
        for record in records
        if start_ns <= record["timestamp_ns"] <= end_ns
    ]
    validate_sample_coverage(covered, start_ns, end_ns, label)
    selected = covered[1:]
    require(
        len(selected) >= 27,
        "{} lacks fully measured intervals".format(label),
    )
    frequency_field = (
        "Bzy_MHz"
        if any(
            record["values"].get("Bzy_MHz") is not None
            for record in selected
        )
        else "Avg_MHz"
    )
    require(
        all(
            record["values"].get("Busy%") is not None
            and record["values"].get(frequency_field) is not None
            and record["values"].get("PkgWatt") is not None
            for record in selected
        ),
        "{} contains unusable required values".format(label),
    )

    def values(name):
        return [
            record["values"][name]
            for record in selected
            if record["values"].get(name) is not None
        ]

    metrics = {
        "turbostat_records": len(selected),
        "turbostat_busy_percent_median": median(values("Busy%")),
        "turbostat_bzy_mhz_median": median(values("Bzy_MHz")),
        "turbostat_avg_mhz_median": median(values("Avg_MHz")),
        "turbostat_pkg_watts_median": median(values("PkgWatt")),
        "turbostat_ram_watts_median": median(values("RAMWatt")),
        "turbostat_pkg_temp_c_max": (
            max(values("PkgTmp")) if values("PkgTmp") else None
        ),
    }
    require(
        (
            metrics["turbostat_bzy_mhz_median"] is not None
            or metrics["turbostat_avg_mhz_median"] is not None
        )
        and metrics["turbostat_pkg_watts_median"] is not None,
        "{} lacks usable turbostat frequency/power data".format(label),
    )
    return metrics


def cell_telemetry(samples, turbostat, row):
    start_ns = int(row["measurement_start_ns"])
    end_ns = int(row["end_ns"])
    selected = [
        sample
        for sample in samples
        if start_ns <= int(sample["timestamp_ns"]) <= end_ns
    ]
    label = "cell {}".format(row["run_index"])
    validate_sample_coverage(selected, start_ns, end_ns, label)
    first, last = selected[0], selected[-1]
    elapsed = (
        int(last["monotonic_ns"]) - int(first["monotonic_ns"])
    ) / 1_000_000_000.0
    require(elapsed >= 27.0, "{} telemetry duration is short".format(label))

    server_frequencies = [
        statistics.mean(
            float(sample["cpu_freq_khz"][str(cpu)])
            for cpu in SERVER_CPUS
        )
        for sample in selected
    ]
    client_frequencies = [
        statistics.mean(
            float(sample["cpu_freq_khz"][str(cpu)])
            for cpu in PGBENCH_CPUS
        )
        for sample in selected
    ]
    temperatures = [
        value / 1000.0
        for sample in selected
        for value in sample["temperatures_millic"].values()
    ]
    interrupt_source, interrupt_rate = dominant_source(
        first, last, "interrupts", SERVER_CPUS, elapsed
    )
    softirq_source, softirq_rate = dominant_source(
        first, last, "softirqs", SERVER_CPUS, elapsed
    )
    metrics = {
        "system_samples": len(selected),
        "system_elapsed_seconds": elapsed,
        "server_freq_khz_median": median(server_frequencies),
        "server_freq_khz_min": min(server_frequencies),
        "server_freq_khz_max": max(server_frequencies),
        "client_freq_khz_median": median(client_frequencies),
        "server_cpu_utilization": cpu_utilization(
            first, last, SERVER_CPUS
        ),
        "client_cpu_utilization": cpu_utilization(
            first, last, PGBENCH_CPUS
        ),
        "server_interrupts_per_second": counter_rate(
            summed_delta(first, last, "interrupts", SERVER_CPUS),
            elapsed,
        ),
        "server_softirqs_per_second": counter_rate(
            summed_delta(first, last, "softirqs", SERVER_CPUS),
            elapsed,
        ),
        "dominant_server_interrupt_source": interrupt_source,
        "dominant_server_interrupts_per_second": interrupt_rate,
        "dominant_server_softirq_source": softirq_source,
        "dominant_server_softirqs_per_second": softirq_rate,
        "block_read_sectors_per_second": counter_rate(
            block_delta(first, last, 2), elapsed
        ),
        "block_write_sectors_per_second": counter_rate(
            block_delta(first, last, 6), elapsed
        ),
        "node0_numa_miss_per_second": counter_rate(
            named_delta(first, last, "node0_numastat", "numa_miss"),
            elapsed,
        ),
        "node1_numa_miss_per_second": counter_rate(
            named_delta(first, last, "node1_numastat", "numa_miss"),
            elapsed,
        ),
        "node0_other_node_per_second": counter_rate(
            named_delta(first, last, "node0_numastat", "other_node"),
            elapsed,
        ),
        "node1_other_node_per_second": counter_rate(
            named_delta(first, last, "node1_numastat", "other_node"),
            elapsed,
        ),
        "postgres_cpu_cores": (
            process_cpu_seconds(selected, {"postgres", "postmaster"})
            / elapsed
        ),
        "pgbench_cpu_cores": (
            process_cpu_seconds(selected, {"pgbench"}) / elapsed
        ),
        "osquery_cpu_cores": (
            process_cpu_seconds(selected, {"osqueryd"}) / elapsed
        ),
        "exporter_cpu_cores": (
            process_cpu_seconds(
                selected,
                {"node_exporter", "process-exporte"},
            )
            / elapsed
        ),
        "package0_watts": package_power(
            first, last, elapsed, "package-0"
        ),
        "package1_watts": package_power(
            first, last, elapsed, "package-1"
        ),
        "temperature_c_max": max(temperatures) if temperatures else None,
    }
    metrics.update(turbostat_metrics(
        turbostat,
        start_ns,
        end_ns,
        "turbostat " + label,
    ))
    return metrics


def analyze(root):
    worker_root = root / "worker"
    telemetry_root = root / "telemetry"
    with (worker_root / "results.csv").open(
        newline="", encoding="utf-8"
    ) as stream:
        reader = csv.DictReader(stream)
        require(
            tuple(reader.fieldnames or ()) == RESULT_FIELDS,
            "results.csv schema differs",
        )
        rows = list(reader)
    require(len(rows) == EXPECTED_ROWS, "results.csv is incomplete")
    require(
        [int(row["run_index"]) for row in rows]
        == list(range(1, EXPECTED_ROWS + 1)),
        "run indices are incomplete or unordered",
    )
    require(
        {
            (row["config"], int(row["repetition"]))
            for row in rows
        }
        == {
            (config, repetition)
            for config in EXPECTED_CONFIGS
            for repetition in range(1, EXPECTED_REPETITIONS + 1)
        },
        "configuration/repetition matrix is incomplete",
    )
    for row in rows:
        run_index = int(row["run_index"])
        start_ns = int(row["start_ns"])
        measurement_start_ns = int(row["measurement_start_ns"])
        end_ns = int(row["end_ns"])
        measurement_seconds = (
            end_ns - measurement_start_ns
        ) / 1_000_000_000.0
        warmup_seconds = (
            measurement_start_ns - start_ns
        ) / 1_000_000_000.0
        require(
            start_ns < measurement_start_ns < end_ns
            and 9.5 <= warmup_seconds <= 10.5
            and 28.0 <= measurement_seconds <= 32.0,
            "cell {} has invalid measurement boundaries".format(
                run_index
            ),
        )
        require(
            row["server_cpus"] == "1-63:2"
            and row["pgbench_cpus"] == "0-14:2",
            "cell {} has invalid CPU masks".format(run_index),
        )
        require(
            row["pgbench_log"]
            == "logs/pgbench-{}.log".format(run_index)
            and row["server_log"]
            == "logs/server-{}.log".format(run_index),
            "cell {} has invalid log paths".format(run_index),
        )
        for field in (
            "tps",
            "latency_ms",
            "measurement_interval_s",
            "within_cell_cv_percent",
            "pgbench_cpu_percent",
            "pgbench_capacity_fraction",
        ):
            require(
                math.isfinite(float(row[field])),
                "cell {} has non-finite {}".format(run_index, field),
            )
        require(
            float(row["tps"]) > 0
            and float(row["latency_ms"]) >= 0
            and 28.0 <= float(row["measurement_interval_s"]) <= 31.0
            and int(row["measurement_samples"]) >= 28
            and float(row["within_cell_cv_percent"]) >= 0
            and 0 <= float(row["pgbench_capacity_fraction"]) < 0.9,
            "cell {} has invalid workload metrics".format(run_index),
        )
    samples = [
        json.loads(line)
        for line in (telemetry_root / "system.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
    ]
    require(len(samples) >= 100, "system telemetry is incomplete")
    require(
        all(sample.get("schema_version") == 1 for sample in samples),
        "system telemetry schema differs",
    )
    require(
        all(
            sample["intel_pstate"]["no_turbo"] == 0
            for sample in samples
        ),
        "turbo state changed or was disabled during the diagnostic",
    )
    turbostat = parse_turbostat(telemetry_root / "turbostat.tsv")

    enriched = []
    for row in rows:
        item = dict(row)
        item.update(cell_telemetry(samples, turbostat, row))
        enriched.append(item)

    output_fields = list(enriched[0])
    with (root / "analysis.csv").open(
        "x", newline="", encoding="utf-8"
    ) as stream:
        writer = csv.DictWriter(stream, fieldnames=output_fields)
        writer.writeheader()
        writer.writerows(enriched)

    by_config = defaultdict(list)
    for row in enriched:
        by_config[row["config"]].append(float(row["tps"]))
    config_summary = {}
    for config, values in by_config.items():
        config_summary[config] = {
            "n": len(values),
            "mean_tps": statistics.mean(values),
            "stdev_tps": statistics.stdev(values),
            "cv_percent": (
                statistics.stdev(values)
                / statistics.mean(values)
                * 100.0
            ),
            "minimum_tps": min(values),
            "maximum_tps": max(values),
        }

    by_key = {
        (row["config"], int(row["repetition"])): row
        for row in enriched
    }
    paired = []
    for repetition in range(1, EXPECTED_REPETITIONS + 1):
        left = float(by_key[("baseline-a", repetition)]["tps"])
        right = float(by_key[("baseline-b", repetition)]["tps"])
        paired.append((right - left) / ((left + right) / 2.0) * 100.0)

    correlation_fields = (
        "server_freq_khz_median",
        "client_freq_khz_median",
        "server_cpu_utilization",
        "client_cpu_utilization",
        "server_interrupts_per_second",
        "server_softirqs_per_second",
        "dominant_server_interrupts_per_second",
        "dominant_server_softirqs_per_second",
        "block_read_sectors_per_second",
        "block_write_sectors_per_second",
        "node0_numa_miss_per_second",
        "node1_numa_miss_per_second",
        "node0_other_node_per_second",
        "node1_other_node_per_second",
        "postgres_cpu_cores",
        "pgbench_cpu_cores",
        "osquery_cpu_cores",
        "exporter_cpu_cores",
        "package0_watts",
        "package1_watts",
        "temperature_c_max",
        "turbostat_busy_percent_median",
        "turbostat_bzy_mhz_median",
        "turbostat_avg_mhz_median",
        "turbostat_pkg_watts_median",
        "turbostat_ram_watts_median",
        "turbostat_pkg_temp_c_max",
    )
    metric_correlations = {}
    for field in correlation_fields:
        pairs = [
            (float(row["tps"]), row[field])
            for row in enriched
            if row[field] is not None
        ]
        metric_correlations[field] = {
            "n": len(pairs),
            "r": (
                correlation(
                    [pair[0] for pair in pairs],
                    [float(pair[1]) for pair in pairs],
                )
                if pairs
                else None
            ),
        }

    report = {
        "schema_version": 1,
        "rows": len(enriched),
        "turbo_expected": "enabled",
        "configurations": config_summary,
        "paired_symmetric_percent": {
            "values": paired,
            "mean": statistics.mean(paired),
            "stdev": statistics.stdev(paired),
        },
        "exploratory_tps_correlations": metric_correlations,
        "maximum_within_cell_cv_percent": max(
            float(row["within_cell_cv_percent"]) for row in enriched
        ),
        "maximum_pgbench_capacity_fraction": max(
            float(row["pgbench_capacity_fraction"]) for row in enriched
        ),
    }
    (root / "analysis.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    lines = [
        "# W6c power-state diagnostic",
        "",
        "Rows: **{}**. Turbo remained enabled.".format(len(enriched)),
        "",
        "| Configuration | Mean TPS | CV | Range |",
        "|---|---:|---:|---:|",
    ]
    for config in EXPECTED_CONFIGS:
        item = config_summary[config]
        lines.append(
            "| {} | {:.1f} | {:.3f}% | {:.1f}–{:.1f} |".format(
                config,
                item["mean_tps"],
                item["cv_percent"],
                item["minimum_tps"],
                item["maximum_tps"],
            )
        )
    lines.extend([
        "",
        "Mean paired A/A difference: **{:+.3f}%**.".format(
            statistics.mean(paired)
        ),
        "",
        "Maximum within-cell CV: **{:.3f}%**.".format(
            report["maximum_within_cell_cv_percent"]
        ),
        "",
        "## Exploratory TPS correlations",
        "",
        "These coefficients are diagnostic clues, not significance tests.",
        "",
    ])
    for field, item in metric_correlations.items():
        value = item["r"]
        lines.append(
            "- {} (n={}): {}".format(
                field,
                item["n"],
                "unavailable" if value is None else "{:+.3f}".format(value),
            )
        )
    (root / "analysis.md").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    report = analyze(args.root.resolve())
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
