#!/usr/bin/env python3
"""Synthetic validation for the W6c telemetry analyzer."""

import csv
import importlib.util
import json
import tempfile
from pathlib import Path


HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location(
    "w6c_analyze", HERE / "analyze.py"
)
ANALYZE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ANALYZE)


def write_fixture(root, turbo_value=0):
    worker = root / "worker"
    telemetry = root / "telemetry"
    worker.mkdir()
    telemetry.mkdir()
    base_ns = 1_700_000_000_000_000_000
    result_fields = list(ANALYZE.RESULT_FIELDS)
    rows = []
    for index in range(16):
        config = "baseline-a" if index % 2 == 0 else "baseline-b"
        repetition = index // 2 + 1
        measurement_start = base_ns + (index * 40 + 5) * 1_000_000_000
        rows.append({
            "run_index": index + 1,
            "timestamp_utc": "2026-09-17T00:00:00Z",
            "config": config,
            "repetition": repetition,
            "tps": 540000 + index * 2500,
            "latency_ms": 0.1,
            "measurement_samples": 29,
            "measurement_interval_s": 29,
            "within_cell_cv_percent": 0.2,
            "warmup_to_measurement_percent": -1.0,
            "pgbench_cpu_percent": 10.0,
            "pgbench_capacity_fraction": 0.0125,
            "start_ns": measurement_start - 10_000_000_000,
            "measurement_start_ns": measurement_start,
            "end_ns": measurement_start + 30_000_000_000,
            "postmaster_pid": 1000 + index,
            "server_cpus": "1-63:2",
            "pgbench_cpus": "0-14:2",
            "pgbench_log": "logs/pgbench-{}.log".format(index + 1),
            "server_log": "logs/server-{}.log".format(index + 1),
        })
    with (worker / "results.csv").open(
        "w", newline="", encoding="utf-8"
    ) as stream:
        writer = csv.DictWriter(stream, fieldnames=result_fields)
        writer.writeheader()
        writer.writerows(rows)

    final_second = 16 * 40 + 40
    with (telemetry / "system.jsonl").open(
        "w", encoding="utf-8"
    ) as system_stream, (telemetry / "turbostat.tsv").open(
        "w", encoding="utf-8"
    ) as turbo_stream:
        turbo_stream.write(
            "timestamp_ns\tPackage Core CPU Avg_MHz Busy% "
            "Bzy_MHz TSC_MHz PkgTmp PkgWatt RAMWatt\n"
        )
        turbo_stream.write(
            "{}\tPackage Core CPU Avg_MHz Busy% "
            "Bzy_MHz TSC_MHz PkgTmp PkgWatt RAMWatt\n".format(base_ns)
        )
        for second in range(final_second):
            timestamp = base_ns + second * 1_000_000_000
            proc_stat = {}
            frequency = {}
            interrupt_totals = {}
            interrupt_source = {}
            softirq_totals = {}
            softirq_source = {}
            for cpu in range(64):
                proc_stat[str(cpu)] = [
                    second * 100 + cpu,
                    second,
                    second * 20,
                    second * 30,
                    second,
                    second * 2,
                    second * 3,
                    0,
                ]
                frequency[str(cpu)] = (
                    2_600_000 + second * 1000 + cpu * 100
                )
                interrupt_totals[str(cpu)] = second * (cpu + 1)
                interrupt_source[str(cpu)] = second * (cpu + 1)
                softirq_totals[str(cpu)] = second * 2 * (cpu + 1)
                softirq_source[str(cpu)] = second * 2 * (cpu + 1)
            sample = {
                "schema_version": 1,
                "timestamp_ns": timestamp,
                "monotonic_ns": second * 1_000_000_000,
                "proc_stat": proc_stat,
                "loadavg": "0.10 0.10 0.10 1/100 1",
                "interrupts": {
                    "totals": interrupt_totals,
                    "sources": {
                        "24:eth0-TxRx-0": interrupt_source,
                    },
                },
                "softirqs": {
                    "totals": softirq_totals,
                    "sources": {
                        "NET_RX:": softirq_source,
                    },
                },
                "cpu_freq_khz": frequency,
                "intel_pstate": {
                    "no_turbo": turbo_value,
                    "min_perf_pct": 20,
                    "max_perf_pct": 100,
                },
                "node0_numastat": {
                    "numa_miss": second * 2,
                    "other_node": second,
                },
                "node1_numastat": {
                    "numa_miss": second * 3,
                    "other_node": second * 2,
                },
                "block_stat": {
                    "259:0:nvme0n1": [
                        second,
                        0,
                        second * 8,
                        0,
                        second,
                        0,
                        second * 16,
                        0,
                        0,
                        0,
                        0,
                    ],
                },
                "process_cpu": {
                    "100": {
                        "comm": "postgres",
                        "user_ticks": second * 50,
                        "system_ticks": second * 10,
                        "starttime_ticks": 1000,
                    },
                    "200": {
                        "comm": "pgbench",
                        "user_ticks": second * 10,
                        "system_ticks": second * 2,
                        "starttime_ticks": 2000,
                    },
                    "300": {
                        "comm": "osqueryd",
                        "user_ticks": second,
                        "system_ticks": 0,
                        "starttime_ticks": 3000,
                    },
                },
                "clock_ticks_per_second": 100,
                "rapl": {
                    "intel-rapl:0": {
                        "name": "package-0",
                        "energy_uj": second * 100_000_000,
                        "max_energy_range_uj": 2**62,
                    },
                    "intel-rapl:1": {
                        "name": "package-1",
                        "energy_uj": second * 120_000_000,
                        "max_energy_range_uj": 2**62,
                    },
                },
                "temperatures_millic": {
                    "/sys/class/hwmon/hwmon0:coretemp:"
                    "Package id 0:temp1_input": 65000 + second,
                },
            }
            system_stream.write(json.dumps(sample) + "\n")
            turbo_stream.write(
                "{}\t- - - {} 70.0 {} 2400.0 "
                "70.0 220.0 30.0\n".format(
                    timestamp,
                    2000 + second,
                    2900 + second,
                )
            )


def main():
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        write_fixture(root)
        report = ANALYZE.analyze(root)
        assert report["rows"] == 16
        assert set(report["configurations"]) == {
            "baseline-a",
            "baseline-b",
        }
        assert (
            report["exploratory_tps_correlations"][
                "server_freq_khz_median"
            ]["n"]
            == 16
        )
        assert (
            report["exploratory_tps_correlations"][
                "server_freq_khz_median"
            ]["r"]
            > 0.9
        )
        assert (root / "analysis.csv").is_file()
        assert (root / "analysis.json").is_file()
        assert (root / "analysis.md").is_file()

    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        write_fixture(root, turbo_value=1)
        try:
            ANALYZE.analyze(root)
        except RuntimeError as error:
            assert "turbo" in str(error)
        else:
            raise AssertionError("disabled turbo was not rejected")

    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        write_fixture(root)
        path = root / "telemetry/system.jsonl"
        lines = path.read_text(encoding="utf-8").splitlines()
        del lines[20]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        try:
            ANALYZE.analyze(root)
        except RuntimeError as error:
            assert "gap" in str(error)
        else:
            raise AssertionError("a missing system sample was not rejected")

    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        write_fixture(root)
        path = root / "telemetry/turbostat.tsv"
        lines = path.read_text(encoding="utf-8").splitlines()
        timestamp, payload = lines[22].split("\t", 1)
        fields = payload.split()
        fields[-2] = "-"
        lines[22] = timestamp + "\t" + " ".join(fields)
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        try:
            ANALYZE.analyze(root)
        except RuntimeError as error:
            assert "unusable" in str(error)
        else:
            raise AssertionError(
                "an unusable turbostat value was not rejected"
            )

    print("w6c-power-state self-test: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
