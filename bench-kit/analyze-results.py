#!/usr/bin/env python3
"""Verify and summarize a v11 wait-event benchmark results tree.

Unlike the v10 analyzer, this compares seven configurations against
vanilla in a single matrix (no v9/v10 pair), and reports two estimators
per contrast -- a paired Student t interval and an exact Wilcoxon
signed-rank / Hodges-Lehmann interval (wilcoxon.py) -- so that a
session-level throughput plateau that would otherwise drag the mean off
zero is visible as "unresolved" rather than a false resolved difference
(brief-v11-wpc-kit.md).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import build_manifest_rules
from cpu_affinity import validate_affinity_proof, validate_host_report
from benchmark_protocol import (
    ACTIVE_CONFIGS,
    ALL_CONTRASTS,
    BOUND_KIT_FILES,
    BUILD_FOR_CONFIG,
    CLIENTS_FOR_WORKLOAD,
    CONFIGS,
    EARLY_AA_GATE_MAX_HALF_WIDTH_PERCENT,
    EARLY_AA_GATE_REPETITIONS,
    EARLY_AA_GATE_WORKLOAD,
    FULL_PROFILE,
    MODULE_GUCS,
    PGBENCH_WORKLOADS,
    RESULT_FIELDS,
    SHARED_BUFFERS_FOR_WORKLOAD,
    SMOKE_PROFILE,
    TRACE_CONFIGS,
    W1_FUNCTIONS,
    W3_PROTOCOL,
    WORKLOADS,
    pgbench_margin_log,
)
from latin_square import verify_schedule as verify_latin_square_schedule
from stats_common import classify_contrast, confidence_interval, describe
from w3_qualification import QualificationError, analyze_file
from wilcoxon import wilcoxon_signed_rank


class InvalidResults(Exception):
    """Raised when retained evidence is incomplete or inconsistent."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise InvalidResults(message)


def evidence_equal(left: object, right: object) -> bool:
    """Compare retained/recomputed evidence with float roundoff tolerance."""
    if isinstance(left, bool) or isinstance(right, bool):
        return left is right
    if isinstance(left, int) and isinstance(right, int):
        return left == right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        return math.isclose(float(left), float(right), rel_tol=1e-14, abs_tol=1e-15)
    if isinstance(left, dict) and isinstance(right, dict):
        return left.keys() == right.keys() and all(
            evidence_equal(left[key], right[key]) for key in left
        )
    if isinstance(left, list) and isinstance(right, list):
        return len(left) == len(right) and all(
            evidence_equal(a, b) for a, b in zip(left, right)
        )
    return left == right


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def finite_number(value: str, label: str, *, positive: bool = False) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise InvalidResults(f"{label} is not numeric: {value!r}") from exc
    require(math.isfinite(parsed), f"{label} is not finite")
    if positive:
        require(parsed > 0, f"{label} must be positive")
    return parsed


def locate(root: Path) -> tuple[Path, Path, Path]:
    if (root / "results" / "results.csv").is_file():
        build = root / "build"
        if not (build / "manifest.json").is_file():
            build = root / "work"
        kit = root / "kit"
        if not kit.is_dir():
            kit = root
        return root / "results", build, kit
    if (root / "results.csv").is_file():
        build = root.parent / "build"
        if not (build / "manifest.json").is_file():
            build = root.parent / "work"
        kit = root.parent / "kit"
        if not kit.is_dir():
            kit = root.parent
        return root, build, kit
    raise InvalidResults(f"could not find results.csv under {root}")


def read_csv(path: Path, expected_fields: tuple | None = None) -> list:
    require(path.is_file(), f"missing {path}")
    with path.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        if expected_fields is not None:
            require(tuple(reader.fieldnames or ()) == expected_fields,
                    f"unexpected CSV schema in {path}")
        return list(reader)


def verify_manifest(build_dir: Path) -> dict:
    path = build_dir / "manifest.json"
    require(path.is_file(), f"missing build manifest: {path}")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    require(manifest.get("schema_version") == 11, "unsupported build manifest schema")
    require(manifest.get("benchmark_series") == "wet-v11",
            "unexpected build benchmark series")
    require(manifest.get("compiler_cache") == "disabled",
            "build manifest does not prove compiler caches were disabled")
    require(manifest.get("build_system") == "configure-make",
            "build did not use the required configure/make backend")
    require(
        manifest.get("configure_flags")
        == ["--disable-rpath", "--without-icu", "--without-readline", "--without-zlib"],
        "build flags differ from the fixed protocol",
    )
    builds = {item["name"]: item for item in manifest.get("builds", [])}
    require(set(builds) == {"baseline-a", "baseline-b", "patched", "control"},
            f"unexpected build set: {sorted(builds)}")
    # Single shared rule (build_manifest_rules.py): five binaries hashed in
    # all four builds, fixture hashed in all four builds, tracing module
    # present and hash-matched in "patched" only, absent (or "none")
    # everywhere else. This used to be a private, and backwards, copy of
    # that rule right here -- see build_manifest_rules.validate_manifest_
    # hashes()'s docstring and reports/wpf-report.md, Addendum 5.
    try:
        build_manifest_rules.validate_manifest_hashes(builds)
    except build_manifest_rules.ManifestMismatch as exc:
        raise InvalidResults(str(exc)) from exc
    binaries = ("postgres", "pgbench", "psql", "initdb", "pg_ctl", "test_wait_primitive")
    for binary in binaries:
        require(
            builds["baseline-a"]["sha256"][binary] == builds["baseline-b"]["sha256"][binary],
            f"independent baseline builds differ for {binary}",
        )
    require(builds["baseline-a"].get("install_tree") == builds["baseline-b"].get("install_tree"),
            "independent baseline installation trees differ")
    require(len({item["fixture_tree"] for item in builds.values()}) == 1,
            "source fixture tree differs across build records")
    require(
        builds["patched"]["sha256"]["postgres"] != builds["control"]["sha256"]["postgres"],
        "patched and control postgres binaries are byte-identical -- the "
        "control patch (patches-control) is supposed to change core codegen",
    )
    return manifest


def verify_protocol(protocol: dict) -> None:
    require(protocol.get("schema_version") == 11, "unsupported protocol schema")
    require(protocol.get("benchmark_series") == "wet-v11",
            "unexpected protocol benchmark series")
    mode = protocol.get("mode")
    require(mode in {"full", "smoke"}, f"unsupported protocol mode: {mode}")
    expected = FULL_PROFILE if mode == "full" else SMOKE_PROFILE
    for name, value in expected.items():
        require(protocol.get(name) == value,
                f"protocol {name} is {protocol.get(name)!r}, expected {value!r}")
    require(tuple(protocol.get("configs", ())) == CONFIGS,
            "protocol configuration order differs")
    require(tuple(protocol.get("workloads", ())) == WORKLOADS,
            "protocol workload order differs")
    require(protocol.get("module_name") == "pg_wait_event_tracing",
            "protocol module name differs")
    require(protocol.get("guc_capture") == MODULE_GUCS["capture"],
            "protocol capture GUC differs")
    require(protocol.get("guc_max_tranches") == MODULE_GUCS["max_tranches"],
            "protocol tranche GUC differs")
    require(protocol.get("guc_trace_ring_size") == MODULE_GUCS["trace_ring_size"],
            "protocol trace-ring GUC differs")
    validate_affinity_proof(protocol.get("cpu_affinity_protocol"))
    require(
        protocol.get("server_cpus") and protocol.get("pgbench_cpus"),
        "protocol lacks configured CPU-affinity masks",
    )
    require(protocol.get("max_pgbench_thread_capacity_fraction") == 0.90,
            "pgbench saturation limit differs from the fixed protocol")
    require(protocol.get("w3_qualification") == W3_PROTOCOL,
            "W3 thresholds differ from the fixed protocol")
    gate = protocol.get("analysis", {}).get("early_aa_gate", {})
    require(
        gate.get("check_after_repetitions") == list(EARLY_AA_GATE_REPETITIONS)
        and gate.get("workload") == EARLY_AA_GATE_WORKLOAD
        and gate.get("max_half_width_percent") == EARLY_AA_GATE_MAX_HALF_WIDTH_PERCENT,
        "early A/A gate protocol differs from the fixed rule",
    )


def verify_host(root: Path, protocol: dict, manifest: dict) -> dict:
    hashes = protocol.get("host_check_sha256")
    require(
        isinstance(hashes, dict) and set(hashes) == {"host-check.txt", "host-check.json"},
        "protocol lacks the exact host-check hash set",
    )
    for name, expected in hashes.items():
        path = root / name
        require(path.is_file() and not path.is_symlink(), f"missing regular host report: {name}")
        require(sha256(path) == expected, f"host report hash mismatch: {name}")
    report = json.loads((root / "host-check.json").read_text(encoding="utf-8"))
    try:
        validate_host_report(report)
    except ValueError as error:
        raise InvalidResults(str(error)) from error
    require(report.get("hostname") == manifest.get("build_host"),
            "host check and build manifest are from different hosts")
    affinity = report["cpu_affinity_protocol"]
    require(
        affinity.get("server_cpus") == protocol.get("server_cpus")
        and affinity.get("pgbench_cpus") == protocol.get("pgbench_cpus"),
        "host-check CPU masks differ from the masks recorded in protocol.json",
    )
    process_count = report.get("co_resident_postgres_process_count")
    require(isinstance(process_count, int) and process_count >= 0,
            "host report lacks a valid PostgreSQL process count")
    expected_isolation = "co-resident" if process_count else "dedicated"
    require(report.get("host_isolation") == expected_isolation,
            "host isolation label disagrees with PostgreSQL process count")
    return report


def verify_plateau_probe(root: Path) -> dict:
    path = root / "plateau-probe-result.json"
    require(path.is_file(), "missing plateau-probe-result.json")
    data = json.loads(path.read_text(encoding="utf-8"))
    require(data.get("selected_variant") in {"pinned", "unpinned"},
            "plateau probe did not record a valid selected_variant")
    for key in ("pinned_tps", "unpinned_tps"):
        values = data.get(key)
        require(isinstance(values, list) and len(values) == 4,
                f"plateau probe {key} must have exactly 4 sessions")
    require(isinstance(data.get("numactl_available"), bool),
            "plateau probe lacks numactl_available")
    require(isinstance(data.get("server_numa_node"), int),
            "plateau probe lacks server_numa_node")
    return data


def verify_bound_kit(protocol: dict, kit_dir: Path) -> None:
    bound = protocol.get("bound_file_sha256")
    require(isinstance(bound, dict), "protocol has no bound kit-file hashes")
    require(set(bound) == set(BOUND_KIT_FILES),
            "protocol does not bind the exact required kit-file set")
    root = kit_dir.resolve()
    for relative, expected in bound.items():
        candidate = Path(relative)
        require(
            not candidate.is_absolute() and relative == candidate.as_posix()
            and ".." not in candidate.parts,
            f"unsafe bound kit path: {relative}",
        )
        path = kit_dir / relative
        try:
            path.resolve().relative_to(root)
            inside_root = True
        except ValueError:
            inside_root = False
        require(path.is_file() and not path.is_symlink() and inside_root,
                f"archive omits regular bound kit file: {relative}")
        require(
            isinstance(expected, str) and len(expected) == 64
            and all(char in "0123456789abcdef" for char in expected),
            f"invalid bound kit hash: {relative}",
        )
        require(sha256(path) == expected, f"bound kit file hash mismatch: {relative}")


def verify_schedule(results_dir: Path, rows: list, protocol: dict) -> list:
    schedule_rows = read_csv(
        results_dir / "schedule.csv",
        ("run_index", "config", "workload", "repetition", "block", "position"),
    )
    expected_count = int(protocol["expected_cells"])
    require(len(schedule_rows) == expected_count,
            f"schedule has {len(schedule_rows)} rows, expected {expected_count}")
    runs = int(protocol["runs_per_cell"])
    schedule = []
    per_workload_positions: dict[str, dict[int, list]] = defaultdict(lambda: defaultdict(dict))
    for position_index, row in enumerate(schedule_rows, 1):
        item = (
            int(row["run_index"]), row["config"], row["workload"],
            int(row["repetition"]), row["block"], int(row["position"]),
        )
        require(item[0] == position_index,
                f"schedule run_index is not sequential at row {position_index}")
        require(item[1] in CONFIGS, f"unknown schedule config: {item[1]}")
        require(item[2] in WORKLOADS, f"unknown schedule workload: {item[2]}")
        require(1 <= item[5] <= len(CONFIGS), f"schedule position out of range: {item[5]}")
        require(item[4] == f"{item[2]}-{item[3]:02d}",
                f"schedule block id does not match workload/repetition: {item}")
        per_workload_positions[item[2]][item[3]][item[5]] = item[1]
        schedule.append(item)

    require(len(rows) == expected_count,
            f"results has {len(rows)} rows, expected {expected_count}")
    observed = [
        (int(row["run_index"]), row["config"], row["workload"], int(row["repetition"]),
         row["block"], int(row["position"]))
        for row in rows
    ]
    require(observed == schedule, "results rows do not match the recorded schedule exactly")

    for workload, by_repetition in per_workload_positions.items():
        for repetition, by_position in by_repetition.items():
            require(sorted(by_position) == list(range(1, len(CONFIGS) + 1)),
                    f"{workload} repetition {repetition} does not have a full position set")
            require(sorted(by_position.values()) == sorted(CONFIGS),
                    f"{workload} repetition {repetition} is not a complete configuration block")
        if protocol["mode"] == "full":
            order_by_repetition = [
                [by_repetition[repetition][position] for position in range(1, len(CONFIGS) + 1)]
                for repetition in range(1, runs + 1)
            ]
            try:
                verify_latin_square_schedule(order_by_repetition, CONFIGS, runs // len(CONFIGS))
            except ValueError as error:
                raise InvalidResults(f"{workload}: {error}") from error
    return schedule


def verify_rows(rows: list, protocol: dict) -> None:
    expected_keys = {
        (config, workload, repetition)
        for config in CONFIGS
        for workload in WORKLOADS
        for repetition in range(1, int(protocol["runs_per_cell"]) + 1)
    }
    actual_keys = {(row["config"], row["workload"], int(row["repetition"])) for row in rows}
    require(len(actual_keys) == len(rows), "duplicate result cell")
    require(actual_keys == expected_keys, "result matrix is incomplete")
    expected_seed = str(protocol["seed"])
    server_cpus = protocol["server_cpus"]
    pgbench_cpus = protocol["pgbench_cpus"]
    for row in rows:
        label = f"{row['config']}/{row['workload']}/rep{row['repetition']}"
        require(row["seed"] == expected_seed, f"{label}: seed mismatch")
        require(row["build"] == BUILD_FOR_CONFIG[row["config"]],
                f"{label}: build/config mismatch")
        require(row["shared_buffers"] == SHARED_BUFFERS_FOR_WORKLOAD[row["workload"]],
                f"{label}: shared_buffers mismatch")
        require(row["server_cpus"] == server_cpus and row["pgbench_cpus"] == pgbench_cpus,
                f"{label}: CPU affinity differs from the run's protocol")
        log_path = Path(row["server_log"])
        require(not log_path.is_absolute() and ".." not in log_path.parts,
                f"{label}: unsafe server-log path")

        if row["workload"] == "W1":
            finite_number(row["ns_per_iteration"], f"{label} ns_per_iteration", positive=True)
            require(row["iterations"] == str(protocol["w1_iterations"] * len(W1_FUNCTIONS)),
                    f"{label}: wrong W1 iteration count")
            require(row["clients"] == "" and row["duration_s"] == "" and row["warmup_s"] == "",
                    f"{label}: unexpected pgbench fields")
            for field in ("tps", "latency_avg_ms", "measurement_samples",
                          "measurement_interval_s", "pgbench_cpu_percent",
                          "pgbench_cpu_capacity_fraction"):
                require(row[field] == "", f"{label}: unexpected {field}")
        else:
            require(row["clients"] == str(CLIENTS_FOR_WORKLOAD[row["workload"]]),
                    f"{label}: client count mismatch")
            require(row["duration_s"] == str(protocol["duration_seconds"]),
                    f"{label}: duration mismatch")
            require(row["warmup_s"] == str(protocol["warmup_seconds"]),
                    f"{label}: warmup mismatch")
            require(row["iterations"] == "" and row["ns_per_iteration"] == "",
                    f"{label}: unexpected W1 fields")
            finite_number(row["tps"], f"{label} tps", positive=True)
            finite_number(row["latency_avg_ms"], f"{label} latency", positive=True)
            samples = int(row["measurement_samples"])
            require(samples > 0, f"{label}: no progress samples")
            interval = finite_number(row["measurement_interval_s"],
                                      f"{label} measurement interval", positive=True)
            require(
                protocol["duration_seconds"] - 2 <= interval <= protocol["duration_seconds"] + 1,
                f"{label}: unexpected measurement interval {interval}",
            )
            finite_number(row["pgbench_cpu_percent"], f"{label} pgbench CPU")
            capacity = finite_number(row["pgbench_cpu_capacity_fraction"],
                                      f"{label} pgbench CPU capacity")
            require(0 <= capacity < 0.90, f"{label}: pgbench client capacity out of range")

        if row["cpu_freq_khz_mean"]:
            freq_mean = finite_number(row["cpu_freq_khz_mean"], f"{label} freq mean", positive=True)
            freq_min = finite_number(row["cpu_freq_khz_min"], f"{label} freq min", positive=True)
            freq_max = finite_number(row["cpu_freq_khz_max"], f"{label} freq max", positive=True)
            require(freq_min <= freq_mean <= freq_max,
                    f"{label}: frequency covariate ordering is inconsistent")
        if row["numa_local_fraction"]:
            fraction = finite_number(row["numa_local_fraction"], f"{label} NUMA-local fraction")
            require(0.0 <= fraction <= 1.0, f"{label}: NUMA-local fraction out of range")
        if row["meminfo_cached_kb_before"]:
            finite_number(row["meminfo_cached_kb_before"], f"{label} cached before", positive=True)
            finite_number(row["meminfo_cached_kb_after"], f"{label} cached after", positive=True)
        if row["load_average_before"]:
            finite_number(row["load_average_before"], f"{label} load average")


def verify_completion(results_dir: Path, rows: list) -> None:
    path = results_dir / "matrix-complete.json"
    require(path.is_file(), "matrix-complete.json is missing")
    marker = json.loads(path.read_text(encoding="utf-8"))
    require(marker.get("rows") == len(rows), "matrix-complete row count mismatch")
    for name in ("results.csv", "schedule.csv", "protocol.json", "progress.json",
                 "aa-early.jsonl"):
        expected = marker.get("sha256", {}).get(name)
        require(expected == sha256(results_dir / name), f"matrix-complete hash mismatch: {name}")


def verify_progress(results_dir: Path, rows: list) -> None:
    progress = json.loads((results_dir / "progress.json").read_text(encoding="utf-8"))
    require(progress.get("state") == "complete", "matrix progress does not report completion")
    require(progress.get("cells_completed") == len(rows),
            "matrix progress completed-cell count is wrong")
    require(progress.get("cells_total") == len(rows),
            "matrix progress total-cell count is wrong")


def verify_early_aa(results_dir: Path, protocol: dict) -> None:
    entries = [
        json.loads(line)
        for line in (results_dir / "aa-early.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if protocol["mode"] == "smoke":
        require(not entries, "smoke run unexpectedly contains A/A gate checkpoints")
        return
    require(
        [item.get("repetitions_complete") for item in entries] == list(EARLY_AA_GATE_REPETITIONS),
        "early A/A gate checkpoints are incomplete",
    )
    require(all(item.get("passed") is True for item in entries),
            "an early A/A gate failed but the matrix continued")
    require(all(item.get("workload") == EARLY_AA_GATE_WORKLOAD for item in entries),
            "early A/A gate ran on the wrong workload")


def verify_runtime_evidence(results_dir: Path, rows: list, protocol: dict) -> dict:
    by_index = {int(row["run_index"]): row for row in rows}
    max_capacity = 0.0
    saturated = []
    for index, row in by_index.items():
        server_log = results_dir / row["server_log"]
        require(server_log.is_file(), f"missing server log for run {index}")
        text = server_log.read_text(encoding="utf-8", errors="replace")
        require("database system is shut down" in text,
                f"run {index} has no clean-shutdown marker")
        init_log = results_dir / "logs" / f"server-{index}.log.initdb"
        require(init_log.is_file(), f"missing initdb log for run {index}")

        if row["workload"] in PGBENCH_WORKLOADS:
            load_path = results_dir / "client-load" / f"cell-{index}.json"
            require(load_path.is_file(), f"missing pgbench CPU sample for run {index}")
            load = json.loads(load_path.read_text(encoding="utf-8"))
            capacity = float(load["thread_capacity_fraction"])
            max_capacity = max(max_capacity, capacity)
            if capacity >= 0.90:
                saturated.append(index)
            require((results_dir / "logs" / f"pgbench-{index}.log").is_file(),
                    f"missing pgbench log for run {index}")

        if row["workload"] in {"W4", "W5", "W6c"}:
            for name in (f"server-setup-{index}.log", f"pgbench-init-{index}.log"):
                require((results_dir / "logs" / name).is_file(), f"missing setup log: {name}")

        if row["workload"] == "W1":
            detail_path = results_dir / "w1-detail" / f"cell-{index}.json"
            require(detail_path.is_file(), f"missing W1 per-function detail for run {index}")
            detail = json.loads(detail_path.read_text(encoding="utf-8"))
            require(set(detail) == set(W1_FUNCTIONS),
                    f"W1 detail for run {index} does not cover all five functions")
            for value in detail.values():
                require(isinstance(value, (int, float)) and math.isfinite(value) and value > 0,
                        f"W1 detail for run {index} has a non-positive/non-finite value")

        proof_expected = row["config"] in ACTIVE_CONFIGS
        proof_path = results_dir / "recording-proofs" / f"cell-{index}.csv"
        require(proof_path.is_file() == proof_expected,
                f"mode-proof presence mismatch for run {index}")
        if proof_expected:
            fields = (("timing_calls", "trace_records") if row["workload"] == "W1" else
                      ("client_count", "clients_recording", "timing_calls",
                       "representative_trace_records"))
            proof = read_csv(proof_path, fields)
            require(len(proof) == 1, f"mode proof {index} must contain one row")
            item = proof[0]
            trace_records = int(item["trace_records" if row["workload"] == "W1"
                                     else "representative_trace_records"])
            require(int(item["timing_calls"]) > 0, f"timing proof is empty for run {index}")
            if row["config"] in TRACE_CONFIGS:
                require(trace_records > 0, f"trace proof is empty for run {index}")
            else:
                require(trace_records == 0,
                        f"stats proof unexpectedly has trace rows at run {index}")

        qualification_expected = row["workload"] == "W3" and row["config"] in ACTIVE_CONFIGS
        summary_path = results_dir / "w3-qualification" / f"cell-{index}.json"
        raw_path = results_dir / "w3-qualification" / f"cell-{index}.tsv"
        require(summary_path.is_file() == qualification_expected
                and raw_path.is_file() == qualification_expected,
                f"W3 qualification presence mismatch for run {index}")
        if qualification_expected:
            qualification = json.loads(summary_path.read_text(encoding="utf-8"))
            recomputed = analyze_file(raw_path, float(qualification["snapshot_seconds"]))
            require(evidence_equal(qualification, recomputed),
                    f"W3 summary does not match raw evidence for run {index}")
            require(qualification.get("passed") is True,
                    f"W3 qualification failed for run {index}")

    for name in ("baseline-a", "baseline-b", "patched", "control"):
        path = results_dir / "pg-test-timing" / f"{name}.txt"
        require(path.is_file() and path.stat().st_size > 0,
                f"missing pg_test_timing capture for {name}")

    return {
        "max_pgbench_thread_capacity_fraction": max_capacity,
        "cells_at_or_above_90_percent_client_capacity": len(saturated),
    }


def metric_value(row: dict, workload: str) -> float:
    if workload == "W1":
        return float(row["ns_per_iteration"])
    return math.log(float(row["tps"]))


def statistical_analysis(rows: list, protocol: dict) -> dict:
    by_key = {(row["config"], row["workload"], int(row["repetition"])): row for row in rows}
    runs = int(protocol["runs_per_cell"])
    analyses = {}
    pgbench_margin = pgbench_margin_log(2.0)

    for workload in WORKLOADS:
        margin = 2.0 if workload == "W1" else pgbench_margin
        units = "ns/iteration" if workload == "W1" else "log(tps)"

        def paired_diffs(treatment: str, reference: str) -> list:
            return [
                metric_value(by_key[(treatment, workload, repetition)], workload)
                - metric_value(by_key[(reference, workload, repetition)], workload)
                for repetition in range(1, runs + 1)
            ]

        arms = {
            config: describe([
                metric_value(by_key[(config, workload, repetition)], workload)
                for repetition in range(1, runs + 1)
            ])
            for config in CONFIGS
        }

        contrasts = {}
        for label, treatment, reference in ALL_CONTRASTS:
            diffs = paired_diffs(treatment, reference)
            t_ci = confidence_interval(diffs)
            wilcoxon_result = wilcoxon_signed_rank(diffs).to_dict()
            classification = classify_contrast(
                t_ci["lower_95"], t_ci["upper_95"],
                wilcoxon_result["hl_lower"], wilcoxon_result["hl_upper"],
                margin,
            )
            contrasts[label] = {
                "treatment": treatment,
                "reference": reference,
                "t_interval": t_ci,
                "wilcoxon": wilcoxon_result,
                "margin": margin,
                "units": units,
                "classification": classification,
            }

        aa = contrasts["master-aa"]
        analyses[workload] = {
            "units": units,
            "margin": margin,
            "arms": arms,
            "contrasts": contrasts,
            "aa_resolution_floor": {
                "t_half_width": (aa["t_interval"]["upper_95"] - aa["t_interval"]["lower_95"]) / 2,
                "hl_half_width": (aa["wilcoxon"]["hl_upper"] - aa["wilcoxon"]["hl_lower"]) / 2,
            },
        }
    return analyses


def block_diagnostics(rows: list, protocol: dict) -> dict:
    """One row per cell, grouped by (workload, block): brief-v11-wpc-kit.md
    "Add a per-block diagnostics table: block index, order position of each
    configuration, TPS, mean frequency, NUMA local fraction, so plateaus are
    visible."""
    by_workload: dict[str, list] = defaultdict(list)
    for row in rows:
        metric = row["ns_per_iteration"] if row["workload"] == "W1" else row["tps"]
        by_workload[row["workload"]].append({
            "block": row["block"],
            "position": int(row["position"]),
            "config": row["config"],
            "metric": float(metric) if metric else None,
            "mean_freq_ghz": (
                float(row["cpu_freq_khz_mean"]) / 1e6 if row["cpu_freq_khz_mean"] else None
            ),
            "numa_local_fraction": (
                float(row["numa_local_fraction"]) if row["numa_local_fraction"] else None
            ),
        })
    for workload, entries in by_workload.items():
        entries.sort(key=lambda item: (item["block"], item["position"]))
    return by_workload


def wrap72(text: str) -> str:
    import textwrap
    return "\n".join(textwrap.wrap(text, width=72)) or text


def render_markdown(report: dict) -> str:
    lines = [
        "# Wait-event tracing v11 benchmark analysis",
        "",
        "Evidence completeness and integrity: **PASS**",
        "",
        wrap72(
            "Host isolation: "
            + report["host_environment"]["host_isolation"]
            + f" ({report['host_environment']['co_resident_postgres_process_count']} "
            "pre-existing PostgreSQL processes)."
        ),
        "",
        wrap72(
            "Plateau probe selected the "
            + report["plateau_probe"]["selected_variant"]
            + " dataset-clone/initdb variant (pinned spread "
            f"{report['plateau_probe']['pinned_spread_percent']:.3g}%, unpinned "
            f"spread {report['plateau_probe']['unpinned_spread_percent']:.3g}%)."
        ),
        "",
        wrap72(
            "Statistical labels use a paired 95% Student t interval AND an "
            "exact Wilcoxon signed-rank Hodges-Lehmann 95% interval. A "
            "contrast is 'equivalent' only if both intervals lie inside "
            "the margin, 'resolved' only if both exclude zero on the same "
            "side, and 'unresolved' otherwise -- so a single-mode plateau "
            "that would move the mean alone cannot be reported as a "
            "resolved difference."
        ),
        "",
    ]
    for workload in WORKLOADS:
        item = report["statistics"][workload]
        floor = item["aa_resolution_floor"]
        lines.extend([
            f"## {workload} ({item['units']}, margin +/-{item['margin']:.4g})",
            "",
            wrap72(
                "A/A resolution floor (master-aa vs master): "
                f"t half-width {floor['t_half_width']:.4g}, "
                f"Hodges-Lehmann half-width {floor['hl_half_width']:.4g}."
            ),
            "",
            "| Contrast | t mean | t 95% | HL est | HL 95% | Class |",
            "|---|---:|---:|---:|---:|---|",
        ])
        for label, contrast in item["contrasts"].items():
            t_ci = contrast["t_interval"]
            hl = contrast["wilcoxon"]
            lines.append(
                f"| {label} | {t_ci['mean']:.4g} | "
                f"[{t_ci['lower_95']:.4g},{t_ci['upper_95']:.4g}] | "
                f"{hl['hl_estimate']:.4g} | "
                f"[{hl['hl_lower']:.4g},{hl['hl_upper']:.4g}] | "
                f"{contrast['classification']} |"
            )
        lines.append("")

    lines.extend(["## Client driver", ""])
    load = report["client_load"]
    lines.append(wrap72(
        "Maximum sampled pgbench thread-capacity fraction: "
        f"{load['max_pgbench_thread_capacity_fraction']:.3f}. Cells at or "
        f"above 90%: {load['cells_at_or_above_90_percent_client_capacity']}."
    ))
    lines.append("")

    lines.extend(["## Per-block diagnostics", "", wrap72(
        "One row per measured cell, in schedule order, so a session-level "
        "throughput plateau shows up as a run of similar values at "
        "adjacent positions rather than being averaged away."
    ), ""])
    for workload in WORKLOADS:
        entries = report["block_diagnostics"].get(workload, [])
        lines.extend([
            f"### {workload}",
            "",
            "| Block | Pos | Config | Metric | Freq(GHz) | NUMA-local |",
            "|---|---:|---|---:|---:|---:|",
        ])
        for entry in entries:
            metric = f"{entry['metric']:.4g}" if entry["metric"] is not None else ""
            freq = f"{entry['mean_freq_ghz']:.3g}" if entry["mean_freq_ghz"] is not None else ""
            numa = (f"{entry['numa_local_fraction']:.3g}"
                    if entry["numa_local_fraction"] is not None else "")
            lines.append(
                f"| {entry['block']} | {entry['position']} | {entry['config']} | "
                f"{metric} | {freq} | {numa} |"
            )
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--output-json", type=Path)
    parser.add_argument("--output-markdown", type=Path)
    args = parser.parse_args()

    try:
        root = args.root.resolve()
        results_dir, build_dir, kit_dir = locate(root)
        protocol_path = results_dir / "protocol.json"
        require(protocol_path.is_file(), "protocol.json is missing")
        protocol = json.loads(protocol_path.read_text(encoding="utf-8"))
        verify_protocol(protocol)
        require(
            (results_dir / "seed.txt").read_text(encoding="utf-8").strip()
            == str(protocol["seed"]),
            "seed.txt disagrees with protocol",
        )

        manifest = verify_manifest(build_dir)
        manifest_path = build_dir / "manifest.json"
        require(protocol.get("build_manifest_sha256") == sha256(manifest_path),
                "build manifest is not the one bound when the matrix started")
        evidence_root = root
        if not (evidence_root / "host-check.json").is_file():
            evidence_root = results_dir.parent
        host_report = verify_host(evidence_root, protocol, manifest)
        plateau_probe = verify_plateau_probe(evidence_root)
        verify_bound_kit(protocol, kit_dir)
        rows = read_csv(results_dir / "results.csv", RESULT_FIELDS)
        verify_rows(rows, protocol)
        verify_schedule(results_dir, rows, protocol)
        verify_completion(results_dir, rows)
        verify_progress(results_dir, rows)
        verify_early_aa(results_dir, protocol)
        client_load = verify_runtime_evidence(results_dir, rows, protocol)
        statistics_report = statistical_analysis(rows, protocol)
        diagnostics = block_diagnostics(rows, protocol)

        report = {
            "valid": True,
            "integrity_valid": True,
            "host_environment": host_report,
            "plateau_probe": plateau_probe,
            "row_count": len(rows),
            "protocol": protocol,
            "build_manifest": manifest,
            "client_load": client_load,
            "statistics": statistics_report,
            "block_diagnostics": diagnostics,
        }
        output_json = args.output_json or results_dir / "analysis.json"
        output_md = args.output_markdown or results_dir / "analysis.md"
        output_json.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n",
                                encoding="utf-8")
        output_md.write_text(render_markdown(report), encoding="utf-8")
        print(f"verification: PASS ({len(rows)} complete cells)")
        print(f"analysis JSON: {output_json}")
        print(f"analysis report: {output_md}")
        return 0
    except (InvalidResults, KeyError, TypeError, ValueError, OSError,
            json.JSONDecodeError, QualificationError) as exc:
        print(f"verification: FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
