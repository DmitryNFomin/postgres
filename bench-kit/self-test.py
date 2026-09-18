#!/usr/bin/env python3
"""Exercise the 560-cell v11 verifier with synthetic, then tampered,
evidence. No PostgreSQL server is ever started by this script -- every
byte of evidence is generated data (brief-v11-wpc-kit.md hard rule)."""

from __future__ import annotations

import csv
import datetime
import hashlib
import json
import random
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

from benchmark_protocol import (
    ACTIVE_CONFIGS,
    BOUND_KIT_FILES,
    CONFIGS,
    EARLY_AA_GATE_REPETITIONS,
    EARLY_AA_GATE_WORKLOAD,
    PGBENCH_WORKLOADS,
    RESULT_FIELDS,
    RUNS,
    SHARED_BUFFERS_FOR_WORKLOAD,
    CLIENTS_FOR_WORKLOAD,
    TRACE_CONFIGS,
    W1_FUNCTIONS,
    WORKLOADS,
    pgbench_margin_log,
)
from latin_square import build_schedule
from stats_common import confidence_interval, classify_contrast, t_only_classification
from w3_qualification import analyze_file
from wilcoxon import wilcoxon_signed_rank

CSV_FIELDS = RESULT_FIELDS
BUILD_NAMES = ("baseline-a", "baseline-b", "patched", "control")


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path: Path, data: object) -> None:
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_analyzer(analyzer, root, *, expect_success, expected_error=None):
    process = subprocess.run(
        [sys.executable, str(analyzer), str(root),
         "--output-json", str(root / "analysis.json"),
         "--output-markdown", str(root / "analysis.md")],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    if (process.returncode == 0) != expect_success:
        raise RuntimeError(
            "unexpected analyzer result\n"
            f"stdout:\n{process.stdout}\nstderr:\n{process.stderr}"
        )
    if expected_error is not None and expected_error not in process.stderr:
        raise RuntimeError(
            f"analyzer did not report {expected_error!r}\n"
            f"stdout:\n{process.stdout}\nstderr:\n{process.stderr}"
        )
    return process


def run_collector(source_kit: Path, root: Path) -> None:
    """End-to-end exercise of 03-collect.sh and analyze-raw-archive.sh
    against the synthetic tree (still no PostgreSQL server anywhere)."""
    collector = root / "collector"
    shutil.copytree(root / "kit", collector)
    for name in ("self-test.py", "run-benchmark.sh", "README.md",
                 "BAREMETAL-RUNBOOK-v11.md"):
        source = source_kit / name
        if source.is_file():
            shutil.copyfile(source, collector / name)
    shutil.copytree(root / "results", collector / "results")
    shutil.copytree(root / "disassembly", collector / "disassembly")
    shutil.copytree(root / "build", collector / "work")
    shutil.copyfile(root / "host-check.txt", collector / "host-check.txt")
    shutil.copyfile(root / "host-check.json", collector / "host-check.json")
    shutil.copyfile(root / "plateau-probe-result.json",
                     collector / "plateau-probe-result.json")
    process = subprocess.run(
        ["bash", str(collector / "03-collect.sh")],
        cwd=collector, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    if process.returncode != 0:
        raise RuntimeError(
            f"synthetic collector failed\nstdout:\n{process.stdout}\nstderr:\n{process.stderr}"
        )
    archives = list(collector.glob("results-*.tar.gz"))
    if len(archives) != 1:
        raise RuntimeError("synthetic collector did not create one archive")
    sidecar = Path(str(archives[0]) + ".sha256")
    check = subprocess.run(
        ["sha256sum", "-c", sidecar.name], cwd=collector,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    if check.returncode != 0:
        raise RuntimeError(
            f"synthetic collector sidecar failed\nstdout:\n{check.stdout}\nstderr:\n{check.stderr}"
        )
    analysis_root = root / "local-analysis"
    analysis = subprocess.run(
        ["bash", str(source_kit / "analyze-raw-archive.sh"), str(archives[0]),
         str(analysis_root)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    if analysis.returncode != 0:
        raise RuntimeError(
            "synthetic local raw-archive analysis failed\n"
            f"stdout:\n{analysis.stdout}\nstderr:\n{analysis.stderr}"
        )
    report = json.loads((analysis_root / "analysis.json").read_text(encoding="utf-8"))
    if not report.get("valid"):
        raise RuntimeError("synthetic local raw-archive analysis was not valid")


def verify_runner_contract(source_kit: Path) -> None:
    for script in source_kit.glob("*.sh"):
        subprocess.run(["bash", "-n", str(script)], check=True)
    subprocess.run([str(source_kit / "wait-for-idle.sh"), "--self-test"],
                    check=True, stdout=subprocess.DEVNULL)
    runner = (source_kit / "02-run-matrix.sh").read_text(encoding="utf-8")
    match = re.search(r'^CSV_HEADER="([^"]+)"$', runner, re.MULTILINE)
    if match is None or tuple(match.group(1).split(",")) != RESULT_FIELDS:
        raise RuntimeError("runner CSV header differs from canonical schema")


def _module_sha(config_build: str) -> str:
    return "none" if config_build in ("baseline-a", "baseline-b") else (
        "8" * 64 if config_build == "patched" else "9" * 64
    )


def _binary_sha(config_build: str, binary: str) -> str:
    # baseline-a/baseline-b share every hash; patched/control differ from
    # each other on "postgres" (the layout-control contrast is specifically
    # about core postgres codegen) but share the rest for simplicity.
    base_digit = {"postgres": "1", "pgbench": "2", "psql": "3",
                  "initdb": "4", "pg_ctl": "5"}[binary]
    if config_build in ("baseline-a", "baseline-b"):
        return base_digit * 64
    if config_build == "control" and binary == "postgres":
        return "c" * 64
    return (base_digit + "a") * 32


def build_manifest_and_logs(build_dir: Path) -> dict:
    (build_dir / "build-logs").mkdir(parents=True, exist_ok=True)
    fixture_sha = "f" * 64
    builds = []
    for name in BUILD_NAMES:
        module_sha = _module_sha(name)
        binaries = {b: _binary_sha(name, b) for b in
                    ("postgres", "pgbench", "psql", "initdb", "pg_ctl")}
        sha256_map = dict(binaries)
        sha256_map["test_wait_primitive"] = fixture_sha
        sha256_map["pg_wait_event_tracing"] = module_sha
        install_tree = {
            f"bin/{binary}": {"type": "file", "sha256": value}
            for binary, value in binaries.items()
        }
        install_tree["lib/postgresql/test_wait_primitive.so"] = {
            "type": "file", "sha256": fixture_sha,
        }
        if module_sha != "none":
            install_tree["lib/postgresql/pg_wait_event_tracing.so"] = {
                "type": "file", "sha256": module_sha,
            }
        log_path = build_dir / "build-logs" / f"{name}.log"
        log_path.write_text(f"synthetic build log for {name}\n", encoding="utf-8")
        builds.append({
            "name": name,
            "commit": "0" * 40,
            "install_tree": install_tree,
            "fixture_tree": "e" * 40,
            "sha256": sha256_map,
        })
    manifest = {
        "schema_version": 11,
        "benchmark_series": "wet-v11",
        "build_host": "synthetic-host",
        "compiler_cache": "disabled",
        "build_system": "configure-make",
        "configure_flags": [
            "--disable-rpath", "--without-icu", "--without-readline", "--without-zlib",
        ],
        "builds": builds,
    }
    write_json(build_dir / "manifest.json", manifest)
    write_json(build_dir / "source-manifest.json", {
        "schema_version": 11, "benchmark_series": "wet-v11",
        "commits": {"master": "0" * 40, "patched": "1" * 40, "control": "2" * 40},
    })
    return manifest


def make_host_check(root: Path) -> None:
    (root / "host-check.txt").write_text("synthetic clean host\n", encoding="utf-8")
    write_json(root / "host-check.json", {
        "hostname": "synthetic-host",
        "warning_count": 0,
        "co_resident_postgres_process_count": 0,
        "host_isolation": "dedicated",
        "cpu_affinity_protocol": {
            "verified": True,
            "shared_physical_core": False,
            "server_cpus": "1-15",
            "pgbench_cpus": "16-23",
            "server_cpu_ids": list(range(1, 16)),
            "pgbench_cpu_ids": list(range(16, 24)),
            "server_numa_nodes": [0],
            "pgbench_numa_nodes": [0],
        },
    })


def make_plateau_probe(root: Path) -> None:
    write_json(root / "plateau-probe-result.json", {
        "server_numa_node": 0,
        "numactl_available": True,
        "pinned_tps": [980.0, 985.0, 990.0, 975.0],
        "unpinned_tps": [960.0, 1010.0, 940.0, 1020.0],
        "pinned_spread_percent": 0.6,
        "unpinned_spread_percent": 3.4,
        "selected_variant": "pinned",
    })


def make_disassembly(root: Path) -> None:
    """Synthetic stand-in for 01b-disassemble.sh's output, so the
    03-collect.sh exercise below (which now requires disassembly/) has
    something to stage without actually running objdump."""
    functions = (
        "WaitEventSetWait", "FileReadV", "LWLockAcquire", "XLogWrite",
        "SlruInternalWritePage", "CopyReadLine",
        "pgaio_io_perform_synchronously",
    )
    for name in BUILD_NAMES:
        build_dir = root / name
        build_dir.mkdir(parents=True, exist_ok=True)
        for func in functions:
            (build_dir / f"{func}.txt").write_text(
                f"synthetic disassembly of {func} for {name}\n",
                encoding="utf-8",
            )
    write_json(root / "PROVENANCE.json", {
        "gcc_version": "gcc (synthetic) 8.5.0",
        "objdump_version": "GNU objdump (synthetic) 2.30",
    })


def create_tree(source_kit: Path, root: Path, *, plateau: bool = False):
    """Build one complete synthetic 560-cell (7x5x16) full-mode evidence
    tree. If plateau=True, the trace-vs-master W6c contrast is seeded with
    a bimodal (7-high/9-low) pattern that a t-only rule would call resolved
    but the combined t+Wilcoxon/HL rule must call "unresolved" -- this is
    the synthetic plateau scenario brief-v11-wpc-kit.md asks for."""
    kit = root / "kit"
    build = root / "build"
    results = root / "results"
    disassembly = root / "disassembly"
    for path in (kit / "workloads", build, results / "logs",
                 results / "recording-proofs", results / "w3-qualification",
                 results / "client-load", results / "w1-detail",
                 results / "pg-test-timing"):
        path.mkdir(parents=True, exist_ok=True)
    make_disassembly(disassembly)

    for relative in BOUND_KIT_FILES:
        destination = kit / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_kit / relative, destination)

    manifest = build_manifest_and_logs(build)
    make_host_check(root)
    make_plateau_probe(root)
    for name in BUILD_NAMES:
        (results / "pg-test-timing" / f"{name}.txt").write_text(
            f"synthetic pg_test_timing for {name}\n", encoding="utf-8"
        )

    seed = 8675309
    (results / "seed.txt").write_text(f"{seed}\n", encoding="ascii")

    rng = random.Random(seed)
    per_workload_schedule = {
        workload: build_schedule(rng, CONFIGS, RUNS) for workload in WORKLOADS
    }
    schedule = []
    for repetition in range(1, RUNS + 1):
        workload_order = list(WORKLOADS)
        rng.shuffle(workload_order)
        for workload in workload_order:
            order = per_workload_schedule[workload][repetition - 1]
            block = f"{workload}-{repetition:02d}"
            for position, config in enumerate(order, 1):
                schedule.append((config, workload, repetition, block, position))
    with (results / "schedule.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(("run_index", "config", "workload", "repetition", "block", "position"))
        for i, (config, workload, repetition, block, position) in enumerate(schedule, 1):
            writer.writerow((i, config, workload, repetition, block, position))

    build_for = {name: name for name in ("control",)}
    build_for.update({"master": "baseline-a", "master-aa": "baseline-b",
                       "hook-null": "patched", "module-off": "patched",
                       "stats": "patched", "trace": "patched"})

    protocol = {
        "schema_version": 11,
        "benchmark_series": "wet-v11",
        "mode": "full",
        "seed": seed,
        "runs_per_cell": RUNS,
        "expected_cells": len(CONFIGS) * len(WORKLOADS) * RUNS,
        "duration_seconds": 30,
        "warmup_seconds": 10,
        "quiescence_seconds": 5,
        "pgbench_scale": 100,
        "w1_iterations": 100_000_000,
        "configs": list(CONFIGS),
        "workloads": list(WORKLOADS),
        "module_name": "pg_wait_event_tracing",
        "guc_capture": "pg_wait_event_tracing.capture",
        "guc_max_tranches": "pg_wait_event_tracing.max_tranches",
        "guc_trace_ring_size": "pg_wait_event_tracing.trace_ring_size",
        "server_cpus": "1-15",
        "pgbench_cpus": "16-23",
        "cpu_affinity_protocol": {
            "verified": True,
            "shared_physical_core": False,
            "server_cpus": "1-15",
            "pgbench_cpus": "16-23",
            "server_cpu_ids": list(range(1, 16)),
            "pgbench_cpu_ids": list(range(16, 24)),
        },
        "build_manifest_sha256": digest(build / "manifest.json"),
        "host_check_sha256": {
            name: digest(root / name) for name in ("host-check.txt", "host-check.json")
        },
        "bound_file_sha256": {relative: digest(kit / relative) for relative in BOUND_KIT_FILES},
        "analysis": {
            "early_aa_gate": {
                "check_after_repetitions": list(EARLY_AA_GATE_REPETITIONS),
                "workload": EARLY_AA_GATE_WORKLOAD,
                "max_half_width_percent": 1.0,
            },
        },
        "max_pgbench_thread_capacity_fraction": 0.90,
        "w3_qualification": __import__("benchmark_protocol").W3_PROTOCOL,
    }
    write_json(results / "protocol.json", protocol)

    base_tps = {
        "master": 1000.0, "master-aa": 1000.0, "control": 1000.0,
        "hook-null": 1000.0, "module-off": 999.0, "stats": 997.0, "trace": 995.0,
    }
    base_ns = {
        "master": 10.0, "master-aa": 10.0, "control": 10.0,
        "hook-null": 10.1, "module-off": 10.2, "stats": 10.6, "trace": 11.0,
    }

    # brief-v11-wpc-kit.md plateau scenario: 7 of 16 repetitions with a
    # large positive excursion and 9 with a small negative one, verified
    # in wilcoxon.py's own development to give a t interval that excludes
    # zero (t alone: "resolved") while the Hodges-Lehmann interval still
    # spans zero (combined rule: "unresolved").
    plateau_pattern = [6.0] * 7 + [-1.0] * 9
    rng.shuffle(plateau_pattern)

    jitter_rng = random.Random(seed + 1)
    rows = []
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    for run_index, (config, workload, repetition, block, position) in enumerate(schedule, 1):
        is_pgbench = workload != "W1"
        jitter = jitter_rng.uniform(-0.5, 0.5)
        if is_pgbench:
            tps = base_tps[config] + jitter
            if plateau and workload == "W6c" and config == "trace":
                master_tps = base_tps["master"] + jitter
                import math
                tps = master_tps * math.exp(plateau_pattern[repetition - 1])
            ns_per_iteration = ""
        else:
            tps = ""
            ns_per_iteration = base_ns[config] + jitter

        clients = CLIENTS_FOR_WORKLOAD.get(workload, "")
        row = {
            "run_index": run_index, "seed": seed, "timestamp_utc": now,
            "block": block, "position": position,
            "config": config, "build": build_for[config], "workload": workload,
            "repetition": repetition,
            "shared_buffers": SHARED_BUFFERS_FOR_WORKLOAD[workload],
            "clients": clients,
            "duration_s": 30 if is_pgbench else "",
            "warmup_s": 10 if is_pgbench else "",
            "iterations": 100_000_000 * len(W1_FUNCTIONS) if workload == "W1" else "",
            "ns_per_iteration": ns_per_iteration,
            "tps": tps,
            "latency_avg_ms": 1.0 if is_pgbench else "",
            "measurement_samples": 30 if is_pgbench else "",
            "measurement_interval_s": 30.0 if is_pgbench else "",
            "pgbench_cpu_percent": 100.0 if is_pgbench else "",
            "pgbench_cpu_capacity_fraction": 0.2 if is_pgbench else "",
            "cpu_freq_khz_mean": 3_000_000,
            "cpu_freq_khz_min": 2_900_000,
            "cpu_freq_khz_max": 3_100_000,
            "numa_local_fraction": 0.95,
            "meminfo_cached_kb_before": 1_000_000,
            "meminfo_cached_kb_after": 1_010_000,
            "load_average_before": 0.1,
            "timing_clock_source": "tsc",
            "server_cpus": "1-15",
            "pgbench_cpus": "16-23",
            "server_log": f"logs/server-{run_index}.log",
        }
        rows.append(row)

        (results / row["server_log"]).write_text(
            "database system is shut down\n", encoding="utf-8"
        )
        (results / "logs" / f"server-{run_index}.log.initdb").write_text(
            "initdb synthetic\n", encoding="utf-8"
        )
        if is_pgbench:
            write_json(results / "client-load" / f"cell-{run_index}.json", {
                "pid": 12345, "pgbench_cpu_percent": 100.0,
                "thread_count": 8, "thread_capacity_fraction": 0.2,
            })
            (results / "logs" / f"pgbench-{run_index}.log").write_text(
                "synthetic pgbench\n", encoding="utf-8"
            )
        if workload in ("W4", "W5", "W6c"):
            (results / "logs" / f"server-setup-{run_index}.log").write_text(
                "database system is shut down\n", encoding="utf-8"
            )
            (results / "logs" / f"pgbench-init-{run_index}.log").write_text(
                "synthetic init\n", encoding="utf-8"
            )
        if workload == "W1":
            write_json(results / "w1-detail" / f"cell-{run_index}.json", {
                fn: 5.0 + i for i, fn in enumerate(W1_FUNCTIONS)
            })
        if config in ACTIVE_CONFIGS:
            proof = results / "recording-proofs" / f"cell-{run_index}.csv"
            trace_records = 1 if config in TRACE_CONFIGS else 0
            if workload == "W1":
                proof.write_text(
                    f"timing_calls,trace_records\n100,{trace_records}\n", encoding="utf-8"
                )
            else:
                proof.write_text(
                    "client_count,clients_recording,timing_calls,"
                    f"representative_trace_records\n{clients},{clients},100,{trace_records}\n",
                    encoding="utf-8",
                )
        if workload == "W3" and config in ACTIVE_CONFIGS:
            raw = results / "w3-qualification" / f"cell-{run_index}.tsv"
            histogram = ["9000"] + ["0"] * 31
            raw.write_text(
                "LWLock\tProcArray\t9000\t9\t2\t" + ",".join(histogram) + "\n",
                encoding="utf-8",
            )
            write_json(results / "w3-qualification" / f"cell-{run_index}.json",
                       analyze_file(raw, 3.0))

    with (results / "results.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    with (results / "aa-early.jsonl").open("w", encoding="utf-8") as stream:
        for repetition in EARLY_AA_GATE_REPETITIONS:
            stream.write(json.dumps({
                "repetitions_complete": repetition,
                "workload": EARLY_AA_GATE_WORKLOAD,
                "half_width_percent": 0.3,
                "passed": True,
            }, sort_keys=True) + "\n")

    write_json(results / "progress.json", {
        "state": "complete", "mode": "full",
        "cells_completed": len(rows), "cells_total": len(rows),
    })
    completion_files = ("results.csv", "schedule.csv", "protocol.json",
                         "progress.json", "aa-early.jsonl")
    write_json(results / "matrix-complete.json", {
        "rows": len(rows),
        "sha256": {name: digest(results / name) for name in completion_files},
    })
    return kit, build, results


def resync_completion(results: Path) -> None:
    """Recompute matrix-complete.json after a tamper edits a bound file."""
    completion_files = ("results.csv", "schedule.csv", "protocol.json",
                         "progress.json", "aa-early.jsonl")
    completion = json.loads((results / "matrix-complete.json").read_text(encoding="utf-8"))
    completion["sha256"] = {name: digest(results / name) for name in completion_files}
    write_json(results / "matrix-complete.json", completion)


def test_plateau_scenario_unit() -> None:
    """Direct unit-level check of the exact numbers self-test injects into
    the trace/W6c contrast: t-only would call it resolved, the combined
    rule must call it unresolved."""
    diffs = [6.0] * 7 + [-1.0] * 9
    t_ci = confidence_interval(diffs)
    wilcoxon_result = wilcoxon_signed_rank(diffs)
    margin = pgbench_margin_log(2.0)
    t_only = t_only_classification(t_ci["lower_95"], t_ci["upper_95"], margin)
    combined = classify_contrast(
        t_ci["lower_95"], t_ci["upper_95"],
        wilcoxon_result.hl_lower, wilcoxon_result.hl_upper, margin,
    )
    if t_only != "resolved_positive":
        raise RuntimeError(
            f"plateau fixture is not a t-alone-resolves case: t_only={t_only}"
        )
    if combined != "unresolved":
        raise RuntimeError(
            f"combined rule did not stay unresolved on the plateau fixture: {combined}"
        )
    if not (wilcoxon_result.hl_lower < 0 < wilcoxon_result.hl_upper):
        raise RuntimeError("Hodges-Lehmann interval unexpectedly excludes zero")


def test_plateau_scenario_end_to_end(source_kit: Path, analyzer: Path) -> None:
    """Build a full synthetic tree with the plateau injected into the
    trace-vs-master W6c contrast and confirm analyze-results.py itself
    reports that contrast as "unresolved"."""
    with tempfile.TemporaryDirectory(prefix="wet-v11-plateau-") as temporary:
        root = Path(temporary)
        _, _, results = create_tree(source_kit, root, plateau=True)
        run_analyzer(analyzer, root, expect_success=True)
        report = json.loads((root / "analysis.json").read_text(encoding="utf-8"))
        contrast = report["statistics"]["W6c"]["contrasts"]["trace"]
        if contrast["classification"] != "unresolved":
            raise RuntimeError(
                "end-to-end plateau scenario did not classify as unresolved: "
                f"{contrast}"
            )
        t_only = t_only_classification(
            contrast["t_interval"]["lower_95"], contrast["t_interval"]["upper_95"],
            contrast["margin"],
        )
        if t_only not in ("resolved_positive", "resolved_negative"):
            raise RuntimeError(
                f"end-to-end plateau fixture's t interval is not itself resolved: {t_only}"
            )


def main() -> int:
    source_kit = Path(__file__).resolve().parent
    verify_runner_contract(source_kit)
    analyzer = source_kit / "analyze-results.py"

    test_plateau_scenario_unit()

    with tempfile.TemporaryDirectory(prefix="wet-v11-self-test-") as temporary:
        root = Path(temporary)
        kit, build, results = create_tree(source_kit, root)
        run_analyzer(analyzer, root, expect_success=True)
        baseline_report = json.loads((root / "analysis.json").read_text(encoding="utf-8"))
        if baseline_report["row_count"] != len(CONFIGS) * len(WORKLOADS) * RUNS:
            raise RuntimeError("baseline analysis row count is wrong")

        manifest_path = build / "manifest.json"
        host_path = root / "host-check.json"
        results_csv = results / "results.csv"
        protocol_path = results / "protocol.json"

        # --- tamper: CPU affinity masks overlap ---------------------------
        saved_protocol = protocol_path.read_bytes()
        protocol = json.loads(protocol_path.read_text(encoding="utf-8"))
        protocol["cpu_affinity_protocol"]["pgbench_cpu_ids"] = list(range(1, 16))
        write_json(protocol_path, protocol)
        resync_completion(results)
        run_analyzer(analyzer, root, expect_success=False,
                     expected_error="server/pgbench CPU sets overlap")
        protocol_path.write_bytes(saved_protocol)
        resync_completion(results)

        # --- tamper: host report affinity disagrees with protocol ---------
        saved_host = host_path.read_bytes()
        host = json.loads(host_path.read_text(encoding="utf-8"))
        host["cpu_affinity_protocol"]["server_cpus"] = "1-31"
        write_json(host_path, host)
        protocol = json.loads(protocol_path.read_text(encoding="utf-8"))
        protocol["host_check_sha256"]["host-check.json"] = digest(host_path)
        write_json(protocol_path, protocol)
        run_analyzer(analyzer, root, expect_success=False,
                     expected_error="host-check CPU masks differ")
        host_path.write_bytes(saved_host)
        protocol_path.write_bytes(saved_protocol)

        # --- tamper: results.csv edited without updating matrix-complete --
        saved_results_csv = results_csv.read_bytes()
        with results_csv.open(newline="", encoding="utf-8") as stream:
            rows = list(csv.DictReader(stream))
        rows[0]["tps"] = "999999" if rows[0]["tps"] else rows[0]["tps"]
        with results_csv.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        run_analyzer(analyzer, root, expect_success=False,
                     expected_error="matrix-complete hash mismatch")
        results_csv.write_bytes(saved_results_csv)
        resync_completion(results)

        # --- tamper: patched/control postgres binaries made identical -----
        saved_manifest = manifest_path.read_bytes()
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        by_name = {item["name"]: item for item in manifest["builds"]}
        by_name["control"]["sha256"]["postgres"] = by_name["patched"]["sha256"]["postgres"]
        write_json(manifest_path, manifest)
        run_analyzer(analyzer, root, expect_success=False,
                     expected_error="byte-identical")
        manifest_path.write_bytes(saved_manifest)

        # --- tamper: a recording proof is missing --------------------------
        proof_candidates = sorted((results / "recording-proofs").glob("cell-*.csv"))
        victim = proof_candidates[0]
        saved_proof = victim.read_bytes()
        victim.unlink()
        run_analyzer(analyzer, root, expect_success=False)
        victim.write_bytes(saved_proof)

        # --- tamper: bound kit-file set incomplete -------------------------
        protocol = json.loads(protocol_path.read_text(encoding="utf-8"))
        protocol["bound_file_sha256"].pop("02-run-matrix.sh")
        write_json(protocol_path, protocol)
        run_analyzer(analyzer, root, expect_success=False)
        protocol = json.loads(saved_protocol.decode("utf-8"))
        write_json(protocol_path, protocol)

        # --- tamper: forged W3 qualification summary ------------------------
        w3_summaries = sorted((results / "w3-qualification").glob("cell-*.json"))
        if w3_summaries:
            victim = w3_summaries[0]
            saved = victim.read_bytes()
            write_json(victim, {"passed": True})
            run_analyzer(analyzer, root, expect_success=False)
            victim.write_bytes(saved)

        # --- tamper: early A/A gate reports failure but matrix continued ---
        aa_path = results / "aa-early.jsonl"
        saved_aa = aa_path.read_bytes()
        entries = [json.loads(line) for line in saved_aa.decode("utf-8").splitlines()]
        entries[0]["passed"] = False
        with aa_path.open("w", encoding="utf-8") as stream:
            for entry in entries:
                stream.write(json.dumps(entry, sort_keys=True) + "\n")
        resync_completion(results)
        run_analyzer(analyzer, root, expect_success=False,
                     expected_error="early A/A gate failed")
        aa_path.write_bytes(saved_aa)
        resync_completion(results)

        run_analyzer(analyzer, root, expect_success=True)
        run_collector(source_kit, root)

    # --- the plateau scenario itself (both a unit check and end-to-end) ---
    test_plateau_scenario_end_to_end(source_kit, analyzer)

    print(
        "self-test: PASS "
        f"({len(CONFIGS) * len(WORKLOADS) * RUNS} valid synthetic cells and "
        "seven-configuration contrasts verified; CPU-affinity, results/"
        "matrix-complete, manifest, mode-proof, bound-kit-file, W3, and "
        "early-A/A-gate tampering rejected; synthetic plateau scenario "
        "confirmed 'unresolved' under the combined t + Wilcoxon/HL rule "
        "even though the t interval alone would call it resolved)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
