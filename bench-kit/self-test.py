#!/usr/bin/env python3
"""Exercise the 480-cell verifier with synthetic, then tampered, evidence."""

from __future__ import annotations

import csv
import datetime
import hashlib
import json
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
    RESULT_FIELDS,
    TRACE_CONFIGS,
    W3_PROTOCOL,
    WORKLOADS,
)
from w3_qualification import analyze_file


ACTIVE = ACTIVE_CONFIGS
CSV_FIELDS = RESULT_FIELDS


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path: Path, data: object) -> None:
    path.write_text(
        json.dumps(data, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def run_analyzer(analyzer: Path, root: Path, *, expect_success: bool) -> None:
    process = subprocess.run(
        [
            sys.executable,
            str(analyzer),
            str(root),
            "--output-json",
            str(root / "analysis.json"),
            "--output-markdown",
            str(root / "analysis.md"),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if (process.returncode == 0) != expect_success:
        raise RuntimeError(
            "unexpected analyzer result\n"
            f"stdout:\n{process.stdout}\nstderr:\n{process.stderr}"
        )


def run_collector(source_kit: Path, root: Path) -> None:
    collector = root / "collector"
    shutil.copytree(root / "kit", collector)
    for name in ("self-test.py", "run-benchmark.sh", "README.md"):
        shutil.copyfile(source_kit / name, collector / name)
    shutil.copytree(root / "results", collector / "results")
    shutil.copytree(root / "build", collector / "work")
    shutil.copyfile(root / "host-check.txt", collector / "host-check.txt")
    shutil.copyfile(root / "host-check.json", collector / "host-check.json")
    process = subprocess.run(
        ["bash", str(collector / "03-collect.sh")],
        cwd=collector,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if process.returncode != 0:
        raise RuntimeError(
            "synthetic collector failed\n"
            f"stdout:\n{process.stdout}\nstderr:\n{process.stderr}"
        )
    archives = list(collector.glob("results-*.tar.gz"))
    if len(archives) != 1:
        raise RuntimeError("synthetic collector did not create one archive")
    sidecar = Path(str(archives[0]) + ".sha256")
    check = subprocess.run(
        ["sha256sum", "-c", sidecar.name],
        cwd=collector,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if check.returncode != 0:
        raise RuntimeError(
            "synthetic collector sidecar failed\n"
            f"stdout:\n{check.stdout}\nstderr:\n{check.stderr}"
        )
    analysis_root = root / "local-analysis"
    analysis = subprocess.run(
        [
            "bash",
            str(source_kit / "analyze-raw-archive.sh"),
            str(archives[0]),
            str(analysis_root),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if analysis.returncode != 0:
        raise RuntimeError(
            "synthetic local raw-archive analysis failed\n"
            f"stdout:\n{analysis.stdout}\nstderr:\n{analysis.stderr}"
        )
    report = json.loads(
        (analysis_root / "analysis.json").read_text(encoding="utf-8")
    )
    if report["performance_suitability"]["status"] != "PASS":
        raise RuntimeError("synthetic local analysis did not pass")


def verify_runner_contract(source_kit: Path) -> None:
    for script in source_kit.glob("*.sh"):
        subprocess.run(["bash", "-n", str(script)], check=True)
    subprocess.run(
        [str(source_kit / "wait-for-idle.sh"), "--self-test"],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    runner = (source_kit / "02-run-matrix.sh").read_text(encoding="utf-8")
    match = re.search(r'^CSV_HEADER="([^"]+)"$', runner, re.MULTILINE)
    if match is None or tuple(match.group(1).split(",")) != RESULT_FIELDS:
        raise RuntimeError("runner CSV header differs from canonical schema")


def create_tree(source_kit: Path, root: Path) -> tuple[Path, Path, Path]:
    kit = root / "kit"
    build = root / "build"
    results = root / "results"
    for path in (
        kit / "workloads",
        build,
        results / "logs",
        results / "recording-proofs",
        results / "w3-qualification",
        results / "client-load",
    ):
        path.mkdir(parents=True, exist_ok=True)

    bound = BOUND_KIT_FILES
    for relative in bound:
        destination = kit / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_kit / relative, destination)

    fixture_hash = "1" * 64
    compiler = {"c": {"id": "synthetic", "version": "1"}}
    install_tree = {
        "bin/postgres": {"type": "file", "sha256": "1" * 64},
    }
    source_manifest = {
        "schema_version": 3,
        "benchmark_series": "wet-v10",
        "repo_url": "https://github.com/DmitryNFomin/postgres.git",
        "commits": {
            "baseline": "765efece39ba3fb04fdf20b1dadcd9ecea76fbc9",
            "v9": "40bffed8a92291c27a5d1956a5cd18dd3609f397",
            "v10": "c12783fbf86e8116526afe4566d58bf90c3478e0",
        },
        "common_base": "0c5d6269614e107d1d2d669f82f63f7e232b30c9",
        "source_date_epoch": 1789301160,
        "fixture_tree": "93dde50fc966a3ab01f4218010ec65548370d6d6",
        "reference": {
            "name": "null-hook-fast-path",
            "commit": "40bffed8a92291c27a5d1956a5cd18dd3609f397",
            "parent_commit": "d7b4584a901241258604eef1f03dfd6b3f1fa926",
            "branch_prediction_hint": "none",
        },
        "comparison": {
            "name": "inline-attachment-needed-guard",
            "reference_commit":
                "40bffed8a92291c27a5d1956a5cd18dd3609f397",
            "treatment_commit":
                "c12783fbf86e8116526afe4566d58bf90c3478e0",
            "treatment_parent_commit":
                "40bffed8a92291c27a5d1956a5cd18dd3609f397",
        },
        "archives": {
            "baseline": {
                "filename": "postgres-baseline.tar.gz",
                "sha256": "b" * 64,
            },
            "v9": {
                "filename": "postgres-v9.tar.gz",
                "sha256": "c" * 64,
            },
            "v10": {
                "filename": "postgres-v10.tar.gz",
                "sha256": "d" * 64,
            },
        },
    }
    write_json(build / "source-manifest.json", source_manifest)
    (build / "build-logs").mkdir()
    binary_hashes = {
        name: str(index) * 64
        for index, name in enumerate(
            (
                "postgres",
                "pgbench",
                "psql",
                "initdb",
                "pg_ctl",
                "test_wait_primitive",
            ),
            1,
        )
    }
    builds = []
    for name, commit, module in (
        (
            "baseline-a",
            "765efece39ba3fb04fdf20b1dadcd9ecea76fbc9",
            "none",
        ),
        (
            "baseline-b",
            "765efece39ba3fb04fdf20b1dadcd9ecea76fbc9",
            "none",
        ),
        (
            "v9",
            "40bffed8a92291c27a5d1956a5cd18dd3609f397",
            "8" * 64,
        ),
        (
            "v10",
            "c12783fbf86e8116526afe4566d58bf90c3478e0",
            "9" * 64,
        ),
    ):
        hashes = dict(binary_hashes)
        hashes["test_wait_primitive"] = fixture_hash
        hashes["pg_wait_event_tracing"] = module
        log_path = build / "build-logs" / f"{name}.log"
        compiler_path = build / "build-logs" / f"{name}-compilers.json"
        install_tree_path = (
            build / "build-logs" / f"{name}-install-tree.json"
        )
        log_path.write_text(
            f"synthetic build log for {name}\n", encoding="utf-8"
        )
        write_json(compiler_path, compiler)
        write_json(install_tree_path, install_tree)
        builds.append(
            {
                "name": name,
                "commit": commit,
                "compiler": compiler,
                "install_tree": install_tree,
                "fixture_tree": "93dde50fc966a3ab01f4218010ec65548370d6d6",
                "provenance_sha256": {
                    "build_log": digest(log_path),
                    "compiler_json": digest(compiler_path),
                    "install_tree_json": digest(install_tree_path),
                },
                "sha256": hashes,
            }
        )
    manifest = {
        "schema_version": 6,
        "benchmark_series": "wet-v10",
        "treatment": "inline-attachment-needed-guard",
        "build_host": "synthetic-host",
        "common_base": "0c5d6269614e107d1d2d669f82f63f7e232b30c9",
        "source_date_epoch": 1789301160,
        "compiler_cache": "disabled",
        "build_system": "configure-make",
        "make_version": "GNU Make 4.3",
        "configure_flags": [
            "--disable-rpath",
            "--without-icu",
            "--without-readline",
            "--without-zlib",
        ],
        "bundled_source": {
            "manifest_sha256": digest(build / "source-manifest.json"),
            "baseline_archive_sha256": "b" * 64,
            "v9_archive_sha256": "c" * 64,
            "v10_archive_sha256": "d" * 64,
        },
        "builds": builds,
    }
    write_json(build / "manifest.json", manifest)

    (root / "host-check.txt").write_text(
        "synthetic clean host\n", encoding="utf-8"
    )
    write_json(
        root / "host-check.json",
        {
            "hostname": "synthetic-host",
            "warning_count": 0,
            "co_resident_postgres_process_count": 0,
            "host_isolation": "dedicated",
        },
    )

    seed = 8675309
    (results / "seed.txt").write_text(f"{seed}\n", encoding="ascii")
    schedule = []
    index = 0
    for repetition in range(1, 13):
        for workload in WORKLOADS:
            for config in CONFIGS:
                index += 1
                schedule.append((index, config, workload, repetition))
    with (results / "schedule.csv").open(
        "w", newline="", encoding="utf-8"
    ) as stream:
        writer = csv.writer(stream)
        writer.writerow(("run_index", "config", "workload", "repetition"))
        writer.writerows(schedule)

    protocol = {
        "schema_version": 5,
        "benchmark_series": "wet-v10",
        "treatment": "inline-attachment-needed-guard",
        "mode": "full",
        "seed": seed,
        "runs_per_cell": 12,
        "expected_cells": 480,
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
        "server_cpus": None,
        "pgbench_cpus": None,
        "cpu_pinning": "unpinned (default)",
        "build_manifest_sha256": digest(build / "manifest.json"),
        "host_check_sha256": {
            name: digest(root / name)
            for name in ("host-check.txt", "host-check.json")
        },
        "bound_file_sha256": {
            relative: digest(kit / relative) for relative in bound
        },
        "analysis": {
            "primary_reference": "master",
            "primary_comparison": "v10 minus v9 for each capture mode",
            "pairing_key": "workload + repetition",
            "v10_v9_w1_contrast":
                "v10 minus v9, nanoseconds per iteration",
            "v10_v9_pgbench_contrast":
                "(v10 / v9 - 1) * 100 percent",
            "w1_equivalence_margin_ns": 2.0,
            "pgbench_equivalence_margin_percent": 2.0,
            "aa_stability": {
                "max_cv_percent_each_arm": 2.0,
                "max_absolute_paired_bias_percent": 2.0,
            },
            "early_aa_gate": {
                "check_after_repetitions": [1, 3, 6, 9],
                "max_absolute_single_pair_percent": 10.0,
                "max_cv_percent_each_arm": 5.0,
                "max_absolute_mean_paired_bias_percent": 5.0,
            },
        },
        "max_pgbench_thread_capacity_fraction": 0.90,
        "w3_qualification": W3_PROTOCOL,
    }
    write_json(results / "protocol.json", protocol)

    telemetry = []
    rows = []
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    for run_index, config, workload, repetition in schedule:
        build_name = {
            "master": "baseline-a",
            "master-aa": "baseline-b",
        }.get(config, config.split("-", 1)[0])
        is_pgbench = workload != "W1"
        clients = {"W3": 8, "W4": 16, "W6c": 32}.get(workload, "")
        row = {
            "run_index": run_index,
            "seed": seed,
            "timestamp_utc": now,
            "config": config,
            "build": build_name,
            "workload": workload,
            "repetition": repetition,
            "shared_buffers": {
                "W1": "128MB",
                "W3": "16MB",
                "W4": "4GB",
                "W6c": "32MB",
            }[workload],
            "clients": clients,
            "duration_s": 30 if is_pgbench else "",
            "warmup_s": 10 if is_pgbench else "",
            "iterations": 100_000_000 if workload == "W1" else "",
            "ns_per_iteration": 10.0 if workload == "W1" else "",
            "tps": 1000.0 if is_pgbench else "",
            "latency_avg_ms": 1.0 if is_pgbench else "",
            "measurement_samples": 30 if is_pgbench else "",
            "measurement_interval_s": 30.0 if is_pgbench else "",
            "pgbench_cpu_percent": 100.0 if is_pgbench else "",
            "pgbench_cpu_capacity_fraction": 0.125 if is_pgbench else "",
            "cpu_freq_khz_median_before": 3_000_000,
            "cpu_freq_khz_median_after": 3_000_000,
            "server_cpus": "",
            "pgbench_cpus": "",
            "server_log": f"logs/server-{run_index}.log",
        }
        rows.append(row)
        (results / row["server_log"]).write_text(
            "database system is shut down\n", encoding="utf-8"
        )
        (results / "logs" / f"server-{run_index}.log.initdb").write_text(
            "initdb synthetic\n", encoding="utf-8"
        )
        for phase in ("before", "after"):
            telemetry.append(
                {
                    "run_index": run_index,
                    "phase": phase,
                    "config": config,
                    "workload": workload,
                    "repetition": repetition,
                }
            )
        if is_pgbench:
            write_json(
                results / "client-load" / f"cell-{run_index}.json",
                {
                    "pid": 12345,
                    "pgbench_cpu_percent": 100.0,
                    "thread_count": 8,
                    "thread_capacity_fraction": 0.125,
                },
            )
            (results / "logs" / f"pgbench-{run_index}.log").write_text(
                "synthetic pgbench\n", encoding="utf-8"
            )
        if workload in ("W4", "W6c"):
            (
                results / "logs" / f"server-setup-{run_index}.log"
            ).write_text("database system is shut down\n", encoding="utf-8")
            (
                results / "logs" / f"pgbench-init-{run_index}.log"
            ).write_text("synthetic init\n", encoding="utf-8")
        if config in ACTIVE:
            proof = results / "recording-proofs" / f"cell-{run_index}.csv"
            if workload == "W1":
                proof.write_text(
                    "timing_calls,trace_records\n"
                    f"100,{1 if config in TRACE_CONFIGS else 0}\n",
                    encoding="utf-8",
                )
            else:
                proof.write_text(
                    "client_count,clients_recording,timing_calls,"
                    "representative_trace_records\n"
                    f"{clients},{clients},100,"
                    f"{1 if config in TRACE_CONFIGS else 0}\n",
                    encoding="utf-8",
                )
        if workload == "W3" and config in ACTIVE:
            raw = results / "w3-qualification" / f"cell-{run_index}.tsv"
            histogram = ["9000"] + ["0"] * 31
            raw.write_text(
                "LWLock\tProcArray\t9000\t9\t2\t"
                + ",".join(histogram)
                + "\n",
                encoding="utf-8",
            )
            write_json(
                results / "w3-qualification" / f"cell-{run_index}.json",
                analyze_file(raw, 3.0),
            )

    with (results / "results.csv").open(
        "w", newline="", encoding="utf-8"
    ) as stream:
        writer = csv.DictWriter(stream, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    with (results / "telemetry.jsonl").open(
        "w", encoding="utf-8"
    ) as stream:
        for item in telemetry:
            stream.write(json.dumps(item, sort_keys=True) + "\n")
    with (results / "aa-early.jsonl").open(
        "w", encoding="utf-8"
    ) as stream:
        for repetition in (1, 3, 6, 9):
            stream.write(
                json.dumps(
                    {"repetitions_complete": repetition, "passed": True},
                    sort_keys=True,
                )
                + "\n"
            )
    write_json(
        results / "progress.json",
        {
            "state": "complete",
            "mode": "full",
            "cells_completed": 480,
            "cells_total": 480,
        },
    )
    completion_files = (
        "results.csv",
        "schedule.csv",
        "telemetry.jsonl",
        "protocol.json",
        "progress.json",
        "aa-early.jsonl",
    )
    write_json(
        results / "matrix-complete.json",
        {
            "rows": 480,
            "sha256": {
                name: digest(results / name) for name in completion_files
            },
        },
    )
    return kit, build, results


def main() -> int:
    source_kit = Path(__file__).resolve().parent
    verify_runner_contract(source_kit)
    analyzer = source_kit / "analyze-results.py"
    with tempfile.TemporaryDirectory(prefix="wet-v10-self-test-") as temporary:
        root = Path(temporary)
        _, _, results = create_tree(source_kit, root)
        run_analyzer(analyzer, root, expect_success=True)

        host_path = root / "host-check.json"
        protocol_path = results / "protocol.json"
        completion_path = results / "matrix-complete.json"
        saved_host = host_path.read_bytes()
        saved_protocol = protocol_path.read_bytes()
        saved_completion = completion_path.read_bytes()
        host = json.loads(host_path.read_text(encoding="utf-8"))
        host["co_resident_postgres_process_count"] = 30
        host["host_isolation"] = "co-resident"
        write_json(host_path, host)
        protocol = json.loads(protocol_path.read_text(encoding="utf-8"))
        protocol["host_check_sha256"]["host-check.json"] = digest(host_path)
        write_json(protocol_path, protocol)
        completion = json.loads(
            completion_path.read_text(encoding="utf-8")
        )
        completion["sha256"]["protocol.json"] = digest(protocol_path)
        write_json(completion_path, completion)
        run_analyzer(analyzer, root, expect_success=True)
        report = json.loads(
            (root / "analysis.json").read_text(encoding="utf-8")
        )
        if report["performance_suitability"]["status"] != "PASS_WITH_CAVEAT":
            raise RuntimeError("co-resident host caveat was not reported")
        host_path.write_bytes(saved_host)
        protocol_path.write_bytes(saved_protocol)
        completion_path.write_bytes(saved_completion)

        proof = results / "recording-proofs" / "cell-5.csv"
        saved = proof.read_bytes()
        proof.unlink()
        run_analyzer(analyzer, root, expect_success=False)
        proof.write_bytes(saved)

        protocol = json.loads(protocol_path.read_text(encoding="utf-8"))
        protocol["bound_file_sha256"].pop("02-run-matrix.sh")
        write_json(protocol_path, protocol)
        run_analyzer(analyzer, root, expect_success=False)
        protocol["bound_file_sha256"]["02-run-matrix.sh"] = digest(
            root / "kit" / "02-run-matrix.sh"
        )
        write_json(protocol_path, protocol)

        summary = results / "w3-qualification" / "cell-15.json"
        saved = summary.read_bytes()
        write_json(summary, {"passed": True, "checks": {}})
        run_analyzer(analyzer, root, expect_success=False)
        summary.write_bytes(saved)

        run_collector(source_kit, root)

        results_csv = results / "results.csv"
        original_results = results_csv.read_bytes()
        with results_csv.open(newline="", encoding="utf-8") as stream:
            rows = list(csv.DictReader(stream))
        for row in rows:
            if row["config"] == "master-aa" and row["workload"] == "W4":
                row["tps"] = "1100"
        with results_csv.open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=CSV_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        completion = json.loads(
            completion_path.read_text(encoding="utf-8")
        )
        completion["sha256"]["results.csv"] = digest(results_csv)
        write_json(completion_path, completion)
        run_analyzer(analyzer, root, expect_success=False)
        results_csv.write_bytes(original_results)

    print(
        "self-test: PASS "
        "(480 valid cells and direct v10/v9 contrasts verified; "
        "co-resident caveat accepted; "
        "missing proof, incomplete binding, "
        "forged W3 summary, and failed A/A suitability rejected; "
        "raw collection and local clean-room analysis passed)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
