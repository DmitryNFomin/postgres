#!/usr/bin/env python3
"""Verify and summarize a v10 wait-event benchmark results tree."""

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

from cpu_affinity import validate_host_report
from benchmark_protocol import (
    ACTIVE_CONFIGS,
    BOUND_KIT_FILES,
    CONFIGS,
    CPU_PINNING,
    FULL_PROFILE,
    PGBENCH_CPUS,
    PGBENCH_WORKLOADS,
    RESULT_FIELDS,
    SMOKE_PROFILE,
    SERVER_CPUS,
    TRACE_CONFIGS,
    V10_V9_PAIRS,
    W3_PROTOCOL,
    WORKLOADS,
)
from w3_qualification import QualificationError, analyze_file


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
        return math.isclose(
            float(left),
            float(right),
            rel_tol=1e-14,
            abs_tol=1e-15,
        )
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


def t95(df: int) -> float:
    values = (
        None, 12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365,
        2.306, 2.262, 2.228, 2.201, 2.179, 2.160, 2.145, 2.131,
        2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069,
        2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042,
    )
    return values[df] if df < len(values) else 1.96


def confidence_interval(values: list[float]) -> dict[str, float | int]:
    require(bool(values), "cannot calculate an interval over no values")
    mean = statistics.mean(values)
    if len(values) == 1:
        lower = upper = mean
    else:
        half = t95(len(values) - 1) * statistics.stdev(values)
        half /= math.sqrt(len(values))
        lower, upper = mean - half, mean + half
    return {
        "n": len(values),
        "mean": mean,
        "lower_95": lower,
        "upper_95": upper,
    }


def describe(values: list[float]) -> dict[str, float | int]:
    mean = statistics.mean(values)
    stdev = statistics.stdev(values) if len(values) > 1 else 0.0
    return {
        "n": len(values),
        "mean": mean,
        "median": statistics.median(values),
        "stdev": stdev,
        "cv_percent": stdev / mean * 100 if mean else float("inf"),
        "min": min(values),
        "max": max(values),
    }


def classify_interval(interval: dict[str, float | int], margin: float) -> str:
    lower = float(interval["lower_95"])
    upper = float(interval["upper_95"])
    if lower >= -margin and upper <= margin:
        return "equivalent_within_predeclared_margin"
    if lower > 0:
        return "statistically_resolved_positive_difference"
    if upper < 0:
        return "statistically_resolved_negative_difference"
    return "no_statistically_resolved_difference"


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


def read_csv(
    path: Path,
    expected_fields: tuple[str, ...] | None = None,
) -> list[dict[str, str]]:
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
    require(manifest.get("schema_version") == 6,
            "unsupported build manifest schema")
    require(
        manifest.get("benchmark_series") == "wet-v10"
        and manifest.get("treatment") == "inline-attachment-needed-guard",
        "unexpected build benchmark series or treatment",
    )
    require(manifest.get("compiler_cache") == "disabled",
            "build manifest does not prove compiler caches were disabled")
    require(
        manifest.get("common_base")
        == "0c5d6269614e107d1d2d669f82f63f7e232b30c9",
        "unexpected baseline/v9/v10 common base",
    )
    require(manifest.get("source_date_epoch") == 1789301160,
            "unexpected reproducibility epoch")
    require(manifest.get("build_system") == "configure-make",
            "build did not use the required configure/make backend")
    require(
        isinstance(manifest.get("make_version"), str)
        and manifest["make_version"].startswith("GNU Make "),
        "build manifest lacks the GNU Make version",
    )
    require(
        manifest.get("configure_flags")
        == [
            "--disable-rpath",
            "--without-icu",
            "--without-readline",
            "--without-zlib",
        ],
        "build flags differ from the fixed protocol",
    )
    bundled = manifest.get("bundled_source")
    require(
        isinstance(bundled, dict)
        and set(bundled)
        == {
            "manifest_sha256",
            "baseline_archive_sha256",
            "v9_archive_sha256",
            "v10_archive_sha256",
        },
        "build manifest lacks bundled-source provenance",
    )
    for name, digest in bundled.items():
        require(
            isinstance(digest, str)
            and len(digest) == 64
            and all(char in "0123456789abcdef" for char in digest),
            f"invalid bundled-source hash: {name}",
        )
    source_manifest_path = build_dir / "source-manifest.json"
    require(
        source_manifest_path.is_file() and not source_manifest_path.is_symlink(),
        "archived build provenance lacks source-manifest.json",
    )
    require(
        sha256(source_manifest_path) == bundled["manifest_sha256"],
        "source manifest hash differs from build manifest",
    )
    source_manifest = json.loads(
        source_manifest_path.read_text(encoding="utf-8")
    )
    require(source_manifest.get("schema_version") == 3,
            "unsupported source manifest schema")
    require(
        source_manifest.get("benchmark_series") == "wet-v10",
        "unexpected source benchmark series",
    )
    require(
        source_manifest.get("repo_url")
        == "https://github.com/DmitryNFomin/postgres.git",
        "source manifest repository differs",
    )
    require(
        source_manifest.get("commits")
        == {
            "baseline": "765efece39ba3fb04fdf20b1dadcd9ecea76fbc9",
            "v9": "40bffed8a92291c27a5d1956a5cd18dd3609f397",
            "v10": "c12783fbf86e8116526afe4566d58bf90c3478e0",
        },
        "source manifest commits differ",
    )
    require(
        source_manifest.get("common_base")
        == "0c5d6269614e107d1d2d669f82f63f7e232b30c9"
        and source_manifest.get("source_date_epoch") == 1789301160
        and source_manifest.get("fixture_tree")
        == "93dde50fc966a3ab01f4218010ec65548370d6d6",
        "source manifest ancestry or fixture identity differs",
    )
    require(
        source_manifest.get("reference")
        == {
            "name": "null-hook-fast-path",
            "commit": "40bffed8a92291c27a5d1956a5cd18dd3609f397",
            "parent_commit": "d7b4584a901241258604eef1f03dfd6b3f1fa926",
            "branch_prediction_hint": "none",
        },
        "source manifest v9 reference metadata differs",
    )
    require(
        source_manifest.get("comparison")
        == {
            "name": "inline-attachment-needed-guard",
            "reference_commit":
                "40bffed8a92291c27a5d1956a5cd18dd3609f397",
            "treatment_commit":
                "c12783fbf86e8116526afe4566d58bf90c3478e0",
            "treatment_parent_commit":
                "40bffed8a92291c27a5d1956a5cd18dd3609f397",
        },
        "source manifest v10 comparison metadata differs",
    )
    require(
        set(source_manifest.get("archives", {}))
        == {"baseline", "v9", "v10"}
        and source_manifest["archives"]["baseline"].get("filename")
        == "postgres-baseline.tar.gz"
        and source_manifest["archives"]["v9"].get("filename")
        == "postgres-v9.tar.gz"
        and source_manifest["archives"]["v10"].get("filename")
        == "postgres-v10.tar.gz"
        and source_manifest.get("archives", {}).get(
            "baseline", {}
        ).get("sha256")
        == bundled["baseline_archive_sha256"]
        and source_manifest.get("archives", {}).get("v9", {}).get(
            "sha256"
        )
        == bundled["v9_archive_sha256"]
        and source_manifest.get("archives", {}).get("v10", {}).get(
            "sha256"
        )
        == bundled["v10_archive_sha256"],
        "source archive hashes differ between manifests",
    )
    build_logs = build_dir / "build-logs"
    require(build_logs.is_dir() and not build_logs.is_symlink(),
            "archived build provenance lacks build logs")
    for build_name in ("baseline-a", "baseline-b", "v9", "v10"):
        for suffix in (".log", "-compilers.json", "-install-tree.json"):
            log_path = build_logs / f"{build_name}{suffix}"
            require(
                log_path.is_file()
                and not log_path.is_symlink()
                and log_path.stat().st_size > 0,
                f"missing build provenance file: {log_path.name}",
            )
    builds = {item["name"]: item for item in manifest.get("builds", [])}
    require(set(builds) == {"baseline-a", "baseline-b", "v9", "v10"},
            f"unexpected build set: {sorted(builds)}")
    for name, build in builds.items():
        compiler_path = build_logs / f"{name}-compilers.json"
        install_tree_path = build_logs / f"{name}-install-tree.json"
        log_path = build_logs / f"{name}.log"
        require(
            json.loads(compiler_path.read_text(encoding="utf-8"))
            == build.get("compiler"),
            f"compiler metadata mismatch for {name}",
        )
        require(
            json.loads(install_tree_path.read_text(encoding="utf-8"))
            == build.get("install_tree"),
            f"installed file-tree metadata mismatch for {name}",
        )
        require(
            build.get("provenance_sha256")
            == {
                "build_log": sha256(log_path),
                "compiler_json": sha256(compiler_path),
                "install_tree_json": sha256(install_tree_path),
            },
            f"build provenance hash mismatch for {name}",
        )
        summaries = build.get("sha256")
        install_tree = build.get("install_tree")
        require(
            isinstance(summaries, dict) and isinstance(install_tree, dict),
            f"invalid binary or install-tree summary for {name}",
        )
        for binary in ("postgres", "pgbench", "psql", "initdb", "pg_ctl"):
            entry = install_tree.get(f"bin/{binary}")
            require(
                isinstance(entry, dict)
                and entry.get("type") == "file"
                and entry.get("sha256") == summaries.get(binary),
                f"{name}/{binary} summary differs from install tree",
            )
        for library in ("test_wait_primitive", "pg_wait_event_tracing"):
            candidates = [
                item
                for relative, item in install_tree.items()
                if relative.startswith("lib/")
                and Path(relative).name.startswith(f"{library}.")
                and isinstance(item, dict)
                and item.get("type") == "file"
            ]
            summary = summaries.get(library)
            if summary == "none":
                require(
                    not candidates,
                    f"{name}/{library} is omitted from its binary summary",
                )
            else:
                require(
                    len(candidates) == 1
                    and candidates[0].get("sha256") == summary,
                    f"{name}/{library} summary differs from install tree",
                )
    expected_commits = {
        "baseline-a": "765efece39ba3fb04fdf20b1dadcd9ecea76fbc9",
        "baseline-b": "765efece39ba3fb04fdf20b1dadcd9ecea76fbc9",
        "v9": "40bffed8a92291c27a5d1956a5cd18dd3609f397",
        "v10": "c12783fbf86e8116526afe4566d58bf90c3478e0",
    }
    for name, expected in expected_commits.items():
        require(builds[name]["commit"] == expected,
                f"{name} commit is not pinned value")
    binaries = ("postgres", "pgbench", "psql", "initdb", "pg_ctl",
                "test_wait_primitive")
    for binary in binaries:
        require(
            builds["baseline-a"]["sha256"][binary]
            == builds["baseline-b"]["sha256"][binary],
            f"independent baseline builds differ for {binary}",
        )
    require(
        builds["baseline-a"].get("install_tree")
        == builds["baseline-b"].get("install_tree"),
        "independent baseline installation trees differ",
    )
    require(len({
        item["fixture_tree"] for item in builds.values()
    }) == 1, "source fixture tree differs across build records")
    for binary in binaries:
        require(
            builds["v9"]["sha256"][binary]
            == builds["v10"]["sha256"][binary],
            f"v9/v10 builds differ unexpectedly for {binary}",
        )
    def normalize_tracing_module(tree: dict) -> dict:
        normalized = {}
        for relative, value in tree.items():
            item = dict(value)
            if (
                relative.startswith("lib/")
                and Path(relative).name.startswith(
                    "pg_wait_event_tracing."
                )
                and item.get("type") == "file"
            ):
                item["sha256"] = "<tracing-module>"
            normalized[relative] = item
        return normalized

    require(
        normalize_tracing_module(builds["v9"]["install_tree"])
        == normalize_tracing_module(builds["v10"]["install_tree"]),
        "v9/v10 installation trees differ outside the tracing module",
    )
    for name in ("v9", "v10"):
        require(
            builds[name]["sha256"]["pg_wait_event_tracing"] != "none",
            f"{name} build lacks pg_wait_event_tracing",
        )
    require(
        builds["v9"]["sha256"]["pg_wait_event_tracing"]
        != builds["v10"]["sha256"]["pg_wait_event_tracing"],
        "v9 and v10 tracing modules are byte-identical",
    )
    for name in ("baseline-a", "baseline-b"):
        require(builds[name]["sha256"]["pg_wait_event_tracing"] == "none",
                f"{name} unexpectedly contains pg_wait_event_tracing")
    return manifest


def verify_protocol(protocol: dict) -> None:
    require(protocol.get("schema_version") == 6,
            "unsupported protocol schema")
    require(
        protocol.get("benchmark_series") == "wet-v10"
        and protocol.get("treatment") == "inline-attachment-needed-guard",
        "unexpected protocol benchmark series or treatment",
    )
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
    require(protocol.get("guc_capture") == "pg_wait_event_tracing.capture",
            "protocol capture GUC differs")
    require(
        protocol.get("guc_max_tranches")
        == "pg_wait_event_tracing.max_tranches",
        "protocol tranche GUC differs",
    )
    require(
        protocol.get("guc_trace_ring_size")
        == "pg_wait_event_tracing.trace_ring_size",
        "protocol trace-ring GUC differs",
    )
    require(
        protocol.get("server_cpus") == SERVER_CPUS
        and protocol.get("pgbench_cpus") == PGBENCH_CPUS
        and protocol.get("cpu_pinning") == CPU_PINNING,
        "CPU affinity differs from the fixed protocol",
    )
    analysis = protocol.get("analysis", {})
    require(
        analysis.get("primary_reference") == "master"
        and analysis.get("primary_comparison")
        == "v10 minus v9 for each capture mode"
        and analysis.get("pairing_key") == "workload + repetition"
        and analysis.get("v10_v9_w1_contrast")
        == "v10 minus v9, nanoseconds per iteration"
        and analysis.get("v10_v9_pgbench_contrast")
        == "(v10 / v9 - 1) * 100 percent",
        "direct v10/v9 analysis protocol differs",
    )
    require(analysis.get("w1_equivalence_margin_ns") == 2.0,
            "W1 equivalence margin differs from the fixed protocol")
    require(
        analysis.get("pgbench_equivalence_margin_percent") == 2.0,
        "pgbench equivalence margin differs from the fixed protocol",
    )
    stability = analysis.get("aa_stability", {})
    require(stability.get("max_cv_percent_each_arm") == 2.0,
            "A/A CV limit differs from the fixed protocol")
    require(
        stability.get("max_absolute_paired_bias_percent") == 2.0,
        "A/A paired-bias limit differs from the fixed protocol",
    )
    require(
        analysis.get("early_aa_gate")
        == {
            "check_after_repetitions": [1, 3, 6, 9],
            "max_absolute_single_pair_percent": 10.0,
            "max_cv_percent_each_arm": 5.0,
            "max_absolute_mean_paired_bias_percent": 5.0,
        },
        "early A/A gate differs from the fixed protocol",
    )
    require(protocol.get("max_pgbench_thread_capacity_fraction") == 0.90,
            "pgbench saturation limit differs from the fixed protocol")
    require(protocol.get("w3_qualification") == W3_PROTOCOL,
            "W3 thresholds differ from the fixed protocol")


def verify_host(root: Path, protocol: dict, manifest: dict) -> dict:
    hashes = protocol.get("host_check_sha256")
    require(
        isinstance(hashes, dict)
        and set(hashes) == {"host-check.txt", "host-check.json"},
        "protocol lacks the exact host-check hash set",
    )
    for name, expected in hashes.items():
        path = root / name
        require(path.is_file() and not path.is_symlink(),
                f"missing regular host report: {name}")
        require(sha256(path) == expected, f"host report hash mismatch: {name}")
    report = json.loads((root / "host-check.json").read_text(encoding="utf-8"))
    try:
        validate_host_report(report)
    except ValueError as error:
        raise InvalidResults(str(error)) from error
    require(report.get("hostname") == manifest.get("build_host"),
            "host check and build manifest are from different hosts")
    process_count = report.get("co_resident_postgres_process_count")
    require(isinstance(process_count, int) and process_count >= 0,
            "host report lacks a valid PostgreSQL process count")
    expected_isolation = "co-resident" if process_count else "dedicated"
    require(report.get("host_isolation") == expected_isolation,
            "host isolation label disagrees with PostgreSQL process count")
    return report


def verify_bound_kit(protocol: dict, kit_dir: Path) -> None:
    bound = protocol.get("bound_file_sha256")
    require(isinstance(bound, dict), "protocol has no bound kit-file hashes")
    require(set(bound) == set(BOUND_KIT_FILES),
            "protocol does not bind the exact required kit-file set")
    root = kit_dir.resolve()
    for relative, expected in bound.items():
        candidate = Path(relative)
        require(
            not candidate.is_absolute()
            and relative == candidate.as_posix()
            and ".." not in candidate.parts,
            f"unsafe bound kit path: {relative}",
        )
        path = kit_dir / relative
        try:
            path.resolve().relative_to(root)
            inside_root = True
        except ValueError:
            inside_root = False
        require(
            path.is_file()
            and not path.is_symlink()
            and inside_root,
            f"archive omits regular bound kit file: {relative}",
        )
        require(
            isinstance(expected, str)
            and len(expected) == 64
            and all(char in "0123456789abcdef" for char in expected),
            f"invalid bound kit hash: {relative}",
        )
        require(sha256(path) == expected,
                f"bound kit file hash mismatch: {relative}")


def verify_schedule(
    results_dir: Path,
    rows: list[dict[str, str]],
    protocol: dict,
) -> list[tuple[int, str, str, int]]:
    schedule_rows = read_csv(
        results_dir / "schedule.csv",
        ("run_index", "config", "workload", "repetition"),
    )
    expected_count = int(protocol["expected_cells"])
    require(len(schedule_rows) == expected_count,
            f"schedule has {len(schedule_rows)} rows, expected {expected_count}")
    schedule: list[tuple[int, str, str, int]] = []
    for position, row in enumerate(schedule_rows, 1):
        item = (
            int(row["run_index"]),
            row["config"],
            row["workload"],
            int(row["repetition"]),
        )
        require(item[0] == position,
                f"schedule run_index is not sequential at row {position}")
        require(item[1] in CONFIGS, f"unknown schedule config: {item[1]}")
        require(item[2] in WORKLOADS, f"unknown schedule workload: {item[2]}")
        schedule.append(item)

    require(len(rows) == expected_count,
            f"results has {len(rows)} rows, expected {expected_count}")
    observed = [
        (int(row["run_index"]), row["config"], row["workload"],
         int(row["repetition"]))
        for row in rows
    ]
    require(observed == schedule,
            "results rows do not match the recorded schedule exactly")

    block_size = len(CONFIGS)
    blocks_by_repetition: dict[int, list[str]] = defaultdict(list)
    for start in range(0, len(schedule), block_size):
        block = schedule[start:start + block_size]
        require(len(block) == block_size, "truncated schedule block")
        workloads = {item[2] for item in block}
        repetitions = {item[3] for item in block}
        require(len(workloads) == 1 and len(repetitions) == 1,
                f"mixed workload/repetition block at schedule row {start + 1}")
        require({item[1] for item in block} == set(CONFIGS),
                f"incomplete configuration block at schedule row {start + 1}")
        blocks_by_repetition[block[0][3]].append(block[0][2])
    for repetition in range(1, int(protocol["runs_per_cell"]) + 1):
        require(set(blocks_by_repetition[repetition]) == set(WORKLOADS),
                f"repetition {repetition} lacks a workload block")
    return schedule


def verify_rows(rows: list[dict[str, str]], protocol: dict) -> None:
    expected_keys = {
        (config, workload, repetition)
        for config in CONFIGS
        for workload in WORKLOADS
        for repetition in range(1, int(protocol["runs_per_cell"]) + 1)
    }
    actual_keys = {
        (row["config"], row["workload"], int(row["repetition"]))
        for row in rows
    }
    require(len(actual_keys) == len(rows), "duplicate result cell")
    require(actual_keys == expected_keys, "result matrix is incomplete")
    expected_seed = str(protocol["seed"])
    build_for = {
        "master": "baseline-a",
        "master-aa": "baseline-b",
        "v9-hook-null": "v9",
        "v9-module-off": "v9",
        "v9-stats": "v9",
        "v9-trace": "v9",
        "v10-hook-null": "v10",
        "v10-module-off": "v10",
        "v10-stats": "v10",
        "v10-trace": "v10",
    }
    shared_buffers_for = {
        "W1": "128MB",
        "W3": "16MB",
        "W4": "4GB",
        "W6c": "32MB",
    }
    clients_for = {"W3": "8", "W4": "16", "W6c": "32"}
    for row in rows:
        label = (
            f"{row['config']}/{row['workload']}/rep{row['repetition']}"
        )
        require(row["seed"] == expected_seed, f"{label}: seed mismatch")
        require(row["build"] == build_for[row["config"]],
                f"{label}: build/config mismatch")
        require(row["shared_buffers"] == shared_buffers_for[row["workload"]],
                f"{label}: shared_buffers mismatch")
        require(
            row["server_cpus"] == SERVER_CPUS
            and row["pgbench_cpus"] == PGBENCH_CPUS,
            f"{label}: CPU affinity differs from the fixed protocol",
        )
        log_path = Path(row["server_log"])
        require(not log_path.is_absolute() and ".." not in log_path.parts,
                f"{label}: unsafe server-log path")
        if row["workload"] == "W1":
            finite_number(row["ns_per_iteration"],
                          f"{label} ns_per_iteration", positive=True)
            require(row["iterations"] == str(protocol["w1_iterations"]),
                    f"{label}: wrong W1 iteration count")
            require(
                row["clients"] == ""
                and row["duration_s"] == ""
                and row["warmup_s"] == "",
                f"{label}: unexpected pgbench fields",
            )
            for field in (
                "tps", "latency_avg_ms", "measurement_samples",
                "measurement_interval_s", "pgbench_cpu_percent",
                "pgbench_cpu_capacity_fraction",
            ):
                require(row[field] == "", f"{label}: unexpected {field}")
        else:
            require(row["clients"] == clients_for[row["workload"]],
                    f"{label}: client count mismatch")
            require(row["duration_s"] == str(protocol["duration_seconds"]),
                    f"{label}: duration mismatch")
            require(row["warmup_s"] == str(protocol["warmup_seconds"]),
                    f"{label}: warmup mismatch")
            require(row["iterations"] == "" and row["ns_per_iteration"] == "",
                    f"{label}: unexpected W1 fields")
            finite_number(row["tps"], f"{label} tps", positive=True)
            finite_number(row["latency_avg_ms"],
                          f"{label} latency", positive=True)
            samples = int(row["measurement_samples"])
            require(samples > 0, f"{label}: no progress samples")
            interval = finite_number(
                row["measurement_interval_s"],
                f"{label} measurement interval", positive=True)
            require(
                protocol["duration_seconds"] - 2
                <= interval
                <= protocol["duration_seconds"] + 1,
                f"{label}: unexpected measurement interval {interval}",
            )
            finite_number(row["pgbench_cpu_percent"],
                          f"{label} pgbench CPU")
            capacity = finite_number(
                row["pgbench_cpu_capacity_fraction"],
                f"{label} pgbench CPU capacity")
            require(capacity >= 0, f"{label}: negative CPU capacity")


def verify_completion(results_dir: Path, rows: list[dict[str, str]]) -> None:
    path = results_dir / "matrix-complete.json"
    require(path.is_file(), "matrix-complete.json is missing")
    marker = json.loads(path.read_text(encoding="utf-8"))
    require(marker.get("rows") == len(rows),
            "matrix-complete row count mismatch")
    for name in (
        "results.csv",
        "schedule.csv",
        "telemetry.jsonl",
        "protocol.json",
        "progress.json",
        "aa-early.jsonl",
    ):
        expected = marker.get("sha256", {}).get(name)
        require(expected == sha256(results_dir / name),
                f"matrix-complete hash mismatch: {name}")


def verify_progress(results_dir: Path, rows: list[dict[str, str]]) -> None:
    progress = json.loads(
        (results_dir / "progress.json").read_text(encoding="utf-8")
    )
    require(progress.get("state") == "complete",
            "matrix progress does not report completion")
    require(progress.get("cells_completed") == len(rows),
            "matrix progress completed-cell count is wrong")
    require(progress.get("cells_total") == len(rows),
            "matrix progress total-cell count is wrong")


def verify_early_aa(results_dir: Path, protocol: dict) -> None:
    entries = [
        json.loads(line)
        for line in (results_dir / "aa-early.jsonl").read_text(
            encoding="utf-8"
        ).splitlines()
        if line.strip()
    ]
    if protocol["mode"] == "smoke":
        require(not entries, "smoke run unexpectedly contains A/A noise gates")
        return
    require(
        [item.get("repetitions_complete") for item in entries] == [1, 3, 6, 9],
        "early A/A noise-gate checkpoints are incomplete",
    )
    require(all(item.get("passed") is True for item in entries),
            "an early A/A noise gate failed")


def verify_telemetry(results_dir: Path, rows: list[dict[str, str]]) -> None:
    path = results_dir / "telemetry.jsonl"
    require(path.is_file(), "telemetry.jsonl is missing")
    entries = [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    require(len(entries) == 2 * len(rows),
            f"telemetry has {len(entries)} rows, expected {2 * len(rows)}")
    by_index = {int(row["run_index"]): row for row in rows}
    seen: dict[int, set[str]] = defaultdict(set)
    for entry in entries:
        index = int(entry["run_index"])
        require(index in by_index, f"unknown telemetry run index: {index}")
        row = by_index[index]
        require(
            entry.get("config") == row["config"]
            and entry.get("workload") == row["workload"]
            and int(entry.get("repetition")) == int(row["repetition"]),
            f"telemetry metadata mismatch at run {index}",
        )
        require(entry["phase"] in {"before", "after"},
                f"bad telemetry phase at run {index}")
        require(entry["phase"] not in seen[index],
                f"duplicate telemetry phase at run {index}")
        seen[index].add(entry["phase"])
    require(set(seen) == {int(row["run_index"]) for row in rows},
            "telemetry run indexes do not match results")
    require(all(phases == {"before", "after"} for phases in seen.values()),
            "a result cell lacks before/after telemetry")


def verify_runtime_evidence(
    results_dir: Path,
    rows: list[dict[str, str]],
    protocol: dict,
) -> dict[str, float | int]:
    by_index = {int(row["run_index"]): row for row in rows}
    expected_client_files = 0
    expected_proofs: set[str] = set()
    expected_w3_summaries: set[str] = set()
    expected_w3_raw: set[str] = set()
    max_capacity = 0.0
    saturated: list[int] = []
    for index, row in by_index.items():
        server_log = results_dir / row["server_log"]
        require(server_log.is_file(), f"missing server log for run {index}")
        text = server_log.read_text(encoding="utf-8", errors="replace")
        require("database system is shut down" in text,
                f"run {index} has no clean-shutdown marker")
        init_log = results_dir / "logs" / f"server-{index}.log.initdb"
        require(init_log.is_file(), f"missing initdb log for run {index}")

        if row["workload"] in PGBENCH_WORKLOADS:
            expected_client_files += 1
            load_path = results_dir / "client-load" / f"cell-{index}.json"
            require(load_path.is_file(),
                    f"missing pgbench CPU sample for run {index}")
            load = json.loads(load_path.read_text(encoding="utf-8"))
            cpu = float(load["pgbench_cpu_percent"])
            threads = int(load["thread_count"])
            capacity = float(load["thread_capacity_fraction"])
            require(
                math.isfinite(cpu)
                and cpu >= 0
                and threads > 0
                and math.isfinite(capacity)
                and capacity >= 0,
                f"invalid pgbench CPU primitives for run {index}",
            )
            computed_capacity = cpu / (threads * 100.0)
            require(
                math.isclose(
                    capacity, computed_capacity, rel_tol=1e-9, abs_tol=1e-12
                ),
                f"derived pgbench CPU capacity is wrong for run {index}",
            )
            reported_cpu = float(row["pgbench_cpu_percent"])
            require(
                math.isclose(cpu, reported_cpu, rel_tol=1e-9, abs_tol=1e-12),
                f"pgbench CPU sample mismatch for run {index}",
            )
            reported = float(row["pgbench_cpu_capacity_fraction"])
            require(math.isclose(capacity, reported, rel_tol=1e-9,
                                 abs_tol=1e-12),
                    f"pgbench CPU sample mismatch for run {index}")
            require(
                capacity
                < float(protocol["max_pgbench_thread_capacity_fraction"]),
                f"pgbench client is saturated in run {index}",
            )
            max_capacity = max(max_capacity, capacity)
            if capacity >= 0.90:
                saturated.append(index)
            pgbench_log = results_dir / "logs" / f"pgbench-{index}.log"
            require(pgbench_log.is_file(),
                    f"missing pgbench log for run {index}")

        if row["workload"] in {"W4", "W6c"}:
            for name in (
                f"server-setup-{index}.log",
                f"pgbench-init-{index}.log",
            ):
                path = results_dir / "logs" / name
                require(path.is_file(), f"missing setup log: {name}")
            setup_text = (
                results_dir / "logs" / f"server-setup-{index}.log"
            ).read_text(encoding="utf-8", errors="replace")
            require("database system is shut down" in setup_text,
                    f"setup server {index} has no clean-shutdown marker")

        proof_expected = (
            row["config"] in ACTIVE_CONFIGS
            and row["workload"] in WORKLOADS
        )
        proof_path = results_dir / "recording-proofs" / f"cell-{index}.csv"
        if proof_expected:
            expected_proofs.add(proof_path.name)
        require(proof_path.is_file() == proof_expected,
                f"recording-proof presence mismatch for run {index}")
        if proof_expected:
            proof_fields = (
                ("timing_calls", "trace_records")
                if row["workload"] == "W1"
                else (
                    "client_count",
                    "clients_recording",
                    "timing_calls",
                    "representative_trace_records",
                )
            )
            proof = read_csv(proof_path, proof_fields)
            require(len(proof) == 1,
                    f"recording proof {index} must contain one row")
            item = proof[0]
            if row["workload"] == "W1":
                require(int(item["timing_calls"]) > 0,
                        f"W1 timing proof is empty for run {index}")
                trace_records = int(item["trace_records"])
            else:
                clients = int(row["clients"])
                require(int(item["client_count"]) == clients,
                        f"recording proof client count mismatch for run {index}")
                require(int(item["clients_recording"]) == clients,
                        f"not all clients recorded in run {index}")
                require(int(item["timing_calls"]) > 0,
                        f"timing proof is empty for run {index}")
                trace_records = int(item["representative_trace_records"])
            if row["config"] in TRACE_CONFIGS:
                require(trace_records > 0,
                        f"trace proof is empty for run {index}")
            else:
                require(trace_records == 0,
                        f"stats proof unexpectedly has trace rows at run {index}")

        qualification_expected = (
            row["workload"] == "W3" and row["config"] in ACTIVE_CONFIGS
        )
        summary_path = results_dir / "w3-qualification" / f"cell-{index}.json"
        raw_path = results_dir / "w3-qualification" / f"cell-{index}.tsv"
        if qualification_expected:
            expected_w3_summaries.add(summary_path.name)
            expected_w3_raw.add(raw_path.name)
        require(
            summary_path.is_file() == qualification_expected
            and raw_path.is_file() == qualification_expected,
            f"W3 qualification presence mismatch for run {index}",
        )
        if qualification_expected:
            qualification = json.loads(summary_path.read_text(encoding="utf-8"))
            snapshot = qualification.get("snapshot_seconds")
            minimum_snapshot = max(int(protocol["warmup_seconds"]) // 3, 1)
            require(
                isinstance(snapshot, (int, float))
                and minimum_snapshot <= snapshot < protocol["warmup_seconds"],
                f"W3 snapshot duration is wrong for run {index}",
            )
            recomputed = analyze_file(
                raw_path, float(snapshot)
            )
            require(evidence_equal(qualification, recomputed),
                    f"W3 summary does not match raw evidence for run {index}")
            require(qualification.get("passed") is True,
                    f"W3 qualification failed for run {index}")
            require(
                set(qualification.get("checks", {}))
                == {
                    "lwlock_calls_per_second",
                    "lwlock_fraction",
                    "procarray_fraction",
                    "io_fraction",
                    "histogram_coverage",
                    "p50",
                    "p95",
                }
                and all(qualification["checks"].values()),
                    f"W3 qualification check failed for run {index}")

    client_files = list((results_dir / "client-load").glob("cell-*.json"))
    require(len(client_files) == expected_client_files,
            "unexpected number of pgbench CPU evidence files")
    require(
        {path.name for path in (results_dir / "recording-proofs").iterdir()}
        == expected_proofs,
        "unexpected recording-proof file set",
    )
    w3_files = list((results_dir / "w3-qualification").iterdir())
    require(
        {path.name for path in w3_files if path.suffix == ".json"}
        == expected_w3_summaries
        and {path.name for path in w3_files if path.suffix == ".tsv"}
        == expected_w3_raw
        and len(w3_files)
        == len(expected_w3_summaries) + len(expected_w3_raw),
        "unexpected W3 qualification file set",
    )
    return {
        "max_pgbench_thread_capacity_fraction": max_capacity,
        "cells_at_or_above_90_percent_client_capacity": len(saturated),
    }


def statistical_analysis(
    rows: list[dict[str, str]],
    protocol: dict,
) -> dict:
    by_key = {
        (row["config"], row["workload"], int(row["repetition"])): row
        for row in rows
    }
    analyses = {}
    w1_margin = float(
        protocol["analysis"]["w1_equivalence_margin_ns"])
    pgbench_margin = float(
        protocol["analysis"]["pgbench_equivalence_margin_percent"])
    runs = int(protocol["runs_per_cell"])

    for workload in WORKLOADS:
        metric = "ns_per_iteration" if workload == "W1" else "tps"
        units = "ns/iteration" if workload == "W1" else "percent"
        margin = w1_margin if workload == "W1" else pgbench_margin
        arms = {}
        contrasts = {}
        v10_vs_v9 = {}

        def paired_contrast(
            treatment_config: str,
            reference_config: str,
        ) -> dict[str, float | int | str]:
            paired = []
            for repetition in range(1, runs + 1):
                treatment = float(
                    by_key[(treatment_config, workload, repetition)][metric]
                )
                reference = float(
                    by_key[(reference_config, workload, repetition)][metric]
                )
                if workload == "W1":
                    paired.append(treatment - reference)
                else:
                    paired.append((treatment / reference - 1.0) * 100.0)
            interval = confidence_interval(paired)
            interval["classification"] = classify_interval(interval, margin)
            interval["margin"] = margin
            interval["units"] = units
            return interval

        for config in CONFIGS:
            values = [
                float(by_key[(config, workload, repetition)][metric])
                for repetition in range(1, runs + 1)
            ]
            arms[config] = describe(values)
            if config == "master":
                continue
            contrasts[config] = paired_contrast(config, "master")

        for mode, v10_config, v9_config in V10_V9_PAIRS:
            interval = paired_contrast(v10_config, v9_config)
            interval["treatment"] = v10_config
            interval["reference"] = v9_config
            v10_vs_v9[mode] = interval

        aa_pairs = []
        for repetition in range(1, runs + 1):
            left = float(
                by_key[("master-aa", workload, repetition)][metric])
            right = float(
                by_key[("master", workload, repetition)][metric])
            aa_pairs.append((left - right) / ((left + right) / 2.0) * 100)
        aa_interval = confidence_interval(aa_pairs)
        max_cv = float(
            protocol["analysis"]["aa_stability"]
            ["max_cv_percent_each_arm"])
        max_bias = float(
            protocol["analysis"]["aa_stability"]
            ["max_absolute_paired_bias_percent"])
        aa_passed = (
            arms["master"]["cv_percent"] <= max_cv
            and arms["master-aa"]["cv_percent"] <= max_cv
            and aa_interval["lower_95"] >= -max_bias
            and aa_interval["upper_95"] <= max_bias
        )
        analyses[workload] = {
            "metric": metric,
            "arms": arms,
            "contrasts_vs_master": contrasts,
            "v10_vs_v9": v10_vs_v9,
            "aa_stability": {
                "paired_symmetric_percent": aa_interval,
                "passed": aa_passed,
                "max_cv_percent_each_arm": max_cv,
                "max_absolute_paired_bias_percent": max_bias,
            },
        }
    return analyses


def render_markdown(report: dict) -> str:
    suitability = report["performance_suitability"]
    lines = [
        "# Wait-event tracing v10 benchmark analysis",
        "",
        "Evidence completeness and integrity: **PASS**",
        "",
        "Performance suitability: "
        f"**{suitability['status']}**"
        + (
            f" ({suitability['reason']})"
            if suitability.get("reason")
            else ""
        ),
        "",
        "Host isolation: "
        f"**{report['host_environment']['host_isolation']}** "
        f"({report['host_environment']['co_resident_postgres_process_count']} "
        "pre-existing PostgreSQL processes).",
        "",
        "Statistical labels use paired 95% Student t intervals. "
        "Equivalence is reported only when the full interval lies inside "
        "the predeclared margin.",
        "For direct comparisons, positive W1 values mean v10 is slower; "
        "positive pgbench values mean v10 has higher throughput.",
        "",
    ]
    for workload in WORKLOADS:
        item = report["statistics"][workload]
        unit = "ns/iteration" if workload == "W1" else "%"
        lines.extend([
            f"## {workload}",
            "",
            "### Direct v10 minus v9 comparison",
            "",
            "| Mode | Paired difference | 95% interval | Classification |",
            "|---|---:|---:|---|",
        ])
        for mode, _, _ in V10_V9_PAIRS:
            contrast = item["v10_vs_v9"][mode]
            lines.append(
                f"| {mode} | {contrast['mean']:.6g} {unit} | "
                f"[{contrast['lower_95']:.6g}, "
                f"{contrast['upper_95']:.6g}] {unit} | "
                f"{contrast['classification']} |"
            )
        lines.extend([
            "",
            "### Secondary comparison against vanilla",
            "",
            "| Configuration | Paired difference | 95% interval | Classification |",
            "|---|---:|---:|---|",
        ])
        for config in CONFIGS[1:]:
            contrast = item["contrasts_vs_master"][config]
            lines.append(
                f"| {config} | {contrast['mean']:.6g} {unit} | "
                f"[{contrast['lower_95']:.6g}, "
                f"{contrast['upper_95']:.6g}] {unit} | "
                f"{contrast['classification']} |"
            )
        aa = item["aa_stability"]
        lines.extend([
            "",
            f"A/A stability: **{'PASS' if aa['passed'] else 'FAIL'}**.",
            "",
        ])
    load = report["client_load"]
    lines.extend([
        "## Client driver",
        "",
        f"Maximum sampled pgbench thread-capacity fraction: "
        f"{load['max_pgbench_thread_capacity_fraction']:.3f}.",
        f"Cells at or above 90%: "
        f"{load['cells_at_or_above_90_percent_client_capacity']}.",
        "",
        "CPU pinning mode: "
        f"`{report['protocol']['cpu_pinning']}`.",
        "",
    ])
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
        require((results_dir / "seed.txt").read_text(
            encoding="utf-8").strip() == str(protocol["seed"]),
            "seed.txt disagrees with protocol")

        manifest = verify_manifest(build_dir)
        manifest_path = build_dir / "manifest.json"
        require(
            protocol.get("build_manifest_sha256") == sha256(manifest_path),
            "build manifest is not the one bound when the matrix started",
        )
        evidence_root = root
        if not (evidence_root / "host-check.json").is_file():
            evidence_root = results_dir.parent
        host_report = verify_host(evidence_root, protocol, manifest)
        verify_bound_kit(protocol, kit_dir)
        rows = read_csv(results_dir / "results.csv", RESULT_FIELDS)
        verify_rows(rows, protocol)
        verify_schedule(results_dir, rows, protocol)
        verify_completion(results_dir, rows)
        verify_progress(results_dir, rows)
        verify_early_aa(results_dir, protocol)
        verify_telemetry(results_dir, rows)
        client_load = verify_runtime_evidence(results_dir, rows, protocol)
        statistics_report = statistical_analysis(rows, protocol)
        aa_passed = all(
            item["aa_stability"]["passed"]
            for item in statistics_report.values()
        )
        if protocol["mode"] == "smoke":
            suitability = {
                "status": "NOT_APPLICABLE",
                "reason": "smoke mode is functional validation only",
            }
            valid = True
        elif aa_passed:
            if host_report["host_isolation"] == "co-resident":
                suitability = {
                    "status": "PASS_WITH_CAVEAT",
                    "reason": (
                        "all baseline A/A gates passed, but the host had "
                        f"{host_report['co_resident_postgres_process_count']} "
                        "pre-existing PostgreSQL processes"
                    ),
                }
            else:
                suitability = {
                    "status": "PASS",
                    "reason": "all baseline A/A gates passed",
                }
            valid = True
        else:
            failed = ", ".join(
                workload
                for workload, item in statistics_report.items()
                if not item["aa_stability"]["passed"]
            )
            suitability = {
                "status": "FAIL",
                "reason": f"baseline A/A gate failed for {failed}",
            }
            valid = False

        report = {
            "valid": valid,
            "integrity_valid": True,
            "performance_suitability": suitability,
            "host_environment": host_report,
            "row_count": len(rows),
            "protocol": protocol,
            "build_manifest": manifest,
            "client_load": client_load,
            "statistics": statistics_report,
        }
        output_json = args.output_json or results_dir / "analysis.json"
        output_md = args.output_markdown or results_dir / "analysis.md"
        output_json.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        output_md.write_text(render_markdown(report), encoding="utf-8")
        if not valid:
            print(
                f"verification: FAIL: {suitability['reason']}",
                file=sys.stderr,
            )
        else:
            print(f"verification: PASS ({len(rows)} complete cells)")
        print(f"analysis JSON: {output_json}")
        print(f"analysis report: {output_md}")
        return 0 if valid else 1
    except (InvalidResults, KeyError, TypeError, ValueError, OSError,
            json.JSONDecodeError, QualificationError) as exc:
        print(f"verification: FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
