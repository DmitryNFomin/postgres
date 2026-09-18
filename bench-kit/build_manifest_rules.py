#!/usr/bin/env python3
"""Shared installed-build validation rules for the v11 bench-kit.

Both 02-run-matrix.sh (preflight, before every matrix run) and
run-benchmark.sh --reuse-builds (verify_reusable_builds.py) must apply
the exact same rules for what an installed build under work/install/ is
allowed to look like -- this is the one place those rules live, so the
two can never drift out of sync again. (They did once: a
work/manifest.json from a build that had installed the
pg_wait_event_tracing module into "control" passed --reuse-builds's old
binary-only check, then failed 02-run-matrix.sh's own manifest check
right after the plateau probe -- see reports/wpf-report.md, Addendum 4.)

The rule, for every one of the four builds (baseline-a, baseline-b,
control, patched):
  - postgres/pgbench/psql/initdb/pg_ctl under bin/ must exist and match
    the SHA-256 recorded in the manifest;
  - the test_wait_primitive fixture under lib/ must exist and match its
    recorded hash, in all four builds;
  - the pg_wait_event_tracing module under lib/ must exist, be unique,
    and match its recorded hash in "patched" only; it must be absent in
    every other build.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

REQUIRED_BINARIES = ("postgres", "pgbench", "psql", "initdb", "pg_ctl")
FIXTURE_GLOB = "lib/**/test_wait_primitive.*"
MODULE_GLOB = "lib/**/pg_wait_event_tracing.*"
BUILD_WITH_MODULE = "patched"


class ManifestMismatch(Exception):
    """An installed build does not match its manifest record, or the
    manifest itself is not in the expected shape."""


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def validate_manifest_shape(manifest: dict) -> None:
    if manifest.get("schema_version") != 11:
        raise ManifestMismatch("unsupported build manifest schema")
    if manifest.get("benchmark_series") != "wet-v11":
        raise ManifestMismatch("unexpected build benchmark series")


def validate_installed_build(name: str, prefix: Path, expected: dict) -> None:
    """Validate one build's installed prefix against its manifest.json
    "sha256" record. Raises ManifestMismatch with a specific reason on
    any mismatch; returns None if everything matches."""
    for binary in REQUIRED_BINARIES:
        path = prefix / "bin" / binary
        expected_hash = expected.get(binary)
        if not expected_hash:
            raise ManifestMismatch(
                f"{name}: manifest has no recorded hash for {binary}"
            )
        if not path.is_file():
            raise ManifestMismatch(f"{name}: missing installed binary {path}")
        if digest(path) != expected_hash:
            raise ManifestMismatch(f"{name}/{binary} differs from build manifest")

    fixture = next(prefix.glob(FIXTURE_GLOB), None)
    expected_fixture = expected.get("test_wait_primitive")
    if not expected_fixture:
        raise ManifestMismatch(
            f"{name}: manifest has no recorded test_wait_primitive hash"
        )
    if fixture is None or digest(fixture) != expected_fixture:
        raise ManifestMismatch(f"{name}/test_wait_primitive differs from manifest")

    modules = [path for path in prefix.glob(MODULE_GLOB) if path.is_file()]
    if name == BUILD_WITH_MODULE:
        expected_module = expected.get("pg_wait_event_tracing")
        if not expected_module or expected_module == "none":
            raise ManifestMismatch(
                f"{name}: manifest has no recorded tracing-module hash"
            )
        if len(modules) != 1:
            raise ManifestMismatch(f"{name} prefix has no unique tracing module")
        if digest(modules[0]) != expected_module:
            raise ManifestMismatch(f"{name} tracing module differs from manifest")
    elif modules:
        raise ManifestMismatch(f"{name} unexpectedly contains the tracing module")


def validate_installed_builds(manifest: dict, prefixes: dict) -> None:
    """prefixes: {build_name: prefix_path_string}. Validates the
    manifest's shape, that it describes exactly this set of builds, and
    every one of them against validate_installed_build(). Raises
    ManifestMismatch on the first problem found."""
    validate_manifest_shape(manifest)
    builds = {item["name"]: item for item in manifest.get("builds", [])}
    if set(builds) != set(prefixes):
        raise ManifestMismatch(
            "build manifest does not describe the required prefixes"
        )
    for name, prefix_text in prefixes.items():
        validate_installed_build(
            name, Path(prefix_text), builds[name].get("sha256") or {}
        )
