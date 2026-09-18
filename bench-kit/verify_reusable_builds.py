#!/usr/bin/env python3
"""Verify that work/manifest.json's recorded builds still match what
01-build-all.sh would produce right now, so run-benchmark.sh --reuse-builds
can skip the 20-to-40-minute build phase after a restart.

Recomputes, from the files actually on disk:
  - the bundled source-archive SHA-256 hashes (source-manifest.json,
    postgres-{master,patched,control}.tar.gz) against manifest.json's
    "bundled_source" section;
  - the installed postgres/pgbench/psql/initdb/pg_ctl SHA-256 for every
    build's runtime_prefix against that build's manifest.json record.

Exits 0 only if everything still matches byte-for-byte; exits 1 (with a
diagnostic on stderr) otherwise. Never modifies anything.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def fail(message: str) -> None:
    print(f"verify_reusable_builds: {message}", file=sys.stderr)


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: verify_reusable_builds.py KIT_DIR", file=sys.stderr)
        return 2
    kit_dir = Path(sys.argv[1])
    manifest_path = kit_dir / "work" / "manifest.json"
    if not manifest_path.is_file():
        fail(f"missing {manifest_path}")
        return 1
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        fail(f"could not read {manifest_path}: {exc}")
        return 1

    if manifest.get("schema_version") != 11:
        fail("unsupported build manifest schema_version")
        return 1
    if manifest.get("benchmark_series") != "wet-v11":
        fail("unexpected build manifest benchmark_series")
        return 1

    bundled = manifest.get("bundled_source") or {}
    source_dir = kit_dir / "source"
    source_files = {
        "manifest_sha256": source_dir / "source-manifest.json",
        "master_archive_sha256": source_dir / "postgres-master.tar.gz",
        "patched_archive_sha256": source_dir / "postgres-patched.tar.gz",
        "control_archive_sha256": source_dir / "postgres-control.tar.gz",
    }
    ok = True
    for key, path in source_files.items():
        expected = bundled.get(key)
        if not expected:
            # Fake-binary self-tests (SELFTEST_FAKE_PREFIX) never populate
            # bundled_source; nothing real to reuse-verify there, and
            # run-benchmark.sh never passes --reuse-builds in that mode.
            continue
        if not path.is_file():
            fail(f"missing bundled source file: {path}")
            ok = False
            continue
        actual = digest(path)
        if actual != expected:
            fail(f"{path.name} hash changed: manifest has {expected}, disk has {actual}")
            ok = False

    builds = manifest.get("builds")
    if not isinstance(builds, list) or not builds:
        fail("build manifest has no builds")
        return 1

    for build in builds:
        name = build.get("name", "?")
        prefix = Path(build.get("runtime_prefix", ""))
        expected_sha = build.get("sha256") or {}
        for binary in ("postgres", "pgbench", "psql", "initdb", "pg_ctl"):
            path = prefix / "bin" / binary
            expected = expected_sha.get(binary)
            if not expected:
                fail(f"{name}: manifest has no recorded hash for {binary}")
                ok = False
                continue
            if not path.is_file():
                fail(f"{name}: missing installed binary {path}")
                ok = False
                continue
            actual = digest(path)
            if actual != expected:
                fail(
                    f"{name}/{binary} changed since the build manifest was "
                    f"written: manifest has {expected}, disk has {actual}"
                )
                ok = False

    if ok:
        print(
            f"verify_reusable_builds: {len(builds)} build(s) match "
            "work/manifest.json byte-for-byte"
        )
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
