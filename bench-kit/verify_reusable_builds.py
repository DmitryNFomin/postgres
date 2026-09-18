#!/usr/bin/env python3
"""Verify that work/manifest.json's recorded builds still match what
01-build-all.sh would produce right now, so run-benchmark.sh --reuse-builds
can skip the 20-to-40-minute build phase after a restart.

Recomputes, from the files actually on disk:
  - the bundled source-archive SHA-256 hashes (source-manifest.json,
    postgres-{master,patched,control}.tar.gz) against manifest.json's
    "bundled_source" section;
  - every installed build against build_manifest_rules.py's
    validate_installed_builds() -- the exact same postgres/pgbench/psql/
    initdb/pg_ctl, test_wait_primitive fixture, and pg_wait_event_tracing
    module rules 02-run-matrix.sh's own preflight applies. This is the
    same module both use, on purpose: a work/manifest.json that passed a
    binary-only check here once got past --reuse-builds and then failed
    02-run-matrix.sh's manifest check right after the plateau probe (a
    build had installed the tracing module into "control", which
    --reuse-builds never looked at) -- see reports/wpf-report.md,
    Addendum 4.

Exits 0 only if everything still matches byte-for-byte; exits 1 (with a
diagnostic on stderr) otherwise. Never modifies anything.
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_manifest_rules import ManifestMismatch, validate_installed_builds


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

    builds = manifest.get("builds")
    if not isinstance(builds, list) or not builds:
        fail("build manifest has no builds")
        return 1
    prefixes = {
        build.get("name"): build.get("runtime_prefix") for build in builds
    }
    try:
        validate_installed_builds(manifest, prefixes)
    except ManifestMismatch as exc:
        fail(str(exc))
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

    if ok:
        print(
            f"verify_reusable_builds: {len(builds)} build(s) match "
            "work/manifest.json byte-for-byte"
        )
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
