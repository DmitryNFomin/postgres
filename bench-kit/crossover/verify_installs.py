#!/usr/bin/env python3
"""Verify the reused v11-kit "patched" installation tree against the
bound build manifest (brief-v11-wpc-kit.md: crossover runs on the patched
installation only)."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


def verify_install(manifest: dict, name: str, prefix: Path) -> None:
    if not prefix.is_dir() or prefix.is_symlink():
        raise RuntimeError(
            "{} installation root is missing or linked".format(name)
        )
    builds = {item["name"]: item for item in manifest["builds"]}
    expected = builds[name]["install_tree"]
    actual_paths = {
        path.relative_to(prefix).as_posix()
        for path in prefix.rglob("*")
        if path.is_file() or path.is_symlink()
    }
    if actual_paths != set(expected):
        raise RuntimeError("{} install-tree file set differs".format(name))
    for relative, item in expected.items():
        path = prefix / relative
        if item["type"] == "file":
            if not path.is_file() or path.is_symlink():
                raise RuntimeError(
                    "{} {} type differs".format(name, relative)
                )
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if digest != item["sha256"]:
                raise RuntimeError("{} {} differs".format(name, relative))
        elif item["type"] == "symlink":
            if (
                not path.is_symlink()
                or path.readlink().as_posix() != item["target"]
            ):
                raise RuntimeError(
                    "{} {} symlink differs".format(name, relative)
                )
        else:
            raise RuntimeError("{} has unknown manifest type".format(relative))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    parser.add_argument("patched", type=Path)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    if (
        manifest.get("schema_version") != 11
        or manifest.get("benchmark_series") != "wet-v11"
    ):
        raise RuntimeError("unexpected v11-kit build manifest")
    verify_install(manifest, "patched", args.patched)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
