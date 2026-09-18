#!/usr/bin/env python3
"""Load and validate sources.conf, the single place pinned commits live.

See the "Launch note" in brief-v11-wpc-kit.md: the three commit hashes are
filled in later by the coordinator. Every script that needs them reads
this file (bash scripts source it directly -- it is plain KEY=VALUE --
Python scripts use load()/require_no_placeholders() below) so there is
exactly one place to edit and exactly one gate that blocks a run while a
placeholder remains.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

PLACEHOLDER_RE = re.compile(r"^<[A-Za-z0-9_]+>$")

REQUIRED_KEYS = (
    "MASTER_SHA",
    "V11_SHA",
    "CONTROL_SHA",
    "POSTGRES_REPO_URL",
    "CONTROL_PATCH_GLOB",
    "V11_PATCH_GLOB",
    "MODULE_NAME",
    "HOOKS_INSTALLED_FUNCTION",
)


def load(path: Path | str) -> dict[str, str]:
    path = Path(path)
    data: dict[str, str] = {}
    for lineno, raw_line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), 1
    ):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if not sep:
            raise ValueError(
                f"{path}:{lineno}: malformed line (expected KEY=VALUE): "
                f"{raw_line!r}"
            )
        key = key.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise ValueError(f"{path}:{lineno}: invalid key name: {key!r}")
        data[key] = value.strip()
    missing = [key for key in REQUIRED_KEYS if key not in data]
    if missing:
        raise ValueError(
            f"{path} is missing required key(s): {', '.join(missing)}"
        )
    return data


def placeholder_keys(data: dict[str, str]) -> list[str]:
    return sorted(key for key, value in data.items() if PLACEHOLDER_RE.match(value))


def require_no_placeholders(data: dict[str, str], path: Path | str) -> None:
    remaining = placeholder_keys(data)
    if remaining:
        raise SystemExit(
            f"{path} still has placeholder value(s) for: "
            + ", ".join(remaining)
            + " -- the coordinator must fill these in (see the brief's "
            "Launch note) before this kit can run."
        )


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print(
            "usage: sources_conf.py check PATH | get PATH KEY "
            "| placeholders PATH",
            file=sys.stderr,
        )
        return 2
    command = argv[0]
    path = Path(argv[1])
    try:
        data = load(path)
        if command == "check":
            require_no_placeholders(data, path)
            print("sources.conf: no placeholders remain")
            return 0
        if command == "placeholders":
            remaining = placeholder_keys(data)
            for key in remaining:
                print(key)
            return 1 if remaining else 0
        if command == "get":
            if len(argv) != 3:
                raise SystemExit("usage: sources_conf.py get PATH KEY")
            key = argv[2]
            if key not in data:
                raise SystemExit(f"unknown sources.conf key: {key}")
            print(data[key])
            return 0
        raise SystemExit(f"unknown command: {command}")
    except (OSError, ValueError) as exc:
        print(f"sources_conf.py: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
