#!/usr/bin/env python3
"""Validate target runtime values that have platform length limits."""

from __future__ import annotations

import argparse
import os
from pathlib import Path


MAX_UNIX_SOCKET_PATH_BYTES = 107


def postgres_socket_path(socket_directory: Path, port: int) -> Path:
    return socket_directory / ".s.PGSQL.{}".format(port)


def validate_socket_path(socket_directory: Path, port: int) -> None:
    path = postgres_socket_path(socket_directory, port)
    length = len(os.fsencode(str(path)))
    if length > MAX_UNIX_SOCKET_PATH_BYTES:
        raise RuntimeError(
            "PostgreSQL socket path is {} bytes, maximum is {}: {}".format(
                length,
                MAX_UNIX_SOCKET_PATH_BYTES,
                path,
            )
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("socket_directory", type=Path)
    parser.add_argument("port", type=int)
    args = parser.parse_args()
    validate_socket_path(args.socket_directory, args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
