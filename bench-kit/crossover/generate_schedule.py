#!/usr/bin/env python3
"""Generate the balanced 16-session schedule (8 sequence A, 8 sequence B).

No build pairing (brief-v11-wpc-kit.md): every session runs the same
patched installation, so the only thing to balance is which of the two
bracket sequences (A/B) each session uses."""

from __future__ import annotations

import argparse
import csv
import random
import secrets
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from protocol import SCHEDULE_FIELDS, SESSIONS


def generate(path: Path, sessions: int = SESSIONS, seed: int | None = None) -> int:
    if sessions <= 0 or sessions % 2:
        raise ValueError("session count must be a positive even number")
    if seed is None:
        seed = secrets.randbits(64)
    randomizer = random.Random(seed)
    sequences = ["A"] * (sessions // 2) + ["B"] * (sessions // 2)
    randomizer.shuffle(sequences)

    rows = [
        {
            "session_index": session_index,
            "sequence": sequence,
            "pgbench_seed": session_index,
        }
        for session_index, sequence in enumerate(sequences, 1)
    ]

    with path.open("x", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=SCHEDULE_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return seed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path)
    parser.add_argument("--sessions", type=int, default=SESSIONS)
    args = parser.parse_args()
    print(generate(args.path, args.sessions))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
