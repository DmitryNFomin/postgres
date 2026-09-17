#!/usr/bin/env python3
"""Generate balanced adjacent v9/v10 session pairs."""

from __future__ import annotations

import argparse
import csv
import random
import secrets
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from protocol import BUILDS, PAIRS, SCHEDULE_FIELDS


def generate(path: Path, pairs: int = PAIRS, seed: int | None = None) -> int:
    if pairs <= 0 or pairs % 4:
        raise ValueError("pair count must be a positive multiple of four")
    if seed is None:
        seed = secrets.randbits(64)
    randomizer = random.Random(seed)
    combinations = [
        (sequence, first_build)
        for sequence in ("A", "B")
        for first_build in BUILDS
        for _ in range(pairs // 4)
    ]
    randomizer.shuffle(combinations)

    rows = []
    session_index = 1
    for pair_index, (sequence, first_build) in enumerate(combinations, 1):
        build_order = [
            first_build,
            next(build for build in BUILDS if build != first_build),
        ]
        for pair_position, build in enumerate(build_order, 1):
            rows.append({
                "session_index": session_index,
                "pair_index": pair_index,
                "pair_position": pair_position,
                "build": build,
                "sequence": sequence,
                "pgbench_seed": pair_index,
            })
            session_index += 1

    with path.open("x", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=SCHEDULE_FIELDS,
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
    return seed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path)
    parser.add_argument("--pairs", type=int, default=PAIRS)
    args = parser.parse_args()
    print(generate(args.path, args.pairs))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
