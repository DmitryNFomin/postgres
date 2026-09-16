#!/usr/bin/env python3
"""Generate the randomized baseline A/B diagnostic schedule."""

import argparse
import csv
import random
import secrets
from pathlib import Path


CONFIGS = ("baseline-a", "baseline-b")


def generate(path, repetitions):
    seed = secrets.randbits(64)
    randomizer = random.Random(seed)
    rows = []
    for repetition in range(1, repetitions + 1):
        configs = list(CONFIGS)
        randomizer.shuffle(configs)
        rows.extend((config, repetition) for config in configs)
    with path.open("x", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(("run_index", "config", "repetition"))
        for index, (config, repetition) in enumerate(rows, 1):
            writer.writerow((index, config, repetition))
    return seed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("path", type=Path)
    parser.add_argument("repetitions", type=int)
    args = parser.parse_args()
    if args.repetitions <= 0:
        raise SystemExit("repetitions must be positive")
    print(generate(args.path, args.repetitions))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
