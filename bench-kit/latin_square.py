#!/usr/bin/env python3
"""7x7 Latin-square configuration order for the v11 randomized-block matrix.

brief-v11-wpc-kit.md: "One repetition = one block in which all seven
configurations run once for a given workload, in an order from a 7x7
Latin square chosen so that over 16 repetitions every configuration
occupies every position at least twice."

Construction: pick a random base permutation P of the 7 configurations,
then row r (0..6) is P cyclically rotated by r. This is a standard
cyclic Latin square: every row is a permutation of the configurations,
and for any fixed column/position i, as r ranges over 0..6 the entry
P[(i + r) % 7] ranges over every configuration exactly once (the map
r -> (i + r) % 7 is a bijection on {0..6}). So "each row used at least
twice across 16 repetitions" is exactly the condition that makes every
position see every configuration at least twice.

16 repetitions = 2 * 7 + 2, so 7 rows are used twice each and 2 rows
(chosen at random) are used a third time; which two rows are tripled,
and the order the 16 row-uses are dealt out in, are both randomized so
the extra weight and any time-of-run confound are not fixed in advance.
"""

from __future__ import annotations

import random
from typing import Sequence


def build_schedule(
    rng: random.Random,
    configs: Sequence[str],
    runs: int,
) -> list[list[str]]:
    """Return `runs` config orders (one per repetition), each a full
    permutation of `configs`, satisfying the "every position sees every
    configuration at least floor(runs / len(configs)) times" property.
    """
    n = len(configs)
    if runs < 1:
        raise ValueError(f"need at least 1 repetition; got {runs}")
    # The real protocol always calls this with runs=16 >= n=7 (RUNS in
    # benchmark_protocol.py, unconditional even in fake mode -- see its
    # comment), which is what actually gives the "every position sees
    # every configuration at least floor(runs / n) times" balance
    # verify_schedule() checks. For runs < n (only ever true for a
    # SELFTEST_FAKE_PREFIX FULL_PROFILE, which compresses runs_per_cell to
    # 1) that quotient is 0, so verify_schedule() is called with
    # min_occurrences_per_position=0 and the balance guarantee is
    # vacuous -- there is nothing to balance over a single repetition
    # anyway. The quotient/remainder math below already handles runs < n
    # correctly (it degrades to "pick `runs` distinct rows at random");
    # only this guard needs to stop pretending 1..n-1 is invalid.
    base = list(configs)
    rng.shuffle(base)

    quotient, remainder = divmod(runs, n)
    row_uses = [quotient] * n
    for extra_row in rng.sample(range(n), remainder):
        row_uses[extra_row] += 1
    row_sequence = [row for row, count in enumerate(row_uses) for _ in range(count)]
    assert len(row_sequence) == runs
    rng.shuffle(row_sequence)

    schedule = []
    for row in row_sequence:
        schedule.append([base[(i + row) % n] for i in range(n)])
    return schedule


def verify_schedule(
    schedule: Sequence[Sequence[str]],
    configs: Sequence[str],
    min_occurrences_per_position: int,
) -> None:
    """Raise ValueError if `schedule` does not meet the Latin-square
    position-coverage guarantee build_schedule() promises."""
    configs = list(configs)
    n = len(configs)
    for repetition, order in enumerate(schedule, 1):
        if sorted(order) != sorted(configs):
            raise ValueError(
                f"repetition {repetition} is not a complete permutation "
                "of the configuration set"
            )
    for position in range(n):
        counts = {}
        for order in schedule:
            counts[order[position]] = counts.get(order[position], 0) + 1
        for config in configs:
            if counts.get(config, 0) < min_occurrences_per_position:
                raise ValueError(
                    f"position {position} sees configuration {config!r} "
                    f"only {counts.get(config, 0)} time(s), fewer than "
                    f"the required {min_occurrences_per_position}"
                )


if __name__ == "__main__":
    import sys

    demo_configs = ("master", "master-aa", "control", "hook-null",
                     "module-off", "stats", "trace")
    seed = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    runs = int(sys.argv[2]) if len(sys.argv) > 2 else 16
    schedule = build_schedule(random.Random(seed), demo_configs, runs)
    verify_schedule(schedule, demo_configs, runs // len(demo_configs))
    for repetition, order in enumerate(schedule, 1):
        print(repetition, order)
