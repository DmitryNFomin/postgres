#!/usr/bin/env python3
"""Exact Wilcoxon signed-rank test and Hodges-Lehmann confidence interval.

Standard library only, implemented from scratch for the v11 bare-metal
benchmark kit (see brief-v11-wpc-kit.md). This module is used by
analyze-results.py to compute a rank-based estimator that is reported
alongside the paired Student t interval; a contrast is only ever reported
as a resolved difference when both estimators agree, which is what keeps
the analysis honest under the session-level throughput plateaus seen in
the v10 runs (a single-mode t interval can be dragged off zero by a
plateau even when the bulk of the blocks show no difference).

Method (two-sided, paired one-sample test against zero; Hollander & Wolfe,
"Nonparametric Statistical Methods", ch. 3):

  1. Drop exact zero differences (standard Wilcoxon convention). Let n be
     the number of remaining nonzero differences.
  2. Rank |d_i|, 1..n, using midranks for ties.
  3. W+ = sum of ranks whose difference is positive.
  4. Exact null distribution: assuming no ties, W+ is the sum of a random
     subset of {1, ..., n} where every element is included independently
     with probability 1/2 (each rank's sign is +/- with equal probability
     under the null of a symmetric distribution centered at zero). The
     count of subsets achieving each possible sum is the coefficient of
     x^sum in the product (1+x^1)(1+x^2)...(1+x^n), computed by a simple
     O(n^2) dynamic program. This is exact for any n, and n <= 16 keeps
     the table (at most 137 sums) tiny.
  5. When |d_i| ties are present the exact permutation distribution above
     is no longer strictly valid (the standard textbook caveat), so this
     module falls back to the usual normal approximation with the
     tie-correction term for the variance. This only affects the
     between-tie-group case; it never affects the Hodges-Lehmann point
     estimate, which is always the exact median of Walsh averages.
  6. Hodges-Lehmann estimate: the median of the n(n+1)/2 Walsh averages
     (d_i + d_j) / 2 for i <= j.
  7. Distribution-free 95% CI for the Hodges-Lehmann estimate: order the
     Walsh averages, find the largest critical count C such that the
     exact (or normal-approximated) null CDF of W+ at C-1 is <= alpha/2,
     and take the C-th and (M-C+1)-th order statistics (M = n(n+1)/2).
     This is the standard Wilcoxon-interval-inversion construction and is
     conservative (coverage >= 1 - alpha) whenever the discrete null
     distribution cannot hit alpha/2 exactly, which is the safe direction
     for an equivalence claim.
"""

from __future__ import annotations

import math
from typing import Sequence


def _midranks(values: Sequence[float]) -> list[float]:
    """Rank 1..n with average ("mid") ranks for ties, ascending order."""
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
            j += 1
        average_rank = (i + 1 + j + 1) / 2.0
        for k in range(i, j + 1):
            ranks[order[k]] = average_rank
        i = j + 1
    return ranks


def _exact_subset_sum_counts(n: int) -> list[int]:
    """Coefficients of prod_{i=1}^n (1 + x^i): counts[s] = #subsets summing to s."""
    total = n * (n + 1) // 2
    counts = [0] * (total + 1)
    counts[0] = 1
    running_total = 0
    for i in range(1, n + 1):
        running_total += i
        for s in range(running_total, i - 1, -1):
            counts[s] += counts[s - i]
    return counts


def _exact_cdf(counts: list[int], total_mass: int, k: int) -> float:
    """P(W+ <= k) under the exact null distribution."""
    if k < 0:
        return 0.0
    if k >= len(counts) - 1:
        return 1.0
    return sum(counts[: k + 1]) / total_mass


class WilcoxonResult:
    __slots__ = (
        "n",
        "n_dropped_zero",
        "w_plus",
        "w_minus",
        "p_value",
        "exact",
        "hl_estimate",
        "hl_lower",
        "hl_upper",
        "alpha",
    )

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def to_dict(self) -> dict:
        return {name: getattr(self, name) for name in self.__slots__}


def _walsh_averages(diffs: Sequence[float]) -> list[float]:
    n = len(diffs)
    averages = []
    for i in range(n):
        for j in range(i, n):
            averages.append((diffs[i] + diffs[j]) / 2.0)
    return averages


def _critical_count(
    n: int,
    alpha: float,
    counts: list[int] | None,
    total_mass: int | None,
) -> int:
    """Largest C such that P(W+ <= C - 1) <= alpha/2 (exact or normal)."""
    half_alpha = alpha / 2.0
    if counts is not None:
        cumulative = 0
        c = 0
        for s, count in enumerate(counts):
            candidate_cumulative = cumulative + count
            if candidate_cumulative / total_mass <= half_alpha:
                cumulative = candidate_cumulative
                c = s + 1
            else:
                break
        return max(c, 0)
    # Normal approximation fallback (ties present): find largest integer
    # k such that Phi((k + 0.5 - mu) / sigma) <= alpha/2, continuity
    # corrected, then C = k + 1.
    mu = n * (n + 1) / 4.0
    sigma = math.sqrt(n * (n + 1) * (2 * n + 1) / 24.0)
    if sigma == 0:
        return 0
    z = _inverse_normal_cdf(half_alpha)
    k = mu + z * sigma - 0.5
    return max(int(math.floor(k)) + 1, 0)


def _inverse_normal_cdf(p: float) -> float:
    """Acklam's rational approximation to the standard normal quantile."""
    if p <= 0.0:
        return -37.0
    if p >= 1.0:
        return 37.0
    a = (
        -3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02,
        1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00,
    )
    b = (
        -5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02,
        6.680131188771972e01, -1.328068155288572e01,
    )
    c = (
        -7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00,
        -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00,
    )
    d = (
        7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00,
        3.754408661907416e00,
    )
    p_low = 0.02425
    p_high = 1 - p_low
    if p < p_low:
        q = math.sqrt(-2 * math.log(p))
        return (
            ((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]
        ) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    if p <= p_high:
        q = p - 0.5
        r = q * q
        return (
            (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5])
            * q
        ) / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
    q = math.sqrt(-2 * math.log(1 - p))
    return -(
        ((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]
    ) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)


def wilcoxon_signed_rank(
    diffs: Sequence[float],
    alpha: float = 0.05,
) -> WilcoxonResult:
    """Exact (or, under ties, normal-approximated) paired Wilcoxon test
    plus the distribution-free Hodges-Lehmann point estimate and interval.
    """
    if not diffs:
        raise ValueError("wilcoxon_signed_rank requires at least one pair")
    nonzero = [d for d in diffs if d != 0.0]
    n_dropped_zero = len(diffs) - len(nonzero)
    n = len(nonzero)
    if n == 0:
        return WilcoxonResult(
            n=0, n_dropped_zero=n_dropped_zero, w_plus=0.0, w_minus=0.0,
            p_value=1.0, exact=True, hl_estimate=0.0, hl_lower=0.0,
            hl_upper=0.0, alpha=alpha,
        )

    absolutes = [abs(d) for d in nonzero]
    ranks = _midranks(absolutes)
    has_ties = len(set(absolutes)) != n
    w_plus = sum(r for r, d in zip(ranks, nonzero) if d > 0)
    total_rank = n * (n + 1) / 2.0
    w_minus = total_rank - w_plus

    counts = None
    total_mass = None
    if not has_ties:
        counts = _exact_subset_sum_counts(n)
        total_mass = 1 << n
        # w_plus is an integer when there are no ties. By the symmetry of
        # the null distribution (W+ and total_rank - W+ are identically
        # distributed), P(W+ >= k) = P(W+ <= total_rank - k).
        w_plus_int = int(round(w_plus))
        total_rank_int = int(round(total_rank))
        lower_tail = _exact_cdf(counts, total_mass, w_plus_int)
        upper_tail = _exact_cdf(counts, total_mass, total_rank_int - w_plus_int)
        p_value = min(1.0, 2.0 * min(lower_tail, upper_tail))
        exact = True
    else:
        mu = n * (n + 1) / 4.0
        tie_groups: dict[float, int] = {}
        for value in absolutes:
            tie_groups[value] = tie_groups.get(value, 0) + 1
        tie_correction = sum(t**3 - t for t in tie_groups.values())
        variance = n * (n + 1) * (2 * n + 1) / 24.0 - tie_correction / 48.0
        sigma = math.sqrt(max(variance, 0.0))
        if sigma == 0:
            p_value = 1.0
        else:
            # Continuity-corrected two-sided normal approximation.
            z = (abs(w_plus - mu) - 0.5) / sigma
            z = max(z, 0.0)
            p_value = min(1.0, 2.0 * (1.0 - _normal_cdf(z)))
        exact = False

    walsh = sorted(_walsh_averages(nonzero))
    m = len(walsh)
    hl_estimate = _median(walsh)

    c = _critical_count(n, alpha, counts, total_mass)
    # Order statistics are 1-indexed in the textbook formula.
    lower_index = c
    upper_index = m - c + 1
    if lower_index < 1 or upper_index > m or lower_index > upper_index:
        # n too small (or alpha too strict) to achieve any nontrivial
        # distribution-free interval; fall back to the full range, which
        # is conservative (never narrower than what the data supports).
        hl_lower = walsh[0]
        hl_upper = walsh[-1]
    else:
        hl_lower = walsh[lower_index - 1]
        hl_upper = walsh[upper_index - 1]

    return WilcoxonResult(
        n=n,
        n_dropped_zero=n_dropped_zero,
        w_plus=w_plus,
        w_minus=w_minus,
        p_value=p_value,
        exact=exact,
        hl_estimate=hl_estimate,
        hl_lower=hl_lower,
        hl_upper=hl_upper,
        alpha=alpha,
    )


def _median(values: Sequence[float]) -> float:
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def _normal_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


if __name__ == "__main__":
    import sys

    values = [float(token) for token in sys.argv[1:]]
    if not values:
        print("usage: wilcoxon.py D1 D2 D3 ...", file=sys.stderr)
        raise SystemExit(2)
    result = wilcoxon_signed_rank(values)
    for key, value in result.to_dict().items():
        print(f"{key}: {value}")
