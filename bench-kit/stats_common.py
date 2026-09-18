#!/usr/bin/env python3
"""Shared estimator/classification helpers for analyze-results.py and
self-test.py (kept in one place so the self-test's plateau scenario
exercises the exact same code the real analyzer runs)."""

from __future__ import annotations

import math
import statistics


def t95(df: int) -> float:
    values = (
        None, 12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365,
        2.306, 2.262, 2.228, 2.201, 2.179, 2.160, 2.145, 2.131,
        2.120, 2.110, 2.101, 2.093, 2.086, 2.080, 2.074, 2.069,
        2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042,
    )
    return values[df] if df < len(values) else 1.96


def confidence_interval(values: list) -> dict:
    if not values:
        raise ValueError("cannot calculate an interval over no values")
    mean = statistics.mean(values)
    if len(values) == 1:
        lower = upper = mean
    else:
        half = t95(len(values) - 1) * statistics.stdev(values)
        half /= math.sqrt(len(values))
        lower, upper = mean - half, mean + half
    return {"n": len(values), "mean": mean, "lower_95": lower, "upper_95": upper}


def describe(values: list) -> dict:
    mean = statistics.mean(values)
    stdev = statistics.stdev(values) if len(values) > 1 else 0.0
    return {
        "n": len(values),
        "mean": mean,
        "median": statistics.median(values),
        "stdev": stdev,
        "cv_percent": stdev / mean * 100 if mean else float("inf"),
        "min": min(values),
        "max": max(values),
    }


def classify_contrast(
    t_lower: float, t_upper: float, hl_lower: float, hl_upper: float,
    margin: float,
) -> str:
    """brief-v11-wpc-kit.md, "Analysis": equivalent iff both intervals lie
    inside the margin; faster/slower iff both exclude zero on the same
    side; otherwise unresolved."""
    t_in_margin = t_lower >= -margin and t_upper <= margin
    hl_in_margin = hl_lower >= -margin and hl_upper <= margin
    if t_in_margin and hl_in_margin:
        return "equivalent"
    if t_lower > 0 and hl_lower > 0:
        return "resolved_positive"
    if t_upper < 0 and hl_upper < 0:
        return "resolved_negative"
    return "unresolved"


def t_only_classification(t_lower: float, t_upper: float, margin: float) -> str:
    """What a t-only rule (no rank estimator) would report -- used solely
    by self-test.py to demonstrate the combined rule's plateau safeguard;
    the real analyzer never calls this."""
    if t_lower >= -margin and t_upper <= margin:
        return "equivalent"
    if t_lower > 0:
        return "resolved_positive"
    if t_upper < 0:
        return "resolved_negative"
    return "unresolved"
