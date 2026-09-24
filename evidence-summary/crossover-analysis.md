# Persistent-backend W6c crossover (second stage, patched only)

Evidence validation: **PASS**

Statistical suitability: **PASS** (off/off placebo and bracket-drift intervals are inside the predeclared margin)

## Tool overhead (patched installation, 16 sessions)

| Contrast | Estimate | 95% CI | Classification |
|---|---:|---:|---|
| stats vs off | -0.537% | [-0.664%, -0.410%] | equivalent |
| trace vs off | -1.030% | [-1.163%, -0.897%] | equivalent |
| trace vs stats | -0.495% | [-0.685%, -0.305%] | equivalent |
| off/off placebo | -0.142% | [-0.331%, +0.047%] | equivalent |
| stats bracket drift | -0.390% | [-0.709%, -0.069%] | equivalent |
| trace bracket drift | -0.267% | [-0.609%, +0.077%] | equivalent |

The confidence intervals use independent sessions as the unit of analysis (no build pairing: every session runs the patched installation). One-second rows are never treated as independent replicates.
