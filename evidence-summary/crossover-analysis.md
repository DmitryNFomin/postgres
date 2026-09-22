# Persistent-backend W6c crossover (second stage, patched only)

Evidence validation: **PASS**

Statistical suitability: **PASS** (off/off placebo and bracket-drift intervals are inside the predeclared margin)

## Tool overhead (patched installation, 16 sessions)

| Contrast | Estimate | 95% CI | Classification |
|---|---:|---:|---|
| stats vs off | -0.491% | [-0.640%, -0.340%] | equivalent |
| trace vs off | -0.885% | [-0.977%, -0.792%] | equivalent |
| trace vs stats | -0.396% | [-0.554%, -0.238%] | equivalent |
| off/off placebo | -0.087% | [-0.259%, +0.085%] | equivalent |
| stats bracket drift | -0.316% | [-0.570%, -0.063%] | equivalent |
| trace bracket drift | -0.271% | [-0.634%, +0.094%] | equivalent |

The confidence intervals use independent sessions as the unit of analysis (no build pairing: every session runs the patched installation). One-second rows are never treated as independent replicates.
