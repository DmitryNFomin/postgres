# Persistent-backend W6c crossover

Evidence validation: **PASS**

Statistical suitability: **FAIL** (a placebo or bracket-drift interval exceeds the predeclared margin)

## Tool overhead within each build

| Build | Contrast | Estimate | 95% CI | Classification |
|---|---|---:|---:|---|
| v9 | stats vs off | -0.190% | [-0.469%, +0.090%] | equivalent |
| v9 | trace vs off | -0.459% | [-0.737%, -0.180%] | equivalent |
| v9 | off/off placebo | -0.302% | [-0.832%, +0.231%] | equivalent |
| v9 | stats bracket drift | -0.636% | [-1.092%, -0.179%] | slower |
| v9 | trace bracket drift | -0.772% | [-1.487%, -0.052%] | slower |
| v10 | stats vs off | -0.024% | [-0.292%, +0.245%] | equivalent |
| v10 | trace vs off | -0.211% | [-0.612%, +0.192%] | equivalent |
| v10 | off/off placebo | -0.232% | [-0.557%, +0.095%] | equivalent |
| v10 | stats bracket drift | -0.468% | [-1.063%, +0.130%] | unresolved |
| v10 | trace bracket drift | -1.115% | [-1.682%, -0.545%] | slower |

## V10 minus v9

| Contrast | Estimate | 95% CI | Classification |
|---|---:|---:|---|
| stats overhead change | +0.167% | [-0.193%, +0.528%] | equivalent |
| trace overhead change | +0.250% | [-0.276%, +0.778%] | equivalent |
| off throughput change | -1.102% | [-2.541%, +0.358%] | unresolved |
| stats throughput change | -0.793% | [-2.413%, +0.855%] | unresolved |
| trace throughput change | -1.000% | [-2.447%, +0.469%] | unresolved |

The confidence intervals use sessions or adjacent v9/v10 session pairs as independent observations. One-second rows are never treated as independent replicates.
