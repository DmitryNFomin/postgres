# v11 wait-event-tracing bare-metal evidence (2026-09-21, sanitized)

This branch publishes the privacy-sanitized evidence from the 2026-09-21
bare-metal benchmark of the PostgreSQL wait-event tracing v11 patch
series. It contains both complete raw archives and the derived analysis.

## What was measured

- Bench kit: commit `d75ba43a258` on branch `v11-notes`.
- Patched series under test: `wet-v11` at `96c24a28006`.
- Control build: `bench-v11-control` at `f25ba2b70d3`.
- Host class: 2-socket Intel Xeon, 64 cores, SMT off, turbo off; server
  and pgbench each pinned to 8 physical cores of one NUMA node; Rocky
  Linux 8; GCC 8.5.

Two stages:

1. **Matrix stage** (560 cells): a 16-block Latin square crossing five
   workloads (W1 microbenchmark, W3/W4/W5/W6c pgbench-driven) against
   seven configs per block (`master`, `master-aa`, `control`,
   `hook-null`, `module-off`, `stats`, `trace`).
2. **Crossover stage** (16 independent sessions): the W6c persistent-
   backend bracketed crossover, patched installation only, alternating
   `off`/`stats`/`trace` within each session to measure tool overhead
   without build-to-build pairing.

An early online A/A gate (paired master-vs-master-aa W4 half-width, no
more than 1% to continue) passed twice during the matrix run: **0.95%**
after 6 repetitions and **0.62%** after 10 repetitions.

## Downloads and key files

- [Sanitized matrix archive](artifacts/results-benchmark-host-20260921T223220Z-sanitized.tar.gz) / [sha256](artifacts/results-benchmark-host-20260921T223220Z-sanitized.tar.gz.sha256)
- [Sanitized crossover archive](artifacts/w6c-persistent-crossover-20260921T223345Z-sanitized.tar.gz) / [sha256](artifacts/w6c-persistent-crossover-20260921T223345Z-sanitized.tar.gz.sha256)
- [Matrix analysis](evidence-summary/matrix-analysis.md) ([JSON](evidence-summary/matrix-analysis.json))
- [Crossover analysis](evidence-summary/crossover-analysis.md) ([JSON](evidence-summary/crossover-analysis.json))
- [Sanitized host-check.txt](evidence-summary/host-check.txt)
- [Plateau probe result](evidence-summary/plateau-probe-result.json)
- [Early A/A gate log](evidence-summary/aa-early.jsonl)
- [Disassembly (build provenance)](evidence-summary/disassembly/)
- [Sanitization record](SANITIZATION.md) / [machine-readable](SANITIZATION.json)
- [Evidence manifest](EVIDENCE-MANIFEST.sha256)

## Headline results: matrix stage

Statistical labels use a paired 95% Student t interval AND an exact
Wilcoxon signed-rank Hodges-Lehmann 95% interval. A contrast is
'equivalent' only if both intervals lie inside the margin, 'resolved'
only if both exclude zero on the same side, and 'unresolved' otherwise.

## W1 (ns/iteration, margin +/-2)

A/A resolution floor (master-aa vs master): t half-width 0.9781, Hodges-
Lehmann half-width 1.031.

| Contrast | t mean | t 95% | HL est | HL 95% | Class |
|---|---:|---:|---:|---:|---|
| master-aa | 0.2534 | [-0.7248,1.232] | -0.05159 | [-0.6562,1.407] | equivalent |
| control | 0.3733 | [-0.608,1.355] | 0.4299 | [-0.6857,1.475] | equivalent |
| hook-null | 0.9601 | [0.05558,1.865] | 1.012 | [0.01056,1.911] | equivalent |
| module-off | 1.502 | [0.9011,2.104] | 1.517 | [0.8933,2.113] | resolved_positive |
| stats | 28.49 | [27.46,29.52] | 28.59 | [27.41,29.6] | resolved_positive |
| trace | 24.88 | [24.02,25.73] | 24.96 | [24,25.92] | resolved_positive |
| hook-null-minus-control | 0.5869 | [-0.1555,1.329] | 0.7241 | [-0.2131,1.311] | equivalent |
| module-off-minus-hook-null | 0.5423 | [-0.3641,1.449] | 0.485 | [-0.5272,1.472] | equivalent |
| stats-minus-module-off | 26.99 | [26.13,27.84] | 26.99 | [26.11,27.96] | resolved_positive |
| trace-minus-module-off | 23.38 | [22.56,24.19] | 23.43 | [22.52,24.35] | resolved_positive |

## W3 (log(tps), margin +/-0.0198)

A/A resolution floor (master-aa vs master): t half-width 0.01778,
Hodges-Lehmann half-width 0.02028.

| Contrast | t mean | t 95% | HL est | HL 95% | Class |
|---|---:|---:|---:|---:|---|
| master-aa | -0.007379 | [-0.02516,0.0104] | -0.01005 | [-0.02855,0.01201] | unresolved |
| control | 0.01453 | [-0.005394,0.03445] | 0.01567 | [-0.008481,0.03899] | unresolved |
| hook-null | 0.008238 | [-0.01003,0.0265] | 0.00786 | [-0.01183,0.02773] | unresolved |
| module-off | 0.01851 | [0.007088,0.02993] | 0.01523 | [0.01135,0.03006] | resolved_positive |
| stats | -0.04771 | [-0.06357,-0.03185] | -0.04814 | [-0.06349,-0.03133] | resolved_negative |
| trace | -0.04552 | [-0.06334,-0.0277] | -0.04382 | [-0.0605,-0.02744] | resolved_negative |
| hook-null-minus-control | -0.006288 | [-0.01579,0.003217] | -0.005068 | [-0.01286,0.001613] | equivalent |
| module-off-minus-hook-null | 0.01027 | [-0.0001127,0.02065] | 0.01106 | [0.001418,0.02106] | unresolved |
| stats-minus-module-off | -0.06622 | [-0.08101,-0.05143] | -0.06598 | [-0.08168,-0.05165] | resolved_negative |
| trace-minus-module-off | -0.06403 | [-0.08112,-0.04693] | -0.06297 | [-0.07702,-0.04579] | resolved_negative |

## W4 (log(tps), margin +/-0.0198)

A/A resolution floor (master-aa vs master): t half-width 0.00402,
Hodges-Lehmann half-width 0.00435.

| Contrast | t mean | t 95% | HL est | HL 95% | Class |
|---|---:|---:|---:|---:|---|
| master-aa | 0.003745 | [-0.0002747,0.007765] | 0.002961 | [-0.0006035,0.008096] | equivalent |
| control | 0.001315 | [-0.002912,0.005542] | 0.0005365 | [-0.003333,0.00594] | equivalent |
| hook-null | 0.006019 | [0.0007069,0.01133] | 0.006514 | [0.002889,0.01145] | equivalent |
| module-off | 0.008422 | [0.002163,0.01468] | 0.008595 | [0.002121,0.015] | equivalent |
| stats | 0.004615 | [-0.000856,0.01009] | 0.004858 | [-0.0003436,0.01002] | equivalent |
| trace | -0.004524 | [-0.01038,0.001337] | -0.005106 | [-0.01052,0.001232] | equivalent |
| hook-null-minus-control | 0.004704 | [-0.002561,0.01197] | 0.007938 | [-0.002706,0.01201] | equivalent |
| module-off-minus-hook-null | 0.002402 | [-0.004109,0.008913] | 0.002625 | [-0.003744,0.008768] | equivalent |
| stats-minus-module-off | -0.003807 | [-0.01028,0.002671] | -0.003957 | [-0.01074,0.002345] | equivalent |
| trace-minus-module-off | -0.01295 | [-0.02039,-0.005499] | -0.01338 | [-0.01961,-0.006394] | resolved_negative |

## W5 (log(tps), margin +/-0.0198)

A/A resolution floor (master-aa vs master): t half-width 0.003744,
Hodges-Lehmann half-width 0.003871.

| Contrast | t mean | t 95% | HL est | HL 95% | Class |
|---|---:|---:|---:|---:|---|
| master-aa | 0.002114 | [-0.00163,0.005858] | 0.001635 | [-0.001638,0.006103] | equivalent |
| control | 0.009784 | [0.00607,0.0135] | 0.009736 | [0.006481,0.01373] | equivalent |
| hook-null | 0.007203 | [0.002919,0.01149] | 0.007336 | [0.003019,0.0119] | equivalent |
| module-off | 0.008786 | [0.00582,0.01175] | 0.007911 | [0.005637,0.01204] | equivalent |
| stats | 0.00365 | [-1.013e-05,0.00731] | 0.003064 | [-0.000436,0.007507] | equivalent |
| trace | -0.003649 | [-0.008366,0.001068] | -0.003333 | [-0.008239,0.001376] | equivalent |
| hook-null-minus-control | -0.002581 | [-0.006755,0.001593] | -0.001375 | [-0.007607,0.0006526] | equivalent |
| module-off-minus-hook-null | 0.001583 | [-0.002091,0.005256] | 0.000369 | [-0.002096,0.005076] | equivalent |
| stats-minus-module-off | -0.005135 | [-0.007661,-0.00261] | -0.004715 | [-0.008099,-0.002755] | equivalent |
| trace-minus-module-off | -0.01243 | [-0.01595,-0.008918] | -0.01115 | [-0.01656,-0.008937] | equivalent |

## W6c (log(tps), margin +/-0.0198)

A/A resolution floor (master-aa vs master): t half-width 0.003984,
Hodges-Lehmann half-width 0.00439.

| Contrast | t mean | t 95% | HL est | HL 95% | Class |
|---|---:|---:|---:|---:|---|
| master-aa | -0.0002237 | [-0.004208,0.00376] | -0.0007591 | [-0.005117,0.003662] | equivalent |
| control | 0.009331 | [0.006253,0.01241] | 0.009205 | [0.0056,0.01256] | equivalent |
| hook-null | 0.00948 | [0.005908,0.01305] | 0.008468 | [0.005941,0.01357] | equivalent |
| module-off | 0.006634 | [0.002298,0.01097] | 0.005074 | [0.001957,0.01102] | equivalent |
| stats | 0.0002677 | [-0.004241,0.004777] | 0.0004562 | [-0.004628,0.005041] | equivalent |
| trace | -0.003515 | [-0.00785,0.00082] | -0.005184 | [-0.008138,0.001742] | equivalent |
| hook-null-minus-control | 0.0001487 | [-0.002387,0.002685] | 0.0002331 | [-0.002389,0.002756] | equivalent |
| module-off-minus-hook-null | -0.002846 | [-0.005252,-0.0004396] | -0.002786 | [-0.0053,-0.0004996] | equivalent |
| stats-minus-module-off | -0.006366 | [-0.01025,-0.00248] | -0.006586 | [-0.01031,-0.002128] | equivalent |
| trace-minus-module-off | -0.01015 | [-0.01275,-0.007545] | -0.01029 | [-0.0127,-0.008261] | equivalent |

Maximum sampled pgbench thread-capacity fraction across the matrix: 0.325.
Cells at or above 90%: 0.

## Headline results: crossover stage

Evidence validation: **PASS**. Statistical suitability: **PASS** (off/off
placebo and bracket-drift intervals are inside the predeclared margin).

| Contrast | Estimate | 95% CI | Classification |
|---|---:|---:|---|
| stats vs off | -0.491% | [-0.640%, -0.340%] | equivalent |
| trace vs off | -0.885% | [-0.977%, -0.792%] | equivalent |
| trace vs stats | -0.396% | [-0.554%, -0.238%] | equivalent |
| off/off placebo | -0.087% | [-0.259%, +0.085%] | equivalent |
| stats bracket drift | -0.316% | [-0.570%, -0.063%] | equivalent |
| trace bracket drift | -0.271% | [-0.634%, +0.094%] | equivalent |

16 sessions, independent-session analysis (no build pairing: every
session runs the patched installation).

## Per-function W1 table

Recomputed from `results/w1-detail/*.json` joined to `results/results.csv`
by `run_index`, paired over the 16 W1 blocks, 95% Student t interval
(ns/call; positive means the first config is slower):

| Contrast | file_read | latch_set | latch_timeout | report_only | usleep0 |
|---|---:|---:|---:|---:|---:|
| master-aa | +1.219 [-3.654,+6.092] | -0.015 [-0.090,+0.061] | +0.063 [-0.156,+0.283] | -0.000 [-0.003,+0.003] | -0.000 [-0.001,+0.001] |
| hook-null-minus-control | +2.559 [-1.123,+6.240] | +1.621 [+1.507,+1.735] | -1.244 [-1.614,-0.873] | -0.001 [-0.004,+0.001] | -0.001 [-0.002,+0.000] |
| module-off-minus-hook-null | +2.033 [-2.613,+6.679] | +0.093 [+0.012,+0.174] | +0.583 [+0.033,+1.133] | +0.002 [-0.000,+0.005] | +0.001 [-0.001,+0.002] |
| stats-minus-module-off | +64.668 [+60.302,+69.035] | +33.222 [+33.078,+33.365] | +37.061 [+36.187,+37.935] | -0.005 [-0.007,-0.002] | -0.001 [-0.002,+0.001] |
| trace-minus-module-off | +43.333 [+39.849,+46.817] | +34.742 [+34.633,+34.852] | +38.810 [+37.661,+39.958] | -0.005 [-0.008,-0.003] | -0.001 [-0.002,+0.000] |

`master-aa` is the A/A row (master-aa minus master). The other four rows
form the causal chain `control -> hook-null -> module-off -> {stats,
trace}`. `file_read` is noisy at the per-call level (wide A/A interval)
but the stats/trace overhead on it is still resolved and an order of
magnitude larger than its own noise floor; `latch_set` and
`latch_timeout` show clearly resolved stats/trace overhead of ~33-39
ns/call against a sub-ns/call noise floor; `report_only` and `usleep0`
show no resolvable overhead at any stage.

## Re-verifying this evidence

1. Download both sanitized archives and their `.sha256` sidecars from
   `artifacts/` and confirm the checksums.
2. Extract the matrix archive and run its own embedded kit copy against
   itself (this avoids depending on whatever state a local checkout of
   the kit happens to be in): `python3 kit/analyze-results.py . --output-json out.json --output-markdown out.md`
   from the extraction root. It should print `verification: PASS (560
   complete cells)` and `out.md` should match
   `evidence-summary/matrix-analysis.md`.
3. Extract the crossover archive, copy `evidence/worker`,
   `evidence/provenance`, and `evidence/tools` into a scratch directory
   (its `analyze.py` refuses to overwrite the `analysis.json`/`.md` that
   are already in the archive), and run
   `python3 tools/analyze.py /path/to/scratch`. It should reproduce
   `evidence-summary/crossover-analysis.md`.
4. See `SANITIZATION.md` for exactly what was changed and why the
   analysis JSON's manifest/host-check hash fields differ from what a
   from-scratch run would print.

Never start a PostgreSQL server or run the full kit against this
sanitized evidence expecting it to reproduce the raw per-transaction
logs bit-for-bit for anything beyond the numeric measurements listed
above -- process IDs and their derived digests were left untouched and
will differ from a fresh run by construction.
