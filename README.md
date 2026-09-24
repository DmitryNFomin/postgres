# v11 wait-event-tracing bare-metal evidence (2026-09-23, sanitized, run 2)

This branch publishes the privacy-sanitized evidence from the 2026-09-23
bare-metal benchmark of the PostgreSQL wait-event tracing v11 patch
series, run **after** the deferred-accounting change (per-wait
accounting moved out of the critical section) landed in patch 0004. It
is the second of two matrix-stage evidence runs; the first (before
deferral) is published on `v11-evidence-20260921`.

## What was measured

- Bench kit: commit `19fd9b6d818` on branch `v11-notes`; the package
  actually installed and run on the host was release
  `v11-kit-r2-20260922`.
- Patched series under test: series tip `e26e6333322` (includes the
  deferred-accounting change in patch 0004).
- Control build: `bench-v11-control` at `7e6deb83597`.
- Host class: 2-socket Intel Xeon server, SMT off, turbo off; server
  pinned to physical cores `1-15:2`, pgbench pinned to physical cores
  `17-31:2` (one NUMA node each); Rocky Linux 8; GCC 8.5.

Two stages:

1. **Matrix stage** (560 cells): a 16-block Latin square crossing five
   workloads (W1 microbenchmark, W3/W4/W5/W6c pgbench-driven) against
   seven configs per block (`master`, `master-aa`, `control`,
   `hook-null`, `module-off`, `stats`, `trace`).
2. **Crossover stage** (16 independent sessions): the W6c persistent-
   backend bracketed crossover, patched installation only, alternating
   `off`/`stats`/`trace` within each session to measure tool overhead
   without build-to-build pairing. Run 2's crossover is present on this
   branch (added in a follow-up commit after the matrix stage was
   published; it was not available when the branch was first built).

An early online A/A gate (paired master-vs-master-aa W4 half-width, no
more than 1% to continue) passed twice during the matrix run: **0.82%**
after 6 repetitions and **0.81%** after 10 repetitions.

## Downloads and key files

- [Sanitized matrix archive](artifacts/results-benchmark-host-20260923T201507Z-sanitized.tar.gz) / [sha256](artifacts/results-benchmark-host-20260923T201507Z-sanitized.tar.gz.sha256)
- [Sanitized crossover archive](artifacts/w6c-persistent-crossover-20260923T201633Z-sanitized.tar.gz) / [sha256](artifacts/w6c-persistent-crossover-20260923T201633Z-sanitized.tar.gz.sha256)
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

Host isolation: dedicated (0 pre-existing PostgreSQL processes). Plateau
probe selected the pinned dataset-clone/initdb variant (pinned spread
0.444%, unpinned spread 0.568%).

## W1 (ns/iteration, margin +/-2)

A/A resolution floor (master-aa vs master): t half-width 0.8365, Hodges-
Lehmann half-width 0.8717.

| Contrast | t mean | t 95% | HL est | HL 95% | Class |
|---|---:|---:|---:|---:|---|
| master-aa | -0.1838 | [-1.02,0.6527] | -0.3695 | [-1.006,0.7379] | equivalent |
| control | -0.4675 | [-1.383,0.4477] | -0.3642 | [-1.453,0.4938] | equivalent |
| hook-null | 0.5491 | [0.03423,1.064] | 0.5577 | [-0.01378,1.133] | equivalent |
| module-off | 0.5216 | [-0.2115,1.255] | 0.5739 | [-0.2707,1.308] | equivalent |
| stats | 28.86 | [28.14,29.58] | 28.81 | [28.1,29.68] | resolved_positive |
| trace | 25.55 | [24.84,26.27] | 25.45 | [24.85,26.3] | resolved_positive |
| hook-null-minus-control | 1.017 | [-0.02452,2.058] | 1.114 | [-0.07443,2.058] | unresolved |
| module-off-minus-hook-null | -0.02757 | [-0.7962,0.741] | -0.2376 | [-0.8138,0.7008] | equivalent |
| stats-minus-module-off | 28.34 | [27.54,29.14] | 28.13 | [27.55,29.25] | resolved_positive |
| trace-minus-module-off | 25.03 | [24.5,25.56] | 25.14 | [24.55,25.6] | resolved_positive |

## W3 (log(tps), margin +/-0.0198)

A/A resolution floor (master-aa vs master): t half-width 0.02302,
Hodges-Lehmann half-width 0.02413.

| Contrast | t mean | t 95% | HL est | HL 95% | Class |
|---|---:|---:|---:|---:|---|
| master-aa | -0.009055 | [-0.03207,0.01397] | -0.006914 | [-0.03427,0.01398] | unresolved |
| control | 0.01837 | [0.004271,0.03246] | 0.01661 | [0.009507,0.02657] | resolved_positive |
| hook-null | 0.02612 | [0.0106,0.04165] | 0.02744 | [0.01184,0.03959] | resolved_positive |
| module-off | 0.004002 | [-0.01937,0.02737] | 0.005873 | [-0.009892,0.0215] | unresolved |
| stats | -0.03298 | [-0.0582,-0.007761] | -0.03434 | [-0.05978,-0.008046] | resolved_negative |
| trace | -0.03052 | [-0.04772,-0.01331] | -0.03263 | [-0.04941,-0.01312] | resolved_negative |
| hook-null-minus-control | 0.007756 | [-0.004154,0.01967] | 0.009947 | [-0.00117,0.01946] | equivalent |
| module-off-minus-hook-null | -0.02212 | [-0.0467,0.002462] | -0.01422 | [-0.03563,0.0009935] | unresolved |
| stats-minus-module-off | -0.03698 | [-0.0643,-0.009664] | -0.03952 | [-0.06421,-0.01242] | resolved_negative |
| trace-minus-module-off | -0.03452 | [-0.05721,-0.01182] | -0.03859 | [-0.05529,-0.01637] | resolved_negative |

## W4 (log(tps), margin +/-0.0198)

A/A resolution floor (master-aa vs master): t half-width 0.005053,
Hodges-Lehmann half-width 0.004046.

| Contrast | t mean | t 95% | HL est | HL 95% | Class |
|---|---:|---:|---:|---:|---|
| master-aa | -0.001254 | [-0.006308,0.003799] | -0.0008212 | [-0.004879,0.003212] | equivalent |
| control | -0.0004271 | [-0.003522,0.002668] | -0.0007173 | [-0.003956,0.002707] | equivalent |
| hook-null | 0.002141 | [-0.003205,0.007486] | 0.002601 | [-0.00329,0.007757] | equivalent |
| module-off | 0.001432 | [-0.003844,0.006708] | 0.001308 | [-0.004483,0.006547] | equivalent |
| stats | 0.0008901 | [-0.003754,0.005534] | 0.00173 | [-0.003679,0.005983] | equivalent |
| trace | -0.01 | [-0.01288,-0.00712] | -0.009496 | [-0.01259,-0.007418] | equivalent |
| hook-null-minus-control | 0.002568 | [-0.003144,0.00828] | 0.004856 | [-0.004825,0.008441] | equivalent |
| module-off-minus-hook-null | -0.0007091 | [-0.00571,0.004292] | -0.00108 | [-0.00613,0.003792] | equivalent |
| stats-minus-module-off | -0.0005417 | [-0.005757,0.004674] | -0.0007373 | [-0.005711,0.004806] | equivalent |
| trace-minus-module-off | -0.01143 | [-0.01664,-0.006224] | -0.01141 | [-0.01683,-0.00556] | equivalent |

## W5 (log(tps), margin +/-0.0198)

A/A resolution floor (master-aa vs master): t half-width 0.002855,
Hodges-Lehmann half-width 0.002763.

| Contrast | t mean | t 95% | HL est | HL 95% | Class |
|---|---:|---:|---:|---:|---|
| master-aa | 0.002469 | [-0.0003855,0.005324] | 0.001591 | [-0.0006626,0.004863] | equivalent |
| control | 0.009992 | [0.006493,0.01349] | 0.009388 | [0.00662,0.01338] | equivalent |
| hook-null | 0.006037 | [0.002804,0.00927] | 0.006871 | [0.0052,0.008582] | equivalent |
| module-off | 0.005472 | [0.00193,0.009013] | 0.006815 | [0.001473,0.008948] | equivalent |
| stats | 0.002372 | [-0.001585,0.006329] | 0.003038 | [-0.001342,0.005734] | equivalent |
| trace | -0.003303 | [-0.006841,0.0002341] | -0.00413 | [-0.0059,-0.0006831] | equivalent |
| hook-null-minus-control | -0.003955 | [-0.007208,-0.0007027] | -0.003049 | [-0.007156,-0.001493] | equivalent |
| module-off-minus-hook-null | -0.0005652 | [-0.005274,0.004143] | -0.0004294 | [-0.005196,0.002265] | equivalent |
| stats-minus-module-off | -0.0031 | [-0.006449,0.000248] | -0.003503 | [-0.006812,0.0001211] | equivalent |
| trace-minus-module-off | -0.008775 | [-0.01314,-0.004415] | -0.009695 | [-0.01354,-0.004487] | equivalent |

## W6c (log(tps), margin +/-0.0198)

A/A resolution floor (master-aa vs master): t half-width 0.002403,
Hodges-Lehmann half-width 0.002278.

| Contrast | t mean | t 95% | HL est | HL 95% | Class |
|---|---:|---:|---:|---:|---|
| master-aa | 0.001419 | [-0.0009847,0.003822] | 0.0009416 | [-0.001057,0.0035] | equivalent |
| control | 0.00611 | [0.002309,0.009911] | 0.006077 | [0.002173,0.009716] | equivalent |
| hook-null | 0.005433 | [0.001828,0.009037] | 0.006045 | [0.001622,0.009058] | equivalent |
| module-off | 0.004393 | [0.0007062,0.008079] | 0.004426 | [0.0003699,0.008368] | equivalent |
| stats | -0.003514 | [-0.006562,-0.0004658] | -0.003943 | [-0.006873,-0.0004387] | equivalent |
| trace | -0.005387 | [-0.008204,-0.002571] | -0.005761 | [-0.008103,-0.00282] | equivalent |
| hook-null-minus-control | -0.0006774 | [-0.005003,0.003648] | -0.000966 | [-0.0042,0.00373] | equivalent |
| module-off-minus-hook-null | -0.00104 | [-0.004778,0.002698] | -0.001225 | [-0.004888,0.002865] | equivalent |
| stats-minus-module-off | -0.007907 | [-0.01184,-0.003975] | -0.008904 | [-0.01228,-0.003775] | equivalent |
| trace-minus-module-off | -0.00978 | [-0.01309,-0.006471] | -0.009826 | [-0.01311,-0.006558] | equivalent |

Maximum sampled pgbench thread-capacity fraction across the matrix:
0.326. Cells at or above 90%: 0.

## Headline results: crossover stage

Evidence validation: **PASS**. Statistical suitability: **PASS** (off/off
placebo and bracket-drift intervals are inside the predeclared margin).
Run 2's crossover is present on this branch.

| Contrast | Estimate | 95% CI | Classification |
|---|---:|---:|---|
| stats vs off | -0.537% | [-0.664%, -0.410%] | equivalent |
| trace vs off | -1.030% | [-1.163%, -0.897%] | equivalent |
| trace vs stats | -0.495% | [-0.685%, -0.305%] | equivalent |
| off/off placebo | -0.142% | [-0.331%, +0.047%] | equivalent |
| stats bracket drift | -0.390% | [-0.709%, -0.069%] | equivalent |
| trace bracket drift | -0.267% | [-0.609%, +0.077%] | equivalent |

16 sessions, independent-session analysis (no build pairing: every
session runs the patched installation).

## Run 1 vs run 2 comparison

Run 1 (2026-09-21, before deferral) is published on
`v11-evidence-20260921`; run 2 (this branch) is after the
deferred-accounting change in patch 0004. Numbers below are exactly the
run-1/run-2 pair from the coordinator's cover-letter appendix
(`brief-v11-wpl-cover-letter.md`).

**W1, already-set latch, ns/call, paired 95% (run 2 (run 1)):**

| Contrast | Run 2 | Run 1 |
|---|---:|---:|
| hook-null minus control | +1.63 [+1.48, +1.77] | +1.62 [+1.51, +1.74] |
| module-off minus hook-null | -0.05 [-0.17, +0.06] | +0.09 [+0.01, +0.17] |
| stats minus module-off | +36.85 [+36.75, +36.94] | +33.22 |
| trace minus module-off | +38.35 [+38.26, +38.44] | +34.74 |

**pgbench, percent, paired 95%, run 2 (run 1):**

| Workload | stats vs module-off | trace vs module-off | A/A floor | hook-null vs control |
|---|---:|---:|---:|---:|
| W3 | -3.7 [-6.4, -1.0] (-6.6 [-8.1, -5.1]) | -3.5 [-5.7, -1.2] (-6.4 [-8.1, -4.7]) | 2.3 (1.8) | +0.8 [-0.4, +2.0] |
| W4 | -0.05 [-0.58, +0.47] (-0.4 [-1.0, +0.3]) | -1.1 [-1.7, -0.6] (-1.3 [-2.0, -0.5]) | 0.51 (0.40) | +0.26 [-0.31, +0.83] |
| W5 | -0.31 [-0.64, +0.02] (-0.5 [-0.8, -0.3]) | -0.88 [-1.31, -0.44] (-1.2 [-1.6, -0.9]) | 0.29 (0.37) | -0.40 [-0.72, -0.07] |
| W6c | -0.79 [-1.18, -0.40] (-0.6 [-1.0, -0.2]) | -0.98 [-1.31, -0.65] (-1.0 [-1.3, -0.8]) | 0.24 (0.40) | -0.07 [-0.50, +0.36] |

W3 lock-wait rate: 406k/s (run 2), 408k/s (run 1), ~5 per transaction.

Crossover (W6c, 16 persistent sessions, run 2 (run 1)): stats vs off
-0.54 [-0.66, -0.41] (-0.49 [-0.64, -0.34]), trace vs off -1.03 [-1.16,
-0.90] (-0.89 [-0.98, -0.79]), placebo -0.14 [-0.33, +0.05] (-0.09
[-0.26, +0.09]). Run 2's crossover is present.

Deferral roughly halved the W3 overhead of `stats`/`trace` relative to
`module-off` (run 1's worst case, about -6.5% median across estimators,
to run 2's about -3.6%), at the cost of +2 to +4 ns/call more W1
per-call overhead in `stats`/`trace` (deferred per-wait accounting adds
work outside the critical section).

## Per-function W1 table

Recomputed from this branch's own `results/w1-detail/*.json` joined to
`results/results.csv` by `run_index`, paired over the 16 W1 blocks, 95%
Student t interval (ns/call; positive means the first config is
slower):

| Contrast | file_read | latch_set | latch_timeout | report_only | usleep0 |
|---|---:|---:|---:|---:|---:|
| master-aa | -0.985 [-5.139,+3.169] | -0.016 [-0.091,+0.059] | +0.081 [-0.154,+0.316] | +0.001 [-0.003,+0.004] | -0.000 [-0.001,+0.001] |
| hook-null-minus-control | +4.721 [-0.408,+9.849] | +1.626 [+1.482,+1.770] | -1.263 [-1.890,-0.636] | -0.001 [-0.004,+0.003] | +0.000 [-0.001,+0.001] |
| module-off-minus-hook-null | -0.329 [-4.106,+3.449] | -0.052 [-0.166,+0.063] | +0.240 [-0.329,+0.809] | +0.002 [-0.001,+0.006] | -0.001 [-0.002,+0.001] |
| stats-minus-module-off | +68.368 [+64.619,+72.118] | +36.846 [+36.747,+36.944] | +36.481 [+35.310,+37.652] | -0.006 [-0.008,-0.003] | +0.000 [-0.001,+0.001] |
| trace-minus-module-off | +47.565 [+45.120,+50.010] | +38.352 [+38.260,+38.444] | +39.241 [+37.928,+40.553] | -0.006 [-0.008,-0.003] | -0.000 [-0.001,+0.000] |

`master-aa` is the A/A row (master-aa minus master). The other four rows
form the causal chain `control -> hook-null -> module-off -> {stats,
trace}`. `latch_set` and `latch_timeout` show clearly resolved
stats/trace overhead of ~36-39 ns/call against a sub-ns/call noise
floor, consistent with run 1; `report_only` and `usleep0` show no
resolvable overhead at any stage, also consistent with run 1.

## Re-verifying this evidence

1. Download the sanitized matrix archive and its `.sha256` sidecar from
   `artifacts/` and confirm the checksum.
2. Extract it and run its own embedded kit copy against itself (this
   avoids depending on whatever state a local checkout of the kit
   happens to be in): `python3 kit/analyze-results.py . --output-json
   out.json --output-markdown out.md` from the extraction root. It
   should print `verification: PASS (560 complete cells)` and `out.md`
   should match `evidence-summary/matrix-analysis.md`.
3. Download the sanitized crossover archive and its `.sha256` sidecar
   the same way. Extract it, copy `evidence/worker`,
   `evidence/provenance`, and `evidence/tools` into a scratch directory
   (its `analyze.py` refuses to overwrite the `analysis.json`/`.md`
   that are already in the archive), and run `python3
   tools/analyze.py /path/to/scratch`. It should reproduce
   `evidence-summary/crossover-analysis.md`.
4. See `SANITIZATION.md` for exactly what was changed and why the
   analysis JSON's manifest/host-check hash fields differ from what a
   from-scratch run would print.

Never start a PostgreSQL server or run the full kit against this
sanitized evidence expecting it to reproduce the raw per-transaction
logs bit-for-bit for anything beyond the numeric measurements listed
above -- process IDs were left untouched and will differ from a fresh
run by construction.
