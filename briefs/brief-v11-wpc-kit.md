# WPC: v11 bare-metal benchmark package

Deliver one self-contained archive that a person copies to an idle Linux
bare-metal host and starts with one command. It measures every v11
configuration against vanilla in one randomized run, proves each
configuration's mode before timing it, and produces an analysis that
stays honest under the session-level plateaus seen in the v10 runs.

You extend the existing v10 kit; do not start from scratch. Start point:
`<workspace>/work/git/postgres_patch/v9_v10/notes-v10/bench-kit/`
(launcher `run-benchmark.sh`, `00-check-host.sh`, `01-build-all.sh`,
`02-run-matrix.sh`, `03-collect.sh`, `benchmark_protocol.py`,
`analyze-results.py`, `analyze-raw-archive.sh`, `self-test.py`,
`w3_qualification.py`, `cpu_affinity.py`, `wait-for-idle.sh`,
`workloads/`, `make-baremetal-package.sh`) and the crossover harness in
`<workspace>/work/git/postgres_patch/v9_v10/notes-v10/benchmarks/w6c-persistent-crossover/`.
Read both READMEs and the v10 runbook first. Copy them into
`<workspace>/work/git/postgres_patch/v11/bench-kit/` and work there.
Python 3.9+ stdlib only, bash, awk. No PostgreSQL server may be started
on this Mac; the kit's `self-test.py` uses synthetic data.

## Sources (filled in by the coordinator before launch)

| Build | Commit | Notes |
|---|---|---|
| baseline A and B | `<MASTER_SHA>` | postgres/postgres master, two independent builds, must be byte-identical |
| patched | `<V11_SHA>` | v11 series tip (five commits) |
| control | `<CONTROL_SHA>` | patched + `patches-control/*.patch` (timed sites compiled out) |

The package carries checksummed source snapshots plus the standalone
patches, as the v10 kit does, so the host needs no network.

## Configurations (seven, all in one matrix)

`master`, `master-aa`, `control`, `hook-null`, `module-off`, `stats`,
`trace`. Mode proofs, run once per cell before the timed window, from a
proof session on the same server; failure aborts the run:

- master, master-aa, control, hook-null: `SHOW shared_preload_libraries`
  is empty and `pg_extension` has no `pg_wait_event_tracing`.
- module-off: library in `shared_preload_libraries`, `SHOW
  pg_wait_event_tracing.capture` = off, `SELECT
  pg_wait_event_tracing_hooks_installed()` = false in the proof session.
- stats: capture = stats; after 2 s of load, `pg_stat_wait_event_timing`
  has rows for every pgbench backend PID.
- trace: as stats, plus `pg_get_wait_event_trace()` returns at least one
  wait record for a pgbench PID.

Server config per configuration: shared_buffers per workload as in v10;
`pg_wait_event_tracing.max_tranches = 192`,
`pg_wait_event_tracing.trace_ring_size = '4MB'` for stats/trace.

## Workloads

W1 (test_wait_primitive functions, 1e8 iterations, all five functions;
the fixture module from `bench-v8-baseline`'s
`src/test/modules/test_wait_primitive`, carried in the package and built
against each installation via PGXS), W3, W4, W5 (TPC-B, 16 clients, 4 GB),
W6c. Definitions, warm-up (10 s discarded) and measured window (30 s)
exactly as v10. W3 keeps its qualification step.

## Design

- **Randomized complete blocks.** One repetition = one block in which all
  seven configurations run once for a given workload, in an order from a
  7x7 Latin square chosen so that over 16 repetitions every configuration
  occupies every position at least twice. Fresh cluster per cell; the
  dataset is created once per block by the vanilla binaries and cloned per
  cell.
- **16 repetitions**, with an early A/A gate after repetitions 6 and 10:
  if the master vs master-aa paired 95% interval half-width exceeds 1.0%
  on W4, stop and report "host unsuitable" (raw data retained).
- **Plateau probe in preflight:** eight vanilla-only W6c sessions, four
  with the dataset clone and initdb pinned to the server's NUMA node via
  `numactl --cpunodebind=N --membind=N` (or `taskset` to the server mask
  if numactl is absent), four unpinned, alternating. Report both spreads.
  The matrix then uses the variant with the smaller spread and records
  which. If `numactl` is missing and the host has more than one NUMA
  node, warn but continue.
- **Covariates per cell**, recorded in the JSON row: per-CPU
  `scaling_cur_freq` sampled every 5 s during the window (mean, min,
  max), the postmaster's `/proc/<pid>/numa_maps` summary at the end of the
  window, `/proc/meminfo` Cached before and after, load average before,
  and the effective clock source (`SHOW timing_clock_source` plus one run
  of `pg_test_timing` per installation in preflight).
- **Second stage, crossover**: after the matrix, run the v10
  persistent-backend crossover unchanged in protocol but on the patched
  installation only (there is no v9/v10 pair now): 16 sessions, sequences
  A and B, stats/trace vs off with placebo. Reuse the existing harness
  code; adapt paths and remove the pair logic.
- **CPU affinity**: configurable `SERVER_CPUS`/`PGBENCH_CPUS` with the
  same runtime verification as r3, but no longer hard-coded to one
  topology. The host check prints the topology and refuses to run if the
  two masks share a physical core or if either is empty.

## Analysis (`analyze-results.py`)

For every workload and every contrast below, on log(TPS) (or ns/iter
for W1):

- Contrasts against vanilla: control, hook-null, module-off, stats, trace
  each minus master; master-aa minus master (the A/A floor).
- Contrasts against the layout control: hook-null minus control.
- Contrasts within the patched binary: module-off minus hook-null, stats
  minus module-off, trace minus module-off.

Two estimators, both reported, both paired by block:

1. Student t 95% interval of the mean paired difference.
2. Hodges-Lehmann estimate with a 95% interval from the Wilcoxon
   signed-rank distribution (exact for n <= 16; implement it, stdlib
   only).

Classification per contrast: "equivalent" if BOTH intervals lie inside
the predeclared margin (W1: +/-2 ns; pgbench: +/-2%); "slower"/"faster" if
both exclude zero on the same side; otherwise "unresolved". Print the
A/A half-width next to every table as the run's resolution floor. Add a
per-block diagnostics table: block index, order position of each
configuration, TPS, mean frequency, NUMA local fraction, so plateaus are
visible. Output `analysis.md` (72 columns, mailing-list ready) and
`analysis.json`.

## Package

`make-baremetal-package.sh` produces `wet-v11-baremetal-r1.tar.gz` and
`.sha256`, with a manifest of every file hash, as v10 did. Runbook
`BAREMETAL-RUNBOOK-v11.md`: same shape as v10's, updated for seven
configurations, the plateau probe, the two-stage run, the 8 to 10 hour
duration, and the two result files to send back. The launcher is
fail-closed exactly as before: package verification, synthetic
self-test of the analysis on generated data (including a generated
plateau pattern that must be classified "unresolved" by the rank
estimator when the t estimator alone would call it), idle wait, host
check, builds, smoke matrix, full matrix, crossover, collect.

## Verification here

`bash -n` on every script, `shellcheck` if installed, `python3 -m
py_compile`, and `python3 self-test.py` must pass. Run
`make-baremetal-package.sh` and verify the archive lists and checksums.
No server, no real benchmark.

## Deliverable

`<workspace>/work/git/postgres_patch/v11/reports/wpc-report.md`:
what was reused from v10 unchanged, what changed and why, the self-test
output, the package path and SHA-256, and anything not done. Final chat
message at most 8 lines.

## Launch note (coordinator, 2026-09-18)

The three commit hashes are not final yet. Put them in ONE place, a
`sources.conf` (or equivalent) read by every script, with the literal
placeholders `<MASTER_SHA>`, `<V11_SHA>`, `<CONTROL_SHA>`, and make the
launcher refuse to run while any placeholder remains. Develop and
self-test everything else now. Do NOT run `make-baremetal-package.sh` for
real; instead make sure it works with a `--dry-run` that lists what it
would snapshot. The coordinator fills the hashes and packages afterwards.
The control patch to carry is
`<workspace>/work/git/postgres_patch/v11/patches-control/0001-*.patch`;
the v11 series patches will be the five files in
`<workspace>/work/git/postgres_patch/v11/patches-v11/` (they may be
regenerated with the same names before packaging; reference them by glob).
The module's diagnostic function is `pg_wait_event_tracing_hooks_installed()`
(exists in the v11 series). The test_wait_primitive fixture source is in
branch `fork/bench-v8-baseline` of <workspace>/work/git/postgres
under `src/test/modules/test_wait_primitive/` (read with `git show`, do
not check anything out there).
