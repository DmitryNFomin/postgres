# WPC: v11 bare-metal benchmark package — report

Kit: `<workspace>/work/git/postgres_patch/v11/bench-kit/`
Brief: `<workspace>/work/git/postgres_patch/v11/briefs/brief-v11-wpc-kit.md`

## Summary

The v10 kit (`v9_v10/notes-v10/bench-kit/`) and crossover harness
(`v9_v10/notes-v10/benchmarks/w6c-persistent-crossover/`) were copied into
`v11/bench-kit/` (crossover under `v11/bench-kit/crossover/`) and extended
to the v11 protocol: seven configurations against vanilla in one matrix,
16 repetitions on a 7x7 Latin square, mode proofs, a plateau probe, per-cell
covariates, a combined t + Wilcoxon/Hodges-Lehmann analysis, configurable
CPU affinity, and a `sources.conf` gate for the three commit hashes the
coordinator has not filled in yet. No PostgreSQL server was started at any
point (`self-test.py` uses only generated data); no git worktree or
checkout was modified (only read-only `git show`/`git ls-tree`/`git
rev-parse` against `<workspace>/work/git/postgres`, never checked
out).

## Reused from v10 unchanged (in substance)

- Overall shape of the launcher (`run-benchmark.sh`): fail-closed phases,
  lock file, heartbeat logging, `--status`/`--preflight-only`.
- `00-check-host.sh` structure outside the CPU-affinity section (tool
  checks, memory/swap/disk, load average, virtualization, co-resident
  PostgreSQL inventory).
- `wait-for-idle.sh` unchanged.
- `w3_qualification.py` unchanged (W3 short-`ProcArray`-LWLock thresholds
  and the exact TSV/histogram recomputation logic).
- The pgbench measurement method in `02-run-matrix.sh` (continuous
  warmup+duration run, post-warmup progress parsing, saturation check) and
  the recording-proof approach (`workloads/recording-proof.sql`,
  `workloads/w3-qualification.sql` — these already used `pg_stat_wait_
  event_timing`/`pg_get_wait_event_trace(procnumber)`, matching v11's
  naming exactly, so they needed no changes).
- The fail-closed archive/package integrity pattern (`03-collect.sh`,
  `analyze-raw-archive.sh`, `make-baremetal-package.sh`): staged manifest
  hashing, clean-room re-extraction and re-verification.
- The crossover's bracketed 8-block sequence design (off/stats/stats/off/
  off/trace/trace/off, and its mirror), placebo/bracket-drift margins, and
  per-block extraction/validation pipeline (`extract_blocks.py`,
  `analyze.py`'s validation chain).

## What changed, and why

- **`sources.conf` / `sources_conf.py`** (new): the one place `MASTER_SHA`,
  `V11_SHA`, `CONTROL_SHA` live, plus the source repo, patch globs, fixture
  branch/path, and module/function names. `run-benchmark.sh`,
  `01-build-all.sh`, and `make-baremetal-package.sh` all call
  `sources_conf.py check` and refuse to run while a placeholder remains
  (verified: `./run-benchmark.sh` prints the exact refusal and exits
  nonzero before touching the host).
- **Seven configurations, five workloads** (`benchmark_protocol.py`):
  `master`, `master-aa`, `control`, `hook-null`, `module-off`, `stats`,
  `trace` against `master`; `hook-null` against `control`; and
  `module-off`/`stats`/`trace` against `hook-null`/`module-off` within the
  patched binary. W1 now runs all five `test_wait_primitive` functions per
  cell (combined into one ns/iteration figure, with per-function detail
  kept in `results/w1-detail/`). W5 (TPC-B, no `-S`) was added alongside
  W1/W3/W4/W6c.
  W1's fixture is carried as a snapshot taken with `git show` from
  `fork/bench-v8-baseline` (never checked out), stored at
  `bench-kit/fixture-src/test_wait_primitive/`, and overlaid onto each of
  the four builds by `01-build-all.sh`.
- **Four builds, not ten**: `baseline-a`, `baseline-b`, `patched` (v11
  series), `control` (patched + `patches-control/0001-*.patch`).
  `01-build-all.sh` and `analyze-results.py`'s manifest verification were
  rewritten around this; the control-vs-patched postgres-binary difference
  is asserted directly instead of the old v9/v10 module-difference check.
- **16 repetitions, 7x7 Latin-square schedule** (`latin_square.py`, new):
  cyclic Latin square with randomized base permutation and randomized
  choice of which rows get a third repetition, guaranteeing every
  configuration occupies every schedule position at least twice over 16
  reps. `analyze-results.py` re-verifies this property from the recorded
  schedule using the same module. Verified for 200 random seeds plus the
  self-test's fixed seed.
- **Early A/A gate moved to reps 6 and 10** on W4 only, testing the paired
  95% t half-width against a 1.0% limit (`02-run-matrix.sh`
  `check_early_aa`); failure reports "host unsuitable" and retains raw
  data, matching the brief exactly (was reps 1/3/6/9 with CV+bias checks
  in v10).
- **Plateau probe** (`plateau-probe.sh`, new): eight vanilla-only W6c
  sessions, four with dataset-clone/initdb pinned via `numactl
  --cpunodebind=N --membind=N` (falling back to `taskset` if `numactl` is
  absent — a non-blocking warning, not a `warn()` that halts the host
  check), four unpinned, alternating; writes
  `plateau-probe-result.json` recording both spreads and the selected
  variant, which `02-run-matrix.sh` reads to decide whether to wrap
  initdb/dataset-clone in `NUMA_WRAP`.
- **Covariates per cell**: `02-run-matrix.sh` now records, as extra CSV
  columns, per-CPU frequency mean/min/max sampled every 5s during the
  window, the postmaster's `numa_maps`-derived local-page fraction,
  `/proc/meminfo` Cached before/after, load average before, and
  `SHOW timing_clock_source`; `pg_test_timing` is captured once per
  installation in preflight.
- **Configurable CPU affinity**: `cpu_affinity.py` was rewritten to read
  `SERVER_CPUS`/`PGBENCH_CPUS` from the environment instead of asserting
  one fixed two-socket/64-core topology; it now fails closed only on
  empty/overlapping masks or a shared physical core (via
  `lscpu -p=CPU,CORE,SOCKET,NODE`), and `00-check-host.sh` prints whatever
  topology it finds rather than asserting a fixed shape.
- **Statistics** (`wilcoxon.py`, new, stdlib only): exact Wilcoxon
  signed-rank distribution via an O(n^2) subset-sum DP (verified against
  brute-force enumeration over 200 random no-tie cases and against a
  Monte Carlo coverage check of the Hodges-Lehmann interval, ~95% at n=10);
  falls back to a normal approximation with tie correction when `|d_i|`
  ties are present. `analyze-results.py` reports both a paired Student t
  interval and this Hodges-Lehmann interval for every contrast, on
  log(TPS) for pgbench workloads (ns/iteration for W1), and classifies a
  contrast as "equivalent" only if both intervals lie inside the margin,
  "resolved" only if both exclude zero on the same side, and "unresolved"
  otherwise. A per-block diagnostics table (one row per cell: block,
  schedule position, config, metric, mean frequency, NUMA-local fraction)
  is included in `analysis.md`/`analysis.json` specifically to make
  session-level plateaus visible.
- **Second-stage crossover** (`crossover/`): rewritten to drop all
  `v9`/`v10` pairing. `protocol.py`/`generate_schedule.py` now describe 16
  independent sessions balanced 8 sequence-A/8 sequence-B (no
  `pair_index`/`pair_position`/`build` fields); each session clones its own
  neutral dataset directly (no shared pair fixture); `extract_blocks.py`,
  `analyze.py`, `verify_installs.py`, and `host_state.py` were updated to
  match (single "patched" install, one `overhead` block instead of
  `by_build`/`v10_vs_v9`, and topology-agnostic host-state validation
  paired with `SERVER_CPUS`/`PGBENCH_CPUS` inherited from the same
  environment as the matrix stage). `run.sh`/`run-worker.sh` no longer pin
  a hardcoded expected package/build-manifest SHA-256 (that value cannot
  be known before the coordinator fills in `sources.conf`); they instead
  hash-bind whatever kit is present into the crossover's own
  `protocol.json`, the same pattern the matrix stage already used.
  `run-benchmark.sh` runs the crossover's own smoke test and then the full
  run as the final two phases, after the 560-cell matrix.
- **`make-baremetal-package.sh` `--dry-run`**: added per the Launch note.
  It lists every kit/workload/runbook/crossover file, resolves and lists
  the control and v11-series patches by their configured glob, lists the
  bundled fixture snapshot, and prints the configured (still-placeholder)
  commit hashes — all without touching git or writing any file. Verified:
  `./make-baremetal-package.sh --dry-run` runs clean with the placeholders
  still in `sources.conf` (real packaging is refused separately, before
  reaching any git command, by the `sources_conf.py check` call in the
  non-dry-run path).
- Portability fix unrelated to the protocol: `COPYFILE_DISABLE=1` was
  added to `03-collect.sh`/`make-baremetal-package.sh` so `tar` does not
  emit macOS AppleDouble (`._*`) sidecar files into the archives during
  development/self-test on this Mac; a no-op on the Linux target.

## Self-test output

```
$ python3 self-test.py            # bench-kit/
self-test: PASS (560 valid synthetic cells and seven-configuration
contrasts verified; CPU-affinity, results/matrix-complete, manifest,
mode-proof, bound-kit-file, W3, and early-A/A-gate tampering rejected;
synthetic plateau scenario confirmed 'unresolved' under the combined
t + Wilcoxon/HL rule even though the t interval alone would call it
resolved)

$ python3 self-test.py            # bench-kit/crossover/
w6c-persistent-crossover self-test: PASS
```

The plateau scenario (both a direct unit check in
`test_plateau_scenario_unit()` and an end-to-end check that seeds the
trace-vs-master W6c contrast in a synthetic 560-cell tree and re-derives
the classification through the real `analyze-results.py`) uses 16 paired
differences (7 at +6.0, 9 at −1.0 in log(TPS) units) verified in
development to give a paired-t 95% interval of `[0.152, 3.973]` (excludes
zero: a t-only rule would call it "resolved_positive") while the exact
Wilcoxon/Hodges-Lehmann 95% interval is `[−1.0, 2.5]` (spans zero), so the
combined rule reports "unresolved."

`self-test.py` also runs `03-collect.sh` and `analyze-raw-archive.sh`
end-to-end against the synthetic tree (clean-room archive re-verification
and a passing local analysis), and statically checks every `*.sh` with
`bash -n` plus the runner's CSV header against the canonical schema.

Full verification sweep, all passing:
`bash -n` on every script (bench-kit and crossover); `shellcheck` on every
script (0.11.0, installed — only pre-existing info/low-severity notes
remain, e.g. `SC2094` on an intentionally self-excluding `find`, `SC2086`
on a deliberate `/proc/pid/stat` word-split, `SC2155` in a log helper —
none are functional bugs); `python3 -m py_compile` on every `.py` file;
`python3 self-test.py` in both `bench-kit/` and `bench-kit/crossover/`.

## Package

`make-baremetal-package.sh` was run only with `--dry-run`, per the Launch
note (the three commit hashes are still literal placeholders in
`sources.conf`). The dry run lists 19 kit files, 3 workload files, the
runbook, the 13-file crossover harness, the one control patch and five
v11-series patches (resolved by the configured globs under
`patches-control/` and `patches-v11/`), and the bundled
`test_wait_primitive` fixture snapshot — and confirms no archive is
produced and no git command runs. There is therefore no package path or
SHA-256 to report yet; the coordinator fills `sources.conf`, then runs
`make-baremetal-package.sh` (without `--dry-run`, with `POSTGRES_REPO_PATH`
set) to produce `wet-v11-baremetal-r1.tar.gz` and its `.sha256`.

## Not done / left for the coordinator or a real host

- The three commit hashes in `sources.conf` (`MASTER_SHA`, `V11_SHA`,
  `CONTROL_SHA`) — explicitly deferred by the Launch note.
- Real packaging (`make-baremetal-package.sh` without `--dry-run`) and
  everything downstream of it (host copy, `run-benchmark.sh` for real,
  the actual 8-10 hour run) — requires the hashes above and a real Linux
  bare-metal host; this environment permits neither a real PostgreSQL
  server nor git worktree changes.
- `pg_wait_event_tracing_hooks_installed()` is referenced by name per the
  brief's Launch note; it was not visible in the current
  `patches-v11/*.patch` files by grep, so its presence should be
  double-checked once the patches are regenerated before packaging.
- The crossover's own `make-package.sh` was adapted (naming, git-repo
  fallback) but not executed, for the same reasons as the main
  `make-baremetal-package.sh`.
- Deep byte-level build-provenance verification in `analyze-results.py`
  was intentionally scoped down from v10's version (which hard-pinned
  exact source/commit/compiler metadata for a specific already-known
  v9/v10 comparison): v11's `verify_manifest()` checks schema, baseline
  reproducibility, module presence/absence, and that patched/control
  postgres binaries differ, but does not re-derive a fixed source-manifest
  hash chain the way v10 did, since the real commit provenance does not
  exist yet.
