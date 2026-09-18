# PostgreSQL wait-event-tracing v11 benchmark kit

This is a two-command, fail-closed benchmark for an idle Linux bare-metal
host. It measures every v11 configuration against vanilla PostgreSQL in one
randomized run, proves each configuration's mode before timing it, and
analyzes the result with an estimator pair (paired Student t, and an
exact Wilcoxon signed-rank / Hodges-Lehmann interval) chosen to stay honest
under the session-level throughput plateaus seen in the v10 runs.

The build uses PostgreSQL's bundled `configure` script and GNU Make with
ICU, readline, and zlib disabled because the benchmark does not use those
features. It does not require Meson, Ninja, root access, or network access:
the package carries checksummed source snapshots and standalone patches.

**Before anything else can run**, `sources.conf` must have its three
placeholder commit hashes filled in by the coordinator (see the "Launch
note" in brief-v11-wpc-kit.md). `run-benchmark.sh` refuses to start while
any placeholder remains.

After verifying and extracting the delivered archive, set the two CPU masks
for this host (see `BAREMETAL-RUNBOOK-v11.md`), then run:

```sh
export SERVER_CPUS=... PGBENCH_CPUS=...
./run-benchmark.sh
```

No SSH access is used. The launcher verifies the package, checks the host,
runs the synthetic self-test, waits for an idle host, builds four controlled
trees (two vanilla, patched, control), runs the plateau probe, runs a smoke
matrix, runs the full matrix, runs the second-stage persistent-backend
crossover, and creates a checksummed raw-evidence archive.

## Observe a run

The terminal receives timestamped phase output and a heartbeat every
30 seconds. The same unedited output is retained under `run-logs/`.

```sh
./run-benchmark.sh --status
```

This prints `status.json` plus recent log output. Matrix cell count, current
cell, elapsed time, and ETA are also written to `results/progress.json`.

Failures stop immediately and retain diagnostics. The launcher does not
merge or resume a partial matrix.

## Configurations, workloads, and design

Seven configurations in one matrix: `master`, `master-aa`, `control`,
`hook-null`, `module-off`, `stats`, `trace` (see `benchmark_protocol.py` for
the exact build/GUC mapping). Five workloads: W1 (all five
`test_wait_primitive` functions), W3, W4, W5 (TPC-B, 16 clients, 4 GB), W6c.

- **Randomized complete blocks**: one repetition = one 7x7-Latin-square
  configuration order per workload (`latin_square.py`); 16 repetitions, with
  every configuration occupying every schedule position at least twice.
- **Mode proofs** run once per cell, before the timed window, from a proof
  session on the same server; failure aborts the run.
- **Early A/A gate** after repetitions 6 and 10: if the master vs
  master-aa paired 95% interval half-width on W4 exceeds 1.0%, the run
  stops and reports "host unsuitable" (raw data retained).
- **Plateau probe** (`plateau-probe.sh`) runs in preflight: eight
  vanilla-only W6c sessions, four with the dataset clone/initdb pinned to
  the server's NUMA node, four unpinned. The matrix uses whichever variant
  has the smaller spread.
- **Covariates** recorded per cell: per-CPU frequency (mean/min/max over the
  window), NUMA-local page fraction, `/proc/meminfo` Cached before/after,
  load average before, and the effective timing clock source.
- **Second stage**: the v10 persistent-backend crossover (`crossover/`),
  reused unchanged in protocol but adapted to run on the patched
  installation only (no v9/v10 pair).
- **CPU affinity** is configurable (`SERVER_CPUS`/`PGBENCH_CPUS`), not
  pinned to one topology; the host check refuses to run if the two masks
  share a physical core or if either is empty.

## Matrix and analysis

Mode proofs: active capture (stats/trace) must prove timing rows in
`pg_stat_wait_event_timing` for every workload client; trace must also prove
a real client wait record via `pg_get_wait_event_trace()`. Each active W3
cell must meet fixed short-`ProcArray`-LWLock rate, fraction, histogram, and
I/O thresholds.

Analysis (`analyze-results.py`) reports, for every workload and contrast, on
log(TPS) (or ns/iteration for W1): a paired 95% Student t interval, and a
95% Hodges-Lehmann interval from the exact Wilcoxon signed-rank
distribution (implemented from scratch in `wilcoxon.py`, stdlib only).
A contrast is "equivalent" only if both intervals lie inside the
predeclared margin (±2 ns for W1, ±2% for pgbench), "faster"/"slower" only
if both intervals exclude zero on the same side, and "unresolved"
otherwise -- which is what keeps a single-mode plateau from being reported
as a resolved difference. Output is `analysis.md` (72 columns) and
`analysis.json`, including a per-block diagnostics table (block index,
per-configuration schedule position, TPS, mean frequency, NUMA-local
fraction).

Final analysis runs locally with:

```sh
./analyze-raw-archive.sh results-<hostname>-<date>.tar.gz
```

The raw archive is retained even if the final suitability gate fails.
