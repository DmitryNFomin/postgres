# PostgreSQL wait-event-tracing v10 benchmark kit

This is a one-command, fail-closed benchmark for an idle Linux bare-metal
host. It directly compares the pinned v9 reference and v10 attachment-guard
treatment in one randomized run. This r3 kit is topology-specific: it requires
the verified two-socket, 64-core CPU map described in the runbook.

The build uses PostgreSQL's bundled `configure` script and GNU Make with ICU,
readline, and zlib disabled because the benchmark does not use those features.
It does not require Meson, Ninja, root access, or network access.

After verifying and extracting the delivered archive, run:

```sh
./run-benchmark.sh
```

No SSH access is used. The launcher verifies the package, checks the host,
runs a synthetic 480-cell verifier test, builds four controlled trees, runs a
40-cell smoke matrix, runs the full 480-cell matrix, and creates a checksummed
raw-evidence archive.

Before the full matrix, it automatically waits for build/smoke load to clear
for one minute and repeats the host check.

Existing low-activity PostgreSQL clusters do not share the benchmark's private
socket or port. They are inventoried as co-resident provenance instead of
blocking the run. The final report carries a co-residency caveat, and the
mandatory A/A gates still reject unstable measurements.

See `BAREMETAL-RUNBOOK-v10.md` in this directory for the exact executor steps.

## Observe a run

The terminal receives timestamped phase output and a heartbeat every
30 seconds. The same unedited output is retained under `run-logs/`.

```sh
./run-benchmark.sh --status
```

This prints `status.json` plus recent log output. Matrix cell count, current
cell, elapsed time, and ETA are also written to `results/progress.json`.

Failures stop immediately and retain diagnostics. The launcher does not merge
or resume a partial matrix.

## Fixed controls

- Baseline commit: `765efece39ba3fb04fdf20b1dadcd9ecea76fbc9`.
- v9 reference: `40bffed8a92291c27a5d1956a5cd18dd3609f397`.
- v10 treatment: `c12783fbf86e8116526afe4566d58bf90c3478e0`.
- v9 null-hook optimization: one hook-pointer load, no end-event read when the
  hook is null, direct depth-guard restoration, and no branch prediction hint.
- v10 optimization: an always-inline `pwet_attach_needed` guard around the
  unchanged slow attachment/retry path.
- Both standalone optimization patches under `patches/` in the packaged kit.
- Three checksummed offline source snapshots.
- Two independently compiled baselines plus separate v9 and v10 builds.
- Compiler caches disabled and byte-identical baseline binaries required.
- Identical compile paths and equal-length runtime prefixes.
- Fixed 12 repetitions and 480 cells, not environment-tunable.
- PostgreSQL fixed to CPUs `1-63:2` on socket/NUMA node 1.
- Pgbench fixed to CPUs `0-14:2` on socket/NUMA node 0.
- Fail-closed sysfs, `lscpu`, `taskset`, and runtime process-affinity proofs.
- Fresh cluster and neutral baseline dataset setup for every cell.
- Randomized complete configuration blocks.
- Required clean shutdown before data deletion.
- Active recording proofs tied to real pgbench PIDs.
- W3 evidence recomputed from retained raw TSV data.
- Client saturation and early/final baseline A/A gates.
- Build-manifest, host-report, harness, and workload hash binding.

## Matrix and analysis

The ten configurations are `master`, `master-aa`, and v9/v10 versions of
`hook-null`, `module-off`, `stats`, and `trace`. They run W1, W3, W4, and
W6c 12 times each.

Active capture must prove timing rows for every workload client. Trace must
also prove a real client wait record. Each active W3 cell must meet fixed
short-`ProcArray`-LWLock rate, fraction, histogram, and I/O thresholds.

Pgbench uses a 10-second discarded warmup and an approximately 30-second
measured window. TPS and latency are weighted by actual interval duration and
transaction count.

Analysis uses repetition-paired two-sided 95% Student t intervals:

- Primary W1: v10 minus v9 in each mode, ±2 ns equivalence margin.
- Primary W3/W4/W6c: `(v10 / v9 - 1) * 100`, ±2% margin.
- W1: configuration minus `master`, ±2 ns equivalence margin.
- W3/W4/W6c: `(configuration / master - 1) * 100`, ±2% margin.

Equivalence requires the complete interval inside the margin. An interval
crossing zero is only “no statistically resolved difference.” Final analysis
and A/A suitability run locally with:

```sh
./analyze-raw-archive.sh results-<hostname>-<date>.tar.gz
```

The raw archive is retained even if the final suitability gate fails.
