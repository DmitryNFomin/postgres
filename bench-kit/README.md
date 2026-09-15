# PostgreSQL wait-event-tracing v9 benchmark kit

This is a one-command, fail-closed benchmark for an idle Linux bare-metal
host. It verifies and measures the pinned v9 wait-event hook and
`pg_wait_event_tracing` extension.

The build uses PostgreSQL's bundled `configure` script and GNU Make with ICU,
readline, and zlib disabled because the benchmark does not use those features.
It does not require Meson, Ninja, root access, or network access.

After verifying and extracting the delivered archive, run:

```sh
./run-benchmark.sh
```

No SSH access is used. The launcher verifies the package, runs a synthetic
288-cell verifier test, checks the host, builds three controlled trees, runs a
24-cell smoke matrix, runs the full 288-cell matrix, and creates a checksummed
raw-evidence archive.

Before the full matrix, it automatically waits for build/smoke load to clear
for one minute and repeats the host check.

Existing low-activity PostgreSQL clusters do not share the benchmark's private
socket or port. They are inventoried as co-resident provenance instead of
blocking the run. The final report carries a co-residency caveat, and the
mandatory A/A gates still reject unstable measurements.

See `BAREMETAL-RUNBOOK-v9.md` in this directory for the exact executor steps.

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
- Patched commit: `40bffed8a92291c27a5d1956a5cd18dd3609f397`.
- Null-hook optimization: one hook-pointer load, no end-event read when the
  hook is null, direct depth-guard restoration, and no branch prediction hint.
- Standalone optimization patch under `patches/` in the packaged kit.
- Checksummed offline source snapshots.
- Two independently compiled baseline builds plus one patched build.
- Compiler caches disabled and byte-identical baseline binaries required.
- Identical compile paths and equal-length runtime prefixes.
- Fixed 12 repetitions and 288 cells, not environment-tunable.
- CPU pinning deliberately unset by the launcher.
- Fresh cluster and neutral baseline dataset setup for every cell.
- Randomized complete configuration blocks.
- Required clean shutdown before data deletion.
- Active recording proofs tied to real pgbench PIDs.
- W3 evidence recomputed from retained raw TSV data.
- Client saturation and early/final baseline A/A gates.
- Build-manifest, host-report, harness, and workload hash binding.

## Matrix and analysis

The six configurations are `master`, `master-aa`, `hook-null`, `module-off`,
`stats`, and `trace`. They run W1, W3, W4, and W6c 12 times each.

Active capture must prove timing rows for every workload client. Trace must
also prove a real client wait record. Each active W3 cell must meet fixed
short-`ProcArray`-LWLock rate, fraction, histogram, and I/O thresholds.

Pgbench uses a 10-second discarded warmup and an approximately 30-second
measured window. TPS and latency are weighted by actual interval duration and
transaction count.

Analysis uses repetition-paired two-sided 95% Student t intervals:

- W1: configuration minus `master`, ±2 ns equivalence margin.
- W3/W4/W6c: `(configuration / master - 1) * 100`, ±2% margin.

Equivalence requires the complete interval inside the margin. An interval
crossing zero is only “no statistically resolved difference.” Final analysis
and A/A suitability run locally with:

```sh
./analyze-raw-archive.sh results-<hostname>-<date>.tar.gz
```

The raw archive is retained even if the final suitability gate fails.
