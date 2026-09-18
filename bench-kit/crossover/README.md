# Persistent-backend W6c crossover (second stage)

This package measures wait-event tracing overhead while statistically
blocking the stable fast/slow W6c throughput states seen in the v10 runs.
It reuses the v10 crossover protocol unchanged, but on the v11 kit's
patched installation only -- there is no v9/v10 pair (brief-v11-wpc-kit.md).

Each server session creates 32 pgbench connections once and keeps those
same PostgreSQL backend PIDs alive through eight 30-second blocks. Capture
mode changes by configuration reload:

```text
sequence A: off, stats, stats, off, off, trace, trace, off
sequence B: off, trace, trace, off, off, stats, stats, off
```

The active blocks are bracketed by off blocks. The adjacent central off/off
blocks are a placebo measurement. 16 independent sessions are balanced 8
sequence A / 8 sequence B. Each session clones its own neutral scale-100
dataset (no dataset sharing between sessions -- that was only needed for
the old v9/v10 pairing).

The analysis unit is one complete server session. One-second aggregate rows
are used only to form block totals and within-block diagnostics; they are
never treated as independent statistical observations.

## What it estimates

- stats versus module-loaded capture-off overhead;
- trace versus module-loaded capture-off overhead;
- trace versus stats overhead;
- off/off placebo drift and within-block stability.

It does not estimate vanilla PostgreSQL versus `shared_preload_libraries`.
That contrast is measured in the main seven-configuration matrix
(`02-run-matrix.sh`), not here.

## Fixed protocol

- 16 independent sessions, patched installation only;
- 8 blocks per session, 30 measured seconds per block;
- 3-second post-proof settling period;
- W6c: `pgbench -S`, scale 100, 32 clients, 8 threads, 32 MB
  `shared_buffers`;
- CPU affinity: `SERVER_CPUS`/`PGBENCH_CPUS`, inherited from the same
  environment the matrix stage used (configurable, not one fixed topology);
- 95% Student t intervals over sessions;
- +/-2% equivalence margin for performance contrasts;
- +/-1% off/off placebo margin.

The estimated runtime is approximately 2.5 to 3.5 hours. Leave the machine
idle and run inside `tmux` or `screen`.

The launcher prints its staging directory before measurement starts. To
follow per-block progress from another terminal:

```sh
tail -f /var/tmp/w6c-persistent-crossover.<random>/evidence/driver.log
```

## Run

This second stage is started automatically by the main kit's
`run-benchmark.sh` after the 560-cell matrix completes. To run it by hand
(e.g. to retry just this stage):

```sh
./run.sh --smoke /path/to/wet-v11-baremetal-r2
./run.sh /path/to/wet-v11-baremetal-r2
```

The mandatory smoke run takes approximately 1-2 minutes. It starts the
patched build, creates 32 persistent backends, and exercises both balanced
capture sequences using short blocks. The full benchmark is refused until
that smoke run succeeds for this package, kit, executor, and host.

Run as the original non-root executor. The launcher refuses root, verifies
the crossover package, verifies the complete kit package and installation
tree, runs the kit's idle gate, checks current topology, SMT, turbo, and CPU
governors before and after measurement, and refuses an already-running
PostgreSQL server. It automatically selects an available Python 3.9 or newer;
set `PYTHON_BIN_OVERRIDE=/absolute/path/to/python` only if that interpreter
has a nonstandard command name.

On success, return both files printed by the launcher:

```text
/var/tmp/w6c-persistent-crossover.<random>/w6c-persistent-crossover-<UTC>.tar.gz
/var/tmp/w6c-persistent-crossover.<random>/w6c-persistent-crossover-<UTC>.tar.gz.sha256
```

The archive contains the raw aggregate logs, exact backend PID sets, mode
proofs, event boundaries, schedule, protocol, server logs, kit provenance,
analysis, and an evidence manifest. If the run fails, preserve the staging
path printed in the terminal and return `driver.log`.
