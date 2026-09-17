# Persistent-backend W6c crossover

This package measures wait-event tracing overhead while statistically
blocking the stable fast/slow W6c throughput states seen in the r3 matrix.
It reuses the exact verified r3 v9 and v10 installations, but it does not
modify or resume the partial r3 results.

Each server session creates 32 pgbench connections once and keeps those
same PostgreSQL backend PIDs alive through eight 30-second blocks. Capture
mode changes by configuration reload:

```text
sequence A: off, stats, stats, off, off, trace, trace, off
sequence B: off, trace, trace, off, off, stats, stats, off
```

The active blocks are bracketed by off blocks. The adjacent central off/off
blocks are a placebo measurement. Sequence and first-build order form a
balanced 2 x 2 allocation over 16 adjacent v9/v10 pairs. Both sessions in a
pair clone the same neutral scale-100 dataset and use the same pgbench seed.

The analysis unit is one complete server session. V10/v9 analysis uses one
adjacent matched session pair. One-second aggregate rows are used only to
form block totals and within-block diagnostics; they are never treated as
independent statistical observations.

## What it estimates

- stats versus module-loaded capture-off overhead within v9 and v10;
- trace versus module-loaded capture-off overhead within v9 and v10;
- v10 versus v9 change in off, stats, and trace throughput;
- v10 versus v9 change in stats and trace overhead;
- off/off placebo drift and within-block stability.

It does not estimate vanilla PostgreSQL versus `shared_preload_libraries`.
That contrast still requires separate server starts.

## Fixed protocol

- 16 adjacent v9/v10 pairs, 32 server sessions;
- 8 blocks per session, 30 measured seconds per block;
- 3-second post-proof settling period;
- W6c: `pgbench -S`, scale 100, 32 clients, 8 threads, 32 MB
  `shared_buffers`;
- PostgreSQL on CPUs `1-63:2`, pgbench on CPUs `0-14:2`;
- 95% Student t intervals over sessions or matched session pairs;
- ±2% equivalence margin for performance contrasts;
- ±1% off/off placebo margin.

The estimated runtime is approximately 2.5 to 3.5 hours. Leave the machine
idle and run inside `tmux` or `screen`.

The launcher prints its staging directory before measurement starts. To
follow per-block progress from another terminal:

```sh
tail -f /var/tmp/w6c-persistent-crossover.<random>/evidence/driver.log
```

## Run

Copy the crossover archive and checksum beside the already extracted r3 kit,
verify the delivery digest, then:

```sh
sha256sum wet-v10-w6c-persistent-crossover-r5.tar.gz
# Compare that exact value with the independently supplied delivery digest.
sha256sum -c wet-v10-w6c-persistent-crossover-r5.tar.gz.sha256
tar -xzf wet-v10-w6c-persistent-crossover-r5.tar.gz
cd wet-v10-w6c-persistent-crossover-r5
./run.sh --smoke /home/benchmark-user/wet-v10-baremetal-r3
./run.sh /home/benchmark-user/wet-v10-baremetal-r3
```

The mandatory smoke run takes approximately 1–2 minutes. It starts both
verified builds, creates 32 persistent backends, and exercises both balanced
capture sequences using short blocks. The full benchmark is refused until
that smoke run succeeds for this package, r3 kit, executor, and host.

Run as the original non-root r3 executor. The launcher refuses root, verifies
the crossover package, verifies the complete r3 package and installation
trees, runs the r3 idle gate, checks current topology, SMT, turbo, and CPU
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
proofs, event boundaries, schedule, protocol, server logs, r3 provenance,
analysis, and an evidence manifest. If the run fails, preserve the staging
path printed in the terminal and return `driver.log`.
