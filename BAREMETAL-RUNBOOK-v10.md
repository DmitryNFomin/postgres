# Bare-metal measurement runbook, wait-event tracing v10

No SSH access or remote automation is required. Copy the two delivered files
to the executor account on one otherwise-idle Linux bare-metal
host:

- `wet-v10-baremetal-r3.tar.gz`
- `wet-v10-baremetal-r3.tar.gz.sha256`

Put them directly under the executor account's home directory. Then verify,
extract, and start the complete run:

```sh
sha256sum -c wet-v10-baremetal-r3.tar.gz.sha256
tar -xzf wet-v10-baremetal-r3.tar.gz
cd wet-v10-baremetal-r3
./run-benchmark.sh
```

Run the final command inside `tmux` or `screen`. It is the only benchmark
command you need to start. Do not run it as root. Leave the host idle.

## What the launcher does

The launcher is fail-closed and runs these phases in order:

1. Verify every packaged script, workload, document, and source archive.
2. Check the Linux host and prerequisites.
3. Run a sub-minute synthetic verifier and shell/awk portability self-test.
4. Wait for one idle minute.
5. Build two independent baselines plus separate v9 and v10 sources with
   PostgreSQL's bundled `configure` script and GNU Make.
6. Run a short 40-cell smoke matrix covering all configurations/workloads.
7. Verify the smoke evidence.
8. Wait automatically for one idle minute, then repeat the host check.
9. Run the fixed 480-cell measurement matrix.
10. Package and checksum the raw evidence without final analysis.

The package contains checksummed source snapshots for the three pinned
commits, plus standalone v9 and v10 optimization patches, so the executor
does not depend on GitHub availability during the run.

The launcher clears inherited affinity and protocol overrides, then enforces
the r3 CPU placement: PostgreSQL on CPUs `1-63:2` (socket/NUMA node 1) and
pgbench on CPUs `0-14:2` (eight cores on socket/NUMA node 0). The full run
remains 12 repetitions, 480 cells, 30 measured seconds, and the same declared
margins and A/A thresholds.

## Early failure and observability

The launcher prints timestamped phase transitions and a heartbeat every
30 seconds. All output is copied verbatim to:

```text
run-logs/benchmark-<UTC timestamp>.log
```

At any time, a read-only status view is available:

```sh
./run-benchmark.sh --status
```

The status is also stored in `status.json`. During matrix execution,
`results/progress.json` (or `smoke-results/progress.json`) reports the active
cell, completed cell count, elapsed time, and ETA.

The run stops immediately and prints diagnostics when any of these fail:

- package checksum or synthetic verifier self-test;
- bare-metal, idle-host, tool, RAM, disk, or governor checks;
- either baseline reproducibility comparison;
- any server start or clean shutdown;
- capture mode, SQL extension, client-PID recording, or trace-ring proof;
- W3 rate, histogram, target-event, or I/O qualification;
- pgbench parsing, failed transactions, or client-driver saturation;
- early baseline A/A noise gates after repetitions 1, 3, 6, and 9;
- post-smoke idle/cooldown and final host checks;
- final evidence integrity or raw-archive verification.

Partial output and logs are preserved for diagnosis. The launcher never merges
or silently resumes a partial matrix. If it fails, return `status.json` and the
unedited `run-logs/benchmark-*.log` before deleting anything.

To check only package integrity, the synthetic verifier, and host readiness:

```sh
./run-benchmark.sh --preflight-only
```

The default run already performs this preflight automatically.

## Host requirements

- Linux bare metal with exactly two 32-core sockets, CPUs `0-63` online,
  SMT off, and at least 16 GB RAM.
- Socket/NUMA node 0 must contain the even CPUs `0,2,...,62`; socket/NUMA
  node 1 must contain the odd CPUs `1,3,...,63`.
- At least 30 GB free under the extracted kit.
- GCC, Python 3.9 or newer, Perl, Bison, Flex, GNU Make, `ar`, `ranlib`,
  tar, `lscpu`, and `taskset`. Meson, Ninja, ICU, readline headers, and zlib
  headers are not needed.
- CPU frequency governor set to `performance`.
- No active build, backup, or interactive workload. Existing low-activity
  PostgreSQL clusters are recorded as co-resident provenance and do not block.
- Extract directly under the executor account's home to keep Unix socket
  paths short.

The checker is read-only apart from testing the required masks on a disposable
`true` process. It never changes governor, turbo, SMT, or persistent host
settings. It verifies both sysfs NUMA CPU lists and `lscpu` socket mappings.
Any warning blocks the run before compilation.

## Why r3 uses fixed CPU affinity

The unpinned r2 run correctly stopped at its repetition-3 A/A gate because one
W3 baseline control cell was an outlier. W3 has eight clients repeatedly
handing off an exclusive `ProcArrayLock`; unrestricted scheduling can move
that lock handoff across sockets. R3 keeps every PostgreSQL process on one
socket and all eight pgbench threads on the other. The launcher verifies the
postmaster and pgbench process masks at runtime. Exact masks are retained in
the protocol and every result row, and analysis rejects altered or
incompatible topology evidence.

## Pinned sources and matrix

| Build | Commit |
|---|---|
| baseline A and B | `765efece39ba3fb04fdf20b1dadcd9ecea76fbc9` |
| v9 reference, optimized null-hook path | `40bffed8a92291c27a5d1956a5cd18dd3609f397` |
| v10 treatment, inline attachment guard | `c12783fbf86e8116526afe4566d58bf90c3478e0` |

Compared with v8, the v9 source snapshots the begin/end hook pointers
once per timed report. The end path reads the volatile wait-event value only
when an end hook will consume it. The recursion guard is restored with direct
assignments, avoiding post-callback reload/arithmetic. There is deliberately
no `likely()` or `unlikely()` hint, so compiler layout is not explicitly
biased against enabled collection. Hook ordering, chaining, recursion
protection, stats, and trace behavior are unchanged.

V10 adds an always-inline `pwet_attach_needed` check around the unchanged
attachment implementation. When no attachment is pending, parse and executor
hooks avoid an out-of-line call. The original safe-point, retry, stats, and
trace transitions remain in the slow path.

Ten configurations (`master`, `master-aa`, and v9/v10 versions of `hook-null`,
`module-off`, `stats`, and `trace`) run W1, W3, W4, and W6c 12 times each.
Each workload/repetition is a complete randomized ten-configuration block.
Every cell gets a fresh cluster. Dataset creation uses neutral baseline
binaries before treatment startup.

The analysis uses repetition-paired 95% Student t intervals. The predeclared
equivalence margins are ±2 ns/iteration for W1 and ±2% for pgbench. The
primary contrasts pair v10 against v9 in each mode. Secondary contrasts pair
all configurations against vanilla. Co-resident PostgreSQL processes are
reported as a suitability caveat even when the baseline A/A gates pass.

Expected duration:

| Phase | Approximate time |
|---|---:|
| integrity, idle gate, self-test, host check | 2 to 21 minutes |
| four controlled builds | 20 to 35 minutes |
| 40-cell smoke matrix | 8 to 20 minutes |
| 480-cell full matrix | 7 to 9 hours |
| raw collection | a few minutes |

## Successful output

On success, return these two files without editing or filtering them:

1. `results-<hostname>-<date>.tar.gz`
2. `results-<hostname>-<date>.tar.gz.sha256`

The sidecar uses standard `sha256sum -c` format. The archive contains raw
rows, schedule, logs, recording proofs, W3 evidence, host data, bound build
provenance, exact verifier code, and checksums. It is integrity-verified before
the launcher reports success.

Copy both result files back beside the extracted r3 kit, then run locally:

```sh
./analyze-raw-archive.sh results-<hostname>-<date>.tar.gz
```

This verifies, extracts, and analyzes the archive locally. It writes
`analysis.json` and `analysis.md` under a new `*-local-analysis` directory.
Final A/A suitability is enforced there; raw evidence is retained even when
the suitability result fails.

The archive records host name, OS user, and working paths. Scrub those only
after independent verification and before publication.
