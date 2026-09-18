# Bare-metal measurement runbook, wait-event tracing v11

No SSH access or remote automation is required. Copy the two delivered files
to the executor account on one otherwise-idle Linux bare-metal host:

- `wet-v11-baremetal-r1.tar.gz`
- `wet-v11-baremetal-r1.tar.gz.sha256`

Put them directly under the executor account's home directory. Then verify,
extract, set this host's CPU masks, and start the complete run:

```sh
sha256sum -c wet-v11-baremetal-r1.tar.gz.sha256
tar -xzf wet-v11-baremetal-r1.tar.gz
cd wet-v11-baremetal-r1
lscpu -e                      # find two disjoint CPU sets on separate cores
export SERVER_CPUS=...        # e.g. 1-31 (PostgreSQL)
export PGBENCH_CPUS=...       # e.g. 32-39 (pgbench), no shared physical core
./run-benchmark.sh
```

Run the final command inside `tmux` or `screen`. It is the only benchmark
command you need to start (it runs both the seven-configuration matrix and
the second-stage persistent-backend crossover). Do not run it as root.
Leave the host idle.

Unlike the v10 kit, this kit is not tied to one fixed two-socket topology:
you tell it which CPUs to use for PostgreSQL and which for pgbench, and it
verifies at runtime that both masks are non-empty, online, and share no
physical core (see "CPU affinity" below).

## What the launcher does

The launcher is fail-closed and runs these phases in order:

1. Verify `sources.conf` has no remaining placeholder (refuses to start
   otherwise) and verify every packaged script, workload, and source
   archive.
2. Check the Linux host and prerequisites, including the CPU-affinity
   masks above.
3. Run the synthetic self-test (`self-test.py`) -- no PostgreSQL server is
   started for this step; it uses generated data only.
4. Wait for one idle minute.
5. Build two independent vanilla baselines plus the patched (v11 series)
   and control sources with PostgreSQL's bundled `configure` script and
   GNU Make.
6. Run the plateau probe (`plateau-probe.sh`): eight vanilla-only W6c
   sessions (four with the dataset clone/initdb pinned to the server's
   NUMA node, four unpinned) and record which variant has the smaller
   session-to-session spread.
7. Run a short smoke matrix covering all 35 configuration/workload cells.
8. Wait automatically for one idle minute, then repeat the host check.
9. Run the full 560-cell measurement matrix (7 configurations x 5
   workloads x 16 repetitions).
10. Run the second-stage persistent-backend crossover (`crossover/`) on the
    patched installation.
11. Package and checksum the raw evidence from both stages, without final
    analysis.

The package contains checksummed source snapshots for the three pinned
commits plus the standalone control and v11-series patches, so the executor
does not depend on network availability during the run.

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
`results/progress.json` (or `smoke-results/progress.json`) reports the
active cell, completed cell count, elapsed time, and ETA.

The run stops immediately and prints diagnostics when any of these fail:

- `sources.conf` placeholder check, package checksum, or self-test;
- bare-metal, idle-host, tool, RAM, disk, governor, or CPU-affinity checks
  (including a shared physical core between PostgreSQL and pgbench);
- baseline reproducibility comparison;
- any server start or clean shutdown;
- a mode proof (module presence/absence, capture level,
  `pg_wait_event_tracing_hooks_installed()`, or active-capture recording
  rows) for any of the seven configurations;
- W3 rate, histogram, target-event, or I/O qualification;
- pgbench parsing, failed transactions, or client-driver saturation;
- the early A/A gate on W4 after repetition 6 or 10 (half-width over 1.0%
  is reported as "host unsuitable", raw data retained);
- post-smoke idle/cooldown and final host checks;
- the crossover's own smoke run or fixed protocol checks;
- final evidence integrity or raw-archive verification.

Partial output and logs are preserved for diagnosis. The launcher never
merges or silently resumes a partial matrix. If it fails, return
`status.json` and the unedited `run-logs/benchmark-*.log` before deleting
anything.

To check only package integrity, the self-test, and host readiness:

```sh
./run-benchmark.sh --preflight-only
```

## Host requirements

- Linux bare metal, SMT off recommended, at least 8 physical cores and
  16 GB RAM.
- Two disjoint CPU sets for `SERVER_CPUS`/`PGBENCH_CPUS` that share no
  physical core (checked via `lscpu -p=CPU,CORE,SOCKET,NODE`).
- At least 30 GB free under the extracted kit.
- GCC, Python 3.9 or newer, Perl, Bison, Flex, GNU Make, `ar`, `ranlib`,
  `tar`, `lscpu`, and `taskset`. `numactl` is recommended (used by the
  plateau probe) but not required -- if it is absent and the host has more
  than one NUMA node, the check warns but continues, and the probe falls
  back to `taskset`-only pinning of the pinned variant.
- CPU frequency governor set to `performance`.
- No active build, backup, or interactive workload. Existing low-activity
  PostgreSQL clusters are recorded as co-resident provenance and do not
  block.
- Extract directly under the executor account's home to keep Unix socket
  paths short.

The checker is read-only apart from testing the configured masks on a
disposable `true` process. It never changes governor, turbo, SMT, or
persistent host settings.

## Pinned sources and matrix

Filled in by the coordinator in `sources.conf` (the kit refuses to run
while a placeholder remains):

| Build | Commit | Notes |
|---|---|---|
| baseline A and B | `MASTER_SHA` | two independent builds, must be byte-identical |
| patched | `V11_SHA` | v11 series tip (five commits) |
| control | `CONTROL_SHA` | patched + `patches-control/*.patch` (timed sites compiled out) |

Seven configurations (`master`, `master-aa`, `control`, `hook-null`,
`module-off`, `stats`, `trace`) run W1, W3, W4, W5, and W6c 16 times each,
in a randomized-complete-block schedule built from a 7x7 Latin square (every
configuration occupies every schedule position at least twice over the 16
repetitions). Every cell gets a fresh cluster; dataset creation uses
vanilla baseline-A binaries before treatment startup.

Analysis pairs a Student t interval with an exact Wilcoxon signed-rank /
Hodges-Lehmann interval; the predeclared equivalence margins are ±2
ns/iteration for W1 and ±2% for pgbench (compared on log(TPS)). A contrast
is reported as a resolved difference only when both intervals agree; a
single-mode plateau that only the t interval would call is instead reported
as "unresolved."

Expected duration:

| Phase | Approximate time |
|---|---:|
| integrity, idle gate, self-test, host check | 2 to 21 minutes |
| four controlled builds | 20 to 40 minutes |
| plateau probe (8 sessions) | 10 to 20 minutes |
| smoke matrix (35 cells) | 10 to 25 minutes |
| 560-cell full matrix | 6.5 to 8.5 hours |
| second-stage crossover | 2.5 to 3.5 hours |
| raw collection | a few minutes |

Total: roughly 8 to 10 hours, matching the brief.

## Successful output

On success, return these two files without editing or filtering them:

1. `results-<hostname>-<date>.tar.gz` (matrix stage)
2. The crossover archive printed by its own launcher (second stage)

Both sidecars use standard `sha256sum -c` format. Copy both result sets
back beside the extracted kit, then run locally:

```sh
./analyze-raw-archive.sh results-<hostname>-<date>.tar.gz
```

This verifies, extracts, and analyzes the archive locally. It writes
`analysis.json` and `analysis.md` under a new `*-local-analysis` directory.
The crossover's own `analyze.py` does the same for the second stage.

The archive records host name, OS user, and working paths. Scrub those only
after independent verification and before publication.
