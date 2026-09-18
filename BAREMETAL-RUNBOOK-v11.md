# Bare-metal measurement runbook, wait-event tracing v11

<!-- This file is the source of truth. The top-level copy at
     ../BAREMETAL-RUNBOOK-v11.md is generated from it (kept identical by
     hand) so both the kit and the notes repo can each carry their own
     copy without drifting; edit only this one. -->

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

## Laptop workflow

The full path from a coordinator's laptop (macOS or Linux) to a result back
on that laptop. This kit never does its own SSH; you drive `scp`/`ssh` by
hand:

```sh
# 1. On the laptop: get the sources and build the package.
git clone git@github.com:DmitryNFomin/postgres.git pg && cd pg
git fetch origin wet-v11 bench-v11-control
git worktree add ../v11-notes origin/v11-notes   # or checkout
cd ../v11-notes/bench-kit
POSTGRES_REPO_PATH=/path/to/pg ./make-baremetal-package.sh ../dist

# 2. Copy the two files to the Rocky Linux 8 host.
scp ../dist/wet-v11-baremetal-r1.tar.gz \
    ../dist/wet-v11-baremetal-r1.tar.gz.sha256 \
    executor@HOST:~/

# 3. Start the run under tmux on the host (see "Rocky Linux 8 preparation"
#    below if the host is not set up yet).
ssh executor@HOST
tmux new -s v11-bench
sha256sum -c wet-v11-baremetal-r1.tar.gz.sha256
tar -xzf wet-v11-baremetal-r1.tar.gz
cd wet-v11-baremetal-r1
lscpu -e
export SERVER_CPUS=...   # e.g. 1-31
export PGBENCH_CPUS=...  # e.g. 32-39
./run-benchmark.sh
# detach with Ctrl-b d; reattach any time from a new ssh session with:
#   tmux attach -t v11-bench

# 4. After it finishes (8 to 10 hours later), from the laptop: fetch the
#    two result archives run-benchmark.sh printed the paths for.
scp executor@HOST:~/wet-v11-baremetal-r1/results-*.tar.gz \
    executor@HOST:~/wet-v11-baremetal-r1/results-*.tar.gz.sha256 .
scp "executor@HOST:/var/tmp/w6c-persistent-crossover-*.tar.gz" \
    "executor@HOST:/var/tmp/w6c-persistent-crossover-*.tar.gz.sha256" .
./analyze-raw-archive.sh results-*.tar.gz
```

The clone's remote name does not matter (`origin` above, or `fork`, or
anything else you name it): `make-baremetal-package.sh` resolves
`MASTER_SHA`/`V11_SHA`/`CONTROL_SHA` by commit hash via `git archive`, not
by a branch/remote-qualified ref, and the `test_wait_primitive` fixture is
packaged from the kit's own `fixture-src/` snapshot -- it is never read
from any git ref, so no particular remote needs a particular name or a
particular branch fetched for it. Only step 1's `git fetch origin wet-v11
bench-v11-control` matters (substituting your remote's actual name for
`origin`), so both branches the three commits above live on are present in
the clone before packaging.

## Rocky Linux 8 preparation

One-time setup on a fresh Rocky Linux 8 executor host, as any user with
`sudo`:

```sh
sudo dnf install -y gcc make bison flex perl-core python39 numactl util-linux tar gzip tmux
```

- `python39` installs as `python3.9`. The kit also accepts `python3.11`
  (`sudo dnf install -y python3.11`, from the same base repos) if you
  prefer it. `00-check-host.sh` and every other kit script try, in order,
  `$PYTHON_BIN_OVERRIDE`, `python3.11`, `python3.9`, then the stock
  `python3` (3.6 on Rocky 8, too old to use) and report which interpreter
  they picked; if neither dnf package name matches what ends up installed,
  set `PYTHON_BIN_OVERRIDE=/path/to/your/python3.9-or-newer/binary`.
- `readline-devel` and `zlib-devel` are **not** needed: the kit's
  `configure` invocation always passes `--without-readline --without-zlib`.
  Only add them if you deliberately change that.
- `gcc` 8.5 (Rocky 8's default) is fully supported. The host check only
  warns on something older than gcc 8; it does not require anything newer.
  `binutils` (which provides `objdump`, used by `01b-disassemble.sh`) is
  pulled in automatically as a `gcc` dependency.
- CPU governor, without `cpupower` (not installed by the line above):
  ```sh
  cat /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
  ```
  Every line must read `performance`; if not, ask whoever administers the
  host to set it -- `00-check-host.sh` only reads this file, it never
  writes it.
- SELinux does not matter for this kit: everything it does (build,
  install, run PostgreSQL, write results) happens entirely under the
  executor account's own `$HOME`, which is unconfined under Rocky 8's
  stock targeted policy regardless of enforcing/permissive mode.

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
6. Capture `objdump` disassembly of seven hot-path functions
   (`01b-disassemble.sh`) from each of the four installed binaries, plus
   the exact `gcc`/`objdump` versions used, as codegen evidence on the
   actual benchmark toolchain (Rocky 8's gcc 8.5), for comparison against
   `reports/wpd-report.md` (built with gcc 13).
7. Run the plateau probe (`plateau-probe.sh`): eight vanilla-only W6c
   sessions (four with the dataset clone/initdb pinned to the server's
   NUMA node, four unpinned) and record which variant has the smaller
   session-to-session spread.
8. Run a short smoke matrix covering all 35 configuration/workload cells.
9. Wait automatically for one idle minute, then repeat the host check.
10. Run the full 560-cell measurement matrix (7 configurations x 5
    workloads x 16 repetitions).
11. Run the second-stage persistent-backend crossover (`crossover/`) on the
    patched installation.
12. Package and checksum the raw evidence from both stages, without final
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
- GCC 8 or newer (Rocky Linux 8's stock gcc 8.5 is fine), a Python 3.9+
  interpreter (the stock Rocky 8 `python3` is 3.6, too old -- see "Rocky
  Linux 8 preparation" below), Perl, Bison, Flex, GNU Make, `ar`, `ranlib`,
  `objdump`, `tar`, `lscpu`, and `taskset`. `numactl` is recommended (used
  by the plateau probe) but not required -- if it is absent and the host
  has more than one NUMA node, the check warns but continues, and the
  probe falls back to `taskset`-only pinning of the pinned variant.
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

1. `results-<hostname>-<date>.tar.gz` (matrix stage; includes
   `results/disassembly/`, the gcc-8.5 codegen evidence from step 6 above)
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
