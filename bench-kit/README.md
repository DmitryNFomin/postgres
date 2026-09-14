# PostgreSQL wait-event-tracing benchmark kit (v8)

This kit measures the performance cost of a PostgreSQL patch series that
adds wait-event timing (how long the server spends waiting on locks, I/O,
and similar events). You do not need to understand the patch to run it --
just follow the four commands below, in order, on a quiet, dedicated Linux
machine.

It corresponds to `BAREMETAL-RUNBOOK-v8.md`. If anything here and that
runbook disagree, this README describes what the scripts actually do.

## The two-builds, five-configurations point

This is the one thing worth understanding before you start, because it is
why the build step is fast (~12 minutes) rather than slow (~25 minutes).

There are only **two** PostgreSQL builds:

| Build | Branch | What it is |
|---|---|---|
| `baseline` | `bench-v8-baseline` | Unmodified PostgreSQL, plus the small benchmark helper extension |
| `patched` | `bench-v8-patched` | The wait-event-tracing patch series on the same commit, plus the same helper extension |

But the measurement matrix runs **five** configurations, because four of
them are the *same* `patched` binary started with a different
`postgresql.conf`:

| # | Configuration | Build | postgresql.conf |
|---|---|---|---|
| 1 | `master` | baseline | (nothing extra) |
| 2 | `hook-null` | patched | (nothing extra -- collector not preloaded) |
| 3 | `module-off` | patched | `shared_preload_libraries = 'pg_wait_event_tracing'`, capture off |
| 4 | `stats` | patched | collector loaded, capture at the statistics level |
| 5 | `trace` | patched | collector loaded, capture at the trace level |

`01-build-all.sh` therefore compiles exactly twice. `02-run-matrix.sh`
reuses those two installed trees for all five configurations, switching
only the config file between runs.

## The extension's exact names

Read directly from `contrib/pg_wait_event_tracing/pg_wait_event_tracing.c`
in the patched tree, not guessed:

- Library / extension name: **`pg_wait_event_tracing`**
  (`shared_preload_libraries = 'pg_wait_event_tracing'`)
- Capture-level GUC: **`pg_wait_event_tracing.capture`**, one of the exact
  spellings `off`, `stats`, or `trace` (not "statistics" -- the module
  spells the middle level `stats`).
- Shared-memory sizing GUCs (both `PGC_POSTMASTER`, i.e. only take effect
  at server start): `pg_wait_event_tracing.max_tranches` (default 192) and
  `pg_wait_event_tracing.trace_ring_size` (default 4 MB, must be a power
  of two). The kit sets both explicitly at their defaults for the `stats`
  and `trace` configurations, so the configuration is self-documenting.

## Prerequisites

- A dedicated, otherwise-idle bare-metal Linux machine. No other tenants,
  builds, or backups running. 8+ physical cores, 16+ GB RAM, ~30 GB free
  disk under wherever you put this kit.
- Build tools: gcc, meson, ninja, perl, bison, flex, git, and the readline
  and zlib development packages.
- `python3`. It is not called out separately in the runbook, but `meson`
  itself needs it, so any machine that can build PostgreSQL already has
  it. The scripts use it for small JSON/CSV/statistics tasks.
- No root required. Everything lives under this kit's own directory.
- About 3 hours where nothing else touches the machine.

## The four commands, in order

```sh
# 1. Look at the machine. Read-only; never changes a setting. Seconds.
./00-check-host.sh

# 2. Build both trees from the pinned commits below. ~12 minutes.
./01-build-all.sh

# 3. Run the full matrix: 5 configurations x 4 workloads x 12 repetitions,
#    in one randomized order. ~2.5-3.5 hours. Safe under tmux/screen.
./02-run-matrix.sh

# 4. Package everything into one archive.
./03-collect.sh
```

If any step fails, it prints which cell (if any) failed and stops rather
than continuing with a half-finished matrix -- send that output back
rather than trying to fix it yourself.

### The commit this kit measures

`01-build-all.sh` ships with these already filled in near the top of the
file -- nothing to edit before running it:

```sh
REPO_URL="https://github.com/DmitryNFomin/postgres.git"
COMMIT_BASELINE="765efece39ba3fb04fdf20b1dadcd9ecea76fbc9"   # tip of bench-v8-baseline
COMMIT_PATCHED="d7b4584a901241258604eef1f03dfd6b3f1fa926"    # tip of bench-v8-patched
```

`COMMIT_BASELINE` is unmodified PostgreSQL plus the benchmark fixture;
`COMMIT_PATCHED` is the same base commit with the wait-event-tracing
series applied, plus the byte-identical fixture. The script still checks
for a stray `@@...@@` marker and refuses to run if it ever finds one (for
instance, if this kit is repurposed for a different pinned commit pair
later and the edit is only half done).

## What each step actually does

- **`00-check-host.sh`**: prints and saves (to `host-check.txt` /
  `host-check.json`) the CPU model, physical core count, hyperthreading
  state, kernel version, per-core frequency governor, turbo/boost state,
  memory, swap, free disk, load average, and other running processes. It
  warns loudly -- but does not stop -- if the governor is not
  `performance`, load average is above 0.5, free disk is under 30 GB, or
  there are fewer than 8 physical cores. It never changes anything.

- **`01-build-all.sh`**: clones the fork once (or fetches if already
  cloned), checks out `bench-v8-baseline` and `bench-v8-patched` at the
  pinned commits into separate git worktrees, and builds each with:

  ```sh
  meson setup <builddir> <srcdir> --prefix=<prefix> --buildtype=release -Dcassert=false
  ```

  `--buildtype=release` is meson's fully optimized build (comparable to
  `-O3`, no debug info) -- release-equivalent optimization, not this
  project's normal development default (`debugoptimized`). `-Dcassert=false`
  turns assertions off. **This is deliberate: a performance measurement
  must not carry assertion overhead, so this is a timing build, not a
  correctness build.** Both trees use the identical flags. The benchmark's
  wait-primitive microbenchmark extension (`src/test/modules/test_wait_primitive`,
  present on both branches) is then built and installed with PGXS against
  each tree's own `pg_config`, since meson only stages test modules for
  its own temporary test installs, not for a normal `--prefix` install.
  Compiler version, the configure line, each branch's commit, and each
  installed `postgres` binary's SHA-256 go into `work/manifest.json`. The
  script also hard-fails if the `patched` build does not contain the
  `pg_wait_event_tracing` shared library, or if the `baseline` build does
  -- a wrong commit on either branch would otherwise build "successfully"
  while silently measuring nothing.

- **`02-run-matrix.sh`**: builds one random permutation of all 240 cells
  (5 configurations x 4 workloads x 12 repetitions) up front, prints and
  saves the seed used, and then runs every cell in that single order --
  deliberately not grouped by configuration or by workload, so that
  thermal drift or background noise cannot systematically favor one
  configuration. Each cell gets a brand-new `initdb`, a server start, the
  measurement, a server stop, and removal of that cell's data directory,
  all under `results/`, never under `/tmp`. Every pgbench-driven workload
  runs for `warmup + duration` seconds continuously and only the
  progress samples taken *after* the warmup window count towards the
  reported throughput and latency -- the same warmup-then-discard method
  the earlier (v7) measurement harness used, just folded into one
  `pgbench` invocation instead of two. The microbenchmark workload (W1)
  similarly makes one small discarded warmup call before the measured
  call. One row per cell is appended to `results/results.csv` as it
  completes; each server's log is saved under `results/logs/`.

  Before running the workload, every cell **proves** the server is
  actually in the state its configuration claims, rather than trusting
  that `shared_preload_libraries`/capture took effect: for `master` and
  `hook-null` it asserts the module is absent (`SHOW pg_wait_event_tracing.capture`
  must fail, and `shared_preload_libraries` must not name the module); for
  `module-off`/`stats`/`trace` it asserts the module is loaded and
  `SHOW pg_wait_event_tracing.capture` returns exactly `off`/`stats`/`trace`.
  For `stats` and `trace` only, it additionally checks, while running the
  workload, that the collector recorded a non-zero number of calls (see
  "Proving the collector is really on" below for exactly when and how) --
  a `stats` or `trace` cell that recorded nothing is treated as a failed
  cell, not a fast one, and `die()`s with the cell identity rather than
  continuing.

- **`03-collect.sh`**: packages `results/results.csv`, the schedule and
  seed, `telemetry.jsonl`, `protocol.json`, every server log, the build
  manifest, and the host-check report into
  `results-<hostname>-<date>.tar.gz`, prints its path and size, and warns
  (without refusing) if the CSV has fewer than 240 data rows.

## The workloads

| ID | What it is | shared_buffers | Notes |
|---|---|---|---|
| W1 | isolated wait-primitive microbenchmark, 10^8 iterations | 128 MB | `test_wait_primitive_latch_set()`, called once as a small discarded warmup, once measured |
| W3 | short LWLock contention, 8 pgbench clients | 16 MB | custom one-line script: `SELECT test_wait_primitive_lwlock_contention(100);` |
| W4 | pgbench read-only (`-S`), 16 clients | 4 GB | scale 100; every heap page is scanned once before timing so the measured window is warm-cache, not cold-start I/O |
| W6c | pgbench read-only (`-S`), 32 clients | 32 MB | scale 100; deliberately *not* pre-warmed -- the dataset does not fit, and that eviction pressure is the point |

pgbench's own initialization uses `--unlogged-tables` for W4 and W6c. This
only affects the (discarded) setup phase -- both workloads are pure
`SELECT`-only during the measured window, so WAL behavior never enters the
measurement -- and it matters here specifically because this kit
re-initializes the dataset from scratch for every one of the 120 W4/W6c
cells (see "fresh data directory per cell" above), so cutting setup time
matters far more than it would in a harness that reused one dataset across
many runs.

## Proving the collector is really on

A `stats`/`trace` cell that silently recorded nothing would come back
looking identical to `module-off` -- a false "wait tracing is free"
result, and exactly the kind of bug this kit exists to catch rather than
publish. So for the `stats` and `trace` configurations only,
`02-run-matrix.sh` checks, in addition to the `SHOW` checks described
above, that the collector actually collected something:

- **W1**: the same connection that makes the measured
  `test_wait_primitive_latch_set()` call also reads back
  `pg_stat_get_wait_event_timing(pg_backend_pid())` (summed `calls`) in
  the same statement, before that connection closes.
- **W3/W4/W6c**: roughly halfway through the (discarded) warmup window --
  about 5 seconds in, with the default 10-second warmup -- while pgbench's
  client connections are fully up and generating waits, a separate `psql`
  connection reads `SELECT coalesce(sum(calls), 0) FROM pg_stat_wait_event_timing`
  (the cluster-wide view). This is the same concurrent-snapshot technique
  the v7 harness used for its own W3 qualification check, but timed to
  land inside the warmup window rather than near the end of the run, so it
  never touches any of the thirty one-second samples that make up the
  reported TPS/latency for that cell.

Both checks read from `pg_stat_wait_event_timing` (or the SRF underneath
it, `pg_stat_get_wait_event_timing()`), defined in
`contrib/pg_wait_event_tracing/pg_wait_event_tracing--1.0.sql`. The
concurrent-snapshot design matters here, not just as a v7 habit: this
module frees a backend's own recorded stats when that backend
disconnects, so checking *after* pgbench has already exited would always
read zero, regardless of whether capture actually worked. Either check
coming back zero is treated as a failed cell (`die()`), not a fast one.

## CPU pinning (optional)

The supported default is to leave the server and pgbench client processes
unpinned and let the OS scheduler place them -- this kit does not know
your machine's core topology and will not guess it. If you do know it and
want to isolate the server from client-side contention (worthwhile when
chasing a small effect), set `SERVER_CPUS` and/or `PGBENCH_CPUS` before
running `02-run-matrix.sh`, in whatever range syntax `taskset -c` accepts:

```sh
SERVER_CPUS=0-7 PGBENCH_CPUS=8-15 ./02-run-matrix.sh
```

Each variable is validated up front (the script refuses to start if
`taskset -c` rejects it) and both are recorded in `results/protocol.json`
and as `server_cpus`/`pgbench_cpus` columns on every row of
`results/results.csv`, so the analysis always knows which mode produced a
given row.

## What differs from the earlier (v7) harness, on purpose

This kit reuses the earlier harness's statistical method (warmup-then-discard,
12 repetitions, a seeded/recorded random schedule, one CSV row per run) but
simplifies the mechanics for a five-configuration, four-workload matrix run
by a non-expert on a single unfamiliar machine:

- **Fresh `initdb` and a fresh data directory for every single cell**,
  removed immediately after. The v7 harness shared one prepared data
  directory across many repetitions of the same workload/config family to
  save time; this kit trades that speed for the simplicity of "every cell
  starts from the same, known-clean state," which is also what the
  runbook promises.
- **One global randomized order** across all 240 cells (configuration x
  workload x repetition), rather than a separate per-workload randomized
  block. This is a direct instruction for this kit, not an oversight.
- **CPU pinning is opt-in, not automatic.** The v7 harness always pinned
  the server and pgbench client processes to disjoint CPU sets supplied in
  its config file. This kit defaults to no pinning, since a wrong
  hard-coded CPU range on an unfamiliar machine is worse than none, and
  only applies `taskset` when `SERVER_CPUS`/`PGBENCH_CPUS` are explicitly
  set (see "CPU pinning (optional)" above) -- for someone who does know
  this machine's topology and wants it.
- **No W3 "qualification" snapshot or statistical gating.** The v7 harness
  computed LWLock-wait histograms and pass/fail equivalence margins live,
  during the run, to decide whether to keep going. This kit only records
  raw measurements (plus the narrower "did capture record anything at
  all" check described above, which exists to catch a broken measurement,
  not to judge the patch); all further analysis happens after the archive
  comes back, as the runbook's "what to send back" section describes.

## What to send back

One file: `results-<hostname>-<date>.tar.gz`, produced by `03-collect.sh`.
Do not edit, filter, or summarize it.

**Privacy note:** this archive records this machine's hostname, your OS
username, and the working directory paths used for the run (they show up
in the build manifest and in server log lines). Mention that when you hand
it over -- those details are scrubbed before anything is published.

## Rough timings

| Step | Time |
|---|---|
| `00-check-host.sh` | seconds |
| `01-build-all.sh` | ~12 minutes (two builds, not five) |
| `02-run-matrix.sh` | ~2.5-3.5 hours |
| `03-collect.sh` | under a minute |

## If something looks wrong

Send the output rather than fixing it yourself. Useful things to mention:
anything `00-check-host.sh` warned about, any build failure, any cell that
`02-run-matrix.sh` reports as failed, or a run that finishes with fewer
than 240 rows in `results/results.csv`.
