# Bare-metal measurement runbook, wait-event tracing v8

For the person or agent running this. You do not need to know the patch.
Follow the steps; send back one archive. Everything is read-only except the
temporary data directories the scripts create.

**Status: READY.** The code under test is fixed at the commits listed in
section 3. Nothing here needs editing before you start.

## 1. What this measures and why it matters

The patch adds wait-event timing to PostgreSQL. Three costs must be measured
on real hardware, because the numbers go into a public mailing-list post:

1. **Off** — the feature compiled in but switched off. This must be
   indistinguishable from unmodified PostgreSQL. This is the number the
   reviewers care most about.
2. **Loaded but idle** — the collector module loaded, capture disabled.
3. **On** — capture enabled, at the statistics level and at the trace level.

An earlier round measured a stand-in implementation. This round measures the
real module, so the published numbers describe the code that will ship.

## 2. Host requirements

- **Dedicated bare metal, otherwise idle.** No other tenants, no builds, no
  backups running. A noisy host invalidates the whole run.
- Linux, 8+ physical cores, 16+ GB RAM, ~30 GB free disk.
- Build tools: gcc, meson, ninja, **python3**, perl, bison, flex, readline
  and zlib development packages, git.
- No root needed. Everything runs as an ordinary user under one directory.
- Nothing else may use the machine for the duration, about 3 to 4 hours.

Record these before starting (the scripts also capture them):
CPU model, core count, kernel version, CPU frequency governor, and whether
turbo/boost is enabled. Do not change them; just record them.

## 3. What you will run

Everything is built from one repository,
`https://github.com/DmitryNFomin/postgres.git`, at two fixed commits:

| Build | Branch | Commit |
|---|---|---|
| `baseline` | `bench-v8-baseline` | `765efece39ba3fb04fdf20b1dadcd9ecea76fbc9` |
| `patched` | `bench-v8-patched` | `d7b4584a901241258604eef1f03dfd6b3f1fa926` |

Both carry the same benchmark helper extension, byte for byte. The only
difference between them is the patch series itself.

There are **two builds but five run configurations** — configurations 2 to 5
share the `patched` binary and differ only in `postgresql.conf`:

| # | Name | Build | What it is |
|---|---|---|---|
| 1 | `master` | baseline | unmodified PostgreSQL, the baseline |
| 2 | `hook-null` | patched | patch present, collector not loaded |
| 3 | `module-off` | patched | collector loaded, capture disabled |
| 4 | `stats` | patched | capture at statistics level |
| 5 | `trace` | patched | capture at trace level |

Four workloads:

| ID | What it is | Why |
|---|---|---|
| W1 | isolated wait microbenchmark, 10^8 iterations | the direct per-wait cost |
| W3 | short lock contention, 8 clients | the case reviewers doubt most |
| W4 | pgbench read-only, 16 clients, 4 GB shared buffers | ordinary read workload |
| W6c | pgbench read-only, 32 clients, 32 MB shared buffers | eviction pressure |

Each cell is repeated 12 times — 240 runs — in one randomized order, with a
fresh server per run. That is what makes the comparison trustworthy: every
configuration meets the same machine conditions.

The scripts check their own work as they go. Each run proves the feature is
in the state it claims (module loaded or absent, capture at the expected
level) and, where capture is on, that it is genuinely recording. A run that
cannot prove this stops with an error rather than quietly producing a
meaningless number.

## 4. Steps

```
# 1. Get the kit (one archive, provided with this runbook)
tar xzf wet-v8-bench-kit.tar.gz && cd wet-v8-bench-kit

# 2. Check the host looks sane; prints CPU, governor, memory, disk
./00-check-host.sh

# 3. Build both configurations from the fixed commits (~12 min)
./01-build-all.sh

# 4. Run the matrix (2.5-3.5 h, unattended; safe under tmux/screen)
./02-run-matrix.sh

# 5. Package the results
./03-collect.sh      # writes results-<hostname>-<date>.tar.gz
```

Put the kit somewhere with a **short path** — directly under your home
directory is ideal. Unix sockets have a hard path-length limit and the
script will refuse to start if the path is too long.

If a step fails, stop and send the output. Do not re-run a partial matrix on
top of an existing results directory; the scripts refuse it anyway.

**CPU pinning (optional, off by default).** The kit can pin the server and
the benchmark client to separate CPU ranges via the `SERVER_CPUS` and
`PGBENCH_CPUS` environment variables. Leave them unset unless you have been
given specific values for this machine. Do not invent ranges.

## 5. Rules while it runs

- Do not use the machine for anything else, including logging in to "just
  check something". An interactive session skews the short-wait workload.
- Do not change CPU governor, turbo, or any sysctl mid-run.
- If the run is interrupted, start over from step 4. Partial matrices are not
  usable: the comparison depends on all configurations meeting the same
  conditions.

## 6. What to send back

One file: `results-<hostname>-<date>.tar.gz`, produced by step 5. It contains
the raw per-run numbers, the server logs, the build manifest with compiler
version and binary hashes, the randomization seed and run order, and the host
telemetry.

Do not edit, filter or summarize it. The analysis is done here, and the raw
rows are what make the published numbers checkable by reviewers.

**Privacy note:** the archive records the hostname, the OS user name and the
working directory paths. Say so when you hand it over; those are scrubbed
before anything is published.

## 7. Rough timings

| Step | Time |
|---|---|
| host check | seconds |
| build both configurations | 10 to 15 minutes |
| run the matrix | 2.5 to 3.5 hours |
| collect | a minute |

The matrix is dominated by creating a fresh database for each of the 240
runs. That cost is deliberate: it is what keeps the runs independent.

## 8. If something looks wrong

Send the output rather than fixing it. Useful signals to mention: the host
check reporting a non-performance governor, any build failure, any server
that fails to start, a run that takes dramatically longer than its peers, or
any error mentioning that capture was not active.
