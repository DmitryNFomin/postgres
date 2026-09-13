# Bare-metal measurement runbook, wait-event tracing v8

For the person or agent running this. You do not need to know the patch.
Follow the steps; send back one archive. Everything is read-only except the
temporary data directories the scripts create.

**Status: DRAFT — the commit hash and the prepared branch name are filled in
when the series is assembled. Do not start before you get the final version.**

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
- Build tools: gcc, meson, ninja, perl, bison, flex, readline and zlib
  development packages, git.
- No root needed. Everything runs as an ordinary user under one directory.
- Nothing else may use the machine for the duration, about 3 hours.

Record these before starting (the scripts also capture them):
CPU model, core count, kernel version, CPU frequency governor, and whether
turbo/boost is enabled. Do not change them; just record them.

## 3. What you will run

Five configurations of the same PostgreSQL commit:

| # | Name | What it is |
|---|---|---|
| 1 | `master` | unmodified PostgreSQL, the baseline |
| 2 | `hook-null` | the patch's core hook present, no collector loaded |
| 3 | `module-off` | collector loaded, capture disabled |
| 4 | `stats` | capture at statistics level |
| 5 | `trace` | capture at trace level |

Four workloads:

| ID | What it is | Why |
|---|---|---|
| W1 | isolated wait microbenchmark, 10^8 iterations | the direct per-wait cost |
| W3 | short lock contention, 8 clients | the case reviewers doubt most |
| W4 | pgbench read-only, 16 clients, 4 GB shared buffers | ordinary read workload |
| W6c | pgbench read-only, 32 clients, 32 MB shared buffers | eviction pressure |

Each cell is repeated 12 times, in randomized order, with a fresh server per
run. That is what makes the comparison trustworthy: every configuration meets
the same machine conditions.

## 4. Steps

```
# 1. Get the kit (one archive, provided with the final runbook)
tar xzf wet-v8-bench-kit.tar.gz && cd wet-v8-bench-kit

# 2. Check the host looks sane; prints CPU, governor, memory, disk
./00-check-host.sh

# 3. Build all five configurations from the fixed commit (~25 min)
./01-build-all.sh

# 4. Run the matrix (~2.5 h, unattended; safe to run under tmux/screen)
./02-run-matrix.sh

# 5. Package the results
./03-collect.sh      # writes results-<hostname>-<date>.tar.gz
```

If a step fails, stop and send the output. Do not re-run a partial matrix on
top of an existing results directory; the scripts refuse it anyway.

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
version and binary hashes, and the host telemetry.

Do not edit, filter or summarize it. The analysis is done here, and the raw
rows are what make the published numbers checkable by reviewers.

**Privacy note:** the archive records the hostname, the OS user name and the
working directory paths. Say so when you hand it over; those are scrubbed
before anything is published.

## 7. Rough timings

| Step | Time |
|---|---|
| host check | seconds |
| build five configurations | 20 to 30 minutes |
| run the matrix | 2 to 2.5 hours |
| collect | a minute |

## 8. If something looks wrong

Send the output rather than fixing it. Useful signals to mention: the host
check reporting a non-performance governor, any build failure, any server
that fails to start, or a run that takes dramatically longer than its peers.
