# Bare-metal measurement runbook, wait-event tracing v11

<!-- This file is the source of truth. The top-level copy at
     ../BAREMETAL-RUNBOOK-v11.md is generated from it (kept identical by
     hand) so both the kit and the notes repo can each carry their own
     copy without drifting; edit only this one. -->

No SSH access or remote automation is required. Copy the two delivered files
to the executor account on one otherwise-idle Linux bare-metal host:

- `wet-v11-baremetal-r2.tar.gz`
- `wet-v11-baremetal-r2.tar.gz.sha256`

Put them directly under the executor account's home directory. Then verify,
extract, set this host's CPU masks, and start the complete run:

```sh
sha256sum -c wet-v11-baremetal-r2.tar.gz.sha256
tar -xzf wet-v11-baremetal-r2.tar.gz
cd wet-v11-baremetal-r2
lscpu -e                      # derive same-node masks -- see "Deriving CPU masks" below
export SERVER_CPUS=...        # 8 physical cores, one NUMA node (PostgreSQL)
export PGBENCH_CPUS=...       # the next 8 physical cores, the SAME node (pgbench)
./run-benchmark.sh
```

Run the final command inside `tmux` or `screen`. It is the only benchmark
command you need to start (it runs both the seven-configuration matrix and
the second-stage persistent-backend crossover). Do not run it as root.
Leave the host idle.

Unlike the v10 kit, this kit is not tied to one fixed two-socket topology:
you tell it which CPUs to use for PostgreSQL and which for pgbench, and it
verifies at runtime that both masks are non-empty, online, share no
physical core, and each stay within a single NUMA node (see "Deriving CPU
masks" below).

## Deriving CPU masks

Both `SERVER_CPUS` and `PGBENCH_CPUS` must be derived from the actual
target host's own `lscpu -e` output, every time -- masks copied from a
different host's topology are not portable (CPU numbering, which CPUs
belong to which NUMA node, and even how many nodes exist all vary by
machine and BIOS).

```sh
lscpu -e
```

Read the `NODE` column. **Both masks must land on the same NUMA node** --
`00-check-host.sh` refuses to start (no override) if either one spans
more than one node, because a cross-socket mask lets the scheduler and
the memory allocator split work and pages across both nodes, adding
run-to-run variance that has nothing to do with the code under test, and
makes the recorded `numa_local_fraction` evidence meaningless (a mask
that spans every node on the host makes "local" mean "anywhere," so the
fraction reads 1.0 regardless of where memory actually landed -- see
`reports/wpf-report.md` Addendum 6). Two node arrangements come up in
practice:

- **Contiguous numbering** (a node owns a contiguous block of CPU ids,
  e.g. node 0 = CPUs 0-31, node 1 = CPUs 32-63): `SERVER_CPUS` is the
  first 8 physical cores of one node, `PGBENCH_CPUS` the next 8 physical
  cores of the SAME node, e.g. `SERVER_CPUS=0-7 PGBENCH_CPUS=8-15`.
- **Interleaved numbering** (CPU ids alternate between nodes, e.g. even
  CPU ids = node 0, odd = node 1 -- seen on some two-socket Xeon
  platforms): use taskset's stride syntax (`start-end:stride`) to select
  8 physical cores from a single node without crossing to the other,
  e.g. on a host where node 1 is the odd CPUs 1,3,5,...,63:
  `SERVER_CPUS=1-15:2 PGBENCH_CPUS=17-31:2`.

Either way: 8 physical cores for the server, 8 for pgbench, both on the
same node (the v7 protocol) -- not more, not fewer. Too few starves the
workload; a server set much larger than the client counts the matrix
actually drives (32 clients at most, in W6c and the crossover)
undersubscribes it instead, letting the scheduler migrate PostgreSQL
backends across idle cores and adding scheduling variance unrelated to
the code under test (`00-check-host.sh` warns, non-blocking, if
`SERVER_CPUS` has more than 2x that 32-client maximum).

If either mask spans more than one NUMA node, `00-check-host.sh` refuses
to start and prints the live topology plus a concrete recommended pair
in the same taskset syntax shown above.

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
scp ../dist/wet-v11-baremetal-r2.tar.gz \
    ../dist/wet-v11-baremetal-r2.tar.gz.sha256 \
    executor@HOST:~/

# 3. Start the run under tmux on the host (see "Rocky Linux 8 preparation"
#    below if the host is not set up yet).
ssh executor@HOST
tmux new -s v11-bench
sha256sum -c wet-v11-baremetal-r2.tar.gz.sha256
tar -xzf wet-v11-baremetal-r2.tar.gz
cd wet-v11-baremetal-r2
lscpu -e                  # derive same-node masks -- see "Deriving CPU masks" above
export SERVER_CPUS=...   # 8 physical cores, one NUMA node
export PGBENCH_CPUS=...  # the next 8 physical cores, the SAME node
./run-benchmark.sh
# detach with Ctrl-b d; reattach any time from a new ssh session with:
#   tmux attach -t v11-bench

# 4. After it finishes (8 to 10 hours later), from the laptop: fetch the
#    two result archives run-benchmark.sh printed the paths for.
scp executor@HOST:~/wet-v11-baremetal-r2/results-*.tar.gz \
    executor@HOST:~/wet-v11-baremetal-r2/results-*.tar.gz.sha256 .
scp "executor@HOST:/var/tmp/w6c-persistent-crossover-*.tar.gz" \
    "executor@HOST:/var/tmp/w6c-persistent-crossover-*.tar.gz.sha256" .
./analyze-raw-archive.sh results-*.tar.gz
```

## Restarting after a failure

If the run dies partway through (for example, a plateau-probe crash), fix
the problem on the laptop, then get a fixed package back onto the host
without a fresh `git clone`:

```sh
# 1. On the laptop, in the notes checkout: pull the fix.
cd v11-notes && git pull

# 2. Rebuild the package (same command as the laptop workflow above).
cd bench-kit
POSTGRES_REPO_PATH=/path/to/pg ./make-baremetal-package.sh ../dist

# 3. Copy it to the host, same as before.
scp ../dist/wet-v11-baremetal-r2.tar.gz \
    ../dist/wet-v11-baremetal-r2.tar.gz.sha256 \
    executor@HOST:~/

# 4. On the host: extract into a NEW directory (or delete the old one
#    first) -- never extract a new package on top of an old one.
ssh executor@HOST
sha256sum -c wet-v11-baremetal-r2.tar.gz.sha256
tar -xzf wet-v11-baremetal-r2.tar.gz
cd wet-v11-baremetal-r2
```

The four builds from the failed run took 20 to 40 minutes and are still
good if nothing about the build changed (only e.g. a benchmarking-script
fix, not a source/patch change). Reuse them instead of paying for a
rebuild by copying the OLD extraction's `work/` directory into the new
one, then passing `--reuse-builds`.

**Copy `work/` only -- nothing else.** The rest of a failed run's
directory is either refused outright or would just be stale evidence from
the failed attempt sitting next to a fresh one:

- `results/`, `smoke-results/`, and any `results-*.tar.gz`/`.sha256`
  archive are **refused unconditionally** if present -- with or without
  `--reuse-builds` -- because a partial matrix can never be resumed or
  merged; every configuration has to run under the same machine
  conditions for the comparison to mean anything. If the failed run got
  as far as a smoke matrix (as in the case this note documents: it passed
  the plateau probe and the 35-cell smoke matrix, then died in
  smoke-verification), its `smoke-results/` is exactly the evidence a
  fresh `--reuse-builds` run needs to *not* see, or it refuses to start.
- `disassembly/` is refused if present **without** `--reuse-builds` (a
  disassembly run cannot be resumed either), but is harmless to leave
  out of the new extraction even with `--reuse-builds`: if it is absent
  there, `run-benchmark.sh` just re-runs `01b-disassemble.sh` fresh
  (a few minutes, not worth copying).
- `host-check.txt`/`host-check.json` and `plateau-probe-result.json` are
  not refused if present, but do not copy them either: the preflight,
  final-host-check, and plateau-probe phases regenerate all three from
  scratch on every run, and a copied-over one from the failed attempt's
  host state would just be stale (or, if it somehow disagreed with the
  new run's actual host state, confusing to debug).
- `run-logs/`, `status.json`, and `.benchmark.lock` belong to the failed
  run's own directory; a fresh extraction starts these clean on its own
  and none of them are things `--reuse-builds` looks at.

So the restart sequence is:

```sh
# The failed run's directory is abandoned entirely except for one thing:
cp -a ../wet-v11-baremetal-r1/work .   # the failed run's build output --
                                        # and only this
export SERVER_CPUS=...   # the same-node pair used for the original run
export PGBENCH_CPUS=...
./run-benchmark.sh --reuse-builds
```

`--reuse-builds` recomputes the SHA-256 of every bundled source archive and
every installed `postgres`/`pgbench`/`psql`/`initdb`/`pg_ctl` binary under
`work/install/` (plus the `test_wait_primitive` fixture and the
`pg_wait_event_tracing` module -- present, hash-matched, in the `patched`
build only; absent everywhere else, `build_manifest_rules.py`) and compares
them against `work/manifest.json`; it only skips
`01-build-all.sh`/`01b-disassemble.sh` if every one of those still matches
byte-for-byte, and refuses to run at all otherwise (it never silently
rebuilds or reuses something that no longer matches).

The fake-binaries self-test exercises exactly this restart sequence end to
end (`selftest-fakebin/run-selftest.sh`, the "--reuse-builds restart"
step): build once, delete everything but `work/`, extract a second fake
kit copy, copy that one directory across, and confirm `--reuse-builds`
both passes its own build-reuse verification and runs the rest of the
launcher to completion.

Changelog: r1 crashed in the plateau probe; r2 fixed that, then (still as
r2) failed again in smoke-verification because `analyze-results.py` had
its own, backwards copy of the module-presence rule (it required
`pg_wait_event_tracing` in the `control` build, when the rule everywhere
else has always been "patched" build only) -- see `reports/wpf-report.md`,
Addendum 5. `analyze-results.py` now calls `build_manifest_rules.py`
instead of encoding that rule itself.

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
- A fresh Rocky Linux 8 host (in particular a cloud/VM image) may already
  ship a PostgreSQL service, pre-installed and enabled, from an unrelated
  PGDG package. Check for one and stop and disable it before running this
  kit -- `00-check-host.sh` now refuses to start (see "Host requirements"
  below) if any `postgres`/`postmaster` process is running, on this host
  or any other cluster:
  ```sh
  systemctl list-units --type=service | grep -i postgres
  sudo systemctl stop <unit>       # e.g. postgresql-13.service
  sudo systemctl disable <unit>    # so it does not come back on reboot
  ```

## Rehearsing the launcher in a VM

`BENCHMARK_REHEARSAL=1` runs the real launcher against real PostgreSQL
builds and a real pgbench, inside a VM instead of dedicated bare metal --
useful for exercising everything the fake-binaries self-test structurally
cannot (real `configure`/`make`, real `pg_ctl`/`pgbench`, real timing)
before trusting a from-scratch kit on real hardware. The real build
(`configure`/`make` x4) is unchanged and still the dominant cost -- plan
for the runbook's normal 20-to-40-minute estimate, longer still under an
emulated, non-native-architecture VM. But the matrix/plateau-probe/
crossover repetition and session counts and the full-matrix measurement
window ARE compressed (1 repetition instead of 16, a 5-second measurement
window instead of 30, 2 plateau-probe and 2 crossover sessions instead of
8 and 16) so a rehearsal finishes in around an hour beyond the build,
not the runbook's 6.5-to-8.5-hour full-matrix estimate -- while still
exercising all 7 configurations and all 5 workloads, every phase, with
real binaries. Per-cell/session overhead that is not itself a repetition
count (quiescence, pgbench data-set scale, crossover block/settle/warmup
durations) stays at its real value, with two exceptions:

- The warmup window (`FULL_PROFILE`/`SMOKE_PROFILE` `warmup_seconds`)
  widens to 30s (from the real 10s/3s) under `BENCHMARK_REHEARSAL=1`. A
  real host passing its own smoke/full matrix at the tighter real value
  proves the mode-proof query (`recording-proof.sql`,
  `w3-qualification.sql`) has headroom there, but the same query can
  legitimately run slower on a VM or under emulation without that
  meaning anything about the patch -- found when a rehearsal died at
  smoke cell 12 (`config=trace`, `W4`) with "mode proof consumed the
  entire warmup window" under QEMU emulation. `02-run-matrix.sh`'s own
  guard for this is unchanged and still fail-closed outside rehearsal
  mode (no override); if even the widened 30s budget is not enough, it
  logs a REHEARSAL NOTE with the measured duration instead of failing
  the run. Every measured proof duration is recorded per cell, in both
  modes, as `results/recording-proofs/cell-<N>.csv.duration_s` -- useful
  on a real host too, to see actual headroom against the warmup budget
  before a slower one ever gets close to it.
- `FULL_PROFILE`'s `w1_iterations` drops to 1e6 (from the real 1e8) under
  `BENCHMARK_REHEARSAL=1`. W1 is a single backend looping over
  `test_wait_primitive` with no pgbench and no warmup/duration window
  bounding it the way pgbench workloads have, so nothing else limits its
  run time. A real host executes 1e8 iterations in well under a minute;
  under QEMU emulation, the underlying syscall per iteration
  (latch/timeout/file-read/usleep) is slow enough that a real rehearsal
  ran one W1 cell for 53+ minutes at 99% CPU before being killed by
  hand -- seven W1 cells (one per configuration) would have added
  roughly six hours. `SMOKE_PROFILE`'s `w1_iterations` (1e5 real) was
  never a problem and is unchanged.

Independently of both of the above, every matrix cell (`run_cell()` in
`02-run-matrix.sh`, wrapping server start through server stop) now has a
wall-time budget: 30 minutes in real mode, tightened to 10 minutes under
`BENCHMARK_REHEARSAL=1`. This is not a rehearsal-only relaxation -- the
real-mode budget is generous enough that no real cell has ever come
close to it, so it costs nothing there, but it means a genuinely hung
cell on real hardware also surfaces with a clear "cell exceeded its
wall-time budget" message naming the cell and elapsed time, instead of
silently consuming hours (exactly what the 53-minute W1 stall above
would otherwise have done indefinitely).

```sh
sha256sum -c wet-v11-baremetal-r2.tar.gz.sha256
tar -xzf wet-v11-baremetal-r2.tar.gz
cd wet-v11-baremetal-r2
lscpu -e
export SERVER_CPUS=...
export PGBENCH_CPUS=...
BENCHMARK_REHEARSAL=1 ./run-benchmark.sh
```

Set it in the environment before `./run-benchmark.sh` (or
`--preflight-only`); `00-check-host.sh`/`benchmark_protocol.py`/
`crossover/protocol.py` read it directly, same as
`SERVER_CPUS`/`PGBENCH_CPUS`. Beyond the repetition/session/window
compression above, it relaxes exactly four host checks that a VM can
never satisfy, logging each as a non-blocking "REHEARSAL NOTE" instead of
refusing to start:

- fewer than 8 physical cores;
- less than 16 GB total RAM;
- no readable cpufreq governor (common under QEMU/TCG, which does not
  expose one at all);
- virtualization detected (`systemd-detect-virt` reporting anything
  other than `none`).

Every other check is unchanged and still fail-closed: required tools,
gcc floor, disk space, load average, swap, CPU-affinity disjointness and
no-shared-physical-core, and -- above all -- every mode proof, build/
fixture hash, and statistical check the real matrix and crossover run.
`analyze-results.py`/`analyze-raw-archive.sh` read the
`benchmark_rehearsal` flag `00-check-host.sh` recorded in
`host-check.json` and stamp `analysis.md`/`analysis.json` and their
console output "REHEARSAL, NOT EVIDENCE" -- a rehearsal's numbers
describe a shared, resource-constrained, virtualized host measured with a
single repetition and a short window, and must never be cited as
performance evidence for the patch. Never set
`BENCHMARK_REHEARSAL=1` for a real evidence-gathering run.

`BENCHMARK_REHEARSAL_SKIP_SELFTEST=1`, honoured only alongside
`BENCHMARK_REHEARSAL=1` (real mode ignores it entirely, however it is
set), skips the `kit-self-test` phase's ~26-minute nested fake-binaries
self-test on later rehearsal attempts against an unchanged package. The
skip is justified by data, not by the operator's say-so:
`make-baremetal-package.sh` now runs `self-test.py` against the fully
staged package content *before* generating `PACKAGE-MANIFEST.sha256`
over it, and only then writes `PACKAGE-SELFTEST.json` (`"passed": true`,
a timestamp, and the packaging host) into the package -- so that file
becomes one more entry the manifest covers, tying the attestation to
this exact package's own already-verified integrity chain (there is no
way to copy it onto a different package, and a bare dev checkout, with
no `PACKAGE-MANIFEST.sha256` at all, can never produce one).
`run-benchmark.sh` only skips `kit-self-test` when `PACKAGE-SELFTEST.json`
is present and says `"passed": true` -- which, by the time it is read,
has already passed the package-integrity phase's `PACKAGE-MANIFEST.sha256`
check above it. Use this to iterate faster across several rehearsal
attempts against the same package (e.g. after a `--reuse-builds` restart
following an unrelated failure); the rehearsal that is finally reported
as evidence the launcher works on the target must still be one that ran
with `kit-self-test` included.

## What the launcher does

The launcher is fail-closed and runs these phases in order:

1. Verify `sources.conf` has no remaining placeholder (refuses to start
   otherwise) and verify every packaged script, workload, and source
   archive.
2. Check the Linux host and prerequisites, including the CPU-affinity
   masks above.
3. Run the self-test (`self-test.py`): synthetic-data tampering checks
   against `analyze-results.py`, plus a fake-binaries self-test
   (`selftest-fakebin/run-selftest.sh`) that drives `01-build-all.sh`
   (`SELFTEST_FAKE_PREFIX`), `plateau-probe.sh`, `01b-disassemble.sh`, a
   35-cell `02-run-matrix.sh` smoke matrix, and `03-collect.sh` through
   their real shell control flow against stub `pg_ctl`/`initdb`/
   `pgbench`/`psql` binaries -- catching bugs like a `pg_ctl stop` message
   leaking into a captured value (the cause of the plateau-probe crash
   r2 fixes) before a real server is ever started. No PostgreSQL server
   is started for this step; it also runs on the coordinator's laptop via
   `./run-benchmark.sh --preflight-only`.
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
- a co-resident `postgres`/`postmaster` process anywhere on the host, at
  either the initial preflight or the final host check before the full
  matrix (no override -- stop it first);
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
  physical core (checked via `lscpu -p=CPU,CORE,SOCKET,NODE`), **and each
  confined to a single NUMA node** -- `00-check-host.sh` refuses to start
  (no override) if either mask spans more than one node. A cross-socket
  mask lets the scheduler and the memory allocator split work and pages
  across both nodes, which adds run-to-run variance that has nothing to
  do with the code under test, and makes the recorded
  `numa_local_fraction` evidence meaningless (a mask that spans every
  node on the host makes "local" mean "anywhere," so the fraction reads
  1.0 no matter where memory actually landed -- see
  `reports/wpf-report.md` Addendum 6). Derive both masks from this
  host's own `lscpu -e` (see "Deriving CPU masks" below); do not reuse a
  mask pair from a different host's topology.
- At least 30 GB free under the extracted kit.
- GCC 8 or newer (Rocky Linux 8's stock gcc 8.5 is fine), a Python 3.9+
  interpreter (the stock Rocky 8 `python3` is 3.6, too old -- see "Rocky
  Linux 8 preparation" below), Perl, Bison, Flex, GNU Make, `ar`, `ranlib`,
  `objdump`, `tar`, `lscpu`, and `taskset`. `numactl` is recommended (used
  by the plateau probe) but not required -- if it is absent and the host
  has more than one NUMA node, the check warns but continues, and the
  probe falls back to `taskset`-only pinning of the pinned variant.
- CPU frequency governor set to `performance`.
- No active build, backup, or interactive workload, and **no other
  PostgreSQL server running anywhere on the host** -- `00-check-host.sh`
  refuses to start (naming the offending `postgres`/`postmaster` pid(s))
  if it finds one, at both the initial preflight and the final host check
  run again just before the full matrix; there is no override. This used
  to be recorded as non-blocking co-resident provenance, with the actual
  refusal only ever surfacing hours later from `crossover/run.sh`'s own
  "otherwise idle" check, after the entire multi-hour measurement matrix
  had already run for nothing (reports/wpf-report.md, Addendum 5) --
  `crossover/run.sh`'s check still runs too, as a second line of defence,
  but should never be how this is first discovered. Stop (and, if it
  should not come back on reboot, disable) any such service first; see
  "Rocky Linux 8 preparation" below.
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
