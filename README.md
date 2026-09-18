# v11 wait-event tracing: notes, benchmark kit, and patches

`v11` is a five-patch series on top of PostgreSQL master
`311df1dc0392f06973cf98eac51d63cb007267ce`, with tip
`96c24a28006fb6ff66264998c698f5230d32ad26` on branch `wet-v11`. It adds
begin/end hooks for timed wait events, converts the wait_start/end call
sites to the timed pair, and layers a `pg_wait_event_tracing` module
(statistics level, then trace level) on top, plus a `test_wait_hook`
test module that exercises the hook contract (patch 0003).
`test_wait_primitive` is NOT part of the series; it is the W1 benchmark
fixture carried inside the kit. A companion branch, `bench-v11-control`, carries a single
extra commit that strips the hook-pointer test and slow-path call back
out of the timed pair (leaving only the volatile store); it is the
layout control used for the hook-null contrast in the benchmark and is
not part of the submitted series.

## What changed versus v8

- Cold out-of-line hook path with a branch hint (patch 0001): the inline
  timed pair is one `unlikely()`-hinted pointer test per side; the
  enabled path (depth guard, indirect call) lives in two cold, noinline
  functions in wait_event.c. The hint is needed because GCC's static
  predictor treats a pointer compared with NULL as non-NULL and would
  otherwise lay out the enabled path as the fall-through. See
  `reports/wpd-report.md` for the x86-64 GCC disassembly evidence.
- Lazy per-process hook installation (patch 0004): the module installs
  `wait_event_begin_hook`/`wait_event_end_hook` in a process only when
  `pg_wait_event_tracing.capture` first becomes non-off in that process,
  and never removes them, so backends that never enable capture run the
  hook-null path. Chaining onto a previously installed consumer is
  preserved; when there is no previous hook, a non-chaining wrapper
  variant is installed so the hot path carries no previous-hook test.
  Diagnostic: `pg_wait_event_tracing_hooks_installed()`.
- Single recording gate and process-local in-flight state (patch 0004):
  each level has one backend-local pointer that is non-NULL exactly when
  recording is allowed, maintained at every site that changes its
  inputs, replacing three tests per side on the hot path; the current
  wait's start time and event code are process-local statics instead of
  fields in the shared payload. The recorded data is identical to v8;
  recording inside the capture assign hook is masked to what the stored
  GUC value permitted, so the module's own attach waits are not counted.

## Producing the benchmark package

On a machine with GitHub access:

```
git clone git@github.com:DmitryNFomin/postgres.git pg && cd pg
git fetch origin wet-v11 bench-v11-control
git worktree add ../v11-notes origin/v11-notes   # or checkout
cd ../v11-notes/bench-kit
POSTGRES_REPO_PATH=/path/to/pg ./make-baremetal-package.sh ../dist
```

Then copy `dist/wet-v11-baremetal-r1.tar.gz` and its `.sha256` sidecar to
the bare-metal host and follow `BAREMETAL-RUNBOOK-v11.md` (also present
in this repo at the top level, and inside the package itself).

The `crossover/` second-stage (persistent-backend crossover) is run
directly from inside this same package by `run-benchmark.sh` — it calls
`crossover/run.sh` for a smoke pass and then the full run, in sequence,
as phases of the one benchmark invocation. There is no separate
crossover package to build or ship: `crossover/make-package.sh` exists
in the kit but is not invoked by `run-benchmark.sh`, and the executor
does not need to run it. It is a standalone tool for producing a
crossover-only bundle should that ever be wanted on its own; ignore it
for a normal end-to-end run.

Expected duration: 8 to 10 hours end to end (matrix stage plus the
second-stage crossover). When it finishes, send back the two archives
`run-benchmark.sh` prints paths for: the matrix archive/checksum and the
crossover archive/checksum, unedited.

The package produced from this exact tree, with `sources.conf` pinned
to the three SHAs above, has this SHA-256 (`wet-v11-baremetal-r1.tar.gz`,
92900734 bytes):

```
0a5121a466fa89bb4040832ebb16b3892fa66474758d6235fcc43435d1db1641
```

The executor's own build from a fresh clone should reproduce this exact
digest; if it does not, stop and compare `sources.conf` and the patch
sets before running anything.

## Where to look for more

- `reports/wpd-report.md` — codegen evidence (disassembly/compiler output
  confirming the intended inlining/out-of-lining behavior of the hook
  sites).
- `reports/wpe-report.md` — how the five-patch series was assembled and
  reviewed.
- `bench-kit/` — the full benchmark kit source (scripts, workloads,
  `crossover/`, the `test_wait_primitive` fixture, stats tooling,
  `sources.conf`).
- `patches-v11/` — the five submitted v11 patches.
- `patches-control/` — the single control patch (layout control only,
  not submitted).
- `codegen/` — supporting codegen evidence/artifacts referenced by the
  wpd report.
- `briefs/` — the coordination briefs this work was executed against.
