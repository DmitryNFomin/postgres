# v11 wait-event tracing: notes, benchmark kit, and patches

`v11` is a five-patch series on top of PostgreSQL master
`311df1dc0392f06973cf98eac51d63cb007267ce`, with tip
`96c24a28006fb6ff66264998c698f5230d32ad26` on branch `wet-v11`. It adds
begin/end hooks for timed wait events, converts the wait_start/end call
sites to the timed pair, and layers a `pg_wait_event_tracing` module
(statistics level, then trace level) on top, plus a `test_wait_primitive`
test module. A companion branch, `bench-v11-control`, carries a single
extra commit that strips the hook-pointer test and slow-path call back
out of the timed pair (leaving only the volatile store); it is the
layout control used for the hook-null contrast in the benchmark and is
not part of the submitted series.

## What changed versus v8

- Cold out-of-line hook path with the hint: the hook check now sits
  behind an `unlikely()` branch hint and calls out to an out-of-line
  slow-path function only when a hook is actually installed, instead of
  inlining the hook-invocation logic at every wait_start/end site.
- Lazy per-process hook install and a non-chaining variant: hooks are
  installed lazily, once per backend, rather than eagerly at startup,
  and the hook variable itself is a single non-chaining pointer rather
  than a chain that every installer has to thread through.
- Single recording gate and local in-flight state: there is one gate
  that decides whether an event gets recorded at all, and the
  in-flight/duration bookkeeping is kept in local (per-call) state
  instead of shared mutable state touched on every call.

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
