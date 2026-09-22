# v11 wait-event tracing: notes, benchmark kit, and patches

`v11` is a five-patch series on top of PostgreSQL master
`311df1dc0392f06973cf98eac51d63cb007267ce`, with tip
`e26e633332292da0a104b22ad8c38282346faf8d` on branch `wet-v11`. It adds
begin/end hooks for timed wait events, converts the wait_start/end call
sites to the timed pair, and layers a `pg_wait_event_tracing` module
(statistics level, then trace level) on top, plus a `test_wait_hook`
test module that exercises the hook contract (patch 0003).
`test_wait_primitive` is NOT part of the series; it is the W1 benchmark
fixture carried inside the kit. A companion branch, `bench-v11-control`, carries a single
extra commit that strips the hook-pointer test and slow-path call back
out of the timed pair (leaving only the volatile store); it is the
layout control used for the hook-null contrast in the benchmark and is
not part of the submitted series. `bench-v11-control` is expected to
fail this fork's CI: the module's own regress/TAP suites require the
hook sites this branch deliberately compiles out. Its commit message
ends with `[skip ci]` for exactly that reason, and it is excluded from
CI on that basis, not omitted by oversight.

## What v11 contains beyond v8

v11 includes the two optimisation commits measured in the v9 and v10
rounds, folded into the series: v9 (hook pointer loaded once per timed
report; the end path reads the volatile wait-event value only when a
hook will consume it; the depth guard restored with direct stores) is
folded into patch 0001, and v10 (an always-inline test of the
backend-local attach-needed flag in front of the out-of-line attachment
path, reached from every parsed and executed statement) is folded into
patch 0004. On top of those, v11 adds:

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

The clone's remote name does not matter -- it is `origin` above only
because that is what a plain `git clone` names it; the package build does
not read any remote-qualified ref. `MASTER_SHA`/`V11_SHA`/`CONTROL_SHA`
are resolved by commit hash, and the `test_wait_primitive` (W1) fixture is
packaged from the kit's own `bench-kit/fixture-src/` snapshot, never read
from git. The only thing that must happen first is `git fetch origin
wet-v11 bench-v11-control` (substituting your remote's actual name), so
both branches the three pinned commits live on are present in the clone.

Then copy `dist/wet-v11-baremetal-r2.tar.gz` and its `.sha256` sidecar to
the bare-metal host and follow `BAREMETAL-RUNBOOK-v11.md` (also present
in this repo at the top level, and inside the package itself) -- see its
"Laptop workflow" section for the full clone-to-scp-to-ssh-to-tmux path,
and its "Rocky Linux 8 preparation" section for the one-time host setup
(package list, Python 3.9+ interpreter selection, governor check without
`cpupower`, and why SELinux does not matter here). This kit builds on
either macOS or Linux; make-baremetal-package.sh falls back from
`sha256sum` to `shasum -a 256` when only the latter is present (stock
macOS).

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
to the three SHAs above, has this SHA-256 (`wet-v11-baremetal-r2.tar.gz`,
93009599 bytes):

```
a241464d1cd6dfbdc680ce781b78efccc6784f23697ec90d1c036ca7fd74f408
```

The outer `.tar.gz` embeds each file's mtime and is not byte-for-byte
reproducible across separate builds (two builds from the identical tree
and inputs do not produce this same digest); what matters is that the
package's *contents* match, which `PACKAGE-MANIFEST.sha256` inside the
package verifies per-file, and which `run-benchmark.sh` checks before
starting. If the executor's own build's digest differs from the one
above, extract both and diff `PACKAGE-MANIFEST.sha256`; if that content
manifest differs too, stop and compare `sources.conf` and the patch
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
