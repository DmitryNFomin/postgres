# Bare-metal benchmark kit v8: build report

Kit written to: `<workspace>/bench-kit-v8/`

This host was never used to start a PostgreSQL server, run `meson test`,
`pg_regress`, a TAP test, or `pg_ctl` -- per the hard rules, only the
`postgres/` git repository was read (via `git show <branch>:<path>` and
`git ls-tree`) to get exact names, and the kit's own shell scripts were
validated with `bash -n` and `shellcheck` only. No files in
`<workspace>/postgres` were modified.

## 1. Where the contrib module actually lives

`contrib/pg_wait_event_tracing/` does **not** exist on the current
`wet-series` branch (HEAD at the start of this task). It exists on branch
**`wet-v8`**, which carries the full v8 patch series described in
`IMPLEMENTATION-PLAN-v8.md`. All source quotes below are from
`contrib/pg_wait_event_tracing/pg_wait_event_tracing.c` as it exists on
`wet-v8` (read via `git show wet-v8:contrib/pg_wait_event_tracing/pg_wait_event_tracing.c`).

## 2. Exact GUC names and spellings (quoted, not guessed)

**Capture-level GUC**, `contrib/pg_wait_event_tracing/pg_wait_event_tracing.c:342-347` and `:3015-3025`:

```c
342: static const struct config_enum_entry pwet_capture_options[] = {
343:    {"off", PWET_CAPTURE_OFF, false},
344:    {"stats", PWET_CAPTURE_STATS, false},
345:    {"trace", PWET_CAPTURE_TRACE, false},
346:    {NULL, 0, false}
347: };
...
3015:   DefineCustomEnumVariable("pg_wait_event_tracing.capture",
3016:                            "Controls wait event collection.",
...
3019:                            PWET_CAPTURE_OFF,
3020:                            pwet_capture_options,
3021:                            PGC_SUSET,
```

So the GUC is **`pg_wait_event_tracing.capture`**, `PGC_SUSET`, and the
three legal values are exactly `off`, `stats`, `trace`. Note the middle
level is spelled **`stats`**, not "statistics" -- the task brief and the
runbook both say "statistics level" in prose, but the value you write in
`postgresql.conf` is `stats`. The kit uses `stats` (`02-run-matrix.sh`,
`append_config_lines()`).

**Shared-memory sizing GUCs**, both `PGC_POSTMASTER` (only take effect at
server start), `pg_wait_event_tracing.c:3026-3049`:

```c
3026:   DefineCustomIntVariable("pg_wait_event_tracing.max_tranches",
3027:                           "Maximum distinct LWLock tranches tracked per backend.",
...
3030:                           192,
3031:                           16,
3032:                           65534,
3033:                           PGC_POSTMASTER,
...
3038:   DefineCustomIntVariable("pg_wait_event_tracing.trace_ring_size",
3039:                           "Per-backend trace ring size.",
...
3042:                           4096,
3043:                           8,
3044:                           32768,
3045:                           PGC_POSTMASTER,
3046:                           GUC_UNIT_KB | GUC_NOT_IN_SAMPLE,
3047:                           pwet_check_trace_ring_size,
```

So: **`pg_wait_event_tracing.max_tranches`** (default 192, range 16-65534)
and **`pg_wait_event_tracing.trace_ring_size`** (default 4096, unit KB, so
default = 4 MB, range 8-32768 KB). The check hook
(`pwet_check_trace_ring_size`, `:369-380`) requires the KB value itself to
be a power of two ("Each record is 32 bytes, so kb is a power of two iff
the record count is"). The kit sets both explicitly to `192` and `'4MB'`
(= 4096 KB = 2^12, satisfies the power-of-two check) for the `stats` and
`trace` configurations, rather than leaving them as silent defaults.

**Extension / library name**, `contrib/pg_wait_event_tracing/pg_wait_event_tracing.control:4` and `Makefile:3`:

```
module_pathname = '$libdir/pg_wait_event_tracing'
MODULE_big = pg_wait_event_tracing
```

So `shared_preload_libraries = 'pg_wait_event_tracing'` is the exact
spelling used in `postgresql.conf`.

## 3. The benchmark fixture (`test_wait_primitive`)

Not part of `contrib/`; it is a `src/test/modules/test_wait_primitive`
helper, provided as "exact source" in
`v7_review/.../source/fixture/test-wait-primitive-exact-source.tar.gz`.
Its SQL file (`test_wait_primitive--1.0.sql`) defines exactly these
functions, which the kit calls by these names:

- `test_wait_primitive_latch_set(bigint) RETURNS double precision` -- W1,
  ns/iteration, called once as a small discarded warmup and once measured
  at 10^8 iterations.
- `test_wait_primitive_lwlock_contention(bigint) RETURNS bigint` -- W3,
  invoked once per pgbench transaction via a one-line custom script
  (`SELECT test_wait_primitive_lwlock_contention(100);`), which the kit
  generates on the fly rather than shipping as a separate file.
- (`latch_timeout`, `file_read`, `usleep0`, `report_only` also exist but
  are not used by this kit -- the task specified W1 as a single 10^8-iteration
  primitive, and `latch_set` at 10^8 is exactly what the v7 harness used
  for that iteration count.)

I could not find `test_wait_primitive.c` at the tip of any current branch
(it shows up in git history under an older commit,
`39097830952 test_wait_primitive: microbenchmark for wait primitive overhead`,
not reachable from `wet-v8`'s tip); I relied on the "exact source" fixture
tarball instead, since the task pointed at it directly and it is
internally consistent with how the v7 harness calls it. The kit assumes
the fixture lives at the same path, `src/test/modules/test_wait_primitive`,
on both `bench-v8-baseline` and `bench-v8-patched` (stated as an assumption
in `01-build-all.sh`'s comments and below in "Things I was unsure about").

## 4. Kit file list

```
<workspace>/bench-kit-v8/
├── 00-check-host.sh   244 lines  read-only host sanity check
├── 01-build-all.sh    227 lines  clone + two meson builds + manifest
├── 02-run-matrix.sh   571 lines  the 240-cell measurement driver
├── 03-collect.sh       96 lines  packages the results archive
└── README.md          228 lines  executor-facing instructions
```

Nothing else was written under the kit directory. The one-line W3 SQL
script and all of `work/`, `results/`, `host-check.*` are created by the
scripts themselves at run time on the executor's machine, not shipped.

## 5. What was reused from the v7 harness, and what was written fresh

Found under `v7_review/wait-hook-peer-review-20260909.tar` ->
`tests/executed-harness/amended/` (the executed "amended" variant, per
`AMENDED-PROTOCOL-W1.md`) and its non-amended sibling
`tests/executed-harness/strict/run_full_matrix.sh` (916 lines), which the
amended variant wraps and does not replace.

**Reused directly (same method, adapted to bash/Python already used
there):**

- Warmup-then-discard for pgbench workloads: one continuous
  `pgbench -T (warmup+duration) -P 1` run; only progress lines with
  `elapsed > warmup` count toward the mean TPS; latency computed from
  Little's Law (`1000 * clients / tps`) rather than pgbench's own summary
  latency. This is `record_pgbench()`'s post-warmup branch in
  `run_full_matrix.sh:513-556`, reused nearly verbatim as a Python snippet
  inside `run_pgbench_measured()` in `02-run-matrix.sh`.
- W1's warmup formula: `warmup = iterations / 100`, floored to at least 1,
  called once and discarded before the measured call
  (`run_full_matrix.sh:748-772`, `run_w1_cell()`).
- The pgbench regex for progress lines:
  `r"^progress: ([0-9.]+) s, ([0-9.]+) tps"` (`run_full_matrix.sh:539-541`),
  copied as-is.
- Pre-warming the full table scan only for the workload whose dataset fits
  shared_buffers (v7's W4, this kit's W4), and deliberately not doing so
  for the workload with eviction pressure (v7's W6-family, this kit's
  W6c) -- `run_full_matrix.sh:693-696` and its `shared_buffers_for()`.
- Workload definitions: W3's `-c 8 -j 8` against a custom LWLock-contention
  script, W4's `-S -c 16 -j 8` @ 4 GB, W6c's `-S -c 32 -j 8` @ 32 MB,
  and pgbench scale 100 -- `run_full_matrix.sh`'s `pgbench_args()`,
  `clients_for()`, `shared_buffers_for()`.
- A seeded, recorded, reproducible random schedule generator
  (`random.Random(seed).shuffle(...)`, schedule written to a CSV/TSV,
  seed derived/recorded up front) -- `write_schedule()` in
  `run_full_matrix.sh:785-825`. The *shape* of the schedule differs (see
  below), but the mechanism (seed a PRNG, shuffle, write the order to a
  file before running) is the same idea.
- `stop_server`/error-trap structure: track the currently-running
  server's prefix and data directory in globals, `trap stop_server EXIT`,
  best-effort `pg_ctl -m fast stop` (`run_full_matrix.sh:353-363`).
- CPU-frequency telemetry via `/sys/devices/system/cpu/cpu*/cpufreq/scaling_cur_freq`,
  recorded before/after each cell as JSON lines (`record_telemetry()`,
  `run_full_matrix.sh:255-292`).
- SHA-256 build manifest with compiler version, configure line, and binary
  hashes (`package_common.sh`'s `package_sha256`, and
  `build_full_matrix.sh`'s manifest-writing).

**Written fresh for this kit, because the task explicitly asks for a
different structure than v7 used:**

- **One global randomized schedule** across all 5 configs x 4 workloads x
  12 repetitions (240 cells), not blocked by configuration or by workload.
  v7 built a separate Latin-square-style schedule *per workload* (each
  repetition = all configs in one random column order); the task
  explicitly asked for the coarser, simpler "whole matrix, one shuffle"
  design instead, so I did not carry over v7's per-workload Latin square.
- **Fresh `initdb` + fresh data directory for every single cell**, removed
  immediately after. v7 prepared one base data directory per
  workload/config family and reused it (via `cp --reflink`) across all 12
  repetitions, to save time. The task's hard requirement
  ("Fresh initdb + server start per cell... data dir removed after the
  cell") is stricter than what v7 did, so this is new, and it is the
  reason `02-run-matrix.sh` uses `--unlogged-tables` during pgbench's
  setup phase for W4/W6c (documented in the README) -- a deliberate,
  new-to-this-kit optimization to keep 120 from-scratch pgbench
  initializations from blowing the ~2.5h target, safe specifically because
  both workloads are pure read-only during the measured window.
- **Two builds instead of many.** v7's `build_full_matrix.sh` built 13
  named configurations (master, master-aa, v6-off, several
  Borodin/corrected variants and their `-stub` layouts). This kit's task
  collapses that to exactly two real builds (`baseline`, `patched`) with
  four of the five run-time configurations sharing the `patched` binary
  and differing only by `postgresql.conf` -- this "stage_alias" idea
  existed in v7 too (`build_full_matrix.sh`'s `stage_alias()` symlinking
  `*-module-off`/`*-stats`/`*-trace` to one `*-null` build), so the
  *concept* is v7's, but the five-name mapping and the conf-line generator
  (`append_config_lines()` in `02-run-matrix.sh`) are new, built directly
  from the exact GUC names above rather than v7's now-superseded names
  (`wait_event_capture`, `pg_wait_event_timing.capture`).
- **No CPU pinning.** v7 pinned server and pgbench-client processes to
  disjoint `taskset` CPU ranges supplied via `SERVER_CPUS`/`PGBENCH_CPUS`
  config variables. This kit does not, since it would require a
  non-expert executor to correctly partition an unknown machine's core
  topology up front; documented as a deliberate simplification in the
  README.
- **No W3 LWLock "qualification" snapshot, no W0/W1/W3 statistical gates.**
  v7's harness computed live pass/fail decisions (equivalence margins,
  LWLock histogram coverage, calls-per-second thresholds) to decide
  whether to continue the run. The task only asks this kit to *record*
  measurements ("One row per run appended to a CSV... anything else the
  v7 harness recorded") with analysis happening afterward, so none of
  that gating logic was carried over -- it would have meant reinventing
  policy decisions (what margin fails the patch?) that are explicitly out
  of scope for a measurement kit.
- **CSV schema.** v7 emitted JSON Lines (`raw-results.jsonl`) with
  workload-specific fields (including a full W3 LWLock histogram per row
  for W3Q). The task asks for one CSV; I designed an 18-column schema
  covering both microbenchmark and pgbench metrics in one row shape
  (blank fields where not applicable), documented at the top of
  `02-run-matrix.sh`.
- Host-check script (`00-check-host.sh`) has no v7 analogue as a
  standalone step -- v7 recorded a `host.json` inline at the start of
  `run_full_matrix.sh` with a similar but smaller set of fields (no swap,
  no disk space, no top-processes, no warning thresholds). I reused its
  `/sys` read patterns for governor/SMT/turbo but wrote the WARN
  thresholds and the rest of the report fresh, since the task assigns
  this to its own script with specific WARN conditions v7 never checked.

## 6. Every placeholder to substitute

Only in `01-build-all.sh`, three lines near the top:

```sh
REPO_URL="@@REPO_URL@@"
COMMIT_BASELINE="@@COMMIT_BASELINE@@"
COMMIT_PATCHED="@@COMMIT_PATCHED@@"
```

`COMMIT_BASELINE` must be a commit reachable on `bench-v8-baseline`,
`COMMIT_PATCHED` one reachable on `bench-v8-patched`, both in the
repository at `REPO_URL`. The script hard-fails with a clear message if
any `@@...@@` marker is still present. No other file in the kit has a
placeholder.

## 7. `bash -n` and shellcheck results

All four scripts, run from `bench-kit-v8/`:

```
$ bash -n 00-check-host.sh && shellcheck 00-check-host.sh; echo "exit=$?"
exit=0        (no output from either -- clean)

$ bash -n 01-build-all.sh && shellcheck 01-build-all.sh; echo "exit=$?"
exit=0        (no output from either -- clean)

$ bash -n 02-run-matrix.sh && shellcheck 02-run-matrix.sh; echo "exit=$?"
exit=0        (no output from either -- clean)

$ bash -n 03-collect.sh && shellcheck 03-collect.sh; echo "exit=$?"
exit=0        (no output from either -- clean)
```

shellcheck version on this host: 0.9.0, run at its default settings (no
suppressions used). No findings of any severity on any of the four
scripts.

One correctness bug was caught and fixed during this process, not by
shellcheck (which does not do this kind of cross-reference check) but by
manual review: `02-run-matrix.sh`'s `die()` function calls `stop_server`,
which in an earlier draft was defined *after* `die()`'s first call sites
(the results-directory-exists check, tool checks, build-prefix checks).
Since bash resolves function names at call time but a function must
already have been *defined* (executed as a statement) earlier in the
script, an early `die()` call would have failed with
"stop_server: command not found" instead of showing the intended error.
Fixed by moving the server-lifecycle block (state variables,
`run_from_prefix`, `stop_server`, `on_error`, the trap registrations)
directly after `die()`/`log()`, before any check that can call `die()`.

## 8. Things I was unsure about

- **Fixture path assumption.** I assumed `src/test/modules/test_wait_primitive`
  is the fixture's path on both `bench-v8-baseline` and `bench-v8-patched`,
  based on the "exact source" tarball's own internal path and v7's usage.
  I could not verify this against either branch directly since neither
  exists yet (they are described as being created when the series is
  assembled, per the runbook's DRAFT status). If the fixture ends up
  somewhere else, `01-build-all.sh`'s `FIXTURE_REL` variable is the one
  line to edit.
- **Timing vs. the "~2.5h" target.** With a fresh `initdb` (and, for
  W4/W6c, a fresh `pgbench -i -s 100`) on every one of 240 cells, my
  rough estimate is closer to 2.5-3.5 hours than a flat 2.5h, dominated by
  120 pgbench initializations. I used `--unlogged-tables` during setup to
  claw some of that back (safe here specifically because W4/W6c are
  read-only during the measured window), and said "2.5-3.5 hours" in the
  README rather than promising the runbook's flat figure. If real
  hardware turns out much slower or faster, `PGBENCH_SCALE`, `DURATION`,
  and `WARMUP_SECONDS` are all overridable via environment variables
  without editing the script.
- **CPU pinning.** Originally dropped entirely (see §5). **Superseded by
  §9 below**: reinstated as opt-in, off by default.
- **`python3` as an implicit prerequisite.** The runbook's host-requirements
  list does not mention it, but `meson` requires it, so I treated it as
  always present and used it for JSON/CSV/regex work rather than
  reimplementing that in pure bash/awk. Flagged explicitly in the README's
  prerequisites section in case that assumption is wrong on some unusual
  host.
- **Unix-socket path length.** `02-run-matrix.sh` checks the constructed
  socket path length and fails with a clear message if the kit is
  installed somewhere with too long a path, rather than letting the
  executor hit a cryptic `connect() failed` error later. I did not test
  this path on an actual over-length directory (would have required
  starting a server), so this is reasoned from the known ~100-108 byte
  `sun_path` limit, not verified empirically.

## 9. Post-review corrections (2026-09-14)

The coordinator reviewed the kit independently and confirmed the GUC
names/spellings and the fixture assumption in §§2-3 above are correct as
written -- no change needed there. Three corrections were requested and
applied, touching `01-build-all.sh` and `02-run-matrix.sh` only. No server
was started on this host to make or verify these changes; the extra
object name below was found by reading
`contrib/pg_wait_event_tracing/pg_wait_event_tracing--1.0.sql` on branch
`wet-v8` via `git show`, the same read-only method used throughout.

### 9.1 Per-cell proof that the feature is actually active

Previously every cell trusted that `shared_preload_libraries` and
`pg_wait_event_tracing.capture` took effect. `02-run-matrix.sh` now calls
a new `assert_capture_state()` right after `start_server()`, before any
workload runs:

- `master`/`hook-null`: asserts `SHOW shared_preload_libraries` does not
  mention `pg_wait_event_tracing`, **and** that `SHOW pg_wait_event_tracing.capture`
  itself fails (`ERROR: unrecognized configuration parameter` is the
  expected, passing outcome -- the check treats an unexpectedly
  *successful* `SHOW` as the failure).
- `module-off`/`stats`/`trace`: asserts `shared_preload_libraries` does
  name the module, and `SHOW pg_wait_event_tracing.capture` returns
  exactly `off`/`stats`/`trace`.
- Any mismatch calls `die()` (cell identity included via `$CURRENT_CELL`),
  not a warning.

**The exact object and query chosen for "did it record anything," and
why it has to run concurrently with the workload rather than after it:**
reading `contrib/pg_wait_event_tracing/pg_wait_event_tracing--1.0.sql`
(wet-v8) turned up `pg_stat_wait_event_timing` (a view over
`pg_stat_get_wait_event_timing(NULL)`, both defined in that file, with a
`calls bigint` column, `GRANT SELECT ... TO pg_read_all_stats`) as the
live per-backend call-count surface. But
`pg_wait_event_tracing.c:1614-1645` (`pwet_release_stats()`, called from
`pwet_before_shmem_exit()`) shows the module `dsa_free()`s a backend's
stats payload **when that backend disconnects**. pgbench's client
connections (the ones that actually did the waiting for W3/W4/W6c) are
long gone by the time a post-workload check would run, so checking
strictly "after" would always read zero regardless of whether capture
worked -- not a hypothetical, a guaranteed false negative. So, matching
exactly what the v7 harness's W3 qualification snapshot already did for
the same reason:

- **W3/W4/W6c** (`run_pgbench_measured()`, stats/trace configs only):
  pgbench now always runs backgrounded; when `check_recording=yes`, the
  script sleeps `WARMUP_SECONDS / 2` (~5s of the default 10s warmup),
  confirms pgbench is still alive (`kill -0` -- a safety net at this point
  in the run, not something the check depends on), and runs:
  ```sql
  SELECT coalesce(sum(calls), 0) FROM pg_stat_wait_event_timing;
  ```
  cluster-wide, while pgbench's clients are fully connected and already
  generating waits. Zero (or a non-numeric result) -> `die()`. **Corrected
  2026-09-14** (originally sampled ~1s before the run ended, which lands
  inside the measured window, not outside it -- see the note below).
- **W1** (single connection, no pgbench): folded into the same
  statement as the measured call, so the check runs in the same backend
  before it can disconnect and free its own stats:
  ```sql
  WITH m AS (SELECT test_wait_primitive_latch_set(100000000) AS ns)
  SELECT m.ns,
         (SELECT coalesce(sum(calls), 0)
          FROM pg_stat_get_wait_event_timing(pg_backend_pid()))
  FROM m;
  ```
  (run via `psql -qAt -F ','`, so the two columns come back as one
  comma-joined line; `ns_per_iteration` and the recorded-count are split
  with `${combined%%,*}` / `${combined##*,}`.) Zero -> `die()`. Unchanged
  by the 2026-09-14 correction -- the fixture function is `VOLATILE`, so
  the CTE cannot be inlined and is materialized before the scalar
  subquery runs, and the second `SELECT` cannot retroactively change the
  first's already-computed return value either way.

**Where each check actually runs, precisely:** the W1 check is genuinely
outside the measurement in the sense that matters -- it executes after
`test_wait_primitive_latch_set()`'s return value for that call is already
fixed, in the same statement, so it cannot perturb `ns_per_iteration`.
The W3/W4/W6c check is **not** outside pgbench's run (pgbench is still
executing transactions throughout, including during warmup); what makes
it safe is that it lands inside the `WARMUP_SECONDS` window, which the
post-warmup progress filter (`elapsed > warmup_s`) already excludes from
`tps`/`latency_ms` entirely -- not that it runs before or after pgbench.
Before this correction, the snapshot fired ~1 second before the run
ended, which is squarely inside the thirty measured one-second progress
samples in a `stats`/`trace` cell only: an asymmetric perturbation of
exactly the comparison this kit exists to produce (biasing against, not
for, stats/trace overhead -- it could not have produced a false "no
overhead" result -- but with no business being there regardless). Moved
to mid-warmup so it cannot touch a measured sample at all.

### 9.2 `01-build-all.sh` fails on the wrong branch

`build_one()` already computed `module_sha` (`none` when the shared
library is absent). It now asserts on it, per build name, right after
that computation:

- `patched`: `die()`s if `module_sha == none` (the series is supposed to
  be there).
- `baseline`: `die()`s if `module_sha != none` (the baseline is supposed
  to be unmodified).

Each message names the likely cause (`COMMIT_PATCHED`/`COMMIT_BASELINE`
not actually on the branch it's supposed to be on).

### 9.3 CPU pinning restored as opt-in

`SERVER_CPUS` and `PGBENCH_CPUS` are now read from the environment
(default: empty = unpinned). If set, each is validated once at startup
via `taskset -c <value> true` (fails loudly, not silently, if the range
is unusable) and never derived automatically. `start_server()` prefixes
`pg_ctl` with `taskset -c "$SERVER_CPUS"` when set;
`run_pgbench_measured()` does the same for `pgbench` with
`PGBENCH_CPUS`. Both values are recorded once in `results/protocol.json`
(`server_cpus`, `pgbench_cpus`, plus a `cpu_pinning` summary field) and
repeated on every row of `results/results.csv` as new `server_cpus` /
`pgbench_cpus` columns (20 columns total now, up from 18). README updated
with a "CPU pinning (optional)" section and the "no CPU pinning" bullet
under "what differs from v7" corrected to describe it as opt-in rather
than absent.

### 9.4 Validation of the changed scripts

Only `01-build-all.sh` and `02-run-matrix.sh` were touched.
`00-check-host.sh` and `03-collect.sh` are unchanged from §7. Re-run after
all three corrections:

```
$ bash -n 01-build-all.sh && shellcheck 01-build-all.sh; echo rc=$?
rc=0   (no output from either)

$ bash -n 02-run-matrix.sh && shellcheck 02-run-matrix.sh; echo rc=$?
rc=0   (no output from either)
```

One shellcheck finding came up mid-edit and was fixed, not suppressed:
SC2015 ("A && B || C is not if-then-else") on both new
`[[ regex ]] && (( arith > 0 )) || die ...` one-liners (the W1 combined
check and the W3/W4/W6c cluster-wide check). Rewritten as explicit
`if [[ ! regex ]] || (( arith == 0 )); then die ...; fi` in both places,
which also happens to be easier to read. Final shellcheck run on all four
scripts: exit 0, zero findings, no suppressions used.

## 10. Second round of post-review corrections (2026-09-14)

The coordinator confirmed the concurrent-snapshot adaptation and the W1
CTE form (materialized ahead of the scalar subquery because the fixture
function is `VOLATILE`) were both correct and should stay. Two further
changes were requested and applied, touching `02-run-matrix.sh` and
`01-build-all.sh` only. No server was started on this host for either.

### 10.1 Moved the W3/W4/W6c snapshot out of the measured window

The coordinator's read of my own §9.1 was right: the snapshot fired ~1
second before pgbench's run ended, which is inside the 30 measured
one-second progress samples for that cell, not outside them --
contradicting what §9.1 claimed. It biases against, never for,
stats/trace overhead (so it could not manufacture a false "tracing is
free" result), but it had no business perturbing published numbers at
all, and only in `stats`/`trace` cells.

Fixed in `run_pgbench_measured()`: the snapshot delay is now
`WARMUP_SECONDS / 2` (~5s of the default 10s warmup) instead of
`total - 1`. By then pgbench's clients are fully connected and already
generating waits -- `sum(calls) > 0` proves capture just as well as it
did at the old timing -- and the sample now falls entirely inside the
window the post-warmup progress filter (`elapsed > warmup_s`) already
discards, so it cannot affect `tps`/`latency_ms` for any cell. The
`kill -0` liveness check is kept (pgbench exiting 5 seconds into a
40-second run would still be worth catching) but, as instructed, it is no
longer load-bearing for the check's soundness -- the timing choice itself
is what makes the sample harmless now, not the liveness check. The W1
check is untouched, per instruction: it already runs after
`test_wait_primitive_latch_set()`'s return value is fixed, in the same
statement, so nothing about it needed to move.

§9.1 above has been corrected in place (not left standing) to state
plainly that the W3/W4/W6c check runs *inside* pgbench's execution, safe
because it lands in the discarded warmup window -- not because it runs
"outside" pgbench's run, which was never true and is no longer claimed.
`README.md`'s "Proving the collector is really on" section and its
"CHANGE 1"-equivalent wording were updated the same way (mid-warmup
timing, not "about one second before pgbench ends").

### 10.2 Placeholders substituted

`01-build-all.sh`'s three `@@...@@` values are now:

```sh
REPO_URL="https://github.com/DmitryNFomin/postgres.git"
COMMIT_BASELINE="765efece39ba3fb04fdf20b1dadcd9ecea76fbc9"
COMMIT_PATCHED="d7b4584a901241258604eef1f03dfd6b3f1fa926"
```

`BRANCH_BASELINE`/`BRANCH_PATCHED` were left as-is (`bench-v8-baseline`/
`bench-v8-patched`), as instructed -- they already matched. The
`@@...@@`-detection guard (`for placeholder in ...; do [[ "$placeholder"
!= *"@@"* ]] || die ...; done`) was left in place, unmodified; it simply
has nothing left to trip on. `README.md`'s "Before step 2: fill in three
placeholders" section, which instructed editing blank placeholders, is
now stale given the kit ships with real values, so it was replaced with
"The commit this kit measures," showing the same three values as shipped
and explaining what each commit is, with the guard's continued presence
noted for if this kit is ever repointed at a different commit pair later.

**Placeholder-marker scan of the whole kit** (`grep -rn "@@" .` from
`bench-kit-v8/`): the only remaining hits are the guard's own source code
in `01-build-all.sh` (the `*"@@"*` pattern and its error message, which
must stay) and one explanatory sentence in `README.md` describing that
guard. No `@@REPO_URL@@`-style unfilled value remains anywhere in the
kit.

### 10.3 Validation

Only `01-build-all.sh` and `02-run-matrix.sh` changed in this round.

```
$ bash -n 01-build-all.sh && bash -n 02-run-matrix.sh && echo "combined bash -n: OK"
combined bash -n: OK

$ shellcheck 01-build-all.sh 02-run-matrix.sh; echo "combined shellcheck exit=$?"
combined shellcheck exit=0
```

Zero findings, no suppressions. A full four-script sweep
(`00-check-host.sh 01-build-all.sh 02-run-matrix.sh 03-collect.sh`) was
re-run after both changes and is likewise clean: `bash -n` exit 0 and
`shellcheck` exit 0 on every script.
