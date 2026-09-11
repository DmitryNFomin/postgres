# WP1: test_wait_hook module — deliverable report

## Where
- Worktree: `<workspace>/wt-v8-wp1`
- Branch: `wet-v8-wp1`, based on `wet-v7-rfc`
- Build dir: `<workspace>/build-v8-wp1`
  (`--buildtype=debugoptimized -Dcassert=true -Dwerror=true -Dinjection_points=true`,
  meson/ninja from `.venv-v7-rfc/bin`)
- Main checkout at `<workspace>/postgres` was left
  untouched on `wet-series` throughout; nothing was pushed.

## Commit
`1156a3806cf665b97c56ec4f2e741b1bce6a87af` on `wet-v8-wp1` (amended once,
after the review fix below; the original pre-fix hash was
`36bee5bc5608c8f6f5a3f571a822cd346a51a664`), author
`Dmitry Fomin <fomin.list@gmail.com>`:

    test_wait_hook: test module for the wait event hook contract

Trailer: `Discussion: https://postgr.es/m/CAPHG-0mAOn05ae6Kqx1wHXxzOk4E5W7ajjd=QBhgkR7a0uyQmw@mail.gmail.com`

## Diffstat vs. `wet-v7-rfc`

```
 src/test/modules/Makefile                          |   1 +
 src/test/modules/meson.build                       |   1 +
 src/test/modules/test_wait_hook/.gitignore         |   4 +
 src/test/modules/test_wait_hook/Makefile           |  23 ++
 src/test/modules/test_wait_hook/README             |  50 +++
 .../test_wait_hook/expected/test_wait_hook.out     | 172 ++++++++++++
 src/test/modules/test_wait_hook/meson.build        |  33 ++
 .../modules/test_wait_hook/sql/test_wait_hook.sql  |  75 +++++
 .../modules/test_wait_hook/test_wait_hook--1.0.sql |  89 ++++++
 src/test/modules/test_wait_hook/test_wait_hook.c   | 339 +++++++++++++++++++
 .../modules/test_wait_hook/test_wait_hook.control  |   4 +
 11 files changed, 791 insertions(+)
```

Touches only `src/test/modules/**`, as required.

## Review fix

A review of the original commit (`36bee5bc560`) found two correctness
problems in the regression test, fixed in place (single amended commit,
no new commit added):

1. **Noise from the session's own waits.** The hooks are installed for
   the whole backend, not just for the statement under test. Between
   statements this session blocks in `secure_read()` on a `ClientRead`
   wait, and ordinary catalog page reads (`FileReadV`) can block on
   `IO:DataFileRead` — both are timed waits on this branch (converted by
   `wet-v7-rfc`), so they were landing in the ring alongside the `PgSleep`
   events the test cares about, making the expected output
   nondeterministic. **Fix:** `ring_push()` now records only events with
   `wait_event_info == WAIT_EVENT_PG_SLEEP`; this also subsumes the
   previous `== 0` filter (`WAIT_EVENT_PG_SLEEP` is never 0), with the
   reasoning documented in the comment above `ring_push()` in
   `test_wait_hook.c`.
2. **`pg_sleep()` is not reliably a single wait.** `pg_sleep()`'s loop
   calls `WaitLatch()` with `WL_LATCH_SET`; if the process latch happens
   to already be set for reasons unrelated to the test, `WaitLatch()`
   returns immediately and `pg_sleep()` loops around for a second wait,
   recording a spurious extra begin/end pair. **Fix:** added
   `test_wait_hook_wait()`, a new SQL-callable function that performs
   exactly one `WaitLatch(MyLatch, WL_TIMEOUT | WL_EXIT_ON_PM_DEATH, 1,
   WAIT_EVENT_PG_SLEEP)` with no `WL_LATCH_SET`, so an already-set latch
   cannot shorten or repeat it. Every `SELECT pg_sleep(0.01)` in
   `sql/test_wait_hook.sql` was replaced with
   `SELECT test_wait_hook_wait()`.

Because the filter in (1) only ever accepted `PgSleep` events to begin
with, and (2) only changes the wait's source (not its semantics or the
scenario logic), the six scenarios and their expected row *content* are
unchanged; only the SELECT text and the resulting column header
(`test_wait_hook_wait` instead of `pg_sleep`) differ, and `README` and
`test_wait_hook--1.0.sql`'s doc comment for the new function were updated
accordingly. The full rebuild (`ninja -j2` with `-Dwerror=true`,
`headerscheck`, `cpluspluscheck`) was re-run after the fix and is clean;
see "Build validation actually performed" below.

## What the module does

`src/test/modules/test_wait_hook/test_wait_hook.c` starts from the harness
extracted from `04-timed-site-hook-vs-tested-master.patch` (a microbenchmark
counting hook invocations), but that harness didn't test the contract at
all, so the module was rewritten around the actual requirements in
`src/include/utils/wait_event.h` (backend-local-only state; no waits/
allocations/locks/errors in a hook body; the `wait_event_hook_depth`
re-entrancy guard; chaining consumers call the previous begin hook before
their own begin work and their own end work before the previous end hook).

Two installable consumers, `A` and `B`, each record `{begin/end, consumer,
wait_event_info, depth}` into a 64-entry backend-local ring (preallocated
static array, no palloc/locks/waits from within a hook), filtered to
`PgSleep` events only (see "Review fix" below). SQL surface:
`test_wait_hook_install(consumer)`, `test_wait_hook_uninstall_all()`
(LIFO unwind of saved previous hook pointers), `test_wait_hook_events()`
(SRF, drains the ring), `test_wait_hook_nested_wait_in_hook(bool)`,
`test_wait_hook_error_after_start()`, and `test_wait_hook_wait()` (a
single, deterministic `PgSleep` wait used by the regression test).

## The six required test outcomes

The regression test (`sql/test_wait_hook.sql`,
`expected/test_wait_hook.out`) was authored, and the expected output was
constructed by tracing the exact code paths involved
(`test_wait_hook_wait()`'s single `WaitLatch` call, `WaitEventSetWait`'s
start/end calls, `AbortTransaction`'s unconditional cleanup call) rather
than captured from a live run — see "Not done" below. The traced
outcomes:

1. **No consumer installed**: `test_wait_hook_wait()` produces zero rows
   from `test_wait_hook_events()`.
2. **Consumer A installed**: one `test_wait_hook_wait()` yields exactly 2
   rows — `begin/A/PgSleep/1`, `end/A/PgSleep/1`.
3. **A then B installed**: one `test_wait_hook_wait()` yields exactly 4
   rows in order — `begin/A`, `begin/B`, `end/B`, `end/A` (all `PgSleep`,
   depth 1), confirming outer-begin-first/inner-end-first chaining.
4. **Nested-wait mode on** (consumer A only): one `test_wait_hook_wait()`
   yields exactly 1 row — `begin/A/PgSleep/1`. The inner `WaitLatch` inside
   A's begin hook is depth-guarded (no extra begin/end), and its call to
   `pgstat_report_wait_end_timed()` unconditionally clears the raw
   wait-event state, so the *outer* end call reads back 0 and is filtered
   out by the ring's own filter (which only keeps `WAIT_EVENT_PG_SLEEP`,
   so 0 is never recorded) — hence only the begin survives. Mode is
   turned back off afterward.
5. **Error path**: `test_wait_hook_error_after_start()` reports a wait
   start (recorded as `begin/A/PgSleep/1` via the normal hook call) and
   then raises an error with no matching end call of its own;
   `AbortTransaction()`'s unconditional `pgstat_report_wait_end_timed()`
   cleanup call performs the missing end (recorded as `end/A/PgSleep/1`),
   so `test_wait_hook_events()` shows the matched pair. A following
   `test_wait_hook_wait()` then records a normal pair again, showing the
   depth guard and `my_wait_event_info` were left consistent by the abort.
6. **Uninstall**: after `test_wait_hook_uninstall_all()`,
   `test_wait_hook_wait()` again produces zero rows.

## Build validation actually performed

- `meson setup` with the required options: **clean**.
- `ninja -j2` (full build, `-Dwerror=true`): **clean**, `test_wait_hook.so`
  linked with no warnings/errors.
- `ninja headerscheck`: **clean**.
- `ninja cpluspluscheck`: **clean**.
- `git diff --stat wet-v7-rfc`: confirmed to touch only
  `src/test/modules/**`.

## Not done, and why

Per an explicit hard-rule instruction issued mid-task, **no PostgreSQL
server process was started** on this machine (no `meson test`, `pg_regress`,
`initdb`, `pg_ctl`, or manual `psql` smoke test). Consequently:

- `meson test --suite setup --suite test_wait_hook` and
  `meson test --suite regress` were **not run**. `meson test --list`
  confirms the suite is correctly registered
  (`test_wait_hook - postgresql:test_wait_hook/regress`), but the
  regression test has not been executed against a live backend.
- The expected output above is the product of careful manual reasoning
  about `test_wait_hook_wait`/`WaitEventSetWait`/`AbortTransaction` and
  psql's exact aligned-table formatting rules (verified against several
  other modules' existing `expected/*.out` files and against
  `src/fe_utils/print.c`), not an observed run. It should be treated as a
  first draft to be confirmed (and touched up for any off-by-one
  formatting slip) by the fork's CI, which does run the regression suite.
  This also means the review-flagged nondeterminism (ClientRead/file-I/O
  noise, `pg_sleep`'s double-wait risk) was diagnosed and fixed by code
  inspection, not reproduced by an actual failing run.
