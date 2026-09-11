# WP1: test module for the wait-event hook contract (v8-0003)

Plan: <workspace>/IMPLEMENTATION-PLAN-v8.md (§3.2).
Repo: <workspace>/postgres. The user's checkout is
on `wet-series`; DO NOT change it. Work in a new worktree:

    git -C <workspace>/postgres worktree add \
        <workspace>/wt-v8-wp1 -b wet-v8-wp1 wet-v7-rfc

Meson/ninja live in <workspace>/.venv-v7-rfc/bin
(put it first on PATH). Build dir: <workspace>/build-v8-wp1
with `--buildtype=debugoptimized -Dcassert=true -Dwerror=true -Dinjection_points=true`.
Disk is tight: no other build dirs; delete nothing that is not yours.

## What the hook is
`wet-v7-rfc` adds to src/include/utils/wait_event.h: `wait_event_begin_hook`,
`wait_event_end_hook` (type `void (*)(uint32 wait_event_info)`), the depth
guard `wait_event_hook_depth`, and the inline pair
`pgstat_report_wait_start_timed()/pgstat_report_wait_end_timed()`, used at 94
sites (all of src/backend + src/common/controldata_utils.c). Read that header
first. Contract (from its comment): hooks use only preallocated backend-local
state; no waits, allocations, locks or errors inside; the depth guard prevents
re-entry; a chaining consumer saves the previous pointers, calls previous-begin
before its own begin work, and its own end work before previous-end.

## Starting point
The package has a first version of this module. Extract it from the patch:

    P=<scratch>/v7rev/wait-hook-peer-review-20260909/source/patches/04-timed-site-hook-vs-tested-master.patch
    git apply --include='src/test/modules/test_wait_hook/*' --include='src/test/modules/Makefile' --include='src/test/modules/meson.build' "$P"

(If the two registration hunks do not apply on this base, add the lines by
hand.) Read what it does, keep what is good, and turn it into a proper test
module: `src/test/modules/test_wait_hook/` with Makefile, meson.build,
control, `test_wait_hook--1.0.sql`, `test_wait_hook.c`, `sql/test_wait_hook.sql`,
`expected/test_wait_hook.out`, README (short). Follow the conventions of a
neighbour such as src/test/modules/test_custom_rmgrs or test_misc.

## Required behaviour of the module (backend-local, no shared memory)
- A ring of the last N (say 64) hook events: {kind begin/end, consumer id A/B,
  wait_event_info, depth seen}. All preallocated static arrays; hooks must not
  palloc, lock, wait or elog.
- SQL functions: `test_wait_hook_install(consumer text)` installs consumer 'A'
  or 'B' (B chains onto whatever was installed before, using the contract's
  order); `test_wait_hook_uninstall_all()` restores the previous pointers;
  `test_wait_hook_events()` SRF returning the recorded events in order and
  clearing the ring; `test_wait_hook_nested_wait_in_hook(bool)` toggles a mode
  where consumer A's begin hook itself calls WaitLatch(MyLatch, WL_TIMEOUT |
  WL_EXIT_ON_PM_DEATH, 1, WAIT_EVENT_PG_SLEEP) (this deliberately violates the
  contract to prove the depth guard); `test_wait_hook_error_after_start()`
  performs pgstat_report_wait_start_timed(WAIT_EVENT_PG_SLEEP) then elog(ERROR)
  without an end.
- Everything must be safe when hooks are NULL and after uninstall.

## Regression test (sql/expected) must show
1. Before install: `SELECT pg_sleep(0.01)` produces no events.
2. Consumer A installed: `pg_sleep(0.01)` yields exactly one begin and one end
   with wait_event_info = WAIT_EVENT_PG_SLEEP (compare via
   pg_get_wait_event? simplest: the SRF exposes the event name through
   pgstat_get_wait_event(), so the expected output shows 'PgSleep').
3. Consumers A then B: order is A.begin, B.begin, B.end, A.end.
4. Nested-wait mode on: one pg_sleep yields exactly one A.begin (the inner
   WaitLatch's timed pair is skipped by the depth guard: no extra begin/end);
   mode off afterwards.
5. Error path: calling test_wait_hook_error_after_start() inside a
   transaction that then aborts yields begin(PgSleep) and the cleanup end
   (from AbortTransaction's converted pgstat_report_wait_end_timed(); its
   wait_event_info is whatever my_wait_event_info held, so report it as-is);
   a following pg_sleep records a normal pair (state is consistent).
6. Uninstall: no events afterwards.

Keep the test deterministic (no timing-dependent counts: pg_sleep(0.01) is a
single WaitLatch call → exactly one pair; avoid queries that wait on I/O).

## Validate (HARD RULE: never start a PostgreSQL server on this host)
- meson setup + ninja (werror, -j2) + `ninja headerscheck` (+ cpluspluscheck).
- Do NOT run `meson test`, pg_regress, TAP, initdb, pg_ctl or any smoke
  server. Author expected/ output by reasoning; tests run on the fork's CI.
- `git diff --stat wet-v7-rfc` must touch only src/test/modules/**.

## Commit
One commit on `wet-v8-wp1`, author Dmitry Fomin <fomin.list@gmail.com>,
subject "test_wait_hook: test module for the wait event hook contract",
body: what it tests, trailer
`Discussion: https://postgr.es/m/CAPHG-0mAOn05ae6Kqx1wHXxzOk4E5W7ajjd=QBhgkR7a0uyQmw@mail.gmail.com`.
Do not push. Do not touch other branches.

## Deliverable
<workspace>/wp1-report.md: commit hash, diffstat,
the six test outcomes, anything you could not do and why. Final message
<= 8 lines.
