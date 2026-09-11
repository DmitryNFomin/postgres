# WP4a report: TAP tests for contrib/pg_wait_event_tracing (v8-0004)

Branch `wet-v8-wp4a`, worktree
`<workspace>/wt-v8-wp4a`. Build dir
`<workspace>/build-v8-wp4a`
(`--buildtype=debugoptimized -Dcassert=true -Dwerror=true
-Dinjection_points=true`, `.venv-v7-rfc` meson/ninja, `ninja -j2` only).
The main checkout at `<workspace>/postgres` stayed
on `wet-series` throughout; nothing was pushed at any point.

Originally branched off `wet-v8-wp2` at `f4bf69380b8`, with one commit
(`c412a42b20d`). The coordinator then rebased `wet-v8-wp4a` onto the
current `wet-v8-wp2` tip (`02fe98d0f97`, two unrelated regress-test fixes
found by an earlier CI run) and added one commit of their own on top
(`2c140de41c8`, making t/001's sessions 2 and 3 actually attach). Two
further CI runs against that lineage each found more problems, each
fixed in its own commit on top, described below.

Four commits on `wet-v8-wp4a` from this WP (plus the coordinator's
`2c140de41c8` in between the first two, not authored by this session):

| Commit | Subject |
|---|---|
| `74310b3a69bb1466fd72a1a7679e9c2b36ff5a69` | pg_wait_event_tracing: TAP tests for memory bound, ownership and reset |
| `2c140de41c8e58817c5f7c7738a8447f789ac123` (coordinator) | pg_wait_event_tracing: make the memory TAP test's later sessions attach |
| `53888ba66f23e1345e7b9afe0c4c59cfdad55431` | pg_wait_event_tracing: fix two CI failure causes in the TAP tests |
| `cbb10ad23dc80a319fcf0fed682087cd5fa6ecfe` | pg_wait_event_tracing: two more CI failure causes in the TAP tests |

Author `Dmitry Fomin <fomin.list@gmail.com>` throughout, trailer
`Discussion: https://postgr.es/m/CAPHG-0mAOn05ae6Kqx1wHXxzOk4E5W7ajjd=QBhgkR7a0uyQmw@mail.gmail.com`.

## Files

- `contrib/pg_wait_event_tracing/t/001_memory.pl`
- `contrib/pg_wait_event_tracing/t/002_ownership.pl`
- `contrib/pg_wait_event_tracing/t/003_reset_acl.pl`
- `contrib/pg_wait_event_tracing/t/004_reset_race.pl`
- `contrib/pg_wait_event_tracing/meson.build` (new `'tap'` entry alongside
  the existing `'regress'` entry, copying `src/test/modules/test_slru`'s
  combined pattern)
- `contrib/pg_wait_event_tracing/Makefile` (`TAP_TESTS = 1`,
  `EXTRA_INSTALL = src/test/modules/injection_points`,
  `export enable_injection_points`, copying `src/test/modules/test_shmem`
  and `test_slru`)

## CI run 34614510950 and its two fixes (commit `53888ba66f2`)

Result: t/001 OK (5 subtests, Linux and macOS — this already included the
coordinator's `2c140de41c8` fix). t/004 reported SKIP with 0 subtests on
every platform, meaning every assertion sat in the `SKIP` block and reuse
never happened. t/002 and t/003 DIED on every platform.

**Cause 1 (t/002, t/003 deaths): `pg_wait_event_tracing.capture` is
`PGC_SUSET`.** `SET pg_wait_event_tracing.capture = stats` issued by
`regress_a`/`regress_b` (non-superuser roles) raised a permission error,
and `query_safe()` dies on any stderr output. Fix, in both files: right
after the `CREATE ROLE` statements,
`GRANT SET ON PARAMETER pg_wait_event_tracing.capture TO regress_a, regress_b;`,
with a comment explaining why. (t/001 and t/004 use only the default
superuser connection to `SET` this GUC, so neither needed the grant.)

**Cause 2 (t/002, t/004 reuse): the PGPROC free list on master is FIFO,
not LIFO.** `src/backend/storage/lmgr/proc.c`: `InitProcess()` calls
`dlist_pop_head_node(procgloballist)`; `ProcKill()` returns via
`dlist_push_tail(procgloballist, ...)`. A freed ProcNumber is only handed
out again once every other free slot has been used first, so "connect
right after the exit" — what both files originally did — never actually
got A's slot. Fix, in both files: `max_connections = 10` to keep the free
list short, plus a bounded loop (up to `3 * max_connections` attempts)
that opens candidate connections one at a time, each reading its own
ProcNumber via
`SELECT id FROM pg_stat_get_backend_idset() AS id WHERE pg_stat_get_backend_pid(id) = pg_backend_pid();`
(no `proacl` entry on either function in `pg_proc.dat`, i.e. default
`PUBLIC` `EXECUTE`, confirmed to work from the non-superuser candidates in
t/002), quitting and retrying until one matches A's ProcNumber or the
budget runs out. t/004's loop runs while R is still parked at the
injection point; `injection_points_wakeup` + `$R->quit` were moved to run
unconditionally right after, so R can never be left blocked if the loop
exhausts its budget. Both files keep their `SKIP` block as a safety net,
naming how many attempts were made.

## CI run 34627876249 on `53888ba66f2` and its two fixes (commit
`cbb10ad23dc`, on top)

Result: Linux/macOS now pass t/001 (7), t/003 (25) and t/004 (2) — the
FIFO-aware reuse loop works and the race check really runs. Two causes
remained.

**Cause A (t/002 dies on Linux/macOS, after checks 1-5 pass):** the final
check ran `$B->query("SELECT * FROM pg_stat_wait_event_timing")` on B's
own `BackgroundPsql` session to provoke a permission-denied error.
`BackgroundPsql` starts `psql` with `on_error_stop => 1`, so that expected
error makes `psql` itself exit, and `BackgroundPsql.pm` (line 278) dies
with `"process ended prematurely"` instead of leaving the error on
`$B->{stderr}` for inspection. Fixed by doing this one check with a
one-shot `$node->psql(... connstr => ... user=regress_b)` connection
instead (the same pattern `t/003`'s `reset_as()` helper already used),
asserting on its exit code and captured stderr rather than `$B`'s.

**Cause B (Windows, all three of t/002, t/003, t/004): one `pg_sleep(0.01)`
sometimes records 2+ PgSleep waits.** CI logs showed "got 2 for expected 1"
and "4 or 5 for expected 3". `pg_sleep()` loops, calling `WaitLatch` again
until its own `GetCurrentTimestamp()` says the requested time is up; on
Windows the latch timeout and that clock can disagree, so it loops and
genuinely records more than one wait for a single call. The module is
right to count every one of them, so no assertion may equate "one
`pg_sleep` call" with "one recorded wait". Fixed, per file (each file also
gained a short comment on this):

- **t/003**: fixtures and the two successful cross-backend resets (a2
  resets A, sig resets B) now assert `calls >= 1` rather than `= 1`; the
  decisive signal for a successful reset stays `reset_count` going up by
  exactly 1 after the target's next statement, which the wait-count
  variability doesn't touch. The two refused resets (b resets SU, b
  resets A) now read `calls` and `reset_count` immediately before the
  attempt and assert both are unchanged after it, rather than comparing
  against a literal `1`. The synchronous self-resets are unchanged
  (`calls == 0` right after the reset is a hard guarantee regardless of
  how many waits preceded it, and the `reset_count` +1 check already
  didn't depend on wait counts).
- **t/002**: B's own activity no longer uses `pg_sleep()` at all. After
  enabling capture, B now runs two plain `SELECT 1;` statements: the
  first attaches (`post_parse_analyze_hook` picks it up, matching
  `2c140de41c8`'s finding that the bare `SET`'s own assign hook does not
  reliably attach), and sending the second is what completes the
  `ClientRead` wait *between* the two statements — now that a payload
  exists to record into — turning it into an actual row. B is then
  checked, both as the superuser reader and via B's own function call,
  for "at least one row" and "no `PgSleep` row" (a `PgSleep` row could
  only be A's leftover data, which is exactly what fix 2 guards
  against). The `procnumber` cross-check now reads from any of B's rows
  (`LIMIT 1`) instead of specifically a `PgSleep` row, since there no
  longer is one.
- **t/004**: kept `reset_count = 0` as the decisive check (unaffected by
  wait-count variability) and changed the final PgSleep count check from
  `= 3` to `>= 3`.

Validated the same way both times: `ninja -j2` (clean under `-Dwerror=true`;
no C files changed in either round, so this mostly reconfirmed the
existing build), `perl -c` on all four files (all pass both rounds),
`meson test --list` (all four still listed). No server was started, and
only the existing `build-v8-wp4a` build directory was touched (the host's
disk was reported tight for the second round; no new build dir was
created).

## What each test asserts (current design)

### t/001_memory.pl (fix 1, sparse allocation)

`max_connections = 200`. Measures
`sum(size) FROM pg_dsm_registry_allocations WHERE name LIKE 'pg_wait_event_tracing%'`
(this matches exactly two registry rows: the control DSM segment
`pg_wait_event_tracing` and the DSA area `pg_wait_event_tracing_stats`).

- Baseline before any capture is exactly 0.
- One session enables `capture = stats` and runs `pg_sleep(0.01)`; growth
  over baseline must be `< 4 MiB`.
- A second session enables `capture = stats`, then runs `pg_sleep(0.01)`
  and asserts it has a row in the view before its footprint is measured
  (the coordinator's `2c140de41c8` fix — the bare `SET` does not reliably
  attach). Growth over the first measurement must be `< 512 KiB`.
- The first session sets `capture = off`; its pid must show 0 rows in
  `pg_stat_wait_event_timing`.
- A third session enables `capture = stats`, same attach-then-assert
  pattern; footprint must not exceed the second measurement (`<=`).

### t/002_ownership.pl (fix 2, ownership across ProcNumber reuse)

Roles `regress_a`, `regress_b`, granted `SET` on the capture GUC. Session
A (regress_a): `capture = stats`, `pg_sleep(0.01)`, records pid and
`procnumber` from the view. Quits A, polls until A's pid is gone from
`pg_stat_activity`, then runs the bounded candidate loop as `regress_b`
to find a session that actually reused A's ProcNumber.

`SKIP` block (9 assertions), gated on the loop finding a match (diagnostic
skip naming the attempt count, not a silent pass, if it didn't):

- Before enabling anything: 0 rows in both `pg_stat_wait_event_timing`
  and `pg_stat_wait_event_timing_overflow` for B's pid.
- B enables `capture = stats`, runs two `SELECT 1;` statements (not
  `pg_sleep` — see Cause B above); the view's own `procnumber` column
  (from any of B's rows) agrees with what the loop found.
- Superuser reader sees at least one row for B, and no `PgSleep` row (a
  `PgSleep` row could only be A's leftover data).
- B itself, reading via `pg_stat_get_wait_event_timing(pg_backend_pid())`
  directly (not the view, which is revoked from `PUBLIC`), sees the same:
  at least one row, no `PgSleep` row — exercises the self-privilege
  branch of `PWET_HAS_STATS_PRIVS` rather than a `pg_read_all_stats`
  grant.
- A one-shot `$node->psql(... user=regress_b)` connection querying the
  view directly gets a nonzero exit code and `permission denied` on
  stderr (Cause A above — this used to run on B's own session and crash
  the test instead of asserting anything).

### t/003_reset_acl.pl (fix 4, reset authorization)

Roles `regress_su` (superuser), `regress_a`, `regress_a2` (member of
`regress_a`), `regress_b`, `regress_sig` (member of `pg_signal_backend`),
all five registered via `auth_extra`; `regress_a` and `regress_b` also
granted `SET` on the capture GUC. Three live `background_psql` targets
(SU, A, B), each `capture = stats` plus one `pg_sleep(0.01)`,
sanity-checked at `calls >= 1` before the actual cases. Cross-backend
reset calls go through a small `reset_as($role, $pid)` helper that opens
a one-shot `$node->psql(... connstr => ... user=$role)` connection (no
injection point is involved in this file, so the request itself is a
fast, synchronous lock-protected bump — no need to keep the actor
connected).

- **regress_b resets itself (NULL, then own pid): succeeds, synchronously**
  — asserts `calls` drops to exactly 0 and `reset_count` increments by
  exactly 1, immediately, no extra wait needed (self-reset bypasses the
  async generation-bump path entirely).
- **regress_a2 resets regress_a: succeeds** — asserts exit code 0, empty
  stderr, then drives A through one more `pg_sleep(0.01)` and checks
  `calls >= 1` and `reset_count` incremented by exactly 1 (the decisive
  signal — see Cause B above for why `calls` is `>= 1`, not `= 1`). This
  is the asynchronous path: the request only bumps a generation counter,
  consumed at the target's *next* `wait_end()` — which, in practice,
  fires first for the idle-time `ClientRead` wait as the test script
  sends A's next query, clearing `PgSleep`'s dense entry before the
  `pg_sleep()` call re-populates it.
- **regress_sig resets regress_b: succeeds** — same asynchronous-reset
  pattern as above.
- **regress_b resets regress_su: permission denied** — exit code nonzero,
  stderr matches `permission denied`; both `calls` and `reset_count` for
  SU, read immediately before the attempt, are confirmed unchanged after
  it (the ACL check runs strictly before `pwet_request_reset()`, so a
  rejected call touches no state).
- **regress_b resets regress_a: permission denied** — same pattern, both
  of A's counters confirmed unchanged against their just-before values.
- **any role (used: regress_b) resets the checkpointer's pid: WARNING, not
  an error** — exit code 0, stderr matches
  `is not a PostgreSQL backend process` (`BackendPidGetProc()` returns
  NULL for an auxiliary pid, short-circuiting before any ACL check).
- **regress_b calls `pg_stat_reset_wait_event_timing_all()`: ERROR** — exit
  code nonzero, stderr matches `permission denied`, regardless of the
  default `REVOKE EXECUTE FROM PUBLIC` (this is the C-level
  `superuser()` hard-require, not just the grant state).

### t/004_reset_race.pl (fix 5, reset race)

`plan skip_all` unless `$ENV{enable_injection_points} eq 'yes'`.
`max_connections = 10`. `shared_preload_libraries = 'pg_wait_event_tracing, injection_points'`.
A: one recorded wait, pid and ProcNumber noted. Superuser session R
attaches `pg-wait-event-tracing-reset-before-publish` in `'wait'` mode,
then sends `pg_stat_reset_wait_event_timing(<A pid>)` via `query_until`
(non-blocking). Polls `pg_stat_activity` until R shows that wait_event,
then quits A and waits for its pid to disappear.

While R is still parked, runs the bounded candidate loop to find a
session that reused A's ProcNumber. If one is found, it enables
`capture = stats` and runs two `pg_sleep(0.01)` calls *before* R is
released — attaching with a fresh owner token while R is still parked, so
R's stale request finds the ProcNumber already reassigned rather than
merely unowned. Either way, `injection_points_wakeup` + `$R->quit` run
unconditionally right after.

`SKIP` block (2 assertions), gated on the loop finding a match: B runs
one more `pg_sleep(0.01)` after the release, then:

- B's `calls` for PgSleep is `>= 3` (all three of its own waits still
  there; `>=` rather than `=`, per Cause B above).
- B's `reset_count` is exactly `0` — the decisive check: the reset aimed
  at A's stale owner token was not consumed by B, proving
  `pwet_request_reset()`'s owner-token re-check under the lock did its
  job.

The injection point is detached unconditionally at the end.

## Things worth the reviewer checking in CI logs

1. **This round's fixes are themselves unexecuted**, per the hard rule.
   Cause A's reasoning (`BackgroundPsql`'s `on_error_stop => 1` making an
   expected permission error fatal to the session) came from reading
   `BackgroundPsql.pm` and matching the coordinator's report of exactly
   which line dies; Cause B's reasoning came from the coordinator's CI
   log excerpts plus `pg_sleep()`'s known WaitLatch-retry-loop behavior.
   Neither was reproduced locally. The next CI run is the first real
   confirmation.
2. **t/002's new double-`SELECT 1;` timing for B, unverified.** The
   reasoning: after `SET capture = stats`, the *first* `SELECT 1;`'s
   arrival ends a `ClientRead` wait that began before the attach (so
   `pwet_my_stats` was still NULL then, and that particular wait is not
   recorded), but the attach itself completes during this first
   statement's `post_parse_analyze_hook`. The *second* `SELECT 1;`'s
   arrival then ends the `ClientRead` wait that began right after the
   first statement's response was sent — this time with `pwet_my_stats`
   already set — so that gap is what actually produces B's first
   completed row, visible to a separate superuser connection from that
   point on. If this reasoning is wrong (e.g. if attach or `ClientRead`
   accounting behaves differently than assumed here), the "superuser
   sees at least one row for B" assertion is the one most likely to fail;
   the module's own regress test's comment about "`ClientRead` may be
   recorded again before the next statement runs" is the closest existing
   corroboration I found, but I did not execute anything to confirm the
   exact statement-count needed.
3. **The candidate loop's attempt budget (`3 * max_connections` = 30)**
   in t/002 and t/004 is unmeasured against a real server's actual
   free-list size (which also covers autovacuum/background-worker slots,
   not just regular connections); the `SKIP` diagnostic names the attempt
   count if it ever runs out, which should make a too-tight budget
   visible rather than a silent always-skip.
4. **`debug_parallel_query` gate**, confirmed necessary by the first CI
   run (t/001 passed with it in place). All four files carry
   `$node->append_conf('postgresql.conf', "debug_parallel_query = off");`
   before `$node->start`.
5. **t/003's asynchronous-reset timing reasoning** (the target's very
   first post-request `wait_end()` being the idle-time `ClientRead` wait
   as the test script sends its next query) is now indirectly
   corroborated by CI: t/003 passed all 25 subtests in the second CI run,
   which included this exact reasoning for both successful cross-backend
   resets — so this part is no longer purely theoretical, at least on
   Linux/macOS. Windows results for t/003 after this round's fixes are
   still unconfirmed.
