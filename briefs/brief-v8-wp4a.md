# WP4a: TAP tests for the statistics level (fixes 1, 2, 4, 5)

Plan: <workspace>/IMPLEMENTATION-PLAN-v8.md §4.1–4.6.
Code under test: contrib/pg_wait_event_tracing on branch `wet-v8-wp2`
(read wp2-report.md first for function names, columns and the injection
point name `pg-wait-event-tracing-reset-before-publish`).

## HARD RULES
- NEVER start a PostgreSQL server on this host: no `meson test`, prove,
  pg_regress, initdb, pg_ctl. You WRITE the TAP tests; they execute on the
  fork's CI. Allowed locally: meson setup, `ninja -j2`, headerscheck,
  cpluspluscheck, and `perl -c` syntax checks of the .pl files (with
  PERL5LIB pointing at src/test/perl).
- User's checkout stays on `wet-series`. Do not push. Do not touch other
  branches or worktrees.

## Setup
    git -C <workspace>/postgres worktree add \
        <workspace>/wt-v8-wp4a -b wet-v8-wp4a wet-v8-wp2
Meson/ninja from <workspace>/.venv-v7-rfc/bin.
Build dir <workspace>/build-v8-wp4a, options
`--buildtype=debugoptimized -Dcassert=true -Dwerror=true -Dinjection_points=true`.

## Wiring
contrib/pg_wait_event_tracing/meson.build: add a `'tap'` entry listing the
four tests, with `'env': {'enable_injection_points':
get_option('injection_points') ? 'yes' : 'no'}` (copy the pattern from an
existing module that does this, e.g. src/test/modules/injection_points or
test_misc). Makefile: `TAP_TESTS = 1` and `EXTRA_INSTALL =
src/test/modules/injection_points` where needed (see how other contrib
modules with TAP + injection points do it). Every node: 
`shared_preload_libraries = 'pg_wait_event_tracing'` via `append_conf`,
then `CREATE EXTENSION pg_wait_event_tracing`. Use PostgreSQL::Test::Cluster
and BackgroundPsql for concurrent sessions; poll with `poll_query_until`.
**Windows CI:** any role a test logs in as (not just SET ROLE) must be
registered at init, or SSPI authentication fails on the Windows jobs:
`$node->init(auth_extra => ['--create-role', 'regress_a,regress_a2,regress_b,regress_sig,regress_su']);`
(see other TAP tests that use auth_extra for the exact form). Connect as a
role with `$node->background_psql('postgres', connstr => $node->connstr('postgres') . ' user=regress_a')`
or the equivalent helper; check the helper signatures in src/test/perl.
These cross-backend permission cases were removed from the contrib
regression test for exactly this reason; t/003 is now their only home.

## t/001_memory.pl (fix 1)
`max_connections = 200`. Read the module's footprint from
`pg_dsm_registry_allocations` (sum of size for names starting with
`pg_wait_event_tracing`) — before any capture, then after ONE session sets
`pg_wait_event_tracing.capture = stats` and runs `SELECT pg_sleep(0.01)`.
Assert the total stays below 4 MiB (v6's dense design would need
≈ 238 slots × ~206 KiB ≈ 48 MiB here; the DSA's initial segment makes the
absolute number ~1 MiB, so do not assert < 1 MiB). Then a second session
enables stats: assert growth < 512 KiB. Then the first session sets capture
off: assert its pid has no rows in `pg_stat_wait_event_timing` and that a
third session enabling stats does not grow the footprint (freed payload is
reused).

## t/002_ownership.pl (fix 2)
PGPROC free list is LIFO, so a new connection right after an exit reuses the
exiting backend's ProcNumber when nothing else connects in between.
Session A (role regress_a): capture = stats, `pg_sleep(0.01)`, record A's
pid and procnumber from the timing view. Quit A; `poll_query_until` A's pid
is gone from pg_stat_activity. Connect B (role regress_b, capture off).
Assert: no rows for B's pid in `pg_stat_wait_event_timing` and
`pg_stat_wait_event_timing_overflow`, and no rows anywhere carrying A's
counters under B's pid. Finally B enables stats, sleeps, and the test
asserts B's procnumber equals A's (proves reuse happened; if not, `skip`
with a diagnostic rather than pass silently). Run the reader as a
superuser and again as regress_b to cover the permission boundary.

## t/003_reset_acl.pl (fix 4)
Roles: regress_su (superuser), regress_a and regress_a2 (same role
membership: regress_a2 is a member of regress_a), regress_b, regress_sig
(member of pg_signal_backend). Targets are live BackgroundPsql sessions
with capture = stats and one recorded wait. Cases:
- own reset (NULL and own pid) by regress_b: succeeds;
- regress_a2 resets regress_a's session: succeeds;
- regress_sig resets regress_b's session: succeeds;
- regress_b resets regress_su's session: ERROR, permission denied;
- regress_b resets regress_a's session: ERROR, permission denied;
- any role resets the checkpointer pid (from pg_stat_activity
  backend_type = 'checkpointer'): WARNING "is not a PostgreSQL backend
  process", no error;
- regress_b calls `pg_stat_reset_wait_event_timing_all()`: ERROR.
For the successful cross-backend cases, assert the target's counters are
cleared after its next wait (asynchronous reset: make the target run
`pg_sleep(0.01)` then check reset_count or counts via the view).

## t/004_reset_race.pl (fix 5)
`plan skip_all` unless `$ENV{enable_injection_points} eq 'yes'`.
CREATE EXTENSION injection_points. A (capture = stats, one wait). Superuser
session R: `injection_points_attach('pg-wait-event-tracing-reset-before-publish',
'wait')`, then run `pg_stat_reset_wait_event_timing(<A pid>)` in the
background (BackgroundPsql query without waiting) and poll
pg_stat_activity until R shows wait_event = the injection point. Quit A,
wait until gone. Connect B (reuses the ProcNumber), capture = stats,
`pg_sleep(0.01)` twice. `injection_points_wakeup(...)` and let R finish.
B runs one more wait. Assert B's counters are intact (PgSleep count = 3 or
whatever B recorded) and B's reset count is 0 — the reset aimed at A was
not consumed by B. Detach the injection point.

## Validate and deliver
- `ninja -j2` build (werror), `perl -c` on each test, `meson test --list`
  shows the four TAP tests (listing does not start a server).
- One commit on wet-v8-wp4a, author Dmitry Fomin <fomin.list@gmail.com>,
  subject "pg_wait_event_tracing: TAP tests for memory bound, ownership and
  reset", Discussion trailer as in the other commits.
- Report <workspace>/wp4a-report.md: commit hash,
  what each test asserts, anything uncertain about the expected server
  behaviour (so the reviewer can check it in CI logs). Final message <= 8 lines.
