# WP4b: TAP tests for the trace level (fix 3, seqlock, wrap)

Plan: <workspace>/IMPLEMENTATION-PLAN-v8.md §5.1, §5.2, §5.5.
Code under test: contrib/pg_wait_event_tracing on branch `wet-v8-wp3` (read
wp3-report.md first: SQL names, record types, ring states, sweep function).

## HARD RULES
- NEVER start a PostgreSQL server on this host (no meson test, prove,
  pg_regress, initdb, pg_ctl). Write the tests; they run on the fork's CI.
  Allowed: meson setup, `ninja -j2`, headerscheck, cpluspluscheck, `perl -c`
  with PERL5LIB=src/test/perl, `meson test --list`.
- User's checkout stays on `wet-series`. Do not push. Do not touch other
  branches or worktrees. Nothing inside the wait hooks may allocate, lock,
  wait or ereport (applies if you add injection points to the writer).

## Setup
    git -C <workspace>/postgres worktree add \
        <workspace>/wt-v8-wp4b -b wet-v8-wp4b wet-v8-wp3
Build dir <workspace>/build-v8-wp4b, same options as
the other WPs (debugoptimized, cassert, werror, injection_points), meson from
.venv-v7-rfc. Add the tests to the module's existing `'tap'` list (WP4a
created it on another branch; if it is absent here, create the entry the same
way and the reviewer will merge).

Every node: `shared_preload_libraries = 'pg_wait_event_tracing'` via
append_conf, start, then `CREATE EXTENSION pg_wait_event_tracing` in each
database the test queries — the library is preloaded, but the views and
functions exist only after CREATE EXTENSION (WP2's regress test forgot this
and failed on every CI platform). Any role a test logs in as must be
registered with `auth_extra => ['--create-role', '...']` at init, or
Windows SSPI authentication fails. Every node also gets
`debug_parallel_query = off` via append_conf: the macOS CI job initdb's all
nodes with `debug_parallel_query = regress`, which moves plain SELECTs such
as `SELECT pg_sleep(...)` into parallel workers, whose waits land in the
worker's own ring.

**Never equate one `pg_sleep()` with one recorded wait.** pg_sleep() loops
on WaitLatch until its own clock says the time is up; on Windows CI one
`pg_sleep(0.01)` was recorded as 2 waits (and 3 sleeps as 4–5). Assert
presence (`>= 1`), relative change, reset_count, or the presence/absence of
a specific wait event — never an exact PgSleep count. Also: a check that
expects an ERROR must not run in a BackgroundPsql session (on_error_stop
kills it); use a one-shot `$node->psql(...)` and inspect its stderr.

## t/005_orphan_reuse.pl (fix 3; must also pass on the Windows CI jobs)
Session A: capture = trace, `pg_sleep(0.01)`, record pid, procnumber and the
PgSleep record from the trace view. Quit A; poll until gone. Assert A's ring
is still readable post-mortem (ORPHANED, A's pid, A's record present).
Then get a session B onto A's ProcNumber. The PGPROC free list on master is
FIFO (InitProcess pops the head, ProcKill pushes to the tail), so an
immediate reconnect does NOT reuse the slot. Set `max_connections = 10` on
the node and loop, at most 3 * max_connections times: open a candidate
session, read its ProcNumber with `SELECT id FROM pg_stat_get_backend_idset()
AS id WHERE pg_stat_get_backend_pid(id) = pg_backend_pid()` (same numbering
as the module's procnumber), keep it if it equals A's, otherwise quit it and
retry; `skip` with a diagnostic if the loop runs out. B then sets
capture = trace and runs `pg_sleep(0.02)`. Assert: B owns a fresh ring (B's pid, B's record present,
A's record gone for this ProcNumber), without calling the sweep function.
Then a separate case: A2 traces and exits, nobody reuses; call
`pg_stat_clear_orphaned_wait_event_rings()` as superuser and assert the
orphan disappears; as a non-superuser the call errors.

## t/010_trace_seqlock.pl (port of v6's test)
Port `git show wet-series:src/test/modules/test_misc/t/016_wait_event_trace_seqlock.pl`
to the module: same hazard, same assertions (the reader must reject the
stale previous-cycle record at the in-flight slot). v6 used an injection
point inside the writer between advancing write_pos and stamping the
record's seq; check whether the module's writer has an equivalent point
(wp3-report.md). If not, add one in a separate commit, guarded by
USE_INJECTION_POINTS, with the same name scheme
(`pg-wait-event-tracing-<what>`), and note that INJECTION_POINT() inside a
hook is only acceptable because it is compiled only in injection-point test
builds and the test attaches a 'wait' action from another session — say so
in a comment. `plan skip_all` unless enable_injection_points = yes.

## t/011_trace_wrap.pl
Small `trace_ring_size` (the GUC minimum). One session produces more records
than the ring holds (loop of `pg_sleep(0)` is NOT a wait on Linux; use a
PL/pgSQL loop of `pg_sleep(0.001)` or a module-independent latch wait that
records one PgSleep per iteration). Assert: record count equals the ring
capacity, sequence numbers are contiguous and the oldest ones were
overwritten; a concurrent reader session reading repeatedly during the loop
never returns a record that fails the identity check (no duplicates, no
gaps inside one read); reading after the writer disables trace and after
the writer exits both succeed.

## Deliver
- One commit on wet-v8-wp4b (plus the optional injection-point commit before
  it), author Dmitry Fomin <fomin.list@gmail.com>, Discussion trailer.
- Report <workspace>/wp4b-report.md: commit
  hashes, what each test asserts, uncertainties for the reviewer to check in
  CI logs. Final message <= 8 lines.
