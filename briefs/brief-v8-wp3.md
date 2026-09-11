# WP3: contrib/pg_wait_event_tracing, trace level (v8-0005)

Plan: <workspace>/IMPLEMENTATION-PLAN-v8.md — read
§2, §4.1, §4.2, §4.2a, §5 (all of it: 5.1–5.5), §7 before starting.

## HARD RULES
- NEVER start a PostgreSQL server on this host: no `meson test`, pg_regress,
  TAP, initdb, pg_ctl, psql against a server. Compile only: meson setup,
  `ninja -j2`, `ninja headerscheck`, `ninja cpluspluscheck`. Tests are
  written here and executed on the fork's CI by the reviewer.
- The user's checkout at <workspace>/postgres stays
  on `wet-series`; never change it. Do not push. Do not touch other branches
  or worktrees.
- Nothing inside the begin/end wait hooks may allocate, lock, wait or
  ereport. Allocation happens only at the safe points already used by the
  statistics level (assign hook when IsNormalProcessingMode(), else
  post_parse_analyze / ExecutorStart), never inside a hook.

## Setup
    git -C <workspace>/postgres worktree add \
        <workspace>/wt-v8-wp3 -b wet-v8-wp3 wet-v8-wp2
Meson/ninja: <workspace>/.venv-v7-rfc/bin first on
PATH. Build dir <workspace>/build-v8-wp3 with
`--buildtype=debugoptimized -Dcassert=true -Dwerror=true -Dinjection_points=true`.
Disk is tight: one build dir only.

## Inputs
1. The statistics-level module on `wet-v8-wp2`: contrib/pg_wait_event_tracing/.
   Its PwetSlot already reserves `trace_ptr` and `trace_state`; do not
   reshape the slot except to add fields the trace level needs.
2. The package's trace code, saved verbatim when WP2 stripped it:
   <workspace>/wp3-trace-parts-from-package.c.txt
3. The v6 reference (read-only; features, record layout, reader algorithm,
   SQL names and column sets):
       git show wet-series:src/backend/utils/activity/wait_event_timing.c
       git show wet-series:src/include/utils/wait_event_timing.h
       git show wet-series:src/backend/catalog/system_views.sql      (pg_backend_wait_event_trace view)
       git diff wet-series-base wet-series -- src/include/catalog/pg_proc.dat
       git show wet-series:src/test/modules/test_misc/t/016_wait_event_trace_seqlock.pl
       git show wet-series:doc/src/sgml/monitoring.sgml               (trace + markers sections)
   NEVER copy v6's core wiring (proc.c/auxprocess.c/postinit.c/postgres.c/
   execMain.c/backend_status.c calls).
4. Defect analysis: sections "V6-3", "V6-6", "V6-7" and §13 "Trace" and
   "Query markers" of
   <scratch>/v7rev/wait-hook-peer-review-20260909/docs/SOURCE-REVIEW-v6-v7.md

## Milestones = one commit each on wet-v8-wp3

### M1 — ring, writer, reader
- `pg_wait_event_tracing.capture` gains the value `trace` (trace implies
  stats, as in v6). New GUC `pg_wait_event_tracing.trace_ring_size`
  (PGC_POSTMASTER, same default/min/max/unit as v6's wait_event_trace_ring_size).
- Trace DSA via `GetNamedDSA("pg_wait_event_tracing_trace", ...)`; ring
  allocated at attach time (same safe points) when capture = trace, freed
  or orphaned per M2.
- Writer: in the end hook, append one 32-byte record per completed wait
  (keep v6's record layout and StaticAssert), single-writer seqlock with
  v6's position-encoded identity check. No allocation, no locks.
- Readers, v6 names and column sets: `pg_get_wait_event_trace()`,
  `pg_get_backend_wait_event_trace(pid)`, view `pg_backend_wait_event_trace`.
  Port v6's reader algorithm (identity check rejects stale/in-flight
  records). Owner-token check as in the statistics readers.
- SQL objects go into a new upgrade-free script: since nothing is released
  yet, extend `pg_wait_event_tracing--1.0.sql` in place.

### M2 — orphan lifecycle (fix 3) and sweep
- On owner exit (before_shmem_exit): ring state → ORPHANED, owner token
  retained, ring kept readable post-mortem (flight recorder, as in v6).
- Reclaim when the successor on that ProcNumber attaches trace: under the
  control lock, if state is ORPHANED and the owner token differs from mine,
  free the old ring (or reuse it if the size matches) and publish mine.
  Nothing runs at process start, so EXEC_BACKEND ordering cannot matter;
  say so in a comment.
- `pg_stat_clear_orphaned_wait_event_rings()` ported (superuser-only, as v6).
- Readers can read ORPHANED rings (post-mortem) and report them with the
  owner's pid.

### M3 — query markers (fix 6) + attribution function + regress tests
Implement exactly the marker table and state machine of plan §5.3:
- `QueryStart` from post_parse_analyze_hook, `ExecStart`/`ExecEnd` from
  ExecutorStart/ExecutorEnd hooks with nesting depth, `UtilityStart`/
  `UtilityEnd` from ProcessUtility_hook, `TxnCommit`/`TxnAbort` from a
  RegisterXactCallback callback (register once per backend at trace attach,
  a safe point), and `Idle` synthesized in the BEGIN hook when
  wait_event_info == WAIT_EVENT_CLIENT_READ and the per-backend marker state
  is AFTER_STATEMENT. Markers are written only when capture = trace. The
  marker writes in the begin hook must obey the hook rules (the ring is
  already allocated; no allocation).
- Query id: do NOT call EnableQueryId() (that would force query jumbling on
  every server that preloads the library even with capture off). QueryStart
  is emitted regardless and carries queryId, which is 0 unless
  compute_query_id is on or another module enabled it. Put this in a code
  comment; the docs WP will document it.
- Markers are written into the same ring as wait records, with a record
  type field, as in v6 (check v6's record type values and keep them where
  they exist; add Idle, UtilityStart/End, TxnCommit/Abort).
- SQL function `pg_wait_event_trace_by_statement(procnumber int)` in the
  extension script (LANGUAGE SQL over the trace SRF, window functions),
  implementing the attribution rule of §5.3: statement interval from
  QueryStart/UtilityStart to the earliest of the next Idle, the next
  depth-0 QueryStart/UtilityStart, or TxnAbort; rows grouped per statement
  and wait event; `<idle>` for waits between Idle and the next start;
  `<unattributed>` before the first marker in the ring.
- Every regression .sql file you add must itself run
  `CREATE EXTENSION IF NOT EXISTS pg_wait_event_tracing;` before using any
  view or function: the library is preloaded, but the SQL objects come only
  from the extension script (WP2's test forgot this and failed on every CI
  platform). Each file in REGRESS starts from a fresh database.
- Right after CREATE EXTENSION, every regression file must run
  `SET debug_parallel_query = off;` (with a comment): the macOS CI job
  forces `debug_parallel_query = regress`, which runs plain SELECTs such as
  `SELECT pg_sleep(...)` in a parallel worker, so their waits and markers
  land in the worker's ring, not the session's (WP2's test failed on macOS
  exactly this way).
- Never expect an exact number of PgSleep records from one `pg_sleep()`:
  it loops on WaitLatch until its own clock says the time is up, and on
  Windows CI one `pg_sleep(0.01)` produced 2 recorded waits. In expected
  output show marker records and a boolean such as "at least one PgSleep
  between these markers", never a count of PgSleep rows.
- Regression tests (contrib sql/expected), deterministic sequences only
  (filter the SRF to marker records plus PgSleep waits; use pg_sleep inside
  statements only where the expected output does not depend on counts):
  single autocommit statement; explicit transaction with two statements
  (Idle between); multi-statement simple-query string (no Idle between);
  utility statement; error mid-statement (TxnAbort closes it); nested SQL
  function (depth 1 inside depth 0); `pg_wait_event_trace_by_statement`
  output on a controlled sequence. Author expected output by careful
  reasoning; state in the report that it was not executed.

## Deliverable
<workspace>/wp3-report.md: per milestone the commit
hash, diffstat, how fix 3 and fix 6 are implemented (function names), the
marker record types table, what the regress tests assert, open questions.
Final message <= 10 lines.
