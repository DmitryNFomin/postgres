# WP4b report: TAP tests for the trace level (fix 3, seqlock, wrap)

Branch `wet-v8-wp4b`, base `wet-v8-wp3` (per the coordinator's override).
Worktree `<workspace>/wt-v8-wp4b`. Build dir
`<workspace>/build-v8-wp4b`
(`--buildtype=debugoptimized -Dcassert=true -Dwerror=true
-Dinjection_points=true`, `.venv-v7-rfc` meson/ninja, `ninja -j2` only).
The main checkout at `<workspace>/postgres`
stayed on `wet-series` throughout; nothing was pushed; no other worktree
was touched (`wt-v8-wp5`, present and locked from another session, was
left alone).

Two commits on `wet-v8-wp4b`, both author `Dmitry Fomin
<fomin.list@gmail.com>`, trailer `Discussion:
https://postgr.es/m/CAPHG-0mAOn05ae6Kqx1wHXxzOk4E5W7ajjd=QBhgkR7a0uyQmw@mail.gmail.com`:

| # | Commit | Subject |
|---|---|---|
| 1 | `5ce93d72f25` | pg_wait_event_tracing: add a test hook for the trace writer's seqlock |
| 2 | `3d10f58e521` | pg_wait_event_tracing: add TAP tests for the trace level |

`git diff --stat wet-v8-wp3..HEAD` touches only
`contrib/pg_wait_event_tracing/pg_wait_event_tracing.c`,
`contrib/pg_wait_event_tracing/meson.build`, and four new files under
`contrib/pg_wait_event_tracing/t/`.

## Commit 1 — injection point in the trace writer

wp3-report.md's M1 (`pg_wait_event_tracing.c`) has no equivalent to v6's
`INJECTION_POINT("wait-event-trace-after-write-pos")`; the module's only
existing injection point is `pwet_request_reset()`'s unrelated
`pg-wait-event-tracing-reset-before-publish` (stats-level reset race,
fix 5). Added `INJECTION_POINT("pg-wait-event-tracing-trace-after-write-pos",
NULL)` to `pwet_wait_end()`'s trace-record write path, between the
`pg_atomic_write_u64(&pwet_my_trace->write_pos, pos + 1)` advance and
`rec->seq = seq` — the exact hazard window v6's test defends against.
Followed the existing call's convention: no explicit `#ifdef
USE_INJECTION_POINTS` at the call site, relying on
`utils/injection_point.h`'s own compile-time no-op (`((void) name)`)
when the build wasn't configured with injection points; documented why
that makes the call acceptable inside a hook that must otherwise never
allocate, lock, wait, or ereport, directly on the call site.
`pwet_trace_write_marker()` (used for every query-attribution marker)
has the identical two-store seqlock pattern but was **not** separately
instrumented — noted in the comment that one hazard-window test on the
shared protocol is enough; this is the one place I made a judgment call
rather than following an explicit instruction.

## Commit 2 — four TAP test files

### t/005_orphan_reuse.pl (fix 3; per the brief, must also pass on Windows)

Part 1: session A enables trace, records one `pg_sleep(0.01)`, captures
its pid, procnumber, and the **exact** `timestamp_ns` of its PgSleep
record (not just "a PgSleep row exists") from its own ring. A quits;
after it's gone from `pg_stat_activity`, asserts the orphaned ring is
still readable via `pg_get_wait_event_trace(procnumber)`, still tagged
with A's pid, and the exact record (by `timestamp_ns`) survives
untouched. Then loops (FIFO-aware, `max_connections=10`, up to 30
attempts, reading each candidate's own ProcNumber via
`pg_stat_get_backend_idset()`/`pg_stat_get_backend_pid()`) until a
successor B lands on A's ProcNumber; B enables trace and records its own
wait, **without calling the sweep function**, and the test asserts B
owns a fresh ring (its own pid), A's specific record (by timestamp) is
gone, and B's own PgSleep record is present. `SKIP`s (not fails) if no
successor reused the ProcNumber within the attempt budget.

Part 2: a second session A2 traces and exits with nobody reusing its
ProcNumber; asserts the orphan is readable, that
`pg_stat_clear_orphaned_wait_event_rings()` errors with "permission
denied" for a non-superuser (one-shot `$node->psql` with a
`regress_orphan` role, per the hard rule against expecting an ERROR in a
`BackgroundPsql` session), and that the superuser sweep frees it
(`freed >= 1`, and the ring is empty afterward).

### t/010_trace_seqlock.pl (port of v6's test)

Direct port of
`test_misc/t/016_wait_event_trace_seqlock.pl` onto this module's names
(`pg_wait_event_tracing.capture`/`trace_ring_size`,
`pg_get_wait_event_trace`, `pg_stat_get_wait_event_timing` — all
unchanged from v6's own names, so the port is almost entirely mechanical
besides the GUC prefix and the new injection point name). Same
mechanism: 400 `pg_sleep(0.001)` calls in one statement wrap a
256-record (8kB, GUC minimum) ring; arms
`pg-wait-event-tracing-trace-after-write-pos` with a `wait` action; a
`\echo`-fronted `SELECT 1` on the writer completes its blocked
`ClientRead` wait and wedges mid-record; a cross-backend read must
return exactly `ring_size - 1` rows (skipping the in-flight slot) and be
stable on a repeat read; releasing the injection point and resyncing
restores a full `ring_size`-row read. One documented difference from
v6: this module's ring holds a mix of wait records and query-attribution
markers (fix 6, added after v6), not wait records only — noted in a
comment; the identity-check counting logic is unaffected either way,
since it operates uniformly on whichever record type occupies each
position.

### t/011_trace_wrap.pl

Minimum-size ring (8kB/256 records) again. Writer launches (via the
`\echo`-fronted `query_until` trick, not waited on) one statement doing
800 `pg_sleep(0.003)` calls (~2.4s), guaranteeing several full wraps. A
separate reader session polls `count(*)`/`count(DISTINCT seq)`/`min(seq)`/`max(seq)`
five times (300ms apart) **during** the write burst, asserting every
read has no duplicate `seq`, no gap other than possibly the single
newest (still in-flight) record, and never exceeds ring capacity — using
the fact that `seq` is the writer's absolute append counter (not a
ring-wrapped index), so a valid read is structurally always a contiguous
block. After polling the writer to `state = 'idle'`, asserts the settled
ring's `count(*)` equals ring capacity exactly, `max(seq)-min(seq)+1`
equals that count (contiguous), and `min(seq) > 0` (the oldest records,
starting at 0, were evicted). Finally checks that reading succeeds (no
error) both after the writer disables trace (ring freed, read comes back
empty) and after the writer re-enables trace, records one more wait, and
exits outright (ring orphaned, read finds it non-empty) — full
reclaim/sweep mechanics are t/005's job, this only checks "the read
itself doesn't fail" in both cases.

### t/012_trace_markers.pl (the two extra cases from the coordinator's override)

Not one of the three brief-named files; added as a fourth file since
neither extra case fits the orphan/seqlock/wrap concerns of 005/010/011.
Numbered 012 (after every number already claimed elsewhere in the plan)
to avoid colliding with any other WP's reserved slot.

**(a) Idle marker.** Verified directly in `be-secure.c`
(`secure_read()`): `WAIT_EVENT_CLIENT_READ` is only reported from the
branch taken after a non-blocking read returns `EWOULDBLOCK` — never
around a read satisfied immediately from already-buffered bytes — which
is why the regress test's Idle-presence assertions were flaky in both
directions and got filtered out entirely (per wp3-report.md). This test
controls that directly: a `BackgroundPsql` session runs one statement,
a real 2-second Perl-side `sleep()`, then another statement; asserts an
`Idle` marker appears between their non-`Idle` `Query`-type markers.
Separately, `$node->safe_psql` sends `SELECT 1; SELECT 2;` on one input
line (one simple-query protocol message, per the same assumption the
module's own regress test case 3 relies on and flags as needing a real
CI run to fully trust — this test shares that one risk, called out in
its header) and asserts no `Idle` appears between them.

**(b) `pwet_marker_txn_abort()`'s defensive `pwet_exec_depth` reset.**
The regress test's case 5 (`SELECT 1/0`) raises at planning time before
`ExecutorStart`, so it can't exercise this. Built instead: a table with
rows `(1), (0)`, and a **PROCEDURE** (not a function) whose PL/pgSQL body
loops over the table `ORDER BY d DESC` and `PERFORM 1 / r.d` — the first
iteration (d=1) succeeds, the second (d=0) divides by zero mid-execution.
Using `CALL` rather than `SELECT function()` was a deliberate, reasoned
choice, not literally what the override's example wording said: `CALL`
is dispatched through `ProcessUtility` (`T_CallStmt`, confirmed in
`utility.c`), not the executor, so the outer call contributes only a
`UtilityStart` marker and never touches `pwet_exec_depth` itself, leaving
**exactly one** nested executor level (the failing `PERFORM`'s own)
unclosed by the error — matching the override's own "off by one" wording
literally. (A plain `SELECT function()` wrapping the same PL/pgSQL body
would leave depth stuck at 2, not 1, since the outer `SELECT` call is
itself a second, separate `ExecStart` level — I traced this by hand
before choosing `CALL`/`PROCEDURE` over `SELECT`/`FUNCTION`.) Uses
`background_psql('postgres', on_error_stop => 0)` so the session survives
the expected error (a plain `BackgroundPsql` session would die on it, per
the hard rule, but the hard rule's one-shot-`psql` guidance is for a
check that only needs to observe the error and stop — here the same
session must keep going to check the *next* statement, which
`on_error_stop => 0` is the established idiom for, per multiple existing
uses of exactly this pattern elsewhere in `src/test/`); manually resets
`$psql2->{stderr} = ''` after asserting on it, matching
`test_aio/t/001_aio.pl`'s `psql_like()` helper, since `BackgroundPsql`
never clears `{stderr}` itself and a later `query_safe` would otherwise
misreport the stale text as a fresh failure. The following statement's
own (self-referential) `ExecStart` marker is asserted to report `depth =
0`.

## Deliberate deviations from the brief's literal wording

1. The override's example for case (b) said "a PL/pgSQL function whose
   PERFORM divides by a column value that is zero on the second row";
   I used a **procedure** called via `CALL` instead of a function called
   via `SELECT`, because only that produces the literal "off by one"
   depth the override itself describes as the failure mode without the
   fix (worked out by hand-tracing `pwet_marker_exec_start()`/
   `pwet_marker_exec_end()`/`pwet_marker_utility_start()` against both
   shapes). If the reviewer prefers the literal function/SELECT form
   despite it producing a depth-2 (not depth-1) leak, that is a small,
   mechanical change (drop `PROCEDURE`+`CALL` for
   `FUNCTION`+`SELECT ... ()`, and check `depth = '0'` still, since the
   assertion itself doesn't depend on which stuck value would have
   appeared without the fix).
2. The two extra cases live in a new file, `t/012_trace_markers.pl`, not
   inside 005/010/011 — none of those three files' own subject matter
   (orphan reuse, seqlock, wrap) fits either case.

## Uncertainties for the reviewer to check in CI logs

1. **t/012's same-line-batching assumption** (Idle absence case): shares
   the one assumption wp3-report.md flagged as unconfirmed in the
   regress file itself (psql really sends two same-line
   semicolon-separated statements as one simple-query protocol message).
   Not independently re-verified here; if the regress test's case 3 is
   confirmed green in CI, this case should be too.
2. **t/012's Idle-presence case's `sleep(2)`**: chosen as a generous,
   round margin against CI scheduling jitter, following the same
   reasoning as other real-time-sensitive tests in this series, but not
   empirically tuned against an actual CI run. If flaky, raising it is
   the first thing to try.
3. **t/010's mixed record content**: v6's original test assumed a
   wait-record-only ring; this module's ring also carries query
   markers (fix 6). I reasoned through why the count assertions
   (`ring_records` full, `ring_records - 1` wedged) hold regardless of
   the exact mix of record types at each ring position, but this
   reasoning has not been checked against a real run.
4. **t/011's concurrent-read timing**: the 800×0.003s writer burst and
   5×300ms reader polls are sized to comfortably overlap on a normal
   machine, but a very slow or very fast CI runner could finish the
   burst before all five polls complete. This does not make the test
   flaky (the assertions hold equally well against a fully-settled ring),
   just reduces how much "truly concurrent" coverage that specific run
   gets — no action needed unless the reviewer wants tighter guarantees.

## Validation performed

- `meson setup build-v8-wp4b --buildtype=debugoptimized -Dcassert=true
  -Dwerror=true -Dinjection_points=true` — clean.
- `ninja -j2 contrib/pg_wait_event_tracing/pg_wait_event_tracing.so` —
  clean under `-Dwerror=true`, re-verified at both commits.
- `ninja -j2 headerscheck` and `ninja -j2 cpluspluscheck` — both pass at
  the final commit (built the five generated grammar headers
  `gram.h`/`repl_gram.h`/`syncrep_gram.h`/`jsonpath_gram.h`/`pl_gram.h`
  once up front, as WP3 did; no full-tree build).
- `perl -c` (with `PERL5LIB=src/test/perl`) on all four new files —
  syntax OK.
- `meson introspect --tests` — confirms
  `pg_wait_event_tracing/005_orphan_reuse`,
  `/010_trace_seqlock`, `/011_trace_wrap`, and `/012_trace_markers` are
  all registered alongside the pre-existing `/regress` and
  `/006_server_processes` (listing only; no server started).
- **Not run** (hard rule: compile only, no server): `meson test` in any
  form, `pg_regress`, `initdb`/`pg_ctl`, TAP execution. No PostgreSQL
  server process was started at any point in this session.
- Disk: `build-v8-wp4b` stayed at ~24 MiB throughout (module objects and
  the five generated grammar headers only); host had 19 GiB free at the
  end of the session (well above the brief's ~1.5 GiB warning — likely
  other agents' worktrees/builds from earlier sessions were cleaned up
  since). Left the build dir in place, matching WP3/WP2b's own choice
  not to delete theirs when disk pressure wasn't acute.
