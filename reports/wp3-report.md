# WP3 report: contrib/pg_wait_event_tracing, trace level (v8-0005)

Branch `wet-v8-wp3`, base `wet-v8-wp2b` (per the coordinator's override, not
`wet-v8-wp2` as the original brief said). Worktree
`<workspace>/wt-v8-wp3`. Build dir
`<workspace>/build-v8-wp3`
(`--buildtype=debugoptimized -Dcassert=true -Dwerror=true
-Dinjection_points=true`, `.venv-v7-rfc` meson/ninja, `ninja -j2` only). The
main checkout at `<workspace>/postgres` stayed on
`wet-series` throughout; nothing was pushed; no other worktree was touched.

**Mid-task rebase:** the coordinator landed `aa93d6e5fd4` ("create the server
region only when it was requested") on `wet-v8-wp2b` while this WP was
starting. My branch's tip (`f2079090fe2`) was a direct ancestor of the new
`wet-v8-wp2b` tip, so `git rebase wet-v8-wp2b` was a pure fast-forward, no
conflicts, done before any WP3 edits existed.

Four commits on `wet-v8-wp3` (M1, M2, M3, where M3 also folds in three
coordinator review-fixes per their explicit instruction "fold into M3 or a
follow-up commit"):

| # | Commit | Subject |
|---|---|---|
| M1 | `a7e709759f5` | pg_wait_event_tracing: add the trace ring, writer, and readers |
| M2 | `e36829e9ac5` | pg_wait_event_tracing: orphan the trace ring on exit instead of freeing it (fix 3) |
| M3 | `dd2f1ba4eb8` | pg_wait_event_tracing: add query-attribution markers and by-statement view (fix 6) |

All three: author `Dmitry Fomin <fomin.list@gmail.com>`, trailer
`Discussion: https://postgr.es/m/CAPHG-0mAOn05ae6Kqx1wHXxzOk4E5W7ajjd=QBhgkR7a0uyQmw@mail.gmail.com`.
`git diff --stat wet-v8-wp2b..HEAD -- contrib/` touches only
`contrib/pg_wait_event_tracing/**` (6 files, 1831 insertions, 74 deletions).

## Postmaster/fork trap (coordinator's mid-task note)

Checked every new trace-level per-process static (`pwet_trace_dsa`,
`pwet_my_trace`, `pwet_trace_records_per_ring`, `pwet_marker_state`,
`pwet_exec_depth`, `pwet_xact_callback_registered`) against the WP2b-class
bug (a value wrongly computed/cached in the postmaster, then inherited as
wrong-for-this-process by a fork()ed child): none of them can be set before a
fork. The postmaster itself runs `wait_event_begin_hook`/`wait_event_end_hook`
(`ServerLoop`'s `WaitEventSetWait`), but every trace/marker write is gated on
`pwet_my_procno != INVALID_PROC_NUMBER` (trace) or fires only from
`post_parse_analyze`/`ExecutorStart`/`ExecutorEnd`/`ProcessUtility`/the xact
callback (markers) -- none of which the postmaster ever reaches (it has no
valid `MyProcNumber`, parses no queries, runs no executor, processes no
utility statements, runs no transactions). `pwet_marker_state`/
`pwet_exec_depth` therefore never leave their zero-valued initial state
before any fork, which is also the correct starting state for a freshly
forked child. `pwet_trace_records_per_ring` is additionally
identity-independent (derived only from the `PGC_POSTMASTER` GUC
`trace_ring_size`, latched identically for every process in this postmaster
run, exactly like the pre-existing `pwet_max_tranches` usage), so even a
hypothetical shared/inherited value could not be wrong. Documented as a code
comment on the static declarations.

No new "decision made once in the postmaster, must be read back by children
instead of re-derived" state was introduced (the pattern from `aa93d6e5fd4`'s
`PwetRegionHeader`): trace doesn't touch the fixed server-process region at
all (out of scope per the brief's override 2), and `GetNamedDSA()` (the trace
DSA's creation/attach mechanism) already solves the "created once, attached
everywhere including EXEC_BACKEND children" problem generically, the same way
the stats DSA already relies on it.

## M1 -- ring, writer, readers (`a7e709759f5`)

`pg_wait_event_tracing.capture` gains `trace` (implies stats, as `pwet_wait_begin`/
`pwet_wait_end`/`pwet_can_attach` already gate on `!= OFF`). New GUC
`pg_wait_event_tracing.trace_ring_size` (`PGC_POSTMASTER`, default/min/max/unit
and power-of-two check hook ported from v6's `wait_event_trace_ring_size`).
`PwetTraceRecord`/`PwetTraceState` and the seqlock writer/reader protocol are
ported from `wp3-trace-parts-from-package.c.txt`, StaticAssert kept.

`PwetSlot` gains **two** new fields, not one:  `trace_owner_pid`/
`trace_owner_start`, deliberately independent of the existing `owner_pid`/
`owner_start`. Rationale (this was the one real design decision in M1): a
trace ring's producer must stay identifiable even after that process has
exited and a *successor* has already claimed the same ProcNumber's *stats*
slot (needed for M2's orphan lifecycle) -- sharing one owner token between
stats and trace would make the successor's routine stats attach silently
reattribute the predecessor's still-orphaned trace ring to itself.

`pwet_attach_trace()`/`pwet_release_trace()` attach/release at the existing
safe points, never in the begin/end hooks. Readers, v6 names/column sets plus
a new `depth` column (added for M3's marker set, so it didn't need a second
signature change): `pg_get_backend_wait_event_trace()`,
`pg_get_wait_event_trace(procnumber)`, view `pg_backend_wait_event_trace`.
v6's position-encoded identity seqlock check ported verbatim.

## M2 -- orphan lifecycle and sweep, fix 3 (`e36829e9ac5`)

`pwet_orphan_trace()`: on exit, `trace_state` -> `ORPHANED`, `trace_ptr` and
`trace_owner_pid`/`start` retained (not freed, not cleared) -- called from
`pwet_before_shmem_exit()` instead of `pwet_release_trace()` (which still
runs on a *live* step-down, e.g. trace -> stats, and still frees
immediately, matching v6's "operator explicitly disabled it" reasoning).
Reclaim needed no new code: `pwet_attach_trace()` (from M1) already frees any
pre-existing ring at the slot before publishing a fresh one, regardless of
ACTIVE/ORPHANED, and it only ever runs from safe points -- nothing runs at
process start, so v6's EXEC_BACKEND ordering defect (V6-3, a clear-orphan-at-init
step racing shared-memory attachment) cannot recur by construction, not by a
matching fix.

New SQL function `pg_stat_clear_orphaned_wait_event_rings()`, one deliberate
deviation from v6: hard `superuser()` in C, not just a revocable
`REVOKE EXECUTE FROM PUBLIC`, matching how fix 4 already made
`pg_stat_reset_wait_event_timing_all()` superuser-only in C on this module.
`pg_get_wait_event_trace(procnumber)` gained an `owner_pid` output column (v6
has none) so a post-mortem reader can identify an orphan's producer without a
second lookup that would fail anyway (the producer's `PgBackendStatus` entry
is gone).

## M3 -- query markers, fix 6, and review fixes (`dd2f1ba4eb8`)

### Marker sources (function names) and record types

| Marker | Emitted from | `record_type` value |
|---|---|---|
| `QueryStart` | `pwet_post_parse_analyze()` -> `pwet_marker_query_start()` | `PWET_TRACE_QUERY_START` = 1 |
| `ExecStart` | `pwet_ExecutorStart()` -> `pwet_marker_exec_start()` | `PWET_TRACE_EXEC_START` = 3 |
| `ExecEnd` | `pwet_ExecutorEnd()` (new hook) -> `pwet_marker_exec_end()` | `PWET_TRACE_EXEC_END` = 4 |
| `UtilityStart` | `pwet_ProcessUtility()` (new hook) -> `pwet_marker_utility_start()` | `PWET_TRACE_UTILITY_START` = 5 |
| `UtilityEnd` | `pwet_ProcessUtility()` -> `pwet_marker_utility_end()` | `PWET_TRACE_UTILITY_END` = 6 |
| `TxnCommit` | `pwet_xact_callback()` (new `RegisterXactCallback`) -> `pwet_marker_txn_commit()` | `PWET_TRACE_TXN_COMMIT` = 7 |
| `TxnAbort` | `pwet_xact_callback()` -> `pwet_marker_txn_abort()` | `PWET_TRACE_TXN_ABORT` = 8 |
| `Idle` | `pwet_wait_begin()` directly (the begin hook itself), gated on `wait_event_info == WAIT_EVENT_CLIENT_READ` and marker state `AFTER_STATEMENT` | `PWET_TRACE_IDLE` = 9 |
| (wait record) | `pwet_wait_end()` | `PWET_TRACE_WAIT` = 0 |

Values 0/1/3/4 kept where the peer-review package already used them; the
package's `QUERY_END` (2) has no v8 equivalent and its value is left
unassigned rather than reassigned. State machine: `IDLE ->
(QueryStart|UtilityStart|ExecStart) -> OPEN -> (ExecEnd@depth0|UtilityEnd|
TxnCommit|TxnAbort) -> AFTER_STATEMENT -> (first ClientRead wait) -> IDLE`
(emitting `Idle`); a start marker while `AFTER_STATEMENT` goes straight back
to `OPEN`, no `Idle` emitted (pipelined batch / multi-statement string /
already-buffered next statement in an explicit transaction). `QueryStart`
and `UtilityStart` now carry the executor nesting depth in effect when they
fire (not always 0): `post_parse_analyze` can itself run from inside an
already-open outer statement (SPI from a SQL/PL function), and the
attribution function's "next start" boundary is specifically gated on depth
0 so a nested start doesn't appear to close the outer statement.

`EnableQueryId()` is deliberately never called (documented on
`pwet_post_parse_analyze()`); `QueryStart` fires unconditionally and carries
whatever `queryId` happens to be, 0 unless `compute_query_id` is on or
another module enabled it.

### Attribution function

`pg_wait_event_trace_by_statement(procnumber)`, `LANGUAGE SQL` over
`pg_get_wait_event_trace()`, window functions (`count(*) FILTER (...) OVER
(ORDER BY seq ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` builds a
monotonic "bucket group" id every time a boundary marker is seen; the
boundary's own row supplies each bucket's label). Boundary markers:
`QueryStart`/`UtilityStart` at depth 0 (open a statement bucket, labelled by
that marker's own `seq` and `query_id`), `Idle` and `TxnAbort` (open the
synthetic `<idle>` bucket). Waits before the first boundary in the ring are
`<unattributed>`. **Judgment call, not explicit in the plan**: the plan says
`TxnAbort` ends a statement's interval but doesn't name what the *following*
waits belong to before the next real activity; treating it as opening
`<idle>` (rather than inventing a third bucket) was my reading, documented in
the SQL comment.

### Review fixes folded into M3 (coordinator's mid-task review of M1/M2)

**(A) CRITICAL, fixed:** `pwet_maybe_attach()`'s outer gate was
`!pwet_can_attach()`, which is unconditionally false for a fixed-region
ProcNumber -- so the trace-attach branch was dead code for every server-side
process (checkpointer, walwriter, background writer, startup, WAL receiver,
I/O workers, autovacuum workers, WAL senders), exactly the processes plan
sec 4.2a/5.2 say should trace after a reload. Split into `pwet_at_safe_point()`
(identity/mode only), `pwet_can_attach()` (safe point + capture wants it +
not a fixed ProcNumber -- the DSA path specifically), `pwet_can_attach_trace()`
(safe point + capture == trace + a ProcNumber identity already exists --
trace has no fixed-region exclusion). `pwet_maybe_attach()` now gates on the
safe-point test alone. Also duplicated the `before_shmem_exit()` registration
into `pwet_claim_fixed_slot()` (the begin-hook path): a fixed-slot process's
stats "attach" never goes through `pwet_maybe_attach()` at all, so without
this a fixed-slot process's trace ring, once reachable, would stay `ACTIVE`
forever after exit, invisible to the orphan sweep. Verified
`before_shmem_exit()` only writes a fixed-size array slot
(`ipc.c`'s `before_shmem_exit_list[MAX_ON_EXITS]`) -- no allocation, lock, or
`ereport` on the non-full path, so it's within the hook's rules. **Left for
WP4b, per the coordinator:** an actual TAP test exercising "a server-side
process traces after a reload" (needs a real server).

**(B) REAL, fixed:** `pg_get_wait_event_trace()` held `pwet_lock` `LW_SHARED`
across the *entire* call including every `tuplestore_putvalues()`, even
though `emit_wait_event_trace()`'s own comment described releasing it first.
Restructured: `emit_wait_event_trace(procnumber, rsinfo)` now resolves the
ring and copies validated records under the lock, releases it, then emits.
Also moved the worst-case buffer `palloc()` before the lock, sized from
`pwet_trace_records_per_ring` (the cluster-wide capacity, computed here if
not already cached) rather than the specific ring's size, matching v6's own
rationale (a palloc that size can bottom out in a `mmap()` syscall).

**(C) Answered, no code change:** checked v6 directly
(`git show wet-series:src/backend/catalog/system_views.sql`) -- v6 also
revokes `EXECUTE` on `pg_get_backend_wait_event_trace()` from `PUBLIC` and
grants it only to `pg_read_all_stats`, with its own comment explaining why
("so a role that can enable trace cannot read its own ring via the function
and bypass the view"). M1's extension script already matched this verbatim.
The asymmetry with `pg_stat_get_wait_event_timing()` (PUBLIC-executable,
filtered per row) is intentional in both v6 and here: trace rows carry raw
`query_id` and wait sequences precise enough to leak information across a
SECURITY DEFINER call chain, which an aggregate count/duration does not.

### Regression tests (`sql`/`expected`/`pg_wait_event_tracing_trace.sql`, wired
into both `Makefile` and `meson.build`'s `REGRESS`/`sql` lists)

Every case: `CREATE EXTENSION IF NOT EXISTS`, `SET debug_parallel_query = off`
(both per the brief's hard rules -- WP2's test failed CI on exactly these two
points). Cases, in order: (1) single autocommit statement; (2) explicit
transaction, two statements on separate lines (`Idle` between, no
`TxnCommit`/`TxnAbort` until the final `COMMIT`); (3) two statements on the
same input line, asserting *no* `Idle` between them; (4) a utility statement
(`CREATE TABLE`) -- and note the `TxnCommit`/`UtilityEnd` *ordering differs*
from case 2's `COMMIT` (fires before `UtilityEnd` there, since the utility
statement itself is what ends the transaction; after, here, since the
implicit per-statement commit happens in the command loop after
`ProcessUtility_hook` returns); (5) `SELECT 1/0` -- unmatched `ExecStart`,
`TxnAbort` closes it, no `ExecEnd`; (6) a nested SQL-language function call
(warmed up once outside the measured window to avoid the parse-caching
question), asserting `depth` 0/1/1/0 on the Exec markers; (7)
`pg_wait_event_trace_by_statement()` attribution of a `pg_sleep()`, asserting
only presence/non-`<idle>` attribution, never an exact wait count (per plan
sec 5.5).

Reading a session's own ring via SQL is unavoidably self-referential (the
observing `SELECT` writes its own `QueryStart`+`ExecStart` before its body
runs, and the "mark" `SELECT` used to record a starting position finishes
writing its own `ExecEnd`+`TxnCommit`+`Idle` *after* that position was
captured mid-execution). Every case uses one deterministic pattern
(documented in the file's header) to strip exactly that noise via array
slicing (`[4:count(*)-2]`), verified by hand-tracing the marker sequence
through each statement kind.

**Not executed** (hard rule: no server). The `.out` file was generated with a
small Python formatter (`pgfmt.py`, kept in the scratchpad, not committed)
that I calibrated byte-for-byte against this module's own already-committed
`pg_wait_event_tracing.out` (reproduced its multi-column and boolean-column
blocks exactly) before generating the new content mechanically -- the same
approach WP2 used. **Highest-risk assumption, needs CI confirmation before
trusting case 3**: that psql really sends two same-line semicolon-separated
statements from a script file as one simple-query protocol message rather
than splitting them regardless of line breaks; this is called out explicitly
in the test file's own comment. Every other case's reasoning (marker
ordering, depth nesting, autocommit-vs-explicit-transaction commit timing) is
independent of that assumption.

## Open questions

1. Case 3's same-line-batching assumption (above) is the one thing in this
   WP I could not verify by reasoning alone with high confidence -- it needs
   a real CI run before the file can be trusted.
2. The `<idle>`-for-`TxnAbort` bucketing choice in
   `pg_wait_event_trace_by_statement()` is my reading of an underspecified
   corner of plan sec 5.3, not an explicit instruction; worth the owner's
   confirmation.
3. Per the coordinator, an actual TAP test for "a server-side process traces
   after a reload" (exercising fix (A) above end-to-end) is deferred to
   WP4b, since it needs a real server this WP cannot run.
4. `docs/SOURCE-REVIEW-v6-v7.md`'s V6-7 (the advertised direct-reader API
   isn't exported) doesn't need a separate fix here: every shared structure
   (`PwetSlot`, `PwetTraceState`, `PwetTraceRecord`, `pwet_lock`,
   `pwet_trace_dsa`) is `static` to this one file; the SQL SRFs are the only
   surface, matching plan sec 5.4 (fix 7) directly, with nothing further to
   do.

## Validation performed

- `meson setup build-v8-wp3 --buildtype=debugoptimized -Dcassert=true
  -Dwerror=true -Dinjection_points=true` -- clean.
- `ninja -j2 contrib/pg_wait_event_tracing/pg_wait_event_tracing.so` --
  clean under `-Dwerror=true`, re-verified at the M1, M2, and final (M3 +
  review fixes) commit boundaries.
- `ninja -j2 headerscheck` and `ninja -j2 cpluspluscheck` -- both pass,
  re-verified at the M2 and final state (only the generated grammar headers
  needed for these, `gram.h`/`repl_gram.h`/`syncrep_gram.h`/
  `jsonpath_gram.h`/`pl_gram.h`, were built once up front; no full-tree
  build).
- `meson introspect --tests` -- confirmed the new
  `pg_wait_event_tracing_trace` file is wired into the same `regress` test
  entry as the existing one (`--tests pg_wait_event_tracing
  pg_wait_event_tracing_trace`), listing only, no server started.
- **Not run** (hard rule: compile only, no server): `meson test` in any
  form, `pg_regress`, `initdb`/`pg_ctl`. No server process was started at
  any point in this session.
- Disk: shared with other concurrent agents' worktrees/builds on this host;
  dropped from ~4.6 GiB to ~1.6 GiB free over the session from causes
  outside this WP's own build (`build-v8-wp3` itself stayed ~24-28 MiB
  throughout, module objects and the five generated grammar headers only).
  Only `build-v8-wp3` was used, `ninja -j2` throughout.
