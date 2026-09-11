# WP5: documentation for pg_wait_event_tracing + removal audit

Plan: <workspace>/IMPLEMENTATION-PLAN-v8.md §2, §4,
§5 (especially §5.3 markers), §6, §7. Code: the assembled branch named by the
reviewer when launching this WP (it contains WP2 + WP3 + WP4 commits). Read
wp2-report.md, wp3-report.md, and the extension script
contrib/pg_wait_event_tracing/pg_wait_event_tracing--1.0.sql: the docs must
describe what the code does, with exact names and columns taken from the
script, not from memory or from v6.

## HARD RULES
- NEVER start a PostgreSQL server on this host. Allowed: meson setup, ninja,
  and the documentation build/validation targets (e.g. `ninja docs` or the
  xmllint validation target), if xmllint/xsltproc are installed; if they are
  not, say so in the report instead of installing anything system-wide.
- User's checkout stays on `wet-series`. Do not push. Do not touch other
  branches or worktrees.

## Deliverables (one commit per item, author Dmitry Fomin <fomin.list@gmail.com>, Discussion trailer)

### D1 — doc/src/sgml/pgwaiteventtracing.sgml (register in contrib.sgml and filelist.sgml)
Port the substance of v6's docs (`git show wet-series:doc/src/sgml/monitoring.sgml`,
`git show wet-series:doc/src/sgml/config.sgml`, the wait_event_timing
sections) and rewrite them for a contrib module, following the structure of
doc/src/sgml/pgstatstatements.sgml. Sections, in order:
1. What it is: exact per-backend wait statistics (count, total, max,
   32-bucket histogram) and an optional ordered trace of completed waits
   with query markers; three levels off / stats / trace.
2. Loading: shared_preload_libraries, CREATE EXTENSION, the three GUCs with
   type, default, context and meaning.
3. Views and functions: reference tables generated from the script (every
   column with type and one-line meaning); grants (pg_read_all_stats).
4. Statistics semantics: per live backend only (gone when the backend exits
   or disables capture); a session's statistics do NOT include the waits of
   its parallel workers — each worker is a separate backend with its own
   rows (backend_type "parallel worker") that disappear when the worker
   exits, so summing a parallel query's waits needs capture in the workers
   and reading them while they run, or the trace level; histogram bucket
   boundaries, the overflow view and
   what lands there (capacity per class, LWLock tranche limit), which call
   sites are timed (the converted pgstat_report_wait_start_timed sites; an
   extension's own hand-annotated waits only after it opts in), and
   auxiliary-process behaviour as decided in plan §4.2a (the reviewer will
   tell you which option was chosen).
5. Memory: control table per ProcNumber, payload per collecting backend
   (exact bytes from wp2-report.md), ring size per tracing backend; nothing
   proportional to max_connections unless backends enable capture.
6. Resetting and permissions: own reset; cross-backend reset rules (same as
   pg_signal_backend(): superuser-owned or role-less targets need
   superuser; otherwise same role or pg_signal_backend; auxiliary PIDs
   rejected); reset is asynchronous (applied at the target's next wait);
   _all() superuser-only.
7. Trace ring and post-mortem reading: state the orphan contract BEFORE any
   example (ring survives an orderly backend exit until the ProcNumber is
   reused by a tracing backend or the sweep function runs; not after a
   crash restart); the sweep function.
8. **Query markers and statement attribution** — the owner's priority; be
   exhaustive. The marker table of plan §5.3 (source, meaning). Worked
   timelines as literal record sequences for: autocommit statement;
   explicit transaction with two statements and client think time;
   multi-statement simple-query string; pipelined extended-protocol batch;
   COPY FROM STDIN; an error mid-statement; a SQL function calling another
   statement (nesting depth). The attribution rule exactly as implemented in
   `pg_wait_event_trace_by_statement()`, with its `<idle>` and
   `<unattributed>` rows. A "Limitations" list: no end-of-message marker for
   the pipelined protocol (the next QueryStart bounds a queued statement);
   waits of parallel workers are recorded under the worker, not the
   leader's statement; waits before the first marker in the ring are
   unattributed; QueryStart carries queryId 0 unless compute_query_id is on
   (or another module enabled it); ClientRead inside COPY FROM STDIN
   belongs to the COPY.
9. Reading from other extensions: SQL functions only; shared structures are
   private (this is fix 7 — do not describe any direct shared-memory reader).
10. Overhead: leave a clearly marked placeholder paragraph
    `<!-- COST NUMBERS: filled in after the bare-metal re-measurement -->`
    with the structure (off / module loaded / stats / trace) but no numbers.

### D2 — doc/src/sgml/xfunc.sgml
In the custom wait events section: one paragraph explaining
pgstat_report_wait_start_timed()/pgstat_report_wait_end_timed() (what they
add over the ordinary pair: the begin/end hooks for timing consumers such as
pg_wait_event_tracing; same cost as the ordinary pair when no consumer is
loaded), and switch the example to the timed pair with a
`#if PG_VERSION_NUM >= 200000` guard falling back to the ordinary pair.

### D3 — removal audit (report only, no commit unless something is found)
Against the branch base (`wet-v7-rfc`), list every path the module commits
touch; it must be only contrib/pg_wait_event_tracing/**, contrib/Makefile,
contrib/meson.build, src/test/modules/test_wait_hook/**,
src/test/modules/{Makefile,meson.build}, doc/src/sgml/{pgwaiteventtracing,
contrib,filelist,xfunc}.sgml. Grep the whole branch for leftovers of v6:
`wait_event_capture` (unprefixed), `wait_event_timing_max_tranches`,
`wait_event_trace_ring_size`, `--enable-wait-event-timing`,
`USE_WAIT_EVENT_TIMING`, `wait_event_timing.h`, `WaitEventTraceCtl`,
`pgstat_set_wait_event_timing_storage`, OIDs 9956–9962 — each must have zero
hits outside the module's own internal names. Also grep the demo scripts
under <workspace>/blog/demo/run_demo*.sh for the
old GUC names and list the lines that need updating (do not edit them).

## Report
<workspace>/wp5-report.md: commit hashes, docs
build/validation result (or why it could not run), the audit result, and
any place where the code and the plan disagreed (the docs follow the code;
list the disagreement for the reviewer). Final message <= 8 lines.
