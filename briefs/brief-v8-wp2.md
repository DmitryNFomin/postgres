# WP2: contrib/pg_wait_event_tracing, statistics level (v8-0004)

Plan: <workspace>/IMPLEMENTATION-PLAN-v8.md — read
§0, §1, §2, §3.1, §4, §7 before starting. This WP delivers the statistics level
only; the trace level (ring, markers, trace SRFs) is WP3.

Repo: <workspace>/postgres. The user's checkout is
on `wet-series`; DO NOT change it. Work in a new worktree:

    git -C <workspace>/postgres worktree add \
        <workspace>/wt-v8-wp2 -b wet-v8-wp2 wet-v7-rfc

Meson/ninja: <workspace>/.venv-v7-rfc/bin first on
PATH. Build dir <workspace>/build-v8-wp2, options
`--buildtype=debugoptimized -Dcassert=true -Dwerror=true -Dinjection_points=true`.
Another agent builds in parallel on this 4-core host: use `ninja -j2`. Disk is
tight (12 GB free): one build dir only.

## Inputs
1. The base branch already has the hook: read src/include/utils/wait_event.h
   on `wet-v7-rfc` (hook pointers, depth guard, timed pair, contract comment).
2. The package collector (start from THIS, not from v6):
       P=<scratch>/v7rev/wait-hook-peer-review-20260909/source/patches/04-timed-site-hook-vs-tested-master.patch
       git apply --include='contrib/pg_wait_event_timing/*' --include='contrib/Makefile' --include='contrib/meson.build' "$P"
   then `git mv contrib/pg_wait_event_timing contrib/pg_wait_event_tracing` and
   rename files/identifiers accordingly (library, control file, SQL script,
   GUC prefix). Keep the internal C prefix `pwet_`.
3. The v6 reference for features and SQL column sets (read-only):
       git show wet-series:src/backend/utils/activity/wait_event_timing.c
       git show wet-series:src/include/utils/wait_event_timing.h
       git show wet-series:src/backend/catalog/system_views.sql   (the four wait_event views near the end)
       git diff wet-series-base wet-series -- src/include/catalog/pg_proc.dat
       git show wet-series:src/test/regress/sql/wait_event_timing.sql
       git show wet-series:src/test/regress/expected/wait_event_timing.out
   NEVER copy v6's core wiring (proc.c/postinit.c calls, GUC table rows,
   pg_proc OIDs, generator changes, configure flag).
4. Defect analysis with required corrections/tests: the "V6-1".."V6-5"
   sections of <scratch>/v7rev/wait-hook-peer-review-20260909/docs/SOURCE-REVIEW-v6-v7.md

## Milestones = commits on wet-v8-wp2 (one commit each, so they can be reviewed)

### M1 — port and rename; stats level builds and works
- Module `contrib/pg_wait_event_tracing/`: Makefile, meson.build (registered in
  contrib/Makefile and contrib/meson.build), `pg_wait_event_tracing.control`,
  `pg_wait_event_tracing--1.0.sql`, `pg_wait_event_tracing.c` (+ a header if
  you split files), `pg_wait_event_tracing_data.h` (the static class table).
- `_PG_init`: require `process_shared_preload_libraries_in_progress` (error
  otherwise, like pg_stat_statements); define GUCs
  `pg_wait_event_tracing.capture` (enum: off|stats for this WP; PGC_SUSET,
  assign hook) and `pg_wait_event_tracing.max_tranches` (PGC_POSTMASTER,
  default 192); install the begin/end hooks with chaining per the contract;
  register `before_shmem_exit` release.
- Shared memory: control segment via `GetNamedDSMSegment("pg_wait_event_tracing", ...)`
  with init callback; DSA via `GetNamedDSA`. If the package collector already
  does this, keep it; remove its "legacy" shmem_request fallback if it is dead
  code on this master.
- STRIP the trace-level pieces out of this WP (ring writer, trace DSA, trace
  SRF, query markers/hooks for markers, trace_ring_size GUC, the 32-byte
  record). Save the removed code verbatim to
  <workspace>/wp3-trace-parts-from-package.c.txt
  for WP3. Keep the slot fields reserved for the ring (trace_ptr,
  trace_state) so WP3 does not change the layout.
- SQL surface (names and COLUMN SETS exactly as v6, so docs/blog/demos stay
  valid): views `pg_stat_wait_event_timing`, `pg_stat_wait_event_timing_overflow`,
  `pg_wait_event_timing_histogram_buckets`; functions
  `pg_stat_get_wait_event_timing()`, `pg_stat_get_wait_event_timing_overflow()`,
  `pg_stat_reset_wait_event_timing(pid int)`, `pg_stat_reset_wait_event_timing_all()`.
  GRANT SELECT on the views to pg_read_all_stats as v6 did (check v6's grants).
  Drop the package's `_state` SRF unless v6 had an equivalent.
- Build (werror), `ninja headerscheck`; manual smoke: preload, CREATE EXTENSION,
  SET pg_wait_event_tracing.capture = stats, pg_sleep(0.05), SELECT from the
  timing view shows a PgSleep row with count 1 and a total near 50 ms.

### M2 — the four fixes of this level (see plan §4.1–4.4; SOURCE-REVIEW V6-1/2/4/5)
- Fix 1 (sparse): verify no code path allocates for more than the calling
  backend; the control table is per-ProcNumber small entries only; payload
  allocated only at safe points (assign hook when IsNormalProcessingMode(),
  else post_parse_analyze/ExecutorStart), never inside the begin/end hooks.
- Fix 2 (ownership): slot gets owner_pid + owner_start (MyStartTimestamp) and
  a generation; readers compare against
  pgstat_get_beentry_by_proc_number(procno)->st_procpid / st_proc_start_timestamp
  and skip on mismatch; release clears owner + bumps generation + dsa_free.
- Fix 4 (reset ACL): `pg_stat_reset_wait_event_timing(pid)` replicates the
  checks of pg_signal_backend() in src/backend/storage/ipc/signalfuncs.c:
  target must be a normal backend (auxiliary PIDs rejected with the same
  wording as there); superuser-owned or role-less target needs superuser;
  else has_privs_of_role(GetUserId(), target role) or ROLE_PG_SIGNAL_BACKEND.
  `_all()` superuser-only (or pg_read_all_stats? no: superuser).
- Fix 5 (reset race): resolve pid→ProcNumber with BackendPidGetProc, then
  take the control lock, re-check owner_pid (and owner_start vs beentry),
  bump reset_generation, release; owner consumes at its next wait_end.
  Add an injection point `pg-wait-event-tracing-reset-before-publish`
  between resolution and taking the lock (guarded by USE_INJECTION_POINTS)
  for WP4's TAP test.
- Capacity table (§3.1): check today's counts per class in
  src/backend/utils/activity/wait_event_names.txt; ensure headroom ≥ 8; add
  SRF `pg_wait_event_tracing_capacity()` returning (type text, capacity int);
  `_PG_init` WARNING if any class already exceeds capacity.

### M3 — regression tests (contrib sql/expected, run by `meson test --suite pg_wait_event_tracing`)
Port v6's regress test and adapt: enable/disable, timing view rows, histogram
buckets view, overflow view, reset self, reset ACL denial for a
non-superuser against a superuser target (use two roles; expected error),
and the capacity check:
    SELECT type, count(*) FROM pg_wait_events GROUP BY type   vs capacity SRF
    → assert count <= capacity - 4 for every class present in the table.
The contrib test harness must preload the library: set
`shared_preload_libraries` in the module's Makefile/meson (see
contrib/pg_stat_statements for the `regress_args`/`--temp-config` pattern).

## Validate before finishing (HARD RULE: never start a PostgreSQL server on this host)
- werror build (-j2), headerscheck, cpluspluscheck. Do NOT run `meson test`,
  pg_regress, TAP, initdb, pg_ctl, or the M1 "manual smoke" server. Author
  expected/ output by reasoning; tests run on the fork's CI.
- `git diff --stat wet-v7-rfc` touches only contrib/**.
- Three commits on wet-v8-wp2 (M1, M2, M3), author Dmitry Fomin
  <fomin.list@gmail.com>, each with the trailer
  `Discussion: https://postgr.es/m/CAPHG-0mAOn05ae6Kqx1wHXxzOk4E5W7ajjd=QBhgkR7a0uyQmw@mail.gmail.com`.
  Do not push. Do not touch other branches or worktrees.

## Deliverable
<workspace>/wp2-report.md: per milestone the commit
hash, diffstat, what was kept/removed from the package collector, how each of
fixes 1/2/4/5 is implemented (function names), test results, open questions.
Final message <= 10 lines.
