# WP2b: server-side processes collect from process start (option A)

Plan: <workspace>/IMPLEMENTATION-PLAN-v8.md — read
§4.1, §4.2, and **§4.2a in full** (the decided design) before starting. Also
read <workspace>/wp2-report.md.

## HARD RULES
- NEVER start a PostgreSQL server on this host: no meson test, prove,
  pg_regress, initdb, pg_ctl. Compile only: meson setup, `ninja -j2`,
  headerscheck, cpluspluscheck, `perl -c` on TAP files. Tests run on the
  fork's CI (the reviewer pushes).
- User's checkout stays on `wet-series`. Do not push. Do not touch other
  branches or worktrees (another agent is writing TAP tests on wet-v8-wp4a).
- **Hook rules:** inside the wait begin/end hooks: no allocation, no lock
  (LWLock or spinlock), no wait, no ereport/elog. Plain loads/stores,
  pg_atomic_*, pg_read_barrier/pg_write_barrier, memset/memcpy on already
  mapped memory are allowed.

## Setup
    git -C <workspace>/postgres worktree add \
        <workspace>/wt-v8-wp2b -b wet-v8-wp2b wet-v8-wp2
Build dir <workspace>/build-v8-wp2b,
`--buildtype=debugoptimized -Dcassert=true -Dwerror=true -Dinjection_points=true`,
meson/ninja from <workspace>/.venv-v7-rfc/bin.

## Why this exists (short)
Server-side processes never reach the module's attach points (they do not
parse queries or start the executor through the hook), so as of wet-v8-wp2
they never collect until a configuration reload: checkpointer, WAL writer,
background writer, startup/recovery, WAL receiver, archiver, WAL summarizer,
I/O workers, autovacuum launcher and workers, slotsync worker, background
workers (incl. logical replication workers), WAL senders. v6 covered them.

## M1 — move the control table to fixed shared memory (design correction)
A server-side process must reach its control slot from inside the hook,
where it cannot attach anything. So the control table moves from
`GetNamedDSMSegment()` back to fixed shared memory, always mapped in every
process (the module already requires shared_preload_libraries):
- `_PG_init`: install `shmem_request_hook` and `shmem_startup_hook`
  (chain previous hooks; keep the "must be in shared_preload_libraries"
  error).
- request: `RequestAddinShmemSpace(control size)` and one LWLock via
  `RequestNamedLWLockTranche("pg_wait_event_tracing", 1)`.
- startup: `ShmemInitStruct("pg_wait_event_tracing control", …)` under
  AddinShmemInitLock, init slots on !found; `GetNamedLWLockTranche(...)`.
  Verify that under EXEC_BACKEND the startup hook runs again in every child
  and re-sets the static pointers (look at how pg_stat_statements does it).
- Remove pwet_ensure_control()'s DSM-registry path; keep GetNamedDSA for
  the client-backend payloads exactly as now (attach only at safe points).
- Confirm MaxBackends and the GUC values are final when shmem_request_hook
  runs (postmaster.c order: config, preload, InitializeMaxBackends?,
  shmem requests) and write the finding in a comment.
Commit M1 alone: behaviour unchanged, only where the control table lives.

## M2 — reserved region + claim protocol (plan §4.2a)
- Range R = [MaxConnections, MaxBackends + 6 + io_max_workers): compute
  the exact expression from proc.c (the "6" is NUM_AUXILIARY_PROCS -
  MAX_IO_WORKERS; use that expression, not a literal). Clamp the upper end to
  MaxBackends + NUM_AUXILIARY_PROCS.
- In shmem_request_hook: if `pwet_capture != PWET_CAPTURE_OFF` at that
  moment, also request |R| × stride for the region; in the startup hook
  `ShmemInitStruct("pg_wait_event_tracing server processes", …)`, zeroed on
  creation; remember R and the region pointer in statics (and re-derive them
  in EXEC_BACKEND children).
- Claim in the begin hook, per plan §4.2a steps (1)–(4); per-process static
  flag; the slot's owner_pid/owner_start/reset_generation live in the fixed
  control table (now always mapped). Zero the payload only when the previous
  owner token differs.
- wait_end for a claimed fixed slot: identical recording code; reset via
  the control slot's reset_generation, as for DSA slots.
- Readers (`pg_stat_get_wait_event_timing`, `_overflow`): for a ProcNumber
  in R with the region present, lockless read with the double-read owner
  check of §4.2a; otherwise the existing locked DSA read.
- pwet_maybe_attach(): never uses the DSA path for a ProcNumber in R while
  the region exists. When the region does NOT exist (capture was off at
  start), server-side processes keep today's behaviour: DSA attach in the
  assign hook at the next configuration reload.
- Assign hook in a server-side process with a claimed fixed slot: capture
  → off stops writing (pwet_my_stats = NULL, owner_pid = 0, claimed flag
  false); → on re-claims at the next begin hook.
- The trace level is out of scope here (WP3); leave a comment that server
  processes trace only after a reload.

## M3 — TAP test t/006_server_processes.pl
(Wire it into the module's meson `'tap'` list and Makefile TAP_TESTS; if the
list does not exist on this branch, create it the way wp4a's brief
describes.) Cases, per plan §4.2a "Tests":
1. Node with `pg_wait_event_tracing.capture = stats` and
   `shared_preload_libraries` in postgresql.conf, started, NO reload: after
   a checkpoint (`CHECKPOINT`) and a little write activity, rows exist in
   `pg_stat_wait_event_timing` for backend_type checkpointer, walwriter,
   background writer, and (io_method = worker is the default) io worker.
2. `pg_shmem_allocations` has "pg_wait_event_tracing server processes" with
   size >= |R| × stride (compute |R| from the node's settings in SQL).
3. A standby created from a base backup of that node (same config) shows
   rows for backend_type startup (recovery waits) without any reload.
4. Second node with capture off at start: no region in pg_shmem_allocations;
   `ALTER SYSTEM SET pg_wait_event_tracing.capture = stats; SELECT
   pg_reload_conf();` then after some activity rows appear for checkpointer.
Use poll_query_until for anything timing-dependent.

## Deliver
Three commits on wet-v8-wp2b (M1, M2, M3), author Dmitry Fomin
<fomin.list@gmail.com>, Discussion trailer as in the other commits.
Report <workspace>/wp2b-report.md: commit hashes,
the exact R expression and the reserved bytes for default settings, how the
claim and the lockless read are ordered (barriers), what you verified in
postmaster.c/proc.c, open questions. Final message <= 10 lines.
