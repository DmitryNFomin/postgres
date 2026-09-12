# v8 implementation plan: contrib collector, seven fixes, all wiring

Written 2026-09-09, decisions applied 2026-09-09 evening. Supersedes Work
Packages A and C of IMPLEMENTATION-PLAN-v7.md (WP B, the hook, and WP D, the
benchmark, are DONE and posted as v7-0001/0002 on 2026-09-09). Target: post the
full series as **v8** on the thread before the September commitfest closes
(2026-09-30), fallback before the November one.

## 0. Starting material (FACT)

| Source | What it gives us |
|---|---|
| Branch `wet-v7-rfc` (tip dc8f6dfafd3, on master 412ef97d925c) | 0001 hook + 0002 conversions, CI green, posted |
| Package patch 04 `contrib/pg_wait_event_timing/` (1,151 C + 55 SQL + 56 data.h + build files) | A hook-shaped collector that already has: sparse per-backend control table, DSA attach/release at safe points, `before_shmem_exit` release, begin/end hooks with chaining, LWLock tranche hash, 32-bucket histograms, overflow counters, trace ring writer, query markers via `post_parse_analyze_hook` / `ExecutorStart_hook` / `ExecutorEnd_hook`, SRFs `pg_wait_event_timing_stats/_overflow/_trace/_state/_reset`, custom GUCs `capture` and `trace_ring_size`, shmem request/init/attach with a legacy fallback |
| Package `src/test/modules/test_wait_hook/` (143 C + 37 SQL) | A second hook consumer used for chaining-order smoke tests |
| Branch `wet-series` (v6, 3,109-line recorder) | The full-feature reference: histogram-buckets view, per-PID reset + reset-all, orphan-ring sweep function, seqlock trace reader with identity check, docs (783 + 106 sgml lines), regress test (113 sql / 188 expected), TAP seqlock test (122 lines, injection points) |
| `docs/SOURCE-REVIEW-v6-v7.md` §V6-1..V6-7 and §13 | Required correction + required test per defect; the correctness test matrix |
| Master APIs available (checked 2026-09-09) | `GetNamedDSMSegment`, `GetNamedDSA`, `GetNamedDSHash` (dsm_registry.h); `shmem_request_hook`, `shmem_startup_hook`; `post_parse_analyze_hook`, `ExecutorStart/End_hook`, `ProcessUtility_hook`; `RegisterXactCallback` |

Strategy that follows from this: **start from the package collector, not from v6.**
Port the missing v6 features into it, apply the seven fixes, then add docs and
tests. Do not port v6's core lifecycle calls, generator output, configure flag,
catalog OIDs or GUC table entries: all of that disappears.

## 1. Series layout for v8 (DECIDED 2026-09-09)

```
v8-0001  Add begin/end hooks for timed wait events            (posted as v7-0001, unchanged unless reviewed)
v8-0002  Convert wait_start/end call sites to the timed pair  (posted as v7-0002, unchanged unless reviewed)
v8-0003  test_wait_hook: test module for the hook contract    (chaining order, depth guard, NULL, error path)
v8-0004  contrib/pg_wait_event_tracing: statistics level      (module, GUCs, sparse slots, ownership, reset+ACL, views, regress tests, docs)
v8-0005  contrib/pg_wait_event_tracing: trace level           (rings, seqlock reader, orphan lifecycle, markers, trace SRFs, TAP tests, docs)
cover letter: what changed since v6/v7, the seven fixes, re-measured enabled numbers, link to package, the rename
```

Core footprint = 0001 + 0002 only (0003 is a test module). Reviewers can stop
after 0003 and have a complete, tested core change; 0004 is usable alone.

## 2. Names (DECIDED 2026-09-09)

| Thing | v6 | v8 |
|---|---|---|
| Library / extension | core | **`pg_wait_event_tracing`** (`shared_preload_libraries` + `CREATE EXTENSION`, the pg_stat_statements model). The mail of 2026-09-09 said `pg_wait_event_timing`; the cover letter notes the rename ("covers both the statistics and the trace level"). |
| GUCs | `wait_event_capture`, `wait_event_timing_max_tranches`, `wait_event_trace_ring_size` | `pg_wait_event_tracing.capture` (off/stats/trace, PGC_SUSET), `pg_wait_event_tracing.max_tranches` (PGC_POSTMASTER), `pg_wait_event_tracing.trace_ring_size` (PGC_POSTMASTER) |
| Views | `pg_stat_wait_event_timing`, `pg_stat_wait_event_timing_overflow`, `pg_backend_wait_event_trace`, `pg_wait_event_timing_histogram_buckets` | same names, created by the extension script |
| Functions | `pg_stat_get_wait_event_timing`, `..._overflow`, `pg_get_wait_event_trace`, `pg_get_backend_wait_event_trace`, `pg_stat_reset_wait_event_timing(pid)`, `..._all()`, `pg_stat_clear_orphaned_wait_event_rings()` | same names; internal C symbols prefixed `pwet_`; `pg_stat_reset_wait_event_timing(pid)` keeps its signature |
| Grants | `pg_read_all_stats` on views | same, in the script |
| Docs page | monitoring.sgml sections | `doc/src/sgml/pgwaiteventtracing.sgml` |

Keeping the SQL names preserves the docs, the blog post and the demo scripts.

## 3. Core wiring beyond 0001/0002: none (DECIDED)

### 3.1 Per-class event counts — hardcoded table with an enforcing test

The module maps `wait_event_info` (class byte + 16-bit id) to a dense slot
using a static table of per-class capacities (Lock 16, Buffer 16, Activity 32,
Client 16, Extension 128, IPC 64, Timeout 16, IO 128, InjectionPoint 128 — the
package's `pg_wait_event_timing_data.h`, checked against today's
`wait_event_names.txt` at implementation time and given headroom of at least 8
per class). Events beyond capacity go to the overflow counter (counted, not
timed). **Enforcement:** a module regression test compares each class's
capacity with `SELECT count(*) FROM pg_wait_events GROUP BY type`, and fails
when any class is within 4 of its capacity, so whoever adds events past the
headroom must bump the table in the same commit. (REVISED 2026-09-11: the
`_PG_init` WARNING probe is dropped. Verified on master it cannot work:
built-in name lookups return "unknown wait event", never NULL, so it would
always warn; and for Extension/InjectionPoint the lookup takes
WaitEventCustomLock and searches a shared hash, which does not exist in the
postmaster during shared_preload_libraries, so it would crash startup.)
Capacities after WP2 review: Lock 32, Buffer 16, Activity 32, Client 32,
Extension 128, IPC 128, Timeout 32, IO 128, InjectionPoint 32 (was 128) —
counts in use on master 2026-09-11: Lock 12, Buffer 4, Activity 18,
Client 9, IPC 64, Timeout 11, IO 83.

### 3.2 Hook contract tests (0003)

`src/test/modules/test_wait_hook` from the package, extended: NULL hook; one
consumer records begin/end for a timed wait; two consumers chained A then B
produce A.begin, B.begin, B.end, A.end; a wait attempted inside a hook is not
re-entered (depth guard); begin followed by ERROR without end leaves the next
pair consistent (the cleanup end from AbortTransaction fires). Regression
(sql/expected) tests; this is the test for 0001.

### 3.3 Everything else uses existing core mechanisms

Markers: existing hooks plus `RegisterXactCallback` (§5.3). Shared memory: DSM
registry (§4.1). Reset authorization: `pg_signal_backend()` rules replicated in
the module (§4.3). The v6 marker in `postgres.c` for the pipelined protocol is
dropped and documented (§5.3). Auxiliary processes get the hooks through
`shared_preload_libraries` like any backend.

## 4. Statistics level (0004): design and the fixes it carries

### 4.1 Shared memory layout

```
control segment  (GetNamedDSMSegment("pg_wait_event_tracing", size, init_cb))
  LWLock            lock                     (tranche id from LWLockNewTrancheId at init, name registered)
  dsa_handle        stats_dsa / trace_dsa    (GetNamedDSA, created at first need under lock)
  PwetSlot          slots[MaxBackends + NUM_AUXILIARY_PROCS]
      dsa_pointer   stats_ptr                InvalidDsaPointer when not collecting
      dsa_pointer   trace_ptr                InvalidDsaPointer when no ring
      uint8         trace_state              FREE / ACTIVE / ORPHANED
      int           owner_pid                0 when unowned
      TimestampTz   owner_start              MyStartTimestamp of the owner
      pg_atomic_uint32 generation            bumped on every ownership change
      pg_atomic_uint32 reset_generation      reset requests (see 4.4)
```

Per-slot cost ~64 bytes: 64 KiB at 1000 slots. The 203 KiB stats payload and
the ring exist only for backends that enabled capture. **Fix 1.**

### 4.2 Attach, never inside the hook

Attach (allocate payload, publish pointer + owner token under the lock) runs
only at safe points: the GUC assign hook when `IsNormalProcessingMode()`, else
the module's `post_parse_analyze_hook` / `ExecutorStart_hook` (what the package
collector does today). The begin/end hooks test `my_stats != NULL` and return.
Release on `capture = off` and in `before_shmem_exit`: under the lock, clear
owner, bump generation, `dsa_free`. Stats are live-only (documented in v6),
so nothing is retained after exit. **Fix 2:** every reader compares
`slots[i].owner_pid/owner_start` with the backend-status entry for that
ProcNumber and skips the slot on mismatch; a successor with capture off never
has a payload to be shown anyway.

### 4.2a Server-side processes — DECIDED 2026-09-11: option A, widened

**Decision (owner, 2026-09-11): A.** While specifying it, the gap turned out
wider than the auxiliary processes: autovacuum workers (autovacuum.c calls
vacuum directly: no parse analysis, no executor hook), physical WAL senders
(replication grammar only) and logical replication apply workers (worker.c
has no ExecutorStart/parse/SPI path) never reach an attach point either.
So A covers **every server-side process slot**.

Layout (verified, proc.c on master): ProcNumbers are assigned by type in one
array — [0, MaxConnections) client backends; then autovacuum launcher and
workers plus the special workers (autovacuum_worker_slots +
NUM_SPECIAL_WORKER_PROCS); then background workers, which include parallel
query workers and logical replication workers (max_worker_processes); then
WAL senders (max_wal_senders), ending at MaxBackends; then auxiliary
processes at [MaxBackends, MaxBackends + NUM_AUXILIARY_PROCS), each taking the
first free auxiliary slot (InitAuxiliaryProcess), so with at most
6 + io_max_workers concurrent auxiliary processes their slots stay below
MaxBackends + 6 + io_max_workers. Server-side slots are therefore the single
contiguous range **R = [MaxConnections, MaxBackends + 6 + io_max_workers)**.

Mechanism:
- Only if `pg_wait_event_tracing.capture` is non-off in the configuration at
  postmaster start (known in `shmem_request_hook`: postmaster.c loads the
  configuration, then preload libraries, then shared-memory requests —
  verified). Otherwise nothing is reserved and server-side processes attach
  through the DSA path at the first configuration reload after capture is
  enabled (the enum assign hook runs on every reload even for unchanged
  values — verified in set_config_with_handle).
- `shmem_request_hook`: `RequestAddinShmemSpace(|R| × stride)`;
  `shmem_startup_hook`: `ShmemInitStruct("pg_wait_event_tracing server
  processes", …)`, zeroed at creation; under EXEC_BACKEND the startup hook
  re-attaches in every child. The payload stride is the same as the DSA
  payload.
- **Claim protocol inside the begin hook** (obeys the hook rules: no
  allocation, no lock, no wait, no error). A process whose MyProcNumber is
  in R, with the region present and capture on, claims its slot on its first
  begin-hook call (per-process static flag): (1) `owner_pid = 0`, write
  barrier; (2) zero the payload if the slot's previous owner token is not
  this process (one-time memset of ~206 KiB); (3) `owner_start =
  MyStartTimestamp`, write barrier, `owner_pid = MyProcPid`; (4) set the
  cached `pwet_my_stats`. No exit callback is needed: a dead owner is
  filtered by the readers' beentry check, and the next process on that slot
  re-claims it.
- Readers of fixed slots do not take the control lock (the payload is never
  freed): read `owner_pid`/`owner_start`, read barrier, copy the payload,
  read barrier, re-read both; skip the slot unless unchanged and equal to
  the beentry. DSA slots keep the existing locked read.
- Disabling capture by reload in a server-side process (assign hook, safe
  point) stops writing and clears `owner_pid`; re-enabling re-claims at the
  next begin hook.
- The DSA path is never used for a ProcNumber in R while the region exists.
- Trace level is not covered by the reservation (a ring per server-side
  process would cost MiBs each): server-side processes start tracing at the
  first configuration reload with capture = trace, via the DSA path.
  Documented.

Memory, reserved only when capture is on at start:
|R| × stride, where |R| = autovacuum_worker_slots + NUM_SPECIAL_WORKER_PROCS
+ max_worker_processes + max_wal_senders + 6 + io_max_workers, and stride =
212,664 bytes at max_tranches = 192 (after WP2 review). With master defaults
(16 + 2 + 8 + 10 + 6 + 8 = 50 slots): **≈ 10.1 MiB**. Earlier estimates to
the owner (2–8 MiB, auxiliary processes only) were before the widening; the
owner was told. If io_max_workers is raised after start, the extra I/O
workers are covered from the next reload (documented).

Implementation: WP2b (brief-v8-wp2b.md), which also owns the TAP test
t/006_server_processes.pl and moves the control table to fixed shared
memory (needed so a claiming process can reach its slot inside the hook).

Tests (TAP, in WP4a as t/006_server_processes.pl): start a node with
capture = stats in postgresql.conf, no reload; assert rows for
checkpointer, walwriter, background writer and an I/O worker; assert
`pg_shmem_allocations` shows the named region with the expected size; a
standby node started from a base backup shows startup-process recovery
waits without any reload; with capture off at start, no region exists and a
reload after `ALTER SYSTEM SET … = stats` makes server-side rows appear.

#### Original gap analysis (kept for the record)

Attach points are the settings assign hook (when `IsNormalProcessingMode()`)
and `post_parse_analyze` / `ExecutorStart`. Auxiliary processes (checkpointer,
WAL writer, background writer, startup/recovery, WAL receiver, archiver, WAL
summarizer, and the up-to-32 I/O workers that perform reads under the default
`io_method = worker`) never parse a query, and in a forked child the assign
hook does not run at start. Verified on master: a configuration reload runs
the enum assign hook for every file setting even when unchanged
(`set_config_with_handle`, `if (changeVal) ... assign_hook`), and auxiliary
processes are in NormalProcessing in their main loop, so **they attach at the
first reload after start; they miss everything between server start and that
reload** — including crash-recovery waits in the startup process and I/O
worker reads right after start. v6 covered them from process start.
NUM_AUXILIARY_PROCS = 6 + MAX_IO_WORKERS = 38.

Options (no core change in A–C):
- A (recommended): when capture is non-off in the configuration at server
  start (known in `shmem_request_hook`), reserve fixed shared memory for the
  38 auxiliary slots (~38 × ~206 KiB ≈ 7.6 MiB, bounded, independent of
  max_connections, zero when capture is off at start); an auxiliary process's
  hooks use its preassigned payload directly (address arithmetic from
  MyProcNumber, no allocation). Enabling later still works through the reload
  path (DSA attach in the assign hook). Cost: a second storage path; readers
  handle both.
- B: a module background worker that, when capture is configured on at start,
  triggers one configuration reload; every process re-runs the assign hook and
  auxiliary processes attach via DSA. One storage path, ~40 lines, one extra
  "reloading configuration files" log line; still misses the first seconds
  (crash recovery). Reviewers may see it as a trick.
- C: document only ("auxiliary processes collect from the first configuration
  reload after capture is enabled; run SELECT pg_reload_conf() after start").
- D: a small core hook at the end of auxiliary process initialisation —
  reopens the core patch; not recommended.

### 4.2b Findings from CI and review, 2026-09-11

- **Assign hook sees the old value.** guc.c calls the enum assign hook
  before storing the new value (`assign_hook(newval, …); *conf->variable =
  newval;`), so an attach decision inside the assign hook that tests the
  stored variable sees "off" when capture is being turned on. Client
  backends hide it (they attach at the next post_parse_analyze); server-side
  processes would never attach after a reload. Fixed in WP2b by deciding on
  the incoming value.
- **Parallel workers are separate backends.** Their waits go to their own
  rows (and, at trace level, their own ring); a session's statistics do not
  include its workers' waits. Every regression/TAP test that expects a
  session's own wait rows sets `debug_parallel_query = off`, because the
  macOS CI job forces `regress`. Documented in the docs page (WP5 §4).

- **ProcNumber reuse is FIFO.** proc.c on master: InitProcess takes the
  head of the free list, ProcKill returns to the tail, so a freed
  ProcNumber is reused only after every other free slot. Reuse tests use a
  small max_connections and loop until a session lands on the target slot,
  reading a session's own ProcNumber with `pg_stat_get_backend_idset()` +
  `pg_stat_get_backend_pid()` (both ProcNumber-based on master).
- **Capture is PGC_SUSET.** Tests that enable it from non-superuser roles
  `GRANT SET ON PARAMETER pg_wait_event_tracing.capture` first.
  (Both found by CI run 34614510950 of WP4a.)
- **The postmaster runs the wait hooks too, and fork() children inherit
  whatever it cached.** postmaster.c's ServerLoop waits through
  WaitEventSetWait(), which calls the timed pair; the module's hooks are
  installed in _PG_init() in the postmaster. WP2b cached "is this process
  eligible for a fixed slot" on the first begin hook; the postmaster computed
  it with MyProcNumber = INVALID_PROC_NUMBER ("no"), so every forked child on
  Linux/macOS inherited "already checked: no" and never claimed its slot,
  while EXEC_BACKEND children on Windows started fresh and passed (CI run
  34640640603: t/006 9/9 on MinGW and MSVC, 4–5 failures on Linux/macOS).
  Rule for all module code: a per-process static written in a hook, assign
  hook or marker hook must either never be written while MyProc is NULL, or
  be keyed to MyProcPid so a child recomputes it. A Windows-passes /
  Linux-fails split is the signature of this class of bug.
- **Two eligibility questions, not one (WP3 review, 2026-09-12).**
  `pwet_can_attach()` answers "may I take the DSA stats path?", and WP2b made
  it refuse every ProcNumber in the reserved region. WP3 then gated trace
  attach behind the same helper, so a process with a reserved slot could
  never attach a ring at all, contradicting §4.2a/§5.2 ("server-side
  processes trace from the first reload"). Split it: a safe-point test used
  by both paths, a DSA-path test on top of it, and a trace test that needs
  only the safe point plus an established ProcNumber. Register the exit
  callback before attaching a ring on any path, or a server-side process's
  ring stays ACTIVE with a dead owner.
- **Do not emit rows under the control lock (WP3 review).** The trace
  reader buffered validated records so the lock could be dropped before
  `tuplestore_putvalues()`, but the caller held LW_SHARED across the whole
  call: up to 131072 rows per slot, possibly spilling to disk, while
  attach/release/reset/sweep wait for the exclusive lock.
- **One pg_sleep() is not one wait.** pg_sleep loops on WaitLatch until its
  own timestamp clock says the time is up; on Windows the latch timeout and
  that clock disagree, so one pg_sleep(0.01) was recorded as 2 waits (CI run
  34627876249). The module is right to count each wait; tests assert
  presence, relative change, reset_count or specific events, never exact
  PgSleep counts. Checks expecting an ERROR use one-shot psql, because a
  BackgroundPsql session dies on the first error (on_error_stop).

### 4.3 Reset authorization (fix 4)

`pg_stat_reset_wait_event_timing(pid)`: target must be a normal backend with a
backend-status entry (auxiliary PIDs rejected); superuser-owned or role-less
targets require superuser; otherwise same role or `pg_signal_backend`
membership. Same predicate as `pg_signal_backend()` in signalfuncs.c,
replicated as a static helper. `_all()` stays superuser-only.

### 4.4 Reset race (fix 5)

Resolve pid → ProcNumber via `BackendPidGetProc`, then take the control lock,
re-check `slots[n].owner_pid == pid` (and owner_start against the beentry),
bump `reset_generation`, release. The owner consumes it at its next `wait_end`
by comparing against its cached value; because the owner token is verified
under the same lock that publishes the request, a successor cannot inherit
it. Test with an injection point between resolution and publication.

### 4.5 SQL surface

Views and functions of §2, implemented as SRFs over the slot table under the
shared lock per slot. `pg_wait_event_timing_histogram_buckets` view ported from
v6. Overflow view as in v6 (unknown event, capacity, LWLock hash full).

### 4.6 Tests (0004)

- regress: `sql/pg_wait_event_tracing.sql` ported from v6 (views,
  enable/disable, histogram buckets, overflow rows, reset self) plus the
  capacity-vs-`pg_wait_events` check of §3.1.
- TAP `t/001_memory.pl`: `max_connections=200`, enable stats in one session,
  wait, assert DSA growth < 1 MiB via `pg_dsm_registry_allocations`; disable
  → payload released.
- TAP `t/002_ownership.pl`: capture in A, record ProcNumber, exit, create
  sessions until reuse, capture off in B → no rows for B; repeat across roles.
- TAP `t/003_reset_acl.pl`: own reset; same-role target; `pg_signal_backend`
  member; non-superuser vs superuser target (refused); auxiliary PID (refused).
- TAP `t/004_reset_race.pl` (injection points): request published against a
  successor is not consumed.

## 5. Trace level (0005): design and the fixes it carries

### 5.1 Ring and reader

Ring in `trace_dsa`, single-writer seqlock with the v6 position-encoded
identity check; reader = v6 `pg_get_wait_event_trace` logic ported; 32-byte
records (StaticAssert kept). `trace_ring_size` PGC_POSTMASTER.

### 5.2 Orphan lifecycle without core help (fix 3)

On owner exit the ring is not freed: state → ORPHANED, owner token retained,
readable post-mortem (flight recorder, as in v6). Reclaim happens when the
**successor** on that ProcNumber enables trace: under the lock, if state is
ORPHANED and the owner token differs from mine, free or reuse the ring, then
publish mine. Administrative sweep `pg_stat_clear_orphaned_wait_event_rings()`
kept. Because nothing runs at process start, EXEC_BACKEND ordering cannot
matter. TAP `t/005_orphan_reuse.pl` (enable in A, record, exit, reuse
ProcNumber, enable in B, verify B owns a fresh ring and A's orphan was
reclaimed) runs on the Windows CI jobs too.

### 5.3 Query markers (fix 6) — DECIDED 2026-09-09: no core hook; explicit
boundaries in the data; attribution shipped as a tested function; documented
exhaustively. The owner is specifically worried about the "in-between" waits
(after a statement ends, before the next starts); every item below exists to
make that interval unambiguous.

Marker sources (all existing mechanisms):

| Marker | Emitted from | Meaning |
|---|---|---|
| `QueryStart` | `post_parse_analyze_hook`, non-zero query id | a statement is open (extended protocol: at Parse) |
| `ExecStart` / `ExecEnd` | `ExecutorStart_hook` / `ExecutorEnd_hook`, nesting depth carried | executor run of that statement (nested runs from SQL functions/PL have depth > 0) |
| `UtilityStart` / `UtilityEnd` | `ProcessUtility_hook` | a utility statement, incl. COPY FROM STDIN whose client reads are INSIDE it |
| `TxnCommit` / `TxnAbort` | `RegisterXactCallback` (XACT_EVENT_COMMIT / ABORT, and the PARALLEL/PREPARE variants) | end of the transaction; the commit WAL-flush wait precedes it and belongs to the last statement |
| `Idle` | **synthesized by the collector**: in the begin hook, when `wait_event_info == WAIT_EVENT_CLIENT_READ` and the marker state machine is in AFTER_STATEMENT | the backend is waiting for the client with no statement open: the explicit end of the previous statement's interval |

Marker state machine in the collector (per backend, trace level only):
IDLE → (QueryStart|UtilityStart|ExecStart) → OPEN → (ExecEnd at depth 0 |
UtilityEnd | TxnCommit | TxnAbort) → AFTER_STATEMENT → first ClientRead begin
→ emits `Idle`, → IDLE. QueryStart while AFTER_STATEMENT (pipelined batch,
multi-statement simple-query string, explicit transaction with the next
statement already buffered) goes straight to OPEN with no `Idle`, which is
correct: there was no idle time. Errors: `TxnAbort` closes whatever is open;
an `ExecStart` without `ExecEnd` therefore always ends at `TxnAbort`.

Attribution rule, shipped as SQL function `pg_wait_event_trace_by_statement(procnumber)`
(SRF over `pg_get_wait_event_trace`) and covered by regression tests: a
statement's interval runs from its `QueryStart` (or `UtilityStart`) to the
earliest of the next `Idle`, the next `QueryStart`/`UtilityStart` at depth 0,
or `TxnAbort`; waits inside are summed per wait event; waits between `Idle`
and the next start are reported as a synthetic row `<idle>`; waits before the
first marker of the ring as `<unattributed>`.

What is NOT provided and is documented as such: the v6 end-of-message marker
for the pipelined extended protocol (needs a core hook that does not exist;
the next `QueryStart` bounds a queued statement, which is exact when messages
are already buffered); attribution of parallel workers' waits to the leader's
statement (they are recorded under the worker's own ProcNumber; documented,
with the leader's query id available in the worker's markers as future work);
waits before a session's first statement (`<unattributed>`).

Tests (regress, deterministic): single autocommit statement → QueryStart,
ExecStart, ExecEnd, TxnCommit, Idle; explicit transaction with two statements
→ ... ExecEnd, Idle, QueryStart ...; multi-statement simple-query string →
no Idle between the two; utility statement; error inside a statement →
TxnAbort closes it; nested SQL function → depth 1 markers inside depth 0;
`pg_wait_event_trace_by_statement` sums match a hand computation on the same
ring contents.

### 5.4 Reader API (fix 7)

SQL SRFs are the only supported interface. The direct-reader documentation is
removed; all shared structures are private to the module.

### 5.5 Tests (0005)

- TAP `t/010_trace_seqlock.pl`: the v6 seqlock test, ported (injection points).
- TAP `t/005_orphan_reuse.pl` as above; `t/011_trace_wrap.pl`: ring wrap,
  reader during write / disable / owner exit; sweep.
- regress: markers for one statement, several in one message, utility
  statement, error mid-statement, nested function call, commit marker.

## 6. Documentation

- `doc/src/sgml/pgwaiteventtracing.sgml` (contrib page, listed in contrib.sgml
  and filelist.sgml): ported from monitoring.sgml + config.sgml, rewritten for
  the module. Keep: live-only statistics, ring bounds, orphan contract stated
  before the post-mortem example, the three modes with costs (numbers from the
  re-measurement, §8). Fix: markers section (5.3); remove direct reader (5.4);
  reset authorization rules (4.3).
- `xfunc.sgml` custom wait events: one paragraph + the example switched to
  `pgstat_report_wait_start_timed()/end_timed()` with a version guard.
- Hook contract comment in `wait_event.h` (already in 0001) is the reference.

## 7. Removal list (what must NOT survive from v6)

configure/meson `--enable-wait-event-timing`; `pg_config.h.in` symbol;
`guc_parameters.dat`/`guc_tables.c`/`postgresql.conf.sample` entries;
`system_views.sql` views; `pg_proc.dat` OIDs 9956-9962; `lwlocklist.h`,
`subsystemlist.h`, `wait_classes.h` additions; `proc.c`/`auxprocess.c`/
`postinit.c`/`postgres.c`/`execMain.c`/`backend_status.c` calls;
`wait_event_timing.c/.h`; `wait_event_timing_data.h` generation and its
Makefile/meson/.gitignore/headerscheck lines; `typedefs.list` entries;
`src/test/regress` files and `parallel_schedule` lines; test_misc TAP test;
the CI task "build one task with --enable-wait-event-timing" (v6 0004).
Acceptance: `git diff master -- src/ doc/` for 0004+0005 touches only
`contrib/`, `doc/src/sgml/pgwaiteventtracing.sgml`, `doc/src/sgml/contrib.sgml`,
`doc/src/sgml/filelist.sgml`, `doc/src/sgml/xfunc.sgml`.

## 8. Validation before posting

RULE (owner, 2026-09-09): **no PostgreSQL server process is ever started on
the development host.** Local validation is compile-only. All test execution
happens on the fork's CI or on a host the owner provides.

1. Local: meson debugoptimized + cassert + werror + injection_points build,
   `headerscheck`, `cpluspluscheck`. No `meson test`, no pg_regress, no TAP,
   no smoke servers. Expected regression outputs are authored by reasoning
   and corrected from CI logs.
2. Fork CI matrix green (9 jobs, incl. both Windows slices for the
   process-reuse test). Each WP branch is pushed to the fork as `ci/wet-v8-wpN`
   to run the full test suite; iterate from the CI logs (`gh run view -R
   DmitryNFomin/postgres <id> --log-failed`). Budget ~45 min per iteration.
3. Demos: `blog/demo/run_demo_v2.sh` and `run_demo_trace.sh` re-run against the
   module (GUC names updated) on the owner's host at the end; transcripts
   equivalent.
4. Re-measurement on the bare-metal host (USER sets it up when everything is
   ready; ~3 h): configurations master, NULL (0001+0002), module-off, stats,
   trace; workloads W1 already-set latch, W3, W4, W6c; 12 paired reps, same
   harness as the package (`tests/executed-harness/amended`, with the real
   module). Numbers go into the cover letter and the docs cost paragraph.
5. Series: rebase on master, `git format-patch -v8`, `git am` round trip, cfbot
   dry run on the fork.

## 9. Execution model

- Development machine: this host (4 cores, 15 GB, 12 GB free disk → at most
  two worktrees + build dirs at a time; delete build dirs when a WP closes).
  Branches `wet-v8-wp<N>` from `wet-v7-rfc`, one worktree each, merged in
  order into `wet-v8`; builds with `.venv-v7-rfc` meson,
  `--buildtype=debugoptimized -Dcassert=true -Dwerror=true -Dinjection_points=true`.
- Hands-on work by Claude Code subagents on the cheap models (sonnet; haiku
  for mechanical steps), each with a written brief (`blog/demo/brief-v8-wp<N>.md`)
  and a deliverable report (`pg_patch_wait_events/wp<N>-report.md`); the main
  model writes briefs, reviews every diff, and adjudicates.

| WP | Content | Depends on | Est. days |
|---|---|---|---|
| WP1 | 0003 test_wait_hook module + regress tests — **DONE 2026-09-11**: branch `wet-v8-wp1` commit 1156a3806cf (after review fix: ring filtered to PgSleep, single-wait helper instead of pg_sleep); fork CI run 34580643617 all green, test executed and passed on Linux Meson, Autoconf, Windows VS | — | 1 |
| WP2 | 0004 module: port collector under the new name, add v6 features, fixes 1/2/4/5, SQL script, capacity test, regress — **DONE 2026-09-11** (compile-clean; CI pending): `wet-v8-wp2` M1 7e518f5bb94, M2 6311e336b25, M3 8ba6561d410, review fixes f4bf69380b8; CI run 34599726028 failed on every platform only because the regress test lacked CREATE EXTENSION (the preloaded library itself started cleanly on Linux, macOS, MinGW and MSVC); fixed in fd0f427c385; re-run 34605913822 green on 8/9 jobs, macOS failed because its CI job forces `debug_parallel_query = regress`, so pg_sleep ran in a parallel worker and the wait was (correctly) recorded under the worker; test fixed in 02fe98d0f97 (`SET debug_parallel_query = off`); **CI run 34609838704 all green, test executed and passed on macOS, Linux Meson 64, Autoconf, Windows MSVC and MinGW**. WP2b and WP4a branched from f4bf69380b8 and must be rebased onto the WP2 tip at assembly | — | 5 |
| WP2b | option A: control table to fixed shmem; reserved region + in-hook claim for server-side processes; t/006 — **code done 2026-09-11** (M1 control table to fixed shmem, M2 region + lock-free claim + assign-hook fix via `pwet_capture_effective`, M3 t/006), rebased onto the WP2 tip. Review: claim/read barrier pairing correct; **critical bug** — the startup hook creates the ~10 MB region even when capture was off at start (the range never depends on capture), so the postmaster cannot start with capture off (the default); fix sent: store presence and bounds in an always-allocated shared header set by the postmaster, read by EXEC_BACKEND children. Also t/006 must skip the I/O-worker check unless io_method = worker (CI runs some jobs with io_uring); bump `generation` on claim/release. Bug confirmed by the pre-fix CI run 34638388634: `FATAL: not enough shared memory for data structure "pg_wait_event_tracing server processes" (10633200 bytes requested)` at postmaster start for the capture-off regress node and t/006 node2 (Linux, Windows). **Fix commit aa93d6e5fd4 reviewed and correct** (always-allocated `PwetRegionHeader` written only by the postmaster when it first creates shared memory; every process, EXEC_BACKEND children included, reads presence and bounds from it; t/006 skips the I/O-worker check unless io_method = worker; `generation` bumped on claim/release). A second bug then showed up as a Windows-passes/Linux-fails split: the eligibility cache was inherited from the postmaster across fork (see §4.2b); fixed in 64f135699e0 by keying it to MyProcPid. **DONE 2026-09-12: CI run 34700842756 all green; t/006 passes on every platform (9 checks, 8 where io_method has no workers and that check skips).** | WP2 | 1.5 |
| WP3 | launched 2026-09-11 on `wet-v8-wp2b`. M1 ring/writer/readers a7e709759f5 and M2 orphan lifecycle e36829e9ac5 landed 2026-09-12; M3 (markers, attribution, regress) in progress. Review of M1/M2: seqlock port faithful to v6 (position-encoded identity, barriers both sides), orphan design free of V6-3's ordering trap, ring size forced to a power of two. Three items sent back: (A) reserved-slot processes could never attach a ring, and never registered the exit callback; (B) rows emitted under the shared lock; (C) grants question — the own-session trace function is restricted to pg_read_all_stats while the statistics equivalent is PUBLIC with per-row checks. See §4.2b | WP2b | 4 |
| WP3 | 0005 trace: ring, reader, orphans (fix 3), markers incl. xact callback (fix 6), SRFs | WP2 skeleton | 4 |
| WP4a | TAP tests 001-004 (fixes 1, 2, 4, 5) — **DONE 2026-09-11**: `wet-v8-wp4a` (rebased on WP2 tip) 74310b3a69b + 2c140de41c8 + 53888ba66f2 + cbb10ad23dc; after three CI rounds (causes: capture is PGC_SUSET; PGPROC free list is FIFO; BackgroundPsql dies on the first error; one pg_sleep can be several waits on Windows) **CI run 34636885229 all green, every test executed with no skips**: regress 1, 001 7, 002 9, 003 27, 004 2 on Linux 32/64, macOS, MinGW, MSVC | WP2 | 3 (overlaps) |
| WP4b | TAP tests 005, 010, 011 (trace level) | WP3 | 2 |
| WP5 | Docs page, xfunc paragraph, removal audit (§7) | WP2/WP3 | 2 (overlaps) |
| WP6 | CI, demos, re-measurement, cover letter, series assembly | all | 3 |

Critical path ≈ WP2 → WP3 → WP6 ≈ 12 working days; calendar target: first full
local `meson test` green by 2026-09-22, CI + re-measure by 2026-09-26, post by
2026-09-29.

## 9a. To revisit with the owner after v8 is posted (owner, 2026-09-11)

The owner asked to come back to these three deliberate omissions once the v8
work is finished. Each is documented in v8; none blocks posting.

1. **No end-of-message marker** for the extended/pipelined protocol: needs a
   new core hook in the protocol loop (postgres.c). Cost of adding: a third
   core patch to defend. Loss without it: for pipelining clients the end of a
   queued statement is taken from the next statement's start.
2. **No trace for server-side processes before a reload**: option A reserves
   statistics memory only (~10 MiB); rings (~4 MB each) for ~50 processes
   would be ~200 MB. Possible follow-up: an opt-in setting naming process
   types that get a ring at startup (crash-recovery tracing is the real gap).
3. **No C-level reader API**: SQL functions are the only supported interface
   (fix for v6 bug 7). Possible follow-up: a versioned C interface designed
   against a concrete consumer (e.g. EXPLAIN WAITS).

## 10. Decisions

1. Names: DECIDED — `pg_wait_event_tracing`, GUC words unchanged, SQL names unchanged.
2. Series layout: DECIDED — five patches as in §1.
3. Class counts: DECIDED — hardcoded table + enforcing test, no core change.
4. Markers scope (§5.3): recommended, awaiting a yes.
5. Re-measurement host: user provides when everything is ready (§8.4).
