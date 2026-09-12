# WP2b report: server-side processes collect from process start (option A)

Branch `wet-v8-wp2b`, originally based on `f4bf69380b8` (wet-v8-wp2's tip
when this WP started); the coordinator rebased it onto wet-v8-wp2's
current tip `02fe98d0f97` (two commits touching only the regress
sql/expected, unrelated to this WP) partway through review, so M1/M2/M3
below carry new hashes post-rebase. Worktree
`<workspace>/wt-v8-wp2b`. Build dir
`<workspace>/build-v8-wp2b`
(`--buildtype=debugoptimized -Dcassert=true -Dwerror=true
-Dinjection_points=true`, `.venv-v7-rfc` meson/ninja, `ninja -j2` only).
The main checkout at `<workspace>/postgres`
stayed on `wet-series` throughout; nothing was pushed.

Five commits on `wet-v8-wp2b` (M1, M2, M3, plus two review-fix commits
added after CI/coordinator review):

| # | Commit | Subject |
|---|---|---|
| M1 | `b03eca58d46` | pg_wait_event_tracing: move the control table to fixed shared memory |
| M2 | `ff43c7fb571` | pg_wait_event_tracing: reserve server-process slots and claim them lock-free |
| M3 | `f2079090fe2` | pg_wait_event_tracing: add TAP test for server-side process collection |
| fix 1 | `aa93d6e5fd4` | pg_wait_event_tracing: create the server region only when it was requested |
| fix 2 | `64f135699e0` | pg_wait_event_tracing: do not inherit the fixed-slot eligibility cache across fork |

All four: author `Dmitry Fomin <fomin.list@gmail.com>`, trailer
`Discussion: https://postgr.es/m/CAPHG-0mAOn05ae6Kqx1wHXxzOk4E5W7ajjd=QBhgkR7a0uyQmw@mail.gmail.com`.
The coordinator will squash the fix commit into M2 at assembly, per their
instruction; its content is described inline below, next to the M2
material it corrects.

## M1 — control table to fixed shared memory (`b03eca58d46`)

A server-side process must reach its control slot from inside the begin
hook, where it cannot attach anything, so the control table moves from
the DSM registry (`GetNamedDSMSegment()`) to fixed shared memory:
`shmem_request_hook`/`shmem_startup_hook`, `RequestAddinShmemSpace()` +
`RequestNamedLWLockTranche("pg_wait_event_tracing", 1)`,
`ShmemInitStruct("pg_wait_event_tracing control", ...)` +
`GetNamedLWLockTranche(...)`. `PwetControl`'s embedded `LWLock` is gone
(a flexible-array-only struct isn't valid C); `pwet_ctl` is now a plain
`PwetSlot *` array with the lock kept separately as `pwet_lock`, the
same shape `pg_stat_statements` uses. `pwet_ensure_control()` and all its
call sites are removed; `GetNamedDSA()` keeps attaching the per-backend
DSA payload exactly as before. Behaviour is otherwise unchanged — every
remaining caller was re-read against the pre-move version.

**Finding, recorded as a comment on `pwet_shmem_request()`:** confirmed
directly in `postmaster.c` (not inferred) that it calls, in order,
`SelectConfigFiles()`, `process_shared_preload_libraries()` (which runs
every library's `_PG_init()`), `InitializeMaxBackends()`, and only then
`process_shmem_requests()` — so `PWET_NUM_SLOTS`'s use of `MaxBackends`,
and (from M2 on) `pwet_capture`'s reflection of the config file, are both
already final by the time `shmem_request_hook` runs.

**Finding, not asked for but load-bearing:** this master (post-`ShmemRequestStruct`/`RegisterShmemCallbacks`
shmem-API refactor) no longer has `AddinShmemInitLock` at all — grepped
the whole tree; the only surviving reference is one doc paragraph about
the unrelated post-startup `LWLockNewTrancheId()` path. `ShmemInitStruct()`
now serializes its own lookup-or-create step internally via
`ShmemIndexLock`, so no wrapper lock is needed or possible; `pwet_shmem_startup()`
just calls it directly, unlike pg_stat_statements on older masters that
still have `AddinShmemInitLock` and wrap the call in it.

**EXEC_BACKEND:** confirmed in `ipci.c` that `shmem_startup_hook` runs
once in the postmaster (`CreateSharedMemoryAndSemaphores()`) and again in
every EXEC_BACKEND child (`AttachSharedMemoryStructs()`), matching how
pg_stat_statements relies on it; `pwet_ctl`/`pwet_lock` are re-derived
there each time since they're process-local pointers, not shared state.

## M2 — reserved region + claim protocol (`ff43c7fb571`, region-presence
part later corrected in the fix commit)

**R expression:** `R = [MaxConnections, MaxBackends + PWET_NON_IO_AUX_PROCS + io_max_workers)`,
clamped to `MaxBackends + NUM_AUXILIARY_PROCS`, where
`PWET_NON_IO_AUX_PROCS = NUM_AUXILIARY_PROCS - MAX_IO_WORKERS` (proc.h's
own "6", expressed from its constants rather than as a literal).
`|R| = autovacuum_worker_slots + NUM_SPECIAL_WORKER_PROCS(2) + max_worker_processes
+ max_wal_senders + PWET_NON_IO_AUX_PROCS(6) + io_max_workers`.

**Reserved bytes at default settings:** `MaxConnections=100`,
`autovacuum_worker_slots=16`, `max_worker_processes=8`,
`max_wal_senders=10`, `io_max_workers=8` → `MaxBackends = 100+16+8+10+2 =
136`, `|R| = 16+2+8+10+6+8 = 50` (equivalently `R=[100,150)`, size 50).
Stride at the default `pg_wait_event_tracing.max_tranches=192` is
212,664 bytes (verified in WP2's own report by compiling a standalone C
program mirroring the struct layout on that platform — not re-verified
here, since M2 doesn't change `PwetStats`'s layout). Reserved region size
= `50 × 212,664 = 10,633,200 bytes ≈ 10.14 MiB`.

**Verified in proc.c (`ProcGlobalShmemInit()`/`InitAuxiliaryProcess()`),
not just taken from the plan:** ProcNumbers are handed out in one array —
`[0, MaxConnections)` client backends, then autovacuum
launcher/workers+special workers, then background workers (parallel
query and logical-rep workers included), then WAL senders, ending at
`MaxBackends`; auxiliary processes then fill `[MaxBackends, MaxBackends +
NUM_AUXILIARY_PROCS)` by **first-free linear search**, not by type
(`InitAuxiliaryProcess()`'s `for (proctype = 0; proctype <
NUM_AUXILIARY_PROCS; proctype++) if (auxproc->pid == 0) break;`). Since
the scan always starts from index 0, the highest occupied index at any
instant is bounded by the current live-process count, so with at most
`PWET_NON_IO_AUX_PROCS + io_max_workers` such processes concurrently
alive, none of their ProcNumbers reach past R's upper bound — **as long
as `io_max_workers` doesn't grow past its postmaster-start value.**
`io_max_workers` is `PGC_SIGHUP`: if raised by a later reload, additional
I/O workers beyond the reserved slice fall back to the DSA path once
they reach a safe point, since `pwet_can_attach()`'s eligibility check is
against the *stored* R, not "is this any server-side process" — this
only shrinks fixed-slot coverage, it never lets a process write outside
the reserved bytes. Recorded as a comment on `pwet_compute_server_region()`.

**Claim protocol ordering (barriers), from `pwet_claim_fixed_slot()`,
called once per process from the begin hook:**
1. `owner_pid = 0` (plain store), `pg_write_barrier()`.
2. If the slot's stored owner token (`owner_pid`/`owner_start`) doesn't
   match this process's own identity, `memset()` the whole payload to
   zero and re-initialize just the LWLock-hash header fields (mirrors
   what the DSA attach path does after `DSA_ALLOC_ZERO`).
3. `owner_start = MyStartTimestamp`, `pg_write_barrier()`, `owner_pid =
   MyProcPid`, then `pg_atomic_fetch_add_u32(&slot->generation, 1)`
   (added by the fix commit — see item (c) below).
4. Cache `pwet_my_stats`/`pwet_my_procno` locally; seed
   `pwet_last_reset_generation` from the slot's `reset_generation`.

**Lockless read ordering**, from `pwet_fixed_owner_matches()` /
`pwet_fixed_owner_unchanged()`: read `owner_pid` and `owner_start`,
`pg_read_barrier()` (pairs with the claim's step-1 write barrier — a
reader can never observe a stale payload attributed to a *new* owner_pid
because the payload write in step 2/3 cannot be reordered before it),
compare against the target's live `PgBackendStatus`; if it matches, copy
the payload, `pg_read_barrier()` (pairs with the claim's step-3 write
barrier), re-read both fields and accept only if unchanged. `wait_end()`
needed **no changes at all** — it already addresses the payload and
`reset_generation` generically via `pwet_my_stats`/`pwet_my_procno`,
which the claim populates identically for either kind of slot.

**Folded-in fix (per the coordinator, found in wet-v8-wp2 during this WP,
required because this WP depends on the affected path):** `guc.c`'s
`set_config_with_handle()` (`case PGC_ENUM: ... if (conf->assign_hook)
conf->assign_hook(newval, newextra); *conf->variable = newval;`) calls
the assign hook **before** storing the new value — verified by reading
that exact code, not just trusting the report. `pwet_can_attach()`'s old
`pwet_capture == PWET_CAPTURE_OFF` check therefore saw the *stale* value
for the whole synchronous call chain triggered from
`pwet_assign_capture()`. A client backend hid this (the next statement's
`post_parse_analyze`/`ExecutorStart` attaches once `pwet_capture` is
actually updated), but a server-side process has no next statement: with
the region absent it depends entirely on that one synchronous call to
attach via DSA, and with the region present a released fixed slot
depends on `pwet_wait_begin()`'s own re-claim (which doesn't go through
`pwet_can_attach()` at all — unaffected). New `pwet_capture_effective`
mirrors `pwet_capture` except `pwet_assign_capture()` updates it *first*;
`pwet_can_attach()` — the only place this mattered — now tests
`pwet_capture_effective`. The begin/end hooks deliberately keep testing
`pwet_capture` itself, per the coordinator's instruction, so recording
never starts or stops on a value that hasn't actually taken effect.

## M3 — TAP test (`f2079090fe2`, later corrected)

`t/006_server_processes.pl`, wired into `meson.build`'s existing `tests`
entry (new `'tap'` key, no `env` needed) and `Makefile` (`TAP_TESTS =
1`). Node1 (capture=stats+preload in `postgresql.conf`, no reload):
checkpointer/walwriter/background-writer rows present, plus an io-worker
row when `io_method = worker` (case 1); `pg_shmem_allocations` region
size ≥ `|R| × 200000` bytes, a deliberately conservative
platform-independent lower bound rather than reproducing the exact
struct layout in Perl (case 2); a standby from a base backup shows
`startup` rows with no reload on either side (case 3). Node2 (capture
off at start): no region exists; `ALTER SYSTEM` + `pg_reload_conf()` +
activity makes checkpointer rows appear (case 4 — this is the regression
test for the folded-in assign-hook fix, *and*, after the fix commit, also
for the region-presence bug in (a) below: node2 is exactly the "capture
off at postmaster start" case that used to crash the postmaster). Applied
the coordinator's four CI lessons: `debug_parallel_query = off` on every
node; `pg_wait_event_tracing.capture` only ever set via `postgresql.conf`
or `ALTER SYSTEM` (both bypass the `PGC_SUSET` runtime-`SET` check); no
`BackgroundPsql`/no test expects an `ERROR`; every assertion is a
`poll_query_until` presence check (`EXISTS(...)`), never an exact wait
count.

## Fix commit — "create the server region only when it was requested"

Four items from the coordinator's review, all in this one commit:

**(a) CRITICAL, fixed.** `pwet_shmem_startup()` was calling
`pwet_compute_server_region()` and creating the ~10 MiB region whenever
`end > start` — but that range never depends on `pwet_capture` and is
never actually empty (`start = MaxConnections` is always less than
`end ≥ MaxBackends + PWET_NON_IO_AUX_PROCS + io_max_workers`), so the old
comment claiming otherwise was simply wrong. With capture off at
postmaster start (**the default**), `pwet_shmem_request()` correctly
never reserved those bytes, but `pwet_shmem_startup()` tried to
`ShmemInitStruct()` them anyway — `ereport(FATAL, "not enough shared
memory for data structure \"pg_wait_event_tracing server
processes\"...")`, and the postmaster could not start at all with the
module preloaded. (Node1 in the TAP test, which starts with capture
already on, would not have caught this; node2, capture off at start, is
exactly the reproduction case, and would have failed the whole test file
by making the postmaster refuse to start.)

Fixed by adding `PwetRegionHeader` (`bool server_region_present; int
server_region_start; int server_region_end;`), its own always-requested
`ShmemInitStruct("pg_wait_event_tracing header", ...)`, and a new static
`pwet_region_requested` set in `pwet_shmem_request()` from `pwet_capture`
immediately before conditionally requesting the region's bytes.
`pwet_shmem_startup()` now creates the header via `ShmemInitStruct()`;
only when it is newly created (`!found`, which is only ever true in the
postmaster, since an EXEC_BACKEND child only ever attaches to
already-created shared memory) does it consult `pwet_region_requested`/
`pwet_compute_server_region()` and fill the header; every other call
(every EXEC_BACKEND child, and any later re-entry) just reads
`server_region_present`/`start`/`end` back out of the header, and only
opens `ShmemInitStruct(PWET_SERVER_REGION_NAME, ...)` when
`server_region_present` is true. This is required, not just simpler: an
EXEC_BACKEND child re-runs `_PG_init()`, so its own `pwet_capture` can
differ from the value at postmaster start (e.g. after a reload) — the
header is what stops that discrepancy from ever changing whether the
already-fixed-size region is treated as present. `pwet_is_fixed_procnumber()`
needed no changes: it already only ever consults
`pwet_server_region`/`pwet_server_region_start`/`pwet_server_region_end`,
which now come from the header instead of being recomputed unconditionally.

**(b) fixed.** `t/006`'s "io worker" check now reads `SHOW io_method` on
node1 first and only runs the check (in a `SKIP:` block) when it is
`'worker'`; several CI jobs force `io_method = io_uring` via
`PG_TEST_INITDB_EXTRA_OPTS`, which has no I/O workers at all, so the
check would otherwise time out there instead of skipping cleanly.

**(c) fixed.** `pwet_claim_fixed_slot()` and `pwet_release_fixed_slot()`
now bump `slot->generation` (`pg_atomic_fetch_add_u32`) alongside the
ownership change, matching section 4.1's "bumped on every ownership
change" and the DSA path's own behaviour in `pwet_attach_stats()`/
`pwet_release_stats()`. This resolves open question 1 from the prior
version of this report. An atomic add remains allowed inside the begin
hook per the hard rules.

**(d)** See the new open question below.

## Fix 2 — "do not inherit the fixed-slot eligibility cache across fork"

CI run 34640640603 on `aa93d6e5fd4`: `t/006` passed 9/9 on both Windows
jobs (MinGW and MSVC) but failed on Linux (Meson 32-bit, Meson 64-bit,
Autoconf) and macOS — checkpointer, walwriter, background writer, io
worker, and the standby's startup process never got rows (the node2
reload checks and the region-size check, which don't depend on a
fork()ed process claiming a slot, passed everywhere).

**Root cause.** `pwet_wait_begin()`'s eligibility cache
(`pwet_fixed_slot_checked`/`pwet_fixed_slot_eligible`, a bare "have we
ever checked" bool plus the answer) is process-local static memory — but
the **postmaster itself** also calls the begin hook: its `ServerLoop()`
waits through `WaitEventSetWait()` (confirmed at
`src/backend/postmaster/postmaster.c:1693`), which reports through the
same timed pair every other wait event does, and the hook is installed
in `_PG_init()`, which runs in the postmaster during
`process_shared_preload_libraries()`. The postmaster's `MyProcNumber` is
`INVALID_PROC_NUMBER` for its entire life (confirmed: only
`InitProcess()`/`InitAuxiliaryProcess()`, in `proc.c`, ever set it to
anything else, and the postmaster calls neither), so the very first time
the postmaster itself hits `pwet_wait_begin()`, the cache computes and
stores `checked=true, eligible=false` — **permanently, in the
postmaster's own memory image.** Every child the postmaster later
`fork()`s (checkpointer, background writer, WAL writer, every ordinary
backend, the startup process) inherits that exact memory image,
`checked=true, eligible=false` included, and — since the cache says
"already checked" — never re-derives its own real answer from its own,
actual `MyProcNumber`, so it never claims its fixed slot, no matter that
`MyProcNumber` is right there. An `EXEC_BACKEND` child (Windows) does not
inherit any of this: it starts from a freshly zeroed image via `exec()`,
not a copy of the parent's memory, which is exactly why the Windows jobs
passed and every fork()-based platform failed.

**Fix.** Replaced the bare bool with `pwet_fixed_slot_checked_pid`
(`static int`, matching `MyProcPid`'s own type), and recompute whenever
`pwet_fixed_slot_checked_pid != MyProcPid` — which is true for every
process's own first call, forked or exec'd, since no two simultaneously
live processes share a pid, and the postmaster (which never updates this
static, for the reason below) always leaves it at its zero-initialized
default for every child to inherit. If `MyProcNumber` is still
`INVALID_PROC_NUMBER` when this runs (always true in the postmaster;
possible only very early, before `InitProcess()`, in any other process),
neither claims nor caches — it just returns, so a later call retries;
this is a few extra branches per wait forever in the postmaster
specifically (it never gets a ProcNumber, so it retries on every single
wait for its whole life), acceptable since the postmaster's own waits
are not a hot path.

**Audit of every other static the hooks/claim/assign-hook read or write,
for the same "written in the postmaster, wrongly inherited by fork"
problem** (per the coordinator's request):

- `pwet_my_stats`, `pwet_my_procno`, `pwet_last_reset_generation`: only
  ever written by `pwet_claim_fixed_slot()` or `pwet_attach_stats()`.
  Both are gated by checks that are unconditionally false in the
  postmaster — `pwet_claim_fixed_slot()` is only reached when
  `pwet_fixed_slot_eligible` is true, which (after this fix) can only
  happen for a process with a valid `MyProcNumber`, which the postmaster
  never has; `pwet_attach_stats()` goes through `pwet_can_attach()`,
  which explicitly checks `MyProc == NULL || MyProcNumber ==
  INVALID_PROC_NUMBER` and returns false for the postmaster on that
  basis alone. So all three stay at their fresh-process default
  (NULL/`INVALID_PROC_NUMBER`/0) in the postmaster forever, and every
  forked child correctly inherits exactly those same defaults — the
  values a fresh process would also start with. No bug.
- `pwet_capture_effective`: **is** written in the postmaster (every time
  `pwet_assign_capture()` runs there — once for the initial
  `DefineCustomEnumVariable()` placeholder application in `_PG_init()`,
  and again on every subsequent reload, since the postmaster reprocesses
  the config file on SIGHUP too, to know what to pass to future
  children). But this is intentional and correct, not a bug: outside the
  narrow synchronous window of an actual `assign_hook` call in progress,
  `pwet_capture_effective` always equals `pwet_capture` (nothing else
  ever writes either), and `pwet_capture` itself is a GUC variable that a
  forked child is *supposed* to inherit from the postmaster (that is how
  every ordinary GUC reaches a freshly forked child at all) — so a
  child's inherited `pwet_capture_effective` is, by construction, always
  consistent with its own inherited `pwet_capture` at fork time.
- `pwet_attach_needed`: set in `_PG_init()` and `pwet_assign_capture()`
  in the postmaster and inherited by every fork()ed child — but this is
  the existing, already-relied-upon mechanism by which a client backend
  learns to attach at its first `post_parse_analyze`/`ExecutorStart`
  call; inheriting it is the point, not a bug. A fixed-slot-eligible
  child ignores it entirely regardless (`pwet_can_attach()`'s R-guard
  blocks the DSA path outright), so it cannot cause the fixed-slot bug
  either way.
- `pwet_exit_started`, `pwet_stats_writes_disabled`,
  `pwet_exit_callback_registered`, `pwet_active`: all false (respectively
  true for `pwet_active`) in the postmaster for its whole life (the first
  three only ever become true via code paths — `pwet_before_shmem_exit()`,
  `pwet_maybe_attach()`'s success path — that are themselves unreachable
  in the postmaster, by the same reasoning as `pwet_my_stats` above), and
  those are exactly the values a fresh child should start with too. No bug.
- `pwet_ctl`, `pwet_lock`, `pwet_server_region`,
  `pwet_server_region_start`, `pwet_server_region_end`,
  `pwet_server_stride`, `pwet_stats_dsa`, `pwet_region_requested`: all
  either raw pointers into shared memory (valid at the same address in
  every fork()ed child by construction — this is the *correct,
  documented* fork-inheritance path, contrasted with EXEC_BACKEND's
  explicit re-derivation, in `pwet_shmem_startup()`'s own comment) or, for
  `pwet_region_requested`, meaningless outside the one postmaster-only
  code path that reads it (see the fix-1 section above). No bug.

Conclusion: `pwet_fixed_slot_checked`/`pwet_fixed_slot_eligible` was the
only static with this problem. `t/006` (already merged) is the
regression test for it; no test changes were needed.

## Validation performed

- `meson setup build-v8-wp2b --buildtype=debugoptimized -Dcassert=true
  -Dwerror=true -Dinjection_points=true` — clean.
- `ninja -j2 contrib/pg_wait_event_tracing/pg_wait_event_tracing.so` —
  clean under `-Dwerror=true`, re-verified after every commit including
  both fix commits (not just once at the end).
- `ninja -j2 headerscheck` and `ninja -j2 cpluspluscheck` — both pass at
  every commit boundary, including the final fix 2 commit.
- `meson test --list` (before the fix commits) showed both
  `pg_wait_event_tracing/regress` and
  `pg_wait_event_tracing/006_server_processes` registered (listing only;
  starts no server); the fix commit does not touch `meson.build`, so this
  was not re-run after it.
- `perl -c` (with `PERL5LIB=src/test/perl`) on `t/006_server_processes.pl`
  — syntax OK, re-checked after the `SKIP:` block was added.
- **Not run** (hard rule: compile only, no server): `meson test` in any
  form, `pg_regress`, `initdb`/`pg_ctl`, TAP execution. No server process
  was started at any point in this session — which is exactly why bug (a)
  was not caught before the coordinator's review: it can only manifest as
  an actual postmaster start failure.
- One accidental unscoped `ninja -j2` (no target) was started earlier in
  this session while investigating `meson test --list` and was killed
  within a few seconds once noticed; its partial backend object files
  were deleted immediately afterward. `build-v8-wp2b`'s steady-state
  footprint is ~61 MiB throughout — module objects and a handful of
  explicitly-built generated headers only, not a full tree build.

## Open questions

1. ~~`generation` not bumped by the fixed-slot claim/release paths~~ —
   resolved by fix (c).
2. ~~M3's `t/006_server_processes.pl` is unexecuted~~ — it has since run
   on the fork's CI (run 34640640603, on fix 1's commit `aa93d6e5fd4`):
   `regress` passes everywhere and the startup fix works (the postmaster
   starts with capture off); `t/006` itself passed 9/9 on both Windows
   jobs and failed on Linux (Meson 32/64, Autoconf) and macOS, which is
   exactly what caught fix 2's fork-inheritance bug (see the "Fix 2"
   section above) — the node2 reload checks and the region-size check,
   which don't depend on any fork()ed process claiming a slot, passed on
   every platform in that run. Fix 2 has not yet had its own CI run as of
   this report.
3. Per WP2b's brief, the trace level (WP3) is explicitly out of scope for
   the reservation: server-side processes still only start tracing at
   the first reload with `capture = trace`, via the DSA path, same as
   the pre-existing gap for stats before this WP. Documented as a
   comment on `pwet_claim_fixed_slot()`'s caller.
4. **New, per the coordinator (item (d)):** the timing counters
   (`count`/`total_ns`/`max_ns`/histogram buckets, all `int64`) are
   copied by a reader with no synchronisation against the owning
   process's plain (non-atomic) writes to them — the claim protocol's
   barriers only order the *ownership* fields (`owner_pid`/`owner_start`)
   against the payload as a whole being torn by a *claim*, not against
   the owner concurrently incrementing an individual counter mid-read.
   On a 32-bit platform, a `int64` load/store is not guaranteed atomic,
   so a concurrent reader can in principle observe a torn 64-bit value.
   This is pre-existing on the DSA path too (same reader code, same lack
   of synchronisation with `pwet_wait_end()`'s writes) and not something
   this WP introduces or changes; per the coordinator, this will be
   called out in the documentation rather than fixed in code.
