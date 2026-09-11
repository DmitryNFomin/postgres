# WP2 report: contrib/pg_wait_event_tracing, statistics level (v8-0004)

Branch `wet-v8-wp2` off `wet-v7-rfc`, worktree
`<workspace>/wt-v8-wp2`. Build dir
`<workspace>/build-v8-wp2`
(`--buildtype=debugoptimized -Dcassert=true -Dwerror=true
-Dinjection_points=true`, `.venv-v7-rfc` meson/ninja, `ninja -j2` only).
The main checkout at `<workspace>/postgres`
stayed on `wet-series` throughout; nothing was pushed.

Four commits on `wet-v8-wp2` (M1, M2, M3, plus a review-fix commit added
after M2/M3 were reviewed):

| # | Commit | Subject |
|---|---|---|
| M1 | `7e518f5bb946ff5a8a0dadecdc9e7e3388a0e421` | pg_wait_event_tracing: port the statistics-level collector |
| M2 | `6311e336b2523877278ee55dd83a35c98d497bcb` | pg_wait_event_tracing: fix ownership, reset ACL, and reset race |
| M3 | `8ba6561d41027839b280b1ec1a1371acba8741a0` | pg_wait_event_tracing: add regression tests |
| fix | `f4bf69380b8a3fbf870bf2f1a68cf521140dfcaf` | pg_wait_event_tracing: fix review defects in M2/M3 |

`git diff --stat wet-v7-rfc..HEAD` touches only `contrib/**` (11 files,
1785 insertions, 0 deletions outside contrib). All four commits: author
`Dmitry Fomin <fomin.list@gmail.com>`, trailer
`Discussion: https://postgr.es/m/CAPHG-0mAOn05ae6Kqx1wHXxzOk4E5W7ajjd=QBhgkR7a0uyQmw@mail.gmail.com`.

The "fix" commit exists because the coordinator reviewed M2 and M3 while
this WP was still in progress and found three defects; rather than
rewrite history, the fixes landed as a fourth, separate commit on top
(the brief's own note says the owner will squash this into M2 when
assembling the series). Its content is described inline below, next to
the M2/M3 material it corrects.

## M1 — port and rename (`7e518f5bb94`)

**Diffstat:** 8 files changed, 1279 insertions(+) (all new files except
`contrib/Makefile`/`contrib/meson.build`, +1 line each).

**Kept from the package** (`contrib/pg_wait_event_timing/pg_wait_event_timing.c`,
applied via `git apply --include=... patches/04-timed-site-hook-vs-tested-master.patch`):
the sparse per-backend DSA payload strategy, the begin/end hook wiring
with chaining, the dense per-class flat array plus LWLock-tranche hash
for timing entries, and the histogram bucketing math
(`pwet_timing_bucket()`, unchanged and still matches v6's power-of-two
bucket boundaries exactly).

**Changed from the package:**
- Shared memory now goes through the DSM registry
  (`GetNamedDSMSegment("pg_wait_event_tracing", ...)` for a small,
  always-resident control segment; `GetNamedDSA("pg_wait_event_tracing_stats", ...)`
  for the per-backend payload area) instead of the package's
  `shmem_request_hook` + `RequestNamedLWLockTranche()` + `ShmemRequestStruct`/
  `RegisterShmemCallbacks` approach. That whole mechanism is gone — it isn't
  "dead code that happened to also be there", it's the *only* way the
  package obtained its LWLock tranche, so removing it required switching
  the control segment's own lock to an embedded `LWLock` initialized via
  `LWLockNewTrancheId()` inside the `GetNamedDSMSegment()` init callback
  (`pwet_control_init()`), matching how `GetNamedDSA()` sets up its own
  tranche internally.
- One unified `PwetSlot` array (`stats_ptr`, plus `trace_ptr`/`trace_state`
  reserved for WP3, plus a `generation` counter) instead of the package's
  separate timing/trace control arrays — WP3 will not need to reshape this
  segment.
- SQL surface renamed to v6's names/column sets: views
  `pg_stat_wait_event_timing`, `pg_stat_wait_event_timing_overflow`,
  `pg_wait_event_timing_histogram_buckets`; functions
  `pg_stat_get_wait_event_timing(pid)`, `pg_stat_get_wait_event_timing_overflow(pid)`,
  `pg_stat_reset_wait_event_timing(pid)`, `pg_stat_reset_wait_event_timing_all()`.
  Added the `backend_type` column v6 had and the package didn't. Package's
  `pg_wait_event_timing_state`/`_trace` SRFs dropped (no v6 equivalent for
  `_state`; `_trace` is WP3's).
- `_PG_init()` now errors (`ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE`,
  "pg_wait_event_tracing must be loaded via \"shared_preload_libraries\"",
  same wording as `pg_stat_statements`'s and `sepgsql`'s own message) if
  not preloaded, rather than silently returning.
- `before_shmem_exit()` registration stays lazy (inside
  `pwet_maybe_attach()`, on each backend's first attach), **not** moved
  into `_PG_init()` despite the brief's bullet phrasing suggesting that:
  `InitPostmasterChild()` calls `on_exit_reset()` early in every
  forked/exec'd backend, which would discard any registration made from
  `_PG_init()` running in the postmaster. This is unchanged from the
  package, which already had it right.

**Stripped for this commit** (statistics level only): the trace ring
writer, trace DSA, trace SRF, query markers and the
`post_parse_analyze_hook`/`ExecutorEnd_hook` bodies that wrote them, the
`trace_ring_size` GUC, the 32-byte trace record, `EnableQueryId()`. Saved
verbatim, with original line numbers and a to-do list for WP3, in
`<workspace>/wp3-trace-parts-from-package.c.txt`.
`post_parse_analyze_hook`/`ExecutorStart_hook` are kept, stripped to their
attach-triggering role only; the capture enum this WP defines is
`off`/`stats` only (`trace` is WP3's).

**Deliberately still buggy in M1** (matching the package/v6, so M2's diff
is clean and reviewable): `pg_stat_reset_wait_event_timing(pid)` used
`has_privs_of_role(GetUserId(), ROLE_PG_SIGNAL_BACKEND)` as its only ACL
check and fell back to `AuxiliaryPidGetProc()` (V6-4's shape); its
publish path bumped a `reset_generation` counter without re-checking
ownership under the lock (V6-5's shape). The reader SRFs didn't check
slot ownership at all, only `DsaPointerIsValid()` (would have been
V6-2's shape, though see note below).

**Smoke test reasoning (not executed — see Validation):** preload,
`CREATE EXTENSION`, `SET pg_wait_event_tracing.capture = stats`,
`pg_sleep(0.05)`, `SELECT * FROM pg_stat_wait_event_timing` shows one
PgSleep row, count 1, total near 50ms — traced through the code path by
hand (`pwet_wait_begin`/`pwet_wait_end` → `pwet_timing_index()` routes
`PG_WAIT_TIMEOUT | PG_SLEEP` to the Timeout class's dense slot) and
believed correct; M1's build was verified (see Validation), the SQL
smoke path itself was not run against a live server.

## M2 — the four fixes (`6311e336b25`, InjectionPoint-capacity part later corrected in the fix commit)

**Diffstat:** 3 files changed, 254 insertions(+), 63 deletions(-).

- **Fix 1 (sparse, V6-1) — verified, no code change:** every allocation
  path (`pwet_assign_capture()` when `IsNormalProcessingMode()`, else
  `pwet_post_parse_analyze()`/`pwet_ExecutorStart()` via
  `pwet_maybe_attach()` → `pwet_attach_stats()`) runs outside
  `pwet_wait_begin()`/`pwet_wait_end()`; the two hooks only touch
  `pwet_my_stats`, a preallocated pointer. The control table
  (`PwetSlot[MaxBackends + NUM_AUXILIARY_PROCS]`, ~24 bytes/slot) is the
  only thing always resident; the payload
  (`pwet_stats_payload_size(pwet_max_tranches)`, ~208 KiB at the default
  192) is DSA-allocated per backend only on actual capture.

- **Fix 2 (ownership, V6-2) — `pwet_check_reset_privileges` is unrelated;
  the actual mechanism is in `PwetSlot`/the two reader SRFs:** `PwetSlot`
  gained `owner_pid` (int) and `owner_start` (`TimestampTz`, cached
  `MyStartTimestamp`). `pwet_attach_stats()` publishes them next to
  `stats_ptr` under the control lock; `pwet_release_stats()` clears them
  (in addition to `dsa_free()`ing and invalidating the pointer, which the
  package already did on both capture-off and exit — see note below).
  `pg_stat_get_wait_event_timing()` and `..._overflow()` now compare
  `slot->owner_pid`/`owner_start` against
  `pgstat_get_beentry_by_proc_number(procnumber)->st_procpid`/
  `st_proc_start_timestamp` and skip the slot on a mismatch.
  *Note:* the package's release path already freed and invalidated
  `stats_ptr` on both `capture = off` and `before_shmem_exit`, so V6-2's
  literal failure mode (stale live data shown to a successor) was not
  reproducible against this specific package/M1 baseline — the plan
  itself says as much ("a successor with capture off never has a payload
  to be shown anyway"). Fix 2 is implemented as required, as a defense-in-
  depth invariant, not as a fix to an observed crash/data-leak in M1.

- **Fix 4 (reset ACL, V6-4):** `pwet_check_reset_privileges()` replicates
  `pg_signal_backend()`'s target-authorization rule (superuser-owned or
  role-less target needs superuser; else `has_privs_of_role()` of the
  target role or `pg_signal_backend`) — **without** the autovacuum-worker
  carve-out `pg_signal_backend()` has, since autovac workers are
  role-less and so already require superuser under the simpler rule; I
  judged this the more conservative choice for a function that erases
  diagnostic state, but it's a deviation from an exact line-by-line port
  of `pg_signal_backend()` worth the owner's confirmation. Auxiliary PIDs
  are rejected with `pg_signal_backend()`'s own wording ("PID %d is not a
  PostgreSQL backend process", via `ereport(WARNING, ...)`, not an error)
  by dropping the `AuxiliaryPidGetProc()` fallback —
  `BackendPidGetProc()` alone only resolves normal backends.
  `pg_stat_reset_wait_event_timing_all()` now hard-requires `superuser()`
  in C (fixed in the follow-up commit to actually be reachable — see
  "Review defects" below), not just the extension script's revocable
  `REVOKE EXECUTE FROM PUBLIC`.

- **Fix 5 (reset race, V6-5):** `reset_generation` moved from the DSA
  payload into `PwetSlot` (always resident), so a request can be
  published under the same control lock that guards ownership.
  `pg_stat_reset_wait_event_timing(pid)` resolves `BackendPidGetProc()`,
  runs the ACL check, then captures the target's
  `st_proc_start_timestamp` from its live `PgBackendStatus` entry;
  `pwet_request_reset()` takes the lock and bumps `reset_generation` only
  if `owner_pid`/`owner_start` still match — a successor that has since
  reused the ProcNumber is left alone. `pwet_wait_end()` now reads
  `reset_generation` from the control slot (lock-free atomic read; the
  owner is the sole reader) instead of the payload. An injection point
  `pg-wait-event-tracing-reset-before-publish` sits between resolution
  and taking the lock, for WP4's TAP test.

- **Capacity table (plan §3.1):** bumped Lock 16→32, Client 16→32,
  Timeout 16→32, IPC 64→128 in `pg_wait_event_tracing_data.h` — actual
  counts in `wait_event_names.txt` on this master are 12/9/11/64, so the
  old table left IPC with **zero** headroom and the other three under the
  required 8. Added `pwet_class_names[]` and
  `pg_wait_event_tracing_capacity()` (type, capacity — one row per dense
  class plus `LWLock` at `max_tranches`).
  **Removed in the follow-up commit** (coordinator review, see below):
  `pwet_check_class_capacities()`, a `_PG_init()`-time WARNING scan that
  cannot work as designed.

## M3 — regression tests (`8ba6561d410`, later corrected)

**Diffstat:** 5 files changed, 418 insertions(+).

`contrib/pg_wait_event_tracing/{sql,expected}/pg_wait_event_tracing.sql`/`.out`,
`pg_wait_event_tracing.conf` (`shared_preload_libraries = 'pg_wait_event_tracing'`,
following `contrib/pg_stat_statements`'s `--temp-config` pattern), and
`Makefile`/`meson.build` wiring (`REGRESS`/`REGRESS_OPTS`,
`NO_INSTALLCHECK = 1` / `runningcheck: false`, since the suite needs the
library preloaded).

Coverage: `SHOW` default, histogram-buckets view (32 rows, exact bucket
boundaries), enable + `pg_sleep` + row/overflow invariants
(`calls`/histogram-sum/`total_time_ms`/`max_time_us` consistency), the
`pg_stat_wait_event_timing` view with `backend_type`, an unknown pid
(zero rows), self-reset (`NULL` and no-arg forms) and its effect on the
view/`reset_count`, an unknown-pid reset (WARNING, no-op — see below),
disable-then-verify-the-row-is-gone (exercises fix 2's ownership check,
not just fix 1's sparse allocation), `_all()`'s superuser requirement
even when `EXECUTE` is explicitly granted to a non-superuser role, and
the capacity table's coverage/headroom versus `pg_wait_events`.

**Not run locally** — the hard rule for this WP is compile-only, no
server. `sql/expected` were authored by reasoning through exact query
semantics and psql's aligned-output formatting; I built a small formatter
script and calibrated it byte-for-byte against v6's own committed
`wait_event_timing.out` (same histogram-bucket view, same
boolean/count/pg_sleep-void-result shapes) before generating the new
content mechanically, including determining and confirming a formatting
rule (blank lines appear in `.out` only automatically after a printed
result block, never from source blank lines) from three cross-checked
core/contrib examples. **This still needs a real run on the fork's CI**;
I have no way to confirm it byte-for-byte without executing it.

## Review defects fixed (`f4bf69380b8`)

The coordinator reviewed M2 and M3 while this WP was in progress and
found three problems, all fixed in this commit:

1. **`pwet_check_class_capacities()` cannot work and was removed
   entirely**, along with its `_PG_init()` call. For built-in classes,
   the generated per-class name functions return "unknown wait event"
   rather than NULL for an out-of-range id (and `GetLockNameFromTagType()`
   likewise always returns a string), so the probe's
   `pgstat_get_wait_event(probe) != NULL` check is always true — every
   server start would have logged 7 spurious WARNINGs. For Extension/
   InjectionPoint it's worse: `pgstat_get_wait_event()` calls
   `GetWaitEventCustomIdentifier()`, which for ids at/past the custom
   base does `LWLockAcquire()` + `hash_search()` and `elog(ERROR)` when
   absent — and `_PG_init()` runs in the postmaster during
   `shared_preload_libraries` processing, before shared memory or
   LWLocks exist, so this would have **crashed postmaster startup**. The
   regression test comparing `pg_wait_events` against
   `pg_wait_event_tracing_capacity()` is unaffected and is now the sole
   enforcement.
2. **InjectionPoint capacity lowered 128→32** in
   `pg_wait_event_tracing_data.h` (injection points only exist in
   `--enable-injection-points` test builds); `PWET_NUM_EVENTS` 656→560,
   `pwet_class_offset[]` unaffected (InjectionPoint is the last dense
   class). **`pwet_stats_payload_size(192)` is now 212,664 bytes
   (~207.7 KiB)**, down from 239,544 (verified by compiling a standalone
   C program mirroring the exact struct layout — `sizeof(PwetTimingEntry)
   = 280`, `sizeof(PwetLWLockHashEntry) = 4`, `sizeof(PwetStats) =
   156,856` on this platform — not by hand arithmetic).
3. Two M3 defects: (a) the unknown-pid reset test's comment/expected
   output said "silent no-op", but M2's Fix 4 makes it emit
   `WARNING:  PID %d is not a PostgreSQL backend process` — fixed the
   comment and added the WARNING line to `expected/`. (b) removed the
   whole dblink-based cross-backend ACL section: the Makefile has no
   `EXTRA_INSTALL = contrib/dblink` (so `check-world` can't create the
   extension) and Windows CI's SSPI-based `pg_regress` can't authenticate
   as a freshly created role without `--create-role`. Those cases
   (pg_signal_backend member vs. ordinary target; non-superuser vs.
   superuser target) move to TAP test `t/003_reset_acl.pl` (WP4a); the
   regress test keeps only what needs no second connection — `_all()`'s
   superuser requirement.

## Open questions

1. **Fix 4's autovacuum-worker carve-out**: I omitted
   `pg_signal_backend()`'s `ROLE_PG_SIGNAL_AUTOVACUUM_WORKER` exception
   (autovac workers are role-less, so they already require superuser
   under my simpler rule). Confirm this is the intended, more
   conservative policy for a function that erases diagnostic state
   rather than signals a process.
2. **Auxiliary processes never reach an attach point** (raised by the
   coordinator, not implemented): there is no
   `post_parse_analyze_hook`/`ExecutorStart_hook` equivalent for
   auxiliary processes, and the GUC assign hook does not run in a forked
   aux child at startup — aux processes never execute SQL through the
   parse/executor pipeline at all. So an aux process's stats never
   populate even when capture is enabled cluster-wide before its fork.
   Giving aux processes a safe attach point (e.g., an explicit call from
   each aux process's main loop) is a design decision for the owner;
   this WP does not attempt it.
3. **The M3 regress test is unexecuted.** The dblink removal eliminates
   the highest-risk part, but the remaining file (in particular the
   exact blank-line/error-message formatting) still needs a real
   `meson test --suite pg_wait_event_tracing` run before it can be
   trusted.
4. Per the brief, WP3 needs `wp3-trace-parts-from-package.c.txt`'s
   guidance to move onto the `PwetSlot`/`GetNamedDSA()`-based design M1/M2
   established, which is a real (if mechanical) adaptation, not a
   drop-in of the saved code.

## Validation performed

- `meson setup build-v8-wp2 --buildtype=debugoptimized -Dcassert=true
  -Dwerror=true -Dinjection_points=true` — clean, no errors.
- `ninja -j2 contrib/pg_wait_event_tracing/pg_wait_event_tracing.so` —
  clean compile under `-Dwerror=true` after every commit (re-verified
  after the final fix commit too).
- `ninja -j2 headerscheck` and `ninja -j2 cpluspluscheck` — both pass,
  final state.
- `git diff --stat wet-v7-rfc..HEAD` — touches only `contrib/**`.
- **Not run** (hard rule: compile only, no server): `meson test` in any
  form, `pg_regress`, `initdb`/`pg_ctl`. No server process was started at
  any point in this session.
- Build dir used: only `build-v8-wp2`, `ninja -j2` throughout; peak
  footprint ~24 MiB (only this module's objects were built, not the full
  tree), well within the disk budget shared with the parallel WP1 build.
