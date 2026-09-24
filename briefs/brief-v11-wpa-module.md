# WPA: module-side overhead work, `contrib/pg_wait_event_tracing`

Goal: two commits on a branch `wet-v11-wpa`, each a candidate fixup for
patch 0004 ("pg_wait_event_tracing: statistics level"). Both must leave the
set of recorded waits, their durations, histograms and trace records
byte-for-byte identical to the base. Performance only.

## Repository and rules

- Main repo `<workspace>/work/git/postgres` (checkout on
  REL_17_STABLE; never check it out or touch its tree). Base branch:
  `wet-v11` (local branch, five commits on origin/master).
- Create worktree:
  `git -C <workspace>/work/git/postgres worktree add -b wet-v11-wpa <workspace>/work/git/postgres_patch/wet-v11-wpa wet-v11`
- Never push. Never use bare `git stash`. Do not touch other worktrees.
- Commit metadata: author/committer `Dmitry Fomin <fomin.list@gmail.com>`
  (set GIT_AUTHOR_* and GIT_COMMITTER_* env), a real explanatory body, and
  the single trailer
  `Discussion: https://postgr.es/m/CAPHG-0mAOn05ae6Kqx1wHXxzOk4E5W7ajjd=QBhgkR7a0uyQmw@mail.gmail.com`.
  No Co-Authored-By, no Claude trailer, no other trailer. These commits go
  to pgsql-hackers.
- Follow PostgreSQL C style (tabs, brace on own line, comments in the
  existing voice of the file). Read the surrounding comments before
  editing: this file documents its invariants in long comments and every
  one you invalidate must be rewritten, not left stale.
- Building and running the module's tests locally IS allowed for this
  work package (regress + TAP start temporary servers; that is fine).

## Build and test commands

```
cd <workspace>/work/git/postgres_patch/wet-v11-wpa
export PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig:/opt/homebrew/opt/openssl@3/lib/pkgconfig:/opt/homebrew/opt/readline/lib/pkgconfig
meson setup build --buildtype=debugoptimized -Dcassert=true -Dwerror=true -Dinjection_points=true -Dtap_tests=enabled -Dssl=openssl -Dldap=disabled -Dprefix=$PWD/install
ninja -C build && ninja -C build install     # install is REQUIRED on macOS: SIP strips DYLD_LIBRARY_PATH, tests need libpq at the prefix
meson test -C build --suite setup --suite test_wait_hook --suite pg_wait_event_tracing --print-errorlogs
```
All 14 tests must pass before and after each commit (run once on the base
to confirm the environment, then after each commit).

## Commit A1: install the wait hooks lazily, per process

Today `_PG_init()` (near the end of the file) sets
`wait_event_begin_hook`/`wait_event_end_hook` unconditionally, so every
process in the cluster pays the indirect call on every timed wait even
when it never enables capture. The hook pointers are process-local
globals, so a process may install them for itself when it first needs
them.

Change:

1. Remove the four hook-installing lines from `_PG_init()`. Leave the
   `prev_*` variables and the shmem/parse/executor hooks as they are.
2. Add `static bool pwet_wait_hooks_installed;` and a function
   `pwet_install_wait_hooks(void)` that, if not yet installed in this
   process, saves the previous pointers, installs, and sets the flag.
   Install the non-chaining variant (item 4) when the saved previous
   pointer is NULL, otherwise the chaining variant.
3. Call `pwet_install_wait_hooks()` from `pwet_assign_capture()` whenever
   `newval != PWET_CAPTURE_OFF`, before any attach logic runs. Never
   uninstall anywhere (write the reason in a comment: a later consumer may
   have saved our pointer as its previous hook; removing ourselves would
   cut it out of the chain).
4. Non-chaining variants: turn the bodies of `pwet_wait_begin()` and
   `pwet_wait_end()` into `static pg_always_inline` helpers taking a
   `bool chain` parameter (or equivalent), and provide two thin exported
   wrappers each: `pwet_wait_begin`/`pwet_wait_begin_nochain` and the same
   for end, so the previous-hook NULL test disappears from the hot path
   when there is nothing to chain. No duplicated logic.
5. Verify the four scenarios by reading the code and say so in the commit
   body: (a) capture off in postgresql.conf, nobody enables: no process
   installs; (b) `SET pg_wait_event_tracing.capture = stats` in a session:
   that session installs inside the SET; (c) capture non-off in
   postgresql.conf: the postmaster installs during `_PG_init()` because
   `DefineCustomEnumVariable()` applies the configured value through the
   assign hook, and children inherit; on EXEC_BACKEND every child re-runs
   `_PG_init()` and the assignment; (d) reload turning capture on: every
   process installs from its own assign hook. Confirm (c) by reading
   guc.c's `define_custom_variable()`/`reapply_stacked_values` path and
   cite the function you relied on.
6. Diagnostic SQL function `pg_wait_event_tracing_hooks_installed()`
   returning boolean for the calling backend (are the wait hooks
   installed in this process). Add to `pg_wait_event_tracing--1.0.sql`
   next to `pg_wait_event_tracing_capacity`, C implementation, and one
   `<varlistentry>` in `doc/src/sgml/pgwaiteventtracing.sgml` in the
   "Other Functions" list. Grant EXECUTE to PUBLIC (it reveals nothing).
7. Tests: extend the existing regress test `sql/pg_wait_event_tracing.sql`
   (and its expected output) with: hooks_installed is false in a fresh
   session with capture off; true after `SET ... = stats`; still true
   after `SET ... = off`. Add a TAP test `t/007_lazy_hooks.pl` with two
   sessions: A enables stats and records a wait, B never enables; B's
   `pg_wait_event_tracing_hooks_installed()` is false and B has no rows in
   `pg_stat_wait_event_timing` while A has; then a reload with capture =
   stats makes B report true. Register the test in `meson.build` and
   `Makefile`.
8. Docs: one paragraph in the module page's loading/overview section
   saying that a process installs the hooks the first time capture is
   non-off in that process and keeps them installed, so backends that
   never enable capture run with the hooks absent.

Also read `t/006_server_processes.pl` and the comments around
`pwet_claim_fixed_slot()`: server-side processes claim their slot from the
begin hook, which now exists only after the assign hook installed it. Both
paths in 006 (capture set at start; capture set by reload) must still pass
and you must explain in the commit body why they do.

## Commit A2: one recording gate and process-local in-flight state

Two refactors, no behaviour change.

(a) Gate. `pwet_wait_begin()` tests `pwet_capture`, `pwet_stats_writes_disabled`
and `pwet_my_stats`; `pwet_wait_end()` tests the same three and, for
trace, `pwet_capture == TRACE`, `pwet_trace_writes_disabled`,
`pwet_my_trace`. Introduce two backend-local pointers:

- `pwet_rec_stats`: equals `pwet_my_stats` when
  `pwet_capture_effective != OFF && !pwet_stats_writes_disabled &&
  pwet_my_stats != NULL`, else NULL;
- `pwet_rec_trace`: equals `pwet_my_trace` when
  `pwet_capture_effective == TRACE && !pwet_trace_writes_disabled &&
  pwet_my_trace != NULL`, else NULL.

Maintain them with ONE helper `pwet_update_rec_pointers(void)` called at
every site that assigns any of the six inputs. Enumerate those sites with
`grep -n 'pwet_stats_writes_disabled =\|pwet_trace_writes_disabled =\|pwet_my_stats =\|pwet_my_trace =\|pwet_capture_effective ='`
and list every line in the commit body. Note `pwet_capture` itself is
stored by guc.c after the assign hook returns; the helper must use
`pwet_capture_effective` (already the "becoming" value; read the RULE
comment on it) and the assign hook must call the helper after updating
it. The hot paths then test only the rec pointer in the attached case.
The not-attached case in `pwet_wait_begin()` (fixed-slot claim when
`pwet_my_stats == NULL` and capture is on) must keep working: structure it
as `if (pwet_rec_stats == NULL) { existing slow logic guarded by the
original three conditions; return unless it attached and updated the
pointer; }`.

(b) In-flight state. `PwetStats.wait_start` and `PwetStats.current_event`
are written by the owning backend only and read by nobody else (verify
with grep and state it). Move them to process-local statics
`pwet_wait_start` and `pwet_current_event`; remove the two struct fields;
update every use (assign hook reset, begin, end, release paths). Confirm
nothing computes the payload size from those fields in a way that changes
observable memory bounds checked by `t/001_memory.pl`.

Commit body: state explicitly that the set of recorded waits is unchanged
and why (the rec pointer is non-NULL exactly when the three conditions
held before), and list the invariant sites.

## Deliverable

`<workspace>/work/git/postgres_patch/v11/reports/wpa-report.md`:
commit hashes and subjects; test results before/after each commit (the
14-line summary from meson test); the list of invariant sites for A2; any
comment you rewrote and why; anything left undone. Also
`git format-patch -2 -o <workspace>/work/git/postgres_patch/v11/patches-wpa/`.
Final chat message at most 8 lines.
