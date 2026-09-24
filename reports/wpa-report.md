# WPA report: `contrib/pg_wait_event_tracing` module-side overhead work

Branch `wet-v11-wpa`, worktree `<workspace>/work/git/postgres_patch/wet-v11-wpa`,
based on local branch `wet-v11` (5 commits on `origin/master`, tip
`0cf1c2feaef "pg_wait_event_tracing: trace level"`).

## Commits (current, after both amendments)

1. `3100bbf8741` — **pg_wait_event_tracing: install the wait hooks lazily, per process** (A1,
   amended a second time — see "A1 amendment / rebase" below; was
   `28e55cafb4d`)
2. `5b243e4e8e2` — **pg_wait_event_tracing: one recording gate, process-local in-flight state** (A2,
   rebased onto the amended A1, tree content unchanged — was `027735cad29`,
   originally `b2c14ba08ec`; see "A2 amendment" and "A1 amendment / rebase"
   below)

Both authored/committed as `Dmitry Fomin <fomin.list@gmail.com>`, each with
a single `Discussion:` trailer, no other trailers. Verified with
`git log --format='%an <%ae>%n%cn <%ce>'` on both commits.

Patch files: `<workspace>/work/git/postgres_patch/v11/patches-wpa/0001-*.patch`,
`.../0002-*.patch` (from `git format-patch -2`).

## Build/test environment

```
cd <workspace>/work/git/postgres_patch/wet-v11-wpa
export PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig:/opt/homebrew/opt/openssl@3/lib/pkgconfig:/opt/homebrew/opt/readline/lib/pkgconfig
meson setup build --buildtype=debugoptimized -Dcassert=true -Dwerror=true -Dinjection_points=true -Dtap_tests=enabled -Dssl=openssl -Dldap=disabled -Dprefix=$PWD/install
ninja -C build && ninja -C build install
meson test -C build --suite setup --suite test_wait_hook --suite pg_wait_event_tracing --print-errorlogs
```
`ninja -C build install` was run after every source change (macOS/SIP
strips `DYLD_LIBRARY_PATH`, and libpq for the tests must come from the
install prefix). meson's own `tmp_install` test-staging copy needed the
`setup` suite (or a manual `meson install --only-changed --no-rebuild
--destdir=build/tmp_install`) to pick up SQL-file changes; this is
mentioned only because it cost a couple of iterations, not because
anything in the brief was wrong.

## Test results

**Base (before any commit), 14 tests:**
```
 1/14 setup                          - tmp_install            OK
 2/14 setup                          - install_test_files     OK
 3/14 setup                          - initdb_cache            OK
 4/14 pg_wait_event_tracing/001_memory                        OK   7 subtests
 5/14 pg_wait_event_tracing/002_ownership                     OK   9 subtests
 6/14 test_wait_hook/regress                                  OK   1 subtest
 7/14 pg_wait_event_tracing/005_orphan_reuse                  OK  10 subtests
 8/14 pg_wait_event_tracing/regress                           OK   2 subtests
 9/14 pg_wait_event_tracing/004_reset_race                    OK   2 subtests
10/14 pg_wait_event_tracing/003_reset_acl                     OK  27 subtests
11/14 pg_wait_event_tracing/010_trace_seqlock                 OK   5 subtests
12/14 pg_wait_event_tracing/006_server_processes              OK  10 subtests
13/14 pg_wait_event_tracing/011_trace_wrap                    OK  21 subtests
14/14 pg_wait_event_tracing/012_trace_markers                 OK   4 subtests
Ok: 14  Fail: 0
```

**After commit A1 (28e55cafb4d), 15 tests** (A1 adds `t/007_lazy_hooks.pl`,
registered in `meson.build`):
```
 1/15 setup: tmp_install, install_test_files, initdb_cache     OK  OK  OK
 4/15 test_wait_hook/regress                                   OK   1 subtest
 5/15 pg_wait_event_tracing/007_lazy_hooks                     OK   5 subtests
 6/15 pg_wait_event_tracing/001_memory                         OK   7 subtests
 7/15 pg_wait_event_tracing/regress                            OK   2 subtests
 8/15 pg_wait_event_tracing/002_ownership                      OK   9 subtests
 9/15 pg_wait_event_tracing/005_orphan_reuse                   OK  10 subtests
10/15 pg_wait_event_tracing/004_reset_race                     OK   2 subtests
11/15 pg_wait_event_tracing/003_reset_acl                      OK  27 subtests
12/15 pg_wait_event_tracing/010_trace_seqlock                  OK   5 subtests
13/15 pg_wait_event_tracing/006_server_processes               OK   9 subtests
14/15 pg_wait_event_tracing/012_trace_markers                  OK   4 subtests
15/15 pg_wait_event_tracing/011_trace_wrap                     OK  21 subtests
Ok: 15  Fail: 0
```
(006_server_processes: 9 or 10 subtests pass run-to-run — the test's own
per-backend-type checks are soft/SKIP-based on a slow runner, as its
in-file comment documents; the disjunction subtest that is the real
assertion always passed.)

**After commit A2 (b2c14ba08ec), same 15 tests, run 3 times in a row to
check for flakiness in the hot-path refactor (including the
`010_trace_seqlock` and `004_reset_race` race-sensitive tests):**
```
Ok: 15  Fail: 0   (run 1)
Ok: 15  Fail: 0   (run 2)
Ok: 15  Fail: 0   (run 3)
```

All runs' full logs are in `build/meson-logs/testlog.txt` inside the
worktree (overwritten each run; not preserved separately per the brief's
instructions, which asked only for the summaries here).

## A1: lazy per-process hook installation

Implemented per the brief: `pwet_wait_hooks_installed` flag +
`pwet_install_wait_hooks()`, called from `pwet_assign_capture()` when
`newval != PWET_CAPTURE_OFF`, before any attach logic. Hooks are never
uninstalled (comment explains why: a later chained consumer could be cut
out). `pwet_wait_begin()`/`pwet_wait_end()` bodies became
`pg_always_inline` `pwet_wait_begin_impl()`/`pwet_wait_end_impl()` taking
a `bool chain`; `pwet_wait_begin_nochain()`/`pwet_wait_end_nochain()` added
as the non-chaining wrappers. No duplicated logic.

The four scenarios were verified by reading the code (see the A1 commit
body for the full argument) and by `guc.c`'s `define_custom_variable()`
(the `hentry != NULL` / placeholder-exists branch) and its
`reapply_stacked_values()` recursion, which route a config-file value
through `set_config_option_ext()` into the assign hook before `_PG_init()`
returns — confirmed by reading `src/backend/utils/misc/guc.c` directly
(lines ~4854–4990 in this checkout), not inferred.

`t/006_server_processes.pl`'s two paths (capture set at postmaster start;
capture set later by reload) both still pass because a server-side
process's stats "attach" is `pwet_claim_fixed_slot()`, called from inside
`pwet_wait_begin()` itself — and by the time any process can wait on
anything with capture already on, the hooks are already installed (case
(c)/(d) in the commit body cover the two respective scenarios).

Added `pg_wait_event_tracing_hooks_installed()` (C function + SQL
declaration next to `pg_wait_event_tracing_capacity`, `GRANT EXECUTE ...
TO PUBLIC`), a doc `<varlistentry>` in "Other Functions", and a new
paragraph in "Loading the Module" describing the lazy-install behaviour.

Regression test `sql/pg_wait_event_tracing.sql` extended with three
`pg_wait_event_tracing_hooks_installed()` checks (fresh session: false;
after `SET ... = stats`: true; after `RESET`: still true) and
`expected/pg_wait_event_tracing.out` updated to match (copied from a
verified-passing actual-output run, to get `psql`'s column-width padding
exactly right).

New TAP test `t/007_lazy_hooks.pl`: two long-lived sessions on a node that
starts with capture off; session A enables stats and records a wait,
session B never does; B's `pg_wait_event_tracing_hooks_installed()` is
false and B has no rows in `pg_stat_wait_event_timing` while A does; a
subsequent `ALTER SYSTEM ... capture = stats; SELECT pg_reload_conf();`
then makes B report `true` too. Registered in `meson.build`'s `tap.tests`
list.

**Makefile**: not touched. `src/Makefile.global.in`'s `prove_check`/
`prove_installcheck` definitions already glob `t/*.pl` when `PROVE_TESTS`
is unset (which is `contrib/pg_wait_event_tracing/Makefile`'s case, since
it just sets `TAP_TESTS = 1`), so the new test file is picked up
automatically; adding an explicit per-file line would have had no
effect and there was nothing to add.

## A2: one recording gate, process-local in-flight state

**(a)** `pwet_rec_stats`/`pwet_rec_trace` added, maintained by one helper
`pwet_update_rec_pointers()`. Its doc comment states, and the code
implements, the formulas from the brief exactly, substituting
`pwet_capture_effective` for `pwet_capture` per the brief's instruction
(safe because the two values differ only during the assign hook's own
synchronous call chain — see the RULE comment on `pwet_capture_effective`
already in the file). Full list of the 17 assignment lines the helper is
wired to (grouped into 14 call sites where consecutive assignments in the
same function share one call) is in the A2 commit body; reproduced here
via:
```
grep -n 'pwet_stats_writes_disabled = \|pwet_trace_writes_disabled = \|pwet_my_stats = \|pwet_my_trace = \|pwet_capture_effective = ' contrib/pg_wait_event_tracing/pg_wait_event_tracing.c
```
```
1161  pwet_claim_fixed_slot():                              pwet_my_stats = payload;
1206  pwet_release_fixed_slot():                             pwet_my_stats = NULL;
1306  pwet_attach_trace():                                   pwet_my_trace = ts;
1367  pwet_release_trace() (nothing-to-release return):      pwet_my_trace = NULL;
1372-1373  pwet_release_trace() (main path):                 pwet_trace_writes_disabled = true; pwet_my_trace = NULL;
1391  pwet_release_trace() (restore on non-exit):            pwet_trace_writes_disabled = was_disabled;
1619  pwet_attach_stats():                                   pwet_my_stats = state;
1746  pwet_release_stats() (nothing-to-release return):      pwet_my_stats = NULL;
1751-1752  pwet_release_stats() (main path):                 pwet_stats_writes_disabled = true; pwet_my_stats = NULL;
1769  pwet_release_stats() (restore on non-exit):            pwet_stats_writes_disabled = was_disabled;
1778-1779  pwet_before_shmem_exit():                         pwet_stats_writes_disabled = true; pwet_trace_writes_disabled = true;
1806  pwet_assign_capture():                                 pwet_capture_effective = newval;
3188  pwet_orphan_trace() (nothing-to-orphan return):        pwet_my_trace = NULL;
3193  pwet_orphan_trace() (main path):                       pwet_my_trace = NULL;
```
`pwet_wait_begin_impl()`'s not-yet-attached case is restructured exactly
as specified: `if (pwet_rec_stats == NULL) { <original three-condition
guarded slow path, unchanged>; pwet_update_rec_pointers(); if
(pwet_rec_stats == NULL) return; }`. `pwet_wait_end_impl()`'s stats gate
became `state = pwet_rec_stats;` and its trace gate became `if
(pwet_rec_trace != NULL)`, using `pwet_rec_trace` in place of
`pwet_my_trace` inside that block.

**(b)** `PwetStats.wait_start`/`.current_event` moved to process-local
statics `pwet_wait_start`/`pwet_current_event`; struct fields removed.
Verified with `grep -n '\->wait_start\|\->current_event'` that no
cross-backend reader ever touched them (only `pwet_wait_begin_impl()`,
`pwet_wait_end_impl()`, `pwet_assign_capture()`'s reset, and
`pwet_reset_own()` did, all of which run only in the owning backend).
Confirmed no remaining `->wait_start`/`->current_event` references after
the change (grep comes back with only the explanatory comment, no code).
`t/001_memory.pl`'s checks are footprint inequalities (`< 4 MiB`, `<
512 KiB`, `<=` a prior measurement) against generous margins, never an
exact size, so the payload shrinking by `sizeof(instr_time) +
sizeof(uint32)` cannot regress them — confirmed by reading the test file.

## Comments rewritten and why

- `_PG_init()`: the four hook-install lines were replaced with a comment
  explaining hooks now install lazily (A1) — a stale comment there would
  have contradicted the code.
- `pwet_assign_capture()`: added a comment for the new
  `pwet_install_wait_hooks()` call explaining ordering relative to the
  existing "becoming value" comment (A1), and inline-adjusted the
  existing wait_start/current_event reset comment area for the A2(b)
  field move (no prose changed there, just the storage referenced).
- `PwetStats`'s doc comment: added a paragraph explaining why
  `wait_start`/`current_event` are no longer struct members (A2b), so the
  comment doesn't go stale relative to the field list right below it.
- Declaration-block comments for the new statics
  (`pwet_wait_hooks_installed`, `pwet_wait_start`/`pwet_current_event`,
  `pwet_rec_stats`/`pwet_rec_trace`) and the new functions
  (`pwet_install_wait_hooks()`, `pwet_update_rec_pointers()`) are new,
  not rewrites, but each states the invariant it's responsible for so a
  future patch has something to update instead of re-deriving it.
- The wait-record trace comment inside `pwet_wait_end_impl()` (previously
  explaining "gated on `pwet_capture` itself, not `pwet_capture_effective`,
  because ...") was rewritten for A2(a): the gate is now the
  `pwet_rec_trace` pointer, so the old comment's premise (a live
  three-condition test at that call site) no longer applies; replaced
  with a short pointer to `pwet_update_rec_pointers()`'s own comment,
  which now carries that reasoning.

## Anything left undone

Nothing from the brief. One judgment call, called out for visibility:
the Makefile registration item is a no-op given the existing `t/*.pl`
glob (explained above under A1); I did not add a redundant explicit
listing since `contrib/pg_wait_event_tracing/Makefile` has no
`PROVE_TESTS` line to extend and no other test file is listed there
either.

## A2 amendment (post-review fix)

Coordinator review found a real defect in the original A2
(`b2c14ba08ec`): `pwet_rec_stats`/`pwet_rec_trace` were derived from
`pwet_capture_effective`, but `pwet_wait_begin()`/`pwet_wait_end()` can
run *synchronously inside* `pwet_assign_capture()` itself, via
`pwet_maybe_attach() -> pwet_attach_stats()/pwet_attach_trace()`, both of
which take an LWLock (a timed wait). Before A2, the hot path tested the
*stored* `pwet_capture`, which guc.c does not update until after the
assign hook returns, so it never counted anything during the hook. A2,
by testing `pwet_capture_effective` (set at hook entry), would count
extra waits in three of the four live transitions: `off -> stats` and
`off/stats -> trace` would start recording mid-hook, the moment the new
payload/ring attached.

**Fix applied to `pwet_assign_capture()`** (per coordinator's exact
recipe): at entry, save the stored `pwet_capture` (`old_stored`) and
both writes-disabled flags; mask `pwet_trace_writes_disabled = true`
unconditionally and `pwet_stats_writes_disabled = true` only when
`old_stored == PWET_CAPTURE_OFF`, for the duration of the hook; set
`pwet_capture_effective` and call `pwet_update_rec_pointers()` as
before; restructured the function's single early return into an
`if (pwet_active && !pwet_exit_started) { ... }` block so there is one
exit path; on that exit, restore both flags to their saved values (or
leave them `true` if `pwet_exit_started`), and call
`pwet_update_rec_pointers()` once more. Verified the nesting against
`pwet_release_stats()`/`pwet_release_trace()`'s own internal
save/restore of the same two flags: each of those restores to "what it
saw on entry to itself" (our masked value), so our own final restore,
one level further out, correctly puts back the pre-hook value.

**Comments rewritten as part of this fix** (both were made stale by A2
and are now corrected, not just newly documented):
- Added a full explanation of the masking rule directly above the mask
  in `pwet_assign_capture()`.
- Rewrote `pwet_update_rec_pointers()`'s doc comment: it previously
  claimed `pwet_capture_effective == pwet_capture` "at every call to
  pwet_wait_begin_impl()/pwet_wait_end_impl()", which is false for a
  call happening inside the assign hook's own chain. Now states the
  equivalence in two parts (outside the chain: the two values are
  identical; inside it: `pwet_assign_capture()`'s masking is what keeps
  the answer correct).
- Rewrote the RECORDING-decision bullet in the pre-existing RULE comment
  on `pwet_capture_effective` (this bullet predates A2 but A2 had
  silently invalidated it without updating it): it claimed every
  recording-decision site, including the wait hooks, is "never invoked
  synchronously from inside pwet_assign_capture()". That remains true
  for `pwet_trace_write_marker()` and the parse/executor/utility/xact
  hooks, but not for `pwet_wait_begin()`/`pwet_wait_end()`; the comment
  now calls that out as the documented exception and points to the
  masking rule.

**Regress test** (`sql/pg_wait_event_tracing.sql`): added, immediately
after `SET pg_wait_event_tracing.capture = stats;` and before the first
`pg_sleep()`, a check that this backend has zero `LWLock`-type rows in
`pg_stat_wait_event_timing`. `expected/pg_wait_event_tracing.out`
updated from a verified-passing actual-output run.

**Base confirmation (required before trusting the new expectation)**:
checked out commit `0cf1c2feaef` (the tip of `wet-v11`, i.e. the base
this branch forked from) *detached* in the same `wet-v11-wpa` worktree
(the `wet-v11` branch name itself is checked out in a different
worktree, so a detached checkout of its commit hash was used instead —
never touched the `wet-v11` worktree or the main checkout). Rebuilt,
reinstalled, and ran a manual `initdb`/`pg_ctl start` temporary instance
(port 55432, a scratch `$TMPDIR`, not part of any test harness) with a
single psql session: `CREATE EXTENSION`, `SET debug_parallel_query =
off`, `SET pg_wait_event_tracing.capture = stats`, then the same
LWLock-count query. Result: **0**, confirming the check pins a
pre-existing invariant rather than a newly invented expectation. Server
stopped and the scratch data directory removed; then checked back out to
the `wet-v11-wpa` branch (verified `git status --porcelain` clean before
and after switching, and `git log` shows both commits still on top).

**Testing after the fix**: rebuilt, `ninja install`, and ran the
setup + test_wait_hook + pg_wait_event_tracing suites twice
(15/15 both times) immediately after the code/test changes, and twice
more after returning from the base-commit detour (15/15, then one run
showed a single flaky failure in `t/007_lazy_hooks.pl`'s
reload-turns-on-capture check — `poll`-free, single-shot
`query_safe()` right after `pg_reload_conf()`, most likely starved by
the CPU contention from the concurrent rebuild/temporary-instance work
in the base-commit check just before it; five subsequent runs, three in
isolation and two full-suite, all passed 15/15). This flake is in a
test written for A1 (already accepted), is unrelated to the A2 masking
fix itself (the failing assertion is about hook installation via
reload, not recording), and was not reproducible once system load
settled; noted here rather than silently ignored, per the instruction
to report rather than paper over anything uncertain. Not touched
further since A1 is already accepted and out of scope for this
amendment.

**Commit**: amended `b2c14ba08ec` in place with `git commit --amend`
(author/committer unchanged: `Dmitry Fomin <fomin.list@gmail.com>`,
single `Discussion:` trailer) to `027735cad29`, keeping the A2(a)/A2(b)
structure and the invariant-site list (updated with the new
`pwet_assign_capture()` mask/restore lines), replacing the equivalence
argument with the two-part (outside-chain / inside-chain-via-masking)
one above, and removing the false "effective == capture at every
hot-path call" claim. Patches in `patches-wpa/` regenerated (old files
deleted first, `git format-patch -2` rerun).

## A1 amendment / rebase (fixing the flake reported above)

The flake noted at the end of the "A2 amendment" section (a single
`is()` check right after `pg_reload_conf()` occasionally observing `'f'`
on session B) was confirmed by the coordinator to be a real, not merely
theoretical, race: `pg_reload_conf()` only asks the postmaster to signal
every backend with `SIGHUP` and returns immediately; each backend,
including session B's, applies the new configuration (running
`pwet_assign_capture()` again) only the next time it checks for
interrupts — in practice, the next command it processes — which is an
arbitrarily short but non-zero delay after `pg_reload_conf()` returns.
A single immediate `query_safe()` can therefore see the pre-reload value
on a slow or loaded CI runner.

**Fix** (in `contrib/pg_wait_event_tracing/t/007_lazy_hooks.pl`):
replaced the final `is(...)` with a bounded poll loop — up to
`10 * $PostgreSQL::Test::Utils::timeout_default` attempts,
`usleep(100_000)` between them, the same bound `Cluster.pm`'s
`poll_query_until()` uses — except the query runs via `query_safe()` on
session B's own long-lived `background_psql` handle (not a fresh
connection, since the installed-hooks flag is process-local to B and
`poll_query_until()` itself always opens a new connection per attempt).
Added `use Time::HiRes qw(usleep);`. A comment on the loop explains the
`SIGHUP`-timing reason. The earlier "B has no rows" check was left
alone, as instructed (it is a negative check taken before the reload,
so there is no analogous race for it to have).

**Amend + rebase procedure** (both required since A1 is not the branch
tip):
1. `git checkout --detach 28e55cafb4d` (A1) in the `wet-v11-wpa`
   worktree — a detached checkout, not a branch checkout, because the
   `wet-v11` branch itself is already checked out in its own separate
   worktree.
2. Applied the test fix, rebuilt, `ninja install`, refreshed
   `tmp_install`, and ran `t/007_lazy_hooks.pl` alone to confirm it
   passes before committing.
3. `git commit --amend` (author/committer `Dmitry Fomin
   <fomin.list@gmail.com>`) with the original A1 message plus one new
   paragraph describing the polling fix and its rationale, same single
   `Discussion:` trailer → new A1 = `3100bbf8741`.
4. `git rebase --onto 3100bbf8741 28e55cafb4d wet-v11-wpa` — replays A2
   (the only commit between old A1 and the branch tip) onto the amended
   A1. Rebase reported success with no conflicts (a clean cherry-pick,
   as required, since A2 never touches `t/007_lazy_hooks.pl`). New A2 =
   `5b243e4e8e2`.
5. Confirmed the rebase changed nothing in A2's own content:
   `git diff 027735cad29 5b243e4e8e2` shows only the A1-amendment diff
   to `t/007_lazy_hooks.pl` (present because it's now part of A1's
   tree, not because A2 changed), nothing else.

**Testing**: rebuilt, `ninja install`, refreshed `tmp_install`, and ran
the setup + test_wait_hook + pg_wait_event_tracing suites three times
in a row on the final rebased branch: **15/15 all three runs**, no
flakes, `t/007_lazy_hooks.pl`'s 5 subtests included every time.

**Patches**: `patches-wpa/*.patch` deleted and regenerated with
`git format-patch -2` against the final `3100bbf8741`/`5b243e4e8e2`
pair.

No push; only the `wet-v11-wpa` worktree was used (detached-checkout
step included) — the `wet-v11` branch's own worktree and the main
checkout were never touched.
