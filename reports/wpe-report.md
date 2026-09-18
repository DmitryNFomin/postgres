# WPE: fold WPA and WPB into the v11 series, publish to the fork

## Inputs confirmed before starting

```
$ git log --oneline wet-v11..wet-v11-wpb
f4a9d7d2ccd Move timed wait-event hook path out of line, hint the null-hook branch

$ git log --oneline wet-v11..wet-v11-wpa
5b243e4e8e2 pg_wait_event_tracing: one recording gate, process-local in-flight state
3100bbf8741 pg_wait_event_tracing: install the wait hooks lazily, per process
```

Base: `origin/master` at `311df1dc0392f06973cf98eac51d63cb007267ce` (unchanged
from the 311df1dc039 base named in the brief; no rebase needed). Old
`wet-v11` tip tagged as `wet-v11-pre-fold` = `0cf1c2feaefb749ea0d13886f55db5a75919026f`.

## Final five-commit series (base 311df1dc039)

| # | Hash | Subject |
|---|---|---|
| 0001 | `efcdfa704ac39791d81b489f0879f2a2cd76c435` | Add begin/end hooks for timed wait events |
| 0002 | `1a8fd426bd83f3a0020353f974803ae2b8766d87` | Convert wait_start/end call sites to the timed pair |
| 0003 | `e783c12a17169f63080f6bf7362056ebdfddd900` | test_wait_hook: test module for the wait event hook contract |
| 0004 | `b847d7c01531f797e9a7613ee407465b9e887a34` | pg_wait_event_tracing: statistics level |
| 0005 | `96c24a28006fb6ff66264998c698f5230d32ad26` | pg_wait_event_tracing: trace level |

0001 = old 0001 + wpb folded in (message's last paragraph replaced with wpb's
condensed explanation). 0002 and 0003 unchanged in content. 0004 = old 0004 +
A1 + A2 folded in (message got two new paragraphs). 0005 unchanged in content
except the conflict resolutions below.

Patch-id of 0002 (must start `ee70fa0e47ff2796`):

```
$ git show 1a8fd426bd8 | git patch-id --stable
ee70fa0e47ff2796255d34f29d3bf6028ace4a38 1a8fd426bd83f3a0020353f974803ae2b8766d87
```

All five commits: author/committer `Dmitry Fomin <fomin.list@gmail.com>`, one
`Discussion:` trailer each, no other trailer (verified with
`git interpret-trailers --parse` on each commit body).

## Build method

Non-interactive: cherry-picked 0001, then `f4a9d7d2ccd` (wpb) with
`--no-commit` and amended into one commit; cherry-picked 0002, 0003 as-is;
cherry-picked 0004, then A1 (`3100bbf8741`) and A2 (`5b243e4e8e2`) each with
`--no-commit`, resolving conflicts, squashed into one 0004 commit with the
combined message; cherry-picked 0005 (`0cf1c2feaef`) with conflicts resolved.
(An intermediate slip left a stray duplicate 0004 commit in the chain after a
`git reset --soft` targeted the wrong point; caught by the six-commit `git
log` and fixed by rebuilding the last two commits with `git commit-tree`
against the correct parent before finalizing `wet-v11`.)

## Conflicts and resolutions

**wpb into 0001**: no conflict — wpb only touches `wait_event.h`/`wait_event.c`,
which nothing else in the series between 0001 and wpb's original position
touches.

**A1 onto 0004** (3 conflicting files):
- `contrib/pg_wait_event_tracing/meson.build`: A1's diff carried unrelated
  context (0005's `t/010_trace_seqlock.pl` etc., not yet present at the 0004
  stage) alongside its real addition (`t/007_lazy_hooks.pl`); kept only the
  real addition, deferring the trace test entries to the 0005 reapply.
- `pg_wait_event_tracing--1.0.sql`: same pattern — A1's hunk carried all of
  0005's trace-level SQL as unrelated context around its one real addition
  (`pg_wait_event_tracing_hooks_installed()`); kept only that function.
- `pg_wait_event_tracing.c`: three conflict blocks, all the same pattern —
  A1's diff hunks include large stretches of 0005's trace code as context
  because A1 was originally written on top of the full (0001-0005) tip. Kept
  only A1's actual additions (`pwet_wait_hooks_installed`, the
  `pwet_wait_begin`/`pwet_wait_end` chain/no-chain wrapper split,
  `pwet_install_wait_hooks()`, the `_PG_init()` comment swap), deferring
  every trace-only function (`pwet_attach_trace`, `pwet_release_trace`, the
  trace SQL functions, etc.) to the 0005 reapply.

**A2 onto (0004+A1)** (1 conflicting file, `pg_wait_event_tracing.c`, 10
conflict blocks, same root cause as A1's — A2 was also written against the
full tip): kept A2's stats-only content (`pwet_rec_stats`,
`pwet_update_rec_pointers()` narrowed to the stats formula, the
`pwet_wait_begin_impl`/`pwet_wait_end_impl` gate-pointer refactor, the
`pwet_assign_capture()` masking narrowed to `pwet_stats_writes_disabled`
only, and every `pwet_update_rec_pointers()` call site that exists at the
stats-only stage: `pwet_claim_fixed_slot`, `pwet_release_fixed_slot`,
`pwet_attach_stats`, `pwet_release_stats` x3, `pwet_before_shmem_exit`,
`pwet_assign_capture` x2, plus the wait-begin slow-path recompute), deferring
every trace-gated branch (`pwet_rec_trace`, the trace masking half of
`pwet_assign_capture`, the trace record-append block in
`pwet_wait_end_impl`, the idle-marker synthesis in `pwet_wait_begin_impl`)
to the 0005 reapply. Also caught and reverted a case where git's 3-way merge
silently (no conflict marker) inserted the two-level version of
`pwet_update_rec_pointers()` referencing not-yet-existing trace symbols;
narrowed it back to stats-only by hand.

**0005 reapply onto (0004+A1+A2)** (8 conflict blocks in `pg_wait_event_tracing.c`,
plus small mechanical ones in `meson.build` and `--1.0.sql`): every block
was a union of "our stats/A1/A2 addition" + "0005's trace addition" at the
same anchor point (`PG_FUNCTION_INFO_V1` list, the `pwet_wait_start`/
`pwet_rec_stats` static declarations, `pwet_claim_fixed_slot`'s exit-callback
registration, `pwet_before_shmem_exit`, `pwet_assign_capture`'s mask/unmask
and release-trace call, `pwet_wait_begin`/`pwet_wait_end` wrapper bodies vs.
the pre-A1 monolithic bodies, the trace record-append and idle-marker
blocks, and the tail SQL-function set). Resolved each as a union, then
manually re-added the A2-only content that had been deferred in the previous
step (the `pwet_rec_trace` declaration and its share of the
`pwet_update_rec_pointers()`/RULE-comment text, the trace half of the
`pwet_assign_capture()` mask, the trace-append block using `pwet_rec_trace`,
the idle-marker call inside `pwet_wait_begin_impl`, and
`pwet_update_rec_pointers()` calls in `pwet_attach_trace`/`pwet_release_trace`
x3/`pwet_orphan_trace` x2) — these are exactly the sites A2's own commit
message enumerates. All conflicts were mechanical unions/deferrals; none
required a judgment call beyond "which side belongs at this history point."

**Work-package tags removed** (grepped for `adyen`, `Claude`, `WPA`, `WPB`,
`fixup`, and by extension similar internal-only references found while
resolving conflicts):
- `sql/pg_wait_event_tracing.sql` + `expected/pg_wait_event_tracing.out`:
  `"(WPA fixup a1)"` and `"(fix a1)"` dropped from two comments (kept the
  "Lazy, per-process hook installation" / "Hooks, once installed, are never
  removed" wording).
- Same two files: `"(WPA fixup a2)"` dropped from the "Pin the
  recording-gate equivalence" comment.
- `t/007_lazy_hooks.pl`: `"(WPA fixup a1)"` dropped from the file header
  comment.
- `pg_wait_event_tracing.c`: `"the coordinator flagged on WP2b"` (pre-existing
  in 0005 itself, not from A1/A2) reworded to "the usual postmaster/fork
  trap"; `"The docs WP documents this trade-off"` (also pre-existing in
  0005) reworded to "The documentation notes this trade-off".

## Empty-diff proof

Built a scratch branch from `wet-v11-pre-fold` with `wpb`, `A1`, `A2`
cherry-picked on top unmodified, then diffed it against the new `wet-v11`
tip. Result: only the work-package-tag text listed above differs; no other
line changed:

```
$ git diff scratch-verify wet-v11 -- .
 contrib/pg_wait_event_tracing/expected/pg_wait_event_tracing.out | 6 +++---
 contrib/pg_wait_event_tracing/pg_wait_event_tracing.c            | 8 ++++----
 contrib/pg_wait_event_tracing/sql/pg_wait_event_tracing.sql      | 6 +++---
 contrib/pg_wait_event_tracing/t/007_lazy_hooks.pl                | 4 ++--
 4 files changed, 12 insertions(+), 12 deletions(-)
```
(all 12+12 lines are exactly the tag removals above.)

## Build and test

`meson setup` reused the existing `build/` directory (already configured with
`werror=true`, `ldap=disabled`). `ninja -C build`: clean, no warnings/errors
(308 targets). `ninja -C build install`: clean.

`meson test -C build --suite setup --suite test_wait_hook --suite pg_wait_event_tracing`:

```
Ok:                15
Fail:              0
```
15/15 as expected (3 setup + 1 test_wait_hook regress + 11 pg_wait_event_tracing
suites: 007_lazy_hooks, 001_memory, 002_ownership, 005_orphan_reuse, regress,
004_reset_race, 003_reset_acl, 010_trace_seqlock, 006_server_processes,
012_trace_markers, 011_trace_wrap).

## format-patch / git am round trip

Deleted old `v11-000*.patch` files, regenerated with:
`git format-patch -v11 -5 -o /Users/dmitryfomin/work/git/postgres_patch/v11/patches-v11/`.

Applied with `git am` onto a throwaway branch (`am-roundtrip`) from
`origin/master`:

```
Applying: Add begin/end hooks for timed wait events
Applying: Convert wait_start/end call sites to the timed pair
Applying: test_wait_hook: test module for the wait event hook contract
Applying: pg_wait_event_tracing: statistics level
Applying: pg_wait_event_tracing: trace level
```

Tree hash comparison:

```
am-roundtrip tree: 58ee61e8a40127e337a444a35bec231b3db8c734
wet-v11 tree:      58ee61e8a40127e337a444a35bec231b3db8c734
```

Identical — clean round trip.

## Publish

```
$ git push git@github.com:DmitryNFomin/postgres.git wet-v11:wet-v11
 * [new branch]              wet-v11 -> wet-v11

$ git push git@github.com:DmitryNFomin/postgres.git wet-v11:ci/wet-v11
 * [new branch]              wet-v11 -> ci/wet-v11
```

(HTTPS `fork` remote had no usable credential in this environment —
`could not read Username for 'https://github.com'` — so both pushes used the
equivalent `git@github.com:DmitryNFomin/postgres.git` SSH URL, which the
environment's SSH key already authenticates against; no other remote or
branch was touched.)

No other branch was pushed.

## CI run

```
$ curl -s "https://api.github.com/repos/DmitryNFomin/postgres/actions/runs?branch=ci/wet-v11"
```
Run id 35342207165, head_sha `96c24a28006fb6ff66264998c698f5230d32ad26`
(matches the pushed `ci/wet-v11` tip), status `in_progress` at query time.

URL: https://github.com/DmitryNFomin/postgres/actions/runs/35342207165
