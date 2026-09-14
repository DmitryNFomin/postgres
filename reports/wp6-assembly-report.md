# WP6 (part 1): v8 series assembly report

Branch `wet-v8` built in worktree `<workspace>/wt-v8`,
rebased onto `upstream/master` at `0c5d6269614` ("Fail REPACK in presence of
!indisready indexes"). Main checkout untouched, still on `wet-series`. Nothing
pushed.

## The five commits

| # | Commit (final, after corrections) | Subject | Files | Insertions/Deletions |
|---|---|---|---|---|
| 1 | `0df248c07fe` | Add begin/end hooks for timed wait events | 2 | +63 |
| 2 | `d157435f39b` | Convert wait_start/end call sites to the timed pair | 42 | +205/-205 |
| 3 | `f21149ca333` | test_wait_hook: test module for the wait event hook contract | 11 | +791 |
| 4 | `7792f16ace6` | pg_wait_event_tracing: statistics level | 21 | +4862/-1 |
| 5 | `79cbeaaf356` | pg_wait_event_tracing: trace level | 12 | +2717/-57 |

(Commit hashes above are the *final* ones, after the two post-review corrections in
"Post-assembly corrections" below; the diffstats for patches 4 and 5 include the
7 and 4 `typedefs.list` lines added there respectively.)

Patches 1 and 2 are exactly v7-0001/v7-0002's content (author, message, and
diff unchanged); only their parent commit changed because of the rebase.
Author on all five: `Dmitry Fomin <fomin.list@gmail.com>`. None of the five
carry a `Co-Authored-By` or Claude/session trailer; each ends with only:
`Discussion: https://postgr.es/m/CAPHG-0mAOn05ae6Kqx1wHXxzOk4E5W7ajjd=QBhgkR7a0uyQmw@mail.gmail.com`.

`git diff --stat upstream/master..wet-v8` touches exactly the file set the
brief specified: `src/include/utils/wait_event.h`,
`src/backend/utils/activity/wait_event.c`, the 42 converted call-site files
(counted and confirmed = 42), `src/test/modules/test_wait_hook/**` and
`src/test/modules/{Makefile,meson.build}`, `contrib/pg_wait_event_tracing/**`
and `contrib/{Makefile,meson.build}`, and
`doc/src/sgml/{pgwaiteventtracing,contrib,filelist,xfunc}.sgml` — plus one
file outside that original allowlist, `src/tools/pgindent/typedefs.list`,
added by the post-review correction below (11 lines, a standard, expected
touch for any patch introducing new struct typedefs). 82 files total,
+8581/-206. Nothing else is touched.

## How patches 4 and 5 were built

The input branches do not form the simple wp2->wp2b->wp3->wp4b chain plus two
side branches that a first read suggests: `wet-v8-wp1` (test_wait_hook) and
`wet-v8-wp2` both branch directly from `wet-v7-rfc`'s tip, *not* from each
other. `wet-v8-wp4a` branches from `wet-v8-wp2` (before wp2b), and
`wet-v8-wp5` branches from `wet-v8-wp3` (before wp4b). This was confirmed with
`git merge-base --is-ancestor` before doing anything else.

Rather than an interactive rebase with 31 pick/fixup lines spanning four
divergent lineages (which risks silently duplicating content in the two
small files three of the five lineages all touch), each of patch 4 and patch
5 was assembled by taking each file from the tip of whichever branch last
touched it (`git checkout <tip> -- <path>`), since every branch's tip already
contains its own predecessors' commits squashed into one net tree state:

- Patch 4's `pg_wait_event_tracing.c`, `--1.0.sql` base, `.control`, `.conf`,
  `_data.h`, the stats regress test, and `t/006_server_processes.pl` all came
  from `wet-v8-wp2b`'s tip (already the net result of wp2's 6 commits + wp2b's
  5). `t/001-004_*.pl` came from `wet-v8-wp4a`'s tip. The whole documentation
  page, `xfunc.sgml`, and the `contrib.sgml`/`filelist.sgml` registration came
  from `wet-v8-wp5`'s tip (see "Docs split" below for why the whole page).
- Patch 5's `pg_wait_event_tracing.c` and `t/006_server_processes.pl` came
  from `wet-v8-wp4b`'s tip (already wp3's 5 commits + wp4b's 4, and wp4b never
  touches the docs or the files wp4a/wp5 add). The trace regress test files
  came from `wet-v8-wp3`'s tip (unchanged since). `t/005,010,011,012` came
  from `wet-v8-wp4b`'s tip.

Two files needed hand-merging because two divergent lineages both edited
them from the same starting point (this is the "every conflict resolved"
section the brief asks for — git itself raised no `am`/`rebase` conflicts
here since these were plain `checkout --` overlays, not applies, but the
files could not simply be taken from one side):

1. **`contrib/pg_wait_event_tracing/Makefile`**: `wet-v8-wp4a`'s own fix
   commit (`53888ba66f2`) added `TAP_TESTS = 1` (already present via wp2b's
   `f2079090fe2`) plus `EXTRA_INSTALL = src/test/modules/injection_points` and
   `export enable_injection_points`. `wet-v8-wp4b`'s fix commit
   (`76080e7f5d1`) added the *identical* two `EXTRA_INSTALL`/`export` lines
   independently (its own commit message says so explicitly: "matching
   WP4a"). Net result: wp4b's tip Makefile already has everything wp4a's
   lineage needed, byte-for-byte, except `REGRESS = pg_wait_event_tracing`
   vs. `... pg_wait_event_tracing_trace` (wp3's own addition). Patch 4 uses
   wp2b's Makefile plus the `EXTRA_INSTALL`/`export` lines by hand; patch 5's
   Makefile is wp2b's Makefile plus those two lines plus the `_trace` REGRESS
   suffix — verified byte-identical to wp4a's and wp4b's own tip files where
   they overlap.
2. **`contrib/pg_wait_event_tracing/meson.build`**: both `wet-v8-wp4a` and
   `wet-v8-wp2b`'s `f2079090fe2` added a `'tap': {...}` key to the same
   `tests` dict from the same wp2 base. Hand-merged into one `'tap'` block:
   `'env'` (from wp4a, needed by `t/004` and later by `t/010`) plus a
   `'tests'` list in numeric order (`001,002,003,004` in patch 4;
   `005,006,010,011,012` appended in patch 5). Also merged the `'regress'`
   block's `sql` list (`pg_wait_event_tracing` then, in patch 5,
   `pg_wait_event_tracing_trace`, from wp3).

No other file is touched by more than one lineage.

## SQL file ordering

`pg_wait_event_tracing--1.0.sql` is built the same way logically: patch 4
ends with the two `GRANT` lines for the histogram-buckets view and the
capacity function (from wp5's `523c16f2389`/`82cc5367b57`, which only ever
reference stats-level objects); patch 5 inserts the trace-level SQL (from
wp3) between the stats content and that grant block, so the final file has
grants after both levels' objects exist, matching the file's real, cherry-
picked-onto-wp4b shape.

One trivial squash-cleanup, noted per the brief's "do not preserve the
correction history" instruction: `wet-v8-wp5`'s real history left a stray
double blank line at that spot (`523c16f2389` inserted the grant block there
originally with its own blank-line padding; `82cc5367b57` moved the block to
the file's end but left one of those blank lines behind). This assembly
does not reproduce that leftover blank line — deliberate, not an oversight.

## Docs split decision

The whole `doc/src/sgml/pgwaiteventtracing.sgml` (contributed by wp5, 1516
lines) is in **patch 4**, undivided, per the brief's explicit fallback. The
split is not natural: the page's single "Views and Functions" `sect2`
interleaves stats-level and trace-level objects (e.g. its "Other Functions"
subsection documents `pg_stat_reset_wait_event_timing[_all]` — stats — next
to `pg_stat_clear_orphaned_wait_event_rings()` — trace — in the same
`<variablelist>`, with a cross-reference from the stats section to
`pgwaiteventtracing-trace-ring`). Splitting would mean rewriting this
section's structure, not just cutting along an existing seam, so the whole
page (and `xfunc.sgml`, `contrib.sgml`, `filelist.sgml` registration, per the
brief) goes into patch 4. Trade-off: a reviewer applying only patches 1-4
sees documentation for trace-level views/functions that do not exist yet in
that state's SQL script. This is a documentation-only inconsistency (no
build or test impact) and is called out here as instructed.

## Tree-equality check

Built independently via a scratch git index (`GIT_INDEX_FILE`, no working-
tree checkout) overlaying `wet-v8-wp4b`'s tree with `wet-v8-wp1`'s tree at
its own path, `wet-v8-wp4a`'s and `wet-v8-wp5`'s unique files, and the same
hand-merged `meson.build`, all via `git update-index --cacheinfo` +
`git write-tree` (no filesystem writes). Result compared against the
pre-rebase HEAD tree with `git diff <ref-tree> HEAD`:

```
contrib/pg_wait_event_tracing/pg_wait_event_tracing--1.0.sql | 1 -
1 file changed, 1 deletion(-)
```

The single line is exactly the documented stray-blank-line cleanup above.
Everything else — all 20 other patch-4/5 files, the merged Makefile/
meson.build, the doc pages — is byte-identical to a literal
wp4b+wp1+wp4a+wp5 composition. Re-checked after the rebase onto
upstream/master (`git diff <old-tree> wet-v8 -- <patch-4/5 paths>`): empty,
confirming the rebase did not alter any of this content (the rebase only
touched patch 1/2's 44 files, which is where upstream's own 51 intervening
commits landed).

## Rebase onto upstream/master

`git rebase upstream/master` (fetched immediately before, tip `0c5d6269614`)
completed with **zero conflicts** across all 5 commits. Confirmed the 44
files patches 1+2 touch are otherwise picking up genuine unrelated upstream
changes correctly (e.g. `Oid` -> `Oid8` widening in `xlog.c`/
`xlogrecovery.c`, a new `primaryFlushWakeupPending` mechanism, a
`copyfromparse.c` speculative-read change) with no interaction with the
wait-event hook conversions themselves.

## Build validation (compile only, no server started)

Build dir `<workspace>/build-v8`,
`meson setup --buildtype=debugoptimized -Dcassert=true -Dwerror=true
-Dinjection_points=true`, ninja/meson from `.venv-v7-rfc/bin`:

- `ninja -j2` at the final commit (`wet-v8` tip, all 5 patches applied):
  **all 1898 targets built clean**, including
  `contrib/pg_wait_event_tracing/pg_wait_event_tracing.so` and
  `src/test/modules/test_wait_hook/test_wait_hook.so`, under `-Dwerror=true`.
- `ninja headerscheck`: clean, no output.
- `ninja cpluspluscheck`: clean, no output.
- Patch 1 alone (pre-correction tip `2e97029fbfa`, same diff as the final
  `0df248c07fe` — the correction only reworded the message) and patch 1+2
  alone (`ee902a25304`, same diff as final `d157435f39b`): each checked in
  its own detached throwaway worktree (`wt-v8-check1`/`wt-v8-check2`) with
  its own build dir, same meson flags. Both `meson setup` and `ninja -j2`
  completed with **1894/1894 targets, exit 0, zero errors and zero
  warnings** under `-Dwerror=true`. Worktrees and build dirs removed
  afterward; main checkout confirmed still on `wet-series`, clean.
- After the two post-review corrections (message reword on patch 1, plus
  `typedefs.list` entries on patches 4 and 5), re-ran `ninja -j2` at the new
  final commit: build completed cleanly (179 relink/no-op targets touched by
  the mtime bump from the rebase, zero recompiles since no compiled source
  changed — `typedefs.list` is pgindent-only and the patch-1 change is
  message-only), then `ninja headerscheck` and `ninja cpluspluscheck` again,
  both clean. Confirmed rather than assumed, per instruction.

## format-patch / git am round trip

First pass (before the two post-review corrections below): `git format-patch
-v8 -5 -o .../patches-v8/` produced 5 patch files (11126 lines total, no
`Co-Authored-By`/session trailer on any), and `git am` of all 5 onto a fresh
branch from `upstream/master` (throwaway worktree, removed after) applied
cleanly with zero conflicts, tree hash `2fbfea1b43357e8d50eb8f8b4e705076d516fb0e`
== `wet-v8`'s own tree hash at that point.

Final pass (after the corrections): regenerated the same way. `git am` of
the 5 regenerated patches onto a second fresh branch from `upstream/master`
(second throwaway worktree, removed after) again applied **cleanly with
zero conflicts**. Resulting tree hash: `548a43681ac7ed5283ba5d87f4d7900800b969f2`,
identical to `wet-v8`'s current tree hash. Round trip re-confirmed exact.

## Post-assembly corrections

Two corrections came in via a review of the first-pass assembly, both
independently verified against the actual repository content before being
applied (not taken on faith):

1. **Patch 1's commit body.** It was a single 159-character unwrapped
   sentence and never described the hook contract (which lived only in
   patch 3's message, about the module that exercises it, not the patch that
   introduces it). Replaced with a wrapped, multi-paragraph body covering why
   the hooks exist, the two new reporting functions and their relationship
   to the unmodified ordinary pair, the hook-execution constraints
   (no palloc/lock/wait/ereport, `wait_event_hook_depth` re-entry guard,
   chaining order), and the no-hook-installed cost. Applied via
   `git rebase -i` with `edit` on the patch-1 commit and
   `git commit --amend -F <new body>`. Patch 3's message was left untouched,
   as instructed. Verified before applying: `git patch-id --stable` on the
   pre-correction patch 1 and patch 2 tips reproduced `689d58a70e22...` and
   `ee70fa0e47ff...` exactly as the review claimed (these hashes depend only
   on the diff, which this correction never touches, so patch-id identity
   with the posted v7 pair is preserved by construction — confirmed again
   after the reword).
2. **`src/tools/pgindent/typedefs.list`.** The series introduces 11 new
   struct typedefs and the file listed none of them. Rather than trust the
   suggested patch/type mapping, re-derived it independently: grepped
   `pg_wait_event_tracing.c` for each of the 11 closing `} TypeName;` lines,
   then checked which are already present in `wet-v8-wp2b`'s tip (patch 4's
   source) versus only appearing from `wet-v8-wp3` onward (patch 5's
   source). Result: `PwetCaptureLevel`, `PwetLWLockHash`,
   `PwetLWLockHashEntry`, `PwetRegionHeader`, `PwetSlot`, `PwetStats`,
   `PwetTimingEntry` (7) belong to patch 4; `PwetMarkerState`,
   `PwetTraceRecord`, `PwetTraceRowFields`, `PwetTraceState` (4) belong to
   patch 5 — matching the review's proposed split exactly. Inserted each
   into `typedefs.list`'s existing sort order (a plain ASCII sort; the whole
   `Pwet*` block sorts between `PushFunction` and `PyCFunction`), applied via
   `edit` stops on patches 4 and 5 in the same rebase, one `git commit
   --amend --no-edit` each after staging the file.

After both corrections: full `ninja -j2` re-run at the new final commit
(clean, see "Build validation" above), `format-patch` regenerated, and the
`git am` round trip redone onto a fresh `upstream/master` branch — tree hash
match reconfirmed (see "format-patch / git am round trip" above). Patch-ids
of patches 1 and 2 reconfirmed unchanged after the reword
(`git patch-id --stable` on the final commits reproduced the same two
hashes). No other content was touched by this pass.

## Decisions made along the way

- Built patches 4 and 5 by direct per-file `checkout --` composition from
  each source branch's tip rather than an interactive rebase with 31 mixed
  pick/fixup lines across divergent lineages, to keep the two genuinely
  shared files (Makefile, meson.build) under explicit manual control instead
  of relying on git's context-based 3-way merge to guess the right outcome
  (it would not have flagged the meson.build duplicate-key situation as a
  conflict at all — see above).
- Cleaned up one stray blank line in the SQL script left behind by a
  squashed fix commit (documented above) rather than mechanically
  reproducing it, per the brief's "do not preserve the correction history"
  instruction.
- Kept the whole `pgwaiteventtracing.sgml` page in patch 4 rather than
  attempting a section-level split, per the brief's explicit fallback,
  because the page's structure genuinely interleaves the two levels in one
  subsection (documented above).
- Did not attempt to renumber or otherwise "clean up" the trace-level
  record-type constants left non-contiguous by history (`QUERY_END=2`
  reserved-but-unused, etc.); that is module design carried over verbatim
  from the source branches, not squash residue, and out of scope for this
  assembly task.
