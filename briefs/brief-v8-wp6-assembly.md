# WP6 (part 1): assemble the v8 series

Goal: turn seven work-package branches into FIVE clean patches on one branch
`wet-v8`, rebased onto current upstream/master, such that a reviewer sees the
design, not the trail of corrections.

## HARD RULES
- NEVER start a PostgreSQL server on this host: no `meson test`, pg_regress,
  TAP, initdb, pg_ctl. Compile only: meson setup, `ninja -j2` (module and
  full build), `ninja headerscheck`, `ninja cpluspluscheck`. The reviewer
  pushes to the fork for CI.
- Keep the main checkout <workspace>/postgres on
  `wet-series`. Do not push anything. Work in a worktree:
      git -C <workspace>/postgres worktree add \
          <workspace>/wt-v8 -b wet-v8 wet-v8-wp4b
  (`wet-v8-wp4b` is the tip of the stack: wp2 → wp2b → wp3 → wp4b.)
- Build dir <workspace>/build-v8,
  `--buildtype=debugoptimized -Dcassert=true -Dwerror=true -Dinjection_points=true`,
  meson/ninja from `.venv-v7-rfc/bin`.

## Input branches, in stack order
| Branch | Tip | Content |
|---|---|---|
| `wet-v7-rfc` | dc8f6dfafd3 | v7-0001 hook, v7-0002 call-site conversions (already posted) |
| `wet-v8-wp1` | 1156a3806cf | src/test/modules/test_wait_hook |
| `wet-v8-wp2` | 02fe98d0f97 | contrib module, statistics level (M1, M2, M3 + 3 fix commits) |
| `wet-v8-wp2b` | 64f135699e0 | control table to fixed shmem, reserved region, in-hook claim (M1–M3 + 2 fix commits) |
| `wet-v8-wp3` | 20bc081dbd2 | trace level: ring, orphans, markers (M1–M3 + 2 fix commits) |
| `wet-v8-wp4a` | cbb10ad23dc | TAP 001–004 (branched from wp2; NOT in the wp4b stack — cherry-pick it) |
| `wet-v8-wp4b` | e035e517730 | injection point + TAP 005/010/011/012 |
| `wet-v8-wp5` | 82cc5367b57 | docs page, xfunc paragraph, grants (branched from wp3 — cherry-pick it) |

## Target series (five patches, in this order)
1. **Add begin/end hooks for timed wait events** — exactly v7-0001, unchanged.
2. **Convert wait_start/end call sites to the timed pair** — exactly v7-0002, unchanged.
3. **test_wait_hook: test module for the wait event hook contract** — wp1.
4. **pg_wait_event_tracing: statistics level** — everything from wp2 + wp2b +
   wp4a + the statistics parts of wp5 (the module's own docs page sections are
   part of patch 5; see below), squashed so the module arrives correct the
   first time: sparse per-backend payloads, owner tokens, reset ACL and race,
   fixed-shmem control table, reserved region for server-side processes,
   regress test, TAP 001–004 and 006.
5. **pg_wait_event_tracing: trace level** — everything from wp3 + wp4b: ring,
   writer, seqlock readers, orphan lifecycle, markers, attribution function,
   trace regress test, TAP 005/010/011/012, and the injection point.
Documentation: `doc/src/sgml/pgwaiteventtracing.sgml` covers both levels.
Split it if a clean split is natural (statistics sections in patch 4, trace
sections in patch 5); if not, put the whole page in patch 4 and say so in the
report. `xfunc.sgml` and `filelist.sgml`/`contrib.sgml` registration go in
patch 4.

## Method
- Squash, do not preserve the correction history: every "fix CI-found …"
  commit must disappear into the patch it corrects. The final tree must be
  IDENTICAL to `wet-v8-wp4b` plus the cherry-picked wp4a and wp5 content —
  verify with `git diff` against a reference tree and report the result.
- Rebase the result onto current `upstream/master` (fetch first). Resolve any
  conflict conservatively and list every conflict you resolved in the report.
- Commit messages: author `Dmitry Fomin <fomin.list@gmail.com>`, a real
  explanatory body (what and why, not a changelog), and the trailer
  `Discussion: https://postgr.es/m/CAPHG-0mAOn05ae6Kqx1wHXxzOk4E5W7ajjd=QBhgkR7a0uyQmw@mail.gmail.com`.
  Do NOT add Co-Authored-By or any Claude/session trailer to these five
  commits: they are destined for pgsql-hackers.
- Then: `git format-patch -v8 -5 -o <workspace>/patches-v8/`
  and round-trip: `git am` the five patches onto a fresh throwaway branch from
  upstream/master and confirm the resulting tree hash equals `wet-v8`'s.

## Validate (compile only)
- Full `ninja -j2` build with werror, `headerscheck`, `cpluspluscheck` — at the
  FINAL commit, and also confirm patch 1 and patch 2 still build alone
  (checkout each in a detached worktree if that is cheap; if not, say so).
- `git diff --stat upstream/master..wet-v8` must touch only:
  src/include/utils/wait_event.h, src/backend/utils/activity/wait_event.c,
  the 42 converted call-site files, src/test/modules/test_wait_hook/**,
  src/test/modules/{Makefile,meson.build}, contrib/pg_wait_event_tracing/**,
  contrib/{Makefile,meson.build}, doc/src/sgml/{pgwaiteventtracing,contrib,
  filelist,xfunc}.sgml. Anything else is a mistake — report it.

## Deliverable
<workspace>/wp6-assembly-report.md: the five commit
hashes and subjects, diffstat per patch, the tree-equality check results, every
conflict resolved during the rebase, the build results, and anything you had to
decide (especially the docs split). Final message <= 10 lines.
