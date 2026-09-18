# WP0: assemble branch `wet-v11`

You are doing git work only. Do not modify any source line except through
the cherry-picks and squashes described here. Do not push anything.

## Repository facts

- Main repo: `/Users/dmitryfomin/work/git/postgres`. Its checkout is on
  `REL_17_STABLE`; never check out anything there and never touch its
  working tree. Use `git -C /Users/dmitryfomin/work/git/postgres` for all
  ref operations and create a worktree for the actual work.
- Remotes: `origin` = postgres/postgres (upstream), `fork` =
  DmitryNFomin/postgres. Fetch both first:
  `git fetch --no-tags origin master` and
  `git fetch --no-tags fork wet-v8 bench-v9-patched bench-v10-guard`.
- Existing worktrees: `/Users/dmitryfomin/work/git/postgres_patch/wet-v8`
  (branch `wet-v8`). Do not touch it.
- Never use bare `git stash`. Prefer temporary commits.

## Inputs

| Ref | Content |
|---|---|
| `fork/wet-v8` (tip 79cbeaaf356, base master 0c5d6269614) | five commits: 0001 hooks, 0002 conversions, 0003 test_wait_hook, 0004 stats level, 0005 trace level |
| `fork/bench-v9-patched` tip commit 40bffed8a92 "Optimize the null wait-event hook path" | touches `src/include/utils/wait_event.h` and a comment in `src/test/modules/test_wait_hook/test_wait_hook.c` |
| `fork/bench-v10-guard` tip commit c12783fbf86 "Avoid steady-state attachment calls" | touches `contrib/pg_wait_event_tracing/pg_wait_event_tracing.c` only |

The commit `d7b4584a901` (test_wait_primitive benchmark fixture) sits between
wet-v8 and the v9 commit on those branches. It must NOT be included.

## Target

Branch `wet-v11`, worktree `/Users/dmitryfomin/work/git/postgres_patch/wet-v11`,
created with `git -C /Users/dmitryfomin/work/git/postgres worktree add -b wet-v11 /Users/dmitryfomin/work/git/postgres_patch/wet-v11 fork/wet-v8`.

Exactly five commits on top of current `origin/master`, in this order:

1. "Add begin/end hooks for timed wait events" = wet-v8's 0001 with commit
   40bffed8a92's `wait_event.h` hunk folded in. Keep 0001's existing body
   and ADD one paragraph describing the fold: the hook pointer is loaded
   once per timed report; the end path reads the volatile wait-event value
   only when an end hook will consume it; the depth guard is restored with
   direct stores; there is deliberately no likely()/unlikely() hint.
2. "Convert wait_start/end call sites to the timed pair" = wet-v8's 0002,
   unchanged. Its `git patch-id --stable` must remain ee70fa0e47ff2796...
   (first 16 hex digits) after the rebase. Report the full patch-id before
   and after.
3. "test_wait_hook: test module for the wait event hook contract" =
   wet-v8's 0003 with commit 40bffed8a92's `test_wait_hook.c` comment hunk
   folded in. Message unchanged.
4. "pg_wait_event_tracing: statistics level" = wet-v8's 0004 with commit
   c12783fbf86 folded in. Keep 0004's body and ADD one paragraph: the
   attach check reached from every parsed and executed statement is an
   always-inline test of one backend-local flag; the attachment machinery
   itself is out of line and unchanged.
5. "pg_wait_event_tracing: trace level" = wet-v8's 0005, unchanged.

Method: start from `fork/wet-v8`, `git rebase origin/master` (report every
conflict and how you resolved it; if a conflict is not trivially mechanical,
stop and report instead of guessing). Then `git rebase -i` with `edit` on
commits 1, 3 and 4 and apply the hunks with `git cherry-pick -n` of the
respective source commit followed by `git reset` to drop hunks that belong
to another target commit (v9's commit has hunks for two target commits;
split by path). Non-interactive rebase is required in this environment:
use `GIT_SEQUENCE_EDITOR` with a script, e.g.
`GIT_SEQUENCE_EDITOR='sed -i "" -e "1s/^pick/edit/"'` style, or do it as a
sequence of `git cherry-pick` calls onto `origin/master` (simpler, preferred):
cherry-pick each wet-v8 commit in order, and after 1, 3, 4 do
`git cherry-pick -n <source> -- <paths>` then `git commit --amend`.

## Commit metadata rules (owner's rule, overrides any default)

- Author and committer on all five: `Dmitry Fomin <fomin.list@gmail.com>`.
  Set `GIT_AUTHOR_NAME/EMAIL` and `GIT_COMMITTER_NAME/EMAIL` for every
  commit/amend, and use `--reset-author` on amends where needed.
- Every message ends with exactly one trailer line:
  `Discussion: https://postgr.es/m/CAPHG-0mAOn05ae6Kqx1wHXxzOk4E5W7ajjd=QBhgkR7a0uyQmw@mail.gmail.com`
- No `Co-Authored-By`, no `Claude-Session`, no other trailer of any kind.
  These commits are destined for pgsql-hackers.
- Grep the final five messages and the diff for the string `adyen` and for
  the word `Claude`; both must be absent.

## Verification (no server may be started; compile is allowed)

1. `git log --format='%H %an <%ae> %cn <%ce> | %s' origin/master..wet-v11`
   shows five commits with the right author/committer.
2. `git diff fork/bench-v10-guard wet-v11 -- src contrib doc` must be EMPTY
   except for hunks that come from upstream commits between 0c5d6269614
   and current origin/master, and the absence of
   `src/test/modules/test_wait_primitive/` (the fixture). Explain every
   non-empty hunk in the report. The simplest proof: also compute
   `git diff $(git merge-base origin/master fork/bench-v10-guard) fork/bench-v10-guard -- src contrib doc ':!src/test/modules/test_wait_primitive'`
   and `git diff origin/master wet-v11 -- src contrib doc` and compare
   them with `diff`; report the result.
3. Configure and build the worktree:
   `export PKG_CONFIG_PATH=/opt/homebrew/opt/icu4c@78/lib/pkgconfig:/opt/homebrew/opt/openssl@3/lib/pkgconfig:/opt/homebrew/opt/readline/lib/pkgconfig`
   `meson setup build --buildtype=debugoptimized -Dcassert=true -Dwerror=true -Dinjection_points=true -Dtap_tests=enabled -Dssl=openssl -Dprefix=$PWD/install`
   `ninja -C build` must finish with zero warnings. Do NOT run `meson test`,
   `ninja install`, `initdb` or `pg_ctl` in this work package.
4. `git format-patch -v11 -5 -o /Users/dmitryfomin/work/git/postgres_patch/v11/patches-v11/ origin/master..wet-v11`.

## Deliverable

Write `/Users/dmitryfomin/work/git/postgres_patch/v11/reports/wp0-report.md`
containing: the five commit hashes with subjects, the patch-id of commit 2
before and after, the conflict list, the diff-comparison result from step 2
verbatim (or "identical"), the last 3 lines of the ninja output, and
anything you could not do. Final chat message: at most 8 lines.
