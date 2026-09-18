# WPE: fold WPA and WPB into the v11 series, publish to the fork

Git work only, plus one compile. No PostgreSQL server is started.

## Inputs (all local branches in /Users/dmitryfomin/work/git/postgres)

| Branch | Content |
|---|---|
| `wet-v11` | five commits on origin/master (0001 hooks, 0002 conversions, 0003 test_wait_hook, 0004 stats level, 0005 trace level) |
| `wet-v11-wpb` | `wet-v11` + one commit: cold slow path + unlikely hint (touches wait_event.h, wait_event.c) |
| `wet-v11-wpa` | `wet-v11` + two commits: A1 lazy hook install (module, SQL, tests, docs), A2 single gate + local in-flight state (module) |

Confirm each branch's commit list with `git log --oneline wet-v11..<branch>`
before starting and paste it into the report.

## Target: rewrite `wet-v11` in place as five commits

Work in the existing worktree `/Users/dmitryfomin/work/git/postgres_patch/wet-v11`
(branch `wet-v11`). First tag the current tip: `git tag wet-v11-pre-fold`.

1. 0001 = current 0001 + the wpb commit folded in. Message: keep the
   current body, but REPLACE its last paragraph (the one that says the hook
   pointer is loaded once and that neither branch is marked likely or
   unlikely) with the wpb commit's explanation, condensed to one paragraph:
   the enabled path lives in two cold out-of-line functions in
   wait_event.c; the inline pair is one unlikely()-hinted pointer test per
   side, the hint chosen because GCC's static predictor treats a pointer
   compared with NULL as non-NULL; recorded data, hook contract and the
   ordinary pair are unchanged.
2. 0002 unchanged; its `git patch-id --stable` must still start with
   ee70fa0e47ff2796.
3. 0003 unchanged.
4. 0004 = current 0004 + A1 + A2 folded in. Message: keep the body, ADD
   two paragraphs taken from A1's and A2's messages, condensed: (i) the
   wait hooks are installed lazily per process from the capture assign
   hook and never removed, so a process that never enables capture runs
   the hook-null path; the non-chaining wrapper variant; the diagnostic
   function and the new TAP test; (ii) one recording-gate pointer per
   level maintained at every input assignment site, in-flight wait state
   process-local, and the assign-hook masking that keeps the recorded set
   identical to the pre-fold behaviour. Since A1/A2 touch files that
   0005 (trace level) also modifies, fold by cherry-picking A1 and A2 onto
   0004 during an interactive rebase and re-applying 0005 on top; resolve
   conflicts conservatively and list each one. If a conflict is not
   mechanical, stop and report.
5. 0005 unchanged in content except conflict resolution from step 4.

Non-interactive: build the series with a sequence of `git cherry-pick`
onto `origin/master` (re-fetch first; if origin/master moved since the
base 311df1dc039, rebase onto the new tip and report the new base).
Metadata on all five: author/committer `Dmitry Fomin <fomin.list@gmail.com>`,
one `Discussion:` trailer, no other trailer; grep the five messages and
the full diff for `adyen`, `Claude`, `WPA`, `WPB`, `fixup` and remove any
internal work-package references from comments or messages (they must not
reach pgsql-hackers). Keep test comments that mention "lazy" etc.; just
drop the "(WPA fixup a1)" style tags.

## Verification

- `git diff wet-v11-pre-fold` after applying wpb+A1+A2 on top of the old
  tip in a scratch branch must be EMPTY against the new `wet-v11` tip
  (i.e. the fold changes only history, not content, apart from the
  removed work-package tags, which you list).
- Build in the worktree (meson line as in the WPA brief, with
  `-Dldap=disabled`), `ninja -C build` clean with -Dwerror; then
  `ninja -C build install` and run
  `meson test -C build --suite setup --suite test_wait_hook --suite pg_wait_event_tracing`
  once (15 tests expected).
- `git format-patch -v11 -5 -o /Users/dmitryfomin/work/git/postgres_patch/v11/patches-v11/`
  after deleting the old v11-000* files there; `git am` round trip onto a
  throwaway branch from origin/master must apply cleanly and give the same
  tree hash as `wet-v11`.

## Publish

Push is authorized for this step only, to the fork:
`git push fork wet-v11:wet-v11` (create) and
`git push fork wet-v11:ci/wet-v11` (CI runs on every push). Do NOT push
any other branch. Record the two push results and the GitHub Actions run
URL (query `https://api.github.com/repos/DmitryNFomin/postgres/actions/runs?branch=ci/wet-v11` with curl; `gh` is not installed).

## Deliverable

`/Users/dmitryfomin/work/git/postgres_patch/v11/reports/wpe-report.md`:
five hashes and subjects, base commit, conflicts and resolutions, the
empty-diff proof, patch-id of 0002, test summary, push output, CI run
URL. Final chat message at most 8 lines.
