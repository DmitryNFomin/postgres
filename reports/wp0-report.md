# WP0: assemble branch `wet-v11` — report

## Branch and worktree

Created with:
`git -C <workspace>/work/git/postgres worktree add -b wet-v11 <workspace>/work/git/postgres_patch/wet-v11 fork/wet-v8`

`origin/master` at time of assembly: `311df1dc0392f06973cf98eac51d63cb007267ce`.

## Final five commits

```
0cf1c2feaefb749ea0d13886f55db5a75919026f pg_wait_event_tracing: trace level
cc488efcee6c53f8d1431c046407ccf46ed8dae2 pg_wait_event_tracing: statistics level
6a1bb44866be358e85bd6115b90ac40a6327e39c test_wait_hook: test module for the wait event hook contract
18145244bfb8c01021ee6ce9634e4d14d22d9d6f Convert wait_start/end call sites to the timed pair
085071ef4be9971bc7fe0bdc89295308017a5b0d Add begin/end hooks for timed wait events
```

All five: Author = Committer = `Dmitry Fomin <fomin.list@gmail.com>`. Each
message ends with exactly one trailer line:
`Discussion: https://postgr.es/m/CAPHG-0mAOn05ae6Kqx1wHXxzOk4E5W7ajjd=QBhgkR7a0uyQmw@mail.gmail.com`
(verified with `git log --format='%H %an <%ae> %cn <%ce> | %s' origin/master..wet-v11`).

## Commit 2 patch-id

Before rebase (fork/wet-v8's `d157435f39be497326926400f34dc9ebc045ac48`):
`ee70fa0e47ff2796255d34f29d3bf6028ace4a38`

After rebase + fold of commits 1/3/4 around it
(`18145244bfb8c01021ee6ce9634e4d14d22d9d6f`):
`ee70fa0e47ff2796255d34f29d3bf6028ace4a38`

Identical (first 16 hex digits `ee70fa0e47ff2796`, as required). Commit 2's
own hash changed (94d729a41c0e... -> 18145244bfb8...) only because its parent
tree changed when commit 1 was amended; content is untouched.

## Conflicts

### `git rebase origin/master` (wet-v8 -> origin/master)

Zero conflicts. All five original wet-v8 commits replayed cleanly.

### Fold of `40bffed8a92` ("Optimize the null wait-event hook path") into commit 1

`git cherry-pick -n 40bffed8a92` conflicted on
`src/test/modules/test_wait_hook/test_wait_hook.c`
("CONFLICT (modify/delete): deleted in HEAD and modified in 40bffed8a92") —
mechanical and expected: that file does not exist yet at commit 1's point in
history (it is added by commit 3). The `wait_event.h` hunk applied cleanly
and staged with no conflict. Resolved by dropping the test_wait_hook.c side
entirely (`git rm --cached` + removing the working-tree file) and keeping
only the staged `wait_event.h` change, which matches `40bffed8a92`'s hunk
for that file exactly.

### Fold of `40bffed8a92`'s comment hunk into commit 3

`git cherry-pick -n 40bffed8a92` at commit 3 applied without any conflict:
`wait_event.h` was already in its post-fold state (from commit 1) so that
hunk was a no-op, leaving only the `test_wait_hook.c` comment-wording hunk
staged, verbatim.

### Fold of `c12783fbf86` ("Avoid steady-state attachment calls") into commit 4

`git cherry-pick -n c12783fbf86` produced a genuine content conflict in
`contrib/pg_wait_event_tracing/pg_wait_event_tracing.c`, inside
`pwet_maybe_attach()`. Root cause: `c12783fbf86`'s parent tree already
contains commit 5's ("trace level") changes to that function (guard
`!pwet_at_safe_point()`, the `bool ready` local, and the DSA/trace attach
split), because on the fork/bench-v9-patched and fork/bench-v10-guard
branches all of wet-v8 (0001-0005) is already merged in, whereas at commit 4
of wet-v11 the trace-level changes do not exist yet — commit 4 still has the
original, simpler guard `if (!pwet_attach_needed || !pwet_can_attach())
return;`. This is a real but mechanical structural conflict: the fix being
folded (`pwet_maybe_attach` becomes an always-inline flag test that calls a
new out-of-line `pwet_maybe_attach_slow()`) is orthogonal to which guard
condition is current at this point in the series. Resolved by hand-applying
the same structural split (`pwet_maybe_attach` -> flag test +
`pwet_maybe_attach_slow()`) to commit 4's actual body, keeping its existing
`pwet_can_attach()` guard and `pwet_attach_stats()` logic verbatim inside the
new slow function, plus updating the forward declaration
(`static pg_always_inline void pwet_maybe_attach(void); static void
pwet_maybe_attach_slow(void);`).

### Replay of original commit 5 ("trace level") after commit 4 was reshaped

`git rebase --continue` hit a second conflict in the same function, for the
same underlying reason (commit 5's original diff assumes the pre-split
single-function `pwet_maybe_attach`, but commit 4 now has the split form).
All lines outside the top of the function auto-merged cleanly (the exit
callback registration, the DSA/trace attach block, the `ready` logic).
Resolved the top of the function by keeping the fast/slow split from commit
4 and applying commit 5's actual change to the slow path: added `bool
ready;`, added commit 5's comment, and replaced the guard with
`if (!pwet_at_safe_point()) return;` (dropping the redundant
`pwet_attach_needed` test, already performed by the fast-path wrapper, the
same simplification already applied to the guard in commit 4's fold).

Both `pg_wait_event_tracing.c` conflicts were single, localized, well-
understood structural conflicts caused entirely by feature ordering; neither
required guessing at intent, so no other conflict was left unresolved and
none was skipped.

## Diff-comparison verification (step 2)

`git diff fork/bench-v10-guard wet-v11 -- src contrib doc` is non-empty
(large, ~32k lines) because it includes every upstream commit between
`0c5d6269614` and current `origin/master`. The targeted comparison the brief
asks for instead:

```
git diff $(git merge-base origin/master fork/bench-v10-guard) fork/bench-v10-guard \
    -- src contrib doc ':!src/test/modules/test_wait_primitive'      # "A"
git diff origin/master wet-v11 -- src contrib doc                    # "B"
diff A B
```

`diff A B` is **not** literally empty (156 lines of diff-of-diffs output),
but every difference falls into exactly the three explained categories,
verified by grepping the diff-of-diffs for actual added/removed content
lines (as opposed to `index`/`@@` line-number noise):

1. **Line-number-only shifts** in `@@` hunk headers and blob `index` hashes
   for files with unrelated upstream changes between the merge-base and
   current `origin/master` (`wait_event.c`, `xlog.c`, `twophase.c`,
   `typedefs.list`, etc.) — content of the hunks themselves is identical,
   only surrounding line numbers moved.
2. **One deliberate one-line difference**, exactly at the guard in
   `pwet_maybe_attach_slow()`:
   `fork/bench-v10-guard`: `if (!pwet_attach_needed || !pwet_at_safe_point())`
   `wet-v11`: `if (!pwet_at_safe_point())`
   — expected: wet-v11's fast wrapper already tested `pwet_attach_needed`
   before calling the slow path, so the redundant test was dropped from the
   slow path in this series, consistent with how the same guard was already
   simplified when 40bffed8a92/c12783fbf86 was folded into commit 4.
3. **Absence of `test_wait_primitive`**: `Makefile`/`meson.build` lines
   adding `test_wait_primitive` to `SUBDIRS`/`subdir()` are present only in
   the "A" side (bench-v10-guard) and absent from wet-v11, exactly as
   required (the fixture must not be included).

No other content-level differences were found. Conclusion: **wet-v11's
tree, modulo the fixture's exclusion and the one documented guard
simplification, is identical to fork/bench-v10-guard rebased onto current
origin/master.**

## Build

`meson setup build --buildtype=debugoptimized -Dcassert=true -Dwerror=true
-Dinjection_points=true -Dtap_tests=enabled -Dssl=openssl
-Dprefix=$PWD/install` configured successfully.

First `ninja -C build` attempt failed, but only on pre-existing,
unrelated code: `src/interfaces/libpq/fe-connect.c`'s OpenLDAP calls trip
`-Werror,-Wdeprecated-declarations` against this machine's macOS 15.5 SDK
(`ldap_unbind`, `ldap_simple_bind`, etc., deprecated since macOS 10.10/10.11).
This is confirmed unrelated to the five patch commits: it does not touch any
file the series touches, and `wet-v8`'s own last successful local build used
`werror=False` (not `true`), i.e. this SDK/`-Werror` interaction is a
standing environment issue, not something introduced here. Since fixing
`fe-connect.c` is out of scope ("git work only", no source changes beyond
the described cherry-picks/squashes), verification was completed by
reconfiguring with `meson configure build -Dldap=disabled` (a local build
option, no source touched, no commit affected) and rebuilding.

Last 3 lines of the successful `ninja -C build` run (ldap disabled):

```
[1880/1883] Linking target src/test/modules/test_wait_lsn/test_wait_lsn.dylib
[1881/1883] Linking target src/test/modules/xid_wraparound/xid_wraparound.dylib
[1882/1883] Linking target src/interfaces/ecpg/test/pg_regress_ecpg
```

Exit code 0, zero warnings, zero errors in that run (`grep -ci
"warning:\|error:"` over the full log returns 0). No `meson test`, `ninja
install`, `initdb`, or `pg_ctl` was run, and no server was started.

## `the employer domain` / `Claude` grep

`git log --format='%B' origin/master..wet-v11` and
`git diff origin/master..wet-v11 -- src contrib doc`, both grepped
case-insensitively for `the employer domain` and `claude`: no matches in either.
Also grepped the five generated `.patch` files: no matches.

## Deliverables produced

- Worktree: `<workspace>/work/git/postgres_patch/wet-v11` (branch
  `wet-v11`).
- Patches: `<workspace>/work/git/postgres_patch/v11/patches-v11/`
  (`v11-0001` .. `v11-0005`).
- Main checkout (`<workspace>/work/git/postgres`, `REL_17_STABLE`)
  and the `wet-v8` worktree were not touched (verified via `git status`
  before and after; only pre-existing untracked build artifacts/`.DS_Store`
  files were present, none created by this work).
- Nothing was pushed; no server was started.

## Could not do / deviations

- `ninja -C build` could not reach zero warnings with `-Dldap=disabled`
  omitted, due to the pre-existing macOS SDK/OpenLDAP deprecation issue
  described above, unrelated to this series. Worked around locally with
  `-Dldap=disabled` for verification purposes only; no source file and no
  commit was changed to work around it.
- Everything else in the brief was completed as specified.
