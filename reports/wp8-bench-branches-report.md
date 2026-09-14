# wp8: bench-v8-baseline / bench-v8-patched — build & push report

Date: 2026-09-14

## Branch tips

| Branch | Tip commit | Fixture commit |
|---|---|---|
| `bench-v8-baseline` | `765efece39ba3fb04fdf20b1dadcd9ecea76fbc9` | same as tip (fixture is the only commit added on top of `0c5d6269614`) |
| `bench-v8-patched`  | `d7b4584a901241258604eef1f03dfd6b3f1fa926` | same as tip (fixture is the only commit added on top of `wet-v8` = `79cbeaaf35641bdd35201c30fe797d946abcb0d2`) |

Both fixture commits carry the subject `test_wait_primitive: benchmark fixture for wait-event measurement` and body:

```
Measurement-only fixture for bare-metal A/B benchmarking of the
wait_event_timing patch series. Not part of the submitted series.
```

No `Co-Authored-By` / Claude / session trailer, as required.

## Construction

- `bench-v8-baseline`: worktree created at `0c5d6269614` (base). `git cherry-pick -n 39097830952` (the 10-file `test_wait_primitive` module) followed by `git apply` of patch 06 (extracted from the tar via `tar xf ... -O .../06-deterministic-benchmark-fixture.patch`, 4 files / +35 lines, applied cleanly with no fuzz). Squashed into one commit with `GIT_AUTHOR_*`/`GIT_COMMITTER_*` forced to `Dmitry Fomin <fomin.list@gmail.com>`.
- `bench-v8-patched`: same two-piece fixture applied identically on top of a `wet-v8` worktree (cherry-pick auto-merged cleanly in `Makefile`/`meson.build`, patch 06 applied with no fuzz), squashed the same way.

## Authorship / privacy verification

```
$ git log --format='%H | %an <%ae> | %cn <%ce>' 0c5d6269614..bench-v8-baseline
765efece39ba3fb04fdf20b1dadcd9ecea76fbc9 | Dmitry Fomin <fomin.list@gmail.com> | Dmitry Fomin <fomin.list@gmail.com>

$ git log --format='%H | %an <%ae> | %cn <%ce>' 0c5d6269614..bench-v8-patched
d7b4584a901241258604eef1f03dfd6b3f1fa926 | Dmitry Fomin <fomin.list@gmail.com> | Dmitry Fomin <fomin.list@gmail.com>
79cbeaaf35641bdd35201c30fe797d946abcb0d2 | Dmitry Fomin <fomin.list@gmail.com> | Dmitry Fomin <fomin.list@gmail.com>
7792f16ace6ea16d96d5adce42cfd4c88dd44751 | Dmitry Fomin <fomin.list@gmail.com> | Dmitry Fomin <fomin.list@gmail.com>
f21149ca3338cfbbce86ccd2328dc7d63c1026d3 | Dmitry Fomin <fomin.list@gmail.com> | Dmitry Fomin <fomin.list@gmail.com>
d157435f39be497326926400f34dc9ebc045ac48 | Dmitry Fomin <fomin.list@gmail.com> | Dmitry Fomin <fomin.list@gmail.com>
0df248c07fe89d6578708499c503621f9f3b52fa | Dmitry Fomin <fomin.list@gmail.com> | Dmitry Fomin <fomin.list@gmail.com>
```

Grep for forbidden strings (the employer domain and personal address carried by the imported fixture patch's header, plus the OS user name, case-insensitive) over author/committer/message of every new commit on both branches: **clean** on both. Additionally grepped the fixture commit's diff content and the full fixture tree content on both branches (not just metadata) for the same strings: **clean**. The imported fixture patch's original `From:` header never entered git history — it was consumed by `git apply` (working-tree patch, not `git am`), and the commit was made with forced author/committer env vars.

## Checks 1-6

**1. Fixture trees identical on both branches**
```
$ git diff bench-v8-baseline:src/test/modules/test_wait_primitive bench-v8-patched:src/test/modules/test_wait_primitive
(empty output, exit 0)
```

**2. `bench-v8-patched~1` tree == `wet-v8` tree, and the 5 commits above base match exactly**
```
$ git rev-parse bench-v8-patched~1^{tree}
548a43681ac7ed5283ba5d87f4d7900800b969f2
$ git rev-parse wet-v8^{tree}
548a43681ac7ed5283ba5d87f4d7900800b969f2

$ git log --format='%H' 0c5d6269614..wet-v8
79cbeaaf35641bdd35201c30fe797d946abcb0d2
7792f16ace6ea16d96d5adce42cfd4c88dd44751
f21149ca3338cfbbce86ccd2328dc7d63c1026d3
d157435f39be497326926400f34dc9ebc045ac48
0df248c07fe89d6578708499c503621f9f3b52fa

$ git log --format='%H' 0c5d6269614..bench-v8-patched~1
(identical list, same order)
```

**3. `test_wait_primitive--1.0.sql` defines all six functions on both branches**
```
CREATE FUNCTION test_wait_primitive_latch_set(bigint)
CREATE FUNCTION test_wait_primitive_latch_timeout(bigint)
CREATE FUNCTION test_wait_primitive_file_read(bigint)
CREATE FUNCTION test_wait_primitive_usleep0(bigint)
CREATE FUNCTION test_wait_primitive_report_only(bigint)
CREATE FUNCTION test_wait_primitive_lwlock_contention(bigint)
```
identical on `bench-v8-baseline` and `bench-v8-patched`.

**4. Module `Makefile` retains `ifdef USE_PGXS` branch on both branches** — confirmed present verbatim (`ifdef USE_PGXS` / `PG_CONFIG = pg_config` / `PGXS := $(shell $(PG_CONFIG) --pgxs)` / `include $(PGXS)`) on both.

**5. Compile both branches, separate worktrees/build dirs**

Toolchain: `<workspace>/.venv-v7-rfc/bin` (meson 1.12.0, ninja 1.13.2). Command: `meson setup <build> <worktree> --buildtype=debugoptimized -Dcassert=true -Dwerror=true` then `ninja -j2` (host has 4 cores; both builds run concurrently).

- Worktrees: `wt-bench-baseline` (branch `bench-v8-baseline`), `wt-bench-patched` (branch `bench-v8-patched`).
- Build dirs: `build-bench-baseline`, `build-bench-patched`.
- Both `meson setup` runs succeeded (gcc 13.3.0, cassert=true, werror=true, no config errors).
- Both `ninja -j2` runs completed with no errors:
  - `bench-v8-baseline`: **1893/1893** targets built, final line `[1893/1893] Linking target src/interfaces/ecpg/test/pg_regress_ecpg`.
  - `bench-v8-patched`: **1897/1897** targets built (4 more than baseline — the wet-v8 series adds its own `test_wait_hook` test module etc.), final line `[1897/1897] Linking target src/interfaces/ecpg/test/pg_regress_ecpg`.
- `test_wait_primitive.so` linked on both:
  ```
  -rwxrwxr-x 1 user user 55496 ... build-bench-baseline/.../test_wait_primitive.so
  -rwxrwxr-x 1 user user 55496 ... build-bench-patched/.../test_wait_primitive.so
  ```
  Identical byte size on both, consistent with the byte-identical fixture source (baseline links against unmodified core; patched links against `wet-v8` core — same size here because `test_wait_primitive.c` doesn't reference anything from the wet-v8 patch itself).
- Re-ran `ninja -j2` on each build dir afterward to confirm a clean exit status explicitly: both printed `ninja: no work to do.`, exit code 0.
- Grepped both build logs for `error|failed` (excluding filename false-positives like `wait_error.c.o`, `utils_error_elog.c.o`): no real errors on either.

**6. Main checkout untouched**
```
$ git status --short && git branch --show-current && git log -1 --format='%H'
(clean)
wet-series
186314d20a3b88412d5721059daf01ffff78c0e3
```
Matches the pre-task HEAD exactly — never checked out or modified.

## Push results

Neither branch existed on the fork beforehand (`git ls-remote origin refs/heads/bench-v8-*` returned nothing). Plain (non-force) pushes:

```
To https://github.com/DmitryNFomin/postgres.git
 * [new branch]              bench-v8-baseline -> bench-v8-baseline
To https://github.com/DmitryNFomin/postgres.git
 * [new branch]              bench-v8-patched -> bench-v8-patched
```

Post-push verification that remote tips match local:
```
765efece39ba3fb04fdf20b1dadcd9ecea76fbc9	refs/heads/bench-v8-baseline
d7b4584a901241258604eef1f03dfd6b3f1fa926	refs/heads/bench-v8-patched
```
Matches local `git rev-parse bench-v8-baseline bench-v8-patched` exactly.

`wet-v8` was never checked out, touched, rebased, or pushed by this task — its own worktree at `<workspace>/wt-v8` was left exactly as found; the patched branch's fixture was built by cherry-picking commits from it into a brand-new worktree, not by modifying it.

## Cleanup

Removed both scratch worktrees (`git worktree remove wt-bench-baseline`, `wt-bench-patched`) and both build directories (`build-bench-baseline`, `build-bench-patched`). `git worktree list` afterward shows only the original `postgres` (wet-series) and pre-existing `wt-v8` (wet-v8) worktrees. The two new local branches `bench-v8-baseline`/`bench-v8-patched` remain in the main repo's refs (branches aren't deleted by worktree removal, and weren't asked to be).

## Decisions made

- Ran `ninja -j2` for both builds **concurrently** (host has 4 cores) rather than strictly sequentially, since the task only mandates `-j2` per build and slowness was explicitly declared acceptable, not sequentiality.
- Used `git apply` (not `git am`) for patch 06 so its `From:`/`Date:` header (which carried a personal employer address) never touched any commit object; author/committer for the resulting squash commit were forced via `GIT_AUTHOR_NAME/EMAIL` and `GIT_COMMITTER_NAME/EMAIL` env vars on the `git commit` invocation.
- Went beyond the letter of the privacy check by also grepping fixture diff content and full tree content (not just commit metadata) for the forbidden strings, since the patch's file contents were an unaudited input.
- Verified ninja's actual exit status explicitly via a harmless re-run (`ninja: no work to do.`, exit 0) in addition to eyeballing the background log tails, since the builds were launched via `nohup ... &` and their real exit codes weren't otherwise captured.
