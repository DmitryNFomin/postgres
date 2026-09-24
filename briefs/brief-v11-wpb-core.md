# WPB: core hook-null path, `src/include/utils/wait_event.h`

Goal: one commit on branch `wet-v11-wpb`, a candidate fixup for patch 0001
("Add begin/end hooks for timed wait events"). It changes only how the
compiler lays out the timed pair; the recorded data and the hook contract
are unchanged.

## Repository and rules

- Main repo `<workspace>/work/git/postgres` (checkout on
  REL_17_STABLE; never check it out or touch its tree). Base: local branch
  `wet-v11`.
- Worktree:
  `git -C <workspace>/work/git/postgres worktree add -b wet-v11-wpb <workspace>/work/git/postgres_patch/wet-v11-wpb wet-v11`
- Never push. Never bare `git stash`. Do not touch other worktrees.
- Commit metadata: author/committer `Dmitry Fomin <fomin.list@gmail.com>`,
  explanatory body, single trailer
  `Discussion: https://postgr.es/m/CAPHG-0mAOn05ae6Kqx1wHXxzOk4E5W7ajjd=QBhgkR7a0uyQmw@mail.gmail.com`,
  no Co-Authored-By, no Claude trailer.
- PostgreSQL C style. Building and running tests locally is allowed.

## Current code (after v9 fold, on wet-v11)

`pgstat_report_wait_start_timed()` stores the event, loads
`wait_event_begin_hook` into a local, and if non-NULL and
`wait_event_hook_depth == 0` sets depth to 1, calls, sets depth to 0.
`pgstat_report_wait_end_timed()` loads `wait_event_end_hook`, and if
non-NULL and depth is 0 reads the volatile event value, sets depth, calls,
resets depth; then stores 0. All of that is inline at 94 call sites.

Problem: GCC's built-in branch heuristics predict a pointer compared with
NULL as non-NULL, so with no hint the hook call is laid out as the
fall-through path and the null path as the taken branch. The enabled
path (depth stores, indirect call) is also inlined at every site, which
costs code size and forces spills into the enclosing function's prologue
(the v7 measurement saw four extra stack references in XLogWrite).

## Change

1. In `wait_event.c` add two out-of-line functions:
   `pg_noinline pg_attribute_cold void pgstat_wait_event_hook_begin_slow(uint32 wait_event_info)`
   and `pg_noinline pg_attribute_cold void pgstat_wait_event_hook_end_slow(void)`.
   Begin-slow: `if (wait_event_hook_depth == 0) { depth = 1; hook(info); depth = 0; }`
   using the current value of `wait_event_begin_hook` (re-read inside; it
   cannot change between the inline test and the call in the same
   process, but say so in a comment). End-slow: same with the end hook,
   reading the volatile `*my_wait_event_info` itself to obtain the event.
   Check `c.h` for the exact spelling of the cold/noinline attributes
   available on master (`pg_attribute_cold`, `pg_noinline`) and use them.
2. In `wait_event.h` the inline pair becomes:
   ```
   start_timed: *(volatile uint32 *) my_wait_event_info = info;
                if (unlikely(wait_event_begin_hook != NULL))
                    pgstat_wait_event_hook_begin_slow(info);
   end_timed:   if (unlikely(wait_event_end_hook != NULL))
                    pgstat_wait_event_hook_end_slow();
                *(volatile uint32 *) my_wait_event_info = 0;
   ```
   Keep the exported names `wait_event_begin_hook`, `wait_event_end_hook`,
   `wait_event_hook_depth` exactly as they are: `test_wait_hook` and the
   contrib module use them. Keep the ordering guarantees: begin hook runs
   after the event is published, end hook runs before it is cleared.
3. Rewrite the header comment that currently says there is deliberately
   no likely()/unlikely() hint. New text: the hint is placed for the
   no-consumer path, which is the path every backend runs unless a
   consumer is installed in that process; the enabled path pays one jump
   to the cold section; the depth guard and chaining are unchanged.
4. Commit body: what changed, why (the GCC pointer heuristic; code size at
   94 sites; spill sinking), what did not (recorded data, hook contract,
   ordinary pair still byte-identical to master), and how it was verified.

## Verification

1. Build as in the WPA brief (same meson line, then `ninja -C build`,
   `ninja -C build install`), then
   `meson test -C build --suite setup --suite test_wait_hook --suite pg_wait_event_tracing --print-errorlogs`.
   All 14 must pass; `test_wait_hook`'s expected output includes the
   observed depth values and must not change.
2. `ninja -C build headerscheck` and `ninja -C build cpluspluscheck` clean
   (if those targets exist in this build; report if not).
3. Codegen sanity on this machine (arm64 clang, not the real target):
   for `WaitEventSetWait` in `build/src/backend/storage/ipc/libpostgres...`
   or the `postgres` binary, `objdump -d --disassemble-symbols=WaitEventSetWait build/src/backend/postgres`
   before and after the commit, count instructions and count references
   to `wait_event_begin_hook`/`wait_event_end_hook` (via `nm` addresses or
   the adrp/ldr pairs). Also for `XLogWrite` count `stp`/`ldp`/`str`/`ldr`
   with `sp` operands (stack references). Put both counts before/after in
   the report. This is a sanity check only; the x86-64 GCC measurement is
   done elsewhere.
4. `git diff wet-v11 wet-v11-wpb --stat` must touch only
   `src/include/utils/wait_event.h` and
   `src/backend/utils/activity/wait_event.c`.

## Deliverable

`<workspace>/work/git/postgres_patch/v11/reports/wpb-report.md`
with the commit hash, test summary, codegen counts before/after, and
anything undone; plus `git format-patch -1 -o <workspace>/work/git/postgres_patch/v11/patches-wpb/`.
Final chat message at most 8 lines.

Note: add `-Dldap=disabled` to the meson line (macOS SDK OpenLDAP deprecation warnings fail -Dwerror; unrelated to this series).
