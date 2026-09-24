# WPB core report: out-of-line hook slow path

Branch: `wet-v11-wpb` (worktree `<workspace>/work/git/postgres_patch/wet-v11-wpb`),
based on `wet-v11` @ `0cf1c2feaef`.

Commit: `b8302eecbc7f94cb4745f53e1c58a8e226f3f0bf`
"Move timed wait-event hook path out of line, hint the null-hook branch"
Author/committer: Dmitry Fomin <fomin.list@gmail.com>
Trailer: `Discussion: https://postgr.es/m/CAPHG-0mAOn05ae6Kqx1wHXxzOk4E5W7ajjd=QBhgkR7a0uyQmw@mail.gmail.com`
(no Co-Authored-By / Claude trailer)

Patch exported to:
`<workspace>/work/git/postgres_patch/v11/patches-wpb/0001-Move-timed-wait-event-hook-path-out-of-line-hint-the.patch`

`git diff wet-v11 wet-v11-wpb --stat` touches only:
- `src/include/utils/wait_event.h`
- `src/backend/utils/activity/wait_event.c`

## Change summary

- Added `pgstat_wait_event_hook_begin_slow()` and
  `pgstat_wait_event_hook_end_slow()` to `wait_event.c`, marked
  `pg_noinline pg_attribute_cold`. They hold exactly the depth-guarded
  hook-call logic previously inlined, re-reading the relevant hook
  pointer from the global (documented as safe since it cannot change
  between the inline test and this call in-process).
- `wait_event.h`'s `pgstat_report_wait_start_timed()` /
  `pgstat_report_wait_end_timed()` now do the ordinary wait-event
  store/clear plus a single `unlikely()`-hinted pointer test that calls
  out to the matching slow function when true.
- Rewrote the header comment above the pair to explain the hint
  placement (no-consumer path is what every backend runs; enabled path
  costs one extra taken jump).
- Exported names `wait_event_begin_hook`, `wait_event_end_hook`,
  `wait_event_hook_depth` unchanged (type, name, linkage). Ordering
  guarantees unchanged (event published before begin hook runs, cleared
  after end hook runs). Non-timed `pgstat_report_wait_start/end()` pair
  untouched.

## Test summary

Build: `meson setup build --buildtype=debugoptimized -Dcassert=true
-Dwerror=true -Dinjection_points=true -Dtap_tests=enabled -Dssl=openssl
-Dldap=disabled -Dprefix=$PWD/install`, then `ninja -C build`,
`ninja -C build install` -- all clean, no warnings from the two changed
files.

`meson test -C build --suite setup --suite test_wait_hook --suite
pg_wait_event_tracing --print-errorlogs`:

```
Ok:                14
Fail:              0
```

All 14 tests pass, including `test_wait_hook/regress` (1 subtest) whose
expected output (observed depth values) is byte-for-byte the same file
as on `wet-v11` -- not modified by this commit.

`ninja -C build headerscheck` and `ninja -C build cpluspluscheck`: both
targets exist and were run, but both fail in this environment for a
pre-existing, unrelated reason -- `fatal error: 'openssl/ssl.h' file not
found` while checking `libpq/libpq-be.h`, `libpq/libpq.h`,
`libpq/oauth.h`, `interfaces/libpq/*` (macOS SDK/homebrew openssl include
path issue). The identical set of failures, with identical messages,
reproduces running `ninja headerscheck` against an unmodified `wet-v11`
build tree, so this is not a regression from this change. Neither
`wait_event.h` nor `wait_event.c` appear among the failing headers/files
in either run.

## Codegen sanity (arm64 clang, this machine only)

"Before" = `wet-v11` build (`<workspace>/work/git/postgres_patch/wet-v11/build`,
same meson options, already built). "After" = this commit's build.
Both are `debugoptimized` builds of `build/src/backend/postgres`
(`objdump -d --disassemble-symbols=<sym>`, symbols carry a leading `_`
on this Mach-O target).

### `WaitEventSetWait`

| metric | before | after |
|---|---|---|
| total instructions | 334 | 318 |
| loads of `wait_event_begin_hook` (offset `#0x7b8` off the hook page) | 1 | 1 |
| loads of `wait_event_end_hook` (offset `#0x7c0`) | 1 | 1 |
| inline references to `wait_event_hook_depth` (offset `#0x7c8`) | 2 | 0 |

The hook-pointer test is still done exactly as often (needed for the
`unlikely()` test itself); the depth-guard load/store that used to be
inlined alongside it is gone from the caller, moved into the new cold
functions.

### `XLogWrite`

| metric | before | after |
|---|---|---|
| total instructions | 542 | 530 |
| loads of `wait_event_begin_hook` (`#0x7b8`) | 1 | 1 |
| loads of `wait_event_end_hook` (`#0x7c0`) | 1 | 1 |
| inline references to `wait_event_hook_depth` (`#0x7c8`) | 1 | 0 |
| `stp`/`ldp`/`str`/`ldr` with an `sp` operand | 7 / 9 / 7 / 22 | 7 / 9 / 7 / 22 |

Stack-reference counts in `XLogWrite` are unchanged on this arm64/clang
build; the AArch64 calling convention and register allocator do not
reproduce the four extra x86-64/GCC stack spills the v7 measurement
found. Per the brief, this is a sanity check only -- the x86-64 GCC
spill measurement is done elsewhere. Total instruction count in
`XLogWrite` still drops (542 -> 530), consistent with less code being
inlined at the call site.

The two new functions (`pgstat_wait_event_hook_begin_slow`,
`pgstat_wait_event_hook_end_slow`) are present in the binary as
standalone, non-inlined symbols (confirmed via `nm`/`objdump`); on this
Mach-O/clang target they are not physically relocated to a separate
`.text.unlikely`-equivalent section (unlike ELF/GCC hot-cold splitting),
but they are out of line at every call site, which is what this change
requires.

## Undone / notes

- `headerscheck`/`cpluspluscheck` could not be verified clean end-to-end
  due to the pre-existing environment issue above; confirmed it is not
  caused by this change (reproduces on unmodified `wet-v11`, and neither
  changed file appears in the failure list).
- The x86-64/GCC stack-spill re-measurement referenced in the brief's
  motivation (four extra stack references in `XLogWrite`) is out of
  scope for this arm64/clang machine and, per the brief, is done
  elsewhere.

## Amendment

Per review, the header comment above `pgstat_report_wait_start_timed()`
was rewritten so the *why* of the `unlikely()` hint (GCC's static
branch predictor treats a NULL-compared pointer as non-NULL) lives in
the code itself, not only in the commit body. Only
`src/include/utils/wait_event.h` changed; the commit was amended in
place (author, committer, body text, and the single `Discussion:`
trailer all unchanged; `--author` re-specified to `Dmitry Fomin
<fomin.list@gmail.com>` and author/committer dates preserved).

New commit: `f4a9d7d2ccdb1034e1b441195bd8c97d25d1e7bf`
"Move timed wait-event hook path out of line, hint the null-hook branch"

`git diff wet-v11 wet-v11-wpb --stat` still touches only
`src/include/utils/wait_event.h` and
`src/backend/utils/activity/wait_event.c`.

Patch regenerated (old file deleted first):
`<workspace>/work/git/postgres_patch/v11/patches-wpb/0001-Move-timed-wait-event-hook-path-out-of-line-hint-the.patch`

Rebuilt with `ninja -C build` (clean) and re-ran `meson test -C build
--suite setup --suite test_wait_hook --suite pg_wait_event_tracing
--print-errorlogs`:

```
Ok:                14
Fail:              0
```

All 14 tests still pass, `test_wait_hook/regress` output unchanged. Not
pushed.
