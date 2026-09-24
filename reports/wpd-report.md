# WPD: x86-64 GCC codegen comparison for the timed wait pair

Compile-only. No PostgreSQL server was started at any point (no
`initdb`/`pg_ctl`/`make check`). Nothing was pushed to any git remote.
All work happened on the remote box `pg-build` (178.105.201.195,
Ubuntu 24.04, GCC 13.3.0, 4 vCPU / 7 GB) under `/root/codegen/`.

## Build provenance

- Base commit: `311df1dc0392f06973cf98eac51d63cb007267ce` on
  `postgres/postgres` (this happened to be the tip of the mirror's
  `master` at clone time, so the pre-existing `master` branch needed
  no reset).
- `git am` of the five `patches-v11/v11-000{1..5}-*.patch` onto `v11`,
  and of `patches-wpb/0001-*.patch` onto `v11-wpb` (branched from
  `v11`): both applied cleanly, **no fuzz, no warnings** (plain
  `Applying: <subject>` lines only for all six patches).
- `control` branch = `v11-wpb` + one commit that deletes the hook-test
  and slow-call lines from `pgstat_report_wait_start_timed()` /
  `pgstat_report_wait_end_timed()` in `src/include/utils/wait_event.h`,
  leaving only the volatile store each ordinary function does (`wait_event.c`
  untouched). Exported as
  `patches-control/0001-control-make-timed-wait-event-pair-bodies-identical-.patch`.
  `git diff v11-wpb control --stat` touches only that one header line
  range (5 deletions).
- Build order: `master`, `v11`, `v11-wpb`, `control`, sequentially, same
  path (`/root/codegen/src`), same object tree, same prefix, each
  preceded by `make -s distclean; git clean -fdxq`.
- Configure line (identical for all four):
  `./configure --prefix=/root/codegen/prefix CFLAGS='-O2 -g0' --without-icu --without-readline --without-zlib`
- `configure` reports for all four:
  `using compiler=gcc (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0`
  `using CFLAGS=-Wall -Wmissing-prototypes -Wpointer-arith -Wdeclaration-after-statement -Werror=vla -Wmissing-format-attribute -Wold-style-declaration -Wimplicit-fallthrough=5 -Wcast-function-type -Wshadow=compatible-local -Wformat-security -fno-strict-aliasing -fwrapv -fexcess-precision=standard -Wno-format-truncation -Wno-stringop-truncation -O2 -g0 -Wstrict-prototypes -Wold-style-definition`
- `gcc -v` first line: `Using built-in specs.` (compiler identity per
  configure line above: GCC 13.3.0, Ubuntu 13.3.0-6ubuntu2~24.04.1).
- `make -s -j4 >make.log 2>&1`: all four `make.log` files are **empty
  (0 bytes)** — clean builds, no warnings, no errors, for all four trees.
- SHA-256 of `src/backend/postgres` for each tree:

| Binary | SHA-256 |
|---|---|
| master | `9314350300e4041b2ad121fd90e8a2da470b39c54541b138520dfc52b4df8b66` |
| v11 | `d141ee06ab44b1055ae497bef52d51253903eb709696a01f02a4dee2c630a2b2` |
| v11-wpb | `ffd283c4da05b1b953d161e15afe40261186944a24a53f98dc9af709307cca27` |
| control | `8c42db66dd134b193066a6a1480f564b60df507ebec5b0ca17c9b6e29f0cb5c5` |

All four distinct. `v11-wpb`'s hash was independently reproduced by a
second from-clean rebuild on the same branch (see verification note
under table 5) — identical SHA-256 both times, i.e. the build is
deterministic under this protocol.

Raw disassembly tree, configure.log head, make.log tails (all empty),
sha256 list, symbol/section dumps and `size` output are under
`<workspace>/work/git/postgres_patch/v11/codegen/`.

## 1. Instruction count

Lines starting with an address and colon, per `objdump -d
--disassemble=<func>` output.

| Function | master | v11 | v11-wpb | control |
|---|---:|---:|---:|---:|
| WaitEventSetWait | 304 | 327 | 308 | 304 |
| FileReadV | 57 | 77 | 61 | 57 |
| LWLockAcquire | 75 | 96 | 80 | 75 |
| XLogWrite | 398 | 419 | 402 | 398 |
| SlruInternalWritePage | 134 | 134 | 134 | 134 |
| CopyReadLine | 444 | 444 | 444 | 444 |
| pgaio_io_perform_synchronously | 66 | 110 | 73 | 66 |

`SlruInternalWritePage` and `CopyReadLine` are identical across all
four columns and have zero hook references (table 3) — at this base
commit these two functions do not call the timed wait-event pair, so
none of the patches touch their codegen.

## 2. Stack-memory operand count (`[rsp` / `[rbp` occurrences)

Occurrences counted, not instructions (a line with two would count as
two; none did — every hit had exactly one bracketed `[rsp`/`[rbp`
operand, no `[rbp` hits at all in these functions).

| Function | master | v11 | v11-wpb | control |
|---|---:|---:|---:|---:|
| WaitEventSetWait | 42 | 42 | 42 | 42 |
| FileReadV | 2 | 2 | 2 | 2 |
| LWLockAcquire | 1 | 1 | 1 | 1 |
| XLogWrite | 38 | 38 | 38 | 38 |
| SlruInternalWritePage | 14 | 14 | 14 | 14 |
| CopyReadLine | 31 | 31 | 31 | 31 |
| pgaio_io_perform_synchronously | 0 | 4 | 0 | 0 |

## 3. Hook-pointer reference count

Lines whose rip-relative symbolic comment names
`wait_event_begin_hook` or `wait_event_end_hook`.

| Function | master | v11 | v11-wpb | control |
|---|---:|---:|---:|---:|
| WaitEventSetWait | 0 | 2 | 2 | 0 |
| FileReadV | 0 | 2 | 2 | 0 |
| LWLockAcquire | 0 | 2 | 2 | 0 |
| XLogWrite | 0 | 2 | 2 | 0 |
| SlruInternalWritePage | 0 | 0 | 0 | 0 |
| CopyReadLine | 0 | 0 | 0 | 0 |
| pgaio_io_perform_synchronously | 0 | 3 | 4 | 0 |

`pgaio_io_perform_synchronously` has two begin-hook call sites (a
retry/second I/O path), hence 3 in `v11` (2 begin + 1 end) and 4 in
`v11-wpb` (2 begin + 2 end) rather than the 2/2 pattern of the other
four functions.

## 4. Branch layout at each hook test

For every `cmp .../mov+test` against `wait_event_begin_hook` or
`wait_event_end_hook`, the mnemonic of the very next conditional jump
and whether its target address is greater than the jump's own address
(forward). `master` and `control` have no such tests (0 rows — the
hook check isn't compiled in). Counts of each (mnemonic, direction)
form, summed over all seven functions:

| Binary | jne forward | jne backward | je forward | je backward |
|---|---:|---:|---:|---:|
| master | 0 | 0 | 0 | 0 |
| v11 | 0 | 0 | 11 | 0 |
| v11-wpb | 0 | 10 | 2 | 0 |
| control | 0 | 0 | 0 | 0 |

`v11` is uniformly `mov reg,[hook]; test reg,reg; je <forward, over the
call>` — the null-pointer case is the one that jumps (taken branch);
the hook-active case is the fall-through into the call sequence.

`v11-wpb` is uniformly `cmp [hook],0; jne <target>` where the target is
a `.cold`-suffixed out-of-line block; 10 of 12 tests have that target
at a lower address than the branch (`jne backward` by raw address —
the `.cold` partition for these translation units landed early in
`.text`, ahead of the hot code, not literally "after" it; direction is
reported as measured, not interpreted). In these 10, the null case
falls through and the hook-active case takes the jump to the cold
block — the intended layout.

The other 2 of 12 (`LWLockAcquire`'s begin-hook test, and one of
`pgaio_io_perform_synchronously`'s two end-hook tests) compile to
`cmp [hook],0; je <forward, to the post-hook resumption point>`
immediately followed by an unconditional `jmp .cold`. By the counting
rule this is a `je forward`, i.e. formally "null path is the taken
branch" — but the taken branch is a single short hop straight to the
resumption code, while the alternate (hook-active) path falls through
into one extra unconditional jump before reaching the cold block. So
even these 2 outliers still send hook-active traffic through the cold
partition and keep the null path off of an extra jump; they just don't
fit the plain `jne`-to-cold shape the other 10 do. The raw disassembly
under `codegen/dis/v11-wpb/LWLockAcquire.txt` and
`.../pgaio_io_perform_synchronously.txt` shows both instances.

## 5. Slow-function symbols and section placement; `.text`

`objdump -t <bin> | grep pgstat_wait_event_hook_(begin|end)_slow`:

- `master`, `v11`: not found (functions don't exist pre-wpb).
- `v11-wpb`: both present, `objdump -t` lists their section as `.text`.
- `control`: both present, `objdump -t` lists their section as `.text`.

`readelf -S <bin> | grep text`: all four binaries have a single `.text`
output section at the same address/offset (`00000000000e7fb0`) — there
is no separate `.text.unlikely` **output** section in the linked
binary for any of them; GNU ld's default script folds `.text.unlikely`
input sections into the `.text` output section, so `objdump -t`'s
per-symbol section name is `.text` regardless.

To check the compiler's own placement (the actual "which section"
question) we inspected the **object file**
`src/backend/utils/activity/wait_event.c.o` before linking, for both
`v11-wpb` and `control` (`control`'s `wait_event.c` is byte-identical
to `v11-wpb`'s — confirmed with `git diff v11-wpb control --
src/backend/utils/activity/wait_event.c`, zero lines — and a
from-clean rebuild of `v11-wpb` reproduced the same
`ffd283c4...` SHA-256, so this is a real, reproducible artifact, not a
stale one):

```
  5 .text.unlikely 0000019a  ...
0000000000000000 g F .text.unlikely  0000000000000029 pgstat_wait_event_hook_begin_slow
0000000000000029 g F .text.unlikely  0000000000000032 pgstat_wait_event_hook_end_slow
```

Both functions are in `.text.unlikely` at the object-file level for
both `v11-wpb` and `control`, i.e. `.text.unlikely` as expected — it
just doesn't survive as a distinct section name after linking on this
toolchain/linker-script combination.

`size <bin>` (bytes):

| Binary | text | data | bss |
|---|---:|---:|---:|
| master | 10483381 | 272376 | 277184 |
| v11 | 10496015 | 272376 | 277216 |
| v11-wpb | 10491773 | 272376 | 277216 |
| control | 10483821 | 272376 | 277216 |

## Reading of the three questions

**(a) Does `v11-wpb` restore `XLogWrite`'s stack-operand count to
`master`'s (v7 saw 36 vs 40)?** Not applicable here as a "restore":
in this v11 build, `XLogWrite`'s `[rsp]`/`[rbp]` count is **38 in all
four binaries** (`master`, `v11`, `v11-wpb`, `control`) — there is no
gap between `master` and `v11` to restore at this base commit/compiler.
The 36-vs-40 split the v7 package saw does not reproduce here; the
counts simply don't move for this function under this patch series and
toolchain.

**(b) Is the null path the fall-through in `v11-wpb` and NOT in
`v11`?** In `v11`, yes to the second half: every one of the 11 hook
tests is `je forward`, meaning the null-hook case is the one that
jumps and the hook-active case is the fall-through — the null (common)
path is never the fall-through in `v11`. In `v11-wpb`, 10 of 12 tests
are `jne` with the hook-active case taking the jump to a `.cold` block
and the null case falling through, which is the intended fall-through
layout; the other 2 of 12 use a `je`-forward-plus-unconditional-`jmp`
shape where the null case is technically a taken (but very short,
forward) branch rather than a literal fall-through, while the
hook-active case still ends up routed through the cold partition. So:
mostly yes, with two call sites in `v11-wpb` that achieve the same
practical routing (hook-active off the hot path) through a different
instruction shape rather than a true fall-through for the null case.

**(c) Do `control` and `master` differ in these seven functions at
all, and where?** No, not in instruction content: after normalizing
away absolute addresses/displacements (which shift because `control`'s
binary retains the two now-unreferenced `pgstat_wait_event_hook_*_slow`
functions and other incidental layout effects), the disassembly of all
seven functions is line-for-line identical between `master` and
`control` — same instruction counts (table 1), same stack-operand
counts (table 2), zero hook references in both (table 3), zero branch
tests in both (table 4). The only measured difference between the two
binaries is overall `.text` size (10483821 vs 10483381 bytes, +440),
attributable to the two vestigial slow functions `control` still
carries in `wait_event.c` (91 bytes of code, no longer called from
anywhere, not eliminated because they're external symbols and the link
was not `--gc-sections`) plus whatever small padding/alignment shift
that and other layout differences elsewhere in the binary produce.
