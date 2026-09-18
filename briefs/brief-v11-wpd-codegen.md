# WPD: x86-64 GCC codegen comparison for the timed wait pair

Compile-only work on a remote Linux box. No PostgreSQL server is started
anywhere. Nothing is pushed to any git remote.

## Hosts

- Remote build box: `ssh root@178.105.201.195` (hostname `pg-build`,
  Ubuntu 24.04, GCC 13.3, 4 vCPU, 7 GB RAM, 70 GB free). Key auth is set
  up; use `ssh -o BatchMode=yes`. Work under `/root/codegen/`.
- Local: patches are under `/Users/dmitryfomin/work/git/postgres_patch/v11/`:
  `patches-v11/v11-000{1..5}-*.patch` (the series) and
  `patches-wpb/*.patch` (one commit: cold slow path + unlikely hint).
  Copy them to the box with scp. Do not touch any local git worktree.

## Trees to build (all on the same base)

Base commit: `311df1dc039` on postgres/postgres master. On the box:
```
git clone --filter=blob:none https://github.com/postgres/postgres.git /root/codegen/src
cd /root/codegen/src && git checkout 311df1dc039
```
(If the filtered clone is slow, a full clone is acceptable.)

| Name | Content |
|---|---|
| `master` | base only |
| `v11` | base + the five v11 patches (`git am`) |
| `v11-wpb` | `v11` + the wpb patch |
| `control` | `v11-wpb` + a control patch you create (below) |

Create each as a local branch in that clone. `git am` must apply with no
fuzz warnings; report any.

Control patch: in `src/include/utils/wait_event.h`, make the two timed
functions' bodies identical to the ordinary pair's bodies (start: the
volatile store only; end: the volatile store of 0 only). Leave everything
else, including the slow functions in `wait_event.c`, untouched. Commit it
on branch `control` and save it with `git format-patch -1` to
`/Users/dmitryfomin/work/git/postgres_patch/v11/patches-control/` (scp
back). This is the "call sites compiled out" layout control used in the v7
measurement package.

## Build protocol (layout control matters)

Every build must use the SAME source path, SAME object path and SAME
configured prefix, sequentially, so binary layout differences come from
the code and not from embedded strings. In `/root/codegen/src`:
```
git checkout <branch> && make -s distclean 2>/dev/null; git clean -fdxq
./configure --prefix=/root/codegen/prefix CFLAGS='-O2 -g0' --without-icu --without-readline --without-zlib >configure.log 2>&1
make -s -j4 >make.log 2>&1
cp src/backend/postgres /root/codegen/bin/<branch>.postgres
sha256sum /root/codegen/bin/<branch>.postgres
```
Do `master` first. Record the exact configure line, `gcc -v` first line,
and the four SHA-256 values.

## Analysis

For each of the four binaries and each of these seven functions:
`WaitEventSetWait`, `FileReadV`, `LWLockAcquire`, `XLogWrite`,
`SlruInternalWritePage`, `CopyReadLine`, `pgaio_io_perform_synchronously`:

```
objdump -d --no-show-raw-insn -M intel --disassemble=<func> /root/codegen/bin/<b>.postgres > /root/codegen/dis/<b>/<func>.txt
```

Report per (binary, function):
1. instruction count (lines that start with an address and a colon);
2. stack-memory operand count: lines containing `[rsp` or `[rbp`
   (this is the metric the v7 package called "stack-memory operand
   counts"; count operands, not instructions, if a line has two);
3. hook-pointer references: lines mentioning `wait_event_begin_hook` or
   `wait_event_end_hook` in the symbolic comment objdump prints for
   rip-relative operands;
4. branch layout at each hook test: for every `cmp ... [rip+0x...]  # ... <wait_event_begin_hook>` or `<wait_event_end_hook>` (or an equivalent `mov`+`test`), record the very next conditional jump mnemonic and whether its target address is greater than the current address (forward). A `jne` forward means the null path is the fall-through (the goal); a `je` means the null path is the taken branch. Give counts of each form per binary.

Also for every binary:
5. `objdump -t <bin> | grep -E 'pgstat_wait_event_hook_(begin|end)_slow'` and
   `readelf -S <bin> | grep -E 'text'` plus, for `v11-wpb` and `control`,
   which section the two slow functions live in (`.text.unlikely` expected);
6. `size <bin>` (text/data/bss).

Save the raw disassembly directory tree and copy it back with
`scp -r root@178.105.201.195:/root/codegen/dis /Users/dmitryfomin/work/git/postgres_patch/v11/codegen/`,
together with `configure.log` head, the four `make.log` tails, and the
SHA list.

## Deliverable

`/Users/dmitryfomin/work/git/postgres_patch/v11/reports/wpd-report.md`:
one table per metric (rows = functions, columns = the four binaries),
the branch-layout counts, the section placement of the slow functions,
sizes, build provenance, and a plain-language reading of three
questions: (a) does `v11-wpb` restore `XLogWrite`'s stack-operand count
to `master`'s (v7 saw 36 vs 40)? (b) is the null path the fall-through
in `v11-wpb` and NOT in `v11`? (c) do `control` and `master` differ in
these seven functions at all, and where? Do not interpret beyond what
the counts show. Final chat message at most 8 lines.
