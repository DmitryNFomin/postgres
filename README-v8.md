# wait-event tracing v8: work notes for independent verification

This branch holds no PostgreSQL code. It holds the plan, the per-work-package
briefs, the implementers' reports, and the verification notes behind the
v8 series of the wait-event timing patch. It lets a reviewer, human or model,
check the work independently from the code branches in this repository.

Everything here was produced by a delegated workflow: one model wrote the
plan and the briefs and reviewed every diff, and implementer agents wrote the
code. **Nothing here has been reviewed by the PostgreSQL community yet.**
Local paths are replaced by `<workspace>` (the working directory beside the
PostgreSQL checkout) and `<scratch>` (a session temp directory).

## Context

| What | Where |
|---|---|
| pgsql-hackers thread | https://www.postgresql.org/message-id/CAPHG-0mAOn05ae6Kqx1wHXxzOk4E5W7ajjd=QBhgkR7a0uyQmw@mail.gmail.com |
| Andrey Borodin's review that triggered v7/v8 (2026-09-05) | https://www.postgresql.org/message-id/9F597BE1-53A7-4615-A2D4-60AF0CAE48E6@yandex-team.ru |
| v7 reply: benchmarks, decision, v6 defects, two core patches (2026-09-09) | https://www.postgresql.org/message-id/CAPHG-0n55AyeBbS=s0FF4P0pHD6RA-4sBfj-SysExwKfF6THqQ@mail.gmail.com |
| Commitfest entry | https://commitfest.postgresql.org/patch/6984/ |
| Benchmark and peer-review package (redacted, with VERIFY.sh) | https://github.com/DmitryNFomin/pg-wait-event-hook-benchmark/releases/tag/v2026-09-09 |

Where a brief says `<scratch>/v7rev/wait-hook-peer-review-20260909/...`, that
is the extracted package above. In particular `docs/SOURCE-REVIEW-v6-v7.md`
is the defect analysis the fixes implement.

## Files

- `IMPLEMENTATION-PLAN-v8.md` — the plan of record: design, decisions,
  findings from review and CI (section 4.2b), work-package table with status.
- `briefs/brief-v8-wp*.md` — the exact instructions each implementer got.
- `reports/wp*-report.md` — what each implementer says it did.
- `verification/v7review-verification.md` — independent check of the
  peer-review package (VERIFY.sh, the seven v6 defects re-derived with
  file:line).

## Code branches (this repository)

All are based on master `412ef97d925c`. Each work package is a branch; the
`ci/…` names are identical copies pushed to run this fork's CI.

| Branch | Content | Status |
|---|---|---|
| `wet-v7-rfc` | v7-0001 begin/end hooks + v7-0002 timed-pair call-site conversions (posted to the list) | CI green; cfbot green |
| `wet-v8-wp1` | `src/test/modules/test_wait_hook`: the hook contract test | CI run 34580643617 green; test executed on Linux, Autoconf, Windows |
| `wet-v8-wp2` | `contrib/pg_wait_event_tracing`, statistics level; v6 defect fixes 1, 2, 4, 5 | CI run 34609838704 green on all 9 jobs |
| `wet-v8-wp4a` | TAP tests 001–004 for fixes 1, 2, 4, 5 | CI run 34636885229 green, all tests executed, no skips |
| `wet-v8-wp2b` | server-side processes collect from process start: control table in fixed shared memory, reserved region, lock-free in-hook claim | code done; see known issues |
| `wet-v8-wp3` | trace level: ring, reader, orphan lifecycle, markers, attribution | in progress |

CI runs are at https://github.com/DmitryNFomin/postgres/actions. Local
validation on the development host is compile-only by the owner's rule; all
test execution happens on this fork's CI.

## What to verify

1. **Plan vs. code.** For each branch, compare its brief with the diff
   against its base (`git diff <base>..<branch>`). Report anything the brief
   required that is missing, or anything added that it did not ask for.
2. **The hook contract** (`src/include/utils/wait_event.h` on
   `wet-v7-rfc`): begin/end hooks must use only preallocated backend-local
   state, with no allocation, lock, wait or error. Check every function
   reachable from `pwet_wait_begin()` / `pwet_wait_end()` in
   `contrib/pg_wait_event_tracing/pg_wait_event_tracing.c`.
3. **The v6 defect fixes**, against `docs/SOURCE-REVIEW-v6-v7.md` in the
   package (V6-1 … V6-7): fixed on `wet-v8-wp2` are 1 (sparse memory),
   2 (owner token checked by readers), 4 (reset authorisation as
   `pg_signal_backend()`), 5 (reset race, owner re-check under the lock).
   3, 6, 7 are in progress.
4. **Concurrency on `wet-v8-wp2b`**: `pwet_claim_fixed_slot()` vs.
   `pwet_fixed_owner_matches()` / `pwet_fixed_owner_unchanged()` (barrier
   pairing), and ownership/reset interplay with `pwet_request_reset()`.
5. **Tests**: do the regression and TAP tests actually assert what their
   names claim, on every platform (see plan section 4.2b for the platform
   traps already hit)?
6. **Design decisions** a reviewer may disagree with: plan section 4.2a
   (reserved memory for server-side processes, ~10 MiB at default settings,
   only when capture is on at start) and section 5.3 (query markers without
   a new core hook).

## Known open issues (confirm them, and look for others)

- `wet-v8-wp2b`: the shared-memory startup hook creates the server-process
  region even when capture was off at postmaster start (the slot range does
  not depend on capture), although it was not reserved; with capture off,
  the default, the postmaster would fail to start. A fix is in progress.
- `wet-v8-wp2b`: `t/006_server_processes.pl` waits for an I/O worker, but
  some CI jobs run with `io_method = io_uring`, which has none.
- Assign-hook ordering (guc.c calls the enum assign hook before storing the
  value): fixed on `wet-v8-wp2b` via `pwet_capture_effective`, not yet on
  `wet-v8-wp2`.
- Counters are int64 and are read without synchronisation with the owner's
  writes; on 32-bit platforms a reader can see a torn value.
