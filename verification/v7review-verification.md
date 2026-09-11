# Independent verification of the wait-hook peer-review package

Verified 2026-09-09.  `pkg/` below means the package root named in the
verification brief.  PostgreSQL source anchors are from
`git show wet-series:<path>`; the reviewed head is `186314d20a3`.
(`pkg/docs/BUG-FINDINGS-SUMMARY.txt:29-40`)  I did not run a benchmark
workload.

## Part 1 — package verification

`./VERIFY.sh` exited 0.  Its complete stdout/stderr is in
`blog/demo/v7review-verify-output.txt`.

| Stage | Result | Evidence |
|---|---|---|
| 1. Package manifest | PASS | `blog/demo/v7review-verify-output.txt:1` |
| 2. Protocol-bound evidence | PASS | `blog/demo/v7review-verify-output.txt:2` |
| 3. Strict and complete report reproduction | PASS | `blog/demo/v7review-verify-output.txt:3`; the strict reporter's status 4 is explicitly expected before byte comparison (`pkg/VERIFY.sh:288-315`) |
| 4. Peer-review contrast reproduction | PASS | `blog/demo/v7review-verify-output.txt:4` |
| 5. Self-contained source and patch reconstruction | PASS | `blog/demo/v7review-verify-output.txt:5` |
| 6. Full-archive identity declarations | PASS | `blog/demo/v7review-verify-output.txt:6` |

The script reported `VERIFY: all checks passed`; no stage was unavailable.
(`blog/demo/v7review-verify-output.txt:7`)

## Part 2 — adversarial source verification

| Finding | Independent source trace | Verdict | Severity I assign |
|---|---|---|---|
| V6-1 dense allocation | The first capturing hot-path attach is allowed to create storage (`wet-series:src/backend/utils/activity/wait_event_timing.c:506-512`).  It computes `NUM_WAIT_EVENT_TIMING_SLOTS * stride` and allocates that entire region with `DSA_ALLOC_ZERO` (`wet-series:src/backend/utils/activity/wait_event_timing.c:559-605`).  This is lazy cluster-wide allocation, not sparse per-backend allocation. | **REAL** | **P1** — surprising, potentially large allocation in a wait path. |
| V6-2 stale predecessor data | Backend initialization only nulls local pointers and performs trace-orphan cleanup; it does not clear the timing slot (`wet-series:src/backend/utils/activity/wait_event_timing.c:945-973`).  Exit likewise clears only local pointers (`wet-series:src/backend/utils/activity/wait_event_timing.c:975-989`).  The timing counters are zeroed only in lazy attach, reached on a backend's first wait with capture enabled (`wet-series:src/backend/utils/activity/wait_event_timing.c:844-852`, `wet-series:src/backend/utils/activity/wait_event_timing.c:901-924`).  The timing reader indexes the slot, then checks only that the *current* backend-status entry is live and visible to the caller before emitting every nonzero old counter (`wet-series:src/backend/utils/activity/wait_event_timing.c:2141-2171`); the overflow reader has the same live-entry/permission checks and no owner check (`wet-series:src/backend/utils/activity/wait_event_timing.c:2229-2251`).  Therefore A can record and exit, B can reuse the ProcNumber with capture off, and a reader can combine B's PID/type/user metadata with A's counters. | **REAL** | **P1** — cross-PID and potentially cross-role statistics misattribution. |
| V6-3 EXEC_BACKEND ordering | Both normal and auxiliary initialization call `pgstat_set_wait_event_timing_storage()` before their `EXEC_BACKEND` `AttachSharedMemoryStructs()` calls (`wet-series:src/backend/storage/lmgr/proc.c:543-578`, `wet-series:src/backend/storage/lmgr/proc.c:719-760`).  The former invokes orphan cleanup (`wet-series:src/backend/utils/activity/wait_event_timing.c:953-972`), but the trace-control pointer starts NULL and cleanup immediately returns while it is NULL (`wet-series:src/backend/utils/activity/wait_event_timing.c:327-333`, `wet-series:src/backend/utils/activity/wait_event_timing.c:1700-1707`).  The request registers `&WaitEventTraceCtl` as the destination pointer (`wet-series:src/backend/utils/activity/wait_event_timing.c:1175-1181`); only the later attach walk re-establishes such struct pointers (`wet-series:src/backend/storage/ipc/ipci.c:82-105`, `wet-series:src/backend/storage/ipc/shmem.c:648-657`).  A surviving ORPHANED slot is then explicitly skipped by trace attach (`wet-series:src/backend/utils/activity/wait_event_timing.c:1434-1456`). | **REAL** | **P1** — ordinary ProcNumber reuse can silently disable trace on EXEC_BACKEND. |
| V6-4 reset authorization | Cross-backend reset checks only membership in `pg_signal_backend`, then accepts either a normal or auxiliary PID (`wet-series:src/backend/utils/activity/wait_event_timing.c:2322-2345`).  Actual backend signaling refuses auxiliary processes, protects role-less and superuser-owned targets from nonsuperusers, and otherwise checks target-role or `pg_signal_backend` privilege (`wet-series:src/backend/storage/ipc/signalfuncs.c:52-101`).  The claimed authorization gap is exact. | **REAL** | **P2** — unauthorized destruction of diagnostic state, including privileged/role-less targets. |
| V6-5 PID-to-ProcNumber race | `BackendPidGetProc()` acquires `ProcArrayLock`, finds the `PGPROC`, releases the lock, and returns a pointer whose continued meaning is explicitly the caller's responsibility (`wet-series:src/backend/storage/ipc/procarray.c:3153-3175`).  Reset later derives the ProcNumber and publishes after that unlocked lookup (`wet-series:src/backend/utils/activity/wait_event_timing.c:2333-2345`); publication unconditionally bumps the indexed slot generation and latches its current occupant (`wet-series:src/backend/utils/activity/wait_event_timing.c:2257-2279`).  A successor that has already lazy-attached snapshots the old generation (`wet-series:src/backend/utils/activity/wait_event_timing.c:904-924`) and will consume a later bump at its next timed `wait_end` (`wet-series:src/backend/utils/activity/wait_event_timing.c:1881-1900`).  If the bump occurs *before* B attaches, B snapshots the bumped value at attach and does not consume it.  The race is thus conditional but genuinely reachable; no operating-system PID reuse is needed. | **REAL** | **P2** — wrong-backend reset under an injectable ownership race. |
| V6-6 marker contract | Documentation promises matched Parse/Bind/Execute phase pairs and nested pairs (`wet-series:doc/src/sgml/monitoring.sgml:4532-4549`).  Code emits markers only when stored `st_query_id` changes (`wet-series:src/backend/utils/activity/backend_status.c:651-693`); Parse reports the nonzero ID only after transform/jumble/hooks (`wet-series:src/backend/parser/analyze.c:130-157`), while Bind and Execute flush the prior ID and start the next sequentially (`wet-series:src/backend/tcop/postgres.c:1729-1750`, `wet-series:src/backend/tcop/postgres.c:2235-2254`).  `ExecEnd` is also emitted before the ExecutorEnd hook/body rather than after cleanup (`wet-series:src/backend/executor/execMain.c:479-488`). | **REAL** | **P2 (contract)** — documented phase durations cannot be derived as promised. |
| V6-7 direct-reader API | Documentation calls direct access through `WaitEventTraceCtl->lock` the supported extension interface (`wet-series:doc/src/sgml/monitoring.sgml:4443-4468`).  The pointer is file-static (`wet-series:src/backend/utils/activity/wait_event_timing.c:327-333`); the public header describes the protocol and exports other trace symbols but no control-pointer/DSA accessor (`wet-series:src/include/utils/wait_event_timing.h:313-358`). | **REAL** | **P2 (API)** — the advertised external interface cannot compile. |

## Part 3 — experiment-integrity spot checks

### Timed-site coverage

An independent textual count over patch 04 produced 92 deleted ordinary
`pgstat_report_wait_start(` call lines and 94 added
`pgstat_report_wait_start_timed(` occurrences.  The latter comprise 92 call
sites plus the declaration and definition at patch lines 3212 and 3264; the
first and last converted calls are at lines 1574 and 3197.
(`pkg/source/patches/04-timed-site-hook-vs-tested-master.patch:1573-1581`,
`pkg/source/patches/04-timed-site-hook-vs-tested-master.patch:3192-3200`,
`pkg/source/patches/04-timed-site-hook-vs-tested-master.patch:3208-3213`,
`pkg/source/patches/04-timed-site-hook-vs-tested-master.patch:3259-3268`)
This confirms conversion of all 92 audited starts; it does not by itself prove
semantic begin/end placement, a limitation the source review also records.
(`pkg/docs/SOURCE-REVIEW-v6-v7.md:1884-1889`)

### W1 cached `FileReadV`

I recomputed the 12 repetition-paired contrasts directly from the W1 JSON
rows: transition NULL minus control is +0.076879 ns/iteration and timed-site
NULL minus control is +2.522631 ns/iteration.  The relevant observations are
distributed across the raw W1 block; for example, timed-site deltas turn
strongly positive in repetitions 8-12 while both signs occur earlier.
(`pkg/evidence/complete-matrix/results/w1ro02/raw-results.jsonl:25-744`, with
representative timed-site rows in
`pkg/evidence/complete-matrix/results/w1ro02/raw-results.jsonl:457-487`,
`pkg/evidence/complete-matrix/results/w1ro02/raw-results.jsonl:522-592`, and
`pkg/evidence/complete-matrix/results/w1ro02/raw-results.jsonl:642-717`)

Patch 04 changes `FileReadV` from one ordinary start/end pair to one timed
start/end pair; there is no second hook pair (`pkg/source/patches/04-timed-site-hook-vs-tested-master.patch:2822-2834`).
The timed helpers store/clear wait state and perform exactly one begin and one
end hook check (`pkg/source/patches/04-timed-site-hook-vs-tested-master.patch:3230-3249`,
`pkg/source/patches/04-timed-site-hook-vs-tested-master.patch:3259-3277`).
The retained transition-NULL and timed-site-NULL `FileReadV` instruction
streams are identical (including addresses) apart from the objdump input-path
header; their two controls likewise have identical instruction streams.
(`pkg/evidence/complete-matrix/results/w1ro02/spill-inspection/borodin-null-FileReadV.objdump:11-88`,
`pkg/evidence/complete-matrix/results/w1ro02/spill-inspection/corrected-null-FileReadV.objdump:11-88`,
`pkg/evidence/complete-matrix/results/w1ro02/spill-inspection/borodin-stub-FileReadV.objdump:11-67`,
`pkg/evidence/complete-matrix/results/w1ro02/spill-inspection/corrected-stub-FileReadV.objdump:11-67`)

Thus there is expected extra work versus control (two NULL-hook pointer
tests/branches), but no double hook and no variant-specific `FileReadV` layout
that explains why one paired estimate is larger.  The raw rows and identical
code do not support a more specific causal claim.

### Register-pressure evidence

Stack-memory-operand counts are available for master and both layout controls,
not just the two hooks.  They are counts of stack references, not proof that
each reference is a compiler spill.
(`pkg/evidence/complete-matrix/results/w1ro02/spill-inspection/report.txt:4-8`)

| Function | master | transition hook | transition control | timed-site hook | timed-site control |
|---|---:|---:|---:|---:|---:|
| `WaitEventSetWait` | 40 | 40 | 40 | 40 | 40 |
| `FileReadV` | 0 | 0 | 0 | 0 | 0 |
| `LWLockAcquire` | 0 | 0 | 0 | 0 | 0 |
| `XLogWrite` | 36 | 40 | 36 | 40 | 36 |
| `SlruInternalWritePage` | 12 | 12 | 12 | 12 | 12 |
| `CopyReadLine` | 32 | 32 | 32 | 32 | 32 |
| `pgaio_io_perform_synchronously` | 0 | 0 | 0 | 0 | 0 |

These are the retained master, transition-hook/control, and timed-site-hook/control
rows respectively (`pkg/evidence/complete-matrix/results/w1ro02/spill-inspection/report.txt:8-15`,
`pkg/evidence/complete-matrix/results/w1ro02/spill-inspection/report.txt:23-50`).
Therefore hook-versus-master and hook-versus-control add four stack references
only in `XLogWrite`; the other six selected functions have equal counts.

### Exit-141 recovery provenance

The executed wrapper runs all three workload groups before invoking spill
inspection (`pkg/tests/executed-harness/amended/run_amended_matrix.sh:85-91`),
and the retained W4-W6 driver output reaches every position of W6d repetition
12 before naming the raw result file (`pkg/tests/executed-harness/amended/amended-w1ro02-W4-W6.out:865-877`).
The original inspection ran under `pipefail` and used an early-exit `awk` on
`objdump -t` (`pkg/evidence/complete-matrix/results/w1ro02/recovery-exit-141/inspect_x86_spills.failed.sh:1-2`,
`pkg/evidence/complete-matrix/results/w1ro02/recovery-exit-141/inspect_x86_spills.failed.sh:63-75`),
while the retained exit marker is 141 (`pkg/evidence/complete-matrix/results/w1ro02/recovery-exit-141/original-driver.exit:1`).
This confirms that the identified failing pipeline is post-workload and is a
concrete source of the observed status 141.

The same-run runner rejects a workload as soon as that workload already has
raw rows (`pkg/tests/executed-harness/amended/run_full_matrix.sh:774-783`,
`pkg/tests/executed-harness/amended/run_full_matrix.sh:827-836`).  The package
contains exactly the predeclared 2,040 schedule-matching rows, which the
verification script checks and which stage 2 passed
(`pkg/VERIFY.sh:176-235`; `blog/demo/v7review-verify-output.txt:2`); the final
row is W6d repetition 12 at 2026-09-09T07:26:45Z
(`pkg/evidence/complete-matrix/results/w1ro02/raw-results.jsonl:2040`).  The
recovery record says only inspection/reporting were rerun and records
finalization 0 (`pkg/evidence/complete-matrix/results/w1ro02/recovery-exit-141/RECOVERY.txt:1-6`,
`pkg/evidence/complete-matrix/results/w1ro02/recovery-exit-141/finalization.exit:1`).
These artifacts are internally consistent with no workload rerun.  As a
forensic limitation, a self-contained package without an independent host
audit trail cannot prove the negative against deliberate replacement of raw
files.

### Amendment timing

Within the bound run, the *operational* report-only amendment precedes the
data: the wrapper refuses any other W1 mode
(`pkg/tests/executed-harness/amended/run_amended_matrix.sh:12-20`),
the runner writes or validates `protocol.json` before executing workloads
(`pkg/tests/executed-harness/amended/run_full_matrix.sh:150-169`,
`pkg/tests/executed-harness/amended/run_full_matrix.sh:227-252`), and the bound
protocol records `gate_mode: report-only`
(`pkg/evidence/complete-matrix/results/w1ro02/protocol.json:28-30`).  The first
W2 row is timestamped 2026-09-08T16:53:42Z
(`pkg/evidence/complete-matrix/results/w1ro02/raw-results.jsonl:745`).

The narrower assertion that the prose amendment document itself was written
before W2 is **not independently timestamp-confirmed**.  Its only retained
provenance is the self-declared date “Prepared 2026-09-08 before collecting
any W2-W6 measurements” (`pkg/docs/AMENDED-PROTOCOL-W1.md:1-4`), the same
calendar date as the first W2 row, with no creation time or pre-W2 external
hash/timestamp.  Verdict: protocol behavior **CONFIRMED**; document-precedence
claim **PARTIAL / not independently established**.
