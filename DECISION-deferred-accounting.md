# Decision: defer wait accounting out of the critical section (v11, 2026-09-22)

## Finding

The v11 bare-metal run of 2026-09-21 (host the benchmark host, 2 sockets, server and
pgbench on 8 physical cores each of NUMA node 1, turbo off, 16 repetitions,
A/A half-width 0.4% on W4) measured the enabled cost of the collector as:

| workload | stats vs module-off | trace vs module-off |
|---|---:|---:|
| W4 read-only, 16 clients | -0.4% [-1.0, +0.3] | -1.3% [-2.0, -0.5] |
| W5 TPC-B, 16 clients | -0.5% [-0.8, -0.3] | -1.2% [-1.6, -0.9] |
| W6c eviction, 32 clients | -0.6% [-1.0, -0.2] | -1.0% [-1.3, -0.8] |
| W3 short ProcArrayLock storm, 8 clients | -6.6% [-8.1, -5.1] | -6.4% [-8.1, -4.7] |

W1 measured the per-wait cost of recording at 33 ns (stats) and 35 ns
(trace) on the already-set-latch path. W3 runs 408k LWLock waits per second
at 85k transactions per second, about 5 waits per transaction, so the
additive cost would be about 0.17%. The measured 6.6% is forty times that.

## Mechanism

`pgstat_report_wait_end_timed()` runs the moment `LWLockAcquire()` returns,
which is inside the critical section: the backend already holds the lock
the other clients are queued on. The collector's end hook then reads the
clock, looks up the event's slot, updates count, total, max and histogram in
the shared payload, and in trace mode appends a 32-byte record under a
seqlock. Every nanosecond of that, including any cache miss on the payload,
extends the lock hold time, and every queued waiter pays it. A contended
lock amplifies the per-wait cost; an uncontended wait does not, which is
why W4, W5 and W6c show only the additive half percent.

## Options considered

1. Leave it and state the mechanism in the cover letter. Honest, but the
   worst-case number stays at 6.6% and every reviewer will ask about it.
2. Skip recording for LWLock waits. Rejected: loses data.
3. Record a cheaper summary inside the lock (count only) and the duration
   elsewhere. Rejected: changes what is captured for the histogram.
4. Defer the accounting: at `wait_end` read the clock and stash
   (event, duration, timestamp) in a one-slot backend-local buffer; flush
   that record through the existing accounting at the next `wait_start`,
   which normally runs after the lock has been released, and at every
   point where ordering or lifetime matters. Chosen.

## What option 4 preserves and what it changes

Preserved, by construction:
- durations are measured between the same two instants as before; the
  trace record's timestamp is the same `wait_end` clock read;
- counts, totals, maxima, histogram buckets and trace records are computed
  from the same stored numbers, so the values are identical;
- order is preserved: the pending record is flushed before the next wait
  is timed, before any marker is written, before a capture change, a reset,
  a release of the payload or ring, and at process exit;
- reset semantics are unchanged: the reset generation is applied at the
  flush, at the same position in the sequence as today.

Changed:
- visibility latency: a completed wait becomes visible to readers at the
  next flush point instead of immediately. In practice that is
  microseconds (the next timed wait); at the latest the end of the
  statement, because going idle is itself a timed wait. A backend that
  computes for a long time after its last wait shows that wait late.
- crash loss: if a backend is killed (not exited) between a wait ending
  and the next flush, that single pending record is lost. Statistics are
  released at exit anyway; for a post-mortem trace ring this means the
  crashed backend's last wait may be absent. One record, only on a crash.

## Decision

Implement option 4 in v11, patch 0004, because the owner's priority is
minimum overhead with no loss in the recorded data, the change keeps every
recorded value and their order, and the two behavioural changes are
bounded and documented. Verify with the module's regress and TAP suites
plus a new test of the flush points, the fork's CI, a VM rehearsal, and
one more bare-metal run. Report both the pre-change and post-change W3
numbers in the cover letter, with this mechanism, so the reader can see
what the deferral bought.

Owner's decision on 2026-09-22 after review of this trade-off.
