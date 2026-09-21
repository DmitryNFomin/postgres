"""Single source, for the crossover's own host-state validation, of what
BENCHMARK_REHEARSAL=1 relaxes.

00-check-host.sh (the main kit's preflight) relaxes exactly four checks a
VM is structurally incapable of satisfying, logging each as a
non-blocking "REHEARSAL NOTE" instead of refusing to start:

  1. fewer than 8 physical cores;
  2. less than 16 GB total RAM;
  3. no readable cpufreq scaling_governor at all (common under QEMU/TCG,
     which never exposes the cpufreq sysfs tree);
  4. virtualization detected (systemd-detect-virt reports anything other
     than "none").

Only the third of those has a crossover-side counterpart -- host_state.py's
own governor check -- so this module's only job is to make that one check
agree with 00-check-host.sh's exact semantics instead of encoding a
second, independent copy of them:

  - an EMPTY/unreadable governor list (no
    /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor at all) is
    relaxed under BENCHMARK_REHEARSAL=1, logged as a REHEARSAL NOTE;
  - a NON-EMPTY governor list containing anything other than
    "performance" is NEVER relaxed, rehearsal or not -- a governor sysfs
    tree that genuinely exists and genuinely reports a non-performance
    value is not something a VM's structural incapacity explains, exactly
    as 00-check-host.sh's own NON_PERF branch (an unconditional warn(),
    not warn_unless_rehearsal()) treats it.

crossover/ is packaged and verified as a fully self-contained unit
(CROSSOVER-MANIFEST.sha256, crossover/make-package.sh) and must not import
anything from outside crossover/, so this cannot literally be the same
Python module 00-check-host.sh's bash checks use; it exists as the single
place the crossover side's copy of check #3's semantics is written down,
imported by every crossover file that needs it rather than re-encoded per
call site.
"""

from __future__ import annotations

import os


def rehearsal_enabled() -> bool:
    """Same test as 00-check-host.sh's REHEARSAL=1 and
    benchmark_protocol.py's _REHEARSAL: BENCHMARK_REHEARSAL=1 in the
    environment, nothing else. Crossover phases are launched as ordinary
    subprocesses of run-benchmark.sh (or run by hand in the same shell the
    operator set the variable in), and crossover/analyze.py always runs
    synchronously, on the same host, as part of the same run.sh invocation
    that captured host state -- unlike the top-level matrix's
    analyze-raw-archive.sh, the crossover has no "verify this archive
    later, on a different machine" step, so the ambient environment here
    is always the one 00-check-host.sh's preflight already saw earlier in
    the same run."""
    return os.environ.get("BENCHMARK_REHEARSAL") == "1"


def governor_check_relaxed(governors) -> bool:
    """True iff the given (possibly empty) governor list should be
    accepted despite not being all "performance", under the current
    process's rehearsal state. See the module docstring for the exact
    rule -- only an EMPTY list is ever relaxed."""
    return (not governors) and rehearsal_enabled()
