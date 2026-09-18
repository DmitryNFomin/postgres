#!/usr/bin/env bash
# lib-python.sh -- shared Python interpreter resolution for the v11
# bare-metal kit. Sourced (never executed) by every kit script that needs
# Python 3.9+.
#
# Rocky Linux 8's stock `python3` is 3.6, which is too old for this kit.
# The operator installs a newer interpreter from dnf (python3.11, or
# python39 which installs as python3.9; see BAREMETAL-RUNBOOK-v11.md,
# "Rocky Linux 8 preparation"). This file defines:
#
#   resolve_python  -- tries, in order, $PYTHON_BIN_OVERRIDE (if set),
#                       python3.11, python3.9, then plain python3; sets
#                       PYTHON_BIN to the first one that reports
#                       sys.version_info >= (3, 9) and returns 0. If none
#                       qualifies, PYTHON_BIN is still set to the first
#                       candidate binary that exists at all (so a caller
#                       that only wants best-effort JSON output, say, has
#                       something to invoke) and the function returns 1.
#
#   require_python  -- calls resolve_python and, on failure, calls the
#                       caller's die() (which must already be defined)
#                       with a message naming the dnf package to install.
#
# Every candidate is validated with an actual version check rather than
# assumed from its name, so a distro that ships an unexpectedly old (or
# new) `python3.11` binary is still handled correctly.
resolve_python() {
  PYTHON_BIN=""
  local candidate resolved
  for candidate in "${PYTHON_BIN_OVERRIDE:-}" python3.11 python3.9 python3; do
    [[ -n "$candidate" ]] || continue
    # Walk every match on $PATH for this name, not just the first: a
    # version manager (pyenv, etc.) can shadow a perfectly good
    # /usr/bin/python3.9+ with a shim for an inactive/uninstalled version
    # that exits nonzero when actually run, and command -v only ever
    # returns that first (broken) hit.
    while IFS= read -r resolved; do
      [[ -x "$resolved" ]] || continue
      [[ -n "$PYTHON_BIN" ]] || PYTHON_BIN=$resolved
      if "$resolved" -c \
          'import sys; raise SystemExit(sys.version_info < (3, 9))' \
          >/dev/null 2>&1; then
        PYTHON_BIN=$resolved
        return 0
      fi
    done < <(type -a -p "$candidate" 2>/dev/null)
  done
  return 1
}

# require_python [FAIL_FN] -- FAIL_FN defaults to "die"; pass a different
# name if the calling script's failure function is called something else
# (e.g. run-benchmark.sh's "fail").
require_python() {
  local fail_fn=${1:-die}
  resolve_python && return 0
  if [[ -n "${PYTHON_BIN_OVERRIDE:-}" ]]; then
    "$fail_fn" "PYTHON_BIN_OVERRIDE=$PYTHON_BIN_OVERRIDE is not a working Python 3.9+ interpreter"
    return
  fi
  "$fail_fn" "no Python 3.9 or newer interpreter found (tried \$PYTHON_BIN_OVERRIDE, python3.11, python3.9, python3 on \$PATH); on Rocky Linux 8 install one with: sudo dnf install -y python3.11 (or: sudo dnf install -y python39), then re-run -- or set PYTHON_BIN_OVERRIDE=/path/to/a/python3.9-or-newer/binary"
}
