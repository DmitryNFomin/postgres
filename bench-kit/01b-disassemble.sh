#!/usr/bin/env bash
# 01b-disassemble.sh
#
# Captures the actual-toolchain (Rocky Linux 8: gcc 8.5) disassembly of the
# hot-path functions covered by reports/wpd-report.md (which used gcc 13),
# for each of the four independent builds 01-build-all.sh just produced.
# This gives codegen evidence on the real benchmark toolchain to compare
# against that report. Read-only: never starts a PostgreSQL server, never
# touches git.
#
# Output lands under $SCRIPT_DIR/disassembly/, not results/disassembly/,
# because 02-run-matrix.sh (full mode) requires $SCRIPT_DIR/results to not
# already exist when it starts; 03-collect.sh copies this directory into
# the final archive at results/disassembly/.
set -Eeuo pipefail
export LC_ALL=C

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
die() { echo "01b-disassemble.sh: ERROR: $*" >&2; exit 1; }
log() { printf '%s\n' "[$(date -u +%H:%M:%S)] $*"; }

# shellcheck source=lib-python.sh
source "$SCRIPT_DIR/lib-python.sh"
require_python

WORK="$SCRIPT_DIR/work"
INSTALL_ROOT="$WORK/install"
OUT_DIR="$SCRIPT_DIR/disassembly"

for tool in objdump cc; do
  command -v "$tool" >/dev/null 2>&1 || die "required tool not found: $tool"
done

[[ -d "$INSTALL_ROOT" ]] ||
  die "missing $INSTALL_ROOT -- run ./01-build-all.sh first"
[[ ! -e "$OUT_DIR" ]] ||
  die "disassembly already exists: $OUT_DIR (preserving it for inspection)"

# Hot-path functions named in the codegen review this evidence is meant to
# be compared against (reports/wpd-report.md).
FUNCTIONS=(
  WaitEventSetWait
  FileReadV
  LWLockAcquire
  XLogWrite
  SlruInternalWritePage
  CopyReadLine
  pgaio_io_perform_synchronously
)

# Matches the runtime_leaf values 01-build-all.sh installs each build to
# (build_one calls at the bottom of that script).
BUILDS=(baseline-a baseline-b patched control)
leaf_for_build() {
  case "$1" in
    baseline-a) echo base-a ;;
    baseline-b) echo base-b ;;
    patched) echo patchd ;;
    control) echo ctrlop ;;
    *) die "unknown build name: $1" ;;
  esac
}

mkdir -p "$OUT_DIR"

for build in "${BUILDS[@]}"; do
  leaf=$(leaf_for_build "$build")
  binary="$INSTALL_ROOT/$leaf/bin/postgres"
  [[ -x "$binary" ]] ||
    die "missing installed postgres binary for $build: $binary"
  build_out="$OUT_DIR/$build"
  mkdir -p "$build_out"
  log "Disassembling $build ($binary)"
  for func in "${FUNCTIONS[@]}"; do
    out_file="$build_out/$func.txt"
    if objdump -d --no-show-raw-insn -M intel --disassemble="$func" \
        "$binary" >"$out_file.tmp" 2>"$out_file.err"; then
      mv -- "$out_file.tmp" "$out_file"
      rm -f -- "$out_file.err"
    else
      # Not every function is necessarily resolvable in every build (it
      # may be inlined away, or absent from an older codepath); record
      # that fact rather than failing the whole benchmark run over it.
      {
        echo "objdump could not disassemble $func in $binary"
        cat "$out_file.err" 2>/dev/null || true
      } >"$out_file"
      rm -f -- "$out_file.tmp" "$out_file.err"
    fi
  done
done

"$PYTHON_BIN" - "$OUT_DIR/PROVENANCE.json" <<'PY'
import json
import subprocess
import sys
from pathlib import Path

out = Path(sys.argv[1])


def first_line(cmd):
    return subprocess.run(
        cmd, check=True, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, text=True,
    ).stdout.splitlines()[0]


data = {
    "gcc_version": first_line(["cc", "--version"]),
    "objdump_version": first_line(["objdump", "--version"]),
}
out.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY

log "Disassembly evidence written under $OUT_DIR"
