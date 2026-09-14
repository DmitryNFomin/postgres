#!/usr/bin/env bash
# 03-collect.sh
#
# Packages everything the analysis needs into one archive:
#   results-<hostname>-<date>.tar.gz
#
# It contains the raw per-run CSV, every server log, the build manifest
# (compiler version, configure line, git commits, binary checksums), the
# host telemetry from 00-check-host.sh, and the seed and run order from
# 02-run-matrix.sh. Nothing is filtered, summarized, or edited -- the
# analysis happens after this archive is sent back.
set -Eeuo pipefail
export LC_ALL=C

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WORK="$SCRIPT_DIR/work"
RESULTS="$SCRIPT_DIR/results"

die() { echo "03-collect.sh: ERROR: $*" >&2; exit 1; }
log() { printf '%s\n' "$*"; }

for tool in tar sha256sum; do
  command -v "$tool" >/dev/null 2>&1 || die "required tool not found: $tool"
done

[[ -d "$RESULTS" ]] ||
  die "no results directory at $RESULTS -- run ./02-run-matrix.sh first"
[[ -f "$RESULTS/results.csv" ]] ||
  die "$RESULTS/results.csv is missing -- the matrix did not produce a CSV"

# Sanity check, non-fatal: warn if the matrix looks incomplete rather than
# silently packaging a partial run.
ROW_COUNT=$(($(wc -l <"$RESULTS/results.csv") - 1))
EXPECTED=240
if [[ "$ROW_COUNT" -lt "$EXPECTED" ]]; then
  echo "" >&2
  echo "*** WARNING: results.csv has $ROW_COUNT data row(s), expected $EXPECTED. ***" >&2
  echo "*** This looks like a partial matrix (did 02-run-matrix.sh finish?). ***" >&2
  echo "*** Packaging it anyway, since you asked to collect -- say so when you send it. ***" >&2
  echo "" >&2
fi

HOSTNAME_TAG=$(hostname -s 2>/dev/null || hostname)
DATE_TAG=$(date +%Y%m%d)
ARCHIVE="$SCRIPT_DIR/results-$HOSTNAME_TAG-$DATE_TAG.tar.gz"
[[ ! -e "$ARCHIVE" ]] ||
  die "archive already exists: $ARCHIVE
Remove it, rename it, or move it aside if you want to collect again today."

STAGE=$(mktemp -d "$SCRIPT_DIR/.collect-XXXXXX")
cleanup() { rm -rf -- "$STAGE"; }
trap cleanup EXIT

mkdir -p "$STAGE/results" "$STAGE/build"

log "Collecting results from $RESULTS"
cp "$RESULTS/results.csv" "$STAGE/results/"
[[ -f "$RESULTS/schedule.csv" ]] && cp "$RESULTS/schedule.csv" "$STAGE/results/"
[[ -f "$RESULTS/seed.txt" ]] && cp "$RESULTS/seed.txt" "$STAGE/results/"
[[ -f "$RESULTS/protocol.json" ]] && cp "$RESULTS/protocol.json" "$STAGE/results/"
[[ -f "$RESULTS/telemetry.jsonl" ]] && cp "$RESULTS/telemetry.jsonl" "$STAGE/results/"
if [[ -d "$RESULTS/logs" ]]; then
  cp -a "$RESULTS/logs" "$STAGE/results/logs"
fi
# The per-cell data directories and the unix socket directory are working
# state, not results -- they should already be gone (02-run-matrix.sh
# removes each data directory after its cell), but exclude them
# defensively in case a run was interrupted mid-cell.

log "Collecting the build manifest"
if [[ -f "$WORK/manifest.json" ]]; then
  cp "$WORK/manifest.json" "$STAGE/build/"
else
  echo "*** WARNING: no build manifest found at $WORK/manifest.json ***" >&2
fi

log "Collecting host telemetry"
for f in "$SCRIPT_DIR/host-check.txt" "$SCRIPT_DIR/host-check.json"; do
  [[ -f "$f" ]] && cp "$f" "$STAGE/"
done
if [[ ! -f "$STAGE/host-check.txt" ]]; then
  echo "*** WARNING: no host-check.txt found -- did you run ./00-check-host.sh? ***" >&2
fi

tar -czf "$ARCHIVE" -C "$STAGE" .
sha256sum "$ARCHIVE" | awk '{print $1}' >"$ARCHIVE.sha256"

SIZE_HUMAN=$(du -h "$ARCHIVE" | cut -f1)
log ""
log "Archive:      $ARCHIVE"
log "Size:         $SIZE_HUMAN"
log "SHA-256 file: $ARCHIVE.sha256"
log ""
log "This archive records this machine's hostname, your OS username, and"
log "the working directory paths used for the run. Say so when you hand it"
log "over -- those get scrubbed before anything is published."
