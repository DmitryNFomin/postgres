#!/usr/bin/env bash
set -Eeuo pipefail
export LC_ALL=C

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)
DIST_DIR=${1:-"$REPO_DIR/dist"}
PACKAGE=wet-v10-w6c-power-diag-r1
STAGE=$(mktemp -d)
ARCHIVE="$DIST_DIR/$PACKAGE.tar.gz"
TEMP_ARCHIVE=""
TEMP_SIDECAR=""
VERIFY=""
SUCCESS=0
ARCHIVE_PUBLISHED=0
SIDECAR_PUBLISHED=0

cleanup() {
  rm -rf -- "$STAGE" "${VERIFY:-}"
  rm -f -- "${TEMP_ARCHIVE:-}" "${TEMP_SIDECAR:-}"
  if [[ "$SUCCESS" -ne 1 ]]; then
    [[ "$ARCHIVE_PUBLISHED" -eq 0 ]] || rm -f -- "$ARCHIVE"
    [[ "$SIDECAR_PUBLISHED" -eq 0 ]] || rm -f -- "$ARCHIVE.sha256"
  fi
}
trap cleanup EXIT

[[ ! -e "$ARCHIVE" && ! -L "$ARCHIVE" &&
   ! -e "$ARCHIVE.sha256" && ! -L "$ARCHIVE.sha256" ]] ||
  { echo "output already exists: $ARCHIVE" >&2; exit 1; }
mkdir -p "$DIST_DIR" "$STAGE/$PACKAGE"
TEMP_ARCHIVE=$(mktemp "$DIST_DIR/.$PACKAGE.XXXXXX.tar.gz")
TEMP_SIDECAR=$(mktemp "$DIST_DIR/.$PACKAGE.XXXXXX.sha256")
for name in \
  README.md analyze.py run-root.sh run-worker.sh run_turbostat.py \
  sample_system.py \
  self-test.py; do
  [[ -f "$SCRIPT_DIR/$name" && ! -L "$SCRIPT_DIR/$name" ]] ||
    { echo "missing regular input: $name" >&2; exit 1; }
  cp "$SCRIPT_DIR/$name" "$STAGE/$PACKAGE/"
done
chmod 755 \
  "$STAGE/$PACKAGE/analyze.py" \
  "$STAGE/$PACKAGE/run-root.sh" \
  "$STAGE/$PACKAGE/run-worker.sh" \
  "$STAGE/$PACKAGE/run_turbostat.py" \
  "$STAGE/$PACKAGE/sample_system.py" \
  "$STAGE/$PACKAGE/self-test.py"
(
  cd "$STAGE/$PACKAGE"
  find . -type f ! -name DIAGNOSTIC-MANIFEST.sha256 -print0 |
    sort -z |
    xargs -0 sha256sum >DIAGNOSTIC-MANIFEST.sha256
)
chmod 644 \
  "$STAGE/$PACKAGE/README.md" \
  "$STAGE/$PACKAGE/DIAGNOSTIC-MANIFEST.sha256"
tar -czf "$TEMP_ARCHIVE" -C "$STAGE" "$PACKAGE"
ARCHIVE_SHA256=$(sha256sum "$TEMP_ARCHIVE" | awk '{print $1}')
printf '%s  %s\n' "$ARCHIVE_SHA256" "$PACKAGE.tar.gz" \
  >"$TEMP_SIDECAR"
VERIFY=$(mktemp -d)
tar -xzf "$TEMP_ARCHIVE" -C "$VERIFY"
(
  cd "$VERIFY/$PACKAGE"
  sha256sum -c DIAGNOSTIC-MANIFEST.sha256
  for script in *.sh; do bash -n "$script"; done
  python3 - <<'PY'
from pathlib import Path
for path in Path(".").glob("*.py"):
    compile(path.read_bytes(), str(path), "exec")
PY
  PYTHONDONTWRITEBYTECODE=1 ./self-test.py
)
rm -rf -- "$VERIFY"
VERIFY=""
mv -n "$TEMP_ARCHIVE" "$ARCHIVE"
[[ ! -e "$TEMP_ARCHIVE" ]] ||
  { echo "archive output appeared during publication" >&2; exit 1; }
TEMP_ARCHIVE=""
ARCHIVE_PUBLISHED=1
mv -n "$TEMP_SIDECAR" "$ARCHIVE.sha256"
[[ ! -e "$TEMP_SIDECAR" ]] ||
  { echo "checksum output appeared during publication" >&2; exit 1; }
TEMP_SIDECAR=""
SIDECAR_PUBLISHED=1
SUCCESS=1
printf 'Package: %s\nChecksum: %s\n' "$ARCHIVE" "$ARCHIVE.sha256"
