#!/usr/bin/env bash
set -Eeuo pipefail
export LC_ALL=C

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_DIR=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)
DIST_DIR=${1:-"$REPO_DIR/dist"}
PACKAGE=wet-v10-w6c-persistent-crossover-r5
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
  README.md analyze.py extract_blocks.py generate_schedule.py host_state.py \
  make-package.sh protocol.py run-worker.sh run.sh self-test.py \
  verify_affinity.py verify_installs.py verify_runtime.py; do
  [[ -f "$SCRIPT_DIR/$name" && ! -L "$SCRIPT_DIR/$name" ]] ||
    { echo "missing regular input: $name" >&2; exit 1; }
  cp "$SCRIPT_DIR/$name" "$STAGE/$PACKAGE/"
done
chmod 755 "$STAGE/$PACKAGE"/*.py "$STAGE/$PACKAGE"/*.sh
chmod 644 "$STAGE/$PACKAGE/README.md"
(
  cd "$STAGE/$PACKAGE"
  find . -type f ! -name CROSSOVER-MANIFEST.sha256 -print0 |
    sort -z |
    xargs -0 sha256sum >CROSSOVER-MANIFEST.sha256
)
chmod 644 "$STAGE/$PACKAGE/CROSSOVER-MANIFEST.sha256"
tar -czf "$TEMP_ARCHIVE" -C "$STAGE" "$PACKAGE"
ARCHIVE_SHA256=$(sha256sum "$TEMP_ARCHIVE" | awk '{print $1}')
printf '%s  %s\n' "$ARCHIVE_SHA256" "$PACKAGE.tar.gz" >"$TEMP_SIDECAR"
chmod 644 "$TEMP_ARCHIVE" "$TEMP_SIDECAR"

VERIFY=$(mktemp -d)
tar -xzf "$TEMP_ARCHIVE" -C "$VERIFY"
(
  cd "$VERIFY/$PACKAGE"
  sha256sum -c CROSSOVER-MANIFEST.sha256
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
printf 'Package: %s\nChecksum: %s\nSHA-256: %s\n' \
  "$ARCHIVE" "$ARCHIVE.sha256" "$ARCHIVE_SHA256"
