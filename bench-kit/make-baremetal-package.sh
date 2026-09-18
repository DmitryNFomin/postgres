#!/usr/bin/env bash
# Create the self-contained package that the executor copies to Linux.
#
# brief-v11-wpc-kit.md, "Launch note": the three commit hashes in
# sources.conf are not final yet. This script supports --dry-run, which
# lists exactly what a real run would snapshot (kit files, workloads,
# patches resolved by glob, and the pinned source commits/fixture) without
# requiring real commits to exist, without touching any git worktree, and
# without producing an archive. Real packaging (no --dry-run) refuses to
# run while any sources.conf placeholder remains.
set -Eeuo pipefail
export LC_ALL=C
export COPYFILE_DISABLE=1  # macOS: avoid AppleDouble (._*) sidecar files in tar archives

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
V11_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
PACKAGE_NAME=wet-v11-baremetal-r2

DRY_RUN=0
DIST_DIR=""
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    -h|--help)
      cat <<'EOF'
Usage:
  ./make-baremetal-package.sh [--dry-run] [DIST_DIR]

--dry-run lists exactly what would be snapshotted (kit files, workloads,
patches resolved by their configured glob, and the pinned source commits)
without touching git, without requiring real commit hashes, and without
producing an archive. This is the only mode used before the coordinator
fills in sources.conf.
EOF
      exit 0
      ;;
    *) DIST_DIR="$arg" ;;
  esac
done
DIST_DIR=${DIST_DIR:-"$V11_ROOT/dist"}

die() { echo "make-baremetal-package.sh: ERROR: $*" >&2; exit 1; }
log() { printf '%s\n' "$*"; }

for tool in python3 tar find; do
  command -v "$tool" >/dev/null 2>&1 || die "missing required packaging tool: $tool"
done

# This script runs on the coordinator's laptop, which may be macOS (no
# sha256sum unless coreutils is installed) or Linux (sha256sum always
# present). Prefer sha256sum where it exists; fall back to shasum -a 256
# (present on every stock macOS), which accepts the same "-c CHECKSUMS"
# verification syntax sha256sum does.
if command -v sha256sum >/dev/null 2>&1; then
  SHA256=(sha256sum)
elif command -v shasum >/dev/null 2>&1; then
  SHA256=(shasum -a 256)
else
  die "missing required packaging tool: sha256sum (or shasum)"
fi

SOURCES_CONF="$SCRIPT_DIR/sources.conf"
[[ -f "$SOURCES_CONF" ]] || die "missing $SOURCES_CONF"

KIT_FILES=(
  00-check-host.sh
  01-build-all.sh
  01b-disassemble.sh
  02-run-matrix.sh
  03-collect.sh
  build_manifest_rules.py
  lib-python.sh
  plateau-probe.sh
  analyze-raw-archive.sh
  wait-for-idle.sh
  analyze-results.py
  benchmark_protocol.py
  cpu_affinity.py
  w3_qualification.py
  wilcoxon.py
  stats_common.py
  latin_square.py
  sources.conf
  sources_conf.py
  self-test.py
  run-benchmark.sh
  verify_reusable_builds.py
  README.md
)
WORKLOAD_FILES=(
  w3-short-lwlock.sql
  recording-proof.sql
  w3-qualification.sql
)

for name in "${KIT_FILES[@]}"; do
  [[ -f "$SCRIPT_DIR/$name" && ! -L "$SCRIPT_DIR/$name" ]] ||
    die "missing regular package input: $name"
done
for name in "${WORKLOAD_FILES[@]}"; do
  [[ -f "$SCRIPT_DIR/workloads/$name" && ! -L "$SCRIPT_DIR/workloads/$name" ]] ||
    die "missing regular workload input: $name"
done
[[ -f "$V11_ROOT/bench-kit/BAREMETAL-RUNBOOK-v11.md" ]] ||
  die "missing $V11_ROOT/bench-kit/BAREMETAL-RUNBOOK-v11.md"
[[ -d "$SCRIPT_DIR/fixture-src/test_wait_primitive" ]] ||
  die "missing bundled test_wait_primitive fixture snapshot"
[[ -f "$SCRIPT_DIR/fixture-src/MANIFEST.json" ]] ||
  die "missing $SCRIPT_DIR/fixture-src/MANIFEST.json"
[[ -d "$SCRIPT_DIR/crossover" ]] ||
  die "missing crossover/ (second-stage persistent-backend harness)"
[[ -d "$SCRIPT_DIR/selftest-fakebin" ]] ||
  die "missing selftest-fakebin/ (fake-binaries self-test, run by self-test.py)"

# The test_wait_primitive fixture (W1) is carried as a snapshot committed
# in the kit (fixture-src/), not read from any git ref -- the ref it was
# originally captured from (see fixture-src/MANIFEST.json) may not exist,
# or may not be fetched, on this machine or the executor's. Verify the
# snapshot against its manifest before packaging it, on both --dry-run and
# a real build, so corruption is caught early either way. This needs only
# python3's hashlib; it never touches git.
if ! python3 - "$SCRIPT_DIR/fixture-src" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
manifest = json.loads((root / "MANIFEST.json").read_text(encoding="utf-8"))
expected = manifest.get("files_sha256", {})

def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()

actual = {
    p.relative_to(root).as_posix(): digest(p)
    for p in sorted(root.rglob("*"))
    if p.is_file() and p.name != "MANIFEST.json"
}
if actual != expected:
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    mismatched = sorted(
        k for k in (set(expected) & set(actual)) if expected[k] != actual[k]
    )
    print(
        f"fixture-src MANIFEST.json verification failed: "
        f"missing={missing} extra={extra} mismatched={mismatched}",
        file=sys.stderr,
    )
    raise SystemExit(1)
PY
then
  die "bundled test_wait_primitive fixture-src failed MANIFEST.json verification"
fi

CONTROL_PATCH_GLOB=$(python3 "$SCRIPT_DIR/sources_conf.py" get "$SOURCES_CONF" CONTROL_PATCH_GLOB)
V11_PATCH_GLOB=$(python3 "$SCRIPT_DIR/sources_conf.py" get "$SOURCES_CONF" V11_PATCH_GLOB)
# Intentional glob expansion of a pattern configured in sources.conf; the
# matched filenames are plain patch names with no shell metacharacters.
# Bash pathname expansion already yields results in sorted order.
shopt -s nullglob
# shellcheck disable=SC2206
CONTROL_PATCHES_ABS=("$V11_ROOT"/$CONTROL_PATCH_GLOB)
# shellcheck disable=SC2206
V11_PATCHES_ABS=("$V11_ROOT"/$V11_PATCH_GLOB)
shopt -u nullglob
CONTROL_PATCHES=()
for path in "${CONTROL_PATCHES_ABS[@]}"; do
  CONTROL_PATCHES+=("${path#"$V11_ROOT"/}")
done
V11_PATCHES=()
for path in "${V11_PATCHES_ABS[@]}"; do
  V11_PATCHES+=("${path#"$V11_ROOT"/}")
done
[[ ${#CONTROL_PATCHES[@]} -ge 1 ]] ||
  die "no patch files matched CONTROL_PATCH_GLOB=$CONTROL_PATCH_GLOB under $V11_ROOT"
[[ ${#V11_PATCHES[@]} -eq 5 ]] ||
  die "expected exactly five v11-series patch files matching V11_PATCH_GLOB=$V11_PATCH_GLOB under $V11_ROOT, found ${#V11_PATCHES[@]}"

if [[ "$DRY_RUN" -eq 1 ]]; then
  log "DRY RUN: no archive will be produced; nothing outside this process is touched."
  log ""
  log "sources.conf placeholders remaining (packaging is refused for real until empty):"
  python3 "$SCRIPT_DIR/sources_conf.py" placeholders "$SOURCES_CONF" | sed 's/^/  - /' ||
    log "  (none)"
  log ""
  log "Kit files that would be snapshotted (${#KIT_FILES[@]}):"
  for name in "${KIT_FILES[@]}"; do log "  kit/$name"; done
  log "Workload files that would be snapshotted (${#WORKLOAD_FILES[@]}):"
  for name in "${WORKLOAD_FILES[@]}"; do log "  kit/workloads/$name"; done
  log "Runbook: kit/BAREMETAL-RUNBOOK-v11.md"
  log "Crossover second-stage harness: kit/crossover/ ($(find "$SCRIPT_DIR/crossover" -type f | wc -l | tr -d ' ') files)"
  log "Fake-binaries self-test: kit/selftest-fakebin/ ($(find "$SCRIPT_DIR/selftest-fakebin" -type f | wc -l | tr -d ' ') files)"
  log ""
  log "Control patch(es) matching $CONTROL_PATCH_GLOB (${#CONTROL_PATCHES[@]}):"
  for p in "${CONTROL_PATCHES[@]}"; do log "  $p"; done
  log "V11-series patches matching $V11_PATCH_GLOB (${#V11_PATCHES[@]}):"
  for p in "${V11_PATCHES[@]}"; do log "  $p"; done
  log ""
  log "Bundled test_wait_primitive fixture snapshot (verified against fixture-src/MANIFEST.json above; never read from git):"
  find "$SCRIPT_DIR/fixture-src/test_wait_primitive" -type f | sed "s#^#  #"
  log ""
  log "Source snapshots that would be taken with 'git archive' from sources.conf:"
  for key in MASTER_SHA V11_SHA CONTROL_SHA; do
    value=$(python3 "$SCRIPT_DIR/sources_conf.py" get "$SOURCES_CONF" "$key")
    log "  $key=$value"
  done
  log ""
  log "DRY RUN complete. No files were written; no git command touched a worktree."
  exit 0
fi

# --- real packaging below: refuses to run while a placeholder remains ---
python3 "$SCRIPT_DIR/sources_conf.py" check "$SOURCES_CONF" ||
  die "sources.conf still has placeholder(s); this is a real (non-dry-run) package build"

: "${POSTGRES_REPO_PATH:?set POSTGRES_REPO_PATH to the local postgres.git clone that contains MASTER_SHA/V11_SHA/CONTROL_SHA}"
[[ -d "$POSTGRES_REPO_PATH/.git" ]] ||
  die "POSTGRES_REPO_PATH does not look like a git repository: $POSTGRES_REPO_PATH"

MASTER_SHA=$(python3 "$SCRIPT_DIR/sources_conf.py" get "$SOURCES_CONF" MASTER_SHA)
V11_SHA=$(python3 "$SCRIPT_DIR/sources_conf.py" get "$SOURCES_CONF" V11_SHA)
CONTROL_SHA=$(python3 "$SCRIPT_DIR/sources_conf.py" get "$SOURCES_CONF" CONTROL_SHA)
REPO_URL=$(python3 "$SCRIPT_DIR/sources_conf.py" get "$SOURCES_CONF" POSTGRES_REPO_URL)

STAGE=$(mktemp -d)
ARCHIVE="$DIST_DIR/$PACKAGE_NAME.tar.gz"
SIDECAR="$ARCHIVE.sha256"
VERIFY=""
SUCCESS=0
OUTPUTS_OWNED=0
cleanup() {
  rm -rf -- "$STAGE"
  [[ -z "$VERIFY" ]] || rm -rf -- "$VERIFY"
  if [[ "$OUTPUTS_OWNED" -eq 1 && "$SUCCESS" -ne 1 ]]; then
    rm -f -- "$ARCHIVE" "$SIDECAR"
  fi
}
trap cleanup EXIT

[[ ! -e "$ARCHIVE" && ! -e "$SIDECAR" ]] ||
  die "refusing to overwrite existing package: $ARCHIVE"
OUTPUTS_OWNED=1
mkdir -p "$DIST_DIR" "$STAGE/$PACKAGE_NAME/source" \
  "$STAGE/$PACKAGE_NAME/workloads" "$STAGE/$PACKAGE_NAME/crossover" \
  "$STAGE/$PACKAGE_NAME/selftest-fakebin"

for name in "${KIT_FILES[@]}"; do
  cp "$SCRIPT_DIR/$name" "$STAGE/$PACKAGE_NAME/"
done
for name in "${WORKLOAD_FILES[@]}"; do
  cp "$SCRIPT_DIR/workloads/$name" "$STAGE/$PACKAGE_NAME/workloads/"
done
cp -a "$SCRIPT_DIR/crossover/." "$STAGE/$PACKAGE_NAME/crossover/"
cp -a "$SCRIPT_DIR/selftest-fakebin/." "$STAGE/$PACKAGE_NAME/selftest-fakebin/"
cp "$V11_ROOT/bench-kit/BAREMETAL-RUNBOOK-v11.md" "$STAGE/$PACKAGE_NAME/"

MASTER_ARCHIVE="$STAGE/$PACKAGE_NAME/source/postgres-master.tar.gz"
PATCHED_ARCHIVE="$STAGE/$PACKAGE_NAME/source/postgres-patched.tar.gz"
CONTROL_ARCHIVE="$STAGE/$PACKAGE_NAME/source/postgres-control.tar.gz"
FIXTURE_DEST="$STAGE/$PACKAGE_NAME/source/test_wait_primitive"
SOURCE_MANIFEST="$STAGE/$PACKAGE_NAME/source/source-manifest.json"

git -C "$POSTGRES_REPO_PATH" archive --format=tar.gz \
  -o "$MASTER_ARCHIVE" "$MASTER_SHA"
git -C "$POSTGRES_REPO_PATH" archive --format=tar.gz \
  -o "$PATCHED_ARCHIVE" "$V11_SHA"
git -C "$POSTGRES_REPO_PATH" archive --format=tar.gz \
  -o "$CONTROL_ARCHIVE" "$CONTROL_SHA"

# Package the fixture straight from the verified kit snapshot -- no git ref
# is read or required on this machine (that is the whole point of this
# fix; see fixture-src/MANIFEST.json for where the snapshot came from).
cp -a "$SCRIPT_DIR/fixture-src/test_wait_primitive" "$FIXTURE_DEST"
FIXTURE_PROVENANCE=$(python3 -c '
import json, sys
data = json.load(open(sys.argv[1], encoding="utf-8"))
print("{}@{}".format(data["source_branch"], data["commit"]))
' "$SCRIPT_DIR/fixture-src/MANIFEST.json")

for patch in "${CONTROL_PATCHES[@]}" "${V11_PATCHES[@]}"; do
  dest_dir=$(dirname "$patch")
  mkdir -p "$STAGE/$PACKAGE_NAME/$dest_dir"
  cp "$V11_ROOT/$patch" "$STAGE/$PACKAGE_NAME/$patch"
done

python3 - "$SOURCE_MANIFEST" "$MASTER_ARCHIVE" "$PATCHED_ARCHIVE" \
  "$CONTROL_ARCHIVE" "$FIXTURE_DEST" "$REPO_URL" "$MASTER_SHA" "$V11_SHA" \
  "$CONTROL_SHA" "$FIXTURE_PROVENANCE" \
  "$(git -C "$POSTGRES_REPO_PATH" show -s --format=%ct "$MASTER_SHA")" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

(out, master_path, patched_path, control_path, fixture_dir, repo_url,
 master_commit, v11_commit, control_commit, fixture_provenance,
 source_date_epoch) = sys.argv[1:]

def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()

fixture_files = {
    p.relative_to(Path(fixture_dir).parent).as_posix(): digest(p)
    for p in sorted(Path(fixture_dir).rglob("*"))
    if p.is_file()
}

data = {
    "schema_version": 11,
    "benchmark_series": "wet-v11",
    "repo_url": repo_url,
    "commits": {
        "master": master_commit,
        "patched": v11_commit,
        "control": control_commit,
    },
    "source_date_epoch": int(source_date_epoch),
    "fixture": {
        # Not a git tree object -- the fixture is packaged from the kit's
        # own fixture-src/ snapshot, never read from git. This is the
        # origin branch/commit that fixture-src/MANIFEST.json recorded
        # when the snapshot was captured, kept only for provenance.
        "tree": fixture_provenance,
        "files_sha256": fixture_files,
    },
    "archives": {
        "master": {"filename": Path(master_path).name, "sha256": digest(master_path)},
        "patched": {"filename": Path(patched_path).name, "sha256": digest(patched_path)},
        "control": {"filename": Path(control_path).name, "sha256": digest(control_path)},
    },
}
Path(out).write_text(
    json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8"
)
PY

chmod 755 \
  "$STAGE/$PACKAGE_NAME/00-check-host.sh" \
  "$STAGE/$PACKAGE_NAME/01-build-all.sh" \
  "$STAGE/$PACKAGE_NAME/01b-disassemble.sh" \
  "$STAGE/$PACKAGE_NAME/02-run-matrix.sh" \
  "$STAGE/$PACKAGE_NAME/03-collect.sh" \
  "$STAGE/$PACKAGE_NAME/plateau-probe.sh" \
  "$STAGE/$PACKAGE_NAME/analyze-raw-archive.sh" \
  "$STAGE/$PACKAGE_NAME/wait-for-idle.sh" \
  "$STAGE/$PACKAGE_NAME/analyze-results.py" \
  "$STAGE/$PACKAGE_NAME/cpu_affinity.py" \
  "$STAGE/$PACKAGE_NAME/w3_qualification.py" \
  "$STAGE/$PACKAGE_NAME/wilcoxon.py" \
  "$STAGE/$PACKAGE_NAME/stats_common.py" \
  "$STAGE/$PACKAGE_NAME/latin_square.py" \
  "$STAGE/$PACKAGE_NAME/sources_conf.py" \
  "$STAGE/$PACKAGE_NAME/self-test.py" \
  "$STAGE/$PACKAGE_NAME/run-benchmark.sh"

(
  cd "$STAGE/$PACKAGE_NAME"
  # The manifest is excluded from its own input list above, then created by
  # this redirect -- not a read/write race on the same file.
  # shellcheck disable=SC2094
  find . -type f ! -name PACKAGE-MANIFEST.sha256 -print0 \
    | sort -z \
    | xargs -0 "${SHA256[@]}" >PACKAGE-MANIFEST.sha256
)

tar -czf "$ARCHIVE" -C "$STAGE" "$PACKAGE_NAME"
(
  cd "$DIST_DIR"
  "${SHA256[@]}" "$(basename "$ARCHIVE")" >"$(basename "$SIDECAR")"
)

VERIFY=$(mktemp -d)
python3 - "$ARCHIVE" <<'PY'
import subprocess
import sys
from pathlib import PurePosixPath

entries = subprocess.run(
    ["tar", "-tzf", sys.argv[1]],
    check=True,
    stdout=subprocess.PIPE,
    text=True,
).stdout.splitlines()
for entry in entries:
    path = PurePosixPath(entry)
    if path.is_absolute() or ".." in path.parts:
        raise SystemExit(f"unsafe package member: {entry}")
PY
tar -xzf "$ARCHIVE" -C "$VERIFY"
(
  cd "$VERIFY/$PACKAGE_NAME"
  "${SHA256[@]}" -c PACKAGE-MANIFEST.sha256 >/dev/null
  PYTHONDONTWRITEBYTECODE=1 python3 ./self-test.py
)
(
  cd "$DIST_DIR"
  "${SHA256[@]}" -c "$(basename "$SIDECAR")"
)

SUCCESS=1
printf 'Package: %s\n' "$ARCHIVE"
printf 'Checksum: %s\n' "$SIDECAR"
printf 'Start command after extraction: ./%s/run-benchmark.sh\n' "$PACKAGE_NAME"
