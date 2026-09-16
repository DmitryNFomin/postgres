#!/usr/bin/env bash
# Verify, extract, and analyze a raw archive copied back from the test host.
set -Eeuo pipefail
export LC_ALL=C

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ARCHIVE=${1:-}
if [[ -z "$ARCHIVE" || "$ARCHIVE" == -h || "$ARCHIVE" == --help ]]; then
  cat <<'EOF'
Usage:
  ./analyze-raw-archive.sh RESULTS.tar.gz [OUTPUT_DIRECTORY]

The RESULTS.tar.gz.sha256 sidecar must be next to the archive.
All verification and statistical analysis run on this local machine.
EOF
  [[ -n "$ARCHIVE" ]] && exit 0
  exit 2
fi

[[ -f "$ARCHIVE" && ! -L "$ARCHIVE" ]] || {
  echo "analyze-raw-archive.sh: ERROR: missing regular archive: $ARCHIVE" >&2
  exit 1
}
SIDECAR="$ARCHIVE.sha256"
[[ -f "$SIDECAR" && ! -L "$SIDECAR" ]] || {
  echo "analyze-raw-archive.sh: ERROR: missing regular sidecar: $SIDECAR" >&2
  exit 1
}

ARCHIVE=$(cd "$(dirname "$ARCHIVE")" && pwd)/$(basename "$ARCHIVE")
SIDECAR="$ARCHIVE.sha256"
BASE=$(basename "$ARCHIVE" .tar.gz)
OUTPUT=${2:-"$PWD/$BASE-local-analysis"}

python3 - "$ARCHIVE" "$SIDECAR" "$OUTPUT" "$SCRIPT_DIR" <<'PY'
import hashlib
import os
import subprocess
import sys
import tarfile
from pathlib import Path, PurePosixPath

archive, sidecar, output, trusted_kit = map(Path, sys.argv[1:])

fields = sidecar.read_text(encoding="ascii").strip().split()
if len(fields) != 2 or fields[1] != archive.name:
    raise SystemExit(
        "analyze-raw-archive.sh: ERROR: malformed or mismatched sidecar"
    )
expected = fields[0]
if len(expected) != 64 or any(c not in "0123456789abcdef" for c in expected):
    raise SystemExit("analyze-raw-archive.sh: ERROR: invalid sidecar digest")
digest = hashlib.sha256()
with archive.open("rb") as archive_stream:
    for block in iter(lambda: archive_stream.read(1024 * 1024), b""):
        digest.update(block)
    if digest.hexdigest() != expected:
        raise SystemExit(
            "analyze-raw-archive.sh: ERROR: archive checksum mismatch"
        )

    if output.exists():
        raise SystemExit(
            f"analyze-raw-archive.sh: ERROR: output already exists: {output}"
        )
    output.mkdir(parents=True)
    try:
        archive_stream.seek(0)
        with tarfile.open(fileobj=archive_stream, mode="r:gz") as bundle:
            seen_members = set()
            for member in bundle.getmembers():
                path = PurePosixPath(member.name)
                if (
                    member.name in seen_members
                    or path.is_absolute()
                    or ".." in path.parts
                    or not (member.isfile() or member.isdir())
                ):
                    raise RuntimeError(
                        f"unsafe archive member: {member.name}"
                    )
                seen_members.add(member.name)
            bundle.extractall(output)
    except Exception:
        for root, directories, files in os.walk(output, topdown=False):
            for name in files:
                Path(root, name).unlink()
            for name in directories:
                Path(root, name).rmdir()
        output.rmdir()
        raise

try:
    manifest_path = output / "MANIFEST.sha256"
    listed = {}
    for line in manifest_path.read_text(encoding="ascii").splitlines():
        digest_text, separator, relative = line.partition("  ")
        path = PurePosixPath(relative)
        if (
            not separator
            or len(digest_text) != 64
            or any(c not in "0123456789abcdef" for c in digest_text)
            or path.is_absolute()
            or ".." in path.parts
            or relative in listed
        ):
            raise RuntimeError("invalid raw-archive manifest")
        listed[relative] = digest_text

    actual = {
        path.relative_to(output).as_posix()
        for path in output.rglob("*")
        if path.is_file() and path != manifest_path
    }
    if actual != set(listed):
        raise RuntimeError("raw-archive manifest file set differs")
    for relative, expected_file_hash in listed.items():
        path = output / relative
        if path.is_symlink():
            raise RuntimeError(f"symlinked evidence file: {relative}")
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected_file_hash:
            raise RuntimeError(f"evidence hash mismatch: {relative}")
    for name in (
        "benchmark_protocol.py",
        "cpu_affinity.py",
        "w3_qualification.py",
    ):
        trusted = trusted_kit / name
        archived = output / "kit" / name
        if hashlib.sha256(trusted.read_bytes()).digest() != hashlib.sha256(
                archived.read_bytes()).digest():
            raise RuntimeError(f"local/archive analyzer mismatch: {name}")
except Exception:
    for root, directories, files in os.walk(output, topdown=False):
        for name in files:
            Path(root, name).unlink()
        for name in directories:
            Path(root, name).rmdir()
    output.rmdir()
    raise

analyzer = trusted_kit / "analyze-results.py"
command = [
    sys.executable,
    str(analyzer),
    str(output),
    "--output-json",
    str(output / "analysis.json"),
    "--output-markdown",
    str(output / "analysis.md"),
]
print(f"Raw archive integrity: PASS ({len(actual)} files)")
result = subprocess.run(command)
print(f"Local analysis directory: {output}")
raise SystemExit(result.returncode)
PY
