# Sanitized benchmark evidence

This branch publishes a privacy-sanitized derivative of the two 2026-09-21
bare-metal archives: the wait-event-tracing v11 matrix run (560 cells) and
the W6c persistent-backend crossover (16 sessions).

- Host name, the employer domain contained in its FQDN, the OS user name,
  and the home/staging directories built from them were replaced with
  neutral tokens (`benchmark-host`, `benchmark-user`,
  `/home/benchmark-user/wet-v11-baremetal.SANITIZED`,
  `/var/tmp/w6c-persistent-crossover.SANITIZED`) everywhere they appeared:
  host-check reports, build/provenance manifests, server and initdb logs,
  disassembly toolchain paths, and the crossover tool scripts' usage text.
- Unrelated host process-list telemetry (osqueryd, node_exporter,
  process-exporte, systemd, lldpad, consul, auditbeat) was removed from
  every copy of `host-check.txt`.
- Generated Python bytecode (`__pycache__`) was removed from the crossover
  archive's `tools/` directory.
- Every numeric measurement is byte-for-byte unchanged: `results.csv`,
  `results/w1-detail/*.json`, `schedule.csv`, `aa-early.jsonl`, and
  `plateau-probe-result.json` are identical to the source archives.
- Both archives embed a hash of their own build/provenance manifest (and,
  for the matrix stage, of `host-check.txt`/`host-check.json`) inside
  `results/protocol.json` / `worker/protocol.json`, checked by the kit's
  analyzer before it will trust the data. Sanitizing the manifest and
  host-check files changed their bytes, so those specific hash fields
  were recomputed and updated to match; every other protocol field
  (experiment constants, schedule seed) is unchanged. The matching entry
  in `results/matrix-complete.json` was updated the same way.
- `MANIFEST.sha256` (matrix) and `evidence/EVIDENCE-MANIFEST.sha256`
  (crossover) were regenerated for the sanitized files. The kit's own
  trusted numeric modules under `kit/` (`benchmark_protocol.py`,
  `cpu_affinity.py`, `w3_qualification.py`, `wilcoxon.py`,
  `stats_common.py`, `latin_square.py`, `sources_conf.py`) were left
  byte-for-byte untouched -- they never contained the replaced strings.
  `evidence/tools/CROSSOVER-MANIFEST.sha256` was regenerated because
  three tool scripts contained the (now-sanitized) staging-path example
  text; `evidence/provenance/CROSSOVER-MANIFEST.sha256` and
  `.../PACKAGE-MANIFEST.sha256` were intentionally left as the original
  acquisition-time records, matching the v10 publication's precedent.
- Backend/pgbench process IDs (`worker/backend-pids/*`, aggregate-log
  filename suffixes, `mode-proofs.csv` PID hashes) were left untouched:
  they are not personally identifying and pseudonymizing them was not
  requested.

See `SANITIZATION.json` for the machine-readable transformation record,
including the original and sanitized archive SHA-256 sums.

## Verification

Both sanitized archives were extracted and re-analyzed with the archive's
own frozen kit snapshot (avoiding an unrelated local kit-drift issue on
the workstation used to prepare this branch -- see the report below).
`evidence-summary/matrix-analysis.md` and `evidence-summary/crossover-
analysis.md` reproduce byte-identical to the analyses saved before
sanitization. The JSON analysis outputs differ only in the manifest/host-
check hash fields described above, which legitimately changed, plus (for
the crossover JSON only) a floating-point difference in the 16th
significant digit that also appears when the *unmodified* original
archive is re-analyzed on this workstation -- a platform artifact, not a
sanitization effect.
