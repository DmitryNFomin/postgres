# Sanitized benchmark evidence

This branch publishes a privacy-sanitized derivative of the 2026-09-23
bare-metal evidence for the wait-event-tracing v11 patch series, run
after the deferred-accounting change in patch 0004: the matrix archive
(560 cells), and, added in a follow-up commit once it became available,
the W6c persistent-backend crossover archive (16 independent sessions).

### Matrix archive (560 cells)

- Host name, the employer domain contained in its FQDN, the OS user
  name, and the home/staging directory built from them were replaced
  with neutral tokens (`benchmark-host`, `benchmark-user`,
  `/home/benchmark-user/wet-v11-baremetal-r2.SANITIZED`) everywhere they
  appeared: host-check reports, the build/provenance manifest, server
  and initdb logs, and disassembly toolchain paths. The host and user
  tokens are the *same* tokens used on `v11-evidence-20260921`, so the
  two evidence branches are directly comparable; the staging-path token
  differs only in the `-r2` suffix that was actually part of this run's
  real directory name.
- Every data row under host-check.txt's "Top processes by CPU" section
  was removed (facter, osqueryd, bash, node_exporter, process-exporte,
  systemd, lldpad -- none are benchmark processes; the host-check runs
  before the benchmark starts and separately reports "Co-resident
  PostgreSQL processes: none"). The column header line was kept with a
  redaction note.
- No `__pycache__` directories were present in this archive.
- Every numeric measurement is byte-for-byte unchanged: `results.csv`,
  `results/w1-detail/*.json` (all 112 files), `schedule.csv`,
  `aa-early.jsonl`, and `plateau-probe-result.json` are identical to the
  source archive.
- The archive's `results/protocol.json` embeds a sha256 of
  `build/manifest.json` and of `host-check.txt`/`host-check.json`, and
  `results/matrix-complete.json` embeds a sha256 of `results/protocol.json`
  -- both checked by the kit's analyzer before it will trust the data.
  Sanitizing the manifest and host-check files changed their bytes, so
  those three hash fields were recomputed and updated to match; every
  other protocol field (experiment constants, schedule seed, CPU
  affinity masks, `bound_file_sha256` for the kit scripts) is unchanged.
- `MANIFEST.sha256` was regenerated for the sanitized files. The kit's
  own trusted modules under `kit/` were left byte-for-byte untouched --
  none of them contained the replaced strings.
- Backend/pgbench process IDs embedded in log lines were left untouched:
  they are not personally identifying and pseudonymizing them was not
  requested.

### Crossover archive (16 sessions, added in a follow-up commit)

Same real host and OS user as the matrix archive; the crossover
archive's own staging directory used the *same* `wet-v11-baremetal-r2`
home path (tokenized identically) plus a separate `/var/tmp/mktemp`
working directory for the crossover run itself.

- The host FQDN and the home/staging path were replaced with the same
  tokens as the matrix archive, everywhere they appeared: the
  provenance host-check report/JSON, host-before/after snapshots, the
  build/provenance manifest, `driver.log`, and all 48 per-session
  `worker/logs/{init,server,server-setup}-*.log*` files.
  Separately, the crossover's own mktemp staging directory
  (`/var/tmp/w6c-persistent-crossover.<random suffix>` -- one real
  suffix used by this run, one literal `mktemp` template-mask string in
  `tools/run.sh`, and one hardcoded example path in `tools/self-test.py`
  -- three distinct 6-character suffixes total) was replaced everywhere
  with `/var/tmp/w6c-persistent-crossover.SANITIZED`.
- The OS user name was replaced with `benchmark-user` everywhere it
  appeared standalone (`driver.log`'s executor line, `initdb`'s "owned
  by user" lines, `provenance/runtime.txt`, `provenance/smoke-pass.txt`).
- 58 files had content changed in total (7 for the hostname/domain, 19
  for the home/staging path, 21 for the standalone username, 51 for the
  mktemp staging pattern -- these sets overlap, e.g. `driver.log` and
  the per-session logs match more than one pattern).
- The same 7 unrelated process-list rows were removed from this
  archive's `provenance/host-check.txt` as from the matrix archive's
  copy -- both are the same host-check snapshot, taken once before both
  stages ran.
- 4 generated `__pycache__/*.pyc` files were removed from
  `evidence/tools/`.
- The archive's `worker/protocol.json` embeds a sha256 of
  `provenance/manifest.json` (`kit_build_manifest_sha256`) -- checked by
  `tools/analyze.py` before it will trust the data. Sanitizing
  `manifest.json` changed its bytes, so this one field was recomputed
  and updated to match; every other protocol field (experiment
  constants, schedule seed, CPU affinity masks, `bound_file_sha256` for
  the kit's bound scripts) is unchanged. `tools/run.sh` and
  `tools/self-test.py` (where the mktemp-pattern doc/usage strings were
  sanitized) are *not* in the analyzer's bound-file set, so no other
  hash needed rebinding.
- `evidence/tools/CROSSOVER-MANIFEST.sha256` was regenerated (it covers
  every file under `tools/`, including the two now-sanitized scripts).
  `evidence/provenance/CROSSOVER-MANIFEST.sha256` and
  `evidence/provenance/PACKAGE-MANIFEST.sha256` were intentionally left
  as the original acquisition-time records, matching the matrix stage's
  own `MANIFEST.sha256` precedent from `v11-evidence-20260921`.
  `evidence/EVIDENCE-MANIFEST.sha256` (top-level, covers every file in
  the archive) was regenerated.
- No numeric measurement byte was touched: `worker/results.csv`,
  `worker/schedule.csv`, `worker/seed.txt`, `worker/events.jsonl`,
  `worker/mode-proofs.csv`, `session-estimates.csv`, and
  `analysis.md`/`analysis.json` are byte-identical to the source
  archive.
- Backend/pgbench process IDs (`worker/backend-pids/*`, aggregate-log
  filename suffixes) were left untouched: not personally identifying,
  pseudonymizing them was not requested.

See `SANITIZATION.json` for the machine-readable transformation record,
including the original and sanitized archive SHA-256 sums and the
before/after hash values for the rebound fields.

## Verification

**Matrix archive**: extracted and re-analyzed with its own frozen kit
snapshot (`kit/analyze-results.py`), avoiding a dependency on whatever
state a local checkout of the kit happens to be in (the workstation's
local `bench-kit/` has drifted since these archives were produced --
see `v11/reports/wpj-report.md` for the same issue encountered on the
first evidence branch). The analyzer printed `verification: PASS (560
complete cells)`.

`evidence-summary/matrix-analysis.md` reproduces **byte-identical** to
the analysis saved before sanitization
(`results-20260923/matrix-analysis.md`). The analysis JSON was diffed
against the pre-sanitization JSON with every `*hash*`/`*sha256*` field
stripped from both sides first: the only remaining differences are the
sanitized string values themselves (hostname, kernel string, staging
paths) -- zero numeric measurement differs.

**Crossover archive**: extracted the sanitized archive; its
`EVIDENCE-MANIFEST.sha256` and `tools/CROSSOVER-MANIFEST.sha256`
self-checks both pass. Copied `evidence/worker`, `evidence/provenance`,
and `evidence/tools` into a scratch directory and ran
`tools/analyze.py .`; it exited 0 and wrote `analysis.md`/`analysis.json`
that are **byte-identical** to the source archive's own
`evidence/analysis.md`/`analysis.json` (which were left untouched by
sanitization) -- confirming the sanitized archive reproduces the same
statistics: stats vs off -0.537% [-0.664%, -0.410%], trace vs off
-1.030% [-1.163%, -0.897%], off/off placebo -0.142% [-0.331%, +0.047%],
suitability PASS.
