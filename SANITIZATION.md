# Sanitized benchmark evidence

This branch publishes a privacy-sanitized derivative of the 2026-09-23
bare-metal matrix archive (560 cells) for the wait-event-tracing v11
patch series, run after the deferred-accounting change in patch 0004.
No crossover archive for this date was available when this branch was
built; if the coordinator supplies one later it will be added in a
follow-up commit and this file updated.

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

See `SANITIZATION.json` for the machine-readable transformation record,
including the original and sanitized archive SHA-256 sums and the
before/after hash values for the three rebound fields.

## Verification

The sanitized archive was extracted and re-analyzed with its own frozen
kit snapshot (`kit/analyze-results.py`), avoiding a dependency on
whatever state a local checkout of the kit happens to be in (the
workstation's local `bench-kit/` has drifted since these archives were
produced -- see `v11/reports/wpj-report.md` for the same issue
encountered on the first evidence branch). The analyzer printed
`verification: PASS (560 complete cells)`.

`evidence-summary/matrix-analysis.md` reproduces **byte-identical** to
the analysis saved before sanitization
(`results-20260923/matrix-analysis.md`). The analysis JSON was diffed
against the pre-sanitization JSON with every `*hash*`/`*sha256*` field
stripped from both sides first: the only remaining differences are the
sanitized string values themselves (hostname, kernel string, staging
paths) -- zero numeric measurement differs.
