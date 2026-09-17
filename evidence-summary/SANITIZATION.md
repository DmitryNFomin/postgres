# Sanitized benchmark evidence

This is a privacy-sanitized derivative of the completed persistent-backend W6c crossover archive.

- Hostname, executor name, home paths, kernel string, transient staging path, tar ownership names, and the unrelated host process list were removed or replaced.
- PostgreSQL, pgbench, and backend process IDs were consistently pseudonymized. Backend-list hashes and proof references were recomputed.
- Generated Python bytecode was removed.
- Aggregate pgbench logs, pgbench summary logs, event boundaries, schedule, session estimates, and analysis outputs are byte-identical to the source archive. The protocol's experiment constants are unchanged; only its sanitized provenance-manifest hash was updated.
- `EVIDENCE-MANIFEST.sha256` and the tools manifest validate the sanitized files. The original evidence and r5 tool manifests are retained under `provenance/` for provenance.

See `SANITIZATION.json` for the machine-readable transformation record and source archive SHA-256.
