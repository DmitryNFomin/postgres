# Wait-event tracing v10 benchmark

This workspace packages a same-run bare-metal comparison of the v9 reference
and the v10 attachment-needed guard.

- Vanilla baseline: `765efece39ba3fb04fdf20b1dadcd9ecea76fbc9`
- v9 reference: `40bffed8a92291c27a5d1956a5cd18dd3609f397`
- v10 treatment: `c12783fbf86e8116526afe4566d58bf90c3478e0`
- v9 patch: `patches-v9/0006-optimize-null-wait-event-hook-path.patch`
- v10 patch: `patches-v10/0007-inline-attachment-needed-guard.patch`
- Executor runbook: `BAREMETAL-RUNBOOK-v10.md`
- Package output: `dist/wet-v10-baremetal-r3.tar.gz`

The v10 source adds one always-inline `pwet_attach_needed` test around the
unchanged attachment implementation. The package measures v9 and v10 in
matching hook-null, module-off, stats, and trace modes.

R3 is built for the target host's verified two-socket CPU map. It fixes
PostgreSQL to socket/NUMA node 1 and pgbench to eight CPUs on socket/NUMA
node 0, while preserving all r2 A/A rejection thresholds.

The follow-up persistent-backend crossover, which blocks W6c throughput
plateaus within long-lived sessions, is under
`benchmarks/w6c-persistent-crossover`.

The `briefs`, `reports`, `patches-v8`, `README-v8.md`, and
`IMPLEMENTATION-PLAN-v8.md` files are retained as historical v8 design
material. The runnable v10 kit is under `bench-kit`.
