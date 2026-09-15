# Wait-event tracing v9 benchmark

This workspace packages a new bare-metal benchmark series for the wait-event
hook null-path optimization.

- Vanilla baseline: `765efece39ba3fb04fdf20b1dadcd9ecea76fbc9`
- Optimized patched source: `40bffed8a92291c27a5d1956a5cd18dd3609f397`
- Source patch: `patches-v9/0006-optimize-null-wait-event-hook-path.patch`
- Executor runbook: `BAREMETAL-RUNBOOK-v9.md`
- Package output: `dist/wet-v9-baremetal-r1.tar.gz`

The optimized source loads each hook pointer once and avoids reading the
volatile wait-event value when no end hook exists. It preserves enabled
stats/trace behavior and uses no `likely()` or `unlikely()` hint.

The `briefs`, `reports`, `patches-v8`, `README-v8.md`, and
`IMPLEMENTATION-PLAN-v8.md` files are retained as historical v8 design
material. The runnable v9 kit is under `bench-kit`.
