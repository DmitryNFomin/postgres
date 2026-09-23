# selftest-fakebin

Fake `pg_ctl`, `initdb`, `pgbench`, `psql`, `postgres`, `pg_test_timing`,
`numactl`, `taskset`, `lscpu`, `objdump`, `cc`/`gcc` stand-ins used only by
`selftest-fakebin/run-selftest.sh` (invoked by `self-test.py`). They let the
kit's real shell control flow -- `run_session()`/`run_cell()` loops, `pg_ctl`
start/stop, output capture via `$(...)`, mode-proof `psql` checks, CSV
writing -- run end to end without ever starting a real PostgreSQL server or
requiring Rocky-Linux-only tools (`numactl`, `lscpu`, `taskset`, a real
`/proc`), so the suite also runs on a laptop.

Activated by setting `SELFTEST_FAKE_PREFIX=$PWD/selftest-fakebin` (or
equivalent) before running `01-build-all.sh`; that flag then propagates
through `work/manifest.json` and the environment to every later stage.

Key mechanisms, all self-contained under each fake data directory (never
touching a real cluster):

- `pg_ctl start` backgrounds a plain `sleep`, records its pid in
  `postmaster.pid` (the only thing the real kit reads back), and writes
  the real `pg_ctl` "waiting for server to start.... done / server
  started" and "waiting for server to shut down.... done / server
  stopped" lines -- the exact text whose leak into a `$(...)` capture
  caused the plateau-probe bug this self-test guards against.
- `pg_ctl start` also drops a `.active-datadir` pointer file in the
  socket directory, and seeds `.selftest-capture-state` /
  `.selftest-extensions` marker files inside the data directory from
  whatever `postgresql.conf` lines `02-run-matrix.sh`/`run-worker.sh`
  actually wrote for that cell.
- `psql` has no real backend: it answers `SHOW`, `CREATE EXTENSION`,
  `SELECT count(*) FROM pg_extension`, `ALTER SYSTEM SET ...capture`,
  hooks-installed, and `pg_stat_activity`/wait-event-timing style queries
  by reading those marker files and pattern-matching the query text (or
  the `-f` script's basename for `recording-proof.sql` /
  `w3-qualification.sql`), so a cell's mode proof reflects the config
  that cell's `postgresql.conf` actually requested.
- `pgbench`'s `SELFTEST_PGBENCH_EXTRA_SLEEP_MS` (default 0): extra real
  milliseconds of sleep injected after every simulated second, honoured
  only here. Lets a self-test deliberately simulate a slow/busy laptop
  with real elapsed time instead of a synthetic timestamp -- see
  `self-test.py`'s `test_fake_pgbench_extra_sleep()` and
  `crossover/self-test.py`'s `test_extraction_slow_fake_block()`, both
  regression tests for `benchmark_protocol.duration_interval_bounds()` /
  `crossover/protocol.py`'s `MEASUREMENT_NS_LOW`/`HIGH` being mode-aware.
  Never set outside a self-test.

This directory is not a real PostgreSQL installation and must never be
pointed at by `PGBENCH_KIT_PREFIX`-style variables outside the self-test.
