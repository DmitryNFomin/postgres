# test_wait_primitive

This test extension measures nanoseconds per iteration for five tight loops.
Both `INSTR_TIME` reads are outside each loop, and a volatile sink makes the
loop results observable.  Pass a positive `bigint` iteration count.

`test_wait_primitive_latch_set` sets `MyLatch` once and leaves it set for the
entire timed loop.  On master commit `798bdcae89d`,
`src/backend/storage/ipc/waiteventset.c:1111-1125` constructs the
`WL_LATCH_SET` result and breaks because the output buffer is full.  Thus it
returns before `WaitEventSetWaitBlock()` at line 1143 and makes no kernel wait.
The latch is reset only after timing finishes.

`test_wait_primitive_latch_timeout` resets `MyLatch` once, then uses a zero
timeout with `WL_TIMEOUT`.  It exercises the real `WaitEventSetWaitBlock()`
polling path (for example, `epoll_wait(..., 0)` on Linux) without sleeping.

`test_wait_primitive_file_read` creates and writes one page through the `fd.c`
API, then repeatedly `FileRead`s that page at offset zero.  The write primes
the OS page cache, so each iteration is a real `pread`-style call without a
physical-device read.  `test_wait_primitive_usleep0` mirrors wait reporting
around `pg_usleep(0)`.  `test_wait_primitive_report_only` measures only the
wait-reporting start/end pair.

For a quick functional check after building PostgreSQL, run either
`make -C src/test/modules/test_wait_primitive check` in an in-tree build or
`make USE_PGXS=1 PG_CONFIG=/path/to/pg_config installcheck` against an
installed server.  Benchmark orchestration is intentionally kept outside the
PostgreSQL tree in `bench-v7/`.
