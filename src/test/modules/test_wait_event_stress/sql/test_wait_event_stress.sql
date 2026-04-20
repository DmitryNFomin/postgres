CREATE EXTENSION test_wait_event_stress;

-- Basic stress test: verify function works (requires capture to be on so
-- the instrumentation path actually executes work we can time).
SET wait_event_capture = stats;
SELECT stress_wait_events(10000) > 0 AS stress_ok;
RESET wait_event_capture;

-- LWLock hash overflow test: register 200 tranches (> 192 limit)
-- This should trigger a WARNING about hash table being full
SET wait_event_capture = stats;

-- Start from a clean slate so we can make deterministic assertions
-- about the overflow counter.
SELECT pg_stat_reset_wait_event_timing(NULL);
SELECT lwlock_overflow_count AS before_overflow
FROM pg_stat_wait_event_timing_overflow
WHERE pid = pg_backend_pid();

SET client_min_messages = warning;
SELECT test_lwlock_hash_overflow(200);
RESET client_min_messages;

-- After overflow the counter must be visible from SQL.
SELECT lwlock_overflow_count > 0 AS overflow_visible
FROM pg_stat_wait_event_timing_overflow
WHERE pid = pg_backend_pid();

-- Reset clears the overflow counter (pins the fix for issue #9).
SELECT pg_stat_reset_wait_event_timing(NULL);
SELECT lwlock_overflow_count = 0 AS lw_cleared,
       flat_overflow_count = 0 AS flat_cleared
FROM pg_stat_wait_event_timing_overflow
WHERE pid = pg_backend_pid();

-- Verify the function returns the count
SELECT test_lwlock_hash_overflow(10);

RESET wait_event_capture;
DROP EXTENSION test_wait_event_stress;
