CREATE EXTENSION test_wait_event_stress;

-- Basic stress test: verify function works
SELECT stress_wait_events(100) > 0 AS stress_ok;

-- LWLock hash overflow test: register 200 tranches (> 192 limit)
-- This should trigger a WARNING about hash table being full
SET wait_event_timing = on;
SET client_min_messages = warning;
SELECT test_lwlock_hash_overflow(200);
RESET client_min_messages;

-- Verify the function returns the count
SELECT test_lwlock_hash_overflow(10);

RESET wait_event_timing;
DROP EXTENSION test_wait_event_stress;
