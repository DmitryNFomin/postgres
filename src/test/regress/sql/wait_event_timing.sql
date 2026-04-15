--
-- Test wait event timing infrastructure
--
-- These tests verify the wait event timing SQL interface.
-- They require --enable-wait-event-timing at compile time.
--

-- Check GUC default
SHOW wait_event_trace;

-- Enable timing for this test (PGC_SUSET, requires superuser)
SET wait_event_timing = on;

-- Verify views exist (zero rows is fine, just checking structure)
SELECT * FROM pg_stat_wait_event_timing LIMIT 0;
SELECT * FROM pg_stat_wait_event_timing_by_query LIMIT 0;
SELECT * FROM pg_stat_wait_event_trace LIMIT 0;

-- Verify column types of timing view
SELECT
    a.attname,
    pg_catalog.format_type(a.atttypid, a.atttypmod) as type
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname = 'pg_catalog'
  AND c.relname = 'pg_stat_wait_event_timing'
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;

-- Generate a wait event
SELECT pg_sleep(0.1);

-- Verify PgSleep event appears with correct structure
SELECT
    pid = pg_backend_pid() AS pid_ok,
    backend_type,
    wait_event_type,
    wait_event,
    calls >= 1 AS has_calls,
    total_time_ms > 0 AS has_time,
    avg_time_us > 0 AS has_avg,
    max_time_us > 0 AS has_max,
    pg_typeof(histogram) AS hist_type,
    array_length(histogram, 1) AS hist_len,
    calls = (SELECT sum(x) FROM unnest(histogram) x) AS hist_invariant
FROM pg_stat_wait_event_timing
WHERE wait_event = 'PgSleep';

-- Test reset function (own backend)
SELECT pg_stat_reset_wait_event_timing(NULL);
SELECT count(*) AS after_reset
FROM pg_stat_wait_event_timing
WHERE wait_event = 'PgSleep';

-- Test trace ring buffer (need compute_query_id for query markers)
SET compute_query_id = on;
SET wait_event_trace = on;
SELECT pg_sleep(0.01);

SELECT
    wait_event_type,
    wait_event,
    duration_us >= 0 AS dur_ok,
    seq >= 0 AS seq_ok
FROM pg_stat_wait_event_trace
WHERE wait_event = 'PgSleep';

-- Test query markers exist in trace
SELECT count(*) > 0 AS has_query_markers
FROM pg_stat_wait_event_trace
WHERE wait_event_type = 'Query';

-- Test by_query (requires trace + compute_query_id, both already on)
SELECT pg_sleep(0.01);

SELECT
    pid = pg_backend_pid() AS pid_ok,
    backend_type,
    query_id IS NOT NULL AS has_qid,
    wait_event = 'PgSleep' AS is_pgsleep,
    calls >= 1 AS has_calls,
    total_time_ms > 0 AS has_time
FROM pg_stat_wait_event_timing_by_query
WHERE wait_event = 'PgSleep';

-- Reset does not crash
SELECT pg_stat_reset_wait_event_timing(NULL);

-- Invalid backend_id errors
SELECT pg_stat_reset_wait_event_timing(99999);

-- Trace read with invalid backend_id returns empty
SELECT count(*) AS invalid_trace
FROM pg_stat_get_wait_event_trace(99999);

-- Clean up
RESET wait_event_timing;
RESET wait_event_trace;
RESET compute_query_id;
