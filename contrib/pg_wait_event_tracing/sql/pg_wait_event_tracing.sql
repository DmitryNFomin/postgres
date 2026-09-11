--
-- PG_WAIT_EVENT_TRACING
--
-- Exercises the statistics level: the capture GUC, the stats surface
-- (pg_stat_get_wait_event_timing(), the pg_stat_wait_event_timing and
-- histogram-buckets views, overflow counters), reset (self and
-- cross-backend, including its authorization), and the per-class capacity
-- table.  The trace level is a separate patch and is not exercised here.
--
CREATE EXTENSION pg_wait_event_tracing;

-- Default is off.
SHOW pg_wait_event_tracing.capture;

-- The taxonomy view is pure SQL.
SELECT count(*) AS buckets FROM pg_wait_event_timing_histogram_buckets;
SELECT bucket_idx, lower_ns, upper_ns, label
FROM pg_wait_event_timing_histogram_buckets
WHERE bucket_idx IN (0, 1, 31)
ORDER BY bucket_idx;

-- Enable stats capture and generate a deterministic wait: pg_sleep emits a
-- Timeout / PgSleep wait.
SET pg_wait_event_tracing.capture = stats;
SELECT pg_sleep(0.1);

-- PgSleep must now be recorded for this backend, with the per-event
-- invariants holding.  We print only booleans so the output is stable.
SELECT calls >= 1 AS calls_ok,
       calls = (SELECT sum(h) FROM unnest(histogram) AS h) AS hist_sum_eq_calls,
       total_time_ms > 0 AS total_positive,
       max_time_us > 0 AS max_positive,
       array_length(histogram, 1)
         = (SELECT count(*)::int FROM pg_wait_event_timing_histogram_buckets)
         AS histogram_len_ok
FROM pg_stat_get_wait_event_timing(pg_backend_pid())
WHERE wait_event = 'PgSleep';

-- The view surfaces the same row, with backend_type attached (v6 column
-- set).
SELECT backend_type, wait_event_type, wait_event
FROM pg_stat_wait_event_timing
WHERE pid = pg_backend_pid() AND wait_event = 'PgSleep';

-- A non-NULL pid that does not exist yields no rows (silent, not an
-- error).
SELECT count(*) AS rows_for_bogus_pid
FROM pg_stat_get_wait_event_timing(-1);

-- Overflow/reset counters for this backend.  A plain test backend uses few
-- LWLock tranches and no out-of-range classes, so both overflow counters
-- are zero, and a fresh backend has not been reset.
SELECT lwlock_overflow_count, flat_overflow_count, reset_count
FROM pg_stat_wait_event_timing_overflow
WHERE pid = pg_backend_pid();

-- Resetting our own backend is synchronous: the PgSleep row is cleared and
-- reset_count advances.  (Filtering to PgSleep because inter-command waits
-- such as ClientRead may be recorded again before the next statement
-- runs.)
SELECT pg_stat_reset_wait_event_timing(NULL);
SELECT count(*) AS pgsleep_rows_after_reset
FROM pg_stat_wait_event_timing
WHERE pid = pg_backend_pid() AND wait_event = 'PgSleep';
SELECT reset_count
FROM pg_stat_wait_event_timing_overflow
WHERE pid = pg_backend_pid();

-- The pid argument defaults to NULL, so a no-argument call resets the
-- caller's own backend.
SELECT pg_stat_reset_wait_event_timing();

-- Resetting an unknown pid is a WARNING, not an ERROR, matching
-- pg_signal_backend()'s own wording; the reset itself is a no-op.
SELECT pg_stat_reset_wait_event_timing(2147483647);

-- Disabling capture releases the payload (fix 1/2): even though the pid is
-- unchanged, every row for it disappears, because the reader checks
-- ownership, not just "is there a payload here".
RESET pg_wait_event_tracing.capture;
SELECT pg_sleep(0.05);
SELECT count(*) AS rows_after_disable
FROM pg_stat_wait_event_timing
WHERE pid = pg_backend_pid();

--
-- Reset authorization (fix 4).  The pg_signal_backend-member-vs-ordinary-
-- target and non-superuser-vs-superuser-target cases need a second, real
-- backend with a different owning role; those live in the TAP test
-- t/003_reset_acl.pl (WP4a), which can create and authenticate as extra
-- roles portably (this regress test cannot: no second connection is
-- available here, and resetting your own pid always takes the synchronous
-- self-reset path regardless of role).  What is single-session-testable is
-- _all()'s superuser requirement, which holds even for a role granted
-- EXECUTE directly, not just relying on the extension script's default
-- REVOKE EXECUTE FROM PUBLIC.
--
CREATE ROLE regress_pwet_signaler;
GRANT EXECUTE ON FUNCTION pg_stat_reset_wait_event_timing_all()
    TO regress_pwet_signaler;
SET ROLE regress_pwet_signaler;
SELECT pg_stat_reset_wait_event_timing_all();
RESET ROLE;
REVOKE EXECUTE ON FUNCTION pg_stat_reset_wait_event_timing_all()
    FROM regress_pwet_signaler;
DROP ROLE regress_pwet_signaler;
--
-- Per-class capacity (plan sec 3.1).  Every class pg_wait_events knows
-- about must have a capacity row, and every class must have at least 4
-- events of headroom below its capacity, so that whoever adds an event
-- past that headroom is caught here rather than by silent overflow
-- counting.
--
SELECT count(*) AS classes_missing_capacity
FROM (SELECT DISTINCT type FROM pg_wait_events) t
WHERE NOT EXISTS (
    SELECT 1 FROM pg_wait_event_tracing_capacity() c WHERE c.type = t.type);

SELECT bool_and(cap.capacity - cnt.n >= 4) AS capacity_headroom_ok
FROM (SELECT type, count(*) AS n FROM pg_wait_events GROUP BY type) cnt
JOIN pg_wait_event_tracing_capacity() cap USING (type);

RESET pg_wait_event_tracing.capture;
