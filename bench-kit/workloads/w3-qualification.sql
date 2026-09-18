\if :{?appname}
\else
\echo 'appname variable is required'
\quit
\endif

WITH target AS
(
  SELECT w.*
  FROM pg_stat_wait_event_timing AS w
  JOIN pg_stat_activity AS a USING (pid)
  WHERE a.application_name = :'appname'
    AND a.backend_type = 'client backend'
),
events AS
(
  SELECT wait_event_type,
         wait_event,
         sum(calls)::bigint AS calls,
         sum(total_time_ms)::double precision AS total_time_ms,
         max(max_time_us)::double precision AS max_time_us
  FROM target
  GROUP BY wait_event_type, wait_event
),
histograms AS
(
  SELECT wait_event_type,
         wait_event,
         ord,
         sum(bucket)::bigint AS bucket
  FROM target
  CROSS JOIN LATERAL
    unnest(histogram) WITH ORDINALITY AS h(bucket, ord)
  GROUP BY wait_event_type, wait_event, ord
)
SELECT e.wait_event_type,
       e.wait_event,
       e.calls,
       e.total_time_ms,
       e.max_time_us,
       array_to_string(array_agg(h.bucket ORDER BY h.ord), ',')
FROM events AS e
JOIN histograms AS h USING (wait_event_type, wait_event)
GROUP BY e.wait_event_type, e.wait_event, e.calls,
         e.total_time_ms, e.max_time_us
ORDER BY e.calls DESC, e.wait_event_type, e.wait_event;
