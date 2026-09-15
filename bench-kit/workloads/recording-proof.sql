\if :{?appname}
\else
\echo 'appname variable is required'
\quit
\endif

\if :{?expect_trace}
\else
\echo 'expect_trace variable is required'
\quit
\endif

WITH clients AS MATERIALIZED
(
  SELECT pid, backend_type
  FROM pg_stat_activity
  WHERE application_name = :'appname'
    AND backend_type = 'client backend'
),
timing AS MATERIALIZED
(
  SELECT t.pid, sum(t.calls)::bigint AS calls
  FROM pg_stat_wait_event_timing AS t
  JOIN clients AS c USING (pid)
  GROUP BY t.pid
),
representative AS MATERIALIZED
(
  SELECT t.procnumber
  FROM pg_stat_wait_event_timing AS t
  JOIN clients AS c USING (pid)
  ORDER BY t.pid
  LIMIT 1
)
SELECT
  (SELECT count(*) FROM clients) AS client_count,
  (SELECT count(*) FROM timing WHERE calls > 0) AS clients_recording,
  coalesce((SELECT sum(calls) FROM timing), 0) AS timing_calls,
  CASE WHEN :'expect_trace'::boolean THEN
    coalesce((
      SELECT count(*)
      FROM representative AS r,
           LATERAL pg_get_wait_event_trace(r.procnumber)
      WHERE wait_event_type <> 'Query'
    ), 0)
  ELSE 0 END AS representative_trace_records;
