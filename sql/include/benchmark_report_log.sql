\set ECHO none

-- Ref. https://stackoverflow.com/a/32597876/1023558
\set benchmark_log_filename :benchmark_log_filename
-- now benchmark_log_filename is set to the string ':benchmark_log_filename' if was not already set.
-- Checking it using a CASE statement:
SELECT CASE
  WHEN :'benchmark_log_filename'= ':benchmark_log_filename'
  THEN 'false'
  ELSE 'true'
END::BOOL AS "benchmark_log_filename_is_set" \gset
-- \gset call at end of the query to set the variable "benchmark_log_filename_is_set"
\if :benchmark_log_filename_is_set
\else
\echo ":benchmark_log_filename is missing set it with \set benchmark_log_filename ..."
\quit
\endif

\o :benchmark_log_filename

-- Calculate rows per second
WITH benchmark_events AS (
  SELECT
    seq_id,
    timestamp,
    event,
    row_count,
    is_performance_benchmark,
    regexp_replace(event, ' (start|end)$', '') AS operation,
    CASE
      WHEN event LIKE '% start' THEN 'start'
      WHEN event LIKE '% end' THEN 'end'
      ELSE 'milestone'
    END AS phase,
    FIRST_VALUE(timestamp) OVER (ORDER BY seq_id) as benchmark_start_time,
    LAG(timestamp) OVER (ORDER BY seq_id) as prev_event_time
  FROM
    benchmark
),
benchmark_durations AS (
  SELECT
    *,
    LAG(timestamp) OVER (PARTITION BY operation ORDER BY seq_id) AS operation_start_time
  FROM
    benchmark_events
)
SELECT
  CASE WHEN phase = 'end' THEN operation ELSE event END AS event,
  row_count,
  format_duration(timestamp - benchmark_start_time) AS time_from_start,
  CASE
    WHEN phase = 'end' THEN format_duration(timestamp - operation_start_time)
    WHEN phase = 'milestone' THEN format_duration(timestamp - prev_event_time)
    ELSE ''
  END AS time_from_prev,
  row_count || ' rows' AS row_count,
  CASE
    WHEN is_performance_benchmark AND phase = 'end' THEN
        '~' ||
            CASE
            WHEN EXTRACT(EPOCH FROM (timestamp - operation_start_time)) > 0.001 THEN
                ROUND(row_count::FLOAT8 / EXTRACT(EPOCH FROM (timestamp - operation_start_time)))::text
            ELSE
                '0'
            END
        || ' rows/s'
    ELSE
        ''
  END AS rows_per_second
FROM
  benchmark_durations
WHERE phase <> 'start'
ORDER BY
  seq_id;

\o

\set ECHO all