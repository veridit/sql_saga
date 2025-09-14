-- Calculate rows per second
CREATE OR REPLACE FUNCTION format_duration(p_interval interval) RETURNS TEXT AS $$
BEGIN
    IF p_interval IS NULL THEN RETURN ''; END IF;
    IF EXTRACT(EPOCH FROM p_interval) >= 1 THEN
        RETURN ROUND(EXTRACT(EPOCH FROM p_interval))::numeric || ' secs';
    ELSE
        RETURN ROUND(EXTRACT(EPOCH FROM p_interval) * 1000)::numeric || ' ms';
    END IF;
END;
$$ LANGUAGE plpgsql;

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
