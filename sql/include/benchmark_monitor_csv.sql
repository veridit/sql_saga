\set ECHO none

-- Ref. https://stackoverflow.com/a/32597876/1023558
\set monitor_log_filename :monitor_log_filename
-- now monitor_log_filename is set to the string ':monitor_log_filename' if was not already set.
-- Checking it using a CASE statement:
SELECT CASE
  WHEN :'monitor_log_filename'= ':monitor_log_filename'
  THEN 'false'
  ELSE 'true'
END::BOOL AS "monitor_log_filename_is_set" \gset
-- \gset call at end of the query to set the variable "monitor_log_filename_is_set"
\if :monitor_log_filename_is_set
\else
\echo ":monitor_log_filename is missing set it with \set monitor_log_filename ..."
\endif

-- This file is included by benchmark tests to dump the pg_stat_monitor log.
-- It expects the variable :monitor_log_filename to be set by the caller.

-- Check if pg_stat_monitor is installed, and output performance log if so.
SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_monitor') AS pg_stat_monitor_exists \gset
\if :pg_stat_monitor_exists
    -- A simple, stable check that the performance log was populated.
    SELECT count(*) > 0 AS monitor_log_has_rows FROM pg_temp.benchmark_monitor_log;

    -- Redirect detailed performance log to its own CSV file.
    \pset format csv
    \o :monitor_log_filename
    \copy (SELECT * FROM benchmark_monitor_log_filtered) to stdout with csv header
    \o
    \pset format aligned
\else
    SELECT 'pg_stat_monitor not installed, skipping monitor log output.' as notice;
\endif

\set ECHO all
