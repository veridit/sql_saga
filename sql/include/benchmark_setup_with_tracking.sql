-- benchmark_setup_with_tracking.sql
--
-- Use this instead of benchmark_setup.sql when you need detailed query profiling
-- via pg_stat_monitor. This enables pgsm_track='all' which tracks nested queries
-- inside PL/pgSQL functions.
--
-- WARNING: This causes O(n) overhead that grows with each batch as more queries
-- are tracked. Only use for debugging/profiling, not for measuring true performance.

\i sql/include/benchmark_setup.sql

-- Enable nested query tracking for detailed profiling
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_monitor') THEN
        EXECUTE 'SET pg_stat_monitor.pgsm_track = ''all''';
    END IF;
END;
$$;
