\set ECHO none

-- This file is included by benchmark tests to clean up performance monitoring helpers.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_monitor') THEN
        CALL sql_saga.benchmark_teardown();
        -- Reset tracking to default to ensure test isolation.
        SET pg_stat_monitor.pgsm_track = 'top';
    END IF;
END;
$$;

\set ECHO all