\set ECHO none

-- This file is included by all benchmark tests. It sets up a common logging
-- table and initializes performance monitoring if enabled.

-- These helpers are defined here to be available to all benchmark tests,
-- without polluting the main extension source. They are defined with `CREATE OR REPLACE`
-- to be idempotent across multiple inclusions of this file.
CREATE OR REPLACE PROCEDURE sql_saga.benchmark_reset_pg_stat_monitor()
LANGUAGE plpgsql SECURITY DEFINER AS $procedure$
BEGIN
    -- This check is intentionally redundant with the caller's check. It ensures
    -- this helper is a no-op if called in an environment without pg_stat_monitor.
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_monitor') THEN
        PERFORM pg_stat_monitor_reset();
    END IF;
END;
$procedure$;

CREATE OR REPLACE PROCEDURE sql_saga.benchmark_reset()
LANGUAGE plpgsql SECURITY DEFINER AS $procedure$
BEGIN
    CALL sql_saga.benchmark_reset_pg_stat_monitor();
END;
$procedure$;

CREATE OR REPLACE FUNCTION sql_saga.__internal_get_pg_stat_monitor_data()
RETURNS TABLE(
    queryid text,
    calls bigint,
    total_exec_time double precision,
    rows bigint,
    shared_blks_hit bigint,
    shared_blks_read bigint,
    temp_blks_read bigint,
    temp_blks_written bigint,
    total_plan_time double precision,
    wal_records bigint,
    wal_bytes numeric,
    query text
)
LANGUAGE plpgsql SECURITY DEFINER AS $function$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_monitor') THEN
        RETURN QUERY EXECUTE 'SELECT queryid::text, calls, total_exec_time, rows, shared_blks_hit, shared_blks_read, temp_blks_read, temp_blks_written, total_plan_time, wal_records, wal_bytes, query FROM pg_stat_monitor';
    END IF;
END;
$function$;

CREATE OR REPLACE PROCEDURE sql_saga.benchmark_log_and_reset(p_event TEXT)
LANGUAGE plpgsql SECURITY DEFINER AS $procedure$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_monitor') THEN
        INSERT INTO pg_temp.benchmark_monitor_log (log_id, event, label, queryid, calls, total_exec_time, "rows", shared_blks_hit, shared_blks_read, temp_blks_read, temp_blks_written, total_plan_time, wal_records, wal_bytes, query)
        SELECT
            substr(md5(random()::text), 1, 3), p_event, NULL,
            d.queryid, d.calls, d.total_exec_time, d.rows, d.shared_blks_hit, d.shared_blks_read,
            d.temp_blks_read, d.temp_blks_written, d.total_plan_time, d.wal_records, d.wal_bytes, d.query
        FROM sql_saga.__internal_get_pg_stat_monitor_data() d
        -- Filter out the anonymous DO blocks used to call this logging function.
        WHERE d.query NOT SIMILAR TO 'DO (\$[a-zA-Z0-9_]*\$|$$)%';

        PERFORM pg_stat_monitor_reset();
    END IF;
END;
$procedure$;

CREATE OR REPLACE PROCEDURE sql_saga.benchmark_teardown()
LANGUAGE plpgsql SECURITY DEFINER AS $procedure$
BEGIN
    DROP PROCEDURE IF EXISTS sql_saga.benchmark_log_and_reset(TEXT);
    DROP FUNCTION IF EXISTS sql_saga.__internal_get_pg_stat_monitor_data();
    DROP PROCEDURE IF EXISTS sql_saga.benchmark_reset_pg_stat_monitor();
    DROP PROCEDURE IF EXISTS sql_saga.benchmark_teardown();
    DROP PROCEDURE IF EXISTS sql_saga.benchmark_reset();
    DROP VIEW IF EXISTS benchmark_monitor_log_filtered;
END;
$procedure$;


DO $$
BEGIN
    -- Only create the table if it doesn't already exist in this session.
    IF to_regclass('pg_temp.benchmark') IS NULL THEN
        CREATE TEMPORARY TABLE benchmark (
          seq_id BIGINT GENERATED ALWAYS AS IDENTITY,
          timestamp TIMESTAMPTZ DEFAULT clock_timestamp(),
          event TEXT,
          row_count INTEGER,
          is_performance_benchmark BOOLEAN NOT NULL
        );
        GRANT ALL ON TABLE benchmark TO sql_saga_unprivileged_user;
    END IF;

    IF to_regclass('pg_temp.benchmark_explain_output') IS NULL THEN
        CREATE TEMP TABLE benchmark_explain_output(line TEXT) ON COMMIT PRESERVE ROWS;
        GRANT ALL ON TABLE benchmark_explain_output TO sql_saga_unprivileged_user;
    END IF;

    -- Initialize performance monitoring if pg_stat_monitor extension exists.
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_monitor')
    THEN
        -- NOTE: pgsm_track='all' is NOT enabled by default because it causes O(n)
        -- overhead that grows with each batch as more queries are tracked.
        --
        -- Tests that need detailed query profiling should include benchmark_setup_with_tracking.sql
        -- instead, or call: SET pg_stat_monitor.pgsm_track = 'all'; after this file.

        IF to_regclass('pg_temp.benchmark_monitor_log') IS NULL THEN
            CREATE TEMP TABLE benchmark_monitor_log (
                log_id TEXT,
                event TEXT,
                label TEXT,
                queryid TEXT,
                calls BIGINT,
                total_exec_time DOUBLE PRECISION,
                rows BIGINT,
                shared_blks_hit BIGINT,
                shared_blks_read BIGINT,
                temp_blks_read BIGINT,
                temp_blks_written BIGINT,
                total_plan_time DOUBLE PRECISION,
                wal_records BIGINT,
                wal_bytes NUMERIC,
                query TEXT
            )
            ON COMMIT PRESERVE ROWS;
            
            GRANT ALL ON TABLE benchmark_monitor_log TO sql_saga_unprivileged_user;

            CREATE TEMP VIEW benchmark_monitor_log_filtered AS
                SELECT
                    log_id,
                    event,
                    label,
                    queryid,
                    calls,
                    total_exec_time,
                    "rows",
                    shared_blks_hit,
                    shared_blks_read,
                    temp_blks_read,
                    temp_blks_written,
                    total_plan_time,
                    wal_records,
                    wal_bytes,
                    replace(query, '
', ' ') AS query
                FROM pg_temp.benchmark_monitor_log
                WHERE total_exec_time > 10.0 OR calls > 100
                ORDER BY event, label, total_exec_time DESC, query;
                
            GRANT ALL ON benchmark_monitor_log_filtered TO sql_saga_unprivileged_user;

        END IF;
    END IF;
END;
$$;

-- Add helper for benchmark reporting, before user switching in tests.
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


-- Reset stats at the very end of setup, so the setup itself is not benchmarked.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_monitor') THEN
        CALL sql_saga.benchmark_reset_pg_stat_monitor();
    END IF;
END;
$$;

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('BEGIN', 0, false);

\set ECHO all
