\set ECHO none

-- Check for required variables
\set benchmark_explain_log_filename :benchmark_explain_log_filename
SELECT CASE WHEN :'benchmark_explain_log_filename' = ':benchmark_explain_log_filename' THEN 'false' ELSE 'true' END::BOOL AS "benchmark_explain_log_filename_is_set" \gset
\if :benchmark_explain_log_filename_is_set
\else
  \echo ":benchmark_explain_log_filename is missing. Set it with \set benchmark_explain_log_filename ..."
  \quit
\endif

\set benchmark_source_table :benchmark_source_table
SELECT CASE WHEN :'benchmark_source_table' = ':benchmark_source_table' THEN 'false' ELSE 'true' END::BOOL AS "benchmark_source_table_is_set" \gset
\if :benchmark_source_table_is_set
\else
  \echo ":benchmark_source_table is missing. Set it with \set benchmark_source_table ..."
  \quit
\endif

\set benchmark_target_table :benchmark_target_table
SELECT CASE WHEN :'benchmark_target_table' = ':benchmark_target_table' THEN 'false' ELSE 'true' END::BOOL AS "benchmark_target_table_is_set" \gset
\if :benchmark_target_table_is_set
\else
  \echo ":benchmark_target_table is missing. Set it with \set benchmark_target_table ..."
  \quit
\endif

CREATE OR REPLACE PROCEDURE benchmark_explain_plan_run(p_source_table regclass, p_target_table regclass)
LANGUAGE plpgsql AS $procedure$
DECLARE
    plan_sqls JSONB;
    sql_step JSONB;
    sql_stmt TEXT;
    rec RECORD;
    v_target_table_name TEXT := p_target_table::text;
BEGIN
    -- The temp table is created outside, so we just clear it.
    DELETE FROM benchmark_explain_output;

    INSERT INTO benchmark_explain_output VALUES (chr(10) || '--- Performance Monitoring: EXPLAIN the cached planner query from previous temporal_merge call ---');

    -- The previous call to temporal_merge has already populated the plan cache.
    -- We retrieve the plan, re-run its setup steps to recreate temp tables,
    -- and then EXPLAIN its main query.

    -- Find the plan SQLs for the source table of this scenario.
    SELECT c.plan_sqls INTO plan_sqls
    FROM pg_temp.temporal_merge_plan_cache c
    WHERE c.cache_key LIKE '%' || p_source_table::text;

    IF plan_sqls IS NULL THEN
        RAISE EXCEPTION 'Could not find plan in cache for source table %', p_source_table;
    END IF;

    -- Clean up temp tables created by the temporal_merge call so we can re-execute them for EXPLAIN.
    CALL sql_saga.temporal_merge_drop_temp_tables();

    INSERT INTO benchmark_explain_output VALUES (format('--- Explaining %s steps from cache for source %s', jsonb_array_length(plan_sqls), p_source_table));
    FOR sql_step IN SELECT * FROM jsonb_array_elements(plan_sqls)
    LOOP
        INSERT INTO benchmark_explain_output VALUES ('---');
        sql_stmt := sql_step->>'sql';

        IF sql_step->>'type' = 'setup' THEN
            INSERT INTO benchmark_explain_output VALUES ('Executing setup: ' || sql_stmt);
            EXECUTE sql_stmt;
        ELSE
            INSERT INTO benchmark_explain_output VALUES ('Explaining: ' || sql_stmt);
            FOR rec IN EXECUTE 'EXPLAIN (ANALYZE, COSTS ON, TIMING ON, SUMMARY ON, BUFFERS) ' || sql_stmt
            LOOP
                INSERT INTO benchmark_explain_output VALUES(rec."QUERY PLAN");
            END LOOP;
        END IF;
    END LOOP;
END;
$procedure$;

-- Notice how we can access psql variables (e.g., :'benchmark_source_table') in this top-level script scope
-- and pass them as arguments to procedures. However, psql variables can NOT be referenced inside the body of
-- server-side constructs like procedures, functions, or DO blocks, as those are sent to the server as a
-- single literal string before psql has a chance to substitute the variables.
CALL benchmark_explain_plan_run(:'benchmark_source_table'::regclass, :'benchmark_target_table'::regclass);

\o :benchmark_explain_log_filename
\pset format unaligned
\pset tuples_only on
SELECT * FROM benchmark_explain_output;
\pset format aligned
\pset tuples_only off
\o

-- To check for regressions, we use a temporary function. This allows us to pass the psql variable
-- :'benchmark_target_table' as a parameter, avoiding the DO block variable substitution issue.
CREATE FUNCTION pg_temp.check_for_seq_scan(p_table_name TEXT) RETURNS void AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM benchmark_explain_output
        WHERE line LIKE '%Seq Scan on ' || p_table_name || ' %'
    ) THEN
        RAISE EXCEPTION 'Performance regression detected: EXPLAIN plan contains a "Seq Scan on %".', p_table_name;
    END IF;
END;
$$ LANGUAGE plpgsql;

SELECT pg_temp.check_for_seq_scan(:'benchmark_target_table'::text);
DROP FUNCTION pg_temp.check_for_seq_scan;

DROP PROCEDURE benchmark_explain_plan_run;

\set ECHO all
