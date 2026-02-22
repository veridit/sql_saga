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
    --
    -- The plan cache only exists when the PL/pgSQL planner was used.
    -- The native Rust planner inserts directly into temporal_merge_plan
    -- and does not populate the plan cache. Skip gracefully in that case.
    IF to_regclass('pg_temp.temporal_merge_plan_cache') IS NULL THEN
        -- Native Rust planner was used. Report cache stats instead of EXPLAIN replay.
        INSERT INTO benchmark_explain_output VALUES ('--- Native planner: EXPLAIN replay not available (no plan cache) ---');

        IF to_regproc('sql_saga.temporal_merge_native_cache_stats') IS NOT NULL THEN
            INSERT INTO benchmark_explain_output VALUES ('--- Native planner cache stats ---');
            FOR rec IN SELECT stat_name || ': ' || stat_value AS line
                       FROM sql_saga.temporal_merge_native_cache_stats()
            LOOP
                INSERT INTO benchmark_explain_output VALUES (rec.line);
            END LOOP;
        END IF;

        IF to_regproc('sql_saga.temporal_merge_executor_introspect') IS NOT NULL THEN
            INSERT INTO benchmark_explain_output VALUES ('--- Executor partition info for ' || p_target_table::text || ' ---');
            DECLARE
                v_cache sql_saga.temporal_merge_executor_cache;
                v_part INT;
            BEGIN
                SELECT * INTO v_cache FROM sql_saga.temporal_merge_executor_introspect(
                    target_table => p_target_table::oid,
                    source_table => p_source_table::oid
                );
                INSERT INTO benchmark_explain_output VALUES (format('partition_count: %s', COALESCE(array_length(v_cache.partition_join_clauses, 1), 0)));
                FOR v_part IN 1..COALESCE(array_length(v_cache.partition_join_clauses, 1), 0) LOOP
                    INSERT INTO benchmark_explain_output VALUES (format('partition %s: filter=%s  join=%s', v_part, v_cache.partition_plan_filters[v_part], v_cache.partition_join_clauses[v_part]));
                END LOOP;
            EXCEPTION WHEN OTHERS THEN
                INSERT INTO benchmark_explain_output VALUES ('--- Could not introspect executor cache: ' || SQLERRM || ' ---');
            END;
        END IF;

        RETURN;
    END IF;

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

-- Check for seq scans on the target table. While seq scans as part of Hash Joins
-- can be optimal for small source tables, we flag them as potential regressions
-- to be manually reviewed. If the seq scan is acceptable (e.g., part of an efficient
-- hash join), update the expected output to include the error message.
--
-- We use \if with a query result instead of a function to avoid volatile pg_temp_XX
-- function names appearing in error context messages.
SELECT EXISTS (
    SELECT 1 FROM benchmark_explain_output
    WHERE line LIKE '%Seq Scan on ' || :'benchmark_target_table' || ' %'
) AS has_seq_scan \gset

\if :has_seq_scan
\warn Performance regression detected: EXPLAIN plan contains a Seq Scan on :benchmark_target_table
\endif

DROP PROCEDURE benchmark_explain_plan_run;

\set ECHO all
