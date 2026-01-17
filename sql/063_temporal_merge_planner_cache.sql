\i sql/include/test_setup.sql

BEGIN;
\echo '----------------------------------------------------------------------------'
\echo 'Test: `temporal_merge_plan` EXPLAIN output for performance monitoring'
\echo 'This test verifies that the planner function produces a stable and efficient'
\echo 'EXPLAIN plan. Its output is captured to monitor for performance regressions.'
\echo '----------------------------------------------------------------------------'

-- Instruct temporal_merge to RAISE NOTICE the sql used for analysis.
SET client_min_messages TO NOTICE;
SET sql_saga.temporal_merge.log_sql = true;

CREATE SCHEMA tmpc;

--------------------------------------------------------------------------------
-- SCENARIO 1: Simple Surrogate Primary Key (id int NOT NULL)
--------------------------------------------------------------------------------
SAVEPOINT s1;
\echo '\n--- Scenario 1: Simple Surrogate Primary Key (id int NOT NULL) ---'

CREATE TABLE tmpc.target (id int NOT NULL, valid_range daterange, valid_from date, valid_until date, value text);
SELECT sql_saga.add_era('tmpc.target', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');

CREATE TABLE tmpc.source1 (row_id int, id int NOT NULL, valid_from date, valid_until date, value text);

\echo '--- Setting up tables with indexes and data for a realistic plan ---'
SELECT sql_saga.add_unique_key(table_oid => 'tmpc.target'::regclass, column_names => ARRAY['id'], key_type => 'primary');
CREATE INDEX ON tmpc.source1 (id);
CREATE INDEX ON tmpc.source1 USING gist (daterange(valid_from, valid_until));
\echo '\d tmpc.target'
\d tmpc.target
\echo '\d tmpc.source1'
\d tmpc.source1

-- Insert enough rows to make an index scan more attractive to the planner.
INSERT INTO tmpc.target (id, valid_from, valid_until, value)
SELECT i, '2023-01-01', '2024-01-01', 'A' FROM generate_series(1, 1000) as i;
INSERT INTO tmpc.source1 VALUES (1, 500, '2023-06-01', '2023-07-01', 'B');
ANALYZE tmpc.target;
ANALYZE tmpc.source1;

\echo '\n--- Performance Monitoring: EXPLAIN the cached planner query ---'
DO $$
DECLARE
    plan_sqls JSONB;
    sql_step JSONB;
    sql_stmt TEXT;
    rec RECORD;
    has_seq_scan BOOLEAN;
BEGIN
    -- Run once to populate cache. Output is discarded.
    PERFORM * FROM sql_saga.temporal_merge_plan(
        target_table => 'tmpc.target'::regclass,
        source_table => 'tmpc.source1'::regclass,
        identity_columns => '{id}'::text[],
        mode => 'MERGE_ENTITY_PATCH'::sql_saga.temporal_merge_mode,
        era_name => 'valid'
    );

    -- Find the plan SQLs for the source table of this scenario.
    SELECT c.plan_sqls INTO plan_sqls
    FROM pg_temp.temporal_merge_plan_cache c
    WHERE c.cache_key LIKE '%' || 'tmpc.source1'::regclass::text;

    -- Clean up temp tables created by the first run so we can re-execute them.
    CALL sql_saga.temporal_merge_drop_temp_tables();

    RAISE NOTICE '--- Explaining % steps from cache', jsonb_array_length(plan_sqls);
    FOR sql_step IN SELECT * FROM jsonb_array_elements(plan_sqls)
    LOOP
        RAISE NOTICE '---';
        sql_stmt := sql_step->>'sql';

        IF sql_step->>'type' = 'setup' THEN
            RAISE NOTICE 'Executing setup: %', sql_stmt;
            EXECUTE sql_stmt;
        ELSE
            RAISE NOTICE 'Explaining: %', sql_stmt;
            has_seq_scan := false;
            FOR rec IN EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS) ' || sql_stmt
            LOOP
                RAISE NOTICE '%', rec."QUERY PLAN";
                -- The schema is hardcoded here, which is fine for this specific test.
                -- We add a space to not match "target_rows".
                IF rec."QUERY PLAN" LIKE '%Seq Scan on target %' THEN
                    has_seq_scan := true;
                END IF;
            END LOOP;

            IF has_seq_scan THEN
                RAISE EXCEPTION 'Performance regression detected: EXPLAIN plan contains a "Seq Scan on target".';
            END IF;
        END IF;
    END LOOP;
END;
$$;
\echo '--- OK: Verified that EXPLAIN plan is optimal. ---'
ROLLBACK TO SAVEPOINT s1;

--------------------------------------------------------------------------------
-- SCENARIO 2: Composite Natural Key (NOT NULL columns)
--------------------------------------------------------------------------------
SAVEPOINT s2;
\echo '\n--- Scenario 2: Composite Natural Key (NOT NULL columns) ---'

CREATE TABLE tmpc.target_nk_not_null (type TEXT NOT NULL, lu_id INT NOT NULL, value TEXT, valid_range daterange, valid_from date, valid_until date);
SELECT sql_saga.add_era('tmpc.target_nk_not_null', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
CREATE TABLE tmpc.source_nk_not_null (row_id int, type text NOT NULL, lu_id int NOT NULL, value text, valid_from date, valid_until date);

\echo '--- Setting up tables with indexes and data for a realistic plan ---'
SELECT sql_saga.add_unique_key(table_oid => 'tmpc.target_nk_not_null'::regclass, column_names => ARRAY['type', 'lu_id'], key_type => 'primary');
INSERT INTO tmpc.target_nk_not_null (type, lu_id, valid_from, valid_until, value) SELECT 'A', i, '2023-01-01', '2024-01-01', 'LU' FROM generate_series(1, 1000) as i;
INSERT INTO tmpc.source_nk_not_null VALUES (1, 'A', 500, 'LU-patched', '2023-06-01', '2023-07-01');
ANALYZE tmpc.target_nk_not_null;
ANALYZE tmpc.source_nk_not_null;

\echo '\d tmpc.target_nk_not_null'
\d tmpc.target_nk_not_null
\echo '\d tmpc.source_nk_not_null'
\d tmpc.source_nk_not_null

\echo '\n--- Performance Monitoring: EXPLAIN the cached planner query (Natural Key, NOT NULL) ---'
DO $$
DECLARE
    plan_sqls JSONB;
    sql_step JSONB;
    sql_stmt TEXT;
    rec RECORD;
    has_seq_scan BOOLEAN;
BEGIN
    -- Run once to populate cache. Output is discarded.
    PERFORM * FROM sql_saga.temporal_merge_plan(
        target_table => 'tmpc.target_nk_not_null'::regclass,
        source_table => 'tmpc.source_nk_not_null'::regclass,
        identity_columns => '{type,lu_id}'::text[],
        mode => 'MERGE_ENTITY_PATCH'::sql_saga.temporal_merge_mode,
        era_name => 'valid'
    );

    -- Find the plan SQLs for the source table of this scenario.
    SELECT c.plan_sqls INTO plan_sqls
    FROM pg_temp.temporal_merge_plan_cache c
    WHERE c.cache_key LIKE '%' || 'tmpc.source_nk_not_null'::regclass::text;

    -- Clean up temp tables created by the first run so we can re-execute them.
    CALL sql_saga.temporal_merge_drop_temp_tables();

    RAISE NOTICE '--- Explaining % steps from cache', jsonb_array_length(plan_sqls);
    FOR sql_step IN SELECT * FROM jsonb_array_elements(plan_sqls)
    LOOP
        RAISE NOTICE '---';
        sql_stmt := sql_step->>'sql';

        IF sql_step->>'type' = 'setup' THEN
            RAISE NOTICE 'Executing setup: %', sql_stmt;
            EXECUTE sql_stmt;
        ELSE
            RAISE NOTICE 'Explaining: %', sql_stmt;
            has_seq_scan := false;
            FOR rec IN EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS) ' || sql_stmt
            LOOP
                RAISE NOTICE '%', rec."QUERY PLAN";
                -- We add a space to not match temp tables.
                IF rec."QUERY PLAN" LIKE '%Seq Scan on target_nk_not_null %' THEN
                    has_seq_scan := true;
                END IF;
            END LOOP;

            IF has_seq_scan THEN
                RAISE EXCEPTION 'Performance regression detected: EXPLAIN plan for non-null natural key contains a "Seq Scan on target_nk_not_null".';
            END IF;
        END IF;
    END LOOP;
END;
$$;
\echo '--- OK: Verified that EXPLAIN plan for non-null natural key is optimal. ---'
ROLLBACK TO SAVEPOINT s2;

--------------------------------------------------------------------------------
-- SCENARIO 3: Composite Natural Key with NULLable XOR columns
--------------------------------------------------------------------------------
SAVEPOINT s3;
\echo '\n--- Scenario 3: Composite Natural Key with NULLable XOR columns ---'

CREATE TABLE tmpc.target_nk (type TEXT NOT NULL, lu_id INT, es_id INT, value TEXT, valid_range daterange, valid_from date, valid_until date,
    CONSTRAINT lu_or_es_id_check CHECK ((lu_id IS NOT NULL AND es_id IS NULL) OR (lu_id IS NULL AND es_id IS NOT NULL))
);
SELECT sql_saga.add_era('tmpc.target_nk', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
CREATE TABLE tmpc.source_nk (row_id int, type text NOT NULL, lu_id int, es_id int, value text, valid_from date, valid_until date,
    CONSTRAINT source_lu_or_es_id_check CHECK ((lu_id IS NOT NULL AND es_id IS NULL) OR (lu_id IS NULL AND es_id IS NOT NULL))
);

\echo '--- Setting up natural key tables with partial indexes and data ---'
SELECT sql_saga.add_unique_key(table_oid => 'tmpc.target_nk'::regclass, column_names => ARRAY['type', 'lu_id'], key_type => 'predicated', predicate => 'es_id IS NULL');
SELECT sql_saga.add_unique_key(table_oid => 'tmpc.target_nk'::regclass, column_names => ARRAY['type', 'es_id'], key_type => 'predicated', predicate => 'lu_id IS NULL');
INSERT INTO tmpc.target_nk (type, lu_id, es_id, valid_from, valid_until, value) SELECT 'A', i, NULL, '2023-01-01', '2024-01-01', 'LU' FROM generate_series(1, 1000) as i;
INSERT INTO tmpc.target_nk (type, lu_id, es_id, valid_from, valid_until, value) SELECT 'B', NULL, i, '2023-01-01', '2024-01-01', 'ES' FROM generate_series(1, 1000) as i;
INSERT INTO tmpc.source_nk VALUES (1, 'A', 500, NULL, 'LU-patched', '2023-06-01', '2023-07-01');
INSERT INTO tmpc.source_nk VALUES (2, 'B', NULL, 500, 'ES-patched', '2023-06-01', '2023-07-01');
ANALYZE tmpc.target_nk;
ANALYZE tmpc.source_nk;

\echo '\d tmpc.target_nk'
\d tmpc.target_nk
\echo '\d tmpc.source_nk'
\d tmpc.source_nk

\echo '\n--- Performance Monitoring: EXPLAIN the cached planner query (Natural Key, NULLable) ---'
DO $$
DECLARE
    plan_sqls JSONB;
    sql_step JSONB;
    sql_stmt TEXT;
    rec RECORD;
    has_seq_scan BOOLEAN;
BEGIN
    -- Run once to populate cache. Output is discarded.
    PERFORM * FROM sql_saga.temporal_merge_plan(
        target_table => 'tmpc.target_nk'::regclass,
        source_table => 'tmpc.source_nk'::regclass,
        identity_columns => '{type,lu_id,es_id}'::text[],
        mode => 'MERGE_ENTITY_PATCH'::sql_saga.temporal_merge_mode,
        era_name => 'valid'
    );

    -- Find the plan SQLs for the source table of this scenario.
    SELECT c.plan_sqls INTO plan_sqls
    FROM pg_temp.temporal_merge_plan_cache c
    WHERE c.cache_key LIKE '%' || 'tmpc.source_nk'::regclass::text;

    -- Clean up temp tables created by the first run so we can re-execute them.
    CALL sql_saga.temporal_merge_drop_temp_tables();

    RAISE NOTICE '--- Explaining % steps from cache', jsonb_array_length(plan_sqls);
    FOR sql_step IN SELECT * FROM jsonb_array_elements(plan_sqls)
    LOOP
        RAISE NOTICE '---';
        sql_stmt := sql_step->>'sql';

        IF sql_step->>'type' = 'setup' THEN
            RAISE NOTICE 'Executing setup: %', sql_stmt;
            EXECUTE sql_stmt;
        ELSE
            RAISE NOTICE 'Explaining: %', sql_stmt;
            has_seq_scan := false;
            FOR rec IN EXECUTE 'EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS) ' || sql_stmt
            LOOP
                RAISE NOTICE '%', rec."QUERY PLAN";
                -- We add a space to not match temp tables or other target tables.
                IF rec."QUERY PLAN" LIKE '%Seq Scan on target_nk %' THEN
                    has_seq_scan := true;
                END IF;
            END LOOP;

            IF has_seq_scan THEN
                RAISE EXCEPTION 'Performance regression detected: EXPLAIN plan for nullable natural key contains a "Seq Scan on target_nk".';
            END IF;
        END IF;
    END LOOP;
END;
$$;
\echo '--- OK: Verified that EXPLAIN plan for natural key is optimal. ---'
ROLLBACK TO SAVEPOINT s3;

SET client_min_messages TO NOTICE;
ROLLBACK;
\i sql/include/test_teardown.sql
