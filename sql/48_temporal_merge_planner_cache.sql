\i sql/include/test_setup.sql

BEGIN;
\echo '----------------------------------------------------------------------------'
\echo 'Test: `temporal_merge_plan` caching and EXPLAIN output'
\echo 'This test verifies that the planner function correctly caches its prepared'
\echo 'statement and provides a stable EXPLAIN plan for regression monitoring.'
\echo '----------------------------------------------------------------------------'

SET client_min_messages TO WARNING;
CREATE SCHEMA tmpc;

CREATE TABLE tmpc.target (id int, valid_from date, valid_until date, value text);
SELECT sql_saga.add_era('tmpc.target', 'valid_from', 'valid_until');

CREATE TEMP TABLE source1 (row_id int, id int, valid_from date, valid_until date, value text);
CREATE TEMP TABLE source2 (row_id int, id int, valid_from date, valid_until date, value text);

\echo '--- Call planner for source1 ---'
-- This call will generate and cache the plan.
SELECT * FROM sql_saga.temporal_merge_plan(
    p_target_table      => 'tmpc.target'::regclass,
    p_source_table      => 'source1'::regclass,
    p_id_columns        => '{id}'::text[],
    p_ephemeral_columns => '{}'::text[],
    p_mode              => 'MERGE_ENTITY_PATCH'::sql_saga.temporal_merge_mode,
    p_era_name          => 'valid'
);

\echo '--- Call planner for source2 ---'
-- This call should generate a *different* plan because the source table is different.
SELECT * FROM sql_saga.temporal_merge_plan(
    p_target_table      => 'tmpc.target'::regclass,
    p_source_table      => 'source2'::regclass,
    p_id_columns        => '{id}'::text[],
    p_ephemeral_columns => '{}'::text[],
    p_mode              => 'MERGE_ENTITY_PATCH'::sql_saga.temporal_merge_mode,
    p_era_name          => 'valid'
);

\echo '--- Call planner for source1 again ---'
-- This call should hit the cache for the first plan.
SELECT * FROM sql_saga.temporal_merge_plan(
    p_target_table      => 'tmpc.target'::regclass,
    p_source_table      => 'source1'::regclass,
    p_id_columns        => '{id}'::text[],
    p_ephemeral_columns => '{}'::text[],
    p_mode              => 'MERGE_ENTITY_PATCH'::sql_saga.temporal_merge_mode,
    p_era_name          => 'valid'
);

\echo '--- Verify that two distinct plans are cached ---'
SELECT count(*)::int as num_cached_plans FROM pg_prepared_statements WHERE name LIKE 'tm_plan_%';

\echo '\n--- Performance Monitoring: EXPLAIN the cached planner query for source1 ---'
\echo '--- This output is captured to monitor for regressions in the query plan, such as the introduction of inefficient joins. ---'
-- Use \gset to capture the dynamically generated EXPLAIN command into a variable.
-- This avoids echoing the command with its unstable MD5 hash.
SELECT format('EXPLAIN (COSTS OFF) EXECUTE %I;', name) as explain_command
FROM pg_prepared_statements
WHERE name LIKE 'tm_plan_%' AND statement LIKE '%FROM source1 t%'
\gset

-- Now, echo a stable placeholder for the regression test's expected output...
\echo EXPLAIN (COSTS OFF) EXECUTE tm_plan_<...>;
-- ...and then execute the actual command we captured.
:explain_command

SET client_min_messages TO NOTICE;
ROLLBACK;
\i sql/include/test_teardown.sql
