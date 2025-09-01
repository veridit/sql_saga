\i sql/include/test_setup.sql

BEGIN;

-- TODO: Add more tests for parameter edge cases.
--
-- Future Scenarios to Test:
--
-- 1. `p_founding_id_column` behavior:
--    - Scenario 1b (Existing Entity): Create a target entity. Create a source table
--      with a `founding_id` and data that updates the existing entity. Verify
--      that `founding_id` is correctly ignored and the operation proceeds as a
--      normal update.
--
-- 2. Table structure variations:
--
-- 3. Parameter combinations:
--    - Scenario 3a: Test `p_update_source_with_assigned_entity_ids = true` in
--      conjunction with a multi-row `p_founding_id_column` scenario to ensure
--      the generated surrogate key is correctly back-filled to all source rows
--      sharing the founding_id.

SAVEPOINT scenario_6;
-- Scenario 5: Test `temporal_merge` with `GENERATED ALWAYS` columns
CREATE TABLE tm_gen_col_target (
    id int,
    value int,
    value_x2 int GENERATED ALWAYS AS (value * 2) STORED,
    valid_from date,
    valid_until date
);
SELECT sql_saga.add_era('tm_gen_col_target', 'valid_from', 'valid_until');
SELECT sql_saga.add_unique_key('tm_gen_col_target', ARRAY['id']);

CREATE TEMP TABLE tm_gen_col_source (
    row_id int,
    id int,
    value int,
    valid_from date,
    valid_until date
);
INSERT INTO tm_gen_col_source VALUES (1, 1, 100, '2024-01-01', 'infinity');

CALL sql_saga.temporal_merge(
    p_target_table      := 'tm_gen_col_target'::regclass,
    p_source_table      := 'tm_gen_col_source'::regclass,
    p_id_columns        := ARRAY['id']::text[],
    p_ephemeral_columns := '{}'::text[],
    p_mode              := 'insert_only'::sql_saga.temporal_merge_mode
);

-- Verify that the generated column was computed correctly.
\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES (1, 100, 200, '2024-01-01'::date, 'infinity'::date))
    AS t(id, value, value_x2, valid_from, valid_until);
\echo '--- Orchestrator: Actual Final State ---'
TABLE tm_gen_col_target;
ROLLBACK TO SAVEPOINT scenario_6;

SAVEPOINT scenario_8;
-- Scenario 8: Test `temporal_merge` with existing target data and a new source entity.
-- This reproduces the bug where the planner incorrectly mixes entities.
CREATE TABLE tm_mix_bug_target (id int, value text, valid_from date, valid_until date);
SELECT sql_saga.add_era('tm_mix_bug_target', 'valid_from', 'valid_until');
SELECT sql_saga.add_unique_key('tm_mix_bug_target', ARRAY['id']);
INSERT INTO tm_mix_bug_target VALUES (1, 'existing', '2023-01-01', 'infinity');

CREATE TEMP TABLE tm_mix_bug_source (row_id int, id int, value text, valid_from date, valid_until date);
INSERT INTO tm_mix_bug_source VALUES (1, 2, 'new', '2024-01-01', 'infinity');

CALL sql_saga.temporal_merge(
    p_target_table      := 'tm_mix_bug_target'::regclass,
    p_source_table      := 'tm_mix_bug_source'::regclass,
    p_id_columns        := ARRAY['id']::text[],
    p_ephemeral_columns := '{}'::text[],
    p_mode              := 'upsert_replace'::sql_saga.temporal_merge_mode
);

-- Verify the plan does not contain chaotic operations.
\echo '--- Orchestrator: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{1}'::int[], 'INSERT'::sql_saga.planner_action, '{"id": 2}'::jsonb)
) AS t(plan_op_seq, source_row_ids, operation, entity_ids);
\echo '--- Orchestrator: Actual Plan ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids
FROM __temp_last_sql_saga_temporal_merge_plan
ORDER BY plan_op_seq;

-- Verify final state is correct.
\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (1, 'existing', '2023-01-01'::date, 'infinity'::date),
    (2, 'new', '2024-01-01'::date, 'infinity'::date)
) AS t(id, value, valid_from, valid_until);
\echo '--- Orchestrator: Actual Final State ---'
TABLE tm_mix_bug_target ORDER BY id;
ROLLBACK TO SAVEPOINT scenario_8;

SAVEPOINT scenario_9;
-- Scenario 9: Test that `temporal_merge` fails gracefully if p_id_columns is empty or NULL.
CREATE TABLE tm_bad_params_target (id int, value text, valid_from date, valid_until date);
SELECT sql_saga.add_era('tm_bad_params_target', 'valid_from', 'valid_until');
CREATE TEMP TABLE tm_bad_params_source (row_id int, id int, value text, valid_from date, valid_until date);

-- Test with NULL p_id_columns
DO $$
BEGIN
    CALL sql_saga.temporal_merge(
        p_target_table      := 'tm_bad_params_target'::regclass,
        p_source_table      := 'tm_bad_params_source'::regclass,
        p_id_columns        := NULL,
        p_ephemeral_columns := '{}'::text[],
        p_mode              := 'upsert_replace'::sql_saga.temporal_merge_mode
    );
    RAISE EXCEPTION 'temporal_merge should have failed for NULL p_id_columns';
EXCEPTION WHEN others THEN
    RAISE NOTICE 'Caught expected error for NULL p_id_columns: %', SQLERRM;
END;
$$;

-- Test with empty p_id_columns
DO $$
BEGIN
    CALL sql_saga.temporal_merge(
        p_target_table      := 'tm_bad_params_target'::regclass,
        p_source_table      := 'tm_bad_params_source'::regclass,
        p_id_columns        := ARRAY[]::TEXT[],
        p_ephemeral_columns := '{}'::text[],
        p_mode              := 'upsert_replace'::sql_saga.temporal_merge_mode
    );
    RAISE EXCEPTION 'temporal_merge should have failed for empty p_id_columns';
EXCEPTION WHEN others THEN
    RAISE NOTICE 'Caught expected error for empty p_id_columns: %', SQLERRM;
END;
$$;
ROLLBACK TO SAVEPOINT scenario_9;

SAVEPOINT scenario_10;
-- Scenario 10: Test that `temporal_merge` fails if non-existent column names are provided.
CREATE TABLE tm_bad_cols_target (id int, value text, valid_from date, valid_until date);
SELECT sql_saga.add_era('tm_bad_cols_target', 'valid_from', 'valid_until');
CREATE TEMP TABLE tm_bad_cols_source (real_row_id int, id int, value text, valid_from date, valid_until date, real_founding_id int);

-- Test with non-existent p_source_row_id_column
DO $$
BEGIN
    CALL sql_saga.temporal_merge(
        p_target_table      := 'tm_bad_cols_target'::regclass,
        p_source_table      := 'tm_bad_cols_source'::regclass,
        p_id_columns        := ARRAY['id'],
        p_source_row_id_column := 'non_existent_row_id'::name
    );
    RAISE EXCEPTION 'temporal_merge should have failed for non-existent p_source_row_id_column';
EXCEPTION WHEN others THEN
    RAISE NOTICE 'Caught expected error for non-existent p_source_row_id_column: %', SQLERRM;
END;
$$;

-- Test with non-existent p_founding_id_column
DO $$
BEGIN
    CALL sql_saga.temporal_merge(
        p_target_table      := 'tm_bad_cols_target'::regclass,
        p_source_table      := 'tm_bad_cols_source'::regclass,
        p_id_columns        := ARRAY['id'],
        p_source_row_id_column := 'real_row_id',
        p_founding_id_column := 'non_existent_founding_id'::name
    );
    RAISE EXCEPTION 'temporal_merge should have failed for non-existent p_founding_id_column';
EXCEPTION WHEN others THEN
    RAISE NOTICE 'Caught expected error for non-existent p_founding_id_column: %', SQLERRM;
END;
$$;
ROLLBACK TO SAVEPOINT scenario_10;

SAVEPOINT scenario_11;
-- Scenario 11: Test using a non-default p_source_row_id_column name.
CREATE TABLE tm_custom_rowid_target (id int, value text, valid_from date, valid_until date);
SELECT sql_saga.add_era('tm_custom_rowid_target', 'valid_from', 'valid_until');
SELECT sql_saga.add_unique_key('tm_custom_rowid_target', ARRAY['id']);

CREATE TEMP TABLE tm_custom_rowid_source (
    source_pk int, -- Custom row identifier
    id int,
    value text,
    valid_from date,
    valid_until date
);
INSERT INTO tm_custom_rowid_source VALUES (101, 1, 'A', '2024-01-01', 'infinity');

CALL sql_saga.temporal_merge(
    p_target_table      := 'tm_custom_rowid_target'::regclass,
    p_source_table      := 'tm_custom_rowid_source'::regclass,
    p_id_columns        := ARRAY['id'],
    p_ephemeral_columns := '{}'::text[],
    p_source_row_id_column := 'source_pk'::name
);

-- Verify that the merge was successful and feedback works.
\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES (1, 'A', '2024-01-01'::date, 'infinity'::date)) AS t(id, value, valid_from, valid_until);
\echo '--- Orchestrator: Actual Final State ---'
TABLE tm_custom_rowid_target;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (101, '[{"id": 1}]'::jsonb, 'APPLIED'::sql_saga.temporal_merge_status))
    AS t(source_row_id, target_entity_ids, status);
\echo '--- Orchestrator: Actual Feedback ---'
SELECT source_row_id, target_entity_ids, status FROM __temp_last_sql_saga_temporal_merge WHERE source_row_id = 101;
ROLLBACK TO SAVEPOINT scenario_11;

SAVEPOINT scenario_12;
-- Scenario 12: Test that `temporal_merge` fails if the default p_source_row_id_column ('row_id') does not exist.
CREATE TABLE tm_no_rowid_target (id int, value text, valid_from date, valid_until date);
SELECT sql_saga.add_era('tm_no_rowid_target', 'valid_from', 'valid_until');
CREATE TEMP TABLE tm_no_rowid_source (some_other_pk int, id int, value text, valid_from date, valid_until date);

DO $$
BEGIN
    CALL sql_saga.temporal_merge(
        p_target_table      := 'tm_no_rowid_target'::regclass,
        p_source_table      := 'tm_no_rowid_source'::regclass,
        p_id_columns        := ARRAY['id'],
        p_ephemeral_columns := '{}'::text[]
        -- p_source_row_id_column is deliberately omitted to test the default
    );
    RAISE EXCEPTION 'temporal_merge should have failed for missing default row_id column';
EXCEPTION WHEN others THEN
    RAISE NOTICE 'Caught expected error for missing default row_id column: %', SQLERRM;
END;
$$;
ROLLBACK TO SAVEPOINT scenario_12;

SAVEPOINT scenario_13;
-- Scenario 13: Test `temporal_merge` with non-standard column and era names.
CREATE TABLE tm_weird_names (
    unit_pk int,
    some_value text,
    period_begins date,
    period_ends date
);
SELECT sql_saga.add_era('tm_weird_names', 'period_begins', 'period_ends', 'timeline');
SELECT sql_saga.add_unique_key('tm_weird_names', ARRAY['unit_pk'], 'timeline');

CREATE TEMP TABLE tm_weird_names_source (
    source_op_id int,
    unit_pk int,
    some_value text,
    period_begins date,
    period_ends date
);
INSERT INTO tm_weird_names_source VALUES (1, 1, 'X', '2024-01-01', 'infinity');

CALL sql_saga.temporal_merge(
    p_target_table      := 'tm_weird_names'::regclass,
    p_source_table      := 'tm_weird_names_source'::regclass,
    p_id_columns        := ARRAY['unit_pk'],
    p_ephemeral_columns := '{}'::text[],
    p_era_name          := 'timeline'::name,
    p_source_row_id_column := 'source_op_id'::name
);

-- Verify merge was successful
\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES (1, 'X', '2024-01-01'::date, 'infinity'::date)) AS t(unit_pk, some_value, period_begins, period_ends);
\echo '--- Orchestrator: Actual Final State ---'
TABLE tm_weird_names;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (1, '[{"unit_pk": 1}]'::jsonb, 'APPLIED'::sql_saga.temporal_merge_status))
    AS t(source_row_id, target_entity_ids, status);
\echo '--- Orchestrator: Actual Feedback ---'
SELECT source_row_id, target_entity_ids, status FROM __temp_last_sql_saga_temporal_merge;
ROLLBACK TO SAVEPOINT scenario_13;

SAVEPOINT scenario_14;
-- Scenario 14: Test `temporal_merge` with a target table that has no data columns.
CREATE TABLE tm_no_data_cols_target (
    id int,
    valid_from date,
    valid_until date
);
SELECT sql_saga.add_era('tm_no_data_cols_target', 'valid_from', 'valid_until');
SELECT sql_saga.add_unique_key('tm_no_data_cols_target', ARRAY['id']);

CREATE TEMP TABLE tm_no_data_cols_source (
    row_id int,
    id int,
    valid_from date,
    valid_until date
);
INSERT INTO tm_no_data_cols_source VALUES (1, 1, '2024-01-01', 'infinity');

CALL sql_saga.temporal_merge(
    p_target_table      := 'tm_no_data_cols_target'::regclass,
    p_source_table      := 'tm_no_data_cols_source'::regclass,
    p_id_columns        := ARRAY['id'],
    p_ephemeral_columns := '{}'::text[],
    p_mode              := 'insert_only'
);

-- Verify merge was successful and feedback is correct
\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES (1, '2024-01-01'::date, 'infinity'::date)) AS t(id, valid_from, valid_until);
\echo '--- Orchestrator: Actual Final State ---'
TABLE tm_no_data_cols_target;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (1, '[{"id": 1}]'::jsonb, 'APPLIED'::sql_saga.temporal_merge_status))
    AS t(source_row_id, target_entity_ids, status);
\echo '--- Orchestrator: Actual Feedback ---'
SELECT source_row_id, target_entity_ids, status FROM __temp_last_sql_saga_temporal_merge;
ROLLBACK TO SAVEPOINT scenario_14;

SAVEPOINT scenario_15;
-- Scenario 15: Test `temporal_merge` with extra columns in the source table.
-- These columns should be ignored by the procedure.
CREATE TABLE tm_extra_cols_target (id int, value text, valid_from date, valid_until date);
SELECT sql_saga.add_era('tm_extra_cols_target', 'valid_from', 'valid_until');
SELECT sql_saga.add_unique_key('tm_extra_cols_target', ARRAY['id']);

CREATE TEMP TABLE tm_extra_cols_source (
    row_id int,
    id int,
    value text,
    extra_col_1 text, -- This should be ignored
    valid_from date,
    valid_until date,
    extra_col_2 int   -- This should also be ignored
);
INSERT INTO tm_extra_cols_source VALUES (1, 1, 'A', 'ignore me', '2024-01-01', 'infinity', 999);

CALL sql_saga.temporal_merge(
    p_target_table      := 'tm_extra_cols_target'::regclass,
    p_source_table      := 'tm_extra_cols_source'::regclass,
    p_id_columns        := ARRAY['id'],
    p_ephemeral_columns := '{}'::text[],
    p_mode              := 'insert_only'
);

-- Verify merge was successful
\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES (1, 'A', '2024-01-01'::date, 'infinity'::date)) AS t(id, value, valid_from, valid_until);
\echo '--- Orchestrator: Actual Final State ---'
TABLE tm_extra_cols_target;
ROLLBACK TO SAVEPOINT scenario_15;

SAVEPOINT scenario_16;
-- Scenario 16: Test `p_founding_id_column` for creating a new entity from multiple source rows.
CREATE TABLE tm_founding_target (
    id serial primary key,
    entity_ident text,
    value text,
    valid_from date,
    valid_until date
);
SELECT sql_saga.add_era('tm_founding_target', 'valid_from', 'valid_until');
SELECT sql_saga.add_unique_key('tm_founding_target', ARRAY['entity_ident']);

CREATE TEMP TABLE tm_founding_source (
    row_id int,
    founding_group_id int, -- This is the founding_id
    entity_ident text,
    value text,
    valid_from date,
    valid_until date
);
INSERT INTO tm_founding_source VALUES
(1, 101, 'NEW_ENTITY_1', 'A', '2024-01-01', '2024-06-01'),
(2, 101, 'NEW_ENTITY_1', 'B', '2024-06-01', 'infinity');

CALL sql_saga.temporal_merge(
    p_target_table      := 'tm_founding_target'::regclass,
    p_source_table      := 'tm_founding_source'::regclass,
    p_id_columns        := ARRAY['entity_ident'],
    p_ephemeral_columns := '{}'::text[],
    p_mode              := 'insert_only',
    p_founding_id_column := 'founding_group_id'::name
);

-- Verify that a single new entity was created with two historical slices.
\echo '--- Orchestrator: Expected Final State (A single entity with a consistent surrogate key) ---'
SELECT * FROM (VALUES
    (1, 'NEW_ENTITY_1', 'A', '2024-01-01'::date, '2024-06-01'::date),
    (1, 'NEW_ENTITY_1', 'B', '2024-06-01'::date, 'infinity'::date)
) AS t (id, entity_ident, value, valid_from, valid_until);

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, entity_ident, value, valid_from, valid_until
FROM tm_founding_target
ORDER BY valid_from;

-- Verify that both source rows were associated with the same new entity.
\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES
    (1, '[{"id": 1, "entity_ident": "NEW_ENTITY_1"}]'::jsonb),
    (2, '[{"id": 1, "entity_ident": "NEW_ENTITY_1"}]'::jsonb)
) AS t (source_row_id, target_entity_ids);

\echo '--- Orchestrator: Actual Feedback ---'
SELECT source_row_id, target_entity_ids
FROM __temp_last_sql_saga_temporal_merge
ORDER BY source_row_id;
ROLLBACK TO SAVEPOINT scenario_16;


ROLLBACK;
\i sql/include/test_teardown.sql

