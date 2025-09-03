\i sql/include/test_setup.sql

BEGIN;
\echo '----------------------------------------------------------------------------'
\echo 'Test Suite: `sql_saga.temporal_merge` Mode Behaviors'
\echo 'Description:'
\echo '  This test suite verifies the distinct behavior of each `temporal_merge_mode`.'
\echo '  It focuses on the "happy path" for each mode to provide clear, readable'
\echo '  examples of their intended use.'
\echo '----------------------------------------------------------------------------'

SET client_min_messages TO WARNING;
CREATE SCHEMA tmm; -- Temporal Merge Modes

-- Target table for all scenarios
CREATE TABLE tmm.target (id int, valid_from date, valid_until date, value text);
SELECT sql_saga.add_era('tmm.target', 'valid_from', 'valid_until');

-- Helper to reset state
CREATE PROCEDURE tmm.reset_target() LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE tmm.target;
    INSERT INTO tmm.target VALUES
        (1, '2024-01-01', '2025-01-01', 'Original A'),
        (2, '2024-01-01', '2025-01-01', 'Original B');
END;
$$;

--------------------------------------------------------------------------------
\echo 'Scenario 1: `upsert_patch` (Update existing, Insert new)'
--------------------------------------------------------------------------------
CALL tmm.reset_target();
CREATE TEMP TABLE temp_source_1 (row_id int, id int, valid_from date, valid_until date, value text);
INSERT INTO temp_source_1 VALUES
    (101, 1, '2024-06-01', '2024-09-01', 'Patched A'), -- Update entity 1
    (102, 3, '2024-01-01', '2025-01-01', 'New C');     -- Insert entity 3

\echo '--- Target: Initial State ---'
SELECT * FROM tmm.target ORDER BY id, valid_from;
\echo '--- Source: Data to merge ---'
SELECT * FROM temp_source_1 ORDER BY row_id;

CALL sql_saga.temporal_merge('tmm.target'::regclass, 'temp_source_1'::regclass, '{id}'::text[], '{}'::text[], 'upsert_patch'::sql_saga.temporal_merge_mode, 'valid');

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{101}'::INT[], 'UPDATE'::sql_saga.planner_action, '{"id": 1}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2024-06-01'::DATE, '{"value": "Original A"}'::JSONB, 'starts'::sql_saga.allen_interval_relation),
    (2, '{101}'::INT[], 'INSERT'::sql_saga.planner_action, '{"id": 1}'::JSONB, NULL::DATE,         '2024-06-01'::DATE, '2024-09-01'::DATE, '{"value": "Patched A"}'::JSONB,  NULL::sql_saga.allen_interval_relation),
    (3, '{101}'::INT[], 'INSERT'::sql_saga.planner_action, '{"id": 1}'::JSONB, NULL::DATE,         '2024-09-01'::DATE, '2025-01-01'::DATE, '{"value": "Original A"}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (4, '{102}'::INT[], 'INSERT'::sql_saga.planner_action, '{"id": 3}'::JSONB, NULL::DATE,         '2024-01-01'::DATE, '2025-01-01'::DATE, '{"value": "New C"}'::JSONB,     NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation) ORDER BY plan_op_seq;
\echo '--- Planner: Actual Plan ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES
    (101, '[{"id": 1}]'::jsonb, 'APPLIED'::sql_saga.temporal_merge_status),
    (102, '[{"id": 3}]'::jsonb, 'APPLIED'::sql_saga.temporal_merge_status)
) t(source_row_id, target_entity_ids, status) ORDER BY source_row_id;
\echo '--- Orchestrator: Actual Feedback ---'
SELECT source_row_id, target_entity_ids, status FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id;

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{101}'::INT[], 'UPDATE'::sql_saga.planner_action, '{"id": 1}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2024-06-01'::DATE, '{"value": "Original A"}'::JSONB, 'starts'::sql_saga.allen_interval_relation),
    (2, '{101}'::INT[], 'INSERT'::sql_saga.planner_action, '{"id": 1}'::JSONB, NULL::DATE,         '2024-06-01'::DATE, '2024-09-01'::DATE, '{"value": "Patched A"}'::JSONB,  NULL::sql_saga.allen_interval_relation),
    (3, '{101}'::INT[], 'INSERT'::sql_saga.planner_action, '{"id": 1}'::JSONB, NULL::DATE,         '2024-09-01'::DATE, '2025-01-01'::DATE, '{"value": "Original A"}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (4, '{102}'::INT[], 'INSERT'::sql_saga.planner_action, '{"id": 3}'::JSONB, NULL::DATE,         '2024-01-01'::DATE, '2025-01-01'::DATE, '{"value": "New C"}'::JSONB,     NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation) ORDER BY plan_op_seq;
\echo '--- Planner: Actual Plan ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (1, '2024-01-01'::date, '2024-06-01'::date, 'Original A'),
    (1, '2024-06-01'::date, '2024-09-01'::date, 'Patched A'),
    (1, '2024-09-01'::date, '2025-01-01'::date, 'Original A'),
    (2, '2024-01-01'::date, '2025-01-01'::date, 'Original B'),
    (3, '2024-01-01'::date, '2025-01-01'::date, 'New C')
) t(id, valid_from, valid_until, value) ORDER BY id, valid_from;

\echo '--- Orchestrator: Actual Final State ---'
SELECT * FROM tmm.target ORDER BY id, valid_from;
DROP TABLE temp_source_1;

--------------------------------------------------------------------------------
\echo 'Scenario 2: `upsert_replace` (Update existing, Insert new, NULLs overwrite)'
--------------------------------------------------------------------------------
CALL tmm.reset_target();
CREATE TEMP TABLE temp_source_2 (row_id int, id int, valid_from date, valid_until date, value text);
INSERT INTO temp_source_2 VALUES
    (201, 1, '2024-06-01', '2024-09-01', NULL),     -- Replace entity 1 with NULL
    (202, 3, '2024-01-01', '2025-01-01', 'New C');  -- Insert entity 3

\echo '--- Target: Initial State ---'
SELECT * FROM tmm.target ORDER BY id, valid_from;
\echo '--- Source: Data to merge ---'
SELECT * FROM temp_source_2 ORDER BY row_id;

CALL sql_saga.temporal_merge('tmm.target'::regclass, 'temp_source_2'::regclass, '{id}'::text[], '{}'::text[], 'upsert_replace'::sql_saga.temporal_merge_mode, 'valid');

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{201}'::INT[], 'UPDATE'::sql_saga.planner_action, '{"id": 1}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2024-06-01'::DATE, '{"value": "Original A"}'::JSONB, 'starts'::sql_saga.allen_interval_relation),
    (2, '{201}'::INT[], 'INSERT'::sql_saga.planner_action, '{"id": 1}'::JSONB, NULL::DATE,         '2024-06-01'::DATE, '2024-09-01'::DATE, '{"value": null}'::JSONB,        NULL::sql_saga.allen_interval_relation),
    (3, '{201}'::INT[], 'INSERT'::sql_saga.planner_action, '{"id": 1}'::JSONB, NULL::DATE,         '2024-09-01'::DATE, '2025-01-01'::DATE, '{"value": "Original A"}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (4, '{202}'::INT[], 'INSERT'::sql_saga.planner_action, '{"id": 3}'::JSONB, NULL::DATE,         '2024-01-01'::DATE, '2025-01-01'::DATE, '{"value": "New C"}'::JSONB,     NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation) ORDER BY plan_op_seq;
\echo '--- Planner: Actual Plan ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES
    (201, '[{"id": 1}]'::jsonb, 'APPLIED'::sql_saga.temporal_merge_status),
    (202, '[{"id": 3}]'::jsonb, 'APPLIED'::sql_saga.temporal_merge_status)
) t(source_row_id, target_entity_ids, status) ORDER BY source_row_id;
\echo '--- Orchestrator: Actual Feedback ---'
SELECT source_row_id, target_entity_ids, status FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (1, '2024-01-01'::date, '2024-06-01'::date, 'Original A'),
    (1, '2024-06-01'::date, '2024-09-01'::date, NULL::text),
    (1, '2024-09-01'::date, '2025-01-01'::date, 'Original A'),
    (2, '2024-01-01'::date, '2025-01-01'::date, 'Original B'),
    (3, '2024-01-01'::date, '2025-01-01'::date, 'New C')
) t(id, valid_from, valid_until, value) ORDER BY id, valid_from;

\echo '--- Orchestrator: Actual Final State ---'
SELECT * FROM tmm.target ORDER BY id, valid_from;
DROP TABLE temp_source_2;

--------------------------------------------------------------------------------
\echo 'Scenario 3: `patch_only` (Update existing, IGNORE new)'
--------------------------------------------------------------------------------
CALL tmm.reset_target();
CREATE TEMP TABLE temp_source_3 (row_id int, id int, valid_from date, valid_until date, value text);
INSERT INTO temp_source_3 VALUES
    (301, 1, '2024-06-01', '2024-09-01', 'Patched A'), -- Update entity 1
    (302, 3, '2024-01-01', '2025-01-01', 'New C');     -- IGNORED: Insert entity 3

\echo '--- Target: Initial State ---'
SELECT * FROM tmm.target ORDER BY id, valid_from;
\echo '--- Source: Data to merge ---'
SELECT * FROM temp_source_3 ORDER BY row_id;

CALL sql_saga.temporal_merge('tmm.target'::regclass, 'temp_source_3'::regclass, '{id}'::text[], '{}'::text[], 'patch_only'::sql_saga.temporal_merge_mode, 'valid');

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES
    (301, '[{"id": 1}]'::jsonb, 'APPLIED'::sql_saga.temporal_merge_status),
    (302, '[]'::jsonb, 'TARGET_NOT_FOUND'::sql_saga.temporal_merge_status)
) t(source_row_id, target_entity_ids, status) ORDER BY source_row_id;
\echo '--- Orchestrator: Actual Feedback ---'
SELECT source_row_id, target_entity_ids, status FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id;

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{301}'::INT[], 'UPDATE'::sql_saga.planner_action, '{"id": 1}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2024-06-01'::DATE, '{"value": "Original A"}'::JSONB, 'starts'::sql_saga.allen_interval_relation),
    (2, '{301}'::INT[], 'INSERT'::sql_saga.planner_action, '{"id": 1}'::JSONB, NULL::DATE,         '2024-06-01'::DATE, '2024-09-01'::DATE, '{"value": "Patched A"}'::JSONB,  NULL::sql_saga.allen_interval_relation),
    (3, '{301}'::INT[], 'INSERT'::sql_saga.planner_action, '{"id": 1}'::JSONB, NULL::DATE,         '2024-09-01'::DATE, '2025-01-01'::DATE, '{"value": "Original A"}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);
\echo '--- Planner: Actual Plan ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (1, '2024-01-01'::date, '2024-06-01'::date, 'Original A'),
    (1, '2024-06-01'::date, '2024-09-01'::date, 'Patched A'),
    (1, '2024-09-01'::date, '2025-01-01'::date, 'Original A'),
    (2, '2024-01-01'::date, '2025-01-01'::date, 'Original B')
) t(id, valid_from, valid_until, value) ORDER BY id, valid_from;

\echo '--- Orchestrator: Actual Final State ---'
SELECT * FROM tmm.target ORDER BY id, valid_from;
DROP TABLE temp_source_3;

--------------------------------------------------------------------------------
\echo 'Scenario 4: `replace_only` (Update existing, IGNORE new, NULLs overwrite)'
--------------------------------------------------------------------------------
CALL tmm.reset_target();
CREATE TEMP TABLE temp_source_4 (row_id int, id int, valid_from date, valid_until date, value text);
INSERT INTO temp_source_4 VALUES
    (401, 1, '2024-06-01', '2024-09-01', NULL),     -- Replace entity 1
    (402, 3, '2024-01-01', '2025-01-01', 'New C');  -- IGNORED: Insert entity 3

\echo '--- Target: Initial State ---'
SELECT * FROM tmm.target ORDER BY id, valid_from;
\echo '--- Source: Data to merge ---'
SELECT * FROM temp_source_4 ORDER BY row_id;

CALL sql_saga.temporal_merge('tmm.target'::regclass, 'temp_source_4'::regclass, '{id}'::text[], '{}'::text[], 'replace_only'::sql_saga.temporal_merge_mode, 'valid');

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES
    (401, '[{"id": 1}]'::jsonb, 'APPLIED'::sql_saga.temporal_merge_status),
    (402, '[]'::jsonb, 'TARGET_NOT_FOUND'::sql_saga.temporal_merge_status)
) t(source_row_id, target_entity_ids, status) ORDER BY source_row_id;
\echo '--- Orchestrator: Actual Feedback ---'
SELECT source_row_id, target_entity_ids, status FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id;

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{401}'::INT[], 'UPDATE'::sql_saga.planner_action, '{"id": 1}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2024-06-01'::DATE, '{"value": "Original A"}'::JSONB, 'starts'::sql_saga.allen_interval_relation),
    (2, '{401}'::INT[], 'INSERT'::sql_saga.planner_action, '{"id": 1}'::JSONB, NULL::DATE,         '2024-06-01'::DATE, '2024-09-01'::DATE, '{"value": null}'::JSONB,        NULL::sql_saga.allen_interval_relation),
    (3, '{401}'::INT[], 'INSERT'::sql_saga.planner_action, '{"id": 1}'::JSONB, NULL::DATE,         '2024-09-01'::DATE, '2025-01-01'::DATE, '{"value": "Original A"}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);
\echo '--- Planner: Actual Plan ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (1, '2024-01-01'::date, '2024-06-01'::date, 'Original A'),
    (1, '2024-06-01'::date, '2024-09-01'::date, NULL::text),
    (1, '2024-09-01'::date, '2025-01-01'::date, 'Original A'),
    (2, '2024-01-01'::date, '2025-01-01'::date, 'Original B')
) t(id, valid_from, valid_until, value) ORDER BY id, valid_from;

\echo '--- Orchestrator: Actual Final State ---'
SELECT * FROM tmm.target ORDER BY id, valid_from;
DROP TABLE temp_source_4;

--------------------------------------------------------------------------------
\echo 'Scenario 5: `insert_only` (IGNORE existing, Insert new)'
--------------------------------------------------------------------------------
CALL tmm.reset_target();
CREATE TEMP TABLE temp_source_5 (row_id int, id int, valid_from date, valid_until date, value text);
INSERT INTO temp_source_5 VALUES
    (501, 1, '2024-06-01', '2024-09-01', 'Patched A'), -- IGNORED: Update entity 1
    (502, 3, '2024-01-01', '2025-01-01', 'New C');     -- Insert entity 3

\echo '--- Target: Initial State ---'
SELECT * FROM tmm.target ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_5 ORDER BY row_id;

CALL sql_saga.temporal_merge('tmm.target'::regclass, 'temp_source_5'::regclass, '{id}'::text[], '{}'::text[], 'insert_only'::sql_saga.temporal_merge_mode, 'valid');

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, NULL::INT[], 'IDENTICAL'::sql_saga.planner_action, '{"id": 1}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"value": "Original A"}'::JSONB, 'equals'::sql_saga.allen_interval_relation),
    (2, '{502}'::INT[], 'INSERT'::sql_saga.planner_action, '{"id": 3}'::JSONB, NULL::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"value": "New C"}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation) ORDER BY plan_op_seq;
\echo '--- Planner: Actual Plan ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES
    (501, '[]'::jsonb, 'SKIPPED'::sql_saga.temporal_merge_status),
    (502, '[{"id": 3}]'::jsonb, 'APPLIED'::sql_saga.temporal_merge_status)
) t(source_row_id, target_entity_ids, status) ORDER BY source_row_id;
\echo '--- Orchestrator: Actual Feedback ---'
SELECT source_row_id, target_entity_ids, status FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (1, '2024-01-01'::date, '2025-01-01'::date, 'Original A'),
    (2, '2024-01-01'::date, '2025-01-01'::date, 'Original B'),
    (3, '2024-01-01'::date, '2025-01-01'::date, 'New C')
) t(id, valid_from, valid_until, value) ORDER BY id, valid_from;

\echo '--- Orchestrator: Actual Final State ---'
SELECT * FROM tmm.target ORDER BY id, valid_from;
DROP TABLE temp_source_5;

--------------------------------------------------------------------------------
\echo 'Scenario 6: `upsert_patch` with identical data (should be SKIPPED)'
--------------------------------------------------------------------------------
CALL tmm.reset_target();
CREATE TEMP TABLE temp_source_6 (row_id int, id int, valid_from date, valid_until date, value text);
INSERT INTO temp_source_6 VALUES
    (601, 1, '2024-01-01', '2025-01-01', 'Original A'); -- Identical data

\echo '--- Target: Initial State ---'
SELECT * FROM tmm.target ORDER BY id, valid_from;
\echo '--- Source: Data to merge ---'
SELECT * FROM temp_source_6 ORDER BY row_id;

CALL sql_saga.temporal_merge('tmm.target'::regclass, 'temp_source_6'::regclass, '{id}'::text[], '{}'::text[], 'upsert_patch'::sql_saga.temporal_merge_mode, 'valid');

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES
    (601, '[]'::jsonb, 'SKIPPED'::sql_saga.temporal_merge_status)
) t(source_row_id, target_entity_ids, status) ORDER BY source_row_id;
\echo '--- Orchestrator: Actual Feedback ---'
SELECT source_row_id, target_entity_ids, status FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id;

\echo '--- Planner: Expected Plan (a single IDENTICAL operation) ---'
SELECT * FROM (VALUES
    (1, '{601}'::INT[], 'IDENTICAL'::sql_saga.planner_action, '{"id": 1}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"value": "Original A"}'::JSONB, 'equals'::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);
\echo '--- Planner: Actual Plan ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan;

\echo '--- Orchestrator: Expected Final State (unchanged) ---'
SELECT * FROM (VALUES
    (1, '2024-01-01'::date, '2025-01-01'::date, 'Original A'),
    (2, '2024-01-01'::date, '2025-01-01'::date, 'Original B')
) t(id, valid_from, valid_until, value) ORDER BY id, valid_from;

\echo '--- Orchestrator: Actual Final State ---'
SELECT * FROM tmm.target ORDER BY id, valid_from;
DROP TABLE temp_source_6;

ROLLBACK;
\i sql/include/test_teardown.sql
