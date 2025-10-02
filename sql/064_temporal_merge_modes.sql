\i sql/include/test_setup.sql

BEGIN;
\echo '----------------------------------------------------------------------------'
\echo 'Test Suite: Comprehensive `sql_saga.temporal_merge` Mode Behaviors'
\echo 'Description:'
\echo '  This test suite provides a comprehensive validation of each `temporal_merge_mode`.'
\echo '  It uses a consistent, rich target timeline with gaps and adjacencies, and'
\echo '  a varied source dataset to test pre, post, adjacent, gap, and entry-crossing'
\echo '  scenarios. Each scenario clearly prints the plan, feedback, and final state.'
\echo '----------------------------------------------------------------------------'

SET client_min_messages TO NOTICE;
CREATE SCHEMA tmm; -- Temporal Merge Modes

-- Target table for all scenarios
CREATE TABLE tmm.target (id int, value text, edit_comment text, valid_from date, valid_until date);
SELECT sql_saga.add_era('tmm.target', 'valid_from', 'valid_until');

-- Helper to reset state to a rich timeline
CREATE PROCEDURE tmm.reset_target() LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE tmm.target;
    INSERT INTO tmm.target VALUES
        -- Entity 1 has adjacent slices and a gap
        (1, 'A1', 'Slice 1',           '2024-01-01', '2024-02-01'),
        (1, 'A2', 'Slice 2 (Adjacent)','2024-02-01', '2024-03-01'),
        (1, 'A3', 'Slice 3 (After Gap)','2024-04-01', '2024-05-01'),
        -- Entity 2 is a simple, long-lived entity
        (2, 'B',  'Long-lived',        '2023-01-01', '2026-01-01');
END;
$$;

--------------------------------------------------------------------------------
\echo 'Scenario 1: `MERGE_ENTITY_PATCH`'
\echo 'Use Case: Standard upsert. Patches existing data, preserves timeline gaps, and inserts new entities.'
--------------------------------------------------------------------------------
SAVEPOINT s1;
CALL tmm.reset_target();
CREATE TEMP TABLE source_1 (row_id int, id int, value text, edit_comment text, valid_from date, valid_until date) ON COMMIT DROP;
INSERT INTO source_1 VALUES
    -- 1.1: Surgical patch inside slice 2
    (101, 1, 'A2-patch', 'Patch inside S2', '2024-02-10', '2024-02-20'),
    -- 1.2: Bridge the gap between Slice 2 and 3
    (102, 1, 'A2.5-bridge', 'Bridge the gap', '2024-03-01', '2024-04-01'),
    -- 1.3: Insert a new entity 3
    (103, 3, 'C', 'New entity', '2024-01-01', '2025-01-01');

\echo '--- Target: Initial State ---'
TABLE tmm.target ORDER BY id, valid_from;
\echo '--- Source: Data to merge ---'
TABLE source_1 ORDER BY row_id;

CALL sql_saga.temporal_merge(target_table => 'tmm.target'::regclass, source_table => 'source_1'::regclass, primary_identity_columns => '{id}'::text[], ephemeral_columns => '{edit_comment}'::text[], mode => 'MERGE_ENTITY_PATCH'::sql_saga.temporal_merge_mode, era_name => 'valid');

\echo '--- Planner: Actual Plan ---'
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
\echo '--- Executor: Actual Feedback ---'
TABLE pg_temp.temporal_merge_feedback ORDER BY source_row_id;
\echo '--- Target: Expected Final State ---'
SELECT * FROM (VALUES
    (1, 'A1', 'Slice 1', '2024-01-01'::date, '2024-02-01'::date),
    (1, 'A2', 'Slice 2 (Adjacent)', '2024-02-01'::date, '2024-02-10'::date),
    (1, 'A2-patch', 'Patch inside S2', '2024-02-10'::date, '2024-02-20'::date),
    (1, 'A2', 'Slice 2 (Adjacent)', '2024-02-20'::date, '2024-03-01'::date),
    (1, 'A2.5-bridge', 'Bridge the gap', '2024-03-01'::date, '2024-04-01'::date),
    (1, 'A3', 'Slice 3 (After Gap)', '2024-04-01'::date, '2024-05-01'::date),
    (2, 'B', 'Long-lived', '2023-01-01'::date, '2026-01-01'::date),
    (3, 'C', 'New entity', '2024-01-01'::date, '2025-01-01'::date)
) t(id, value, edit_comment, valid_from, valid_until) ORDER BY id, valid_from;
\echo '--- Target: Final State ---'
TABLE tmm.target ORDER BY id, valid_from;
ROLLBACK TO SAVEPOINT s1;

--------------------------------------------------------------------------------
\echo 'Scenario 2: `MERGE_ENTITY_REPLACE`'
\echo 'Use Case: Upsert with NULL overwrites. Replaces existing data, preserves timeline gaps, inserts new entities.'
--------------------------------------------------------------------------------
SAVEPOINT s2;
CALL tmm.reset_target();
CREATE TEMP TABLE source_2 (row_id int, id int, value text, edit_comment text, valid_from date, valid_until date) ON COMMIT DROP;
INSERT INTO source_2 VALUES
    -- 2.1: Surgical replace inside slice 2
    (201, 1, 'A2-replace', 'Replace inside S2', '2024-02-10', '2024-02-20'),
    -- 2.2: Surgical replace with NULL inside slice 3
    (202, 1, NULL, 'Replace with NULL', '2024-04-10', '2024-04-20'),
    -- 2.3: Insert a new entity 3
    (203, 3, 'C', 'New entity', '2024-01-01', '2025-01-01');

\echo '--- Target: Initial State ---'
TABLE tmm.target ORDER BY id, valid_from;
\echo '--- Source: Data to merge ---'
TABLE source_2 ORDER BY row_id;

CALL sql_saga.temporal_merge(target_table => 'tmm.target'::regclass, source_table => 'source_2'::regclass, primary_identity_columns => '{id}'::text[], ephemeral_columns => '{edit_comment}'::text[], mode => 'MERGE_ENTITY_REPLACE'::sql_saga.temporal_merge_mode, era_name => 'valid');

\echo '--- Planner: Actual Plan ---'
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
\echo '--- Executor: Actual Feedback ---'
TABLE pg_temp.temporal_merge_feedback ORDER BY source_row_id;
\echo '--- Target: Expected Final State ---'
SELECT * FROM (VALUES
    (1, 'A1', 'Slice 1', '2024-01-01'::date, '2024-02-01'::date),
    (1, 'A2', 'Slice 2 (Adjacent)', '2024-02-01'::date, '2024-02-10'::date),
    (1, 'A2-replace', 'Replace inside S2', '2024-02-10'::date, '2024-02-20'::date),
    (1, 'A2', 'Slice 2 (Adjacent)', '2024-02-20'::date, '2024-03-01'::date),
    (1, 'A3', 'Slice 3 (After Gap)', '2024-04-01'::date, '2024-04-10'::date),
    (1, NULL, 'Replace with NULL', '2024-04-10'::date, '2024-04-20'::date),
    (1, 'A3', 'Slice 3 (After Gap)', '2024-04-20'::date, '2024-05-01'::date),
    (2, 'B', 'Long-lived', '2023-01-01'::date, '2026-01-01'::date),
    (3, 'C', 'New entity', '2024-01-01'::date, '2025-01-01'::date)
) t(id, value, edit_comment, valid_from, valid_until) ORDER BY id, valid_from;
\echo '--- Target: Final State ---'
TABLE tmm.target ORDER BY id, valid_from;
ROLLBACK TO SAVEPOINT s2;

--------------------------------------------------------------------------------
\echo 'Scenario 3: `PATCH_FOR_PORTION_OF`'
\echo 'Use Case: Surgical patch on existing entities only; new entities are ignored.'
--------------------------------------------------------------------------------
SAVEPOINT s3;
CALL tmm.reset_target();
CREATE TEMP TABLE source_3 (row_id int, id int, value text, edit_comment text, valid_from date, valid_until date) ON COMMIT DROP;
INSERT INTO source_3 VALUES
    -- 3.1: Surgical patch inside slice 2
    (301, 1, 'A2-patch', 'Patch inside S2', '2024-02-10', '2024-02-20'),
    -- 3.2: Attempt to insert a new entity 3 (should be skipped)
    (302, 3, 'C', 'New entity', '2024-01-01', '2025-01-01');

\echo '--- Target: Initial State ---'
TABLE tmm.target ORDER BY id, valid_from;
\echo '--- Source: Data to merge ---'
TABLE source_3 ORDER BY row_id;

CALL sql_saga.temporal_merge(target_table => 'tmm.target'::regclass, source_table => 'source_3'::regclass, primary_identity_columns => '{id}'::text[], ephemeral_columns => '{edit_comment}'::text[], mode => 'PATCH_FOR_PORTION_OF'::sql_saga.temporal_merge_mode, era_name => 'valid');

\echo '--- Planner: Actual Plan ---'
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
\echo '--- Executor: Actual Feedback ---'
TABLE pg_temp.temporal_merge_feedback ORDER BY source_row_id;
\echo '--- Target: Expected Final State ---'
SELECT * FROM (VALUES
    (1, 'A1', 'Slice 1', '2024-01-01'::date, '2024-02-01'::date),
    (1, 'A2', 'Slice 2 (Adjacent)', '2024-02-01'::date, '2024-02-10'::date),
    (1, 'A2-patch', 'Patch inside S2', '2024-02-10'::date, '2024-02-20'::date),
    (1, 'A2', 'Slice 2 (Adjacent)', '2024-02-20'::date, '2024-03-01'::date),
    (1, 'A3', 'Slice 3 (After Gap)', '2024-04-01'::date, '2024-05-01'::date),
    (2, 'B', 'Long-lived', '2023-01-01'::date, '2026-01-01'::date)
) t(id, value, edit_comment, valid_from, valid_until) ORDER BY id, valid_from;
\echo '--- Target: Final State ---'
TABLE tmm.target ORDER BY id, valid_from;
ROLLBACK TO SAVEPOINT s3;

--------------------------------------------------------------------------------
\echo 'Scenario 4: `REPLACE_FOR_PORTION_OF`'
\echo 'Use Case: Surgical replace on existing entities only; new entities are ignored.'
--------------------------------------------------------------------------------
SAVEPOINT s4;
CALL tmm.reset_target();
CREATE TEMP TABLE source_4 (row_id int, id int, value text, edit_comment text, valid_from date, valid_until date) ON COMMIT DROP;
INSERT INTO source_4 VALUES
    -- 4.1: Surgical replace inside slice 2
    (401, 1, 'A2-replace', 'Replace inside S2', '2024-02-10', '2024-02-20'),
    -- 4.2: Attempt to insert a new entity 3 (should be skipped)
    (402, 3, 'C', 'New entity', '2024-01-01', '2025-01-01');

\echo '--- Target: Initial State ---'
TABLE tmm.target ORDER BY id, valid_from;
\echo '--- Source: Data to merge ---'
TABLE source_4 ORDER BY row_id;

CALL sql_saga.temporal_merge(target_table => 'tmm.target'::regclass, source_table => 'source_4'::regclass, primary_identity_columns => '{id}'::text[], ephemeral_columns => '{edit_comment}'::text[], mode => 'REPLACE_FOR_PORTION_OF'::sql_saga.temporal_merge_mode, era_name => 'valid');

\echo '--- Planner: Actual Plan ---'
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
\echo '--- Executor: Actual Feedback ---'
TABLE pg_temp.temporal_merge_feedback ORDER BY source_row_id;
\echo '--- Target: Expected Final State ---'
SELECT * FROM (VALUES
    (1, 'A1', 'Slice 1', '2024-01-01'::date, '2024-02-01'::date),
    (1, 'A2', 'Slice 2 (Adjacent)', '2024-02-01'::date, '2024-02-10'::date),
    (1, 'A2-replace', 'Replace inside S2', '2024-02-10'::date, '2024-02-20'::date),
    (1, 'A2', 'Slice 2 (Adjacent)', '2024-02-20'::date, '2024-03-01'::date),
    (1, 'A3', 'Slice 3 (After Gap)', '2024-04-01'::date, '2024-05-01'::date),
    (2, 'B', 'Long-lived', '2023-01-01'::date, '2026-01-01'::date)
) t(id, value, edit_comment, valid_from, valid_until) ORDER BY id, valid_from;
\echo '--- Target: Final State ---'
TABLE tmm.target ORDER BY id, valid_from;
ROLLBACK TO SAVEPOINT s4;

--------------------------------------------------------------------------------
\echo 'Scenario 5: `INSERT_NEW_ENTITIES`'
\echo 'Use Case: Insert only new entities; existing entities are ignored.'
--------------------------------------------------------------------------------
SAVEPOINT s5;
CALL tmm.reset_target();
CREATE TEMP TABLE source_5 (row_id int, id int, value text, edit_comment text, valid_from date, valid_until date) ON COMMIT DROP;
INSERT INTO source_5 VALUES
    -- 5.1: Attempt to patch entity 1 (should be skipped)
    (501, 1, 'A2-patch', 'Patch inside S2', '2024-02-10', '2024-02-20'),
    -- 5.2: Insert a new entity 3
    (502, 3, 'C', 'New entity', '2024-01-01', '2025-01-01');

\echo '--- Target: Initial State ---'
TABLE tmm.target ORDER BY id, valid_from;
\echo '--- Source: Data to merge ---'
TABLE source_5 ORDER BY row_id;

CALL sql_saga.temporal_merge(target_table => 'tmm.target'::regclass, source_table => 'source_5'::regclass, primary_identity_columns => '{id}'::text[], ephemeral_columns => '{edit_comment}'::text[], mode => 'INSERT_NEW_ENTITIES'::sql_saga.temporal_merge_mode, era_name => 'valid');

\echo '--- Planner: Actual Plan ---'
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
\echo '--- Executor: Actual Feedback ---'
TABLE pg_temp.temporal_merge_feedback ORDER BY source_row_id;
\echo '--- Target: Expected Final State ---'
SELECT * FROM (VALUES
    (1, 'A1', 'Slice 1', '2024-01-01'::date, '2024-02-01'::date),
    (1, 'A2', 'Slice 2 (Adjacent)', '2024-02-01'::date, '2024-03-01'::date),
    (1, 'A3', 'Slice 3 (After Gap)', '2024-04-01'::date, '2024-05-01'::date),
    (2, 'B', 'Long-lived', '2023-01-01'::date, '2026-01-01'::date),
    (3, 'C', 'New entity', '2024-01-01'::date, '2025-01-01'::date)
) t(id, value, edit_comment, valid_from, valid_until) ORDER BY id, valid_from;
\echo '--- Target: Final State ---'
TABLE tmm.target ORDER BY id, valid_from;
ROLLBACK TO SAVEPOINT s5;

--------------------------------------------------------------------------------
\echo 'Scenario 6: `DELETE_FOR_PORTION_OF`'
\echo 'Use Case: Surgical delete on existing entities only; new entities are ignored.'
--------------------------------------------------------------------------------
SAVEPOINT s6;
CALL tmm.reset_target();
CREATE TEMP TABLE source_6 (row_id int, id int, value text, edit_comment text, valid_from date, valid_until date) ON COMMIT DROP;
INSERT INTO source_6 VALUES
    -- 6.1: Surgical delete inside slice 2
    (601, 1, '__DELETE__', 'Delete inside S2', '2024-02-10', '2024-02-20'),
    -- 6.2: Attempt to insert a new entity 3 (should be skipped)
    (602, 3, 'C', 'New entity', '2024-01-01', '2025-01-01');

\echo '--- Target: Initial State ---'
TABLE tmm.target ORDER BY id, valid_from;
\echo '--- Source: Data to merge ---'
TABLE source_6 ORDER BY row_id;

CALL sql_saga.temporal_merge(target_table => 'tmm.target'::regclass, source_table => 'source_6'::regclass, primary_identity_columns => '{id}'::text[], ephemeral_columns => '{edit_comment}'::text[], mode => 'DELETE_FOR_PORTION_OF'::sql_saga.temporal_merge_mode, era_name => 'valid');

\echo '--- Planner: Actual Plan ---'
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
\echo '--- Executor: Actual Feedback ---'
TABLE pg_temp.temporal_merge_feedback ORDER BY source_row_id;
\echo '--- Target: Expected Final State ---'
SELECT * FROM (VALUES
    (1, 'A1', 'Slice 1', '2024-01-01'::date, '2024-02-01'::date),
    (1, 'A2', 'Slice 2 (Adjacent)', '2024-02-01'::date, '2024-02-10'::date),
    (1, 'A2', 'Slice 2 (Adjacent)', '2024-02-20'::date, '2024-03-01'::date),
    (1, 'A3', 'Slice 3 (After Gap)', '2024-04-01'::date, '2024-05-01'::date),
    (2, 'B', 'Long-lived', '2023-01-01'::date, '2026-01-01'::date)
) t(id, value, edit_comment, valid_from, valid_until) ORDER BY id, valid_from;
\echo '--- Target: Final State ---'
TABLE tmm.target ORDER BY id, valid_from;
ROLLBACK TO SAVEPOINT s6;

--------------------------------------------------------------------------------
\echo 'Scenario 7: `MERGE_ENTITY_UPSERT`'
\echo 'Use Case: The new workhorse. Partial update on existing, insert for new. NULLs are explicit.'
--------------------------------------------------------------------------------
SAVEPOINT s7;
CALL tmm.reset_target();
CREATE TEMP TABLE source_7 (row_id int, id int, value text, valid_from date, valid_until date) ON COMMIT DROP;
INSERT INTO source_7 VALUES
    -- 7.1: Overwrite slice 2 with NULL, but don't provide edit_comment (it should be preserved)
    (701, 1, NULL, '2024-02-10', '2024-02-20'),
    -- 7.2: Insert a new entity 3
    (702, 3, 'C', '2024-01-01', '2025-01-01');

\echo '--- Target: Initial State ---'
TABLE tmm.target ORDER BY id, valid_from;
\echo '--- Source: Data to merge ---'
TABLE source_7 ORDER BY row_id;

CALL sql_saga.temporal_merge(target_table => 'tmm.target'::regclass, source_table => 'source_7'::regclass, primary_identity_columns => '{id}'::text[], ephemeral_columns => '{edit_comment}'::text[], mode => 'MERGE_ENTITY_UPSERT'::sql_saga.temporal_merge_mode, era_name => 'valid');

\echo '--- Planner: Actual Plan ---'
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
\echo '--- Executor: Actual Feedback ---'
TABLE pg_temp.temporal_merge_feedback ORDER BY source_row_id;
\echo '--- Target: Expected Final State ---'
SELECT * FROM (VALUES
    (1, 'A1', 'Slice 1', '2024-01-01'::date, '2024-02-01'::date),
    (1, 'A2', 'Slice 2 (Adjacent)', '2024-02-01'::date, '2024-02-10'::date),
    (1, NULL, 'Slice 2 (Adjacent)', '2024-02-10'::date, '2024-02-20'::date),
    (1, 'A2', 'Slice 2 (Adjacent)', '2024-02-20'::date, '2024-03-01'::date),
    (1, 'A3', 'Slice 3 (After Gap)', '2024-04-01'::date, '2024-05-01'::date),
    (2, 'B',  'Long-lived',        '2023-01-01'::date, '2026-01-01'::date),
    (3, 'C', NULL, '2024-01-01'::date, '2025-01-01'::date)
) t(id, value, edit_comment, valid_from, valid_until) ORDER BY id, valid_from;
\echo '--- Target: Final State ---'
TABLE tmm.target ORDER BY id, valid_from;
ROLLBACK TO SAVEPOINT s7;

--------------------------------------------------------------------------------
\echo 'Scenario 8: `UPDATE_FOR_PORTION_OF`'
\echo 'Use Case: Surgical partial update on existing entities only; new entities are ignored.'
--------------------------------------------------------------------------------
SAVEPOINT s8;
CALL tmm.reset_target();
CREATE TEMP TABLE source_8 (row_id int, id int, value text, valid_from date, valid_until date) ON COMMIT DROP;
INSERT INTO source_8 VALUES
    -- 8.1: Surgical update on slice 2. edit_comment is not provided and should be preserved.
    (801, 1, 'A2-updated', '2024-02-10', '2024-02-20'),
    -- 8.2: Attempt to insert a new entity 3 (should be skipped)
    (802, 3, 'C', '2024-01-01', '2025-01-01');

\echo '--- Target: Initial State ---'
TABLE tmm.target ORDER BY id, valid_from;
\echo '--- Source: Data to merge ---'
TABLE source_8 ORDER BY row_id;

CALL sql_saga.temporal_merge(target_table => 'tmm.target'::regclass, source_table => 'source_8'::regclass, primary_identity_columns => '{id}'::text[], ephemeral_columns => '{edit_comment}'::text[], mode => 'UPDATE_FOR_PORTION_OF'::sql_saga.temporal_merge_mode, era_name => 'valid');

\echo '--- Planner: Actual Plan ---'
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
\echo '--- Executor: Actual Feedback ---'
TABLE pg_temp.temporal_merge_feedback ORDER BY source_row_id;
\echo '--- Target: Expected Final State ---'
SELECT * FROM (VALUES
    (1, 'A1', 'Slice 1', '2024-01-01'::date, '2024-02-01'::date),
    (1, 'A2', 'Slice 2 (Adjacent)', '2024-02-01'::date, '2024-02-10'::date),
    (1, 'A2-updated', 'Slice 2 (Adjacent)', '2024-02-10'::date, '2024-02-20'::date),
    (1, 'A2', 'Slice 2 (Adjacent)', '2024-02-20'::date, '2024-03-01'::date),
    (1, 'A3', 'Slice 3 (After Gap)', '2024-04-01'::date, '2024-05-01'::date),
    (2, 'B',  'Long-lived',        '2023-01-01'::date, '2026-01-01'::date)
) t(id, value, edit_comment, valid_from, valid_until) ORDER BY id, valid_from;
\echo '--- Target: Final State ---'
TABLE tmm.target ORDER BY id, valid_from;
ROLLBACK TO SAVEPOINT s8;

ROLLBACK;
\i sql/include/test_teardown.sql
