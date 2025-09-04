\i sql/include/test_setup.sql

BEGIN;
\echo '----------------------------------------------------------------------------'
\echo 'Test Suite: `sql_saga.temporal_merge` Deletion Semantics'
\echo 'Description:'
\echo '  This test suite verifies the behavior of the `p_delete_mode` parameter,'
\echo '  using a rich target timeline to test interactions with gaps and adjacencies.'
\echo '----------------------------------------------------------------------------'

SET client_min_messages TO WARNING;
CREATE SCHEMA tmd; -- Temporal Merge Delete

-- Target table for all scenarios
CREATE TABLE tmd.target (id int, value text, edit_comment text, valid_from date, valid_until date);
SELECT sql_saga.add_era('tmd.target', 'valid_from', 'valid_until');

-- Helper to reset state to a rich timeline
CREATE PROCEDURE tmd.reset_target() LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE tmd.target;
    INSERT INTO tmd.target VALUES
        -- Entity 1 has adjacent slices and a gap
        (1, 'A1', 'Slice 1',           '2024-01-01', '2024-02-01'),
        (1, 'A2', 'Slice 2 (Adjacent)','2024-02-01', '2024-03-01'),
        (1, 'A3', 'Slice 3 (After Gap)','2024-04-01', '2024-05-01'),
        -- Entity 2 is a simple, long-lived entity
        (2, 'B',  'Long-lived',        '2023-01-01', '2026-01-01');
END;
$$;

--------------------------------------------------------------------------------
\echo 'Scenario 1: `DELETE_MISSING_TIMELINE`'
\echo 'Use Case: For an entity in the source, any part of its timeline NOT covered by the source is deleted.'
--------------------------------------------------------------------------------
SAVEPOINT s1;
CALL tmd.reset_target();
CREATE TEMP TABLE source_1 (row_id int, id int, value text, edit_comment text, valid_from date, valid_until date) ON COMMIT DROP;
-- Source covers only part of entity 1's history (Slice 2 and the gap).
-- The start (Slice 1) and end (Slice 3) should be deleted.
INSERT INTO source_1 VALUES (101, 1, 'A2-updated', 'Update S2 and bridge gap', '2024-02-01', '2024-04-01');

\echo '--- Target: Initial State ---'
TABLE tmd.target ORDER BY id, valid_from;
\echo '--- Source: Data to merge ---'
TABLE source_1 ORDER BY row_id;

CALL sql_saga.temporal_merge(
    p_target_table      := 'tmd.target'::regclass,
    p_source_table      := 'source_1'::regclass,
    p_id_columns        := '{id}'::text[],
    p_ephemeral_columns := '{edit_comment}'::text[],
    p_mode              := 'MERGE_ENTITY_REPLACE',
    p_delete_mode       := 'DELETE_MISSING_TIMELINE',
    p_era_name          := 'valid'
);

\echo '--- Planner: Actual Plan ---'
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
\echo '--- Executor: Actual Feedback ---'
TABLE pg_temp.temporal_merge_feedback ORDER BY source_row_id;
\echo '--- Target: Expected Final State ---'
SELECT * FROM (VALUES
    -- Entity 1's timeline is now only the part specified in the source.
    (1, 'A2-updated', 'Update S2 and bridge gap', '2024-02-01'::date, '2024-04-01'::date),
    -- Entity 2 is untouched because it was not in the source.
    (2, 'B', 'Long-lived', '2023-01-01'::date, '2026-01-01'::date)
) t(id, value, edit_comment, valid_from, valid_until) ORDER BY id, valid_from;
\echo '--- Target: Final State ---'
TABLE tmd.target ORDER BY id, valid_from;
ROLLBACK TO SAVEPOINT s1;

--------------------------------------------------------------------------------
\echo 'Scenario 2: `DELETE_MISSING_ENTITIES`'
\echo 'Use Case: Any entity in the target that is NOT present in the source is completely deleted.'
--------------------------------------------------------------------------------
SAVEPOINT s2;
CALL tmd.reset_target();
CREATE TEMP TABLE source_2 (row_id int, id int, value text, edit_comment text, valid_from date, valid_until date) ON COMMIT DROP;
-- Source only contains entity 1, so entity 2 should be deleted.
-- The source patches entity 1, but its timeline outside this patch should be preserved.
INSERT INTO source_2 VALUES (201, 1, 'A2-patch', 'Patch S2', '2024-02-10', '2024-02-20');

\echo '--- Target: Initial State ---'
TABLE tmd.target ORDER BY id, valid_from;
\echo '--- Source: Data to merge ---'
TABLE source_2 ORDER BY row_id;

CALL sql_saga.temporal_merge(
    p_target_table      := 'tmd.target'::regclass,
    p_source_table      := 'source_2'::regclass,
    p_id_columns        := '{id}'::text[],
    p_ephemeral_columns := '{edit_comment}'::text[],
    p_mode              := 'MERGE_ENTITY_REPLACE',
    p_delete_mode       := 'DELETE_MISSING_ENTITIES',
    p_era_name          := 'valid'
);

\echo '--- Planner: Actual Plan ---'
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
\echo '--- Executor: Actual Feedback ---'
TABLE pg_temp.temporal_merge_feedback ORDER BY source_row_id;
\echo '--- Target: Expected Final State ---'
SELECT * FROM (VALUES
    -- Entity 1's timeline is patched, but not truncated. Entity 2 is gone.
    (1, 'A1', 'Slice 1', '2024-01-01'::date, '2024-02-01'::date),
    (1, 'A2', 'Slice 2 (Adjacent)', '2024-02-01'::date, '2024-02-10'::date),
    (1, 'A2-patch', 'Patch S2', '2024-02-10'::date, '2024-02-20'::date),
    (1, 'A2', 'Slice 2 (Adjacent)', '2024-02-20'::date, '2024-03-01'::date),
    (1, 'A3', 'Slice 3 (After Gap)', '2024-04-01'::date, '2024-05-01'::date)
) t(id, value, edit_comment, valid_from, valid_until) ORDER BY id, valid_from;
\echo '--- Target: Final State ---'
TABLE tmd.target ORDER BY id, valid_from;
ROLLBACK TO SAVEPOINT s2;

--------------------------------------------------------------------------------
\echo 'Scenario 3: `DELETE_MISSING_TIMELINE_AND_ENTITIES`'
\echo 'Use Case: A full "source-as-truth" sync. Timelines of source entities'
\echo '             are replaced, and target entities not in source are deleted.'
--------------------------------------------------------------------------------
SAVEPOINT s3;
CALL tmd.reset_target();
CREATE TEMP TABLE source_3 (row_id int, id int, value text, edit_comment text, valid_from date, valid_until date) ON COMMIT DROP;
-- Source only contains entity 1. Its timeline should be replaced, and entity 2 should be deleted.
INSERT INTO source_3 VALUES (301, 1, 'A2-updated', 'Update S2 and bridge gap', '2024-02-01', '2024-04-01');

\echo '--- Target: Initial State ---'
TABLE tmd.target ORDER BY id, valid_from;
\echo '--- Source: Data to merge ---'
TABLE source_3 ORDER BY row_id;

CALL sql_saga.temporal_merge(
    p_target_table      := 'tmd.target'::regclass,
    p_source_table      := 'source_3'::regclass,
    p_id_columns        := '{id}'::text[],
    p_ephemeral_columns := '{edit_comment}'::text[],
    p_mode              := 'MERGE_ENTITY_REPLACE',
    p_delete_mode       := 'DELETE_MISSING_TIMELINE_AND_ENTITIES',
    p_era_name          := 'valid'
);

\echo '--- Planner: Actual Plan ---'
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
\echo '--- Executor: Actual Feedback ---'
TABLE pg_temp.temporal_merge_feedback ORDER BY source_row_id;
\echo '--- Target: Expected Final State ---'
SELECT * FROM (VALUES
    -- Entity 1's timeline is replaced, and Entity 2 is gone.
    (1, 'A2-updated', 'Update S2 and bridge gap', '2024-02-01'::date, '2024-04-01'::date)
) t(id, value, edit_comment, valid_from, valid_until) ORDER BY id, valid_from;
\echo '--- Target: Final State ---'
TABLE tmd.target ORDER BY id, valid_from;
ROLLBACK TO SAVEPOINT s3;

ROLLBACK;
\i sql/include/test_teardown.sql
