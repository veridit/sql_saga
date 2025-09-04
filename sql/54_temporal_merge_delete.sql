\i sql/include/test_setup.sql

BEGIN;
\echo '----------------------------------------------------------------------------'
\echo 'Test Suite: `sql_saga.temporal_merge` Deletion Semantics'
\echo 'Description:'
\echo '  This test suite verifies the behavior of the `p_delete_mode` parameter.'
\echo '----------------------------------------------------------------------------'

SET client_min_messages TO WARNING;
CREATE SCHEMA tmd; -- Temporal Merge Delete

-- Target table for all scenarios
CREATE TABLE tmd.target (id int, valid_from date, valid_until date, value text);
SELECT sql_saga.add_era('tmd.target', 'valid_from', 'valid_until');

-- Helper to reset state
CREATE PROCEDURE tmd.reset_target() LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE tmd.target;
    INSERT INTO tmd.target VALUES
        -- Entity 1 has three distinct historical periods
        (1, '2024-01-01', '2024-04-01', 'Original A'),
        (1, '2024-04-01', '2024-08-01', 'Original B'),
        (1, '2024-08-01', '2025-01-01', 'Original C'),
        -- Entity 2 will be used to test entity-level deletion
        (2, '2024-01-01', '2025-01-01', 'Untouched');
END;
$$;

--------------------------------------------------------------------------------
\echo 'Scenario 1: `DELETE_MISSING_TIMELINE`'
\echo 'Description: Verifies that for an entity present in the source, any part of its'
\echo '             timeline NOT covered by the source is deleted.'
--------------------------------------------------------------------------------
CALL tmd.reset_target();
CREATE TEMP TABLE temp_source_1 (row_id int, id int, valid_from date, valid_until date, value text);
-- The source only specifies the middle part of entity 1's timeline.
-- The beginning and end should be deleted. Entity 2 is not in the source.
INSERT INTO temp_source_1 VALUES (101, 1, '2024-04-01', '2024-08-01', 'Replacement B');

\echo '--- Target: Initial State ---'
SELECT * FROM tmd.target ORDER BY id, valid_from;
\echo '--- Source: Data to merge ---'
SELECT * FROM temp_source_1 ORDER BY row_id;

CALL sql_saga.temporal_merge(
    p_target_table := 'tmd.target'::regclass,
    p_source_table := 'temp_source_1'::regclass,
    p_id_columns := '{id}'::text[],
    p_ephemeral_columns := '{}'::text[],
    p_mode := 'MERGE_ENTITY_REPLACE',
    p_delete_mode := 'DELETE_MISSING_TIMELINE'
);

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    -- Entity 1's timeline is now only the part specified in the source
    (1, '2024-04-01'::date, '2024-08-01'::date, 'Replacement B'),
    -- Entity 2 is untouched because it was not in the source
    (2, '2024-01-01'::date, '2025-01-01'::date, 'Untouched')
) t(id, valid_from, valid_until, value) ORDER BY id, valid_from;

\echo '--- Orchestrator: Actual Final State ---'
SELECT * FROM tmd.target ORDER BY id, valid_from;
DROP TABLE temp_source_1;

--------------------------------------------------------------------------------
\echo 'Scenario 2: `DELETE_MISSING_ENTITIES`'
\echo 'Description: Verifies that any entity in the target that is NOT present'
\echo '             in the source is completely deleted. The timeline of entities'
\echo '             present in the source is preserved.'
--------------------------------------------------------------------------------
CALL tmd.reset_target();
CREATE TEMP TABLE temp_source_2 (row_id int, id int, valid_from date, valid_until date, value text);
-- The source only contains entity 1. Entity 2 should be deleted from the target.
-- The source only specifies a small part of entity 1's timeline, but the rest should be preserved.
INSERT INTO temp_source_2 VALUES (201, 1, '2024-06-01', '2024-07-01', 'Replacement Mid-B');

\echo '--- Target: Initial State ---'
SELECT * FROM tmd.target ORDER BY id, valid_from;
\echo '--- Source: Data to merge ---'
SELECT * FROM temp_source_2 ORDER BY row_id;

CALL sql_saga.temporal_merge(
    p_target_table := 'tmd.target'::regclass,
    p_source_table := 'temp_source_2'::regclass,
    p_id_columns := '{id}'::text[],
    p_ephemeral_columns := '{}'::text[],
    p_mode := 'MERGE_ENTITY_REPLACE',
    p_delete_mode := 'DELETE_MISSING_ENTITIES'
);

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    -- Entity 1's timeline is patched, but not truncated. Entity 2 is gone.
    (1, '2024-01-01'::date, '2024-04-01'::date, 'Original A'),
    (1, '2024-04-01'::date, '2024-06-01'::date, 'Original B'),
    (1, '2024-06-01'::date, '2024-07-01'::date, 'Replacement Mid-B'),
    (1, '2024-07-01'::date, '2024-08-01'::date, 'Original B'),
    (1, '2024-08-01'::date, '2025-01-01'::date, 'Original C')
) t(id, valid_from, valid_until, value) ORDER BY id, valid_from;

\echo '--- Orchestrator: Actual Final State ---'
SELECT * FROM tmd.target ORDER BY id, valid_from;
DROP TABLE temp_source_2;

--------------------------------------------------------------------------------
\echo 'Scenario 3: `DELETE_MISSING_TIMELINE_AND_ENTITIES`'
\echo 'Description: Verifies a full synchronization: timelines of source entities'
\echo '             are replaced, and target entities not in source are deleted.'
--------------------------------------------------------------------------------
CALL tmd.reset_target();
CREATE TEMP TABLE temp_source_3 (row_id int, id int, valid_from date, valid_until date, value text);
-- The source only contains entity 1. Its timeline should be replaced, and entity 2 should be deleted.
INSERT INTO temp_source_3 VALUES (301, 1, '2024-04-01', '2024-08-01', 'Replacement B');

\echo '--- Target: Initial State ---'
SELECT * FROM tmd.target ORDER BY id, valid_from;
\echo '--- Source: Data to merge ---'
SELECT * FROM temp_source_3 ORDER BY row_id;

CALL sql_saga.temporal_merge(
    p_target_table := 'tmd.target'::regclass,
    p_source_table := 'temp_source_3'::regclass,
    p_id_columns := '{id}'::text[],
    p_ephemeral_columns := '{}'::text[],
    p_mode := 'MERGE_ENTITY_REPLACE',
    p_delete_mode := 'DELETE_MISSING_TIMELINE_AND_ENTITIES'
);

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    -- Entity 1's timeline is replaced, and Entity 2 is gone.
    (1, '2024-04-01'::date, '2024-08-01'::date, 'Replacement B')
) t(id, valid_from, valid_until, value) ORDER BY id, valid_from;

\echo '--- Orchestrator: Actual Final State ---'
SELECT * FROM tmd.target ORDER BY id, valid_from;
DROP TABLE temp_source_3;


ROLLBACK;
\i sql/include/test_teardown.sql
