\i sql/include/test_setup.sql

BEGIN;
\echo '----------------------------------------------------------------------------'
\echo 'Test: Temporal Merge with Dependent Row Support'
\echo 'This test validates that temporal_merge can resolve dependencies within a'
\echo 'single batch (e.g., an INSERT and a subsequent REPLACE for the same new'
\echo 'entity) using the p_founding_id_column parameter.'
\echo '----------------------------------------------------------------------------'

-- Setup
CREATE SCHEMA tmisd;

-- Target table with a surrogate key
CREATE TABLE tmisd.establishment (
    id SERIAL NOT NULL,
    name TEXT,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL,
    edit_comment TEXT,
    PRIMARY KEY (id, valid_from)
);
SELECT sql_saga.add_era('tmisd.establishment', 'valid_from', 'valid_until');
-- The conceptual key for the entity
SELECT sql_saga.add_unique_key('tmisd.establishment', ARRAY['id'], 'valid', 'tmisd_establishment_uk');

-- Helper procedure to reset target table state between scenarios
CREATE PROCEDURE tmisd.reset_target() LANGUAGE plpgsql AS $$
DECLARE
    v_seq_name TEXT;
BEGIN
    TRUNCATE tmisd.establishment;
    -- TRUNCATE ... RESTART IDENTITY should work, but seems to behave unexpectedly
    -- within the transaction block of a pg_regress test. We reset manually
    -- for robustness.
    v_seq_name := pg_get_serial_sequence('tmisd.establishment', 'id');
    IF v_seq_name IS NOT NULL THEN
        EXECUTE 'ALTER SEQUENCE ' || v_seq_name || ' RESTART WITH 1;';
    END IF;
END;
$$;

-- Source table with the founding_id column
CREATE TEMP TABLE temp_source_1 (
    row_id INT,
    founding_id INT, -- This is the key column for grouping
    id INT, -- This is NULL for new entities
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL,
    name TEXT,
    edit_comment TEXT
) ON COMMIT DROP;

\echo '--- Scenario 1: INSERT of new entity with a subsequent REPLACE in the same batch ---'

CALL tmisd.reset_target();
-- Populate source data.
-- row_id=1 "founds" the new entity.
-- row_id=2 is a historical correction for the entity founded by row_id=1.
-- Both have the same founding_id.
INSERT INTO temp_source_1 VALUES
(1, 1, NULL, '2023-01-01', '2023-12-31', 'Initial Name', 'First slice'),
(2, 1, NULL, '2023-06-01', '2023-12-31', 'Corrected Name', 'Second slice, replaces part of first');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, name, valid_from, valid_until, edit_comment FROM tmisd.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_1 ORDER BY row_id;

-- Run the orchestrator.
CALL sql_saga.temporal_merge(
    p_target_table             => 'tmisd.establishment',
    p_source_table             => 'temp_source_1',
    p_id_columns               => '{id}'::TEXT[],
    p_ephemeral_columns        => '{edit_comment}'::TEXT[],
    p_mode                     => 'MERGE_ENTITY_REPLACE',
    p_era_name                 => 'valid',
    p_founding_id_column       => 'founding_id',
    p_update_source_with_assigned_entity_ids => true
);

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{1}'::INT[], 'INSERT'::sql_saga.planner_action, '{"id": 1, "founding_id": "1"}'::JSONB, NULL::DATE, '2023-01-01'::DATE, '2023-06-01'::DATE, '{"name": "Initial Name", "edit_comment": "First slice"}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (2, '{2}'::INT[], 'INSERT'::sql_saga.planner_action, '{"id": 1, "founding_id": "1"}'::JSONB, NULL::DATE, '2023-06-01'::DATE, '2023-12-31'::DATE, '{"name": "Corrected Name", "edit_comment": "Second slice, replaces part of first"}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation) ORDER BY plan_op_seq;

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES
    (1, '[{"id": 1}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_status, NULL::TEXT),
    (2, '[{"id": 1}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_status, NULL::TEXT)
) AS t (source_row_id, target_entity_ids, status, error_message) ORDER BY source_row_id;

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id;

\echo '--- Orchestrator: Expected Final State (A single entity with two historical slices) ---'
SELECT * FROM (VALUES
    (1, 'Initial Name', '2023-01-01'::DATE, '2023-06-01'::DATE, 'First slice'::TEXT),
    (1, 'Corrected Name', '2023-06-01'::DATE, '2023-12-31'::DATE, 'Second slice, replaces part of first'::TEXT)
) AS t (id, name, valid_from, valid_until, edit_comment);
\echo '--- Orchestrator: Actual Final State ---'
SELECT id, name, valid_from, valid_until, edit_comment FROM tmisd.establishment ORDER BY id, valid_from;

\echo '--- Source Table: Expected state after back-fill ---'
SELECT * FROM (VALUES
    (1, 1, 1, '2023-01-01'::date, '2023-12-31'::date, 'Initial Name', 'First slice'),
    (2, 1, 1, '2023-06-01'::date, '2023-12-31'::date, 'Corrected Name', 'Second slice, replaces part of first')
) t(row_id, founding_id, id, valid_from, valid_until, name, edit_comment) ORDER BY row_id;
\echo '--- Source Table: Actual state after back-fill ---'
SELECT * FROM temp_source_1 ORDER BY row_id;
DROP TABLE temp_source_1;


\echo '--- Scenario 2: Batch with multiple new entities, each with internal dependencies ---'

CALL tmisd.reset_target();
-- Recreate source table for the new scenario
CREATE TEMP TABLE temp_source_2 (
    row_id INT,
    founding_id INT,
    id INT,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL,
    name TEXT,
    edit_comment TEXT
) ON COMMIT DROP;

INSERT INTO temp_source_2 VALUES
-- Entity 1 (founding_id=10) has a `during` split
(101, 10, NULL, '2024-01-01', '2024-12-31', 'Entity 10 Original', 'E10-S1'),
(102, 10, NULL, '2024-06-01', '2024-09-01', 'Entity 10 Update', 'E10-S2'),
-- Entity 2 (founding_id=20) has a `finishes` split
(201, 20, NULL, '2025-01-01', '2025-12-31', 'Entity 20 Initial', 'E20-S1'),
(202, 20, NULL, '2025-07-01', '2025-12-31', 'Entity 20 New End', 'E20-S2');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, name, valid_from, valid_until, edit_comment FROM tmisd.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_2 ORDER BY row_id;

-- Run the orchestrator
CALL sql_saga.temporal_merge(
    p_target_table             => 'tmisd.establishment',
    p_source_table             => 'temp_source_2',
    p_id_columns               => '{id}'::TEXT[],
    p_ephemeral_columns        => '{edit_comment}'::TEXT[],
    p_mode                     => 'MERGE_ENTITY_REPLACE',
    p_era_name                 => 'valid',
    p_founding_id_column       => 'founding_id',
    p_update_source_with_assigned_entity_ids => true
);

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{101}'::INT[], 'INSERT'::sql_saga.planner_action, '{"id": 1, "founding_id": "10"}'::JSONB, NULL::DATE, '2024-01-01'::DATE, '2024-06-01'::DATE, '{"name": "Entity 10 Original", "edit_comment": "E10-S1"}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (2, '{102}'::INT[], 'INSERT'::sql_saga.planner_action, '{"id": 1, "founding_id": "10"}'::JSONB, NULL::DATE, '2024-06-01'::DATE, '2024-09-01'::DATE, '{"name": "Entity 10 Update", "edit_comment": "E10-S2"}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (3, '{101}'::INT[], 'INSERT'::sql_saga.planner_action, '{"id": 1, "founding_id": "10"}'::JSONB, NULL::DATE, '2024-09-01'::DATE, '2024-12-31'::DATE, '{"name": "Entity 10 Original", "edit_comment": "E10-S1"}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (4, '{201}'::INT[], 'INSERT'::sql_saga.planner_action, '{"id": 2, "founding_id": "20"}'::JSONB, NULL::DATE, '2025-01-01'::DATE, '2025-07-01'::DATE, '{"name": "Entity 20 Initial", "edit_comment": "E20-S1"}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (5, '{202}'::INT[], 'INSERT'::sql_saga.planner_action, '{"id": 2, "founding_id": "20"}'::JSONB, NULL::DATE, '2025-07-01'::DATE, '2025-12-31'::DATE, '{"name": "Entity 20 New End", "edit_comment": "E20-S2"}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation) ORDER BY plan_op_seq;

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES
    (101, '[{"id": 1}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_status, NULL::TEXT),
    (102, '[{"id": 1}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_status, NULL::TEXT),
    (201, '[{"id": 2}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_status, NULL::TEXT),
    (202, '[{"id": 2}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_status, NULL::TEXT)
) AS t (source_row_id, target_entity_ids, status, error_message) ORDER BY source_row_id;

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (1, 'Entity 10 Original', '2024-01-01'::DATE, '2024-06-01'::DATE, 'E10-S1'::TEXT),
    (1, 'Entity 10 Update', '2024-06-01'::DATE, '2024-09-01'::DATE, 'E10-S2'::TEXT),
    (1, 'Entity 10 Original', '2024-09-01'::DATE, '2024-12-31'::DATE, 'E10-S1'::TEXT),
    (2, 'Entity 20 Initial', '2025-01-01'::DATE, '2025-07-01'::DATE, 'E20-S1'::TEXT),
    (2, 'Entity 20 New End', '2025-07-01'::DATE, '2025-12-31'::DATE, 'E20-S2'::TEXT)
) AS t (id, name, valid_from, valid_until, edit_comment) ORDER BY id, valid_from;

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, name, valid_from, valid_until, edit_comment FROM tmisd.establishment ORDER BY id, valid_from;

\echo '--- Source Table: Expected state after back-fill ---'
SELECT * FROM (VALUES
    (101, 10, 1, '2024-01-01'::date, '2024-12-31'::date, 'Entity 10 Original', 'E10-S1'),
    (102, 10, 1, '2024-06-01'::date, '2024-09-01'::date, 'Entity 10 Update', 'E10-S2'),
    (201, 20, 2, '2025-01-01'::date, '2025-12-31'::date, 'Entity 20 Initial', 'E20-S1'),
    (202, 20, 2, '2025-07-01'::date, '2025-12-31'::date, 'Entity 20 New End', 'E20-S2')
) t(row_id, founding_id, id, valid_from, valid_until, name, edit_comment) ORDER BY row_id;
\echo '--- Source Table: Actual state after back-fill ---'
SELECT * FROM temp_source_2 ORDER BY row_id;
DROP TABLE temp_source_2;


\echo '--- Scenario 3: founding_id set split across two separate temporal_merge calls ---'
\echo '--- This demonstrates that founding_id only resolves dependencies within a single batch. ---'
\echo '--- Splitting a founding set across calls will result in multiple distinct entities. ---'

CALL tmisd.reset_target();
-- A single source table for both calls. Note: ON COMMIT DROP is omitted so we
-- can inspect the final state. The table is dropped manually.
CREATE TEMP TABLE temp_source_3 (
    row_id INT,
    founding_id INT,
    id INT,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL,
    name TEXT,
    edit_comment TEXT
);

-- Two updatable views, each representing a separate batch for temporal_merge
CREATE TEMP VIEW v_source_3a AS SELECT * FROM temp_source_3 WHERE row_id = 301;
CREATE TEMP VIEW v_source_3b AS SELECT * FROM temp_source_3 WHERE row_id = 302;

INSERT INTO temp_source_3 VALUES
(301, 30, NULL, '2026-01-01', '2026-12-31', 'Entity 30 Original', 'E30-S1'),
(302, 30, NULL, '2026-06-01', '2026-09-01', 'Entity 30 Update',   'E30-S2');

\echo '--- Source: Initial state before any calls ---'
SELECT * FROM temp_source_3 ORDER BY row_id;

\echo '--- Source for call 1 (via view v_source_3a): ---'
SELECT * FROM v_source_3a ORDER BY row_id;
\echo '--- Source for call 2 (via view v_source_3b): ---'
SELECT * FROM v_source_3b ORDER BY row_id;

\echo '--- First call: processing only the first part of the founding set (row_id=301) via v_source_3a ---'
CALL sql_saga.temporal_merge(
    p_target_table             => 'tmisd.establishment',
    p_source_table             => 'v_source_3a',
    p_id_columns               => '{id}'::TEXT[],
    p_ephemeral_columns        => '{edit_comment}'::TEXT[],
    p_mode                     => 'MERGE_ENTITY_REPLACE',
    p_era_name                 => 'valid',
    p_founding_id_column       => 'founding_id',
    p_update_source_with_assigned_entity_ids => true
);

\echo '--- Target: State after first call (one new entity created) ---'
-- Look for entities created after the first two scenarios
SELECT id, name, valid_from, valid_until, edit_comment FROM tmisd.establishment ORDER BY id, valid_from;

\echo '--- Source: State after first call (id back-filled for row_id=301) ---'
SELECT * FROM (VALUES
    (301, 30, 1, '2026-01-01'::date, '2026-12-31'::date, 'Entity 30 Original', 'E30-S1'),
    (302, 30, NULL, '2026-06-01'::date, '2026-09-01'::date, 'Entity 30 Update',   'E30-S2')
) t(row_id, founding_id, id, valid_from, valid_until, name, edit_comment) ORDER BY row_id;
\echo '--- Source: Actual state after first call ---'
SELECT * FROM temp_source_3 ORDER BY row_id;

\echo '--- Second call: processing the second part of the founding set (row_id=302) via v_source_3b ---'
-- In the second call, founding_id=30 is seen again. Since the context is a new
-- source view, temporal_merge treats it as the founding of a new entity. It
-- does not know that founding_id=30 was used in a previous call.
CALL sql_saga.temporal_merge(
    p_target_table             => 'tmisd.establishment',
    p_source_table             => 'v_source_3b',
    p_id_columns               => '{id}'::TEXT[],
    p_ephemeral_columns        => '{edit_comment}'::TEXT[],
    p_mode                     => 'MERGE_ENTITY_REPLACE',
    p_era_name                 => 'valid',
    p_founding_id_column       => 'founding_id',
    p_update_source_with_assigned_entity_ids => true
);

\echo '--- Orchestrator: Expected Final State (Two separate entities created) ---'
-- Entity with id=1 from the first call
-- Entity with id=2 from the second call (treated as a new founding event)
SELECT * FROM (VALUES
    (1, 'Entity 30 Original', '2026-01-01'::DATE, '2026-12-31'::DATE, 'E30-S1'::TEXT),
    (2, 'Entity 30 Update', '2026-06-01'::DATE, '2026-09-01'::DATE, 'E30-S2'::TEXT)
) AS t (id, name, valid_from, valid_until, edit_comment) ORDER BY id, valid_from;

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, name, valid_from, valid_until, edit_comment FROM tmisd.establishment ORDER BY id, valid_from;

\echo '--- Source Table: Expected state after both calls ---'
SELECT * FROM (VALUES
    (301, 30, 1, '2026-01-01'::date, '2026-12-31'::date, 'Entity 30 Original', 'E30-S1'),
    (302, 30, 2, '2026-06-01'::date, '2026-09-01'::date, 'Entity 30 Update',   'E30-S2')
) t(row_id, founding_id, id, valid_from, valid_until, name, edit_comment) ORDER BY row_id;
\echo '--- Source Table: Actual state after both calls ---'
SELECT * FROM temp_source_3 ORDER BY row_id;

-- Cleanup for this scenario
DROP VIEW v_source_3a;
DROP VIEW v_source_3b;
DROP TABLE temp_source_3;

-- Final Cleanup
DROP TABLE tmisd.establishment;
DROP SCHEMA tmisd CASCADE;

ROLLBACK;
\i sql/include/test_teardown.sql
