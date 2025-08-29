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

-- Run the orchestrator. This call will fail until the API is updated.
CREATE TEMP TABLE actual_feedback_1 (LIKE sql_saga.temporal_merge_result) ON COMMIT DROP;
INSERT INTO actual_feedback_1
SELECT * FROM sql_saga.temporal_merge(
    p_target_table             => 'tmisd.establishment',
    p_source_table             => 'temp_source_1',
    p_id_columns               => '{id}'::TEXT[],
    p_ephemeral_columns        => '{edit_comment}'::TEXT[],
    p_mode                     => 'upsert_replace',
    p_era_name                 => 'valid',
    p_founding_id_column       => 'founding_id'
);

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{1}'::INT[], 'INSERT'::sql_saga.plan_operation_type, '{"id": null, "founding_id": "1"}'::JSONB, NULL::DATE, '2023-01-01'::DATE, '2023-06-01'::DATE, '{"name": "Initial Name", "edit_comment": "First slice"}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (2, '{2}'::INT[], 'INSERT'::sql_saga.plan_operation_type, '{"id": null, "founding_id": "1"}'::JSONB, NULL::DATE, '2023-06-01'::DATE, '2023-12-31'::DATE, '{"name": "Corrected Name", "edit_comment": "Second slice, replaces part of first"}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation) ORDER BY plan_op_seq;

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM __temp_last_sql_saga_temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES
    (1, '[{"id": 1}]'::JSONB, 'SUCCESS'::sql_saga.set_result_status, NULL::TEXT),
    (2, '[{"id": 1}]'::JSONB, 'SUCCESS'::sql_saga.set_result_status, NULL::TEXT)
) AS t (source_row_id, target_entity_ids, status, error_message) ORDER BY source_row_id;

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM actual_feedback_1 ORDER BY source_row_id;

\echo '--- Orchestrator: Expected Final State (A single entity with two historical slices) ---'
SELECT * FROM (VALUES
    (1, 'Initial Name', '2023-01-01'::DATE, '2023-06-01'::DATE, 'First slice'::TEXT),
    (1, 'Corrected Name', '2023-06-01'::DATE, '2023-12-31'::DATE, 'Second slice, replaces part of first'::TEXT)
) AS t (id, name, valid_from, valid_until, edit_comment);
\echo '--- Orchestrator: Actual Final State ---'
SELECT id, name, valid_from, valid_until, edit_comment FROM tmisd.establishment WHERE id = 1 ORDER BY valid_from;
DROP TABLE temp_source_1;


\echo '--- Scenario 2: Batch with multiple new entities, each with internal dependencies ---'

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
CREATE TEMP TABLE actual_feedback_2 (LIKE sql_saga.temporal_merge_result) ON COMMIT DROP;
INSERT INTO actual_feedback_2
SELECT * FROM sql_saga.temporal_merge(
    p_target_table             => 'tmisd.establishment',
    p_source_table             => 'temp_source_2',
    p_id_columns               => '{id}'::TEXT[],
    p_ephemeral_columns        => '{edit_comment}'::TEXT[],
    p_mode                     => 'upsert_replace',
    p_era_name                 => 'valid',
    p_founding_id_column       => 'founding_id'
);

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{101}'::INT[], 'INSERT'::sql_saga.plan_operation_type, '{"id": null, "founding_id": "10"}'::JSONB, NULL::DATE, '2024-01-01'::DATE, '2024-06-01'::DATE, '{"name": "Entity 10 Original", "edit_comment": "E10-S1"}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (2, '{102}'::INT[], 'INSERT'::sql_saga.plan_operation_type, '{"id": null, "founding_id": "10"}'::JSONB, NULL::DATE, '2024-06-01'::DATE, '2024-09-01'::DATE, '{"name": "Entity 10 Update", "edit_comment": "E10-S2"}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (3, '{101}'::INT[], 'INSERT'::sql_saga.plan_operation_type, '{"id": null, "founding_id": "10"}'::JSONB, NULL::DATE, '2024-09-01'::DATE, '2024-12-31'::DATE, '{"name": "Entity 10 Original", "edit_comment": "E10-S1"}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (4, '{201}'::INT[], 'INSERT'::sql_saga.plan_operation_type, '{"id": null, "founding_id": "20"}'::JSONB, NULL::DATE, '2025-01-01'::DATE, '2025-07-01'::DATE, '{"name": "Entity 20 Initial", "edit_comment": "E20-S1"}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (5, '{202}'::INT[], 'INSERT'::sql_saga.plan_operation_type, '{"id": null, "founding_id": "20"}'::JSONB, NULL::DATE, '2025-07-01'::DATE, '2025-12-31'::DATE, '{"name": "Entity 20 New End", "edit_comment": "E20-S2"}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation) ORDER BY plan_op_seq;

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM __temp_last_sql_saga_temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES
    (101, '[{"id": 2}]'::JSONB, 'SUCCESS'::sql_saga.set_result_status, NULL::TEXT),
    (102, '[{"id": 2}]'::JSONB, 'SUCCESS'::sql_saga.set_result_status, NULL::TEXT),
    (201, '[{"id": 3}]'::JSONB, 'SUCCESS'::sql_saga.set_result_status, NULL::TEXT),
    (202, '[{"id": 3}]'::JSONB, 'SUCCESS'::sql_saga.set_result_status, NULL::TEXT)
) AS t (source_row_id, target_entity_ids, status, error_message) ORDER BY source_row_id;

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM actual_feedback_2 ORDER BY source_row_id;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (2, 'Entity 10 Original', '2024-01-01'::DATE, '2024-06-01'::DATE, 'E10-S1'::TEXT),
    (2, 'Entity 10 Update', '2024-06-01'::DATE, '2024-09-01'::DATE, 'E10-S2'::TEXT),
    (2, 'Entity 10 Original', '2024-09-01'::DATE, '2024-12-31'::DATE, 'E10-S1'::TEXT),
    (3, 'Entity 20 Initial', '2025-01-01'::DATE, '2025-07-01'::DATE, 'E20-S1'::TEXT),
    (3, 'Entity 20 New End', '2025-07-01'::DATE, '2025-12-31'::DATE, 'E20-S2'::TEXT)
) AS t (id, name, valid_from, valid_until, edit_comment) ORDER BY id, valid_from;

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, name, valid_from, valid_until, edit_comment FROM tmisd.establishment WHERE id > 1 ORDER BY id, valid_from;
DROP TABLE temp_source_2;


-- Final Cleanup
DROP TABLE tmisd.establishment;
DROP SCHEMA tmisd CASCADE;

ROLLBACK;
\i sql/include/test_teardown.sql
