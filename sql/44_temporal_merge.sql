-- =============================================================================
-- Test Suite: `sql_saga.temporal_merge` (Single Key)
--
-- Description:
--   This test suite provides comprehensive validation for the unified
--   `temporal_merge_plan` and `temporal_merge` functions for entities with
--   a single-column primary key.
--
-- Table of Contents:
--   - Setup
--   - Scenario 1: Initial Data Load (Empty Target)
--   - Scenarios for `upsert_patch` mode (Allen's Interval Algebra)
--     - Scenario 2: `starts`
--     - Scenario 3: `finishes`
--     - Scenario 4: `during` / `contains`
--     - Scenario 5: `overlaps`
--     - Scenario 6: `overlapped by`
--     - Scenario 7: `meets`
--     - Scenario 8: `met by`
--     - Scenario 9: `before`
--     - Scenario 10: `after`
--     - Scenario 11: `equals`
--   - Scenarios for `upsert_replace` mode
--     - Scenario 12: `starts`
--     - Scenario 13: `finishes`
--     - Scenario 14: `during` / `contains`
--     - Scenario 15: `overlaps`
--     - Scenario 16: `equals`
--     - Scenario 17: `replace` with NULL source value
--   - Scenarios for `_only` modes (Filtering Behavior)
--     - Scenario 18: `patch_only` on non-existent entity
--     - Scenario 19: `replace_only` on non-existent entity
--   - Scenarios for NULL handling in `patch` mode
--     - Scenario 20: `upsert_patch` with NULL source value
--     - Scenario 21: `patch_only` with NULL source value
--   - Scenarios for Ephemeral Columns
--     - Scenario 22: `equals` with different ephemeral data
--     - Scenario 23: `equals` with identical data (should be SKIPPED)
--   - Scenarios for Multi-Row Source Data
--     - Scenario 24: Multiple disjoint source rows
--     - Scenario 25: Multiple overlapping source rows
--     - Scenario 26: Multiple source rows creating a hole
--   - Scenarios for `sql_saga` Integration
--     - Scenario 27: `starts` with deferred foreign key
--   - Scenarios for `insert_defaulted_columns`
--     - Scenario 28: Initial INSERT with default columns
--   - Scenarios for SAVEPOINT and Transactional Correctness
--     - Scenario 29: Test `meets` relation
--     - Scenario 30: Test `starts` relation
--     - Scenario 31: Test `during` relation
--     - Scenario 32: Test `overlaps` relation
--     - Scenario 33: Test `finishes` relation
--     - Scenario 34: Test `equals` relation
--   - Scenarios for Batch-Level Feedback
--     - Scenario 35: `patch_only` with mixed valid/invalid entities
--   - Scenarios for Merging and Coalescing
--     - Scenario 36: `upsert_patch` with two consecutive, identical source rows
--     - Scenario 37: `upsert_patch` with three consecutive, identical source rows
--     - Scenario 38: `upsert_patch` with two consecutive but DIFFERENT source rows
--     - Scenario 39: `upsert_patch` where source row is consecutive with existing target row
--   - Scenarios for `insert_defaulted_columns`
--     - Scenario 40: `INSERT` with `created_at` defaulted
--   - Final Cleanup
-- =============================================================================

\i sql/include/test_setup.sql

BEGIN;
SET client_min_messages TO WARNING;

-- Test schema
CREATE SCHEMA temporal_merge_test;

-- Sequences for auto-generated IDs
CREATE SEQUENCE temporal_merge_test.legal_unit_id_seq;
CREATE SEQUENCE temporal_merge_test.establishment_id_seq;

-- Target tables (simplified versions for testing)
CREATE TABLE temporal_merge_test.legal_unit (
    id INT PRIMARY KEY,
    name TEXT
);

CREATE TABLE temporal_merge_test.establishment (
    id INT NOT NULL,
    legal_unit_id INT NOT NULL,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL,
    name TEXT,
    employees INT,
    edit_comment TEXT,
    PRIMARY KEY (id, valid_from)
);
SELECT sql_saga.add_era('temporal_merge_test.establishment', 'valid_from', 'valid_until');

-- Helper procedure to reset target table state between scenarios
CREATE PROCEDURE temporal_merge_test.reset_target() LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE temporal_merge_test.establishment;
    TRUNCATE temporal_merge_test.legal_unit;
    -- Seed with a legal unit for FK constraints
    INSERT INTO temporal_merge_test.legal_unit (id, name) VALUES (1, 'Test LU');
    ALTER SEQUENCE temporal_merge_test.establishment_id_seq RESTART WITH 1;
END;
$$;

-- psql variables for the test
\set target_schema 'temporal_merge_test'
\set target_table 'temporal_merge_test.establishment'
\set source_schema 'pg_temp'
\set entity_id_cols '{id}'

\set ephemeral_cols '{edit_comment}'

--------------------------------------------------------------------------------
-- Scenarios for MERGE_ENTITY_PATCH (`insert_or_update`)
--------------------------------------------------------------------------------
\echo '================================================================================'
\echo 'Begin Scenarios for MERGE_ENTITY_PATCH mode'
\echo '================================================================================'

--------------------------------------------------------------------------------
\echo 'Scenario 1: Initial Insert of a new entity'
\echo 'Mode: MERGE_ENTITY_PATCH'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
CREATE TEMP TABLE temp_source_1 (
    row_id INT, legal_unit_id INT, id INT, valid_from DATE NOT NULL, valid_until DATE NOT NULL, name TEXT, employees INT, edit_comment TEXT
) ON COMMIT DROP;
INSERT INTO temp_source_1 VALUES (101, 1, nextval('temporal_merge_test.establishment_id_seq'), '2024-01-01', '2025-01-01', 'New EST', 10, 'Initial Insert');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_1 ORDER BY row_id;

-- Run the orchestrator and store its feedback
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_1',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);
SELECT source_row_id, target_entity_ids, status, error_message FROM pg_temp.temporal_merge_feedback;

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{101}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 1}'::JSONB, NULL::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": "New EST", "employees": 10, "edit_comment": "Initial Insert", "legal_unit_id": 1}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (101, '[{"id": 1}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (1, 1, '2024-01-01'::DATE, '2025-01-01'::DATE, 'New EST'::TEXT, 10, 'Initial Insert'::TEXT)
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 1 ORDER BY valid_from;

DROP TABLE temp_source_1;

--------------------------------------------------------------------------------
\echo 'Scenario 2: `MERGE_ENTITY_PATCH` with `starts` relation'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
INSERT INTO temporal_merge_test.establishment (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment) VALUES (2, 1, '2024-01-01', '2026-01-01', 'Original', 20, 'Original slice');
CREATE TEMP TABLE temp_source_2 ( row_id INT, id INT, legal_unit_id INT, valid_from DATE NOT NULL, valid_until DATE NOT NULL, name TEXT, employees INT, edit_comment TEXT ) ON COMMIT DROP;
INSERT INTO temp_source_2 VALUES (102, 2, 1, '2024-01-01', '2025-01-01', 'Patched', 25, 'Starts patch');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_2 ORDER BY row_id;

-- Run the orchestrator and store its feedback
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_2',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{102}'::INT[], 'UPDATE'::sql_saga.temporal_merge_plan_action, '{"id": 2}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": "Patched", "employees": 25, "legal_unit_id": 1, "edit_comment": "Starts patch"}'::JSONB, 'starts'::sql_saga.allen_interval_relation),
    (2, '{102}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 2}'::JSONB, NULL::DATE,         '2025-01-01'::DATE, '2026-01-01'::DATE, '{"name": "Original", "employees": 20, "legal_unit_id": 1, "edit_comment": "Original slice"}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (102, '[{"id": 2}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (2, 1, '2024-01-01'::DATE, '2025-01-01'::DATE, 'Patched', 25, 'Starts patch'),
    (2, 1, '2025-01-01'::DATE, '2026-01-01'::DATE, 'Original', 20, 'Original slice')
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 2 ORDER BY valid_from;
DROP TABLE temp_source_2;

--------------------------------------------------------------------------------
\echo 'Scenario 3: `MERGE_ENTITY_PATCH` with `finishes` relation'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
INSERT INTO temporal_merge_test.establishment (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment) VALUES (3, 1, '2024-01-01', '2026-01-01', 'Original', 30, 'Original slice');
CREATE TEMP TABLE temp_source_3 ( row_id INT, id INT, legal_unit_id INT, valid_from DATE NOT NULL, valid_until DATE NOT NULL, name TEXT, employees INT, edit_comment TEXT) ON COMMIT DROP;
INSERT INTO temp_source_3 VALUES (103, 3, 1, '2025-01-01', '2026-01-01', 'Patched', 35, 'Finishes patch');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_3 ORDER BY row_id;

-- Run the orchestrator and store its feedback
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_3',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{103}'::INT[], 'UPDATE'::sql_saga.temporal_merge_plan_action, '{"id": 3}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": "Original", "employees": 30, "legal_unit_id": 1, "edit_comment": "Original slice"}'::JSONB, 'starts'::sql_saga.allen_interval_relation),
    (2, '{103}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 3}'::JSONB, NULL::DATE,         '2025-01-01'::DATE, '2026-01-01'::DATE, '{"name": "Patched", "employees": 35, "legal_unit_id": 1, "edit_comment": "Finishes patch"}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (103, '[{"id": 3}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (3, 1, '2024-01-01'::DATE, '2025-01-01'::DATE, 'Original', 30, 'Original slice'),
    (3, 1, '2025-01-01'::DATE, '2026-01-01'::DATE, 'Patched', 35, 'Finishes patch')
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 3 ORDER BY valid_from;
DROP TABLE temp_source_3;

--------------------------------------------------------------------------------
\echo 'Scenario 4: `MERGE_ENTITY_PATCH` with `during` relation'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
INSERT INTO temporal_merge_test.establishment (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment) VALUES (4, 1, '2024-01-01', '2026-01-01', 'Original', 40, 'Original slice');
CREATE TEMP TABLE temp_source_4 ( row_id INT, id INT, legal_unit_id INT, valid_from DATE NOT NULL, valid_until DATE NOT NULL, name TEXT, employees INT, edit_comment TEXT) ON COMMIT DROP;
INSERT INTO temp_source_4 VALUES (104, 4, 1, '2024-07-01', '2025-01-01', 'Patched', 45, 'During patch');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_4 ORDER BY row_id;

-- Run the orchestrator and store its feedback
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_4',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{104}'::INT[], 'UPDATE'::sql_saga.temporal_merge_plan_action, '{"id": 4}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2024-07-01'::DATE, '{"name": "Original", "employees": 40, "legal_unit_id": 1, "edit_comment": "Original slice"}'::JSONB, 'starts'::sql_saga.allen_interval_relation),
    (2, '{104}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 4}'::JSONB, NULL::DATE,         '2024-07-01'::DATE, '2025-01-01'::DATE, '{"name": "Patched", "employees": 45, "legal_unit_id": 1, "edit_comment": "During patch"}'::JSONB,  NULL::sql_saga.allen_interval_relation),
    (3, '{104}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 4}'::JSONB, NULL::DATE,         '2025-01-01'::DATE, '2026-01-01'::DATE, '{"name": "Original", "employees": 40, "legal_unit_id": 1, "edit_comment": "Original slice"}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (104, '[{"id": 4}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (4, 1, '2024-01-01'::DATE, '2024-07-01'::DATE, 'Original', 40, 'Original slice'),
    (4, 1, '2024-07-01'::DATE, '2025-01-01'::DATE, 'Patched', 45, 'During patch'),
    (4, 1, '2025-01-01'::DATE, '2026-01-01'::DATE, 'Original', 40, 'Original slice')
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 4 ORDER BY valid_from;
DROP TABLE temp_source_4;

--------------------------------------------------------------------------------
\echo 'Scenario 5: `MERGE_ENTITY_PATCH` with `overlaps` relation'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
INSERT INTO temporal_merge_test.establishment (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment) VALUES (5, 1, '2024-01-01', '2025-01-01', 'Original', 50, 'Original slice');
CREATE TEMP TABLE temp_source_5 ( row_id INT, id INT, legal_unit_id INT, valid_from DATE NOT NULL, valid_until DATE NOT NULL, name TEXT, employees INT, edit_comment TEXT) ON COMMIT DROP;
INSERT INTO temp_source_5 VALUES (105, 5, 1, '2024-07-01', '2025-07-01', 'Patched', 55, 'Overlaps patch');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_5 ORDER BY row_id;

-- Run the orchestrator and store its feedback
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_5',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{105}'::INT[], 'UPDATE'::sql_saga.temporal_merge_plan_action, '{"id": 5}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2024-07-01'::DATE, '{"name": "Original", "employees": 50, "legal_unit_id": 1, "edit_comment": "Original slice"}'::JSONB, 'starts'::sql_saga.allen_interval_relation),
    (2, '{105}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 5}'::JSONB, NULL::DATE,         '2024-07-01'::DATE, '2025-07-01'::DATE, '{"name": "Patched", "employees": 55, "legal_unit_id": 1, "edit_comment": "Overlaps patch"}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (105, '[{"id": 5}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (5, 1, '2024-01-01'::DATE, '2024-07-01'::DATE, 'Original', 50, 'Original slice'),
    (5, 1, '2024-07-01'::DATE, '2025-07-01'::DATE, 'Patched', 55, 'Overlaps patch')
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 5 ORDER BY valid_from;
DROP TABLE temp_source_5;

--------------------------------------------------------------------------------
\echo 'Scenario 6: `MERGE_ENTITY_PATCH` with `overlapped by` relation'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
INSERT INTO temporal_merge_test.establishment (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment) VALUES (6, 1, '2024-07-01', '2025-07-01', 'Original', 60, 'Original slice');
CREATE TEMP TABLE temp_source_6 ( row_id INT, id INT, legal_unit_id INT, valid_from DATE NOT NULL, valid_until DATE NOT NULL, name TEXT, employees INT, edit_comment TEXT) ON COMMIT DROP;
INSERT INTO temp_source_6 VALUES (106, 6, 1, '2024-01-01', '2025-01-01', 'Patched', 65, 'Overlapped by patch');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_6 ORDER BY row_id;

-- Run the orchestrator and store its feedback
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_6',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{106}'::INT[], 'UPDATE'::sql_saga.temporal_merge_plan_action, '{"id": 6}'::JSONB, '2024-07-01'::DATE, '2025-01-01'::DATE, '2025-07-01'::DATE, '{"name": "Original", "employees": 60, "legal_unit_id": 1, "edit_comment": "Original slice"}'::JSONB, 'finishes'::sql_saga.allen_interval_relation),
    (2, '{106}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 6}'::JSONB, NULL::DATE,         '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": "Patched", "employees": 65, "legal_unit_id": 1, "edit_comment": "Overlapped by patch"}'::JSONB,   NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (106, '[{"id": 6}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (6, 1, '2024-01-01'::DATE, '2025-01-01'::DATE, 'Patched', 65, 'Overlapped by patch'),
    (6, 1, '2025-01-01'::DATE, '2025-07-01'::DATE, 'Original', 60, 'Original slice')
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 6 ORDER BY valid_from;
DROP TABLE temp_source_6;

--------------------------------------------------------------------------------
\echo 'Scenario 7: `MERGE_ENTITY_PATCH` with `meets` relation'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
INSERT INTO temporal_merge_test.establishment (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment) VALUES (7, 1, '2025-01-01', '2026-01-01', 'Original', 70, 'Original slice');
CREATE TEMP TABLE temp_source_7 ( row_id INT, id INT, legal_unit_id INT, valid_from DATE NOT NULL, valid_until DATE NOT NULL, name TEXT, employees INT, edit_comment TEXT) ON COMMIT DROP;
INSERT INTO temp_source_7 VALUES (107, 7, 1, '2024-01-01', '2025-01-01', 'Patched', 75, 'Meets patch');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_7 ORDER BY row_id;

-- Run the orchestrator and store its feedback
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_7',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{107}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 7}'::JSONB, NULL::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": "Patched", "employees": 75, "legal_unit_id": 1, "edit_comment": "Meets patch"}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (2, NULL::INT[], 'SKIP_IDENTICAL'::sql_saga.temporal_merge_plan_action, '{"id": 7}'::JSONB, '2025-01-01'::DATE, '2025-01-01'::DATE, '2026-01-01'::DATE, '{"name": "Original", "employees": 70, "legal_unit_id": 1, "edit_comment": "Original slice"}'::JSONB, 'equals'::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation) ORDER BY plan_op_seq;

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (107, '[{"id": 7}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (7, 1, '2024-01-01'::DATE, '2025-01-01'::DATE, 'Patched', 75, 'Meets patch'),
    (7, 1, '2025-01-01'::DATE, '2026-01-01'::DATE, 'Original', 70, 'Original slice')
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 7 ORDER BY valid_from;
DROP TABLE temp_source_7;

--------------------------------------------------------------------------------
\echo 'Scenario 8: `MERGE_ENTITY_PATCH` with `met by` relation'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
INSERT INTO temporal_merge_test.establishment (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment) VALUES (8, 1, '2024-01-01', '2025-01-01', 'Original', 80, 'Original slice');
CREATE TEMP TABLE temp_source_8 ( row_id INT, id INT, legal_unit_id INT, valid_from DATE NOT NULL, valid_until DATE NOT NULL, name TEXT, employees INT, edit_comment TEXT) ON COMMIT DROP;
INSERT INTO temp_source_8 VALUES (108, 8, 1, '2025-01-01', '2026-01-01', 'Patched', 85, 'Met by patch');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_8 ORDER BY row_id;

-- Run the orchestrator and store its feedback
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_8',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, NULL::INT[], 'SKIP_IDENTICAL'::sql_saga.temporal_merge_plan_action, '{"id": 8}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": "Original", "employees": 80, "legal_unit_id": 1, "edit_comment": "Original slice"}'::JSONB, 'equals'::sql_saga.allen_interval_relation),
    (2, '{108}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 8}'::JSONB, NULL::DATE, '2025-01-01'::DATE, '2026-01-01'::DATE, '{"name": "Patched", "employees": 85, "legal_unit_id": 1, "edit_comment": "Met by patch"}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation) ORDER BY plan_op_seq;

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (108, '[{"id": 8}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (8, 1, '2024-01-01'::DATE, '2025-01-01'::DATE, 'Original', 80, 'Original slice'),
    (8, 1, '2025-01-01'::DATE, '2026-01-01'::DATE, 'Patched', 85, 'Met by patch')
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 8 ORDER BY valid_from;
DROP TABLE temp_source_8;

--------------------------------------------------------------------------------
\echo 'Scenario 9: `MERGE_ENTITY_PATCH` with `before` relation (non-contiguous)'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
INSERT INTO temporal_merge_test.establishment (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment) VALUES (9, 1, '2026-01-01', '2027-01-01', 'Original', 90, 'Original slice');
CREATE TEMP TABLE temp_source_9 ( row_id INT, id INT, legal_unit_id INT, valid_from DATE NOT NULL, valid_until DATE NOT NULL, name TEXT, employees INT, edit_comment TEXT) ON COMMIT DROP;
INSERT INTO temp_source_9 VALUES (109, 9, 1, '2024-01-01', '2025-01-01', 'Patched', 95, 'Before patch');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_9 ORDER BY row_id;

-- Run the orchestrator and store its feedback
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_9',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{109}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 9}'::JSONB, NULL::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": "Patched", "employees": 95, "legal_unit_id": 1, "edit_comment": "Before patch"}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (2, NULL::INT[], 'SKIP_IDENTICAL'::sql_saga.temporal_merge_plan_action, '{"id": 9}'::JSONB, '2026-01-01'::DATE, '2026-01-01'::DATE, '2027-01-01'::DATE, '{"name": "Original", "employees": 90, "legal_unit_id": 1, "edit_comment": "Original slice"}'::JSONB, 'equals'::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation) ORDER BY plan_op_seq;

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (109, '[{"id": 9}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (9, 1, '2024-01-01'::DATE, '2025-01-01'::DATE, 'Patched', 95, 'Before patch'),
    (9, 1, '2026-01-01'::DATE, '2027-01-01'::DATE, 'Original', 90, 'Original slice')
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 9 ORDER BY valid_from;
DROP TABLE temp_source_9;

--------------------------------------------------------------------------------
\echo 'Scenario 10: `MERGE_ENTITY_PATCH` with `after` relation (non-contiguous)'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
INSERT INTO temporal_merge_test.establishment (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment) VALUES (10, 1, '2024-01-01', '2025-01-01', 'Original', 100, 'Original slice');
CREATE TEMP TABLE temp_source_10 ( row_id INT, id INT, legal_unit_id INT, valid_from DATE NOT NULL, valid_until DATE NOT NULL, name TEXT, employees INT, edit_comment TEXT) ON COMMIT DROP;
INSERT INTO temp_source_10 VALUES (110, 10, 1, '2025-01-02', '2026-01-01', 'Patched', 105, 'After patch');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_10 ORDER BY row_id;

-- Run the orchestrator and store its feedback
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_10',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, NULL::INT[], 'SKIP_IDENTICAL'::sql_saga.temporal_merge_plan_action, '{"id": 10}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": "Original", "employees": 100, "legal_unit_id": 1, "edit_comment": "Original slice"}'::JSONB, 'equals'::sql_saga.allen_interval_relation),
    (2, '{110}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 10}'::JSONB, NULL::DATE, '2025-01-02'::DATE, '2026-01-01'::DATE, '{"name": "Patched", "employees": 105, "legal_unit_id": 1, "edit_comment": "After patch"}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation) ORDER BY plan_op_seq;

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (110, '[{"id": 10}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (10, 1, '2024-01-01'::DATE, '2025-01-01'::DATE, 'Original', 100, 'Original slice'),
    (10, 1, '2025-01-02'::DATE, '2026-01-01'::DATE, 'Patched', 105, 'After patch')
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 10 ORDER BY valid_from;
DROP TABLE temp_source_10;

--------------------------------------------------------------------------------
\echo 'Scenario 11: `MERGE_ENTITY_PATCH` with `equals` relation'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
INSERT INTO temporal_merge_test.establishment (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment) VALUES (11, 1, '2024-01-01', '2025-01-01', 'Original', 110, 'Original slice');
CREATE TEMP TABLE temp_source_11 ( row_id INT, id INT, legal_unit_id INT, valid_from DATE NOT NULL, valid_until DATE NOT NULL, name TEXT, employees INT, edit_comment TEXT) ON COMMIT DROP;
INSERT INTO temp_source_11 VALUES (111, 11, 1, '2024-01-01', '2025-01-01', 'Patched', 115, 'Equals patch');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_11 ORDER BY row_id;

-- Run the orchestrator and store its feedback
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_11',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{111}'::INT[], 'UPDATE'::sql_saga.temporal_merge_plan_action, '{"id": 11}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": "Patched", "employees": 115, "legal_unit_id": 1, "edit_comment": "Equals patch"}'::JSONB, 'equals'::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (111, '[{"id": 11}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (11, 1, '2024-01-01'::DATE, '2025-01-01'::DATE, 'Patched', 115, 'Equals patch')
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 11 ORDER BY valid_from;
DROP TABLE temp_source_11;

--------------------------------------------------------------------------------
\echo '================================================================================'
\echo 'Begin Scenarios for MERGE_ENTITY_REPLACE mode'
\echo '================================================================================'
--------------------------------------------------------------------------------
\echo 'Scenario 12: MERGE_ENTITY_REPLACE with starts relation'
\echo 'Mode: MERGE_ENTITY_REPLACE'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
INSERT INTO temporal_merge_test.establishment (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment) VALUES (12, 1, '2024-01-01', '2026-01-01', 'Original', 20, 'Original slice');
-- Source data
CREATE TEMP TABLE temp_source_12 ( row_id INT, id INT, legal_unit_id INT, valid_from DATE, valid_until DATE, name TEXT, employees INT, edit_comment TEXT ) ON COMMIT DROP;
INSERT INTO temp_source_12 VALUES (112, 12, 1, '2024-01-01', '2025-01-01', 'Replaced', 25, 'Starts replace');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_12 ORDER BY row_id;

-- Run merge
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_12',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_REPLACE',
    era_name => 'valid'
);
-- Verify plan
\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{112}'::INT[], 'UPDATE'::sql_saga.temporal_merge_plan_action, '{"id": 12}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": "Replaced", "employees": 25, "legal_unit_id": 1, "edit_comment": "Starts replace"}'::JSONB, 'starts'::sql_saga.allen_interval_relation),
    (2, '{112}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 12}'::JSONB, NULL::DATE, '2025-01-01'::DATE, '2026-01-01'::DATE, '{"name": "Original", "employees": 20, "legal_unit_id": 1, "edit_comment": "Original slice"}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);
\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
-- Verify feedback
\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (112, '[{"id": 12}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);
\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;
-- Verify final state
\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (12, 1, '2024-01-01'::DATE, '2025-01-01'::DATE, 'Replaced', 25, 'Starts replace'),
    (12, 1, '2025-01-01'::DATE, '2026-01-01'::DATE, 'Original', 20, 'Original slice')
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);
\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 12 ORDER BY valid_from;
DROP TABLE temp_source_12;

--------------------------------------------------------------------------------
\echo 'Scenario 13: MERGE_ENTITY_REPLACE with finishes relation'
\echo 'Mode: MERGE_ENTITY_REPLACE'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
INSERT INTO temporal_merge_test.establishment (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment) VALUES (13, 1, '2024-01-01', '2026-01-01', 'Original', 30, 'Original slice');
-- Source data
CREATE TEMP TABLE temp_source_13 ( row_id INT, id INT, legal_unit_id INT, valid_from DATE, valid_until DATE, name TEXT, employees INT, edit_comment TEXT ) ON COMMIT DROP;
INSERT INTO temp_source_13 VALUES (113, 13, 1, '2025-01-01', '2026-01-01', 'Replaced', 35, 'Finishes replace');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_13 ORDER BY row_id;

-- Run merge
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_13',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_REPLACE',
    era_name => 'valid'
);
-- Verify plan
\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{113}'::INT[], 'UPDATE'::sql_saga.temporal_merge_plan_action, '{"id": 13}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": "Original", "employees": 30, "legal_unit_id": 1, "edit_comment": "Original slice"}'::JSONB, 'starts'::sql_saga.allen_interval_relation),
    (2, '{113}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 13}'::JSONB, NULL::DATE, '2025-01-01'::DATE, '2026-01-01'::DATE, '{"name": "Replaced", "employees": 35, "legal_unit_id": 1, "edit_comment": "Finishes replace"}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);
\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
-- Verify feedback
\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (113, '[{"id": 13}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);
\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;
-- Verify final state
\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (13, 1, '2024-01-01'::DATE, '2025-01-01'::DATE, 'Original', 30, 'Original slice'),
    (13, 1, '2025-01-01'::DATE, '2026-01-01'::DATE, 'Replaced', 35, 'Finishes replace')
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);
\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 13 ORDER BY valid_from;
DROP TABLE temp_source_13;

--------------------------------------------------------------------------------
\echo 'Scenario 14: MERGE_ENTITY_REPLACE with during relation'
\echo 'Mode: MERGE_ENTITY_REPLACE'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
INSERT INTO temporal_merge_test.establishment (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment) VALUES (14, 1, '2024-01-01', '2026-01-01', 'Original', 40, 'Original slice');
-- Source data
CREATE TEMP TABLE temp_source_14 ( row_id INT, id INT, legal_unit_id INT, valid_from DATE, valid_until DATE, name TEXT, employees INT, edit_comment TEXT ) ON COMMIT DROP;
INSERT INTO temp_source_14 VALUES (114, 14, 1, '2024-07-01', '2025-01-01', 'Replaced', 45, 'During replace');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_14 ORDER BY row_id;

-- Run merge
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_14',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_REPLACE',
    era_name => 'valid'
);
-- Verify plan
\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{114}'::INT[], 'UPDATE'::sql_saga.temporal_merge_plan_action, '{"id": 14}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2024-07-01'::DATE, '{"name": "Original", "employees": 40, "legal_unit_id": 1, "edit_comment": "Original slice"}'::JSONB, 'starts'::sql_saga.allen_interval_relation),
    (2, '{114}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 14}'::JSONB, NULL::DATE, '2024-07-01'::DATE, '2025-01-01'::DATE, '{"name": "Replaced", "employees": 45, "legal_unit_id": 1, "edit_comment": "During replace"}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (3, '{114}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 14}'::JSONB, NULL::DATE, '2025-01-01'::DATE, '2026-01-01'::DATE, '{"name": "Original", "employees": 40, "legal_unit_id": 1, "edit_comment": "Original slice"}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);
\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
-- Verify feedback
\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (114, '[{"id": 14}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);
\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;
-- Verify final state
\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (14, 1, '2024-01-01'::DATE, '2024-07-01'::DATE, 'Original', 40, 'Original slice'),
    (14, 1, '2024-07-01'::DATE, '2025-01-01'::DATE, 'Replaced', 45, 'During replace'),
    (14, 1, '2025-01-01'::DATE, '2026-01-01'::DATE, 'Original', 40, 'Original slice')
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);
\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 14 ORDER BY valid_from;
DROP TABLE temp_source_14;

--------------------------------------------------------------------------------
\echo 'Scenario 15: MERGE_ENTITY_REPLACE with overlaps relation'
\echo 'Mode: MERGE_ENTITY_REPLACE'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
INSERT INTO temporal_merge_test.establishment (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment) VALUES (15, 1, '2024-01-01', '2025-01-01', 'Original', 50, 'Original slice');
-- Source data
CREATE TEMP TABLE temp_source_15 ( row_id INT, id INT, legal_unit_id INT, valid_from DATE, valid_until DATE, name TEXT, employees INT, edit_comment TEXT ) ON COMMIT DROP;
INSERT INTO temp_source_15 VALUES (115, 15, 1, '2024-07-01', '2025-07-01', 'Replaced', 55, 'Overlaps replace');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_15 ORDER BY row_id;

-- Run merge
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_15',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_REPLACE',
    era_name => 'valid'
);
-- Verify plan
\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{115}'::INT[], 'UPDATE'::sql_saga.temporal_merge_plan_action, '{"id": 15}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2024-07-01'::DATE, '{"name": "Original", "employees": 50, "legal_unit_id": 1, "edit_comment": "Original slice"}'::JSONB, 'starts'::sql_saga.allen_interval_relation),
    (2, '{115}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 15}'::JSONB, NULL::DATE, '2024-07-01'::DATE, '2025-07-01'::DATE, '{"name": "Replaced", "employees": 55, "legal_unit_id": 1, "edit_comment": "Overlaps replace"}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);
\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
-- Verify feedback
\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (115, '[{"id": 15}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);
\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;
-- Verify final state
\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (15, 1, '2024-01-01'::DATE, '2024-07-01'::DATE, 'Original', 50, 'Original slice'),
    (15, 1, '2024-07-01'::DATE, '2025-07-01'::DATE, 'Replaced', 55, 'Overlaps replace')
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);
\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 15 ORDER BY valid_from;
DROP TABLE temp_source_15;

--------------------------------------------------------------------------------
\echo 'Scenario 16: MERGE_ENTITY_REPLACE with equals relation'
\echo 'Mode: MERGE_ENTITY_REPLACE'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
INSERT INTO temporal_merge_test.establishment (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment) VALUES (16, 1, '2024-01-01', '2025-01-01', 'Original', 110, 'Original slice');
-- Source data
CREATE TEMP TABLE temp_source_16 ( row_id INT, id INT, legal_unit_id INT, valid_from DATE, valid_until DATE, name TEXT, employees INT, edit_comment TEXT ) ON COMMIT DROP;
INSERT INTO temp_source_16 VALUES (116, 16, 1, '2024-01-01', '2025-01-01', 'Replaced', 115, 'Equals replace');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_16 ORDER BY row_id;

-- Run merge
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_16',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_REPLACE',
    era_name => 'valid'
);
-- Verify plan
\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{116}'::INT[], 'UPDATE'::sql_saga.temporal_merge_plan_action, '{"id": 16}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": "Replaced", "employees": 115, "legal_unit_id": 1, "edit_comment": "Equals replace"}'::JSONB, 'equals'::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);
\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
-- Verify feedback
\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (116, '[{"id": 16}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);
\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;
-- Verify final state
\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (16, 1, '2024-01-01'::DATE, '2025-01-01'::DATE, 'Replaced', 115, 'Equals replace')
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);
\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 16 ORDER BY valid_from;
DROP TABLE temp_source_16;

\echo 'Scenario 17: `MERGE_ENTITY_REPLACE` with `equals` relation (Source NULL replaces existing value)'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
INSERT INTO temporal_merge_test.establishment (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment) VALUES (17, 1, '2024-01-01', '2025-01-01', 'Old Name', 10, 'Old Comment');
CREATE TEMP TABLE temp_source_17 (
    row_id INT, id INT, legal_unit_id INT, valid_from DATE NOT NULL, valid_until DATE NOT NULL, name TEXT, edit_comment TEXT, employees INT
) ON COMMIT DROP;
INSERT INTO temp_source_17 VALUES (117, 17, 1, '2024-01-01', '2025-01-01', NULL, 'Replaced with NULL', NULL);

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_17 ORDER BY row_id;

-- Run the orchestrator and store its feedback
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_17',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_REPLACE',
    era_name => 'valid'
);

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{117}'::INT[], 'UPDATE'::sql_saga.temporal_merge_plan_action, '{"id": 17}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": null, "employees": null, "legal_unit_id": 1, "edit_comment": "Replaced with NULL"}'::JSONB, 'equals'::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (117, '[{"id": 17}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (17, 1, '2024-01-01'::DATE, '2025-01-01'::DATE, NULL::TEXT, NULL::INT, 'Replaced with NULL'::TEXT)
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 17 ORDER BY valid_from;

DROP TABLE temp_source_17;

--------------------------------------------------------------------------------
\echo 'Begin Scenarios for _only modes (Filtering Behavior)'
--------------------------------------------------------------------------------
\echo 'Scenario 18: `PATCH_FOR_PORTION_OF` on non-existent entity'
\echo 'Mode: PATCH_FOR_PORTION_OF'
--------------------------------------------------------------------------------
-- Reset state
CALL temporal_merge_test.reset_target();
-- Source data
CREATE TEMP TABLE temp_source_18 ( row_id INT, id INT, legal_unit_id INT, valid_from DATE, valid_until DATE, name TEXT, employees INT, edit_comment TEXT ) ON COMMIT DROP;
INSERT INTO temp_source_18 VALUES (118, 18, 1, '2024-01-01', '2025-01-01', 'Should not be inserted', 10, 'Patch only comment');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_18 ORDER BY row_id;

-- Run merge
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_18',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'PATCH_FOR_PORTION_OF',
    era_name => 'valid'
);
SELECT source_row_id, target_entity_ids, status, error_message FROM pg_temp.temporal_merge_feedback;
SELECT source_row_id, target_entity_ids, status, error_message FROM pg_temp.temporal_merge_feedback;
-- Verify plan
\echo '--- Planner: Expected Plan (Empty since not found) ---'
SELECT * FROM pg_temp.temporal_merge_plan WHERE FALSE;
\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
-- Verify feedback
\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (118, '[]'::JSONB, 'SKIPPED_NO_TARGET'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);
\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;
-- Verify final state
\echo '--- Orchestrator: Expected Final State (empty) ---'
\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment;
DROP TABLE temp_source_18;

--------------------------------------------------------------------------------
\echo 'Scenario 19: `REPLACE_FOR_PORTION_OF` on non-existent entity'
\echo 'Mode: REPLACE_FOR_PORTION_OF'
--------------------------------------------------------------------------------
-- Reset state
CALL temporal_merge_test.reset_target();
-- Source data
CREATE TEMP TABLE temp_source_19 ( row_id INT, id INT, legal_unit_id INT, valid_from DATE, valid_until DATE, name TEXT, employees INT, edit_comment TEXT ) ON COMMIT DROP;
INSERT INTO temp_source_19 VALUES (119, 19, 1, '2024-01-01', '2025-01-01', 'Should not be inserted', 10, 'Replace only comment');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_19 ORDER BY row_id;

-- Run merge
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_19',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'REPLACE_FOR_PORTION_OF',
    era_name => 'valid'
);
-- Verify plan
\echo '--- Planner: Expected Plan (Empty since not found) ---'
SELECT * FROM pg_temp.temporal_merge_plan WHERE FALSE;
\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
-- Verify feedback
\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (119, '[]'::JSONB, 'SKIPPED_NO_TARGET'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);
\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;
-- Verify final state
\echo '--- Orchestrator: Expected Final State (empty) ---'
\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment;
DROP TABLE temp_source_19;

--------------------------------------------------------------------------------
\echo 'Begin Scenarios for NULL handling in patch mode'
--------------------------------------------------------------------------------
\echo 'Scenario 20: MERGE_ENTITY_PATCH with NULL source value (should NOT overwrite)'
\echo 'Mode: MERGE_ENTITY_PATCH'
--------------------------------------------------------------------------------
-- Reset state
CALL temporal_merge_test.reset_target();
INSERT INTO temporal_merge_test.establishment (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment) VALUES (20, 1, '2024-01-01', '2025-01-01', 'Original Name', 10, 'Original Comment');
-- Source data
CREATE TEMP TABLE temp_source_20 ( row_id INT, id INT, legal_unit_id INT, valid_from DATE, valid_until DATE, name TEXT, employees INT, edit_comment TEXT ) ON COMMIT DROP;
INSERT INTO temp_source_20 VALUES (120, 20, 1, '2024-01-01', '2025-01-01', NULL, 15, 'Patch with NULL');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_20 ORDER BY row_id;

-- Run merge
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_20',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);
-- Verify plan
\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{120}'::INT[], 'UPDATE'::sql_saga.temporal_merge_plan_action, '{"id": 20}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": "Original Name", "employees": 15, "legal_unit_id": 1, "edit_comment": "Patch with NULL"}'::JSONB, 'equals'::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);
\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
-- Verify feedback
\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (120, '[{"id": 20}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);
\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;
-- Verify final state
\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (20, 1, '2024-01-01'::DATE, '2025-01-01'::DATE, 'Original Name', 15, 'Patch with NULL')
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);
\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 20 ORDER BY valid_from;
DROP TABLE temp_source_20;

--------------------------------------------------------------------------------
\echo 'Scenario 21: `PATCH_FOR_PORTION_OF` with NULL source value (should NOT overwrite)'
\echo 'Mode: PATCH_FOR_PORTION_OF'
--------------------------------------------------------------------------------
-- Reset state
CALL temporal_merge_test.reset_target();
INSERT INTO temporal_merge_test.establishment (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment) VALUES (21, 1, '2024-01-01', '2025-01-01', 'Original Name', 10, 'Original Comment');
-- Source data
CREATE TEMP TABLE temp_source_21 ( row_id INT, id INT, legal_unit_id INT, valid_from DATE, valid_until DATE, name TEXT, employees INT, edit_comment TEXT ) ON COMMIT DROP;
INSERT INTO temp_source_21 VALUES (121, 21, 1, '2024-01-01', '2025-01-01', NULL, 15, 'Patch with NULL');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_21 ORDER BY row_id;

-- Run merge
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_21',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'PATCH_FOR_PORTION_OF',
    era_name => 'valid'
);
-- Verify plan
\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{121}'::INT[], 'UPDATE'::sql_saga.temporal_merge_plan_action, '{"id": 21}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": "Original Name", "employees": 15, "legal_unit_id": 1, "edit_comment": "Patch with NULL"}'::JSONB, 'equals'::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);
\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
-- Verify feedback
\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (121, '[{"id": 21}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);
\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;
-- Verify final state
\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (21, 1, '2024-01-01'::DATE, '2025-01-01'::DATE, 'Original Name', 15, 'Patch with NULL')
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);
\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 21 ORDER BY valid_from;
DROP TABLE temp_source_21;

--------------------------------------------------------------------------------
\echo 'Begin Scenarios for Ephemeral Columns'
--------------------------------------------------------------------------------
\echo 'Scenario 22: MERGE_ENTITY_PATCH with different ephemeral data (should be APPLIED)'
\echo 'Mode: MERGE_ENTITY_PATCH'
--------------------------------------------------------------------------------
-- Reset state
CALL temporal_merge_test.reset_target();
INSERT INTO temporal_merge_test.establishment (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment) VALUES (22, 1, '2024-01-01', '2025-01-01', 'Same', 10, 'Old Comment');
-- Source data
CREATE TEMP TABLE temp_source_22 ( row_id INT, id INT, legal_unit_id INT, valid_from DATE, valid_until DATE, name TEXT, employees INT, edit_comment TEXT ) ON COMMIT DROP;
INSERT INTO temp_source_22 VALUES (122, 22, 1, '2024-01-01', '2025-01-01', 'Same', 10, 'New Comment');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_22 ORDER BY row_id;

-- Run merge
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_22',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);
-- Verify plan
\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{122}'::INT[], 'UPDATE'::sql_saga.temporal_merge_plan_action, '{"id": 22}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": "Same", "employees": 10, "legal_unit_id": 1, "edit_comment": "New Comment"}'::JSONB, 'equals'::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);
\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
-- Verify feedback
\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (122, '[{"id": 22}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);
\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;
-- Verify final state
\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (22, 1, '2024-01-01'::DATE, '2025-01-01'::DATE, 'Same', 10, 'New Comment')
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);
\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 22 ORDER BY valid_from;
DROP TABLE temp_source_22;

--------------------------------------------------------------------------------
\echo 'Scenario 23: MERGE_ENTITY_PATCH with identical data including ephemeral (should be SKIPPED)'
\echo 'Mode: MERGE_ENTITY_PATCH'
--------------------------------------------------------------------------------
-- Reset state
CALL temporal_merge_test.reset_target();
INSERT INTO temporal_merge_test.establishment (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment) VALUES (23, 1, '2024-01-01', '2025-01-01', 'Same', 10, 'Same Comment');
-- Source data
CREATE TEMP TABLE temp_source_23 ( row_id INT, id INT, legal_unit_id INT, valid_from DATE, valid_until DATE, name TEXT, employees INT, edit_comment TEXT ) ON COMMIT DROP;
INSERT INTO temp_source_23 VALUES (123, 23, 1, '2024-01-01', '2025-01-01', 'Same', 10, 'Same Comment');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_23 ORDER BY row_id;

-- Run merge
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_23',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);
-- Verify plan
\echo '--- Planner: Expected Plan (a single IDENTICAL operation) ---'
SELECT * FROM (VALUES
    (1, '{123}'::INT[], 'SKIP_IDENTICAL'::sql_saga.temporal_merge_plan_action, '{"id": 23}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": "Same", "employees": 10, "edit_comment": "Same Comment", "legal_unit_id": 1}'::JSONB, 'equals'::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);
\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
-- Verify feedback
\echo '--- Orchestrator: Expected Feedback (SKIPPED because data is identical) ---'
SELECT * FROM (VALUES (123, '[]'::JSONB, 'SKIPPED_IDENTICAL'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);
\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;
-- Verify final state
\echo '--- Orchestrator: Expected Final State (unchanged) ---'
SELECT * FROM (VALUES
    (23, 1, '2024-01-01'::DATE, '2025-01-01'::DATE, 'Same', 10, 'Same Comment')
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);
\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 23 ORDER BY valid_from;
DROP TABLE temp_source_23;

--------------------------------------------------------------------------------
\echo 'Begin Scenarios for Multi-Row Source Data'
--------------------------------------------------------------------------------
\echo 'Scenario 24: MERGE_ENTITY_PATCH with multiple disjoint source rows'
\echo 'Mode: MERGE_ENTITY_PATCH'
--------------------------------------------------------------------------------
-- Reset state
CALL temporal_merge_test.reset_target();
INSERT INTO temporal_merge_test.establishment (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment) VALUES (24, 1, '2024-01-01', '2028-01-01', 'Original', 10, 'Original slice');
-- Source data: two separate, non-contiguous patches
CREATE TEMP TABLE temp_source_24 ( row_id INT, id INT, legal_unit_id INT, valid_from DATE, valid_until DATE, name TEXT, employees INT, edit_comment TEXT ) ON COMMIT DROP;
INSERT INTO temp_source_24 VALUES
(124, 24, 1, '2025-01-01', '2026-01-01', 'Patch 1', 15, 'First patch'),
(125, 24, 1, '2027-01-01', '2028-01-01', 'Patch 2', 20, 'Second patch');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_24 ORDER BY row_id;

-- Run merge
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_24',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);
-- Verify plan
\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{124}'::INT[], 'UPDATE'::sql_saga.temporal_merge_plan_action, '{"id": 24}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": "Original", "employees": 10, "legal_unit_id": 1, "edit_comment": "Original slice"}'::JSONB, 'starts'::sql_saga.allen_interval_relation),
    (2, '{124}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 24}'::JSONB, NULL::DATE, '2025-01-01'::DATE, '2026-01-01'::DATE, '{"name": "Patch 1", "employees": 15, "legal_unit_id": 1, "edit_comment": "First patch"}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (3, '{125}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 24}'::JSONB, NULL::DATE, '2026-01-01'::DATE, '2027-01-01'::DATE, '{"name": "Original", "employees": 10, "legal_unit_id": 1, "edit_comment": "Original slice"}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (4, '{125}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 24}'::JSONB, NULL::DATE, '2027-01-01'::DATE, '2028-01-01'::DATE, '{"name": "Patch 2", "employees": 20, "legal_unit_id": 1, "edit_comment": "Second patch"}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);
\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
-- Verify feedback
\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES
    (124, '[{"id": 24}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT),
    (125, '[{"id": 24}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)
) AS t (source_row_id, target_entity_ids, status, error_message) ORDER BY source_row_id;
\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id;
-- Verify final state
\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (24, 1, '2024-01-01'::DATE, '2025-01-01'::DATE, 'Original', 10, 'Original slice'),
    (24, 1, '2025-01-01'::DATE, '2026-01-01'::DATE, 'Patch 1', 15, 'First patch'),
    (24, 1, '2026-01-01'::DATE, '2027-01-01'::DATE, 'Original', 10, 'Original slice'),
    (24, 1, '2027-01-01'::DATE, '2028-01-01'::DATE, 'Patch 2', 20, 'Second patch')
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);
\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 24 ORDER BY valid_from;
DROP TABLE temp_source_24;


--------------------------------------------------------------------------------
\echo 'Scenario 35: `SAVEPOINT` test demonstrating necessity of `process_*` call ordering'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
CREATE TEMP TABLE temp_source_35 ( row_id INT, legal_unit_id INT, id INT, valid_from DATE NOT NULL, valid_until DATE NOT NULL, name TEXT, employees INT, edit_comment TEXT ) ON COMMIT DROP;
SAVEPOINT before_wrong_order;

\echo '--- Stage 1: Prove that `patch_only` before `MERGE_ENTITY_PATCH` is a non-operative action ---'
INSERT INTO temp_source_35 VALUES (301, 1, 3, '2022-01-01', '2023-01-01', 'NewCo UPDATE', 15, 'Should not be inserted');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_35 ORDER BY row_id;

\echo '--- Orchestrator: Calling with `patch_only` on non-existent entity... ---'
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_35',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'PATCH_FOR_PORTION_OF',
    era_name => 'valid'
);
\echo '--- Orchestrator: Expected Feedback (MISSING_TARGET) ---'
SELECT * FROM (VALUES (301, '[]'::JSONB, 'SKIPPED_NO_TARGET'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);
\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;
\echo '--- Orchestrator: Final state of target table (expected empty, proving data loss) ---'
SELECT 0 as row_count WHERE NOT EXISTS (SELECT 1 FROM temporal_merge_test.establishment WHERE id = 3);
\echo '--- Orchestrator: Actual state of target table ---'
SELECT count(*) as row_count FROM temporal_merge_test.establishment WHERE id = 3;
ROLLBACK TO SAVEPOINT before_wrong_order;

\echo '--- Stage 2: Prove that `MERGE_ENTITY_PATCH`-then-`patch_only` succeeds ---'
\echo '--- Orchestrator: Calling with `MERGE_ENTITY_PATCH`... ---'
TRUNCATE temp_source_35;
INSERT INTO temp_source_35 VALUES (301, 1, 3, '2021-01-01', '2022-01-01', 'NewCo INSERT', 10, 'Initial Insert');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_35 ORDER BY row_id;

CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_35',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);
\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (301, '[{"id": 3}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);
\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;

\echo '--- Orchestrator: Calling with `patch_only`... ---'
TRUNCATE temp_source_35;
INSERT INTO temp_source_35 VALUES (302, 1, 3, '2022-01-01', '2023-01-01', NULL, 15, 'Successful Update');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_35 ORDER BY row_id;

CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_35',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'PATCH_FOR_PORTION_OF',
    era_name => 'valid'
);
\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (302, '[{"id": 3}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);
\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;

\echo '--- Orchestrator: Final state of target table (expected complete history) ---'
SELECT * FROM (VALUES
    (3, 1, '2021-01-01'::DATE, '2022-01-01'::DATE, 'NewCo INSERT'::TEXT, 10, 'Initial Insert'::TEXT),
    (3, 1, '2022-01-01'::DATE, '2023-01-01'::DATE, 'NewCo INSERT'::TEXT, 15, 'Successful Update'::TEXT)
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);
\echo '--- Orchestrator: Actual state of target table ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 3 ORDER BY valid_from;
RELEASE SAVEPOINT before_wrong_order;
DROP TABLE temp_source_35;

\echo '--- Orchestrator: Expected Final State (after successful run) ---'
SELECT 2 AS row_count;
\echo '--- Orchestrator: Actual Final State ---'
SELECT count(*) AS row_count FROM temporal_merge_test.establishment WHERE id = 3;

--------------------------------------------------------------------------------
-- Scenarios for Merging and Coalescing
--------------------------------------------------------------------------------
\echo '================================================================================'
\echo 'Begin Scenarios for Merging and Coalescing'
\echo '================================================================================'

--------------------------------------------------------------------------------
\echo 'Scenario 36: `MERGE_ENTITY_PATCH` with consecutive, identical source rows (should merge into one operation)'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
CREATE TEMP TABLE temp_source_36 (
    row_id INT, legal_unit_id INT, id INT, valid_from DATE NOT NULL, valid_until DATE NOT NULL, name TEXT, employees INT
) ON COMMIT DROP;
-- Two source rows, contiguous in time, with identical data.
INSERT INTO temp_source_36 VALUES
(401, 1, 4, '2023-01-02', '2023-07-01', 'Continuous Op', 20),
(402, 1, 4, '2023-07-01', '2024-01-01', 'Continuous Op', 20);

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_36 ORDER BY row_id;

-- Run the orchestrator and store its feedback
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_36',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);

\echo '--- Planner: Expected Plan (A single INSERT for the full period) ---'
SELECT * FROM (VALUES
    (1, '{401, 402}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 4}'::JSONB, NULL::DATE, '2023-01-02'::DATE, '2024-01-01'::DATE, '{"name": "Continuous Op", "employees": 20, "legal_unit_id": 1}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES
    (401, '[{"id": 4}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT),
    (402, '[{"id": 4}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)
) AS t (source_row_id, target_entity_ids, status, error_message) ORDER BY source_row_id;

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id;

\echo '--- Orchestrator: Expected Final State (A single merged row) ---'
SELECT * FROM (VALUES
    (4, 1, '2023-01-02'::DATE, '2024-01-01'::DATE, 'Continuous Op'::TEXT, 20, NULL::TEXT)
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 4 ORDER BY valid_from;

DROP TABLE temp_source_36;

--------------------------------------------------------------------------------
\echo 'Scenario 37: `MERGE_ENTITY_PATCH` with three consecutive, identical source rows (should merge)'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
CREATE TEMP TABLE temp_source_37 (
    row_id INT, legal_unit_id INT, id INT, valid_from DATE NOT NULL, valid_until DATE NOT NULL, name TEXT, employees INT
) ON COMMIT DROP;
INSERT INTO temp_source_37 VALUES
(501, 1, 5, '2023-01-02', '2023-04-01', 'Three-part Op', 30),
(502, 1, 5, '2023-04-01', '2023-07-01', 'Three-part Op', 30),
(503, 1, 5, '2023-07-01', '2023-10-01', 'Three-part Op', 30);

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_37 ORDER BY row_id;

-- Run the orchestrator and store its feedback
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_37',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);

\echo '--- Planner: Expected Plan (A single INSERT for the full period) ---'
SELECT * FROM (VALUES
    (1, '{501, 502, 503}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 5}'::JSONB, NULL::DATE, '2023-01-02'::DATE, '2023-10-01'::DATE, '{"name": "Three-part Op", "employees": 30, "legal_unit_id": 1}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES
    (501, '[{"id": 5}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT),
    (502, '[{"id": 5}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT),
    (503, '[{"id": 5}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)
) AS t (source_row_id, target_entity_ids, status, error_message) ORDER BY source_row_id;

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (5, 1, '2023-01-02'::DATE, '2023-10-01'::DATE, 'Three-part Op'::TEXT, 30, NULL::TEXT)
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 5 ORDER BY valid_from;
DROP TABLE temp_source_37;

--------------------------------------------------------------------------------
\echo 'Scenario 38: `MERGE_ENTITY_PATCH` with two consecutive but DIFFERENT source rows (should NOT merge)'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
CREATE TEMP TABLE temp_source_38 (
    row_id INT, legal_unit_id INT, id INT, valid_from DATE NOT NULL, valid_until DATE NOT NULL, name TEXT, employees INT
) ON COMMIT DROP;
INSERT INTO temp_source_38 VALUES
(601, 1, 6, '2023-01-02', '2023-07-01', 'First Part', 40),
(602, 1, 6, '2023-07-01', '2024-01-01', 'Second Part', 50);

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_38 ORDER BY row_id;

-- Run the orchestrator and store its feedback
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_38',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);

\echo '--- Planner: Expected Plan (Two separate INSERTs) ---'
SELECT * FROM (VALUES
    (1, '{601}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 6}'::JSONB, NULL::DATE, '2023-01-02'::DATE, '2023-07-01'::DATE, '{"name": "First Part", "employees": 40, "legal_unit_id": 1}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (2, '{602}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 6}'::JSONB, NULL::DATE, '2023-07-01'::DATE, '2024-01-01'::DATE, '{"name": "Second Part", "employees": 50, "legal_unit_id": 1}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation) ORDER BY plan_op_seq;

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES
    (601, '[{"id": 6}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT),
    (602, '[{"id": 6}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)
) AS t (source_row_id, target_entity_ids, status, error_message) ORDER BY source_row_id;

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (6, 1, '2023-01-02'::DATE, '2023-07-01'::DATE, 'First Part'::TEXT, 40, NULL),
    (6, 1, '2023-07-01'::DATE, '2024-01-01'::DATE, 'Second Part'::TEXT, 50, NULL)
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 6 ORDER BY valid_from;
DROP TABLE temp_source_38;

--------------------------------------------------------------------------------
\echo 'Scenario 39: `MERGE_ENTITY_PATCH` where source row is consecutive with existing target row (should merge/extend)'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
INSERT INTO temporal_merge_test.establishment (id, legal_unit_id, valid_from, valid_until, name, employees) VALUES (7, 1, '2023-01-01', '2023-07-01', 'Existing Op', 60);
CREATE TEMP TABLE temp_source_39 (
    row_id INT, legal_unit_id INT, id INT, valid_from DATE NOT NULL, valid_until DATE NOT NULL, name TEXT, employees INT
) ON COMMIT DROP;
-- This source row meets the existing target row, with identical data.
INSERT INTO temp_source_39 VALUES (701, 1, 7, '2023-07-01', '2024-01-01', 'Existing Op', 60);

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_39 ORDER BY row_id;

-- Run the orchestrator and store its feedback
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_39',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);

\echo '--- Planner: Expected Plan (A single UPDATE extending the target row) ---'
SELECT * FROM (VALUES
    (1, '{701}'::INT[], 'UPDATE'::sql_saga.temporal_merge_plan_action, '{"id": 7}'::JSONB, '2023-01-01'::DATE, '2023-01-01'::DATE, '2024-01-01'::DATE, '{"name": "Existing Op", "employees": 60, "edit_comment": null, "legal_unit_id": 1}'::JSONB, 'started by'::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (701, '[{"id": 7}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;

\echo '--- Orchestrator: Expected Final State (A single merged row) ---'
SELECT * FROM (VALUES
    (7, 1, '2023-01-01'::DATE, '2024-01-01'::DATE, 'Existing Op'::TEXT, 60, NULL::TEXT)
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment);

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment WHERE id = 7 ORDER BY valid_from;

DROP TABLE temp_source_39;

-- Final Cleanup before independent tests.
DROP PROCEDURE temporal_merge_test.reset_target();
DROP TABLE temporal_merge_test.establishment;
DROP TABLE temporal_merge_test.legal_unit;
DROP SEQUENCE temporal_merge_test.establishment_id_seq;
DROP SEQUENCE temporal_merge_test.legal_unit_id_seq;
DROP SCHEMA temporal_merge_test CASCADE;

SET client_min_messages TO NOTICE;
ROLLBACK;

BEGIN;
SET client_min_messages TO WARNING;

-- Test schema
CREATE SCHEMA temporal_merge_test;

-- Sequences for auto-generated IDs
CREATE SEQUENCE temporal_merge_test.legal_unit_id_seq;
CREATE SEQUENCE temporal_merge_test.establishment_id_seq;

-- Target tables (simplified versions for testing)
CREATE TABLE temporal_merge_test.legal_unit (
    id INT PRIMARY KEY,
    name TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE temporal_merge_test.establishment (
    id INT NOT NULL,
    legal_unit_id INT NOT NULL,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    name TEXT,
    employees INT,
    edit_comment TEXT,
    PRIMARY KEY (id, valid_from)
);
SELECT sql_saga.add_era('temporal_merge_test.establishment', 'valid_from', 'valid_until');

-- Helper procedure to reset target table state between scenarios
CREATE PROCEDURE temporal_merge_test.reset_target() LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE temporal_merge_test.establishment;
    TRUNCATE temporal_merge_test.legal_unit;
    -- Seed with a legal unit for FK constraints
    INSERT INTO temporal_merge_test.legal_unit (id, name) VALUES (1, 'Test LU');
    ALTER SEQUENCE temporal_merge_test.establishment_id_seq RESTART WITH 1;
END;
$$;

-- psql variables for the test
\set target_schema 'temporal_merge_test'
\set target_table 'temporal_merge_test.establishment'
\set source_schema 'pg_temp'
\set entity_id_cols '{id}'

\set ephemeral_cols '{edit_comment}'

--------------------------------------------------------------------------------
\echo 'Scenario 40: `INSERT` with `created_at` defaulted'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
CREATE TEMP TABLE temp_source_40 (
    row_id INT, legal_unit_id INT, id INT, valid_from DATE NOT NULL, valid_until DATE NOT NULL, name TEXT, employees INT, edit_comment TEXT
) ON COMMIT DROP;
INSERT INTO temp_source_40 VALUES (801, 1, 40, '2024-01-01', '2025-01-01', 'Default Test', 10, 'Default Insert');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_40 ORDER BY row_id;

-- Run the orchestrator and store its feedback
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_40',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);

\echo '--- Planner: Expected Plan ---'
-- The planner should NOT include `created_at` in the data payload, allowing the DB default to apply.
SELECT * FROM (VALUES
    (1, '{801}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 40}'::JSONB, NULL::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": "Default Test", "employees": 10, "edit_comment": "Default Insert", "legal_unit_id": 1}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (801, '[{"id": 40}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;

\echo '--- Orchestrator: Expected Final State (created_at should NOT be null) ---'
-- We only check that created_at is not null, as the exact time is non-deterministic.
SELECT 1 AS row_count WHERE EXISTS (SELECT 1 FROM temporal_merge_test.establishment WHERE id = 40 AND created_at IS NOT NULL);

\echo '--- Orchestrator: Actual Final State ---'
SELECT count(*)::INT AS row_count FROM temporal_merge_test.establishment WHERE id = 40 AND created_at IS NOT NULL;

DROP TABLE temp_source_40;

-- Final Cleanup
DROP PROCEDURE temporal_merge_test.reset_target();
DROP TABLE temporal_merge_test.establishment;
DROP TABLE temporal_merge_test.legal_unit;
DROP SEQUENCE temporal_merge_test.establishment_id_seq;
DROP SEQUENCE temporal_merge_test.legal_unit_id_seq;
DROP SCHEMA temporal_merge_test CASCADE;

SET client_min_messages TO NOTICE;
ROLLBACK;

BEGIN;
SET client_min_messages TO WARNING;

-- Test schema
CREATE SCHEMA temporal_merge_test_vt;

-- Target table with valid_to and a trigger to sync valid_until
CREATE TABLE temporal_merge_test_vt.test_target (
    id INT NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE, -- Inclusive end date
    valid_until DATE NOT NULL,
    name TEXT,
    PRIMARY KEY (id, valid_from)
);

SELECT sql_saga.add_era('temporal_merge_test_vt.test_target', 'valid_from', 'valid_until', synchronize_valid_to_column := 'valid_to');

-- Helper procedure
CREATE PROCEDURE temporal_merge_test_vt.reset_target() LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE temporal_merge_test_vt.test_target;
END;
$$;

-- psql variables
\set target_schema 'temporal_merge_test_vt'
\set target_table 'temporal_merge_test_vt.test_target'
\set source_schema 'pg_temp'
\set entity_id_cols '{id}'
\set ephemeral_cols '{}'

--------------------------------------------------------------------------------
\echo 'Scenario 41: `MERGE_ENTITY_PATCH` on a table using `synchronize_valid_to_until` trigger'
--------------------------------------------------------------------------------
CALL temporal_merge_test_vt.reset_target();
-- Initial data in target. `valid_until` is set by the trigger.
INSERT INTO temporal_merge_test_vt.test_target (id, valid_from, valid_to, name) VALUES (1, '2024-01-01', '2025-12-31', 'Original');

CREATE TEMP TABLE temp_source_41 (
    row_id INT, id INT, valid_from DATE NOT NULL, valid_until DATE NOT NULL, valid_to DATE, name TEXT
) ON COMMIT DROP;
-- Source data for the merge. valid_until must be provided to the planner.
INSERT INTO temp_source_41 VALUES (901, 1, '2024-01-01', '2025-01-01', '2024-12-31', 'Patched');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, valid_from, valid_to, valid_until, name FROM temporal_merge_test_vt.test_target ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_41 ORDER BY row_id;

-- Run the orchestrator and store its feedback
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_41',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{901}'::INT[], 'UPDATE'::sql_saga.temporal_merge_plan_action, '{"id": 1}'::JSONB, '2024-01-01'::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": "Patched", "valid_to": "2024-12-31"}'::JSONB, 'starts'::sql_saga.allen_interval_relation),
    (2, '{901}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 1}'::JSONB, NULL::DATE, '2025-01-01'::DATE, '2026-01-01'::DATE, '{"name": "Original", "valid_to": "2025-12-31"}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES (901, '[{"id": 1}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;

\echo '--- Orchestrator: Expected Final State (valid_until should be correct for both segments) ---'
SELECT * FROM (VALUES
    (1, '2024-01-01'::DATE, '2024-12-31'::DATE, '2025-01-01'::DATE, 'Patched'),
    (1, '2025-01-01'::DATE, '2025-12-31'::DATE, '2026-01-01'::DATE, 'Original')
) AS t (id, valid_from, valid_to, valid_until, name);

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, valid_from, valid_to, valid_until, name FROM temporal_merge_test_vt.test_target WHERE id = 1 ORDER BY valid_from;

DROP TABLE temp_source_41;

-- Final Cleanup
DROP PROCEDURE temporal_merge_test_vt.reset_target();
DROP TABLE temporal_merge_test_vt.test_target;
DROP SCHEMA temporal_merge_test_vt CASCADE;

SET client_min_messages TO NOTICE;
ROLLBACK;


BEGIN;
SET client_min_messages TO WARNING;

-- Test schema for multi-entity batch test
CREATE SCHEMA temporal_merge_test_me;

-- Target table
CREATE TABLE temporal_merge_test_me.test_target (
    id INT NOT NULL,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL,
    status TEXT,
    PRIMARY KEY (id, valid_from)
);
SELECT sql_saga.add_era('temporal_merge_test_me.test_target', 'valid_from', 'valid_until');

-- Helper procedure
CREATE PROCEDURE temporal_merge_test_me.reset_target() LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE temporal_merge_test_me.test_target;
END;
$$;

-- psql variables
\set target_schema 'temporal_merge_test_me'
\set target_table 'temporal_merge_test_me.test_target'
\set source_schema 'pg_temp'
\set entity_id_cols '{id}'
\set ephemeral_cols '{}'

--------------------------------------------------------------------------------
\echo 'Scenario 42: Multi-entity batch with status change (Bug Reproduction for #106)'
--------------------------------------------------------------------------------
CREATE TEMP TABLE temp_source_42 (
    row_id INT, id INT, valid_from DATE NOT NULL, valid_until DATE NOT NULL, status TEXT
) ON COMMIT DROP;
-- Entity 1 ("Oslo"): single continuous record
INSERT INTO temp_source_42 VALUES (101, 1, '2010-01-01', 'infinity', 'active');
-- Entity 2 ("Omegn"): contiguous records with a status change
INSERT INTO temp_source_42 VALUES (102, 2, '2010-01-01', '2011-01-01', 'active');
INSERT INTO temp_source_42 VALUES (103, 2, '2011-01-01', 'infinity', 'passive');

-- Run the orchestrator and store its feedback
CALL temporal_merge_test_me.reset_target();

\echo '--- Target: Initial State (before merge) ---'
SELECT id, valid_from, valid_until, status FROM temporal_merge_test_me.test_target ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_42 ORDER BY row_id;

CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_42',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_REPLACE',
    era_name => 'valid'
);

\echo '--- Planner: Expected Plan (Entity 1 should have one INSERT, Entity 2 should have two) ---'
SELECT * FROM (VALUES
    (1, '{101}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 1}'::JSONB, NULL::DATE, '2010-01-01'::DATE, 'infinity'::DATE, '{"status": "active"}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (2, '{102}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 2}'::JSONB, NULL::DATE, '2010-01-01'::DATE, '2011-01-01'::DATE, '{"status": "active"}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (3, '{103}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 2}'::JSONB, NULL::DATE, '2011-01-01'::DATE, 'infinity'::DATE, '{"status": "passive"}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation)
ORDER BY (entity_ids->>'id')::INT, new_valid_from;

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY (entity_ids->>'id')::INT, new_valid_from;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES
    (101, '[{"id": 1}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT),
    (102, '[{"id": 2}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT),
    (103, '[{"id": 2}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)
) AS t (source_row_id, target_entity_ids, status, error_message) ORDER BY source_row_id;

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id;

\echo '--- Orchestrator: Expected Final State (Entity 1 should NOT be split) ---'
SELECT * FROM (VALUES
    (1, '2010-01-01'::DATE, 'infinity'::DATE, 'active'),
    (2, '2010-01-01'::DATE, '2011-01-01'::DATE, 'active'),
    (2, '2011-01-01'::DATE, 'infinity'::DATE, 'passive')
) AS t (id, valid_from, valid_until, status) ORDER BY id, valid_from;

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, valid_from, valid_until, status FROM temporal_merge_test_me.test_target ORDER BY id, valid_from;

DROP TABLE temp_source_42;
DROP PROCEDURE temporal_merge_test_me.reset_target();
DROP TABLE temporal_merge_test_me.test_target;
DROP SCHEMA temporal_merge_test_me CASCADE;

SET client_min_messages TO NOTICE;
ROLLBACK;

BEGIN;
SET client_min_messages TO WARNING;

-- Test schema
CREATE SCHEMA temporal_merge_test;

-- Sequences for auto-generated IDs
CREATE SEQUENCE temporal_merge_test.legal_unit_id_seq;
CREATE SEQUENCE temporal_merge_test.establishment_id_seq;

-- Target tables (simplified versions for testing)
CREATE TABLE temporal_merge_test.legal_unit (
    id INT PRIMARY KEY,
    name TEXT
);

CREATE TABLE temporal_merge_test.establishment (
    id INT NOT NULL,
    legal_unit_id INT NOT NULL,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL,
    name TEXT,
    employees INT,
    edit_comment TEXT,
    PRIMARY KEY (id, valid_from)
);
SELECT sql_saga.add_era('temporal_merge_test.establishment', 'valid_from', 'valid_until');

-- Helper procedure to reset target table state between scenarios
CREATE PROCEDURE temporal_merge_test.reset_target() LANGUAGE plpgsql AS $$
BEGIN
    TRUNCATE temporal_merge_test.establishment;
    TRUNCATE temporal_merge_test.legal_unit;
    -- Seed with a legal unit for FK constraints
    INSERT INTO temporal_merge_test.legal_unit (id, name) VALUES (1, 'Test LU');
    ALTER SEQUENCE temporal_merge_test.establishment_id_seq RESTART WITH 1;
END;
$$;

-- psql variables for the test
\set target_schema 'temporal_merge_test'
\set target_table 'temporal_merge_test.establishment'
\set source_schema 'pg_temp'
\set entity_id_cols '{id}'
\set ephemeral_cols '{edit_comment}'
--------------------------------------------------------------------------------
\echo 'Scenario 43: Multi-entity batch with data change (Realistic reproduction of #106)'
--------------------------------------------------------------------------------
CALL temporal_merge_test.reset_target();
CREATE TEMP TABLE temp_source_43 (
    row_id INT, id INT, legal_unit_id INT, valid_from DATE NOT NULL, valid_until DATE NOT NULL, name TEXT, employees INT, edit_comment TEXT
) ON COMMIT DROP;
-- Entity 10 ("Continuous"): single continuous record
INSERT INTO temp_source_43 VALUES (1001, 10, 1, '2010-01-01', 'infinity', 'Continuous', 50, 'comment');
-- Entity 20 ("Changes"): contiguous records with a data change
INSERT INTO temp_source_43 VALUES (1002, 20, 1, '2010-01-01', '2011-01-01', 'Changes', 100, 'comment');
INSERT INTO temp_source_43 VALUES (1003, 20, 1, '2011-01-01', 'infinity',   'Changes', 150, 'comment');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_43 ORDER BY row_id;

-- Run the orchestrator and store its feedback
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_43',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_PATCH',
    era_name => 'valid'
);

\echo '--- Planner: Expected Plan (Entity 10 should have one INSERT, Entity 20 should have two) ---'
SELECT * FROM (VALUES
    (1, '{1001}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 10}'::JSONB, NULL::DATE, '2010-01-01'::DATE, 'infinity'::DATE,   '{"name": "Continuous", "employees": 50, "edit_comment": "comment", "legal_unit_id": 1}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (2, '{1002}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 20}'::JSONB, NULL::DATE, '2010-01-01'::DATE, '2011-01-01'::DATE, '{"name": "Changes", "employees": 100, "edit_comment": "comment", "legal_unit_id": 1}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (3, '{1003}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": 20}'::JSONB, NULL::DATE, '2011-01-01'::DATE, 'infinity'::DATE,   '{"name": "Changes", "employees": 150, "edit_comment": "comment", "legal_unit_id": 1}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation)
ORDER BY (entity_ids->>'id')::INT, new_valid_from;

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY (entity_ids->>'id')::INT, new_valid_from;

\echo '--- Orchestrator: Expected Feedback ---'
SELECT * FROM (VALUES
    (1001, '[{"id": 10}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT),
    (1002, '[{"id": 20}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT),
    (1003, '[{"id": 20}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)
) AS t (source_row_id, target_entity_ids, status, error_message) ORDER BY source_row_id;

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id;

\echo '--- Orchestrator: Expected Final State (Entity 10 should NOT be split) ---'
SELECT * FROM (VALUES
    (10, 1, '2010-01-01'::DATE, 'infinity'::DATE,   'Continuous', 50, 'comment'),
    (20, 1, '2010-01-01'::DATE, '2011-01-01'::DATE, 'Changes',    100, 'comment'),
    (20, 1, '2011-01-01'::DATE, 'infinity'::DATE,   'Changes',    150, 'comment')
) AS t (id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment) ORDER BY id, valid_from;

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, legal_unit_id, valid_from, valid_until, name, employees, edit_comment FROM temporal_merge_test.establishment ORDER BY id, valid_from;

DROP TABLE temp_source_43;

-- Final Cleanup
DROP PROCEDURE temporal_merge_test.reset_target();
DROP TABLE temporal_merge_test.establishment;
DROP TABLE temporal_merge_test.legal_unit;
DROP SEQUENCE temporal_merge_test.establishment_id_seq;
DROP SEQUENCE temporal_merge_test.legal_unit_id_seq;
DROP SCHEMA temporal_merge_test CASCADE;

SET client_min_messages TO NOTICE;
ROLLBACK;

BEGIN;
SET client_min_messages TO WARNING;

-- Test schema
CREATE SCHEMA temporal_merge_test_serial;

-- Target table with SERIAL surrogate key
CREATE TABLE temporal_merge_test_serial.test_target (
    id SERIAL PRIMARY KEY,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL,
    name TEXT,
    UNIQUE (id, valid_from)
);
SELECT sql_saga.add_era('temporal_merge_test_serial.test_target', 'valid_from', 'valid_until');

-- psql variables for the test
\set target_schema 'temporal_merge_test_serial'
\set target_table 'temporal_merge_test_serial.test_target'
\set source_schema 'pg_temp'
\set entity_id_cols '{id}'
\set ephemeral_cols '{}'

--------------------------------------------------------------------------------
\echo 'Scenario 44: `INSERT` with SERIAL surrogate key should return the generated ID'
--------------------------------------------------------------------------------
CREATE TEMP TABLE temp_source_44 (
    row_id INT, id INT, valid_from DATE NOT NULL, valid_until DATE NOT NULL, name TEXT
) ON COMMIT DROP;
-- ID is NULL, to be generated by the database
INSERT INTO temp_source_44 VALUES (1001, NULL, '2024-01-01', '2025-01-01', 'Serial Widget');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, valid_from, valid_until, name FROM temporal_merge_test_serial.test_target ORDER BY id, valid_from;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_44 ORDER BY row_id;

-- Run the orchestrator and store its feedback
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_44',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => :'ephemeral_cols'::TEXT[],
    mode => 'MERGE_ENTITY_REPLACE',
    era_name => 'valid',
    update_source_with_identity => true
);

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{1001}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": null, "row_id": "1001"}'::JSONB, NULL::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": "Serial Widget"}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation);

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Orchestrator: Expected Feedback (Should return generated ID 1) ---'
SELECT * FROM (VALUES (1001, '[{"id": 1}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)) AS t (source_row_id, target_entity_ids, status, error_message);

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (1, '2024-01-01'::DATE, '2025-01-01'::DATE, 'Serial Widget'::TEXT)
) AS t (id, valid_from, valid_until, name);

\echo '--- Orchestrator: Actual Final State ---'
SELECT id, valid_from, valid_until, name FROM temporal_merge_test_serial.test_target WHERE id = 1 ORDER BY valid_from;

\echo '--- Source Table: Expected state after back-fill ---'
SELECT * FROM (VALUES (1001, 1, '2024-01-01'::DATE, '2025-01-01'::DATE, 'Serial Widget')) t(row_id, id, valid_from, valid_until, name);
\echo '--- Source Table: Actual state after back-fill ---'
SELECT row_id, id, valid_from, valid_until, name FROM temp_source_44 ORDER BY row_id;

DROP TABLE temp_source_44;

-- Final Cleanup
DROP TABLE temporal_merge_test_serial.test_target;
DROP SCHEMA temporal_merge_test_serial CASCADE;

SET client_min_messages TO NOTICE;
ROLLBACK;

BEGIN;
SET client_min_messages TO WARNING;

-- Use the same schema as Scenario 44 for simplicity
CREATE SCHEMA temporal_merge_test_multi_insert;
CREATE TABLE temporal_merge_test_multi_insert.test_target (
    id SERIAL PRIMARY KEY,
    name TEXT,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL
);
SELECT sql_saga.add_era('temporal_merge_test_multi_insert.test_target', 'valid_from', 'valid_until');

\set target_schema 'temporal_merge_test_multi_insert'
\set target_table 'temporal_merge_test_multi_insert.test_target'
\set source_schema 'pg_temp'
\set entity_id_cols '{id}'

--------------------------------------------------------------------------------
\echo 'Scenario 45: Batch INSERT of multiple new entities'
--------------------------------------------------------------------------------
CREATE TEMP TABLE temp_source_45 (
    row_id INT, id INT, name TEXT, valid_from DATE NOT NULL, valid_until DATE NOT NULL
) ON COMMIT DROP;
-- Source contains two distinct new entities
INSERT INTO temp_source_45 VALUES
(2001, NULL, 'Entity One', '2024-01-01', '2025-01-01'),
(2002, NULL, 'Entity Two', '2024-01-01', '2025-01-01');

\echo '--- Target: Initial State (before merge) ---'
SELECT id, name, valid_from, valid_until FROM temporal_merge_test_multi_insert.test_target ORDER BY id;
\echo '--- Source: Data to be merged ---'
SELECT * FROM temp_source_45 ORDER BY row_id;

-- Run the orchestrator and store its feedback
CALL sql_saga.temporal_merge(
    target_table => :'target_table'::regclass,
    source_table => 'temp_source_45',
    identity_columns => :'entity_id_cols'::TEXT[],
    ephemeral_columns => '{}'::TEXT[],
    mode => 'MERGE_ENTITY_REPLACE',
    era_name => 'valid',
    update_source_with_identity => true
);

\echo '--- Planner: Expected Plan ---'
SELECT * FROM (VALUES
    (1, '{2001}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": null, "row_id": "2001"}'::JSONB, NULL::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": "Entity One"}'::JSONB, NULL::sql_saga.allen_interval_relation),
    (2, '{2002}'::INT[], 'INSERT'::sql_saga.temporal_merge_plan_action, '{"id": null, "row_id": "2002"}'::JSONB, NULL::DATE, '2024-01-01'::DATE, '2025-01-01'::DATE, '{"name": "Entity Two"}'::JSONB, NULL::sql_saga.allen_interval_relation)
) AS t (plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation) ORDER BY (data->>'name');

\echo '--- Planner: Actual Plan (from Orchestrator) ---'
SELECT plan_op_seq, source_row_ids, operation, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY (data->>'name');

\echo '--- Orchestrator: Expected Feedback (One distinct result per source row) ---'
SELECT * FROM (VALUES
    (2001, '[{"id": 1}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT),
    (2002, '[{"id": 2}]'::JSONB, 'APPLIED'::sql_saga.temporal_merge_feedback_status, NULL::TEXT)
) AS t (source_row_id, target_entity_ids, status, error_message) ORDER BY source_row_id;

\echo '--- Orchestrator: Actual Feedback ---'
SELECT * FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id;

\echo '--- Orchestrator: Expected Final State ---'
SELECT * FROM (VALUES
    (1, 'Entity One', '2024-01-01'::DATE, '2025-01-01'::DATE),
    (2, 'Entity Two', '2024-01-01'::DATE, '2025-01-01'::DATE)
) AS t (id, name, valid_from, valid_until);
\echo '--- Orchestrator: Actual Final State ---'
SELECT id, name, valid_from, valid_until FROM temporal_merge_test_multi_insert.test_target ORDER BY id;

\echo '--- Source Table: Expected state after back-fill ---'
SELECT * FROM (VALUES
    (2001, 1, 'Entity One', '2024-01-01'::DATE, '2025-01-01'::DATE),
    (2002, 2, 'Entity Two', '2024-01-01'::DATE, '2025-01-01'::DATE)
) t(row_id, id, name, valid_from, valid_until) ORDER BY row_id;
\echo '--- Source Table: Actual state after back-fill ---'
SELECT * FROM temp_source_45 ORDER BY row_id;
DROP TABLE temp_source_45;

--------------------------------------------------------------------------------
\echo 'Scenario 46: `MERGE_ENTITY_PATCH` correctly carries over NOT NULL columns when splitting'
--------------------------------------------------------------------------------
-- This test is a variant of the bug found in `PATCH_FOR_PORTION_OF`.
-- It ensures that `MERGE_ENTITY_PATCH` also correctly inherits values for
-- columns that are NOT NULL on the target but absent from the source when
-- a new historical slice is created.
SAVEPOINT s46;
-- Setup a simple temporal table with a NOT NULL column in a separate schema
CREATE SCHEMA tm_44_not_null_test;
CREATE TABLE tm_44_not_null_test.test_unit (
    id int,
    name text NOT NULL,
    value int,
    valid_from date,
    valid_until date
);
SELECT sql_saga.add_era('tm_44_not_null_test.test_unit'::regclass);
SELECT sql_saga.add_unique_key('tm_44_not_null_test.test_unit'::regclass, ARRAY['id']);

-- Insert an initial record
INSERT INTO tm_44_not_null_test.test_unit (id, name, value, valid_from, valid_until)
VALUES (1, 'Initial Name', 100, '2024-01-01', 'infinity');

-- Create a source table for the patch. It's missing the `name` column.
CREATE TEMP TABLE source_46 (
    row_id int generated by default as identity,
    id int,
    value int,
    valid_from date,
    valid_until date
) ON COMMIT DROP;
INSERT INTO source_46 (id, value, valid_from, valid_until) VALUES (1, 200, '2024-06-01', 'infinity');

\echo '--- Target: Initial State ---'
TABLE tm_44_not_null_test.test_unit;
\echo '--- Source: Data to merge (missing "name" column) ---'
TABLE source_46;

CALL sql_saga.temporal_merge(
  target_table => 'tm_44_not_null_test.test_unit'::regclass,
  source_table => 'source_46'::regclass,
  identity_columns => ARRAY['id'],
  ephemeral_columns => ARRAY[]::TEXT[],
  mode => 'MERGE_ENTITY_PATCH'::sql_saga.temporal_merge_mode,
  source_row_id_column => 'row_id'
);

\echo '--- Planner: Actual Plan ---'
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
\echo '--- Executor: Actual Feedback ---'
TABLE pg_temp.temporal_merge_feedback ORDER BY source_row_id;
\echo '--- Target: Expected Final State ---'
SELECT * FROM (VALUES
    (1, 'Initial Name', 100, '2024-01-01'::date, '2024-06-01'::date),
    (1, 'Initial Name', 200, '2024-06-01'::date, 'infinity'::date)
) AS v(id, name, value, valid_from, valid_until) ORDER BY id, valid_from;
\echo '--- Target: Final State ---'
TABLE tm_44_not_null_test.test_unit ORDER BY id, valid_from;

ROLLBACK TO SAVEPOINT s46;

-- Final Cleanup
DROP TABLE temporal_merge_test_multi_insert.test_target;
DROP SCHEMA temporal_merge_test_multi_insert CASCADE;

SET client_min_messages TO NOTICE;
ROLLBACK;
\i sql/include/test_teardown.sql
