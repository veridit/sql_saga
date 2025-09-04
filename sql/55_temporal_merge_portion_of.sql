\i sql/include/test_setup.sql

BEGIN;

CREATE SCHEMA tm_portion_of;
CREATE TABLE tm_portion_of.target(
    id int,
    payload TEXT,
    edit_comment TEXT,
    valid_from DATE,
    valid_until DATE
);

SELECT sql_saga.add_era('tm_portion_of.target'::regclass, 'valid_from', 'valid_until', 'valid');
SELECT sql_saga.add_unique_key('tm_portion_of.target'::regclass, '{id}', 'valid');

-- Helper function to reset target state
CREATE OR REPLACE PROCEDURE tm_portion_of.reset_target() AS $$
BEGIN
    TRUNCATE tm_portion_of.target;
    -- Add control entities that should not be affected by the merge
    INSERT INTO tm_portion_of.target (id, payload, edit_comment, valid_from, valid_until) VALUES
        (0, 'Control PRE', 'Unaffected', '2020-01-01', '2025-01-01');

    -- Target entity has two adjacent periods, a gap, and a final period
    INSERT INTO tm_portion_of.target (id, payload, edit_comment, valid_from, valid_until) VALUES
        (1, 'A-1', 'Adjacent 1', '2021-01-01', '2022-01-01'),
        (1, 'A-2', 'Adjacent 2', '2022-01-01', '2023-01-01'),
        (1, 'B-1', 'After Gap',  '2024-01-01', '2025-01-01');

    INSERT INTO tm_portion_of.target (id, payload, edit_comment, valid_from, valid_until) VALUES
        (2, 'Control POST', 'Unaffected', '2020-01-01', '2025-01-01');
END;
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
\echo 'Scenario 1: `PATCH_FOR_PORTION_OF` performs a surgical patch'
--------------------------------------------------------------------------------
CALL tm_portion_of.reset_target();
CREATE TEMP TABLE source_1 (row_id int, id int, payload text, edit_comment text, valid_from date, valid_until date) ON COMMIT DROP;
-- Source data to test multiple patch scenarios at once
INSERT INTO source_1 (row_id, id, payload, edit_comment, valid_from, valid_until) VALUES
    -- 1. Ignored change (identical data)
    (101, 1, 'A-1', 'Adjacent 1', '2021-01-01', '2022-01-01'),
    -- 2. Straddling change (covers half of each adjacent period)
    (102, 1, 'Patched-Straddle', 'Surgical Patch Straddling', '2021-07-01', '2022-07-01'),
    -- 3. Perfectly aligned change (on the segment after the gap)
    (103, 1, 'Patched-Aligned', 'Surgical Patch Aligned', '2024-01-01', '2025-01-01');

CALL sql_saga.temporal_merge(p_target_table => 'tm_portion_of.target'::regclass, p_source_table => 'source_1'::regclass, p_id_columns => '{id}'::text[], p_ephemeral_columns => '{edit_comment}'::text[], p_mode => 'PATCH_FOR_PORTION_OF'::sql_saga.temporal_merge_mode, p_era_name => 'valid');
\echo '--- Plan and Feedback ---'
SELECT * FROM temporal_merge_plan;
SELECT source_row_id, status FROM temporal_merge_feedback ORDER BY source_row_id;
\echo '--- Expected Final State ---'
SELECT * FROM (VALUES
    (0, 'Control PRE', 'Unaffected', '2020-01-01'::date, '2025-01-01'::date),
    (1, 'A-1', 'Adjacent 1', '2021-01-01'::date, '2021-07-01'::date),
    (1, 'Patched-Straddle', 'Surgical Patch Straddling', '2021-07-01'::date, '2022-07-01'::date),
    (1, 'A-2', 'Adjacent 2', '2022-07-01'::date, '2023-01-01'::date),
    (1, 'Patched-Aligned', 'Surgical Patch Aligned', '2024-01-01'::date, '2025-01-01'::date),
    (2, 'Control POST', 'Unaffected', '2020-01-01'::date, '2025-01-01'::date)
) AS v(id, payload, edit_comment, valid_from, valid_until) ORDER BY id, valid_from;
\echo '--- Actual Final State ---'
SELECT id, payload, edit_comment, valid_from, valid_until FROM tm_portion_of.target ORDER BY id, valid_from;

--------------------------------------------------------------------------------
\echo 'Scenario 2: `REPLACE_FOR_PORTION_OF` performs a surgical replacement'
--------------------------------------------------------------------------------
CALL tm_portion_of.reset_target();
CREATE TEMP TABLE source_2 (row_id int, id int, payload text, edit_comment text, valid_from date, valid_until date) ON COMMIT DROP;
-- Source data to test multiple replace scenarios at once
INSERT INTO source_2 (row_id, id, payload, edit_comment, valid_from, valid_until) VALUES
    -- 1. Ignored change (identical data)
    (201, 1, 'A-1', 'Adjacent 1', '2021-01-01', '2022-01-01'),
    -- 2. Straddling change (covers half of each adjacent period)
    (202, 1, 'Replaced-Straddle', 'Surgical Replace Straddling', '2021-07-01', '2022-07-01'),
    -- 3. Perfectly aligned change (on the segment after the gap)
    (203, 1, 'Replaced-Aligned', 'Surgical Replace Aligned', '2024-01-01', '2025-01-01');

CALL sql_saga.temporal_merge(p_target_table => 'tm_portion_of.target'::regclass, p_source_table => 'source_2'::regclass, p_id_columns => '{id}'::text[], p_ephemeral_columns => '{edit_comment}'::text[], p_mode => 'REPLACE_FOR_PORTION_OF'::sql_saga.temporal_merge_mode, p_era_name => 'valid');
\echo '--- Plan and Feedback ---'
SELECT * FROM temporal_merge_plan;
SELECT source_row_id, status FROM temporal_merge_feedback ORDER BY source_row_id;
\echo '--- Expected Final State ---'
SELECT * FROM (VALUES
    (0, 'Control PRE', 'Unaffected', '2020-01-01'::date, '2025-01-01'::date),
    (1, 'A-1', 'Adjacent 1', '2021-01-01'::date, '2021-07-01'::date),
    (1, 'Replaced-Straddle', 'Surgical Replace Straddling', '2021-07-01'::date, '2022-07-01'::date),
    (1, 'A-2', 'Adjacent 2', '2022-07-01'::date, '2023-01-01'::date),
    (1, 'Replaced-Aligned', 'Surgical Replace Aligned', '2024-01-01'::date, '2025-01-01'::date),
    (2, 'Control POST', 'Unaffected', '2020-01-01'::date, '2025-01-01'::date)
) AS v(id, payload, edit_comment, valid_from, valid_until) ORDER BY id, valid_from;
\echo '--- Actual Final State ---'
SELECT id, payload, edit_comment, valid_from, valid_until FROM tm_portion_of.target ORDER BY id, valid_from;

--------------------------------------------------------------------------------
\echo 'Scenario 3: `DELETE_FOR_PORTION_OF` carves out a piece of the timeline'
--------------------------------------------------------------------------------
CALL tm_portion_of.reset_target();
CREATE TEMP TABLE source_3 (row_id int, id int, payload text, edit_comment text, valid_from date, valid_until date) ON COMMIT DROP;
-- Source data to test multiple delete scenarios at once
INSERT INTO source_3 (row_id, id, payload, edit_comment, valid_from, valid_until) VALUES
    -- 1. Straddling delete (covers half of each adjacent period)
    (301, 1, NULL, 'Surgical Delete Straddling', '2021-07-01', '2022-07-01'),
    -- 2. Perfectly aligned delete (on the segment after the gap)
    (302, 1, NULL, 'Surgical Delete Aligned', '2024-01-01', '2025-01-01');

CALL sql_saga.temporal_merge(p_target_table => 'tm_portion_of.target'::regclass, p_source_table => 'source_3'::regclass, p_id_columns => '{id}'::text[], p_ephemeral_columns => '{edit_comment}'::text[], p_mode => 'DELETE_FOR_PORTION_OF'::sql_saga.temporal_merge_mode, p_era_name => 'valid');
\echo '--- Plan and Feedback ---'
SELECT * FROM temporal_merge_plan;
SELECT source_row_id, status FROM temporal_merge_feedback ORDER BY source_row_id;
\echo '--- Expected Final State ---'
SELECT * FROM (VALUES
    (0, 'Control PRE', 'Unaffected', '2020-01-01'::date, '2025-01-01'::date),
    (1, 'A-1', 'Adjacent 1', '2021-01-01'::date, '2021-07-01'::date),
    (1, 'A-2', 'Adjacent 2', '2022-07-01'::date, '2023-01-01'::date),
    (2, 'Control POST', 'Unaffected', '2020-01-01'::date, '2025-01-01'::date)
) AS v(id, payload, edit_comment, valid_from, valid_until) ORDER BY id, valid_from;
\echo '--- Actual Final State ---'
SELECT id, payload, edit_comment, valid_from, valid_until FROM tm_portion_of.target ORDER BY id, valid_from;

--------------------------------------------------------------------------------
\echo 'Scenario 4: `..._FOR_PORTION_OF` modes return TARGET_NOT_FOUND for non-existent entities'
--------------------------------------------------------------------------------
TRUNCATE tm_portion_of.target;
CREATE TEMP TABLE source_4 (row_id int, id int, payload text, edit_comment text, valid_from date, valid_until date) ON COMMIT DROP;
-- Source attempts to modify an entity that does not exist in the target
INSERT INTO source_4 VALUES (104, 999, 'No-op', 'Should be SKIPPED_NO_TARGET', '2021-06-01', '2021-09-01');

\echo '--- PATCH_FOR_PORTION_OF ---'
CALL sql_saga.temporal_merge(p_target_table => 'tm_portion_of.target'::regclass, p_source_table => 'source_4'::regclass, p_id_columns => '{id}'::text[], p_ephemeral_columns => '{edit_comment}'::text[], p_mode => 'PATCH_FOR_PORTION_OF'::sql_saga.temporal_merge_mode, p_era_name => 'valid');
SELECT operation, entity_ids FROM temporal_merge_plan;
SELECT source_row_id, status FROM temporal_merge_feedback ORDER BY source_row_id;

\echo '--- REPLACE_FOR_PORTION_OF ---'
CALL sql_saga.temporal_merge(p_target_table => 'tm_portion_of.target'::regclass, p_source_table => 'source_4'::regclass, p_id_columns => '{id}'::text[], p_ephemeral_columns => '{edit_comment}'::text[], p_mode => 'REPLACE_FOR_PORTION_OF'::sql_saga.temporal_merge_mode, p_era_name => 'valid');
SELECT operation, entity_ids FROM temporal_merge_plan;
SELECT source_row_id, status FROM temporal_merge_feedback ORDER BY source_row_id;

\echo '--- DELETE_FOR_PORTION_OF ---'
CALL sql_saga.temporal_merge(p_target_table => 'tm_portion_of.target'::regclass, p_source_table => 'source_4'::regclass, p_id_columns => '{id}'::text[], p_ephemeral_columns => '{edit_comment}'::text[], p_mode => 'DELETE_FOR_PORTION_OF'::sql_saga.temporal_merge_mode, p_era_name => 'valid');
SELECT operation, entity_ids FROM temporal_merge_plan;
SELECT source_row_id, status FROM temporal_merge_feedback ORDER BY source_row_id;

ROLLBACK;

\i sql/include/test_teardown.sql
