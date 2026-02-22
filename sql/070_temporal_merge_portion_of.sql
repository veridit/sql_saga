\i sql/include/test_setup.sql

BEGIN;
\echo '----------------------------------------------------------------------------'
\echo 'Test Suite: `..._FOR_PORTION_OF` Mode Edge Cases'
\echo 'Description:'
\echo '  This test suite verifies the behavior of the `..._FOR_PORTION_OF` modes'
\echo '  with a focus on complex timeline interactions, such as operations that'
\echo '  straddle adjacent historical slices.'
\echo '----------------------------------------------------------------------------'

SET client_min_messages TO NOTICE;
CREATE SCHEMA tm_portion_of;
CREATE TABLE tm_portion_of.target(
    id int,
    payload TEXT,
    edit_comment TEXT,
    valid_range daterange,
    valid_from DATE,
    valid_until DATE
);

SELECT sql_saga.add_era('tm_portion_of.target'::regclass, 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until',
    era_name => 'valid');
SELECT sql_saga.add_unique_key('tm_portion_of.target'::regclass, '{id}', 'valid');

-- Helper function to reset target state
CREATE OR REPLACE PROCEDURE tm_portion_of.reset_target() AS $$
BEGIN
    TRUNCATE tm_portion_of.target;
    -- Target entity has two adjacent periods, a gap, and a final period
    INSERT INTO tm_portion_of.target (id, payload, edit_comment, valid_from, valid_until) VALUES
        (1, 'A-1', 'Adjacent 1', '2021-01-01', '2022-01-01'),
        (1, 'A-2', 'Adjacent 2', '2022-01-01', '2023-01-01'),
        (1, 'B-1', 'After Gap',  '2024-01-01', '2025-01-01');
END;
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
\echo 'Scenario 1: `PATCH_FOR_PORTION_OF` performs a surgical patch straddling adjacent slices'
--------------------------------------------------------------------------------
SAVEPOINT s1;
CALL tm_portion_of.reset_target();
CREATE TEMP TABLE source_1 (row_id int, id int, payload text, edit_comment text, valid_from date, valid_until date) ON COMMIT DROP;
-- A straddling change that covers half of each adjacent period
INSERT INTO source_1 (row_id, id, payload, edit_comment, valid_from, valid_until) VALUES
    (101, 1, 'Patched-Straddle', 'Surgical Patch Straddling', '2021-07-01', '2022-07-01');

\echo '--- Target: Initial State ---'
TABLE tm_portion_of.target ORDER BY id, valid_from;
\echo '--- Source: Data to merge ---'
TABLE source_1 ORDER BY row_id;

CALL sql_saga.temporal_merge(target_table => 'tm_portion_of.target'::regclass, source_table => 'source_1'::regclass, primary_identity_columns => '{id}'::text[], ephemeral_columns => '{edit_comment}'::text[], mode => 'PATCH_FOR_PORTION_OF'::sql_saga.temporal_merge_mode, era_name => 'valid');

\echo '--- Planner: Actual Plan ---'
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
\echo '--- Executor: Actual Feedback ---'
TABLE pg_temp.temporal_merge_feedback ORDER BY source_row_id;
\echo '--- Target: Expected Final State ---'
SELECT * FROM (VALUES
    (1, 'A-1', 'Adjacent 1', '2021-01-01'::date, '2021-07-01'::date),
    (1, 'Patched-Straddle', 'Surgical Patch Straddling', '2021-07-01'::date, '2022-07-01'::date),
    (1, 'A-2', 'Adjacent 2', '2022-07-01'::date, '2023-01-01'::date),
    (1, 'B-1', 'After Gap', '2024-01-01'::date, '2025-01-01'::date)
) AS v(id, payload, edit_comment, valid_from, valid_until) ORDER BY id, valid_from;
\echo '--- Target: Final State ---'
TABLE tm_portion_of.target ORDER BY id, valid_from;
ROLLBACK TO SAVEPOINT s1;

--------------------------------------------------------------------------------
\echo 'Scenario 2: `REPLACE_FOR_PORTION_OF` performs a surgical replacement straddling adjacent slices'
--------------------------------------------------------------------------------
SAVEPOINT s2;
CALL tm_portion_of.reset_target();
CREATE TEMP TABLE source_2 (row_id int, id int, payload text, edit_comment text, valid_from date, valid_until date) ON COMMIT DROP;
-- A straddling change that covers half of each adjacent period
INSERT INTO source_2 (row_id, id, payload, edit_comment, valid_from, valid_until) VALUES
    (201, 1, 'Replaced-Straddle', 'Surgical Replace Straddling', '2021-07-01', '2022-07-01');

\echo '--- Target: Initial State ---'
TABLE tm_portion_of.target ORDER BY id, valid_from;
\echo '--- Source: Data to merge ---'
TABLE source_2 ORDER BY row_id;

CALL sql_saga.temporal_merge(target_table => 'tm_portion_of.target'::regclass, source_table => 'source_2'::regclass, primary_identity_columns => '{id}'::text[], ephemeral_columns => '{edit_comment}'::text[], mode => 'REPLACE_FOR_PORTION_OF'::sql_saga.temporal_merge_mode, era_name => 'valid');

\echo '--- Planner: Actual Plan ---'
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
\echo '--- Executor: Actual Feedback ---'
TABLE pg_temp.temporal_merge_feedback ORDER BY source_row_id;
\echo '--- Target: Expected Final State ---'
SELECT * FROM (VALUES
    (1, 'A-1', 'Adjacent 1', '2021-01-01'::date, '2021-07-01'::date),
    (1, 'Replaced-Straddle', 'Surgical Replace Straddling', '2021-07-01'::date, '2022-07-01'::date),
    (1, 'A-2', 'Adjacent 2', '2022-07-01'::date, '2023-01-01'::date),
    (1, 'B-1', 'After Gap', '2024-01-01'::date, '2025-01-01'::date)
) AS v(id, payload, edit_comment, valid_from, valid_until) ORDER BY id, valid_from;
\echo '--- Target: Final State ---'
TABLE tm_portion_of.target ORDER BY id, valid_from;
ROLLBACK TO SAVEPOINT s2;

--------------------------------------------------------------------------------
\echo 'Scenario 3: `DELETE_FOR_PORTION_OF` carves out a piece of the timeline straddling adjacent slices'
--------------------------------------------------------------------------------
SAVEPOINT s3;
CALL tm_portion_of.reset_target();
CREATE TEMP TABLE source_3 (row_id int, id int, payload text, edit_comment text, valid_from date, valid_until date) ON COMMIT DROP;
-- A straddling delete that covers half of each adjacent period
INSERT INTO source_3 (row_id, id, payload, edit_comment, valid_from, valid_until) VALUES
    (301, 1, '__DELETE__', 'Surgical Delete Straddling', '2021-07-01', '2022-07-01');

\echo '--- Target: Initial State ---'
TABLE tm_portion_of.target ORDER BY id, valid_from;
\echo '--- Source: Data to merge ---'
TABLE source_3 ORDER BY row_id;

CALL sql_saga.temporal_merge(target_table => 'tm_portion_of.target'::regclass, source_table => 'source_3'::regclass, primary_identity_columns => '{id}'::text[], ephemeral_columns => '{edit_comment}'::text[], mode => 'DELETE_FOR_PORTION_OF'::sql_saga.temporal_merge_mode, era_name => 'valid');

\echo '--- Planner: Actual Plan ---'
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
\echo '--- Executor: Actual Feedback ---'
TABLE pg_temp.temporal_merge_feedback ORDER BY source_row_id;
\echo '--- Target: Expected Final State ---'
SELECT * FROM (VALUES
    (1, 'A-1', 'Adjacent 1', '2021-01-01'::date, '2021-07-01'::date),
    (1, 'A-2', 'Adjacent 2', '2022-07-01'::date, '2023-01-01'::date),
    (1, 'B-1', 'After Gap', '2024-01-01'::date, '2025-01-01'::date)
) AS v(id, payload, edit_comment, valid_from, valid_until) ORDER BY id, valid_from;
\echo '--- Target: Final State ---'
TABLE tm_portion_of.target ORDER BY id, valid_from;
ROLLBACK TO SAVEPOINT s3;

--------------------------------------------------------------------------------
\echo 'Scenario 4: `PATCH_FOR_PORTION_OF` correctly carries over NOT NULL columns when splitting'
--------------------------------------------------------------------------------
SAVEPOINT s4;
-- 1. Setup a simple temporal table with a NOT NULL column
CREATE TABLE tm_portion_of.test_unit_not_null (
    id int,
    name text NOT NULL,
    value int,
    valid_range daterange,
    valid_from date,
    valid_until date
);
SELECT sql_saga.add_era('tm_portion_of.test_unit_not_null'::regclass, 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('tm_portion_of.test_unit_not_null'::regclass, ARRAY['id']);

-- 2. Insert an initial record
INSERT INTO tm_portion_of.test_unit_not_null (id, name, value, valid_from, valid_until)
VALUES (1, 'Initial Name', 100, '2024-01-01', 'infinity');

-- 3. Create a source table for the patch.
--    This patch starts *after* the initial record, forcing a split.
--    Crucially, it is missing the `name` column, which has a NOT NULL constraint.
CREATE TEMP TABLE source_4 (
    row_id int generated by default as identity,
    id int,
    value int,
    valid_from date,
    valid_until date
);
INSERT INTO source_4 (id, value, valid_from, valid_until) VALUES (1, 200, '2024-06-01', 'infinity');

\echo '--- Target: Initial State ---'
TABLE tm_portion_of.test_unit_not_null;
\echo '--- Source: Data to merge (missing "name" column) ---'
TABLE source_4;

CALL sql_saga.temporal_merge(
  target_table => 'tm_portion_of.test_unit_not_null'::regclass,
  source_table => 'source_4'::regclass,
  primary_identity_columns => ARRAY['id'],
  ephemeral_columns => ARRAY[]::TEXT[],
  mode => 'PATCH_FOR_PORTION_OF'::sql_saga.temporal_merge_mode,
  row_id_column => 'row_id'
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
TABLE tm_portion_of.test_unit_not_null ORDER BY id, valid_from;
ROLLBACK TO SAVEPOINT s4;

ROLLBACK;

-- ============================================================================
-- Regression: PATCH_FOR_PORTION_OF with extending source range (StatBus bug)
-- ============================================================================
-- When source range extends beyond target, the extending segment should be
-- ignored. Previously, the native planner created a spurious INSERT for the
-- extending segment with only source columns, causing "null value in column
-- 'name'" when target-inherited NOT NULL columns were missing from source.
-- PL/pgSQL correctly filters via: WHEN 'PATCH_FOR_PORTION_OF' THEN t_data_payload IS NOT NULL
-- ============================================================================
BEGIN;
\echo '----------------------------------------------------------------------------'
\echo 'Regression: PATCH_FOR_PORTION_OF extending source range'
\echo '----------------------------------------------------------------------------'

CREATE SCHEMA patch_extend;
CREATE TABLE patch_extend.legal_unit (
    id int GENERATED BY DEFAULT AS IDENTITY,
    name text NOT NULL,
    address text,
    enterprise_id integer,
    valid_range daterange NOT NULL,
    valid_from date,
    valid_until date
);
SELECT sql_saga.add_era('patch_extend.legal_unit', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
ALTER TABLE patch_extend.legal_unit ADD PRIMARY KEY (id, valid_range WITHOUT OVERLAPS);
SELECT sql_saga.add_unique_key('patch_extend.legal_unit', ARRAY['id'], key_type => 'primary');
SELECT sql_saga.add_unique_key('patch_extend.legal_unit', ARRAY['name'], key_type => 'natural');

\echo '--- Step 1: Seed with full data ---'
CREATE TEMP TABLE source_seed (
    row_id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    name text,
    address text,
    enterprise_id integer,
    valid_from date,
    valid_until date
) ON COMMIT DROP;
INSERT INTO source_seed (name, address, enterprise_id, valid_from, valid_until) VALUES
    ('Company One', 'Street 1', NULL, '2023-01-01', '2023-12-31'),
    ('Company Two', 'Street 2', NULL, '2023-01-01', '2023-12-31');

CALL sql_saga.temporal_merge(
    target_table => 'patch_extend.legal_unit',
    source_table => 'source_seed',
    mode => 'MERGE_ENTITY_UPSERT'
);

\echo '--- After seed ---'
SELECT id, name, address, enterprise_id, valid_from, valid_until
FROM patch_extend.legal_unit ORDER BY id, valid_from;

\echo '--- Step 2: PATCH with fewer columns and extending range ---'
-- Source only has name + enterprise_id (no address column).
-- Source range [2023-01-01, infinity) extends beyond target [2023-01-01, 2023-12-31).
-- Expected: only the overlapping portion is patched. No INSERT beyond target.
CREATE TEMP TABLE source_patch (
    row_id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    name text,
    enterprise_id integer,
    valid_from date,
    valid_until date
) ON COMMIT DROP;
INSERT INTO source_patch (name, enterprise_id, valid_from, valid_until) VALUES
    ('Company Two', 42, '2023-01-01', 'infinity');

CALL sql_saga.temporal_merge(
    target_table => 'patch_extend.legal_unit',
    source_table => 'source_patch',
    mode => 'PATCH_FOR_PORTION_OF'
);

\echo '--- After PATCH ---'
SELECT id, name, address, enterprise_id, valid_from, valid_until
FROM patch_extend.legal_unit ORDER BY id, valid_from;

\echo '--- Verify: Company One unchanged ---'
SELECT count(*) AS company_one_rows
FROM patch_extend.legal_unit WHERE name = 'Company One';

\echo '--- Verify: Company Two patched, no extending row ---'
SELECT count(*) AS company_two_rows,
       count(*) FILTER (WHERE enterprise_id = 42) AS patched_rows,
       count(*) FILTER (WHERE valid_from >= '2023-12-31') AS extending_rows
FROM patch_extend.legal_unit WHERE name = 'Company Two';

ROLLBACK;

\i sql/include/test_teardown.sql
