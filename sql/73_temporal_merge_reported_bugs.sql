\i sql/include/test_setup.sql

BEGIN;

-- For psql execution, stop on first error
\set ON_ERROR_STOP on

CREATE EXTENSION IF NOT EXISTS sql_saga;

\echo '--------------------------------------------------------------------------------'
\echo 'Scenario 1: Demonstrate correct timeline extension with natural keys'
\echo 'A new record is created instead of extending an existing one if the source'
\echo 'provides a NULL stable key and no natural key is specified for lookup.'
\echo 'This test demonstrates both the incorrect and correct usage patterns.'
\echo '--------------------------------------------------------------------------------'

-- 1. Setup
CREATE SCHEMA IF NOT EXISTS tmrb;
DROP TABLE IF EXISTS tmrb.my_stat_for_unit;
CREATE TABLE tmrb.my_stat_for_unit (
    id SERIAL,
    legal_unit_id int NOT NULL,
    value_int int,
    valid_from date NOT NULL,
    valid_until date,
    PRIMARY KEY (id, valid_from)
);
SELECT sql_saga.add_era('tmrb.my_stat_for_unit'::regclass);
SELECT sql_saga.add_unique_key('tmrb.my_stat_for_unit'::regclass, '{id}');
SELECT sql_saga.add_unique_key('tmrb.my_stat_for_unit'::regclass, '{legal_unit_id}');

-- 2. First Import: Create initial record.
CREATE TEMP TABLE source_data_1 (
    row_id int, founding_row_id int, id int, legal_unit_id int, value_int int, valid_from date, valid_until date
) ON COMMIT DROP;

INSERT INTO source_data_1 VALUES
(1, 1, NULL, 100, 4, '2023-01-01', '2023-04-01');

\echo '--- Source Data for First Import ---'
TABLE source_data_1;

CALL sql_saga.temporal_merge(
    p_target_table => 'tmrb.my_stat_for_unit',
    p_source_table => 'source_data_1',
    p_identity_columns => ARRAY['id'],
    p_natural_identity_columns => NULL,
    p_ephemeral_columns => NULL,
    p_identity_correlation_column => 'founding_row_id'
);

\echo '--- Target Table after First Import (Expect 1 row) ---'
SELECT id, legal_unit_id, value_int, valid_from, valid_until FROM tmrb.my_stat_for_unit;

-- 3. Second Import: Consecutive period, SAME value.
CREATE TEMP TABLE source_data_2 (
    row_id int, founding_row_id int, id int, legal_unit_id int, value_int int, valid_from date, valid_until date
) ON COMMIT DROP;
INSERT INTO source_data_2 VALUES
(2, 1, NULL, 100, 4, '2023-04-01', '2023-07-01');

\echo '--- Case 1a: Incorrect Usage (no natural key) ---'
\echo '--- Source Data for Second Import ---'
TABLE source_data_2;

-- This call incorrectly creates a new entity because the source `id` is NULL
-- and no natural key is provided to find the existing entity.
CALL sql_saga.temporal_merge(
    p_target_table => 'tmrb.my_stat_for_unit',
    p_source_table => 'source_data_2',
    p_identity_columns => ARRAY['id'],
    p_natural_identity_columns => NULL,
    p_ephemeral_columns => NULL,
    p_identity_correlation_column => 'founding_row_id'
);

\echo '--- Target Table after Second Import (INCORRECT: 2 rows instead of 1 extended row) ---'
SELECT id, legal_unit_id, value_int, valid_from, valid_until FROM tmrb.my_stat_for_unit ORDER BY id, valid_from;

-- 4. Reset and demonstrate correct usage.
TRUNCATE tmrb.my_stat_for_unit;
TRUNCATE source_data_1;
INSERT INTO source_data_1 VALUES (1, 1, NULL, 100, 4, '2023-01-01', '2023-04-01');
CALL sql_saga.temporal_merge(
    p_target_table => 'tmrb.my_stat_for_unit',
    p_source_table => 'source_data_1',
    p_identity_columns => '{id}',
    p_natural_identity_columns => NULL,
    p_ephemeral_columns => NULL,
    p_identity_correlation_column => 'founding_row_id'
);

\echo '--- Case 1b: Correct Usage (with natural key) ---'
-- This call correctly finds the existing entity via the natural key `legal_unit_id`
-- and extends its timeline because the data has not changed.
CALL sql_saga.temporal_merge(
    p_target_table => 'tmrb.my_stat_for_unit',
    p_source_table => 'source_data_2',
    p_identity_columns => ARRAY['id'],
    p_natural_identity_columns => ARRAY['legal_unit_id'],
    p_ephemeral_columns => NULL,
    p_identity_correlation_column => 'founding_row_id'
);

\echo '--- Target Table after Second Import (CORRECT: 1 extended row) ---'
SELECT id, legal_unit_id, value_int, valid_from, valid_until FROM tmrb.my_stat_for_unit;

\echo '--------------------------------------------------------------------------------'
\echo 'Scenario 2: Demonstrate correct Primary Key usage for SCD Type 2'
\echo 'An SCD Type 2 update requires a composite primary key that includes a'
\echo 'temporal column (e.g., PRIMARY KEY (id, valid_from)). This allows multiple'
\echo 'historical versions of an entity to be stored. This test demonstrates'
\echo 'the failure with an incorrect PK and the success with a correct one.'
\echo '--------------------------------------------------------------------------------'

-- 1. Setup with incorrect schema
DROP TABLE IF EXISTS tmrb.my_stat_for_unit_bad_pk;
CREATE TABLE tmrb.my_stat_for_unit_bad_pk (
    id SERIAL PRIMARY KEY, -- Incorrect: PK should include a temporal column
    legal_unit_id int NOT NULL,
    value_int int,
    valid_from date NOT NULL,
    valid_until date
);
SELECT sql_saga.add_era('tmrb.my_stat_for_unit_bad_pk'::regclass);

-- 2. First Import
TRUNCATE source_data_1;
INSERT INTO source_data_1 VALUES (1, 1, NULL, 100, 4, '2023-01-01', '2023-04-01');
CALL sql_saga.temporal_merge(
    p_target_table => 'tmrb.my_stat_for_unit_bad_pk',
    p_source_table => 'source_data_1',
    p_identity_columns => '{id}',
    p_natural_identity_columns => NULL,
    p_ephemeral_columns => NULL,
    p_update_source_with_identity => true
);

\echo '--- Target after first import ---'
TABLE tmrb.my_stat_for_unit_bad_pk;
\echo '--- Source after ID back-fill ---'
TABLE source_data_1;

-- 3. Second Import with changed data.
TRUNCATE source_data_2;
INSERT INTO source_data_2 VALUES (2, 1, 1, 100, 5, '2023-04-01', '2023-07-01');

\echo '--- Case 2a: Incorrect Schema (PK on id only) ---'
\echo '--- Source for second import (with changed value and known ID) ---'
TABLE source_data_2;

-- This call will fail because it tries to INSERT a new history slice with id=1,
-- which violates the PRIMARY KEY constraint.
SET client_min_messages TO NOTICE;
DO $$
BEGIN
CALL sql_saga.temporal_merge(
    p_target_table => 'tmrb.my_stat_for_unit_bad_pk',
    p_source_table => 'source_data_2',
    p_identity_columns => ARRAY['id'],
    p_natural_identity_columns => NULL,
    p_ephemeral_columns => NULL
);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Caught expected error: %', SQLERRM;
END;
$$;
RESET client_min_messages;

-- 4. Setup with correct schema
DROP TABLE IF EXISTS tmrb.my_stat_for_unit_good_pk;
CREATE TABLE tmrb.my_stat_for_unit_good_pk (
    id SERIAL NOT NULL,
    legal_unit_id int NOT NULL,
    value_int int,
    valid_from date NOT NULL,
    valid_until date,
    PRIMARY KEY (id, valid_from) -- Correct: Composite PK
);
SELECT sql_saga.add_era('tmrb.my_stat_for_unit_good_pk'::regclass);
TRUNCATE source_data_1;
INSERT INTO source_data_1 VALUES (1, 1, NULL, 100, 4, '2023-01-01', '2023-04-01');
CALL sql_saga.temporal_merge(
    p_target_table => 'tmrb.my_stat_for_unit_good_pk',
    p_source_table => 'source_data_1',
    p_identity_columns => '{id}',
    p_natural_identity_columns => NULL,
    p_ephemeral_columns => NULL,
    p_update_source_with_identity => true
);

\echo '--- Case 2b: Correct Schema (Composite PK) ---'
TRUNCATE source_data_2;
INSERT INTO source_data_2 VALUES (2, 1, 1, 100, 5, '2023-04-01', '2023-07-01');
\echo '--- Source for second import ---'
TABLE source_data_2;

CALL sql_saga.temporal_merge(
    p_target_table => 'tmrb.my_stat_for_unit_good_pk',
    p_source_table => 'source_data_2',
    p_identity_columns => ARRAY['id'],
    p_natural_identity_columns => NULL,
    p_ephemeral_columns => NULL
);

\echo '--- Planner output for Case 2b ---'
SELECT plan_op_seq, source_row_ids, operation, timeline_update_effect, entity_ids, old_valid_from, new_valid_from, new_valid_until, data, relation FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Target after second import (CORRECT: 2 historical rows for id=1) ---'
SELECT * FROM tmrb.my_stat_for_unit_good_pk ORDER BY id, valid_from;

\echo '--------------------------------------------------------------------------------'
\echo 'Scenario 3: Demonstrate correct usage of GENERATED ... AS IDENTITY'
\echo '`GENERATED ALWAYS AS IDENTITY` is incompatible with SCD Type 2 history because'
\echo 'it prevents re-inserting a stable ID for a new history slice. The correct'
\echo 'pattern is `GENERATED BY DEFAULT AS IDENTITY` with a composite primary key.'
\echo 'This test demonstrates both the failure and the correct pattern.'
\echo '--------------------------------------------------------------------------------'

-- 1. Setup with incorrect `GENERATED ALWAYS` schema
DROP TABLE IF EXISTS tmrb.my_stat_for_unit_gen_always;
CREATE TABLE tmrb.my_stat_for_unit_gen_always (
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    legal_unit_id int NOT NULL,
    value_int int,
    valid_from date NOT NULL,
    valid_until date
);
SELECT sql_saga.add_era('tmrb.my_stat_for_unit_gen_always'::regclass);

-- 2. Attempt to import, which will fail
TRUNCATE source_data_1;
INSERT INTO source_data_1 VALUES (1, 1, NULL, 100, 4, '2023-01-01', '2023-04-01');

\echo '--- Source Data for Import ---'
TABLE source_data_1;

\echo '--- Calling temporal_merge (This will fail because of GENERATED ALWAYS) ---'
DO $$
BEGIN
CALL sql_saga.temporal_merge(
    p_target_table => 'tmrb.my_stat_for_unit_gen_always',
    p_source_table => 'source_data_1',
    p_identity_columns => ARRAY['id'],
    p_natural_identity_columns => NULL,
    p_ephemeral_columns => NULL,
    p_identity_correlation_column => 'founding_row_id'
);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Caught expected error: %', SQLERRM;
END;
$$;

-- 3. Setup with correct `GENERATED BY DEFAULT` schema
DROP TABLE IF EXISTS tmrb.my_stat_for_unit_gen_default;
CREATE TABLE tmrb.my_stat_for_unit_gen_default (
    id int GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    legal_unit_id int NOT NULL,
    value_int int,
    valid_from date NOT NULL,
    valid_until date,
    PRIMARY KEY (id, valid_from)
);
SELECT sql_saga.add_era('tmrb.my_stat_for_unit_gen_default'::regclass);

-- 4. Import new entity, which succeeds
TRUNCATE source_data_1;
INSERT INTO source_data_1 VALUES (1, 1, NULL, 100, 4, '2023-01-01', '2023-04-01');
CALL sql_saga.temporal_merge(
    p_target_table => 'tmrb.my_stat_for_unit_gen_default',
    p_source_table => 'source_data_1',
    p_identity_columns => ARRAY['id'],
    p_natural_identity_columns => NULL,
    p_ephemeral_columns => NULL,
    p_update_source_with_identity => true
);
\echo '--- Target after first import ---'
TABLE tmrb.my_stat_for_unit_gen_default;

-- 5. Import an update for the same entity, which also succeeds
TRUNCATE source_data_2;
INSERT INTO source_data_2 VALUES (2, 1, 1, 100, 5, '2023-04-01', '2023-07-01');
\echo '--- Source for second import ---'
TABLE source_data_2;

CALL sql_saga.temporal_merge(
    p_target_table => 'tmrb.my_stat_for_unit_gen_default',
    p_source_table => 'source_data_2',
    p_identity_columns => ARRAY['id'],
    p_natural_identity_columns => NULL,
    p_ephemeral_columns => NULL
);

\echo '--- Target after second import (CORRECT: 2 historical rows for id=1) ---'
SELECT * FROM tmrb.my_stat_for_unit_gen_default ORDER BY id, valid_from;


\echo '--------------------------------------------------------------------------------'
\echo 'Scenario 4: Reproduce NOT NULL violation on UPDATE (Bug #1)'
\echo 'The executor must not include identity columns in the SET clause of UPDATEs.'
\echo '--------------------------------------------------------------------------------'
CREATE TABLE tmrb.bug_repro_1 (
    id SERIAL NOT NULL,
    legal_unit_id int NOT NULL,
    value_int int,
    valid_from date NOT NULL,
    valid_until date,
    -- The PK does NOT include the stable identifier `id`, which is what causes the bug.
    PRIMARY KEY (legal_unit_id, valid_from)
);
SELECT sql_saga.add_era('tmrb.bug_repro_1'::regclass);
SELECT sql_saga.add_unique_key('tmrb.bug_repro_1'::regclass, '{legal_unit_id}');

-- Insert initial record
TRUNCATE source_data_1;
INSERT INTO source_data_1 VALUES (1, 1, NULL, 100, 4, '2023-01-01', 'infinity');
CALL sql_saga.temporal_merge(
    p_target_table => 'tmrb.bug_repro_1',
    p_source_table => 'source_data_1',
    p_identity_columns => '{id}',
    p_natural_identity_columns => '{legal_unit_id}',
    p_update_source_with_identity => true
);
\echo '--- Target after initial import ---'
TABLE tmrb.bug_repro_1;

-- This source data will cause an UPDATE on the existing id=1
TRUNCATE source_data_2;
INSERT INTO source_data_2 VALUES (2, 1, 1, 100, 5, '2023-04-01', 'infinity');
\echo '--- Source for second import (SCD Type 2 update) ---'
TABLE source_data_2;

\echo '--- Calling temporal_merge (should now succeed) ---'
CALL sql_saga.temporal_merge(
    p_target_table => 'tmrb.bug_repro_1',
    p_source_table => 'source_data_2',
    p_identity_columns => ARRAY['id'],
    p_natural_identity_columns => ARRAY['legal_unit_id'],
    p_mode => 'MERGE_ENTITY_REPLACE'
);
\echo '--- Target after second import (CORRECT: 2 historical rows for id=1) ---'
SELECT * FROM tmrb.bug_repro_1 ORDER BY id, valid_from;

\echo '--------------------------------------------------------------------------------'
\echo 'Scenario 5: Reproduce invalid boolean cast on GUC (Bug #2)'
\echo 'The logging logic must handle empty string GUC values gracefully.'
\echo '--------------------------------------------------------------------------------'
\echo '--- Setting log_plan GUC to an empty string ---'
SET sql_saga.temporal_merge.log_plan = '';
\echo '--- Calling temporal_merge (should now succeed) ---'
-- The content of the call doesn't matter, as the error happens during parameter
-- processing before the planner runs. The fix ensures this now runs without error.
CALL sql_saga.temporal_merge(
    p_target_table => 'tmrb.bug_repro_1',
    p_source_table => 'source_data_2',
    p_identity_columns => ARRAY['id'],
    p_natural_identity_columns => NULL
);
-- Reset GUC for any subsequent tests
RESET sql_saga.temporal_merge.log_plan;


ROLLBACK;

\i sql/include/test_teardown.sql
