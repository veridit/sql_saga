\i sql/include/test_setup.sql

BEGIN;

-- Scenario: Test temporal_merge with a composite natural key that also
-- serves as a temporal foreign key.
--
-- The table `unit_employment` tracks the number of employees for a given
-- legal unit and establishment. The combination of (legal_unit_id, establishment_id)
-- is the natural key for an employment record over time.

-- Parent table 1: legal_unit
CREATE TABLE nk_legal_unit (
    legal_unit_id int,
    name text,
    valid_from date,
    valid_until date,
    PRIMARY KEY (legal_unit_id, valid_from)
);
SELECT sql_saga.add_era('nk_legal_unit'::regclass, 'valid_from', 'valid_until');
SELECT sql_saga.add_unique_key('nk_legal_unit'::regclass, ARRAY['legal_unit_id']);
INSERT INTO nk_legal_unit VALUES (1, 'Global Corp', '2020-01-01', 'infinity');

-- Parent table 2: establishment
CREATE TABLE nk_establishment (
    establishment_id int,
    address text,
    valid_from date,
    valid_until date,
    PRIMARY KEY (establishment_id, valid_from)
);
SELECT sql_saga.add_era('nk_establishment'::regclass, 'valid_from', 'valid_until');
SELECT sql_saga.add_unique_key('nk_establishment'::regclass, ARRAY['establishment_id']);
INSERT INTO nk_establishment VALUES (101, 'Main St Office', '2020-01-01', 'infinity');
INSERT INTO nk_establishment VALUES (102, 'Side St Office', '2022-01-01', 'infinity');

-- Child table with composite natural/foreign key
CREATE TABLE nk_unit_employment (
    legal_unit_id int,
    establishment_id int,
    num_employees int,
    valid_from date,
    valid_until date,
    PRIMARY KEY (legal_unit_id, establishment_id, valid_from)
);
SELECT sql_saga.add_era('nk_unit_employment'::regclass, 'valid_from', 'valid_until');
SELECT sql_saga.add_unique_key('nk_unit_employment'::regclass, ARRAY['legal_unit_id', 'establishment_id']);

-- Foreign keys from the natural key columns to their respective parent tables.
SELECT sql_saga.add_foreign_key(
    'nk_unit_employment'::regclass, ARRAY['legal_unit_id'], 'valid', 'nk_legal_unit_legal_unit_id_valid'
);
SELECT sql_saga.add_foreign_key(
    'nk_unit_employment'::regclass, ARRAY['establishment_id'], 'valid', 'nk_establishment_establishment_id_valid'
);

-- Source table for the merge operation
CREATE TEMP TABLE nk_source (
    row_id int,
    legal_unit_id int,
    establishment_id int,
    num_employees int,
    valid_from date,
    valid_until date
);

-- Step 1: INSERT a new employment record.
INSERT INTO nk_source VALUES (1, 1, 101, 50, '2021-01-01', 'infinity');

CALL sql_saga.temporal_merge(
    p_target_table := 'nk_unit_employment'::regclass,
    p_source_table := 'nk_source'::regclass,
    p_id_columns := ARRAY['legal_unit_id', 'establishment_id'],
    p_ephemeral_columns := '{}'::text[],
    p_mode := 'upsert_replace'::sql_saga.temporal_merge_mode
);

\echo '--- INSERT: Expected Final State ---'
SELECT * FROM (VALUES
    (1, 101, 50, '2021-01-01'::date, 'infinity'::date)
) AS t(legal_unit_id, establishment_id, num_employees, valid_from, valid_until);

\echo '--- INSERT: Actual Final State ---'
TABLE nk_unit_employment;

-- Step 2: UPDATE the employment record.
TRUNCATE nk_source;
-- Update: The number of employees changed in 2023.
INSERT INTO nk_source VALUES (2, 1, 101, 75, '2023-01-01', 'infinity');

CALL sql_saga.temporal_merge(
    p_target_table := 'nk_unit_employment'::regclass,
    p_source_table := 'nk_source'::regclass,
    p_id_columns := ARRAY['legal_unit_id', 'establishment_id'],
    p_ephemeral_columns := '{}'::text[],
    p_mode := 'upsert_replace'::sql_saga.temporal_merge_mode
);

\echo '--- UPDATE: Expected Final State ---'
SELECT * FROM (VALUES
    (1, 101, 50, '2021-01-01'::date, '2023-01-01'::date),
    (1, 101, 75, '2023-01-01'::date, 'infinity'::date)
) AS t(legal_unit_id, establishment_id, num_employees, valid_from, valid_until);

\echo '--- UPDATE: Actual Final State ---'
TABLE nk_unit_employment ORDER BY valid_from;

-- Step 3: End-date the employment record.
TRUNCATE nk_source;
-- End-date: The employment ended at the start of 2024.
INSERT INTO nk_source VALUES (3, 1, 101, 75, '2023-01-01', '2024-01-01');

CALL sql_saga.temporal_merge(
    p_target_table := 'nk_unit_employment'::regclass,
    p_source_table := 'nk_source'::regclass,
    p_id_columns := ARRAY['legal_unit_id', 'establishment_id'],
    p_ephemeral_columns := '{}'::text[],
    p_mode := 'upsert_replace'::sql_saga.temporal_merge_mode
);

\echo '--- END-DATE: Expected Final State ---'
SELECT * FROM (VALUES
    (1, 101, 50, '2021-01-01'::date, '2023-01-01'::date),
    (1, 101, 75, '2023-01-01'::date, 'infinity'::date)
) AS t(legal_unit_id, establishment_id, num_employees, valid_from, valid_until);

\echo '--- END-DATE: Actual Final State ---'
TABLE nk_unit_employment ORDER BY valid_from;


ROLLBACK;

\i sql/include/test_teardown.sql
