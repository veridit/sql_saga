\i sql/include/test_setup.sql

BEGIN;
\echo '----------------------------------------------------------------------------'
\echo 'Test: README Usage Examples'
\echo 'This test provides a runnable, self-contained demonstration of the public'
\echo 'API examples documented in README.md, ensuring they remain correct and'
\echo 'functional. It covers the full lifecycle from setup to data loading'
\echo 'with temporal_merge to teardown.'
\echo '----------------------------------------------------------------------------'

SET client_min_messages TO NOTICE;
CREATE SCHEMA readme;

--------------------------------------------------------------------------------
\echo '--- 1. Setup: Create and Activate Tables (from README) ---'
--------------------------------------------------------------------------------

\echo '--- Creating tables: legal_unit, establishment, projects ---'
CREATE TABLE readme.legal_unit (
  id SERIAL NOT NULL,
  legal_ident VARCHAR NOT NULL,
  name VARCHAR NOT NULL,
  status TEXT, -- e.g., 'active', 'inactive'
  valid_from DATE,
  valid_to DATE, -- Optional: for human-readable inclusive end dates
  valid_until DATE
);

CREATE TABLE readme.establishment (
  id SERIAL NOT NULL,
  name VARCHAR NOT NULL,
  address TEXT NOT NULL,
  legal_unit_id INTEGER, -- Note: Nullable for initial insert before back-filling
  valid_from DATE,
  valid_until DATE
);

CREATE TABLE readme.projects (id serial primary key, name text, legal_unit_id int);

CREATE TABLE readme.unit_with_range (
  id SERIAL NOT NULL,
  name TEXT,
  start_num INT,
  until_num INT,
  num_range INT4RANGE
);

\echo '--- Activating sql_saga ---'
-- Register the table as a temporal table (an "era") using default column names.
-- Explicitly enable synchronization for the 'valid_to' column.
SELECT sql_saga.add_era('readme.legal_unit'::regclass, synchronize_valid_to_column := 'valid_to');
-- Add temporal unique keys. A name is generated if the last argument is omitted.
SELECT sql_saga.add_unique_key(table_oid => 'readme.legal_unit'::regclass, column_names => ARRAY['id'], key_type => 'natural', unique_key_name => 'legal_unit_id_valid');
SELECT sql_saga.add_unique_key(table_oid => 'readme.legal_unit'::regclass, column_names => ARRAY['legal_ident'], key_type => 'natural', unique_key_name => 'legal_unit_legal_ident_valid');
-- Add a predicated unique key (e.g., only active units must have a unique name).
SELECT sql_saga.add_unique_key(
    table_oid => 'readme.legal_unit'::regclass,
    column_names => ARRAY['name'],
    key_type => 'predicated',
    predicate => 'status = ''active''',
    unique_key_name => 'legal_unit_active_name_valid'
);

SELECT sql_saga.add_era(table_oid => 'readme.establishment'::regclass, valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'readme.establishment'::regclass, column_names => ARRAY['id'], key_type => 'natural', unique_key_name => 'establishment_id_valid');
SELECT sql_saga.add_unique_key(table_oid => 'readme.establishment'::regclass, column_names => ARRAY['name'], key_type => 'natural', unique_key_name => 'establishment_name_valid');
-- Add a temporal foreign key.
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'readme.establishment'::regclass,
    fk_column_names => ARRAY['legal_unit_id'],
    pk_table_oid => 'readme.legal_unit'::regclass,
    pk_column_names => ARRAY['id']
);

-- Add a foreign key from a regular table to a temporal table.
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'readme.projects'::regclass,
    fk_column_names => ARRAY['legal_unit_id'],
    pk_table_oid => 'readme.legal_unit'::regclass,
    pk_column_names => ARRAY['id']
);

SELECT sql_saga.add_era(table_oid => 'readme.unit_with_range'::regclass, valid_from_column_name => 'start_num', valid_until_column_name => 'until_num', synchronize_range_column := 'num_range');
SELECT sql_saga.add_unique_key(table_oid => 'readme.unit_with_range'::regclass, column_names => ARRAY['id'], key_type => 'natural', unique_key_name => 'unit_with_range_id_valid');

\echo '--- Verification: Check metadata tables ---'
SELECT table_schema, table_name, era_name FROM sql_saga.era WHERE table_schema = 'readme' ORDER BY table_name;
SELECT table_schema, table_name, unique_key_name FROM sql_saga.unique_keys WHERE table_schema = 'readme' ORDER BY unique_key_name;
SELECT table_schema, table_name, foreign_key_name FROM sql_saga.foreign_keys WHERE table_schema = 'readme' ORDER BY foreign_key_name;

--------------------------------------------------------------------------------
\echo '\n--- 2. Data Loading with temporal_merge ---'
\echo 'Demonstrates a realistic multi-step import process with ID back-filling.'
--------------------------------------------------------------------------------
CREATE TEMP TABLE source_data (
    row_id INT,
    identity_correlation_id INT, -- To group related rows for a single new entity
    entity_type TEXT,
    legal_ident TEXT,
    legal_unit_id INT, -- Starts NULL, to be back-filled
    name TEXT,
    status TEXT,
    address TEXT,
    valid_from DATE,
    valid_until DATE
) ON COMMIT DROP;

INSERT INTO source_data VALUES
-- Legal Unit 1: "SpareParts Corp" with a name change history
(101, 1, 'legal_unit', 'LU001', NULL, 'AutoSpareParts INC', 'active', NULL, '2023-07-01', '2024-01-01'),
(102, 1, 'legal_unit', 'LU001', NULL, 'SpareParts Corporation', 'active', NULL, '2024-01-01', 'infinity'),
-- Establishment 1, belonging to Legal Unit 1
(201, 1, 'establishment', NULL, NULL, 'Main Branch', NULL, '123 Innovation Drive', '2023-08-01', 'infinity'),
-- Legal Unit 2: "General Refinement LLC"
(103, 2, 'legal_unit', 'LU002', NULL, 'General Refinement LLC', 'active', NULL, '2023-01-01', 'infinity'),
-- Establishment 2, belonging to Legal Unit 2
(202, 2, 'establishment', NULL, NULL, 'Refinery Plant', NULL, '456 Industrial Ave', '2023-01-01', 'infinity');

\echo '--- Source data for merge ---'
SELECT * FROM source_data ORDER BY row_id;

\echo '\n--- Step 2a: Merge legal_unit data ---'
CREATE TEMP TABLE source_legal_unit ON COMMIT DROP AS SELECT row_id, identity_correlation_id, legal_unit_id AS id, legal_ident, name, status, valid_from, valid_until FROM source_data WHERE entity_type = 'legal_unit';


CALL sql_saga.temporal_merge(
    target_table => 'readme.legal_unit'::regclass,
    source_table => 'source_legal_unit'::regclass,
    identity_columns => '{id}'::text[],
    ephemeral_columns => '{}',
    mode => 'MERGE_ENTITY_REPLACE',
    era_name => 'valid',
    founding_id_column => 'identity_correlation_id',
    update_source_with_identity => true
);

\echo '--- Verification: Feedback from temporal_merge ---'
SELECT * FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
SELECT * FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id;

\echo '--- Verification: Final state of legal_unit table ---'
SELECT id, legal_ident, name, status, valid_from, valid_until FROM readme.legal_unit ORDER BY id, valid_from;

\echo '--- Verification: Check if IDs were back-filled into the source temp table ---'
SELECT row_id, identity_correlation_id, id AS legal_unit_id FROM source_legal_unit ORDER BY row_id;

\echo '\n--- Step 2b: Back-fill generated legal_unit_id to main source table ---'
UPDATE source_data sd
SET legal_unit_id = slu.id
FROM source_legal_unit slu
WHERE sd.identity_correlation_id = slu.identity_correlation_id;

\echo '--- Verification: Main source table after ID back-fill ---'
SELECT row_id, identity_correlation_id, entity_type, legal_unit_id, name FROM source_data ORDER BY row_id;

\echo '\n--- Step 2c: Merge establishment data ---'
CREATE TEMP TABLE source_establishment ON COMMIT DROP AS SELECT row_id, identity_correlation_id, NULL::INT AS id, legal_unit_id, name, address, valid_from, valid_until FROM source_data WHERE entity_type = 'establishment';


CALL sql_saga.temporal_merge(
    target_table => 'readme.establishment'::regclass,
    source_table => 'source_establishment'::regclass,
    identity_columns => '{id}'::text[],
    ephemeral_columns => '{}',
    mode => 'MERGE_ENTITY_REPLACE',
    era_name => 'valid',
    founding_id_column => 'identity_correlation_id'
);

\echo '--- Verification: Feedback from temporal_merge ---'
SELECT * FROM pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
SELECT * FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id;

\echo '--- Verification: Final state of establishment table ---'
SELECT id, name, address, legal_unit_id, valid_from, valid_until FROM readme.establishment ORDER BY id, valid_from;

--------------------------------------------------------------------------------
\echo '\n--- 2.5. Updatable Views for Simplified Data Management ---'
--------------------------------------------------------------------------------

\echo '--- Creating updatable views for legal_unit ---'
-- A stable function to override now() for deterministic testing
CREATE FUNCTION readme.test_now() RETURNS date AS $$ SELECT '2024-06-01'::date $$ LANGUAGE sql;

SELECT sql_saga.add_for_portion_of_view('readme.legal_unit'::regclass);
SELECT sql_saga.add_current_view('readme.legal_unit'::regclass, current_func_name := 'readme.test_now()');
SELECT sql_saga.add_for_portion_of_view('readme.unit_with_range'::regclass);
\d readme.legal_unit__for_portion_of_valid
\d readme.legal_unit__current_valid
\d readme.unit_with_range__for_portion_of_valid

\echo '\n--- Using the for_portion_of view (Historical Split) ---'
\echo 'Mark legal_unit 1 as inactive for a portion of its history'
UPDATE readme.legal_unit__for_portion_of_valid
SET
    status = 'inactive',
    valid_from = '2023-09-01',
    valid_until = '2023-11-01'
WHERE id = 1;

\echo '--- Verification: legal_unit history after split ---'
SELECT id, status, valid_from, valid_until FROM readme.legal_unit WHERE id = 1 ORDER BY valid_from;

\echo '\n--- Using the for_portion_of view (Historical Correction) ---'
\echo 'Correct the name of legal_unit 1 across its entire history'
-- Note: This is a bulk update on all historical records for this entity.
-- The valid_from/valid_until are NOT provided in the SET clause.
UPDATE readme.legal_unit__for_portion_of_valid
SET name = 'Corrected SpareParts'
WHERE id = 1;

\echo '--- Verification: legal_unit history after correction ---'
SELECT id, name, valid_from, valid_until FROM readme.legal_unit WHERE id = 1 ORDER BY valid_from;


\echo '\n--- Using the current view ---'
\echo '--- INSERT: A new legal unit becomes active ---'
INSERT INTO readme.legal_unit__current_valid (legal_ident, name, status) VALUES ('LU003', 'New Ventures Inc', 'active');
\echo '--- Verification: The new unit is now current ---'
TABLE readme.legal_unit__current_valid ORDER BY legal_ident;

\echo '\n--- UPDATE (SCD Type 2): Change the name of the new unit ---'
-- Simulate the passage of time for the SCD Type 2 update
CREATE OR REPLACE FUNCTION readme.test_now() RETURNS date AS $$ SELECT '2024-07-01'::date $$ LANGUAGE sql;
UPDATE readme.legal_unit__current_valid SET name = 'New Ventures LLC' WHERE legal_ident = 'LU003';
\echo '--- Verification: Full history of the renamed unit ---'
SELECT id, name, valid_from, valid_until FROM readme.legal_unit WHERE legal_ident = 'LU003' ORDER BY valid_from;

\echo '\n--- DELETE (Soft-Delete): Archive the new unit ---'
-- Simulate the passage of time for the soft-delete
CREATE OR REPLACE FUNCTION readme.test_now() RETURNS date AS $$ SELECT '2024-08-01'::date $$ LANGUAGE sql;
DELETE FROM readme.legal_unit__current_valid WHERE legal_ident = 'LU003';
\echo '--- Verification: The new unit is no longer current ---'
TABLE readme.legal_unit__current_valid ORDER BY legal_ident;
\echo '--- Verification: The historical record shows the unit''s timeline has ended ---'
SELECT id, status, valid_from, valid_until FROM readme.legal_unit WHERE legal_ident = 'LU003' ORDER BY valid_from;

--------------------------------------------------------------------------------
\echo '\n--- 3. Deactivation (from README) ---'
--------------------------------------------------------------------------------
\echo '--- Deactivating sql_saga ---'
-- Drop the views first, as they depend on the underlying temporal table setup.
SELECT sql_saga.drop_for_portion_of_view('readme.legal_unit'::regclass);
SELECT sql_saga.drop_current_view('readme.legal_unit'::regclass);
SELECT sql_saga.drop_for_portion_of_view('readme.unit_with_range'::regclass);

-- Foreign keys must be dropped before the unique keys they reference.
-- For temporal tables, era_name is not needed if the table has only one era.
SELECT sql_saga.drop_foreign_key(
    table_oid => 'readme.establishment'::regclass,
    column_names => ARRAY['legal_unit_id']
);
-- For regular tables, era_name is always omitted.
SELECT sql_saga.drop_foreign_key(
    table_oid => 'readme.projects'::regclass,
    column_names => ARRAY['legal_unit_id']
);

SELECT sql_saga.drop_unique_key(table_oid => 'readme.establishment'::regclass, column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.drop_unique_key(table_oid => 'readme.establishment'::regclass, column_names => ARRAY['name'], era_name => 'valid');
SELECT sql_saga.drop_era('readme.establishment'::regclass);


SELECT sql_saga.drop_unique_key(table_oid => 'readme.legal_unit'::regclass, column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.drop_unique_key(table_oid => 'readme.legal_unit'::regclass, column_names => ARRAY['legal_ident'], era_name => 'valid');
-- For predicated unique keys, the predicate is not needed for dropping.
SELECT sql_saga.drop_unique_key(table_oid => 'readme.legal_unit'::regclass, column_names => ARRAY['name'], era_name => 'valid');
SELECT sql_saga.drop_era('readme.legal_unit'::regclass);

SELECT sql_saga.drop_unique_key(table_oid => 'readme.unit_with_range'::regclass, column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.drop_era('readme.unit_with_range'::regclass);

\echo '--- Verification: Check metadata tables are empty for this schema ---'
-- These queries should return no rows. Any rows returned indicate a cleanup failure.
SELECT * FROM sql_saga.era WHERE table_schema = 'readme';
SELECT * FROM sql_saga.unique_keys WHERE table_schema = 'readme';
SELECT * FROM sql_saga.foreign_keys WHERE table_schema = 'readme';


--------------------------------------------------------------------------------
\echo '--- 4. Cleanup ---'
--------------------------------------------------------------------------------
DROP FUNCTION readme.test_now();
DROP TABLE readme.legal_unit, readme.establishment, readme.projects, readme.unit_with_range CASCADE;
DROP SCHEMA readme;

ROLLBACK;
\i sql/include/test_teardown.sql
