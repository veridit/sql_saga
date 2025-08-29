\i sql/include/test_setup.sql

BEGIN;
\echo '----------------------------------------------------------------------------'
\echo 'Test: README Usage Examples'
\echo 'This test provides a runnable, self-contained demonstration of the public'
\echo 'API examples documented in README.md, ensuring they remain correct and'
\echo 'functional. It covers the full lifecycle from setup to data loading'
\echo 'with temporal_merge to teardown.'
\echo '----------------------------------------------------------------------------'

SET client_min_messages TO WARNING;
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
  valid_until DATE,
  valid_to DATE -- Optional: for human-readable inclusive end dates
);

-- Optional: a trigger to keep valid_to and valid_until in sync.
CREATE TRIGGER legal_unit_synchronize_validity
    BEFORE INSERT OR UPDATE ON readme.legal_unit
    FOR EACH ROW EXECUTE FUNCTION sql_saga.synchronize_valid_to_until();

CREATE TABLE readme.establishment (
  id SERIAL NOT NULL,
  name VARCHAR NOT NULL,
  address TEXT NOT NULL,
  legal_unit_id INTEGER, -- Note: Nullable for initial insert before back-filling
  valid_from DATE,
  valid_until DATE
);

CREATE TABLE readme.projects (id serial primary key, name text, legal_unit_id int);

\echo '--- Activating sql_saga ---'
-- Register the table as a temporal table (an "era")
SELECT sql_saga.add_era('readme.legal_unit', 'valid_from', 'valid_until');
-- Add temporal unique keys. A name is generated if the last argument is omitted.
SELECT sql_saga.add_unique_key('readme.legal_unit', ARRAY['id'], unique_key_name => 'legal_unit_id_valid');
SELECT sql_saga.add_unique_key('readme.legal_unit', ARRAY['legal_ident'], unique_key_name => 'legal_unit_legal_ident_valid');
-- Add a predicated unique key (e.g., only active units must have a unique name).
SELECT sql_saga.add_unique_key(
    'readme.legal_unit',
    column_names => ARRAY['name'],
    predicate => 'status = ''active''',
    unique_key_name => 'legal_unit_active_name_valid'
);

SELECT sql_saga.add_era('readme.establishment', 'valid_from', 'valid_until');
SELECT sql_saga.add_unique_key('readme.establishment', ARRAY['id'], unique_key_name => 'establishment_id_valid');
SELECT sql_saga.add_unique_key('readme.establishment', ARRAY['name'], unique_key_name => 'establishment_name_valid');
-- Add a temporal foreign key. It references a temporal unique key.
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'readme.establishment',
    fk_column_names => ARRAY['legal_unit_id'],
    fk_era_name => 'valid',
    unique_key_name => 'legal_unit_id_valid'
);

-- Add a foreign key from a standard table to a temporal table.
-- Note that fk_era_name is omitted for the standard table.
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'readme.projects',
    fk_column_names => ARRAY['legal_unit_id'],
    unique_key_name => 'legal_unit_id_valid'
);

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
    founding_id INT, -- To group related rows for a single new entity
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
(201, 1, 'establishment', NULL, NULL, 'Main Branch', NULL, '123 Innovation Drive', '2023-07-01', 'infinity'),
-- Legal Unit 2: "General Refinement LLC"
(103, 2, 'legal_unit', 'LU002', NULL, 'General Refinement LLC', 'active', NULL, '2023-01-01', 'infinity'),
-- Establishment 2, belonging to Legal Unit 2
(202, 2, 'establishment', NULL, NULL, 'Refinery Plant', NULL, '456 Industrial Ave', '2023-01-01', 'infinity');

\echo '--- Source data for merge ---'
SELECT * FROM source_data ORDER BY row_id;

\echo '\n--- Step 2a: Merge legal_unit data ---'
CREATE TEMP TABLE source_legal_unit ON COMMIT DROP AS SELECT row_id, founding_id, legal_unit_id AS id, legal_ident, name, status, valid_from, valid_until FROM source_data WHERE entity_type = 'legal_unit';

CALL sql_saga.temporal_merge(
    p_target_table => 'readme.legal_unit',
    p_source_table => 'source_legal_unit',
    p_id_columns => '{id}',
    p_ephemeral_columns => '{}',
    p_mode => 'upsert_replace',
    p_era_name => 'valid',
    p_founding_id_column => 'founding_id',
    p_update_source_with_assigned_entity_ids => true
);

\echo '--- Verification: Final state of legal_unit table ---'
SELECT id, legal_ident, name, status, valid_from, valid_until FROM readme.legal_unit ORDER BY id, valid_from;

\echo '--- Verification: Check if IDs were back-filled into the source temp table ---'
SELECT row_id, founding_id, id AS legal_unit_id FROM source_legal_unit ORDER BY row_id;

\echo '\n--- Step 2b: Back-fill generated legal_unit_id to main source table ---'
UPDATE source_data sd
SET legal_unit_id = slu.id
FROM source_legal_unit slu
WHERE sd.founding_id = slu.founding_id;

\echo '--- Verification: Main source table after ID back-fill ---'
SELECT row_id, founding_id, entity_type, legal_unit_id, name FROM source_data ORDER BY row_id;

\echo '\n--- Step 2c: Merge establishment data ---'
CREATE TEMP TABLE source_establishment ON COMMIT DROP AS SELECT row_id, founding_id, NULL::INT AS id, legal_unit_id, name, address, valid_from, valid_until FROM source_data WHERE entity_type = 'establishment';

CALL sql_saga.temporal_merge(
    p_target_table => 'readme.establishment',
    p_source_table => 'source_establishment',
    p_id_columns => '{id}',
    p_ephemeral_columns => '{}',
    p_mode => 'upsert_replace',
    p_era_name => 'valid',
    p_founding_id_column => 'founding_id'
);

\echo '--- Verification: Final state of establishment table ---'
SELECT id, name, address, legal_unit_id, valid_from, valid_until FROM readme.establishment ORDER BY id, valid_from;


--------------------------------------------------------------------------------
\echo '\n--- 3. Deactivation (from README) ---'
--------------------------------------------------------------------------------
\echo '--- Deactivating sql_saga ---'

-- Foreign keys must be dropped before the unique keys they reference.
SELECT sql_saga.drop_foreign_key('readme.establishment'::regclass, ARRAY['legal_unit_id'], 'valid');
-- For standard-to-temporal FKs, era_name is omitted.
SELECT sql_saga.drop_foreign_key('readme.projects'::regclass, ARRAY['legal_unit_id']);

SELECT sql_saga.drop_unique_key('readme.establishment'::regclass, ARRAY['id'], 'valid');
SELECT sql_saga.drop_unique_key('readme.establishment'::regclass, ARRAY['name'], 'valid');
SELECT sql_saga.drop_era('readme.establishment');


SELECT sql_saga.drop_unique_key('readme.legal_unit'::regclass, ARRAY['id'], 'valid');
SELECT sql_saga.drop_unique_key('readme.legal_unit'::regclass, ARRAY['legal_ident'], 'valid');
-- For predicated unique keys, the predicate is not needed for dropping.
SELECT sql_saga.drop_unique_key('readme.legal_unit'::regclass, ARRAY['name'], 'valid');
SELECT sql_saga.drop_era('readme.legal_unit');

\echo '--- Verification: Check metadata tables are empty for this schema ---'
SELECT count(*)::int AS remaining_eras FROM sql_saga.era WHERE table_schema = 'readme';
SELECT count(*)::int AS remaining_uks FROM sql_saga.unique_keys WHERE table_schema = 'readme';
SELECT count(*)::int AS remaining_fks FROM sql_saga.foreign_keys WHERE table_schema = 'readme';


--------------------------------------------------------------------------------
\echo '--- 4. Cleanup ---'
--------------------------------------------------------------------------------
DROP TABLE readme.legal_unit, readme.establishment, readme.projects;
DROP SCHEMA readme;

SET client_min_messages TO NOTICE;
ROLLBACK;
\i sql/include/test_teardown.sql
