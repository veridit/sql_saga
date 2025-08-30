--
-- Test for regular-to-temporal foreign keys
--
-- The test covers the full lifecycle of the feature:
-- 1. Create a temporal table and a regular table.
-- 2. Add a unique key to the temporal table.
-- 3. Add a regular-to-temporal foreign key.
-- 4. Verify that INSERT/UPDATE on the regular table is validated.
-- 5. Verify that UPDATE/DELETE on the temporal table is restricted.
-- 6. Verify DDL protection (e.g., DROP of tables, functions, constraints).
-- 7. Verify rename_following logic for tables and columns.
-- 8. Drop the foreign key.
-- 9. Verify that restrictions are removed.
--

\i sql/include/test_setup.sql

BEGIN;

-- 1. Create a temporal table and a regular table.
CREATE TABLE temporal_pk (
    id integer NOT NULL,
    name text,
    valid_from date,
    valid_until date
);

CREATE TABLE regular_fk (
    id integer PRIMARY KEY,
    pk_id integer,
    name text
);

-- 2. Add a unique key to the temporal table.
SELECT sql_saga.add_era('temporal_pk', 'valid_from', 'valid_until');
-- Provide an explicit name for the unique key to make it easy to reference
SELECT sql_saga.add_unique_key(
    table_oid => 'temporal_pk',
    column_names => ARRAY['id'],
    unique_key_name => 'temporal_pk_id_key'
);

-- 3. Add a regular-to-temporal foreign key.
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'regular_fk',
    fk_column_names => ARRAY['pk_id'],
    unique_key_name => 'temporal_pk_id_key'
);

-- Verify helper function and CHECK constraint were created
\echo
\echo "--- After add_foreign_key ---"
-- The helper function should be in the schema of the referenced table and have a predictable name.
\df+ public.temporal_pk_id_exists
-- The CHECK constraint should be on the regular_fk table
\d regular_fk

-- 4. Verify that INSERT/UPDATE on the regular table is validated.
\echo
\echo "--- DML Validation ---"

-- First, insert a valid row into the referenced temporal table
INSERT INTO temporal_pk (id, name, valid_from, valid_until)
VALUES (1, 'Entity One', '2023-01-01', 'infinity');

-- Test valid INSERTs
INSERT INTO regular_fk (id, pk_id, name) VALUES (101, 1, 'Link to Entity One');
INSERT INTO regular_fk (id, pk_id, name) VALUES (102, NULL, 'No link');
TABLE regular_fk ORDER BY id;

-- Test invalid INSERT
-- This should fail because pk_id=2 does not exist in temporal_pk
SAVEPOINT before_invalid_insert;
INSERT INTO regular_fk (id, pk_id, name) VALUES (103, 2, 'Link to non-existent');
ROLLBACK TO SAVEPOINT before_invalid_insert;

-- Test valid UPDATE
UPDATE regular_fk SET pk_id = 1 WHERE id = 102;
TABLE regular_fk ORDER BY id;

-- Test invalid UPDATE
-- This should fail because pk_id=2 does not exist in temporal_pk
SAVEPOINT before_invalid_update;
UPDATE regular_fk SET pk_id = 2 WHERE id = 101;
ROLLBACK TO SAVEPOINT before_invalid_update;
TABLE regular_fk ORDER BY id;

-- 5. Verify that UPDATE/DELETE on the temporal table is restricted.
\echo
\echo "--- UK Trigger Validation ---"

-- Test that a DELETE on the referenced table is blocked
SAVEPOINT before_uk_delete;
-- This should fail because regular_fk.pk_id = 1 references this row.
DELETE FROM temporal_pk WHERE id = 1;
ROLLBACK TO SAVEPOINT before_uk_delete;

-- Test that an UPDATE of the referenced key is blocked
SAVEPOINT before_uk_update;
-- This should fail because it would orphan the reference from regular_fk.
UPDATE temporal_pk SET id = 2 WHERE id = 1;
ROLLBACK TO SAVEPOINT before_uk_update;

-- Test that an UPDATE of a non-key column is allowed
UPDATE temporal_pk SET name = 'Entity One Updated' WHERE id = 1;
TABLE temporal_pk;

-- Verify the row in regular_fk is unchanged
TABLE regular_fk ORDER BY id;


-- 6. Verify DDL protection
\echo
\echo "--- DDL Protection ---"
SAVEPOINT before_ddl;
-- This should fail because the FK helper function depends on the table
DROP TABLE temporal_pk;
ROLLBACK TO SAVEPOINT before_ddl;

SAVEPOINT before_ddl2;
-- This should fail because the CHECK constraint depends on the helper function
DROP FUNCTION public.temporal_pk_id_exists(integer);
ROLLBACK TO SAVEPOINT before_ddl2;

-- 8. Drop the foreign key
\echo
\echo "--- Drop Foreign Key ---"
SELECT sql_saga.drop_foreign_key('regular_fk'::regclass, ARRAY['pk_id']);

-- 9. Verify that restrictions are removed.
\echo
\echo "--- Verify Restrictions Removed ---"
-- Deleting from temporal_pk should now succeed
DELETE FROM temporal_pk WHERE id = 1;
TABLE temporal_pk;

-- The helper function and CHECK constraint should be gone
\df+ public.temporal_pk_id_exists
\d regular_fk

-- 7. Verify rename_following logic is NOT implemented for this FK type yet.
-- This is a known limitation. Event triggers do not yet track CHECK constraints
-- or helper functions. This can be addressed in a future task.

ROLLBACK;

\i sql/include/test_teardown.sql
