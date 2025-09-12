\i sql/include/test_setup.sql

--
-- This script reproduces a bug where sql_saga's C-based FK triggers fail to
-- resolve the correct target table when the table name is ambiguous due to the
-- `search_path`.
--

-- Setup schema as superuser before starting transaction.
-- Drop if exists ensures a clean state if the previous run failed.
DROP SCHEMA IF EXISTS saga_bug_test CASCADE;
CREATE SCHEMA saga_bug_test;
GRANT ALL ON SCHEMA saga_bug_test TO sql_saga_unprivileged_user;

BEGIN;

SET ROLE TO sql_saga_unprivileged_user;

SET client_min_messages TO NOTICE;
SET datestyle TO 'ISO, DMY';

-- Set a search_path that will cause ambiguity. Because `public` is first,
-- any unqualified table name that exists in both schemas will resolve to public.
SET search_path TO public, saga_bug_test;

-- 1. Setup Tables

-- Create the "impostor" table in the `public` schema.
CREATE TABLE public.legal_unit (
    id INT NOT NULL,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL,
    name TEXT,
    PRIMARY KEY (id, valid_from)
);
SELECT sql_saga.add_era('public.legal_unit', 'valid_from', 'valid_until');
-- On the impostor table, create a unique key with a non-interfering name.
SELECT sql_saga.add_unique_key('public.legal_unit', ARRAY['id'], 'valid', key_type => 'natural', unique_key_name => 'impostor_uk');


-- Create the real parent table in the `saga_bug_test` schema.
CREATE TABLE saga_bug_test.legal_unit (
    id INT NOT NULL,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL,
    name TEXT,
    PRIMARY KEY (id, valid_from)
);
SELECT sql_saga.add_era('saga_bug_test.legal_unit', 'valid_from', 'valid_until');
-- Use a different, non-interfering name for the unique key on the correct table.
SELECT sql_saga.add_unique_key('saga_bug_test.legal_unit', ARRAY['id'], 'valid', key_type => 'natural', unique_key_name => 'correct_table_uk');

-- Child table with a temporal FK
CREATE TABLE saga_bug_test.establishment (
    id INT NOT NULL,
    legal_unit_id INT, -- Temporal FK
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL,
    name TEXT,
    PRIMARY KEY (id, valid_from)
);
SELECT sql_saga.add_era('saga_bug_test.establishment', 'valid_from', 'valid_until');
SELECT sql_saga.add_unique_key('saga_bug_test.establishment', ARRAY['id'], key_type => 'natural');

-- Add the temporal FK, correctly pointing to the unique key on the
-- `saga_bug_test.legal_unit` table.
-- If the bug exists, the C-trigger will get confused by the search_path and
-- check `public.legal_unit` anyway, causing a failure.
-- If the bug is fixed, the trigger will check the correct table, and the
-- INSERT will succeed.
SELECT sql_saga.add_foreign_key(
    'saga_bug_test.establishment',
    ARRAY['legal_unit_id'],
    'valid',
    'correct_table_uk' -- This UK is on saga_bug_test.legal_unit
);

-- 2. Insert Data
-- Insert the parent record into the *correct* table.
INSERT INTO saga_bug_test.legal_unit (id, valid_from, valid_until, name) VALUES
(1, '2023-12-31', 'infinity', 'Parent LU');

-- This INSERT should succeed.
-- EXPECTED BEHAVIOR: INSERT succeeds because the parent record exists in `saga_bug_test.legal_unit`.
-- ACTUAL BEHAVIOR (BUG): INSERT fails with a foreign key violation. The trigger
-- incorrectly checks for a parent record in `public.legal_unit` (the impostor
-- table), which is empty.
INSERT INTO saga_bug_test.establishment (id, legal_unit_id, valid_from, valid_until, name) VALUES
(101, 1, '2024-01-01', '2025-01-01', 'Child EST');

-- If we get here, the bug is fixed.
SELECT 'BUG IS FIXED: INSERT into child table succeeded.' as status;

-- 3. Verify UPDATE trigger
-- Insert another parent record and a child record to be updated.
INSERT INTO saga_bug_test.legal_unit (id, valid_from, valid_until, name) VALUES
(2, '2023-12-31', 'infinity', 'Another Parent LU');

INSERT INTO saga_bug_test.establishment (id, legal_unit_id, valid_from, valid_until, name) VALUES
(102, 2, '2024-01-02', 'infinity', 'Child EST to be updated');

-- This UPDATE should succeed.
-- The `fk_update_check_c` trigger will fire. If the bug still existed, it
-- would incorrectly check `public.legal_unit` and fail. With the fix, it
-- checks the correct table and succeeds.
UPDATE saga_bug_test.establishment
SET legal_unit_id = 1
WHERE id = 102;

-- If we get here, the UPDATE trigger is also fixed.
SELECT 'BUG IS FIXED: UPDATE on child table succeeded.' as status;


ROLLBACK;

\i sql/include/test_teardown.sql
