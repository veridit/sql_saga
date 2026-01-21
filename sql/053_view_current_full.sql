\i sql/include/test_setup.sql

CREATE ROLE view_test_role;

BEGIN;

-- A stable function to override now() for deterministic testing
CREATE FUNCTION test_now() RETURNS date AS $$ SELECT '2024-02-29'::date $$ LANGUAGE sql;
GRANT EXECUTE ON FUNCTION test_now() TO PUBLIC;

-- Scenario 1: Test with a table in a non-public schema
CREATE SCHEMA test_schema;
CREATE TABLE test_schema.schema_test (
    id int,
    value text,
    valid_range daterange,
    valid_from date,
    valid_until date
);
SELECT sql_saga.add_era('test_schema.schema_test', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('test_schema.schema_test', ARRAY['id'], key_type => 'primary');
SELECT sql_saga.add_current_view('test_schema.schema_test'::regclass, delete_mode := 'delete_as_cutoff', current_func_name := 'test_now()');

\d test_schema.schema_test__current_valid
TABLE sql_saga.updatable_view;

INSERT INTO test_schema.schema_test__current_valid (id, value) VALUES (1, 'A');
TABLE test_schema.schema_test;
TABLE pg_temp.temporal_merge_plan;
TABLE pg_temp.temporal_merge_feedback;
DROP TABLE pg_temp.temporal_merge_plan;
CALL sql_saga.temporal_merge_drop_cache();
DROP TABLE pg_temp.temporal_merge_feedback;

SELECT sql_saga.drop_current_view('test_schema.schema_test'::regclass);

-- Scenario 2: ACL and Ownership tests
CREATE TABLE acl_test (id int, value text, status text, comment text, valid_range daterange, valid_from date, valid_until date);
ALTER TABLE acl_test OWNER to view_test_role;
SELECT sql_saga.add_era('acl_test', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('acl_test', ARRAY['id']);

SET ROLE view_test_role;
-- This should succeed as the role owns the table
SELECT sql_saga.add_current_view('acl_test'::regclass, delete_mode := 'delete_as_documented_ending', current_func_name := 'test_now()');
RESET ROLE;

-- Verify owner of the view is correct
\d acl_test__current_valid
TABLE sql_saga.updatable_view;

SAVEPOINT preserve_role_and_state_before_no_permission;
-- Verify that permissions are handled correctly
GRANT SELECT ON acl_test TO sql_saga_unprivileged_user;
SET ROLE sql_saga_unprivileged_user;
SELECT CURRENT_ROLE;
SAVEPOINT no_insert;
-- Should fail, as we only have SELECT on the base table, which propagates to the
-- view, but we do not have INSERT on the view.
INSERT INTO acl_test__current_valid (id, value, status) VALUES (1, 'no', 'active');
ROLLBACK TO no_insert;



SAVEPOINT no_update;
-- Should fail, as we only have SELECT on the base table, which propagates to the
-- view, but we do not have UPDATE on the view.
UPDATE acl_test__current_valid SET value = 'no update' WHERE id = 2;
ROLLBACK TO no_update;
ROLLBACK TO SAVEPOINT preserve_role_and_state_before_no_permission;

-- Test that INSERT permission on base table doesn't grant UPDATE
SAVEPOINT preserve_role_and_state_before_failing_update;
GRANT INSERT ON acl_test TO sql_saga_unprivileged_user;
SET ROLE sql_saga_unprivileged_user;
-- This should fail before the trigger fires, as the user does not have UPDATE
-- permission on the view. The health_checks trigger ensures that view
-- permissions are synchronized with the base table permissions.
UPDATE acl_test__current_valid SET value = 'no update' WHERE id = 2;
ROLLBACK TO preserve_role_and_state_before_failing_update;

SAVEPOINT can_insert;
GRANT SELECT, INSERT, UPDATE, DELETE ON acl_test TO sql_saga_unprivileged_user;

-- Insert a record with an old start date to test non-empty range soft-delete
INSERT INTO acl_test (id, valid_from, valid_until, value, status, comment) VALUES (2, '2024-01-01', 'infinity', 'initial', 'active', 'pre-existing');

SET ROLE sql_saga_unprivileged_user;
SELECT CURRENT_ROLE;

-- Test soft-delete on a record with a non-empty lifetime.
-- This should perform an UPDATE to close the record.
UPDATE acl_test__current_valid SET valid_from = 'infinity', status = 'deleted', comment = 'Closed pre-existing' WHERE id = 2;
TABLE acl_test ORDER BY id, valid_from;

-- Test soft-delete on a record created in the same "instant".
-- This should perform a DELETE as the record had no valid lifetime.
INSERT INTO acl_test__current_valid (id, value, status) VALUES (1, 'yes', 'active');
TABLE acl_test ORDER BY id, valid_from;
UPDATE acl_test__current_valid SET valid_from = 'infinity', status = 'deleted', comment = 'Closed same-day' WHERE id = 1;
TABLE acl_test ORDER BY id, valid_from;

ROLLBACK TO can_insert;

SELECT sql_saga.drop_current_view('acl_test'::regclass);

-- Scenario 3: Range-only table (no synchronized columns)
-- This tests that the current view works with tables that have ONLY
-- a range column, without valid_from/valid_until/valid_to synchronized columns.
SAVEPOINT scenario_3;

\echo 'Scenario 3: Range-only table for current view'

CREATE TABLE range_only_employees (
    id int,
    valid_range daterange NOT NULL,
    name text,
    department text,
    PRIMARY KEY (id, valid_range WITHOUT OVERLAPS)
);

-- Register era (range-only, no synchronized columns)
SELECT sql_saga.add_era('range_only_employees', 'valid_range');
SELECT sql_saga.add_unique_key('range_only_employees', ARRAY['id']);

-- Populate with initial data using range column
INSERT INTO range_only_employees (id, valid_range, name, department) VALUES
    (1, '[2023-01-01, 2024-01-01)', 'Alice', 'Engineering'),
    (1, '[2024-01-01, infinity)', 'Alice', 'R&D'),
    (2, '[2023-05-01, infinity)', 'Bob', 'Sales');

\echo '--- Initial base table state ---'
TABLE range_only_employees ORDER BY id, valid_range;

-- Add the current view
SELECT sql_saga.add_current_view('range_only_employees'::regclass, current_func_name := 'test_now()');
\d range_only_employees__current_valid

-- Test SELECT: Should show only current records (Alice in R&D and Bob)
\echo '--- Current view SELECT ---'
TABLE range_only_employees__current_valid ORDER BY id;

-- Test INSERT: Carol joins the company
\echo '--- INSERT via current view ---'
INSERT INTO range_only_employees__current_valid (id, name, department)
VALUES (3, 'Carol', 'Marketing');

TABLE range_only_employees ORDER BY id, valid_range;

-- Test UPDATE (SCD Type 2): Bob moves to Management
\echo '--- UPDATE via current view (SCD Type 2) ---'
UPDATE range_only_employees__current_valid SET department = 'Management' WHERE id = 2;

TABLE range_only_employees ORDER BY id, valid_range;

-- Test DELETE (soft delete): Carol leaves (same day as creation = hard delete)
\echo '--- DELETE via current view (soft delete on same-day entity = hard delete) ---'
DELETE FROM range_only_employees__current_valid WHERE id = 3;

TABLE range_only_employees ORDER BY id, valid_range;

-- Current view should now show only Alice and Bob
\echo '--- Final current view state ---'
TABLE range_only_employees__current_valid ORDER BY id;

SELECT sql_saga.drop_current_view('range_only_employees'::regclass);
ROLLBACK TO SAVEPOINT scenario_3;


-- Scenario 4: Era-level ephemeral_columns with CURRENT view
-- Tests that ephemeral_columns set via add_era() are properly configured
-- and that temporal_merge operations via FOR_PORTION_OF will use them.
-- The CURRENT view doesn't directly call temporal_merge with ephemeral_columns
-- for SCD Type 2 updates (it uses direct SQL), but this test verifies:
-- 1. Era-level ephemeral_columns can be configured alongside CURRENT view
-- 2. Combining CURRENT view with FOR_PORTION_OF uses era-level ephemeral_columns
SAVEPOINT scenario_4;

\echo 'Scenario 4: Era-level ephemeral_columns with CURRENT view'

CREATE TABLE current_ephemeral_test (
    id int,
    name text NOT NULL,
    department text,
    valid_range daterange,
    valid_from date,
    valid_until date,
    -- Audit columns: will have DIFFERENT values across adjacent rows
    edit_at timestamptz NOT NULL DEFAULT '2024-01-01 10:00:00+00'::timestamptz,
    edit_by_user_id int NOT NULL DEFAULT 1
);

-- KEY: Set ephemeral_columns at the ERA level via add_era()
SELECT sql_saga.add_era('current_ephemeral_test', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until',
    ephemeral_columns => ARRAY['edit_at', 'edit_by_user_id']);

-- Verify era has ephemeral_columns set
\echo '--- Era configuration shows ephemeral_columns ---'
SELECT era_name, ephemeral_columns FROM sql_saga.era WHERE table_name = 'current_ephemeral_test';

SELECT sql_saga.add_unique_key('current_ephemeral_test', ARRAY['id'], key_type => 'primary');

-- Create CURRENT view (era-level ephemeral_columns are available for any temporal_merge calls)
SELECT sql_saga.add_current_view('current_ephemeral_test'::regclass, current_func_name := 'test_now()');

\d current_ephemeral_test__current_valid

-- Insert initial data via base table (not via CURRENT view to have full control)
-- Create two adjacent rows with DIFFERENT names and DIFFERENT audit values
INSERT INTO current_ephemeral_test (id, name, department, valid_from, valid_until, edit_at, edit_by_user_id)
VALUES
    (1, 'Name A', 'Engineering', '2024-01-01', '2024-02-29', '2024-01-15 10:00:00+00', 100),
    (1, 'Name B', 'Engineering', '2024-02-29', 'infinity', '2024-02-20 09:30:00+00', 200);

\echo '--- Initial State: Two adjacent rows with different names and audit values ---'
SELECT id, name, department, valid_from, valid_until, edit_at, edit_by_user_id
FROM current_ephemeral_test
ORDER BY valid_from;

-- Add FOR_PORTION_OF view to test coalescing with era-level ephemeral_columns
SELECT sql_saga.add_for_portion_of_view('current_ephemeral_test'::regclass);

-- Update entire timeline to same name - should coalesce to 1 row
-- (era-level ephemeral_columns excludes edit_at/edit_by_user_id from comparison)
UPDATE current_ephemeral_test__for_portion_of_valid
SET name = 'UNIFIED NAME', valid_from = '2024-01-01', valid_until = 'infinity'
WHERE id = 1;

\echo '--- After FOR_PORTION_OF unification: Expected 1 coalesced row ---'
\echo '--- (era-level ephemeral_columns enables coalescing despite different audit values) ---'
SELECT id, name, department, valid_from, valid_until, edit_at, edit_by_user_id
FROM current_ephemeral_test
ORDER BY valid_from;

-- Verification: should be exactly 1 row
SELECT count(*) as row_count FROM current_ephemeral_test;

SELECT sql_saga.drop_for_portion_of_view('current_ephemeral_test'::regclass);
SELECT sql_saga.drop_current_view('current_ephemeral_test'::regclass);
ROLLBACK TO SAVEPOINT scenario_4;

ROLLBACK;

DROP ROLE view_test_role;

CALL sql_saga.temporal_merge_drop_cache();

\i sql/include/test_teardown.sql
