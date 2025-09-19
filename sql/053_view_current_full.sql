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
    valid_from date,
    valid_until date,
    value text
);
SELECT sql_saga.add_era('test_schema.schema_test', 'valid_from', 'valid_until');
SELECT sql_saga.add_unique_key('test_schema.schema_test', ARRAY['id']);
SELECT sql_saga.add_current_view('test_schema.schema_test'::regclass, delete_mode := 'delete_as_cutoff', current_func_name := 'test_now()');

\d test_schema.schema_test__current_valid
TABLE sql_saga.updatable_view;

INSERT INTO test_schema.schema_test__current_valid (id, value) VALUES (1, 'A');
TABLE test_schema.schema_test;
TABLE pg_temp.temporal_merge_plan;
TABLE pg_temp.temporal_merge_feedback;
DROP TABLE pg_temp.temporal_merge_plan;
DROP TABLE pg_temp.temporal_merge_feedback;
DROP TABLE pg_temp.temporal_merge_cache;

SELECT sql_saga.drop_current_view('test_schema.schema_test'::regclass);

-- Scenario 2: ACL and Ownership tests
CREATE TABLE acl_test (id int, valid_from date, valid_until date, value text, status text, comment text);
ALTER TABLE acl_test OWNER to view_test_role;
SELECT sql_saga.add_era('acl_test', 'valid_from', 'valid_until');
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
INSERT INTO acl_test VALUES (2, '2024-01-01', 'infinity', 'initial', 'active', 'pre-existing');

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

ROLLBACK;

DROP ROLE view_test_role;

\i sql/include/test_teardown.sql
