\i sql/include/test_setup.sql

CREATE ROLE view_test_role;

BEGIN;

-- Scenario 1: Test with a table in a non-public schema
CREATE SCHEMA test_schema;
CREATE TABLE test_schema.schema_test (
    id int,
    value text,
    valid_from date,
    valid_until date
);
SELECT sql_saga.add_era('test_schema.schema_test', 'valid_from', 'valid_until');
SELECT sql_saga.add_unique_key('test_schema.schema_test', ARRAY['id']);
SELECT sql_saga.add_for_portion_of_view('test_schema.schema_test'::regclass);

\d test_schema.schema_test__for_portion_of_valid
TABLE sql_saga.updatable_view;

INSERT INTO test_schema.schema_test__for_portion_of_valid VALUES (1, 'A', '2024-01-01', 'infinity');
TABLE test_schema.schema_test;

SELECT sql_saga.drop_for_portion_of_view('test_schema.schema_test'::regclass);

-- Scenario 2: ACL and Ownership tests
CREATE TABLE acl_test (id int, value text, valid_from date, valid_until date);
ALTER TABLE acl_test OWNER to view_test_role;
SELECT sql_saga.add_era('acl_test', 'valid_from', 'valid_until');
SELECT sql_saga.add_unique_key('acl_test', ARRAY['id']);

SET ROLE view_test_role;
-- This should succeed as the role owns the table
SELECT sql_saga.add_for_portion_of_view('acl_test'::regclass);
RESET ROLE;

-- Verify owner of the view is correct
\d acl_test__for_portion_of_valid
TABLE sql_saga.updatable_view;

-- Verify that permissions are handled correctly
GRANT SELECT ON acl_test TO sql_saga_unprivileged_user;

SAVEPOINT no_insert;
SET ROLE sql_saga_unprivileged_user;
SELECT CURRENT_ROLE;
-- Should fail, as we only have SELECT on the base table
INSERT INTO acl_test__for_portion_of_valid VALUES (1, 'no', '2024-01-01', 'infinity');
ROLLBACK TO no_insert;

SAVEPOINT can_insert;
GRANT INSERT, UPDATE, DELETE ON acl_test TO sql_saga_unprivileged_user;
SET ROLE sql_saga_unprivileged_user;
SELECT CURRENT_ROLE;
-- Should now succeed
INSERT INTO acl_test__for_portion_of_valid VALUES (1, 'yes', '2024-01-01', 'infinity');
TABLE acl_test;
ROLLBACK TO can_insert;

SELECT sql_saga.drop_for_portion_of_view('acl_test'::regclass);

ROLLBACK;

DROP ROLE view_test_role;

\i sql/include/test_teardown.sql
