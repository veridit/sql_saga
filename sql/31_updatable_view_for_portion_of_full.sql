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

-- DML must be on the base table now
INSERT INTO test_schema.schema_test VALUES (1, 'A', '2024-01-01', 'infinity');
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
INSERT INTO acl_test (id, value, valid_from, valid_until) VALUES (1, 'initial', '2024-01-01', 'infinity');
GRANT SELECT ON acl_test TO sql_saga_unprivileged_user;

SAVEPOINT no_update;
SET ROLE sql_saga_unprivileged_user;
SELECT CURRENT_ROLE;
-- Should fail, as we only have SELECT on the base table
UPDATE acl_test__for_portion_of_valid SET value = 'no', valid_from = '2024-02-01', valid_until = '2024-03-01' WHERE id = 1;
ROLLBACK TO no_update;

SAVEPOINT can_update;
GRANT UPDATE, INSERT ON acl_test TO sql_saga_unprivileged_user;
SET ROLE sql_saga_unprivileged_user;
SELECT CURRENT_ROLE;
-- Should now succeed
UPDATE acl_test__for_portion_of_valid SET value = 'yes', valid_from = '2024-02-01', valid_until = '2024-03-01' WHERE id = 1;
TABLE acl_test ORDER BY valid_from;
ROLLBACK TO can_update;

SELECT sql_saga.drop_for_portion_of_view('acl_test'::regclass);


-- Scenario 3: Drop behavior
CREATE TABLE drop_test (id int PRIMARY KEY, value text, valid_from date, valid_until date);
SELECT sql_saga.add_era('drop_test', 'valid_from', 'valid_until');
SELECT sql_saga.add_for_portion_of_view('drop_test'::regclass);

-- RESTRICT should fail because the view depends on the era
SAVEPOINT before_drop_era;
SELECT sql_saga.drop_era('drop_test'::regclass, 'valid', 'RESTRICT');
ROLLBACK TO before_drop_era;

-- CASCADE should succeed and drop the view
SELECT sql_saga.drop_era('drop_test'::regclass, 'valid', 'CASCADE');
-- The view should be gone from the metadata
TABLE sql_saga.updatable_view;
-- DROP TABLE should now succeed
DROP TABLE drop_test;

-- Recreate for next test
CREATE TABLE drop_test (id int PRIMARY KEY, value text, valid_from date, valid_until date);
SELECT sql_saga.add_era('drop_test', 'valid_from', 'valid_until');
SELECT sql_saga.add_for_portion_of_view('drop_test'::regclass);

-- Dropping table directly should fail due to dependent view
SAVEPOINT expect_fail;
DROP TABLE drop_test;
ROLLBACK TO SAVEPOINT expect_fail;

-- We must drop the view first, then the table
SELECT sql_saga.drop_for_portion_of_view('drop_test'::regclass);
DROP TABLE drop_test;


-- Scenario 4: Table with no primary key (using temporal unique key as identifier)
CREATE TABLE no_pk_test (
    id integer,
    value text,
    valid_from date,
    valid_until date
);
SELECT sql_saga.add_era('no_pk_test', 'valid_from', 'valid_until');
SELECT sql_saga.add_unique_key('no_pk_test', ARRAY['id']);
SELECT sql_saga.add_for_portion_of_view('no_pk_test');
INSERT INTO no_pk_test (id, value, valid_from, valid_until) VALUES (1, 'initial', '2020-01-01', '2021-01-01');
TABLE no_pk_test;
UPDATE no_pk_test__for_portion_of_valid SET value = 'updated', valid_from = '2020-06-01', valid_until = '2020-09-01' WHERE id = 1;
TABLE no_pk_test ORDER BY valid_from;
SELECT sql_saga.drop_for_portion_of_view('no_pk_test'::regclass);
SELECT sql_saga.drop_unique_key('no_pk_test', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('no_pk_test');
DROP TABLE no_pk_test;


-- Scenario 5: Table with non-B-tree-indexable column types
CREATE TABLE non_btree_test (
    id integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    pt point,
    value text,
    valid_from integer,
    valid_until integer
);
SELECT sql_saga.add_era('non_btree_test', 'valid_from', 'valid_until', 'p');
SELECT sql_saga.add_for_portion_of_view('non_btree_test', 'p');
INSERT INTO non_btree_test (pt, value, valid_from, valid_until) VALUES ('(0, 0)', 'sample', 10, 41);
TABLE non_btree_test;

-- Test with explicit WHERE clause on PK
SAVEPOINT before_btree_update;
UPDATE non_btree_test__for_portion_of_p SET value = 'simple', valid_from = 21, valid_until = 31 WHERE id = 1;
TABLE non_btree_test ORDER BY valid_from;
ROLLBACK TO before_btree_update;

-- Check that state was restored
TABLE non_btree_test;

-- Test without explicit WHERE clause (legacy behavior)
UPDATE non_btree_test__for_portion_of_p SET value = 'simple', valid_from = 21, valid_until = 31;
TABLE non_btree_test ORDER BY valid_from;

SELECT sql_saga.drop_for_portion_of_view('non_btree_test', 'p');
SELECT sql_saga.drop_era('non_btree_test', 'p');
DROP TABLE non_btree_test;


ROLLBACK;

DROP ROLE view_test_role;

\i sql/include/test_teardown.sql
