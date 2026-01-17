\i sql/include/test_setup.sql

CREATE ROLE view_test_role;

BEGIN;

-- Scenario 1: Test with a table in a non-public schema
SAVEPOINT scenario_1;
CREATE SCHEMA test_schema;
CREATE TABLE test_schema.schema_test (
    id int,
    value text,
    valid_range daterange,
    valid_from date,
    valid_until date
);
SELECT sql_saga.add_era('test_schema.schema_test', 'valid_range');
SELECT sql_saga.add_unique_key('test_schema.schema_test', ARRAY['id']);
SELECT sql_saga.add_for_portion_of_view('test_schema.schema_test'::regclass);

\d test_schema.schema_test__for_portion_of_valid
TABLE sql_saga.updatable_view;

-- DML must be on the base table now
INSERT INTO test_schema.schema_test (id, value, valid_from, valid_until) VALUES (1, 'A', '2024-01-01', 'infinity');
TABLE test_schema.schema_test;

SELECT sql_saga.drop_for_portion_of_view('test_schema.schema_test'::regclass);
ROLLBACK TO SAVEPOINT scenario_1;

-- Scenario 2: ACL and Ownership tests
SAVEPOINT scenario_2;
CREATE TABLE acl_test (id int, value text, valid_range daterange, valid_from date, valid_until date);
ALTER TABLE acl_test OWNER to view_test_role;
SELECT sql_saga.add_era('acl_test', 'valid_range');
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
-- Drop the session's temporary schema to avoid permission issues when changing roles.
DROP SCHEMA IF EXISTS pg_temp CASCADE;
SET ROLE sql_saga_unprivileged_user;
SELECT CURRENT_ROLE;
-- Should fail, as we only have SELECT on the base table
UPDATE acl_test__for_portion_of_valid SET value = 'no', valid_from = '2024-02-01', valid_until = '2024-03-01' WHERE id = 1;
ROLLBACK TO no_update;

-- An UPDATE that splits a record should now succeed even without DELETE permission.
SAVEPOINT expect_update_without_delete_priv;
GRANT UPDATE, INSERT ON acl_test TO sql_saga_unprivileged_user;
-- Drop the session's temporary schema to avoid permission issues when changing roles.
DROP SCHEMA IF EXISTS pg_temp CASCADE;
SET ROLE sql_saga_unprivileged_user;
UPDATE acl_test__for_portion_of_valid SET value = 'no', valid_from = '2024-02-01', valid_until = '2024-03-01' WHERE id = 1;
TABLE acl_test ORDER BY valid_from;
ROLLBACK TO SAVEPOINT expect_update_without_delete_priv;
RESET ROLE;


SAVEPOINT can_update;
-- NOTE: With DELETE permission, the update should definitely succeed.
GRANT UPDATE, INSERT, DELETE ON acl_test TO sql_saga_unprivileged_user;
-- Drop the session's temporary schema to avoid permission issues when changing roles.
DROP SCHEMA IF EXISTS pg_temp CASCADE;
SET ROLE sql_saga_unprivileged_user;
SELECT CURRENT_ROLE;
-- Should now succeed
UPDATE acl_test__for_portion_of_valid SET value = 'yes', valid_from = '2024-02-01', valid_until = '2024-03-01' WHERE id = 1;
TABLE acl_test ORDER BY valid_from;
ROLLBACK TO can_update;

SELECT sql_saga.drop_for_portion_of_view('acl_test'::regclass);
ROLLBACK TO SAVEPOINT scenario_2;


-- Scenario 3: Drop behavior
SAVEPOINT scenario_3;
CREATE TABLE drop_test (id int, value text, valid_range daterange, valid_from date, valid_until date, PRIMARY KEY (id, valid_range WITHOUT OVERLAPS));
SELECT sql_saga.add_era('drop_test', 'valid_range');
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
CREATE TABLE drop_test (id int, value text, valid_range daterange, valid_from date, valid_until date, PRIMARY KEY (id, valid_range WITHOUT OVERLAPS));
SELECT sql_saga.add_era('drop_test', 'valid_range');
SELECT sql_saga.add_for_portion_of_view('drop_test'::regclass);

-- Dropping table directly should fail due to dependent view
SAVEPOINT expect_fail;
DROP TABLE drop_test;
ROLLBACK TO SAVEPOINT expect_fail;

-- We must drop the view first, then the table
SELECT sql_saga.drop_for_portion_of_view('drop_test'::regclass);
DROP TABLE drop_test;
ROLLBACK TO SAVEPOINT scenario_3;


-- Scenario 4: Table with no primary key (using temporal unique key as identifier)
SAVEPOINT scenario_4;
CREATE TABLE no_pk_test (
    id integer,
    value text,
    valid_range daterange,
    valid_from date,
    valid_until date
);
SELECT sql_saga.add_era('no_pk_test', 'valid_range');
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
ROLLBACK TO SAVEPOINT scenario_4;


-- Scenario 5: Table with non-B-tree-indexable column types
SAVEPOINT scenario_5;
CREATE TABLE non_btree_test (
    id integer GENERATED BY DEFAULT AS IDENTITY,
    pt point,
    value text,
    valid int4range,
    valid_from integer,
    valid_until integer,
    PRIMARY KEY (id, valid WITHOUT OVERLAPS)
);
SELECT sql_saga.add_era('non_btree_test', 'valid', 'p');
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
ROLLBACK TO SAVEPOINT scenario_5;


-- Scenario 6: Test for handling of various generated/identity columns
-- in the FOR PORTION OF update trigger.
SAVEPOINT scenario_6;

-- Drop the session's temporary schema to avoid permission issues when changing roles.
DROP SCHEMA IF EXISTS pg_temp CASCADE;

SET ROLE sql_saga_unprivileged_user;

-- The update_portion_of() trigger function must correctly identify and
-- exclude all varieties of generated columns (SERIAL, IDENTITY, STORED)
-- from the INSERT statements it dynamically generates when splitting a row.
CREATE TABLE gen_cols_test (
    id_serial BIGSERIAL,
    id_gen_default BIGINT GENERATED BY DEFAULT AS IDENTITY,
    id_gen_always BIGINT GENERATED ALWAYS AS IDENTITY,
    id_stored BIGINT GENERATED ALWAYS AS (id_serial * 2) STORED,
    product TEXT,
    """valid_range""" int4range,
    """from""" INTEGER,
    """until""" INTEGER,
    price NUMERIC,
    PRIMARY KEY (id_gen_default, """valid_range""" WITHOUT OVERLAPS)
);

-- Add sql_saga features with non-standard, quoted column names
SELECT sql_saga.add_era('gen_cols_test', '"valid_range"', era_name => 'p',
    valid_from_column_name => '"from"', 
    valid_until_column_name => '"until"');
SELECT sql_saga.add_for_portion_of_view('gen_cols_test', 'p');

-- Insert initial data
INSERT INTO gen_cols_test (product, """from""", """until""", price) VALUES ('Widget', 10, 20, 100);

TABLE gen_cols_test;

-- Perform an update that splits the existing row.
-- This will trigger an INSERT within the update_portion_of function.
-- If the generated columns are not correctly excluded from the INSERT,
-- this statement will fail, especially for the GENERATED ALWAYS columns.
UPDATE gen_cols_test__for_portion_of_p SET """from""" = 15, """until""" = 20, price = 80;
-- Verify the result. We expect two rows now.
TABLE gen_cols_test ORDER BY """from""";

-- This also automatically rolls back the SET ROLE
ROLLBACK TO SAVEPOINT scenario_6;


-- Scenario 7: Temporal foreign key with DELETE-first execution strategy
-- Demonstrates that DEFERRABLE temporal FKs work with DELETE-first execution.
-- The DELETE-first strategy creates temporary gaps, which are tolerated by
-- DEFERRABLE constraints and checked at transaction end.
SAVEPOINT scenario_7;

CREATE TABLE fk_parent (
    id integer,
    value text,
    valid_range daterange,
    valid_from date,
    valid_until date,
    PRIMARY KEY (id, valid_range WITHOUT OVERLAPS)
);
SELECT sql_saga.add_era('fk_parent', 'valid_range');
SELECT sql_saga.add_unique_key('fk_parent', ARRAY['id']);
SELECT sql_saga.add_for_portion_of_view('fk_parent');

CREATE TABLE fk_child (
    id integer,
    parent_id integer,
    value text,
    valid_range daterange,
    valid_from date,
    valid_until date,
    PRIMARY KEY (id, valid_range WITHOUT OVERLAPS)
);
SELECT sql_saga.add_era('fk_child', 'valid_range');
SELECT sql_saga.add_unique_key('fk_child', ARRAY['id']);
SELECT sql_saga.add_foreign_key(
    'fk_child'::regclass, ARRAY['parent_id'],
    'fk_parent'::regclass, ARRAY['id']
);

INSERT INTO fk_parent (id, value, valid_from, valid_until) VALUES (1, 'parent', '2024-01-01', '2025-01-01');
INSERT INTO fk_child (id, parent_id, value, valid_from, valid_until) VALUES (101, 1, 'child', '2024-01-01', '2025-01-01');

-- This update now succeeds due to the temporal_merge rewrite.
-- The INSERT-then-UPDATE strategy avoids the transient foreign key violation
-- that would occur with a DELETE-then-INSERT approach.
SAVEPOINT expect_fk_update_to_succeed;
UPDATE fk_parent__for_portion_of_valid SET value = 'updated', valid_from = '2024-01-01', valid_until = '2024-06-01' WHERE id = 1;
TABLE fk_parent ORDER BY valid_range;
ROLLBACK TO SAVEPOINT expect_fk_update_to_succeed;

ROLLBACK TO SAVEPOINT scenario_7;


-- Scenario 8: Test valid_to handling, coalescing, and exact matches
SAVEPOINT scenario_8;
CREATE TABLE valid_to_test (
    id integer,
    value text,
    valid_range daterange,
    valid_from date,
    valid_to date,
    valid_until date,
    PRIMARY KEY (id, valid_range WITHOUT OVERLAPS)
);
SELECT sql_saga.add_era('valid_to_test', 'valid_range', valid_to_column_name => 'valid_to');
SELECT sql_saga.add_for_portion_of_view('valid_to_test');

INSERT INTO valid_to_test (id, value, valid_from, valid_until) VALUES (1, 'A', '2024-01-01', '2025-01-01');
TABLE valid_to_test;

-- Test 1: Using valid_to should now work correctly.
SAVEPOINT test_valid_to;
-- The new trigger implementation correctly derives valid_until from valid_to.
UPDATE valid_to_test__for_portion_of_valid SET value = 'B', valid_from = '2024-02-01', valid_to = '2024-02-28' WHERE id = 1;
TABLE valid_to_test ORDER BY valid_from;
ROLLBACK TO SAVEPOINT test_valid_to;

-- Test 2: Coalescing of adjacent identical segments.
SAVEPOINT test_coalescing;
-- Before changing
TABLE valid_to_test ORDER BY valid_from;
-- Update one slice. This creates a 3-way split.
UPDATE valid_to_test__for_portion_of_valid SET value = 'B', valid_from = '2024-02-01', valid_until = '2024-03-01' WHERE id = 1;
-- After changing
TABLE valid_to_test ORDER BY valid_from;

-- Update an adjacent slice with the same data. The new logic should coalesce them.
UPDATE valid_to_test__for_portion_of_valid SET value = 'B', valid_from = '2024-03-01', valid_until = '2024-04-01' WHERE id = 1;
-- The rewritten version correctly coalesces the two 'B' rows into a single row.
TABLE valid_to_test ORDER BY valid_from;
ROLLBACK TO SAVEPOINT test_coalescing;

-- Verify state after rollback
TABLE valid_to_test;

-- Test 3: Exact match update. This should now perform a simple UPDATE.
SAVEPOINT test_exact_match;
UPDATE valid_to_test__for_portion_of_valid SET value = 'C', valid_from = '2024-01-01', valid_to = '2024-12-31' WHERE id = 1;
-- We expect one row, with the value updated.
TABLE valid_to_test ORDER BY valid_from;
ROLLBACK TO SAVEPOINT test_exact_match;

-- Test 4: Exact match UPDATE with WHERE clause (data correction on a specific historical slice)
SAVEPOINT exact_match_update_with_where_should_succeed;
UPDATE valid_to_test__for_portion_of_valid
SET
    value = 'D',
    valid_from = '2024-01-01',
    valid_to = '2024-12-31'
WHERE id = 1
  -- For a REST query on a view with valid_to, this is the expected pattern
  AND valid_from >= '2024-01-01'
  AND valid_to <= '2024-12-31';
-- Since this is an exact match, temporal_merge should generate a simple UPDATE.
-- There should be no DELETE.
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
TABLE pg_temp.temporal_merge_feedback ORDER BY source_row_id;

-- Verify value is updated on just that one row.
TABLE valid_to_test ORDER BY valid_from;
ROLLBACK TO exact_match_update_with_where_should_succeed;


-- Test 5: Exact match UPDATE without WHERE on temporal columns
SAVEPOINT exact_temporal_match_without_where_should_succeed;
UPDATE valid_to_test__for_portion_of_valid
SET
    value = 'E',
    valid_from = '2024-01-01',
    valid_to = '2024-12-31'
WHERE id = 1;
-- Verify there is not DELETE in the plan
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
TABLE pg_temp.temporal_merge_feedback ORDER BY source_row_id;

-- Verify value is updated on just that one row.
TABLE valid_to_test ORDER BY valid_from;
ROLLBACK TO exact_temporal_match_without_where_should_succeed;


ROLLBACK TO SAVEPOINT scenario_8;


-- Scenario 9: Reproduce statbus regression with valid_to, where an incorrect plan is generated.
SAVEPOINT scenario_9;
CREATE TABLE statbus_repro (
    id integer,
    category_id integer,
    valid_range daterange,
    valid_from date,
    valid_to date,
    valid_until date,
    PRIMARY KEY (id, valid_range WITHOUT OVERLAPS)
);
SELECT sql_saga.add_era('statbus_repro', 'valid_range', valid_to_column_name => 'valid_to');
SELECT sql_saga.add_unique_key('statbus_repro', ARRAY['id']);
SELECT sql_saga.add_for_portion_of_view('statbus_repro');

INSERT INTO statbus_repro (id, category_id, valid_from, valid_to) VALUES (1, 13, '2023-01-01', '2025-12-31');
TABLE statbus_repro;

-- Test 1: Update using valid_until (the baseline for correct behavior)
SAVEPOINT test_valid_until;
UPDATE statbus_repro__for_portion_of_valid
 SET category_id = 30, valid_from = '2024-01-01', valid_until = '2025-01-01'
 WHERE id = 1;

-- Check plan: should be 1 UPDATE and 2 INSERTs for the 3-way split
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
-- Check result
TABLE statbus_repro ORDER BY valid_from;
ROLLBACK TO SAVEPOINT test_valid_until;


-- Test 2: Update using valid_to (this is expected to show the regression)
SAVEPOINT test_valid_to;
UPDATE statbus_repro__for_portion_of_valid
 SET category_id = 30, valid_from = '2024-01-01', valid_to = '2024-12-31'
 WHERE id = 1;

-- Check plan: should be 1 UPDATE and 2 INSERTs for the 3-way split
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;
-- Check result
TABLE statbus_repro ORDER BY valid_from;
ROLLBACK TO SAVEPOINT test_valid_to;


ROLLBACK TO SAVEPOINT scenario_9;

-- Scenario 10: Era validation - should fail when era doesn't exist
SAVEPOINT scenario_10;

CREATE TABLE no_era_test (
    id int,
    value text,
    valid_range daterange
);

-- Should fail: no era registered on this table
SAVEPOINT expect_error_1;
SELECT sql_saga.add_for_portion_of_view('no_era_test'::regclass);
ROLLBACK TO SAVEPOINT expect_error_1;

-- Should also fail with explicit non-existent era name
SAVEPOINT expect_error_2;
SELECT sql_saga.add_for_portion_of_view('no_era_test'::regclass, 'nonexistent');
ROLLBACK TO SAVEPOINT expect_error_2;

ROLLBACK TO SAVEPOINT scenario_10;

ROLLBACK;

DROP ROLE view_test_role;

\i sql/include/test_teardown.sql
