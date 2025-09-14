\i sql/include/test_setup.sql

BEGIN;

-- Test the new declarative add_foreign_key function.

-- Scenario 1: Target is a NATURAL key
SAVEPOINT natural_key_target;

-- 1. Setup: Temporal parent and child tables, and a regular child table.
CREATE TABLE parent_natural(id int, valid_from int, valid_until int);
SELECT sql_saga.add_era('parent_natural'::regclass, 'valid_from', 'valid_until', 'p');
SELECT sql_saga.add_unique_key('parent_natural'::regclass, ARRAY['id']::name[], 'p', key_type => 'natural');

CREATE TABLE temporal_child_natural(id int, parent_id int, valid_from int, valid_until int);
SELECT sql_saga.add_era('temporal_child_natural'::regclass, 'valid_from', 'valid_until', 'q');

CREATE TABLE regular_child_natural(id int, parent_id int);

\echo
\echo '--- Test: Declarative add_foreign_key for temporal-to-temporal (natural key) ---'

-- Use the new declarative function
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'temporal_child_natural'::regclass,
    fk_column_names => ARRAY['parent_id'],
    pk_table_oid => 'parent_natural'::regclass,
    pk_column_names => ARRAY['id']
);

-- Verify it was created correctly
TABLE sql_saga.foreign_keys;

-- Cleanup
SELECT sql_saga.drop_foreign_key('temporal_child_natural'::regclass, ARRAY['parent_id']);


\echo
\echo '--- Test: Declarative add_foreign_key for regular-to-temporal (natural key) ---'

-- Use the new declarative function
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'regular_child_natural'::regclass,
    fk_column_names => ARRAY['parent_id'],
    pk_table_oid => 'parent_natural'::regclass,
    pk_column_names => ARRAY['id']
);

-- Verify it was created correctly
TABLE sql_saga.foreign_keys;

-- Cleanup
SELECT sql_saga.drop_foreign_key('regular_child_natural'::regclass, ARRAY['parent_id']);
RELEASE SAVEPOINT natural_key_target;


-- Scenario 2: Target is a PRIMARY key
SAVEPOINT primary_key_target;

-- 1. Setup: Temporal parent and child tables, and a regular child table.
CREATE TABLE parent_pk(
    id int,
    valid_from int,
    valid_until int,
    -- Note: A temporal primary key must include a temporal column
    PRIMARY KEY (id, valid_from) DEFERRABLE
);
SELECT sql_saga.add_era('parent_pk'::regclass, 'valid_from', 'valid_until', 'p');
SELECT sql_saga.add_unique_key('parent_pk'::regclass, ARRAY['id']::name[], 'p', key_type => 'primary');

CREATE TABLE temporal_child_pk(id int, parent_id int, valid_from int, valid_until int);
SELECT sql_saga.add_era('temporal_child_pk'::regclass, 'valid_from', 'valid_until', 'q');

CREATE TABLE regular_child_pk(id int, parent_id int);

\echo
\echo '--- Test: Declarative add_foreign_key for temporal-to-temporal (primary key) ---'

-- Use the new declarative function
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'temporal_child_pk'::regclass,
    fk_column_names => ARRAY['parent_id'],
    pk_table_oid => 'parent_pk'::regclass,
    pk_column_names => ARRAY['id']
);

-- Verify it was created correctly
TABLE sql_saga.foreign_keys;

-- Cleanup
SELECT sql_saga.drop_foreign_key('temporal_child_pk'::regclass, ARRAY['parent_id']);


\echo
\echo '--- Test: Declarative add_foreign_key for regular-to-temporal (primary key) ---'

-- Use the new declarative function
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'regular_child_pk'::regclass,
    fk_column_names => ARRAY['parent_id'],
    pk_table_oid => 'parent_pk'::regclass,
    pk_column_names => ARRAY['id']
);

-- Verify it was created correctly
TABLE sql_saga.foreign_keys;

-- Cleanup
SELECT sql_saga.drop_foreign_key('regular_child_pk'::regclass, ARRAY['parent_id']);
RELEASE SAVEPOINT primary_key_target;


-- Full cleanup
DROP TABLE parent_natural, temporal_child_natural, regular_child_natural;
DROP TABLE parent_pk, temporal_child_pk, regular_child_pk;

ROLLBACK;

\i sql/include/test_teardown.sql
