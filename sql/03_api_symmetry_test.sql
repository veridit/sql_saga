\i sql/include/test_setup.sql

SET ROLE TO sql_saga_unprivileged_user;

BEGIN;

-- Test that the overloaded drop_* functions work as expected.

CREATE TABLE parent(id int, valid_from int, valid_until int);
SELECT sql_saga.add_era('parent', 'valid_from', 'valid_until', 'p');
SELECT sql_saga.add_unique_key('parent', ARRAY['id'], 'p');

CREATE TABLE child(id int, parent_id int, valid_from int, valid_until int);
SELECT sql_saga.add_era('child', 'valid_from', 'valid_until', 'q');
SELECT sql_saga.add_foreign_key('child', ARRAY['parent_id'], 'q', 'parent_id_p');

-- Test overloaded drop_foreign_key
TABLE sql_saga.foreign_keys;
SELECT sql_saga.drop_foreign_key('child', ARRAY['parent_id'], 'q');
TABLE sql_saga.foreign_keys;

-- Test original drop_foreign_key (with positional and named arguments)
SELECT sql_saga.add_foreign_key('child', ARRAY['parent_id'], 'q', 'parent_id_p');
TABLE sql_saga.foreign_keys;
-- by name (positional)
SELECT sql_saga.drop_foreign_key('child', 'child_parent_id_q');
TABLE sql_saga.foreign_keys;

-- Re-create to test named arguments
SELECT sql_saga.add_foreign_key('child', ARRAY['parent_id'], 'q', 'parent_id_p');
TABLE sql_saga.foreign_keys;
-- by name (named)
SELECT sql_saga.drop_foreign_key(table_oid => 'child', key_name => 'child_parent_id_q');
TABLE sql_saga.foreign_keys;

-- Test overloaded drop_unique_key
TABLE sql_saga.unique_keys;
SELECT sql_saga.drop_unique_key('parent', ARRAY['id'], 'p');
TABLE sql_saga.unique_keys;

-- Test original drop_unique_key (with positional and named arguments)
SELECT sql_saga.add_unique_key('parent', ARRAY['id'], 'p');
TABLE sql_saga.unique_keys;
-- by name (positional)
SELECT sql_saga.drop_unique_key('parent', 'parent_id_p');
TABLE sql_saga.unique_keys;

-- Re-create to test named arguments
SELECT sql_saga.add_unique_key('parent', ARRAY['id'], 'p');
TABLE sql_saga.unique_keys;
-- by name (named)
SELECT sql_saga.drop_unique_key(table_oid => 'parent', key_name => 'parent_id_p');
TABLE sql_saga.unique_keys;

DROP TABLE child;
DROP TABLE parent;

ROLLBACK;

\i sql/include/test_teardown.sql
