\i sql/include/test_setup.sql

SET ROLE TO sql_saga_unprivileged_user;

BEGIN;

-- Test that the overloaded drop_* functions work as expected.

CREATE TABLE parent(id int, valid_from int, valid_until int);
SELECT sql_saga.add_era('parent'::regclass, 'valid_from', 'valid_until', 'p');
SELECT sql_saga.add_unique_key('parent'::regclass, ARRAY['id']::name[], 'p', p_key_type => 'natural');

CREATE TABLE child(id int, parent_id int, valid_from int, valid_until int);
SELECT sql_saga.add_era('child'::regclass, 'valid_from', 'valid_until', 'q');
SELECT sql_saga.add_foreign_key('child'::regclass, ARRAY['parent_id']::name[], 'q', 'parent_id_p');

-- Test overloaded drop_foreign_key for temporal-to-temporal
TABLE sql_saga.foreign_keys;
SELECT sql_saga.drop_foreign_key('child'::regclass, ARRAY['parent_id']::name[], 'q');
TABLE sql_saga.foreign_keys;

-- Test original drop_foreign_key (with positional and named arguments)
SELECT sql_saga.add_foreign_key('child'::regclass, ARRAY['parent_id']::name[], 'q', 'parent_id_p');
TABLE sql_saga.foreign_keys;
-- by name (positional)
SELECT sql_saga.drop_foreign_key_by_name('child'::regclass, 'child_parent_id_q');
TABLE sql_saga.foreign_keys;

-- Re-create to test named arguments
SELECT sql_saga.add_foreign_key('child'::regclass, ARRAY['parent_id']::name[], 'q', 'parent_id_p');
TABLE sql_saga.foreign_keys;
-- by name (named)
SELECT sql_saga.drop_foreign_key_by_name(table_oid => 'child'::regclass, key_name => 'child_parent_id_q');
TABLE sql_saga.foreign_keys;

-- Test overloaded drop_foreign_key for regular-to-temporal
CREATE TABLE regular_child(id int, parent_id int);
SELECT sql_saga.add_foreign_key('regular_child'::regclass, ARRAY['parent_id']::name[], 'parent_id_p');
TABLE sql_saga.foreign_keys;
SELECT sql_saga.drop_foreign_key('regular_child'::regclass, ARRAY['parent_id']::name[]);
TABLE sql_saga.foreign_keys;
DROP TABLE regular_child;

-- Test overloaded drop_unique_key
TABLE sql_saga.unique_keys;
SELECT sql_saga.drop_unique_key('parent'::regclass, ARRAY['id']::name[], 'p');
TABLE sql_saga.unique_keys;

-- Test original drop_unique_key (with positional and named arguments)
SELECT sql_saga.add_unique_key('parent'::regclass, ARRAY['id']::name[], 'p', p_key_type => 'natural');
TABLE sql_saga.unique_keys;
-- by name (positional)
SELECT sql_saga.drop_unique_key_by_name('parent'::regclass, 'parent_id_p');
TABLE sql_saga.unique_keys;

-- Re-create to test named arguments
SELECT sql_saga.add_unique_key('parent'::regclass, ARRAY['id']::name[], 'p', p_key_type => 'natural');
TABLE sql_saga.unique_keys;
-- by name (named)
SELECT sql_saga.drop_unique_key_by_name(table_oid => 'parent'::regclass, key_name => 'parent_id_p');
TABLE sql_saga.unique_keys;

-- Test symmetrical add_era/drop_era with column management
CREATE TABLE era_symmetry_test(id int);
\d era_symmetry_test

-- 1. Create era and columns together
SELECT sql_saga.add_era('era_symmetry_test'::regclass, 'v_from', 'v_until', 'v', create_columns => true);
\d era_symmetry_test

-- 2. Drop era and columns together
SELECT sql_saga.drop_era('era_symmetry_test'::regclass, 'v', cleanup => true);
\d era_symmetry_test

-- 3. Verify add_era fails without create_columns => true
SAVEPOINT before_fail;
SELECT sql_saga.add_era('era_symmetry_test'::regclass, 'v_from', 'v_until', 'v');
ROLLBACK TO SAVEPOINT before_fail;

-- 4. Create columns manually, then add era
ALTER TABLE era_symmetry_test ADD COLUMN v_from timestamptz, ADD COLUMN v_until timestamptz;
SELECT sql_saga.add_era('era_symmetry_test'::regclass, 'v_from', 'v_until', 'v');
\d era_symmetry_test

-- 5. Drop era but leave columns (default behavior)
SELECT sql_saga.drop_era('era_symmetry_test'::regclass, 'v');
\d era_symmetry_test

DROP TABLE era_symmetry_test;

-- Note: The FK and UK were already dropped in the earlier parts of this test.
-- We only need to drop the eras and tables.
SELECT sql_saga.drop_era('child'::regclass, 'q');
DROP TABLE child;

SELECT sql_saga.drop_era('parent'::regclass, 'p');
DROP TABLE parent;

ROLLBACK;

\i sql/include/test_teardown.sql
