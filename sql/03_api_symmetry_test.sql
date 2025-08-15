-- Test that the overloaded drop_* functions work as expected.

CREATE TABLE parent(id int, s int, e int);
SELECT sql_saga.add_era('parent', 's', 'e', 'p');
SELECT sql_saga.add_unique_key('parent', ARRAY['id'], 'p');

CREATE TABLE child(id int, parent_id int, s int, e int);
SELECT sql_saga.add_era('child', 's', 'e', 'q');
SELECT sql_saga.add_foreign_key('child', ARRAY['parent_id'], 'q', 'parent_id_p');

-- Test overloaded drop_foreign_key
TABLE sql_saga.foreign_keys;
SELECT sql_saga.drop_foreign_key('child', ARRAY['parent_id'], 'q');
TABLE sql_saga.foreign_keys;

-- Test original drop_foreign_key
SELECT sql_saga.add_foreign_key('child', ARRAY['parent_id'], 'q', 'parent_id_p');
TABLE sql_saga.foreign_keys;
SELECT sql_saga.drop_foreign_key('child', 'child_parent_id_q');
TABLE sql_saga.foreign_keys;

-- Test overloaded drop_unique_key
TABLE sql_saga.unique_keys;
SELECT sql_saga.drop_unique_key('parent', ARRAY['id'], 'p');
TABLE sql_saga.unique_keys;

-- Test original drop_unique_key
SELECT sql_saga.add_unique_key('parent', ARRAY['id'], 'p');
TABLE sql_saga.unique_keys;
SELECT sql_saga.drop_unique_key('parent', 'parent_id_p');
TABLE sql_saga.unique_keys;

DROP TABLE child;
DROP TABLE parent;
