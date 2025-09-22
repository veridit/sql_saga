
\i sql/include/test_setup.sql

CREATE SCHEMA fk_idx;
SET search_path TO fk_idx, sql_saga, public;

-----------------------------------------
-- Temporal to Temporal Foreign Keys
-----------------------------------------
\echo '--- Temporal to Temporal FKs ---'

\echo 'Scenario 1: Auto-create index (default behavior)'
CREATE TABLE parent_t (id int, valid_from date, valid_until date);
SELECT add_era('parent_t'::regclass);
SELECT add_unique_key('parent_t'::regclass, ARRAY['id'], unique_key_name => 'parent_t_id_valid');
CREATE TABLE child_t (id int, parent_id int, valid_from date, valid_until date);
SELECT add_era('child_t'::regclass);

SELECT add_foreign_key(
    fk_table_oid => 'child_t'::regclass, fk_column_names => ARRAY['parent_id'],
    pk_table_oid => 'parent_t'::regclass, pk_column_names => ARRAY['id'],
    foreign_key_name => 'child_parent_fk_1'
);

SELECT foreign_key_name, fk_index_name FROM foreign_keys WHERE foreign_key_name = 'child_parent_fk_1';
\d child_t

\echo '-> Dropping FK should also drop the auto-created index'
SELECT drop_foreign_key_by_name('child_t'::regclass, 'child_parent_fk_1');
\d child_t
DROP TABLE parent_t, child_t CASCADE;

\echo 'Scenario 2: Disable auto-creation of index'
CREATE TABLE parent_t (id int, valid_from date, valid_until date);
SELECT add_era('parent_t'::regclass);
SELECT add_unique_key('parent_t'::regclass, ARRAY['id'], unique_key_name => 'parent_t_id_valid');
CREATE TABLE child_t (id int, parent_id int, valid_from date, valid_until date);
SELECT add_era('child_t'::regclass);

SELECT add_foreign_key(
    fk_table_oid => 'child_t'::regclass, fk_column_names => ARRAY['parent_id'],
    pk_table_oid => 'parent_t'::regclass, pk_column_names => ARRAY['id'],
    foreign_key_name => 'child_parent_fk_2', create_index => false
);

SELECT foreign_key_name, fk_index_name FROM foreign_keys WHERE foreign_key_name = 'child_parent_fk_2';
\d child_t
SELECT drop_foreign_key_by_name('child_t'::regclass, 'child_parent_fk_2');
DROP TABLE parent_t, child_t CASCADE;

\echo 'Scenario 3: Manually created index exists'
CREATE TABLE parent_t (id int, valid_from date, valid_until date);
SELECT add_era('parent_t'::regclass);
SELECT add_unique_key('parent_t'::regclass, ARRAY['id'], unique_key_name => 'parent_t_id_valid');
CREATE TABLE child_t (id int, parent_id int, valid_from date, valid_until date);
SELECT add_era('child_t'::regclass);
CREATE INDEX manual_child_t_idx ON child_t USING GIST (parent_id, daterange(valid_from, valid_until));

SELECT add_foreign_key(
    fk_table_oid => 'child_t'::regclass, fk_column_names => ARRAY['parent_id'],
    pk_table_oid => 'parent_t'::regclass, pk_column_names => ARRAY['id'],
    foreign_key_name => 'child_parent_fk_3'
);
SELECT foreign_key_name, fk_index_name FROM foreign_keys WHERE foreign_key_name = 'child_parent_fk_3';

\echo '-> Dropping FK should NOT drop the manually created index'
SELECT drop_foreign_key_by_name('child_t'::regclass, 'child_parent_fk_3');
\d child_t
DROP TABLE parent_t, child_t CASCADE;

\echo 'Scenario 4: Auto-created index is not dropped when requested'
CREATE TABLE parent_t (id int, valid_from date, valid_until date);
SELECT add_era('parent_t'::regclass);
SELECT add_unique_key('parent_t'::regclass, ARRAY['id'], unique_key_name => 'parent_t_id_valid');
CREATE TABLE child_t (id int, parent_id int, valid_from date, valid_until date);
SELECT add_era('child_t'::regclass);
SELECT add_foreign_key(
    fk_table_oid => 'child_t'::regclass, fk_column_names => ARRAY['parent_id'],
    pk_table_oid => 'parent_t'::regclass, pk_column_names => ARRAY['id'],
    foreign_key_name => 'child_parent_fk_4'
);
\d child_t

\echo '-> Dropping with drop_index=false should not drop the index'
SELECT drop_foreign_key_by_name('child_t'::regclass, 'child_parent_fk_4', drop_index := false);
\d child_t
DROP TABLE parent_t, child_t CASCADE;


-----------------------------------------
-- Regular to Temporal Foreign Keys
-----------------------------------------
\echo '--- Regular to Temporal FKs ---'

\echo 'Scenario 1: Auto-create index (default behavior)'
CREATE TABLE parent_r (id int, valid_from date, valid_until date);
SELECT add_era('parent_r'::regclass);
SELECT add_unique_key('parent_r'::regclass, ARRAY['id'], unique_key_name => 'parent_r_id_valid');
CREATE TABLE child_r (id int, parent_id int);

SELECT add_foreign_key(
    fk_table_oid => 'child_r'::regclass, fk_column_names => ARRAY['parent_id'],
    pk_table_oid => 'parent_r'::regclass, pk_column_names => ARRAY['id'],
    foreign_key_name => 'child_parent_r_fk_1'
);

SELECT foreign_key_name, fk_index_name FROM foreign_keys WHERE foreign_key_name = 'child_parent_r_fk_1';
\d child_r

\echo '-> Dropping FK should also drop the auto-created index'
SELECT drop_foreign_key_by_name('child_r'::regclass, 'child_parent_r_fk_1');
\d child_r
DROP TABLE parent_r, child_r CASCADE;

\echo 'Scenario 2: Disable auto-creation of index'
CREATE TABLE parent_r (id int, valid_from date, valid_until date);
SELECT add_era('parent_r'::regclass);
SELECT add_unique_key('parent_r'::regclass, ARRAY['id'], unique_key_name => 'parent_r_id_valid');
CREATE TABLE child_r (id int, parent_id int);

SELECT add_foreign_key(
    fk_table_oid => 'child_r'::regclass, fk_column_names => ARRAY['parent_id'],
    pk_table_oid => 'parent_r'::regclass, pk_column_names => ARRAY['id'],
    foreign_key_name => 'child_parent_r_fk_2', create_index => false
);

SELECT foreign_key_name, fk_index_name FROM foreign_keys WHERE foreign_key_name = 'child_parent_r_fk_2';
\d child_r
SELECT drop_foreign_key_by_name('child_r'::regclass, 'child_parent_r_fk_2');
DROP TABLE parent_r, child_r CASCADE;

\echo 'Scenario 3: Manually created index exists'
CREATE TABLE parent_r (id int, valid_from date, valid_until date);
SELECT add_era('parent_r'::regclass);
SELECT add_unique_key('parent_r'::regclass, ARRAY['id'], unique_key_name => 'parent_r_id_valid');
CREATE TABLE child_r (id int, parent_id int);
CREATE INDEX manual_child_r_idx ON child_r (parent_id);

SELECT add_foreign_key(
    fk_table_oid => 'child_r'::regclass, fk_column_names => ARRAY['parent_id'],
    pk_table_oid => 'parent_r'::regclass, pk_column_names => ARRAY['id'],
    foreign_key_name => 'child_parent_r_fk_3'
);
SELECT foreign_key_name, fk_index_name FROM foreign_keys WHERE foreign_key_name = 'child_parent_r_fk_3';

\echo '-> Dropping FK should NOT drop the manually created index'
SELECT drop_foreign_key_by_name('child_r'::regclass, 'child_parent_r_fk_3');
\d child_r
DROP TABLE parent_r, child_r CASCADE;

\i sql/include/test_teardown.sql
