\i sql/include/test_setup.sql

BEGIN;

/* Run tests as unprivileged user */
SET ROLE TO sql_saga_unprivileged_user;

/* DDL on unrelated tables should not be affected */
CREATE TABLE unrelated(a int);
ALTER TABLE unrelated RENAME a TO b;
DROP TABLE unrelated;

/*
 * If anything we store as "name" is renamed, we need to update our catalogs or
 * throw an error.
 */

/* era */
CREATE TABLE rename_test(col1 text, col2 bigint, col3 time, s integer, e integer);
SELECT sql_saga.add_era('rename_test', 's', 'e', 'p');
TABLE sql_saga.era;
ALTER TABLE rename_test RENAME s TO start;
ALTER TABLE rename_test RENAME e TO "end";
TABLE sql_saga.era;
ALTER TABLE rename_test RENAME start TO "s < e";
TABLE sql_saga.era;
ALTER TABLE rename_test RENAME "end" TO "embedded "" symbols";
TABLE sql_saga.era;
ALTER TABLE rename_test RENAME CONSTRAINT rename_test_p_check TO start_before_end;
TABLE sql_saga.era;

/* api */
ALTER TABLE rename_test ADD COLUMN id integer PRIMARY KEY;
SELECT sql_saga.add_for_portion_of_view('rename_test', 'p');
TABLE sql_saga.updatable_view;
ALTER TRIGGER for_portion_of_p ON rename_test__for_portion_of_p RENAME TO portion_trigger;
TABLE sql_saga.updatable_view;
SELECT sql_saga.drop_for_portion_of_view('rename_test', 'p');
ALTER TABLE rename_test DROP COLUMN id;


/* unique_keys */
SELECT sql_saga.add_unique_key('rename_test', ARRAY['col2', 'col1', 'col3'], 'p');
TABLE sql_saga.unique_keys;
ALTER TABLE rename_test RENAME COLUMN col1 TO "COLUMN1";
ALTER TABLE rename_test RENAME CONSTRAINT rename_test_col2_col1_col3_p_uniq TO unconst;
ALTER TABLE rename_test RENAME CONSTRAINT rename_test_col2_col1_col3_p_excl TO exconst;
TABLE sql_saga.unique_keys;

/* foreign_keys */
CREATE TABLE rename_test_ref (LIKE rename_test);
SELECT sql_saga.add_era('rename_test_ref', 's < e', 'embedded " symbols', 'q');
TABLE sql_saga.era;
SELECT sql_saga.add_temporal_foreign_key('rename_test_ref', ARRAY['col2', 'COLUMN1', 'col3'], 'q', 'rename_test_col2_col1_col3_p');
TABLE sql_saga.foreign_keys;
SAVEPOINT pristine;
ALTER TABLE rename_test_ref RENAME COLUMN "COLUMN1" TO col1;
TABLE sql_saga.foreign_keys; -- The column name should be updated here
ROLLBACK TO SAVEPOINT pristine;
ALTER TRIGGER "rename_test_ref_col2_COLUMN1_col3_q_fk_insert" ON rename_test_ref RENAME TO fk_insert;
ROLLBACK TO SAVEPOINT pristine;
ALTER TRIGGER "rename_test_ref_col2_COLUMN1_col3_q_fk_update" ON rename_test_ref RENAME TO fk_update;
ROLLBACK TO SAVEPOINT pristine;
ALTER TRIGGER "rename_test_ref_col2_COLUMN1_col3_q_uk_update" ON rename_test RENAME TO uk_update;
ROLLBACK TO SAVEPOINT pristine;
ALTER TRIGGER "rename_test_ref_col2_COLUMN1_col3_q_uk_delete" ON rename_test RENAME TO uk_delete;
ROLLBACK TO SAVEPOINT pristine;
TABLE sql_saga.foreign_keys;

SELECT sql_saga.drop_foreign_key('rename_test_ref', ARRAY['col2', 'COLUMN1', 'col3'], 'q');
SELECT sql_saga.drop_unique_key('rename_test', ARRAY['col2', 'COLUMN1', 'col3'], 'p');
SELECT sql_saga.drop_era('rename_test', 'p');
DROP TABLE rename_test;

SELECT sql_saga.drop_era('rename_test_ref','q');
DROP TABLE rename_test_ref;

ROLLBACK;

\i sql/include/test_teardown.sql
