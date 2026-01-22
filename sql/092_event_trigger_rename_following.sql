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
CREATE TABLE rename_test(col1 text, col2 bigint, col3 time, valid_range int4range NOT NULL, s integer, e integer);
SELECT sql_saga.add_era('rename_test', 'valid_range', 'p', valid_from_column_name => 's', valid_until_column_name => 'e');
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
TABLE sql_saga.unique_keys;

/* foreign_keys */
CREATE TABLE rename_test_ref (LIKE rename_test);
SELECT sql_saga.add_era('rename_test_ref', 'valid_range', 'q', valid_from_column_name => 's < e', valid_until_column_name => 'embedded " symbols');
TABLE sql_saga.era;
SELECT sql_saga.add_temporal_foreign_key('rename_test_ref', ARRAY['col2', 'COLUMN1', 'col3'], 'q', 'rename_test_col2_col1_col3_p');
TABLE sql_saga.foreign_keys;
SAVEPOINT pristine;
ALTER TABLE rename_test_ref RENAME COLUMN "COLUMN1" TO col1;
TABLE sql_saga.foreign_keys; -- The column name should be updated here
ROLLBACK TO SAVEPOINT pristine;

/* Test protection of synchronize_temporal_columns_trigger */
ALTER TRIGGER rename_test_p_sync_temporal_trg ON rename_test RENAME TO my_trigger;
ROLLBACK TO SAVEPOINT pristine;
ALTER TRIGGER rename_test_ref_q_sync_temporal_trg ON rename_test_ref RENAME TO another_trigger;
ROLLBACK TO SAVEPOINT pristine;

TABLE sql_saga.foreign_keys;

SELECT sql_saga.drop_foreign_key('rename_test_ref', ARRAY['col2', 'COLUMN1', 'col3'], 'q');
SELECT sql_saga.drop_unique_key('rename_test', ARRAY['col2', 'COLUMN1', 'col3'], 'p');
SELECT sql_saga.drop_era('rename_test', 'p');
DROP TABLE rename_test;

SELECT sql_saga.drop_era('rename_test_ref','q');
DROP TABLE rename_test_ref;

ROLLBACK;

\echo '\n--- System Versioning Rename Following Tests ---'

CREATE TABLE sv_rename_test (
    id serial PRIMARY KEY,
    data text
);

SELECT sql_saga.add_system_versioning('sv_rename_test');

-- Get current constraint/trigger names from metadata
\echo '--- Initial metadata ---'
SELECT infinity_check_constraint, generated_always_trigger, write_history_trigger, truncate_trigger
FROM sql_saga.system_time_era
WHERE table_name = 'sv_rename_test';

-- Rename the infinity check constraint
ALTER TABLE sv_rename_test RENAME CONSTRAINT sv_rename_test_system_valid_range_infinity_check TO sv_rename_test_inf_check_renamed;

\echo '--- After renaming infinity_check_constraint ---'
SELECT infinity_check_constraint
FROM sql_saga.system_time_era
WHERE table_name = 'sv_rename_test';

-- Rename the generated_always trigger
ALTER TRIGGER sv_rename_test_system_time_generated_always ON sv_rename_test RENAME TO sv_rename_test_gen_always_renamed;

\echo '--- After renaming generated_always_trigger ---'
SELECT generated_always_trigger
FROM sql_saga.system_time_era
WHERE table_name = 'sv_rename_test';

-- Rename the write_history trigger
ALTER TRIGGER sv_rename_test_system_time_write_history ON sv_rename_test RENAME TO sv_rename_test_write_hist_renamed;

\echo '--- After renaming write_history_trigger ---'
SELECT write_history_trigger
FROM sql_saga.system_time_era
WHERE table_name = 'sv_rename_test';

-- Rename the truncate trigger
ALTER TRIGGER sv_rename_test_truncate ON sv_rename_test RENAME TO sv_rename_test_trunc_renamed;

\echo '--- After renaming truncate_trigger ---'
SELECT truncate_trigger
FROM sql_saga.system_time_era
WHERE table_name = 'sv_rename_test';

-- Verify all renames tracked
\echo '--- Final metadata (all renamed) ---'
SELECT infinity_check_constraint, generated_always_trigger, write_history_trigger, truncate_trigger
FROM sql_saga.system_time_era
WHERE table_name = 'sv_rename_test';

-- Cleanup
SELECT sql_saga.drop_system_versioning('sv_rename_test');
DROP TABLE sv_rename_test;

\i sql/include/test_teardown.sql
