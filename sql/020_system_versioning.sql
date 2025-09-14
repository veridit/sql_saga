\i sql/include/test_setup.sql

CREATE TABLE system_versioning_test (
    id int PRIMARY KEY,
    name text
);

-- Add system versioning
SELECT sql_saga.add_system_versioning('system_versioning_test');

-- Check that columns and triggers were added
\d system_versioning_test

-- Check metadata tables
SELECT table_schema, table_name, era_name, valid_from_column_name, valid_until_column_name FROM sql_saga.era WHERE table_name = 'system_versioning_test';
SELECT table_schema, table_name, era_name, infinity_check_constraint, generated_always_trigger, write_history_trigger, truncate_trigger FROM sql_saga.system_time_era WHERE table_name = 'system_versioning_test';
SELECT table_schema, table_name, era_name, history_table_name, view_table_name FROM sql_saga.system_versioning WHERE table_name = 'system_versioning_test';

-- Check history table and view
\d system_versioning_test_history
\d system_versioning_test_with_history

-- Drop system versioning
SELECT sql_saga.drop_system_versioning('system_versioning_test');

-- Check that objects are gone
\d system_versioning_test
\d system_versioning_test_history
\d system_versioning_test_with_history

-- Check that metadata is gone
SELECT table_schema, table_name, era_name FROM sql_saga.era WHERE table_name = 'system_versioning_test';
SELECT table_schema, table_name, era_name FROM sql_saga.system_time_era WHERE table_name = 'system_versioning_test';
SELECT table_schema, table_name, era_name FROM sql_saga.system_versioning WHERE table_name = 'system_versioning_test';

\i sql/include/test_teardown.sql
