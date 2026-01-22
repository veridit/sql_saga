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
SELECT table_schema, table_name, era_name, range_column_name, valid_from_column_name, valid_until_column_name FROM sql_saga.era WHERE table_name = 'system_versioning_test';
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

\echo '\n--- Test TRUNCATE propagation to history ---'

CREATE TABLE sv_truncate_test (
    id serial PRIMARY KEY,
    data text
);

SELECT sql_saga.add_system_versioning('sv_truncate_test');

-- Insert some data
INSERT INTO sv_truncate_test (data) VALUES ('row1'), ('row2'), ('row3');

-- Update to create history
UPDATE sv_truncate_test SET data = data || '_updated';

\echo '--- Before TRUNCATE: base table ---'
SELECT COUNT(*) AS base_count FROM sv_truncate_test;

\echo '--- Before TRUNCATE: history table ---'
SELECT COUNT(*) AS history_count FROM sv_truncate_test_history;

-- TRUNCATE should propagate to history
TRUNCATE sv_truncate_test;

\echo '--- After TRUNCATE: base table ---'
SELECT COUNT(*) AS base_count FROM sv_truncate_test;

\echo '--- After TRUNCATE: history table (should also be empty) ---'
SELECT COUNT(*) AS history_count FROM sv_truncate_test_history;

-- Cleanup
SELECT sql_saga.drop_system_versioning('sv_truncate_test');
DROP TABLE sv_truncate_test;

\echo '\n--- Test cross-schema system versioning ---'

-- Create a separate schema
CREATE SCHEMA sv_test_schema;

CREATE TABLE sv_test_schema.versioned_table (
    id serial PRIMARY KEY,
    name text
);

-- Add system versioning to table in non-public schema
SELECT sql_saga.add_system_versioning('sv_test_schema.versioned_table');

-- Verify metadata shows correct schema
\echo '--- Metadata for cross-schema table ---'
SELECT table_schema, table_name, history_schema_name, history_table_name 
FROM sql_saga.system_versioning 
WHERE table_name = 'versioned_table';

-- Verify history table created in same schema
\echo '--- History table exists in sv_test_schema ---'
SELECT schemaname, tablename 
FROM pg_tables 
WHERE tablename = 'versioned_table_history' AND schemaname = 'sv_test_schema';

-- Insert and update to create history
INSERT INTO sv_test_schema.versioned_table (name) VALUES ('test');
UPDATE sv_test_schema.versioned_table SET name = 'test_updated';

\echo '--- History created in cross-schema table ---'
SELECT COUNT(*) AS history_count FROM sv_test_schema.versioned_table_history;

-- Cleanup
SELECT sql_saga.drop_system_versioning('sv_test_schema.versioned_table');
DROP TABLE sv_test_schema.versioned_table;
DROP SCHEMA sv_test_schema;

\echo '\n--- Error case tests ---'

-- Test: Cannot add system versioning to a partitioned table
CREATE TABLE sv_partitioned (
    id int,
    created_at date
) PARTITION BY RANGE (created_at);

\echo '--- Attempting system versioning on partitioned table (should fail) ---'
DO $$
BEGIN
    PERFORM sql_saga.add_system_versioning('sv_partitioned');
    RAISE EXCEPTION 'Should have failed for partitioned table';
EXCEPTION WHEN others THEN
    RAISE NOTICE 'Correctly rejected: %', SQLERRM;
END;
$$;

DROP TABLE sv_partitioned;

-- Test: Cannot add system versioning to a view
CREATE TABLE sv_base_for_view (id int PRIMARY KEY, data text);
CREATE VIEW sv_view AS SELECT * FROM sv_base_for_view;

\echo '--- Attempting system versioning on view (should fail) ---'
DO $$
BEGIN
    PERFORM sql_saga.add_system_versioning('sv_view');
    RAISE EXCEPTION 'Should have failed for view';
EXCEPTION WHEN others THEN
    RAISE NOTICE 'Correctly rejected: %', SQLERRM;
END;
$$;

DROP VIEW sv_view;
DROP TABLE sv_base_for_view;

-- Test: Cannot drop system_time era directly (must use drop_system_versioning)
CREATE TABLE sv_drop_era_test (id int PRIMARY KEY, data text);
SELECT sql_saga.add_system_versioning('sv_drop_era_test');

\echo '--- Attempting to drop system_time era directly (should fail) ---'
DO $$
BEGIN
    PERFORM sql_saga.drop_era('sv_drop_era_test', 'system_time');
    RAISE EXCEPTION 'Should have failed - must use drop_system_versioning()';
EXCEPTION WHEN others THEN
    RAISE NOTICE 'Correctly rejected: %', SQLERRM;
END;
$$;

-- Proper cleanup
SELECT sql_saga.drop_system_versioning('sv_drop_era_test');
DROP TABLE sv_drop_era_test;

\i sql/include/test_teardown.sql
