\i sql/include/test_setup.sql

-- Test that a table can have both a standard era and system versioning.

-- 1. Create a table
CREATE TABLE temporal_and_versioned (
    id int,
    name text,
    valid_from date,
    valid_until date
);

-- 2. Add 'valid' era and a unique key
SELECT sql_saga.add_era('temporal_and_versioned', 'valid_from', 'valid_until');
SELECT sql_saga.add_unique_key('temporal_and_versioned', '{id}');

-- 3. Add system versioning
SELECT sql_saga.add_system_versioning('temporal_and_versioned');

-- Verify that all columns, constraints, and triggers are present
\d temporal_and_versioned

-- 4. Perform some DML
INSERT INTO temporal_and_versioned (id, name, valid_from, valid_until)
VALUES (1, 'Initial Name', '2023-01-01', 'infinity');

-- Update a non-key, non-temporal column.
-- This should create a history row, but not affect the temporal aspect.
UPDATE temporal_and_versioned
SET name = 'First Update'
WHERE id = 1 AND valid_from = '2023-01-01';

-- Check the current and history tables
SELECT id, name, valid_from, valid_until FROM temporal_and_versioned;
SELECT id, name, valid_from, valid_until FROM temporal_and_versioned_history ORDER BY system_time_start;

-- 5. Drop system versioning and check that the era is still intact
SELECT sql_saga.drop_system_versioning('temporal_and_versioned', cleanup => true);
\d temporal_and_versioned

-- 6. Drop the unique key and the era to return table to original state
SELECT sql_saga.drop_unique_key('temporal_and_versioned', ARRAY['id'], 'valid', cleanup => true);
SELECT sql_saga.drop_era('temporal_and_versioned', cleanup => true);
\d temporal_and_versioned

\i sql/include/test_teardown.sql
