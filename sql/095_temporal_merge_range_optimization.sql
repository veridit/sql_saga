-- Test that temporal_merge executor uses pre-computed range columns
-- This test verifies the optimization where ranges are pre-computed in the plan

\i sql/include/test_setup.sql

BEGIN;

CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Use the standard temporal table pattern from other tests
CREATE TABLE test_entity (
    id INT,
    name TEXT,
    valid_range daterange,
    valid_from date GENERATED ALWAYS AS (lower(valid_range)) STORED,
    valid_until date GENERATED ALWAYS AS (upper(valid_range)) STORED
);

SELECT sql_saga.add_era('test_entity', 'valid_range');
SELECT sql_saga.add_unique_key('test_entity', ARRAY['id']);

-- Insert initial data
INSERT INTO test_entity (id, name, valid_range) VALUES
    (1, 'Original', '[2024-01-01,2024-12-31)');

-- Create source with update
CREATE TEMP TABLE source_data (
    row_id INT GENERATED ALWAYS AS IDENTITY,
    id INT,
    name TEXT,
    valid_from DATE,
    valid_until DATE,
    valid_range daterange GENERATED ALWAYS AS (daterange(valid_from, valid_until, '[)')) STORED
);

INSERT INTO source_data (id, name, valid_from, valid_until) VALUES
    (1, 'Updated', '2024-06-01', '2024-12-31');

-- Run temporal_merge
CALL sql_saga.temporal_merge(
    'test_entity',
    'source_data',
    ARRAY['id']
);

-- The key test: verify the plan has pre-computed ranges
-- These should be populated as TEXT representations of the ranges
SELECT 
    operation,
    old_valid_range IS NOT NULL AS has_old_range,
    new_valid_range IS NOT NULL AS has_new_range,
    -- Show a substring to verify they look like ranges
    substring(old_valid_range from 1 for 10) as old_range_start,
    substring(new_valid_range from 1 for 10) as new_range_start
FROM temporal_merge_plan
WHERE operation IN ('UPDATE', 'INSERT')
ORDER BY plan_op_seq;

-- Also show the actual ranges to verify format
SELECT 
    operation,
    old_valid_range,
    new_valid_range
FROM temporal_merge_plan
WHERE operation IN ('UPDATE', 'INSERT')
ORDER BY plan_op_seq;

-- Verify the result
SELECT id, name, valid_range FROM test_entity ORDER BY id, valid_range;

ROLLBACK;

\i sql/include/test_teardown.sql