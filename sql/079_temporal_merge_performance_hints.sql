\i sql/include/test_setup.sql

BEGIN;

CREATE TABLE target_table (
    id INT,
    value TEXT,
    valid_range daterange NOT NULL,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL,
    PRIMARY KEY (id, valid_range WITHOUT OVERLAPS)
);
SELECT sql_saga.add_era('target_table', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('target_table', ARRAY['id']);

-- Also add an updatable view to ensure it doesn't interfere.
SELECT sql_saga.add_current_view('target_table');

-- Scenario 1: No GIST index on source table.
CREATE TEMP TABLE source_table_no_index (
    row_id SERIAL,
    id INT,
    value TEXT,
    valid_range daterange NOT NULL,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL
);

\echo
\echo '# Scenario 1a: Small table (< 512 rows) without index. Expect NO warning.'
\echo
INSERT INTO source_table_no_index (id, value, valid_range, valid_from, valid_until) 
VALUES (1, 'A', daterange('2023-01-01', NULL, '[)'), '2023-01-01', 'infinity');
ANALYZE source_table_no_index;
-- No warning should be emitted here.
CALL sql_saga.temporal_merge(
    'target_table',
    'source_table_no_index',
    primary_identity_columns => ARRAY['id']
);

\echo
\echo '# Scenario 1b: Large table (>= 512 rows) without index. Expect a WARNING and HINT.'
\echo
-- Insert enough rows to cross the threshold.
INSERT INTO source_table_no_index (id, value, valid_range, valid_from, valid_until)
SELECT g, 'A', daterange('2023-01-01', NULL, '[)'), '2023-01-01', 'infinity' FROM generate_series(2, 600) g;
ANALYZE source_table_no_index;
-- The warning should be emitted here.
CALL sql_saga.temporal_merge(
    'target_table',
    'source_table_no_index',
    primary_identity_columns => ARRAY['id']
);

-- Scenario 2: GIST index exists on source table, should be silent.
CREATE TEMP TABLE source_table_with_index (
    row_id SERIAL,
    id INT,
    value TEXT,
    valid_range daterange NOT NULL,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL
);

-- The recommended index
CREATE INDEX ON source_table_with_index USING GIST (valid_range);

-- Insert enough rows to cross the threshold; warning should still be suppressed by the index.
INSERT INTO source_table_with_index (id, value, valid_range, valid_from, valid_until)
SELECT g, 'B', daterange('2024-01-01', NULL, '[)'), '2024-01-01', 'infinity' FROM generate_series(1, 600) g;
ANALYZE source_table_with_index;

\echo
\echo '# Scenario 2: Large table with index. Expect no warning.'
\echo
-- This call should NOT produce a warning.
CALL sql_saga.temporal_merge(
    'target_table',
    'source_table_with_index',
    primary_identity_columns => ARRAY['id']
);

-- Scenario 3: Source is a VIEW. Check if index on underlying table is detected.
\echo
\echo '# Scenario 3: Source is a VIEW. Check if index on underlying table is detected.'
\echo
-- Base table for the view
CREATE TEMP TABLE source_table_for_view (
    row_id SERIAL,
    id INT,
    value TEXT,
    valid_range daterange NOT NULL,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL
);

-- The view itself
CREATE TEMP VIEW source_view AS
SELECT * FROM source_table_for_view;

\echo
\echo '# Scenario 3a: View over small table with NO index. Expect NO warning.'
\echo
INSERT INTO source_table_for_view (id, value, valid_range, valid_from, valid_until) 
VALUES (2, 'C', daterange('2025-01-01', NULL, '[)'), '2025-01-01', 'infinity');
ANALYZE source_table_for_view;

CALL sql_saga.temporal_merge(
    'target_table',
    'source_view'::regclass,
    primary_identity_columns => ARRAY['id']
);

\echo
\echo '# Scenario 3b: View over large table with NO index. Expect a WARNING.'
\echo
-- Insert enough rows to cross the threshold.
INSERT INTO source_table_for_view (id, value, valid_range, valid_from, valid_until)
SELECT g, 'C', daterange('2025-01-01', NULL, '[)'), '2025-01-01', 'infinity' FROM generate_series(1000, 1600) g;
ANALYZE source_table_for_view;

CALL sql_saga.temporal_merge(
    'target_table',
    'source_view'::regclass,
    primary_identity_columns => ARRAY['id']
);

-- Add the recommended index to the *base table*
CREATE INDEX ON source_table_for_view USING GIST (valid_range);

-- Manually truncate the internal cache to ensure the new index is detected in the next call.
-- This is necessary because the cache is session-local and this entire test runs in a single transaction.
CALL sql_saga.temporal_merge_drop_cache();

\echo
\echo '# Scenario 3c: View over large table WITH index. Expect NO warning.'
\echo
-- The table is already large and now has an index.
INSERT INTO source_table_for_view (id, value, valid_range, valid_from, valid_until) 
VALUES (2, 'D', daterange('2026-01-01', NULL, '[)'), '2026-01-01', 'infinity');
ANALYZE source_table_for_view;

CALL sql_saga.temporal_merge(
    'target_table',
    'source_view'::regclass,
    primary_identity_columns => ARRAY['id']
);

-- Verify final state of target table to ensure merge worked
SELECT COUNT(*) FROM target_table;

\echo
\echo '# Verify final state of the current view'
\echo
SELECT count(*) FROM target_table__current_valid;

ROLLBACK;

\i sql/include/test_teardown.sql
