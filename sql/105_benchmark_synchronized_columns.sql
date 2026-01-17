\i sql/include/test_setup.sql
\i sql/include/benchmark_setup.sql

SET ROLE TO sql_saga_unprivileged_user;

--------------------------------------------------------------------------------
-- BENCHMARK: Tables with Synchronized Columns (Trigger-based)
--
-- This benchmark uses tables with valid_from, valid_until, AND valid_range.
-- The synchronization trigger fires on every INSERT/UPDATE to keep them in sync.
-- The range column is the source of truth; boundary columns are derived.
-- This measures the overhead of:
-- - Native WITHOUT OVERLAPS constraints
-- - Native temporal foreign keys
-- - PLUS the synchronization trigger on every row operation
--------------------------------------------------------------------------------

\echo '--- Creating Tables with Synchronized Columns ---'

-- Note: valid_range is source of truth, valid_from/valid_until are derived by trigger
CREATE TABLE parent_synced (
    id INTEGER,
    valid_range daterange NOT NULL,
    valid_from DATE,
    valid_until DATE,
    name VARCHAR NOT NULL
);

CREATE TABLE child_synced (
    id INTEGER,
    valid_range daterange NOT NULL,
    valid_from DATE,
    valid_until DATE,
    parent_id INTEGER NOT NULL,
    description TEXT NOT NULL
);

-- Add eras with synchronization (this creates the sync trigger)
-- The trigger will populate valid_from/valid_until from valid_range
SELECT sql_saga.add_era('parent_synced'::regclass, 'valid_range', 'valid',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_era('child_synced'::regclass, 'valid_range', 'valid',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');

-- Add unique keys (uses WITHOUT OVERLAPS)
SELECT sql_saga.add_unique_key('parent_synced'::regclass, ARRAY['id'], 'valid');
SELECT sql_saga.add_unique_key('child_synced'::regclass, ARRAY['id'], 'valid');

-- Add temporal foreign key (uses native PG18 temporal FK)
SELECT sql_saga.add_foreign_key(
    'child_synced'::regclass,
    ARRAY['parent_id'],
    'parent_synced'::regclass,
    ARRAY['id'],
    fk_era_name => 'valid',
    create_index => true
);

-- Create performance indexes
CREATE INDEX ON parent_synced USING GIST (valid_range);
CREATE INDEX ON child_synced USING GIST (valid_range);

\echo '--- Populating Tables with Synchronized Columns ---'
-- Insert via valid_range; trigger will sync to valid_from/valid_until
INSERT INTO parent_synced (id, valid_range, name)
SELECT i, daterange('2015-01-01', 'infinity', '[)'), 'Company ' || i
FROM generate_series(1, 20000) i;

INSERT INTO child_synced (id, valid_range, parent_id, description)
SELECT i, daterange('2015-01-01', 'infinity', '[)'), i, 'Shop ' || i
FROM generate_series(1, 10000) i;

ANALYZE parent_synced;
ANALYZE child_synced;

--------------------------------------------------------------------------------
\echo '--- DML Benchmarks on Tables with Synchronized Columns ---'
--------------------------------------------------------------------------------

-- Parent INSERT
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced Parent INSERT start', 0, false);
CALL sql_saga.benchmark_reset();
INSERT INTO parent_synced (id, valid_range, name)
SELECT i, daterange('2015-01-01', 'infinity', '[)'), 'New Company ' || i
FROM generate_series(20001, 21000) i;
CALL sql_saga.benchmark_log_and_reset('Synced Parent INSERT');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced Parent INSERT end', 1000, true);

-- Parent UPDATE (temporal key change - modify range)
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced Parent UPDATE Key start', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE parent_synced SET valid_range = daterange('2014-01-01', upper(valid_range), '[)')
WHERE id BETWEEN 1 AND 1000;
CALL sql_saga.benchmark_log_and_reset('Synced Parent UPDATE Key');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced Parent UPDATE Key end', 1000, true);

-- Parent UPDATE (non-key column)
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced Parent UPDATE Non-Key start', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE parent_synced SET name = 'Updated Company'
WHERE id BETWEEN 1001 AND 2000;
CALL sql_saga.benchmark_log_and_reset('Synced Parent UPDATE Non-Key');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced Parent UPDATE Non-Key end', 1000, true);

-- Parent DELETE (no children)
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced Parent DELETE (no children) start', 0, false);
CALL sql_saga.benchmark_reset();
DELETE FROM parent_synced WHERE id BETWEEN 20001 AND 21000;
CALL sql_saga.benchmark_log_and_reset('Synced Parent DELETE (no children)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced Parent DELETE (no children) end', 1000, true);

-- Child INSERT
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced Child INSERT start', 0, false);
CALL sql_saga.benchmark_reset();
INSERT INTO child_synced (id, valid_range, parent_id, description)
SELECT i, daterange('2015-01-01', 'infinity', '[)'), i, 'New Shop ' || i
FROM generate_series(10001, 11000) i;
CALL sql_saga.benchmark_log_and_reset('Synced Child INSERT');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced Child INSERT end', 1000, true);

-- Child UPDATE (temporal key change)
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced Child UPDATE Key start', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE child_synced SET valid_range = daterange('2014-01-01', upper(valid_range), '[)')
WHERE id BETWEEN 1 AND 1000;
CALL sql_saga.benchmark_log_and_reset('Synced Child UPDATE Key');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced Child UPDATE Key end', 1000, true);

-- Child UPDATE (non-key column)
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced Child UPDATE Non-Key start', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE child_synced SET description = 'Updated Shop'
WHERE id BETWEEN 1001 AND 2000;
CALL sql_saga.benchmark_log_and_reset('Synced Child UPDATE Non-Key');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced Child UPDATE Non-Key end', 1000, true);

-- Child DELETE
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced Child DELETE start', 0, false);
CALL sql_saga.benchmark_reset();
DELETE FROM child_synced WHERE id BETWEEN 9001 AND 10000;
CALL sql_saga.benchmark_log_and_reset('Synced Child DELETE');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced Child DELETE end', 1000, true);

--------------------------------------------------------------------------------
\echo '--- temporal_merge Benchmarks on Tables with Synchronized Columns ---'
--------------------------------------------------------------------------------

-- Reset data for temporal_merge tests
DELETE FROM child_synced WHERE id > 10000;
DELETE FROM parent_synced WHERE id > 20000;
ANALYZE parent_synced;
ANALYZE child_synced;

-- Parent temporal_merge
CREATE TEMP TABLE parent_source_synced (
    row_id INT,
    id INT,
    valid_range daterange,
    name VARCHAR
);
INSERT INTO parent_source_synced
SELECT i, i, daterange('2015-01-01', 'infinity', '[)'), 'Merged Company ' || i
FROM generate_series(4001, 5000) AS i;
CREATE INDEX ON parent_source_synced(id);
CREATE INDEX ON parent_source_synced USING GIST (valid_range);
ANALYZE parent_source_synced;

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced temporal_merge Parent start', 0, false);
CALL sql_saga.benchmark_reset();
CALL sql_saga.temporal_merge('parent_synced'::regclass, 'parent_source_synced'::regclass, ARRAY['id']);
CALL sql_saga.benchmark_log_and_reset('Synced temporal_merge Parent');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced temporal_merge Parent end', 1000, true);

-- Child temporal_merge
CREATE TEMP TABLE child_source_synced (
    row_id INT,
    id INT,
    valid_range daterange,
    parent_id INT,
    description TEXT
);
INSERT INTO child_source_synced
SELECT i, i, daterange('2015-01-01', 'infinity', '[)'), i, 'Merged Shop ' || i
FROM generate_series(4001, 5000) AS i;
CREATE INDEX ON child_source_synced(id);
CREATE INDEX ON child_source_synced USING GIST (valid_range);
ANALYZE child_source_synced;

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced temporal_merge Child start', 0, false);
CALL sql_saga.benchmark_reset();
CALL sql_saga.temporal_merge('child_synced'::regclass, 'child_source_synced'::regclass, ARRAY['id']);
CALL sql_saga.benchmark_log_and_reset('Synced temporal_merge Child');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced temporal_merge Child end', 1000, true);

--------------------------------------------------------------------------------
\echo '--- Teardown ---'
--------------------------------------------------------------------------------

SELECT sql_saga.drop_foreign_key('child_synced'::regclass, ARRAY['parent_id'], 'valid');
SELECT sql_saga.drop_unique_key('child_synced'::regclass, ARRAY['id'], 'valid');
SELECT sql_saga.drop_unique_key('parent_synced'::regclass, ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('child_synced'::regclass, cleanup => true);
SELECT sql_saga.drop_era('parent_synced'::regclass, cleanup => true);

DROP TABLE child_synced;
DROP TABLE parent_synced;

\echo '-- Performance log from pg_stat_monitor --'
\set monitor_log_filename expected/performance/105_benchmark_synchronized_columns_monitor.csv
\i sql/include/benchmark_monitor_csv.sql

-- Verify the benchmark events and row counts
SELECT event, row_count FROM benchmark ORDER BY seq_id;

-- Capture performance metrics to a separate file
\set benchmark_log_filename expected/performance/105_benchmark_synchronized_columns_report.log
\i sql/include/benchmark_report_log.sql

\i sql/include/benchmark_teardown.sql
\i sql/include/test_teardown.sql
