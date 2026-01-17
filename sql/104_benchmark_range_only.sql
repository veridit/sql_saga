\i sql/include/test_setup.sql
\i sql/include/benchmark_setup.sql

SET ROLE TO sql_saga_unprivileged_user;

--------------------------------------------------------------------------------
-- BENCHMARK: Range-Only Tables (No Column Synchronization)
--
-- This benchmark uses tables with ONLY a valid_range column.
-- There is NO synchronization trigger, so we can measure the raw overhead of:
-- - Native WITHOUT OVERLAPS constraints
-- - Native temporal foreign keys
--------------------------------------------------------------------------------

\echo '--- Creating Range-Only Tables ---'

CREATE TABLE parent_range_only (
    id INTEGER,
    valid_range daterange NOT NULL,
    name VARCHAR NOT NULL
);

CREATE TABLE child_range_only (
    id INTEGER,
    valid_range daterange NOT NULL,
    parent_id INTEGER NOT NULL,
    description TEXT NOT NULL
);

-- Add eras (range-only, no synchronization)
SELECT sql_saga.add_era('parent_range_only'::regclass, 'valid_range', 'valid');
SELECT sql_saga.add_era('child_range_only'::regclass, 'valid_range', 'valid');

-- Add unique keys (uses WITHOUT OVERLAPS)
SELECT sql_saga.add_unique_key('parent_range_only'::regclass, ARRAY['id'], 'valid');
SELECT sql_saga.add_unique_key('child_range_only'::regclass, ARRAY['id'], 'valid');

-- Add temporal foreign key (uses native PG18 temporal FK)
SELECT sql_saga.add_foreign_key(
    'child_range_only'::regclass,
    ARRAY['parent_id'],
    'parent_range_only'::regclass,
    ARRAY['id'],
    fk_era_name => 'valid',
    create_index => true
);

-- Create performance indexes
CREATE INDEX ON parent_range_only USING GIST (valid_range);
CREATE INDEX ON child_range_only USING GIST (valid_range);

\echo '--- Populating Range-Only Tables ---'
INSERT INTO parent_range_only (id, valid_range, name)
SELECT i, daterange('2015-01-01', 'infinity', '[)'), 'Company ' || i
FROM generate_series(1, 20000) i;

INSERT INTO child_range_only (id, valid_range, parent_id, description)
SELECT i, daterange('2015-01-01', 'infinity', '[)'), i, 'Shop ' || i
FROM generate_series(1, 10000) i;

ANALYZE parent_range_only;
ANALYZE child_range_only;

--------------------------------------------------------------------------------
\echo '--- DML Benchmarks on Range-Only Tables ---'
--------------------------------------------------------------------------------

-- Parent INSERT
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Range-Only Parent INSERT start', 0, false);
CALL sql_saga.benchmark_reset();
INSERT INTO parent_range_only (id, valid_range, name)
SELECT i, daterange('2015-01-01', 'infinity', '[)'), 'New Company ' || i
FROM generate_series(20001, 21000) i;
CALL sql_saga.benchmark_log_and_reset('Range-Only Parent INSERT');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Range-Only Parent INSERT end', 1000, true);

-- Parent UPDATE (temporal key change - shrinks range)
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Range-Only Parent UPDATE Key start', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE parent_range_only SET valid_range = daterange('2014-01-01', upper(valid_range), '[)')
WHERE id BETWEEN 1 AND 1000;
CALL sql_saga.benchmark_log_and_reset('Range-Only Parent UPDATE Key');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Range-Only Parent UPDATE Key end', 1000, true);

-- Parent UPDATE (non-key column)
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Range-Only Parent UPDATE Non-Key start', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE parent_range_only SET name = 'Updated Company'
WHERE id BETWEEN 1001 AND 2000;
CALL sql_saga.benchmark_log_and_reset('Range-Only Parent UPDATE Non-Key');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Range-Only Parent UPDATE Non-Key end', 1000, true);

-- Parent DELETE (with children - should fail or cascade)
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Range-Only Parent DELETE (no children) start', 0, false);
CALL sql_saga.benchmark_reset();
DELETE FROM parent_range_only WHERE id BETWEEN 20001 AND 21000;
CALL sql_saga.benchmark_log_and_reset('Range-Only Parent DELETE (no children)');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Range-Only Parent DELETE (no children) end', 1000, true);

-- Child INSERT
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Range-Only Child INSERT start', 0, false);
CALL sql_saga.benchmark_reset();
INSERT INTO child_range_only (id, valid_range, parent_id, description)
SELECT i, daterange('2015-01-01', 'infinity', '[)'), i, 'New Shop ' || i
FROM generate_series(10001, 11000) i;
CALL sql_saga.benchmark_log_and_reset('Range-Only Child INSERT');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Range-Only Child INSERT end', 1000, true);

-- Child UPDATE (temporal key change)
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Range-Only Child UPDATE Key start', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE child_range_only SET valid_range = daterange('2014-01-01', upper(valid_range), '[)')
WHERE id BETWEEN 1 AND 1000;
CALL sql_saga.benchmark_log_and_reset('Range-Only Child UPDATE Key');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Range-Only Child UPDATE Key end', 1000, true);

-- Child UPDATE (non-key column)
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Range-Only Child UPDATE Non-Key start', 0, false);
CALL sql_saga.benchmark_reset();
UPDATE child_range_only SET description = 'Updated Shop'
WHERE id BETWEEN 1001 AND 2000;
CALL sql_saga.benchmark_log_and_reset('Range-Only Child UPDATE Non-Key');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Range-Only Child UPDATE Non-Key end', 1000, true);

-- Child DELETE
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Range-Only Child DELETE start', 0, false);
CALL sql_saga.benchmark_reset();
DELETE FROM child_range_only WHERE id BETWEEN 9001 AND 10000;
CALL sql_saga.benchmark_log_and_reset('Range-Only Child DELETE');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Range-Only Child DELETE end', 1000, true);

--------------------------------------------------------------------------------
\echo '--- temporal_merge Benchmarks on Range-Only Tables ---'
--------------------------------------------------------------------------------

-- Reset data for temporal_merge tests
DELETE FROM child_range_only WHERE id > 10000;
DELETE FROM parent_range_only WHERE id > 20000;
ANALYZE parent_range_only;
ANALYZE child_range_only;

-- Parent temporal_merge
CREATE TEMP TABLE parent_source_range_only (
    row_id INT,
    id INT,
    valid_range daterange,
    name VARCHAR
);
INSERT INTO parent_source_range_only
SELECT i, i, daterange('2015-01-01', 'infinity', '[)'), 'Merged Company ' || i
FROM generate_series(4001, 5000) AS i;
CREATE INDEX ON parent_source_range_only(id);
CREATE INDEX ON parent_source_range_only USING GIST (valid_range);
ANALYZE parent_source_range_only;

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Range-Only temporal_merge Parent start', 0, false);
CALL sql_saga.benchmark_reset();
CALL sql_saga.temporal_merge('parent_range_only'::regclass, 'parent_source_range_only'::regclass, ARRAY['id']);
CALL sql_saga.benchmark_log_and_reset('Range-Only temporal_merge Parent');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Range-Only temporal_merge Parent end', 1000, true);

-- Child temporal_merge
CREATE TEMP TABLE child_source_range_only (
    row_id INT,
    id INT,
    valid_range daterange,
    parent_id INT,
    description TEXT
);
INSERT INTO child_source_range_only
SELECT i, i, daterange('2015-01-01', 'infinity', '[)'), i, 'Merged Shop ' || i
FROM generate_series(4001, 5000) AS i;
CREATE INDEX ON child_source_range_only(id);
CREATE INDEX ON child_source_range_only USING GIST (valid_range);
ANALYZE child_source_range_only;

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Range-Only temporal_merge Child start', 0, false);
CALL sql_saga.benchmark_reset();
CALL sql_saga.temporal_merge('child_range_only'::regclass, 'child_source_range_only'::regclass, ARRAY['id']);
CALL sql_saga.benchmark_log_and_reset('Range-Only temporal_merge Child');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Range-Only temporal_merge Child end', 1000, true);

--------------------------------------------------------------------------------
\echo '--- Teardown ---'
--------------------------------------------------------------------------------

SELECT sql_saga.drop_foreign_key('child_range_only'::regclass, ARRAY['parent_id'], 'valid');
SELECT sql_saga.drop_unique_key('child_range_only'::regclass, ARRAY['id'], 'valid');
SELECT sql_saga.drop_unique_key('parent_range_only'::regclass, ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('child_range_only'::regclass, cleanup => true);
SELECT sql_saga.drop_era('parent_range_only'::regclass, cleanup => true);

DROP TABLE child_range_only;
DROP TABLE parent_range_only;

\echo '-- Performance log from pg_stat_monitor --'
\set monitor_log_filename expected/performance/104_benchmark_range_only_monitor.csv
\i sql/include/benchmark_monitor_csv.sql

-- Verify the benchmark events and row counts
SELECT event, row_count FROM benchmark ORDER BY seq_id;

-- Capture performance metrics to a separate file
\set benchmark_log_filename expected/performance/104_benchmark_range_only_report.log
\i sql/include/benchmark_report_log.sql

\i sql/include/benchmark_teardown.sql
\i sql/include/test_teardown.sql
