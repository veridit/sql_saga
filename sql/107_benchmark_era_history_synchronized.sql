\i sql/include/test_setup.sql
\i sql/include/benchmark_setup.sql

SET ROLE TO sql_saga_unprivileged_user;

--------------------------------------------------------------------------------
-- BENCHMARK: Era + System Versioning with Synchronized Columns
--
-- This benchmark uses tables with valid_range AND valid_from/valid_until columns.
-- The synchronization trigger fires on every INSERT/UPDATE to keep them in sync.
-- System versioning (system_time) is enabled for history tracking.
-- This measures the combined overhead of:
-- - Native WITHOUT OVERLAPS constraints
-- - Native temporal foreign keys
-- - System versioning triggers (write_history, generated_always)
-- - PLUS the application-time synchronization trigger
--------------------------------------------------------------------------------

\echo '--- Creating Synchronized Era + History Tables ---'

CREATE TABLE parent_era_history_sync (
    id INTEGER,
    valid_range daterange NOT NULL,
    valid_from DATE,
    valid_until DATE,
    name VARCHAR NOT NULL
);

CREATE TABLE child_era_history_sync (
    id INTEGER,
    valid_range daterange NOT NULL,
    valid_from DATE,
    valid_until DATE,
    parent_id INTEGER NOT NULL,
    description TEXT NOT NULL
);

-- Add application-time eras with synchronization (creates sync trigger)
SELECT sql_saga.add_era('parent_era_history_sync'::regclass, 'valid_range', 'valid',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_era('child_era_history_sync'::regclass, 'valid_range', 'valid',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');

-- Add unique keys (uses WITHOUT OVERLAPS)
SELECT sql_saga.add_unique_key('parent_era_history_sync'::regclass, ARRAY['id'], 'valid');
SELECT sql_saga.add_unique_key('child_era_history_sync'::regclass, ARRAY['id'], 'valid');

-- Add temporal foreign key (uses native PG18 temporal FK)
SELECT sql_saga.add_foreign_key(
    'child_era_history_sync'::regclass,
    ARRAY['parent_id'],
    'parent_era_history_sync'::regclass,
    ARRAY['id'],
    fk_era_name => 'valid',
    create_index => true
);

-- Add system versioning (creates system_time era and history triggers)
SELECT sql_saga.add_system_versioning('parent_era_history_sync'::regclass);
SELECT sql_saga.add_system_versioning('child_era_history_sync'::regclass);

-- Create performance indexes
CREATE INDEX ON parent_era_history_sync USING GIST (valid_range);
CREATE INDEX ON child_era_history_sync USING GIST (valid_range);

\echo '--- Populating Synchronized Era + History Tables ---'

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced Era+History INSERTs start', 0, false);
CALL sql_saga.benchmark_reset();
BEGIN;
DO $$
BEGIN
    FOR i IN 1..10000 LOOP
        INSERT INTO parent_era_history_sync (id, valid_range, name)
        VALUES (i, daterange('2015-01-01', 'infinity', '[)'), 'Company ' || i);
        INSERT INTO child_era_history_sync (id, valid_range, parent_id, description)
        VALUES (i, daterange('2015-01-01', 'infinity', '[)'), i, 'Shop ' || i);
    END LOOP;
END; $$;
END;
CALL sql_saga.benchmark_log_and_reset('Synced Era+History INSERTs');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced Era+History INSERTs end', 10000, true);

ANALYZE parent_era_history_sync;
ANALYZE child_era_history_sync;

--------------------------------------------------------------------------------
\echo '--- UPDATE Benchmarks on Synchronized Era + History Tables ---'
--------------------------------------------------------------------------------

-- UPDATE with deferred constraints (temporal key change - split timeline)
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced Era+History Update deferred start', 0, false);
CALL sql_saga.benchmark_reset();
BEGIN;
    SET CONSTRAINTS ALL DEFERRED;
    UPDATE parent_era_history_sync
    SET valid_range = daterange(lower(valid_range), '2016-01-01', '[)')
    WHERE id <= 10000 AND lower(valid_range) = '2015-01-01';
    INSERT INTO parent_era_history_sync (id, valid_range, name)
    SELECT id, daterange('2016-01-01', 'infinity', '[)'), name
    FROM parent_era_history_sync
    WHERE upper(valid_range) = '2016-01-01';
    SET CONSTRAINTS ALL IMMEDIATE;
END;
CALL sql_saga.benchmark_log_and_reset('Synced Era+History Update deferred');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced Era+History Update deferred end', 10000, true);

-- UPDATE non-key column (no temporal constraint impact)
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced Era+History Update non-key start', 0, false);
CALL sql_saga.benchmark_reset();
BEGIN;
    UPDATE parent_era_history_sync SET name = 'New ' || name WHERE id <= 10000;
END;
CALL sql_saga.benchmark_log_and_reset('Synced Era+History Update non-key');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Synced Era+History Update non-key end', 10000, true);

-- Show row counts
SELECT 'parent_era_history_sync' AS type, COUNT(*) AS count FROM parent_era_history_sync
UNION ALL
SELECT 'child_era_history_sync' AS type, COUNT(*) AS count FROM child_era_history_sync;

--------------------------------------------------------------------------------
\echo '--- Teardown ---'
--------------------------------------------------------------------------------

SELECT sql_saga.drop_foreign_key('child_era_history_sync'::regclass, ARRAY['parent_id'], 'valid');
SELECT sql_saga.drop_unique_key('child_era_history_sync'::regclass, ARRAY['id'], 'valid');
SELECT sql_saga.drop_unique_key('parent_era_history_sync'::regclass, ARRAY['id'], 'valid');
SELECT sql_saga.drop_system_versioning('child_era_history_sync'::regclass, cleanup => true);
SELECT sql_saga.drop_system_versioning('parent_era_history_sync'::regclass, cleanup => true);
SELECT sql_saga.drop_era('child_era_history_sync'::regclass, cleanup => true);
SELECT sql_saga.drop_era('parent_era_history_sync'::regclass, cleanup => true);

DROP TABLE child_era_history_sync;
DROP TABLE parent_era_history_sync;

\echo '-- Performance log from pg_stat_monitor --'
\set monitor_log_filename expected/performance/107_benchmark_era_history_synchronized_monitor.csv
\i sql/include/benchmark_monitor_csv.sql

-- Verify the benchmark events and row counts
SELECT event, row_count FROM benchmark ORDER BY seq_id;

-- Capture performance metrics to a separate file
\set benchmark_log_filename expected/performance/107_benchmark_era_history_synchronized_report.log
\i sql/include/benchmark_report_log.sql

\i sql/include/benchmark_teardown.sql
\i sql/include/test_teardown.sql
