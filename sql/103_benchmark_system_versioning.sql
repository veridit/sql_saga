\i sql/include/test_setup.sql
\i sql/include/benchmark_setup.sql

SET ROLE TO sql_saga_unprivileged_user;
--
-- Benchmark with System Versioning ONLY
--

CREATE TABLE legal_unit_sv (
  id INTEGER,
  name varchar NOT NULL
);

CREATE TABLE establishment_sv (
  id INTEGER,
  legal_unit_id INTEGER NOT NULL,
  postal_place TEXT NOT NULL
);

-- Enable system versioning
SELECT sql_saga.add_system_versioning(table_oid => 'legal_unit_sv');
SELECT sql_saga.add_system_versioning(table_oid => 'establishment_sv');

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('History Only Benchmark', 0, false);

-- With immediate constraints (the default)
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('History INSERTs start', 0, false);
CALL sql_saga.benchmark_reset();
BEGIN;
-- Optimized: Use bulk INSERT with generate_series (10-50x faster than row-by-row)
INSERT INTO legal_unit_sv (id, name) 
SELECT i, 'Company ' || i FROM generate_series(1, 10000) i;
INSERT INTO establishment_sv (id, legal_unit_id, postal_place) 
SELECT i, i, 'Shop ' || i FROM generate_series(1, 10000) i;
END;
CALL sql_saga.benchmark_log_and_reset('History INSERTs');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('History INSERTs end', 10000, true);

-- UPDATE
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('History Update start', 0, false);
CALL sql_saga.benchmark_reset();
BEGIN;
  UPDATE legal_unit_sv SET name = 'New ' || name WHERE id <= 10000;
END;
CALL sql_saga.benchmark_log_and_reset('History Update');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('History Update end', 10000, true);

SELECT 'legal_unit_sv' AS type, COUNT(*) AS count FROM legal_unit_sv
UNION ALL
SELECT 'establishment_sv' AS type, COUNT(*) AS count FROM establishment_sv;


--
-- Benchmark with Eras AND History (System Versioning) - using range-only approach
--

CREATE TABLE legal_unit_era_history (
  id INTEGER,
  valid_range daterange NOT NULL,
  name varchar NOT NULL
);

CREATE TABLE establishment_era_history (
  id INTEGER,
  valid_range daterange NOT NULL,
  legal_unit_id INTEGER NOT NULL,
  postal_place TEXT NOT NULL
);

-- Enable Eras and System Versioning
SELECT sql_saga.add_era('legal_unit_era_history'::regclass, 'valid_range', 'valid');
SELECT sql_saga.add_era('establishment_era_history'::regclass, 'valid_range', 'valid');
SELECT sql_saga.add_unique_key(table_oid => 'legal_unit_era_history', column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.add_unique_key(table_oid => 'establishment_era_history', column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.add_temporal_foreign_key(
    fk_table_oid => 'establishment_era_history',
    fk_column_names => ARRAY['legal_unit_id'],
    fk_era_name => 'valid',
    unique_key_name => 'legal_unit_era_history_id_valid'
);
SELECT sql_saga.add_system_versioning(table_oid => 'legal_unit_era_history');
SELECT sql_saga.add_system_versioning(table_oid => 'establishment_era_history');


INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Era + History Benchmark', 0, false);

-- With immediate constraints (the default)
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Era + History INSERTs start', 0, false);
CALL sql_saga.benchmark_reset();
BEGIN;
-- Optimized: Use bulk INSERT with generate_series (10-50x faster than row-by-row)
INSERT INTO legal_unit_era_history (id, valid_range, name) 
SELECT i, daterange('2015-01-01', 'infinity', '[)'), 'Company ' || i 
FROM generate_series(1, 10000) i;
INSERT INTO establishment_era_history (id, valid_range, legal_unit_id, postal_place) 
SELECT i, daterange('2015-01-01', 'infinity', '[)'), i, 'Shop ' || i 
FROM generate_series(1, 10000) i;
END;
CALL sql_saga.benchmark_log_and_reset('Era + History INSERTs');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Era + History INSERTs end', 10000, true);

-- UPDATE with delayed commit checking
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Era + History Update deferred constraints start', 0, false);
CALL sql_saga.benchmark_reset();
BEGIN;
  SET CONSTRAINTS ALL DEFERRED;
  UPDATE legal_unit_era_history SET valid_range = daterange(lower(valid_range), '2016-01-01', '[)') WHERE id <= 10000 AND lower(valid_range) = '2015-01-01';
  INSERT INTO legal_unit_era_history (id, valid_range, name) SELECT id, daterange('2016-01-01', 'infinity', '[)'), name FROM legal_unit_era_history WHERE upper(valid_range) = '2016-01-01';
  SET CONSTRAINTS ALL IMMEDIATE;
END;
CALL sql_saga.benchmark_log_and_reset('Era + History Update deferred constraints');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Era + History Update deferred constraints end', 10000, true);

-- UPDATE with immediate constraints (non-key column)
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Era + History Update non-key start', 0, false);
CALL sql_saga.benchmark_reset();
BEGIN;
  UPDATE legal_unit_era_history SET name = 'New ' || name WHERE id <= 10000;
END;
CALL sql_saga.benchmark_log_and_reset('Era + History Update non-key');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Era + History Update non-key end', 10000, true);

SELECT 'legal_unit_era_history' AS type, COUNT(*) AS count FROM legal_unit_era_history
UNION ALL
SELECT 'establishment_era_history' AS type, COUNT(*) AS count FROM establishment_era_history;


-- Teardown for SV tables
SELECT sql_saga.drop_system_versioning('establishment_sv', cleanup => true);
SELECT sql_saga.drop_system_versioning('legal_unit_sv', cleanup => true);

-- Teardown for Era + History tables
SELECT sql_saga.drop_foreign_key('establishment_era_history', ARRAY['legal_unit_id'], 'valid');
SELECT sql_saga.drop_unique_key('establishment_era_history', ARRAY['id'], 'valid');
SELECT sql_saga.drop_system_versioning('establishment_era_history', cleanup => true);
SELECT sql_saga.drop_era('establishment_era_history', cleanup => true);

SELECT sql_saga.drop_unique_key('legal_unit_era_history', ARRAY['id'], 'valid');
SELECT sql_saga.drop_system_versioning('legal_unit_era_history', cleanup => true);
SELECT sql_saga.drop_era('legal_unit_era_history', cleanup => true);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Constraints disabled', 0, false);

DROP TABLE establishment_sv;
DROP TABLE legal_unit_sv;
DROP TABLE establishment_era_history;
DROP TABLE legal_unit_era_history;

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Tear down complete', 0, false);

\echo '-- Performance log from pg_stat_monitor --'
\set monitor_log_filename expected/performance/103_benchmark_system_versioning_benchmark_monitor.csv
\i sql/include/benchmark_monitor_csv.sql

-- Verify the benchmark events and row counts, but exclude volatile timing data
-- from the regression test output to ensure stability.
SELECT event, row_count FROM benchmark ORDER BY seq_id;

-- Capture performance metrics to a separate file for manual review.
\set benchmark_log_filename expected/performance/103_benchmark_system_versioning_benchmark_report.log
\i sql/include/benchmark_report_log.sql

\i sql/include/benchmark_teardown.sql
\i sql/include/test_teardown.sql
