\i sql/include/test_setup.sql

SET ROLE TO sql_saga_unprivileged_user;

SET sql_saga.temporal_merge.use_pg_stat_monitor = true;

\i sql/include/benchmark_setup.sql

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
BEGIN;
DO $$
BEGIN
  FOR i IN 1..10000 LOOP
    INSERT INTO legal_unit_sv (id, name) VALUES (i, 'Company ' || i);
    INSERT INTO establishment_sv (id, legal_unit_id, postal_place) VALUES (i, i, 'Shop ' || i);
  END LOOP;
END; $$;
END;
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('History INSERTs end', 10000, true);

-- UPDATE
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('History Update start', 0, false);
BEGIN;
  UPDATE legal_unit_sv SET name = 'New ' || name WHERE id <= 10000;
END;
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('History Update end', 10000, true);

SELECT 'legal_unit_sv' AS type, COUNT(*) AS count FROM legal_unit_sv
UNION ALL
SELECT 'establishment_sv' AS type, COUNT(*) AS count FROM establishment_sv;


--
-- Benchmark with Eras AND History (System Versioning)
--

CREATE TABLE legal_unit_era_history (
  id INTEGER,
  valid_from date,
  valid_until date,
  name varchar NOT NULL
);

CREATE TABLE establishment_era_history (
  id INTEGER,
  valid_from date,
  valid_until date,
  legal_unit_id INTEGER NOT NULL,
  postal_place TEXT NOT NULL
);

-- Enable Eras and System Versioning
SELECT sql_saga.add_era(table_oid => 'legal_unit_era_history', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_era(table_oid => 'establishment_era_history', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
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
BEGIN;
DO $$
BEGIN
  FOR i IN 1..10000 LOOP
    INSERT INTO legal_unit_era_history (id, valid_from, valid_until, name) VALUES (i, '2015-01-01', 'infinity', 'Company ' || i);
    INSERT INTO establishment_era_history (id, valid_from, valid_until, legal_unit_id, postal_place) VALUES (i, '2015-01-01', 'infinity', i, 'Shop ' || i);
  END LOOP;
END; $$;
END;
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Era + History INSERTs end', 10000, true);

-- UPDATE with delayed commit checking
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Era + History Update deferred constraints start', 0, false);
BEGIN;
  SET CONSTRAINTS ALL DEFERRED;
  UPDATE legal_unit_era_history SET valid_until = '2016-01-01' WHERE id <= 10000 AND valid_from = '2015-01-01';
  INSERT INTO legal_unit_era_history (id, valid_from, valid_until, name) SELECT id, '2016-01-01', 'infinity', name FROM legal_unit_era_history WHERE valid_until = '2016-01-01';
  SET CONSTRAINTS ALL IMMEDIATE;
END;
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Era + History Update deferred constraints end', 10000, true);

-- UPDATE with immediate constraints (non-key column)
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Era + History Update non-key start', 0, false);
BEGIN;
  UPDATE legal_unit_era_history SET name = 'New ' || name WHERE id <= 10000;
END;
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
-- Check if pg_stat_monitor is installed, and output performance log if so.
SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_monitor') AS pg_stat_monitor_exists \gset
\if :pg_stat_monitor_exists
    -- The output of this query will be captured in the test's .out file.
    -- We select a stable subset of columns to avoid volatility.
    -- We don't show time, just I/O and row counts. Query is truncated.
    -- This provides a baseline for performance regression testing.
    SELECT event, calls, rows_retrieved, shared_blks_hit, shared_blks_read, left(query, 80) as query_part
    FROM pg_temp.temporal_merge_performance_log
    ORDER BY event, total_time DESC, query_part;
\else
    SELECT 'pg_stat_monitor not installed, skipping performance log output.' as notice;
\endif

-- Verify the benchmark events and row counts, but exclude volatile timing data
-- from the regression test output to ensure stability.
SELECT event, row_count FROM benchmark ORDER BY seq_id;

-- Capture performance metrics to a separate file for manual review.
\o expected/performance/103_benchmark_system_versioning_performance.out

\i sql/include/benchmark_report.sql

-- Stop redirecting output
\o

\i sql/include/test_teardown.sql
