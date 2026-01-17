\i sql/include/test_setup.sql
\i sql/include/benchmark_setup.sql

SET ROLE TO sql_saga_unprivileged_user;

--------------------------------------------------------------------------------
-- BENCHMARK: temporal_merge Performance with Range-Only Tables
--
-- This benchmark measures temporal_merge performance using the recommended
-- range-only table pattern with proper indexing via add_foreign_key(create_index => true).
--
-- For comparison of range-only vs synchronized columns, see tests 104 and 105.
--------------------------------------------------------------------------------

CREATE TABLE legal_unit_tm (
  id INTEGER,
  valid_range daterange NOT NULL,
  name varchar NOT NULL
);

CREATE TABLE establishment_tm (
  id INTEGER,
  valid_range daterange NOT NULL,
  legal_unit_id INTEGER NOT NULL,
  postal_place TEXT NOT NULL
);

-- Enable sql_saga constraints (creates optimal GIST index for FK via create_index => true)
SELECT sql_saga.add_era('legal_unit_tm'::regclass, 'valid_range', 'valid');
SELECT sql_saga.add_era('establishment_tm'::regclass, 'valid_range', 'valid');
SELECT sql_saga.add_unique_key(table_oid => 'legal_unit_tm', column_names => ARRAY['id'], era_name => 'valid', unique_key_name => 'legal_unit_tm_id_valid');
SELECT sql_saga.add_unique_key(table_oid => 'establishment_tm', column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'establishment_tm',
    fk_column_names => ARRAY['legal_unit_id'],
    pk_table_oid => 'legal_unit_tm',
    pk_column_names => ARRAY['id'],
    fk_era_name => 'valid',
    create_index => true
);

-- Additional indices recommended by temporal_merge for source/target lookup
CREATE INDEX ON legal_unit_tm USING BTREE (id);
CREATE INDEX ON legal_unit_tm USING GIST (valid_range);
CREATE INDEX ON establishment_tm USING BTREE (id);
CREATE INDEX ON establishment_tm USING GIST (valid_range);

-- Show indices for documentation
\echo '--- Table Indices ---'
SELECT indexname, indexdef FROM pg_indexes WHERE tablename IN ('legal_unit_tm', 'establishment_tm') ORDER BY tablename, indexname;


--------------------------------------------------------------------------------
\echo '--- Temporal Merge SEED (2000 rows) ---'
--------------------------------------------------------------------------------

BEGIN;
  CREATE TEMPORARY TABLE legal_unit_seed_source (row_id int, id int, valid_range daterange, name varchar);
  CREATE TEMPORARY TABLE establishment_seed_source (row_id int, id int, valid_range daterange, legal_unit_id int, postal_place text);

  INSERT INTO legal_unit_seed_source SELECT i, i, daterange('2015-01-01', 'infinity', '[)'), 'Company ' || i FROM generate_series(1, 2000) AS i;
  INSERT INTO establishment_seed_source SELECT i, i, daterange('2015-01-01', 'infinity', '[)'), i, 'Shop ' || i FROM generate_series(1, 2000) AS i;

  CREATE INDEX ON legal_unit_seed_source USING BTREE (id);
  CREATE INDEX ON legal_unit_seed_source USING GIST (valid_range);
  CREATE INDEX ON establishment_seed_source USING BTREE (id);
  CREATE INDEX ON establishment_seed_source USING GIST (valid_range);

  ANALYZE legal_unit_seed_source;
  ANALYZE establishment_seed_source;

  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge SEED Parent', 0, false);
  CALL sql_saga.benchmark_reset();
  CALL sql_saga.temporal_merge(target_table => 'legal_unit_tm'::regclass, source_table => 'legal_unit_seed_source'::regclass, primary_identity_columns => ARRAY['id'], ephemeral_columns => ARRAY[]::TEXT[]);
  CALL sql_saga.benchmark_log_and_reset('Temporal Merge SEED Parent');
  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge SEED Parent end', 2000, true);

  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge SEED Child', 0, false);
  CALL sql_saga.benchmark_reset();
  CALL sql_saga.temporal_merge(target_table => 'establishment_tm'::regclass, source_table => 'establishment_seed_source'::regclass, primary_identity_columns => ARRAY['id'], ephemeral_columns => ARRAY[]::TEXT[]);
  CALL sql_saga.benchmark_log_and_reset('Temporal Merge SEED Child');
  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge SEED Child end', 2000, true);
END;


--------------------------------------------------------------------------------
\echo '--- Temporal Merge 20% INSERT + 80% PATCH (2500 rows) ---'
--------------------------------------------------------------------------------

BEGIN;
  CREATE TEMPORARY TABLE legal_unit_source (row_id int, id int, valid_range daterange, name varchar);
  CREATE TEMPORARY TABLE establishment_source (row_id int, id int, valid_range daterange, legal_unit_id int, postal_place text);

  -- Source data for 80% PATCH (update existing)
  INSERT INTO legal_unit_source SELECT i, i, daterange('2015-01-01', 'infinity', '[)'), 'Updated Company ' || i FROM generate_series(1, 2000) AS i;
  INSERT INTO establishment_source SELECT i, i, daterange('2015-01-01', 'infinity', '[)'), i, 'Updated Shop ' || i FROM generate_series(1, 2000) AS i;

  -- Source data for 20% INSERT (new entities)
  INSERT INTO legal_unit_source SELECT i, i, daterange('2015-01-01', 'infinity', '[)'), 'Company ' || i FROM generate_series(2001, 2500) AS i;
  INSERT INTO establishment_source SELECT i, i, daterange('2015-01-01', 'infinity', '[)'), i, 'Shop ' || i FROM generate_series(2001, 2500) AS i;

  CREATE INDEX ON legal_unit_source USING BTREE (id);
  CREATE INDEX ON legal_unit_source USING GIST (valid_range);
  CREATE INDEX ON establishment_source USING BTREE (id);
  CREATE INDEX ON establishment_source USING GIST (valid_range);

  ANALYZE legal_unit_source;
  ANALYZE establishment_source;

  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge 20% INSERT 80% PATCH Parent', 0, false);
  CALL sql_saga.benchmark_reset();
  CALL sql_saga.temporal_merge(target_table => 'legal_unit_tm'::regclass, source_table => 'legal_unit_source'::regclass, primary_identity_columns => ARRAY['id'], ephemeral_columns => ARRAY[]::TEXT[]);
  CALL sql_saga.benchmark_log_and_reset('Temporal Merge 20% INSERT 80% PATCH Parent');
  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge 20% INSERT 80% PATCH Parent end', 2500, true);

  \set benchmark_explain_log_filename expected/performance/101_benchmark_temporal_merge_explain.log
  \set benchmark_source_table 'legal_unit_source'
  \set benchmark_target_table 'legal_unit_tm'
  \i sql/include/benchmark_explain_log.sql

  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge 20% INSERT 80% PATCH Child', 0, false);
  CALL sql_saga.benchmark_reset();
  CALL sql_saga.temporal_merge(target_table => 'establishment_tm'::regclass, source_table => 'establishment_source'::regclass, primary_identity_columns => ARRAY['id'], ephemeral_columns => ARRAY[]::TEXT[]);
  CALL sql_saga.benchmark_log_and_reset('Temporal Merge 20% INSERT 80% PATCH Child');
  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge 20% INSERT 80% PATCH Child end', 2500, true);
END;

SELECT 'legal_unit_tm' AS type, COUNT(*) AS count FROM legal_unit_tm
UNION ALL
SELECT 'establishment_tm' AS type, COUNT(*) AS count FROM establishment_tm;


-- Teardown
SELECT sql_saga.drop_foreign_key('establishment_tm', ARRAY['legal_unit_id'], 'valid');
SELECT sql_saga.drop_unique_key('establishment_tm', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('establishment_tm', cleanup => true);
SELECT sql_saga.drop_unique_key('legal_unit_tm', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('legal_unit_tm', cleanup => true);

DROP TABLE establishment_tm;
DROP TABLE legal_unit_tm;

\echo '-- Performance log from pg_stat_monitor --'
\set monitor_log_filename expected/performance/101_benchmark_temporal_merge_benchmark_monitor.csv
\i sql/include/benchmark_monitor_csv.sql

-- Verify the benchmark events and row counts
SELECT event, row_count FROM benchmark ORDER BY seq_id;

-- Capture performance metrics to a separate file for manual review
\set benchmark_log_filename expected/performance/101_benchmark_temporal_merge_benchmark_report.log
\i sql/include/benchmark_report_log.sql

\i sql/include/benchmark_teardown.sql
\i sql/include/test_teardown.sql
