\i sql/include/test_setup.sql

SET ROLE TO sql_saga_unprivileged_user;

SET sql_saga.temporal_merge.use_pg_stat_monitor = true;

\i sql/include/benchmark_setup.sql

-- Enable detailed temporal_merge index logging for this benchmark session.
--SET sql_saga.temporal_merge.log_index_checks = true;

CREATE TABLE legal_unit_tm (
  id INTEGER,
  valid_from date,
  valid_until date,
  name varchar NOT NULL
);

CREATE TABLE establishment_tm (
  id INTEGER,
  valid_from date,
  valid_until date,
  legal_unit_id INTEGER NOT NULL,
  postal_place TEXT NOT NULL
);

-- Enable sql_saga constraints
SELECT sql_saga.add_era(table_oid => 'legal_unit_tm', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_era(table_oid => 'establishment_tm', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
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

-- Indices according to warning from temporal_merge
CREATE INDEX ON legal_unit_tm USING BTREE (id);
CREATE INDEX ON legal_unit_tm USING GIST (daterange(valid_from, valid_until));
CREATE INDEX ON establishment_tm USING BTREE (id);
CREATE INDEX ON establishment_tm USING GIST (daterange(valid_from, valid_until));


-- Seed 80% of the data.
BEGIN;
  CREATE TEMPORARY TABLE legal_unit_seed_source (row_id int, id int, valid_from date, valid_until date, name varchar);
  CREATE TEMPORARY TABLE establishment_seed_source (row_id int, id int, valid_from date, valid_until date, legal_unit_id int, postal_place text);

  INSERT INTO legal_unit_seed_source SELECT i, i, '2015-01-01', 'infinity', 'Company ' || i FROM generate_series(1, 8000) AS i;
  INSERT INTO establishment_seed_source SELECT i, i, '2015-01-01', 'infinity', i, 'Shop ' || i FROM generate_series(1, 8000) AS i;
  -- Add indices suggested by temporal_merge warning.
  CREATE INDEX ON legal_unit_seed_source USING BTREE (id);
  CREATE INDEX ON legal_unit_seed_source USING GIST (daterange(valid_from, valid_until, '[)'));
  CREATE INDEX ON establishment_seed_source USING BTREE (id);
  CREATE INDEX ON establishment_seed_source USING GIST (daterange(valid_from, valid_until, '[)'));

  ANALYZE legal_unit_seed_source;
  ANALYZE establishment_seed_source;

  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge SEED Parent (Triggers ON)', 0, false);
  CALL sql_saga.temporal_merge(target_table => 'legal_unit_tm'::regclass, source_table => 'legal_unit_seed_source'::regclass, primary_identity_columns => ARRAY['id'], ephemeral_columns => ARRAY[]::TEXT[]);
  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge SEED Parent (Triggers ON) end', 8000, true);

  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge SEED Child (Triggers ON)', 0, false);
  CALL sql_saga.temporal_merge(target_table => 'establishment_tm'::regclass, source_table => 'establishment_seed_source'::regclass, primary_identity_columns => ARRAY['id'], ephemeral_columns => ARRAY[]::TEXT[]);
  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge SEED Child (Triggers ON) end', 8000, true);
END;


BEGIN;
  CREATE TEMPORARY TABLE legal_unit_source (row_id int, id int, valid_from date, valid_until date, name varchar);
  CREATE TEMPORARY TABLE establishment_source (row_id int, id int, valid_from date, valid_until date, legal_unit_id int, postal_place text);

  -- Source data for 80% PATCH
  INSERT INTO legal_unit_source SELECT i, i, '2015-01-01', 'infinity', 'Updated Company ' || i FROM generate_series(1, 8000) AS i;
  INSERT INTO establishment_source SELECT i, i, '2015-01-01', 'infinity', i, 'Updated Shop ' || i FROM generate_series(1, 8000) AS i;

  -- Source data for 20% INSERT
  INSERT INTO legal_unit_source SELECT i, i, '2015-01-01', 'infinity', 'Company ' || i FROM generate_series(8001, 10000) AS i;
  INSERT INTO establishment_source SELECT i, i, '2015-01-01', 'infinity', i, 'Shop ' || i FROM generate_series(8001, 10000) AS i;

  -- Add indices suggested by temporal_merge performance hints.
  -- The BTREE index on the lookup key (`id`) is critical for the initial filtering of target rows.
  -- The GIST index on the temporal range is critical for the main timeline reconstruction logic.
  CREATE INDEX ON legal_unit_source USING BTREE (id);
  CREATE INDEX ON legal_unit_source USING GIST (daterange(valid_from, valid_until, '[)'));
  CREATE INDEX ON establishment_source USING BTREE (id);
  CREATE INDEX ON establishment_source USING GIST (daterange(valid_from, valid_until, '[)'));

  ANALYZE legal_unit_source;
  ANALYZE establishment_source;

  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge 20% INSERT 80% PATCH Parent (Triggers ON)', 0, false);
  CALL sql_saga.temporal_merge(target_table => 'legal_unit_tm'::regclass, source_table => 'legal_unit_source'::regclass, primary_identity_columns => ARRAY['id'], ephemeral_columns => ARRAY[]::TEXT[]);
  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge 20% INSERT 80% PATCH Parent (Triggers ON) end', 10000, true);

  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge 20% INSERT 80% PATCH Child (Triggers ON)', 0, false);
  CALL sql_saga.temporal_merge(target_table => 'establishment_tm'::regclass, source_table => 'establishment_source'::regclass, primary_identity_columns => ARRAY['id'], ephemeral_columns => ARRAY[]::TEXT[]);
  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge 20% INSERT 80% PATCH Child (Triggers ON) end', 10000, true);
END;

SELECT 'legal_unit_tm' AS type, COUNT(*) AS count FROM legal_unit_tm
UNION ALL
SELECT 'establishment_tm' AS type, COUNT(*) AS count FROM establishment_tm;


--
-- Benchmark with temporal_merge (Triggers Disabled)
--

CREATE TABLE legal_unit_tm_dt (
  id INTEGER,
  valid_from date,
  valid_until date,
  name varchar NOT NULL
);

CREATE TABLE establishment_tm_dt (
  id INTEGER,
  valid_from date,
  valid_until date,
  legal_unit_id INTEGER NOT NULL,
  postal_place TEXT NOT NULL
);

-- This is the critical index for FK performance on the parent table.
CREATE INDEX ON establishment_tm_dt USING GIST (legal_unit_id, daterange(valid_from, valid_until));

-- Enable sql_saga constraints
SELECT sql_saga.add_era(table_oid => 'legal_unit_tm_dt', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_era(table_oid => 'establishment_tm_dt', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'legal_unit_tm_dt', column_names => ARRAY['id'], era_name => 'valid', unique_key_name => 'legal_unit_tm_dt_id_valid');
SELECT sql_saga.add_unique_key(table_oid => 'establishment_tm_dt', column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'establishment_tm_dt',
    fk_column_names => ARRAY['legal_unit_id'],
    pk_table_oid => 'legal_unit_tm_dt',
    pk_column_names => ARRAY['id'],
    fk_era_name => 'valid'
);

-- Seed 80% of the data.
BEGIN;
  CALL sql_saga.disable_temporal_triggers('legal_unit_tm_dt', 'establishment_tm_dt');
  CREATE TEMPORARY TABLE legal_unit_seed_source_dt (row_id int, id int, valid_from date, valid_until date, name varchar);
  CREATE TEMPORARY TABLE establishment_seed_source_dt (row_id int, id int, valid_from date, valid_until date, legal_unit_id int, postal_place text);

  INSERT INTO legal_unit_seed_source_dt SELECT i, i, '2015-01-01', 'infinity', 'Company ' || i FROM generate_series(1, 8000) AS i;
  INSERT INTO establishment_seed_source_dt SELECT i, i, '2015-01-01', 'infinity', i, 'Shop ' || i FROM generate_series(1, 8000) AS i;

  -- Add indices suggested by temporal_merge warning.
  CREATE INDEX ON legal_unit_seed_source_dt USING BTREE (id);
  CREATE INDEX ON legal_unit_seed_source_dt USING GIST (daterange(valid_from, valid_until, '[)'));
  CREATE INDEX ON establishment_seed_source_dt USING BTREE (id);
  CREATE INDEX ON establishment_seed_source_dt USING GIST (daterange(valid_from, valid_until, '[)'));

  ANALYZE legal_unit_seed_source_dt;
  ANALYZE establishment_seed_source_dt;

  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge SEED Parent (Triggers OFF)', 0, false);
  CALL sql_saga.temporal_merge(target_table => 'legal_unit_tm_dt'::regclass, source_table => 'legal_unit_seed_source_dt'::regclass, primary_identity_columns => ARRAY['id'], ephemeral_columns => ARRAY[]::TEXT[]);
  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge SEED Parent (Triggers OFF) end', 8000, true);

  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge SEED Child (Triggers OFF)', 0, false);
  CALL sql_saga.temporal_merge(target_table => 'establishment_tm_dt'::regclass, source_table => 'establishment_seed_source_dt'::regclass, primary_identity_columns => ARRAY['id'], ephemeral_columns => ARRAY[]::TEXT[]);
  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge SEED Child (Triggers OFF) end', 8000, true);

  CALL sql_saga.enable_temporal_triggers('legal_unit_tm_dt', 'establishment_tm_dt');
END;


BEGIN;
  CALL sql_saga.disable_temporal_triggers('legal_unit_tm_dt', 'establishment_tm_dt');
  CREATE TEMPORARY TABLE legal_unit_source_dt (row_id int, id int, valid_from date, valid_until date, name varchar);
  CREATE TEMPORARY TABLE establishment_source_dt (row_id int, id int, valid_from date, valid_until date, legal_unit_id int, postal_place text);

  -- Source data for 80% PATCH
  INSERT INTO legal_unit_source_dt SELECT i, i, '2015-01-01', 'infinity', 'Updated Company ' || i FROM generate_series(1, 8000) AS i;
  INSERT INTO establishment_source_dt SELECT i, i, '2015-01-01', 'infinity', i, 'Updated Shop ' || i FROM generate_series(1, 8000) AS i;

  -- Source data for 20% INSERT
  INSERT INTO legal_unit_source_dt SELECT i, i, '2015-01-01', 'infinity', 'Company ' || i FROM generate_series(8001, 10000) AS i;
  INSERT INTO establishment_source_dt SELECT i, i, '2015-01-01', 'infinity', i, 'Shop ' || i FROM generate_series(8001, 10000) AS i;

  -- Add indices suggested by temporal_merge warning.
  CREATE INDEX ON legal_unit_source_dt USING BTREE (id);
  CREATE INDEX ON legal_unit_source_dt USING GIST (daterange(valid_from, valid_until, '[)'));
  CREATE INDEX ON establishment_source_dt USING BTREE (id);
  CREATE INDEX ON establishment_source_dt USING GIST (daterange(valid_from, valid_until, '[)'));

  ANALYZE legal_unit_source_dt;
  ANALYZE establishment_source_dt;

  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge 20% INSERT 80% PATCH Parent (Triggers OFF)', 0, false);
  CALL sql_saga.temporal_merge(target_table => 'legal_unit_tm_dt'::regclass, source_table => 'legal_unit_source_dt'::regclass, primary_identity_columns => ARRAY['id'], ephemeral_columns => ARRAY[]::TEXT[]);
  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge 20% INSERT 80% PATCH Parent (Triggers OFF) end', 10000, true);

  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge 20% INSERT 80% PATCH Child (Triggers OFF)', 0, false);
  CALL sql_saga.temporal_merge(target_table => 'establishment_tm_dt'::regclass, source_table => 'establishment_source_dt'::regclass, primary_identity_columns => ARRAY['id'], ephemeral_columns => ARRAY[]::TEXT[]);
  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge 20% INSERT 80% PATCH Child (Triggers OFF) end', 10000, true);

  CALL sql_saga.enable_temporal_triggers('legal_unit_tm_dt', 'establishment_tm_dt');
END;

SELECT 'legal_unit_tm_dt' AS type, COUNT(*) AS count FROM legal_unit_tm_dt
UNION ALL
SELECT 'establishment_tm_dt' AS type, COUNT(*) AS count FROM establishment_tm_dt;


-- Teardown for temporal_merge tables
SELECT sql_saga.drop_foreign_key('establishment_tm', ARRAY['legal_unit_id'], 'valid');
SELECT sql_saga.drop_unique_key('establishment_tm', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('establishment_tm', cleanup => true);
SELECT sql_saga.drop_unique_key('legal_unit_tm', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('legal_unit_tm', cleanup => true);

-- Teardown for temporal_merge tables with disabled triggers
SELECT sql_saga.drop_foreign_key('establishment_tm_dt', ARRAY['legal_unit_id'], 'valid');
SELECT sql_saga.drop_unique_key('establishment_tm_dt', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('establishment_tm_dt', cleanup => true);
SELECT sql_saga.drop_unique_key('legal_unit_tm_dt', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('legal_unit_tm_dt', cleanup => true);

DROP TABLE establishment_tm;
DROP TABLE legal_unit_tm;
DROP TABLE establishment_tm_dt;
DROP TABLE legal_unit_tm_dt;

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
\o expected/performance/101_benchmark_temporal_merge_performance.out

\i sql/include/benchmark_report.sql

-- Stop redirecting output
\o

\i sql/include/test_teardown.sql
