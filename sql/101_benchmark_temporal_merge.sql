\i sql/include/test_setup.sql

SET ROLE TO sql_saga_unprivileged_user;

CREATE TEMPORARY TABLE benchmark (
  timestamp TIMESTAMPTZ DEFAULT NOW(),
  event TEXT,
  row_count INTEGER
);

INSERT INTO benchmark (event, row_count) VALUES ('BEGIN', 0);

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
SELECT sql_saga.add_unique_key(table_oid => 'legal_unit_tm', column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.add_unique_key(table_oid => 'establishment_tm', column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.add_temporal_foreign_key(
    fk_table_oid => 'establishment_tm',
    fk_column_names => ARRAY['legal_unit_id'],
    fk_era_name => 'valid',
    unique_key_name => 'legal_unit_tm_id_valid'
);

-- Seed 80% of the data.
INSERT INTO benchmark (event, row_count) VALUES ('Temporal Merge SEED (Triggers ON) start', 0);
BEGIN;
  CREATE TEMPORARY TABLE legal_unit_seed_source (row_id int, id int, valid_from date, valid_until date, name varchar);
  CREATE TEMPORARY TABLE establishment_seed_source (row_id int, id int, valid_from date, valid_until date, legal_unit_id int, postal_place text);

  INSERT INTO legal_unit_seed_source SELECT i, i, '2015-01-01', 'infinity', 'Company ' || i FROM generate_series(1, 8000) AS i;
  INSERT INTO establishment_seed_source SELECT i, i, '2015-01-01', 'infinity', i, 'Shop ' || i FROM generate_series(1, 8000) AS i;

  CALL sql_saga.temporal_merge(target_table => 'legal_unit_tm'::regclass, source_table => 'legal_unit_seed_source'::regclass, identity_columns => ARRAY['id'], ephemeral_columns => ARRAY[]::TEXT[]);
  CALL sql_saga.temporal_merge(target_table => 'establishment_tm'::regclass, source_table => 'establishment_seed_source'::regclass, identity_columns => ARRAY['id'], ephemeral_columns => ARRAY[]::TEXT[]);
END;
INSERT INTO benchmark (event, row_count) VALUES ('Temporal Merge SEED (Triggers ON) end', 16000);


INSERT INTO benchmark (event, row_count) VALUES ('Temporal Merge 20% INSERT 80% PATCH (Triggers ON) start', 0);
BEGIN;
  CREATE TEMPORARY TABLE legal_unit_source (row_id int, id int, valid_from date, valid_until date, name varchar);
  CREATE TEMPORARY TABLE establishment_source (row_id int, id int, valid_from date, valid_until date, legal_unit_id int, postal_place text);

  -- Source data for 80% PATCH
  INSERT INTO legal_unit_source SELECT i, i, '2015-01-01', 'infinity', 'Updated Company ' || i FROM generate_series(1, 8000) AS i;
  INSERT INTO establishment_source SELECT i, i, '2015-01-01', 'infinity', i, 'Updated Shop ' || i FROM generate_series(1, 8000) AS i;

  -- Source data for 20% INSERT
  INSERT INTO legal_unit_source SELECT i, i, '2015-01-01', 'infinity', 'Company ' || i FROM generate_series(8001, 10000) AS i;
  INSERT INTO establishment_source SELECT i, i, '2015-01-01', 'infinity', i, 'Shop ' || i FROM generate_series(8001, 10000) AS i;

  CALL sql_saga.temporal_merge(target_table => 'legal_unit_tm'::regclass, source_table => 'legal_unit_source'::regclass, identity_columns => ARRAY['id'], ephemeral_columns => ARRAY[]::TEXT[]);
  CALL sql_saga.temporal_merge(target_table => 'establishment_tm'::regclass, source_table => 'establishment_source'::regclass, identity_columns => ARRAY['id'], ephemeral_columns => ARRAY[]::TEXT[]);
END;
INSERT INTO benchmark (event, row_count) VALUES ('Temporal Merge 20% INSERT 80% PATCH (Triggers ON) end', 20000);

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

-- Enable sql_saga constraints
SELECT sql_saga.add_era(table_oid => 'legal_unit_tm_dt', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_era(table_oid => 'establishment_tm_dt', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'legal_unit_tm_dt', column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.add_unique_key(table_oid => 'establishment_tm_dt', column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.add_temporal_foreign_key(
    fk_table_oid => 'establishment_tm_dt',
    fk_column_names => ARRAY['legal_unit_id'],
    fk_era_name => 'valid',
    unique_key_name => 'legal_unit_tm_dt_id_valid'
);

-- Seed 80% of the data.
INSERT INTO benchmark (event, row_count) VALUES ('Temporal Merge SEED (Triggers OFF) start', 0);
BEGIN;
  CALL sql_saga.disable_temporal_triggers('legal_unit_tm_dt', 'establishment_tm_dt');
  CREATE TEMPORARY TABLE legal_unit_seed_source_dt (row_id int, id int, valid_from date, valid_until date, name varchar);
  CREATE TEMPORARY TABLE establishment_seed_source_dt (row_id int, id int, valid_from date, valid_until date, legal_unit_id int, postal_place text);

  INSERT INTO legal_unit_seed_source_dt SELECT i, i, '2015-01-01', 'infinity', 'Company ' || i FROM generate_series(1, 8000) AS i;
  INSERT INTO establishment_seed_source_dt SELECT i, i, '2015-01-01', 'infinity', i, 'Shop ' || i FROM generate_series(1, 8000) AS i;

  CALL sql_saga.temporal_merge(target_table => 'legal_unit_tm_dt'::regclass, source_table => 'legal_unit_seed_source_dt'::regclass, identity_columns => ARRAY['id'], ephemeral_columns => ARRAY[]::TEXT[]);
  CALL sql_saga.temporal_merge(target_table => 'establishment_tm_dt'::regclass, source_table => 'establishment_seed_source_dt'::regclass, identity_columns => ARRAY['id'], ephemeral_columns => ARRAY[]::TEXT[]);
  CALL sql_saga.enable_temporal_triggers('legal_unit_tm_dt', 'establishment_tm_dt');
END;
INSERT INTO benchmark (event, row_count) VALUES ('Temporal Merge SEED (Triggers OFF) end', 16000);


INSERT INTO benchmark (event, row_count) VALUES ('Temporal Merge 20% INSERT 80% PATCH (Triggers OFF) start', 0);
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

  CALL sql_saga.temporal_merge(target_table => 'legal_unit_tm_dt'::regclass, source_table => 'legal_unit_source_dt'::regclass, identity_columns => ARRAY['id'], ephemeral_columns => ARRAY[]::TEXT[]);
  CALL sql_saga.temporal_merge(target_table => 'establishment_tm_dt'::regclass, source_table => 'establishment_source_dt'::regclass, identity_columns => ARRAY['id'], ephemeral_columns => ARRAY[]::TEXT[]);
  CALL sql_saga.enable_temporal_triggers('legal_unit_tm_dt', 'establishment_tm_dt');
END;
INSERT INTO benchmark (event, row_count) VALUES ('Temporal Merge 20% INSERT 80% PATCH (Triggers OFF) end', 20000);

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

INSERT INTO benchmark (event, row_count) VALUES ('Constraints disabled', 0);

DROP TABLE establishment_tm;
DROP TABLE legal_unit_tm;
DROP TABLE establishment_tm_dt;
DROP TABLE legal_unit_tm_dt;

INSERT INTO benchmark (event, row_count) VALUES ('Tear down complete', 0);

-- Verify the benchmark events and row counts, but exclude volatile timing data
-- from the regression test output to ensure stability.
SELECT event, row_count FROM benchmark ORDER BY timestamp;

-- Capture performance metrics to a separate file for manual review.
\o expected/101_benchmark_temporal_merge_performance.out

-- Calculate rows per second
CREATE OR REPLACE FUNCTION round_to_nearest_100(value FLOAT8) RETURNS FLOAT8 AS $$
BEGIN
    RETURN round(value / 100.0) * 100.0;
END;
$$ LANGUAGE plpgsql;

WITH benchmark_events AS (
  SELECT
    timestamp,
    event,
    row_count,
    regexp_replace(event, ' (start|end)$', '') AS operation,
    CASE
      WHEN event LIKE '% start' THEN 'start'
      WHEN event LIKE '% end' THEN 'end'
      ELSE 'milestone'
    END AS phase,
    FIRST_VALUE(timestamp) OVER (ORDER BY timestamp) as benchmark_start_time,
    LAG(timestamp) OVER (ORDER BY timestamp) as prev_event_time
  FROM
    benchmark
),
benchmark_durations AS (
  SELECT
    *,
    LAG(timestamp) OVER (PARTITION BY operation ORDER BY timestamp) AS operation_start_time
  FROM
    benchmark_events
)
SELECT
  CASE WHEN phase = 'end' THEN operation ELSE event END AS event,
  row_count,
  ROUND(EXTRACT(EPOCH FROM (timestamp - benchmark_start_time)))::numeric || ' secs' AS time_from_start,
  COALESCE(
    CASE
      WHEN phase = 'end' THEN
        ROUND(EXTRACT(EPOCH FROM (timestamp - operation_start_time)))::numeric || ' secs'
      WHEN phase = 'milestone' THEN
        ROUND(EXTRACT(EPOCH FROM (timestamp - prev_event_time)))::numeric || ' secs'
    END,
    ''
  ) AS time_from_prev,
  row_count || ' rows' AS row_count,
  '~' || COALESCE(
    CASE
      WHEN phase = 'end' AND EXTRACT(EPOCH FROM (timestamp - operation_start_time)) > 0 THEN
        round_to_nearest_100(row_count::FLOAT8 / EXTRACT(EPOCH FROM (timestamp - operation_start_time)))::text
      WHEN phase = 'milestone' AND EXTRACT(EPOCH FROM (timestamp - prev_event_time)) > 0 AND row_count > 0 THEN
        round_to_nearest_100(row_count::FLOAT8 / EXTRACT(EPOCH FROM (timestamp - prev_event_time)))::text
      ELSE
        '0'
    END,
    '0'
  ) || ' rows/s' AS rows_per_second
FROM
  benchmark_durations
WHERE phase <> 'start'
ORDER BY
  timestamp;

-- Stop redirecting output
\o

\i sql/include/test_teardown.sql
