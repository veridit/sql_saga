\i sql/include/test_setup.sql

SET ROLE TO sql_saga_unprivileged_user;

CREATE TABLE legal_unit (
  id INTEGER,
  valid_from date,
  valid_until date,
  name varchar NOT NULL
);

CREATE TABLE establishment (
  id INTEGER,
  valid_from date,
  valid_until date,
  legal_unit_id INTEGER NOT NULL,
  postal_place TEXT NOT NULL
);

CREATE TEMPORARY TABLE benchmark (
  timestamp TIMESTAMPTZ DEFAULT NOW(),
  event TEXT,
  row_count INTEGER
);

-- Record the start of the setup
INSERT INTO benchmark (event, row_count) VALUES ('BEGIN', 0);

-- Enable sql_saga constraints
SELECT sql_saga.add_era(table_oid => 'legal_unit', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_era(table_oid => 'establishment', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'legal_unit', column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.add_unique_key(table_oid => 'establishment', column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'establishment',
    fk_column_names => ARRAY['legal_unit_id'],
    fk_era_name => 'valid',
    unique_key_name => 'legal_unit_id_valid'
);

-- Record after enabling constraints
INSERT INTO benchmark (event, row_count) VALUES ('Constraints enabled', 0);

-- Count number of different units before, during, and after.
SELECT 'legal_unit' AS type, COUNT(*) AS count FROM legal_unit
UNION ALL
SELECT 'establishment' AS type, COUNT(*) AS count FROM establishment;

-- With delayed constraints
INSERT INTO benchmark (event, row_count) VALUES ('Era INSERTs delayed constraints start', 0);
BEGIN;
SET CONSTRAINTS ALL DEFERRED;

DO $$
BEGIN
  FOR i IN 1..10000 LOOP
    INSERT INTO legal_unit (id, valid_from, valid_until, name) VALUES
    (i, '2015-01-01', 'infinity', 'Company ' || i);

    INSERT INTO establishment (id, valid_from, valid_until, legal_unit_id, postal_place) VALUES
    (i, '2015-01-01', 'infinity', i, 'Shop ' || i);
  END LOOP;
END; $$;

SET CONSTRAINTS ALL IMMEDIATE;
END;
INSERT INTO benchmark (event, row_count) VALUES ('Era INSERTs delayed constraints end', 10000);


SELECT 'legal_unit' AS type, COUNT(*) AS count FROM legal_unit
UNION ALL
SELECT 'establishment' AS type, COUNT(*) AS count FROM establishment;

-- With immediate constraints (the default)
INSERT INTO benchmark (event, row_count) VALUES ('Era INSERTs start', 0);
BEGIN;
DO $$
BEGIN
  FOR i IN 10001..20000 LOOP
    INSERT INTO legal_unit (id, valid_from, valid_until, name) VALUES
    (i, '2015-01-01', 'infinity', 'Company ' || i);

    INSERT INTO establishment (id, valid_from, valid_until, legal_unit_id, postal_place) VALUES
    (i, '2015-01-01', 'infinity', i, 'Shop ' || i);
  END LOOP;
END; $$;
END;
INSERT INTO benchmark (event, row_count) VALUES ('Era INSERTs end', 10000);

SELECT 'legal_unit' AS type, COUNT(*) AS count FROM legal_unit
UNION ALL
SELECT 'establishment' AS type, COUNT(*) AS count FROM establishment;

-- UPDATE with delayed commit checking
INSERT INTO benchmark (event, row_count) VALUES ('Era Update deferred constraints start', 0);
BEGIN;
  SET CONSTRAINTS ALL DEFERRED;
  UPDATE legal_unit SET valid_until = '2016-01-01' WHERE id <= 10000 AND valid_from = '2015-01-01';

  INSERT INTO legal_unit (id, valid_from, valid_until, name)
    SELECT id, '2016-01-01', 'infinity', name
    FROM legal_unit WHERE valid_until = '2016-01-01';

  SET CONSTRAINTS ALL IMMEDIATE;
END;
INSERT INTO benchmark (event, row_count) VALUES ('Era Update deferred constraints end', 10000);

-- UPDATE with immediate constraints (non-key column)
INSERT INTO benchmark (event, row_count) VALUES ('Era Update non-key start', 0);
BEGIN;
  UPDATE legal_unit SET name = 'New ' || name WHERE id > 10000;
END;
INSERT INTO benchmark (event, row_count) VALUES ('Era Update non-key end', 10000);

SELECT 'legal_unit' AS type, COUNT(*) AS count FROM legal_unit
UNION ALL
SELECT 'establishment' AS type, COUNT(*) AS count FROM establishment;


--
-- Benchmark with temporal_merge
--

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
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'establishment_tm',
    fk_column_names => ARRAY['legal_unit_id'],
    fk_era_name => 'valid',
    unique_key_name => 'legal_unit_tm_id_valid'
);

-- Seed 80% of the data.
INSERT INTO benchmark (event, row_count) VALUES ('Temporal Merge SEED start', 0);
BEGIN;
  CREATE TEMPORARY TABLE legal_unit_seed_source (row_id int, id int, valid_from date, valid_until date, name varchar);
  CREATE TEMPORARY TABLE establishment_seed_source (row_id int, id int, valid_from date, valid_until date, legal_unit_id int, postal_place text);

  INSERT INTO legal_unit_seed_source SELECT i, i, '2015-01-01', 'infinity', 'Company ' || i FROM generate_series(1, 8000) AS i;
  INSERT INTO establishment_seed_source SELECT i, i, '2015-01-01', 'infinity', i, 'Shop ' || i FROM generate_series(1, 8000) AS i;

  CALL sql_saga.temporal_merge(p_target_table => 'legal_unit_tm'::regclass, p_source_table => 'legal_unit_seed_source'::regclass, p_id_columns => ARRAY['id'], p_ephemeral_columns => ARRAY[]::TEXT[]);
  CALL sql_saga.temporal_merge(p_target_table => 'establishment_tm'::regclass, p_source_table => 'establishment_seed_source'::regclass, p_id_columns => ARRAY['id'], p_ephemeral_columns => ARRAY[]::TEXT[]);
END;
INSERT INTO benchmark (event, row_count) VALUES ('Temporal Merge SEED end', 16000);


INSERT INTO benchmark (event, row_count) VALUES ('Temporal Merge 20% INSERT 80% PATCH start', 0);
BEGIN;
  CREATE TEMPORARY TABLE legal_unit_source (row_id int, id int, valid_from date, valid_until date, name varchar);
  CREATE TEMPORARY TABLE establishment_source (row_id int, id int, valid_from date, valid_until date, legal_unit_id int, postal_place text);

  -- Source data for 80% PATCH
  INSERT INTO legal_unit_source SELECT i, i, '2015-01-01', 'infinity', 'Updated Company ' || i FROM generate_series(1, 8000) AS i;
  INSERT INTO establishment_source SELECT i, i, '2015-01-01', 'infinity', i, 'Updated Shop ' || i FROM generate_series(1, 8000) AS i;

  -- Source data for 20% INSERT
  INSERT INTO legal_unit_source SELECT i, i, '2015-01-01', 'infinity', 'Company ' || i FROM generate_series(8001, 10000) AS i;
  INSERT INTO establishment_source SELECT i, i, '2015-01-01', 'infinity', i, 'Shop ' || i FROM generate_series(8001, 10000) AS i;

  CALL sql_saga.temporal_merge(p_target_table => 'legal_unit_tm'::regclass, p_source_table => 'legal_unit_source'::regclass, p_id_columns => ARRAY['id'], p_ephemeral_columns => ARRAY[]::TEXT[]);
  CALL sql_saga.temporal_merge(p_target_table => 'establishment_tm'::regclass, p_source_table => 'establishment_source'::regclass, p_id_columns => ARRAY['id'], p_ephemeral_columns => ARRAY[]::TEXT[]);
END;
INSERT INTO benchmark (event, row_count) VALUES ('Temporal Merge 20% INSERT 80% PATCH end', 20000);

SELECT 'legal_unit_tm' AS type, COUNT(*) AS count FROM legal_unit_tm
UNION ALL
SELECT 'establishment_tm' AS type, COUNT(*) AS count FROM establishment_tm;


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

INSERT INTO benchmark (event, row_count) VALUES ('History Only Benchmark', 0);

-- With immediate constraints (the default)
INSERT INTO benchmark (event, row_count) VALUES ('History INSERTs start', 0);
BEGIN;
DO $$
BEGIN
  FOR i IN 1..10000 LOOP
    INSERT INTO legal_unit_sv (id, name) VALUES (i, 'Company ' || i);
    INSERT INTO establishment_sv (id, legal_unit_id, postal_place) VALUES (i, i, 'Shop ' || i);
  END LOOP;
END; $$;
END;
INSERT INTO benchmark (event, row_count) VALUES ('History INSERTs end', 10000);

-- UPDATE
INSERT INTO benchmark (event, row_count) VALUES ('History Update start', 0);
BEGIN;
  UPDATE legal_unit_sv SET name = 'New ' || name WHERE id <= 10000;
END;
INSERT INTO benchmark (event, row_count) VALUES ('History Update end', 10000);

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
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'establishment_era_history',
    fk_column_names => ARRAY['legal_unit_id'],
    fk_era_name => 'valid',
    unique_key_name => 'legal_unit_era_history_id_valid'
);
SELECT sql_saga.add_system_versioning(table_oid => 'legal_unit_era_history');
SELECT sql_saga.add_system_versioning(table_oid => 'establishment_era_history');


INSERT INTO benchmark (event, row_count) VALUES ('Era + History Benchmark', 0);

-- With immediate constraints (the default)
INSERT INTO benchmark (event, row_count) VALUES ('Era + History INSERTs start', 0);
BEGIN;
DO $$
BEGIN
  FOR i IN 1..10000 LOOP
    INSERT INTO legal_unit_era_history (id, valid_from, valid_until, name) VALUES (i, '2015-01-01', 'infinity', 'Company ' || i);
    INSERT INTO establishment_era_history (id, valid_from, valid_until, legal_unit_id, postal_place) VALUES (i, '2015-01-01', 'infinity', i, 'Shop ' || i);
  END LOOP;
END; $$;
END;
INSERT INTO benchmark (event, row_count) VALUES ('Era + History INSERTs end', 10000);

-- UPDATE with delayed commit checking
INSERT INTO benchmark (event, row_count) VALUES ('Era + History Update deferred constraints start', 0);
BEGIN;
  SET CONSTRAINTS ALL DEFERRED;
  UPDATE legal_unit_era_history SET valid_until = '2016-01-01' WHERE id <= 10000 AND valid_from = '2015-01-01';
  INSERT INTO legal_unit_era_history (id, valid_from, valid_until, name) SELECT id, '2016-01-01', 'infinity', name FROM legal_unit_era_history WHERE valid_until = '2016-01-01';
  SET CONSTRAINTS ALL IMMEDIATE;
END;
INSERT INTO benchmark (event, row_count) VALUES ('Era + History Update deferred constraints end', 10000);

-- UPDATE with immediate constraints (non-key column)
INSERT INTO benchmark (event, row_count) VALUES ('Era + History Update non-key start', 0);
BEGIN;
  UPDATE legal_unit_era_history SET name = 'New ' || name WHERE id <= 10000;
END;
INSERT INTO benchmark (event, row_count) VALUES ('Era + History Update non-key end', 10000);

SELECT 'legal_unit_era_history' AS type, COUNT(*) AS count FROM legal_unit_era_history
UNION ALL
SELECT 'establishment_era_history' AS type, COUNT(*) AS count FROM establishment_era_history;


-- Teardown sql_saga constraints
SELECT sql_saga.drop_foreign_key('establishment', ARRAY['legal_unit_id'], 'valid');
SELECT sql_saga.drop_unique_key('establishment', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('establishment', cleanup => true);
SELECT sql_saga.drop_unique_key('legal_unit', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('legal_unit', cleanup => true);

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

-- Teardown for temporal_merge tables
SELECT sql_saga.drop_foreign_key('establishment_tm', ARRAY['legal_unit_id'], 'valid');
SELECT sql_saga.drop_unique_key('establishment_tm', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('establishment_tm', cleanup => true);
SELECT sql_saga.drop_unique_key('legal_unit_tm', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('legal_unit_tm', cleanup => true);

INSERT INTO benchmark (event, row_count) VALUES ('Constraints disabled', 0);

DROP TABLE establishment;
DROP TABLE legal_unit;
DROP TABLE establishment_tm;
DROP TABLE legal_unit_tm;
DROP TABLE establishment_sv;
DROP TABLE legal_unit_sv;
DROP TABLE establishment_era_history;
DROP TABLE legal_unit_era_history;

INSERT INTO benchmark (event, row_count) VALUES ('Tear down complete', 0);

-- Verify the benchmark events and row counts, but exclude volatile timing data
-- from the regression test output to ensure stability.
SELECT event, row_count FROM benchmark ORDER BY timestamp;

-- Capture performance metrics to a separate file for manual review.
-- This output is not part of the main regression test, so timing variations
-- will not cause the test to fail. You should commit this file to git.
\o expected/43_benchmark_performance.out

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
