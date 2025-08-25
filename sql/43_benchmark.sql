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
INSERT INTO benchmark (event, row_count) VALUES ('INSERTs delayed constraints start', 0);
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
INSERT INTO benchmark (event, row_count) VALUES ('INSERTs delayed constraints end', 10000);


SELECT 'legal_unit' AS type, COUNT(*) AS count FROM legal_unit
UNION ALL
SELECT 'establishment' AS type, COUNT(*) AS count FROM establishment;

-- With immediate constraints
INSERT INTO benchmark (event, row_count) VALUES ('INSERTs immediate constraints start', 0);
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
INSERT INTO benchmark (event, row_count) VALUES ('INSERTs immediate constraints end', 10000);

SELECT 'legal_unit' AS type, COUNT(*) AS count FROM legal_unit
UNION ALL
SELECT 'establishment' AS type, COUNT(*) AS count FROM establishment;

-- UPDATE with delayed commit checking
INSERT INTO benchmark (event, row_count) VALUES ('Update deferred constraints start', 0);
BEGIN;
  SET CONSTRAINTS ALL DEFERRED;
  UPDATE legal_unit SET valid_until = '2016-01-01' WHERE id <= 10000 AND valid_from = '2015-01-01';

  INSERT INTO legal_unit (id, valid_from, valid_until, name)
    SELECT id, '2016-01-01', 'infinity', name
    FROM legal_unit WHERE valid_until = '2016-01-01';

  SET CONSTRAINTS ALL IMMEDIATE;
END;
INSERT INTO benchmark (event, row_count) VALUES ('Update deferred constraints end', 20000);

SELECT 'legal_unit' AS type, COUNT(*) AS count FROM legal_unit
UNION ALL
SELECT 'establishment' AS type, COUNT(*) AS count FROM establishment;


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

INSERT INTO benchmark (event, row_count) VALUES ('System Versioning Only Benchmark', 0);

-- With immediate constraints
INSERT INTO benchmark (event, row_count) VALUES ('SV INSERTs immediate constraints start', 0);
BEGIN;
DO $$
BEGIN
  FOR i IN 1..10000 LOOP
    INSERT INTO legal_unit_sv (id, name) VALUES (i, 'Company ' || i);
    INSERT INTO establishment_sv (id, legal_unit_id, postal_place) VALUES (i, i, 'Shop ' || i);
  END LOOP;
END; $$;
END;
INSERT INTO benchmark (event, row_count) VALUES ('SV INSERTs immediate constraints end', 10000);

-- UPDATE
INSERT INTO benchmark (event, row_count) VALUES ('SV Update start', 0);
BEGIN;
  UPDATE legal_unit_sv SET name = 'New ' || name WHERE id <= 10000;
END;
INSERT INTO benchmark (event, row_count) VALUES ('SV Update end', 10000);

SELECT 'legal_unit_sv' AS type, COUNT(*) AS count FROM legal_unit_sv
UNION ALL
SELECT 'establishment_sv' AS type, COUNT(*) AS count FROM establishment_sv;


--
-- Benchmark with Eras AND System Versioning
--

CREATE TABLE legal_unit_combo (
  id INTEGER,
  valid_from date,
  valid_until date,
  name varchar NOT NULL
);

CREATE TABLE establishment_combo (
  id INTEGER,
  valid_from date,
  valid_until date,
  legal_unit_id INTEGER NOT NULL,
  postal_place TEXT NOT NULL
);

-- Enable Eras and System Versioning
SELECT sql_saga.add_era(table_oid => 'legal_unit_combo', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_era(table_oid => 'establishment_combo', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'legal_unit_combo', column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.add_unique_key(table_oid => 'establishment_combo', column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'establishment_combo',
    fk_column_names => ARRAY['legal_unit_id'],
    fk_era_name => 'valid',
    unique_key_name => 'legal_unit_combo_id_valid'
);
SELECT sql_saga.add_system_versioning(table_oid => 'legal_unit_combo');
SELECT sql_saga.add_system_versioning(table_oid => 'establishment_combo');


INSERT INTO benchmark (event, row_count) VALUES ('Eras and SV Benchmark', 0);

-- With immediate constraints
INSERT INTO benchmark (event, row_count) VALUES ('Combo INSERTs immediate constraints start', 0);
BEGIN;
DO $$
BEGIN
  FOR i IN 1..10000 LOOP
    INSERT INTO legal_unit_combo (id, valid_from, valid_until, name) VALUES (i, '2015-01-01', 'infinity', 'Company ' || i);
    INSERT INTO establishment_combo (id, valid_from, valid_until, legal_unit_id, postal_place) VALUES (i, '2015-01-01', 'infinity', i, 'Shop ' || i);
  END LOOP;
END; $$;
END;
INSERT INTO benchmark (event, row_count) VALUES ('Combo INSERTs immediate constraints end', 10000);

-- UPDATE with delayed commit checking
INSERT INTO benchmark (event, row_count) VALUES ('Combo Update deferred constraints start', 0);
BEGIN;
  SET CONSTRAINTS ALL DEFERRED;
  UPDATE legal_unit_combo SET valid_until = '2016-01-01' WHERE id <= 10000 AND valid_from = '2015-01-01';
  INSERT INTO legal_unit_combo (id, valid_from, valid_until, name) SELECT id, '2016-01-01', 'infinity', name FROM legal_unit_combo WHERE valid_until = '2016-01-01';
  SET CONSTRAINTS ALL IMMEDIATE;
END;
INSERT INTO benchmark (event, row_count) VALUES ('Combo Update deferred constraints end', 20000);

SELECT 'legal_unit_combo' AS type, COUNT(*) AS count FROM legal_unit_combo
UNION ALL
SELECT 'establishment_combo' AS type, COUNT(*) AS count FROM establishment_combo;


-- Teardown sql_saga constraints
SELECT sql_saga.drop_foreign_key('establishment', ARRAY['legal_unit_id'], 'valid');
SELECT sql_saga.drop_unique_key('establishment', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('establishment', cleanup => true);
SELECT sql_saga.drop_unique_key('legal_unit', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('legal_unit', cleanup => true);

-- Teardown for SV tables
SELECT sql_saga.drop_system_versioning('establishment_sv', cleanup => true);
SELECT sql_saga.drop_system_versioning('legal_unit_sv', cleanup => true);

-- Teardown for combo tables
SELECT sql_saga.drop_foreign_key('establishment_combo', ARRAY['legal_unit_id'], 'valid');
SELECT sql_saga.drop_unique_key('establishment_combo', ARRAY['id'], 'valid');
SELECT sql_saga.drop_system_versioning('establishment_combo', cleanup => true);
SELECT sql_saga.drop_era('establishment_combo', cleanup => true);

SELECT sql_saga.drop_unique_key('legal_unit_combo', ARRAY['id'], 'valid');
SELECT sql_saga.drop_system_versioning('legal_unit_combo', cleanup => true);
SELECT sql_saga.drop_era('legal_unit_combo', cleanup => true);

INSERT INTO benchmark (event, row_count) VALUES ('Constraints disabled', 0);

DROP TABLE establishment;
DROP TABLE legal_unit;
DROP TABLE establishment_sv;
DROP TABLE legal_unit_sv;
DROP TABLE establishment_combo;
DROP TABLE legal_unit_combo;

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

WITH benchmark_calculated AS (
  SELECT
    timestamp,
    event,
    row_count,
    ROUND(EXTRACT(EPOCH FROM (timestamp - FIRST_VALUE(timestamp) OVER (ORDER BY timestamp)))::NUMERIC, 0) AS time_from_start,
    ROUND(EXTRACT(EPOCH FROM (timestamp - LAG(timestamp) OVER (ORDER BY timestamp)))::NUMERIC, 0) AS time_from_prev,
    CASE
      WHEN EXTRACT(EPOCH FROM (timestamp - LAG(timestamp) OVER (ORDER BY timestamp))) = 0
      THEN NULL
      ELSE round_to_nearest_100(row_count::FLOAT8 / EXTRACT(EPOCH FROM (timestamp - LAG(timestamp) OVER (ORDER BY timestamp))))
    END AS rows_per_second
  FROM
    benchmark
)
SELECT
  event,
  row_count,
  time_from_start || ' secs' AS time_from_start,
  COALESCE(time_from_prev || ' secs', '') AS time_from_prev,
  row_count || ' rows' AS row_count,
  '~' || COALESCE(rows_per_second, 0)::TEXT || ' rows/s' AS rows_per_second
FROM
  benchmark_calculated
ORDER BY
  timestamp;

-- Stop redirecting output
\o

\i sql/include/test_teardown.sql
