CREATE EXTENSION IF NOT EXISTS sql_saga CASCADE;

CREATE TABLE legal_unit (
  id INTEGER,
  valid_after date GENERATED ALWAYS AS (valid_from - INTERVAL '1 day') STORED,
  valid_from date,
  valid_to date,
  name varchar NOT NULL
);

CREATE TABLE establishment (
  id INTEGER,
  valid_after date GENERATED ALWAYS AS (valid_from - INTERVAL '1 day') STORED,
  valid_from date,
  valid_to date,
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
SELECT sql_saga.add_era('legal_unit', 'valid_after', 'valid_to');
SELECT sql_saga.add_era('establishment', 'valid_after', 'valid_to');
SELECT sql_saga.add_unique_key('legal_unit', ARRAY['id'], 'valid');
SELECT sql_saga.add_unique_key('establishment', ARRAY['id'], 'valid');
SELECT sql_saga.add_foreign_key('establishment', ARRAY['legal_unit_id'], 'valid', 'legal_unit_id_valid');

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
    INSERT INTO legal_unit (id, valid_from, valid_to, name) VALUES
    (i, '2015-01-01', 'infinity', 'Company ' || i);

    INSERT INTO establishment (id, valid_from, valid_to, legal_unit_id, postal_place) VALUES
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
    INSERT INTO legal_unit (id, valid_from, valid_to, name) VALUES
    (i, '2015-01-01', 'infinity', 'Company ' || i);

    INSERT INTO establishment (id, valid_from, valid_to, legal_unit_id, postal_place) VALUES
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
  UPDATE legal_unit SET valid_to = '2015-12-31' WHERE id <= 10000 AND valid_from = '2015-01-01';

  INSERT INTO legal_unit (id, valid_from, valid_to, name)
    SELECT id, '2016-01-01', 'infinity', name
    FROM legal_unit WHERE valid_to = '2015-12-31';

  SET CONSTRAINTS ALL IMMEDIATE;
END;
INSERT INTO benchmark (event, row_count) VALUES ('Update deferred constraints end', 20000);

SELECT 'legal_unit' AS type, COUNT(*) AS count FROM legal_unit
UNION ALL
SELECT 'establishment' AS type, COUNT(*) AS count FROM establishment;

-- Teardown sql_saga constraints
SELECT sql_saga.drop_foreign_key('establishment', 'establishment_legal_unit_id_valid');
SELECT sql_saga.drop_unique_key('legal_unit', 'legal_unit_id_valid');
SELECT sql_saga.drop_unique_key('establishment','establishment_id_valid');
SELECT sql_saga.drop_era('legal_unit');
SELECT sql_saga.drop_era('establishment');

INSERT INTO benchmark (event, row_count) VALUES ('Constraints disabled', 0);

DROP TABLE establishment;
DROP TABLE legal_unit;

DROP EXTENSION sql_saga;
DROP EXTENSION btree_gist;

INSERT INTO benchmark (event, row_count) VALUES ('Tear down complete', 0);

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
