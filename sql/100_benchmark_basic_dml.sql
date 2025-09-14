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

\i sql/include/benchmark_setup.sql

-- Record the start of the setup

-- Enable sql_saga constraints
SELECT sql_saga.add_era(table_oid => 'legal_unit', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_era(table_oid => 'establishment', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'legal_unit', column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.add_unique_key(table_oid => 'establishment', column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.add_temporal_foreign_key(
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


-- Teardown sql_saga constraints
SELECT sql_saga.drop_foreign_key('establishment', ARRAY['legal_unit_id'], 'valid');
SELECT sql_saga.drop_unique_key('establishment', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('establishment', cleanup => true);
SELECT sql_saga.drop_unique_key('legal_unit', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('legal_unit', cleanup => true);

INSERT INTO benchmark (event, row_count) VALUES ('Constraints disabled', 0);

DROP TABLE establishment;
DROP TABLE legal_unit;

INSERT INTO benchmark (event, row_count) VALUES ('Tear down complete', 0);

-- Verify the benchmark events and row counts, but exclude volatile timing data
-- from the regression test output to ensure stability.
SELECT event, row_count FROM benchmark ORDER BY seq_id;

-- Capture performance metrics to a separate file for manual review.
\o expected/100_benchmark_basic_dml_performance.out

\i sql/include/benchmark_report.sql

-- Stop redirecting output
\o

\i sql/include/test_teardown.sql
