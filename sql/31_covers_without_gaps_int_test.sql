\i sql/include/test_setup.sql

BEGIN;

SET ROLE TO sql_saga_unprivileged_user;

-- First, create the integer shifts table
CREATE TABLE int_shifts (
  job_id INTEGER,
  worker_id INTEGER,
  valid_from INT,
  valid_until INT
);
-- Add era and unique key via sql_saga
SELECT sql_saga.add_era('int_shifts', 'valid_from', 'valid_until');
SELECT sql_saga.add_unique_key('int_shifts', ARRAY['job_id', 'worker_id']);

TABLE sql_saga.era;
TABLE sql_saga.unique_keys;

-- Insert test data into the integer shifts table
INSERT INTO int_shifts(job_id, worker_id, valid_from, valid_until) VALUES
  (1, 1, 1, 6),
  (1, 2, 6, 12);

-- This test checks for an exact match with one range
-- Expected: TRUE
SELECT sql_saga.covers_without_gaps(int4range(valid_from, valid_until), int4range(1, 6) ORDER BY valid_from)
FROM int_shifts
WHERE job_id = 1;

-- This test checks for an exact match with two consecutive ranges
-- Expected: TRUE
SELECT sql_saga.covers_without_gaps(int4range(valid_from, valid_until), int4range(1, 12) ORDER BY valid_from)
FROM int_shifts
WHERE job_id = 1;

-- Test 3: Range with Extra at the Beginning
-- Expected: TRUE
SELECT sql_saga.covers_without_gaps(int4range(valid_from, valid_until), int4range(2, 6) ORDER BY valid_from)
FROM int_shifts
WHERE job_id = 1;

-- Test 4: Range with Extra at the End
-- Expected: TRUE
SELECT sql_saga.covers_without_gaps(int4range(valid_from, valid_until), int4range(2, 12) ORDER BY valid_from)
FROM int_shifts
WHERE job_id = 1;

-- Test 5: Range with Extra on Both Sides
-- Expected: TRUE
SELECT sql_saga.covers_without_gaps(int4range(valid_from, valid_until), int4range(3, 12) ORDER BY valid_from)
FROM int_shifts
WHERE job_id = 1;

-- Test 6: Range that Misses Completely
-- Expected: FALSE
SELECT sql_saga.covers_without_gaps(int4range(valid_from, valid_until), int4range(21, 26) ORDER BY valid_from)
FROM int_shifts
WHERE job_id = 1;

-- Test 7: Range with Uncovered Time at the Beginning
-- Expected: FALSE
SELECT sql_saga.covers_without_gaps(int4range(valid_from, valid_until), int4range(0, 6) ORDER BY valid_from)
FROM int_shifts
WHERE job_id = 1;

-- Test 8: Range with Uncovered Time at the End
-- Expected: FALSE
SELECT sql_saga.covers_without_gaps(int4range(valid_from, valid_until), int4range(1, 15) ORDER BY valid_from)
FROM int_shifts
WHERE job_id = 1;


SELECT sql_saga.drop_unique_key('int_shifts', 'int_shifts_job_id_worker_id_valid');
SELECT sql_saga.drop_era('int_shifts');

DROP TABLE int_shifts;

ROLLBACK;

\i sql/include/test_teardown.sql
