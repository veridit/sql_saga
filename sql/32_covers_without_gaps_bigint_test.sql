-- First, create the integer shifts table
CREATE TABLE bigint_shifts (
  job_id INTEGER,
  worker_id INTEGER,
  valid_from BIGINT,
  valid_until BIGINT
);
-- Add era and unique key via sql_saga
SELECT sql_saga.add_era('bigint_shifts', 'valid_from', 'valid_until');
SELECT sql_saga.add_unique_key('bigint_shifts', ARRAY['job_id', 'worker_id']);

TABLE sql_saga.era;
TABLE sql_saga.unique_keys;


INSERT INTO bigint_shifts(job_id, worker_id, valid_from, valid_until) VALUES
-- Insert test data into the integer shifts table
  (1, 1, 1000000000, 6000000001),
  (1, 2, 6000000001, 12000000001); 

-- This test checks for an exact match with one range
-- Expected: TRUE
SELECT sql_saga.covers_without_gaps(int8range(valid_from, valid_until), int8range(1000000000, 6000000001) ORDER BY valid_from)
FROM bigint_shifts
WHERE job_id = 1;

-- This test checks for an exact match with two consecutive ranges
-- Expected: TRUE
SELECT sql_saga.covers_without_gaps(int8range(valid_from, valid_until), int8range(1000000000, 12000000001) ORDER BY valid_from)
FROM bigint_shifts
WHERE job_id = 1;

-- Test 3: Range with Extra at the Beginning
-- Expected: TRUE
SELECT sql_saga.covers_without_gaps(int8range(valid_from, valid_until), int8range(2000000000, 6000000001) ORDER BY valid_from)
FROM bigint_shifts
WHERE job_id = 1;

-- Test 4: Range with Extra at the End
-- Expected: TRUE
SELECT sql_saga.covers_without_gaps(int8range(valid_from, valid_until), int8range(1000000000, 11000000001) ORDER BY valid_from)
FROM bigint_shifts
WHERE job_id = 1;

-- Test 5: Range with Extra on Both Sides
-- Expected: TRUE
SELECT sql_saga.covers_without_gaps(int8range(valid_from, valid_until), int8range(2000000000, 11000000001) ORDER BY valid_from)
FROM bigint_shifts
WHERE job_id = 1;

-- Test 6: Range that Misses Completely
-- Expected: FALSE
SELECT sql_saga.covers_without_gaps(int8range(valid_from, valid_until), int8range(20000000000, 25000000001) ORDER BY valid_from)
FROM bigint_shifts
WHERE job_id = 1;

-- Test 7: Range with Uncovered Time at the Beginning
-- Expected: FALSE
SELECT sql_saga.covers_without_gaps(int8range(valid_from, valid_until), int8range(1, 12000000001) ORDER BY valid_from)
FROM bigint_shifts
WHERE job_id = 1;

-- Test 8: Range with Uncovered Time at the End
-- Expected: FALSE
SELECT sql_saga.covers_without_gaps(int8range(valid_from, valid_until), int8range(1000000000, 15000000001) ORDER BY valid_from)
FROM bigint_shifts
WHERE job_id = 1;


SELECT sql_saga.drop_unique_key('bigint_shifts', 'bigint_shifts_job_id_worker_id_valid');
SELECT sql_saga.drop_era('bigint_shifts');

DROP TABLE bigint_shifts;
