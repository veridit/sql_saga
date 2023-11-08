-- First, create the integer shifts table
CREATE TABLE numeric_shifts (
  job_id INTEGER,
  worker_id INTEGER,
  valid_from NUMERIC,
  valid_to NUMERIC
);
-- Add era and unique key via sql_saga
SELECT sql_saga.add_era('numeric_shifts', 'valid_from', 'valid_to');
SELECT sql_saga.add_unique_key('numeric_shifts', ARRAY['job_id', 'worker_id']);

TABLE sql_saga.era;
TABLE sql_saga.unique_keys;


-- Insert test data into the integer shifts table
INSERT INTO numeric_shifts(job_id, worker_id, valid_from, valid_to) VALUES
  (1, 1, 1.5, 6.5),
  (1, 2, 6.5, 12.5); 

TABLE numeric_shifts;

-- This test checks for an exact match with one range
-- Expected: TRUE
SELECT sql_saga.no_gaps(numrange(valid_from, valid_to), numrange(1.5, 6.5))
FROM numeric_shifts
WHERE job_id = 1;

-- This test checks for an exact match with two consecutive ranges
-- Expected: TRUE
SELECT sql_saga.no_gaps(numrange(valid_from, valid_to), numrange(1.5, 12.5))
FROM numeric_shifts
WHERE job_id = 1;

-- Test 3: Range with Extra at the Beginning
-- Expected: TRUE
SELECT sql_saga.no_gaps(numrange(valid_from, valid_to), numrange(2.5, 6.5))
FROM numeric_shifts
WHERE job_id = 1;

-- Test 4: Range with Extra at the End
-- Expected: TRUE
SELECT sql_saga.no_gaps(numrange(valid_from, valid_to), numrange(1.5, 11.5))
FROM numeric_shifts
WHERE job_id = 1;

-- Test 5: Range with Extra on Both Sides
-- Expected: TRUE
SELECT sql_saga.no_gaps(numrange(valid_from, valid_to), numrange(2.5, 11.5))
FROM numeric_shifts
WHERE job_id = 1;

-- Test 6: Range that Misses Completely
-- Expected: FALSE
SELECT sql_saga.no_gaps(numrange(valid_from, valid_to), numrange(20.5, 25.5))
FROM numeric_shifts
WHERE job_id = 1;

-- Test 7: Range with Uncovered Time at the Beginning
-- Expected: FALSE
SELECT sql_saga.no_gaps(numrange(valid_from, valid_to), numrange(0.0, 12.0))
FROM numeric_shifts
WHERE job_id = 1;

-- Test 8: Range with Uncovered Time at the End
-- Expected: FALSE
SELECT sql_saga.no_gaps(numrange(valid_from, valid_to), numrange(1.5, 15.5))
FROM numeric_shifts
WHERE job_id = 1;

SELECT sql_saga.drop_unique_key('numeric_shifts', 'numeric_shifts_job_id_worker_id_valid');
SELECT sql_saga.drop_era('numeric_shifts');

DROP TABLE numeric_shifts;