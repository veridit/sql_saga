\i sql/include/test_setup.sql

BEGIN;

SET ROLE TO sql_saga_unprivileged_user;

CREATE TABLE date_shifts (
  job_id INTEGER,
  worker_id INTEGER,
  valid_from DATE,
  valid_until DATE
);
-- Add era and unique key via sql_saga
SELECT sql_saga.add_era('date_shifts', 'valid_from', 'valid_until', p_add_bounds_check := false);
SELECT sql_saga.add_unique_key('date_shifts', ARRAY['job_id', 'worker_id']);

TABLE sql_saga.era;
TABLE sql_saga.unique_keys;


-- Insert test data into the integer date_shifts table
INSERT INTO date_shifts(job_id, worker_id, valid_from, valid_until) VALUES
  (1, 1, '2017-11-23', '2017-11-25'),
  (1, 2, '2017-11-25', '2017-11-29'),
  (2, 3, '2017-11-23', '2017-11-25'),
  (2, 4, '2017-11-26', '2017-11-29'),
  (3, 5, '-infinity',  '2017-11-25'),
  (3, 5, '2017-11-25', '2017-11-29'),
  (4, 6, '2017-11-23', '2017-11-25'),
  (4, 7, '2017-11-25', 'infinity')
;

-- TRUE:

-- it covers when the range matches one exactly:
SELECT  sql_saga.covers_without_gaps(daterange(valid_from,valid_until), daterange('2017-11-23', '2017-11-25') ORDER BY valid_from)
FROM    date_shifts
WHERE   job_id = 1;

-- it covers when the range matches two exactly:
SELECT  sql_saga.covers_without_gaps(daterange(valid_from,valid_until), daterange('2017-11-23', '2017-11-29') ORDER BY valid_from)
FROM    date_shifts
WHERE   job_id = 1;

-- it covers when the range has extra in front:
SELECT  sql_saga.covers_without_gaps(daterange(valid_from,valid_until), daterange('2017-11-24', '2017-11-29') ORDER BY valid_from)
FROM    date_shifts
WHERE   job_id = 1;

-- it covers when the range has extra behind:
SELECT  sql_saga.covers_without_gaps(daterange(valid_from,valid_until), daterange('2017-11-23', '2017-11-27') ORDER BY valid_from)
FROM    date_shifts
WHERE   job_id = 1;

-- it covers when the range has extra on both sides:
SELECT  sql_saga.covers_without_gaps(daterange(valid_from,valid_until), daterange('2017-11-24', '2017-11-27') ORDER BY valid_from)
FROM    date_shifts
WHERE   job_id = 1;

-- an infinite start will cover a finite target:
SELECT  sql_saga.covers_without_gaps(daterange(valid_from,valid_until), daterange('2017-11-23', '2017-11-29') ORDER BY valid_from)
FROM    date_shifts
WHERE   job_id = 3;

-- an infinite start will cover an infinite target:
SELECT  sql_saga.covers_without_gaps(daterange(valid_from,valid_until), daterange('-infinity', '2017-11-29') ORDER BY valid_from)
FROM    date_shifts
WHERE   job_id = 3;

-- an infinite end will cover a finite target:
SELECT  sql_saga.covers_without_gaps(daterange(valid_from,valid_until), daterange('2017-11-23', '2017-11-29') ORDER BY valid_from)
FROM    date_shifts
WHERE   job_id = 4;

-- an infinite end will cover an infinite target:
SELECT  sql_saga.covers_without_gaps(daterange(valid_from,valid_until), daterange('2017-11-23', 'infinity') ORDER BY valid_from)
FROM    date_shifts
WHERE   job_id = 4;

-- FALSE:

-- it does not cover when the range is null:
SELECT  sql_saga.covers_without_gaps(NULL, daterange('2017-11-23', '2017-11-29'))
FROM    date_shifts
WHERE   job_id = 1;

-- it does not cover when the range misses completely:
SELECT  sql_saga.covers_without_gaps(daterange(valid_from,valid_until), daterange('2017-11-30', '2017-12-01'))
FROM    date_shifts
WHERE   job_id = 1;

-- it does not cover when the range has something at the beginning:
SELECT  sql_saga.covers_without_gaps(daterange(valid_from,valid_until), daterange('2017-11-22', '2017-11-29'))
FROM    date_shifts
WHERE   job_id = 1;

-- it does not cover when the range has something at the end:
SELECT  sql_saga.covers_without_gaps(daterange(valid_from,valid_until), daterange('2017-11-23', '2017-11-30'))
FROM    date_shifts
WHERE   job_id = 1;

-- it does not cover when the range has something in the middle:
SELECT  sql_saga.covers_without_gaps(daterange(valid_from,valid_until), daterange('2017-11-23', '2017-11-29'))
FROM    date_shifts
WHERE   job_id = 2;

-- it does not cover when the range is lower-unbounded:
SELECT  sql_saga.covers_without_gaps(daterange(valid_from,valid_until), daterange('-infinity', '2017-11-29'))
FROM    date_shifts
WHERE   job_id = 1;

-- it does not cover when the range is upper-unbounded:
SELECT  sql_saga.covers_without_gaps(daterange(valid_from,valid_until), daterange('2017-11-23', 'infinity'))
FROM    date_shifts
WHERE   job_id = 1;

-- it does not cover when the range is both-sides-unbounded:
SELECT  sql_saga.covers_without_gaps(daterange(valid_from,valid_until), daterange('-infinity', 'infinity'))
FROM    date_shifts
WHERE   job_id = 1;

-- an infinite start will not cover a finite target if there is uncovered time at the end:
SELECT  sql_saga.covers_without_gaps(daterange(valid_from,valid_until), daterange('2017-11-23', '2017-11-30'))
FROM    date_shifts
WHERE   job_id = 3;

-- an infinite start will not cover an infinite target if there is uncovered time at the end:
SELECT  sql_saga.covers_without_gaps(daterange(valid_from,valid_until), daterange('-infinity', '2017-11-30'))
FROM    date_shifts
WHERE   job_id = 3;

-- an infinite end will not cover a finite target if there is uncovered time at the beginning:
SELECT  sql_saga.covers_without_gaps(daterange(valid_from,valid_until), daterange('2017-11-22', '2017-11-29'))
FROM    date_shifts
WHERE   job_id = 4;

-- an infinite end will not cover an infinite target if there is uncovered time at the beginning:
SELECT  sql_saga.covers_without_gaps(daterange(valid_from,valid_until), daterange('2017-11-22', 'infinity'))
FROM    date_shifts
WHERE   job_id = 4;


-- NULL:

-- it is unknown when the target is null:
SELECT  sql_saga.covers_without_gaps(daterange(valid_from,valid_until), null)
FROM    date_shifts
WHERE   job_id = 1;

-- Errors:

-- it fails if the input ranges go backwards:
SELECT  sql_saga.covers_without_gaps(daterange(valid_from,valid_until) , daterange('2017-11-26', '2017-11-28') ORDER BY worker_id DESC)
FROM    date_shifts
WHERE   job_id = 1;

-- TODO: handle an empty target range? e.g. [5, 5)
-- Or maybe since that is a self-contradiction maybe ignore that case?

DELETE FROM date_shifts;

SELECT sql_saga.drop_unique_key('date_shifts', ARRAY['job_id', 'worker_id'], 'valid');
SELECT sql_saga.drop_era('date_shifts');

DROP TABLE date_shifts;

ROLLBACK;

\i sql/include/test_teardown.sql
