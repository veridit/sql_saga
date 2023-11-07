CREATE TABLE timestamp_shifts (
  job_id INTEGER,
  worker_id INTEGER,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP
);
-- Add era and unique key via sql_saga
SELECT sql_saga.add_era('timestamp_shifts', 'valid_from', 'valid_to');
SELECT sql_saga.add_unique_key('timestamp_shifts', ARRAY['job_id', 'worker_id']);

TABLE sql_saga.era;
TABLE sql_saga.unique_keys;

INSERT INTO timestamp_shifts(job_id, worker_id, valid_from, valid_to) VALUES
  (1, 1, '2017-11-27 06:00:00', '2017-11-27 12:00:00'),
  (1, 2, '2017-11-27 12:00:00', '2017-11-27 17:00:00'),
  (2, 3, '2017-11-27 06:00:00', '2017-11-27 12:00:00'),
  (2, 4, '2017-11-27 13:00:00', '2017-11-27 17:00:00'),
  (3, 5, '-infinity',           '2017-11-27 12:00:00'),
  (3, 5, '2017-11-27 12:00:00', '2017-11-27 17:00:00'),
  (4, 6, '2017-11-27 06:00:00', '2017-11-27 12:00:00'),
  (4, 7, '2017-11-27 12:00:00', 'infinity')
;

-- TRUE:

-- it covers when the range matches one exactly:
SELECT  sql_saga.no_gaps(tstzrange(valid_from,valid_to), tstzrange('2017-11-27 06:00:00', '2017-11-27 12:00:00'))
FROM    timestamp_shifts
WHERE   job_id = 1;

-- it covers when the range matches two exactly:
SELECT  sql_saga.no_gaps(tstzrange(valid_from,valid_to), tstzrange('2017-11-27 06:00:00', '2017-11-27 17:00:00'))
FROM    timestamp_shifts
WHERE   job_id = 1;

-- it covers when the range has extra in front:
SELECT  sql_saga.no_gaps(tstzrange(valid_from,valid_to), tstzrange('2017-11-27 08:00:00', '2017-11-27 17:00:00'))
FROM    timestamp_shifts
WHERE   job_id = 1;

-- it covers when the range has extra behind:
SELECT  sql_saga.no_gaps(tstzrange(valid_from,valid_to), tstzrange('2017-11-27 06:00:00', '2017-11-27 14:00:00'))
FROM    timestamp_shifts
WHERE   job_id = 1;

-- it covers when the range has extra on both sides:
SELECT  sql_saga.no_gaps(tstzrange(valid_from,valid_to), tstzrange('2017-11-27 08:00:00', '2017-11-27 14:00:00'))
FROM    timestamp_shifts
WHERE   job_id = 1;

-- an infinite start will cover a finite target:
SELECT  sql_saga.no_gaps(tstzrange(valid_from,valid_to), tstzrange('2017-11-27 06:00:00', '2017-11-27 17:00:00'))
FROM    timestamp_shifts
WHERE   job_id = 3;

-- an infinite start will cover an infinite target:
SELECT  sql_saga.no_gaps(tstzrange(valid_from,valid_to), tstzrange('-infinity', '2017-11-27 17:00:00'))
FROM    timestamp_shifts
WHERE   job_id = 3;

-- an infinite end will cover a finite target:
SELECT  sql_saga.no_gaps(tstzrange(valid_from,valid_to), tstzrange('2017-11-27 06:00:00', '2017-11-27 17:00:00'))
FROM    timestamp_shifts
WHERE   job_id = 4;

-- an infinite end will cover an infinite target:
SELECT  sql_saga.no_gaps(tstzrange(valid_from,valid_to), tstzrange('2017-11-27 06:00:00', 'infinity'))
FROM    timestamp_shifts
WHERE   job_id = 4;

-- FALSE:

-- it does not cover when the range is null:
SELECT  sql_saga.no_gaps(NULL, tstzrange('2017-11-27 08:00:00', '2017-11-27 14:00:00'))
FROM    timestamp_shifts
WHERE   job_id = 1;

-- it does not cover when the range misses completely:
SELECT  sql_saga.no_gaps(tstzrange(valid_from,valid_to), tstzrange('2017-11-29 08:00:00', '2017-11-29 14:00:00'))
FROM    timestamp_shifts
WHERE   job_id = 1;

-- it does not cover when the range has something at the beginning:
SELECT  sql_saga.no_gaps(tstzrange(valid_from,valid_to), tstzrange('2017-11-27 04:00:00', '2017-11-27 14:00:00'))
FROM    timestamp_shifts
WHERE   job_id = 1;

-- it does not cover when the range has something at the end:
SELECT  sql_saga.no_gaps(tstzrange(valid_from,valid_to), tstzrange('2017-11-27 06:00:00', '2017-11-27 20:00:00'))
FROM    timestamp_shifts
WHERE   job_id = 1;

-- it does not cover when the range has something in the middle:
SELECT  sql_saga.no_gaps(tstzrange(valid_from,valid_to), tstzrange('2017-11-27 06:00:00', '2017-11-27 17:00:00'))
FROM    timestamp_shifts
WHERE   job_id = 2;

-- it does not cover when the range is lower-unbounded:
SELECT  sql_saga.no_gaps(tstzrange(valid_from,valid_to), tstzrange('-infinity', '2017-11-27 17:00:00'))
FROM    timestamp_shifts
WHERE   job_id = 1;

-- it does not cover when the range is upper-unbounded:
SELECT  sql_saga.no_gaps(tstzrange(valid_from,valid_to), tstzrange('2017-11-27 06:00:00', 'infinity'))
FROM    timestamp_shifts
WHERE   job_id = 1;

-- it does not cover when the range is both-sides-unbounded:
SELECT  sql_saga.no_gaps(tstzrange(valid_from,valid_to), tstzrange('-infinity', 'infinity'))
FROM    timestamp_shifts
WHERE   job_id = 1;

-- an infinite start will not cover a finite target if there is uncovered time at the end:
SELECT  sql_saga.no_gaps(tstzrange(valid_from,valid_to), tstzrange('2017-11-27 06:00:00', '2017-11-27 20:00:00'))
FROM    timestamp_shifts
WHERE   job_id = 3;

-- an infinite start will not cover an infinite target if there is uncovered time at the end:
SELECT  sql_saga.no_gaps(tstzrange(valid_from,valid_to), tstzrange('-infinity', '2017-11-27 20:00:00'))
FROM    timestamp_shifts
WHERE   job_id = 3;

-- an infinite end will not cover a finite target if there is uncovered time at the beginning:
SELECT  sql_saga.no_gaps(tstzrange(valid_from,valid_to), tstzrange('2017-11-27 03:00:00', '2017-11-27 17:00:00'))
FROM    timestamp_shifts
WHERE   job_id = 4;

-- an infinite end will not cover an infinite target if there is uncovered time at the beginning:
SELECT  sql_saga.no_gaps(tstzrange(valid_from,valid_to), tstzrange('2017-11-27 03:00:00', 'infinity'))
FROM    timestamp_shifts
WHERE   job_id = 4;


-- NULL:

-- it is unknown when the target is null:
SELECT  sql_saga.no_gaps(tstzrange(valid_from,valid_to), null)
FROM    timestamp_shifts
WHERE   job_id = 1;

-- Errors:

-- it fails if the input ranges go backwards:
SELECT  sql_saga.no_gaps(tstzrange(valid_from,valid_to), tstzrange('2017-11-27 13:00:00', '2017-11-27 20:00:00') ORDER BY worker_id DESC)
FROM    timestamp_shifts
WHERE   job_id = 1;

-- TODO: handle an empty target range? e.g. [5, 5)
-- Or maybe since that is a self-contradiction maybe ignore that case?

DELETE FROM timestamp_shifts;
