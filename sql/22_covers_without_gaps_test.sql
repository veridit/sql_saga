-- Setup: Create a local table for this test only to ensure isolation.
CREATE TABLE covers_test_shifts (
    job_id      integer,
    worker_id   integer,
    valid_after timestamptz,
    valid_to    timestamptz
);

INSERT INTO covers_test_shifts(job_id, worker_id, valid_after, valid_to) VALUES
  -- job_id=1: contiguous finite ranges: [06:00, 12:00), [12:00, 17:00)
  (1, 1, '2017-11-27 06:00:00', '2017-11-27 12:00:00'),
  (1, 2, '2017-11-27 12:00:00', '2017-11-27 17:00:00'),
  -- job_id=2: finite ranges with a gap: [06:00, 12:00), [13:00, 17:00)
  (2, 3, '2017-11-27 06:00:00', '2017-11-27 12:00:00'),
  (2, 4, '2017-11-27 13:00:00', '2017-11-27 17:00:00'),
  -- job_id=3: lower-unbounded range: (-infinity, 17:00)
  (3, 5, '-infinity',           '2017-11-27 12:00:00'),
  (3, 6, '2017-11-27 12:00:00', '2017-11-27 17:00:00'),
  -- job_id=4: upper-unbounded range: [06:00, infinity)
  (4, 7, '2017-11-27 06:00:00', '2017-11-27 12:00:00'),
  (4, 8, '2017-11-27 12:00:00', 'infinity')
;
-- job_id=5 is intentionally left empty for testing the no-input-rows scenario.

-- =============================================================================
-- TRUE: Cases where ranges should be considered covered
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Sub-group: Finite Ranges
-- -----------------------------------------------------------------------------

-- it covers when the target range is identical to the input ranges
SELECT  sql_saga.covers_without_gaps(tstzrange(valid_after,valid_to), tstzrange('2017-11-27 06:00:00', '2017-11-27 17:00:00') ORDER BY valid_after)
FROM    covers_test_shifts
WHERE   job_id = 1;

-- it covers when the target is a subset of the input ranges (extra coverage on both sides)
SELECT  sql_saga.covers_without_gaps(tstzrange(valid_after,valid_to), tstzrange('2017-11-27 08:00:00', '2017-11-27 14:00:00') ORDER BY valid_after)
FROM    covers_test_shifts
WHERE   job_id = 1;

-- it covers when the target is a subset (extra coverage at the start)
SELECT  sql_saga.covers_without_gaps(tstzrange(valid_after,valid_to), tstzrange('2017-11-27 08:00:00', '2017-11-27 17:00:00') ORDER BY valid_after)
FROM    covers_test_shifts
WHERE   job_id = 1;

-- it covers when the target is a subset (extra coverage at the end)
SELECT  sql_saga.covers_without_gaps(tstzrange(valid_after,valid_to), tstzrange('2017-11-27 06:00:00', '2017-11-27 14:00:00') ORDER BY valid_after)
FROM    covers_test_shifts
WHERE   job_id = 1;

-- -----------------------------------------------------------------------------
-- Sub-group: Infinite Ranges
-- -----------------------------------------------------------------------------

-- it covers when a lower-unbounded input covers a finite target
SELECT  sql_saga.covers_without_gaps(tstzrange(valid_after,valid_to), tstzrange('2017-11-27 06:00:00', '2017-11-27 17:00:00') ORDER BY valid_after)
FROM    covers_test_shifts
WHERE   job_id = 3;

-- it covers when a lower-unbounded input covers a lower-unbounded target
SELECT  sql_saga.covers_without_gaps(tstzrange(valid_after,valid_to), tstzrange('-infinity', '2017-11-27 17:00:00') ORDER BY valid_after)
FROM    covers_test_shifts
WHERE   job_id = 3;

-- it covers when an upper-unbounded input covers a finite target
SELECT  sql_saga.covers_without_gaps(tstzrange(valid_after,valid_to), tstzrange('2017-11-27 06:00:00', '2017-11-27 17:00:00') ORDER BY valid_after)
FROM    covers_test_shifts
WHERE   job_id = 4;

-- it covers when an upper-unbounded input covers an upper-unbounded target
SELECT  sql_saga.covers_without_gaps(tstzrange(valid_after,valid_to), tstzrange('2017-11-27 06:00:00', 'infinity') ORDER BY valid_after)
FROM    covers_test_shifts
WHERE   job_id = 4;

-- it does NOT cover when ranges meet at an exclusive boundary for continuous types
SELECT sql_saga.covers_without_gaps(v, tstzrange('2024-01-01 10:00', '2024-01-01 14:00', '()'))
FROM (
  SELECT v FROM (VALUES
    (tstzrange('2024-01-01 10:00', '2024-01-01 12:00', '()')), -- (10, 12)
    (tstzrange('2024-01-01 12:00', '2024-01-01 14:00', '()'))  -- (12, 14)
  ) AS t(v) ORDER BY v
) AS sorted_v;

-- it covers with [) boundaries (the default)
SELECT sql_saga.covers_without_gaps(v, tstzrange('2024-01-01 10:00', '2024-01-01 14:00', '[)'))
FROM (VALUES
  (tstzrange('2024-01-01 10:00', '2024-01-01 12:00', '[)')), -- [10, 12)
  (tstzrange('2024-01-01 12:00', '2024-01-01 14:00', '[)'))  -- [12, 14)
) AS t(v);

-- it covers with [] boundaries
SELECT sql_saga.covers_without_gaps(v, tstzrange('2024-01-01 10:00', '2024-01-01 14:00', '[]'))
FROM (VALUES
  (tstzrange('2024-01-01 10:00', '2024-01-01 12:00', '[]')), -- [10, 12]
  (tstzrange('2024-01-01 12:00', '2024-01-01 14:00', '[]'))  -- [12, 14] -- overlaps are not gaps
) AS t(v);

-- it covers with (] boundaries
SELECT sql_saga.covers_without_gaps(v, tstzrange('2024-01-01 10:00', '2024-01-01 14:00', '(]'))
FROM (VALUES
  (tstzrange('2024-01-01 10:00', '2024-01-01 12:00', '(]')), -- (10, 12]
  (tstzrange('2024-01-01 12:00', '2024-01-01 14:00', '(]'))  -- (12, 14] -- contiguous
) AS t(v);

-- it covers with tsrange (timezone-naive)
SELECT sql_saga.covers_without_gaps(v, tsrange('2024-01-01 10:00', '2024-01-01 14:00', '[)'))
FROM (VALUES
  (tsrange('2024-01-01 10:00', '2024-01-01 12:00', '[)')), -- [10, 12)
  (tsrange('2024-01-01 12:00', '2024-01-01 14:00', '[)'))  -- [12, 14)
) AS t(v);


-- =============================================================================
-- FALSE: Cases where ranges should NOT be considered covered
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Sub-group: Finite Ranges
-- -----------------------------------------------------------------------------

-- it does not cover when there is a gap at the beginning
SELECT  sql_saga.covers_without_gaps(tstzrange(valid_after,valid_to), tstzrange('2017-11-27 04:00:00', '2017-11-27 14:00:00'))
FROM    covers_test_shifts
WHERE   job_id = 1;

-- it does not cover when there is a gap at the end
SELECT  sql_saga.covers_without_gaps(tstzrange(valid_after,valid_to), tstzrange('2017-11-27 06:00:00', '2017-11-27 20:00:00'))
FROM    covers_test_shifts
WHERE   job_id = 1;

-- it does not cover when there is a gap in the middle
SELECT  sql_saga.covers_without_gaps(tstzrange(valid_after,valid_to), tstzrange('2017-11-27 06:00:00', '2017-11-27 17:00:00'))
FROM    covers_test_shifts
WHERE   job_id = 2;

-- it does not cover when a [) and () meet at the boundary (gap at 12)
SELECT sql_saga.covers_without_gaps(v, tstzrange('2024-01-01 10:00', '2024-01-01 14:00', '[)'))
FROM (
  SELECT v FROM (VALUES
    (tstzrange('2024-01-01 10:00', '2024-01-01 12:00', '[)')), -- [10, 12)
    (tstzrange('2024-01-01 12:00', '2024-01-01 14:00', '()'))  -- (12, 14)
  ) AS t(v) ORDER BY v
) AS sorted_v;

-- it does not cover when a () and [) meet at the boundary (gap at 12)
SELECT sql_saga.covers_without_gaps(v, tstzrange('2024-01-01 10:00', '2024-01-01 14:00', '[)'))
FROM (
  SELECT v FROM (VALUES
    (tstzrange('2024-01-01 10:00', '2024-01-01 12:00', '()')), -- (10, 12)
    (tstzrange('2024-01-01 12:00', '2024-01-01 14:00', '[)'))  -- [12, 14)
  ) AS t(v) ORDER BY v
) AS sorted_v;

-- it does not cover when there is no overlap at all
SELECT  sql_saga.covers_without_gaps(tstzrange(valid_after,valid_to), tstzrange('2017-11-29 08:00:00', '2017-11-29 14:00:00'))
FROM    covers_test_shifts
WHERE   job_id = 1;

-- -----------------------------------------------------------------------------
-- Sub-group: Infinite Ranges
-- -----------------------------------------------------------------------------

-- it does not cover when finite input ranges are tested against a lower-unbounded target
SELECT  sql_saga.covers_without_gaps(tstzrange(valid_after,valid_to), tstzrange('-infinity', '2017-11-27 17:00:00'))
FROM    covers_test_shifts
WHERE   job_id = 1;

-- it does not cover when finite input ranges are tested against an upper-unbounded target
SELECT  sql_saga.covers_without_gaps(tstzrange(valid_after,valid_to), tstzrange('2017-11-27 06:00:00', 'infinity'))
FROM    covers_test_shifts
WHERE   job_id = 1;

-- it does not cover when finite input ranges are tested against a fully-unbounded target
SELECT  sql_saga.covers_without_gaps(tstzrange(valid_after,valid_to), tstzrange('-infinity', 'infinity'))
FROM    covers_test_shifts
WHERE   job_id = 1;

-- it does not cover when a lower-unbounded input has a gap at the end of a finite target
SELECT  sql_saga.covers_without_gaps(tstzrange(valid_after,valid_to), tstzrange('2017-11-27 06:00:00', '2017-11-27 20:00:00'))
FROM    covers_test_shifts
WHERE   job_id = 3;

-- it does not cover when an upper-unbounded input has a gap at the beginning of a finite target
SELECT  sql_saga.covers_without_gaps(tstzrange(valid_after,valid_to), tstzrange('2017-11-27 03:00:00', '2017-11-27 17:00:00'))
FROM    covers_test_shifts
WHERE   job_id = 4;

-- it does not cover when a preceding range hides a gap at the start of the target
SELECT  sql_saga.covers_without_gaps(v, tstzrange('2024-01-10', '2024-01-20'))
FROM    (VALUES (tstzrange('2024-01-01', '2024-01-05')), (tstzrange('2024-01-12', '2024-01-25'))) AS t(v);

-- -----------------------------------------------------------------------------
-- Sub-group: Mixed Boundary Semantics (using discrete ranges for clarity)
-- -----------------------------------------------------------------------------

-- it does not cover when exclusive bounds create a gap for a discrete type
SELECT sql_saga.covers_without_gaps(v, int4range(1, 10, '[]'))
FROM (
  SELECT v FROM (VALUES
    (int4range(1, 5, '[)')), -- covers 1,2,3,4
    (int4range(5, 10, '(]'))  -- covers 6,7,8,9,10. The integer 5 is missing.
  ) AS t(v) ORDER BY v
) AS sorted_v;

-- it covers when mixed bounds are contiguous for a discrete type
SELECT sql_saga.covers_without_gaps(v, int4range(1, 10, '[]'))
FROM (
  SELECT v FROM (VALUES
    (int4range(1, 5, '[]')), -- covers 1,2,3,4,5
    (int4range(5, 10, '(]'))  -- covers 6,7,8,9,10
  ) AS t(v) ORDER BY v
) AS sorted_v;

-- it covers when mixed bounds are contiguous for a discrete type (case 2)
SELECT sql_saga.covers_without_gaps(v, int4range(1, 10, '[]'))
FROM (
  SELECT v FROM (VALUES
    (int4range(1, 5, '[)')), -- covers 1,2,3,4
    (int4range(5, 10, '[]'))  -- covers 5,6,7,8,9,10
  ) AS t(v) ORDER BY v
) AS sorted_v;

-- it does not cover when the target's inclusive bound is not met by an exclusive input bound
SELECT sql_saga.covers_without_gaps(v, int4range(1, 10, '[]'))
FROM (
  SELECT v FROM (VALUES
    (int4range(1, 10, '[)')) -- covers 1..9, but target needs 10
  ) AS t(v) ORDER BY v
) AS sorted_v;


-- =============================================================================
-- NULL handling
-- =============================================================================

-- it returns NULL when the target is NULL
SELECT  sql_saga.covers_without_gaps(tstzrange(valid_after,valid_to), null)
FROM    covers_test_shifts
WHERE   job_id = 1;

-- it returns NULL when there are no input ranges (Postgres behavior for aggregates with FINALFUNC_EXTRA)
SELECT sql_saga.covers_without_gaps(tstzrange(valid_after,valid_to), tstzrange('2017-11-27 06:00:00', '2017-11-27 17:00:00'))
FROM covers_test_shifts
WHERE job_id = 5;

-- it ignores NULL input ranges and still finds coverage
SELECT sql_saga.covers_without_gaps(v, r) FROM (VALUES
  (tstzrange('2024-01-01', '2024-01-10')),
  (NULL),
  (tstzrange('2024-01-10', '2024-01-20'))
) AS t(v), LATERAL (SELECT tstzrange('2024-01-01', '2024-01-20')) AS target(r)
GROUP BY target.r;

-- it returns FALSE if NULLs in the input set prevent full coverage
SELECT sql_saga.covers_without_gaps(v, r) FROM (VALUES
  (tstzrange('2024-01-01', '2024-01-10')),
  (NULL)
) AS t(v), LATERAL (SELECT tstzrange('2024-01-01', '2024-01-20')) AS target(r)
GROUP BY target.r;

-- it handles multiple aggregate groups correctly (to detect memory leaks)
SELECT job_id, sql_saga.covers_without_gaps(tstzrange(valid_after, valid_to), r)
FROM covers_test_shifts,
LATERAL (SELECT 
    CASE job_id
        WHEN 1 THEN tstzrange('2017-11-27 06:00:00', '2017-11-27 17:00:00')
        WHEN 2 THEN tstzrange('2017-11-27 06:00:00', '2017-11-27 17:00:00')
    END) AS target(r)
WHERE job_id IN (1, 2)
GROUP BY job_id, target.r
ORDER BY job_id;

-- =============================================================================
-- Errors: Cases that should raise an error
-- =============================================================================

-- it fails if the input ranges are not sorted by their start value
SELECT  sql_saga.covers_without_gaps(tstzrange(valid_after,valid_to), tstzrange('2017-11-27 13:00:00', '2017-11-27 20:00:00') ORDER BY worker_id DESC)
FROM    covers_test_shifts
WHERE   job_id = 1;

-- Cleanup
DROP TABLE covers_test_shifts;
