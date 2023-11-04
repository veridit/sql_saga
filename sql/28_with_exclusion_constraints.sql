-- MOVING THE TIME OF A CHANGE

SELECT enable_sql_saga_for_shifts_houses_and_rooms();

-- 1. Small shift to a later time

--
--
-- 1.1. Small shift to a later time, moving both ranges at once:
--
--

DELETE FROM rooms;
DELETE FROM houses;

BEGIN;
INSERT INTO houses VALUES
  (1, 150000, '2015-01-01', '2016-01-01'),
  (1, 200000, '2016-01-01', '2017-01-01')
;
INSERT INTO rooms VALUES
  (1, 1, '2015-01-01', '2017-01-01')
;
--SET CONSTRAINTS ALL DEFERRED;
WITH changed AS (
    SELECT id, '2015-01-01'::TIMESTAMPTZ AS valid_from, '2016-06-01'::TIMESTAMPTZ AS valid_to FROM houses WHERE valid_from = '2015-01-01' AND id = 1
    UNION ALL
    SELECT id, '2016-06-01', '2017-01-01' FROM houses WHERE valid_from = '2016-01-01' AND id = 1
)
UPDATE houses
SET valid_from = changed.valid_from
  , valid_to = changed.valid_to
FROM changed
WHERE houses.id = changed.id
;
--SET CONSTRAINTS ALL IMMEDIATE;
END;
--
--
-- 1.2. Small shift to a later time, moving the earlier range first:
--
--

DELETE FROM rooms;
DELETE FROM houses;

INSERT INTO houses VALUES 
  (1, 150000, '2015-01-01', '2016-01-01'),
  (1, 200000, '2016-01-01', '2017-01-01')
;

INSERT INTO rooms VALUES
  (1, 1,'2015-01-01', '2017-01-01')
;

--
-- 1.2.1. You can't move the time in two transactions.
--

UPDATE  houses
SET     (valid_from, valid_to) = ('2015-01-01', '2016-06-01')
WHERE   id = 1 AND valid_from = '2015-01-01'
;

UPDATE  houses
SET     (valid_from, valid_to) = ('2016-06-01', '2017-01-01')
WHERE   id = 1 AND valid_from = '2016-01-01'
;

--
-- 1.2.2. When the exclusion constraint is checked immediately,
--        you can't move the time in one transaction with two statements.
--

BEGIN;
SET CONSTRAINTS houses_id_tstzrange_excl IMMEDIATE;
UPDATE  houses
SET     (valid_from, valid_to) = ('2015-01-01', '2016-06-01')
WHERE   id = 1 AND valid_from = '2015-01-01'
;

UPDATE  houses
SET     (valid_from, valid_to) = ('2016-06-01', '2017-01-01')
WHERE   id = 1 AND valid_from = '2016-01-01'
;
COMMIT;

--
-- 1.2.3. When the exclusion constraint is checked deferred,
--        you can move the time in one transaction with two statements.
--

BEGIN;
\d houses
SET CONSTRAINTS houses_id_tstzrange_excl DEFERRED;
UPDATE  houses
SET     (valid_from, valid_to) = ('2015-01-01', '2016-06-01')
WHERE   id = 1 AND valid_from = '2015-01-01'
;

UPDATE  houses
SET     (valid_from, valid_to) = ('2016-06-01', '2017-01-01')
WHERE   id = 1 AND valid_from = '2016-01-01'
;
COMMIT;

--
--
-- 1.3. Small shift to a later time, moving the later range first:
--
--

DELETE FROM rooms;
DELETE FROM houses;

INSERT INTO houses VALUES
  (1, 150000, '2015-01-01', '2016-01-01'),
  (1, 200000, '2016-01-01', '2017-01-01')
;

INSERT INTO rooms VALUES
  (1, 1, '2015-01-01', '2017-01-01')
;

--
-- 1.3.1. You can't move the time in two transactions.
--

UPDATE  houses
SET     (valid_from, valid_to) = ('2016-06-01', '2017-01-01')
WHERE   id = 1 AND valid_from = '2016-01-01'
;

UPDATE  houses
SET     (valid_from, valid_to) = ('2015-01-01', '2016-06-01')
WHERE   id = 1 AND valid_from = '2015-01-01'
;

--
-- 1.3.2. When the exclusion constraint is checked immediately,
--        you can move the time in one transaction with two statements.
--

BEGIN;
SET CONSTRAINTS houses_id_tstzrange_excl IMMEDIATE;
UPDATE  houses
SET     (valid_from, valid_to) = ('2016-06-01', '2017-01-01')
WHERE   id = 1 AND valid_from = '2016-01-01'
;

UPDATE  houses
SET     (valid_from, valid_to) = ('2015-01-01', '2016-06-01')
WHERE   id = 1 AND valid_from = '2015-01-01'
;
COMMIT;

--
-- 1.3.3. When the exclusion constraint is checked deferred,
--        you can move the time in one transaction with two statements.
--

BEGIN;
SET CONSTRAINTS houses_id_tstzrange_excl DEFERRED;
UPDATE  houses
SET     (valid_from, valid_to) = ('2016-09-01', '2017-01-01')
WHERE   id = 1 AND valid_from = '2016-06-01'
;

UPDATE  houses
SET     (valid_from, valid_to) = ('2015-01-01', '2016-09-01')
WHERE   id = 1 AND valid_from = '2015-01-01'
;
COMMIT;

-- 2. Small shift to an earlier time
-- 2.1 Small shift to an earlier time, moving both ranges at once:
DELETE FROM rooms;
DELETE FROM houses;

INSERT INTO houses VALUES
  (1, 150000, '2015-01-01', '2016-01-01'),
  (1, 200000, '2016-01-01', '2017-01-01')
;

INSERT INTO rooms VALUES
  (1, 1, '2015-01-01', '2017-01-01')
;

UPDATE  houses
SET     (valid_from, valid_to) =
          CASE
          WHEN valid_from = '2015-01-01' THEN ('2015-01-01', '2015-06-01')
          WHEN valid_from = '2016-01-01' THEN ('2015-06-01', '2017-01-01')
          ELSE NULL -- Can't RAISE here but NULL will cause it to fail.
          END
WHERE   id = 1
;

-- 2.2 Small shift to an earlier time, moving the earlier range first:
DELETE FROM rooms;
DELETE FROM houses;

INSERT INTO houses VALUES
  (1, 150000, '2015-01-01', '2016-01-01'),
  (1, 200000, '2016-01-01', '2017-01-01')
;

INSERT INTO rooms VALUES
  (1, 1, '2015-01-01', '2017-01-01')
;

--
-- 2.2.1. You can't move the time in two transactions.
--

UPDATE  houses
SET     (valid_from, valid_to) = ('2015-01-01', '2015-06-01')
WHERE   id = 1 AND valid_from = '2015-01-01'
;

UPDATE  houses
SET     (valid_from, valid_to) = ('2015-06-01', '2017-01-01')
WHERE   id = 1 AND valid_from = '2016-01-01'
;

--
-- 2.2.2. When the exclusion constraint is checked immediately,
--        you can move the time in one transaction with two statements.
--

BEGIN;
SET CONSTRAINTS houses_id_tstzrange_excl IMMEDIATE;
UPDATE  houses
SET     (valid_from, valid_to) = ('2015-01-01', '2015-06-01')
WHERE   id = 1 AND valid_from = '2015-01-01'
;

UPDATE  houses
SET     (valid_from, valid_to) = ('2015-06-01', '2017-01-01')
WHERE   id = 1 AND valid_from = '2016-01-01'
;
COMMIT;

--
-- 2.2.3. When the exclusion constraint is checked deferred,
--        you can move the time in one transaction with two statements.
--

BEGIN;
SET CONSTRAINTS houses_id_tstzrange_excl DEFERRED;
UPDATE  houses
SET     (valid_from, valid_to) = ('2015-01-01', '2015-03-01')
WHERE   id = 1 AND valid_from = '2015-01-01'
;

UPDATE  houses
SET     (valid_from, valid_to) = ('2015-03-01', '2017-01-01')
WHERE   id = 1 AND valid_from = '2015-06-01'
;
COMMIT;

-- 2.3 Small shift to an earlier time, moving the later range first:
DELETE FROM rooms;
DELETE FROM houses;

INSERT INTO houses VALUES
  (1, 150000, '2015-01-01', '2016-01-01'),
  (1, 200000, '2016-01-01', '2017-01-01')
;

INSERT INTO rooms VALUES
  (1, 1, '2015-01-01', '2017-01-01')
;

--
-- 2.3.1. You can't move the time in two transactions.
--

UPDATE  houses
SET     (valid_from, valid_to) = ('2015-06-01', '2017-01-01')
WHERE   id = 1 AND valid_from = '2016-01-01'
;

UPDATE  houses
SET     (valid_from, valid_to) = ('2015-01-01', '2015-06-01')
WHERE   id = 1 AND valid_from = '2015-01-01'
;

--
-- 2.3.2. When the exclusion constraint is checked immediately,
--        you can't move the time in one transaction with two statements.
--

BEGIN;
SET CONSTRAINTS houses_id_tstzrange_excl IMMEDIATE;
UPDATE  houses
SET     (valid_from, valid_to) = ('2015-06-01', '2017-01-01')
WHERE   id = 1 AND valid_from = '2016-01-01'
;

UPDATE  houses
SET     (valid_from, valid_to) = ('2015-01-01', '2015-06-01')
WHERE   id = 1 AND valid_from = '2015-01-01'
;
COMMIT;

--
-- 2.3.3. When the exclusion constraint is checked deferred,
--        you can move the time in one transaction with two statements.
--

BEGIN;
SET CONSTRAINTS houses_id_tstzrange_excl DEFERRED;
UPDATE  houses
SET     (valid_from, valid_to) = ('2015-06-01', '2017-01-01')
WHERE   id = 1 AND valid_from = '2016-01-01'
;

UPDATE  houses
SET     (valid_from, valid_to) = ('2015-01-01', '2015-06-01')
WHERE   id = 1 AND valid_from = '2015-01-01'
;
COMMIT;

-- 3. Large shift to a later time (all the way past the later range)
-- 3.1. Large shift to a later time (all the way past the later range), earlier first:
-- TODO
-- 3.2. Large shift to a later time (all the way past the later range), later first:
-- TODO

-- 4. Large shift to an earlier time (all the way past the earlier range)
-- 4.1. Large shift to an earlier time (all the way past the earlier range), earlier first:
-- TODO
-- 4.2. Large shift to an earlier time (all the way past the earlier range), later first:
-- TODO

-- 5. Swap the ranges
-- 5.1. Swap the ranges, earlier first:
-- TODO
-- 5.2. Swap the ranges, later first:
-- TODO

SELECT disable_sql_saga_for_shifts_houses_and_rooms();