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

INSERT INTO houses VALUES
  (1, 150000, '2015-01-01', '2016-01-01'),
  (1, 200000, '2016-01-01', '2017-01-01')
;
INSERT INTO rooms VALUES
  (1, 1, '2015-01-01', '2017-01-01')
;
BEGIN;
SET CONSTRAINTS ALL DEFERRED;

UPDATE houses
SET valid_after = new_valid_after,
    valid_to = new_valid_to
FROM (VALUES
    (1, '2015-01-01'::TIMESTAMPTZ, '2015-01-01'::TIMESTAMPTZ, '2016-06-01'::TIMESTAMPTZ),
    (1, '2016-01-01'::TIMESTAMPTZ, '2016-06-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ)
) AS change(id, old_valid_after, new_valid_after, new_valid_to)
WHERE houses.id = change.id
  AND valid_after = old_valid_after;

SET CONSTRAINTS ALL IMMEDIATE;
COMMIT;
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
SET     (valid_after, valid_to) = ('2015-01-01', '2016-06-01')
WHERE   id = 1 AND valid_after = '2015-01-01'
;

UPDATE  houses
SET     (valid_after, valid_to) = ('2016-06-01', '2017-01-01')
WHERE   id = 1 AND valid_after = '2016-01-01'
;

--
-- 1.2.2. When the exclusion constraint is checked immediately,
--        you can't move the time in one transaction with two statements.
--

BEGIN;
SET CONSTRAINTS houses_id_tstzrange_excl IMMEDIATE;
UPDATE  houses
SET     (valid_after, valid_to) = ('2015-01-01', '2016-06-01')
WHERE   id = 1 AND valid_after = '2015-01-01'
;

UPDATE  houses
SET     (valid_after, valid_to) = ('2016-06-01', '2017-01-01')
WHERE   id = 1 AND valid_after = '2016-01-01'
;
COMMIT;

--
-- 1.2.3. When the exclusion constraint is checked deferred,
--        you can move the time in one transaction with two statements.
--

BEGIN;
SET CONSTRAINTS houses_id_tstzrange_excl DEFERRED;
SET CONSTRAINTS rooms_house_id_valid_uk_update DEFERRED;

UPDATE  houses
SET     (valid_after, valid_to) = ('2015-01-01', '2016-06-01')
WHERE   id = 1 AND valid_after = '2015-01-01'
;

UPDATE  houses
SET     (valid_after, valid_to) = ('2016-06-01', '2017-01-01')
WHERE   id = 1 AND valid_after = '2016-01-01'
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
SET     (valid_after, valid_to) = ('2016-06-01', '2017-01-01')
WHERE   id = 1 AND valid_after = '2016-01-01'
;

UPDATE  houses
SET     (valid_after, valid_to) = ('2015-01-01', '2016-06-01')
WHERE   id = 1 AND valid_after = '2015-01-01'
;

--
-- 1.3.2. When the exclusion constraint is checked immediately,
--        you can move the time in one transaction with two statements.
--

BEGIN;
SET CONSTRAINTS houses_id_tstzrange_excl IMMEDIATE;
SET CONSTRAINTS rooms_house_id_valid_uk_update DEFERRED;

UPDATE  houses
SET     (valid_after, valid_to) = ('2016-06-01', '2017-01-01')
WHERE   id = 1 AND valid_after = '2016-01-01'
;

UPDATE  houses
SET     (valid_after, valid_to) = ('2015-01-01', '2016-06-01')
WHERE   id = 1 AND valid_after = '2015-01-01'
;
COMMIT;

--
-- 1.3.3. When the exclusion constraint is checked deferred,
--        you can move the time in one transaction with two statements.
--

BEGIN;
SET CONSTRAINTS houses_id_tstzrange_excl DEFERRED;
SET CONSTRAINTS rooms_house_id_valid_uk_update DEFERRED;

UPDATE  houses
SET     (valid_after, valid_to) = ('2016-09-01', '2017-01-01')
WHERE   id = 1 AND valid_after = '2016-06-01'
;

UPDATE  houses
SET     (valid_after, valid_to) = ('2015-01-01', '2016-09-01')
WHERE   id = 1 AND valid_after = '2015-01-01'
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
BEGIN;
SET CONSTRAINTS ALL DEFERRED;

UPDATE houses
SET valid_after = new_valid_after,
    valid_to = new_valid_to
FROM (VALUES
    (1, '2015-01-01'::TIMESTAMPTZ, '2015-01-01'::TIMESTAMPTZ, '2015-06-01'::TIMESTAMPTZ),
    (1, '2016-01-01'::TIMESTAMPTZ, '2015-06-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ)
) AS change(id, old_valid_after, new_valid_after, new_valid_to)
WHERE houses.id = change.id
  AND valid_after = old_valid_after;

SET CONSTRAINTS ALL IMMEDIATE;
COMMIT;

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
SET     (valid_after, valid_to) = ('2015-01-01', '2015-06-01')
WHERE   id = 1 AND valid_after = '2015-01-01'
;

UPDATE  houses
SET     (valid_after, valid_to) = ('2015-06-01', '2017-01-01')
WHERE   id = 1 AND valid_after = '2016-01-01'
;

--
-- 2.2.2. When the exclusion constraint is checked immediately,
--        you can move the time in one transaction with two statements.
--

BEGIN;
SET CONSTRAINTS houses_id_tstzrange_excl IMMEDIATE;
SET CONSTRAINTS rooms_house_id_valid_uk_update DEFERRED;

UPDATE  houses
SET     (valid_after, valid_to) = ('2015-01-01', '2015-06-01')
WHERE   id = 1 AND valid_after = '2015-01-01'
;

UPDATE  houses
SET     (valid_after, valid_to) = ('2015-06-01', '2017-01-01')
WHERE   id = 1 AND valid_after = '2016-01-01'
;
COMMIT;

--
-- 2.2.3. When the exclusion constraint is checked deferred,
--        you can move the time in one transaction with two statements.
--

BEGIN;
SET CONSTRAINTS houses_id_tstzrange_excl DEFERRED;
SET CONSTRAINTS rooms_house_id_valid_uk_update DEFERRED;

UPDATE  houses
SET     (valid_after, valid_to) = ('2015-01-01', '2015-03-01')
WHERE   id = 1 AND valid_after = '2015-01-01'
;

UPDATE  houses
SET     (valid_after, valid_to) = ('2015-03-01', '2017-01-01')
WHERE   id = 1 AND valid_after = '2015-06-01'
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
SET     (valid_after, valid_to) = ('2015-06-01', '2017-01-01')
WHERE   id = 1 AND valid_after = '2016-01-01'
;

UPDATE  houses
SET     (valid_after, valid_to) = ('2015-01-01', '2015-06-01')
WHERE   id = 1 AND valid_after = '2015-01-01'
;

--
-- 2.3.2. When the exclusion constraint is checked immediately,
--        you can't move the time in one transaction with two statements.
--

BEGIN;
SET CONSTRAINTS houses_id_tstzrange_excl IMMEDIATE;
UPDATE  houses
SET     (valid_after, valid_to) = ('2015-06-01', '2017-01-01')
WHERE   id = 1 AND valid_after = '2016-01-01'
;

UPDATE  houses
SET     (valid_after, valid_to) = ('2015-01-01', '2015-06-01')
WHERE   id = 1 AND valid_after = '2015-01-01'
;
COMMIT;

--
-- 2.3.3. When the exclusion constraint is checked deferred,
--        you can move the time in one transaction with two statements.
--

BEGIN;
SET CONSTRAINTS houses_id_tstzrange_excl DEFERRED;
SET CONSTRAINTS rooms_house_id_valid_uk_update DEFERRED;

UPDATE  houses
SET     (valid_after, valid_to) = ('2015-06-01', '2017-01-01')
WHERE   id = 1 AND valid_after = '2016-01-01'
;

UPDATE  houses
SET     (valid_after, valid_to) = ('2015-01-01', '2015-06-01')
WHERE   id = 1 AND valid_after = '2015-01-01'
;
COMMIT;

-- 3. Large shift to a later time (all the way past the later range)
-- 3.1. Large shift to a later time (all the way past the later range), earlier first:
-- Delete existing data and re-insert
DELETE FROM rooms;
DELETE FROM houses;

INSERT INTO houses VALUES
  (1, 150000, '2015-01-01', '2016-01-01'),
  (1, 200000, '2016-01-01', '2017-01-01');

INSERT INTO rooms VALUES
  (1, 1, '2015-01-01', '2017-01-01');

-- Perform large shift
BEGIN;
SET CONSTRAINTS ALL DEFERRED;

-- Update earlier range first
UPDATE houses
SET valid_after = '2018-01-01', valid_to = '2019-01-01'
WHERE id = 1 AND valid_after = '2015-01-01';

-- Update later range
UPDATE houses
SET valid_after = '2019-01-01', valid_to = '2020-01-01'
WHERE id = 1 AND valid_after = '2016-01-01';

COMMIT;

-- 3.2. Large shift to a later time (all the way past the later range), later first:
-- Similar setup as above but update the later range first
BEGIN;
SET CONSTRAINTS ALL DEFERRED;

-- Update later range first
UPDATE houses
SET valid_after = '2019-01-01', valid_to = '2020-01-01'
WHERE id = 1 AND valid_after = '2016-01-01';

-- Update earlier range
UPDATE houses
SET valid_after = '2018-01-01', valid_to = '2019-01-01'
WHERE id = 1 AND valid_after = '2015-01-01';

COMMIT;


-- 4. Large shift to an earlier time (all the way past the earlier range)
-- 4.1. Large shift to an earlier time (all the way past the earlier range), earlier first:
-- Delete and re-insert
DELETE FROM rooms;
DELETE FROM houses;

INSERT INTO houses VALUES
  (1, 150000, '2020-01-01', '2021-01-01'),
  (1, 200000, '2021-01-01', '2022-01-01');

INSERT INTO rooms VALUES
  (1, 1, '2020-01-01', '2022-01-01');

-- Perform shift
BEGIN;
SET CONSTRAINTS ALL DEFERRED;

-- Update earlier range first
UPDATE houses
SET valid_after = '2018-01-01', valid_to = '2019-01-01'
WHERE id = 1 AND valid_after = '2020-01-01';

-- Update later range
UPDATE houses
SET valid_after = '2019-01-01', valid_to = '2020-01-01'
WHERE id = 1 AND valid_to = '2022-01-01';

-- Adjust rooms
UPDATE rooms
SET valid_after = '2018-01-01', valid_to = '2020-01-01'
WHERE id = 1;

COMMIT;

-- 4.2. Large shift to an earlier time (all the way past the earlier range), later first:
-- Similar setup as above but update the later range first
BEGIN;
SET CONSTRAINTS ALL DEFERRED;

-- Update later range first
UPDATE houses
SET valid_after = '2019-01-01', valid_to = '2020-01-01'
WHERE id = 1 AND valid_after = '2021-01-01';

-- Update earlier range
UPDATE houses
SET valid_after = '2018-01-01', valid_to = '2019-01-01'
WHERE id = 1 AND valid_after = '2020-01-01';

-- Adjust rooms
UPDATE rooms
SET valid_after = '2018-01-01', valid_to = '2020-01-01'
WHERE id = 1;

COMMIT;


-- 5. Swap the ranges
-- 5.1. Swap the ranges, earlier first:
-- Setup
DELETE FROM rooms;
DELETE FROM houses;

INSERT INTO houses VALUES
  (1, 150000, '2020-01-01', '2021-01-01'),
  (1, 200000, '2021-01-01', '2022-01-01');

INSERT INTO rooms VALUES
  (1, 1, '2020-01-01', '2022-01-01');

-- Swap ranges
BEGIN;
SET CONSTRAINTS ALL DEFERRED;

-- Update earlier range to later period
UPDATE houses
SET valid_after = '2021-01-01', valid_to = '2022-01-01'
WHERE id = 1 AND assessment = 150000;

-- Update later range to earlier period
UPDATE houses
SET valid_after = '2020-01-01', valid_to = '2021-01-01'
WHERE id = 1 AND assessment = 200000;

COMMIT;

-- 5.2. Swap the ranges, later first:
-- Similar setup as above but update the later range to earlier period first
BEGIN;
SET CONSTRAINTS ALL DEFERRED;

-- Update later range to earlier period first
UPDATE houses
SET valid_after = '2020-01-01', valid_to = '2021-01-01'
WHERE id = 1 AND assessment = 200000;

-- Update earlier range to later period
UPDATE houses
SET valid_after = '2021-01-01', valid_to = '2022-01-01'
WHERE id = 1 AND assessment = 150000;

COMMIT;


SELECT disable_sql_saga_for_shifts_houses_and_rooms();