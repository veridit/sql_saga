-- ON UPDATE RESTRICT
SELECT enable_sql_saga_for_shifts_houses_and_rooms();

INSERT INTO houses VALUES
  (1, 150000, '2015-01-01'::TIMESTAMPTZ, '2016-01-01'::TIMESTAMPTZ),
  (1, 200000, '2016-01-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ),
  (2, 300000, '2015-01-01'::TIMESTAMPTZ, '2016-01-01'::TIMESTAMPTZ),
  (3, 100000, '2014-01-01'::TIMESTAMPTZ, '2015-01-01'::TIMESTAMPTZ),
  (3, 200000, '2015-01-01'::TIMESTAMPTZ, 'infinity')
;

-- You can update a finite pk id with no references
UPDATE houses SET id = 4 WHERE id = 1;
UPDATE houses SET id = 1 WHERE id = 4;

-- You can update a finite pk range with no references
UPDATE houses SET valid_from = '1999-01-01'::TIMESTAMPTZ, valid_to = '2000-01-01'::TIMESTAMPTZ WHERE id = 1 AND tstzrange(valid_from, valid_to) @> '2015-06-01'::timestamptz;
UPDATE houses SET valid_from = '2015-01-01'::TIMESTAMPTZ, valid_to = '2016-01-01'::TIMESTAMPTZ WHERE id = 1 AND tstzrange(valid_from, valid_to) @> '1999-06-01'::timestamptz;

-- You can update a finite pk range that is partly covered elsewhere
INSERT INTO rooms VALUES (1, 1, '2016-01-01'::TIMESTAMPTZ, '2016-06-01'::TIMESTAMPTZ);
UPDATE houses SET valid_from = '2016-01-01'::TIMESTAMPTZ, valid_to = '2016-09-01'::TIMESTAMPTZ WHERE id = 1 AND tstzrange(valid_from, valid_to) @> '2016-06-01'::timestamptz;
UPDATE houses SET valid_from = '2016-01-01'::TIMESTAMPTZ, valid_to = '2017-01-01'::TIMESTAMPTZ WHERE id = 1 AND tstzrange(valid_from, valid_to) @> '2016-06-01'::timestamptz;
DELETE FROM rooms;

-- You can't update a finite pk id that is partly covered
INSERT INTO rooms VALUES (1, 1, '2016-01-01'::TIMESTAMPTZ, '2016-06-01'::TIMESTAMPTZ);
UPDATE houses SET id = 4 WHERE id = 1;
DELETE FROM rooms;

-- You can't update a finite pk range that is partly covered
INSERT INTO rooms VALUES (1, 1, '2016-01-01'::TIMESTAMPTZ, '2016-06-01'::TIMESTAMPTZ);
UPDATE houses SET valid_from = '2017-01-01'::TIMESTAMPTZ, valid_to = '2018-01-01'::TIMESTAMPTZ WHERE id = 1 AND tstzrange(valid_from, valid_to) @> '2016-06-01'::timestamptz;
DELETE FROM rooms;

-- You can't update a finite pk id that is exactly covered
INSERT INTO rooms VALUES (1, 1, '2016-01-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ);
UPDATE houses SET id = 4 WHERE id = 1;
DELETE FROM rooms;

-- You can't update a finite pk range that is exactly covered
INSERT INTO rooms VALUES (1, 1, '2016-01-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ);
UPDATE houses SET valid_from = '2017-01-01'::TIMESTAMPTZ, valid_to = '2018-01-01'::TIMESTAMPTZ WHERE id = 1 AND tstzrange(valid_from, valid_to) @> '2016-06-01'::timestamptz;
DELETE FROM rooms;

-- You can't update a finite pk id that is more than covered
INSERT INTO rooms VALUES (1, 1, '2015-06-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ);
UPDATE houses SET id = 4 WHERE id = 1;
DELETE FROM rooms;

-- You can't update a finite pk range that is more than covered
INSERT INTO rooms VALUES (1, 1, '2015-06-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ);
UPDATE houses SET valid_from = '2017-01-01'::TIMESTAMPTZ, valid_to = '2018-01-01'::TIMESTAMPTZ WHERE id = 1 AND tstzrange(valid_from, valid_to) @> '2016-06-01'::timestamptz;
DELETE FROM rooms;

-- You can update an infinite pk id with no references
INSERT INTO rooms VALUES (1, 3, '2014-06-01'::TIMESTAMPTZ, '2015-01-01'::TIMESTAMPTZ);
UPDATE houses SET id = 4 WHERE id = 3 and tstzrange(valid_from, valid_to) @> '2016-01-01'::timestamptz;
UPDATE houses SET id = 3 WHERE id = 4;
DELETE FROM rooms;

-- You can update an infinite pk range with no references
INSERT INTO rooms VALUES (1, 3, '2014-06-01'::TIMESTAMPTZ, '2015-01-01'::TIMESTAMPTZ);
UPDATE houses SET valid_from = '2017-01-01'::TIMESTAMPTZ, valid_to = '2018-01-01'::TIMESTAMPTZ WHERE id = 3 and tstzrange(valid_from, valid_to) @> '2016-01-01'::timestamptz;
UPDATE houses SET valid_from = '2015-01-01'::TIMESTAMPTZ, valid_to = 'infinity' WHERE id = 3 and tstzrange(valid_from, valid_to) @> '2017-06-01'::timestamptz;
DELETE FROM rooms;

-- You can't update an infinite pk id that is partly covered
INSERT INTO rooms VALUES (1, 3, '2016-01-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ);
UPDATE houses SET id = 4 WHERE id = 3 and tstzrange(valid_from, valid_to) @> '2016-01-01'::timestamptz;
DELETE FROM rooms;

-- You can't update an infinite pk range that is partly covered
INSERT INTO rooms VALUES (1, 3, '2016-01-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ);
UPDATE houses SET valid_from = '2017-01-01'::TIMESTAMPTZ, valid_to = '2018-01-01'::TIMESTAMPTZ WHERE id = 3 and tstzrange(valid_from, valid_to) @> '2016-01-01'::timestamptz;
DELETE FROM rooms;

-- You can't update an infinite pk id that is exactly covered
INSERT INTO rooms VALUES (1, 3, '2015-01-01'::TIMESTAMPTZ, 'infinity');
UPDATE houses SET id = 4 WHERE id = 3 and tstzrange(valid_from, valid_to) @> '2016-01-01'::timestamptz;
DELETE FROM rooms;

-- You can't update an infinite pk range that is exactly covered
INSERT INTO rooms VALUES (1, 3, '2015-01-01'::TIMESTAMPTZ, 'infinity');
UPDATE  houses SET valid_from = '2017-01-01'::TIMESTAMPTZ, valid_to = '2018-01-01'::TIMESTAMPTZ WHERE id = 3 and tstzrange(valid_from, valid_to) @> '2016-01-01'::timestamptz;
DELETE FROM rooms;

-- You can't update an infinite pk id that is more than covered
INSERT INTO rooms VALUES (1, 3, '2014-06-01'::TIMESTAMPTZ, 'infinity');
UPDATE houses SET id = 4 WHERE id = 3 and tstzrange(valid_from, valid_to) @> '2016-01-01'::timestamptz;
DELETE FROM rooms;

-- You can't update an infinite pk range that is more than covered
INSERT INTO rooms VALUES (1, 3, '2014-06-01'::TIMESTAMPTZ, 'infinity');
UPDATE houses SET valid_from = '2017-01-01'::TIMESTAMPTZ, valid_to = '2018-01-01'::TIMESTAMPTZ WHERE id = 3 and tstzrange(valid_from, valid_to) @> '2016-01-01'::timestamptz;
DELETE FROM rooms;

-- ON UPDATE NOACTION
-- TODO

-- ON UPDATE CASCADE
-- TODO

-- ON UPDATE SET NULL
-- TODO

-- ON UPDATE SET DEFAULT
-- TODO

DELETE FROM rooms;
DELETE FROM houses;
SELECT disable_sql_saga_for_shifts_houses_and_rooms();
