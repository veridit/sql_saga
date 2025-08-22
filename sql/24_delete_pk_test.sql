SELECT enable_sql_saga_for_shifts_houses_and_rooms();

INSERT INTO houses VALUES
  (1, 150000, '2015-01-01'::TIMESTAMPTZ, '2016-01-01'::TIMESTAMPTZ),
  (1, 200000, '2016-01-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ),
  (2, 300000, '2015-01-01'::TIMESTAMPTZ, '2016-01-01'::TIMESTAMPTZ),
  (3, 100000, '2014-01-01'::TIMESTAMPTZ, '2015-01-01'::TIMESTAMPTZ),
  (3, 200000, '2015-01-01'::TIMESTAMPTZ, 'infinity')
;

-- You can delete a pk with no references
DELETE FROM houses WHERE id = 2;

-- You can delete a finite pk range with no references
INSERT INTO rooms VALUES (1, 1, '2016-06-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ);
DELETE FROM houses WHERE id = 1 and tstzrange(valid_from, valid_until) @> '2015-06-01'::timestamptz;
INSERT INTO houses VALUES (1, 200000, '2015-01-01'::TIMESTAMPTZ, '2016-01-01'::TIMESTAMPTZ);
DELETE FROM rooms;

-- You can't delete a finite pk range that is partly covered
INSERT INTO rooms VALUES (1, 1, '2016-01-01'::TIMESTAMPTZ, '2016-06-01'::TIMESTAMPTZ);
DELETE FROM houses WHERE id = 1 and tstzrange(valid_from, valid_until) @> '2016-06-01'::timestamptz;
DELETE FROM rooms;

-- You can't delete a finite pk range that is exactly covered
INSERT INTO rooms VALUES (1, 1, '2016-01-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ);
DELETE FROM houses WHERE id = 1 and tstzrange(valid_from, valid_until) @> '2016-06-01'::timestamptz;
DELETE FROM rooms;

-- You can't delete a finite pk range that is more than covered
INSERT INTO rooms VALUES (1, 1, '2015-06-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ);
DELETE FROM houses WHERE id = 1 and tstzrange(valid_from, valid_until) @> '2016-06-01'::timestamptz;
DELETE FROM rooms;

-- You can delete an infinite pk range with no references
INSERT INTO rooms VALUES (1, 3, '2014-06-01'::TIMESTAMPTZ, '2015-01-01'::TIMESTAMPTZ);
DELETE FROM houses WHERE id = 3 and tstzrange(valid_from, valid_until) @> '2016-01-01'::timestamptz;
INSERT INTO houses VALUES (3, 200000, '2015-01-01'::TIMESTAMPTZ, 'infinity');
DELETE FROM rooms;

-- You can't delete an infinite pk range that is partly covered
INSERT INTO rooms VALUES (1, 3, '2016-01-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ);
DELETE FROM houses WHERE id = 3 and tstzrange(valid_from, valid_until) @> '2016-01-01'::timestamptz;
DELETE FROM rooms;

-- You can't delete an infinite pk range that is exactly covered
INSERT INTO rooms VALUES (1, 3, '2015-01-01'::TIMESTAMPTZ, 'infinity');
DELETE FROM houses WHERE id = 3 and tstzrange(valid_from, valid_until) @> '2016-01-01'::timestamptz;
DELETE FROM rooms;

-- You can't delete an infinite pk range that is more than covered
INSERT INTO rooms VALUES (1, 3, '2014-06-01'::TIMESTAMPTZ, 'infinity');
DELETE FROM houses WHERE id = 3 and tstzrange(valid_from, valid_until) @> '2016-01-01'::timestamptz;
DELETE FROM rooms;

-- ON DELETE NOACTION
-- (same behavior as RESTRICT, but different entry function so it should have separate tests)
-- TODO: Write some tests against normal FKs just to see NOACTION vs RESTRICT

-- ON DELETE CASCADE
-- TODO

-- ON DELETE SET NULL
-- TODO

-- ON DELETE SET DEFAULT
-- TODO

DELETE FROM rooms;
DELETE FROM houses;
SELECT disable_sql_saga_for_shifts_houses_and_rooms();
