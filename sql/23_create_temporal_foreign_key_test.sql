INSERT INTO houses VALUES
  (1, 150000, '2015-01-01'::TIMESTAMPTZ, '2016-01-01'::TIMESTAMPTZ),
  (1, 200000, '2016-01-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ)
;

-- it works on an empty table
SELECT enable_sql_saga_for_shifts_houses_and_rooms();
SELECT disable_sql_saga_for_shifts_houses_and_rooms();

-- it works on a table with a NULL foreign key
INSERT INTO rooms(id,house_id,valid_after,valid_to) VALUES (1, NULL, '2015-01-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ);
SELECT enable_sql_saga_for_shifts_houses_and_rooms();
SELECT disable_sql_saga_for_shifts_houses_and_rooms();
DELETE FROM rooms;

-- it works on a table with a FK fulfilled by one row
INSERT INTO rooms(id,house_id,valid_after,valid_to) VALUES (1, 1, '2015-01-01'::TIMESTAMPTZ, '2016-01-01'::TIMESTAMPTZ);
SELECT enable_sql_saga_for_shifts_houses_and_rooms();
SELECT disable_sql_saga_for_shifts_houses_and_rooms();
DELETE FROM rooms;

-- it works on a table with a FK fulfilled by two rows
INSERT INTO rooms(id,house_id,valid_after,valid_to) VALUES (1, 1, '2015-01-01'::TIMESTAMPTZ, '2016-06-01'::TIMESTAMPTZ);
SELECT enable_sql_saga_for_shifts_houses_and_rooms();
SELECT disable_sql_saga_for_shifts_houses_and_rooms();
DELETE FROM rooms;

-- it fails on a table with a missing foreign key
INSERT INTO rooms(id,house_id,valid_after,valid_to) VALUES (1, 2, '2015-01-01'::TIMESTAMPTZ, '2016-01-01'::TIMESTAMPTZ);
SELECT enable_sql_saga_for_shifts_houses_and_rooms();
SELECT disable_sql_saga_for_shifts_houses_and_rooms();
DELETE FROM rooms;

-- it fails on a table with a completely-uncovered foreign key
INSERT INTO rooms(id,house_id,valid_after,valid_to) VALUES (1, 1, '2010-01-01'::TIMESTAMPTZ, '2011-01-01'::TIMESTAMPTZ);
SELECT enable_sql_saga_for_shifts_houses_and_rooms();
SELECT disable_sql_saga_for_shifts_houses_and_rooms();
DELETE FROM rooms;

-- it fails on a table with a partially-covered foreign key
INSERT INTO rooms(id,house_id,valid_after,valid_to) VALUES (1, 1, '2015-01-01'::TIMESTAMPTZ, '2018-01-01'::TIMESTAMPTZ);
SELECT enable_sql_saga_for_shifts_houses_and_rooms();
SELECT disable_sql_saga_for_shifts_houses_and_rooms();
DELETE FROM rooms;

DELETE FROM rooms;
DELETE FROM houses;
