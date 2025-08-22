SELECT enable_sql_saga_for_shifts_houses_and_rooms();

INSERT INTO houses (id, assessment, valid_from, valid_until) VALUES
  (1, 150000, '2015-01-01'::TIMESTAMPTZ, '2016-01-01'::TIMESTAMPTZ),
  (1, 200000, '2016-01-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ),
  (2, 300000, '2015-01-01'::TIMESTAMPTZ, '2016-01-01'::TIMESTAMPTZ),
  (3, 100000, '2014-01-01'::TIMESTAMPTZ, '2015-01-01'::TIMESTAMPTZ),
  (3, 200000, '2015-01-01'::TIMESTAMPTZ, 'infinity'::TIMESTAMPTZ),
  (4, 200000, '-infinity'::TIMESTAMPTZ, '2014-01-01'::TIMESTAMPTZ)
;


-- You can insert a NULL fk
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, NULL, '2010-01-01'::TIMESTAMPTZ, '2011-01-01'::TIMESTAMPTZ);
DELETE FROM rooms;

-- You can insert a finite fk exactly covered by one row
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 1, '2015-01-01'::TIMESTAMPTZ, '2016-01-01'::TIMESTAMPTZ);
DELETE FROM rooms;

-- You can insert a finite fk more than covered by one row
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 1, '2015-01-01'::TIMESTAMPTZ, '2015-06-01'::TIMESTAMPTZ);
DELETE FROM rooms;

-- You can insert a finite fk exactly covered by two rows
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 1, '2015-01-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ);
DELETE FROM rooms;

-- You can insert a finite fk more than covered by two rows
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 1, '2015-01-01'::TIMESTAMPTZ, '2016-06-01'::TIMESTAMPTZ);
DELETE FROM rooms;

-- You can't insert a finite fk id not covered by any row
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 7, '2015-01-01'::TIMESTAMPTZ, '2016-01-01'::TIMESTAMPTZ);

-- You can't insert a finite fk range not covered by any row
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 1, '1999-01-01'::TIMESTAMPTZ, '2000-01-01'::TIMESTAMPTZ);

-- You can't insert a finite fk partially covered by one row
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 1, '2014-01-01'::TIMESTAMPTZ, '2015-06-01'::TIMESTAMPTZ);

-- You can't insert a finite fk partially covered by two rows
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 1, '2014-01-01'::TIMESTAMPTZ, '2016-06-01'::TIMESTAMPTZ);

-- You can insert an infinite fk exactly covered by one row
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 3, '2015-01-01'::TIMESTAMPTZ, 'infinity'::TIMESTAMPTZ);
DELETE FROM rooms;

-- You can insert an infinite fk more than covered by one row
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 3, '2016-01-01'::TIMESTAMPTZ, 'infinity'::TIMESTAMPTZ);
DELETE FROM rooms;

-- You can insert an infinite fk exactly covered by two rows
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 3, '2014-01-01'::TIMESTAMPTZ, 'infinity'::TIMESTAMPTZ);
DELETE FROM rooms;

-- You can insert an infinite fk more than covered by two rows
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 3, '2014-06-01'::TIMESTAMPTZ, 'infinity'::TIMESTAMPTZ);
DELETE FROM rooms;

-- You can't insert an infinite fk id not covered by any row
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 7, '2015-01-01'::TIMESTAMPTZ, 'infinity'::TIMESTAMPTZ);

-- You can't insert an infinite fk range not covered by any row
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 1, '2020-01-01'::TIMESTAMPTZ, 'infinity'::TIMESTAMPTZ);

-- You can't insert an infinite fk partially covered by one row
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 4, '-infinity'::TIMESTAMPTZ, '2020-01-01'::TIMESTAMPTZ);

-- You can't insert an infinite fk partially covered by two rows
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 3, '1990-01-01'::TIMESTAMPTZ, 'infinity'::TIMESTAMPTZ);

DELETE FROM rooms;
DELETE FROM houses;
SELECT disable_sql_saga_for_shifts_houses_and_rooms();
