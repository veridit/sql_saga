SELECT enable_sql_saga_for_shifts_houses_and_rooms();

INSERT INTO houses (id, assessment, valid_from, valid_until) VALUES
  (1, 150000, '2015-01-01'::TIMESTAMPTZ, '2016-01-01'::TIMESTAMPTZ),
  (1, 200000, '2016-01-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ),
  (2, 300000, '2015-01-01'::TIMESTAMPTZ, '2016-01-01'::TIMESTAMPTZ),
  (3, 100000, '2014-01-01'::TIMESTAMPTZ, '2015-01-01'::TIMESTAMPTZ),
  (3, 200000, '2015-01-01'::TIMESTAMPTZ, 'infinity'::TIMESTAMPTZ),
  (4, 200000, '-infinity'::TIMESTAMPTZ, '2014-01-01'::TIMESTAMPTZ)
;


-- You can update an fk id to NULL
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 1, '2015-01-01'::TIMESTAMPTZ, '2015-06-01'::TIMESTAMPTZ);
UPDATE rooms SET house_id = NULL;
DELETE FROM rooms;

-- You can update the range when the fk is NULL
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, NULL, '2015-01-01'::TIMESTAMPTZ, '2015-06-01'::TIMESTAMPTZ);
UPDATE rooms SET (valid_from, valid_until) = ('1999-01-01'::TIMESTAMPTZ, '2000-01-01');
DELETE FROM rooms;

-- You can update a finite fk exactly covered by one row
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 1, '2015-01-01'::TIMESTAMPTZ, '2015-02-01'::TIMESTAMPTZ);
UPDATE rooms SET (valid_from, valid_until) = ('2015-01-01'::TIMESTAMPTZ, '2016-01-01');
DELETE FROM rooms;

-- You can update a finite fk more than covered by one row
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 1, '2015-01-01'::TIMESTAMPTZ, '2015-02-01'::TIMESTAMPTZ);
UPDATE rooms SET (valid_from, valid_until) = ('2015-01-01'::TIMESTAMPTZ, '2015-06-01');
DELETE FROM rooms;

-- You can update a finite fk exactly covered by two rows
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 1, '2015-01-01'::TIMESTAMPTZ, '2015-02-01'::TIMESTAMPTZ);
UPDATE rooms SET (valid_from, valid_until) = ('2015-01-01'::TIMESTAMPTZ, '2017-01-01');
DELETE FROM rooms;

-- You can update a finite fk more than covered by two rows
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 1, '2015-01-01'::TIMESTAMPTZ, '2015-02-01'::TIMESTAMPTZ);
UPDATE rooms SET (valid_from, valid_until) = ('2015-01-01'::TIMESTAMPTZ, '2016-06-01');
DELETE FROM rooms;

-- You can't update a finite fk id not covered by any row
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 1, '2015-01-01'::TIMESTAMPTZ, '2015-02-01'::TIMESTAMPTZ);
UPDATE rooms SET house_id = 7;
DELETE FROM rooms;

-- You can't update a finite fk range not covered by any row
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 1, '2015-01-01'::TIMESTAMPTZ, '2015-02-01'::TIMESTAMPTZ);
UPDATE rooms SET (valid_from, valid_until) = ('1999-01-01'::TIMESTAMPTZ, '2000-01-01');
DELETE FROM rooms;

-- You can't update a finite fk partially covered by one row
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 1, '2015-01-01'::TIMESTAMPTZ, '2015-02-01'::TIMESTAMPTZ);
UPDATE rooms SET (valid_from, valid_until) = ('2014-01-01'::TIMESTAMPTZ, '2015-06-01');
DELETE FROM rooms;

-- You can't update a finite fk partially covered by two rows
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 1, '2015-01-01'::TIMESTAMPTZ, '2015-02-01'::TIMESTAMPTZ);
UPDATE rooms SET (valid_from, valid_until) = ('2014-01-01'::TIMESTAMPTZ, '2016-06-01');
DELETE FROM rooms;

-- You can update an infinite fk exactly covered by one row
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 3, '2015-01-01'::TIMESTAMPTZ, '2015-02-01'::TIMESTAMPTZ);
UPDATE rooms SET (valid_from, valid_until) = ('2015-01-01'::TIMESTAMPTZ, 'infinity');
DELETE FROM rooms;

-- You can update an infinite fk more than covered by one row
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 3, '2015-01-01'::TIMESTAMPTZ, '2015-02-01'::TIMESTAMPTZ);
UPDATE rooms SET (valid_from, valid_until) = ('2016-01-01'::TIMESTAMPTZ, 'infinity');
DELETE FROM rooms;

-- You can update an infinite fk exactly covered by two rows
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 3, '2015-01-01'::TIMESTAMPTZ, '2015-02-01'::TIMESTAMPTZ);
UPDATE rooms SET (valid_from, valid_until) = ('2014-01-01'::TIMESTAMPTZ, 'infinity');
DELETE FROM rooms;

-- You can update an infinite fk more than covered by two rows
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 3, '2015-01-01'::TIMESTAMPTZ, '2015-02-01'::TIMESTAMPTZ);
UPDATE rooms SET (valid_from, valid_until) = ('2014-06-01'::TIMESTAMPTZ, 'infinity');
DELETE FROM rooms;

-- You can't update an infinite fk id not covered by any row
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 3, '2015-01-01'::TIMESTAMPTZ, '2015-02-01'::TIMESTAMPTZ);
UPDATE rooms SET house_id = 7;
DELETE FROM rooms;

-- You can't update an infinite fk range not covered by any row
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 1, '2015-01-01'::TIMESTAMPTZ, '2015-02-01'::TIMESTAMPTZ);
UPDATE rooms SET (valid_from, valid_until) = ('2020-01-01'::TIMESTAMPTZ, 'infinity');
DELETE FROM rooms;

-- You can't update an infinite fk partially covered by one row
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 4, '-infinity', '2012-01-01'::TIMESTAMPTZ);
UPDATE rooms SET (valid_from, valid_until) = ('-infinity', '2020-01-01');
DELETE FROM rooms;

-- You can't update an infinite fk partially covered by two rows
INSERT INTO rooms (id, house_id, valid_from, valid_until) VALUES (1, 3, '2015-01-01'::TIMESTAMPTZ, '2015-02-01'::TIMESTAMPTZ);
UPDATE rooms SET (valid_from, valid_until) = ('1990-01-01'::TIMESTAMPTZ, 'infinity');
DELETE FROM rooms;

DELETE FROM rooms;
DELETE FROM houses;
SELECT disable_sql_saga_for_shifts_houses_and_rooms();
