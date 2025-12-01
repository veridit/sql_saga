\i sql/include/test_setup.sql

BEGIN;

SET ROLE TO sql_saga_unprivileged_user;

\i sql/support/shifts_houses_rooms_tables.sql
\i sql/support/shifts_houses_rooms_enable_saga.sql
\i sql/support/houses_data.sql

SAVEPOINT s;

-- You can update an fk id to NULL
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 1, '[2015-01-01,2015-06-01)');
UPDATE rooms SET house_id = NULL;
ROLLBACK TO SAVEPOINT s;

-- You can update the range when the fk is NULL
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, NULL, '[2015-01-01,2015-06-01)');
UPDATE rooms SET valid_range = '[1999-01-01,2000-01-01)';
ROLLBACK TO SAVEPOINT s;

-- You can update a finite fk exactly covered by one row
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 1, '[2015-01-01,2015-02-01)');
UPDATE rooms SET valid_range = '[2015-01-01,2016-01-01)';
ROLLBACK TO SAVEPOINT s;

-- You can update a finite fk more than covered by one row
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 1, '[2015-01-01,2015-02-01)');
UPDATE rooms SET valid_range = '[2015-01-01,2015-06-01)';
ROLLBACK TO SAVEPOINT s;

-- You can update a finite fk exactly covered by two rows
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 1, '[2015-01-01,2015-02-01)');
UPDATE rooms SET valid_range = '[2015-01-01,2017-01-01)';
ROLLBACK TO SAVEPOINT s;

-- You can update a finite fk more than covered by two rows
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 1, '[2015-01-01,2015-02-01)');
UPDATE rooms SET valid_range = '[2015-01-01,2016-06-01)';
ROLLBACK TO SAVEPOINT s;

-- You can't update a finite fk id not covered by any row
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 1, '[2015-01-01,2015-02-01)');
UPDATE rooms SET house_id = 7;
ROLLBACK TO SAVEPOINT s;

-- You can't update a finite fk range not covered by any row
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 1, '[2015-01-01,2015-02-01)');
UPDATE rooms SET valid_range = '[1999-01-01,2000-01-01)';
ROLLBACK TO SAVEPOINT s;

-- You can't update a finite fk partially covered by one row
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 1, '[2015-01-01,2015-02-01)');
UPDATE rooms SET valid_range = '[2014-01-01,2015-06-01)';
ROLLBACK TO SAVEPOINT s;

-- You can't update a finite fk partially covered by two rows
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 1, '[2015-01-01,2015-02-01)');
UPDATE rooms SET valid_range = '[2014-01-01,2016-06-01)';
ROLLBACK TO SAVEPOINT s;

-- You can update an infinite fk exactly covered by one row
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 3, '[2015-01-01,2015-02-01)');
UPDATE rooms SET valid_range = '[2015-01-01,infinity)';
ROLLBACK TO SAVEPOINT s;

-- You can update an infinite fk more than covered by one row
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 3, '[2015-01-01,2015-02-01)');
UPDATE rooms SET valid_range = '[2016-01-01,infinity)';
ROLLBACK TO SAVEPOINT s;

-- You can update an infinite fk exactly covered by two rows
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 3, '[2015-01-01,2015-02-01)');
UPDATE rooms SET valid_range = '[2014-01-01,infinity)';
ROLLBACK TO SAVEPOINT s;

-- You can update an infinite fk more than covered by two rows
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 3, '[2015-01-01,2015-02-01)');
UPDATE rooms SET valid_range = '[2014-06-01,infinity)';
ROLLBACK TO SAVEPOINT s;

-- You can't update an infinite fk id not covered by any row
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 3, '[2015-01-01,2015-02-01)');
UPDATE rooms SET house_id = 7;
ROLLBACK TO SAVEPOINT s;

-- You can't update an infinite fk range not covered by any row
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 1, '[2015-01-01,2015-02-01)');
UPDATE rooms SET valid_range = '[2020-01-01,infinity)';
ROLLBACK TO SAVEPOINT s;

-- You can't update an infinite fk partially covered by one row
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 4, '[-infinity,2012-01-01)');
UPDATE rooms SET valid_range = '[-infinity,2020-01-01)';
ROLLBACK TO SAVEPOINT s;

-- You can't update an infinite fk partially covered by two rows
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 3, '[2015-01-01,2015-02-01)');
UPDATE rooms SET valid_range = '[1990-01-01,infinity)';
ROLLBACK TO SAVEPOINT s;

ROLLBACK;

\i sql/include/test_teardown.sql
