\i sql/include/test_setup.sql

BEGIN;

SET ROLE TO sql_saga_unprivileged_user;

\i sql/support/shifts_houses_rooms_tables.sql
\i sql/support/shifts_houses_rooms_enable_saga.sql
\i sql/support/houses_data.sql

SAVEPOINT before_fail;

-- You can insert a NULL fk
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, NULL, '[2010-01-01,2011-01-01)');
ROLLBACK TO SAVEPOINT before_fail;

-- You can insert a finite fk exactly covered by one row
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 1, '[2015-01-01,2016-01-01)');
ROLLBACK TO SAVEPOINT before_fail;

-- You can insert a finite fk more than covered by one row
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 1, '[2015-01-01,2015-06-01)');
ROLLBACK TO SAVEPOINT before_fail;

-- You can insert a finite fk exactly covered by two rows
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 1, '[2015-01-01,2017-01-01)');
ROLLBACK TO SAVEPOINT before_fail;

-- You can insert a finite fk more than covered by two rows
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 1, '[2015-01-01,2016-06-01)');
ROLLBACK TO SAVEPOINT before_fail;

-- You can't insert a finite fk id not covered by any row
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 7, '[2015-01-01,2016-01-01)');
ROLLBACK TO SAVEPOINT before_fail;

-- You can't insert a finite fk range not covered by any row
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 1, '[1999-01-01,2000-01-01)');
ROLLBACK TO SAVEPOINT before_fail;

-- You can't insert a finite fk partially covered by one row
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 1, '[2014-01-01,2015-06-01)');
ROLLBACK TO SAVEPOINT before_fail;

-- You can't insert a finite fk partially covered by two rows
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 1, '[2014-01-01,2016-06-01)');
ROLLBACK TO SAVEPOINT before_fail;

-- You can insert an infinite fk exactly covered by one row
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 3, '[2015-01-01,infinity)');
ROLLBACK TO SAVEPOINT before_fail;

-- You can insert an infinite fk more than covered by one row
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 3, '[2016-01-01,infinity)');
ROLLBACK TO SAVEPOINT before_fail;

-- You can insert an infinite fk exactly covered by two rows
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 3, '[2014-01-01,infinity)');
ROLLBACK TO SAVEPOINT before_fail;

-- You can insert an infinite fk more than covered by two rows
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 3, '[2014-06-01,infinity)');
ROLLBACK TO SAVEPOINT before_fail;

-- You can't insert an infinite fk id not covered by any row
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 7, '[2015-01-01,infinity)');
ROLLBACK TO SAVEPOINT before_fail;

-- You can't insert an infinite fk range not covered by any row
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 1, '[2020-01-01,infinity)');
ROLLBACK TO SAVEPOINT before_fail;

-- You can't insert an infinite fk partially covered by one row
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 4, '[-infinity,2020-01-01)');
ROLLBACK TO SAVEPOINT before_fail;

-- You can't insert an infinite fk partially covered by two rows
INSERT INTO rooms (id, house_id, valid_range) VALUES (1, 3, '[1990-01-01,infinity)');
ROLLBACK TO SAVEPOINT before_fail;

ROLLBACK;

\i sql/include/test_teardown.sql
