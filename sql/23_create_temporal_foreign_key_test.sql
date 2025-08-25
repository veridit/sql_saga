\i sql/include/test_setup.sql

BEGIN;

SET ROLE TO sql_saga_unprivileged_user;

\i sql/support/shifts_houses_rooms_tables.sql

INSERT INTO houses VALUES
  (1, 150000, '2015-01-01'::DATE, '2016-01-01'::DATE),
  (1, 200000, '2016-01-01'::DATE, '2017-01-01'::DATE)
;

-- it works on an empty table
SAVEPOINT s;
\i sql/support/shifts_houses_rooms_enable_saga.sql
ROLLBACK TO SAVEPOINT s;

-- it works on a table with a NULL foreign key
SAVEPOINT s;
INSERT INTO rooms(id,house_id,valid_from,valid_until) VALUES (1, NULL, '2015-01-01'::DATE, '2017-01-01'::DATE);
\i sql/support/shifts_houses_rooms_enable_saga.sql
ROLLBACK TO SAVEPOINT s;

-- it works on a table with a FK fulfilled by one row
SAVEPOINT s;
INSERT INTO rooms(id,house_id,valid_from,valid_until) VALUES (1, 1, '2015-01-01'::DATE, '2016-01-01'::DATE);
\i sql/support/shifts_houses_rooms_enable_saga.sql
ROLLBACK TO SAVEPOINT s;

-- it works on a table with a FK fulfilled by two rows
SAVEPOINT s;
INSERT INTO rooms(id,house_id,valid_from,valid_until) VALUES (1, 1, '2015-01-01'::DATE, '2016-06-01'::DATE);
\i sql/support/shifts_houses_rooms_enable_saga.sql
ROLLBACK TO SAVEPOINT s;

-- it fails on a table with a missing foreign key
SAVEPOINT s;
INSERT INTO rooms(id,house_id,valid_from,valid_until) VALUES (1, 2, '2015-01-01'::DATE, '2016-01-01'::DATE);
\i sql/support/shifts_houses_rooms_enable_saga.sql
ROLLBACK TO SAVEPOINT s;

-- it fails on a table with a completely-uncovered foreign key
SAVEPOINT s;
INSERT INTO rooms(id,house_id,valid_from,valid_until) VALUES (1, 1, '2010-01-01'::DATE, '2011-01-01'::DATE);
\i sql/support/shifts_houses_rooms_enable_saga.sql
ROLLBACK TO SAVEPOINT s;

-- it fails on a table with a partially-covered foreign key
SAVEPOINT s;
INSERT INTO rooms(id,house_id,valid_from,valid_until) VALUES (1, 1, '2015-01-01'::DATE, '2018-01-01'::DATE);
\i sql/support/shifts_houses_rooms_enable_saga.sql
ROLLBACK TO SAVEPOINT s;

ROLLBACK;

\i sql/include/test_teardown.sql
