\i sql/include/test_setup.sql

BEGIN;

SET ROLE TO sql_saga_unprivileged_user;

\i sql/support/shifts_houses_rooms_tables.sql
\i sql/support/shifts_houses_rooms_enable_saga.sql
\i sql/support/houses_data.sql

-- You can delete a pk with no references
DELETE FROM houses WHERE id = 2;

-- You can delete a finite pk range with no references
SAVEPOINT test;
INSERT INTO rooms VALUES (1, 1, '2016-06-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ);
DELETE FROM houses WHERE id = 1 and tstzrange(valid_from, valid_until) @> '2015-06-01'::timestamptz;
ROLLBACK TO SAVEPOINT test;

-- You can't delete a finite pk range that is partly covered
SAVEPOINT test;
INSERT INTO rooms VALUES (1, 1, '2016-01-01'::TIMESTAMPTZ, '2016-06-01'::TIMESTAMPTZ);
DELETE FROM houses WHERE id = 1 and tstzrange(valid_from, valid_until) @> '2016-06-01'::timestamptz;
ROLLBACK TO SAVEPOINT test;

-- You can't delete a finite pk range that is exactly covered
SAVEPOINT test;
INSERT INTO rooms VALUES (1, 1, '2016-01-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ);
DELETE FROM houses WHERE id = 1 and tstzrange(valid_from, valid_until) @> '2016-06-01'::timestamptz;
ROLLBACK TO SAVEPOINT test;

-- You can't delete a finite pk range that is more than covered
SAVEPOINT test;
INSERT INTO rooms VALUES (1, 1, '2015-06-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ);
DELETE FROM houses WHERE id = 1 and tstzrange(valid_from, valid_until) @> '2016-06-01'::timestamptz;
ROLLBACK TO SAVEPOINT test;

-- You can delete an infinite pk range with no references
SAVEPOINT test;
INSERT INTO rooms VALUES (1, 3, '2014-06-01'::TIMESTAMPTZ, '2015-01-01'::TIMESTAMPTZ);
DELETE FROM houses WHERE id = 3 and tstzrange(valid_from, valid_until) @> '2016-01-01'::timestamptz;
ROLLBACK TO SAVEPOINT test;

-- You can't delete an infinite pk range that is partly covered
SAVEPOINT test;
INSERT INTO rooms VALUES (1, 3, '2016-01-01'::TIMESTAMPTZ, '2017-01-01'::TIMESTAMPTZ);
DELETE FROM houses WHERE id = 3 and tstzrange(valid_from, valid_until) @> '2016-01-01'::timestamptz;
ROLLBACK TO SAVEPOINT test;

-- You can't delete an infinite pk range that is exactly covered
SAVEPOINT test;
INSERT INTO rooms VALUES (1, 3, '2015-01-01'::TIMESTAMPTZ, 'infinity');
DELETE FROM houses WHERE id = 3 and tstzrange(valid_from, valid_until) @> '2016-01-01'::timestamptz;
ROLLBACK TO SAVEPOINT test;

-- You can't delete an infinite pk range that is more than covered
SAVEPOINT test;
INSERT INTO rooms VALUES (1, 3, '2014-06-01'::TIMESTAMPTZ, 'infinity');
DELETE FROM houses WHERE id = 3 and tstzrange(valid_from, valid_until) @> '2016-01-01'::timestamptz;
ROLLBACK TO SAVEPOINT test;

ROLLBACK;

\i sql/include/test_teardown.sql
