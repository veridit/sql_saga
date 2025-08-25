\i sql/include/test_setup.sql

SET ROLE TO sql_saga_unprivileged_user;

\i sql/support/shifts_houses_rooms_tables.sql
\i sql/support/shifts_houses_rooms_enable_saga.sql

-- This test uses COMMIT, so it cannot be wrapped in a single transaction.
-- Manual cleanup is required.

\i sql/support/houses_data.sql

-- You can update a finite pk id with no references
BEGIN;
UPDATE houses SET id = 4 WHERE id = 2;
ABORT;

-- You can update a finite pk range with no references
BEGIN;
UPDATE houses SET valid_from = '1999-01-01', valid_until = '2000-01-01' WHERE id = 1 AND daterange(valid_from, valid_until) @> '2015-06-01'::DATE;
UPDATE houses SET valid_from = '2015-01-01', valid_until = '2016-01-01' WHERE id = 1 AND daterange(valid_from, valid_until) @> '1999-06-01'::DATE;
ABORT;

-- You can update a finite pk range that is partly covered elsewhere
BEGIN;
INSERT INTO rooms VALUES (1, 1, '2016-01-01', '2016-06-01');
UPDATE houses SET valid_from = '2016-01-01', valid_until = '2016-09-01' WHERE id = 1 AND daterange(valid_from, valid_until) @> '2016-06-01'::DATE;
UPDATE houses SET valid_from = '2016-01-01', valid_until = '2017-01-01' WHERE id = 1 AND daterange(valid_from, valid_until) @> '2016-06-01'::DATE;
ABORT;

-- You can't update a finite pk id that is partly covered
BEGIN;
INSERT INTO rooms VALUES (1, 1, '2016-01-01', '2016-06-01');
-- The following UPDATE should fail.
UPDATE houses SET id = 4 WHERE id = 1;
ABORT;

-- You can't update a finite pk range that is partly covered
BEGIN;
INSERT INTO rooms VALUES (1, 1, '2016-01-01', '2016-06-01');
-- The following UPDATE should fail.
UPDATE houses SET (valid_from, valid_until) = ('2017-01-01', '2018-01-01') WHERE id = 1 AND daterange(valid_from, valid_until) @> '2016-06-01'::DATE;
ABORT;

-- You can't update a finite pk id that is exactly covered
BEGIN;
-- The following INSERT should pass.
INSERT INTO rooms VALUES (1, 1, '2016-01-01', '2017-01-01');
-- The following UPDATE should fail.
UPDATE houses SET id = 4 WHERE id = 1;
ABORT;

-- You can't update a finite pk range that is exactly covered
BEGIN;
-- The following INSERT should pass.
INSERT INTO rooms VALUES (1, 1, '2016-01-01', '2017-01-01');
-- The following UPDATE should fail.
UPDATE houses SET (valid_from, valid_until) = ('2017-01-01', '2018-01-01') WHERE id = 1 AND daterange(valid_from, valid_until) @> '2016-06-01'::DATE;
ABORT;

-- You can't update a finite pk id that is more than covered
BEGIN;
-- The following INSERT should pass.
INSERT INTO rooms VALUES (1, 1, '2015-06-01', '2017-01-01');
-- The following UPDATE should fail.
UPDATE houses SET id = 4 WHERE id = 1;
ABORT;

-- You can't update a finite pk range that is more than covered
BEGIN;
-- The following INSERT should pass.
INSERT INTO rooms VALUES (1, 1, '2015-06-01', '2017-01-01');
-- The following UPDATE should fail.
UPDATE houses SET (valid_from, valid_until) = ('2017-01-01', '2018-01-01') WHERE id = 1 AND daterange(valid_from, valid_until) @> '2016-06-01'::DATE;
ABORT;

-- You can update an infinite pk id with no references
BEGIN;
INSERT INTO rooms VALUES (1, 3, '2014-06-01', '2015-01-01');
UPDATE houses SET id = 4 WHERE id = 3 and daterange(valid_from, valid_until) @> '2016-01-01'::DATE;
UPDATE houses SET id = 3 WHERE id = 4;
ABORT;

-- You can update an infinite pk range with no references
BEGIN;
INSERT INTO rooms VALUES (1, 3, '2014-06-01', '2015-01-01');
UPDATE houses SET (valid_from, valid_until) = ('2017-01-01', '2018-01-01') WHERE id = 3 and daterange(valid_from, valid_until) @> '2016-01-01'::DATE;
UPDATE houses SET (valid_from, valid_until) = ('2015-01-01', 'infinity') WHERE id = 3 and daterange(valid_from, valid_until) @> '2017-06-01'::DATE;
ABORT;

-- You can't update an infinite pk id that is partly covered
BEGIN;
INSERT INTO rooms VALUES (1, 3, '2016-01-01', '2017-01-01');
UPDATE houses SET id = 4 WHERE id = 3 and daterange(valid_from, valid_until) @> '2016-01-01'::DATE;
ABORT;

-- You can't update an infinite pk range that is partly covered
BEGIN;
-- The following INSERT should pass.
INSERT INTO rooms VALUES (1, 3, '2016-01-01', '2017-01-01');
-- The following UPDATE should fail.
UPDATE houses SET (valid_from, valid_until) = ('2017-01-01', '2018-01-01') WHERE id = 3 and daterange(valid_from, valid_until) @> '2016-01-01'::DATE;
ABORT;

-- You can't update an infinite pk id that is exactly covered
BEGIN;
INSERT INTO rooms VALUES (1, 3, '2015-01-01', 'infinity');
UPDATE houses SET id = 4 WHERE id = 3 and daterange(valid_from, valid_until) @> '2016-01-01'::DATE;
ABORT;

-- You can't update an infinite pk range that is exactly covered
BEGIN;
INSERT INTO rooms VALUES (1, 3, '2015-01-01', 'infinity');
UPDATE  houses SET (valid_from, valid_until) = ('2017-01-01', '2018-01-01') WHERE id = 3 and daterange(valid_from, valid_until) @> '2016-01-01'::DATE;
ABORT;

-- You can't update an infinite pk id that is more than covered
BEGIN;
INSERT INTO rooms VALUES (1, 3, '2014-06-01', 'infinity');
UPDATE houses SET id = 4 WHERE id = 3 and daterange(valid_from, valid_until) @> '2016-01-01'::DATE;
ABORT;

-- You can't update an infinite pk range that is more than covered
BEGIN;
INSERT INTO rooms VALUES (1, 3, '2014-06-01', 'infinity');
UPDATE houses SET (valid_from, valid_until) = ('2017-01-01', '2018-01-01') WHERE id = 3 and daterange(valid_from, valid_until) @> '2016-01-01'::DATE;
ABORT;

-- Manual Cleanup
DROP TABLE rooms;
DROP TABLE houses;
DROP TABLE shifts;

\i sql/include/test_teardown.sql
