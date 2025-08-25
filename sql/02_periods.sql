\i sql/include/test_setup.sql

BEGIN;

/* Run tests as unprivileged user */
SET ROLE TO sql_saga_unprivileged_user;

/* Basic period definitions with dates */
CREATE TABLE basic (val text, valid_from date, valid_until date);
TABLE sql_saga.era;
SELECT sql_saga.add_era('basic', 'valid_from', 'valid_until', 'bp');
TABLE sql_saga.era;
SELECT sql_saga.drop_era('basic', 'bp');
TABLE sql_saga.era;
SELECT sql_saga.add_era('basic', 'valid_from', 'valid_until', 'bp', bounds_check_constraint => 'c');
TABLE sql_saga.era;
SELECT sql_saga.drop_era('basic', 'bp', cleanup => true);
TABLE sql_saga.era;
SELECT sql_saga.add_era('basic', 'valid_from', 'valid_until', 'bp');
TABLE sql_saga.era;
/* Test constraints */
SAVEPOINT pristine;
INSERT INTO basic (val, valid_from, valid_until) VALUES ('x', null, null); --fail
ROLLBACK TO SAVEPOINT pristine;
INSERT INTO basic (val, valid_from, valid_until) VALUES ('x', '3000-01-01', null); --fail
ROLLBACK TO SAVEPOINT pristine;
INSERT INTO basic (val, valid_from, valid_until) VALUES ('x', null, '1000-01-01'); --fail
ROLLBACK TO SAVEPOINT pristine;
INSERT INTO basic (val, valid_from, valid_until) VALUES ('x', '3000-01-01', '1000-01-01'); --fail
ROLLBACK TO SAVEPOINT pristine;
INSERT INTO basic (val, valid_from, valid_until) VALUES ('x', '1000-01-01', '3000-01-01'); --success
TABLE basic;
/* Test dropping the whole thing */
DROP TABLE basic;
TABLE sql_saga.era;

ROLLBACK;

\i sql/include/test_teardown.sql

