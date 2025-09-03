\i sql/include/test_setup.sql

BEGIN;

/* Run tests as unprivileged user */
SET ROLE TO sql_saga_unprivileged_user;

/* Test default values for types that support infinity */
CREATE TABLE defaults_date (val text, valid_from date, valid_until date, valid_to date);
-- For synchronized tables, the trigger handles defaults, not the table DDL.
SELECT sql_saga.add_era('defaults_date', p_synchronize_valid_to_column => 'valid_to');
\d defaults_date
-- Test implicit default (omitting column)
INSERT INTO defaults_date (val, valid_from) VALUES ('a', '2000-01-01');
-- Test explicit NULL default
INSERT INTO defaults_date (val, valid_from, valid_until) VALUES ('b', '2001-01-01', NULL);
TABLE defaults_date ORDER BY val;
DROP TABLE defaults_date;

CREATE TABLE defaults_ts (val text, valid_from timestamptz, valid_until timestamptz);
SELECT sql_saga.add_era('defaults_ts');
\d defaults_ts
INSERT INTO defaults_ts (val, valid_from) VALUES ('a', '2000-01-01');
TABLE defaults_ts;
DROP TABLE defaults_ts;

CREATE TABLE defaults_numeric (val text, valid_from numeric, valid_until numeric);
SELECT sql_saga.add_era('defaults_numeric');
\d defaults_numeric
INSERT INTO defaults_numeric (val, valid_from) VALUES ('a', 100);
TABLE defaults_numeric;
-- This should now fail as 'infinity' is not less than 'infinity'
SAVEPOINT expect_inf_inf_fail;
INSERT INTO defaults_numeric (val, valid_from, valid_until) VALUES ('b', 'infinity', 'infinity');
ROLLBACK TO SAVEPOINT expect_inf_inf_fail;
TABLE defaults_numeric;
DROP TABLE defaults_numeric;


/* Test that -infinity is not allowed for valid_from */
CREATE TABLE no_neg_inf (val text, valid_from date, valid_until date);
SELECT sql_saga.add_era('no_neg_inf');
\d no_neg_inf
-- This should fail due to the new check constraint
SAVEPOINT expect_neg_inf_fail;
INSERT INTO no_neg_inf (val, valid_from, valid_until) VALUES ('c', '-infinity', '2000-01-01');
ROLLBACK TO SAVEPOINT expect_neg_inf_fail;
DROP TABLE no_neg_inf;


/* Test that no default is set for types that do not support infinity */
CREATE TABLE defaults_int (val text, valid_from int, valid_until int);
SELECT sql_saga.add_era('defaults_int');
\d defaults_int
-- This should fail because valid_until is NOT NULL and has no default
SAVEPOINT expect_fail;
INSERT INTO defaults_int (val, valid_from) VALUES ('a', 1);
ROLLBACK TO SAVEPOINT expect_fail;
TABLE defaults_int;
DROP TABLE defaults_int;

/* Test optional flags */
CREATE TABLE flags_test (val text, valid_from date, valid_until date);
-- No defaults
SELECT sql_saga.add_era('flags_test', p_add_defaults => false);
\d flags_test
-- This should fail because there is no default
SAVEPOINT no_default_fail;
INSERT INTO flags_test (val, valid_from) VALUES ('a', '2000-01-01');
ROLLBACK TO SAVEPOINT no_default_fail;
-- No check
SELECT sql_saga.drop_era('flags_test');
-- Recreate the table to ensure the old CHECK constraint is gone.
DROP TABLE flags_test;
CREATE TABLE flags_test (val text, valid_from date, valid_until date);
SELECT sql_saga.add_era('flags_test', p_add_bounds_check => false);
\d flags_test
INSERT INTO flags_test VALUES ('a', '2020-01-01', '2000-01-01'); -- should succeed
TABLE flags_test;
DROP TABLE flags_test;

ROLLBACK;

\i sql/include/test_teardown.sql
