\i sql/include/test_setup.sql

BEGIN;

/* Run tests as unprivileged user */
SET ROLE TO sql_saga_unprivileged_user;

/* Test default values for types that support infinity */
CREATE TABLE defaults_date (val text, valid_range daterange, valid_from date, valid_to date, valid_until date);
-- For synchronized tables, the trigger handles defaults, not the table DDL.
SELECT sql_saga.add_era('defaults_date', 'valid_range');
\d defaults_date
-- Test implicit default (omitting column)
INSERT INTO defaults_date (val, valid_range) VALUES ('a', daterange('2000-01-01', NULL));
-- Test explicit NULL default
INSERT INTO defaults_date (val, valid_range) VALUES ('b', daterange('2001-01-01', 'infinity'));
TABLE defaults_date ORDER BY val;
DROP TABLE defaults_date;

CREATE TABLE defaults_ts (val text, valid_range tstzrange, valid_from timestamptz, valid_until timestamptz);
SELECT sql_saga.add_era('defaults_ts','valid_range');
\d defaults_ts
INSERT INTO defaults_ts (val, valid_range) VALUES ('a', tstzrange('2000-01-01', NULL));
TABLE defaults_ts;
DROP TABLE defaults_ts;

CREATE TABLE defaults_numeric (val text, valid_range numrange, valid_from numeric, valid_until numeric);
SELECT sql_saga.add_era('defaults_numeric', 'valid_range');
\d defaults_numeric
INSERT INTO defaults_numeric (val, valid_range) VALUES ('a', numrange(100, NULL));
TABLE defaults_numeric;
-- This should fail because numrange('infinity', 'infinity') creates an empty range
-- The trigger now explicitly checks for empty ranges with a clear error message
SAVEPOINT expect_inf_inf_fail;
INSERT INTO defaults_numeric (val, valid_range) VALUES ('b', numrange('infinity', 'infinity'));
ROLLBACK TO SAVEPOINT expect_inf_inf_fail;
TABLE defaults_numeric;
DROP TABLE defaults_numeric;


/* Test that -infinity is not allowed for valid_from */
CREATE TABLE no_neg_inf (val text, valid_range daterange, valid_from date, valid_until date);
SELECT sql_saga.add_era('no_neg_inf', 'valid_range');
\d no_neg_inf
-- This should fail due to the new check constraint
SAVEPOINT expect_neg_inf_fail;
INSERT INTO no_neg_inf (val, valid_range) VALUES ('c', daterange('-infinity', '2000-01-01'));
ROLLBACK TO SAVEPOINT expect_neg_inf_fail;
DROP TABLE no_neg_inf;


/* Test that no default is set for types that do not support infinity */
CREATE TABLE defaults_int (val text, valid_range int4range, valid_from int, valid_until int);
SELECT sql_saga.add_era('defaults_int', 'valid_range');
\d defaults_int
-- This should fail because valid_until is NOT NULL and has no default
SAVEPOINT expect_fail;
INSERT INTO defaults_int (val, valid_range) VALUES ('a', int4range(1, NULL));
ROLLBACK TO SAVEPOINT expect_fail;
TABLE defaults_int;
DROP TABLE defaults_int;



/* Test optional flags */
CREATE TABLE flags_test (val text, valid_range daterange, valid_from date, valid_until date);

-- Test with no defaults
SAVEPOINT test_no_defaults;
SELECT sql_saga.add_era('flags_test', 'valid_range', add_defaults => false);
\d flags_test
-- This should fail because there is no default
SAVEPOINT no_default_fail;
INSERT INTO flags_test (val, valid_range) VALUES ('a', daterange('2000-01-01', NULL));
ROLLBACK TO SAVEPOINT no_default_fail;
ROLLBACK TO SAVEPOINT test_no_defaults;

-- Test with no bounds check
SAVEPOINT test_no_bounds_check;
SELECT sql_saga.add_era('flags_test', 'valid_range', add_bounds_check => false);
\d flags_test
-- Range types have built-in validation that prevents invalid bounds
-- This should fail with range validation error (even with add_bounds_check => false)
SAVEPOINT expect_range_error;
INSERT INTO flags_test (val, valid_range) VALUES ('a', daterange('2020-01-01', '2000-01-01'));
ROLLBACK TO SAVEPOINT expect_range_error;
-- Insert a valid range to verify the table works
INSERT INTO flags_test (val, valid_range) VALUES ('a', daterange('2000-01-01', '2020-01-01'));
TABLE flags_test;
ROLLBACK TO SAVEPOINT test_no_bounds_check;

DROP TABLE flags_test;

ROLLBACK;

\i sql/include/test_teardown.sql
