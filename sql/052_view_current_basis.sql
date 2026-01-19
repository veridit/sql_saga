\i sql/include/test_setup.sql

BEGIN;

-- A stable function to override now() for deterministic testing
CREATE FUNCTION test_now() RETURNS date AS $$ SELECT '2024-02-29'::date $$ LANGUAGE sql;

CREATE TABLE employees (
    id int,
    valid_range daterange,
    valid_from date,
    valid_until date,
    name text,
    department text
);

-- Register era and unique key
SELECT sql_saga.add_era('employees', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('employees', ARRAY['id']);

-- Populate with some data
INSERT INTO employees (id, valid_from, valid_until, name, department) VALUES
(1, '2023-01-01', '2024-01-01', 'Alice', 'Engineering'),
(1, '2024-01-01', 'infinity', 'Alice', 'R&D'),
(2, '2023-05-01', 'infinity', 'Bob', 'Sales');

TABLE employees ORDER BY id, valid_from;

-- Test API Lifecycle
-- ==================

-- Add the current view, overriding the now() function for stable tests
SELECT sql_saga.add_current_view('employees'::regclass, current_func_name := 'test_now()');
\d employees__current_valid
TABLE sql_saga.updatable_view;

-- Test Error Handling for non-time-based eras
SAVEPOINT before_failure_setup;
CREATE TABLE widgets (id int, valid_range int4range, valid_from int, valid_until int);
SELECT sql_saga.add_era('widgets', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SAVEPOINT expect_fail;
-- This will fail because the era is not time-based, which is the point of the test.
SELECT sql_saga.add_current_view('widgets'::regclass, current_func_name := 'test_now()');
ROLLBACK TO expect_fail;
ROLLBACK TO before_failure_setup;

-- Test DML Semantics
-- ==================

-- Test SELECT
-- Should show only currently active records and hide temporal columns.
-- Only Alice (R&D) and Bob should be visible.
CREATE VIEW employees_current_view_select AS
TABLE employees__current_valid ORDER BY id;
TABLE employees_current_view_select;

-- Test INSERT (SCD Type 2)
-- Carol joins the company
INSERT INTO employees__current_valid (id, name, department)
VALUES (3, 'Carol', 'Marketing');

-- The new record should be visible in the base table with a recent timestamp,
-- and the view should now show Carol.
TABLE employees ORDER BY id, valid_from;
TABLE employees_current_view_select;

-- Test UPDATE (SCD Type 2)
-- Bob moves from Sales to Management
UPDATE employees__current_valid SET department = 'Management' WHERE id = 2;

-- The old record for Bob should be closed out, and a new one created.
-- The view should show Bob in Management.
TABLE employees ORDER BY id, valid_from;
TABLE employees_current_view_select;

-- Test DELETE (Soft delete)
-- Alice leaves the company. With the default 'delete_as_cutoff' mode, a
-- standard DELETE performs a soft-delete.
DELETE FROM employees__current_valid WHERE id = 1;

-- Alice's current record should be closed out. She should no longer be in the current view.
TABLE employees ORDER BY id, valid_from;
TABLE employees_current_view_select;

-- Drop the dependent view first
DROP VIEW employees_current_view_select;

-- Drop the view
SELECT sql_saga.drop_current_view('employees'::regclass);
TABLE sql_saga.updatable_view;

-- The view should no longer exist.
SAVEPOINT view_is_gone;
SELECT * FROM employees__current_valid;
ROLLBACK TO view_is_gone;

-- Test Error Handling for missing era
-- ====================================
SAVEPOINT missing_era_test;

CREATE TABLE no_era_table (
    id int,
    value text,
    valid_range daterange
);

-- Should fail: no era registered on this table
SAVEPOINT expect_error_1;
SELECT sql_saga.add_current_view('no_era_table'::regclass);
ROLLBACK TO SAVEPOINT expect_error_1;

-- Should also fail with explicit non-existent era name
SAVEPOINT expect_error_2;
SELECT sql_saga.add_current_view('no_era_table'::regclass, 'nonexistent');
ROLLBACK TO SAVEPOINT expect_error_2;

ROLLBACK TO missing_era_test;

ROLLBACK;

\i sql/include/test_teardown.sql
