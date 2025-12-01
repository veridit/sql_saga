\i sql/include/test_setup.sql

-- Test Scenario 1: Add constraint to an empty table, then insert data.
-- ====================================================================

CREATE TABLE employees (
    id int,
    name text,
    department_id int,
    valid_range daterange,
    valid_from date,
    valid_until date
);

SELECT sql_saga.add_era('employees', 'valid_range');

-- Add a unique key with a predicate.
SELECT sql_saga.add_unique_key(
    table_oid => 'employees',
    column_names => ARRAY['name'],
    key_type => 'predicated',
    predicate => 'department_id IS NOT NULL',
    unique_key_name => 'employees_name_if_department_valid'
);
\d employees

-- 1. Inserts that should PASS
-- Two employees with same name, but NULL department (outside predicate)
INSERT INTO employees(id,name,department_id,valid_from,valid_until) VALUES (1, 'Alice', NULL, '2020-01-01', 'infinity');
INSERT INTO employees(id,name,department_id,valid_from,valid_until) VALUES (2, 'Alice', NULL, '2021-01-01', 'infinity');
-- Two employees with different names in the same department
INSERT INTO employees(id,name,department_id,valid_from,valid_until) VALUES (3, 'Bob', 101, '2020-01-01', 'infinity');
INSERT INTO employees(id,name,department_id,valid_from,valid_until) VALUES (4, 'Charlie', 101, '2021-01-01', 'infinity');

-- 2. Inserts that should FAIL (due to temporal overlap)
-- Overlapping period for an existing employee in a department
INSERT INTO employees(id,name,department_id,valid_from,valid_until) VALUES (5, 'Bob', 101, '2020-06-01', '2021-06-01');

-- 3. Inserts that should FAIL (due to exact duplicate record)
INSERT INTO employees(id,name,department_id,valid_from,valid_until) VALUES (6, 'Charlie', 101, '2021-01-01', 'infinity');

SELECT id, name, department_id, valid_range, valid_from, valid_until FROM employees ORDER BY id;


-- 4. Drop the predicated unique key
SELECT sql_saga.drop_unique_key('employees'::regclass, ARRAY['name']::name[], 'valid');
-- Verify that the unique key and its constraints/indexes are gone
SELECT unique_key_name FROM sql_saga.unique_keys WHERE table_schema = 'public' AND table_name = 'employees';
\d employees


-- Test Scenario 2: Add constraint to a table with existing data.
-- ===============================================================

CREATE TABLE contractors (
    id int,
    name text,
    agency_id int,
    valid_range daterange,
    valid_from date,
    valid_until date
);
SELECT sql_saga.add_era('contractors', 'valid_range');

-- Insert initial data, including data that will violate the future constraint.
-- Two 'David' records in agency 202 have overlapping periods.
INSERT INTO contractors(id,name,agency_id,valid_from,valid_until) VALUES (101, 'David', 202, '2020-01-01', '2022-01-01');
INSERT INTO contractors(id,name,agency_id,valid_from,valid_until) VALUES (102, 'David', 202, '2021-01-01', '2023-01-01');
-- This is fine, NULL agency is outside predicate.
INSERT INTO contractors(id,name,agency_id,valid_from,valid_until) VALUES (103, 'Eve', NULL, '2020-01-01', 'infinity');
INSERT INTO contractors(id,name,agency_id,valid_from,valid_until) VALUES (104, 'Eve', NULL, '2021-01-01', 'infinity');

-- Attempt to add the unique key. This should fail due to the overlapping 'David' records.
SELECT sql_saga.add_unique_key(
    table_oid => 'contractors',
    column_names => ARRAY['name'],
    key_type => 'predicated',
    predicate => 'agency_id IS NOT NULL'
);

-- Correct the data by deleting one of the conflicting records.
DELETE FROM contractors WHERE id = 102;

-- Try adding the unique key again. This should now succeed.
SELECT sql_saga.add_unique_key(
    table_oid => 'contractors',
    column_names => ARRAY['name'],
    key_type => 'predicated',
    predicate => 'agency_id IS NOT NULL',
    unique_key_name => 'contractors_name_if_agency_valid'
);

-- Test the new constraint with an insert that should fail.
INSERT INTO contractors(id,name,agency_id,valid_from,valid_until) VALUES (105, 'David', 202, '2021-01-01', '2023-01-01');

SELECT id, name, agency_id, valid_range, valid_from, valid_until FROM contractors ORDER BY id;

DROP TABLE employees;
DROP TABLE contractors;

\i sql/include/test_teardown.sql
