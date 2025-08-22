-- Use a blank search path, so every table must
-- be prefixed with a schema
SELECT pg_catalog.set_config('search_path', '', false);

CREATE EXTENSION sql_saga CASCADE;

CREATE SCHEMA exposed;
CREATE SCHEMA hidden;

CREATE TABLE exposed.employees (
  id INTEGER,
  valid_from date,
  valid_to date,
  valid_until date,
  name varchar NOT NULL,
  role varchar NOT NULL
);

CREATE TABLE hidden.staff (
  id INTEGER,
  valid_from date,
  valid_to date,
  valid_until date,
  salary FLOAT,
  employee_id INTEGER
);

CREATE TRIGGER synchronize_employees_validity BEFORE INSERT OR UPDATE ON exposed.employees
    FOR EACH ROW EXECUTE FUNCTION sql_saga.synchronize_valid_to_until();
CREATE TRIGGER synchronize_staff_validity BEFORE INSERT OR UPDATE ON hidden.staff
    FOR EACH ROW EXECUTE FUNCTION sql_saga.synchronize_valid_to_until();

-- Before using sql_saga
\d exposed.employees
\d hidden.staff

-- Verify that enable and disable each work correctly.
SELECT sql_saga.add_era('exposed.employees', 'valid_from', 'valid_until');
SELECT sql_saga.add_era('hidden.staff', 'valid_from', 'valid_until');
TABLE sql_saga.era;

SELECT sql_saga.add_unique_key('exposed.employees', ARRAY['id'], 'valid');
SELECT sql_saga.add_unique_key('hidden.staff', ARRAY['id'], 'valid');
TABLE sql_saga.unique_keys;

SELECT sql_saga.add_foreign_key('hidden.staff', ARRAY['employee_id'], 'valid', 'employees_id_valid');
TABLE sql_saga.foreign_keys;

-- While sql_saga is active
\d exposed.employees
\d hidden.staff

-- Test data.
-- We only insert valid_from and valid_to, the trigger will set valid_after.
INSERT INTO exposed.employees (id, valid_from, valid_to, name, role) VALUES
(101, '2022-01-01', '2022-06-30', 'Alice Johnson', 'Junior Manager'),
(101, '2022-07-01', '2023-12-31', 'Alice Johnson', 'Senior Manager'),
(102, '2022-01-01', '2022-08-31', 'Bob Smith', 'Junior Engineer'),
(102, '2022-09-01', '2023-12-31', 'Bob Smith', 'Senior Engineer'),
(103, '2022-01-01', '2022-12-31', 'Charlie Brown', 'Designer'),
(104, '2022-01-01', '2022-05-31', 'Diana Prince', 'Junior Analyst'),
(104, '2022-06-01', '2023-12-31', 'Diana Prince', 'Senior Analyst');

INSERT INTO hidden.staff (id, valid_from, valid_to, employee_id, salary) VALUES
(201, '2022-01-01', '2022-07-31',101 , 50000.00),
(201, '2022-08-01', '2023-12-31',101 , 60000.00), -- Salary increase in August, a month after role change in July
(202, '2022-01-01', '2022-09-30',102 , 55000.00),
(202, '2022-10-01', '2023-12-31',102 , 70000.00), -- Salary increase in October, a month after role change in September
(203, '2022-01-01', '2022-12-31',103 , 48000.00),
(204, '2022-01-01', '2022-06-30',104 , 45000.00),
(204, '2022-07-01', '2023-12-31',104 , 55000.00); -- Salary increase in July, a month after role change in June


-- Fail
DELETE FROM exposed.employees WHERE id = 101;

-- Success
DELETE FROM hidden.staff WHERE employee_id = 101;
DELETE FROM exposed.employees WHERE id = 101;

-- Fail
UPDATE hidden.staff SET valid_until = 'infinity' WHERE employee_id = 103;

BEGIN;
-- Regression
SAVEPOINT regression;
TABLE hidden.staff;
UPDATE hidden.staff SET valid_to = 'infinity' WHERE employee_id = 103;
TABLE hidden.staff;
ABORT;

-- Success
UPDATE exposed.employees SET valid_to = 'infinity' WHERE id = 103;
UPDATE hidden.staff SET valid_to = 'infinity' WHERE employee_id = 103;

-- Teardown

SELECT sql_saga.drop_foreign_key('hidden.staff', 'staff_employee_id_valid');
TABLE sql_saga.foreign_keys;

SELECT sql_saga.drop_unique_key('exposed.employees', 'employees_id_valid');
SELECT sql_saga.drop_unique_key('hidden.staff','staff_id_valid');
TABLE sql_saga.unique_keys;

SELECT sql_saga.drop_era('exposed.employees');
SELECT sql_saga.drop_era('hidden.staff');
TABLE sql_saga.era;

-- After removing sql_saga, it should be as before.
\d exposed.employees
\d hidden.staff

DROP TABLE exposed.employees;
DROP TABLE hidden.staff;

DROP EXTENSION sql_saga;
DROP EXTENSION btree_gist;
