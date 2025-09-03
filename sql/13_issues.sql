\i sql/include/test_setup.sql

BEGIN;

/* Run tests as unprivileged user */
SET ROLE TO sql_saga_unprivileged_user;

/* https://github.com/xocolatl/periods/issues/27 */

CREATE TABLE uk(id integer, f integer, u integer);
SELECT sql_saga.add_era('uk', 'f', 'u', 'p');
SELECT sql_saga.add_unique_key('uk', ARRAY['id'], 'p');

CREATE TABLE fk(id integer, uk_id integer, f integer, u integer);
SELECT sql_saga.add_era('fk', 'f', 'u', 'q');
SELECT sql_saga.add_unique_key('fk', ARRAY['id'], 'q');
SELECT sql_saga.add_foreign_key('fk', ARRAY['uk_id'], 'q', 'uk_id_p');
--
TABLE sql_saga.era;
TABLE sql_saga.foreign_keys;

--
INSERT INTO uk(id, f, u)        VALUES    (1, 1, 3),    (1, 3, 5);
INSERT INTO fk(id, uk_id, f, u) VALUES (1, 1, 1, 2), (2, 1, 2, 5);

-- Make sure the data is there before we start deleting
TABLE uk;
TABLE fk;

--expected: fail
SAVEPOINT s1;
DELETE FROM uk WHERE (id, f, u) = (1, 1, 3);
ROLLBACK TO SAVEPOINT s1;

TABLE uk;
TABLE fk;

--expected: fail
SAVEPOINT s2;
DELETE FROM uk WHERE (id, f, u) = (1, 3, 5);
ROLLBACK TO SAVEPOINT s2;

INSERT INTO uk(id, f, u)        VALUES    (2, 1, 5);
INSERT INTO fk(id, uk_id, f, u) VALUES (4, 2, 2, 4);

TABLE uk;
TABLE fk;

--expected: fail
SAVEPOINT s3;
UPDATE uk SET u = 3 WHERE (id, f, u) = (2, 1, 5);
ROLLBACK TO SAVEPOINT s3;

TABLE uk;
TABLE fk;

-- Create non contiguous time
INSERT INTO uk(id, f, u)        VALUES    (3, 1, 3),
                                          (3, 4, 5);

-- Reference over non contiguous time - should fail
SAVEPOINT s4;
INSERT INTO fk(id, uk_id, f, u) VALUES (5, 3, 1, 5);
ROLLBACK TO SAVEPOINT s4;


-- Create overlappig range - should fail
SAVEPOINT s5;
INSERT INTO uk(id, f, u)        VALUES    (4, 1, 4),
                                          (4, 3, 5);
ROLLBACK TO SAVEPOINT s5;

SELECT sql_saga.drop_foreign_key('fk', ARRAY['uk_id'], 'q');
SELECT sql_saga.drop_unique_key('fk', ARRAY['id'], 'q');
SELECT sql_saga.drop_era('fk', 'q');
DROP TABLE fk;

SELECT sql_saga.drop_unique_key('uk', ARRAY['id'], 'p');
SELECT sql_saga.drop_era('uk', 'p');
DROP TABLE uk;

-- Test for issue with duplicated column in UPDATE OF list for FK trigger
CREATE TABLE t_uk (
    id int,
    valid_after date,
    valid_to date
);
SELECT sql_saga.add_era('t_uk', 'valid_after', 'valid_to');
SELECT sql_saga.add_unique_key('t_uk', ARRAY['id']);

CREATE TABLE t_fk (
    id int,
    uk_id int,
    valid_after date,
    valid_to date
);
SELECT sql_saga.add_era('t_fk', 'valid_after', 'valid_to');

-- This call failed in a downstream project because it generated:
-- CREATE ... TRIGGER ... AFTER UPDATE OF uk_id, valid_after, valid_to, valid_to ON ...
-- The bug is that 'valid_to' is added to the list twice if it's both the
-- valid_until_column_name and a column named 'valid_to' exists.
SAVEPOINT s_fk_error;
SELECT sql_saga.add_foreign_key('t_fk', ARRAY['uk_id'], 'valid', 't_uk_id_valid');
ROLLBACK TO SAVEPOINT s_fk_error;

-- cleanup
-- Since the transaction for creating the FK was rolled back, we can just
-- clean up the tables and eras.
SELECT sql_saga.drop_era('t_fk');
DROP TABLE t_fk;
SELECT sql_saga.drop_unique_key('t_uk', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('t_uk');
DROP TABLE t_uk;


-- Test generic synchronized column for FK trigger
CREATE TABLE t_uk_gen (
    id int,
    valid_from date,
    valid_until date,
    valid_end_date date -- synchronized column
);
SELECT sql_saga.add_era('t_uk_gen', p_synchronize_valid_to_column := 'valid_end_date');
SELECT sql_saga.add_unique_key('t_uk_gen', ARRAY['id']);

CREATE TABLE t_fk_gen (
    id int,
    uk_id int,
    valid_from date,
    valid_until date,
    valid_end_date date -- synchronized column
);
SELECT sql_saga.add_era('t_fk_gen', p_synchronize_valid_to_column := 'valid_end_date');

-- This should succeed and create a trigger that watches valid_end_date
SELECT sql_saga.add_foreign_key('t_fk_gen', ARRAY['uk_id'], 'valid', 't_uk_gen_id_valid');

-- cleanup
SELECT sql_saga.drop_foreign_key('t_fk_gen', ARRAY['uk_id'], 'valid');
SELECT sql_saga.drop_era('t_fk_gen');
DROP TABLE t_fk_gen;
SELECT sql_saga.drop_unique_key('t_uk_gen', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('t_uk_gen');
DROP TABLE t_uk_gen;


-- Test case for bug with infinite parent validity
CREATE TABLE legal_unit_bug (
    id INT NOT NULL,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL,
    name TEXT,
    PRIMARY KEY (id, valid_from)
);
SELECT sql_saga.add_era('legal_unit_bug', 'valid_from', 'valid_until');
SELECT sql_saga.add_unique_key('legal_unit_bug', ARRAY['id']);

CREATE TABLE establishment_bug (
    id INT NOT NULL,
    legal_unit_id INT, -- Temporal FK
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL,
    name TEXT,
    PRIMARY KEY (id, valid_from)
);
SELECT sql_saga.add_era('establishment_bug', 'valid_from', 'valid_until');
-- Note: A unique key on 'id' is not strictly necessary for this test but good practice.
SELECT sql_saga.add_unique_key('establishment_bug', ARRAY['id']); 

-- Add the temporal foreign key constraint
SELECT sql_saga.add_foreign_key('establishment_bug', ARRAY['legal_unit_id'], 'valid', 'legal_unit_bug_id_valid');

INSERT INTO legal_unit_bug (id, valid_from, valid_until, name) VALUES
(1, '2023-12-31', 'infinity', 'Parent LU');

-- This should succeed, as the child's validity is fully contained within the parent's.
INSERT INTO establishment_bug (id, legal_unit_id, valid_from, valid_until, name) VALUES
(101, 1, '2024-01-01', '2025-01-01', 'Child EST');

-- Verify insert
SELECT * FROM establishment_bug;

SELECT sql_saga.drop_foreign_key('establishment_bug', ARRAY['legal_unit_id'], 'valid');
SELECT sql_saga.drop_unique_key('establishment_bug', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('establishment_bug', 'valid');
DROP TABLE establishment_bug;

SELECT sql_saga.drop_unique_key('legal_unit_bug', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('legal_unit_bug', 'valid');
DROP TABLE legal_unit_bug;

ROLLBACK;

\i sql/include/test_teardown.sql
