/* Run tests as unprivileged user */
SET ROLE TO sql_saga_unprivileged_user;

/* https://github.com/xocolatl/periods/issues/27 */

CREATE TABLE uk(id integer, s integer, e integer);
SELECT sql_saga.add_era('uk', 's', 'e', 'p');
SELECT sql_saga.add_unique_key('uk', ARRAY['id'], 'p');

CREATE TABLE fk(id integer, uk_id integer, s integer, e integer);
SELECT sql_saga.add_era('fk', 's', 'e', 'q');
SELECT sql_saga.add_unique_key('fk', ARRAY['id'], 'q');
SELECT sql_saga.add_foreign_key('fk', ARRAY['uk_id'], 'q', 'uk_id_p');
--
TABLE sql_saga.periods;
TABLE sql_saga.foreign_keys;

--
INSERT INTO uk(id, s, e)        VALUES    (1, 1, 3),    (1, 3, 5);
INSERT INTO fk(id, uk_id, s, e) VALUES (1, 1, 1, 2), (2, 1, 2, 5);

-- Make sure the data is there before we start deleting
TABLE uk;
TABLE fk;

--expected: fail
DELETE FROM uk WHERE (id, s, e) = (1, 1, 3);

TABLE uk;
TABLE fk;

--expected: fail
DELETE FROM uk WHERE (id, s, e) = (1, 3, 5);

INSERT INTO uk(id, s, e)        VALUES    (2, 1, 5);
INSERT INTO fk(id, uk_id, s, e) VALUES (4, 2, 2, 4);

TABLE uk;
TABLE fk;

--expected: fail
UPDATE uk SET e = 3 WHERE (id, s, e) = (2, 1, 5);

TABLE uk;
TABLE fk;

-- Create non contiguous time
INSERT INTO uk(id, s, e)        VALUES    (3, 1, 3),
                                          (3, 4, 5);

-- Reference over non contiguous time - should fail
INSERT INTO fk(id, uk_id, s, e) VALUES (5, 3, 1, 5);


-- Create overlappig range - should fail
INSERT INTO uk(id, s, e)        VALUES    (4, 1, 4),
                                          (4, 3, 5);
DROP TABLE uk;
DROP TABLE fk;

-- Test case for bug with infinite parent validity
CREATE TABLE legal_unit_bug (
    id INT NOT NULL,
    valid_after DATE NOT NULL,
    valid_to DATE NOT NULL,
    name TEXT,
    PRIMARY KEY (id, valid_after)
);
SELECT sql_saga.add_era('legal_unit_bug', 'valid_after', 'valid_to');
SELECT sql_saga.add_unique_key('legal_unit_bug', ARRAY['id']);

CREATE TABLE establishment_bug (
    id INT NOT NULL,
    legal_unit_id INT, -- Temporal FK
    valid_after DATE NOT NULL,
    valid_to DATE NOT NULL,
    name TEXT,
    PRIMARY KEY (id, valid_after)
);
SELECT sql_saga.add_era('establishment_bug', 'valid_after', 'valid_to');
-- Note: A unique key on 'id' is not strictly necessary for this test but good practice.
SELECT sql_saga.add_unique_key('establishment_bug', ARRAY['id']); 

-- Add the temporal foreign key constraint
SELECT sql_saga.add_foreign_key('establishment_bug', ARRAY['legal_unit_id'], 'valid', 'legal_unit_bug_id_valid');

INSERT INTO legal_unit_bug (id, valid_after, valid_to, name) VALUES
(1, '2023-12-30', 'infinity', 'Parent LU');

-- This should succeed, as the child's validity is fully contained within the parent's.
INSERT INTO establishment_bug (id, legal_unit_id, valid_after, valid_to, name) VALUES
(101, 1, '2023-12-31', '2024-12-31', 'Child EST');

-- Verify insert
SELECT * FROM establishment_bug;

DROP TABLE establishment_bug;
DROP TABLE legal_unit_bug;
