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
