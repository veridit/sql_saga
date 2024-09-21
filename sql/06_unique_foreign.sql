/* Run tests as unprivileged user */
SET ROLE TO sql_saga_unprivileged_user;

-- Unique keys are already pretty much guaranteed by the underlying features of
-- PostgreSQL, but test them anyway.
CREATE TABLE uk (id integer, s integer, e integer, CONSTRAINT uk_pkey PRIMARY KEY (id, s, e) DEFERRABLE);
SELECT sql_saga.add_era('uk', 's', 'e', 'p');
SELECT sql_saga.add_unique_key('uk'::regclass, ARRAY['id'], 'p', unique_key_name => 'uk_id_p', unique_constraint => 'uk_pkey');
TABLE sql_saga.unique_keys;
INSERT INTO uk (id, s, e) VALUES (100, 1, 3), (100, 3, 4), (100, 4, 10); -- success
INSERT INTO uk (id, s, e) VALUES (200, 1, 3), (200, 3, 4), (200, 5, 10); -- success
INSERT INTO uk (id, s, e) VALUES (300, 1, 3), (300, 3, 5), (300, 4, 10); -- fail

CREATE TABLE fk (id integer, uk_id integer, s integer, e integer, PRIMARY KEY (id));
SELECT sql_saga.add_era('fk', 's', 'e', 'q');
SELECT sql_saga.add_foreign_key('fk', ARRAY['uk_id'], 'q', 'uk_id_p',
    foreign_key_name => 'fk_uk_id_q',
    fk_insert_trigger => 'fki',
    fk_update_trigger => 'fku',
    uk_update_trigger => 'uku',
    uk_delete_trigger => 'ukd');
TABLE sql_saga.foreign_keys;
SELECT sql_saga.drop_foreign_key('fk', 'fk_uk_id_q');
SELECT sql_saga.add_foreign_key('fk', ARRAY['uk_id'], 'q', 'uk_id_p', foreign_key_name => 'fk_uk_id_q');
TABLE sql_saga.foreign_keys;

-- INSERT
INSERT INTO fk VALUES (0, 100, 0, 1); -- fail
INSERT INTO fk VALUES (0, 100, 0, 10); -- fail
INSERT INTO fk VALUES (0, 100, 1, 11); -- fail
INSERT INTO fk VALUES (1, 100, 1, 3); -- success
INSERT INTO fk VALUES (2, 100, 1, 10); -- success
-- UPDATE
UPDATE fk SET e = 20 WHERE id = 1; -- fail
UPDATE fk SET e = 6 WHERE id = 1; -- success
UPDATE uk SET s = 2 WHERE (id, s, e) = (100, 1, 3); -- fail
UPDATE uk SET s = 0 WHERE (id, s, e) = (100, 1, 3); -- success
-- DELETE
DELETE FROM uk WHERE (id, s, e) = (100, 3, 4); -- fail
DELETE FROM uk WHERE (id, s, e) = (200, 3, 5); -- success

DROP TABLE fk;
DROP TABLE uk;
