\i sql/include/test_setup.sql

BEGIN;

/* Run tests as unprivileged user */
SET ROLE TO sql_saga_unprivileged_user;

-- Unique keys are already pretty much guaranteed by the underlying features of
-- PostgreSQL, but test them anyway.
CREATE TABLE uk (id integer, valid_from integer, valid_until integer, CONSTRAINT uk_pkey PRIMARY KEY (id, valid_from, valid_until) DEFERRABLE);
SELECT sql_saga.add_era('uk', 'valid_from', 'valid_until', 'p');
-- Adopt an existing primary key
SELECT sql_saga.add_unique_key('uk'::regclass, ARRAY['id'], 'p', key_type => 'primary', unique_key_name => 'uk_id_p', unique_constraint => 'uk_pkey');
TABLE sql_saga.unique_keys;
INSERT INTO uk (id, valid_from, valid_until) VALUES (100, 2, 4), (100, 4, 5), (100, 5, 11); -- success
INSERT INTO uk (id, valid_from, valid_until) VALUES (200, 2, 4), (200, 4, 5), (200, 6, 11); -- success

SAVEPOINT pristine;
INSERT INTO uk (id, valid_from, valid_until) VALUES (300, 2, 4), (300, 4, 6), (300, 5, 11); -- fail
ROLLBACK TO SAVEPOINT pristine;

CREATE TABLE fk (id integer, uk_id integer, valid_from integer, valid_until integer, PRIMARY KEY (id));
SELECT sql_saga.add_era('fk', 'valid_from', 'valid_until', 'q');
SELECT sql_saga.add_temporal_foreign_key('fk', ARRAY['uk_id'], 'q', 'uk_id_p',
    foreign_key_name => 'fk_uk_id_q',
    fk_insert_trigger => 'fki',
    fk_update_trigger => 'fku',
    uk_update_trigger => 'uku',
    uk_delete_trigger => 'ukd');
TABLE sql_saga.foreign_keys;
SELECT sql_saga.drop_foreign_key('fk', ARRAY['uk_id'], 'q');
SELECT sql_saga.add_temporal_foreign_key('fk', ARRAY['uk_id'], 'q', 'uk_id_p', foreign_key_name => 'fk_uk_id_q');
TABLE sql_saga.foreign_keys;

-- INSERT
SAVEPOINT insert_test;
INSERT INTO fk VALUES (0, 100, 1, 2); -- fail
ROLLBACK TO SAVEPOINT insert_test;
INSERT INTO fk VALUES (0, 100, 1, 11); -- fail
ROLLBACK TO SAVEPOINT insert_test;
INSERT INTO fk VALUES (0, 100, 2, 12); -- fail
ROLLBACK TO SAVEPOINT insert_test;

INSERT INTO fk VALUES (1, 100, 2, 4); -- success
INSERT INTO fk VALUES (2, 100, 2, 11); -- success
-- UPDATE
SAVEPOINT update_fk_test;
UPDATE fk SET valid_until = 21 WHERE id = 1; -- fail
ROLLBACK TO SAVEPOINT update_fk_test;

UPDATE fk SET valid_until = 7 WHERE id = 1; -- success

SAVEPOINT update_uk_test;
UPDATE uk SET valid_from = 3 WHERE (id, valid_from, valid_until) = (100, 2, 4); -- fail
ROLLBACK TO SAVEPOINT update_uk_test;

UPDATE uk SET valid_from = 1 WHERE (id, valid_from, valid_until) = (100, 2, 4); -- success
-- DELETE
SAVEPOINT delete_test;
DELETE FROM uk WHERE (id, valid_from, valid_until) = (100, 4, 5); -- fail
ROLLBACK TO SAVEPOINT delete_test;
DELETE FROM uk WHERE (id, valid_from, valid_until) = (200, 4, 6); -- success

SELECT sql_saga.drop_foreign_key('fk', ARRAY['uk_id'], 'q');
SELECT sql_saga.drop_era('fk', 'q');
DROP TABLE fk;

SELECT sql_saga.drop_unique_key('uk', ARRAY['id'], 'p');
SELECT sql_saga.drop_era('uk', 'p');
DROP TABLE uk;

-- Test primary key creation
CREATE TABLE pk_test (id integer, valid_from integer, valid_until integer);
SELECT sql_saga.add_era('pk_test', 'valid_from', 'valid_until');
SELECT sql_saga.add_unique_key('pk_test', '{id}', key_type => 'primary');
\d pk_test
TABLE sql_saga.unique_keys;
SELECT sql_saga.drop_unique_key('pk_test', '{id}');
SELECT sql_saga.drop_era('pk_test');
DROP TABLE pk_test;


\echo '--- Test: add_unique_key validation for incompatible simple PRIMARY KEY ---'
CREATE TABLE public.simple_pk (
    id int PRIMARY KEY DEFERRABLE,
    valid_from date,
    valid_until date
);
SELECT sql_saga.add_era('public.simple_pk');
SAVEPOINT expect_error;
\echo 'Attempting to add a temporal primary key to a table with a simple primary key (should fail)'
SELECT sql_saga.add_unique_key('public.simple_pk', '{id}', key_type => 'primary');
ROLLBACK TO SAVEPOINT expect_error;
DROP TABLE public.simple_pk;


\echo '--- Test: add_unique_key validation for GENERATED ALWAYS identity column ---'
CREATE TABLE public.generated_always (
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY DEFERRABLE,
    valid_from date,
    valid_until date
);
SELECT sql_saga.add_era('public.generated_always');
SAVEPOINT expect_error_2;
\echo 'Attempting to add a temporal primary key to a table with a GENERATED ALWAYS identity (should fail)'
SELECT sql_saga.add_unique_key('public.generated_always', '{id}', key_type => 'primary');
ROLLBACK TO SAVEPOINT expect_error_2;
DROP TABLE public.generated_always;

ROLLBACK;

\i sql/include/test_teardown.sql
