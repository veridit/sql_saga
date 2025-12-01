\i sql/include/test_setup.sql

BEGIN;

/* Run tests as unprivileged user */
SET ROLE TO sql_saga_unprivileged_user;

-- Unique keys are already pretty much guaranteed by the underlying features of
-- PostgreSQL, but test them anyway.
CREATE TABLE uk (id integer, valid_range int4range, valid_from integer, valid_until integer, CONSTRAINT uk_pkey PRIMARY KEY (id, valid_range WITHOUT OVERLAPS));

SELECT sql_saga.add_era('uk', 'valid_range', 'p');
-- Adopt an existing primary key
SELECT sql_saga.add_unique_key('uk'::regclass, ARRAY['id'], 'p', key_type => 'primary', unique_key_name => 'uk_id_p', unique_constraint => 'uk_pkey');
TABLE sql_saga.unique_keys;
INSERT INTO uk (id, valid_range) VALUES (100, int4range(2, 4)), (100, int4range(4, 5)), (100, int4range(5, 11)); -- success
INSERT INTO uk (id, valid_range) VALUES (200, int4range(2, 4)), (200, int4range(4, 5)), (200, int4range(6, 11)); -- success

SAVEPOINT pristine;
INSERT INTO uk (id, valid_range) VALUES (300, int4range(2, 4)), (300, int4range(4, 6)), (300, int4range(5, 11)); -- fail (overlapping ranges)
ROLLBACK TO SAVEPOINT pristine;

CREATE TABLE fk (id integer, uk_id integer, valid_range int4range, valid_from integer, valid_until integer, PRIMARY KEY (id, valid_range WITHOUT OVERLAPS) DEFERRABLE);
SELECT sql_saga.add_era('fk', 'valid_range', 'q');
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
INSERT INTO fk VALUES (0, 100, int4range(1, 2)); -- fail (not covered by uk)
ROLLBACK TO SAVEPOINT insert_test;
INSERT INTO fk VALUES (0, 100, int4range(1, 11)); -- fail (extends beyond uk coverage)
ROLLBACK TO SAVEPOINT insert_test;
INSERT INTO fk VALUES (0, 100, int4range(2, 12)); -- fail (extends beyond uk coverage)
ROLLBACK TO SAVEPOINT insert_test;

INSERT INTO fk VALUES (1, 100, int4range(2, 4)); -- success
INSERT INTO fk VALUES (2, 100, int4range(2, 11)); -- success
-- UPDATE
SAVEPOINT update_fk_test;
UPDATE fk SET valid_range = int4range(2, 21) WHERE id = 1; -- fail (extends beyond uk coverage)
ROLLBACK TO SAVEPOINT update_fk_test;

UPDATE fk SET valid_range = int4range(2, 7) WHERE id = 1; -- success

SAVEPOINT update_uk_test;
UPDATE uk SET valid_range = int4range(3, 4) WHERE (id, valid_range) = (100, int4range(2, 4)); -- fail (would break fk reference)
ROLLBACK TO SAVEPOINT update_uk_test;

UPDATE uk SET valid_range = int4range(1, 4) WHERE (id, valid_range) = (100, int4range(2, 4)); -- success (extends coverage)
-- DELETE
SAVEPOINT delete_test;
DELETE FROM uk WHERE (id, valid_range) = (100, int4range(4, 5)); -- fail (referenced by fk)
ROLLBACK TO SAVEPOINT delete_test;
DELETE FROM uk WHERE (id, valid_range) = (200, int4range(4, 5)); -- success (not referenced)

SELECT sql_saga.drop_foreign_key('fk', ARRAY['uk_id'], 'q');
SELECT sql_saga.drop_era('fk', 'q');
DROP TABLE fk;

SELECT sql_saga.drop_unique_key('uk', ARRAY['id'], 'p');
SELECT sql_saga.drop_era('uk', 'p');
DROP TABLE uk;

-- Test primary key creation
CREATE TABLE pk_test (id integer, valid_range int4range, valid_from integer, valid_until integer);
SELECT sql_saga.add_era('pk_test', 'valid_range');
SELECT sql_saga.add_unique_key('pk_test', '{id}', key_type => 'primary');
\d pk_test
TABLE sql_saga.unique_keys;
SELECT sql_saga.drop_unique_key('pk_test', '{id}');
SELECT sql_saga.drop_era('pk_test');
DROP TABLE pk_test;


\echo '--- Test: add_unique_key validation for incompatible simple PRIMARY KEY ---'
CREATE TABLE public.simple_pk (
    id int PRIMARY KEY DEFERRABLE,
    valid_range daterange,
    valid_from date,
    valid_until date
);
SELECT sql_saga.add_era('public.simple_pk', 'valid_range');
SAVEPOINT expect_error;
\echo 'Attempting to add a temporal primary key to a table with a simple primary key (should fail)'
SELECT sql_saga.add_unique_key('public.simple_pk', '{id}', key_type => 'primary');
ROLLBACK TO SAVEPOINT expect_error;
DROP TABLE public.simple_pk;


\echo '--- Test: add_unique_key validation for GENERATED ALWAYS identity column ---'
CREATE TABLE public.generated_always (
    id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY DEFERRABLE,
    valid_range daterange,
    valid_from date,
    valid_until date
);
SELECT sql_saga.add_era('public.generated_always', 'valid_range');
SAVEPOINT expect_error_2;
\echo 'Attempting to add a temporal primary key to a table with a GENERATED ALWAYS identity (should fail)'
SELECT sql_saga.add_unique_key('public.generated_always', '{id}', key_type => 'primary');
ROLLBACK TO SAVEPOINT expect_error_2;
DROP TABLE public.generated_always;

ROLLBACK;

\i sql/include/test_teardown.sql
