\set ECHO none
\set QUIET 1
SET client_min_messages = warning;
\i sql/include/test_setup.sql
-- A natural key with mutually_exclusive_columns allows at most one of them per row (hug bjd8v7). add_unique_key adds
-- CHECK (num_nonnulls(<columns>) <= 1) and records it in unique_keys.check_constraint; all NULL stays allowed (before
-- the key starts). drop_unique_key drops it, a rename is followed, a direct DROP CONSTRAINT is refused, and
-- check_integrity reports it missing with the repair.
CREATE SCHEMA xc_167;
SET search_path TO xc_167, public;
CREATE TABLE xc (id int NOT NULL, lu int, es int, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('xc', 'valid_range') AS ignored \gset
SELECT sql_saga.add_unique_key('xc', ARRAY['id'], key_type => 'primary') AS ignored \gset
\set ECHO all
\set QUIET 0
\set VERBOSITY terse
SET client_min_messages = notice;

\echo '==== add_unique_key adds the CHECK, says so, and records it'
SELECT sql_saga.add_unique_key('xc', ARRAY['lu', 'es'], key_type => 'natural', mutually_exclusive_columns => ARRAY['lu', 'es']);
SET client_min_messages = warning;
SELECT conname, pg_get_constraintdef(oid) AS definition FROM pg_constraint
WHERE conrelid = 'xc'::regclass AND contype = 'c' AND conname LIKE '%xor%';
SELECT unique_key_name, check_constraint FROM sql_saga.unique_keys WHERE table_name = 'xc' ORDER BY 1;
SELECT sql_saga.check_integrity();

\echo '-- one column, or none (before the key starts), is allowed; two are refused'
BEGIN;
INSERT INTO xc VALUES (1, 10, NULL, '[2020-01-01,2030-01-01)'), (2, NULL, 10, '[2020-01-01,2030-01-01)'),
                      (3, NULL, NULL, '[2020-01-01,2030-01-01)');
SAVEPOINT two_columns;
INSERT INTO xc VALUES (4, 20, 20, '[2020-01-01,2030-01-01)');
ROLLBACK TO SAVEPOINT two_columns;
ROLLBACK;

\echo '==== a direct DROP CONSTRAINT is refused (drop protection)'
BEGIN;
ALTER TABLE xc DROP CONSTRAINT xc_lu_es_valid_xor_check;
ROLLBACK;

\echo '==== a renamed CHECK is followed'
BEGIN;
ALTER TABLE xc RENAME CONSTRAINT xc_lu_es_valid_xor_check TO xc_one_of_lu_es;
SELECT unique_key_name, check_constraint FROM sql_saga.unique_keys WHERE table_name = 'xc' AND check_constraint IS NOT NULL;
SELECT sql_saga.check_integrity();
ROLLBACK;

\echo '==== check_integrity reports a missing CHECK with the repair'
\echo '-- the state 1.14 left (no CHECK), bypassing the event triggers as a restore would:'
BEGIN;
SET LOCAL session_replication_role = replica;
ALTER TABLE xc DROP CONSTRAINT xc_lu_es_valid_xor_check;
UPDATE sql_saga.unique_keys SET check_constraint = NULL WHERE unique_key_name = 'xc_lu_es_valid';
RESET session_replication_role;
\set VERBOSITY default
SAVEPOINT report;
SELECT sql_saga.check_integrity();
ROLLBACK TO SAVEPOINT report;
\set VERBOSITY terse
\echo '-- the repair named in the hint makes it clean:'
ALTER TABLE xc_167.xc ADD CONSTRAINT xc_lu_es_valid_xor_check CHECK ((num_nonnulls(lu, es) <= 1));
UPDATE sql_saga.unique_keys SET check_constraint = 'xc_lu_es_valid_xor_check' WHERE unique_key_name = 'xc_lu_es_valid';
SELECT sql_saga.check_integrity();
ROLLBACK;

\echo '-- a CHECK of that name with another definition is not the key''s:'
BEGIN;
SET LOCAL session_replication_role = replica;
ALTER TABLE xc DROP CONSTRAINT xc_lu_es_valid_xor_check;
ALTER TABLE xc ADD CONSTRAINT xc_lu_es_valid_xor_check CHECK (num_nonnulls(lu, es) <= 2);
RESET session_replication_role;
SAVEPOINT report;
SELECT sql_saga.check_integrity();
ROLLBACK TO SAVEPOINT report;
ROLLBACK;

\echo '==== drop_unique_key drops the CHECK with the key'
SELECT sql_saga.drop_unique_key('xc', ARRAY['lu', 'es']);
SELECT conname FROM pg_constraint WHERE conrelid = 'xc'::regclass AND contype IN ('c', 'u', 'x') ORDER BY 1;

\echo '==== add_unique_key adopts a CHECK of that name and definition, and refuses existing rows that give two'
ALTER TABLE xc ADD CONSTRAINT xc_lu_es_valid_xor_check CHECK (num_nonnulls(lu, es) <= 1);
SELECT sql_saga.add_unique_key('xc', ARRAY['lu', 'es'], key_type => 'natural', mutually_exclusive_columns => ARRAY['lu', 'es']) AS key_name;
SELECT unique_key_name, check_constraint FROM sql_saga.unique_keys WHERE table_name = 'xc' AND check_constraint IS NOT NULL;
SELECT sql_saga.drop_unique_key('xc', ARRAY['lu', 'es']);
SELECT conname FROM pg_constraint WHERE conrelid = 'xc'::regclass AND contype = 'c' AND conname LIKE '%xor%';
BEGIN;
INSERT INTO xc VALUES (5, 30, 30, '[2020-01-01,2030-01-01)');
SAVEPOINT refuse;
SELECT sql_saga.add_unique_key('xc', ARRAY['lu', 'es'], key_type => 'natural', mutually_exclusive_columns => ARRAY['lu', 'es']);
ROLLBACK TO SAVEPOINT refuse;
ROLLBACK;

\set ECHO none
\set QUIET 1
\set VERBOSITY default
DROP SCHEMA xc_167 CASCADE;
RESET search_path;
\i sql/include/test_teardown.sql
