\i sql/include/test_setup.sql

-- A role that may insert into the child table of a regular-to-temporal foreign key can insert, as with a
-- native foreign key: it needs no SELECT on the referenced table, and no EXECUTE granted on sql_saga's helper,
-- also in a hardened database that revokes EXECUTE from PUBLIC by default.
SET client_min_messages = warning;
\set SHOW_CONTEXT never

CREATE ROLE hardened_writer;
BEGIN;
ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;

CREATE TABLE par (id int, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('par', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('par', ARRAY['id'], key_type => 'primary', unique_key_name => 'par_id_valid');
INSERT INTO par VALUES (1, '[2020-01-01,infinity)');
CREATE TABLE chi (id int PRIMARY KEY, par_id int);
SELECT sql_saga.add_regular_foreign_key('chi', ARRAY['par_id'], 'par_id_valid');
GRANT INSERT ON chi TO hardened_writer;

SET ROLE hardened_writer;
INSERT INTO chi VALUES (1, 1);
SAVEPOINT missing_parent;
INSERT INTO chi VALUES (2, 99); -- refused by the foreign key, not by a missing privilege
ROLLBACK TO SAVEPOINT missing_parent;
RESET ROLE;
TABLE chi;

-- A second foreign key to the same key shares the helper; it stays usable by the writer.
CREATE TABLE chi2 (id int PRIMARY KEY, par_id int);
SELECT sql_saga.add_regular_foreign_key('chi2', ARRAY['par_id'], 'par_id_valid');
GRANT INSERT ON chi2 TO hardened_writer;
SET ROLE hardened_writer;
INSERT INTO chi2 VALUES (1, 1);
RESET ROLE;
SELECT count(*) AS helpers FROM pg_proc WHERE proname = 'par_id_exists';

-- A function that merely has the helper's name and signature is not the helper: it is refused, not taken over.
CREATE TABLE chi3 (id int PRIMARY KEY, par_id int);
CREATE FUNCTION public.not_the_helper(integer) RETURNS boolean LANGUAGE sql STABLE STRICT AS $$ SELECT true $$;
SAVEPOINT impostor;
SELECT sql_saga.add_regular_foreign_key('chi3', ARRAY['par_id'], 'par_id_valid', fk_helper_function => 'public.not_the_helper');
ROLLBACK TO SAVEPOINT impostor;
SELECT p.prosecdef,
       EXISTS (SELECT FROM aclexplode(p.proacl) AS a WHERE a.grantee = 0) AS public_can_execute
FROM pg_proc AS p WHERE p.proname = 'not_the_helper';

-- A key type whose equality operator lives outside pg_catalog compares with that operator, as a native
-- foreign key does: citext matches case-insensitively.
CREATE EXTENSION citext;
CREATE TABLE cpar (code citext, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('cpar', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('cpar', ARRAY['code'], key_type => 'primary', unique_key_name => 'cpar_code_valid');
INSERT INTO cpar VALUES ('Abc', '[2020-01-01,infinity)');
CREATE TABLE cchi (id int PRIMARY KEY, cpar_code citext);
SELECT sql_saga.add_regular_foreign_key('cchi', ARRAY['cpar_code'], 'cpar_code_valid');
INSERT INTO cchi VALUES (1, 'aBC');
TABLE cchi;
ROLLBACK;
DROP ROLE hardened_writer;

RESET client_min_messages;
\i sql/include/test_teardown.sql
