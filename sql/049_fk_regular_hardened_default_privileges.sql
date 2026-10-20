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

-- A second foreign key to the same key stays usable by the writer. Since 1.3 no helper function is
-- generated at all (the referencing side is a constraint trigger), so there is nothing to share or grant.
CREATE TABLE chi2 (id int PRIMARY KEY, par_id int);
SELECT sql_saga.add_regular_foreign_key('chi2', ARRAY['par_id'], 'par_id_valid');
GRANT INSERT ON chi2 TO hardened_writer;
SET ROLE hardened_writer;
INSERT INTO chi2 VALUES (1, 1);
SAVEPOINT missing_parent2;
INSERT INTO chi2 VALUES (2, 99); -- refused by the foreign key, not by a missing privilege
ROLLBACK TO SAVEPOINT missing_parent2;
RESET ROLE;
SELECT count(*) AS helpers FROM pg_proc WHERE proname = 'par_id_exists';

-- An explicit fk_helper_function is refused (the option is withdrawn): a user function is never taken over
-- and is left exactly as it was.
CREATE TABLE chi3 (id int PRIMARY KEY, par_id int);
CREATE FUNCTION public.not_the_helper(integer) RETURNS boolean LANGUAGE sql STABLE STRICT AS $$ SELECT true $$;
SAVEPOINT impostor;
SELECT sql_saga.add_regular_foreign_key('chi3', ARRAY['par_id'], 'par_id_valid', fk_helper_function => 'public.not_the_helper');
ROLLBACK TO SAVEPOINT impostor;
SELECT p.prosecdef,
       EXISTS (SELECT FROM aclexplode(p.proacl) AS a WHERE a.grantee = 0) AS public_can_execute
FROM pg_proc AS p WHERE p.proname = 'not_the_helper';
SELECT count(*) AS chi3_foreign_keys FROM sql_saga.foreign_keys WHERE table_name = 'chi3';

-- A custom fk_check_constraint name still works: it names the referencing-side constraint trigger.
SAVEPOINT custom_check_name;
SELECT sql_saga.add_regular_foreign_key('chi3', ARRAY['par_id'], 'par_id_valid', fk_check_constraint => 'chi3_custom_check');
SELECT fk.fk_check_constraint, fk.fk_helper_function IS NULL AS no_helper,
       t.tgfoid::regprocedure AS trigger_function
FROM sql_saga.foreign_keys AS fk
JOIN pg_trigger AS t ON t.tgrelid = 'chi3'::regclass AND t.tgname = fk.fk_check_constraint
WHERE fk.table_name = 'chi3';
GRANT INSERT ON chi3 TO hardened_writer;
SET ROLE hardened_writer;
INSERT INTO chi3 VALUES (1, 1);
SAVEPOINT chi3_missing_parent;
INSERT INTO chi3 VALUES (2, 99);
ROLLBACK TO SAVEPOINT chi3_missing_parent;
RESET ROLE;
ROLLBACK TO SAVEPOINT custom_check_name;

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
