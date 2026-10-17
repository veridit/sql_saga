\i sql/include/test_setup.sql

/*
 * Renaming a column of the REFERENCED unique key (ALTER TABLE parent RENAME COLUMN <key column>).
 *
 * The unique key's column_names follow the rename (as before). Every regular foreign key that
 * references the key keeps working because its generated objects are regenerated in place:
 *   - the helper function keeps its name, signature, owner, SECURITY DEFINER and search_path, and its
 *     body compares the renamed column (it is shared by all regular FKs referencing the key);
 *   - the referenced-side triggers keep their names and firing modes, and their arguments name the
 *     renamed column;
 *   - no stored object name in sql_saga.foreign_keys changes, and the CHECK constraint is unchanged.
 * A temporal foreign key (native PERIOD constraint) follows by attnum and keeps its name.
 * check_integrity() passes after the rename, and detects a regular FK whose helper function body or
 * referenced-side trigger arguments do not match the current columns (made here with event triggers
 * silenced by session_replication_role = replica).
 */
\set SHOW_CONTEXT never
\set VERBOSITY default
SET client_min_messages = warning;

CREATE SCHEMA fkk;
SET search_path = fkk, public;

-- decoy_id: a same-typed column that a wrong helper body could compare instead of the key.
CREATE TABLE fkk.par (id int, label text, decoy_id int, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('fkk.par', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('fkk.par', ARRAY['id']::name[], 'valid', key_type => 'natural');
INSERT INTO fkk.par (id, label, valid_range) VALUES (1, 'a', '[2020-01-01,2030-01-01)'), (2, 'b', '[2020-01-01,2030-01-01)'),
                           (3, 'c', '[2020-01-01,2030-01-01)'), (4, 'd', '[2020-01-01,2030-01-01)');

-- Two regular FKs share the helper function fkk.par_id_exists(integer).
CREATE TABLE fkk.rchild (id int PRIMARY KEY, par_id int);
SELECT sql_saga.add_regular_foreign_key('fkk.rchild', ARRAY['par_id']::name[], 'par_id_valid');
CREATE TABLE fkk.rchild2 (id int PRIMARY KEY, owner_id int);
SELECT sql_saga.add_regular_foreign_key('fkk.rchild2', ARRAY['owner_id']::name[], 'par_id_valid');

CREATE TABLE fkk.tchild (id int, par_id int, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('fkk.tchild', 'valid_range', 'valid');
SELECT sql_saga.add_temporal_foreign_key('fkk.tchild', ARRAY['par_id']::name[], 'valid', 'par_id_valid');

INSERT INTO fkk.rchild VALUES (1, 1);
INSERT INTO fkk.rchild2 VALUES (1, 4);
INSERT INTO fkk.tchild VALUES (1, 2, '[2021-01-01,2022-01-01)');

-- Non-default firing modes on rchild's referenced-side triggers must survive.
ALTER TABLE fkk.par ENABLE ALWAYS TRIGGER rchild_par_id_fkey_uk_delete;
ALTER TABLE fkk.par ENABLE REPLICA TRIGGER rchild_par_id_fkey_uk_update;

CREATE VIEW fkk.fk_state AS
SELECT fk.foreign_key_name, fk.type, fk.column_names, fk.fk_check_constraint, fk.fk_helper_function,
       fk.fk_index_name, fk.uk_update_trigger, fk.uk_delete_trigger
FROM sql_saga.foreign_keys AS fk
WHERE fk.table_schema = 'fkk'
ORDER BY fk.foreign_key_name;

CREATE VIEW fkk.uk_state AS
SELECT uk.unique_key_name, uk.column_names, uk.unique_constraint
FROM sql_saga.unique_keys AS uk
WHERE uk.table_schema = 'fkk'
ORDER BY uk.unique_key_name;

-- Referenced-side triggers: mode, and the fk/uk column lists from their arguments.
CREATE VIEW fkk.par_triggers AS
SELECT t.tgname, t.tgenabled, t.tgfoid::regprocedure AS function,
       (string_to_array(encode(t.tgargs, 'escape'), '\000'))[3] AS arg_fk_table,
       (string_to_array(encode(t.tgargs, 'escape'), '\000'))[4] AS arg_fk_columns,
       (string_to_array(encode(t.tgargs, 'escape'), '\000'))[10] AS arg_uk_columns
FROM pg_catalog.pg_trigger AS t
WHERE t.tgrelid = 'fkk.par'::regclass AND NOT t.tgisinternal
  AND t.tgfoid IN ('sql_saga.uk_update_check_c()'::regprocedure, 'sql_saga.uk_delete_check_c()'::regprocedure)
ORDER BY t.tgname;

-- The helper owner is compared with its pre-rename owner and with the referenced table owner (role names
-- differ between machines).
CREATE TABLE fkk.helper_owner_before AS
SELECT p.proowner FROM pg_catalog.pg_proc AS p
WHERE p.pronamespace = 'fkk'::regnamespace AND p.proname = 'par_id_exists';

CREATE VIEW fkk.helper AS
SELECT p.oid::regprocedure AS helper,
       p.proowner = (SELECT o.proowner FROM fkk.helper_owner_before AS o) AS owner_unchanged,
       p.proowner = (SELECT c.relowner FROM pg_catalog.pg_class AS c WHERE c.oid = 'fkk.par'::regclass) AS owned_by_table_owner,
       p.prosecdef,
       p.proconfig, btrim(p.prosrc, E' \t\r\n') AS body
FROM pg_catalog.pg_proc AS p
WHERE p.pronamespace = 'fkk'::regnamespace AND p.proname = 'par_id_exists';

CREATE VIEW fkk.child_constraints AS
SELECT c.conrelid::regclass AS table_name, c.conname, pg_catalog.pg_get_constraintdef(c.oid) AS def
FROM pg_catalog.pg_constraint AS c
WHERE c.conrelid IN ('fkk.rchild'::regclass, 'fkk.rchild2'::regclass, 'fkk.tchild'::regclass) AND c.contype IN ('c', 'f')
  AND c.conname NOT LIKE '%valid_check'
ORDER BY 1, 2;

\echo '--- Before the rename'
TABLE fkk.uk_state;
TABLE fkk.fk_state;
TABLE fkk.par_triggers;
TABLE fkk.helper;
TABLE fkk.child_constraints;
SELECT sql_saga.check_integrity();

\echo '--- Rename the referenced key column'
ALTER TABLE fkk.par RENAME COLUMN id TO pid;

\echo '--- After the rename: key columns follow, helper and triggers regenerated, names unchanged'
TABLE fkk.uk_state;
TABLE fkk.fk_state;
TABLE fkk.par_triggers;
TABLE fkk.helper;
TABLE fkk.child_constraints;
SELECT sql_saga.check_integrity();
SELECT 'integrity checked after rename' AS done;

\echo '--- Regular FKs: referencing side'
BEGIN;
INSERT INTO fkk.rchild VALUES (2, 3);
INSERT INTO fkk.rchild2 VALUES (2, 3);
ROLLBACK;
BEGIN;
INSERT INTO fkk.rchild VALUES (3, 99);
ROLLBACK;
BEGIN;
INSERT INTO fkk.rchild2 VALUES (3, 99);
ROLLBACK;

\echo '--- Regular FKs: referenced side'
BEGIN;
DELETE FROM fkk.par WHERE pid = 1;
ROLLBACK;
BEGIN;
DELETE FROM fkk.par WHERE pid = 4;
ROLLBACK;
BEGIN;
UPDATE fkk.par SET pid = 5 WHERE pid = 4;
ROLLBACK;
BEGIN;
UPDATE fkk.par SET valid_range = '[2020-01-01,2020-06-01)' WHERE pid = 1;
UPDATE fkk.par SET label = 'changed' WHERE pid = 1;
ROLLBACK;
BEGIN;
DELETE FROM fkk.par WHERE pid = 3;
SELECT pid FROM fkk.par ORDER BY pid;
ROLLBACK;

\echo '--- Firing modes still effective under replica role'
BEGIN;
SET LOCAL session_replication_role = replica;
SAVEPOINT replica_delete;
DELETE FROM fkk.par WHERE pid = 1;
ROLLBACK TO SAVEPOINT replica_delete;
SAVEPOINT replica_update;
UPDATE fkk.par SET pid = 6 WHERE pid = 1;
ROLLBACK TO SAVEPOINT replica_update;
ROLLBACK;

\echo '--- Temporal FK: both sides'
BEGIN;
INSERT INTO fkk.tchild VALUES (2, 99, '[2021-01-01,2022-01-01)');
ROLLBACK;
BEGIN;
DELETE FROM fkk.par WHERE pid = 2;
ROLLBACK;

\echo '--- drop_foreign_key after the rename removes the real objects; the shared helper survives the first drop'
BEGIN;
SELECT sql_saga.drop_foreign_key('fkk.rchild', ARRAY['par_id']::name[]);
TABLE fkk.par_triggers;
SELECT helper FROM fkk.helper;
SELECT sql_saga.drop_foreign_key('fkk.rchild2', ARRAY['owner_id']::name[]);
TABLE fkk.par_triggers;
SELECT count(*) AS helpers_left FROM fkk.helper;
SELECT sql_saga.check_integrity();
ROLLBACK;

\echo '--- check_integrity detects helper and trigger argument drift'
-- Helper body comparing a column that no longer exists.
BEGIN;
SET LOCAL check_function_bodies = off;
SET LOCAL session_replication_role = replica;
CREATE OR REPLACE FUNCTION fkk.par_id_exists(integer) RETURNS boolean LANGUAGE sql STABLE STRICT
SECURITY DEFINER SET search_path = pg_catalog
AS $$ SELECT EXISTS (SELECT 1 FROM fkk.par AS uk WHERE uk.id OPERATOR(pg_catalog.=) $1); $$;
RESET session_replication_role;
SAVEPOINT check_helper_missing_column;
SELECT sql_saga.check_integrity();
ROLLBACK TO SAVEPOINT check_helper_missing_column;
ROLLBACK;

-- Helper body comparing an existing but wrong column (same type as the key).
BEGIN;
SET LOCAL session_replication_role = replica;
CREATE OR REPLACE FUNCTION fkk.par_id_exists(integer) RETURNS boolean LANGUAGE sql STABLE STRICT
SECURITY DEFINER SET search_path = pg_catalog
AS $$ SELECT EXISTS (SELECT 1 FROM fkk.par AS uk WHERE uk.decoy_id OPERATOR(pg_catalog.=) $1); $$;
RESET session_replication_role;
SAVEPOINT check_helper_wrong_column;
SELECT sql_saga.check_integrity();
ROLLBACK TO SAVEPOINT check_helper_wrong_column;
ROLLBACK;

-- Helper body that is semantically wrong (the right column, but a condition that is always true).
BEGIN;
SET LOCAL session_replication_role = replica;
CREATE OR REPLACE FUNCTION fkk.par_id_exists(integer) RETURNS boolean LANGUAGE sql STABLE STRICT
SECURITY DEFINER SET search_path = pg_catalog
AS $$ SELECT EXISTS (SELECT 1 FROM fkk.par AS uk WHERE uk.pid OPERATOR(pg_catalog.=) $1 OR true); $$;
RESET session_replication_role;
SAVEPOINT check_helper_wrong_body;
SELECT sql_saga.check_integrity();
ROLLBACK TO SAVEPOINT check_helper_wrong_body;
ROLLBACK;

-- Referenced-side trigger whose arguments name the old referenced column.
BEGIN;
SET LOCAL session_replication_role = replica;
DROP TRIGGER rchild_par_id_fkey_uk_delete ON fkk.par;
CREATE CONSTRAINT TRIGGER rchild_par_id_fkey_uk_delete AFTER DELETE ON fkk.par FROM fkk.rchild DEFERRABLE FOR EACH ROW
EXECUTE PROCEDURE sql_saga.uk_delete_check_c('rchild_par_id_fkey', 'fkk', 'rchild', '{par_id}', '', '', '', 'fkk', 'par', '{id}', 'valid', 'null', 'null', 'SIMPLE', 'NO ACTION', 'NO ACTION', 'regular_to_temporal');
RESET session_replication_role;
SAVEPOINT check_trigger_old_uk_column;
SELECT sql_saga.check_integrity();
ROLLBACK TO SAVEPOINT check_trigger_old_uk_column;
ROLLBACK;

-- Referenced-side trigger whose arguments name a nonexistent referencing column.
BEGIN;
SET LOCAL session_replication_role = replica;
DROP TRIGGER rchild_par_id_fkey_uk_update ON fkk.par;
CREATE CONSTRAINT TRIGGER rchild_par_id_fkey_uk_update AFTER UPDATE OF pid ON fkk.par FROM fkk.rchild DEFERRABLE FOR EACH ROW
EXECUTE PROCEDURE sql_saga.uk_update_check_c('rchild_par_id_fkey', 'fkk', 'rchild', '{gone_id}', '', '', '', 'fkk', 'par', '{pid}', 'valid', 'null', 'null', 'SIMPLE', 'NO ACTION', 'NO ACTION', 'regular_to_temporal');
RESET session_replication_role;
SAVEPOINT check_trigger_bad_fk_column;
SELECT sql_saga.check_integrity();
ROLLBACK TO SAVEPOINT check_trigger_bad_fk_column;
ROLLBACK;

\echo '--- A helper replaced by its owner with an unrelated body is not regenerated: the key column rename is refused'
-- The rename regenerates only a helper it can attribute to sql_saga (the canonical body for the key
-- columns before the rename, SECURITY DEFINER). Anything else is refused before any change, so the
-- user's function (body, SECURITY INVOKER) and the catalog stay as they were.
-- Autocommit statements, so the state after the ALTER is what the ALTER left behind.
CREATE OR REPLACE FUNCTION fkk.par_id_exists(integer) RETURNS boolean LANGUAGE sql STABLE STRICT
SECURITY INVOKER
AS $$ SELECT $1 IS NOT NULL; $$;
SELECT helper, owner_unchanged, prosecdef, proconfig, body FROM fkk.helper;
ALTER TABLE fkk.par RENAME COLUMN pid TO pkey;
SELECT helper, owner_unchanged, prosecdef, proconfig, body FROM fkk.helper;
SELECT a.attname FROM pg_catalog.pg_attribute AS a
WHERE a.attrelid = 'fkk.par'::regclass AND a.attnum = 1;
TABLE fkk.uk_state;
TABLE fkk.fk_state;
TABLE fkk.par_triggers;
SELECT sql_saga.check_integrity();
-- Restore the canonical helper (and the column name, should the rename have gone through).
DO $restore$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_attribute AS a
               WHERE a.attrelid = 'fkk.par'::regclass AND a.attname = 'pkey' AND NOT a.attisdropped) THEN
        ALTER TABLE fkk.par RENAME COLUMN pkey TO pid;
    END IF;
END;
$restore$;
CREATE OR REPLACE FUNCTION fkk.par_id_exists(integer) RETURNS boolean LANGUAGE sql STABLE STRICT
SECURITY DEFINER SET search_path = pg_catalog
AS $$ SELECT EXISTS (SELECT 1 FROM fkk.par AS uk WHERE uk.pid OPERATOR(pg_catalog.=) $1); $$;
SELECT helper, owner_unchanged, prosecdef, proconfig, body FROM fkk.helper;
SELECT sql_saga.check_integrity();

\echo '--- A canonical helper keeps its owner, ACL, SECURITY DEFINER and full proconfig through the rename'
BEGIN;
ALTER FUNCTION fkk.par_id_exists(integer) SET statement_timeout = '1234ms';
GRANT EXECUTE ON FUNCTION fkk.par_id_exists(integer) TO sql_saga_unprivileged_user;
CREATE TABLE fkk.helper_props_before AS
SELECT p.proowner, p.proacl, p.prosecdef, p.proconfig FROM pg_catalog.pg_proc AS p
WHERE p.oid = 'fkk.par_id_exists(integer)'::regprocedure;
SELECT p.proconfig, p.prosecdef FROM fkk.helper_props_before AS p;
ALTER TABLE fkk.par RENAME COLUMN pid TO pkey;
SELECT p.oid::regprocedure AS helper,
       p.proowner = b.proowner AS owner_unchanged,
       p.proacl = b.proacl AS acl_unchanged,
       EXISTS (SELECT 1 FROM pg_catalog.aclexplode(p.proacl) AS a
               WHERE a.grantee = 'sql_saga_unprivileged_user'::regrole AND a.privilege_type = 'EXECUTE') AS grant_kept,
       p.prosecdef, p.proconfig, btrim(p.prosrc, E' \t\r\n') AS body
FROM pg_catalog.pg_proc AS p, fkk.helper_props_before AS b
WHERE p.oid = 'fkk.par_id_exists(integer)'::regprocedure;
TABLE fkk.uk_state;
TABLE fkk.par_triggers;
INSERT INTO fkk.rchild VALUES (4, 3);
SAVEPOINT missing_parent;
INSERT INTO fkk.rchild VALUES (5, 99);
ROLLBACK TO SAVEPOINT missing_parent;
SELECT sql_saga.check_integrity();
SELECT 'integrity checked with extra helper settings' AS done;
ROLLBACK;

\echo '--- Final check_integrity'
SELECT sql_saga.check_integrity();
SELECT 'integrity checked' AS done;

SET client_min_messages = warning;
RESET search_path;
DROP SCHEMA fkk CASCADE;
\i sql/include/test_teardown.sql
RESET client_min_messages;
