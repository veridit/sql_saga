\i sql/include/test_setup.sql

/*
 * Two regular foreign keys referencing the same unique key share one generated helper function.
 * Sharing must also work when the helper's schema or name needs quoting (the referenced table lives
 * in a schema with a space, its name and key column contain spaces and capitals).
 * Both FKs must be added, use the same helper, enforce both sides, survive dropping one of them,
 * and check_integrity() must pass.
 */
\set SHOW_CONTEXT never
\set VERBOSITY default
SET client_min_messages = warning;

CREATE SCHEMA "Shared Q";
SET search_path = "Shared Q", public;

CREATE TABLE "Shared Q"."Par T" ("Key Id" int, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('"Shared Q"."Par T"', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('"Shared Q"."Par T"', ARRAY['Key Id']::name[], 'valid', key_type => 'natural');
INSERT INTO "Shared Q"."Par T" VALUES (1, '[2020-01-01,2030-01-01)'), (2, '[2020-01-01,2030-01-01)'), (3, '[2020-01-01,2030-01-01)');

CREATE TABLE "Shared Q".c1 (id int PRIMARY KEY, par_id int);
CREATE TABLE "Shared Q".c2 (id int PRIMARY KEY, owner_id int);
SELECT sql_saga.add_regular_foreign_key('"Shared Q".c1', ARRAY['par_id']::name[], 'Par T_Key Id_valid');
SELECT sql_saga.add_regular_foreign_key('"Shared Q".c2', ARRAY['owner_id']::name[], 'Par T_Key Id_valid');

SELECT foreign_key_name, fk_helper_function FROM sql_saga.foreign_keys WHERE table_schema = 'Shared Q' ORDER BY 1;
SELECT p.oid::regprocedure AS helper FROM pg_catalog.pg_proc AS p WHERE p.pronamespace = '"Shared Q"'::regnamespace;
SELECT sql_saga.check_integrity();

-- c1 references key 1 only, c2 references key 2 only, key 3 is unreferenced, so a surviving sibling FK
-- cannot mask a broken one.
INSERT INTO "Shared Q".c1 VALUES (1, 1);
INSERT INTO "Shared Q".c2 VALUES (1, 2);
BEGIN;
INSERT INTO "Shared Q".c1 VALUES (2, 99);
ROLLBACK;
BEGIN;
DELETE FROM "Shared Q"."Par T" WHERE "Key Id" = 1;
ROLLBACK;
BEGIN;
INSERT INTO "Shared Q".c2 VALUES (2, 99);
ROLLBACK;
BEGIN;
DELETE FROM "Shared Q"."Par T" WHERE "Key Id" = 2;
ROLLBACK;
BEGIN;
DELETE FROM "Shared Q"."Par T" WHERE "Key Id" = 3;
ROLLBACK;

-- Dropping one FK keeps the shared helper for the other; dropping both removes it.
BEGIN;
SELECT sql_saga.drop_foreign_key('"Shared Q".c1', ARRAY['par_id']::name[]);
SELECT p.oid::regprocedure AS helper FROM pg_catalog.pg_proc AS p WHERE p.pronamespace = '"Shared Q"'::regnamespace;
SELECT sql_saga.check_integrity();
-- The surviving FK c2 still enforces both sides.
SAVEPOINT c2_missing_parent;
INSERT INTO "Shared Q".c2 VALUES (3, 99);
ROLLBACK TO SAVEPOINT c2_missing_parent;
SAVEPOINT c2_referenced_parent;
DELETE FROM "Shared Q"."Par T" WHERE "Key Id" = 2;
ROLLBACK TO SAVEPOINT c2_referenced_parent;
-- Key 1 is no longer referenced once c1's FK is gone.
SAVEPOINT c1_parent_free;
DELETE FROM "Shared Q"."Par T" WHERE "Key Id" = 1;
ROLLBACK TO SAVEPOINT c1_parent_free;
SELECT sql_saga.drop_foreign_key('"Shared Q".c2', ARRAY['owner_id']::name[]);
SELECT count(*) AS helpers_left FROM pg_catalog.pg_proc AS p WHERE p.pronamespace = '"Shared Q"'::regnamespace;
ROLLBACK;

SELECT sql_saga.check_integrity();
SELECT 'integrity checked' AS done;

SET client_min_messages = warning;
RESET search_path;
DROP SCHEMA "Shared Q" CASCADE;
\i sql/include/test_teardown.sql
RESET client_min_messages;
