\set ECHO none
\i sql/include/test_setup.sql

-- Keep diagnostics stable: PL/pgSQL CONTEXT contains changing source line numbers.
\set SHOW_CONTEXT never
SET client_min_messages = warning;

BEGIN;

CREATE SCHEMA dp_scope;
SET search_path = dp_scope, public;

CREATE FUNCTION dp_scope.probe(command text, expected_sqlstate text, expected_message text)
RETURNS text
LANGUAGE plpgsql
AS $probe$
DECLARE
    actual_sqlstate text;
    actual_message text;
BEGIN
    BEGIN
        EXECUTE command;
        RAISE EXCEPTION USING ERRCODE = 'Z0001', MESSAGE = 'dp_scope rollback successful command';
    EXCEPTION
        WHEN SQLSTATE 'Z0001' THEN
            IF expected_sqlstate IS NULL THEN
                RETURN 'ok';
            END IF;
            RETURN format('unexpected success, expected %s: %s', expected_sqlstate, expected_message);
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS actual_sqlstate = RETURNED_SQLSTATE, actual_message = MESSAGE_TEXT;
            IF expected_sqlstate IS NULL THEN
                RETURN format('unexpected error %s: %s', actual_sqlstate, actual_message);
            END IF;
            IF (actual_sqlstate, actual_message) IS DISTINCT FROM (expected_sqlstate, expected_message) THEN
                RETURN format('unexpected error %s: %s', actual_sqlstate, actual_message);
            END IF;
            RETURN format('%s: %s', actual_sqlstate, actual_message);
    END;
END;
$probe$;

CREATE FUNCTION dp_scope.family(name text, family_schema text DEFAULT 'dp_scope')
RETURNS void
LANGUAGE plpgsql
AS $family$
BEGIN
    EXECUTE format('CREATE TABLE %I.%I (id int PRIMARY KEY, tenant text, payload text)', family_schema, name);
    PERFORM sql_saga.add_system_versioning(format('%I.%I', family_schema, name)::regclass);
    EXECUTE format('ALTER TABLE %I.%I ENABLE ROW LEVEL SECURITY', family_schema, name);
    EXECUTE format('CREATE POLICY tenant_policy ON %I.%I FOR SELECT USING (tenant = current_user)', family_schema, name);
END;
$family$;

\echo '=== statement scope: deterministic restore-window reproduction'
SAVEPOINT restore_window;
UPDATE sql_saga.installation SET token = 'oldtoken';
SELECT dp_scope.family('family_a');
SELECT dp_scope.family('family_b');

SET LOCAL session_replication_role = replica;
DROP TRIGGER family_a_system_time_write_history ON dp_scope.family_a;
SET LOCAL session_replication_role = origin;

SELECT ste.write_history_trigger AS catalog_trigger,
       to_regclass('dp_scope.family_a') IS NOT NULL AS base_retained,
       NOT EXISTS (
           SELECT FROM pg_catalog.pg_trigger AS t
           WHERE t.tgrelid = 'dp_scope.family_a'::regclass
             AND t.tgname = ste.write_history_trigger) AS trigger_missing
FROM sql_saga.system_time_era AS ste
WHERE (ste.table_schema, ste.table_name) = ('dp_scope', 'family_a');

UPDATE sql_saga.installation SET token = 'newtoken';

SELECT 'old-token cleanup in family B' AS case_name,
       dp_scope.probe($$SELECT sql_saga.sync_history_policies('dp_scope.family_b')$$, NULL, NULL) AS result;
SELECT 'base policy change with nested stale-policy drop in family B' AS case_name,
       dp_scope.probe($$ALTER POLICY tenant_policy ON dp_scope.family_b USING (tenant IS NOT NULL)$$, NULL, NULL) AS result;
SELECT 'unrelated ordinary drop' AS case_name,
       dp_scope.probe($$CREATE TABLE dp_scope.disposable(); DROP TABLE dp_scope.disposable$$, NULL, NULL) AS result;
SELECT 'same-family old-token cleanup in damaged family A' AS case_name,
       dp_scope.probe($$SELECT sql_saga.sync_history_policies('dp_scope.family_a')$$, NULL, NULL) AS result;

SELECT ste.write_history_trigger AS catalog_trigger,
       NOT EXISTS (
           SELECT FROM pg_catalog.pg_trigger AS t
           WHERE t.tgrelid = 'dp_scope.family_a'::regclass
             AND t.tgname = ste.write_history_trigger) AS still_missing
FROM sql_saga.system_time_era AS ste
WHERE (ste.table_schema, ste.table_name) = ('dp_scope', 'family_a');

ROLLBACK TO SAVEPOINT restore_window;
UPDATE sql_saga.installation SET token = 'newtoken';

\echo '=== direct and cascade protection keeps today''s exact errors'
SELECT dp_scope.family('direct_b');
SELECT 'direct write-history trigger' AS case_name,
       dp_scope.probe(
           $$DROP TRIGGER direct_b_system_time_write_history ON dp_scope.direct_b$$,
           'P0001',
           'cannot drop trigger "direct_b_system_time_write_history" on table "dp_scope.direct_b" because it is used in SYSTEM_TIME period') AS result;
SELECT 'direct managed policy' AS case_name,
       dp_scope.probe(
           $$DROP POLICY sql_saga_history_reader_newtoken ON dp_scope.direct_b_history$$,
           'P0001',
           'cannot drop or alter policy "sql_saga_history_reader_newtoken" on "dp_scope.direct_b_history" because it is managed by sql_saga for table "dp_scope.direct_b"') AS result;
SELECT 'history table cascade' AS case_name,
       dp_scope.probe(
           $$DROP TABLE dp_scope.direct_b_history CASCADE$$,
           'P0001',
           'cannot drop table "dp_scope.direct_b_history" because it is used in SYSTEM VERSIONING for table "dp_scope.direct_b"') AS result;
SELECT 'history view cascade' AS case_name,
       dp_scope.probe(
           $$DROP VIEW dp_scope.direct_b_with_history CASCADE$$,
           'P0001',
           'cannot drop view "dp_scope.direct_b_with_history" because it is used in SYSTEM VERSIONING for table "dp_scope.direct_b"') AS result;
SELECT 'query function cascade' AS case_name,
       dp_scope.probe(
           $$DROP FUNCTION dp_scope.direct_b__as_of(timestamptz) CASCADE$$,
           'P0001',
           'cannot drop function "dp_scope.direct_b__as_of(timestamp with time zone)" because it is used in SYSTEM VERSIONING for table "dp_scope.direct_b"') AS result;

CREATE TYPE dp_scope.period_int4range AS RANGE (SUBTYPE = integer);
CREATE TABLE dp_scope.type_member (id int, valid_range dp_scope.period_int4range NOT NULL);
SELECT sql_saga.add_era('dp_scope.type_member', 'valid_range', 'valid');
SELECT 'range type cascade' AS case_name,
       dp_scope.probe(
           $$DROP TYPE dp_scope.period_int4range CASCADE$$,
           'P0001',
           'cannot drop rangetype "dp_scope.period_int4range" because it is used in period "valid" on table "dp_scope.type_member"') AS result;

CREATE TABLE dp_scope.extension_member (id text, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('dp_scope.extension_member', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('dp_scope.extension_member', ARRAY['id'], key_type => 'primary');
-- Keep sql_saga's event trigger installed while exercising the btree_gist object cascade.
-- The surrounding transaction rolls this catalog-only test fixture change back.
DELETE FROM pg_catalog.pg_depend AS d
WHERE d.classid = 'pg_catalog.pg_extension'::regclass
  AND d.objid = (SELECT e.oid FROM pg_catalog.pg_extension AS e WHERE e.extname = 'sql_saga')
  AND d.refclassid = 'pg_catalog.pg_extension'::regclass
  AND d.refobjid = (SELECT e.oid FROM pg_catalog.pg_extension AS e WHERE e.extname = 'btree_gist');
SELECT 'btree_gist extension cascade' AS case_name,
       dp_scope.probe(
           $$DROP EXTENSION btree_gist CASCADE$$,
           'P0001',
           'cannot drop constraint "extension_member_pkey" on table "dp_scope.extension_member" because it is used in era unique key "extension_member_id_valid"') AS result;

ALTER EXTENSION sql_saga DROP FUNCTION sql_saga.write_history();
SELECT 'shared trigger function cascade' AS case_name,
       dp_scope.probe(
           $$DROP FUNCTION sql_saga.write_history() CASCADE$$,
           'P0001',
           'cannot drop trigger "direct_b_system_time_write_history" on table "dp_scope.direct_b" because it is used in SYSTEM_TIME period') AS result;
ALTER EXTENSION sql_saga ADD FUNCTION sql_saga.write_history();

\echo '=== family sanctions, schema cascades, owner cascades, and deterministic first errors'
SELECT dp_scope.family('sanctioned_base');
SELECT 'base table cascade is sanctioned' AS case_name,
       dp_scope.probe($$DROP TABLE dp_scope.sanctioned_base CASCADE$$, NULL, NULL) AS result;
SELECT 'sanctioned API' AS case_name,
       dp_scope.probe($$SELECT sql_saga.drop_system_versioning('dp_scope.sanctioned_base')$$, NULL, NULL) AS result;

CREATE SCHEMA complete_schema;
SELECT dp_scope.family('whole_family', 'complete_schema');
SELECT 'drop schema containing complete family' AS case_name,
       dp_scope.probe($$DROP SCHEMA complete_schema CASCADE$$, NULL, NULL) AS result;

CREATE SCHEMA member_schema;
SELECT dp_scope.family('split_family');
SET LOCAL session_replication_role = replica;
ALTER TABLE dp_scope.split_family_history SET SCHEMA member_schema;
SET LOCAL session_replication_role = origin;
SELECT 'drop schema containing member only' AS case_name,
       dp_scope.probe(
           $$DROP SCHEMA member_schema CASCADE$$,
           'P0001',
           'cannot drop view "dp_scope.split_family_history_gated" because it is used in SYSTEM VERSIONING for table "dp_scope.split_family"') AS result;

CREATE ROLE dp_complete_owner;
CREATE SCHEMA dp_owned AUTHORIZATION dp_complete_owner;
SELECT dp_scope.family('owned_family', 'dp_owned');
ALTER TABLE dp_owned.owned_family OWNER TO dp_complete_owner;
SELECT 'drop owned containing complete family' AS case_name,
       dp_scope.probe($$DROP OWNED BY dp_complete_owner CASCADE$$, NULL, NULL) AS result;

CREATE ROLE dp_member_owner;
SELECT dp_scope.family('owner_split');
SET LOCAL session_replication_role = replica;
ALTER TABLE dp_scope.owner_split_history OWNER TO dp_member_owner;
SET LOCAL session_replication_role = origin;
SELECT 'drop owned containing member only' AS case_name,
       dp_scope.probe(
           $$DROP OWNED BY dp_member_owner CASCADE$$,
           'P0001',
           'cannot drop table "dp_scope.owner_split_history" because it is used in SYSTEM VERSIONING for table "dp_scope.owner_split"') AS result;

SELECT dp_scope.family('mixed_a');
SELECT dp_scope.family('mixed_b');
SELECT 'sanctioned A plus unsanctioned B member' AS case_name,
       dp_scope.probe(
           $$DROP TABLE dp_scope.mixed_a, dp_scope.mixed_b_history CASCADE$$,
           'P0001',
           'cannot drop table "dp_scope.mixed_b_history" because it is used in SYSTEM VERSIONING for table "dp_scope.mixed_b"') AS result;
SELECT 'deterministic first error in multi-member cascade' AS case_name,
       dp_scope.probe(
           $$DROP TABLE dp_scope.mixed_b_history, dp_scope.direct_b_history CASCADE$$,
           'P0001',
           'cannot drop table "dp_scope.direct_b_history" because it is used in SYSTEM VERSIONING for table "dp_scope.direct_b"') AS result;

\echo '=== guards remain narrow when set manually'
SELECT 'history-policy guard does not sanction history table' AS case_name,
       set_config('sql_saga.managing_history_policies', 'on', true) AS guard,
       dp_scope.probe(
           $$DROP TABLE dp_scope.direct_b_history CASCADE$$,
           'P0001',
           'cannot drop table "dp_scope.direct_b_history" because it is used in SYSTEM VERSIONING for table "dp_scope.direct_b"') AS result;
SELECT set_config('sql_saga.managing_history_policies', 'off', true);
SELECT set_config('sql_saga.rebuilding_system_versioning_views', 'dp_scope.mixed_a'::regclass::oid::text, true) IS NOT NULL AS guard_set \gset
SELECT 'rebuild guard for A does not sanction B' AS case_name,
       dp_scope.probe(
           $$DROP VIEW dp_scope.mixed_b_with_history CASCADE$$,
           'P0001',
           'cannot drop view "dp_scope.mixed_b_with_history" because it is used in SYSTEM VERSIONING for table "dp_scope.mixed_b"') AS result;
SELECT set_config('sql_saga.rebuilding_system_versioning_views', '', true);

ROLLBACK;
RESET client_min_messages;

\i sql/include/test_teardown.sql
