\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
SET client_min_messages = warning;
\o /dev/null

/*
 * 1.3 small items (pre-existing in 1.2):
 *   1. Identifiers that need quoting: an UPDATE through a for_portion_of view, and disable/enable_temporal_triggers,
 *      built "schema.table"::regclass by concatenation and failed on schemas/tables such as "Odd ""Schema""" and
 *      "Ta'ble" (double quote, apostrophe, space, capitals).
 *   2. Catalog follows renames: era.ephemeral_columns follows ALTER TABLE ... RENAME COLUMN, and
 *      unique_keys.pk_consistency_constraint_names follows ALTER TABLE ... RENAME CONSTRAINT. check_integrity is ok
 *      afterwards and behaviour (coalescing, drop protection, drop_unique_key) is unchanged.
 *   3. disable_temporal_triggers on native temporal foreign keys: they have no sql_saga triggers to disable. It says
 *      so in a NOTICE naming them and the SET CONSTRAINTS statement that defers them; nothing else changes.
 */

BEGIN;
CREATE SCHEMA "Odd ""Schema""";
SET search_path = "Odd ""Schema""", public;

CREATE FUNCTION public.q131_attempt(case_name text, statement text)
RETURNS text LANGUAGE plpgsql AS $q131_attempt$
BEGIN
    BEGIN
        EXECUTE statement;
        RETURN case_name || ' | ok';
    EXCEPTION WHEN OTHERS THEN
        RETURN format('%s | %s: %s', case_name, SQLSTATE, SQLERRM);
    END;
END;
$q131_attempt$;

CREATE FUNCTION public.q131_integrity(case_name text)
RETURNS text LANGUAGE plpgsql AS $q131_integrity$
BEGIN
    BEGIN
        PERFORM sql_saga.check_integrity();
        RETURN case_name || ' | check_integrity ok';
    EXCEPTION WHEN OTHERS THEN
        RETURN format('%s | check_integrity %s: %s', case_name, SQLSTATE, SQLERRM);
    END;
END;
$q131_integrity$;

/* A referenced era table with an odd schema, table and column names, an ephemeral column and a for_portion_of view. */
CREATE TABLE "Ta'ble" ("ID" integer NOT NULL, "Na me" text, "Edit At" timestamptz, valid_from date, valid_until date, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('"Odd ""Schema"""."Ta''ble"', 'valid_range', 'valid',
    valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until', ephemeral_columns => ARRAY['Edit At']::name[]);
SELECT sql_saga.add_unique_key('"Odd ""Schema"""."Ta''ble"', ARRAY['ID'], 'valid', key_type => 'primary', unique_key_name => 'ta_ble_id_valid');
SELECT sql_saga.add_for_portion_of_view('"Odd ""Schema"""."Ta''ble"');

/* A regular table referencing it (sql_saga triggers on "Ta'ble") and a temporal child (native temporal foreign key). */
CREATE TABLE "Re g" (id integer PRIMARY KEY, "Tab ID" integer);
SELECT sql_saga.add_regular_foreign_key('"Odd ""Schema"""."Re g"', ARRAY['Tab ID'], 'ta_ble_id_valid', foreign_key_name => 'Re g FK');
CREATE TABLE "Chi ld" (id integer NOT NULL, "Tab ID" integer, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('"Odd ""Schema"""."Chi ld"', 'valid_range', 'valid');
SELECT sql_saga.add_foreign_key('"Odd ""Schema"""."Chi ld"', ARRAY['Tab ID'], '"Odd ""Schema"""."Ta''ble"', ARRAY['ID'], foreign_key_name => 'Chi ld FK');

/* A plain family with ordinary names, to show that an odd name elsewhere in the catalog does not break it. */
CREATE TABLE public.plain_parent (id integer NOT NULL, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('public.plain_parent', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('public.plain_parent', ARRAY['id'], 'valid', key_type => 'primary', unique_key_name => 'plain_parent_id_valid');
CREATE TABLE public.plain_ref (id integer PRIMARY KEY, parent_id integer);
SELECT sql_saga.add_regular_foreign_key('public.plain_ref', ARRAY['parent_id'], 'plain_parent_id_valid', foreign_key_name => 'plain_ref_fk');

INSERT INTO "Ta'ble" ("ID", "Na me", "Edit At", valid_from, valid_until) VALUES
    (1, 'one', '2024-01-01', '2024-01-01', '2024-06-01'),
    (1, 'uno', '2024-02-01', '2024-06-01', 'infinity'),
    (2, 'two', '2024-01-01', '2024-01-01', 'infinity');
INSERT INTO "Re g" VALUES (100, 1);
INSERT INTO "Chi ld" VALUES (10, 1, '[2024-03-01,infinity)');
INSERT INTO public.plain_parent VALUES (1, '[2024-01-01,infinity)');
INSERT INTO public.plain_ref VALUES (1, 1);

CREATE FUNCTION public.q131_rows()
RETURNS SETOF text LANGUAGE sql AS $q131_rows$
    SELECT format('%s | %s | %s | %s', t."ID", t."Na me", (to_jsonb(t) ->> 'Edit At')::timestamptz::date, t.valid_range)
    FROM "Odd ""Schema"""."Ta'ble" AS t ORDER BY t."ID", t.valid_range;
$q131_rows$;

CREATE FUNCTION public.q131_triggers()
RETURNS SETOF text LANGUAGE sql AS $q131_triggers$
    SELECT format('%s | %s | %s', t.tgrelid::regclass, t.tgname, t.tgenabled)
    FROM pg_catalog.pg_trigger AS t
    WHERE t.tgname IN ('Re g FK_uk_update', 'Re g FK_uk_delete', 'plain_ref_fk_uk_update', 'plain_ref_fk_uk_delete')
    ORDER BY t.tgname COLLATE "C";
$q131_triggers$;

\o
\echo '--- baseline ---'
SELECT public.q131_integrity('fresh');
SELECT foreign_key_name, type, uk_update_trigger, uk_delete_trigger FROM sql_saga.foreign_keys ORDER BY foreign_key_name COLLATE "C";
SELECT public.q131_triggers();
SELECT public.q131_rows();

/* ---------------------------------------------------------------------------------------------------------------- */
\echo '--- 1a: UPDATE through the for_portion_of view of an odd-named table ---'
SAVEPOINT view_update;
SELECT public.q131_attempt('data-only update of ID 2',
    $$UPDATE "Odd ""Schema"""."Ta'ble__for_portion_of_valid" SET "Na me" = 'deux' WHERE "ID" = 2$$);
SELECT public.q131_attempt('update of a portion of ID 2',
    $$UPDATE "Odd ""Schema"""."Ta'ble__for_portion_of_valid" SET "Na me" = 'dos', valid_from = '2025-01-01', valid_until = '2026-01-01' WHERE "ID" = 2$$);
SELECT public.q131_rows();
ROLLBACK TO SAVEPOINT view_update;
RELEASE SAVEPOINT view_update;

/* ---------------------------------------------------------------------------------------------------------------- */
\echo '--- 1b: disable/enable_temporal_triggers with odd names in the catalog ---'
SAVEPOINT manage_triggers;
SELECT public.q131_attempt('disable plain_parent', $$CALL sql_saga.disable_temporal_triggers('public.plain_parent')$$);
SELECT public.q131_triggers();
SELECT public.q131_attempt('enable plain_parent', $$CALL sql_saga.enable_temporal_triggers('public.plain_parent')$$);
SELECT public.q131_triggers();
SELECT public.q131_attempt('disable Ta''ble', $$CALL sql_saga.disable_temporal_triggers('"Odd ""Schema"""."Ta''ble"')$$);
SELECT public.q131_triggers();
SELECT public.q131_attempt('enable Ta''ble', $$CALL sql_saga.enable_temporal_triggers('"Odd ""Schema"""."Ta''ble"')$$);
SELECT public.q131_triggers();
SELECT public.q131_attempt('disable both', $$CALL sql_saga.disable_temporal_triggers('"Odd ""Schema"""."Ta''ble"', 'public.plain_parent')$$);
SELECT public.q131_triggers();
SELECT public.q131_attempt('enable both', $$CALL sql_saga.enable_temporal_triggers('"Odd ""Schema"""."Ta''ble"', 'public.plain_parent')$$);
SELECT public.q131_triggers();
ROLLBACK TO SAVEPOINT manage_triggers;
RELEASE SAVEPOINT manage_triggers;

/* ---------------------------------------------------------------------------------------------------------------- */
\echo '--- 2a: era.ephemeral_columns follows RENAME COLUMN ---'
SAVEPOINT ephemeral_rename;
SELECT e.table_name, e.ephemeral_columns FROM sql_saga.era AS e WHERE e.table_name = 'Ta''ble';
ALTER TABLE "Ta'ble" RENAME COLUMN "Edit At" TO "Edited ""At""";
SELECT e.table_name, e.ephemeral_columns FROM sql_saga.era AS e WHERE e.table_name = 'Ta''ble';
SELECT public.q131_integrity('after ephemeral column rename');
/* A non-ephemeral rename leaves the list alone. */
ALTER TABLE "Ta'ble" RENAME COLUMN "Na me" TO "Na_me";
SELECT e.table_name, e.ephemeral_columns FROM sql_saga.era AS e WHERE e.table_name = 'Ta''ble';
ALTER TABLE "Ta'ble" RENAME COLUMN "Na_me" TO "Na me";
SELECT public.q131_integrity('after non-ephemeral column rename and back');
/*
 * Behaviour unchanged: the renamed column is still ephemeral. Making ID 1 'one' on [2024-06-01,infinity) with a
 * different edit time coalesces with the adjacent 'one' segment into a single row, through the view (era-level
 * ephemeral columns) and through temporal_merge directly (auto-discovered from the era).
 */
SAVEPOINT through_view;
SELECT public.q131_attempt('view update after rename',
    $$UPDATE "Odd ""Schema"""."Ta'ble__for_portion_of_valid" SET "Na me" = 'one', "Edited ""At""" = '2024-07-01', valid_from = '2024-06-01', valid_until = 'infinity' WHERE "ID" = 1$$);
SELECT format('%s | %s | %s | %s', t."ID", t."Na me", t."Edited ""At"""::date, t.valid_range) FROM "Ta'ble" AS t ORDER BY t."ID", t.valid_range;
ROLLBACK TO SAVEPOINT through_view;
RELEASE SAVEPOINT through_view;
SAVEPOINT through_merge;
CREATE TEMP TABLE q131_source (row_id bigint, "ID" integer, "Na me" text, "Edited ""At""" timestamptz, valid_range daterange) ON COMMIT DROP;
INSERT INTO q131_source VALUES (1, 1, 'one', '2024-07-01', '[2024-06-01,infinity)');
SELECT public.q131_attempt('temporal_merge after rename',
    $$CALL sql_saga.temporal_merge(target_table => '"Odd ""Schema"""."Ta''ble"', source_table => 'q131_source', primary_identity_columns => ARRAY['ID'], mode => 'MERGE_ENTITY_PATCH')$$);
SELECT format('%s | %s | %s | %s', t."ID", t."Na me", t."Edited ""At"""::date, t.valid_range) FROM "Ta'ble" AS t ORDER BY t."ID", t.valid_range;
ROLLBACK TO SAVEPOINT through_merge;
RELEASE SAVEPOINT through_merge;
ROLLBACK TO SAVEPOINT ephemeral_rename;
RELEASE SAVEPOINT ephemeral_rename;

/* ---------------------------------------------------------------------------------------------------------------- */
\echo '--- 2b: unique_keys.pk_consistency_constraint_names follows RENAME CONSTRAINT ---'
\o /dev/null
CREATE TABLE "Nat Tab" (id integer NOT NULL, "Co de" text NOT NULL, valid_range daterange NOT NULL, PRIMARY KEY (id, valid_range WITHOUT OVERLAPS));
SELECT sql_saga.add_era('"Odd ""Schema"""."Nat Tab"', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('"Odd ""Schema"""."Nat Tab"', ARRAY['Co de'], 'valid', key_type => 'natural', unique_key_name => 'nat_code');
INSERT INTO "Nat Tab" VALUES (1, 'A', '[2024-01-01,2025-01-01)');
\o
SAVEPOINT consistency_rename;
SELECT uk.unique_key_name, uk.pk_consistency_constraint_names FROM sql_saga.unique_keys AS uk WHERE uk.unique_key_name = 'nat_code';
ALTER TABLE "Nat Tab" RENAME CONSTRAINT nat_code_pk_consistency_excl TO "Nat ""Code"" Excl";
SELECT uk.unique_key_name, uk.pk_consistency_constraint_names FROM sql_saga.unique_keys AS uk WHERE uk.unique_key_name = 'nat_code';
SELECT public.q131_integrity('after consistency constraint rename');
/*
 * Behaviour unchanged: the constraint still enforces (code A later under another id violates only the consistency
 * constraint, not the temporal unique key), is still drop-protected, and drop_unique_key removes it.
 */
SELECT public.q131_attempt('code A later under another id',
    $$INSERT INTO "Odd ""Schema"""."Nat Tab" VALUES (2, 'A', '[2025-01-01,infinity)')$$);
SELECT public.q131_attempt('drop the renamed constraint',
    $$ALTER TABLE "Odd ""Schema"""."Nat Tab" DROP CONSTRAINT "Nat ""Code"" Excl"$$);
SELECT public.q131_attempt('drop_unique_key',
    $$SELECT sql_saga.drop_unique_key('"Odd ""Schema"""."Nat Tab"', ARRAY['Co de'], 'valid')$$);
SELECT c.conname FROM pg_catalog.pg_constraint AS c WHERE c.conrelid = '"Nat Tab"'::regclass AND c.contype <> 'n' ORDER BY c.conname COLLATE "C";
SELECT public.q131_integrity('after drop_unique_key');
ROLLBACK TO SAVEPOINT consistency_rename;
RELEASE SAVEPOINT consistency_rename;
SELECT public.q131_integrity('rolled back');

/* ---------------------------------------------------------------------------------------------------------------- */
\echo '--- 3: disable_temporal_triggers and native temporal foreign keys ---'
\o /dev/null
CREATE TABLE kid (id integer NOT NULL, ta_id integer, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('"Odd ""Schema""".kid', 'valid_range', 'valid');
SELECT sql_saga.add_foreign_key('"Odd ""Schema""".kid', ARRAY['ta_id'], '"Odd ""Schema"""."Ta''ble"', ARRAY['ID'], foreign_key_name => 'kid_fk');
\o
SELECT 'comment documents SET CONSTRAINTS', obj_description('sql_saga.disable_temporal_triggers(regclass[])'::regprocedure, 'pg_proc') LIKE '%native temporal foreign key%SET CONSTRAINTS%DEFERRED%';
SAVEPOINT native_notice;
SET client_min_messages = notice;
/* The referencing side only: no sql_saga triggers to disable, the native key of "Chi ld" is named. */
SELECT public.q131_attempt('disable Chi ld', $$CALL sql_saga.disable_temporal_triggers('"Odd ""Schema"""."Chi ld"')$$);
/* Two referencing tables: both native keys, in one notice. */
SELECT public.q131_attempt('disable kid and Chi ld', $$CALL sql_saga.disable_temporal_triggers('"Odd ""Schema""".kid', '"Odd ""Schema"""."Chi ld"')$$);
/* The referenced side (with a referencing table again): the regular key's triggers are disabled, each native key named once. */
SELECT public.q131_attempt('disable Ta''ble and Chi ld', $$CALL sql_saga.disable_temporal_triggers('"Odd ""Schema"""."Ta''ble"', '"Odd ""Schema"""."Chi ld"')$$);
/* No native key involved: silent, as before. enable_temporal_triggers is silent. */
SELECT public.q131_attempt('disable plain_parent', $$CALL sql_saga.disable_temporal_triggers('public.plain_parent')$$);
SELECT public.q131_attempt('enable Ta''ble and plain_parent', $$CALL sql_saga.enable_temporal_triggers('"Odd ""Schema"""."Ta''ble"', 'public.plain_parent')$$);
SET client_min_messages = warning;
SELECT public.q131_triggers();
/* Nothing else changed on the native keys: still DEFERRABLE INITIALLY IMMEDIATE, no triggers disabled. */
SELECT c.conname, c.condeferrable, c.condeferred,
       (SELECT count(*) FROM pg_catalog.pg_trigger AS t WHERE t.tgconstraint = c.oid AND t.tgenabled = 'D') AS disabled_triggers
FROM pg_catalog.pg_constraint AS c WHERE c.conname IN ('Chi ld FK', 'kid_fk') ORDER BY c.conname COLLATE "C";
/*
 * The named statement works: with it (and the regular key's triggers disabled), removing and restoring the
 * referenced entity is tolerated until the checks run.
 */
SAVEPOINT deferred_native;
SELECT public.q131_attempt('disable Ta''ble', $$CALL sql_saga.disable_temporal_triggers('"Odd ""Schema"""."Ta''ble"')$$);
SET CONSTRAINTS "Odd ""Schema"""."Chi ld FK", "Odd ""Schema""".kid_fk DEFERRED;
SELECT public.q131_attempt('delete entity 1 with the native keys deferred', $$DELETE FROM "Odd ""Schema"""."Ta'ble" WHERE "ID" = 1$$);
SELECT public.q131_attempt('restore entity 1',
    $$INSERT INTO "Odd ""Schema"""."Ta'ble" ("ID", "Na me", valid_from, valid_until) VALUES (1, 'back', '2024-01-01', 'infinity')$$);
SELECT public.q131_attempt('deferred checks at SET CONSTRAINTS ALL IMMEDIATE', 'SET CONSTRAINTS ALL IMMEDIATE');
ROLLBACK TO SAVEPOINT deferred_native;
RELEASE SAVEPOINT deferred_native;
/* Without it, the native key still checks at statement end: disable_temporal_triggers did not change it. */
SELECT public.q131_attempt('disable Ta''ble', $$CALL sql_saga.disable_temporal_triggers('"Odd ""Schema"""."Ta''ble"')$$);
SELECT public.q131_attempt('delete a referenced row without SET CONSTRAINTS',
    $$DELETE FROM "Odd ""Schema"""."Ta'ble" WHERE "ID" = 1 AND valid_from = '2024-06-01'$$);
ROLLBACK TO SAVEPOINT native_notice;
RELEASE SAVEPOINT native_notice;

\echo '--- final ---'
SELECT public.q131_integrity('end');
SELECT public.q131_triggers();
\o /dev/null
ROLLBACK;
\i sql/include/test_teardown.sql
\set ECHO none
