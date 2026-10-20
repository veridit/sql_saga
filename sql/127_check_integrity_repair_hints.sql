\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
SET client_min_messages = warning;
\o /dev/null

/*
 * check_integrity must (1) verify that every sql_saga event trigger exists, is a member of the extension and is
 * not disabled, and (2) name the repair that actually fits when a catalogued era table or era column has been
 * renamed or moved behind sql_saga's back (sql_saga 1.0 let that happen: it neither refused nor followed such
 * renames). "drop_era and add_era" is not that repair: it cascades to keys, foreign keys and views, and cannot
 * even run when the catalogued table name no longer resolves. Renaming the object back is the repair, and each
 * case below proves it by applying exactly the repair the message names and running check_integrity again.
 */

BEGIN;
CREATE SCHEMA integrity_hint;
SET search_path = integrity_hint, public;

CREATE FUNCTION integrity_hint.attempt(case_name text, expected_message text)
RETURNS text LANGUAGE plpgsql AS $attempt$
DECLARE
    actual_state text;
    actual_message text;
BEGIN
    BEGIN
        PERFORM sql_saga.check_integrity();
        RETURN case_name || ' | unexpected success';
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS actual_state = RETURNED_SQLSTATE, actual_message = MESSAGE_TEXT;
        IF (actual_state, actual_message) IS DISTINCT FROM ('P0001', expected_message) THEN
            RETURN format('%s | unexpected %s: %s', case_name, actual_state, actual_message);
        END IF;
        RETURN format('%s | %s: %s', case_name, actual_state, actual_message);
    END;
END;
$attempt$;

CREATE FUNCTION integrity_hint.valid(case_name text)
RETURNS text LANGUAGE plpgsql AS $valid$
BEGIN
    BEGIN
        PERFORM sql_saga.check_integrity();
        RETURN case_name || ' | ok';
    EXCEPTION WHEN OTHERS THEN
        RETURN format('%s | unexpected %s: %s', case_name, SQLSTATE, SQLERRM);
    END;
END;
$valid$;

/* A small but complete family: era with synchronized boundaries, primary key and a temporal FK. */
CREATE TABLE parent (id integer NOT NULL, name text, valid_from date, valid_until date, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('parent', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('parent', ARRAY['id'], 'valid', key_type => 'primary', unique_key_name => 'parent_id_valid');
CREATE TABLE child (id integer NOT NULL, parent_id integer, valid_from date, valid_until date, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('child', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_foreign_key('child', ARRAY['parent_id'], 'parent', ARRAY['id'], foreign_key_name => 'child_parent_id_valid');
INSERT INTO parent (id, name, valid_from, valid_until) VALUES (1, 'one', '2024-01-01', 'infinity');
INSERT INTO child (id, parent_id, valid_from, valid_until) VALUES (10, 1, '2024-06-01', 'infinity');
\o
\echo '--- baseline ---'
SELECT integrity_hint.valid('fresh family');

/*
 * The canonical shape of every sql_saga event trigger, read from this fresh install, so it is the definition in the
 * extension script (src/2j_event_triggers.sql) and not another hand-written copy. Every advertised repair below is
 * compared against it, which makes a drift between check_integrity's repair text and the extension script visible.
 */
CREATE TEMP TABLE canonical_event_trigger ON COMMIT DROP AS
SELECT e.evtname, e.evtevent, e.evtfoid::regprocedure::text AS evtfunction, e.evtenabled,
       ARRAY(SELECT t FROM unnest(e.evttags) AS t ORDER BY t COLLATE "C") AS evttags
FROM pg_catalog.pg_event_trigger AS e
WHERE e.evtname LIKE 'sql\_saga\_%';

CREATE FUNCTION integrity_hint.shape(trigger_name name)
RETURNS text LANGUAGE sql AS $shape$
    SELECT CASE
        WHEN NOT EXISTS (SELECT 1 FROM pg_catalog.pg_event_trigger AS e WHERE e.evtname = shape.trigger_name)
            THEN trigger_name || ' | missing'
        WHEN EXISTS (
            SELECT e.evtname, e.evtevent, e.evtfoid::regprocedure::text, e.evtenabled,
                   ARRAY(SELECT t FROM unnest(e.evttags) AS t ORDER BY t COLLATE "C")
            FROM pg_catalog.pg_event_trigger AS e
            JOIN pg_catalog.pg_depend AS d ON d.classid = 'pg_catalog.pg_event_trigger'::regclass AND d.objid = e.oid
                 AND d.refclassid = 'pg_catalog.pg_extension'::regclass AND d.deptype = 'e'
            JOIN pg_catalog.pg_extension AS x ON x.oid = d.refobjid AND x.extname = 'sql_saga'
            WHERE e.evtname = shape.trigger_name
            INTERSECT
            SELECT c.evtname, c.evtevent, c.evtfunction, c.evtenabled, c.evttags
            FROM pg_temp.canonical_event_trigger AS c
            WHERE c.evtname = shape.trigger_name)
            THEN trigger_name || ' | canonical shape and member'
        ELSE trigger_name || ' | differs from the extension script'
    END;
$shape$;

/* The repair check_integrity currently advertises (the text after "; repair with "), to be run verbatim by \gexec. */
CREATE FUNCTION integrity_hint.advertised_repair()
RETURNS text LANGUAGE plpgsql AS $advertised_repair$
DECLARE
    actual_message text;
BEGIN
    BEGIN
        PERFORM sql_saga.check_integrity();
        RETURN NULL;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS actual_message = MESSAGE_TEXT;
        RETURN substring(actual_message FROM '; repair with (.*)$');
    END;
END;
$advertised_repair$;

/* The full message, for scenarios whose exact text is pinned in the expected output rather than in the call. */
CREATE FUNCTION integrity_hint.message(case_name text)
RETURNS text LANGUAGE plpgsql AS $message$
BEGIN
    BEGIN
        PERFORM sql_saga.check_integrity();
        RETURN case_name || ' | unexpected success';
    EXCEPTION WHEN OTHERS THEN
        RETURN format('%s | %s: %s', case_name, SQLSTATE, SQLERRM);
    END;
END;
$message$;

SELECT integrity_hint.shape(c.evtname) FROM pg_temp.canonical_event_trigger AS c ORDER BY c.evtname COLLATE "C";

/*
 * Part 1: event triggers. StatBus migrations disable sql_saga_health_checks around GRANTs and re-enable it.
 * A sql_saga_rename_following that is left disabled silently desynchronizes the catalogue from the tables, so
 * check_integrity must refuse it and name the trigger and the exact repair.
 */
\echo '--- part 1: event triggers ---'
SELECT e.evtname || ' | ' || e.evtenabled::text
FROM pg_event_trigger AS e
WHERE e.evtname LIKE 'sql_saga%'
ORDER BY e.evtname COLLATE "C";

SAVEPOINT event_trigger_disabled;
ALTER EVENT TRIGGER sql_saga_rename_following DISABLE;
SELECT integrity_hint.attempt('rename_following disabled',
    'sql_saga: integrity violation for event trigger "sql_saga_rename_following": event trigger is disabled; repair with ALTER EVENT TRIGGER sql_saga_rename_following ENABLE');
ALTER EVENT TRIGGER sql_saga_rename_following ENABLE;
SELECT integrity_hint.valid('rename_following re-enabled');
ROLLBACK TO SAVEPOINT event_trigger_disabled;
RELEASE SAVEPOINT event_trigger_disabled;

SAVEPOINT health_checks_disabled;
ALTER EVENT TRIGGER sql_saga_health_checks DISABLE;
SELECT integrity_hint.attempt('health_checks disabled',
    'sql_saga: integrity violation for event trigger "sql_saga_health_checks": event trigger is disabled; repair with ALTER EVENT TRIGGER sql_saga_health_checks ENABLE');
ALTER EVENT TRIGGER sql_saga_health_checks ENABLE;
SELECT integrity_hint.valid('health_checks re-enabled');
ROLLBACK TO SAVEPOINT health_checks_disabled;
RELEASE SAVEPOINT health_checks_disabled;

/* Several disabled: the first in name order (COLLATE "C") is reported, deterministically. */
SAVEPOINT several_disabled;
ALTER EVENT TRIGGER sql_saga_rename_following DISABLE;
ALTER EVENT TRIGGER sql_saga_drop_protection DISABLE;
SELECT integrity_hint.attempt('two disabled, first by name',
    'sql_saga: integrity violation for event trigger "sql_saga_drop_protection": event trigger is disabled; repair with ALTER EVENT TRIGGER sql_saga_drop_protection ENABLE');
ALTER EVENT TRIGGER sql_saga_drop_protection ENABLE;
SELECT integrity_hint.attempt('two disabled, next by name',
    'sql_saga: integrity violation for event trigger "sql_saga_rename_following": event trigger is disabled; repair with ALTER EVENT TRIGGER sql_saga_rename_following ENABLE');
ALTER EVENT TRIGGER sql_saga_rename_following ENABLE;
SELECT integrity_hint.valid('both re-enabled');
ROLLBACK TO SAVEPOINT several_disabled;
RELEASE SAVEPOINT several_disabled;

/* ENABLE REPLICA and ENABLE ALWAYS are not disabled (evtenabled R / A). */
SAVEPOINT replica_always;
ALTER EVENT TRIGGER sql_saga_health_checks ENABLE ALWAYS;
ALTER EVENT TRIGGER sql_saga_drop_protection ENABLE REPLICA;
SELECT integrity_hint.valid('ENABLE ALWAYS and ENABLE REPLICA are accepted');
ROLLBACK TO SAVEPOINT replica_always;
RELEASE SAVEPOINT replica_always;

/*
 * An event trigger that still exists but is no longer a member of the extension is not sql_saga's any more.
 * It exists, so attaching it again is the repair.
 */
SAVEPOINT not_member;
ALTER EXTENSION sql_saga DROP EVENT TRIGGER sql_saga_rename_following;
SELECT integrity_hint.attempt('rename_following not an extension member',
    'sql_saga: integrity violation for event trigger "sql_saga_rename_following": event trigger is not a member of extension sql_saga; repair with ALTER EXTENSION sql_saga ADD EVENT TRIGGER sql_saga_rename_following');
ALTER EXTENSION sql_saga ADD EVENT TRIGGER sql_saga_rename_following;
SELECT integrity_hint.valid('rename_following added back');
ROLLBACK TO SAVEPOINT not_member;
RELEASE SAVEPOINT not_member;

/*
 * Dropped outright (after removing it from the extension, the only way to drop a member). ALTER EXTENSION ...
 * ADD EVENT TRIGGER cannot repair this: there is no object left to attach, and ALTER EXTENSION sql_saga UPDATE
 * does not recreate it either (there is no update script to the installed version). The repair is to recreate the
 * trigger with its exact definition from the extension script and then attach it, so the message names both
 * statements, in that order, verbatim. The test runs exactly those two statements and re-checks.
 * Covered for both shapes in the extension script: with a WHEN TAG filter and without one.
 */
SAVEPOINT dropped;
ALTER EXTENSION sql_saga DROP EVENT TRIGGER sql_saga_record_alter_function_resolved;
DROP EVENT TRIGGER sql_saga_record_alter_function_resolved;
SELECT integrity_hint.attempt('record_alter_function_resolved dropped',
    $$sql_saga: integrity violation for event trigger "sql_saga_record_alter_function_resolved": event trigger is missing; repair with CREATE EVENT TRIGGER sql_saga_record_alter_function_resolved ON ddl_command_start WHEN TAG IN ('ALTER FUNCTION', 'ALTER ROUTINE') EXECUTE FUNCTION sql_saga.__internal_record_alter_function_resolved(); ALTER EXTENSION sql_saga ADD EVENT TRIGGER sql_saga_record_alter_function_resolved$$);
CREATE EVENT TRIGGER sql_saga_record_alter_function_resolved ON ddl_command_start WHEN TAG IN ('ALTER FUNCTION', 'ALTER ROUTINE') EXECUTE FUNCTION sql_saga.__internal_record_alter_function_resolved();
ALTER EXTENSION sql_saga ADD EVENT TRIGGER sql_saga_record_alter_function_resolved;
SELECT integrity_hint.valid('record_alter_function_resolved recreated and added back');
ROLLBACK TO SAVEPOINT dropped;
RELEASE SAVEPOINT dropped;

SAVEPOINT dropped_no_filter;
ALTER EXTENSION sql_saga DROP EVENT TRIGGER sql_saga_rename_following;
DROP EVENT TRIGGER sql_saga_rename_following;
SELECT integrity_hint.attempt('rename_following dropped',
    'sql_saga: integrity violation for event trigger "sql_saga_rename_following": event trigger is missing; repair with CREATE EVENT TRIGGER sql_saga_rename_following ON ddl_command_end EXECUTE FUNCTION sql_saga.rename_following(); ALTER EXTENSION sql_saga ADD EVENT TRIGGER sql_saga_rename_following');
CREATE EVENT TRIGGER sql_saga_rename_following ON ddl_command_end EXECUTE FUNCTION sql_saga.rename_following();
ALTER EXTENSION sql_saga ADD EVENT TRIGGER sql_saga_rename_following;
SELECT integrity_hint.valid('rename_following recreated and added back');
ROLLBACK TO SAVEPOINT dropped_no_filter;
RELEASE SAVEPOINT dropped_no_filter;

/*
 * Every one of the five: drop it, run exactly the advertised repair (\gexec, verbatim), and require the recreated
 * trigger to have the canonical shape captured from the fresh install, then a clean check_integrity.
 */
SAVEPOINT dropped_each;
ALTER EXTENSION sql_saga DROP EVENT TRIGGER sql_saga_drop_protection;
DROP EVENT TRIGGER sql_saga_drop_protection;
SELECT integrity_hint.message('sql_saga_drop_protection dropped');
SELECT integrity_hint.advertised_repair() \gexec
SELECT integrity_hint.shape('sql_saga_drop_protection');
SELECT integrity_hint.valid('sql_saga_drop_protection repaired');
ROLLBACK TO SAVEPOINT dropped_each;
ALTER EXTENSION sql_saga DROP EVENT TRIGGER sql_saga_health_checks;
DROP EVENT TRIGGER sql_saga_health_checks;
SELECT integrity_hint.message('sql_saga_health_checks dropped');
SELECT integrity_hint.advertised_repair() \gexec
SELECT integrity_hint.shape('sql_saga_health_checks');
SELECT integrity_hint.valid('sql_saga_health_checks repaired');
ROLLBACK TO SAVEPOINT dropped_each;
ALTER EXTENSION sql_saga DROP EVENT TRIGGER sql_saga_record_alter_function_resolved;
DROP EVENT TRIGGER sql_saga_record_alter_function_resolved;
SELECT integrity_hint.message('sql_saga_record_alter_function_resolved dropped');
SELECT integrity_hint.advertised_repair() \gexec
SELECT integrity_hint.shape('sql_saga_record_alter_function_resolved');
SELECT integrity_hint.valid('sql_saga_record_alter_function_resolved repaired');
ROLLBACK TO SAVEPOINT dropped_each;
ALTER EXTENSION sql_saga DROP EVENT TRIGGER sql_saga_refuse_managed_relation_identity_change;
DROP EVENT TRIGGER sql_saga_refuse_managed_relation_identity_change;
SELECT integrity_hint.message('sql_saga_refuse_managed_relation_identity_change dropped');
SELECT integrity_hint.advertised_repair() \gexec
SELECT integrity_hint.shape('sql_saga_refuse_managed_relation_identity_change');
SELECT integrity_hint.valid('sql_saga_refuse_managed_relation_identity_change repaired');
ROLLBACK TO SAVEPOINT dropped_each;
ALTER EXTENSION sql_saga DROP EVENT TRIGGER sql_saga_rename_following;
DROP EVENT TRIGGER sql_saga_rename_following;
SELECT integrity_hint.message('sql_saga_rename_following dropped');
SELECT integrity_hint.advertised_repair() \gexec
SELECT integrity_hint.shape('sql_saga_rename_following');
SELECT integrity_hint.valid('sql_saga_rename_following repaired');
ROLLBACK TO SAVEPOINT dropped_each;
RELEASE SAVEPOINT dropped_each;

/*
 * Drift: the trigger exists and is a member, but its definition is not the extension's (here the tag filter lost
 * ALTER ROUTINE). check_integrity must refuse it and name a repair that restores the canonical definition: as a
 * member it cannot be dropped directly, so the repair detaches, drops, recreates and attaches it again.
 */
SAVEPOINT drifted;
ALTER EXTENSION sql_saga DROP EVENT TRIGGER sql_saga_record_alter_function_resolved;
DROP EVENT TRIGGER sql_saga_record_alter_function_resolved;
CREATE EVENT TRIGGER sql_saga_record_alter_function_resolved ON ddl_command_start WHEN TAG IN ('ALTER FUNCTION') EXECUTE FUNCTION sql_saga.__internal_record_alter_function_resolved();
ALTER EXTENSION sql_saga ADD EVENT TRIGGER sql_saga_record_alter_function_resolved;
SELECT integrity_hint.shape('sql_saga_record_alter_function_resolved');
SELECT integrity_hint.attempt('record_alter_function_resolved definition drifted',
    $$sql_saga: integrity violation for event trigger "sql_saga_record_alter_function_resolved": event trigger definition differs from the extension's; repair with ALTER EXTENSION sql_saga DROP EVENT TRIGGER sql_saga_record_alter_function_resolved; DROP EVENT TRIGGER sql_saga_record_alter_function_resolved; CREATE EVENT TRIGGER sql_saga_record_alter_function_resolved ON ddl_command_start WHEN TAG IN ('ALTER FUNCTION', 'ALTER ROUTINE') EXECUTE FUNCTION sql_saga.__internal_record_alter_function_resolved(); ALTER EXTENSION sql_saga ADD EVENT TRIGGER sql_saga_record_alter_function_resolved$$);
SELECT integrity_hint.advertised_repair() \gexec
SELECT integrity_hint.shape('sql_saga_record_alter_function_resolved');
SELECT integrity_hint.valid('record_alter_function_resolved definition restored');
ROLLBACK TO SAVEPOINT drifted;
RELEASE SAVEPOINT drifted;

/* A disabled event trigger is reported before any table-level damage it may have let through. */
SAVEPOINT disabled_then_damage;
ALTER EVENT TRIGGER sql_saga_rename_following DISABLE;
ALTER EVENT TRIGGER sql_saga_refuse_managed_relation_identity_change DISABLE;
ALTER TABLE parent RENAME COLUMN valid_range TO period;
SELECT integrity_hint.attempt('disabled trigger reported first',
    'sql_saga: integrity violation for event trigger "sql_saga_refuse_managed_relation_identity_change": event trigger is disabled; repair with ALTER EVENT TRIGGER sql_saga_refuse_managed_relation_identity_change ENABLE');
ROLLBACK TO SAVEPOINT disabled_then_damage;
RELEASE SAVEPOINT disabled_then_damage;

/*
 * Part 2: era tables and era columns renamed or moved without sql_saga following (the shape sql_saga 1.0 left
 * behind: audit scenarios s14, s23, s15, s16). The damage is simulated on the current version by disabling the
 * guard and the follower for the one statement, then re-enabling them, so only the catalogue mismatch remains.
 */
\echo '--- part 2: renamed era table / era column ---'
CREATE PROCEDURE integrity_hint.unguarded(ddl text)
LANGUAGE plpgsql AS $unguarded$
BEGIN
    ALTER EVENT TRIGGER sql_saga_refuse_managed_relation_identity_change DISABLE;
    ALTER EVENT TRIGGER sql_saga_rename_following DISABLE;
    ALTER EVENT TRIGGER sql_saga_health_checks DISABLE;
    EXECUTE ddl;
    ALTER EVENT TRIGGER sql_saga_health_checks ENABLE;
    ALTER EVENT TRIGGER sql_saga_rename_following ENABLE;
    ALTER EVENT TRIGGER sql_saga_refuse_managed_relation_identity_change ENABLE;
END;
$unguarded$;

/*
 * s14: table renamed. The catalogue still says parent; renaming it back is the repair. parent is the referenced
 * side of the temporal FK, so the repair is proven for the referenced table, and below for the referencing child.
 * The rename back runs with the guard and the follower enabled again.
 */
SAVEPOINT table_renamed;
CALL integrity_hint.unguarded('ALTER TABLE integrity_hint.parent RENAME TO parent_renamed');
SELECT integrity_hint.attempt('era table renamed',
    'sql_saga: integrity violation for table "integrity_hint.parent": catalogued base table is missing; if it was renamed or moved, rename or move it back to integrity_hint.parent');
ALTER TABLE integrity_hint.parent_renamed RENAME TO parent;
SELECT integrity_hint.valid('era table renamed back (parent)');
ROLLBACK TO SAVEPOINT table_renamed;
RELEASE SAVEPOINT table_renamed;

/* Same, with the repair applied: the follower and the guard are enabled again, and the rename back is allowed. */
SAVEPOINT table_renamed_repaired;
CALL integrity_hint.unguarded('ALTER TABLE integrity_hint.child RENAME TO child_renamed');
SELECT integrity_hint.attempt('era table renamed (child)',
    'sql_saga: integrity violation for table "integrity_hint.child": catalogued base table is missing; if it was renamed or moved, rename or move it back to integrity_hint.child');
ALTER TABLE integrity_hint.child_renamed RENAME TO child;
SELECT integrity_hint.valid('era table renamed back');
ROLLBACK TO SAVEPOINT table_renamed_repaired;
RELEASE SAVEPOINT table_renamed_repaired;

/* s23: table moved to another schema; quoted names are rendered as SQL identifiers. */
SAVEPOINT table_moved;
CREATE SCHEMA "Odd Schema";
CREATE TABLE "Odd Schema"."Odd Table" (id integer NOT NULL, valid_from date, valid_until date, valid_range daterange NOT NULL);
\o /dev/null
SELECT sql_saga.add_era('"Odd Schema"."Odd Table"', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
\o
SELECT integrity_hint.valid('quoted family');
CALL integrity_hint.unguarded('ALTER TABLE "Odd Schema"."Odd Table" SET SCHEMA integrity_hint');
SELECT integrity_hint.attempt('era table moved',
    'sql_saga: integrity violation for table "Odd Schema.Odd Table": catalogued base table is missing; if it was renamed or moved, rename or move it back to "Odd Schema"."Odd Table"');
ALTER TABLE integrity_hint."Odd Table" SET SCHEMA "Odd Schema";
SELECT integrity_hint.valid('era table moved back');
ROLLBACK TO SAVEPOINT table_moved;
RELEASE SAVEPOINT table_moved;

/* s15: era range column renamed. drop_era and add_era is not the repair; renaming the column back is. */
SAVEPOINT range_renamed;
CALL integrity_hint.unguarded('ALTER TABLE integrity_hint.parent RENAME COLUMN valid_range TO period');
SELECT integrity_hint.attempt('era range column renamed',
    'sql_saga: integrity violation for era "valid" on table "integrity_hint.parent": column "valid_range" is missing; if it was renamed, rename it back with ALTER TABLE integrity_hint.parent RENAME COLUMN <current name> TO valid_range');
ALTER TABLE integrity_hint.parent RENAME COLUMN period TO valid_range;
SELECT integrity_hint.valid('era range column renamed back');
INSERT INTO parent (id, name, valid_from, valid_until) VALUES (2, 'two', '2024-01-01', 'infinity');
SELECT 'synchronization works after repair | ' || valid_range::text FROM parent WHERE id = 2;
ROLLBACK TO SAVEPOINT range_renamed;
RELEASE SAVEPOINT range_renamed;

/* s16: valid_from renamed. */
SAVEPOINT from_renamed;
CALL integrity_hint.unguarded('ALTER TABLE integrity_hint.child RENAME COLUMN valid_from TO starts_on');
SELECT integrity_hint.attempt('era valid_from column renamed',
    'sql_saga: integrity violation for era "valid" on table "integrity_hint.child": column "valid_from" is missing; if it was renamed, rename it back with ALTER TABLE integrity_hint.child RENAME COLUMN <current name> TO valid_from');
ALTER TABLE integrity_hint.child RENAME COLUMN starts_on TO valid_from;
SELECT integrity_hint.valid('era valid_from column renamed back');
ROLLBACK TO SAVEPOINT from_renamed;
RELEASE SAVEPOINT from_renamed;

/* valid_until renamed, with quoting in the statement. */
SAVEPOINT until_renamed;
CALL integrity_hint.unguarded($$ALTER TABLE integrity_hint.child RENAME COLUMN valid_until TO "Ends On"$$);
SELECT integrity_hint.attempt('era valid_until column renamed',
    'sql_saga: integrity violation for era "valid" on table "integrity_hint.child": column "valid_until" is missing; if it was renamed, rename it back with ALTER TABLE integrity_hint.child RENAME COLUMN <current name> TO valid_until');
ALTER TABLE integrity_hint.child RENAME COLUMN "Ends On" TO valid_until;
SELECT integrity_hint.valid('era valid_until column renamed back');
ROLLBACK TO SAVEPOINT until_renamed;
RELEASE SAVEPOINT until_renamed;

/* Quoted table and column names render as SQL identifiers in the repair statement. */
SAVEPOINT quoted_column_renamed;
CREATE SCHEMA "Odd Schema";
CREATE TABLE "Odd Schema"."Odd Table" (id integer NOT NULL, "Valid From" date, valid_until date, "Valid Range" daterange NOT NULL);
\o /dev/null
SELECT sql_saga.add_era('"Odd Schema"."Odd Table"', 'Valid Range', 'valid', valid_from_column_name => 'Valid From', valid_until_column_name => 'valid_until');
\o
CALL integrity_hint.unguarded($$ALTER TABLE "Odd Schema"."Odd Table" RENAME COLUMN "Valid Range" TO vr$$);
SELECT integrity_hint.attempt('quoted era range column renamed',
    'sql_saga: integrity violation for era "valid" on table "Odd Schema.Odd Table": column "Valid Range" is missing; if it was renamed, rename it back with ALTER TABLE "Odd Schema"."Odd Table" RENAME COLUMN <current name> TO "Valid Range"');
ALTER TABLE "Odd Schema"."Odd Table" RENAME COLUMN vr TO "Valid Range";
SELECT integrity_hint.valid('quoted era range column renamed back');
ROLLBACK TO SAVEPOINT quoted_column_renamed;
RELEASE SAVEPOINT quoted_column_renamed;

/*
 * Damage that renaming back cannot repair keeps its existing message (the column exists with the wrong type, or
 * a named check was replaced): drop_era and add_era stays the repair there. Covered in 117; one witness here so
 * the split between the two messages is visible in one place.
 */
SAVEPOINT wrong_type_keeps_message;
UPDATE pg_attribute SET atttypid = 'text'::regtype WHERE attrelid = 'integrity_hint.child'::regclass AND attname = 'valid_range';
SELECT integrity_hint.attempt('era range column of the wrong type',
    'sql_saga: integrity violation for era "valid" on table "integrity_hint.child": column or range type metadata is noncanonical; repair with drop_era and add_era');
ROLLBACK TO SAVEPOINT wrong_type_keeps_message;
RELEASE SAVEPOINT wrong_type_keeps_message;

\echo '--- final ---'
SELECT integrity_hint.valid('unchanged family at the end');
\o /dev/null
ROLLBACK;
\i sql/include/test_teardown.sql
