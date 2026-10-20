\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/*
 * The referencing (child) side of a regular foreign key is a constraint trigger, not a CHECK constraint
 * (design B, tmp/fable_audit_v13.md section 2.3 and P2 item 8; proof tmp/v13_design_B_proof.md). sql/125 states
 * enforcement and restore safety; this test states the rest of the contract:
 *
 * Part 1  temporal_merge on a referenced parent: reshaping the parent's timeline (MERGE_ENTITY_REPLACE with
 *         DELETE_MISSING_TIMELINE, a MERGE_ENTITY_UPSERT split) succeeds while every reference stays valid at
 *         the end, for a regular and for a temporal child; a merge that really orphans a child is refused.
 *         Today the regular case fails: temporal_merge defers only u/x/f constraints, never sql_saga's
 *         constraint triggers, and the referenced-side check refuses deleting one timeline segment of a key
 *         that still exists in the parent.
 * Part 2  the child-side object: after add_foreign_key the child carries one constraint trigger, deferrable,
 *         initially immediate, AFTER INSERT OR UPDATE OF the foreign key columns, FOR EACH ROW; the catalog
 *         names it in foreign_keys.fk_check_constraint (a constraint trigger is a pg_constraint row of type
 *         't', so the column keeps its meaning, and SET CONSTRAINTS accepts the name). drop_foreign_key
 *         removes it, drop protection refuses dropping it directly, RENAME COLUMN of a child key column and of
 *         a referenced column keep enforcement and check_integrity, check_integrity reports the trigger
 *         missing or disabled with a repair that works when run verbatim.
 * Part 3  disable_temporal_triggers / enable_temporal_triggers (StatBus's admin.disable_temporal_triggers passes
 *         every parent and child table) disable and re-enable the child-side trigger too.
 * Part 4  MATCH: SIMPLE and FULL are honoured as by the temporal C code (FULL refuses a partly NULL key);
 *         PARTIAL is refused when the foreign key is created.
 *
 * Every probe is rolled back (savepoints) and reports only accepted/refused unless a message is the subject.
 */

BEGIN;
CREATE SCHEMA rft;
SET search_path = rft, public;

-- Run a statement and keep its effects: 'ok', or the SQLSTATE of the error (effects rolled back).
CREATE FUNCTION rft.run(statement text) RETURNS text LANGUAGE plpgsql AS $run$
BEGIN
    IF statement IS NULL THEN
        RETURN 'nothing to run';
    END IF;
    BEGIN
        EXECUTE statement;
        RETURN 'ok';
    EXCEPTION WHEN OTHERS THEN
        RETURN 'error ' || SQLSTATE;
    END;
END;
$run$;

-- One statement, then every deferrable constraint checked (end of transaction), rolled back.
CREATE FUNCTION rft.probe(statement text) RETURNS text LANGUAGE plpgsql AS $probe$
BEGIN
    BEGIN
        EXECUTE statement;
        SET CONSTRAINTS ALL IMMEDIATE;
        RAISE EXCEPTION USING ERRCODE = 'P0004', MESSAGE = 'accepted';
    EXCEPTION
        WHEN assert_failure THEN RETURN 'accepted';
        WHEN integrity_constraint_violation THEN RETURN 'refused';
    END;
END;
$probe$;

-- Like probe, but any error is reported by SQLSTATE (for refusals whose wording is not specified here).
CREATE FUNCTION rft.outcome(statement text) RETURNS text LANGUAGE plpgsql AS $outcome$
BEGIN
    BEGIN
        EXECUTE statement;
        SET CONSTRAINTS ALL IMMEDIATE;
        RAISE EXCEPTION USING ERRCODE = 'P0004', MESSAGE = 'accepted';
    EXCEPTION
        WHEN assert_failure THEN RETURN 'accepted';
        WHEN OTHERS THEN RETURN 'refused ' || SQLSTATE;
    END;
END;
$outcome$;

-- An error whose exact message is the subject: SQLSTATE and message, rolled back.
CREATE FUNCTION rft.error_of(statement text) RETURNS text LANGUAGE plpgsql AS $error_of$
BEGIN
    BEGIN
        EXECUTE statement;
        RAISE EXCEPTION USING ERRCODE = 'P0004', MESSAGE = 'accepted';
    EXCEPTION
        WHEN assert_failure THEN RETURN 'accepted';
        WHEN OTHERS THEN RETURN SQLSTATE || ': ' || SQLERRM;
    END;
END;
$error_of$;

CREATE FUNCTION rft.integrity() RETURNS text LANGUAGE plpgsql AS $integrity$
BEGIN
    BEGIN
        PERFORM sql_saga.check_integrity();
        RETURN 'ok';
    EXCEPTION WHEN OTHERS THEN
        RETURN SQLSTATE || ': ' || SQLERRM;
    END;
END;
$integrity$;

-- The repair check_integrity currently advertises (the text after "; repair with "), for \gexec.
CREATE FUNCTION rft.advertised_repair() RETURNS text LANGUAGE plpgsql AS $advertised_repair$
BEGIN
    BEGIN
        PERFORM sql_saga.check_integrity();
        RETURN NULL;
    EXCEPTION WHEN OTHERS THEN
        RETURN substring(SQLERRM FROM '; repair with (.*)$');
    END;
END;
$advertised_repair$;

/*
 * The user-visible shape of the child-side object of a regular foreign key: every non-internal trigger on the
 * child table, whether the catalog names it, and its constraint/trigger properties.
 */
CREATE FUNCTION rft.child_side(fk_name name) RETURNS SETOF text LANGUAGE sql AS $child_side$
    SELECT format('%s | %s | catalogued %s | %s | %s | %s | %s | %s',
        fk.foreign_key_name,
        t.tgname,
        (t.tgname = fk.fk_check_constraint)::text,
        CASE WHEN c.contype = 't' THEN 'constraint trigger' ELSE 'plain trigger' END,
        CASE WHEN c.condeferrable THEN 'deferrable' ELSE 'not deferrable' END
            || CASE WHEN c.condeferred THEN ' initially deferred' ELSE ' initially immediate' END,
        CASE WHEN t.tgtype & 2 = 2 THEN 'before' WHEN t.tgtype & 64 = 64 THEN 'instead of' ELSE 'after' END
            || concat_ws(' or ',
                   CASE WHEN t.tgtype & 4 = 4 THEN ' insert' END,
                   CASE WHEN t.tgtype & 16 = 16 THEN 'update' END
                       || CASE WHEN t.tgattr::int2[] <> '{}' THEN ' of ' || (
                              SELECT string_agg(a.attname, ', ' ORDER BY u.ord)
                              FROM unnest(t.tgattr::int2[]) WITH ORDINALITY AS u (attnum, ord)
                              JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attnum) = (t.tgrelid, u.attnum)) ELSE '' END,
                   CASE WHEN t.tgtype & 8 = 8 THEN 'delete' END,
                   CASE WHEN t.tgtype & 32 = 32 THEN 'truncate' END),
        CASE WHEN t.tgtype & 1 = 1 THEN 'for each row' ELSE 'for each statement' END,
        CASE t.tgenabled WHEN 'O' THEN 'enabled' WHEN 'D' THEN 'disabled' ELSE 'enabled ' || t.tgenabled::text END)
    FROM sql_saga.foreign_keys AS fk
    JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = format('%I.%I', fk.table_schema, fk.table_name)::regclass AND NOT t.tgisinternal
    LEFT JOIN pg_catalog.pg_constraint AS c ON c.oid = t.tgconstraint
    WHERE fk.foreign_key_name = child_side.fk_name
    UNION ALL
    SELECT format('%s | no trigger on the child table', fk.foreign_key_name)
    FROM sql_saga.foreign_keys AS fk
    WHERE fk.foreign_key_name = child_side.fk_name
      AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t
                      WHERE t.tgrelid = format('%I.%I', fk.table_schema, fk.table_name)::regclass AND NOT t.tgisinternal)
    ORDER BY 1;
$child_side$;

-- CHECK constraints on a table that call a function outside pg_catalog (design B leaves none).
CREATE FUNCTION rft.helper_checks(table_oid regclass) RETURNS bigint LANGUAGE sql AS $helper_checks$
    SELECT count(*)
    FROM pg_catalog.pg_constraint AS c
    JOIN pg_catalog.pg_depend AS d
      ON (d.classid, d.objid, d.refclassid) = ('pg_catalog.pg_constraint'::regclass, c.oid, 'pg_catalog.pg_proc'::regclass)
    JOIN pg_catalog.pg_proc AS p ON p.oid = d.refobjid
    WHERE c.conrelid = helper_checks.table_oid AND c.contype = 'c' AND p.pronamespace <> 'pg_catalog'::regnamespace;
$helper_checks$;

/*
 * The parent: a temporal table with a primary key (id) and a composite natural key (ident_type, ident).
 * Legal units 1 and 2 have two timeline segments each, 3 and 4 one.
 */
CREATE TABLE rft.legal_unit (id int NOT NULL, ident_type text NOT NULL, ident text NOT NULL, name text,
                             valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('rft.legal_unit', 'valid_range', 'valid',
    valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('rft.legal_unit', ARRAY['id'], key_type => 'primary', unique_key_name => 'legal_unit_id_valid');
SELECT sql_saga.add_unique_key('rft.legal_unit', ARRAY['ident_type', 'ident'], key_type => 'natural',
    unique_key_name => 'legal_unit_ident_valid');
INSERT INTO rft.legal_unit (id, ident_type, ident, name, valid_from, valid_until) VALUES
    (1, 'tax', '100', 'a', '2020-01-01', '2022-01-01'),
    (1, 'tax', '100', 'b', '2022-01-01', 'infinity'),
    (2, 'tax', '200', 'a', '2020-01-01', '2022-01-01'),
    (2, 'tax', '200', 'b', '2022-01-01', 'infinity'),
    (3, 'tax', '300', 'a', '2020-01-01', 'infinity'),
    (4, 'tax', '400', 'a', '2020-01-01', 'infinity');

-- Regular children: activity -> legal unit 1 by id, contact -> (tax, 300) by the natural key (MATCH FULL).
CREATE TABLE rft.activity (id int PRIMARY KEY, legal_unit_id int, note text);
CREATE TABLE rft.contact (id int PRIMARY KEY, ident_type text, ident text);
SELECT sql_saga.add_foreign_key('rft.activity', ARRAY['legal_unit_id'], 'rft.legal_unit', ARRAY['id']);
SELECT sql_saga.add_regular_foreign_key('rft.contact', ARRAY['ident_type', 'ident'], 'legal_unit_ident_valid', match_type => 'FULL');
-- Temporal child: stat -> legal unit 2 over its whole timeline.
CREATE TABLE rft.stat (id int NOT NULL, legal_unit_id int, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('rft.stat', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_foreign_key('rft.stat', ARRAY['legal_unit_id'], 'rft.legal_unit', ARRAY['id']);
INSERT INTO rft.activity VALUES (1, 1, NULL), (2, NULL, NULL);
INSERT INTO rft.contact VALUES (1, 'tax', '300'), (2, NULL, NULL);
INSERT INTO rft.stat (id, legal_unit_id, valid_from, valid_until) VALUES (1, 2, '2020-01-01', 'infinity');

CREATE TEMP TABLE merge_source (row_id int, id int, ident_type text, ident text, name text, valid_from date, valid_until date);

-- A merge of the given source rows into legal_unit, checked at the end, rolled back.
CREATE FUNCTION rft.merge(mode sql_saga.temporal_merge_mode, delete_mode sql_saga.temporal_merge_delete_mode, source_rows text)
RETURNS text LANGUAGE plpgsql AS $merge$
BEGIN
    TRUNCATE pg_temp.merge_source;
    EXECUTE 'INSERT INTO pg_temp.merge_source VALUES ' || source_rows;
    RETURN rft.probe(format($$CALL sql_saga.temporal_merge('rft.legal_unit', 'pg_temp.merge_source', ARRAY['id'],
        mode => %L, delete_mode => %L)$$, mode, delete_mode));
END;
$merge$;

-- legal_unit's timeline after a merge, kept (the caller rolls back).
CREATE FUNCTION rft.merged(mode sql_saga.temporal_merge_mode, delete_mode sql_saga.temporal_merge_delete_mode, source_rows text)
RETURNS text LANGUAGE plpgsql AS $merged$
DECLARE
    timeline text;
BEGIN
    TRUNCATE pg_temp.merge_source;
    EXECUTE 'INSERT INTO pg_temp.merge_source VALUES ' || source_rows;
    BEGIN
        CALL sql_saga.temporal_merge('rft.legal_unit', 'pg_temp.merge_source', ARRAY['id'], mode => mode, delete_mode => delete_mode);
        SET CONSTRAINTS ALL IMMEDIATE;
    EXCEPTION WHEN integrity_constraint_violation THEN
        RETURN 'refused';
    END;
    SELECT string_agg(format('%s %s [%s,%s)', lu.id, lu.name, lu.valid_from, lu.valid_until), ', ' ORDER BY lu.id, lu.valid_from)
    INTO timeline FROM rft.legal_unit AS lu;
    RETURN timeline;
END;
$merged$;

\o
\echo '--- baseline'
SELECT rft.integrity();

\echo '--- part 1: temporal_merge on a parent referenced by a regular child (activity -> 1) and a temporal child (stat -> 2)'
-- Coalesce the two segments of legal unit 1 into one (one DELETE, one UPDATE): the key exists throughout.
SELECT 'regular child: REPLACE + DELETE_MISSING_TIMELINE coalesces the timeline | '
    || rft.merge('MERGE_ENTITY_REPLACE', 'DELETE_MISSING_TIMELINE', $$(1, 1, 'tax', '100', 'c', '2020-01-01', 'infinity')$$);
SAVEPOINT merged;
SELECT 'regular child: resulting timeline | '
    || rft.merged('MERGE_ENTITY_REPLACE', 'DELETE_MISSING_TIMELINE', $$(1, 1, 'tax', '100', 'c', '2020-01-01', 'infinity')$$);
SELECT 'regular child: check_integrity after the merge | ' || rft.integrity();
ROLLBACK TO SAVEPOINT merged;
-- Shrink legal unit 1 to [2023, infinity): its first segment is deleted, the key still exists.
SELECT 'regular child: REPLACE + DELETE_MISSING_TIMELINE shrinks the timeline | '
    || rft.merge('MERGE_ENTITY_REPLACE', 'DELETE_MISSING_TIMELINE', $$(1, 1, 'tax', '100', 'c', '2023-01-01', 'infinity')$$);
-- Split the first segment of legal unit 1 in three.
SELECT 'regular child: UPSERT splits the timeline | '
    || rft.merge('MERGE_ENTITY_UPSERT', 'NONE', $$(1, 1, 'tax', '100', 'm', '2021-01-01', '2021-06-01')$$);
-- Legal unit 1 is not in the source: deleted, activity 1 orphaned.
SELECT 'regular child: REPLACE + DELETE_MISSING_ENTITIES orphaning it is refused | '
    || rft.merge('MERGE_ENTITY_REPLACE', 'DELETE_MISSING_ENTITIES',
                 $$(1, 2, 'tax', '200', 'a', '2020-01-01', 'infinity'), (2, 3, 'tax', '300', 'a', '2020-01-01', 'infinity'), (3, 4, 'tax', '400', 'a', '2020-01-01', 'infinity')$$);
-- The same for the referenced key of contact (natural key (tax, 300) of legal unit 3) together with 1.
SELECT 'regular children: REPLACE + DELETE_MISSING_TIMELINE on legal units 1 and 3 | '
    || rft.merge('MERGE_ENTITY_REPLACE', 'DELETE_MISSING_TIMELINE',
                 $$(1, 1, 'tax', '100', 'c', '2020-01-01', 'infinity'), (2, 3, 'tax', '300', 'c', '2021-01-01', 'infinity')$$);
SELECT 'temporal child: REPLACE + DELETE_MISSING_TIMELINE coalesces the timeline | '
    || rft.merge('MERGE_ENTITY_REPLACE', 'DELETE_MISSING_TIMELINE', $$(1, 2, 'tax', '200', 'c', '2020-01-01', 'infinity')$$);
SELECT 'temporal child: UPSERT splits the timeline | '
    || rft.merge('MERGE_ENTITY_UPSERT', 'NONE', $$(1, 2, 'tax', '200', 'm', '2021-01-01', '2021-06-01')$$);
-- The source covers legal unit 2 only from 2022: its first segment is deleted, stat 1 is uncovered in 2020-2021.
SELECT 'temporal child: REPLACE + DELETE_MISSING_TIMELINE uncovering it is refused | '
    || rft.merge('MERGE_ENTITY_REPLACE', 'DELETE_MISSING_TIMELINE', $$(1, 2, 'tax', '200', 'c', '2022-01-01', 'infinity')$$);
SELECT 'both children: one merge reshaping legal units 1 and 2 | '
    || rft.merge('MERGE_ENTITY_REPLACE', 'DELETE_MISSING_TIMELINE',
                 $$(1, 1, 'tax', '100', 'c', '2020-01-01', 'infinity'), (2, 2, 'tax', '200', 'c', '2020-01-01', 'infinity')$$);
-- Remove every old segment of legal unit 1 and insert its replacement [2019, 2020): the whole old timeline is
-- replaced. (Gap tolerance itself is established by the explicit deferred delete-all and reinsert probe below.)
SELECT 'regular child: REPLACE + DELETE_MISSING_TIMELINE replacing every old segment | '
    || rft.merge('MERGE_ENTITY_REPLACE', 'DELETE_MISSING_TIMELINE', $$(1, 1, 'tax', '100', 'c', '2019-01-01', '2020-01-01')$$);
SAVEPOINT replaced;
SELECT 'regular child: timeline after replacing every old segment | '
    || rft.merged('MERGE_ENTITY_REPLACE', 'DELETE_MISSING_TIMELINE', $$(1, 1, 'tax', '100', 'c', '2019-01-01', '2020-01-01')$$);
ROLLBACK TO SAVEPOINT replaced;

\echo '--- part 1: referenced-side semantics without temporal_merge (a regular reference needs the key at some time)'
SELECT 'DELETE one of the two timeline rows of legal unit 1 | '
    || rft.probe($$DELETE FROM rft.legal_unit WHERE id = 1 AND valid_from = '2022-01-01'$$);
SELECT 'deferred: DELETE every row of legal unit 1, reinsert the key, then check | '
    || rft.probe($$SET CONSTRAINTS ALL DEFERRED; DELETE FROM rft.legal_unit WHERE id = 1;
                   INSERT INTO rft.legal_unit (id, ident_type, ident, name, valid_from, valid_until) VALUES (1, 'tax', '100', 'z', '2019-01-01', 'infinity')$$);
SELECT 'deferred: DELETE every row of legal unit 1 without reinsert, then check | '
    || rft.probe($$SET CONSTRAINTS ALL DEFERRED; DELETE FROM rft.legal_unit WHERE id = 1$$);
SELECT 'immediate: DELETE every row of legal unit 1 | ' || rft.probe('DELETE FROM rft.legal_unit WHERE id = 1');
SELECT 'deferred: change the natural key of legal unit 3 and change it back, then check | '
    || rft.probe($$SET CONSTRAINTS ALL DEFERRED; UPDATE rft.legal_unit SET ident = '301' WHERE id = 3;
                   UPDATE rft.legal_unit SET ident = '300' WHERE id = 3$$);

\echo '--- part 1: temporal_merge defers only sql_saga''s own constraint triggers'
/*
 * SET CONSTRAINTS schema.name selects every constraint of that name in the schema. A constraint sql_saga does not
 * manage, in the same schema and with the same name as a constraint temporal_merge defers, would be deferred (and
 * later forced immediate) too. temporal_merge must refuse instead, before any DML, naming the conflicting
 * constraint. Changes made by a refused CALL are rolled back, so whether DML ran is observed through a sequence
 * (nextval is not transactional): a BEFORE INSERT/UPDATE/DELETE trigger on the merge target advances it.
 * Controls: the same unrelated constraint in another schema does not conflict; a direct DELETE is unaffected.
 */
SAVEPOINT deferral_collision;
\o /dev/null
CREATE SEQUENCE rft.target_dml_seen;
CREATE FUNCTION rft.note_target_dml() RETURNS trigger LANGUAGE plpgsql AS $note_target_dml$
BEGIN
    PERFORM nextval('rft.target_dml_seen');
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$note_target_dml$;
CREATE TRIGGER note_target_dml BEFORE INSERT OR UPDATE OR DELETE ON rft.legal_unit
    FOR EACH ROW EXECUTE FUNCTION rft.note_target_dml();
-- The number of legal_unit rows DML has touched so far (a sequence never goes back, also not on rollback).
CREATE FUNCTION rft.target_dml_count() RETURNS bigint LANGUAGE sql AS $target_dml_count$
    SELECT CASE WHEN s.is_called THEN s.last_value ELSE 0 END FROM rft.target_dml_seen AS s;
$target_dml_count$;
CREATE FUNCTION rft.unrelated_noop() RETURNS trigger LANGUAGE plpgsql AS $unrelated_noop$
BEGIN
    RETURN NULL;
END;
$unrelated_noop$;
CREATE TABLE rft.unrelated (id int);
TRUNCATE pg_temp.merge_source;
INSERT INTO pg_temp.merge_source VALUES (1, 1, 'tax', '100', 'c', '2020-01-01', 'infinity');
\o
-- Control: an unrelated constraint of that name in another schema does not conflict.
SAVEPOINT other_schema;
\o /dev/null
CREATE SCHEMA rft_other;
CREATE TABLE rft_other.unrelated (id int);
SELECT format('CREATE CONSTRAINT TRIGGER %I AFTER INSERT ON rft_other.unrelated DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION rft.unrelated_noop()',
              fk.uk_delete_trigger)
FROM sql_saga.foreign_keys AS fk WHERE fk.foreign_key_name = 'activity_legal_unit_id_fkey' \gexec
\o
SELECT 'same-named constraint in another schema, temporal_merge | '
    || rft.run($$CALL sql_saga.temporal_merge('rft.legal_unit', 'pg_temp.merge_source', ARRAY['id'],
                 mode => 'MERGE_ENTITY_REPLACE', delete_mode => 'DELETE_MISSING_TIMELINE')$$);
ROLLBACK TO SAVEPOINT other_schema;
-- The same unrelated constraint in the schema of the managed one.
\o /dev/null
SELECT format('CREATE CONSTRAINT TRIGGER %I AFTER INSERT ON rft.unrelated DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION rft.unrelated_noop()',
              fk.uk_delete_trigger)
FROM sql_saga.foreign_keys AS fk WHERE fk.foreign_key_name = 'activity_legal_unit_id_fkey' \gexec
\o
SELECT 'same-named unrelated constraint, direct DELETE of a segment | '
    || rft.outcome($$DELETE FROM rft.legal_unit WHERE id = 1 AND valid_from = '2022-01-01'$$);
SELECT 'same-named unrelated constraint, DML seen before temporal_merge | ' || (rft.target_dml_count() > 0);
\o /dev/null
SELECT setval('rft.target_dml_seen', 1, false);
\o
SELECT 'same-named unrelated constraint, temporal_merge | '
    || rft.error_of($$CALL sql_saga.temporal_merge('rft.legal_unit', 'pg_temp.merge_source', ARRAY['id'],
                 mode => 'MERGE_ENTITY_REPLACE', delete_mode => 'DELETE_MISSING_TIMELINE')$$);
SELECT 'same-named unrelated constraint, DML rows seen in the refused temporal_merge | ' || rft.target_dml_count();
ROLLBACK TO SAVEPOINT deferral_collision;

-- The same with names that need quoting: the refusal quotes them as identifiers.
SAVEPOINT deferral_collision_quoted;
\o /dev/null
CREATE SCHEMA "q ""s'ch";
CREATE FUNCTION "q ""s'ch".unrelated_noop() RETURNS trigger LANGUAGE plpgsql AS $unrelated_noop$
BEGIN
    RETURN NULL;
END;
$unrelated_noop$;
CREATE TABLE "q ""s'ch"."Par'ent ""T""" (id int NOT NULL, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('"q ""s''ch"."Par''ent ""T"""', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('"q ""s''ch"."Par''ent ""T"""', ARRAY['id'], key_type => 'primary', unique_key_name => 'q par "k"');
CREATE TABLE "q ""s'ch"."Chi'ld" (id int PRIMARY KEY, par_id int);
SELECT sql_saga.add_foreign_key('"q ""s''ch"."Chi''ld"', ARRAY['par_id'], '"q ""s''ch"."Par''ent ""T"""', ARRAY['id'],
    foreign_key_name => 'q fk "r"');
INSERT INTO "q ""s'ch"."Par'ent ""T""" VALUES (1, '[2020-01-01,2022-01-01)'), (1, '[2022-01-01,infinity)');
INSERT INTO "q ""s'ch"."Chi'ld" VALUES (1, 1);
CREATE TABLE "q ""s'ch"."Un'related ""U""" (id int);
SELECT format('CREATE CONSTRAINT TRIGGER %I AFTER INSERT ON "q ""s''ch"."Un''related ""U""" DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION "q ""s''ch".unrelated_noop()',
              fk.uk_delete_trigger)
FROM sql_saga.foreign_keys AS fk WHERE fk.foreign_key_name = 'q fk "r"' \gexec
CREATE TEMP TABLE quoted_source (row_id int, id int, valid_range daterange);
INSERT INTO pg_temp.quoted_source VALUES (1, 1, '[2020-01-01,infinity)');
\o
SELECT 'quoted names, temporal_merge | '
    || rft.error_of($$CALL sql_saga.temporal_merge('"q ""s''ch"."Par''ent ""T"""', 'pg_temp.quoted_source', ARRAY['id'],
                 mode => 'MERGE_ENTITY_REPLACE', delete_mode => 'DELETE_MISSING_TIMELINE')$$);
ROLLBACK TO SAVEPOINT deferral_collision_quoted;

SELECT 'legal_unit unchanged after the rolled back merges | '
    || string_agg(format('%s %s [%s,%s)', lu.id, lu.name, lu.valid_from, lu.valid_until), ', ' ORDER BY lu.id, lu.valid_from)
FROM rft.legal_unit AS lu;

\echo '--- part 2: the child side is a catalogued constraint trigger'
SELECT rft.child_side('activity_legal_unit_id_fkey');
SELECT rft.child_side('contact_ident_type_ident_fkey');
SELECT 'CHECK constraints calling a function on activity, contact | ' || rft.helper_checks('rft.activity') || ', ' || rft.helper_checks('rft.contact');
-- SET CONSTRAINTS accepts the catalogued name: deferred, a child row may precede its parent row.
SAVEPOINT set_constraints;
SELECT 'SET CONSTRAINTS <fk_check_constraint> DEFERRED, child before parent | ' || rft.outcome(format(
    $$SET CONSTRAINTS %I DEFERRED; INSERT INTO rft.activity VALUES (10, 5, NULL);
      INSERT INTO rft.legal_unit (id, ident_type, ident, valid_from, valid_until) VALUES (5, 'tax', '500', '2020-01-01', 'infinity')$$,
    (SELECT fk.fk_check_constraint FROM sql_saga.foreign_keys AS fk WHERE fk.foreign_key_name = 'activity_legal_unit_id_fkey')));
ROLLBACK TO SAVEPOINT set_constraints;

\echo '--- part 2: deferred child checks validate surviving row versions only'
-- Like a native deferred FK, an obsolete INSERT/UPDATE event must not reject a row that was deleted
-- or corrected before the constraint is checked. Each outcome rolls its entire probe back.
SELECT 'deferred child: invalid insert then delete | ' || rft.outcome(
    $$SET CONSTRAINTS ALL DEFERRED; INSERT INTO rft.activity VALUES (90, 999, NULL); DELETE FROM rft.activity WHERE id = 90$$);
SELECT 'deferred child: invalid insert then correct | ' || rft.outcome(
    $$SET CONSTRAINTS ALL DEFERRED; INSERT INTO rft.activity VALUES (90, 999, NULL); UPDATE rft.activity SET legal_unit_id = 1 WHERE id = 90$$);
SELECT 'deferred child: invalid update then correct | ' || rft.outcome(
    $$SET CONSTRAINTS ALL DEFERRED; UPDATE rft.activity SET legal_unit_id = 999 WHERE id = 1; UPDATE rft.activity SET legal_unit_id = 1 WHERE id = 1$$);
SELECT 'deferred child: invalid insert survives | ' || rft.outcome(
    $$SET CONSTRAINTS ALL DEFERRED; INSERT INTO rft.activity VALUES (90, 999, NULL)$$);
SELECT 'deferred child: valid insert then invalid update survives | ' || rft.outcome(
    $$SET CONSTRAINTS ALL DEFERRED; INSERT INTO rft.activity VALUES (90, 1, NULL); UPDATE rft.activity SET legal_unit_id = 999 WHERE id = 90$$);
SELECT 'deferred child: invalid insert then SIMPLE null | ' || rft.outcome(
    $$SET CONSTRAINTS ALL DEFERRED; INSERT INTO rft.activity VALUES (90, 999, NULL); UPDATE rft.activity SET legal_unit_id = NULL WHERE id = 90$$);
SELECT 'deferred child: FULL partial null then correct | ' || rft.outcome(
    $$SET CONSTRAINTS ALL DEFERRED; INSERT INTO rft.contact VALUES (90, 'tax', NULL); UPDATE rft.contact SET ident = '300' WHERE id = 90$$);
-- An UPDATE that does not touch the key columns makes the orphaning version obsolete without firing a new check
-- (the trigger is UPDATE OF the key columns): the surviving row still references a missing key and must be refused.
SELECT 'deferred child: invalid insert then payload-only update survives | ' || rft.outcome(
    $$SET CONSTRAINTS ALL DEFERRED; INSERT INTO rft.activity VALUES (90, 999, NULL); UPDATE rft.activity SET note = 'second' WHERE id = 90$$);
SELECT 'deferred child: invalid key update then payload-only update survives | ' || rft.outcome(
    $$SET CONSTRAINTS ALL DEFERRED; UPDATE rft.activity SET legal_unit_id = 999 WHERE id = 1; UPDATE rft.activity SET note = 'second' WHERE id = 1$$);
SELECT 'deferred child: FULL partial null then payload-only update survives | ' || rft.outcome(
    $$SET CONSTRAINTS ALL DEFERRED; INSERT INTO rft.contact VALUES (90, 'tax', NULL); UPDATE rft.contact SET id = 91 WHERE id = 90$$);

\echo '--- part 2: drop protection'
SELECT 'DROP TRIGGER on the child-side trigger | ' || rft.error_of('DROP TRIGGER activity_legal_unit_id_check ON rft.activity');
SELECT 'ALTER TABLE DROP CONSTRAINT on the child-side trigger | '
    || split_part(rft.outcome('ALTER TABLE rft.activity DROP CONSTRAINT activity_legal_unit_id_check'), ' ', 1);
SELECT rft.child_side('activity_legal_unit_id_fkey');

\echo '--- part 2: a direct ALTER TRIGGER ... RENAME of the child-side trigger is refused'
SELECT 'ALTER TRIGGER RENAME on the child-side trigger | '
    || rft.error_of('ALTER TRIGGER activity_legal_unit_id_check ON rft.activity RENAME TO activity_renamed');
SELECT rft.child_side('activity_legal_unit_id_fkey');
SELECT 'after the refused rename: catalog name | ' || fk.fk_check_constraint
FROM sql_saga.foreign_keys AS fk WHERE fk.foreign_key_name = 'activity_legal_unit_id_fkey';
SELECT 'after the refused rename: missing parent | ' || rft.probe('INSERT INTO rft.activity VALUES (3, 99, NULL)');
SELECT 'after the refused rename: check_integrity | ' || rft.integrity();
SAVEPOINT after_rename;
SELECT 'after the refused rename: disable_temporal_triggers | ' || rft.run($$CALL sql_saga.disable_temporal_triggers('rft.activity')$$);
SELECT rft.child_side('activity_legal_unit_id_fkey');
SELECT 'after the refused rename: enable_temporal_triggers | ' || rft.run($$CALL sql_saga.enable_temporal_triggers('rft.activity')$$);
SELECT rft.child_side('activity_legal_unit_id_fkey');
SELECT 'after the refused rename: drop_foreign_key | ' || rft.run($$SELECT sql_saga.drop_foreign_key('rft.activity', ARRAY['legal_unit_id'])$$);
SELECT 'after the refused rename and drop_foreign_key: triggers and constraints on activity | '
    || (SELECT count(*) FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = 'rft.activity'::regclass AND NOT t.tgisinternal) || ', '
    || (SELECT count(*) FROM pg_catalog.pg_constraint AS c WHERE c.conrelid = 'rft.activity'::regclass AND c.contype IN ('c', 't'));
ROLLBACK TO SAVEPOINT after_rename;

\echo '--- part 2: drop_foreign_key removes the child-side trigger'
SAVEPOINT drop_fk;
SELECT 'drop_foreign_key | ' || rft.run($$SELECT sql_saga.drop_foreign_key('rft.activity', ARRAY['legal_unit_id'])$$);
SELECT 'after drop_foreign_key: catalog rows, triggers, CHECKs on activity | '
    || (SELECT count(*) FROM sql_saga.foreign_keys AS fk WHERE fk.table_name = 'activity') || ', '
    || (SELECT count(*) FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = 'rft.activity'::regclass AND NOT t.tgisinternal) || ', '
    || (SELECT count(*) FROM pg_catalog.pg_constraint AS c WHERE c.conrelid = 'rft.activity'::regclass AND c.contype IN ('c', 't'));
SELECT 'after drop_foreign_key: a missing parent is accepted | ' || rft.probe('INSERT INTO rft.activity VALUES (3, 99, NULL)');
SELECT 'after drop_foreign_key: check_integrity | ' || rft.integrity();
ROLLBACK TO SAVEPOINT drop_fk;

\echo '--- part 2: RENAME COLUMN of a child key column'
SAVEPOINT rename_child;
SELECT 'ALTER TABLE rft.activity RENAME COLUMN legal_unit_id TO lu_id | ' || rft.run('ALTER TABLE rft.activity RENAME COLUMN legal_unit_id TO lu_id');
SELECT 'ALTER TABLE rft.contact RENAME COLUMN ident TO ident_code | ' || rft.run('ALTER TABLE rft.contact RENAME COLUMN ident TO ident_code');
SELECT 'catalog column_names | ' || string_agg(format('%s %s', fk.foreign_key_name, fk.column_names), ', ' ORDER BY fk.foreign_key_name)
FROM sql_saga.foreign_keys AS fk WHERE fk.table_name IN ('activity', 'contact');
SELECT rft.child_side('activity_legal_unit_id_fkey');
SELECT rft.child_side('contact_ident_type_ident_fkey');
SELECT 'renamed child: missing parent | ' || rft.probe('INSERT INTO rft.activity VALUES (3, 99, NULL)');
SELECT 'renamed child: existing parent | ' || rft.probe('INSERT INTO rft.activity VALUES (3, 4, NULL)');
SELECT 'renamed child: update to missing | ' || rft.probe('UPDATE rft.activity SET lu_id = 99 WHERE id = 1');
SELECT 'renamed composite child: missing parent | ' || rft.probe($$INSERT INTO rft.contact VALUES (3, 'tax', '999')$$);
SELECT 'renamed composite child: existing parent | ' || rft.probe($$INSERT INTO rft.contact VALUES (3, 'tax', '400')$$);
SELECT 'renamed child: deleting the referenced parent | ' || rft.probe('DELETE FROM rft.legal_unit WHERE id = 1');
SELECT 'renamed child: check_integrity | ' || rft.integrity();
ROLLBACK TO SAVEPOINT rename_child;

\echo '--- part 2: RENAME COLUMN of a referenced column'
SAVEPOINT rename_parent;
SELECT 'ALTER TABLE rft.legal_unit RENAME COLUMN id TO unit_id | ' || rft.run('ALTER TABLE rft.legal_unit RENAME COLUMN id TO unit_id');
SELECT 'ALTER TABLE rft.legal_unit RENAME COLUMN ident TO ident_code | ' || rft.run('ALTER TABLE rft.legal_unit RENAME COLUMN ident TO ident_code');
SELECT 'renamed parent: missing parent | ' || rft.probe('INSERT INTO rft.activity VALUES (3, 99, NULL)');
SELECT 'renamed parent: existing parent | ' || rft.probe('INSERT INTO rft.activity VALUES (3, 4, NULL)');
SELECT 'renamed parent: composite missing parent | ' || rft.probe($$INSERT INTO rft.contact VALUES (3, 'tax', '999')$$);
SELECT 'renamed parent: composite existing parent | ' || rft.probe($$INSERT INTO rft.contact VALUES (3, 'tax', '400')$$);
SELECT 'renamed parent: deleting the referenced parent | ' || rft.probe('DELETE FROM rft.legal_unit WHERE unit_id = 1');
SELECT 'renamed parent: deleting an unreferenced parent | ' || rft.probe('DELETE FROM rft.legal_unit WHERE unit_id = 4');
SELECT 'renamed parent: check_integrity | ' || rft.integrity();
ROLLBACK TO SAVEPOINT rename_parent;

\echo '--- part 2: check_integrity reports a disabled child-side trigger, and its repair works'
SAVEPOINT disabled_trigger;
SELECT 'disable the child-side trigger | ' || rft.run('ALTER TABLE rft.activity DISABLE TRIGGER activity_legal_unit_id_check');
SELECT 'disabled | ' || rft.integrity();
SELECT 'disabled, run the advertised repair | ' || rft.run(rft.advertised_repair());
SELECT 'disabled, after the advertised repair | ' || rft.integrity();
SELECT rft.child_side('activity_legal_unit_id_fkey');
SELECT 'disabled, after the advertised repair: missing parent | ' || rft.probe('INSERT INTO rft.activity VALUES (3, 99, NULL)');
ROLLBACK TO SAVEPOINT disabled_trigger;

\echo '--- part 2: check_integrity reports a missing child-side trigger, and its repair works'
SAVEPOINT missing_trigger;
-- Event triggers (drop protection) do not fire for a replica session: the narrowest way to lose the trigger.
SET LOCAL session_replication_role = replica;
SELECT 'drop the child-side trigger as replica | ' || rft.run('DROP TRIGGER activity_legal_unit_id_check ON rft.activity');
RESET session_replication_role;
SELECT 'missing | ' || rft.integrity();
SELECT 'missing, run the advertised repair | ' || rft.run(rft.advertised_repair());
SELECT 'missing, after the advertised repair | ' || rft.integrity();
SELECT rft.child_side('activity_legal_unit_id_fkey');
SELECT 'missing, after the advertised repair: missing parent | ' || rft.probe('INSERT INTO rft.activity VALUES (3, 99, NULL)');
ROLLBACK TO SAVEPOINT missing_trigger;

\echo '--- part 2: names that need quoting (double quote, apostrophe, space, upper case)'
SAVEPOINT quoted_names;
SELECT 'create schema and tables | ' || rft.run($$
    CREATE SCHEMA "odd ""s'ch";
    CREATE TABLE "odd ""s'ch"."Par'ent ""T""" (id int NOT NULL, valid_range daterange NOT NULL);
    CREATE TABLE "odd ""s'ch"."Chi'ld ""R""" (id int PRIMARY KEY, "Par ""id'" int)$$);
SELECT 'add era, key and foreign key | ' || rft.run($$
    SELECT sql_saga.add_era('"odd ""s''ch"."Par''ent ""T"""', 'valid_range', 'valid');
    SELECT sql_saga.add_unique_key('"odd ""s''ch"."Par''ent ""T"""', ARRAY['id'], key_type => 'primary', unique_key_name => 'par "odd" ''k''');
    SELECT sql_saga.add_foreign_key('"odd ""s''ch"."Chi''ld ""R"""', ARRAY['Par "id'''], '"odd ""s''ch"."Par''ent ""T"""', ARRAY['id'],
        foreign_key_name => 'fk "odd" ''r''');
    INSERT INTO "odd ""s'ch"."Par'ent ""T""" VALUES (1, '[2020-01-01,infinity)');
    INSERT INTO "odd ""s'ch"."Chi'ld ""R""" VALUES (1, 1)$$);
SELECT rft.child_side('fk "odd" ''r''');
SELECT 'quoted: missing parent | ' || rft.probe($$INSERT INTO "odd ""s'ch"."Chi'ld ""R""" VALUES (2, 99)$$);
SELECT 'quoted: existing parent | ' || rft.probe($$INSERT INTO "odd ""s'ch"."Chi'ld ""R""" VALUES (2, 1)$$);
SELECT 'quoted: check_integrity | ' || rft.integrity();
SELECT 'quoted: ALTER TRIGGER RENAME on the child-side trigger | ' || rft.error_of(format('ALTER TRIGGER %I ON %s RENAME TO renamed',
    (SELECT fk.fk_check_constraint FROM sql_saga.foreign_keys AS fk WHERE fk.foreign_key_name = 'fk "odd" ''r'''),
    '"odd ""s''ch"."Chi''ld ""R"""'));
SELECT rft.child_side('fk "odd" ''r''');
SAVEPOINT quoted_disabled;
SELECT 'quoted: disable the child-side trigger | ' || rft.run(format('ALTER TABLE %s DISABLE TRIGGER %I', '"odd ""s''ch"."Chi''ld ""R"""',
    (SELECT fk.fk_check_constraint FROM sql_saga.foreign_keys AS fk WHERE fk.foreign_key_name = 'fk "odd" ''r''')));
SELECT 'quoted: disabled | ' || rft.integrity();
SELECT 'quoted: disabled, run the advertised repair | ' || rft.run(rft.advertised_repair());
SELECT 'quoted: disabled, after the advertised repair | ' || rft.integrity();
SELECT rft.child_side('fk "odd" ''r''');
SELECT 'quoted: disabled, after the advertised repair: missing parent | ' || rft.probe($$INSERT INTO "odd ""s'ch"."Chi'ld ""R""" VALUES (2, 99)$$);
ROLLBACK TO SAVEPOINT quoted_disabled;
SAVEPOINT quoted_missing;
SET LOCAL session_replication_role = replica;
SELECT 'quoted: drop the child-side trigger as replica | ' || rft.run(format('DROP TRIGGER %I ON %s',
    (SELECT fk.fk_check_constraint FROM sql_saga.foreign_keys AS fk WHERE fk.foreign_key_name = 'fk "odd" ''r'''),
    '"odd ""s''ch"."Chi''ld ""R"""'));
RESET session_replication_role;
SELECT 'quoted: missing | ' || rft.integrity();
SELECT 'quoted: missing, run the advertised repair | ' || rft.run(rft.advertised_repair());
SELECT 'quoted: missing, after the advertised repair | ' || rft.integrity();
SELECT rft.child_side('fk "odd" ''r''');
SELECT 'quoted: missing, after the advertised repair: missing parent | ' || rft.probe($$INSERT INTO "odd ""s'ch"."Chi'ld ""R""" VALUES (2, 99)$$);
ROLLBACK TO SAVEPOINT quoted_missing;
ROLLBACK TO SAVEPOINT quoted_names;

\echo '--- part 3: disable_temporal_triggers / enable_temporal_triggers include the child-side trigger'
SAVEPOINT manage_triggers;
-- StatBus shape: every parent and child table in one call.
SELECT 'CALL sql_saga.disable_temporal_triggers(''rft.legal_unit'', ''rft.activity'', ''rft.contact'', ''rft.stat'') | ' || rft.run($$CALL sql_saga.disable_temporal_triggers('rft.legal_unit', 'rft.activity', 'rft.contact', 'rft.stat')$$);
SELECT rft.child_side('activity_legal_unit_id_fkey');
SELECT rft.child_side('contact_ident_type_ident_fkey');
SELECT 'disabled: activity accepts a missing parent | ' || rft.probe('INSERT INTO rft.activity VALUES (3, 99, NULL)');
SELECT 'disabled: contact accepts a missing parent | ' || rft.probe($$INSERT INTO rft.contact VALUES (3, 'tax', '999')$$);
SELECT 'disabled: deleting a referenced parent is accepted | ' || rft.probe('DELETE FROM rft.legal_unit WHERE id = 1');
SELECT 'CALL sql_saga.enable_temporal_triggers(''rft.legal_unit'', ''rft.activity'', ''rft.contact'', ''rft.stat'') | ' || rft.run($$CALL sql_saga.enable_temporal_triggers('rft.legal_unit', 'rft.activity', 'rft.contact', 'rft.stat')$$);
SELECT rft.child_side('activity_legal_unit_id_fkey');
SELECT rft.child_side('contact_ident_type_ident_fkey');
SELECT 're-enabled: activity refuses a missing parent | ' || rft.probe('INSERT INTO rft.activity VALUES (3, 99, NULL)');
SELECT 're-enabled: contact refuses a missing parent | ' || rft.probe($$INSERT INTO rft.contact VALUES (3, 'tax', '999')$$);
SELECT 're-enabled: deleting a referenced parent is refused | ' || rft.probe('DELETE FROM rft.legal_unit WHERE id = 1');
SELECT 're-enabled: check_integrity | ' || rft.integrity();
-- The child table alone.
SELECT 'CALL sql_saga.disable_temporal_triggers(''rft.activity'') | ' || rft.run($$CALL sql_saga.disable_temporal_triggers('rft.activity')$$);
SELECT rft.child_side('activity_legal_unit_id_fkey');
SELECT 'child alone disabled: activity accepts a missing parent | ' || rft.probe('INSERT INTO rft.activity VALUES (3, 99, NULL)');
SELECT 'child alone disabled: deleting a referenced parent is still refused | ' || rft.probe('DELETE FROM rft.legal_unit WHERE id = 1');
SELECT 'CALL sql_saga.enable_temporal_triggers(''rft.activity'') | ' || rft.run($$CALL sql_saga.enable_temporal_triggers('rft.activity')$$);
SELECT 'child alone re-enabled: activity refuses a missing parent | ' || rft.probe('INSERT INTO rft.activity VALUES (3, 99, NULL)');
ROLLBACK TO SAVEPOINT manage_triggers;

\echo '--- part 4: MATCH FULL (contact) and MATCH SIMPLE (activity)'
SELECT 'FULL: existing key | ' || rft.probe($$INSERT INTO rft.contact VALUES (3, 'tax', '400')$$);
SELECT 'FULL: missing key | ' || rft.probe($$INSERT INTO rft.contact VALUES (3, 'tax', '999')$$);
SELECT 'FULL: all NULL | ' || rft.probe($$INSERT INTO rft.contact VALUES (3, NULL, NULL)$$);
SELECT 'FULL: first column NULL | ' || rft.probe($$INSERT INTO rft.contact VALUES (3, NULL, '400')$$);
SELECT 'FULL: second column NULL | ' || rft.probe($$INSERT INTO rft.contact VALUES (3, 'tax', NULL)$$);
SELECT 'FULL: update to partly NULL | ' || rft.probe($$UPDATE rft.contact SET ident = NULL WHERE id = 1$$);
SELECT 'FULL: update all NULL to partly NULL | ' || rft.probe($$UPDATE rft.contact SET ident_type = 'tax' WHERE id = 2$$);
SELECT 'SIMPLE: NULL | ' || rft.probe('INSERT INTO rft.activity VALUES (3, NULL, NULL)');
SAVEPOINT match_simple_composite;
SELECT 'create contact_simple | ' || rft.run('CREATE TABLE rft.contact_simple (id int PRIMARY KEY, ident_type text, ident text)');
SELECT 'SIMPLE composite: add | ' || rft.run($$SELECT sql_saga.add_regular_foreign_key('rft.contact_simple', ARRAY['ident_type', 'ident'], 'legal_unit_ident_valid')$$);
SELECT 'SIMPLE composite: partly NULL | ' || rft.probe($$INSERT INTO rft.contact_simple VALUES (1, 'tax', NULL)$$);
SELECT 'SIMPLE composite: missing key | ' || rft.probe($$INSERT INTO rft.contact_simple VALUES (1, 'tax', '999')$$);
ROLLBACK TO SAVEPOINT match_simple_composite;

\echo '--- part 4: MATCH PARTIAL is refused at creation'
SAVEPOINT match_partial;
CREATE TABLE rft.contact_partial (id int PRIMARY KEY, ident_type text, ident text);
CREATE TABLE rft.stat_partial (id int NOT NULL, ident_type text, ident text, valid_range daterange NOT NULL);
SELECT 'era on stat_partial | ' || rft.run($$SELECT sql_saga.add_era('rft.stat_partial', 'valid_range', 'valid')$$);
SELECT 'regular PARTIAL | ' || rft.run($$SELECT sql_saga.add_regular_foreign_key('rft.contact_partial', ARRAY['ident_type', 'ident'], 'legal_unit_ident_valid', match_type => 'PARTIAL')$$);
SELECT 'temporal PARTIAL | ' || rft.run($$SELECT sql_saga.add_temporal_foreign_key('rft.stat_partial', ARRAY['ident_type', 'ident'], 'valid', 'legal_unit_ident_valid', match_type => 'PARTIAL')$$);
SELECT 'PARTIAL leaves no catalog row, trigger or constraint | '
    || (SELECT count(*) FROM sql_saga.foreign_keys AS fk WHERE fk.table_name IN ('contact_partial', 'stat_partial')) || ', '
    || (SELECT count(*) FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid IN ('rft.contact_partial'::regclass, 'rft.legal_unit'::regclass)
            AND NOT t.tgisinternal AND t.tgname LIKE '%partial%') || ', '
    || (SELECT count(*) FROM pg_catalog.pg_constraint AS c
        WHERE (c.conrelid = 'rft.contact_partial'::regclass AND c.contype IN ('c', 't'))
           OR (c.conrelid = 'rft.stat_partial'::regclass AND c.contype = 'f'));
ROLLBACK TO SAVEPOINT match_partial;

\echo '--- part 5: lifecycle attribution (an unrelated CHECK, a noncanonical child-side trigger)'
-- Own fixture: parent rft.p5 (key "B", "A"), child rft.c5 with the child columns ("Y", "X") paired by position.
SAVEPOINT part5;
CREATE TABLE rft.p5 (id int NOT NULL, "A" int NOT NULL, "B" int NOT NULL, valid_range daterange NOT NULL);
SELECT 'part 5: era | ' || rft.run($$SELECT sql_saga.add_era('rft.p5', 'valid_range', 'valid')$$);
SELECT 'part 5: key | ' || rft.run($$SELECT sql_saga.add_unique_key('rft.p5', ARRAY['B', 'A'], key_type => 'natural', unique_key_name => 'p5_key')$$);
CREATE TABLE rft.c5 (id int PRIMARY KEY, "X" int, "Y" int);
SELECT 'part 5: foreign key | ' || rft.run($$SELECT sql_saga.add_regular_foreign_key('rft.c5', ARRAY['Y', 'X'], 'p5_key', foreign_key_name => 'c5_fkey', fk_check_constraint => 'c5_child')$$);
INSERT INTO rft.p5 VALUES (1, 10, 20, '[2020-01-01,infinity)');
INSERT INTO rft.c5 VALUES (1, 10, 20);

-- 5a: the child-side trigger is gone and a user CHECK has the same name: it is the user's, not sql_saga's.
SAVEPOINT user_check;
SET LOCAL session_replication_role = replica;
DROP TRIGGER c5_child ON rft.c5;
ALTER TABLE rft.c5 ADD CONSTRAINT c5_child CHECK ("X" >= 0);
SET LOCAL session_replication_role = origin;
SELECT 'part 5a: drop_foreign_key_by_name with an unrelated same-name CHECK | ' || rft.run($$SELECT sql_saga.drop_foreign_key_by_name('rft.c5', 'c5_fkey')$$);
SELECT 'part 5a: the unrelated CHECK is kept | ' || count(*)
FROM pg_catalog.pg_constraint WHERE conrelid = 'rft.c5'::regclass AND conname = 'c5_child' AND contype = 'c';
ROLLBACK TO SAVEPOINT user_check;

-- 5b: the same, then the parent table is dropped: the surviving-child cleanup keeps the user CHECK too.
SAVEPOINT user_check_parent_drop;
SET LOCAL session_replication_role = replica;
DROP TRIGGER c5_child ON rft.c5;
ALTER TABLE rft.c5 ADD CONSTRAINT c5_child CHECK ("X" >= 0);
SET LOCAL session_replication_role = origin;
SELECT 'part 5b: DROP TABLE of the parent with an unrelated same-name CHECK on the child | ' || rft.run('DROP TABLE rft.p5 CASCADE');
SELECT 'part 5b: the unrelated CHECK is kept | ' || count(*)
FROM pg_catalog.pg_constraint WHERE conrelid = 'rft.c5'::regclass AND conname = 'c5_child' AND contype = 'c';
ROLLBACK TO SAVEPOINT user_check_parent_drop;

-- 5c: the child-side trigger was replaced by the same definition with a WHEN (false) predicate (noncanonical).
-- A rename of a referenced key column refuses it; a rename of a child key column must refuse it too and leave
-- trigger, catalog and column unchanged, not silently replace it with the canonical trigger.
SAVEPOINT user_when;
CREATE TEMP TABLE part5_ddl AS
SELECT replace(pg_catalog.pg_get_triggerdef(t.oid), 'FOR EACH ROW EXECUTE', 'FOR EACH ROW WHEN (false) EXECUTE') AS ddl
FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = 'rft.c5'::regclass AND t.tgname = 'c5_child';
SET LOCAL session_replication_role = replica;
DROP TRIGGER c5_child ON rft.c5;
SELECT ddl FROM pg_temp.part5_ddl \gexec
SET LOCAL session_replication_role = origin;
SELECT 'part 5c: the replacement has a predicate | ' || (t.tgqual IS NOT NULL)::text
FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = 'rft.c5'::regclass AND t.tgname = 'c5_child';
SELECT 'part 5c: rename of a referenced key column | ' || rft.outcome('ALTER TABLE rft.p5 RENAME COLUMN "A" TO "A2"');
-- rft.run keeps the effect of an accepted rename, so the state lines below observe it.
SELECT 'part 5c: rename of a child key column | ' || rft.run('ALTER TABLE rft.c5 RENAME COLUMN "X" TO "X2"');
SELECT 'part 5c: predicate kept | ' || (t.tgqual IS NOT NULL)::text
FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = 'rft.c5'::regclass AND t.tgname = 'c5_child';
SELECT 'part 5c: catalog columns | ' || fk.column_names::text FROM sql_saga.foreign_keys AS fk WHERE fk.foreign_key_name = 'c5_fkey';
SELECT 'part 5c: child column "X" kept | ' || count(*)::text
FROM pg_catalog.pg_attribute WHERE attrelid = 'rft.c5'::regclass AND attname = 'X' AND NOT attisdropped;
ROLLBACK TO SAVEPOINT user_when;
ROLLBACK TO SAVEPOINT part5;

\echo '--- final'
SELECT rft.integrity();

\o /dev/null
ROLLBACK;
\i sql/include/test_teardown.sql
