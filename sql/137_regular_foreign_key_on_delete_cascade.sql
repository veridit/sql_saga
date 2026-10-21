\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/*
 * ON DELETE CASCADE for a regular (non-temporal) to temporal foreign key (StatBus request, todo.md).
 *
 * A regular child row references a key that exists in the temporal parent at some time. With
 * delete_action => 'CASCADE' the child row is deleted when the LAST parent row carrying its key is
 * deleted. Deleting one timeline segment while another segment of the same key remains keeps the
 * child: this is the same rule the NO ACTION check uses to decide whether a delete orphans a child.
 *
 * Part 1  The API: CASCADE is accepted and recorded for regular_to_temporal keys. temporal_to_temporal
 *         refuses it (PostgreSQL 18 refuses ON DELETE CASCADE on PERIOD keys too). SET NULL and SET
 *         DEFAULT, and every non-default update_action, stay refused.
 * Part 2  The cascade point: one segment deleted keeps the child, the last segment cascades, a DELETE
 *         of the whole entity cascades, other keys' children are untouched.
 * Part 3  Composite natural key with MATCH FULL: exactly the children of the vanished key go; a child
 *         with a NULL key (not a reference) is never deleted.
 * Part 4  temporal_merge reshaping a parent's timeline (the key exists at the end) does not cascade;
 *         DELETE_MISSING_ENTITIES that removes the key does.
 * Part 5  NO ACTION, the default, still refuses deleting the last segment of a referenced key.
 *
 * Every probe runs at the end of its own savepoint and is rolled back.
 */

BEGIN;
CREATE SCHEMA rfc;
SET search_path = rfc, public;

-- Run a statement, check every deferrable constraint, report 'ok' or the SQLSTATE; effects kept on ok.
CREATE FUNCTION rfc.run(statement text) RETURNS text LANGUAGE plpgsql AS $run$
BEGIN
    BEGIN
        EXECUTE statement;
        SET CONSTRAINTS ALL IMMEDIATE;
        RETURN 'ok';
    EXCEPTION WHEN OTHERS THEN
        RETURN 'error ' || SQLSTATE;
    END;
END;
$run$;

-- An error whose message is the subject: SQLSTATE and message.
CREATE FUNCTION rfc.error_of(statement text) RETURNS text LANGUAGE plpgsql AS $error_of$
BEGIN
    BEGIN
        EXECUTE statement;
        RETURN 'accepted';
    EXCEPTION WHEN OTHERS THEN
        RETURN SQLSTATE || ': ' || SQLERRM;
    END;
END;
$error_of$;

CREATE FUNCTION rfc.integrity() RETURNS text LANGUAGE plpgsql AS $integrity$
BEGIN
    BEGIN
        PERFORM sql_saga.check_integrity();
        RETURN 'ok';
    EXCEPTION WHEN OTHERS THEN
        RETURN SQLSTATE || ': ' || SQLERRM;
    END;
END;
$integrity$;

/*
 * The parent: legal units 1 and 2 have two timeline segments, 3 and 4 one.
 * Primary key (id), natural key (ident_type, ident).
 */
CREATE TABLE rfc.legal_unit (id int NOT NULL, ident_type text NOT NULL, ident text NOT NULL, name text,
                             valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('rfc.legal_unit', 'valid_range', 'valid',
    valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('rfc.legal_unit', ARRAY['id'], key_type => 'primary', unique_key_name => 'legal_unit_id_valid');
SELECT sql_saga.add_unique_key('rfc.legal_unit', ARRAY['ident_type', 'ident'], key_type => 'natural',
    unique_key_name => 'legal_unit_ident_valid');
INSERT INTO rfc.legal_unit (id, ident_type, ident, name, valid_from, valid_until) VALUES
    (1, 'tax', '100', 'a', '2020-01-01', '2022-01-01'),
    (1, 'tax', '100', 'b', '2022-01-01', 'infinity'),
    (2, 'tax', '200', 'a', '2020-01-01', '2022-01-01'),
    (2, 'tax', '200', 'b', '2022-01-01', 'infinity'),
    (3, 'tax', '300', 'a', '2020-01-01', 'infinity'),
    (4, 'tax', '400', 'a', '2020-01-01', 'infinity');

-- Addendum children (StatBus's unit_notes / tag_for_unit / external_ident shape).
CREATE TABLE rfc.note (id int PRIMARY KEY, legal_unit_id int, body text);         -- CASCADE by id
CREATE TABLE rfc.tag (id int PRIMARY KEY, legal_unit_id int);                      -- NO ACTION by id
CREATE TABLE rfc.ext_ident (id int PRIMARY KEY, ident_type text, ident text);      -- CASCADE by natural key, MATCH FULL
CREATE TABLE rfc.probe_fk (id int PRIMARY KEY, legal_unit_id int);                 -- API probes

CREATE TABLE rfc.stat (id int NOT NULL, legal_unit_id int, valid_range daterange NOT NULL,
                       valid_from date, valid_until date);
SELECT sql_saga.add_era('rfc.stat', 'valid_range', 'valid',
    valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');

CREATE TEMP TABLE merge_source (row_id int, id int, ident_type text, ident text, name text, valid_from date, valid_until date);

-- legal_unit's ids, and a child table's ids, as one line.
CREATE FUNCTION rfc.ids(rel regclass) RETURNS text LANGUAGE plpgsql AS $ids$
DECLARE
    result text;
BEGIN
    EXECUTE format('SELECT coalesce(string_agg(id::text, '','' ORDER BY id), ''none'') FROM %s', rel) INTO result;
    RETURN result;
END;
$ids$;

-- A merge into legal_unit from the given source rows; returns 'ok' or the SQLSTATE.
CREATE FUNCTION rfc.merge(mode sql_saga.temporal_merge_mode, delete_mode sql_saga.temporal_merge_delete_mode, source_rows text)
RETURNS text LANGUAGE plpgsql AS $merge$
BEGIN
    TRUNCATE pg_temp.merge_source;
    EXECUTE 'INSERT INTO pg_temp.merge_source VALUES ' || source_rows;
    RETURN rfc.run(format($$CALL sql_saga.temporal_merge('rfc.legal_unit', 'pg_temp.merge_source', ARRAY['id'],
        mode => %L, delete_mode => %L)$$, mode, delete_mode));
END;
$merge$;

\o
\echo '--- part 1: the API'
SELECT 'regular_to_temporal CASCADE is accepted | '
    || rfc.run($$SELECT sql_saga.add_foreign_key('rfc.note', ARRAY['legal_unit_id'], 'rfc.legal_unit', ARRAY['id'],
                                                 delete_action => 'CASCADE', foreign_key_name => 'note_legal_unit_id_fkey')$$);
SELECT 'catalog records it | ' || fk.foreign_key_name || ' ' || fk.type || ' delete ' || fk.delete_action || ' update ' || fk.update_action
FROM sql_saga.foreign_keys AS fk WHERE fk.foreign_key_name = 'note_legal_unit_id_fkey';
SELECT 'add_regular_foreign_key CASCADE, MATCH FULL, natural key | '
    || rfc.run($$SELECT sql_saga.add_regular_foreign_key('rfc.ext_ident', ARRAY['ident_type', 'ident'], 'legal_unit_ident_valid',
                                                         match_type => 'FULL', delete_action => 'CASCADE',
                                                         foreign_key_name => 'ext_ident_fkey')$$);
SELECT 'NO ACTION default | '
    || rfc.run($$SELECT sql_saga.add_foreign_key('rfc.tag', ARRAY['legal_unit_id'], 'rfc.legal_unit', ARRAY['id'],
                                                 foreign_key_name => 'tag_legal_unit_id_fkey')$$);
SELECT 'regular_to_temporal SET NULL | '
    || rfc.error_of($$SELECT sql_saga.add_foreign_key('rfc.probe_fk', ARRAY['legal_unit_id'], 'rfc.legal_unit', ARRAY['id'],
                                                      delete_action => 'SET NULL')$$);
SELECT 'regular_to_temporal SET DEFAULT | '
    || rfc.error_of($$SELECT sql_saga.add_foreign_key('rfc.probe_fk', ARRAY['legal_unit_id'], 'rfc.legal_unit', ARRAY['id'],
                                                      delete_action => 'SET DEFAULT')$$);
SELECT 'regular_to_temporal update_action CASCADE | '
    || rfc.error_of($$SELECT sql_saga.add_foreign_key('rfc.probe_fk', ARRAY['legal_unit_id'], 'rfc.legal_unit', ARRAY['id'],
                                                      update_action => 'CASCADE')$$);
SELECT 'temporal_to_temporal CASCADE | '
    || rfc.error_of($$SELECT sql_saga.add_foreign_key('rfc.stat', ARRAY['legal_unit_id'], 'rfc.legal_unit', ARRAY['id'],
                                                      delete_action => 'CASCADE')$$);
SELECT 'no foreign key was left behind by the refusals | ' || count(*)::text
FROM sql_saga.foreign_keys AS fk WHERE (fk.table_schema, fk.table_name) IN (('rfc', 'probe_fk'), ('rfc', 'stat'));
SELECT 'check_integrity | ' || rfc.integrity();

INSERT INTO rfc.note VALUES (1, 1, 'on 1'), (2, 1, 'also on 1'), (3, 2, 'on 2'), (4, 3, 'on 3'), (5, NULL, 'no unit');
INSERT INTO rfc.tag VALUES (1, 4);
INSERT INTO rfc.ext_ident VALUES (1, 'tax', '300'), (2, 'tax', '400'), (3, NULL, NULL), (4, 'tax', '100');

\echo '--- part 2: the cascade point'
SAVEPOINT p;
SELECT 'delete one segment of 1 | ' || rfc.run($$DELETE FROM rfc.legal_unit WHERE id = 1 AND valid_from = '2020-01-01'$$);
SELECT 'notes after deleting one segment of 1 | ' || rfc.ids('rfc.note');
SELECT 'delete the last segment of 1 | ' || rfc.run($$DELETE FROM rfc.legal_unit WHERE id = 1$$);
SELECT 'notes after deleting the last segment of 1 | ' || rfc.ids('rfc.note');
SELECT 'ext_ident (by natural key) after deleting 1 | ' || rfc.ids('rfc.ext_ident');
SELECT 'check_integrity | ' || rfc.integrity();
ROLLBACK TO SAVEPOINT p;
SELECT 'delete the whole entity 2 in one statement | ' || rfc.run($$DELETE FROM rfc.legal_unit WHERE id = 2$$);
SELECT 'notes after deleting 2 | ' || rfc.ids('rfc.note');
ROLLBACK TO SAVEPOINT p;
SELECT 'delete 1 and 2 in one statement | ' || rfc.run($$DELETE FROM rfc.legal_unit WHERE id IN (1, 2)$$);
SELECT 'notes after deleting 1 and 2 | ' || rfc.ids('rfc.note');
ROLLBACK TO SAVEPOINT p;

\echo '--- part 3: composite natural key, MATCH FULL'
SELECT 'delete legal unit 3 (tax 300; note 4 cascades, ext_ident 1 cascades) | ' || rfc.run($$DELETE FROM rfc.legal_unit WHERE id = 3$$);
SELECT 'notes | ' || rfc.ids('rfc.note');
SELECT 'ext_ident (NULL key 3 kept) | ' || rfc.ids('rfc.ext_ident');
ROLLBACK TO SAVEPOINT p;

\echo '--- part 4: temporal_merge'
SELECT 'REPLACE + DELETE_MISSING_TIMELINE coalesces 1 | '
    || rfc.merge('MERGE_ENTITY_REPLACE', 'DELETE_MISSING_TIMELINE', $$(1, 1, 'tax', '100', 'c', '2020-01-01', 'infinity')$$);
SELECT 'notes after coalescing 1 | ' || rfc.ids('rfc.note');
ROLLBACK TO SAVEPOINT p;
SELECT 'REPLACE + DELETE_MISSING_TIMELINE shrinks 1 | '
    || rfc.merge('MERGE_ENTITY_REPLACE', 'DELETE_MISSING_TIMELINE', $$(1, 1, 'tax', '100', 'c', '2023-01-01', 'infinity')$$);
SELECT 'notes after shrinking 1 | ' || rfc.ids('rfc.note');
ROLLBACK TO SAVEPOINT p;
SELECT 'UPSERT splits 1 | '
    || rfc.merge('MERGE_ENTITY_UPSERT', 'NONE', $$(1, 1, 'tax', '100', 'm', '2021-01-01', '2021-06-01')$$);
SELECT 'notes after splitting 1 | ' || rfc.ids('rfc.note');
ROLLBACK TO SAVEPOINT p;
SELECT 'REPLACE + DELETE_MISSING_ENTITIES without 1 (cascades 1''s notes) | '
    || rfc.merge('MERGE_ENTITY_REPLACE', 'DELETE_MISSING_ENTITIES',
                 $$(1, 2, 'tax', '200', 'a', '2020-01-01', 'infinity'), (2, 3, 'tax', '300', 'a', '2020-01-01', 'infinity'), (3, 4, 'tax', '400', 'a', '2020-01-01', 'infinity')$$);
SELECT 'legal units | ' || rfc.ids('rfc.legal_unit');
SELECT 'notes | ' || rfc.ids('rfc.note');
ROLLBACK TO SAVEPOINT p;

\echo '--- part 5: NO ACTION still refuses'
SELECT 'delete legal unit 4 (tag 1, NO ACTION) | ' || rfc.run($$DELETE FROM rfc.legal_unit WHERE id = 4$$);
SELECT 'tags | ' || rfc.ids('rfc.tag');
SELECT 'legal units | ' || rfc.ids('rfc.legal_unit');
ROLLBACK TO SAVEPOINT p;

\echo '--- part 6: interactions'
-- disable_temporal_triggers (StatBus's bulk-load path) and the replica role skip the cascade, as they skip the check.
CALL sql_saga.disable_temporal_triggers('rfc.legal_unit', 'rfc.note');
SELECT 'disable_temporal_triggers, delete 2 | ' || rfc.run($$DELETE FROM rfc.legal_unit WHERE id = 2$$);
SELECT 'notes (note 3 not cascaded) | ' || rfc.ids('rfc.note');
ROLLBACK TO SAVEPOINT p;
SET LOCAL session_replication_role = replica;
SELECT 'session_replication_role replica, delete 2 | ' || rfc.run($$DELETE FROM rfc.legal_unit WHERE id = 2$$);
RESET session_replication_role;
SELECT 'notes (note 3 not cascaded) | ' || rfc.ids('rfc.note');
ROLLBACK TO SAVEPOINT p;
-- Rename following keeps the action and the cascade.
ALTER TABLE rfc.note RENAME COLUMN legal_unit_id TO unit_id;
SELECT 'child column renamed, catalog | ' || fk.column_names::text || ' delete ' || fk.delete_action
FROM sql_saga.foreign_keys AS fk WHERE fk.foreign_key_name = 'note_legal_unit_id_fkey';
SELECT 'child column renamed, delete 2 | ' || rfc.run($$DELETE FROM rfc.legal_unit WHERE id = 2$$);
SELECT 'notes | ' || rfc.ids('rfc.note');
ROLLBACK TO SAVEPOINT p;
ALTER TABLE rfc.legal_unit RENAME COLUMN id TO unit_id;
SELECT 'parent key column renamed, delete 2 | ' || rfc.run($$DELETE FROM rfc.legal_unit WHERE unit_id = 2$$);
SELECT 'notes | ' || rfc.ids('rfc.note');
ROLLBACK TO SAVEPOINT p;
-- drop_foreign_key removes the cascade with the rest of the foreign key.
SELECT 'drop_foreign_key | ' || rfc.run($$SELECT sql_saga.drop_foreign_key('rfc.note', ARRAY['legal_unit_id'])$$);
SELECT 'after drop_foreign_key, delete 2 | ' || rfc.run($$DELETE FROM rfc.legal_unit WHERE id = 2$$);
SELECT 'notes (nothing cascaded) | ' || rfc.ids('rfc.note');
ROLLBACK TO SAVEPOINT p;
-- The cascade runs as the extension owner: the parent's writer needs no rights on the child, as with native CASCADE.
CREATE ROLE rfc_parent_writer;
GRANT USAGE ON SCHEMA rfc TO rfc_parent_writer;
GRANT SELECT, DELETE ON rfc.legal_unit TO rfc_parent_writer;
SET LOCAL ROLE rfc_parent_writer;
SELECT 'writer without rights on note, delete 2 | ' || rfc.run($$DELETE FROM rfc.legal_unit WHERE id = 2$$);
RESET ROLE;
SELECT 'notes | ' || rfc.ids('rfc.note');
ROLLBACK TO SAVEPOINT p;
-- The child's own triggers fire for cascaded rows, and a native cascade from the child continues the chain.
CREATE TABLE rfc.note_part (id int PRIMARY KEY, note_id int REFERENCES rfc.note ON DELETE CASCADE);
CREATE TABLE rfc.note_audit (note_id int);
CREATE FUNCTION rfc.note_audit_delete() RETURNS trigger LANGUAGE plpgsql AS $note_audit_delete$
BEGIN
    INSERT INTO rfc.note_audit VALUES (OLD.id);
    RETURN OLD;
END;
$note_audit_delete$;
CREATE TRIGGER note_audit AFTER DELETE ON rfc.note FOR EACH ROW EXECUTE FUNCTION rfc.note_audit_delete();
INSERT INTO rfc.note_part VALUES (30, 3), (40, 4);
SELECT 'chain, delete 2 | ' || rfc.run($$DELETE FROM rfc.legal_unit WHERE id = 2$$);
SELECT 'note_parts (30 cascaded natively from note 3) | ' || rfc.ids('rfc.note_part');
SELECT 'note audit (child trigger fired) | ' || coalesce(string_agg(na.note_id::text, ','), 'none') FROM rfc.note_audit AS na;
ROLLBACK TO SAVEPOINT p;

\echo '--- part 7: StatBus shape, two nullable references with one set (MATCH SIMPLE), and deferred timing'
-- unit_notes references either a legal unit or an establishment: two CASCADE foreign keys, one per column.
CREATE TABLE rfc.establishment (id int NOT NULL, valid_range daterange NOT NULL, valid_from date, valid_until date);
\o /dev/null
SELECT sql_saga.add_era('rfc.establishment', 'valid_range', 'valid',
    valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('rfc.establishment', ARRAY['id'], key_type => 'primary', unique_key_name => 'establishment_id_valid');
\o
INSERT INTO rfc.establishment (id, valid_from, valid_until) VALUES (1, '2020-01-01', 'infinity'), (2, '2020-01-01', 'infinity');
CREATE TABLE rfc.unit_notes (id int PRIMARY KEY, legal_unit_id int, establishment_id int,
                             CHECK (num_nonnulls(legal_unit_id, establishment_id) = 1));
SELECT 'unit_notes -> legal_unit CASCADE | '
    || rfc.run($$SELECT sql_saga.add_foreign_key('rfc.unit_notes', ARRAY['legal_unit_id'], 'rfc.legal_unit', ARRAY['id'], delete_action => 'CASCADE')$$);
SELECT 'unit_notes -> establishment CASCADE | '
    || rfc.run($$SELECT sql_saga.add_foreign_key('rfc.unit_notes', ARRAY['establishment_id'], 'rfc.establishment', ARRAY['id'], delete_action => 'CASCADE')$$);
-- Legal unit 2 and establishment 2 have the same id; only the matching column's rows may go.
INSERT INTO rfc.unit_notes VALUES (1, 2, NULL), (2, NULL, 2), (3, 3, NULL), (4, NULL, 1);
SAVEPOINT q;
SELECT 'delete legal unit 2 | ' || rfc.run($$DELETE FROM rfc.legal_unit WHERE id = 2$$);
SELECT 'unit_notes (only legal unit 2''s note 1 gone, establishment 2''s note 2 kept) | ' || rfc.ids('rfc.unit_notes');
ROLLBACK TO SAVEPOINT q;
SELECT 'delete establishment 2 | ' || rfc.run($$DELETE FROM rfc.establishment WHERE id = 2$$);
SELECT 'unit_notes (only establishment 2''s note 2 gone) | ' || rfc.ids('rfc.unit_notes');
ROLLBACK TO SAVEPOINT q;
-- The foreign key's triggers are DEFERRABLE INITIALLY IMMEDIATE: by default the cascade runs at the end of the
-- DELETE statement. When the caller defers constraints, it runs at the check point, and a key re-inserted before
-- then keeps its children (the same rule as a temporal_merge, which defers these triggers itself).
SET CONSTRAINTS ALL DEFERRED;
DELETE FROM rfc.legal_unit WHERE id = 3;
SELECT 'deferred, right after DELETE of 3 | ' || rfc.ids('rfc.unit_notes');
SET CONSTRAINTS ALL IMMEDIATE;
SELECT 'deferred, at the check point (note 3 gone) | ' || rfc.ids('rfc.unit_notes');
ROLLBACK TO SAVEPOINT q;
SET CONSTRAINTS ALL DEFERRED;
DELETE FROM rfc.legal_unit WHERE id = 3;
INSERT INTO rfc.legal_unit (id, ident_type, ident, name, valid_from, valid_until) VALUES (3, 'tax', '300', 'again', '2024-01-01', 'infinity');
SET CONSTRAINTS ALL IMMEDIATE;
SELECT 'deferred, key 3 re-inserted before the check point (note 3 kept) | ' || rfc.ids('rfc.unit_notes');
ROLLBACK TO SAVEPOINT q;
ROLLBACK TO SAVEPOINT p;
SELECT 'check_integrity at the end | ' || rfc.integrity();

ROLLBACK;
\i sql/include/test_teardown.sql
