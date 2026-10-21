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
SELECT 'check_integrity at the end | ' || rfc.integrity();

ROLLBACK;
\i sql/include/test_teardown.sql
