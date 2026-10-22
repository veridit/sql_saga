\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/*
 * TRUNCATE of the parent of a regular (non-temporal) to temporal foreign key (1.4).
 *
 * TRUNCATE fires no row triggers, so before 1.4 it silently left the children of a regular foreign key
 * without a parent, whatever its delete action. PostgreSQL refuses TRUNCATE of a table referenced by a
 * native foreign key, also one with ON DELETE CASCADE, unless the referencing table is truncated in the
 * same statement. sql_saga does the same for regular foreign keys:
 *
 * Part 1  TRUNCATE of the parent alone is refused (0A000, the native message, DETAIL and HINT) while a
 *         child row references it, for NO ACTION and for CASCADE, and nothing is truncated.
 * Part 2  TRUNCATE of the parent together with the child (either order) succeeds, as natively.
 * Part 3  A child with no referencing rows (empty, or only NULL keys) does not block TRUNCATE.
 * Part 4  The object: one statement-level AFTER TRUNCATE trigger per regular foreign key on the parent,
 *         catalogued in foreign_keys.uk_truncate_trigger; drop_foreign_key removes it, DROP TRIGGER is
 *         refused by drop protection, disable_temporal_triggers disables it, check_integrity reports it
 *         missing or disabled, and a column rename keeps it working.
 * Part 5  A temporal-to-temporal foreign key is native, so PostgreSQL itself refuses (unchanged).
 */

BEGIN;
CREATE SCHEMA rft;
SET search_path = rft, public;

CREATE FUNCTION rft.outcome(statement text) RETURNS text LANGUAGE plpgsql AS $outcome$
DECLARE
    detail text;
    hint text;
BEGIN
    BEGIN
        EXECUTE statement;
        RETURN 'ok';
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS detail = PG_EXCEPTION_DETAIL, hint = PG_EXCEPTION_HINT;
        RETURN SQLSTATE || ': ' || SQLERRM || coalesce(' DETAIL: ' || nullif(detail, ''), '') || coalesce(' HINT: ' || nullif(hint, ''), '');
    END;
END;
$outcome$;

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

CREATE TABLE rft.legal_unit (id int NOT NULL, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('rft.legal_unit', 'valid_range', 'valid',
    valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('rft.legal_unit', ARRAY['id'], key_type => 'primary', unique_key_name => 'legal_unit_id_valid');
INSERT INTO rft.legal_unit (id, valid_from, valid_until) VALUES (1, '2020-01-01', 'infinity'), (2, '2020-01-01', 'infinity');

CREATE TABLE rft.note (id int PRIMARY KEY, legal_unit_id int);   -- CASCADE
CREATE TABLE rft.tag (id int PRIMARY KEY, legal_unit_id int);    -- NO ACTION
SELECT sql_saga.add_foreign_key('rft.note', ARRAY['legal_unit_id'], 'rft.legal_unit', ARRAY['id'],
    delete_action => 'CASCADE', foreign_key_name => 'note_fk');
SELECT sql_saga.add_foreign_key('rft.tag', ARRAY['legal_unit_id'], 'rft.legal_unit', ARRAY['id'],
    foreign_key_name => 'tag_fk');
INSERT INTO rft.note VALUES (1, 1);
INSERT INTO rft.tag VALUES (1, 2);

CREATE FUNCTION rft.counts() RETURNS text LANGUAGE sql AS $counts$
    SELECT format('legal_unit %s, note %s, tag %s',
        (SELECT count(*) FROM rft.legal_unit), (SELECT count(*) FROM rft.note), (SELECT count(*) FROM rft.tag));
$counts$;

CREATE TABLE rft.stat (id int NOT NULL, legal_unit_id int, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('rft.stat', 'valid_range', 'valid',
    valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');

\o
\echo '--- part 1: TRUNCATE of the parent alone is refused while children reference it'
SAVEPOINT p;
SELECT 'TRUNCATE legal_unit (note CASCADE, tag NO ACTION) | ' || rft.outcome('TRUNCATE rft.legal_unit');
SELECT 'nothing truncated | ' || rft.counts();
DELETE FROM rft.tag;
SELECT 'TRUNCATE legal_unit, only the CASCADE child references it | ' || rft.outcome('TRUNCATE rft.legal_unit');
SELECT 'nothing truncated | ' || rft.counts();
ROLLBACK TO SAVEPOINT p;
DELETE FROM rft.note;
SELECT 'TRUNCATE legal_unit, only the NO ACTION child references it | ' || rft.outcome('TRUNCATE rft.legal_unit');
ROLLBACK TO SAVEPOINT p;

\echo '--- part 2: TRUNCATE together with the children succeeds'
SELECT 'TRUNCATE legal_unit, note, tag | ' || rft.outcome('TRUNCATE rft.legal_unit, rft.note, rft.tag');
SELECT 'all empty | ' || rft.counts();
ROLLBACK TO SAVEPOINT p;
SELECT 'TRUNCATE tag, note, legal_unit (children first) | ' || rft.outcome('TRUNCATE rft.tag, rft.note, rft.legal_unit');
SELECT 'all empty | ' || rft.counts();
ROLLBACK TO SAVEPOINT p;

\echo '--- part 3: children without references do not block'
UPDATE rft.note SET legal_unit_id = NULL;
DELETE FROM rft.tag;
SELECT 'only NULL keys and an empty child | ' || rft.outcome('TRUNCATE rft.legal_unit');
SELECT 'parent empty, NULL-key note kept | ' || rft.counts();
ROLLBACK TO SAVEPOINT p;

\echo '--- part 4: the managed object'
SELECT 'object | ' || fk.foreign_key_name || ' | ' || coalesce(fk.uk_truncate_trigger::text, 'no catalog name') || ' | '
    || coalesce(CASE WHEN t.tgtype = 32 THEN 'after truncate for each statement' ELSE 'tgtype ' || t.tgtype END, 'no trigger') || ' | '
    || coalesce(t.tgfoid::regprocedure::text, '-') || ' | ' || coalesce(CASE t.tgenabled WHEN 'O' THEN 'enabled' ELSE t.tgenabled::text END, '-')
FROM sql_saga.foreign_keys AS fk
LEFT JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = 'rft.legal_unit'::regclass AND t.tgname = fk.uk_truncate_trigger
WHERE fk.table_schema = 'rft' AND fk.type = 'regular_to_temporal'
ORDER BY fk.foreign_key_name;
SELECT 'check_integrity | ' || rft.integrity();
SELECT 'DROP TRIGGER directly | ' || rft.outcome(format('DROP TRIGGER %I ON rft.legal_unit',
    (SELECT uk_truncate_trigger FROM sql_saga.foreign_keys WHERE foreign_key_name = 'note_fk')));
SAVEPOINT q;
SELECT 'disable_temporal_triggers | ' || rft.outcome($$CALL sql_saga.disable_temporal_triggers('rft.legal_unit')$$);
SELECT 'truncate trigger disabled | ' || t.tgenabled::text
FROM pg_catalog.pg_trigger AS t
WHERE t.tgrelid = 'rft.legal_unit'::regclass AND t.tgname = (SELECT uk_truncate_trigger FROM sql_saga.foreign_keys WHERE foreign_key_name = 'note_fk');
SELECT 'check_integrity reports it | ' || rft.integrity();
SELECT 'TRUNCATE while disabled (as for the delete check) | ' || rft.outcome('TRUNCATE rft.legal_unit');
ROLLBACK TO SAVEPOINT q;
ALTER TABLE rft.legal_unit RENAME COLUMN id TO unit_id;
SELECT 'after renaming the referenced column, TRUNCATE | ' || rft.outcome('TRUNCATE rft.legal_unit');
SELECT 'check_integrity | ' || rft.integrity();
ROLLBACK TO SAVEPOINT q;
ALTER TABLE rft.note RENAME COLUMN legal_unit_id TO unit_id;
SELECT 'after renaming the child column, TRUNCATE | ' || rft.outcome('TRUNCATE rft.legal_unit');
ROLLBACK TO SAVEPOINT q;
SELECT 'drop_foreign_key note_fk | ' || rft.outcome($$SELECT sql_saga.drop_foreign_key('rft.note', ARRAY['legal_unit_id'])$$);
SELECT 'truncate triggers left for note_fk | ' || count(*)
FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = 'rft.legal_unit'::regclass AND t.tgname LIKE 'note_fk%truncate%';
SELECT 'after drop_foreign_key, only tag blocks | ' || rft.outcome('TRUNCATE rft.legal_unit');
ROLLBACK TO SAVEPOINT q;

\echo '--- part 5: temporal-to-temporal foreign keys are native'
\o /dev/null
SELECT sql_saga.add_foreign_key('rft.stat', ARRAY['legal_unit_id'], 'rft.legal_unit', ARRAY['id']);
INSERT INTO rft.stat (id, legal_unit_id, valid_from, valid_until) VALUES (1, 1, '2021-01-01', 'infinity');
\o
DELETE FROM rft.note; DELETE FROM rft.tag;
SELECT 'TRUNCATE legal_unit with a temporal child | ' || rft.outcome('TRUNCATE rft.legal_unit');
ROLLBACK TO SAVEPOINT p;
SELECT 'check_integrity at the end | ' || rft.integrity();

ROLLBACK;
\i sql/include/test_teardown.sql
