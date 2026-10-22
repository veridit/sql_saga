\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/*
 * Regular (non-temporal) to temporal foreign keys, partitioned and inheritance tables (1.4, review finding F4).
 *
 * Before 1.4 the referencing-side lookups scanned inheritance children and partitions, while ON DELETE CASCADE
 * and the TRUNCATE check used ONLY, so a partitioned child kept its rows (orphans) when the parent key went.
 * A partitioned referencing table also had triggers cloned to its partitions, which ATTACH PARTITION does not
 * validate. sql_saga does not support partitioned tables (add_era, add_system_versioning), so:
 *
 * Part 1  add_foreign_key refuses a partitioned referencing table, and nothing is created.
 * Part 2  A partition itself can reference: rows routed through the partitioned table are checked, and
 *         CASCADE removes them.
 * Part 3  As native foreign keys, a foreign key covers only its own table, not inheritance children, on
 *         either side: an inheritance child of the referencing table is not checked, does not block a delete
 *         and is not cascaded; a key present only in an inheritance child of the referenced table does not
 *         satisfy the foreign key, neither for new rows nor for existing rows when it is added.
 */

BEGIN;
CREATE SCHEMA rip;
SET search_path = rip, public;

CREATE FUNCTION rip.outcome(statement text) RETURNS text LANGUAGE plpgsql AS $outcome$
BEGIN
    BEGIN
        EXECUTE statement;
        RETURN 'ok';
    EXCEPTION WHEN OTHERS THEN
        RETURN SQLSTATE || ': ' || SQLERRM;
    END;
END;
$outcome$;

CREATE TABLE rip.legal_unit (id int NOT NULL, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('rip.legal_unit', 'valid_range', 'valid',
    valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('rip.legal_unit', ARRAY['id'], key_type => 'primary', unique_key_name => 'legal_unit_id_valid');
INSERT INTO rip.legal_unit (id, valid_from, valid_until) VALUES (1, '2020-01-01', 'infinity'), (2, '2020-01-01', 'infinity'), (3, '2020-01-01', 'infinity');

\o
\echo '--- part 1: a partitioned referencing table is refused'
CREATE TABLE rip.pnote (pid int, legal_unit_id int) PARTITION BY RANGE (pid);
CREATE TABLE rip.pnote_1 PARTITION OF rip.pnote FOR VALUES FROM (0) TO (100);
SELECT 'add_foreign_key on a partitioned table | ' || rip.outcome($$SELECT sql_saga.add_foreign_key('rip.pnote', ARRAY['legal_unit_id'], 'rip.legal_unit', ARRAY['id'], delete_action => 'CASCADE')$$);
SELECT 'catalogued foreign keys | ' || count(*) FROM sql_saga.foreign_keys;
SELECT 'triggers on pnote and its partition | ' || count(*) FROM pg_trigger WHERE tgrelid IN ('rip.pnote'::regclass, 'rip.pnote_1'::regclass);

\echo '--- part 2: a partition can reference; rows routed to it are checked and cascaded'
\o /dev/null
SELECT sql_saga.add_foreign_key('rip.pnote_1', ARRAY['legal_unit_id'], 'rip.legal_unit', ARRAY['id'],
    delete_action => 'CASCADE', foreign_key_name => 'pnote_1_fk');
\o
SELECT 'insert through the partitioned table, key present | ' || rip.outcome($$INSERT INTO rip.pnote VALUES (1, 1)$$);
SELECT 'insert through the partitioned table, key missing | ' || rip.outcome($$INSERT INTO rip.pnote VALUES (2, 99)$$);
SAVEPOINT p2;
DELETE FROM rip.legal_unit WHERE id = 1;
SET CONSTRAINTS ALL IMMEDIATE;
SELECT 'pnote rows after deleting key 1 (cascade) | ' || count(*) FROM rip.pnote;
ROLLBACK TO SAVEPOINT p2;

\echo '--- part 3: inheritance children are not covered, on either side, as natively'
CREATE TABLE rip.note (legal_unit_id int);
CREATE TABLE rip.note_sub () INHERITS (rip.note);
\o /dev/null
SELECT sql_saga.add_foreign_key('rip.note', ARRAY['legal_unit_id'], 'rip.legal_unit', ARRAY['id'],
    delete_action => 'CASCADE', foreign_key_name => 'note_fk');
\o
SELECT 'insert into the inheritance child, key missing (not checked) | ' || rip.outcome($$INSERT INTO rip.note_sub VALUES (99)$$);
INSERT INTO rip.note VALUES (2);
INSERT INTO rip.note_sub VALUES (2);
SAVEPOINT p3;
DELETE FROM rip.legal_unit WHERE id = 2;
SET CONSTRAINTS ALL IMMEDIATE;
SELECT 'after deleting key 2: note rows | ' || (SELECT count(*) FROM ONLY rip.note) || ', note_sub rows | ' || (SELECT count(*) FROM rip.note_sub);
ROLLBACK TO SAVEPOINT p3;

CREATE TABLE rip.tag (legal_unit_id int);
\o /dev/null
SELECT sql_saga.add_foreign_key('rip.tag', ARRAY['legal_unit_id'], 'rip.legal_unit', ARRAY['id'], foreign_key_name => 'tag_fk');
\o
INSERT INTO rip.tag VALUES (3);
CREATE TABLE rip.note_only_sub () INHERITS (rip.tag);
INSERT INTO rip.note_only_sub VALUES (3);
SAVEPOINT p3b;
DELETE FROM ONLY rip.tag;
SELECT 'NO ACTION: delete key 3, referenced only from the inheritance child | ' || rip.outcome($$DELETE FROM rip.legal_unit WHERE id = 3; SET CONSTRAINTS ALL IMMEDIATE$$);
ROLLBACK TO SAVEPOINT p3b;

CREATE TABLE rip.legal_unit_sub () INHERITS (rip.legal_unit);
INSERT INTO rip.legal_unit_sub (id, valid_range, valid_from, valid_until) VALUES (77, '[2020-01-01,infinity)', '2020-01-01', 'infinity');
SELECT 'key only in an inheritance child of the parent, new row | ' || rip.outcome($$INSERT INTO rip.tag VALUES (77)$$);
CREATE TABLE rip.late (legal_unit_id int);
INSERT INTO rip.late VALUES (77);
SELECT 'key only in an inheritance child of the parent, existing row | ' || rip.outcome($$SELECT sql_saga.add_foreign_key('rip.late', ARRAY['legal_unit_id'], 'rip.legal_unit', ARRAY['id'], foreign_key_name => 'late_fk')$$);

SELECT 'integrity | ' || rip.outcome($$SELECT sql_saga.check_integrity()$$);

ROLLBACK;
\i sql/include/test_teardown.sql
