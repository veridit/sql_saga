\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/*
 * Forged foreign key triggers (1.4, review finding F1).
 *
 * sql_saga's foreign key trigger functions run their lookups (and the ON DELETE CASCADE delete) with the rights of
 * the extension owner, from the table and column names in their trigger arguments. Any role that can create a
 * trigger on a table of its own can call them with made-up arguments. Up to 1.3 that gave an existence oracle on
 * tables the role cannot read, and with CASCADE arguments it deleted rows from them.
 *
 * From 1.4 every such function first checks that it fires as the trigger sql_saga created for a registered foreign
 * key: the catalog row exists, the trigger has the catalogued name on the catalogued table, and its arguments are
 * exactly the canonical arguments. Otherwise it refuses with 42501 before reading or changing anything.
 *
 * Part 1  A forged uk_delete trigger with CASCADE arguments: refused, the victim's rows are untouched.
 * Part 2  The existence oracle through uk_delete, uk_update, regular_fk_check and uk_truncate: every attempt is
 *         refused with the same error, whatever the key, so nothing about the unreadable table is revealed.
 * Part 3  A copy of a genuine trigger's arguments on another table (replay): refused.
 * Part 4  The genuine triggers keep working (control).
 */

BEGIN;
CREATE SCHEMA ff;
CREATE ROLE ff_attacker;

CREATE FUNCTION ff.outcome(statement text) RETURNS text LANGUAGE plpgsql AS $outcome$
BEGIN
    BEGIN
        EXECUTE statement;
        RETURN 'ok';
    EXCEPTION WHEN OTHERS THEN
        RETURN SQLSTATE || ': ' || SQLERRM;
    END;
END;
$outcome$;
GRANT USAGE ON SCHEMA ff TO ff_attacker;
GRANT EXECUTE ON FUNCTION ff.outcome(text) TO ff_attacker;

-- The victim: a table the attacker can see but not read, delete from or write.
CREATE SCHEMA ff_secret;
CREATE TABLE ff_secret.victim (id int PRIMARY KEY, note text);
INSERT INTO ff_secret.victim VALUES (42, 'must survive'), (43, 'other');
GRANT USAGE ON SCHEMA ff_secret TO ff_attacker;

-- A genuine regular foreign key (control and replay source).
CREATE TABLE ff.parent (id int NOT NULL, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('ff.parent', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('ff.parent', ARRAY['id'], key_type => 'primary', unique_key_name => 'ff_parent_pk');
INSERT INTO ff.parent (id, valid_from, valid_until) VALUES (1, '2020-01-01', 'infinity'), (2, '2020-01-01', 'infinity');
CREATE TABLE ff.child (id int PRIMARY KEY, parent_id int);
SELECT sql_saga.add_foreign_key('ff.child', ARRAY['parent_id'], 'ff.parent', ARRAY['id'],
    delete_action => 'CASCADE', foreign_key_name => 'ff_child_fk');
INSERT INTO ff.child VALUES (1, 1);

-- The attacker's own tables.
CREATE SCHEMA ff_att AUTHORIZATION ff_attacker;
SET LOCAL ROLE ff_attacker;
CREATE TABLE ff_att.bait (id int);
CREATE TABLE ff_att.bait_child (id int);

-- Every forged trigger names ff_secret.victim as the "child" and the attacker's bait as the "parent".
CREATE CONSTRAINT TRIGGER forged_delete AFTER DELETE ON ff_att.bait DEFERRABLE INITIALLY IMMEDIATE
    FOR EACH ROW EXECUTE PROCEDURE sql_saga.uk_delete_check_c(
        'forged_fk', 'ff_secret', 'victim', '{id}', '', '', '', 'ff_att', 'bait', '{id}', '', '', '',
        'SIMPLE', 'NO ACTION', 'CASCADE', 'regular_to_temporal');
CREATE CONSTRAINT TRIGGER forged_update AFTER UPDATE ON ff_att.bait DEFERRABLE INITIALLY IMMEDIATE
    FOR EACH ROW EXECUTE PROCEDURE sql_saga.uk_update_check_c(
        'forged_fk', 'ff_secret', 'victim', '{id}', '', '', '', 'ff_att', 'bait', '{id}', '', '', '',
        'SIMPLE', 'NO ACTION', 'NO ACTION', 'regular_to_temporal');
CREATE TRIGGER forged_truncate AFTER TRUNCATE ON ff_att.bait FOR EACH STATEMENT
    EXECUTE FUNCTION sql_saga.uk_truncate_check_c(
        'forged_fk', 'ff_secret', 'victim', '{id}', '', '', '', 'ff_att', 'bait', '{id}', '', '', '',
        'SIMPLE', 'NO ACTION', 'NO ACTION', 'regular_to_temporal');
-- The child-side check with the victim as "parent": an existence oracle on its keys.
CREATE CONSTRAINT TRIGGER forged_child AFTER INSERT ON ff_att.bait_child DEFERRABLE INITIALLY IMMEDIATE
    FOR EACH ROW EXECUTE FUNCTION sql_saga.regular_fk_check_c(
        'forged_fk', 'ff_att', 'bait_child', '{id}', '', '', '', 'ff_secret', 'victim', '{id}', '', '', '',
        'SIMPLE', 'NO ACTION', 'NO ACTION', 'regular_to_temporal');
-- A replay: the genuine foreign key's exact delete-trigger arguments, on the attacker's own table.
CREATE CONSTRAINT TRIGGER replayed_delete AFTER DELETE ON ff_att.bait_child DEFERRABLE INITIALLY IMMEDIATE
    FOR EACH ROW EXECUTE PROCEDURE sql_saga.uk_delete_check_c(
        'ff_child_fk', 'ff', 'child', '{parent_id}', '', '', '', 'ff', 'parent', '{id}', 'valid', 'valid_from', 'valid_until',
        'SIMPLE', 'NO ACTION', 'CASCADE', 'regular_to_temporal');
INSERT INTO ff_att.bait VALUES (42), (98), (99);
RESET ROLE;

\o
\echo '--- part 1: a forged CASCADE delete trigger'
SET LOCAL ROLE ff_attacker;
SELECT 'DELETE of bait 42 (forged CASCADE onto the victim) | ' || ff.outcome('DELETE FROM ff_att.bait WHERE id = 42');
RESET ROLE;
SELECT 'victim rows | ' || string_agg(id::text, ',' ORDER BY id) FROM ff_secret.victim;

\echo '--- part 2: no existence oracle'
SET LOCAL ROLE ff_attacker;
SELECT 'uk_delete, key 43 exists in the victim | ' || ff.outcome('DELETE FROM ff_att.bait WHERE id = 99; INSERT INTO ff_att.bait VALUES (43); DELETE FROM ff_att.bait WHERE id = 43');
SELECT 'uk_delete, key 98 absent from the victim | ' || ff.outcome('DELETE FROM ff_att.bait WHERE id = 98');
SELECT 'uk_update, key 98 absent | ' || ff.outcome('UPDATE ff_att.bait SET id = 97 WHERE id = 98');
SELECT 'regular_fk_check, key 43 present | ' || ff.outcome('INSERT INTO ff_att.bait_child VALUES (43)');
SELECT 'regular_fk_check, key 98 absent | ' || ff.outcome('INSERT INTO ff_att.bait_child VALUES (98)');
SELECT 'uk_truncate | ' || ff.outcome('TRUNCATE ff_att.bait');
RESET ROLE;

\echo '--- part 3: replaying a genuine trigger''s arguments on another table'
ALTER TABLE ff_att.bait_child DISABLE TRIGGER forged_child;
INSERT INTO ff_att.bait_child VALUES (1);
SET LOCAL ROLE ff_attacker;
SELECT 'DELETE with replayed arguments | ' || ff.outcome('DELETE FROM ff_att.bait_child WHERE id = 1');
RESET ROLE;
SELECT 'genuine child rows | ' || string_agg(id::text, ',' ORDER BY id) FROM ff.child;

\echo '--- part 4: the genuine triggers still work'
SAVEPOINT g;
SELECT 'genuine CASCADE, delete parent 1 | ' || ff.outcome('DELETE FROM ff.parent WHERE id = 1');
SELECT 'genuine child rows after cascade | ' || coalesce(string_agg(id::text, ','), 'none') FROM ff.child;
ROLLBACK TO SAVEPOINT g;
SELECT 'genuine child insert of a missing key | ' || ff.outcome('INSERT INTO ff.child VALUES (9, 99)');
SELECT 'genuine TRUNCATE of the parent alone | ' || ff.outcome('TRUNCATE ff.parent');
SELECT 'check_integrity | ' || ff.outcome('SELECT sql_saga.check_integrity()');

\echo '--- part 4b: a verified trigger stays verified only while it is unchanged (R7 cache)'
-- In one transaction: the genuine delete trigger is verified on first use; then the extension owner's catalog row is
-- dropped by hand (as a broken installation would). The next firing must be refused, not served from the cache.
SAVEPOINT c;
SELECT 'first genuine firing (verified, key survives) | ' || ff.outcome('DELETE FROM ff.parent WHERE id = 2');
INSERT INTO ff.parent (id, valid_from, valid_until) VALUES (2, '2020-01-01', 'infinity');
ALTER TABLE ff.parent DISABLE TRIGGER USER;
ALTER TABLE ff.parent ENABLE TRIGGER USER;
DELETE FROM sql_saga.foreign_keys WHERE foreign_key_name = 'ff_child_fk';
SELECT 'firing after the catalog row is gone (after DDL on the table) | ' || ff.outcome('DELETE FROM ff.parent WHERE id = 1');
ROLLBACK TO SAVEPOINT c;

\echo '--- part 5: identity of the cascade (F2) and no rights needed on the child (F8)'
/*
 * The child table is owned by ff_child_owner. A trigger on it records who runs it. As native ON DELETE CASCADE, the
 * cascaded delete and the child's triggers run as the child table's owner, never as the extension owner. The parent's
 * writer (ff_writer) has DELETE on the parent and no rights at all on the child or its schema.
 */
\o /dev/null
CREATE ROLE ff_child_owner;
CREATE ROLE ff_writer;
CREATE SCHEMA ff_child AUTHORIZATION ff_child_owner;
GRANT USAGE ON SCHEMA ff TO ff_child_owner, ff_writer;
GRANT SELECT, REFERENCES ON ff.parent TO ff_child_owner;
GRANT SELECT, DELETE ON ff.parent TO ff_writer;
GRANT USAGE ON SCHEMA sql_saga TO ff_child_owner, ff_writer;
CREATE TABLE ff_child.audit (who text, session text);
CREATE TABLE ff_child.note (id int PRIMARY KEY, parent_id int);
ALTER TABLE ff_child.audit OWNER TO ff_child_owner;
ALTER TABLE ff_child.note OWNER TO ff_child_owner;
CREATE FUNCTION ff_child.note_deleted() RETURNS trigger LANGUAGE plpgsql AS $note_deleted$
BEGIN
    INSERT INTO ff_child.audit VALUES (current_user, session_user);
    RETURN OLD;
END;
$note_deleted$;
ALTER FUNCTION ff_child.note_deleted() OWNER TO ff_child_owner;
CREATE TRIGGER note_deleted AFTER DELETE ON ff_child.note FOR EACH ROW EXECUTE FUNCTION ff_child.note_deleted();
SELECT sql_saga.add_foreign_key('ff_child.note', ARRAY['parent_id'], 'ff.parent', ARRAY['id'],
    delete_action => 'CASCADE', foreign_key_name => 'ff_note_fk');
INSERT INTO ff_child.note VALUES (1, 2);
\o
SAVEPOINT w;
SET LOCAL ROLE ff_writer;
SELECT 'writer without rights on the child schema deletes parent 2 | ' || ff.outcome('DELETE FROM ff.parent WHERE id = 2');
RESET ROLE;
SELECT 'child rows after the cascade | ' || coalesce(string_agg(id::text, ','), 'none') FROM ff_child.note;
SELECT 'child trigger ran as | ' || who FROM ff_child.audit;
SELECT 'that role is a superuser | ' || rolsuper::text FROM pg_roles WHERE rolname = (SELECT who FROM ff_child.audit);
ROLLBACK TO SAVEPOINT w;

ROLLBACK;
\i sql/include/test_teardown.sql
