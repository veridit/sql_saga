\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/*
 * System versioning never runs user code with the extension owner's rights (1.4, review round 2, R1b).
 *
 * The system-versioning trigger functions are SECURITY DEFINER, so a table owner can create them without EXECUTE
 * grants, and they write the history as the extension owner. Up to 1.3 that meant user code reached from the
 * history write -- a trigger, a column default, a CHECK or index expression on the history table, all chosen by
 * the table owner -- ran as the extension owner, usually a superuser: any role that may create a table could become
 * superuser. Native PostgreSQL runs such code as the role whose statement fires it, or as the table owner for
 * referential actions, never with borrowed rights.
 *
 * From 1.4 the history write (INSERT and the TRUNCATE that follows a base TRUNCATE) runs as the owner of the history
 * table, as native referential actions run as the referencing table's owner. The writer of the base table still
 * needs no rights on the history table.
 *
 * Part 1  A trigger and a CHECK expression on the history table see the history owner, not a superuser.
 * Part 2  A writer with INSERT/UPDATE/DELETE/TRUNCATE on the base table only, and no rights on the history table,
 *         can still write: the history is kept.
 * Part 3  A trigger created by hand on another table with write_history or truncate_system_versioning cannot reach
 *         a registered history.
 *
 * History rows are written only for row versions that lived at least one transaction, so the steps commit.
 */

CREATE ROLE svu_owner;
CREATE ROLE svu_writer;
CREATE ROLE svu_attacker;
CREATE SCHEMA svu AUTHORIZATION svu_owner;
CREATE SCHEMA svu_att AUTHORIZATION svu_attacker;
GRANT USAGE ON SCHEMA svu TO svu_writer;
GRANT USAGE ON SCHEMA sql_saga TO svu_owner, svu_writer, svu_attacker;

SET ROLE svu_owner;
CREATE TABLE svu.t (id int NOT NULL, note text);
SELECT sql_saga.add_system_versioning('svu.t');
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON svu.t TO svu_writer;
CREATE TABLE svu.seen (who text, super boolean, via text);
GRANT INSERT ON svu.seen TO PUBLIC;
CREATE FUNCTION svu.spy() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO svu.seen SELECT current_user, r.rolsuper, TG_TABLE_NAME || ' ' || TG_OP
    FROM pg_catalog.pg_roles AS r WHERE r.rolname = current_user;
    RETURN NULL;
END $$;
CREATE TRIGGER spy AFTER INSERT ON svu.t_history FOR EACH ROW EXECUTE FUNCTION svu.spy();
CREATE TRIGGER spy_truncate AFTER TRUNCATE ON svu.t_history FOR EACH STATEMENT EXECUTE FUNCTION svu.spy();
-- A CHECK expression on the history table, evaluated for every history row, records who evaluates it.
CREATE FUNCTION svu.check_spy(note text) RETURNS boolean LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    INSERT INTO svu.seen SELECT current_user, r.rolsuper, 't_history CHECK'
    FROM pg_catalog.pg_roles AS r WHERE r.rolname = current_user;
    RETURN true;
END $$;
ALTER TABLE svu.t_history ADD CONSTRAINT spy_check CHECK (svu.check_spy(note));
RESET ROLE;

\o
\echo '--- part 1: user code on the history table runs as the history owner'
SET ROLE svu_owner;
INSERT INTO svu.t VALUES (1, 'a');
UPDATE svu.t SET note = 'b';
RESET ROLE;
SELECT who || ' | superuser ' || super || ' | ' || via FROM svu.seen ORDER BY via;

\echo '--- part 2: a writer with rights on the base table only keeps the history'
DELETE FROM svu.seen;
SET ROLE svu_writer;
UPDATE svu.t SET note = 'c';
DELETE FROM svu.t;
SELECT 'writer can read the history | ' || has_table_privilege('svu.t_history', 'SELECT');
RESET ROLE;
SELECT 'history rows | ' || count(*) FROM svu.t_history;
SELECT who || ' | superuser ' || super || ' | ' || via FROM svu.seen ORDER BY via;
SET ROLE svu_writer;
INSERT INTO svu.t VALUES (2, 'x');
TRUNCATE svu.t;
RESET ROLE;
SELECT 'after the writer''s TRUNCATE, history rows | ' || count(*) FROM svu.t_history;
SELECT who || ' | superuser ' || super || ' | ' || via FROM svu.seen WHERE via LIKE '%TRUNCATE' ORDER BY via;

\echo '--- part 3: forged triggers cannot reach a registered history'
SET ROLE svu_owner;
INSERT INTO svu.t VALUES (3, 'kept');
UPDATE svu.t SET note = 'kept 2';
RESET ROLE;
CREATE TEMP TABLE svu_before AS SELECT count(*) AS n FROM svu.t_history;
SET ROLE svu_attacker;
CREATE TABLE svu_att.t (id int NOT NULL, note text, system_valid_range tstzrange);
CREATE TRIGGER f AFTER TRUNCATE ON svu_att.t FOR EACH STATEMENT EXECUTE FUNCTION sql_saga.truncate_system_versioning();
CREATE TRIGGER w AFTER DELETE ON svu_att.t FOR EACH ROW EXECUTE FUNCTION sql_saga.write_history('system_valid_range');
TRUNCATE svu_att.t;
RESET ROLE;
SELECT 'registered history unchanged | ' || ((SELECT count(*) FROM svu.t_history) = (SELECT n FROM svu_before))::text;

RESET ROLE;
DROP SCHEMA svu CASCADE;
DROP SCHEMA svu_att CASCADE;
REVOKE USAGE ON SCHEMA sql_saga FROM svu_owner, svu_writer, svu_attacker;
DROP ROLE svu_owner;
DROP ROLE svu_writer;
DROP ROLE svu_attacker;
\i sql/include/test_teardown.sql
