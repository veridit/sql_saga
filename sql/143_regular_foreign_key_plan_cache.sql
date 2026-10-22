\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/*
 * The foreign key triggers' plan caches do not grow with the number of transactions in a session (1.4).
 *
 * The triggers keep their prepared plans for the rest of the transaction. Up to 1.3 the plans were never freed, so
 * a long-lived session (a worker, a connection pool) grew by the plans of every transaction that fired a trigger:
 * about 1 MB per transaction with sixty regular foreign keys.
 *
 * Part 1  Many transactions in one session that fire the referencing-side and the referenced-side triggers (and the
 *         cascade) leave the number of cached plans where it was after the first.
 * Part 2  A trigger first fired in a subtransaction that rolls back still works later in the same transaction.
 */

CREATE SCHEMA ppc;
CREATE TABLE ppc.unit (id int NOT NULL, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('ppc.unit', 'valid_range', 'valid',
    valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('ppc.unit', ARRAY['id'], key_type => 'primary', unique_key_name => 'unit_id_valid');
INSERT INTO ppc.unit (id, valid_from, valid_until) SELECT g, '2020-01-01', 'infinity' FROM generate_series(1, 10) AS g;
CREATE TABLE ppc.note (unit_id int);
CREATE TABLE ppc.tag (unit_id int);
SELECT sql_saga.add_foreign_key('ppc.note', ARRAY['unit_id'], 'ppc.unit', ARRAY['id'], delete_action => 'CASCADE', foreign_key_name => 'note_fk');
SELECT sql_saga.add_foreign_key('ppc.tag', ARRAY['unit_id'], 'ppc.unit', ARRAY['id'], foreign_key_name => 'tag_fk');

-- One transaction's work: a child insert, a parent update and a delete that cascades, then undone.
CREATE PROCEDURE ppc.work(transactions int) LANGUAGE plpgsql AS $work$
BEGIN
    FOR i IN 1..transactions LOOP
        INSERT INTO ppc.note VALUES (1);
        INSERT INTO ppc.tag VALUES (2);
        UPDATE ppc.unit SET id = 11 WHERE id = 3;
        DELETE FROM ppc.unit WHERE id = 1;
        INSERT INTO ppc.unit (id, valid_from, valid_until) VALUES (1, '2020-01-01', 'infinity');
        UPDATE ppc.unit SET id = 3 WHERE id = 11;
        DELETE FROM ppc.tag;
        COMMIT;
    END LOOP;
END;
$work$;

CREATE FUNCTION ppc.cached_plans() RETURNS bigint LANGUAGE sql AS $cached_plans$
    SELECT count(*) FROM pg_catalog.pg_backend_memory_contexts WHERE name = 'CachedPlanSource';
$cached_plans$;

CALL ppc.work(1);
CREATE TEMP TABLE baseline AS SELECT ppc.cached_plans() AS plans;
CALL ppc.work(200);

\o
\echo '--- part 1: no growth across transactions'
SELECT 'cached plans grew by | ' || (ppc.cached_plans() - (SELECT plans FROM baseline));
SELECT 'notes, units | ' || (SELECT count(*) FROM ppc.note) || ', ' || (SELECT count(*) FROM ppc.unit);

\echo '--- part 2: first firing in a subtransaction that rolls back'
BEGIN;
SAVEPOINT s;
INSERT INTO ppc.tag VALUES (5);
ROLLBACK TO SAVEPOINT s;
SAVEPOINT s2;
INSERT INTO ppc.tag VALUES (99);
ROLLBACK TO SAVEPOINT s2;
INSERT INTO ppc.tag VALUES (6);
SELECT 'tags | ' || string_agg(unit_id::text, ',') FROM ppc.tag;
ROLLBACK;

\echo '--- part 3: transactions that abort inside a trigger, then work again'
INSERT INTO ppc.tag VALUES (99);
INSERT INTO ppc.tag VALUES (2);
DELETE FROM ppc.unit WHERE id = 2;
INSERT INTO ppc.note VALUES (4);
DELETE FROM ppc.unit WHERE id = 4;
DELETE FROM ppc.tag;
SELECT 'notes after cascade, units | ' || (SELECT count(*) FROM ppc.note) || ', ' || (SELECT count(*) FROM ppc.unit);
SELECT 'cached plans after the run, compared with the first transaction | ' || (ppc.cached_plans() - (SELECT plans FROM baseline));

\o /dev/null
DROP SCHEMA ppc CASCADE;
\i sql/include/test_teardown.sql
