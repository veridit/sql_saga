\set ECHO none
\set QUIET 1
\set ON_ERROR_STOP on
SET client_min_messages = warning;
\i sql/include/test_setup.sql
-- 1.8: the history table of a system-versioned table must stay persistent, like its base table. Up to 1.7 the
-- check read sql_saga.era.audit_schema_name/audit_table_name, columns left from the periods extension that no
-- release ever wrote, so ALTER TABLE <history> SET UNLOGGED was accepted. 1.8 removes those columns and checks
-- the history tables sql_saga.system_versioning names. Everything here is rolled back at the end.
BEGIN;
CREATE SCHEMA hp_160;
SET LOCAL search_path = hp_160, public;
CREATE TABLE doc (id int, valid_range daterange, valid_from date, valid_until date, PRIMARY KEY (id, valid_range WITHOUT OVERLAPS));
SELECT sql_saga.add_era('doc', 'valid_range') AS ignored \gset
SELECT sql_saga.add_system_versioning('doc') AS ignored \gset
CREATE TABLE plain_history (id int);

SELECT column_name FROM information_schema.columns
WHERE table_schema = 'sql_saga' AND table_name = 'era' AND column_name LIKE 'audit%'
ORDER BY column_name;
\echo PASS sql_saga.era has no audit columns

SAVEPOINT unlogged_history;
DO $assert$
BEGIN
    BEGIN
        ALTER TABLE doc_history SET UNLOGGED;
        RAISE EXCEPTION 'Expected refusal' USING ERRCODE = 'P0002';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN
        IF SQLERRM <> 'history table "hp_160.doc_history" must remain persistent because it is used in SYSTEM VERSIONING for table "hp_160.doc"' THEN
            RAISE EXCEPTION 'Unexpected message: %', SQLERRM;
        END IF;
    END;
    IF (SELECT relpersistence FROM pg_class WHERE oid = 'doc_history'::regclass) <> 'p' THEN
        RAISE EXCEPTION 'history table is no longer persistent';
    END IF;
END $assert$;
\echo PASS SET UNLOGGED on a history table is refused and nothing changes
ROLLBACK TO unlogged_history;

SAVEPOINT hint;
DO $show$
DECLARE state text; msg text; hint text;
BEGIN
    ALTER TABLE doc_history SET UNLOGGED;
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS state = RETURNED_SQLSTATE, msg = MESSAGE_TEXT, hint = PG_EXCEPTION_HINT;
    RAISE WARNING '% %', state, msg;
    RAISE WARNING 'HINT: %', hint;
END $show$;
ROLLBACK TO hint;

SAVEPOINT unlogged_base;
DO $assert$
BEGIN
    BEGIN
        ALTER TABLE doc SET UNLOGGED;
        RAISE EXCEPTION 'Expected refusal' USING ERRCODE = 'P0002';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM NOT LIKE 'table "hp_160.doc" must remain persistent%' THEN RAISE; END IF;
    END;
END $assert$;
\echo PASS SET UNLOGGED on the base table is still refused
ROLLBACK TO unlogged_base;

SAVEPOINT unrelated;
ALTER TABLE plain_history SET UNLOGGED;
\echo PASS an unrelated table may become UNLOGGED
ROLLBACK TO unrelated;

SAVEPOINT bypassed;
-- Made UNLOGGED while the event triggers were off: check_integrity names the repair.
SET LOCAL session_replication_role = replica;
ALTER TABLE doc_history SET UNLOGGED;
RESET session_replication_role;
DO $assert$
BEGIN
    BEGIN
        PERFORM sql_saga.check_integrity();
        RAISE EXCEPTION 'Expected integrity violation' USING ERRCODE = 'P0002';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM <> 'sql_saga: integrity violation for system-versioned table "hp_160.doc": history table "hp_160.doc_history" is not persistent; repair with ALTER TABLE hp_160.doc_history SET LOGGED' THEN
            RAISE;
        END IF;
    END;
    ALTER TABLE doc_history SET LOGGED;
    PERFORM sql_saga.check_integrity();
END $assert$;
\echo PASS check_integrity names ALTER TABLE ... SET LOGGED, which repairs it
ROLLBACK TO bypassed;

SELECT sql_saga.check_integrity() AS ignored \gset
\echo PASS check_integrity
ROLLBACK;
\i sql/include/test_teardown.sql
\echo PASS 1.8 history persistence without the era audit columns
