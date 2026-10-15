\i sql/include/test_setup.sql

-- Error context would name PL/pgSQL line numbers, which are an implementation detail.
\set SHOW_CONTEXT never

BEGIN;

/* Run tests as unprivileged user */
SET ROLE TO sql_saga_unprivileged_user;

/* DDL on unrelated tables should not be affected */
CREATE UNLOGGED TABLE unrelated();
ALTER TABLE unrelated SET LOGGED;
ALTER TABLE unrelated SET UNLOGGED;
DROP TABLE unrelated;

/* Ensure tables with periods are persistent */
CREATE UNLOGGED TABLE log (id bigint, valid_range daterange NOT NULL, valid_from date, valid_until date);
SAVEPOINT s1;
SELECT sql_saga.add_era('log', 'valid_range', 'p', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until'); -- fails
ROLLBACK TO SAVEPOINT s1;
ALTER TABLE log SET LOGGED;
SELECT sql_saga.add_era('log', 'valid_range', 'p', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until'); -- passes
SAVEPOINT s2;
ALTER TABLE log SET UNLOGGED; -- fails
ROLLBACK TO SAVEPOINT s2;
DROP TABLE log;

ROLLBACK;

--------------------------------------------------------------------------------
-- SYSTEM VERSIONING HEALTH CHECKS
--------------------------------------------------------------------------------

\echo '\n--- System Versioning Health Checks ---'

-- These tests need to run outside the transaction with controlled commits
-- because event triggers fire at end of DDL statements

-- Create a test table with system versioning
CREATE TABLE hc_sysver (
    id serial PRIMARY KEY,
    data text
);
SELECT sql_saga.add_system_versioning('hc_sysver');

-- Verify objects exist
\echo '--- Verify system versioning objects created ---'
SELECT table_name, history_table_name, view_table_name FROM sql_saga.system_versioning WHERE table_name = 'hc_sysver';

-- Test 1: access to history follows SELECT on the base, decided when history is read
\echo '\n--- Test: access to history follows the base table, with no propagation ---'
INSERT INTO hc_sysver (data) VALUES ('v1');
UPDATE hc_sysver SET data = 'v2';
GRANT SELECT ON hc_sysver TO sql_saga_unprivileged_user;
SET ROLE sql_saga_unprivileged_user;
SELECT count(*) AS with_history_rows FROM hc_sysver_with_history;
SELECT count(*) AS as_of_rows FROM hc_sysver__as_of(now());
RESET ROLE;

-- Test 2: REVOKE on the base ends history access at once
\echo '\n--- Test: REVOKE on the base ends history access ---'
REVOKE SELECT ON hc_sysver FROM sql_saga_unprivileged_user;
SET ROLE sql_saga_unprivileged_user;
SELECT count(*) AS with_history_rows FROM hc_sysver_with_history; -- fail
SELECT count(*) AS history_rows FROM hc_sysver_history;
RESET ROLE;

-- Test 2b: the fixed state of the history objects is enforced by the health check
\echo '\n--- Test: health check keeps the fixed state of the history objects ---'
ALTER TABLE hc_sysver_history DISABLE ROW LEVEL SECURITY; -- fail
ALTER TABLE hc_sysver_history FORCE ROW LEVEL SECURITY; -- fail: hc_sysver is not forced
DROP POLICY sql_saga_history_base_select ON hc_sysver_history; -- fail
ALTER POLICY sql_saga_history_reader ON hc_sysver_history USING (true); -- fail
DROP POLICY sql_saga_history_reader ON hc_sysver_history; -- fail
ALTER VIEW hc_sysver_with_history SET (security_invoker = off); -- fail
CREATE OR REPLACE VIEW hc_sysver_history_gated AS SELECT * FROM hc_sysver_history; -- fail: the gate would be lost
\echo '--- A hashed policy copy is managed too'
ALTER TABLE hc_sysver ENABLE ROW LEVEL SECURITY;
-- NOTICEs off: they name the copy, whose name contains the per-installation hash.
SET client_min_messages = warning;
CREATE POLICY everyone ON hc_sysver FOR SELECT USING (true);
RESET client_min_messages;
-- The copy's name holds a per-installation hash, so only the outcome of dropping it is shown.
DO $$
DECLARE
    copy_name name;
BEGIN
    SELECT p.polname INTO copy_name FROM pg_policy AS p
    WHERE p.polrelid = 'hc_sysver_history'::regclass AND p.polname ~ '^sql_saga_[0-9a-f]{16}$';
    EXECUTE format('DROP POLICY %I ON hc_sysver_history', copy_name);
    RAISE NOTICE 'dropping the copy succeeded';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'dropping the copy failed: %', replace(SQLERRM, copy_name, '<copy>');
END;
$$;
SELECT count(*) AS copies FROM pg_policy AS p
WHERE p.polrelid = 'hc_sysver_history'::regclass AND p.polname ~ '^sql_saga_[0-9a-f]{16}$';
SET client_min_messages = warning;
DROP POLICY everyone ON hc_sysver;
RESET client_min_messages;
ALTER TABLE hc_sysver DISABLE ROW LEVEL SECURITY;
SELECT c.relname, c.relrowsecurity, c.relforcerowsecurity,
       coalesce('security_invoker=on' = ANY (c.reloptions), false) AS security_invoker
FROM pg_class AS c
WHERE c.relname IN ('hc_sysver_history', 'hc_sysver_with_history')
ORDER BY c.relname;

-- Test 3: Ownership propagation
\echo '\n--- Test: Ownership propagation ---'
-- Change owner of base table
ALTER TABLE hc_sysver OWNER TO sql_saga_unprivileged_user;

-- Verify history objects ownership changed
\echo '--- Checking history object ownership ---'
SELECT
    c.relname,
    pg_get_userbyid(c.relowner) AS owner
FROM pg_class c
WHERE c.relname IN ('hc_sysver', 'hc_sysver_history', 'hc_sysver_history_gated', 'hc_sysver_with_history')
ORDER BY c.relname;

-- Verify function ownership changed
\echo '--- Checking function ownership ---'
SELECT
    p.proname,
    pg_get_userbyid(p.proowner) AS owner
FROM sql_saga.system_versioning sv
JOIN pg_proc p ON p.oid = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]::regprocedure[])
WHERE sv.table_name = 'hc_sysver'
ORDER BY p.proname;

-- Cleanup - change owner back before dropping
ALTER TABLE hc_sysver OWNER TO CURRENT_USER;
SELECT sql_saga.drop_system_versioning('hc_sysver');
DROP TABLE hc_sysver;

\i sql/include/test_teardown.sql
