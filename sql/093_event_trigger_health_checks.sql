\i sql/include/test_setup.sql

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

-- Test 1: GRANT propagation
\echo '\n--- Test: GRANT propagation ---'
GRANT SELECT ON hc_sysver TO sql_saga_unprivileged_user;

-- Verify grants propagated to history table
\echo '--- Checking history table privileges ---'
SELECT has_table_privilege('sql_saga_unprivileged_user', 'hc_sysver_history', 'SELECT') AS history_select;

-- Verify grants propagated to view
\echo '--- Checking history view privileges ---'
SELECT has_table_privilege('sql_saga_unprivileged_user', 'hc_sysver_with_history', 'SELECT') AS view_select;

-- Verify EXECUTE granted on functions
\echo '--- Checking function privileges ---'
SELECT
    has_function_privilege('sql_saga_unprivileged_user', sv.func_as_of::regprocedure, 'EXECUTE') AS as_of_execute,
    has_function_privilege('sql_saga_unprivileged_user', sv.func_between::regprocedure, 'EXECUTE') AS between_execute,
    has_function_privilege('sql_saga_unprivileged_user', sv.func_between_symmetric::regprocedure, 'EXECUTE') AS between_symmetric_execute,
    has_function_privilege('sql_saga_unprivileged_user', sv.func_from_to::regprocedure, 'EXECUTE') AS from_to_execute
FROM sql_saga.system_versioning sv
WHERE sv.table_name = 'hc_sysver';

-- Test 2: REVOKE propagation
\echo '\n--- Test: REVOKE propagation ---'
REVOKE SELECT ON hc_sysver FROM sql_saga_unprivileged_user;

-- Verify revokes propagated
\echo '--- Checking privileges revoked from history objects ---'
SELECT
    has_table_privilege('sql_saga_unprivileged_user', 'hc_sysver_history', 'SELECT') AS history_select,
    has_table_privilege('sql_saga_unprivileged_user', 'hc_sysver_with_history', 'SELECT') AS view_select;

\echo '--- Checking function privileges revoked ---'
SELECT
    has_function_privilege('sql_saga_unprivileged_user', sv.func_as_of::regprocedure, 'EXECUTE') AS as_of_execute,
    has_function_privilege('sql_saga_unprivileged_user', sv.func_between::regprocedure, 'EXECUTE') AS between_execute,
    has_function_privilege('sql_saga_unprivileged_user', sv.func_between_symmetric::regprocedure, 'EXECUTE') AS between_symmetric_execute,
    has_function_privilege('sql_saga_unprivileged_user', sv.func_from_to::regprocedure, 'EXECUTE') AS from_to_execute
FROM sql_saga.system_versioning sv
WHERE sv.table_name = 'hc_sysver';

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
WHERE c.relname IN ('hc_sysver', 'hc_sysver_history', 'hc_sysver_with_history')
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
