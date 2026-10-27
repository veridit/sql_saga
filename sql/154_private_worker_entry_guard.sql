\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/* A named EXECUTE grant is not authorization to enter a private worker. Only the public trampoline or its owner may. */
CREATE ROLE eg_owner;
CREATE ROLE eg_other;
CREATE SCHEMA eg AUTHORIZATION eg_owner;
GRANT USAGE ON SCHEMA eg, sql_saga TO eg_other;

CREATE FUNCTION eg.attempt(statement text) RETURNS text LANGUAGE plpgsql AS $attempt$
BEGIN
    BEGIN
        EXECUTE statement;
        RETURN 'ok';
    EXCEPTION WHEN OTHERS THEN
        RETURN SQLSTATE || ': ' || SQLERRM;
    END;
END;
$attempt$;
GRANT EXECUTE ON FUNCTION eg.attempt(text) TO PUBLIC;

CREATE FUNCTION eg.nested_worker() RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER
SET search_path = sql_saga, pg_catalog AS $nested_worker$
BEGIN
    RETURN sql_saga.__api_add_era('eg.t', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
END;
$nested_worker$;
ALTER FUNCTION eg.nested_worker() OWNER TO eg_other;
GRANT EXECUTE ON FUNCTION eg.nested_worker() TO PUBLIC;

SET ROLE eg_owner;
CREATE TABLE eg.t (id int, valid_range daterange, valid_from date, valid_until date);
INSERT INTO eg.t VALUES (1, daterange('2024-01-01', '2025-01-01', '[)'), date '2024-01-01', date '2025-01-01');
CREATE TABLE eg.sv (id int);
SELECT sql_saga.add_system_versioning('eg.sv');
RESET ROLE;
GRANT EXECUTE ON FUNCTION sql_saga.__api_add_era(regclass, name, name, boolean, name, name, name, boolean, boolean, boolean, regtype, name, name[]) TO eg_other;
GRANT EXECUTE ON FUNCTION sql_saga.__internal_regular_fk_initial_check(text, text, text, regclass, regclass, name[], name[]) TO eg_other;
GRANT EXECUTE ON FUNCTION sql_saga.__internal_refresh_system_time_columns_snapshot(oid) TO eg_other;
GRANT EXECUTE ON FUNCTION sql_saga.__internal_history_policy_sync_active(oid) TO eg_other;
GRANT EXECUTE ON FUNCTION sql_saga.__internal_truncate_as_owner(regclass) TO eg_other;
GRANT EXECUTE ON FUNCTION sql_saga.__internal_entry_guard() TO eg_other;

SET ROLE eg_other;
\o
SELECT 'direct API worker with named grant | ' || eg.attempt($$SELECT sql_saga.__api_add_era('eg.t', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until')$$);
SELECT 'nested definer worker with named grant | ' || eg.attempt($$SELECT eg.nested_worker()$$);
SELECT 'direct validator with named grant | ' || eg.attempt($$SELECT sql_saga.__internal_regular_fk_initial_check('DROP TABLE eg.t', 'SELECT 1', 'SELECT 1', 'eg.t'::regclass, 'eg.t'::regclass, ARRAY['id'], ARRAY['id'])$$);
SELECT 'direct snapshot writer with named grant | ' || eg.attempt($$SELECT sql_saga.__internal_refresh_system_time_columns_snapshot('eg.sv'::regclass::oid)$$);
SELECT 'direct history state helper with named grant | ' || eg.attempt($$SELECT sql_saga.__internal_history_policy_sync_active('eg.sv'::regclass::oid)$$);
SELECT 'direct TRUNCATE helper with named grant | ' || eg.attempt($$SELECT sql_saga.__internal_truncate_as_owner('eg.t'::regclass)$$);
SELECT 'direct guard with named grant | ' || eg.attempt($$SELECT sql_saga.__internal_entry_guard()$$);
SELECT 'table and catalog unchanged | ' || (to_regclass('eg.t') IS NOT NULL AND NOT EXISTS (SELECT FROM sql_saga.era WHERE table_schema = 'eg' AND table_name = 't'))::text;

\o /dev/null
RESET ROLE;
\o
SELECT 'table row preserved after direct TRUNCATE | ' || (count(*) = 1)::text FROM eg.t;
\o /dev/null
SET ROLE eg_owner;
SELECT sql_saga.add_era('eg.t', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
INSERT INTO eg.sv VALUES (1);
TRUNCATE eg.sv;
\o
SELECT 'public API positive control | ' || EXISTS (SELECT FROM sql_saga.era WHERE table_schema = 'eg' AND table_name = 't')::text;
SELECT 'public TRUNCATE positive control | ' || (NOT EXISTS (SELECT FROM eg.sv))::text;

\o /dev/null
RESET ROLE;
DROP SCHEMA eg CASCADE;
DROP OWNED BY eg_owner, eg_other;
DROP ROLE eg_owner, eg_other;
\i sql/include/test_teardown.sql
