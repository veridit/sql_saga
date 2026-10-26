\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/*
 * Predicated unique keys: user text is one expression, parsed and executed as the caller (Q4, D1).
 *
 * Before the fix the predicate was interpolated into ALTER TABLE run as the extension owner. A table owner could
 * smuggle another ALTER TABLE action into it. An unqualified function on the caller's search_path also failed after
 * the worker's path was hardened. The native statement accepts one index predicate, resolves names on the caller's
 * path and evaluates it with native rights.
 */

CREATE ROLE pred_owner;
CREATE ROLE pred_other;
CREATE SCHEMA pred AUTHORIZATION pred_owner;
GRANT USAGE ON SCHEMA sql_saga TO pred_owner, pred_other;
GRANT USAGE, CREATE ON SCHEMA pred TO pred_other;

SET ROLE pred_owner;
CREATE TABLE pred.k (id int NOT NULL, flag boolean, valid_range daterange NOT NULL, valid_from date, valid_until date);
INSERT INTO pred.k VALUES (1, true, daterange('2020-01-01', 'infinity'), '2020-01-01', 'infinity');
SELECT sql_saga.add_era('pred.k', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
CREATE FUNCTION pred.assert_caller(id int) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF current_user <> 'pred_owner' THEN
        RAISE EXCEPTION 'predicate executed as %', current_user;
    END IF;
    RETURN id >= 0;
END;
$$;
RESET ROLE;

CREATE FUNCTION pred.try(stmt text) RETURNS text LANGUAGE plpgsql AS $try$
DECLARE
    res text;
BEGIN
    BEGIN
        SET LOCAL ROLE pred_owner;
        EXECUTE stmt;
        res := 'allowed';
        RAISE EXCEPTION USING ERRCODE = 'P0099';
    EXCEPTION
        WHEN SQLSTATE 'P0099' THEN NULL;
        WHEN OTHERS THEN res := 'refused ' || SQLSTATE;
    END;
    RESET ROLE;
    RETURN res;
END;
$try$;
GRANT EXECUTE ON FUNCTION pred.try(text) TO PUBLIC;

-- The same, keeping a successful change. This lets a failed probe continue to the next part.
CREATE FUNCTION pred.run(stmt text) RETURNS text LANGUAGE plpgsql AS $run$
DECLARE
    res text;
BEGIN
    BEGIN
        SET LOCAL ROLE pred_owner;
        EXECUTE stmt;
        res := 'allowed';
    EXCEPTION WHEN OTHERS THEN
        res := 'refused ' || SQLSTATE || ': ' || SQLERRM;
    END;
    RESET ROLE;
    RETURN res;
END;
$run$;
GRANT EXECUTE ON FUNCTION pred.run(text) TO PUBLIC;

CREATE FUNCTION pred.try_other(stmt text) RETURNS text LANGUAGE plpgsql AS $try_other$
DECLARE
    res text;
BEGIN
    BEGIN
        SET LOCAL ROLE pred_other;
        EXECUTE stmt;
        res := 'allowed';
        RAISE EXCEPTION USING ERRCODE = 'P0099';
    EXCEPTION
        WHEN SQLSTATE 'P0099' THEN NULL;
        WHEN OTHERS THEN res := 'refused ' || SQLSTATE;
    END;
    RESET ROLE;
    RETURN res;
END;
$try_other$;
GRANT EXECUTE ON FUNCTION pred.try_other(text) TO PUBLIC;

\o
\echo '--- part 1: an ALTER TABLE action smuggled into the predicate is refused'
SELECT pred.try($stmt$SELECT sql_saga.add_unique_key('pred.k', ARRAY['id'], key_type => 'predicated', predicate => $pred$flag) DEFERRABLE, OWNER TO postgres, ADD CONSTRAINT injected EXCLUDE USING gist (id WITH =) WHERE (true$pred$)$stmt$);
SELECT 'owner after the attempt | ' || relowner::regrole FROM pg_class WHERE oid = 'pred.k'::regclass;
SELECT 'injected constraint exists | ' || count(*) FROM pg_constraint WHERE conrelid = 'pred.k'::regclass AND conname = 'injected';
SELECT 'catalog row exists | ' || count(*) FROM sql_saga.unique_keys WHERE table_schema = 'pred' AND table_name = 'k';

\echo '--- part 2: a qualified predicate is evaluated as the caller, not the extension owner'
SELECT 'qualified predicate | ' || pred.run($$SELECT sql_saga.add_unique_key('pred.k', ARRAY['id'], key_type => 'predicated', predicate => 'pred.assert_caller(id)', unique_key_name => 'k_qualified')$$);
SELECT 'qualified key integrity | ' || sql_saga.check_integrity();

\o /dev/null
DO $$
BEGIN
    IF EXISTS (SELECT FROM sql_saga.unique_keys WHERE unique_key_name = 'k_qualified') THEN
        PERFORM sql_saga.drop_unique_key_by_name('pred.k', 'k_qualified');
    END IF;
END;
$$;
\o
\echo '--- part 3: an unqualified predicate resolves on the caller''s search_path'
SELECT 'unqualified predicate | ' || pred.run($$SET search_path = pred, pg_catalog;
    SELECT sql_saga.add_unique_key('pred.k', ARRAY['id'], key_type => 'predicated', predicate => 'assert_caller(id)', unique_key_name => 'k_unqualified')$$);
SELECT 'unqualified key integrity | ' || sql_saga.check_integrity();

\echo '--- part 4: helpers are private and cannot lend the outer caller to nested user code'
SELECT 'direct execute helper | ' || pred.try_other($$SELECT sql_saga.__internal_execute_as_caller('SELECT 1')$$);
SELECT 'direct predicate helper | ' || pred.try_other($$SELECT sql_saga.__internal_predicate_for_table('pred.k', 'true')$$);
-- Even an explicit grant to the other role does not let its SECURITY DEFINER function borrow pred_owner's call.
GRANT EXECUTE ON FUNCTION sql_saga.__internal_execute_as_caller(text) TO pred_other;
SET ROLE pred_other;
CREATE FUNCTION pred.evil() RETURNS boolean LANGUAGE plpgsql IMMUTABLE SECURITY DEFINER AS $$
BEGIN
    PERFORM sql_saga.__internal_execute_as_caller('ALTER TABLE pred.k ADD COLUMN hijack int');
    RETURN true;
END;
$$;
RESET ROLE;
SELECT 'nested definer function | ' || pred.try($$SELECT sql_saga.add_unique_key('pred.k', ARRAY['id'], key_type => 'predicated', predicate => 'pred.evil()', unique_key_name => 'evil_key')$$);
SELECT 'hijack column exists | ' || count(*) FROM pg_attribute WHERE attrelid = 'pred.k'::regclass AND attname = 'hijack';
SELECT 'evil key catalog row exists | ' || count(*) FROM sql_saga.unique_keys WHERE unique_key_name = 'evil_key';

\o /dev/null
DROP SCHEMA pred CASCADE;
DROP OWNED BY pred_owner, pred_other;
DROP ROLE pred_other;
DROP ROLE pred_owner;
\i sql/include/test_teardown.sql
