\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/*
 * Who sql_saga's management functions act for, compared with PostgreSQL (1.4, review round 2, R1 and R2).
 *
 * The management functions change tables with the extension owner's rights, so each must first authorize the role
 * that called it exactly as the DDL it stands for would: ALTER TABLE checks the current user at the moment of the
 * call. Inside a SECURITY DEFINER function that is the function's owner, not the session user.
 *
 * Part 1  (R1) No SECURITY DEFINER function that changes a table is executable by PUBLIC without checking the
 *         caller: the internal workers are not executable by PUBLIC, and a call by a role without rights changes
 *         nothing.
 * Part 2  (R2) A SECURITY DEFINER wrapper owned by the table owner, called by another role: ALTER TABLE in the
 *         wrapper is allowed, and so is sql_saga.
 * Part 3  (R2) A SECURITY DEFINER wrapper owned by another role, called by the table owner: ALTER TABLE in the
 *         wrapper is refused, and so is sql_saga (it must not borrow the caller's ownership).
 * Part 4  SET ROLE: after SET ROLE to the owner, both are allowed; after SET ROLE away from it, both are refused.
 * Part 5  The audit: every SECURITY DEFINER function of the extension, with whether PUBLIC can execute it. A new one
 *         changes this list, so it is reviewed.
 */

BEGIN;
CREATE ROLE cw_owner;
CREATE ROLE cw_other;
CREATE SCHEMA cw AUTHORIZATION cw_owner;
GRANT USAGE ON SCHEMA cw TO cw_other;
GRANT USAGE ON SCHEMA sql_saga TO cw_owner, cw_other;

CREATE FUNCTION cw.outcome(statement text) RETURNS text LANGUAGE plpgsql AS $outcome$
BEGIN
    BEGIN
        EXECUTE statement;
        RETURN 'ok';
    EXCEPTION WHEN OTHERS THEN
        RETURN SQLSTATE || ': ' || SQLERRM;
    END;
END;
$outcome$;
GRANT EXECUTE ON FUNCTION cw.outcome(text) TO PUBLIC;

-- Everything sql_saga could change on cw's tables.
CREATE FUNCTION cw.state() RETURNS text LANGUAGE sql AS $state$
    SELECT md5(concat_ws('|',
        (SELECT string_agg(e.table_name || '.' || e.era_name, ',' ORDER BY 1) FROM sql_saga.era AS e WHERE e.table_schema = 'cw'),
        (SELECT string_agg(s.table_name, ',' ORDER BY 1) FROM sql_saga.system_time_era AS s WHERE s.table_schema = 'cw'),
        (SELECT string_agg(c.relname || '.' || t.tgname, ',' ORDER BY 1) FROM pg_trigger AS t JOIN pg_class AS c ON c.oid = t.tgrelid
          WHERE c.relnamespace = 'cw'::regnamespace AND NOT t.tgisinternal),
        (SELECT string_agg(c.relname || '.' || a.attname, ',' ORDER BY 1) FROM pg_attribute AS a JOIN pg_class AS c ON c.oid = a.attrelid
          WHERE c.relnamespace = 'cw'::regnamespace AND a.attnum > 0 AND NOT a.attisdropped),
        (SELECT string_agg(k.conname, ',' ORDER BY 1) FROM pg_constraint AS k WHERE k.connamespace = 'cw'::regnamespace)));
$state$;
GRANT EXECUTE ON FUNCTION cw.state() TO PUBLIC;

SET LOCAL ROLE cw_owner;
CREATE TABLE cw.t (id int NOT NULL, valid_range daterange NOT NULL, valid_from date, valid_until date);
CREATE TABLE cw.sv (id int NOT NULL);
SELECT sql_saga.add_era('cw.t', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_system_versioning('cw.sv');
GRANT SELECT ON ALL TABLES IN SCHEMA cw TO cw_other;
RESET ROLE;
CREATE TABLE cw.bare (id int);
ALTER TABLE cw.bare OWNER TO cw_owner;
GRANT SELECT ON cw.bare TO cw_other;

-- Part 2: owned by the table owner, called by cw_other.
CREATE FUNCTION cw.owner_wrapper_alter() RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    ALTER TABLE cw.t ADD COLUMN by_owner_wrapper int;
END $$;
CREATE FUNCTION cw.owner_wrapper_saga() RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    PERFORM sql_saga.set_era_ephemeral_columns('cw.t', 'valid', ARRAY['valid_from']);
END $$;
ALTER FUNCTION cw.owner_wrapper_alter() OWNER TO cw_owner;
ALTER FUNCTION cw.owner_wrapper_saga() OWNER TO cw_owner;
GRANT EXECUTE ON FUNCTION cw.owner_wrapper_alter(), cw.owner_wrapper_saga() TO cw_other;

-- Part 3: owned by cw_other, called by the table owner.
CREATE FUNCTION cw.other_wrapper_alter() RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    ALTER TABLE cw.t ADD COLUMN by_other_wrapper int;
END $$;
CREATE FUNCTION cw.other_wrapper_saga() RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    PERFORM sql_saga.add_era('cw.bare', 'r', 'evil', create_columns => true, range_type => 'daterange');
END $$;
ALTER FUNCTION cw.other_wrapper_alter() OWNER TO cw_other;
ALTER FUNCTION cw.other_wrapper_saga() OWNER TO cw_other;
GRANT EXECUTE ON FUNCTION cw.other_wrapper_alter(), cw.other_wrapper_saga() TO cw_owner;

\o
\echo '--- part 1: no SECURITY DEFINER worker is open to PUBLIC without a caller check'
CREATE TEMP TABLE before AS SELECT cw.state() AS s;
GRANT SELECT ON before TO PUBLIC;
SET LOCAL ROLE cw_other;
SELECT '__internal_add_system_time_era | ' || cw.outcome($$SELECT sql_saga.__internal_add_system_time_era('cw.bare')$$);
SELECT '__internal_reconcile_system_versioning_family | ' || cw.outcome($$SELECT sql_saga.__internal_reconcile_system_versioning_family('cw.sv')$$);
SELECT '__internal_sync_history_policies | ' || cw.outcome($$SELECT sql_saga.__internal_sync_history_policies('cw.sv'::regclass::oid)$$);
SELECT 'drop_system_time_era | ' || cw.outcome($$SELECT sql_saga.drop_system_time_era('cw.sv')$$);
SELECT 'rebuild_system_versioning_views | ' || cw.outcome($$SELECT sql_saga.rebuild_system_versioning_views('cw.sv')$$);
RESET ROLE;
SELECT 'nothing changed | ' || (cw.state() = (SELECT s FROM before))::text;

\echo '--- part 2: a SECURITY DEFINER wrapper owned by the table owner, called by another role'
SAVEPOINT p2;
SET LOCAL ROLE cw_other;
SELECT 'native ALTER TABLE in the wrapper | ' || cw.outcome($$SELECT cw.owner_wrapper_alter()$$);
SELECT 'sql_saga in the wrapper | ' || cw.outcome($$SELECT cw.owner_wrapper_saga()$$);
RESET ROLE;
ROLLBACK TO SAVEPOINT p2;

\echo '--- part 3: a SECURITY DEFINER wrapper owned by another role, called by the table owner'
SET LOCAL ROLE cw_owner;
SELECT 'native ALTER TABLE in the wrapper | ' || cw.outcome($$SELECT cw.other_wrapper_alter()$$);
SELECT 'sql_saga in the wrapper | ' || cw.outcome($$SELECT cw.other_wrapper_saga()$$);
RESET ROLE;
SELECT 'nothing changed | ' || (cw.state() = (SELECT s FROM before))::text;

\echo '--- part 4: SET ROLE decides, as for native DDL'
-- PostgreSQL 16+: inheritance is a property of the membership grant, taken from the role when it is granted.
CREATE ROLE cw_session NOINHERIT IN ROLE cw_owner, cw_other;
GRANT USAGE ON SCHEMA cw, sql_saga TO cw_session;
GRANT SELECT ON cw.bare TO cw_session;
SET LOCAL ROLE cw_session;
SELECT 'no SET ROLE (NOINHERIT member): native | ' || cw.outcome($$ALTER TABLE cw.bare ADD COLUMN x int$$);
SELECT 'no SET ROLE (NOINHERIT member): sql_saga | ' || cw.outcome($$SELECT sql_saga.add_era('cw.bare', 'r', 'e1', create_columns => true, range_type => 'daterange')$$);
SET LOCAL ROLE cw_owner;
SAVEPOINT p4;
SELECT 'SET ROLE owner: native | ' || cw.outcome($$ALTER TABLE cw.bare ADD COLUMN x int$$);
SELECT 'SET ROLE owner: sql_saga | ' || cw.outcome($$SELECT sql_saga.add_era('cw.bare', 'r', 'e1', create_columns => true, range_type => 'daterange')$$);
ROLLBACK TO SAVEPOINT p4;
SET LOCAL ROLE cw_other;
SELECT 'SET ROLE other: native | ' || cw.outcome($$ALTER TABLE cw.bare ADD COLUMN x int$$);
SELECT 'SET ROLE other: sql_saga | ' || cw.outcome($$SELECT sql_saga.add_era('cw.bare', 'r', 'e1', create_columns => true, range_type => 'daterange')$$);
RESET ROLE;

\echo '--- part 5: every SECURITY DEFINER function of the extension, and whether PUBLIC can execute it'
SELECT p.oid::regprocedure::text || ' | PUBLIC ' || CASE WHEN has_function_privilege('public', p.oid, 'EXECUTE') THEN 'yes' ELSE 'no' END
FROM pg_proc AS p JOIN pg_namespace AS n ON n.oid = p.pronamespace
WHERE n.nspname = 'sql_saga' AND p.prosecdef
ORDER BY p.oid::regprocedure::text COLLATE "C";

SELECT ARRAY(
    SELECT p.oid::regprocedure::text
    FROM pg_proc AS p JOIN pg_namespace AS n ON n.oid = p.pronamespace
    WHERE n.nspname = 'sql_saga' AND p.prosecdef
      AND has_function_privilege('public', p.oid, 'EXECUTE')
    ORDER BY 1
) = ARRAY[
    'sql_saga.truncate_system_versioning()',
    'sql_saga.write_history()'
] AS public_definer_set_matches;

ROLLBACK;
\i sql/include/test_teardown.sql
