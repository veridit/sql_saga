\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
SET client_min_messages = warning;
\o /dev/null

BEGIN;
CREATE SCHEMA hpol;
CREATE SCHEMA "Hpol Q";
SET search_path = hpol, public;

-- check_integrity outcome: message and hint, and whether the call changed any policy.
CREATE FUNCTION hpol.check(case_name text) RETURNS text LANGUAGE plpgsql AS $check$
DECLARE before_state text; msg text; hint text;
BEGIN
    before_state := hpol.policies();
    BEGIN
        PERFORM sql_saga.check_integrity();
        msg := 'ok';
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS msg = MESSAGE_TEXT, hint = PG_EXCEPTION_HINT;
        msg := format(E'%s\n  HINT: %s', msg, nullif(hint, ''));
    END;
    IF before_state IS DISTINCT FROM hpol.policies() THEN
        RETURN case_name || ' | checker changed policies';
    END IF;
    RETURN case_name || ' | ' || msg;
END;
$check$;

-- Run one statement, reporting success or the error (message and hint).
CREATE FUNCTION hpol.run(case_name text, stmt text) RETURNS text LANGUAGE plpgsql AS $run$
DECLARE msg text; hint text;
BEGIN
    EXECUTE stmt;
    RETURN case_name || ' | ok';
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS msg = MESSAGE_TEXT, hint = PG_EXCEPTION_HINT;
    RETURN format(E'%s | %s\n  HINT: %s', case_name, msg, nullif(hint, ''));
END;
$run$;

CREATE TABLE hpol.hp (id integer PRIMARY KEY, tenant text);
ALTER TABLE hpol.hp ENABLE ROW LEVEL SECURITY;
SELECT sql_saga.add_system_versioning('hpol.hp');
CREATE TABLE "Hpol Q"."Q Tab" (id integer PRIMARY KEY, tenant text);
ALTER TABLE "Hpol Q"."Q Tab" ENABLE ROW LEVEL SECURITY;
SELECT sql_saga.add_system_versioning('"Hpol Q"."Q Tab"');
-- Snapshot of every user (non-managed) policy on the base and history tables (definition included).
CREATE FUNCTION hpol.policies() RETURNS text LANGUAGE sql AS $policies$
    SELECT coalesce(string_agg(format('%s:%s:%s', p.polrelid::regclass, p.polname,
                                      pg_catalog.pg_get_expr(p.polqual, p.polrelid)), ', '
                               ORDER BY p.polrelid::regclass::text, p.polname), '<none>')
    FROM pg_catalog.pg_policy AS p
    WHERE p.polrelid IN ('hpol.hp'::regclass, 'hpol.hp_history'::regclass,
                         '"Hpol Q"."Q Tab"'::regclass, '"Hpol Q"."Q Tab_history"'::regclass)
      AND NOT sql_saga.__internal_is_managed_history_policy(p.polname);
$policies$;

\o

SELECT hpol.check('healthy baseline');

-- A user policy on the history table, created with event triggers bypassed (as a 1.2 install could carry).
SET session_replication_role = replica;
CREATE POLICY user_history_policy ON hpol.hp_history USING (tenant = current_user);
RESET session_replication_role;

SELECT hpol.check('plain unmanaged history policy');
SELECT hpol.policies();
-- The generic sync suggestion alone cannot repair this: it refuses to touch the user's policy.
SELECT hpol.run('plain sync alone', $$SELECT sql_saga.sync_history_policies('hpol.hp')$$);
SELECT hpol.policies();
-- The advertised manual repair: drop from history, the user decides and creates the base policy, then sync.
-- (Creating on the base first is refused while the user policy still sits on the history.)
SELECT hpol.run('plain drop from history', $$DROP POLICY user_history_policy ON hpol.hp_history$$);
SELECT hpol.run('plain create on base', $$CREATE POLICY user_history_policy ON hpol.hp USING (tenant = current_user)$$);
SELECT hpol.run('plain sync', $$SELECT sql_saga.sync_history_policies('hpol.hp')$$);
SELECT hpol.check('plain after repair');
SELECT hpol.policies();

SET session_replication_role = replica;
CREATE POLICY "User ""Q"" Policy" ON "Hpol Q"."Q Tab_history" USING (true);
RESET session_replication_role;

SELECT hpol.check('quoted unmanaged history policy');
SELECT hpol.run('quoted sync alone', $$SELECT sql_saga.sync_history_policies('"Hpol Q"."Q Tab"')$$);
SELECT hpol.policies();
-- Here the user decides the policy is not needed on the base: drop from history, then sync.
SELECT hpol.run('quoted drop from history', $$DROP POLICY "User ""Q"" Policy" ON "Hpol Q"."Q Tab_history"$$);
SELECT hpol.run('quoted sync', $$SELECT sql_saga.sync_history_policies('"Hpol Q"."Q Tab"')$$);
SELECT hpol.check('quoted after repair');
SELECT hpol.policies();

ROLLBACK;

\o /dev/null
\i sql/include/test_teardown.sql
\o
