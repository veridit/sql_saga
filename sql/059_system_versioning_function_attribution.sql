\set ECHO none
\set QUIET 1
\i sql/include/test_setup.sql
\o /dev/null

SET client_min_messages = warning;
SET timezone = 'UTC';
CREATE SCHEMA function_guard;
CREATE SCHEMA function_guard_moved;
SET search_path = function_guard, public;

DO $role$
BEGIN
    CREATE ROLE function_guard_owner;
EXCEPTION WHEN duplicate_object THEN
END
$role$;

CREATE TABLE family_a (id integer PRIMARY KEY, payload text);
CREATE TABLE family_b (id integer PRIMARY KEY, payload text);
SELECT sql_saga.add_system_versioning('family_a');
SELECT sql_saga.add_system_versioning('family_b');

CREATE VIEW ordinary_view AS SELECT 1 AS id;
CREATE FUNCTION ordinary_setof() RETURNS SETOF ordinary_view LANGUAGE sql STABLE AS $$ SELECT * FROM ordinary_view $$;
CREATE FUNCTION managed_setof(x timestamptz) RETURNS SETOF family_a_with_history LANGUAGE sql STABLE AS $$ SELECT * FROM family_a_with_history $$;
CREATE FUNCTION managed_setof_two(x timestamptz, y timestamptz) RETURNS SETOF family_a_with_history LANGUAGE sql STABLE AS $$ SELECT * FROM family_a_with_history $$;

CREATE FUNCTION function_guard.guc_empty() RETURNS boolean LANGUAGE sql STABLE AS $guc_empty$
SELECT coalesce(current_setting('sql_saga.alter_function_resolved', true), '') = '';
$guc_empty$;

CREATE FUNCTION function_guard.attempt(case_name text, ddl text, expected_identity text DEFAULT NULL)
RETURNS TABLE(test_case text, sqlstate text, message text, identity_preserved boolean, guc_empty boolean)
LANGUAGE plpgsql AS $attempt$
DECLARE
    caught_state text;
    caught_message text;
BEGIN
    BEGIN
        EXECUTE ddl;
        RAISE EXCEPTION USING ERRCODE = 'ZX001', MESSAGE = 'command succeeded';
    EXCEPTION
        WHEN SQLSTATE 'ZX001' THEN
            caught_state := 'NO_ERROR';
            caught_message := 'command succeeded';
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS caught_state = RETURNED_SQLSTATE, caught_message = MESSAGE_TEXT;
    END;
    RETURN QUERY SELECT case_name, caught_state, caught_message,
        expected_identity IS NULL OR to_regprocedure(expected_identity) IS NOT NULL,
        function_guard.guc_empty();
END;
$attempt$;

CREATE TEMP TABLE results (
    seq integer GENERATED ALWAYS AS IDENTITY,
    test_case text,
    sqlstate text,
    message text,
    identity_preserved boolean,
    guc_empty boolean
);

-- Exact catalogued identity changes, every role and both syntaxes.
INSERT INTO results(test_case, sqlstate, message, identity_preserved, guc_empty)
SELECT * FROM function_guard.attempt('as_of rename', 'ALTER FUNCTION function_guard.family_a__as_of(timestamptz) RENAME TO forbidden_as_of', 'function_guard.family_a__as_of(timestamptz)');
INSERT INTO results(test_case, sqlstate, message, identity_preserved, guc_empty)
SELECT * FROM function_guard.attempt('as_of set schema', 'ALTER FUNCTION function_guard.family_a__as_of(timestamptz) SET SCHEMA function_guard_moved', 'function_guard.family_a__as_of(timestamptz)');
INSERT INTO results(test_case, sqlstate, message, identity_preserved, guc_empty)
SELECT * FROM function_guard.attempt('between rename', 'ALTER ROUTINE function_guard.family_a__between(timestamptz,timestamptz) RENAME TO forbidden_between', 'function_guard.family_a__between(timestamptz,timestamptz)');
INSERT INTO results(test_case, sqlstate, message, identity_preserved, guc_empty)
SELECT * FROM function_guard.attempt('between set schema', 'ALTER ROUTINE function_guard.family_a__between(timestamptz,timestamptz) SET SCHEMA function_guard_moved', 'function_guard.family_a__between(timestamptz,timestamptz)');
INSERT INTO results(test_case, sqlstate, message, identity_preserved, guc_empty)
SELECT * FROM function_guard.attempt('between_symmetric rename', 'ALTER FUNCTION function_guard.family_a__between_symmetric(timestamptz,timestamptz) RENAME TO forbidden_symmetric', 'function_guard.family_a__between_symmetric(timestamptz,timestamptz)');
INSERT INTO results(test_case, sqlstate, message, identity_preserved, guc_empty)
SELECT * FROM function_guard.attempt('between_symmetric set schema', 'ALTER FUNCTION function_guard.family_a__between_symmetric(timestamptz,timestamptz) SET SCHEMA function_guard_moved', 'function_guard.family_a__between_symmetric(timestamptz,timestamptz)');
INSERT INTO results(test_case, sqlstate, message, identity_preserved, guc_empty)
SELECT * FROM function_guard.attempt('from_to rename', 'ALTER ROUTINE function_guard.family_a__from_to(timestamptz,timestamptz) RENAME TO forbidden_from_to', 'function_guard.family_a__from_to(timestamptz,timestamptz)');
INSERT INTO results(test_case, sqlstate, message, identity_preserved, guc_empty)
SELECT * FROM function_guard.attempt('from_to set schema', 'ALTER ROUTINE function_guard.family_a__from_to(timestamptz,timestamptz) SET SCHEMA function_guard_moved', 'function_guard.family_a__from_to(timestamptz,timestamptz)');

-- Unrelated and identity-preserving operations remain legal.
INSERT INTO results(test_case, sqlstate, message, identity_preserved, guc_empty)
SELECT * FROM function_guard.attempt('unrelated SETOF view property', 'ALTER FUNCTION function_guard.ordinary_setof() COST 101');
INSERT INTO results(test_case, sqlstate, message, identity_preserved, guc_empty)
SELECT * FROM function_guard.attempt('managed return type property', 'ALTER FUNCTION function_guard.managed_setof(timestamptz) COST 101');
INSERT INTO results(test_case, sqlstate, message, identity_preserved, guc_empty)
SELECT * FROM function_guard.attempt('managed return type rename', 'ALTER FUNCTION function_guard.managed_setof(timestamptz) RENAME TO managed_setof_renamed', 'function_guard.managed_setof(timestamptz)');
ALTER FUNCTION managed_setof(timestamptz) RENAME TO managed_setof_renamed;
ALTER FUNCTION managed_setof_renamed(timestamptz) RENAME TO managed_setof;
INSERT INTO results(test_case, sqlstate, message, identity_preserved, guc_empty)
SELECT * FROM function_guard.attempt('as_of owner', 'ALTER FUNCTION function_guard.family_a__as_of(timestamptz) OWNER TO function_guard_owner', 'function_guard.family_a__as_of(timestamptz)');
INSERT INTO results(test_case, sqlstate, message, identity_preserved, guc_empty)
SELECT * FROM function_guard.attempt('as_of property', 'ALTER ROUTINE function_guard.family_a__as_of(timestamptz) COST 101', 'function_guard.family_a__as_of(timestamptz)');
INSERT INTO results(test_case, sqlstate, message, identity_preserved, guc_empty)
SELECT * FROM function_guard.attempt('between owner', 'ALTER FUNCTION function_guard.family_a__between(timestamptz,timestamptz) OWNER TO function_guard_owner', 'function_guard.family_a__between(timestamptz,timestamptz)');
INSERT INTO results(test_case, sqlstate, message, identity_preserved, guc_empty)
SELECT * FROM function_guard.attempt('between property', 'ALTER ROUTINE function_guard.family_a__between(timestamptz,timestamptz) COST 101', 'function_guard.family_a__between(timestamptz,timestamptz)');
INSERT INTO results(test_case, sqlstate, message, identity_preserved, guc_empty)
SELECT * FROM function_guard.attempt('between_symmetric owner', 'ALTER FUNCTION function_guard.family_a__between_symmetric(timestamptz,timestamptz) OWNER TO function_guard_owner', 'function_guard.family_a__between_symmetric(timestamptz,timestamptz)');
INSERT INTO results(test_case, sqlstate, message, identity_preserved, guc_empty)
SELECT * FROM function_guard.attempt('between_symmetric property', 'ALTER ROUTINE function_guard.family_a__between_symmetric(timestamptz,timestamptz) COST 101', 'function_guard.family_a__between_symmetric(timestamptz,timestamptz)');
INSERT INTO results(test_case, sqlstate, message, identity_preserved, guc_empty)
SELECT * FROM function_guard.attempt('from_to owner', 'ALTER FUNCTION function_guard.family_a__from_to(timestamptz,timestamptz) OWNER TO function_guard_owner', 'function_guard.family_a__from_to(timestamptz,timestamptz)');
INSERT INTO results(test_case, sqlstate, message, identity_preserved, guc_empty)
SELECT * FROM function_guard.attempt('from_to property', 'ALTER ROUTINE function_guard.family_a__from_to(timestamptz,timestamptz) COST 101', 'function_guard.family_a__from_to(timestamptz,timestamptz)');
DROP FUNCTION managed_setof(timestamptz);
DROP FUNCTION managed_setof_two(timestamptz,timestamptz);

-- Stale A plus unrelated B, and the four matching-signature counterexamples.
CREATE TEMP TABLE counterexamples(
    role text,
    operation text,
    allowed boolean,
    guc_empty boolean,
    missing_identity_unresolved boolean
);
DO $counterexamples$
DECLARE
    r record;
    user_name text;
    user_args text;
    user_decl text;
    stored_identity text;
    ok boolean;
BEGIN
    FOR r IN
        SELECT x.role, x.identity,
               CASE WHEN x.role = 'as_of' THEN 'timestamptz' ELSE 'timestamptz,timestamptz' END AS args,
               CASE WHEN x.role = 'as_of' THEN 'x timestamptz' ELSE 'x timestamptz, y timestamptz' END AS argdecl
        FROM sql_saga.system_versioning AS sv
        CROSS JOIN LATERAL (VALUES
            ('as_of', sv.func_as_of), ('between', sv.func_between),
            ('between_symmetric', sv.func_between_symmetric), ('from_to', sv.func_from_to)
        ) AS x(role, identity)
        WHERE sv.table_schema = 'function_guard' AND sv.table_name = 'family_a'
    LOOP
        user_name := 'user_' || r.role;
        user_args := r.args;
        user_decl := r.argdecl;
        stored_identity := r.identity::text;
        PERFORM set_config('session_replication_role', 'replica', true);
        EXECUTE format('DROP FUNCTION %s', r.identity);
        PERFORM set_config('session_replication_role', 'origin', true);
        EXECUTE format('CREATE FUNCTION function_guard.%I(%s) RETURNS SETOF function_guard.family_a_with_history LANGUAGE sql STABLE AS $body$ SELECT * FROM function_guard.family_a_with_history $body$', user_name, user_decl);

            BEGIN EXECUTE format('ALTER FUNCTION function_guard.%I(%s) RENAME TO %I', user_name, user_args, user_name || '_renamed'); ok := true;
            EXCEPTION WHEN OTHERS THEN ok := false; END;
            INSERT INTO counterexamples VALUES (r.role, 'rename', ok, function_guard.guc_empty(), to_regprocedure(stored_identity) IS NULL);
            IF to_regprocedure(format('function_guard.%I(%s)', user_name || '_renamed', user_args)) IS NOT NULL THEN
                EXECUTE format('ALTER FUNCTION function_guard.%I(%s) RENAME TO %I', user_name || '_renamed', user_args, user_name);
            END IF;

            BEGIN EXECUTE format('ALTER FUNCTION function_guard.%I(%s) SET SCHEMA function_guard_moved', user_name, user_args); ok := true;
            EXCEPTION WHEN OTHERS THEN ok := false; END;
            INSERT INTO counterexamples VALUES (r.role, 'set schema', ok, function_guard.guc_empty(), to_regprocedure(stored_identity) IS NULL);
            IF to_regprocedure(format('function_guard_moved.%I(%s)', user_name, user_args)) IS NOT NULL THEN
                EXECUTE format('ALTER FUNCTION function_guard_moved.%I(%s) SET SCHEMA function_guard', user_name, user_args);
            END IF;

            BEGIN EXECUTE format('ALTER FUNCTION function_guard.%I(%s) OWNER TO function_guard_owner', user_name, user_args); ok := true;
            EXCEPTION WHEN OTHERS THEN ok := false; END;
            INSERT INTO counterexamples VALUES (r.role, 'owner', ok, function_guard.guc_empty(), to_regprocedure(stored_identity) IS NULL);

            BEGIN EXECUTE format('ALTER FUNCTION function_guard.%I(%s) COST 111', user_name, user_args); ok := true;
            EXCEPTION WHEN OTHERS THEN ok := false; END;
            INSERT INTO counterexamples VALUES (r.role, 'property', ok, function_guard.guc_empty(), to_regprocedure(stored_identity) IS NULL);

            -- A is stale while an unrelated function in B is altered.
            BEGIN EXECUTE 'ALTER FUNCTION function_guard.family_b__as_of(timestamptz) COST 102'; ok := true;
            EXCEPTION WHEN OTHERS THEN ok := false; END;
        INSERT INTO counterexamples VALUES (r.role, 'stale A unrelated B', ok, function_guard.guc_empty(), to_regprocedure(stored_identity) IS NULL);

        IF to_regprocedure(format('function_guard.%I(%s)', user_name, user_args)) IS NOT NULL THEN
            EXECUTE format('DROP FUNCTION function_guard.%I(%s)', user_name, user_args);
        ELSIF to_regprocedure(format('function_guard.%I(%s)', user_name || '_renamed', user_args)) IS NOT NULL THEN
            EXECUTE format('DROP FUNCTION function_guard.%I(%s)', user_name || '_renamed', user_args);
        ELSIF to_regprocedure(format('function_guard_moved.%I(%s)', user_name, user_args)) IS NOT NULL THEN
            EXECUTE format('DROP FUNCTION function_guard_moved.%I(%s)', user_name, user_args);
        END IF;
        PERFORM sql_saga.rebuild_system_versioning_views('function_guard.family_a');
    END LOOP;
END
$counterexamples$;

-- Several roles missing before one unrelated statement must not be blamed.
DO $multiple_missing$
DECLARE
    ok boolean;
    as_of_identity text;
    between_identity text;
BEGIN
    SELECT sv.func_as_of::text, sv.func_between::text
    INTO as_of_identity, between_identity
    FROM sql_saga.system_versioning AS sv
    WHERE sv.table_schema = 'function_guard' AND sv.table_name = 'family_a';
    PERFORM set_config('session_replication_role', 'replica', true);
    DROP FUNCTION family_a__as_of(timestamptz);
    DROP FUNCTION family_a__between(timestamptz,timestamptz);
    PERFORM set_config('session_replication_role', 'origin', true);
    BEGIN ALTER FUNCTION ordinary_setof() COST 103; ok := true;
    EXCEPTION WHEN OTHERS THEN ok := false; END;
    INSERT INTO counterexamples VALUES (
        'multiple', 'unrelated property', ok, function_guard.guc_empty(),
        to_regprocedure(as_of_identity) IS NULL AND to_regprocedure(between_identity) IS NULL
    );
    PERFORM sql_saga.rebuild_system_versioning_views('function_guard.family_a');
END
$multiple_missing$;

-- Sequential commands, savepoint atomicity, and nested utility statements.
BEGIN;
ALTER FUNCTION family_a__as_of(timestamptz) COST 104;
SELECT function_guard.guc_empty() AS sequential_1_guc_empty \gset
ALTER ROUTINE family_a__between(timestamptz,timestamptz) COST 105;
SELECT function_guard.guc_empty() AS sequential_2_guc_empty \gset
ALTER FUNCTION family_a__between_symmetric(timestamptz,timestamptz) COST 106;
SELECT function_guard.guc_empty() AS sequential_3_guc_empty \gset
ALTER ROUTINE family_a__from_to(timestamptz,timestamptz) COST 107;
SELECT function_guard.guc_empty() AS sequential_4_guc_empty \gset
ROLLBACK;

BEGIN;
SAVEPOINT before_refusal;
\set VERBOSITY terse
\set ON_ERROR_STOP off
ALTER FUNCTION family_a__as_of(timestamptz) RENAME TO savepoint_bad;
\set ON_ERROR_STOP on
\set VERBOSITY default
ROLLBACK TO SAVEPOINT before_refusal;
SELECT to_regprocedure('function_guard.family_a__as_of(timestamptz)') IS NOT NULL AS savepoint_identity,
       function_guard.guc_empty() AS savepoint_guc_empty \gset
COMMIT;

DO $nested$ BEGIN EXECUTE 'ALTER FUNCTION function_guard.family_a__as_of(timestamptz) COST 106'; END $nested$;
SELECT function_guard.guc_empty() AS nested_do_guc_empty \gset
CREATE FUNCTION nested_alter() RETURNS void LANGUAGE plpgsql AS $nested_alter$
BEGIN EXECUTE 'ALTER ROUTINE function_guard.family_a__between(timestamptz,timestamptz) COST 107'; END;
$nested_alter$;
SELECT nested_alter();
SELECT function_guard.guc_empty() AS nested_wrapper_guc_empty \gset
ALTER FUNCTION family_a__as_of(timestamptz) COST 100;
ALTER FUNCTION family_a__between(timestamptz,timestamptz) COST 100;

-- No start snapshot: dynamically disable the future recording trigger while end health checks remain active.
DO $disable_start$
DECLARE trigger_name name;
BEGIN
    SELECT et.evtname INTO trigger_name
    FROM pg_event_trigger AS et
    JOIN pg_proc AS p ON p.oid = et.evtfoid
    JOIN pg_namespace AS n ON n.oid = p.pronamespace
    WHERE et.evtevent = 'ddl_command_start'
      AND n.nspname = 'sql_saga'
      AND pg_get_functiondef(p.oid) LIKE '%alter_function_resolved%'
    ORDER BY et.evtname LIMIT 1;
    IF trigger_name IS NOT NULL THEN EXECUTE format('ALTER EVENT TRIGGER %I DISABLE', trigger_name); END IF;
END
$disable_start$;
INSERT INTO results(test_case, sqlstate, message, identity_preserved, guc_empty)
SELECT * FROM function_guard.attempt('absent snapshot property', 'ALTER FUNCTION function_guard.family_a__as_of(timestamptz) COST 108', 'function_guard.family_a__as_of(timestamptz)');
INSERT INTO results(test_case, sqlstate, message, identity_preserved, guc_empty)
SELECT * FROM function_guard.attempt('absent snapshot rename', 'ALTER FUNCTION function_guard.family_a__as_of(timestamptz) RENAME TO absent_snapshot_name', 'function_guard.family_a__as_of(timestamptz)');

DO $enable_start$
DECLARE trigger_name name;
BEGIN
    SELECT et.evtname INTO trigger_name
    FROM pg_event_trigger AS et
    JOIN pg_proc AS p ON p.oid = et.evtfoid
    JOIN pg_namespace AS n ON n.oid = p.pronamespace
    WHERE et.evtevent = 'ddl_command_start'
      AND n.nspname = 'sql_saga'
      AND pg_get_functiondef(p.oid) LIKE '%alter_function_resolved%'
    ORDER BY et.evtname LIMIT 1;
    IF trigger_name IS NOT NULL THEN EXECUTE format('ALTER EVENT TRIGGER %I ENABLE', trigger_name); END IF;
END
$enable_start$;

CREATE FUNCTION function_guard.plan_checks()
RETURNS TABLE(no_function_scan boolean, base_predicate_pushed boolean, history_predicate_pushed boolean)
LANGUAGE plpgsql AS $plan_checks$
DECLARE
    plan_text text := '';
    line record;
BEGIN
    FOR line IN EXECUTE $$EXPLAIN (COSTS OFF) SELECT * FROM function_guard.family_a__as_of(TIMESTAMPTZ '2026-01-01 00:00:00+00') WHERE id = 42$$
    LOOP
        plan_text := plan_text || E'\n' || line."QUERY PLAN";
    END LOOP;
    RETURN QUERY SELECT plan_text !~ 'Function Scan',
        plan_text ~ 'Index Cond: \(id = 42\)',
        plan_text ~ 'system_valid_range.*id = 42';
END;
$plan_checks$;

\o
\pset format unaligned
\pset tuples_only on
SELECT seq, test_case, sqlstate, message, identity_preserved, guc_empty FROM results ORDER BY seq;
SELECT role, operation, allowed, guc_empty, missing_identity_unresolved FROM counterexamples ORDER BY role, operation;
SELECT :'sequential_1_guc_empty' AS sequential_1_guc_empty,
       :'sequential_2_guc_empty' AS sequential_2_guc_empty,
       :'sequential_3_guc_empty' AS sequential_3_guc_empty,
       :'sequential_4_guc_empty' AS sequential_4_guc_empty,
       :'savepoint_identity' AS savepoint_identity,
       :'savepoint_guc_empty' AS savepoint_guc_empty,
       :'nested_do_guc_empty' AS nested_do_guc_empty,
       :'nested_wrapper_guc_empty' AS nested_wrapper_guc_empty;
SELECT * FROM function_guard.plan_checks();

\o /dev/null
DROP SCHEMA function_guard_moved CASCADE;
DROP SCHEMA function_guard CASCADE;
DROP OWNED BY function_guard_owner;
DROP ROLE function_guard_owner;
\i sql/include/test_teardown.sql
