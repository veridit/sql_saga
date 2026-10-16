\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
SET client_min_messages = warning;
\o /dev/null

BEGIN;
CREATE SCHEMA integrity_test;
SET search_path = integrity_test, public;

CREATE ROLE integrity_untrusted;
CREATE ROLE integrity_granted;
GRANT USAGE ON SCHEMA sql_saga TO integrity_untrusted, integrity_granted;
GRANT USAGE ON SCHEMA integrity_test TO integrity_untrusted, integrity_granted;

CREATE FUNCTION integrity_test.attempt(case_name text, expected_state text, expected_message text)
RETURNS text LANGUAGE plpgsql AS $attempt$
DECLARE
    actual_state text;
    actual_message text;
BEGIN
    BEGIN
        EXECUTE 'SELECT sql_saga.check_integrity()';
        RETURN format('%s | unexpected success', case_name);
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS actual_state = RETURNED_SQLSTATE, actual_message = MESSAGE_TEXT;
        IF (actual_state, actual_message) IS DISTINCT FROM (expected_state, expected_message) THEN
            RETURN format('%s | unexpected %s: %s', case_name, actual_state, actual_message);
        END IF;
        RETURN format('%s | %s: %s', case_name, actual_state, actual_message);
    END;
END;
$attempt$;

CREATE FUNCTION integrity_test.valid(case_name text)
RETURNS text LANGUAGE plpgsql AS $valid$
BEGIN
    BEGIN
        EXECUTE 'SELECT sql_saga.check_integrity()';
        RETURN case_name || ' | ok';
    EXCEPTION WHEN OTHERS THEN
        RETURN format('%s | unexpected %s: %s', case_name, SQLSTATE, SQLERRM);
    END;
END;
$valid$;

CREATE FUNCTION integrity_test.unrelated_ddl(case_name text)
RETURNS text LANGUAGE plpgsql AS $unrelated_ddl$
BEGIN
    EXECUTE format('CREATE TABLE integrity_test.%I (id integer)', 'unrelated_' || case_name);
    EXECUTE format('DROP TABLE integrity_test.%I', 'unrelated_' || case_name);
    RETURN case_name || ' | unrelated DDL ok';
END;
$unrelated_ddl$;

CREATE TEMP TABLE integrity_snapshots (case_name text PRIMARY KEY, snapshot jsonb) ON COMMIT DROP;
CREATE FUNCTION integrity_test.snapshot_state()
RETURNS jsonb LANGUAGE sql STABLE AS $snapshot_state$
SELECT jsonb_build_object(
    'era', (SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.table_schema COLLATE "C", x.table_name COLLATE "C", x.era_name COLLATE "C"), '[]') FROM sql_saga.era AS x WHERE x.table_schema='integrity_test'),
    'system_time', (SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.table_schema COLLATE "C", x.table_name COLLATE "C"), '[]') FROM sql_saga.system_time_era AS x WHERE x.table_schema IN ('integrity_test', 'integrity_moved')),
    'system_versioning', (SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.table_schema COLLATE "C", x.table_name COLLATE "C"), '[]') FROM sql_saga.system_versioning AS x WHERE x.table_schema IN ('integrity_test', 'integrity_moved')),
    'unique_keys', (SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.table_schema COLLATE "C", x.table_name COLLATE "C", x.unique_key_name COLLATE "C"), '[]') FROM sql_saga.unique_keys AS x WHERE x.table_schema='integrity_test'),
    'foreign_keys', (SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.table_schema COLLATE "C", x.table_name COLLATE "C"), '[]') FROM sql_saga.foreign_keys AS x WHERE x.table_schema='integrity_test'),
    'updatable_view', (SELECT coalesce(jsonb_agg(to_jsonb(x) ORDER BY x.view_schema COLLATE "C", x.view_name COLLATE "C"), '[]') FROM sql_saga.updatable_view AS x WHERE x.table_schema='integrity_test'),
    'relations', (SELECT coalesce(jsonb_agg(jsonb_build_array(n.nspname, c.relname, c.relkind) ORDER BY n.nspname COLLATE "C", c.relname COLLATE "C"), '[]') FROM pg_class AS c JOIN pg_namespace AS n ON n.oid=c.relnamespace WHERE n.nspname IN ('integrity_test', 'integrity_moved')),
    'triggers', (SELECT coalesce(jsonb_agg(jsonb_build_array(n.nspname, c.relname, t.tgname, t.tgenabled) ORDER BY n.nspname COLLATE "C", c.relname COLLATE "C", t.tgname COLLATE "C"), '[]') FROM pg_trigger AS t JOIN pg_class AS c ON c.oid=t.tgrelid JOIN pg_namespace AS n ON n.oid=c.relnamespace WHERE n.nspname IN ('integrity_test', 'integrity_moved') AND NOT t.tgisinternal),
    'functions', (SELECT coalesce(jsonb_agg(jsonb_build_array(n.nspname, p.proname, pg_get_function_identity_arguments(p.oid), p.prosrc, p.prorettype::regtype::text, p.provolatile, p.prosecdef, p.proleakproof, p.proisstrict, p.proparallel) ORDER BY n.nspname COLLATE "C", p.proname COLLATE "C", pg_get_function_identity_arguments(p.oid) COLLATE "C"), '[]') FROM pg_proc AS p JOIN pg_namespace AS n ON n.oid=p.pronamespace WHERE n.nspname IN ('integrity_test', 'integrity_moved')),
    'policies', (SELECT coalesce(jsonb_agg(jsonb_build_array(n.nspname, c.relname, p.polname, pg_get_expr(p.polqual, p.polrelid), pg_get_expr(p.polwithcheck, p.polrelid)) ORDER BY n.nspname COLLATE "C", c.relname COLLATE "C", p.polname COLLATE "C"), '[]') FROM pg_policy AS p JOIN pg_class AS c ON c.oid=p.polrelid JOIN pg_namespace AS n ON n.oid=c.relnamespace WHERE n.nspname IN ('integrity_test', 'integrity_moved'))
)
$snapshot_state$;
CREATE FUNCTION integrity_test.save_snapshot(case_name text) RETURNS void LANGUAGE sql AS $save_snapshot$
INSERT INTO integrity_snapshots VALUES (case_name, integrity_test.snapshot_state())
$save_snapshot$;
CREATE FUNCTION integrity_test.snapshot_unchanged(case_name text) RETURNS boolean LANGUAGE sql STABLE AS $snapshot_unchanged$
SELECT s.snapshot = integrity_test.snapshot_state() FROM integrity_snapshots AS s WHERE s.case_name=snapshot_unchanged.case_name
$snapshot_unchanged$;

CREATE FUNCTION integrity_test.managed_leftover_count(family text)
RETURNS integer LANGUAGE plpgsql STABLE AS $managed_leftover_count$
DECLARE n integer;
BEGIN
    IF family = 'period_parent_predicated' THEN
        SELECT count(*)::integer INTO n FROM (
            SELECT 1 FROM sql_saga.era AS x WHERE x.table_schema='integrity_test' AND x.table_name=family
            UNION ALL SELECT 1 FROM sql_saga.system_time_era AS x WHERE x.table_schema='integrity_test' AND x.table_name=family
            UNION ALL SELECT 1 FROM sql_saga.system_versioning AS x WHERE x.table_schema='integrity_test' AND x.table_name=family
            UNION ALL SELECT 1 FROM sql_saga.unique_keys WHERE unique_key_name='period_parent_predicated'
            UNION ALL SELECT 1 FROM sql_saga.foreign_keys WHERE unique_key_name='period_parent_predicated'
            UNION ALL SELECT 1 FROM sql_saga.updatable_view AS x WHERE x.table_schema='integrity_test' AND (x.table_name=family OR x.view_name=family)
            UNION ALL SELECT 1 FROM pg_constraint WHERE conrelid='integrity_test.period_parent'::regclass AND conname LIKE 'period_parent_predicated%'
            UNION ALL SELECT 1 FROM pg_class AS c JOIN pg_namespace AS ns ON ns.oid=c.relnamespace WHERE ns.nspname='integrity_test' AND c.relname LIKE 'period_parent_predicated%'
            UNION ALL SELECT 1 FROM pg_trigger AS t JOIN pg_class AS c ON c.oid=t.tgrelid JOIN pg_namespace AS ns ON ns.oid=c.relnamespace WHERE ns.nspname='integrity_test' AND t.tgname LIKE 'period_parent_predicated%'
            UNION ALL SELECT 1 FROM pg_proc AS p JOIN pg_namespace AS ns ON ns.oid=p.pronamespace WHERE ns.nspname='integrity_test' AND p.proname LIKE 'period_parent_predicated%'
            UNION ALL SELECT 1 FROM pg_policy AS p JOIN pg_class AS c ON c.oid=p.polrelid JOIN pg_namespace AS ns ON ns.oid=c.relnamespace WHERE ns.nspname='integrity_test' AND c.relname LIKE 'period_parent_predicated%'
        ) AS x;
    ELSIF family = 'period_parent__for_portion_of_valid' THEN
        SELECT count(*)::integer INTO n FROM (
            SELECT 1 FROM sql_saga.era AS x WHERE x.table_schema='integrity_test' AND x.table_name=family
            UNION ALL SELECT 1 FROM sql_saga.system_time_era AS x WHERE x.table_schema='integrity_test' AND x.table_name=family
            UNION ALL SELECT 1 FROM sql_saga.system_versioning AS x WHERE x.table_schema='integrity_test' AND x.table_name=family
            UNION ALL SELECT 1 FROM sql_saga.unique_keys AS x WHERE x.table_schema='integrity_test' AND x.table_name=family
            UNION ALL SELECT 1 FROM sql_saga.foreign_keys AS x WHERE x.table_schema='integrity_test' AND x.table_name=family
            UNION ALL SELECT 1 FROM sql_saga.updatable_view WHERE view_schema='integrity_test' AND view_name='period_parent__for_portion_of_valid'
            UNION ALL SELECT 1 FROM pg_constraint AS c JOIN pg_class AS r ON r.oid=c.conrelid WHERE r.oid=to_regclass('integrity_test.period_parent__for_portion_of_valid')
            UNION ALL SELECT 1 FROM pg_class AS c JOIN pg_namespace AS ns ON ns.oid=c.relnamespace WHERE ns.nspname='integrity_test' AND c.relname='period_parent__for_portion_of_valid'
            UNION ALL SELECT 1 FROM pg_trigger AS t JOIN pg_class AS c ON c.oid=t.tgrelid WHERE c.oid=to_regclass('integrity_test.period_parent__for_portion_of_valid') AND NOT t.tgisinternal
            UNION ALL SELECT 1 FROM pg_proc AS p JOIN pg_namespace AS ns ON ns.oid=p.pronamespace WHERE ns.nspname='integrity_test' AND p.proname LIKE 'period_parent__for_portion_of_valid%'
            UNION ALL SELECT 1 FROM pg_policy AS p JOIN pg_class AS c ON c.oid=p.polrelid WHERE c.oid=to_regclass('integrity_test.period_parent__for_portion_of_valid')
        ) AS x;
    ELSIF family IN ('sv_trigger', 'sv_excluded') THEN
        SELECT count(*)::integer INTO n FROM (
            SELECT 1 FROM sql_saga.era AS x WHERE x.table_schema='integrity_test' AND x.table_name=family AND x.era_name='system_time'
            UNION ALL SELECT 1 FROM sql_saga.system_time_era AS x WHERE x.table_schema='integrity_test' AND x.table_name=family
            UNION ALL SELECT 1 FROM sql_saga.system_versioning AS x WHERE x.table_schema='integrity_test' AND x.table_name=family
            UNION ALL SELECT 1 FROM pg_attribute AS a
                WHERE a.attrelid=format('integrity_test.%I', family)::regclass AND a.attnum > 0 AND NOT a.attisdropped
                  AND a.attname <> ALL(CASE family WHEN 'sv_excluded' THEN ARRAY['id','tenant','payload']::name[] ELSE ARRAY['id','payload']::name[] END)
            UNION ALL SELECT 1 FROM pg_constraint AS c
                WHERE c.conrelid=format('integrity_test.%I', family)::regclass
                  AND c.conname <> ALL(ARRAY[family || '_id_not_null', family || '_pkey']::name[])
            UNION ALL SELECT 1 FROM pg_class AS c JOIN pg_index AS i ON i.indexrelid=c.oid
                WHERE i.indrelid=format('integrity_test.%I', family)::regclass AND c.relname <> family || '_pkey'
            UNION ALL SELECT 1 FROM pg_trigger AS t
                WHERE t.tgrelid=format('integrity_test.%I', family)::regclass AND NOT t.tgisinternal
            UNION ALL SELECT 1 FROM pg_proc AS p JOIN pg_namespace AS ns ON ns.oid=p.pronamespace
                WHERE ns.nspname='sql_saga' AND p.proname LIKE family || '\_%system_time%' ESCAPE '\'
            UNION ALL SELECT 1 WHERE to_regclass(format('integrity_test.%I_history', family)) IS NOT NULL
            UNION ALL SELECT 1 WHERE to_regclass(format('integrity_test.%I_with_history', family)) IS NOT NULL
            UNION ALL SELECT 1 WHERE to_regclass(format('integrity_test.%I_history_gated', family)) IS NOT NULL
            UNION ALL SELECT 1 WHERE to_regprocedure(format('integrity_test.%I__as_of(timestamptz)', family)) IS NOT NULL
            UNION ALL SELECT 1 WHERE to_regprocedure(format('integrity_test.%I__between(timestamptz,timestamptz)', family)) IS NOT NULL
            UNION ALL SELECT 1 WHERE to_regprocedure(format('integrity_test.%I__between_symmetric(timestamptz,timestamptz)', family)) IS NOT NULL
            UNION ALL SELECT 1 WHERE to_regprocedure(format('integrity_test.%I__from_to(timestamptz,timestamptz)', family)) IS NOT NULL
        ) AS x;
    ELSE
        SELECT count(*)::integer INTO n FROM (
            SELECT 1 FROM sql_saga.era AS x WHERE x.table_schema='integrity_test' AND x.table_name=family
            UNION ALL SELECT 1 FROM sql_saga.system_time_era AS x WHERE x.table_schema='integrity_test' AND x.table_name=family
            UNION ALL SELECT 1 FROM sql_saga.system_versioning AS x WHERE x.table_schema='integrity_test' AND x.table_name=family
            UNION ALL SELECT 1 FROM sql_saga.unique_keys AS x WHERE x.table_schema='integrity_test' AND x.table_name=family
            UNION ALL SELECT 1 FROM sql_saga.foreign_keys AS x WHERE x.table_schema='integrity_test' AND x.table_name=family
            UNION ALL SELECT 1 FROM sql_saga.updatable_view AS x WHERE x.table_schema='integrity_test' AND x.table_name=family
            UNION ALL SELECT 1 FROM pg_trigger AS t JOIN pg_class AS c ON c.oid=t.tgrelid JOIN pg_namespace AS ns ON ns.oid=c.relnamespace WHERE ns.nspname='integrity_test' AND NOT t.tgisinternal AND (c.relname=family OR c.relname LIKE family || '\_%' ESCAPE '\')
            UNION ALL SELECT 1 FROM pg_constraint AS c JOIN pg_class AS r ON r.oid=c.conrelid JOIN pg_namespace AS ns ON ns.oid=r.relnamespace WHERE ns.nspname='integrity_test' AND (r.relname=family OR c.conname LIKE family || '%')
            UNION ALL SELECT 1 FROM pg_class AS c JOIN pg_namespace AS ns ON ns.oid=c.relnamespace WHERE ns.nspname='integrity_test' AND c.relname LIKE family || '\_%' ESCAPE '\'
            UNION ALL SELECT 1 FROM pg_proc AS p JOIN pg_namespace AS ns ON ns.oid=p.pronamespace WHERE ns.nspname='integrity_test' AND p.proname LIKE family || '\_\_%' ESCAPE '\'
            UNION ALL SELECT 1 FROM pg_policy AS p JOIN pg_class AS c ON c.oid=p.polrelid JOIN pg_namespace AS ns ON ns.oid=c.relnamespace WHERE ns.nspname='integrity_test' AND (c.relname=family OR c.relname LIKE family || '\_%' ESCAPE '\')
        ) AS x;
    END IF;
    RETURN n;
END;
$managed_leftover_count$;

CREATE FUNCTION integrity_test.repair_attempt(case_name text, repair_kind text, finish_repair boolean DEFAULT true, verify_integrity boolean DEFAULT true)
RETURNS text LANGUAGE plpgsql AS $repair_attempt$
DECLARE
    drop_result boolean;
    family_name text := CASE repair_kind WHEN 'unique' THEN 'period_parent_predicated' WHEN 'for_portion_of' THEN 'period_parent__for_portion_of_valid' ELSE repair_kind END;
    managed_trigger_names name[];
    managed_constraint_names name[];
    managed_column_names name[];
    sync_trigger_name name;
    sync_function_name name;
    leftover_names text[];
BEGIN
    BEGIN
        IF integrity_test.managed_leftover_count(family_name) = 0 THEN
            RAISE EXCEPTION 'orphan inventory was vacuous before % drop', repair_kind;
        END IF;
        CASE repair_kind
        WHEN 'sv_trigger' THEN
            SELECT ARRAY[ste.generated_always_trigger, ste.write_history_trigger, ste.truncate_trigger],
                   ARRAY[ste.infinity_check_constraint, e.bounds_check_constraint, e.boundary_check_constraint],
                   array_remove(ARRAY[e.range_column_name, e.valid_from_column_name, e.valid_until_column_name, e.valid_to_column_name], NULL),
                   e.sync_temporal_trg_name,
                   e.sync_temporal_trg_function_name
            INTO managed_trigger_names, managed_constraint_names, managed_column_names, sync_trigger_name, sync_function_name
            FROM sql_saga.system_time_era AS ste
            JOIN sql_saga.era AS e USING (table_schema, table_name, era_name)
            WHERE table_schema='integrity_test' AND table_name='sv_trigger';
            drop_result := sql_saga.drop_system_versioning('integrity_test.sv_trigger');
            SELECT array_agg(name ORDER BY name COLLATE "C")
            INTO leftover_names
            FROM (
                SELECT t.tgname::text AS name FROM pg_trigger AS t
                WHERE t.tgrelid='integrity_test.sv_trigger'::regclass
                  AND (t.tgname = ANY(managed_trigger_names) OR t.tgname=sync_trigger_name)
                UNION ALL
                SELECT c.conname::text FROM pg_constraint AS c
                WHERE c.conrelid='integrity_test.sv_trigger'::regclass AND c.conname=ANY(managed_constraint_names)
                UNION ALL
                SELECT a.attname::text FROM pg_attribute AS a
                WHERE a.attrelid='integrity_test.sv_trigger'::regclass AND a.attname=ANY(managed_column_names) AND NOT a.attisdropped
                UNION ALL
                SELECT p.proname::text FROM pg_proc AS p JOIN pg_namespace AS ns ON ns.oid=p.pronamespace
                WHERE ns.nspname='sql_saga' AND p.proname=sync_function_name
            ) AS leftovers;
            IF EXISTS (SELECT FROM sql_saga.system_versioning WHERE table_schema='integrity_test' AND table_name='sv_trigger')
               OR EXISTS (SELECT FROM sql_saga.system_time_era WHERE table_schema='integrity_test' AND table_name='sv_trigger')
               OR cardinality(leftover_names) > 0
               OR to_regclass('integrity_test.sv_trigger_history') IS NOT NULL
               OR to_regclass('integrity_test.sv_trigger_with_history') IS NOT NULL
               OR to_regprocedure('integrity_test.sv_trigger__as_of(timestamptz)') IS NOT NULL
               OR to_regprocedure('integrity_test.sv_trigger__between(timestamptz,timestamptz)') IS NOT NULL
               OR to_regprocedure('integrity_test.sv_trigger__between_symmetric(timestamptz,timestamptz)') IS NOT NULL
               OR to_regprocedure('integrity_test.sv_trigger__from_to(timestamptz,timestamptz)') IS NOT NULL THEN
                RAISE EXCEPTION 'tolerant drop_system_versioning left managed cleanup orphan(s): %', coalesce(leftover_names::text, '{}');
            END IF;
        WHEN 'sv_excluded' THEN
            SELECT ARRAY[ste.generated_always_trigger, ste.write_history_trigger, ste.truncate_trigger],
                   ARRAY[ste.infinity_check_constraint, e.bounds_check_constraint, e.boundary_check_constraint],
                   array_remove(ARRAY[e.range_column_name, e.valid_from_column_name, e.valid_until_column_name, e.valid_to_column_name], NULL),
                   e.sync_temporal_trg_name,
                   e.sync_temporal_trg_function_name
            INTO managed_trigger_names, managed_constraint_names, managed_column_names, sync_trigger_name, sync_function_name
            FROM sql_saga.system_time_era AS ste
            JOIN sql_saga.era AS e USING (table_schema, table_name, era_name)
            WHERE table_schema='integrity_test' AND table_name='sv_excluded';
            drop_result := sql_saga.drop_system_versioning('integrity_test.sv_excluded');
            SELECT array_agg(name ORDER BY name COLLATE "C")
            INTO leftover_names
            FROM (
                SELECT t.tgname::text AS name FROM pg_trigger AS t
                WHERE t.tgrelid='integrity_test.sv_excluded'::regclass
                  AND (t.tgname = ANY(managed_trigger_names) OR t.tgname=sync_trigger_name)
                UNION ALL
                SELECT c.conname::text FROM pg_constraint AS c
                WHERE c.conrelid='integrity_test.sv_excluded'::regclass AND c.conname=ANY(managed_constraint_names)
                UNION ALL
                SELECT a.attname::text FROM pg_attribute AS a
                WHERE a.attrelid='integrity_test.sv_excluded'::regclass AND a.attname=ANY(managed_column_names) AND NOT a.attisdropped
                UNION ALL
                SELECT p.proname::text FROM pg_proc AS p JOIN pg_namespace AS ns ON ns.oid=p.pronamespace
                WHERE ns.nspname='sql_saga' AND p.proname=sync_function_name
            ) AS leftovers;
            IF EXISTS (SELECT FROM sql_saga.system_versioning WHERE table_schema='integrity_test' AND table_name='sv_excluded')
               OR EXISTS (SELECT FROM sql_saga.system_time_era WHERE table_schema='integrity_test' AND table_name='sv_excluded')
               OR cardinality(leftover_names) > 0
               OR to_regclass('integrity_test.sv_excluded_history') IS NOT NULL
               OR to_regclass('integrity_test.sv_excluded_with_history') IS NOT NULL
               OR to_regprocedure('integrity_test.sv_excluded__as_of(timestamptz)') IS NOT NULL
               OR to_regprocedure('integrity_test.sv_excluded__between(timestamptz,timestamptz)') IS NOT NULL
               OR to_regprocedure('integrity_test.sv_excluded__between_symmetric(timestamptz,timestamptz)') IS NOT NULL
               OR to_regprocedure('integrity_test.sv_excluded__from_to(timestamptz,timestamptz)') IS NOT NULL THEN
                RAISE EXCEPTION 'tolerant drop_system_versioning left excluded-family managed cleanup orphan(s): %', coalesce(leftover_names::text, '{}');
            END IF;
        WHEN 'unique' THEN
            PERFORM sql_saga.drop_unique_key_by_name('integrity_test.period_parent', 'period_parent_predicated');
            IF EXISTS (SELECT FROM sql_saga.unique_keys WHERE table_schema='integrity_test' AND table_name='period_parent' AND unique_key_name='period_parent_predicated')
               OR EXISTS (SELECT FROM pg_constraint WHERE conrelid='integrity_test.period_parent'::regclass AND conname LIKE 'period_parent_predicated%')
               OR EXISTS (SELECT FROM pg_class AS c JOIN pg_namespace AS n ON n.oid=c.relnamespace WHERE n.nspname='integrity_test' AND c.relname LIKE 'period_parent_predicated%') THEN
                RAISE EXCEPTION 'tolerant drop_unique_key left an orphan';
            END IF;
        WHEN 'for_portion_of' THEN
            drop_result := sql_saga.drop_for_portion_of_view('integrity_test.period_parent', 'valid');
            IF to_regclass('integrity_test.period_parent__for_portion_of_valid') IS NOT NULL THEN
                RAISE EXCEPTION 'tolerant drop_for_portion_of_view left an orphan';
            END IF;
        ELSE
            RAISE EXCEPTION 'unknown repair kind %', repair_kind;
        END CASE;
        -- The orphan inventory and the drop's return value describe the state right after the tolerant
        -- drop, so they are asserted before the add recreates the family.
        IF integrity_test.managed_leftover_count(family_name) <> 0 THEN
            RAISE EXCEPTION 'tolerant % drop left % managed orphan(s)', repair_kind, integrity_test.managed_leftover_count(family_name);
        END IF;
        IF repair_kind IN ('sv_trigger', 'sv_excluded', 'for_portion_of') AND drop_result IS DISTINCT FROM true THEN
            RAISE EXCEPTION '% returned %, expected true', repair_kind, drop_result;
        END IF;
        IF finish_repair THEN
            CASE repair_kind
            WHEN 'sv_trigger' THEN PERFORM sql_saga.add_system_versioning('integrity_test.sv_trigger');
            WHEN 'sv_excluded' THEN PERFORM sql_saga.add_system_versioning('integrity_test.sv_excluded');
            WHEN 'unique' THEN
                PERFORM sql_saga.add_unique_key('integrity_test.period_parent', ARRAY['id']::name[], 'valid', key_type => 'predicated', predicate => 'category IS NOT NULL', unique_key_name => 'period_parent_predicated');
            WHEN 'for_portion_of' THEN PERFORM sql_saga.add_for_portion_of_view('integrity_test.period_parent', 'valid');
            END CASE;
            IF verify_integrity THEN
                EXECUTE 'SELECT sql_saga.check_integrity()';
            END IF;
        END IF;
        RETURN format('%s | inventory_before_nonzero=t; inventory_after_zero=t; drop_return=%s; ok', case_name, CASE WHEN repair_kind='unique' THEN 'void' ELSE drop_result::text END);
    EXCEPTION WHEN OTHERS THEN
        RETURN format('%s | unexpected %s: %s', case_name, SQLSTATE, SQLERRM);
    END;
END;
$repair_attempt$;

CREATE FUNCTION integrity_test.refusal_attempt(case_name text, command text, expected_state text, expected_message text)
RETURNS text LANGUAGE plpgsql AS $refusal_attempt$
DECLARE actual_state text; actual_message text;
BEGIN
    BEGIN
        EXECUTE command;
        RETURN case_name || ' | unexpected success';
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS actual_state=RETURNED_SQLSTATE, actual_message=MESSAGE_TEXT;
        IF (actual_state, actual_message) IS DISTINCT FROM (expected_state, expected_message) THEN
            RETURN format('%s | unexpected %s: %s', case_name, actual_state, actual_message);
        END IF;
        RETURN format('%s | %s: %s', case_name, actual_state, actual_message);
    END;
END;
$refusal_attempt$;

-- Several valid families covering plain SYSTEM VERSIONING, synchronized eras,
-- every unique-key kind, both FK kinds, both updatable-view kinds, and RLS policies.
CREATE TABLE sv_trigger (id integer PRIMARY KEY, payload text);
CREATE TABLE sv_excluded (id integer PRIMARY KEY, tenant text, payload text);
CREATE TABLE sv_snapshot (id integer PRIMARY KEY, payload text);
CREATE TABLE sv_history (id integer PRIMARY KEY, payload text);
CREATE TABLE sv_policy (id integer PRIMARY KEY, tenant text, payload text);
CREATE TABLE sv_functions (id integer PRIMARY KEY, payload text);
-- Intentionally create z before a so catalog/creation order differs from C-collated identity order.
CREATE TABLE z_order_family (id integer PRIMARY KEY, payload text);
CREATE TABLE a_order_family (id integer PRIMARY KEY, payload text);
SELECT sql_saga.add_system_versioning('sv_trigger');
SELECT sql_saga.add_system_versioning('sv_excluded');
SELECT sql_saga.set_system_time_era_excluded_columns('sv_excluded', ARRAY['tenant']::name[]);
SELECT sql_saga.add_system_versioning('sv_snapshot');
SELECT sql_saga.add_system_versioning('sv_history');
ALTER TABLE sv_policy ENABLE ROW LEVEL SECURITY;
CREATE POLICY sv_policy_tenant ON sv_policy USING (tenant = current_user);
SELECT sql_saga.add_system_versioning('sv_policy');
SELECT sql_saga.add_system_versioning('sv_functions');
SELECT sql_saga.add_system_versioning('z_order_family');
SELECT sql_saga.add_system_versioning('a_order_family');

CREATE TABLE period_parent (id integer, category text, valid_from date, valid_until date, valid_range daterange);
CREATE TABLE period_child (id integer, parent_id integer, valid_from date, valid_until date, valid_range daterange);
CREATE TABLE regular_child (id integer, parent_id integer);
SELECT sql_saga.add_era('period_parent', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('period_parent', ARRAY['id']::name[], 'valid', key_type => 'primary', unique_key_name => 'period_parent_primary');
SELECT sql_saga.add_unique_key('period_parent', ARRAY['category']::name[], 'valid', key_type => 'natural', unique_key_name => 'period_parent_natural', enforce_consistency_with_primary_key => false);
SELECT sql_saga.add_unique_key('period_parent', ARRAY['id']::name[], 'valid', key_type => 'predicated', predicate => 'category IS NOT NULL', unique_key_name => 'period_parent_predicated');
SELECT sql_saga.add_for_portion_of_view('period_parent', 'valid');
SELECT sql_saga.add_current_view('period_parent', 'valid');
SELECT sql_saga.add_era('period_child', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('period_child', ARRAY['id']::name[], 'valid', key_type => 'primary', unique_key_name => 'period_child_primary');
SELECT sql_saga.add_foreign_key('period_child', ARRAY['parent_id']::name[], 'period_parent', ARRAY['id']::name[]);
SELECT sql_saga.add_regular_foreign_key('regular_child', ARRAY['parent_id']::name[], 'period_parent_primary');

\o
SELECT integrity_test.valid('fresh multi-layout installation');

-- PUBLIC is revoked. The extension owner retains EXECUTE, and a trusted role may receive it deliberately.
SET ROLE integrity_untrusted;
SELECT integrity_test.attempt('PUBLIC cannot execute', '42501', 'permission denied for function check_integrity');
RESET ROLE;
SELECT integrity_test.valid('extension owner can execute');
DO $grant$
BEGIN
    IF to_regprocedure('sql_saga.check_integrity()') IS NOT NULL THEN
        GRANT EXECUTE ON FUNCTION sql_saga.check_integrity() TO integrity_granted;
    END IF;
END;
$grant$;
SET ROLE integrity_granted;
SELECT integrity_test.valid('granted role can execute');
RESET ROLE;
DO $revoke$
BEGIN
    IF to_regprocedure('sql_saga.check_integrity()') IS NOT NULL THEN
        REVOKE EXECUTE ON FUNCTION sql_saga.check_integrity() FROM integrity_granted;
    END IF;
END;
$revoke$;

-- Tolerance for missing members must not weaken the v1.1 RESTRICT dependency refusals.
SAVEPOINT legacy_unique_restrict;
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT trigger_name INTO n FROM sql_saga.updatable_view
    WHERE table_schema='integrity_test' AND table_name='period_parent' AND view_type='current';
    EXECUTE format('DROP TRIGGER %I ON integrity_test.period_parent__current_valid', n);
END $damage$;
RESET session_replication_role;
SELECT integrity_test.save_snapshot('legacy_unique_restrict');
SELECT integrity_test.refusal_attempt(
    'damaged unique key keeps dependent-FK RESTRICT refusal',
    $$SELECT sql_saga.drop_unique_key_by_name('integrity_test.period_parent', 'period_parent_primary', 'RESTRICT')$$,
    'P0001',
    'cannot drop unique key "period_parent_primary" because foreign key "period_child_parent_id_valid" on table "integrity_test.period_child" depends on it');
SELECT integrity_test.snapshot_unchanged('legacy_unique_restrict');
ROLLBACK TO SAVEPOINT legacy_unique_restrict;
RELEASE SAVEPOINT legacy_unique_restrict;

SAVEPOINT legacy_era_restrict;
SET session_replication_role = replica;
DROP TRIGGER for_portion_of_valid ON period_parent__for_portion_of_valid;
RESET session_replication_role;
SELECT integrity_test.save_snapshot('legacy_era_restrict');
SELECT integrity_test.refusal_attempt(
    'damaged era keeps dependent-key RESTRICT refusal',
    $$SELECT sql_saga.drop_era('integrity_test.period_parent', 'valid', 'RESTRICT')$$,
    'P0001',
    'era valid is part of a UNIQUE or PRIMARY KEY');
SELECT integrity_test.snapshot_unchanged('legacy_era_restrict');
ROLLBACK TO SAVEPOINT legacy_era_restrict;
RELEASE SAVEPOINT legacy_era_restrict;

-- Missing SYSTEM_TIME trigger. The checker must not recreate it.
SAVEPOINT missing_trigger;
SELECT integrity_test.valid('SYSTEM_TIME trigger valid before damage');
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT generated_always_trigger INTO n FROM sql_saga.system_time_era WHERE table_schema='integrity_test' AND table_name='sv_trigger';
    EXECUTE format('DROP TRIGGER %I ON integrity_test.sv_trigger', n);
END $damage$;
RESET session_replication_role;
SELECT integrity_test.unrelated_ddl('missing_trigger');
SELECT integrity_test.save_snapshot('missing_trigger');
SELECT integrity_test.attempt('missing SYSTEM_TIME trigger', 'P0001', 'sql_saga: integrity violation for table "integrity_test.sv_trigger": SYSTEM_TIME trigger is missing; repair with drop_system_versioning and add_system_versioning');
SELECT integrity_test.snapshot_unchanged('missing_trigger');
SELECT count(*) = 2 AS checker_did_not_repair_missing_trigger FROM pg_trigger WHERE tgrelid='integrity_test.sv_trigger'::regclass AND NOT tgisinternal;
SET LOCAL client_min_messages = notice;
SELECT integrity_test.repair_attempt('SYSTEM_TIME trigger tolerant drop/add repair', 'sv_trigger');
SET LOCAL client_min_messages = warning;
ROLLBACK TO SAVEPOINT missing_trigger;
RELEASE SAVEPOINT missing_trigger;
SELECT integrity_test.valid('missing trigger rollback restores integrity');

-- Deferred Step 2 case: excluded column removed while metadata and snapshot remain.
SAVEPOINT excluded_column;
SELECT integrity_test.valid('excluded column valid before damage');
SET session_replication_role = replica;
ALTER TABLE sv_excluded DROP COLUMN tenant CASCADE;
RESET session_replication_role;
SELECT integrity_test.unrelated_ddl('excluded_column');
SELECT integrity_test.save_snapshot('excluded_column');
SELECT integrity_test.attempt('missing excluded column', 'P0001', 'sql_saga: integrity violation for table "integrity_test.sv_excluded": excluded SYSTEM_TIME column "tenant" is missing; repair with set_system_time_era_excluded_columns or drop_system_versioning');
SELECT integrity_test.snapshot_unchanged('excluded_column');
SELECT excluded_column_names = ARRAY['tenant']::name[] AS checker_did_not_repair_excluded_metadata FROM sql_saga.system_time_era WHERE table_schema='integrity_test' AND table_name='sv_excluded';
SELECT NOT EXISTS (SELECT FROM pg_attribute WHERE attrelid='integrity_test.sv_excluded'::regclass AND attname='tenant' AND NOT attisdropped) AS checker_did_not_repair_excluded_column;
SET LOCAL client_min_messages = notice;
SELECT integrity_test.repair_attempt('excluded column tolerant drop/add repair', 'sv_excluded');
SET LOCAL client_min_messages = warning;
ROLLBACK TO SAVEPOINT excluded_column;
RELEASE SAVEPOINT excluded_column;

-- Stale table-columns snapshot. Canonical rebuild is the supported repair.
SAVEPOINT stale_snapshot;
SELECT integrity_test.valid('SYSTEM_TIME snapshot valid before damage');
UPDATE sql_saga.system_time_era SET table_columns_snapshot=ARRAY['stale']::name[] WHERE table_schema='integrity_test' AND table_name='sv_snapshot';
SELECT integrity_test.unrelated_ddl('stale_snapshot');
SELECT integrity_test.save_snapshot('stale_snapshot');
SELECT integrity_test.attempt('stale SYSTEM_TIME snapshot', 'P0001', 'sql_saga: integrity violation for table "integrity_test.sv_snapshot": SYSTEM_TIME column snapshot is stale; repair with rebuild_system_versioning_views');
SELECT integrity_test.snapshot_unchanged('stale_snapshot');
SELECT table_columns_snapshot = ARRAY['stale']::name[] AS checker_did_not_repair_snapshot FROM sql_saga.system_time_era WHERE table_schema='integrity_test' AND table_name='sv_snapshot';
SELECT sql_saga.rebuild_system_versioning_views('integrity_test.sv_snapshot');
SELECT integrity_test.valid('snapshot rebuild restores integrity');
ROLLBACK TO SAVEPOINT stale_snapshot;
RELEASE SAVEPOINT stale_snapshot;

-- Missing temporal unique object, catalog row retained.
SAVEPOINT unique_object;
SELECT integrity_test.valid('temporal unique valid before damage');
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT coalesce(unique_constraint, exclude_constraint) INTO n FROM sql_saga.unique_keys WHERE unique_key_name='period_parent_predicated';
    EXECUTE format('ALTER TABLE integrity_test.period_parent DROP CONSTRAINT %I', n);
END $damage$;
RESET session_replication_role;
SELECT integrity_test.unrelated_ddl('unique_object');
SELECT integrity_test.save_snapshot('unique_object');
SELECT integrity_test.attempt('missing temporal unique object', 'P0001', 'sql_saga: integrity violation for unique key "period_parent_predicated" on table "integrity_test.period_parent": managed constraint or index is missing; repair with drop_unique_key and add_unique_key');
SELECT integrity_test.snapshot_unchanged('unique_object');
SELECT to_regclass('integrity_test.period_parent_predicated') IS NULL AS checker_did_not_repair_unique_object;
SET LOCAL client_min_messages = notice;
SELECT integrity_test.repair_attempt('temporal unique tolerant drop/add repair', 'unique');
SET LOCAL client_min_messages = warning;
ROLLBACK TO SAVEPOINT unique_object;
RELEASE SAVEPOINT unique_object;

-- Missing regular-to-temporal helper trigger, catalog row retained.
SAVEPOINT fk_helper;
SELECT integrity_test.valid('foreign-key helper valid before damage');
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT uk_update_trigger INTO n FROM sql_saga.foreign_keys WHERE table_schema='integrity_test' AND table_name='regular_child';
    EXECUTE format('DROP TRIGGER %I ON integrity_test.period_parent', n);
END $damage$;
RESET session_replication_role;
SELECT integrity_test.unrelated_ddl('fk_helper');
SELECT integrity_test.save_snapshot('fk_helper');
SELECT integrity_test.attempt('missing foreign-key helper trigger', 'P0001', 'sql_saga: integrity violation for foreign key on table "integrity_test.regular_child": referenced-side helper trigger is missing; repair with drop_foreign_key and add_regular_foreign_key');
SELECT integrity_test.snapshot_unchanged('fk_helper');
SELECT NOT EXISTS (SELECT FROM pg_trigger WHERE tgrelid='integrity_test.period_parent'::regclass AND tgname=(SELECT uk_update_trigger FROM sql_saga.foreign_keys WHERE table_schema='integrity_test' AND table_name='regular_child')) AS checker_did_not_repair_fk_helper;
SET LOCAL client_min_messages = notice;
SELECT integrity_test.managed_leftover_count('regular_child') > 0 AS foreign_key_inventory_before_nonzero;
SELECT sql_saga.drop_foreign_key('integrity_test.regular_child', ARRAY['parent_id']::name[]) AS returned_void;
SELECT integrity_test.managed_leftover_count('regular_child') = 0 AS tolerant_drop_foreign_key_left_no_orphan;
SELECT sql_saga.add_regular_foreign_key('integrity_test.regular_child', ARRAY['parent_id']::name[], 'period_parent_primary');
SET LOCAL client_min_messages = warning;
SELECT integrity_test.valid('foreign-key API repair restores integrity');
ROLLBACK TO SAVEPOINT fk_helper;
RELEASE SAVEPOINT fk_helper;

-- Updatable view is replaced without its identifier or trigger, catalog row retained.
SAVEPOINT updatable_view;
SELECT integrity_test.valid('updatable view valid before damage');
SET session_replication_role = replica;
DROP VIEW period_parent__for_portion_of_valid;
CREATE VIEW period_parent__for_portion_of_valid AS SELECT id, category, valid_from, valid_until, valid_range FROM period_parent;
RESET session_replication_role;
SELECT integrity_test.unrelated_ddl('updatable_view');
SELECT integrity_test.save_snapshot('updatable_view');
SELECT integrity_test.attempt('broken FOR PORTION OF view', 'P0001', 'sql_saga: integrity violation for updatable view "integrity_test.period_parent__for_portion_of_valid" of table "integrity_test.period_parent": identifier column or trigger is missing; repair with drop_for_portion_of_view and add_for_portion_of_view');
SELECT integrity_test.snapshot_unchanged('updatable_view');
SELECT count(*) = 0 AS checker_did_not_repair_view_trigger FROM pg_trigger WHERE tgrelid='integrity_test.period_parent__for_portion_of_valid'::regclass AND NOT tgisinternal;
SET LOCAL client_min_messages = notice;
SELECT integrity_test.repair_attempt('updatable-view tolerant drop/add repair', 'for_portion_of');
SET LOCAL client_min_messages = warning;
ROLLBACK TO SAVEPOINT updatable_view;
RELEASE SAVEPOINT updatable_view;
SELECT integrity_test.valid('updatable-view rollback restores integrity');

-- History member removed, family catalog retained.
SAVEPOINT history_member;
SELECT integrity_test.valid('history family valid before damage');
SET session_replication_role = replica;
DROP VIEW sv_history_with_history CASCADE;
RESET session_replication_role;
SELECT integrity_test.unrelated_ddl('history_member');
SELECT integrity_test.save_snapshot('history_member');
SELECT integrity_test.attempt('missing public history view', 'P0001', 'sql_saga: integrity violation for system-versioned table "integrity_test.sv_history": history view "integrity_test.sv_history_with_history" is missing; repair with rebuild_system_versioning_views');
SELECT integrity_test.snapshot_unchanged('history_member');
SELECT to_regclass('integrity_test.sv_history_with_history') IS NULL AS checker_did_not_repair_history_view;
SELECT sql_saga.rebuild_system_versioning_views('integrity_test.sv_history');
SELECT integrity_test.valid('history-family rebuild restores integrity');
ROLLBACK TO SAVEPOINT history_member;
RELEASE SAVEPOINT history_member;

-- Managed history-policy shape damage. Diagnostics never disclose expressions or ACLs.
SAVEPOINT policy_shape;
SELECT integrity_test.valid('managed policies valid before damage');
CREATE TEMP TABLE dropped_policy_name (policy_name name) ON COMMIT DROP;
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT pol.polname INTO n FROM pg_policy AS pol WHERE pol.polrelid='integrity_test.sv_policy_history'::regclass ORDER BY pol.polname LIMIT 1;
    EXECUTE format('DROP POLICY %I ON integrity_test.sv_policy_history', n);
    INSERT INTO dropped_policy_name VALUES (n);
END $damage$;
RESET session_replication_role;
SELECT integrity_test.unrelated_ddl('policy_shape');
SELECT integrity_test.save_snapshot('policy_shape');
SELECT integrity_test.attempt('managed policy shape', 'P0001', 'sql_saga: integrity violation for system-versioned table "integrity_test.sv_policy": managed history policy set is inconsistent; repair with sync_history_policies');
SELECT integrity_test.snapshot_unchanged('policy_shape');
SELECT NOT EXISTS (SELECT FROM pg_policy AS p JOIN dropped_policy_name AS d ON d.policy_name=p.polname WHERE p.polrelid='integrity_test.sv_policy_history'::regclass) AS checker_did_not_repair_policy;
SELECT sql_saga.sync_history_policies('integrity_test.sv_policy');
SELECT integrity_test.valid('policy sync restores integrity');
ROLLBACK TO SAVEPOINT policy_shape;
RELEASE SAVEPOINT policy_shape;

-- Deferred Step 3 case: each missing generated role and several missing roles are diagnosed.
SAVEPOINT missing_functions;
SELECT integrity_test.valid('generated as_of valid before damage');
SET session_replication_role = replica;
DROP FUNCTION sv_functions__as_of(timestamptz);
RESET session_replication_role;
SELECT integrity_test.unrelated_ddl('missing_as_of');
SELECT integrity_test.save_snapshot('generated_functions');
SELECT integrity_test.attempt('missing generated as_of role', 'P0001', 'sql_saga: integrity violation for system-versioned table "integrity_test.sv_functions": generated query function role "as_of" is missing; repair with rebuild_system_versioning_views');
SELECT integrity_test.snapshot_unchanged('generated_functions');
SELECT to_regprocedure('integrity_test.sv_functions__as_of(timestamptz)') IS NULL AS checker_did_not_repair_missing_function;
SELECT sql_saga.rebuild_system_versioning_views('integrity_test.sv_functions');
SELECT integrity_test.valid('query-function rebuild restores integrity');
ROLLBACK TO SAVEPOINT missing_functions;
RELEASE SAVEPOINT missing_functions;

SAVEPOINT missing_between;
SELECT integrity_test.valid('generated between valid before damage');
SET session_replication_role = replica;
DROP FUNCTION sv_functions__between(timestamptz,timestamptz);
RESET session_replication_role;
SELECT integrity_test.unrelated_ddl('missing_between');
SELECT integrity_test.save_snapshot('missing_between');
SELECT integrity_test.attempt('missing generated between role', 'P0001', 'sql_saga: integrity violation for system-versioned table "integrity_test.sv_functions": generated query function role "between" is missing; repair with rebuild_system_versioning_views');
SELECT integrity_test.snapshot_unchanged('missing_between');
SELECT to_regprocedure('integrity_test.sv_functions__between(timestamptz,timestamptz)') IS NULL AS checker_did_not_repair_missing_between;
SELECT sql_saga.rebuild_system_versioning_views('integrity_test.sv_functions');
SELECT integrity_test.valid('between rebuild restores integrity');
ROLLBACK TO SAVEPOINT missing_between;
RELEASE SAVEPOINT missing_between;

SAVEPOINT missing_between_symmetric;
SELECT integrity_test.valid('generated between_symmetric valid before damage');
SET session_replication_role = replica;
DROP FUNCTION sv_functions__between_symmetric(timestamptz,timestamptz);
RESET session_replication_role;
SELECT integrity_test.unrelated_ddl('missing_between_symmetric');
SELECT integrity_test.save_snapshot('missing_between_symmetric');
SELECT integrity_test.attempt('missing generated between_symmetric role', 'P0001', 'sql_saga: integrity violation for system-versioned table "integrity_test.sv_functions": generated query function role "between_symmetric" is missing; repair with rebuild_system_versioning_views');
SELECT integrity_test.snapshot_unchanged('missing_between_symmetric');
SELECT to_regprocedure('integrity_test.sv_functions__between_symmetric(timestamptz,timestamptz)') IS NULL AS checker_did_not_repair_missing_between_symmetric;
SELECT sql_saga.rebuild_system_versioning_views('integrity_test.sv_functions');
SELECT integrity_test.valid('between_symmetric rebuild restores integrity');
ROLLBACK TO SAVEPOINT missing_between_symmetric;
RELEASE SAVEPOINT missing_between_symmetric;

SAVEPOINT missing_from_to;
SELECT integrity_test.valid('generated from_to valid before damage');
SET session_replication_role = replica;
DROP FUNCTION sv_functions__from_to(timestamptz,timestamptz);
RESET session_replication_role;
SELECT integrity_test.unrelated_ddl('missing_from_to');
SELECT integrity_test.save_snapshot('missing_from_to');
SELECT integrity_test.attempt('missing generated from_to role', 'P0001', 'sql_saga: integrity violation for system-versioned table "integrity_test.sv_functions": generated query function role "from_to" is missing; repair with rebuild_system_versioning_views');
SELECT integrity_test.snapshot_unchanged('missing_from_to');
SELECT to_regprocedure('integrity_test.sv_functions__from_to(timestamptz,timestamptz)') IS NULL AS checker_did_not_repair_missing_from_to;
SELECT sql_saga.rebuild_system_versioning_views('integrity_test.sv_functions');
SELECT integrity_test.valid('from_to rebuild restores integrity');
ROLLBACK TO SAVEPOINT missing_from_to;
RELEASE SAVEPOINT missing_from_to;

SAVEPOINT several_missing_functions;
SELECT integrity_test.valid('several generated roles valid before damage');
SET session_replication_role = replica;
DROP FUNCTION sv_functions__as_of(timestamptz);
DROP FUNCTION sv_functions__between(timestamptz,timestamptz);
RESET session_replication_role;
SELECT integrity_test.unrelated_ddl('several_missing_roles');
SELECT integrity_test.save_snapshot('several_missing_roles');
SELECT integrity_test.attempt('several missing roles deterministic first', 'P0001', 'sql_saga: integrity violation for system-versioned table "integrity_test.sv_functions": generated query function role "as_of" is missing; repair with rebuild_system_versioning_views');
SELECT integrity_test.snapshot_unchanged('several_missing_roles');
SELECT to_regprocedure('integrity_test.sv_functions__as_of(timestamptz)') IS NULL AND to_regprocedure('integrity_test.sv_functions__between(timestamptz,timestamptz)') IS NULL AS checker_did_not_repair_several_missing_roles;
SELECT sql_saga.rebuild_system_versioning_views('integrity_test.sv_functions');
SELECT integrity_test.valid('several-role rebuild restores integrity');
ROLLBACK TO SAVEPOINT several_missing_functions;
RELEASE SAVEPOINT several_missing_functions;

-- Correct identity and properties but wrong return-family dependency.
SAVEPOINT broken_dependency;
SELECT integrity_test.valid('generated dependency valid before damage');
SET session_replication_role = replica;
DROP FUNCTION sv_functions__as_of(timestamptz);
CREATE FUNCTION sv_functions__as_of(p_at timestamptz) RETURNS SETOF sv_history_with_history LANGUAGE sql STABLE AS $body$ SELECT * FROM integrity_test.sv_history_with_history WHERE system_valid_range @> p_at $body$;
RESET session_replication_role;
SELECT integrity_test.unrelated_ddl('broken_dependency');
SELECT integrity_test.save_snapshot('broken_dependency');
SELECT integrity_test.attempt('broken generated-function return dependency', 'P0001', 'sql_saga: integrity violation for system-versioned table "integrity_test.sv_functions": generated query function role "as_of" has a noncanonical return-type dependency; repair with rebuild_system_versioning_views');
SELECT integrity_test.snapshot_unchanged('broken_dependency');
SELECT pg_get_function_result('integrity_test.sv_functions__as_of(timestamptz)'::regprocedure) LIKE '%sv_history_with_history' AS checker_did_not_repair_dependency;
SELECT sql_saga.rebuild_system_versioning_views('integrity_test.sv_functions');
SELECT integrity_test.valid('dependency rebuild restores integrity');
ROLLBACK TO SAVEPOINT broken_dependency;
RELEASE SAVEPOINT broken_dependency;

-- Canonical name/signature/properties and return type, but deliberately wrong prosrc.
SAVEPOINT wrong_prosrc;
SELECT integrity_test.valid('generated as_of body valid before damage');
SET session_replication_role = replica;
DROP FUNCTION sv_functions__as_of(timestamptz);
CREATE FUNCTION sv_functions__as_of(p_at timestamptz) RETURNS SETOF sv_functions_with_history LANGUAGE sql STABLE AS $wrong_body$ SELECT * FROM integrity_test.sv_functions_with_history WHERE false $wrong_body$;
RESET session_replication_role;
SELECT integrity_test.unrelated_ddl('wrong_prosrc');
SELECT integrity_test.save_snapshot('wrong_prosrc');
SELECT integrity_test.attempt('noncanonical generated-function body', 'P0001', 'sql_saga: integrity violation for system-versioned table "integrity_test.sv_functions": generated query function role "as_of" definition is noncanonical; repair with rebuild_system_versioning_views');
SELECT integrity_test.snapshot_unchanged('wrong_prosrc');
SELECT position('WHERE false' in pg_get_functiondef('integrity_test.sv_functions__as_of(timestamptz)'::regprocedure)) > 0 AS checker_did_not_repair_body;
SELECT sql_saga.rebuild_system_versioning_views('integrity_test.sv_functions');
SELECT integrity_test.valid('body rebuild restores integrity');
ROLLBACK TO SAVEPOINT wrong_prosrc;
RELEASE SAVEPOINT wrong_prosrc;

SAVEPOINT wrong_between_prosrc;
SELECT integrity_test.valid('generated between body valid before damage');
SET session_replication_role = replica;
DROP FUNCTION sv_functions__between(timestamptz,timestamptz);
CREATE FUNCTION sv_functions__between(p_start timestamptz, p_end timestamptz) RETURNS SETOF sv_functions_with_history LANGUAGE sql STABLE AS $wrong_body$ SELECT * FROM integrity_test.sv_functions_with_history WHERE false $wrong_body$;
RESET session_replication_role;
SELECT integrity_test.unrelated_ddl('wrong_between_prosrc');
SELECT integrity_test.save_snapshot('wrong_between_prosrc');
SELECT integrity_test.attempt('noncanonical generated between body', 'P0001', 'sql_saga: integrity violation for system-versioned table "integrity_test.sv_functions": generated query function role "between" definition is noncanonical; repair with rebuild_system_versioning_views');
SELECT integrity_test.snapshot_unchanged('wrong_between_prosrc');
SELECT position('WHERE false' in pg_get_functiondef('integrity_test.sv_functions__between(timestamptz,timestamptz)'::regprocedure)) > 0 AS checker_did_not_repair_between_body;
SELECT sql_saga.rebuild_system_versioning_views('integrity_test.sv_functions');
SELECT integrity_test.valid('between body rebuild restores integrity');
ROLLBACK TO SAVEPOINT wrong_between_prosrc;
RELEASE SAVEPOINT wrong_between_prosrc;

SAVEPOINT wrong_between_symmetric_prosrc;
SELECT integrity_test.valid('generated between_symmetric body valid before damage');
SET session_replication_role = replica;
DROP FUNCTION sv_functions__between_symmetric(timestamptz,timestamptz);
CREATE FUNCTION sv_functions__between_symmetric(p_start timestamptz, p_end timestamptz) RETURNS SETOF sv_functions_with_history LANGUAGE sql STABLE AS $wrong_body$ SELECT * FROM integrity_test.sv_functions_with_history WHERE false $wrong_body$;
RESET session_replication_role;
SELECT integrity_test.unrelated_ddl('wrong_between_symmetric_prosrc');
SELECT integrity_test.save_snapshot('wrong_between_symmetric_prosrc');
SELECT integrity_test.attempt('noncanonical generated between_symmetric body', 'P0001', 'sql_saga: integrity violation for system-versioned table "integrity_test.sv_functions": generated query function role "between_symmetric" definition is noncanonical; repair with rebuild_system_versioning_views');
SELECT integrity_test.snapshot_unchanged('wrong_between_symmetric_prosrc');
SELECT position('WHERE false' in pg_get_functiondef('integrity_test.sv_functions__between_symmetric(timestamptz,timestamptz)'::regprocedure)) > 0 AS checker_did_not_repair_between_symmetric_body;
SELECT sql_saga.rebuild_system_versioning_views('integrity_test.sv_functions');
SELECT integrity_test.valid('between_symmetric body rebuild restores integrity');
ROLLBACK TO SAVEPOINT wrong_between_symmetric_prosrc;
RELEASE SAVEPOINT wrong_between_symmetric_prosrc;

SAVEPOINT wrong_from_to_prosrc;
SELECT integrity_test.valid('generated from_to body valid before damage');
SET session_replication_role = replica;
DROP FUNCTION sv_functions__from_to(timestamptz,timestamptz);
CREATE FUNCTION sv_functions__from_to(p_start timestamptz, p_end timestamptz) RETURNS SETOF sv_functions_with_history LANGUAGE sql STABLE AS $wrong_body$ SELECT * FROM integrity_test.sv_functions_with_history WHERE false $wrong_body$;
RESET session_replication_role;
SELECT integrity_test.unrelated_ddl('wrong_from_to_prosrc');
SELECT integrity_test.save_snapshot('wrong_from_to_prosrc');
SELECT integrity_test.attempt('noncanonical generated from_to body', 'P0001', 'sql_saga: integrity violation for system-versioned table "integrity_test.sv_functions": generated query function role "from_to" definition is noncanonical; repair with rebuild_system_versioning_views');
SELECT integrity_test.snapshot_unchanged('wrong_from_to_prosrc');
SELECT position('WHERE false' in pg_get_functiondef('integrity_test.sv_functions__from_to(timestamptz,timestamptz)'::regprocedure)) > 0 AS checker_did_not_repair_from_to_body;
SELECT sql_saga.rebuild_system_versioning_views('integrity_test.sv_functions');
SELECT integrity_test.valid('from_to body rebuild restores integrity');
ROLLBACK TO SAVEPOINT wrong_from_to_prosrc;
RELEASE SAVEPOINT wrong_from_to_prosrc;

-- Deferred Step 3 stale identities produced by bypassed rename and schema move.
SAVEPOINT renamed_role;
SELECT integrity_test.valid('generated renamed role valid before damage');
SET session_replication_role = replica;
ALTER FUNCTION sv_functions__as_of(timestamptz) RENAME TO sv_functions__as_of_moved;
RESET session_replication_role;
SELECT integrity_test.unrelated_ddl('renamed_generated_role');
SELECT integrity_test.save_snapshot('renamed_generated_role');
SELECT integrity_test.attempt('renamed generated as_of role', 'P0001', 'sql_saga: integrity violation for system-versioned table "integrity_test.sv_functions": generated query function role "as_of" is missing; repair with rebuild_system_versioning_views');
SELECT integrity_test.snapshot_unchanged('renamed_generated_role');
SELECT to_regprocedure('integrity_test.sv_functions__as_of(timestamptz)') IS NULL AND to_regprocedure('integrity_test.sv_functions__as_of_moved(timestamptz)') IS NOT NULL AS checker_did_not_repair_renamed_role;
DROP FUNCTION sv_functions__as_of_moved(timestamptz);
SELECT sql_saga.rebuild_system_versioning_views('integrity_test.sv_functions');
SELECT integrity_test.valid('renamed-role rebuild restores integrity');
ROLLBACK TO SAVEPOINT renamed_role;
RELEASE SAVEPOINT renamed_role;

SAVEPOINT moved_role;
SELECT integrity_test.valid('generated moved role valid before damage');
CREATE SCHEMA integrity_moved;
SET session_replication_role = replica;
ALTER FUNCTION sv_functions__between(timestamptz,timestamptz) SET SCHEMA integrity_moved;
RESET session_replication_role;
SELECT integrity_test.unrelated_ddl('moved_generated_role');
SELECT integrity_test.save_snapshot('moved_generated_role');
SELECT integrity_test.attempt('moved generated between role', 'P0001', 'sql_saga: integrity violation for system-versioned table "integrity_test.sv_functions": generated query function role "between" is missing; repair with rebuild_system_versioning_views');
SELECT integrity_test.snapshot_unchanged('moved_generated_role');
SELECT to_regprocedure('integrity_test.sv_functions__between(timestamptz,timestamptz)') IS NULL AND to_regprocedure('integrity_moved.sv_functions__between(timestamptz,timestamptz)') IS NOT NULL AS checker_did_not_repair_moved_role;
DROP FUNCTION integrity_moved.sv_functions__between(timestamptz,timestamptz);
SELECT sql_saga.rebuild_system_versioning_views('integrity_test.sv_functions');
SELECT integrity_test.valid('moved-role rebuild restores integrity');
ROLLBACK TO SAVEPOINT moved_role;
RELEASE SAVEPOINT moved_role;

-- Cross-class priority in one managed family: unique keys precede updatable views.
SAVEPOINT cross_class_one_family;
SELECT integrity_test.valid('cross-class one-family valid before damage');
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT coalesce(unique_constraint, exclude_constraint) INTO n FROM sql_saga.unique_keys WHERE unique_key_name='period_parent_predicated';
    EXECUTE format('ALTER TABLE integrity_test.period_parent DROP CONSTRAINT %I', n);
END $damage$;
DROP VIEW period_parent__for_portion_of_valid;
CREATE VIEW period_parent__for_portion_of_valid AS SELECT id, category, valid_from, valid_until, valid_range FROM period_parent;
RESET session_replication_role;
SELECT integrity_test.unrelated_ddl('cross_class_one_family');
SELECT integrity_test.save_snapshot('cross_class_one_family');
SELECT integrity_test.attempt('cross-class one-family first unique', 'P0001', 'sql_saga: integrity violation for unique key "period_parent_predicated" on table "integrity_test.period_parent": managed constraint or index is missing; repair with drop_unique_key and add_unique_key');
SELECT integrity_test.snapshot_unchanged('cross_class_one_family');
SELECT to_regclass('integrity_test.period_parent_predicated') IS NULL AND NOT EXISTS (SELECT FROM pg_trigger WHERE tgrelid='integrity_test.period_parent__for_portion_of_valid'::regclass AND NOT tgisinternal) AS checker_did_not_repair_cross_class_one_family;
SET LOCAL client_min_messages = notice;
SELECT integrity_test.repair_attempt('cross-class one-family repair first unique', 'unique', verify_integrity => false);
SELECT integrity_test.attempt('cross-class one-family next updatable view', 'P0001', 'sql_saga: integrity violation for updatable view "integrity_test.period_parent__for_portion_of_valid" of table "integrity_test.period_parent": identifier column or trigger is missing; repair with drop_for_portion_of_view and add_for_portion_of_view');
SELECT integrity_test.repair_attempt('cross-class one-family repair next view', 'for_portion_of');
SET LOCAL client_min_messages = warning;
ROLLBACK TO SAVEPOINT cross_class_one_family;
RELEASE SAVEPOINT cross_class_one_family;

-- Cross-class priority across families: SYSTEM_TIME precedes generated functions.
SAVEPOINT cross_class_two_families;
SELECT integrity_test.valid('cross-class two-family valid before damage');
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT generated_always_trigger INTO n FROM sql_saga.system_time_era WHERE table_schema='integrity_test' AND table_name='sv_trigger';
    EXECUTE format('DROP TRIGGER %I ON integrity_test.sv_trigger', n);
END $damage$;
DROP FUNCTION sv_functions__as_of(timestamptz);
RESET session_replication_role;
SELECT integrity_test.unrelated_ddl('cross_class_two_families');
SELECT integrity_test.save_snapshot('cross_class_two_families');
SELECT integrity_test.attempt('cross-class two-family first SYSTEM_TIME', 'P0001', 'sql_saga: integrity violation for table "integrity_test.sv_trigger": SYSTEM_TIME trigger is missing; repair with drop_system_versioning and add_system_versioning');
SELECT integrity_test.snapshot_unchanged('cross_class_two_families');
SELECT count(*) = 2 AND to_regprocedure('integrity_test.sv_functions__as_of(timestamptz)') IS NULL AS checker_did_not_repair_cross_class_two_families FROM pg_trigger WHERE tgrelid='integrity_test.sv_trigger'::regclass AND NOT tgisinternal;
SET LOCAL client_min_messages = notice;
SELECT integrity_test.repair_attempt('cross-class two-family repair first SYSTEM_TIME', 'sv_trigger', verify_integrity => false);
SET LOCAL client_min_messages = warning;
SELECT integrity_test.attempt('cross-class two-family next generated function', 'P0001', 'sql_saga: integrity violation for system-versioned table "integrity_test.sv_functions": generated query function role "as_of" is missing; repair with rebuild_system_versioning_views');
SELECT sql_saga.rebuild_system_versioning_views('integrity_test.sv_functions');
SELECT integrity_test.valid('cross-class two-family repairs restore integrity');
ROLLBACK TO SAVEPOINT cross_class_two_families;
RELEASE SAVEPOINT cross_class_two_families;

-- Same-class order is schema-qualified identity under C collation, not creation/catalog order.
SAVEPOINT same_class_order;
SELECT integrity_test.valid('same-class stable-order valid before damage');
SET session_replication_role = replica;
DROP FUNCTION z_order_family__as_of(timestamptz);
DROP FUNCTION a_order_family__as_of(timestamptz);
RESET session_replication_role;
SELECT integrity_test.unrelated_ddl('same_class_order');
SELECT integrity_test.save_snapshot('same_class_order');
SELECT integrity_test.attempt('same-class stable first a_order_family', 'P0001', 'sql_saga: integrity violation for system-versioned table "integrity_test.a_order_family": generated query function role "as_of" is missing; repair with rebuild_system_versioning_views');
SELECT integrity_test.snapshot_unchanged('same_class_order');
SELECT to_regprocedure('integrity_test.a_order_family__as_of(timestamptz)') IS NULL AND to_regprocedure('integrity_test.z_order_family__as_of(timestamptz)') IS NULL AS checker_did_not_repair_same_class_order;
SELECT sql_saga.rebuild_system_versioning_views('integrity_test.a_order_family');
SELECT integrity_test.attempt('same-class stable next z_order_family', 'P0001', 'sql_saga: integrity violation for system-versioned table "integrity_test.z_order_family": generated query function role "as_of" is missing; repair with rebuild_system_versioning_views');
SELECT sql_saga.rebuild_system_versioning_views('integrity_test.z_order_family');
SELECT integrity_test.valid('same-class stable-order API repairs restore integrity');
ROLLBACK TO SAVEPOINT same_class_order;
RELEASE SAVEPOINT same_class_order;

-- The public surface intentionally has no all-defects report API.
SELECT to_regprocedure('sql_saga.check_integrity_report()') IS NULL AS no_public_report_api;

ROLLBACK;
\i sql/include/test_teardown.sql
