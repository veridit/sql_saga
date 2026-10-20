\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
SET client_min_messages = warning;
\o /dev/null

BEGIN;
CREATE SCHEMA integrity_g4;
SET search_path = integrity_g4, public;
CREATE ROLE integrity_g4_untrusted;
CREATE ROLE integrity_g4_other;
GRANT USAGE ON SCHEMA sql_saga TO integrity_g4_untrusted;
GRANT USAGE ON SCHEMA integrity_g4 TO integrity_g4_untrusted;

CREATE FUNCTION integrity_g4.attempt(case_name text, expected_message text)
RETURNS text LANGUAGE plpgsql AS $attempt$
DECLARE actual_state text; actual_message text; before_state jsonb;
BEGIN
    before_state := integrity_g4.snapshot_state();
    BEGIN
        PERFORM sql_saga.check_integrity();
        IF before_state IS DISTINCT FROM integrity_g4.snapshot_state() THEN
            RETURN case_name || ' | checker changed state';
        END IF;
        RETURN case_name || ' | unexpected success';
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS actual_state = RETURNED_SQLSTATE, actual_message = MESSAGE_TEXT;
        IF before_state IS DISTINCT FROM integrity_g4.snapshot_state() THEN
            RETURN case_name || ' | checker changed state';
        END IF;
        IF actual_state <> 'P0001' OR actual_message <> expected_message THEN
            RETURN format('%s | unexpected %s: %s', case_name, actual_state, actual_message);
        END IF;
        RETURN format('%s | P0001: %s', case_name, actual_message);
    END;
END;
$attempt$;

CREATE FUNCTION integrity_g4.valid(case_name text)
RETURNS text LANGUAGE plpgsql AS $valid$
BEGIN
    PERFORM sql_saga.check_integrity();
    RETURN case_name || ' | ok';
EXCEPTION WHEN OTHERS THEN
    RETURN format('%s | unexpected %s: %s', case_name, SQLSTATE, SQLERRM);
END;
$valid$;

CREATE FUNCTION integrity_g4.snapshot_state()
RETURNS jsonb LANGUAGE sql STABLE AS $snapshot_state$
SELECT jsonb_build_object(
    'proc', (SELECT jsonb_agg(jsonb_build_array(p.proname, p.prokind, p.prolang, p.proretset, p.prorettype, p.proargtypes::text, p.proallargtypes, p.proargmodes, p.proargnames, p.provolatile, p.proisstrict, p.prosecdef, p.proleakproof, p.proparallel, p.prosupport, p.procost, p.prorows, p.proconfig, p.proowner, p.proacl, p.prosrc, p.prosqlbody IS NULL) ORDER BY p.proname COLLATE "C") FROM pg_proc AS p JOIN pg_namespace AS n ON n.oid=p.pronamespace WHERE n.nspname='integrity_g4'),
    'class', (SELECT jsonb_agg(jsonb_build_array(c.relname, c.relkind, c.relpersistence, c.relrowsecurity, c.relforcerowsecurity, c.relowner, c.relacl, pg_get_viewdef(c.oid, true)) ORDER BY c.relname COLLATE "C") FROM pg_class AS c JOIN pg_namespace AS n ON n.oid=c.relnamespace WHERE n.nspname='integrity_g4'),
    'trigger', (SELECT jsonb_agg(jsonb_build_array(c.relname, t.tgname, t.tgtype, t.tgenabled, t.tgisinternal, encode(t.tgargs,'escape'), p.proname, pg_get_triggerdef(t.oid, false)) ORDER BY c.relname COLLATE "C", t.tgname COLLATE "C") FROM pg_trigger AS t JOIN pg_class AS c ON c.oid=t.tgrelid JOIN pg_namespace AS n ON n.oid=c.relnamespace JOIN pg_proc AS p ON p.oid=t.tgfoid WHERE n.nspname='integrity_g4' AND NOT t.tgisinternal),
    'constraint', (SELECT jsonb_agg(jsonb_build_array(c.relname, x.conname, x.contype, pg_get_constraintdef(x.oid, false)) ORDER BY c.relname COLLATE "C", x.conname COLLATE "C") FROM pg_constraint AS x JOIN pg_class AS c ON c.oid=x.conrelid JOIN pg_namespace AS n ON n.oid=c.relnamespace WHERE n.nspname='integrity_g4'),
    'index', (SELECT jsonb_agg(jsonb_build_array(c.relname, c.relkind, pg_get_indexdef(c.oid)) ORDER BY c.relname COLLATE "C") FROM pg_class AS c JOIN pg_namespace AS n ON n.oid=c.relnamespace WHERE n.nspname='integrity_g4' AND c.relkind IN ('i','I')),
    'attribute', (SELECT jsonb_agg(jsonb_build_array(c.relname, a.attname, a.atttypid, a.attnum, a.attnotnull, a.attisdropped) ORDER BY c.relname COLLATE "C", a.attnum) FROM pg_attribute AS a JOIN pg_class AS c ON c.oid=a.attrelid JOIN pg_namespace AS n ON n.oid=c.relnamespace WHERE n.nspname='integrity_g4' AND a.attnum > 0),
    'type', (SELECT jsonb_agg(jsonb_build_array(t.typname, t.typtype, t.typelem, t.typarray, t.typrelid, t.typowner, t.typacl) ORDER BY t.typname COLLATE "C") FROM pg_type AS t JOIN pg_namespace AS n ON n.oid=t.typnamespace WHERE n.nspname='integrity_g4'),
    'depend', (SELECT jsonb_agg(jsonb_build_array(d.classid, d.objid, d.objsubid, d.refclassid, d.refobjid, d.refobjsubid, d.deptype) ORDER BY d.classid, d.objid, d.objsubid, d.refclassid, d.refobjid, d.refobjsubid, d.deptype) FROM pg_depend AS d WHERE d.objid IN (SELECT p.oid FROM pg_proc AS p JOIN pg_namespace AS n ON n.oid=p.pronamespace WHERE n.nspname='integrity_g4') OR d.refobjid IN (SELECT t.oid FROM pg_type AS t JOIN pg_namespace AS n ON n.oid=t.typnamespace WHERE n.nspname='integrity_g4')),
    'policy', (SELECT jsonb_agg(jsonb_build_array(c.relname, p.polname, p.polcmd, p.polpermissive, p.polroles, pg_get_expr(p.polqual,p.polrelid), pg_get_expr(p.polwithcheck,p.polrelid)) ORDER BY c.relname COLLATE "C", p.polname COLLATE "C") FROM pg_policy AS p JOIN pg_class AS c ON c.oid=p.polrelid JOIN pg_namespace AS n ON n.oid=c.relnamespace WHERE n.nspname='integrity_g4'),
    'era', (SELECT jsonb_agg(to_jsonb(x) ORDER BY x.table_name COLLATE "C", x.era_name COLLATE "C") FROM sql_saga.era AS x WHERE x.table_schema='integrity_g4'),
    'system_time', (SELECT jsonb_agg(to_jsonb(x) ORDER BY x.table_name COLLATE "C") FROM sql_saga.system_time_era AS x WHERE x.table_schema='integrity_g4'),
    'system_versioning', (SELECT jsonb_agg(to_jsonb(x) ORDER BY x.table_name COLLATE "C") FROM sql_saga.system_versioning AS x WHERE x.table_schema='integrity_g4'),
    'unique_keys', (SELECT jsonb_agg(to_jsonb(x) ORDER BY x.table_name COLLATE "C", x.unique_key_name COLLATE "C") FROM sql_saga.unique_keys AS x WHERE x.table_schema='integrity_g4'),
    'foreign_keys', (SELECT jsonb_agg(to_jsonb(x) ORDER BY x.table_name COLLATE "C", x.foreign_key_name COLLATE "C") FROM sql_saga.foreign_keys AS x WHERE x.table_schema='integrity_g4'),
    'updatable_view', (SELECT jsonb_agg(to_jsonb(x) ORDER BY x.view_name COLLATE "C") FROM sql_saga.updatable_view AS x WHERE x.view_schema='integrity_g4')
)
$snapshot_state$;

CREATE FUNCTION integrity_g4.acl_attempt(case_name text, call_sql text, expected_function text)
RETURNS text LANGUAGE plpgsql AS $acl_attempt$
DECLARE actual_state text; actual_message text;
BEGIN
    BEGIN
        EXECUTE 'SET LOCAL ROLE integrity_g4_untrusted';
        EXECUTE call_sql;
        EXECUTE 'RESET ROLE';
        RETURN case_name || ' | unexpected success';
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS actual_state=RETURNED_SQLSTATE, actual_message=MESSAGE_TEXT;
        EXECUTE 'RESET ROLE';
        IF actual_state <> '42501' OR actual_message <> 'permission denied for function ' || expected_function THEN
            RETURN format('%s | unexpected %s: %s', case_name, actual_state, actual_message);
        END IF;
        RETURN format('%s | 42501: %s', case_name, actual_message);
    END;
END;
$acl_attempt$;

CREATE FUNCTION integrity_g4.nondisclosure_attempt(case_name text, expected_message text, forbidden text[])
RETURNS text LANGUAGE plpgsql AS $nondisclosure_attempt$
DECLARE actual_state text; actual_message text; token text;
BEGIN
    BEGIN
        PERFORM sql_saga.check_integrity();
        RETURN case_name || ' | unexpected success';
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS actual_state=RETURNED_SQLSTATE, actual_message=MESSAGE_TEXT;
        IF actual_state <> 'P0001' OR actual_message <> expected_message THEN
            RETURN format('%s | unexpected %s: %s', case_name, actual_state, actual_message);
        END IF;
        FOREACH token IN ARRAY forbidden LOOP
            IF position(token IN actual_message) <> 0 THEN
                RETURN format('%s | disclosed forbidden text', case_name);
            END IF;
        END LOOP;
        RETURN case_name || ' | no disclosure';
    END;
END;
$nondisclosure_attempt$;

CREATE FUNCTION integrity_g4.dependency_restrict_attempt(case_name text, command text, expected_message text)
RETURNS text LANGUAGE plpgsql AS $dependency_restrict_attempt$
DECLARE actual_state text; actual_message text; before_state jsonb;
BEGIN
    before_state := integrity_g4.snapshot_state();
    BEGIN
        EXECUTE command;
        RETURN case_name || ' | unexpected success';
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS actual_state=RETURNED_SQLSTATE, actual_message=MESSAGE_TEXT;
        IF before_state IS DISTINCT FROM integrity_g4.snapshot_state() THEN
            RETURN case_name || ' | refusal was not atomic';
        END IF;
        IF (actual_state, actual_message) IS DISTINCT FROM ('2BP01', expected_message) THEN
            RETURN format('%s | unexpected %s: %s', case_name, actual_state, actual_message);
        END IF;
        RETURN format('%s | 2BP01: %s | atomic', case_name, actual_message);
    END;
END;
$dependency_restrict_attempt$;

CREATE FUNCTION integrity_g4.function_case(case_name text, damage_sql text)
RETURNS text LANGUAGE plpgsql AS $function_case$
DECLARE before_state jsonb; result text;
BEGIN
    EXECUTE damage_sql;
    before_state := integrity_g4.snapshot_state();
    result := integrity_g4.attempt(case_name,
        'sql_saga: integrity violation for system-versioned table "integrity_g4.sv": generated query function role "as_of" definition is noncanonical; repair with rebuild_system_versioning_views');
    IF before_state IS DISTINCT FROM integrity_g4.snapshot_state() THEN
        RETURN case_name || ' | checker changed state';
    END IF;
    BEGIN
        PERFORM sql_saga.rebuild_system_versioning_views('integrity_g4.sv');
        IF integrity_g4.valid(case_name || ' repair') <> case_name || ' repair | ok' THEN
            RETURN case_name || ' | rebuild did not repair';
        END IF;
    EXCEPTION WHEN OTHERS THEN
        RETURN format('%s | rebuild failed %s: %s', case_name, SQLSTATE, SQLERRM);
    END;
    RETURN result;
END;
$function_case$;

CREATE TABLE sv (id integer PRIMARY KEY, tenant text, payload text);
ALTER TABLE sv ENABLE ROW LEVEL SECURITY;
CREATE POLICY sv_tenant ON sv USING (tenant = current_user);
SELECT sql_saga.add_system_versioning('sv');
INSERT INTO sv (id, tenant, payload) VALUES (117, current_user, 'ROWDATA_MARKER_7f3a');
CREATE TABLE a_sv (id integer PRIMARY KEY, payload text);
CREATE TABLE z_sv (id integer PRIMARY KEY, payload text);
SELECT sql_saga.add_system_versioning('a_sv');
SELECT sql_saga.add_system_versioning('z_sv');

CREATE TABLE period_parent (id integer, category text, valid_from date, valid_until date, valid_range daterange);
CREATE TABLE period_child (id integer, parent_id integer, valid_from date, valid_until date, valid_range daterange);
CREATE TABLE period_parent_wrong (id integer, alt_id integer, valid_range daterange,
    UNIQUE (id, valid_range WITHOUT OVERLAPS),
    UNIQUE (alt_id, valid_range WITHOUT OVERLAPS));
CREATE TABLE era_wrong (id integer, valid_from date, valid_until date, valid_range daterange);
SELECT sql_saga.add_era('era_wrong', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_era('period_parent', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('period_parent', ARRAY['id']::name[], 'valid', key_type => 'primary', unique_key_name => 'period_parent_primary');
SELECT sql_saga.add_unique_key('period_parent', ARRAY['id']::name[], 'valid', key_type => 'predicated', predicate => 'category IS NOT NULL', unique_key_name => 'period_parent_predicated');
SELECT sql_saga.add_unique_key('period_parent', ARRAY['category']::name[], 'valid', key_type => 'natural', unique_key_name => 'period_parent_natural', enforce_consistency_with_primary_key => false);
ALTER TABLE period_parent ADD COLUMN alt_id integer;
ALTER TABLE period_parent ADD CONSTRAINT period_parent_alt_temporal UNIQUE (alt_id, valid_range WITHOUT OVERLAPS);
ALTER TABLE period_parent ADD CONSTRAINT period_parent_id_plain UNIQUE (id);
SELECT sql_saga.add_for_portion_of_view('period_parent', 'valid');
SELECT sql_saga.add_era('period_child', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('period_child', ARRAY['id']::name[], 'valid', key_type => 'primary', unique_key_name => 'period_child_primary');
SELECT sql_saga.add_foreign_key('period_child', ARRAY['parent_id']::name[], 'period_parent', ARRAY['id']::name[]);

\o
SELECT integrity_g4.valid('complete inventory baseline');

-- This exact name+identity-argument list was derived from a fresh v1.1 installation.
-- New 1.2 inspection and formatting helpers are private.
-- One intentional 1.2 change: add_system_versioning gained a trailing history_table_must_exist boolean
-- (DEFAULT false, so every 1.1 call form still works); its identity below is the 1.2 one. One intentional
-- 1.2 addition: sync_all_history_policies(), the final command of a parallel restore (README). Everything
-- else must match 1.1 exactly.
CREATE TEMP TABLE integrity_v11_public_function (identity text PRIMARY KEY) ON COMMIT DROP;
INSERT INTO integrity_v11_public_function VALUES
('__find_or_create_fk_index(p_fk_table_oid regclass, p_fk_column_names name[], p_fk_era_row sql_saga.era, p_type sql_saga.fg_type, p_create_index boolean)'),
('__internal_add_system_time_era(table_oid regclass, range_column_name name, infinity_check_constraint name, generated_always_trigger name, write_history_trigger name, truncate_trigger name, excluded_column_names name[])'),
('__internal_check_history_columns(table_oid oid, history_table_oid oid, without_columns name[])'),
('__internal_column_type_sql(type_oid oid, typmod integer, collation_oid oid)'),
('__internal_ddl_command_affects_managed_object()'),
('__internal_deny_history(table_oid oid)'),
('__internal_display_name(object_name regclass)'),
('__internal_generate_pk_consistency_constraints(p_unique_key_name name, p_natural_key_columns name[], p_mutually_exclusive_columns name[], p_primary_key_column name)'),
('__internal_history_base_select_policy_name()'),
('__internal_history_copy_name(base_policy_name name)'),
('__internal_history_family(rel oid)'),
('__internal_history_gated_name(history_table_name name)'),
('__internal_history_gated_view_sql(table_oid oid, history_table_oid oid, columns text)'),
('__internal_history_policies_synchronized(table_oid oid, history_table_oid oid)'),
('__internal_history_policy_copies(table_oid oid)'),
('__internal_history_policy_drift(table_oid oid, history_table_oid oid, OUT missing integer, OUT stale integer, OUT unmanaged integer)'),
('__internal_history_reader_policy_name()'),
('__internal_installation_token()'),
('__internal_is_managed_history_policy(policy_name name)'),
('__internal_make_name(resizable text[], fixed text, separator text, extra integer)'),
('__internal_make_updatable_view_name(table_name name, era_name name, view_type_suffix text)'),
('__internal_policy_missing_history_column(base_policy_oid oid, history_table_oid oid)'),
('__internal_policy_role_list(roles oid[])'),
('__internal_qualified_name(rel oid)'),
('__internal_rebuild_system_versioning_views(table_oid oid, without_columns name[])'),
('__internal_rebuilding_system_versioning_views(table_oid oid)'),
('__internal_reconcile_system_versioning_family(table_oid regclass, without_columns name[])'),
('__internal_serialize(table_name regclass)'),
('__internal_sync_history_policies(table_oid oid, dropped_base_policies name[])'),
('add_current_view(table_oid regclass, era_name name, delete_mode name, current_func_name text)'),
('add_era(table_oid regclass, range_column_name name, era_name name, synchronize_columns boolean, valid_from_column_name name, valid_until_column_name name, valid_to_column_name name, create_columns boolean, add_defaults boolean, add_bounds_check boolean, range_type regtype, bounds_check_constraint name, ephemeral_columns name[])'),
('add_for_portion_of_view(table_oid regclass, era_name name, ephemeral_columns name[])'),
('add_foreign_key(fk_table_oid regclass, fk_column_names name[], pk_table_oid regclass, pk_column_names name[], fk_era_name name, match_type sql_saga.fk_match_types, update_action sql_saga.fk_actions, delete_action sql_saga.fk_actions, foreign_key_name name, create_index boolean)'),
('add_regular_foreign_key(fk_table_oid regclass, fk_column_names name[], unique_key_name name, match_type sql_saga.fk_match_types, update_action sql_saga.fk_actions, delete_action sql_saga.fk_actions, foreign_key_name name, fk_check_constraint name, fk_helper_function text, uk_update_trigger name, uk_delete_trigger name, create_index boolean)'),
('add_synchronize_temporal_columns_trigger(IN table_oid regclass, IN era_name name)'),
('add_system_versioning(table_oid regclass, history_table_name name, view_name name, function_as_of_name name, function_between_name name, function_between_symmetric_name name, function_from_to_name name, history_table_must_exist boolean)'),
('add_temporal_foreign_key(fk_table_oid regclass, fk_column_names name[], fk_era_name name, unique_key_name name, match_type sql_saga.fk_match_types, update_action sql_saga.fk_actions, delete_action sql_saga.fk_actions, foreign_key_name name, uk_update_trigger name, uk_delete_trigger name, create_index boolean)'),
('add_unique_key(table_oid regclass, column_names name[], era_name name, key_type sql_saga.unique_key_type, enforce_consistency_with_primary_key boolean, unique_key_name name, unique_constraint name, exclude_constraint name, predicate text, mutually_exclusive_columns name[])'),
('covers_without_gaps(anyrange, anyrange)'),
('covers_without_gaps_finalfn(internal, anyrange, anyrange)'),
('covers_without_gaps_transfn(internal, anyrange, anyrange)'),
('disable_temporal_triggers(VARIADIC p_table_oids regclass[])'),
('drop_current_view(table_oid regclass, era_name name, drop_behavior sql_saga.drop_behavior)'),
('drop_era(table_oid regclass, era_name name, drop_behavior sql_saga.drop_behavior, cleanup boolean)'),
('drop_for_portion_of_view(table_oid regclass, era_name name, drop_behavior sql_saga.drop_behavior, cleanup boolean)'),
('drop_foreign_key(table_oid regclass, column_names name[], era_name name, drop_behavior sql_saga.drop_behavior, drop_index boolean)'),
('drop_foreign_key_by_name(table_oid regclass, key_name name, drop_index boolean)'),
('drop_synchronize_temporal_columns_trigger(IN table_oid regclass, IN era_name name)'),
('drop_system_time_era(table_oid regclass, drop_behavior sql_saga.drop_behavior, cleanup boolean)'),
('drop_system_versioning(table_oid regclass, drop_behavior sql_saga.drop_behavior, cleanup boolean)'),
('drop_unique_key(table_oid regclass, column_names name[], era_name name, drop_behavior sql_saga.drop_behavior, cleanup boolean)'),
('drop_unique_key_by_name(table_oid regclass, key_name name, drop_behavior sql_saga.drop_behavior, cleanup boolean)'),
('enable_temporal_triggers(VARIADIC p_table_oids regclass[])'),
('first(anyelement)'),
('first_sfunc(agg_state anyelement, next_val anyelement)'),
('get_allen_relation(x_from anycompatible, x_until anycompatible, y_from anycompatible, y_until anycompatible)'),
('manage_temporal_fk_triggers(IN p_action sql_saga.trigger_action, VARIADIC p_table_oids regclass[])'),
('native_library_version()'),
('rebuild_system_versioning_views(table_oid regclass, without_columns name[])'),
('set_era_ephemeral_columns(table_class regclass, era_name name, ephemeral_columns name[])'),
('set_system_time_era_excluded_columns(table_oid regclass, excluded_column_names name[])'),
('sync_all_history_policies()'),
('sync_history_policies(table_oid regclass)'),
('temporal_merge(IN target_table regclass, IN source_table regclass, IN primary_identity_columns text[], IN mode sql_saga.temporal_merge_mode, IN era_name name, IN row_id_column name, IN founding_id_column name, IN update_source_with_identity boolean, IN natural_identity_columns text[], IN delete_mode sql_saga.temporal_merge_delete_mode, IN update_source_with_feedback boolean, IN feedback_status_column name, IN feedback_status_key name, IN feedback_error_column name, IN feedback_error_key name, IN ephemeral_columns text[], IN delay_constraints boolean)'),
('temporal_merge_drop_cache()'),
('temporal_merge_drop_temp_tables()'),
('temporal_merge_execute(IN target_table regclass, IN source_table regclass, IN identity_columns text[], IN mode sql_saga.temporal_merge_mode, IN era_name name, IN row_id_column name, IN founding_id_column name, IN update_source_with_identity boolean, IN lookup_columns text[], IN delete_mode sql_saga.temporal_merge_delete_mode, IN update_source_with_feedback boolean, IN feedback_status_column name, IN feedback_status_key name, IN feedback_error_column name, IN feedback_error_key name, IN ephemeral_columns text[], IN delay_constraints boolean)'),
('temporal_merge_executor_introspect(target_table oid, source_table oid, identity_columns text[], lookup_columns text[], era_name text, ephemeral_columns text[], founding_id_column text, row_id_column text)'),
('temporal_merge_native_cache_reset()'),
('temporal_merge_native_cache_stats(OUT stat_name text, OUT stat_value bigint)'),
('temporal_merge_plan(target_table regclass, source_table regclass, mode sql_saga.temporal_merge_mode, era_name name, identity_columns text[], row_id_column name, founding_id_column name, delete_mode sql_saga.temporal_merge_delete_mode, lookup_keys jsonb, ephemeral_columns text[], p_log_trace boolean, p_log_sql boolean)'),
('temporal_merge_plan_native(target_table oid, source_table oid, mode text, era_name text, identity_columns text[], row_id_column text, founding_id_column text, delete_mode text, lookup_keys jsonb, ephemeral_columns text[], p_log_trace boolean, p_log_sql boolean)');

SELECT p.prorettype IN ('event_trigger'::regtype, 'trigger'::regtype) AS health_checks_is_noncallable_trigger_function
FROM pg_proc AS p
JOIN pg_namespace AS n ON n.oid=p.pronamespace
WHERE n.nspname='sql_saga' AND p.proname='health_checks';

SELECT ARRAY(
           SELECT identity
           FROM (
               SELECT p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')' AS identity
               FROM pg_proc AS p
               JOIN pg_namespace AS n ON n.oid=p.pronamespace
               WHERE n.nspname='sql_saga'
                 AND p.prorettype NOT IN ('event_trigger'::regtype, 'trigger'::regtype)
                 AND has_function_privilege('public', p.oid, 'EXECUTE')) AS public_function
           ORDER BY identity COLLATE "C")
       = ARRAY(SELECT identity FROM integrity_v11_public_function ORDER BY identity COLLATE "C")
       AS public_function_set_matches_v11;

SELECT integrity_g4.acl_attempt(
    'canonical generator private',
    $$SELECT sql_saga.__internal_system_versioning_query_functions('integrity_g4.sv'::regclass::oid)$$,
    '__internal_system_versioning_query_functions');
SELECT integrity_g4.acl_attempt(
    'function attribution helper private',
    $$SELECT sql_saga.__internal_system_versioning_function_is_attributable(NULL, NULL, NULL)$$,
    '__internal_system_versioning_function_is_attributable');
SELECT integrity_g4.acl_attempt(
    'trigger attribution helper private',
    $$SELECT sql_saga.__internal_trigger_is_attributable(NULL, NULL, NULL, NULL, NULL)$$,
    '__internal_trigger_is_attributable');
SELECT integrity_g4.acl_attempt(
    'view attribution helper private',
    $$SELECT sql_saga.__internal_system_versioning_view_is_attributable(NULL, NULL, NULL)$$,
    '__internal_system_versioning_view_is_attributable');
SELECT integrity_g4.acl_attempt(
    'integrity checker private',
    $$SELECT sql_saga.check_integrity()$$,
    'check_integrity');
SELECT integrity_g4.acl_attempt(
    'two-name display helper private',
    $$SELECT sql_saga.__internal_display_name('integrity_g4'::name, 'sv'::name)$$,
    '__internal_display_name');

-- 5.3 item 1: base relation kind, persistence, and catalogue coherence.
SAVEPOINT wrong_base_kind;
SET session_replication_role = replica;
DROP TABLE era_wrong CASCADE;
CREATE VIEW era_wrong AS SELECT 1 AS id, current_date AS valid_from, current_date + 1 AS valid_until, daterange(current_date,current_date+1) AS valid_range;
RESET session_replication_role;
SELECT integrity_g4.attempt('catalogued base wrong relation kind', 'sql_saga: integrity violation for table "integrity_g4.era_wrong": catalogued base relation kind or persistence is noncanonical; repair with drop_era and add_era');
ROLLBACK TO SAVEPOINT wrong_base_kind;
RELEASE SAVEPOINT wrong_base_kind;

SAVEPOINT wrong_base_persistence;
SET session_replication_role = replica;
ALTER TABLE era_wrong SET UNLOGGED;
RESET session_replication_role;
SELECT integrity_g4.attempt('catalogued base wrong persistence', 'sql_saga: integrity violation for table "integrity_g4.era_wrong": catalogued base relation kind or persistence is noncanonical; repair with drop_era and add_era');
ROLLBACK TO SAVEPOINT wrong_base_persistence;
RELEASE SAVEPOINT wrong_base_persistence;

SAVEPOINT incoherent_catalog_relationship;
UPDATE sql_saga.era SET era_name='wrong_valid' WHERE table_schema='integrity_g4' AND table_name='era_wrong';
SELECT integrity_g4.attempt('incoherent era catalogue relationship', 'sql_saga: integrity violation for table "integrity_g4.era_wrong": catalogue relationships are incoherent; repair with drop_era and add_era');
ROLLBACK TO SAVEPOINT incoherent_catalog_relationship;
RELEASE SAVEPOINT incoherent_catalog_relationship;

-- 5.3 item 2: era column/check/synchronization structure.
SAVEPOINT wrong_era_column_type;
UPDATE pg_attribute SET atttypid='text'::regtype WHERE attrelid='integrity_g4.era_wrong'::regclass AND attname='valid_range';
SELECT integrity_g4.attempt('wrong era range column type', 'sql_saga: integrity violation for era "valid" on table "integrity_g4.era_wrong": column or range type metadata is noncanonical; repair with drop_era and add_era');
ROLLBACK TO SAVEPOINT wrong_era_column_type;
RELEASE SAVEPOINT wrong_era_column_type;

SAVEPOINT wrong_era_check;
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT bounds_check_constraint INTO n FROM sql_saga.era WHERE table_schema='integrity_g4' AND table_name='era_wrong';
    EXECUTE format('ALTER TABLE integrity_g4.era_wrong DROP CONSTRAINT %I', n);
    EXECUTE format('ALTER TABLE integrity_g4.era_wrong ADD CONSTRAINT %I CHECK (true)', n);
END $damage$;
RESET session_replication_role;
SELECT integrity_g4.attempt('wrong named era check', 'sql_saga: integrity violation for era "valid" on table "integrity_g4.era_wrong": named check or synchronization object is noncanonical; repair with drop_era and add_era');
ROLLBACK TO SAVEPOINT wrong_era_check;
RELEASE SAVEPOINT wrong_era_check;

SAVEPOINT wrong_era_sync_trigger;
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT sync_temporal_trg_name INTO n FROM sql_saga.era WHERE table_schema='integrity_g4' AND table_name='era_wrong';
    EXECUTE format('DROP TRIGGER %I ON integrity_g4.era_wrong', n);
    EXECUTE format('CREATE TRIGGER %I BEFORE INSERT ON integrity_g4.era_wrong FOR EACH ROW EXECUTE FUNCTION suppress_redundant_updates_trigger()', n);
END $damage$;
RESET session_replication_role;
SELECT integrity_g4.attempt('wrong era synchronization trigger', 'sql_saga: integrity violation for era "valid" on table "integrity_g4.era_wrong": named check or synchronization object is noncanonical; repair with drop_era and add_era');
ROLLBACK TO SAVEPOINT wrong_era_sync_trigger;
RELEASE SAVEPOINT wrong_era_sync_trigger;

-- 5.3 item 9: a stored identity resolves, but to a same-named wrong object.
SAVEPOINT wrong_stored_function_identity;
SET session_replication_role = replica;
ALTER FUNCTION sv__as_of(timestamptz) RENAME TO sv__as_of_original;
CREATE FUNCTION sv__as_of(p_at timestamptz) RETURNS SETOF sv_with_history LANGUAGE sql STABLE AS $wrong$ SELECT * FROM integrity_g4.sv_with_history WHERE false $wrong$;
RESET session_replication_role;
SELECT integrity_g4.attempt('stored function identity resolves wrong object', 'sql_saga: integrity violation for system-versioned table "integrity_g4.sv": generated query function role "as_of" definition is noncanonical; repair with rebuild_system_versioning_views');
ROLLBACK TO SAVEPOINT wrong_stored_function_identity;
RELEASE SAVEPOINT wrong_stored_function_identity;

-- SYSTEM_TIME trigger identity, not merely its name, is invariant.
SAVEPOINT wrong_system_time_trigger;
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT generated_always_trigger INTO n FROM sql_saga.system_time_era WHERE table_schema='integrity_g4' AND table_name='sv';
    EXECUTE format('DROP TRIGGER %I ON integrity_g4.sv', n);
    EXECUTE format('CREATE TRIGGER %I BEFORE INSERT ON integrity_g4.sv FOR EACH ROW EXECUTE FUNCTION suppress_redundant_updates_trigger()', n);
END $damage$;
RESET session_replication_role;
SELECT integrity_g4.attempt('wrong-function SYSTEM_TIME trigger', 'sql_saga: integrity violation for table "integrity_g4.sv": SYSTEM_TIME trigger definition is noncanonical; repair with drop_system_versioning and add_system_versioning');
ROLLBACK TO SAVEPOINT wrong_system_time_trigger;
RELEASE SAVEPOINT wrong_system_time_trigger;

-- Unique and foreign-key objects must structurally implement their catalogue rows.
SAVEPOINT wrong_unique;
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT exclude_constraint INTO n FROM sql_saga.unique_keys WHERE unique_key_name='period_parent_predicated';
    EXECUTE format('ALTER TABLE integrity_g4.period_parent DROP CONSTRAINT %I', n);
    EXECUTE format('ALTER TABLE integrity_g4.period_parent ADD CONSTRAINT %I CHECK (true)', n);
END $damage$;
RESET session_replication_role;
SELECT integrity_g4.attempt('wrong temporal unique constraint', 'sql_saga: integrity violation for unique key "period_parent_predicated" on table "integrity_g4.period_parent": managed constraint or index is noncanonical; repair with drop_unique_key and add_unique_key');
ROLLBACK TO SAVEPOINT wrong_unique;
RELEASE SAVEPOINT wrong_unique;

SAVEPOINT wrong_unique_index;
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT unique_constraint INTO n FROM sql_saga.unique_keys WHERE unique_key_name='period_parent_natural';
    EXECUTE format('ALTER TABLE integrity_g4.period_parent DROP CONSTRAINT %I', n);
    EXECUTE format('CREATE INDEX %I ON integrity_g4.period_parent (category)', n);
END $damage$;
RESET session_replication_role;
SELECT integrity_g4.attempt('wrong temporal unique index', 'sql_saga: integrity violation for unique key "period_parent_natural" on table "integrity_g4.period_parent": managed constraint or index is noncanonical; repair with drop_unique_key and add_unique_key');
ROLLBACK TO SAVEPOINT wrong_unique_index;
RELEASE SAVEPOINT wrong_unique_index;

SAVEPOINT wrong_fk;
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT foreign_key_name INTO n FROM sql_saga.foreign_keys WHERE table_schema='integrity_g4' AND table_name='period_child';
    EXECUTE format('ALTER TABLE integrity_g4.period_child DROP CONSTRAINT %I', n);
    EXECUTE format('ALTER TABLE integrity_g4.period_child ADD CONSTRAINT %I CHECK (true)', n);
END $damage$;
RESET session_replication_role;
SELECT integrity_g4.attempt('wrong temporal foreign-key constraint', 'sql_saga: integrity violation for foreign key on table "integrity_g4.period_child": managed constraint or trigger is noncanonical; repair with drop_foreign_key and add_foreign_key');
ROLLBACK TO SAVEPOINT wrong_fk;
RELEASE SAVEPOINT wrong_fk;

SAVEPOINT wrong_fk_referenced_table;
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT foreign_key_name INTO n FROM sql_saga.foreign_keys WHERE table_schema='integrity_g4' AND table_name='period_child';
    EXECUTE format('ALTER TABLE integrity_g4.period_child DROP CONSTRAINT %I', n);
    EXECUTE format('ALTER TABLE integrity_g4.period_child ADD CONSTRAINT %I FOREIGN KEY (parent_id, PERIOD valid_range) REFERENCES integrity_g4.period_parent_wrong (id, PERIOD valid_range) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE', n);
END $damage$;
RESET session_replication_role;
SELECT integrity_g4.attempt('native temporal FK references wrong table', 'sql_saga: integrity violation for foreign key on table "integrity_g4.period_child": managed constraint or trigger is noncanonical; repair with drop_foreign_key and add_foreign_key');
ROLLBACK TO SAVEPOINT wrong_fk_referenced_table;
RELEASE SAVEPOINT wrong_fk_referenced_table;

SAVEPOINT wrong_fk_referenced_columns;
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT foreign_key_name INTO n FROM sql_saga.foreign_keys WHERE table_schema='integrity_g4' AND table_name='period_child';
    EXECUTE format('ALTER TABLE integrity_g4.period_child DROP CONSTRAINT %I', n);
    EXECUTE format('ALTER TABLE integrity_g4.period_child ADD CONSTRAINT %I FOREIGN KEY (parent_id, PERIOD valid_range) REFERENCES integrity_g4.period_parent (alt_id, PERIOD valid_range) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE', n);
END $damage$;
RESET session_replication_role;
SELECT integrity_g4.attempt('native temporal FK references wrong columns', 'sql_saga: integrity violation for foreign key on table "integrity_g4.period_child": managed constraint or trigger is noncanonical; repair with drop_foreign_key and add_foreign_key');
ROLLBACK TO SAVEPOINT wrong_fk_referenced_columns;
RELEASE SAVEPOINT wrong_fk_referenced_columns;

SAVEPOINT wrong_fk_without_period;
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT foreign_key_name INTO n FROM sql_saga.foreign_keys WHERE table_schema='integrity_g4' AND table_name='period_child';
    EXECUTE format('ALTER TABLE integrity_g4.period_child DROP CONSTRAINT %I', n);
    EXECUTE format('ALTER TABLE integrity_g4.period_child ADD CONSTRAINT %I FOREIGN KEY (parent_id) REFERENCES integrity_g4.period_parent (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION DEFERRABLE', n);
END $damage$;
RESET session_replication_role;
SELECT integrity_g4.attempt('native temporal FK lacks PERIOD', 'sql_saga: integrity violation for foreign key on table "integrity_g4.period_child": managed constraint or trigger is noncanonical; repair with drop_foreign_key and add_foreign_key');
ROLLBACK TO SAVEPOINT wrong_fk_without_period;
RELEASE SAVEPOINT wrong_fk_without_period;

SAVEPOINT wrong_fk_actions;
SET session_replication_role = replica;
UPDATE pg_constraint
SET condeferrable=false, condeferred=false, confupdtype='r', confdeltype='c'
WHERE conrelid='integrity_g4.period_child'::regclass
  AND conname=(SELECT foreign_key_name FROM sql_saga.foreign_keys WHERE table_schema='integrity_g4' AND table_name='period_child');
RESET session_replication_role;
SELECT integrity_g4.attempt('native temporal FK has wrong deferrability and actions', 'sql_saga: integrity violation for foreign key on table "integrity_g4.period_child": managed constraint or trigger is noncanonical; repair with drop_foreign_key and add_foreign_key');
SET LOCAL client_min_messages = notice;
SELECT sql_saga.drop_foreign_key_by_name('integrity_g4.period_child', (SELECT foreign_key_name FROM sql_saga.foreign_keys WHERE table_schema='integrity_g4' AND table_name='period_child'));
SET LOCAL client_min_messages = warning;
SELECT EXISTS (SELECT FROM pg_constraint AS c WHERE c.conrelid='integrity_g4.period_child'::regclass AND c.conname LIKE 'period_child_parent_id%') AS noncanonical_native_fk_survives_tolerant_drop;
DO $repair$ DECLARE n name; BEGIN
    SELECT c.conname INTO n FROM pg_constraint AS c
    WHERE c.conrelid='integrity_g4.period_child'::regclass AND c.conname LIKE 'period_child_parent_id%';
    IF n IS NOT NULL THEN EXECUTE format('ALTER TABLE integrity_g4.period_child DROP CONSTRAINT %I', n); END IF;
END $repair$;
SELECT sql_saga.add_foreign_key('integrity_g4.period_child', ARRAY['parent_id']::name[], 'integrity_g4.period_parent', ARRAY['id']::name[]);
SELECT integrity_g4.valid('native temporal FK documented API repair');
ROLLBACK TO SAVEPOINT wrong_fk_actions;
RELEASE SAVEPOINT wrong_fk_actions;

-- Updatable view identifier and exact trigger attachment/function are invariant.
SAVEPOINT wrong_updatable_trigger;
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT trigger_name INTO n FROM sql_saga.updatable_view WHERE view_schema='integrity_g4' AND view_name='period_parent__for_portion_of_valid';
    EXECUTE format('DROP TRIGGER %I ON integrity_g4.period_parent__for_portion_of_valid', n);
    EXECUTE format('CREATE TRIGGER %I INSTEAD OF INSERT ON integrity_g4.period_parent__for_portion_of_valid FOR EACH ROW EXECUTE FUNCTION suppress_redundant_updates_trigger()', n);
END $damage$;
RESET session_replication_role;
SELECT integrity_g4.attempt('wrong updatable-view trigger', 'sql_saga: integrity violation for updatable view "integrity_g4.period_parent__for_portion_of_valid" of table "integrity_g4.period_parent": identifier column or trigger definition is noncanonical; repair with drop_for_portion_of_view and add_for_portion_of_view');
ROLLBACK TO SAVEPOINT wrong_updatable_trigger;
RELEASE SAVEPOINT wrong_updatable_trigger;

SAVEPOINT missing_identifier_surviving_trigger;
SET session_replication_role = replica;
ALTER VIEW period_parent__for_portion_of_valid RENAME COLUMN id TO wrong_id;
RESET session_replication_role;
SELECT integrity_g4.attempt('missing identifier with surviving trigger', 'sql_saga: integrity violation for updatable view "integrity_g4.period_parent__for_portion_of_valid" of table "integrity_g4.period_parent": identifier column or trigger definition is noncanonical; repair with drop_for_portion_of_view and add_for_portion_of_view');
ROLLBACK TO SAVEPOINT missing_identifier_surviving_trigger;
RELEASE SAVEPOINT missing_identifier_surviving_trigger;

-- History-family shape, access, RLS, owner, ACL, and policy invariants are isolated.
SAVEPOINT wrong_public_view_shape;
SET session_replication_role = replica;
CREATE OR REPLACE VIEW sv_with_history AS SELECT id, tenant, 'secret_view_marker'::text AS payload, system_valid_range FROM sv;
RESET session_replication_role;
SELECT integrity_g4.nondisclosure_attempt('wrong public history view shape', 'sql_saga: integrity violation for system-versioned table "integrity_g4.sv": history family definition is noncanonical; repair with rebuild_system_versioning_views', ARRAY['ROWDATA_MARKER_7f3a','secret_view_marker','SELECT']);
ROLLBACK TO SAVEPOINT wrong_public_view_shape;
RELEASE SAVEPOINT wrong_public_view_shape;

SAVEPOINT wrong_gated_view_shape;
SET session_replication_role = replica;
CREATE OR REPLACE VIEW sv_history_gated AS SELECT * FROM sv_history;
RESET session_replication_role;
SELECT integrity_g4.attempt('wrong gated history view shape', 'sql_saga: integrity violation for system-versioned table "integrity_g4.sv": history family definition is noncanonical; repair with rebuild_system_versioning_views');
ROLLBACK TO SAVEPOINT wrong_gated_view_shape;
RELEASE SAVEPOINT wrong_gated_view_shape;

SAVEPOINT wrong_view_options;
SET session_replication_role = replica;
ALTER VIEW sv_with_history SET (security_barrier = true);
RESET session_replication_role;
SELECT integrity_g4.attempt('wrong public history view options', 'sql_saga: integrity violation for system-versioned table "integrity_g4.sv": history family definition is noncanonical; repair with rebuild_system_versioning_views');
ROLLBACK TO SAVEPOINT wrong_view_options;
RELEASE SAVEPOINT wrong_view_options;

SAVEPOINT wrong_history_rls;
SET session_replication_role = replica;
ALTER TABLE sv_history DISABLE ROW LEVEL SECURITY;
RESET session_replication_role;
SELECT integrity_g4.attempt('wrong history RLS flags', 'sql_saga: integrity violation for system-versioned table "integrity_g4.sv": history family definition is noncanonical; repair with sync_history_policies or rebuild_system_versioning_views');
ROLLBACK TO SAVEPOINT wrong_history_rls;
RELEASE SAVEPOINT wrong_history_rls;

SAVEPOINT wrong_history_owner;
SET session_replication_role = replica;
ALTER TABLE sv_history OWNER TO integrity_g4_other;
RESET session_replication_role;
SELECT integrity_g4.attempt('wrong history owner', 'sql_saga: integrity violation for system-versioned table "integrity_g4.sv": history family definition is noncanonical; repair with drop_system_versioning and add_system_versioning');
ROLLBACK TO SAVEPOINT wrong_history_owner;
RELEASE SAVEPOINT wrong_history_owner;

SAVEPOINT wrong_history_acl;
SET session_replication_role = replica;
GRANT SELECT ON sv_history TO integrity_g4_untrusted;
RESET session_replication_role;
SELECT integrity_g4.nondisclosure_attempt('wrong history ACL', 'sql_saga: integrity violation for system-versioned table "integrity_g4.sv": history family definition is noncanonical; repair with rebuild_system_versioning_views', ARRAY['ROWDATA_MARKER_7f3a','integrity_g4_untrusted','=r']);
ROLLBACK TO SAVEPOINT wrong_history_acl;
RELEASE SAVEPOINT wrong_history_acl;

SAVEPOINT wrong_history_policy_expression;
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT p.polname INTO n FROM pg_policy AS p WHERE p.polrelid='integrity_g4.sv_history'::regclass ORDER BY p.polname COLLATE "C" LIMIT 1;
    EXECUTE format('DROP POLICY %I ON integrity_g4.sv_history', n);
    EXECUTE format('CREATE POLICY %I ON integrity_g4.sv_history USING (tenant = %L)', n, 'secret_policy_marker');
END $damage$;
RESET session_replication_role;
SELECT integrity_g4.nondisclosure_attempt('wrong history policy expression', 'sql_saga: integrity violation for system-versioned table "integrity_g4.sv": managed history policy set is inconsistent; repair with sync_history_policies', ARRAY['ROWDATA_MARKER_7f3a','secret_policy_marker','tenant =']);
ROLLBACK TO SAVEPOINT wrong_history_policy_expression;
RELEASE SAVEPOINT wrong_history_policy_expression;

-- One isolated case for every generated-function property in design 5.3 item 8.
SAVEPOINT fn_kind;
SELECT integrity_g4.function_case('function kind', $$UPDATE pg_proc SET prokind='p' WHERE oid='integrity_g4.sv__as_of(timestamptz)'::regprocedure$$);
ROLLBACK TO SAVEPOINT fn_kind;
RELEASE SAVEPOINT fn_kind;
SAVEPOINT fn_args;
SELECT integrity_g4.function_case('argument vector', $$UPDATE pg_proc SET proargnames=ARRAY['wrong_name'] WHERE oid='integrity_g4.sv__as_of(timestamptz)'::regprocedure$$);
ROLLBACK TO SAVEPOINT fn_args;
RELEASE SAVEPOINT fn_args;
SAVEPOINT fn_retset;
SELECT integrity_g4.function_case('result-set behavior', $$UPDATE pg_proc SET proretset=false WHERE oid='integrity_g4.sv__as_of(timestamptz)'::regprocedure$$);
ROLLBACK TO SAVEPOINT fn_retset;
RELEASE SAVEPOINT fn_retset;
SAVEPOINT fn_language;
SELECT integrity_g4.function_case('language', $$UPDATE pg_proc SET prolang=(SELECT oid FROM pg_language WHERE lanname='plpgsql') WHERE oid='integrity_g4.sv__as_of(timestamptz)'::regprocedure$$);
ROLLBACK TO SAVEPOINT fn_language;
RELEASE SAVEPOINT fn_language;
SAVEPOINT fn_volatility;
SELECT integrity_g4.function_case('volatility', $$ALTER FUNCTION integrity_g4.sv__as_of(timestamptz) IMMUTABLE$$);
ROLLBACK TO SAVEPOINT fn_volatility;
RELEASE SAVEPOINT fn_volatility;
SAVEPOINT fn_strict;
SELECT integrity_g4.function_case('strictness', $$ALTER FUNCTION integrity_g4.sv__as_of(timestamptz) STRICT$$);
ROLLBACK TO SAVEPOINT fn_strict;
RELEASE SAVEPOINT fn_strict;
SAVEPOINT fn_security;
SELECT integrity_g4.function_case('security definer', $$ALTER FUNCTION integrity_g4.sv__as_of(timestamptz) SECURITY DEFINER$$);
ROLLBACK TO SAVEPOINT fn_security;
RELEASE SAVEPOINT fn_security;
SAVEPOINT fn_leakproof;
SELECT integrity_g4.function_case('leakproof', $$ALTER FUNCTION integrity_g4.sv__as_of(timestamptz) LEAKPROOF$$);
ROLLBACK TO SAVEPOINT fn_leakproof;
RELEASE SAVEPOINT fn_leakproof;
SAVEPOINT fn_parallel;
SELECT integrity_g4.function_case('parallel', $$ALTER FUNCTION integrity_g4.sv__as_of(timestamptz) PARALLEL SAFE$$);
ROLLBACK TO SAVEPOINT fn_parallel;
RELEASE SAVEPOINT fn_parallel;
SAVEPOINT fn_support;
SELECT integrity_g4.function_case('support function', $$UPDATE pg_proc SET prosupport='textlike_support'::regproc WHERE oid='integrity_g4.sv__as_of(timestamptz)'::regprocedure$$);
ROLLBACK TO SAVEPOINT fn_support;
RELEASE SAVEPOINT fn_support;
SAVEPOINT fn_cost;
SELECT integrity_g4.function_case('cost', $$ALTER FUNCTION integrity_g4.sv__as_of(timestamptz) COST 99$$);
ROLLBACK TO SAVEPOINT fn_cost;
RELEASE SAVEPOINT fn_cost;
SAVEPOINT fn_rows;
SELECT integrity_g4.function_case('rows estimate', $$ALTER FUNCTION integrity_g4.sv__as_of(timestamptz) ROWS 99$$);
ROLLBACK TO SAVEPOINT fn_rows;
RELEASE SAVEPOINT fn_rows;
SAVEPOINT fn_config;
ALTER FUNCTION integrity_g4.sv__as_of(timestamptz) SET secret.setting='secret_config_marker';
SELECT integrity_g4.nondisclosure_attempt('configuration', 'sql_saga: integrity violation for system-versioned table "integrity_g4.sv": generated query function role "as_of" definition is noncanonical; repair with rebuild_system_versioning_views', ARRAY['ROWDATA_MARKER_7f3a','secret_config_marker','secret.setting']);
ROLLBACK TO SAVEPOINT fn_config;
RELEASE SAVEPOINT fn_config;
SAVEPOINT fn_owner;
SELECT integrity_g4.function_case('owner', $$ALTER FUNCTION integrity_g4.sv__as_of(timestamptz) OWNER TO integrity_g4_other$$);
ROLLBACK TO SAVEPOINT fn_owner;
RELEASE SAVEPOINT fn_owner;
SAVEPOINT fn_acl;
GRANT EXECUTE ON FUNCTION integrity_g4.sv__as_of(timestamptz) TO integrity_g4_untrusted;
SELECT integrity_g4.nondisclosure_attempt('ACL', 'sql_saga: integrity violation for system-versioned table "integrity_g4.sv": generated query function role "as_of" definition is noncanonical; repair with rebuild_system_versioning_views', ARRAY['ROWDATA_MARKER_7f3a','integrity_g4_untrusted','=X']);
ROLLBACK TO SAVEPOINT fn_acl;
RELEASE SAVEPOINT fn_acl;
SAVEPOINT fn_sqlbody;
SELECT integrity_g4.function_case('parsed SQL body', $$CREATE OR REPLACE FUNCTION integrity_g4.sv__as_of(p_at timestamptz) RETURNS SETOF integrity_g4.sv_with_history LANGUAGE sql STABLE BEGIN ATOMIC SELECT * FROM integrity_g4.sv_with_history WHERE system_valid_range @> p_at; END$$);
ROLLBACK TO SAVEPOINT fn_sqlbody;
RELEASE SAVEPOINT fn_sqlbody;

SAVEPOINT fn_argtypes;
UPDATE pg_proc SET proargtypes='23'::oidvector WHERE oid='integrity_g4.sv__as_of(timestamptz)'::regprocedure;
SELECT integrity_g4.attempt('actual argument type vector', 'sql_saga: integrity violation for system-versioned table "integrity_g4.sv": generated query function role "as_of" is missing; repair with rebuild_system_versioning_views');
ROLLBACK TO SAVEPOINT fn_argtypes;
RELEASE SAVEPOINT fn_argtypes;

SAVEPOINT fn_argmodes;
UPDATE pg_proc SET proallargtypes=ARRAY['timestamptz'::regtype]::oid[], proargmodes=ARRAY['i']::"char"[] WHERE oid='integrity_g4.sv__as_of(timestamptz)'::regprocedure;
SELECT integrity_g4.attempt('actual argument mode vector', 'sql_saga: integrity violation for system-versioned table "integrity_g4.sv": generated query function role "as_of" definition is noncanonical; repair with rebuild_system_versioning_views');
ROLLBACK TO SAVEPOINT fn_argmodes;
RELEASE SAVEPOINT fn_argmodes;

SAVEPOINT fn_return_type;
UPDATE pg_proc SET prorettype='integrity_g4.a_sv_with_history'::regtype WHERE oid='integrity_g4.sv__as_of(timestamptz)'::regprocedure;
SELECT integrity_g4.attempt('exact return type', 'sql_saga: integrity violation for system-versioned table "integrity_g4.sv": generated query function role "as_of" has a noncanonical return-type dependency; repair with rebuild_system_versioning_views');
ROLLBACK TO SAVEPOINT fn_return_type;
RELEASE SAVEPOINT fn_return_type;

SAVEPOINT fn_prosrc;
UPDATE pg_proc SET prosrc='SELECT * FROM integrity_g4.sv_with_history WHERE false /* secret_body_marker */' WHERE oid='integrity_g4.sv__as_of(timestamptz)'::regprocedure;
SELECT integrity_g4.nondisclosure_attempt('canonical source body hidden', 'sql_saga: integrity violation for system-versioned table "integrity_g4.sv": generated query function role "as_of" definition is noncanonical; repair with rebuild_system_versioning_views', ARRAY['ROWDATA_MARKER_7f3a','secret_body_marker','SELECT *']);
ROLLBACK TO SAVEPOINT fn_prosrc;
RELEASE SAVEPOINT fn_prosrc;

SAVEPOINT fn_missing_normal_dependency;
DELETE FROM pg_depend WHERE classid='pg_proc'::regclass AND objid='integrity_g4.sv__as_of(timestamptz)'::regprocedure AND refclassid='pg_type'::regclass AND deptype='n';
SELECT integrity_g4.attempt('exactly one normal return dependency', 'sql_saga: integrity violation for system-versioned table "integrity_g4.sv": generated query function role "as_of" has a noncanonical return-type dependency; repair with rebuild_system_versioning_views');
ROLLBACK TO SAVEPOINT fn_missing_normal_dependency;
RELEASE SAVEPOINT fn_missing_normal_dependency;

SAVEPOINT fn_wrong_composite_dependency;
UPDATE pg_depend SET refobjid='integrity_g4.a_sv_with_history'::regtype
WHERE classid='pg_proc'::regclass AND objid='integrity_g4.sv__as_of(timestamptz)'::regprocedure AND refclassid='pg_type'::regclass AND deptype='n';
SELECT integrity_g4.attempt('dependency to exact family composite', 'sql_saga: integrity violation for system-versioned table "integrity_g4.sv": generated query function role "as_of" has a noncanonical return-type dependency; repair with rebuild_system_versioning_views');
ROLLBACK TO SAVEPOINT fn_wrong_composite_dependency;
RELEASE SAVEPOINT fn_wrong_composite_dependency;

SAVEPOINT fn_wrong_composite_relation;
UPDATE pg_type SET typrelid='integrity_g4.a_sv_with_history'::regclass WHERE oid='integrity_g4.sv_with_history'::regtype;
SELECT integrity_g4.attempt('family composite points to exact view', 'sql_saga: integrity violation for system-versioned table "integrity_g4.sv": generated query function role "as_of" has a noncanonical return-type dependency; repair with rebuild_system_versioning_views');
ROLLBACK TO SAVEPOINT fn_wrong_composite_relation;
RELEASE SAVEPOINT fn_wrong_composite_relation;

-- Deterministic first-error priority across classes and C-collated order within a class.
SAVEPOINT priority_cross_class;
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT generated_always_trigger INTO n FROM sql_saga.system_time_era WHERE table_schema='integrity_g4' AND table_name='sv';
    EXECUTE format('DROP TRIGGER %I ON integrity_g4.sv', n);
    EXECUTE format('CREATE TRIGGER %I BEFORE INSERT ON integrity_g4.sv FOR EACH ROW EXECUTE FUNCTION suppress_redundant_updates_trigger()', n);
    SELECT exclude_constraint INTO n FROM sql_saga.unique_keys WHERE unique_key_name='period_parent_predicated';
    EXECUTE format('ALTER TABLE integrity_g4.period_parent DROP CONSTRAINT %I', n);
    EXECUTE format('ALTER TABLE integrity_g4.period_parent ADD CONSTRAINT %I CHECK (true)', n);
END $damage$;
RESET session_replication_role;
SELECT integrity_g4.attempt('multi-defect cross-class priority', 'sql_saga: integrity violation for table "integrity_g4.sv": SYSTEM_TIME trigger definition is noncanonical; repair with drop_system_versioning and add_system_versioning');
ROLLBACK TO SAVEPOINT priority_cross_class;
RELEASE SAVEPOINT priority_cross_class;

SAVEPOINT priority_same_class;
SET session_replication_role = replica;
DO $damage$ DECLARE r record; BEGIN
    FOR r IN SELECT table_name, generated_always_trigger FROM sql_saga.system_time_era WHERE table_schema='integrity_g4' AND table_name IN ('a_sv','z_sv') LOOP
        EXECUTE format('DROP TRIGGER %I ON integrity_g4.%I', r.generated_always_trigger, r.table_name);
        EXECUTE format('CREATE TRIGGER %I BEFORE INSERT ON integrity_g4.%I FOR EACH ROW EXECUTE FUNCTION suppress_redundant_updates_trigger()', r.generated_always_trigger, r.table_name);
    END LOOP;
END $damage$;
RESET session_replication_role;
SELECT integrity_g4.attempt('multi-defect same-class stable order', 'sql_saga: integrity violation for table "integrity_g4.a_sv": SYSTEM_TIME trigger definition is noncanonical; repair with drop_system_versioning and add_system_versioning');
ROLLBACK TO SAVEPOINT priority_same_class;
RELEASE SAVEPOINT priority_same_class;

-- Tolerant metadata-first drops preserve same-named non-attributable user objects.
CREATE TEMP TABLE replacement_state (kind text PRIMARY KEY, identity text, definition text) ON COMMIT DROP;
SAVEPOINT preserve_function;
SET session_replication_role = replica;
DROP FUNCTION sv__as_of(timestamptz);
CREATE FUNCTION sv__as_of(p_at timestamptz) RETURNS SETOF integer LANGUAGE sql STABLE AS $user_body$ SELECT 1 WHERE false $user_body$;
RESET session_replication_role;
INSERT INTO replacement_state
SELECT 'function', p.oid::regprocedure::text, p.prosrc
FROM pg_proc AS p WHERE p.oid='integrity_g4.sv__as_of(timestamptz)'::regprocedure;
SET LOCAL client_min_messages = notice;
SELECT sql_saga.drop_system_versioning('integrity_g4.sv');
SET LOCAL client_min_messages = warning;
SELECT EXISTS (SELECT FROM replacement_state AS s JOIN pg_proc AS p ON p.oid=to_regprocedure(s.identity) WHERE s.kind='function' AND p.prosrc=s.definition) AS noncanonical_function_identity_and_definition_survive;
SELECT NOT EXISTS (SELECT FROM sql_saga.system_versioning WHERE table_schema='integrity_g4' AND table_name='sv') AS function_family_metadata_removed;
SELECT to_regprocedure('integrity_g4.sv__between(timestamptz,timestamptz)') IS NULL AS mixed_family_attributable_between_dropped;
SELECT to_regprocedure('integrity_g4.sv__between_symmetric(timestamptz,timestamptz)') IS NULL AS mixed_family_attributable_between_symmetric_dropped;
SELECT to_regprocedure('integrity_g4.sv__from_to(timestamptz,timestamptz)') IS NULL AS mixed_family_attributable_from_to_dropped;
SELECT to_regclass('integrity_g4.sv_with_history') IS NULL AS mixed_family_attributable_public_view_dropped;
SELECT to_regclass('integrity_g4.sv_history_gated') IS NULL AS mixed_family_attributable_gated_view_dropped;
SELECT to_regclass('integrity_g4.sv_history') IS NULL AS mixed_family_attributable_history_table_dropped;
SELECT NOT EXISTS (SELECT FROM pg_trigger WHERE tgrelid='integrity_g4.sv'::regclass AND tgname IN ('sv_system_time_generated_always','sv_system_time_write_history','sv_system_time_truncate')) AS mixed_family_attributable_triggers_dropped;
SELECT NOT EXISTS (SELECT FROM sql_saga.system_time_era WHERE table_schema='integrity_g4' AND table_name='sv') AS mixed_family_system_time_metadata_removed;
SELECT NOT EXISTS (SELECT FROM sql_saga.era WHERE table_schema='integrity_g4' AND table_name='sv') AS mixed_family_era_metadata_removed;
SELECT NOT EXISTS (SELECT FROM sql_saga.updatable_view WHERE table_schema='integrity_g4' AND table_name='sv') AS mixed_family_updatable_view_metadata_removed;
ROLLBACK TO SAVEPOINT preserve_function;
RELEASE SAVEPOINT preserve_function;

-- (a) v1.1 parity: a separately named user function depends on a managed view.
-- RESTRICT refuses atomically; CASCADE is explicit consent to native PostgreSQL dependency semantics.
SAVEPOINT separate_dependent_restrict;
CREATE FUNCTION separate_user_dep(p_at timestamptz) RETURNS SETOF sv_with_history LANGUAGE sql STABLE AS $user_body$ SELECT * FROM integrity_g4.sv_with_history WHERE false $user_body$;
SELECT integrity_g4.dependency_restrict_attempt(
    'separate dependent RESTRICT v1.1 parity',
    $$SELECT sql_saga.drop_system_versioning('integrity_g4.sv', 'RESTRICT')$$,
    'cannot drop view integrity_g4.sv_with_history because other objects depend on it');
SELECT to_regprocedure('integrity_g4.separate_user_dep(timestamptz)') IS NOT NULL AS restrict_separate_dependent_survives;
SELECT EXISTS (SELECT FROM sql_saga.system_versioning WHERE table_schema='integrity_g4' AND table_name='sv') AS restrict_separate_family_metadata_survives;
ROLLBACK TO SAVEPOINT separate_dependent_restrict;
RELEASE SAVEPOINT separate_dependent_restrict;

SAVEPOINT separate_dependent_cascade;
CREATE FUNCTION separate_user_dep(p_at timestamptz) RETURNS SETOF sv_with_history LANGUAGE sql STABLE AS $user_body$ SELECT * FROM integrity_g4.sv_with_history WHERE false $user_body$;
SET LOCAL client_min_messages = notice;
SELECT sql_saga.drop_system_versioning('integrity_g4.sv', 'CASCADE');
SET LOCAL client_min_messages = warning;
SELECT to_regprocedure('integrity_g4.separate_user_dep(timestamptz)') IS NULL AS cascade_separate_dependent_dropped;
SELECT NOT EXISTS (SELECT FROM sql_saga.system_versioning WHERE table_schema='integrity_g4' AND table_name='sv') AS cascade_separate_family_metadata_removed;
ROLLBACK TO SAVEPOINT separate_dependent_cascade;
RELEASE SAVEPOINT separate_dependent_cascade;

-- (b) New 1.2 semantics: 1.1 deleted this same-named replacement by name even under RESTRICT.
-- Version 1.2 preserves it, so RESTRICT reaches the native view dependency and refuses atomically;
-- CASCADE remains explicit consent to remove the dependent replacement with the managed view. Because the
-- cascade removes it, sql_saga must not claim it was left in place: only PostgreSQL's cascade NOTICE appears.
SAVEPOINT dependent_replacement_restrict;
SET session_replication_role = replica;
DROP FUNCTION sv__as_of(timestamptz);
CREATE FUNCTION sv__as_of(p_at timestamptz) RETURNS SETOF sv_with_history LANGUAGE sql STABLE AS $user_body$ SELECT * FROM integrity_g4.sv_with_history WHERE false $user_body$;
RESET session_replication_role;
SELECT integrity_g4.dependency_restrict_attempt(
    'dependent replacement RESTRICT',
    $$SELECT sql_saga.drop_system_versioning('integrity_g4.sv', 'RESTRICT')$$,
    'cannot drop view integrity_g4.sv_with_history because other objects depend on it');
SELECT to_regprocedure('integrity_g4.sv__as_of(timestamptz)') IS NOT NULL AS restrict_dependent_replacement_survives;
SELECT EXISTS (SELECT FROM sql_saga.system_versioning WHERE table_schema='integrity_g4' AND table_name='sv') AS restrict_family_metadata_survives;
ROLLBACK TO SAVEPOINT dependent_replacement_restrict;
RELEASE SAVEPOINT dependent_replacement_restrict;

SAVEPOINT dependent_replacement_cascade;
SET session_replication_role = replica;
DROP FUNCTION sv__as_of(timestamptz);
CREATE FUNCTION sv__as_of(p_at timestamptz) RETURNS SETOF sv_with_history LANGUAGE sql STABLE AS $user_body$ SELECT * FROM integrity_g4.sv_with_history WHERE false $user_body$;
RESET session_replication_role;
SET LOCAL client_min_messages = notice;
SELECT sql_saga.drop_system_versioning('integrity_g4.sv', 'CASCADE');
SET LOCAL client_min_messages = warning;
SELECT to_regprocedure('integrity_g4.sv__as_of(timestamptz)') IS NULL AS cascade_dependent_replacement_dropped;
SELECT NOT EXISTS (SELECT FROM sql_saga.system_versioning WHERE table_schema='integrity_g4' AND table_name='sv') AS cascade_family_metadata_removed;
ROLLBACK TO SAVEPOINT dependent_replacement_cascade;
RELEASE SAVEPOINT dependent_replacement_cascade;

SAVEPOINT preserve_view;
SET session_replication_role = replica;
DROP FUNCTION sv__as_of(timestamptz);
DROP FUNCTION sv__between(timestamptz,timestamptz);
DROP FUNCTION sv__between_symmetric(timestamptz,timestamptz);
DROP FUNCTION sv__from_to(timestamptz,timestamptz);
DROP VIEW sv_with_history;
CREATE VIEW sv_with_history AS SELECT id, tenant, payload FROM sv;
RESET session_replication_role;
INSERT INTO replacement_state SELECT 'view', 'integrity_g4.sv_with_history', pg_get_viewdef('integrity_g4.sv_with_history'::regclass, false);
SET LOCAL client_min_messages = notice;
SELECT sql_saga.drop_system_versioning('integrity_g4.sv');
SET LOCAL client_min_messages = warning;
SELECT EXISTS (SELECT FROM replacement_state AS s WHERE s.kind='view' AND to_regclass(s.identity) IS NOT NULL AND pg_get_viewdef(to_regclass(s.identity), false)=s.definition) AS noncanonical_view_identity_and_definition_survive;
SELECT NOT EXISTS (SELECT FROM sql_saga.system_versioning WHERE table_schema='integrity_g4' AND table_name='sv') AS view_family_metadata_removed;
ROLLBACK TO SAVEPOINT preserve_view;
RELEASE SAVEPOINT preserve_view;

SAVEPOINT preserve_trigger;
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT generated_always_trigger INTO n FROM sql_saga.system_time_era WHERE table_schema='integrity_g4' AND table_name='sv';
    EXECUTE format('DROP TRIGGER %I ON integrity_g4.sv', n);
    EXECUTE format('CREATE TRIGGER %I BEFORE INSERT ON integrity_g4.sv FOR EACH ROW EXECUTE FUNCTION suppress_redundant_updates_trigger()', n);
END $damage$;
RESET session_replication_role;
INSERT INTO replacement_state
SELECT 'trigger', t.tgname, pg_get_triggerdef(t.oid, false)
FROM pg_trigger AS t JOIN sql_saga.system_time_era AS ste ON ste.generated_always_trigger=t.tgname
WHERE ste.table_schema='integrity_g4' AND ste.table_name='sv' AND t.tgrelid='integrity_g4.sv'::regclass;
SET LOCAL client_min_messages = notice;
SELECT sql_saga.drop_system_versioning('integrity_g4.sv');
SET LOCAL client_min_messages = warning;
SELECT EXISTS (SELECT FROM replacement_state AS s JOIN pg_trigger AS t ON t.tgrelid='integrity_g4.sv'::regclass AND t.tgname=s.identity WHERE s.kind='trigger' AND pg_get_triggerdef(t.oid,false)=s.definition) AS noncanonical_trigger_identity_and_definition_survive;
SELECT NOT EXISTS (SELECT FROM sql_saga.system_versioning WHERE table_schema='integrity_g4' AND table_name='sv') AS trigger_family_metadata_removed;
ROLLBACK TO SAVEPOINT preserve_trigger;
RELEASE SAVEPOINT preserve_trigger;

SAVEPOINT preserve_constraint;
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT exclude_constraint INTO n FROM sql_saga.unique_keys WHERE unique_key_name='period_parent_predicated';
    EXECUTE format('ALTER TABLE integrity_g4.period_parent DROP CONSTRAINT %I', n);
    EXECUTE format('ALTER TABLE integrity_g4.period_parent ADD CONSTRAINT %I EXCLUDE USING gist (category WITH =, valid_range WITH &&) WHERE (category IS NULL) DEFERRABLE INITIALLY DEFERRED', n);
END $damage$;
RESET session_replication_role;
SELECT integrity_g4.attempt('wrong same-kind predicated exclusion constraint', 'sql_saga: integrity violation for unique key "period_parent_predicated" on table "integrity_g4.period_parent": managed constraint or index is noncanonical; repair with drop_unique_key and add_unique_key');
INSERT INTO replacement_state
SELECT 'constraint', c.conname, pg_get_constraintdef(c.oid, false) FROM pg_constraint AS c
WHERE c.conrelid='integrity_g4.period_parent'::regclass AND c.conname LIKE 'period_parent_predicated%';
SET LOCAL client_min_messages = notice;
SELECT sql_saga.drop_unique_key_by_name('integrity_g4.period_parent', 'period_parent_predicated');
SET LOCAL client_min_messages = warning;
SELECT EXISTS (SELECT FROM replacement_state AS s JOIN pg_constraint AS c ON c.conrelid='integrity_g4.period_parent'::regclass AND c.conname=s.identity WHERE s.kind='constraint' AND pg_get_constraintdef(c.oid,false)=s.definition) AS noncanonical_constraint_identity_and_definition_survive;
SELECT NOT EXISTS (SELECT FROM sql_saga.unique_keys WHERE unique_key_name='period_parent_predicated') AS constraint_family_metadata_removed;
ROLLBACK TO SAVEPOINT preserve_constraint;
RELEASE SAVEPOINT preserve_constraint;

SAVEPOINT preserve_exclusion_operator;
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT exclude_constraint INTO n FROM sql_saga.unique_keys WHERE unique_key_name='period_parent_predicated';
    EXECUTE format('ALTER TABLE integrity_g4.period_parent DROP CONSTRAINT %I', n);
    EXECUTE format('ALTER TABLE integrity_g4.period_parent ADD CONSTRAINT %I EXCLUDE USING gist (id WITH =, valid_range WITH =) WHERE (category IS NOT NULL)', n);
END $damage$;
RESET session_replication_role;
SELECT integrity_g4.attempt('predicated exclusion differs only by operator', 'sql_saga: integrity violation for unique key "period_parent_predicated" on table "integrity_g4.period_parent": managed constraint or index is noncanonical; repair with drop_unique_key and add_unique_key');
INSERT INTO replacement_state
SELECT 'operator_constraint', c.conname, pg_get_constraintdef(c.oid, false) FROM pg_constraint AS c
WHERE c.conrelid='integrity_g4.period_parent'::regclass AND c.conname LIKE 'period_parent_predicated%';
SET LOCAL client_min_messages = notice;
SELECT sql_saga.drop_unique_key_by_name('integrity_g4.period_parent', 'period_parent_predicated');
SET LOCAL client_min_messages = warning;
SELECT EXISTS (SELECT FROM replacement_state AS s JOIN pg_constraint AS c ON c.conrelid='integrity_g4.period_parent'::regclass AND c.conname=s.identity WHERE s.kind='operator_constraint' AND pg_get_constraintdef(c.oid,false)=s.definition) AS operator_only_exclusion_survives;
SELECT NOT EXISTS (SELECT FROM sql_saga.unique_keys WHERE unique_key_name='period_parent_predicated') AS operator_exclusion_family_metadata_removed;
ROLLBACK TO SAVEPOINT preserve_exclusion_operator;
RELEASE SAVEPOINT preserve_exclusion_operator;

SAVEPOINT preserve_exclusion_predicate;
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT exclude_constraint INTO n FROM sql_saga.unique_keys WHERE unique_key_name='period_parent_predicated';
    EXECUTE format('ALTER TABLE integrity_g4.period_parent DROP CONSTRAINT %I', n);
    EXECUTE format('ALTER TABLE integrity_g4.period_parent ADD CONSTRAINT %I EXCLUDE USING gist (id WITH =, valid_range WITH &&) WHERE (category IS NULL) DEFERRABLE INITIALLY IMMEDIATE', n);
END $damage$;
RESET session_replication_role;
SELECT integrity_g4.attempt('predicated exclusion differs only by predicate', 'sql_saga: integrity violation for unique key "period_parent_predicated" on table "integrity_g4.period_parent": managed constraint or index is noncanonical; repair with drop_unique_key and add_unique_key');
INSERT INTO replacement_state
SELECT 'predicate_constraint', c.conname, pg_get_constraintdef(c.oid, false) FROM pg_constraint AS c
WHERE c.conrelid='integrity_g4.period_parent'::regclass AND c.conname LIKE 'period_parent_predicated%';
SET LOCAL client_min_messages = notice;
SELECT sql_saga.drop_unique_key_by_name('integrity_g4.period_parent', 'period_parent_predicated');
SET LOCAL client_min_messages = warning;
SELECT EXISTS (SELECT FROM replacement_state AS s JOIN pg_constraint AS c ON c.conrelid='integrity_g4.period_parent'::regclass AND c.conname=s.identity WHERE s.kind='predicate_constraint' AND pg_get_constraintdef(c.oid,false)=s.definition) AS predicate_only_exclusion_survives;
SELECT NOT EXISTS (SELECT FROM sql_saga.unique_keys WHERE unique_key_name='period_parent_predicated') AS predicate_exclusion_family_metadata_removed;
ROLLBACK TO SAVEPOINT preserve_exclusion_predicate;
RELEASE SAVEPOINT preserve_exclusion_predicate;

SAVEPOINT preserve_index;
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT unique_constraint INTO n FROM sql_saga.unique_keys WHERE unique_key_name='period_parent_natural';
    EXECUTE format('ALTER TABLE integrity_g4.period_parent DROP CONSTRAINT %I', n);
    EXECUTE format('CREATE UNIQUE INDEX %I ON integrity_g4.period_parent (category DESC)', n);
END $damage$;
RESET session_replication_role;
SELECT integrity_g4.attempt('wrong exact managed unique index definition', 'sql_saga: integrity violation for unique key "period_parent_natural" on table "integrity_g4.period_parent": managed constraint or index is noncanonical; repair with drop_unique_key and add_unique_key');
INSERT INTO replacement_state
SELECT 'index', c.relname, pg_get_indexdef(c.oid) FROM pg_class AS c JOIN pg_namespace AS n ON n.oid=c.relnamespace
WHERE n.nspname='integrity_g4' AND c.relname LIKE 'period_parent_natural%' AND c.relkind='i';
SET LOCAL client_min_messages = notice;
SELECT sql_saga.drop_unique_key_by_name('integrity_g4.period_parent', 'period_parent_natural');
SET LOCAL client_min_messages = warning;
SELECT EXISTS (SELECT FROM replacement_state AS s JOIN pg_class AS c ON c.relname=s.identity JOIN pg_namespace AS n ON n.oid=c.relnamespace WHERE s.kind='index' AND n.nspname='integrity_g4' AND c.relkind='i' AND pg_get_indexdef(c.oid)=s.definition) AS noncanonical_index_identity_and_definition_survive;
SELECT NOT EXISTS (SELECT FROM sql_saga.unique_keys WHERE unique_key_name='period_parent_natural') AS index_family_metadata_removed;
ROLLBACK TO SAVEPOINT preserve_index;
RELEASE SAVEPOINT preserve_index;

SAVEPOINT preserve_unique_constraint;
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT unique_constraint INTO n FROM sql_saga.unique_keys WHERE unique_key_name='period_child_primary';
    EXECUTE format('ALTER TABLE integrity_g4.period_child DROP CONSTRAINT %I', n);
    EXECUTE format('ALTER TABLE integrity_g4.period_child ADD CONSTRAINT %I UNIQUE (parent_id, valid_range WITHOUT OVERLAPS)', n);
END $damage$;
RESET session_replication_role;
SELECT integrity_g4.attempt('wrong same-kind temporal unique constraint', 'sql_saga: integrity violation for unique key "period_child_primary" on table "integrity_g4.period_child": managed constraint or index is noncanonical; repair with drop_unique_key and add_unique_key');
INSERT INTO replacement_state
SELECT 'unique_constraint', c.conname, pg_get_constraintdef(c.oid, false) FROM pg_constraint AS c
JOIN sql_saga.unique_keys AS uk ON uk.unique_constraint=c.conname
WHERE uk.unique_key_name='period_child_primary' AND c.conrelid='integrity_g4.period_child'::regclass;
SET LOCAL client_min_messages = notice;
SELECT sql_saga.drop_unique_key_by_name('integrity_g4.period_child', 'period_child_primary');
SET LOCAL client_min_messages = warning;
SELECT EXISTS (SELECT FROM replacement_state AS s JOIN pg_constraint AS c ON c.conrelid='integrity_g4.period_child'::regclass AND c.conname=s.identity WHERE s.kind='unique_constraint' AND pg_get_constraintdef(c.oid,false)=s.definition) AS noncanonical_unique_constraint_survives;
SELECT NOT EXISTS (SELECT FROM sql_saga.unique_keys WHERE unique_key_name='period_child_primary') AS unique_constraint_family_metadata_removed;
ROLLBACK TO SAVEPOINT preserve_unique_constraint;
RELEASE SAVEPOINT preserve_unique_constraint;

SAVEPOINT preserve_check_constraint;
SET session_replication_role = replica;
ALTER TABLE integrity_g4.period_parent ADD CONSTRAINT period_parent_natural_check CHECK (category IS NOT NULL);
UPDATE sql_saga.unique_keys SET check_constraint='period_parent_natural_check' WHERE unique_key_name='period_parent_natural';
ALTER TABLE integrity_g4.period_parent DROP CONSTRAINT period_parent_natural_check;
ALTER TABLE integrity_g4.period_parent ADD CONSTRAINT period_parent_natural_check CHECK (category IS NULL);
RESET session_replication_role;
SELECT integrity_g4.attempt('wrong same-name managed check constraint', 'sql_saga: integrity violation for unique key "period_parent_natural" on table "integrity_g4.period_parent": managed constraint or index is noncanonical; repair with drop_unique_key and add_unique_key');
INSERT INTO replacement_state
SELECT 'check_constraint', c.conname, pg_get_constraintdef(c.oid, false) FROM pg_constraint AS c
WHERE c.conrelid='integrity_g4.period_parent'::regclass AND c.conname='period_parent_natural_check';
SET LOCAL client_min_messages = notice;
SELECT sql_saga.drop_unique_key_by_name('integrity_g4.period_parent', 'period_parent_natural');
SET LOCAL client_min_messages = warning;
SELECT EXISTS (SELECT FROM replacement_state AS s JOIN pg_constraint AS c ON c.conrelid='integrity_g4.period_parent'::regclass AND c.conname=s.identity WHERE s.kind='check_constraint' AND pg_get_constraintdef(c.oid,false)=s.definition) AS noncanonical_check_constraint_survives;
SELECT NOT EXISTS (SELECT FROM sql_saga.unique_keys WHERE unique_key_name='period_parent_natural') AS check_constraint_family_metadata_removed;
ROLLBACK TO SAVEPOINT preserve_check_constraint;
RELEASE SAVEPOINT preserve_check_constraint;

-- Canonical attributable counterparts are removed by the same shared attribution layer.
SAVEPOINT attributable_system_versioning_members;
SELECT sql_saga.drop_system_versioning('integrity_g4.sv');
SELECT to_regprocedure('integrity_g4.sv__as_of(timestamptz)') IS NULL AS attributable_function_dropped;
SELECT to_regclass('integrity_g4.sv_with_history') IS NULL AS attributable_view_dropped;
SELECT NOT EXISTS (SELECT FROM pg_trigger WHERE tgrelid='integrity_g4.sv'::regclass AND tgname='sv_system_time_generated_always') AS attributable_trigger_dropped;
ROLLBACK TO SAVEPOINT attributable_system_versioning_members;
RELEASE SAVEPOINT attributable_system_versioning_members;

SAVEPOINT attributable_constraint;
INSERT INTO replacement_state(kind, identity) SELECT 'attributable_constraint', coalesce(exclude_constraint, unique_constraint)::text FROM sql_saga.unique_keys WHERE unique_key_name='period_parent_predicated';
SELECT sql_saga.drop_unique_key_by_name('integrity_g4.period_parent', 'period_parent_predicated');
SELECT NOT EXISTS (SELECT FROM replacement_state AS s JOIN pg_constraint AS c ON c.conrelid='integrity_g4.period_parent'::regclass AND c.conname=s.identity::name WHERE s.kind='attributable_constraint') AS attributable_constraint_dropped;
ROLLBACK TO SAVEPOINT attributable_constraint;
RELEASE SAVEPOINT attributable_constraint;

SAVEPOINT attributable_index;
INSERT INTO replacement_state(kind, identity) SELECT 'attributable_index', unique_constraint::text FROM sql_saga.unique_keys WHERE unique_key_name='period_parent_natural';
SELECT sql_saga.drop_unique_key_by_name('integrity_g4.period_parent', 'period_parent_natural');
SELECT NOT EXISTS (SELECT FROM replacement_state AS s WHERE s.kind='attributable_index' AND to_regclass(format('integrity_g4.%I', s.identity)) IS NOT NULL) AS attributable_index_dropped;
ROLLBACK TO SAVEPOINT attributable_index;
RELEASE SAVEPOINT attributable_index;

ROLLBACK;
\i sql/include/test_teardown.sql
