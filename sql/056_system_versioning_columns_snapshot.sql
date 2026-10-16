\set ECHO none
\i sql/include/test_setup.sql
\set SHOW_CONTEXT never
\pset format unaligned
\pset tuples_only on
SET client_min_messages = warning;

BEGIN;
CREATE SCHEMA snapshot_test;
CREATE ROLE snapshot_table_owner;
CREATE ROLE snapshot_unrelated_role;
CREATE ROLE snapshot_owner_member;
GRANT snapshot_table_owner TO snapshot_owner_member;
GRANT USAGE ON SCHEMA snapshot_test TO snapshot_table_owner, snapshot_unrelated_role;
GRANT USAGE ON SCHEMA snapshot_test TO snapshot_owner_member;
SET search_path = snapshot_test, public;

CREATE FUNCTION snapshot_test.live_columns(base regclass)
RETURNS name[] LANGUAGE sql AS $live_columns$
    SELECT array_agg(a.attname ORDER BY a.attnum)
    FROM pg_catalog.pg_attribute AS a
    WHERE a.attrelid = base
      AND a.attnum > 0
      AND NOT a.attisdropped
$live_columns$;

-- Dynamic SQL lets this RED test continue on 1.1-equivalent HEAD, where the catalogue column is absent.
CREATE FUNCTION snapshot_test.stored_snapshot(base regclass)
RETURNS text LANGUAGE plpgsql AS $stored_snapshot$
DECLARE
    result text;
BEGIN
    BEGIN
        EXECUTE $$
            SELECT ste.table_columns_snapshot::text
            FROM sql_saga.system_time_era AS ste
            WHERE (ste.table_schema, ste.table_name) =
                  ((SELECT n.nspname FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace WHERE c.oid = $1),
                   (SELECT c.relname FROM pg_catalog.pg_class AS c WHERE c.oid = $1))
        $$ INTO result USING base;
        RETURN result;
    EXCEPTION WHEN undefined_column THEN
        RETURN '<missing table_columns_snapshot>';
    END;
END;
$stored_snapshot$;

CREATE FUNCTION snapshot_test.replace_snapshot(base regclass, replacement name[])
RETURNS boolean LANGUAGE plpgsql AS $replace_snapshot$
BEGIN
    BEGIN
        EXECUTE $$
            UPDATE sql_saga.system_time_era AS ste
            SET table_columns_snapshot = $2
            WHERE (ste.table_schema, ste.table_name) =
                  ((SELECT n.nspname FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace WHERE c.oid = $1),
                   (SELECT c.relname FROM pg_catalog.pg_class AS c WHERE c.oid = $1))
        $$ USING base, replacement;
        RETURN true;
    EXCEPTION WHEN undefined_column THEN
        RETURN false;
    END;
END;
$replace_snapshot$;

CREATE FUNCTION snapshot_test.snapshot_result(case_name text, base regclass)
RETURNS text LANGUAGE sql AS $snapshot_result$
    SELECT format('%s | stored=%s | live=%s | equal=%s',
                  case_name,
                  snapshot_test.stored_snapshot(base),
                  snapshot_test.live_columns(base)::text,
                  snapshot_test.stored_snapshot(base) = snapshot_test.live_columns(base)::text)
$snapshot_result$;

CREATE FUNCTION snapshot_test.case_result(case_name text, command text, expected_sqlstate text, expected_message text)
RETURNS text LANGUAGE plpgsql AS $case_result$
DECLARE
    actual_sqlstate text;
    actual_message text;
BEGIN
    BEGIN
        EXECUTE command;
        IF expected_sqlstate IS NULL THEN
            RETURN case_name || ' | ok';
        END IF;
        RETURN format('%s | unexpected success, expected %s: %s', case_name, expected_sqlstate, expected_message);
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS actual_sqlstate = RETURNED_SQLSTATE, actual_message = MESSAGE_TEXT;
        IF expected_sqlstate IS NULL THEN
            RETURN format('%s | unexpected error %s: %s', case_name, actual_sqlstate, actual_message);
        END IF;
        IF (actual_sqlstate, actual_message) IS DISTINCT FROM (expected_sqlstate, expected_message) THEN
            RETURN format('%s | unexpected error %s: %s', case_name, actual_sqlstate, actual_message);
        END IF;
        RETURN format('%s | %s: %s', case_name, actual_sqlstate, actual_message);
    END;
END;
$case_result$;

CREATE FUNCTION snapshot_test.role_case_result(case_name text, role_name name, command text, expected_sqlstate text, expected_message text)
RETURNS text LANGUAGE plpgsql AS $role_case_result$
DECLARE
    actual_sqlstate text;
    actual_message text;
BEGIN
    BEGIN
        EXECUTE format('SET LOCAL ROLE %I', role_name);
        EXECUTE command;
        EXECUTE 'RESET ROLE';
        IF expected_sqlstate IS NULL THEN
            RETURN case_name || ' | ok';
        END IF;
        RETURN format('%s | unexpected success, expected %s: %s', case_name, expected_sqlstate, expected_message);
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS actual_sqlstate = RETURNED_SQLSTATE, actual_message = MESSAGE_TEXT;
        EXECUTE 'RESET ROLE';
        IF (actual_sqlstate, actual_message) IS DISTINCT FROM (expected_sqlstate, expected_message) THEN
            RETURN format('%s | unexpected error %s: %s', case_name, actual_sqlstate, actual_message);
        END IF;
        RETURN format('%s | %s: %s', case_name, actual_sqlstate, actual_message);
    END;
END;
$role_case_result$;

CREATE TABLE family_a (id int PRIMARY KEY, tenant text, payload text);
CREATE TABLE family_b (id int PRIMARY KEY, tenant text, payload text);
CREATE TABLE family_c (id int PRIMARY KEY, payload text);
CREATE TABLE family_gateway (id int PRIMARY KEY, payload text);
ALTER TABLE family_gateway OWNER TO snapshot_table_owner;
CREATE INDEX family_c_payload_idx ON family_c (payload);
CREATE UNIQUE INDEX family_c_replica_idx ON family_c (id);
CREATE FUNCTION snapshot_test.family_c_user_trigger()
RETURNS trigger LANGUAGE plpgsql AS $family_c_user_trigger$
BEGIN
    RETURN NEW;
END;
$family_c_user_trigger$;
CREATE TRIGGER family_c_user_trigger
BEFORE UPDATE ON family_c
FOR EACH ROW EXECUTE FUNCTION snapshot_test.family_c_user_trigger();
SELECT sql_saga.add_system_versioning('family_a');
SELECT sql_saga.add_system_versioning('family_b');
SELECT sql_saga.add_system_versioning('family_c');
SELECT sql_saga.set_system_time_era_excluded_columns('family_a', ARRAY['tenant']::name[]);
SELECT sql_saga.set_system_time_era_excluded_columns('family_b', ARRAY['tenant']::name[]);

\echo === rebuild gateway authorization and nondisclosure
-- The refusal keeps the 1.1 wording and SQLSTATE (must be owner, 42501) and is identical whether or not the
-- table is a registered family, so the gateway does not reveal family existence to an unrelated role.
SELECT snapshot_test.role_case_result(
    'unrelated role before registration',
    'snapshot_unrelated_role',
    $$SELECT sql_saga.rebuild_system_versioning_views('snapshot_test.family_gateway')$$,
    '42501',
    'must be owner of table family_gateway');
SELECT sql_saga.add_system_versioning('family_gateway');
SELECT snapshot_test.role_case_result(
    'unrelated role after registration',
    'snapshot_unrelated_role',
    $$SELECT sql_saga.rebuild_system_versioning_views('snapshot_test.family_gateway')$$,
    '42501',
    'must be owner of table family_gateway');
SELECT snapshot_test.role_case_result(
    'base owner rebuild',
    'snapshot_table_owner',
    $$SELECT sql_saga.rebuild_system_versioning_views('snapshot_test.family_gateway')$$,
    NULL,
    NULL);
SELECT snapshot_test.role_case_result(
    'owner member rebuild',
    'snapshot_owner_member',
    $$SELECT sql_saga.rebuild_system_versioning_views('snapshot_test.family_gateway')$$,
    NULL,
    NULL);

-- Inheritance fixtures: the parent is unmanaged and each child is independently managed.
CREATE TABLE inherit_one_parent (id int, payload text);
CREATE TABLE inherit_one_child () INHERITS (inherit_one_parent);
CREATE TABLE inherit_two_parent (id int, payload text);
CREATE TABLE inherit_two_child_a () INHERITS (inherit_two_parent);
CREATE TABLE inherit_two_child_b () INHERITS (inherit_two_parent);
CREATE TABLE inherit_drop_parent (id int, payload text, drop_me text);
CREATE TABLE inherit_drop_child () INHERITS (inherit_drop_parent);
CREATE TABLE inherit_rename_parent (id int, payload text);
CREATE TABLE inherit_rename_child () INHERITS (inherit_rename_parent);
CREATE TABLE inherit_excluded_parent (id int, excluded text);
CREATE TABLE inherit_excluded_child () INHERITS (inherit_excluded_parent);
CREATE TABLE inherit_only_add_parent (id int, payload text);
CREATE TABLE inherit_only_add_child () INHERITS (inherit_only_add_parent);
CREATE TABLE inherit_only_drop_parent (id int, payload text, keep_local text);
CREATE TABLE inherit_only_drop_child () INHERITS (inherit_only_drop_parent);
CREATE TABLE inherit_only_rename_parent (id int, payload text);
CREATE TABLE inherit_only_rename_child () INHERITS (inherit_only_rename_parent);
CREATE TABLE inherit_stale_parent (id int, payload text);
CREATE TABLE inherit_stale_child () INHERITS (inherit_stale_parent);
DO $inheritance_fixtures$
BEGIN
    PERFORM sql_saga.add_system_versioning('inherit_one_child');
    PERFORM sql_saga.add_system_versioning('inherit_two_child_a');
    PERFORM sql_saga.add_system_versioning('inherit_two_child_b');
    PERFORM sql_saga.add_system_versioning('inherit_drop_child');
    PERFORM sql_saga.add_system_versioning('inherit_rename_child');
    PERFORM sql_saga.add_system_versioning('inherit_excluded_child');
    PERFORM sql_saga.add_system_versioning('inherit_only_add_child');
    PERFORM sql_saga.add_system_versioning('inherit_only_drop_child');
    PERFORM sql_saga.add_system_versioning('inherit_only_rename_child');
    PERFORM sql_saga.add_system_versioning('inherit_stale_child');
    PERFORM sql_saga.set_system_time_era_excluded_columns(
        'inherit_excluded_child', ARRAY['excluded']::name[]);
END;
$inheritance_fixtures$;

\echo === snapshot lifecycle and content
SELECT snapshot_test.snapshot_result('family creation A', 'family_a');
SELECT snapshot_test.snapshot_result('family creation B', 'family_b');
ALTER TABLE family_b ADD COLUMN note text;
SELECT snapshot_test.snapshot_result('supported ADD', 'family_b');
ALTER TABLE family_b DROP COLUMN note;
SELECT snapshot_test.snapshot_result('supported DROP', 'family_b');
ALTER TABLE family_b RENAME COLUMN payload TO body;
SELECT snapshot_test.snapshot_result('supported non-excluded RENAME', 'family_b');
ALTER TABLE family_b_history RENAME COLUMN payload TO body;
SELECT snapshot_test.replace_snapshot('family_b', ARRAY['stale']::name[]) AS snapshot_damage_installed;
SELECT snapshot_test.snapshot_result('before canonical rebuild', 'family_b');
SELECT sql_saga.rebuild_system_versioning_views('family_b');
SELECT snapshot_test.snapshot_result('canonical rebuild refresh', 'family_b');

\echo === direct excluded-column protection
SELECT snapshot_test.case_result(
    'excluded RENAME on same base',
    $$ALTER TABLE snapshot_test.family_b RENAME COLUMN tenant TO account$$,
    'P0001',
    'cannot drop or rename column "tenant" on table "snapshot_test.family_b" because it is excluded from system_time era');
SELECT snapshot_test.case_result(
    'excluded DROP refused at sql_drop',
    $$ALTER TABLE snapshot_test.family_b DROP COLUMN tenant CASCADE$$,
    'P0001',
    'cannot drop or rename column "tenant" on table "snapshot_test.family_b" because it is excluded from system_time era');

\echo === ambiguous multi-action is atomic
ALTER EVENT TRIGGER sql_saga_drop_protection DISABLE;
ALTER EVENT TRIGGER sql_saga_health_checks DISABLE;
SELECT snapshot_test.case_result(
    'excluded name plus other shape changes',
    $$ALTER TABLE snapshot_test.family_b DROP COLUMN tenant CASCADE, ADD COLUMN account text, ADD COLUMN extra integer$$,
    'P0001',
    'cannot determine whether excluded column "tenant" on table "snapshot_test.family_b" was renamed because ALTER TABLE changed multiple columns; split column-shape changes into separate ALTER TABLE statements');
ALTER EVENT TRIGGER sql_saga_health_checks ENABLE;
ALTER EVENT TRIGGER sql_saga_drop_protection ENABLE;
SELECT snapshot_test.snapshot_result('ambiguous statement rolled back', 'family_b');
SELECT snapshot_test.live_columns('family_b')::text = '{id,tenant,body,system_valid_range}' AS ambiguous_rollback_preserved_shape;

\echo === relation-scoped attribution and no damage blessing
SAVEPOINT relation_scope_damage;
-- Damage A under the narrow test bypass, then issue one B statement that produces several command rows.
SET session_replication_role = replica;
ALTER TABLE family_a DROP COLUMN tenant CASCADE;
RESET session_replication_role;
SELECT snapshot_test.snapshot_result('bypass damage remains visible in A', 'family_a');
ALTER EVENT TRIGGER sql_saga_health_checks DISABLE;
SELECT snapshot_test.case_result(
    'stale A does not blame multi-row ALTER on B',
    $$ALTER TABLE snapshot_test.family_b ADD COLUMN b_code integer UNIQUE$$,
    NULL,
    NULL);
ALTER EVENT TRIGGER sql_saga_health_checks ENABLE;
SELECT snapshot_test.snapshot_result('B refreshed after its own ALTER', 'family_b');
SELECT snapshot_test.snapshot_result('unrelated B DDL did not bless A', 'family_a');
-- Step 4 RED will assert that sql_saga.check_integrity() diagnoses this exact damaged family A.
SELECT snapshot_test.live_columns('family_a')::text <> snapshot_test.stored_snapshot('family_a') AS integrity_precondition_detects_a;
ROLLBACK TO SAVEPOINT relation_scope_damage;
RELEASE SAVEPOINT relation_scope_damage;

\echo === same-base non-shape ALTER TABLE does not bless stale snapshot
SAVEPOINT non_shape_set_statistics;
SELECT snapshot_test.replace_snapshot('family_c', ARRAY['stale']::name[]) AS set_statistics_damage_installed;
SELECT snapshot_test.snapshot_result('SET STATISTICS stale precondition', 'family_c');
ALTER TABLE family_c ALTER COLUMN payload SET STATISTICS 100;
SELECT snapshot_test.snapshot_result('SET STATISTICS keeps stale snapshot', 'family_c');
ROLLBACK TO SAVEPOINT non_shape_set_statistics;
RELEASE SAVEPOINT non_shape_set_statistics;

SAVEPOINT non_shape_set_storage;
SELECT snapshot_test.replace_snapshot('family_c', ARRAY['stale']::name[]) AS set_storage_damage_installed;
SELECT snapshot_test.snapshot_result('SET STORAGE stale precondition', 'family_c');
ALTER TABLE family_c ALTER COLUMN payload SET STORAGE EXTENDED;
SELECT snapshot_test.snapshot_result('SET STORAGE keeps stale snapshot', 'family_c');
ROLLBACK TO SAVEPOINT non_shape_set_storage;
RELEASE SAVEPOINT non_shape_set_storage;

SAVEPOINT non_shape_set_default;
SELECT snapshot_test.replace_snapshot('family_c', ARRAY['stale']::name[]) AS set_default_damage_installed;
SELECT snapshot_test.snapshot_result('SET DEFAULT stale precondition', 'family_c');
ALTER TABLE family_c ALTER COLUMN payload SET DEFAULT 'default payload';
SELECT snapshot_test.snapshot_result('SET DEFAULT keeps stale snapshot', 'family_c');
ROLLBACK TO SAVEPOINT non_shape_set_default;
RELEASE SAVEPOINT non_shape_set_default;

SAVEPOINT non_shape_add_check;
SELECT snapshot_test.replace_snapshot('family_c', ARRAY['stale']::name[]) AS add_check_damage_installed;
SELECT snapshot_test.snapshot_result('ADD CHECK stale precondition', 'family_c');
ALTER TABLE family_c ADD CONSTRAINT family_c_payload_check CHECK (payload IS NULL OR length(payload) >= 0) NOT VALID;
SELECT snapshot_test.snapshot_result('ADD CHECK keeps stale snapshot', 'family_c');
ROLLBACK TO SAVEPOINT non_shape_add_check;
RELEASE SAVEPOINT non_shape_add_check;

SAVEPOINT non_shape_disable_trigger;
SELECT snapshot_test.replace_snapshot('family_c', ARRAY['stale']::name[]) AS disable_trigger_damage_installed;
SELECT snapshot_test.snapshot_result('DISABLE TRIGGER stale precondition', 'family_c');
ALTER TABLE family_c DISABLE TRIGGER family_c_user_trigger;
SELECT snapshot_test.snapshot_result('DISABLE TRIGGER keeps stale snapshot', 'family_c');
ROLLBACK TO SAVEPOINT non_shape_disable_trigger;
RELEASE SAVEPOINT non_shape_disable_trigger;

SAVEPOINT non_shape_enable_trigger;
ALTER TABLE family_c DISABLE TRIGGER family_c_user_trigger;
SELECT snapshot_test.replace_snapshot('family_c', ARRAY['stale']::name[]) AS enable_trigger_damage_installed;
SELECT snapshot_test.snapshot_result('ENABLE TRIGGER stale precondition', 'family_c');
ALTER TABLE family_c ENABLE TRIGGER family_c_user_trigger;
SELECT snapshot_test.snapshot_result('ENABLE TRIGGER keeps stale snapshot', 'family_c');
ROLLBACK TO SAVEPOINT non_shape_enable_trigger;
RELEASE SAVEPOINT non_shape_enable_trigger;

SAVEPOINT non_shape_fillfactor;
SELECT snapshot_test.replace_snapshot('family_c', ARRAY['stale']::name[]) AS fillfactor_damage_installed;
SELECT snapshot_test.snapshot_result('SET fillfactor stale precondition', 'family_c');
ALTER TABLE family_c SET (fillfactor = 80);
SELECT snapshot_test.snapshot_result('SET fillfactor keeps stale snapshot', 'family_c');
ROLLBACK TO SAVEPOINT non_shape_fillfactor;
RELEASE SAVEPOINT non_shape_fillfactor;

SAVEPOINT non_shape_owner;
SELECT snapshot_test.replace_snapshot('family_c', ARRAY['stale']::name[]) AS owner_damage_installed;
SELECT snapshot_test.snapshot_result('OWNER TO stale precondition', 'family_c');
ALTER TABLE family_c OWNER TO snapshot_table_owner;
SELECT snapshot_test.snapshot_result('OWNER TO keeps stale snapshot', 'family_c');
ROLLBACK TO SAVEPOINT non_shape_owner;
RELEASE SAVEPOINT non_shape_owner;

SAVEPOINT non_shape_rename_constraint;
SELECT snapshot_test.replace_snapshot('family_c', ARRAY['stale']::name[]) AS rename_constraint_damage_installed;
SELECT snapshot_test.snapshot_result('RENAME CONSTRAINT stale precondition', 'family_c');
ALTER TABLE family_c RENAME CONSTRAINT family_c_pkey TO family_c_primary_key;
SELECT snapshot_test.snapshot_result('RENAME CONSTRAINT keeps stale snapshot', 'family_c');
ROLLBACK TO SAVEPOINT non_shape_rename_constraint;
RELEASE SAVEPOINT non_shape_rename_constraint;

SAVEPOINT non_shape_cluster;
SELECT snapshot_test.replace_snapshot('family_c', ARRAY['stale']::name[]) AS cluster_damage_installed;
SELECT snapshot_test.snapshot_result('CLUSTER ON stale precondition', 'family_c');
ALTER TABLE family_c CLUSTER ON family_c_payload_idx;
SELECT snapshot_test.snapshot_result('CLUSTER ON keeps stale snapshot', 'family_c');
ROLLBACK TO SAVEPOINT non_shape_cluster;
RELEASE SAVEPOINT non_shape_cluster;

SAVEPOINT non_shape_replica_full;
SELECT snapshot_test.replace_snapshot('family_c', ARRAY['stale']::name[]) AS replica_full_damage_installed;
SELECT snapshot_test.snapshot_result('REPLICA IDENTITY FULL stale precondition', 'family_c');
ALTER TABLE family_c REPLICA IDENTITY FULL;
SELECT snapshot_test.snapshot_result('REPLICA IDENTITY FULL keeps stale snapshot', 'family_c');
ROLLBACK TO SAVEPOINT non_shape_replica_full;
RELEASE SAVEPOINT non_shape_replica_full;

SAVEPOINT non_shape_replica_index;
SELECT snapshot_test.replace_snapshot('family_c', ARRAY['stale']::name[]) AS replica_index_damage_installed;
SELECT snapshot_test.snapshot_result('REPLICA IDENTITY USING INDEX stale precondition', 'family_c');
ALTER TABLE family_c REPLICA IDENTITY USING INDEX family_c_replica_idx;
SELECT snapshot_test.snapshot_result('REPLICA IDENTITY USING INDEX keeps stale snapshot', 'family_c');
ROLLBACK TO SAVEPOINT non_shape_replica_index;
RELEASE SAVEPOINT non_shape_replica_index;

\echo === managed base move and rename do not bless stale snapshot
CREATE SCHEMA snapshot_moved;
SAVEPOINT non_shape_set_schema;
SELECT snapshot_test.replace_snapshot('family_c', ARRAY['stale']::name[]) AS set_schema_damage_installed;
SELECT snapshot_test.snapshot_result('SET SCHEMA stale precondition', 'family_c');
SELECT snapshot_test.case_result(
    'SET SCHEMA preserves native refusal',
    $$ALTER TABLE snapshot_test.family_c SET SCHEMA snapshot_moved$$,
    '55006',
    $msg$cannot rename or move system-versioned table "snapshot_test.family_c"; to move it with its history, run sql_saga.drop_system_versioning('snapshot_test.family_c', cleanup => false) and sql_saga.drop_system_time_era('snapshot_test.family_c', cleanup => false), move or rename table "snapshot_test.family_c" and history table "snapshot_test.family_c_history" together, then run sql_saga.add_system_versioning(table_oid, history_table_name => name, history_table_must_exist => true)$msg$);
SELECT snapshot_test.snapshot_result('SET SCHEMA refusal keeps stale snapshot', 'family_c');
ROLLBACK TO SAVEPOINT non_shape_set_schema;
RELEASE SAVEPOINT non_shape_set_schema;

SAVEPOINT non_shape_rename_table;
SELECT snapshot_test.replace_snapshot('family_c', ARRAY['stale']::name[]) AS rename_table_damage_installed;
SELECT snapshot_test.snapshot_result('RENAME TABLE stale precondition', 'family_c');
SELECT snapshot_test.case_result(
    'RENAME TABLE preserves native refusal',
    $$ALTER TABLE snapshot_test.family_c RENAME TO family_c_renamed$$,
    '55006',
    $msg$cannot rename or move system-versioned table "snapshot_test.family_c"; to move it with its history, run sql_saga.drop_system_versioning('snapshot_test.family_c', cleanup => false) and sql_saga.drop_system_time_era('snapshot_test.family_c', cleanup => false), move or rename table "snapshot_test.family_c" and history table "snapshot_test.family_c_history" together, then run sql_saga.add_system_versioning(table_oid, history_table_name => name, history_table_must_exist => true)$msg$);
SELECT snapshot_test.snapshot_result('RENAME TABLE refusal keeps stale snapshot', 'family_c');
ROLLBACK TO SAVEPOINT non_shape_rename_table;
RELEASE SAVEPOINT non_shape_rename_table;

\echo === ALTER COLUMN TYPE preserves existing native behavior
SAVEPOINT alter_column_type;
SELECT snapshot_test.case_result(
    'ALTER COLUMN TYPE on managed base',
    $$ALTER TABLE snapshot_test.family_c ALTER COLUMN payload TYPE varchar(100)$$,
    '0A000',
    'cannot alter type of a column used by a view or rule');
ROLLBACK TO SAVEPOINT alter_column_type;
RELEASE SAVEPOINT alter_column_type;

\echo === inherited parent column-shape changes follow managed children
ALTER TABLE inherit_one_parent ADD COLUMN parent_added text;
SELECT snapshot_test.snapshot_result('one managed child follows parent ADD', 'inherit_one_child');

ALTER TABLE inherit_two_parent ADD COLUMN parent_added text;
SELECT snapshot_test.snapshot_result('first managed child follows shared parent ADD', 'inherit_two_child_a');
SELECT snapshot_test.snapshot_result('second managed child follows shared parent ADD', 'inherit_two_child_b');

SELECT snapshot_test.case_result(
    'parent DROP inherited used column preserves direct refusal',
    $$ALTER TABLE snapshot_test.inherit_drop_parent DROP COLUMN drop_me CASCADE$$,
    'P0001',
    'cannot drop view "snapshot_test.inherit_drop_child_with_history" because it is used in SYSTEM VERSIONING for table "snapshot_test.inherit_drop_child"');
SELECT snapshot_test.snapshot_result('refused parent DROP preserves child snapshot', 'inherit_drop_child');
SELECT format(
    'refused parent DROP family usable | with_history=%s | as_of=%s',
    (SELECT count(*) >= 0 FROM inherit_drop_child_with_history),
    (SELECT count(*) >= 0 FROM inherit_drop_child__as_of(pg_catalog.now())));
SELECT snapshot_test.case_result(
    'supported parent DROP pre-rebuild excludes child column',
    $$SELECT sql_saga.rebuild_system_versioning_views('snapshot_test.inherit_drop_child', ARRAY['drop_me']::name[])$$,
    NULL,
    NULL);
SELECT snapshot_test.case_result(
    'supported parent DROP after view exclusion',
    $$ALTER TABLE snapshot_test.inherit_drop_parent DROP COLUMN drop_me$$,
    NULL,
    NULL);
ALTER TABLE inherit_drop_child_history DROP COLUMN drop_me;
SELECT snapshot_test.case_result(
    'supported parent DROP final rebuild',
    $$SELECT sql_saga.rebuild_system_versioning_views('snapshot_test.inherit_drop_child')$$,
    NULL,
    NULL);
SELECT snapshot_test.snapshot_result('supported parent DROP refreshes child snapshot', 'inherit_drop_child');
SELECT format(
    'supported parent DROP family usable | with_history=%s | as_of=%s',
    (SELECT count(*) >= 0 FROM inherit_drop_child_with_history),
    (SELECT count(*) >= 0 FROM inherit_drop_child__as_of(pg_catalog.now())));

ALTER TABLE inherit_rename_parent RENAME COLUMN payload TO renamed_payload;
SELECT snapshot_test.snapshot_result('managed child follows parent RENAME', 'inherit_rename_child');

SAVEPOINT inherited_excluded_rename;
SELECT snapshot_test.case_result(
    'parent RENAME of child excluded column',
    $$ALTER TABLE snapshot_test.inherit_excluded_parent RENAME COLUMN excluded TO account$$,
    'P0001',
    'cannot drop or rename column "excluded" on table "snapshot_test.inherit_excluded_child" because it is excluded from system_time era');
ROLLBACK TO SAVEPOINT inherited_excluded_rename;
RELEASE SAVEPOINT inherited_excluded_rename;
SELECT snapshot_test.snapshot_result('excluded parent RENAME refusal preserves child', 'inherit_excluded_child');

\echo === ALTER TABLE ONLY parent inheritance behavior
SELECT snapshot_test.case_result(
    'ONLY parent ADD preserves native refusal',
    $$ALTER TABLE ONLY snapshot_test.inherit_only_add_parent ADD COLUMN only_added text$$,
    '42P16',
    'column must be added to child tables too');
SELECT snapshot_test.snapshot_result('ONLY parent ADD leaves child unchanged', 'inherit_only_add_child');

ALTER TABLE ONLY inherit_only_drop_parent DROP COLUMN keep_local;
SELECT snapshot_test.snapshot_result('ONLY parent DROP leaves child column local', 'inherit_only_drop_child');

SELECT snapshot_test.case_result(
    'ONLY parent RENAME preserves native refusal',
    $$ALTER TABLE ONLY snapshot_test.inherit_only_rename_parent RENAME COLUMN payload TO body$$,
    '42P16',
    'inherited column "payload" must be renamed in child tables too');
SELECT snapshot_test.snapshot_result('ONLY parent RENAME leaves child unchanged', 'inherit_only_rename_child');

\echo === inherited stale-child precondition follows direct DDL semantics
SELECT snapshot_test.replace_snapshot('inherit_stale_child', ARRAY['stale']::name[]) AS inherited_stale_damage_installed;
SELECT snapshot_test.snapshot_result('inherited child stale precondition', 'inherit_stale_child');
ALTER TABLE inherit_stale_parent ADD COLUMN parent_added text;
SELECT snapshot_test.snapshot_result('parent ADD absorbs inherited child pre-existing discrepancy', 'inherit_stale_child');

\echo === snapshot refresh helper is not externally executable
SET ROLE snapshot_table_owner;
SELECT snapshot_test.case_result(
    'table owner cannot refresh snapshot',
    $$SELECT sql_saga.__internal_refresh_system_time_columns_snapshot('snapshot_test.family_c'::regclass::oid)$$,
    '42501',
    'permission denied for function __internal_refresh_system_time_columns_snapshot');
RESET ROLE;
SET ROLE snapshot_unrelated_role;
SELECT snapshot_test.case_result(
    'unrelated role cannot refresh snapshot',
    $$SELECT sql_saga.__internal_refresh_system_time_columns_snapshot('snapshot_test.family_c'::regclass::oid)$$,
    '42501',
    'permission denied for function __internal_refresh_system_time_columns_snapshot');
RESET ROLE;

\echo === SECURITY DEFINER PUBLIC-execute allowlist invariant
SELECT count(*) = 0
FROM pg_catalog.pg_proc AS p
JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
WHERE n.nspname = 'sql_saga'
  AND p.prosecdef
  AND pg_catalog.has_function_privilege('public', p.oid, 'EXECUTE')
  AND p.oid::regprocedure::text <> ALL (ARRAY[
      'sql_saga.__internal_add_system_time_era(regclass,name,name,name,name,name,name[])',
      'sql_saga.__internal_ddl_command_affects_managed_object()',
      'sql_saga.__internal_reconcile_system_versioning_family(regclass,name[])',
      'sql_saga.__internal_sync_history_policies(oid,name[])',
      'sql_saga.add_current_view(regclass,name,name,text)',
      'sql_saga.add_era(regclass,name,name,boolean,name,name,name,boolean,boolean,boolean,regtype,name,name[])',
      'sql_saga.add_for_portion_of_view(regclass,name,name[])',
      'sql_saga.add_foreign_key(regclass,name[],regclass,name[],name,sql_saga.fk_match_types,sql_saga.fk_actions,sql_saga.fk_actions,name,boolean)',
      'sql_saga.add_regular_foreign_key(regclass,name[],name,sql_saga.fk_match_types,sql_saga.fk_actions,sql_saga.fk_actions,name,name,text,name,name,boolean)',
      'sql_saga.add_synchronize_temporal_columns_trigger(regclass,name)',
      'sql_saga.add_system_versioning(regclass,name,name,name,name,name,name)',
      'sql_saga.add_temporal_foreign_key(regclass,name[],name,name,sql_saga.fk_match_types,sql_saga.fk_actions,sql_saga.fk_actions,name,name,name,boolean)',
      'sql_saga.add_unique_key(regclass,name[],name,sql_saga.unique_key_type,boolean,name,name,name,text,name[])',
      'sql_saga.drop_current_view(regclass,name,sql_saga.drop_behavior)',
      'sql_saga.drop_era(regclass,name,sql_saga.drop_behavior,boolean)',
      'sql_saga.drop_for_portion_of_view(regclass,name,sql_saga.drop_behavior,boolean)',
      'sql_saga.drop_foreign_key_by_name(regclass,name,boolean)',
      'sql_saga.drop_foreign_key(regclass,name[],name,sql_saga.drop_behavior,boolean)',
      'sql_saga.drop_protection()',
      'sql_saga.drop_synchronize_temporal_columns_trigger(regclass,name)',
      'sql_saga.drop_system_time_era(regclass,sql_saga.drop_behavior,boolean)',
      'sql_saga.drop_system_versioning(regclass,sql_saga.drop_behavior,boolean)',
      'sql_saga.drop_unique_key_by_name(regclass,name,sql_saga.drop_behavior,boolean)',
      'sql_saga.drop_unique_key(regclass,name[],name,sql_saga.drop_behavior,boolean)',
      'sql_saga.generated_always_as_row_start_end()',
      'sql_saga.health_checks()',
      'sql_saga.rename_following()',
      'sql_saga.rebuild_system_versioning_views(regclass,name[])',
      'sql_saga.set_era_ephemeral_columns(regclass,name,name[])',
      'sql_saga.set_system_time_era_excluded_columns(regclass,name[])',
      'sql_saga.truncate_system_versioning()',
      'sql_saga.write_history()'
  ]::text[]);

ROLLBACK;
\i sql/include/test_teardown.sql
