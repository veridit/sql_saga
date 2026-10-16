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
CREATE TABLE era_wrong (id integer, valid_from date, valid_until date, valid_range daterange);
SELECT sql_saga.add_era('era_wrong', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_era('period_parent', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('period_parent', ARRAY['id']::name[], 'valid', key_type => 'primary', unique_key_name => 'period_parent_primary');
SELECT sql_saga.add_unique_key('period_parent', ARRAY['id']::name[], 'valid', key_type => 'predicated', predicate => 'category IS NOT NULL', unique_key_name => 'period_parent_predicated');
SELECT sql_saga.add_unique_key('period_parent', ARRAY['category']::name[], 'valid', key_type => 'natural', unique_key_name => 'period_parent_natural', enforce_consistency_with_primary_key => false);
SELECT sql_saga.add_for_portion_of_view('period_parent', 'valid');
SELECT sql_saga.add_era('period_child', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('period_child', ARRAY['id']::name[], 'valid', key_type => 'primary', unique_key_name => 'period_child_primary');
SELECT sql_saga.add_foreign_key('period_child', ARRAY['parent_id']::name[], 'period_parent', ARRAY['id']::name[]);

\o
SELECT integrity_g4.valid('complete inventory baseline');

-- The explicit allowlist is derived from doc/api.md sections before
-- "Internal and Helper Functions". Everything else in sql_saga is private.
CREATE TEMP TABLE integrity_public_api (name name PRIMARY KEY) ON COMMIT DROP;
INSERT INTO integrity_public_api VALUES
('add_era'),('drop_era'),('set_era_ephemeral_columns'),
('add_unique_key'),('drop_unique_key'),('drop_unique_key_by_name'),
('add_foreign_key'),('add_regular_foreign_key'),('add_temporal_foreign_key'),('drop_foreign_key'),('drop_foreign_key_by_name'),
('add_current_view'),('add_for_portion_of_view'),('drop_current_view'),('drop_for_portion_of_view'),
('disable_temporal_triggers'),('enable_temporal_triggers'),('manage_temporal_fk_triggers'),('temporal_merge'),
('add_system_versioning'),('drop_system_time_era'),('drop_system_versioning'),('rebuild_system_versioning_views'),
('set_system_time_era_excluded_columns'),('sync_history_policies'),
('covers_without_gaps'),('first'),('check_integrity');

SELECT p.prorettype IN ('event_trigger'::regtype, 'trigger'::regtype) AS health_checks_is_noncallable_trigger_function
FROM pg_proc AS p
JOIN pg_namespace AS n ON n.oid=p.pronamespace
WHERE n.nspname='sql_saga' AND p.proname='health_checks';

SELECT count(*) = 0 AS every_non_api_sql_saga_function_is_private
FROM pg_proc AS p
JOIN pg_namespace AS n ON n.oid=p.pronamespace
WHERE n.nspname='sql_saga'
  AND p.prorettype NOT IN ('event_trigger'::regtype, 'trigger'::regtype)
  AND NOT EXISTS (SELECT FROM integrity_public_api AS a WHERE a.name=p.proname)
  AND EXISTS (
      SELECT FROM aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) AS x
      WHERE x.grantee=0 AND x.privilege_type='EXECUTE');

SELECT integrity_g4.acl_attempt(
    'canonical generator private',
    $$SELECT sql_saga.__internal_system_versioning_query_functions('integrity_g4.sv'::regclass::oid)$$,
    '__internal_system_versioning_query_functions');

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
CREATE FUNCTION sv__as_of(p_at timestamptz) RETURNS SETOF sv_with_history LANGUAGE sql STABLE AS $user_body$ SELECT * FROM integrity_g4.sv_with_history WHERE false $user_body$;
RESET session_replication_role;
INSERT INTO replacement_state
SELECT 'function', p.oid::regprocedure::text, p.prosrc
FROM pg_proc AS p WHERE p.oid='integrity_g4.sv__as_of(timestamptz)'::regprocedure;
SET LOCAL client_min_messages = notice;
SELECT sql_saga.drop_system_versioning('integrity_g4.sv');
SET LOCAL client_min_messages = warning;
SELECT EXISTS (SELECT FROM replacement_state AS s JOIN pg_proc AS p ON p.oid=to_regprocedure(s.identity) WHERE s.kind='function' AND p.prosrc=s.definition) AS noncanonical_function_identity_and_definition_survive;
SELECT NOT EXISTS (SELECT FROM sql_saga.system_versioning WHERE table_schema='integrity_g4' AND table_name='sv') AS function_family_metadata_removed;
ROLLBACK TO SAVEPOINT preserve_function;
RELEASE SAVEPOINT preserve_function;

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
    EXECUTE format('ALTER TABLE integrity_g4.period_parent ADD CONSTRAINT %I CHECK (true)', n);
END $damage$;
RESET session_replication_role;
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

SAVEPOINT preserve_index;
SET session_replication_role = replica;
DO $damage$ DECLARE n name; BEGIN
    SELECT unique_constraint INTO n FROM sql_saga.unique_keys WHERE unique_key_name='period_parent_natural';
    EXECUTE format('ALTER TABLE integrity_g4.period_parent DROP CONSTRAINT %I', n);
    EXECUTE format('CREATE INDEX %I ON integrity_g4.period_parent (category)', n);
END $damage$;
RESET session_replication_role;
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
