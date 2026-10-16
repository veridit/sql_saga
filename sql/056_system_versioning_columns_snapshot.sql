\set ECHO none
\i sql/include/test_setup.sql
\set SHOW_CONTEXT never
\pset format unaligned
\pset tuples_only on
SET client_min_messages = warning;

BEGIN;
CREATE SCHEMA snapshot_test;
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

CREATE TABLE family_a (id int PRIMARY KEY, tenant text, payload text);
CREATE TABLE family_b (id int PRIMARY KEY, tenant text, payload text);
SELECT sql_saga.add_system_versioning('family_a');
SELECT sql_saga.add_system_versioning('family_b');
SELECT sql_saga.set_system_time_era_excluded_columns('family_a', ARRAY['tenant']::name[]);
SELECT sql_saga.set_system_time_era_excluded_columns('family_b', ARRAY['tenant']::name[]);

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

ROLLBACK;
\i sql/include/test_teardown.sql
