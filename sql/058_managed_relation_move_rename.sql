\set ECHO none
\set QUIET 1
\i sql/include/test_setup.sql
\o /dev/null

SET client_min_messages = warning;

CREATE SCHEMA move_guard;
CREATE SCHEMA move_destination;
CREATE SCHEMA "Quoted Schema";
SET search_path = move_guard, public;

CREATE FUNCTION move_guard.catalog_state() RETURNS jsonb LANGUAGE sql STABLE AS $catalog_state$
SELECT jsonb_build_object(
    'era', coalesce((SELECT jsonb_agg(to_jsonb(x) ORDER BY x.table_schema, x.table_name, x.era_name) FROM sql_saga.era AS x), '[]'::jsonb),
    'system_time_era', coalesce((SELECT jsonb_agg(to_jsonb(x) ORDER BY x.table_schema, x.table_name, x.era_name) FROM sql_saga.system_time_era AS x), '[]'::jsonb),
    'unique_keys', coalesce((SELECT jsonb_agg(to_jsonb(x) ORDER BY x.unique_key_name) FROM sql_saga.unique_keys AS x), '[]'::jsonb),
    'foreign_keys', coalesce((SELECT jsonb_agg(to_jsonb(x) ORDER BY x.foreign_key_name) FROM sql_saga.foreign_keys AS x), '[]'::jsonb),
    'system_versioning', coalesce((SELECT jsonb_agg(to_jsonb(x) ORDER BY x.table_schema, x.table_name) FROM sql_saga.system_versioning AS x), '[]'::jsonb),
    'updatable_view', coalesce((SELECT jsonb_agg(to_jsonb(x) ORDER BY x.view_schema, x.view_name) FROM sql_saga.updatable_view AS x), '[]'::jsonb)
);
$catalog_state$;

CREATE FUNCTION move_guard.attempt(
    case_name text,
    ddl text,
    verify_sql text
) RETURNS TABLE (
    test_case text,
    sqlstate text,
    message text,
    catalog_unchanged boolean,
    family_usable boolean
) LANGUAGE plpgsql AS $attempt$
DECLARE
    before_state jsonb;
    caught_state text;
    caught_message text;
    usable boolean := true;
BEGIN
    before_state := move_guard.catalog_state();
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

    BEGIN
        EXECUTE verify_sql;
    EXCEPTION WHEN OTHERS THEN
        usable := false;
    END;

    RETURN QUERY SELECT case_name, caught_state, caught_message,
        before_state = move_guard.catalog_state(), usable;
END;
$attempt$;

CREATE TEMP TABLE results (
    seq integer GENERATED ALWAYS AS IDENTITY,
    test_case text,
    sqlstate text,
    message text,
    catalog_unchanged boolean,
    family_usable boolean
);

-- SYSTEM VERSIONING base and derived family.
CREATE TABLE sv_base (id integer PRIMARY KEY, payload text);
SELECT sql_saga.add_system_versioning('sv_base');

-- Era-only base.
CREATE TABLE era_base (id integer, payload text, valid_range daterange, valid_from date, valid_until date);
SELECT sql_saga.add_era('era_base', 'valid_range');

-- Era with both updatable-view classes.
CREATE FUNCTION public.move_guard_now() RETURNS date LANGUAGE sql STABLE AS $$ SELECT DATE '2026-10-16' $$;
CREATE TABLE view_base (id integer, payload text, valid_range daterange, valid_from date, valid_until date);
SELECT sql_saga.add_era('view_base', 'valid_range');
SELECT sql_saga.add_unique_key('view_base', ARRAY['id']);
SELECT sql_saga.add_for_portion_of_view('view_base');
SELECT sql_saga.add_current_view('view_base', current_func_name => 'public.move_guard_now()');

-- Referenced and referencing temporal-FK bases.
CREATE TABLE fk_parent (id integer, valid_range daterange, valid_from date, valid_until date);
CREATE TABLE fk_child (id integer, parent_id integer, valid_range daterange, valid_from date, valid_until date);
SELECT sql_saga.add_era('fk_parent', 'valid_range', add_bounds_check => false);
SELECT sql_saga.add_unique_key('fk_parent', ARRAY['id']);
SELECT sql_saga.add_era('fk_child', 'valid_range', add_bounds_check => false);
SELECT sql_saga.add_unique_key('fk_child', ARRAY['id']);
SELECT sql_saga.add_temporal_foreign_key('fk_child', ARRAY['parent_id'], 'valid', 'fk_parent_id_valid');

-- Catalogued regular-to-temporal FK child, which has no era of its own.
CREATE TABLE regular_child (id integer, parent_id integer);
SELECT sql_saga.add_regular_foreign_key(
    'regular_child', ARRAY['parent_id'], 'fk_parent_id_valid', foreign_key_name => 'regular_child_fk');

-- Quoted system-versioned base.
CREATE TABLE "Quoted Schema"."Odd Table" (id integer PRIMARY KEY, payload text);
SELECT sql_saga.add_system_versioning('"Quoted Schema"."Odd Table"');

-- Non-managed controls must remain legal.
CREATE TABLE unmanaged (id integer);
ALTER TABLE unmanaged RENAME TO unmanaged_renamed;
ALTER TABLE unmanaged_renamed SET SCHEMA move_destination;

-- Base identities, including IF EXISTS and every important layout.
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('sv base rename', 'ALTER TABLE move_guard.sv_base RENAME TO sv_base_new', 'SELECT count(*) FROM move_guard.sv_base_with_history');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('sv base move', 'ALTER TABLE move_guard.sv_base SET SCHEMA move_destination', 'SELECT count(*) FROM move_guard.sv_base_with_history');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('sv base IF EXISTS rename', 'ALTER TABLE IF EXISTS move_guard.sv_base RENAME TO sv_base_new', 'SELECT count(*) FROM move_guard.sv_base_with_history');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('sv base IF EXISTS move', 'ALTER TABLE IF EXISTS move_guard.sv_base SET SCHEMA move_destination', 'SELECT count(*) FROM move_guard.sv_base_with_history');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('era base rename', 'ALTER TABLE move_guard.era_base RENAME TO era_base_new', 'SELECT count(*) FROM move_guard.era_base');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('era base move', 'ALTER TABLE move_guard.era_base SET SCHEMA move_destination', 'SELECT count(*) FROM move_guard.era_base');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('view base rename', 'ALTER TABLE move_guard.view_base RENAME TO view_base_new', 'SELECT count(*) FROM move_guard.view_base__current_valid');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('view base move', 'ALTER TABLE move_guard.view_base SET SCHEMA move_destination', 'SELECT count(*) FROM move_guard.view_base__for_portion_of_valid');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('FK parent rename', 'ALTER TABLE move_guard.fk_parent RENAME TO fk_parent_new', 'SELECT count(*) FROM move_guard.fk_parent');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('FK parent move', 'ALTER TABLE move_guard.fk_parent SET SCHEMA move_destination', 'SELECT count(*) FROM move_guard.fk_parent');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('FK child rename', 'ALTER TABLE move_guard.fk_child RENAME TO fk_child_new', 'SELECT count(*) FROM move_guard.fk_child');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('FK child move', 'ALTER TABLE move_guard.fk_child SET SCHEMA move_destination', 'SELECT count(*) FROM move_guard.fk_child');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('regular FK child rename', 'ALTER TABLE move_guard.regular_child RENAME TO regular_child_new', 'SELECT count(*) FROM move_guard.regular_child');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('regular FK child move', 'ALTER TABLE move_guard.regular_child SET SCHEMA move_destination', 'SELECT count(*) FROM move_guard.regular_child');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('quoted sv base rename', 'ALTER TABLE "Quoted Schema"."Odd Table" RENAME TO "New Odd Table"', 'SELECT count(*) FROM "Quoted Schema"."Odd Table_with_history"');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('quoted sv base move', 'ALTER TABLE "Quoted Schema"."Odd Table" SET SCHEMA move_destination', 'SELECT count(*) FROM "Quoted Schema"."Odd Table_with_history"');

-- History table.
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('history rename', 'ALTER TABLE move_guard.sv_base_history RENAME TO sv_history_new', 'SELECT count(*) FROM move_guard.sv_base_with_history');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('history move', 'ALTER TABLE move_guard.sv_base_history SET SCHEMA move_destination', 'SELECT count(*) FROM move_guard.sv_base_with_history');

-- SYSTEM VERSIONING generated views, using both ALTER VIEW and ALTER TABLE syntax.
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('with-history ALTER VIEW rename', 'ALTER VIEW move_guard.sv_base_with_history RENAME TO sv_all_new', 'SELECT count(*) FROM move_guard.sv_base_with_history');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('with-history ALTER VIEW move', 'ALTER VIEW move_guard.sv_base_with_history SET SCHEMA move_destination', 'SELECT count(*) FROM move_guard.sv_base_with_history');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('with-history ALTER TABLE rename', 'ALTER TABLE move_guard.sv_base_with_history RENAME TO sv_all_new', 'SELECT count(*) FROM move_guard.sv_base_with_history');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('with-history ALTER TABLE move', 'ALTER TABLE move_guard.sv_base_with_history SET SCHEMA move_destination', 'SELECT count(*) FROM move_guard.sv_base_with_history');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('gated ALTER VIEW rename', 'ALTER VIEW move_guard.sv_base_history_gated RENAME TO sv_gate_new', 'SELECT count(*) FROM move_guard.sv_base_history_gated');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('gated ALTER VIEW move', 'ALTER VIEW move_guard.sv_base_history_gated SET SCHEMA move_destination', 'SELECT count(*) FROM move_guard.sv_base_history_gated');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('gated ALTER TABLE rename', 'ALTER TABLE move_guard.sv_base_history_gated RENAME TO sv_gate_new', 'SELECT count(*) FROM move_guard.sv_base_history_gated');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('gated ALTER TABLE move', 'ALTER TABLE move_guard.sv_base_history_gated SET SCHEMA move_destination', 'SELECT count(*) FROM move_guard.sv_base_history_gated');

-- Updatable generated views and both syntaxes.
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('current ALTER VIEW rename', 'ALTER VIEW move_guard.view_base__current_valid RENAME TO current_new', 'SELECT count(*) FROM move_guard.view_base__current_valid');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('current ALTER VIEW move', 'ALTER VIEW move_guard.view_base__current_valid SET SCHEMA move_destination', 'SELECT count(*) FROM move_guard.view_base__current_valid');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('current ALTER TABLE rename', 'ALTER TABLE move_guard.view_base__current_valid RENAME TO current_new', 'SELECT count(*) FROM move_guard.view_base__current_valid');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('current ALTER TABLE move', 'ALTER TABLE move_guard.view_base__current_valid SET SCHEMA move_destination', 'SELECT count(*) FROM move_guard.view_base__current_valid');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('for-portion ALTER VIEW rename', 'ALTER VIEW move_guard.view_base__for_portion_of_valid RENAME TO portion_new', 'SELECT count(*) FROM move_guard.view_base__for_portion_of_valid');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('for-portion ALTER VIEW move', 'ALTER VIEW move_guard.view_base__for_portion_of_valid SET SCHEMA move_destination', 'SELECT count(*) FROM move_guard.view_base__for_portion_of_valid');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('for-portion ALTER TABLE rename', 'ALTER TABLE move_guard.view_base__for_portion_of_valid RENAME TO portion_new', 'SELECT count(*) FROM move_guard.view_base__for_portion_of_valid');
INSERT INTO results (test_case, sqlstate, message, catalog_unchanged, family_usable)
SELECT * FROM move_guard.attempt('for-portion ALTER TABLE move', 'ALTER TABLE move_guard.view_base__for_portion_of_valid SET SCHEMA move_destination', 'SELECT count(*) FROM move_guard.view_base__for_portion_of_valid');

-- Functional smoke after every refusal has preserved the original family identities.
INSERT INTO sv_base (id, payload) VALUES (1, 'before');
UPDATE sv_base SET payload = 'after' WHERE id = 1;
INSERT INTO era_base (id, payload, valid_from, valid_until) VALUES (1, 'era', '2026-01-01', 'infinity');
INSERT INTO fk_parent (id, valid_from, valid_until) VALUES (1, '2026-01-01', 'infinity');
INSERT INTO fk_child (id, parent_id, valid_from, valid_until) VALUES (1, 1, '2026-01-01', 'infinity');
INSERT INTO regular_child (id, parent_id) VALUES (1, 1);

\o
\set QUIET 0
\pset format unaligned
\pset tuples_only on
SELECT seq, test_case, sqlstate, message, catalog_unchanged, family_usable FROM results ORDER BY seq;
SELECT 'unmanaged_rename_and_move_worked', to_regclass('move_destination.unmanaged_renamed') IS NOT NULL;
SELECT 'family_smoke',
    (SELECT count(*) FROM sv_base_history) = 1,
    (SELECT count(*) FROM sv_base_with_history) = 2,
    (SELECT count(*) FROM view_base__current_valid) = 0,
    (SELECT count(*) FROM view_base__for_portion_of_valid) = 0,
    (SELECT count(*) FROM fk_child) = 1,
    (SELECT count(*) FROM regular_child) = 1;

\set QUIET 1
\i sql/include/test_teardown.sql
