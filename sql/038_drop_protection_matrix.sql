\set ECHO none
\i sql/include/test_setup.sql
\set SHOW_CONTEXT never
\pset format unaligned
\pset tuples_only on
SET client_min_messages = warning;

BEGIN;
UPDATE sql_saga.installation SET token = 'newtoken';
CREATE SCHEMA dp_matrix;
SET search_path = dp_matrix, public;

CREATE FUNCTION dp_matrix.probe(command text, expected_sqlstate text, expected_message text)
RETURNS text LANGUAGE plpgsql AS $probe$
DECLARE
    actual_sqlstate text;
    actual_message text;
BEGIN
    BEGIN
        EXECUTE command;
        RAISE EXCEPTION USING ERRCODE = 'Z0001', MESSAGE = 'rollback successful command';
    EXCEPTION
        WHEN SQLSTATE 'Z0001' THEN
            IF expected_sqlstate IS NULL THEN RETURN 'ok'; END IF;
            RETURN format('unexpected success, expected %s: %s', expected_sqlstate, expected_message);
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS actual_sqlstate = RETURNED_SQLSTATE, actual_message = MESSAGE_TEXT;
            IF expected_sqlstate IS NULL THEN RETURN format('unexpected error %s: %s', actual_sqlstate, actual_message); END IF;
            IF (actual_sqlstate, actual_message) IS DISTINCT FROM (expected_sqlstate, expected_message) THEN
                RETURN format('unexpected error %s: %s', actual_sqlstate, actual_message);
            END IF;
            RETURN format('%s: %s', actual_sqlstate, actual_message);
    END;
END;
$probe$;

CREATE FUNCTION dp_matrix.system_family(family_name text, family_schema text DEFAULT 'dp_matrix')
RETURNS void LANGUAGE plpgsql AS $family$
BEGIN
    EXECUTE format('CREATE TABLE %I.%I (id int PRIMARY KEY, tenant text, payload text)', family_schema, family_name);
    PERFORM sql_saga.add_system_versioning(format('%I.%I', family_schema, family_name)::regclass);
    PERFORM sql_saga.set_system_time_era_excluded_columns(format('%I.%I', family_schema, family_name)::regclass, ARRAY['tenant']::name[]);
    EXECUTE format('ALTER TABLE %I.%I ENABLE ROW LEVEL SECURITY', family_schema, family_name);
    EXECUTE format('CREATE POLICY tenant_policy ON %I.%I FOR SELECT USING (tenant = current_user)', family_schema, family_name);
END;
$family$;

CREATE FUNCTION dp_matrix.case_result(case_name text, command text, expected_sqlstate text, expected_message text)
RETURNS text LANGUAGE sql AS $case_result$
    SELECT case_name || ' | ' || dp_matrix.probe(command, expected_sqlstate, expected_message)
$case_result$;

\echo === direct protected object matrix
SELECT dp_matrix.system_family('sv');
SELECT dp_matrix.case_result('direct infinity constraint', $$ALTER TABLE dp_matrix.sv DROP CONSTRAINT sv_system_valid_range_infinity_check$$, 'P0001', 'cannot drop constraint "sv_system_valid_range_infinity_check" on table "dp_matrix.sv" because it is used in SYSTEM_TIME period');
SELECT dp_matrix.case_result('direct generated-always trigger', $$DROP TRIGGER sv_system_time_generated_always ON dp_matrix.sv$$, 'P0001', 'cannot drop trigger "sv_system_time_generated_always" on table "dp_matrix.sv" because it is used in SYSTEM_TIME period');
SELECT dp_matrix.case_result('direct write-history trigger', $$DROP TRIGGER sv_system_time_write_history ON dp_matrix.sv$$, 'P0001', 'cannot drop trigger "sv_system_time_write_history" on table "dp_matrix.sv" because it is used in SYSTEM_TIME period');
SELECT dp_matrix.case_result('direct truncate trigger', $$DROP TRIGGER sv_truncate ON dp_matrix.sv$$, 'P0001', 'cannot drop trigger "sv_truncate" on table "dp_matrix.sv" because it is used in SYSTEM_TIME period');
SELECT dp_matrix.case_result('direct excluded column', $$ALTER TABLE dp_matrix.sv DROP COLUMN tenant CASCADE$$, 'P0001', 'cannot drop or rename column "tenant" on table "dp_matrix.sv" because it is excluded from system_time era');
SELECT dp_matrix.case_result('direct history table', $$DROP TABLE dp_matrix.sv_history CASCADE$$, 'P0001', 'cannot drop table "dp_matrix.sv_history" because it is used in SYSTEM VERSIONING for table "dp_matrix.sv"');
SELECT dp_matrix.case_result('direct public history view', $$DROP VIEW dp_matrix.sv_with_history CASCADE$$, 'P0001', 'cannot drop view "dp_matrix.sv_with_history" because it is used in SYSTEM VERSIONING for table "dp_matrix.sv"');
SELECT dp_matrix.case_result('direct gated history view', $$DROP VIEW dp_matrix.sv_history_gated CASCADE$$, 'P0001', 'cannot drop view "dp_matrix.sv_history_gated" because it is used in SYSTEM VERSIONING for table "dp_matrix.sv"');
SELECT dp_matrix.case_result('direct as-of function', $$DROP FUNCTION dp_matrix.sv__as_of(timestamptz) CASCADE$$, 'P0001', 'cannot drop function "dp_matrix.sv__as_of(timestamp with time zone)" because it is used in SYSTEM VERSIONING for table "dp_matrix.sv"');
SELECT dp_matrix.case_result('direct between function', $$DROP FUNCTION dp_matrix.sv__between(timestamptz,timestamptz) CASCADE$$, 'P0001', 'cannot drop function "dp_matrix.sv__between(timestamp with time zone,timestamp with time zone)" because it is used in SYSTEM VERSIONING for table "dp_matrix.sv"');
SELECT dp_matrix.case_result('direct between-symmetric function', $$DROP FUNCTION dp_matrix.sv__between_symmetric(timestamptz,timestamptz) CASCADE$$, 'P0001', 'cannot drop function "dp_matrix.sv__between_symmetric(timestamp with time zone,timestamp with time zone)" because it is used in SYSTEM VERSIONING for table "dp_matrix.sv"');
SELECT dp_matrix.case_result('direct from-to function', $$DROP FUNCTION dp_matrix.sv__from_to(timestamptz,timestamptz) CASCADE$$, 'P0001', 'cannot drop function "dp_matrix.sv__from_to(timestamp with time zone,timestamp with time zone)" because it is used in SYSTEM VERSIONING for table "dp_matrix.sv"');
SELECT dp_matrix.case_result('direct managed policy', $$DROP POLICY sql_saga_history_reader_newtoken ON dp_matrix.sv_history$$, 'P0001', 'cannot drop or alter policy "sql_saga_history_reader_newtoken" on "dp_matrix.sv_history" because it is managed by sql_saga for table "dp_matrix.sv"');

CREATE TABLE dp_matrix.era_t (id int, valid_range daterange NOT NULL, valid_from date, valid_until date, payload text);
SELECT sql_saga.add_era('dp_matrix.era_t', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT dp_matrix.case_result('direct era period column', $$ALTER TABLE dp_matrix.era_t DROP COLUMN valid_from CASCADE$$, 'P0001', 'cannot drop column "dp_matrix.era_t.valid_from" because it is part of the period "valid"');
SELECT dp_matrix.case_result('direct era bounds constraint cleanup', $$ALTER TABLE dp_matrix.era_t DROP CONSTRAINT era_t_valid_check$$, NULL, NULL);
SELECT dp_matrix.case_result('direct era synchronization trigger cleanup', $$DROP TRIGGER era_t_valid_sync_temporal_trg ON dp_matrix.era_t$$, NULL, NULL);

ALTER TABLE dp_matrix.era_t ADD CONSTRAINT era_t_pk PRIMARY KEY (id, valid_range WITHOUT OVERLAPS);
SELECT sql_saga.add_unique_key('dp_matrix.era_t', ARRAY['id'], 'valid', key_type => 'primary', unique_key_name => 'era_primary', unique_constraint => 'era_t_pk');
SELECT dp_matrix.case_result('direct temporal unique constraint', $$ALTER TABLE dp_matrix.era_t DROP CONSTRAINT era_t_pk$$, 'P0001', 'cannot drop constraint "era_t_pk" on table "dp_matrix.era_t" because it is used in era unique key "era_primary"');
SELECT dp_matrix.case_result('direct standard temporal backing index', $$DROP INDEX dp_matrix.era_t_pk$$, '2BP01', 'cannot drop index era_t_pk because constraint era_t_pk on table era_t requires it');

CREATE TABLE dp_matrix.pred_t (id int, active boolean, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('dp_matrix.pred_t', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('dp_matrix.pred_t', ARRAY['id'], 'valid', key_type => 'predicated', unique_key_name => 'pred_key', predicate => 'active');
SELECT dp_matrix.case_result('direct predicated temporal constraint', $$ALTER TABLE dp_matrix.pred_t DROP CONSTRAINT pred_key_excl$$, 'P0001', 'cannot drop constraint "pred_key_excl" on table "dp_matrix.pred_t" because it is used in era unique key "pred_key"');
SELECT dp_matrix.case_result('direct predicated temporal backing index', $$DROP INDEX dp_matrix.pred_key_excl$$, '2BP01', 'cannot drop index pred_key_excl because constraint pred_key_excl on table pred_t requires it');

CREATE TABLE dp_matrix.view_t (id int, valid_range daterange NOT NULL, PRIMARY KEY (id, valid_range WITHOUT OVERLAPS));
SELECT sql_saga.add_era('dp_matrix.view_t', 'valid_range', 'valid');
SELECT sql_saga.add_for_portion_of_view('dp_matrix.view_t', 'valid');
SELECT sql_saga.add_current_view('dp_matrix.view_t', 'valid');
SELECT dp_matrix.case_result('direct FOR PORTION OF view', $$DROP VIEW dp_matrix.view_t__for_portion_of_valid$$, 'P0001', 'cannot drop view "dp_matrix.view_t__for_portion_of_valid", call "sql_saga.drop_for_portion_of_view()" instead');
SELECT dp_matrix.case_result('direct FOR PORTION OF trigger', $$DROP TRIGGER for_portion_of_valid ON dp_matrix.view_t__for_portion_of_valid$$, 'P0001', 'cannot drop trigger "for_portion_of_valid" on view "dp_matrix.view_t__for_portion_of_valid" because it is part of an updatable view for era "valid" on table "dp_matrix.view_t"');
SELECT dp_matrix.case_result('direct current view', $$DROP VIEW dp_matrix.view_t__current_valid$$, 'P0001', 'cannot drop view "dp_matrix.view_t__current_valid", call "sql_saga.drop_current_view()" instead');
SELECT dp_matrix.case_result('direct current trigger', $$DROP TRIGGER current_valid ON dp_matrix.view_t__current_valid$$, 'P0001', 'cannot drop trigger "current_valid" on view "dp_matrix.view_t__current_valid" because it is part of an updatable view for era "valid" on table "dp_matrix.view_t"');
SELECT dp_matrix.case_result('direct updatable-view identifier', $$ALTER TABLE dp_matrix.view_t DROP CONSTRAINT view_t_pkey$$, 'P0001', 'table "dp_matrix.view_t" must have a primary key or a single-column temporal unique key to support its for_portion_of view "dp_matrix.view_t__for_portion_of_valid" for era "valid"; drop that view first with sql_saga.drop_for_portion_of_view()');

CREATE TABLE dp_matrix.fk_parent (id int, valid_range daterange NOT NULL);
CREATE TABLE dp_matrix.fk_temporal_child (parent_id int, valid_range daterange NOT NULL);
CREATE TABLE dp_matrix.fk_regular_child (parent_id int);
SELECT sql_saga.add_era('dp_matrix.fk_parent', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('dp_matrix.fk_parent', ARRAY['id'], 'valid', key_type => 'primary', unique_key_name => 'fk_parent_key');
SELECT sql_saga.add_era('dp_matrix.fk_temporal_child', 'valid_range', 'valid');
SELECT sql_saga.add_temporal_foreign_key('dp_matrix.fk_temporal_child', ARRAY['parent_id'], 'valid', 'fk_parent_key', foreign_key_name => 'temporal_fk');
SELECT sql_saga.add_regular_foreign_key('dp_matrix.fk_regular_child', ARRAY['parent_id'], 'fk_parent_key', foreign_key_name => 'regular_fk');
SELECT dp_matrix.case_result('direct temporal FK constraint cleanup', $$ALTER TABLE dp_matrix.fk_temporal_child DROP CONSTRAINT temporal_fk$$, NULL, NULL);
SELECT dp_matrix.case_result('direct regular FK uk-update trigger', $$DROP TRIGGER regular_fk_uk_update ON dp_matrix.fk_parent$$, 'P0001', 'cannot drop trigger "regular_fk_uk_update" on table "dp_matrix.fk_parent" because it is used in era foreign key "regular_fk"');
SELECT dp_matrix.case_result('direct regular FK uk-delete trigger', $$DROP TRIGGER regular_fk_uk_delete ON dp_matrix.fk_parent$$, 'P0001', 'cannot drop trigger "regular_fk_uk_delete" on table "dp_matrix.fk_parent" because it is used in era foreign key "regular_fk"');

\echo === sanctioned API and guard matrix
SELECT dp_matrix.case_result('drop_for_portion_of_view API', $$SELECT sql_saga.drop_for_portion_of_view('dp_matrix.view_t', 'valid')$$, NULL, NULL);
SELECT dp_matrix.case_result('drop_current_view API', $$SELECT sql_saga.drop_current_view('dp_matrix.view_t', 'valid')$$, NULL, NULL);
SELECT dp_matrix.case_result('drop_foreign_key temporal API', $$SELECT sql_saga.drop_foreign_key('dp_matrix.fk_temporal_child', ARRAY['parent_id'], 'valid')$$, NULL, NULL);
SELECT dp_matrix.case_result('drop_foreign_key regular API', $$SELECT sql_saga.drop_foreign_key('dp_matrix.fk_regular_child', ARRAY['parent_id'])$$, NULL, NULL);
CREATE TABLE dp_matrix.drop_unique_t (id int, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('dp_matrix.drop_unique_t', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('dp_matrix.drop_unique_t', ARRAY['id'], 'valid', key_type => 'primary', unique_key_name => 'drop_unique_key');
SELECT dp_matrix.case_result('drop_unique_key API', $$SELECT sql_saga.drop_unique_key('dp_matrix.drop_unique_t', ARRAY['id'], 'valid')$$, NULL, NULL);
CREATE TABLE dp_matrix.drop_era_t (id int, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('dp_matrix.drop_era_t', 'valid_range', 'valid');
SELECT dp_matrix.case_result('drop_era API', $$SELECT sql_saga.drop_era('dp_matrix.drop_era_t', 'valid')$$, NULL, NULL);
SELECT dp_matrix.case_result('drop_system_versioning API', $$SELECT sql_saga.drop_system_versioning('dp_matrix.sv')$$, NULL, NULL);
SELECT dp_matrix.case_result('exact-family rebuild', $$SELECT sql_saga.rebuild_system_versioning_views('dp_matrix.sv')$$, NULL, NULL);
SELECT dp_matrix.case_result('history policy synchronization', $$SELECT sql_saga.sync_history_policies('dp_matrix.sv')$$, NULL, NULL);

SELECT set_config('sql_saga.managing_history_policies', 'on', true) IS NOT NULL AS guard_set \gset
SELECT dp_matrix.case_result('policy guard cannot drop unrelated history', $$DROP TABLE dp_matrix.sv_history CASCADE$$, 'P0001', 'cannot drop table "dp_matrix.sv_history" because it is used in SYSTEM VERSIONING for table "dp_matrix.sv"');
SELECT set_config('sql_saga.managing_history_policies', 'off', true) IS NOT NULL AS guard_reset \gset
SELECT set_config('sql_saga.rebuilding_system_versioning_views', 'dp_matrix.sv'::regclass::oid::text, true) IS NOT NULL AS guard_set \gset
SELECT dp_matrix.case_result('rebuild guard cannot drop unrelated view', $$DROP VIEW dp_matrix.view_t__current_valid CASCADE$$, 'P0001', 'cannot drop view "dp_matrix.view_t__current_valid", call "sql_saga.drop_current_view()" instead');
SELECT set_config('sql_saga.rebuilding_system_versioning_views', '', true) IS NOT NULL AS guard_reset \gset
SELECT set_config('sql_saga.clean_restore', 'on', true) IS NOT NULL AS guard_set \gset
SELECT dp_matrix.case_result('clean-restore marker alone cannot drop history', $$DROP TABLE dp_matrix.sv_history CASCADE$$, 'P0001', 'cannot drop table "dp_matrix.sv_history" because it is used in SYSTEM VERSIONING for table "dp_matrix.sv"');
SAVEPOINT clean_restore;
SET LOCAL check_function_bodies = off;
SET LOCAL row_security = off;
DROP TABLE dp_matrix.sv_history CASCADE;
SELECT 'clean-restore teardown recognition | ok';
ROLLBACK TO SAVEPOINT clean_restore;
SELECT set_config('sql_saga.clean_restore', 'off', true) IS NOT NULL AS guard_reset \gset

\echo === family, schema, owner, type, and function cascade matrix
SELECT dp_matrix.system_family('base_history');
SELECT dp_matrix.case_result('base plus history explicitly', $$DROP TABLE dp_matrix.base_history, dp_matrix.base_history_history CASCADE$$, NULL, NULL);
SELECT dp_matrix.system_family('complete_a');
SELECT dp_matrix.system_family('complete_b');
SELECT dp_matrix.case_result('multiple complete families', $$DROP TABLE dp_matrix.complete_a, dp_matrix.complete_b CASCADE$$, NULL, NULL);
SELECT dp_matrix.system_family('priority_a');
CREATE SCHEMA dp_priority_payload;
SET LOCAL session_replication_role = replica;
ALTER TABLE dp_matrix.priority_a_history SET SCHEMA dp_priority_payload;
SET LOCAL session_replication_role = origin;
SELECT dp_matrix.case_result('deterministic several-class priority', $$DROP SCHEMA dp_priority_payload CASCADE$$, 'P0001', 'cannot drop view "dp_matrix.priority_a_history_gated" because it is used in SYSTEM VERSIONING for table "dp_matrix.priority_a"');

CREATE SCHEMA dp_schema_mix;
SELECT dp_matrix.system_family('schema_complete', 'dp_schema_mix');
SELECT dp_matrix.system_family('schema_split');
SET LOCAL session_replication_role = replica;
ALTER TABLE dp_matrix.schema_split_history SET SCHEMA dp_schema_mix;
SET LOCAL session_replication_role = origin;
SELECT dp_matrix.case_result('schema complete A plus member-only B', $$DROP SCHEMA dp_schema_mix CASCADE$$, 'P0001', 'cannot drop view "dp_matrix.schema_split_history_gated" because it is used in SYSTEM VERSIONING for table "dp_matrix.schema_split"');
CREATE SCHEMA dp_schema_one;
CREATE SCHEMA dp_schema_two;
SELECT dp_matrix.system_family('schema_one_complete', 'dp_schema_one');
SELECT dp_matrix.system_family('schema_two_split');
SET LOCAL session_replication_role = replica;
ALTER TABLE dp_matrix.schema_two_split_history SET SCHEMA dp_schema_two;
SET LOCAL session_replication_role = origin;
SELECT dp_matrix.case_result('multiple schemas independent sanction', $$DROP SCHEMA dp_schema_one, dp_schema_two CASCADE$$, 'P0001', 'cannot drop view "dp_matrix.schema_two_split_history_gated" because it is used in SYSTEM VERSIONING for table "dp_matrix.schema_two_split"');

CREATE ROLE dp_matrix_owner;
CREATE SCHEMA dp_owned_mix AUTHORIZATION dp_matrix_owner;
SELECT dp_matrix.system_family('owner_complete', 'dp_owned_mix');
ALTER TABLE dp_owned_mix.owner_complete OWNER TO dp_matrix_owner;
SELECT dp_matrix.system_family('owner_split');
SET LOCAL session_replication_role = replica;
ALTER TABLE dp_matrix.owner_split_history OWNER TO dp_matrix_owner;
SET LOCAL session_replication_role = origin;
SELECT dp_matrix.case_result('owner complete A plus member-only B', $$DROP OWNED BY dp_matrix_owner CASCADE$$, 'P0001', 'cannot drop table "dp_matrix.owner_split_history" because it is used in SYSTEM VERSIONING for table "dp_matrix.owner_split"');
CREATE ROLE dp_matrix_new_owner;
SELECT dp_matrix.case_result('REASSIGN OWNED is non-drop', $$REASSIGN OWNED BY dp_matrix_owner TO dp_matrix_new_owner$$, NULL, NULL);

CREATE TYPE dp_matrix.native_range AS RANGE (SUBTYPE = integer);
CREATE TABLE dp_matrix.native_type_member (id int, valid_range dp_matrix.native_range NOT NULL);
SELECT sql_saga.add_era('dp_matrix.native_type_member', 'valid_range', 'valid');
SELECT dp_matrix.case_result('native DROP TYPE RESTRICT', $$DROP TYPE dp_matrix.native_range RESTRICT$$, '2BP01', 'cannot drop type native_range because other objects depend on it');

DELETE FROM sql_saga.system_time_era AS ste
WHERE (ste.table_schema, ste.table_name) <> ('dp_matrix', 'sv');

ALTER EXTENSION sql_saga DROP FUNCTION sql_saga.generated_always_as_row_start_end();
SELECT dp_matrix.case_result('generated-always shared function cascade', $$DROP FUNCTION sql_saga.generated_always_as_row_start_end() CASCADE$$, 'P0001', 'cannot drop trigger "sv_system_time_generated_always" on table "dp_matrix.sv" because it is used in SYSTEM_TIME period');
ALTER EXTENSION sql_saga ADD FUNCTION sql_saga.generated_always_as_row_start_end();
ALTER EXTENSION sql_saga DROP FUNCTION sql_saga.write_history();
SELECT dp_matrix.case_result('write-history shared function cascade', $$DROP FUNCTION sql_saga.write_history() CASCADE$$, 'P0001', 'cannot drop trigger "sv_system_time_write_history" on table "dp_matrix.sv" because it is used in SYSTEM_TIME period');
ALTER EXTENSION sql_saga ADD FUNCTION sql_saga.write_history();
ALTER EXTENSION sql_saga DROP FUNCTION sql_saga.truncate_system_versioning();
SELECT dp_matrix.case_result('truncate shared function cascade', $$DROP FUNCTION sql_saga.truncate_system_versioning() CASCADE$$, 'P0001', 'cannot drop trigger "sv_truncate" on table "dp_matrix.sv" because it is used in SYSTEM_TIME period');
ALTER EXTENSION sql_saga ADD FUNCTION sql_saga.truncate_system_versioning();
ALTER EXTENSION sql_saga DROP FUNCTION sql_saga.for_portion_of_trigger();
SELECT dp_matrix.case_result('FOR PORTION OF shared function cascade', $$DROP FUNCTION sql_saga.for_portion_of_trigger() CASCADE$$, 'P0001', 'cannot drop trigger "for_portion_of_valid" on view "dp_matrix.view_t__for_portion_of_valid" because it is part of an updatable view for era "valid" on table "dp_matrix.view_t"');
ALTER EXTENSION sql_saga ADD FUNCTION sql_saga.for_portion_of_trigger();
ALTER EXTENSION sql_saga DROP FUNCTION sql_saga.current_view_trigger();
SELECT dp_matrix.case_result('current-view shared function cascade', $$DROP FUNCTION sql_saga.current_view_trigger() CASCADE$$, 'P0001', 'cannot drop trigger "current_valid" on view "dp_matrix.view_t__current_valid" because it is part of an updatable view for era "valid" on table "dp_matrix.view_t"');
ALTER EXTENSION sql_saga ADD FUNCTION sql_saga.current_view_trigger();
ALTER EXTENSION sql_saga DROP FUNCTION sql_saga.generated_always_as_row_start_end();
ALTER EXTENSION sql_saga DROP FUNCTION sql_saga.write_history();
ALTER EXTENSION sql_saga DROP FUNCTION sql_saga.truncate_system_versioning();
SELECT dp_matrix.case_result('multi-class shared function priority', $$DROP FUNCTION sql_saga.generated_always_as_row_start_end(), sql_saga.write_history(), sql_saga.truncate_system_versioning() CASCADE$$, 'P0001', 'cannot drop trigger "sv_system_time_generated_always" on table "dp_matrix.sv" because it is used in SYSTEM_TIME period');
ALTER EXTENSION sql_saga ADD FUNCTION sql_saga.generated_always_as_row_start_end();
ALTER EXTENSION sql_saga ADD FUNCTION sql_saga.write_history();
ALTER EXTENSION sql_saga ADD FUNCTION sql_saga.truncate_system_versioning();

ROLLBACK;
RESET client_min_messages;
\pset tuples_only off
\pset format aligned
\i sql/include/test_teardown.sql
