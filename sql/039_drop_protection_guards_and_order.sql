\set ECHO none
\i sql/include/test_setup.sql
\set SHOW_CONTEXT never
\pset format unaligned
\pset tuples_only on
SET client_min_messages = warning;

BEGIN;
UPDATE sql_saga.installation SET token = 'guardtoken';
CREATE SCHEMA dp_guard_order;
SET search_path = dp_guard_order, public;

CREATE FUNCTION dp_guard_order.probe(command text, expected_sqlstate text, expected_message text)
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

CREATE FUNCTION dp_guard_order.case_result(case_name text, command text, expected_message text)
RETURNS text LANGUAGE sql AS $case_result$
    SELECT case_name || ' | ' || dp_guard_order.probe(command, 'P0001', expected_message)
$case_result$;

CREATE FUNCTION dp_guard_order.system_family(family_name text)
RETURNS void LANGUAGE plpgsql AS $family$
BEGIN
    EXECUTE format('CREATE TABLE dp_guard_order.%I (id int PRIMARY KEY, tenant text, payload text)', family_name);
    PERFORM sql_saga.add_system_versioning(format('dp_guard_order.%I', family_name)::regclass);
END;
$family$;

\echo === policy guard narrowness
SELECT dp_guard_order.system_family('guard_a');
SELECT dp_guard_order.system_family('guard_b');
SELECT set_config('sql_saga.managing_history_policies', 'on', true) IS NOT NULL AS guard_set \gset
SELECT dp_guard_order.case_result(
    'manual on cannot drop family B policy',
    $$DROP POLICY sql_saga_history_reader_guardtoken ON dp_guard_order.guard_b_history$$,
    'cannot drop or alter policy "sql_saga_history_reader_guardtoken" on "dp_guard_order.guard_b_history" because it is managed by sql_saga for table "dp_guard_order.guard_b"');
SELECT dp_guard_order.case_result(
    'manual on cannot drop same-family A policy',
    $$DROP POLICY sql_saga_history_reader_guardtoken ON dp_guard_order.guard_a_history$$,
    'cannot drop or alter policy "sql_saga_history_reader_guardtoken" on "dp_guard_order.guard_a_history" because it is managed by sql_saga for table "dp_guard_order.guard_a"');
SELECT set_config('sql_saga.managing_history_policies', 'dp_guard_order.guard_a', true) IS NOT NULL AS guard_set \gset
SELECT dp_guard_order.case_result(
    'manual family identity cannot drop family B policy',
    $$DROP POLICY sql_saga_history_reader_guardtoken ON dp_guard_order.guard_b_history$$,
    'cannot drop or alter policy "sql_saga_history_reader_guardtoken" on "dp_guard_order.guard_b_history" because it is managed by sql_saga for table "dp_guard_order.guard_b"');
SELECT set_config('sql_saga.managing_history_policies', 'off', true) IS NOT NULL AS guard_reset \gset
SELECT 'real policy-sync API remains sanctioned | ' || dp_guard_order.probe(
    $$SELECT sql_saga.sync_history_policies('dp_guard_order.guard_a')$$, NULL, NULL);

\echo === deterministic same-class dropped ordinality
-- Determinism here means: the same statement against the same freshly-created database state
-- always reports the same error. PostgreSQL follows dependency/drop order, which dump/restore may
-- recreate differently. Every case below makes sql_saga catalog order A then B, but payload order B
-- then A, as verified independently by tmp/pair_dp1/order_probe_039.sql in three fresh databases.

CREATE TABLE dp_guard_order.period_order (
    id int,
    valid_range daterange NOT NULL,
    valid_from date,
    valid_until date);
SELECT sql_saga.add_era(
    'dp_guard_order.period_order', 'valid_range', 'valid',
    valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT dp_guard_order.case_result(
    'two period columns choose statement-first valid_until',
    $$ALTER TABLE dp_guard_order.period_order DROP COLUMN valid_from CASCADE, DROP COLUMN valid_until CASCADE$$,
    'cannot drop column "dp_guard_order.period_order.valid_until" because it is part of the period "valid"');

SELECT sql_saga.set_system_time_era_excluded_columns(
    'dp_guard_order.guard_a', ARRAY['tenant', 'payload']::name[]);
SELECT dp_guard_order.case_result(
    'two excluded columns choose statement-first payload',
    $$ALTER TABLE dp_guard_order.guard_a DROP COLUMN tenant CASCADE, DROP COLUMN payload CASCADE$$,
    'cannot drop or rename column "payload" on table "dp_guard_order.guard_a" because it is excluded from system_time era');

CREATE TABLE dp_guard_order.uk_order (
    id_a int NOT NULL,
    id_b int NOT NULL,
    valid_range daterange NOT NULL,
    CONSTRAINT uk_order_a UNIQUE (id_a, valid_range WITHOUT OVERLAPS),
    CONSTRAINT uk_order_b UNIQUE (id_b, valid_range WITHOUT OVERLAPS));
SELECT sql_saga.add_era('dp_guard_order.uk_order', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key(
    'dp_guard_order.uk_order', ARRAY['id_a'], 'valid',
    unique_key_name => 'uk_a', unique_constraint => 'uk_order_a');
SELECT sql_saga.add_unique_key(
    'dp_guard_order.uk_order', ARRAY['id_b'], 'valid',
    unique_key_name => 'uk_b', unique_constraint => 'uk_order_b');
SELECT dp_guard_order.case_result(
    'two unique constraints choose statement-first B',
    $$ALTER TABLE dp_guard_order.uk_order DROP CONSTRAINT uk_order_a, DROP CONSTRAINT uk_order_b$$,
    'cannot drop constraint "uk_order_b" on table "dp_guard_order.uk_order" because it is used in era unique key "uk_b"');

-- Catalog order is guard_a then guard_b. Recreate the shared-function dependencies in B then A order.
DO $reverse_trigger_dependencies$
DECLARE
    a_def text;
    b_def text;
BEGIN
    SELECT pg_catalog.pg_get_triggerdef(t.oid) INTO a_def
    FROM pg_catalog.pg_trigger AS t
    WHERE (t.tgrelid, t.tgname) = ('dp_guard_order.guard_a'::regclass, 'guard_a_system_time_write_history');
    SELECT pg_catalog.pg_get_triggerdef(t.oid) INTO b_def
    FROM pg_catalog.pg_trigger AS t
    WHERE (t.tgrelid, t.tgname) = ('dp_guard_order.guard_b'::regclass, 'guard_b_system_time_write_history');
    PERFORM set_config('session_replication_role', 'replica', true);
    DROP TRIGGER guard_a_system_time_write_history ON dp_guard_order.guard_a;
    DROP TRIGGER guard_b_system_time_write_history ON dp_guard_order.guard_b;
    EXECUTE b_def;
    EXECUTE a_def;
    PERFORM set_config('session_replication_role', 'origin', true);
END;
$reverse_trigger_dependencies$;
ALTER EXTENSION sql_saga DROP FUNCTION sql_saga.write_history();
SELECT dp_guard_order.case_result(
    'two write-history triggers choose payload-first B',
    $$DROP FUNCTION sql_saga.write_history() CASCADE$$,
    'cannot drop trigger "guard_b_system_time_write_history" on table "dp_guard_order.guard_b" because it is used in SYSTEM_TIME period');
ALTER EXTENSION sql_saga ADD FUNCTION sql_saga.write_history();

CREATE TABLE dp_guard_order.fk_parent (id int, valid_range daterange NOT NULL);
CREATE TABLE dp_guard_order.fk_child_a (parent_id int);
CREATE TABLE dp_guard_order.fk_child_b (parent_id int);
SELECT sql_saga.add_era('dp_guard_order.fk_parent', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key(
    'dp_guard_order.fk_parent', ARRAY['id'], 'valid',
    key_type => 'primary', unique_key_name => 'fk_parent_key');
SELECT sql_saga.add_regular_foreign_key(
    'dp_guard_order.fk_child_a', ARRAY['parent_id'], 'fk_parent_key', foreign_key_name => 'fk_a');
SELECT sql_saga.add_regular_foreign_key(
    'dp_guard_order.fk_child_b', ARRAY['parent_id'], 'fk_parent_key', foreign_key_name => 'fk_b');
DO $reverse_fk_trigger_dependencies$
DECLARE
    a_def text;
    b_def text;
BEGIN
    SELECT pg_catalog.pg_get_triggerdef(t.oid) INTO a_def
    FROM pg_catalog.pg_trigger AS t
    WHERE (t.tgrelid, t.tgname) = ('dp_guard_order.fk_parent'::regclass, 'fk_a_uk_update');
    SELECT pg_catalog.pg_get_triggerdef(t.oid) INTO b_def
    FROM pg_catalog.pg_trigger AS t
    WHERE (t.tgrelid, t.tgname) = ('dp_guard_order.fk_parent'::regclass, 'fk_b_uk_update');
    PERFORM set_config('session_replication_role', 'replica', true);
    DROP TRIGGER fk_a_uk_update ON dp_guard_order.fk_parent;
    DROP TRIGGER fk_b_uk_update ON dp_guard_order.fk_parent;
    EXECUTE b_def;
    EXECUTE a_def;
    PERFORM set_config('session_replication_role', 'origin', true);
END;
$reverse_fk_trigger_dependencies$;
ALTER EXTENSION sql_saga DROP FUNCTION sql_saga.uk_update_check_c();
SELECT dp_guard_order.case_result(
    'two FK update triggers choose payload-first B',
    $$DROP FUNCTION sql_saga.uk_update_check_c() CASCADE$$,
    'cannot drop trigger "fk_b_uk_update" on table "dp_guard_order.fk_parent" because it is used in era foreign key "fk_b"');
ALTER EXTENSION sql_saga ADD FUNCTION sql_saga.uk_update_check_c();

CREATE TABLE dp_guard_order.view_a (id int, valid_range daterange NOT NULL, PRIMARY KEY (id, valid_range WITHOUT OVERLAPS));
CREATE TABLE dp_guard_order.view_b (id int, valid_range daterange NOT NULL, PRIMARY KEY (id, valid_range WITHOUT OVERLAPS));
SELECT sql_saga.add_era('dp_guard_order.view_a', 'valid_range', 'valid');
SELECT sql_saga.add_era('dp_guard_order.view_b', 'valid_range', 'valid');
SELECT sql_saga.add_current_view('dp_guard_order.view_a', 'valid');
SELECT sql_saga.add_current_view('dp_guard_order.view_b', 'valid');
SELECT dp_guard_order.case_result(
    'two updatable views choose payload-first B',
    $$DROP VIEW dp_guard_order.view_a__current_valid, dp_guard_order.view_b__current_valid CASCADE$$,
    'cannot drop view "dp_guard_order.view_b__current_valid", call "sql_saga.drop_current_view()" instead');

DO $reverse_view_trigger_dependencies$
DECLARE
    a_def text;
    b_def text;
BEGIN
    SELECT pg_catalog.pg_get_triggerdef(t.oid) INTO a_def
    FROM pg_catalog.pg_trigger AS t
    WHERE (t.tgrelid, t.tgname) = ('dp_guard_order.view_a__current_valid'::regclass, 'current_valid');
    SELECT pg_catalog.pg_get_triggerdef(t.oid) INTO b_def
    FROM pg_catalog.pg_trigger AS t
    WHERE (t.tgrelid, t.tgname) = ('dp_guard_order.view_b__current_valid'::regclass, 'current_valid');
    PERFORM set_config('session_replication_role', 'replica', true);
    DROP TRIGGER current_valid ON dp_guard_order.view_a__current_valid;
    DROP TRIGGER current_valid ON dp_guard_order.view_b__current_valid;
    EXECUTE b_def;
    EXECUTE a_def;
    PERFORM set_config('session_replication_role', 'origin', true);
END;
$reverse_view_trigger_dependencies$;
ALTER EXTENSION sql_saga DROP FUNCTION sql_saga.current_view_trigger();
SELECT dp_guard_order.case_result(
    'two updatable-view triggers choose payload-first B',
    $$DROP FUNCTION sql_saga.current_view_trigger() CASCADE$$,
    'cannot drop trigger "current_valid" on view "dp_guard_order.view_b__current_valid" because it is part of an updatable view for era "valid" on table "dp_guard_order.view_b"');
ALTER EXTENSION sql_saga ADD FUNCTION sql_saga.current_view_trigger();

SELECT dp_guard_order.case_result(
    'two query functions choose statement-first B',
    $$DROP FUNCTION dp_guard_order.guard_a__as_of(timestamptz), dp_guard_order.guard_b__as_of(timestamptz) CASCADE$$,
    'cannot drop function "dp_guard_order.guard_b__as_of(timestamp with time zone)" because it is used in SYSTEM VERSIONING for table "dp_guard_order.guard_b"');
SELECT dp_guard_order.case_result(
    'two history tables choose statement-first B',
    $$DROP TABLE dp_guard_order.guard_a_history, dp_guard_order.guard_b_history CASCADE$$,
    'cannot drop table "dp_guard_order.guard_b_history" because it is used in SYSTEM VERSIONING for table "dp_guard_order.guard_b"');

CREATE ROLE dp_guard_policy_role;
SELECT set_config('sql_saga.managing_history_policies', 'on', true) IS NOT NULL AS guard_set \gset
DROP POLICY sql_saga_history_reader_guardtoken ON dp_guard_order.guard_a_history;
DROP POLICY sql_saga_history_reader_guardtoken ON dp_guard_order.guard_b_history;
CREATE POLICY sql_saga_history_reader_guardtoken ON dp_guard_order.guard_b_history
    FOR SELECT TO dp_guard_policy_role USING (true);
CREATE POLICY sql_saga_history_reader_guardtoken ON dp_guard_order.guard_a_history
    FOR SELECT TO dp_guard_policy_role USING (true);
SELECT set_config('sql_saga.managing_history_policies', 'off', true) IS NOT NULL AS guard_reset \gset
SELECT dp_guard_order.case_result(
    'two managed policies choose payload-first B',
    $$DROP OWNED BY dp_guard_policy_role CASCADE$$,
    'cannot drop or alter policy "sql_saga_history_reader_guardtoken" on "dp_guard_order.guard_b_history" because it is managed by sql_saga for table "dp_guard_order.guard_b"');

ROLLBACK;
\i sql/include/test_teardown.sql
