\i sql/include/test_setup.sql
\set VERBOSITY terse
SET client_min_messages = warning;
CREATE SCHEMA defer_test;
GRANT USAGE, CREATE ON SCHEMA defer_test TO sql_saga_unprivileged_user;
SET ROLE sql_saga_unprivileged_user;
CREATE TABLE defer_test.parent (id integer, valid_range daterange NOT NULL, payload text, valid_from date, valid_until date);
SELECT sql_saga.add_era(table_oid => 'defer_test.parent', range_column_name => 'valid_range', era_name => 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'defer_test.parent', column_names => ARRAY['id'], era_name => 'valid');
CREATE TABLE defer_test.child (id integer PRIMARY KEY, parent_id integer, payload text);
SELECT sql_saga.add_foreign_key(fk_table_oid => 'defer_test.child', fk_column_names => ARRAY['parent_id'], pk_table_oid => 'defer_test.parent', pk_column_names => ARRAY['id']);
CREATE TABLE defer_test.cascade_child (id integer PRIMARY KEY, parent_id integer);
SELECT sql_saga.add_foreign_key(fk_table_oid => 'defer_test.cascade_child', fk_column_names => ARRAY['parent_id'], pk_table_oid => 'defer_test.parent', pk_column_names => ARRAY['id'], delete_action => 'CASCADE');
CREATE TABLE defer_test.temporal_child (id integer, parent_id integer, valid_range daterange NOT NULL, payload text);
SELECT sql_saga.add_era('defer_test.temporal_child', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key(table_oid => 'defer_test.temporal_child', column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.add_foreign_key(fk_table_oid => 'defer_test.temporal_child', fk_column_names => ARRAY['parent_id'], pk_table_oid => 'defer_test.parent', pk_column_names => ARRAY['id'], foreign_key_name => 'defer_native_fk');
INSERT INTO defer_test.parent (id, valid_range, payload) VALUES (1, '[2020-01-01,2030-01-01)', 'before');
INSERT INTO defer_test.child VALUES (1, 1, 'before');
INSERT INTO defer_test.cascade_child VALUES (1, 1);
INSERT INTO defer_test.temporal_child VALUES (1, 1, '[2020-01-01,2030-01-01)', 'before');

\echo 'A gap spanning merge calls is accepted only after it is repaired'
BEGIN;
CALL sql_saga.defer_temporal_checks('defer_test.parent');
CALL sql_saga.defer_temporal_checks('defer_test.child', 'defer_test.temporal_child');
CREATE TEMP TABLE defer_source (row_id integer, id integer, valid_range daterange, payload text) ON COMMIT DROP;
INSERT INTO defer_source VALUES (1, 1, '[2020-01-01,2030-01-01)', 'after');
CALL sql_saga.temporal_merge(target_table => 'defer_test.parent', source_table => 'defer_source', primary_identity_columns => ARRAY['id'], mode => 'DELETE_FOR_PORTION_OF');
SELECT (SELECT count(*) FROM defer_test.parent) AS parent_gap,
       (SELECT count(*) FROM defer_test.child) AS child_retained,
       (SELECT count(*) FROM defer_test.cascade_child) AS cascade_retained;
CALL sql_saga.temporal_merge(target_table => 'defer_test.parent', source_table => 'defer_source', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_REPLACE');
SELECT payload, valid_from, valid_until FROM defer_test.parent;
COMMIT;
SELECT COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.commit_deferred_constraints', true), ''), '{}') = '{}' AS registry_cleared;

\echo 'Real commit rejects a final regular FK violation with 23503 and rolls back'
BEGIN;
CALL sql_saga.defer_temporal_checks('defer_test.child');
INSERT INTO defer_test.child VALUES (2, 999, 'invalid');
COMMIT;
\echo commit_sqlstate :SQLSTATE
SELECT count(*) AS invalid_children FROM defer_test.child WHERE id = 2;

\echo 'Native temporal FK is deferred in the child-to-parent direction too'
BEGIN;
CALL sql_saga.defer_temporal_checks('defer_test.temporal_child');
INSERT INTO defer_test.temporal_child VALUES (2, 2, '[2020-01-01,2030-01-01)', 'new');
INSERT INTO defer_test.parent (id, valid_range, payload) VALUES (2, '[2020-01-01,2030-01-01)', 'new');
COMMIT;
BEGIN;
CALL sql_saga.defer_temporal_checks('defer_test.temporal_child');
INSERT INTO defer_test.temporal_child VALUES (3, 999, '[2020-01-01,2030-01-01)', 'invalid');
COMMIT;
\echo native_commit_sqlstate :SQLSTATE
SELECT count(*) AS invalid_temporal_children FROM defer_test.temporal_child WHERE id = 3;

\echo 'Obsolete child tuple versions are not checked'
BEGIN;
CALL sql_saga.defer_temporal_checks('defer_test.child');
INSERT INTO defer_test.child VALUES (3, 999, 'invalid first version');
UPDATE defer_test.child SET parent_id = 1 WHERE id = 3;
INSERT INTO defer_test.child VALUES (4, 999, 'deleted first version');
DELETE FROM defer_test.child WHERE id = 4;
COMMIT;
SELECT id, parent_id FROM defer_test.child ORDER BY id;

\echo 'Parent key updates may be repaired in the same transaction'
BEGIN;
CALL sql_saga.defer_temporal_checks('defer_test.parent');
UPDATE defer_test.parent SET id = 10 WHERE id = 1;
UPDATE defer_test.child SET parent_id = 10 WHERE parent_id = 1;
UPDATE defer_test.cascade_child SET parent_id = 10 WHERE parent_id = 1;
UPDATE defer_test.temporal_child SET parent_id = 10 WHERE parent_id = 1;
COMMIT;

\echo 'Savepoint rollback undoes both constraint mode and executor registry'
BEGIN;
SAVEPOINT opt_in;
CALL sql_saga.defer_temporal_checks('defer_test.child');
ROLLBACK TO SAVEPOINT opt_in;
INSERT INTO defer_test.child VALUES (5, 999, 'must fail immediately');
\echo immediate_after_rollback_sqlstate :SQLSTATE
ROLLBACK;

\echo 'Immediate and deferred trigger diagnostics are identical'
DO $diagnostics$
DECLARE
    immediate_message text;
    deferred_message text;
    immediate_state text;
    deferred_state text;
BEGIN
    BEGIN
        INSERT INTO defer_test.child VALUES (6, 999, 'invalid');
    EXCEPTION WHEN foreign_key_violation THEN
        GET STACKED DIAGNOSTICS immediate_message = MESSAGE_TEXT, immediate_state = RETURNED_SQLSTATE;
    END;
    BEGIN
        CALL sql_saga.defer_temporal_checks('defer_test.child');
        INSERT INTO defer_test.child VALUES (6, 999, 'invalid');
        SET CONSTRAINTS ALL IMMEDIATE;
    EXCEPTION WHEN foreign_key_violation THEN
        GET STACKED DIAGNOSTICS deferred_message = MESSAGE_TEXT, deferred_state = RETURNED_SQLSTATE;
    END;
    IF immediate_state IS DISTINCT FROM '23503' OR immediate_state IS DISTINCT FROM deferred_state
       OR immediate_message IS DISTINCT FROM deferred_message THEN
        RAISE EXCEPTION 'diagnostics differ: % / % / % / %', immediate_state, deferred_state, immediate_message, deferred_message;
    END IF;
END;
$diagnostics$;

\echo 'Unrelated constraints stay immediate and colliding names fail closed'
CREATE TABLE defer_test.unrelated (id integer PRIMARY KEY);
CREATE TABLE defer_test.unrelated_child (parent_id integer REFERENCES defer_test.unrelated DEFERRABLE);
BEGIN;
CALL sql_saga.defer_temporal_checks('defer_test.parent');
INSERT INTO defer_test.unrelated_child VALUES (999);
\echo unrelated_sqlstate :SQLSTATE
ROLLBACK;
ALTER TABLE defer_test.unrelated_child ADD CONSTRAINT defer_native_fk FOREIGN KEY (parent_id) REFERENCES defer_test.unrelated DEFERRABLE;
BEGIN;
CALL sql_saga.defer_temporal_checks('defer_test.parent');
\echo collision_sqlstate :SQLSTATE
ROLLBACK;
ALTER TABLE defer_test.unrelated_child DROP CONSTRAINT defer_native_fk;

\echo 'TRUNCATE stays protected immediately'
BEGIN;
CALL sql_saga.defer_temporal_checks('defer_test.child');
TRUNCATE defer_test.parent;
\echo truncate_sqlstate :SQLSTATE
ROLLBACK;
BEGIN;
CALL sql_saga.defer_temporal_checks('defer_test.parent');
TRUNCATE defer_test.parent, defer_test.temporal_child;
\echo regular_truncate_sqlstate :SQLSTATE
ROLLBACK;

\echo 'CASCADE runs only for keys absent in the final state'
INSERT INTO defer_test.parent (id, valid_range, payload) VALUES (20, '[2020-01-01,2030-01-01)', 'cascade');
INSERT INTO defer_test.cascade_child VALUES (20, 20);
BEGIN;
CALL sql_saga.defer_temporal_checks('defer_test.cascade_child');
DELETE FROM defer_test.parent WHERE id = 20;
SELECT count(*) AS cascade_child_before_commit FROM defer_test.cascade_child WHERE id = 20;
COMMIT;
SELECT count(*) AS cascade_child_after_commit FROM defer_test.cascade_child WHERE id = 20;

\echo 'NULL input is rejected, empty input is harmless, disabled checks are rejected'
CALL sql_saga.defer_temporal_checks(VARIADIC ARRAY[]::regclass[]);
CALL sql_saga.defer_temporal_checks(VARIADIC NULL::regclass[]);
\echo null_sqlstate :SQLSTATE
CALL sql_saga.defer_temporal_checks(VARIADIC ARRAY[NULL]::regclass[]);
\echo null_element_sqlstate :SQLSTATE
BEGIN;
CALL sql_saga.disable_temporal_triggers('defer_test.child');
CALL sql_saga.defer_temporal_checks('defer_test.child');
\echo disabled_sqlstate :SQLSTATE
ROLLBACK;

\echo 'Deferral grants no DML rights'
RESET ROLE;
REVOKE ALL ON defer_test.parent, defer_test.child FROM sql_saga_unprivileged_user;
-- The fixture owner retains rights, so use a distinct no-login role for the negative probe.
CREATE ROLE saga_defer_reader;
GRANT USAGE ON SCHEMA sql_saga, defer_test TO saga_defer_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA sql_saga TO saga_defer_reader;
SET ROLE saga_defer_reader;
BEGIN;
CALL sql_saga.defer_temporal_checks('defer_test.child');
DELETE FROM defer_test.child;
\echo no_dml_rights_sqlstate :SQLSTATE
ROLLBACK;
RESET ROLE;
GRANT INSERT ON defer_test.child TO saga_defer_reader;
ALTER TABLE defer_test.child ENABLE ROW LEVEL SECURITY;
SET ROLE saga_defer_reader;
BEGIN;
CALL sql_saga.defer_temporal_checks('defer_test.child');
INSERT INTO defer_test.child VALUES (50, 10, 'RLS must reject');
\echo rls_sqlstate :SQLSTATE
ROLLBACK;
RESET ROLE;
DROP OWNED BY saga_defer_reader;
DROP ROLE saga_defer_reader;

\echo 'Repeated managed names are allowed only when every matching check was explicitly selected'
CREATE TABLE defer_test.parent_a (id integer, valid_range daterange NOT NULL, payload text);
CREATE TABLE defer_test.parent_b (id integer, valid_range daterange NOT NULL, payload text);
SELECT sql_saga.add_era('defer_test.parent_a', 'valid_range', 'valid');
SELECT sql_saga.add_era('defer_test.parent_b', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key(table_oid => 'defer_test.parent_a', column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.add_unique_key(table_oid => 'defer_test.parent_b', column_names => ARRAY['id'], era_name => 'valid');
CREATE TABLE defer_test.child_a (parent_id integer);
CREATE TABLE defer_test.child_b (parent_id integer);
SELECT sql_saga.add_regular_foreign_key(fk_table_oid => 'defer_test.child_a', fk_column_names => ARRAY['parent_id'], unique_key_name => 'parent_a_id_valid', foreign_key_name => 'defer_a_fk', uk_update_trigger => 'defer_shared_update');
SELECT sql_saga.add_regular_foreign_key(fk_table_oid => 'defer_test.child_b', fk_column_names => ARRAY['parent_id'], unique_key_name => 'parent_b_id_valid', foreign_key_name => 'defer_b_fk', uk_update_trigger => 'defer_shared_update');
INSERT INTO defer_test.parent_a VALUES (1, '[2020-01-01,2030-01-01)', 'old');
INSERT INTO defer_test.parent_b VALUES (1, '[2020-01-01,2030-01-01)', 'old');
INSERT INTO defer_test.child_a VALUES (1);
INSERT INTO defer_test.child_b VALUES (1);
BEGIN;
CALL sql_saga.defer_temporal_checks('defer_test.parent_a');
\echo unselected_shared_name_sqlstate :SQLSTATE
ROLLBACK;
BEGIN;
CALL sql_saga.defer_temporal_checks('defer_test.parent_a', 'defer_test.parent_b');
CREATE TEMP TABLE shared_source (row_id integer, id integer, valid_range daterange, payload text) ON COMMIT DROP;
INSERT INTO shared_source VALUES (1, 1, '[2020-01-01,2030-01-01)', 'new');
CALL sql_saga.temporal_merge(target_table => 'defer_test.parent_a', source_table => 'shared_source', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_REPLACE');
CALL sql_saga.temporal_merge(target_table => 'defer_test.parent_b', source_table => 'shared_source', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_REPLACE');
COMMIT;
SELECT payload FROM defer_test.parent_a UNION ALL SELECT payload FROM defer_test.parent_b;
SELECT sql_saga.check_integrity();
DROP SCHEMA defer_test CASCADE;
RESET client_min_messages;
\set VERBOSITY default
\i sql/include/test_teardown.sql
