\i sql/include/test_setup.sql

BEGIN;
\set ON_ERROR_STOP on
CREATE EXTENSION IF NOT EXISTS sql_saga;

CREATE SCHEMA mtt;

-- Setup: Parent and child tables with a temporal foreign key.
-- The child also has a synchronized column to ensure its trigger is NOT affected.
CREATE TABLE mtt.parent (id int, name text, valid_from date, valid_until date, valid_to date);
SELECT sql_saga.add_era('mtt.parent', synchronize_valid_to_column := 'valid_to');
SELECT sql_saga.add_unique_key(
    'mtt.parent',
    '{id}',
    unique_key_name => 'parent_id_valid',
    unique_constraint => 'parent_id_valid_uniq',
    exclude_constraint => 'parent_id_valid_excl'
);

CREATE TABLE mtt.child (id int, parent_id int, name text, valid_from date, valid_until date, valid_to date);
SELECT sql_saga.add_era('mtt.child', synchronize_valid_to_column := 'valid_to');
SELECT sql_saga.add_unique_key(
    'mtt.child',
    '{id}',
    unique_key_name => 'child_id_valid',
    unique_constraint => 'child_id_valid_uniq',
    exclude_constraint => 'child_id_valid_excl'
);
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'mtt.child'::regclass,
    fk_column_names => ARRAY['parent_id'],
    fk_era_name => 'valid',
    unique_key_name => 'parent_id_valid'
);

-- Helper function to check trigger status
CREATE OR REPLACE FUNCTION mtt.get_trigger_status(p_table_oid regclass)
RETURNS TABLE(trigger_name name, is_enabled text) AS $$
    SELECT tgname,
           CASE tgenabled
               WHEN 'O' THEN 'enabled' -- Origin and local
               WHEN 'R' THEN 'enabled' -- Replica
               WHEN 'A' THEN 'enabled' -- Always
               WHEN 'D' THEN 'disabled'
           END
    FROM pg_trigger
    WHERE tgrelid = p_table_oid
      AND NOT tgisinternal
    ORDER BY tgname;
$$ LANGUAGE sql;


\echo '--- Trigger status BEFORE disabling ---'
\echo '-- Triggers on child table --'
SELECT * FROM mtt.get_trigger_status('mtt.child');
\echo '-- Triggers on parent table --'
SELECT * FROM mtt.get_trigger_status('mtt.parent');

\echo '--- Disabling triggers for both tables ---'
CALL sql_saga.disable_temporal_triggers('mtt.parent', 'mtt.child');

\echo '--- Trigger status AFTER disabling (FK triggers disabled, sync triggers enabled) ---'
\echo '-- Triggers on child table --'
SELECT * FROM mtt.get_trigger_status('mtt.child');
\echo '-- Triggers on parent table --'
SELECT * FROM mtt.get_trigger_status('mtt.parent');

\echo '--- Enabling triggers for both tables ---'
CALL sql_saga.enable_temporal_triggers('mtt.parent', 'mtt.child');

\echo '--- Trigger status AFTER enabling (all triggers enabled) ---'
\echo '-- Triggers on child table --'
SELECT * FROM mtt.get_trigger_status('mtt.child');
\echo '-- Triggers on parent table --'
SELECT * FROM mtt.get_trigger_status('mtt.parent');

ROLLBACK;
\i sql/include/test_teardown.sql
