\i sql/include/test_setup.sql

/*
 * Renaming a referencing column of a managed foreign key (ALTER TABLE child RENAME COLUMN).
 *
 * PostgreSQL renames neither constraints nor triggers when a column is renamed, and sql_saga follows
 * a rename by bringing its catalog in line with what PostgreSQL did (as for unique keys, whose
 * unique_key_name and constraint names stay unchanged after a column rename). So after the rename:
 *   - foreign_key_name, uk_update_trigger, uk_delete_trigger, fk_check_constraint and fk_index_name keep
 *     the names of the real objects, which themselves keep their names;
 *   - column_names and the snapshot follow the new column name;
 *   - for a regular FK the referenced-side helper triggers keep enforcing on UPDATE/DELETE of the
 *     referenced table against the renamed column (on 1.1 and 1.2 HEAD their arguments still name the
 *     old column, so any DELETE on the referenced table fails with "column fk.par_id does not exist");
 *   - for a temporal FK, the native PERIOD constraint keeps its name (it follows the column by attnum)
 *     and uk_update_trigger / uk_delete_trigger stay NULL;
 *   - drop_foreign_key by the new column list drops the real objects;
 *   - check_integrity() passes.
 */
\set SHOW_CONTEXT never
\set VERBOSITY default
SET client_min_messages = warning;

CREATE SCHEMA fkr;
SET search_path = fkr, public;

CREATE TABLE fkr.par (id int, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('fkr.par', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('fkr.par', ARRAY['id']::name[], 'valid', key_type => 'natural');
INSERT INTO fkr.par VALUES (1, '[2020-01-01,2030-01-01)'), (2, '[2020-01-01,2030-01-01)'), (3, '[2020-01-01,2030-01-01)');

CREATE TABLE fkr.rchild (id int PRIMARY KEY, par_id int);
SELECT sql_saga.add_regular_foreign_key('fkr.rchild', ARRAY['par_id']::name[], 'par_id_valid');

CREATE TABLE fkr.tchild (id int, par_id int, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('fkr.tchild', 'valid_range', 'valid');
SELECT sql_saga.add_temporal_foreign_key('fkr.tchild', ARRAY['par_id']::name[], 'valid', 'par_id_valid');

INSERT INTO fkr.rchild VALUES (1, 1);
INSERT INTO fkr.tchild VALUES (1, 2, '[2021-01-01,2022-01-01)');

CREATE VIEW fkr.fk_state AS
SELECT fk.foreign_key_name, fk.type, fk.column_names, fk.fk_table_columns_snapshot,
       fk.fk_check_constraint, fk.fk_index_name, fk.uk_update_trigger, fk.uk_delete_trigger
FROM sql_saga.foreign_keys AS fk
WHERE fk.table_schema = 'fkr'
ORDER BY fk.foreign_key_name;

-- Real referenced-side triggers, with the foreign key name they carry as first argument.
CREATE VIEW fkr.par_triggers AS
SELECT t.tgname, t.tgfoid::regprocedure AS function,
       (string_to_array(encode(t.tgargs, 'escape'), '\000'))[1] AS arg_foreign_key_name
FROM pg_catalog.pg_trigger AS t
WHERE t.tgrelid = 'fkr.par'::regclass AND NOT t.tgisinternal
ORDER BY t.tgname;

CREATE VIEW fkr.child_constraints AS
SELECT c.conrelid::regclass AS table_name, c.conname, c.contype, pg_catalog.pg_get_constraintdef(c.oid) AS def
FROM pg_catalog.pg_constraint AS c
WHERE c.conrelid IN ('fkr.rchild'::regclass, 'fkr.tchild'::regclass) AND c.contype IN ('c', 'f')
ORDER BY 1, 2;

-- Referencing-side constraint trigger of the regular FK (sql_saga 1.3 shape: no CHECK, no helper).
-- Its definition shows the watched column and the column list argument, both of which must follow a rename.
CREATE VIEW fkr.child_triggers AS
SELECT t.tgrelid::regclass AS table_name, t.tgname, t.tgfoid::regprocedure AS function, pg_catalog.pg_get_triggerdef(t.oid) AS def
FROM pg_catalog.pg_trigger AS t
WHERE t.tgrelid IN ('fkr.rchild'::regclass, 'fkr.tchild'::regclass) AND NOT t.tgisinternal
ORDER BY 1, 2;

-- Catalog names with no real object behind them. Must be empty.
CREATE VIEW fkr.drift AS
SELECT fk.foreign_key_name, 'uk_update_trigger' AS what, fk.uk_update_trigger AS catalog_name
FROM sql_saga.foreign_keys AS fk
WHERE fk.table_schema = 'fkr' AND fk.uk_update_trigger IS NOT NULL
  AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = 'fkr.par'::regclass AND t.tgname = fk.uk_update_trigger)
UNION ALL
SELECT fk.foreign_key_name, 'uk_delete_trigger', fk.uk_delete_trigger
FROM sql_saga.foreign_keys AS fk
WHERE fk.table_schema = 'fkr' AND fk.uk_delete_trigger IS NOT NULL
  AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = 'fkr.par'::regclass AND t.tgname = fk.uk_delete_trigger)
UNION ALL
SELECT fk.foreign_key_name, 'native foreign key constraint', fk.foreign_key_name
FROM sql_saga.foreign_keys AS fk
WHERE fk.table_schema = 'fkr' AND fk.type = 'temporal_to_temporal'
  AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS c
                  WHERE c.conrelid = format('%I.%I', fk.table_schema, fk.table_name)::regclass
                    AND c.conname = fk.foreign_key_name AND c.contype = 'f')
UNION ALL
SELECT fk.foreign_key_name, 'fk_check_constraint', fk.fk_check_constraint
FROM sql_saga.foreign_keys AS fk
WHERE fk.table_schema = 'fkr' AND fk.fk_check_constraint IS NOT NULL
  -- One object: the constraint trigger named fk_check_constraint, with its own contype 't' constraint.
  AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t
                  JOIN pg_catalog.pg_constraint AS c ON c.oid = t.tgconstraint
                  WHERE t.tgrelid = format('%I.%I', fk.table_schema, fk.table_name)::regclass
                    AND t.tgname = fk.fk_check_constraint
                    AND t.tgfoid = 'sql_saga.regular_fk_check_c()'::regprocedure
                    AND c.conrelid = t.tgrelid AND c.conname = t.tgname AND c.contype = 't')
UNION ALL
SELECT fk.foreign_key_name, 'fk_index_name', fk.fk_index_name
FROM sql_saga.foreign_keys AS fk
WHERE fk.table_schema = 'fkr' AND fk.fk_index_name IS NOT NULL
  AND to_regclass(format('%I.%I', fk.table_schema, fk.fk_index_name)) IS NULL
ORDER BY 1, 2;

\echo '--- Before the rename'
TABLE fkr.fk_state;
TABLE fkr.par_triggers;
TABLE fkr.child_constraints;
TABLE fkr.child_triggers;
TABLE fkr.drift;
SELECT sql_saga.check_integrity();

-- Referenced-side enforcement before the rename (reference error texts for after the rename).
BEGIN;
DELETE FROM fkr.par WHERE id = 1;
ROLLBACK;

\echo '--- Rename the referencing column of both foreign keys'
ALTER TABLE fkr.rchild RENAME COLUMN par_id TO parent_id;
ALTER TABLE fkr.tchild RENAME COLUMN par_id TO parent_id;

\echo '--- After the rename: names unchanged, columns followed, no drift'
TABLE fkr.fk_state;
TABLE fkr.par_triggers;
TABLE fkr.child_constraints;
TABLE fkr.child_triggers;
TABLE fkr.drift;

\echo '--- Regular FK: referencing side still enforced'
BEGIN;
INSERT INTO fkr.rchild VALUES (2, 99);
ROLLBACK;
INSERT INTO fkr.rchild VALUES (2, 3);

\echo '--- Regular FK: referenced side still enforced (helper triggers use the new column)'
BEGIN;
DELETE FROM fkr.par WHERE id = 1;
ROLLBACK;
BEGIN;
UPDATE fkr.par SET id = 4 WHERE id = 3;
ROLLBACK;

\echo '--- Temporal FK: both sides still enforced'
BEGIN;
INSERT INTO fkr.tchild VALUES (2, 99, '[2021-01-01,2022-01-01)');
ROLLBACK;
BEGIN;
DELETE FROM fkr.par WHERE id = 2;
ROLLBACK;

\echo '--- check_integrity after the rename'
SELECT sql_saga.check_integrity();
SELECT 'integrity checked after rename' AS done;

\echo '--- Unreferenced parent rows can still be deleted'
BEGIN;
DELETE FROM fkr.rchild WHERE id = 1;
DELETE FROM fkr.par WHERE id = 1;
ROLLBACK;

\echo '--- drop_foreign_key by the new column list drops the real objects'
BEGIN;
SELECT sql_saga.drop_foreign_key('fkr.rchild', ARRAY['parent_id']::name[]);
TABLE fkr.par_triggers;
SELECT conname FROM pg_catalog.pg_constraint WHERE conrelid = 'fkr.rchild'::regclass AND contype IN ('c', 't') ORDER BY 1;
TABLE fkr.child_triggers;
TABLE fkr.fk_state;
ROLLBACK;

BEGIN;
SELECT sql_saga.drop_foreign_key('fkr.tchild', ARRAY['parent_id']::name[], 'valid');
SELECT conname FROM pg_catalog.pg_constraint WHERE conrelid = 'fkr.tchild'::regclass AND contype = 'f' ORDER BY 1;
TABLE fkr.fk_state;
ROLLBACK;

\echo '--- Trigger firing modes survive the rename (each trigger keeps its own tgenabled)'
CREATE VIEW fkr.par_trigger_modes AS
SELECT t.tgname, t.tgenabled
FROM pg_catalog.pg_trigger AS t
WHERE t.tgrelid = 'fkr.par'::regclass AND NOT t.tgisinternal
ORDER BY t.tgname;

-- Modes D (update trigger) and A (delete trigger).
BEGIN;
ALTER TABLE fkr.par DISABLE TRIGGER rchild_par_id_fkey_uk_update;
ALTER TABLE fkr.par ENABLE ALWAYS TRIGGER rchild_par_id_fkey_uk_delete;
TABLE fkr.par_trigger_modes;
ALTER TABLE fkr.rchild RENAME COLUMN parent_id TO par_ref;
TABLE fkr.par_trigger_modes;
-- A disabled referenced-side trigger is an integrity violation (reported with its repair); the mode itself survived.
SAVEPOINT disabled_integrity;
SELECT sql_saga.check_integrity();
ROLLBACK TO SAVEPOINT disabled_integrity;
-- The ALWAYS delete trigger still enforces under replica role.
SET LOCAL session_replication_role = replica;
SAVEPOINT replica_delete;
DELETE FROM fkr.par WHERE id = 3;
ROLLBACK TO SAVEPOINT replica_delete;
ROLLBACK;

-- Modes R (update trigger) and O (delete trigger).
BEGIN;
ALTER TABLE fkr.par ENABLE REPLICA TRIGGER rchild_par_id_fkey_uk_update;
TABLE fkr.par_trigger_modes;
ALTER TABLE fkr.rchild RENAME COLUMN parent_id TO par_ref;
TABLE fkr.par_trigger_modes;
SELECT sql_saga.check_integrity();
-- The REPLICA update trigger still enforces under replica role.
SET LOCAL session_replication_role = replica;
SAVEPOINT replica_update;
UPDATE fkr.par SET id = 4 WHERE id = 3;
ROLLBACK TO SAVEPOINT replica_update;
ROLLBACK;

\echo '--- Final check_integrity'
SELECT sql_saga.check_integrity();
SELECT 'integrity checked' AS done;

SET client_min_messages = warning;
RESET search_path;
DROP SCHEMA fkr CASCADE;
\i sql/include/test_teardown.sql
RESET client_min_messages;
