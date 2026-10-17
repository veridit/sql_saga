\i sql/include/test_setup.sql

/*
 * Renaming the columns of an era (ALTER TABLE ... RENAME COLUMN): the synchronized boundary
 * columns (valid_from, valid_until) and the range column.
 *
 * The era follows the rename:
 *   - sql_saga.era.range_column_name / valid_from_column_name / valid_until_column_name name the
 *     renamed columns; no stored object name (checks, sync trigger, sync function) changes;
 *   - the boundary CHECK constraint (from < until AND from > '-infinity') is still recognized, so a
 *     later RENAME CONSTRAINT of it is followed as well;
 *   - the synchronization trigger keeps its name and firing mode, fires on UPDATE OF the renamed
 *     columns, and its function reads and writes the renamed columns (insert and update work);
 *   - unique_keys.column_names does not change (the range column is not a key column).
 * Dependent foreign keys follow:
 *   - regular FKs referencing the table: the referenced-side triggers keep their names and firing
 *     modes; their AFTER UPDATE OF column list and their bound arguments name the renamed columns;
 *     both sides enforce;
 *   - a temporal FK (native PERIOD constraint) follows by attnum and keeps its name; both sides
 *     enforce, also when the REFERENCING table's era columns are renamed (FK child era).
 * check_integrity() passes after every rename.
 */
\set SHOW_CONTEXT never
\set VERBOSITY default
SET client_min_messages = warning;

CREATE SCHEMA e5;
SET search_path = e5, public;

CREATE TABLE e5.par (id int, label text, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('e5.par', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('e5.par', ARRAY['id']::name[], 'valid', key_type => 'natural');
INSERT INTO e5.par (id, label, valid_from, valid_until) VALUES
    (1, 'a', '2020-01-01', '2030-01-01'), (2, 'b', '2020-01-01', '2030-01-01'),
    (3, 'c', '2020-01-01', '2030-01-01'), (4, 'd', '2020-01-01', '2030-01-01');

-- Two regular FKs referencing par, with non-default firing modes on some referenced-side triggers.
CREATE TABLE e5.rchild (id int PRIMARY KEY, par_id int);
SELECT sql_saga.add_regular_foreign_key('e5.rchild', ARRAY['par_id']::name[], 'par_id_valid');
CREATE TABLE e5.rchild2 (id int PRIMARY KEY, owner_id int);
SELECT sql_saga.add_regular_foreign_key('e5.rchild2', ARRAY['owner_id']::name[], 'par_id_valid');
ALTER TABLE e5.par ENABLE ALWAYS TRIGGER rchild_par_id_fkey_uk_delete;
ALTER TABLE e5.par ENABLE REPLICA TRIGGER rchild2_owner_id_fkey_uk_update;

-- A temporal FK whose referencing table has its own synchronized era (sync trigger fires ALWAYS).
CREATE TABLE e5.tchild (id int, par_id int, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('e5.tchild', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_temporal_foreign_key('e5.tchild', ARRAY['par_id']::name[], 'valid', 'par_id_valid');
ALTER TABLE e5.tchild ENABLE ALWAYS TRIGGER tchild_valid_sync_temporal_trg;

INSERT INTO e5.rchild VALUES (1, 1);
INSERT INTO e5.rchild2 VALUES (1, 4);
INSERT INTO e5.tchild (id, par_id, valid_from, valid_until) VALUES (1, 2, '2021-01-01', '2022-01-01');

CREATE VIEW e5.era_state AS
SELECT e.table_name, e.era_name, e.range_column_name, e.valid_from_column_name, e.valid_until_column_name,
       e.bounds_check_constraint, e.boundary_check_constraint, e.sync_temporal_trg_name, e.sync_temporal_trg_function_name
FROM sql_saga.era AS e
WHERE e.table_schema = 'e5'
ORDER BY e.table_name, e.era_name;

CREATE VIEW e5.uk_state AS
SELECT uk.unique_key_name, uk.column_names, uk.unique_constraint
FROM sql_saga.unique_keys AS uk
WHERE uk.table_schema = 'e5'
ORDER BY uk.unique_key_name;

CREATE VIEW e5.fk_state AS
SELECT fk.foreign_key_name, fk.type, fk.column_names, fk.fk_era_name, fk.fk_check_constraint, fk.fk_helper_function,
       fk.uk_update_trigger, fk.uk_delete_trigger, fk.fk_index_name
FROM sql_saga.foreign_keys AS fk
WHERE fk.table_schema = 'e5'
ORDER BY fk.foreign_key_name;

-- Era checks of both tables (boundary and bounds) with their definitions.
CREATE VIEW e5.era_checks AS
SELECT c.conrelid::regclass AS table_name, c.conname, pg_catalog.pg_get_constraintdef(c.oid) AS def
FROM pg_catalog.pg_constraint AS c
WHERE c.conrelid IN ('e5.par'::regclass, 'e5.tchild'::regclass) AND c.contype = 'c'
ORDER BY 1, 2;

-- Non-internal triggers on both tables: mode, function, UPDATE OF column list, and (for the
-- referenced-side FK triggers) the uk column, era and bound arguments.
CREATE VIEW e5.triggers AS
SELECT t.tgrelid::regclass AS table_name, t.tgname, t.tgenabled, t.tgfoid::regproc AS function,
       substring(pg_catalog.pg_get_triggerdef(t.oid) FROM 'UPDATE OF (.*) ON ') AS update_of,
       (string_to_array(encode(t.tgargs, 'escape'), '\000'))[10:13] AS uk_args
FROM pg_catalog.pg_trigger AS t
WHERE t.tgrelid IN ('e5.par'::regclass, 'e5.tchild'::regclass) AND NOT t.tgisinternal
ORDER BY 1, 2;

-- The sync functions must not name any original era column, and must name every current one.
CREATE VIEW e5.sync_functions AS
SELECT e.table_name, p.proname,
       p.prosrc ~ '\m(valid_range|valid_from|valid_until)\M' AS names_original_columns,
       position(pg_catalog.format('NEW.%s ', quote_ident(e.range_column_name)) IN p.prosrc) > 0 AS reads_range,
       position(pg_catalog.format('NEW.%s ', quote_ident(e.valid_from_column_name)) IN p.prosrc) > 0 AS reads_from,
       position(pg_catalog.format('NEW.%s ', quote_ident(e.valid_until_column_name)) IN p.prosrc) > 0 AS reads_until
FROM sql_saga.era AS e
JOIN pg_catalog.pg_proc AS p ON p.pronamespace = 'sql_saga'::regnamespace AND p.proname = e.sync_temporal_trg_function_name
WHERE e.table_schema = 'e5'
ORDER BY e.table_name;

CREATE VIEW e5.child_constraints AS
SELECT c.conrelid::regclass AS table_name, c.conname, pg_catalog.pg_get_constraintdef(c.oid) AS def
FROM pg_catalog.pg_constraint AS c
WHERE c.conrelid IN ('e5.rchild'::regclass, 'e5.rchild2'::regclass, 'e5.tchild'::regclass) AND c.contype IN ('c', 'f')
  AND c.conname NOT IN ('tchild_check', 'tchild_valid_check')
ORDER BY 1, 2;

CREATE VIEW e5.helper AS
SELECT p.oid::regprocedure AS helper, btrim(p.prosrc, E' \t\r\n') AS body
FROM pg_catalog.pg_proc AS p
WHERE p.pronamespace = 'e5'::regnamespace AND p.proname = 'par_id_exists';

\echo '--- Before the renames'
TABLE e5.era_state;
TABLE e5.uk_state;
TABLE e5.fk_state;
TABLE e5.era_checks;
TABLE e5.triggers;
TABLE e5.sync_functions;
TABLE e5.child_constraints;
TABLE e5.helper;
SELECT sql_saga.check_integrity();

\echo '--- Rename valid_from on the referenced table: the era follows at once'
ALTER TABLE e5.par RENAME COLUMN valid_from TO starts_on;
TABLE e5.era_state;
SELECT sql_saga.check_integrity();
SELECT 'integrity checked after the first rename' AS done;

\echo '--- Rename valid_until (to a name that needs quoting) and the range column on the referenced table'
ALTER TABLE e5.par RENAME COLUMN valid_until TO "Ends Before";
ALTER TABLE e5.par RENAME COLUMN valid_range TO period;

\echo '--- Rename the era columns of the referencing table of the temporal FK (range column first)'
ALTER TABLE e5.tchild RENAME COLUMN valid_range TO child_period;
ALTER TABLE e5.tchild RENAME COLUMN valid_from TO child_from;
ALTER TABLE e5.tchild RENAME COLUMN valid_until TO child_until;

\echo '--- After the renames: the era follows, names and firing modes unchanged'
TABLE e5.era_state;
TABLE e5.uk_state;
TABLE e5.fk_state;
TABLE e5.era_checks;
TABLE e5.triggers;
TABLE e5.sync_functions;
TABLE e5.child_constraints;
TABLE e5.helper;
SELECT sql_saga.check_integrity();
SELECT 'integrity checked after the renames' AS done;

\echo '--- The boundary CHECK constraints are still recognized: renaming them is followed'
BEGIN;
ALTER TABLE e5.par RENAME CONSTRAINT par_check TO par_boundary;
ALTER TABLE e5.tchild RENAME CONSTRAINT tchild_check TO tchild_boundary;
SELECT e.table_name, e.boundary_check_constraint FROM sql_saga.era AS e WHERE e.table_schema = 'e5' ORDER BY 1;
SELECT sql_saga.check_integrity();
ROLLBACK;

\echo '--- Synchronization on the referenced table: insert via bounds, via range, update of each'
BEGIN;
INSERT INTO e5.par (id, label, starts_on, "Ends Before") VALUES (10, 'bounds', '2021-01-01', '2022-01-01');
INSERT INTO e5.par (id, label, period) VALUES (11, 'range', '[2021-01-01,2023-01-01)');
UPDATE e5.par SET period = '[2021-06-01,2022-06-01)' WHERE id = 10;
UPDATE e5.par SET "Ends Before" = '2024-01-01' WHERE id = 11;
SELECT id, label, period, starts_on, "Ends Before" FROM e5.par WHERE id >= 10 ORDER BY id;
SAVEPOINT inconsistent;
INSERT INTO e5.par (id, label, period, starts_on, "Ends Before") VALUES (12, 'bad', '[2021-01-01,2022-01-01)', '2021-02-01', '2022-01-01');
ROLLBACK TO SAVEPOINT inconsistent;
SAVEPOINT minus_infinity;
-- The boundary CHECK (renamed columns) rejects a -infinity start.
INSERT INTO e5.par (id, label, starts_on, "Ends Before") VALUES (13, 'minus infinity', '-infinity', '2021-01-01');
ROLLBACK TO SAVEPOINT minus_infinity;
SAVEPOINT no_period;
-- The synchronization error names the renamed columns.
INSERT INTO e5.par (id, label) VALUES (14, 'no period');
ROLLBACK TO SAVEPOINT no_period;
ROLLBACK;

\echo '--- Synchronization on the referencing table of the temporal FK'
BEGIN;
INSERT INTO e5.tchild (id, par_id, child_from, child_until) VALUES (2, 3, '2021-01-01', '2022-01-01');
UPDATE e5.tchild SET child_period = '[2021-03-01,2021-09-01)' WHERE id = 2;
SELECT id, par_id, child_period, child_from, child_until FROM e5.tchild ORDER BY id;
ROLLBACK;

\echo '--- Regular FKs: referencing side'
BEGIN;
INSERT INTO e5.rchild VALUES (2, 3);
INSERT INTO e5.rchild2 VALUES (2, 3);
ROLLBACK;
BEGIN;
INSERT INTO e5.rchild VALUES (3, 99);
ROLLBACK;
BEGIN;
INSERT INTO e5.rchild2 VALUES (3, 99);
ROLLBACK;

\echo '--- Regular FKs: referenced side'
BEGIN;
DELETE FROM e5.par WHERE id = 1;
ROLLBACK;
BEGIN;
DELETE FROM e5.par WHERE id = 4;
ROLLBACK;
BEGIN;
UPDATE e5.par SET id = 5 WHERE id = 1;
ROLLBACK;
BEGIN;
-- A referenced row's period may change through any of the renamed columns (the key survives).
UPDATE e5.par SET period = '[2020-01-01,2020-06-01)' WHERE id = 1;
UPDATE e5.par SET starts_on = '2019-01-01' WHERE id = 1;
UPDATE e5.par SET "Ends Before" = '2031-01-01' WHERE id = 1;
SELECT id, period, starts_on, "Ends Before" FROM e5.par WHERE id = 1;
ROLLBACK;
BEGIN;
DELETE FROM e5.par WHERE id = 3;
SELECT id FROM e5.par ORDER BY id;
ROLLBACK;

\echo '--- Firing modes still effective under replica role'
BEGIN;
SET LOCAL session_replication_role = replica;
SAVEPOINT replica_delete;
DELETE FROM e5.par WHERE id = 1;
ROLLBACK TO SAVEPOINT replica_delete;
SAVEPOINT replica_update;
UPDATE e5.par SET id = 6 WHERE id = 4;
ROLLBACK TO SAVEPOINT replica_update;
SAVEPOINT replica_sync;
INSERT INTO e5.tchild (id, par_id, child_from, child_until) VALUES (3, 2, '2021-01-01', '2022-01-01');
SELECT id, child_period FROM e5.tchild WHERE id = 3;
ROLLBACK TO SAVEPOINT replica_sync;
ROLLBACK;

\echo '--- Temporal FK: both sides'
BEGIN;
INSERT INTO e5.tchild (id, par_id, child_from, child_until) VALUES (4, 99, '2021-01-01', '2022-01-01');
ROLLBACK;
BEGIN;
INSERT INTO e5.tchild (id, par_id, child_from, child_until) VALUES (5, 2, '2019-01-01', '2022-01-01');
ROLLBACK;
BEGIN;
DELETE FROM e5.par WHERE id = 2;
ROLLBACK;
BEGIN;
UPDATE e5.par SET starts_on = '2021-06-01' WHERE id = 2;
ROLLBACK;

\echo '--- Final check_integrity'
SELECT sql_saga.check_integrity();
SELECT 'integrity checked' AS done;

SET client_min_messages = warning;
RESET search_path;
DROP SCHEMA e5 CASCADE;
\i sql/include/test_teardown.sql
RESET client_min_messages;
