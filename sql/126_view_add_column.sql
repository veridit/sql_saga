\i sql/include/test_setup.sql

/*
 * Updatable views follow the column shape of their base table (tmp/fable_audit_v13.md finding 3, A3).
 *
 * add_for_portion_of_view and add_current_view create views whose column list is frozen when the view is
 * created. ALTER TABLE ... ADD COLUMN on the base afterwards is a normal, supported operation (StatBus did it
 * twice: stat_for_unit.stat GENERATED ALWAYS ... STORED, and location.region_version_id plain + FK + CHECK).
 * After every such ADD COLUMN:
 *   - the view exposes the new column (same name and type, appended after the existing ones);
 *   - the view keeps its identity (same OID), so grants on it and user views built on it survive;
 *   - sql_saga.updatable_view.generated_columns lists exactly the base's GENERATED columns;
 *   - an UPDATE through the view sets the new column for the given portion and preserves it elsewhere,
 *     and a generated column is computed by the base, never written;
 *   - check_integrity() passes.
 * DROP COLUMN of a column sql_saga does not manage succeeds without CASCADE: the view is rebuilt without
 * the column (keeping the grants made on it), generated_columns is recomputed, check_integrity() passes.
 * While a user view depends on the managed view, that DROP COLUMN is refused (the rebuild would drop it).
 * DROP COLUMN of a column sql_saga manages (the identifier) stays refused.
 */
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;

CREATE SCHEMA vac;
SET search_path = vac, public;
CREATE ROLE vac_reader;

-- A settable "now" for the current view, so the test is deterministic and each current-view update
-- happens on its own day.
SET vac.today = '2024-06-01';
CREATE FUNCTION vac.test_now() RETURNS date LANGUAGE sql STABLE AS $$ SELECT current_setting('vac.today')::date $$;

-- check_integrity(), reported as one line so that a failure does not hide the steps after it.
CREATE FUNCTION vac.integrity(step text) RETURNS text LANGUAGE plpgsql AS $integrity$
BEGIN
    PERFORM sql_saga.check_integrity();
    RETURN step || ': ok';
EXCEPTION WHEN OTHERS THEN
    RETURN step || ': ' || SQLERRM;
END;
$integrity$;

-- Runs one statement, always rolled back, and reports whether it was accepted or refused.
CREATE FUNCTION vac.probe(statement text) RETURNS text LANGUAGE plpgsql AS $probe$
BEGIN
    BEGIN
        SET CONSTRAINTS ALL IMMEDIATE;
        EXECUTE statement;
        RAISE EXCEPTION USING ERRCODE = 'P0004', MESSAGE = 'accepted';
    EXCEPTION
        WHEN assert_failure THEN RETURN 'accepted';
        WHEN OTHERS THEN RETURN 'refused';
    END;
END;
$probe$;

-- Base columns versus view columns, by name and type, in attnum order.
CREATE VIEW vac.view_shape AS
SELECT uv.view_name, uv.view_type,
       (SELECT array_agg(format('%s %s', a.attname, format_type(a.atttypid, a.atttypmod)) ORDER BY a.attnum)
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = format('%I.%I', uv.table_schema, uv.table_name)::regclass
          AND a.attnum > 0 AND NOT a.attisdropped) AS base_columns,
       (SELECT array_agg(format('%s %s', a.attname, format_type(a.atttypid, a.atttypmod)) ORDER BY a.attnum)
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = format('%I.%I', uv.view_schema, uv.view_name)::regclass
          AND a.attnum > 0 AND NOT a.attisdropped) AS view_columns,
       uv.generated_columns,
       (SELECT COALESCE(array_agg(a.attname ORDER BY a.attnum), '{}')
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = format('%I.%I', uv.table_schema, uv.table_name)::regclass
          AND a.attnum > 0 AND NOT a.attisdropped
          AND (a.attgenerated <> '' OR a.attidentity = 'a')) AS base_generated_columns
FROM sql_saga.updatable_view AS uv
WHERE uv.table_schema = 'vac'
ORDER BY uv.view_name;

-- The view's OID and the grant made on it must survive every column change.
CREATE TABLE vac.view_identity AS
SELECT uv.view_name, format('%I.%I', uv.view_schema, uv.view_name)::regclass::oid AS view_oid
FROM sql_saga.updatable_view AS uv WHERE false;
CREATE VIEW vac.view_identity_check AS
SELECT vi.view_name,
       format('vac.%I', vi.view_name)::regclass::oid = vi.view_oid AS same_oid,
       has_table_privilege('vac_reader', format('vac.%I', vi.view_name), 'SELECT') AS reader_can_select
FROM vac.view_identity AS vi
ORDER BY vi.view_name;

CREATE TABLE vac.region (id int PRIMARY KEY);
INSERT INTO vac.region VALUES (1), (2), (-1); -- -1 exists, so region_id = -1 can only be refused by the CHECK

-- for_portion_of view on a table shaped like StatBus's stat_for_unit / location.
CREATE TABLE vac.unit_stat (
    id int NOT NULL,
    name text,
    value_int int,
    valid_range daterange NOT NULL,
    valid_from date,
    valid_until date
);
SELECT sql_saga.add_era('vac.unit_stat', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('vac.unit_stat', ARRAY['id'], key_type => 'primary');
SELECT sql_saga.add_for_portion_of_view('vac.unit_stat');

-- current view on a second table.
CREATE TABLE vac.unit_loc (
    id int NOT NULL,
    name text,
    value_int int,
    valid_range daterange NOT NULL,
    valid_from date,
    valid_until date
);
SELECT sql_saga.add_era('vac.unit_loc', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('vac.unit_loc', ARRAY['id'], key_type => 'primary');
SELECT sql_saga.add_current_view('vac.unit_loc', current_func_name => 'vac.test_now()');

INSERT INTO vac.unit_stat (id, name, value_int, valid_from, valid_until) VALUES (1, 'a', 1, '2024-01-01', 'infinity');
INSERT INTO vac.unit_loc (id, name, value_int, valid_from, valid_until) VALUES (1, 'a', 1, '2024-01-01', 'infinity');

-- Grants on the views and a user view built on one of them (StatBus copies base ACLs to its views).
GRANT SELECT ON vac.unit_stat__for_portion_of_valid, vac.unit_loc__current_valid TO vac_reader;
CREATE VIEW vac.unit_stat_names AS SELECT id, name FROM vac.unit_stat__for_portion_of_valid;
INSERT INTO vac.view_identity
SELECT uv.view_name, format('%I.%I', uv.view_schema, uv.view_name)::regclass::oid
FROM sql_saga.updatable_view AS uv WHERE uv.table_schema = 'vac';

TABLE vac.view_shape;
SELECT vac.integrity('after creating the views');

\echo '--- 1. ADD COLUMN (plain)'
ALTER TABLE vac.unit_stat ADD COLUMN note text;
ALTER TABLE vac.unit_loc ADD COLUMN note text;
TABLE vac.view_shape;
TABLE vac.view_identity_check;
SELECT vac.integrity('after ADD COLUMN note');

UPDATE vac.unit_stat__for_portion_of_valid SET note = 'mid', valid_from = '2024-03-01', valid_until = '2024-05-01' WHERE id = 1;
SELECT id, name, value_int, note, valid_from, valid_until FROM vac.unit_stat ORDER BY id, valid_from;
UPDATE vac.unit_loc__current_valid SET note = 'now' WHERE id = 1;
SELECT id, name, value_int, note, valid_from, valid_until FROM vac.unit_loc ORDER BY id, valid_from;
-- A portion update of another column preserves the new column's values.
UPDATE vac.unit_stat__for_portion_of_valid SET name = 'b', valid_from = '2024-04-01', valid_until = '2024-08-01' WHERE id = 1;
SELECT id, name, value_int, note, valid_from, valid_until FROM vac.unit_stat ORDER BY id, valid_from;
SELECT id, name, note FROM vac.unit_stat__for_portion_of_valid WHERE valid_from = '2024-03-01';
SELECT id, name, note FROM vac.unit_loc__current_valid;
SELECT vac.integrity('after updates through the views with note');

\echo '--- 2. ADD COLUMN ... GENERATED ALWAYS AS (...) STORED'
ALTER TABLE vac.unit_stat ADD COLUMN doubled int GENERATED ALWAYS AS (value_int * 2) STORED;
ALTER TABLE vac.unit_loc ADD COLUMN doubled int GENERATED ALWAYS AS (value_int * 2) STORED;
TABLE vac.view_shape;
TABLE vac.view_identity_check;
SELECT vac.integrity('after ADD COLUMN doubled (generated)');

-- The generated column is computed by the base for the new portion and kept elsewhere.
UPDATE vac.unit_stat__for_portion_of_valid SET value_int = 5, valid_from = '2024-02-01', valid_until = '2024-03-01' WHERE id = 1;
SELECT id, name, value_int, doubled, note, valid_from, valid_until FROM vac.unit_stat ORDER BY id, valid_from;
SET vac.today = '2024-07-01';
UPDATE vac.unit_loc__current_valid SET value_int = 7 WHERE id = 1;
SELECT id, name, value_int, doubled, note, valid_from, valid_until FROM vac.unit_loc ORDER BY id, valid_from;
SELECT id, value_int, doubled FROM vac.unit_loc__current_valid;
SELECT vac.integrity('after updates through the views with doubled');

\echo '--- 3. ADD COLUMN with a foreign key and a CHECK (StatBus location.region_version_id)'
ALTER TABLE vac.unit_stat ADD COLUMN region_id int REFERENCES vac.region (id) CHECK (region_id > 0);
ALTER TABLE vac.unit_loc ADD COLUMN region_id int REFERENCES vac.region (id) CHECK (region_id > 0);
TABLE vac.view_shape;
TABLE vac.view_identity_check;
SELECT vac.integrity('after ADD COLUMN region_id');

UPDATE vac.unit_stat__for_portion_of_valid SET region_id = 2, valid_from = '2024-06-01', valid_until = 'infinity' WHERE id = 1;
SELECT id, name, value_int, doubled, note, region_id, valid_from, valid_until FROM vac.unit_stat ORDER BY id, valid_from;
SET vac.today = '2024-08-01';
UPDATE vac.unit_loc__current_valid SET region_id = 1 WHERE id = 1;
SELECT id, name, value_int, doubled, note, region_id, valid_from, valid_until FROM vac.unit_loc ORDER BY id, valid_from;
-- The new column's constraints are enforced through the views.
SELECT vac.probe($$UPDATE vac.unit_stat__for_portion_of_valid SET region_id = 1, valid_from = '2024-01-01', valid_until = '2024-02-01' WHERE id = 1$$) AS portion_valid_region;
SELECT vac.probe($$UPDATE vac.unit_stat__for_portion_of_valid SET region_id = 99, valid_from = '2024-01-01', valid_until = '2024-02-01' WHERE id = 1$$) AS portion_missing_region;
SELECT vac.probe($$UPDATE vac.unit_stat__for_portion_of_valid SET region_id = -1, valid_from = '2024-01-01', valid_until = '2024-02-01' WHERE id = 1$$) AS portion_check_violation;
SET vac.today = '2024-08-15';
SELECT vac.probe($$UPDATE vac.unit_loc__current_valid SET region_id = 2 WHERE id = 1$$) AS current_valid_region;
SELECT vac.probe($$UPDATE vac.unit_loc__current_valid SET region_id = 99 WHERE id = 1$$) AS current_missing_region;
SELECT vac.probe($$UPDATE vac.unit_loc__current_valid SET region_id = -1 WHERE id = 1$$) AS current_check_violation;
SELECT vac.integrity('after updates through the views with region_id');

\echo '--- 4. User view on the for_portion_of view still works'
SELECT id, name FROM vac.unit_stat_names ORDER BY id, name;

\echo '--- 5. DROP COLUMN of a column sql_saga does not manage: the view is rebuilt without it'
-- A view cannot lose a column in place, so the rebuild replaces the managed view. While a user view
-- depends on the managed view that would silently drop the user's view, so the DROP COLUMN is refused.
SELECT vac.probe($$ALTER TABLE vac.unit_stat DROP COLUMN doubled$$) AS drop_column_under_user_view;
DROP VIEW vac.unit_stat_names;
ALTER TABLE vac.unit_stat DROP COLUMN doubled;
ALTER TABLE vac.unit_loc DROP COLUMN note;
TABLE vac.view_shape;
SELECT vi.view_name, has_table_privilege('vac_reader', format('vac.%I', vi.view_name), 'SELECT') AS reader_can_select
FROM vac.view_identity AS vi ORDER BY vi.view_name;
SELECT vac.integrity('after DROP COLUMN');

UPDATE vac.unit_stat__for_portion_of_valid SET note = 'late', valid_from = '2024-09-01', valid_until = 'infinity' WHERE id = 1;
SELECT id, name, value_int, note, region_id, valid_from, valid_until FROM vac.unit_stat ORDER BY id, valid_from;
SET vac.today = '2024-09-01';
UPDATE vac.unit_loc__current_valid SET value_int = 8 WHERE id = 1;
SELECT id, name, value_int, doubled, region_id, valid_from, valid_until FROM vac.unit_loc ORDER BY id, valid_from;
SELECT vac.integrity('after updates through the views after DROP COLUMN');

\echo '--- 6. DROP COLUMN of the identifier stays refused'
SELECT vac.probe($$ALTER TABLE vac.unit_stat DROP COLUMN id$$) AS drop_identifier_column;
SELECT vac.integrity('after the refused identifier drop');

\echo '--- 7. RENAME COLUMN of a column sql_saga does not manage: the view column is renamed with it'
-- The views were rebuilt in step 5: remember their current OIDs. A rename needs no rebuild, so the
-- views keep their OIDs, grants (table and column level) and comments.
TRUNCATE vac.view_identity;
INSERT INTO vac.view_identity
SELECT uv.view_name, format('%I.%I', uv.view_schema, uv.view_name)::regclass::oid
FROM sql_saga.updatable_view AS uv WHERE uv.table_schema = 'vac';
COMMENT ON COLUMN vac.unit_stat__for_portion_of_valid.name IS 'display name';
COMMENT ON COLUMN vac.unit_loc__current_valid.name IS 'display name';
GRANT UPDATE (name) ON vac.unit_stat__for_portion_of_valid, vac.unit_loc__current_valid TO vac_reader;
ALTER TABLE vac.unit_stat RENAME COLUMN name TO label;
ALTER TABLE vac.unit_loc RENAME COLUMN name TO label;
TABLE vac.view_shape;
TABLE vac.view_identity_check;
SELECT vi.view_name,
       col_description(format('vac.%I', vi.view_name)::regclass, a.attnum) AS label_comment,
       has_column_privilege('vac_reader', format('vac.%I', vi.view_name), 'label', 'UPDATE') AS reader_can_update_label
FROM vac.view_identity AS vi
JOIN pg_catalog.pg_attribute AS a ON a.attrelid = format('vac.%I', vi.view_name)::regclass AND a.attname = 'label'
ORDER BY vi.view_name;
SELECT vac.integrity('after RENAME COLUMN name TO label');

-- Updates through the views write the renamed column, and preserve it elsewhere.
UPDATE vac.unit_stat__for_portion_of_valid SET label = 'renamed', valid_from = '2024-10-01', valid_until = 'infinity' WHERE id = 1;
SELECT id, label, value_int, note, region_id, valid_from, valid_until FROM vac.unit_stat ORDER BY id, valid_from;
SET vac.today = '2024-10-01';
UPDATE vac.unit_loc__current_valid SET label = 'renamed' WHERE id = 1;
SELECT id, label, value_int, doubled, region_id, valid_from, valid_until FROM vac.unit_loc ORDER BY id, valid_from;
SELECT id, label FROM vac.unit_loc__current_valid;
SELECT vac.integrity('after updates through the views after RENAME COLUMN');

\echo '--- 8. RENAME COLUMN of a column sql_saga manages: accepted, and the catalogs follow (as in 1.2)'
BEGIN;
ALTER TABLE vac.unit_stat RENAME COLUMN id TO unit_id;
SELECT uk.unique_key_name, uk.column_names FROM sql_saga.unique_keys AS uk WHERE (uk.table_schema, uk.table_name) = ('vac', 'unit_stat');
ROLLBACK;
BEGIN;
ALTER TABLE vac.unit_stat RENAME COLUMN valid_from TO starts_on;
SELECT e.era_name, e.valid_from_column_name, e.valid_until_column_name FROM sql_saga.era AS e WHERE (e.table_schema, e.table_name) = ('vac', 'unit_stat');
ROLLBACK;
BEGIN;
ALTER TABLE vac.unit_loc RENAME COLUMN valid_range TO validity;
SELECT e.era_name, e.range_column_name FROM sql_saga.era AS e WHERE (e.table_schema, e.table_name) = ('vac', 'unit_loc');
ROLLBACK;
SELECT vac.integrity('at the end');

SET client_min_messages = warning;
RESET search_path;
DROP SCHEMA vac CASCADE;
DROP ROLE vac_reader;
RESET vac.today;
\i sql/include/test_teardown.sql
RESET client_min_messages;
