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

\echo '--- 8. RENAME COLUMN of a column sql_saga manages: accepted, the catalogs and both views follow'
-- The view columns of both tables, in attnum order.
CREATE VIEW vac.view_column_names AS
SELECT uv.view_name,
       (SELECT array_agg(a.attname ORDER BY a.attnum)
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = format('%I.%I', uv.view_schema, uv.view_name)::regclass
          AND a.attnum > 0 AND NOT a.attisdropped) AS view_columns
FROM sql_saga.updatable_view AS uv
WHERE uv.table_schema = 'vac'
ORDER BY uv.view_name;

-- The identifier of the for_portion_of view.
BEGIN;
ALTER TABLE vac.unit_stat RENAME COLUMN id TO unit_id;
SELECT uk.unique_key_name, uk.column_names FROM sql_saga.unique_keys AS uk WHERE (uk.table_schema, uk.table_name) = ('vac', 'unit_stat');
TABLE vac.view_column_names;
SELECT vac.integrity('after RENAME COLUMN id TO unit_id');
UPDATE vac.unit_stat__for_portion_of_valid SET value_int = 9, valid_from = '2024-11-01', valid_until = 'infinity' WHERE unit_id = 1;
SELECT unit_id, label, value_int, valid_from, valid_until FROM vac.unit_stat WHERE valid_from >= '2024-10-01' ORDER BY unit_id, valid_from;
SELECT vac.integrity('after an update through the view on unit_id');
ROLLBACK;

-- The era's valid_from column of the for_portion_of view.
BEGIN;
ALTER TABLE vac.unit_stat RENAME COLUMN valid_from TO starts_on;
SELECT e.era_name, e.valid_from_column_name, e.valid_until_column_name FROM sql_saga.era AS e WHERE (e.table_schema, e.table_name) = ('vac', 'unit_stat');
TABLE vac.view_column_names;
SELECT vac.integrity('after RENAME COLUMN valid_from TO starts_on');
UPDATE vac.unit_stat__for_portion_of_valid SET value_int = 9, starts_on = '2024-11-01', valid_until = 'infinity' WHERE id = 1;
SELECT id, label, value_int, starts_on, valid_until FROM vac.unit_stat WHERE starts_on >= '2024-10-01' ORDER BY id, starts_on;
SELECT vac.integrity('after an update through the view on starts_on');
ROLLBACK;

-- The era's range column of the current view.
BEGIN;
ALTER TABLE vac.unit_loc RENAME COLUMN valid_range TO validity;
SELECT e.era_name, e.range_column_name FROM sql_saga.era AS e WHERE (e.table_schema, e.table_name) = ('vac', 'unit_loc');
TABLE vac.view_column_names;
SELECT vac.integrity('after RENAME COLUMN valid_range TO validity');
SET LOCAL vac.today = '2024-11-01';
UPDATE vac.unit_loc__current_valid SET value_int = 9 WHERE id = 1;
SELECT id, label, value_int, validity FROM vac.unit_loc WHERE valid_from >= '2024-10-01' ORDER BY id, valid_from;
SELECT id, value_int, validity FROM vac.unit_loc__current_valid;
SELECT vac.integrity('after an update through the view on validity');
ROLLBACK;

\echo '--- 9. An unprivileged owner in a hardened database: ADD / DROP / RENAME on its own tables work'
-- The role owns its tables and has no EXECUTE granted on sql_saga's internal functions, and new functions
-- are not executable by PUBLIC. The event triggers must not make the role's DDL depend on such grants.
CREATE ROLE vac_owner NOSUPERUSER;
CREATE SCHEMA vac_own AUTHORIZATION vac_owner;
GRANT USAGE ON SCHEMA sql_saga TO vac_owner;
ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE vac_owner REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
SET ROLE vac_owner;
-- A table sql_saga does not manage.
CREATE TABLE vac_own.plain (a int, b int, c int);
ALTER TABLE vac_own.plain DROP COLUMN b;
ALTER TABLE vac_own.plain RENAME TO plain2;
SELECT array_agg(a.attname ORDER BY a.attnum) AS plain2_columns
FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = 'vac_own.plain2'::regclass AND a.attnum > 0 AND NOT a.attisdropped;
-- A table with a managed view.
CREATE TABLE vac_own.owned (id int NOT NULL, name text, extra text, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('vac_own.owned', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('vac_own.owned', ARRAY['id'], key_type => 'primary');
SELECT sql_saga.add_for_portion_of_view('vac_own.owned');
INSERT INTO vac_own.owned (id, name, extra, valid_from, valid_until) VALUES (1, 'a', 'x', '2024-01-01', 'infinity');
ALTER TABLE vac_own.owned ADD COLUMN more text;
ALTER TABLE vac_own.owned DROP COLUMN extra;
SELECT array_agg(a.attname ORDER BY a.attnum) AS owned_view_columns,
       (SELECT c.relowner::regrole FROM pg_catalog.pg_class AS c WHERE c.oid = 'vac_own.owned__for_portion_of_valid'::regclass) AS view_owner
FROM pg_catalog.pg_attribute AS a
WHERE a.attrelid = 'vac_own.owned__for_portion_of_valid'::regclass AND a.attnum > 0 AND NOT a.attisdropped;
UPDATE vac_own.owned__for_portion_of_valid SET more = 'm', valid_from = '2024-03-01', valid_until = '2024-05-01' WHERE id = 1;
SELECT id, name, more, valid_from, valid_until FROM vac_own.owned ORDER BY id, valid_from;
RESET ROLE;
ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO PUBLIC;
SELECT vac.integrity('after DDL by the unprivileged owner');

\echo '--- 10. The pending rebuild state is not writable by the session user'
-- The views a DROP COLUMN rebuilds are remembered between ddl_command_start and ddl_command_end in an
-- internal table that only sql_saga's own functions can read or write: a setting the user controls is
-- never read, and the table refuses the user.
SELECT c.relkind,
       c.relowner = (SELECT e.extowner FROM pg_catalog.pg_extension AS e WHERE e.extname = 'sql_saga') AS owned_by_extension_owner,
       EXISTS (SELECT FROM pg_catalog.pg_depend AS d
               WHERE d.classid = 'pg_catalog.pg_class'::regclass AND d.objid = c.oid AND d.deptype = 'e') AS extension_member,
       c.oid = ANY ((SELECT e.extconfig FROM pg_catalog.pg_extension AS e WHERE e.extname = 'sql_saga')::oid[]) AS dumped_as_config,
       has_table_privilege('vac_owner', c.oid, 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE') AS owner_role_has_any_privilege
FROM pg_catalog.pg_class AS c WHERE c.oid = 'sql_saga.__internal_updatable_view_pending'::regclass;
SET ROLE vac_owner;
-- Asking for the rebuild of a view that does not exist, through a setting: ignored.
SET sql_saga.updatable_view_rebuild = '[{"row": {"view_schema": "vac_own", "view_name": "sneaky", "view_type": "for_portion_of", "table_schema": "vac_own", "table_name": "owned", "era_name": "valid", "trigger_name": "sneaky", "current_func": null, "generated_columns": [], "ephemeral_columns": null}, "owner": "vac_owner", "acl": null, "comment": null, "columns": [], "triggers": [{"name": "sneaky", "definition": "CREATE TABLE vac_own.f1_injected_marker ()", "enabled": "O", "comment": null}], "dropped_columns": []}]';
ALTER TABLE vac_own.owned ADD COLUMN another text;
RESET sql_saga.updatable_view_rebuild;
-- Writing the internal table directly: refused.
INSERT INTO sql_saga.__internal_updatable_view_pending DEFAULT VALUES;
DELETE FROM sql_saga.__internal_updatable_view_pending;
RESET ROLE;
SELECT r.rolsuper AS owner_is_superuser FROM pg_catalog.pg_roles AS r WHERE r.rolname = 'vac_owner';
SELECT to_regclass('vac_own.sneaky') AS sneaky_view,
       to_regclass('vac_own.f1_injected_marker') AS injected_marker,
       (SELECT count(*) FROM sql_saga.updatable_view AS uv WHERE uv.view_schema = 'vac_own') AS vac_own_managed_views;
-- The pending state lives only within the statement, and goes away with a failed ALTER TABLE.
SELECT vac.probe($$ALTER TABLE vac.unit_stat DROP COLUMN note, ALTER COLUMN label TYPE int USING 1/0$$) AS failing_drop_column;
SELECT count(*) AS pending_rows FROM sql_saga.__internal_updatable_view_pending;
ALTER TABLE vac_own.owned DROP COLUMN another;
SELECT count(*) AS pending_rows FROM sql_saga.__internal_updatable_view_pending;
TABLE vac.view_shape;
SELECT vac.integrity('after the pending state checks');

\echo '--- 11. A user rule on a managed view: DROP COLUMN is refused, the view is left intact'
-- The rebuild would drop the rule with the view, so it is refused up front, like a dependent user view.
CREATE TABLE vac.rule_log (event text);
CREATE RULE log_delete AS ON DELETE TO vac.unit_stat__for_portion_of_valid DO ALSO INSERT INTO vac.rule_log VALUES ('deleted');
TRUNCATE vac.view_identity;
INSERT INTO vac.view_identity
SELECT uv.view_name, format('%I.%I', uv.view_schema, uv.view_name)::regclass::oid
FROM sql_saga.updatable_view AS uv WHERE uv.table_schema = 'vac';
ALTER TABLE vac.unit_stat DROP COLUMN note;
TABLE vac.view_shape;
TABLE vac.view_identity_check;
SELECT r.rulename FROM pg_catalog.pg_rewrite AS r
WHERE r.ev_class = 'vac.unit_stat__for_portion_of_valid'::regclass ORDER BY r.rulename;
DROP RULE log_delete ON vac.unit_stat__for_portion_of_valid;
DROP TABLE vac.rule_log;
SELECT vac.integrity('after the refused DROP COLUMN under a rule');

\echo '--- 12. View options set by the user (security_barrier, check_option) survive ADD and DROP COLUMN'
ALTER VIEW vac.unit_stat__for_portion_of_valid SET (security_barrier = true, check_option = local);
ALTER VIEW vac.unit_loc__current_valid SET (security_barrier = true, check_option = local);
CREATE VIEW vac.view_options AS
SELECT uv.view_name,
       ARRAY(SELECT o FROM unnest(c.reloptions) AS o ORDER BY o) AS reloptions
FROM sql_saga.updatable_view AS uv
JOIN pg_catalog.pg_class AS c ON c.oid = format('%I.%I', uv.view_schema, uv.view_name)::regclass
WHERE uv.table_schema = 'vac'
ORDER BY uv.view_name;
TABLE vac.view_options;
ALTER TABLE vac.unit_stat ADD COLUMN extra text;
ALTER TABLE vac.unit_loc ADD COLUMN extra text;
TABLE vac.view_options;
ALTER TABLE vac.unit_stat DROP COLUMN extra;
ALTER TABLE vac.unit_loc DROP COLUMN extra;
TABLE vac.view_options;
TABLE vac.view_shape;
SELECT vac.integrity('after ADD and DROP COLUMN with view options');

\echo '--- 13. A view-level ephemeral column follows a rename, and stays protected from DROP COLUMN'
CREATE TABLE vac.audited (id int NOT NULL, name text, edit_at text, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('vac.audited', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('vac.audited', ARRAY['id'], key_type => 'primary');
SELECT sql_saga.add_for_portion_of_view('vac.audited', ephemeral_columns => ARRAY['edit_at']::name[]);
INSERT INTO vac.audited (id, name, edit_at, valid_from, valid_until) VALUES (1, 'a', 'e0', '2024-01-01', 'infinity');
ALTER TABLE vac.audited RENAME COLUMN edit_at TO edited_at;
SELECT uv.view_name, uv.ephemeral_columns FROM sql_saga.updatable_view AS uv WHERE (uv.table_schema, uv.table_name) = ('vac', 'audited');
SELECT array_agg(a.attname ORDER BY a.attnum) AS audited_view_columns
FROM pg_catalog.pg_attribute AS a
WHERE a.attrelid = 'vac.audited__for_portion_of_valid'::regclass AND a.attnum > 0 AND NOT a.attisdropped;
SELECT vac.integrity('after RENAME COLUMN edit_at TO edited_at');
UPDATE vac.audited__for_portion_of_valid SET name = 'b', edited_at = 'e1', valid_from = '2024-03-01', valid_until = '2024-05-01' WHERE id = 1;
SELECT id, name, edited_at, valid_from, valid_until FROM vac.audited ORDER BY id, valid_from;
SELECT vac.probe($$ALTER TABLE vac.audited DROP COLUMN edited_at$$) AS drop_renamed_ephemeral_column;
SELECT uv.view_name, uv.ephemeral_columns FROM sql_saga.updatable_view AS uv WHERE (uv.table_schema, uv.table_name) = ('vac', 'audited');
SELECT vac.integrity('after the refused ephemeral DROP COLUMN');

\echo '--- 14. DROP COLUMN keeps a view column comment or default that has no column grant'
-- Only a comment on one view column, only a default on another, no column-level grant anywhere.
CREATE TABLE vac.facts (id int NOT NULL, name text, note text, extra text, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('vac.facts', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('vac.facts', ARRAY['id'], key_type => 'primary');
SELECT sql_saga.add_for_portion_of_view('vac.facts');
COMMENT ON COLUMN vac.facts__for_portion_of_valid.name IS 'display name';
ALTER VIEW vac.facts__for_portion_of_valid ALTER COLUMN note SET DEFAULT 'n';
ALTER TABLE vac.facts DROP COLUMN extra;
SELECT a.attname, a.attacl IS NULL AS no_column_grant,
       col_description(a.attrelid, a.attnum) AS column_comment,
       pg_get_expr(ad.adbin, ad.adrelid) AS column_default
FROM pg_catalog.pg_attribute AS a
LEFT JOIN pg_catalog.pg_attrdef AS ad ON (ad.adrelid, ad.adnum) = (a.attrelid, a.attnum)
WHERE a.attrelid = 'vac.facts__for_portion_of_valid'::regclass AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum;
SELECT vac.integrity('after DROP COLUMN with a comment-only and a default-only view column');

\echo '--- 15. The ADD COLUMN notice names exactly the added columns, also after an earlier DROP COLUMN'
-- After a DROP COLUMN the table has a dropped-column hole, so attnums no longer count the columns.
CREATE TABLE vac.holes (id int NOT NULL, name text, gone1 text, gone2 text, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('vac.holes', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('vac.holes', ARRAY['id'], key_type => 'primary');
SELECT sql_saga.add_for_portion_of_view('vac.holes');
ALTER TABLE vac.holes DROP COLUMN gone1, DROP COLUMN gone2;
SET client_min_messages = notice;
ALTER TABLE vac.holes ADD COLUMN added1 text;
ALTER TABLE vac.holes ADD COLUMN added2 text, ADD COLUMN added3 int;
SET client_min_messages = warning;
SELECT array_agg(a.attname ORDER BY a.attnum) AS holes_view_columns
FROM pg_catalog.pg_attribute AS a
WHERE a.attrelid = 'vac.holes__for_portion_of_valid'::regclass AND a.attnum > 0 AND NOT a.attisdropped;
SELECT vac.integrity('after ADD COLUMN with dropped-column holes');

\echo '--- 16. A DROP COLUMN whose ddl_command_end did not run leaves a pending row: check_integrity reports it'
-- Disabling only sql_saga_rename_following lets the start hook drop the view without the end hook
-- recreating it. check_integrity first reports the disabled event trigger, then, once it is enabled
-- again, the view left behind in the pending state, with the statements that clear it.
BEGIN;
ALTER EVENT TRIGGER sql_saga_rename_following DISABLE;
ALTER TABLE vac.facts DROP COLUMN note;
SELECT vac.integrity('with sql_saga_rename_following disabled');
ALTER EVENT TRIGGER sql_saga_rename_following ENABLE;
SELECT to_regclass('vac.facts__for_portion_of_valid') AS facts_view,
       (SELECT count(*) FROM sql_saga.updatable_view AS uv WHERE (uv.table_schema, uv.table_name) = ('vac', 'facts')) AS facts_managed_views,
       (SELECT count(*) FROM sql_saga.__internal_updatable_view_pending) AS pending_rows;
SELECT vac.integrity('with the pending row left behind');
-- The repair the report names.
DELETE FROM sql_saga.__internal_updatable_view_pending WHERE (view_schema, view_name) = ('vac', 'facts__for_portion_of_valid');
SELECT sql_saga.add_for_portion_of_view('vac.facts', era_name => 'valid');
SELECT (SELECT count(*) FROM sql_saga.__internal_updatable_view_pending) AS pending_rows,
       array_agg(a.attname ORDER BY a.attnum) AS facts_view_columns
FROM pg_catalog.pg_attribute AS a
WHERE a.attrelid = 'vac.facts__for_portion_of_valid'::regclass AND a.attnum > 0 AND NOT a.attisdropped;
SELECT vac.integrity('after the repair of the pending row');
ROLLBACK;

\echo '--- 17. The repair named for a pending for_portion_of view restores its ephemeral_columns'
-- vac.audited's view has ephemeral_columns {edited_at} (section 13). The repair text from check_integrity
-- is executed verbatim; the recreated view must carry the same ephemeral_columns as the dropped one.
ALTER TABLE vac.audited ADD COLUMN scratch text;
CREATE FUNCTION vac.run_reported_repair() RETURNS text LANGUAGE plpgsql AS $run_reported_repair$
DECLARE
    v_message text;
BEGIN
    BEGIN
        PERFORM sql_saga.check_integrity();
        RETURN 'nothing reported';
    EXCEPTION WHEN OTHERS THEN
        v_message := SQLERRM;
    END;
    IF position('; repair with ' IN v_message) = 0 THEN
        RETURN 'no repair in: ' || v_message;
    END IF;
    EXECUTE substring(v_message FROM position('; repair with ' IN v_message) + length('; repair with '));
    RETURN 'executed the reported repair';
END;
$run_reported_repair$;
BEGIN;
ALTER EVENT TRIGGER sql_saga_rename_following DISABLE;
ALTER TABLE vac.audited DROP COLUMN scratch;
ALTER EVENT TRIGGER sql_saga_rename_following ENABLE;
SELECT to_regclass('vac.audited__for_portion_of_valid') AS audited_view,
       (SELECT p.ephemeral_columns FROM sql_saga.__internal_updatable_view_pending AS p
        WHERE (p.view_schema, p.view_name) = ('vac', 'audited__for_portion_of_valid')) AS pending_ephemeral_columns;
SELECT vac.integrity('with the pending ephemeral view left behind');
SELECT vac.run_reported_repair();
SELECT uv.view_name, uv.ephemeral_columns,
       (SELECT count(*) FROM sql_saga.__internal_updatable_view_pending) AS pending_rows
FROM sql_saga.updatable_view AS uv WHERE (uv.table_schema, uv.table_name) = ('vac', 'audited');
SELECT vac.integrity('after the repair of the pending ephemeral view');
ROLLBACK;
ALTER TABLE vac.audited DROP COLUMN scratch;
SELECT uv.view_name, uv.ephemeral_columns FROM sql_saga.updatable_view AS uv WHERE (uv.table_schema, uv.table_name) = ('vac', 'audited');
SELECT vac.integrity('at the end');

SET client_min_messages = warning;
RESET search_path;
DROP SCHEMA vac CASCADE;
DROP SCHEMA vac_own CASCADE;
DROP OWNED BY vac_owner;
DROP ROLE vac_owner;
DROP ROLE vac_reader;
RESET vac.today;
\i sql/include/test_teardown.sql
RESET client_min_messages;
