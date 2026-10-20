\i sql/include/test_setup.sql

/*
 * Managed updatable views follow ALTER TABLE ... ALTER COLUMN ... TYPE (1.3).
 *
 * add_for_portion_of_view and add_current_view create views that reference every column of the base.
 * PostgreSQL refuses ALTER COLUMN TYPE of a column a view uses ("cannot alter type of a column used by
 * a view or rule"), so with sql_saga's views in place a plain type change of an ordinary column was
 * impossible without dropping and re-adding the views by hand.
 *
 * Owner decisions:
 *   - When sql_saga's own views are the only dependants of the column, the views follow the type change:
 *     the ALTER succeeds, the view column has the new type (the view may get a new OID), and grants,
 *     column grants, comments, column defaults, user triggers on the view, view options and extension
 *     membership are kept. A NOTICE says exactly what was done. Updates through the views write the
 *     new type. check_integrity() passes.
 *   - sql_saga never modifies objects outside its purview: when a USER object (a view, a rule, ...)
 *     depends on the managed view, the ALTER is refused before any change, naming the user object, with a
 *     HINT to drop it, run the ALTER TABLE, and recreate it.
 *   - A user view directly on the base column is PostgreSQL's business: its own refusal stands.
 *
 * Columns sql_saga manages:
 *   - An identifier (unique key column) that no sql_saga foreign key uses: follows like an ordinary column.
 *     Neither the unique_keys catalog nor the views' INSTEAD OF triggers record its type (only its name);
 *     PostgreSQL rebuilds the WITHOUT OVERLAPS constraint itself.
 *   - A column a sql_saga foreign key uses, on either side (the referenced key column, or the referencing
 *     column of a regular_to_temporal or temporal_to_temporal key): refused by sql_saga, naming the foreign
 *     key, with a HINT to drop it with drop_foreign_key_by_name, alter both sides, and add it again. The
 *     regular key's helper function has a type-bound signature, and both kinds require matching types, so
 *     a one-sided change cannot be followed; sql_saga never alters the other table itself.
 *   - The era range column and its boundary columns (valid_from / valid_until / valid_to): refused by sql_saga.
 *     The era catalog caches range_type, multirange_type and range_subtype, and the generated synchronization
 *     trigger and function, the bounds CHECK constraints and the views' WHERE clause are built for that type;
 *     nothing rebuilds them for a new type. The HINT describes the public migration: drop the era's
 *     dependants (foreign keys, unique keys, updatable views) with their drop functions, drop_era with
 *     cleanup => false, change the range and its boundary columns together, add_era again and recreate the
 *     dependants. Section 2b runs that migration once for a range-only era. Today a change of the range
 *     column of an era without boundary columns is accepted when no view exists and leaves the era catalog
 *     noncanonical (check_integrity refuses); with boundary columns PostgreSQL refuses because of the
 *     synchronization trigger.
 *   - Inheritance: a type change on a parent recurses to a child with a managed view; the child's view
 *     follows the same way, and the child's era columns are refused the same way.
 */
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;

CREATE SCHEMA vat;
SET search_path = vat, public;
CREATE ROLE vat_reader;

-- A settable "now" for the current view, so the test is deterministic.
SET vat.today = '2024-06-01';
CREATE FUNCTION vat.test_now() RETURNS date LANGUAGE sql STABLE AS $$ SELECT current_setting('vat.today')::date $$;

-- check_integrity(), reported as one line so that a failure does not hide the steps after it.
CREATE FUNCTION vat.integrity(step text) RETURNS text LANGUAGE plpgsql AS $integrity$
BEGIN
    PERFORM sql_saga.check_integrity();
    RETURN step || ': ok';
EXCEPTION WHEN OTHERS THEN
    RETURN step || ': ' || SQLERRM;
END;
$integrity$;

-- Runs one statement in a subtransaction that is always rolled back. Reports 'accepted', or the error
-- with its SQLSTATE, DETAIL and HINT. Nothing of the statement is ever kept.
CREATE FUNCTION vat.attempt(statement text) RETURNS text LANGUAGE plpgsql AS $attempt$
DECLARE
    v_state text;
    v_message text;
    v_detail text;
    v_hint text;
BEGIN
    EXECUTE statement;
    RAISE EXCEPTION USING ERRCODE = 'P0004', MESSAGE = 'accepted';
EXCEPTION
WHEN assert_failure THEN
    RETURN 'accepted';
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_message = MESSAGE_TEXT,
                            v_detail = PG_EXCEPTION_DETAIL, v_hint = PG_EXCEPTION_HINT;
    RETURN format('%s: %s', v_state, v_message)
        || CASE WHEN v_detail <> '' THEN format(E'\nDETAIL: %s', v_detail) ELSE '' END
        || CASE WHEN v_hint <> '' THEN format(E'\nHINT: %s', v_hint) ELSE '' END;
END;
$attempt$;

-- Base columns versus view columns, by name and type, in attnum order.
CREATE VIEW vat.view_shape AS
SELECT uv.view_name,
       (SELECT array_agg(format('%s %s', a.attname, format_type(a.atttypid, a.atttypmod)) ORDER BY a.attnum)
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = format('%I.%I', uv.table_schema, uv.table_name)::regclass
          AND a.attnum > 0 AND NOT a.attisdropped) AS base_columns,
       (SELECT array_agg(format('%s %s', a.attname, format_type(a.atttypid, a.atttypmod)) ORDER BY a.attnum)
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = format('%I.%I', uv.view_schema, uv.view_name)::regclass
          AND a.attnum > 0 AND NOT a.attisdropped) AS view_columns
FROM sql_saga.updatable_view AS uv
WHERE uv.table_schema = 'vat'
ORDER BY uv.view_name;

-- Everything about a managed view that the user may have set and that must survive a rebuild.
CREATE VIEW vat.view_facts AS
SELECT uv.view_name,
       ARRAY(SELECT o FROM unnest(c.reloptions) AS o ORDER BY o) AS reloptions,
       has_table_privilege('vat_reader', c.oid, 'SELECT') AS reader_select,
       has_column_privilege('vat_reader', c.oid, 'name', 'UPDATE') AS reader_update_name,
       obj_description(c.oid, 'pg_class') AS view_comment,
       (SELECT array_agg(format('%s: comment=%s default=%s', a.attname,
                                coalesce(col_description(c.oid, a.attnum), '-'),
                                coalesce(pg_get_expr(ad.adbin, ad.adrelid), '-')) ORDER BY a.attnum)
        FROM pg_catalog.pg_attribute AS a
        LEFT JOIN pg_catalog.pg_attrdef AS ad ON (ad.adrelid, ad.adnum) = (a.attrelid, a.attnum)
        WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
          AND (ad.oid IS NOT NULL OR col_description(c.oid, a.attnum) IS NOT NULL)) AS column_facts,
       (SELECT array_agg(format('%s (%s) %s', t.tgname, t.tgenabled,
                                coalesce(obj_description(t.oid, 'pg_trigger'), '-')) ORDER BY t.tgname)
        FROM pg_catalog.pg_trigger AS t
        WHERE t.tgrelid = c.oid AND NOT t.tgisinternal) AS triggers,
       (SELECT x.extname FROM pg_catalog.pg_depend AS d JOIN pg_catalog.pg_extension AS x ON x.oid = d.refobjid
        WHERE d.classid = 'pg_catalog.pg_class'::regclass AND d.objid = c.oid AND d.objsubid = 0
          AND d.refclassid = 'pg_catalog.pg_extension'::regclass AND d.deptype = 'e') AS extension
FROM sql_saga.updatable_view AS uv
JOIN pg_catalog.pg_class AS c ON c.oid = format('%I.%I', uv.view_schema, uv.view_name)::regclass
WHERE uv.table_schema = 'vat'
ORDER BY uv.view_name;

-- The OIDs of the managed views, to show which steps keep them.
CREATE TABLE vat.view_identity (view_name name, view_oid oid);
CREATE FUNCTION vat.remember_views() RETURNS void LANGUAGE sql AS $remember_views$
    DELETE FROM vat.view_identity;
    INSERT INTO vat.view_identity
    SELECT uv.view_name, format('%I.%I', uv.view_schema, uv.view_name)::regclass::oid
    FROM sql_saga.updatable_view AS uv WHERE uv.table_schema = 'vat';
$remember_views$;
CREATE VIEW vat.view_identity_check AS
SELECT vi.view_name, to_regclass(format('vat.%I', vi.view_name))::oid = vi.view_oid AS same_oid
FROM vat.view_identity AS vi
ORDER BY vi.view_name;

-- for_portion_of view.
CREATE TABLE vat.unit_stat (
    id int NOT NULL,
    name text,
    value_int int,
    valid_range daterange NOT NULL,
    valid_from date,
    valid_until date
);
SELECT sql_saga.add_era('vat.unit_stat', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('vat.unit_stat', ARRAY['id'], key_type => 'primary');
SELECT sql_saga.add_for_portion_of_view('vat.unit_stat');

-- current view.
CREATE TABLE vat.unit_loc (
    id int NOT NULL,
    name text,
    value_int int,
    valid_range daterange NOT NULL,
    valid_from date,
    valid_until date
);
SELECT sql_saga.add_era('vat.unit_loc', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('vat.unit_loc', ARRAY['id'], key_type => 'primary');
SELECT sql_saga.add_current_view('vat.unit_loc', current_func_name => 'vat.test_now()');

INSERT INTO vat.unit_stat (id, name, value_int, valid_from, valid_until) VALUES (1, 'a', 1, '2024-01-01', 'infinity');
INSERT INTO vat.unit_loc (id, name, value_int, valid_from, valid_until) VALUES (1, 'a', 1, '2024-01-01', 'infinity');

-- What the user set on the views: grants, a column grant, comments, column defaults, user triggers
-- (one with a comment), view options and extension membership.
GRANT SELECT ON vat.unit_stat__for_portion_of_valid, vat.unit_loc__current_valid TO vat_reader;
GRANT UPDATE (name) ON vat.unit_stat__for_portion_of_valid, vat.unit_loc__current_valid TO vat_reader;
COMMENT ON VIEW vat.unit_stat__for_portion_of_valid IS 'stat portions';
COMMENT ON VIEW vat.unit_loc__current_valid IS 'current locations';
COMMENT ON COLUMN vat.unit_stat__for_portion_of_valid.name IS 'display name';
COMMENT ON COLUMN vat.unit_loc__current_valid.name IS 'display name';
ALTER VIEW vat.unit_stat__for_portion_of_valid ALTER COLUMN name SET DEFAULT 'unnamed';
ALTER VIEW vat.unit_loc__current_valid ALTER COLUMN name SET DEFAULT 'unnamed';
ALTER VIEW vat.unit_loc__current_valid ALTER COLUMN value_int SET DEFAULT 7;
CREATE TABLE vat.update_log (view_name text, at_step text);
CREATE FUNCTION vat.log_update() RETURNS trigger LANGUAGE plpgsql AS $log_update$
BEGIN
    INSERT INTO vat.update_log VALUES (TG_TABLE_NAME, current_setting('vat.step'));
    RETURN NULL;
END;
$log_update$;
CREATE TRIGGER log_update AFTER UPDATE ON vat.unit_stat__for_portion_of_valid FOR EACH STATEMENT EXECUTE FUNCTION vat.log_update();
CREATE TRIGGER log_update AFTER UPDATE ON vat.unit_loc__current_valid FOR EACH STATEMENT EXECUTE FUNCTION vat.log_update();
COMMENT ON TRIGGER log_update ON vat.unit_stat__for_portion_of_valid IS 'audit';
ALTER VIEW vat.unit_stat__for_portion_of_valid SET (security_barrier = true, check_option = local);
ALTER VIEW vat.unit_loc__current_valid SET (check_option = cascaded);
-- Extension membership (a view put into an extension): kept as it was.
ALTER EXTENSION btree_gist ADD VIEW vat.unit_stat__for_portion_of_valid;

TABLE vat.view_shape;
TABLE vat.view_facts;
SELECT vat.integrity('after creating the views');

\echo '--- 1a. ALTER COLUMN TYPE text -> varchar(20) of an ordinary column: the views follow'
SET client_min_messages = notice;
ALTER TABLE vat.unit_stat ALTER COLUMN name TYPE varchar(20);
ALTER TABLE vat.unit_loc ALTER COLUMN name TYPE varchar(20);
SET client_min_messages = warning;
TABLE vat.view_shape;
TABLE vat.view_facts;
SELECT count(*) AS pending_rows FROM sql_saga.__internal_updatable_view_pending;
SELECT vat.integrity('after name TYPE varchar(20)');

-- Updates through the views write the new type; the user triggers still fire.
SET vat.step = '1a';
UPDATE vat.unit_stat__for_portion_of_valid SET name = 'mid', valid_from = '2024-03-01', valid_until = '2024-05-01' WHERE id = 1;
SELECT id, name, pg_typeof(name) AS name_type, value_int, valid_from, valid_until FROM vat.unit_stat ORDER BY id, valid_from;
SET vat.today = '2024-07-01';
UPDATE vat.unit_loc__current_valid SET name = 'now' WHERE id = 1;
SELECT id, name, pg_typeof(name) AS name_type, value_int, valid_from, valid_until FROM vat.unit_loc ORDER BY id, valid_from;
-- The new type is enforced through the view (on a later day, so only the type can refuse it).
SET vat.today = '2024-07-15';
SELECT vat.attempt($$UPDATE vat.unit_loc__current_valid SET name = repeat('x', 21) WHERE id = 1$$) AS too_long_name;
TABLE vat.update_log;
-- The column default of the view still applies: an INSERT through the current view.
INSERT INTO vat.unit_loc__current_valid (id) VALUES (2);
SELECT id, name, value_int, valid_from, valid_until FROM vat.unit_loc WHERE id = 2;
SELECT vat.integrity('after updates through the views with varchar(20)');

\echo '--- 1b. ALTER COLUMN TYPE int -> bigint USING ...: the views follow'
SET client_min_messages = notice;
ALTER TABLE vat.unit_stat ALTER COLUMN value_int TYPE bigint USING value_int::bigint;
ALTER TABLE vat.unit_loc ALTER COLUMN value_int TYPE bigint USING value_int::bigint;
SET client_min_messages = warning;
TABLE vat.view_shape;
TABLE vat.view_facts;
SELECT vat.integrity('after value_int TYPE bigint');

-- A value that only fits the new type, written through both views.
SET vat.step = '1b';
UPDATE vat.unit_stat__for_portion_of_valid SET value_int = 5000000000, valid_from = '2024-04-01', valid_until = '2024-06-01' WHERE id = 1;
SELECT id, name, value_int, pg_typeof(value_int) AS value_type, valid_from, valid_until FROM vat.unit_stat ORDER BY id, valid_from;
SET vat.today = '2024-08-01';
UPDATE vat.unit_loc__current_valid SET value_int = 5000000000 WHERE id = 1;
SELECT id, name, value_int, pg_typeof(value_int) AS value_type, valid_from, valid_until FROM vat.unit_loc WHERE id = 1 ORDER BY valid_from;
SELECT id, value_int FROM vat.unit_loc__current_valid ORDER BY id;
TABLE vat.update_log;
SELECT vat.integrity('after updates through the views with bigint');

\echo '--- 2a. ALTER COLUMN TYPE of the identifier: the views follow, their triggers still work'
-- The identifier is recorded by name only (unique_keys.column_names, the INSTEAD OF trigger arguments).
SET client_min_messages = notice;
ALTER TABLE vat.unit_stat ALTER COLUMN id TYPE bigint;
ALTER TABLE vat.unit_loc ALTER COLUMN id TYPE bigint;
SET client_min_messages = warning;
TABLE vat.view_shape;
TABLE vat.view_facts;
SELECT uk.table_name, uk.unique_key_name, uk.column_names,
       pg_get_constraintdef(con.oid) AS constraint_definition
FROM sql_saga.unique_keys AS uk
JOIN pg_catalog.pg_constraint AS con ON con.conname = uk.unique_constraint
     AND con.conrelid = format('%I.%I', uk.table_schema, uk.table_name)::regclass
WHERE uk.table_schema = 'vat' ORDER BY uk.table_name;
SELECT vat.integrity('after id TYPE bigint');

SET vat.step = '2a';
UPDATE vat.unit_stat__for_portion_of_valid SET name = 'big', valid_from = '2024-09-01', valid_until = 'infinity' WHERE id = 1;
SELECT id, pg_typeof(id) AS id_type, name, value_int, valid_from, valid_until FROM vat.unit_stat ORDER BY id, valid_from;
SET vat.today = '2024-09-01';
UPDATE vat.unit_loc__current_valid SET name = 'big' WHERE id = 1;
INSERT INTO vat.unit_loc__current_valid (id, name) VALUES (3000000000, 'huge');
SELECT id, pg_typeof(id) AS id_type, name, value_int, valid_from, valid_until FROM vat.unit_loc ORDER BY id, valid_from;
TABLE vat.update_log;
SELECT vat.integrity('after updates through the views with a bigint id');

\echo '--- 2b. ALTER COLUMN TYPE of the era range column or a boundary column: refused by sql_saga'
SELECT vat.remember_views();
SELECT vat.attempt($$ALTER TABLE vat.unit_stat ALTER COLUMN valid_range TYPE tsrange USING tsrange(lower(valid_range), upper(valid_range))$$) AS stat_range;
SELECT vat.attempt($$ALTER TABLE vat.unit_stat ALTER COLUMN valid_from TYPE timestamp$$) AS stat_valid_from;
SELECT vat.attempt($$ALTER TABLE vat.unit_loc ALTER COLUMN valid_range TYPE tsrange USING tsrange(lower(valid_range), upper(valid_range))$$) AS loc_range;
SELECT vat.attempt($$ALTER TABLE vat.unit_loc ALTER COLUMN valid_until TYPE timestamp$$) AS loc_valid_until;
-- The views refused for were not touched: the OIDs saved before the attempts.
TABLE vat.view_identity_check;
TABLE vat.view_shape;

-- A valid_to column is type-bound like the other boundary columns.
CREATE TABLE vat.with_to (id int NOT NULL, valid_range daterange NOT NULL, valid_from date, valid_until date, valid_to date);
SELECT sql_saga.add_era('vat.with_to', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until', valid_to_column_name => 'valid_to');
SELECT vat.attempt($$ALTER TABLE vat.with_to ALTER COLUMN valid_to TYPE timestamp$$) AS with_to_valid_to;

-- An era without boundary columns, with and without a view.
CREATE TABLE vat.bare (id int NOT NULL, name text, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('vat.bare', 'valid_range');
SELECT sql_saga.add_unique_key('vat.bare', ARRAY['id'], key_type => 'primary');
SELECT vat.attempt($$ALTER TABLE vat.bare ALTER COLUMN valid_range TYPE tsrange USING tsrange(lower(valid_range), upper(valid_range))$$) AS bare_range_without_view;
SELECT sql_saga.add_current_view('vat.bare', current_func_name => 'vat.test_now()');
SELECT vat.attempt($$ALTER TABLE vat.bare ALTER COLUMN valid_range TYPE tsrange USING tsrange(lower(valid_range), upper(valid_range))$$) AS bare_range_with_view;
SELECT e.table_name, e.range_type, e.range_subtype FROM sql_saga.era AS e WHERE e.table_schema = 'vat' ORDER BY e.table_name;
SELECT sql_saga.drop_current_view('vat.bare');
SELECT vat.integrity('after the refused era column type changes');

-- The HINT's migration, through public functions only, for a range-only era with no key, foreign key or view.
CREATE TABLE vat.migrated (id int NOT NULL, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('vat.migrated', 'valid_range');
INSERT INTO vat.migrated VALUES (1, '[2024-01-01,2024-06-01)');
SELECT vat.attempt($$ALTER TABLE vat.migrated ALTER COLUMN valid_range TYPE tsrange USING tsrange(lower(valid_range), upper(valid_range))$$) AS migrated_before_drop_era;
SELECT sql_saga.drop_era('vat.migrated', cleanup => false);
ALTER TABLE vat.migrated ALTER COLUMN valid_range TYPE tsrange USING tsrange(lower(valid_range), upper(valid_range));
SELECT sql_saga.add_era('vat.migrated', 'valid_range');
SELECT e.range_type, e.range_subtype FROM sql_saga.era AS e WHERE (e.table_schema, e.table_name) = ('vat', 'migrated');
SELECT id, valid_range FROM vat.migrated;
SELECT vat.attempt($$INSERT INTO vat.migrated VALUES (2, 'empty')$$) AS migrated_empty_range;
SELECT vat.integrity('after the era migration');

\echo '--- 2c. ALTER COLUMN TYPE of a column a sql_saga foreign key uses, on either side: refused, naming the key'
CREATE TABLE vat.fk_parent (id int NOT NULL, name text, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('vat.fk_parent', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('vat.fk_parent', ARRAY['id'], key_type => 'primary');
CREATE TABLE vat.fk_child_r (id int PRIMARY KEY, parent_id int);
SELECT sql_saga.add_regular_foreign_key('vat.fk_child_r', ARRAY['parent_id'], 'fk_parent_id_valid');
CREATE TABLE vat.fk_child_t (id int NOT NULL, parent_id int, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('vat.fk_child_t', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_temporal_foreign_key('vat.fk_child_t', ARRAY['parent_id'], 'valid', 'fk_parent_id_valid');
INSERT INTO vat.fk_parent (id, name, valid_from, valid_until) VALUES (1, 'p', '2024-01-01', 'infinity');
INSERT INTO vat.fk_child_r VALUES (1, 1);
INSERT INTO vat.fk_child_t (id, parent_id, valid_from, valid_until) VALUES (1, 1, '2024-02-01', 'infinity');
-- What a refusal must leave unchanged (independent of how a regular foreign key is implemented).
CREATE VIEW vat.fk_state AS
SELECT fk.foreign_key_name, fk.type, fk.table_name, fk.column_names,
       (SELECT array_agg(format('%s.%s %s', c.relname, a.attname, format_type(a.atttypid, a.atttypmod)) ORDER BY c.relname, a.attname)
        FROM pg_catalog.pg_attribute AS a JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid
        WHERE c.relnamespace = 'vat'::regnamespace AND c.relname IN ('fk_parent', 'fk_child_r', 'fk_child_t')
          AND a.attname IN ('id', 'parent_id')) AS key_column_types
FROM sql_saga.foreign_keys AS fk WHERE fk.table_schema = 'vat' ORDER BY fk.foreign_key_name;
-- Whether a statement is refused by an integrity constraint (class 23), without its implementation-specific text.
CREATE FUNCTION vat.enforced(statement text) RETURNS text LANGUAGE plpgsql AS $enforced$
BEGIN
    BEGIN
        EXECUTE statement;
        RAISE EXCEPTION USING ERRCODE = 'P0004', MESSAGE = 'accepted';
    EXCEPTION
        WHEN assert_failure THEN RETURN 'accepted';
        WHEN integrity_constraint_violation OR check_violation OR foreign_key_violation THEN RETURN 'refused by the foreign key';
    END;
END;
$enforced$;
TABLE vat.fk_state;
SELECT vat.attempt($$ALTER TABLE vat.fk_parent ALTER COLUMN id TYPE bigint$$) AS referenced_key_column;
SELECT vat.attempt($$ALTER TABLE vat.fk_child_r ALTER COLUMN parent_id TYPE bigint$$) AS regular_referencing_column;
SELECT vat.attempt($$ALTER TABLE vat.fk_child_t ALTER COLUMN parent_id TYPE bigint$$) AS temporal_referencing_column;
TABLE vat.fk_state;
-- Both keys still enforce.
SELECT vat.enforced($$INSERT INTO vat.fk_child_r VALUES (2, 99)$$) AS regular_fk_enforced;
SELECT vat.enforced($$INSERT INTO vat.fk_child_t (id, parent_id, valid_from, valid_until) VALUES (2, 99, '2024-02-01', 'infinity')$$) AS temporal_fk_enforced;
-- A column no foreign key uses still changes type.
SELECT vat.attempt($$ALTER TABLE vat.fk_parent ALTER COLUMN name TYPE varchar(10)$$) AS unrelated_parent_column;
SELECT vat.integrity('after the refused foreign key column type changes');
-- The HINT's way: drop the foreign keys, change both sides, add them again.
SELECT sql_saga.drop_foreign_key_by_name('vat.fk_child_r', 'fk_child_r_parent_id_fkey');
SELECT sql_saga.drop_foreign_key_by_name('vat.fk_child_t', 'fk_child_t_parent_id_valid');
ALTER TABLE vat.fk_parent ALTER COLUMN id TYPE bigint;
ALTER TABLE vat.fk_child_r ALTER COLUMN parent_id TYPE bigint;
ALTER TABLE vat.fk_child_t ALTER COLUMN parent_id TYPE bigint;
SELECT sql_saga.add_regular_foreign_key('vat.fk_child_r', ARRAY['parent_id'], 'fk_parent_id_valid');
SELECT sql_saga.add_temporal_foreign_key('vat.fk_child_t', ARRAY['parent_id'], 'valid', 'fk_parent_id_valid');
TABLE vat.fk_state;
INSERT INTO vat.fk_parent (id, name, valid_from, valid_until) VALUES (3000000000, 'big', '2024-01-01', 'infinity');
INSERT INTO vat.fk_child_r VALUES (3, 3000000000);
INSERT INTO vat.fk_child_t (id, parent_id, valid_from, valid_until) VALUES (3, 3000000000, '2024-02-01', 'infinity');
SELECT vat.enforced($$INSERT INTO vat.fk_child_r VALUES (4, 3000000001)$$) AS regular_fk_enforced_bigint;
SELECT vat.enforced($$INSERT INTO vat.fk_child_t (id, parent_id, valid_from, valid_until) VALUES (4, 3000000001, '2024-02-01', 'infinity')$$) AS temporal_fk_enforced_bigint;
SELECT vat.integrity('after the foreign key migration');

\echo '--- 3. A user view on the managed view: refused before any change, naming the user view'
CREATE VIEW vat.stat_names AS SELECT id, name FROM vat.unit_stat__for_portion_of_valid;
CREATE VIEW vat.loc_names AS SELECT id, name FROM vat.unit_loc__current_valid;
SELECT vat.remember_views();
SELECT vat.attempt($$ALTER TABLE vat.unit_stat ALTER COLUMN name TYPE varchar(30)$$) AS stat_under_user_view;
SELECT vat.attempt($$ALTER TABLE vat.unit_loc ALTER COLUMN name TYPE varchar(30)$$) AS loc_under_user_view;
-- Also when the user view does not use the changed column: the rebuild would still drop it.
SELECT vat.attempt($$ALTER TABLE vat.unit_stat ALTER COLUMN value_int TYPE numeric$$) AS stat_other_column_under_user_view;
-- Nothing changed: same view OIDs, old types, the user views still there.
TABLE vat.view_identity_check;
TABLE vat.view_shape;
TABLE vat.view_facts;
SELECT c.relname AS user_view, pg_get_viewdef(c.oid) AS definition
FROM pg_catalog.pg_class AS c WHERE c.oid IN ('vat.stat_names'::regclass, 'vat.loc_names'::regclass) ORDER BY c.relname;
SELECT count(*) AS pending_rows FROM sql_saga.__internal_updatable_view_pending;
SELECT vat.integrity('after the refusal under user views');
-- A user rule on a managed view is a user object too.
DROP VIEW vat.loc_names;
CREATE TABLE vat.rule_log (event text);
CREATE RULE log_delete AS ON DELETE TO vat.unit_loc__current_valid DO ALSO INSERT INTO vat.rule_log VALUES ('deleted');
SELECT vat.attempt($$ALTER TABLE vat.unit_loc ALTER COLUMN name TYPE varchar(30)$$) AS loc_under_user_rule;
TABLE vat.view_identity_check;
SELECT r.rulename FROM pg_catalog.pg_rewrite AS r
WHERE r.ev_class = 'vat.unit_loc__current_valid'::regclass ORDER BY r.rulename;
DROP RULE log_delete ON vat.unit_loc__current_valid;
DROP TABLE vat.rule_log;
-- The HINT's way: drop the user view, ALTER, recreate it.
DROP VIEW vat.stat_names;
SET client_min_messages = notice;
ALTER TABLE vat.unit_stat ALTER COLUMN name TYPE varchar(30);
SET client_min_messages = warning;
CREATE VIEW vat.stat_names AS SELECT id, name FROM vat.unit_stat__for_portion_of_valid;
SELECT id, name, pg_typeof(name) AS name_type FROM vat.stat_names ORDER BY id, name;
DROP VIEW vat.stat_names;
SELECT vat.integrity('after following the HINT');

\echo '--- 4. A user view directly on the base column: PostgreSQL refuses, sql_saga does not interfere'
CREATE VIEW vat.stat_direct AS SELECT id, name FROM vat.unit_stat;
SELECT vat.remember_views();
-- PostgreSQL's own error (SQLSTATE 0A000, its message). Its DETAIL names the first dependent view it
-- finds, which is not sql_saga's to decide, so only the first line is shown.
SELECT split_part(vat.attempt($$ALTER TABLE vat.unit_stat ALTER COLUMN name TYPE varchar(40)$$), E'\n', 1) AS stat_under_direct_user_view;
TABLE vat.view_identity_check;
TABLE vat.view_shape;
SELECT count(*) AS pending_rows FROM sql_saga.__internal_updatable_view_pending;
DROP VIEW vat.stat_direct;
SELECT vat.integrity('after PostgreSQL''s refusal');

\echo '--- 5a. ALTER COLUMN TYPE combined with ADD COLUMN and with DROP COLUMN in one statement'
ALTER TABLE vat.unit_stat ADD COLUMN gone int;
ALTER TABLE vat.unit_loc ADD COLUMN gone int;
SET client_min_messages = notice;
ALTER TABLE vat.unit_stat ALTER COLUMN name TYPE varchar(50), ADD COLUMN note text;
ALTER TABLE vat.unit_loc ALTER COLUMN name TYPE varchar(50), DROP COLUMN gone;
ALTER TABLE vat.unit_stat DROP COLUMN gone, ALTER COLUMN note TYPE varchar(10), ADD COLUMN extra int;
SET client_min_messages = warning;
TABLE vat.view_shape;
TABLE vat.view_facts;
SET vat.step = '5a';
UPDATE vat.unit_stat__for_portion_of_valid SET note = 'n', extra = 1, valid_from = '2024-10-01', valid_until = 'infinity' WHERE id = 1;
SELECT id, name, note, extra, valid_from, valid_until FROM vat.unit_stat WHERE valid_from >= '2024-09-01' ORDER BY id, valid_from;
SET vat.today = '2024-10-01';
UPDATE vat.unit_loc__current_valid SET name = repeat('y', 50) WHERE id = 1;
SELECT id, length(name) AS name_length, valid_from, valid_until FROM vat.unit_loc WHERE id = 1 ORDER BY valid_from;
SELECT count(*) AS pending_rows FROM sql_saga.__internal_updatable_view_pending;
SELECT vat.integrity('after combined statements');

\echo '--- 5b. Inside a rolled-back transaction: nothing is left'
SELECT vat.remember_views();
BEGIN;
ALTER TABLE vat.unit_stat ALTER COLUMN name TYPE text;
ALTER TABLE vat.unit_loc ALTER COLUMN name TYPE text, ADD COLUMN temp_col int;
TABLE vat.view_shape;
SELECT vat.integrity('inside the transaction');
ROLLBACK;
TABLE vat.view_identity_check;
TABLE vat.view_shape;
TABLE vat.view_facts;
SELECT count(*) AS pending_rows FROM sql_saga.__internal_updatable_view_pending;
-- A type change that fails on the data: nothing is left either.
SELECT vat.attempt($$ALTER TABLE vat.unit_stat ALTER COLUMN name TYPE int USING 1/0$$) AS failing_type_change;
TABLE vat.view_identity_check;
SELECT count(*) AS pending_rows FROM sql_saga.__internal_updatable_view_pending;
-- The restored views still work, with the restored types (varchar(50) refuses 51 characters).
SET vat.step = '5b';
UPDATE vat.unit_stat__for_portion_of_valid SET note = 'r', valid_from = '2024-11-01', valid_until = 'infinity' WHERE id = 1;
SELECT id, name, note, valid_from, valid_until FROM vat.unit_stat WHERE valid_from >= '2024-10-01' ORDER BY id, valid_from;
SET vat.today = '2024-11-01';
SELECT vat.attempt($$UPDATE vat.unit_loc__current_valid SET name = repeat('z', 51) WHERE id = 1$$) AS too_long_after_rollback;
UPDATE vat.unit_loc__current_valid SET name = 'after rollback' WHERE id = 1;
SELECT id, name, valid_from, valid_until FROM vat.unit_loc__current_valid ORDER BY id;
SELECT view_name, at_step FROM vat.update_log WHERE at_step = '5b' ORDER BY view_name;
SELECT vat.integrity('after the rollback');

\echo '--- 6. An ordinary owner, quoted identifiers and an ephemeral payload column'
-- The owner has no EXECUTE on sql_saga's internal functions and new functions are not executable by PUBLIC
-- (as in 126 section 9). Schema, table and payload column need quoting; edit_by is ephemeral.
CREATE ROLE vat_owner NOSUPERUSER;
CREATE SCHEMA "Odd ""Sch" AUTHORIZATION vat_owner;
GRANT USAGE ON SCHEMA sql_saga TO vat_owner;
ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE vat_owner REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
SET ROLE vat_owner;
CREATE TABLE "Odd ""Sch"."Ta'ble" (id int NOT NULL, "Pay ""load""" int, edit_by int, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('"Odd ""Sch"."Ta''ble"', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('"Odd ""Sch"."Ta''ble"', ARRAY['id'], key_type => 'primary');
SELECT sql_saga.add_for_portion_of_view('"Odd ""Sch"."Ta''ble"', ephemeral_columns => ARRAY['edit_by']);
INSERT INTO "Odd ""Sch"."Ta'ble" (id, "Pay ""load""", edit_by, valid_from, valid_until) VALUES (1, 1, 10, '2024-01-01', 'infinity');
-- Warm the view's DML path.
UPDATE "Odd ""Sch"."Ta'ble__for_portion_of_valid" SET "Pay ""load""" = 2, edit_by = 11, valid_from = '2024-03-01', valid_until = '2024-05-01' WHERE id = 1;
SET client_min_messages = notice;
ALTER TABLE "Odd ""Sch"."Ta'ble" ALTER COLUMN "Pay ""load""" TYPE bigint, ALTER COLUMN edit_by TYPE bigint;
SET client_min_messages = warning;
UPDATE "Odd ""Sch"."Ta'ble__for_portion_of_valid" SET "Pay ""load""" = 5000000000, edit_by = 5000000001, valid_from = '2024-07-01', valid_until = '2024-08-01' WHERE id = 1;
-- Only the ephemeral column differs from the neighbour: the segments coalesce, keeping the payload.
UPDATE "Odd ""Sch"."Ta'ble__for_portion_of_valid" SET "Pay ""load""" = 5000000000, edit_by = 12, valid_from = '2024-08-01', valid_until = '2024-09-01' WHERE id = 1;
RESET ROLE;
SELECT id, "Pay ""load""", edit_by, valid_from, valid_until FROM "Odd ""Sch"."Ta'ble" ORDER BY valid_from;
SELECT uv.view_name, uv.ephemeral_columns, c.relowner::regrole AS view_owner,
       (SELECT array_agg(format('%s %s', a.attname, format_type(a.atttypid, a.atttypmod)) ORDER BY a.attnum)
        FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped) AS view_columns
FROM sql_saga.updatable_view AS uv
JOIN pg_catalog.pg_class AS c ON c.oid = format('%I.%I', uv.view_schema, uv.view_name)::regclass
WHERE uv.view_schema = 'Odd "Sch';
ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO PUBLIC;
SELECT count(*) AS pending_rows FROM sql_saga.__internal_updatable_view_pending;
SELECT vat.integrity('after the owner''s type change');

\echo '--- 7. Inheritance: a type change on a parent recurses to a child with a managed view'
CREATE TABLE vat.inh_parent (id int NOT NULL, payload int, valid_range daterange NOT NULL, valid_from date, valid_until date);
CREATE TABLE vat.inh_child () INHERITS (vat.inh_parent);
SELECT sql_saga.add_era('vat.inh_child', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('vat.inh_child', ARRAY['id'], key_type => 'primary');
SELECT sql_saga.add_for_portion_of_view('vat.inh_child');
INSERT INTO vat.inh_child (id, payload, valid_from, valid_until) VALUES (1, 1, '2024-01-01', 'infinity');
SET client_min_messages = notice;
ALTER TABLE vat.inh_parent ALTER COLUMN payload TYPE bigint;
SET client_min_messages = warning;
SELECT c.relname, format_type(a.atttypid, a.atttypmod) AS payload_type
FROM pg_catalog.pg_attribute AS a JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid
WHERE c.oid IN ('vat.inh_parent'::regclass, 'vat.inh_child'::regclass, 'vat.inh_child__for_portion_of_valid'::regclass)
  AND a.attname = 'payload' ORDER BY c.relname;
UPDATE vat.inh_child__for_portion_of_valid SET payload = 5000000000, valid_from = '2024-03-01', valid_until = '2024-04-01' WHERE id = 1;
SELECT id, payload, valid_from, valid_until FROM vat.inh_child ORDER BY valid_from;
-- The child's era range column, reached through the parent: refused like on the child itself.
SELECT vat.attempt($$ALTER TABLE vat.inh_parent ALTER COLUMN valid_range TYPE tsrange USING tsrange(lower(valid_range), upper(valid_range))$$) AS parent_range_reaching_child_era;
SELECT c.relname, format_type(a.atttypid, a.atttypmod) AS range_type
FROM pg_catalog.pg_attribute AS a JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid
WHERE c.oid IN ('vat.inh_parent'::regclass, 'vat.inh_child'::regclass) AND a.attname = 'valid_range' ORDER BY c.relname;
SELECT vat.integrity('at the end');

\echo '--- 8. A type change whose ddl_command_end did not run leaves a pending view: check_integrity reports it'
-- Disabling only sql_saga_rename_following lets the start hook drop the view without the end hook recreating it.
-- The diagnostic names a column change in general (not only DROP COLUMN) and gives the statements that repair it.
-- Rolled back: nothing pending or orphaned is left.
BEGIN;
CREATE TABLE vat.pending_type (id integer NOT NULL, payload integer, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('vat.pending_type', 'valid_range') \g /dev/null
SELECT sql_saga.add_unique_key('vat.pending_type', ARRAY['id'], key_type => 'primary') \g /dev/null
SELECT sql_saga.add_for_portion_of_view('vat.pending_type') \g /dev/null
ALTER EVENT TRIGGER sql_saga_rename_following DISABLE;
ALTER TABLE vat.pending_type ALTER COLUMN payload TYPE bigint;
ALTER EVENT TRIGGER sql_saga_rename_following ENABLE;
SELECT to_regclass('vat.pending_type__for_portion_of_valid') AS pending_type_view,
       (SELECT count(*) FROM sql_saga.__internal_updatable_view_pending) AS pending_rows;
SELECT vat.integrity('with the pending TYPE view left behind');
ROLLBACK;
SELECT to_regclass('vat.pending_type') AS pending_type_table,
       (SELECT count(*) FROM sql_saga.__internal_updatable_view_pending) AS pending_rows;
SELECT vat.integrity('after ROLLBACK of the pending TYPE view');

SET client_min_messages = warning;
ALTER EXTENSION btree_gist DROP VIEW vat.unit_stat__for_portion_of_valid;
RESET search_path;
DROP SCHEMA vat CASCADE;
DROP SCHEMA "Odd ""Sch" CASCADE;
DROP OWNED BY vat_owner;
DROP ROLE vat_owner;
DROP ROLE vat_reader;
RESET vat.today;
RESET vat.step;
\i sql/include/test_teardown.sql
RESET client_min_messages;
