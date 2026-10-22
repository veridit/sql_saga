\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/*
 * Ownership of the tables sql_saga's public functions change (1.4, review finding F3).
 *
 * sql_saga's API functions are SECURITY DEFINER: they add and drop constraints, triggers, indexes, views and catalog
 * rows on user tables as the extension owner. Up to 1.3 most of them did not check who called them, so a role with
 * no more than SELECT on a table could change another role's table this way, and with a CASCADE foreign key delete
 * its rows. PostgreSQL itself requires ownership for ALTER TABLE, CREATE TRIGGER and ADD CONSTRAINT.
 *
 * From 1.4 every such function first requires the calling role to own (be a member of the owner role of) each table
 * it changes, with SQLSTATE 42501 "must be owner of table ...". A foreign key also needs REFERENCES on the referenced
 * table, as a native foreign key does.
 *
 * Part 1  A role with only SELECT on the tables is refused by every function, and nothing changes.
 * Part 2  A member of the owner role passes the check (the owner itself is covered by every other test).
 * Part 3  A foreign key from an owned child to a parent without REFERENCES is refused; with REFERENCES it works.
 */

BEGIN;
CREATE ROLE ow_owner;
CREATE ROLE ow_other;
CREATE ROLE ow_member IN ROLE ow_owner;
CREATE SCHEMA ow AUTHORIZATION ow_owner;
GRANT USAGE ON SCHEMA ow TO ow_other, ow_member;
GRANT USAGE ON SCHEMA sql_saga TO ow_owner, ow_other, ow_member;

CREATE FUNCTION ow.outcome(statement text) RETURNS text LANGUAGE plpgsql AS $outcome$
BEGIN
    BEGIN
        EXECUTE statement;
        RETURN 'ok';
    EXCEPTION WHEN OTHERS THEN
        RETURN SQLSTATE || ': ' || SQLERRM;
    END;
END;
$outcome$;
GRANT EXECUTE ON FUNCTION ow.outcome(text) TO PUBLIC;

-- A fingerprint of everything sql_saga could change on ow_owner's tables: catalog rows, triggers, constraints, indexes.
CREATE FUNCTION ow.state() RETURNS text LANGUAGE sql AS $state$
    SELECT md5(concat_ws('|',
        (SELECT string_agg(e.era_name, ',' ORDER BY e.era_name) FROM sql_saga.era AS e WHERE e.table_schema = 'ow'),
        (SELECT string_agg(u.unique_key_name, ',' ORDER BY u.unique_key_name) FROM sql_saga.unique_keys AS u WHERE u.table_schema = 'ow'),
        (SELECT string_agg(f.foreign_key_name, ',' ORDER BY f.foreign_key_name) FROM sql_saga.foreign_keys AS f WHERE f.table_schema = 'ow'),
        (SELECT string_agg(v.view_name, ',' ORDER BY v.view_name) FROM sql_saga.updatable_view AS v WHERE v.table_schema = 'ow'),
        (SELECT string_agg(s.table_name, ',' ORDER BY s.table_name) FROM sql_saga.system_versioning AS s WHERE s.table_schema = 'ow'),
        (SELECT string_agg(t.tgname, ',' ORDER BY t.tgname) FROM pg_trigger AS t JOIN pg_class AS c ON c.oid = t.tgrelid
          WHERE c.relnamespace = 'ow'::regnamespace AND NOT t.tgisinternal),
        (SELECT string_agg(k.conname, ',' ORDER BY k.conname) FROM pg_constraint AS k WHERE k.connamespace = 'ow'::regnamespace),
        (SELECT string_agg(c.relname, ',' ORDER BY c.relname) FROM pg_class AS c WHERE c.relnamespace = 'ow'::regnamespace)));
$state$;

SET LOCAL ROLE ow_owner;
-- parent: a temporal table with an era, a primary key, a for_portion_of view, a current view and synchronized columns.
CREATE TABLE ow.parent (id int NOT NULL, name text, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('ow.parent', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('ow.parent', ARRAY['id'], key_type => 'primary', unique_key_name => 'ow_parent_pk');
SELECT sql_saga.add_for_portion_of_view('ow.parent');
INSERT INTO ow.parent (id, name, valid_from, valid_until) VALUES (1, 'a', '2020-01-01', 'infinity');
-- child: a regular table with a regular foreign key to parent.
CREATE TABLE ow.child (id int PRIMARY KEY, parent_id int);
SELECT sql_saga.add_foreign_key('ow.child', ARRAY['parent_id'], 'ow.parent', ARRAY['id'], foreign_key_name => 'ow_child_fk');
-- bare: a table with nothing on it yet.
CREATE TABLE ow.bare (id int NOT NULL, valid_range daterange NOT NULL, valid_from date, valid_until date);
-- sv: a system-versioned table.
CREATE TABLE ow.sv (id int PRIMARY KEY, x text);
SELECT sql_saga.add_system_versioning('ow.sv');
-- other_child: a child of ow_other's own, used for the REFERENCES check.
RESET ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA ow TO ow_other;
CREATE SCHEMA ow_o AUTHORIZATION ow_other;
SET LOCAL ROLE ow_other;
CREATE TABLE ow_o.child (id int PRIMARY KEY, parent_id int);
RESET ROLE;
CREATE TEMP TABLE before AS SELECT ow.state() AS s;

\o
\echo '--- part 1: a role with only SELECT is refused by every function, and nothing changes'
SET LOCAL ROLE ow_other;
SELECT 'add_era | ' || ow.outcome($$SELECT sql_saga.add_era('ow.bare', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until')$$);
SELECT 'drop_era | ' || ow.outcome($$SELECT sql_saga.drop_era('ow.parent', 'valid', 'CASCADE')$$);
SELECT 'set_era_ephemeral_columns | ' || ow.outcome($$SELECT sql_saga.set_era_ephemeral_columns('ow.parent', 'valid', ARRAY['name'])$$);
SELECT 'add_unique_key | ' || ow.outcome($$SELECT sql_saga.add_unique_key('ow.parent', ARRAY['name'], key_type => 'natural')$$);
SELECT 'drop_unique_key | ' || ow.outcome($$SELECT sql_saga.drop_unique_key('ow.parent', ARRAY['id'], drop_behavior => 'CASCADE')$$);
SELECT 'drop_unique_key_by_name | ' || ow.outcome($$SELECT sql_saga.drop_unique_key_by_name('ow.parent', 'ow_parent_pk', 'CASCADE')$$);
SELECT 'add_foreign_key onto a table it does not own | ' || ow.outcome($$SELECT sql_saga.add_foreign_key('ow.child', ARRAY['id'], 'ow.parent', ARRAY['id'])$$);
SELECT 'add_regular_foreign_key | ' || ow.outcome($$SELECT sql_saga.add_regular_foreign_key('ow.child', ARRAY['id'], 'ow_parent_pk')$$);
SELECT 'drop_foreign_key | ' || ow.outcome($$SELECT sql_saga.drop_foreign_key('ow.child', ARRAY['parent_id'])$$);
SELECT 'drop_foreign_key_by_name | ' || ow.outcome($$SELECT sql_saga.drop_foreign_key_by_name('ow.child', 'ow_child_fk')$$);
SELECT 'add_for_portion_of_view | ' || ow.outcome($$SELECT sql_saga.add_for_portion_of_view('ow.parent')$$);
SELECT 'drop_for_portion_of_view | ' || ow.outcome($$SELECT sql_saga.drop_for_portion_of_view('ow.parent')$$);
SELECT 'add_current_view | ' || ow.outcome($$SELECT sql_saga.add_current_view('ow.parent')$$);
SELECT 'drop_current_view | ' || ow.outcome($$SELECT sql_saga.drop_current_view('ow.parent')$$);
SELECT 'add_synchronize_temporal_columns_trigger | ' || ow.outcome($$CALL sql_saga.add_synchronize_temporal_columns_trigger('ow.parent', 'valid')$$);
SELECT 'drop_synchronize_temporal_columns_trigger | ' || ow.outcome($$CALL sql_saga.drop_synchronize_temporal_columns_trigger('ow.parent', 'valid')$$);
SELECT 'add_system_versioning | ' || ow.outcome($$SELECT sql_saga.add_system_versioning('ow.child')$$);
SELECT 'drop_system_versioning | ' || ow.outcome($$SELECT sql_saga.drop_system_versioning('ow.sv')$$);
SELECT 'drop_system_time_era | ' || ow.outcome($$SELECT sql_saga.drop_system_time_era('ow.sv')$$);
SELECT 'set_system_time_era_excluded_columns | ' || ow.outcome($$SELECT sql_saga.set_system_time_era_excluded_columns('ow.sv', ARRAY['x'])$$);
SELECT 'rebuild_system_versioning_views | ' || ow.outcome($$SELECT sql_saga.rebuild_system_versioning_views('ow.sv')$$);
RESET ROLE;
SELECT 'nothing changed on ow_owner''s tables | ' || (ow.state() = (SELECT s FROM before))::text;

\echo '--- part 2: a member of the owner role passes the ownership check'
SAVEPOINT m;
SET LOCAL ROLE ow_member;
SELECT 'add_era as a member of the owner role | ' || ow.outcome($$SELECT sql_saga.add_era('ow.bare', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until')$$);
SELECT 'set_era_ephemeral_columns as a member | ' || ow.outcome($$SELECT sql_saga.set_era_ephemeral_columns('ow.parent', 'valid', ARRAY['name'])$$);
RESET ROLE;
ROLLBACK TO SAVEPOINT m;

\echo '--- part 3: a foreign key needs REFERENCES on the referenced table'
SET LOCAL ROLE ow_other;
SELECT 'own child, parent without REFERENCES | ' || ow.outcome($$SELECT sql_saga.add_foreign_key('ow_o.child', ARRAY['parent_id'], 'ow.parent', ARRAY['id'])$$);
RESET ROLE;
GRANT REFERENCES ON ow.parent TO ow_other;
SET LOCAL ROLE ow_other;
SELECT 'own child, parent with REFERENCES | ' || ow.outcome($$SELECT sql_saga.add_foreign_key('ow_o.child', ARRAY['parent_id'], 'ow.parent', ARRAY['id'])$$);
RESET ROLE;

\echo '--- part 4: as native DROP ... CASCADE, the parent owner removes a dependent foreign key owned by another role'
SET LOCAL ROLE ow_owner;
SELECT 'drop the referencing key directly, not owning it | ' || ow.outcome($$SELECT sql_saga.drop_foreign_key_by_name(NULL, 'child_parent_id_fkey')$$);
SELECT 'drop_era CASCADE (views, keys, dependent foreign keys) | ' || ow.outcome($$SELECT sql_saga.drop_era('ow.parent', 'valid', 'CASCADE')$$);
RESET ROLE;
SELECT 'foreign keys left | ' || coalesce(string_agg(fk.foreign_key_name, ', ' ORDER BY fk.foreign_key_name), '(none)') FROM sql_saga.foreign_keys AS fk;
SELECT 'regular foreign key triggers left on ow_o.child | ' || count(*) FROM pg_trigger WHERE tgrelid = 'ow_o.child'::regclass AND NOT tgisinternal;

ROLLBACK;
\i sql/include/test_teardown.sql
