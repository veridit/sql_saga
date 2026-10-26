\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/*
 * The history table of system versioning is authorized against its own owner (1.4, review round 3, Q1).
 *
 * add_system_versioning stands for native DDL: CREATE TABLE <history> (LIKE <base>), CREATE VIEW and CREATE FUNCTION
 * in the base table's schema, or, when the history table exists, ALTER TABLE <history> OWNER TO <base owner>. Up to
 * the round-3 fix it checked only the base table: a role that owned any table of a schema could name another role's
 * table there as its history, and sql_saga gave the table to the attacker, enabled RLS on it and granted SELECT to
 * PUBLIC. It also created the history, the views and the functions in a schema where the caller may not create.
 *
 * Native rules, applied to the role that called sql_saga:
 *   CREATE TABLE/VIEW/FUNCTION needs CREATE on the schema;
 *   ALTER TABLE ... OWNER TO needs ownership of the table, the right to SET ROLE to the new owner, and CREATE on the
 *   schema for the new owner (none of this when the owner does not change);
 *   DROP TABLE needs ownership of the table.
 *
 * Part 1  Adopting another role's table is refused; nothing changes on it.
 * Part 2  Adopting one's own table, and a table of a role one inherits, works.
 * Part 3  Without CREATE on the schema, add_system_versioning, add_for_portion_of_view and add_current_view are
 *         refused, as the native CREATE statements are.
 * Part 4  A history table that another role owns (diverged with the event triggers off) is not dropped by
 *         drop_system_versioning and not re-owned by rebuild_system_versioning_views.
 */

CREATE ROLE hsv_victim;
CREATE ROLE hsv_evil;
CREATE ROLE hsv_group;
CREATE ROLE hsv_member IN ROLE hsv_group;
CREATE ROLE hsv_nocreate;
CREATE SCHEMA hsv;
GRANT USAGE, CREATE ON SCHEMA hsv TO hsv_victim, hsv_evil, hsv_group, hsv_member;
GRANT USAGE ON SCHEMA hsv TO hsv_nocreate;
GRANT USAGE ON SCHEMA sql_saga TO hsv_victim, hsv_evil, hsv_member, hsv_nocreate;

SET ROLE hsv_victim;
CREATE TABLE hsv.victim (id int, secret text, system_valid_range tstzrange);
INSERT INTO hsv.victim VALUES (1, 'top secret', tstzrange(now(), now()));
RESET ROLE;

CREATE FUNCTION hsv.try(r text, stmt text) RETURNS text LANGUAGE plpgsql AS $try$
DECLARE
    res text;
BEGIN
    BEGIN
        EXECUTE format('SET LOCAL ROLE %I', r);
        EXECUTE stmt;
        res := 'allowed';
        RAISE EXCEPTION USING ERRCODE = 'P0099';  -- always rolled back: each probe stands alone
    EXCEPTION
        WHEN SQLSTATE 'P0099' THEN NULL;
        WHEN OTHERS THEN res := 'refused ' || SQLSTATE || ': ' || SQLERRM;
    END;
    RESET ROLE;
    RETURN res;
END;
$try$;
GRANT EXECUTE ON FUNCTION hsv.try(text, text) TO PUBLIC;

\o
\echo '--- part 1: adopting another role''s table is refused'
SET ROLE hsv_evil;
CREATE TABLE hsv.mine (id int, secret text);
RESET ROLE;
SELECT hsv.try('hsv_evil', $$SELECT sql_saga.add_system_versioning('hsv.mine', history_table_name => 'victim')$$);
SELECT 'victim owner: ' || relowner::regrole || ', rls: ' || relrowsecurity || ', acl: ' || coalesce(relacl::text, 'default')
FROM pg_class WHERE oid = 'hsv.victim'::regclass;
SELECT 'registered as history: ' || count(*) FROM sql_saga.system_versioning WHERE history_table_name = 'victim';
SELECT 'mine has system versioning: ' || count(*) FROM sql_saga.system_versioning WHERE table_name = 'mine';
-- Native: the same takeover is refused.
SELECT 'native: ' || hsv.try('hsv_evil', $$ALTER TABLE hsv.victim OWNER TO hsv_evil$$);

\echo '--- part 2: adopting one''s own table, and a table of an inherited role, works'
SET ROLE hsv_evil;
CREATE TABLE hsv.mine_history (id int, secret text, system_valid_range tstzrange);
SELECT sql_saga.add_system_versioning('hsv.mine', history_table_must_exist => true);
RESET ROLE;
SELECT 'mine_history owner: ' || relowner::regrole FROM pg_class WHERE oid = 'hsv.mine_history'::regclass;
SET ROLE hsv_group;
CREATE TABLE hsv.grp_history (id int, system_valid_range tstzrange);
RESET ROLE;
SET ROLE hsv_member;
CREATE TABLE hsv.grp (id int);
SELECT sql_saga.add_system_versioning('hsv.grp', history_table_must_exist => true);
RESET ROLE;
SELECT 'grp_history owner: ' || relowner::regrole FROM pg_class WHERE oid = 'hsv.grp_history'::regclass;
SELECT 'native: ' || hsv.try('hsv_member', $$ALTER TABLE hsv.mine_history OWNER TO hsv_member$$);

\echo '--- part 3: no CREATE on the schema'
SET ROLE hsv_victim;
CREATE TABLE hsv.nc (id int NOT NULL, valid_range daterange NOT NULL, valid_from date, valid_until date);
RESET ROLE;
ALTER TABLE hsv.nc OWNER TO hsv_nocreate;
SELECT 'add_system_versioning: ' || hsv.try('hsv_nocreate', $$SELECT sql_saga.add_system_versioning('hsv.nc')$$);
SELECT 'native CREATE TABLE: ' || hsv.try('hsv_nocreate', $$CREATE TABLE hsv.nc_history (LIKE hsv.nc)$$);
SELECT sql_saga.add_era('hsv.nc', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('hsv.nc', ARRAY['id'], key_type => 'primary', unique_key_name => 'nc_pk');
SELECT 'add_for_portion_of_view: ' || hsv.try('hsv_nocreate', $$SELECT sql_saga.add_for_portion_of_view('hsv.nc')$$);
SELECT 'add_current_view: ' || hsv.try('hsv_nocreate', $$SELECT sql_saga.add_current_view('hsv.nc')$$);
SELECT 'native CREATE VIEW: ' || hsv.try('hsv_nocreate', $$CREATE VIEW hsv.nc_v AS TABLE hsv.nc$$);
GRANT CREATE ON SCHEMA hsv TO hsv_nocreate;
SELECT 'with CREATE, add_system_versioning: ' || hsv.try('hsv_nocreate', $$SELECT sql_saga.add_system_versioning('hsv.nc')$$);
SELECT 'with CREATE, add_for_portion_of_view: ' || hsv.try('hsv_nocreate', $$SELECT sql_saga.add_for_portion_of_view('hsv.nc')$$);
SELECT 'with CREATE, add_current_view: ' || hsv.try('hsv_nocreate', $$SELECT sql_saga.add_current_view('hsv.nc')$$);
REVOKE CREATE ON SCHEMA hsv FROM hsv_nocreate;

\echo '--- part 4: a history table another role owns is neither dropped nor re-owned'
\o /dev/null
ALTER EVENT TRIGGER sql_saga_health_checks DISABLE;
ALTER TABLE hsv.mine_history OWNER TO hsv_victim;
ALTER EVENT TRIGGER sql_saga_health_checks ENABLE;
\o
-- The rebuild recreates the views and functions only; the history keeps its owner.
SELECT 'rebuild_system_versioning_views: ' || hsv.try('hsv_evil', $$SELECT sql_saga.rebuild_system_versioning_views('hsv.mine')$$);
SELECT 'drop_system_versioning: ' || hsv.try('hsv_evil', $$SELECT sql_saga.drop_system_versioning('hsv.mine')$$);
SELECT 'native DROP TABLE: ' || hsv.try('hsv_evil', $$DROP TABLE hsv.mine_history$$);
SELECT 'mine_history owner: ' || relowner::regrole FROM pg_class WHERE oid = 'hsv.mine_history'::regclass;
\o /dev/null
ALTER EVENT TRIGGER sql_saga_health_checks DISABLE;
ALTER TABLE hsv.mine_history OWNER TO hsv_evil;
ALTER EVENT TRIGGER sql_saga_health_checks ENABLE;
\o
SELECT 'owner again, rebuild_system_versioning_views: ' || hsv.try('hsv_evil', $$SELECT sql_saga.rebuild_system_versioning_views('hsv.mine')$$);
SELECT 'owner again, drop_system_versioning: ' || hsv.try('hsv_evil', $$SELECT sql_saga.drop_system_versioning('hsv.mine')$$);
-- Native DROP TABLE also lets the schema owner drop; so does drop_system_versioning.
ALTER SCHEMA hsv OWNER TO hsv_evil;
\o /dev/null
ALTER EVENT TRIGGER sql_saga_health_checks DISABLE;
ALTER TABLE hsv.mine_history OWNER TO hsv_victim;
ALTER EVENT TRIGGER sql_saga_health_checks ENABLE;
\o
SELECT 'schema owner, drop_system_versioning: ' || hsv.try('hsv_evil', $$SELECT sql_saga.drop_system_versioning('hsv.mine')$$);
SELECT 'schema owner, native DROP TABLE of another role''s table: ' || hsv.try('hsv_evil', $$DROP TABLE hsv.victim$$);
ALTER SCHEMA hsv OWNER TO CURRENT_USER;

\o /dev/null
SELECT sql_saga.drop_system_versioning('hsv.mine');
SELECT sql_saga.drop_system_versioning('hsv.grp');
SELECT sql_saga.drop_unique_key('hsv.nc', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('hsv.nc');
DROP SCHEMA hsv CASCADE;
DROP OWNED BY hsv_victim, hsv_evil, hsv_member, hsv_group, hsv_nocreate;
DROP ROLE hsv_victim;
DROP ROLE hsv_evil;
DROP ROLE hsv_member;
DROP ROLE hsv_group;
DROP ROLE hsv_nocreate;
\i sql/include/test_teardown.sql
