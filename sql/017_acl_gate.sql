\i sql/include/test_setup.sql

/*
 * GRANT/REVOKE and the FOR PORTION OF view (access-time authority).
 *
 * Downstream incident (statbus-481/482): a base table is granted to a role that inherits another
 * grantee. An unrelated REVOKE ... ON FUNCTION ... FROM PUBLIC then raised
 * "cannot revoke INSERT directly from <view>" and aborted a pg_restore.
 *
 * Correct behaviour:
 *  - the view holds no copy of who may use it: it is security_invoker and granted to PUBLIC, so
 *    PostgreSQL checks the caller's privileges on the base table at access time;
 *  - no GRANT/REVOKE, on managed or unrelated objects, makes sql_saga act, fail or emit a NOTICE;
 *  - granting the view directly gives no access to the base data.
 */
\set SHOW_CONTEXT never

CREATE ROLE acl_gate_ru;
CREATE ROLE acl_gate_au IN ROLE acl_gate_ru;   -- au inherits ru
CREATE ROLE acl_gate_x;

-- Explicit (grantee, privilege) entries of the named relations, owner excluded.
CREATE FUNCTION pg_temp.acl_entries(names text[])
RETURNS TABLE (object_name name, grantee text, privilege_type text)
LANGUAGE sql AS $$
    SELECT c.relname,
           CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE a.grantee::regrole::text END,
           a.privilege_type
    FROM pg_class AS c
    CROSS JOIN LATERAL aclexplode(c.relacl) AS a
    WHERE c.relname = ANY (names)
      AND a.grantee <> c.relowner
    ORDER BY c.relname, CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE a.grantee::regrole::text END COLLATE "C", a.privilege_type
$$;

CREATE TABLE est (id int, valid_range daterange NOT NULL, valid_from date, valid_until date, name text,
                  PRIMARY KEY (id, valid_range WITHOUT OVERLAPS));
SET client_min_messages = warning;
SELECT sql_saga.add_era('est', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
RESET client_min_messages;
SELECT sql_saga.add_for_portion_of_view('est', 'valid');
INSERT INTO est (id, valid_from, valid_until, name) VALUES (1, '2020-01-01', 'infinity', 'one');
GRANT USAGE ON SCHEMA public TO acl_gate_ru, acl_gate_x;

\echo '--- The view is security_invoker and granted to PUBLIC, independent of the base ACL'
SELECT c.relname, c.reloptions FROM pg_class AS c WHERE c.relname = 'est__for_portion_of_valid';
SELECT * FROM pg_temp.acl_entries(ARRAY['est', 'est__for_portion_of_valid']);

\echo '--- Grant the base table to a role; a role inheriting it gets the same access through the view'
-- An UPDATE through the view runs temporal_merge as the caller, which writes with INSERT/UPDATE/DELETE.
GRANT SELECT, INSERT, UPDATE, DELETE ON est TO acl_gate_ru;
SELECT * FROM pg_temp.acl_entries(ARRAY['est', 'est__for_portion_of_valid']);
SET ROLE acl_gate_au;
SELECT id, name FROM est__for_portion_of_valid ORDER BY id;
UPDATE est__for_portion_of_valid SET name = 'changed' WHERE id = 1;
SELECT id, name FROM est__for_portion_of_valid ORDER BY id;
RESET ROLE;

\echo '--- A role without privileges on the base cannot use the view'
SET ROLE acl_gate_x;
SELECT id, name FROM est__for_portion_of_valid ORDER BY id; -- fail
RESET ROLE;

\echo '--- The incident: an unrelated REVOKE on an unrelated function must succeed'
CREATE FUNCTION public.upgrade_schedule(text, boolean) RETURNS int LANGUAGE sql AS 'SELECT 1';
REVOKE ALL ON FUNCTION public.upgrade_schedule(text, boolean) FROM PUBLIC;
SELECT has_function_privilege('acl_gate_x', 'public.upgrade_schedule(text, boolean)', 'EXECUTE') AS x_can_execute;

\echo '--- Unrelated GRANT/REVOKE on tables, sequences and schemas must succeed silently'
CREATE TABLE unrel (a int);
CREATE SEQUENCE unrel_seq;
GRANT SELECT ON unrel TO acl_gate_ru;
REVOKE SELECT ON unrel FROM acl_gate_ru;
GRANT USAGE ON SEQUENCE unrel_seq TO acl_gate_ru;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO acl_gate_ru;
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE SELECT ON TABLES FROM acl_gate_ru;

\echo '--- REVOKE on the base takes effect through the view at once, with no action by sql_saga'
REVOKE SELECT ON est FROM acl_gate_ru;
SELECT * FROM pg_temp.acl_entries(ARRAY['est', 'est__for_portion_of_valid']);
SET ROLE acl_gate_au;
SELECT id, name FROM est__for_portion_of_valid ORDER BY id; -- fail: no SELECT on est
RESET ROLE;

\echo '--- Direct GRANT on the view gives no access to the base data'
GRANT SELECT, UPDATE ON est__for_portion_of_valid TO acl_gate_x;
SET ROLE acl_gate_x;
SELECT id, name FROM est__for_portion_of_valid ORDER BY id; -- fail: no SELECT on est
RESET ROLE;

\echo '--- Direct REVOKE on the view is accepted (it can only reduce access)'
REVOKE SELECT, UPDATE ON est__for_portion_of_valid FROM acl_gate_x;
REVOKE UPDATE ON est__for_portion_of_valid FROM acl_gate_au;

\echo '--- A GRANT naming both the base and the view in one statement is accepted'
GRANT UPDATE ON est, est__for_portion_of_valid TO acl_gate_x;
SELECT * FROM pg_temp.acl_entries(ARRAY['est', 'est__for_portion_of_valid']);

/* Clean up */
SELECT sql_saga.drop_for_portion_of_view('est', 'valid');
SET client_min_messages = warning;
SELECT sql_saga.drop_era('est', 'valid');
RESET client_min_messages;
DROP TABLE est;
DROP TABLE unrel;
DROP SEQUENCE unrel_seq;
DROP FUNCTION public.upgrade_schedule(text, boolean);
-- DROP OWNED also removes privileges on the schema and default privileges, whatever state the
-- statements above left behind.
DROP OWNED BY acl_gate_au, acl_gate_ru, acl_gate_x;
DROP ROLE acl_gate_au;
DROP ROLE acl_gate_ru;
DROP ROLE acl_gate_x;

\i sql/include/test_teardown.sql
