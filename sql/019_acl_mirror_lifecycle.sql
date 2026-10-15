\i sql/include/test_setup.sql

/*
 * Derived-object privileges across the life of a family (access-time authority):
 *   F10 derived objects created AFTER the base was granted have the same fixed ACL as any other;
 *   ALTER TABLE ... OWNER TO and REASSIGN OWNED move every derived object to the new owner, and the
 *   fixed ACL stays; column grants on the base work through the views at access time; grant options
 *   on the base are not copied anywhere; PUBLIC on the base is an ordinary grantee of the base only.
 */
\set SHOW_CONTEXT never
SET client_min_messages = warning;

CREATE ROLE acl_life_r;
CREATE ROLE acl_life_s;
CREATE ROLE acl_life_o1;
CREATE ROLE acl_life_o2;

CREATE FUNCTION pg_temp.acl_entries(prefix text)
RETURNS TABLE (object_name text, grantee text, privilege_type text)
LANGUAGE sql AS $$
    SELECT o.name, CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE a.grantee::regrole::text END, a.privilege_type
    FROM (
        SELECT c.relname::text AS name, c.relacl AS acl, c.relowner AS owner
        FROM pg_class AS c
        WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'v') AND c.relname LIKE prefix || '%'
        UNION ALL
        SELECT p.proname::text, p.proacl, p.proowner
        FROM pg_proc AS p
        WHERE p.pronamespace = 'public'::regnamespace AND p.proname LIKE prefix || '\_\_%'
    ) AS o
    CROSS JOIN LATERAL aclexplode(o.acl) AS a
    WHERE a.grantee <> o.owner
    ORDER BY o.name COLLATE "C", CASE WHEN a.grantee = 0 THEN 'PUBLIC' ELSE a.grantee::regrole::text END COLLATE "C", a.privilege_type
$$;

CREATE TABLE unrel (a int);
CREATE TABLE t (id int, valid_range daterange NOT NULL, x text, PRIMARY KEY (id, valid_range WITHOUT OVERLAPS));
SELECT sql_saga.add_era('t', 'valid_range', 'valid');
RESET client_min_messages;

\echo '--- F10: derived objects created after the base was granted get the fixed ACL'
GRANT SELECT, UPDATE ON t TO acl_life_r;
SELECT sql_saga.add_for_portion_of_view('t', 'valid');
SELECT sql_saga.add_current_view('t', 'valid');
SELECT * FROM pg_temp.acl_entries('t');
-- ... so an unrelated REVOKE afterwards succeeds.
GRANT SELECT ON unrel TO acl_life_s;
REVOKE SELECT ON unrel FROM acl_life_s;

\echo '--- F10: the same for system versioning'
SET client_min_messages = warning;
CREATE TABLE s (id int PRIMARY KEY, x text);
RESET client_min_messages;
GRANT SELECT, INSERT ON s TO acl_life_r;
SELECT sql_saga.add_system_versioning('s');
SELECT * FROM pg_temp.acl_entries('s');
GRANT SELECT ON unrel TO acl_life_s;
REVOKE SELECT ON unrel FROM acl_life_s;

\echo '--- Column grants on the base work through the views at access time'
-- The views select every column of t, so a reader needs SELECT on all of them.
-- (acl_entries lists table-level entries only; column grants live in pg_attribute.attacl.)
GRANT SELECT (id, x, valid_range) ON t TO acl_life_s;
GRANT USAGE ON SCHEMA public TO acl_life_s;
SELECT * FROM pg_temp.acl_entries('t');
INSERT INTO t (id, valid_range, x) VALUES (1, '[2020-01-01,infinity)', 'x1');
SET ROLE acl_life_s;
SELECT x FROM t__current_valid;
RESET ROLE;
REVOKE SELECT (id) ON t FROM acl_life_s;
SET ROLE acl_life_s;
SELECT x FROM t__current_valid; -- fail: no SELECT on t.id
RESET ROLE;
REVOKE SELECT (x, valid_range) ON t FROM acl_life_s;

\echo '--- Grant options on the base are not copied to the views'
GRANT SELECT ON t TO acl_life_s WITH GRANT OPTION;
SELECT c.relname, a.grantee::regrole AS grantee, a.privilege_type, a.is_grantable
FROM pg_class AS c, aclexplode(c.relacl) AS a
WHERE c.relname IN ('t', 't__for_portion_of_valid', 't__current_valid') AND a.grantee = 'acl_life_s'::regrole
ORDER BY 1, 2, 3;
REVOKE GRANT OPTION FOR SELECT ON t FROM acl_life_s;
REVOKE SELECT ON t FROM acl_life_s;

\echo '--- PUBLIC on the base is the base''s business; the derived objects keep their fixed ACL'
GRANT SELECT ON t TO PUBLIC;
GRANT SELECT ON s TO PUBLIC;
SELECT * FROM pg_temp.acl_entries('t') WHERE grantee = 'PUBLIC';
SELECT * FROM pg_temp.acl_entries('s') WHERE grantee = 'PUBLIC';
REVOKE SELECT ON t FROM PUBLIC;
REVOKE SELECT ON s FROM PUBLIC;
SELECT * FROM pg_temp.acl_entries('t') WHERE grantee = 'PUBLIC';
SELECT p.proname, has_function_privilege('acl_life_s', p.oid, 'EXECUTE') AS public_execute
FROM pg_proc AS p
WHERE p.pronamespace = 'public'::regnamespace AND p.proname LIKE 's\_\_%'
ORDER BY p.proname;

\echo '--- ALTER TABLE ... OWNER TO: derived objects follow the owner and keep the fixed ACL'
ALTER TABLE t OWNER TO acl_life_o1;
ALTER TABLE s OWNER TO acl_life_o1;
SELECT c.relname, c.relowner::regrole AS owner
FROM pg_class AS c
WHERE c.relname IN ('t', 't__for_portion_of_valid', 't__current_valid', 's', 's_history', 's_history_gated', 's_with_history')
ORDER BY 1;
SELECT * FROM pg_temp.acl_entries('t');
SELECT * FROM pg_temp.acl_entries('s');
GRANT SELECT ON unrel TO acl_life_s;
REVOKE SELECT ON unrel FROM acl_life_s;

\echo '--- Ownership moves to a role that was a grantee: its old grant entries disappear everywhere'
GRANT SELECT ON t TO acl_life_o2;
ALTER TABLE t OWNER TO acl_life_o2;
SELECT * FROM pg_temp.acl_entries('t');
GRANT SELECT ON unrel TO acl_life_s;
REVOKE SELECT ON unrel FROM acl_life_s;

\echo '--- REASSIGN OWNED'
REASSIGN OWNED BY acl_life_o2 TO acl_life_o1;
SELECT c.relname, c.relowner::regrole AS owner
FROM pg_class AS c
WHERE c.relname IN ('t', 't__for_portion_of_valid', 't__current_valid')
ORDER BY 1;
SELECT * FROM pg_temp.acl_entries('t');
GRANT SELECT ON unrel TO acl_life_s;
REVOKE SELECT ON unrel FROM acl_life_s;

/* Clean up */
SET client_min_messages = warning;
SELECT sql_saga.drop_system_versioning('s');
SELECT sql_saga.drop_current_view('t', 'valid');
SELECT sql_saga.drop_for_portion_of_view('t', 'valid');
SELECT sql_saga.drop_era('t', 'valid');
DROP TABLE s, t, unrel;
DROP OWNED BY acl_life_r, acl_life_s, acl_life_o1, acl_life_o2;
DROP ROLE acl_life_r, acl_life_s, acl_life_o1, acl_life_o2;
RESET client_min_messages;

\i sql/include/test_teardown.sql
