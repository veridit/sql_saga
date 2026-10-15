\i sql/include/test_setup.sql

/*
 * Privileges on the derived objects of system-versioned tables (access-time authority).
 *
 * Fixed state, independent of the base table's ACL (tmp/design_final.md sections 2, 3, 9):
 *   B_with_history          security_invoker view, PUBLIC SELECT
 *   B_history_gated         owner-rights view, PUBLIC SELECT, raises 42501 unless the caller has SELECT on B
 *   B_history               PUBLIC SELECT, RLS enabled (not forced), policies sql_saga_history_reader
 *                           (permissive) and sql_saga_history_base_select (restrictive)
 *   as-of functions         explicit PUBLIC EXECUTE
 * Who may read history is decided when it is read, by the caller's SELECT on B. No GRANT or REVOKE
 * needs any action by sql_saga.
 *
 * Defects of the earlier ACL-copying implementation, asserted here as behaviour (tmp/design_a_review.md):
 *   F3  REVOKE from a role that inherits another grantee.
 *   F4  a grantee on one table gained privileges on another table's history table.
 *   F5  after one GRANT on a versioned table, unrelated GRANTs failed.
 *   F6  once a versioned table had a grantee, unrelated REVOKEs failed.
 *   F11 an unrelated REVOKE stripped PUBLIC EXECUTE from as-of functions.
 *   F13 with EXECUTE revoked from PUBLIC by default privileges, grantees could not call the as-of functions.
 */
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;

CREATE ROLE acl_sv_ru;
CREATE ROLE acl_sv_au IN ROLE acl_sv_ru;
CREATE ROLE acl_sv_ya;
CREATE ROLE acl_sv_wa;
CREATE ROLE acl_sv_wb;
CREATE ROLE acl_sv_p5;
CREATE ROLE acl_sv_hd;
CREATE ROLE acl_sv_other;
CREATE ROLE acl_sv_z;

-- Explicit (grantee, privilege) entries of relations and functions, owner excluded.
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

-- Family fa: FOR PORTION OF. Family b: system versioning.
CREATE TABLE fa (id int, valid_range daterange NOT NULL, x text, PRIMARY KEY (id, valid_range WITHOUT OVERLAPS));
SELECT sql_saga.add_era('fa', 'valid_range', 'valid');
CREATE TABLE b (id int PRIMARY KEY, secret text);
INSERT INTO b VALUES (1, 'b-secret');
-- Granted BEFORE versioning (F10).
GRANT SELECT ON b TO acl_sv_wb;
SELECT sql_saga.add_system_versioning('b');
UPDATE b SET secret = 'b-secret-v2' WHERE id = 1;
SELECT sql_saga.add_for_portion_of_view('fa', 'valid');
RESET client_min_messages;

\echo '--- Fixed state of the history objects'
SELECT c.relname,
       coalesce('security_invoker=on' = ANY (c.reloptions), false) AS security_invoker,
       c.relrowsecurity, c.relforcerowsecurity
FROM pg_class AS c
WHERE c.relname IN ('b_history', 'b_history_gated', 'b_with_history')
ORDER BY c.relname;
SELECT p.polname, p.polpermissive, p.polcmd, p.polroles = '{0}' AS to_public
FROM pg_policy AS p
WHERE p.polrelid = 'b_history'::regclass
ORDER BY p.polname;
SELECT * FROM pg_temp.acl_entries('b');

\echo '--- F4: a grant on one family never reaches another family''s history'
GRANT SELECT, UPDATE, DELETE ON fa TO acl_sv_wa;
GRANT SELECT ON fa TO acl_sv_ya;
SELECT * FROM pg_temp.acl_entries('b');
SET ROLE acl_sv_wb;
SELECT count(*) AS wb_with_history FROM b_with_history;
SELECT count(*) AS wb_history_direct FROM b_history;
SELECT count(*) AS wb_as_of_past FROM b__as_of('-infinity');
RESET ROLE;
SET ROLE acl_sv_wa;
SELECT count(*) AS wa_with_history FROM b_with_history; -- fail
SELECT count(*) AS wa_history_direct FROM b_history;
SELECT count(*) AS wa_as_of_past FROM b__as_of('-infinity'); -- fail
RESET ROLE;

\echo '--- F4: no grantee can rewrite the audit trail'
SET ROLE acl_sv_wb;
UPDATE b_history SET secret = 'TAMPERED' WHERE id = 1; -- fail
DELETE FROM b_history; -- fail
RESET ROLE;
\echo '--- A stray write grant on the history table is harmless: no policy admits writing'
GRANT UPDATE, DELETE ON b_history TO acl_sv_wb;
SET ROLE acl_sv_wb;
UPDATE b_history SET secret = 'TAMPERED' WHERE id = 1;
DELETE FROM b_history;
RESET ROLE;
REVOKE UPDATE, DELETE ON b_history FROM acl_sv_wb;
SELECT count(*) FILTER (WHERE secret = 'TAMPERED') AS tampered_rows, count(*) AS history_rows FROM b_history;

\echo '--- F5: after a grant on a versioned table, unrelated GRANTs still work'
GRANT SELECT ON b TO acl_sv_p5;
GRANT SELECT ON unrel TO acl_sv_p5;

\echo '--- F6: after a grant on a versioned table, unrelated REVOKEs still work'
REVOKE SELECT ON unrel FROM acl_sv_p5;

\echo '--- F3: a role inheriting a grantee reads history; revoking from the grantee ends it at once'
GRANT SELECT ON b TO acl_sv_ru;
SET ROLE acl_sv_au;
SELECT count(*) AS au_with_history FROM b_with_history;
RESET ROLE;
REVOKE SELECT ON b FROM acl_sv_ru;
SET ROLE acl_sv_au;
SELECT count(*) AS au_with_history FROM b_with_history; -- fail
SELECT count(*) AS au_history_direct FROM b_history;
RESET ROLE;
GRANT SELECT ON unrel TO acl_sv_z;
REVOKE SELECT ON unrel FROM acl_sv_z;

\echo '--- GRANT ... ON ALL TABLES IN SCHEMA is accepted; history stays unwritable'
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA public TO acl_sv_z;
SET ROLE acl_sv_z;
INSERT INTO b_history (id, secret, system_valid_range) VALUES (99, 'forged', tstzrange('2000-01-01', '2000-01-02')); -- fail
RESET ROLE;
SELECT count(*) AS forged_rows FROM b_history WHERE id = 99;
REVOKE SELECT, INSERT ON ALL TABLES IN SCHEMA public FROM acl_sv_z;

\echo '--- F11: unrelated REVOKEs leave PUBLIC EXECUTE on the as-of functions alone'
SET client_min_messages = warning;
CREATE TABLE c (id int PRIMARY KEY, x text);
SELECT sql_saga.add_system_versioning('c');
RESET client_min_messages;
GRANT SELECT ON unrel TO acl_sv_z;
REVOKE SELECT ON unrel FROM acl_sv_z;
SELECT p.proname, has_function_privilege('acl_sv_other', p.oid, 'EXECUTE') AS other_can_execute
FROM pg_proc AS p
WHERE p.pronamespace = 'public'::regnamespace AND p.proname LIKE 'c\_\_%'
ORDER BY p.proname;

\echo '--- F13: EXECUTE revoked from PUBLIC by default privileges'
ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
SET client_min_messages = warning;
CREATE TABLE d (id int PRIMARY KEY, x text);
SELECT sql_saga.add_system_versioning('d');
RESET client_min_messages;
INSERT INTO d VALUES (1, 'd1');
GRANT SELECT ON d TO acl_sv_hd;
SELECT r.rolname,
       has_function_privilege(r.rolname, 'd__as_of(timestamp with time zone)', 'EXECUTE') AS as_of,
       has_function_privilege(r.rolname, 'd__between(timestamp with time zone,timestamp with time zone)', 'EXECUTE') AS between_,
       has_function_privilege(r.rolname, 'd__between_symmetric(timestamp with time zone,timestamp with time zone)', 'EXECUTE') AS between_symmetric,
       has_function_privilege(r.rolname, 'd__from_to(timestamp with time zone,timestamp with time zone)', 'EXECUTE') AS from_to
FROM (VALUES ('acl_sv_hd'), ('acl_sv_other')) AS r (rolname)
ORDER BY r.rolname;
SET ROLE acl_sv_hd;
SELECT id, x FROM d__as_of(now());
RESET ROLE;
SET ROLE acl_sv_other;
SELECT id, x FROM d__as_of(now()); -- fail: no SELECT on d
RESET ROLE;
ALTER DEFAULT PRIVILEGES GRANT EXECUTE ON FUNCTIONS TO PUBLIC;

\echo '--- DROP OWNED BY removes the grantee; its history access ends with its base grant'
DROP OWNED BY acl_sv_p5;
SET ROLE acl_sv_p5;
SELECT count(*) AS p5_with_history FROM b_with_history; -- fail
RESET ROLE;
SELECT * FROM pg_temp.acl_entries('b');

/* Clean up */
SET client_min_messages = warning;
SELECT sql_saga.drop_system_versioning('b');
SELECT sql_saga.drop_system_versioning('c');
SELECT sql_saga.drop_system_versioning('d');
SELECT sql_saga.drop_for_portion_of_view('fa', 'valid');
SELECT sql_saga.drop_era('fa', 'valid');
DROP TABLE b, c, d, fa, unrel;
DROP OWNED BY acl_sv_ru, acl_sv_au, acl_sv_ya, acl_sv_wa, acl_sv_wb, acl_sv_p5, acl_sv_hd, acl_sv_other, acl_sv_z;
DROP ROLE acl_sv_au, acl_sv_ru, acl_sv_ya, acl_sv_wa, acl_sv_wb, acl_sv_p5, acl_sv_hd, acl_sv_other, acl_sv_z;
RESET client_min_messages;

\i sql/include/test_teardown.sql
