\i sql/include/test_setup.sql

/*
 * Row-level security on history (tmp/design_final.md sections 3, 8, 9, 10, 12).
 *
 * When RLS on the base table B is active for the caller, history read through B_with_history and
 * the as-of functions is filtered by B's own SELECT policies:
 *   - every SELECT/ALL policy P on B has one copy on B_history, FOR SELECT, same roles, same
 *     permissive/restrictive, same USING with columns matched BY NAME;
 *   - the copy is named sql_saga_<16 hex>, the hash of P's name and a per-installation token (so the
 *     base policy of a copy is found by recomputing the hash; copies carry no comment, because pg_dump
 *     would replay it for a copy that the restore has already replaced); copies are created, replaced
 *     and dropped by sql_saga when policies on B change;
 *   - INSERT/UPDATE/DELETE-only policies are not copied;
 *   - a fixed RESTRICTIVE policy keeps roles without SELECT on B out of B_history (direct reads);
 *   - a missing or stale copy makes reads fail with a HINT, never returns unfiltered rows;
 *   - a policy that references a column B_history lacks is refused with a clear error.
 */
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;

CREATE ROLE rls_a;        -- tenant a
CREATE ROLE rls_b;        -- tenant b
CREATE ROLE rls_none;     -- no privilege on the base at all
GRANT USAGE ON SCHEMA public TO rls_a, rls_b, rls_none;

CREATE TABLE doc (id int PRIMARY KEY, tenant text NOT NULL, body text);
SELECT sql_saga.add_system_versioning('doc');
INSERT INTO doc VALUES (1, 'a', 'a-v1'), (2, 'b', 'b-v1');
UPDATE doc SET body = body || '+';          -- one history row per tenant
GRANT SELECT ON doc TO rls_a, rls_b;
ALTER TABLE doc ENABLE ROW LEVEL SECURITY;
-- A fixed installation token makes the copy names deterministic, so the NOTICEs that report every
-- policy change sql_saga makes on doc_history are shown and checked below.
UPDATE sql_saga.installation SET token = 'test0001';
RESET client_min_messages;

-- The sql_saga-managed policies on doc_history, with names reduced to a stable shape.
-- plpgsql, so that the body is resolved when called (sql_saga.installation is part of the design).
CREATE FUNCTION pg_temp.history_policies()
RETURNS TABLE (name_shape text, copied_from text, permissive boolean, cmd "char", roles text, qual text)
LANGUAGE plpgsql AS $$ BEGIN RETURN QUERY
    SELECT CASE WHEN p.polname ~ '^sql_saga_[0-9a-f]{16}$' THEN 'sql_saga_<hash>' ELSE p.polname::text END,
           (SELECT bp.polname::text
              FROM pg_policy AS bp CROSS JOIN sql_saga.installation AS t
             WHERE bp.polrelid = 'doc'::regclass
               AND p.polname = 'sql_saga_' || left(md5(bp.polname || t.token), 16)),
           p.polpermissive, p.polcmd,
           (SELECT string_agg(CASE WHEN r = 0 THEN 'PUBLIC' ELSE r::regrole::text END, ',' ORDER BY 1)
              FROM unnest(p.polroles) AS r),
           CASE WHEN p.polname ~ '^sql_saga_[0-9a-f]{16}$' THEN pg_get_expr(p.polqual, p.polrelid) END
    FROM pg_policy AS p
    WHERE p.polrelid = 'doc_history'::regclass
    ORDER BY CASE WHEN p.polname ~ '^sql_saga_[0-9a-f]{16}$' THEN 'sql_saga_<hash>' ELSE p.polname::text END COLLATE "C",
             2;
END $$;

\echo '--- Without policies on doc (RLS enabled, default deny): no copies, nobody sees rows'
SELECT name_shape, copied_from, permissive, cmd FROM pg_temp.history_policies();
SET ROLE rls_a;
SELECT id, body FROM doc_with_history ORDER BY id, body;
RESET ROLE;

\echo '--- CREATE POLICY on doc creates its copy on doc_history'
CREATE POLICY tenant_a ON doc FOR SELECT TO rls_a USING (tenant = 'a');
CREATE POLICY tenant_b ON doc FOR ALL TO rls_b USING (tenant = 'b');
CREATE POLICY insert_only ON doc FOR INSERT TO rls_a WITH CHECK (true);
SELECT * FROM pg_temp.history_policies();

\echo '--- Each tenant sees only its own current and past rows'
SELECT lower(h.system_valid_range) AS t0 FROM doc_history AS h WHERE h.id = 1 \gset
SET ROLE rls_a;
SELECT id, body FROM doc_with_history ORDER BY id, body;
SELECT id, body FROM doc__as_of(:'t0') ORDER BY id;
SELECT count(*) AS between_rows FROM doc__between('-infinity', 'infinity');
SELECT count(*) AS between_symmetric_rows FROM doc__between_symmetric('infinity', '-infinity');
SELECT count(*) AS from_to_rows FROM doc__from_to('-infinity', 'infinity');
RESET ROLE;
SET ROLE rls_b;
SELECT id, body FROM doc_with_history ORDER BY id, body;
RESET ROLE;

\echo '--- Copy names: unique per policy, hashed with the installation token, valid identifiers'
SELECT count(DISTINCT p.polname) AS distinct_copies,
       bool_and(EXISTS (SELECT FROM pg_policy AS bp CROSS JOIN sql_saga.installation AS t
                        WHERE bp.polrelid = 'doc'::regclass
                          AND p.polname = 'sql_saga_' || left(md5(bp.polname || t.token), 16))) AS name_is_hash_of_policy_and_token,
       bool_and(obj_description(p.oid, 'pg_policy') IS NULL) AS no_comment
FROM pg_policy AS p
WHERE p.polrelid = 'doc_history'::regclass AND p.polname ~ '^sql_saga_[0-9a-f]{16}$';

\echo '--- A role without SELECT on doc reads nothing from doc_history directly'
CREATE POLICY everyone ON doc FOR SELECT TO PUBLIC USING (tenant = 'a');
SET ROLE rls_none;
SELECT count(*) AS direct_history_rows FROM doc_history;
SELECT count(*) FROM doc_with_history; -- fail
RESET ROLE;
DROP POLICY everyone ON doc;

\echo '--- ALTER POLICY on doc replaces its copy'
ALTER POLICY tenant_a ON doc USING (tenant = 'a' AND id = 1);
SELECT name_shape, copied_from, qual FROM pg_temp.history_policies() WHERE copied_from = 'tenant_a';
SET ROLE rls_a;
SELECT id, body FROM doc_with_history ORDER BY id, body;
RESET ROLE;

\echo '--- ALTER POLICY ... RENAME follows'
ALTER POLICY tenant_a ON doc RENAME TO tenant_alpha;
SELECT name_shape, copied_from FROM pg_temp.history_policies() WHERE copied_from IS NOT NULL;

\echo '--- DROP POLICY on doc drops its copy'
DROP POLICY tenant_b ON doc;
SELECT name_shape, copied_from FROM pg_temp.history_policies() WHERE copied_from IS NOT NULL;

\echo '--- Copies refer to columns by name, rendered against the history table'
SELECT name_shape, copied_from, qual FROM pg_temp.history_policies() WHERE copied_from IS NOT NULL;

\echo '--- A policy referencing a column the history lacks is refused'
ALTER TABLE doc ADD COLUMN added_later text;
CREATE POLICY by_added ON doc FOR SELECT TO rls_a USING (added_later = 'x'); -- fail
SELECT count(*) AS by_added_policies FROM pg_policy WHERE polname = 'by_added';

\echo '--- A missing copy makes reads fail with a HINT, never return unfiltered rows'
-- Event triggers do not fire under session_replication_role = replica, so a policy created there
-- gets no copy (as when policies arrive by logical replication).
SET session_replication_role = replica;
CREATE POLICY late ON doc FOR SELECT TO rls_a USING (tenant = 'a');
RESET session_replication_role;
\set VERBOSITY default
SET ROLE rls_a;
SELECT id, body FROM doc_with_history; -- fail
RESET ROLE;
\set VERBOSITY terse
SELECT sql_saga.sync_history_policies('doc');
SET ROLE rls_a;
SELECT id, body FROM doc_with_history ORDER BY id, body;
RESET ROLE;

\echo '--- A stale copy (base policy changed while event triggers were off) also fails closed'
SET session_replication_role = replica;
ALTER POLICY late ON doc USING (tenant = 'b');
RESET session_replication_role;
\set VERBOSITY default
SET ROLE rls_a;
SELECT id, body FROM doc_with_history; -- fail
RESET ROLE;
\set VERBOSITY terse
SELECT sql_saga.sync_history_policies('doc');
DROP POLICY late ON doc;

\echo '--- Current and FOR PORTION OF views apply the base policies to the caller'
SET client_min_messages = warning;
CREATE TABLE tdoc (id int, tenant text NOT NULL, valid_range daterange NOT NULL, body text,
                   PRIMARY KEY (id, valid_range WITHOUT OVERLAPS));
SELECT sql_saga.add_era('tdoc', 'valid_range', 'valid');
SELECT sql_saga.add_for_portion_of_view('tdoc', 'valid');
SELECT sql_saga.add_current_view('tdoc', 'valid');
INSERT INTO tdoc VALUES (1, 'a', '[2020-01-01,infinity)', 'a'), (2, 'b', '[2020-01-01,infinity)', 'b');
GRANT SELECT ON tdoc TO rls_a;
ALTER TABLE tdoc ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_a ON tdoc FOR SELECT TO rls_a USING (tenant = 'a');
RESET client_min_messages;
SET ROLE rls_a;
SELECT id, tenant FROM tdoc__current_valid ORDER BY id;
SELECT id, tenant FROM tdoc__for_portion_of_valid ORDER BY id;
RESET ROLE;

\echo '--- The owner is exempt from RLS on doc and reads all history'
SELECT count(*) AS owner_rows FROM doc_with_history;

\echo '--- FORCE ROW LEVEL SECURITY: a non-superuser owner is filtered on current rows and on history alike'
-- FORCE on doc is mirrored to doc_history (owner decision 5, tmp/design_final.md section 15).
CREATE ROLE rls_owner;
GRANT USAGE, CREATE ON SCHEMA public TO rls_owner;
ALTER TABLE doc OWNER TO rls_owner;
ALTER TABLE doc FORCE ROW LEVEL SECURITY;
SELECT c.relname, c.relforcerowsecurity FROM pg_class AS c WHERE c.relname IN ('doc', 'doc_history') ORDER BY 1;
SET ROLE rls_owner;
SELECT count(*) AS forced_owner_rows FROM doc_with_history;
RESET ROLE;
CREATE POLICY owner_sees_a ON doc FOR SELECT TO rls_owner USING (tenant = 'a');
SET ROLE rls_owner;
SELECT id, body FROM doc_with_history ORDER BY id, body;
RESET ROLE;
DROP POLICY owner_sees_a ON doc;
ALTER TABLE doc NO FORCE ROW LEVEL SECURITY;
SELECT c.relname, c.relforcerowsecurity FROM pg_class AS c WHERE c.relname IN ('doc', 'doc_history') ORDER BY 1;
SET ROLE rls_owner;
SELECT count(*) AS unforced_owner_rows FROM doc_with_history;
RESET ROLE;
ALTER TABLE doc OWNER TO CURRENT_USER;

\echo '--- The installation token is extension state that pg_dump does not dump, and nothing records "synced"'
SELECT count(*) AS token_rows FROM sql_saga.installation;
SELECT EXISTS (SELECT FROM pg_extension AS e, unnest(e.extconfig) AS c(oid)
               WHERE e.extname = 'sql_saga' AND c.oid = 'sql_saga.installation'::regclass) AS token_dumped;
-- The only catalog columns about "sync" are the era's synchronization trigger names.
SELECT c.relname, a.attname
FROM pg_attribute AS a JOIN pg_class AS c ON c.oid = a.attrelid
WHERE c.relnamespace = 'sql_saga'::regnamespace AND c.relkind = 'r' AND a.attnum > 0 AND a.attname ~ 'sync'
ORDER BY 1, 2;
-- and the token table holds only the token.
SELECT a.attname FROM pg_attribute AS a
WHERE a.attrelid = 'sql_saga.installation'::regclass AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum;

/* Clean up */
SET client_min_messages = warning;
SELECT sql_saga.drop_current_view('tdoc', 'valid');
SELECT sql_saga.drop_for_portion_of_view('tdoc', 'valid');
SELECT sql_saga.drop_era('tdoc', 'valid');
SELECT sql_saga.drop_system_versioning('doc');
DROP TABLE doc, tdoc;
DROP OWNED BY rls_a, rls_b, rls_none, rls_owner;
DROP ROLE rls_a, rls_b, rls_none, rls_owner;
RESET client_min_messages;

\i sql/include/test_teardown.sql
