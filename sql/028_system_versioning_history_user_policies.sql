\i sql/include/test_setup.sql

/*
 * Policies on a history table are managed by sql_saga (tmp/design_final.md decision 7).
 *
 * The history table's row-level security is the base table's: its policies are copies of the base
 * table's SELECT/ALL policies plus two fixed ones. A policy of one's own on the history table would be
 * OR-ed with the copies and could show RLS-subject callers history the base table hides from them.
 * So CREATE POLICY on a history table is refused, with a hint to create it on the base table, where it
 * is copied from. Managed names (as replayed by pg_restore) are still accepted.
 */
\set SHOW_CONTEXT never
\set VERBOSITY default
SET client_min_messages = warning;

CREATE ROLE hup_a; GRANT USAGE ON SCHEMA public TO hup_a;
CREATE TABLE doc (id int PRIMARY KEY, tenant text NOT NULL);
SELECT sql_saga.add_system_versioning('doc');
INSERT INTO doc VALUES (1, 'a'), (2, 'b');
UPDATE doc SET tenant = tenant;
GRANT SELECT ON doc TO hup_a;
ALTER TABLE doc ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_a ON doc FOR SELECT TO hup_a USING (tenant = 'a');
RESET client_min_messages;

\echo '--- A policy of ones own on the history table is refused, with what to do instead'
CREATE POLICY mine ON doc_history FOR SELECT USING (true); -- fail
SELECT count(*) AS mine_policies FROM pg_policy WHERE polname = 'mine';

\echo '--- Any command: a FOR ALL policy of ones own is refused too'
CREATE POLICY mine_all ON doc_history FOR ALL USING (true); -- fail

\echo '--- Renaming a managed history policy to a name of ones own is refused'
-- (the managed name ends in the installation token, so it is looked up rather than spelled out)
\set ECHO none
SELECT format('ALTER POLICY %I ON doc_history RENAME TO mine_renamed', p.polname)
FROM pg_policy AS p
WHERE p.polrelid = 'doc_history'::regclass AND p.polname LIKE 'sql_saga_history_reader_%' \gexec
\set ECHO all
SELECT count(*) AS renamed_policies FROM pg_policy WHERE polname = 'mine_renamed';

\echo '--- So tenant a still sees only its own current and past rows'
SET ROLE hup_a;
SELECT id FROM doc_with_history ORDER BY id;
RESET ROLE;

\echo '--- The same policy on the base table is accepted and copied to the history'
SET client_min_messages = warning;
CREATE POLICY mine ON doc FOR SELECT TO hup_a USING (true);
RESET client_min_messages;
SET ROLE hup_a;
SELECT id FROM doc_with_history ORDER BY id;
RESET ROLE;

\echo '--- A managed name (as pg_restore replays it) is accepted and synchronized'
SET client_min_messages = warning;
SELECT format('CREATE POLICY %I ON doc_history FOR SELECT USING (true)',
              'sql_saga_' || left(md5('tenant_a' || 'otherinstallation'), 16)) \gexec
RESET client_min_messages;
SELECT count(*) AS orphaned_copies
FROM pg_policy AS p
WHERE p.polrelid = 'doc_history'::regclass
  AND p.polname = 'sql_saga_' || left(md5('tenant_a' || 'otherinstallation'), 16);

/* Clean up */
SET client_min_messages = warning;
SELECT sql_saga.drop_system_versioning('doc');
DROP TABLE doc;
DROP OWNED BY hup_a;
DROP ROLE hup_a;
RESET client_min_messages;

\i sql/include/test_teardown.sql
