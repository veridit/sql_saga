\i sql/include/test_setup.sql

/*
 * The guard that lets sql_saga's own history-policy DDL through cannot be forged.
 *
 * sql_saga manages the row-level security of every history table: its policies are copies of the
 * base table's SELECT/ALL policies plus two fixed ones, RLS is enabled, and FORCE mirrors the base
 * table (tmp/design_final.md decisions 7-10). The event triggers refuse direct changes to these, and
 * re-synchronize the history when the base table's policies or RLS settings change.
 *
 * The synchronizer authorizes its own nested DDL through a private, transaction-scoped guard row
 * (sql_saga.__internal_history_policy_sync_active). The setting sql_saga.managing_history_policies is
 * an ordinary custom GUC that any role can set, so it must never authorize anything, nor suppress a
 * synchronization:
 *   A. with the GUC set by hand (to 'on', or to the base table's OID), a table owner still cannot
 *      create a policy of its own on the history, alter, rename or drop a managed history policy,
 *      or disable / (un)force RLS on the history;
 *   B. with the GUC set by hand, policy and RLS DDL on the base table is still copied to the history;
 *   C. the legitimate paths keep working: base policy create/alter/drop, sync_history_policies,
 *      pg_restore replaying managed policies, add_system_versioning on a forced base table, and
 *      drop_system_versioning; each synchronization reports exactly what it did, once.
 *
 * Every attempt runs in a subtransaction that is rolled back, so the cases are independent. The
 * outcome is "refused: <error>" or "accepted: <state>", where <state> checks the history family
 * against what sql_saga is meant to maintain ("in sync") or shows what an RLS-subject reader sees.
 */
\set SHOW_CONTEXT never
\set VERBOSITY default
SET client_min_messages = warning;

-- Deterministic policy names: they end in the per-installation token.
UPDATE sql_saga.installation SET token = 'guardtoken';

CREATE ROLE g123_owner;
CREATE ROLE g123_reader;
CREATE SCHEMA g123 AUTHORIZATION g123_owner;
GRANT USAGE ON SCHEMA g123 TO g123_reader;
SET search_path = g123, public;

-- The state sql_saga is meant to maintain on the history of table_oid: RLS enabled, FORCE as on the
-- base, exactly the two fixed policies plus one up-to-date copy per SELECT/ALL base policy.
CREATE FUNCTION g123.family_state(table_oid regclass)
RETURNS text LANGUAGE sql AS $family_state$
    WITH fam AS (
        SELECT f.base_oid, f.history_oid FROM sql_saga.__internal_history_family(table_oid) AS f
        WHERE f.base_oid = table_oid
    ), expected AS (
        -- The fixed policies' USING expressions name the base table; they are checked by what a reader sees.
        SELECT sql_saga.__internal_history_reader_policy_name() AS polname, true AS permissive, '{0}'::oid[] AS roles, NULL::text AS qual
        UNION ALL
        SELECT sql_saga.__internal_history_base_select_policy_name(), false, '{0}'::oid[], NULL
        UNION ALL
        SELECT c.copy_name, c.permissive, c.roles, coalesce(c.qual, 'true')
        FROM fam CROSS JOIN LATERAL sql_saga.__internal_history_policy_copies(fam.base_oid) AS c
    ), actual AS (
        SELECT p.polname, p.polpermissive AS permissive, p.polroles AS roles, pg_get_expr(p.polqual, p.polrelid) AS qual
        FROM fam JOIN pg_policy AS p ON p.polrelid = fam.history_oid
    ), problems AS (
        SELECT 'history RLS disabled' AS problem
        FROM fam JOIN pg_class AS h ON h.oid = fam.history_oid WHERE NOT h.relrowsecurity
        UNION ALL
        SELECT format('history FORCE %s, base FORCE %s', h.relforcerowsecurity, b.relforcerowsecurity)
        FROM fam JOIN pg_class AS h ON h.oid = fam.history_oid JOIN pg_class AS b ON b.oid = fam.base_oid
        WHERE h.relforcerowsecurity <> b.relforcerowsecurity
        UNION ALL
        SELECT 'unmanaged history policy ' || a.polname
        FROM actual AS a WHERE NOT EXISTS (SELECT FROM expected AS e WHERE e.polname = a.polname)
        UNION ALL
        SELECT 'missing history policy ' || e.polname
        FROM expected AS e WHERE NOT EXISTS (SELECT FROM actual AS a WHERE a.polname = e.polname)
        UNION ALL
        SELECT 'stale history policy ' || a.polname
        FROM actual AS a JOIN expected AS e ON e.polname = a.polname
        WHERE (a.permissive, a.roles) IS DISTINCT FROM (e.permissive, e.roles)
           OR (e.qual IS NOT NULL AND a.qual IS DISTINCT FROM e.qual)
    )
    SELECT CASE WHEN NOT EXISTS (SELECT FROM fam) THEN 'not versioned'
                WHEN NOT EXISTS (SELECT FROM problems) THEN 'in sync'
                ELSE 'OUT OF SYNC: ' || (SELECT string_agg(p.problem, ', ' ORDER BY p.problem) FROM problems AS p)
           END;
$family_state$;

-- The history rows of tenant b that g123_reader (who may only see tenant a) can read.
CREATE FUNCTION g123.reader_sees_tenant_b_history()
RETURNS text LANGUAGE plpgsql AS $reader_sees$
DECLARE
    previous_role text := current_setting('role');
    n bigint;
BEGIN
    PERFORM set_config('role', 'g123_reader', true);
    SELECT count(*) INTO n FROM g123.doc_history AS h WHERE h.tenant = 'b';
    PERFORM set_config('role', previous_role, true);
    RETURN format('reader sees %s tenant b history rows', n);
END;
$reader_sees$;

-- Runs command (as the caller) with sql_saga.managing_history_policies set to guard (unless NULL),
-- then observe; rolls everything back. Error messages are reported without PL/pgSQL context.
-- The guard '<base oid>' stands for the OID of family, the base table of the family the command
-- touches (the value the synchronizer itself sets for that family).
CREATE FUNCTION g123.attempt(guard text, family regclass, command text, observe text)
RETURNS text LANGUAGE plpgsql AS $attempt$
DECLARE
    outcome text;
BEGIN
    BEGIN
        IF guard = '<base oid>' THEN
            guard := family::oid::text;
        END IF;
        IF guard IS NOT NULL THEN
            PERFORM set_config('sql_saga.managing_history_policies', guard, true);
        END IF;
        EXECUTE command;
        EXECUTE observe INTO outcome;
        RAISE EXCEPTION USING ERRCODE = 'Z0001', MESSAGE = outcome;
    EXCEPTION
        WHEN SQLSTATE 'Z0001' THEN
            GET STACKED DIAGNOSTICS outcome = MESSAGE_TEXT;
            RETURN 'accepted: ' || outcome;
        WHEN OTHERS THEN
            RETURN 'refused: ' || SQLERRM;
    END;
END;
$attempt$;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA g123 TO PUBLIC;

-- A versioned table with a tenant policy, owned by an ordinary role.
SET ROLE g123_owner;
CREATE TABLE g123.doc (id int PRIMARY KEY, tenant text NOT NULL);
SELECT sql_saga.add_system_versioning('g123.doc');
INSERT INTO g123.doc VALUES (1, 'a'), (2, 'b');
UPDATE g123.doc SET tenant = tenant;
GRANT SELECT ON g123.doc TO g123_reader;
ALTER TABLE g123.doc ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_a ON g123.doc FOR SELECT TO g123_reader USING (tenant = 'a');
-- A second one whose base table forces row level security.
CREATE TABLE g123.forced (id int PRIMARY KEY, tenant text NOT NULL);
ALTER TABLE g123.forced ENABLE ROW LEVEL SECURITY;
ALTER TABLE g123.forced FORCE ROW LEVEL SECURITY;
CREATE POLICY everyone ON g123.forced FOR ALL USING (true);
SELECT sql_saga.add_system_versioning('g123.forced');
-- A third one without row level security on its base table.
CREATE TABLE g123.plain (id int PRIMARY KEY, tenant text NOT NULL);
CREATE POLICY everyone ON g123.plain FOR SELECT USING (true);
SELECT sql_saga.add_system_versioning('g123.plain');
RESET ROLE;

\echo '--- Starting point: all histories in sync; tenant b history hidden from the reader'
SELECT g123.family_state('g123.doc') AS doc, g123.family_state('g123.forced') AS forced,
       g123.family_state('g123.plain') AS plain, g123.reader_sees_tenant_b_history() AS reader;
SELECT c.relname, c.relrowsecurity, c.relforcerowsecurity FROM pg_class AS c
WHERE c.relnamespace = 'g123'::regnamespace AND c.relkind = 'r' ORDER BY c.relname;
SELECT p.polname FROM pg_policy AS p WHERE p.polrelid = 'g123.doc_history'::regclass ORDER BY p.polname;

/*
 * A. Direct DDL on the history's row-level security, by its owner. Refused with or without the GUC.
 * "base oid" is the GUC set to the OID of the base table of the family the command touches (what
 * the synchronizer sets for that family).
 */
CREATE TEMP TABLE guards (ord int, label text, guard text);
INSERT INTO guards VALUES
    (1, 'unset', NULL),
    (2, 'on', 'on'),
    (3, 'base oid', '<base oid>');
CREATE TEMP TABLE attacks (ord int, label text, command text, observe text, family regclass DEFAULT 'g123.doc');
INSERT INTO attacks (ord, label, command, observe) VALUES
    (1, 'own policy on history',
        $$CREATE POLICY mine ON g123.doc_history FOR SELECT USING (true)$$,
        $$SELECT g123.reader_sees_tenant_b_history() || ', ' || g123.family_state('g123.doc')$$),
    (2, 'own FOR ALL policy on history',
        $$CREATE POLICY mine_all ON g123.doc_history FOR ALL USING (true)$$,
        $$SELECT g123.reader_sees_tenant_b_history() || ', ' || g123.family_state('g123.doc')$$),
    (3, 'alter fixed policy',
        $$ALTER POLICY sql_saga_history_reader_guardtoken ON g123.doc_history USING (true)$$,
        $$SELECT g123.reader_sees_tenant_b_history() || ', ' || g123.family_state('g123.doc')$$),
    (4, 'alter copy qual',
        format('ALTER POLICY %I ON g123.doc_history USING (true)', sql_saga.__internal_history_copy_name('tenant_a')),
        $$SELECT g123.reader_sees_tenant_b_history() || ', ' || g123.family_state('g123.doc')$$),
    (5, 'alter copy roles',
        format('ALTER POLICY %I ON g123.doc_history TO PUBLIC', sql_saga.__internal_history_copy_name('tenant_a')),
        $$SELECT g123.family_state('g123.doc')$$),
    (6, 'rename fixed policy to own name',
        $$ALTER POLICY sql_saga_history_reader_guardtoken ON g123.doc_history RENAME TO mine_renamed$$,
        $$SELECT g123.family_state('g123.doc')$$),
    (7, 'drop fixed policy',
        $$DROP POLICY sql_saga_history_base_select_guardtoken ON g123.doc_history$$,
        $$SELECT g123.reader_sees_tenant_b_history() || ', ' || g123.family_state('g123.doc')$$),
    (8, 'drop copy',
        format('DROP POLICY %I ON g123.doc_history', sql_saga.__internal_history_copy_name('tenant_a')),
        $$SELECT g123.reader_sees_tenant_b_history() || ', ' || g123.family_state('g123.doc')$$),
    (9, 'disable history RLS',
        $$ALTER TABLE g123.doc_history DISABLE ROW LEVEL SECURITY$$,
        $$SELECT g123.reader_sees_tenant_b_history() || ', ' || g123.family_state('g123.doc')$$),
    (10, 'force history RLS of unforced base',
        $$ALTER TABLE g123.doc_history FORCE ROW LEVEL SECURITY$$,
        $$SELECT g123.family_state('g123.doc')$$);
INSERT INTO attacks (ord, label, command, observe, family) VALUES
    (11, 'no force history RLS of forced base',
        $$ALTER TABLE g123.forced_history NO FORCE ROW LEVEL SECURITY$$,
        $$SELECT g123.family_state('g123.forced')$$, 'g123.forced');

GRANT SELECT ON guards, attacks TO g123_owner;

\echo '--- A: direct DDL on the history''s row level security is refused, whatever the GUC says'
SET ROLE g123_owner;
SELECT a.label AS attempt, g.label AS guc, g123.attempt(g.guard, a.family, a.command, a.observe) AS outcome
FROM attacks AS a CROSS JOIN guards AS g
ORDER BY a.ord, g.ord;
RESET ROLE;

/*
 * B. Policy and RLS DDL on the base table is copied to the history, whatever the GUC says. With the
 * GUC set by hand the event trigger must not skip the synchronization: a restrictive base policy left
 * uncopied would show RLS-subject readers history rows the base table hides. ENABLE / DISABLE on the
 * base never turns the history's row level security off; what the history shows follows the base.
 */
CREATE TEMP TABLE base_ddl (ord int, label text, command text, observe text, family regclass DEFAULT 'g123.doc');
INSERT INTO base_ddl (ord, label, command, observe) VALUES
    (1, 'create restrictive base policy',
        $$CREATE POLICY no_b ON g123.doc AS RESTRICTIVE FOR SELECT USING (tenant <> 'b')$$,
        $$SELECT g123.family_state('g123.doc')$$),
    (2, 'create permissive base policy',
        $$CREATE POLICY see_all ON g123.doc FOR SELECT TO g123_reader USING (true)$$,
        $$SELECT g123.reader_sees_tenant_b_history() || ', ' || g123.family_state('g123.doc')$$),
    (3, 'alter base policy qual',
        $$ALTER POLICY tenant_a ON g123.doc USING (tenant = 'a' AND id > 0)$$,
        $$SELECT g123.family_state('g123.doc')$$),
    (4, 'rename base policy',
        $$ALTER POLICY tenant_a ON g123.doc RENAME TO tenant_alpha$$,
        $$SELECT g123.family_state('g123.doc')$$),
    (5, 'drop base policy',
        $$DROP POLICY tenant_a ON g123.doc$$,
        $$SELECT g123.family_state('g123.doc')$$),
    (6, 'force base RLS',
        $$ALTER TABLE g123.doc FORCE ROW LEVEL SECURITY$$,
        $$SELECT g123.family_state('g123.doc')$$),
    (8, 'disable base RLS',
        $$ALTER TABLE g123.doc DISABLE ROW LEVEL SECURITY$$,
        $$SELECT g123.reader_sees_tenant_b_history() || ', ' || g123.family_state('g123.doc')$$),
    (9, 'disable then enable base RLS',
        $$ALTER TABLE g123.doc DISABLE ROW LEVEL SECURITY; ALTER TABLE g123.doc ENABLE ROW LEVEL SECURITY$$,
        $$SELECT g123.reader_sees_tenant_b_history() || ', ' || g123.family_state('g123.doc')$$);
INSERT INTO base_ddl (ord, label, command, observe, family) VALUES
    (7, 'no force base RLS',
        $$ALTER TABLE g123.forced NO FORCE ROW LEVEL SECURITY$$,
        $$SELECT g123.family_state('g123.forced')$$, 'g123.forced'),
    (10, 'disable base RLS of forced base',
        $$ALTER TABLE g123.forced DISABLE ROW LEVEL SECURITY$$,
        $$SELECT g123.family_state('g123.forced')$$, 'g123.forced'),
    (11, 'disable and no force base RLS',
        $$ALTER TABLE g123.forced DISABLE ROW LEVEL SECURITY, NO FORCE ROW LEVEL SECURITY$$,
        $$SELECT g123.family_state('g123.forced')$$, 'g123.forced'),
    (12, 'enable base RLS',
        $$ALTER TABLE g123.plain ENABLE ROW LEVEL SECURITY$$,
        $$SELECT g123.family_state('g123.plain')$$, 'g123.plain'),
    (13, 'enable and force base RLS',
        $$ALTER TABLE g123.plain ENABLE ROW LEVEL SECURITY, FORCE ROW LEVEL SECURITY$$,
        $$SELECT g123.family_state('g123.plain')$$, 'g123.plain'),
    (14, 'force base RLS while disabled',
        $$ALTER TABLE g123.plain FORCE ROW LEVEL SECURITY$$,
        $$SELECT g123.family_state('g123.plain')$$, 'g123.plain');

GRANT SELECT ON base_ddl TO g123_owner;

\echo '--- B: policy and RLS DDL on the base table reaches the history, whatever the GUC says'
SET ROLE g123_owner;
SELECT d.label AS base_ddl, g.label AS guc, g123.attempt(g.guard, d.family, d.command, d.observe) AS outcome
FROM base_ddl AS d CROSS JOIN guards AS g
ORDER BY d.ord, g.ord;
RESET ROLE;

/*
 * C. The legitimate paths that change history policies keep working.
 */
\echo '--- C1: sync_history_policies, also with the GUC set by hand'
SET ROLE g123_owner;
SELECT g.label AS guc,
       g123.attempt(g.guard, 'g123.doc', $$SELECT sql_saga.sync_history_policies('g123.doc')$$,
                    $$SELECT g123.family_state('g123.doc')$$) AS outcome
FROM guards AS g ORDER BY g.ord;
RESET ROLE;

\echo '--- C2: pg_restore replaying managed history policies of another installation (they are dropped)'
SET ROLE g123_owner;
SELECT r.label AS replayed, g123.attempt(NULL, 'g123.doc', r.command, $$SELECT g123.family_state('g123.doc')$$) AS outcome
FROM (VALUES
    (1, 'copy of another installation',
        format('CREATE POLICY %I ON g123.doc_history FOR SELECT USING (true)', 'sql_saga_' || left(md5('tenant_a' || 'othertoken'), 16))),
    (2, 'fixed reader policy of another installation',
        $$CREATE POLICY sql_saga_history_reader_othertoken ON g123.doc_history FOR SELECT USING (true)$$),
    (3, 'fixed base-select policy of another installation',
        $$CREATE POLICY sql_saga_history_base_select_othertoken ON g123.doc_history AS RESTRICTIVE FOR SELECT USING (true)$$)
) AS r(ord, label, command)
ORDER BY r.ord;
RESET ROLE;

\echo '--- C3: add_system_versioning of a base table with forced RLS and policies, then drop_system_versioning'
SET ROLE g123_owner;
CREATE TABLE g123.late (id int PRIMARY KEY, tenant text NOT NULL);
ALTER TABLE g123.late ENABLE ROW LEVEL SECURITY;
ALTER TABLE g123.late FORCE ROW LEVEL SECURITY;
CREATE POLICY mine ON g123.late FOR SELECT USING (tenant = current_user);
CREATE POLICY no_b ON g123.late AS RESTRICTIVE FOR ALL USING (tenant <> 'b');
SELECT g.label AS guc,
       g123.attempt(g.guard, 'g123.late', $$SELECT sql_saga.add_system_versioning('g123.late')$$,
                    $$SELECT g123.family_state('g123.late')$$) AS add_outcome
FROM guards AS g ORDER BY g.ord;
SELECT g.label AS guc,
       g123.attempt(g.guard, 'g123.forced', $$SELECT sql_saga.drop_system_versioning('g123.forced')$$,
                    $$SELECT g123.family_state('g123.forced') || ', history table ' || coalesce(to_regclass('g123.forced_history')::text, 'gone')$$) AS drop_outcome
FROM guards AS g ORDER BY g.ord;
RESET ROLE;

\echo '--- C4: each synchronization reports exactly what it did, once (no re-entrant synchronization)'
SET ROLE g123_owner;
SET client_min_messages = notice;
CREATE POLICY tenant_b ON g123.doc FOR SELECT TO g123_reader USING (tenant = 'b');
ALTER POLICY tenant_b ON g123.doc USING (tenant = 'b' AND id > 0);
DROP POLICY tenant_b ON g123.doc;
ALTER TABLE g123.doc FORCE ROW LEVEL SECURITY;
ALTER TABLE g123.doc NO FORCE ROW LEVEL SECURITY;
SELECT sql_saga.sync_history_policies('g123.doc');
SET client_min_messages = warning;
RESET ROLE;
-- Several copies to restore at once: each is created once, by the one synchronization.
SET client_min_messages = warning;
CREATE POLICY tenant_b ON g123.doc FOR SELECT TO g123_reader USING (tenant = 'b');
CREATE POLICY tenant_c ON g123.doc FOR SELECT TO g123_reader USING (tenant = 'c');
SET session_replication_role = replica; -- event triggers off: lose the copies behind sql_saga's back
SELECT format('DROP POLICY %I ON g123.doc_history', sql_saga.__internal_history_copy_name(n))
FROM unnest(ARRAY['tenant_a', 'tenant_b', 'tenant_c']::name[]) AS n \gexec
RESET session_replication_role;
SELECT g123.family_state('g123.doc') AS doc;
SET ROLE g123_owner;
SET client_min_messages = notice;
SELECT sql_saga.sync_history_policies('g123.doc');
SET client_min_messages = warning;
DROP POLICY tenant_b ON g123.doc;
DROP POLICY tenant_c ON g123.doc;
RESET ROLE;
SELECT g123.family_state('g123.doc') AS doc, g123.reader_sees_tenant_b_history() AS reader;

\echo '--- The guard never stays behind: no guard row and no setting left in this session'
SELECT count(*) AS guard_rows FROM sql_saga.history_policy_sync_guard;
SELECT coalesce(nullif(current_setting('sql_saga.managing_history_policies', true), ''), 'off') AS guc;

SET client_min_messages = warning;
DROP SCHEMA g123 CASCADE;
DROP TABLE guards, attacks, base_ddl;
DROP ROLE g123_owner;
DROP ROLE g123_reader;
RESET search_path;
RESET client_min_messages;

\i sql/include/test_teardown.sql
