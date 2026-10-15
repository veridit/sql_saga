\i sql/include/test_setup.sql

/*
 * Manual schema changes of a system-versioned table (tmp/design_manual_schema_changes.md, owner
 * answers in its section 12; tmp/design_final.md section 11).
 *
 * sql_saga never changes the base table B or its history table H by itself. The owner changes both
 * by hand and then calls sql_saga.rebuild_system_versioning_views(B), which recreates B_with_history,
 * B_history_gated and the four as-of functions from the current columns:
 *   - columns correspond by NAME, never by physical position;
 *   - every column of B must exist in H with the same type, typmod and collation; H may have more;
 *   - B_with_history shows B's columns in B's order, then the history-only columns in H's order;
 *     current rows show NULL for history-only columns, and history rows written from B get NULL;
 *   - without_columns leaves columns out of the views for one call, so that the owner can drop or
 *     rename them with plain ALTER TABLE; nothing is stored;
 *   - a write that needs history fails, with the column and the statement that fixes it, when B has a
 *     column H lacks or has with another type; rebuild refuses the same state with the same error;
 *   - the access shape (owner, ACL, security_invoker, gate, RLS branches) is the one
 *     add_system_versioning creates, and it stays protected outside rebuild.
 *
 * Shapes avoided here: a column dropped from B BEFORE add_system_versioning (history then has the
 * range column at another attnum), a separate pair.
 */
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;

CREATE ROLE ms_other;     -- neither owner nor member of the owner role
CREATE ROLE ms_reader;    -- reads pa through a policy
CREATE ROLE ms_a;         -- tenant a of rt
CREATE ROLE ms_none;      -- no privilege on rt at all
CREATE ROLE ms_owner;     -- non-superuser owner of rt
GRANT USAGE ON SCHEMA public TO ms_other, ms_reader, ms_a, ms_none, ms_owner;
-- A fixed installation token makes the policy copy names in NOTICEs deterministic.
UPDATE sql_saga.installation SET token = 'test0001';

CREATE TABLE ms_plain (id int PRIMARY KEY);
CREATE TABLE ms (id int PRIMARY KEY, payload text);
SELECT sql_saga.add_system_versioning('ms');
INSERT INTO ms VALUES (1, 'one'), (2, 'two');
UPDATE ms SET payload = payload || '+' WHERE id = 1;
RESET client_min_messages;

-- The objects of a family, by their registered names (the gated view is named after the history).
CREATE FUNCTION pg_temp.family_objects(base regclass)
RETURNS TABLE (ord integer, kind text, objoid oid)
LANGUAGE sql AS $$
    SELECT u.ord, u.kind, u.objoid
    FROM sql_saga.system_versioning AS sv
    CROSS JOIN LATERAL (VALUES
        (1, 'view', to_regclass(format('%I.%I', sv.view_schema_name, sv.view_table_name))::oid),
        (2, 'view', to_regclass(format('%I.%I', sv.history_schema_name, sv.history_table_name || '_gated'))::oid),
        (3, 'function', to_regprocedure(sv.func_as_of)::oid),
        (4, 'function', to_regprocedure(sv.func_between)::oid),
        (5, 'function', to_regprocedure(sv.func_between_symmetric)::oid),
        (6, 'function', to_regprocedure(sv.func_from_to)::oid)) AS u (ord, kind, objoid)
    WHERE to_regclass(format('%I.%I', sv.table_schema, sv.table_name)) = base;
$$;

-- Columns of the two views, with types, in the order the views expose them.
CREATE FUNCTION pg_temp.family_columns(base regclass)
RETURNS TABLE (view_name name, columns text)
LANGUAGE sql AS $$
    SELECT c.relname,
           (SELECT string_agg(format('%I %s', a.attname, format_type(a.atttypid, a.atttypmod)), ', ' ORDER BY a.attnum)
              FROM pg_attribute AS a WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped)
    FROM pg_temp.family_objects(base) AS f JOIN pg_class AS c ON c.oid = f.objoid
    WHERE f.kind = 'view'
    ORDER BY f.ord;
$$;

-- Runs the four as-of functions and reports the rows and the column names they return.
CREATE FUNCTION pg_temp.query_functions(base regclass)
RETURNS TABLE (function_name name, row_count bigint, result_columns text)
LANGUAGE plpgsql AS $$
DECLARE
    f record;
    args text;
BEGIN
    FOR f IN SELECT o.ord, p.oid, p.proname FROM pg_temp.family_objects(base) AS o JOIN pg_proc AS p ON p.oid = o.objoid ORDER BY o.ord LOOP
        args := CASE f.ord WHEN 3 THEN 'now()'
                           WHEN 5 THEN '''infinity'', ''-infinity'''
                           ELSE '''-infinity'', ''infinity''' END;
        function_name := f.proname;
        EXECUTE format($q$
            SELECT count(*),
                   (SELECT string_agg(k.key, ', ' ORDER BY k.n)
                      FROM json_object_keys((array_agg(row_to_json(r)))[1]) WITH ORDINALITY AS k (key, n))
            FROM %s(%s) AS r$q$, f.oid::regproc, args)
            INTO row_count, result_columns;
        RETURN NEXT;
    END LOOP;
END;
$$;

-- Owner, options and privileges of every object of the family.
CREATE FUNCTION pg_temp.access_shape(base regclass)
RETURNS TABLE (object_name name, owned_by_base_owner boolean, options text, public_privileges text, other_grantees bigint)
LANGUAGE sql AS $$
    SELECT coalesce(c.relname, p.proname),
           coalesce(c.relowner, p.proowner) = (SELECT b.relowner FROM pg_class AS b WHERE b.oid = base),
           CASE WHEN f.kind = 'view' THEN coalesce(array_to_string(c.reloptions, ','), '')
                WHEN p.prosecdef THEN 'SECURITY DEFINER' ELSE 'SECURITY INVOKER' END,
           (SELECT string_agg(x.privilege_type, ',' ORDER BY x.privilege_type COLLATE "C")
              FROM aclexplode(coalesce(c.relacl, p.proacl)) AS x WHERE x.grantee = 0),
           (SELECT count(*) FROM aclexplode(coalesce(c.relacl, p.proacl)) AS x
             WHERE x.grantee NOT IN (0, coalesce(c.relowner, p.proowner)))
    FROM pg_temp.family_objects(base) AS f
    LEFT JOIN pg_class AS c ON f.kind = 'view' AND c.oid = f.objoid
    LEFT JOIN pg_proc AS p ON f.kind = 'function' AND p.oid = f.objoid
    ORDER BY f.ord;
$$;

-- Everything about the family's views and functions that a rebuild may not change by accident:
-- definitions, columns, owners, options and privileges (not OIDs, which a rebuild renews).
CREATE FUNCTION pg_temp.fingerprint(base regclass)
RETURNS text
LANGUAGE sql AS $$
    SELECT string_agg(
        CASE f.kind
        WHEN 'view' THEN concat_ws(' | ', c.relname, c.relowner::regrole::text, c.reloptions::text,
                                   pg_get_viewdef(c.oid),
                                   (SELECT string_agg(format('%s %s', a.attname, format_type(a.atttypid, a.atttypmod)), ',' ORDER BY a.attnum)
                                      FROM pg_attribute AS a WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped),
                                   (SELECT string_agg(format('%s:%s', x.grantee, x.privilege_type), ',' ORDER BY x.grantee, x.privilege_type COLLATE "C")
                                      FROM aclexplode(c.relacl) AS x))
        ELSE concat_ws(' | ', p.proowner::regrole::text, pg_get_functiondef(p.oid),
                       (SELECT string_agg(format('%s:%s', x.grantee, x.privilege_type), ',' ORDER BY x.grantee, x.privilege_type COLLATE "C")
                          FROM aclexplode(p.proacl) AS x))
        END, E'\n' ORDER BY f.ord)
    FROM pg_temp.family_objects(base) AS f
    LEFT JOIN pg_class AS c ON f.kind = 'view' AND c.oid = f.objoid
    LEFT JOIN pg_proc AS p ON f.kind = 'function' AND p.oid = f.objoid;
$$;

SELECT pg_temp.fingerprint('ms') AS fp_added \gset

\echo '--- 2. Rebuilding an unchanged family is idempotent and reproduces add_system_versioning'
SELECT table_name, history_table_name, view_table_name, func_as_of, func_between, func_between_symmetric, func_from_to
FROM sql_saga.system_versioning WHERE table_name = 'ms';
SELECT sql_saga.rebuild_system_versioning_views('ms');
SELECT pg_temp.fingerprint('ms') = :'fp_added' AS same_as_add_system_versioning;
SELECT sql_saga.rebuild_system_versioning_views('ms');
SELECT pg_temp.fingerprint('ms') = :'fp_added' AS same_after_second_rebuild;
SELECT table_name, history_table_name, view_table_name, func_as_of, func_between, func_between_symmetric, func_from_to
FROM sql_saga.system_versioning WHERE table_name = 'ms';
SELECT * FROM pg_temp.family_columns('ms');
SELECT * FROM pg_temp.access_shape('ms');
SELECT * FROM pg_temp.query_functions('ms');

\echo '--- 1 and 17. Arguments and authorization are checked first; a refused call changes nothing'
SELECT sql_saga.rebuild_system_versioning_views(NULL); -- fail
SELECT sql_saga.rebuild_system_versioning_views('ms_plain'); -- fail
SET ROLE ms_other;
SELECT sql_saga.rebuild_system_versioning_views('ms'); -- fail
\echo :LAST_ERROR_SQLSTATE
RESET ROLE;
SELECT sql_saga.rebuild_system_versioning_views('ms', ARRAY[NULL]::name[]); -- fail
SELECT sql_saga.rebuild_system_versioning_views('ms', ARRAY['payload', 'payload']); -- fail
SELECT sql_saga.rebuild_system_versioning_views('ms', ARRAY['no_such_column']); -- fail
-- Every as-of function filters on the system time column, so it cannot be left out.
SELECT sql_saga.rebuild_system_versioning_views('ms', ARRAY['system_valid_range']); -- fail
SELECT pg_temp.fingerprint('ms') = :'fp_added' AS unchanged_by_refused_calls;

\echo '--- 14. Outside rebuild, the views and functions stay protected'
-- CASCADE gets past PostgreSQL's own dependency check, so sql_saga's protection is what refuses.
-- PostgreSQL's preliminary "drop cascades to N other objects" NOTICE is not part of the design.
SET client_min_messages = warning;
DROP VIEW ms_with_history CASCADE; -- fail
DROP VIEW ms_history_gated CASCADE; -- fail
DROP FUNCTION ms__as_of(timestamp with time zone); -- fail
RESET client_min_messages;
ALTER VIEW ms_with_history SET (security_invoker = off); -- fail
CREATE OR REPLACE VIEW ms_history_gated AS SELECT * FROM ms_history; -- fail: the gate would be lost
-- A stray grant is not part of the fixed shape: a rebuild does not carry it over, on any of the objects.
GRANT INSERT ON ms_with_history TO ms_other;
GRANT SELECT ON ms_history_gated TO ms_other;
GRANT EXECUTE ON FUNCTION ms__as_of(timestamp with time zone) TO ms_other;
GRANT EXECUTE ON FUNCTION ms__between(timestamp with time zone, timestamp with time zone) TO ms_other;
GRANT EXECUTE ON FUNCTION ms__between_symmetric(timestamp with time zone, timestamp with time zone) TO ms_other;
GRANT EXECUTE ON FUNCTION ms__from_to(timestamp with time zone, timestamp with time zone) TO ms_other;
SELECT object_name, other_grantees FROM pg_temp.access_shape('ms');
SELECT sql_saga.rebuild_system_versioning_views('ms');
SELECT object_name, other_grantees FROM pg_temp.access_shape('ms');
SELECT pg_temp.fingerprint('ms') = :'fp_added' AS unchanged;

\echo '--- 3. A column added by hand to both tables appears in the views after the rebuild'
ALTER TABLE ms ADD COLUMN extra text;
ALTER TABLE ms_history ADD COLUMN extra text;
-- Nothing is propagated: the views are as before.
SELECT * FROM pg_temp.family_columns('ms');
SELECT sql_saga.rebuild_system_versioning_views('ms');
SELECT * FROM pg_temp.family_columns('ms');
SELECT * FROM pg_temp.query_functions('ms');
UPDATE ms SET extra = 'x2' WHERE id = 2;
UPDATE ms SET payload = 'two+' WHERE id = 2;
SELECT id, payload, extra FROM ms_history WHERE id = 2 ORDER BY system_valid_range;

\echo '--- 4. A column only in the history table: NULL for current rows and for new history rows'
ALTER TABLE ms_history ADD COLUMN legacy_note text;
UPDATE ms_history SET legacy_note = 'from the old system' WHERE id = 1;
SELECT sql_saga.rebuild_system_versioning_views('ms');
SELECT * FROM pg_temp.family_columns('ms');
SELECT id, payload, extra, legacy_note FROM ms_with_history ORDER BY id, system_valid_range;
UPDATE ms SET payload = 'one++' WHERE id = 1;
SELECT id, payload, legacy_note FROM ms_history WHERE id = 1 ORDER BY system_valid_range;
SELECT * FROM pg_temp.query_functions('ms');

\echo '--- 5. Columns match by name, whatever their physical positions'
SET client_min_messages = warning;
CREATE TABLE pa (id int PRIMARY KEY, a text);
SELECT sql_saga.add_system_versioning('pa');
RESET client_min_messages;
-- Added in another order, and after a dropped slot, in the history.
ALTER TABLE pa ADD COLUMN p text, ADD COLUMN q text;
ALTER TABLE pa_history ADD COLUMN gap text;
ALTER TABLE pa_history DROP COLUMN gap;
ALTER TABLE pa_history ADD COLUMN q text, ADD COLUMN p text;
SELECT b.attname AS column_name, b.attnum AS base_attnum, h.attnum AS history_attnum
FROM pg_attribute AS b
JOIN pg_attribute AS h ON h.attrelid = 'pa_history'::regclass AND h.attname = b.attname AND NOT h.attisdropped
WHERE b.attrelid = 'pa'::regclass AND b.attnum > 0 AND NOT b.attisdropped
ORDER BY b.attnum;
SELECT sql_saga.rebuild_system_versioning_views('pa');
SELECT * FROM pg_temp.family_columns('pa');
INSERT INTO pa (id, a, p, q) VALUES (1, 'a1', 'p1', 'keep'), (2, 'a2', 'p2', 'skip');
UPDATE pa SET a = a || '+';
SELECT id, a, p, q FROM pa_history ORDER BY id;
-- A policy on q, copied to the history by name, filters history rows by q.
GRANT SELECT ON pa TO ms_reader;
ALTER TABLE pa ENABLE ROW LEVEL SECURITY;
CREATE POLICY by_q ON pa FOR SELECT TO ms_reader USING (q = 'keep');
SET ROLE ms_reader;
SELECT id, a, p, q FROM pa_with_history ORDER BY id, system_valid_range;
RESET ROLE;

\echo '--- 7. Dropping a column: leave it out of the views, drop it from both tables, rebuild'
ALTER TABLE ms ADD COLUMN obsolete text;
ALTER TABLE ms_history ADD COLUMN obsolete text;
SELECT sql_saga.rebuild_system_versioning_views('ms');
ALTER TABLE ms DROP COLUMN obsolete; -- fail: the views use it
SELECT sql_saga.rebuild_system_versioning_views('ms', ARRAY['obsolete']);
-- Still in both tables, no longer in the views.
SELECT * FROM pg_temp.family_columns('ms');
ALTER TABLE ms DROP COLUMN obsolete;
ALTER TABLE ms_history DROP COLUMN obsolete;
SELECT sql_saga.rebuild_system_versioning_views('ms');
SELECT * FROM pg_temp.family_columns('ms');
SELECT * FROM pg_temp.access_shape('ms');
SELECT * FROM pg_temp.query_functions('ms');

\echo '--- 8. Dropping a history-only column works the same way'
ALTER TABLE ms_history ADD COLUMN history_annotation text;
SELECT sql_saga.rebuild_system_versioning_views('ms');
SELECT * FROM pg_temp.family_columns('ms') WHERE view_name = 'ms_with_history';
ALTER TABLE ms_history DROP COLUMN history_annotation; -- fail: the views use it
SELECT sql_saga.rebuild_system_versioning_views('ms', ARRAY['history_annotation']);
ALTER TABLE ms_history DROP COLUMN history_annotation;
SELECT sql_saga.rebuild_system_versioning_views('ms');
SELECT * FROM pg_temp.family_columns('ms');

\echo '--- 9. A base column missing from the history: inserts work, history writes fail with the fix'
ALTER TABLE ms ADD COLUMN missing_in_history integer;
-- An insert writes no history.
INSERT INTO ms (id, payload, missing_in_history) VALUES (3, 'three', 3);
SELECT count(*) AS history_rows_before FROM ms_history;
\set VERBOSITY default
UPDATE ms SET payload = 'two++' WHERE id = 2; -- fail
\echo :LAST_ERROR_SQLSTATE
DELETE FROM ms WHERE id = 3; -- fail
SELECT sql_saga.rebuild_system_versioning_views('ms'); -- fail: the same state is refused
\set VERBOSITY terse
-- Nothing was written.
SELECT id, payload, missing_in_history FROM ms ORDER BY id;
SELECT count(*) AS history_rows_after FROM ms_history;
SELECT * FROM pg_temp.family_columns('ms') WHERE view_name = 'ms_with_history';
-- The fix from the HINT.
ALTER TABLE ms_history ADD COLUMN missing_in_history integer;
SELECT sql_saga.rebuild_system_versioning_views('ms');
UPDATE ms SET payload = 'three+', missing_in_history = 30 WHERE id = 3;
SELECT id, payload, missing_in_history FROM ms_history WHERE id = 3 ORDER BY system_valid_range;
SELECT * FROM pg_temp.family_columns('ms') WHERE view_name = 'ms_with_history';

\echo '--- 10. Same name, another type: history writes and rebuild fail with the fix'
ALTER TABLE ms ADD COLUMN mismatch bigint;
ALTER TABLE ms_history ADD COLUMN mismatch integer;
\set VERBOSITY default
UPDATE ms SET payload = 'two++' WHERE id = 2; -- fail
\echo :LAST_ERROR_SQLSTATE
SELECT sql_saga.rebuild_system_versioning_views('ms'); -- fail
\set VERBOSITY terse
SELECT id, payload FROM ms WHERE id = 2;
ALTER TABLE ms_history ALTER COLUMN mismatch TYPE bigint;
SELECT sql_saga.rebuild_system_versioning_views('ms');
UPDATE ms SET payload = 'two++', mismatch = 2 WHERE id = 2;
UPDATE ms SET payload = 'two+++' WHERE id = 2;
SELECT id, payload, mismatch FROM ms_history WHERE id = 2 ORDER BY system_valid_range;

\echo '--- 10b. User-defined types are schema-qualified identically for writes and rebuilds'
CREATE DOMAIN public.visible_domain AS text;
SET search_path = public;
CREATE TABLE ms_domain (id int PRIMARY KEY, payload visible_domain);
SELECT sql_saga.add_system_versioning('ms_domain');
INSERT INTO ms_domain VALUES (1, 'one');
SELECT sql_saga.rebuild_system_versioning_views('ms_domain', ARRAY['payload']);
ALTER TABLE ms_domain_history ALTER COLUMN payload TYPE text;

CREATE FUNCTION pg_temp.capture_domain_mismatch(operation text)
RETURNS TABLE (message text, hint text)
LANGUAGE plpgsql AS $$
BEGIN
    IF operation = 'write' THEN
        UPDATE ms_domain SET payload = 'two' WHERE id = 1;
    ELSE
        PERFORM sql_saga.rebuild_system_versioning_views('ms_domain');
    END IF;
EXCEPTION WHEN datatype_mismatch THEN
    GET STACKED DIAGNOSTICS message = MESSAGE_TEXT, hint = PG_EXCEPTION_HINT;
    RETURN NEXT;
END;
$$;

CREATE TEMP TABLE domain_mismatch AS
SELECT operation, message, hint
FROM (VALUES ('write'), ('rebuild')) AS v (operation)
CROSS JOIN LATERAL pg_temp.capture_domain_mismatch(operation);

TABLE domain_mismatch;
SELECT count(DISTINCT (message, hint)) = 1 AS byte_identical,
       bool_and(message LIKE '%public.visible_domain%') AS error_schema_qualified,
       bool_and(hint LIKE '%TYPE public.visible_domain;%') AS hint_schema_qualified
FROM domain_mismatch;

-- The DDL portion of the HINT works even when no user schema is on search_path.
SET search_path = pg_catalog;
DO $execute_hint$
DECLARE
    ddl text;
BEGIN
    SELECT split_part(substring(hint FROM 6), ';', 1) INTO ddl
    FROM pg_temp.domain_mismatch
    LIMIT 1;
    EXECUTE ddl;
END;
$execute_hint$;
SET search_path = public;
SELECT sql_saga.rebuild_system_versioning_views('ms_domain');
UPDATE ms_domain SET payload = 'two' WHERE id = 1;
SELECT id, payload::text FROM ms_domain_history ORDER BY system_valid_range;

-- A type in sql_saga's own schema is schema-qualified too, so its HINT also runs without sql_saga on the path.
ALTER TABLE ms_domain ADD COLUMN behavior sql_saga.drop_behavior;
ALTER TABLE ms_domain_history ADD COLUMN behavior text;
CREATE TEMP TABLE saga_type_mismatch AS
SELECT operation, message, hint
FROM (VALUES ('write'), ('rebuild')) AS v (operation)
CROSS JOIN LATERAL pg_temp.capture_domain_mismatch(operation);
TABLE saga_type_mismatch;
SELECT count(DISTINCT (message, hint)) = 1 AS byte_identical,
       bool_and(hint LIKE '%TYPE sql_saga.drop_behavior;%') AS hint_schema_qualified
FROM saga_type_mismatch;
SET search_path = pg_catalog;
DO $execute_hint$
DECLARE
    ddl text;
BEGIN
    -- text to an enum has no automatic cast, so the user adds USING to the suggested ALTER.
    SELECT split_part(substring(hint FROM 6), ';', 1) || ' USING behavior::sql_saga.drop_behavior' INTO ddl
    FROM pg_temp.saga_type_mismatch
    LIMIT 1;
    EXECUTE ddl;
END;
$execute_hint$;
SET search_path = public;
SELECT sql_saga.rebuild_system_versioning_views('ms_domain');
UPDATE ms_domain SET behavior = 'CASCADE' WHERE id = 1;
UPDATE ms_domain SET behavior = 'RESTRICT' WHERE id = 1; -- history keeps the replaced CASCADE version
SELECT id, behavior::text FROM ms_domain_history WHERE behavior IS NOT NULL ORDER BY system_valid_range;

\echo '--- 11. A typmod or collation difference is a type difference too'
ALTER TABLE ms ADD COLUMN code varchar(20);
ALTER TABLE ms_history ADD COLUMN code varchar(30);
\set VERBOSITY default
UPDATE ms SET payload = 'one+++' WHERE id = 1; -- fail
\set VERBOSITY terse
ALTER TABLE ms_history ALTER COLUMN code TYPE varchar(20);
ALTER TABLE ms ADD COLUMN label text COLLATE "C";
ALTER TABLE ms_history ADD COLUMN label text COLLATE "POSIX";
\set VERBOSITY default
UPDATE ms SET payload = 'one+++' WHERE id = 1; -- fail
SELECT sql_saga.rebuild_system_versioning_views('ms'); -- fail
\set VERBOSITY terse
ALTER TABLE ms_history ALTER COLUMN label TYPE text COLLATE "C";
SELECT sql_saga.rebuild_system_versioning_views('ms');
UPDATE ms SET payload = 'one+++', code = 'c1', label = 'l1' WHERE id = 1;
UPDATE ms SET payload = 'one++++' WHERE id = 1;
SELECT id, payload, code, label FROM ms_history WHERE id = 1 ORDER BY system_valid_range;
SELECT * FROM pg_temp.family_columns('ms') WHERE view_name = 'ms_with_history';

\echo '--- 11b. Changing a column type: an excluded column is not checked by rebuild, but writes still are'
-- without_columns only shapes the views. It is not stored, so the history writer does not know about
-- it and never leaves out a base value: a write in the intermediate state fails with the fix.
SELECT sql_saga.rebuild_system_versioning_views('ms', ARRAY['code']);
ALTER TABLE ms ALTER COLUMN code TYPE varchar(40);
-- code now differs between ms and ms_history; leaving it out, the rebuild succeeds.
SELECT sql_saga.rebuild_system_versioning_views('ms', ARRAY['code']);
SELECT * FROM pg_temp.family_columns('ms') WHERE view_name = 'ms_with_history';
\set VERBOSITY default
UPDATE ms SET payload = 'one+++++', code = 'c2' WHERE id = 1; -- fail
\echo :LAST_ERROR_SQLSTATE
\set VERBOSITY terse
SELECT id, payload, code FROM ms WHERE id = 1;
ALTER TABLE ms_history ALTER COLUMN code TYPE varchar(40);
SELECT sql_saga.rebuild_system_versioning_views('ms');
UPDATE ms SET payload = 'one+++++', code = 'c2' WHERE id = 1;
UPDATE ms SET payload = 'one++++++' WHERE id = 1;
SELECT id, payload, code FROM ms_history WHERE id = 1 ORDER BY system_valid_range;
SELECT * FROM pg_temp.family_columns('ms') WHERE view_name = 'ms_with_history';

\echo '--- 12. History-only columns get NULL: no default is applied, constraints are the owner''s'
ALTER TABLE ms_history ADD COLUMN with_default text DEFAULT 'dflt';
UPDATE ms SET payload = 'three++' WHERE id = 3;
-- Rows that existed when the column was added read the default; the new history row has NULL.
SELECT id, payload, with_default FROM ms_history WHERE id = 3 ORDER BY system_valid_range;
ALTER TABLE ms_history ADD COLUMN must_have text NOT NULL DEFAULT 'old';
ALTER TABLE ms_history ALTER COLUMN must_have DROP DEFAULT;
UPDATE ms SET payload = 'three+++' WHERE id = 3; -- fail: PostgreSQL's own NOT NULL error
SELECT id, payload FROM ms WHERE id = 3;
ALTER TABLE ms_history DROP COLUMN must_have, DROP COLUMN with_default;
UPDATE ms SET payload = 'three+++' WHERE id = 3;

\echo '--- 15. A rebuild is part of the transaction: rolled back, the old views are back'
SELECT pg_temp.fingerprint('ms') AS fp_before_tx \gset
BEGIN;
SELECT sql_saga.rebuild_system_versioning_views('ms', ARRAY['extra']);
SELECT * FROM pg_temp.family_columns('ms') WHERE view_name = 'ms_with_history';
ALTER TABLE ms DROP COLUMN no_such_column; -- fail
ROLLBACK;
SELECT pg_temp.fingerprint('ms') = :'fp_before_tx' AS restored;
SELECT * FROM pg_temp.family_columns('ms') WHERE view_name = 'ms_with_history';
SELECT * FROM pg_temp.query_functions('ms');

\echo '--- A user object that depends on the views blocks the rebuild with PostgreSQL''s own error, never CASCADE'
CREATE VIEW ms_report AS SELECT id, payload FROM ms_with_history;
DO $$
BEGIN
    PERFORM sql_saga.rebuild_system_versioning_views('ms');
    RAISE EXCEPTION 'rebuild succeeded';
EXCEPTION WHEN dependent_objects_still_exist THEN
    RAISE NOTICE 'rebuild refused: SQLSTATE %', SQLSTATE;
END;
$$;
SELECT count(*) AS report_rows FROM ms_report;
SELECT pg_temp.fingerprint('ms') = :'fp_before_tx' AS unchanged;
DROP VIEW ms_report;

\echo '--- 6 and 13. Row-level security survives rebuilds, and a column a policy uses is renamed'
SET client_min_messages = warning;
CREATE TABLE rt (id int PRIMARY KEY, tenant text NOT NULL, body text);
SELECT sql_saga.add_system_versioning('rt');
INSERT INTO rt VALUES (1, 'a', 'a1'), (2, 'b', 'b1');
UPDATE rt SET body = body || '+';
GRANT SELECT ON rt TO ms_a;
ALTER TABLE rt ENABLE ROW LEVEL SECURITY;
RESET client_min_messages;
CREATE POLICY tenant_a ON rt FOR SELECT TO ms_a USING (tenant = 'a');
ALTER TABLE rt_history ADD COLUMN legacy text;
UPDATE rt_history SET legacy = 'old ' || tenant;
SELECT sql_saga.rebuild_system_versioning_views('rt');
SELECT * FROM pg_temp.family_columns('rt');
SET ROLE ms_a;
SELECT id, tenant, body, legacy FROM rt_with_history ORDER BY id, system_valid_range;
SELECT id, tenant, body, legacy FROM rt__between('-infinity', 'infinity') ORDER BY id, system_valid_range;
RESET ROLE;
SET ROLE ms_none;
SELECT count(*) FROM rt_with_history; -- fail
SELECT count(*) AS direct_history_rows FROM rt_history;
RESET ROLE;
-- Rename tenant to account. Renaming the base first is refused: the history has no account yet,
-- so the policy cannot be copied (fail closed, never a wrong filter). The sync checks every copy before
-- it changes anything, so the refusal is the only message: no NOTICE about a change that is not made.
SELECT sql_saga.rebuild_system_versioning_views('rt', ARRAY['tenant']);
ALTER TABLE rt RENAME COLUMN tenant TO account; -- fail
ALTER TABLE rt_history RENAME COLUMN tenant TO account;
ALTER TABLE rt RENAME COLUMN tenant TO account;
SELECT sql_saga.rebuild_system_versioning_views('rt');
SELECT * FROM pg_temp.family_columns('rt');
SELECT * FROM pg_temp.query_functions('rt');
SELECT pg_get_expr(p.polqual, p.polrelid) AS copy_qual
FROM pg_policy AS p WHERE p.polrelid = 'rt_history'::regclass AND p.polname = 'sql_saga_bf833db18461d646';
UPDATE rt SET body = 'a3' WHERE id = 1;
SET ROLE ms_a;
SELECT id, account, body, legacy FROM rt_with_history ORDER BY id, system_valid_range;
RESET ROLE;
SELECT id, account, body FROM rt_history ORDER BY id, system_valid_range;
-- A copy missing because event triggers were off still fails closed; rebuild also synchronizes.
SET session_replication_role = replica;
CREATE POLICY late ON rt FOR SELECT TO ms_a USING (account = 'a');
RESET session_replication_role;
\set VERBOSITY default
SET ROLE ms_a;
SELECT count(*) FROM rt_with_history; -- fail
RESET ROLE;
\set VERBOSITY terse
SELECT sql_saga.rebuild_system_versioning_views('rt');
SET ROLE ms_a;
SELECT count(*) AS tenant_a_rows FROM rt_with_history;
RESET ROLE;
-- A non-superuser owner may rebuild; the objects stay its own, with the same access shape.
ALTER TABLE rt OWNER TO ms_owner;
SET ROLE ms_owner;
SELECT sql_saga.rebuild_system_versioning_views('rt');
RESET ROLE;
SELECT relowner::regrole AS rt_owner FROM pg_class WHERE oid = 'rt'::regclass;
SELECT * FROM pg_temp.access_shape('rt');
-- FORCE ROW LEVEL SECURITY still filters the owner on current rows and on history alike.
ALTER TABLE rt FORCE ROW LEVEL SECURITY;
SET ROLE ms_owner;
SELECT count(*) AS forced_owner_rows FROM rt_with_history;
RESET ROLE;
ALTER TABLE rt NO FORCE ROW LEVEL SECURITY;
SET ROLE ms_owner;
SELECT count(*) AS owner_rows FROM rt_with_history;
RESET ROLE;
ALTER TABLE rt OWNER TO CURRENT_USER;

\echo '--- 16. Registered custom names, another schema, identifiers that need quoting'
SET client_min_messages = warning;
CREATE SCHEMA ms_s;
CREATE TABLE ms_s."Ledger" ("Id" int PRIMARY KEY, "Amount" numeric, "Note" text);
SELECT sql_saga.add_system_versioning('ms_s."Ledger"',
    history_table_name => 'Ledger_hist', view_name => 'Ledger_all',
    function_as_of_name => 'ledger_at', function_between_name => 'ledger_between',
    function_between_symmetric_name => 'ledger_between_sym', function_from_to_name => 'ledger_from_to');
INSERT INTO ms_s."Ledger" VALUES (1, 10, 'first');
UPDATE ms_s."Ledger" SET "Amount" = 11;
RESET client_min_messages;
ALTER TABLE ms_s."Ledger_hist" ADD COLUMN "Legacy Ref" text;
SELECT sql_saga.rebuild_system_versioning_views('ms_s."Ledger"', ARRAY['Note']);
ALTER TABLE ms_s."Ledger" RENAME COLUMN "Note" TO "Memo";
ALTER TABLE ms_s."Ledger_hist" RENAME COLUMN "Note" TO "Memo";
SELECT sql_saga.rebuild_system_versioning_views('ms_s."Ledger"');
SELECT history_schema_name, history_table_name, view_table_name, func_as_of, func_between_symmetric
FROM sql_saga.system_versioning WHERE table_schema = 'ms_s';
SELECT * FROM pg_temp.family_columns('ms_s."Ledger"');
SELECT * FROM pg_temp.access_shape('ms_s."Ledger"');
SELECT * FROM pg_temp.query_functions('ms_s."Ledger"');
UPDATE ms_s."Ledger" SET "Memo" = 'second';
SELECT "Id", "Amount", "Memo", "Legacy Ref" FROM ms_s."Ledger_all" ORDER BY system_valid_range;

/* Clean up */
SET client_min_messages = warning;
SELECT sql_saga.drop_system_versioning('ms');
SELECT sql_saga.drop_system_versioning('pa');
SELECT sql_saga.drop_system_versioning('rt');
SELECT sql_saga.drop_system_versioning('ms_s."Ledger"');
SELECT sql_saga.drop_system_versioning('ms_domain');
DROP TABLE ms, ms_plain, pa, rt, ms_domain;
DROP SCHEMA ms_s CASCADE;
DROP DOMAIN public.visible_domain;
DROP OWNED BY ms_other, ms_reader, ms_a, ms_none, ms_owner;
DROP ROLE ms_other, ms_reader, ms_a, ms_none, ms_owner;
RESET client_min_messages;

\i sql/include/test_teardown.sql
