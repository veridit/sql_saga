/*
 * Access-time authority and row-level security on history (tmp/design_final.md).
 *
 * Derived objects hold no copy of who may use them: views are security_invoker and granted to
 * PUBLIC, so PostgreSQL checks the caller's rights on the base table when the data is used.
 * History is reached through B_with_history, whose body is
 *     SELECT .. FROM B
 *     UNION ALL SELECT .. FROM B_history_gated                     -- caller not subject to B's RLS
 *     UNION ALL SELECT .. FROM B_history WHERE row_security_active(B)   -- caller subject to B's RLS
 * B_history_gated is owner-rights and raises 42501 unless the caller may SELECT B. B_history is
 * granted SELECT to PUBLIC, has RLS enabled (forced exactly when B is), and carries:
 *   sql_saga_history_reader       PERMISSIVE: SELECT on B and B's RLS not active for the caller
 *   sql_saga_history_base_select  RESTRICTIVE: SELECT on B (keeps roles without it out entirely)
 *   sql_saga_<16 hex>             one FOR SELECT copy of every SELECT/ALL policy on B; the name is
 *                                 left(md5(<base policy name> || <installation token>), 16)
 */

-- Per-installation token. Deliberately NOT an extension configuration table: pg_dump does not dump
-- it, so a restore target mints its own token and dumped copies (old token) never collide with the
-- copies sql_saga creates there.
CREATE TABLE sql_saga.installation (token text NOT NULL);
CREATE UNIQUE INDEX installation_singleton ON sql_saga.installation ((true));
INSERT INTO sql_saga.installation (token) VALUES (left(md5(random()::text || clock_timestamp()::text), 8));
GRANT SELECT ON TABLE sql_saga.installation TO PUBLIC;
COMMENT ON TABLE sql_saga.installation IS 'Per-installation token used in the names of history policy copies. Not dumped: every installation, including a restore target, has its own.';

CREATE FUNCTION sql_saga.__internal_installation_token()
RETURNS text
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_installation_token$
    SELECT i.token FROM sql_saga.installation AS i LIMIT 1;
$__internal_installation_token$;
GRANT EXECUTE ON FUNCTION sql_saga.__internal_installation_token() TO PUBLIC;

CREATE FUNCTION sql_saga.__internal_history_copy_name(base_policy_name name)
RETURNS name
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_history_copy_name$
    SELECT ('sql_saga_' || left(md5(base_policy_name::text || sql_saga.__internal_installation_token()), 16))::name;
$__internal_history_copy_name$;
GRANT EXECUTE ON FUNCTION sql_saga.__internal_history_copy_name(name) TO PUBLIC;

-- Names of the two fixed policies on every history table. Like the copies, they carry the
-- installation token, so that pg_restore replaying the dumped ones (old token) never collides with
-- the ones sql_saga creates in the restore target; the dumped ones are then dropped as stale.
CREATE FUNCTION sql_saga.__internal_history_reader_policy_name()
RETURNS name
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_history_reader_policy_name$
    SELECT ('sql_saga_history_reader_' || sql_saga.__internal_installation_token())::name;
$__internal_history_reader_policy_name$;

CREATE FUNCTION sql_saga.__internal_history_base_select_policy_name()
RETURNS name
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_history_base_select_policy_name$
    SELECT ('sql_saga_history_base_select_' || sql_saga.__internal_installation_token())::name;
$__internal_history_base_select_policy_name$;

-- Is this policy on a history table managed by sql_saga (fixed policy of any installation, or a copy)?
CREATE FUNCTION sql_saga.__internal_is_managed_history_policy(policy_name name)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_is_managed_history_policy$
    SELECT policy_name ~ '^sql_saga_([0-9a-f]{16}|history_reader_[0-9a-z]+|history_base_select_[0-9a-z]+)$';
$__internal_is_managed_history_policy$;
GRANT EXECUTE ON FUNCTION sql_saga.__internal_is_managed_history_policy(name) TO PUBLIC;

-- The access gate of B_history_gated: raises the same error PostgreSQL raises for the base.
CREATE FUNCTION sql_saga.__internal_deny_history(table_oid oid)
RETURNS boolean
LANGUAGE plpgsql
STABLE
AS $__internal_deny_history$
BEGIN
    RAISE EXCEPTION 'permission denied for table %', table_oid::regclass::text
        USING ERRCODE = 'insufficient_privilege';
END;
$__internal_deny_history$;
GRANT EXECUTE ON FUNCTION sql_saga.__internal_deny_history(oid) TO PUBLIC;

-- Name of B_history_gated, derived from the history table name (not stored in the catalog).
CREATE FUNCTION sql_saga.__internal_history_gated_name(history_table_name name)
RETURNS name
LANGUAGE sql
IMMUTABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_history_gated_name$
    SELECT sql_saga.__internal_make_name(ARRAY[history_table_name], 'gated');
$__internal_history_gated_name$;

/*
 * The family of a system-versioned table: base, history and view oids, by base or history oid.
 */
CREATE FUNCTION sql_saga.__internal_history_family(rel oid)
RETURNS TABLE (base_oid oid, history_oid oid, view_oid oid, gated_oid oid)
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_history_family$
    SELECT b.oid, h.oid, v.oid, to_regclass(format('%I.%I', sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name)))::oid
    FROM sql_saga.system_versioning AS sv
    JOIN pg_catalog.pg_namespace AS bn ON bn.nspname = sv.table_schema
    JOIN pg_catalog.pg_class AS b ON b.relnamespace = bn.oid AND b.relname = sv.table_name
    JOIN pg_catalog.pg_namespace AS hn ON hn.nspname = sv.history_schema_name
    JOIN pg_catalog.pg_class AS h ON h.relnamespace = hn.oid AND h.relname = sv.history_table_name
    LEFT JOIN pg_catalog.pg_namespace AS vn ON vn.nspname = sv.view_schema_name
    LEFT JOIN pg_catalog.pg_class AS v ON v.relnamespace = vn.oid AND v.relname = sv.view_table_name
    WHERE rel IN (b.oid, h.oid);
$__internal_history_family$;

/*
 * True while sql_saga.__internal_rebuild_system_versioning_views replaces the views and as-of
 * functions of this base table (transaction-local setting). drop_protection and health_checks let
 * exactly those objects of exactly this family be dropped and recreated; nothing else is exempt.
 * Like sql_saga.managing_history_policies, a caller could set it by hand: it only lets the owner
 * of these objects, who could change them anyway, skip a guard against mistakes.
 */
CREATE FUNCTION sql_saga.__internal_rebuilding_system_versioning_views(table_oid oid)
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_rebuilding_system_versioning_views$
    SELECT coalesce(current_setting('sql_saga.rebuilding_system_versioning_views', true) = table_oid::text, false);
$__internal_rebuilding_system_versioning_views$;

/*
 * Desired policy copies on B_history: one per SELECT/ALL policy on B. The USING expression is the
 * base policy's, deparsed with column names, so it is resolved by name on the history table.
 */
CREATE FUNCTION sql_saga.__internal_history_policy_copies(table_oid oid)
RETURNS TABLE (base_policy name, copy_name name, permissive boolean, roles oid[], qual text)
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_history_policy_copies$
    SELECT p.polname, sql_saga.__internal_history_copy_name(p.polname), p.polpermissive, p.polroles,
           pg_catalog.pg_get_expr(p.polqual, p.polrelid)
    FROM pg_catalog.pg_policy AS p
    WHERE p.polrelid = table_oid
      AND p.polcmd IN ('r', '*');
$__internal_history_policy_copies$;
GRANT EXECUTE ON FUNCTION sql_saga.__internal_history_policy_copies(oid) TO PUBLIC;

-- Schema-qualified name of a relation, independent of search_path (for regclass constants in SQL text).
CREATE FUNCTION sql_saga.__internal_qualified_name(rel oid)
RETURNS text
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_qualified_name$
    SELECT format('%I.%I', n.nspname, c.relname)
    FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.oid = rel;
$__internal_qualified_name$;

-- SQL list of policy roles, as used in CREATE POLICY ... TO <list>.
CREATE FUNCTION sql_saga.__internal_policy_role_list(roles oid[])
RETURNS text
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_policy_role_list$
    SELECT coalesce(string_agg(CASE WHEN r = 0 THEN 'PUBLIC' ELSE quote_ident(r::regrole::text) END, ', ' ORDER BY o), 'PUBLIC')
    FROM unnest(roles) WITH ORDINALITY AS u (r, o);
$__internal_policy_role_list$;

/*
 * Columns referenced by a base policy that the history table lacks (or has with another type).
 * Policies resolve columns by name on the history table, so such a copy cannot be made.
 */
CREATE FUNCTION sql_saga.__internal_policy_missing_history_column(base_policy_oid oid, history_table_oid oid)
RETURNS name
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_policy_missing_history_column$
    SELECT ba.attname
    FROM pg_catalog.pg_policy AS p
    JOIN pg_catalog.pg_depend AS d
      ON d.classid = 'pg_catalog.pg_policy'::regclass AND d.objid = p.oid
     AND d.refclassid = 'pg_catalog.pg_class'::regclass AND d.refobjid = p.polrelid AND d.refobjsubid > 0
    JOIN pg_catalog.pg_attribute AS ba ON ba.attrelid = p.polrelid AND ba.attnum = d.refobjsubid
    LEFT JOIN pg_catalog.pg_attribute AS ha
      ON ha.attrelid = history_table_oid AND ha.attname = ba.attname AND NOT ha.attisdropped
    WHERE p.oid = base_policy_oid
      AND (ha.attname IS NULL OR ha.atttypid <> ba.atttypid)
    ORDER BY ba.attnum
    LIMIT 1;
$__internal_policy_missing_history_column$;

/*
 * Reports how far B_history's policy copies are from the desired set: copies missing, and copies
 * present but stale (definition differs) or orphaned (no base policy hashes to them).
 * The desired USING is compared after the same parse a copy would get, on the history table.
 */
CREATE FUNCTION sql_saga.__internal_history_policy_drift(table_oid oid, history_table_oid oid,
    OUT missing integer, OUT stale integer, OUT unmanaged integer)
LANGUAGE plpgsql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_history_policy_drift$
DECLARE
    d record;
    h record;
BEGIN
    missing := 0;
    stale := 0;
    -- Policies on the history table that sql_saga does not manage (created while event triggers were
    -- off, decision 7): OR-ed with the copies, they could widen what RLS-subject callers see.
    unmanaged := (SELECT count(*) FROM pg_catalog.pg_policy AS p
                  WHERE p.polrelid = history_table_oid
                    AND NOT sql_saga.__internal_is_managed_history_policy(p.polname));
    FOR d IN SELECT * FROM sql_saga.__internal_history_policy_copies(table_oid) LOOP
        SELECT p.polcmd, p.polpermissive, p.polroles,
               pg_catalog.pg_get_expr(p.polqual, p.polrelid) AS qual,
               (SELECT pg_catalog.pg_get_expr(bp.polqual, history_table_oid)
                  FROM pg_catalog.pg_policy AS bp
                 WHERE bp.polrelid = table_oid AND bp.polname = d.base_policy) AS base_qual_on_history
        INTO h
        FROM pg_catalog.pg_policy AS p
        WHERE p.polrelid = history_table_oid AND p.polname = d.copy_name;
        IF NOT FOUND THEN
            missing := missing + 1;
        ELSIF h.polcmd <> 'r' OR h.polpermissive <> d.permissive OR h.polroles <> d.roles
              OR h.qual IS DISTINCT FROM d.qual THEN
            stale := stale + 1;
        END IF;
    END LOOP;
    stale := stale + (
        SELECT count(*)
        FROM pg_catalog.pg_policy AS p
        WHERE p.polrelid = history_table_oid
          AND p.polname ~ '^sql_saga_[0-9a-f]{16}$'
          AND NOT EXISTS (SELECT FROM sql_saga.__internal_history_policy_copies(table_oid) AS c
                          WHERE c.copy_name = p.polname));
END;
$__internal_history_policy_drift$;
GRANT EXECUTE ON FUNCTION sql_saga.__internal_history_policy_drift(oid, oid) TO PUBLIC;

/*
 * Read-time guard used by B_with_history: when the caller is subject to B's RLS, history is read
 * through the copies, so they must be in sync. Fails closed: never returns unfiltered rows.
 * Cost: only evaluated (once per query, One-Time Filter) when row_security_active(B) is true.
 */
-- SECURITY INVOKER without a fixed search_path, so the relation names in the message read as the
-- caller would write them (doc, not public.doc). Every reference is schema-qualified.
CREATE FUNCTION sql_saga.__internal_history_policies_synchronized(table_oid oid, history_table_oid oid)
RETURNS boolean
LANGUAGE plpgsql
STABLE
AS $__internal_history_policies_synchronized$
DECLARE
    drift record;
BEGIN
    SELECT * INTO drift FROM sql_saga.__internal_history_policy_drift(table_oid, history_table_oid);
    IF drift.unmanaged > 0 THEN
        RAISE EXCEPTION 'sql_saga: history of % is not synchronized (% policy copies missing, % stale, % not managed by sql_saga)',
            table_oid::pg_catalog.regclass, drift.missing, drift.stale, drift.unmanaged
            USING HINT = pg_catalog.format('Drop the policies not managed by sql_saga on %s (define them on %s instead), then run: SELECT sql_saga.sync_history_policies(%L);',
                history_table_oid::pg_catalog.regclass, table_oid::pg_catalog.regclass, table_oid::pg_catalog.regclass::pg_catalog.text);
    ELSIF drift.missing > 0 OR drift.stale > 0 THEN
        RAISE EXCEPTION 'sql_saga: history of % is not synchronized (% policy copies missing, % stale)',
            table_oid::pg_catalog.regclass, drift.missing, drift.stale
            USING HINT = pg_catalog.format('Run: SELECT sql_saga.sync_history_policies(%L);', table_oid::pg_catalog.regclass::pg_catalog.text);
    END IF;
    RETURN true;
END;
$__internal_history_policies_synchronized$;
GRANT EXECUTE ON FUNCTION sql_saga.__internal_history_policies_synchronized(oid, oid) TO PUBLIC;

/*
 * Makes B_history's row-level security match the design for B: RLS enabled, FORCE as on B, the two
 * fixed policies, and exactly one current copy per SELECT/ALL policy on B. Idempotent; every change
 * is reported with a NOTICE naming the statement. Dumped copies from another installation carry
 * another token, so they show up as orphans here and are dropped.
 */
CREATE FUNCTION sql_saga.__internal_sync_history_policies(table_oid oid, dropped_base_policies name[] DEFAULT '{}')
RETURNS void
LANGUAGE plpgsql
SET search_path = sql_saga, pg_catalog, public
AS $__internal_sync_history_policies$
DECLARE
    fam record;
    d record;
    p record;
    missing_column name;
    stmt text;
    previous_guard text;
    force_mode text;
    reader_qual text;
    base_select_qual text;
BEGIN
    SELECT * INTO fam FROM sql_saga.__internal_history_family(table_oid) AS f WHERE f.base_oid = table_oid;
    IF NOT FOUND THEN
        RETURN;
    END IF;

    -- A policy sql_saga does not manage blocks synchronization: sql_saga never drops a user's policy
    -- (decision 7); the user moves it to the base table.
    FOR p IN
        SELECT hp.polname FROM pg_catalog.pg_policy AS hp
        WHERE hp.polrelid = fam.history_oid AND NOT sql_saga.__internal_is_managed_history_policy(hp.polname)
        ORDER BY hp.polname
    LOOP
        RAISE EXCEPTION 'sql_saga: policies on history table "%" are managed by sql_saga', sql_saga.__internal_display_name(fam.history_oid::regclass)
            USING HINT = format('Drop policy "%s" on "%s" and create it on "%s"; sql_saga copies its SELECT and ALL policies to the history.',
                p.polname, sql_saga.__internal_display_name(fam.history_oid::regclass), sql_saga.__internal_display_name(fam.base_oid::regclass));
    END LOOP;

    -- Validate every desired copy before changing anything, so that a refusal reports only the
    -- refusal: no NOTICE describes a change that the error then undoes.
    FOR d IN
        SELECT c.base_policy, bp.oid AS base_policy_oid
        FROM sql_saga.__internal_history_policy_copies(fam.base_oid) AS c
        JOIN pg_catalog.pg_policy AS bp ON bp.polrelid = fam.base_oid AND bp.polname = c.base_policy
        ORDER BY c.base_policy
    LOOP
        missing_column := sql_saga.__internal_policy_missing_history_column(d.base_policy_oid, fam.history_oid);
        IF missing_column IS NOT NULL THEN
            RAISE EXCEPTION 'sql_saga: policy % on % references column %, which % lacks',
                d.base_policy, fam.base_oid::regclass, missing_column, fam.history_oid::regclass;
        END IF;
    END LOOP;

    -- While on, the event triggers neither refuse nor re-synchronize the policy DDL issued below
    -- (otherwise every CREATE POLICY here would start a nested synchronization). The previous value
    -- is restored at the end, so a nested call cannot switch the guard off for its caller; on error
    -- the transaction-local setting is discarded with the (sub)transaction.
    previous_guard := current_setting('sql_saga.managing_history_policies', true);
    PERFORM set_config('sql_saga.managing_history_policies', 'on', true);

    -- RLS enabled, FORCE mirrored from the base.
    IF NOT (SELECT c.relrowsecurity FROM pg_catalog.pg_class AS c WHERE c.oid = fam.history_oid) THEN
        stmt := format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', fam.history_oid::regclass);
        EXECUTE stmt;
        RAISE DEBUG 'sql_saga: %', stmt;
    END IF;
    IF (SELECT c.relforcerowsecurity FROM pg_catalog.pg_class AS c WHERE c.oid = fam.history_oid)
       IS DISTINCT FROM (SELECT c.relforcerowsecurity FROM pg_catalog.pg_class AS c WHERE c.oid = fam.base_oid) THEN
        force_mode := CASE WHEN (SELECT c.relforcerowsecurity FROM pg_catalog.pg_class AS c WHERE c.oid = fam.base_oid)
                           THEN 'FORCE' ELSE 'NO FORCE' END;
        EXECUTE format('ALTER TABLE %s %s ROW LEVEL SECURITY', fam.history_oid::regclass, force_mode);
        RAISE NOTICE 'sql_saga: Set row level security on % to %', fam.history_oid::regclass, force_mode;
    END IF;

    -- The two fixed policies.
    -- regclass constants: PostgreSQL records a dependency on the base and pg_dump writes them by name.
    reader_qual := format('(has_table_privilege(%1$L::regclass::oid, ''SELECT''::text) AND (NOT row_security_active(%1$L::regclass::oid)))', sql_saga.__internal_qualified_name(fam.base_oid));
    base_select_qual := format('has_table_privilege(%L::regclass::oid, ''SELECT''::text)', sql_saga.__internal_qualified_name(fam.base_oid));
    FOR p IN
        SELECT sql_saga.__internal_history_reader_policy_name() AS name, true AS permissive, reader_qual AS qual
        UNION ALL
        SELECT sql_saga.__internal_history_base_select_policy_name(), false, base_select_qual
    LOOP
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_policy AS hp WHERE hp.polrelid = fam.history_oid AND hp.polname = p.name) THEN
            stmt := format('CREATE POLICY %I ON %s AS %s FOR SELECT TO PUBLIC USING (%s)',
                p.name, fam.history_oid::regclass, CASE WHEN p.permissive THEN 'PERMISSIVE' ELSE 'RESTRICTIVE' END, p.qual);
            EXECUTE stmt;
            -- Part of creating the history (add_system_versioning), or a repair: no NOTICE.
            RAISE DEBUG 'sql_saga: %', stmt;
        END IF;
    END LOOP;

    -- Drop fixed policies of another installation (replayed from a dump).
    FOR p IN
        SELECT hp.polname
        FROM pg_catalog.pg_policy AS hp
        WHERE hp.polrelid = fam.history_oid
          AND hp.polname ~ '^sql_saga_history_(reader|base_select)_'
          AND hp.polname NOT IN (sql_saga.__internal_history_reader_policy_name(),
                                 sql_saga.__internal_history_base_select_policy_name())
        ORDER BY hp.polname
    LOOP
        EXECUTE format('DROP POLICY %I ON %s', p.polname, fam.history_oid::regclass);
        RAISE DEBUG 'sql_saga: dropped policy % on %, a fixed policy of another installation', p.polname, fam.history_oid::regclass;
    END LOOP;

    -- Drop copies that are orphaned or stale. The base policy of a copy is found by its hash, among
    -- the current base policies and those just dropped (passed by the sql_drop event trigger).
    FOR p IN
        SELECT hp.polname,
               coalesce(c.base_policy,
                        (SELECT dp FROM unnest(dropped_base_policies) AS dp
                         WHERE sql_saga.__internal_history_copy_name(dp) = hp.polname LIMIT 1)) AS base_policy
        FROM pg_catalog.pg_policy AS hp
        LEFT JOIN sql_saga.__internal_history_policy_copies(fam.base_oid) AS c ON c.copy_name = hp.polname
        WHERE hp.polrelid = fam.history_oid
          AND hp.polname ~ '^sql_saga_[0-9a-f]{16}$'
          AND (c.copy_name IS NULL
               OR hp.polcmd <> 'r' OR hp.polpermissive <> c.permissive OR hp.polroles <> c.roles
               OR pg_catalog.pg_get_expr(hp.polqual, hp.polrelid) IS DISTINCT FROM c.qual)
        ORDER BY hp.polname
    LOOP
        EXECUTE format('DROP POLICY %I ON %s', p.polname, fam.history_oid::regclass);
        IF p.base_policy IS NOT NULL THEN
            RAISE NOTICE 'sql_saga: Dropped policy % on %, the history copy of policy % on %',
                p.polname, fam.history_oid::regclass, p.base_policy, fam.base_oid::regclass;
        ELSE
            RAISE NOTICE 'sql_saga: Dropped policy % on %, which copies no current policy of %',
                p.polname, fam.history_oid::regclass, fam.base_oid::regclass;
        END IF;
    END LOOP;

    -- Create missing copies.
    FOR d IN
        SELECT c.*, bp.oid AS base_policy_oid
        FROM sql_saga.__internal_history_policy_copies(fam.base_oid) AS c
        JOIN pg_catalog.pg_policy AS bp ON bp.polrelid = fam.base_oid AND bp.polname = c.base_policy
        WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_policy AS hp WHERE hp.polrelid = fam.history_oid AND hp.polname = c.copy_name)
        ORDER BY c.base_policy
    LOOP
        missing_column := sql_saga.__internal_policy_missing_history_column(d.base_policy_oid, fam.history_oid);
        IF missing_column IS NOT NULL THEN
            RAISE EXCEPTION 'sql_saga: policy % on % references column %, which % lacks',
                d.base_policy, fam.base_oid::regclass, missing_column, fam.history_oid::regclass;
        END IF;
        stmt := format('CREATE POLICY %I ON %s AS %s FOR SELECT TO %s USING (%s)',
            d.copy_name, fam.history_oid::regclass,
            CASE WHEN d.permissive THEN 'PERMISSIVE' ELSE 'RESTRICTIVE' END,
            sql_saga.__internal_policy_role_list(d.roles), coalesce(d.qual, 'true'));
        BEGIN
            EXECUTE stmt;
        EXCEPTION WHEN undefined_table OR undefined_column THEN
            -- A self-reference by the base table's name inside a subquery cannot be resolved on history.
            RAISE EXCEPTION 'sql_saga: policy % on % cannot be applied to %: %',
                d.base_policy, fam.base_oid::regclass, fam.history_oid::regclass, SQLERRM;
        END;
        RAISE NOTICE 'sql_saga: Created policy % on % as the history copy of policy % on %',
            d.copy_name, fam.history_oid::regclass, d.base_policy, fam.base_oid::regclass;
    END LOOP;

    PERFORM set_config('sql_saga.managing_history_policies', coalesce(previous_guard, 'off'), true);
END;
$__internal_sync_history_policies$;

-- SECURITY INVOKER: the caller must own the history table (it is owned by the base table's owner),
-- because the synchronization runs ALTER TABLE and CREATE/DROP POLICY on it as the caller.
-- (A SECURITY DEFINER function would see its own owner as current_user and could not tell.)
CREATE FUNCTION sql_saga.sync_history_policies(table_oid regclass)
RETURNS void
LANGUAGE plpgsql
SET search_path = sql_saga, pg_catalog
AS $sync_history_policies$
BEGIN
    IF NOT EXISTS (SELECT FROM sql_saga.__internal_history_family(table_oid) AS f WHERE f.base_oid = table_oid) THEN
        RAISE EXCEPTION 'table % does not have SYSTEM VERSIONING', table_oid;
    END IF;
    IF NOT pg_catalog.pg_has_role((SELECT c.relowner FROM pg_catalog.pg_class AS c WHERE c.oid = table_oid), 'USAGE') THEN
        RAISE EXCEPTION 'must be owner of table %', table_oid USING ERRCODE = 'insufficient_privilege';
    END IF;
    PERFORM sql_saga.__internal_sync_history_policies(table_oid);
END;
$sync_history_policies$;
COMMENT ON FUNCTION sql_saga.sync_history_policies(regclass) IS
'Makes the row-level security of the history table of a system-versioned table match its base table: one copy per SELECT/ALL policy, FORCE as on the base. sql_saga does this automatically on policy DDL; this function repairs a history whose policies changed while event triggers were off.';

/*
 * The text of B_history_gated: owner-rights view of B_history, readable only by callers that may
 * SELECT B, and only while B's RLS does not apply to them (otherwise history is read as the caller).
 */
CREATE FUNCTION sql_saga.__internal_history_gated_view_sql(table_oid oid, history_table_oid oid, columns text)
RETURNS text
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_history_gated_view_sql$
    SELECT format(
        'SELECT %1$s FROM %2$s WHERE CASE WHEN pg_catalog.has_table_privilege(%3$L::regclass::oid, ''SELECT''::text) THEN true ELSE sql_saga.__internal_deny_history(%3$L::regclass::oid) END AND NOT pg_catalog.row_security_active(%3$L::regclass::oid)',
        columns, history_table_oid::regclass, sql_saga.__internal_qualified_name(table_oid));
$__internal_history_gated_view_sql$;
