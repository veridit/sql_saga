CREATE OR REPLACE FUNCTION sql_saga.health_checks()
 RETURNS event_trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 -- Set a safe search path for this SECURITY DEFINER function
 SET search_path = sql_saga, pg_catalog, public
AS
$function$
#variable_conflict use_variable
DECLARE
    cmd text;
    r record;
    managed_policy record;
    gated_columns text;
    expected_gate text;
    alter_function_resolved text;
BEGIN
    IF tg_tag IN ('ALTER FUNCTION', 'ALTER ROUTINE') THEN
        alter_function_resolved := current_setting('sql_saga.alter_function_resolved', true);
        /* Consume the statement snapshot before doing any comparison or raising. */
        PERFORM set_config('sql_saga.alter_function_resolved', '', true);

        IF alter_function_resolved IS NULL OR alter_function_resolved = '' THEN
            RETURN;
        END IF;

        FOR r IN
            SELECT to_regclass(format('%I.%I', x.table_schema, x.table_name)) AS table_oid,
                   x.identity
            FROM jsonb_to_recordset(alter_function_resolved::jsonb) AS x(
                table_schema name,
                table_name name,
                role text,
                identity text)
            WHERE to_regprocedure(x.identity) IS NULL
            ORDER BY x.table_schema, x.table_name, x.role
        LOOP
            RAISE EXCEPTION 'cannot drop or rename function "%" because it is used in SYSTEM VERSIONING for table "%"',
                sql_saga.__internal_display_function_name(r.identity), sql_saga.__internal_display_name(r.table_oid);
        END LOOP;
    END IF;

    /* sql_drop protects exact managed members before DROP reaches ddl_command_end. */
    IF tg_tag LIKE 'DROP %' THEN
        RETURN;
    END IF;

    -- Exit early if the DDL command does not affect a managed object.
    IF NOT sql_saga.__internal_ddl_command_affects_managed_object() THEN
        RETURN;
    END IF;

    /* Make sure that all of our tables are still persistent */
    FOR r IN
        SELECT to_regclass(format('%I.%I', e.table_schema /* %I */, e.table_name /* %I */)) AS table_oid
        FROM sql_saga.era AS e
        JOIN pg_catalog.pg_class AS c ON c.relname = e.table_name
        JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = e.table_schema
        WHERE c.relpersistence <> 'p'
    LOOP
        RAISE EXCEPTION 'table "%" must remain persistent because it has an era',
            sql_saga.__internal_display_name(r.table_oid);
    END LOOP;

    /* And the history tables, too */
    FOR r IN
        SELECT to_regclass(format('%I.%I', e.table_schema /* %I */, e.table_name /* %I */)) AS table_oid
        FROM sql_saga.era AS e
        JOIN pg_catalog.pg_class AS c ON c.relname = e.audit_table_name
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace AND n.nspname = e.audit_schema_name
        WHERE e.audit_table_name IS NOT NULL AND c.relpersistence <> 'p'
    LOOP
        RAISE EXCEPTION 'history table "%" must remain persistent because it has an era',
            sql_saga.__internal_display_name(r.table_oid);
    END LOOP;

    /* Fix up history and for-portion objects ownership */
    FOR cmd IN
        /* Fix ownership of history tables */
        SELECT format('ALTER TABLE %I.%I OWNER TO %I',
            sv.history_schema_name /* %I */, sv.history_table_name /* %I */, t.relowner::regrole /* %I */)
        FROM sql_saga.system_versioning AS sv
        JOIN pg_class AS t ON t.relname = sv.table_name
        JOIN pg_namespace AS tn ON tn.oid = t.relnamespace AND tn.nspname = sv.table_schema
        JOIN pg_class AS ht ON ht.relname = sv.history_table_name
        JOIN pg_namespace AS htn ON htn.oid = ht.relnamespace AND htn.nspname = sv.history_schema_name
        WHERE t.relowner <> ht.relowner

        UNION ALL

        /* Fix ownership of system versioning views */
        SELECT format('ALTER VIEW %I.%I OWNER TO %I',
            sv.view_schema_name /* %I */, sv.view_table_name /* %I */, t.relowner::regrole /* %I */)
        FROM sql_saga.system_versioning AS sv
        JOIN pg_class AS t ON t.relname = sv.table_name
        JOIN pg_namespace AS tn ON tn.oid = t.relnamespace AND tn.nspname = sv.table_schema
        JOIN pg_class AS vt ON vt.relname = sv.view_table_name
        JOIN pg_namespace AS vtn ON vtn.oid = vt.relnamespace AND vtn.nspname = sv.view_schema_name
        WHERE t.relowner <> vt.relowner

        UNION ALL

        /* Fix ownership of the gated history views */
        SELECT format('ALTER VIEW %s OWNER TO %I', f.gated_oid::regclass, t.relowner::regrole)
        FROM sql_saga.system_versioning AS sv
        JOIN pg_class AS t ON t.relname = sv.table_name
        JOIN pg_namespace AS tn ON tn.oid = t.relnamespace AND tn.nspname = sv.table_schema
        CROSS JOIN LATERAL sql_saga.__internal_history_family(t.oid) AS f
        JOIN pg_class AS gv ON gv.oid = f.gated_oid
        WHERE f.base_oid = t.oid AND t.relowner <> gv.relowner

        UNION ALL

        /* Fix ownership of for-portion-of views */
        SELECT format('ALTER VIEW %I.%I OWNER TO %I', v.view_schema /* %I */, v.view_name /* %I */, t.relowner::regrole /* %I */)
        FROM sql_saga.updatable_view v
        JOIN pg_class t ON t.relname = v.table_name
        JOIN pg_namespace tn ON tn.oid = t.relnamespace AND tn.nspname = v.table_schema
        JOIN pg_class vt ON vt.relname = v.view_name
        JOIN pg_namespace vn ON vn.oid = vt.relnamespace AND vn.nspname = v.view_schema
        WHERE t.relowner <> vt.relowner

        UNION ALL

        /* Fix ownership of system versioning functions */
        SELECT format('ALTER FUNCTION %s OWNER TO %I', p.oid::regprocedure, t.relowner::regrole)
        FROM sql_saga.system_versioning AS sv
        JOIN pg_class AS t ON t.relname = sv.table_name
        JOIN pg_namespace AS tn ON tn.oid = t.relnamespace AND tn.nspname = sv.table_schema
        JOIN pg_proc AS p ON p.oid IN (to_regprocedure(sv.func_as_of), to_regprocedure(sv.func_between),
                                       to_regprocedure(sv.func_between_symmetric), to_regprocedure(sv.func_from_to))
        WHERE t.relowner <> p.proowner
    LOOP
        EXECUTE cmd;
    END LOOP;

    /*
     * Row-level security of history tables (tmp/design_final.md sections 3, 8-10, 15).
     * Policy DDL on a versioned base table (or its history), and ENABLE/DISABLE/FORCE/NO FORCE on
     * either, re-synchronizes that family's history policies. Direct changes to the policies
     * sql_saga manages, and to the history's RLS settings, are refused.
     */
    FOR r IN
        SELECT DISTINCT f.base_oid, f.history_oid, ev.command_tag, ev.objid, ev.classid
        FROM pg_event_trigger_ddl_commands() AS ev
        LEFT JOIN pg_catalog.pg_policy AS pol
               ON ev.classid = 'pg_catalog.pg_policy'::regclass AND pol.oid = ev.objid
        CROSS JOIN LATERAL sql_saga.__internal_history_family(
            CASE WHEN ev.classid = 'pg_catalog.pg_policy'::regclass THEN pol.polrelid ELSE ev.objid END) AS f
        WHERE ev.command_tag IN ('CREATE POLICY', 'ALTER POLICY', 'ALTER TABLE')
    LOOP
        IF current_setting('sql_saga.managing_history_policies', true) IS DISTINCT FROM 'on' THEN
            -- Refuse direct DDL on managed history policies.
            -- CREATE POLICY of a managed name is what pg_restore does when it replays a dump: the sync
            -- below then keeps it (current installation) or drops it (another installation's token).
            IF r.classid = 'pg_catalog.pg_policy'::regclass THEN
                SELECT p.polname, p.polrelid INTO managed_policy
                FROM pg_catalog.pg_policy AS p WHERE p.oid = r.objid;
                IF managed_policy.polrelid = r.history_oid THEN
                    IF NOT sql_saga.__internal_is_managed_history_policy(managed_policy.polname) THEN
                        -- A policy of one's own on the history table (decision 7): it would be OR-ed
                        -- with the copies and could show more history than the base table allows.
                        -- ALTER POLICY ... RENAME TO a non-managed name lands here too.
                        RAISE EXCEPTION 'sql_saga: policies on history table "%" are managed by sql_saga', sql_saga.__internal_display_name(r.history_oid::regclass)
                            USING HINT = format('Create the policy on "%s"; sql_saga copies its SELECT and ALL policies to the history.',
                                                sql_saga.__internal_display_name(r.base_oid::regclass));
                    ELSIF r.command_tag = 'ALTER POLICY' THEN
                        RAISE EXCEPTION 'cannot drop or alter policy "%" on "%" because it is managed by sql_saga for table "%"',
                            managed_policy.polname, sql_saga.__internal_display_name(r.history_oid::regclass), sql_saga.__internal_display_name(r.base_oid::regclass);
                    END IF;
                END IF;
            END IF;
            -- Refuse direct changes to the history's RLS settings.
            IF r.command_tag = 'ALTER TABLE' AND r.objid = r.history_oid
               AND NOT EXISTS (SELECT FROM pg_catalog.pg_class AS h, pg_catalog.pg_class AS b
                               WHERE h.oid = r.history_oid AND b.oid = r.base_oid
                                 AND h.relrowsecurity AND h.relforcerowsecurity = b.relforcerowsecurity) THEN
                RAISE EXCEPTION 'history table "%" must keep row level security enabled, and forced exactly when its base table is, because it is used in SYSTEM VERSIONING for table "%"',
                    sql_saga.__internal_display_name(r.history_oid::regclass), sql_saga.__internal_display_name(r.base_oid::regclass);
            END IF;
        END IF;
        -- Not during the teardown by pg_restore --clean (its ALTER TABLE .. DROP CONSTRAINT on the base
        -- would recreate the history policies drop_protection has just dropped for the teardown).
        IF current_setting('sql_saga.managing_history_policies', true) IS DISTINCT FROM 'on'
           AND NOT sql_saga.__internal_is_clean_teardown()
           AND (r.objid <> r.history_oid OR r.classid = 'pg_catalog.pg_policy'::regclass) THEN
            PERFORM sql_saga.__internal_sync_history_policies(r.base_oid);
        END IF;
    END LOOP;

    /* The fixed shape of views sql_saga manages. */
    FOR r IN
        SELECT ev.objid AS view_oid, v.base_oid, v.kind
        FROM pg_event_trigger_ddl_commands() AS ev
        JOIN (
            SELECT format('%I.%I', sv.view_schema_name, sv.view_table_name)::regclass::oid AS view_oid,
                   format('%I.%I', sv.table_schema, sv.table_name)::regclass::oid AS base_oid,
                   'SYSTEM VERSIONING'::text AS kind
            FROM sql_saga.system_versioning AS sv
            WHERE to_regclass(format('%I.%I', sv.view_schema_name, sv.view_table_name)) IS NOT NULL
            UNION ALL
            SELECT format('%I.%I', uv.view_schema, uv.view_name)::regclass::oid,
                   format('%I.%I', uv.table_schema, uv.table_name)::regclass::oid,
                   CASE uv.view_type WHEN 'for_portion_of' THEN 'FOR PORTION OF' ELSE 'a current view' END
            FROM sql_saga.updatable_view AS uv
            WHERE to_regclass(format('%I.%I', uv.view_schema, uv.view_name)) IS NOT NULL
        ) AS v ON v.view_oid = ev.objid
        WHERE ev.command_tag IN ('ALTER VIEW', 'CREATE VIEW', 'ALTER TABLE')
    LOOP
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_class AS c
                       WHERE c.oid = r.view_oid AND 'security_invoker=on' = ANY (c.reloptions)) THEN
            RAISE EXCEPTION 'view "%" must keep security_invoker because it is used in % for table "%"',
                sql_saga.__internal_display_name(r.view_oid::regclass), r.kind, sql_saga.__internal_display_name(r.base_oid::regclass);
        END IF;
    END LOOP;

    FOR r IN
        SELECT ev.objid AS view_oid, f.base_oid, f.history_oid
        FROM pg_event_trigger_ddl_commands() AS ev
        JOIN sql_saga.system_versioning AS sv
          ON to_regclass(format('%I.%I', sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name)))::oid = ev.objid
        CROSS JOIN LATERAL sql_saga.__internal_history_family(format('%I.%I', sv.table_schema, sv.table_name)::regclass) AS f
        WHERE ev.command_tag IN ('ALTER VIEW', 'CREATE VIEW', 'ALTER TABLE')
    LOOP
        -- The gate must be exactly the generated definition (owner-rights, same body), not just
        -- contain its parts: e.g. "WHERE true OR (gate)" keeps every function reference but opens
        -- the view. The canonical body is rendered from the view's own column list, so the check
        -- holds whatever columns the history has, and both are deparsed the same way.
        gated_columns := (SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum)
                          FROM pg_catalog.pg_attribute AS a
                          WHERE a.attrelid = r.view_oid AND a.attnum > 0 AND NOT a.attisdropped);
        EXECUTE 'CREATE TEMPORARY VIEW sql_saga_expected_gate AS '
            || sql_saga.__internal_history_gated_view_sql(r.base_oid, r.history_oid, gated_columns);
        expected_gate := pg_catalog.pg_get_viewdef('pg_temp.sql_saga_expected_gate'::regclass);
        DROP VIEW pg_temp.sql_saga_expected_gate;
        IF EXISTS (SELECT FROM pg_catalog.pg_class AS c WHERE c.oid = r.view_oid AND c.reloptions IS NOT NULL)
           OR pg_catalog.pg_get_viewdef(r.view_oid) IS DISTINCT FROM expected_gate THEN
            RAISE EXCEPTION 'view "%" must keep its access gate because it is used in SYSTEM VERSIONING for table "%"',
                sql_saga.__internal_display_name(r.view_oid::regclass), sql_saga.__internal_display_name(r.base_oid::regclass);
        END IF;
    END LOOP;

END;
$function$;

CREATE FUNCTION sql_saga.__internal_system_versioning_function_is_attributable(
    table_oid oid,
    function_identity text,
    function_role text)
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_system_versioning_function_is_attributable$
    SELECT EXISTS (
        SELECT 1
        FROM sql_saga.__internal_system_versioning_query_functions(table_oid) AS q
        JOIN pg_catalog.pg_proc AS p ON p.oid = pg_catalog.to_regprocedure(q.identity)
        JOIN pg_catalog.pg_class AS base ON base.oid = table_oid
        JOIN sql_saga.system_versioning AS sv
          ON pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name)) = table_oid
        JOIN pg_catalog.pg_class AS result_view
          ON result_view.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.view_schema_name, sv.view_table_name))
        WHERE q.identity = function_identity
          AND q.role = function_role
          AND p.prokind = 'f'
          AND p.prolang = (SELECT l.oid FROM pg_catalog.pg_language AS l WHERE l.lanname = 'sql')
          AND p.prorettype = result_view.reltype
          AND p.proretset
          AND p.provolatile = 's'
          AND NOT p.proisstrict
          AND NOT p.prosecdef
          AND NOT p.proleakproof
          AND p.proparallel = 'u'
          AND p.prosupport = 0
          AND p.procost = 100
          AND p.prorows = 1000
          AND p.proconfig IS NULL
          AND p.proowner = base.relowner
          AND p.prosrc = q.body
          AND p.prosqlbody IS NULL
          AND p.proargnames IS NULL
          AND p.proallargtypes IS NULL
          AND p.proargmodes IS NULL
          AND p.proargtypes = CASE q.role
                WHEN 'as_of' THEN '1184'::pg_catalog.oidvector
                ELSE '1184 1184'::pg_catalog.oidvector
              END
          AND NOT EXISTS (
              SELECT 1
              FROM pg_catalog.aclexplode(coalesce(p.proacl, pg_catalog.acldefault('f', p.proowner))) AS x
              WHERE x.grantee NOT IN (0, p.proowner)
                 OR (x.grantee = 0 AND x.privilege_type <> 'EXECUTE'))
          AND EXISTS (
              SELECT 1
              FROM pg_catalog.aclexplode(coalesce(p.proacl, pg_catalog.acldefault('f', p.proowner))) AS x
              WHERE x.grantee = 0 AND x.privilege_type = 'EXECUTE')
          AND 1 = (
              SELECT pg_catalog.count(*)
              FROM pg_catalog.pg_depend AS d
              WHERE d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
                AND d.objid = p.oid
                AND d.refclassid = 'pg_catalog.pg_type'::pg_catalog.regclass
                AND d.deptype = 'n')
          AND EXISTS (
              SELECT 1
              FROM pg_catalog.pg_depend AS d
              WHERE d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
                AND d.objid = p.oid
                AND d.refclassid = 'pg_catalog.pg_type'::pg_catalog.regclass
                AND d.refobjid = result_view.reltype
                AND d.deptype = 'n')
          AND EXISTS (
              SELECT 1 FROM pg_catalog.pg_type AS t
              WHERE t.oid = p.prorettype AND t.typrelid = result_view.oid)
    );
$__internal_system_versioning_function_is_attributable$;

CREATE FUNCTION sql_saga.__internal_system_versioning_member_attribution(table_oid oid)
RETURNS TABLE(
    as_of_attributable boolean,
    between_attributable boolean,
    between_symmetric_attributable boolean,
    from_to_attributable boolean,
    public_view_attributable boolean,
    gated_view_attributable boolean)
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_system_versioning_member_attribution$
    WITH family AS (
        SELECT base.relowner,
               history.oid AS history_oid,
               public_view.oid AS public_view_oid,
               gated_view.oid AS gated_view_oid,
               public_view.reltype AS public_view_type
        FROM pg_catalog.pg_class AS base
        JOIN pg_catalog.pg_namespace AS bn ON bn.oid = base.relnamespace
        JOIN sql_saga.system_versioning AS sv
          ON (sv.table_schema, sv.table_name) = (bn.nspname, base.relname)
        LEFT JOIN pg_catalog.pg_namespace AS hn ON hn.nspname = sv.history_schema_name
        LEFT JOIN pg_catalog.pg_class AS history
          ON history.relnamespace = hn.oid AND history.relname = sv.history_table_name
        LEFT JOIN pg_catalog.pg_namespace AS vn ON vn.nspname = sv.view_schema_name
        LEFT JOIN pg_catalog.pg_class AS public_view
          ON public_view.relnamespace = vn.oid AND public_view.relname = sv.view_table_name
        LEFT JOIN pg_catalog.pg_class AS gated_view
          ON gated_view.relnamespace = hn.oid
         AND gated_view.relname = sql_saga.__internal_history_gated_name(sv.history_table_name)
        WHERE base.oid = table_oid
    ), function_objects AS (
        SELECT q.role, q.body, p.*
        FROM family AS f
        CROSS JOIN LATERAL sql_saga.__internal_system_versioning_query_functions(table_oid) AS q
        LEFT JOIN pg_catalog.pg_proc AS p ON p.oid = pg_catalog.to_regprocedure(q.identity)
    ), function_acl AS (
        SELECT fo.oid,
               pg_catalog.bool_and(x.grantee IN (0, fo.proowner)
                   AND (x.grantee <> 0 OR x.privilege_type = 'EXECUTE')) AS valid,
               pg_catalog.bool_or(x.grantee = 0 AND x.privilege_type = 'EXECUTE') AS public_execute
        FROM function_objects AS fo
        LEFT JOIN LATERAL pg_catalog.aclexplode(
            coalesce(fo.proacl, pg_catalog.acldefault('f', fo.proowner))) AS x ON true
        WHERE fo.oid IS NOT NULL
        GROUP BY fo.oid
    ), function_dependencies AS (
        SELECT d.objid,
               pg_catalog.count(*) AS dependency_count,
               pg_catalog.bool_or(d.refobjid = f.public_view_type) AS depends_on_public_view
        FROM family AS f
        JOIN pg_catalog.pg_depend AS d
          ON d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
         AND d.refclassid = 'pg_catalog.pg_type'::pg_catalog.regclass
         AND d.deptype = 'n'
        JOIN function_objects AS fo ON fo.oid = d.objid
        GROUP BY d.objid
    ), functions AS (
        SELECT fo.role,
               fo.oid IS NOT NULL
               AND fo.prokind = 'f'
               AND fo.prolang = (SELECT l.oid FROM pg_catalog.pg_language AS l WHERE l.lanname = 'sql')
               AND fo.prorettype = f.public_view_type AND fo.proretset AND fo.provolatile = 's'
               AND NOT fo.proisstrict AND NOT fo.prosecdef AND NOT fo.proleakproof
               AND fo.proparallel = 'u' AND fo.prosupport = 0 AND fo.procost = 100 AND fo.prorows = 1000
               AND fo.proconfig IS NULL AND fo.proowner = f.relowner AND fo.prosrc = fo.body
               AND fo.prosqlbody IS NULL AND fo.proargnames IS NULL AND fo.proallargtypes IS NULL
               AND fo.proargmodes IS NULL
               AND fo.proargtypes = CASE fo.role WHEN 'as_of' THEN '1184'::pg_catalog.oidvector ELSE '1184 1184'::pg_catalog.oidvector END
               AND acl.valid AND acl.public_execute
               AND dep.dependency_count = 1 AND dep.depends_on_public_view AS attributable
        FROM family AS f
        CROSS JOIN function_objects AS fo
        LEFT JOIN function_acl AS acl ON acl.oid = fo.oid
        LEFT JOIN function_dependencies AS dep ON dep.objid = fo.oid
    ), view_objects AS (
        SELECT 'public'::text AS role, v.*, f.relowner AS base_owner,
               f.history_oid, f.gated_view_oid
        FROM family AS f JOIN pg_catalog.pg_class AS v ON v.oid = f.public_view_oid
        UNION ALL
        SELECT 'gated', v.*, f.relowner, f.history_oid, f.gated_view_oid
        FROM family AS f JOIN pg_catalog.pg_class AS v ON v.oid = f.gated_view_oid
    ), view_acl AS (
        SELECT vo.oid,
               pg_catalog.bool_and(x.grantee IN (0, vo.relowner)
                   AND (x.grantee <> 0 OR x.privilege_type = 'SELECT')) AS valid,
               pg_catalog.bool_or(x.grantee = 0 AND x.privilege_type = 'SELECT') AS public_select
        FROM view_objects AS vo
        LEFT JOIN LATERAL pg_catalog.aclexplode(
            coalesce(vo.relacl, pg_catalog.acldefault('r', vo.relowner))) AS x ON true
        GROUP BY vo.oid
    ), view_dependencies AS (
        SELECT vo.oid,
               pg_catalog.bool_or(d.refobjid = table_oid) AS depends_on_base,
               pg_catalog.bool_or(d.refobjid = vo.history_oid) AS depends_on_history,
               pg_catalog.bool_or(d.refobjid = vo.gated_view_oid) AS depends_on_gated,
               pg_catalog.bool_or(d.refobjid = 'sql_saga.__internal_deny_history(oid)'::pg_catalog.regprocedure) AS depends_on_deny
        FROM view_objects AS vo
        LEFT JOIN pg_catalog.pg_rewrite AS rw ON rw.ev_class = vo.oid
        LEFT JOIN pg_catalog.pg_depend AS d
          ON d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass AND d.objid = rw.oid
        GROUP BY vo.oid
    ), views AS (
        SELECT vo.role,
               vo.relkind = 'v' AND vo.relpersistence = 'p' AND vo.relowner = vo.base_owner
               AND acl.valid AND acl.public_select
               AND CASE vo.role
                   WHEN 'public' THEN vo.reloptions = ARRAY['security_invoker=on']::text[]
                       AND dep.depends_on_base AND dep.depends_on_history AND dep.depends_on_gated
                   WHEN 'gated' THEN vo.reloptions IS NULL AND dep.depends_on_deny
                   ELSE false
               END AS attributable
        FROM view_objects AS vo
        LEFT JOIN view_acl AS acl ON acl.oid = vo.oid
        LEFT JOIN view_dependencies AS dep ON dep.oid = vo.oid
    )
    SELECT coalesce(pg_catalog.bool_and(attributable) FILTER (WHERE role = 'as_of'), false),
           coalesce(pg_catalog.bool_and(attributable) FILTER (WHERE role = 'between'), false),
           coalesce(pg_catalog.bool_and(attributable) FILTER (WHERE role = 'between_symmetric'), false),
           coalesce(pg_catalog.bool_and(attributable) FILTER (WHERE role = 'from_to'), false),
           coalesce((SELECT v.attributable FROM views AS v WHERE v.role = 'public'), false),
           coalesce((SELECT v.attributable FROM views AS v WHERE v.role = 'gated'), false)
    FROM functions;
$__internal_system_versioning_member_attribution$;

CREATE FUNCTION sql_saga.__internal_trigger_is_attributable(
    relation_oid oid,
    trigger_name name,
    function_identity regprocedure,
    expected_tgtype integer,
    expected_argument text DEFAULT NULL)
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_trigger_is_attributable$
    SELECT EXISTS (
        SELECT 1 FROM pg_catalog.pg_trigger AS t
        WHERE t.tgrelid = relation_oid
          AND t.tgname = trigger_name
          AND NOT t.tgisinternal
          AND t.tgfoid = function_identity
          AND t.tgtype = expected_tgtype
          AND CASE WHEN expected_argument IS NULL
                   THEN t.tgnargs = 0
                   ELSE t.tgnargs = 1 AND pg_catalog.encode(t.tgargs, 'escape') = expected_argument || E'\\000'
              END
    );
$__internal_trigger_is_attributable$;

CREATE FUNCTION sql_saga.__internal_system_versioning_view_is_attributable(
    table_oid oid,
    view_oid oid,
    view_role text)
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_system_versioning_view_is_attributable$
    SELECT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_class AS base
        JOIN sql_saga.system_versioning AS sv
          ON pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name)) = base.oid
        JOIN pg_catalog.pg_class AS v ON v.oid = view_oid
        WHERE base.oid = table_oid
          AND v.relkind = 'v'
          AND v.relpersistence = 'p'
          AND v.relowner = base.relowner
          AND NOT EXISTS (
              SELECT 1 FROM pg_catalog.aclexplode(coalesce(v.relacl, pg_catalog.acldefault('r', v.relowner))) AS x
              WHERE x.grantee NOT IN (0, v.relowner) OR (x.grantee = 0 AND x.privilege_type <> 'SELECT'))
          AND EXISTS (
              SELECT 1 FROM pg_catalog.aclexplode(coalesce(v.relacl, pg_catalog.acldefault('r', v.relowner))) AS x
              WHERE x.grantee = 0 AND x.privilege_type = 'SELECT')
          AND CASE view_role
              WHEN 'public' THEN
                  v.reloptions = ARRAY['security_invoker=on']::text[]
                  AND EXISTS (
                      SELECT 1 FROM pg_catalog.pg_rewrite AS rw
                      JOIN pg_catalog.pg_depend AS d ON d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass AND d.objid = rw.oid
                      WHERE rw.ev_class = v.oid AND d.refobjid = table_oid)
                  AND EXISTS (
                      SELECT 1 FROM pg_catalog.pg_rewrite AS rw
                      JOIN pg_catalog.pg_depend AS d ON d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass AND d.objid = rw.oid
                      WHERE rw.ev_class = v.oid
                        AND d.refobjid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.history_schema_name, sv.history_table_name)))
                  AND EXISTS (
                      SELECT 1 FROM pg_catalog.pg_rewrite AS rw
                      JOIN pg_catalog.pg_depend AS d ON d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass AND d.objid = rw.oid
                      WHERE rw.ev_class = v.oid
                        AND d.refobjid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.history_schema_name,
                            sql_saga.__internal_history_gated_name(sv.history_table_name))))
              WHEN 'gated' THEN
                  v.reloptions IS NULL
                  AND EXISTS (
                      SELECT 1 FROM pg_catalog.pg_rewrite AS rw
                      JOIN pg_catalog.pg_depend AS d ON d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass AND d.objid = rw.oid
                      WHERE rw.ev_class = v.oid
                        AND d.refobjid = 'sql_saga.__internal_deny_history(oid)'::pg_catalog.regprocedure)
              ELSE false
              END
    );
$__internal_system_versioning_view_is_attributable$;

CREATE FUNCTION sql_saga.__internal_unique_key_inventory(filter_unique_key_name name DEFAULT NULL)
RETURNS TABLE(
    unique_key_name name,
    table_oid oid,
    unique_attributable boolean,
    exclude_attributable boolean,
    check_attributable boolean,
    object_missing boolean)
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_unique_key_inventory$
    WITH keys AS (
        SELECT uk.*, e.range_column_name,
               pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.table_name))::oid AS table_oid
        FROM sql_saga.unique_keys AS uk
        LEFT JOIN sql_saga.era AS e USING (table_schema, table_name, era_name)
        WHERE filter_unique_key_name IS NULL OR uk.unique_key_name = filter_unique_key_name
    ), expected AS (
        SELECT k.*,
               x.expected_attnums,
               pg_catalog.cardinality(x.expected_attnums) = pg_catalog.cardinality(k.column_names) + 1 AS columns_valid
        FROM keys AS k
        LEFT JOIN LATERAL (
            SELECT pg_catalog.array_agg(a.attnum::smallint ORDER BY u.n) AS expected_attnums
            FROM pg_catalog.unnest(k.column_names || k.range_column_name) WITH ORDINALITY AS u(column_name, n)
            JOIN pg_catalog.pg_attribute AS a
              ON a.attrelid = k.table_oid AND a.attname = u.column_name
             AND a.attnum > 0 AND NOT a.attisdropped
        ) AS x ON true
    )
    SELECT k.unique_key_name, k.table_oid,
        CASE
            WHEN k.table_oid IS NULL OR NOT k.columns_valid THEN false
            WHEN uk.unique_constraint IS NULL THEN true
            WHEN uk.key_type = 'predicated' THEN EXISTS (
                SELECT 1 FROM pg_catalog.pg_index AS i
                JOIN pg_catalog.pg_class AS ic ON ic.oid = i.indexrelid
                JOIN pg_catalog.pg_am AS am ON am.oid = ic.relam
                WHERE i.indexrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.unique_constraint))
                  AND i.indrelid = k.table_oid AND i.indisunique AND i.indisvalid AND i.indisready
                  AND i.indnkeyatts = pg_catalog.cardinality(k.expected_attnums)
                  AND i.indkey::text = pg_catalog.array_to_string(k.expected_attnums, ' ')
                  AND i.indexprs IS NULL AND am.amname = 'btree'
                  AND pg_catalog.regexp_replace(
                          pg_catalog.regexp_replace(
                              pg_catalog.pg_get_expr(i.indpred, i.indrelid),
                              '::(text|character varying)', '', 'g'),
                          '\(([[:alnum:]_".]+)\)', '\1', 'g')
                      IS NOT DISTINCT FROM
                      CASE WHEN uk.predicate IS NULL THEN NULL ELSE '(' || uk.predicate || ')' END)
            ELSE EXISTS (
                SELECT 1 FROM pg_catalog.pg_constraint AS c
                JOIN pg_catalog.pg_index AS i ON i.indexrelid = c.conindid
                JOIN pg_catalog.pg_class AS ic ON ic.oid = i.indexrelid
                JOIN pg_catalog.pg_am AS am ON am.oid = ic.relam
                WHERE c.conrelid = k.table_oid AND c.conname = uk.unique_constraint
                  AND c.contype = CASE uk.key_type WHEN 'primary' THEN 'p' ELSE 'u' END
                  AND c.conkey = k.expected_attnums AND c.conperiod
                  AND NOT c.condeferrable AND NOT c.condeferred
                  AND i.indrelid = k.table_oid AND i.indisunique AND i.indisvalid AND i.indisready
                  AND i.indnkeyatts = pg_catalog.cardinality(k.expected_attnums)
                  AND i.indkey::text = pg_catalog.array_to_string(k.expected_attnums, ' ')
                  AND i.indexprs IS NULL AND i.indpred IS NULL AND am.amname = 'gist')
        END AS unique_attributable,
        CASE
            WHEN k.table_oid IS NULL OR NOT k.columns_valid THEN false
            WHEN uk.exclude_constraint IS NULL THEN true
            ELSE EXISTS (
                SELECT 1 FROM pg_catalog.pg_constraint AS c
                JOIN pg_catalog.pg_index AS i ON i.indexrelid = c.conindid
                JOIN pg_catalog.pg_class AS ic ON ic.oid = i.indexrelid
                JOIN pg_catalog.pg_am AS am ON am.oid = ic.relam
                WHERE c.conrelid = k.table_oid AND c.conname = uk.exclude_constraint
                  AND c.contype = 'x' AND c.conkey = k.expected_attnums
                  AND c.condeferrable AND NOT c.condeferred
                  AND i.indrelid = k.table_oid AND i.indisvalid AND i.indisready
                  AND i.indnkeyatts = pg_catalog.cardinality(k.expected_attnums)
                  AND i.indkey::text = pg_catalog.array_to_string(k.expected_attnums, ' ')
                  AND i.indexprs IS NULL AND am.amname = 'gist'
                  AND pg_catalog.regexp_replace(
                          pg_catalog.regexp_replace(
                              pg_catalog.pg_get_expr(i.indpred, i.indrelid),
                              '::(text|character varying)', '', 'g'),
                          '\(([[:alnum:]_".]+)\)', '\1', 'g')
                      IS NOT DISTINCT FROM
                      CASE WHEN uk.predicate IS NULL THEN NULL ELSE '(' || uk.predicate || ')' END
                  AND pg_catalog.cardinality(c.conexclop) = pg_catalog.cardinality(k.expected_attnums)
                  AND NOT EXISTS (
                      SELECT 1
                      FROM pg_catalog.unnest(c.conexclop) WITH ORDINALITY AS x(oproid, n)
                      JOIN pg_catalog.pg_operator AS o ON o.oid = x.oproid
                      WHERE (x.n <= pg_catalog.cardinality(uk.column_names) AND o.oprname <> '=')
                         OR (x.n = pg_catalog.cardinality(k.expected_attnums) AND o.oprname <> '&&')))
        END AS exclude_attributable,
        uk.check_constraint IS NULL AS check_attributable,
        k.table_oid IS NULL
        OR (uk.unique_constraint IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint AS c
                            WHERE c.conrelid = k.table_oid AND c.conname = uk.unique_constraint)
            AND pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.unique_constraint)) IS NULL)
        OR (uk.exclude_constraint IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint AS c
                            WHERE c.conrelid = k.table_oid AND c.conname = uk.exclude_constraint)) AS object_missing
    FROM expected AS k
    CROSS JOIN LATERAL (SELECT k.*) AS uk;
$__internal_unique_key_inventory$;

CREATE FUNCTION sql_saga.__internal_unique_key_attribution(unique_key_name name)
RETURNS TABLE(unique_attributable boolean, exclude_attributable boolean, check_attributable boolean)
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_unique_key_attribution$
    WITH key AS (
        SELECT uk.*, e.range_column_name, t.oid AS table_oid, tn.oid AS table_namespace_oid
        FROM sql_saga.unique_keys AS uk
        LEFT JOIN sql_saga.era AS e USING (table_schema, table_name, era_name)
        LEFT JOIN pg_catalog.pg_namespace AS tn ON tn.nspname = uk.table_schema
        LEFT JOIN pg_catalog.pg_class AS t ON t.relnamespace = tn.oid AND t.relname = uk.table_name
        WHERE uk.unique_key_name = $1
    ), expected AS (
        SELECT k.*, x.expected_attnums,
               pg_catalog.cardinality(x.expected_attnums) = pg_catalog.cardinality(k.column_names) + 1 AS columns_valid
        FROM key AS k
        LEFT JOIN LATERAL (
            SELECT pg_catalog.array_agg(a.attnum::smallint ORDER BY u.n) AS expected_attnums
            FROM pg_catalog.unnest(k.column_names || k.range_column_name) WITH ORDINALITY AS u(column_name, n)
            JOIN pg_catalog.pg_attribute AS a
              ON a.attrelid = k.table_oid AND a.attname = u.column_name
             AND a.attnum > 0 AND NOT a.attisdropped
        ) AS x ON true
    )
    SELECT CASE
               WHEN k.table_oid IS NULL OR NOT k.columns_valid THEN false
               WHEN k.unique_constraint IS NULL THEN true
               WHEN k.key_type = 'predicated' THEN
                   ui.indexrelid IS NOT NULL AND ui.indrelid = k.table_oid
                   AND ui.indisunique AND ui.indisvalid AND ui.indisready
                   AND ui.indnkeyatts = pg_catalog.cardinality(k.expected_attnums)
                   AND ui.indkey::text = pg_catalog.array_to_string(k.expected_attnums, ' ')
                   AND ui.indexprs IS NULL AND uam.amname = 'btree'
                   AND pg_catalog.regexp_replace(
                           pg_catalog.regexp_replace(pg_catalog.pg_get_expr(ui.indpred, ui.indrelid),
                               '::(text|character varying)', '', 'g'),
                           '\(([[:alnum:]_".]+)\)', '\1', 'g')
                       IS NOT DISTINCT FROM '(' || k.predicate || ')'
               ELSE uc.oid IS NOT NULL
                   AND uc.contype = CASE k.key_type WHEN 'primary' THEN 'p' ELSE 'u' END
                   AND uc.conkey = k.expected_attnums AND uc.conperiod
                   AND NOT uc.condeferrable AND NOT uc.condeferred
                   AND ui.indrelid = k.table_oid AND ui.indisunique AND ui.indisvalid AND ui.indisready
                   AND ui.indnkeyatts = pg_catalog.cardinality(k.expected_attnums)
                   AND ui.indkey::text = pg_catalog.array_to_string(k.expected_attnums, ' ')
                   AND ui.indexprs IS NULL AND ui.indpred IS NULL AND uam.amname = 'gist'
           END,
           CASE
               WHEN k.exclude_constraint IS NULL THEN true
               WHEN k.table_oid IS NULL OR NOT k.columns_valid THEN false
               ELSE ec.oid IS NOT NULL AND ec.contype = 'x' AND ec.conkey = k.expected_attnums
                   AND ec.condeferrable AND NOT ec.condeferred
                   AND ei.indrelid = k.table_oid AND ei.indisvalid AND ei.indisready
                   AND ei.indnkeyatts = pg_catalog.cardinality(k.expected_attnums)
                   AND ei.indkey::text = pg_catalog.array_to_string(k.expected_attnums, ' ')
                   AND ei.indexprs IS NULL AND eam.amname = 'gist'
                   AND pg_catalog.regexp_replace(
                           pg_catalog.regexp_replace(pg_catalog.pg_get_expr(ei.indpred, ei.indrelid),
                               '::(text|character varying)', '', 'g'),
                           '\(([[:alnum:]_".]+)\)', '\1', 'g')
                       IS NOT DISTINCT FROM CASE WHEN k.predicate IS NULL THEN NULL ELSE '(' || k.predicate || ')' END
                   AND pg_catalog.cardinality(ec.conexclop) = pg_catalog.cardinality(k.expected_attnums)
                   AND NOT EXISTS (
                       SELECT 1
                       FROM pg_catalog.unnest(ec.conexclop) WITH ORDINALITY AS x(oproid, n)
                       JOIN pg_catalog.pg_operator AS o ON o.oid = x.oproid
                       WHERE (x.n <= pg_catalog.cardinality(k.column_names) AND o.oprname <> '=')
                          OR (x.n = pg_catalog.cardinality(k.expected_attnums) AND o.oprname <> '&&'))
           END,
           k.check_constraint IS NULL
    FROM expected AS k
    LEFT JOIN pg_catalog.pg_constraint AS uc
      ON k.key_type <> 'predicated' AND uc.conrelid = k.table_oid AND uc.conname = k.unique_constraint
    LEFT JOIN pg_catalog.pg_class AS uic
      ON uic.relnamespace = k.table_namespace_oid AND uic.relname = k.unique_constraint
    LEFT JOIN pg_catalog.pg_index AS ui
      ON ui.indexrelid = CASE WHEN k.key_type = 'predicated' THEN uic.oid ELSE uc.conindid END
    LEFT JOIN pg_catalog.pg_class AS uidx ON uidx.oid = ui.indexrelid
    LEFT JOIN pg_catalog.pg_am AS uam ON uam.oid = uidx.relam
    LEFT JOIN pg_catalog.pg_constraint AS ec
      ON k.exclude_constraint IS NOT NULL AND ec.conrelid = k.table_oid AND ec.conname = k.exclude_constraint
    LEFT JOIN pg_catalog.pg_index AS ei ON ei.indexrelid = ec.conindid
    LEFT JOIN pg_catalog.pg_class AS eidx ON eidx.oid = ei.indexrelid
    LEFT JOIN pg_catalog.pg_am AS eam ON eam.oid = eidx.relam;
$__internal_unique_key_attribution$;

CREATE FUNCTION sql_saga.__internal_temporal_foreign_key_is_attributable(foreign_key_name name)
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_temporal_foreign_key_is_attributable$
    WITH family AS (
        SELECT fk.*,
               pg_catalog.to_regclass(pg_catalog.format('%I.%I', fk.table_schema, fk.table_name))::oid AS fk_table_oid,
               pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.table_name))::oid AS uk_table_oid,
               fk_era.range_column_name AS fk_range_column_name,
               uk_era.range_column_name AS uk_range_column_name,
               uk.column_names AS uk_column_names
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        LEFT JOIN sql_saga.era AS fk_era
          ON (fk_era.table_schema, fk_era.table_name, fk_era.era_name)
           = (fk.table_schema, fk.table_name, fk.fk_era_name)
        LEFT JOIN sql_saga.era AS uk_era
          ON (uk_era.table_schema, uk_era.table_name, uk_era.era_name)
           = (uk.table_schema, uk.table_name, uk.era_name)
        WHERE fk.foreign_key_name = $1 AND fk.type = 'temporal_to_temporal'
    ), expected AS (
        SELECT f.*,
               fk_cols.attnums AS fk_attnums,
               uk_cols.attnums AS uk_attnums
        FROM family AS f
        LEFT JOIN LATERAL (
            SELECT pg_catalog.array_agg(a.attnum::smallint ORDER BY u.n) AS attnums
            FROM pg_catalog.unnest(f.column_names || f.fk_range_column_name) WITH ORDINALITY AS u(column_name, n)
            JOIN pg_catalog.pg_attribute AS a
              ON a.attrelid = f.fk_table_oid AND a.attname = u.column_name
             AND a.attnum > 0 AND NOT a.attisdropped
        ) AS fk_cols ON true
        LEFT JOIN LATERAL (
            SELECT pg_catalog.array_agg(a.attnum::smallint ORDER BY u.n) AS attnums
            FROM pg_catalog.unnest(f.uk_column_names || f.uk_range_column_name) WITH ORDINALITY AS u(column_name, n)
            JOIN pg_catalog.pg_attribute AS a
              ON a.attrelid = f.uk_table_oid AND a.attname = u.column_name
             AND a.attnum > 0 AND NOT a.attisdropped
        ) AS uk_cols ON true
    )
    SELECT coalesce(bool_and(
        c.oid IS NOT NULL
        AND c.contype = 'f'
        AND c.conrelid = e.fk_table_oid
        AND c.confrelid = e.uk_table_oid
        AND c.conkey = e.fk_attnums
        AND c.confkey = e.uk_attnums
        AND c.conperiod
        AND c.condeferrable
        AND NOT c.condeferred
        AND c.convalidated
        AND c.conenforced
        AND c.confmatchtype = CASE e.match_type WHEN 'FULL' THEN 'f' WHEN 'PARTIAL' THEN 'p' ELSE 's' END
        AND c.confupdtype = CASE e.update_action::text WHEN 'RESTRICT' THEN 'r' WHEN 'CAS' || 'CADE' THEN 'c' WHEN 'SET NULL' THEN 'n' WHEN 'SET DEFAULT' THEN 'd' ELSE 'a' END
        AND c.confdeltype = CASE e.delete_action::text WHEN 'RESTRICT' THEN 'r' WHEN 'CAS' || 'CADE' THEN 'c' WHEN 'SET NULL' THEN 'n' WHEN 'SET DEFAULT' THEN 'd' ELSE 'a' END), false)
    FROM expected AS e
    LEFT JOIN pg_catalog.pg_constraint AS c
      ON c.conrelid = e.fk_table_oid AND c.conname = e.foreign_key_name;
$__internal_temporal_foreign_key_is_attributable$;

CREATE FUNCTION sql_saga.__internal_unique_key_object_is_attributable(
    unique_key_name name,
    object_kind text)
RETURNS boolean
LANGUAGE plpgsql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_unique_key_object_is_attributable$
#variable_conflict use_variable
DECLARE
    uk sql_saga.unique_keys;
    table_oid oid;
    range_column name;
    expected_attnums smallint[];
    object_oid oid;
    attributable boolean;
BEGIN
    SELECT x.* INTO uk FROM sql_saga.unique_keys AS x
    WHERE x.unique_key_name = $1;
    IF NOT FOUND THEN RETURN false; END IF;

    IF object_kind = 'exclude' AND uk.exclude_constraint IS NULL THEN RETURN true; END IF;
    IF object_kind = 'check' THEN RETURN uk.check_constraint IS NULL; END IF;

    IF object_kind = 'unique' AND uk.key_type <> 'predicated' THEN
        table_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.table_name));
        IF table_oid IS NULL THEN RETURN false; END IF;
        SELECT e.range_column_name INTO range_column
        FROM sql_saga.era AS e
        WHERE (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name);
        IF range_column IS NULL THEN RETURN false; END IF;
        SELECT pg_catalog.array_agg(a.attnum::smallint ORDER BY u.n)
        INTO expected_attnums
        FROM pg_catalog.unnest(uk.column_names || range_column) WITH ORDINALITY AS u(column_name, n)
        JOIN pg_catalog.pg_attribute AS a
          ON a.attrelid = table_oid AND a.attname = u.column_name
         AND a.attnum > 0 AND NOT a.attisdropped;
        IF pg_catalog.cardinality(expected_attnums) IS DISTINCT FROM pg_catalog.cardinality(uk.column_names) + 1 THEN
            RETURN false;
        END IF;
        RETURN EXISTS (
            SELECT 1
            FROM pg_catalog.pg_constraint AS c
            JOIN pg_catalog.pg_index AS i ON i.indexrelid = c.conindid
            JOIN pg_catalog.pg_class AS ic ON ic.oid = i.indexrelid
            JOIN pg_catalog.pg_am AS am ON am.oid = ic.relam
            WHERE c.conrelid = table_oid AND c.conname = uk.unique_constraint
              AND c.contype = CASE uk.key_type WHEN 'primary' THEN 'p' ELSE 'u' END
              AND c.conkey = expected_attnums AND c.conperiod
              AND NOT c.condeferrable AND NOT c.condeferred
              AND i.indrelid = table_oid AND i.indisunique AND i.indisvalid AND i.indisready
              AND i.indnkeyatts = pg_catalog.cardinality(expected_attnums)
              AND i.indkey::text = pg_catalog.array_to_string(expected_attnums, ' ')
              AND i.indexprs IS NULL AND i.indpred IS NULL AND am.amname = 'gist');
    END IF;

    SELECT CASE object_kind
        WHEN 'unique' THEN a.unique_attributable
        WHEN 'exclude' THEN a.exclude_attributable
        WHEN 'check' THEN a.check_attributable
        ELSE false
    END
    INTO attributable
    FROM sql_saga.__internal_unique_key_attribution(unique_key_name) AS a;
    RETURN attributable;
END;
$__internal_unique_key_object_is_attributable$;

CREATE FUNCTION sql_saga.check_integrity()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = sql_saga, pg_catalog
AS $check_integrity$
DECLARE
    r record;
    base_oid oid;
    history_oid oid;
    view_oid oid;
    expected_columns name[];
    function_oid oid;
    expected_return_type oid;
    missing_column name;
BEGIN
    /* Base relation and era invariants are the first dependency layer. */
    FOR r IN
        SELECT e.*,
               pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))::oid AS inventory_table_oid,
               CASE
                   WHEN pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name)) IS NULL THEN 1
                   WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_class AS c
                               WHERE c.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                                 AND (c.relkind <> 'r' OR c.relpersistence <> 'p')) THEN 2
                   WHEN (e.sync_temporal_trg_function_name IS NOT NULL
                         AND e.sync_temporal_trg_function_name <> sql_saga.__internal_make_name(
                             ARRAY[e.table_schema, e.table_name, e.era_name], 'template_sync'))
                     OR EXISTS (SELECT 1 FROM sql_saga.system_time_era AS ste
                                WHERE (ste.table_schema, ste.table_name) = (e.table_schema, e.table_name)
                                  AND NOT EXISTS (SELECT 1 FROM sql_saga.era AS x
                                                  WHERE (x.table_schema, x.table_name, x.era_name) =
                                                        (ste.table_schema, ste.table_name, ste.era_name)))
                     OR EXISTS (SELECT 1 FROM sql_saga.unique_keys AS uk
                                WHERE (uk.table_schema, uk.table_name) = (e.table_schema, e.table_name)
                                  AND NOT EXISTS (SELECT 1 FROM sql_saga.era AS x
                                                  WHERE (x.table_schema, x.table_name, x.era_name) =
                                                        (uk.table_schema, uk.table_name, uk.era_name))) THEN 3
                   WHEN NOT EXISTS (
                       SELECT 1 FROM pg_catalog.pg_attribute AS a
                       JOIN pg_catalog.pg_range AS pr ON pr.rngtypid = e.range_type
                       WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                         AND a.attname = e.range_column_name AND a.attnum > 0 AND NOT a.attisdropped
                         AND a.atttypid = e.range_type AND pr.rngmultitypid = e.multirange_type
                         AND pr.rngsubtype = e.range_subtype
                         AND (SELECT t.typcategory FROM pg_catalog.pg_type AS t WHERE t.oid = e.range_subtype) = e.range_subtype_category) THEN 4
                   WHEN (e.valid_from_column_name IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_attribute AS a
                            WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND a.attname = e.valid_from_column_name AND a.atttypid = e.range_subtype
                              AND a.attnum > 0 AND NOT a.attisdropped))
                     OR (e.valid_until_column_name IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_attribute AS a
                            WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND a.attname = e.valid_until_column_name AND a.atttypid = e.range_subtype
                              AND a.attnum > 0 AND NOT a.attisdropped))
                     OR (e.bounds_check_constraint IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_constraint AS c
                            WHERE c.conrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND c.conname = e.bounds_check_constraint AND c.contype = 'c'
                              AND pg_catalog.pg_get_constraintdef(c.oid, false) LIKE '%NOT isempty(%'))
                     OR (e.boundary_check_constraint IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_constraint AS c
                            WHERE c.conrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND c.conname = e.boundary_check_constraint AND c.contype = 'c'
                              AND pg_catalog.pg_get_constraintdef(c.oid, false) LIKE '% < %'))
                     OR (e.sync_temporal_trg_name IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_trigger AS t
                            WHERE t.tgrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND t.tgname = e.sync_temporal_trg_name AND NOT t.tgisinternal AND t.tgtype = 23
                              AND t.tgfoid = pg_catalog.to_regprocedure(
                                  pg_catalog.format('sql_saga.%I()', e.sync_temporal_trg_function_name)))) THEN 5
               END AS violation
        FROM sql_saga.era AS e
        ORDER BY e.table_schema COLLATE "C", e.table_name COLLATE "C", e.era_name COLLATE "C"
    LOOP
        base_oid := r.inventory_table_oid;
        IF r.violation = 1 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%.%": catalogued base table is missing',
                r.table_schema, r.table_name;
        END IF;
        IF r.violation = 2 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": catalogued base relation kind or persistence is noncanonical; repair with drop_era and add_era',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF r.violation = 3 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": catalogue relationships are incoherent; repair with drop_era and add_era',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF r.violation = 4 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for era "%" on table "%": column or range type metadata is noncanonical; repair with drop_era and add_era',
                r.era_name, sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF r.violation = 5 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for era "%" on table "%": named check or synchronization object is noncanonical; repair with drop_era and add_era',
                r.era_name, sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
    END LOOP;

    /* SYSTEM_TIME invariants have the highest managed-object priority. */
    FOR r IN
        SELECT ste.*, e.range_column_name
        FROM sql_saga.system_time_era AS ste
        JOIN sql_saga.era AS e USING (table_schema, table_name, era_name)
        ORDER BY ste.table_schema COLLATE "C", ste.table_name COLLATE "C"
    LOOP
        base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
        IF base_oid IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%.%": catalogued base table is missing', r.table_schema, r.table_name;
        END IF;

        IF NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.generated_always_trigger)
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.write_history_trigger)
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.truncate_trigger) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": SYSTEM_TIME trigger is missing; repair with drop_system_versioning and add_system_versioning',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;

        IF NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_constraint AS c
                WHERE c.conrelid = base_oid AND c.conname = r.infinity_check_constraint
                  AND c.contype = 'c'
                  AND pg_catalog.pg_get_constraintdef(c.oid, false) LIKE '%upper(%''infinity''%')
           OR NOT sql_saga.__internal_trigger_is_attributable(
                base_oid, r.generated_always_trigger,
                'sql_saga.generated_always_as_row_start_end()'::regprocedure, 23, r.range_column_name)
           OR NOT sql_saga.__internal_trigger_is_attributable(
                base_oid, r.write_history_trigger,
                'sql_saga.write_history()'::regprocedure, 25, r.range_column_name)
           OR NOT sql_saga.__internal_trigger_is_attributable(
                base_oid, r.truncate_trigger,
                'sql_saga.truncate_system_versioning()'::regprocedure, 32, NULL) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": SYSTEM_TIME trigger definition is noncanonical; repair with drop_system_versioning and add_system_versioning',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;

        SELECT x.attname INTO missing_column
        FROM pg_catalog.unnest(r.excluded_column_names) AS x(attname)
        WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = base_oid AND a.attname = x.attname AND a.attnum > 0 AND NOT a.attisdropped)
        ORDER BY x.attname COLLATE "C" LIMIT 1;
        IF FOUND THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": excluded SYSTEM_TIME column "%" is missing; repair with set_system_time_era_excluded_columns or drop_system_versioning',
                sql_saga.__internal_display_name(base_oid::regclass), missing_column;
        END IF;

        /* The snapshot detects added and removed columns only; compare it as a set, because a
         * restore recreates inherited children with inherited columns first. */
        SELECT pg_catalog.array_agg(a.attname ORDER BY a.attname COLLATE "C") INTO expected_columns
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = base_oid AND a.attnum > 0 AND NOT a.attisdropped;
        IF expected_columns IS DISTINCT FROM (
                SELECT pg_catalog.array_agg(s.attname ORDER BY s.attname COLLATE "C")
                FROM pg_catalog.unnest(r.table_columns_snapshot) AS s(attname)) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": SYSTEM_TIME column snapshot is stale; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
    END LOOP;

    FOR r IN
        SELECT uk.*, i.table_oid, i.object_missing,
               i.unique_attributable, i.exclude_attributable, i.check_attributable
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.__internal_unique_key_inventory() AS i USING (unique_key_name)
        ORDER BY uk.table_schema COLLATE "C", uk.table_name COLLATE "C", uk.unique_key_name COLLATE "C"
    LOOP
        base_oid := r.table_oid;
        IF r.object_missing THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for unique key "%" on table "%": managed constraint or index is missing; repair with drop_unique_key and add_unique_key',
                r.unique_key_name, sql_saga.__internal_display_name(base_oid::regclass);
        END IF;

        IF NOT r.unique_attributable OR NOT r.exclude_attributable OR NOT r.check_attributable THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for unique key "%" on table "%": managed constraint or index is noncanonical; repair with drop_unique_key and add_unique_key',
                r.unique_key_name, sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
    END LOOP;

    FOR r IN
        SELECT fk.*, uk.table_schema AS uk_schema, uk.table_name AS uk_table
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        ORDER BY fk.table_schema COLLATE "C", fk.table_name COLLATE "C", fk.foreign_key_name COLLATE "C"
    LOOP
        IF r.type = 'temporal_to_temporal' THEN
            base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
            IF base_oid IS NULL
               OR NOT sql_saga.__internal_temporal_foreign_key_is_attributable(r.foreign_key_name) THEN
                RAISE EXCEPTION 'sql_saga: integrity violation for foreign key on table "%.%": managed constraint or trigger is noncanonical; repair with drop_foreign_key and add_foreign_key',
                    r.table_schema, r.table_name;
            END IF;
        ELSE
            base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.uk_schema, r.uk_table));
            IF base_oid IS NULL
               OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.uk_update_trigger)
               OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.uk_delete_trigger) THEN
                RAISE EXCEPTION 'sql_saga: integrity violation for foreign key on table "%.%": referenced-side helper trigger is missing; repair with drop_foreign_key and add_regular_foreign_key',
                    r.table_schema, r.table_name;
            END IF;
            IF NOT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_trigger AS t
                    WHERE t.tgrelid = base_oid AND t.tgname = r.uk_update_trigger
                      AND t.tgfoid = 'sql_saga.uk_update_check_c()'::regprocedure)
               OR NOT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_trigger AS t
                    WHERE t.tgrelid = base_oid AND t.tgname = r.uk_delete_trigger
                      AND t.tgfoid = 'sql_saga.uk_delete_check_c()'::regprocedure)
               OR NOT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_constraint AS c
                    WHERE c.conrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name))
                      AND c.conname = r.fk_check_constraint AND c.contype = 'c') THEN
                RAISE EXCEPTION 'sql_saga: integrity violation for foreign key on table "%.%": managed constraint or trigger is noncanonical; repair with drop_foreign_key and add_foreign_key',
                    r.table_schema, r.table_name;
            END IF;
        END IF;
    END LOOP;

    FOR r IN
        SELECT v.* FROM sql_saga.updatable_view AS v
        ORDER BY v.table_schema COLLATE "C", v.table_name COLLATE "C", v.view_type, v.view_schema COLLATE "C", v.view_name COLLATE "C"
    LOOP
        view_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.view_schema, r.view_name));
        IF view_oid IS NULL
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = view_oid AND t.tgname = r.trigger_name) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for updatable view "%.%" of table "%.%": identifier column or trigger is missing; repair with drop_%_view and add_%_view',
                r.view_schema, r.view_name, r.table_schema, r.table_name,
                CASE r.view_type WHEN 'for_portion_of' THEN 'for_portion_of' ELSE 'current' END,
                CASE r.view_type WHEN 'for_portion_of' THEN 'for_portion_of' ELSE 'current' END;
        END IF;
        IF EXISTS (
                SELECT 1
                FROM pg_catalog.pg_attribute AS a
                WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name))
                  AND a.attnum > 0 AND NOT a.attisdropped
                  AND NOT EXISTS (
                      SELECT 1 FROM pg_catalog.pg_attribute AS va
                      WHERE va.attrelid = view_oid AND va.attname = a.attname
                        AND va.atttypid = a.atttypid AND va.attnum > 0 AND NOT va.attisdropped))
           OR NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_trigger AS t
                WHERE t.tgrelid = view_oid AND t.tgname = r.trigger_name
                  AND NOT t.tgisinternal AND t.tgtype = 93
                  AND t.tgfoid = CASE r.view_type
                      WHEN 'for_portion_of' THEN 'sql_saga.for_portion_of_trigger()'::regprocedure
                      ELSE 'sql_saga.current_view_trigger()'::regprocedure END) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for updatable view "%.%" of table "%.%": identifier column or trigger definition is noncanonical; repair with drop_%_view and add_%_view',
                r.view_schema, r.view_name, r.table_schema, r.table_name,
                CASE r.view_type WHEN 'for_portion_of' THEN 'for_portion_of' ELSE 'current' END,
                CASE r.view_type WHEN 'for_portion_of' THEN 'for_portion_of' ELSE 'current' END;
        END IF;
    END LOOP;

    FOR r IN
        SELECT sv.*, a.public_view_attributable, a.gated_view_attributable
        FROM sql_saga.system_versioning AS sv
        CROSS JOIN LATERAL sql_saga.__internal_system_versioning_member_attribution(
            pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name))::oid) AS a
        ORDER BY sv.table_schema COLLATE "C", sv.table_name COLLATE "C"
    LOOP
        base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
        history_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.history_schema_name, r.history_table_name));
        view_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.view_schema_name, r.view_table_name));
        IF view_oid IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history view "%.%" is missing; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass), r.view_schema_name, r.view_table_name;
        END IF;
        IF history_oid IS NULL OR NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_class AS h
                WHERE h.oid = history_oid AND h.relkind = 'r' AND h.relpersistence = 'p') THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history family definition is noncanonical; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF EXISTS (
                SELECT 1 FROM pg_catalog.pg_class AS b JOIN pg_catalog.pg_class AS h ON h.oid = history_oid
                WHERE b.oid = base_oid AND (NOT h.relrowsecurity OR h.relforcerowsecurity <> b.relforcerowsecurity)) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history family definition is noncanonical; repair with sync_history_policies or rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF EXISTS (
                SELECT 1 FROM pg_catalog.pg_class AS b JOIN pg_catalog.pg_class AS h ON h.oid = history_oid
                WHERE b.oid = base_oid AND h.relowner <> b.relowner) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history family definition is noncanonical; repair with drop_system_versioning and add_system_versioning',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF EXISTS (
                SELECT 1 FROM pg_catalog.pg_class AS h
                WHERE h.oid = history_oid AND (
                    EXISTS (SELECT 1 FROM pg_catalog.aclexplode(coalesce(h.relacl, pg_catalog.acldefault('r', h.relowner))) AS x
                            WHERE x.grantee NOT IN (0, h.relowner) OR (x.grantee = 0 AND x.privilege_type <> 'SELECT'))
                    OR NOT EXISTS (SELECT 1 FROM pg_catalog.aclexplode(coalesce(h.relacl, pg_catalog.acldefault('r', h.relowner))) AS x
                                   WHERE x.grantee = 0 AND x.privilege_type = 'SELECT')))
           OR NOT r.public_view_attributable
           OR NOT r.gated_view_attributable THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history family definition is noncanonical; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        BEGIN
            IF history_oid IS NULL OR NOT sql_saga.__internal_history_policies_synchronized(base_oid, history_oid) THEN
                RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'policy mismatch';
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": managed history policy set is inconsistent; repair with sync_history_policies',
                sql_saga.__internal_display_name(base_oid::regclass);
        END;
    END LOOP;

    FOR r IN
        SELECT sv.table_schema, sv.table_name, sv.view_schema_name, sv.view_table_name, q.*,
               CASE q.role
                   WHEN 'as_of' THEN a.as_of_attributable
                   WHEN 'between' THEN a.between_attributable
                   WHEN 'between_symmetric' THEN a.between_symmetric_attributable
                   ELSE a.from_to_attributable
               END AS definition_attributable
        FROM sql_saga.system_versioning AS sv
        CROSS JOIN LATERAL sql_saga.__internal_system_versioning_member_attribution(
            pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name))::oid) AS a
        CROSS JOIN LATERAL sql_saga.__internal_system_versioning_query_functions(
            pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name))::oid) AS q
        ORDER BY sv.table_schema COLLATE "C", sv.table_name COLLATE "C",
                 CASE q.role WHEN 'as_of' THEN 1 WHEN 'between' THEN 2 WHEN 'between_symmetric' THEN 3 ELSE 4 END
    LOOP
        base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
        function_oid := pg_catalog.to_regprocedure(r.identity);
        IF function_oid IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%.%": generated query function role "%" is missing; repair with rebuild_system_versioning_views',
                r.table_schema, r.table_name, r.role;
        END IF;
        SELECT c.reltype INTO expected_return_type FROM pg_catalog.pg_class AS c
        WHERE c.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.view_schema_name, r.view_table_name));
        IF (SELECT p.prorettype FROM pg_catalog.pg_proc AS p WHERE p.oid = function_oid) IS DISTINCT FROM expected_return_type
           OR 1 <> (
                SELECT pg_catalog.count(*)
                FROM pg_catalog.pg_depend AS d
                WHERE d.classid = 'pg_catalog.pg_proc'::regclass
                  AND d.objid = function_oid
                  AND d.refclassid = 'pg_catalog.pg_type'::regclass
                  AND d.deptype = 'n')
           OR NOT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_depend AS d
                JOIN pg_catalog.pg_type AS t ON t.oid = d.refobjid
                WHERE d.classid = 'pg_catalog.pg_proc'::regclass
                  AND d.objid = function_oid
                  AND d.refclassid = 'pg_catalog.pg_type'::regclass
                  AND d.refobjid = expected_return_type
                  AND d.deptype = 'n'
                  AND t.typrelid = pg_catalog.to_regclass(
                      pg_catalog.format('%I.%I', r.view_schema_name, r.view_table_name))) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%.%": generated query function role "%" has a noncanonical return-type dependency; repair with rebuild_system_versioning_views',
                r.table_schema, r.table_name, r.role;
        END IF;
        IF NOT r.definition_attributable THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%.%": generated query function role "%" definition is noncanonical; repair with rebuild_system_versioning_views',
                r.table_schema, r.table_name, r.role;
        END IF;
    END LOOP;
END;
$check_integrity$;

REVOKE ALL ON FUNCTION sql_saga.check_integrity() FROM PUBLIC;

COMMENT ON FUNCTION sql_saga.check_integrity() IS
'Validates all catalogued sql_saga families without modifying them, and raises on the first deterministic integrity violation.';

COMMENT ON FUNCTION sql_saga.health_checks() IS
'An event trigger function that runs after DDL commands to check for inconsistencies in sql_saga''s metadata catalogs.';
