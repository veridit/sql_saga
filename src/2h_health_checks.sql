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
BEGIN
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
            r.table_oid;
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
            r.table_oid;
    END LOOP;

    /* Check that our system versioning functions are still here */
    FOR r IN
        SELECT to_regclass(format('%I.%I', sv.table_schema /* %I */, sv.table_name /* %I */)) AS table_oid,
               u.fn
        FROM sql_saga.system_versioning AS sv
        CROSS JOIN LATERAL UNNEST(ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]) AS u (fn)
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_proc AS p
            WHERE p.oid = u.fn::regprocedure
        )
    LOOP
        RAISE EXCEPTION 'cannot drop or rename function "%" because it is used in SYSTEM VERSIONING for table "%"',
            r.fn, r.table_oid;
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
        JOIN pg_proc AS p ON p.oid = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]::regprocedure[])
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
                        RAISE EXCEPTION 'sql_saga: policies on history table "%" are managed by sql_saga', r.history_oid::regclass
                            USING HINT = format('Create the policy on "%s"; sql_saga copies its SELECT and ALL policies to the history.',
                                                r.base_oid::regclass);
                    ELSIF r.command_tag = 'ALTER POLICY' THEN
                        RAISE EXCEPTION 'cannot drop or alter policy "%" on "%" because it is managed by sql_saga for table "%"',
                            managed_policy.polname, r.history_oid::regclass, r.base_oid::regclass;
                    END IF;
                END IF;
            END IF;
            -- Refuse direct changes to the history's RLS settings.
            IF r.command_tag = 'ALTER TABLE' AND r.objid = r.history_oid
               AND NOT EXISTS (SELECT FROM pg_catalog.pg_class AS h, pg_catalog.pg_class AS b
                               WHERE h.oid = r.history_oid AND b.oid = r.base_oid
                                 AND h.relrowsecurity AND h.relforcerowsecurity = b.relforcerowsecurity) THEN
                RAISE EXCEPTION 'history table "%" must keep row level security enabled, and forced exactly when its base table is, because it is used in SYSTEM VERSIONING for table "%"',
                    r.history_oid::regclass, r.base_oid::regclass;
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
                r.view_oid::regclass, r.kind, r.base_oid::regclass;
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
                r.view_oid::regclass, r.base_oid::regclass;
        END IF;
    END LOOP;

END;
$function$;

COMMENT ON FUNCTION sql_saga.health_checks() IS
'An event trigger function that runs after DDL commands to check for inconsistencies in sql_saga''s metadata catalogs.';
