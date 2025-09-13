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
    --    save_search_path := pg_catalog.current_setting('search_path');
    --    PERFORM pg_catalog.set_config('search_path', 'pg_catalog, pg_temp', true);
    --    FOR r IN
    --        SELECT *
    --        FROM sql_saga.era AS sv
    --        CROSS JOIN LATERAL UNNEST(ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]) AS u (fn)
    --        WHERE NOT EXISTS (
    --            SELECT FROM pg_catalog.pg_proc AS p
    --            WHERE p.oid::regprocedure::text = u.fn
    --        )
    --    LOOP
    --        RAISE EXCEPTION 'cannot drop or rename function "%" because it is used in SYSTEM VERSIONING for table "%"',
    --            r.fn, r.table_oid;
    --    END LOOP;
    --    PERFORM pg_catalog.set_config('search_path', save_search_path, true);

    /* Fix up history and for-portion objects ownership */
    FOR cmd IN
        --        SELECT format('ALTER %s %s OWNER TO %I',
        --            CASE ht.relkind
        --                WHEN 'p' THEN 'TABLE'
        --                WHEN 'r' THEN 'TABLE'
        --                WHEN 'v' THEN 'VIEW'
        --            END,
        --            ht.oid::regclass, t.relowner::regrole)
        --        FROM sql_saga.system_versioning AS sv
        --        JOIN pg_class AS t ON t.oid = sv.table_name
        --        JOIN pg_class AS ht ON ht.oid IN (sv.audit_table_name, sv.view_oid)
        --        WHERE t.relowner <> ht.relowner
        --
        --        UNION ALL

        SELECT format('ALTER VIEW %I.%I OWNER TO %I', v.view_schema /* %I */, v.view_name /* %I */, t.relowner::regrole /* %I */)
        FROM sql_saga.updatable_view v
        JOIN pg_class t ON t.relname = v.table_name
        JOIN pg_namespace tn ON tn.oid = t.relnamespace AND tn.nspname = v.table_schema
        JOIN pg_class vt ON vt.relname = v.view_name
        JOIN pg_namespace vn ON vn.oid = vt.relnamespace AND vn.nspname = v.view_schema
        WHERE t.relowner <> vt.relowner

        --        UNION ALL
        --
        --        SELECT format('ALTER FUNCTION %s OWNER TO %I', p.oid::regprocedure, t.relowner::regrole)
        --        FROM sql_saga.system_versioning AS sv
        --        JOIN pg_class AS t ON t.oid = sv.table_name
        --        JOIN pg_proc AS p ON p.oid = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]::regprocedure[])
        --        WHERE t.relowner <> p.proowner
    LOOP
        EXECUTE cmd;
    END LOOP;

    /* Check GRANTs */
    IF EXISTS (
        SELECT FROM pg_event_trigger_ddl_commands() AS ev_ddl
        WHERE ev_ddl.command_tag = 'GRANT')
    THEN
        FOR r IN
            SELECT *,
                   EXISTS (
                       SELECT
                       FROM pg_class AS _c
                       CROSS JOIN LATERAL aclexplode(COALESCE(_c.relacl, acldefault('r', _c.relowner))) AS _acl
                       WHERE _c.oid = objects.table_oid
                         AND _acl.grantee = objects.grantee
                         AND _acl.privilege_type = objects.base_privilege_type
                   ) AS on_base_table
            FROM (
--                SELECT sv.table_oid,
--                       c.oid::regclass::text AS object_name,
--                       c.relkind AS object_type,
--                       acl.privilege_type,
--                       acl.privilege_type AS base_privilege_type,
--                       acl.grantee,
--                       'h' AS history_or_portion
--                FROM sql_saga.system_versioning AS sv
--                JOIN pg_class AS c ON c.oid IN (sv.audit_table_name, sv.view_oid)
--                CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
--
--                UNION ALL
--
                SELECT to_regclass(format('%I.%I', v.table_schema /* %I */, v.table_name /* %I */)) AS table_oid,
                       vt.oid::regclass::text AS object_name,
                       vt.relkind AS object_type,
                       acl.privilege_type,
                       acl.privilege_type AS base_privilege_type,
                       acl.grantee,
                       'p' AS history_or_portion,
                       v.view_type
                FROM sql_saga.updatable_view v
                JOIN pg_class vt ON vt.relname = v.view_name
                JOIN pg_namespace vn ON vn.oid = vt.relnamespace AND vn.nspname = v.view_schema
                CROSS JOIN LATERAL aclexplode(COALESCE(vt.relacl, acldefault('r', vt.relowner))) AS acl

--                UNION ALL
--
--                SELECT sv.table_oid,
--                       p.oid::regprocedure::text,
--                       'f',
--                       acl.privilege_type,
--                       'SELECT',
--                       acl.grantee,
--                       'h'
--                FROM sql_saga.system_versioning AS sv
--                JOIN pg_proc AS p ON p.oid = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]::regprocedure[])
--                CROSS JOIN LATERAL aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) AS acl
            ) AS objects
            ORDER BY object_name, object_type, privilege_type
        LOOP
            IF
                r.history_or_portion = 'h' AND
                (r.object_type, r.privilege_type) NOT IN (('r', 'SELECT'), ('p', 'SELECT'), ('v', 'SELECT'), ('f', 'EXECUTE'))
            THEN
                RAISE EXCEPTION 'cannot grant % to "%"; history objects are read-only',
                    r.privilege_type, r.object_name;
            END IF;

            IF NOT r.on_base_table THEN
                RAISE EXCEPTION 'cannot grant % directly to "%"; grant % to "%" instead',
                    r.privilege_type, r.object_name, r.base_privilege_type, r.table_oid;
            END IF;
        END LOOP;

        /* Propagate GRANTs */
        FOR cmd IN
            SELECT format('GRANT %s ON %s %s TO %s',
                          string_agg(DISTINCT privilege_type, ', '), /* %s */
                          object_type, /* %s */
                          string_agg(DISTINCT object_name, ', '), /* %s */
                          string_agg(DISTINCT COALESCE(a.rolname, 'public'), ', ') /* %s */
            )
            FROM (
--                SELECT 'TABLE' AS object_type,
--                       hc.oid::regclass::text AS object_name,
--                       'SELECT' AS privilege_type,
--                       acl.grantee
--                FROM sql_saga.system_versioning AS sv
--                JOIN pg_class AS c ON c.oid = sv.table_name
--                CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
--                JOIN pg_class AS hc ON hc.oid IN (sv.audit_table_name, sv.view_oid)
--                WHERE acl.privilege_type = 'SELECT'
--                  AND NOT has_table_privilege(acl.grantee, hc.oid, 'SELECT')
--
--                UNION ALL
--
                SELECT 'TABLE' AS object_type,
                       vt.oid::regclass::text AS object_name,
                       acl.privilege_type AS privilege_type,
                       acl.grantee
                FROM sql_saga.updatable_view v
                JOIN pg_class t ON t.relname = v.table_name
                JOIN pg_namespace tn ON tn.oid = t.relnamespace AND tn.nspname = v.table_schema
                CROSS JOIN LATERAL aclexplode(COALESCE(t.relacl, acldefault('r', t.relowner))) AS acl
                JOIN pg_class vt ON vt.relname = v.view_name
                JOIN pg_namespace vn ON vn.oid = vt.relnamespace AND vn.nspname = v.view_schema
                WHERE NOT has_table_privilege(acl.grantee, vt.oid, acl.privilege_type)

--                UNION ALL
--
--                SELECT 'FUNCTION',
--                       hp.oid::regprocedure::text,
--                       'EXECUTE',
--                       acl.grantee
--                FROM sql_saga.system_versioning AS sv
--                JOIN pg_class AS c ON c.oid = sv.table_name
--                CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
--                JOIN pg_proc AS hp ON hp.oid = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]::regprocedure[])
--                WHERE acl.privilege_type = 'SELECT'
--                  AND NOT has_function_privilege(acl.grantee, hp.oid, 'EXECUTE')
            ) AS objects
            LEFT JOIN pg_authid AS a ON a.oid = objects.grantee
            GROUP BY object_type
        LOOP
            SET session_replication_role = 'replica';
            EXECUTE cmd;
            RESET session_replication_role;
        END LOOP;
    END IF;

    /* Check REVOKEs */
    IF EXISTS (
        SELECT FROM pg_event_trigger_ddl_commands() AS ev_ddl
        WHERE ev_ddl.command_tag = 'REVOKE')
    THEN
        FOR r IN
--            SELECT sv.table_name,
--                   hc.oid::regclass::text AS object_name,
--                   acl.privilege_type,
--                   acl.privilege_type AS base_privilege_type
--            FROM sql_saga.system_versioning AS sv
--            JOIN pg_class AS c ON c.oid = sv.table_name
--            CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
--            JOIN pg_class AS hc ON hc.oid IN (sv.audit_table_name, sv.view_oid)
--            WHERE acl.privilege_type = 'SELECT'
--              AND NOT EXISTS (
--                SELECT
--                FROM aclexplode(COALESCE(hc.relacl, acldefault('r', hc.relowner))) AS _acl
--                WHERE _acl.privilege_type = 'SELECT'
--                  AND _acl.grantee = acl.grantee)
--
--            UNION ALL

            SELECT to_regclass(format('%I.%I', v.table_schema /* %I */, v.table_name /* %I */)) AS table_oid,
                   vt.oid::regclass::text AS object_name,
                   acl.privilege_type,
                   acl.privilege_type AS base_privilege_type
            FROM sql_saga.updatable_view v
            JOIN pg_class t ON t.relname = v.table_name
            JOIN pg_namespace tn ON tn.oid = t.relnamespace AND tn.nspname = v.table_schema
            CROSS JOIN LATERAL aclexplode(COALESCE(t.relacl, acldefault('r', t.relowner))) AS acl
            JOIN pg_class vt ON vt.relname = v.view_name
            JOIN pg_namespace vn ON vn.oid = vt.relnamespace AND vn.nspname = v.view_schema
            WHERE NOT EXISTS (
                SELECT
                FROM aclexplode(COALESCE(vt.relacl, acldefault('r', vt.relowner))) AS _acl
                WHERE _acl.privilege_type = acl.privilege_type
                  AND _acl.grantee = acl.grantee)

--            UNION ALL
--
--            SELECT sv.table_name,
--                   hp.oid::regprocedure::text,
--                   'EXECUTE',
--                   'SELECT'
--            FROM sql_saga.system_versioning AS sv
--            JOIN pg_class AS c ON c.oid = sv.table_name
--            CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
--            JOIN pg_proc AS hp ON hp.oid = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]::regprocedure[])
--            WHERE acl.privilege_type = 'SELECT'
--              AND NOT EXISTS (
--                SELECT
--                FROM aclexplode(COALESCE(hp.proacl, acldefault('f', hp.proowner))) AS _acl
--                WHERE _acl.privilege_type = 'EXECUTE'
--                  AND _acl.grantee = acl.grantee)
--
            ORDER BY table_oid, object_name
        LOOP
            RAISE EXCEPTION 'cannot revoke % directly from "%", revoke % from "%" instead',
                r.privilege_type, r.object_name, r.base_privilege_type, r.table_oid;
        END LOOP;

        /* Propagate REVOKEs */
        FOR cmd IN
            SELECT format('REVOKE %s ON %s %s FROM %s',
                          string_agg(DISTINCT privilege_type, ', '), /* %s */
                          object_type, /* %s */
                          string_agg(DISTINCT object_name, ', '), /* %s */
                          string_agg(DISTINCT COALESCE(a.rolname, 'public'), ', ') /* %s */
            )
            FROM (
--                SELECT 'TABLE' AS object_type,
--                       hc.oid::regclass::text AS object_name,
--                       'SELECT' AS privilege_type,
--                       hacl.grantee
--                FROM sql_saga.system_versioning AS sv
--                JOIN pg_class AS hc ON hc.oid IN (sv.audit_table_name, sv.view_oid)
--                CROSS JOIN LATERAL aclexplode(COALESCE(hc.relacl, acldefault('r', hc.relowner))) AS hacl
--                WHERE hacl.privilege_type = 'SELECT'
--                  AND NOT has_table_privilege(hacl.grantee, sv.table_name, 'SELECT')
--
--                UNION ALL

                SELECT 'TABLE' AS object_type,
                       vt.oid::regclass::text AS object_name,
                       hacl.privilege_type,
                       hacl.grantee
                FROM sql_saga.updatable_view v
                JOIN pg_class vt ON vt.relname = v.view_name
                JOIN pg_namespace vn ON vn.oid = vt.relnamespace AND vn.nspname = v.view_schema
                CROSS JOIN LATERAL aclexplode(COALESCE(vt.relacl, acldefault('r', vt.relowner))) AS hacl
                WHERE NOT has_table_privilege(hacl.grantee, to_regclass(format('%I.%I', v.table_schema /* %I */, v.table_name /* %I */)), hacl.privilege_type)

--                UNION ALL
--
--                SELECT 'FUNCTION' AS object_type,
--                       hp.oid::regprocedure::text AS object_name,
--                       'EXECUTE' AS privilege_type,
--                       hacl.grantee
--                FROM sql_saga.system_versioning AS sv
--                JOIN pg_proc AS hp ON hp.oid = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]::regprocedure[])
--                CROSS JOIN LATERAL aclexplode(COALESCE(hp.proacl, acldefault('f', hp.proowner))) AS hacl
--                WHERE hacl.privilege_type = 'EXECUTE'
--                  AND NOT has_table_privilege(hacl.grantee, sv.table_name, 'SELECT')
            ) AS objects
            LEFT JOIN pg_authid AS a ON a.oid = objects.grantee
            GROUP BY object_type
        LOOP
            SET session_replication_role = 'replica';
            EXECUTE cmd;
            RESET session_replication_role;
        END LOOP;
    END IF;

END;
$function$;

COMMENT ON FUNCTION sql_saga.health_checks() IS
'An event trigger function that runs after DDL commands to check for inconsistencies in sql_saga''s metadata catalogs.';
