CREATE OR REPLACE FUNCTION sql_saga.__internal_set_alter_function_resolved(function_identity text)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, public
AS
$function$
DECLARE
    resolved text;
BEGIN
    /* Every start invocation replaces any prior statement's consumed value. */
    IF function_identity IS NULL THEN
        PERFORM set_config('sql_saga.alter_function_resolved', '', true);
        RETURN;
    END IF;

    SELECT coalesce(jsonb_agg(jsonb_build_object(
            'table_schema', f.table_schema,
            'table_name', f.table_name,
            'role', f.role,
            'identity', f.identity)
        ORDER BY f.table_schema, f.table_name, f.role)::text, '[]')
    INTO resolved
    FROM (
        SELECT sv.table_schema, sv.table_name, 'as_of' AS role, sv.func_as_of AS identity
        FROM sql_saga.system_versioning AS sv
        WHERE sv.func_as_of = function_identity
        UNION ALL
        SELECT sv.table_schema, sv.table_name, 'between', sv.func_between
        FROM sql_saga.system_versioning AS sv
        WHERE sv.func_between = function_identity
        UNION ALL
        SELECT sv.table_schema, sv.table_name, 'between_symmetric', sv.func_between_symmetric
        FROM sql_saga.system_versioning AS sv
        WHERE sv.func_between_symmetric = function_identity
        UNION ALL
        SELECT sv.table_schema, sv.table_name, 'from_to', sv.func_from_to
        FROM sql_saga.system_versioning AS sv
        WHERE sv.func_from_to = function_identity
    ) AS f
    WHERE to_regprocedure(f.identity) IS NOT NULL;

    /* Catalog values only: never persist an OID or reg* value across the statement. */
    PERFORM set_config('sql_saga.alter_function_resolved', resolved, true);
END;
$function$;

REVOKE ALL ON FUNCTION sql_saga.__internal_set_alter_function_resolved(text) FROM PUBLIC;

COMMENT ON FUNCTION sql_saga.__internal_set_alter_function_resolved(text) IS
'Records a parsed ALTER FUNCTION or ALTER ROUTINE target when it is a resolving generated SYSTEM VERSIONING function identity.';
