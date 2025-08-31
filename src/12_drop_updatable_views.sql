CREATE FUNCTION sql_saga.drop_updatable_views(table_oid regclass, era_name name, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT false)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    view_schema_name name;
    view_table_name name;
    trigger_name name;
    target_schema_name name;
    target_table_name name;
BEGIN
    /*
     * If table_oid and era_name are specified, then just drop the views for that.
     *
     * If no period is specified, drop the views for all periods of the table.
     *
     * If no table is specified, drop the views everywhere.
     *
     * If no table is specified but a period is, that doesn't make any sense.
     */
    IF table_oid IS NULL AND era_name IS NOT NULL THEN
        RAISE EXCEPTION 'cannot specify era name without table name';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    IF table_oid IS NOT NULL THEN
        SELECT n.nspname, c.relname
        INTO target_schema_name, target_table_name
        FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.oid = table_oid;
    END IF;

    FOR view_schema_name, view_table_name, trigger_name IN
        DELETE FROM sql_saga.api_view AS fp
        WHERE (table_oid IS NULL OR (fp.table_schema, fp.table_name) = (target_schema_name, target_table_name))
          AND (era_name IS NULL OR fp.era_name = era_name)
        RETURNING fp.view_schema_name, fp.view_table_name, fp.trigger_name
    LOOP
        EXECUTE format('DROP TRIGGER %I on %I.%I', trigger_name, view_schema_name, view_table_name);
        EXECUTE format('DROP VIEW %I.%I %s', view_schema_name, view_table_name, drop_behavior);
    END LOOP;

    RETURN true;
END;
$function$;
