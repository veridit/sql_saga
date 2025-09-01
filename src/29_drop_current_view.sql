CREATE FUNCTION sql_saga.drop_current_view(table_oid regclass, era_name name DEFAULT 'valid', drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT')
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    view_schema name;
    view_name name;
    trigger_name name;
    target_schema_name name;
    target_table_name name;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'table_oid must be specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    SELECT n.nspname, c.relname
    INTO target_schema_name, target_table_name
    FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = table_oid;

    FOR view_schema, view_name, trigger_name IN
        DELETE FROM sql_saga.updatable_view AS v
        WHERE (v.table_schema, v.table_name) = (target_schema_name, target_table_name)
          AND v.era_name = era_name
          AND v.view_type = 'current'
        RETURNING v.view_schema, v.view_name, v.trigger_name
    LOOP
        EXECUTE format('DROP TRIGGER %I on %I.%I', trigger_name, view_schema, view_name);
        EXECUTE format('DROP VIEW %I.%I %s', view_schema, view_name, drop_behavior);
    END LOOP;

    RETURN true;
END;
$function$;
