CREATE FUNCTION sql_saga.drop_system_versioning(table_oid regclass, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT true)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
AS $function$
#variable_conflict use_variable
DECLARE
    system_versioning_row sql_saga.system_versioning;
    is_dropped boolean;
    table_schema name;
    table_name name;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    PERFORM sql_saga.__internal_serialize(table_oid);

    SELECT n.nspname, c.relname
    INTO table_schema, table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = table_oid;

    DELETE FROM sql_saga.system_versioning AS sv
    WHERE (sv.table_schema, sv.table_name) = (table_schema, table_name)
    RETURNING * INTO system_versioning_row;

    IF NOT FOUND THEN
        RAISE DEBUG 'table % does not have SYSTEM VERSIONING', table_oid;
        RETURN false;
    END IF;

    is_dropped := NOT EXISTS (SELECT FROM pg_catalog.pg_class AS c WHERE c.oid = table_oid);

    IF NOT is_dropped THEN
        EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_as_of::regprocedure /* %s */, drop_behavior /* %s */);
        EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_between::regprocedure /* %s */, drop_behavior /* %s */);
        EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_between_symmetric::regprocedure /* %s */, drop_behavior /* %s */);
        EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_from_to::regprocedure /* %s */, drop_behavior /* %s */);
        EXECUTE format('DROP VIEW %I.%I %s', system_versioning_row.view_schema_name /* %I */, system_versioning_row.view_table_name /* %I */, drop_behavior /* %s */);
    END IF;

    IF NOT is_dropped AND cleanup THEN
        PERFORM sql_saga.drop_system_time_era(table_oid, drop_behavior, cleanup);
        EXECUTE format('DROP TABLE %I.%I %s', system_versioning_row.history_schema_name /* %I */, system_versioning_row.history_table_name /* %I */, drop_behavior /* %s */);
    END IF;

    RETURN true;
END;
$function$;
