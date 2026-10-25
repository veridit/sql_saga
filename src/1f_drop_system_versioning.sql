CREATE FUNCTION sql_saga.__api_drop_system_versioning(table_oid regclass, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT true)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, pg_temp
AS $function$
#variable_conflict use_variable
DECLARE
    system_versioning_row sql_saga.system_versioning;
    is_dropped boolean;
    table_schema name;
    table_name name;
    base_oid oid;
    public_view_oid oid;
    gated_view_oid oid;
    as_of_attributable boolean;
    between_attributable boolean;
    between_symmetric_attributable boolean;
    from_to_attributable boolean;
    public_view_attributable boolean;
    gated_view_attributable boolean;
    function_depends_on_public_view boolean;
    function_display text;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;
    PERFORM sql_saga.__internal_require_owner(table_oid, 'drop_system_versioning');

    PERFORM sql_saga.__internal_serialize(table_oid);

    SELECT n.nspname, c.relname
    INTO table_schema, table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = table_oid;

    SELECT sv.* INTO system_versioning_row
    FROM sql_saga.system_versioning AS sv
    WHERE (sv.table_schema, sv.table_name) = (table_schema, table_name)
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE DEBUG 'table % does not have SYSTEM VERSIONING', table_oid;
        RETURN false;
    END IF;

    base_oid := table_oid::oid;
    public_view_oid := to_regclass(format('%I.%I', system_versioning_row.view_schema_name, system_versioning_row.view_table_name));
    gated_view_oid := to_regclass(format('%I.%I', system_versioning_row.history_schema_name,
        sql_saga.__internal_history_gated_name(system_versioning_row.history_table_name)));
    SELECT a.as_of_attributable, a.between_attributable,
           a.between_symmetric_attributable, a.from_to_attributable,
           a.public_view_attributable, a.gated_view_attributable
    INTO as_of_attributable, between_attributable,
         between_symmetric_attributable, from_to_attributable,
         public_view_attributable, gated_view_attributable
    FROM sql_saga.__internal_system_versioning_member_attribution(base_oid) AS a;

    DELETE FROM sql_saga.system_versioning AS sv
    WHERE (sv.table_schema, sv.table_name) = (table_schema, table_name);

    is_dropped := NOT EXISTS (SELECT FROM pg_catalog.pg_class AS c WHERE c.oid = table_oid);

    IF NOT is_dropped THEN
        SELECT EXISTS (
            SELECT 1 FROM pg_catalog.pg_proc AS p
            JOIN pg_catalog.pg_type AS t ON t.oid = p.prorettype
            WHERE p.oid = to_regprocedure(system_versioning_row.func_as_of)
              AND t.typrelid = public_view_oid)
        INTO function_depends_on_public_view;
        IF NOT as_of_attributable AND to_regprocedure(system_versioning_row.func_as_of) IS NOT NULL
           AND NOT function_depends_on_public_view THEN
            RAISE NOTICE 'sql_saga: Left function "%" in place because it is not the sql_saga-generated function for table "%"',
                system_versioning_row.func_as_of, sql_saga.__internal_display_name(table_oid);
        ELSIF NOT between_attributable AND to_regprocedure(system_versioning_row.func_between) IS NOT NULL THEN
            RAISE NOTICE 'sql_saga: Left function "%" in place because it is not the sql_saga-generated function for table "%"',
                system_versioning_row.func_between, sql_saga.__internal_display_name(table_oid);
        ELSIF NOT between_symmetric_attributable AND to_regprocedure(system_versioning_row.func_between_symmetric) IS NOT NULL THEN
            RAISE NOTICE 'sql_saga: Left function "%" in place because it is not the sql_saga-generated function for table "%"',
                system_versioning_row.func_between_symmetric, sql_saga.__internal_display_name(table_oid);
        ELSIF NOT from_to_attributable AND to_regprocedure(system_versioning_row.func_from_to) IS NOT NULL THEN
            RAISE NOTICE 'sql_saga: Left function "%" in place because it is not the sql_saga-generated function for table "%"',
                system_versioning_row.func_from_to, sql_saga.__internal_display_name(table_oid);
        ELSIF NOT public_view_attributable AND public_view_oid IS NOT NULL THEN
            RAISE NOTICE 'sql_saga: Left view "%" in place because it is not the sql_saga-generated view for table "%"',
                public_view_oid::regclass, sql_saga.__internal_display_name(table_oid);
        END IF;
        IF as_of_attributable THEN function_display := sql_saga.__internal_notice_function_name(to_regprocedure(system_versioning_row.func_as_of)); EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_as_of, drop_behavior); RAISE NOTICE 'sql_saga: Dropped function "%" of system-versioned table "%"', function_display, sql_saga.__internal_notice_name(table_schema, table_name); END IF;
        IF between_attributable THEN function_display := sql_saga.__internal_notice_function_name(to_regprocedure(system_versioning_row.func_between)); EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_between, drop_behavior); RAISE NOTICE 'sql_saga: Dropped function "%" of system-versioned table "%"', function_display, sql_saga.__internal_notice_name(table_schema, table_name); END IF;
        IF between_symmetric_attributable THEN function_display := sql_saga.__internal_notice_function_name(to_regprocedure(system_versioning_row.func_between_symmetric)); EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_between_symmetric, drop_behavior); RAISE NOTICE 'sql_saga: Dropped function "%" of system-versioned table "%"', function_display, sql_saga.__internal_notice_name(table_schema, table_name); END IF;
        IF from_to_attributable THEN function_display := sql_saga.__internal_notice_function_name(to_regprocedure(system_versioning_row.func_from_to)); EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_from_to, drop_behavior); RAISE NOTICE 'sql_saga: Dropped function "%" of system-versioned table "%"', function_display, sql_saga.__internal_notice_name(table_schema, table_name); END IF;
        IF public_view_attributable THEN
            EXECUTE format('DROP VIEW %I.%I %s', system_versioning_row.view_schema_name, system_versioning_row.view_table_name, drop_behavior);
            RAISE NOTICE 'sql_saga: Dropped view "%" of system-versioned table "%"', sql_saga.__internal_notice_name(system_versioning_row.view_schema_name, system_versioning_row.view_table_name), sql_saga.__internal_notice_name(table_schema, table_name);
        END IF;
        IF gated_view_attributable THEN
            EXECUTE format('DROP VIEW %I.%I %s', system_versioning_row.history_schema_name,
                sql_saga.__internal_history_gated_name(system_versioning_row.history_table_name), drop_behavior);
            RAISE NOTICE 'sql_saga: Dropped view "%" of system-versioned table "%"', sql_saga.__internal_notice_name(system_versioning_row.history_schema_name, sql_saga.__internal_history_gated_name(system_versioning_row.history_table_name)), sql_saga.__internal_notice_name(table_schema, table_name);
        END IF;
    END IF;

    IF NOT is_dropped AND cleanup THEN
        PERFORM sql_saga.drop_system_time_era(table_oid, drop_behavior, cleanup);
        IF to_regclass(format('%I.%I', system_versioning_row.history_schema_name, system_versioning_row.history_table_name)) IS NOT NULL THEN
            EXECUTE format('DROP TABLE %I.%I %s', system_versioning_row.history_schema_name, system_versioning_row.history_table_name, drop_behavior);
            RAISE NOTICE 'sql_saga: Dropped history table "%" of system-versioned table "%"', sql_saga.__internal_notice_name(system_versioning_row.history_schema_name, system_versioning_row.history_table_name), sql_saga.__internal_notice_name(table_schema, table_name);
        END IF;
    END IF;

    RETURN true;
END;
$function$;

COMMENT ON FUNCTION sql_saga.__api_drop_system_versioning(regclass, sql_saga.drop_behavior, boolean) IS
'Removes system versioning from a table, dropping the history table and all associated objects.';
