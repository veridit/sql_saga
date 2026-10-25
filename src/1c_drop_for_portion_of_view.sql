CREATE FUNCTION sql_saga.__api_drop_for_portion_of_view(table_oid regclass, era_name name DEFAULT 'valid', drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT false)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, pg_temp
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
    /* Without a table this drops the views of every table: each of them must be the caller's. */
    IF table_oid IS NOT NULL THEN
        PERFORM sql_saga.__internal_require_owner(table_oid, 'drop_for_portion_of_view');
    ELSE
        PERFORM sql_saga.__internal_require_owner(pg_catalog.to_regclass(pg_catalog.format('%I.%I', v.table_schema, v.table_name)), 'drop_for_portion_of_view')
        FROM sql_saga.updatable_view AS v
        WHERE v.view_type = 'for_portion_of' AND pg_catalog.to_regclass(pg_catalog.format('%I.%I', v.table_schema, v.table_name)) IS NOT NULL;
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    IF table_oid IS NOT NULL THEN
        SELECT n.nspname, c.relname
        INTO target_schema_name, target_table_name
        FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.oid = table_oid;
    END IF;

    FOR view_schema, view_name, trigger_name IN
        DELETE FROM sql_saga.updatable_view AS v
        WHERE (table_oid IS NULL OR (v.table_schema, v.table_name) = (target_schema_name, target_table_name))
          AND (era_name IS NULL OR v.era_name = era_name)
          AND v.view_type = 'for_portion_of'
        RETURNING v.view_schema, v.view_name, v.trigger_name
    LOOP
        IF to_regclass(format('%I.%I', view_schema, view_name)) IS NOT NULL THEN
            IF EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = to_regclass(format('%I.%I', view_schema, view_name)) AND t.tgname = trigger_name) THEN
                EXECUTE format('DROP TRIGGER %I ON %I.%I', trigger_name, view_schema, view_name);
            END IF;
            EXECUTE format('DROP VIEW IF EXISTS %I.%I %s', view_schema, view_name, drop_behavior);
        END IF;
    END LOOP;

    RETURN true;
END;
$function$;

COMMENT ON FUNCTION sql_saga.__api_drop_for_portion_of_view(regclass, name, sql_saga.drop_behavior, boolean) IS
'Drops the "for_portion_of" view associated with a table and era.';
