CREATE FUNCTION sql_saga.drop_unique_key(
        table_oid regclass,
        column_names name[],
        era_name name DEFAULT 'valid',
        drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT',
        cleanup boolean DEFAULT true
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    table_schema name;
    table_name name;
    key_name_found name;
BEGIN
    SELECT n.nspname, c.relname
    INTO table_schema, table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = table_oid;

    SELECT uk.unique_key_name INTO key_name_found
    FROM sql_saga.unique_keys AS uk
    WHERE (uk.table_schema, uk.table_name) = (table_schema, table_name)
      AND uk.column_names = column_names
      AND uk.era_name = era_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'unique key on table % for columns % with era % does not exist', table_oid, column_names, era_name;
    END IF;

    RAISE DEBUG 'drop_unique_key: Found key name "%" to drop. Forwarding to drop_unique_key_by_name.', key_name_found;
    PERFORM sql_saga.drop_unique_key_by_name(table_oid, key_name_found, drop_behavior, cleanup);
END;
$function$;

COMMENT ON FUNCTION sql_saga.drop_unique_key(regclass, name[], name, sql_saga.drop_behavior, boolean) IS
'Drops a temporal unique key identified by its table, columns, and era.';
