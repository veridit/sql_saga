CREATE FUNCTION sql_saga.set_system_time_era_excluded_columns(
    table_oid regclass,
    excluded_column_names name[])
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    excluded_column_name name;
    table_schema name;
    table_name name;
BEGIN
    SELECT n.nspname, c.relname
    INTO table_schema, table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = table_oid;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    /* Make sure all the excluded columns exist */
    FOR excluded_column_name IN
        SELECT u.name
        FROM unnest(excluded_column_names) AS u (name)
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_attribute AS a
            WHERE (a.attrelid, a.attname) = (table_oid, u.name))
    LOOP
        RAISE EXCEPTION 'column "%" does not exist in table %', excluded_column_name, table_oid;
    END LOOP;

    /* Don't allow system columns to be excluded either */
    FOR excluded_column_name IN
        SELECT u.name
        FROM unnest(excluded_column_names) AS u (name)
        JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (table_oid, u.name)
        WHERE a.attnum < 0
    LOOP
        RAISE EXCEPTION 'cannot exclude system column "%"', excluded_column_name;
    END LOOP;

    /* Do it. */
    UPDATE sql_saga.system_time_era AS ste SET
        excluded_column_names = $2
    WHERE (ste.table_schema, ste.table_name) = (table_schema, table_name);
END;
$function$;

COMMENT ON FUNCTION sql_saga.set_system_time_era_excluded_columns(regclass, name[]) IS
'Sets the list of columns to be excluded from system versioning. Changes to these columns will not create a new history record.';
