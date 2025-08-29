CREATE FUNCTION sql_saga.drop_foreign_key(
    table_oid regclass,
    column_names name[],
    era_name name DEFAULT NULL,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT'
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    key_name_found name;
    fk_schema_name name;
    fk_table_name name;
BEGIN
    SELECT n.nspname, c.relname
    INTO fk_schema_name, fk_table_name
    FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = table_oid;

    SELECT fk.foreign_key_name INTO key_name_found
    FROM sql_saga.foreign_keys AS fk
    WHERE (fk.table_schema, fk.table_name) = (fk_schema_name, fk_table_name)
      AND fk.column_names = column_names
      AND fk.fk_era_name IS NOT DISTINCT FROM era_name;

    IF NOT FOUND THEN
        IF era_name IS NOT NULL THEN
            RAISE EXCEPTION 'foreign key on table % for columns % with era % does not exist', table_oid, column_names, era_name;
        ELSE
            RAISE EXCEPTION 'foreign key on table % for columns % does not exist', table_oid, column_names;
        END IF;
    END IF;

    PERFORM sql_saga.drop_foreign_key_by_name(table_oid, key_name_found);
END;
$function$;
