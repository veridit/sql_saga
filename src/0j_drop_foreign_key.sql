CREATE FUNCTION sql_saga.drop_foreign_key(
    table_oid regclass,
    column_names name[],
    era_name name DEFAULT NULL,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT',
    drop_index boolean DEFAULT true
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = sql_saga, pg_catalog, public
AS
$function$
#variable_conflict use_variable
DECLARE
    key_name_found name;
    fk_schema_name name;
    fk_table_name name;
    v_era_name name := era_name;
BEGIN
    SELECT n.nspname, c.relname
    INTO fk_schema_name, fk_table_name
    FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = table_oid;

    -- If era_name is not provided, try to auto-detect it for temporal tables.
    IF v_era_name IS NULL AND EXISTS (
        SELECT 1 FROM sql_saga.era e
        WHERE (e.table_schema, e.table_name) = (fk_schema_name, fk_table_name)
    ) THEN
        -- If there's more than one era, the user must specify which one to use.
        IF (SELECT count(*) FROM sql_saga.era e WHERE (e.table_schema, e.table_name) = (fk_schema_name, fk_table_name)) > 1 THEN
            RAISE EXCEPTION 'Table %.% has multiple eras. Please specify the era_name parameter to drop the foreign key.',
                quote_ident(fk_schema_name), quote_ident(fk_table_name);
        END IF;

        -- Auto-detect the single era name.
        SELECT e.era_name INTO v_era_name
        FROM sql_saga.era e
        WHERE (e.table_schema, e.table_name) = (fk_schema_name, fk_table_name);
    END IF;

    SELECT fk.foreign_key_name INTO key_name_found
    FROM sql_saga.foreign_keys AS fk
    WHERE (fk.table_schema, fk.table_name) = (fk_schema_name, fk_table_name)
      AND fk.column_names = column_names
      AND fk.fk_era_name IS NOT DISTINCT FROM v_era_name;

    IF NOT FOUND THEN
        IF v_era_name IS NOT NULL THEN
            RAISE EXCEPTION 'foreign key on table % for columns % with era % does not exist', table_oid, column_names, v_era_name;
        ELSE
            RAISE EXCEPTION 'foreign key on table % for columns % does not exist', table_oid, column_names;
        END IF;
    END IF;

    PERFORM sql_saga.drop_foreign_key_by_name(table_oid, key_name_found, drop_index);
END;
$function$;

