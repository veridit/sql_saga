CREATE FUNCTION sql_saga.__api_drop_foreign_key(
    table_oid regclass,
    column_names name[],
    era_name name DEFAULT NULL,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT',
    drop_index boolean DEFAULT true
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = sql_saga, pg_catalog, pg_temp
AS
$function$
#variable_conflict use_variable
DECLARE
    key_name_found name;
    keys_found name[];
    fk_schema_name name;
    fk_table_name name;
    v_era_name name := era_name;
BEGIN
    PERFORM sql_saga.__internal_entry_guard();
    PERFORM sql_saga.__internal_require_owner(table_oid, 'drop_foreign_key');
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

    -- The catalog lists the child columns in the referenced key's order, which may differ from the order the
    -- caller gave add_foreign_key; match them as a set. More than one match is ambiguous.
    SELECT pg_catalog.array_agg(fk.foreign_key_name ORDER BY fk.foreign_key_name COLLATE "C")
    INTO keys_found
    FROM sql_saga.foreign_keys AS fk
    WHERE (fk.table_schema, fk.table_name) = (fk_schema_name, fk_table_name)
      AND pg_catalog.cardinality(fk.column_names) = pg_catalog.cardinality(column_names)
      AND ARRAY(SELECT c FROM pg_catalog.unnest(fk.column_names) AS c ORDER BY c COLLATE "C")
        = ARRAY(SELECT c FROM pg_catalog.unnest(column_names) AS c ORDER BY c COLLATE "C")
      AND fk.fk_era_name IS NOT DISTINCT FROM v_era_name;

    IF pg_catalog.cardinality(keys_found) > 1 THEN
        RAISE EXCEPTION 'foreign key on table % for columns % is ambiguous: it matches foreign keys %; use sql_saga.drop_foreign_key_by_name',
            table_oid, column_names, pg_catalog.array_to_string(keys_found, ', ');
    END IF;
    key_name_found := keys_found[1];

    IF key_name_found IS NULL THEN
        IF v_era_name IS NOT NULL THEN
            RAISE EXCEPTION 'foreign key on table % for columns % with era % does not exist', table_oid, column_names, v_era_name;
        ELSE
            RAISE EXCEPTION 'foreign key on table % for columns % does not exist', table_oid, column_names;
        END IF;
    END IF;

    PERFORM sql_saga.drop_foreign_key_by_name(table_oid, key_name_found, drop_index);
END;
$function$;

