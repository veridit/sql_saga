CREATE FUNCTION sql_saga.set_era_ephemeral_columns(
    table_class regclass,
    era_name name DEFAULT 'valid',
    ephemeral_columns name[] DEFAULT NULL
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER AS $set_era_ephemeral_columns$
#variable_conflict use_variable
DECLARE
    ephemeral_column_name name;
    table_schema name;
    table_name name;
BEGIN
    SELECT n.nspname, c.relname
    INTO table_schema, table_name
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = table_class;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_class);

    /* Make sure all the ephemeral columns exist */
    IF ephemeral_columns IS NOT NULL THEN
        FOR ephemeral_column_name IN
            SELECT u.name
            FROM unnest(ephemeral_columns) AS u (name)
            WHERE NOT EXISTS (
                SELECT FROM pg_catalog.pg_attribute AS a
                WHERE (a.attrelid, a.attname) = (table_class, u.name)
                  AND NOT a.attisdropped)
        LOOP
            RAISE EXCEPTION 'column "%" does not exist in table %', ephemeral_column_name, table_class;
        END LOOP;

        /* Don't allow system columns to be ephemeral */
        FOR ephemeral_column_name IN
            SELECT u.name
            FROM unnest(ephemeral_columns) AS u (name)
            JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (table_class, u.name)
            WHERE a.attnum < 0
        LOOP
            RAISE EXCEPTION 'cannot mark system column "%" as ephemeral', ephemeral_column_name;
        END LOOP;
    END IF;

    UPDATE sql_saga.era AS e
    SET ephemeral_columns = set_era_ephemeral_columns.ephemeral_columns
    WHERE (e.table_schema, e.table_name) = (table_schema, table_name)
      AND e.era_name = set_era_ephemeral_columns.era_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Era "%" not found on table %', era_name, table_class;
    END IF;
END;
$set_era_ephemeral_columns$;

COMMENT ON FUNCTION sql_saga.set_era_ephemeral_columns IS
'Sets the list of ephemeral columns for an era. Ephemeral columns are excluded from coalescing
comparison during temporal_merge operations. These are typically audit columns (e.g., edit_at,
edit_by_user_id) whose changes should not prevent adjacent periods from being merged.

Parameters:
- table_class: The table OID or name
- era_name: The era name (defaults to ''valid'')
- ephemeral_columns: Array of column names to exclude from coalescing, or NULL to clear';
