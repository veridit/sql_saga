CREATE FUNCTION sql_saga.for_portion_of_trigger()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
#variable_conflict use_variable
DECLARE
    info record;
    jnew jsonb;
    identifier_columns text[];
    source_table_name text;
BEGIN
    -- This view only supports UPDATE operations
    IF TG_OP <> 'UPDATE' THEN
        RAISE EXCEPTION 'sql_saga: The "for_portion_of" view only supports UPDATE operations. For INSERTs or DELETEs, please use the base table directly.';
    END IF;

    -- Get metadata about the view's underlying table.
    SELECT v.table_schema, v.table_name, v.era_name,
           e.valid_from_column_name, e.valid_until_column_name, e.synchronize_valid_to_column,
           e.range_subtype_category
    INTO info
    FROM sql_saga.updatable_view AS v
    JOIN sql_saga.era AS e USING (table_schema, table_name, era_name)
    WHERE v.view_schema = TG_TABLE_SCHEMA AND v.view_name = TG_TABLE_NAME
      AND v.view_type = 'for_portion_of';

    IF NOT FOUND THEN
         RAISE EXCEPTION 'sql_saga: could not find metadata for view %.%', quote_ident(TG_TABLE_SCHEMA), quote_ident(TG_TABLE_NAME);
    END IF;

    jnew := to_jsonb(NEW);

    -- The conceptual entity identifier should not include the temporal columns.
    -- We filter them out here to correct for cases where a PRIMARY KEY that
    -- includes a temporal column was introspected by add_for_portion_of_view.
    identifier_columns := array_remove(array_remove(TG_ARGV::text[], info.valid_from_column_name), info.valid_until_column_name);

    -- Create a temporary table to hold the single source row. The table name
    -- is based on the view's OID to be unique per view. It is created only if it
    -- doesn't exist for the session to avoid noisy NOTICEs from CREATE IF NOT EXISTS,
    -- and is truncated before use to ensure it's clean for each row trigger.
    source_table_name := format('__temp_for_portion_of_%s', TG_RELID);

    IF to_regclass('pg_temp.' || quote_ident(source_table_name)) IS NULL THEN
        DECLARE
            v_cols_def text;
        BEGIN
            -- Build column definitions from the base table, but strip any GENERATED
            -- or IDENTITY properties. Using LIKE ... INCLUDING ALL would copy them,
            -- which would cause the subsequent INSERT of the NEW record to fail.
            -- This approach ensures the temp table has plain columns of the correct types.
            SELECT string_agg(
                format('%I %s', pa.attname, format_type(pa.atttypid, pa.atttypmod)),
                ', '
            )
            INTO v_cols_def
            FROM pg_attribute pa
            WHERE pa.attrelid = (info.table_schema || '.' || info.table_name)::regclass
              AND pa.attnum > 0 AND NOT pa.attisdropped;

            EXECUTE format('CREATE TEMP TABLE %I (row_id BIGINT, %s) ON COMMIT DROP',
                source_table_name, v_cols_def);
        END;
    END IF;

    EXECUTE format('DELETE FROM %I', source_table_name);

    -- Insert the NEW record, which contains the entity identifier, the data payload, and the time portion parameters.
    -- Since this trigger operates row-by-row, a static row_id of 1 is sufficient.
    -- If the user specified valid_to instead of valid_until, we must derive valid_until
    -- before inserting the record into our temporary source table.
    IF (jnew->info.valid_until_column_name IS NULL OR jnew->info.valid_until_column_name = 'null'::jsonb) AND jnew->info.synchronize_valid_to_column IS NOT NULL AND jnew->info.synchronize_valid_to_column <> 'null'::jsonb THEN
        DECLARE
            v_new_until jsonb;
            v_rec record;
        BEGIN
            SELECT
                CASE info.range_subtype_category
                    WHEN 'N' THEN -- Numeric types (integer, bigint, numeric)
                        to_jsonb( (jnew->>info.synchronize_valid_to_column)::numeric + 1 )
                    WHEN 'D' THEN -- Date/time types
                        to_jsonb( (jnew->>info.synchronize_valid_to_column)::date + 1 )
                    ELSE
                        NULL
                END INTO v_new_until;

            IF v_new_until IS NULL THEN
                RAISE EXCEPTION 'sql_saga: do not know how to derive "%" from "%" for range subtype category ''%''',
                    info.valid_until_column_name, info.synchronize_valid_to_column, info.range_subtype_category;
            END IF;

            jnew := jnew || jsonb_build_object(info.valid_until_column_name, v_new_until);
            v_rec := jsonb_populate_record(null::record, jnew);

            EXECUTE format('INSERT INTO %I SELECT 1, ($1).*', source_table_name)
            USING v_rec;
        END;
    ELSE
        -- The simple path: valid_until was provided directly.
        EXECUTE format('INSERT INTO %I SELECT 1, ($1).*', source_table_name)
        USING NEW;
    END IF;

    -- Use temporal_merge to apply the change.
    CALL sql_saga.temporal_merge(
        target_table => (info.table_schema || '.' || info.table_name)::regclass,
        source_table => source_table_name::regclass,
        identity_columns => identifier_columns,
        mode => 'UPDATE_FOR_PORTION_OF',
        era_name => info.era_name
    );

    RETURN NEW;
END;
$function$;
