CREATE FUNCTION sql_saga.for_portion_of_trigger()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
#variable_conflict use_variable
DECLARE
    info record;
    jnew jsonb;
    identifier_columns text[];
    temp_source_table_name text;
BEGIN
    -- This view only supports UPDATE operations
    IF TG_OP <> 'UPDATE' THEN
        RAISE EXCEPTION 'sql_saga: The "for_portion_of" view only supports UPDATE operations. For INSERTs or DELETEs, please use the base table directly.';
    END IF;

    -- Get metadata about the view's underlying table.
    SELECT v.table_schema, v.table_name, v.era_name,
           e.valid_from_column_name, e.valid_until_column_name, e.synchronize_valid_to_column,
           e.range_subtype_category, e.range_subtype
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
    temp_source_table_name := format('__temp_for_portion_of_%s', TG_RELID);

    IF to_regclass('pg_temp.' || quote_ident(temp_source_table_name)) IS NULL THEN
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

            EXECUTE format('CREATE TEMP TABLE %I (row_id BIGINT, %s, merge_status jsonb) ON COMMIT DROP',
                temp_source_table_name, v_cols_def);
        END;
    END IF;

    
    -- Avoid banning by pg-safeupdate used by PostgREST
    -- Ref. https://docs.postgrest.org/en/v12/integrations/pg-safeupdate.html
    -- by specifying a WHERE clause.
    EXECUTE format('DELETE FROM %I WHERE true;', temp_source_table_name);

    -- The `for_portion_of` view allows specifying a time slice using either `valid_until` (exclusive)
    -- or `valid_to` (inclusive). To provide a clean, unambiguous source row to the underlying
    -- temporal_merge procedure, the trigger must derive valid_until and pass only that column.
    IF info.synchronize_valid_to_column IS NOT NULL THEN
        DECLARE
             v_valid_to_changed BOOLEAN;
             v_valid_until_changed BOOLEAN;
        BEGIN
            EXECUTE format('SELECT ($1).%I IS DISTINCT FROM ($2).%I', info.synchronize_valid_to_column, info.synchronize_valid_to_column) INTO v_valid_to_changed USING NEW, OLD;
            EXECUTE format('SELECT ($1).%I IS DISTINCT FROM ($2).%I', info.valid_until_column_name, info.valid_until_column_name) INTO v_valid_until_changed USING NEW, OLD;

            -- If valid_to was changed but valid_until was not, then valid_until is stale and must be derived.
            IF v_valid_to_changed AND NOT v_valid_until_changed THEN
                DECLARE
                    v_interval text;
                    v_derived_valid_until jsonb;
                BEGIN
                    CASE info.range_subtype_category
                        WHEN 'D' THEN v_interval := '''1 day''::interval';
                        WHEN 'N' THEN v_interval := '1';
                        ELSE RAISE EXCEPTION 'Unsupported range subtype category for valid_to -> valid_until conversion: %', info.range_subtype_category;
                    END CASE;

                    EXECUTE format('SELECT to_jsonb(($1->>%L)::%s + %s)',
                        info.synchronize_valid_to_column, info.range_subtype, v_interval)
                    INTO v_derived_valid_until
                    USING jnew;

                    jnew := jnew || jsonb_build_object(info.valid_until_column_name, v_derived_valid_until);
                END;
            END IF;
        END;
        -- Always remove valid_to before passing to temporal_merge.
        jnew := jnew - info.synchronize_valid_to_column;
    END IF;

    -- We no longer need the complex IF/ELSE for insertion.
    -- Populate a record from the cleaned-up JSON and insert it.
    BEGIN
        EXECUTE format(
            'INSERT INTO %I SELECT 1, (r).*, NULL FROM jsonb_populate_record(null::%I.%I, $1) AS r',
            temp_source_table_name,
            info.table_schema,
            info.table_name
        )
        USING jnew;
    END;

    -- Use temporal_merge to apply the change.
    CALL sql_saga.temporal_merge(
        target_table => (info.table_schema || '.' || info.table_name)::regclass,
        source_table => temp_source_table_name::regclass,
        primary_identity_columns => identifier_columns,
        mode => 'UPDATE_FOR_PORTION_OF',
        era_name => info.era_name,
        update_source_with_feedback => true,
        feedback_status_column => 'merge_status',
        feedback_status_key => 'temporal_merge'
    );

    -- Check for errors from the merge operation and raise an exception if any occurred.
    DECLARE
        v_error_count integer;
        v_error_message text;
    BEGIN
        EXECUTE format('SELECT count(*), max(merge_status->''temporal_merge''->>''error_message'') FROM %I WHERE (merge_status->''temporal_merge''->>''status'')::sql_saga.temporal_merge_feedback_status = ''ERROR''', temp_source_table_name)
        INTO v_error_count, v_error_message;

        IF v_error_count > 0 THEN
            RAISE EXCEPTION 'sql_saga: applying the change for portion of time failed. First error: %', v_error_message;
        END IF;
    END;

    RETURN NEW;
END;
$function$;
