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
           e.range_column_name, e.valid_from_column_name, e.valid_until_column_name, e.valid_to_column_name,
           e.range_subtype_category, e.range_subtype, e.range_type
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
    identifier_columns := TG_ARGV::text[];
    IF info.range_column_name IS NOT NULL THEN identifier_columns := array_remove(identifier_columns, info.range_column_name); END IF;
    IF info.valid_from_column_name IS NOT NULL THEN identifier_columns := array_remove(identifier_columns, info.valid_from_column_name); END IF;
    IF info.valid_until_column_name IS NOT NULL THEN identifier_columns := array_remove(identifier_columns, info.valid_until_column_name); END IF;
    IF info.valid_to_column_name IS NOT NULL THEN identifier_columns := array_remove(identifier_columns, info.valid_to_column_name); END IF;

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

    -- The `for_portion_of` view allows specifying a time slice using either the authoritative range
    -- column, or the convenient component columns (`valid_from`, `valid_until`). This trigger must
    -- determine the definitive time slice for the operation and pass it to temporal_merge.
    DECLARE
        v_final_range_val jsonb;
        v_range_changed BOOLEAN := false;
        v_from_changed BOOLEAN := false;
        v_until_changed BOOLEAN := false;
    BEGIN
        IF info.range_column_name IS NOT NULL THEN
            EXECUTE format('SELECT ($1).%I IS DISTINCT FROM ($2).%I', info.range_column_name, info.range_column_name) INTO v_range_changed USING NEW, OLD;
        END IF;
        IF info.valid_from_column_name IS NOT NULL THEN
            EXECUTE format('SELECT ($1).%I IS DISTINCT FROM ($2).%I', info.valid_from_column_name, info.valid_from_column_name) INTO v_from_changed USING NEW, OLD;
        END IF;
        IF info.valid_until_column_name IS NOT NULL THEN
            EXECUTE format('SELECT ($1).%I IS DISTINCT FROM ($2).%I', info.valid_until_column_name, info.valid_until_column_name) INTO v_until_changed USING NEW, OLD;
        END IF;

        -- If any temporal column was changed, derive the new range for the operation.
        IF v_range_changed OR v_from_changed OR v_until_changed THEN
            -- If the user set the range column directly, it takes precedence.
            IF v_range_changed THEN
                v_final_range_val := jnew->info.range_column_name;
            -- Otherwise, derive the range from the component from/until columns.
            ELSE
                EXECUTE format('SELECT to_jsonb( %I(($1->>%L)::%s, ($1->>%L)::%s, ''[)'') )',
                    info.range_type,
                    info.valid_from_column_name, info.range_subtype,
                    info.valid_until_column_name, info.range_subtype
                ) INTO v_final_range_val USING jnew;
            END IF;
        ELSE
            -- No temporal columns were changed, so this is a data-only update. The portion
            -- to update is the original time slice from the OLD record.
            EXECUTE format('SELECT to_jsonb(($1).%I)', info.range_column_name) INTO v_final_range_val USING OLD;
        END IF;

        IF v_final_range_val IS NULL OR v_final_range_val = 'null'::jsonb THEN
            RAISE EXCEPTION 'sql_saga: could not determine the time portion for the UPDATE. Please SET either the range column (%) or both component columns (% and %).',
                quote_ident(info.range_column_name), quote_ident(info.valid_from_column_name), quote_ident(info.valid_until_column_name);
        END IF;

        jnew := jsonb_set(jnew, ARRAY[info.range_column_name], v_final_range_val);
    END;

    -- Clean up: always remove the component columns before passing to temporal_merge.
    -- The range column is the single source of truth for the period, and the sync
    -- trigger on the base table will re-populate them correctly upon write.
    IF info.valid_from_column_name IS NOT NULL THEN jnew := jnew - info.valid_from_column_name; END IF;
    IF info.valid_until_column_name IS NOT NULL THEN jnew := jnew - info.valid_until_column_name; END IF;
    IF info.valid_to_column_name IS NOT NULL THEN jnew := jnew - info.valid_to_column_name; END IF;

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
