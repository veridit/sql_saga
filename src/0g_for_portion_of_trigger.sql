CREATE FUNCTION sql_saga.for_portion_of_trigger()
 RETURNS trigger
 LANGUAGE plpgsql
AS $for_portion_of_trigger$
#variable_conflict use_variable
DECLARE
    info record;
    jnew jsonb;
    jold jsonb;
    identifier_columns text[];
    temp_source_table_name text;
    delete_portion boolean := false;
    merge_mode sql_saga.temporal_merge_mode := 'UPDATE_FOR_PORTION_OF';
BEGIN
    -- This view only supports UPDATE operations
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'sql_saga: DELETE is not supported on a "for_portion_of" view.'
            USING ERRCODE = 'feature_not_supported',
                  HINT = 'Use UPDATE with an identifier column set to NULL and the requested temporal bounds. Omit bounds to remove the complete matched history.';
    ELSIF TG_OP <> 'UPDATE' THEN
        RAISE EXCEPTION 'sql_saga: The "for_portion_of" view only supports UPDATE operations. For INSERTs or DELETEs, please use the base table directly.';
    END IF;

    -- Get metadata about the view's underlying table.
    -- Resolution order for ephemeral_columns: view-level override takes precedence over era-level default.
    -- table_oid quotes each part: schema and table names may need quoting ("Odd ""Schema"""."Ta'ble").
    SELECT v.table_schema, v.table_name, v.era_name, v.generated_columns, pg_catalog.format('%I.%I', v.table_schema, v.table_name)::regclass AS table_oid,
           COALESCE(v.ephemeral_columns, e.ephemeral_columns) AS ephemeral_columns,
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
    jold := to_jsonb(OLD);

    -- The conceptual entity identifier excludes temporal columns even when
    -- add_for_portion_of_view introspected a PRIMARY KEY containing them.
    identifier_columns := TG_ARGV::text[];
    IF info.range_column_name IS NOT NULL THEN identifier_columns := array_remove(identifier_columns, info.range_column_name); END IF;
    IF info.valid_from_column_name IS NOT NULL THEN identifier_columns := array_remove(identifier_columns, info.valid_from_column_name); END IF;
    IF info.valid_until_column_name IS NOT NULL THEN identifier_columns := array_remove(identifier_columns, info.valid_until_column_name); END IF;
    IF info.valid_to_column_name IS NOT NULL THEN identifier_columns := array_remove(identifier_columns, info.valid_to_column_name); END IF;

    -- A NULL assignment to an identifier requests removal, never a NULL-key
    -- update. Ignore identifiers already NULL in OLD, so a nullable key does
    -- not turn an unrelated payload correction into a deletion.
    SELECT EXISTS (
        SELECT FROM unnest(identifier_columns) AS ic(column_name)
        WHERE jnew -> ic.column_name = 'null'::jsonb
          AND jnew -> ic.column_name IS DISTINCT FROM jold -> ic.column_name
    ) INTO delete_portion;

    IF delete_portion THEN
        IF EXISTS (
            SELECT FROM jsonb_each(jnew) AS nc(column_name, value)
            WHERE nc.value IS DISTINCT FROM jold -> nc.column_name
              AND NOT (nc.column_name = ANY(identifier_columns) AND nc.value = 'null'::jsonb)
              AND nc.column_name IS DISTINCT FROM info.range_column_name
              AND nc.column_name IS DISTINCT FROM info.valid_from_column_name
              AND nc.column_name IS DISTINCT FROM info.valid_until_column_name
              AND nc.column_name IS DISTINCT FROM info.valid_to_column_name
        ) THEN
            RAISE EXCEPTION 'sql_saga: a portion deletion cannot also change non-temporal column values.'
                USING ERRCODE = 'invalid_parameter_value',
                      HINT = 'Set an identifier column to NULL and change only the requested temporal bounds.';
        END IF;

        -- A middle split can physically UPDATE and INSERT without DELETE.
        -- Require DELETE authority for the logical removal regardless of plan.
        -- All physical DML still runs as the invoker, with its normal RLS.
        IF NOT pg_catalog.has_table_privilege(info.table_oid, 'DELETE') THEN
            RAISE EXCEPTION 'permission denied for table %', info.table_name
                USING ERRCODE = 'insufficient_privilege';
        END IF;

        -- NULL is only the command signal. Every component of the identity
        -- supplied to temporal_merge must come from OLD, including composites.
        SELECT jnew || jsonb_object_agg(ic.column_name, jold -> ic.column_name)
        INTO jnew
        FROM unnest(identifier_columns) AS ic(column_name);
        merge_mode := 'DELETE_FOR_PORTION_OF';
    END IF;

    -- Strip GENERATED columns from jnew using direct array subtraction
    jnew := jnew - info.generated_columns;

    -- For UPDATE operations, we need to handle column filtering carefully to support two cases:
    -- 1. Columns not mentioned in UPDATE -> should preserve existing values across segments
    -- 2. Columns explicitly set (including to NULL) -> should apply the new value
    --
    -- Strategy: Build jnew to contain:
    --   - Identity columns (always from NEW)
    --   - Temporal columns (always from NEW)
    --   - Changed columns (from NEW, may be NULL if explicitly set to NULL)
    --   - Unchanged columns (from OLD, to preserve values across temporal segments)
    --
    -- With range intersection, each trigger sees only its own temporal segment in the target,
    -- so using OLD values for unchanged columns preserves the correct values per segment.
    IF TG_OP = 'UPDATE' THEN
        DECLARE
            filtered_jnew jsonb := '{}'::jsonb;
            key text;
            identity_cols text[] := TG_ARGV::text[];
        BEGIN
            -- Iterate through all keys in jnew
            FOR key IN SELECT jsonb_object_keys(jnew)
            LOOP
                IF key = ANY(identity_cols)
                   OR key = info.range_column_name
                   OR key = info.valid_from_column_name
                   OR key = info.valid_until_column_name
                   OR key = info.valid_to_column_name
                THEN
                    -- Always include identity and temporal columns with NEW value
                    filtered_jnew := filtered_jnew || jsonb_build_object(key, jnew->key);
                ELSIF (jnew->key) IS DISTINCT FROM (jold->key) THEN
                    -- Changed column: use NEW value (may be explicit NULL)
                    filtered_jnew := filtered_jnew || jsonb_build_object(key, jnew->key);
                ELSE
                    -- Unchanged column: use OLD value to preserve across segments
                    filtered_jnew := filtered_jnew || jsonb_build_object(key, jold->key);
                END IF;
            END LOOP;
            
            jnew := filtered_jnew;
        END;
    END IF;

    -- Writes are batched: this row trigger only prepares the row's source and appends it to a queue,
    -- pg_temp.__temp_for_portion_of_<view oid> (or ..._delete for removals). The view's statement-level
    -- trigger, sql_saga.for_portion_of_apply_trigger(), applies each queue with one temporal_merge.
    temp_source_table_name := format('__temp_for_portion_of_%s', TG_RELID);
    IF delete_portion THEN
        temp_source_table_name := temp_source_table_name || '_delete';
    END IF;

    -- A statement on this view issued while its queue is being applied (for example by a trigger on the
    -- base table) would add to, and apply, the queue being read. Refuse it.
    IF TG_RELID::text = ANY (pg_catalog.string_to_array(pg_catalog.current_setting('sql_saga.for_portion_of_applying', true), ',')) THEN
        RAISE EXCEPTION 'sql_saga: cannot update view %.% while a change through it is being applied',
            quote_ident(TG_TABLE_SCHEMA), quote_ident(TG_TABLE_NAME)
            USING ERRCODE = 'object_in_use',
                  HINT = 'Write to the base table directly from the trigger.';
    END IF;

    -- A queued row is applied only by the view's apply trigger. Without it (a view's triggers cannot be
    -- disabled, but one can be dropped despite drop protection), refuse rather than report a change that
    -- is never made. When this row trigger fires, the apply trigger fires too (same view, same role rules).
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_trigger AS t
        WHERE t.tgrelid = TG_RELID
          AND t.tgfoid = 'sql_saga.for_portion_of_apply_trigger()'::pg_catalog.regprocedure
          AND t.tgtype = 16) -- AFTER UPDATE FOR EACH STATEMENT
    THEN
        RAISE EXCEPTION 'sql_saga: view %.% has no apply trigger, so its changes would not be made',
            quote_ident(TG_TABLE_SCHEMA), quote_ident(TG_TABLE_NAME)
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Recreate the view with sql_saga.drop_for_portion_of_view and sql_saga.add_for_portion_of_view.';
    END IF;

    IF to_regclass('pg_temp.' || quote_ident(temp_source_table_name)) IS NULL THEN
        DECLARE
            v_cols_def text;
        BEGIN
            -- Build column definitions excluding the pre-computed generated_columns list
            -- This excludes GENERATED columns that would cause INSERT failures
            SELECT string_agg(
                format('%I %s', pa.attname, format_type(pa.atttypid, pa.atttypmod)),
                ', '
            )
            INTO v_cols_def
            FROM pg_attribute pa
            WHERE pa.attrelid = info.table_oid
              AND pa.attname <> ALL(info.generated_columns)
              AND pa.attnum > 0 AND NOT pa.attisdropped;

            EXECUTE format('CREATE TEMP TABLE %I (row_id bigserial, %s, merge_status jsonb) ON COMMIT DROP',
                temp_source_table_name, v_cols_def);
        END;
    END IF;

    -- The `for_portion_of` view allows specifying a time slice using either the authoritative range
    -- column, or the convenient component columns (`valid_from`, `valid_until`, `valid_to`). This trigger must
    -- determine the definitive time slice for the operation and pass it to temporal_merge.
    DECLARE
        v_final_range_val jsonb;
        v_range_changed BOOLEAN := false;
        v_from_changed BOOLEAN := false;
        v_until_changed BOOLEAN := false;
        v_to_changed BOOLEAN := false;
        v_until_via_to text;
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
        IF info.valid_to_column_name IS NOT NULL THEN
            EXECUTE format('SELECT ($1).%I IS DISTINCT FROM ($2).%I', info.valid_to_column_name, info.valid_to_column_name) INTO v_to_changed USING NEW, OLD;
        END IF;

        -- If any temporal column was changed, derive the new range for the operation.
        IF v_range_changed OR v_from_changed OR v_until_changed OR v_to_changed THEN
            -- The user specified a temporal range for this update. We must intersect this
            -- requested range with the OLD row's existing timeline to ensure each trigger
            -- firing only affects its own temporal segment.
            DECLARE
                v_requested_range_val jsonb;
                v_old_range_val jsonb;
            BEGIN
                -- First, get the OLD row's range
                EXECUTE format('SELECT to_jsonb(($1).%I)', info.range_column_name) 
                INTO v_old_range_val USING OLD;

                -- If the user set the range column directly, use it as the requested range
                IF v_range_changed THEN
                    v_requested_range_val := jnew->info.range_column_name;
                -- Otherwise, derive the requested range from the component from/until/to columns
                ELSE
                    -- If valid_to was changed, convert it to valid_until (add 1 to make it exclusive)
                    IF v_to_changed THEN
                        DECLARE
                            v_new_to text;
                        BEGIN
                            v_new_to := (jnew ->> info.valid_to_column_name);
                            IF v_new_to IS NOT NULL THEN
                                EXECUTE format('SELECT ($1::%s + 1)::text', info.range_subtype)
                                INTO v_until_via_to USING v_new_to;
                            END IF;
                        END;
                    END IF;

                    -- Build the requested range using from and until (preferring converted valid_to if available)
                    EXECUTE format('SELECT to_jsonb( %I(($1->>%L)::%s, COALESCE($2, ($1->>%L))::%s, ''[)'') )',
                        info.range_type,
                        info.valid_from_column_name, info.range_subtype,
                        info.valid_until_column_name, info.range_subtype
                    ) INTO v_requested_range_val USING jnew, v_until_via_to;
                END IF;

                -- Compute the intersection of the requested range with the OLD row's range
                -- This ensures each trigger firing only affects its own temporal segment
                -- Extract the text value from jsonb first (using ->> operator)
                EXECUTE format('SELECT to_jsonb( (($1->>0)::%I) * (($2->>0)::%I) )',
                    info.range_type, info.range_type
                ) INTO v_final_range_val USING v_requested_range_val, v_old_range_val;
                
                -- If the intersection is empty, this row is outside the requested timespan
                -- Return NULL to signal that this trigger firing should be skipped
                IF v_final_range_val::text = '"empty"' THEN
                    RETURN NULL;
                END IF;
            END;
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

    -- Insert the cleaned-up JSON into the temp table using explicit column list
    -- This avoids issues with GENERATED columns that aren't in the temp table
    BEGIN
        DECLARE
            v_insert_cols text;
            v_insert_vals text;
        BEGIN
            -- Build explicit column list and values from jnew
            SELECT 
                string_agg(quote_ident(key), ', '),
                string_agg(format('($1->>%L)::%s', key, format_type(pa.atttypid, pa.atttypmod)), ', ')
            INTO v_insert_cols, v_insert_vals
            FROM jsonb_object_keys(jnew) AS key
            JOIN pg_attribute pa ON pa.attrelid = info.table_oid
                                AND pa.attname = key
            WHERE pa.attnum > 0 AND NOT pa.attisdropped;

            EXECUTE format('INSERT INTO %I (%s) VALUES (%s)',
                temp_source_table_name, v_insert_cols, v_insert_vals)
            USING jnew;
        END;
    END;

    RETURN NEW;
END;
$for_portion_of_trigger$;

/*
 * The statement-level half of a for_portion_of view (1.6): AFTER UPDATE FOR EACH STATEMENT, created by
 * add_for_portion_of_view as the view's apply trigger (updatable_view.trigger_name || '_apply').
 *
 * The INSTEAD OF row trigger, sql_saga.for_portion_of_trigger(), only prepares each changed row's source
 * and queues it. This trigger applies the statement's queues with one temporal_merge each, removals first
 * (DELETE_FOR_PORTION_OF), then changes (UPDATE_FOR_PORTION_OF), and empties them. Each row's source covers
 * its OLD row's period intersected with the requested period, so the queued rows of one entity never
 * overlap, and the row trigger never saw an earlier row's effect: one merge gives the per-row result.
 *
 * It runs inside the statement as the caller (SECURITY INVOKER): an error aborts the statement, and the
 * caller's privileges and row-level security decide every physical change, as before. While a queue is
 * applied, sql_saga.for_portion_of_applying names the view, and the row trigger refuses a nested statement
 * on it (a trigger on the base table writing through the same view).
 */
CREATE FUNCTION sql_saga.for_portion_of_apply_trigger()
 RETURNS trigger
 LANGUAGE plpgsql
AS $for_portion_of_apply_trigger$
#variable_conflict use_variable
DECLARE
    info record;
    identifier_columns text[];
    queue record;
    queue_oid regclass;
    v_nonempty boolean;
    v_error_message text;
    v_applying text;
BEGIN
    IF TG_LEVEL <> 'STATEMENT' OR TG_WHEN <> 'AFTER' OR TG_OP <> 'UPDATE' THEN
        RAISE EXCEPTION 'sql_saga: for_portion_of_apply_trigger() must be an AFTER UPDATE FOR EACH STATEMENT trigger'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    FOR queue IN
        SELECT q.name, q.mode::sql_saga.temporal_merge_mode AS mode
        FROM (VALUES (1, '__temp_for_portion_of_' || TG_RELID || '_delete', 'DELETE_FOR_PORTION_OF'),
                     (2, '__temp_for_portion_of_' || TG_RELID, 'UPDATE_FOR_PORTION_OF')) AS q(ord, name, mode)
        ORDER BY q.ord
    LOOP
        queue_oid := pg_catalog.to_regclass(pg_catalog.format('pg_temp.%I', queue.name));
        CONTINUE WHEN queue_oid IS NULL;
        EXECUTE pg_catalog.format('SELECT EXISTS (SELECT FROM %s)', queue_oid) INTO v_nonempty;
        CONTINUE WHEN NOT v_nonempty;

        IF info IS NULL THEN
            SELECT v.era_name, v.trigger_name, pg_catalog.format('%I.%I', v.table_schema, v.table_name)::regclass AS table_oid,
                   COALESCE(v.ephemeral_columns, e.ephemeral_columns) AS ephemeral_columns,
                   e.range_column_name, e.valid_from_column_name, e.valid_until_column_name, e.valid_to_column_name
            INTO info
            FROM sql_saga.updatable_view AS v
            JOIN sql_saga.era AS e USING (table_schema, table_name, era_name)
            WHERE v.view_schema = TG_TABLE_SCHEMA AND v.view_name = TG_TABLE_NAME
              AND v.view_type = 'for_portion_of';
            IF NOT FOUND THEN
                RAISE EXCEPTION 'sql_saga: could not find metadata for view %.%', quote_ident(TG_TABLE_SCHEMA), quote_ident(TG_TABLE_NAME);
            END IF;

            -- The entity identifier: the row trigger's arguments without the temporal columns.
            SELECT pg_catalog.string_to_array(pg_catalog.rtrim(pg_catalog.encode(t.tgargs, 'escape'), E'\\000'), E'\\000')
            INTO identifier_columns
            FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (TG_RELID, info.trigger_name);
            IF identifier_columns IS NULL THEN
                RAISE EXCEPTION 'sql_saga: the row trigger "%" of view %.% is missing', info.trigger_name,
                    quote_ident(TG_TABLE_SCHEMA), quote_ident(TG_TABLE_NAME);
            END IF;
            identifier_columns := pg_catalog.array_remove(identifier_columns, info.range_column_name);
            identifier_columns := pg_catalog.array_remove(identifier_columns, info.valid_from_column_name);
            identifier_columns := pg_catalog.array_remove(identifier_columns, info.valid_until_column_name);
            identifier_columns := pg_catalog.array_remove(identifier_columns, info.valid_to_column_name);
        END IF;

        v_applying := pg_catalog.current_setting('sql_saga.for_portion_of_applying', true);
        PERFORM pg_catalog.set_config('sql_saga.for_portion_of_applying',
            pg_catalog.concat_ws(',', NULLIF(v_applying, ''), TG_RELID::text), true);

        CALL sql_saga.temporal_merge(
            target_table => info.table_oid,
            source_table => queue_oid,
            primary_identity_columns => identifier_columns,
            mode => queue.mode,
            era_name => info.era_name,
            update_source_with_feedback => true,
            feedback_status_column => 'merge_status',
            feedback_status_key => 'temporal_merge',
            ephemeral_columns => info.ephemeral_columns
        );

        PERFORM pg_catalog.set_config('sql_saga.for_portion_of_applying', COALESCE(v_applying, ''), true);

        EXECUTE pg_catalog.format($SQL$
            SELECT q.merge_status->'temporal_merge'->>'error_message'
            FROM %s AS q
            WHERE (q.merge_status->'temporal_merge'->>'status')::sql_saga.temporal_merge_feedback_status = 'ERROR'
            ORDER BY q.row_id
            LIMIT 1
        $SQL$, queue_oid) INTO v_error_message;
        IF v_error_message IS NOT NULL THEN
            RAISE EXCEPTION 'sql_saga: applying the change for portion of time failed. First error: %', v_error_message;
        END IF;

        EXECUTE pg_catalog.format('TRUNCATE %s', queue_oid);
    END LOOP;

    RETURN NULL;
END;
$for_portion_of_apply_trigger$;
