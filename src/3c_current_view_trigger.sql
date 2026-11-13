/*
 * "Now" for a current view, by the clock the view is bound to: the right-hand side of its "<range> @> <clock>"
 * filter, read from its rule (sql_saga.__internal_current_view_clock) and evaluated here, on a fixed search_path,
 * so that exactly the function the view calls is called, never a same-named object earlier on the caller's path.
 * SECURITY INVOKER: the clock runs with the privileges of whoever writes through the view, as the view's own
 * filter does. EXECUTE for PUBLIC (explicit, also against restrictive default privileges): current_view_trigger
 * calls it as that invoker. It evaluates only an expression PostgreSQL already parsed into the view's rule.
 */
CREATE FUNCTION sql_saga.__internal_current_view_now(view_oid oid, range_subtype regtype)
RETURNS text
LANGUAGE plpgsql
STABLE
STRICT
SECURITY INVOKER
SET search_path = pg_catalog, pg_temp
AS $__internal_current_view_now$
DECLARE
    v_now text;
BEGIN
    EXECUTE pg_catalog.format('SELECT (%s)::%s::text', sql_saga.__internal_current_view_clock(view_oid), range_subtype)
    INTO v_now;
    RETURN v_now;
END;
$__internal_current_view_now$;

GRANT EXECUTE ON FUNCTION sql_saga.__internal_current_view_now(oid, regtype) TO PUBLIC;

CREATE FUNCTION sql_saga.current_view_trigger()
 RETURNS trigger
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    info record;
    identifier_columns name[];
    delete_mode name;
    now_function text;
    source_table_name text;
    -- For manual UPDATE
    where_clause text;
    jnew jsonb;
    insert_column_list text;
    insert_values_list text;
BEGIN
    -- Extract parameters: delete_mode, then identifier_columns
    delete_mode := TG_ARGV[0];
    -- TG_ARGV is a 0-indexed array. The identifier columns start at index 1.
    identifier_columns := TG_ARGV[1:TG_NARGS-1];

    -- Get metadata about the view's underlying table.
    SELECT
        v.table_schema, v.table_name, v.era_name, v.generated_columns, e.range_column_name, e.valid_from_column_name, e.valid_until_column_name, e.valid_to_column_name,
        e.range_subtype, to_regclass(format('%I.%I', v.table_schema /* %I */, v.table_name /* %I */)) as table_oid,
        v.current_func
    INTO info
    FROM sql_saga.updatable_view AS v
    JOIN sql_saga.era AS e USING (table_schema, table_name, era_name)
    WHERE v.view_schema = TG_TABLE_SCHEMA AND v.view_name = TG_TABLE_NAME
      AND v.view_type = 'current';

    IF NOT FOUND THEN
         RAISE EXCEPTION 'sql_saga: could not find metadata for view %.%', quote_ident(TG_TABLE_SCHEMA), quote_ident(TG_TABLE_NAME);
    END IF;

    -- The era's range column and its derived boundary columns say WHEN a version is valid, not
    -- WHO the entity is. Views created before this was enforced in add_current_view may still
    -- pass them as identifier columns (from a temporal primary key), so drop them here as well.
    identifier_columns := array_remove(identifier_columns, info.range_column_name);
    identifier_columns := array_remove(identifier_columns, info.valid_from_column_name);
    identifier_columns := array_remove(identifier_columns, info.valid_until_column_name);
    identifier_columns := array_remove(identifier_columns, info.valid_to_column_name);

    -- "Now" by the clock the view is bound to (read from its rule and evaluated on a fixed search_path), as a
    -- typed literal. Not info.current_func, the text as registered: re-resolving that on the caller's
    -- search_path could call a different, same-named function (or a shadowing now()).
    now_function := format('%L::%s', sql_saga.__internal_current_view_now(TG_RELID, info.range_subtype),
                           info.range_subtype /* %s */);

    IF (TG_OP = 'INSERT') THEN
        -- An INSERT creates a new history, starting now. Inserts are batched (1.7): this row trigger only
        -- prepares the row's source and appends it to pg_temp.__temp_current_view_<view oid>; the view's
        -- statement-level trigger, sql_saga.current_view_apply_trigger(), applies the queue with one
        -- temporal_merge (MERGE_ENTITY_UPSERT), exactly as the former per-row merge did, row by row.
        source_table_name := format('__temp_current_view_%s', TG_RELID);

        -- A statement on this view issued while its queue is being applied (for example by a trigger on the
        -- base table) would add to, and apply, the queue being read. Refuse it.
        IF TG_RELID::text = ANY (pg_catalog.string_to_array(pg_catalog.current_setting('sql_saga.current_view_applying', true), ',')) THEN
            RAISE EXCEPTION 'sql_saga: cannot insert into view %.% while an insert through it is being applied',
                quote_ident(TG_TABLE_SCHEMA), quote_ident(TG_TABLE_NAME)
                USING ERRCODE = 'object_in_use',
                      HINT = 'Write to the base table directly from the trigger.';
        END IF;

        -- A queued row is applied only by the view's apply trigger. Without it (a view's triggers cannot be
        -- disabled, but one can be dropped despite drop protection), refuse rather than report a row never stored.
        IF NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE t.tgrelid = TG_RELID
              AND t.tgfoid = 'sql_saga.current_view_apply_trigger()'::pg_catalog.regprocedure
              AND t.tgtype = 4) -- AFTER INSERT FOR EACH STATEMENT
        THEN
            RAISE EXCEPTION 'sql_saga: view %.% has no apply trigger, so its inserts would not be made',
                quote_ident(TG_TABLE_SCHEMA), quote_ident(TG_TABLE_NAME)
                USING ERRCODE = 'object_not_in_prerequisite_state',
                      HINT = 'Recreate the view with sql_saga.drop_current_view and sql_saga.add_current_view.';
        END IF;

        DECLARE
            v_cols_def text;
            v_cols text;
            v_source_cols_expr text;
            v_row_id bigint;
        BEGIN
            -- The base table's columns, excluding GENERATED columns that cannot be inserted.
            SELECT string_agg(format('%I %s', pa.attname, format_type(pa.atttypid, pa.atttypmod)), ', ' ORDER BY pa.attnum),
                   string_agg(format('%I', pa.attname), ', ' ORDER BY pa.attnum),
                   string_agg(format('($1).%I', pa.attname), ', ' ORDER BY pa.attnum)
            INTO v_cols_def, v_cols, v_source_cols_expr
            FROM pg_attribute pa
            WHERE pa.attrelid = info.table_oid
              AND pa.attname <> ALL(info.generated_columns)
              AND pa.attnum > 0 AND NOT pa.attisdropped;

            IF to_regclass(format('pg_temp.%I', source_table_name)) IS NULL THEN
                EXECUTE format('CREATE TEMP TABLE %I (row_id bigserial, %s, merge_status jsonb) ON COMMIT DROP',
                    source_table_name, v_cols_def);
            END IF;

            EXECUTE format('INSERT INTO pg_temp.%I (%s) SELECT %s RETURNING row_id',
                source_table_name, v_cols, v_source_cols_expr)
            INTO v_row_id USING NEW;

            -- The new history starts now: handle both range-only and boundary column modes.
            IF info.valid_from_column_name IS NOT NULL AND info.valid_until_column_name IS NOT NULL THEN
                EXECUTE format(
                    'UPDATE pg_temp.%I SET %I = %s::%s, %I = ''infinity''::%s WHERE row_id = $1',
                    source_table_name, /* %I */
                    info.valid_from_column_name, /* %I */
                    now_function, /* %s */
                    info.range_subtype, /* %s */
                    info.valid_until_column_name, /* %I */
                    info.range_subtype /* %s */
                ) USING v_row_id;
            ELSE
                EXECUTE format(
                    'UPDATE pg_temp.%I SET %I = %srange(%s, ''infinity'') WHERE row_id = $1',
                    source_table_name, /* %I */
                    info.range_column_name, /* %I */
                    info.range_subtype, /* %s */
                    now_function /* %s */
                ) USING v_row_id;
            END IF;
        END;
        RETURN NEW;

    ELSIF (TG_OP = 'UPDATE') THEN
        -- An UPDATE can be a state change (SCD Type 2) or a documented soft-delete.
        -- We use a protocol: setting valid_from = 'infinity' signals a soft-delete.

        -- Protocol for soft-delete: UPDATE view SET valid_from = 'infinity', comment = '...';
        -- For range-only tables, check if the range starts at infinity
        IF (info.valid_from_column_name IS NOT NULL AND to_jsonb(NEW)->>info.valid_from_column_name = 'infinity') OR
           (info.valid_from_column_name IS NULL AND lower((to_jsonb(NEW)->>info.range_column_name)::daterange) = 'infinity') THEN
            DECLARE
                set_clause text;
                jnew_data jsonb;
                now_value text;
            BEGIN
                -- Get the value of now_function to compare against valid_from
                EXECUTE format('SELECT (%s)::%s::text', now_function /* %s */, info.range_subtype /* %s */) INTO now_value;

                -- Check if created on same "day" - handle both modes
                IF (info.valid_from_column_name IS NOT NULL AND (to_jsonb(OLD)->>info.valid_from_column_name) = now_value) OR
                   (info.valid_from_column_name IS NULL AND lower((to_jsonb(OLD)->>info.range_column_name)::daterange)::text = now_value) THEN
                    -- A soft-delete on the same "day" as creation would violate the
                    -- valid_from < valid_until constraint. In this case, the record
                    -- never existed for any duration, so we delete it entirely.
                    SELECT string_agg(quote_ident(c) || ' = ($1).' || quote_ident(c), ' AND ')
                    INTO where_clause
                    FROM unnest(identifier_columns) AS u(c);

                    IF info.valid_until_column_name IS NOT NULL THEN
                        EXECUTE format('DELETE FROM %I.%I WHERE %s AND %I = ''infinity''',
                            info.table_schema, /* %I */
                            info.table_name, /* %I */
                            where_clause, /* %s */
                            info.valid_until_column_name /* %I */
                        )
                        USING OLD;
                    ELSE
                        -- Range-only mode: check if range extends to infinity
                        EXECUTE format('DELETE FROM %I.%I WHERE %s AND (upper(%I) IS NULL OR upper(%I) = ''infinity''::%s)',
                            info.table_schema, /* %I */
                            info.table_name, /* %I */
                            where_clause, /* %s */
                            info.range_column_name, /* %I */
                            info.range_column_name, /* %I */
                            info.range_subtype /* %s */
                        )
                        USING OLD;
                    END IF;
                ELSE
                    -- Standard soft-delete: end the validity of the current record.
                    SELECT string_agg(quote_ident(c) || ' = ($1).' || quote_ident(c), ' AND ')
                    INTO where_clause
                    FROM unnest(identifier_columns) AS u(c);

                    jnew_data := to_jsonb(NEW) - identifier_columns;
                    
            -- Strip GENERATED columns from jnew_data using direct array subtraction
            jnew_data := jnew_data - info.generated_columns;
                    
                    IF info.range_column_name IS NOT NULL THEN jnew_data := jnew_data - info.range_column_name; END IF;
                    IF info.valid_from_column_name IS NOT NULL THEN jnew_data := jnew_data - info.valid_from_column_name; END IF;
                    IF info.valid_until_column_name IS NOT NULL THEN jnew_data := jnew_data - info.valid_until_column_name; END IF;
                    IF info.valid_to_column_name IS NOT NULL THEN jnew_data := jnew_data - info.valid_to_column_name; END IF;

                    SELECT string_agg(format('%I = %L', key /* %I */, value /* %L */), ', ')
                    INTO set_clause
                    FROM jsonb_each_text(jnew_data);

                    IF info.valid_until_column_name IS NOT NULL THEN
                        -- Add the valid_until clause separately to ensure now_function is executed, not treated as a literal.
                        set_clause := set_clause || format(', %I = %s', info.valid_until_column_name /* %I */, now_function /* %s */);

                        EXECUTE format('UPDATE %I.%I SET %s WHERE %s AND %I = ''infinity''',
                            info.table_schema, /* %I */
                            info.table_name, /* %I */
                            set_clause, /* %s */
                            where_clause, /* %s */
                            info.valid_until_column_name /* %I */
                        )
                        USING OLD;
                    ELSE
                        -- Range-only mode: update the range to end at now
                        set_clause := set_clause || format(', %I = %srange(lower(%I), %s)',
                            info.range_column_name /* %I */,
                            info.range_subtype /* %s */,
                            info.range_column_name /* %I */,
                            now_function /* %s */
                        );

                        EXECUTE format('UPDATE %I.%I SET %s WHERE %s AND (upper(%I) IS NULL OR upper(%I) = ''infinity''::%s)',
                            info.table_schema, /* %I */
                            info.table_name, /* %I */
                            set_clause, /* %s */
                            where_clause, /* %s */
                            info.range_column_name, /* %I */
                            info.range_column_name, /* %I */
                            info.range_subtype /* %s */
                        )
                        USING OLD;
                    END IF;
                END IF;
            END;

        -- Standard SCD Type 2 update
        ELSE
            SELECT string_agg(quote_ident(col) || ' = ($1).' || quote_ident(col), ' AND ')
            INTO where_clause
            FROM unnest(identifier_columns) AS t(col);

            IF info.valid_until_column_name IS NOT NULL THEN
                EXECUTE format('UPDATE %I.%I SET %I = %s WHERE %s AND %I = ''infinity''',
                    info.table_schema, /* %I */
                    info.table_name, /* %I */
                    info.valid_until_column_name, /* %I */
                    now_function, /* %s */
                    where_clause, /* %s */
                    info.valid_until_column_name /* %I */
                )
                USING OLD;
            ELSE
                -- Range-only mode: update the range to end at now
                -- Note: Check for infinity upper bound, not NULL (ranges can have 'infinity' as a value)
                EXECUTE format('UPDATE %I.%I SET %I = %srange(lower(%I), %s) WHERE %s AND (upper(%I) IS NULL OR upper(%I) = ''infinity''::%s)',
                    info.table_schema, /* %I */
                    info.table_name, /* %I */
                    info.range_column_name, /* %I */
                    info.range_subtype, /* %s */
                    info.range_column_name, /* %I */
                    now_function, /* %s */
                    where_clause, /* %s */
                    info.range_column_name, /* %I */
                    info.range_column_name, /* %I */
                    info.range_subtype /* %s */
                )
                USING OLD;
            END IF;

            jnew := to_jsonb(NEW);
            
            -- Strip GENERATED columns from jnew using direct array subtraction
            jnew := jnew - info.generated_columns;
            
            IF info.range_column_name IS NOT NULL THEN jnew := jnew - info.range_column_name; END IF;
            IF info.valid_from_column_name IS NOT NULL THEN jnew := jnew - info.valid_from_column_name; END IF;
            IF info.valid_until_column_name IS NOT NULL THEN jnew := jnew - info.valid_until_column_name; END IF;
            IF info.valid_to_column_name IS NOT NULL THEN jnew := jnew - info.valid_to_column_name; END IF;

            SELECT string_agg(quote_ident(key), ', '), string_agg(quote_nullable(value), ', ')
            INTO insert_column_list, insert_values_list
            FROM jsonb_each_text(jnew);

            IF info.valid_from_column_name IS NOT NULL AND info.valid_until_column_name IS NOT NULL THEN
                insert_column_list := insert_column_list || format(', %I, %I', info.valid_from_column_name /* %I */, info.valid_until_column_name /* %I */);
                insert_values_list := insert_values_list || format(', %s, ''infinity''', now_function /* %s */);
            ELSE
                -- Range-only mode: add the range column
                insert_column_list := insert_column_list || format(', %I', info.range_column_name /* %I */);
                insert_values_list := insert_values_list || format(', %srange(%s, ''infinity'')', info.range_subtype /* %s */, now_function /* %s */);
            END IF;

            EXECUTE format('INSERT INTO %I.%I (%s) VALUES (%s)',
                info.table_schema, /* %I */
                info.table_name, /* %I */
                insert_column_list, /* %s */
                insert_values_list /* %s */
            );
        END IF;

        RETURN NEW;

    ELSIF (TG_OP = 'DELETE') THEN
        CASE delete_mode
            WHEN 'delete_as_cutoff' THEN
                DECLARE
                    now_value text;
                    is_same_day boolean;
                BEGIN
                    -- Get the value of now_function
                    EXECUTE format('SELECT (%s)::%s::text', now_function /* %s */, info.range_subtype /* %s */) INTO now_value;

                    -- Check if created on same "day" - if so, delete entirely instead of creating empty range
                    IF info.valid_from_column_name IS NOT NULL THEN
                        is_same_day := (to_jsonb(OLD)->>info.valid_from_column_name) = now_value;
                    ELSE
                        is_same_day := lower((to_jsonb(OLD)->>info.range_column_name)::daterange)::text = now_value;
                    END IF;

                    SELECT string_agg(quote_ident(col) || ' = ($1).' || quote_ident(col), ' AND ')
                    INTO where_clause
                    FROM unnest(identifier_columns) AS t(col);

                    IF is_same_day THEN
                        -- Entity was created on the same "day" - delete entirely
                        IF info.valid_until_column_name IS NOT NULL THEN
                            EXECUTE format('DELETE FROM %I.%I WHERE %s AND %I = ''infinity''',
                                info.table_schema, /* %I */
                                info.table_name, /* %I */
                                where_clause, /* %s */
                                info.valid_until_column_name /* %I */
                            )
                            USING OLD;
                        ELSE
                            -- Range-only mode
                            EXECUTE format('DELETE FROM %I.%I WHERE %s AND (upper(%I) IS NULL OR upper(%I) = ''infinity''::%s)',
                                info.table_schema, /* %I */
                                info.table_name, /* %I */
                                where_clause, /* %s */
                                info.range_column_name, /* %I */
                                info.range_column_name, /* %I */
                                info.range_subtype /* %s */
                            )
                            USING OLD;
                        END IF;
                    ELSE
                        -- Standard soft-delete: end the validity at now
                        IF info.valid_until_column_name IS NOT NULL THEN
                            EXECUTE format('UPDATE %I.%I SET %I = %s WHERE %s AND %I = ''infinity''',
                                info.table_schema, /* %I */
                                info.table_name, /* %I */
                                info.valid_until_column_name, /* %I */
                                now_function, /* %s */
                                where_clause, /* %s */
                                info.valid_until_column_name /* %I */
                            )
                            USING OLD;
                        ELSE
                            -- Range-only mode: update the range to end at now
                            EXECUTE format('UPDATE %I.%I SET %I = %srange(lower(%I), %s) WHERE %s AND (upper(%I) IS NULL OR upper(%I) = ''infinity''::%s)',
                                info.table_schema, /* %I */
                                info.table_name, /* %I */
                                info.range_column_name, /* %I */
                                info.range_subtype, /* %s */
                                info.range_column_name, /* %I */
                                now_function, /* %s */
                                where_clause, /* %s */
                                info.range_column_name, /* %I */
                                info.range_column_name, /* %I */
                                info.range_subtype /* %s */
                            )
                            USING OLD;
                        END IF;
                    END IF;
                END;
            WHEN 'delete_as_documented_ending' THEN
                RAISE EXCEPTION 'Direct DELETE on a "current" view is disallowed.'
                    USING HINT = 'To end an entity''s timeline, perform an UPDATE. For an undocumented soft-delete, use the protocol: UPDATE ... SET valid_from = ''infinity''. For a documented soft-delete, add a comment or status change: UPDATE ... SET valid_from = ''infinity'', comment = ''...''';
            ELSE
                -- This case should not be reachable due to the check in add_current_view, but it is good practice.
                RAISE EXCEPTION 'sql_saga: internal error: unhandled delete_mode "%"', delete_mode;
        END CASE;
        RETURN OLD;
    END IF;

    RETURN NULL;
END;
$function$;

/*
 * The statement-level half of a current view's INSERT (1.7): AFTER INSERT FOR EACH STATEMENT, created by
 * add_current_view as the view's apply trigger (updatable_view.trigger_name || '_apply').
 *
 * sql_saga.current_view_trigger() only prepares each inserted row's source (its new history starting now) and
 * queues it in pg_temp.__temp_current_view_<view oid>. This trigger applies the queue with one temporal_merge
 * (MERGE_ENTITY_UPSERT, each row its own founding id), exactly as the former per-row merge did, and empties it.
 * Two rows for the same entity give the later row's values, as applying them one by one did.
 *
 * It runs inside the statement as the caller (SECURITY INVOKER): an error aborts the statement, and the
 * caller's privileges and row-level security decide every physical change. While the queue is applied,
 * sql_saga.current_view_applying names the view, and the row trigger refuses a nested insert through it.
 */
CREATE FUNCTION sql_saga.current_view_apply_trigger()
 RETURNS trigger
 LANGUAGE plpgsql
AS $current_view_apply_trigger$
#variable_conflict use_variable
DECLARE
    info record;
    identifier_columns name[];
    queue_oid regclass;
    v_nonempty boolean;
    v_error_message text;
    v_applying text;
BEGIN
    IF TG_LEVEL <> 'STATEMENT' OR TG_WHEN <> 'AFTER' OR TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'sql_saga: current_view_apply_trigger() must be an AFTER INSERT FOR EACH STATEMENT trigger'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    queue_oid := pg_catalog.to_regclass(pg_catalog.format('pg_temp.%I', '__temp_current_view_' || TG_RELID));
    IF queue_oid IS NULL THEN
        RETURN NULL;
    END IF;
    EXECUTE pg_catalog.format('SELECT EXISTS (SELECT FROM %s)', queue_oid) INTO v_nonempty;
    IF NOT v_nonempty THEN
        RETURN NULL;
    END IF;

    SELECT v.era_name, v.trigger_name, pg_catalog.format('%I.%I', v.table_schema, v.table_name)::regclass AS table_oid,
           e.range_column_name, e.valid_from_column_name, e.valid_until_column_name, e.valid_to_column_name
    INTO info
    FROM sql_saga.updatable_view AS v
    JOIN sql_saga.era AS e USING (table_schema, table_name, era_name)
    WHERE v.view_schema = TG_TABLE_SCHEMA AND v.view_name = TG_TABLE_NAME
      AND v.view_type = 'current';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'sql_saga: could not find metadata for view %.%', quote_ident(TG_TABLE_SCHEMA), quote_ident(TG_TABLE_NAME);
    END IF;

    -- The entity identifier: the row trigger's arguments after its delete mode, without the temporal columns.
    SELECT (pg_catalog.string_to_array(pg_catalog.rtrim(pg_catalog.encode(t.tgargs, 'escape'), E'\\000'), E'\\000'))[2:]::name[]
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

    v_applying := pg_catalog.current_setting('sql_saga.current_view_applying', true);
    PERFORM pg_catalog.set_config('sql_saga.current_view_applying',
        pg_catalog.concat_ws(',', NULLIF(v_applying, ''), TG_RELID::text), true);

    CALL sql_saga.temporal_merge(
        target_table             => info.table_oid,
        source_table             => queue_oid,
        primary_identity_columns => identifier_columns::text[],
        ephemeral_columns        => '{}'::text[],
        era_name                 => info.era_name,
        mode                     => 'MERGE_ENTITY_UPSERT'::sql_saga.temporal_merge_mode,
        row_id_column            => 'row_id',
        founding_id_column       => 'row_id',
        update_source_with_feedback => true,
        feedback_status_column   => 'merge_status',
        feedback_status_key      => 'temporal_merge'
    );

    PERFORM pg_catalog.set_config('sql_saga.current_view_applying', COALESCE(v_applying, ''), true);

    EXECUTE pg_catalog.format($SQL$
        SELECT q.merge_status->'temporal_merge'->>'error_message'
        FROM %s AS q
        WHERE (q.merge_status->'temporal_merge'->>'status')::sql_saga.temporal_merge_feedback_status = 'ERROR'
        ORDER BY q.row_id
        LIMIT 1
    $SQL$, queue_oid) INTO v_error_message;
    IF v_error_message IS NOT NULL THEN
        RAISE EXCEPTION 'sql_saga: applying the insert through view %.% failed. First error: %',
            quote_ident(TG_TABLE_SCHEMA), quote_ident(TG_TABLE_NAME), v_error_message;
    END IF;

    EXECUTE pg_catalog.format('TRUNCATE %s', queue_oid);
    RETURN NULL;
END;
$current_view_apply_trigger$;
