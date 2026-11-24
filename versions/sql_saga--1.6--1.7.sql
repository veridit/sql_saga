\echo Use "ALTER EXTENSION sql_saga UPDATE TO '1.7'" to load this file. \quit
CREATE OR REPLACE FUNCTION sql_saga.native_library_version() RETURNS text LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT AS 'sql_saga_native', 'sql_saga_native_version_wrapper';
DO $native_guard_1_7$
BEGIN
    IF (SELECT pg_catalog.array_agg(x::int) FROM pg_catalog.unnest(pg_catalog.string_to_array(sql_saga.native_library_version(), '.')) AS x) < (SELECT pg_catalog.array_agg(x::int) FROM pg_catalog.unnest(pg_catalog.string_to_array('1.7', '.')) AS x) THEN
        RAISE EXCEPTION 'sql_saga: native library version % does not match extension update version 1.7', sql_saga.native_library_version()
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Install the sql_saga 1.7 native library and restart PostgreSQL before retrying ALTER EXTENSION.';
    END IF;
END;
$native_guard_1_7$;
SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('sql_saga 1.7 upgrade'));


/*
 * The statement-level apply trigger of an updatable view is named after its row trigger
 * (updatable_view.trigger_name), as add_for_portion_of_view (1.6) and add_current_view (1.7) create it.
 */
CREATE OR REPLACE FUNCTION sql_saga.__internal_view_apply_trigger_name(view_type sql_saga.updatable_view_type, trigger_name name)
RETURNS name
LANGUAGE sql
IMMUTABLE
AS $__internal_view_apply_trigger_name$
    SELECT (trigger_name || '_apply')::name;
$__internal_view_apply_trigger_name$;

CREATE OR REPLACE FUNCTION sql_saga.current_view_trigger()
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
CREATE OR REPLACE FUNCTION sql_saga.current_view_apply_trigger()
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
CREATE OR REPLACE FUNCTION sql_saga.__api_add_current_view(table_oid regclass, era_name name DEFAULT 'valid', delete_mode name DEFAULT 'delete_as_cutoff', current_func_name text DEFAULT NULL)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, pg_temp
AS
$function$
#variable_conflict use_variable
DECLARE
    r record;
    view_name name;
    trigger_name name;
    target_schema_name name;
    target_table_name name;
    era_exists boolean;
BEGIN
    PERFORM sql_saga.__internal_entry_guard();
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'table_oid must be specified';
    END IF;
    PERFORM sql_saga.__internal_require_owner(table_oid, 'add_current_view');
    /* The view is created in the table's schema for its owner (1.4, review round 3, Q1). */
    PERFORM sql_saga.__internal_require_create(
        (SELECT c.relnamespace FROM pg_catalog.pg_class AS c WHERE c.oid = table_oid),
        (SELECT c.relowner FROM pg_catalog.pg_class AS c WHERE c.oid = table_oid)::regrole, 'add_current_view');

    IF delete_mode NOT IN ('delete_as_cutoff', 'delete_as_documented_ending') THEN
        RAISE EXCEPTION 'delete_mode must be one of ''delete_as_cutoff'' or ''delete_as_documented_ending''';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    SELECT n.nspname, c.relname INTO target_schema_name, target_table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = table_oid;

    -- Validate era exists (fail-fast)
    SELECT EXISTS (
        SELECT 1 FROM sql_saga.era AS p
        WHERE (p.table_schema, p.table_name) = (target_schema_name, target_table_name)
          AND p.era_name = era_name
    ) INTO era_exists;

    IF NOT era_exists THEN
        RAISE EXCEPTION 'era "%" does not exist on table %', era_name, table_oid;
    END IF;

    FOR r IN
        SELECT p.table_schema AS schema_name, p.table_name AS table_name, c.relowner AS table_owner, p.era_name, c.oid AS table_oid, p.range_type, p.range_subtype, p.range_subtype_category, p.range_column_name, p.valid_from_column_name, p.valid_until_column_name, p.valid_to_column_name
        FROM sql_saga.era AS p
        JOIN pg_catalog.pg_class AS c ON c.relname = p.table_name
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace AND n.nspname = p.table_schema
        WHERE (p.table_schema, p.table_name) = (target_schema_name, target_table_name)
          AND p.era_name = era_name
          AND p.era_name <> 'system_time'
          AND NOT EXISTS (
                SELECT FROM sql_saga.updatable_view AS v
                WHERE (v.table_schema, v.table_name, v.era_name) = (p.table_schema, p.table_name, p.era_name)
                  AND v.view_type = 'current')
    LOOP
        DECLARE
            identifier_columns name[];
            identifier_columns_quoted text;
            non_temporal_columns_quoted text;
            range_category char;
            now_function text;
            now_sql text;
            trigger_args text;
            generated_columns name[];
        BEGIN
            -- Verify the era is time-based by checking the cached category
            IF r.range_subtype_category <> 'D' THEN -- 'D' for all DateTime types
                 RAISE EXCEPTION 'Cannot create a "current" view for era "%" on table "%" because it is a range of %, and only date or timestamp is supported.',
                    r.era_name, sql_saga.__internal_display_name(table_oid), r.range_subtype::regtype;
            END IF;

            -- Determine now() function based on range subtype, but allow override for testing
            IF current_func_name IS NULL THEN
                SELECT CASE t.typname
                    WHEN 'date' THEN 'CURRENT_DATE'
                    ELSE 'now()'
                END INTO now_function
                FROM pg_type t
                WHERE t.oid = r.range_subtype;
            ELSE
                now_function := current_func_name;
            END IF;

            -- Validate the function name to prevent SQL injection.
            -- We allow CURRENT_DATE and now() as special keywords, otherwise it must be a valid function. Released
            -- contract (test 135): an unqualified name resolves on sql_saga, pg_catalog, public, not on the caller's
            -- path. That path no longer is the worker's (it must not contain a schema other roles can write, R8), so
            -- it is set for the resolution only, and the view embeds the bound function schema-qualified. This is
            -- name resolution only: the clock runs with the rights of whoever reads through the view.
            now_sql := now_function;
            IF lower(now_function) NOT IN ('current_date', 'now()') THEN
                DECLARE
                    v_worker_path text := current_setting('search_path');
                    v_clock regprocedure;
                BEGIN
                    PERFORM set_config('search_path', 'sql_saga, pg_catalog, public', true);
                    BEGIN
                        v_clock := now_function::regprocedure;
                    EXCEPTION
                        WHEN OTHERS THEN
                            PERFORM set_config('search_path', v_worker_path, true);
                            RAISE EXCEPTION 'current_func_name must be "CURRENT_DATE", "now()", or a valid function signature, but is "%"', now_function;
                    END;
                    PERFORM set_config('search_path', v_worker_path, true);
                    now_sql := pg_catalog.format('%I.%I()',
                        (SELECT n.nspname FROM pg_catalog.pg_proc AS p JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace WHERE p.oid = v_clock),
                        (SELECT p.proname FROM pg_catalog.pg_proc AS p WHERE p.oid = v_clock));
                END;
            END IF;

            -- Find identifier columns (PK or single-column unique key)
            SELECT uk.column_names INTO identifier_columns
            FROM sql_saga.unique_keys uk
            WHERE (uk.table_schema, uk.table_name, uk.era_name) = (r.schema_name, r.table_name, r.era_name)
              AND array_length(uk.column_names, 1) = 1;

            IF identifier_columns IS NULL THEN
                -- Fall back to the primary key. A temporal primary key contains the era's range
                -- column (and possibly its derived boundary columns); those say WHEN a version is
                -- valid, not WHO the entity is, so they are not part of the entity identity.
                SELECT array_agg(a.attname ORDER BY u.ordinality)
                INTO identifier_columns
                FROM pg_catalog.pg_constraint c
                JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS u(attnum, ordinality) ON true
                JOIN pg_catalog.pg_attribute a ON (a.attrelid, a.attnum) = (c.conrelid, u.attnum)
                WHERE (c.conrelid, c.contype) = (r.table_oid, 'p')
                  AND a.attname IS DISTINCT FROM r.range_column_name
                  AND a.attname IS DISTINCT FROM r.valid_from_column_name
                  AND a.attname IS DISTINCT FROM r.valid_until_column_name
                  AND a.attname IS DISTINCT FROM r.valid_to_column_name;
            END IF;

            IF identifier_columns IS NULL THEN
                RAISE EXCEPTION 'table "%" must have a primary key or a single-column temporal unique key for era "%" to support updatable views',
                    sql_saga.__internal_display_name(table_oid), r.era_name;
            END IF;

            -- Compute GENERATED columns to exclude from temp source tables
            -- Includes GENERATED ALWAYS AS IDENTITY and GENERATED ALWAYS AS ... STORED columns
            SELECT COALESCE(array_agg(pa.attname ORDER BY pa.attnum), '{}')
            INTO generated_columns
            FROM pg_attribute pa
            WHERE pa.attrelid = r.table_oid
              AND pa.attnum > 0
              AND NOT pa.attisdropped
              AND (pa.attgenerated <> '' OR pa.attidentity = 'a'); -- GENERATED columns or ALWAYS IDENTITY

            view_name := sql_saga.__internal_make_updatable_view_name(r.table_name, r.era_name, 'current');
            trigger_name := 'current_' || r.era_name;

            -- The view includes all columns from the base table, but is filtered to only show the current records.
            -- This makes its schema consistent with the for_portion_of view, enabling parameter passing via SET.
            PERFORM sql_saga.__internal_execute_as_caller(format('CREATE VIEW %1$I.%2$I WITH (security_barrier=true, security_invoker=on) AS SELECT * FROM %1$I.%3$I WHERE %4$I @> %5$s::%6$s',
                r.schema_name, view_name, r.table_name, r.range_column_name, now_sql, r.range_subtype));
            PERFORM sql_saga.__internal_execute_as_caller(format('ALTER VIEW %1$I.%2$I OWNER TO %s', r.schema_name, view_name, r.table_owner::regrole));
            -- security_invoker: the caller's rights on the base decide; the view itself is open.
            PERFORM sql_saga.__internal_execute_as_caller(format('GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE %I.%I TO PUBLIC', r.schema_name, view_name));

            -- Pass identifier columns, delete mode, and comment column to trigger
            SELECT string_agg(quote_literal(c), ', ')
            INTO identifier_columns_quoted
            FROM unnest(identifier_columns) AS u(c);

            trigger_args := concat_ws(', ',
                quote_literal(delete_mode),
                identifier_columns_quoted
            );

            PERFORM sql_saga.__internal_execute_as_caller(format('CREATE TRIGGER %I INSTEAD OF INSERT OR UPDATE OR DELETE ON %I.%I FOR EACH ROW EXECUTE PROCEDURE sql_saga.current_view_trigger(%s)',
                trigger_name, r.schema_name, view_name, trigger_args));
            -- The row trigger queues each inserted row; the apply trigger merges the statement's inserts once.
            PERFORM sql_saga.__internal_execute_as_caller(format('CREATE TRIGGER %I AFTER INSERT ON %I.%I FOR EACH STATEMENT EXECUTE PROCEDURE sql_saga.current_view_apply_trigger()',
                trigger_name || '_apply', r.schema_name, view_name));
            INSERT INTO sql_saga.updatable_view (view_schema, view_name, view_type, table_schema, table_name, era_name, trigger_name, current_func, generated_columns)
                VALUES (r.schema_name, view_name, 'current', r.schema_name, r.table_name, r.era_name, trigger_name, now_function, generated_columns);
        END;
    END LOOP;

    RETURN true;
END;
$function$;
-- ---- end 1.3 small2 ----

CREATE OR REPLACE FUNCTION sql_saga.__internal_rename_following()
 RETURNS event_trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 -- Set a safe search path for this SECURITY DEFINER function
 SET search_path = sql_saga, pg_catalog, pg_temp
AS
$function$
#variable_conflict use_variable
DECLARE
    r record;
    sql text;
    v_is_alter_trigger boolean := false;
    is_alter_table boolean := false;
    cmd record;
    v_command_changes_shape boolean;
    v_shape_changed_table_oids oid[] := ARRAY[]::oid[];
    v_table_oid oid;
    v_current_columns name[];
    v_removed_columns name[];
    v_added_columns name[];
    v_missing_excluded name;
    v_unique_key_columns_before jsonb;
    v_altered_table_oids oid[];
    v_touched_relation_oids oid[];
BEGIN
    PERFORM sql_saga.__internal_entry_guard();
    /* DROP statements cannot rename or reshape surviving managed objects. */
    IF tg_tag LIKE 'DROP %' THEN
        RETURN;
    END IF;

    FOR cmd IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        IF cmd.command_tag = 'ALTER TABLE' THEN
            is_alter_table := true;
            IF cmd.classid = 'pg_catalog.pg_class'::regclass THEN
                v_command_changes_shape := cmd.objsubid > 0
                    OR sql_saga.__internal_alter_table_changes_column_shape(cmd.command);

                IF v_command_changes_shape THEN
                    IF EXISTS (
                        SELECT 1
                        FROM sql_saga.system_time_era AS ste
                        JOIN pg_catalog.pg_class AS c
                          ON c.oid = cmd.objid
                        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                        WHERE (ste.table_schema, ste.table_name) = (n.nspname, c.relname))
                       AND NOT cmd.objid = ANY (v_shape_changed_table_oids) THEN
                        v_shape_changed_table_oids := pg_catalog.array_append(v_shape_changed_table_oids, cmd.objid);
                    END IF;

                    -- PostgreSQL propagates column-shape changes through table inheritance. Avoid all
                    -- pg_inherits work for the overwhelmingly common no-children case.
                    IF (SELECT c.relhassubclass FROM pg_catalog.pg_class AS c WHERE c.oid = cmd.objid) THEN
                        FOR v_table_oid IN
                            WITH RECURSIVE descendants(table_oid) AS (
                                SELECT i.inhrelid
                                FROM pg_catalog.pg_inherits AS i
                                WHERE i.inhparent = cmd.objid
                                UNION
                                SELECT i.inhrelid
                                FROM descendants AS d
                                JOIN pg_catalog.pg_inherits AS i ON i.inhparent = d.table_oid
                            )
                            SELECT c.oid
                            FROM descendants AS d
                            JOIN pg_catalog.pg_class AS c ON c.oid = d.table_oid
                            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                            JOIN sql_saga.system_time_era AS ste
                              ON (ste.table_schema, ste.table_name) = (n.nspname, c.relname)
                            JOIN LATERAL (
                                SELECT pg_catalog.array_agg(a.attname ORDER BY a.attname COLLATE "C") AS column_names
                                FROM pg_catalog.pg_attribute AS a
                                WHERE a.attrelid = c.oid
                                  AND a.attnum > 0
                                  AND NOT a.attisdropped
                            ) AS live ON true
                            -- Set comparison: only added/removed columns are shape changes.
                            WHERE (SELECT pg_catalog.array_agg(s.attname ORDER BY s.attname COLLATE "C")
                                   FROM pg_catalog.unnest(ste.table_columns_snapshot) AS s(attname))
                                  IS DISTINCT FROM live.column_names
                        LOOP
                            IF NOT v_table_oid = ANY (v_shape_changed_table_oids) THEN
                                v_shape_changed_table_oids := pg_catalog.array_append(
                                    v_shape_changed_table_oids, v_table_oid);
                            END IF;
                        END LOOP;
                    END IF;
                END IF;
            END IF;
        END IF;
    END LOOP;

    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        IF r.command_tag = 'ALTER TRIGGER' THEN
            v_is_alter_trigger := true;
            exit;
        END IF;
    END LOOP;

    -- Exit early if the DDL command does not affect a managed object.
    -- We bypass this for ALTER TRIGGER because the object identity contains
    -- the *new* trigger name, which won't be in our metadata yet.
    IF NOT v_is_alter_trigger
       AND cardinality(v_shape_changed_table_oids) = 0
       AND NOT sql_saga.__internal_ddl_command_affects_managed_object() THEN
        RETURN;
    END IF;

    /*
     * The relations this command touched: tables, views and indexes it names directly, the tables of
     * the constraints, triggers, policies and indexes it names, and their inheritance descendants
     * (PostgreSQL propagates renames to them). Built once from the catalog only.
     *
     * The constraint renames below are found by comparing constraint definitions
     * (pg_get_constraintdef), and deparsing a CHECK constraint takes a lock on its table. They only
     * deparse constraints of these relations: a rename can only happen on a relation the command
     * touched, and a concurrent session (such as another pg_restore -j worker) may hold an exclusive
     * lock on any other table while it waits for one of ours, which would be a deadlock.
     */
    v_touched_relation_oids := ARRAY(
        WITH RECURSIVE touched(relation_oid) AS (
            SELECT CASE ev.classid
                       WHEN 'pg_catalog.pg_class'::regclass THEN
                           coalesce((SELECT i.indrelid FROM pg_catalog.pg_index AS i WHERE i.indexrelid = ev.objid), ev.objid)
                       WHEN 'pg_catalog.pg_constraint'::regclass THEN
                           (SELECT c.conrelid FROM pg_catalog.pg_constraint AS c WHERE c.oid = ev.objid)
                       WHEN 'pg_catalog.pg_trigger'::regclass THEN
                           (SELECT t.tgrelid FROM pg_catalog.pg_trigger AS t WHERE t.oid = ev.objid)
                       WHEN 'pg_catalog.pg_policy'::regclass THEN
                           (SELECT p.polrelid FROM pg_catalog.pg_policy AS p WHERE p.oid = ev.objid)
                   END
            FROM pg_catalog.pg_event_trigger_ddl_commands() AS ev
            UNION
            SELECT i.inhrelid
            FROM touched AS t
            JOIN pg_catalog.pg_inherits AS i ON i.inhparent = t.relation_oid
        )
        SELECT t.relation_oid FROM touched AS t WHERE t.relation_oid IS NOT NULL);

    /*
     * Anything that is stored by reg* type will auto-adjust, but anything we
     * store by name will need to be updated after a rename. One way to do this
     * is to recreate the constraints we have and pull new names out that way.
     * If we are unable to do something like that, we must raise an exception.
     */

    ---
    --- era
    ---

    /*
     * Follow renames of the era's temporal columns (range, valid_from, valid_until, valid_to) on the
     * relations touched by this ALTER TABLE, and regenerate what names them (the synchronization
     * function, the referenced-side triggers of regular foreign keys). Runs before the unique keys
     * are followed, which exclude the era's range column by name.
     */
    IF is_alter_table THEN
        PERFORM sql_saga.__internal_era_column_rename_follow(ARRAY(
            SELECT DISTINCT c.objid
            FROM pg_catalog.pg_event_trigger_ddl_commands() AS c
            WHERE c.command_tag = 'ALTER TABLE' AND c.classid = 'pg_catalog.pg_class'::regclass));
    END IF;

    -- ---- 1.3 small2: era.ephemeral_columns follow RENAME COLUMN, pk consistency names follow RENAME CONSTRAINT ----
    IF is_alter_table THEN
        PERFORM sql_saga.__internal_era_ephemeral_and_pk_consistency_rename_follow();
    END IF;
    -- ---- end 1.3 small2 ----

    -- Follow renames of the bounds check constraint (NOT isempty(range)).
    FOR sql IN
        SELECT format('UPDATE sql_saga.era SET bounds_check_constraint = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            c.conname,
            e.table_schema,
            e.table_name,
            e.era_name
        )
        FROM sql_saga.era AS e
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (e.table_name, (SELECT oid FROM pg_namespace WHERE nspname = e.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        WHERE e.bounds_check_constraint IS NOT NULL
          AND e.bounds_check_constraint <> c.conname
          AND pc.oid = ANY (v_touched_relation_oids)
          AND sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = format('CHECK ((NOT isempty(%I)))', e.range_column_name)
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, e.bounds_check_constraint))
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames of the boundary check constraint (from < until).
    FOR sql IN
        SELECT format('UPDATE sql_saga.era SET boundary_check_constraint = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            c.conname,
            e.table_schema,
            e.table_name,
            e.era_name
        )
        FROM sql_saga.era AS e
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (e.table_name, (SELECT oid FROM pg_namespace WHERE nspname = e.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        WHERE e.boundary_check_constraint IS NOT NULL
          AND e.boundary_check_constraint <> c.conname
          AND e.valid_from_column_name IS NOT NULL
          AND e.valid_until_column_name IS NOT NULL
          AND pc.oid = ANY (v_touched_relation_oids)
          AND sql_saga.__internal_era_boundary_check_matches(sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids), e.valid_from_column_name, e.valid_until_column_name)
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, e.boundary_check_constraint))
    LOOP
        EXECUTE sql;
    END LOOP;

    ---
    --- system_time_era
    ---

    -- Follow renames of the infinity_check_constraint.
    -- The constraint format is: CHECK ((upper(range_column_name) = 'infinity'::timestamp with time zone))
    FOR sql IN
        SELECT format('UPDATE sql_saga.system_time_era SET infinity_check_constraint = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            c.conname,
            ste.table_schema,
            ste.table_name,
            ste.era_name
        )
        FROM sql_saga.system_time_era AS ste
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (ste.table_schema, ste.table_name, ste.era_name)
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (ste.table_name, (SELECT oid FROM pg_namespace WHERE nspname = ste.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        WHERE ste.infinity_check_constraint <> c.conname
          AND pc.oid = ANY (v_touched_relation_oids)
          AND sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = format('CHECK ((upper(%I) = ''infinity''::timestamp with time zone))', e.range_column_name)
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, ste.infinity_check_constraint))
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames of the generated_always_trigger.
    FOR sql IN
        SELECT format('UPDATE sql_saga.system_time_era SET generated_always_trigger = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            t.tgname,
            ste.table_schema,
            ste.table_name,
            ste.era_name
        )
        FROM sql_saga.system_time_era AS ste
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (ste.table_name, (SELECT oid FROM pg_namespace WHERE nspname = ste.table_schema))
        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = pc.oid
        WHERE t.tgname <> ste.generated_always_trigger
          AND t.tgfoid = 'sql_saga.generated_always_as_row_start_end()'::regprocedure
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (pc.oid, ste.generated_always_trigger))
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames of the write_history_trigger.
    FOR sql IN
        SELECT format('UPDATE sql_saga.system_time_era SET write_history_trigger = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            t.tgname,
            ste.table_schema,
            ste.table_name,
            ste.era_name
        )
        FROM sql_saga.system_time_era AS ste
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (ste.table_name, (SELECT oid FROM pg_namespace WHERE nspname = ste.table_schema))
        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = pc.oid
        WHERE t.tgname <> ste.write_history_trigger
          AND t.tgfoid = 'sql_saga.write_history()'::regprocedure
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (pc.oid, ste.write_history_trigger))
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames of the truncate_trigger.
    FOR sql IN
        SELECT format('UPDATE sql_saga.system_time_era SET truncate_trigger = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            t.tgname,
            ste.table_schema,
            ste.table_name,
            ste.era_name
        )
        FROM sql_saga.system_time_era AS ste
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (ste.table_name, (SELECT oid FROM pg_namespace WHERE nspname = ste.table_schema))
        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = pc.oid
        WHERE t.tgname <> ste.truncate_trigger
          AND t.tgfoid = 'sql_saga.truncate_system_versioning()'::regprocedure
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (pc.oid, ste.truncate_trigger))
    LOOP
        EXECUTE sql;
    END LOOP;

    /*
     * Attribute base-column changes only to registered base relations present in this ALTER TABLE
     * invocation. The persisted snapshot supplies the pre-command names without storing OIDs.
     */
    FOREACH v_table_oid IN ARRAY v_shape_changed_table_oids
    LOOP
        SELECT pg_catalog.array_agg(a.attname ORDER BY a.attnum)
        INTO v_current_columns
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = v_table_oid
          AND a.attnum > 0
          AND NOT a.attisdropped;

        SELECT pg_catalog.array_agg(s.column_name ORDER BY s.ordinality)
        INTO v_removed_columns
        FROM sql_saga.system_time_era AS ste
        CROSS JOIN LATERAL pg_catalog.unnest(ste.table_columns_snapshot) WITH ORDINALITY AS s(column_name, ordinality)
        WHERE pg_catalog.to_regclass(pg_catalog.format('%I.%I', ste.table_schema, ste.table_name)) = v_table_oid
          AND NOT s.column_name = ANY (v_current_columns);

        SELECT pg_catalog.array_agg(c.column_name ORDER BY c.ordinality)
        INTO v_added_columns
        FROM pg_catalog.unnest(v_current_columns) WITH ORDINALITY AS c(column_name, ordinality)
        JOIN sql_saga.system_time_era AS ste
          ON pg_catalog.to_regclass(pg_catalog.format('%I.%I', ste.table_schema, ste.table_name)) = v_table_oid
        WHERE NOT c.column_name = ANY (ste.table_columns_snapshot);

        SELECT removed.column_name
        INTO v_missing_excluded
        FROM pg_catalog.unnest(coalesce(v_removed_columns, ARRAY[]::name[])) AS removed(column_name)
        JOIN sql_saga.system_time_era AS ste
          ON pg_catalog.to_regclass(pg_catalog.format('%I.%I', ste.table_schema, ste.table_name)) = v_table_oid
        WHERE removed.column_name = ANY (ste.excluded_column_names)
        ORDER BY removed.column_name
        LIMIT 1;

        IF v_missing_excluded IS NOT NULL THEN
            IF cardinality(v_removed_columns) = 1 AND cardinality(v_added_columns) = 1 THEN
                RAISE EXCEPTION 'cannot drop or rename column "%" on table "%" because it is excluded from system_time era',
                    v_missing_excluded, sql_saga.__internal_display_name(v_table_oid);
            END IF;
            RAISE EXCEPTION 'cannot determine whether excluded column "%" on table "%" was renamed because ALTER TABLE changed multiple columns; split column-shape changes into separate ALTER TABLE statements',
                v_missing_excluded, sql_saga.__internal_display_name(v_table_oid);
        END IF;
    END LOOP;

    ---
    --- updatable_view
    ---
    DECLARE
        v_view_row sql_saga.updatable_view;
        v_view_oid regclass;
        v_new_trigger_name name;
    BEGIN
        FOR v_view_row IN SELECT * FROM sql_saga.updatable_view
        LOOP
            v_view_oid := to_regclass(format('%I.%I', v_view_row.view_schema /* %I */, v_view_row.view_name /* %I */));

            -- Check if the registered trigger still exists on the view
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = v_view_oid AND tgname = v_view_row.trigger_name) THEN
                -- If it's stale, find the *new* row trigger on that view that points to our function, and keep
                -- the apply trigger (1.6 for_portion_of, 1.7 current), named after the row trigger, paired with it.
                SELECT tgname INTO v_new_trigger_name FROM pg_trigger
                WHERE tgrelid = v_view_oid
                  AND tgfoid = CASE v_view_row.view_type
                      WHEN 'for_portion_of' THEN 'sql_saga.for_portion_of_trigger()'::regprocedure
                      ELSE 'sql_saga.current_view_trigger()'::regprocedure END;

                IF FOUND THEN
                    sql := format('UPDATE sql_saga.updatable_view SET trigger_name = %L WHERE (view_schema, view_name) = (%L, %L)',
                        v_new_trigger_name, /* %L */
                        v_view_row.view_schema, /* %L */
                        v_view_row.view_name /* %L */
                    );
                    EXECUTE sql;
                    IF EXISTS (SELECT FROM pg_trigger
                               WHERE tgrelid = v_view_oid AND tgname = v_view_row.trigger_name || '_apply'
                                 AND tgfoid = CASE v_view_row.view_type
                                     WHEN 'for_portion_of' THEN 'sql_saga.for_portion_of_apply_trigger()'::regprocedure
                                     ELSE 'sql_saga.current_view_apply_trigger()'::regprocedure END)
                       AND NOT EXISTS (SELECT FROM pg_trigger WHERE tgrelid = v_view_oid AND tgname = v_new_trigger_name || '_apply') THEN
                        PERFORM sql_saga.__internal_execute_as_caller(format('ALTER TRIGGER %I ON %s RENAME TO %I',
                            v_view_row.trigger_name || '_apply', v_view_oid, v_new_trigger_name || '_apply'));
                        RAISE NOTICE 'sql_saga: Renamed trigger "%" on view "%" to "%" to follow the rename of trigger "%"',
                            v_view_row.trigger_name || '_apply', sql_saga.__internal_display_name(v_view_oid),
                            v_new_trigger_name || '_apply', v_view_row.trigger_name;
                    END IF;
                END IF;
            END IF;
        END LOOP;
    END;

    -- A renamed column of a unique key referenced by a regular foreign key: attribute the dependent
    -- helper functions and referenced-side triggers before anything changes, and remember the key
    -- columns, so that they can be regenerated below. Only relations touched by this ALTER TABLE
    -- are considered; any other ALTER TABLE skips this work.
    IF is_alter_table THEN
        v_altered_table_oids := ARRAY(
            SELECT DISTINCT c.objid
            FROM pg_catalog.pg_event_trigger_ddl_commands() AS c
            WHERE c.command_tag = 'ALTER TABLE' AND c.classid = 'pg_catalog.pg_class'::regclass);
        IF EXISTS (
            SELECT FROM pg_catalog.pg_class AS c
            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
            JOIN sql_saga.unique_keys AS uk ON (uk.table_schema, uk.table_name) = (n.nspname, c.relname)
            JOIN sql_saga.foreign_keys AS fk ON fk.unique_key_name = uk.unique_key_name AND fk.type = 'regular_to_temporal'
            WHERE c.oid = ANY (v_altered_table_oids)) THEN
            v_unique_key_columns_before := sql_saga.__internal_regular_fk_key_rename_prepare(v_altered_table_oids);
        END IF;
    END IF;

    ---
    --- unique_keys
    ---

    FOR sql IN
        -- Follow renames for columns in non-XOR unique keys
        SELECT format('UPDATE sql_saga.unique_keys SET column_names = %L WHERE unique_key_name = %L',
            a.column_names, /* %L */
            uk.unique_key_name /* %L */
        )
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT oid FROM pg_namespace WHERE nspname = uk.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON (c.conrelid, c.conname) = (pc.oid, uk.unique_constraint)
        JOIN LATERAL (
            SELECT array_agg(a.attname ORDER BY u.ordinality) AS column_names
            FROM unnest(c.conkey) WITH ORDINALITY AS u (attnum, ordinality)
            JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attnum) = (pc.oid, u.attnum)
            WHERE a.attname NOT IN (e.range_column_name) -- The native constraint will include the range column
            ) AS a ON true
        WHERE uk.mutually_exclusive_columns IS NULL
          AND uk.unique_constraint IS NOT NULL
          AND uk.column_names <> a.column_names
    LOOP
        EXECUTE sql;
    END LOOP;

    /*
     * Follow renames for columns in XOR unique keys by inspecting the unified constraint.
     * column_names is stored in the order given to add_unique_key (the constraint order) and is
     * rewritten only when the SET of key column names changed (a real rename), never when only the
     * order differs. A renamed column is replaced IN PLACE, in column_names and in
     * mutually_exclusive_columns, so both keep their stored order. The old name is mapped to the new
     * one by attnum: the constraint column (by attnum) whose current name is not catalogued is the
     * renamed one, and the catalogued name no longer present on the table is its old name.
     */
    DECLARE
        v_xor_uk record;
        v_missing_names name[];
        v_unclaimed_names name[];
        v_new_column_names name[];
        v_new_xor_columns name[];
    BEGIN
        FOR v_xor_uk IN
            SELECT uk.unique_key_name, uk.table_schema, uk.table_name, uk.column_names, uk.mutually_exclusive_columns,
                   pc.oid AS table_oid,
                   (SELECT array_agg(a.attname ORDER BY u.ord)
                    FROM unnest(c.conkey) WITH ORDINALITY AS u (attnum, ord)
                    JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attnum) = (pc.oid, u.attnum)
                    WHERE a.attname <> e.range_column_name -- The native constraint includes the range column
                   ) AS constraint_column_names
            FROM sql_saga.unique_keys AS uk
            JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
            JOIN pg_catalog.pg_class AS pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT n.oid FROM pg_catalog.pg_namespace AS n WHERE n.nspname = uk.table_schema))
            JOIN pg_catalog.pg_constraint AS c ON (c.conrelid, c.conname) = (pc.oid, uk.unique_constraint)
            WHERE uk.mutually_exclusive_columns IS NOT NULL
              AND uk.unique_constraint IS NOT NULL
            ORDER BY uk.unique_key_name COLLATE "C"
        LOOP
            -- Same set of names (possibly in another order): not a rename, keep the stored order.
            CONTINUE WHEN v_xor_uk.column_names @> v_xor_uk.constraint_column_names
                      AND v_xor_uk.constraint_column_names @> v_xor_uk.column_names;

            -- Catalogued names no longer present on the table: the old names.
            v_missing_names := ARRAY(
                SELECT col.name
                FROM unnest(v_xor_uk.column_names) WITH ORDINALITY AS col (name, n)
                WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a
                                  WHERE (a.attrelid, a.attname) = (v_xor_uk.table_oid, col.name)
                                    AND a.attnum > 0 AND NOT a.attisdropped)
                ORDER BY col.n);
            -- Constraint columns (identified by attnum) whose current name is not catalogued: the new names.
            v_unclaimed_names := ARRAY(
                SELECT col.name
                FROM unnest(v_xor_uk.constraint_column_names) AS col (name)
                WHERE NOT col.name = ANY (v_xor_uk.column_names));

            IF cardinality(v_missing_names) <> 1 OR cardinality(v_unclaimed_names) <> 1 THEN
                RAISE EXCEPTION 'sql_saga: cannot follow the column rename for XOR unique key "%" on table "%.%": catalogued columns (%) no longer match the columns of its constraint (%); rename one key column per ALTER TABLE statement',
                    v_xor_uk.unique_key_name, v_xor_uk.table_schema, v_xor_uk.table_name,
                    array_to_string(v_xor_uk.column_names, ', '), array_to_string(v_xor_uk.constraint_column_names, ', ');
            END IF;

            v_new_column_names := array_replace(v_xor_uk.column_names, v_missing_names[1], v_unclaimed_names[1]);
            v_new_xor_columns := array_replace(v_xor_uk.mutually_exclusive_columns, v_missing_names[1], v_unclaimed_names[1]);

            UPDATE sql_saga.unique_keys AS uk
            SET column_names = v_new_column_names,
                mutually_exclusive_columns = v_new_xor_columns
            WHERE uk.unique_key_name = v_xor_uk.unique_key_name;
        END LOOP;
    END;

    FOR sql IN
        -- Follow renames for unique_constraint (for native temporal keys)
        SELECT format('UPDATE sql_saga.unique_keys SET unique_constraint = %L WHERE unique_key_name = %L',
            c.conname, /* %L */
            uk.unique_key_name /* %L */
        )
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT oid FROM pg_namespace WHERE nspname = uk.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        WHERE uk.unique_constraint IS NOT NULL AND uk.unique_constraint <> c.conname
          AND NOT EXISTS (SELECT FROM pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, uk.unique_constraint))
          AND pc.oid = ANY (v_touched_relation_oids)
          AND (
            -- Match traditional constraint format
            sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = (
                SELECT format('%s (%s, %I WITHOUT OVERLAPS)',
                    CASE WHEN uk.key_type = 'primary' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END,
                    string_agg(quote_ident(u.name), ', ' ORDER BY u.ordinality),
                    e.range_column_name
                )
                FROM unnest(uk.column_names) WITH ORDINALITY AS u(name, ordinality)
            )
            OR
            -- Match NULLS NOT DISTINCT constraint format
            sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = (
                SELECT format('%s (%s, %I WITHOUT OVERLAPS)',
                    CASE WHEN uk.key_type = 'primary' THEN 'PRIMARY KEY NULLS NOT DISTINCT' ELSE 'UNIQUE NULLS NOT DISTINCT' END,
                    string_agg(quote_ident(u.name), ', ' ORDER BY u.ordinality),
                    e.range_column_name
                )
                FROM unnest(uk.column_names) WITH ORDINALITY AS u(name, ordinality)
            )
          )
    LOOP
        EXECUTE sql;
    END LOOP;

    FOR sql IN
        -- Follow renames for exclude_constraint (for predicated keys)
        SELECT format('UPDATE sql_saga.unique_keys SET exclude_constraint = %L WHERE unique_key_name = %L',
            c.conname, /* %L */
            uk.unique_key_name /* %L */
        )
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT oid FROM pg_namespace WHERE nspname = uk.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        WHERE uk.exclude_constraint IS NOT NULL AND uk.exclude_constraint <> c.conname
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, uk.exclude_constraint))
          AND pc.oid = ANY (v_touched_relation_oids)
          AND sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = (
            SELECT format('EXCLUDE USING gist (%s, %I WITH &&) WHERE %s DEFERRABLE',
                string_agg(quote_ident(u.name) || ' WITH =', ', ' ORDER BY u.ordinality),
                e.range_column_name,
                CASE WHEN left(uk.predicate, 1) = '(' THEN uk.predicate ELSE '(' || uk.predicate || ')' END
            )
            FROM unnest(uk.column_names) WITH ORDINALITY AS u(name, ordinality)
          )
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames for check_constraint (for XOR keys)
    FOR sql IN
        SELECT format('UPDATE sql_saga.unique_keys SET check_constraint = %L WHERE unique_key_name = %L',
            c.conname, uk.unique_key_name
        )
        FROM sql_saga.unique_keys uk
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT oid FROM pg_namespace WHERE nspname = uk.table_schema))
        JOIN pg_constraint c ON c.conrelid = pc.oid
        WHERE uk.check_constraint IS NOT NULL
        AND uk.check_constraint <> c.conname
        AND c.contype = 'c'
        AND NOT EXISTS (SELECT FROM pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, uk.check_constraint))
        AND pc.oid = ANY (v_touched_relation_oids)
        AND sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = format('CHECK (((%s) = 1))', format('num_nonnulls(%s)', (SELECT string_agg(format('%I', col), ', ') FROM unnest(uk.mutually_exclusive_columns) as col)))
    LOOP
        EXECUTE sql;
    END LOOP;

    -- A renamed referenced key column: regenerate the dependent regular foreign key objects.
    IF v_unique_key_columns_before IS DISTINCT FROM '{}'::jsonb AND v_unique_key_columns_before IS NOT NULL THEN
        PERFORM sql_saga.__internal_regular_fk_key_rename_apply(v_unique_key_columns_before);
    END IF;

    ---
    --- foreign_keys
    ---

    DECLARE
        fk_table RECORD;
        current_columns name[];
        removed_columns name[];
        added_columns name[];
        old_col_name name;
        new_col_name name;
        fk_table_oid oid;
    BEGIN
        FOR fk_table IN
            SELECT DISTINCT table_schema, table_name
            FROM sql_saga.foreign_keys
        LOOP
            fk_table_oid := to_regclass(format('%I.%I', fk_table.table_schema /* %I */, fk_table.table_name /* %I */));

            -- If table was dropped, skip it. drop_protection will handle FK violations.
            IF fk_table_oid IS NULL THEN
                CONTINUE;
            END IF;

            -- Get current columns for this table
            SELECT array_agg(a.attname ORDER BY a.attnum) INTO current_columns
            FROM pg_catalog.pg_attribute AS a
            WHERE a.attrelid = fk_table_oid
              AND a.attnum > 0 AND NOT a.attisdropped;

            -- Find if a rename occurred by comparing current columns with the last known snapshot for this table
            DECLARE
                snapshot name[];
            BEGIN
                -- Get a snapshot from any of the FKs on this table (they are all identical for a given table)
                SELECT fk_table_columns_snapshot INTO snapshot FROM sql_saga.foreign_keys
                WHERE (table_schema, table_name) = (fk_table.table_schema, fk_table.table_name)
                LIMIT 1;

                -- If columns are unchanged, we are done with this table
                -- Set comparison: a restore can recreate the table with another column order.
                IF (SELECT array_agg(u.c ORDER BY u.c COLLATE "C") FROM unnest(snapshot) AS u(c))
                   IS NOT DISTINCT FROM
                   (SELECT array_agg(u.c ORDER BY u.c COLLATE "C") FROM unnest(current_columns) AS u(c)) THEN
                    CONTINUE;
                END IF;

                -- Find columns that were in the snapshot but are not in the current table state
                SELECT array_agg(c) INTO removed_columns
                FROM unnest(snapshot) AS u(c)
                WHERE NOT (u.c = ANY(current_columns));

                -- Find columns that are in the current table state but were not in the snapshot
                SELECT array_agg(c) INTO added_columns
                FROM unnest(current_columns) AS u(c)
                WHERE NOT (u.c = ANY(snapshot));

                -- If exactly one column was removed and one was added, we have a rename.
                IF array_length(removed_columns, 1) = 1 AND array_length(added_columns, 1) = 1 THEN
                    old_col_name := removed_columns[1];
                    new_col_name := added_columns[1];

                    -- We have a rename. PostgreSQL renames neither constraints, indexes nor triggers when a
                    -- column is renamed, so every stored object name stays unchanged; only column_names and
                    -- the snapshot follow. A regular FK's referenced-side triggers carry the referencing
                    -- column list in their arguments, so they are re-created with the same names.
                    DECLARE
                        fk_rec RECORD;
                    BEGIN
                        -- Snapshot first: the trigger DDL below re-enters this event trigger, which must
                        -- then see no pending rename.
                        UPDATE sql_saga.foreign_keys
                        SET fk_table_columns_snapshot = current_columns
                        WHERE (table_schema, table_name) = (fk_table.table_schema, fk_table.table_name);

                        FOR fk_rec IN
                            SELECT * FROM sql_saga.foreign_keys
                            WHERE (table_schema, table_name) = (fk_table.table_schema, fk_table.table_name)
                              AND old_col_name = ANY(column_names)
                            ORDER BY foreign_key_name
                        LOOP
                            /*
                             * Before the catalog follows: the referencing-side trigger (1.3) must be exactly
                             * what sql_saga generated for the columns before the rename (its arguments still
                             * name the old column), or it is a user's object and is not overwritten.
                             */
                            IF fk_rec.type = 'regular_to_temporal' AND fk_rec.fk_helper_function IS NULL
                               AND NOT sql_saga.__internal_regular_fk_child_trigger_is_canonical(fk_rec.foreign_key_name) THEN
                                RAISE EXCEPTION 'sql_saga: cannot rename a column of foreign key % on table %: the referencing-side trigger % is not the canonical sql_saga trigger',
                                    pg_catalog.quote_ident(fk_rec.foreign_key_name),
                                    pg_catalog.format('%I.%I', fk_rec.table_schema, fk_rec.table_name),
                                    pg_catalog.quote_ident(fk_rec.fk_check_constraint)
                                    USING HINT = pg_catalog.format('Confirm the intended foreign key definition, then recreate it with sql_saga.drop_foreign_key_by_name(%L, %L) and sql_saga.add_regular_foreign_key.',
                                        pg_catalog.format('%I.%I', fk_rec.table_schema, fk_rec.table_name), fk_rec.foreign_key_name);
                            END IF;
                            UPDATE sql_saga.foreign_keys
                            SET column_names = array_replace(fk_rec.column_names, old_col_name, new_col_name)
                            WHERE foreign_key_name = fk_rec.foreign_key_name;

                            IF fk_rec.type = 'regular_to_temporal' THEN
                                PERFORM sql_saga.__internal_recreate_regular_fk_uk_triggers(fk_rec.foreign_key_name);
                            END IF;
                        END LOOP;
                    END;
                END IF;

                -- Regardless of the operation (RENAME, ADD, DROP), update the snapshot
                -- to reflect the new state of the table for all FKs on that table.
                -- For DROP, drop_protection will have already fired if the column was part of an FK.
                UPDATE sql_saga.foreign_keys
                SET fk_table_columns_snapshot = current_columns
                WHERE (table_schema, table_name) = (fk_table.table_schema, fk_table.table_name);
            END;
        END LOOP;
    END;

    /*
     * Protection logic: Prevent manual renaming or dropping of managed triggers.
     * This logic is disabled for ALTER TABLE commands because any trigger changes
     * are side-effects of a column rename, which is handled above. This avoids
     * false positives caused by MVCC visibility issues within the event trigger.
     */
    IF NOT is_alter_table THEN
        -- Only on explicit ALTER TRIGGER: DROP TRIGGER is refused by drop_protection, and on any other
        -- command (for instance the CREATE POLICY of a history-policy sync while pg_restore has loaded
        -- the catalog rows but not yet created the post-data triggers) a missing trigger is not a rename.
        IF v_is_alter_trigger THEN
            FOR r IN
                SELECT fk.foreign_key_name, to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) AS table_oid, fk.uk_update_trigger AS trigger_name
                FROM sql_saga.foreign_keys AS fk
                JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
                WHERE fk.uk_update_trigger IS NOT NULL
                  AND NOT EXISTS (
                    SELECT FROM pg_catalog.pg_trigger AS t
                    WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', uk.table_schema, uk.table_name)), fk.uk_update_trigger))
                UNION ALL
                SELECT fk.foreign_key_name, to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) AS table_oid, fk.uk_delete_trigger AS trigger_name
                FROM sql_saga.foreign_keys AS fk
                JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
                WHERE fk.uk_delete_trigger IS NOT NULL
                  AND NOT EXISTS (
                    SELECT FROM pg_catalog.pg_trigger AS t
                    WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', uk.table_schema, uk.table_name)), fk.uk_delete_trigger))
                UNION ALL
                /* The TRUNCATE trigger of a regular foreign key on the referenced table (1.4). */
                SELECT fk.foreign_key_name, to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) AS table_oid, fk.uk_truncate_trigger AS trigger_name
                FROM sql_saga.foreign_keys AS fk
                JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
                WHERE fk.uk_truncate_trigger IS NOT NULL
                  AND NOT EXISTS (
                    SELECT FROM pg_catalog.pg_trigger AS t
                    WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', uk.table_schema, uk.table_name)), fk.uk_truncate_trigger))
                UNION ALL
                /* The referencing-side constraint trigger of a regular foreign key (1.3). */
                SELECT fk.foreign_key_name, to_regclass(format('%I.%I', fk.table_schema, fk.table_name)) AS table_oid, fk.fk_check_constraint AS trigger_name
                FROM sql_saga.foreign_keys AS fk
                WHERE fk.type = 'regular_to_temporal'
                  AND fk.fk_check_constraint IS NOT NULL AND fk.fk_check_constraint <> ''
                  AND to_regclass(format('%I.%I', fk.table_schema, fk.table_name)) IS NOT NULL
                  /* A pre-1.3 CHECK referencing side is not a trigger. */
                  AND NOT EXISTS (
                    SELECT FROM pg_catalog.pg_constraint AS c
                    WHERE c.conrelid = to_regclass(format('%I.%I', fk.table_schema, fk.table_name))
                      AND c.conname = fk.fk_check_constraint AND c.contype = 'c')
                  AND NOT EXISTS (
                    SELECT FROM pg_catalog.pg_trigger AS t
                    WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', fk.table_schema, fk.table_name)), fk.fk_check_constraint))
            LOOP
                RAISE EXCEPTION 'cannot drop or rename trigger "%" on table "%" because it is used in an era foreign key "%"',
                    r.trigger_name, sql_saga.__internal_display_name(r.table_oid), r.foreign_key_name;
            END LOOP;
        END IF;

        -- Protect synchronize_temporal_columns_trigger from being renamed or dropped.
        -- This trigger is essential for maintaining consistency between range and boundary columns.
        -- Only check during explicit ALTER TRIGGER commands to avoid MVCC visibility issues
        -- during other operations like ALTER TABLE OWNER or GRANT/REVOKE.
        IF v_is_alter_trigger THEN
            FOR r IN
                SELECT e.era_name, to_regclass(format('%I.%I', e.table_schema, e.table_name)) AS table_oid,
                       e.sync_temporal_trg_name, e.sync_temporal_trg_function_name
                FROM sql_saga.era AS e
                WHERE e.sync_temporal_trg_name IS NOT NULL  -- Only eras with synchronization triggers
                  AND e.sync_temporal_trg_function_name IS NOT NULL
                  AND NOT EXISTS (
                      SELECT FROM pg_catalog.pg_trigger AS t
                      WHERE t.tgrelid = to_regclass(format('%I.%I', e.table_schema, e.table_name))
                        AND t.tgname = e.sync_temporal_trg_name
                        AND t.tgfoid = to_regprocedure(format('sql_saga.%I()', e.sync_temporal_trg_function_name)))
            LOOP
                RAISE EXCEPTION 'cannot drop or rename trigger "%" on table "%" because it is managed by era "%"',
                    r.sync_temporal_trg_name, sql_saga.__internal_display_name(r.table_oid), r.era_name;
            END LOOP;
        END IF;
    END IF;

    IF cardinality(v_shape_changed_table_oids) > 0 THEN
        UPDATE sql_saga.system_time_era AS ste
        SET table_columns_snapshot = live.column_names
        FROM (
            SELECT c.oid AS table_oid,
                   pg_catalog.array_agg(a.attname ORDER BY a.attnum) AS column_names
            FROM pg_catalog.pg_class AS c
            JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.oid
            WHERE c.oid = ANY (v_shape_changed_table_oids)
              AND a.attnum > 0
              AND NOT a.attisdropped
            GROUP BY c.oid
        ) AS live
        WHERE pg_catalog.to_regclass(pg_catalog.format('%I.%I', ste.table_schema, ste.table_name)) = live.table_oid;
    END IF;

    -- ---- 1.3 A3: managed updatable views follow ADD/DROP/RENAME COLUMN (src/2o_updatable_view_column_follow.sql) ----
    /*
     * Recreate the views that __internal_updatable_view_prepare_drop_column dropped at ddl_command_start,
     * then bring the managed views of every table (and inheritance descendant) whose columns this
     * ALTER TABLE added, dropped or renamed (objsubid > 0: RENAME COLUMN) up to the base's columns, and
     * refresh their generated_columns.
     */
    IF is_alter_table THEN
        DECLARE
            v_view_shape_table_oids oid[];
        BEGIN
            v_view_shape_table_oids := ARRAY(
                WITH RECURSIVE reshaped(table_oid) AS (
                    SELECT c.objid
                    FROM pg_catalog.pg_event_trigger_ddl_commands() AS c
                    WHERE c.command_tag = 'ALTER TABLE'
                      AND c.classid = 'pg_catalog.pg_class'::regclass
                      AND (c.objsubid > 0 OR sql_saga.__internal_alter_table_changes_column_shape(c.command))
                    UNION
                    SELECT i.inhrelid
                    FROM reshaped AS rs
                    JOIN pg_catalog.pg_inherits AS i ON i.inhparent = rs.table_oid
                )
                SELECT DISTINCT rs.table_oid FROM reshaped AS rs);
            -- rename_following runs as the extension owner, the only role that can read the pending table.
            IF cardinality(v_view_shape_table_oids) > 0
               OR EXISTS (SELECT FROM sql_saga.__internal_updatable_view_pending) THEN
                PERFORM sql_saga.__internal_updatable_view_follow_shape(v_view_shape_table_oids);
            END IF;
        END;
    END IF;
    -- ---- end 1.3 A3 ----

END;
$function$;

CREATE OR REPLACE FUNCTION sql_saga.check_integrity()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = sql_saga, pg_catalog, pg_temp
AS $check_integrity$
DECLARE
    r record;
    base_oid oid;
    child_oid oid;
    child_enabled "char";
    disabled_trigger name;
    history_oid oid;
    view_oid oid;
    expected_columns name[];
    function_oid oid;
    expected_return_type oid;
    missing_column name;
    mispairing record;
    user_policy record;
BEGIN
    /*
     * sql_saga's event triggers come first: a disabled, detached or dropped one (rename_following above all) lets
     * table-level damage through, so it is reported before any damage it may have caused. Name order, COLLATE "C".
     * ENABLE ALWAYS and ENABLE REPLICA are enabled. A missing trigger cannot be attached again, so its repair
     * recreates it with the definition from the extension script (src/2j_event_triggers.sql) and then attaches it.
     * An existing trigger must also have the extension script's shape (event, tag filter as a set, function): a
     * drifted one is detached, dropped, recreated and attached again. This list is the single copy of 2j that
     * check_integrity holds, so every check_integrity on a fresh install compares it with what 2j actually created.
     */
    FOR r IN
        SELECT d.evtname, d.evtevent, d.evttags, d.evtfoid_text, et.oid AS evt_oid, et.evtenabled,
               EXISTS (SELECT 1 FROM pg_catalog.pg_depend AS dep
                       JOIN pg_catalog.pg_extension AS x ON x.oid = dep.refobjid
                       WHERE dep.classid = 'pg_catalog.pg_event_trigger'::regclass AND dep.objid = et.oid
                         AND dep.refclassid = 'pg_catalog.pg_extension'::regclass AND dep.deptype = 'e'
                         AND x.extname = 'sql_saga') AS is_member,
               (et.evtevent IS NOT DISTINCT FROM d.evtevent
                AND et.evtfoid IS NOT DISTINCT FROM pg_catalog.to_regprocedure(d.evtfoid_text || '()')::oid
                AND ARRAY(SELECT t FROM unnest(et.evttags) AS t ORDER BY t COLLATE "C")
                    = ARRAY(SELECT t FROM unnest(d.evttags) AS t ORDER BY t COLLATE "C")) AS has_canonical_shape,
               pg_catalog.format('CREATE EVENT TRIGGER %I ON %s%s EXECUTE FUNCTION %s(); ALTER EXTENSION sql_saga ADD EVENT TRIGGER %I',
                   d.evtname, d.evtevent,
                   CASE WHEN d.evttags IS NULL THEN ''
                        ELSE pg_catalog.format(' WHEN TAG IN (%s)',
                                 (SELECT pg_catalog.string_agg(pg_catalog.quote_literal(t), ', ' ORDER BY o)
                                  FROM unnest(d.evttags) WITH ORDINALITY AS u (t, o))) END,
                   d.evtfoid_text, d.evtname) AS recreate_sql
        FROM (VALUES
            ('sql_saga_drop_protection'::name, 'sql_drop'::name, NULL::text[], 'sql_saga.drop_protection'::text),
            ('sql_saga_health_checks', 'ddl_command_end', NULL, 'sql_saga.health_checks'),
            ('sql_saga_record_alter_function_resolved', 'ddl_command_start', ARRAY['ALTER FUNCTION', 'ALTER ROUTINE'],
             'sql_saga.record_alter_function_resolved'),
            ('sql_saga_refuse_managed_relation_identity_change', 'ddl_command_start', ARRAY['ALTER TABLE', 'ALTER VIEW', 'ALTER SCHEMA'],
             'sql_saga.refuse_managed_relation_identity_change'),
            ('sql_saga_rename_following', 'ddl_command_end', NULL, 'sql_saga.rename_following')
        ) AS d (evtname, evtevent, evttags, evtfoid_text)
        LEFT JOIN pg_catalog.pg_event_trigger AS et ON et.evtname = d.evtname
        ORDER BY d.evtname COLLATE "C"
    LOOP
        IF r.evt_oid IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for event trigger "%": event trigger is missing; repair with %',
                r.evtname, r.recreate_sql;
        END IF;
        IF NOT r.is_member THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for event trigger "%": event trigger is not a member of extension sql_saga; repair with ALTER EXTENSION sql_saga ADD EVENT TRIGGER %',
                r.evtname, pg_catalog.quote_ident(r.evtname);
        END IF;
        IF NOT r.has_canonical_shape THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for event trigger "%": event trigger definition differs from the extension''s; repair with %',
                r.evtname,
                pg_catalog.format('ALTER EXTENSION sql_saga DROP EVENT TRIGGER %1$I; DROP EVENT TRIGGER %1$I; %2$s',
                    r.evtname, r.recreate_sql);
        END IF;
        IF r.evtenabled = 'D' THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for event trigger "%": event trigger is disabled; repair with ALTER EVENT TRIGGER % ENABLE',
                r.evtname, pg_catalog.quote_ident(r.evtname);
        END IF;
    END LOOP;

    /*
     * A managed view dropped at ddl_command_start of an ALTER TABLE column change (DROP COLUMN or ALTER COLUMN
     * ... TYPE) is recreated, and its
     * pending row consumed, at the ddl_command_end of the same statement (src/2o). A row seen here (other
     * sessions' in-flight rows are invisible) was left behind by a statement whose end trigger did not run.
     */
    FOR r IN
        SELECT p.*,
               pg_catalog.format('DELETE FROM sql_saga.__internal_updatable_view_pending WHERE (view_schema, view_name) = (%L, %L)',
                   p.view_schema, p.view_name)
               || CASE WHEN EXISTS (SELECT FROM sql_saga.updatable_view AS uv
                                    WHERE (uv.view_schema, uv.view_name) = (p.view_schema, p.view_name)) THEN ''
                       WHEN p.view_type = 'for_portion_of' THEN
                           pg_catalog.format('; SELECT sql_saga.add_for_portion_of_view(%L, era_name => %L%s)',
                               pg_catalog.format('%I.%I', p.table_schema, p.table_name), p.era_name,
                               CASE WHEN p.ephemeral_columns IS NOT NULL
                                    THEN pg_catalog.format(', ephemeral_columns => %L', p.ephemeral_columns::text)
                                    ELSE '' END)
                       ELSE
                           pg_catalog.format('; SELECT sql_saga.add_current_view(%L, era_name => %L, delete_mode => %L, current_func_name => %L)',
                               pg_catalog.format('%I.%I', p.table_schema, p.table_name), p.era_name,
                               COALESCE(p.view_delete_mode, 'delete_as_cutoff'), p.current_func) END AS repair_sql
        FROM sql_saga.__internal_updatable_view_pending AS p
        ORDER BY p.view_schema COLLATE "C", p.view_name COLLATE "C"
    LOOP
        RAISE EXCEPTION 'sql_saga: integrity violation for updatable view "%.%" of table "%.%": view was dropped by an ALTER TABLE column change and not recreated, because event trigger sql_saga_rename_following did not run; repair with %',
            r.view_schema, r.view_name, r.table_schema, r.table_name, r.repair_sql;
    END LOOP;

    /* Base relation and era invariants are the first dependency layer. */
    FOR r IN
        SELECT e.*,
               pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))::oid AS inventory_table_oid,
               CASE
                   WHEN pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name)) IS NULL THEN 1
                   WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_class AS c
                               WHERE c.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                                 AND (c.relkind <> 'r' OR c.relpersistence <> 'p')) THEN 2
                   WHEN (e.sync_temporal_trg_function_name IS NOT NULL
                         AND e.sync_temporal_trg_function_name <> sql_saga.__internal_make_name(
                             ARRAY[e.table_schema, e.table_name, e.era_name], 'template_sync'))
                     OR EXISTS (SELECT 1 FROM sql_saga.system_time_era AS ste
                                WHERE (ste.table_schema, ste.table_name) = (e.table_schema, e.table_name)
                                  AND NOT EXISTS (SELECT 1 FROM sql_saga.era AS x
                                                  WHERE (x.table_schema, x.table_name, x.era_name) =
                                                        (ste.table_schema, ste.table_name, ste.era_name)))
                     OR EXISTS (SELECT 1 FROM sql_saga.unique_keys AS uk
                                WHERE (uk.table_schema, uk.table_name) = (e.table_schema, e.table_name)
                                  AND NOT EXISTS (SELECT 1 FROM sql_saga.era AS x
                                                  WHERE (x.table_schema, x.table_name, x.era_name) =
                                                        (uk.table_schema, uk.table_name, uk.era_name))) THEN 3
                   WHEN NOT EXISTS (
                       SELECT 1 FROM pg_catalog.pg_attribute AS a
                       JOIN pg_catalog.pg_range AS pr ON pr.rngtypid = e.range_type
                       WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                         AND a.attname = e.range_column_name AND a.attnum > 0 AND NOT a.attisdropped
                         AND a.atttypid = e.range_type AND pr.rngmultitypid = e.multirange_type
                         AND pr.rngsubtype = e.range_subtype
                         AND (SELECT t.typcategory FROM pg_catalog.pg_type AS t WHERE t.oid = e.range_subtype) = e.range_subtype_category) THEN 4
                   WHEN (e.valid_from_column_name IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_attribute AS a
                            WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND a.attname = e.valid_from_column_name AND a.atttypid = e.range_subtype
                              AND a.attnum > 0 AND NOT a.attisdropped))
                     OR (e.valid_until_column_name IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_attribute AS a
                            WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND a.attname = e.valid_until_column_name AND a.atttypid = e.range_subtype
                              AND a.attnum > 0 AND NOT a.attisdropped))
                     OR (e.bounds_check_constraint IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_constraint AS c
                            WHERE c.conrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND c.conname = e.bounds_check_constraint AND c.contype = 'c'
                              AND pg_catalog.pg_get_constraintdef(c.oid, false) LIKE '%NOT isempty(%'))
                     OR (e.boundary_check_constraint IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_constraint AS c
                            WHERE c.conrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND c.conname = e.boundary_check_constraint AND c.contype = 'c'
                              AND pg_catalog.pg_get_constraintdef(c.oid, false) LIKE '% < %'))
                     OR (e.sync_temporal_trg_name IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_trigger AS t
                            WHERE t.tgrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND t.tgname = e.sync_temporal_trg_name AND NOT t.tgisinternal AND t.tgtype = 23
                              AND t.tgfoid = pg_catalog.to_regprocedure(
                                  pg_catalog.format('sql_saga.%I()', e.sync_temporal_trg_function_name)))) THEN 5
               END AS violation
        FROM sql_saga.era AS e
        ORDER BY e.table_schema COLLATE "C", e.table_name COLLATE "C", e.era_name COLLATE "C"
    LOOP
        base_oid := r.inventory_table_oid;
        IF r.violation = 1 THEN
            /*
             * The catalogue names a table that no longer resolves: most likely renamed or moved without sql_saga
             * following (sql_saga 1.0 did not). drop_era cannot even run then; renaming or moving it back is the repair.
             */
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%.%": catalogued base table is missing; if it was renamed or moved, rename or move it back to %',
                r.table_schema, r.table_name, pg_catalog.format('%I.%I', r.table_schema, r.table_name);
        END IF;
        IF r.violation = 2 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": catalogued base relation kind or persistence is noncanonical; repair with drop_era and add_era',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF r.violation = 3 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": catalogue relationships are incoherent; repair with drop_era and add_era',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF r.violation IN (4, 5) THEN
            /*
             * A catalogued era column that no longer exists under its name was most likely renamed without
             * sql_saga following. Renaming it back is the repair, not drop_era and add_era. A column that exists
             * with the wrong type keeps the noncanonical message below.
             */
            SELECT c.column_name INTO missing_column
            FROM unnest(ARRAY[r.range_column_name, r.valid_from_column_name, r.valid_until_column_name])
                 WITH ORDINALITY AS c (column_name, position)
            WHERE c.column_name IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute AS a
                              WHERE a.attrelid = base_oid AND a.attname = c.column_name
                                AND a.attnum > 0 AND NOT a.attisdropped)
            ORDER BY c.position
            LIMIT 1;
            IF missing_column IS NOT NULL THEN
                RAISE EXCEPTION 'sql_saga: integrity violation for era "%" on table "%": column "%" is missing; if it was renamed, rename it back with ALTER TABLE % RENAME COLUMN <current name> TO %',
                    r.era_name, sql_saga.__internal_display_name(base_oid::regclass), missing_column,
                    pg_catalog.format('%I.%I', r.table_schema, r.table_name), pg_catalog.quote_ident(missing_column);
            END IF;
        END IF;
        IF r.violation = 4 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for era "%" on table "%": column or range type metadata is noncanonical; repair with drop_era and add_era',
                r.era_name, sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF r.violation = 5 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for era "%" on table "%": named check or synchronization object is noncanonical; repair with drop_era and add_era',
                r.era_name, sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
    END LOOP;

    /* SYSTEM_TIME invariants have the highest managed-object priority. */
    FOR r IN
        SELECT ste.*, e.range_column_name
        FROM sql_saga.system_time_era AS ste
        JOIN sql_saga.era AS e USING (table_schema, table_name, era_name)
        ORDER BY ste.table_schema COLLATE "C", ste.table_name COLLATE "C"
    LOOP
        base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
        IF base_oid IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%.%": catalogued base table is missing', r.table_schema, r.table_name;
        END IF;

        IF NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.generated_always_trigger)
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.write_history_trigger)
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.truncate_trigger) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": SYSTEM_TIME trigger is missing; repair with drop_system_versioning and add_system_versioning',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;

        IF NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_constraint AS c
                WHERE c.conrelid = base_oid AND c.conname = r.infinity_check_constraint
                  AND c.contype = 'c'
                  AND pg_catalog.pg_get_constraintdef(c.oid, false) LIKE '%upper(%''infinity''%')
           OR NOT sql_saga.__internal_trigger_is_attributable(
                base_oid, r.generated_always_trigger,
                'sql_saga.generated_always_as_row_start_end()'::regprocedure, 23, r.range_column_name)
           OR NOT sql_saga.__internal_trigger_is_attributable(
                base_oid, r.write_history_trigger,
                'sql_saga.write_history()'::regprocedure, 25, r.range_column_name)
           OR NOT sql_saga.__internal_trigger_is_attributable(
                base_oid, r.truncate_trigger,
                'sql_saga.truncate_system_versioning()'::regprocedure, 32, NULL) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": SYSTEM_TIME trigger definition is noncanonical; repair with drop_system_versioning and add_system_versioning',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;

        SELECT x.attname INTO missing_column
        FROM pg_catalog.unnest(r.excluded_column_names) AS x(attname)
        WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = base_oid AND a.attname = x.attname AND a.attnum > 0 AND NOT a.attisdropped)
        ORDER BY x.attname COLLATE "C" LIMIT 1;
        IF FOUND THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": excluded SYSTEM_TIME column "%" is missing; repair with set_system_time_era_excluded_columns or drop_system_versioning',
                sql_saga.__internal_display_name(base_oid::regclass), missing_column;
        END IF;

        /* The snapshot detects added and removed columns only; compare it as a set, because a
         * restore recreates inherited children with inherited columns first. */
        SELECT pg_catalog.array_agg(a.attname ORDER BY a.attname COLLATE "C") INTO expected_columns
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = base_oid AND a.attnum > 0 AND NOT a.attisdropped;
        IF expected_columns IS DISTINCT FROM (
                SELECT pg_catalog.array_agg(s.attname ORDER BY s.attname COLLATE "C")
                FROM pg_catalog.unnest(r.table_columns_snapshot) AS s(attname)) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": SYSTEM_TIME column snapshot is stale; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
    END LOOP;

    FOR r IN
        SELECT uk.*, i.table_oid, i.object_missing,
               i.unique_attributable, i.exclude_attributable, i.check_attributable
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.__internal_unique_key_inventory() AS i USING (unique_key_name)
        ORDER BY uk.table_schema COLLATE "C", uk.table_name COLLATE "C", uk.unique_key_name COLLATE "C"
    LOOP
        base_oid := r.table_oid;
        IF r.object_missing THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for unique key "%" on table "%": managed constraint or index is missing; repair with drop_unique_key and add_unique_key',
                r.unique_key_name, sql_saga.__internal_display_name(base_oid::regclass);
        END IF;

        IF NOT r.unique_attributable OR NOT r.exclude_attributable OR NOT r.check_attributable THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for unique key "%" on table "%": managed constraint or index is noncanonical; repair with drop_unique_key and add_unique_key',
                r.unique_key_name, sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
    END LOOP;

    FOR r IN
        SELECT fk.*, uk.table_schema AS uk_schema, uk.table_name AS uk_table
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        ORDER BY fk.table_schema COLLATE "C", fk.table_name COLLATE "C", fk.foreign_key_name COLLATE "C"
    LOOP
        /*
         * A foreign key whose objects pair child columns with other referenced columns than the unique key's
         * constraint order (built by sql_saga 1.0/1.1 against a sorted XOR key catalog) is reported as such, with
         * the repair, before the generic attribution checks (which it also fails).
         */
        SELECT * INTO mispairing FROM sql_saga.__internal_foreign_key_mispairing_report(r.foreign_key_name);
        IF mispairing.message IS NOT NULL THEN
            RAISE EXCEPTION USING MESSAGE = mispairing.message, HINT = mispairing.hint;
        END IF;

        IF r.type = 'temporal_to_temporal' THEN
            base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
            IF base_oid IS NULL
               OR NOT sql_saga.__internal_temporal_foreign_key_is_attributable(r.foreign_key_name) THEN
                RAISE EXCEPTION 'sql_saga: integrity violation for foreign key on table "%.%": managed constraint or trigger is noncanonical; repair with drop_foreign_key and add_foreign_key',
                    r.table_schema, r.table_name;
            END IF;
        ELSE
            base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.uk_schema, r.uk_table));
            child_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
            /*
             * 1.3: the referencing side is a constraint trigger named fk_check_constraint. A pre-1.3 row
             * (helper function, CHECK constraint) is diagnosed by the historical checks below.
             */
            IF r.fk_helper_function IS NULL AND child_oid IS NOT NULL THEN
                SELECT t.tgenabled INTO child_enabled FROM pg_catalog.pg_trigger AS t
                WHERE t.tgrelid = child_oid AND t.tgname = r.fk_check_constraint;
                IF NOT FOUND THEN
                    RAISE EXCEPTION 'sql_saga: integrity violation for foreign key %: referencing-side trigger % on table % is missing; repair with %',
                        pg_catalog.quote_ident(r.foreign_key_name), pg_catalog.quote_ident(r.fk_check_constraint),
                        pg_catalog.format('%I.%I', r.table_schema, r.table_name),
                        sql_saga.__internal_foreign_key_repair_sql(r.foreign_key_name);
                END IF;
                IF child_enabled = 'D' THEN
                    RAISE EXCEPTION 'sql_saga: integrity violation for foreign key %: referencing-side trigger % on table % is disabled; repair with ALTER TABLE % ENABLE TRIGGER %',
                        pg_catalog.quote_ident(r.foreign_key_name), pg_catalog.quote_ident(r.fk_check_constraint),
                        pg_catalog.format('%I.%I', r.table_schema, r.table_name),
                        pg_catalog.format('%I.%I', r.table_schema, r.table_name), pg_catalog.quote_ident(r.fk_check_constraint);
                END IF;
            END IF;
            /* Referenced-side triggers disabled: same style. */
            IF base_oid IS NOT NULL THEN
                SELECT t.tgname INTO disabled_trigger FROM pg_catalog.pg_trigger AS t
                WHERE t.tgrelid = base_oid AND t.tgname IN (r.uk_update_trigger, r.uk_delete_trigger, r.uk_truncate_trigger) AND t.tgenabled = 'D'
                ORDER BY t.tgname = r.uk_delete_trigger, t.tgname COLLATE "C" LIMIT 1;
                IF FOUND THEN
                    RAISE EXCEPTION 'sql_saga: integrity violation for foreign key %: referenced-side trigger % on table % is disabled; repair with ALTER TABLE % ENABLE TRIGGER %',
                        pg_catalog.quote_ident(r.foreign_key_name), pg_catalog.quote_ident(disabled_trigger),
                        pg_catalog.format('%I.%I', r.uk_schema, r.uk_table),
                        pg_catalog.format('%I.%I', r.uk_schema, r.uk_table), pg_catalog.quote_ident(disabled_trigger);
                END IF;
            END IF;
            IF base_oid IS NULL
               OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.uk_update_trigger)
               OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.uk_delete_trigger)
               OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.uk_truncate_trigger) THEN
                RAISE EXCEPTION 'sql_saga: integrity violation for foreign key on table "%.%": referenced-side helper trigger is missing; repair with drop_foreign_key and add_regular_foreign_key',
                    r.table_schema, r.table_name;
            END IF;
            IF NOT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_trigger AS t
                    WHERE t.tgrelid = base_oid AND t.tgname = r.uk_update_trigger
                      AND t.tgfoid = 'sql_saga.uk_update_check_c()'::regprocedure)
               OR NOT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_trigger AS t
                    WHERE t.tgrelid = base_oid AND t.tgname = r.uk_delete_trigger
                      AND t.tgfoid = 'sql_saga.uk_delete_check_c()'::regprocedure)
               OR NOT EXISTS (
                    /* AFTER TRUNCATE FOR EACH STATEMENT (tgtype 32), the canonical arguments, no WHEN (1.4). */
                    SELECT 1 FROM pg_catalog.pg_trigger AS t
                    WHERE t.tgrelid = base_oid AND t.tgname = r.uk_truncate_trigger
                      AND t.tgfoid = 'sql_saga.uk_truncate_check_c()'::regprocedure
                      AND t.tgtype = 32 AND t.tgqual IS NULL AND NOT t.tgisinternal
                      AND t.tgargs = sql_saga.__internal_regular_fk_trigger_args(r.foreign_key_name))
               OR (r.fk_helper_function IS NOT NULL AND NOT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_constraint AS c
                    WHERE c.conrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name))
                      AND c.conname = r.fk_check_constraint AND c.contype = 'c'))
               OR NOT sql_saga.__internal_regular_foreign_key_is_attributable(r.foreign_key_name) THEN
                RAISE EXCEPTION 'sql_saga: integrity violation for foreign key on table "%.%": managed constraint or trigger is noncanonical; repair with drop_foreign_key and add_foreign_key',
                    r.table_schema, r.table_name;
            END IF;
        END IF;
    END LOOP;

    FOR r IN
        SELECT v.* FROM sql_saga.updatable_view AS v
        ORDER BY v.table_schema COLLATE "C", v.table_name COLLATE "C", v.view_type, v.view_schema COLLATE "C", v.view_name COLLATE "C"
    LOOP
        view_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.view_schema, r.view_name));
        IF view_oid IS NULL
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = view_oid AND t.tgname = r.trigger_name) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for updatable view "%.%" of table "%.%": identifier column or trigger is missing; repair with drop_%_view and add_%_view',
                r.view_schema, r.view_name, r.table_schema, r.table_name,
                CASE r.view_type WHEN 'for_portion_of' THEN 'for_portion_of' ELSE 'current' END,
                CASE r.view_type WHEN 'for_portion_of' THEN 'for_portion_of' ELSE 'current' END;
        END IF;
        IF EXISTS (
                SELECT 1
                FROM pg_catalog.pg_attribute AS a
                WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name))
                  AND a.attnum > 0 AND NOT a.attisdropped
                  AND NOT EXISTS (
                      SELECT 1 FROM pg_catalog.pg_attribute AS va
                      WHERE va.attrelid = view_oid AND va.attname = a.attname
                        AND va.atttypid = a.atttypid AND va.attnum > 0 AND NOT va.attisdropped))
           OR NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_trigger AS t
                WHERE t.tgrelid = view_oid AND t.tgname = r.trigger_name
                  AND NOT t.tgisinternal AND t.tgtype = 93
                  AND t.tgfoid = CASE r.view_type
                      WHEN 'for_portion_of' THEN 'sql_saga.for_portion_of_trigger()'::regprocedure
                      ELSE 'sql_saga.current_view_trigger()'::regprocedure END)
           -- Each updatable view applies its queued changes with exactly one statement trigger (1.6, current 1.7).
           OR (
                SELECT pg_catalog.count(*) FROM pg_catalog.pg_trigger AS t
                WHERE t.tgrelid = view_oid AND NOT t.tgisinternal AND t.tgqual IS NULL AND t.tgnargs = 0
                  AND t.tgtype = CASE r.view_type WHEN 'for_portion_of' THEN 16 ELSE 4 END
                  AND t.tgfoid = CASE r.view_type
                      WHEN 'for_portion_of' THEN 'sql_saga.for_portion_of_apply_trigger()'::regprocedure
                      ELSE 'sql_saga.current_view_apply_trigger()'::regprocedure END) <> 1 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for updatable view "%.%" of table "%.%": identifier column or trigger definition is noncanonical; repair with drop_%_view and add_%_view',
                r.view_schema, r.view_name, r.table_schema, r.table_name,
                CASE r.view_type WHEN 'for_portion_of' THEN 'for_portion_of' ELSE 'current' END,
                CASE r.view_type WHEN 'for_portion_of' THEN 'for_portion_of' ELSE 'current' END;
        END IF;
    END LOOP;

    FOR r IN
        SELECT sv.*, a.public_view_attributable, a.gated_view_attributable
        FROM sql_saga.system_versioning AS sv
        CROSS JOIN LATERAL sql_saga.__internal_system_versioning_member_attribution(
            pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name))::oid) AS a
        ORDER BY sv.table_schema COLLATE "C", sv.table_name COLLATE "C"
    LOOP
        base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
        history_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.history_schema_name, r.history_table_name));
        view_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.view_schema_name, r.view_table_name));
        IF view_oid IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history view "%.%" is missing; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass), r.view_schema_name, r.view_table_name;
        END IF;
        IF history_oid IS NULL OR NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_class AS h
                WHERE h.oid = history_oid AND h.relkind = 'r' AND h.relpersistence = 'p') THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history family definition is noncanonical; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF EXISTS (
                SELECT 1 FROM pg_catalog.pg_class AS b JOIN pg_catalog.pg_class AS h ON h.oid = history_oid
                WHERE b.oid = base_oid AND (NOT h.relrowsecurity OR h.relforcerowsecurity <> b.relforcerowsecurity)) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history family definition is noncanonical; repair with sync_history_policies or rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF EXISTS (
                SELECT 1 FROM pg_catalog.pg_class AS b JOIN pg_catalog.pg_class AS h ON h.oid = history_oid
                WHERE b.oid = base_oid AND h.relowner <> b.relowner) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history family definition is noncanonical; repair with drop_system_versioning and add_system_versioning',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF EXISTS (
                SELECT 1 FROM pg_catalog.pg_class AS h
                WHERE h.oid = history_oid AND (
                    EXISTS (SELECT 1 FROM pg_catalog.aclexplode(coalesce(h.relacl, pg_catalog.acldefault('r', h.relowner))) AS x
                            WHERE x.grantee NOT IN (0, h.relowner) OR (x.grantee = 0 AND x.privilege_type <> 'SELECT'))
                    OR NOT EXISTS (SELECT 1 FROM pg_catalog.aclexplode(coalesce(h.relacl, pg_catalog.acldefault('r', h.relowner))) AS x
                                   WHERE x.grantee = 0 AND x.privilege_type = 'SELECT')))
           OR NOT r.public_view_attributable
           OR NOT r.gated_view_attributable THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history family definition is noncanonical; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        /*
         * A policy of the user's own on the history table: sync_history_policies refuses to remove it, so the
         * generic repair below cannot work. Name it and give the manual repair. Never touch the user's policy.
         * The first one in name order (COLLATE "C") is reported.
         */
        IF history_oid IS NOT NULL THEN
            SELECT p.polname, bn.nspname AS base_schema, b.relname AS base_name,
                   hn.nspname AS history_schema, h.relname AS history_name
              INTO user_policy
              FROM pg_catalog.pg_policy AS p
              JOIN pg_catalog.pg_class AS h ON h.oid = p.polrelid
              JOIN pg_catalog.pg_namespace AS hn ON hn.oid = h.relnamespace
              JOIN pg_catalog.pg_class AS b ON b.oid = base_oid
              JOIN pg_catalog.pg_namespace AS bn ON bn.oid = b.relnamespace
             WHERE p.polrelid = history_oid
               AND NOT sql_saga.__internal_is_managed_history_policy(p.polname)
             ORDER BY p.polname COLLATE "C"
             LIMIT 1;
            IF FOUND THEN
                RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table %: history table % has policy % not managed by sql_saga',
                    pg_catalog.format('%I.%I', user_policy.base_schema, user_policy.base_name),
                    pg_catalog.format('%I.%I', user_policy.history_schema, user_policy.history_name),
                    pg_catalog.quote_ident(user_policy.polname)
                USING HINT = pg_catalog.format(
                    'Review the policy definition, then run DROP POLICY %1$I ON %2$I.%3$I; if it is still needed, create it on %4$I.%5$I; then run SELECT sql_saga.sync_history_policies(%6$L);',
                    user_policy.polname, user_policy.history_schema, user_policy.history_name,
                    user_policy.base_schema, user_policy.base_name,
                    pg_catalog.format('%I.%I', user_policy.base_schema, user_policy.base_name));
            END IF;
        END IF;
        BEGIN
            IF history_oid IS NULL OR NOT sql_saga.__internal_history_policies_synchronized(base_oid, history_oid) THEN
                RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'policy mismatch';
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": managed history policy set is inconsistent; repair with sync_history_policies',
                sql_saga.__internal_display_name(base_oid::regclass);
        END;
    END LOOP;

    FOR r IN
        SELECT sv.table_schema, sv.table_name, sv.view_schema_name, sv.view_table_name, q.*,
               CASE q.role
                   WHEN 'as_of' THEN a.as_of_attributable
                   WHEN 'between' THEN a.between_attributable
                   WHEN 'between_symmetric' THEN a.between_symmetric_attributable
                   ELSE a.from_to_attributable
               END AS definition_attributable
        FROM sql_saga.system_versioning AS sv
        CROSS JOIN LATERAL sql_saga.__internal_system_versioning_member_attribution(
            pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name))::oid) AS a
        CROSS JOIN LATERAL sql_saga.__internal_system_versioning_query_functions(
            pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name))::oid) AS q
        ORDER BY sv.table_schema COLLATE "C", sv.table_name COLLATE "C",
                 CASE q.role WHEN 'as_of' THEN 1 WHEN 'between' THEN 2 WHEN 'between_symmetric' THEN 3 ELSE 4 END
    LOOP
        base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
        function_oid := pg_catalog.to_regprocedure(r.identity);
        IF function_oid IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%.%": generated query function role "%" is missing; repair with rebuild_system_versioning_views',
                r.table_schema, r.table_name, r.role;
        END IF;
        SELECT c.reltype INTO expected_return_type FROM pg_catalog.pg_class AS c
        WHERE c.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.view_schema_name, r.view_table_name));
        IF (SELECT p.prorettype FROM pg_catalog.pg_proc AS p WHERE p.oid = function_oid) IS DISTINCT FROM expected_return_type
           OR 1 <> (
                SELECT pg_catalog.count(*)
                FROM pg_catalog.pg_depend AS d
                WHERE d.classid = 'pg_catalog.pg_proc'::regclass
                  AND d.objid = function_oid
                  AND d.refclassid = 'pg_catalog.pg_type'::regclass
                  AND d.deptype = 'n')
           OR NOT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_depend AS d
                JOIN pg_catalog.pg_type AS t ON t.oid = d.refobjid
                WHERE d.classid = 'pg_catalog.pg_proc'::regclass
                  AND d.objid = function_oid
                  AND d.refclassid = 'pg_catalog.pg_type'::regclass
                  AND d.refobjid = expected_return_type
                  AND d.deptype = 'n'
                  AND t.typrelid = pg_catalog.to_regclass(
                      pg_catalog.format('%I.%I', r.view_schema_name, r.view_table_name))) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%.%": generated query function role "%" has a noncanonical return-type dependency; repair with rebuild_system_versioning_views',
                r.table_schema, r.table_name, r.role;
        END IF;
        IF NOT r.definition_attributable THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%.%": generated query function role "%" definition is noncanonical; repair with rebuild_system_versioning_views',
                r.table_schema, r.table_name, r.role;
        END IF;
    END LOOP;
END;
$check_integrity$;

/*
 * ddl_command_end of ALTER TABLE (rename_following): recreate the views that
 * __internal_updatable_view_prepare_drop_column dropped, then bring every managed view of the reshaped
 * tables up to the base's columns and refresh generated_columns.
 */
CREATE OR REPLACE FUNCTION sql_saga.__internal_updatable_view_follow_shape(table_oids oid[])
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $__internal_updatable_view_follow_shape$
#variable_conflict use_variable
DECLARE
    v_view record;
    v_row sql_saga.updatable_view;
    v_rename record;
    v_trigger_args text[];
    v_first_column_arg integer;
    v_base_columns text[];
    v_view_columns text[];
    v_added text;
    v_generated name[];
    v_dependent text;
    v_replaced boolean;
BEGIN
    PERFORM sql_saga.__internal_entry_guard();
    PERFORM sql_saga.__internal_updatable_view_restore_pending();

    IF table_oids IS NULL OR pg_catalog.cardinality(table_oids) = 0 THEN
        RETURN;
    END IF;

    FOR v_view IN
        SELECT uv.view_schema, uv.view_name, t.table_oid,
               pg_catalog.to_regclass(pg_catalog.format('%I.%I', uv.view_schema, uv.view_name))::oid AS view_oid
        FROM (SELECT DISTINCT u.table_oid FROM pg_catalog.unnest(table_oids) AS u(table_oid)) AS t
        JOIN pg_catalog.pg_class AS c ON c.oid = t.table_oid
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
        JOIN sql_saga.updatable_view AS uv ON (uv.table_schema, uv.table_name) = (n.nspname, c.relname)
        ORDER BY uv.view_schema COLLATE "C", uv.view_name COLLATE "C"
    LOOP
        CONTINUE WHEN v_view.view_oid IS NULL;
        SELECT uv.* INTO v_row FROM sql_saga.updatable_view AS uv
        WHERE (uv.view_schema, uv.view_name) = (v_view.view_schema, v_view.view_name);

        /*
         * for_portion_of_trigger queues sources in pg_temp.__temp_for_portion_of_<view oid> (and ..._delete)
         * until the end of the transaction. Created before this reshape, they have the old columns.
         */
        IF pg_catalog.to_regclass(pg_catalog.format('pg_temp.%I', '__temp_for_portion_of_' || v_view.view_oid)) IS NOT NULL THEN
            EXECUTE pg_catalog.format('DROP TABLE pg_temp.%I', '__temp_for_portion_of_' || v_view.view_oid);
        END IF;
        IF pg_catalog.to_regclass(pg_catalog.format('pg_temp.%I', '__temp_for_portion_of_' || v_view.view_oid || '_delete')) IS NOT NULL THEN
            EXECUTE pg_catalog.format('DROP TABLE pg_temp.%I', '__temp_for_portion_of_' || v_view.view_oid || '_delete');
        END IF;
        /* current_view_trigger (1.7) queues inserts in pg_temp.__temp_current_view_<view oid>. */
        IF pg_catalog.to_regclass(pg_catalog.format('pg_temp.%I', '__temp_current_view_' || v_view.view_oid)) IS NOT NULL THEN
            EXECUTE pg_catalog.format('DROP TABLE pg_temp.%I', '__temp_current_view_' || v_view.view_oid);
        END IF;

        SELECT pg_catalog.array_agg(pg_catalog.format('%I %s', a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)) ORDER BY a.attnum)
        INTO v_base_columns
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = v_view.table_oid AND a.attnum > 0 AND NOT a.attisdropped;

        SELECT pg_catalog.array_agg(pg_catalog.format('%I %s', a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)) ORDER BY a.attnum)
        INTO v_view_columns
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = v_view.view_oid AND a.attnum > 0 AND NOT a.attisdropped;

        /*
         * Invariant: on entry the view's columns were the base's columns, or a prefix of them (every
         * ALTER TABLE is followed here, and DROP COLUMN rebuilt the view already). So after an ALTER
         * TABLE, equal types in equal positions under some different names can only be a RENAME COLUMN;
         * more base columns with the view's as prefix an ADD COLUMN; anything else (a view found stale,
         * e.g. after the event triggers were disabled) is repaired by a drop and recreate.
         */
        IF v_view_columns IS DISTINCT FROM v_base_columns
           AND (SELECT pg_catalog.array_agg(pg_catalog.format_type(a.atttypid, a.atttypmod) ORDER BY a.attnum) FROM pg_catalog.pg_attribute AS a
                WHERE a.attrelid = v_view.view_oid AND a.attnum > 0 AND NOT a.attisdropped)
             = (SELECT pg_catalog.array_agg(pg_catalog.format_type(a.atttypid, a.atttypmod) ORDER BY a.attnum) FROM pg_catalog.pg_attribute AS a
                WHERE a.attrelid = v_view.table_oid AND a.attnum > 0 AND NOT a.attisdropped) THEN
            /*
             * The base columns were renamed. Rename the view columns in place (same OID, grants,
             * comments), then the identifier columns in the INSTEAD OF trigger's arguments (CREATE OR
             * REPLACE TRIGGER, so the managed trigger is never dropped; its arguments are the delete mode
             * for a current view, then column names only) and the view's ephemeral_columns.
             */
            FOR v_rename IN
                SELECT va.attname AS old_name, ba.attname AS new_name
                FROM (SELECT a.attname, pg_catalog.row_number() OVER (ORDER BY a.attnum) AS n
                      FROM pg_catalog.pg_attribute AS a
                      WHERE a.attrelid = v_view.view_oid AND a.attnum > 0 AND NOT a.attisdropped) AS va
                JOIN (SELECT a.attname, pg_catalog.row_number() OVER (ORDER BY a.attnum) AS n
                      FROM pg_catalog.pg_attribute AS a
                      WHERE a.attrelid = v_view.table_oid AND a.attnum > 0 AND NOT a.attisdropped) AS ba
                  ON ba.n = va.n
                WHERE va.attname <> ba.attname
                ORDER BY va.n
            LOOP
                RAISE DEBUG 'sql_saga: follow rename of column "%" to "%" in view %', v_rename.old_name, v_rename.new_name, v_view.view_oid::pg_catalog.regclass;
                PERFORM sql_saga.__internal_execute_as_caller(pg_catalog.format('ALTER VIEW %s RENAME COLUMN %I TO %I',
                    v_view.view_oid::pg_catalog.regclass, v_rename.old_name, v_rename.new_name));
                RAISE NOTICE 'sql_saga: Renamed column "%" of % view "%" to "%" to follow the rename in table "%"',
                    v_rename.old_name, v_row.view_type, sql_saga.__internal_display_name(v_view.view_oid::pg_catalog.regclass),
                    v_rename.new_name, sql_saga.__internal_display_name(v_view.table_oid::pg_catalog.regclass);

                SELECT pg_catalog.string_to_array(pg_catalog.rtrim(pg_catalog.encode(t.tgargs, 'escape'), E'\\000'), E'\\000')
                INTO v_trigger_args
                FROM pg_catalog.pg_trigger AS t
                WHERE (t.tgrelid, t.tgname) = (v_view.view_oid, v_row.trigger_name);
                -- The current view's first argument is its delete mode, not a column.
                v_first_column_arg := CASE WHEN v_row.view_type = 'current' THEN 2 ELSE 1 END;
                IF v_trigger_args IS NOT NULL
                   AND v_rename.old_name = ANY (v_trigger_args[v_first_column_arg:]) THEN
                    v_trigger_args := v_trigger_args[1:v_first_column_arg - 1]
                                      || pg_catalog.array_replace(v_trigger_args[v_first_column_arg:], v_rename.old_name::text, v_rename.new_name::text);
                    PERFORM sql_saga.__internal_execute_as_caller(pg_catalog.format('CREATE OR REPLACE TRIGGER %I INSTEAD OF INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE PROCEDURE sql_saga.%s(%s)',
                        v_row.trigger_name, v_view.view_oid::pg_catalog.regclass,
                        CASE v_row.view_type WHEN 'current' THEN 'current_view_trigger' ELSE 'for_portion_of_trigger' END,
                        (SELECT pg_catalog.string_agg(pg_catalog.quote_literal(a.arg), ', ' ORDER BY a.n)
                         FROM pg_catalog.unnest(v_trigger_args) WITH ORDINALITY AS a(arg, n))));
                    RAISE NOTICE 'sql_saga: Recreated trigger "%" on % view "%" (CREATE OR REPLACE TRIGGER) with identifier column "%" renamed to "%"',
                        v_row.trigger_name, v_row.view_type, sql_saga.__internal_display_name(v_view.view_oid::pg_catalog.regclass),
                        v_rename.old_name, v_rename.new_name;
                END IF;

                IF v_rename.old_name = ANY (v_row.ephemeral_columns) THEN
                    v_row.ephemeral_columns := pg_catalog.array_replace(v_row.ephemeral_columns, v_rename.old_name, v_rename.new_name);
                    UPDATE sql_saga.updatable_view AS uv
                    SET ephemeral_columns = v_row.ephemeral_columns
                    WHERE (uv.view_schema, uv.view_name) = (v_row.view_schema, v_row.view_name);
                END IF;
            END LOOP;
        ELSIF v_view_columns IS DISTINCT FROM v_base_columns THEN
            v_replaced := false;
            IF v_view_columns = v_base_columns[1:pg_catalog.cardinality(v_view_columns)] THEN
                -- Columns were appended: extend the view in place (same OID, grants, comments, rules,
                -- triggers), repeating its current view options, which CREATE OR REPLACE VIEW would reset.
                -- Inside another extension's script PostgreSQL refuses to replace a view that extension does not
                -- own (SQLSTATE 55000); the subtransaction is rolled back and the drop-and-recreate path below is
                -- taken instead. Any other 55000 from this statement is handled the same way: the fallback either
                -- rebuilds the view completely or refuses with its own message, so nothing is silently lost.
                BEGIN
                    PERFORM sql_saga.__internal_execute_as_caller(
                        sql_saga.__internal_updatable_view_create_sql(v_row, true,
                            (SELECT c.reloptions FROM pg_catalog.pg_class AS c WHERE c.oid = v_view.view_oid),
                            CASE WHEN v_row.view_type = 'current' THEN sql_saga.__internal_current_view_clock(v_view.view_oid) END));
                    v_replaced := true;
                EXCEPTION WHEN object_not_in_prerequisite_state THEN
                    /*
                     * Inside an extension script (CREATE EXTENSION, ALTER EXTENSION ... UPDATE) PostgreSQL
                     * refuses CREATE OR REPLACE of an object that extension does not own ("An extension is not
                     * allowed to replace an object that it does not own"). The drop and recreate below
                     * gives the same view (new OID) and keeps exactly its extension membership, so it is a
                     * correct fallback whatever raised this condition.
                     */
                    RAISE DEBUG 'sql_saga: CREATE OR REPLACE VIEW of % refused (%), recreating it instead', v_view.view_oid::pg_catalog.regclass, SQLERRM;
                END;
            END IF;
            IF v_replaced THEN
                SELECT pg_catalog.string_agg(pg_catalog.quote_ident(a.attname), ', ' ORDER BY a.attnum)
                INTO v_added
                FROM pg_catalog.pg_attribute AS a
                WHERE a.attrelid = v_view.table_oid AND a.attnum > 0 AND NOT a.attisdropped
                  -- By name and type: dropped-column holes make attnums no count of the view's columns.
                  AND NOT pg_catalog.format('%I %s', a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)) = ANY (v_view_columns);
                RAISE NOTICE 'sql_saga: Replaced % view "%" (CREATE OR REPLACE VIEW, keeping its view options) to add column(s) % of table "%"',
                    v_row.view_type, sql_saga.__internal_display_name(v_view.view_oid::pg_catalog.regclass), v_added,
                    sql_saga.__internal_display_name(v_view.table_oid::pg_catalog.regclass);
            ELSE
                -- The view's columns are no prefix of the base's, or CREATE OR REPLACE was refused inside
                -- an extension script: drop and recreate it.
                v_dependent := sql_saga.__internal_updatable_view_foreign_dependent(v_view.view_oid);
                IF v_dependent IS NOT NULL THEN
                    RAISE EXCEPTION 'cannot change the columns of table "%" because sql_saga must rebuild its managed % view "%", and % depends on that view',
                        sql_saga.__internal_display_name(v_view.table_oid::pg_catalog.regclass), v_row.view_type,
                        sql_saga.__internal_display_name(v_view.view_oid::pg_catalog.regclass), v_dependent
                        USING ERRCODE = 'dependent_objects_still_exist',
                              HINT = 'Drop the dependent object, run the ALTER TABLE, then recreate the dependent object.';
                END IF;
                PERFORM sql_saga.__internal_updatable_view_record_and_drop(v_row);
                PERFORM sql_saga.__internal_updatable_view_restore_pending();
                CONTINUE; -- restore recomputed generated_columns
            END IF;
        END IF;

        v_generated := sql_saga.__internal_updatable_view_generated_columns(v_view.table_oid);
        IF v_row.generated_columns IS DISTINCT FROM v_generated THEN
            UPDATE sql_saga.updatable_view AS uv
            SET generated_columns = v_generated
            WHERE (uv.view_schema, uv.view_name) = (v_row.view_schema, v_row.view_name);
        END IF;
    END LOOP;
END;
$__internal_updatable_view_follow_shape$;GRANT EXECUTE ON FUNCTION sql_saga.current_view_apply_trigger() TO PUBLIC;
/*
 * sql_saga 1.7 update: repairs after the 1.7 definitions are loaded (devops/generate_update_scripts_1_7.py).
 * The generator emits the final integrity gate right after this.
 *
 * Every existing current view gets its statement-level apply trigger, exactly as a fresh 1.7 add_current_view
 * creates it: AFTER INSERT FOR EACH STATEMENT, named after the view's row trigger with the suffix _apply. The
 * view, its OID, grants, comments and other triggers are unchanged. Like the for_portion_of apply triggers of the
 * 1.6 update, the trigger belongs to its view, not to the extension. A view that already has a trigger of that
 * name refuses the update, naming it.
 */
DO $create_current_view_apply_triggers$
DECLARE
    v record;
    n integer := 0;
BEGIN
    FOR v IN
        SELECT uv.view_schema, uv.view_name, uv.trigger_name,
               pg_catalog.to_regclass(pg_catalog.format('%I.%I', uv.view_schema, uv.view_name)) AS view_oid
        FROM sql_saga.updatable_view AS uv
        WHERE uv.view_type = 'current'
        ORDER BY uv.view_schema COLLATE "C", uv.view_name COLLATE "C"
    LOOP
        CONTINUE WHEN v.view_oid IS NULL; /* check_integrity reports a missing view */
        IF EXISTS (SELECT FROM pg_catalog.pg_trigger AS t
                   WHERE t.tgrelid = v.view_oid AND t.tgname = v.trigger_name || '_apply') THEN
            RAISE EXCEPTION 'sql_saga: view %.% already has a trigger named %, which sql_saga 1.7 needs for its apply trigger',
                pg_catalog.quote_ident(v.view_schema), pg_catalog.quote_ident(v.view_name),
                pg_catalog.quote_ident(v.trigger_name || '_apply')
                USING ERRCODE = 'duplicate_object',
                      HINT = 'Before updating, rename or drop that trigger, then retry ALTER EXTENSION sql_saga UPDATE.';
        END IF;
        EXECUTE pg_catalog.format('CREATE TRIGGER %I AFTER INSERT ON %s FOR EACH STATEMENT EXECUTE PROCEDURE sql_saga.current_view_apply_trigger()',
            v.trigger_name || '_apply', v.view_oid::pg_catalog.regclass);
        n := n + 1;
    END LOOP;
    IF n > 0 THEN
        RAISE NOTICE 'sql_saga: Created the apply trigger of % current views', n;
    END IF;
END;
$create_current_view_apply_triggers$;
DO $last_step_gate$
BEGIN
    IF (SELECT pg_catalog.array_agg(x::int) FROM pg_catalog.unnest(pg_catalog.string_to_array(sql_saga.native_library_version(), '.')) AS x)
       = (SELECT pg_catalog.array_agg(x::int) FROM pg_catalog.unnest(pg_catalog.string_to_array(
              (SELECT e.extversion FROM pg_catalog.pg_extension AS e WHERE e.extname = 'sql_saga'), '.')) AS x) THEN
        PERFORM sql_saga.check_integrity();
    END IF;
END;
$last_step_gate$;
