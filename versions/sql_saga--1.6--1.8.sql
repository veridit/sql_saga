\echo Use "ALTER EXTENSION sql_saga UPDATE TO '1.8'" to load this file. \quit
CREATE OR REPLACE FUNCTION sql_saga.native_library_version() RETURNS text LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT AS 'sql_saga_native', 'sql_saga_native_version_wrapper';
DO $native_guard_1_8$
BEGIN
    IF sql_saga.native_library_version() <> '1.8' THEN
        RAISE EXCEPTION 'sql_saga: native library version % does not match extension update version 1.8', sql_saga.native_library_version()
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Install the sql_saga 1.8 native library and restart PostgreSQL before retrying ALTER EXTENSION.';
    END IF;
END;
$native_guard_1_8$;
SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('sql_saga 1.8 upgrade'));


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


CREATE OR REPLACE FUNCTION sql_saga.__internal_ddl_command_affects_managed_object()
RETURNS boolean
LANGUAGE plpgsql
SECURITY INVOKER
-- Catalog rows inspected here are public; the event payload is available only in an event trigger.
SET search_path = sql_saga, pg_catalog, pg_temp
AS
$function$
DECLARE
    tags text[];
BEGIN
    SELECT array_agg(command_tag) INTO tags FROM pg_event_trigger_ddl_commands();

    -- GRANT and REVOKE never need any action: privileges on derived objects are decided at access
    -- time by the caller's rights on the base table (doc/design/history-access.md). Answer quickly.
    IF tags <@ ARRAY['GRANT', 'REVOKE', 'ALTER DEFAULT PRIVILEGES'] THEN
        RETURN false;
    END IF;

    -- ALTER TABLE needs relation normalization because its objid can name a constraint, trigger, index,
    -- or policy rather than the base relation. Unknown or empty event payloads remain conservative.
    IF 'ALTER TABLE' = ANY(tags) OR 'CREATE POLICY' = ANY(tags) OR 'ALTER POLICY' = ANY(tags) THEN
        IF NOT EXISTS (SELECT FROM pg_event_trigger_ddl_commands()) THEN
            RETURN true;
        END IF;
        IF EXISTS (SELECT FROM pg_event_trigger_ddl_commands() AS ev WHERE ev.objid = 0) THEN
            RETURN true;
        END IF;
        RETURN EXISTS (
            WITH event_relations AS (
                SELECT ev.objid AS rel_oid
                FROM pg_event_trigger_ddl_commands() AS ev
                WHERE ev.classid = 'pg_catalog.pg_class'::regclass
                UNION
                SELECT c.conrelid
                FROM pg_event_trigger_ddl_commands() AS ev
                JOIN pg_catalog.pg_constraint AS c ON c.oid = ev.objid
                WHERE ev.classid = 'pg_catalog.pg_constraint'::regclass
                UNION
                SELECT t.tgrelid
                FROM pg_event_trigger_ddl_commands() AS ev
                JOIN pg_catalog.pg_trigger AS t ON t.oid = ev.objid
                WHERE ev.classid = 'pg_catalog.pg_trigger'::regclass
                UNION
                SELECT i.indrelid
                FROM pg_event_trigger_ddl_commands() AS ev
                JOIN pg_catalog.pg_index AS i ON i.indexrelid = ev.objid
                WHERE ev.classid = 'pg_catalog.pg_class'::regclass
                UNION
                SELECT p.polrelid
                FROM pg_event_trigger_ddl_commands() AS ev
                JOIN pg_catalog.pg_policy AS p ON p.oid = ev.objid
                WHERE ev.classid = 'pg_catalog.pg_policy'::regclass
            )
            SELECT 1
            FROM event_relations AS er
            JOIN pg_catalog.pg_class AS c ON c.oid = er.rel_oid
            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
            WHERE EXISTS (SELECT 1 FROM sql_saga.era AS e
                          WHERE (e.table_schema, e.table_name) = (n.nspname, c.relname))
               OR EXISTS (SELECT 1 FROM sql_saga.system_time_era AS ste
                          WHERE (ste.table_schema, ste.table_name) = (n.nspname, c.relname))
               OR EXISTS (SELECT 1 FROM sql_saga.unique_keys AS uk
                          WHERE (uk.table_schema, uk.table_name) = (n.nspname, c.relname))
               OR EXISTS (SELECT 1 FROM sql_saga.foreign_keys AS fk
                          WHERE (fk.table_schema, fk.table_name) = (n.nspname, c.relname))
               OR EXISTS (SELECT 1 FROM sql_saga.updatable_view AS uv
                          WHERE (uv.table_schema, uv.table_name) = (n.nspname, c.relname)
                             OR (uv.view_schema, uv.view_name) = (n.nspname, c.relname))
               OR EXISTS (SELECT 1 FROM sql_saga.system_versioning AS sv
                          WHERE (sv.table_schema, sv.table_name) = (n.nspname, c.relname)
                             OR (sv.history_schema_name, sv.history_table_name) = (n.nspname, c.relname)
                             OR (sv.view_schema_name, sv.view_table_name) = (n.nspname, c.relname)
                             OR (sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name)) = (n.nspname, c.relname))
               -- An ALTER can reach managed descendants or ancestors even when this relation is unmanaged.
               OR EXISTS (SELECT 1 FROM pg_catalog.pg_inherits AS i
                          WHERE i.inhparent = er.rel_oid OR i.inhrelid = er.rel_oid)
        );
    END IF;

    -- For other commands (e.g., CREATE TABLE), we can safely use objid to
    -- check if the command affects a managed table.
    RETURN EXISTS (
        SELECT 1 FROM pg_event_trigger_ddl_commands() ddl
        JOIN pg_class c ON c.oid = ddl.objid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE EXISTS (SELECT 1 FROM sql_saga.era e WHERE (e.table_schema, e.table_name) = (n.nspname, c.relname))
           OR EXISTS (SELECT 1 FROM sql_saga.updatable_view v JOIN pg_class vc ON vc.relname = v.view_name JOIN pg_namespace vn ON vn.oid = vc.relnamespace AND vn.nspname = v.view_schema WHERE vc.oid = c.oid)
           -- Also check system_versioning objects (history table, view)
           OR EXISTS (SELECT 1 FROM sql_saga.system_versioning sv WHERE (sv.history_schema_name, sv.history_table_name) = (n.nspname, c.relname))
           OR EXISTS (SELECT 1 FROM sql_saga.system_versioning sv WHERE (sv.view_schema_name, sv.view_table_name) = (n.nspname, c.relname))
           OR EXISTS (SELECT 1 FROM sql_saga.system_versioning sv WHERE (sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name)) = (n.nspname, c.relname))
    );
END;
$function$;
CREATE OR REPLACE FUNCTION sql_saga.__api_add_era(
    table_oid regclass,
    range_column_name name,
    era_name name DEFAULT 'valid',
    synchronize_columns boolean DEFAULT true,
    valid_from_column_name name DEFAULT NULL,
    valid_until_column_name name DEFAULT NULL,
    valid_to_column_name name DEFAULT NULL,
    create_columns boolean DEFAULT false,
    add_defaults boolean DEFAULT true,
    add_bounds_check boolean DEFAULT true,
    range_type regtype DEFAULT NULL,
    bounds_check_constraint name DEFAULT NULL,
    ephemeral_columns name[] DEFAULT NULL)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, pg_temp
AS
$function$
#variable_conflict use_variable
DECLARE
    table_schema name;
    table_name name;
    kind "char";
    persistence "char";
    alter_commands text[] DEFAULT '{}';

    v_range_subtype regtype;
    v_range_subtype_category char(1);
    v_multirange_type regtype;
    v_range_col_notnull boolean;

    valid_from_attnum smallint;
    valid_from_type oid;
    valid_from_collation oid;
    valid_from_notnull boolean;

    valid_until_attnum smallint;
    valid_until_type oid;
    valid_until_collation oid;
    valid_until_notnull boolean;

    sync_col_type_oid oid;
    sync_col_is_generated text;
    trigger_name name;
    v_trigger_applies_defaults boolean := __api_add_era.add_defaults;
    boundary_check_constraint name;
BEGIN
    PERFORM sql_saga.__internal_entry_guard();
    PERFORM sql_saga.__internal_require_owner(table_oid, 'add_era');
    -- Parameter validation for explicit user input.
    IF (__api_add_era.valid_from_column_name IS NOT NULL AND __api_add_era.valid_until_column_name IS NULL) OR
       (__api_add_era.valid_from_column_name IS NULL AND __api_add_era.valid_until_column_name IS NOT NULL)
    THEN
        RAISE EXCEPTION 'valid_from_column_name and valid_until_column_name must either both be provided or both be NULL.';
    END IF;

    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    SELECT n.nspname, c.relname
    INTO table_schema, table_name
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = table_oid;

    IF era_name IS NULL THEN
        RAISE EXCEPTION 'no era name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    /* Period names are limited to lowercase alphanumeric characters for now */
    era_name := lower(era_name);
    IF era_name !~ '^[a-z_][0-9a-z_]*$' THEN
        RAISE EXCEPTION 'only alphanumeric characters are currently allowed';
    END IF;

    /* Must be a regular persistent base table. SQL:2016 11.27 SR 2 */

    SELECT c.relpersistence, c.relkind
    INTO persistence, kind
    FROM pg_catalog.pg_class AS c
    WHERE c.oid = table_oid;

    IF kind <> 'r' THEN
        /*
         * The main reason partitioned tables aren't supported yet is simply
         * because I haven't put any thought into it.
         * Maybe it's trivial, maybe not.
         */
        IF kind = 'p' THEN
            RAISE EXCEPTION 'partitioned tables are not supported yet';
        END IF;

        RAISE EXCEPTION 'relation % is not a table', $1;
    END IF;

    IF persistence <> 'p' THEN
        /* We could probably accept unlogged tables but what's the point? */
        RAISE EXCEPTION 'table "%" must be persistent', sql_saga.__internal_display_name(table_oid);
    END IF;

    /* If requested, create columns if they are missing */
    IF create_columns THEN
        DECLARE
            range_exists boolean;
            from_exists boolean;
            until_exists boolean;
            v_range_type regtype := __api_add_era.range_type;
            v_range_subtype regtype;
            add_clauses text[] := '{}';
        BEGIN
            range_exists := EXISTS(SELECT 1 FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = table_oid AND a.attname = range_column_name AND NOT a.attisdropped);

            IF NOT range_exists THEN
                IF v_range_type IS NULL THEN
                    RAISE EXCEPTION 'range_type must be specified when create_columns is true and the range column "%" does not exist.', range_column_name;
                END IF;
                add_clauses := add_clauses || format('ADD COLUMN %I %s', range_column_name, v_range_type::text);
            END IF;

            IF valid_from_column_name IS NOT NULL THEN
                SELECT r.rngsubtype INTO v_range_subtype FROM pg_catalog.pg_range r WHERE r.rngtypid = v_range_type;
                IF v_range_subtype IS NULL THEN
                    RAISE EXCEPTION 'could not determine subtype for range type %', v_range_type;
                END IF;

                from_exists := EXISTS(SELECT 1 FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = table_oid AND a.attname = valid_from_column_name AND NOT a.attisdropped);
                until_exists := EXISTS(SELECT 1 FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = table_oid AND a.attname = valid_until_column_name AND NOT a.attisdropped);

                IF NOT from_exists AND NOT until_exists THEN
                    add_clauses := add_clauses || format('ADD COLUMN %I %s', valid_from_column_name, v_range_subtype::text);
                    add_clauses := add_clauses || format('ADD COLUMN %I %s', valid_until_column_name, v_range_subtype::text);
                ELSIF from_exists <> until_exists THEN
                    RAISE EXCEPTION 'cannot create columns: one of "%", "%" exists, but not both', valid_from_column_name, valid_until_column_name;
                END IF;
            END IF;

            IF array_length(add_clauses, 1) > 0 THEN
                PERFORM sql_saga.__internal_execute_as_caller(format('ALTER TABLE %s %s', table_oid, array_to_string(add_clauses, ', ')));
            END IF;
        END;
    END IF;

    /*
     * Check if era already exists.  Actually no other application time
     * eras are allowed per spec, but we don't obey that.  We can have as
     * many application time eras as we want.
     *
     * SQL:2016 11.27 SR 5.b
     */
    IF EXISTS (SELECT FROM sql_saga.era AS p WHERE (p.table_schema, p.table_name, p.era_name) = (table_schema, table_name, era_name)) THEN
        RAISE EXCEPTION 'era for "%" already exists on table "%"', era_name, sql_saga.__internal_display_name(table_oid);
    END IF;

    /*
     * Although we are not creating a new object, the SQL standard says that
     * periods are in the same namespace as columns, so prevent that.
     *
     * SQL:2016 11.27 SR 5.c
     */
    IF EXISTS (
        SELECT FROM pg_catalog.pg_attribute AS a
        WHERE (a.attrelid, a.attname) = (table_oid, era_name))
    THEN
        RAISE EXCEPTION 'a column named "%" already exists for table "%"', era_name, sql_saga.__internal_display_name(table_oid);
    END IF;

    /*
     * The authoritative range column must exist and be of a range type.
     */
    DECLARE
        v_range_col_attnum smallint;
        v_range_col_type oid;
        v_range_typtype "char";
    BEGIN
        SELECT a.attnum, a.atttypid, a.attnotnull, t.typtype
        INTO v_range_col_attnum, v_range_col_type, v_range_col_notnull, v_range_typtype
        FROM pg_catalog.pg_attribute AS a
        JOIN pg_catalog.pg_type AS t ON t.oid = a.atttypid
        WHERE (a.attrelid, a.attname) = (table_oid, range_column_name) AND NOT a.attisdropped;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'range column "%" not found in table "%"', range_column_name, sql_saga.__internal_display_name(table_oid);
        END IF;

        IF v_range_col_attnum < 0 THEN
            RAISE EXCEPTION 'system columns cannot be used in an era';
        END IF;

        IF v_range_typtype <> 'r' THEN
            RAISE EXCEPTION 'column "%" is of type % which is not a range type', range_column_name, v_range_col_type::regtype;
        END IF;

        IF range_type IS NOT NULL AND range_type <> v_range_col_type THEN
            RAISE EXCEPTION 'provided range_type % does not match column "%" type %', range_type, range_column_name, v_range_col_type::regtype;
        ELSE
            range_type := v_range_col_type;
        END IF;

        -- Get range subtype and category for later use
        SELECT r.rngsubtype, r.rngmultitypid, t.typcategory
        INTO v_range_subtype, v_multirange_type, v_range_subtype_category
        FROM pg_catalog.pg_range r JOIN pg_catalog.pg_type t ON t.oid = r.rngsubtype
        WHERE r.rngtypid = range_type;
    END;

    -- If from/until columns are provided, validate them against the range subtype.
    IF valid_from_column_name IS NOT NULL THEN
        /* Get start column information */
        SELECT a.attnum, a.atttypid, a.attcollation, a.attnotnull
        INTO valid_from_attnum, valid_from_type, valid_from_collation, valid_from_notnull
        FROM pg_catalog.pg_attribute AS a
        WHERE (a.attrelid, a.attname) = (table_oid, valid_from_column_name);

        IF NOT FOUND THEN
            RAISE EXCEPTION 'column "%" not found in table "%"', valid_from_column_name, sql_saga.__internal_display_name(table_oid);
        END IF;

        IF valid_from_type <> v_range_subtype THEN
            RAISE EXCEPTION 'column "%" type % does not match range subtype %', valid_from_column_name, format_type(valid_from_type, null), format_type(v_range_subtype, null);
        END IF;

        /* Get end column information */
        SELECT a.attnum, a.atttypid, a.attcollation, a.attnotnull
        INTO valid_until_attnum, valid_until_type, valid_until_collation, valid_until_notnull
        FROM pg_catalog.pg_attribute AS a
        WHERE (a.attrelid, a.attname) = (table_oid, valid_until_column_name);

        IF NOT FOUND THEN
            RAISE EXCEPTION 'column "%" not found in table "%"', valid_until_column_name, sql_saga.__internal_display_name(table_oid);
        END IF;

        IF valid_until_type <> v_range_subtype THEN
            RAISE EXCEPTION 'column "%" type % does not match range subtype %', valid_until_column_name, format_type(valid_until_type, null), format_type(v_range_subtype, null);
        END IF;
    END IF;

    /*
     * Period columns must not be nullable.
     *
     * SQL:2016 11.27 SR 5.h
     */
    IF NOT v_range_col_notnull THEN
        alter_commands := alter_commands || format('ALTER COLUMN %I SET NOT NULL', range_column_name);
    END IF;

    IF valid_from_column_name IS NOT NULL THEN
        IF NOT valid_from_notnull THEN
            alter_commands := alter_commands || format('ALTER COLUMN %I SET NOT NULL', valid_from_column_name);
        END IF;
        IF NOT valid_until_notnull THEN
            -- If synchronization is disabled, we can set NOT NULL directly.
            -- Otherwise, the trigger will handle enforcing it via defaults.
            IF NOT synchronize_columns THEN
                 alter_commands := alter_commands || format('ALTER COLUMN %I SET NOT NULL', valid_until_column_name);
            END IF;
        END IF;
    END IF;


    /*
     * Find and appropriate a CHECK constraint.
     * Create one if necessary.
     * SQL:2016 11.27 GR 2.b
     */
    DECLARE
        condef text;
        context text;
        subtype_info record;
    BEGIN
        SELECT v_range_subtype_category AS typcategory, format_type(v_range_subtype, NULL) AS typname
        INTO subtype_info;

        IF add_defaults AND valid_until_column_name IS NOT NULL THEN
            IF subtype_info.typcategory = 'D' OR subtype_info.typname IN ('numeric', 'float4', 'float8') THEN
                IF synchronize_columns THEN
                    -- If synchronization is enabled, the trigger will handle defaults.
                    v_trigger_applies_defaults := true;
                ELSE
                    -- For simple eras without synchronization, set the default directly on the table.
                    alter_commands := alter_commands || format('ALTER COLUMN %I SET DEFAULT ''infinity''', valid_until_column_name);
                END IF;
            END IF;
        END IF;

        IF add_bounds_check THEN
            -- The bounds check ensures the range is not empty: CHECK (NOT isempty(range))
            condef := format('CHECK (NOT isempty(%I))', range_column_name);

            IF bounds_check_constraint IS NOT NULL THEN
                SELECT pg_catalog.pg_get_constraintdef(c.oid)
                INTO context
                FROM pg_catalog.pg_constraint AS c
                WHERE (c.conrelid, c.conname) = (table_oid, bounds_check_constraint) AND c.contype = 'c';

                IF FOUND AND context <> condef THEN
                    RAISE EXCEPTION 'constraint "%" on table "%" does not match expected CHECK (NOT isempty(%))', bounds_check_constraint, sql_saga.__internal_display_name(table_oid), range_column_name;
                ELSIF NOT FOUND THEN
                    alter_commands := alter_commands || format('ADD CONSTRAINT %I %s', bounds_check_constraint, condef);
                END IF;
            ELSE
                SELECT c.conname INTO bounds_check_constraint
                FROM pg_catalog.pg_constraint AS c
                WHERE c.conrelid = table_oid AND c.contype = 'c' AND pg_catalog.pg_get_constraintdef(c.oid) = condef;

                IF NOT FOUND THEN
                    bounds_check_constraint := sql_saga.__internal_make_name(ARRAY[table_name, era_name], 'check');
                    alter_commands := alter_commands || format('ADD CONSTRAINT %I %s', bounds_check_constraint, condef);
                END IF;
            END IF;

            -- If from/until columns exist, add the boundary check constraint.
            -- The boundary check ensures start < end for the boundary columns.
            IF valid_from_column_name IS NOT NULL THEN
                IF subtype_info.typcategory = 'D' OR subtype_info.typname IN ('numeric', 'float4', 'float8') THEN
                    condef := format('CHECK ((%I < %I) AND (%I > ''-infinity''))', valid_from_column_name, valid_until_column_name, valid_from_column_name);
                ELSE
                    condef := format('CHECK ((%I < %I))', valid_from_column_name, valid_until_column_name);
                END IF;

                -- Find existing constraint or create a new one
                SELECT c.conname INTO boundary_check_constraint
                FROM pg_catalog.pg_constraint AS c
                WHERE c.conrelid = table_oid AND c.contype = 'c' AND pg_catalog.pg_get_constraintdef(c.oid) = condef;

                IF NOT FOUND THEN
                    -- Generate a name for the boundary check constraint
                    boundary_check_constraint := sql_saga.__internal_make_name(ARRAY[table_name], 'check');
                    alter_commands := alter_commands || format('ADD CONSTRAINT %I %s', boundary_check_constraint, condef);
                END IF;
            END IF;
        END IF;
    END;

    -- Note: Unlike system_time eras, regular eras do NOT enforce valid_until = infinity.
    -- Users can have closed date ranges for regular temporal data. The infinity check
    -- constraint (SQL:2016 4.15.2.2) is only applied to system_time eras via
    -- __internal_add_system_time_era().

    /* If we've created any work for ourselves, do it now */
    IF alter_commands <> '{}' THEN
        PERFORM sql_saga.__internal_execute_as_caller(format('ALTER TABLE %s %s', table_oid /* %s */, array_to_string(alter_commands, ', ') /* %s */));
    END IF;

    -- Warn about schemas that are incompatible with temporal primary keys (SCD-2 history).
    -- These are not hard errors in add_era, as the user may not intend to use a temporal PK.
    -- The hard error is enforced in add_unique_key(key_type => 'primary').
    DECLARE
        v_pk_contains_temporal_col BOOLEAN;
        v_identity_col_name name;
    BEGIN
        -- Check if a PK exists and if it contains at least one of the temporal columns.
        SELECT EXISTS (
            SELECT 1
            FROM pg_constraint c
            JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
            WHERE c.conrelid = table_oid AND c.contype = 'p'
              AND a.attname IN (__api_add_era.range_column_name, __api_add_era.valid_from_column_name, __api_add_era.valid_until_column_name)
        ) INTO v_pk_contains_temporal_col;

        IF NOT v_pk_contains_temporal_col AND EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conrelid = table_oid AND c.contype = 'p') THEN
            RAISE WARNING 'Table "%" has a simple PRIMARY KEY that does not include temporal columns. This schema is incompatible with SCD Type 2 history.', sql_saga.__internal_display_name(table_oid)
            USING HINT = 'If you plan to use a temporal primary key for this era, you must use a composite primary key that includes a temporal column (e.g., PRIMARY KEY (id, your_range_column)).';
        END IF;

        -- Check for GENERATED ALWAYS AS IDENTITY columns
        SELECT a.attname
        INTO v_identity_col_name
        FROM pg_attribute a
        WHERE a.attrelid = table_oid
          AND a.attidentity = 'a' -- 'a' for ALWAYS
          AND NOT a.attisdropped
        LIMIT 1;

        IF v_identity_col_name IS NOT NULL THEN
            RAISE WARNING 'Table "%" has a GENERATED ALWAYS AS IDENTITY column ("%"). This schema is incompatible with SCD Type 2 history.', sql_saga.__internal_display_name(table_oid), v_identity_col_name
            USING HINT = 'If you plan to use a temporal primary key for this era, the identity column must be GENERATED BY DEFAULT AS IDENTITY to allow inserting historical records.';
        END IF;
    END;

    -- Create the unified synchronization trigger if requested.
    DECLARE
        v_from_col    name := __api_add_era.valid_from_column_name;
        v_until_col   name := __api_add_era.valid_until_column_name;
        v_to_col      name := __api_add_era.valid_to_column_name;
        v_create_template_trigger boolean := false;
        v_sync_cols_text text;
        v_trigger_name name;
    BEGIN
        IF synchronize_columns THEN
            -- Auto-detect conventional column names if they haven't been explicitly provided.
            IF v_from_col IS NULL AND
               EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid = table_oid AND attname = 'valid_from' AND NOT attisdropped) AND
               EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid = table_oid AND attname = 'valid_until' AND NOT attisdropped)
            THEN
                v_from_col := 'valid_from';
                v_until_col := 'valid_until';
            END IF;

            IF v_to_col IS NULL AND
               EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid = table_oid AND attname = 'valid_to' AND NOT attisdropped)
            THEN
                v_to_col := 'valid_to';
            END IF;
        END IF;

        -- If synchronization is enabled and any columns are to be synchronized, create the trigger.
        IF synchronize_columns AND (v_from_col IS NOT NULL OR v_to_col IS NOT NULL) THEN
            DECLARE
                sync_cols name[] := ARRAY[]::name[];
                subtype_is_discrete boolean;
            BEGIN
                -- First, check from/until columns. If either is generated, we cannot synchronize them as a pair.
                IF v_from_col IS NOT NULL THEN
                    IF EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = table_oid AND attname = v_from_col AND NOT attisdropped AND attgenerated = 's')
                    OR EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = table_oid AND attname = v_until_col AND NOT attisdropped AND attgenerated = 's')
                    THEN
                        v_from_col := NULL;
                        v_until_col := NULL;
                    END IF;
                END IF;

                SELECT (t.typcategory IN ('D', 'N') AND t.typname NOT IN ('timestamptz', 'timestamp', 'numeric'))
                INTO subtype_is_discrete
                FROM pg_type t
                WHERE t.oid = v_range_subtype;

                -- Second, validate and check valid_to column.
                IF v_to_col IS NOT NULL THEN
                    IF v_from_col IS NULL THEN
                        -- This can happen if from/until were not provided, or if they were generated.
                        -- We cannot sync `valid_to` without a non-generated `valid_from`.
                        v_to_col := NULL;
                    ELSE
                        DECLARE
                            v_to_col_type oid;
                            v_attgenerated "char";
                        BEGIN
                            SELECT atttypid, attgenerated INTO v_to_col_type, v_attgenerated FROM pg_attribute WHERE attrelid = table_oid AND attname = v_to_col AND NOT attisdropped;
                            IF v_attgenerated = 's' THEN
                               v_to_col := NULL; -- It's generated, don't sync it.
                            ELSIF NOT FOUND THEN
                                RAISE EXCEPTION 'Synchronization column "%" not found on table %.', v_to_col, table_oid;
                            ELSIF v_to_col_type <> v_range_subtype THEN
                                RAISE WARNING 'sql_saga: Synchronization column "%" on table % has an incompatible data type (%). It must match the era subtype (%). Skipping synchronization.', v_to_col, table_oid, v_to_col_type::regtype, v_range_subtype::regtype;
                                v_to_col := NULL;
                            ELSIF NOT subtype_is_discrete THEN
                                RAISE WARNING 'sql_saga: "valid_to" synchronization is only supported for discrete types (date, integer, bigint). Disabling for column "%" on non-discrete era for table %.', v_to_col, table_oid;
                                v_to_col := NULL;
                            END IF;
                        END;
                    END IF;
                END IF;

                -- Third, build the list of non-generated columns to synchronize.
                IF v_to_col IS NOT NULL THEN
                    sync_cols := sync_cols || v_to_col;
                END IF;
                IF v_from_col IS NOT NULL THEN
                    sync_cols := sync_cols || v_from_col || v_until_col;
                END IF;

                -- Finally, if there are any columns left to synchronize, mark the trigger for creation.
                IF array_length(sync_cols, 1) > 0 THEN
                    v_create_template_trigger := true;
                    v_sync_cols_text := array_to_string(sync_cols, ', ');
                    v_trigger_name := format('%s_synchronize_temporal_columns_trigger', table_name);
                END IF;
            END;
        END IF;

        INSERT INTO sql_saga.era (table_schema, table_name, era_name, range_column_name, valid_from_column_name, valid_until_column_name, valid_to_column_name, range_type, multirange_type, range_subtype, range_subtype_category, bounds_check_constraint, boundary_check_constraint, trigger_applies_defaults, ephemeral_columns, catalog_format)
        VALUES (table_schema, table_name, era_name, range_column_name, v_from_col, v_until_col, v_to_col, range_type, v_multirange_type, v_range_subtype, v_range_subtype_category, bounds_check_constraint, boundary_check_constraint, v_trigger_applies_defaults, __api_add_era.ephemeral_columns, '1.8');

        IF v_create_template_trigger THEN
            CALL sql_saga.add_synchronize_temporal_columns_trigger(table_oid, era_name);
            RAISE NOTICE 'sql_saga: Created trigger "%" on table % to synchronize columns: %', v_trigger_name, table_oid, v_sync_cols_text;
        END IF;
    END;

    -- Create performance indexes for temporal operations
    DECLARE
        v_temporal_index_name name;
        v_temporal_index_sql text;
    BEGIN
        -- Create GIST index on temporal range column for fast temporal queries
        v_temporal_index_name := format('%s_%s_gist_idx', table_name, range_column_name);
        
        -- Check if index already exists
        IF NOT EXISTS (
            SELECT 1 FROM pg_index i
            JOIN pg_class c ON c.oid = i.indexrelid
            WHERE i.indrelid = table_oid
            AND c.relname = v_temporal_index_name
        ) THEN
            v_temporal_index_sql := format('CREATE INDEX %I ON %I.%I USING GIST (%I) WITH (fillfactor = 90)',
                v_temporal_index_name, table_schema, table_name, range_column_name);
            
            EXECUTE v_temporal_index_sql;
            RAISE NOTICE 'sql_saga: Created GIST index "%" on column % for temporal_merge performance', 
                v_temporal_index_name, format('%I.%I.%I', table_schema, table_name, range_column_name);
        END IF;
    END;

    RETURN true;
END;
$function$;
CREATE OR REPLACE FUNCTION sql_saga.__internal_add_system_time_era(
    table_oid regclass,
    range_column_name name DEFAULT 'system_valid_range',
    infinity_check_constraint name DEFAULT NULL,
    generated_always_trigger name DEFAULT NULL,
    write_history_trigger name DEFAULT NULL,
    truncate_trigger name DEFAULT NULL,
    excluded_column_names name[] DEFAULT '{}')
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, pg_temp
AS
$function$
#variable_conflict use_variable
DECLARE
    era_name CONSTANT name := 'system_time';
    table_schema name;
    table_name name;
    kind "char";
    persistence "char";
    alter_commands text[] DEFAULT '{}';
    range_attnum smallint;
    range_type regtype;
    range_notnull boolean;
    excluded_column_name name;
BEGIN
    PERFORM sql_saga.__internal_entry_guard();
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;
    /* Private worker of add_system_versioning; checked here too, so it cannot act for anyone else (R1). */
    PERFORM sql_saga.__internal_require_owner(table_oid, 'add_system_versioning');

    PERFORM sql_saga.__internal_serialize(table_oid);

    SELECT n.nspname, c.relname, c.relpersistence, c.relkind
    INTO table_schema, table_name, persistence, kind
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.oid = table_oid;

    IF kind <> 'r' THEN
        IF kind = 'p' THEN
            RAISE EXCEPTION 'partitioned tables are not supported yet';
        END IF;
        RAISE EXCEPTION 'relation % is not a table', table_oid;
    END IF;

    IF persistence <> 'p' THEN
        RAISE EXCEPTION 'table "%" must be persistent', sql_saga.__internal_display_name(table_oid);
    END IF;

    IF EXISTS (SELECT FROM sql_saga.era AS e WHERE (e.table_schema, e.table_name, e.era_name) = (table_schema, table_name, era_name)) THEN
        RAISE EXCEPTION 'era for SYSTEM_TIME already exists on table "%"', sql_saga.__internal_display_name(table_oid);
    END IF;

    IF EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE (a.attrelid, a.attname) = (table_oid, era_name)) THEN
        RAISE EXCEPTION 'a column named system_time already exists for table "%"', sql_saga.__internal_display_name(table_oid);
    END IF;

    SELECT a.attnum, a.atttypid, a.attnotnull
    INTO range_attnum, range_type, range_notnull
    FROM pg_catalog.pg_attribute AS a
    WHERE (a.attrelid, a.attname) = (table_oid, range_column_name);

    IF NOT FOUND THEN
        alter_commands := alter_commands || format('ADD COLUMN %I tstzrange NOT NULL DEFAULT tstzrange(''-infinity'', ''infinity'')', range_column_name);
        range_attnum := 0;
        range_type := 'tstzrange'::regtype;
        range_notnull := true;
    END IF;
    alter_commands := alter_commands || format('ALTER COLUMN %I SET DEFAULT tstzrange(transaction_timestamp(), ''infinity'')', range_column_name);

    IF range_attnum < 0 THEN
        RAISE EXCEPTION 'system columns cannot be used as a system time era range';
    END IF;

    IF range_type <> 'tstzrange'::regtype THEN
        RAISE EXCEPTION 'system time era range column must be of type tstzrange';
    END IF;

    IF NOT range_notnull THEN
        alter_commands := alter_commands || format('ALTER COLUMN %I SET NOT NULL', range_column_name);
    END IF;

    DECLARE
        condef CONSTANT text := format('CHECK ((upper(%I) = ''infinity''::timestamp with time zone))', range_column_name);
    BEGIN
        IF infinity_check_constraint IS NULL THEN
            infinity_check_constraint := sql_saga.__internal_make_name(ARRAY[table_name, range_column_name], 'infinity_check');
        END IF;
        alter_commands := alter_commands || format('ADD CONSTRAINT %I %s', infinity_check_constraint /* %I */, condef /* %s */);
    END;

    IF alter_commands <> '{}' THEN
        PERFORM sql_saga.__internal_execute_as_caller(format('ALTER TABLE %I.%I %s', table_schema /* %I */, table_name /* %I */, array_to_string(alter_commands, ', ') /* %s */));
    END IF;
    IF range_attnum = 0 THEN
        RAISE NOTICE 'sql_saga: Added column "%" to table "%" for system time era', quote_ident(range_column_name), sql_saga.__internal_notice_name(table_schema, table_name);
    END IF;
    RAISE NOTICE 'sql_saga: Created constraint "%" on table "%" for system time era', quote_ident(infinity_check_constraint), sql_saga.__internal_notice_name(table_schema, table_name);

    generated_always_trigger := coalesce(generated_always_trigger, sql_saga.__internal_make_name(ARRAY[table_name], 'system_time_generated_always'));
    PERFORM sql_saga.__internal_execute_as_caller(format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE ON %s FOR EACH ROW EXECUTE PROCEDURE sql_saga.generated_always_as_row_start_end(%L)', generated_always_trigger, table_oid, range_column_name));
    RAISE NOTICE 'sql_saga: Created trigger "%" on table "%" for system time era', quote_ident(generated_always_trigger), sql_saga.__internal_notice_name(table_schema, table_name);

    write_history_trigger := coalesce(write_history_trigger, sql_saga.__internal_make_name(ARRAY[table_name], 'system_time_write_history'));
    PERFORM sql_saga.__internal_execute_as_caller(format('CREATE TRIGGER %I AFTER UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE PROCEDURE sql_saga.write_history(%L)', write_history_trigger, table_oid, range_column_name));
    RAISE NOTICE 'sql_saga: Created trigger "%" on table "%" for system time era', quote_ident(write_history_trigger), sql_saga.__internal_notice_name(table_schema, table_name);

    truncate_trigger := coalesce(truncate_trigger, sql_saga.__internal_make_name(ARRAY[table_name], 'truncate'));
    PERFORM sql_saga.__internal_execute_as_caller(format('CREATE TRIGGER %I AFTER TRUNCATE ON %s FOR EACH STATEMENT EXECUTE PROCEDURE sql_saga.truncate_system_versioning()', truncate_trigger /* %I */, table_oid /* %s */));
    RAISE NOTICE 'sql_saga: Created trigger "%" on table "%" for system time era', quote_ident(truncate_trigger), sql_saga.__internal_notice_name(table_schema, table_name);

    INSERT INTO sql_saga.era (table_schema, table_name, era_name, range_column_name, valid_from_column_name, valid_until_column_name, range_type, multirange_type, range_subtype, range_subtype_category, bounds_check_constraint, catalog_format)
    VALUES (table_schema, table_name, era_name, range_column_name, NULL, NULL, 'tstzrange', 'tstzmultirange', 'timestamptz', 'D', NULL, '1.8');

    INSERT INTO sql_saga.system_time_era (
        table_schema, table_name, era_name, infinity_check_constraint,
        generated_always_trigger, write_history_trigger, truncate_trigger,
        excluded_column_names, table_columns_snapshot)
    SELECT table_schema, table_name, era_name, infinity_check_constraint,
           generated_always_trigger, write_history_trigger, truncate_trigger,
           excluded_column_names,
           pg_catalog.array_agg(a.attname ORDER BY a.attnum)
    FROM pg_catalog.pg_attribute AS a
    WHERE a.attrelid = table_oid
      AND a.attnum > 0
      AND NOT a.attisdropped
    GROUP BY table_schema, table_name, era_name, infinity_check_constraint,
             generated_always_trigger, write_history_trigger, truncate_trigger,
             excluded_column_names;

    RETURN true;
END;
$function$;
CREATE OR REPLACE FUNCTION sql_saga.__internal_health_checks()
 RETURNS event_trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 -- Canonical reads use this safe path; user-object DDL uses the recorded caller and caller path.
 SET search_path = sql_saga, pg_catalog, pg_temp
AS
$function$
#variable_conflict use_variable
DECLARE
    cmd text;
    r record;
    managed_policy record;
    gated_columns text;
    expected_gate text;
    alter_function_resolved text;
    affected_base_oids oid[];
BEGIN
    PERFORM sql_saga.__internal_entry_guard();
    IF tg_tag IN ('ALTER FUNCTION', 'ALTER ROUTINE') THEN
        alter_function_resolved := current_setting('sql_saga.alter_function_resolved', true);
        /* Consume the statement snapshot before doing any comparison or raising. */
        PERFORM set_config('sql_saga.alter_function_resolved', '', true);

        IF alter_function_resolved IS NULL OR alter_function_resolved = '' THEN
            RETURN;
        END IF;

        FOR r IN
            SELECT t.oid AS table_oid,
                   x.identity
            FROM jsonb_to_recordset(alter_function_resolved::jsonb) AS x(
                table_schema name,
                table_name name,
                role text,
                identity text)
            LEFT JOIN pg_catalog.pg_namespace AS tn ON tn.nspname = x.table_schema
            LEFT JOIN pg_catalog.pg_class AS t ON t.relnamespace = tn.oid AND t.relname = x.table_name
            WHERE to_regprocedure(x.identity) IS NULL
            ORDER BY x.table_schema, x.table_name, x.role
        LOOP
            RAISE EXCEPTION 'cannot drop or rename function "%" because it is used in SYSTEM VERSIONING for table "%"',
                sql_saga.__internal_display_function_name(r.identity), sql_saga.__internal_display_name(r.table_oid);
        END LOOP;
    END IF;

    /* sql_drop protects exact managed members before DROP reaches ddl_command_end. */
    IF tg_tag LIKE 'DROP %' THEN
        RETURN;
    END IF;

    -- Exit early if the DDL command does not affect a managed object.
    IF NOT sql_saga.__internal_ddl_command_affects_managed_object() THEN
        RETURN;
    END IF;

    /* Ownership repair is an event-local side effect. ALTER TABLE can report a relation, constraint,
     * trigger or index depending on its subcommand, so normalize all of those to their base relation.
     * Unknown objects deliberately produce no repair candidates; never fall back to every family. */
    WITH event_relations AS (
        SELECT ev.objid AS rel_oid
        FROM pg_catalog.pg_event_trigger_ddl_commands() AS ev
        WHERE ev.classid = 'pg_catalog.pg_class'::regclass
        UNION
        SELECT c.conrelid
        FROM pg_catalog.pg_event_trigger_ddl_commands() AS ev
        JOIN pg_catalog.pg_constraint AS c ON c.oid = ev.objid
        WHERE ev.classid = 'pg_catalog.pg_constraint'::regclass
        UNION
        SELECT t.tgrelid
        FROM pg_catalog.pg_event_trigger_ddl_commands() AS ev
        JOIN pg_catalog.pg_trigger AS t ON t.oid = ev.objid
        WHERE ev.classid = 'pg_catalog.pg_trigger'::regclass
        UNION
        SELECT p.polrelid
        FROM pg_catalog.pg_event_trigger_ddl_commands() AS ev
        JOIN pg_catalog.pg_policy AS p ON p.oid = ev.objid
        WHERE ev.classid = 'pg_catalog.pg_policy'::regclass
        UNION
        SELECT i.indrelid
        FROM pg_catalog.pg_event_trigger_ddl_commands() AS ev
        JOIN pg_catalog.pg_index AS i ON i.indexrelid = ev.objid
        WHERE ev.classid = 'pg_catalog.pg_class'::regclass
    ), affected AS (
        SELECT b.oid AS base_oid
        FROM event_relations AS er
        JOIN pg_catalog.pg_class AS rel ON rel.oid = er.rel_oid
        JOIN pg_catalog.pg_namespace AS rn ON rn.oid = rel.relnamespace
        JOIN sql_saga.system_versioning AS sv
          ON (sv.table_schema, sv.table_name) = (rn.nspname, rel.relname)
          OR (sv.history_schema_name, sv.history_table_name) = (rn.nspname, rel.relname)
          OR (sv.view_schema_name, sv.view_table_name) = (rn.nspname, rel.relname)
          OR (sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name)) = (rn.nspname, rel.relname)
        JOIN pg_catalog.pg_namespace AS bn ON bn.nspname = sv.table_schema
        JOIN pg_catalog.pg_class AS b ON b.relnamespace = bn.oid AND b.relname = sv.table_name
        UNION
        SELECT b.oid
        FROM event_relations AS er
        JOIN pg_catalog.pg_class AS rel ON rel.oid = er.rel_oid
        JOIN pg_catalog.pg_namespace AS rn ON rn.oid = rel.relnamespace
        JOIN sql_saga.updatable_view AS uv
          ON (uv.table_schema, uv.table_name) = (rn.nspname, rel.relname)
          OR (uv.view_schema, uv.view_name) = (rn.nspname, rel.relname)
        JOIN pg_catalog.pg_namespace AS bn ON bn.nspname = uv.table_schema
        JOIN pg_catalog.pg_class AS b ON b.relnamespace = bn.oid AND b.relname = uv.table_name
        UNION
        SELECT b.oid
        FROM pg_catalog.pg_event_trigger_ddl_commands() AS ev
        JOIN pg_catalog.pg_proc AS p ON p.oid = ev.objid
        JOIN sql_saga.system_versioning AS sv
          ON p.oid IN (pg_catalog.to_regprocedure(sv.func_as_of), pg_catalog.to_regprocedure(sv.func_between),
                       pg_catalog.to_regprocedure(sv.func_between_symmetric), pg_catalog.to_regprocedure(sv.func_from_to))
        JOIN pg_catalog.pg_namespace AS bn ON bn.nspname = sv.table_schema
        JOIN pg_catalog.pg_class AS b ON b.relnamespace = bn.oid AND b.relname = sv.table_name
        WHERE ev.classid = 'pg_catalog.pg_proc'::regclass
    )
    SELECT coalesce(array_agg(a.base_oid), '{}'::oid[])
    INTO affected_base_oids
    FROM affected AS a;

    /* Make sure that all of our tables are still persistent */
    FOR r IN
        SELECT c.oid AS table_oid
        FROM sql_saga.era AS e
        JOIN pg_catalog.pg_class AS c ON c.relname = e.table_name
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace AND n.nspname = e.table_schema
        WHERE c.relpersistence <> 'p'
    LOOP
        RAISE EXCEPTION 'table "%" must remain persistent because it has an era',
            sql_saga.__internal_display_name(r.table_oid);
    END LOOP;

    /* And the history tables of system-versioned tables, which must keep every change */
    FOR r IN
        SELECT h.oid AS history_oid,
               pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name)) AS base_oid
        FROM sql_saga.system_versioning AS sv
        JOIN pg_catalog.pg_namespace AS hn ON hn.nspname = sv.history_schema_name
        JOIN pg_catalog.pg_class AS h ON h.relnamespace = hn.oid AND h.relname = sv.history_table_name
        WHERE h.relpersistence <> 'p'
    LOOP
        RAISE EXCEPTION 'history table "%" must remain persistent because it is used in SYSTEM VERSIONING for table "%"',
            sql_saga.__internal_display_name(r.history_oid::regclass), sql_saga.__internal_display_name(r.base_oid)
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = pg_catalog.format('Run: ALTER TABLE %s SET LOGGED;', r.history_oid::regclass);
    END LOOP;

    /* Fix up history and for-portion objects ownership */
    FOR cmd IN
        /* Fix ownership of history tables */
        SELECT format('ALTER TABLE %I.%I OWNER TO %I',
            sv.history_schema_name /* %I */, sv.history_table_name /* %I */, t.relowner::regrole /* %I */)
        FROM sql_saga.system_versioning AS sv
        JOIN pg_class AS t ON t.relname = sv.table_name
        JOIN pg_namespace AS tn ON tn.oid = t.relnamespace AND tn.nspname = sv.table_schema
        JOIN pg_class AS ht ON ht.relname = sv.history_table_name
        JOIN pg_namespace AS htn ON htn.oid = ht.relnamespace AND htn.nspname = sv.history_schema_name
        WHERE t.oid = ANY (affected_base_oids) AND NOT sql_saga.__internal_rebuilding_system_versioning_views(t.oid) AND t.relowner <> ht.relowner

        UNION ALL

        /* Fix ownership of system versioning views */
        SELECT format('ALTER VIEW %I.%I OWNER TO %I',
            sv.view_schema_name /* %I */, sv.view_table_name /* %I */, t.relowner::regrole /* %I */)
        FROM sql_saga.system_versioning AS sv
        JOIN pg_class AS t ON t.relname = sv.table_name
        JOIN pg_namespace AS tn ON tn.oid = t.relnamespace AND tn.nspname = sv.table_schema
        JOIN pg_class AS vt ON vt.relname = sv.view_table_name
        JOIN pg_namespace AS vtn ON vtn.oid = vt.relnamespace AND vtn.nspname = sv.view_schema_name
        WHERE t.oid = ANY (affected_base_oids) AND NOT sql_saga.__internal_rebuilding_system_versioning_views(t.oid) AND t.relowner <> vt.relowner

        UNION ALL

        /* Fix ownership of the gated history views */
        SELECT format('ALTER VIEW %s OWNER TO %I', f.gated_oid::regclass, t.relowner::regrole)
        FROM sql_saga.system_versioning AS sv
        JOIN pg_class AS t ON t.relname = sv.table_name
        JOIN pg_namespace AS tn ON tn.oid = t.relnamespace AND tn.nspname = sv.table_schema
        CROSS JOIN LATERAL sql_saga.__internal_history_family(t.oid) AS f
        JOIN pg_class AS gv ON gv.oid = f.gated_oid
        WHERE f.base_oid = t.oid AND t.oid = ANY (affected_base_oids) AND NOT sql_saga.__internal_rebuilding_system_versioning_views(t.oid) AND t.relowner <> gv.relowner

        UNION ALL

        /* Fix ownership of for-portion-of views */
        SELECT format('ALTER VIEW %I.%I OWNER TO %I', v.view_schema /* %I */, v.view_name /* %I */, t.relowner::regrole /* %I */)
        FROM sql_saga.updatable_view v
        JOIN pg_class t ON t.relname = v.table_name
        JOIN pg_namespace tn ON tn.oid = t.relnamespace AND tn.nspname = v.table_schema
        JOIN pg_class vt ON vt.relname = v.view_name
        JOIN pg_namespace vn ON vn.oid = vt.relnamespace AND vn.nspname = v.view_schema
        WHERE t.oid = ANY (affected_base_oids) AND NOT sql_saga.__internal_rebuilding_system_versioning_views(t.oid) AND t.relowner <> vt.relowner

        UNION ALL

        /* Fix ownership of system versioning functions */
        SELECT format('ALTER FUNCTION %s OWNER TO %I', p.oid::regprocedure, t.relowner::regrole)
        FROM sql_saga.system_versioning AS sv
        JOIN pg_class AS t ON t.relname = sv.table_name
        JOIN pg_namespace AS tn ON tn.oid = t.relnamespace AND tn.nspname = sv.table_schema
        JOIN pg_proc AS p ON p.oid IN (to_regprocedure(sv.func_as_of), to_regprocedure(sv.func_between),
                                       to_regprocedure(sv.func_between_symmetric), to_regprocedure(sv.func_from_to))
        WHERE t.oid = ANY (affected_base_oids) AND NOT sql_saga.__internal_rebuilding_system_versioning_views(t.oid) AND t.relowner <> p.proowner
    LOOP
        PERFORM sql_saga.__internal_execute_as_caller(cmd);
    END LOOP;

    /*
     * Row-level security of history tables (doc/design/history-access.md).
     * Policy DDL on a versioned base table (or its history), and ENABLE/DISABLE/FORCE/NO FORCE on
     * either, re-synchronizes that family's history policies. Direct changes to the policies
     * sql_saga manages, and to the history's RLS settings, are refused. Only the synchronizer's own
     * nested DDL is exempt, recognized by its private guard row
     * (sql_saga.__internal_history_policy_sync_active), never by a setting a caller could forge.
     */
    FOR r IN
        SELECT DISTINCT f.base_oid, f.history_oid, ev.command_tag, ev.objid, ev.classid
        FROM pg_event_trigger_ddl_commands() AS ev
        LEFT JOIN pg_catalog.pg_policy AS pol
               ON ev.classid = 'pg_catalog.pg_policy'::regclass AND pol.oid = ev.objid
        CROSS JOIN LATERAL sql_saga.__internal_history_family(
            CASE WHEN ev.classid = 'pg_catalog.pg_policy'::regclass THEN pol.polrelid ELSE ev.objid END) AS f
        WHERE ev.command_tag IN ('CREATE POLICY', 'ALTER POLICY', 'ALTER TABLE')
    LOOP
        IF NOT sql_saga.__internal_history_policy_sync_active(r.base_oid) THEN
            -- Refuse direct DDL on managed history policies.
            -- CREATE POLICY of a managed name is what pg_restore does when it replays a dump: the sync
            -- below then keeps it (current installation) or drops it (another installation's token).
            IF r.classid = 'pg_catalog.pg_policy'::regclass THEN
                SELECT p.polname, p.polrelid INTO managed_policy
                FROM pg_catalog.pg_policy AS p WHERE p.oid = r.objid;
                IF managed_policy.polrelid = r.history_oid THEN
                    IF NOT sql_saga.__internal_is_managed_history_policy(managed_policy.polname) THEN
                        -- A policy of one's own on the history table (decision 7): it would be OR-ed
                        -- with the copies and could show more history than the base table allows.
                        -- ALTER POLICY ... RENAME TO a non-managed name lands here too.
                        RAISE EXCEPTION 'sql_saga: policies on history table "%" are managed by sql_saga', sql_saga.__internal_display_name(r.history_oid::regclass)
                            USING HINT = format('Create the policy on "%s"; sql_saga copies its SELECT and ALL policies to the history.',
                                                sql_saga.__internal_display_name(r.base_oid::regclass));
                    ELSIF r.command_tag = 'ALTER POLICY' THEN
                        RAISE EXCEPTION 'cannot drop or alter policy "%" on "%" because it is managed by sql_saga for table "%"',
                            managed_policy.polname, sql_saga.__internal_display_name(r.history_oid::regclass), sql_saga.__internal_display_name(r.base_oid::regclass);
                    END IF;
                END IF;
            END IF;
            -- Refuse direct changes to the history's RLS settings.
            IF r.command_tag = 'ALTER TABLE' AND r.objid = r.history_oid
               AND NOT EXISTS (SELECT FROM pg_catalog.pg_class AS h, pg_catalog.pg_class AS b
                               WHERE h.oid = r.history_oid AND b.oid = r.base_oid
                                 AND h.relrowsecurity AND h.relforcerowsecurity = b.relforcerowsecurity) THEN
                RAISE EXCEPTION 'history table "%" must keep row level security enabled, and forced exactly when its base table is, because it is used in SYSTEM VERSIONING for table "%"',
                    sql_saga.__internal_display_name(r.history_oid::regclass), sql_saga.__internal_display_name(r.base_oid::regclass);
            END IF;
        END IF;
        -- Not during the teardown by pg_restore --clean (its ALTER TABLE .. DROP CONSTRAINT on the base
        -- would recreate the history policies drop_protection has just dropped for the teardown).
        -- Not in a declared parallel pg_restore either: the sync locks both tables of the family, which
        -- other workers may hold (deadlock); sync_all_history_policies() runs it after the restore.
        IF NOT sql_saga.__internal_history_policy_sync_active(r.base_oid)
           AND NOT sql_saga.__internal_is_clean_teardown()
           AND NOT sql_saga.__internal_is_parallel_restore()
           AND (r.objid <> r.history_oid OR r.classid = 'pg_catalog.pg_policy'::regclass) THEN
            PERFORM sql_saga.__internal_sync_history_policies(r.base_oid);
        END IF;
    END LOOP;

    /* The fixed shape of views sql_saga manages. Resolve catalog names through pg_class joins, not regclass
     * casts: an invoker event trigger may inspect a family whose schema the DDL caller cannot use. */
    FOR r IN
        SELECT ev.objid AS view_oid, v.base_oid, v.kind
        FROM pg_catalog.pg_event_trigger_ddl_commands() AS ev
        JOIN (
            SELECT vv.oid AS view_oid, b.oid AS base_oid, 'SYSTEM VERSIONING'::text AS kind
            FROM sql_saga.system_versioning AS sv
            JOIN pg_catalog.pg_namespace AS vn ON vn.nspname = sv.view_schema_name
            JOIN pg_catalog.pg_class AS vv ON vv.relnamespace = vn.oid AND vv.relname = sv.view_table_name
            JOIN pg_catalog.pg_namespace AS bn ON bn.nspname = sv.table_schema
            JOIN pg_catalog.pg_class AS b ON b.relnamespace = bn.oid AND b.relname = sv.table_name
            UNION ALL
            SELECT vv.oid, b.oid,
                   CASE uv.view_type WHEN 'for_portion_of' THEN 'FOR PORTION OF' ELSE 'a current view' END
            FROM sql_saga.updatable_view AS uv
            JOIN pg_catalog.pg_namespace AS vn ON vn.nspname = uv.view_schema
            JOIN pg_catalog.pg_class AS vv ON vv.relnamespace = vn.oid AND vv.relname = uv.view_name
            JOIN pg_catalog.pg_namespace AS bn ON bn.nspname = uv.table_schema
            JOIN pg_catalog.pg_class AS b ON b.relnamespace = bn.oid AND b.relname = uv.table_name
        ) AS v ON v.view_oid = ev.objid
        WHERE ev.command_tag IN ('ALTER VIEW', 'CREATE VIEW', 'ALTER TABLE')
    LOOP
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_class AS c
                       WHERE c.oid = r.view_oid AND 'security_invoker=on' = ANY (c.reloptions)) THEN
            RAISE EXCEPTION 'view "%" must keep security_invoker because it is used in % for table "%"',
                sql_saga.__internal_display_name(r.view_oid::regclass), r.kind, sql_saga.__internal_display_name(r.base_oid::regclass);
        END IF;
    END LOOP;

    FOR r IN
        SELECT ev.objid AS view_oid, f.base_oid, f.history_oid
        FROM pg_catalog.pg_event_trigger_ddl_commands() AS ev
        JOIN sql_saga.system_versioning AS sv ON true
        JOIN pg_catalog.pg_namespace AS hn ON hn.nspname = sv.history_schema_name
        JOIN pg_catalog.pg_class AS gv
          ON gv.relnamespace = hn.oid
         AND gv.relname = sql_saga.__internal_history_gated_name(sv.history_table_name)
         AND gv.oid = ev.objid
        JOIN pg_catalog.pg_namespace AS bn ON bn.nspname = sv.table_schema
        JOIN pg_catalog.pg_class AS b ON b.relnamespace = bn.oid AND b.relname = sv.table_name
        CROSS JOIN LATERAL sql_saga.__internal_history_family(b.oid) AS f
        WHERE ev.command_tag IN ('ALTER VIEW', 'CREATE VIEW', 'ALTER TABLE')
    LOOP
        -- The gate must be exactly the generated definition (owner-rights, same body), not just
        -- contain its parts: e.g. "WHERE true OR (gate)" keeps every function reference but opens
        -- the view. The canonical body is rendered from the view's own column list, so the check
        -- holds whatever columns the history has, and both are deparsed the same way.
        gated_columns := (SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum)
                          FROM pg_catalog.pg_attribute AS a
                          WHERE a.attrelid = r.view_oid AND a.attnum > 0 AND NOT a.attisdropped);
        EXECUTE 'CREATE TEMPORARY VIEW sql_saga_expected_gate AS '
            || sql_saga.__internal_history_gated_view_sql(r.base_oid, r.history_oid, gated_columns);
        expected_gate := pg_catalog.pg_get_viewdef('pg_temp.sql_saga_expected_gate'::regclass);
        DROP VIEW pg_temp.sql_saga_expected_gate;
        IF EXISTS (SELECT FROM pg_catalog.pg_class AS c WHERE c.oid = r.view_oid AND c.reloptions IS NOT NULL)
           OR pg_catalog.pg_get_viewdef(r.view_oid) IS DISTINCT FROM expected_gate THEN
            RAISE EXCEPTION 'view "%" must keep its access gate because it is used in SYSTEM VERSIONING for table "%"',
                sql_saga.__internal_display_name(r.view_oid::regclass), sql_saga.__internal_display_name(r.base_oid::regclass);
        END IF;
    END LOOP;

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
                WHERE h.oid = history_oid AND h.relkind = 'r') THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history family definition is noncanonical; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_class AS h WHERE h.oid = history_oid AND h.relpersistence <> 'p') THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history table "%" is not persistent; repair with ALTER TABLE % SET LOGGED',
                sql_saga.__internal_display_name(base_oid::regclass), sql_saga.__internal_display_name(history_oid::regclass),
                history_oid::regclass;
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

-- Unified Planning Function
CREATE OR REPLACE FUNCTION sql_saga.temporal_merge_plan(
    target_table regclass,
    source_table regclass,
    mode sql_saga.temporal_merge_mode,
    era_name name,
    identity_columns TEXT[] DEFAULT NULL,
    row_id_column name DEFAULT 'row_id',
    founding_id_column name DEFAULT NULL,
    delete_mode sql_saga.temporal_merge_delete_mode DEFAULT 'NONE',
    lookup_keys JSONB DEFAULT NULL,
    ephemeral_columns TEXT[] DEFAULT NULL,
    p_log_trace BOOLEAN DEFAULT false,
    p_log_sql BOOLEAN DEFAULT false
) RETURNS SETOF sql_saga.temporal_merge_plan
LANGUAGE plpgsql VOLATILE AS $temporal_merge_plan$
DECLARE
    v_log_vars BOOLEAN;
    v_log_id TEXT;
    --
    -- Phase 1: Introspection & Canonical Lists
    -- These variables are populated in the first block of the procedure. They form
    -- the single source of truth for all column lists used in dynamic SQL.
    -- See `temporal_merge` procedure for a detailed explanation of Identity vs. Lookup keys.
    --
    -- The definitive identity columns (partitioning key) for this merge operation.
    v_identity_columns TEXT[];
    -- Purpose: Stores the columns of the *first* lookup key found in the `lookup_keys` JSONB array.
    -- This is primarily used for generating simple, user-friendly performance hints about indexing.
    -- Semantics: A `text[]` containing the column names of the first key (e.g., `{'email'}`).
    v_representative_lookup_key TEXT[];
    -- Purpose: Stores a flattened, distinct list of all columns from *all* lookup keys. This is the primary
    -- variable for building the robust, multi-key lookup logic used to filter `target_rows` and to project
    -- a consistent set of identity columns throughout the planner's CTEs.
    -- Semantics: A de-duplicated, ordered `text[]` of all column names from all keys (e.g., `{'email', 'employee_nr'}`).
    v_all_lookup_cols TEXT[];
    -- Introspected primary key columns.
    v_pk_cols name[];
    -- Introspected temporal boundary columns (e.g., {'valid_from', 'valid_until'}).
    v_temporal_cols TEXT[];

    --
    -- Canonical Entity Key Lists
    --
    -- The complete unique identifier for a timeline segment of a specific entity.
    -- Contains the original, user-defined column names, which is critical for
    -- the dynamic SQL generation. This list **includes** temporal columns if they
    -- are part of a composite key.
    -- Purpose: Used to build fragments for joins, partitioning, and uniquely
    -- identifying a specific version of a record.
    v_original_entity_segment_key_cols TEXT[];
    -- The identifier for a *conceptual entity*, derived from the segment key but
    -- explicitly excluding all temporal boundary columns. Contains original,
    -- user-defined column names.
    -- Purpose: Building the `entity_keys` feedback payload.
    v_original_entity_key_cols TEXT[];
    -- The set of columns to use for looking up entities in the target table.
    -- Derived from lookup_keys or identity_columns.
    v_lookup_columns TEXT[];

    --
    -- Other planner variables
    --
    v_plan_key_text TEXT;
    v_plan_sqls JSONB;
    v_source_table_ident TEXT;  -- Actual source table name
    v_causal_col name;
    v_causal_column_type regtype;
    v_plan_with_op_entity_id_json_build_expr_part_A TEXT;
    v_tr_qualified_all_id_cols_list TEXT;
    v_discovered_id_cols_list_prefixed TEXT;
    v_coalesced_id_cols_list TEXT;
    v_target_entity_exists_expr TEXT;
    v_target_schema_name name;
    v_target_table_name_only name;
    v_range_constructor regtype;
    v_multirange_type regtype;
    v_range_subtype regtype;
    v_range_subtype_category char(1);
    v_valid_from_col name;
    v_valid_until_col name;
    v_valid_to_col name;
    v_range_col name;
    v_sql TEXT;
    v_ephemeral_columns TEXT[] := COALESCE(ephemeral_columns, '{}'::text[]);
    v_is_founding_mode BOOLEAN := (founding_id_column IS NOT NULL);
    v_is_identifiable_expr TEXT;
    v_source_temporal_cols_expr TEXT;
    v_consistency_check_expr TEXT;
    v_natural_key_join_condition TEXT;
    v_interval TEXT;
    v_constellation TEXT;
    v_grouping_key_expr TEXT;
    v_entity_id_check_is_null_expr_no_alias TEXT;
    -- Column Classification Variables
    v_identity_columns_classified TEXT[];
    v_generated_columns TEXT[];
    v_not_null_default_columns TEXT[];
    v_nullable_default_columns TEXT[];
    v_always_exclude_columns TEXT[];
    v_not_null_no_default_columns TEXT[];
    -- Optimization: mutually_exclusive_columns metadata for performance
    v_mutually_exclusive_cols name[];
BEGIN
    v_log_vars := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_vars', true), ''), 'false')::boolean;
    v_log_id := substr(md5(COALESCE(current_setting('sql_saga.temporal_merge.log_id_seed', true), random()::text)), 1, 3);

    IF to_regclass('pg_temp.temporal_merge_plan_cache') IS NULL THEN
        CREATE TEMP TABLE temporal_merge_plan_cache (
            cache_key TEXT PRIMARY KEY,
            plan_sqls JSONB NOT NULL
        );
    END IF;

    v_causal_col := COALESCE(founding_id_column, row_id_column);
    
    -- Resolve source table identifier early (needed for placeholder substitution in execution phase)
    -- The regclass::TEXT conversion handles schema qualification correctly
    v_source_table_ident := source_table::regclass::TEXT;

    --
    -- Phase 1: Introspection and Column List Generation
    -- This block is the single source of truth for all column lists. All subsequent
    -- logic builds upon these foundational variables.
    --
    -- 1.1: Introspect temporal boundary columns. This must happen first.
    SELECT n.nspname, c.relname
    INTO v_target_schema_name, v_target_table_name_only
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = target_table;

    SELECT e.range_column_name, e.valid_from_column_name, e.valid_until_column_name, e.valid_to_column_name, e.range_type, e.multirange_type, e.range_subtype, e.range_subtype_category
    INTO v_range_col, v_valid_from_col, v_valid_until_col, v_valid_to_col, v_range_constructor, v_multirange_type, v_range_subtype, v_range_subtype_category
    FROM sql_saga.era e
    WHERE e.table_schema = v_target_schema_name AND e.table_name = v_target_table_name_only AND e.era_name = temporal_merge_plan.era_name;

    IF v_range_col IS NULL THEN RAISE EXCEPTION 'No era named "%" found for table "%"', era_name, sql_saga.__internal_display_name(target_table); END IF;

    -- 1.1.1: Query mutually_exclusive_columns metadata for join optimization
    -- We query the first applicable mutually_exclusive_columns array from unique_keys.
    -- This is used to generate optimized CASE/WHEN join conditions.
    SELECT uk.mutually_exclusive_columns INTO v_mutually_exclusive_cols
    FROM sql_saga.unique_keys uk
    WHERE uk.table_schema = v_target_schema_name 
      AND uk.table_name = v_target_table_name_only 
      AND uk.era_name = temporal_merge_plan.era_name
      AND uk.mutually_exclusive_columns IS NOT NULL
    LIMIT 1;

    v_temporal_cols := ARRAY[v_range_col];
    IF v_valid_from_col IS NOT NULL THEN v_temporal_cols := v_temporal_cols || v_valid_from_col; END IF;
    IF v_valid_to_col IS NOT NULL THEN v_temporal_cols := v_temporal_cols || v_valid_to_col; END IF;
    IF v_valid_until_col IS NOT NULL THEN v_temporal_cols := v_temporal_cols || v_valid_until_col; END IF;

    -- 1.2: Introspect the primary key (must happen after v_temporal_cols is defined).
    SELECT COALESCE(array_agg(a.attname ORDER BY array_position(c.conkey, a.attnum)), '{}'::name[]) INTO v_pk_cols
    FROM pg_constraint c JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
    WHERE c.conrelid = target_table AND c.contype = 'p' AND a.attname <> ALL(v_temporal_cols);

    -- 1.3: Introspect and sanitize all key types.
    IF jsonb_typeof(lookup_keys) = 'array' THEN
        SELECT jsonb_agg(k) INTO lookup_keys FROM jsonb_array_elements(lookup_keys) k WHERE jsonb_typeof(k) = 'array';
    END IF;

    IF jsonb_typeof(lookup_keys) = 'array' AND jsonb_array_length(lookup_keys) > 0 AND jsonb_typeof(lookup_keys->0) = 'array' THEN
         SELECT array_agg(value) INTO v_representative_lookup_key FROM jsonb_array_elements_text(lookup_keys->0);
    END IF;

    IF jsonb_typeof(lookup_keys) = 'array' THEN
        SELECT array_agg(DISTINCT c ORDER BY c) INTO v_all_lookup_cols FROM (SELECT jsonb_array_elements_text(k) FROM jsonb_array_elements(lookup_keys) k) AS t(c);
    END IF;

    v_identity_columns := temporal_merge_plan.identity_columns;
    IF (v_identity_columns IS NULL OR cardinality(v_identity_columns) = 0) AND (v_representative_lookup_key IS NOT NULL AND cardinality(v_representative_lookup_key) > 0) THEN
        v_identity_columns := v_representative_lookup_key;
    END IF;

    -- 1.4: (Moved to cache-miss block - v_causal_column_type only used there)

    -- 1.5: Build the final, canonical lists of identity columns.
    v_lookup_columns := COALESCE(v_all_lookup_cols, v_identity_columns);

    v_natural_key_join_condition := COALESCE((SELECT string_agg(format('s1.%1$I IS NOT DISTINCT FROM s2.%1$I', col), ' AND ') FROM unnest(v_all_lookup_cols) as col), 'false');

    -- `v_original_entity_segment_key_cols`: The complete unique identifier for a timeline segment of a specific entity.
    -- This list contains the original, user-defined column names. It **includes** temporal columns if they are part of a
    -- composite key. Its primary purpose is to serve as the single source of truth for all other key lists.
    SELECT array_agg(DISTINCT c ORDER BY c) INTO v_original_entity_segment_key_cols
    FROM (
        SELECT unnest(v_all_lookup_cols) UNION SELECT unnest(v_identity_columns)
        UNION SELECT unnest(v_lookup_columns) UNION SELECT unnest(v_pk_cols)
    ) AS t(c)
    WHERE c IS NOT NULL;

    -- `v_original_entity_key_cols`: The identifier for a *conceptual entity*. This list
    -- is derived from `v_original_entity_segment_key_cols` but explicitly excludes temporal boundary columns.
    -- Purpose: Building joins on the conceptual entity and for the `entity_keys` feedback payload.
    SELECT array_agg(c) INTO v_original_entity_key_cols
    FROM unnest(v_original_entity_segment_key_cols) AS c
    WHERE c <> ALL(v_temporal_cols);

    IF temporal_merge_plan.identity_columns IS NOT NULL AND cardinality(temporal_merge_plan.identity_columns) > 0 AND v_all_lookup_cols IS NOT NULL AND cardinality(v_all_lookup_cols) > 0 THEN
        v_constellation := 'STRATEGY_HYBRID';
    ELSIF temporal_merge_plan.identity_columns IS NOT NULL AND cardinality(temporal_merge_plan.identity_columns) > 0 THEN
        v_constellation := 'STRATEGY_IDENTITY_KEY_ONLY';
    ELSIF v_all_lookup_cols IS NOT NULL AND cardinality(v_all_lookup_cols) > 0 THEN
        v_constellation := 'STRATEGY_LOOKUP_KEY_ONLY';
    ELSE
        v_constellation := 'STRATEGY_UNDEFINED';
    END IF;

    v_entity_id_check_is_null_expr_no_alias := (
        SELECT COALESCE(string_agg(format('%I IS NULL', col), ' AND '), 'true')
        FROM unnest(v_identity_columns) AS col
    );

    -- Build the robust, namespaced entity key expression that will be used in all window functions.
    DECLARE
        v_new_entity_key_part TEXT;
        v_new_entity_key_expr TEXT;
    BEGIN
        -- For new entities, the grouping logic must prioritize the explicit `founding_id_column`.
        IF v_is_founding_mode THEN
            -- If provided, `founding_id_column` (via `causal_id`) is the sole authority for grouping new entities.
            v_new_entity_key_part := 'causal_id::text';
        ELSE
            -- Otherwise, fall back to grouping by the lookup key columns. If no lookup key is available
            -- or all its columns are NULL, the final fallback is the `causal_id` (derived from `row_id`).
            v_new_entity_key_part := COALESCE(
                (SELECT string_agg(format('COALESCE(%I::text, ''_NULL_'')', col), ' || ''__'' || ') FROM unnest(v_all_lookup_cols) AS col),
                'causal_id::text'
            );
        END IF;

        IF v_is_founding_mode THEN
            v_new_entity_key_expr := 'causal_id::text';
        ELSIF v_all_lookup_cols IS NOT NULL AND cardinality(v_all_lookup_cols) > 0 THEN
            v_new_entity_key_expr := format(
                $$CASE WHEN canonical_nk_json IS NOT NULL AND canonical_nk_json <> '{}'::jsonb THEN %s ELSE %s END$$,
                (
                    SELECT COALESCE(string_agg(format('COALESCE(canonical_nk_json->>%L, ''_NULL_'')', col), ' || ''__'' || '), 'causal_id::text')
                    FROM unnest(v_all_lookup_cols) AS col
                ),
                v_new_entity_key_part
            );
        ELSIF v_identity_columns IS NOT NULL AND cardinality(v_identity_columns) > 0 THEN
            -- If no lookup keys are defined, try to use the identity key for grouping new entities.
            -- Only use the identity key for grouping if it's actually provided in the source row.
            -- Otherwise, fall back to the causal ID.
            v_new_entity_key_expr := format(
                $$CASE WHEN NOT (%s) THEN %s ELSE causal_id::text END$$,
                v_entity_id_check_is_null_expr_no_alias,
                (SELECT string_agg(format('COALESCE(%I::text, ''_NULL_'')', col), ' || ''__'' || ') FROM unnest(v_identity_columns) AS col)
            );
        ELSE
            -- If no lookup keys and no identity key are defined, the only available grouping
            -- mechanism for new entities is the causal ID (from founding_id or row_id).
            v_new_entity_key_expr := 'causal_id::text';
        END IF;

        v_grouping_key_expr := format(
            $$CASE
                WHEN is_new_entity
                THEN 'new_entity__' || %2$s
                ELSE 'existing_entity__' || %1$s
            END$$,
            (SELECT string_agg(format('COALESCE(%I::text, ''_NULL_'')', col), ' || ''__'' || ') FROM unnest(v_identity_columns) AS t(col)),
            v_new_entity_key_expr
        );
    END;

    -- 1.6: Sanitize and normalize ephemeral columns list.
    -- Normalize ephemeral columns for stable cache key and better reuse.
    SELECT ARRAY(SELECT DISTINCT e FROM unnest(v_ephemeral_columns) AS e ORDER BY e)
    INTO v_ephemeral_columns;

    -- 1.7: Build the source table's temporal column expression.
    -- This block determines how to project `valid_from` and `valid_until` from the source,
    -- handling `valid_to` conversion automatically. It also performs a consistency
    -- check if both `valid_to` and `valid_until` are present.
    DECLARE
        v_source_has_range_col BOOLEAN;
        v_source_has_valid_from BOOLEAN;
        v_source_has_valid_until BOOLEAN;
        v_source_has_valid_to BOOLEAN;
        v_from_expr TEXT;
        v_until_expr TEXT;
    BEGIN
        -- Introspect all possible temporal source columns.
        SELECT
            EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = source_table AND attname = v_range_col AND NOT attisdropped AND attnum > 0),
            EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = source_table AND attname = v_valid_from_col AND NOT attisdropped AND attnum > 0),
            EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = source_table AND attname = v_valid_until_col AND NOT attisdropped AND attnum > 0)
        INTO v_source_has_range_col, v_source_has_valid_from, v_source_has_valid_until;

        IF v_valid_to_col IS NOT NULL THEN
            SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = source_table AND attname = v_valid_to_col AND NOT attisdropped AND attnum > 0)
            INTO v_source_has_valid_to;
        ELSE
            v_source_has_valid_to := false;
        END IF;

        -- Must have at least a `valid_from` or `range` column.
        IF NOT v_source_has_valid_from AND NOT v_source_has_range_col THEN
             RAISE EXCEPTION 'Source table "%" must have either the range column "%" or the component column "%".', sql_saga.__internal_display_name(source_table), v_range_col, v_valid_from_col;
        END IF;

        -- Build expressions for `_from` and `_until` components.
        IF v_source_has_valid_from THEN
            v_from_expr := format('source_table.%I', v_valid_from_col);
        END IF;

        IF v_source_has_valid_to THEN
            CASE v_range_subtype_category
                WHEN 'D' THEN v_interval := '''1 day''::interval';
                WHEN 'N' THEN v_interval := '1';
                ELSE RAISE EXCEPTION 'Unsupported range subtype for valid_to -> valid_until conversion: %', v_range_constructor::text;
            END CASE;
        END IF;

        -- Build v_until_expr with per-row logic
        IF v_source_has_valid_until AND v_source_has_valid_to THEN
             -- Both exist, so create a COALESCE expression to handle NULL valid_until.
             v_until_expr := format('COALESCE(source_table.%I, (source_table.%I + %s)::%s)', v_valid_until_col, v_valid_to_col, v_interval, v_range_subtype);
        ELSIF v_source_has_valid_until THEN
             v_until_expr := format('source_table.%I', v_valid_until_col);
        ELSIF v_source_has_valid_to THEN
             v_until_expr := format('(source_table.%I + %s)::%s', v_valid_to_col, v_interval, v_range_subtype);
        END IF;

        -- Must have a source for `_until` if there's no range column.
        IF NOT v_source_has_range_col AND v_until_expr IS NULL THEN
            RAISE EXCEPTION 'Source table "%" must have a "%", "%", or "%" column.', sql_saga.__internal_display_name(source_table), v_range_col, v_valid_until_col, v_valid_to_col;
        END IF;

        -- Combine expressions, prioritizing the range column but falling back to components.
        IF v_source_has_range_col THEN
            v_source_temporal_cols_expr := format('COALESCE(lower(source_table.%1$I), %2$s) as valid_from, COALESCE(upper(source_table.%1$I), %3$s) as valid_until',
                v_range_col, COALESCE(v_from_expr, 'NULL'), COALESCE(v_until_expr, 'NULL'));
        ELSE
            v_source_temporal_cols_expr := format('%s as valid_from, %s as valid_until', v_from_expr, v_until_expr);
        END IF;

        -- Build consistency check.
        IF v_source_has_valid_until AND v_source_has_valid_to THEN
            v_consistency_check_expr := format($$((source_table.%1$I IS NULL OR source_table.%2$I IS NULL) OR (((source_table.%1$I + %3$s)::%4$s) IS NOT DISTINCT FROM source_table.%2$I))$$, v_valid_to_col, v_valid_until_col, v_interval, v_range_subtype);
        ELSE
            v_consistency_check_expr := 'true';
        END IF;
    END;

    --
    IF v_log_vars THEN
        RAISE NOTICE '(%) --- temporal_merge_plan variables ---', v_log_id;
        RAISE NOTICE '(%) --- Foundational (Parameters & Introspection) ---', v_log_id;
        RAISE NOTICE '(%) p_identity_columns: %', v_log_id, temporal_merge_plan.identity_columns;
        RAISE NOTICE '(%) p_lookup_keys: %', v_log_id, temporal_merge_plan.lookup_keys;
        RAISE NOTICE '(%) p_ephemeral_columns (original): %', v_log_id, temporal_merge_plan.ephemeral_columns;
        RAISE NOTICE '(%) v_pk_cols (introspected): %', v_log_id, v_pk_cols;
        RAISE NOTICE '(%) v_temporal_cols (introspected): %', v_log_id, v_temporal_cols;
        RAISE NOTICE '(%) v_ephemeral_columns (normalized): %', v_log_id, v_ephemeral_columns;
        RAISE NOTICE '(%) --- Derived Canonical Lists ---', v_log_id;
        RAISE NOTICE '(%) v_identity_columns: %', v_log_id, v_identity_columns;
        RAISE NOTICE '(%) v_representative_lookup_key (first key): %', v_log_id, v_representative_lookup_key;
        RAISE NOTICE '(%) v_all_lookup_cols (all keys): %', v_log_id, v_all_lookup_cols;
        RAISE NOTICE '(%) v_lookup_columns (for entity filtering): %', v_log_id, v_lookup_columns;
        RAISE NOTICE '(%) v_original_entity_segment_key_cols (for joins/partitions): %', v_log_id, v_original_entity_segment_key_cols;
        RAISE NOTICE '(%) v_original_entity_key_cols (for feedback payload): %', v_log_id, v_original_entity_key_cols;
        RAISE NOTICE '(%) --- Temporal Column Handling ---', v_log_id;
        RAISE NOTICE '(%) v_interval: %', v_log_id, v_interval;
        RAISE NOTICE '(%) v_source_temporal_cols_expr: %', v_log_id, v_source_temporal_cols_expr;
        RAISE NOTICE '(%) v_consistency_check_expr: %', v_log_id, v_consistency_check_expr;
    END IF;

    --
    -- Phase 2: Session Cache Management & Validation
    --
    -- Generated plans are cached only per session (pg_temp). There is deliberately no cache shared
    -- between sessions: a shared cache of executable SQL lets one role make another role run SQL
    -- of its choosing (cache poisoning).
    -- The cache key includes source_table OID for within-session uniqueness.
    v_plan_key_text := format('%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s',
        target_table::oid,
        source_table::oid,
        identity_columns,
        v_ephemeral_columns,
        mode,
        era_name,
        row_id_column,
        COALESCE(founding_id_column, ''),
        v_range_constructor,
        delete_mode,
        COALESCE(lookup_keys, '[]'::jsonb),
        p_log_trace,
        source_table::regclass::text -- Add source table name to key to differentiate plans for different temp tables.
    );

    SELECT plan_sqls INTO v_plan_sqls
    FROM pg_temp.temporal_merge_plan_cache
    WHERE cache_key = v_plan_key_text;
    
    IF v_plan_sqls IS NULL THEN -- Cache miss: PREPARE all sql's
        -- On cache miss, proceed with validation and planning.
        -- OPT-1: Consolidated column validation - single query instead of 6+ separate PERFORM queries.
        -- This batch validates all required columns exist in their respective tables at once.
        DECLARE
            v_missing RECORD;
            v_check_identity_in_source BOOLEAN;
        BEGIN
            -- Identity columns only need to exist in source if no lookup key is provided
            v_check_identity_in_source := (v_representative_lookup_key IS NULL OR cardinality(v_representative_lookup_key) = 0);
            
            FOR v_missing IN
                WITH required_columns AS (
                    -- row_id_column must exist in source
                    SELECT 'row_id_column' AS col_type, row_id_column AS col_name, source_table AS tbl
                    UNION ALL
                    -- founding_id_column must exist in source (if provided)
                    SELECT 'founding_id_column', founding_id_column, source_table
                    WHERE founding_id_column IS NOT NULL
                    UNION ALL
                    -- identity_columns must exist in target
                    SELECT 'identity_column (target)', unnest(v_identity_columns), target_table
                    WHERE v_identity_columns IS NOT NULL AND cardinality(v_identity_columns) > 0
                    UNION ALL
                    -- identity_columns must exist in source (only if no lookup key)
                    SELECT 'identity_column (source)', unnest(v_identity_columns), source_table
                    WHERE v_identity_columns IS NOT NULL AND cardinality(v_identity_columns) > 0
                      AND v_check_identity_in_source
                    UNION ALL
                    -- lookup_columns must exist in target
                    SELECT 'lookup_column (target)', unnest(v_all_lookup_cols), target_table
                    WHERE v_all_lookup_cols IS NOT NULL AND cardinality(v_all_lookup_cols) > 0
                    UNION ALL
                    -- lookup_columns must exist in source
                    SELECT 'lookup_column (source)', unnest(v_all_lookup_cols), source_table
                    WHERE v_all_lookup_cols IS NOT NULL AND cardinality(v_all_lookup_cols) > 0
                )
                SELECT rc.col_type, rc.col_name, rc.tbl::regclass::text AS tbl_name
                FROM required_columns rc
                LEFT JOIN pg_attribute a ON a.attrelid = rc.tbl 
                    AND a.attname = rc.col_name 
                    AND a.attnum > 0 
                    AND NOT a.attisdropped
                WHERE a.attname IS NULL
            LOOP
                -- Raise specific error message based on column type
                CASE v_missing.col_type
                    WHEN 'row_id_column' THEN
                        RAISE EXCEPTION 'row_id_column "%" does not exist in source table %s', v_missing.col_name, v_missing.tbl_name;
                    WHEN 'founding_id_column' THEN
                        RAISE EXCEPTION 'founding_id_column "%" does not exist in source table %s', v_missing.col_name, v_missing.tbl_name;
                    WHEN 'identity_column (target)' THEN
                        RAISE EXCEPTION 'identity_column % does not exist in target table %', quote_ident(v_missing.col_name), v_missing.tbl_name;
                    WHEN 'identity_column (source)' THEN
                        RAISE EXCEPTION 'identity_column % does not exist in source table % (and no lookup_columns were provided)', quote_ident(v_missing.col_name), v_missing.tbl_name;
                    WHEN 'lookup_column (target)' THEN
                        RAISE EXCEPTION 'lookup_column % does not exist in target table %', quote_ident(v_missing.col_name), v_missing.tbl_name;
                    WHEN 'lookup_column (source)' THEN
                        RAISE EXCEPTION 'lookup_column % does not exist in source table %', quote_ident(v_missing.col_name), v_missing.tbl_name;
                END CASE;
            END LOOP;
        END; -- END consolidated column validation
    
        IF v_causal_col IS NULL THEN
            RAISE EXCEPTION 'The causal identifier column cannot be NULL. Please provide a non-NULL value for either founding_id_column or row_id_column.';
        END IF;
        
        -- OPT-7a: Moved from pre-cache section - only used inside cache-miss block
        SELECT atttypid INTO v_causal_column_type
        FROM pg_attribute
        WHERE attrelid = source_table
          AND attname = v_causal_col
          AND NOT attisdropped AND attnum > 0;
          
        IF v_causal_column_type IS NULL THEN
            RAISE EXCEPTION 'Causal column "%" does not exist in source table %s', v_causal_col, source_table::text;
        END IF;
        IF v_causal_col = ANY(v_lookup_columns) THEN
            RAISE EXCEPTION 'The causal column (%) cannot be one of the natural identity columns (%)', v_causal_col, v_lookup_columns;
        END IF;
    
        IF v_range_constructor IS NULL THEN
            RAISE EXCEPTION 'Could not determine the range type for era "%" on table "%". Please ensure the era was created correctly, possibly by specifying the range_type parameter in add_era().', era_name, sql_saga.__internal_display_name(target_table);
        END IF;
    
        -- Validate that user has not passed in temporal columns as ephemeral.
        IF v_valid_from_col = ANY(COALESCE(temporal_merge_plan.ephemeral_columns, '{}')) OR v_valid_until_col = ANY(COALESCE(temporal_merge_plan.ephemeral_columns, '{}')) THEN
            RAISE EXCEPTION 'Temporal boundary columns ("%", "%") cannot be specified in ephemeral_columns.', v_valid_from_col, v_valid_until_col;
        END IF;
        IF v_valid_to_col IS NOT NULL AND v_valid_to_col = ANY(COALESCE(temporal_merge_plan.ephemeral_columns, '{}')) THEN
            RAISE EXCEPTION 'Synchronized column "%" is automatically handled and should not be specified in ephemeral_columns.', v_valid_to_col;
        END IF;
        IF v_range_col IS NOT NULL AND v_range_col = ANY(COALESCE(temporal_merge_plan.ephemeral_columns, '{}')) THEN
            RAISE EXCEPTION 'Synchronized column "%" is automatically handled and should not be specified in ephemeral_columns.', v_range_col;
        END IF;
    
        --
        -- Phase 3: Dynamic SQL Fragment Generation
        --
        v_coalesced_id_cols_list := (
            SELECT string_agg(format('COALESCE(p.%1$I, p.discovered_id_%2$s) AS %1$I', col, ord), ', ')
            FROM unnest(v_original_entity_segment_key_cols) WITH ORDINALITY AS c(col, ord)
            WHERE col <> ALL(v_temporal_cols)
        );
        v_target_entity_exists_expr := '(p.discovered_stable_pk_payload IS NOT NULL)';
    
        -- On cache miss, enter a new block to declare variables and do the expensive work.
        DECLARE
            v_sql TEXT;
            v_trace_seed_expr TEXT;
            v_source_data_cols_jsonb_build TEXT;
            v_source_ephemeral_cols_jsonb_build TEXT;
            v_target_data_cols_jsonb_build TEXT;
            v_target_ephemeral_cols_jsonb_build TEXT;
            v_target_ephemeral_cols_jsonb_build_bare TEXT;
            v_target_data_cols_jsonb_build_bare TEXT;
            v_lookup_cols_select_list TEXT;
            v_lookup_cols_select_list_no_alias TEXT;
            v_target_table_ident TEXT;
            v_source_data_payload_expr TEXT;
            v_source_ephemeral_payload_expr TEXT;
            v_final_data_payload_expr TEXT;
            v_resolver_ctes TEXT;
            v_resolver_from TEXT;
            v_diff_join_condition TEXT;
            v_entity_id_check_is_null_expr TEXT;
            v_causal_select_expr TEXT;
            v_target_rows_filter TEXT;
            v_stable_pk_cols_jsonb_build TEXT;
            v_join_on_lookup_cols TEXT;
            v_lateral_join_sr_to_seg TEXT;
            v_lateral_join_tr_to_seg TEXT;
            v_lookup_cols_si_alias_select_list TEXT;
            v_non_temporal_lookup_cols_select_list TEXT;
            v_non_temporal_lookup_cols_select_list_no_alias TEXT;
            v_non_temporal_tr_qualified_lookup_cols TEXT;
            v_identity_cols_trace_expr TEXT;
            v_natural_identity_cols_trace_expr TEXT;
            v_lookup_cols_are_null_expr TEXT;
            v_lookup_cols_sans_valid_from TEXT;
            v_lookup_cols_select_list_no_alias_sans_vf TEXT;
            v_trace_select_list TEXT;
            v_target_rows_lookup_cols_expr TEXT;
            v_source_rows_exists_join_expr TEXT;
            v_atomic_segments_select_list_expr TEXT;
            v_diff_select_expr TEXT;
            v_plan_with_op_entity_id_json_build_expr TEXT;
            v_skip_no_target_entity_id_json_build_expr TEXT;
            v_final_order_by_expr TEXT;
            v_entity_key_for_with_base_payload_expr TEXT;
            v_s_founding_join_condition TEXT;
            v_tr_qualified_lookup_cols TEXT;
            v_stable_id_aggregates_expr TEXT;
            v_stable_id_projection_expr TEXT;
            v_stable_pk_cols_jsonb_build_bare TEXT;
            v_rcte_join_condition TEXT;
            v_unqualified_id_cols_sans_vf TEXT;
            v_qualified_r_id_cols_sans_vf TEXT;
            v_grouping_key_expr_for_union TEXT;
            v_coalesced_payload_expr TEXT;
            v_stable_id_aggregates_expr_prefixed_with_comma TEXT;
            v_plan_select_key_cols TEXT;
            v_grouping_key_cols TEXT;
            v_grouping_key_cols_prefix TEXT;
            v_non_temporal_lookup_cols_select_list_no_alias_prefix TEXT;
            v_stable_id_projection_expr_prefix TEXT;
            v_non_temporal_lookup_cols_select_list_prefix TEXT;
            v_non_temporal_tr_qualified_lookup_cols_prefix TEXT;
            v_lookup_cols_sans_valid_from_prefix TEXT;
            v_lateral_join_entity_id_clause TEXT;
            v_stable_pk_cols_jsonb_build_source TEXT;
            -- OPT-2: Cached nullability info for entity key columns (single query instead of 3)
            v_entity_key_cols_notnull name[];
            v_entity_key_cols_nullable name[];
            -- OPT-3: Cached metadata for lookup columns (nullability + type info)
            v_lookup_cols_meta JSONB;  -- {col_name: {notnull: bool, type: text}}
            v_propagated_id_cols_list TEXT;
            v_lookup_keys_as_jsonb_expr TEXT;
            v_lookup_keys_as_array_expr TEXT;
            v_keys_for_filtering JSONB;
            v_lateral_source_resolver_sql TEXT;
            v_lateral_source_resolver_sql_existing TEXT;
            v_lateral_source_resolver_sql_new TEXT;
            v_lateral_join_sr_to_seg_existing TEXT;
            v_lateral_join_sr_to_seg_new TEXT;
            v_unified_id_cols_projection TEXT;
            v_target_nk_json_expr TEXT;
            v_identity_keys_jsonb_build_expr_d TEXT;
            v_lookup_keys_jsonb_build_expr_d TEXT;
            v_identity_keys_jsonb_build_expr_sr TEXT;
            v_lookup_keys_jsonb_build_expr_sr TEXT;
        BEGIN
            v_plan_sqls := '[]'::jsonb;
            -- On cache miss, proceed with the expensive introspection and query building.
            
            -- OPT-2: Single query to get nullability info for all entity key columns.
            -- This replaces 3 separate pg_attribute queries in join condition builders.
            SELECT 
                COALESCE(array_agg(a.attname) FILTER (WHERE a.attnotnull), '{}'),
                COALESCE(array_agg(a.attname) FILTER (WHERE NOT a.attnotnull), '{}')
            INTO v_entity_key_cols_notnull, v_entity_key_cols_nullable
            FROM unnest(v_original_entity_key_cols) AS col
            JOIN pg_attribute a ON a.attrelid = target_table AND a.attname = col AND NOT a.attisdropped;
            
            -- OPT-3: Single query to get nullability + type info for all lookup columns.
            -- This replaces 4 separate pg_attribute queries in target_rows_filter loop.
            SELECT jsonb_object_agg(
                a.attname,
                jsonb_build_object('notnull', a.attnotnull, 'type', format_type(a.atttypid, a.atttypmod))
            )
            INTO v_lookup_cols_meta
            FROM unnest(v_lookup_columns) AS col
            JOIN pg_attribute a ON a.attrelid = target_table AND a.attname = col AND NOT a.attisdropped;
            
            v_lookup_cols_meta := COALESCE(v_lookup_cols_meta, '{}'::jsonb);
            
            v_unified_id_cols_projection := (
                SELECT string_agg(
                    CASE
                        WHEN col = ANY(COALESCE(v_all_lookup_cols, '{}'))
                        THEN format('(tpu.unified_canonical_nk_json->>%L)::%s AS %I', col, type, col)
                        ELSE format('tpu.%I', col)
                    END,
                    ', ' ORDER BY ord
                )
                FROM (
                    SELECT col, c.ord, format_type(a.atttypid, a.atttypmod) as type
                    FROM unnest(v_original_entity_segment_key_cols) WITH ORDINALITY AS c(col, ord)
                    JOIN pg_attribute a ON a.attrelid = target_table AND a.attname = c.col
                    WHERE col <> ALL(v_temporal_cols)
                ) AS all_id_cols
            );
            v_target_nk_json_expr := (SELECT format('jsonb_strip_nulls(jsonb_build_object(%s))', COALESCE(string_agg(format('%L, t.%I', col, col), ', '), '')) FROM unnest(v_all_lookup_cols) as col);
            v_target_nk_json_expr := COALESCE(v_target_nk_json_expr, '''{}''::jsonb');
    
            v_identity_keys_jsonb_build_expr_d := (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, d.%I', col, col), ', '), '')) FROM unnest(v_identity_columns) as col WHERE col IS NOT NULL);
            v_lookup_keys_jsonb_build_expr_d := (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, d.%I', col, col), ', '), '')) FROM unnest(v_all_lookup_cols) as col WHERE col IS NOT NULL);
            v_identity_keys_jsonb_build_expr_sr := (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, source_row.%I', col, col), ', '), '')) FROM unnest(v_identity_columns) as col WHERE col IS NOT NULL);
            v_lookup_keys_jsonb_build_expr_sr := (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, source_row.%I', col, col), ', '), '')) FROM unnest(v_all_lookup_cols) as col WHERE col IS NOT NULL);
    
            v_lookup_keys_as_jsonb_expr := (SELECT format('jsonb_strip_nulls(jsonb_build_object(%s))', COALESCE(string_agg(format('%L, source_row.%I', col, col), ', '), '')) FROM unnest(v_all_lookup_cols) as col);
            v_lookup_keys_as_jsonb_expr := COALESCE(v_lookup_keys_as_jsonb_expr, '''{}''::jsonb');
            -- Since jsonb_object_keys returns a SET we can not directly use array_length on it, we build an expression v_lookup_keys_as_array_expr that builds an array of the column names with a non null value for this row.
            v_lookup_keys_as_array_expr := (
                SELECT format('ARRAY(SELECT e FROM unnest(ARRAY[%s]::TEXT[]) e WHERE e IS NOT NULL ORDER BY e)',
                    string_agg(format('CASE WHEN source_row.%I IS NOT NULL THEN %L END', col, col), ', ')
                )
                FROM unnest(COALESCE(v_all_lookup_cols, '{}'::TEXT[])) as col
            );
            v_lookup_keys_as_array_expr := COALESCE(v_lookup_keys_as_array_expr, 'ARRAY[]::text[]');
            v_propagated_id_cols_list := (
                SELECT string_agg(format('target_row.%I AS discovered_id_%s',
                    CASE
                        WHEN c.col = v_valid_from_col THEN 'valid_from'::name
                        WHEN c.col = v_valid_until_col THEN 'valid_until'::name
                        ELSE c.col
                    END,
                    c.ord), ', ')
                FROM unnest(v_original_entity_segment_key_cols) WITH ORDINALITY AS c(col, ord)
            );
    
            v_identity_cols_trace_expr := (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, seg.%I', col, col), ', '), '')) FROM unnest(COALESCE(v_identity_columns, '{}')) as col);
            v_natural_identity_cols_trace_expr := (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, seg.%I', col, col), ', '), '')) FROM unnest(COALESCE(v_representative_lookup_key, '{}')) as col);
            -- This expression must be robust to natural key columns not existing in the source table,
            -- which can happen when identity columns are auto-discovered. The subquery ensures that
            -- string_agg over zero rows (which returns NULL) is handled correctly by COALESCE,
            -- which provides a sensible default of 'true'.
            SELECT
                format('(%s)', COALESCE(
                    (
                        SELECT string_agg(
                            CASE
                                WHEN a.attname IS NULL THEN 'true'
                                ELSE format('source_table.%I IS NULL', c.col)
                            END,
                            ' AND ' ORDER BY c.ord
                        )
                        FROM unnest(COALESCE(v_all_lookup_cols, '{}')) WITH ORDINALITY AS c(col, ord)
                        LEFT JOIN pg_attribute a ON a.attrelid = temporal_merge_plan.source_table AND a.attname = c.col AND a.attnum > 0 AND NOT a.attisdropped
                    ),
                    'true'
                ))
            INTO v_lookup_cols_are_null_expr;
    
            IF founding_id_column IS NOT NULL THEN
                -- If an explicit causal grouping column is provided, it might contain NULLs.
                -- We must coalesce to the row_id_column to ensure every new entity
                -- has a unique causal identifier. To handle different data types,
                -- we cast the row_id_column to the type of the primary causal column.
                v_causal_select_expr := format('COALESCE(source_table.%I, source_table.%I::%s)',
                    founding_id_column,
                    row_id_column,
                    v_causal_column_type::text
                );
            ELSE
                -- If no explicit causal column is given, the row_id_column is the causal identifier.
                v_causal_select_expr := format('source_table.%I', row_id_column);
            END IF;
    
            -- Resolve table identifiers to be correctly quoted and schema-qualified.
            v_target_table_ident := target_table::TEXT;
            -- v_source_table_ident is set earlier in the function
    
            -- Construct reusable SQL fragments for entity key column lists and join conditions,
            -- since these are more index friendly than a jsonb object, that we previously had.
            -- The lateral joins from atomic segments to source/target rows must use ALL identity columns
            -- to ensure the join is on a unique entity. This logic correctly maps original column names
            -- to their internal, aliased names, which are used in all relevant CTEs (sr, tr, seg).
            --
            -- PERFORMANCE OPTIMIZATION: Use simple equality (col = col) for NOT NULL columns,
            -- which allows PostgreSQL to use a direct Index Scan instead of BitmapOr with two
            -- index scans, providing ~2x speedup. For NULLABLE columns, we must use the 
            -- NULL-safe pattern (col = col OR (col IS NULL AND col IS NULL)) to correctly
            -- handle NULL values that should match.
            -- OPT-2: Use cached nullability arrays instead of querying pg_attribute
            v_lateral_join_sr_to_seg_existing := (
                SELECT COALESCE(string_agg(
                    CASE
                        WHEN col = ANY(v_entity_key_cols_notnull) THEN
                            format('source_row.%1$I = seg.%1$I', col)
                        ELSE
                            format('(source_row.%1$I = seg.%1$I OR (source_row.%1$I IS NULL AND seg.%1$I IS NULL))', col)
                    END,
                    ' AND '
                ), 'true')
                FROM unnest(v_original_entity_key_cols) AS col
            );
            v_lateral_join_sr_to_seg_new := 'source_row.grouping_key = seg.grouping_key';

            v_lateral_join_sr_to_seg := format($$
                ((seg.is_new_entity AND %2$s)
                OR
                (NOT seg.is_new_entity AND %1$s))
            $$, v_lateral_join_sr_to_seg_existing, v_lateral_join_sr_to_seg_new);
            -- Build the join condition for existing_segments_with_target.
            -- Optimized: Use mutually_exclusive_columns for CASE/WHEN optimization when available.
            -- For other NULLABLE columns, use null-safe pattern (a = b OR (a IS NULL AND b IS NULL))
            -- For NOT NULL columns, use simple = for better query plan performance (Hash Joins).
            IF v_mutually_exclusive_cols IS NOT NULL 
               AND v_mutually_exclusive_cols <@ v_original_entity_key_cols::name[] THEN
                -- Generate optimized CASE/WHEN condition for mutually exclusive columns.
                -- This avoids expensive (col IS NULL AND col IS NULL) checks when columns
                -- are known to be mutually exclusive (only one can be non-NULL at a time).
                -- OPT-2: Use cached nullability arrays instead of querying pg_attribute
                v_lateral_join_tr_to_seg := (
                    WITH me_cols AS (
                        -- Mutually exclusive columns: generate WHEN clauses
                        SELECT string_agg(
                            format(
                                'WHEN target_row.%1$I IS NOT NULL THEN target_row.%1$I = seg.%1$I%2$s',
                                col,
                                -- Add IS NULL checks for other mutually exclusive cols
                                (SELECT COALESCE(' AND ' || string_agg(format('seg.%I IS NULL', other_col), ' AND '), '')
                                 FROM unnest(v_mutually_exclusive_cols) other_col WHERE other_col <> col)
                            ),
                            ' '
                        ) AS case_expr
                        FROM unnest(v_mutually_exclusive_cols) AS col
                    ),
                    non_me_cols AS (
                        -- Non-mutually-exclusive columns: use standard null-safe comparison
                        SELECT COALESCE(string_agg(
                            CASE
                                WHEN col = ANY(v_entity_key_cols_notnull) THEN
                                    format('target_row.%1$I = seg.%1$I', col)
                                ELSE
                                    format('(target_row.%1$I = seg.%1$I OR (target_row.%1$I IS NULL AND seg.%1$I IS NULL))', col)
                            END,
                            ' AND '
                        ), '') AS conditions
                        FROM unnest(v_original_entity_key_cols) AS col
                        WHERE col <> ALL(v_mutually_exclusive_cols)
                    )
                    SELECT 
                        CASE 
                            WHEN me_cols.case_expr IS NOT NULL AND non_me_cols.conditions <> '' THEN
                                format('(CASE %s ELSE FALSE END) AND %s', me_cols.case_expr, non_me_cols.conditions)
                            WHEN me_cols.case_expr IS NOT NULL THEN
                                format('CASE %s ELSE FALSE END', me_cols.case_expr)
                            ELSE 'true'
                        END
                    FROM me_cols, non_me_cols
                );
            ELSE
                -- OPT-2: Use cached nullability arrays instead of querying pg_attribute
                v_lateral_join_tr_to_seg := (
                    SELECT COALESCE(string_agg(
                        CASE
                            WHEN col = ANY(v_entity_key_cols_notnull) THEN
                                format('target_row.%1$I = seg.%1$I', col)
                            ELSE
                                format('(target_row.%1$I = seg.%1$I OR (target_row.%1$I IS NULL AND seg.%1$I IS NULL))', col)
                        END,
                        ' AND '
                    ), 'true')
                    FROM unnest(v_original_entity_key_cols) AS col
                );
            END IF;
    
            -- These variables are still built from the lookup columns, as they are used for the initial
            -- entity discovery and propagation steps, which are driven by the lookup keys.
            SELECT
                string_agg(format('t.%I', col), ', '),
                string_agg(format('si.%I', col), ', ')
            INTO
                v_lookup_cols_select_list,
                v_lookup_cols_si_alias_select_list
            FROM unnest(v_lookup_columns) col;
    
            SELECT COALESCE(string_agg(format('(s_inner.%1$I = dr.%1$I OR (s_inner.%1$I IS NULL AND dr.%1$I IS NULL))', col), ' AND '), 'true')
            INTO v_lateral_join_entity_id_clause
            FROM unnest(v_lookup_columns) col;
    
            -- This variable MUST be built from v_original_entity_segment_key_cols to ensure stable IDs are projected. It excludes
            -- temporal columns, which are handled explicitly in the CTEs that use this list.
            v_non_temporal_lookup_cols_select_list_no_alias := (
                SELECT COALESCE(string_agg(format('%I', col), ', '), '')
                FROM unnest(v_original_entity_segment_key_cols) AS c(col)
                WHERE c.col <> ALL(v_temporal_cols)
            );
            -- This variable is used in the SELECT list for time-point and segment CTEs.
            -- It MUST exclude temporal columns, as those are handled explicitly (e.g., `point as valid_from`).
            -- Including them here would cause a "column specified more than once" error.
            v_lookup_cols_select_list_no_alias := (
                SELECT COALESCE(string_agg(format('%I', c.col), ', '), '')
                FROM unnest(v_original_entity_segment_key_cols) as c(col)
                WHERE c.col <> ALL(v_temporal_cols)
            );
    
            -- The grouping key must contain ALL identity columns to uniquely identify an entity's timeline.
            SELECT string_agg(format('%I', col), ', ')
            INTO v_grouping_key_cols
            FROM unnest(v_original_entity_segment_key_cols) col
            WHERE col <> ALL(v_temporal_cols);
    
            v_grouping_key_cols := COALESCE(v_grouping_key_cols, '');
            v_grouping_key_cols_prefix := COALESCE(NULLIF(v_grouping_key_cols, '') || ', ', '');
    
            -- When the stable ID columns are not part of the partition key, they must be aggregated.
            -- A simple `max()` correctly coalesces the NULL from a source row and the non-NULL
            -- value from the corresponding target row into the single correct stable ID.
            -- A simple `max()` correctly coalesces the NULL from a source row and the non-NULL
            -- value from the corresponding target row into the single correct stable ID.
            -- This expression aggregates *all* identity columns. In the coalescing step, we group by
            -- the island and use max() to get the single, canonical set of identifiers for that island.
            SELECT COALESCE(string_agg(format('max(%I) as %I', col, col), ', '), '')
            INTO v_stable_id_aggregates_expr
            FROM unnest(v_original_entity_key_cols) c(col);
    
            v_stable_id_aggregates_expr_prefixed_with_comma := COALESCE(v_stable_id_aggregates_expr, '');
    
            SELECT COALESCE(string_agg(format('%I', col), ', '), '')
            INTO v_stable_id_projection_expr
            FROM unnest(v_original_entity_segment_key_cols) c(col)
            WHERE col <> ALL(v_lookup_columns);
            v_stable_id_projection_expr_prefix := COALESCE(NULLIF(v_stable_id_projection_expr, '') || ', ', '');
            
            -- Create versions of the lookup column list for SELECT clauses that exclude
            -- temporal columns, to avoid selecting them twice in various CTEs.
            -- This version is for the `source_initial` CTE. It must project NULL for any
            -- identity columns that exist in the target but not the source, to ensure
            -- a UNION-compatible structure with the `target_rows` CTE.
            v_non_temporal_lookup_cols_select_list := (
                SELECT COALESCE(string_agg(
                    CASE
                        WHEN sa.attname IS NOT NULL THEN format('source_table.%I', c.col) -- Column exists in source
                        ELSE format('NULL::%s AS %I', format_type(ta.atttypid, ta.atttypmod), c.col) -- Column exists only in target
                    END,
                ', ' ORDER BY c.ord), '')
                FROM unnest(v_original_entity_segment_key_cols) WITH ORDINALITY AS c(col, ord)
                JOIN pg_attribute ta ON ta.attrelid = target_table AND ta.attname = c.col AND ta.attnum > 0 AND NOT ta.attisdropped
                LEFT JOIN pg_attribute sa ON sa.attrelid = source_table AND sa.attname = c.col AND sa.attnum > 0 AND NOT sa.attisdropped
                WHERE c.col <> ALL(v_temporal_cols)
            );
            v_non_temporal_lookup_cols_select_list_prefix := COALESCE(NULLIF(v_non_temporal_lookup_cols_select_list, '') || ', ', '');
            -- This version is for `target_rows` and subsequent CTEs where all columns are present.
            v_non_temporal_lookup_cols_select_list_no_alias_prefix := COALESCE(NULLIF(v_non_temporal_lookup_cols_select_list_no_alias, '') || ', ', '');
            v_non_temporal_tr_qualified_lookup_cols := (
                SELECT COALESCE(string_agg(format('target_row.%I', c.col), ', '), '')
                FROM unnest(v_original_entity_segment_key_cols) AS c(col)
                WHERE c.col <> ALL(v_temporal_cols)
            );
            v_non_temporal_tr_qualified_lookup_cols_prefix := COALESCE(NULLIF(v_non_temporal_tr_qualified_lookup_cols, '') || ', ', '');
    
            v_lookup_cols_sans_valid_from := (
                SELECT string_agg(format('%I', c.col), ', ')
                FROM unnest(v_original_entity_segment_key_cols) AS c(col)
                WHERE c.col <> ALL(v_temporal_cols)
            );
            v_lookup_cols_sans_valid_from_prefix := COALESCE(NULLIF(v_lookup_cols_sans_valid_from, '') || ', ', '');
            v_lookup_cols_sans_valid_from := COALESCE(v_lookup_cols_sans_valid_from, '');
    
            -- Unqualified list of ID columns (e.g., "id, ident_corr") for GROUP BY and some SELECTs.
            v_unqualified_id_cols_sans_vf := (
                SELECT string_agg(format('%I', c.col), ', ')
                FROM unnest(v_original_entity_segment_key_cols) AS c(col)
                WHERE c.col <> ALL(v_temporal_cols) AND c.col IS NOT NULL
            );
    
            -- Qualified list of ID columns (e.g., "r.id, r.corr_ent") for the recursive part of the CTE.
            v_qualified_r_id_cols_sans_vf := (
                SELECT string_agg(format('r.%I', c.col), ', ')
                FROM unnest(v_original_entity_segment_key_cols) AS c(col)
                WHERE c.col <> ALL(v_temporal_cols)
            );
    
            -- 1. Dynamically build jsonb payload expressions for SOURCE and TARGET tables.
            -- The source payload only includes columns present in the source table.
            -- The target payload includes ALL data columns from the target table.
            WITH source_cols AS (
                SELECT pa.attname
                FROM pg_catalog.pg_attribute pa
                WHERE pa.attrelid = source_table AND pa.attnum > 0 AND NOT pa.attisdropped
            ),
            target_cols AS (
                SELECT pa.attname, pa.attgenerated
                FROM pg_catalog.pg_attribute pa
                WHERE pa.attrelid = target_table AND pa.attnum > 0 AND NOT pa.attisdropped
            ),
            source_data_cols AS (
                SELECT s.attname
                FROM source_cols s JOIN target_cols t ON s.attname = t.attname
                WHERE s.attname NOT IN (row_id_column, 'era_id', 'era_name')
                AND s.attname <> ALL(v_temporal_cols) -- Exclude all temporal columns (range, from, until, to)
                AND s.attname <> ALL(v_original_entity_segment_key_cols)
                AND s.attname <> ALL(v_ephemeral_columns)
            ),
            source_ephemeral_cols AS (
                SELECT s.attname
                FROM source_cols s JOIN target_cols t ON s.attname = t.attname
                WHERE s.attname = ANY(v_ephemeral_columns)
            ),
            target_data_cols AS (
                SELECT t.attname
                FROM target_cols t
                WHERE t.attname NOT IN ('era_id', 'era_name')
                AND t.attname <> ALL(v_temporal_cols) -- Exclude all temporal columns (range, from, until, to)
                AND t.attname <> ALL(v_original_entity_segment_key_cols)
                AND t.attname <> ALL(v_ephemeral_columns)
                AND t.attgenerated = '' -- Exclude generated columns
            ),
            target_ephemeral_cols AS (
                SELECT t.attname
                FROM target_cols t
                WHERE t.attname = ANY(v_ephemeral_columns)
                AND t.attgenerated = '' -- Exclude generated columns
            )
            SELECT
                -- Smart data payload construction using jsonb manipulation
                -- First build the full object, then we'll strip NULL DEFAULT columns in the query
                (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(
                    format('%L, source_table.%I', attname, attname), ', '
                ) FILTER (WHERE attname <> ALL(COALESCE(v_always_exclude_columns, '{}'))), '')) FROM source_data_cols),
                -- Smart ephemeral payload construction with same approach
                (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(
                    format('%L, source_table.%I', attname, attname), ', '
                ) FILTER (WHERE attname <> ALL(COALESCE(v_always_exclude_columns, '{}'))), '')) FROM source_ephemeral_cols),
                (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, t.%I', attname, attname), ', '), '')) FROM target_data_cols),
                (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, %I', attname, attname), ', '), '')) FROM target_data_cols),
                (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, t.%I', attname, attname), ', '), '')) FROM target_ephemeral_cols),
                (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, %I', attname, attname), ', '), '')) FROM target_ephemeral_cols)
            INTO
                v_source_data_cols_jsonb_build,
                v_source_ephemeral_cols_jsonb_build,
                v_target_data_cols_jsonb_build,
                v_target_data_cols_jsonb_build_bare,
                v_target_ephemeral_cols_jsonb_build,
                v_target_ephemeral_cols_jsonb_build_bare;
    
            v_source_data_cols_jsonb_build := COALESCE(v_source_data_cols_jsonb_build, '''{}''::jsonb');
            v_source_ephemeral_cols_jsonb_build := COALESCE(v_source_ephemeral_cols_jsonb_build, '''{}''::jsonb');
            v_target_data_cols_jsonb_build := COALESCE(v_target_data_cols_jsonb_build, '''{}''::jsonb');
            v_target_ephemeral_cols_jsonb_build := COALESCE(v_target_ephemeral_cols_jsonb_build, '''{}''::jsonb');
            v_target_data_cols_jsonb_build_bare := COALESCE(v_target_data_cols_jsonb_build_bare, '''{}''::jsonb');
            v_target_ephemeral_cols_jsonb_build_bare := COALESCE(v_target_ephemeral_cols_jsonb_build_bare, '''{}''::jsonb');
    
            -- Phase 3.5: Comprehensive Column Classification (MOVED BEFORE payload construction)
            -- Classify all target columns to enable intelligent payload construction
            WITH column_classifications AS (
                SELECT 
                    a.attname,
                    a.atthasdef,
                    a.attidentity,
                    a.attgenerated,
                    a.attnotnull,
                    CASE 
                        WHEN a.attidentity IN ('a', 'd') THEN 'IDENTITY'
                        WHEN a.attgenerated <> '' THEN 'GENERATED'  
                        WHEN a.attnotnull AND a.atthasdef THEN 'NOT_NULL_DEFAULT'
                        WHEN NOT a.attnotnull AND a.atthasdef THEN 'NULLABLE_DEFAULT'
                        WHEN a.attnotnull AND NOT a.atthasdef THEN 'NOT_NULL_NO_DEFAULT'
                        ELSE 'REGULAR'
                    END as column_class
                FROM pg_catalog.pg_attribute a
                WHERE a.attrelid = target_table 
                  AND a.attnum > 0 
                  AND NOT a.attisdropped
            )
            SELECT 
                COALESCE(array_agg(attname) FILTER (WHERE column_class = 'IDENTITY'), '{}'),
                COALESCE(array_agg(attname) FILTER (WHERE column_class = 'GENERATED'), '{}'),
                COALESCE(array_agg(attname) FILTER (WHERE column_class = 'NOT_NULL_DEFAULT'), '{}'),
                COALESCE(array_agg(attname) FILTER (WHERE column_class = 'NULLABLE_DEFAULT'), '{}'),
                COALESCE(array_agg(attname) FILTER (WHERE column_class IN ('IDENTITY', 'GENERATED')), '{}'),
                COALESCE(array_agg(attname) FILTER (WHERE column_class = 'NOT_NULL_NO_DEFAULT'), '{}')
            INTO 
                v_identity_columns_classified, v_generated_columns, 
                v_not_null_default_columns, v_nullable_default_columns,
                v_always_exclude_columns, v_not_null_no_default_columns
            FROM column_classifications;

            -- Build a jsonb object of the stable identity columns. These are the keys
            -- that must be preserved across an entity's timeline.
            SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, t.%I', col, col), ', '), ''))
            INTO v_stable_pk_cols_jsonb_build
            FROM unnest(v_identity_columns) col;
    
            SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, %I', col, col), ', '), ''))
            INTO v_stable_pk_cols_jsonb_build_bare
            FROM unnest(v_identity_columns) col;
    
            -- DEBUG: Temporary validation of column classifications
            IF cardinality(v_identity_columns_classified) > 0 THEN
                RAISE DEBUG 'Identity columns detected: %', v_identity_columns_classified;
            END IF;
            IF cardinality(v_generated_columns) > 0 THEN
                RAISE DEBUG 'Generated columns detected: %', v_generated_columns;
            END IF;
            IF cardinality(v_not_null_default_columns) > 0 THEN
                RAISE DEBUG 'NOT NULL + DEFAULT columns detected: %', v_not_null_default_columns;
            END IF;
    
            -- Build source-side stable PK payload. It must project NULL for columns not present in the source.
            -- Use WITH ORDINALITY to preserve array order and ensure deterministic output.
            SELECT COALESCE(format('jsonb_build_object(%s)', string_agg(
                CASE
                    WHEN sa.attname IS NOT NULL THEN format('%L, source_table.%I', c.col, c.col)
                    ELSE format('%L, NULL::%s', c.col, format_type(ta.atttypid, ta.atttypmod))
                END,
            ', ' ORDER BY c.ord)), '{}'::jsonb::text)
            INTO v_stable_pk_cols_jsonb_build_source
            FROM unnest(COALESCE(v_identity_columns, '{}')) WITH ORDINALITY AS c(col, ord)
            LEFT JOIN pg_attribute ta ON ta.attrelid = target_table AND ta.attname = c.col AND ta.attnum > 0 AND NOT ta.attisdropped
            LEFT JOIN pg_attribute sa ON sa.attrelid = source_table AND sa.attname = c.col AND sa.attnum > 0 AND NOT sa.attisdropped;
    
            -- Construct an expression for the source table to check if all stable identity columns are NULL.
            -- If a column doesn't exist in the source, it's treated as NULL.
            SELECT
                COALESCE(string_agg(
                    CASE
                        WHEN a.attname IS NULL THEN 'true' -- Column doesn't exist, so it's conceptually NULL.
                        ELSE format('source_table.%I IS NULL', c.col)
                    END,
                ' AND ' ORDER BY c.ord), 'true')
            INTO
                v_entity_id_check_is_null_expr
            FROM unnest(v_identity_columns) WITH ORDINALITY AS c(col, ord)
            LEFT JOIN pg_attribute a ON a.attrelid = temporal_merge_plan.source_table AND a.attname = c.col AND a.attnum > 0 AND NOT a.attisdropped;
    
            -- Determine the scope of target entities to process based on the mode.
            -- Determine the set of keys to use for filtering the target table.
            -- Prioritize natural keys if provided; otherwise, fall back to the stable primary key.
            v_keys_for_filtering := lookup_keys;
            IF v_keys_for_filtering IS NULL OR jsonb_array_length(v_keys_for_filtering) = 0 THEN
                v_keys_for_filtering := jsonb_build_array(to_jsonb(identity_columns));
            END IF;
    
            IF mode IN ('MERGE_ENTITY_PATCH', 'MERGE_ENTITY_REPLACE') AND delete_mode IN ('DELETE_MISSING_ENTITIES', 'DELETE_MISSING_TIMELINE_AND_ENTITIES') THEN
                -- For modes that might delete entities not in the source, we must scan the entire target table.
                v_target_rows_filter := v_target_table_ident || ' t';
            ELSE
                -- By default, we optimize by only scanning target entities that are present in the source.
                -- This is a critical performance optimization. For each natural key provided, we generate
                -- the most efficient query to filter the target table based on its nullability, and then
                -- UNION the results.
                DECLARE
                    v_union_parts TEXT[];
                    v_key_cols TEXT[];
                    v_key JSONB;
                    v_distinct_on_cols name[];
                    v_distinct_on_cols_list TEXT;
                BEGIN
                    v_union_parts := ARRAY[]::TEXT[];
                    -- When a table contains columns without a default b-tree equality operator (e.g., `point`),
                    -- `SELECT DISTINCT *` can fail. We must fall back to `SELECT DISTINCT ON (...) *`.
                    -- The key for DISTINCT ON must uniquely identify rows in the target table. For a temporal table,
                    -- this is the combination of its stable entity identifier and the temporal start column.
                    -- For range-only tables (no boundary columns), use the range column itself.
                    v_distinct_on_cols := v_identity_columns || COALESCE(v_valid_from_col, v_range_col);
                    v_distinct_on_cols_list := (SELECT string_agg(format('u.%I', col), ', ') FROM unnest(v_distinct_on_cols) AS col);
    
                    FOR v_key IN SELECT * FROM jsonb_array_elements(v_keys_for_filtering)
                    LOOP
                        SELECT array_agg(value) INTO v_key_cols FROM jsonb_array_elements_text(v_key);
                        -- This logic is a simplified version of the single-key optimization path.
                        -- It's applied once for each natural key.
                        DECLARE
                            v_any_nullable_lookup_cols BOOLEAN;
                            v_lookup_cols_si_alias_select_list TEXT;
                        BEGIN
                            -- OPT-3: Use cached metadata instead of querying pg_attribute
                            SELECT bool_or(NOT (v_lookup_cols_meta->col->>'notnull')::boolean)
                            INTO v_any_nullable_lookup_cols
                            FROM unnest(v_key_cols) AS col;
                            v_any_nullable_lookup_cols := COALESCE(v_any_nullable_lookup_cols, false);
    
                            -- This is the crucial fix. The subquery must project the columns for the
                            -- current key, and NULL for all columns belonging to OTHER natural keys
                            -- to ensure the UNION branches have the same structure.
                            -- OPT-3: Use cached metadata instead of querying pg_attribute
                            v_lookup_cols_si_alias_select_list := (
                                SELECT string_agg(
                                    CASE
                                        WHEN c.col = ANY(v_key_cols) THEN format('si.%I', c.col)
                                        ELSE format('NULL::%s', v_lookup_cols_meta->c.col->>'type')
                                    END,
                                ', ')
                                FROM unnest(v_lookup_columns) AS c(col)
                            );
    
                            IF NOT v_any_nullable_lookup_cols THEN
                                v_union_parts := v_union_parts || format($$(
                                    SELECT * FROM %1$s inner_t
                                    WHERE (%2$s) IN (SELECT DISTINCT %3$s FROM source_initial si WHERE (%4$s) IS NOT NULL)
                                )$$, v_target_table_ident,
                                    (SELECT string_agg(format('inner_t.%I', col), ', ') FROM unnest(v_key_cols) col),
                                    (SELECT string_agg(format('si.%I', col), ', ') FROM unnest(v_key_cols) col),
                                    (SELECT string_agg(format('si.%I', col), ', ') FROM unnest(v_key_cols) col)
                                );
                            ELSE
                                -- Nullable columns in the key require special handling.
                                -- The naive NULL-safe join `(si.col = t.col OR (si.col IS NULL AND t.col IS NULL))`
                                -- causes O(n²) bitmap scans because PostgreSQL can't optimize the OR condition.
                                --
                                -- Solution: Partition by NULL pattern and use simple equality joins within each partition.
                                -- For each combination of {NULL, NOT NULL} for nullable columns, we generate a separate
                                -- subquery with explicit IS NULL / IS NOT NULL conditions, allowing index scans.
                                DECLARE
                                    v_nullable_cols name[];
                                    v_notnull_cols name[];
                                    v_partition_parts TEXT[];
                                    v_col name;
                                    v_null_condition TEXT;
                                    v_notnull_condition TEXT;
                                    v_join_cols TEXT;
                                BEGIN
                                    -- OPT-3: Use cached metadata instead of querying pg_attribute (2 queries -> 0)
                                    SELECT 
                                        COALESCE(array_agg(c) FILTER (WHERE NOT (v_lookup_cols_meta->c->>'notnull')::boolean), ARRAY[]::name[]),
                                        COALESCE(array_agg(c) FILTER (WHERE (v_lookup_cols_meta->c->>'notnull')::boolean), ARRAY[]::name[])
                                    INTO v_nullable_cols, v_notnull_cols
                                    FROM unnest(v_key_cols) AS c;
                                    
                                    v_partition_parts := ARRAY[]::TEXT[];
                                    
                                    -- For each nullable column, generate two partitions: one for NULL, one for NOT NULL
                                    -- This handles the common case of mutually exclusive nullable columns (XOR pattern)
                                    FOREACH v_col IN ARRAY v_nullable_cols LOOP
                                        -- Partition where this column IS NOT NULL (use equality join)
                                        v_notnull_condition := format('inner_t.%I IS NOT NULL', v_col);
                                        v_join_cols := (
                                            SELECT string_agg(format('si.%1$I = inner_t.%1$I', c), ' AND ')
                                            FROM unnest(v_notnull_cols || v_col) c
                                        );
                                        v_partition_parts := v_partition_parts || format($$(
                                            SELECT inner_t.*
                                            FROM %1$s inner_t
                                            WHERE %2$s
                                              AND (%3$s) IN (
                                                SELECT %4$s FROM source_initial si WHERE si.%5$I IS NOT NULL
                                              )
                                        )$$,
                                            v_target_table_ident,
                                            v_notnull_condition,
                                            (SELECT string_agg(format('inner_t.%I', c), ', ') FROM unnest(v_notnull_cols || v_col) c),
                                            (SELECT string_agg(format('si.%I', c), ', ') FROM unnest(v_notnull_cols || v_col) c),
                                            v_col
                                        );
                                    END LOOP;
                                    
                                    -- If there are no nullable columns, fall back to simple IN clause
                                    IF cardinality(v_nullable_cols) = 0 THEN
                                        v_partition_parts := ARRAY[format($$(
                                            SELECT * FROM %1$s inner_t
                                            WHERE (%2$s) IN (SELECT %3$s FROM source_initial si)
                                        )$$, 
                                            v_target_table_ident,
                                            (SELECT string_agg(format('inner_t.%I', col), ', ') FROM unnest(v_key_cols) col),
                                            (SELECT string_agg(format('si.%I', col), ', ') FROM unnest(v_key_cols) col)
                                        )];
                                    END IF;
                                    
                                    v_union_parts := v_union_parts || format($$(
                                        SELECT DISTINCT ON (%2$s) * FROM (
                                            %1$s
                                        ) partitioned
                                    )$$, 
                                        array_to_string(v_partition_parts, ' UNION ALL '),
                                        (SELECT string_agg(format('partitioned.%I', col), ', ') FROM unnest(v_distinct_on_cols) AS col)
                                    );
                                END; -- DECLARE
                            END IF;
                        END; -- DECLARE
                    END LOOP;
    
                    -- The final "filter" is a full subquery that replaces the original FROM clause for target_rows.
                    v_target_rows_filter := format($$(
                        SELECT * FROM (
                            SELECT DISTINCT ON (%2$s) * FROM (
                                %1$s
                            ) u
                        )
                    ) AS t $$, array_to_string(v_union_parts, ' UNION ALL '), v_distinct_on_cols_list);
                END;
            END IF;
    
            -- A row is "identifiable" if it provides enough information to uniquely associate it with a conceptual entity.
            -- This logic depends on the merge strategy.
            --
            -- The base logic is: `NOT (stable_key_is_null AND natural_key_is_null)`.
            -- A row is unidentifiable only if it has NO key information at all.
            --
            -- How NULLs are interpreted:
            -- - "natural_key_is_null": This is true if *all* natural key columns are NULL in the source row.
            --   If no natural keys are defined for the merge, this is also true, meaning the planner
            --   correctly relies solely on the stable key.
            -- - "stable_key_is_null": This is true if *all* stable identity columns are NULL in the source row.
            --
            -- Strategy-specific overrides:
            -- - `STRATEGY_STABLE_KEY_ONLY`: A row with a NULL stable key is a valid `INSERT` request.
            --   The row is identified by its `corr_ent` within the batch. Thus, it's always identifiable.
            -- - `founding_id_column` mode: A row is always identifiable, as the `founding_id`
            --   provides the necessary grouping key for new entities.
            v_is_identifiable_expr := format('NOT ((%s) AND (%s))', v_entity_id_check_is_null_expr, v_lookup_cols_are_null_expr);
            IF v_is_founding_mode OR v_constellation = 'STRATEGY_IDENTITY_KEY_ONLY' THEN
                v_is_identifiable_expr := 'true';
            END IF;
    
            -- This logic is structured to be orthogonal:
            -- 1. Payload Handling (_PATCH vs. _REPLACE) is determined first.
            -- 2. Timeline Handling (destructive vs. non-destructive) is determined by delete_mode.
    
            -- First, determine payload semantics based on the mode name.
            IF mode IN ('MERGE_ENTITY_PATCH', 'PATCH_FOR_PORTION_OF') THEN
                -- PATCH modes: Strip NULLs from source payload. NULL values in source are ignored.
                v_source_data_payload_expr := format('jsonb_strip_nulls(%s)', v_source_data_cols_jsonb_build);
                v_source_ephemeral_payload_expr := format('jsonb_strip_nulls(%s)', v_source_ephemeral_cols_jsonb_build);
            ELSIF mode = 'DELETE_FOR_PORTION_OF' THEN
                v_source_data_payload_expr := '''"__DELETE__"''::jsonb';
                v_source_ephemeral_payload_expr := v_source_ephemeral_cols_jsonb_build;
            ELSE -- MERGE_ENTITY_REPLACE, MERGE_ENTITY_UPSERT, REPLACE_FOR_PORTION_OF, UPDATE_FOR_PORTION_OF, INSERT_NEW_ENTITIES
                -- UPDATE/REPLACE modes: Smart payload handling for DEFAULT and NOT NULL columns
                -- Build expressions that exclude NULL values for:
                -- 1. Columns with defaults (use the default)
                -- 2. NOT NULL columns without defaults (avoid constraint violation)
                DECLARE 
                    v_exclude_if_null_cols TEXT[];
                    v_source_columns TEXT[];
                    v_col TEXT;
                    v_data_strip_expr TEXT;
                    v_ephemeral_strip_expr TEXT;
                BEGIN
                    -- Get list of columns that exist in the source
                    SELECT array_agg(attname)
                    INTO v_source_columns
                    FROM pg_catalog.pg_attribute
                    WHERE attrelid = source_table
                      AND attnum > 0
                      AND NOT attisdropped;
                    
                    -- Filter to only columns that exist in both source and target
                    -- and should be excluded if NULL
                    v_exclude_if_null_cols := ARRAY(
                        SELECT unnest(v_not_null_default_columns || v_nullable_default_columns || v_not_null_no_default_columns)
                        INTERSECT
                        SELECT unnest(v_source_columns)
                    );
                    
                    IF cardinality(v_exclude_if_null_cols) > 0 THEN
                        -- Start with the base expression
                        v_data_strip_expr := v_source_data_cols_jsonb_build;
                        v_ephemeral_strip_expr := v_source_ephemeral_cols_jsonb_build;
                        
                        -- For each column that should be excluded if NULL, wrap with a CASE to remove it
                        FOREACH v_col IN ARRAY v_exclude_if_null_cols
                        LOOP
                            v_data_strip_expr := format(
                                $$(CASE WHEN source_table.%I IS NULL THEN (%s) - %L ELSE (%s) END)$$,
                                v_col, v_data_strip_expr, v_col, v_data_strip_expr
                            );
                            v_ephemeral_strip_expr := format(
                                $$(CASE WHEN source_table.%I IS NULL THEN (%s) - %L ELSE (%s) END)$$,
                                v_col, v_ephemeral_strip_expr, v_col, v_ephemeral_strip_expr
                            );
                        END LOOP;
                        
                        v_source_data_payload_expr := v_data_strip_expr;
                        v_source_ephemeral_payload_expr := v_ephemeral_strip_expr;
                    ELSE
                        -- No columns to exclude, use original behavior
                        v_source_data_payload_expr := v_source_data_cols_jsonb_build;
                        v_source_ephemeral_payload_expr := v_source_ephemeral_cols_jsonb_build;
                    END IF;
                END;
            END IF;
    
            -- Second, determine timeline semantics. The default is a non-destructive merge where we
            -- The join condition for the diff links the final state back to the original target row
            -- it was derived from. The join is on the stable identity columns and the original `valid_from`
            -- of the target row, which is preserved as `ancestor_valid_from`.
            -- The join must be on ALL identity columns to correctly match final state rows to their target ancestors,
            -- especially when dealing with composite natural keys.
            IF v_original_entity_segment_key_cols IS NULL OR cardinality(v_original_entity_segment_key_cols) = 0 THEN
                -- This should be prevented by the check in temporal_merge, but as a safeguard:
                RAISE EXCEPTION 'temporal_merge_plan requires at least one identity column. None found or provided for table %.', target_table;
            END IF;
            -- The `diff` CTE's join condition links a final, coalesced segment back to the original target row it was
            -- derived from. The join condition must use the full unique key of the historical row,
            -- which includes temporal columns if they are part of a composite primary key.
            -- The `diff` CTE's join condition links a final, coalesced segment back to the original target row it was
            -- derived from. The join must be on the non-temporal entity identifier and the start of the original
            -- target row's validity period, which is preserved as `ancestor_valid_from`.
            v_diff_join_condition := format(
                $$final_seg.grouping_key = ('existing_entity__' || %s) AND final_seg.ancestor_valid_from = target_seg.valid_from$$,
                (
                    SELECT COALESCE(string_agg(format('COALESCE(target_seg.%I::text, ''_NULL_'')', col), ' || ''__'' || '), '''''')
                    FROM unnest(v_identity_columns) AS t(col)
                )
            );
    
            -- Third, determine payload resolution logic based on the mode. All modes are now stateless.
            IF mode IN ('MERGE_ENTITY_PATCH', 'PATCH_FOR_PORTION_OF', 'MERGE_ENTITY_UPSERT', 'UPDATE_FOR_PORTION_OF') THEN
                -- In PATCH and UPSERT modes, we do a stateless partial update.
                -- The difference is handled by v_source_data_payload_expr, which strips NULLs for PATCH.
                v_resolver_ctes := '';
                v_resolver_from := 'resolved_atomic_segments_with_payloads';
                v_final_data_payload_expr := $$COALESCE(t_data_payload, '{}'::jsonb) || COALESCE(s_data_payload, '{}'::jsonb)$$;
            ELSE -- REPLACE and DELETE modes
                -- In REPLACE/DELETE modes, we do not inherit. Gaps remain gaps.
                v_resolver_ctes := '';
                v_resolver_from := 'resolved_atomic_segments_with_payloads';
                v_final_data_payload_expr := $$CASE WHEN s_data_payload = '"__DELETE__"'::jsonb THEN NULL ELSE COALESCE(s_data_payload, t_data_payload) END$$;
            END IF;
    
            -- For destructive timeline mode, the final payload is always the source payload. This overrides any complex logic from above.
            IF delete_mode IN ('DELETE_MISSING_TIMELINE', 'DELETE_MISSING_TIMELINE_AND_ENTITIES') THEN
                v_resolver_ctes := '';
                v_resolver_from := 'resolved_atomic_segments_with_payloads';
                v_final_data_payload_expr := 's_data_payload';
            END IF;
    
            -- Layer on optional destructive delete modes for missing entities.
            IF delete_mode IN ('DELETE_MISSING_ENTITIES', 'DELETE_MISSING_TIMELINE_AND_ENTITIES') THEN
                -- We need to add a flag to identify entities that are present in the source.
                -- This CTE is chained onto any previous resolver CTEs by using v_resolver_from.
                v_resolver_ctes := v_resolver_ctes || format($$
        resolved_atomic_segments_with_flag AS (
            SELECT *,
                bool_or(s_data_payload IS NOT NULL) OVER (PARTITION BY grouping_key) as entity_is_in_source
            FROM %s
        ),
    $$, v_resolver_from);
                v_resolver_from := 'resolved_atomic_segments_with_flag';
    
                -- If deleting missing entities, the final payload for those entities must be NULL.
                -- For entities present in the source, the existing v_final_data_payload_expr is correct.
                v_final_data_payload_expr := format($$CASE WHEN entity_is_in_source THEN (%s) ELSE NULL END$$, v_final_data_payload_expr);
            END IF;
    
            -- Pre-calculate all expressions to be passed to format() for easier debugging.
            v_target_rows_lookup_cols_expr := (SELECT string_agg(format('t.%I', col), ', ') FROM unnest(v_lookup_columns) as col);
            -- Build the join expression for source_rows_with_matches.
            -- For NULLABLE columns, we MUST use the null-safe pattern (a = b OR (a IS NULL AND b IS NULL))
            -- because NULL = NULL evaluates to NULL (not TRUE), which would fail the join.
            -- For NOT NULL columns, we can use simple = for better query plan performance (Hash Joins).
            v_source_rows_exists_join_expr := (
                SELECT string_agg(
                    format('((%s))', (
                        SELECT string_agg(
                            CASE
                                WHEN a.attnotnull THEN
                                    -- NOT NULL column: safe to use simple =
                                    format('source_row.%1$I = target_row.%2$I', c, CASE WHEN c = v_valid_from_col THEN 'valid_from'::name WHEN c = v_valid_until_col THEN 'valid_until'::name ELSE c END)
                                ELSE
                                    -- NULLABLE column: must use null-safe comparison
                                    format('(source_row.%1$I = target_row.%2$I OR (source_row.%1$I IS NULL AND target_row.%2$I IS NULL))', c, CASE WHEN c = v_valid_from_col THEN 'valid_from'::name WHEN c = v_valid_until_col THEN 'valid_until'::name ELSE c END)
                            END,
                            ' AND ' ORDER BY k.ord
                        )
                        FROM jsonb_array_elements_text(v_key) WITH ORDINALITY AS k(c, ord)
                        LEFT JOIN pg_attribute a ON a.attrelid = target_table AND a.attname = k.c AND NOT a.attisdropped
                    )),
                ' OR ' ORDER BY lk.ord)
                FROM jsonb_array_elements(lookup_keys) WITH ORDINALITY AS lk(v_key, ord)
            );

            IF v_source_rows_exists_join_expr IS NULL THEN
                -- Fallback to identity columns when no lookup keys provided
                v_source_rows_exists_join_expr := (
                    SELECT string_agg(
                        CASE
                            WHEN a.attnotnull THEN
                                format('source_row.%1$I = target_row.%2$I', col, CASE WHEN col = v_valid_from_col THEN 'valid_from'::name WHEN col = v_valid_until_col THEN 'valid_until'::name ELSE col END)
                            ELSE
                                format('(source_row.%1$I = target_row.%2$I OR (source_row.%1$I IS NULL AND target_row.%2$I IS NULL))', col, CASE WHEN col = v_valid_from_col THEN 'valid_from'::name WHEN col = v_valid_until_col THEN 'valid_until'::name ELSE col END)
                        END,
                        ' AND ' ORDER BY ic.ord
                    )
                    FROM unnest(v_identity_columns) WITH ORDINALITY AS ic(col, ord)
                    LEFT JOIN pg_attribute a ON a.attrelid = target_table AND a.attname = ic.col AND NOT a.attisdropped
                );
            END IF;
    
            v_source_rows_exists_join_expr := COALESCE(v_source_rows_exists_join_expr, 'false');
            v_atomic_segments_select_list_expr := (SELECT COALESCE(string_agg(format('seg.%I', col), ', ') || ',', '') || ' seg.corr_ent' FROM unnest(v_lookup_columns) as col);
            -- The `diff` CTE's SELECT list must project all identifying columns to be used in later CTEs.
            -- This requires mapping from the internal, standardized temporal column names (`valid_from`) back
            -- to their original, user-defined names (`"from"`). The `CASE` statement handles the lookup from
            -- the internal CTEs, while the second `format()` parameter ensures the projected column is
            -- correctly aliased back to its original name for downstream CTEs.
            v_diff_select_expr := (
                SELECT string_agg(
                    format('COALESCE(final_seg.%1$I, target_seg.%1$I) as %2$I',
                        -- Use the internal, standardized name for the COALESCE source.
                        CASE
                            WHEN col = v_valid_from_col THEN 'valid_from'::name
                            WHEN col = v_valid_until_col THEN 'valid_until'::name
                            ELSE col
                        END,
                        -- Use the original, user-defined name for the output alias.
                        col
                    ),
                ', ')
                FROM unnest(v_original_entity_segment_key_cols) as col
            ) || ', COALESCE(final_seg.causal_id, target_seg.causal_id) as causal_id';
            -- The `entity_keys` payload must be built *only* from the stable, non-temporal identifier columns.
            v_plan_with_op_entity_id_json_build_expr_part_A := (SELECT COALESCE(string_agg(format('%L, d.%I', col, col), ', '), '') FROM unnest(v_original_entity_key_cols) as col WHERE col IS NOT NULL);
            v_plan_with_op_entity_id_json_build_expr := format('jsonb_build_object(%s) || COALESCE(d.stable_pk_payload, ''{}''::jsonb)', v_plan_with_op_entity_id_json_build_expr_part_A);
            v_skip_no_target_entity_id_json_build_expr := (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, source_row.%I', col, col), ', '), '')) FROM unnest(v_original_entity_key_cols) as col WHERE col IS NOT NULL);
            -- The final sort order should be on the stable entity identifier.
            v_final_order_by_expr := (SELECT string_agg(format('p.%I', col), ', ') FROM unnest(v_original_entity_key_cols) as col WHERE col IS NOT NULL);
            v_entity_key_for_with_base_payload_expr := (
                SELECT COALESCE(string_agg(format('seg.%I', col), ', ') || ',', '') || ' seg.causal_id'
                FROM unnest(v_original_entity_segment_key_cols) as col
                WHERE col.col <> ALL(v_temporal_cols)
            );
            v_s_founding_join_condition := (SELECT string_agg(format('(target_row.%1$I = source_row.%1$I OR (target_row.%1$I IS NULL AND source_row.%1$I IS NULL))', col), ' AND ') FROM unnest(v_lookup_columns) as col);
            v_tr_qualified_lookup_cols := (SELECT string_agg(format('target_row.%I', col), ', ') FROM unnest(v_lookup_columns) as col);
    
            -- Build the null-safe join condition for the recursive CTE.
            v_rcte_join_condition := (
                SELECT string_agg(format('(c.%1$I = r.%1$I OR (c.%1$I IS NULL AND r.%1$I IS NULL))', col), ' AND ')
                FROM unnest(v_lookup_columns) as col
            );
            v_rcte_join_condition := COALESCE(v_rcte_join_condition, 'true');
            -- The causal_id must also match. This is critical for new entities that share a NULL lookup key.
            v_rcte_join_condition := format('(%s) AND (c.causal_id = r.causal_id OR (c.causal_id IS NULL and r.causal_id IS NULL))', v_rcte_join_condition);
    
            -- The `grouping_key` is a robust, composite identifier used in all window functions to group
            -- together all time points and rows that belong to a single conceptual entity. It solves a
            -- critical flaw where the simpler `causal_id` was insufficient.
            --
            -- Problem with `causal_id`: For new entities, `causal_id` was based on the source `row_id`.
            -- If multiple source rows described the same new entity (e.g., with the same natural key),
            -- they would get different `causal_id` values, causing the planner to incorrectly treat them
            -- as separate entities, creating fragmented timelines.
            --
            -- Solution with `grouping_key`:
            -- - For existing entities, the key is built from the stable identity columns (e.g., 'existing_entity__123').
            -- - For new entities, the key is built from the natural key columns (e.g., 'new_entity__E104'). This correctly
            --   groups all source rows for the new entity 'E104' together.
            --
            -- Example: Two source rows for a new employee 'E104' both get the grouping key 'new_entity__E104',
            -- allowing their timelines to be correctly constructed as a single history. `causal_id` alone would have failed.
            v_grouping_key_expr_for_union := format(
                $$CASE
                    WHEN source_row.is_new_entity
                    THEN 'new_entity__' || %2$s
                    ELSE 'existing_entity__' || %1$s
                END$$,
                (SELECT string_agg(format('COALESCE(source_row.%I::text, ''_NULL_'')', col), ' || ''__'' || ') FROM unnest(v_identity_columns) AS t(col)),
                COALESCE(
                    (SELECT string_agg(format('COALESCE(source_row.%I::text, ''_NULL_'')', col), ' || ''__'' || ') FROM unnest(v_all_lookup_cols) AS col),
                    'source_row.causal_id::text'
                )
            );
    
            v_plan_select_key_cols := (SELECT string_agg(format('p.%I', col), ', ') FROM unnest(v_original_entity_key_cols) AS col WHERE col IS NOT NULL);
    
            -- The trace is architecturally permanent but toggled by a GUC that controls
            -- the initial seed.
            /*
            * Performance Note on Tracing Overhead:
            * This implementation is designed to have near-zero performance impact when
            * tracing is disabled. Two patterns are used to achieve this:
            *
            * 1. `NULL || jsonb_build_object(...)`: When `p_log_trace` is false, the
            *    trace seed is set to `NULL::jsonb`. Subsequent trace-building operations
            *    use the pattern `trace || jsonb_build_object(...)`.
            *    As demonstrated by the runnable example below, this
            *    pattern is short-circuited by the PostgreSQL query planner. Because the
            *    `jsonb ||` operator is "strict" (returns `NULL` on `NULL` input), the
            *    planner knows `NULL || anything` will be `NULL` and therefore does not
            *    execute the expensive `jsonb_build_object` calls on the right-hand side.
            *
            *    -- To demonstrate, run in psql:
            *    -- \timing on
            *
            *    -- 1. With a non-NULL left-hand side, the right side is evaluated.
            *    --    The query will pause for ~2 seconds.
            *    SELECT '{}'::jsonb || jsonb_build_object('sleep', pg_sleep(2));
            *
            *    -- 2. With a NULL left-hand side, the planner short-circuits.
            *    --    pg_sleep() is never called, and the query is instantaneous.
            *    SELECT NULL::jsonb || jsonb_build_object('sleep', pg_sleep(2));
            *
            *    -- \timing off
            *
            * 2. `CASE WHEN p_log_trace THEN ... ELSE NULL END`: In other parts of the
            *    planner, an explicit `CASE` statement is used. This is a fundamental
            *    control-flow structure that also guarantees the expensive trace-building
            *    logic is skipped when the GUC is disabled.
            *
            * Both approaches ensure that the architectural decision to have a permanent
            * `trace` column incurs no runtime overhead when tracing is not active.
            */
            IF p_log_trace THEN
                -- Trace seed captures the initial state of each atomic segment with clear naming:
                -- - lookup_key_values: the key columns used to match source to target (not the grouping_key string)
                -- - target_stable_pk: the stable PK from the matched target row (if any)
                -- - segment_stable_pk: the propagated stable PK for this segment
                -- - seg_valid_from/until: the segment's temporal boundaries
                -- - s_valid_from/until: the source row's original temporal boundaries
                -- - t_valid_from/until: the target row's temporal boundaries (if matched)
                -- Note: source_row_id removed as redundant with contributing_row_ids[1]
                v_trace_seed_expr := format($$jsonb_build_object('cte', 'resolved_atomic_segments_with_payloads', 'contributing_row_ids', source_payloads.contributing_row_ids, 'constellation', %4$L, 'lookup_key_values', jsonb_build_object(%1$s), 's_data', source_payloads.data_payload, 't_data', seg.t_data_payload, 's_ephemeral', source_payloads.ephemeral_payload, 't_ephemeral', seg.t_ephemeral_payload, 'seg_valid_from', seg.valid_from, 'seg_valid_until', seg.valid_until, 's_valid_from', source_payloads.valid_from, 's_valid_until', source_payloads.valid_until, 't_valid_from', seg.t_valid_from, 't_valid_until', seg.t_valid_until, 'orig_source_target_relation', sql_saga.get_allen_relation(source_payloads.valid_from, source_payloads.valid_until, seg.t_valid_from, seg.t_valid_until), 'target_stable_pk', seg.target_stable_pk_payload, 'segment_stable_pk', seg.stable_pk_payload, 'seg_is_new_entity', seg.is_new_entity, 'seg_stable_identity_columns_are_null', seg.stable_identity_columns_are_null, 'seg_lookup_columns_unavailable', seg.lookup_columns_unavailable, 'stable_identity_values', %2$s, 'natural_identity_values', %3$s, 'canonical_causal_id', seg.causal_id) as trace$$,
                    (SELECT string_agg(format('%L, seg.%I', col, col), ',') FROM unnest(v_lookup_columns) col),
                    v_identity_cols_trace_expr,
                    v_natural_identity_cols_trace_expr,
                    v_constellation
                );
            ELSE
                v_trace_seed_expr := 'NULL::jsonb as trace';
            END IF;
    
            -- This trace step is inside the coalescing logic. It duplicates the CASE
            -- statement for `is_new_segment` to show exactly what is being compared.
            v_trace_select_list := format($$
                trace || jsonb_build_object(
                    'cte', 'coalesce_check',
                    'causal_id', causal_id,
                    'is_new_segment_calc',
                    CASE
                        WHEN LAG(valid_until) OVER w = valid_from
                        AND (LAG(data_payload - %1$L::text[]) OVER w IS NOT DISTINCT FROM (data_payload - %1$L::text[]))
                        THEN 0
                        ELSE 1
                    END,
                    'current_payload_sans_ephemeral', data_payload - %1$L::text[],
                    'lag_payload_sans_ephemeral', LAG(data_payload - %1$L::text[]) OVER w,
                    'segment_stable_pk', propagated_stable_pk_payload
                ) AS coalesced_trace
            $$, v_ephemeral_columns);
    
            -- Construct the final data payload expression for the coalescing CTE.
            -- This combines the data and ephemeral payloads.
            v_coalesced_payload_expr := $$(sql_saga.first(data_payload ORDER BY valid_from DESC) || sql_saga.first(ephemeral_payload ORDER BY valid_from DESC))$$;
    
            -- If a `valid_to` column is synchronized, we must override its value in the payload
            -- to be consistent with the final calculated `valid_until` of the time slice.
            -- This MUST be done *after* the main data/ephemeral combination.
            IF v_valid_to_col IS NOT NULL THEN
                v_coalesced_payload_expr := format(
                    $$(%s) || jsonb_build_object(%L, to_jsonb(MAX(valid_until) - 1))$$,
                    v_coalesced_payload_expr,
                    v_valid_to_col
                );
            END IF;
            -- This CASE statement builds the correct LATERAL join for resolving source payloads based on the mode.
            -- This CASE statement builds the correct LATERAL join for resolving source payloads based on the mode.
            CASE mode
                WHEN 'MERGE_ENTITY_PATCH', 'PATCH_FOR_PORTION_OF' THEN
                    v_lateral_source_resolver_sql := format($$
                        LEFT JOIN LATERAL (
                            WITH RECURSIVE ordered_sources AS (
                                SELECT
                                    source_row.source_row_id, source_row.data_payload, source_row.ephemeral_payload,
                                    source_row.valid_from, source_row.valid_until, source_row.causal_id,
                                    row_number() OVER (ORDER BY source_row.source_row_id) as rn
                                FROM active_source_rows source_row
                                WHERE %1$s -- v_lateral_join_sr_to_seg
                                AND %2$I(seg.valid_from, seg.valid_until) <@ %2$I(source_row.valid_from, source_row.valid_until)
                            ),
                            running_payload AS (
                                SELECT rn, source_row_id, data_payload, ephemeral_payload, valid_from, valid_until, causal_id, ARRAY[source_row_id::BIGINT] as contributing_row_ids
                                FROM ordered_sources WHERE rn = 1
                                UNION ALL
                                SELECT
                                    s.rn, s.source_row_id,
                                    r.data_payload || jsonb_strip_nulls(s.data_payload),
                                    r.ephemeral_payload || jsonb_strip_nulls(s.ephemeral_payload),
                                    s.valid_from, s.valid_until, s.causal_id,
                                    r.contributing_row_ids || s.source_row_id::BIGINT
                                FROM running_payload r JOIN ordered_sources s ON s.rn = r.rn + 1
                            )
                            SELECT source_row_id, data_payload, ephemeral_payload, valid_from, valid_until, causal_id, contributing_row_ids
                            FROM running_payload
                            ORDER BY rn DESC
                            LIMIT 1
                        ) source_payloads ON true
                    $$, v_lateral_join_sr_to_seg, v_range_constructor);

                    v_lateral_source_resolver_sql_existing := format($$
                        LEFT JOIN LATERAL (
                            WITH RECURSIVE ordered_sources AS (
                                SELECT
                                    source_row.source_row_id, source_row.data_payload, source_row.ephemeral_payload,
                                    source_row.valid_from, source_row.valid_until, source_row.causal_id,
                                    row_number() OVER (ORDER BY source_row.source_row_id) as rn
                                FROM active_source_rows source_row
                                WHERE %1$s
                                AND %2$I(seg.valid_from, seg.valid_until) <@ %2$I(source_row.valid_from, source_row.valid_until)
                            ),
                            running_payload AS (
                                SELECT rn, source_row_id, data_payload, ephemeral_payload, valid_from, valid_until, causal_id, ARRAY[source_row_id::BIGINT] as contributing_row_ids
                                FROM ordered_sources WHERE rn = 1
                                UNION ALL
                                SELECT
                                    s.rn, s.source_row_id,
                                    r.data_payload || jsonb_strip_nulls(s.data_payload),
                                    r.ephemeral_payload || jsonb_strip_nulls(s.ephemeral_payload),
                                    s.valid_from, s.valid_until, s.causal_id,
                                    r.contributing_row_ids || s.source_row_id::BIGINT
                                FROM running_payload r JOIN ordered_sources s ON s.rn = r.rn + 1
                            )
                            SELECT source_row_id, data_payload, ephemeral_payload, valid_from, valid_until, causal_id, contributing_row_ids
                            FROM running_payload
                            ORDER BY rn DESC
                            LIMIT 1
                        ) source_payloads ON true
                    $$, v_lateral_join_sr_to_seg_existing, v_range_constructor);

                    v_lateral_source_resolver_sql_new := format($$
                        LEFT JOIN LATERAL (
                            WITH RECURSIVE ordered_sources AS (
                                SELECT
                                    source_row.source_row_id, source_row.data_payload, source_row.ephemeral_payload,
                                    source_row.valid_from, source_row.valid_until, source_row.causal_id,
                                    row_number() OVER (ORDER BY source_row.source_row_id) as rn
                                FROM active_source_rows source_row
                                WHERE %1$s
                                AND %2$I(seg.valid_from, seg.valid_until) <@ %2$I(source_row.valid_from, source_row.valid_until)
                            ),
                            running_payload AS (
                                SELECT rn, source_row_id, data_payload, ephemeral_payload, valid_from, valid_until, causal_id, ARRAY[source_row_id::BIGINT] as contributing_row_ids
                                FROM ordered_sources WHERE rn = 1
                                UNION ALL
                                SELECT
                                    s.rn, s.source_row_id,
                                    r.data_payload || jsonb_strip_nulls(s.data_payload),
                                    r.ephemeral_payload || jsonb_strip_nulls(s.ephemeral_payload),
                                    s.valid_from, s.valid_until, s.causal_id,
                                    r.contributing_row_ids || s.source_row_id::BIGINT
                                FROM running_payload r JOIN ordered_sources s ON s.rn = r.rn + 1
                            )
                            SELECT source_row_id, data_payload, ephemeral_payload, valid_from, valid_until, causal_id, contributing_row_ids
                            FROM running_payload
                            ORDER BY rn DESC
                            LIMIT 1
                        ) source_payloads ON true
                    $$, v_lateral_join_sr_to_seg_new, v_range_constructor);

                WHEN 'MERGE_ENTITY_UPSERT', 'UPDATE_FOR_PORTION_OF' THEN
                    v_lateral_source_resolver_sql := format($$
                        LEFT JOIN LATERAL (
                            WITH RECURSIVE ordered_sources AS (
                                SELECT
                                    source_row.source_row_id, source_row.data_payload, source_row.ephemeral_payload,
                                    source_row.valid_from, source_row.valid_until, source_row.causal_id,
                                    row_number() OVER (ORDER BY source_row.source_row_id) as rn
                                FROM active_source_rows source_row
                                WHERE %1$s -- v_lateral_join_sr_to_seg
                                AND %2$I(seg.valid_from, seg.valid_until) <@ %2$I(source_row.valid_from, source_row.valid_until)
                            ),
                            running_payload AS (
                                SELECT rn, source_row_id, data_payload, ephemeral_payload, valid_from, valid_until, causal_id, ARRAY[source_row_id::BIGINT] as contributing_row_ids
                                FROM ordered_sources WHERE rn = 1
                                UNION ALL
                                SELECT
                                    s.rn, s.source_row_id,
                                    r.data_payload || s.data_payload,
                                    r.ephemeral_payload || s.ephemeral_payload,
                                    s.valid_from, s.valid_until, s.causal_id,
                                    r.contributing_row_ids || s.source_row_id::BIGINT
                                FROM running_payload r JOIN ordered_sources s ON s.rn = r.rn + 1
                            )
                            SELECT source_row_id, data_payload, ephemeral_payload, valid_from, valid_until, causal_id, contributing_row_ids
                            FROM running_payload
                            ORDER BY rn DESC
                            LIMIT 1
                        ) source_payloads ON true
                    $$, v_lateral_join_sr_to_seg, v_range_constructor);

                    v_lateral_source_resolver_sql_existing := format($$
                        LEFT JOIN LATERAL (
                            WITH RECURSIVE ordered_sources AS (
                                SELECT
                                    source_row.source_row_id, source_row.data_payload, source_row.ephemeral_payload,
                                    source_row.valid_from, source_row.valid_until, source_row.causal_id,
                                    row_number() OVER (ORDER BY source_row.source_row_id) as rn
                                FROM active_source_rows source_row
                                WHERE %1$s
                                AND %2$I(seg.valid_from, seg.valid_until) <@ %2$I(source_row.valid_from, source_row.valid_until)
                            ),
                            running_payload AS (
                                SELECT rn, source_row_id, data_payload, ephemeral_payload, valid_from, valid_until, causal_id, ARRAY[source_row_id::BIGINT] as contributing_row_ids
                                FROM ordered_sources WHERE rn = 1
                                UNION ALL
                                SELECT
                                    s.rn, s.source_row_id,
                                    r.data_payload || s.data_payload,
                                    r.ephemeral_payload || s.ephemeral_payload,
                                    s.valid_from, s.valid_until, s.causal_id,
                                    r.contributing_row_ids || s.source_row_id::BIGINT
                                FROM running_payload r JOIN ordered_sources s ON s.rn = r.rn + 1
                            )
                            SELECT source_row_id, data_payload, ephemeral_payload, valid_from, valid_until, causal_id, contributing_row_ids
                            FROM running_payload
                            ORDER BY rn DESC
                            LIMIT 1
                        ) source_payloads ON true
                    $$, v_lateral_join_sr_to_seg_existing, v_range_constructor);

                    v_lateral_source_resolver_sql_new := format($$
                        LEFT JOIN LATERAL (
                            WITH RECURSIVE ordered_sources AS (
                                SELECT
                                    source_row.source_row_id, source_row.data_payload, source_row.ephemeral_payload,
                                    source_row.valid_from, source_row.valid_until, source_row.causal_id,
                                    row_number() OVER (ORDER BY source_row.source_row_id) as rn
                                FROM active_source_rows source_row
                                WHERE %1$s
                                AND %2$I(seg.valid_from, seg.valid_until) <@ %2$I(source_row.valid_from, source_row.valid_until)
                            ),
                            running_payload AS (
                                SELECT rn, source_row_id, data_payload, ephemeral_payload, valid_from, valid_until, causal_id, ARRAY[source_row_id::BIGINT] as contributing_row_ids
                                FROM ordered_sources WHERE rn = 1
                                UNION ALL
                                SELECT
                                    s.rn, s.source_row_id,
                                    r.data_payload || s.data_payload,
                                    r.ephemeral_payload || s.ephemeral_payload,
                                    s.valid_from, s.valid_until, s.causal_id,
                                    r.contributing_row_ids || s.source_row_id::BIGINT
                                FROM running_payload r JOIN ordered_sources s ON s.rn = r.rn + 1
                            )
                            SELECT source_row_id, data_payload, ephemeral_payload, valid_from, valid_until, causal_id, contributing_row_ids
                            FROM running_payload
                            ORDER BY rn DESC
                            LIMIT 1
                        ) source_payloads ON true
                    $$, v_lateral_join_sr_to_seg_new, v_range_constructor);

                WHEN 'MERGE_ENTITY_REPLACE', 'REPLACE_FOR_PORTION_OF', 'INSERT_NEW_ENTITIES', 'DELETE_FOR_PORTION_OF' THEN
                    v_lateral_source_resolver_sql := format($$
                        LEFT JOIN LATERAL (
                            SELECT source_row.source_row_id, source_row.data_payload, source_row.ephemeral_payload, source_row.valid_from, source_row.valid_until, source_row.causal_id, ARRAY[source_row.source_row_id::BIGINT] as contributing_row_ids
                            FROM active_source_rows source_row
                            WHERE %1$s
                            AND %2$I(seg.valid_from, seg.valid_until) <@ %2$I(source_row.valid_from, source_row.valid_until)
                            ORDER BY source_row.source_row_id DESC
                            LIMIT 1
                        ) source_payloads ON true
                    $$, v_lateral_join_sr_to_seg /* %1$s */, v_range_constructor /* %2$I */);

                    v_lateral_source_resolver_sql_existing := format($$
                        LEFT JOIN LATERAL (
                            SELECT source_row.source_row_id, source_row.data_payload, source_row.ephemeral_payload, source_row.valid_from, source_row.valid_until, source_row.causal_id, ARRAY[source_row.source_row_id::BIGINT] as contributing_row_ids
                            FROM active_source_rows source_row
                            WHERE %1$s
                            AND %2$I(seg.valid_from, seg.valid_until) <@ %2$I(source_row.valid_from, source_row.valid_until)
                            ORDER BY source_row.source_row_id DESC
                            LIMIT 1
                        ) source_payloads ON true
                    $$, v_lateral_join_sr_to_seg_existing, v_range_constructor);

                    v_lateral_source_resolver_sql_new := format($$
                        LEFT JOIN LATERAL (
                            SELECT source_row.source_row_id, source_row.data_payload, source_row.ephemeral_payload, source_row.valid_from, source_row.valid_until, source_row.causal_id, ARRAY[source_row.source_row_id::BIGINT] as contributing_row_ids
                            FROM active_source_rows source_row
                            WHERE %1$s
                            AND %2$I(seg.valid_from, seg.valid_until) <@ %2$I(source_row.valid_from, source_row.valid_until)
                            ORDER BY source_row.source_row_id DESC
                            LIMIT 1
                        ) source_payloads ON true
                    $$, v_lateral_join_sr_to_seg_new, v_range_constructor);

                ELSE
                    RAISE EXCEPTION 'Unhandled temporal_merge_mode in planner: %', mode;
            END CASE;
    
            -- 4. Construct and execute the main query to generate the execution plan.
            --
            -- Phase 4.1: Materialize source_initial as a temporary table for performance and clarity.
            
            v_sql := format($SQL$
                CREATE TEMP TABLE source_initial ON COMMIT DROP AS
                WITH source_data AS (
                    SELECT
                        source_table.%1$I /* row_id_column */ as source_row_id,
                        %2$s /* v_causal_select_expr */ as causal_id,
                        %3$s -- v_non_temporal_lookup_cols_select_list_prefix
                        %4$s, /* v_source_temporal_cols_expr */
                        %5$s /* v_source_data_payload_expr */ AS data_payload,
                        %6$s /* v_source_ephemeral_payload_expr */ AS ephemeral_payload,
                        %7$s /* v_stable_pk_cols_jsonb_build_source */ as stable_pk_payload,
                        %8$s /* v_entity_id_check_is_null_expr */ as stable_identity_columns_are_null,
                        %9$s /* v_lookup_cols_are_null_expr */ as lookup_columns_unavailable,
                        %10$s /* v_is_identifiable_expr */ as is_identifiable,
                        %11$s /* v_consistency_check_expr */ as temporal_columns_are_consistent
                    FROM %12$s /* v_source_table_ident */ source_table
                )
                SELECT 
                    *,
                    -- OPTIMIZATION: Pre-compute range column to avoid repeated daterange() calls
                    -- This provides ~10-15%% performance improvement in eclipse detection
                    %13$I(valid_from, valid_until) as valid_range
                FROM source_data;
            $SQL$,
                row_id_column,                                       /* %1$I */
                v_causal_select_expr,                                /* %2$s */
                v_non_temporal_lookup_cols_select_list_prefix,       /* %3$s */
                v_source_temporal_cols_expr,                         /* %4$s */
                v_source_data_payload_expr,                          /* %5$s */
                v_source_ephemeral_payload_expr,                     /* %6$s */
                v_stable_pk_cols_jsonb_build_source,                 /* %7$s */
                v_entity_id_check_is_null_expr,                      /* %8$s */
                v_lookup_cols_are_null_expr,                         /* %9$s */
                v_is_identifiable_expr,                              /* %10$s */
                v_consistency_check_expr,                            /* %11$s */
                v_source_table_ident,                                /* %12$s */
                v_range_constructor                                  /* %13$I */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            v_sql := 'CREATE INDEX ON source_initial (source_row_id);';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);

            IF v_lookup_columns IS NOT NULL AND cardinality(v_lookup_columns) > 0 THEN
                SELECT format('CREATE INDEX ON source_initial (%s);', string_agg(quote_ident(c), ', '))
                INTO v_sql
                FROM unnest(v_lookup_columns) AS c;
                v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);
                
                -- OPTIMIZATION: Add composite index for eclipse detection
                -- This enables index-only scans providing ~20-30 percent performance improvement
                SELECT format('CREATE INDEX ON source_initial (%s, source_row_id);', string_agg(quote_ident(c), ', '))
                INTO v_sql
                FROM unnest(v_lookup_columns) AS c;
                v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);
            END IF;

            v_sql := 'CREATE INDEX ON source_initial (causal_id);';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);

            v_sql := 'CREATE INDEX ON source_initial (causal_id, source_row_id);';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);

            v_sql := 'ANALYZE source_initial;';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);
    
            v_sql := format($SQL$
                CREATE TEMP TABLE source_with_eclipsed_flag ON COMMIT DROP AS
                SELECT * FROM (
                    -- Path 1: Existing Entities (Window Function on Lookup Keys)
                    SELECT
                        s1.*,
                        COALESCE(
                            range_agg(valid_range) OVER (
                                PARTITION BY %1$s
                                ORDER BY source_row_id DESC
                                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                            ) @> valid_range,
                            false
                        ) as is_eclipsed,
                        array_agg(source_row_id) OVER (
                            PARTITION BY %1$s
                            ORDER BY source_row_id DESC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                        ) as eclipsed_by
                    FROM source_initial s1
                    WHERE NOT s1.lookup_columns_unavailable

                    UNION ALL

                    -- Path 2: New Entities (Window Function on Causal ID)
                    SELECT
                        s1.*,
                        COALESCE(
                            range_agg(valid_range) OVER (
                                PARTITION BY causal_id
                                ORDER BY source_row_id DESC
                                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                            ) @> valid_range,
                            false
                        ) as is_eclipsed,
                        array_agg(source_row_id) OVER (
                            PARTITION BY causal_id
                            ORDER BY source_row_id DESC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                        ) as eclipsed_by
                    FROM source_initial s1
                    WHERE s1.lookup_columns_unavailable
                ) t;
            $SQL$,
                (SELECT string_agg(format('%I', col), ', ') FROM unnest(v_lookup_columns) as col)  -- %1$s
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- Phase 4.2: Construct and execute the main query to generate the execution plan.
            -- Each CTE is materialized into a temporary table for performance analysis and debugging.
            
            -- CTE 3: target_rows
            v_sql := format($SQL$
                CREATE TEMP TABLE target_rows ON COMMIT DROP AS
                SELECT
                    %1$s /* v_non_temporal_lookup_cols_select_list_no_alias_prefix */ -- (non-temporal identity columns)
                    lower(t.%2$I) as valid_from, -- The temporal identity column (e.g., valid_from)
                    NULL::%3$s /* v_causal_column_type */ as causal_id, -- Target rows do not originate from a source row, so causal_id is NULL. Type is introspected.
                    %4$s /* v_stable_pk_cols_jsonb_build_bare */ as stable_pk_payload,
                    upper(t.%2$I) as valid_until,
                    %6$s /* v_target_data_cols_jsonb_build_bare */ AS data_payload,
                    %7$s /* v_target_ephemeral_cols_jsonb_build_bare */ AS ephemeral_payload,
                    %8$s /* v_target_nk_json_expr */ AS canonical_nk_json
                FROM %9$s /* v_target_rows_filter */
            $SQL$,
                v_non_temporal_lookup_cols_select_list_no_alias_prefix, /* %1$s */
                v_range_col,                                            /* %2$I */
                v_causal_column_type,                                   /* %3$s */
                v_stable_pk_cols_jsonb_build_bare,                      /* %4$s */
                NULL,                                                   /* %5$I -- placeholder */
                v_target_data_cols_jsonb_build_bare,                    /* %6$s */
                v_target_ephemeral_cols_jsonb_build_bare,               /* %7$s */
                v_target_nk_json_expr,                                  /* %8$s */
                v_target_rows_filter                                    /* %9$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);

            IF v_original_entity_key_cols IS NOT NULL AND cardinality(v_original_entity_key_cols) > 0 THEN
                v_sql := format('CREATE INDEX ON target_rows (%s);', (SELECT string_agg(quote_ident(c), ', ') FROM unnest(v_original_entity_key_cols) AS c));
                v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);
            END IF;

            v_sql := format('CREATE INDEX ON target_rows USING gist (%s(valid_from, valid_until));', v_range_constructor);
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);

            v_sql := 'ANALYZE target_rows;';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);
    
            -- CTE 4: source_rows_with_matches
            v_sql := format($SQL$
                CREATE TEMP TABLE source_rows_with_matches ON COMMIT DROP AS
                SELECT
                    source_row.*,
                    target_row.stable_pk_payload as discovered_stable_pk_payload,
                    %1$s /* v_propagated_id_cols_list */
                FROM source_with_eclipsed_flag source_row
                LEFT JOIN target_rows target_row ON (%2$s /* v_source_rows_exists_join_expr */)
            $SQL$,
                v_propagated_id_cols_list,      /* %1$s */
                v_source_rows_exists_join_expr  /* %2$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- CTE 5: source_rows_with_aggregates
            v_sql := $SQL$
                CREATE TEMP TABLE source_rows_with_aggregates ON COMMIT DROP AS
                SELECT
                    source_row_id,
                    count(DISTINCT discovered_stable_pk_payload) as match_count,
                    -- Filter out NULL to avoid [null] result for new entities with no matches
                    COALESCE(jsonb_agg(DISTINCT discovered_stable_pk_payload) FILTER (WHERE discovered_stable_pk_payload IS NOT NULL), '[]'::jsonb) as conflicting_ids
                FROM source_rows_with_matches
                GROUP BY source_row_id
            $SQL$;
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- CTE 5: source_rows_with_discovery
            v_sql := $SQL$
                CREATE TEMP TABLE source_rows_with_discovery ON COMMIT DROP AS
                SELECT
                    m.*,
                    a.match_count,
                    a.conflicting_ids,
                    (a.match_count > 1) as is_ambiguous
                FROM source_rows_with_matches m
                JOIN source_rows_with_aggregates a ON m.source_row_id = a.source_row_id
            $SQL$;
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- CTE 6: source_rows
            v_sql := format($SQL$
                CREATE TEMP TABLE source_rows ON COMMIT DROP AS
                SELECT DISTINCT ON (p.source_row_id)
                    p.source_row_id, p.causal_id, p.valid_from, p.valid_until, p.data_payload, p.ephemeral_payload,
                    (p.stable_identity_columns_are_null AND p.discovered_stable_pk_payload IS NULL) as stable_identity_columns_are_null,
                    p.lookup_columns_unavailable, p.is_identifiable,
                    p.is_ambiguous, p.conflicting_ids, p.is_eclipsed, p.eclipsed_by,
                    p.temporal_columns_are_consistent,
                    COALESCE(p.stable_pk_payload, p.discovered_stable_pk_payload) as stable_pk_payload,
                    %1$s /* v_target_entity_exists_expr */ as target_entity_exists,
                    %2$s /* v_coalesced_id_cols_list */
                FROM source_rows_with_discovery p
                ORDER BY p.source_row_id, p.discovered_stable_pk_payload
            $SQL$,
                v_target_entity_exists_expr, /* %1$s */
                v_coalesced_id_cols_list     /* %2$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- CTE 8: source_rows_with_new_flag
            v_sql := $SQL$
                CREATE TEMP TABLE source_rows_with_new_flag ON COMMIT DROP AS
                SELECT *, NOT target_entity_exists as is_new_entity
                FROM source_rows
            $SQL$;
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
            
            -- CTE 9.1: source_rows_with_nk_json
            v_sql := format($SQL$
                CREATE TEMP TABLE source_rows_with_nk_json ON COMMIT DROP AS
                SELECT source_row.*, %1$s /* v_lookup_keys_as_jsonb_expr */ as nk_json, %2$s /* v_lookup_keys_as_array_expr */ as nk_non_null_keys_array
                FROM source_rows_with_new_flag source_row
            $SQL$,
                v_lookup_keys_as_jsonb_expr, /* %1$s */
                v_lookup_keys_as_array_expr  /* %2$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);

            v_sql := 'CREATE INDEX ON source_rows_with_nk_json USING GIN (nk_json);';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);

            v_sql := 'ANALYZE source_rows_with_nk_json;';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);
    
            -- CTE 9.2: source_rows_with_canonical_key
            -- Optimization: When all new entities have uniform NK specificity (same non-null keys),
            -- each row's canonical_nk_json is simply its own nk_json. This avoids an O(n²) LATERAL
            -- join that would otherwise check each row against all others.
            -- The LATERAL is only needed when rows have different specificities (e.g., one row has
            -- NK {a:null, b:1} and another has {a:5, b:1} where the second is "more specific").
            v_sql := format($SQL$
                CREATE TEMP TABLE source_rows_with_canonical_key ON COMMIT DROP AS
                WITH nk_specificity_check AS (
                    SELECT count(DISTINCT nk_non_null_keys_array::text) = 1 AS is_uniform
                    FROM source_rows_with_nk_json
                    WHERE is_new_entity
                )
                SELECT s.*, (%1$s /* v_grouping_key_expr */) as grouping_key
                FROM (
                    SELECT
                        s1.*,
                        CASE 
                            -- Fast path: uniform specificity means each row is its own canonical
                            WHEN (SELECT is_uniform FROM nk_specificity_check) 
                            THEN s1.nk_json
                            -- Slow path: varied specificity requires finding most specific match
                            ELSE (
                                SELECT s2_inner.nk_json
                                FROM source_rows_with_nk_json s2_inner
                                WHERE s1.is_new_entity AND s2_inner.is_new_entity AND s2_inner.nk_json @> s1.nk_json
                                ORDER BY array_length(s2_inner.nk_non_null_keys_array, 1) DESC, s2_inner.nk_non_null_keys_array::text DESC
                                LIMIT 1
                            )
                        END as canonical_nk_json
                    FROM source_rows_with_nk_json s1
                ) s
            $SQL$,
                v_grouping_key_expr /* %1$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
            
            -- CTE 9.3: source_rows_with_early_feedback
            v_sql := format($SQL$
                CREATE TEMP TABLE source_rows_with_early_feedback ON COMMIT DROP AS
                SELECT
                    s.*,
                    CASE
                        WHEN s.is_ambiguous
                        THEN jsonb_build_object( 'operation', 'ERROR'::text, 'message', 'Source row is ambiguous. It matches multiple distinct target entities: ' || s.conflicting_ids::text )
                        WHEN NOT s.is_identifiable AND s.is_new_entity
                        THEN jsonb_build_object( 'operation', 'ERROR'::text, 'message', 'Source row is unidentifiable. It has NULL for all stable identity columns ' || replace(%1$L::text, '"', '') || ' and all natural keys ' || replace(%2$L::text, '"', '') )
                        WHEN NOT s.temporal_columns_are_consistent
                        THEN jsonb_build_object( 'operation', 'ERROR'::text, 'message', 'Source row has inconsistent temporal columns. Column "' || %3$L || '" must be equal to column "' || %4$L || '" + ' || %5$L || '.' )
                        WHEN s.is_eclipsed
                        THEN jsonb_build_object( 'operation', 'SKIP_ECLIPSED'::text, 'message', 'Source row was eclipsed by row_ids=' || s.eclipsed_by::text || ' in the same batch.' )
                        ELSE NULL
                    END as early_feedback
                FROM source_rows_with_canonical_key s
            $SQL$,
                v_identity_columns, /* %1$L */
                lookup_keys,        /* %2$L */
                v_valid_until_col,  /* %3$L */
                v_valid_to_col,     /* %4$L */
                v_interval          /* %5$L */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
            
            -- CTE 10: active_source_rows
            v_sql := format($SQL$
                CREATE TEMP TABLE active_source_rows ON COMMIT DROP AS
                SELECT source_row.*
                FROM source_rows_with_early_feedback source_row
                WHERE source_row.early_feedback IS NULL
                AND CASE %1$L::sql_saga.temporal_merge_mode
                    WHEN 'MERGE_ENTITY_PATCH' THEN true
                    WHEN 'MERGE_ENTITY_REPLACE' THEN true
                    WHEN 'MERGE_ENTITY_UPSERT' THEN true
                    WHEN 'INSERT_NEW_ENTITIES' THEN NOT source_row.target_entity_exists
                    WHEN 'PATCH_FOR_PORTION_OF' THEN source_row.target_entity_exists
                    WHEN 'REPLACE_FOR_PORTION_OF' THEN source_row.target_entity_exists
                    WHEN 'DELETE_FOR_PORTION_OF' THEN source_row.target_entity_exists
                    WHEN 'UPDATE_FOR_PORTION_OF' THEN source_row.target_entity_exists
                    ELSE false
                END
            $SQL$,
                mode /* %1$L */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);

            IF v_original_entity_key_cols IS NOT NULL AND cardinality(v_original_entity_key_cols) > 0 THEN
                v_sql := format('CREATE INDEX ON active_source_rows (%s);', (SELECT string_agg(quote_ident(c), ', ') FROM unnest(v_original_entity_key_cols) AS c));
                v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);
            END IF;

            v_sql := 'CREATE INDEX ON active_source_rows (grouping_key);';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);

            v_sql := format('CREATE INDEX ON active_source_rows USING gist (%s(valid_from, valid_until));', v_range_constructor);
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);

            v_sql := 'ANALYZE active_source_rows;';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);
    
            -- CTE 10: all_rows
            v_sql := format($SQL$
                CREATE TEMP TABLE all_rows ON COMMIT DROP AS
                SELECT %1$s /* v_non_temporal_lookup_cols_select_list_no_alias_prefix */ causal_id, valid_from, valid_until, is_new_entity, stable_pk_payload, stable_identity_columns_are_null, lookup_columns_unavailable, is_identifiable, is_ambiguous, conflicting_ids, canonical_nk_json FROM active_source_rows
                UNION ALL
                SELECT
                    %2$s /* v_non_temporal_tr_qualified_lookup_cols_prefix */
                    target_row.causal_id,
                    target_row.valid_from,
                    target_row.valid_until,
                    false as is_new_entity,
                    target_row.stable_pk_payload,
                    false as stable_identity_columns_are_null,
                    false as lookup_columns_unavailable,
                    true as is_identifiable,
                    false as is_ambiguous,
                    NULL::jsonb as conflicting_ids,
                    target_row.canonical_nk_json
                FROM target_rows target_row
            $SQL$,
                v_non_temporal_lookup_cols_select_list_no_alias_prefix, /* %1$s */
                v_non_temporal_tr_qualified_lookup_cols_prefix          /* %2$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- CTE 11: time_points_raw
            v_sql := format($SQL$
                CREATE TEMP TABLE time_points_raw ON COMMIT DROP AS
                SELECT %1$s, causal_id, valid_from AS point, is_new_entity, stable_pk_payload, stable_identity_columns_are_null, lookup_columns_unavailable, is_identifiable, is_ambiguous, conflicting_ids, canonical_nk_json FROM all_rows
                UNION ALL
                SELECT %1$s, causal_id, valid_until AS point, is_new_entity, stable_pk_payload, stable_identity_columns_are_null, lookup_columns_unavailable, is_identifiable, is_ambiguous, conflicting_ids, canonical_nk_json FROM all_rows
            $SQL$,
                v_lookup_cols_select_list_no_alias /* %1$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
            
            -- CTE 12: time_points_unified
            v_sql := format($SQL$
                CREATE TEMP TABLE time_points_unified ON COMMIT DROP AS
                SELECT
                    *,
                    %1$s AS grouping_key,
                    CASE
                        WHEN is_new_entity THEN causal_id
                        ELSE FIRST_VALUE(causal_id) OVER (PARTITION BY %1$s ORDER BY causal_id ASC NULLS LAST)
                    END as unified_causal_id,
                    FIRST_VALUE(stable_pk_payload) OVER (PARTITION BY %1$s ORDER BY causal_id ASC NULLS FIRST) as unified_stable_pk_payload,
                    FIRST_VALUE(canonical_nk_json) OVER (PARTITION BY %1$s ORDER BY causal_id ASC NULLS FIRST) as unified_canonical_nk_json
                FROM time_points_raw
            $SQL$,
                v_grouping_key_expr /* %1$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- CTE 12.5: time_points_with_unified_ids
            v_sql := format($SQL$
                CREATE TEMP TABLE time_points_with_unified_ids ON COMMIT DROP AS
                SELECT
                    tpu.grouping_key,
                    %1$s,
                    tpu.unified_causal_id as causal_id,
                    tpu.point,
                    tpu.is_new_entity,
                    tpu.unified_stable_pk_payload as stable_pk_payload,
                    tpu.stable_identity_columns_are_null,
                    tpu.lookup_columns_unavailable,
                    tpu.is_identifiable,
                    tpu.is_ambiguous,
                    tpu.conflicting_ids,
                    tpu.unified_canonical_nk_json as canonical_nk_json
                FROM time_points_unified tpu
            $SQL$,
                v_unified_id_cols_projection /* %1$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- CTE 13: time_points
            v_sql := $SQL$
                CREATE TEMP TABLE time_points ON COMMIT DROP AS
                SELECT DISTINCT ON (grouping_key, point) *
                FROM time_points_with_unified_ids
                ORDER BY grouping_key, point, causal_id DESC NULLS LAST
            $SQL$;
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- CTE 14: atomic_segments
            v_sql := format($SQL$
                CREATE TEMP TABLE atomic_segments ON COMMIT DROP AS
                SELECT grouping_key, %1$s, causal_id, point as valid_from, next_point as valid_until, is_new_entity, stable_pk_payload, stable_identity_columns_are_null, lookup_columns_unavailable, is_identifiable, is_ambiguous, conflicting_ids, canonical_nk_json
                FROM (
                    SELECT *, LEAD(point) OVER (PARTITION BY grouping_key ORDER BY point) as next_point
                    FROM time_points
                ) with_lead
                WHERE point IS NOT NULL AND next_point IS NOT NULL AND point < next_point
            $SQL$,
                v_lookup_cols_select_list_no_alias /* %1$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);

            v_sql := 'CREATE INDEX ON atomic_segments (grouping_key);';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);

            v_sql := 'ANALYZE atomic_segments;';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);
            
            -- CTE 15: resolved_atomic_segments_with_payloads (Split-path optimization)
            -- Path 1: Existing entities (use Hash Join for better performance)
            v_sql := format($SQL$
                CREATE TEMP TABLE existing_segments_with_target ON COMMIT DROP AS
                SELECT
                    seg.grouping_key, seg.canonical_nk_json, %1$s, seg.is_new_entity, seg.is_identifiable, seg.is_ambiguous, seg.conflicting_ids, seg.stable_identity_columns_are_null, seg.lookup_columns_unavailable, seg.valid_from, seg.valid_until,
                    target_row.valid_from as t_valid_from, target_row.valid_until as t_valid_until,
                    target_row.data_payload as t_data_payload, target_row.ephemeral_payload as t_ephemeral_payload, seg.stable_pk_payload, 
                    target_row.stable_pk_payload as target_stable_pk_payload,
                    source_payloads.source_row_id, source_payloads.contributing_row_ids,
                    source_payloads.data_payload as s_data_payload, source_payloads.ephemeral_payload as s_ephemeral_payload,
                    source_payloads.valid_from AS s_valid_from, source_payloads.valid_until AS s_valid_until,
                    %2$s
                FROM atomic_segments seg
                LEFT JOIN target_rows target_row 
                    ON %3$s 
                    AND %4$I(seg.valid_from, seg.valid_until) <@ %4$I(target_row.valid_from, target_row.valid_until)
                %5$s
                WHERE NOT seg.is_new_entity
            $SQL$,
                v_entity_key_for_with_base_payload_expr, /* %1$s */
                v_trace_seed_expr,                       /* %2$s */
                v_lateral_join_tr_to_seg,                /* %3$s */
                v_range_constructor,                     /* %4$I */
                v_lateral_source_resolver_sql_existing   /* %5$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);

            -- Analyze existing_segments_with_target after creation
            v_sql := 'ANALYZE existing_segments_with_target;';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);

            -- Path 2: New entities (no target join needed)
            v_sql := format($SQL$
                CREATE TEMP TABLE new_segments_no_target ON COMMIT DROP AS
                SELECT
                    seg.grouping_key, seg.canonical_nk_json, %1$s, seg.is_new_entity, seg.is_identifiable, seg.is_ambiguous, seg.conflicting_ids, seg.stable_identity_columns_are_null, seg.lookup_columns_unavailable, seg.valid_from, seg.valid_until,
                    NULL::%3$s as t_valid_from, NULL::%3$s as t_valid_until,
                    NULL::jsonb as t_data_payload, NULL::jsonb as t_ephemeral_payload, seg.stable_pk_payload,
                    NULL::jsonb as target_stable_pk_payload,
                    source_payloads.source_row_id, source_payloads.contributing_row_ids,
                    source_payloads.data_payload as s_data_payload, source_payloads.ephemeral_payload as s_ephemeral_payload,
                    source_payloads.valid_from AS s_valid_from, source_payloads.valid_until AS s_valid_until,
                    %2$s
                FROM atomic_segments seg
                %4$s
                WHERE seg.is_new_entity
            $SQL$,
                v_entity_key_for_with_base_payload_expr, /* %1$s */
                v_trace_seed_expr,                       /* %2$s */
                v_range_subtype,                         /* %3$s */
                v_lateral_source_resolver_sql_new        /* %4$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);

            -- Analyze new_segments_no_target after creation
            v_sql := 'ANALYZE new_segments_no_target;';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);

            -- Path 3: Union and apply source resolution
            v_sql := format($SQL$
                CREATE TEMP TABLE resolved_atomic_segments_with_payloads ON COMMIT DROP AS
                WITH all_segments AS (
                    SELECT * FROM existing_segments_with_target
                    UNION ALL
                    SELECT * FROM new_segments_no_target
                )
                SELECT
                    seg.*,
                    seg.stable_pk_payload as propagated_stable_pk_payload
                FROM all_segments seg
                WHERE (seg.s_data_payload IS NOT NULL OR seg.t_data_payload IS NOT NULL)
                AND CASE %1$L::sql_saga.temporal_merge_mode
                    WHEN 'PATCH_FOR_PORTION_OF' THEN seg.t_data_payload IS NOT NULL
                    WHEN 'REPLACE_FOR_PORTION_OF' THEN seg.t_data_payload IS NOT NULL
                    WHEN 'DELETE_FOR_PORTION_OF' THEN seg.t_data_payload IS NOT NULL
                    WHEN 'UPDATE_FOR_PORTION_OF' THEN seg.t_data_payload IS NOT NULL
                    ELSE true
                END
            $SQL$,
                mode                                     /* %1$L */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);

            -- PERFORMANCE: Analyze temp table for accurate cardinality estimates
            -- This prevents O(n²) behavior in lateral joins by providing proper statistics
            v_sql := 'ANALYZE resolved_atomic_segments_with_payloads;';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);
    
            v_resolver_from := 'resolved_atomic_segments_with_payloads';
    
            IF delete_mode IN ('DELETE_MISSING_ENTITIES', 'DELETE_MISSING_TIMELINE_AND_ENTITIES') THEN
                v_sql := format($$
                    CREATE TEMP TABLE resolved_atomic_segments_with_flag ON COMMIT DROP AS
                    SELECT *, bool_or(s_data_payload IS NOT NULL) OVER (PARTITION BY grouping_key) as entity_is_in_source
                    FROM %s
                $$, v_resolver_from);
                v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);

                -- PERFORMANCE: Analyze temp table for accurate cardinality estimates
                v_sql := 'ANALYZE resolved_atomic_segments_with_flag;';
                v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);

                v_resolver_from := 'resolved_atomic_segments_with_flag';
            END IF;
    
            -- CTE 15.5: resolved_atomic_segments_with_propagated_ids
            v_sql := format($SQL$
                CREATE TEMP TABLE resolved_atomic_segments_with_propagated_ids ON COMMIT DROP AS
                SELECT
                    *,
                    FIRST_VALUE(canonical_nk_json) OVER (PARTITION BY grouping_key ORDER BY valid_from) as unified_canonical_nk_json,
                    COALESCE(
                        contributing_row_ids,
                        (array_concat_agg(contributing_row_ids) FILTER (WHERE contributing_row_ids IS NOT NULL) OVER (PARTITION BY grouping_key, t_valid_from, look_behind_grp)),
                        (array_concat_agg(contributing_row_ids) FILTER (WHERE contributing_row_ids IS NOT NULL) OVER (PARTITION BY grouping_key, t_valid_from, look_ahead_grp))
                    ) as causal_source_row_ids,
                    COALESCE(
                        s_valid_from,
                        (max(s_valid_from) OVER (PARTITION BY grouping_key, t_valid_from, look_behind_grp)),
                        (max(s_valid_from) OVER (PARTITION BY grouping_key, t_valid_from, look_ahead_grp))
                    ) as propagated_s_valid_from,
                    COALESCE(
                        s_valid_until,
                        (max(s_valid_until) OVER (PARTITION BY grouping_key, t_valid_from, look_behind_grp)),
                        (max(s_valid_until) OVER (PARTITION BY grouping_key, t_valid_from, look_ahead_grp))
                    ) as propagated_s_valid_until
                FROM (
                    SELECT
                        *,
                        sum(CASE WHEN source_row_id IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY grouping_key, t_valid_from ORDER BY valid_from) AS look_behind_grp,
                        sum(CASE WHEN source_row_id IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY grouping_key, t_valid_from ORDER BY valid_from DESC) AS look_ahead_grp
                    FROM %1$s /* v_resolver_from */
                ) with_grp
            $SQL$,
                v_resolver_from /* %1$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- CTE 16: resolved_atomic_segments
            v_sql := format($SQL$
                CREATE TEMP TABLE resolved_atomic_segments ON COMMIT DROP AS
                SELECT
                    grouping_key, %1$s stable_identity_columns_are_null, lookup_columns_unavailable, is_new_entity, is_identifiable, is_ambiguous, conflicting_ids, unified_canonical_nk_json,
                    valid_from, valid_until, t_valid_from, t_valid_until, propagated_s_valid_from, propagated_s_valid_until, causal_source_row_ids, causal_id, propagated_stable_pk_payload as stable_pk_payload,
                    causal_source_row_ids IS NULL AS unaffected_target_only_segment,
                    s_data_payload, t_data_payload,
                    sql_saga.get_allen_relation(propagated_s_valid_from, propagated_s_valid_until, t_valid_from, t_valid_until) AS orig_source_target_relation,
                    %2$s as data_payload,
                    md5((jsonb_strip_nulls(%2$s))::text) as data_hash,
                    CASE 
                        WHEN s_data_payload IS NULL THEN t_ephemeral_payload
                        ELSE COALESCE(t_ephemeral_payload, '{}'::jsonb) || COALESCE(s_ephemeral_payload, '{}'::jsonb)
                    END as ephemeral_payload,
                    CASE WHEN %3$L::boolean
                        THEN trace || jsonb_build_object( 'cte', 'ras', 'segment_stable_pk', propagated_stable_pk_payload, 'final_data_payload', %2$s, 'final_ephemeral_payload', CASE WHEN s_data_payload IS NULL THEN t_ephemeral_payload ELSE COALESCE(t_ephemeral_payload, '{}'::jsonb) || COALESCE(s_ephemeral_payload, '{}'::jsonb) END )
                        ELSE NULL
                    END as trace
                FROM resolved_atomic_segments_with_propagated_ids
            $SQL$,
                v_lookup_cols_sans_valid_from_prefix, /* %1$s */
                v_final_data_payload_expr,            /* %2$s */
                p_log_trace                           /* %3$L */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- CTE 17: island_group
            v_sql := $SQL$
                CREATE TEMP TABLE island_group ON COMMIT DROP AS
                SELECT
                    *,
                    SUM(is_island_start) OVER (PARTITION BY grouping_key ORDER BY valid_from) as island_group_id
                FROM (
                    SELECT
                        *,
                        CASE
                            WHEN prev_valid_until IS NULL
                            OR prev_valid_until <> valid_from
                            OR (prev_data_hash IS DISTINCT FROM data_hash)
                            THEN 1
                            ELSE 0
                        END as is_island_start
                    FROM (
                        SELECT
                            *,
                            LAG(valid_until) OVER w as prev_valid_until,
                            LAG(data_hash) OVER w as prev_data_hash,
                            LAG(data_payload) OVER w as prev_data_payload
                        FROM resolved_atomic_segments ras
                        WHERE ras.data_payload IS NOT NULL
                        WINDOW w AS (PARTITION BY grouping_key ORDER BY valid_from)
                    ) s1
                ) s2
            $SQL$;
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- CTE 17.5: coalesced_final_segments
            v_sql := format($SQL$
                CREATE TEMP TABLE coalesced_final_segments ON COMMIT DROP AS
                SELECT
                    grouping_key, %1$s,
                    sql_saga.first(causal_id ORDER BY valid_from) as causal_id,
                    sql_saga.first(stable_identity_columns_are_null ORDER BY valid_from) as stable_identity_columns_are_null,
                    sql_saga.first(lookup_columns_unavailable ORDER BY valid_from) as lookup_columns_unavailable,
                    sql_saga.first(is_new_entity ORDER BY valid_from) as is_new_entity,
                    sql_saga.first(is_identifiable ORDER BY valid_from) as is_identifiable,
                    sql_saga.first(is_ambiguous ORDER BY valid_from) as is_ambiguous,
                    sql_saga.first(conflicting_ids ORDER BY valid_from) as conflicting_ids,
                    sql_saga.first(unified_canonical_nk_json ORDER BY valid_from) as canonical_nk_json,
                    sql_saga.first(orig_source_target_relation ORDER BY valid_from) as orig_source_target_relation,
                    sql_saga.first(t_valid_from ORDER BY valid_from) as ancestor_valid_from,
                    MIN(valid_from) as valid_from,
                    MAX(valid_until) as valid_until,
                    %2$s as final_payload,
                    sql_saga.first(stable_pk_payload ORDER BY valid_from DESC) as stable_pk_payload,
                    bool_and(unaffected_target_only_segment) as unaffected_target_only_segment,
                    (SELECT array_agg(DISTINCT e) FROM unnest(array_concat_agg(causal_source_row_ids)) e WHERE e IS NOT NULL) as row_ids,
                    CASE WHEN %3$L::boolean
                        THEN jsonb_build_object( 'cte', 'coalesced', 'island_group_id', island_group_id, 'coalesced_stable_pk', sql_saga.first(stable_pk_payload ORDER BY valid_from DESC), 'final_payload', sql_saga.first(data_payload ORDER BY valid_from DESC), 'final_payload_sans_ephemeral', sql_saga.first(data_payload - %4$L::text[] ORDER BY valid_from DESC), 'atomic_traces', jsonb_agg((trace || jsonb_build_object('data_hash', data_hash, 'prev_data_hash', prev_data_hash, 'prev_data_payload', prev_data_payload)) ORDER BY valid_from) )
                        ELSE NULL
                    END as trace
                FROM island_group
                GROUP BY grouping_key, island_group_id
            $SQL$,
                v_stable_id_aggregates_expr, /* %1$s */
                v_coalesced_payload_expr,    /* %2$s */
                p_log_trace,                 /* %3$L */
                v_ephemeral_columns          /* %4$L */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- CTE 18: diff
            v_sql := format($SQL$
                CREATE TEMP TABLE diff ON COMMIT DROP AS
                SELECT
                    final_seg.grouping_key, %1$s,
                    COALESCE(final_seg.is_new_entity, false) as is_new_entity,
                    COALESCE(final_seg.is_identifiable, true) as is_identifiable,
                    COALESCE(final_seg.is_ambiguous, false) as is_ambiguous,
                    final_seg.conflicting_ids,
                    final_seg.canonical_nk_json,
                    COALESCE(final_seg.stable_identity_columns_are_null, false) as stable_identity_columns_are_null,
                    COALESCE(final_seg.lookup_columns_unavailable, false) as lookup_columns_unavailable,
                    final_seg.valid_from AS f_from, final_seg.valid_until AS f_until, final_seg.final_payload AS f_payload, final_seg.row_ids AS f_row_ids, final_seg.stable_pk_payload, final_seg.orig_source_target_relation,
                    CASE WHEN %2$L::boolean
                        THEN final_seg.trace || jsonb_build_object('cte', 'diff', 'diff_stable_pk', final_seg.stable_pk_payload, 'final_seg_causal_id', final_seg.causal_id, 'final_payload_vs_target_payload', jsonb_build_object('f', final_seg.final_payload, 't', target_seg.data_payload))
                        ELSE NULL
                    END as trace,
                    final_seg.unaffected_target_only_segment,
                    target_seg.valid_from as t_from, target_seg.valid_until as t_until, (target_seg.data_payload || target_seg.ephemeral_payload) as t_payload,
                    sql_saga.get_allen_relation(target_seg.valid_from, target_seg.valid_until, final_seg.valid_from, final_seg.valid_until) as b_a_relation
                FROM coalesced_final_segments AS final_seg
                FULL OUTER JOIN target_rows AS target_seg ON %3$s
            $SQL$,
                v_diff_select_expr,    /* %1$s */
                p_log_trace,           /* %2$L */
                v_diff_join_condition  /* %3$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
            
            -- CTE 19: diff_ranked
            v_sql := format($SQL$
                CREATE TEMP TABLE diff_ranked ON COMMIT DROP AS
                SELECT
                    d.*,
                    CASE
                        WHEN d.t_from IS NULL OR d.f_from IS NULL THEN NULL
                        WHEN d.f_from = d.t_from AND d.f_until = d.t_until AND d.f_payload IS NOT DISTINCT FROM d.t_payload THEN NULL
                        ELSE
                            row_number() OVER (
                                PARTITION BY d.grouping_key, d.t_from
                                ORDER BY
                                    CASE WHEN d.f_from = d.t_from THEN 1 ELSE 2 END,
                                    CASE WHEN d.f_payload - %1$L::text[] IS NOT DISTINCT FROM d.t_payload - %1$L::text[] THEN 1 ELSE 2 END,
                                    d.f_from,
                                    d.f_until
                            )
                    END as update_rank
                FROM diff d
            $SQL$,
                v_ephemeral_columns /* %1$L */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
            
            -- CTE 20: plan_with_op
            v_sql := format($SQL$
                CREATE TEMP TABLE plan_with_op ON COMMIT DROP AS
                (
                    SELECT * FROM (
                        SELECT
                            d.f_row_ids as row_ids, d.orig_source_target_relation, d.is_new_entity,
                            CASE
                                WHEN d.is_ambiguous THEN 'ERROR'::sql_saga.temporal_merge_plan_action
                                WHEN d.is_new_entity AND NOT d.is_identifiable THEN 'ERROR'::sql_saga.temporal_merge_plan_action
                                WHEN d.t_from IS NULL THEN 'INSERT'::sql_saga.temporal_merge_plan_action
                                WHEN d.f_from IS NULL THEN 'DELETE'::sql_saga.temporal_merge_plan_action
                                WHEN d.update_rank = 1 THEN 'UPDATE'::sql_saga.temporal_merge_plan_action
                                WHEN d.update_rank > 1 THEN 'INSERT'::sql_saga.temporal_merge_plan_action
                                WHEN d.update_rank IS NULL THEN
                                    CASE
                                        WHEN d.unaffected_target_only_segment THEN NULL
                                        ELSE 'SKIP_IDENTICAL'::sql_saga.temporal_merge_plan_action
                                    END
                                ELSE 'ERROR'::sql_saga.temporal_merge_plan_action
                            END as operation,
                            %1$s,
                            CASE
                                WHEN d.is_new_entity AND d.canonical_nk_json IS NOT NULL
                                THEN d.canonical_nk_json || COALESCE(d.stable_pk_payload, '{}'::jsonb)
                                ELSE %2$s
                            END as entity_keys_json,
                            %3$s as identity_keys,
                            %4$s as lookup_keys,
                            d.causal_id, d.t_from as old_valid_from, d.t_until as old_valid_until, d.f_from as new_valid_from, d.f_until as new_valid_until,
                            CASE
                                WHEN d.is_ambiguous THEN NULL
                                WHEN d.is_new_entity AND NOT d.is_identifiable THEN NULL
                                ELSE d.f_payload
                            END as data,
                            CASE
                                WHEN d.is_ambiguous
                                THEN jsonb_build_object('error', format('Source row is ambiguous. It matches multiple distinct target entities: %%s', d.conflicting_ids))
                                WHEN d.is_new_entity AND NOT d.is_identifiable
                                THEN jsonb_build_object('error', 'Source row is unidentifiable. It has NULL for all stable identity columns ' || replace(%5$L::text, '"', '') || ' and all natural keys ' || replace(%6$L::text, '"', ''))
                                ELSE NULL
                            END as feedback,
                            d.b_a_relation, d.grouping_key,
                            CASE WHEN %7$L::boolean
                                THEN d.trace || jsonb_build_object( 'cte', 'plan_with_op', 'diff_is_new_entity', d.is_new_entity, 'diff_causal_id', d.causal_id, 'entity_keys_from_key_cols', jsonb_build_object(%8$s), 'entity_keys_from_stable_pk', d.stable_pk_payload, 'final_entity_id_json', %2$s )
                                ELSE NULL
                            END as trace
                        FROM diff_ranked d
                        WHERE d.f_row_ids IS NOT NULL OR d.t_payload IS NOT NULL
                    ) with_op
                    WHERE with_op.operation IS NOT NULL
                )
                UNION ALL
                (
                    SELECT
                        ARRAY[source_row.source_row_id::BIGINT],
                        NULL::sql_saga.allen_interval_relation, source_row.is_new_entity,
                        COALESCE(
                            (source_row.early_feedback->>'operation')::sql_saga.temporal_merge_plan_action,
                            CASE
                                WHEN %9$L::sql_saga.temporal_merge_mode = 'INSERT_NEW_ENTITIES' AND source_row.target_entity_exists THEN 'SKIP_FILTERED'::sql_saga.temporal_merge_plan_action
                                WHEN %9$L::sql_saga.temporal_merge_mode IN ('PATCH_FOR_PORTION_OF', 'REPLACE_FOR_PORTION_OF', 'DELETE_FOR_PORTION_OF', 'UPDATE_FOR_PORTION_OF') AND NOT source_row.target_entity_exists THEN 'SKIP_NO_TARGET'::sql_saga.temporal_merge_plan_action
                                ELSE 'ERROR'::sql_saga.temporal_merge_plan_action
                            END
                        ),
                        %1$s,
                        CASE
                            WHEN source_row.is_new_entity AND source_row.canonical_nk_json IS NOT NULL
                            THEN source_row.canonical_nk_json || COALESCE(source_row.stable_pk_payload, '{}'::jsonb)
                            ELSE %10$s
                        END as entity_keys_json,
                        %11$s as identity_keys, %12$s as lookup_keys,
                        source_row.causal_id,
                        NULL, NULL, NULL, NULL, NULL,
                        CASE
                            WHEN source_row.early_feedback IS NOT NULL THEN jsonb_build_object('error', source_row.early_feedback->>'message')
                            ELSE jsonb_build_object('info', 'Source row was correctly filtered by the mode''s logic and did not result in a DML operation.')
                        END,
                        NULL,
                        %13$s AS grouping_key,
                        NULL::jsonb
                    FROM source_rows_with_early_feedback source_row
                    WHERE
                        source_row.early_feedback IS NOT NULL
                        OR NOT (
                            CASE %9$L::sql_saga.temporal_merge_mode
                            WHEN 'MERGE_ENTITY_PATCH' THEN true
                            WHEN 'MERGE_ENTITY_REPLACE' THEN true
                            WHEN 'MERGE_ENTITY_UPSERT' THEN true
                            WHEN 'INSERT_NEW_ENTITIES' THEN NOT source_row.target_entity_exists
                            WHEN 'PATCH_FOR_PORTION_OF' THEN source_row.target_entity_exists
                            WHEN 'REPLACE_FOR_PORTION_OF' THEN source_row.target_entity_exists
                            WHEN 'DELETE_FOR_PORTION_OF' THEN source_row.target_entity_exists
                            WHEN 'UPDATE_FOR_PORTION_OF' THEN source_row.target_entity_exists
                            ELSE false
                        END
                    )
                )
            $SQL$,
                v_lookup_cols_select_list_no_alias,               /* %1$s */
                v_plan_with_op_entity_id_json_build_expr,         /* %2$s */
                v_identity_keys_jsonb_build_expr_d,               /* %3$s */
                v_lookup_keys_jsonb_build_expr_d,                 /* %4$s */
                v_identity_columns,                               /* %5$L */
                lookup_keys,                                      /* %6$L */
                p_log_trace,                                      /* %7$L */
                v_plan_with_op_entity_id_json_build_expr_part_A,  /* %8$s */
                mode,                                             /* %9$L */
                v_skip_no_target_entity_id_json_build_expr,       /* %10$s */
                v_identity_keys_jsonb_build_expr_sr,              /* %11$s */
                v_lookup_keys_jsonb_build_expr_sr,                /* %12$s */
                v_grouping_key_expr_for_union                     /* %13$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- CTE 21: plan
            v_sql := format($SQL$
                CREATE TEMP TABLE plan ON COMMIT DROP AS
                SELECT
                    p.row_ids, p.operation, p.causal_id, p.is_new_entity,
                    %1$s,
                    p.entity_keys_json as entity_keys,
                    p.identity_keys, p.lookup_keys,
                    p.orig_source_target_relation, p.b_a_relation, p.old_valid_from, p.old_valid_until,
                    p.new_valid_from, p.new_valid_until, p.data, p.feedback, p.trace,
                    p.grouping_key,
                    CASE
                        WHEN p.operation <> 'UPDATE' THEN NULL::sql_saga.temporal_merge_update_effect
                        WHEN p.new_valid_from = p.old_valid_from AND p.new_valid_until = p.old_valid_until THEN 'NONE'::sql_saga.temporal_merge_update_effect
                        WHEN p.new_valid_from <= p.old_valid_from AND p.new_valid_until >= p.old_valid_until THEN 'GROW'::sql_saga.temporal_merge_update_effect
                        WHEN p.new_valid_from >= p.old_valid_from AND p.new_valid_until <= p.old_valid_until THEN 'SHRINK'::sql_saga.temporal_merge_update_effect
                        ELSE 'MOVE'::sql_saga.temporal_merge_update_effect
                    END AS update_effect
                FROM plan_with_op p
                LEFT JOIN source_rows_with_new_flag source_row ON source_row.source_row_id = p.row_ids[1]
            $SQL$,
                v_plan_select_key_cols /* %1$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- Final SELECT
            -- Execution strategy: with_temporary_temporal_gaps
            -- Operations are ordered DELETE→UPDATE→INSERT to avoid temporary overlaps (would violate NOT DEFERRABLE unique constraints)
            -- while tolerating temporary gaps (allowed by DEFERRABLE foreign key constraints). This enables native PostgreSQL 18
            -- temporal foreign keys. Within UPDATE operations, the update_effect enum order (NONE→SHRINK→MOVE→GROW) further
            -- minimizes gap duration by contracting timelines before expanding them.
            --
            -- CRITICAL: MOVE operations must be ordered by old_valid_from DESC (later ranges first) to avoid temporal conflicts.
            -- When multiple MOVE operations affect the same entity, processing later ranges first ensures that
            -- the target range doesn't conflict with existing rows that haven't been moved yet.
            --
            -- statement_seq groups operations that can safely execute in a single SQL statement:
            -- - DELETE: all in one statement (deletions don't conflict)
            -- - UPDATE NONE/SHRINK: all in one statement (only contract ranges)
            -- - UPDATE MOVE: each gets its own statement (can create temporary overlaps)
            -- - UPDATE GROW: all in one statement (only expand into gaps)
            -- - INSERT: all in one statement (new ranges don't overlap by design)
            v_sql := format($SQL$
                WITH RECURSIVE ordered_plan AS (
                    SELECT
                        row_number() OVER ( ORDER BY p.grouping_key, %1$s, CASE p.operation WHEN 'DELETE' THEN 1 WHEN 'UPDATE' THEN 2 WHEN 'INSERT' THEN 3 ELSE 4 END, p.update_effect NULLS FIRST, CASE WHEN p.update_effect = 'MOVE' THEN 1 ELSE 0 END, CASE WHEN p.update_effect = 'MOVE' THEN COALESCE(p.old_valid_from, p.new_valid_from) END DESC NULLS LAST, COALESCE(p.old_valid_from, p.new_valid_from), p.new_valid_from, (p.row_ids[1]) )::BIGINT as plan_op_seq,
                        p.*
                    FROM plan p
                ),
                -- For MOVE operations, group into the largest possible batches that can execute together.
                -- Within a single UPDATE statement, all rows see the pre-update state (MVCC snapshot).
                -- So if MOVE A's new_range overlaps MOVE B's old_range, they cannot be in the same statement.
                --
                -- Algorithm: Process MOVEs in plan_op_seq order (which is old_valid_from DESC).
                -- Track a multirange of old_ranges in the current batch. For each MOVE:
                -- - If new_range overlaps the batch's multirange, start a new batch
                -- - Add this MOVE's old_range to the batch's multirange
                --
                -- We use a recursive CTE to accumulate the multirange with reset-on-conflict logic.
                moves_ordered AS (
                    SELECT
                        o.plan_op_seq,
                        o.grouping_key,
                        o.old_valid_from,
                        o.old_valid_until,
                        o.new_valid_from,
                        o.new_valid_until,
                        row_number() OVER (PARTITION BY o.grouping_key ORDER BY o.plan_op_seq) AS move_seq
                    FROM ordered_plan o
                    WHERE o.operation = 'UPDATE' AND o.update_effect = 'MOVE'
                ),
                moves_with_batch AS (
                    -- Base case: first MOVE in each entity starts batch 0
                    SELECT 
                        plan_op_seq, grouping_key, old_valid_from, old_valid_until, 
                        new_valid_from, new_valid_until, move_seq,
                        0::bigint AS move_batch,
                        %2$s(%3$s(old_valid_from, old_valid_until, '[)')) AS batch_old_ranges
                    FROM moves_ordered
                    WHERE move_seq = 1
                    
                    UNION ALL
                    
                    -- Recursive case: check if new MOVE conflicts with batch's multirange
                    SELECT
                        m.plan_op_seq, m.grouping_key, m.old_valid_from, m.old_valid_until,
                        m.new_valid_from, m.new_valid_until, m.move_seq,
                        CASE 
                            WHEN %3$s(m.new_valid_from, m.new_valid_until, '[)') && prev.batch_old_ranges
                            THEN prev.move_batch + 1  -- Conflict: start new batch
                            ELSE prev.move_batch      -- No conflict: same batch
                        END AS move_batch,
                        CASE 
                            WHEN %3$s(m.new_valid_from, m.new_valid_until, '[)') && prev.batch_old_ranges
                            THEN %2$s(%3$s(m.old_valid_from, m.old_valid_until, '[)'))  -- Reset multirange
                            ELSE prev.batch_old_ranges + %2$s(%3$s(m.old_valid_from, m.old_valid_until, '[)'))  -- Add to multirange
                        END AS batch_old_ranges
                    FROM moves_ordered m
                    JOIN moves_with_batch prev 
                      ON prev.grouping_key = m.grouping_key 
                     AND prev.move_seq = m.move_seq - 1
                ),
                with_statement_seq AS (
                    SELECT
                        o.plan_op_seq,
                        CASE
                            WHEN o.operation = 'DELETE' THEN 1
                            WHEN o.operation = 'UPDATE' AND o.update_effect IN ('NONE', 'SHRINK') THEN 2
                            WHEN o.operation = 'UPDATE' AND o.update_effect = 'MOVE' THEN
                                -- Base of 3 for first MOVE batch, +1 for each subsequent batch
                                3 + COALESCE(mb.move_batch, 0)
                            WHEN o.operation = 'UPDATE' AND o.update_effect = 'GROW' THEN 1000000  -- Will be renumbered
                            WHEN o.operation = 'INSERT' THEN 2000000  -- Will be renumbered
                            ELSE 3000000
                        END::INT as raw_statement_seq,
                        o.row_ids, o.operation, o.update_effect, o.causal_id::TEXT, o.is_new_entity, o.entity_keys, o.identity_keys, o.lookup_keys, o.orig_source_target_relation, o.b_a_relation, o.old_valid_from::TEXT,
                        o.old_valid_until::TEXT, o.new_valid_from::TEXT, o.new_valid_until::TEXT, o.data, o.feedback, CASE WHEN o.trace IS NOT NULL THEN o.trace || jsonb_build_object('final_grouping_key', o.grouping_key) ELSE NULL END as trace, o.grouping_key
                    FROM ordered_plan o
                    LEFT JOIN moves_with_batch mb ON mb.plan_op_seq = o.plan_op_seq
                )
                SELECT
                    plan_op_seq,
                    dense_rank() OVER (ORDER BY raw_statement_seq)::INT as statement_seq,
                    row_ids, operation, update_effect, causal_id, is_new_entity, entity_keys, identity_keys, lookup_keys, orig_source_target_relation, b_a_relation, old_valid_from,
                    old_valid_until, new_valid_from, new_valid_until,
                    -- New range columns
                    CASE 
                        WHEN old_valid_from IS NOT NULL AND old_valid_until IS NOT NULL 
                        THEN %3$s(old_valid_from::%4$s, old_valid_until::%5$s, '[)')::TEXT
                        ELSE NULL 
                    END as old_valid_range,
                    CASE 
                        WHEN new_valid_from IS NOT NULL AND new_valid_until IS NOT NULL 
                        THEN %3$s(new_valid_from::%4$s, new_valid_until::%5$s, '[)')::TEXT
                        ELSE NULL 
                    END as new_valid_range,
                    data, feedback, trace, grouping_key
                FROM with_statement_seq
                ORDER BY plan_op_seq;
            $SQL$,
                v_final_order_by_expr, /* %1$s */
                v_multirange_type, /* %2$s - multirange type */
                v_range_constructor, /* %3$s - range type */
                v_range_subtype, /* %4$s - range subtype for from cast */
                v_range_subtype /* %5$s - range subtype for until cast */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'query', 'sql', v_sql);
    
            -- Conditionally log the generated SQL for debugging.
            BEGIN
                IF p_log_sql THEN
                    RAISE NOTICE '--- temporal_merge SQL for % ---', temporal_merge_plan.target_table;
                    RAISE NOTICE '%', v_sql;
                END IF;
            END;
    
            -- Store in the session cache with actual table names
            INSERT INTO pg_temp.temporal_merge_plan_cache (cache_key, plan_sqls)
            VALUES (v_plan_key_text, v_plan_sqls);
        END;
    END IF; -- IF v_plan_sqls IS NULL THEN -- Full cache miss

    -- Delete any temp tables that are created by the prepared statments.
    CALL sql_saga.temporal_merge_drop_temp_tables();
    -- Execute all statements in the plan.
    DECLARE
        v_step JSONB;
        v_step_sql TEXT;
        v_final_query TEXT;
        v_step_start TIMESTAMPTZ;
        v_step_num INT := 0;
        v_step_ms NUMERIC;
        v_log_timing BOOLEAN;
    BEGIN
        -- Check if timing logging is enabled via GUC
        v_log_timing := COALESCE(current_setting('sql_saga.log_step_timing', true), 'off') = 'on';
        
        FOR v_step IN SELECT * FROM jsonb_array_elements(v_plan_sqls)
        LOOP
            v_step_num := v_step_num + 1;
            v_step_start := clock_timestamp();
            
            -- Get the SQL for this step (L1 cache has fully-substituted SQL)
            v_step_sql := v_step->>'sql';
            
            IF v_step->>'type' = 'query' THEN
                v_final_query := v_step_sql;
            ELSE
                EXECUTE v_step_sql;
            END IF;
            
            IF v_log_timing THEN
                v_step_ms := EXTRACT(EPOCH FROM clock_timestamp() - v_step_start) * 1000;
                IF v_step_ms > 5 THEN  -- Only log steps taking > 5ms
                    RAISE NOTICE 'Step % (%ms): %', v_step_num, round(v_step_ms, 1), left(v_step_sql, 100);
                END IF;
            END IF;
        END LOOP;

        RETURN QUERY EXECUTE v_final_query;
    END;
END;
$temporal_merge_plan$;
-- Executor for temporal_merge. This procedure contains the DML logic.
CREATE OR REPLACE PROCEDURE sql_saga.temporal_merge_execute(
    target_table regclass,
    source_table regclass,
    identity_columns TEXT[] DEFAULT NULL,
    mode sql_saga.temporal_merge_mode DEFAULT 'MERGE_ENTITY_PATCH',
    era_name name DEFAULT 'valid',
    row_id_column name DEFAULT 'row_id',
    founding_id_column name DEFAULT NULL,
    update_source_with_identity BOOLEAN DEFAULT false,
    lookup_columns TEXT[] DEFAULT NULL,
    delete_mode sql_saga.temporal_merge_delete_mode DEFAULT 'NONE',
    update_source_with_feedback BOOLEAN DEFAULT false,
    feedback_status_column name DEFAULT NULL,
    feedback_status_key name DEFAULT NULL,
    feedback_error_column name DEFAULT NULL,
    feedback_error_key name DEFAULT NULL,
    ephemeral_columns TEXT[] DEFAULT NULL,
    delay_constraints BOOLEAN DEFAULT true
)
LANGUAGE plpgsql AS $temporal_merge_execute$
DECLARE
    v_lookup_columns TEXT[];
    v_target_table_ident TEXT := temporal_merge_execute.target_table::TEXT;
    v_update_set_clause TEXT;
    v_all_cols_ident TEXT;
    v_all_cols_select TEXT;
    v_entity_key_join_clause TEXT;
    v_entity_key_select_list TEXT;
    v_target_schema_name name;
    v_target_table_name_only name;
    v_valid_from_col name;
    v_valid_until_col name;
    v_valid_to_col name;
    v_range_col name;
    v_range_constructor name;
    v_has_gist_index boolean;
    v_has_target_gist_index boolean;
    v_has_lookup_btree_index boolean;
    v_log_index_checks boolean;
    v_log_trace boolean;
    v_log_sql boolean;
    v_log_feedback boolean;
    v_log_vars boolean;
    v_log_execute boolean;
    v_expected_idx_expr_with_bounds text;
    v_expected_idx_expr_default text;
    v_idx_rec record;
    v_source_rel_oid oid;
    v_source_rel_name_for_hint regclass;
    v_range_subtype regtype;
    v_valid_from_col_type regtype;
    v_valid_until_col_type regtype;
    v_insert_defaulted_columns TEXT[];
    v_founding_defaulted_columns TEXT[];
    v_all_cols_from_jsonb TEXT;
    v_founding_all_cols_ident TEXT;
    v_founding_all_cols_from_jsonb TEXT;
    v_pk_cols name[];
    v_feedback_set_clause TEXT;
    v_sql TEXT;
    v_causal_col name;
    v_log_id TEXT;
    v_summary_line TEXT;
    v_not_null_defaulted_cols TEXT[];
    v_update_statement_seqs INT[];
    v_update_statement_seq INT;
    v_source_col_names TEXT[];
    v_cache sql_saga.temporal_merge_executor_cache;
    v_use_native_executor boolean;
    v_log_step_timing boolean;
    v_t0 timestamptz;
    v_t1 timestamptz;
    v_t2 timestamptz;
    v_t3 timestamptz;
    v_t_delete timestamptz;
    v_t_update timestamptz;
    v_t_insert timestamptz;
    v_t_done timestamptz;
    v_partition_join_clauses TEXT[];
    v_partition_plan_filters TEXT[];
    v_partition_select_lists TEXT[];
    v_partition_count INT;
    v_part INT;
BEGIN
    v_log_trace := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.enable_trace', true), ''), 'false')::boolean;
    v_log_sql := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_sql', true), ''), 'false')::boolean;
    v_log_index_checks := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_index_checks', true), ''), 'false')::boolean;
    v_log_feedback := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_feedback', true), ''), 'false')::boolean;
    v_log_vars := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_vars', true), ''), 'false')::boolean;
    v_log_execute := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_execute', true), ''), 'false')::boolean;
    v_log_step_timing := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_step_timing', true), ''), 'false')::boolean;
    v_log_id := substr(md5(COALESCE(current_setting('sql_saga.temporal_merge.log_id_seed', true), random()::text)), 1, 3);
    IF v_log_step_timing THEN v_t0 := clock_timestamp(); END IF;

    -- Identity and lookup columns are assumed to have been discovered and validated by the main temporal_merge procedure.
    v_lookup_columns := COALESCE(temporal_merge_execute.lookup_columns, temporal_merge_execute.identity_columns);
    v_causal_col := COALESCE(temporal_merge_execute.founding_id_column, temporal_merge_execute.row_id_column);

    -- Use the native executor cache if the shared library is available.
    v_use_native_executor := to_regproc('sql_saga.temporal_merge_executor_introspect') IS NOT NULL;

    IF v_use_native_executor THEN
        -- Single function call replaces ~570 lines of introspection + CTE logic.
        -- On cache hit, returns immediately with zero SPI calls.
        SELECT * INTO v_cache FROM sql_saga.temporal_merge_executor_introspect(
            target_table => temporal_merge_execute.target_table::oid,
            source_table => temporal_merge_execute.source_table::oid,
            identity_columns => temporal_merge_execute.identity_columns,
            lookup_columns => v_lookup_columns,
            era_name => temporal_merge_execute.era_name,
            ephemeral_columns => temporal_merge_execute.ephemeral_columns,
            founding_id_column => temporal_merge_execute.founding_id_column,
            row_id_column => temporal_merge_execute.row_id_column
        );

        -- Unpack cached values into local variables
        v_range_col := v_cache.range_col;
        v_range_constructor := v_cache.range_constructor;
        v_range_subtype := v_cache.range_subtype::regtype;
        v_valid_from_col := v_cache.valid_from_col;
        v_valid_until_col := v_cache.valid_until_col;
        v_valid_to_col := v_cache.valid_to_col;
        v_valid_from_col_type := v_cache.valid_from_col_type::regtype;
        v_valid_until_col_type := v_cache.valid_until_col_type::regtype;
        v_pk_cols := v_cache.pk_cols::name[];
        v_not_null_defaulted_cols := v_cache.not_null_defaulted_cols;
        v_insert_defaulted_columns := v_cache.insert_defaulted_columns;
        v_founding_defaulted_columns := v_cache.founding_defaulted_columns;
        v_source_col_names := v_cache.source_col_names;
        v_update_set_clause := v_cache.update_set_clause;
        v_all_cols_ident := v_cache.all_cols_ident;
        v_all_cols_select := v_cache.all_cols_select;
        v_all_cols_from_jsonb := v_cache.all_cols_from_jsonb;
        v_founding_all_cols_ident := v_cache.founding_all_cols_ident;
        v_founding_all_cols_from_jsonb := v_cache.founding_all_cols_from_jsonb;
        v_entity_key_join_clause := v_cache.entity_key_join_clause;
        v_entity_key_select_list := v_cache.entity_key_select_list;
        v_partition_join_clauses := v_cache.partition_join_clauses;
        v_partition_plan_filters := v_cache.partition_plan_filters;
        v_partition_select_lists := v_cache.partition_select_lists;

        -- Still need schema name for constraint deferral later
        SELECT n.nspname INTO v_target_schema_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.oid = temporal_merge_execute.target_table;

    ELSE
        -- Fallback: PL/pgSQL introspection (original code path)

    -- Introspect source columns to determine which columns are actually provided.
    -- This allows us to handle partial source views correctly, especially for defaulted columns.
    SELECT COALESCE(array_agg(attname::text), '{}')
    INTO v_source_col_names
    FROM pg_attribute
    WHERE attrelid = temporal_merge_execute.source_table
      AND attnum > 0
      AND NOT attisdropped;

    -- Introspect columns that are NOT NULL and have a DEFAULT. For these columns,
    -- an incoming NULL in UPSERT mode should preserve the existing value rather
    -- than cause a NOT NULL violation.
    SELECT COALESCE(array_agg(a.attname), '{}')
    INTO v_not_null_defaulted_cols
    FROM pg_catalog.pg_attribute a
    WHERE a.attrelid = temporal_merge_execute.target_table
      AND a.attnum > 0 AND NOT a.attisdropped AND a.atthasdef AND a.attnotnull;

    -- Introspect era information to get the correct column names
    SELECT n.nspname, c.relname
    INTO v_target_schema_name, v_target_table_name_only
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = temporal_merge_execute.target_table;

    SELECT e.range_column_name, e.valid_from_column_name, e.valid_until_column_name, e.valid_to_column_name, e.range_type::name, e.range_subtype
    INTO v_range_col, v_valid_from_col, v_valid_until_col, v_valid_to_col, v_range_constructor, v_range_subtype
    FROM sql_saga.era e
    WHERE e.table_schema = v_target_schema_name
      AND e.table_name = v_target_table_name_only
      AND e.era_name = temporal_merge_execute.era_name;

    IF v_range_col IS NULL THEN
        RAISE EXCEPTION 'No era named "%" found for table "%"', temporal_merge_execute.era_name, sql_saga.__internal_display_name(temporal_merge_execute.target_table);
    END IF;

    -- Introspect the primary key columns, excluding temporal columns.
    -- They will be excluded from UPDATE SET clauses.
    -- Must happen after temporal columns are discovered.
    SELECT COALESCE(array_agg(a.attname ORDER BY array_position(c.conkey, a.attnum)), '{}'::name[])
    INTO v_pk_cols
    FROM pg_constraint c
    JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
    WHERE c.conrelid = temporal_merge_execute.target_table
      AND c.contype = 'p'
      AND a.attname NOT IN (v_range_col, v_valid_from_col, v_valid_until_col);

    END IF; -- v_use_native_executor (introspection phase 1)

    v_summary_line := format(
        'on %s: mode=>%s, delete_mode=>%s, identity_columns=>%L, lookup_columns=>%L, ephemeral_columns=>%L, founding_id_column=>%L, row_id_column=>%L',
        temporal_merge_execute.target_table,
        temporal_merge_execute.mode,
        temporal_merge_execute.delete_mode,
        temporal_merge_execute.identity_columns,
        temporal_merge_execute.lookup_columns,
        temporal_merge_execute.ephemeral_columns,
        temporal_merge_execute.founding_id_column,
        temporal_merge_execute.row_id_column
    );

    -- To ensure idempotency and avoid permission errors in complex transactions
    -- with role changes, we drop and recreate the feedback table for each call.
    IF to_regclass('pg_temp.temporal_merge_feedback') IS NOT NULL THEN
        DROP TABLE pg_temp.temporal_merge_feedback;
    END IF;
    CREATE TEMP TABLE temporal_merge_feedback (LIKE sql_saga.temporal_merge_feedback) ON COMMIT DROP;

    -- Create a unified, session-local cache for index checks to avoid redundant lookups.
    IF to_regclass('pg_temp.temporal_merge_index_cache') IS NULL THEN
        CREATE TEMP TABLE temporal_merge_index_cache (
            rel_oid oid NOT NULL,
            lookup_columns text[], -- NULL for GIST checks. NOT NULL for BTREE checks.
            has_index boolean NOT NULL,
            hint_rel_name regclass, -- NULL for BTREE checks. NOT NULL for GIST checks.

            -- Enforce that this is either a GIST check or a BTREE check.
            CHECK (
                (lookup_columns IS NULL AND hint_rel_name IS NOT NULL) -- GIST
                OR
                (lookup_columns IS NOT NULL AND hint_rel_name IS NULL) -- BTREE
            )
        ) ON COMMIT DROP;

        -- Unique index for GIST checks
        CREATE UNIQUE INDEX ON temporal_merge_index_cache (rel_oid) WHERE lookup_columns IS NULL;
        -- Unique index for BTREE checks
        CREATE UNIQUE INDEX ON temporal_merge_index_cache (rel_oid, lookup_columns) WHERE lookup_columns IS NOT NULL;
    END IF;

    IF v_log_step_timing THEN v_t1 := clock_timestamp(); END IF;

    -- Prepare expected normalized index expressions and logging flag for index checks
    IF v_valid_from_col IS NOT NULL AND v_valid_until_col IS NOT NULL THEN
        v_expected_idx_expr_with_bounds := format('%s(%s,%s,''[)'')', v_range_constructor, v_valid_from_col, v_valid_until_col);
        v_expected_idx_expr_default := format('%s(%s,%s)', v_range_constructor, v_valid_from_col, v_valid_until_col);
    ELSE
        v_expected_idx_expr_with_bounds := NULL;
        v_expected_idx_expr_default := NULL;
    END IF;

    -- Check cache for original source relation OID. This provides a fast path for repeated calls with the same view.
    SELECT has_index, hint_rel_name
    INTO v_has_gist_index, v_source_rel_name_for_hint
    FROM pg_temp.temporal_merge_index_cache AS tmc
    WHERE rel_oid = temporal_merge_execute.source_table AND tmc.lookup_columns IS NULL;

    IF NOT FOUND THEN
        -- On cache miss, resolve the relation if it's a view, perform the check, and populate the cache.
        DECLARE
            v_source_relkind char;
            v_is_view boolean;
        BEGIN
            SELECT c.relkind INTO v_source_relkind
            FROM pg_class c WHERE c.oid = temporal_merge_execute.source_table;
            v_is_view := (v_source_relkind = 'v');

            IF v_is_view THEN
                -- It's a view, find the underlying table. Assumes a simple view with one base table.
                SELECT d.refobjid INTO v_source_rel_oid
                FROM pg_rewrite r
                JOIN pg_depend d ON r.oid = d.objid
                JOIN pg_class c_dep ON c_dep.oid = d.refobjid
                WHERE r.ev_class = temporal_merge_execute.source_table
                  AND d.refclassid = 'pg_class'::regclass
                  AND r.rulename = '_RETURN'
                  AND c_dep.relkind IN ('r', 'p', 'f') -- regular, partitioned, foreign
                LIMIT 1;

                v_source_rel_name_for_hint := v_source_rel_oid;
            ELSE
                v_source_rel_oid := source_table;
                v_source_rel_name_for_hint := source_table;
            END IF;

            -- If we couldn't resolve a base table for a complex view, we cannot check for an index.
            -- To avoid repeated expensive checks for this source, we assume an index exists and cache that assumption.
            IF v_source_rel_oid IS NULL THEN
                RAISE WARNING 'Could not determine the base table for source relation "%" (it may be a complex view). Skipping GIST index performance check.', sql_saga.__internal_display_name(temporal_merge_execute.source_table);
                v_has_gist_index := true;
                v_source_rel_name_for_hint := temporal_merge_execute.source_table; -- Fallback
            ELSE
                -- We have a resolvable base table. Check cache for it.
                SELECT has_index
                INTO v_has_gist_index
                FROM pg_temp.temporal_merge_index_cache AS tmc
                WHERE rel_oid = v_source_rel_oid AND tmc.lookup_columns IS NULL;

                IF NOT FOUND THEN
                    -- Still a cache miss, so perform the actual check on the base table.
                    SELECT EXISTS (
                        -- Check for an index on the range column itself
                        SELECT 1
                        FROM pg_index ix
                        JOIN pg_class i ON i.oid = ix.indexrelid
                        JOIN pg_am am ON am.oid = i.relam
                        JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = ANY(ix.indkey)
                        WHERE ix.indrelid = v_source_rel_oid
                          AND am.amname = 'gist'
                          AND ix.indkey IS NOT NULL AND array_length(ix.indkey, 1) = 1
                          AND a.attname = v_range_col
                        UNION ALL
                        -- Check for an expression-based index on from/until columns (legacy/synchronized)
                        SELECT 1
                        FROM pg_index ix
                        JOIN pg_class i ON i.oid = ix.indexrelid
                        JOIN pg_am am ON am.oid = i.relam
                        WHERE ix.indrelid = v_source_rel_oid
                          AND am.amname = 'gist'
                          AND ix.indexprs IS NOT NULL
                          AND v_expected_idx_expr_with_bounds IS NOT NULL
                          AND (
                              regexp_replace(pg_get_expr(ix.indexprs, ix.indrelid), '\s|::\w+', '', 'g') = v_expected_idx_expr_with_bounds
                              OR regexp_replace(pg_get_expr(ix.indexprs, ix.indrelid), '\s|::\w+', '', 'g') = v_expected_idx_expr_default
                          )
                    )
                    INTO v_has_gist_index;

                    IF v_log_index_checks THEN
                        RAISE NOTICE 'Index check (SOURCE %) expected: "%", or "%"', v_source_rel_name_for_hint, v_expected_idx_expr_with_bounds, v_expected_idx_expr_default;
                        FOR v_idx_rec IN
                            SELECT i.relname AS index_name,
                                   pg_get_indexdef(i.oid) AS indexdef,
                                   regexp_replace(pg_get_expr(ix.indexprs, ix.indrelid), '\s|::\w+', '', 'g') AS normalized_expr
                            FROM pg_index ix
                            JOIN pg_class i ON i.oid = ix.indexrelid
                            JOIN pg_am am ON am.oid = i.relam
                            WHERE ix.indrelid = v_source_rel_oid
                              AND am.amname = 'gist'
                        LOOP
                            RAISE NOTICE 'GiST index: %, normalized expr: %', v_idx_rec.indexdef, v_idx_rec.normalized_expr;
                        END LOOP;
                        RAISE NOTICE 'Detected has_gist_index(SOURCE): %', v_has_gist_index;
                    END IF;

                    -- Populate cache for the base table.
                    INSERT INTO pg_temp.temporal_merge_index_cache (rel_oid, lookup_columns, has_index, hint_rel_name)
                    VALUES (v_source_rel_oid, NULL, v_has_gist_index, v_source_rel_name_for_hint);
                END IF;
            END IF;

            -- Now, populate the cache for the original source relation OID (which could be the view).
            -- This is the key optimization for subsequent calls with the same view.
            -- We only need to do this if the original source was different from the resolved one (i.e., it was a view).
            IF temporal_merge_execute.source_table <> v_source_rel_oid THEN
                INSERT INTO pg_temp.temporal_merge_index_cache (rel_oid, lookup_columns, has_index, hint_rel_name)
                VALUES (temporal_merge_execute.source_table, NULL, v_has_gist_index, v_source_rel_name_for_hint);
            END IF;
        END;
    END IF;

    IF NOT v_has_gist_index AND v_source_rel_name_for_hint IS NOT NULL THEN
        DECLARE
            v_source_row_count REAL;
        BEGIN
            -- Use the resolved base table OID for the row count check, as views often have reltuples=0.
            SELECT c.reltuples INTO v_source_row_count FROM pg_class c WHERE c.oid = v_source_rel_name_for_hint::oid;
            IF v_source_row_count >= 512 THEN
                RAISE WARNING 'Performance warning: The source relation % lacks a GIST index on its temporal columns.', temporal_merge_execute.source_table
                USING HINT = format('For better performance, consider creating an index, e.g., CREATE INDEX ON %s USING GIST (%I);',
                    v_source_rel_name_for_hint,
                    v_range_col
                );
            END IF;
        END;
    END IF;

    -- Check for GiST index on target table's temporal range.
    -- We have already validated that the target table is a sql_saga era (see the earlier SELECT from sql_saga.era),
    -- and loaded v_valid_from_col, v_valid_until_col and v_range_constructor accordingly. Therefore it is both
    -- correct and sufficient to check for a GiST index on the expression
    --   range_constructor(valid_from, valid_until, '[)')
    -- for performance. Checking for a primary key is orthogonal and does not affect range search performance here.
    SELECT EXISTS (
        -- Check for an index on the range column itself
        SELECT 1
        FROM pg_index ix
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am ON am.oid = i.relam
        JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = ANY(ix.indkey)
        WHERE ix.indrelid = v_target_table_ident::regclass
          AND am.amname = 'gist'
          AND ix.indkey IS NOT NULL AND array_length(ix.indkey, 1) = 1
          AND a.attname = v_range_col
        UNION ALL
        -- Check for an expression-based index on from/until columns (legacy/synchronized)
        SELECT 1
        FROM pg_index ix
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am ON am.oid = i.relam
        WHERE ix.indrelid = v_target_table_ident::regclass
          AND am.amname = 'gist'
          AND ix.indexprs IS NOT NULL
          AND v_expected_idx_expr_with_bounds IS NOT NULL
          AND (
              regexp_replace(pg_get_expr(ix.indexprs, ix.indrelid), '\s|::\w+', '', 'g') = v_expected_idx_expr_with_bounds
              OR regexp_replace(pg_get_expr(ix.indexprs, ix.indrelid), '\s|::\w+', '', 'g') = v_expected_idx_expr_default
          )
    )
    INTO v_has_target_gist_index;

    IF v_log_index_checks THEN
        RAISE NOTICE 'Index check (TARGET %) expected: "%", or "%"', temporal_merge_execute.target_table, v_expected_idx_expr_with_bounds, v_expected_idx_expr_default;
        FOR v_idx_rec IN
            SELECT i.relname AS index_name,
                   pg_get_indexdef(i.oid) AS indexdef,
                   regexp_replace(pg_get_expr(ix.indexprs, ix.indrelid), '\s|::\w+', '', 'g') AS normalized_expr
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_am am ON am.oid = i.relam
            WHERE ix.indrelid = v_target_table_ident::regclass
              AND am.amname = 'gist'
        LOOP
            RAISE NOTICE 'GiST index: %, normalized expr: %', v_idx_rec.indexdef, v_idx_rec.normalized_expr;
        END LOOP;
        RAISE NOTICE 'Detected has_gist_index(TARGET): %', v_has_target_gist_index;
    END IF;

    IF NOT v_has_target_gist_index THEN
        DECLARE
            v_target_row_count REAL;
        BEGIN
            SELECT c.reltuples INTO v_target_row_count FROM pg_class c WHERE c.oid = temporal_merge_execute.target_table;
            IF v_target_row_count >= 512 THEN
                RAISE WARNING 'Performance warning: The target relation % lacks a GIST index on its temporal columns.', temporal_merge_execute.target_table
                USING HINT = format('For better performance, consider creating an index, e.g., CREATE INDEX ON %s USING GIST (%I);',
                    v_target_table_ident,
                    v_range_col
                );
            END IF;
        END;
    END IF;

    -- Check for performance-critical BTREE index on target table's lookup columns.
    -- This check is cached per transaction to avoid redundant lookups.
    SELECT has_index
    INTO v_has_lookup_btree_index
    FROM pg_temp.temporal_merge_index_cache AS tmc
    WHERE rel_oid = temporal_merge_execute.target_table
      AND tmc.lookup_columns = v_lookup_columns;

    IF NOT FOUND THEN
        SELECT EXISTS (
            SELECT 1
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_am am ON am.oid = i.relam
            WHERE ix.indrelid = v_target_table_ident::regclass
              AND am.amname = 'btree'
              AND array_length(ix.indkey, 1) >= cardinality(v_lookup_columns)
              AND (
                  SELECT array_agg(a.attname ORDER BY k.ord)
                  FROM unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord)
                  JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = k.attnum
                  WHERE k.ord <= cardinality(v_lookup_columns)
              )::name[] = v_lookup_columns::name[]
        )
        INTO v_has_lookup_btree_index;

        INSERT INTO pg_temp.temporal_merge_index_cache (rel_oid, lookup_columns, has_index, hint_rel_name)
        VALUES (temporal_merge_execute.target_table, v_lookup_columns, v_has_lookup_btree_index, NULL);
    END IF;

    IF NOT v_has_lookup_btree_index THEN
        DECLARE
            v_target_row_count REAL;
        BEGIN
            SELECT c.reltuples INTO v_target_row_count FROM pg_class c WHERE c.oid = temporal_merge_execute.target_table;
            IF v_target_row_count >= 512 THEN
                RAISE NOTICE 'Performance hint: Consider adding a BTREE index on the target relation''s lookup columns (%) to accelerate entity filtering.', v_lookup_columns
                USING HINT = format(
                    'CREATE INDEX ON %s (%s);',
                    v_target_table_ident,
                    (SELECT string_agg(format('%I', col), ', ') FROM unnest(v_lookup_columns) AS col)
                );
            END IF;
        END;
    END IF;

    IF v_log_step_timing THEN v_t2 := clock_timestamp(); END IF;

    IF NOT v_use_native_executor THEN
    -- Fallback: PL/pgSQL introspection for defaulted columns, column types, entity keys, and CTE

    -- Auto-detect columns that should be excluded from INSERT statements.
    -- We only exclude columns that cannot accept values (GENERATED ALWAYS).
    -- Columns with DEFAULTs (atthasdef) and IDENTITY BY DEFAULT (attidentity='d') are INCLUDED
    -- so that we can preserve values from history (payload).
    -- For these columns, we use COALESCE(payload, default) in the INSERT generation to handle missing values.
    SELECT COALESCE(array_agg(a.attname), '{}')
    INTO v_insert_defaulted_columns
    FROM pg_catalog.pg_attribute a
    WHERE a.attrelid = temporal_merge_execute.target_table
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND (a.attidentity = 'a' OR a.attgenerated <> '');

    -- For founding INSERTs of new entities:
    -- - Exclude IDENTITY BY DEFAULT ('d') so the database generates new IDs (sequences).
    -- - Exclude GENERATED ALWAYS ('a') and GENERATED columns.
    -- - INCLUDE columns with standard DEFAULTs (atthasdef) so we can COALESCE(payload, default).
    --   This allows explicit values from source (if provided) or falls back to default.
    SELECT COALESCE(array_agg(a.attname), '{}')
    INTO v_founding_defaulted_columns
    FROM pg_catalog.pg_attribute a
    WHERE a.attrelid = temporal_merge_execute.target_table
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND (a.attidentity IN ('a', 'd') OR a.attgenerated <> '');

    -- Also exclude synchronized columns, as the trigger will populate them.
    IF v_valid_from_col IS NOT NULL THEN
        v_insert_defaulted_columns := v_insert_defaulted_columns || v_valid_from_col;
    END IF;
    IF v_valid_until_col IS NOT NULL THEN
        v_insert_defaulted_columns := v_insert_defaulted_columns || v_valid_until_col;
    END IF;
    IF v_valid_to_col IS NOT NULL THEN
        v_insert_defaulted_columns := v_insert_defaulted_columns || v_valid_to_col;
    END IF;
    IF v_range_col IS NOT NULL THEN
        v_insert_defaulted_columns := v_insert_defaulted_columns || v_range_col;
    END IF;

    IF v_valid_from_col IS NOT NULL THEN
        SELECT atttypid::regtype INTO v_valid_from_col_type FROM pg_attribute WHERE attrelid = temporal_merge_execute.target_table AND attname = v_valid_from_col;
    ELSE
        -- For range-only tables, use the range subtype for casting
        v_valid_from_col_type := v_range_subtype;
    END IF;
    IF v_valid_until_col IS NOT NULL THEN
        SELECT atttypid::regtype INTO v_valid_until_col_type FROM pg_attribute WHERE attrelid = temporal_merge_execute.target_table AND attname = v_valid_until_col;
    ELSE
        -- For range-only tables, use the range subtype for casting
        v_valid_until_col_type := v_range_subtype;
    END IF;

    -- Dynamically construct join clause for composite entity key.
    -- PERFORMANCE OPTIMIZATION: Pre-extract JSONB keys in the subquery SELECT list
    -- so PostgreSQL can use them directly in Hash/Merge join conditions.
    -- Without this, JSONB extraction in the WHERE clause forces a cross-join
    -- with post-filtering, causing O(N*M) comparisons instead of O(N+M).
    --
    -- For NOT NULL columns: use = for Hash/Merge join performance.
    -- For nullable columns (XOR pattern like legal_unit_id/establishment_id):
    -- use COALESCE sentinel to enable NULL=NULL matching while preserving Hash Join.
    -- IS NOT DISTINCT FROM was tested on PG18 and caused performance issues.
    SELECT
        string_agg(
            format(
                '(entity_keys->>%1$L)::%2$s AS %3$I',
                col,
                format_type(a.atttypid, -1),
                col || '_ek'  -- Suffix to avoid collision with other plan columns
            ),
            ', ' ORDER BY ic.ord
        ),
        string_agg(
            CASE WHEN a.attnotnull THEN
                format(
                    't.%1$I = p.%2$I',
                    col,
                    col || '_ek'
                )
            ELSE
                format(
                    'COALESCE(t.%1$I, (-1)::%3$s) = COALESCE(p.%2$I, (-1)::%3$s)',
                    col,
                    col || '_ek',
                    format_type(a.atttypid, -1)
                )
            END,
            ' AND ' ORDER BY ic.ord
        )
    INTO
        v_entity_key_select_list,
        v_entity_key_join_clause
    FROM unnest(identity_columns) WITH ORDINALITY AS ic(col, ord)
    JOIN pg_attribute a ON a.attrelid = temporal_merge_execute.target_table AND a.attname = ic.col AND NOT a.attisdropped;

    v_entity_key_join_clause := COALESCE(v_entity_key_join_clause, 'true');
    v_entity_key_select_list := COALESCE(v_entity_key_select_list, '');

    -- Partition arrays: single partition for PL/pgSQL fallback (no COALESCE elimination)
    v_partition_join_clauses := ARRAY[v_entity_key_join_clause];
    v_partition_plan_filters := ARRAY['true'];
    v_partition_select_lists := ARRAY[v_entity_key_select_list];

        -- Get dynamic column lists for DML. The data columns are defined as all columns
        -- in the target table, minus the identity and temporal boundary columns.
        -- This is intentionally different from the planner's introspection, as the executor
        -- must be able to handle a final payload that contains columns inherited from
        -- the target's history, which may not be present in the source.
        WITH target_cols AS (
            SELECT pa.attname, pa.atttypid, pa.attgenerated, pa.attidentity, pa.attnum, pa.atthasdef, pa.attnotnull
            FROM pg_catalog.pg_attribute pa
            WHERE pa.attrelid = temporal_merge_execute.target_table AND pa.attnum > 0 AND NOT pa.attisdropped
        ),
        common_data_cols AS (
            SELECT t.attname, t.atttypid, t.attnotnull, t.atthasdef, t.attnum
            FROM target_cols t
            LEFT JOIN pg_attrdef ad ON ad.adrelid = temporal_merge_execute.target_table AND ad.adnum = t.attnum
            WHERE (v_range_col IS NULL OR t.attname <> v_range_col)
              AND (v_valid_from_col IS NULL OR t.attname <> v_valid_from_col)
              AND (v_valid_until_col IS NULL OR t.attname <> v_valid_until_col)
              AND (v_valid_to_col IS NULL OR t.attname <> v_valid_to_col)
              AND t.attname <> ALL(COALESCE(temporal_merge_execute.identity_columns, '{}'))
              AND t.attname <> ALL(v_lookup_columns)
              AND t.attname <> ALL(COALESCE(v_pk_cols, '{}'))
              AND t.attidentity <> 'a' -- Exclude GENERATED ALWAYS AS IDENTITY
              AND t.attgenerated = '' -- Exclude GENERATED ... STORED
              AND COALESCE(pg_get_expr(ad.adbin, temporal_merge_execute.target_table), '') NOT ILIKE 'nextval(%'
        ),
        all_available_cols AS (
            SELECT c.attname, c.atttypid, c.attnotnull, c.atthasdef, c.attnum FROM common_data_cols c
            UNION
            SELECT u.attname, t.atttypid, t.attnotnull, t.atthasdef, t.attnum
            FROM unnest(v_lookup_columns) u(attname)
            JOIN target_cols t ON u.attname = t.attname
            UNION
            SELECT u.attname, t.atttypid, t.attnotnull, t.atthasdef, t.attnum
            FROM unnest(COALESCE(temporal_merge_execute.identity_columns, '{}')) u(attname)
            JOIN target_cols t ON u.attname = t.attname
            UNION
            SELECT pk.attname, t.atttypid, t.attnotnull, t.atthasdef, t.attnum
            FROM unnest(v_pk_cols) pk(attname)
            JOIN target_cols t ON pk.attname = t.attname
        ),
        cols_for_insert AS (
            -- All available columns that DON'T have a default...
            SELECT attname, atttypid, attnotnull, atthasdef, attnum FROM all_available_cols WHERE attname <> ALL(v_insert_defaulted_columns)
            UNION
            -- ...plus all identity and lookup columns and primary key columns, which must be provided for SCD-2 inserts.
            SELECT attname, atttypid, attnotnull, atthasdef, attnum FROM all_available_cols WHERE attname = ANY(COALESCE(temporal_merge_execute.identity_columns, '{}')) OR attname = ANY(v_lookup_columns) OR attname = ANY(v_pk_cols)
        ),
        cols_for_founding_insert AS (
            -- For "founding" INSERTs of new entities, we only include columns that do NOT have a default.
            -- This allows serial/identity columns to be generated by the database.
            SELECT attname, atttypid, attnotnull, atthasdef, attnum
            FROM all_available_cols
            WHERE attname <> ALL(v_founding_defaulted_columns)
        )
        SELECT
            (SELECT string_agg(
                format(
                    '%1$I = CASE WHEN p.data ? %2$L THEN %4$s ELSE t.%1$I END',
                    cdc.attname,
                    cdc.attname,
                    format_type(cdc.atttypid, -1),
                    -- For NOT NULL columns with a DEFAULT, COALESCE with the existing value to prevent NULL overwrites.
                    CASE
                        WHEN cdc.attname = ANY(v_not_null_defaulted_cols)
                        THEN format('COALESCE((p.data->>%1$L)::%2$s, t.%3$I)', cdc.attname, format_type(cdc.atttypid, -1), cdc.attname)
                        ELSE format('(p.data->>%1$L)::%2$s', cdc.attname, format_type(cdc.atttypid, -1))
                    END
                ),
            ', ') FROM common_data_cols cdc),
            (SELECT string_agg(format('%I', cfi.attname), ', ' ORDER BY cfi.attnum) FROM cols_for_insert cfi WHERE cfi.attname IS DISTINCT FROM v_valid_from_col AND cfi.attname IS DISTINCT FROM v_valid_until_col),
            -- For Step 1.3 (historical slices for new entities via jsonb_populate_record):
            -- NOT NULL DEFAULT columns need COALESCE because jsonb_populate_record returns NULL for missing keys.
            (SELECT string_agg(
                CASE
                    WHEN cfi.attnotnull AND cfi.atthasdef THEN
                        format('COALESCE(jpr_all.%1$I, %2$s)',
                            cfi.attname,
                            pg_get_expr(ad.adbin, ad.adrelid)
                        )
                    ELSE
                        format('jpr_all.%I', cfi.attname)
                END,
                ', ' ORDER BY cfi.attnum
             )
             FROM cols_for_insert cfi
             LEFT JOIN pg_attrdef ad ON ad.adrelid = temporal_merge_execute.target_table
                                    AND ad.adnum = cfi.attnum
             WHERE cfi.attname IS DISTINCT FROM v_valid_from_col AND cfi.attname IS DISTINCT FROM v_valid_until_col),
            (SELECT string_agg(
                -- For NOT NULL DEFAULT columns, we must coalesce missing payload values with the DB default expression.
                -- This allows us to include the column in the INSERT (for payload preservation) without risking NULL violation.
                CASE
                    WHEN cfi.attnotnull AND cfi.atthasdef THEN
                        format('COALESCE((s.full_data->>%1$L)::%2$s, %3$s)',
                            cfi.attname,
                            format_type(cfi.atttypid, -1),
                            pg_get_expr(ad.adbin, ad.adrelid)
                        )
                    ELSE
                        format('(s.full_data->>%1$L)::%2$s', cfi.attname, format_type(cfi.atttypid, -1))
                END,
                ', ' ORDER BY cfi.attnum
             )
             FROM cols_for_insert cfi
             LEFT JOIN pg_attrdef ad ON ad.adrelid = temporal_merge_execute.target_table
                                    AND ad.adnum = cfi.attnum
             WHERE cfi.attname IS DISTINCT FROM v_valid_from_col AND cfi.attname IS DISTINCT FROM v_valid_until_col),
            (SELECT string_agg(format('%I', cffi.attname), ', ' ORDER BY cffi.attnum) FROM cols_for_founding_insert cffi WHERE cffi.attname IS DISTINCT FROM v_valid_from_col AND cffi.attname IS DISTINCT FROM v_valid_until_col),
            (SELECT string_agg(
                -- Same logic for founding inserts: Coalesce with default if NOT NULL and has default.
                CASE
                    WHEN cffi.attnotnull AND cffi.atthasdef THEN
                        format('COALESCE((s.full_data->>%1$L)::%2$s, %3$s)',
                            cffi.attname,
                            format_type(cffi.atttypid, -1),
                            pg_get_expr(ad.adbin, ad.adrelid)
                        )
                    ELSE
                        format('(s.full_data->>%1$L)::%2$s', cffi.attname, format_type(cffi.atttypid, -1))
                END,
                ', ' ORDER BY cffi.attnum
             )
             FROM cols_for_founding_insert cffi
             LEFT JOIN pg_attrdef ad ON ad.adrelid = temporal_merge_execute.target_table
                                    AND ad.adnum = cffi.attnum
             WHERE cffi.attname IS DISTINCT FROM v_valid_from_col AND cffi.attname IS DISTINCT FROM v_valid_until_col)
        INTO
            v_update_set_clause,
            v_all_cols_ident,
            v_all_cols_select,
            v_all_cols_from_jsonb,
            v_founding_all_cols_ident,
            v_founding_all_cols_from_jsonb;

    END IF; -- NOT v_use_native_executor (introspection phase 2)

        v_partition_count := COALESCE(array_length(v_partition_join_clauses, 1), 0);

        IF v_log_step_timing THEN
            v_t3 := clock_timestamp();
            RAISE NOTICE '(%) step_timing: introspection=%.1fms, index_checks=%.1fms, column_list_cte=%.1fms',
                v_log_id,
                extract(epoch from v_t1 - v_t0) * 1000,
                extract(epoch from v_t2 - v_t1) * 1000,
                extract(epoch from v_t3 - v_t2) * 1000;
        END IF;

        -- DELETE -> UPDATE -> INSERT order is critical for PostgreSQL 18 temporal FK compatibility.
        DECLARE
            v_deferrable_constraints text[];
            v_set_constraints_sql text;
            v_old_search_path TEXT;
        BEGIN
            /*
             * sql_saga's referenced-side regular foreign key triggers on the target are deferred whatever
             * delay_constraints says: a regular reference only needs the key to exist at some time, so a merge is
             * judged by the timeline it leaves, and a reshaped timeline (a DELETE then an INSERT) neither refuses
             * nor cascades (ON DELETE CASCADE). delay_constraints => false keeps the native constraints immediate.
             */
            IF delay_constraints OR EXISTS (
                SELECT FROM pg_catalog.pg_trigger AS t
                WHERE t.tgrelid = target_table AND t.tgconstraint <> 0
                  AND t.tgfoid IN ('sql_saga.uk_update_check_c()'::pg_catalog.regprocedure,
                                   'sql_saga.uk_delete_check_c()'::pg_catalog.regprocedure))
            THEN
                -- For DELETE-first timeline operations, we create temporary gaps that may violate
                -- foreign key constraints. We need to defer:
                -- 1. Unique/exclusion constraints ON the target table (prevents overlaps)
                -- 2. Foreign keys FROM other tables referencing the target table (tolerates gaps during DELETE)
                -- 3. sql_saga's own constraint triggers for regular foreign keys (contype 't'): the
                --    referenced-side triggers on the target, so a reshaped timeline is checked once the
                --    replacement rows are in, and the referencing-side trigger when the target is a child.
                -- With delay_constraints => false, only the referenced-side triggers of 3.
                DECLARE
                    v_intended oid[];
                    v_conflict record;
                BEGIN
                    SELECT array_agg(c.oid ORDER BY c.oid)
                    INTO v_intended
                    FROM pg_catalog.pg_constraint AS c
                    WHERE c.condeferrable
                      AND (
                          (delay_constraints AND c.conrelid = target_table AND c.contype IN ('u', 'x'))
                          OR
                          (delay_constraints AND c.confrelid = target_table AND c.contype = 'f')
                          OR
                          (c.conrelid = target_table AND c.contype = 't' AND EXISTS (
                              SELECT FROM pg_catalog.pg_trigger AS t
                              WHERE t.tgconstraint = c.oid
                                AND (t.tgfoid IN ('sql_saga.uk_update_check_c()'::pg_catalog.regprocedure,
                                                  'sql_saga.uk_delete_check_c()'::pg_catalog.regprocedure)
                                     OR (delay_constraints AND t.tgfoid = 'sql_saga.regular_fk_check_c()'::pg_catalog.regprocedure))))
                      );

                    /*
                     * SET CONSTRAINTS schema.name selects every constraint of that name in the schema, not one OID.
                     * Refuse, before any DML, when that would also select a constraint outside the intended set:
                     * deferring it (and later forcing it immediate) would change the checking moment of an object
                     * this merge has no business with.
                     */
                    SELECT pg_catalog.format('%I.%I', n.nspname, c.conname) AS deferred_name,
                           other.conname AS other_name,
                           pg_catalog.format('%I.%I', on_.nspname, oc.relname) AS other_table,
                           EXISTS (SELECT FROM pg_catalog.pg_trigger AS t
                                   WHERE t.tgconstraint = other.oid
                                     AND t.tgfoid IN ('sql_saga.uk_update_check_c()'::pg_catalog.regprocedure,
                                                      'sql_saga.uk_delete_check_c()'::pg_catalog.regprocedure,
                                                      'sql_saga.regular_fk_check_c()'::pg_catalog.regprocedure))
                           OR EXISTS (SELECT FROM sql_saga.unique_keys AS uk
                                      WHERE (uk.table_schema, uk.table_name) = (on_.nspname, oc.relname)
                                        AND other.conname IN (uk.unique_constraint, uk.exclude_constraint))
                           OR EXISTS (SELECT FROM sql_saga.foreign_keys AS fk
                                      WHERE (fk.table_schema, fk.table_name, fk.foreign_key_name)
                                          = (on_.nspname, oc.relname, other.conname)) AS other_managed
                    INTO v_conflict
                    FROM pg_catalog.pg_constraint AS c
                    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.connamespace
                    JOIN pg_catalog.pg_constraint AS other
                      ON (other.connamespace, other.conname) = (c.connamespace, c.conname)
                     AND other.oid <> ALL (v_intended)
                    JOIN pg_catalog.pg_class AS oc ON oc.oid = other.conrelid
                    JOIN pg_catalog.pg_namespace AS on_ ON on_.oid = oc.relnamespace
                    WHERE c.oid = ANY (v_intended)
                    ORDER BY c.oid, other.oid
                    LIMIT 1;

                    IF FOUND THEN
                        IF v_conflict.other_managed THEN
                            RAISE EXCEPTION 'sql_saga: temporal_merge on table % cannot defer constraint %: SET CONSTRAINTS by that name would also defer constraint % on table %, which belongs to another sql_saga object; rename one of them',
                                (SELECT pg_catalog.format('%I.%I', tn.nspname, tc.relname) FROM pg_catalog.pg_class AS tc JOIN pg_catalog.pg_namespace AS tn ON tn.oid = tc.relnamespace WHERE tc.oid = target_table),
                                v_conflict.deferred_name, pg_catalog.quote_ident(v_conflict.other_name), v_conflict.other_table
                                USING ERRCODE = 'duplicate_object';
                        END IF;
                        RAISE EXCEPTION 'sql_saga: temporal_merge on table % cannot defer constraint %: SET CONSTRAINTS by that name would also defer constraint % on table %, which sql_saga does not manage; rename that constraint',
                            (SELECT pg_catalog.format('%I.%I', tn.nspname, tc.relname) FROM pg_catalog.pg_class AS tc JOIN pg_catalog.pg_namespace AS tn ON tn.oid = tc.relnamespace WHERE tc.oid = target_table),
                            v_conflict.deferred_name, pg_catalog.quote_ident(v_conflict.other_name), v_conflict.other_table
                            USING ERRCODE = 'duplicate_object';
                    END IF;

                    -- Names are schema-qualified: SET CONSTRAINTS resolves a bare name through search_path.
                    SELECT array_agg(DISTINCT pg_catalog.format('%I.%I', n.nspname, c.conname))
                    INTO v_deferrable_constraints
                    FROM pg_catalog.pg_constraint AS c
                    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.connamespace
                    WHERE c.oid = ANY (v_intended);
                END;

                v_old_search_path := current_setting('search_path');
                EXECUTE format('SET search_path = %I, public', v_target_schema_name);

                IF v_deferrable_constraints IS NOT NULL THEN
                    v_set_constraints_sql := format('SET CONSTRAINTS %s DEFERRED',
                        array_to_string(v_deferrable_constraints, ', ')
                    );
                    IF v_log_execute THEN RAISE NOTICE '(%) Executing: %', v_log_id, v_set_constraints_sql; END IF;
                    EXECUTE v_set_constraints_sql;
                END IF;
            END IF;

            -- This block contains the DML. We wrap it to ensure constraints and search_path are reset.
            BEGIN
                -- PERFORMANCE: Analyze the plan table to give the optimizer accurate row estimates.
                -- Without this, PostgreSQL underestimates cardinality (often rows=1 for filtered enums)
                -- which causes it to choose Nested Loop joins instead of Hash Joins, resulting in
                -- 70x+ slower DML execution. This is critical for UPDATE operations that join
                -- temporal_merge_plan to the target table.
                ANALYZE temporal_merge_plan;

                -- Execute operations ordered by plan_op_seq: DELETE -> UPDATE -> INSERT
                -- The planner generates plan_op_seq to ensure DELETE operations come first,
                -- then UPDATEs (ordered by effect), then INSERTs. This prevents temporal overlaps.
                IF v_log_step_timing THEN v_t_delete := clock_timestamp(); END IF;
                IF v_log_execute THEN
                    RAISE NOTICE '(%) --- temporal_merge_execute: EXECUTING DELETES ---', v_log_id;
                END IF;
                -- 1. Execute DELETE operations (partitioned by nullable identity columns)
                -- Pre-extract entity_keys columns in subquery for efficient join.
                IF (SELECT TRUE FROM temporal_merge_plan WHERE operation = 'DELETE' LIMIT 1) THEN
                    FOR v_part IN 1..v_partition_count LOOP
                        v_sql := format($$ DELETE FROM %1$s t
                            USING (SELECT *, %5$s FROM temporal_merge_plan WHERE operation = 'DELETE' AND %6$s) p
                            WHERE %2$s AND t.%3$I = p.old_valid_range::%4$I;
                        $$, v_target_table_ident, v_partition_join_clauses[v_part], v_range_col, v_range_constructor, v_partition_select_lists[v_part], v_partition_plan_filters[v_part]);
                        IF v_log_sql THEN RAISE NOTICE '(%) DELETE partition %/% SQL: %', v_log_id, v_part, v_partition_count, v_sql; END IF;
                        EXECUTE v_sql;
                    END LOOP;
                END IF;

                IF v_log_step_timing THEN v_t_update := clock_timestamp(); END IF;
                IF v_log_execute THEN
                    RAISE NOTICE '(%) --- temporal_merge_execute: EXECUTING UPDATES ---', v_log_id;
                END IF;
                -- 2. Execute UPDATE operations (batched by statement_seq, partitioned by nullable identity columns)
                -- The planner assigns statement_seq to group operations that can safely execute together:
                -- - NONE/SHRINK operations share a statement_seq (they only contract ranges)
                -- - MOVE operations each get their own statement_seq (can create temporary overlaps)
                -- - GROW operations share a statement_seq (they only expand into gaps)
                -- This minimizes the number of SQL statements while preventing constraint violations.
                SELECT ARRAY_AGG(DISTINCT statement_seq ORDER BY statement_seq) INTO v_update_statement_seqs
                FROM temporal_merge_plan WHERE operation = 'UPDATE';

                IF v_update_statement_seqs IS NOT NULL THEN
                    FOREACH v_update_statement_seq IN ARRAY v_update_statement_seqs LOOP
                        FOR v_part IN 1..v_partition_count LOOP
                            IF v_update_set_clause IS NOT NULL THEN
                                -- Pre-extract entity_keys columns in subquery for efficient join.
                                -- This allows PostgreSQL to use Hash/Merge joins instead of cross-join with filter.
                                v_sql := format($$ UPDATE %1$s t SET %4$I = p.new_valid_range::%5$I, %2$s
                                    FROM (SELECT *, %9$s FROM temporal_merge_plan WHERE operation = 'UPDATE' AND statement_seq = %8$s AND %10$s ORDER BY plan_op_seq) p
                                    WHERE %3$s AND t.%4$I = p.old_valid_range::%5$I;
                                $$, v_target_table_ident, v_update_set_clause, v_partition_join_clauses[v_part], v_range_col, v_range_constructor, NULL, NULL, v_update_statement_seq, v_partition_select_lists[v_part], v_partition_plan_filters[v_part]);
                                IF v_log_sql THEN RAISE NOTICE '(%) UPDATE partition %/% seq=% SQL: %', v_log_id, v_part, v_partition_count, v_update_statement_seq, v_sql; END IF;
                                EXECUTE v_sql;
                            ELSIF v_all_cols_ident IS NOT NULL THEN
                                v_sql := format($$ UPDATE %1$s t SET %3$I = p.new_valid_range::%4$I
                                    FROM (SELECT *, %8$s FROM temporal_merge_plan WHERE operation = 'UPDATE' AND statement_seq = %7$s AND %9$s ORDER BY plan_op_seq) p
                                    WHERE %2$s AND t.%3$I = p.old_valid_range::%4$I;
                                $$, v_target_table_ident, v_partition_join_clauses[v_part], v_range_col, v_range_constructor, NULL, NULL, v_update_statement_seq, v_partition_select_lists[v_part], v_partition_plan_filters[v_part]);
                                IF v_log_sql THEN RAISE NOTICE '(%) UPDATE partition %/% seq=% SQL: %', v_log_id, v_part, v_partition_count, v_update_statement_seq, v_sql; END IF;
                                EXECUTE v_sql;
                            END IF;
                        END LOOP;
                    END LOOP;
                END IF;

                IF v_log_step_timing THEN v_t_insert := clock_timestamp(); END IF;
                IF v_log_execute THEN
                    RAISE NOTICE '(%) --- temporal_merge_execute: EXECUTING INSERTS ---', v_log_id;
                END IF;
                -- 3. Execute INSERT operations and capture generated IDs
                IF v_all_cols_ident IS NOT NULL THEN
                     DECLARE
                        v_entity_id_update_jsonb_build TEXT;
                        v_needs_founding_insert BOOLEAN;
                     BEGIN
                        EXECUTE 'SELECT EXISTS(SELECT 1 FROM temporal_merge_plan WHERE operation = ''INSERT'' AND is_new_entity)'
                        INTO v_needs_founding_insert;

                        -- Build the expression to construct the entity_keys feedback JSONB.
                        WITH target_cols AS (
                            SELECT pa.attname
                            FROM pg_catalog.pg_attribute pa
                            WHERE pa.attrelid = v_target_table_ident::regclass
                              AND pa.attnum > 0 AND NOT pa.attisdropped
                        ),
                        feedback_id_cols AS (
                            SELECT col FROM unnest(COALESCE(temporal_merge_execute.identity_columns, '{}')) as col
                            UNION
                            SELECT pk_col FROM unnest(v_pk_cols) as pk_col WHERE pk_col IN (SELECT attname FROM target_cols) AND pk_col NOT IN (v_valid_from_col, v_valid_until_col)
                        )
                        SELECT
                            format('jsonb_build_object(%s)', string_agg(format('%L, ir.%I', col, col), ', '))
                        INTO
                            v_entity_id_update_jsonb_build
                        FROM feedback_id_cols;

                        -- Stage 1: Handle "founding" inserts for new entities that need generated keys.
                        IF v_needs_founding_insert THEN
                            CREATE TEMP TABLE temporal_merge_entity_id_map (grouping_key TEXT PRIMARY KEY, causal_id TEXT, new_entity_keys JSONB) ON COMMIT DROP;

                            -- Step 1.1: Insert just ONE "founding" row for each new conceptual entity to generate its ID.
                            EXECUTE format($$
                                WITH founding_plan_ops AS (
                                    SELECT DISTINCT ON (p.grouping_key)
                                        p.plan_op_seq,
                                        p.causal_id,
                                        p.grouping_key,
                                        p.new_valid_range,
                                        p.entity_keys || p.data as full_data
                                    FROM temporal_merge_plan p
                                    WHERE p.operation = 'INSERT' AND p.is_new_entity
                                    ORDER BY p.grouping_key, p.plan_op_seq
                                ),
                                id_map_cte AS (
                                    MERGE INTO %1$s t
                                    USING founding_plan_ops s ON false
                                    WHEN NOT MATCHED THEN
                                        INSERT (%2$s, %5$I)
                                        VALUES (%3$s, s.new_valid_range::%7$I)
                                    RETURNING t.*, s.causal_id, s.grouping_key
                                )
                                INSERT INTO temporal_merge_entity_id_map (grouping_key, causal_id, new_entity_keys)
                                SELECT
                                    ir.grouping_key,
                                    ir.causal_id,
                                    %4$s
                                FROM id_map_cte ir;
                            $$,
                                v_target_table_ident,           /* %1$s */
                                v_founding_all_cols_ident,      /* %2$s */
                                v_founding_all_cols_from_jsonb, /* %3$s */
                                v_entity_id_update_jsonb_build, /* %4$s */
                                v_range_col,                    /* %5$I */
                                NULL,                           /* %6$I (placeholder) */
                                v_range_constructor             /* %7$I */
                            );

                            -- Step 1.2: Back-fill the captured, generated IDs into the plan
                            EXECUTE format($$
                                UPDATE temporal_merge_plan p
                                SET entity_keys = p.entity_keys || m.new_entity_keys
                                FROM temporal_merge_entity_id_map m
                                WHERE p.grouping_key = m.grouping_key;
                            $$);

                            -- Step 1.3: Insert the remaining historical slices for the new entities
                            EXECUTE format($$
                                INSERT INTO %1$s (%2$s, %4$I)
                                SELECT %3$s, p.new_valid_range::%6$I
                                FROM temporal_merge_plan p,
                                     LATERAL jsonb_populate_record(null::%1$s, p.entity_keys || p.data) as jpr_all
                                WHERE p.operation = 'INSERT'
                                  AND p.is_new_entity
                                  AND p.causal_id IS NOT NULL
                                  AND NOT EXISTS (
                                    SELECT 1 FROM (
                                        SELECT DISTINCT ON (p_inner.grouping_key) plan_op_seq
                                        FROM temporal_merge_plan p_inner
                                        WHERE p_inner.operation = 'INSERT' AND p_inner.is_new_entity
                                        ORDER BY p_inner.grouping_key, p_inner.plan_op_seq
                                    ) AS founding_ops
                                    WHERE founding_ops.plan_op_seq = p.plan_op_seq
                                  )
                                ORDER BY p.plan_op_seq;
                            $$,
                                v_target_table_ident,       /* %1$s */
                                v_all_cols_ident,           /* %2$s */
                                v_all_cols_select,          /* %3$s */
                                v_range_col,                /* %4$I */
                                NULL,                       /* %5$I (placeholder) */
                                v_range_constructor         /* %6$I */
                            );

                            DROP TABLE temporal_merge_entity_id_map;
                        END IF;

                        -- Stage 2: Handle "non-founding" inserts
                        BEGIN
                            EXECUTE format($$
                                WITH
                                source_for_insert AS (
                                    SELECT
                                        p.plan_op_seq, p.new_valid_range,
                                        p.entity_keys || p.data as full_data
                                    FROM temporal_merge_plan p
                                    WHERE p.operation = 'INSERT' AND NOT p.is_new_entity
                                ),
                                inserted_rows AS (
                                    MERGE INTO %1$s t
                                    USING source_for_insert s ON false
                                    WHEN NOT MATCHED THEN
                                        INSERT (%2$s, %3$I)
                                        VALUES (%5$s, s.new_valid_range::%9$I)
                                    RETURNING t.*, s.plan_op_seq
                                )
                                UPDATE temporal_merge_plan p
                                SET entity_keys = p.entity_keys || %8$s
                                FROM inserted_rows ir
                                WHERE p.plan_op_seq = ir.plan_op_seq;
                            $$,
                                v_target_table_ident,               /* %1$s */
                                v_all_cols_ident,                   /* %2$s */
                                v_range_col,                        /* %3$I */
                                NULL,                               /* %4$I (placeholder) */
                                v_all_cols_from_jsonb,              /* %5$s */
                                NULL,                               /* %6$s (placeholder) */
                                NULL,                               /* %7$s (placeholder) */
                                v_entity_id_update_jsonb_build,     /* %8$s */
                                v_range_constructor                 /* %9$I */
                            );
                        END;
                     END;
                ELSE
                    -- This case handles tables with only temporal and defaulted ID columns.
                     DECLARE
                        v_entity_id_update_jsonb_build TEXT;
                     BEGIN
                        -- Build the expression to construct the entity_keys feedback JSONB.
                        WITH target_cols AS (
                            SELECT pa.attname
                            FROM pg_catalog.pg_attribute pa
                            WHERE pa.attrelid = v_target_table_ident::regclass
                              AND pa.attnum > 0 AND NOT pa.attisdropped
                        ),
                        feedback_id_cols AS (
                            SELECT col FROM unnest(COALESCE(temporal_merge_execute.identity_columns, '{}')) as col
                            UNION
                            SELECT pk_col FROM unnest(v_pk_cols) as pk_col WHERE pk_col IN (SELECT attname FROM target_cols) AND pk_col NOT IN (v_valid_from_col, v_valid_until_col)
                        )
                        SELECT
                            format('jsonb_build_object(%s)', string_agg(format('%L, ir.%I', col, col), ', '))
                        INTO
                            v_entity_id_update_jsonb_build
                        FROM feedback_id_cols;

                        v_sql := format($$
                            WITH
                            source_for_insert AS (
                                SELECT
                                    p.plan_op_seq,
                                    p.new_valid_range,
                                    p.entity_keys || p.data as full_data
                                FROM temporal_merge_plan p
                                WHERE p.operation = 'INSERT'
                            ),
                            inserted_rows AS (
                                MERGE INTO %1$s t
                                USING source_for_insert s ON false
                                WHEN NOT MATCHED THEN
                                    INSERT (%8$s, %3$I)
                                    VALUES (%9$s, s.new_valid_range::%7$I)
                                RETURNING t.*, s.plan_op_seq
                            )
                            UPDATE temporal_merge_plan p
                            SET entity_keys = p.entity_keys || %2$s
                            FROM inserted_rows ir
                            WHERE p.plan_op_seq = ir.plan_op_seq;
                        $$,
                            v_target_table_ident,           /* %1$s */
                            v_entity_id_update_jsonb_build, /* %2$s */
                            v_range_col,                    /* %3$I */
                            NULL,                           /* %4$I (placeholder) */
                            NULL,                           /* %5$s (placeholder) */
                            NULL,                           /* %6$s (placeholder) */
                            v_range_constructor,            /* %7$I */
                            v_all_cols_ident,               /* %8$s */
                            v_all_cols_from_jsonb           /* %9$s */
                        );
                        EXECUTE v_sql;
                     END;
                END IF;

                IF v_log_step_timing THEN
                    v_t_done := clock_timestamp();
                    RAISE NOTICE '(%) executor_timing: delete=%.1fms, update=%.1fms, insert=%.1fms',
                        v_log_id,
                        extract(epoch from v_t_update - v_t_delete) * 1000,
                        extract(epoch from v_t_insert - v_t_update) * 1000,
                        extract(epoch from v_t_done - v_t_insert) * 1000;
                END IF;

                -- Back-fill source table with generated IDs if requested.
                IF temporal_merge_execute.update_source_with_identity THEN
                    DECLARE
                        v_source_update_set_clause TEXT;
                    BEGIN
                        SELECT string_agg(
                            format('%I = (p.entity_keys->>%L)::%s', j.key, j.key, format_type(a.atttypid, a.atttypmod)),
                            ', ' ORDER BY j.key
                        )
                        INTO v_source_update_set_clause
                        FROM (
                            SELECT key FROM jsonb_object_keys(
                                (SELECT entity_keys FROM temporal_merge_plan WHERE entity_keys IS NOT NULL and operation = 'INSERT' LIMIT 1)
                            ) as key
                            WHERE key = ANY(temporal_merge_execute.identity_columns)
                        ) j
                        JOIN pg_attribute a ON a.attname = j.key
                        WHERE a.attrelid = temporal_merge_execute.source_table AND NOT a.attisdropped AND a.attnum > 0;

                        IF v_source_update_set_clause IS NOT NULL THEN
                            -- PERFORMANCE OPTIMIZATION: The previous implementation used
                            -- `s.source_row_id = ANY(p.row_ids)` which caused O(n²) behavior
                            -- due to Nested Loop with array containment check on every row.
                            -- This rewrite unnests row_ids first and uses an equijoin, allowing
                            -- PostgreSQL to use Hash Join for O(n) performance.
                            v_sql := format($$
                                WITH plan_with_row_ids AS (
                                    SELECT p.plan_op_seq, p.entity_keys, unnest(p.row_ids) AS source_row_id
                                    FROM temporal_merge_plan p
                                    WHERE p.operation = 'INSERT' AND p.entity_keys IS NOT NULL
                                ),
                                map_row_to_entity AS (
                                    SELECT DISTINCT ON (pr.source_row_id)
                                        pr.source_row_id,
                                        pr.entity_keys
                                    FROM plan_with_row_ids pr
                                    ORDER BY pr.source_row_id, pr.plan_op_seq
                                )
                                UPDATE %1$s s
                                SET %2$s
                                FROM map_row_to_entity p
                                WHERE s.%3$I = p.source_row_id;
                            $$, temporal_merge_execute.source_table::text, v_source_update_set_clause, temporal_merge_execute.row_id_column);
                            EXECUTE v_sql;
                        END IF;
                    END;
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    -- If an error occurs, ensure constraints and search_path are reset before re-raising.
                    IF v_old_search_path IS NOT NULL THEN
                        IF v_deferrable_constraints IS NOT NULL THEN
                            v_set_constraints_sql := format('SET CONSTRAINTS %s IMMEDIATE',
                                array_to_string(v_deferrable_constraints, ', ')
                            );
                            IF v_log_execute THEN RAISE NOTICE '(%) Executing on error: %', v_log_id, v_set_constraints_sql; END IF;
                            EXECUTE v_set_constraints_sql;
                        END IF;
                        EXECUTE format('SET search_path = %s', v_old_search_path);
                    END IF;
                    RAISE;
            END;

            -- Restore constraints and search_path on successful completion.
            IF v_old_search_path IS NOT NULL THEN
                IF v_deferrable_constraints IS NOT NULL THEN
                    v_set_constraints_sql := format('SET CONSTRAINTS %s IMMEDIATE',
                        array_to_string(v_deferrable_constraints, ', ')
                    );
                    IF v_log_execute THEN RAISE NOTICE '(%) Executing: %', v_log_id, v_set_constraints_sql; END IF;
                    EXECUTE v_set_constraints_sql;
                END IF;
                EXECUTE format('SET search_path = %s', v_old_search_path);
            END IF;
        END;

        -- 4. Generate and store feedback
        v_sql := format($$
            WITH
            all_source_rows AS (
                SELECT t.%2$I AS source_row_id FROM %1$s t
            ),
            plan_unnested AS (
                SELECT unnest(p.row_ids) as source_row_id, p.plan_op_seq, p.entity_keys, p.operation, p.data, p.feedback
                FROM temporal_merge_plan p
            ),
            feedback_groups AS (
                SELECT
                    asr.source_row_id,
                    -- Aggregate all distinct operations for this source row.
                    array_agg(DISTINCT pu.operation) FILTER (WHERE pu.operation IS NOT NULL) as operations,
                    -- Aggregate all distinct entity IDs this source row touched.
                    COALESCE(jsonb_agg(DISTINCT pu.entity_keys) FILTER (WHERE pu.entity_keys IS NOT NULL), '[]'::jsonb) AS target_entity_keys,
                    -- Extract the specific error message from the plan's feedback payload if present.
                    (array_agg(pu.feedback->>'error') FILTER (WHERE pu.operation = 'ERROR' AND pu.feedback ? 'error'))[1] as error_message_from_plan
                FROM all_source_rows asr
                LEFT JOIN plan_unnested pu ON asr.source_row_id = pu.source_row_id
                GROUP BY asr.source_row_id
            )
            INSERT INTO temporal_merge_feedback
                SELECT
                    fg.source_row_id,
                    fg.target_entity_keys,
                    CASE
                        -- This CASE statement must be ordered from most to least specific to correctly classify outcomes.
                        -- This CASE statement directly translates the plan's actions into a final feedback status.
                        -- It is ordered from most to least specific to ensure correctness.
                        WHEN 'ERROR'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) THEN 'ERROR'
                        WHEN 'INSERT'::sql_saga.temporal_merge_plan_action = ANY(fg.operations)
                          OR 'UPDATE'::sql_saga.temporal_merge_plan_action = ANY(fg.operations)
                          OR 'DELETE'::sql_saga.temporal_merge_plan_action = ANY(fg.operations)
                        THEN 'APPLIED'
                        WHEN 'SKIP_NO_TARGET'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) THEN 'SKIPPED_NO_TARGET'
                        WHEN 'SKIP_FILTERED'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) THEN 'SKIPPED_FILTERED'
                        WHEN 'SKIP_ECLIPSED'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) THEN 'SKIPPED_ECLIPSED'
                        WHEN fg.operations = ARRAY['SKIP_IDENTICAL'::sql_saga.temporal_merge_plan_action] THEN 'SKIPPED_IDENTICAL'
                        -- If a source row resulted in no plan operations, it is an internal error.
                        WHEN fg.operations IS NULL THEN 'ERROR'
                        -- This is a safeguard. If the planner produces an unexpected combination of actions, we fail fast.
                        ELSE 'ERROR'
                    END::sql_saga.temporal_merge_feedback_status AS status,
                    CASE
                        WHEN 'ERROR'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) THEN COALESCE(fg.error_message_from_plan, 'Planner generated an ERROR action, indicating an internal logic error.')
                        WHEN fg.operations IS NULL THEN 'Planner failed to generate a plan for this source row.'
                        WHEN NOT (
                             'INSERT'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) OR
                             'UPDATE'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) OR
                             'DELETE'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) OR
                             'SKIP_NO_TARGET'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) OR
                             'SKIP_FILTERED'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) OR
                             'SKIP_ECLIPSED'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) OR
                             fg.operations = ARRAY['SKIP_IDENTICAL'::sql_saga.temporal_merge_plan_action]
                        )
                        THEN 'Planner produced an unhandled combination of actions: ' || fg.operations::text
                        ELSE NULL::TEXT
                    END AS error_message
                FROM feedback_groups fg
                ORDER BY fg.source_row_id;
        $$,
            temporal_merge_execute.source_table::text,       -- %1$s
            temporal_merge_execute.row_id_column      -- %2$I
        );
        EXECUTE v_sql;

    -- Conditionally output the feedback for debugging, based on a session variable.
    DECLARE
        v_feedback_rec RECORD;
    BEGIN
        IF v_log_feedback THEN
            RAISE NOTICE 'temporal_merge feedback (%) %', v_log_id, v_summary_line;
            FOR v_feedback_rec IN SELECT * FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id LOOP
                RAISE NOTICE '(%) %', v_log_id, to_json(v_feedback_rec);
            END LOOP;
        END IF;
    END;

    IF temporal_merge_execute.update_source_with_feedback THEN
        IF temporal_merge_execute.feedback_status_column IS NULL AND temporal_merge_execute.feedback_error_column IS NULL THEN
            RAISE EXCEPTION 'When update_source_with_feedback is true, at least one feedback column (feedback_status_column or feedback_error_column) must be provided.';
        END IF;

        v_feedback_set_clause := '';

        -- If a status column is provided, build its part of the SET clause
        IF temporal_merge_execute.feedback_status_column IS NOT NULL THEN
            IF temporal_merge_execute.feedback_status_key IS NULL THEN
                RAISE EXCEPTION 'When feedback_status_column is provided, feedback_status_key must also be provided.';
            END IF;

            PERFORM 1 FROM pg_attribute WHERE attrelid = temporal_merge_execute.source_table AND attname = temporal_merge_execute.feedback_status_column AND atttypid = 'jsonb'::regtype AND NOT attisdropped AND attnum > 0;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'feedback_status_column "%" does not exist in source table %s or is not of type jsonb', temporal_merge_execute.feedback_status_column, temporal_merge_execute.source_table::text;
            END IF;

            v_feedback_set_clause := v_feedback_set_clause || format(
                '%I = COALESCE(s.%I, ''{}''::jsonb) || jsonb_build_object(%L, f.status)',
                temporal_merge_execute.feedback_status_column, temporal_merge_execute.feedback_status_column, temporal_merge_execute.feedback_status_key
            );
        END IF;

        -- If an error column is provided, build its part of the SET clause
        IF temporal_merge_execute.feedback_error_column IS NOT NULL THEN
            IF temporal_merge_execute.feedback_error_key IS NULL THEN
                RAISE EXCEPTION 'When feedback_error_column is provided, feedback_error_key must also be provided.';
            END IF;

            PERFORM 1 FROM pg_attribute WHERE attrelid = temporal_merge_execute.source_table AND attname = temporal_merge_execute.feedback_error_column AND atttypid = 'jsonb'::regtype AND NOT attisdropped AND attnum > 0;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'feedback_error_column "%" does not exist in source table %s or is not of type jsonb', temporal_merge_execute.feedback_error_column, temporal_merge_execute.source_table::text;
            END IF;

            IF v_feedback_set_clause <> '' THEN
                v_feedback_set_clause := v_feedback_set_clause || ', ';
            END IF;

            v_feedback_set_clause := v_feedback_set_clause || format(
                '%I = CASE WHEN f.error_message IS NOT NULL THEN COALESCE(s.%I, ''{}''::jsonb) || jsonb_build_object(%L, f.error_message) ELSE COALESCE(s.%I, ''{}''::jsonb) - %L END',
                temporal_merge_execute.feedback_error_column, temporal_merge_execute.feedback_error_column, temporal_merge_execute.feedback_error_key, temporal_merge_execute.feedback_error_column, temporal_merge_execute.feedback_error_key
            );
        END IF;

        v_sql := format($$
            UPDATE %1$s s
            SET %2$s
            FROM pg_temp.temporal_merge_feedback f
            WHERE s.%3$I = f.source_row_id;
        $$, temporal_merge_execute.source_table::text, v_feedback_set_clause, temporal_merge_execute.row_id_column);
        EXECUTE v_sql;
    END IF;

END;
$temporal_merge_execute$;

CREATE OR REPLACE FUNCTION sql_saga.__api_add_system_versioning(
    table_oid regclass,
    history_table_name name DEFAULT NULL,
    view_name name DEFAULT NULL,
    function_as_of_name name DEFAULT NULL,
    function_between_name name DEFAULT NULL,
    function_between_symmetric_name name DEFAULT NULL,
    function_from_to_name name DEFAULT NULL,
    history_table_must_exist boolean DEFAULT false)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, pg_temp
AS
$function$
#variable_conflict use_variable
DECLARE
    table_schema name;
    table_name name;
    table_owner regrole;
    persistence "char";
    kind "char";
    era_row sql_saga.era;
    history_table_id oid;
    history_row_count bigint;
    other_schema name;
    other_family record;
BEGIN
    PERFORM sql_saga.__internal_entry_guard();
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;
    PERFORM sql_saga.__internal_require_owner(table_oid, 'add_system_versioning');

    PERFORM sql_saga.__internal_serialize(table_oid);

    SELECT n.nspname, c.relname, c.relowner, c.relpersistence, c.relkind
    INTO table_schema, table_name, table_owner, persistence, kind
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.oid = table_oid;
    /* The history table, views and functions are created in the base table's schema for its owner (Q1). */
    PERFORM sql_saga.__internal_require_create(
        (SELECT c.relnamespace FROM pg_catalog.pg_class AS c WHERE c.oid = table_oid), table_owner, 'add_system_versioning');

    IF EXISTS (SELECT 1 FROM sql_saga.system_versioning sv WHERE (sv.table_schema, sv.table_name) = (table_schema, table_name)) THEN
        RAISE EXCEPTION 'table already has SYSTEM VERSIONING';
    END IF;

    IF kind <> 'r' THEN
        IF kind = 'p' THEN
            RAISE EXCEPTION 'partitioned tables are not supported yet';
        END IF;
        RAISE EXCEPTION 'relation % is not a table', table_oid;
    END IF;

    IF persistence <> 'p' THEN
        RAISE EXCEPTION 'table "%" must be persistent', sql_saga.__internal_display_name(table_oid);
    END IF;

    /*
     * Validate an existing history table before anything is created, so a refusal leaves no trace:
     * missing (with history_table_must_exist), already another family's history, or carrying an era.
     */
    history_table_name := coalesce(history_table_name, sql_saga.__internal_make_name(ARRAY[table_name], 'history'));

    SELECT c.oid INTO history_table_id
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE (n.nspname, c.relname) = (table_schema, history_table_name);

    IF history_table_id IS NULL THEN
        IF history_table_must_exist THEN
            SELECT n.nspname INTO other_schema
            FROM pg_catalog.pg_class AS c
            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
            WHERE c.relname = history_table_name AND c.relkind IN ('r', 'p')
              AND n.nspname <> table_schema
              AND n.nspname NOT LIKE 'pg\_%' AND n.nspname <> 'information_schema'
            ORDER BY n.nspname
            LIMIT 1;
            IF other_schema IS NOT NULL THEN
                RAISE EXCEPTION 'history table "%" does not exist, and history_table_must_exist is true',
                    sql_saga.__internal_display_name(format('%I.%I', table_schema, history_table_name))
                USING ERRCODE = 'undefined_table',
                      HINT = format('Table "%s" has that name in another schema. Move it first: ALTER TABLE %I.%I SET SCHEMA %I;',
                          sql_saga.__internal_display_name(format('%I.%I', other_schema, history_table_name)),
                          other_schema, history_table_name, table_schema);
            END IF;
            RAISE EXCEPTION 'history table "%" does not exist, and history_table_must_exist is true',
                sql_saga.__internal_display_name(format('%I.%I', table_schema, history_table_name))
            USING ERRCODE = 'undefined_table';
        END IF;
    ELSE
        SELECT sv.table_schema, sv.table_name INTO other_family
        FROM sql_saga.system_versioning AS sv
        WHERE (sv.history_schema_name, sv.history_table_name) = (table_schema, history_table_name);
        IF FOUND THEN
            RAISE EXCEPTION 'table "%" is already the history table of system-versioned table "%"',
                sql_saga.__internal_display_name(history_table_id::regclass),
                sql_saga.__internal_display_name(format('%I.%I', other_family.table_schema, other_family.table_name))
            USING ERRCODE = 'object_in_use';
        END IF;
        IF EXISTS (SELECT 1 FROM sql_saga.era AS e WHERE (e.table_schema, e.table_name) = (table_schema, history_table_name)) THEN
            RAISE EXCEPTION 'history tables for SYSTEM VERSIONING cannot have eras';
        END IF;
        /* Adopting stands for ALTER TABLE <history> OWNER TO <base owner> (1.4, review round 3, Q1). */
        PERFORM sql_saga.__internal_require_owner_change(history_table_id::regclass, table_owner, 'add_system_versioning');
        /*
         * Compare with the base table as it will be once versioned: the system time era adds
         * system_valid_range when the base lacks it. Checked here, before any change.
         */
        IF EXISTS (
            WITH
            L (attname, atttypid, atttypmod, attcollation) AS (
                SELECT a.attname, a.atttypid, a.atttypmod, a.attcollation FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = table_oid AND NOT a.attisdropped
                UNION ALL
                SELECT 'system_valid_range'::name, 'pg_catalog.tstzrange'::pg_catalog.regtype::oid, -1, 0::oid
                WHERE NOT EXISTS (SELECT FROM sql_saga.era AS e WHERE (e.table_schema, e.table_name, e.era_name) = (table_schema, table_name, 'system_time'))
                  AND NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE (a.attrelid, a.attname) = (table_oid, 'system_valid_range') AND NOT a.attisdropped)),
            R (attname, atttypid, atttypmod, attcollation) AS (SELECT a.attname, a.atttypid, a.atttypmod, a.attcollation FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = history_table_id AND NOT a.attisdropped)
            SELECT FROM L NATURAL FULL JOIN R WHERE L.attname IS NULL OR R.attname IS NULL)
        THEN
            RAISE EXCEPTION 'base table "%" and history table "%" are not compatible', sql_saga.__internal_display_name(table_oid), sql_saga.__internal_display_name(history_table_id::regclass);
        END IF;
    END IF;

    SELECT e.*
    INTO era_row
    FROM sql_saga.era AS e
    WHERE (e.table_schema, e.table_name, e.era_name) = (table_schema, table_name, 'system_time');

    IF NOT FOUND THEN
        PERFORM sql_saga.__internal_add_system_time_era(table_oid);
        -- Re-fetch era row
        SELECT e.* INTO era_row FROM sql_saga.era e WHERE (e.table_schema, e.table_name, e.era_name) = (table_schema, table_name, 'system_time');
    END IF;

    view_name := coalesce(view_name, sql_saga.__internal_make_name(ARRAY[table_name], 'with_history'));
    function_as_of_name := coalesce(function_as_of_name, sql_saga.__internal_make_name(ARRAY[table_name], '_as_of'));
    function_between_name := coalesce(function_between_name, sql_saga.__internal_make_name(ARRAY[table_name], '_between'));
    function_between_symmetric_name := coalesce(function_between_symmetric_name, sql_saga.__internal_make_name(ARRAY[table_name], '_between_symmetric'));
    function_from_to_name := coalesce(function_from_to_name, sql_saga.__internal_make_name(ARRAY[table_name], '_from_to'));

    IF history_table_id IS NOT NULL THEN
        PERFORM sql_saga.__internal_execute_as_caller(format('ALTER TABLE %s OWNER TO %I', history_table_id::regclass /* %s */, table_owner /* %I */));
        SELECT sql_saga.__internal_execute_as_caller(format('SELECT count(*) FROM %s', history_table_id::regclass /* %s */))::bigint INTO history_row_count;
        RAISE NOTICE 'sql_saga: Adopted existing table "%" (% rows) as the history table of system-versioned table "%"',
            sql_saga.__internal_display_name(history_table_id::regclass), history_row_count,
            sql_saga.__internal_display_name(table_oid);
    ELSE
        PERFORM sql_saga.__internal_execute_as_caller(format('CREATE TABLE %1$I.%2$I (LIKE %1$I.%3$I)', table_schema /* %1$I */, history_table_name /* %2$I */, table_name /* %3$I */));
        history_table_id := format('%I.%I', table_schema /* %I */, history_table_name /* %I */)::regclass;
        PERFORM sql_saga.__internal_execute_as_caller(format('ALTER TABLE %1$I.%2$I OWNER TO %3$I', table_schema /* %1$I */, history_table_name /* %2$I */, table_owner /* %3$I */));
        RAISE NOTICE 'sql_saga: Created history table "%" for system-versioned table "%"',
            sql_saga.__internal_notice_name(table_schema, history_table_name), sql_saga.__internal_notice_name(table_schema, table_name);
        RAISE DEBUG 'history table "%" created for "%", be sure to index it properly', history_table_id::regclass, table_oid;
    END IF;

    /*
     * Access-time authority (doc/design/history-access.md): the history objects are granted to PUBLIC and
     * hold no copy of the base table's ACL. Callers are checked against the base when they read:
     *   - B_history_gated (owner-rights) raises 42501 unless the caller may SELECT B, and serves
     *     callers that B's row-level security does not apply to;
     *   - B_history is read as the caller when B's RLS applies, filtered by the copies of B's
     *     policies (sql_saga.__internal_sync_history_policies), after checking they are in sync.
     */
    PERFORM sql_saga.__internal_execute_as_caller(format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', history_table_id::regclass));
    PERFORM sql_saga.__internal_execute_as_caller(format('GRANT SELECT ON TABLE %s TO PUBLIC', history_table_id::regclass));

    -- Registered first: the generator reads the family's names from the catalog.
    INSERT INTO sql_saga.system_versioning (table_schema, table_name, era_name, history_schema_name, history_table_name, view_schema_name, view_table_name, func_as_of, func_between, func_between_symmetric, func_from_to)
    VALUES (
        table_schema, table_name, 'system_time',
        table_schema, history_table_name,
        table_schema, view_name,
        format('%I.%I(timestamp with time zone)', table_schema /* %I */, function_as_of_name /* %I */),
        format('%I.%I(timestamp with time zone,timestamp with time zone)', table_schema /* %I */, function_between_name /* %I */),
        format('%I.%I(timestamp with time zone,timestamp with time zone)', table_schema /* %I */, function_between_symmetric_name /* %I */),
        format('%I.%I(timestamp with time zone,timestamp with time zone)', table_schema /* %I */, function_from_to_name /* %I */)
    );

    -- B_history_gated, B_with_history and the as-of functions: the same generator as
    -- rebuild_system_versioning_views, so a rebuild of an unchanged family reproduces them exactly.
    PERFORM sql_saga.__internal_reconcile_system_versioning_family(table_oid, '{}');
END;
$function$;
CREATE OR REPLACE FUNCTION sql_saga.__internal_drop_protection()
 RETURNS event_trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, pg_temp
AS
$function$
#variable_conflict use_variable
DECLARE
    r record;
    table_oid regclass;
    era_name name;
    v_teardown boolean := sql_saga.__internal_is_clean_teardown();
    v_dropped_objects jsonb[];
    v_sanctioned_bases jsonb[];
    v_refusal_objects jsonb[];
    v_candidate record;
    v_object_types text[];
    v_teardown_family boolean := false;
BEGIN
    PERFORM sql_saga.__internal_entry_guard();
    /* Materialize this invocation once. Nested DDL gets its own function frame. */
    SELECT coalesce(
        array_agg(to_jsonb(dobj) ORDER BY dobj.ordinality),
        ARRAY[]::jsonb[])
    INTO v_dropped_objects
    FROM pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj;

    SELECT pg_catalog.array_agg(DISTINCT u.data->>'object_type' ORDER BY u.data->>'object_type')
    INTO v_object_types
    FROM unnest(v_dropped_objects) AS u(data);

    /*
     * During a recognized clean teardown, any non-policy object addressed on a system-versioned base or
     * history table must reach the teardown cleanup below (it drops the installation-token history
     * policies). Neither early exit may skip it. Outside a teardown this costs nothing.
     */
    IF v_teardown THEN
        SELECT EXISTS (
            SELECT 1
            FROM unnest(v_dropped_objects) AS u(data)
            CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(object_type text, address_names text[])
            JOIN sql_saga.system_versioning AS sv
              ON (d.address_names[1], d.address_names[2]) IN (
                  (sv.table_schema, sv.table_name), (sv.history_schema_name, sv.history_table_name))
            WHERE d.object_type <> 'policy')
        INTO v_teardown_family;
    END IF;

    IF v_teardown_family THEN
        NULL; /* Admitted: see above. */
    ELSIF v_object_types = ARRAY['function'] AND NOT EXISTS (
        SELECT 1
        FROM unnest(v_dropped_objects) AS u(data)
        JOIN sql_saga.system_versioning AS sv
          ON u.data->>'object_identity' = ANY (
              ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to])) THEN
        RETURN;
    ELSIF v_object_types = ARRAY['view'] AND NOT EXISTS (
        SELECT 1
        FROM unnest(v_dropped_objects) AS u(data)
        CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(address_names text[])
        WHERE EXISTS (
                  SELECT FROM sql_saga.updatable_view AS v
                  WHERE (v.view_schema, v.view_name) = (d.address_names[1], d.address_names[2]))
           OR EXISTS (
                  SELECT FROM sql_saga.system_versioning AS sv
                  WHERE (d.address_names[1], d.address_names[2]) IN (
                      (sv.view_schema_name, sv.view_table_name),
                      (sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name))))) THEN
        RETURN;
    ELSIF v_object_types = ARRAY['table'] AND NOT EXISTS (
        SELECT 1
        FROM unnest(v_dropped_objects) AS u(data)
        CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(address_names text[])
        WHERE EXISTS (
                  SELECT FROM sql_saga.era AS e
                  WHERE (e.table_schema, e.table_name) = (d.address_names[1], d.address_names[2]))
           OR EXISTS (
                  SELECT FROM sql_saga.system_versioning AS sv
                  WHERE (sv.history_schema_name, sv.history_table_name) =
                        (d.address_names[1], d.address_names[2]))) THEN
        RETURN;
    ELSIF v_object_types = ARRAY['trigger'] AND NOT EXISTS (
        SELECT 1
        FROM unnest(v_dropped_objects) AS u(data)
        CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(address_names text[])
        WHERE EXISTS (
                  SELECT FROM sql_saga.system_time_era AS ste
                  WHERE (ste.table_schema, ste.table_name) = (d.address_names[1], d.address_names[2])
                    AND d.address_names[3] IN (
                        ste.generated_always_trigger, ste.write_history_trigger, ste.truncate_trigger))
           OR EXISTS (
                  SELECT FROM sql_saga.updatable_view AS v
                  WHERE (v.view_schema, v.view_name) = (d.address_names[1], d.address_names[2])
                    AND d.address_names[3] IN (v.trigger_name, sql_saga.__internal_view_apply_trigger_name(v.view_type, v.trigger_name)))
           OR EXISTS (
                  SELECT FROM sql_saga.foreign_keys AS fk
                  JOIN sql_saga.unique_keys AS uk USING (unique_key_name)
                  WHERE (uk.table_schema, uk.table_name) = (d.address_names[1], d.address_names[2])
                    AND d.address_names[3] IN (fk.uk_update_trigger, fk.uk_delete_trigger, fk.uk_truncate_trigger))
           OR EXISTS (
                  SELECT FROM sql_saga.foreign_keys AS fk
                  WHERE fk.type = 'regular_to_temporal'
                    AND (fk.table_schema, fk.table_name, fk.fk_check_constraint) =
                        (d.address_names[1], d.address_names[2], d.address_names[3]))) THEN
        RETURN;
    END IF;

    /* Ordinary and metadata-first DDL must not pay for protected-object analysis. */
    IF NOT v_teardown_family AND NOT EXISTS (
        WITH dropped AS MATERIALIZED (
            SELECT d.*
            FROM unnest(v_dropped_objects) AS u(data)
            CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(
                object_type text, object_identity text, address_names text[], objid oid)
        )
        SELECT 1
        FROM dropped AS d
        JOIN sql_saga.era AS e
          ON (d.object_type = 'table'
              AND (d.address_names[1], d.address_names[2]) = (e.table_schema, e.table_name))
          OR (d.object_type = 'table column'
              AND (d.address_names[1], d.address_names[2]) = (e.table_schema, e.table_name)
              AND d.address_names[3] IN (e.valid_from_column_name, e.valid_until_column_name))
          OR d.objid = e.range_type::oid
        UNION ALL
        SELECT 1
        FROM dropped AS d
        JOIN sql_saga.system_time_era AS ste
          ON (d.object_type = 'table constraint'
              AND (d.address_names[1], d.address_names[2], d.address_names[3]) =
                  (ste.table_schema, ste.table_name, ste.infinity_check_constraint))
          OR (d.object_type = 'trigger'
              AND (d.address_names[1], d.address_names[2]) = (ste.table_schema, ste.table_name)
              AND d.address_names[3] IN (ste.generated_always_trigger, ste.write_history_trigger, ste.truncate_trigger))
          OR (d.object_type = 'table column'
              AND (d.address_names[1], d.address_names[2]) = (ste.table_schema, ste.table_name)
              AND d.address_names[3] = ANY (ste.excluded_column_names))
        UNION ALL
        SELECT 1
        FROM dropped AS d
        JOIN sql_saga.updatable_view AS v
          ON (d.object_type = 'view'
              AND (d.address_names[1], d.address_names[2]) = (v.view_schema, v.view_name))
          OR (d.object_type = 'trigger'
              AND (d.address_names[1], d.address_names[2]) = (v.view_schema, v.view_name)
              AND d.address_names[3] IN (v.trigger_name, sql_saga.__internal_view_apply_trigger_name(v.view_type, v.trigger_name)))
          OR (d.object_type IN ('table column', 'table constraint', 'index')
              AND (d.address_names[1], d.address_names[2]) = (v.table_schema, v.table_name))
        UNION ALL
        SELECT 1
        FROM dropped AS d
        JOIN sql_saga.unique_keys AS uk
          ON (d.object_type = 'table constraint'
              AND (d.address_names[1], d.address_names[2]) = (uk.table_schema, uk.table_name)
              AND (d.address_names[3] IN (uk.unique_constraint, uk.exclude_constraint, uk.check_constraint)
                   OR d.address_names[3] = ANY (coalesce(uk.pk_consistency_constraint_names, '{}'))))
          OR (d.object_type = 'index'
              AND (d.address_names[1], d.address_names[2]) = (uk.table_schema, uk.unique_constraint))
        UNION ALL
        SELECT 1
        FROM dropped AS d
        JOIN sql_saga.foreign_keys AS fk
          ON d.object_type = 'trigger'
         AND d.address_names[3] IN (fk.uk_update_trigger, fk.uk_delete_trigger, fk.uk_truncate_trigger)
        JOIN sql_saga.unique_keys AS uk
          ON uk.unique_key_name = fk.unique_key_name
         AND (d.address_names[1], d.address_names[2]) = (uk.table_schema, uk.table_name)
        UNION ALL
        SELECT 1
        FROM dropped AS d
        JOIN sql_saga.foreign_keys AS fk
          ON d.object_type = 'trigger'
         AND fk.type = 'regular_to_temporal'
         AND (d.address_names[1], d.address_names[2], d.address_names[3]) =
             (fk.table_schema, fk.table_name, fk.fk_check_constraint)
        UNION ALL
        SELECT 1
        FROM dropped AS d
        JOIN sql_saga.system_versioning AS sv
          ON (d.object_type = 'table'
              AND (d.address_names[1], d.address_names[2]) = (sv.history_schema_name, sv.history_table_name))
          OR (d.object_type = 'view'
              AND (d.address_names[1], d.address_names[2]) IN (
                  (sv.view_schema_name, sv.view_table_name),
                  (sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name))))
          OR (d.object_type = 'function'
              AND d.object_identity = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]))
        UNION ALL
        SELECT 1
        FROM dropped AS d
        CROSS JOIN LATERAL sql_saga.__internal_history_family(
            to_regclass(format('%I.%I', d.address_names[1], d.address_names[2]))) AS f
        WHERE d.object_type = 'policy'
    ) THEN
        RETURN;
    END IF;

    /*
     * Choose exactly one refusal from this statement's payload. Priority preserves the historical
     * source-class order; ordinality preserves PostgreSQL's dependency order within a class.
     */
    IF NOT v_teardown THEN
        /*
         * Remove complete families before constructing refusal candidates.  The ordinalities
         * identify rows in this invocation's materialized payload, so no dropped-object OID is
         * retained beyond the statement.  Objects not belonging to a dropped base family remain
         * available for mixed sanctioned/unsanctioned statements.
         */
        IF EXISTS (
            SELECT 1
            FROM unnest(v_dropped_objects) AS u(data)
            WHERE u.data->>'object_type' = 'table'
        ) THEN
            SELECT coalesce(
                array_agg(DISTINCT jsonb_build_array(e.table_schema, e.table_name)),
                ARRAY[]::jsonb[])
            INTO v_sanctioned_bases
            FROM unnest(v_dropped_objects) AS u(data)
            CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(
                object_type text, address_names text[])
            JOIN sql_saga.era AS e
              ON d.object_type = 'table'
             AND (d.address_names[1], d.address_names[2]) = (e.table_schema, e.table_name);
        ELSE
            v_sanctioned_bases := ARRAY[]::jsonb[];
        END IF;

        IF cardinality(v_sanctioned_bases) = 0 THEN
            v_refusal_objects := v_dropped_objects;
        ELSIF EXISTS (
            SELECT 1
            FROM unnest(v_dropped_objects) AS u(data)
            WHERE u.data->>'object_type' = 'schema'
        ) AND NOT EXISTS (
            WITH dropped AS MATERIALIZED (
                SELECT d.*
                FROM unnest(v_dropped_objects) AS u(data)
                CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(
                    object_type text, object_identity text, address_names text[])
            )
            SELECT 1
            FROM dropped AS d
            JOIN sql_saga.updatable_view AS v
              ON ((d.object_type = 'view'
                   AND (d.address_names[1], d.address_names[2]) = (v.view_schema, v.view_name))
                  OR (d.object_type = 'trigger'
                      AND (d.address_names[1], d.address_names[2]) = (v.view_schema, v.view_name)
                      AND d.address_names[3] IN (v.trigger_name, sql_saga.__internal_view_apply_trigger_name(v.view_type, v.trigger_name))))
            WHERE NOT jsonb_build_array(v.table_schema, v.table_name) = ANY (v_sanctioned_bases)
            UNION ALL
            SELECT 1
            FROM dropped AS d
            JOIN sql_saga.system_versioning AS sv
              ON (d.object_type = 'table'
                  AND (d.address_names[1], d.address_names[2]) =
                      (sv.history_schema_name, sv.history_table_name))
              OR (d.object_type = 'view'
                  AND (d.address_names[1], d.address_names[2]) IN (
                      (sv.view_schema_name, sv.view_table_name),
                      (sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name))))
              OR (d.object_type = 'function'
                  AND d.object_identity = ANY (ARRAY[
                      sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]))
            WHERE NOT jsonb_build_array(sv.table_schema, sv.table_name) = ANY (v_sanctioned_bases)
        ) THEN
            v_refusal_objects := ARRAY[]::jsonb[];
        ELSE
        WITH dropped AS MATERIALIZED (
            SELECT u.ordinality AS dropped_ordinality, u.data, d.*
            FROM unnest(v_dropped_objects) WITH ORDINALITY AS u(data, ordinality)
            CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(
                object_type text, object_identity text, address_names text[], objid oid)
        ), sanctioned_bases AS MATERIALIZED (
            SELECT b.data->>0 AS table_schema, b.data->>1 AS table_name
            FROM unnest(v_sanctioned_bases) AS b(data)
        )
        SELECT coalesce(
            array_agg(d.data ORDER BY d.dropped_ordinality) FILTER (WHERE NOT EXISTS (
                SELECT 1
                FROM sanctioned_bases AS b
                WHERE (d.address_names[1], d.address_names[2]) = (b.table_schema, b.table_name)
                   OR EXISTS (
                        SELECT 1
                        FROM sql_saga.era AS e
                        WHERE (e.table_schema, e.table_name) = (b.table_schema, b.table_name)
                          AND d.objid = e.range_type::oid)
                   OR (d.object_type = 'index' AND EXISTS (
                        SELECT 1
                        FROM sql_saga.unique_keys AS uk
                        WHERE (uk.table_schema, uk.table_name) = (b.table_schema, b.table_name)
                          AND (d.address_names[1], d.address_names[2]) =
                              (uk.table_schema, uk.unique_constraint)))
                   OR (d.object_type = 'trigger' AND EXISTS (
                        /* A regular FK's child-side trigger depends on (FROM) the dropped referenced table. */
                        SELECT 1
                        FROM sql_saga.foreign_keys AS fk
                        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
                        WHERE (uk.table_schema, uk.table_name) = (b.table_schema, b.table_name)
                          AND fk.type = 'regular_to_temporal'
                          AND (d.address_names[1], d.address_names[2], d.address_names[3]) =
                              (fk.table_schema, fk.table_name, fk.fk_check_constraint)))
                   OR EXISTS (
                        SELECT 1
                        FROM sql_saga.updatable_view AS v
                        WHERE (v.table_schema, v.table_name) = (b.table_schema, b.table_name)
                          AND (d.address_names[1], d.address_names[2]) = (v.view_schema, v.view_name))
                   OR EXISTS (
                        SELECT 1
                        FROM sql_saga.system_versioning AS sv
                        WHERE (sv.table_schema, sv.table_name) = (b.table_schema, b.table_name)
                          AND ((d.address_names[1], d.address_names[2]) IN (
                                  (sv.history_schema_name, sv.history_table_name),
                                  (sv.view_schema_name, sv.view_table_name),
                                  (sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name)))
                               OR d.object_identity = ANY (ARRAY[
                                  sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to])))
            )),
            ARRAY[]::jsonb[])
        INTO v_refusal_objects
        FROM dropped AS d;
        END IF;

        IF cardinality(v_refusal_objects) > 0 THEN
        IF NOT EXISTS (
            SELECT 1
            FROM unnest(v_refusal_objects) AS u(data)
            WHERE u.data->>'object_type' <> 'trigger'
        ) THEN
        WITH dropped AS (
            SELECT u.ordinality AS dropped_ordinality, d.*
            FROM unnest(v_refusal_objects) WITH ORDINALITY AS u(data, ordinality)
            CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(
                object_type text, address_names text[])
        ), candidates AS (
            SELECT CASE d.address_names[3]
                       WHEN ste.generated_always_trigger THEN 40
                       WHEN ste.write_history_trigger THEN 50 ELSE 60 END AS priority,
                   d.dropped_ordinality,
                   format('cannot drop trigger "%s" on table "%s" because it is used in SYSTEM_TIME period',
                          d.address_names[3],
                          sql_saga.__internal_display_name(ste.table_schema, ste.table_name)) AS message
            FROM dropped AS d JOIN sql_saga.system_time_era AS ste
             ON (d.address_names[1], d.address_names[2]) = (ste.table_schema, ste.table_name)
             AND d.address_names[3] IN (ste.generated_always_trigger, ste.write_history_trigger, ste.truncate_trigger)
            UNION ALL
            SELECT 90, d.dropped_ordinality,
                   format('cannot drop trigger "%s" on view "%s" because it is part of an updatable view for era "%s" on table "%s"',
                          d.address_names[3],
                          sql_saga.__internal_display_name(d.address_names[1]::name, d.address_names[2]::name),
                          v.era_name,
                          sql_saga.__internal_display_name(v.table_schema, v.table_name))
            FROM dropped AS d JOIN sql_saga.updatable_view AS v
             ON (d.address_names[1], d.address_names[2]) = (v.view_schema, v.view_name)
             AND d.address_names[3] IN (v.trigger_name, sql_saga.__internal_view_apply_trigger_name(v.view_type, v.trigger_name))
            UNION ALL
            SELECT CASE WHEN d.address_names[3] = fk.uk_update_trigger THEN 120 ELSE 130 END,
                   d.dropped_ordinality,
                   format('cannot drop trigger "%s" on table "%s" because it is used in era foreign key "%s"',
                          d.address_names[3],
                          sql_saga.__internal_display_name(uk.table_schema, uk.table_name),
                          fk.foreign_key_name)
            FROM dropped AS d
            JOIN sql_saga.foreign_keys AS fk ON d.address_names[3] IN (fk.uk_update_trigger, fk.uk_delete_trigger, fk.uk_truncate_trigger)
            JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
            WHERE (d.address_names[1], d.address_names[2]) = (uk.table_schema, uk.table_name)
            UNION ALL
            SELECT 125, d.dropped_ordinality,
                   format('cannot drop trigger "%s" on table "%s" because it is used in era foreign key "%s"',
                          d.address_names[3],
                          sql_saga.__internal_display_name(fk.table_schema, fk.table_name),
                          fk.foreign_key_name)
            FROM dropped AS d
            JOIN sql_saga.foreign_keys AS fk
              ON fk.type = 'regular_to_temporal'
             AND (d.address_names[1], d.address_names[2], d.address_names[3]) =
                 (fk.table_schema, fk.table_name, fk.fk_check_constraint)
        )
        SELECT c.message INTO v_candidate
        FROM candidates AS c
        ORDER BY c.priority, c.dropped_ordinality
        LIMIT 1;
        ELSE
        WITH dropped AS (
            SELECT u.ordinality AS dropped_ordinality, d.*
            FROM unnest(v_refusal_objects) WITH ORDINALITY AS u(data, ordinality)
            CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(
                object_type text, object_identity text, address_names text[], objid oid)
        ), candidates AS (
            SELECT 10 AS priority, d.dropped_ordinality,
                   format('cannot drop column "%s" because it is part of the period "%s"', d.object_identity, e.era_name) AS message
            FROM dropped AS d JOIN sql_saga.era AS e
             ON d.object_type = 'table column'
             AND (d.address_names[1], d.address_names[2]) = (e.table_schema, e.table_name)
             AND d.address_names[3] IN (e.valid_from_column_name, e.valid_until_column_name)
            UNION ALL
            SELECT 20, d.dropped_ordinality,
                   format('cannot drop rangetype "%s" because it is used in period "%s" on table "%s"',
                          d.object_identity, e.era_name,
                          sql_saga.__internal_display_name(e.table_schema, e.table_name))
            FROM dropped AS d JOIN sql_saga.era AS e ON d.objid = e.range_type::oid
            UNION ALL
            SELECT 30, d.dropped_ordinality,
                   format('cannot drop constraint "%s" on table "%s" because it is used in SYSTEM_TIME period',
                          ste.infinity_check_constraint,
                          sql_saga.__internal_display_name(ste.table_schema, ste.table_name))
            FROM dropped AS d JOIN sql_saga.system_time_era AS ste
             ON d.object_type = 'table constraint'
             AND (d.address_names[1], d.address_names[2], d.address_names[3]) = (ste.table_schema, ste.table_name, ste.infinity_check_constraint)
            UNION ALL
            SELECT CASE d.address_names[3]
                       WHEN ste.generated_always_trigger THEN 40
                       WHEN ste.write_history_trigger THEN 50 ELSE 60 END,
                   d.dropped_ordinality,
                   format('cannot drop trigger "%s" on table "%s" because it is used in SYSTEM_TIME period',
                          d.address_names[3],
                          sql_saga.__internal_display_name(ste.table_schema, ste.table_name))
            FROM dropped AS d JOIN sql_saga.system_time_era AS ste
             ON d.object_type = 'trigger'
             AND (d.address_names[1], d.address_names[2]) = (ste.table_schema, ste.table_name)
             AND d.address_names[3] IN (ste.generated_always_trigger, ste.write_history_trigger, ste.truncate_trigger)
            UNION ALL
            SELECT 70, d.dropped_ordinality,
                   format('cannot drop or rename column "%s" on table "%s" because it is excluded from system_time era',
                          d.address_names[3],
                          sql_saga.__internal_display_name(ste.table_schema, ste.table_name))
            FROM dropped AS d JOIN sql_saga.system_time_era AS ste
             ON d.object_type = 'table column'
             AND (d.address_names[1], d.address_names[2]) = (ste.table_schema, ste.table_name)
             AND d.address_names[3] = ANY (ste.excluded_column_names)
            UNION ALL
            SELECT 80, d.dropped_ordinality,
                   format('cannot drop view "%s", call "sql_saga.drop_%s_view()" instead',
                          sql_saga.__internal_display_name(d.object_identity), v.view_type)
            FROM dropped AS d JOIN sql_saga.updatable_view AS v
             ON d.object_type = 'view'
             AND (d.address_names[1], d.address_names[2]) = (v.view_schema, v.view_name)
            UNION ALL
            SELECT 90, d.dropped_ordinality,
                   format('cannot drop trigger "%s" on view "%s" because it is part of an updatable view for era "%s" on table "%s"',
                          d.address_names[3],
                          sql_saga.__internal_display_name(d.address_names[1]::name, d.address_names[2]::name),
                          v.era_name,
                          sql_saga.__internal_display_name(v.table_schema, v.table_name))
            FROM dropped AS d JOIN sql_saga.updatable_view AS v
             ON d.object_type = 'trigger'
             AND (d.address_names[1], d.address_names[2]) = (v.view_schema, v.view_name)
             AND d.address_names[3] IN (v.trigger_name, sql_saga.__internal_view_apply_trigger_name(v.view_type, v.trigger_name))
            UNION ALL
            SELECT 100, d.dropped_ordinality,
                   format('table "%s" must have a primary key or a single-column temporal unique key to support its %s view "%s" for era "%s"; drop that view first with sql_saga.drop_%s_view()',
                          sql_saga.__internal_display_name(v.table_schema, v.table_name), v.view_type,
                          sql_saga.__internal_display_name(v.view_schema, v.view_name), v.era_name, v.view_type)
            FROM dropped AS d JOIN sql_saga.updatable_view AS v
             ON d.object_type IN ('table column', 'table constraint', 'index')
             AND (d.address_names[1], d.address_names[2]) = (v.table_schema, v.table_name)
            WHERE NOT EXISTS (
                      SELECT 1 FROM pg_catalog.pg_constraint AS c
                      WHERE (c.conrelid, c.contype) =
                            (to_regclass(format('%I.%I', v.table_schema, v.table_name)), 'p'))
              AND NOT EXISTS (
                      SELECT 1 FROM sql_saga.unique_keys AS identifier_uk
                      WHERE (identifier_uk.table_schema, identifier_uk.table_name, identifier_uk.era_name) =
                            (v.table_schema, v.table_name, v.era_name)
                        AND array_length(identifier_uk.column_names, 1) = 1
                        AND NOT EXISTS (
                                SELECT 1
                                FROM dropped AS identifier_drop
                                WHERE (identifier_drop.object_type = 'table constraint'
                                       AND (identifier_drop.address_names[1], identifier_drop.address_names[2], identifier_drop.address_names[3]) =
                                           (identifier_uk.table_schema, identifier_uk.table_name, identifier_uk.unique_constraint))
                                   OR (identifier_drop.object_type = 'index'
                                       AND (identifier_drop.address_names[1], identifier_drop.address_names[2]) =
                                           (identifier_uk.table_schema, identifier_uk.unique_constraint))))
            UNION ALL
            SELECT 110, d.dropped_ordinality,
                   format('cannot drop constraint "%s" on table "%s" because it is used in era unique key "%s"',
                          d.address_names[3],
                          sql_saga.__internal_display_name(uk.table_schema, uk.table_name),
                          uk.unique_key_name)
            FROM dropped AS d JOIN sql_saga.unique_keys AS uk
             ON d.object_type = 'table constraint'
             AND (d.address_names[1], d.address_names[2]) = (uk.table_schema, uk.table_name)
             AND (d.address_names[3] IN (uk.unique_constraint, uk.exclude_constraint, uk.check_constraint)
                  OR d.address_names[3] = ANY (coalesce(uk.pk_consistency_constraint_names, '{}')))
            UNION ALL
            SELECT 110, d.dropped_ordinality,
                   format('cannot drop index "%s" on table "%s" because it is used in era unique key "%s"',
                          uk.unique_constraint,
                          sql_saga.__internal_display_name(uk.table_schema, uk.table_name),
                          uk.unique_key_name)
            FROM dropped AS d JOIN sql_saga.unique_keys AS uk
             ON d.object_type = 'index'
             AND (d.address_names[1], d.address_names[2]) =
                 (uk.table_schema, uk.unique_constraint)
            UNION ALL
            SELECT CASE WHEN d.address_names[3] = fk.uk_update_trigger THEN 120 ELSE 130 END,
                   d.dropped_ordinality,
                   format('cannot drop trigger "%s" on table "%s" because it is used in era foreign key "%s"',
                          d.address_names[3],
                          sql_saga.__internal_display_name(uk.table_schema, uk.table_name),
                          fk.foreign_key_name)
            FROM dropped AS d
            JOIN sql_saga.foreign_keys AS fk ON d.address_names[3] IN (fk.uk_update_trigger, fk.uk_delete_trigger, fk.uk_truncate_trigger)
            JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
            WHERE d.object_type = 'trigger'
              AND (d.address_names[1], d.address_names[2]) = (uk.table_schema, uk.table_name)
            UNION ALL
            SELECT 125, d.dropped_ordinality,
                   format('cannot drop trigger "%s" on table "%s" because it is used in era foreign key "%s"',
                          d.address_names[3],
                          sql_saga.__internal_display_name(fk.table_schema, fk.table_name),
                          fk.foreign_key_name)
            FROM dropped AS d
            JOIN sql_saga.foreign_keys AS fk
              ON fk.type = 'regular_to_temporal'
             AND (d.address_names[1], d.address_names[2], d.address_names[3]) =
                 (fk.table_schema, fk.table_name, fk.fk_check_constraint)
            WHERE d.object_type = 'trigger'
              AND true
            UNION ALL
            SELECT 140, d.dropped_ordinality,
                   format('cannot drop table "%s" because it is used in SYSTEM VERSIONING for table "%s"',
                          sql_saga.__internal_display_name(d.object_identity),
                          sql_saga.__internal_display_name(sv.table_schema, sv.table_name))
            FROM dropped AS d JOIN sql_saga.system_versioning AS sv
              ON d.object_type = 'table'
             AND (d.address_names[1], d.address_names[2]) = (sv.history_schema_name, sv.history_table_name)
            UNION ALL
            SELECT 150, d.dropped_ordinality,
                   format('cannot drop view "%s" because it is used in SYSTEM VERSIONING for table "%s"',
                          sql_saga.__internal_display_name(d.object_identity),
                          sql_saga.__internal_display_name(sv.table_schema, sv.table_name))
            FROM dropped AS d JOIN sql_saga.system_versioning AS sv
              ON d.object_type = 'view'
             AND (d.address_names[1], d.address_names[2]) IN (
                    (sv.view_schema_name, sv.view_table_name),
                    (sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name)))
             AND sql_saga.__internal_rebuilding_system_versioning_views(
                     to_regclass(format('%I.%I', sv.table_schema, sv.table_name))::oid) IS NOT TRUE
            UNION ALL
            SELECT 160, d.dropped_ordinality,
                   format('cannot drop function "%s" because it is used in SYSTEM VERSIONING for table "%s"',
                          sql_saga.__internal_display_function_name(d.object_identity),
                          sql_saga.__internal_display_name(sv.table_schema, sv.table_name))
            FROM dropped AS d JOIN sql_saga.system_versioning AS sv
              ON d.object_type = 'function'
             AND d.object_identity = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to])
             AND sql_saga.__internal_rebuilding_system_versioning_views(
                     to_regclass(format('%I.%I', sv.table_schema, sv.table_name))::oid) IS NOT TRUE
            UNION ALL
            SELECT 170, d.dropped_ordinality,
                   format('cannot drop or alter policy "%s" on "%s" because it is managed by sql_saga for table "%s"',
                          d.address_names[3],
                          sql_saga.__internal_display_name(f.history_oid::regclass),
                          sql_saga.__internal_display_name(f.base_oid::regclass))
            FROM dropped AS d
            CROSS JOIN LATERAL sql_saga.__internal_history_family(
                to_regclass(format('%I.%I', d.address_names[1], d.address_names[2]))) AS f
            WHERE d.object_type = 'policy'
              AND to_regclass(format('%I.%I', d.address_names[1], d.address_names[2]))::oid = f.history_oid
              AND sql_saga.__internal_is_managed_history_policy(d.address_names[3]::name)
              AND NOT sql_saga.__internal_history_policy_sync_active(f.base_oid)
        )
        SELECT c.message INTO v_candidate
        FROM candidates AS c
        ORDER BY c.priority, c.dropped_ordinality
        LIMIT 1;
        END IF;

        IF FOUND THEN
            RAISE EXCEPTION '%', v_candidate.message;
        END IF;
        END IF;
    END IF;

    ---
    --- periods
    ---

    /*
     * Teardown by pg_restore --clean (sql_saga.__internal_is_clean_teardown, doc/design/restore.md).
     * The checks below that complain about missing objects are skipped, and so is cleanup of objects
     * pg_restore drops itself. One thing pg_restore cannot do: drop the managed history policies,
     * whose names carry this installation's token while the dump carries another. They depend on the
     * base table, so they are dropped here, as soon as any object of their family goes (DROP TABLE of
     * the base would fail otherwise when the base is dropped before its history). Not on pg_restore's
     * own DROP POLICY statements: those come first, and a later DROP POLICY (without IF EXISTS) of a
     * dumped name already gone would fail. Restore plumbing the caller asked for: reported at DEBUG.
     * The nested DROP POLICY statements below drop only policies, which the family lookup ignores, so
     * they never re-enter this cleanup.
     */
    IF v_teardown THEN
        DECLARE
            family_row record;
            policy_row record;
        BEGIN
            FOR family_row IN
                SELECT DISTINCT to_regclass(format('%I.%I', sv.history_schema_name /* %I */, sv.history_table_name /* %I */)) AS history_oid
                FROM unnest(v_dropped_objects) AS u(data)
                CROSS JOIN LATERAL jsonb_to_record(u.data) AS dobj(
                    object_type text, address_names text[])
                JOIN sql_saga.system_versioning AS sv
                  ON (dobj.address_names[1], dobj.address_names[2]) IN ((sv.table_schema, sv.table_name), (sv.history_schema_name, sv.history_table_name))
                WHERE dobj.object_type <> 'policy'
            LOOP
                CONTINUE WHEN family_row.history_oid IS NULL;
                FOR policy_row IN
                    SELECT p.polname
                    FROM pg_catalog.pg_policy AS p
                    WHERE p.polrelid = family_row.history_oid
                      AND sql_saga.__internal_is_managed_history_policy(p.polname)
                    ORDER BY p.polname
                LOOP
                    EXECUTE format('DROP POLICY IF EXISTS %I ON %s', policy_row.polname /* %I */, family_row.history_oid::regclass /* %s */);
                    RAISE DEBUG 'sql_saga: Dropped policy % on %, part of the teardown by pg_restore --clean',
                        policy_row.polname, family_row.history_oid::regclass;
                END LOOP;
            END LOOP;
        END;
    END IF;

    /* If one of our tables is being dropped, remove references to it */
    FOR r IN
        SELECT dobj.schema_name, dobj.object_name
        FROM unnest(v_dropped_objects) AS u(data)
        CROSS JOIN LATERAL jsonb_to_record(u.data) AS dobj(
            object_type text, schema_name text, object_name text)
        WHERE dobj.object_type = 'table'
          AND EXISTS (SELECT 1 FROM sql_saga.era e WHERE (e.table_schema, e.table_name) = (dobj.schema_name, dobj.object_name))
    LOOP
        -- This table was dropped. Cascade-delete its metadata and any dependent triggers on other tables.
        -- This logic mirrors the metadata-first era drop for a dropped table.

        -- 1. For FKs that reference the dropped table, drop their triggers.
        -- The metadata will be cleaned up through the unique key's cascading foreign key.
        DECLARE
            fk_row record;
            fk_table_oid regclass;
        BEGIN
            FOR fk_row IN
                SELECT fk.*
                FROM sql_saga.foreign_keys fk
                JOIN sql_saga.unique_keys uk ON fk.unique_key_name = uk.unique_key_name
                WHERE (uk.table_schema, uk.table_name) = (r.schema_name, r.object_name)
            LOOP
                fk_table_oid := pg_catalog.to_regclass(format('%I.%I', fk_row.table_schema /* %I */, fk_row.table_name /* %I */));
                -- The referencing table might have been dropped in the same cascading statement. If it still exists, drop its triggers/constraints.
                -- Not during pg_restore --clean: it drops the CHECK (with the table) and the helper function itself.
                IF fk_table_oid IS NOT NULL AND NOT v_teardown THEN
                    IF fk_row.type = 'regular_to_temporal' THEN
                        IF fk_row.fk_check_constraint IS NOT NULL AND EXISTS (
                            SELECT 1 FROM pg_catalog.pg_trigger
                            WHERE tgrelid = fk_table_oid AND tgname = fk_row.fk_check_constraint
                              AND tgfoid = pg_catalog.to_regprocedure('sql_saga.regular_fk_check_c()')) THEN
                            EXECUTE format('DROP TRIGGER %I ON %s', fk_row.fk_check_constraint /* %I */, fk_table_oid /* %s */);
                        /* Historical (pre-1.3) CHECK form. */
                        ELSIF fk_row.fk_check_constraint IS NOT NULL AND fk_row.fk_helper_function IS NOT NULL AND EXISTS (
                            /* Only the released CHECK: it depends on (calls) the catalogued helper function. */
                            SELECT 1 FROM pg_catalog.pg_constraint AS c
                            JOIN pg_catalog.pg_depend AS d
                              ON d.classid = 'pg_catalog.pg_constraint'::regclass AND d.objid = c.oid
                             AND d.refclassid = 'pg_catalog.pg_proc'::regclass
                             AND d.refobjid = pg_catalog.to_regprocedure(fk_row.fk_helper_function)
                            WHERE c.conrelid = fk_table_oid AND c.conname = fk_row.fk_check_constraint AND c.contype = 'c') THEN
                            EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', fk_table_oid /* %s */, fk_row.fk_check_constraint /* %I */);
                        END IF;
                        IF fk_row.fk_helper_function IS NOT NULL THEN
                           EXECUTE format('DROP FUNCTION IF EXISTS %s', fk_row.fk_helper_function /* %s */);
                        END IF;
                    END IF;
                END IF;
            END LOOP;
        END;

        -- 2. For FKs ON the dropped table, remove triggers from the referenced UK table.
        DECLARE
            fk_row record;
            uk_table_oid regclass;
        BEGIN
            FOR fk_row IN
                SELECT fk.*, uk.table_schema AS uk_schema, uk.table_name AS uk_table
                FROM sql_saga.foreign_keys fk JOIN sql_saga.unique_keys uk ON fk.unique_key_name = uk.unique_key_name
                WHERE (fk.table_schema, fk.table_name) = (r.schema_name, r.object_name)
            LOOP
                uk_table_oid := pg_catalog.to_regclass(format('%I.%I', fk_row.uk_schema /* %I */, fk_row.uk_table /* %I */));
                -- Use DROP IF EXISTS because the UK table might have been dropped in the same command.
                IF uk_table_oid IS NOT NULL THEN
                    IF EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = uk_table_oid AND tgname = fk_row.uk_update_trigger) THEN
                        EXECUTE format('DROP TRIGGER %I ON %s', fk_row.uk_update_trigger /* %I */, uk_table_oid /* %s */);
                    END IF;
                    IF EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = uk_table_oid AND tgname = fk_row.uk_delete_trigger) THEN
                        EXECUTE format('DROP TRIGGER %I ON %s', fk_row.uk_delete_trigger /* %I */, uk_table_oid /* %s */);
                    END IF;
                    IF fk_row.uk_truncate_trigger IS NOT NULL
                       AND EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = uk_table_oid AND tgname = fk_row.uk_truncate_trigger) THEN
                        EXECUTE format('DROP TRIGGER %I ON %s', fk_row.uk_truncate_trigger /* %I */, uk_table_oid /* %s */);
                    END IF;
                END IF;
            END LOOP;
        END;

        -- 3. Clean up template sync triggers/functions for each era on this table BEFORE deleting metadata
        DECLARE
            era_row record;
            func_name name;
        BEGIN
            -- Table has been dropped, but we still need to clean up the functions
            FOR era_row IN
                SELECT e.era_name, e.sync_temporal_trg_function_name
                FROM sql_saga.era e
                WHERE (e.table_schema, e.table_name) = (r.schema_name, r.object_name)
                  AND e.sync_temporal_trg_function_name IS NOT NULL  -- Only eras with sync triggers
                  -- Not during pg_restore --clean: it drops the (dumped) function itself, without IF EXISTS.
                  AND NOT v_teardown
            LOOP
                -- Drop the function directly (trigger already gone with table)
                func_name := era_row.sync_temporal_trg_function_name;
                IF to_regprocedure(format('sql_saga.%I()', func_name)) IS NOT NULL THEN
                    -- Do not silently remove a user trigger that reuses this function.
                    EXECUTE format($$DROP FUNCTION sql_saga.%I()$$, func_name);
                END IF;
            END LOOP;
        END;

        -- 4. Delete all metadata for the dropped table.
        DELETE FROM sql_saga.updatable_view WHERE (table_schema, table_name) = (r.schema_name, r.object_name);
        DELETE FROM sql_saga.foreign_keys WHERE (table_schema, table_name) = (r.schema_name, r.object_name);
        DELETE FROM sql_saga.unique_keys WHERE (table_schema, table_name) = (r.schema_name, r.object_name);
        DELETE FROM sql_saga.era WHERE (table_schema, table_name) = (r.schema_name, r.object_name);
    END LOOP;

    /*
     * Policies (doc/design/history-access.md). A dropped policy on a versioned base table
     * drops its history copy. Policies sql_saga manages on a history table cannot be dropped
     * directly. address_names of a policy are {schema, table, policy}.
     */
    DECLARE
    BEGIN
    FOR r IN
        SELECT f.base_oid, f.history_oid,
               to_regclass(format('%I.%I', dobj.address_names[1], dobj.address_names[2]))::oid AS rel_oid,
               array_agg(dobj.address_names[3]::name ORDER BY dobj.ordinality) AS policy_names
        FROM unnest(v_dropped_objects) AS u(data)
        CROSS JOIN LATERAL jsonb_to_record(u.data) AS dobj(
            object_type text, address_names text[], ordinality bigint)
        CROSS JOIN LATERAL sql_saga.__internal_history_family(
            to_regclass(format('%I.%I', dobj.address_names[1], dobj.address_names[2]))) AS f
        WHERE dobj.object_type = 'policy'
        GROUP BY 1, 2, 3
    LOOP
        -- Not during pg_restore --clean: it drops the dumped copies itself, without IF EXISTS.
        IF r.rel_oid = r.base_oid AND NOT v_teardown
           AND NOT sql_saga.__internal_history_policy_sync_active(r.base_oid) THEN
            PERFORM sql_saga.__internal_sync_history_policies(r.base_oid, r.policy_names);
        END IF;
    END LOOP;
    END;
END;
$function$;/*
 * sql_saga 1.8 update: catalog layout changes (schema fragment of devops/generate_update_scripts_1_8.py).
 * Runs on a catalog with the 1.4 layout (1.4 to 1.7): directly from 1.4 to 1.7, or after the embedded update from
 * 1.0 to 1.3. Kept in step with src/0b_schema.sql. The generator places it after the 1.8 routines: the DROP COLUMN
 * fires sql_saga's event triggers, whose 1.7 definitions still read the dropped columns.
 */

/*
 * era: audit_schema_name and audit_table_name came from the periods extension's system versioning, which sql_saga
 * replaced with its own (sql_saga.system_versioning). No release ever wrote them. They are dropped only when every
 * row is NULL; a value means someone wrote one by hand, and the update refuses rather than discard it.
 */
DO $era_audit_columns_1_8$
DECLARE
    n_columns integer;
    n_set bigint;
BEGIN
    SELECT pg_catalog.count(*) INTO n_columns
    FROM pg_catalog.pg_attribute AS a
    WHERE a.attrelid = 'sql_saga.era'::pg_catalog.regclass AND NOT a.attisdropped
      AND a.attname IN ('audit_schema_name', 'audit_table_name');
    IF n_columns <> 2 THEN
        RAISE EXCEPTION 'sql_saga: expected the columns audit_schema_name and audit_table_name of the 1.4 layout on sql_saga.era, found %', n_columns
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Restore the sql_saga catalog tables as the installed release created them, then retry the update.';
    END IF;
    EXECUTE 'SELECT pg_catalog.count(*) FROM sql_saga.era WHERE audit_schema_name IS NOT NULL OR audit_table_name IS NOT NULL'
        INTO n_set;
    IF n_set > 0 THEN
        RAISE EXCEPTION 'sql_saga: % rows of sql_saga.era have audit_schema_name or audit_table_name set; sql_saga never writes them', n_set
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Set both columns to NULL in those rows (no sql_saga release reads them), then retry the update.';
    END IF;
    ALTER TABLE sql_saga.era DROP COLUMN audit_schema_name, DROP COLUMN audit_table_name;
    RAISE NOTICE 'sql_saga: Dropped the unused columns audit_schema_name and audit_table_name of table sql_saga.era';
END;
$era_audit_columns_1_8$;

/*
 * era.catalog_format: the era layout changes, so a 1.4 to 1.7 dump restored directly into 1.8 must fail at its first
 * era row. The released constraint is identified by its exact definition.
 */
DO $catalog_format_1_8$
DECLARE
    released CONSTANT text := 'CHECK (((catalog_format IS NOT NULL) AND (catalog_format = ''1.4''::text)))';
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS c
                   WHERE c.conrelid = 'sql_saga.era'::pg_catalog.regclass
                     AND c.conname = 'restore old sql_saga dumps per doc/upgrade.md'
                     AND pg_catalog.pg_get_constraintdef(c.oid) = released) THEN
        RAISE EXCEPTION 'sql_saga: expected the released catalog_format constraint of sql_saga 1.4 to 1.7 on sql_saga.era'
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Restore the sql_saga catalog tables as the installed release created them, then retry the update.';
    END IF;
    ALTER TABLE sql_saga.era DROP CONSTRAINT "restore old sql_saga dumps per doc/upgrade.md";
    UPDATE sql_saga.era SET catalog_format = '1.8';
    ALTER TABLE sql_saga.era
        ADD CONSTRAINT "restore old sql_saga dumps per doc/upgrade.md" CHECK (catalog_format IS NOT NULL AND catalog_format = '1.8');
    RAISE NOTICE 'sql_saga: Set catalog_format to 1.8 on % era rows of table sql_saga.era', (SELECT pg_catalog.count(*) FROM sql_saga.era);
END;
$catalog_format_1_8$;
/*
 * sql_saga 1.8 update: repairs after the 1.8 definitions are loaded (devops/generate_update_scripts_1_8.py).
 * The generator emits the final integrity gate right after this.
 *
 * None: 1.8 drops two never-written catalog columns (devops/upgrade_1_8_schema.sql) and changes four routines.
 * The final integrity gate checks the persistence of every history table, which the 1.8 health check now
 * guards; a history table made UNLOGGED earlier refuses the update, naming ALTER TABLE ... SET LOGGED.
 */
SELECT sql_saga.check_integrity();
