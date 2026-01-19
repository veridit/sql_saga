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
    source_table_oid regclass;
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
        v.table_schema, v.table_name, v.era_name, e.range_column_name, e.valid_from_column_name, e.valid_until_column_name, e.valid_to_column_name,
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

    -- The function to get the current time is stored in the metadata for testability
    now_function := info.current_func;

    IF (TG_OP = 'INSERT') THEN
        -- An INSERT creates a new history, starting now.
        -- This logic is clean and can use temporal_merge.
        IF to_regclass('pg_temp.current_view_trigger_source_seq') IS NULL THEN
            CREATE TEMP SEQUENCE current_view_trigger_source_seq;
            GRANT USAGE ON SEQUENCE pg_temp.current_view_trigger_source_seq TO PUBLIC;
        END IF;
        source_table_name := format('current_view_trigger_source_%s', nextval('pg_temp.current_view_trigger_source_seq') /* %s */);

        IF to_regclass(source_table_name) IS NOT NULL THEN
            EXECUTE format('DROP TABLE %I', source_table_name /* %I */);
        END IF;

        EXECUTE format(
            'CREATE TEMP TABLE %I ON COMMIT DROP AS SELECT 1 as row_id, ($1).*',
            source_table_name /* %I */
        ) USING NEW;

        -- Handle both range-only and boundary column modes
        IF info.valid_from_column_name IS NOT NULL AND info.valid_until_column_name IS NOT NULL THEN
            -- Boundary columns exist - use them
            EXECUTE format(
                'UPDATE %I SET %I = %s::%s, %I = ''infinity''::%s',
                source_table_name, /* %I */
                info.valid_from_column_name, /* %I */
                now_function, /* %s */
                info.range_subtype, /* %s */
                info.valid_until_column_name, /* %I */
                info.range_subtype /* %s */
            );
        ELSE
            -- Range-only mode - set range column directly
            EXECUTE format(
                'UPDATE %I SET %I = %srange(%s, ''infinity'')',
                source_table_name, /* %I */
                info.range_column_name, /* %I */
                info.range_subtype, /* %s */
                now_function /* %s */
            );
        END IF;
        source_table_oid := to_regclass(pg_my_temp_schema()::regnamespace::text || '.' || quote_ident(source_table_name));

        CALL sql_saga.temporal_merge(
            target_table             => info.table_oid,
            source_table             => source_table_oid,
            primary_identity_columns => identifier_columns::text[],
            ephemeral_columns        => '{}'::text[],
            era_name                 => info.era_name,
            mode                     => 'MERGE_ENTITY_UPSERT'::sql_saga.temporal_merge_mode,
            row_id_column            => 'row_id',
            founding_id_column       => 'row_id'
        );
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
