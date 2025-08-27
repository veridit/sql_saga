CREATE FUNCTION sql_saga.add_foreign_key(
        fk_table_oid regclass,
        fk_column_names name[],
        -- TODO: Simplify API with the following
        -- foreign_table_name regclass,
        -- foreign_fk_column_names name[],
        fk_era_name name,
        unique_key_name name,
        match_type sql_saga.fk_match_types DEFAULT 'SIMPLE',
        update_action sql_saga.fk_actions DEFAULT 'NO ACTION',
        delete_action sql_saga.fk_actions DEFAULT 'NO ACTION',
        foreign_key_name name DEFAULT NULL,
        fk_insert_trigger name DEFAULT NULL,
        fk_update_trigger name DEFAULT NULL,
        uk_update_trigger name DEFAULT NULL,
        uk_delete_trigger name DEFAULT NULL)
 RETURNS name
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    fk_era_row sql_saga.era;
    uk_era_row sql_saga.era;
    uk_row sql_saga.unique_keys;
    fk_schema_name name;
    fk_table_name name;
    uk_table_oid regclass;
    uk_schema_name name;
    uk_table_name name;
    column_attnums smallint[];
    idx integer;
    pass integer;
    upd_action text DEFAULT '';
    del_action text DEFAULT '';
    foreign_columns_with_era_columns text;
    unique_columns_with_era_columns text;
BEGIN
    IF fk_table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(fk_table_oid);

    SELECT n.nspname, c.relname INTO fk_schema_name, fk_table_name
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = fk_table_oid;

    /* Get the period involved */
    SELECT e.*
    INTO fk_era_row
    FROM sql_saga.era AS e
    WHERE (e.table_schema, e.table_name, e.era_name) = (fk_schema_name, fk_table_name, fk_era_name);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'era "%" does not exist', fk_era_name;
    END IF;

    /* Get column attnums from column names */
    SELECT array_agg(a.attnum ORDER BY n.ordinality)
    INTO column_attnums
    FROM unnest(fk_column_names) WITH ORDINALITY AS n (name, ordinality)
    LEFT JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (fk_table_oid, n.name);

    /* System columns are not allowed */
    IF 0 > ANY (column_attnums) THEN
        RAISE EXCEPTION 'index creation on system columns is not supported';
    END IF;

    /* Report if any columns weren't found */
    idx := array_position(column_attnums, NULL);
    IF idx IS NOT NULL THEN
        RAISE EXCEPTION 'column "%" does not exist', fk_column_names[idx];
    END IF;

    /* Make sure the period columns aren't also in the normal columns */
    IF fk_era_row.valid_from_column_name = ANY (fk_column_names) THEN
        RAISE EXCEPTION 'column "%" specified twice', fk_era_row.valid_from_column_name;
    END IF;
    IF fk_era_row.valid_until_column_name = ANY (fk_column_names) THEN
        RAISE EXCEPTION 'column "%" specified twice', fk_era_row.valid_until_column_name;
    END IF;

    /* Get the unique key we're linking to */
    SELECT uk.*
    INTO uk_row
    FROM sql_saga.unique_keys AS uk
    WHERE uk.unique_key_name = unique_key_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'unique key "%" does not exist', unique_key_name;
    END IF;

    /* Get the unique key's eroa */
    SELECT e.*
    INTO uk_era_row
    FROM sql_saga.era AS e
    WHERE (e.table_schema, e.table_name, e.era_name) = (uk_row.table_schema, uk_row.table_name, uk_row.era_name);

    IF fk_era_row.range_type <> uk_era_row.range_type THEN
        RAISE EXCEPTION 'era types "%" and "%" are incompatible',
            fk_era_row.era_name, uk_era_row.era_name;
    END IF;

    uk_schema_name := uk_row.table_schema;
    uk_table_name := uk_row.table_name;
    uk_table_oid := format('%I.%I', uk_schema_name, uk_table_name)::regclass;

    /* Check that all the columns match */
    IF EXISTS (
        SELECT FROM unnest(fk_column_names, uk_row.column_names) AS u (fk_attname, uk_attname)
        JOIN pg_catalog.pg_attribute AS fa ON (fa.attrelid, fa.attname) = (fk_table_oid, u.fk_attname)
        JOIN pg_catalog.pg_attribute AS ua ON (ua.attrelid, ua.attname) = (uk_table_oid, u.uk_attname)
        WHERE (fa.atttypid, fa.atttypmod, fa.attcollation) <> (ua.atttypid, ua.atttypmod, ua.attcollation))
    THEN
        RAISE EXCEPTION 'column types do not match';
    END IF;

    /* The range types must match, too */
    IF fk_era_row.range_type <> uk_era_row.range_type THEN
        RAISE EXCEPTION 'period types do not match';
    END IF;

    /*
     * Generate a name for the foreign constraint.  We don't have to worry about
     * concurrency here because all period ddl commands lock the periods table.
     */
    IF foreign_key_name IS NULL THEN
        foreign_key_name := sql_saga.__internal_make_name(
            ARRAY[(SELECT c.relname FROM pg_catalog.pg_class AS c WHERE c.oid = fk_table_oid)]
               || fk_column_names
               || ARRAY[fk_era_name]);
    END IF;
    pass := 0;
    WHILE EXISTS (
       SELECT FROM sql_saga.foreign_keys AS fk
       WHERE fk.foreign_key_name = foreign_key_name || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END)
    LOOP
       pass := pass + 1;
    END LOOP;
    foreign_key_name := foreign_key_name || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END;

    -- TODO: Consider how update_action should be handled, it seems
    -- clear it should affect the timing of the trigger.
    /* See if we're deferring the constraints or not */
    -- IF update_action = 'NO ACTION' THEN
    --     upd_action := ' DEFERRABLE INITIALLY DEFERRED';
    -- END IF;
    -- IF delete_action = 'NO ACTION' THEN
    --     del_action := ' DEFERRABLE INITIALLY DEFERRED';
    -- END IF;

    /* Get the columns that require checking the constraint */
    SELECT string_agg(quote_ident(u.column_name), ', ' ORDER BY u.ordinality)
    INTO foreign_columns_with_era_columns
    FROM unnest(fk_column_names || fk_era_row.valid_from_column_name || fk_era_row.valid_until_column_name) WITH ORDINALITY AS u (column_name, ordinality);

    -- If a 'valid_to' column exists, add it to the list of columns that
    -- trigger the fk_update_check. This handles cases where a BEFORE trigger
    -- synchronizes valid_to and valid_until, ensuring validation fires correctly
    -- without making the trigger an overly-broad row-level trigger.
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_attribute
        WHERE attrelid = fk_table_oid AND attname = 'valid_to' AND NOT attisdropped
    ) THEN
        foreign_columns_with_era_columns := foreign_columns_with_era_columns || ', ' || quote_ident('valid_to');
    END IF;

    SELECT string_agg(quote_ident(u.column_name), ', ' ORDER BY u.ordinality)
    INTO unique_columns_with_era_columns
    FROM unnest(uk_row.column_names || uk_era_row.valid_from_column_name || uk_era_row.valid_until_column_name) WITH ORDINALITY AS u (column_name, ordinality);

    /* Add all the known variables for the triggers to avoid lookups when executing the triggers. */
    DECLARE
        fk_valid_from_column_name text := fk_era_row.valid_from_column_name;
        fk_valid_until_column_name text := fk_era_row.valid_until_column_name;

        fk_column_names_arr_str text := format('{%s}',fk_column_names);
        uk_column_names_arr_str text := format('{%s}',uk_row.column_names);

        uk_era_name text := uk_era_row.era_name;
        uk_table_oid regclass := uk_table_oid;

        uk_valid_from_column_name text := uk_era_row.valid_from_column_name;
        uk_valid_until_column_name text := uk_era_row.valid_until_column_name;
    BEGIN
        /* Time to make the underlying triggers */
        fk_insert_trigger := coalesce(fk_insert_trigger, sql_saga.__internal_make_name(ARRAY[foreign_key_name], 'fk_insert'));
        EXECUTE format($$
            CREATE CONSTRAINT TRIGGER %17$I AFTER INSERT ON %2$I.%3$I FROM %8$I.%9$I DEFERRABLE FOR EACH ROW EXECUTE PROCEDURE
            sql_saga.fk_insert_check_c(%1$L,%2$L,%3$L,%4$L,%5$L,%6$L,%7$L,%8$L,%9$L,%10$L,%11$L,%12$L,%13$L,%14$L,%15$L,%16$L);
            $$
            -- Parameters for the function call in the template
            , /* %1$  */ foreign_key_name
            , /* %2$  */ fk_schema_name
            , /* %3$  */ fk_table_name
            , /* %4$  */ fk_column_names_arr_str
            , /* %5$  */ fk_era_name
            , /* %6$  */ fk_valid_from_column_name
            , /* %7$  */ fk_valid_until_column_name
            , /* %8$ */ uk_schema_name
            , /* %9$ */ uk_table_name
            , /* %10$ */ uk_column_names_arr_str
            , /* %11$ */ uk_era_name
            , /* %12$ */ uk_valid_from_column_name
            , /* %13$ */ uk_valid_until_column_name
            , /* %14$ */ match_type
            , /* %15$ */ update_action
            , /* %16$ */ delete_action
            -- Other parameters
            , /* %17$  */ fk_insert_trigger
        );

        fk_update_trigger := coalesce(fk_update_trigger, sql_saga.__internal_make_name(ARRAY[foreign_key_name], 'fk_update'));
        EXECUTE format($$
            CREATE CONSTRAINT TRIGGER %17$I AFTER UPDATE OF %18$s ON %2$I.%3$I FROM %8$I.%9$I DEFERRABLE FOR EACH ROW EXECUTE PROCEDURE
            sql_saga.fk_update_check_c(%1$L,%2$L,%3$L,%4$L,%5$L,%6$L,%7$L,%8$L,%9$L,%10$L,%11$L,%12$L,%13$L,%14$L,%15$L,%16$L);
            $$
            -- Parameters for the function call in the template
            , /* %1$  */ foreign_key_name
            , /* %2$  */ fk_schema_name
            , /* %3$  */ fk_table_name
            , /* %4$  */ fk_column_names_arr_str
            , /* %5$  */ fk_era_name
            , /* %6$  */ fk_valid_from_column_name
            , /* %7$  */ fk_valid_until_column_name
            , /* %8$ */ uk_schema_name
            , /* %9$ */ uk_table_name
            , /* %10$ */ uk_column_names_arr_str
            , /* %11$ */ uk_era_name
            , /* %12$ */ uk_valid_from_column_name
            , /* %13$ */ uk_valid_until_column_name
            , /* %14$ */ match_type
            , /* %15$ */ update_action
            , /* %16$ */ delete_action
            -- Other parameters
            , /* %17$   */ fk_update_trigger
            , /* %18$   */ foreign_columns_with_era_columns
        );

        uk_update_trigger := coalesce(uk_update_trigger, sql_saga.__internal_make_name(ARRAY[foreign_key_name], 'uk_update'));
        EXECUTE format($$
            CREATE CONSTRAINT TRIGGER %17$I AFTER UPDATE OF %18$s ON %8$I.%9$I FROM %2$I.%3$I DEFERRABLE FOR EACH ROW EXECUTE PROCEDURE
            sql_saga.uk_update_check_c(%1$L,%2$L,%3$L,%4$L,%5$L,%6$L,%7$L,%8$L,%9$L,%10$L,%11$L,%12$L,%13$L,%14$L,%15$L,%16$L);
            $$
            -- Parameters for the function call in the template
            , /* %1$  */ foreign_key_name
            , /* %2$  */ fk_schema_name
            , /* %3$  */ fk_table_name
            , /* %4$  */ fk_column_names_arr_str
            , /* %5$  */ fk_era_name
            , /* %6$  */ fk_valid_from_column_name
            , /* %7$  */ fk_valid_until_column_name
            , /* %8$ */ uk_schema_name
            , /* %9$ */ uk_table_name
            , /* %10$ */ uk_column_names_arr_str
            , /* %11$ */ uk_era_name
            , /* %12$ */ uk_valid_from_column_name
            , /* %13$ */ uk_valid_until_column_name
            , /* %14$ */ match_type
            , /* %15$ */ update_action
            , /* %16$ */ delete_action
            -- Other parameters
            , /* %17$ */ uk_update_trigger
            , /* %18$ */ unique_columns_with_era_columns
        );

        uk_delete_trigger := coalesce(uk_delete_trigger, sql_saga.__internal_make_name(ARRAY[foreign_key_name], 'uk_delete'));
        EXECUTE format($$
            CREATE CONSTRAINT TRIGGER %17$I AFTER DELETE ON %8$I.%9$I FROM %2$I.%3$I DEFERRABLE FOR EACH ROW EXECUTE PROCEDURE
            sql_saga.uk_delete_check_c(%1$L,%2$L,%3$L,%4$L,%5$L,%6$L,%7$L,%8$L,%9$L,%10$L,%11$L,%12$L,%13$L,%14$L,%15$L,%16$L);
            $$
            -- Parameters for the function call in the template
            , /* %1$  */ foreign_key_name
            , /* %2$  */ fk_schema_name
            , /* %3$  */ fk_table_name
            , /* %4$  */ fk_column_names_arr_str
            , /* %5$  */ fk_era_name
            , /* %6$  */ fk_valid_from_column_name
            , /* %7$  */ fk_valid_until_column_name
            , /* %8$ */ uk_schema_name
            , /* %9$ */ uk_table_name
            , /* %10$ */ uk_column_names_arr_str
            , /* %11$ */ uk_era_name
            , /* %12$ */ uk_valid_from_column_name
            , /* %13$ */ uk_valid_until_column_name
            , /* %14$ */ match_type
            , /* %15$ */ update_action
            , /* %16$ */ delete_action
            -- Other parameters
            , /* %17$  */ uk_delete_trigger
        );

        INSERT INTO sql_saga.foreign_keys
            ( foreign_key_name
            , table_schema
            , table_name
            , column_names
            , era_name
            , unique_key_name
            , match_type
            , update_action
            , delete_action
            , fk_insert_trigger
            , fk_update_trigger
            , uk_update_trigger
            , uk_delete_trigger
            ) VALUES
            ( foreign_key_name
            , fk_schema_name
            , fk_table_name
            , fk_column_names
            , fk_era_name
            , uk_row.unique_key_name
            , match_type
            , update_action
            , delete_action
            , fk_insert_trigger
            , fk_update_trigger
            , uk_update_trigger
            , uk_delete_trigger
            );

        /* Validate the constraint on existing data. */
        DECLARE
            uk_where_clause text;
            violating_row_found boolean;
        BEGIN
            IF match_type = 'FULL' THEN
                DECLARE
                    fk_any_null_clause text;
                    fk_all_null_clause text;
                BEGIN
                    SELECT string_agg(format('fk.%I IS NULL', u.fkc), ' OR ')
                    INTO fk_any_null_clause
                    FROM unnest(fk_column_names) AS u(fkc);

                    SELECT string_agg(format('fk.%I IS NULL', u.fkc), ' AND ')
                    INTO fk_all_null_clause
                    FROM unnest(fk_column_names) AS u(fkc);

                    EXECUTE format(
                        'SELECT EXISTS (SELECT 1 FROM %1$I.%2$I AS fk WHERE (%3$s) AND NOT (%4$s))',
                        fk_schema_name,       -- %1$I
                        fk_table_name,        -- %2$I
                        fk_any_null_clause,   -- %3$s
                        fk_all_null_clause    -- %4$s
                    )
                    INTO violating_row_found;

                    IF violating_row_found THEN
                        RAISE EXCEPTION 'insert or update on table "%" violates foreign key constraint "%" (MATCH FULL with NULLs)',
                                fk_table_oid,
                                foreign_key_name;
                    END IF;
                END;
            END IF;

            -- Now check for coverage violations on rows with non-null keys.
            DECLARE
                fk_not_null_clause text;
            BEGIN
                SELECT string_agg(format('fk.%I IS NOT NULL', u.fkc), ' AND ')
                INTO fk_not_null_clause
                FROM unnest(fk_column_names) AS u(fkc);

                SELECT string_agg(format('uk.%I = fk.%I', u.ukc, u.fkc), ' AND ')
                INTO uk_where_clause
                FROM unnest(uk_row.column_names, fk_column_names) AS u(ukc, fkc);

                EXECUTE format('SELECT EXISTS( ' ||
                    'SELECT 1 FROM %1$I.%2$I AS fk ' ||
                    'WHERE %12$s AND NOT COALESCE(( ' ||
                    '  SELECT sql_saga.covers_without_gaps( ' ||
                    '    %3$s(uk.%4$I, uk.%5$I), ' ||
                    '    %6$s(fk.%7$I, fk.%8$I) ' ||
                    '    ORDER BY uk.%4$I ' ||
                    '  ) ' ||
                    '  FROM %9$I.%10$I AS uk ' ||
                    '  WHERE %11$s ' ||
                    '), false))',
                    fk_schema_name,                       -- %1$I
                    fk_table_name,                        -- %2$I
                    uk_era_row.range_type,                -- %3$s (regtype cast to text)
                    uk_era_row.valid_from_column_name,    -- %4$I
                    uk_era_row.valid_until_column_name,   -- %5$I
                    fk_era_row.range_type,                -- %6$s (regtype cast to text)
                    fk_valid_from_column_name,    -- %7$I
                    fk_valid_until_column_name,   -- %8$I
                    uk_schema_name,                       -- %9$I
                    uk_table_name,                      -- %10$I
                    uk_where_clause,                    -- %11$s
                    fk_not_null_clause                  -- %12$s
                ) INTO violating_row_found;

                IF violating_row_found THEN
                    RAISE EXCEPTION 'insert or update on table "%" violates foreign key constraint "%"',
                        fk_table_oid,
                        foreign_key_name;
                END IF;
            END;
        END;
    END;

    RETURN foreign_key_name;
END;
$function$;
