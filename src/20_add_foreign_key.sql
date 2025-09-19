CREATE OR REPLACE FUNCTION sql_saga.__find_or_create_fk_index(
    p_fk_table_oid regclass,
    p_fk_column_names name[],
    p_fk_era_row sql_saga.era,
    p_type sql_saga.fg_type,
    p_create_index boolean
) RETURNS name
LANGUAGE plpgsql
AS $function$
#variable_conflict use_variable
DECLARE
    v_fk_schema_name name;
    v_fk_table_name name;
    v_index_def text;
    v_index_name name;
    v_index_type name;
    v_existing_index_name name;
    v_fk_attnums smallint[];
BEGIN
    SELECT n.nspname, c.relname INTO v_fk_schema_name, v_fk_table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = p_fk_table_oid;

    SELECT array_agg(a.attnum ORDER BY u.ord) INTO v_fk_attnums
    FROM unnest(p_fk_column_names) WITH ORDINALITY u(col, ord)
    JOIN pg_attribute a ON a.attrelid = p_fk_table_oid AND a.attname = u.col;

    IF p_type = 'temporal_to_temporal' THEN
        -- For temporal FKs, we need a GiST index on the columns and the era range.
        -- Finding a compatible index is very complex due to expression indexes.
        -- This check is intentionally basic: it finds a GiST index where the FK columns are a prefix.
        -- It does not validate the rest of the index (e.g., the range expression), as that is not
        -- feasible in PL/pgSQL. It is a best-effort check to avoid creating a redundant index.
        v_index_type := 'GIST';
        v_index_name := sql_saga.__internal_make_name(
            ARRAY[v_fk_table_name] || p_fk_column_names || ARRAY[p_fk_era_row.era_name, 'gist', 'idx']
        );
        v_index_def := format(
            'CREATE INDEX %I ON %s USING GIST (%s, %s(%I, %I))',
            v_index_name,
            p_fk_table_oid::text,
            (SELECT string_agg(quote_ident(c), ', ') FROM unnest(p_fk_column_names) AS c),
            p_fk_era_row.range_type,
            p_fk_era_row.valid_from_column_name,
            p_fk_era_row.valid_until_column_name
        );

        SELECT c.relname INTO v_existing_index_name
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
        JOIN pg_catalog.pg_am am ON am.oid = c.relam
        WHERE i.indrelid = p_fk_table_oid
          AND i.indisvalid AND am.amname = 'gist'
          AND i.indnkeyatts >= array_length(v_fk_attnums, 1)
          AND (i.indkey::smallint[])[0:array_length(v_fk_attnums, 1)-1] = v_fk_attnums
        LIMIT 1;

    ELSE -- regular_to_temporal
        v_index_type := 'BTREE';
        v_index_name := sql_saga.__internal_make_name(
            ARRAY[v_fk_table_name] || p_fk_column_names || ARRAY['idx']
        );
        v_index_def := format(
            'CREATE INDEX %I ON %s USING BTREE (%s)',
            v_index_name,
            p_fk_table_oid::text,
            (SELECT string_agg(quote_ident(c), ', ') FROM unnest(p_fk_column_names) AS c)
        );

        -- Look for an existing btree index where the FK columns are a prefix.
        SELECT c.relname INTO v_existing_index_name
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
        JOIN pg_catalog.pg_am am ON am.oid = c.relam
        WHERE i.indrelid = p_fk_table_oid
          AND i.indisvalid AND am.amname = 'btree'
          AND i.indnkeyatts >= array_length(v_fk_attnums, 1)
          AND (i.indkey::smallint[])[0:array_length(v_fk_attnums, 1)-1] = v_fk_attnums
        LIMIT 1;
    END IF;

    IF v_existing_index_name IS NOT NULL THEN
        RAISE DEBUG 'Found existing compatible index "%" for foreign key on table %', v_existing_index_name, p_fk_table_oid::text;
        RETURN NULL; -- Compatible index exists, do nothing.
    END IF;

    IF p_create_index THEN
        RAISE NOTICE 'No compatible index found for foreign key on table %. Creating new index: %', p_fk_table_oid::text, v_index_def;
        EXECUTE v_index_def;
        RETURN v_index_name;
    ELSE
        RAISE WARNING 'No index found on table % for foreign key columns (%). Performance may be poor.',
            p_fk_table_oid::text, p_fk_column_names
        USING HINT = format('Consider creating this index: %s', v_index_def);
        RETURN NULL;
    END IF;
END;
$function$;

-- Declarative wrapper for creating foreign keys.
-- This function automatically determines whether to create a temporal or regular FK
-- and looks up the internal unique key name.
CREATE FUNCTION sql_saga.add_foreign_key(
        fk_table_oid regclass,
        fk_column_names name[],
        pk_table_oid regclass,
        pk_column_names name[],
        fk_era_name name DEFAULT NULL, -- Only needed to disambiguate if FK table has multiple eras
        match_type sql_saga.fk_match_types DEFAULT 'SIMPLE',
        update_action sql_saga.fk_actions DEFAULT 'NO ACTION',
        delete_action sql_saga.fk_actions DEFAULT 'NO ACTION',
        foreign_key_name name DEFAULT NULL,
        create_index boolean DEFAULT true
)
RETURNS name
LANGUAGE plpgsql
SECURITY DEFINER
AS
$function_declarative_fk$
#variable_conflict use_variable
DECLARE
    v_fk_schema_name name;
    v_fk_table_name name;
    v_pk_schema_name name;
    v_pk_table_name name;
    v_is_fk_temporal boolean;
    v_unique_key_name name;
    v_fk_era_name name := fk_era_name;
BEGIN
    -- Get schema and table names for both tables
    SELECT n.nspname, c.relname INTO v_fk_schema_name, v_fk_table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = fk_table_oid;

    SELECT n.nspname, c.relname INTO v_pk_schema_name, v_pk_table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = pk_table_oid;

    -- Look up the unique key name based on the PK table and columns.
    -- We match on primary or natural keys, as they are the intended targets for FKs.
    -- The ENUM order ('primary', 'natural', ...) ensures we prefer primary keys.
    SELECT uk.unique_key_name INTO v_unique_key_name
    FROM sql_saga.unique_keys uk
    WHERE uk.table_schema = v_pk_schema_name
      AND uk.table_name = v_pk_table_name
      AND uk.column_names = pk_column_names
      AND uk.key_type IN ('primary', 'natural')
    ORDER BY uk.key_type
    LIMIT 1;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'No primary or natural unique key found on table %.% for columns %',
            quote_ident(v_pk_schema_name), quote_ident(v_pk_table_name), pk_column_names;
    END IF;

    -- Determine if the FK table is temporal
    SELECT EXISTS (
        SELECT 1 FROM sql_saga.era e
        WHERE (e.table_schema, e.table_name) = (v_fk_schema_name, v_fk_table_name)
    ) INTO v_is_fk_temporal;

    IF v_is_fk_temporal THEN
        -- If fk_era_name is not provided, try to auto-detect it.
        IF v_fk_era_name IS NULL THEN
            SELECT e.era_name INTO v_fk_era_name
            FROM sql_saga.era e
            WHERE (e.table_schema, e.table_name) = (v_fk_schema_name, v_fk_table_name);

            -- If there's more than one era, the user must specify which one to use.
            IF (SELECT count(*) FROM sql_saga.era e WHERE (e.table_schema, e.table_name) = (v_fk_schema_name, v_fk_table_name)) > 1 THEN
                RAISE EXCEPTION 'Table %.% has multiple eras. Please specify the fk_era_name parameter.',
                    quote_ident(v_fk_schema_name), quote_ident(v_fk_table_name);
            END IF;
        END IF;

        RETURN sql_saga.add_temporal_foreign_key(
            fk_table_oid => fk_table_oid,
            fk_column_names => fk_column_names,
            fk_era_name => v_fk_era_name,
            unique_key_name => v_unique_key_name,
            match_type => match_type,
            update_action => update_action,
            delete_action => delete_action,
            foreign_key_name => foreign_key_name,
            create_index => create_index
        );
    ELSE
        RETURN sql_saga.add_regular_foreign_key(
            fk_table_oid => fk_table_oid,
            fk_column_names => fk_column_names,
            unique_key_name => v_unique_key_name,
            match_type => match_type,
            update_action => update_action,
            delete_action => delete_action,
            foreign_key_name => foreign_key_name,
            create_index => create_index
        );
    END IF;
END;
$function_declarative_fk$;


-- Overloaded function for regular (non-temporal) to temporal foreign keys
CREATE FUNCTION sql_saga.add_regular_foreign_key(
        fk_table_oid regclass,
        fk_column_names name[],
        unique_key_name name,
        match_type sql_saga.fk_match_types DEFAULT 'SIMPLE',
        update_action sql_saga.fk_actions DEFAULT 'NO ACTION',
        delete_action sql_saga.fk_actions DEFAULT 'NO ACTION',
        foreign_key_name name DEFAULT NULL,
        fk_check_constraint name DEFAULT NULL,
        fk_helper_function text DEFAULT NULL,
        uk_update_trigger name DEFAULT NULL,
        uk_delete_trigger name DEFAULT NULL,
        create_index boolean DEFAULT true)
 RETURNS name
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function_regular_fk$
#variable_conflict use_variable
DECLARE
    uk_era_row sql_saga.era;
    uk_row sql_saga.unique_keys;
    fk_schema_name name;
    fk_table_name name;
    fk_table_columns_snapshot name[];
    uk_table_oid regclass;
    uk_schema_name name;
    uk_table_name name;
    pass integer;
    unique_columns_with_era_columns text;
    uk_where_clause text;
    helper_signature text;
    v_fk_index_name name;
BEGIN
    IF fk_table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    PERFORM sql_saga.__internal_serialize(fk_table_oid);

    SELECT n.nspname, c.relname INTO fk_schema_name, fk_table_name
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = fk_table_oid;

    SELECT array_agg(a.attname ORDER BY a.attnum) INTO fk_table_columns_snapshot
    FROM pg_catalog.pg_attribute AS a
    WHERE a.attrelid = fk_table_oid AND a.attnum > 0 AND NOT a.attisdropped;

    -- Verify that the referencing table is NOT temporal.
    IF EXISTS (
        SELECT 1 FROM sql_saga.era e
        WHERE (e.table_schema, e.table_name) = (fk_schema_name, fk_table_name)
    ) THEN
        RAISE EXCEPTION 'Table %.% is a temporal table. Use the temporal-to-temporal version of add_temporal_foreign_key by providing fk_era_name.',
            quote_ident(fk_schema_name), quote_ident(fk_table_name);
    END IF;

    /* Get the unique key we're linking to */
    SELECT uk.* INTO uk_row
    FROM sql_saga.unique_keys AS uk
    WHERE uk.unique_key_name = unique_key_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'unique key "%" does not exist', unique_key_name;
    END IF;

    /* Get the unique key's era */
    SELECT e.* INTO uk_era_row
    FROM sql_saga.era AS e
    WHERE (e.table_schema, e.table_name, e.era_name) = (uk_row.table_schema, uk_row.table_name, uk_row.era_name);

    uk_schema_name := uk_row.table_schema;
    uk_table_name := uk_row.table_name;
    uk_table_oid := format('%I.%I', uk_schema_name /* %I */, uk_table_name /* %I */)::regclass;

    v_fk_index_name := sql_saga.__find_or_create_fk_index(
        fk_table_oid,
        fk_column_names,
        NULL, -- no era row for regular fk
        'regular_to_temporal',
        create_index
    );

    /* Check that all the columns match */
    IF EXISTS (
        SELECT FROM unnest(fk_column_names, uk_row.column_names) AS u (fk_attname, uk_attname)
        JOIN pg_catalog.pg_attribute AS fa ON (fa.attrelid, fa.attname) = (fk_table_oid, u.fk_attname)
        JOIN pg_catalog.pg_attribute AS ua ON (ua.attrelid, ua.attname) = (uk_table_oid, u.uk_attname)
        WHERE (fa.atttypid, fa.atttypmod, fa.attcollation) <> (ua.atttypid, ua.atttypmod, ua.attcollation))
    THEN
        RAISE EXCEPTION 'column types do not match';
    END IF;

    /* Generate a name for the foreign constraint. */
    IF foreign_key_name IS NULL THEN
        foreign_key_name := sql_saga.__internal_make_name(
            ARRAY[fk_table_name] || fk_column_names || ARRAY['fkey']);
    END IF;
    pass := 0;
    WHILE EXISTS (
       SELECT FROM sql_saga.foreign_keys AS fk
       WHERE fk.foreign_key_name = foreign_key_name || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END)
    LOOP
       pass := pass + 1;
    END LOOP;
    foreign_key_name := foreign_key_name || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END;

    -- Create helper function and CHECK constraint
    DECLARE
        fk_column_signatures text;
        fk_column_list text;
        helper_body text;
        existing_proc oid;
    BEGIN
        SELECT
            string_agg(format_type(a.atttypid, a.atttypmod), ', '),
            string_agg(quote_ident(n.name), ', ')
        INTO
            fk_column_signatures,
            fk_column_list
        FROM unnest(fk_column_names) WITH ORDINALITY AS n (name, ordinality)
        LEFT JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (fk_table_oid, n.name);

        -- The helper function is owned by the schema of the referenced (UK) table.
        -- Its name is derived from the UK table, so it can be shared by multiple FKs.
        fk_helper_function := coalesce(fk_helper_function,
            format('%I.%I', uk_schema_name /* %I */, sql_saga.__internal_make_name(
                ARRAY[uk_table_name] || uk_row.column_names || ARRAY['exists']) /* %I */
            )
        );

        helper_signature := format('%s(%s)', fk_helper_function /* %s */, fk_column_signatures /* %s */);

        SELECT string_agg(format('uk.%I = $%s', u.name /* %I */, u.ordinality /* %s */), ' AND ')
        INTO uk_where_clause
        FROM unnest(uk_row.column_names) WITH ORDINALITY AS u(name, ordinality);

        helper_body := format($$
            CREATE FUNCTION %s
            RETURNS boolean
            LANGUAGE sql STABLE STRICT AS $func_body$
                SELECT EXISTS (SELECT 1 FROM %I.%I AS uk WHERE %s);
            $func_body$;
            $$,
            helper_signature, /* %s */
            uk_schema_name, /* %I */
            uk_table_name, /* %I */
            uk_where_clause /* %s */
        );

        -- Check if a function with this signature already exists.
        SELECT p.oid INTO existing_proc
        FROM pg_catalog.pg_proc p
        JOIN pg_catalog.pg_namespace n ON p.pronamespace = n.oid
        WHERE p.proname = split_part(fk_helper_function, '.', 2)
          AND n.nspname = split_part(fk_helper_function, '.', 1)
          AND pg_catalog.pg_get_function_identity_arguments(p.oid) = fk_column_signatures;

        IF existing_proc IS NULL THEN
             EXECUTE helper_body;
        END IF;

        fk_check_constraint := coalesce(fk_check_constraint, sql_saga.__internal_make_name(
            ARRAY[fk_table_name] || fk_column_names || ARRAY['check']
        ));

        DECLARE
            check_clause text;
        BEGIN
            check_clause := format('%s(%s)', fk_helper_function /* %s */, fk_column_list /* %s */);

            EXECUTE format('ALTER TABLE %I.%I ADD CONSTRAINT %I CHECK (%s)',
                fk_schema_name, /* %I */
                fk_table_name, /* %I */
                fk_check_constraint, /* %I */
                check_clause /* %s */
            );
        END;
    END;


    /* Get the columns that require checking the constraint on the UK table */
    SELECT string_agg(quote_ident(u.column_name), ', ' ORDER BY u.ordinality)
    INTO unique_columns_with_era_columns
    FROM unnest(uk_row.column_names || uk_era_row.valid_from_column_name || uk_era_row.valid_until_column_name) WITH ORDINALITY AS u (column_name, ordinality);

    DECLARE
        fk_column_names_arr_str text := fk_column_names::text;
        uk_column_names_arr_str text := uk_row.column_names::text;
    BEGIN
        uk_update_trigger := coalesce(uk_update_trigger, sql_saga.__internal_make_name(ARRAY[foreign_key_name], 'uk_update'));
        EXECUTE format($$
            CREATE CONSTRAINT TRIGGER %1$I AFTER UPDATE OF %2$s ON %3$I.%4$I FROM %5$I.%6$I DEFERRABLE FOR EACH ROW EXECUTE PROCEDURE
            sql_saga.uk_update_check_c(%7$L, %5$L, %6$L, %8$L, %9$L, %10$L, %11$L, %3$L, %4$L, %12$L, %13$L, %14$L, %15$L, %16$L, %17$L, %18$L, 'regular_to_temporal');
            $$
            , /* %1$I */ uk_update_trigger
            , /* %2$s */ unique_columns_with_era_columns
            , /* %3$I */ uk_schema_name
            , /* %4$I */ uk_table_name
            , /* %5$I */ fk_schema_name
            , /* %6$I */ fk_table_name
            -- Parameters for C function
            , /* %7$L */ foreign_key_name
            , /* %8$L */ fk_column_names_arr_str
            , /* %9$L */ '' -- fk_era_name
            , /* %10$L*/ '' -- fk_valid_from_column_name
            , /* %11$L*/ '' -- fk_valid_until_column_name
            , /* %12$L*/ uk_column_names_arr_str
            , /* %13$L*/ uk_era_row.era_name
            , /* %14$L*/ uk_era_row.valid_from_column_name
            , /* %15$L*/ uk_era_row.valid_until_column_name
            , /* %16$L*/ match_type
            , /* %17$L*/ update_action
            , /* %18$L*/ delete_action
        );

        uk_delete_trigger := coalesce(uk_delete_trigger, sql_saga.__internal_make_name(ARRAY[foreign_key_name], 'uk_delete'));
        EXECUTE format($$
            CREATE CONSTRAINT TRIGGER %1$I AFTER DELETE ON %2$I.%3$I FROM %4$I.%5$I DEFERRABLE FOR EACH ROW EXECUTE PROCEDURE
            sql_saga.uk_delete_check_c(%6$L, %4$L, %5$L, %7$L, %8$L, %9$L, %10$L, %2$L, %3$L, %11$L, %12$L, %13$L, %14$L, %15$L, %16$L, %17$L, 'regular_to_temporal');
            $$
            , /* %1$I */ uk_delete_trigger
            , /* %2$I */ uk_schema_name
            , /* %3$I */ uk_table_name
            , /* %4$I */ fk_schema_name
            , /* %5$I */ fk_table_name
            -- Parameters for C function
            , /* %6$L */ foreign_key_name
            , /* %7$L */ fk_column_names_arr_str
            , /* %8$L */ '' -- fk_era_name
            , /* %9$L */ '' -- fk_valid_from_column_name
            , /* %10$L*/ '' -- fk_valid_until_column_name
            , /* %11$L*/ uk_column_names_arr_str
            , /* %12$L*/ uk_era_row.era_name
            , /* %13$L*/ uk_era_row.valid_from_column_name
            , /* %14$L*/ uk_era_row.valid_until_column_name
            , /* %15$L*/ match_type
            , /* %16$L*/ update_action
            , /* %17$L*/ delete_action
        );
    END;

    INSERT INTO sql_saga.foreign_keys
        ( foreign_key_name, type,              table_schema,   table_name,    column_names,    fk_table_columns_snapshot, unique_key_name, match_type, update_action, delete_action, fk_check_constraint, fk_helper_function, uk_update_trigger, uk_delete_trigger, fk_index_name)
    VALUES
        ( foreign_key_name, 'regular_to_temporal', fk_schema_name, fk_table_name, fk_column_names, fk_table_columns_snapshot, unique_key_name, match_type, update_action, delete_action, fk_check_constraint, helper_signature, uk_update_trigger, uk_delete_trigger, v_fk_index_name);


    /* Validate the constraint on existing data. */
    DECLARE
        violating_row_found boolean;
        fk_not_null_clause text;
    BEGIN
        SELECT string_agg(format('fk.%I IS NOT NULL', u.fkc /* %I */), ' AND ')
        INTO fk_not_null_clause
        FROM unnest(fk_column_names) AS u(fkc);

        SELECT string_agg(format('uk.%I = fk.%I', u.ukc /* %I */, u.fkc /* %I */), ' AND ')
        INTO uk_where_clause
        FROM unnest(uk_row.column_names, fk_column_names) AS u(ukc, fkc);

        EXECUTE format('SELECT EXISTS( ' ||
            'SELECT 1 FROM %1$I.%2$I AS fk ' ||
            'WHERE %3$s AND NOT EXISTS ( ' ||
            '  SELECT 1 FROM %4$I.%5$I AS uk WHERE %6$s' ||
            '))',
            fk_schema_name,       -- %1$I
            fk_table_name,        -- %2$I
            fk_not_null_clause,   -- %3$s
            uk_schema_name,       -- %4$I
            uk_table_name,        -- %5$I
            uk_where_clause       -- %6$s
        ) INTO violating_row_found;

        IF violating_row_found THEN
            RAISE EXCEPTION 'insert or update on table "%" violates foreign key constraint "%"',
                fk_table_oid,
                foreign_key_name;
        END IF;
    END;


    RETURN foreign_key_name;
END;
$function_regular_fk$;


-- Original function for temporal-to-temporal FKs
CREATE FUNCTION sql_saga.add_temporal_foreign_key(
        fk_table_oid regclass,
        fk_column_names name[],
        fk_era_name name,
        unique_key_name name,
        match_type sql_saga.fk_match_types DEFAULT 'SIMPLE',
        update_action sql_saga.fk_actions DEFAULT 'NO ACTION',
        delete_action sql_saga.fk_actions DEFAULT 'NO ACTION',
        foreign_key_name name DEFAULT NULL,
        fk_insert_trigger name DEFAULT NULL,
        fk_update_trigger name DEFAULT NULL,
        uk_update_trigger name DEFAULT NULL,
        uk_delete_trigger name DEFAULT NULL,
        create_index boolean DEFAULT true)
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
    fk_table_columns_snapshot name[];
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
    v_fk_index_name name;
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

    SELECT array_agg(a.attname ORDER BY a.attnum) INTO fk_table_columns_snapshot
    FROM pg_catalog.pg_attribute AS a
    WHERE a.attrelid = fk_table_oid AND a.attnum > 0 AND NOT a.attisdropped;

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
    uk_table_oid := format('%I.%I', uk_schema_name /* %I */, uk_table_name /* %I */)::regclass;

    v_fk_index_name := sql_saga.__find_or_create_fk_index(
        fk_table_oid,
        fk_column_names,
        fk_era_row,
        'temporal_to_temporal',
        create_index
    );

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

    -- If a synchronized 'valid_to'-style column is defined for the era, add it
    -- to the list of columns that trigger the fk_update_check. This is only
    -- done if the column is not already part of the core era columns to avoid
    -- duplication. This handles cases where a separate BEFORE trigger
    -- synchronizes this column with valid_until, ensuring validation fires
    -- correctly without making the trigger an overly-broad row-level trigger.
    IF fk_era_row.synchronize_valid_to_column IS NOT NULL
       AND fk_era_row.synchronize_valid_to_column <> fk_era_row.valid_from_column_name
       AND fk_era_row.synchronize_valid_to_column <> fk_era_row.valid_until_column_name
    THEN
        foreign_columns_with_era_columns := foreign_columns_with_era_columns || ', ' || quote_ident(fk_era_row.synchronize_valid_to_column);
    END IF;

    SELECT string_agg(quote_ident(u.column_name), ', ' ORDER BY u.ordinality)
    INTO unique_columns_with_era_columns
    FROM unnest(uk_row.column_names || uk_era_row.valid_from_column_name || uk_era_row.valid_until_column_name) WITH ORDINALITY AS u (column_name, ordinality);

    /* Add all the known variables for the triggers to avoid lookups when executing the triggers. */
    DECLARE
        fk_valid_from_column_name text := fk_era_row.valid_from_column_name;
        fk_valid_until_column_name text := fk_era_row.valid_until_column_name;

        fk_column_names_arr_str text := fk_column_names::text;
        uk_column_names_arr_str text := uk_row.column_names::text;

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
            sql_saga.uk_update_check_c(%1$L,%2$L,%3$L,%4$L,%5$L,%6$L,%7$L,%8$L,%9$L,%10$L,%11$L,%12$L,%13$L,%14$L,%15$L,%16$L, 'temporal_to_temporal');
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
            sql_saga.uk_delete_check_c(%1$L,%2$L,%3$L,%4$L,%5$L,%6$L,%7$L,%8$L,%9$L,%10$L,%11$L,%12$L,%13$L,%14$L,%15$L,%16$L, 'temporal_to_temporal');
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
            ( foreign_key_name, type, table_schema, table_name, column_names, fk_era_name, fk_table_columns_snapshot, unique_key_name, match_type, update_action, delete_action, fk_insert_trigger, fk_update_trigger, uk_update_trigger, uk_delete_trigger, fk_index_name)
        VALUES
            ( foreign_key_name, 'temporal_to_temporal', fk_schema_name, fk_table_name, fk_column_names, fk_era_name, fk_table_columns_snapshot, uk_row.unique_key_name, match_type, update_action, delete_action, fk_insert_trigger, fk_update_trigger, uk_update_trigger, uk_delete_trigger, v_fk_index_name);

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
                    fk_era_row.valid_from_column_name,    -- %7$I
                    fk_era_row.valid_until_column_name,   -- %8$I
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

COMMENT ON FUNCTION sql_saga.add_regular_foreign_key(regclass, name[], name, sql_saga.fk_match_types, sql_saga.fk_actions, sql_saga.fk_actions, name, name, text, name, name, boolean) IS
'Adds a foreign key from a regular (non-temporal) table to a temporal table. It ensures that any referenced key exists at some point in the target''s history.';

COMMENT ON FUNCTION sql_saga.add_temporal_foreign_key(regclass, name[], name, name, sql_saga.fk_match_types, sql_saga.fk_actions, sql_saga.fk_actions, name, name, name, name, name, boolean) IS
'Adds a temporal foreign key from one temporal table to another. It ensures that for any given time slice in the referencing table, a corresponding valid time slice exists in the referenced table.';

COMMENT ON FUNCTION sql_saga.add_foreign_key(regclass, name[], regclass, name[], name, sql_saga.fk_match_types, sql_saga.fk_actions, sql_saga.fk_actions, name, boolean) IS
'Adds a foreign key constraint. This is a declarative wrapper that automatically determines whether to create a temporal or regular foreign key by introspecting the schema, and looks up the internal unique key name.';
