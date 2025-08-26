CREATE FUNCTION sql_saga.update_portion_of()
 RETURNS trigger
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    info record;
    view_schema_name name;
    view_table_name name;
    test boolean;
    generated_columns_sql text;
    generated_columns text[];

    jnew jsonb;
    fromval jsonb;
    toval jsonb;

    jold jsonb;
    bstartval jsonb;
    bendval jsonb;

    identifier_columns name[];

    pre_row jsonb;
    new_row jsonb;
    post_row jsonb;
    pre_assigned boolean;
    post_assigned boolean;

    SERVER_VERSION CONSTANT integer := current_setting('server_version_num')::integer;

    TEST_SQL CONSTANT text :=
        'VALUES (CAST(%2$L AS %1$s) < CAST(%3$L AS %1$s) AND '
        '        CAST(%3$L AS %1$s) < CAST(%4$L AS %1$s))';

    GENERATED_COLUMNS_SQL_PRE_10 CONSTANT text :=
        'SELECT array_agg(a.attname) '
        'FROM pg_catalog.pg_attribute AS a '
        'LEFT JOIN pg_catalog.pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum '
        'WHERE a.attrelid = $1 '
        '  AND a.attnum > 0 '
        '  AND NOT a.attisdropped '
        '  AND (pg_catalog.pg_get_serial_sequence(a.attrelid::regclass::text, a.attname) IS NOT NULL '
        '    OR (a.atthasdef AND pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) LIKE ''nextval(%)'') '
        '    OR EXISTS (SELECT 1 FROM sql_saga.era AS _p '
        '               JOIN pg_catalog.pg_class _c ON _c.relname = _p.table_name '
        '               JOIN pg_catalog.pg_namespace _n ON _n.oid = _c.relnamespace AND _n.nspname = _p.table_schema '
        '               WHERE _c.oid = a.attrelid AND _p.era_name = ''system_time'' '
        '                 AND a.attname IN (_p.valid_from_column_name, _p.valid_until_column_name)))';

    GENERATED_COLUMNS_SQL_PRE_12 CONSTANT text :=
        'SELECT array_agg(a.attname) '
        'FROM pg_catalog.pg_attribute AS a '
        'LEFT JOIN pg_catalog.pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum '
        'WHERE a.attrelid = $1 '
        '  AND a.attnum > 0 '
        '  AND NOT a.attisdropped '
        '  AND (pg_catalog.pg_get_serial_sequence(a.attrelid::regclass::text, a.attname) IS NOT NULL '
        '    OR (a.atthasdef AND pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) LIKE ''nextval(%)'') '
        '    OR a.attidentity <> '''' '
        '    OR EXISTS (SELECT 1 FROM sql_saga.era AS _p '
        '               JOIN pg_catalog.pg_class _c ON _c.relname = _p.table_name '
        '               JOIN pg_catalog.pg_namespace _n ON _n.oid = _c.relnamespace AND _n.nspname = _p.table_schema '
        '               WHERE _c.oid = a.attrelid AND _p.era_name = ''system_time'' '
        '                 AND a.attname IN (_p.valid_from_column_name, _p.valid_until_column_name)))';

    GENERATED_COLUMNS_SQL_CURRENT CONSTANT text :=
        'SELECT array_agg(a.attname) '
        'FROM pg_catalog.pg_attribute AS a '
        'LEFT JOIN pg_catalog.pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum '
        'WHERE a.attrelid = $1 '
        '  AND a.attnum > 0 '
        '  AND NOT a.attisdropped '
        '  AND (pg_catalog.pg_get_serial_sequence(a.attrelid::regclass::text, a.attname) IS NOT NULL '
        '    OR (a.atthasdef AND pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) LIKE ''nextval(%)'') '
        '    OR a.attidentity <> '''' '
        '    OR a.attgenerated <> '''' '
        '    OR EXISTS (SELECT 1 FROM sql_saga.era AS _p '
        '               JOIN pg_catalog.pg_class _c ON _c.relname = _p.table_name '
        '               JOIN pg_catalog.pg_namespace _n ON _n.oid = _c.relnamespace AND _n.nspname = _p.table_schema '
        '               WHERE _c.oid = a.attrelid AND _p.era_name = ''system_time'' '
        '                 AND a.attname IN (_p.valid_from_column_name, _p.valid_until_column_name)))';

BEGIN
    /*
     * REFERENCES:
     *     SQL:2016 15.13 GR 10
     */

    -- The identifier columns are passed as arguments to the trigger.
    IF TG_NARGS = 0 THEN
        RAISE EXCEPTION 'update_portion_of trigger must be created with identifier columns as arguments';
    END IF;
    identifier_columns := TG_ARGV;

    SELECT n.nspname, c.relname
    INTO view_schema_name, view_table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = TG_RELID;

    /* Get the table information from this view */
    SELECT c.oid AS table_oid,
           fpv.table_schema, fpv.table_name, fpv.era_name,
           e.valid_from_column_name, e.valid_until_column_name,
           format_type(a.atttypid, a.atttypmod) AS datatype
    INTO info
    FROM sql_saga.api_view AS fpv
    JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (fpv.table_schema, fpv.table_name, fpv.era_name)
    JOIN pg_catalog.pg_namespace AS n ON n.nspname = fpv.table_schema
    JOIN pg_catalog.pg_class AS c ON (c.relnamespace, c.relname) = (n.oid, fpv.table_name)
    JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (c.oid, e.valid_from_column_name)
    WHERE (fpv.view_schema_name, fpv.view_table_name) = (view_schema_name, view_table_name);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'table and era information not found for view "%"', TG_RELID::regclass;
    END IF;

    jnew := to_jsonb(NEW);
    fromval := jnew->info.valid_from_column_name;
    toval := jnew->info.valid_until_column_name;

    jold := to_jsonb(OLD);
    bstartval := jold->info.valid_from_column_name;
    bendval := jold->info.valid_until_column_name;

    -- If the new period does not overlap with the old period, do nothing.
    EXECUTE format('SELECT NOT (%L::%s >= %L::%s OR %L::%s >= %L::%s)',
        fromval, info.datatype, bendval, info.datatype, bstartval, info.datatype, toval, info.datatype)
    INTO test;
    IF NOT test THEN
        RETURN NULL;
    END IF;

    -- If the new period does not overlap with the old period, do nothing.
    EXECUTE format('SELECT NOT (%L::%s >= %L::%s OR %L::%s >= %L::%s)',
        fromval, info.datatype, bendval, info.datatype, bstartval, info.datatype, toval, info.datatype)
    INTO test;
    IF NOT test THEN
        RETURN NULL;
    END IF;

    -- If the new period does not overlap with the old period, do nothing.
    EXECUTE format('SELECT %L::%s < %L::%s AND %L::%s < %L::%s',
        bstartval, info.datatype, toval, info.datatype, fromval, info.datatype, bendval, info.datatype)
    INTO test;
    IF NOT test THEN
        RETURN NULL;
    END IF;

    pre_row := jold;
    new_row := jnew;
    post_row := jold;

    /* Reset the period columns */
    new_row := jsonb_set(new_row, ARRAY[info.valid_from_column_name], bstartval);
    new_row := jsonb_set(new_row, ARRAY[info.valid_until_column_name], bendval);

    /* If the period is the only thing changed, do nothing */
    IF new_row = jold THEN
        RETURN NULL;
    END IF;

    pre_assigned := false;
    EXECUTE format(TEST_SQL, info.datatype, bstartval, fromval, bendval) INTO test;
    IF test THEN
        pre_assigned := true;
        pre_row := jsonb_set(pre_row, ARRAY[info.valid_until_column_name], fromval);
        new_row := jsonb_set(new_row, ARRAY[info.valid_from_column_name], fromval);
    END IF;

    post_assigned := false;
    EXECUTE format(TEST_SQL, info.datatype, bstartval, toval, bendval) INTO test;
    IF test THEN
        post_assigned := true;
        new_row := jsonb_set(new_row, ARRAY[info.valid_until_column_name], toval::jsonb);
        post_row := jsonb_set(post_row, ARRAY[info.valid_from_column_name], toval::jsonb);
    END IF;

    IF pre_assigned OR post_assigned THEN
        /* Don't validate foreign keys until all this is done */
        SET CONSTRAINTS ALL DEFERRED;

        /*
         * Find and remove all generated columns from pre_row and post_row.
         * SQL:2016 15.13 GR 10)b)i)
         *
         * We also remove columns that own a sequence as those are a form of
         * generated column.  We do not, however, remove columns that default
         * to nextval() without owning the underlying sequence.
         *
         * Columns belonging to a SYSTEM_TIME period are also removed.
         *
         * Note: Primary key columns are not implicitly treated as generated.
         * Columns are considered generated if they are `serial`, `IDENTITY`, a
         * generated expression, part of the `system_time` period, or have a
         * `DEFAULT` expression that calls `nextval()`. Other columns, including
         * non-generated primary keys that often serve as entity identifiers, are
         * preserved. This is crucial for maintaining historical integrity when
         * splitting rows.
         */
        -- Create a cache table for generated columns if it doesn't exist for this session.
        -- Using to_regclass is a clean way to check for a temp table's existence.
        IF to_regclass('__sql_saga_generated_columns_cache') IS NULL THEN
            CREATE TEMP TABLE __sql_saga_generated_columns_cache (
                table_oid oid PRIMARY KEY,
                column_names name[]
            ) ON COMMIT DROP;
        END IF;

        -- Try to fetch from cache first
        SELECT column_names INTO generated_columns FROM __sql_saga_generated_columns_cache WHERE table_oid = info.table_oid;

        IF NOT FOUND THEN
            -- Not in cache, so query catalogs
            IF SERVER_VERSION < 100000 THEN
                generated_columns_sql := GENERATED_COLUMNS_SQL_PRE_10;
            ELSIF SERVER_VERSION < 120000 THEN
                generated_columns_sql := GENERATED_COLUMNS_SQL_PRE_12;
            ELSE
                generated_columns_sql := GENERATED_COLUMNS_SQL_CURRENT;
            END IF;

            EXECUTE generated_columns_sql
            INTO generated_columns
            USING info.table_oid;

            -- Store in cache for subsequent calls in this transaction
            INSERT INTO __sql_saga_generated_columns_cache (table_oid, column_names) VALUES (info.table_oid, generated_columns);
        END IF;

        /* There may not be any generated columns. */
        IF generated_columns IS NOT NULL THEN
            IF SERVER_VERSION < 100000 THEN
                SELECT jsonb_object_agg(e.key, e.value)
                INTO pre_row
                FROM jsonb_each(pre_row) AS e (key, value)
                WHERE e.key <> ALL (generated_columns);

                SELECT jsonb_object_agg(e.key, e.value)
                INTO post_row
                FROM jsonb_each(post_row) AS e (key, value)
                WHERE e.key <> ALL (generated_columns);
            ELSE
                pre_row := pre_row - generated_columns;
                post_row := post_row - generated_columns;
            END IF;
        END IF;

    END IF;

    IF pre_assigned THEN
        EXECUTE format('INSERT INTO %I.%I (%s) VALUES (%s)',
            info.table_schema,
            info.table_name,
            (SELECT string_agg(quote_ident(key), ', ' ORDER BY key) FROM jsonb_each_text(pre_row)),
            (SELECT string_agg(quote_nullable(value), ', ' ORDER BY key) FROM jsonb_each_text(pre_row)));
    END IF;

    EXECUTE format('UPDATE %I.%I SET %s WHERE %s AND %I = %s AND %I = %s',
                   info.table_schema,
                   info.table_name,
                   (SELECT string_agg(format('%I = %L', j.key, j.value), ', ')
                    FROM (SELECT key, value FROM jsonb_each_text(new_row)
                          EXCEPT ALL
                          SELECT key, value FROM jsonb_each_text(jold)
                         ) AS j
                   ),
                   (SELECT string_agg(format('%I = %L', j.key, j.value), ' AND ')
                    FROM jsonb_each_text(jold) j
                    WHERE j.key = ANY(identifier_columns)
                   ),
                   info.valid_from_column_name,
                   quote_literal(bstartval::text),
                   info.valid_until_column_name,
                   quote_literal(bendval::text)
                  );

    IF post_assigned THEN
        EXECUTE format('INSERT INTO %I.%I (%s) VALUES (%s)',
            info.table_schema,
            info.table_name,
            (SELECT string_agg(quote_ident(key), ', ' ORDER BY key) FROM jsonb_each_text(post_row)),
            (SELECT string_agg(quote_nullable(value), ', ' ORDER BY key) FROM jsonb_each_text(post_row)));
    END IF;

    IF pre_assigned OR post_assigned THEN
        SET CONSTRAINTS ALL IMMEDIATE;
    END IF;

    RETURN NEW;
END;
$function$;
