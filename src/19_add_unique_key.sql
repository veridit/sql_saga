CREATE FUNCTION sql_saga.__internal_generate_pk_consistency_constraints(
    p_unique_key_name name,
    p_natural_key_columns name[],
    p_mutually_exclusive_columns name[],
    p_primary_key_column name
)
RETURNS jsonb -- { "cmds": text[], "names": name[] }
LANGUAGE plpgsql AS
$function$
#variable_conflict use_variable
DECLARE
    v_constraint_cmds text[] := '{}';
    v_constraint_names name[] := '{}';
    v_constraint_name name;
    v_constraint_sql text;
    v_withs text[];
BEGIN
    IF p_mutually_exclusive_columns IS NULL THEN
        v_constraint_name := p_unique_key_name || '_pk_consistency_excl';
        SELECT array_agg(format('%I WITH =', col)) INTO v_withs FROM unnest(p_natural_key_columns) AS col;
        v_withs := v_withs || format('%I WITH <>', p_primary_key_column);
        v_constraint_sql := format('ADD CONSTRAINT %I EXCLUDE USING gist (%s)', v_constraint_name, array_to_string(v_withs, ', '));
        v_constraint_cmds := array_append(v_constraint_cmds, v_constraint_sql);
        v_constraint_names := array_append(v_constraint_names, v_constraint_name);
    ELSE
        DECLARE
            non_xor_cols name[];
            xor_col name;
            where_clause_partial text;
        BEGIN
            non_xor_cols := ARRAY(SELECT c FROM unnest(p_natural_key_columns) AS c WHERE c <> ALL(p_mutually_exclusive_columns));
            FOREACH xor_col IN ARRAY p_mutually_exclusive_columns LOOP
                v_constraint_name := sql_saga.__internal_make_name(ARRAY[p_unique_key_name, xor_col, 'pk_consistency_excl']);
                SELECT array_agg(format('%I WITH =', col)) INTO v_withs FROM unnest(non_xor_cols || xor_col) AS col;
                v_withs := v_withs || format('%I WITH <>', p_primary_key_column);
                where_clause_partial := format('WHERE (%I IS NOT NULL AND (%s))', xor_col, (SELECT string_agg(format('%I IS NULL', other_col), ' AND ') FROM unnest(p_mutually_exclusive_columns) AS other_col WHERE other_col <> xor_col));
                v_constraint_sql := format('ADD CONSTRAINT %I EXCLUDE USING gist (%s) %s', v_constraint_name, array_to_string(v_withs, ', '), where_clause_partial);
                v_constraint_cmds := array_append(v_constraint_cmds, v_constraint_sql);
                v_constraint_names := array_append(v_constraint_names, v_constraint_name);
            END LOOP;
        END;
    END IF;

    RETURN jsonb_build_object('cmds', v_constraint_cmds, 'names', v_constraint_names);
END;
$function$;

CREATE FUNCTION sql_saga.add_unique_key(
        table_oid regclass,
        column_names name[],
        era_name name DEFAULT 'valid',
        key_type sql_saga.unique_key_type DEFAULT 'natural',
        enforce_consistency_with_primary_key boolean DEFAULT NULL,
        unique_key_name name DEFAULT NULL,
        unique_constraint name DEFAULT NULL,
        exclude_constraint name DEFAULT NULL,
        predicate text DEFAULT NULL,
        mutually_exclusive_columns name[] DEFAULT NULL)
 RETURNS name
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    table_schema name;
    table_name name;
    era_row sql_saga.era;
    column_attnums smallint[];
    era_attnums smallint[];
    idx integer;
    constraint_record record;
    pass integer;
    sql text;
    alter_cmds text[];
    v_unique_constraint_found boolean := false;
    v_exclude_constraint_found boolean := false;
    v_check_constraint name;
    unique_sql text;
    exclude_sql text;
    where_clause text;
    partial_index_names name[];
    partial_exclude_constraint_names name[];
    v_pk_consistency_constraint_names name[];
    v_enforce_consistency_original boolean := enforce_consistency_with_primary_key;
BEGIN
    -- If the parameter is NULL (the default), set it to true for natural keys and false otherwise.
    IF enforce_consistency_with_primary_key IS NULL THEN
        enforce_consistency_with_primary_key := (key_type = 'natural');
    END IF;

    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    SELECT n.nspname, c.relname
    INTO table_schema, table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = table_oid;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    SELECT p.*
    INTO era_row
    FROM sql_saga.era AS p
    WHERE (p.table_schema, p.table_name, p.era_name) = (table_schema, table_name, era_name);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'era "%" does not exist', era_name;
    END IF;

    /* For convenience, put the period's attnums in an array */
    era_attnums := ARRAY[
        (SELECT a.attnum FROM pg_catalog.pg_attribute AS a WHERE (a.attrelid, a.attname) = (table_oid, era_row.valid_from_column_name)),
        (SELECT a.attnum FROM pg_catalog.pg_attribute AS a WHERE (a.attrelid, a.attname) = (table_oid, era_row.valid_until_column_name))
    ];

    /* Get attnums from column names */
    SELECT array_agg(a.attnum ORDER BY n.ordinality)
    INTO column_attnums
    FROM unnest(column_names) WITH ORDINALITY AS n (name, ordinality)
    LEFT JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (table_oid, n.name);

    /* System columns are not allowed */
    IF 0 > ANY (column_attnums) THEN
        RAISE EXCEPTION 'index creation on system columns is not supported';
    END IF;

    /* Report if any columns weren't found */
    idx := array_position(column_attnums, NULL);
    IF idx IS NOT NULL THEN
        RAISE EXCEPTION 'column "%" does not exist', column_names[idx];
    END IF;

    /* Make sure the period columns aren't also in the normal columns */
    IF era_row.valid_from_column_name = ANY (column_names) THEN
        RAISE EXCEPTION 'column "%" specified twice', era_row.valid_from_column_name;
    END IF;
    IF era_row.valid_until_column_name = ANY (column_names) THEN
        RAISE EXCEPTION 'column "%" specified twice', era_row.valid_until_column_name;
    END IF;

    /*
     * Columns belonging to a SYSTEM_TIME period are not allowed in a UNIQUE
     * key. SQL:2016 11.7 SR 5)b)
     */
    IF EXISTS (
        SELECT FROM sql_saga.era AS e
        WHERE (e.table_schema, e.table_name, e.era_name) = (era_row.table_schema, era_row.table_name, 'system_time')
          AND ARRAY[e.valid_from_column_name, e.valid_until_column_name] && column_names)
    THEN
        RAISE EXCEPTION 'columns in era for SYSTEM_TIME are not allowed in UNIQUE keys';
    END IF;

    -- Validate parameter combinations
    IF key_type = 'predicated' AND predicate IS NULL THEN
        RAISE EXCEPTION 'a predicate must be provided when key_type is ''predicated''';
    END IF;

    IF key_type IN ('primary', 'natural') AND predicate IS NOT NULL THEN
        RAISE EXCEPTION 'a predicate can only be provided when key_type is ''predicated''';
    END IF;

    IF mutually_exclusive_columns IS NOT NULL THEN
        IF NOT column_names @> mutually_exclusive_columns THEN
            RAISE EXCEPTION 'all mutually_exclusive_columns must be present in column_names';
        END IF;
        IF key_type <> 'natural' THEN
            RAISE EXCEPTION 'mutually_exclusive_columns can only be used with key_type = ''natural''';
        END IF;
    END IF;

    -- This check is now only relevant if the user explicitly provides the parameter.
    IF enforce_consistency_with_primary_key AND key_type <> 'natural' THEN
        RAISE EXCEPTION 'enforce_consistency_with_primary_key can only be used with key_type = ''natural''';
    END IF;

    where_clause := CASE WHEN predicate IS NOT NULL THEN format(' WHERE (%s)', predicate /* %s */) ELSE '' END;

    IF key_type IN ('primary', 'natural') THEN
        -- When creating a primary key, validate that the table schema is compatible with SCD Type 2 history.
        IF key_type = 'primary' THEN
            DECLARE
                identity_column_name name;
            BEGIN
                -- Check for a GENERATED ALWAYS identity column first, as it's a more specific error.
                SELECT a.attname
                INTO identity_column_name
                FROM pg_catalog.pg_attribute a
                WHERE a.attrelid = table_oid
                AND a.attidentity = 'a'; -- 'a' = always, 'd' = by default

                IF identity_column_name IS NOT NULL THEN
                    RAISE EXCEPTION 'table "%" has a GENERATED ALWAYS AS IDENTITY column ("%"); this is incompatible with SCD Type 2 history as it prevents inserting new historical versions of an entity', table_oid, identity_column_name;
                END IF;

                -- Check for a simple primary key that does not include the temporal columns.
                DECLARE
                    pk_columns name[];
                BEGIN
                    SELECT
                        array_agg(a.attname ORDER BY u.ord)
                    INTO
                        pk_columns
                    FROM
                        pg_catalog.pg_constraint c
                        JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS u(attnum, ord) ON TRUE
                        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = u.attnum
                    WHERE
                        c.conrelid = table_oid AND c.contype = 'p';

                    IF pk_columns IS NOT NULL AND NOT (pk_columns @> ARRAY[era_row.valid_from_column_name] OR pk_columns @> ARRAY[era_row.valid_until_column_name]) THEN
                        RAISE EXCEPTION 'table "%" has a simple PRIMARY KEY that does not include the temporal columns; this is incompatible with SCD Type 2 history', table_oid;
                    END IF;
                END;
            END;
        END IF;
        /* If we were given a unique constraint to use, look it up and make sure it matches */
        -- For a primary key, the constraint must include the user columns and at least one of the era columns.
        -- For a natural key, it must include both era columns.
        SELECT format('%s (%s) DEFERRABLE',
            CASE WHEN key_type = 'primary' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END, /* %s */
            string_agg(quote_ident(u.column_name), ', ' ORDER BY u.ordinality) /* %s */
        )
        INTO unique_sql
        FROM unnest(
            CASE WHEN key_type = 'primary' THEN
                column_names || era_row.valid_from_column_name
            ELSE
                column_names || era_row.valid_from_column_name || era_row.valid_until_column_name
            END
        ) WITH ORDINALITY AS u (column_name, ordinality);

        IF unique_constraint IS NULL AND key_type = 'primary' THEN
            -- If this is a primary key and no name was given, check if one already exists.
            SELECT c.conname INTO unique_constraint
            FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.contype) = (table_oid, 'p');
        END IF;

        IF unique_constraint IS NOT NULL THEN
            SELECT c.oid, c.contype, c.condeferrable, c.condeferred, c.conkey
            INTO constraint_record
            FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.conname) = (table_oid, unique_constraint);

            IF FOUND THEN
                v_unique_constraint_found := true;

                IF key_type = 'primary' AND constraint_record.contype <> 'p' THEN
                    RAISE EXCEPTION 'constraint "%" is not a PRIMARY KEY', unique_constraint;
                ELSIF key_type = 'natural' AND constraint_record.contype NOT IN ('p', 'u') THEN
                    RAISE EXCEPTION 'constraint "%" is not a PRIMARY KEY or UNIQUE KEY', unique_constraint;
                END IF;

                IF NOT constraint_record.condeferrable THEN
                    /* For restore purposes, constraints may be deferred,
                     * but everything must be valid at the end fo the transaction
                     */
                    RAISE EXCEPTION 'constraint "%" must be DEFERRABLE', unique_constraint;
                END IF;

                IF constraint_record.condeferred THEN
                    /* By default constraints are NOT deferred,
                     * and the user receives a timely validation error.
                     */
                    RAISE EXCEPTION 'constraint "%" must be INITIALLY IMMEDIATE', unique_constraint;
                END IF;

                -- For a primary key, we only require one of the era columns to be present.
                -- For a natural key, both must be present.
                IF key_type = 'primary' THEN
                    -- Check that the constraint includes the user columns and at least one era column.
                    -- The order does not matter here, just the set of columns.
                    IF NOT (ARRAY(SELECT unnest(constraint_record.conkey) ORDER BY 1) @> ARRAY(SELECT unnest(column_attnums) ORDER BY 1)
                        AND (constraint_record.conkey @> ARRAY[era_attnums[1]] OR constraint_record.conkey @> ARRAY[era_attnums[2]]))
                    THEN
                        RAISE EXCEPTION 'PRIMARY KEY constraint "%" must include the columns (%) and at least one of the temporal columns ("%" or "%")',
                            unique_constraint,
                            array_to_string(column_names, ', '),
                            era_row.valid_from_column_name,
                            era_row.valid_until_column_name;
                    END IF;
                ELSE -- For natural keys, the column order and set must be exact.
                    IF NOT constraint_record.conkey = column_attnums || era_attnums THEN
                        DECLARE
                            v_expected_columns name[];
                            v_actual_columns name[];
                        BEGIN
                            SELECT array_agg(a.attname ORDER BY u.ord)
                            INTO v_expected_columns
                            FROM unnest(column_attnums || era_attnums) WITH ORDINALITY AS u(attnum, ord)
                            JOIN pg_catalog.pg_attribute AS a ON a.attrelid = table_oid AND a.attnum = u.attnum;

                            SELECT array_agg(a.attname ORDER BY u.ord)
                            INTO v_actual_columns
                            FROM unnest(constraint_record.conkey) WITH ORDINALITY AS u(attnum, ord)
                            JOIN pg_catalog.pg_attribute AS a ON a.attrelid = table_oid AND a.attnum = u.attnum;

                            RAISE EXCEPTION 'constraint "%" does not match. Expected columns (%), but found columns (%).',
                                unique_constraint,
                                array_to_string(v_expected_columns, ', '),
                                array_to_string(v_actual_columns, ', ');
                        END;
                    END IF;
                END IF;

                /* Looks good, let's use it. */
            END IF;
        END IF;
    ELSIF key_type = 'predicated' THEN
        -- When a predicate is provided, we use a unique index, not a unique constraint.
        -- It's not supported to use an existing constraint/index in this case.
        IF unique_constraint IS NOT NULL THEN
            RAISE EXCEPTION 'cannot specify an existing unique constraint when a predicate is provided';
        END IF;
    END IF;

    /*
     * If we were given an exclude constraint to use, look it up and make sure
     * it matches.  We do that by generating the text that we expect
     * pg_get_constraintdef() to output and compare against that instead of
     * trying to deal with the internally stored components like we did for the
     * UNIQUE constraint.
     *
     * We will use this same text to create the constraint if it doesn't exist.
     */
    DECLARE
        withs text[];
    BEGIN
        SELECT array_agg(format('%I WITH =', column_name /* %I */) ORDER BY n.ordinality)
        INTO withs
        FROM unnest(column_names) WITH ORDINALITY AS n (column_name, ordinality);

        withs := withs || format('%I(%I, %I) WITH &&',
            era_row.range_type, /* %I */
            era_row.valid_from_column_name, /* %I */
            era_row.valid_until_column_name /* %I */
        );

        exclude_sql := format('EXCLUDE USING gist (%s)%s DEFERRABLE', array_to_string(withs, ', ') /* %s */, where_clause /* %s */);
    END;

    IF exclude_constraint IS NOT NULL THEN
        SELECT c.oid, c.contype, c.condeferrable, c.condeferred, pg_catalog.pg_get_constraintdef(c.oid) AS definition
        INTO constraint_record
        FROM pg_catalog.pg_constraint AS c
        WHERE (c.conrelid, c.conname) = (table_oid, exclude_constraint);

        IF FOUND THEN
            v_exclude_constraint_found := true;

            IF constraint_record.contype <> 'x' THEN
                RAISE EXCEPTION 'constraint "%" is not an EXCLUDE constraint', exclude_constraint;
            END IF;

            IF NOT constraint_record.condeferrable THEN
                /* For restore purposes, constraints may be deferred,
                 * but everything must be valid at the end fo the transaction
                 */
                RAISE EXCEPTION 'constraint "%" must be DEFERRABLE', exclude_constraint;
            END IF;

            IF constraint_record.condeferred THEN
                /* By default constraints are NOT deferred,
                 * and the user receives a timely validation error.
                 */
                RAISE EXCEPTION 'constraint "%" must be INITIALLY IMMEDIATE', exclude_constraint;
            END IF;

            IF constraint_record.definition <> exclude_sql THEN
                RAISE EXCEPTION 'constraint "%" does not match. Expected definition: "%", but found definition: "%".',
                    exclude_constraint,
                    exclude_sql,
                    constraint_record.definition;
            END IF;

            /* Looks good, let's use it. */
        END IF;
    END IF;

    /*
     * Generate a name for the unique constraint.  We don't have to worry about
     * concurrency here because all period ddl commands lock the periods table.
     */
    IF unique_key_name IS NULL THEN
        unique_key_name := sql_saga.__internal_make_name(
            ARRAY[(SELECT c.relname FROM pg_catalog.pg_class AS c WHERE c.oid = table_oid)]
                || column_names
                || ARRAY[era_name]);
    END IF;
    pass := 0;
    WHILE EXISTS (
       SELECT FROM sql_saga.unique_keys AS uk
       WHERE uk.unique_key_name = unique_key_name || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END)
    LOOP
       pass := pass + 1;
    END LOOP;
    unique_key_name := unique_key_name || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END;

    /* Time to make the underlying constraints */
    IF key_type = 'primary' THEN
        SELECT array_agg(format('ALTER COLUMN %I SET NOT NULL', u.column_name))
        INTO alter_cmds
        FROM unnest(column_names) WITH ORDINALITY AS u (column_name, ordinality);
    ELSE
        alter_cmds := '{}';
    END IF;

    IF key_type IN ('primary', 'natural') THEN
        -- For XOR keys, we don't create a single overarching UNIQUE constraint.
        -- Uniqueness is handled by the partial indexes.
        IF mutually_exclusive_columns IS NULL THEN
            IF NOT v_unique_constraint_found THEN
                IF unique_constraint IS NULL THEN
                    -- If this is a primary key, Postgres will name it automatically.
                    -- For natural keys, we'll generate a name for consistency.
                    IF key_type = 'natural' THEN
                        unique_constraint := unique_key_name || '_uniq';
                        alter_cmds := alter_cmds || ('ADD CONSTRAINT ' || quote_ident(unique_constraint) || ' ' || unique_sql);
                    ELSE
                        alter_cmds := alter_cmds || ('ADD ' || unique_sql);
                    END IF;
                ELSE
                     alter_cmds := alter_cmds || ('ADD CONSTRAINT ' || quote_ident(unique_constraint) || ' ' || unique_sql);
                END IF;
            END IF;
        END IF;
    ELSIF key_type = 'predicated' THEN
        -- For predicates, we create a unique index instead of a unique constraint.
        -- We generate a name for it and store it in the `unique_constraint` variable for metadata.
        IF unique_constraint IS NULL THEN
            unique_constraint := unique_key_name || '_idx';
        END IF;
        unique_sql := format('CREATE UNIQUE INDEX %I ON %I.%I (%s)%s',
            unique_constraint, /* %I */
            table_schema, /* %I */
            table_name, /* %I */
            (SELECT string_agg(quote_ident(c), ', ') FROM unnest(column_names || era_row.valid_from_column_name || era_row.valid_until_column_name) AS u(c)), /* %s */
            where_clause /* %s */
        );
        EXECUTE unique_sql;
    END IF;

    -- For XOR keys, we create partial exclusion constraints instead of one broad one.
    IF mutually_exclusive_columns IS NULL THEN
        IF NOT v_exclude_constraint_found THEN
            IF exclude_constraint IS NULL THEN
                exclude_constraint := unique_key_name || '_excl';
            END IF;
            alter_cmds := alter_cmds || ('ADD CONSTRAINT ' || quote_ident(exclude_constraint) || ' ' || exclude_sql);
        END IF;
    ELSE -- mutually_exclusive_columns IS NOT NULL
        DECLARE
            check_constraint_name name;
            check_sql text;
        BEGIN
            -- Generate and add the CHECK constraint for XOR logic
            check_constraint_name := unique_key_name || '_xor_check';
            -- For XOR keys, we store the check constraint's name in the `check_constraint` column for metadata purposes.
            v_check_constraint := check_constraint_name;
            unique_constraint := NULL; -- No single unique constraint for XOR keys.
            check_sql := format(
                'ADD CONSTRAINT %I CHECK ((%s) = 1)',
                check_constraint_name,
                (SELECT string_agg(format('(CASE WHEN %I IS NOT NULL THEN 1 ELSE 0 END)', col), ' + ') FROM unnest(mutually_exclusive_columns) as col)
            );
            alter_cmds := alter_cmds || check_sql;

            -- For each mutually exclusive case, create two things:
            -- 1. A partial B-Tree index for fast lookups (performance).
            -- 2. A partial EXCLUDE constraint for temporal uniqueness (correctness).
            DECLARE
                non_xor_cols name[];
                xor_col name;
            BEGIN
                non_xor_cols := ARRAY(SELECT c FROM unnest(column_names) AS c WHERE c <> ALL(mutually_exclusive_columns));
                FOREACH xor_col IN ARRAY mutually_exclusive_columns
                LOOP
                    DECLARE
                        partial_constraint_name name;
                        partial_index_name name;
                        withs text[];
                        where_clause_partial text;
                        partial_exclude_sql text;
                        partial_index_sql text;
                        where_clause_for_index text;
                    BEGIN
                        -- 1. Create the performance B-Tree index. This is NOT unique; uniqueness is handled by the EXCLUDE constraint.
                        partial_index_name := unique_key_name || '_' || xor_col || '_idx';
                        where_clause_for_index := format('WHERE %I IS NOT NULL AND (%s)',
                            xor_col,
                            (SELECT string_agg(format('%I IS NULL', other_col), ' AND ') FROM unnest(mutually_exclusive_columns) AS other_col WHERE other_col <> xor_col)
                        );
                        partial_index_sql := format(
                            'CREATE INDEX %I ON %I.%I (%s) %s',
                            partial_index_name,
                            table_schema,
                            table_name,
                            (SELECT string_agg(quote_ident(c), ', ') FROM unnest(non_xor_cols || xor_col) AS u(c)),
                            where_clause_for_index
                        );
                        RAISE NOTICE 'sql_saga: creating partial performance index: %', partial_index_sql;
                        EXECUTE partial_index_sql;
                        partial_index_names := partial_index_names || partial_index_name;

                        -- 2. Create the correctness EXCLUDE constraint.
                        partial_constraint_name := unique_key_name || '_' || xor_col || '_excl';
                        SELECT array_agg(format('%I WITH =', column_name))
                        INTO withs
                        FROM unnest(non_xor_cols || xor_col) AS column_name;

                        withs := withs || format('%I(%I, %I) WITH &&',
                            era_row.range_type,
                            era_row.valid_from_column_name,
                            era_row.valid_until_column_name
                        );

                        -- The WHERE clause for a partial constraint must be enclosed in parentheses.
                        where_clause_partial := format('WHERE (%I IS NOT NULL AND (%s))',
                            xor_col,
                            (SELECT string_agg(format('%I IS NULL', other_col), ' AND ') FROM unnest(mutually_exclusive_columns) AS other_col WHERE other_col <> xor_col)
                        );

                        partial_exclude_sql := format('ADD CONSTRAINT %I EXCLUDE USING gist (%s) %s DEFERRABLE',
                            partial_constraint_name,
                            array_to_string(withs, ', '),
                            where_clause_partial
                        );
                        alter_cmds := alter_cmds || partial_exclude_sql;
                        partial_exclude_constraint_names := partial_exclude_constraint_names || partial_constraint_name;
                    END;
                END LOOP;
            END;
            -- The single, broad exclude_constraint is not used for XOR keys.
            exclude_constraint := NULL;
        END;
    END IF;

    -- When adding a natural key, determine the state of PK consistency enforcement.
    IF key_type = 'natural' THEN
        IF enforce_consistency_with_primary_key THEN
            DECLARE
                pk_column_names name[];
                stable_pk_columns name[];
                pk_is_compatible boolean := true;
            BEGIN
                -- A PK exists, so we can enforce immediately.
                SELECT uk.column_names INTO pk_column_names FROM sql_saga.unique_keys uk WHERE (uk.table_schema, uk.table_name) = (table_schema, table_name) AND uk.key_type = 'primary'::sql_saga.unique_key_type;
                IF NOT FOUND THEN
                    SELECT array_agg(a.attname ORDER BY u.ord) INTO pk_column_names FROM pg_catalog.pg_constraint c JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS u(attnum, ord) ON TRUE JOIN pg_catalog.pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = u.attnum WHERE c.conrelid = table_oid AND c.contype = 'p';
                END IF;

                IF pk_column_names IS NOT NULL THEN
                    -- Subtract temporal columns to find the stable part of the PK.
                    SELECT array_agg(c) INTO stable_pk_columns
                    FROM unnest(pk_column_names) as c
                    WHERE c NOT IN (era_row.valid_from_column_name, era_row.valid_until_column_name);

                    IF stable_pk_columns IS NULL OR array_length(stable_pk_columns, 1) = 0 THEN
                        IF v_enforce_consistency_original = true THEN -- Error only if explicitly requested
                            RAISE EXCEPTION 'cannot enforce primary key consistency on table "%" because its PRIMARY KEY (%) does not contain a stable (non-temporal) column', table_oid, array_to_string(pk_column_names, ', ');
                        ELSE -- Silently opt-out if using default (NULL) or explicit false
                            pk_is_compatible := false;
                        END IF;
                    END IF;

                    IF pk_is_compatible AND array_length(stable_pk_columns, 1) > 1 THEN
                        IF v_enforce_consistency_original = true THEN -- Error only if explicitly requested
                            RAISE EXCEPTION 'cannot enforce primary key consistency for natural key (%) on table "%" because its stable PRIMARY KEY component (%) is composite', array_to_string(column_names, ', '), table_oid, array_to_string(stable_pk_columns, ', ');
                        ELSE -- Silently opt-out if using default (NULL) or explicit false
                            pk_is_compatible := false;
                        END IF;
                    END IF;

                    IF pk_is_compatible THEN
                        -- If we get here, we have a single stable PK column.
                        DECLARE
                            pk_column_name name := stable_pk_columns[1];
                            consistency_result jsonb;
                        BEGIN
                            consistency_result := sql_saga.__internal_generate_pk_consistency_constraints(
                                p_unique_key_name => unique_key_name,
                                p_natural_key_columns => column_names,
                                p_mutually_exclusive_columns => mutually_exclusive_columns,
                                p_primary_key_column => pk_column_name
                            );
                            alter_cmds := alter_cmds || (SELECT array_agg(value) FROM jsonb_array_elements_text(consistency_result->'cmds'));
                            v_pk_consistency_constraint_names := (SELECT array_agg(value::name) FROM jsonb_array_elements_text(consistency_result->'names'));
                        END;
                    ELSE
                        -- Incompatible PK, silently opt-out.
                        v_pk_consistency_constraint_names := NULL;
                    END IF;
                ELSE
                    -- No PK exists yet. Set state to "pending" (empty array).
                    v_pk_consistency_constraint_names := '{}';
                END IF;
            END;
        ELSE
            -- User opted out. Set state to "opt-out" (NULL).
            v_pk_consistency_constraint_names := NULL;
        END IF;
    END IF;

    IF alter_cmds <> '{}' THEN
        RAISE NOTICE 'sql_saga: altering table %.% to add constraints: %', table_schema, table_name, array_to_string(alter_cmds, ', ');
        SELECT format('ALTER TABLE %I.%I %s', n.nspname /* %I */, c.relname /* %I */, array_to_string(alter_cmds, ', ') /* %s */)
        INTO sql
        FROM pg_catalog.pg_class AS c
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
        WHERE c.oid = table_oid;

        EXECUTE sql;
    END IF;

    /* If we don't already have a unique_constraint, it must be the one with the highest oid */
    IF unique_constraint IS NULL THEN
        SELECT c.conname
        INTO unique_constraint
        FROM pg_catalog.pg_constraint AS c
        WHERE c.conrelid = table_oid AND c.contype = (CASE WHEN key_type = 'primary' THEN 'p' ELSE 'u' END)
        ORDER BY oid DESC
        LIMIT 1;
    END IF;

    RAISE DEBUG 'add_unique_key: Inserting into sql_saga.unique_keys: name=%, table=%.%, key_type=%, cols=%, era=%, u_constraint=%, ex_constraint=%, check_constraint=%, predicate=%, mut_ex_cols=%, partial_indices=%, partial_constraints=%, pk_consistency_constraints=%',
        unique_key_name, table_schema, table_name, key_type, column_names, era_name, unique_constraint, exclude_constraint, v_check_constraint, predicate, mutually_exclusive_columns, partial_index_names, partial_exclude_constraint_names, v_pk_consistency_constraint_names;
    INSERT INTO sql_saga.unique_keys (unique_key_name, table_schema, table_name, key_type, column_names, era_name, unique_constraint, exclude_constraint, check_constraint, predicate, mutually_exclusive_columns, partial_index_names, partial_exclude_constraint_names, pk_consistency_constraint_names)
    VALUES (unique_key_name, table_schema, table_name, key_type, column_names, era_name, unique_constraint, exclude_constraint, v_check_constraint, predicate, mutually_exclusive_columns, partial_index_names, partial_exclude_constraint_names, v_pk_consistency_constraint_names);

    -- When adding a primary key, scan for any natural keys in a "pending" state.
    IF key_type = 'primary' THEN
        DECLARE
            nk_row sql_saga.unique_keys;
            pk_column_name name;
            consistency_cmds text[];
        BEGIN
            DECLARE
                stable_pk_columns name[];
            BEGIN
                -- Subtract temporal columns from the new PK to find its stable part.
                SELECT array_agg(c) INTO stable_pk_columns
                FROM unnest(column_names) as c
                WHERE c NOT IN (era_row.valid_from_column_name, era_row.valid_until_column_name);

                IF stable_pk_columns IS NULL OR array_length(stable_pk_columns, 1) = 0 THEN
                    IF EXISTS (SELECT 1 FROM sql_saga.unique_keys uk WHERE (uk.table_schema, uk.table_name) = (table_schema, table_name) AND uk.key_type = 'natural' AND uk.pk_consistency_constraint_names = '{}') THEN
                        RAISE EXCEPTION 'cannot add primary key (%) to table "%" because it has no stable (non-temporal) part, which is required by existing natural keys pending consistency enforcement', array_to_string(column_names, ', '), table_oid;
                    END IF;
                ELSIF array_length(stable_pk_columns, 1) > 1 THEN
                    IF EXISTS (SELECT 1 FROM sql_saga.unique_keys uk WHERE (uk.table_schema, uk.table_name) = (table_schema, table_name) AND uk.key_type = 'natural' AND uk.pk_consistency_constraint_names = '{}') THEN
                        RAISE EXCEPTION 'cannot add primary key (%) to table "%" because its stable part (%) is composite, which is not supported for consistency enforcement required by existing natural keys', array_to_string(column_names, ', '), table_oid, array_to_string(stable_pk_columns, ', ');
                    END IF;
                ELSE
                    -- This is a compatible, single-column stable PK. Apply to pending NKs.
                    pk_column_name := stable_pk_columns[1];
                    FOR nk_row IN
                        SELECT * FROM sql_saga.unique_keys uk
                        WHERE (uk.table_schema, uk.table_name) = (table_schema, table_name)
                        AND uk.key_type = 'natural' AND uk.pk_consistency_constraint_names = '{}' -- Find pending keys
                    LOOP
                        DECLARE
                            consistency_result jsonb;
                            newly_created_constraint_names name[];
                        BEGIN
                             consistency_result := sql_saga.__internal_generate_pk_consistency_constraints(
                                p_unique_key_name => nk_row.unique_key_name,
                                p_natural_key_columns => nk_row.column_names,
                                p_mutually_exclusive_columns => nk_row.mutually_exclusive_columns,
                                p_primary_key_column => pk_column_name
                            );
                            consistency_cmds := consistency_cmds || (SELECT array_agg(value) FROM jsonb_array_elements_text(consistency_result->'cmds'));
                            newly_created_constraint_names := (SELECT array_agg(value::name) FROM jsonb_array_elements_text(consistency_result->'names'));
                            UPDATE sql_saga.unique_keys SET pk_consistency_constraint_names = newly_created_constraint_names WHERE unique_key_name = nk_row.unique_key_name;
                        END;
                    END LOOP;

                    IF consistency_cmds IS NOT NULL AND array_length(consistency_cmds, 1) > 0 THEN
                        RAISE NOTICE 'sql_saga: retroactively applying pk consistency for table %.%: %', table_schema, table_name, array_to_string(consistency_cmds, ', ');
                        EXECUTE format('ALTER TABLE %I.%I %s', table_schema, table_name, array_to_string(consistency_cmds, ', '));
                    END IF;
                END IF;
            END;
        END;
    END IF;

    -- Create a standard B-Tree index on the unique key columns to support
    -- fast lookups for foreign key checks (both temporal and regular).
    -- This is only created if a suitable index doesn't already exist.
    DECLARE
        index_name name;
        index_sql text;
        existing_index_oid oid;
    BEGIN
        -- Find an existing B-Tree index on exactly these columns in this order
        SELECT i.indexrelid INTO existing_index_oid
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
        JOIN pg_catalog.pg_am am ON am.oid = c.relam
        WHERE i.indrelid = table_oid
          AND i.indisvalid
          AND i.indnkeyatts = array_length(column_names, 1)
          AND i.indkey::text = array_to_string(column_attnums, ' ')
          AND am.amname = 'btree'
        LIMIT 1;

        IF existing_index_oid IS NULL THEN
            index_name := sql_saga.__internal_make_name(
                ARRAY[table_name] || column_names || ARRAY['idx']
            );
            index_sql := format('CREATE INDEX %I ON %I.%I USING btree (%s)',
                index_name, /* %I */
                table_schema, /* %I */
                table_name, /* %I */
                (SELECT string_agg(quote_ident(c), ', ') FROM unnest(column_names) AS u(c)) /* %s */
            );
            EXECUTE index_sql;
        END IF;
    END;

    RETURN unique_key_name;
END;
$function$;

COMMENT ON FUNCTION sql_saga.add_unique_key(regclass, name[], name, sql_saga.unique_key_type, boolean, name, name, name, text, name[]) IS
'Adds a temporal unique key to a table, ensuring uniqueness across time for a given set of columns within an era. Supports primary, natural, and predicated keys. Can also enforce consistency between a natural key and the primary key.';
