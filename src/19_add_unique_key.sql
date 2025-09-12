CREATE FUNCTION sql_saga.add_unique_key(
        table_oid regclass,
        column_names name[],
        era_name name DEFAULT 'valid',
        key_type sql_saga.unique_key_type DEFAULT 'natural',
        unique_key_name name DEFAULT NULL,
        unique_constraint name DEFAULT NULL,
        exclude_constraint name DEFAULT NULL,
        predicate text DEFAULT NULL)
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
    unique_sql text;
    exclude_sql text;
    where_clause text;
BEGIN
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
        SELECT format('%s (%s) DEFERRABLE',
            CASE WHEN key_type = 'primary' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END, /* %s */
            string_agg(quote_ident(u.column_name), ', ' ORDER BY u.ordinality) /* %s */
        )
        INTO unique_sql
        FROM unnest(column_names || era_row.valid_from_column_name || era_row.valid_until_column_name) WITH ORDINALITY AS u (column_name, ordinality);

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

                IF NOT constraint_record.conkey = column_attnums || era_attnums THEN
                    RAISE EXCEPTION 'constraint "%" does not match', unique_constraint;
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
                RAISE EXCEPTION 'constraint "%" does not match', exclude_constraint;
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
    alter_cmds := '{}';
    IF key_type IN ('primary', 'natural') THEN
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

    IF NOT v_exclude_constraint_found THEN
        IF exclude_constraint IS NULL THEN
            exclude_constraint := unique_key_name || '_excl';
        END IF;
        alter_cmds := alter_cmds || ('ADD CONSTRAINT ' || quote_ident(exclude_constraint) || ' ' || exclude_sql);
    END IF;

    IF alter_cmds <> '{}' THEN
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

    /* If we created an exclude_constraint, we already know its name. */


    INSERT INTO sql_saga.unique_keys (unique_key_name, table_schema, table_name, key_type, column_names, era_name, unique_constraint, exclude_constraint, predicate)
    VALUES (unique_key_name, table_schema, table_name, key_type, column_names, era_name, unique_constraint, exclude_constraint, predicate);

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
