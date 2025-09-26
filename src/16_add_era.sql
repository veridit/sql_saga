CREATE OR REPLACE FUNCTION sql_saga.add_era(
    table_oid regclass,
    valid_from_column_name name DEFAULT 'valid_from',
    valid_until_column_name name DEFAULT 'valid_until',
    era_name name DEFAULT 'valid',
    range_type regtype DEFAULT NULL,
    bounds_check_constraint name DEFAULT NULL,
    synchronize_valid_to_column name DEFAULT NULL,
    synchronize_range_column name DEFAULT NULL,
    create_columns boolean DEFAULT false,
    add_defaults boolean DEFAULT true,
    add_bounds_check boolean DEFAULT true)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    table_schema name;
    table_name name;
    kind "char";
    persistence "char";
    alter_commands text[] DEFAULT '{}';

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
    v_trigger_applies_defaults boolean := false;
BEGIN
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
        RAISE EXCEPTION 'table "%" must be persistent', table_oid;
    END IF;

    /* If requested, create the period columns if they are missing */
    IF create_columns THEN
        DECLARE
            from_exists boolean;
            until_exists boolean;
            column_type_name text;
            add_sql text;
        BEGIN
            from_exists := EXISTS(SELECT 1 FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = table_oid AND a.attname = valid_from_column_name AND NOT a.attisdropped);
            until_exists := EXISTS(SELECT 1 FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = table_oid AND a.attname = valid_until_column_name AND NOT a.attisdropped);

            IF from_exists AND until_exists THEN
                -- Do nothing, columns are already there.
            ELSIF NOT from_exists AND NOT until_exists THEN
                -- Both are missing, create them.
                IF range_type IS NULL THEN
                    column_type_name := 'timestamp with time zone';
                ELSE
                    SELECT format_type(r.rngsubtype, NULL) INTO column_type_name FROM pg_catalog.pg_range r WHERE r.rngtypid = range_type;
                    IF column_type_name IS NULL THEN
                        RAISE EXCEPTION 'range type % not found', range_type;
                    END IF;
                END IF;
                add_sql := format('ALTER TABLE %s ADD COLUMN %I %s, ADD COLUMN %I %s',
                    table_oid, /* %s */
                    valid_from_column_name, /* %I */
                    column_type_name, /* %s */
                    valid_until_column_name, /* %I */
                    column_type_name /* %s */
                );
                EXECUTE add_sql;
            ELSE
                -- One exists but not the other. This is an error.
                RAISE EXCEPTION 'cannot create columns: one of "%", "%" exists, but not both', valid_from_column_name, valid_until_column_name;
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
        RAISE EXCEPTION 'era for "%" already exists on table "%"', era_name, table_oid;
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
        RAISE EXCEPTION 'a column named "%" already exists for table "%"', era_name, table_oid;
    END IF;

    /*
     * Contrary to SYSTEM_TIME periods, the columns must exist already for
     * application time sql_saga.
     *
     * SQL:2016 11.27 SR 5.d
     */

    /* Get start column information */
    SELECT a.attnum, a.atttypid, a.attcollation, a.attnotnull
    INTO valid_from_attnum, valid_from_type, valid_from_collation, valid_from_notnull
    FROM pg_catalog.pg_attribute AS a
    WHERE (a.attrelid, a.attname) = (table_oid, valid_from_column_name);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'column "%" not found in table "%"', valid_from_column_name, table_oid;
    END IF;

    IF valid_from_attnum < 0 THEN
        RAISE EXCEPTION 'system columns cannot be used in an era';
    END IF;

    /* Get end column information */
    SELECT a.attnum, a.atttypid, a.attcollation, a.attnotnull
    INTO valid_until_attnum, valid_until_type, valid_until_collation, valid_until_notnull
    FROM pg_catalog.pg_attribute AS a
    WHERE (a.attrelid, a.attname) = (table_oid, valid_until_column_name);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'column "%" not found in table "%"', valid_until_column_name, table_oid;
    END IF;

    IF valid_until_attnum < 0 THEN
        RAISE EXCEPTION 'system columns cannot be used in an era';
    END IF;

    /*
     * Verify compatibility of start/end columns.  The standard says these must
     * be either date or timestamp, but we allow anything with a corresponding
     * range type because why not.
     *
     * SQL:2016 11.27 SR 5.g
     */
    IF valid_from_type <> valid_until_type THEN
        RAISE EXCEPTION 'start and end columns must be of same type';
    END IF;

    IF valid_from_collation <> valid_until_collation THEN
        RAISE EXCEPTION 'start and end columns must be of same collation';
    END IF;

    /* Get the range type that goes with these columns */
    IF range_type IS NOT NULL THEN
        IF NOT EXISTS (
            SELECT FROM pg_catalog.pg_range AS r
            WHERE (r.rngtypid, r.rngsubtype, r.rngcollation) = (range_type, valid_from_type, valid_from_collation))
        THEN
            RAISE EXCEPTION 'range "%" does not match data type "%"', range_type, valid_from_type;
        END IF;
    ELSE
        SELECT r.rngtypid
        INTO range_type
        FROM pg_catalog.pg_range AS r
        JOIN pg_catalog.pg_opclass AS c ON c.oid = r.rngsubopc
        WHERE (r.rngsubtype, r.rngcollation) = (valid_from_type, valid_from_collation)
          AND c.opcdefault;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'no default range type for %', valid_from_type::regtype;
        END IF;
    END IF;

    /*
     * Period columns must not be nullable.
     *
     * SQL:2016 11.27 SR 5.h
     */
    IF NOT valid_from_notnull THEN
        alter_commands := alter_commands || format('ALTER COLUMN %I SET NOT NULL', valid_from_column_name /* %I */);
    END IF;
    IF NOT valid_until_notnull THEN
        alter_commands := alter_commands || format('ALTER COLUMN %I SET NOT NULL', valid_until_column_name /* %I */);
    END IF;


    /*
     * Find and appropriate a CHECK constraint to make sure that start < end.
     * Create one if necessary.
     *
     * SQL:2016 11.27 GR 2.b
     */
    DECLARE
        condef text;
        context text;
        subtype_info record;
    BEGIN
        SELECT t.typcategory, t.typname
        INTO subtype_info
        FROM pg_catalog.pg_type t
        WHERE t.oid = valid_from_type;

        IF add_defaults THEN
            IF subtype_info.typcategory = 'D' OR subtype_info.typname IN ('numeric', 'float4', 'float8') THEN
                IF synchronize_valid_to_column IS NOT NULL OR synchronize_range_column IS NOT NULL THEN
                    -- If there are synchronized columns, the trigger will handle defaults.
                    v_trigger_applies_defaults := true;
                ELSE
                    -- For simple eras, set the default directly on the table.
                    alter_commands := alter_commands || format('ALTER COLUMN %I SET DEFAULT ''infinity''', valid_until_column_name /* %I */);
                END IF;
            END IF;
        END IF;

        IF add_bounds_check THEN
            IF subtype_info.typcategory = 'D' OR subtype_info.typname IN ('numeric', 'float4', 'float8') THEN
                condef := format('CHECK ((%I < %I) AND (%I > ''-infinity''))', valid_from_column_name /* %I */, valid_until_column_name /* %I */, valid_from_column_name /* %I */);
            ELSE
                condef := format('CHECK ((%I < %I))', valid_from_column_name /* %I */, valid_until_column_name /* %I */);
            END IF;

            IF bounds_check_constraint IS NOT NULL THEN
            /* We were given a name, does it exist? */
            SELECT pg_catalog.pg_get_constraintdef(c.oid)
            INTO context
            FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.conname) = (table_oid, bounds_check_constraint)
              AND c.contype = 'c';

            IF FOUND THEN
                /* Does it match? */
                IF context <> condef THEN
                    RAISE EXCEPTION 'constraint "%" on table "%" does not match', bounds_check_constraint, table_oid;
                END IF;
            ELSE
                /* If it doesn't exist, we'll use the name for the one we create. */
                alter_commands := alter_commands || format('ADD CONSTRAINT %I %s', bounds_check_constraint /* %I */, condef /* %s */);
            END IF;
        ELSE
            /* No name given, can we appropriate one? */
            SELECT c.conname
            INTO bounds_check_constraint
            FROM pg_catalog.pg_constraint AS c
            WHERE c.conrelid = table_oid
              AND c.contype = 'c'
              AND pg_catalog.pg_get_constraintdef(c.oid) = condef;

            /* Make our own then */
            IF NOT FOUND THEN
                bounds_check_constraint := sql_saga.__internal_make_name(ARRAY[table_name, era_name], 'check');
                alter_commands := alter_commands || format('ADD CONSTRAINT %I %s', bounds_check_constraint /* %I */, condef /* %s */);
            END IF;
        END IF;
        END IF;
    END;

-- TODO: Ensure that infinity is the default.
--    /*
--     * Find and appropriate a CHECK constraint to make sure that end = 'infinity'.
--     * Create one if necessary.
--     *
--     * SQL:2016 4.15.2.2
--     */
--    DECLARE
--        condef CONSTANT text := format('CHECK ((%I = ''infinity''::timestamp with time zone))', valid_until_column_name);
--        context text;
--    BEGIN
--        IF infinity_check_constraint IS NOT NULL THEN
--            /* We were given a name, does it exist? */
--            SELECT pg_catalog.pg_get_constraintdef(c.oid)
--            INTO context
--            FROM pg_catalog.pg_constraint AS c
--            WHERE (c.conrelid, c.conname) = (table_class, infinity_check_constraint)
--              AND c.contype = 'c';
--
--            IF FOUND THEN
--                /* Does it match? */
--                IF context <> condef THEN
--                    RAISE EXCEPTION 'constraint "%" on table "%" does not match', infinity_check_constraint, table_class;
--                END IF;
--            ELSE
--                /* If it doesn't exist, we'll use the name for the one we create. */
--                alter_commands := alter_commands || format('ADD CONSTRAINT %I %s', infinity_check_constraint, condef);
--            END IF;
--        ELSE
--            /* No name given, can we appropriate one? */
--            SELECT c.conname
--            INTO infinity_check_constraint
--            FROM pg_catalog.pg_constraint AS c
--            WHERE c.conrelid = table_class
--              AND c.contype = 'c'
--              AND pg_catalog.pg_get_constraintdef(c.oid) = condef;
--
--            /* Make our own then */
--            IF NOT FOUND THEN
--                SELECT c.relname
--                INTO table_name
--                FROM pg_catalog.pg_class AS c
--                WHERE c.oid = table_class;
--
--                infinity_check_constraint := sql_saga._make_name(ARRAY[table_name, valid_until_column_name], 'infinity_check');
--                alter_commands := alter_commands || format('ADD CONSTRAINT %I %s', infinity_check_constraint, condef);
--            END IF;
--        END IF;
--    END;


    /* If we've created any work for ourselves, do it now */
    IF alter_commands <> '{}' THEN
        EXECUTE format('ALTER TABLE %s %s', table_oid /* %s */, array_to_string(alter_commands, ', ') /* %s */);
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
              AND a.attname IN (add_era.valid_from_column_name, add_era.valid_until_column_name)
        ) INTO v_pk_contains_temporal_col;

        IF NOT v_pk_contains_temporal_col AND EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conrelid = table_oid AND c.contype = 'p') THEN
            RAISE WARNING 'Table "%" has a simple PRIMARY KEY that does not include temporal columns. This schema is incompatible with SCD Type 2 history.', table_oid
            USING HINT = 'If you plan to use a temporal primary key for this era, you must use a composite primary key that includes a temporal column (e.g., PRIMARY KEY (id, valid_from)).';
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
            RAISE WARNING 'Table "%" has a GENERATED ALWAYS AS IDENTITY column ("%"). This schema is incompatible with SCD Type 2 history.', table_oid, v_identity_col_name
            USING HINT = 'If you plan to use a temporal primary key for this era, the identity column must be GENERATED BY DEFAULT AS IDENTITY to allow inserting historical records.';
        END IF;
    END;

    DECLARE
        range_subtype regtype;
        range_subtype_category char(1);
    BEGIN
        SELECT r.rngsubtype, t.typcategory
        INTO range_subtype, range_subtype_category
        FROM pg_catalog.pg_range r JOIN pg_catalog.pg_type t ON t.oid = r.rngsubtype
        WHERE r.rngtypid = range_type;

        INSERT INTO sql_saga.era (table_schema, table_name, era_name, valid_from_column_name, valid_until_column_name, range_type, range_subtype, range_subtype_category, bounds_check_constraint, trigger_applies_defaults)
        VALUES (table_schema, table_name, era_name, valid_from_column_name, valid_until_column_name, range_type, range_subtype, range_subtype_category, bounds_check_constraint, v_trigger_applies_defaults);
    END;

    -- Create the unified synchronization trigger if any sync columns are specified.
    IF synchronize_valid_to_column IS NOT NULL OR synchronize_range_column IS NOT NULL THEN
        DECLARE
            v_to_col      name := synchronize_valid_to_column;
            v_range_col   name := synchronize_range_column;
            sync_cols     name[] := ARRAY[]::name[];
            trigger_name  name;
            subtype_is_discrete boolean;
            v_range_subtype regtype;
        BEGIN
            -- Get metadata about the era's subtype
            SELECT e.range_subtype, (t.typcategory IN ('D', 'N') AND t.typname NOT IN ('timestamptz', 'timestamp', 'numeric'))
            INTO v_range_subtype, subtype_is_discrete
            FROM sql_saga.era e JOIN pg_type t ON e.range_subtype = t.oid
            WHERE (e.table_schema, e.table_name, e.era_name) = (table_schema, table_name, era_name);

            -- Validate valid_to column
            IF v_to_col IS NOT NULL THEN
                DECLARE
                    v_to_col_type oid;
                BEGIN
                    SELECT atttypid INTO v_to_col_type FROM pg_attribute WHERE attrelid = table_oid AND attname = v_to_col AND NOT attisdropped;
                    IF NOT FOUND THEN
                        RAISE EXCEPTION 'Synchronization column "%" not found on table %.', v_to_col, table_oid;
                    ELSIF v_to_col_type <> v_range_subtype THEN
                        RAISE WARNING 'sql_saga: Synchronization column "%" on table % has an incompatible data type (%). It must match the era subtype (%). Skipping synchronization.', v_to_col, table_oid, v_to_col_type::regtype, v_range_subtype::regtype;
                        v_to_col := NULL;
                    ELSIF NOT subtype_is_discrete THEN
                        RAISE WARNING 'sql_saga: "valid_to" synchronization is only supported for discrete types (date, integer, bigint). Disabling for column "%" on non-discrete era for table %.', v_to_col, table_oid;
                        v_to_col := NULL;
                    ELSE
                        sync_cols := sync_cols || v_to_col;
                    END IF;
                END;
            END IF;

            -- Validate range column
            IF v_range_col IS NOT NULL THEN
                DECLARE
                    v_range_col_type oid;
                    v_range_col_typtype "char";
                BEGIN
                    SELECT a.atttypid
                    INTO v_range_col_type
                    FROM pg_catalog.pg_attribute AS a
                    WHERE a.attrelid = table_oid AND a.attname = v_range_col AND NOT a.attisdropped;

                    IF NOT FOUND THEN
                        RAISE EXCEPTION 'Synchronization column "%" not found on table %.', v_range_col, table_oid;
                    END IF;

                    SELECT t.typtype
                    INTO v_range_col_typtype
                    FROM pg_catalog.pg_type AS t
                    WHERE t.oid = v_range_col_type;

                    IF v_range_col_typtype <> 'r' THEN
                        RAISE EXCEPTION 'Column "%" provided for range synchronization is type %, which is not a range type.', v_range_col, v_range_col_type::regtype;
                    END IF;

                    sync_cols := sync_cols || v_range_col;
                END;
            END IF;

            IF v_to_col IS NOT NULL OR v_range_col IS NOT NULL THEN
                trigger_name := format('%s_synchronize_temporal_columns_trigger', table_name /* %s */);
                EXECUTE format(
                    'CREATE TRIGGER %I BEFORE INSERT OR UPDATE OF %s ON %s FOR EACH ROW EXECUTE FUNCTION sql_saga.synchronize_temporal_columns(%L, %L, %L, %L, %L, %L)',
                    trigger_name, /* %I */
                    array_to_string(ARRAY[valid_from_column_name, valid_until_column_name] || sync_cols, ', '), /* %s */
                    table_oid::text, /* %s */
                    valid_from_column_name, /* %L */
                    valid_until_column_name, /* %L */
                    v_to_col, /* %L */
                    v_range_col, /* %L */
                    v_range_subtype, /* %L */
                    v_trigger_applies_defaults /* %L */
                );
                RAISE NOTICE 'sql_saga: Created trigger "%" on table % to synchronize columns: %', trigger_name, table_oid, array_to_string(sync_cols, ', ');

                UPDATE sql_saga.era e
                SET synchronize_valid_to_column = v_to_col,
                    synchronize_range_column = v_range_col
                WHERE (e.table_schema, e.table_name, e.era_name) = (table_schema, table_name, era_name);
            END IF;
        END;
    END IF;

    -- Code for creation of triggers, when extending the era api
    --        /* Make sure all the excluded columns exist */
    --    FOR excluded_column_name IN
    --        SELECT u.name
    --        FROM unnest(excluded_column_names) AS u (name)
    --        WHERE NOT EXISTS (
    --            SELECT FROM pg_catalog.pg_attribute AS a
    --            WHERE (a.attrelid, a.attname) = (table_class, u.name))
    --    LOOP
    --        RAISE EXCEPTION 'column "%" does not exist', excluded_column_name;
    --    END LOOP;
    --
    --    /* Don't allow system columns to be excluded either */
    --    FOR excluded_column_name IN
    --        SELECT u.name
    --        FROM unnest(excluded_column_names) AS u (name)
    --        JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (table_class, u.name)
    --        WHERE a.attnum < 0
    --    LOOP
    --        RAISE EXCEPTION 'cannot exclude system column "%"', excluded_column_name;
    --    END LOOP;
    --
    --    generated_always_trigger := coalesce(
    --        generated_always_trigger,
    --        sql_saga._make_name(ARRAY[table_name], 'system_time_generated_always'));
    --    EXECUTE format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE ON %s FOR EACH ROW EXECUTE PROCEDURE sql_saga.generated_always_as_row_start_end()', generated_always_trigger, table_class);
    --
    --    write_history_trigger := coalesce(
    --        write_history_trigger,
    --        sql_saga._make_name(ARRAY[table_name], 'system_time_write_history'));
    --    EXECUTE format('CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE PROCEDURE sql_saga.write_history()', write_history_trigger, table_class);
    --
    --    truncate_trigger := coalesce(
    --        truncate_trigger,
    --        sql_saga._make_name(ARRAY[table_name], 'truncate'));
    --    EXECUTE format('CREATE TRIGGER %I AFTER TRUNCATE ON %s FOR EACH STATEMENT EXECUTE PROCEDURE sql_saga.truncate_era()', truncate_trigger, table_class);


    RETURN true;
END;
$function$;

COMMENT ON FUNCTION sql_saga.add_era(regclass, name, name, name, regtype, name, name, name, boolean, boolean, boolean) IS
'Registers a table as a temporal table using convention-over-configuration. It can create and manage temporal columns, constraints, and synchronization triggers.';
