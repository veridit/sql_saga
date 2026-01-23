CREATE OR REPLACE FUNCTION sql_saga.add_era(
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
    v_trigger_applies_defaults boolean := add_era.add_defaults;
    boundary_check_constraint name;
BEGIN
    -- Parameter validation for explicit user input.
    IF (add_era.valid_from_column_name IS NOT NULL AND add_era.valid_until_column_name IS NULL) OR
       (add_era.valid_from_column_name IS NULL AND add_era.valid_until_column_name IS NOT NULL)
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
        RAISE EXCEPTION 'table "%" must be persistent', table_oid;
    END IF;

    /* If requested, create columns if they are missing */
    IF create_columns THEN
        DECLARE
            range_exists boolean;
            from_exists boolean;
            until_exists boolean;
            v_range_type regtype := add_era.range_type;
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
                EXECUTE format('ALTER TABLE %s %s', table_oid, array_to_string(add_clauses, ', '));
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
            RAISE EXCEPTION 'range column "%" not found in table "%"', range_column_name, table_oid;
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
            RAISE EXCEPTION 'column "%" not found in table "%"', valid_from_column_name, table_oid;
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
            RAISE EXCEPTION 'column "%" not found in table "%"', valid_until_column_name, table_oid;
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
                    RAISE EXCEPTION 'constraint "%" on table "%" does not match expected CHECK (NOT isempty(%))', bounds_check_constraint, table_oid, range_column_name;
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
              AND a.attname IN (add_era.range_column_name, add_era.valid_from_column_name, add_era.valid_until_column_name)
        ) INTO v_pk_contains_temporal_col;

        IF NOT v_pk_contains_temporal_col AND EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conrelid = table_oid AND c.contype = 'p') THEN
            RAISE WARNING 'Table "%" has a simple PRIMARY KEY that does not include temporal columns. This schema is incompatible with SCD Type 2 history.', table_oid
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
            RAISE WARNING 'Table "%" has a GENERATED ALWAYS AS IDENTITY column ("%"). This schema is incompatible with SCD Type 2 history.', table_oid, v_identity_col_name
            USING HINT = 'If you plan to use a temporal primary key for this era, the identity column must be GENERATED BY DEFAULT AS IDENTITY to allow inserting historical records.';
        END IF;
    END;

    -- Create the unified synchronization trigger if requested.
    DECLARE
        v_from_col    name := add_era.valid_from_column_name;
        v_until_col   name := add_era.valid_until_column_name;
        v_to_col      name := add_era.valid_to_column_name;
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

        INSERT INTO sql_saga.era (table_schema, table_name, era_name, range_column_name, valid_from_column_name, valid_until_column_name, valid_to_column_name, range_type, multirange_type, range_subtype, range_subtype_category, bounds_check_constraint, boundary_check_constraint, trigger_applies_defaults, ephemeral_columns)
        VALUES (table_schema, table_name, era_name, range_column_name, v_from_col, v_until_col, v_to_col, range_type, v_multirange_type, v_range_subtype, v_range_subtype_category, bounds_check_constraint, boundary_check_constraint, v_trigger_applies_defaults, add_era.ephemeral_columns);

        IF v_create_template_trigger THEN
            CALL sql_saga.add_synchronize_temporal_columns_trigger(table_oid, era_name);
            RAISE NOTICE 'sql_saga: Created trigger "%" on table % to synchronize columns: %', v_trigger_name, table_oid, v_sync_cols_text;
        END IF;
    END;

    RETURN true;
END;
$function$;

COMMENT ON FUNCTION sql_saga.add_era IS
'Registers a table as a temporal table using convention-over-configuration. It can create and manage temporal columns, constraints, and synchronization triggers.

Parameters:
- ephemeral_columns: Columns excluded from coalescing comparison during temporal_merge operations.
  These are typically audit columns (e.g., edit_at, edit_by_user_id) whose changes should not
  prevent adjacent periods from being merged. This setting serves as a default for all views
  and temporal_merge calls on this era.';
