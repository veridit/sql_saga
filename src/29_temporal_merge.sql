-- The main public procedure for temporal_merge. It orchestrates the planning
-- and execution of the merge operation.
CREATE OR REPLACE PROCEDURE sql_saga.temporal_merge(
    target_table regclass,
    source_table regclass,
    primary_identity_columns TEXT[] DEFAULT NULL,
    mode sql_saga.temporal_merge_mode DEFAULT 'MERGE_ENTITY_PATCH',
    era_name name DEFAULT 'valid',
    row_id_column name DEFAULT 'row_id',
    founding_id_column name DEFAULT NULL,
    update_source_with_identity BOOLEAN DEFAULT false,
    natural_identity_columns TEXT[] DEFAULT NULL,
    delete_mode sql_saga.temporal_merge_delete_mode DEFAULT 'NONE',
    update_source_with_feedback BOOLEAN DEFAULT false,
    feedback_status_column name DEFAULT NULL,
    feedback_status_key name DEFAULT NULL,
    feedback_error_column name DEFAULT NULL,
    feedback_error_key name DEFAULT NULL,
    ephemeral_columns TEXT[] DEFAULT NULL,
    delay_constraints BOOLEAN DEFAULT true
)
LANGUAGE plpgsql AS $temporal_merge$
DECLARE
    -- The `temporal_merge` procedure distinguishes between two types of identity columns,
    -- which serve different roles in the merge process:
    --
    -- 1. Identity Columns (`primary_identity_columns` parameter):
    --    - Role: The partitioning key. This is the set of columns that uniquely and immutably
    --      identifies a single conceptual entity throughout its entire history.
    --    - Example: A surrogate key like `id` or `person_id`. It is typically derived from
    --      the table's primary key (`sql_saga.unique_keys.key_type = 'primary'`).
    --
    -- 2. Lookup Columns (`natural_identity_columns` parameter):
    --    - Role: The lookup key. This is a "business key" used to find an entity when its
    --      identity is not known by the source data.
    --    - Example: A social security number (`ssn`) or `employee_nr`. It is derived
    --      from the table's natural keys (`sql_saga.unique_keys.key_type = 'natural'`).

    v_log_trace boolean;
    v_log_sql boolean;
    v_log_plan boolean;
    v_log_id TEXT;
    v_summary_line TEXT;
    v_identity_cols_discovered TEXT[];
    v_natural_identity_cols_discovered TEXT[];
    v_natural_identity_keys_discovered JSONB;
    v_best_key_found TEXT[];
BEGIN
    -- This is the main user-facing procedure. It is responsible for:
    -- 1. Setting up session-level state (temp tables, logging flags).
    -- 2. Discovering identity columns if not provided.
    -- 3. Calling the planner to generate the execution plan.
    -- 4. Calling the executor to apply the plan to the database.


    v_log_trace := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.enable_trace', true), ''), 'false')::boolean;
    v_log_sql := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_sql', true), ''), 'false')::boolean;
    v_log_plan := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_plan', true), ''), 'false')::boolean;
    v_log_id := substr(md5(COALESCE(current_setting('sql_saga.temporal_merge.log_id_seed', true), random()::text)), 1, 3);

    -- Auto-discover ephemeral_columns from era metadata if not explicitly provided.
    IF temporal_merge.ephemeral_columns IS NULL AND temporal_merge.era_name IS NOT NULL THEN
        SELECT e.ephemeral_columns INTO temporal_merge.ephemeral_columns
        FROM sql_saga.era AS e
        WHERE (e.table_schema, e.table_name) = (
            SELECT n.nspname, c.relname
            FROM pg_class c
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE c.oid = temporal_merge.target_table
        )
        AND e.era_name = temporal_merge.era_name;
    END IF;

    -- Automatic Discovery of identity columns if they are NULL.
    v_identity_cols_discovered := temporal_merge.primary_identity_columns;
    v_natural_identity_cols_discovered := temporal_merge.natural_identity_columns;

    -- If primary_identity_columns is NULL, discover an identity key. Prioritize the primary key,
    -- but fall back to the first available natural key if no primary key is defined.
    IF v_identity_cols_discovered IS NULL THEN
        SELECT uk.column_names INTO v_identity_cols_discovered
        FROM sql_saga.unique_keys uk
        JOIN pg_class c ON uk.table_schema = c.relnamespace::regnamespace::name AND uk.table_name = c.relname
        WHERE c.oid = temporal_merge.target_table
          AND uk.era_name = temporal_merge.era_name
          -- This is a fallback to find a sensible default for the entity identifier.
          -- If a primary key isn't available, a natural key is the next best candidate.
          AND uk.key_type IN ('primary', 'natural')
        ORDER BY (uk.key_type = 'primary') DESC, uk.unique_key_name
        LIMIT 1;

        IF NOT FOUND AND primary_identity_columns IS NULL THEN
             RAISE EXCEPTION 'Could not discover an identity key (primary or natural) for table "%". Please register a key using sql_saga.add_unique_key() or explicitly provide primary_identity_columns.', target_table;
        END IF;
    END IF;

    -- If natural_identity_columns is NULL, discover all available natural keys.
    IF v_natural_identity_cols_discovered IS NULL THEN
        -- Discover all natural keys, ordered from most specific to least specific.
        WITH discovered_keys AS (
            SELECT uk.column_names
            FROM sql_saga.unique_keys uk
            JOIN pg_class c ON uk.table_schema = c.relnamespace::regnamespace::name AND uk.table_name = c.relname
            WHERE c.oid = temporal_merge.target_table
              AND uk.era_name = temporal_merge.era_name
              AND uk.key_type IN ('natural')
            ORDER BY cardinality(uk.column_names), uk.unique_key_name
        )
        SELECT
            -- Aggregate all discovered keys into a JSONB array for the planner.
            (SELECT jsonb_agg(to_jsonb(dk.column_names)) FROM discovered_keys dk),
            -- Selects the first key (ordered by cardinality) to serve as a representative for components
            -- that expect a single key, such as the performance hint generator for BTREE indexes.
            (SELECT dk.column_names FROM discovered_keys dk LIMIT 1)
        INTO
            v_natural_identity_keys_discovered,
            v_natural_identity_cols_discovered;
    ELSE
        -- If the user provided an empty array, they are explicitly disabling natural key lookups.
        IF cardinality(v_natural_identity_cols_discovered) = 0 THEN
            v_natural_identity_keys_discovered := '[]'::jsonb;
        ELSE
            -- If the user provided a key, find all matching superset keys from metadata. This makes lookups
            -- more robust, as the planner can consider all possible unique keys for entity resolution.
            WITH discovered_keys AS (
                SELECT uk.column_names
                FROM sql_saga.unique_keys uk
                JOIN pg_class c ON uk.table_schema = c.relnamespace::regnamespace::name AND uk.table_name = c.relname
                WHERE c.oid = temporal_merge.target_table
                  AND uk.era_name = temporal_merge.era_name
                  AND uk.key_type IN ('natural')
                  -- The discovered key must be a superset of the user-provided columns.
                  AND uk.column_names::text[] @> v_natural_identity_cols_discovered
                ORDER BY cardinality(uk.column_names), uk.unique_key_name
            )
            SELECT
                (SELECT jsonb_agg(to_jsonb(dk.column_names)) FROM discovered_keys dk),
                (SELECT dk.column_names FROM discovered_keys dk LIMIT 1)
            INTO
                v_natural_identity_keys_discovered,
                v_best_key_found;

            IF FOUND AND v_best_key_found IS NOT NULL THEN
                -- Use the best (most specific) superset key as the representative key.
                v_natural_identity_cols_discovered := v_best_key_found;
            ELSE
                -- If no superset key was found, fall back to using the user-provided key as-is.
                v_natural_identity_keys_discovered := jsonb_build_array(to_jsonb(v_natural_identity_cols_discovered));
            END IF;
        END IF;
    END IF;

    -- Validate that if ID back-filling is requested, the necessary columns exist in the source.
    IF update_source_with_identity THEN
        DECLARE
            v_col TEXT;
        BEGIN
            FOREACH v_col IN ARRAY COALESCE(v_identity_cols_discovered, '{}') LOOP
                PERFORM 1 FROM pg_attribute WHERE attrelid = source_table AND attname = v_col AND NOT attisdropped AND attnum > 0;
                IF NOT FOUND THEN
                    RAISE EXCEPTION 'When update_source_with_identity is true, identity_column "%" must exist in the source table "%" to be updated.', v_col, source_table;
                END IF;
            END LOOP;
        END;
    END IF;

    IF COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_vars', true), ''), 'false')::boolean THEN
        RAISE NOTICE '(%) --- temporal_merge discovered variables ---', v_log_id;
        RAISE NOTICE '(%) v_identity_cols_discovered: %', v_log_id, v_identity_cols_discovered;
        RAISE NOTICE '(%) v_natural_identity_cols_discovered: %', v_log_id, v_natural_identity_cols_discovered;
        RAISE NOTICE '(%) v_natural_identity_keys_discovered: %', v_log_id, v_natural_identity_keys_discovered;
    END IF;

    -- An entity must be identifiable. At least one set of identity columns must be provided.
    IF (v_identity_cols_discovered IS NULL OR cardinality(v_identity_cols_discovered) = 0) AND
       (v_natural_identity_cols_discovered IS NULL OR cardinality(v_natural_identity_cols_discovered) = 0)
    THEN
        RAISE EXCEPTION 'Could not determine identity columns for table "%". No primary or natural keys are registered, or they were explicitly disabled with an empty array.', temporal_merge.target_table;
    END IF;

    v_summary_line := format(
        'on %s: mode=>%s, delete_mode=>%s, primary_identity_columns=>%L, natural_identity_columns=>%L, ephemeral_columns=>%L, founding_id_column=>%L, row_id_column=>%L',
        temporal_merge.target_table,
        temporal_merge.mode,
        temporal_merge.delete_mode,
        v_identity_cols_discovered,
        v_natural_identity_keys_discovered,
        temporal_merge.ephemeral_columns,
        temporal_merge.founding_id_column,
        temporal_merge.row_id_column
    );

    -- The plan table is created here and passed implicitly to the planner and executor.
    IF to_regclass('pg_temp.temporal_merge_plan') IS NOT NULL THEN
        DROP TABLE pg_temp.temporal_merge_plan;
    END IF;
    CREATE TEMP TABLE temporal_merge_plan (LIKE sql_saga.temporal_merge_plan, PRIMARY KEY (plan_op_seq)) ON COMMIT DROP;

    -- Step 1: Generate the plan.
    INSERT INTO temporal_merge_plan
    SELECT * FROM sql_saga.temporal_merge_plan(
        target_table => temporal_merge.target_table,
        source_table => temporal_merge.source_table,
        mode => temporal_merge.mode,
        era_name => temporal_merge.era_name,
        identity_columns => v_identity_cols_discovered,
        row_id_column => temporal_merge.row_id_column,
        founding_id_column => temporal_merge.founding_id_column,
        delete_mode => temporal_merge.delete_mode,
        lookup_keys => v_natural_identity_keys_discovered,
        ephemeral_columns => temporal_merge.ephemeral_columns,
        p_log_trace => v_log_trace,
        p_log_sql => v_log_sql
    );

    -- Conditionally output the plan for debugging.
    DECLARE
        v_plan_rec RECORD;
    BEGIN
        IF v_log_plan THEN
            RAISE NOTICE 'temporal_merge plan (%) %', v_log_id, v_summary_line;
            FOR v_plan_rec IN SELECT * FROM temporal_merge_plan ORDER BY plan_op_seq LOOP
                RAISE NOTICE '(%) %', v_log_id, to_json(v_plan_rec);
            END LOOP;
        END IF;
    END;

    -- Step 2: Execute the plan.
    CALL sql_saga.temporal_merge_execute(
        target_table => temporal_merge.target_table,
        source_table => temporal_merge.source_table,
        identity_columns => v_identity_cols_discovered,
        mode => temporal_merge.mode,
        era_name => temporal_merge.era_name,
        row_id_column => temporal_merge.row_id_column,
        founding_id_column => temporal_merge.founding_id_column,
        update_source_with_identity => temporal_merge.update_source_with_identity,
        lookup_columns => v_natural_identity_cols_discovered,
        delete_mode => temporal_merge.delete_mode,
        update_source_with_feedback => temporal_merge.update_source_with_feedback,
        feedback_status_column => temporal_merge.feedback_status_column,
        feedback_status_key => temporal_merge.feedback_status_key,
        feedback_error_column => temporal_merge.feedback_error_column,
        feedback_error_key => temporal_merge.feedback_error_key,
        ephemeral_columns => temporal_merge.ephemeral_columns,
        delay_constraints => temporal_merge.delay_constraints
    );
END;
$temporal_merge$;

COMMENT ON PROCEDURE sql_saga.temporal_merge IS 'Executes a set-based temporal merge operation. It generates a plan using temporal_merge_plan and then executes it.';


CREATE OR REPLACE PROCEDURE sql_saga.temporal_merge_drop_cache()
LANGUAGE plpgsql AS $procedure$
BEGIN
    IF to_regclass('pg_temp.temporal_merge_plan_cache') IS NOT NULL THEN DROP TABLE pg_temp.temporal_merge_plan_cache; END IF;
    IF to_regclass('pg_temp.temporal_merge_index_cache') IS NOT NULL THEN DROP TABLE pg_temp.temporal_merge_index_cache; END IF;
    CALL sql_saga.temporal_merge_drop_temp_tables();
END;
$procedure$;

COMMENT ON PROCEDURE sql_saga.temporal_merge_drop_cache IS
'Drops cached prepared statements and all temporary tables created by the temporal_merge_plan function. This is intended for use in specialized test cases that involve role switching within a single transaction, where automatic ON COMMIT DROP cleanup does not suffice.';
