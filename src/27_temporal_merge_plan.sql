CREATE or replace AGGREGATE array_concat_agg(anycompatiblearray) (   SFUNC = array_cat,   STYPE = anycompatiblearray );

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
    v_plan_sqls TEXT[];
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
BEGIN
    v_log_vars := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_vars', true), ''), 'false')::boolean;
    v_log_id := substr(md5(COALESCE(current_setting('sql_saga.temporal_merge.log_id_seed', true), random()::text)), 1, 3);

    IF to_regclass('pg_temp.temporal_merge_plan_cache') IS NULL THEN
        CREATE TEMP TABLE temporal_merge_plan_cache (
            cache_key TEXT PRIMARY KEY,
            plan_sqls TEXT[] NOT NULL
        );
    END IF;

    v_causal_col := COALESCE(founding_id_column, row_id_column);

    --
    -- Phase 1: Introspection and Column List Generation
    -- This block is the single source of truth for all column lists. All subsequent
    -- logic builds upon these foundational variables.
    --
    -- 1.1: Introspect temporal boundary columns. This must happen first.
    SELECT n.nspname, c.relname
    INTO v_target_schema_name, v_target_table_name_only
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = target_table;

    SELECT e.range_type, e.range_subtype, e.range_subtype_category, e.valid_from_column_name, e.valid_until_column_name, e.synchronize_valid_to_column, e.synchronize_range_column
    INTO v_range_constructor, v_range_subtype, v_range_subtype_category, v_valid_from_col, v_valid_until_col, v_valid_to_col, v_range_col
    FROM sql_saga.era e
    WHERE e.table_schema = v_target_schema_name AND e.table_name = v_target_table_name_only AND e.era_name = temporal_merge_plan.era_name;

    IF v_valid_from_col IS NULL THEN RAISE EXCEPTION 'No era named "%" found for table "%"', era_name, target_table; END IF;

    v_temporal_cols := ARRAY[v_valid_from_col, v_valid_until_col];
    IF v_valid_to_col IS NOT NULL THEN v_temporal_cols := v_temporal_cols || v_valid_to_col; END IF;
    IF v_range_col IS NOT NULL THEN v_temporal_cols := v_temporal_cols || v_range_col; END IF;

    -- 1.2: Introspect and sanitize all key types.
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

    -- 1.3: Introspect the primary key.
    SELECT COALESCE(array_agg(a.attname), '{}'::name[]) INTO v_pk_cols
    FROM pg_constraint c JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
    WHERE c.conrelid = target_table AND c.contype = 'p';

    -- 1.4: Introspect causal column type.
    SELECT atttypid INTO v_causal_column_type
    FROM pg_attribute
    WHERE attrelid = source_table
      AND attname = v_causal_col
      AND NOT attisdropped AND attnum > 0;

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
    -- Automatically treat synchronized columns as ephemeral for change detection.
    IF v_valid_to_col IS NOT NULL THEN
        v_ephemeral_columns := v_ephemeral_columns || v_valid_to_col;
    END IF;
    IF v_range_col IS NOT NULL THEN
        v_ephemeral_columns := v_ephemeral_columns || v_range_col;
    END IF;
    -- Normalize ephemeral columns for stable cache key and better reuse.
    SELECT ARRAY(SELECT DISTINCT e FROM unnest(v_ephemeral_columns) AS e ORDER BY e)
    INTO v_ephemeral_columns;

    -- 1.7: Build the source table's temporal column expression.
    -- This block determines how to project `valid_from` and `valid_until` from the source,
    -- handling `valid_to` conversion automatically. It also performs a consistency
    -- check if both `valid_to` and `valid_until` are present.
    DECLARE
        v_source_has_valid_from BOOLEAN;
        v_source_has_valid_until BOOLEAN;
        v_source_has_valid_to BOOLEAN;
    BEGIN
        SELECT
            EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = source_table AND attname = v_valid_from_col AND NOT attisdropped AND attnum > 0),
            EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = source_table AND attname = v_valid_until_col AND NOT attisdropped AND attnum > 0)
        INTO v_source_has_valid_from, v_source_has_valid_until;

        -- Check for `valid_to` column. If the era metadata specifies a name, use it.
        -- Otherwise, fall back to the convention 'valid_to'.
        IF v_valid_to_col IS NOT NULL THEN
            SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = source_table AND attname = v_valid_to_col AND NOT attisdropped AND attnum > 0)
            INTO v_source_has_valid_to;
        ELSE
            SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = source_table AND attname = 'valid_to' AND NOT attisdropped AND attnum > 0)
            INTO v_source_has_valid_to;
            IF v_source_has_valid_to THEN
                v_valid_to_col := 'valid_to';
            END IF;
        END IF;

        IF NOT v_source_has_valid_from THEN
            RAISE EXCEPTION 'Source table "%" must have a "%" column matching the target era''s valid_from column.', source_table::text, v_valid_from_col;
        END IF;

        -- Determine the correct interval to add to `valid_to` to get `valid_until`,
        -- using the era's metadata as the single source of truth.
        IF v_source_has_valid_to THEN
            CASE v_range_subtype_category
                WHEN 'D' THEN -- Date/time types
                    v_interval := '''1 day''::interval';
                WHEN 'N' THEN -- Numeric types
                    v_interval := '1';
                ELSE
                    RAISE EXCEPTION 'Unsupported range subtype for valid_to -> valid_until conversion: %', v_range_constructor::text;
            END CASE;
        END IF;

        -- The logic follows a clear order of precedence:
        -- 1. If valid_until is present, use it. If valid_to is also present, a
        --    per-row consistency check will be performed inside the main query.
        -- 2. If only valid_to is present, derive valid_until.
        -- 3. If neither is present, raise an error.
        IF v_source_has_valid_until THEN
            -- `valid_until` is the source of truth.
            v_source_temporal_cols_expr := format('source_table.%1$I as valid_from, source_table.%2$I as valid_until', v_valid_from_col, v_valid_until_col);
        ELSIF v_source_has_valid_to THEN
            -- Only `valid_to` is present; derive `valid_until`.
            v_source_temporal_cols_expr := format('source_table.%1$I as valid_from, (source_table.%2$I + %3$s)::%4$s as valid_until', v_valid_from_col, v_valid_to_col, v_interval, v_range_subtype);
        ELSE
            RAISE EXCEPTION 'Source table "%" must have either a "%" column or a "%" column matching the target era.', source_table::text, v_valid_until_col, COALESCE(v_valid_to_col, 'valid_to');
        END IF;

        -- Build the per-row consistency check expression. This check is only performed
        -- if BOTH columns are present in the source.
        IF v_source_has_valid_until AND v_source_has_valid_to THEN
            -- The check is only false if BOTH columns have a value and they are inconsistent.
            -- If one is NULL, it's considered consistent for the purpose of the FOR PORTION OF trigger.
            v_consistency_check_expr := format($$(
                (source_table.%1$I IS NULL OR source_table.%2$I IS NULL)
                OR
                (((source_table.%1$I + %3$s)::%4$s) IS NOT DISTINCT FROM source_table.%2$I)
            )$$, v_valid_to_col, v_valid_until_col, v_interval, v_range_subtype);
        ELSE
            v_consistency_check_expr := 'true';
        END IF;
    END; -- END DECLARE

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
    -- Phase 2: Cache Management & Validation
    --
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

    SELECT plan_sqls INTO v_plan_sqls FROM pg_temp.temporal_merge_plan_cache WHERE cache_key = v_plan_key_text;
    IF NOT FOUND THEN -- PREPARE all sql's
        -- On cache miss, proceed with validation and planning.
        PERFORM 1 FROM pg_attribute WHERE attrelid = source_table AND attname = row_id_column AND NOT attisdropped AND attnum > 0;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'row_id_column "%" does not exist in source table %s', row_id_column, source_table::text;
        END IF;
    
        IF founding_id_column IS NOT NULL THEN
            PERFORM 1 FROM pg_attribute WHERE attrelid = source_table AND attname = founding_id_column AND NOT attisdropped AND attnum > 0;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'founding_id_column "%" does not exist in source table %s', founding_id_column, source_table::text;
            END IF;
        END IF;
    
        -- Validate that identity columns exist in both source and target tables.
        DECLARE
            v_col TEXT;
        BEGIN
            IF v_identity_columns IS NOT NULL AND cardinality(v_identity_columns) > 0 THEN
                FOREACH v_col IN ARRAY v_identity_columns LOOP
                    -- The identity key MUST exist in the target.
                    PERFORM 1 FROM pg_attribute WHERE attrelid = target_table AND attname = v_col AND NOT attisdropped AND attnum > 0;
                    IF NOT FOUND THEN RAISE EXCEPTION 'identity_column % does not exist in target table %', quote_ident(v_col), target_table; END IF;
    
                    -- It only needs to exist in the source IF we are not using a separate lookup key.
                    IF v_representative_lookup_key IS NULL OR cardinality(v_representative_lookup_key) = 0 THEN
                        PERFORM 1 FROM pg_attribute WHERE attrelid = source_table AND attname = v_col AND NOT attisdropped AND attnum > 0;
                        IF NOT FOUND THEN RAISE EXCEPTION 'identity_column % does not exist in source table % (and no lookup_columns were provided)', quote_ident(v_col), source_table; END IF;
                    END IF;
                END LOOP;
            END IF;
    
            IF v_all_lookup_cols IS NOT NULL AND cardinality(v_all_lookup_cols) > 0 THEN
                FOREACH v_col IN ARRAY v_all_lookup_cols LOOP
                    -- Lookup keys must exist in the target table.
                    PERFORM 1 FROM pg_attribute WHERE attrelid = target_table AND attname = v_col AND NOT attisdropped AND attnum > 0;
                    IF NOT FOUND THEN RAISE EXCEPTION 'lookup_column % does not exist in target table %', quote_ident(v_col), target_table; END IF;
    
                    -- Lookup keys must also exist in the source table to be used for lookup.
                    PERFORM 1 FROM pg_attribute WHERE attrelid = source_table AND attname = v_col AND NOT attisdropped AND attnum > 0;
                    IF NOT FOUND THEN RAISE EXCEPTION 'lookup_column % does not exist in source table %', quote_ident(v_col), source_table; END IF;
                END LOOP;
            END IF;
        END; -- END DECLARE
    
        IF v_causal_col IS NULL THEN
            RAISE EXCEPTION 'The causal identifier column cannot be NULL. Please provide a non-NULL value for either founding_id_column or row_id_column.';
        END IF;
        IF v_causal_column_type IS NULL THEN
            RAISE EXCEPTION 'Causal column "%" does not exist in source table %s', v_causal_col, source_table::text;
        END IF;
        IF v_causal_col = ANY(v_lookup_columns) THEN
            RAISE EXCEPTION 'The causal column (%) cannot be one of the natural identity columns (%)', v_causal_col, v_lookup_columns;
        END IF;
    
        IF v_range_constructor IS NULL THEN
            RAISE EXCEPTION 'Could not determine the range type for era "%" on table "%". Please ensure the era was created correctly, possibly by specifying the range_type parameter in add_era().', era_name, target_table;
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
            v_source_schema_name TEXT;
            v_source_table_ident TEXT;
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
            v_propagated_id_cols_list TEXT;
            v_lookup_keys_as_jsonb_expr TEXT;
            v_lookup_keys_as_array_expr TEXT;
            v_keys_for_filtering JSONB;
            v_lateral_source_resolver_sql TEXT;
            v_unified_id_cols_projection TEXT;
            v_target_nk_json_expr TEXT;
            v_identity_keys_jsonb_build_expr_d TEXT;
            v_lookup_keys_jsonb_build_expr_d TEXT;
            v_identity_keys_jsonb_build_expr_sr TEXT;
            v_lookup_keys_jsonb_build_expr_sr TEXT;
        BEGIN
            -- On cache miss, proceed with the expensive introspection and query building.
            v_unified_id_cols_projection := (
                SELECT string_agg(
                    CASE
                        WHEN col = ANY(COALESCE(v_all_lookup_cols, '{}'))
                        THEN format('(tpu.unified_canonical_nk_json->>%L)::%s AS %I', col, type, col)
                        ELSE format('tpu.%I', col)
                    END,
                    ', '
                )
                FROM (
                    SELECT col, format_type(a.atttypid, a.atttypmod) as type
                    FROM unnest(v_original_entity_segment_key_cols) c(col)
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
                            ' AND '
                        )
                        FROM unnest(COALESCE(v_all_lookup_cols, '{}')) AS c(col)
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
    
            SELECT n.nspname INTO v_source_schema_name
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.oid = source_table;
    
            IF v_source_schema_name = 'pg_temp' THEN
                v_source_table_ident := source_table::regclass::TEXT;
            ELSE
                v_source_table_ident := source_table::regclass::TEXT;
            END IF;
    
    
            -- Construct reusable SQL fragments for entity key column lists and join conditions,
            -- since these are more index friendly than a jsonb object, that we previously had.
            -- The lateral joins from atomic segments to source/target rows must use ALL identity columns
            -- to ensure the join is on a unique entity. This logic correctly maps original column names
            -- to their internal, aliased names, which are used in all relevant CTEs (sr, tr, seg).
            v_lateral_join_sr_to_seg := format($$
                (CASE WHEN seg.is_new_entity
                THEN source_row.grouping_key = seg.grouping_key
                ELSE %s
                END)
            $$, (
                SELECT COALESCE(string_agg(format('(source_row.%1$I = seg.%1$I OR (source_row.%1$I IS NULL AND seg.%1$I IS NULL))', col), ' AND '), 'true')
                FROM unnest(v_original_entity_key_cols) AS col
            ));
            v_lateral_join_tr_to_seg := (
                SELECT COALESCE(string_agg(format('(target_row.%1$I = seg.%1$I OR (target_row.%1$I IS NULL AND seg.%1$I IS NULL))', col), ' AND '), 'true')
                FROM unnest(v_original_entity_key_cols) AS col
            );
    
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
                ', '), '')
                FROM unnest(v_original_entity_segment_key_cols) AS c(col)
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
                WHERE s.attname NOT IN (row_id_column, v_valid_from_col, v_valid_until_col, 'era_id', 'era_name')
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
                WHERE t.attname NOT IN (v_valid_from_col, v_valid_until_col, 'era_id', 'era_name')
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
                (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, source_table.%I', attname, attname), ', '), '')) FROM source_data_cols),
                (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, source_table.%I', attname, attname), ', '), '')) FROM source_ephemeral_cols),
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
    
            -- Build a jsonb object of the stable identity columns. These are the keys
            -- that must be preserved across an entity's timeline.
            SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, t.%I', col, col), ', '), ''))
            INTO v_stable_pk_cols_jsonb_build
            FROM unnest(v_identity_columns) col;
    
            SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, %I', col, col), ', '), ''))
            INTO v_stable_pk_cols_jsonb_build_bare
            FROM unnest(v_identity_columns) col;
    
            -- Build source-side stable PK payload. It must project NULL for columns not present in the source.
            SELECT COALESCE(format('jsonb_build_object(%s)', string_agg(
                CASE
                    WHEN sa.attname IS NOT NULL THEN format('%L, source_table.%I', c.col, c.col)
                    ELSE format('%L, NULL::%s', c.col, format_type(ta.atttypid, ta.atttypmod))
                END,
            ', ')), '{}'::jsonb::text)
            INTO v_stable_pk_cols_jsonb_build_source
            FROM unnest(COALESCE(v_identity_columns, '{}')) c(col)
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
                ' AND '), 'true')
            INTO
                v_entity_id_check_is_null_expr
            FROM unnest(v_identity_columns) AS c(col)
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
                    v_distinct_on_cols := v_identity_columns || v_valid_from_col;
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
                            SELECT bool_or(is_nullable)
                            INTO v_any_nullable_lookup_cols
                            FROM (
                                SELECT NOT a.attnotnull as is_nullable
                                FROM unnest(v_key_cols) as c(attname)
                                JOIN pg_attribute a ON a.attname = c.attname AND a.attrelid = target_table
                            ) c;
                            v_any_nullable_lookup_cols := COALESCE(v_any_nullable_lookup_cols, false);
    
                            -- This is the crucial fix. The subquery must project the columns for the
                            -- current key, and NULL for all columns belonging to OTHER natural keys
                            -- to ensure the UNION branches have the same structure.
                            v_lookup_cols_si_alias_select_list := (
                                SELECT string_agg(
                                    CASE
                                        WHEN c.col = ANY(v_key_cols) THEN format('si.%I', c.col)
                                        ELSE format('NULL::%s', format_type(a.atttypid, a.atttypmod))
                                    END,
                                ', ')
                                FROM unnest(v_lookup_columns) AS c(col)
                                JOIN pg_attribute a ON a.attrelid = target_table AND a.attname = c.col
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
                                DECLARE
                                    v_join_clause TEXT;
                                BEGIN
                                    v_join_clause := COALESCE(
                                        (SELECT string_agg(format('(si.%1$I = inner_t.%1$I OR (si.%1$I IS NULL AND inner_t.%1$I IS NULL))', c), ' AND ') FROM unnest(v_key_cols) c),
                                        'true'
                                    );
    
                                    v_union_parts := v_union_parts || format($$(
                                        SELECT DISTINCT ON (%3$s) inner_t.*
                                        FROM %1$s inner_t
                                        JOIN (SELECT DISTINCT %4$s FROM source_initial si) AS si ON (%2$s)
                                    )$$, v_target_table_ident, v_join_clause, (SELECT string_agg(format('inner_t.%I', col), ', ') FROM unnest(v_distinct_on_cols) AS col), (SELECT string_agg(format('si.%I', col), ', ') FROM unnest(v_key_cols) col));
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
                v_source_data_payload_expr := format('jsonb_strip_nulls(%s)', v_source_data_cols_jsonb_build);
                v_source_ephemeral_payload_expr := format('jsonb_strip_nulls(%s)', v_source_ephemeral_cols_jsonb_build);
            ELSIF mode = 'DELETE_FOR_PORTION_OF' THEN
                v_source_data_payload_expr := '''"__DELETE__"''::jsonb';
                v_source_ephemeral_payload_expr := v_source_ephemeral_cols_jsonb_build;
            ELSE -- MERGE_ENTITY_REPLACE, MERGE_ENTITY_UPSERT, REPLACE_FOR_PORTION_OF, UPDATE_FOR_PORTION_OF, INSERT_NEW_ENTITIES
                v_source_data_payload_expr := v_source_data_cols_jsonb_build;
                v_source_ephemeral_payload_expr := v_source_ephemeral_cols_jsonb_build;
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
            v_source_rows_exists_join_expr := (
                SELECT string_agg(
                    format('((%s))', (
                        SELECT string_agg(format('source_row.%1$I IS NOT DISTINCT FROM target_row.%2$I', c, CASE WHEN c = v_valid_from_col THEN 'valid_from'::name WHEN c = v_valid_until_col THEN 'valid_until'::name ELSE c END), ' AND ')
                        FROM jsonb_array_elements_text(v_key) AS c
                    )),
                ' OR ')
                FROM jsonb_array_elements(lookup_keys) AS v_key
            );
    
            IF v_source_rows_exists_join_expr IS NULL THEN
                v_source_rows_exists_join_expr := (SELECT string_agg(format('source_row.%1$I IS NOT DISTINCT FROM target_row.%2$I', col, CASE WHEN col = v_valid_from_col THEN 'valid_from'::name WHEN col = v_valid_until_col THEN 'valid_until'::name ELSE col END), ' AND ') FROM unnest(v_identity_columns) as col);
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
                v_trace_seed_expr := format($$jsonb_build_object('cte', 'resolved_atomic_segments_with_payloads', 'contributing_row_ids', source_payloads.contributing_row_ids, 'constellation', %4$L, 'grouping_key', jsonb_build_object(%1$s), 'source_row_id', source_payloads.source_row_id, 's_data', source_payloads.data_payload, 't_data', target_payloads.data_payload, 's_ephemeral', source_payloads.ephemeral_payload, 't_ephemeral', target_payloads.ephemeral_payload, 's_t_relation', sql_saga.get_allen_relation(source_payloads.valid_from, source_payloads.valid_until, target_payloads.t_valid_from, target_payloads.t_valid_until), 'stable_pk_payload', target_payloads.stable_pk_payload, 'propagated_stable_pk_payload', seg.stable_pk_payload, 'seg_is_new_entity', seg.is_new_entity, 'seg_stable_identity_columns_are_null', seg.stable_identity_columns_are_null, 'seg_natural_identity_column_values_are_null', seg.natural_identity_column_values_are_null, 'stable_identity_values', %2$s, 'natural_identity_values', %3$s, 'canonical_causal_id', seg.causal_id, 'direct_source_causal_id', source_payloads.causal_id) as trace$$,
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
                    'propagated_stable_pk_payload', propagated_stable_pk_payload
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
                ELSE
                    RAISE EXCEPTION 'Unhandled temporal_merge_mode in planner: %', mode;
            END CASE;
    
            -- 4. Construct and execute the main query to generate the execution plan.
            --
            -- Phase 4.1: Materialize source_initial as a temporary table for performance and clarity.
            
            v_sql := format($SQL$
                CREATE TEMP TABLE source_initial ON COMMIT DROP AS
                SELECT
                    source_table.%1$I /* row_id_column */ as source_row_id,
                    %2$s /* v_causal_select_expr */ as causal_id,
                    %3$s -- v_non_temporal_lookup_cols_select_list_prefix
                    %4$s, /* v_source_temporal_cols_expr */
                    %5$s /* v_source_data_payload_expr */ AS data_payload,
                    %6$s /* v_source_ephemeral_payload_expr */ AS ephemeral_payload,
                    %7$s /* v_stable_pk_cols_jsonb_build_source */ as stable_pk_payload,
                    %8$s /* v_entity_id_check_is_null_expr */ as stable_identity_columns_are_null,
                    %9$s /* v_lookup_cols_are_null_expr */ as natural_identity_column_values_are_null,
                    %10$s /* v_is_identifiable_expr */ as is_identifiable,
                    %11$s /* v_consistency_check_expr */ as temporal_columns_are_consistent
                FROM %12$s /* v_source_table_ident */ source_table;
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
                v_source_table_ident                                 /* %12$s */
            );
            v_plan_sqls := v_plan_sqls || v_sql;
    
            v_sql := format($SQL$
                CREATE TEMP TABLE source_with_eclipsed_flag ON COMMIT DROP AS
                SELECT
                    s1.*,
                    eclipse_info.is_eclipsed,
                    eclipse_info.eclipsed_by
                FROM source_initial s1
                CROSS JOIN LATERAL (
                    SELECT
                        COALESCE(sql_saga.covers_without_gaps(%1$I(s2.valid_from, s2.valid_until), %1$I(s1.valid_from, s1.valid_until) ORDER BY s2.valid_from), false) as is_eclipsed,
                        array_agg(s2.source_row_id) as eclipsed_by
                    FROM source_initial s2
                    WHERE
                        (
                            (NOT s1.natural_identity_column_values_are_null AND (%2$s))
                            OR
                            (s1.natural_identity_column_values_are_null AND s1.causal_id = s2.causal_id)
                        )
                        AND
                        -- Only consider newer rows (higher row_id) as potential eclipsers.
                        s2.source_row_id > s1.source_row_id
                ) eclipse_info;
            $SQL$,
                v_range_constructor,          -- %1$I
                v_natural_key_join_condition  -- %2$s
            );
            v_plan_sqls := v_plan_sqls || v_sql;
    
            -- Phase 4.2: Construct and execute the main query to generate the execution plan.
            -- Each CTE is materialized into a temporary table for performance analysis and debugging.
            
            -- CTE 3: target_rows
            v_sql := format($SQL$
                CREATE TEMP TABLE target_rows ON COMMIT DROP AS
                SELECT
                    %1$s /* v_non_temporal_lookup_cols_select_list_no_alias_prefix */ -- (non-temporal identity columns)
                    %2$I /* v_valid_from_col */ as valid_from, -- The temporal identity column (e.g., valid_from)
                    NULL::%3$s /* v_causal_column_type */ as causal_id, -- Target rows do not originate from a source row, so causal_id is NULL. Type is introspected.
                    %4$s /* v_stable_pk_cols_jsonb_build_bare */ as stable_pk_payload,
                    %5$I /* v_valid_until_col */ as valid_until,
                    %6$s /* v_target_data_cols_jsonb_build_bare */ AS data_payload,
                    %7$s /* v_target_ephemeral_cols_jsonb_build_bare */ AS ephemeral_payload,
                    %8$s /* v_target_nk_json_expr */ AS canonical_nk_json
                FROM %9$s /* v_target_rows_filter */
            $SQL$,
                v_non_temporal_lookup_cols_select_list_no_alias_prefix, /* %1$s */
                v_valid_from_col,                                       /* %2$I */
                v_causal_column_type,                                   /* %3$s */
                v_stable_pk_cols_jsonb_build_bare,                      /* %4$s */
                v_valid_until_col,                                      /* %5$I */
                v_target_data_cols_jsonb_build_bare,                    /* %6$s */
                v_target_ephemeral_cols_jsonb_build_bare,               /* %7$s */
                v_target_nk_json_expr,                                  /* %8$s */
                v_target_rows_filter                                    /* %9$s */
            );
            v_plan_sqls := v_plan_sqls || v_sql;
    
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
            v_plan_sqls := v_plan_sqls || v_sql;
    
            -- CTE 5: source_rows_with_aggregates
            v_sql := $SQL$
                CREATE TEMP TABLE source_rows_with_aggregates ON COMMIT DROP AS
                SELECT
                    source_row_id,
                    count(DISTINCT discovered_stable_pk_payload) as match_count,
                    jsonb_agg(DISTINCT discovered_stable_pk_payload) as conflicting_ids
                FROM source_rows_with_matches
                GROUP BY source_row_id
            $SQL$;
            v_plan_sqls := v_plan_sqls || v_sql;
    
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
            v_plan_sqls := v_plan_sqls || v_sql;
    
            -- CTE 6: source_rows
            v_sql := format($SQL$
                CREATE TEMP TABLE source_rows ON COMMIT DROP AS
                SELECT DISTINCT ON (p.source_row_id)
                    p.source_row_id, p.causal_id, p.valid_from, p.valid_until, p.data_payload, p.ephemeral_payload,
                    (p.stable_identity_columns_are_null AND p.discovered_stable_pk_payload IS NULL) as stable_identity_columns_are_null,
                    p.natural_identity_column_values_are_null, p.is_identifiable,
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
            v_plan_sqls := v_plan_sqls || v_sql;
    
            -- CTE 8: source_rows_with_new_flag
            v_sql := $SQL$
                CREATE TEMP TABLE source_rows_with_new_flag ON COMMIT DROP AS
                SELECT *, NOT target_entity_exists as is_new_entity
                FROM source_rows
            $SQL$;
            v_plan_sqls := v_plan_sqls || v_sql;
            
            -- CTE 9.1: source_rows_with_nk_json
            v_sql := format($SQL$
                CREATE TEMP TABLE source_rows_with_nk_json ON COMMIT DROP AS
                SELECT source_row.*, %1$s /* v_lookup_keys_as_jsonb_expr */ as nk_json, %2$s /* v_lookup_keys_as_array_expr */ as nk_non_null_keys_array
                FROM source_rows_with_new_flag source_row
            $SQL$,
                v_lookup_keys_as_jsonb_expr, /* %1$s */
                v_lookup_keys_as_array_expr  /* %2$s */
            );
            v_plan_sqls := v_plan_sqls || v_sql;
    
            -- CTE 9.2: source_rows_with_canonical_key
            v_sql := format($SQL$
                CREATE TEMP TABLE source_rows_with_canonical_key ON COMMIT DROP AS
                SELECT *, (%1$s /* v_grouping_key_expr */) as grouping_key
                FROM (
                    SELECT
                        s1.*,
                        s2.nk_json as canonical_nk_json
                    FROM source_rows_with_nk_json s1
                    LEFT JOIN LATERAL (
                        SELECT s2_inner.nk_json, s2_inner.nk_non_null_keys_array
                        FROM source_rows_with_nk_json s2_inner
                        WHERE s1.is_new_entity AND s2_inner.is_new_entity AND s2_inner.nk_json @> s1.nk_json
                        ORDER BY array_length(s2_inner.nk_non_null_keys_array, 1) DESC, s2_inner.nk_non_null_keys_array::text DESC
                        LIMIT 1
                    ) s2 ON true
                ) s
            $SQL$,
                v_grouping_key_expr /* %1$s */
            );
            v_plan_sqls := v_plan_sqls || v_sql;
            
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
            v_plan_sqls := v_plan_sqls || v_sql;
            
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
            v_plan_sqls := v_plan_sqls || v_sql;
    
            -- CTE 10: all_rows
            v_sql := format($SQL$
                CREATE TEMP TABLE all_rows ON COMMIT DROP AS
                SELECT %1$s /* v_non_temporal_lookup_cols_select_list_no_alias_prefix */ causal_id, valid_from, valid_until, is_new_entity, stable_pk_payload, stable_identity_columns_are_null, natural_identity_column_values_are_null, is_identifiable, is_ambiguous, conflicting_ids, temporal_columns_are_consistent, canonical_nk_json FROM active_source_rows
                UNION ALL
                SELECT
                    %2$s /* v_non_temporal_tr_qualified_lookup_cols_prefix */
                    target_row.causal_id,
                    target_row.valid_from,
                    target_row.valid_until,
                    false as is_new_entity,
                    target_row.stable_pk_payload,
                    false as stable_identity_columns_are_null,
                    false as natural_identity_column_values_are_null,
                    true as is_identifiable,
                    false as is_ambiguous,
                    NULL::jsonb as conflicting_ids,
                    true as temporal_columns_are_consistent,
                    target_row.canonical_nk_json
                FROM target_rows target_row
            $SQL$,
                v_non_temporal_lookup_cols_select_list_no_alias_prefix, /* %1$s */
                v_non_temporal_tr_qualified_lookup_cols_prefix          /* %2$s */
            );
            v_plan_sqls := v_plan_sqls || v_sql;
    
            -- CTE 11: time_points_raw
            v_sql := format($SQL$
                CREATE TEMP TABLE time_points_raw ON COMMIT DROP AS
                SELECT %1$s, causal_id, valid_from AS point, is_new_entity, stable_pk_payload, stable_identity_columns_are_null, natural_identity_column_values_are_null, is_identifiable, is_ambiguous, conflicting_ids, canonical_nk_json FROM all_rows
                UNION ALL
                SELECT %1$s, causal_id, valid_until AS point, is_new_entity, stable_pk_payload, stable_identity_columns_are_null, natural_identity_column_values_are_null, is_identifiable, is_ambiguous, conflicting_ids, canonical_nk_json FROM all_rows
            $SQL$,
                v_lookup_cols_select_list_no_alias /* %1$s */
            );
            v_plan_sqls := v_plan_sqls || v_sql;
            
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
            v_plan_sqls := v_plan_sqls || v_sql;
    
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
                    tpu.natural_identity_column_values_are_null,
                    tpu.is_identifiable,
                    tpu.is_ambiguous,
                    tpu.conflicting_ids,
                    tpu.unified_canonical_nk_json as canonical_nk_json
                FROM time_points_unified tpu
            $SQL$,
                v_unified_id_cols_projection /* %1$s */
            );
            v_plan_sqls := v_plan_sqls || v_sql;
    
            -- CTE 13: time_points
            v_sql := $SQL$
                CREATE TEMP TABLE time_points ON COMMIT DROP AS
                SELECT DISTINCT ON (grouping_key, point) *
                FROM time_points_with_unified_ids
                ORDER BY grouping_key, point, causal_id DESC NULLS LAST
            $SQL$;
            v_plan_sqls := v_plan_sqls || v_sql;
    
            -- CTE 14: atomic_segments
            v_sql := format($SQL$
                CREATE TEMP TABLE atomic_segments ON COMMIT DROP AS
                SELECT grouping_key, %1$s, causal_id, point as valid_from, next_point as valid_until, is_new_entity, stable_pk_payload, stable_identity_columns_are_null, natural_identity_column_values_are_null, is_identifiable, is_ambiguous, conflicting_ids, canonical_nk_json
                FROM (
                    SELECT *, LEAD(point) OVER (PARTITION BY grouping_key ORDER BY point) as next_point
                    FROM time_points
                ) with_lead
                WHERE point IS NOT NULL AND next_point IS NOT NULL AND point < next_point
            $SQL$,
                v_lookup_cols_select_list_no_alias /* %1$s */
            );
            v_plan_sqls := v_plan_sqls || v_sql;
            
            -- CTE 15: resolved_atomic_segments_with_payloads
            v_sql := format($SQL$
                CREATE TEMP TABLE resolved_atomic_segments_with_payloads ON COMMIT DROP AS
                SELECT
                    with_base_payload.*,
                    with_base_payload.stable_pk_payload as propagated_stable_pk_payload
                FROM (
                    SELECT
                        seg.grouping_key, seg.canonical_nk_json, %1$s, seg.is_new_entity, seg.is_identifiable, seg.is_ambiguous, seg.conflicting_ids, seg.stable_identity_columns_are_null, seg.natural_identity_column_values_are_null, seg.valid_from, seg.valid_until,
                        target_payloads.t_valid_from, target_payloads.t_valid_until, source_payloads.source_row_id, source_payloads.contributing_row_ids, source_payloads.data_payload as s_data_payload, source_payloads.ephemeral_payload as s_ephemeral_payload,
                        target_payloads.data_payload as t_data_payload, target_payloads.ephemeral_payload as t_ephemeral_payload, seg.stable_pk_payload, source_payloads.causal_id as s_causal_id, source_payloads.causal_id as direct_source_causal_id,
                        source_payloads.valid_from AS s_valid_from, source_payloads.valid_until AS s_valid_until, %2$s
                    FROM atomic_segments seg
                    LEFT JOIN LATERAL (
                        SELECT target_row.data_payload, target_row.ephemeral_payload, target_row.valid_from as t_valid_from, target_row.valid_until as t_valid_until, target_row.stable_pk_payload
                        FROM target_rows target_row
                        WHERE %3$s AND %4$I(seg.valid_from, seg.valid_until) <@ %4$I(target_row.valid_from, target_row.valid_until)
                    ) target_payloads ON true
                    %5$s
                    WHERE (source_payloads.data_payload IS NOT NULL OR target_payloads.data_payload IS NOT NULL)
                    AND CASE %6$L::sql_saga.temporal_merge_mode
                        WHEN 'PATCH_FOR_PORTION_OF' THEN target_payloads.data_payload IS NOT NULL
                        WHEN 'REPLACE_FOR_PORTION_OF' THEN target_payloads.data_payload IS NOT NULL
                        WHEN 'DELETE_FOR_PORTION_OF' THEN target_payloads.data_payload IS NOT NULL
                        WHEN 'UPDATE_FOR_PORTION_OF' THEN target_payloads.data_payload IS NOT NULL
                        ELSE true
                    END
                ) with_base_payload
            $SQL$,
                v_entity_key_for_with_base_payload_expr, /* %1$s */
                v_trace_seed_expr,                       /* %2$s */
                v_lateral_join_tr_to_seg,                /* %3$s */
                v_range_constructor,                     /* %4$I */
                v_lateral_source_resolver_sql,           /* %5$s */
                mode                                     /* %6$L */
            );
            v_plan_sqls := v_plan_sqls || v_sql;
    
            v_resolver_from := 'resolved_atomic_segments_with_payloads';
    
            IF delete_mode IN ('DELETE_MISSING_ENTITIES', 'DELETE_MISSING_TIMELINE_AND_ENTITIES') THEN
                v_sql := format($$
                    CREATE TEMP TABLE resolved_atomic_segments_with_flag ON COMMIT DROP AS
                    SELECT *, bool_or(s_data_payload IS NOT NULL) OVER (PARTITION BY grouping_key) as entity_is_in_source
                    FROM %s
                $$, v_resolver_from);
                v_plan_sqls := v_plan_sqls || v_sql;
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
                    ) as propagated_contributing_row_ids,
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
            v_plan_sqls := v_plan_sqls || v_sql;
    
            -- CTE 16: resolved_atomic_segments
            v_sql := format($SQL$
                CREATE TEMP TABLE resolved_atomic_segments ON COMMIT DROP AS
                SELECT
                    grouping_key, %1$s stable_identity_columns_are_null, natural_identity_column_values_are_null, is_new_entity, is_identifiable, is_ambiguous, conflicting_ids, unified_canonical_nk_json,
                    valid_from, valid_until, t_valid_from, t_valid_until, propagated_s_valid_from, propagated_s_valid_until, propagated_contributing_row_ids, causal_id, propagated_stable_pk_payload as stable_pk_payload,
                    propagated_contributing_row_ids IS NULL AS unaffected_target_only_segment,
                    s_data_payload, t_data_payload,
                    sql_saga.get_allen_relation(propagated_s_valid_from, propagated_s_valid_until, t_valid_from, t_valid_until) AS s_t_relation,
                    %2$s as data_payload,
                    md5((jsonb_strip_nulls(%2$s))::text) as data_hash,
                    COALESCE(t_ephemeral_payload, '{}'::jsonb) || COALESCE(s_ephemeral_payload, '{}'::jsonb) as ephemeral_payload,
                    CASE WHEN %3$L::boolean
                        THEN trace || jsonb_build_object( 'cte', 'ras', 'propagated_stable_pk_payload', propagated_stable_pk_payload, 'final_data_payload', %2$s, 'final_ephemeral_payload', COALESCE(t_ephemeral_payload, '{}'::jsonb) || COALESCE(s_ephemeral_payload, '{}'::jsonb) )
                        ELSE NULL
                    END as trace,
                    CASE WHEN s_data_payload IS NOT NULL THEN 1 ELSE 2 END as priority
                FROM resolved_atomic_segments_with_propagated_ids
            $SQL$,
                v_lookup_cols_sans_valid_from_prefix, /* %1$s */
                v_final_data_payload_expr,            /* %2$s */
                p_log_trace                           /* %3$L */
            );
            v_plan_sqls := v_plan_sqls || v_sql;
    
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
            v_plan_sqls := v_plan_sqls || v_sql;
    
            -- CTE 17.5: coalesced_final_segments
            v_sql := format($SQL$
                CREATE TEMP TABLE coalesced_final_segments ON COMMIT DROP AS
                SELECT
                    grouping_key, %1$s,
                    sql_saga.first(causal_id ORDER BY valid_from) as causal_id,
                    sql_saga.first(stable_identity_columns_are_null ORDER BY valid_from) as stable_identity_columns_are_null,
                    sql_saga.first(natural_identity_column_values_are_null ORDER BY valid_from) as natural_identity_column_values_are_null,
                    sql_saga.first(is_new_entity ORDER BY valid_from) as is_new_entity,
                    sql_saga.first(is_identifiable ORDER BY valid_from) as is_identifiable,
                    sql_saga.first(is_ambiguous ORDER BY valid_from) as is_ambiguous,
                    sql_saga.first(conflicting_ids ORDER BY valid_from) as conflicting_ids,
                    sql_saga.first(unified_canonical_nk_json ORDER BY valid_from) as canonical_nk_json,
                    sql_saga.first(s_t_relation ORDER BY valid_from) as s_t_relation,
                    sql_saga.first(t_valid_from ORDER BY valid_from) as ancestor_valid_from,
                    MIN(valid_from) as valid_from,
                    MAX(valid_until) as valid_until,
                    %2$s as data_payload,
                    sql_saga.first(stable_pk_payload ORDER BY valid_from DESC) as stable_pk_payload,
                    bool_and(unaffected_target_only_segment) as unaffected_target_only_segment,
                    (SELECT array_agg(DISTINCT e) FROM unnest(array_concat_agg(propagated_contributing_row_ids)) e WHERE e IS NOT NULL) as row_ids,
                    CASE WHEN %3$L::boolean
                        THEN jsonb_build_object( 'cte', 'coalesced', 'island_group_id', island_group_id, 'coalesced_stable_pk_payload', sql_saga.first(stable_pk_payload ORDER BY valid_from DESC), 'final_payload', sql_saga.first(data_payload ORDER BY valid_from DESC), 'final_payload_sans_ephemeral', sql_saga.first(data_payload - %4$L::text[] ORDER BY valid_from DESC), 'atomic_traces', jsonb_agg((trace || jsonb_build_object('data_hash', data_hash, 'prev_data_hash', prev_data_hash, 'prev_data_payload', prev_data_payload)) ORDER BY valid_from) )
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
            v_plan_sqls := v_plan_sqls || v_sql;
    
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
                    COALESCE(final_seg.natural_identity_column_values_are_null, false) as natural_identity_column_values_are_null,
                    final_seg.valid_from AS f_from, final_seg.valid_until AS f_until, final_seg.data_payload AS f_data, final_seg.row_ids AS f_row_ids, final_seg.stable_pk_payload, final_seg.s_t_relation,
                    CASE WHEN %2$L::boolean
                        THEN final_seg.trace || jsonb_build_object('cte', 'diff', 'diff_stable_pk_payload', final_seg.stable_pk_payload, 'final_seg_causal_id', final_seg.causal_id, 'final_payload_vs_target_payload', jsonb_build_object('f', final_seg.data_payload, 't', target_seg.data_payload))
                        ELSE NULL
                    END as trace,
                    final_seg.unaffected_target_only_segment,
                    target_seg.valid_from as t_from, target_seg.valid_until as t_until, (target_seg.data_payload || target_seg.ephemeral_payload) as t_data,
                    sql_saga.get_allen_relation(target_seg.valid_from, target_seg.valid_until, final_seg.valid_from, final_seg.valid_until) as b_a_relation
                FROM coalesced_final_segments AS final_seg
                FULL OUTER JOIN target_rows AS target_seg ON %3$s
            $SQL$,
                v_diff_select_expr,    /* %1$s */
                p_log_trace,           /* %2$L */
                v_diff_join_condition  /* %3$s */
            );
            v_plan_sqls := v_plan_sqls || v_sql;
            
            -- CTE 19: diff_ranked
            v_sql := format($SQL$
                CREATE TEMP TABLE diff_ranked ON COMMIT DROP AS
                SELECT
                    d.*,
                    CASE
                        WHEN d.t_from IS NULL OR d.f_from IS NULL THEN NULL
                        WHEN d.f_from = d.t_from AND d.f_until = d.t_until AND d.f_data IS NOT DISTINCT FROM d.t_data THEN NULL
                        ELSE
                            row_number() OVER (
                                PARTITION BY d.grouping_key, d.t_from
                                ORDER BY
                                    CASE WHEN d.f_from = d.t_from THEN 1 ELSE 2 END,
                                    CASE WHEN d.f_data - %1$L::text[] IS NOT DISTINCT FROM d.t_data - %1$L::text[] THEN 1 ELSE 2 END,
                                    d.f_from,
                                    d.f_until
                            )
                    END as update_rank
                FROM diff d
            $SQL$,
                v_ephemeral_columns /* %1$L */
            );
            v_plan_sqls := v_plan_sqls || v_sql;
            
            -- CTE 20: plan_with_op
            v_sql := format($SQL$
                CREATE TEMP TABLE plan_with_op ON COMMIT DROP AS
                (
                    SELECT * FROM (
                        SELECT
                            d.f_row_ids as row_ids, d.s_t_relation, d.is_new_entity,
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
                                ELSE d.f_data
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
                        WHERE d.f_row_ids IS NOT NULL OR d.t_data IS NOT NULL
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
            v_plan_sqls := v_plan_sqls || v_sql;
    
            -- CTE 21: plan
            v_sql := format($SQL$
                CREATE TEMP TABLE plan ON COMMIT DROP AS
                SELECT
                    p.row_ids, p.operation, p.causal_id, p.is_new_entity,
                    %1$s,
                    p.entity_keys_json as entity_keys,
                    p.identity_keys, p.lookup_keys,
                    p.s_t_relation, p.b_a_relation, p.old_valid_from, p.old_valid_until,
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
            v_plan_sqls := v_plan_sqls || v_sql;
    
            -- Final SELECT
            v_sql := format($SQL$
                SELECT
                    row_number() OVER ( ORDER BY p.grouping_key, %1$s, CASE p.operation WHEN 'INSERT' THEN 1 WHEN 'UPDATE' THEN 2 WHEN 'DELETE' THEN 3 ELSE 4 END, p.update_effect NULLS FIRST, COALESCE(p.old_valid_from, p.new_valid_from), COALESCE(p.new_valid_from, p.old_valid_from), (p.row_ids[1]) )::BIGINT as plan_op_seq,
                    p.row_ids, p.operation, p.update_effect, p.causal_id::TEXT, p.is_new_entity, p.entity_keys, p.identity_keys, p.lookup_keys, p.s_t_relation, p.b_a_relation, p.old_valid_from::TEXT,
                    p.old_valid_until::TEXT, p.new_valid_from::TEXT, p.new_valid_until::TEXT, p.data, p.feedback, CASE WHEN p.trace IS NOT NULL THEN p.trace || jsonb_build_object('final_grouping_key', p.grouping_key) ELSE NULL END, p.grouping_key
                FROM plan p
                ORDER BY plan_op_seq;
            $SQL$,
                v_final_order_by_expr /* %1$s */
            );
            v_plan_sqls := v_plan_sqls || v_sql;
    
            -- Conditionally log the generated SQL for debugging.
            BEGIN
                IF p_log_sql THEN
                    RAISE NOTICE '--- temporal_merge SQL for % ---', temporal_merge_plan.target_table;
                    RAISE NOTICE '%', v_sql;
                END IF;
            END;
    
            INSERT INTO pg_temp.temporal_merge_plan_cache (cache_key, plan_sqls)
            VALUES (v_plan_key_text, v_plan_sqls);
        END;
    END IF; -- IF NOT FOUND THEN -- PREPARE all sql's

    -- Delete any temp tables that are created by the prepared statments.
    CALL sql_saga.temporal_merge_drop_temp_tables();
    -- Execute all statements in the plan. The first N-1 are CREATE TEMP TABLE,
    -- and the last one is the final SELECT.
    FOR i IN 1 .. (cardinality(v_plan_sqls) - 1) LOOP
        EXECUTE v_plan_sqls[i];
    END LOOP;

    RETURN QUERY EXECUTE v_plan_sqls[cardinality(v_plan_sqls)];
END;
$temporal_merge_plan$;

COMMENT ON FUNCTION sql_saga.temporal_merge_plan IS
'Generates a set-based execution plan for a temporal merge. This function is marked VOLATILE because it uses a temporary table to cache its expensive planning query for the duration of the session, which is a side-effect not permitted in STABLE or IMMUTABLE functions.';

CREATE OR REPLACE PROCEDURE sql_saga.temporal_merge_drop_temp_tables()
LANGUAGE plpgsql AS $procedure$
BEGIN
    IF to_regclass('pg_temp.source_initial') IS NOT NULL THEN DROP TABLE pg_temp.source_initial; END IF;
    IF to_regclass('pg_temp.source_with_eclipsed_flag') IS NOT NULL THEN DROP TABLE pg_temp.source_with_eclipsed_flag; END IF;
    IF to_regclass('pg_temp.target_rows') IS NOT NULL THEN DROP TABLE pg_temp.target_rows; END IF;
    IF to_regclass('pg_temp.source_rows_with_matches') IS NOT NULL THEN DROP TABLE pg_temp.source_rows_with_matches; END IF;
    IF to_regclass('pg_temp.source_rows_with_aggregates') IS NOT NULL THEN DROP TABLE pg_temp.source_rows_with_aggregates; END IF;
    IF to_regclass('pg_temp.source_rows_with_discovery') IS NOT NULL THEN DROP TABLE pg_temp.source_rows_with_discovery; END IF;
    IF to_regclass('pg_temp.source_rows') IS NOT NULL THEN DROP TABLE pg_temp.source_rows; END IF;
    IF to_regclass('pg_temp.source_rows_with_new_flag') IS NOT NULL THEN DROP TABLE pg_temp.source_rows_with_new_flag; END IF;
    IF to_regclass('pg_temp.source_rows_with_nk_json') IS NOT NULL THEN DROP TABLE pg_temp.source_rows_with_nk_json; END IF;
    IF to_regclass('pg_temp.source_rows_with_canonical_key') IS NOT NULL THEN DROP TABLE pg_temp.source_rows_with_canonical_key; END IF;
    IF to_regclass('pg_temp.source_rows_with_early_feedback') IS NOT NULL THEN DROP TABLE pg_temp.source_rows_with_early_feedback; END IF;
    IF to_regclass('pg_temp.active_source_rows') IS NOT NULL THEN DROP TABLE pg_temp.active_source_rows; END IF;
    IF to_regclass('pg_temp.all_rows') IS NOT NULL THEN DROP TABLE pg_temp.all_rows; END IF;
    IF to_regclass('pg_temp.time_points_raw') IS NOT NULL THEN DROP TABLE pg_temp.time_points_raw; END IF;
    IF to_regclass('pg_temp.time_points_unified') IS NOT NULL THEN DROP TABLE pg_temp.time_points_unified; END IF;
    IF to_regclass('pg_temp.time_points_with_unified_ids') IS NOT NULL THEN DROP TABLE pg_temp.time_points_with_unified_ids; END IF;
    IF to_regclass('pg_temp.time_points') IS NOT NULL THEN DROP TABLE pg_temp.time_points; END IF;
    IF to_regclass('pg_temp.atomic_segments') IS NOT NULL THEN DROP TABLE pg_temp.atomic_segments; END IF;
    IF to_regclass('pg_temp.resolved_atomic_segments_with_payloads') IS NOT NULL THEN DROP TABLE pg_temp.resolved_atomic_segments_with_payloads; END IF;
    IF to_regclass('pg_temp.resolved_atomic_segments_with_flag') IS NOT NULL THEN DROP TABLE pg_temp.resolved_atomic_segments_with_flag; END IF;
    IF to_regclass('pg_temp.resolved_atomic_segments_with_propagated_ids') IS NOT NULL THEN DROP TABLE pg_temp.resolved_atomic_segments_with_propagated_ids; END IF;
    IF to_regclass('pg_temp.resolved_atomic_segments') IS NOT NULL THEN DROP TABLE pg_temp.resolved_atomic_segments; END IF;
    IF to_regclass('pg_temp.island_group') IS NOT NULL THEN DROP TABLE pg_temp.island_group; END IF;
    IF to_regclass('pg_temp.coalesced_final_segments') IS NOT NULL THEN DROP TABLE pg_temp.coalesced_final_segments; END IF;
    IF to_regclass('pg_temp.diff') IS NOT NULL THEN DROP TABLE pg_temp.diff; END IF;
    IF to_regclass('pg_temp.diff_ranked') IS NOT NULL THEN DROP TABLE pg_temp.diff_ranked; END IF;
    IF to_regclass('pg_temp.plan_with_op') IS NOT NULL THEN DROP TABLE pg_temp.plan_with_op; END IF;
    IF to_regclass('pg_temp.plan') IS NOT NULL THEN DROP TABLE pg_temp.plan; END IF;
END;
$procedure$;

COMMENT ON PROCEDURE sql_saga.temporal_merge_drop_temp_tables IS
'Drops all temporary tables created by the temporal_merge_plan function. This is used before executing the cached prepared statements that create them.';
