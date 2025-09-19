-- Unified Planning Function
CREATE OR REPLACE FUNCTION sql_saga.temporal_merge_plan(
    target_table regclass,
    source_table regclass,
    identity_columns TEXT[],
    mode sql_saga.temporal_merge_mode,
    era_name name,
    row_id_column name DEFAULT 'row_id',
    identity_correlation_column name DEFAULT NULL,
    delete_mode sql_saga.temporal_merge_delete_mode DEFAULT 'NONE',
    natural_identity_columns TEXT[] DEFAULT NULL,
    ephemeral_columns TEXT[] DEFAULT NULL,
    p_log_trace BOOLEAN DEFAULT false,
    p_log_sql BOOLEAN DEFAULT false
) RETURNS SETOF sql_saga.temporal_merge_plan
LANGUAGE plpgsql VOLATILE AS $temporal_merge_plan$
DECLARE
    v_log_vars BOOLEAN;
    v_log_id TEXT;
    v_stable_identity_columns TEXT[];
    v_lookup_columns TEXT[];
    v_all_id_cols TEXT[];
    v_plan_key_text TEXT;
    v_plan_ps_name TEXT;
    v_target_schema_name name;
    v_target_table_name_only name;
    v_range_constructor name;
    v_valid_from_col name;
    v_valid_until_col name;
    v_valid_to_col name;
    v_range_col name;
    v_sql TEXT;
    v_ephemeral_columns TEXT[] := COALESCE(temporal_merge_plan.ephemeral_columns, '{}'::text[]);
BEGIN
    v_log_vars := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_vars', true), ''), 'false')::boolean;
    v_log_id := substr(md5(COALESCE(current_setting('sql_saga.temporal_merge.log_id_seed', true), random()::text)), 1, 3);
    -- An entity must be identifiable. At least one set of identity columns must be provided.
    IF (temporal_merge_plan.identity_columns IS NULL OR cardinality(temporal_merge_plan.identity_columns) = 0) AND
       (temporal_merge_plan.natural_identity_columns IS NULL OR cardinality(temporal_merge_plan.natural_identity_columns) = 0)
    THEN
        RAISE EXCEPTION 'At least one of identity_columns or natural_identity_columns must be a non-empty array.';
    END IF;

    -- Use natural_identity_columns for lookups if provided, otherwise fall back to the stable identity_columns.
    v_lookup_columns := COALESCE(temporal_merge_plan.natural_identity_columns, temporal_merge_plan.identity_columns);
    -- The stable identity is always identity_columns if provided; otherwise, it's the natural key.
    v_stable_identity_columns := COALESCE(temporal_merge_plan.identity_columns, temporal_merge_plan.natural_identity_columns);

    -- Get the distinct, ordered union of all identity-related columns. This is critical for ensuring that
    -- all necessary columns are projected through the CTEs for partitioning and grouping.
    SELECT array_agg(DISTINCT c ORDER BY c) INTO v_all_id_cols FROM (
        SELECT unnest(v_lookup_columns)
        UNION
        SELECT unnest(v_stable_identity_columns)
    ) AS t(c);

    -- Validate that provided column names exist.
    PERFORM 1 FROM pg_attribute WHERE attrelid = temporal_merge_plan.source_table AND attname = temporal_merge_plan.row_id_column AND NOT attisdropped AND attnum > 0;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'row_id_column "%" does not exist in source table %s', temporal_merge_plan.row_id_column, temporal_merge_plan.source_table::text;
    END IF;

    IF temporal_merge_plan.identity_correlation_column IS NOT NULL THEN
        PERFORM 1 FROM pg_attribute WHERE attrelid = temporal_merge_plan.source_table AND attname = temporal_merge_plan.identity_correlation_column AND NOT attisdropped AND attnum > 0;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'identity_correlation_column "%" does not exist in source table %s', temporal_merge_plan.identity_correlation_column, temporal_merge_plan.source_table::text;
        END IF;
    END IF;

    -- Introspect just enough to get the range constructor, which is part of the cache key.
    SELECT n.nspname, c.relname
    INTO v_target_schema_name, v_target_table_name_only
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = temporal_merge_plan.target_table;

    SELECT e.range_type::name, e.valid_from_column_name, e.valid_until_column_name, e.synchronize_valid_to_column, e.synchronize_range_column
    INTO v_range_constructor, v_valid_from_col, v_valid_until_col, v_valid_to_col, v_range_col
    FROM sql_saga.era e
    WHERE e.table_schema = v_target_schema_name
      AND e.table_name = v_target_table_name_only
      AND e.era_name = temporal_merge_plan.era_name;

    IF v_valid_from_col IS NULL THEN
        RAISE EXCEPTION 'No era named "%" found for table "%"', temporal_merge_plan.era_name, temporal_merge_plan.target_table;
    END IF;

    IF v_range_constructor IS NULL THEN
        RAISE EXCEPTION 'Could not determine the range type for era "%" on table "%". Please ensure the era was created correctly, possibly by specifying the range_type parameter in add_era().', temporal_merge_plan.era_name, temporal_merge_plan.target_table;
    END IF;

    -- Validate that user has not passed in temporal columns as ephemeral.
    IF v_valid_from_col = ANY(v_ephemeral_columns) OR v_valid_until_col = ANY(v_ephemeral_columns) THEN
        RAISE EXCEPTION 'Temporal boundary columns ("%", "%") cannot be specified in ephemeral_columns.', v_valid_from_col, v_valid_until_col;
    END IF;
    IF v_valid_to_col IS NOT NULL AND v_valid_to_col = ANY(v_ephemeral_columns) THEN
        RAISE EXCEPTION 'Synchronized column "%" is automatically handled and should not be specified in ephemeral_columns.', v_valid_to_col;
    END IF;
    IF v_range_col IS NOT NULL AND v_range_col = ANY(v_ephemeral_columns) THEN
        RAISE EXCEPTION 'Synchronized column "%" is automatically handled and should not be specified in ephemeral_columns.', v_range_col;
    END IF;

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

    -- Generate the cache key first from all relevant arguments. This is fast.
    v_plan_key_text := format('%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s',
        temporal_merge_plan.target_table::oid,
        temporal_merge_plan.source_table::oid,
        temporal_merge_plan.identity_columns,
        v_ephemeral_columns,
        temporal_merge_plan.mode,
        temporal_merge_plan.era_name,
        temporal_merge_plan.row_id_column,
        COALESCE(temporal_merge_plan.identity_correlation_column, ''),
        v_range_constructor,
        temporal_merge_plan.delete_mode,
        COALESCE(temporal_merge_plan.natural_identity_columns, '{}'::text[]),
        p_log_trace
    );
    v_plan_ps_name := 'tm_plan_' || md5(v_plan_key_text);

    -- If the prepared statement already exists, execute it and exit immediately.
    IF EXISTS (SELECT 1 FROM pg_prepared_statements WHERE name = v_plan_ps_name) THEN
        v_sql := format('EXECUTE %I', v_plan_ps_name);
        RETURN QUERY EXECUTE v_sql;
        RETURN;
    END IF;

    -- On cache miss, enter a new block to declare variables and do the expensive work.
    DECLARE
        v_sql TEXT;
        v_exec_sql TEXT;
        v_trace_seed_expr TEXT;
        v_source_data_cols_jsonb_build TEXT;
        v_source_ephemeral_cols_jsonb_build TEXT;
        v_target_data_cols_jsonb_build TEXT;
        v_target_ephemeral_cols_jsonb_build TEXT;
        v_target_ephemeral_cols_jsonb_build_bare TEXT;
        v_target_data_cols_jsonb_build_bare TEXT;
        v_entity_id_as_jsonb TEXT;
        v_lookup_cols_select_list TEXT;
        v_lookup_cols_select_list_no_alias TEXT;
        v_source_schema_name TEXT;
        v_source_table_ident TEXT;
        v_target_table_ident TEXT;
        v_source_data_payload_expr TEXT;
        v_final_data_payload_expr TEXT;
        v_resolver_ctes TEXT;
        v_resolver_from TEXT;
        v_diff_join_condition TEXT;
        v_entity_id_check_is_null_expr TEXT;
        v_ident_corr_select_expr TEXT;
        v_planner_entity_id_expr TEXT;
        v_target_rows_filter TEXT;
        v_stable_pk_cols_jsonb_build TEXT;
        v_join_on_lookup_cols TEXT;
        v_lateral_join_sr_to_seg TEXT;
        v_lateral_join_tr_to_seg TEXT;
        v_lookup_cols_si_alias_select_list TEXT;
        v_non_temporal_lookup_cols_select_list TEXT;
        v_non_temporal_lookup_cols_select_list_no_alias TEXT;
        v_non_temporal_tr_qualified_lookup_cols TEXT;
        v_lookup_cols_sans_valid_from TEXT;
        v_lookup_cols_select_list_no_alias_sans_vf TEXT;
        v_entity_partition_key_cols TEXT;
        v_correlation_col name;
        v_trace_select_list TEXT;
        v_target_rows_lookup_cols_expr TEXT;
        v_source_rows_exists_join_expr TEXT;
        v_atomic_segments_select_list_expr TEXT;
        v_diff_select_expr TEXT;
        v_plan_with_op_entity_id_json_build_expr TEXT;
        v_skip_no_target_entity_id_json_build_expr TEXT;
        v_final_order_by_expr TEXT;
        v_partition_key_for_with_base_payload_expr TEXT;
        v_s_founding_join_condition TEXT;
        v_tr_qualified_lookup_cols TEXT;
        v_ident_corr_column_type regtype;
        v_stable_id_aggregates_expr TEXT;
        v_stable_id_projection_expr TEXT;
        v_stable_pk_cols_jsonb_build_bare TEXT;
        v_rcte_join_condition TEXT;
        v_unqualified_id_cols_sans_vf TEXT;
        v_qualified_r_id_cols_sans_vf TEXT;
        v_entity_id_check_is_null_expr_no_alias TEXT;
        v_coalesced_payload_expr TEXT;
        v_entity_partition_key_cols_prefix TEXT;
        v_non_temporal_lookup_cols_select_list_no_alias_prefix TEXT;
        v_stable_id_projection_expr_prefix TEXT;
        v_non_temporal_lookup_cols_select_list_prefix TEXT;
        v_non_temporal_tr_qualified_lookup_cols_prefix TEXT;
        v_lookup_cols_sans_valid_from_prefix TEXT;
    BEGIN
        -- On cache miss, proceed with the expensive introspection and query building.
        v_correlation_col := COALESCE(temporal_merge_plan.identity_correlation_column, temporal_merge_plan.row_id_column);

        IF v_correlation_col IS NULL THEN
            RAISE EXCEPTION 'The correlation identifier column cannot be NULL. Please provide a non-NULL value for either identity_correlation_column or row_id_column.';
        END IF;

        SELECT atttypid INTO v_ident_corr_column_type
        FROM pg_attribute
        WHERE attrelid = temporal_merge_plan.source_table
          AND attname = v_correlation_col
          AND NOT attisdropped AND attnum > 0;

        IF v_ident_corr_column_type IS NULL THEN
            RAISE EXCEPTION 'Correlation column "%" does not exist in source table %s', v_correlation_col, temporal_merge_plan.source_table::text;
        END IF;

        IF v_correlation_col = ANY(v_lookup_columns) THEN
             RAISE EXCEPTION 'The correlation column (%) cannot be one of the natural identity columns (%)', v_correlation_col, v_lookup_columns;
        END IF;

        IF temporal_merge_plan.identity_correlation_column IS NOT NULL THEN
            -- If an explicit correlation column is provided, it might contain NULLs.
            -- We must coalesce to the row_id_column to ensure every new entity
            -- has a unique correlation identifier. To handle different data types,
            -- we cast the row_id_column to the type of the primary correlation column.
            v_ident_corr_select_expr := format('COALESCE(t.%I, t.%I::%s) as ident_corr,',
                temporal_merge_plan.identity_correlation_column,
                temporal_merge_plan.row_id_column,
                v_ident_corr_column_type::text
            );
        ELSE
            -- If no explicit correlation column is given, the row_id_column is the correlation identifier.
            v_ident_corr_select_expr := format('t.%I as ident_corr,', temporal_merge_plan.row_id_column);
        END IF;

        -- Resolve table identifiers to be correctly quoted and schema-qualified.
        v_target_table_ident := temporal_merge_plan.target_table::TEXT;

        SELECT n.nspname INTO v_source_schema_name
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.oid = temporal_merge_plan.source_table;

        IF v_source_schema_name = 'pg_temp' THEN
            v_source_table_ident := temporal_merge_plan.source_table::regclass::TEXT;
        ELSE
            v_source_table_ident := temporal_merge_plan.source_table::regclass::TEXT;
        END IF;


        -- Construct reusable SQL fragments for entity key column lists and join conditions,
        -- since these are more index friendly than a jsonb object, that we previously had.
        SELECT
            string_agg(format('t.%I', col), ', '),
            string_agg(format('(f.%1$I = t.%1$I OR (f.%1$I IS NULL AND t.%1$I IS NULL))', col), ' AND '),
            string_agg(format('(sr.%1$I = seg.%1$I OR (sr.%1$I IS NULL AND seg.%1$I IS NULL))', col), ' AND ') || ' AND (NOT seg.is_new_entity OR sr.ident_corr = seg.ident_corr)',
            string_agg(format('(tr.%1$I = seg.%1$I OR (tr.%1$I IS NULL AND seg.%1$I IS NULL))', col), ' AND '),
            string_agg(format('si.%I', col), ', ')
        INTO
            v_lookup_cols_select_list,
            v_join_on_lookup_cols,
            v_lateral_join_sr_to_seg,
            v_lateral_join_tr_to_seg,
            v_lookup_cols_si_alias_select_list
        FROM unnest(v_lookup_columns) col;

        -- This variable MUST be built from v_all_id_cols to ensure stable IDs are projected.
        v_lookup_cols_select_list_no_alias := (SELECT string_agg(format('%I', col), ', ') FROM unnest(v_all_id_cols) col);

        -- This must be built from columns that are stable for an entity's timeline.
        -- When a natural key is provided for lookups, it serves as the basis for partitioning.
        -- The stable surrogate key (e.g., `id`) cannot be used because it may be NULL in the
        -- source for existing entities, which would cause incorrect timeline fragmentation.
        SELECT string_agg(format('%I', col), ', ')
        INTO v_entity_partition_key_cols
        FROM unnest(v_lookup_columns) col;

        v_entity_partition_key_cols := COALESCE(v_entity_partition_key_cols, '');
        v_entity_partition_key_cols_prefix := CASE WHEN v_entity_partition_key_cols = '' THEN '' ELSE v_entity_partition_key_cols || ', ' END;

        -- When the stable ID columns are not part of the partition key, they must be aggregated.
        -- A simple `max()` correctly coalesces the NULL from a source row and the non-NULL
        -- value from the corresponding target row into the single correct stable ID.
        -- A simple `max()` correctly coalesces the NULL from a source row and the non-NULL
        -- value from the corresponding target row into the single correct stable ID.
        SELECT COALESCE(string_agg(format('max(%I) as %I', col, col), ', ') || ', ', '')
        INTO v_stable_id_aggregates_expr
        FROM unnest(v_all_id_cols) c(col)
        WHERE col <> ALL(v_lookup_columns);

        SELECT COALESCE(string_agg(format('%I', col), ', '), '')
        INTO v_stable_id_projection_expr
        FROM unnest(v_all_id_cols) c(col)
        WHERE col <> ALL(v_lookup_columns);
        v_stable_id_projection_expr_prefix := CASE WHEN v_stable_id_projection_expr = '' THEN '' ELSE v_stable_id_projection_expr || ', ' END;
        
        -- Create versions of the lookup column list for SELECT clauses that exclude
        -- temporal columns, to avoid selecting them twice in various CTEs.
        v_non_temporal_lookup_cols_select_list := (
            SELECT COALESCE(string_agg(format('t.%I', col), ', '), '')
            FROM unnest(v_all_id_cols) col
            WHERE col.col NOT IN (v_valid_from_col, v_valid_until_col)
        );
        v_non_temporal_lookup_cols_select_list_prefix := CASE WHEN v_non_temporal_lookup_cols_select_list = '' THEN '' ELSE v_non_temporal_lookup_cols_select_list || ', ' END;
        v_non_temporal_lookup_cols_select_list_no_alias := (
            SELECT COALESCE(string_agg(format('%I', col), ', '), '')
            FROM unnest(v_all_id_cols) col
            WHERE col.col NOT IN (v_valid_from_col, v_valid_until_col)
        );
        v_non_temporal_lookup_cols_select_list_no_alias_prefix := CASE WHEN v_non_temporal_lookup_cols_select_list_no_alias = '' THEN '' ELSE v_non_temporal_lookup_cols_select_list_no_alias || ', ' END;
        v_non_temporal_tr_qualified_lookup_cols := (
            SELECT COALESCE(string_agg(format('tr.%I', col), ', '), '')
            FROM unnest(v_all_id_cols) col
            WHERE col.col NOT IN (v_valid_from_col, v_valid_until_col)
        );
        v_non_temporal_tr_qualified_lookup_cols_prefix := CASE WHEN v_non_temporal_tr_qualified_lookup_cols = '' THEN '' ELSE v_non_temporal_tr_qualified_lookup_cols || ', ' END;

        v_lookup_cols_sans_valid_from := (
            SELECT string_agg(format('%I', col), ', ')
            FROM unnest(v_all_id_cols) col
            WHERE col.col <> v_valid_from_col
        );
        v_lookup_cols_sans_valid_from_prefix := CASE WHEN v_lookup_cols_sans_valid_from IS NULL OR v_lookup_cols_sans_valid_from = '' THEN '' ELSE v_lookup_cols_sans_valid_from || ', ' END;
        v_lookup_cols_sans_valid_from := COALESCE(v_lookup_cols_sans_valid_from, '');

        -- Unqualified list of ID columns (e.g., "id, ident_corr") for GROUP BY and some SELECTs.
        v_unqualified_id_cols_sans_vf := (
            SELECT string_agg(format('%I', col), ', ')
            FROM unnest(v_all_id_cols) col
            WHERE col.col <> v_valid_from_col
        );
        v_unqualified_id_cols_sans_vf := CASE WHEN v_unqualified_id_cols_sans_vf IS NULL THEN 'ident_corr' ELSE v_unqualified_id_cols_sans_vf || ', ident_corr' END;

        -- Qualified list of ID columns (e.g., "r.id, r.ident_corr") for the recursive part of the CTE.
        v_qualified_r_id_cols_sans_vf := (
            SELECT string_agg(format('r.%I', col), ', ')
            FROM unnest(v_all_id_cols) col
            WHERE col.col <> v_valid_from_col
        );
        v_qualified_r_id_cols_sans_vf := CASE WHEN v_qualified_r_id_cols_sans_vf IS NULL THEN 'r.ident_corr' ELSE v_qualified_r_id_cols_sans_vf || ', r.ident_corr' END;

        -- 1. Dynamically build jsonb payload expressions for SOURCE and TARGET tables.
        -- The source payload only includes columns present in the source table.
        -- The target payload includes ALL data columns from the target table.
        WITH source_cols AS (
            SELECT pa.attname
            FROM pg_catalog.pg_attribute pa
            WHERE pa.attrelid = temporal_merge_plan.source_table AND pa.attnum > 0 AND NOT pa.attisdropped
        ),
        target_cols AS (
            SELECT pa.attname, pa.attgenerated
            FROM pg_catalog.pg_attribute pa
            WHERE pa.attrelid = temporal_merge_plan.target_table AND pa.attnum > 0 AND NOT pa.attisdropped
        ),
        source_data_cols AS (
            SELECT s.attname
            FROM source_cols s JOIN target_cols t ON s.attname = t.attname
            WHERE s.attname NOT IN (temporal_merge_plan.row_id_column, v_valid_from_col, v_valid_until_col, 'era_id', 'era_name')
              AND s.attname <> ALL(v_all_id_cols)
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
              AND t.attname <> ALL(v_all_id_cols)
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
            (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, t.%I', attname, attname), ', '), '')) FROM source_data_cols),
            (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, t.%I', attname, attname), ', '), '')) FROM source_ephemeral_cols),
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
        SELECT
            format('jsonb_build_object(%s)', string_agg(format('%L, t.%I', col, col), ', ')),
            format('jsonb_build_object(%s)', string_agg(format('%L, %I', col, col), ', '))
        INTO
            v_stable_pk_cols_jsonb_build,
            v_stable_pk_cols_jsonb_build_bare
        FROM unnest(v_stable_identity_columns) col;

        v_stable_pk_cols_jsonb_build := COALESCE(v_stable_pk_cols_jsonb_build, '''{}''::jsonb');
        v_stable_pk_cols_jsonb_build_bare := COALESCE(v_stable_pk_cols_jsonb_build_bare, '''{}''::jsonb');

        -- Construct an expression to reliably check if all stable identity columns for a source row are NULL.
        -- If so, this is a "founding" event for a new entity.
        SELECT
            COALESCE(string_agg(format('t.%I IS NULL', col), ' AND '), 'true')
        INTO
            v_entity_id_check_is_null_expr
        FROM unnest(v_stable_identity_columns) AS col;

        v_entity_id_check_is_null_expr_no_alias := (
            SELECT COALESCE(string_agg(format('%I IS NULL', col), ' AND '), 'true')
            FROM unnest(v_stable_identity_columns) AS col
        );

        -- Determine the scope of target entities to process based on the mode.
        IF temporal_merge_plan.mode IN ('MERGE_ENTITY_PATCH', 'MERGE_ENTITY_REPLACE') AND temporal_merge_plan.delete_mode IN ('DELETE_MISSING_ENTITIES', 'DELETE_MISSING_TIMELINE_AND_ENTITIES') THEN
            -- For modes that might delete entities not in the source, we must scan the entire target table.
            v_target_rows_filter := v_target_table_ident || ' t';
        ELSE
            -- By default, we optimize by only scanning target entities that are present in the source.
            DECLARE
                v_any_nullable_lookup_cols BOOLEAN;
            BEGIN
                -- Check if any of the lookup columns are nullable in either the source or target table.
                SELECT bool_or(c.is_nullable)
                INTO v_any_nullable_lookup_cols
                FROM (
                    SELECT NOT a.attnotnull as is_nullable
                    FROM unnest(v_lookup_columns) as c(attname)
                    JOIN pg_attribute a ON a.attname = c.attname AND a.attrelid = temporal_merge_plan.target_table
                    UNION ALL
                    SELECT NOT a.attnotnull as is_nullable
                    FROM unnest(v_lookup_columns) as c(attname)
                    JOIN pg_attribute a ON a.attname = c.attname AND a.attrelid = temporal_merge_plan.source_table
                ) c;

                v_any_nullable_lookup_cols := COALESCE(v_any_nullable_lookup_cols, false);

                IF NOT v_any_nullable_lookup_cols THEN
                    -- OPTIMIZED PATH: If all lookup columns are defined as NOT NULL in both tables,
                    -- we can use a simple, fast, SARGable query.
                    v_target_rows_filter := format($$(
                        SELECT * FROM %1$s inner_t
                        WHERE (%2$s) IN (SELECT DISTINCT %3$s FROM source_initial si)
                    )$$, v_target_table_ident, (SELECT string_agg(format('inner_t.%I', col), ', ') FROM unnest(v_lookup_columns) col), v_lookup_cols_si_alias_select_list);
                ELSE
                    -- NULL-SAFE PATH: If any lookup column is nullable, we separate the logic
                    -- for non-NULL and NULL keys into two queries combined by a UNION.
                    DECLARE
                        v_sargable_part TEXT;
                        v_null_part TEXT;
                        v_lookup_cols_not_null_condition TEXT;
                        v_lookup_cols_is_null_condition TEXT;
                        v_join_clause TEXT;
                        v_distinct_on_cols name[];
                        v_distinct_on_cols_list TEXT;
                    BEGIN
                        -- When a table contains columns without a default b-tree equality operator (e.g., `point`),
                        -- `SELECT DISTINCT *` can fail. We must fall back to `SELECT DISTINCT ON (...) *`.
                        -- The key for DISTINCT ON must uniquely identify rows in the target table. For a temporal table,
                        -- this is the combination of its stable entity identifier and the temporal start column.
                        v_distinct_on_cols := v_stable_identity_columns || v_valid_from_col;
                        v_distinct_on_cols_list := (SELECT string_agg(format('inner_t.%I', col), ', ') FROM unnest(v_distinct_on_cols) AS col);

                        -- Condition to identify source rows with NO NULLs in the lookup key.
                        v_lookup_cols_not_null_condition := COALESCE(
                            (SELECT string_agg(format('si.%I IS NOT NULL', col), ' AND ') FROM unnest(v_lookup_columns) AS col),
                            'true'
                        );

                        -- SARGable part for non-NULL keys using an IN clause.
                        v_sargable_part := format($$(
                            SELECT * FROM %1$s inner_t
                            WHERE (%2$s) IN (SELECT DISTINCT %3$s FROM source_initial si WHERE %4$s)
                        )$$, v_target_table_ident, (SELECT string_agg(format('inner_t.%I', col), ', ') FROM unnest(v_lookup_columns) col), v_lookup_cols_si_alias_select_list, v_lookup_cols_not_null_condition);

                        -- Condition to identify source rows with AT LEAST ONE NULL in the lookup key.
                        v_lookup_cols_is_null_condition := COALESCE(
                            (SELECT string_agg(format('si.%I IS NULL', col), ' OR ') FROM unnest(v_lookup_columns) AS col),
                            'false'
                        );

                        -- NULL-aware part for keys with NULLs. This logic distinguishes between
                        -- the common case of a single nullable key column and the more complex
                        -- case of multiple nullable key columns (like an XOR key with partial indexes).
                        DECLARE
                            v_nullable_cols NAME[];
                            v_join_clause TEXT;
                        BEGIN
                            v_nullable_cols := ARRAY(
                                SELECT c.attname FROM unnest(v_lookup_columns) as c(attname)
                                JOIN pg_attribute a ON a.attname = c.attname AND a.attrelid = temporal_merge_plan.target_table
                                WHERE NOT a.attnotnull
                            );

                            -- Heuristic: If there are multiple nullable columns, we assume it's a
                            -- complex XOR-style key that relies on partial indexes for performance.
                            -- In this case, we use a UNION ALL of queries with '=' joins, which is
                            -- known to be planner-friendly for this pattern.
                            IF array_length(v_nullable_cols, 1) > 1 THEN
                                DECLARE
                                    v_union_parts TEXT[];
                                    v_not_nullable_cols_join_clause TEXT;
                                    v_nullable_col NAME;
                                BEGIN
                                    v_not_nullable_cols_join_clause := COALESCE(
                                        (SELECT string_agg(format('si.%1$I = inner_t.%1$I', c.attname), ' AND ')
                                         FROM unnest(v_lookup_columns) c(attname)
                                         JOIN pg_attribute a ON a.attname = c.attname and a.attrelid = temporal_merge_plan.target_table
                                         WHERE a.attnotnull),
                                        'true'
                                    );

                                    v_union_parts := ARRAY[]::TEXT[];

                                    -- For each nullable column, create a query part tailored for a partial index.
                                    FOREACH v_nullable_col IN ARRAY v_nullable_cols
                                    LOOP
                                        DECLARE
                                            v_other_nullable_cols_filter TEXT;
                                        BEGIN
                                            -- Build a filter for both `t` and `si` to match the partial index condition,
                                            -- e.g., "t.es_id IS NULL AND si.es_id IS NULL"
                                            v_other_nullable_cols_filter := COALESCE(
                                                (SELECT string_agg(format('inner_t.%1$I IS NULL AND si.%1$I IS NULL', other.c), ' AND ')
                                                 FROM (SELECT unnest(v_nullable_cols) c) AS other
                                                 WHERE other.c <> v_nullable_col),
                                                'true'
                                            );

                                            v_union_parts := v_union_parts || format($$(
                                                SELECT DISTINCT ON (%5$s) inner_t.*
                                                FROM %1$s inner_t
                                                JOIN source_initial si ON (%2$s AND si.%4$I = inner_t.%4$I)
                                                WHERE %3$s
                                            )$$,
                                                v_target_table_ident,               /* %1$s */
                                                v_not_nullable_cols_join_clause,    /* %2$s */
                                                v_other_nullable_cols_filter,       /* %3$s */
                                                v_nullable_col,                     /* %4$I */
                                                v_distinct_on_cols_list                      /* %5$s */
                                            );
                                        END;
                                    END LOOP;
                                    v_null_part := array_to_string(v_union_parts, ' UNION ALL ');
                                END;
                            ELSE
                                -- For a single nullable key column (or no nullable columns, as a fallback),
                                -- we use a series of index-friendly, null-safe comparisons.
                                -- This pattern `(a = b OR (a IS NULL AND b IS NULL))` is functionally
                                -- equivalent to `a IS NOT DISTINCT FROM b` but allows the planner
                                -- to use standard B-Tree indexes, which is critical for performance.
                                v_join_clause := COALESCE(
                                    (SELECT string_agg(format('(si.%1$I = inner_t.%1$I OR (si.%1$I IS NULL AND inner_t.%1$I IS NULL))', c), ' AND ') FROM unnest(v_lookup_columns) c),
                                    'true'
                                );
                                v_null_part := format($$(
                                    SELECT DISTINCT ON (%4$s) inner_t.*
                                    FROM %1$s inner_t
                                    JOIN source_initial si ON (%2$s)
                                    WHERE %3$s
                                )$$, v_target_table_ident, v_join_clause, v_lookup_cols_is_null_condition, v_distinct_on_cols_list);
                            END IF;
                        END;

                        -- The final "filter" is a full subquery that replaces the original FROM clause for target_rows.
                        -- A UNION ALL is used for performance, and a final DISTINCT ON (primary key) is applied to
                        -- remove duplicates that may arise if a target row is matched by both the SARGable and NULL-aware parts.
                        v_target_rows_filter := format($$(
                            SELECT * FROM (
                                SELECT DISTINCT ON (%3$s) * FROM (
                                    (%1$s)
                                    UNION ALL
                                    (%2$s)
                                ) u
                            )
                        )$$, v_sargable_part, v_null_part, (SELECT string_agg(format('u.%I', col), ', ') FROM unnest(v_distinct_on_cols) AS col));
                    END;
                END IF;
            END;
        END IF;

        -- 2. Construct expressions and resolver CTEs based on the mode.
        -- This logic is structured to be orthogonal:
        -- 1. Payload Handling (_PATCH vs. _REPLACE) is determined first.
        -- 2. Timeline Handling (destructive vs. non-destructive) is determined by delete_mode.

        -- First, determine payload semantics based on the mode name.
        IF temporal_merge_plan.mode IN ('MERGE_ENTITY_PATCH', 'PATCH_FOR_PORTION_OF') THEN
            v_source_data_payload_expr := format('jsonb_strip_nulls(%s)', v_source_data_cols_jsonb_build);
        ELSIF temporal_merge_plan.mode = 'DELETE_FOR_PORTION_OF' THEN
            v_source_data_payload_expr := '''"__DELETE__"''::jsonb';
        ELSE -- MERGE_ENTITY_REPLACE, MERGE_ENTITY_UPSERT, REPLACE_FOR_PORTION_OF, UPDATE_FOR_PORTION_OF, INSERT_NEW_ENTITIES
            v_source_data_payload_expr := v_source_data_cols_jsonb_build;
        END IF;

        -- Second, determine timeline semantics. The default is a non-destructive merge where we
        -- The join condition for the diff links the final state back to the original target row
        -- it was derived from. Because t_from is never NULL, a simple '=' is sufficient and planner-friendly.
        v_diff_join_condition := format('%s AND f.ancestor_valid_from = t.t_from', v_join_on_lookup_cols);

        -- Third, determine payload resolution logic based on the mode. All modes are now stateless.
        IF temporal_merge_plan.mode IN ('MERGE_ENTITY_PATCH', 'PATCH_FOR_PORTION_OF', 'MERGE_ENTITY_UPSERT', 'UPDATE_FOR_PORTION_OF') THEN
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
        IF temporal_merge_plan.delete_mode IN ('DELETE_MISSING_TIMELINE', 'DELETE_MISSING_TIMELINE_AND_ENTITIES') THEN
             v_resolver_ctes := '';
             v_resolver_from := 'resolved_atomic_segments_with_payloads';
             v_final_data_payload_expr := 's_data_payload';
        END IF;

        -- Layer on optional destructive delete modes for missing entities.
        IF temporal_merge_plan.delete_mode IN ('DELETE_MISSING_ENTITIES', 'DELETE_MISSING_TIMELINE_AND_ENTITIES') THEN
            -- We need to add a flag to identify entities that are present in the source.
            -- This CTE is chained onto any previous resolver CTEs by using v_resolver_from.
            v_resolver_ctes := v_resolver_ctes || format($$,
    resolved_atomic_segments_with_flag AS (
        SELECT *,
            bool_or(s_data_payload IS NOT NULL) OVER (PARTITION BY %s) as entity_is_in_source
        FROM %s
    )
$$, v_entity_partition_key_cols, v_resolver_from);
            v_resolver_from := 'resolved_atomic_segments_with_flag';

            -- If deleting missing entities, the final payload for those entities must be NULL.
            -- For entities present in the source, the existing v_final_data_payload_expr is correct.
            v_final_data_payload_expr := format($$CASE WHEN entity_is_in_source THEN (%s) ELSE NULL END$$, v_final_data_payload_expr);
        END IF;

        -- Pre-calculate all expressions to be passed to format() for easier debugging.
        v_target_rows_lookup_cols_expr := (SELECT string_agg(format('t.%I', col), ', ') FROM unnest(v_lookup_columns) as col);
        v_source_rows_exists_join_expr := (SELECT string_agg(format('(si.%1$I = tr.%1$I OR (si.%1$I IS NULL AND tr.%1$I IS NULL))', col), ' AND ') FROM unnest(v_lookup_columns) as col);
        v_atomic_segments_select_list_expr := (SELECT COALESCE(string_agg(format('seg.%I', col), ', ') || ',', '') || ' seg.ident_corr' FROM unnest(v_lookup_columns) as col);
        v_diff_select_expr := (SELECT string_agg(format('COALESCE(f.%1$I, t.%1$I) as %1$I', col), ', ') || ', COALESCE(f.ident_corr, t.ident_corr) as ident_corr' FROM unnest(v_all_id_cols) as col);
        v_plan_with_op_entity_id_json_build_expr := (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, d.%I', col, col), ', '), '')) || ' || COALESCE(d.stable_pk_payload, ''{}''::jsonb)' FROM unnest(v_all_id_cols) as col);
        v_skip_no_target_entity_id_json_build_expr := (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, sr.%I', col, col), ', '), '')) FROM unnest(v_all_id_cols) as col);
        v_final_order_by_expr := (SELECT string_agg(format('p.%I', col), ', ') FROM unnest(v_all_id_cols) as col);
        v_partition_key_for_with_base_payload_expr := (
            SELECT COALESCE(string_agg(format('seg.%I', col), ', ') || ',', '') || ' seg.ident_corr'
            FROM unnest(v_all_id_cols) as col
            WHERE col.col NOT IN (v_valid_from_col, v_valid_until_col)
        );
        v_s_founding_join_condition := (SELECT string_agg(format('(tr.%1$I = sr.%1$I OR (tr.%1$I IS NULL AND sr.%1$I IS NULL))', col), ' AND ') FROM unnest(v_lookup_columns) as col);
        v_tr_qualified_lookup_cols := (SELECT string_agg(format('tr.%I', col), ', ') FROM unnest(v_lookup_columns) as col);

        -- Build the null-safe join condition for the recursive CTE.
        v_rcte_join_condition := (
            SELECT string_agg(format('(c.%1$I = r.%1$I OR (c.%1$I IS NULL AND r.%1$I IS NULL))', col), ' AND ')
            FROM unnest(v_lookup_columns) as col
        );
        v_rcte_join_condition := COALESCE(v_rcte_join_condition, 'true');
        -- The ident_corr must also match. This is critical for new entities that share a NULL lookup key.
        v_rcte_join_condition := format('(%s) AND (c.ident_corr = r.ident_corr OR (c.ident_corr IS NULL and r.ident_corr IS NULL))', v_rcte_join_condition);

        -- The partition key for new entities is handled by the `ident_corr` column itself
        -- in the joins and groupings. Appending a complex CASE here was found to cause
        -- incorrect window function partitioning.

        -- The trace is architecturally permanent but toggled by a GUC that controls
        -- the initial seed.
        /*
         * Performance Note on Tracing Overhead:
         * This implementation is designed to have near-zero performance impact when
         * tracing is disabled. Two patterns are used to achieve this:
         *
         * 1. `NULL || jsonb_build_object(...)`: When `p_log_trace` is false, the
         *    trace seed is set to `NULL::jsonb`. Subsequent trace-building operations
         *    use the pattern `trace || jsonb_build_object(...)`. As demonstrated by
         *    the runnable example below (see also `tmp/null-logic-and-work.sql`), this
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
            v_trace_seed_expr := format($$jsonb_build_object('cte', 'raswp', 'partition_key', jsonb_build_object(%s), 's_row_id', s.source_row_id, 's_data', s.data_payload, 't_data', t.data_payload, 'seg_ident_corr', seg.ident_corr, 'causal_row_id', causal.source_row_id) as trace$$, (SELECT string_agg(format('%L, seg.%I', col, col), ',') FROM unnest(v_lookup_columns) col));
        ELSE
            v_trace_seed_expr := 'NULL::jsonb as trace';
        END IF;

        -- This trace step is inside the coalescing logic. It duplicates the CASE
        -- statement for `is_new_segment` to show exactly what is being compared.
        v_trace_select_list := format($$
            trace || jsonb_build_object(
                'cte', 'coalesce_check',
                'ident_corr', ident_corr,
                'is_new_segment_calc',
                CASE
                    WHEN LAG(valid_until) OVER w = valid_from
                     AND (LAG(data_payload - %1$L::text[]) OVER w IS NOT DISTINCT FROM (data_payload - %1$L::text[]))
                    THEN 0
                    ELSE 1
                END,
                'current_payload_sans_ephemeral', data_payload - %1$L::text[],
                'lag_payload_sans_ephemeral', LAG(data_payload - %1$L::text[]) OVER w
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

        IF v_log_vars THEN
            RAISE NOTICE '(%) --- temporal_merge_plan variables ---', v_log_id;
            RAISE NOTICE '(%) v_source_data_payload_expr (%%2$s): %', v_log_id, v_source_data_payload_expr;
            RAISE NOTICE '(%) v_source_table_ident (%%3$s): %', v_log_id, v_source_table_ident;
            RAISE NOTICE '(%) v_target_table_ident (%%4$s): %', v_log_id, v_target_table_ident;
            RAISE NOTICE '(%) v_ephemeral_columns (%%5$L): %', v_log_id, v_ephemeral_columns;
            RAISE NOTICE '(%) v_target_data_cols_jsonb_build (%%6$s): %', v_log_id, v_target_data_cols_jsonb_build;
            RAISE NOTICE '(%) temporal_merge_plan.mode (%%7$L): %', v_log_id, temporal_merge_plan.mode;
            RAISE NOTICE '(%) v_final_data_payload_expr (%%8$s): %', v_log_id, v_final_data_payload_expr;
            RAISE NOTICE '(%) v_resolver_ctes (%%9$s): %', v_log_id, v_resolver_ctes;
            RAISE NOTICE '(%) v_resolver_from (%%10$s): %', v_log_id, v_resolver_from;
            RAISE NOTICE '(%) v_diff_join_condition (%%11$s): %', v_log_id, v_diff_join_condition;
            RAISE NOTICE '(%) v_entity_id_check_is_null_expr (%%13$s): %', v_log_id, v_entity_id_check_is_null_expr;
            RAISE NOTICE '(%) v_valid_from_col (%%14$I): %', v_log_id, v_valid_from_col;
            RAISE NOTICE '(%) v_valid_until_col (%%15$I): %', v_log_id, v_valid_until_col;
            RAISE NOTICE '(%) v_ident_corr_select_expr (%%16$s): %', v_log_id, v_ident_corr_select_expr;
            RAISE NOTICE '(%) temporal_merge_plan.row_id_column (%%18$I): %', v_log_id, temporal_merge_plan.row_id_column;
            RAISE NOTICE '(%) v_range_constructor (%%19$I): %', v_log_id, v_range_constructor;
            RAISE NOTICE '(%) v_target_rows_filter (%%20$s): %', v_log_id, v_target_rows_filter;
            RAISE NOTICE '(%) v_stable_pk_cols_jsonb_build (%%21$s): %', v_log_id, v_stable_pk_cols_jsonb_build;
            RAISE NOTICE '(%) v_all_id_cols (%%22$L): %', v_log_id, v_all_id_cols;
            RAISE NOTICE '(%) v_lookup_cols_select_list (%%23$s): %', v_log_id, v_lookup_cols_select_list;
            RAISE NOTICE '(%) v_target_rows_lookup_cols_expr (%%24$s): %', v_log_id, v_target_rows_lookup_cols_expr;
            RAISE NOTICE '(%) v_source_rows_exists_join_expr (%%25$s): %', v_log_id, v_source_rows_exists_join_expr;
            RAISE NOTICE '(%) v_lookup_cols_select_list_no_alias (%%26$s): %', v_log_id, v_lookup_cols_select_list_no_alias;
            RAISE NOTICE '(%) v_atomic_segments_select_list_expr (%%27$s): %', v_log_id, v_atomic_segments_select_list_expr;
            RAISE NOTICE '(%) v_lateral_join_sr_to_seg (%%28$s): %', v_log_id, v_lateral_join_sr_to_seg;
            RAISE NOTICE '(%) v_lateral_join_tr_to_seg (%%29$s): %', v_log_id, v_lateral_join_tr_to_seg;
            RAISE NOTICE '(%) v_diff_select_expr (%%30$s): %', v_log_id, v_diff_select_expr;
            RAISE NOTICE '(%) v_plan_with_op_entity_id_json_build_expr (%%31$s): %', v_log_id, v_plan_with_op_entity_id_json_build_expr;
            RAISE NOTICE '(%) v_skip_no_target_entity_id_json_build_expr (%%32$s): %', v_log_id, v_skip_no_target_entity_id_json_build_expr;
            RAISE NOTICE '(%) v_correlation_col (%%33$L): %', v_log_id, v_correlation_col;
            RAISE NOTICE '(%) v_final_order_by_expr (%%34$s): %', v_log_id, v_final_order_by_expr;
            RAISE NOTICE '(%) v_partition_key_for_with_base_payload_expr (%%36$s): %', v_log_id, v_partition_key_for_with_base_payload_expr;
            RAISE NOTICE '(%) v_entity_partition_key_cols (%%37$s -- OBSOLETE): %', v_log_id, v_entity_partition_key_cols;
            RAISE NOTICE '(%) v_s_founding_join_condition (%%38$s): %', v_log_id, v_s_founding_join_condition;
            RAISE NOTICE '(%) v_tr_qualified_lookup_cols (%%39$s): %', v_log_id, v_tr_qualified_lookup_cols;
            RAISE NOTICE '(%) v_ident_corr_column_type (%%40$s): %', v_log_id, v_ident_corr_column_type;
            RAISE NOTICE '(%) v_non_temporal_lookup_cols_select_list (%%41$s -- OBSOLETE): %', v_log_id, v_non_temporal_lookup_cols_select_list;
            RAISE NOTICE '(%) v_non_temporal_lookup_cols_select_list_no_alias (%%42$s -- OBSOLETE): %', v_log_id, v_non_temporal_lookup_cols_select_list_no_alias;
            RAISE NOTICE '(%) v_non_temporal_tr_qualified_lookup_cols (%%43$s -- OBSOLETE): %', v_log_id, v_non_temporal_tr_qualified_lookup_cols;
            RAISE NOTICE '(%) v_lookup_cols_sans_valid_from (%%44$s): %', v_log_id, v_lookup_cols_sans_valid_from;
            RAISE NOTICE '(%) v_unqualified_id_cols_sans_vf (%%45$s): %', v_log_id, v_unqualified_id_cols_sans_vf;
            RAISE NOTICE '(%) v_trace_select_list (%%46$s): %', v_log_id, v_trace_select_list;
            RAISE NOTICE '(%) v_trace_seed_expr (%%47$s): %', v_log_id, v_trace_seed_expr;
            RAISE NOTICE '(%) v_stable_id_aggregates_expr (%%48$s): %', v_log_id, v_stable_id_aggregates_expr;
            RAISE NOTICE '(%) v_stable_id_projection_expr (%%49$s): %', v_log_id, v_stable_id_projection_expr;
            RAISE NOTICE '(%) v_target_data_cols_jsonb_build_bare (%%50$s): %', v_log_id, v_target_data_cols_jsonb_build_bare;
            RAISE NOTICE '(%) v_stable_pk_cols_jsonb_build_bare (%%51$s): %', v_log_id, v_stable_pk_cols_jsonb_build_bare;
            RAISE NOTICE '(%) v_rcte_join_condition (%%52$s): %', v_log_id, v_rcte_join_condition;
            RAISE NOTICE '(%) v_qualified_r_id_cols_sans_vf (%%53$s): %', v_log_id, v_qualified_r_id_cols_sans_vf;
            RAISE NOTICE '(%) p_log_trace (%%54$L): %', v_log_id, p_log_trace;
            RAISE NOTICE '(%) v_entity_id_check_is_null_expr_no_alias (%%55$s): %', v_log_id, v_entity_id_check_is_null_expr_no_alias;
            RAISE NOTICE '(%) v_source_ephemeral_cols_jsonb_build (%%56$s): %', v_log_id, v_source_ephemeral_cols_jsonb_build;
            RAISE NOTICE '(%) v_target_ephemeral_cols_jsonb_build (%%58$s): %', v_log_id, v_target_ephemeral_cols_jsonb_build;
            RAISE NOTICE '(%) v_target_ephemeral_cols_jsonb_build_bare (%%57$s): %', v_log_id, v_target_ephemeral_cols_jsonb_build_bare;
            RAISE NOTICE '(%) v_coalesced_payload_expr (%%59$s): %', v_log_id, v_coalesced_payload_expr;
            RAISE NOTICE '(%) v_non_temporal_lookup_cols_select_list_prefix (%%64$s): %', v_log_id, v_non_temporal_lookup_cols_select_list_prefix;
            RAISE NOTICE '(%) v_non_temporal_tr_qualified_lookup_cols_prefix (%%65$s): %', v_log_id, v_non_temporal_tr_qualified_lookup_cols_prefix;
            RAISE NOTICE '(%) v_lookup_cols_sans_valid_from_prefix (%%66$s): %', v_log_id, v_lookup_cols_sans_valid_from_prefix;
        END IF;
        -- 4. Construct and execute the main query to generate the execution plan.
        v_sql := format($SQL$
WITH
-- CTE 1: source_initial
-- Purpose: Selects and prepares the raw data from the source table.
--
-- Example: A source table row like `(row_id: 101, id: 1, name: 'A', ...)`
-- becomes a tuple with a structured `data_payload` and a boolean `is_new_entity` flag.
-- `(source_row_id: 101, ident_corr: 101, id: 1, ..., data_payload: {"name": "A"}, is_new_entity: false)`
--
-- Formulation: A simple SELECT that dynamically builds the `data_payload` JSONB object from all relevant
-- source columns. The `is_new_entity` flag is critical for downstream logic to distinguish between
-- operations on existing entities versus the creation of new ones.
source_initial AS (
    SELECT
        t.%18$I as source_row_id,
        %16$s
        %64$s
        t.%14$I as valid_from,
        t.%15$I as valid_until,
        %2$s AS data_payload,
        %56$s AS ephemeral_payload,
        %13$s as is_new_entity
    FROM %3$s t
),
-- CTE 2: target_rows
-- Purpose: Selects the relevant historical data from the target table.
--
-- Example: For a source batch affecting entities with `id` 1 and 2, this CTE would select all
-- historical rows for those two entities from the target table, e.g.:
-- `(id: 1, valid_from: '2022-01-01', valid_until: '2023-01-01', ...)`
-- `(id: 2, valid_from: '2022-05-15', valid_until: 'infinity', ...)`
--
-- Formulation: To optimize performance, the `v_target_rows_filter` is a dynamically generated subquery.
-- For keys defined as NOT NULL, it uses a fast `IN` clause. For nullable keys, it uses a more complex but
-- correct and index-friendly `UNION` of `JOIN`s to remain performant.
target_rows AS (
    SELECT
        %62$s -- v_non_temporal_lookup_cols_select_list_no_alias_prefix (non-temporal identity columns)
        %14$I as valid_from, -- The temporal identity column (e.g., valid_from)
        NULL::%40$s as ident_corr, -- Target rows do not originate from a source row, so ident_corr is NULL. Type is introspected.
        %51$s as stable_pk_payload,
        %15$I as valid_until,
        %50$s AS data_payload,
        %57$s AS ephemeral_payload
    FROM %20$s -- v_target_rows_filter is now a subquery with alias t
),
-- CTE 3: source_rows
-- Purpose: Augments the source data with a critical flag: `target_entity_exists`.
-- Formulation: An `EXISTS` subquery checks if a corresponding entity already exists in the target table.
-- This is a key declarative optimization that allows modes like `INSERT_NEW_ENTITIES` to filter out irrelevant
-- source rows very early in the process, avoiding unnecessary work.
source_rows AS (
    SELECT
        si.*,
        EXISTS (SELECT 1 FROM target_rows tr WHERE %25$s) as target_entity_exists -- v_source_rows_exists_join_expr
    FROM source_initial si
),
-- CTE 4: active_source_rows
-- Purpose: Filters the source rows based on the requested `mode`.
-- Formulation: A simple `CASE` statement in the `WHERE` clause declaratively enforces the semantics of each mode.
-- For example, `INSERT_NEW_ENTITIES` will only consider rows where `target_entity_exists` is false. This is another
-- critical performance optimization.
active_source_rows AS (
    SELECT
        sr.*
    FROM source_rows sr
    WHERE CASE %7$L::sql_saga.temporal_merge_mode
        -- MERGE_ENTITY modes process all source rows initially; they handle existing vs. new entities in the planner.
        WHEN 'MERGE_ENTITY_PATCH' THEN true
        WHEN 'MERGE_ENTITY_REPLACE' THEN true
        WHEN 'MERGE_ENTITY_UPSERT' THEN true
        -- INSERT_NEW_ENTITIES is optimized to only consider rows for entities that are new to the target.
        WHEN 'INSERT_NEW_ENTITIES' THEN NOT sr.target_entity_exists
        -- ..._FOR_PORTION_OF modes are optimized to only consider rows for entities that already exist in the target.
        WHEN 'PATCH_FOR_PORTION_OF' THEN sr.target_entity_exists
        WHEN 'REPLACE_FOR_PORTION_OF' THEN sr.target_entity_exists
        WHEN 'DELETE_FOR_PORTION_OF' THEN sr.target_entity_exists
        WHEN 'UPDATE_FOR_PORTION_OF' THEN sr.target_entity_exists
        ELSE false
    END
),
-- CTE 5: all_rows
-- Purpose: Creates the unified set of all relevant time periods (both source and target) for the entities being processed.
-- This is the foundational data set upon which the atomic timeline will be built.
--
-- Example: For an entity, this might produce:
-- `(id: 1, ident_corr: 101, valid_from: '2023-01-01', valid_until: '2023-06-01') -- from source`
-- `(id: 1, ident_corr: 101, valid_from: '2022-01-01', valid_until: '2024-01-01') -- from target`
--
-- Formulation: A `UNION ALL` combines the active source rows with the relevant target rows. For target rows,
-- a `LEFT JOIN LATERAL ... LIMIT 1` is used to find a single, representative `ident_corr` from one of the
-- source rows affecting the same entity. This is a declarative, stateless operation that ensures every timeline
-- segment has the necessary grouping metadata for downstream processing. The `LATERAL` join is more performant
-- than a `DISTINCT ON` subquery for this "find first" pattern.
all_rows AS (
    SELECT %62$s ident_corr, valid_from, valid_until, is_new_entity FROM active_source_rows
    UNION ALL
    SELECT
        %65$s
        -- Propagate the ident_corr from a matching source row to ensure
        -- that all segments of a single conceptual entity are processed
        -- within the same partition.
        s_founding.ident_corr,
        tr.valid_from,
        tr.valid_until,
        false as is_new_entity
    FROM target_rows tr
    LEFT JOIN LATERAL (
        SELECT sr.ident_corr FROM active_source_rows sr
        WHERE %38$s -- v_s_founding_join_condition (now uses 'sr' alias)
        ORDER BY sr.ident_corr -- For deterministic selection
        LIMIT 1
    ) s_founding ON true
),
all_rows_with_unified_corr AS (
    SELECT
        ar.*,
        min(ar.ident_corr) OVER (PARTITION BY %61$s CASE WHEN ar.is_new_entity THEN ar.ident_corr::text ELSE NULL END) as unified_ident_corr
    FROM all_rows ar
),
-- CTE 6: time_points
-- Purpose: Deconstructs all time periods from `all_rows` into a distinct, ordered set of chronological points for each entity.
-- This is the core of the timeline-building process.
--
-- Example: The two periods from the `all_rows` example would be deconstructed into four distinct points:
-- `(id: 1, point: '2022-01-01', ident_corr: 101)`
-- `(id: 1, point: '2023-01-01', ident_corr: 101)`
-- `(id: 1, point: '2023-06-01', ident_corr: 101)`
-- `(id: 1, point: '2024-01-01', ident_corr: 101)`
--
-- Formulation: This is the critical CTE that solves the core partitioning problem. A `GROUP BY` on the time point
-- and a `CASE` statement on the entity's status (new vs. existing) ensures that:
-- 1. All segments for an EXISTING entity are grouped together under a single canonical `ident_corr`.
-- 2. All segments for NEW entities are kept separate by grouping on their unique `ident_corr`.
time_points AS (
    SELECT
        %61$s point,
        %48$s
        min(ident_corr) as ident_corr,
        is_new_entity
    FROM (
        SELECT %26$s, unified_ident_corr as ident_corr, valid_from AS point, is_new_entity FROM all_rows_with_unified_corr
        UNION ALL
        SELECT %26$s, unified_ident_corr as ident_corr, valid_until AS point, is_new_entity FROM all_rows_with_unified_corr
    ) AS points
    GROUP BY
        %61$s point,
        is_new_entity,
        -- This CASE is the key: for new entities (where stable ID is null), we group by
        -- their unique correlation ID. For existing entities, we group them together
        -- by treating this expression as NULL.
        CASE WHEN is_new_entity THEN ident_corr::TEXT ELSE NULL END
),
-- CTE 7: atomic_segments
-- Purpose: Reconstructs the timeline from the points into a set of atomic, non-overlapping, contiguous segments.
--
-- Example: The four points from the `time_points` example would be reconstructed into three atomic segments:
-- `(id: 1, ident_corr: 101, valid_from: '2022-01-01', valid_until: '2023-01-01')`
-- `(id: 1, ident_corr: 101, valid_from: '2023-01-01', valid_until: '2023-06-01')`
-- `(id: 1, ident_corr: 101, valid_from: '2023-06-01', valid_until: '2024-01-01')`
--
-- Formulation: The `LEAD()` window function is a stateless, declarative way to create the `[valid_from, valid_until)`
-- segments from the ordered list of time points. The partitioning is critical here to ensure timelines for
-- different entities are handled independently.
atomic_segments AS (
    SELECT %26$s, ident_corr, point as valid_from, next_point as valid_until, is_new_entity
    FROM (
        SELECT
            *,
            LEAD(point) OVER (PARTITION BY %61$sident_corr ORDER BY point) as next_point
        FROM time_points
    ) with_lead
    WHERE point IS NOT NULL AND next_point IS NOT NULL AND point < next_point
),
-- CTE 8: resolved_atomic_segments_with_payloads
-- Purpose: This is the main workhorse CTE. It enriches each atomic segment with the data payloads from the
-- original source and target rows that cover its time range.
-- Formulation: It uses `LEFT JOIN LATERAL` to declaratively find the correct data for each segment.
-- The `<@` (contained by) operator is the core of this stateless lookup. The `causal` join assigns a
-- `source_row_id` to every segment (including gaps) based on a declarative set of priority rules.
resolved_atomic_segments_with_payloads AS (
    SELECT
        with_base_payload.*,
        -- This window function propagates the stable primary key payload from the original target row
        -- to all new segments created for that entity. This is a declarative, stateless operation
        -- that ensures new historical records retain their correct stable identifier.
        FIRST_VALUE(with_base_payload.stable_pk_payload) OVER (PARTITION BY %61$swith_base_payload.ident_corr ORDER BY with_base_payload.stable_pk_payload IS NULL, with_base_payload.valid_from) AS propagated_stable_pk_payload
    FROM (
        SELECT
            %36$s, -- v_partition_key_for_with_base_payload_expr
            seg.valid_from,
            seg.valid_until,
            t.t_valid_from,
            causal.source_row_id,
            s.data_payload as s_data_payload,
            s.ephemeral_payload as s_ephemeral_payload,
            t.data_payload as t_data_payload,
            t.ephemeral_payload as t_ephemeral_payload,
            t.stable_pk_payload,
            %47$s
        FROM atomic_segments seg
        -- Join to find the original target data for this time slice.
        -- Example: For an atomic segment (id:1, from:'2023-01-01', until:'2023-06-01'), this will find the
        -- original target row `(id:1, from:'2022-01-01', until:'2024-01-01', data:{...})` because the segment
        -- is contained by (`<@`) the target row's period.
        LEFT JOIN LATERAL (
            SELECT tr.data_payload, tr.ephemeral_payload, tr.valid_from as t_valid_from, tr.stable_pk_payload
            FROM target_rows tr
            WHERE %29$s
              AND %19$I(seg.valid_from, seg.valid_until) <@ %19$I(tr.valid_from, tr.valid_until)
        ) t ON true
        -- Join to find the source data for this time slice.
        -- Example: For the same atomic segment, this will find the source row
        -- `(id:1, from:'2023-01-01', until:'2023-06-01', data:{...})` that directly covers it.
        -- If multiple source rows overlap (e.g., corrections), `ORDER BY ... LIMIT 1` ensures we deterministically pick the latest one.
        LEFT JOIN LATERAL (
            SELECT sr.source_row_id, sr.data_payload, sr.ephemeral_payload
            FROM active_source_rows sr
            WHERE %28$s
              AND %19$I(seg.valid_from, seg.valid_until) <@ %19$I(sr.valid_from, sr.valid_until)
            -- In case of overlapping source rows, the one with the highest row_id (latest) wins.
            ORDER BY sr.source_row_id DESC
            LIMIT 1
        ) s ON true
        -- This join finds the single, most relevant "causal" source row for any given atomic segment.
        -- Its purpose is to provide a stable, deterministic link for sorting and feedback, especially for
        -- "gap" segments that do not directly overlap with any source data.
        --
        -- Example: A "gap" segment (from:'2023-06-01', until:'2023-09-01') has no overlapping source data.
        -- The ORDER BY will prioritize a source row that starts at '2023-09-01' (look-ahead) over one that
        -- ends at '2023-06-01' (look-behind), making the next chronological change the cause.
        -- This is a declarative set of priority rules, not a stateful operation.
        LEFT JOIN LATERAL (
            -- Find the single, most relevant "causal" source row for any given atomic segment.
            -- This provides a stable, deterministic link for sorting and feedback, even for
            -- "gap" segments that are not directly covered by any source data.
            SELECT sr.source_row_id
            FROM active_source_rows sr
            WHERE %28$s -- Join on lookup keys
            ORDER BY
                -- Priority 1: A source row that directly overlaps the segment is the primary cause.
                (%19$I(sr.valid_from, sr.valid_until) && %19$I(seg.valid_from, seg.valid_until)) DESC,
                -- Priority 2: For non-overlapping segments (gaps), prioritize an adjacent source row
                -- that starts exactly where the segment ends (the "met by" relationship). This
                -- correctly looks ahead to the next chronological change event.
                (sr.valid_from = seg.valid_until) DESC,
                -- Priority 3: If no row is ahead, prioritize an adjacent source row that ends
                -- exactly where the segment starts (the "meets" relationship). This correctly
                -- looks behind to the previous change event.
                (sr.valid_until = seg.valid_from) DESC,
                -- Priority 4: Final tie-breaker for stability.
                sr.source_row_id DESC
            LIMIT 1
        ) causal ON true
        -- Filter out empty time segments where no source or target data exists.
        WHERE (s.data_payload IS NOT NULL OR t.data_payload IS NOT NULL) -- Filter out gaps
          -- For surgical ..._FOR_PORTION_OF modes, we must clip the source to the target's timeline.
          -- We do this declaratively by only processing atomic segments that have data in the target.
          AND CASE %7$L::sql_saga.temporal_merge_mode
              WHEN 'PATCH_FOR_PORTION_OF' THEN t.data_payload IS NOT NULL
              WHEN 'REPLACE_FOR_PORTION_OF' THEN t.data_payload IS NOT NULL
              WHEN 'DELETE_FOR_PORTION_OF' THEN t.data_payload IS NOT NULL
              WHEN 'UPDATE_FOR_PORTION_OF' THEN t.data_payload IS NOT NULL
              ELSE true
          END
    ) with_base_payload
)
%9$s,
-- CTE 9: resolved_atomic_segments
-- Purpose: Applies the high-level semantic logic for each mode (PATCH, REPLACE, etc.) to calculate
-- the final data payload for each atomic segment.
-- Formulation: The `v_final_data_payload_expr` is a dynamically generated SQL expression. For example,
-- for PATCH/UPSERT modes, it is `COALESCE(t_data_payload, '{}') || COALESCE(s_data_payload, '{}')`.
-- This is a stateless, declarative way to compute the final state for each segment independently.
resolved_atomic_segments AS (
    SELECT
        %66$svalid_from,
        valid_until,
        t_valid_from,
        source_row_id,
        ident_corr,
        propagated_stable_pk_payload AS stable_pk_payload,
        s_data_payload IS NULL AS unaffected_target_only_segment,
        s_data_payload,
        t_data_payload,
        %8$s as data_payload,
        COALESCE(t_ephemeral_payload, '{}'::jsonb) || COALESCE(s_ephemeral_payload, '{}'::jsonb) as ephemeral_payload,
        trace,
        CASE WHEN s_data_payload IS NOT NULL THEN 1 ELSE 2 END as priority
    FROM %10$s
),
-- CTE 10: coalesced_final_segments
-- Purpose: Merges adjacent atomic segments that have identical data payloads (ignoring ephemeral columns).
-- This is a critical optimization to generate the minimal number of DML operations.
--
-- Example: If two adjacent segments have the same final data, e.g.:
-- `(... valid_from: '2022-01-01', valid_until: '2023-01-01', data: {"name":"A"})`
-- `(... valid_from: '2023-01-01', valid_until: '2024-01-01', data: {"name":"A"})`
-- They will be merged into a single segment:
-- `(... valid_from: '2022-01-01', valid_until: '2024-01-01', data: {"name":"A"})`
--
-- Formulation: This is a classic "gaps-and-islands" problem, solved declaratively using a recursive CTE.
-- The anchor member finds the start of each "island" of contiguous, identical data. The recursive member
-- then traverses the chain of adjacent, identical segments.
coalesced_final_segments AS (
    WITH RECURSIVE island_chain AS (
        -- Anchor member: Find the start of each island of contiguous, identical data.
        -- A segment is a "start" if it is not preceded by an adjacent, identical segment.
        SELECT
            %45$s, -- Entity identity columns (e.g., id, ident_corr)
            ras.t_valid_from,
            ras.valid_from,
            ras.valid_until,
            ras.data_payload,
            ras.ephemeral_payload,
            ras.stable_pk_payload,
            ras.source_row_id,
            ras.unaffected_target_only_segment,
            CASE WHEN %54$L::boolean THEN jsonb_build_array(ras.trace) ELSE NULL END as trace,
            -- The island is uniquely identified by its entity and its starting point.
            ras.valid_from AS island_start_from
        FROM (
            -- Use LAG() to peek at the previous segment in an entity's timeline to check for continuity.
            -- The window `w` is partitioned by the entity's stable key and correlation ID for new entities.
            --
            -- Example: For two adjacent segments for entity (id:1):
            -- 1. (id:1, from:'2023-01-01', until:'2023-06-01', data:{...}) -> prev_valid_until is NULL
            -- 2. (id:1, from:'2023-06-01', until:'2024-01-01', data:{...}) -> prev_valid_until is '2023-06-01'
            --
            -- This allows the WHERE clause below to identify segment #1 as the start of a new "island".
            SELECT
                *,
                LAG(valid_until) OVER w as prev_valid_until,
                LAG(data_payload) OVER w as prev_data_payload
            FROM resolved_atomic_segments ras
            WINDOW w AS (PARTITION BY %61$sident_corr ORDER BY valid_from)
        ) ras
        WHERE ras.data_payload IS NOT NULL
          -- It's a start if there's no previous segment, a gap, or the data is different.
          AND (ras.prev_valid_until IS NULL
               OR ras.prev_valid_until <> ras.valid_from
               OR (ras.prev_data_payload IS DISTINCT FROM ras.data_payload))

        UNION ALL

        -- Recursive member: Find the next link in the chain for each island.
        -- Example: If the anchor found an island starting at '2022-01-01' for entity (id:1),
        -- this will join to find the segment that starts at the anchor's `valid_until` if it has
        -- identical data. It will continue chaining until a gap is found or the data changes.
        SELECT
            %53$s,
            r.t_valid_from,
            r.valid_from,
            r.valid_until,
            r.data_payload,
            r.ephemeral_payload,
            r.stable_pk_payload,
            r.source_row_id,
            r.unaffected_target_only_segment,
            CASE WHEN %54$L::boolean THEN c.trace || r.trace ELSE NULL END,
            c.island_start_from -- Propagate the island's unique identifier
        FROM island_chain c
        -- Join to the full set of segments to find the next one.
        JOIN resolved_atomic_segments r
          -- Must be the same entity.
          ON %52$s
          -- Must be temporally adjacent.
          AND c.valid_until = r.valid_from
          -- Must have identical data. Ephemeral data is handled separately.
          AND c.data_payload IS NOT DISTINCT FROM r.data_payload
          AND r.data_payload IS NOT NULL
    )
    SELECT
        %45$s,
        sql_saga.first(t_valid_from ORDER BY valid_from) as ancestor_valid_from,
        MIN(valid_from) as valid_from,
        MAX(valid_until) as valid_until,
        %59$s as data_payload,
        sql_saga.first(stable_pk_payload ORDER BY valid_from DESC) as stable_pk_payload,
        bool_and(unaffected_target_only_segment) as unaffected_target_only_segment,
        array_agg(DISTINCT source_row_id::BIGINT) FILTER (WHERE source_row_id IS NOT NULL) as row_ids,
        CASE WHEN %54$L::boolean
             THEN jsonb_build_object(
                 'cte', 'coalesced',
                 'island_start_from', island_start_from,
                 'final_payload', sql_saga.first(data_payload ORDER BY valid_from DESC),
                 'final_payload_sans_ephemeral', sql_saga.first(data_payload - %5$L::text[] ORDER BY valid_from DESC),
                 'atomic_traces', jsonb_agg(trace ORDER BY valid_from) FILTER (WHERE trace IS NOT NULL)
             )
             ELSE NULL
        END as trace
    FROM island_chain
    GROUP BY
        %45$s,
        island_start_from
),
-- CTE 11: diff
-- Purpose: Compares the final, coalesced state of the timeline with the original state from `target_rows`.
-- Formulation: A `FULL OUTER JOIN` is the standard, declarative way to compare two sets of data.
-- The result of this join provides all the necessary information to determine if a segment represents
-- an INSERT, UPDATE, DELETE, or an unchanged state.
diff AS (
    SELECT
        %30$s, -- v_diff_select_expr
        f.f_from, f.f_until, f.f_data, f.f_row_ids, f.stable_pk_payload, f.trace, f.unaffected_target_only_segment,
        t.t_from, t.t_until, t.t_data,
        -- This function call determines the Allen Interval Relation between the final state and target state segments
        sql_saga.allen_get_relation(f.f_from, f.f_until, t.t_from, t.t_until) as relation
    FROM
    (
        SELECT
            %26$s,
            ancestor_valid_from,
            ident_corr,
            valid_from AS f_from,
            valid_until AS f_until,
            data_payload AS f_data,
            stable_pk_payload,
            row_ids AS f_row_ids,
            unaffected_target_only_segment,
            trace
        FROM coalesced_final_segments
    ) f
    FULL OUTER JOIN
    (
        SELECT
            %26$s,
            ident_corr,
            valid_from as t_from,
            valid_until as t_until,
            data_payload || ephemeral_payload as t_data
        FROM target_rows
    ) t
    ON %11$s -- Join on raw lookup columns and diff condition
),
-- CTE 12: diff_ranked
-- Purpose: When a single original target row is split into multiple new segments, only one of them can be
-- an `UPDATE` in the final plan. This CTE ranks the candidates to deterministically choose one.
-- Formulation: A `row_number()` window function applies a declarative set of rules: the segment that
-- preserves the original start time is the best candidate for an `UPDATE`. This is stateless as it
-- considers all candidates for a given target row at once.
diff_ranked AS (
    SELECT
        d.*,
        -- For each original target row (t_from), we rank the potential final segments that could update it.
        -- Only one can be an UPDATE; the rest must be INSERTs.
        CASE
            -- Segments without a target ancestor or without a final state are not update candidates.
            WHEN d.t_from IS NULL OR d.f_from IS NULL THEN NULL
            -- A segment that is identical to its ancestor is not an update.
            WHEN d.f_from = d.t_from AND d.f_until = d.t_until AND d.f_data IS NOT DISTINCT FROM d.t_data THEN NULL
            -- Otherwise, it's a candidate. Rank them.
            ELSE
                row_number() OVER (
                    PARTITION BY %61$sd.t_from -- Partition by entity and the original row's start time
                    ORDER BY
                        -- The segment that preserves the start time is the best candidate for an UPDATE.
                        CASE WHEN d.f_from = d.t_from THEN 1 ELSE 2 END,
                        -- Tie-break by data similarity.
                        CASE WHEN d.f_data - %5$L::text[] IS NOT DISTINCT FROM d.t_data - %5$L::text[] THEN 1 ELSE 2 END,
                        d.f_from
                )
        END as update_rank
    FROM diff d
),
-- CTE 13: diff_with_propagated_ids
-- Purpose: Fill in NULL causal row IDs for "gap" segments using a "last observation carried forward" (LOCF) strategy.
-- This ensures every segment in an entity's timeline can be traced back to a source change.
-- Formulation: This is a standard gaps-and-islands pattern. The inner SUM() creates a grouping key that changes
-- whenever a non-NULL `f_row_ids` is encountered. The outer `first_value()` then propagates that ID to all
-- subsequent rows within that new group.
diff_with_propagated_ids AS (
    SELECT *,
        first_value(f_row_ids) OVER (PARTITION BY %37$s, ident_corr, row_id_grp ORDER BY f_from) AS propagated_f_row_ids
    FROM (
        SELECT *,
            sum(CASE WHEN f_row_ids IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY %37$s, ident_corr ORDER BY f_from) AS row_id_grp
        FROM diff_ranked
    ) with_grp
),
-- CTE 14: plan_with_op
-- Purpose: Assigns the final DML operation (`INSERT`, `UPDATE`, `DELETE`, `SKIP_IDENTICAL`) to each segment
-- based on the results of the `diff` and `diff_ranked` CTEs.
-- Formulation: A `CASE` statement declaratively translates the state of each segment into its corresponding DML action.
-- It also includes a `UNION ALL` to re-introduce source rows that were filtered out at the beginning, so they can
-- be reported with a `SKIPPED_NO_TARGET` status.
plan_with_op AS (
    (
        SELECT * FROM (
            SELECT
                -- The causal row IDs are propagated to all segments. This COALESCE is a fallback
                -- for "gap" segments that don't directly have a source row. It uses the "last
                -- observation carried forward" pattern to find the causal ID from the last
                -- non-gap segment.
                COALESCE(d.f_row_ids, d.propagated_f_row_ids) as row_ids,
                -- Determine the final DML operation based on the diff and rank.
                CASE
                    WHEN d.t_from IS NULL THEN 'INSERT'::sql_saga.temporal_merge_plan_action
                    -- This case is reached for segments present in the target but absent from the final calculated
                    -- timeline (e.g., in a destructive delete_mode). The `diff` CTE's FULL OUTER JOIN
                    -- creates rows with a NULL f_from for these, which we correctly identify as DELETEs.
                    WHEN d.f_from IS NULL THEN 'DELETE'::sql_saga.temporal_merge_plan_action
                    WHEN d.update_rank = 1 THEN 'UPDATE'::sql_saga.temporal_merge_plan_action
                    WHEN d.update_rank > 1 THEN 'INSERT'::sql_saga.temporal_merge_plan_action
                    -- An identical segment that was influenced by a source row is a true SKIP_IDENTICAL.
                    -- If it's not an unaffected target-only segment, it must have had source influence.
                    WHEN NOT d.unaffected_target_only_segment THEN 'SKIP_IDENTICAL'::sql_saga.temporal_merge_plan_action
                    -- An identical segment that was NOT influenced by a source row is an unaffected
                    -- target segment. We filter it out by returning NULL, as it is not part of the plan of action.
                    ELSE NULL
                END as operation,
                %26$s,
                %31$s as entity_id_json, -- v_plan_with_op_entity_id_json_build_expr
                d.ident_corr,
                d.t_from as old_valid_from,
                d.t_until as old_valid_until,
                d.f_from as new_valid_from,
                d.f_until as new_valid_until,
                d.f_data as data,
                d.relation,
                d.trace
            FROM diff_with_propagated_ids d
            WHERE d.f_row_ids IS NOT NULL OR d.propagated_f_row_ids IS NOT NULL OR d.t_data IS NOT NULL -- Exclude pure deletions of non-existent target data
        ) with_op
        WHERE with_op.operation IS NOT NULL
    )
    UNION ALL
    (
        -- This part of the plan handles source rows that were filtered out by the main logic,
        -- allowing the executor to provide accurate feedback.
        SELECT
            ARRAY[sr.source_row_id::BIGINT],
            CASE
                WHEN %7$L::sql_saga.temporal_merge_mode = 'INSERT_NEW_ENTITIES' AND sr.target_entity_exists THEN 'SKIP_FILTERED'::sql_saga.temporal_merge_plan_action
                WHEN %7$L::sql_saga.temporal_merge_mode IN ('PATCH_FOR_PORTION_OF', 'REPLACE_FOR_PORTION_OF', 'DELETE_FOR_PORTION_OF', 'UPDATE_FOR_PORTION_OF') AND NOT sr.target_entity_exists THEN 'SKIP_NO_TARGET'::sql_saga.temporal_merge_plan_action
                ELSE 'ERROR'::sql_saga.temporal_merge_plan_action -- Should be unreachable, but acts as a fail-fast safeguard.
            END,
            %26$s,
            %32$s, -- v_skip_no_target_entity_id_json_build_expr
            sr.ident_corr,
            NULL, NULL, NULL, NULL, NULL, NULL,
            NULL::jsonb -- trace
        FROM source_rows sr
        WHERE NOT (
            CASE %7$L::sql_saga.temporal_merge_mode
                -- MERGE_ENTITY modes process all source rows initially; they handle existing vs. new entities in the planner.
                WHEN 'MERGE_ENTITY_PATCH' THEN true
                WHEN 'MERGE_ENTITY_REPLACE' THEN true
                WHEN 'MERGE_ENTITY_UPSERT' THEN true
                -- INSERT_NEW_ENTITIES is optimized to only consider rows for entities that are new to the target.
                WHEN 'INSERT_NEW_ENTITIES' THEN NOT sr.target_entity_exists
                -- ..._FOR_PORTION_OF modes are optimized to only consider rows for entities that already exist in the target.
                WHEN 'PATCH_FOR_PORTION_OF' THEN sr.target_entity_exists
                WHEN 'REPLACE_FOR_PORTION_OF' THEN sr.target_entity_exists
                WHEN 'DELETE_FOR_PORTION_OF' THEN sr.target_entity_exists
                WHEN 'UPDATE_FOR_PORTION_OF' THEN sr.target_entity_exists
                ELSE false
            END
        )
    )
),
-- CTE 15: plan
-- Purpose: Performs final calculations for the plan, such as determining the `update_effect` and constructing
-- the final `entity_ids` JSONB object for feedback and ID back-filling.
plan AS (
    SELECT
        p.*,
        -- Re-construct the final entity_ids JSONB at the very end of the plan.
        -- This is the only place it should be used.
        CASE
            WHEN p.operation = 'INSERT' AND sr.is_new_entity AND NOT sr.target_entity_exists
            THEN p.entity_id_json || jsonb_build_object(%33$L, p.ident_corr::text)
            ELSE p.entity_id_json
        END as entity_ids,
        CASE
            WHEN p.operation <> 'UPDATE' THEN NULL::sql_saga.temporal_merge_update_effect
            WHEN p.new_valid_from = p.old_valid_from AND p.new_valid_until = p.old_valid_until THEN 'NONE'::sql_saga.temporal_merge_update_effect
            WHEN p.new_valid_from <= p.old_valid_from AND p.new_valid_until >= p.old_valid_until THEN 'GROW'::sql_saga.temporal_merge_update_effect
            WHEN p.new_valid_from >= p.old_valid_from AND p.new_valid_until <= p.old_valid_until THEN 'SHRINK'::sql_saga.temporal_merge_update_effect
            ELSE 'MOVE'::sql_saga.temporal_merge_update_effect
        END AS update_effect
    FROM plan_with_op p
    -- We must join back to source_rows to get the is_new_entity flag for the final entity_ids construction.
    -- This is safe because row_ids will contain at most one ID for new-entity INSERTs.
    LEFT JOIN source_rows sr ON sr.source_row_id = p.row_ids[1]
)
-- Final SELECT
-- Purpose: Formats the final output and, most importantly, applies the final, stable sort order.
-- Formulation: A `row_number()` window function generates the `plan_op_seq`.
-- Intricacy: The `ORDER BY` clause is critical for the executor's correctness. It ensures that DML
-- operations are performed in a safe order (`INSERT` -> `UPDATE` -> `DELETE`) and that timeline-extending
-- `UPDATE`s happen before timeline-shrinking ones, to prevent transient gaps that could violate FKs.
SELECT
    row_number() OVER (
        ORDER BY
            %34$s, -- v_final_order_by_expr
            CASE p.operation
                WHEN 'INSERT' THEN 1
                WHEN 'UPDATE' THEN 2
                WHEN 'DELETE' THEN 3
                ELSE 4
            END,
            p.update_effect NULLS FIRST,
            COALESCE(p.old_valid_from, p.new_valid_from), -- Stable sort key for all segments of a split
            COALESCE(p.new_valid_from, p.old_valid_from),
            (p.row_ids[1])
    )::BIGINT as plan_op_seq,
    p.row_ids,
    p.operation,
    p.update_effect,
    p.entity_ids,
    p.relation,
    p.old_valid_from::TEXT,
    p.old_valid_until::TEXT,
    p.new_valid_from::TEXT,
    p.new_valid_until::TEXT,
    CASE
        WHEN jsonb_typeof(p.data) = 'object' THEN p.data - %22$L::text[]
        ELSE p.data
    END as data,
    p.trace
FROM plan p
ORDER BY plan_op_seq;
$SQL$,
            v_entity_id_as_jsonb,           /* %1$s -- OBSOLETE, but kept for arg numbering stability for now */
            v_source_data_payload_expr,     /* %2$s */
            v_source_table_ident,           /* %3$s */
            v_target_table_ident,           /* %4$s */
            v_ephemeral_columns,            /* %5$L */
            v_target_data_cols_jsonb_build, /* %6$s */
            temporal_merge_plan.mode,                           /* %7$L */
            v_final_data_payload_expr,      /* %8$s */
            v_resolver_ctes,                /* %9$s */
            v_resolver_from,                /* %10$s */
            v_diff_join_condition,          /* %11$s */
            '',    /* %12$s -- OBSOLETE */
            v_entity_id_check_is_null_expr, /* %13$s */
            v_valid_from_col,               /* %14$I */
            v_valid_until_col,              /* %15$I */
            v_ident_corr_select_expr,      /* %16$s */
            '', -- v_planner_entity_id_expr,       /* %17$s -- OBSOLETE */
            temporal_merge_plan.row_id_column,           /* %18$I */
            v_range_constructor,            /* %19$I */
            v_target_rows_filter,           /* %20$s */
            v_stable_pk_cols_jsonb_build,   /* %21$s */
            v_all_id_cols,                  /* %22$L */
            v_lookup_cols_select_list,      /* %23$s */
            v_target_rows_lookup_cols_expr, /* %24$s */
            v_source_rows_exists_join_expr, /* %25$s */
            v_lookup_cols_select_list_no_alias, /* %26$s */
            v_atomic_segments_select_list_expr, /* %27$s */
            v_lateral_join_sr_to_seg,       /* %28$s */
            v_lateral_join_tr_to_seg,       /* %29$s */
            v_diff_select_expr,             /* %30$s */
            v_plan_with_op_entity_id_json_build_expr, /* %31$s */
            v_skip_no_target_entity_id_json_build_expr, /* %32$s */
            v_correlation_col,              /* %33$L */
            v_final_order_by_expr,          /* %34$s */
            NULL,           /* %35$s -- OBSOLETE v_partition_key_cols */
            v_partition_key_for_with_base_payload_expr, /* %36$s */
            v_entity_partition_key_cols,    /* %37$s */
            v_s_founding_join_condition,    /* %38$s */
            v_tr_qualified_lookup_cols,     /* %39$s */
            v_ident_corr_column_type,       /* %40$s */
            v_non_temporal_lookup_cols_select_list, /* %41$s */
            v_non_temporal_lookup_cols_select_list_no_alias, /* %42$s */
            v_non_temporal_tr_qualified_lookup_cols, /* %43$s */
            v_lookup_cols_sans_valid_from,   /* %44$s */
            v_unqualified_id_cols_sans_vf, /* %45$s */
            v_trace_select_list,            /* %46$s */
            v_trace_seed_expr,              /* %47$s */
            v_stable_id_aggregates_expr,    /* %48$s */
            v_stable_id_projection_expr,     /* %49$s */
            v_target_data_cols_jsonb_build_bare, /* %50$s */
            v_stable_pk_cols_jsonb_build_bare,  /* %51$s */
            v_rcte_join_condition,           /* %52$s */
            v_qualified_r_id_cols_sans_vf,   /* %53$s */
            p_log_trace,                     /* %54$L */
            v_entity_id_check_is_null_expr_no_alias, /* %55$s */
            v_source_ephemeral_cols_jsonb_build, /* %56$s */
            v_target_ephemeral_cols_jsonb_build_bare, /* %57$s */
            v_target_ephemeral_cols_jsonb_build, /* %58$s */
            v_coalesced_payload_expr,        /* %59$s */
            NULL,                           /* %60$s -- OBSOLETE v_relation_expr */
            v_entity_partition_key_cols_prefix, /* %61$s */
            v_non_temporal_lookup_cols_select_list_no_alias_prefix, /* %62$s */
            v_stable_id_projection_expr_prefix, /* %63$s */
            v_non_temporal_lookup_cols_select_list_prefix, /* %64$s */
            v_non_temporal_tr_qualified_lookup_cols_prefix, /* %65$s */
            v_lookup_cols_sans_valid_from_prefix /* %66$s */
        );

        -- Conditionally log the generated SQL for debugging.
        BEGIN
            IF p_log_sql THEN
                RAISE DEBUG '--- temporal_merge SQL for % ---', temporal_merge_plan.target_table;
                RAISE DEBUG '%', v_sql;
            END IF;
        END;

        v_exec_sql := format('PREPARE %I AS %s', v_plan_ps_name, v_sql);
        EXECUTE v_exec_sql;

        v_exec_sql := format('EXECUTE %I', v_plan_ps_name);
        RETURN QUERY EXECUTE v_exec_sql;
    END;
END;
$temporal_merge_plan$;

COMMENT ON FUNCTION sql_saga.temporal_merge_plan(regclass, regclass, text[], sql_saga.temporal_merge_mode, name, name, name, sql_saga.temporal_merge_delete_mode, text[], text[], boolean, boolean) IS
'Generates a set-based execution plan for a temporal merge. This function is marked VOLATILE because it uses PREPARE to cache its expensive planning query for the duration of the session, which is a side-effect not permitted in STABLE or IMMUTABLE functions.';


-- Unified Executor Procedure
CREATE OR REPLACE PROCEDURE sql_saga.temporal_merge(
    target_table regclass,
    source_table regclass,
    identity_columns TEXT[],
    mode sql_saga.temporal_merge_mode DEFAULT 'MERGE_ENTITY_PATCH',
    era_name name DEFAULT 'valid',
    row_id_column name DEFAULT 'row_id',
    identity_correlation_column name DEFAULT NULL,
    update_source_with_identity BOOLEAN DEFAULT false,
    natural_identity_columns TEXT[] DEFAULT NULL,
    delete_mode sql_saga.temporal_merge_delete_mode DEFAULT 'NONE',
    update_source_with_feedback BOOLEAN DEFAULT false,
    feedback_status_column name DEFAULT NULL,
    feedback_status_key name DEFAULT NULL,
    feedback_error_column name DEFAULT NULL,
    feedback_error_key name DEFAULT NULL,
    ephemeral_columns TEXT[] DEFAULT NULL
)
LANGUAGE plpgsql AS $temporal_merge$
DECLARE
    v_lookup_columns TEXT[];
    v_target_table_ident TEXT := temporal_merge.target_table::TEXT;
    v_update_set_clause TEXT;
    v_all_cols_ident TEXT;
    v_all_cols_select TEXT;
    v_entity_key_join_clause TEXT;
    v_target_schema_name name;
    v_target_table_name_only name;
    v_valid_from_col name;
    v_valid_until_col name;
    v_valid_to_col name;
    v_range_col name;
    v_range_constructor name;
    v_has_gist_index boolean;
    v_has_target_gist_index boolean;
    v_has_lookup_btree_index boolean;
    v_log_index_checks boolean;
    v_log_trace boolean;
    v_log_sql boolean;
    v_log_plan boolean;
    v_log_feedback boolean;
    v_log_vars boolean;
    v_expected_idx_expr_with_bounds text;
    v_expected_idx_expr_default text;
    v_idx_rec record;
    v_source_rel_oid oid;
    v_source_rel_name_for_hint regclass;
    v_valid_from_col_type regtype;
    v_valid_until_col_type regtype;
    v_insert_defaulted_columns TEXT[];
    v_all_cols_from_jsonb TEXT;
    v_founding_all_cols_ident TEXT;
    v_founding_all_cols_from_jsonb TEXT;
    v_internal_keys_to_remove TEXT[];
    v_pk_cols name[];
    v_feedback_set_clause TEXT;
    v_sql TEXT;
    v_correlation_col name;
    v_log_id TEXT;
    v_summary_line TEXT;
BEGIN
    v_log_trace := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_trace', true), ''), 'false')::boolean;
    v_log_sql := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_sql', true), ''), 'false')::boolean;
    v_log_index_checks := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_index_checks', true), ''), 'false')::boolean;
    v_log_plan := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_plan', true), ''), 'false')::boolean;
    v_log_feedback := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_feedback', true), ''), 'false')::boolean;
    v_log_vars := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_vars', true), ''), 'false')::boolean;
    v_log_id := substr(md5(COALESCE(current_setting('sql_saga.temporal_merge.log_id_seed', true), random()::text)), 1, 3);

    -- An entity must be identifiable. At least one set of identity columns must be provided.
    IF (temporal_merge.identity_columns IS NULL OR cardinality(temporal_merge.identity_columns) = 0) AND
       (temporal_merge.natural_identity_columns IS NULL OR cardinality(temporal_merge.natural_identity_columns) = 0)
    THEN
        RAISE EXCEPTION 'At least one of identity_columns or natural_identity_columns must be a non-empty array.';
    END IF;

    v_lookup_columns := COALESCE(temporal_merge.natural_identity_columns, temporal_merge.identity_columns);
    v_correlation_col := COALESCE(temporal_merge.identity_correlation_column, temporal_merge.row_id_column);
    
    v_summary_line := format(
        'on %s: mode=>%s, delete_mode=>%s, identity_columns=>%L, natural_identity_columns=>%L, ephemeral_columns=>%L, identity_correlation_column=>%L, row_id_column=>%L',
        temporal_merge.target_table,
        temporal_merge.mode,
        temporal_merge.delete_mode,
        temporal_merge.identity_columns,
        temporal_merge.natural_identity_columns,
        temporal_merge.ephemeral_columns,
        temporal_merge.identity_correlation_column,
        temporal_merge.row_id_column
    );

    -- Introspect the primary key columns. They will be excluded from UPDATE SET clauses.
    SELECT COALESCE(array_agg(a.attname), '{}'::name[])
    INTO v_pk_cols
    FROM pg_constraint c
    JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
    WHERE c.conrelid = temporal_merge.target_table AND c.contype = 'p';

    v_internal_keys_to_remove := ARRAY[]::name[];

    IF temporal_merge.identity_correlation_column IS NOT NULL THEN
        v_internal_keys_to_remove := v_internal_keys_to_remove || temporal_merge.identity_correlation_column;
    ELSE
        v_internal_keys_to_remove := v_internal_keys_to_remove || temporal_merge.row_id_column;
    END IF;

    -- To ensure idempotency and avoid permission errors in complex transactions
    -- with role changes, we drop and recreate the feedback table for each call.
    IF to_regclass('pg_temp.temporal_merge_feedback') IS NOT NULL THEN
        DROP TABLE pg_temp.temporal_merge_feedback;
    END IF;
    CREATE TEMP TABLE temporal_merge_feedback (LIKE sql_saga.temporal_merge_feedback) ON COMMIT DROP;

    -- Create a unified, session-local cache for index checks to avoid redundant lookups.
    IF to_regclass('pg_temp.temporal_merge_cache') IS NULL THEN
        CREATE TEMP TABLE temporal_merge_cache (
            rel_oid oid NOT NULL,
            lookup_columns text[], -- NULL for GIST checks. NOT NULL for BTREE checks.
            has_index boolean NOT NULL,
            hint_rel_name regclass, -- NULL for BTREE checks. NOT NULL for GIST checks.

            -- Enforce that this is either a GIST check or a BTREE check.
            CHECK (
                (lookup_columns IS NULL AND hint_rel_name IS NOT NULL) -- GIST
                OR
                (lookup_columns IS NOT NULL AND hint_rel_name IS NULL) -- BTREE
            )
        ) ON COMMIT DROP;

        -- Unique index for GIST checks
        CREATE UNIQUE INDEX ON temporal_merge_cache (rel_oid) WHERE lookup_columns IS NULL;
        -- Unique index for BTREE checks
        CREATE UNIQUE INDEX ON temporal_merge_cache (rel_oid, lookup_columns) WHERE lookup_columns IS NOT NULL;
    END IF;

    -- Introspect era information to get the correct column names
    SELECT n.nspname, c.relname
    INTO v_target_schema_name, v_target_table_name_only
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = temporal_merge.target_table;

    SELECT e.valid_from_column_name, e.valid_until_column_name, e.synchronize_valid_to_column, e.synchronize_range_column, e.range_type::name
    INTO v_valid_from_col, v_valid_until_col, v_valid_to_col, v_range_col, v_range_constructor
    FROM sql_saga.era e
    WHERE e.table_schema = v_target_schema_name
      AND e.table_name = v_target_table_name_only
      AND e.era_name = temporal_merge.era_name;

    IF v_valid_from_col IS NULL THEN
        RAISE EXCEPTION 'No era named "%" found for table "%"', temporal_merge.era_name, temporal_merge.target_table;
    END IF;

    -- Prepare expected normalized index expressions and logging flag for index checks
    v_expected_idx_expr_with_bounds := format('%s(%s,%s,''[)'')', v_range_constructor, v_valid_from_col, v_valid_until_col);
    v_expected_idx_expr_default := format('%s(%s,%s)', v_range_constructor, v_valid_from_col, v_valid_until_col);

    -- Check cache for original source relation OID. This provides a fast path for repeated calls with the same view.
    SELECT has_index, hint_rel_name
    INTO v_has_gist_index, v_source_rel_name_for_hint
    FROM pg_temp.temporal_merge_cache
    WHERE rel_oid = source_table AND lookup_columns IS NULL;

    IF NOT FOUND THEN
        -- On cache miss, resolve the relation if it's a view, perform the check, and populate the cache.
        DECLARE
            v_source_relkind char;
            v_is_view boolean;
        BEGIN
            SELECT c.relkind INTO v_source_relkind
            FROM pg_class c WHERE c.oid = source_table;
            v_is_view := (v_source_relkind = 'v');

            IF v_is_view THEN
                -- It's a view, find the underlying table. Assumes a simple view with one base table.
                SELECT d.refobjid INTO v_source_rel_oid
                FROM pg_rewrite r
                JOIN pg_depend d ON r.oid = d.objid
                JOIN pg_class c_dep ON c_dep.oid = d.refobjid
                WHERE r.ev_class = source_table
                  AND d.refclassid = 'pg_class'::regclass
                  AND r.rulename = '_RETURN'
                  AND c_dep.relkind IN ('r', 'p', 'f') -- regular, partitioned, foreign
                LIMIT 1;

                v_source_rel_name_for_hint := v_source_rel_oid;
            ELSE
                v_source_rel_oid := source_table;
                v_source_rel_name_for_hint := source_table;
            END IF;

            -- If we couldn't resolve a base table for a complex view, we cannot check for an index.
            -- To avoid repeated expensive checks for this source, we assume an index exists and cache that assumption.
            IF v_source_rel_oid IS NULL THEN
                RAISE WARNING 'Could not determine the base table for source relation "%" (it may be a complex view). Skipping GIST index performance check.', source_table;
                v_has_gist_index := true;
                v_source_rel_name_for_hint := source_table; -- Fallback
            ELSE
                -- We have a resolvable base table. Check cache for it.
                SELECT has_index
                INTO v_has_gist_index
                FROM pg_temp.temporal_merge_cache
                WHERE rel_oid = v_source_rel_oid AND lookup_columns IS NULL;

                IF NOT FOUND THEN
                    -- Still a cache miss, so perform the actual check on the base table.
                    SELECT EXISTS (
                        SELECT 1
                        FROM pg_index ix
                        JOIN pg_class i ON i.oid = ix.indexrelid
                        JOIN pg_am am ON am.oid = i.relam
                        WHERE ix.indrelid = v_source_rel_oid
                        AND am.amname = 'gist'
                        AND ix.indexprs IS NOT NULL
                        -- Normalize the index expression to create a robust, format-agnostic comparison.
                        AND (
                            regexp_replace(pg_get_expr(ix.indexprs, ix.indrelid), '\s|::\w+', '', 'g') = v_expected_idx_expr_with_bounds
                            OR regexp_replace(pg_get_expr(ix.indexprs, ix.indrelid), '\s|::\w+', '', 'g') = v_expected_idx_expr_default
                        )
                    )
                    INTO v_has_gist_index;

                    IF v_log_index_checks THEN
                        RAISE NOTICE 'Index check (SOURCE %) expected: "%", or "%"', v_source_rel_name_for_hint, v_expected_idx_expr_with_bounds, v_expected_idx_expr_default;
                        FOR v_idx_rec IN
                            SELECT i.relname AS index_name,
                                   pg_get_indexdef(i.oid) AS indexdef,
                                   regexp_replace(pg_get_expr(ix.indexprs, ix.indrelid), '\s|::\w+', '', 'g') AS normalized_expr
                            FROM pg_index ix
                            JOIN pg_class i ON i.oid = ix.indexrelid
                            JOIN pg_am am ON am.oid = i.relam
                            WHERE ix.indrelid = v_source_rel_oid
                              AND am.amname = 'gist'
                        LOOP
                            RAISE NOTICE 'GiST index: %, normalized expr: %', v_idx_rec.indexdef, v_idx_rec.normalized_expr;
                        END LOOP;
                        RAISE NOTICE 'Detected has_gist_index(SOURCE): %', v_has_gist_index;
                    END IF;

                    -- Populate cache for the base table.
                    INSERT INTO pg_temp.temporal_merge_cache (rel_oid, lookup_columns, has_index, hint_rel_name)
                    VALUES (v_source_rel_oid, NULL, v_has_gist_index, v_source_rel_name_for_hint);
                END IF;
            END IF;

            -- Now, populate the cache for the original source relation OID (which could be the view).
            -- This is the key optimization for subsequent calls with the same view.
            -- We only need to do this if the original source was different from the resolved one (i.e., it was a view).
            IF source_table <> v_source_rel_oid THEN
                INSERT INTO pg_temp.temporal_merge_cache (rel_oid, lookup_columns, has_index, hint_rel_name)
                VALUES (source_table, NULL, v_has_gist_index, v_source_rel_name_for_hint);
            END IF;
        END;
    END IF;

    IF NOT v_has_gist_index AND v_source_rel_name_for_hint IS NOT NULL THEN
        DECLARE
            v_source_row_count REAL;
        BEGIN
            -- Use the resolved base table OID for the row count check, as views often have reltuples=0.
            SELECT c.reltuples INTO v_source_row_count FROM pg_class c WHERE c.oid = v_source_rel_name_for_hint::oid;
            IF v_source_row_count >= 512 THEN
                RAISE WARNING 'Performance warning: The source relation % lacks a GIST index on its temporal columns.', source_table
                USING HINT = format('For better performance, consider creating an index, e.g., CREATE INDEX ON %s USING GIST (%s(%I, %I, ''[)''));',
                    v_source_rel_name_for_hint,
                    v_range_constructor,
                    v_valid_from_col,
                    v_valid_until_col
                );
            END IF;
        END;
    END IF;

    -- Check for GiST index on target table's temporal range.
    -- We have already validated that the target table is a sql_saga era (see the earlier SELECT from sql_saga.era),
    -- and loaded v_valid_from_col, v_valid_until_col and v_range_constructor accordingly. Therefore it is both
    -- correct and sufficient to check for a GiST index on the expression
    --   range_constructor(valid_from, valid_until, '[)')
    -- for performance. Checking for a primary key is orthogonal and does not affect range search performance here.
    SELECT EXISTS (
        SELECT 1
        FROM pg_index ix
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am ON am.oid = i.relam
        WHERE ix.indrelid = v_target_table_ident::regclass
          AND am.amname = 'gist'
          AND ix.indexprs IS NOT NULL
          AND (
              regexp_replace(pg_get_expr(ix.indexprs, ix.indrelid), '\s|::\w+', '', 'g') = v_expected_idx_expr_with_bounds
              OR regexp_replace(pg_get_expr(ix.indexprs, ix.indrelid), '\s|::\w+', '', 'g') = v_expected_idx_expr_default
          )
    )
    INTO v_has_target_gist_index;

    IF v_log_index_checks THEN
        RAISE NOTICE 'Index check (TARGET %) expected: "%", or "%"', temporal_merge.target_table, v_expected_idx_expr_with_bounds, v_expected_idx_expr_default;
        FOR v_idx_rec IN
            SELECT i.relname AS index_name,
                   pg_get_indexdef(i.oid) AS indexdef,
                   regexp_replace(pg_get_expr(ix.indexprs, ix.indrelid), '\s|::\w+', '', 'g') AS normalized_expr
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_am am ON am.oid = i.relam
            WHERE ix.indrelid = v_target_table_ident::regclass
              AND am.amname = 'gist'
        LOOP
            RAISE NOTICE 'GiST index: %, normalized expr: %', v_idx_rec.indexdef, v_idx_rec.normalized_expr;
        END LOOP;
        RAISE NOTICE 'Detected has_gist_index(TARGET): %', v_has_target_gist_index;
    END IF;

    IF NOT v_has_target_gist_index THEN
        DECLARE
            v_target_row_count REAL;
        BEGIN
            SELECT c.reltuples INTO v_target_row_count FROM pg_class c WHERE c.oid = temporal_merge.target_table;
            IF v_target_row_count >= 512 THEN
                RAISE WARNING 'Performance warning: The target relation % lacks a GIST index on its temporal columns.', temporal_merge.target_table
                USING HINT = format('For better performance, consider creating an index, e.g., CREATE INDEX ON %s USING GIST (%s(%I, %I, ''[)''));',
                    v_target_table_ident,
                    v_range_constructor,
                    v_valid_from_col,
                    v_valid_until_col
                );
            END IF;
        END;
    END IF;

    -- Check for performance-critical BTREE index on target table's lookup columns.
    -- This check is cached per transaction to avoid redundant lookups.
    SELECT has_index
    INTO v_has_lookup_btree_index
    FROM pg_temp.temporal_merge_cache
    WHERE rel_oid = temporal_merge.target_table
      AND lookup_columns = v_lookup_columns;

    IF NOT FOUND THEN
        SELECT EXISTS (
            SELECT 1
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_am am ON am.oid = i.relam
            WHERE ix.indrelid = v_target_table_ident::regclass
              AND am.amname = 'btree'
              AND array_length(ix.indkey, 1) >= cardinality(v_lookup_columns)
              AND (
                  SELECT array_agg(a.attname ORDER BY k.ord)
                  FROM unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord)
                  JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = k.attnum
                  WHERE k.ord <= cardinality(v_lookup_columns)
              )::name[] = v_lookup_columns::name[]
        )
        INTO v_has_lookup_btree_index;

        INSERT INTO pg_temp.temporal_merge_cache (rel_oid, lookup_columns, has_index, hint_rel_name)
        VALUES (temporal_merge.target_table, v_lookup_columns, v_has_lookup_btree_index, NULL);
    END IF;

    IF NOT v_has_lookup_btree_index THEN
        DECLARE
            v_target_row_count REAL;
        BEGIN
            SELECT c.reltuples INTO v_target_row_count FROM pg_class c WHERE c.oid = temporal_merge.target_table;
            IF v_target_row_count >= 512 THEN
                RAISE NOTICE 'Performance hint: Consider adding a BTREE index on the target relation''s lookup columns (%) to accelerate entity filtering.', v_lookup_columns
                USING HINT = format(
                    'CREATE INDEX ON %s (%s);',
                    v_target_table_ident,
                    (SELECT string_agg(format('%I', col), ', ') FROM unnest(v_lookup_columns) AS col)
                );
            END IF;
        END;
    END IF;

    -- Auto-detect columns that should be excluded from INSERT statements.
    -- This includes columns with defaults, identity columns, and generated columns.
    SELECT COALESCE(array_agg(a.attname), '{}')
    INTO v_insert_defaulted_columns
    FROM pg_catalog.pg_attribute a
    WHERE a.attrelid = temporal_merge.target_table
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND (a.atthasdef OR a.attidentity IN ('a', 'd') OR a.attgenerated <> '');

    -- Also exclude synchronized columns, as the trigger will populate them.
    IF v_valid_to_col IS NOT NULL THEN
        v_insert_defaulted_columns := v_insert_defaulted_columns || v_valid_to_col;
    END IF;
    IF v_range_col IS NOT NULL THEN
        v_insert_defaulted_columns := v_insert_defaulted_columns || v_range_col;
    END IF;

    SELECT atttypid::regtype INTO v_valid_from_col_type FROM pg_attribute WHERE attrelid = temporal_merge.target_table AND attname = v_valid_from_col;
    SELECT atttypid::regtype INTO v_valid_until_col_type FROM pg_attribute WHERE attrelid = temporal_merge.target_table AND attname = v_valid_until_col;

    -- Dynamically construct join clause for composite entity key.
    -- This uses an index-friendly, null-safe pattern.
    SELECT
        string_agg(format('(t.%1$I = jpr_entity.%1$I OR (t.%1$I IS NULL AND jpr_entity.%1$I IS NULL))', col), ' AND ')
    INTO
        v_entity_key_join_clause
    FROM unnest(v_lookup_columns) AS col;

    v_entity_key_join_clause := COALESCE(v_entity_key_join_clause, 'true');

    IF to_regclass('pg_temp.temporal_merge_plan') IS NOT NULL THEN
        DROP TABLE pg_temp.temporal_merge_plan;
    END IF;
    CREATE TEMP TABLE temporal_merge_plan (LIKE sql_saga.temporal_merge_plan, PRIMARY KEY (plan_op_seq)) ON COMMIT DROP;

    INSERT INTO temporal_merge_plan
    SELECT * FROM sql_saga.temporal_merge_plan(
        target_table => temporal_merge.target_table,
        source_table => temporal_merge.source_table,
        identity_columns => temporal_merge.identity_columns,
        mode => temporal_merge.mode,
        era_name => temporal_merge.era_name,
        row_id_column => temporal_merge.row_id_column,
        identity_correlation_column => temporal_merge.identity_correlation_column,
        delete_mode => temporal_merge.delete_mode,
        natural_identity_columns => temporal_merge.natural_identity_columns,
        ephemeral_columns => temporal_merge.ephemeral_columns,
        p_log_trace => v_log_trace,
        p_log_sql => v_log_sql
    );

    -- Conditionally output the plan for debugging, based on a session variable.
    DECLARE
        v_plan_rec RECORD;
    BEGIN
        IF v_log_plan THEN
            RAISE NOTICE 'temporal_merge plan (%) %', v_log_id, v_summary_line;
            FOR v_plan_rec IN SELECT * FROM temporal_merge_plan ORDER BY plan_op_seq LOOP
                RAISE NOTICE '(%) %', v_log_id, json_build_object(
                    'plan_op_seq', v_plan_rec.plan_op_seq,
                    'row_ids', v_plan_rec.row_ids,
                    'operation', v_plan_rec.operation,
                    'update_effect', v_plan_rec.update_effect,
                    'entity_ids', v_plan_rec.entity_ids,
                    'old_valid_from', v_plan_rec.old_valid_from,
                    'old_valid_until', v_plan_rec.old_valid_until,
                    'new_valid_from', v_plan_rec.new_valid_from,
                    'new_valid_until', v_plan_rec.new_valid_until,
                    'data', v_plan_rec.data,
                    'relation', v_plan_rec.relation,
                    'trace', v_plan_rec.trace
                );
            END LOOP;
        END IF;
    END;

        -- Get dynamic column lists for DML. The data columns are defined as all columns
        -- in the target table, minus the identity and temporal boundary columns.
        -- This is intentionally different from the planner's introspection, as the executor
        -- must be able to handle a final payload that contains columns inherited from
        -- the target's history, which may not be present in the source.
        WITH target_cols AS (
            SELECT pa.attname, pa.atttypid, pa.attgenerated, pa.attidentity
            FROM pg_catalog.pg_attribute pa
            WHERE pa.attrelid = temporal_merge.target_table AND pa.attnum > 0 AND NOT pa.attisdropped
        ),
        common_data_cols AS (
            SELECT t.attname, t.atttypid
            FROM target_cols t
            WHERE t.attname NOT IN (v_valid_from_col, v_valid_until_col)
              AND t.attname <> ALL(COALESCE(temporal_merge.identity_columns, '{}'))
              AND t.attname <> ALL(v_lookup_columns)
              AND t.attname <> ALL(COALESCE(v_pk_cols, '{}'))
              AND t.attidentity <> 'a' -- Exclude GENERATED ALWAYS AS IDENTITY
              AND t.attgenerated = '' -- Exclude GENERATED ... STORED
        ),
        all_available_cols AS (
            SELECT c.attname, c.atttypid FROM common_data_cols c
            UNION
            SELECT u.attname, t.atttypid
            FROM unnest(v_lookup_columns) u(attname)
            JOIN target_cols t ON u.attname = t.attname
            UNION
            SELECT u.attname, t.atttypid
            FROM unnest(COALESCE(temporal_merge.identity_columns, '{}')) u(attname)
            JOIN target_cols t ON u.attname = t.attname
        ),
        cols_for_insert AS (
            -- All available columns that DON'T have a default...
            SELECT attname, atttypid FROM all_available_cols WHERE attname <> ALL(v_insert_defaulted_columns)
            UNION
            -- ...plus all identity columns (stable and natural), which must be provided for SCD-2 inserts.
            SELECT attname, atttypid FROM all_available_cols WHERE attname = ANY(temporal_merge.identity_columns) OR attname = ANY(v_lookup_columns)
        ),
        cols_for_founding_insert AS (
            -- For "founding" INSERTs of new entities, we only include columns that do NOT have a default.
            -- This allows serial/identity columns to be generated by the database.
            SELECT attname, atttypid
            FROM all_available_cols
            WHERE attname <> ALL(v_insert_defaulted_columns)
        )
        SELECT
            (SELECT string_agg(format('%1$I = CASE WHEN p.data ? %2$L THEN (p.data->>%2$L)::%3$s ELSE t.%1$I END', cdc.attname, cdc.attname, format_type(cdc.atttypid, -1)), ', ') FROM common_data_cols cdc),
            (SELECT string_agg(format('%I', cfi.attname), ', ') FROM cols_for_insert cfi WHERE cfi.attname NOT IN (v_valid_from_col, v_valid_until_col)),
            (SELECT string_agg(format('jpr_all.%I', cfi.attname), ', ') FROM cols_for_insert cfi WHERE cfi.attname NOT IN (v_valid_from_col, v_valid_until_col)),
            (SELECT string_agg(format('(s.full_data->>%L)::%s', cfi.attname, format_type(cfi.atttypid, -1)), ', ')
             FROM cols_for_insert cfi
             WHERE cfi.attname NOT IN (v_valid_from_col, v_valid_until_col)),
            (SELECT string_agg(format('%I', cffi.attname), ', ') FROM cols_for_founding_insert cffi WHERE cffi.attname NOT IN (v_valid_from_col, v_valid_until_col)),
            (SELECT string_agg(format('(s.full_data->>%L)::%s', cffi.attname, format_type(cffi.atttypid, -1)), ', ')
             FROM cols_for_founding_insert cffi
             WHERE cffi.attname NOT IN (v_valid_from_col, v_valid_until_col))
        INTO
            v_update_set_clause,
            v_all_cols_ident,
            v_all_cols_select,
            v_all_cols_from_jsonb,
            v_founding_all_cols_ident,
            v_founding_all_cols_from_jsonb;

        SET CONSTRAINTS ALL DEFERRED;

        -- INSERT -> UPDATE -> DELETE order is critical for sql_saga compatibility.
        -- 1. Execute INSERT operations and capture generated IDs
        IF v_all_cols_ident IS NOT NULL THEN
             DECLARE
                v_entity_id_update_jsonb_build TEXT;
             BEGIN
                -- Build the expression to construct the entity_ids feedback JSONB.
                -- This should include the conceptual entity ID columns AND any surrogate key.
                -- A simple and effective heuristic is to always include a column named 'id' if it exists on the target table.
                WITH target_cols AS (
                    SELECT pa.attname
                    FROM pg_catalog.pg_attribute pa
                    WHERE pa.attrelid = v_target_table_ident::regclass
                      AND pa.attnum > 0 AND NOT pa.attisdropped
                ),
                feedback_id_cols AS (
                    SELECT col FROM unnest(v_lookup_columns) as col
                    UNION
                    SELECT 'id'
                    WHERE 'id' IN (SELECT attname FROM target_cols) AND 'id' <> ALL(v_lookup_columns)
                )
                SELECT
                    format('jsonb_build_object(%s)', string_agg(format('%L, ir.%I', col, col), ', '))
                INTO
                    v_entity_id_update_jsonb_build
                FROM feedback_id_cols;

                -- Stage 1: Handle "founding" inserts for new entities that need generated keys.
                -- This unified "Smart Merge" logic now handles all such cases.
                IF (SELECT TRUE FROM temporal_merge_plan WHERE operation = 'INSERT' AND entity_ids ? v_correlation_col LIMIT 1)
                THEN
                    CREATE TEMP TABLE temporal_merge_entity_id_map (ident_corr TEXT PRIMARY KEY, new_entity_ids JSONB) ON COMMIT DROP;

                    -- Step 1.1: Insert just ONE row for each new conceptual entity to generate its ID.
                    EXECUTE format($$
                        WITH founding_plan_ops AS (
                            SELECT DISTINCT ON (p.entity_ids->>%7$L)
                                p.plan_op_seq,
                                p.entity_ids->>%7$L as ident_corr,
                                p.new_valid_from,
                                p.new_valid_until,
                                p.entity_ids || p.data as full_data
                            FROM temporal_merge_plan p
                            WHERE p.operation = 'INSERT' AND p.entity_ids ? %7$L
                            ORDER BY p.entity_ids->>%7$L, p.plan_op_seq
                        ),
                        id_map_cte AS (
                            MERGE INTO %1$s t
                            USING founding_plan_ops s ON false
                            WHEN NOT MATCHED THEN
                                INSERT (%2$s, %5$I, %6$I)
                                VALUES (%3$s, s.new_valid_from::%8$s, s.new_valid_until::%9$s)
                            RETURNING t.*, s.ident_corr
                        )
                        INSERT INTO temporal_merge_entity_id_map (ident_corr, new_entity_ids)
                        SELECT
                            ir.ident_corr,
                            %4$s -- v_entity_id_update_jsonb_build expression
                        FROM id_map_cte ir;
                    $$,
                        v_target_table_ident,           /* %1$s */
                        v_founding_all_cols_ident,      /* %2$s */
                        v_founding_all_cols_from_jsonb, /* %3$s */
                        v_entity_id_update_jsonb_build, /* %4$s */
                        v_valid_from_col,               /* %5$I */
                        v_valid_until_col,              /* %6$I */
                        v_correlation_col,              /* %7$L */
                        v_valid_from_col_type,          /* %8$s */
                        v_valid_until_col_type          /* %9$s */
                    );

                    -- Step 1.2: Back-fill the generated IDs into the plan for all dependent operations.
                    EXECUTE format($$
                        UPDATE temporal_merge_plan p
                        SET entity_ids = m.new_entity_ids || jsonb_build_object(%1$L, p.entity_ids->>%1$L)
                        FROM temporal_merge_entity_id_map m
                        WHERE p.entity_ids->>%1$L = m.ident_corr;
                    $$, v_correlation_col);

                    -- Step 1.3: Insert the remaining slices for the new entities, which now have the correct foreign key.
                    EXECUTE format($$
                        INSERT INTO %1$s (%2$s, %4$I, %5$I)
                        SELECT %3$s, p.new_valid_from::%7$s, p.new_valid_until::%8$s
                        FROM temporal_merge_plan p,
                             LATERAL jsonb_populate_record(null::%1$s, p.entity_ids || p.data) as jpr_all
                        WHERE p.operation = 'INSERT'
                          AND p.entity_ids ? %6$L -- Only founding inserts
                          AND NOT EXISTS ( -- Exclude the "founding" rows we already inserted in Step 1.1
                            SELECT 1 FROM (
                                SELECT DISTINCT ON (p_inner.entity_ids->>%6$L) plan_op_seq
                                FROM temporal_merge_plan p_inner
                                WHERE p_inner.operation = 'INSERT' AND p_inner.entity_ids ? %6$L
                                ORDER BY p_inner.entity_ids->>%6$L, p_inner.plan_op_seq
                            ) AS founding_ops
                            WHERE founding_ops.plan_op_seq = p.plan_op_seq
                          )
                        ORDER BY p.plan_op_seq;
                    $$,
                        v_target_table_ident,       /* %1$s */
                        v_all_cols_ident,           /* %2$s */
                        v_all_cols_select,          /* %3$s */
                        v_valid_from_col,           /* %4$I */
                        v_valid_until_col,          /* %5$I */
                        v_correlation_col,          /* %6$L */
                        v_valid_from_col_type,      /* %7$s */
                        v_valid_until_col_type      /* %8$s */
                    );

                    DROP TABLE temporal_merge_entity_id_map;
                END IF;

                -- Stage 2: Handle "non-founding" inserts (e.g., for SCD-2), which have pre-existing keys.
                EXECUTE format($$
                    WITH
                    source_for_insert AS (
                        SELECT
                            p.plan_op_seq, p.new_valid_from, p.new_valid_until,
                            p.entity_ids || p.data as full_data
                        FROM temporal_merge_plan p
                        WHERE p.operation = 'INSERT' AND NOT (p.entity_ids ? %1$L)
                    ),
                    inserted_rows AS (
                        MERGE INTO %2$s t
                        USING source_for_insert s ON false
                        WHEN NOT MATCHED THEN
                            INSERT (%3$s, %6$I, %7$I)
                            VALUES (%4$s, s.new_valid_from::%8$s, s.new_valid_until::%9$s)
                        RETURNING t.*, s.plan_op_seq
                    )
                    UPDATE temporal_merge_plan p
                    SET entity_ids = %5$s
                    FROM inserted_rows ir
                    WHERE p.plan_op_seq = ir.plan_op_seq;
                $$,
                    v_correlation_col,                  /* %1$L */
                    v_target_table_ident,               /* %2$s */
                    v_all_cols_ident,                   /* %3$s */
                    v_all_cols_from_jsonb,              /* %4$s */
                    v_entity_id_update_jsonb_build,     /* %5$s */
                    v_valid_from_col,                   /* %6$I */
                    v_valid_until_col,                  /* %7$I */
                    v_valid_from_col_type,              /* %8$s */
                    v_valid_until_col_type              /* %9$s */
                );
             END;
        ELSE
            -- This case handles tables with only temporal and defaulted ID columns.
             DECLARE
                v_entity_id_update_jsonb_build TEXT;
             BEGIN
                -- Build the expression to construct the entity_ids feedback JSONB.
                -- This should include the conceptual entity ID columns AND any surrogate key.
                -- A simple and effective heuristic is to always include a column named 'id' if it exists on the target table.
                WITH target_cols AS (
                    SELECT pa.attname
                    FROM pg_catalog.pg_attribute pa
                    WHERE pa.attrelid = v_target_table_ident::regclass
                      AND pa.attnum > 0 AND NOT pa.attisdropped
                ),
                feedback_id_cols AS (
                    SELECT unnest(temporal_merge.identity_columns) as col
                    UNION
                    SELECT 'id'
                    WHERE 'id' IN (SELECT attname FROM target_cols)
                )
                SELECT
                    format('jsonb_build_object(%s)', string_agg(format('%L, ir.%I', col, col), ', '))
                INTO
                    v_entity_id_update_jsonb_build
                FROM feedback_id_cols;

                -- This case should not be reachable with the "Smart Merge" logic,
                -- but we use the robust MERGE pattern for safety.
                v_sql := format($$
                    WITH
                    source_for_insert AS (
                        SELECT
                            p.plan_op_seq,
                            p.new_valid_from,
                            p.new_valid_until
                        FROM temporal_merge_plan p
                        WHERE p.operation = 'INSERT'
                    ),
                    inserted_rows AS (
                        MERGE INTO %1$s t
                        USING source_for_insert s ON false
                        WHEN NOT MATCHED THEN
                            INSERT (%3$I, %4$I)
                            VALUES (s.new_valid_from::%5$s, s.new_valid_until::%6$s)
                        RETURNING t.*, s.plan_op_seq
                    )
                    UPDATE temporal_merge_plan p
                    SET entity_ids = %2$s
                    FROM inserted_rows ir
                    WHERE p.plan_op_seq = ir.plan_op_seq;
                $$,
                    v_target_table_ident,           /* %1$s */
                    v_entity_id_update_jsonb_build, /* %2$s */
                    v_valid_from_col,               /* %3$I */
                    v_valid_until_col,              /* %4$I */
                    v_valid_from_col_type,          /* %5$s */
                    v_valid_until_col_type          /* %6$s */
                );
                EXECUTE v_sql;
             END;
        END IF;

        -- Back-fill source table with generated IDs if requested.
        IF temporal_merge.update_source_with_identity THEN
            DECLARE
                v_source_update_set_clause TEXT;
            BEGIN
                -- Build a SET clause for the stable identity columns. This writes back
                -- any generated surrogate keys to the source table, but correctly
                -- excludes any natural key columns that were used for lookup only.
                SELECT string_agg(
                    format('%I = (p.entity_ids->>%L)::%s', j.key, j.key, format_type(a.atttypid, a.atttypmod)),
                    ', '
                )
                INTO v_source_update_set_clause
                FROM (
                    SELECT key FROM jsonb_object_keys(
                        (SELECT entity_ids FROM temporal_merge_plan WHERE entity_ids IS NOT NULL and operation = 'INSERT' LIMIT 1)
                    ) as key
                    WHERE key = ANY(temporal_merge.identity_columns)
                ) j
                JOIN pg_attribute a ON a.attname = j.key
                WHERE a.attrelid = temporal_merge.source_table AND NOT a.attisdropped AND a.attnum > 0;

                IF v_source_update_set_clause IS NOT NULL THEN
                    v_sql := format($$
                        WITH map_row_to_entity AS (
                            SELECT DISTINCT ON (s.source_row_id)
                                s.source_row_id,
                                p.entity_ids
                            FROM (SELECT DISTINCT unnest(row_ids) AS source_row_id FROM temporal_merge_plan WHERE operation = 'INSERT') s
                            JOIN temporal_merge_plan p ON s.source_row_id = ANY(p.row_ids)
                            WHERE p.entity_ids IS NOT NULL
                            ORDER BY s.source_row_id, p.plan_op_seq
                        )
                        UPDATE %1$s s
                        SET %2$s
                        FROM map_row_to_entity p
                        WHERE s.%3$I = p.source_row_id;
                    $$, temporal_merge.source_table::text, v_source_update_set_clause, temporal_merge.row_id_column);
                    EXECUTE v_sql;
                END IF;
            END;
        END IF;

        -- 2. Execute UPDATE operations.
        -- As proven by test 58, we can use a single, ordered UPDATE statement.
        -- The ORDER BY on the plan's sequence number ensures that "grow"
        -- operations are processed before "shrink" or "move" operations,
        -- preventing transient gaps that would violate foreign key constraints.
        IF v_update_set_clause IS NOT NULL THEN
            v_sql := format($$ UPDATE %1$s t SET %4$I = p.new_valid_from::%6$s, %5$I = p.new_valid_until::%7$s, %2$s
                FROM (SELECT * FROM temporal_merge_plan WHERE operation = 'UPDATE' ORDER BY plan_op_seq) p,
                     LATERAL jsonb_populate_record(null::%1$s, p.entity_ids) AS jpr_entity
                WHERE %3$s AND t.%4$I = p.old_valid_from::%6$s;
            $$, v_target_table_ident, v_update_set_clause, v_entity_key_join_clause, v_valid_from_col, v_valid_until_col, v_valid_from_col_type, v_valid_until_col_type);
            EXECUTE v_sql;
        ELSIF v_all_cols_ident IS NOT NULL THEN
            v_sql := format($$ UPDATE %1$s t SET %3$I = p.new_valid_from::%5$s, %4$I = p.new_valid_until::%6$s
                FROM (SELECT * FROM temporal_merge_plan WHERE operation = 'UPDATE' ORDER BY plan_op_seq) p,
                     LATERAL jsonb_populate_record(null::%1$s, p.entity_ids) AS jpr_entity
                WHERE %2$s AND t.%3$I = p.old_valid_from::%5$s;
            $$, v_target_table_ident, v_entity_key_join_clause, v_valid_from_col, v_valid_until_col, v_valid_from_col_type, v_valid_until_col_type);
            EXECUTE v_sql;
        END IF;

        -- 3. Execute DELETE operations
        IF (SELECT TRUE FROM temporal_merge_plan WHERE operation = 'DELETE' LIMIT 1) THEN
            v_sql := format($$ DELETE FROM %1$s t
                USING temporal_merge_plan p, LATERAL jsonb_populate_record(null::%1$s, p.entity_ids) AS jpr_entity
                WHERE p.operation = 'DELETE' AND %2$s AND t.%3$I = p.old_valid_from::%4$s;
            $$, v_target_table_ident, v_entity_key_join_clause, v_valid_from_col, v_valid_from_col_type);
            EXECUTE v_sql;
        END IF;

        SET CONSTRAINTS ALL IMMEDIATE;

        -- 4. Generate and store feedback
        v_sql := format($$
            WITH
            all_source_rows AS (
                SELECT t.%2$I AS source_row_id FROM %1$s t
            ),
            plan_unnested AS (
                SELECT unnest(p.row_ids) as source_row_id, p.plan_op_seq, p.entity_ids, p.operation
                FROM temporal_merge_plan p
            ),
            feedback_groups AS (
                SELECT
                    asr.source_row_id,
                    -- Aggregate all distinct operations for this source row.
                    array_agg(DISTINCT pu.operation) FILTER (WHERE pu.operation IS NOT NULL) as operations,
                    -- Aggregate all distinct entity IDs this source row touched.
                    COALESCE(jsonb_agg(DISTINCT (pu.entity_ids - %3$L::text[])) FILTER (WHERE pu.entity_ids IS NOT NULL), '[]'::jsonb) AS target_entity_ids
                FROM all_source_rows asr
                LEFT JOIN plan_unnested pu ON asr.source_row_id = pu.source_row_id
                GROUP BY asr.source_row_id
            )
            INSERT INTO temporal_merge_feedback
                SELECT
                    fg.source_row_id,
                    fg.target_entity_ids,
                    CASE
                        -- This CASE statement must be ordered from most to least specific to correctly classify outcomes.
                        -- This CASE statement directly translates the plan's actions into a final feedback status.
                        -- It is ordered from most to least specific to ensure correctness.
                        WHEN 'ERROR'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) THEN 'ERROR'
                        WHEN 'INSERT'::sql_saga.temporal_merge_plan_action = ANY(fg.operations)
                          OR 'UPDATE'::sql_saga.temporal_merge_plan_action = ANY(fg.operations)
                          OR 'DELETE'::sql_saga.temporal_merge_plan_action = ANY(fg.operations)
                        THEN 'APPLIED'
                        WHEN 'SKIP_NO_TARGET'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) THEN 'SKIPPED_NO_TARGET'
                        WHEN 'SKIP_FILTERED'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) THEN 'SKIPPED_FILTERED'
                        WHEN fg.operations = ARRAY['SKIP_IDENTICAL'::sql_saga.temporal_merge_plan_action] THEN 'SKIPPED_IDENTICAL'
                        -- If a source row resulted in no plan operations, it is an internal error.
                        WHEN fg.operations IS NULL THEN 'ERROR'
                        -- This is a safeguard. If the planner produces an unexpected combination of actions, we fail fast.
                        ELSE 'ERROR'
                    END::sql_saga.temporal_merge_feedback_status AS status,
                    CASE
                        WHEN 'ERROR'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) THEN 'Planner generated an ERROR action, indicating an internal logic error.'
                        WHEN fg.operations IS NULL THEN 'Planner failed to generate a plan for this source row.'
                        WHEN NOT (
                             'INSERT'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) OR
                             'UPDATE'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) OR
                             'DELETE'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) OR
                             'SKIP_NO_TARGET'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) OR
                             'SKIP_FILTERED'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) OR
                             fg.operations = ARRAY['SKIP_IDENTICAL'::sql_saga.temporal_merge_plan_action]
                        )
                        THEN 'Planner produced an unhandled combination of actions: ' || fg.operations::text
                        ELSE NULL::TEXT
                    END AS error_message
                FROM feedback_groups fg
                ORDER BY fg.source_row_id;
        $$,
            temporal_merge.source_table::text,       -- 1
            temporal_merge.row_id_column,     -- 2
            v_internal_keys_to_remove   -- 3
        );
        EXECUTE v_sql;

    -- Conditionally output the feedback for debugging, based on a session variable.
    DECLARE
        v_feedback_rec RECORD;
    BEGIN
        IF v_log_feedback THEN
            RAISE NOTICE 'temporal_merge feedback (%) %', v_log_id, v_summary_line;
            FOR v_feedback_rec IN SELECT * FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id LOOP
                RAISE NOTICE '(%) %', v_log_id, json_build_object(
                    'source_row_id', v_feedback_rec.source_row_id,
                    'target_entity_ids', v_feedback_rec.target_entity_ids,
                    'status', v_feedback_rec.status,
                    'error_message', v_feedback_rec.error_message
                );
            END LOOP;
        END IF;
    END;

    IF temporal_merge.update_source_with_feedback THEN
        IF temporal_merge.feedback_status_column IS NULL AND temporal_merge.feedback_error_column IS NULL THEN
            RAISE EXCEPTION 'When update_source_with_feedback is true, at least one feedback column (feedback_status_column or feedback_error_column) must be provided.';
        END IF;

        v_feedback_set_clause := '';

        -- If a status column is provided, build its part of the SET clause
        IF temporal_merge.feedback_status_column IS NOT NULL THEN
            IF temporal_merge.feedback_status_key IS NULL THEN
                RAISE EXCEPTION 'When feedback_status_column is provided, feedback_status_key must also be provided.';
            END IF;

            PERFORM 1 FROM pg_attribute WHERE attrelid = temporal_merge.source_table AND attname = temporal_merge.feedback_status_column AND atttypid = 'jsonb'::regtype AND NOT attisdropped AND attnum > 0;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'feedback_status_column "%" does not exist in source table %s or is not of type jsonb', temporal_merge.feedback_status_column, temporal_merge.source_table::text;
            END IF;

            v_feedback_set_clause := v_feedback_set_clause || format(
                '%I = COALESCE(s.%I, ''{}''::jsonb) || jsonb_build_object(%L, f.status)',
                temporal_merge.feedback_status_column, temporal_merge.feedback_status_column, temporal_merge.feedback_status_key
            );
        END IF;

        -- If an error column is provided, build its part of the SET clause
        IF temporal_merge.feedback_error_column IS NOT NULL THEN
            IF temporal_merge.feedback_error_key IS NULL THEN
                RAISE EXCEPTION 'When feedback_error_column is provided, feedback_error_key must also be provided.';
            END IF;

            PERFORM 1 FROM pg_attribute WHERE attrelid = temporal_merge.source_table AND attname = temporal_merge.feedback_error_column AND atttypid = 'jsonb'::regtype AND NOT attisdropped AND attnum > 0;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'feedback_error_column "%" does not exist in source table %s or is not of type jsonb', temporal_merge.feedback_error_column, temporal_merge.source_table::text;
            END IF;

            IF v_feedback_set_clause <> '' THEN
                v_feedback_set_clause := v_feedback_set_clause || ', ';
            END IF;

            v_feedback_set_clause := v_feedback_set_clause || format(
                '%I = CASE WHEN f.error_message IS NOT NULL THEN COALESCE(s.%I, ''{}''::jsonb) || jsonb_build_object(%L, f.error_message) ELSE COALESCE(s.%I, ''{}''::jsonb) - %L END',
                temporal_merge.feedback_error_column, temporal_merge.feedback_error_column, temporal_merge.feedback_error_key, temporal_merge.feedback_error_column, temporal_merge.feedback_error_key
            );
        END IF;

        v_sql := format($$
            UPDATE %1$s s
            SET %2$s
            FROM pg_temp.temporal_merge_feedback f
            WHERE s.%3$I = f.source_row_id;
        $$, temporal_merge.source_table::text, v_feedback_set_clause, temporal_merge.row_id_column);
        EXECUTE v_sql;
    END IF;

END;
$temporal_merge$;

COMMENT ON PROCEDURE sql_saga.temporal_merge(regclass, regclass, TEXT[], sql_saga.temporal_merge_mode, name, name, name, boolean, text[], sql_saga.temporal_merge_delete_mode, boolean, name, name, name, name, text[]) IS
'Executes a set-based temporal merge operation. It generates a plan using temporal_merge_plan and then executes it.';

