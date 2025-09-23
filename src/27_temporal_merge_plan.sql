-- Unified Planning Function
CREATE OR REPLACE FUNCTION sql_saga.temporal_merge_plan(
    target_table regclass,
    source_table regclass,
    identity_columns TEXT[],
    mode sql_saga.temporal_merge_mode,
    era_name name,
    row_id_column name DEFAULT 'row_id',
    founding_id_column name DEFAULT NULL,
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

    IF temporal_merge_plan.founding_id_column IS NOT NULL THEN
        PERFORM 1 FROM pg_attribute WHERE attrelid = temporal_merge_plan.source_table AND attname = temporal_merge_plan.founding_id_column AND NOT attisdropped AND attnum > 0;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'founding_id_column "%" does not exist in source table %s', temporal_merge_plan.founding_id_column, temporal_merge_plan.source_table::text;
        END IF;
    END IF;

    -- Validate that identity columns exist in both source and target tables.
    DECLARE
        v_col TEXT;
    BEGIN
        IF temporal_merge_plan.identity_columns IS NOT NULL AND cardinality(temporal_merge_plan.identity_columns) > 0 THEN
            FOREACH v_col IN ARRAY temporal_merge_plan.identity_columns LOOP
                PERFORM 1 FROM pg_attribute WHERE attrelid = temporal_merge_plan.source_table AND attname = v_col AND NOT attisdropped AND attnum > 0;
                IF NOT FOUND THEN RAISE EXCEPTION 'identity_column % does not exist in source table %', quote_ident(v_col), temporal_merge_plan.source_table; END IF;
                PERFORM 1 FROM pg_attribute WHERE attrelid = temporal_merge_plan.target_table AND attname = v_col AND NOT attisdropped AND attnum > 0;
                IF NOT FOUND THEN RAISE EXCEPTION 'identity_column % does not exist in target table %', quote_ident(v_col), temporal_merge_plan.target_table; END IF;
            END LOOP;
        END IF;

        IF temporal_merge_plan.natural_identity_columns IS NOT NULL AND cardinality(temporal_merge_plan.natural_identity_columns) > 0 THEN
            FOREACH v_col IN ARRAY temporal_merge_plan.natural_identity_columns LOOP
                PERFORM 1 FROM pg_attribute WHERE attrelid = temporal_merge_plan.source_table AND attname = v_col AND NOT attisdropped AND attnum > 0;
                IF NOT FOUND THEN RAISE EXCEPTION 'natural_identity_column % does not exist in source table %', quote_ident(v_col), temporal_merge_plan.source_table; END IF;
                PERFORM 1 FROM pg_attribute WHERE attrelid = temporal_merge_plan.target_table AND attname = v_col AND NOT attisdropped AND attnum > 0;
                IF NOT FOUND THEN RAISE EXCEPTION 'natural_identity_column % does not exist in target table %', quote_ident(v_col), temporal_merge_plan.target_table; END IF;
            END LOOP;
        END IF;
    END;

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
        COALESCE(temporal_merge_plan.founding_id_column, ''),
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
        v_natural_identity_cols_are_null_expr TEXT;
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
        v_stable_id_aggregates_expr_prefixed_with_comma TEXT;
        v_entity_partition_key_cols_prefix TEXT;
        v_non_temporal_lookup_cols_select_list_no_alias_prefix TEXT;
        v_stable_id_projection_expr_prefix TEXT;
        v_non_temporal_lookup_cols_select_list_prefix TEXT;
        v_non_temporal_tr_qualified_lookup_cols_prefix TEXT;
        v_lookup_cols_sans_valid_from_prefix TEXT;
        v_lateral_join_entity_id_clause TEXT;
    BEGIN
        -- On cache miss, proceed with the expensive introspection and query building.
        v_identity_cols_trace_expr := (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, seg.%I', col, col), ', '), '')) FROM unnest(COALESCE(temporal_merge_plan.identity_columns, '{}')) as col);
        v_natural_identity_cols_trace_expr := (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, seg.%I', col, col), ', '), '')) FROM unnest(COALESCE(temporal_merge_plan.natural_identity_columns, '{}')) as col);
        v_natural_identity_cols_are_null_expr :=
            CASE
                WHEN temporal_merge_plan.natural_identity_columns IS NULL OR cardinality(temporal_merge_plan.natural_identity_columns) = 0
                THEN 'false'
                ELSE (SELECT format('(%s) IS TRUE', string_agg(format('t.%I IS NULL', col), ' AND ')) FROM unnest(temporal_merge_plan.natural_identity_columns) AS col)
            END;

        v_correlation_col := COALESCE(temporal_merge_plan.founding_id_column, temporal_merge_plan.row_id_column);

        IF v_correlation_col IS NULL THEN
            RAISE EXCEPTION 'The correlation identifier column cannot be NULL. Please provide a non-NULL value for either founding_id_column or row_id_column.';
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

        IF temporal_merge_plan.founding_id_column IS NOT NULL THEN
            -- If an explicit correlation column is provided, it might contain NULLs.
            -- We must coalesce to the row_id_column to ensure every new entity
            -- has a unique correlation identifier. To handle different data types,
            -- we cast the row_id_column to the type of the primary correlation column.
            v_ident_corr_select_expr := format('COALESCE(t.%I, t.%I::%s)',
                temporal_merge_plan.founding_id_column,
                temporal_merge_plan.row_id_column,
                v_ident_corr_column_type::text
            );
        ELSE
            -- If no explicit correlation column is given, the row_id_column is the correlation identifier.
            v_ident_corr_select_expr := format('t.%I', temporal_merge_plan.row_id_column);
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
            string_agg(format('(sr.%1$I = seg.%1$I OR (sr.%1$I IS NULL AND seg.%1$I IS NULL))', col), ' AND ') || ' AND (NOT seg.stable_identity_columns_are_null OR sr.corr_ent = seg.corr_ent)',
            string_agg(format('(tr.%1$I = seg.%1$I OR (tr.%1$I IS NULL AND seg.%1$I IS NULL))', col), ' AND '),
            string_agg(format('si.%I', col), ', ')
        INTO
            v_lookup_cols_select_list,
            v_join_on_lookup_cols,
            v_lateral_join_sr_to_seg,
            v_lateral_join_tr_to_seg,
            v_lookup_cols_si_alias_select_list
        FROM unnest(v_lookup_columns) col;

        SELECT COALESCE(string_agg(format('(s_inner.%1$I = dr.%1$I OR (s_inner.%1$I IS NULL AND dr.%1$I IS NULL))', col), ' AND '), 'true')
        INTO v_lateral_join_entity_id_clause
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
        v_entity_partition_key_cols_prefix := COALESCE(NULLIF(v_entity_partition_key_cols, '') || ', ', '');

        -- When the stable ID columns are not part of the partition key, they must be aggregated.
        -- A simple `max()` correctly coalesces the NULL from a source row and the non-NULL
        -- value from the corresponding target row into the single correct stable ID.
        -- A simple `max()` correctly coalesces the NULL from a source row and the non-NULL
        -- value from the corresponding target row into the single correct stable ID.
        SELECT COALESCE(string_agg(format('max(%I) as %I', col, col), ', '), '')
        INTO v_stable_id_aggregates_expr
        FROM unnest(v_all_id_cols) c(col)
        WHERE col <> ALL(v_lookup_columns);

        v_stable_id_aggregates_expr_prefixed_with_comma := COALESCE(', ' || NULLIF(v_stable_id_aggregates_expr, ''), '');

        SELECT COALESCE(string_agg(format('%I', col), ', '), '')
        INTO v_stable_id_projection_expr
        FROM unnest(v_all_id_cols) c(col)
        WHERE col <> ALL(v_lookup_columns);
        v_stable_id_projection_expr_prefix := COALESCE(NULLIF(v_stable_id_projection_expr, '') || ', ', '');
        
        -- Create versions of the lookup column list for SELECT clauses that exclude
        -- temporal columns, to avoid selecting them twice in various CTEs.
        v_non_temporal_lookup_cols_select_list := (
            SELECT COALESCE(string_agg(format('t.%I', col), ', '), '')
            FROM unnest(v_all_id_cols) col
            WHERE col.col NOT IN (v_valid_from_col, v_valid_until_col)
        );
        v_non_temporal_lookup_cols_select_list_prefix := COALESCE(NULLIF(v_non_temporal_lookup_cols_select_list, '') || ', ', '');
        v_non_temporal_lookup_cols_select_list_no_alias := (
            SELECT COALESCE(string_agg(format('%I', col), ', '), '')
            FROM unnest(v_all_id_cols) col
            WHERE col.col NOT IN (v_valid_from_col, v_valid_until_col)
        );
        v_non_temporal_lookup_cols_select_list_no_alias_prefix := COALESCE(NULLIF(v_non_temporal_lookup_cols_select_list_no_alias, '') || ', ', '');
        v_non_temporal_tr_qualified_lookup_cols := (
            SELECT COALESCE(string_agg(format('tr.%I', col), ', '), '')
            FROM unnest(v_all_id_cols) col
            WHERE col.col NOT IN (v_valid_from_col, v_valid_until_col)
        );
        v_non_temporal_tr_qualified_lookup_cols_prefix := COALESCE(NULLIF(v_non_temporal_tr_qualified_lookup_cols, '') || ', ', '');

        v_lookup_cols_sans_valid_from := (
            SELECT string_agg(format('%I', col), ', ')
            FROM unnest(v_all_id_cols) col
            WHERE col.col <> v_valid_from_col
        );
        v_lookup_cols_sans_valid_from_prefix := COALESCE(NULLIF(v_lookup_cols_sans_valid_from, '') || ', ', '');
        v_lookup_cols_sans_valid_from := COALESCE(v_lookup_cols_sans_valid_from, '');

        -- Unqualified list of ID columns (e.g., "id, ident_corr") for GROUP BY and some SELECTs.
        v_unqualified_id_cols_sans_vf := (
            SELECT string_agg(format('%I', col), ', ')
            FROM unnest(v_all_id_cols) col
            WHERE col.col <> v_valid_from_col
        );

        -- Qualified list of ID columns (e.g., "r.id, r.corr_ent") for the recursive part of the CTE.
        v_qualified_r_id_cols_sans_vf := (
            SELECT string_agg(format('r.%I', col), ', ')
            FROM unnest(v_all_id_cols) col
            WHERE col.col <> v_valid_from_col
        );

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
            -- This is a critical performance optimization. We dynamically generate the most efficient
            -- query to filter the target table based on the nullability of the lookup columns.
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
                    -- OPTIMIZED PATH 1: SARGable IN clause.
                    -- If all lookup columns are defined as NOT NULL in both tables, we can use a simple,
                    -- fast, SARGable query. A "SARGable" (Search ARGument-able) query is one that
                    -- allows the database to efficiently use an index. The `IN (SELECT DISTINCT ...)`
                    -- pattern is highly optimizable by PostgreSQL.
                    v_target_rows_filter := format($$(
                        SELECT * FROM %1$s inner_t
                        WHERE (%2$s) IN (SELECT DISTINCT %3$s FROM source_initial si)
                    )$$, v_target_table_ident, (SELECT string_agg(format('inner_t.%I', col), ', ') FROM unnest(v_lookup_columns) col), v_lookup_cols_si_alias_select_list);
                ELSE
                    -- OPTIMIZED PATH 2: NULL-safe UNION-based filtering.
                    -- If any lookup column is nullable, we separate the logic for non-NULL and NULL
                    -- keys into two queries combined by a UNION. This allows the planner to use
                    -- indexes for the non-NULL part and a specialized strategy for the NULL part.
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

                        -- SARGable part for non-NULL keys using an IN clause. This is identical to the
                        -- fully non-nullable path, but it filters the source rows to exclude any
                        -- keys that contain a NULL.
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

                            -- OPTIMIZED PATH 2a: Handling multiple nullable keys (e.g., XOR keys).
                            -- Heuristic: If there are multiple nullable columns, we assume it's a
                            -- complex XOR-style key that likely relies on partial indexes for performance.
                            -- In this case, we generate a `UNION ALL` of simple `=` joins, a pattern that is
                            -- highly effective at enabling the PostgreSQL planner to use those partial indexes.
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
                                -- OPTIMIZED PATH 2b: Handling a single nullable key.
                                -- We join the target against a pre-filtered, distinct set of source keys.
                                -- This mirrors the performant `IN (SELECT DISTINCT ...)` pattern from the
                                -- SARGable part, but uses a JOIN with a null-safe condition. By pre-filtering
                                -- the source keys, we avoid a very expensive join against the entire source CTE.
                                DECLARE
                                    v_distinct_source_with_nulls TEXT;
                                BEGIN
                                    v_distinct_source_with_nulls := format(
                                        '(SELECT DISTINCT %s FROM source_initial si WHERE %s)',
                                        v_lookup_cols_si_alias_select_list,
                                        v_lookup_cols_is_null_condition
                                    );

                                    v_join_clause := COALESCE(
                                        (SELECT string_agg(format('(si.%1$I = inner_t.%1$I OR (si.%1$I IS NULL AND inner_t.%1$I IS NULL))', c), ' AND ') FROM unnest(v_lookup_columns) c),
                                        'true'
                                    );

                                    v_null_part := format($$(
                                        SELECT DISTINCT ON (%3$s) inner_t.*
                                        FROM %1$s inner_t
                                        JOIN %4$s AS si ON (%2$s)
                                    )$$, v_target_table_ident, v_join_clause, v_distinct_on_cols_list, v_distinct_source_with_nulls);
                                END;
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
            v_resolver_ctes := v_resolver_ctes || format($$
    resolved_atomic_segments_with_flag AS (
        SELECT *,
            bool_or(s_data_payload IS NOT NULL) OVER (PARTITION BY %s) as entity_is_in_source
        FROM %s
    ),
$$, v_entity_partition_key_cols, v_resolver_from);
            v_resolver_from := 'resolved_atomic_segments_with_flag';

            -- If deleting missing entities, the final payload for those entities must be NULL.
            -- For entities present in the source, the existing v_final_data_payload_expr is correct.
            v_final_data_payload_expr := format($$CASE WHEN entity_is_in_source THEN (%s) ELSE NULL END$$, v_final_data_payload_expr);
        END IF;

        -- Pre-calculate all expressions to be passed to format() for easier debugging.
        v_target_rows_lookup_cols_expr := (SELECT string_agg(format('t.%I', col), ', ') FROM unnest(v_lookup_columns) as col);
        v_source_rows_exists_join_expr := (SELECT string_agg(format('(si.%1$I = tr.%1$I OR (si.%1$I IS NULL AND tr.%1$I IS NULL))', col), ' AND ') FROM unnest(v_lookup_columns) as col);
        v_atomic_segments_select_list_expr := (SELECT COALESCE(string_agg(format('seg.%I', col), ', ') || ',', '') || ' seg.corr_ent' FROM unnest(v_lookup_columns) as col);
        v_diff_select_expr := (SELECT string_agg(format('COALESCE(f.%1$I, t.%1$I) as %1$I', col), ', ') || ', COALESCE(f.corr_ent, t.corr_ent) as corr_ent' FROM unnest(v_all_id_cols) as col);
        v_plan_with_op_entity_id_json_build_expr := (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, d.%I', col, col), ', '), '')) || ' || COALESCE(d.stable_pk_payload, ''{}''::jsonb)' FROM unnest(v_all_id_cols) as col);
        v_skip_no_target_entity_id_json_build_expr := (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, sr.%I', col, col), ', '), '')) FROM unnest(v_all_id_cols) as col);
        v_final_order_by_expr := (SELECT string_agg(format('p.%I', col), ', ') FROM unnest(v_all_id_cols) as col);
        v_partition_key_for_with_base_payload_expr := (
            SELECT COALESCE(string_agg(format('seg.%I', col), ', ') || ',', '') || ' seg.corr_ent'
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
        -- The corr_ent must also match. This is critical for new entities that share a NULL lookup key.
        v_rcte_join_condition := format('(%s) AND (c.corr_ent = r.corr_ent OR (c.corr_ent IS NULL and r.corr_ent IS NULL))', v_rcte_join_condition);

        -- The partition key for new entities is handled by the `corr_ent` column itself
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
            v_trace_seed_expr := format($$jsonb_build_object('cte', 'raswp', 'partition_key', jsonb_build_object(%1$s), 's_row_id', s.source_row_id, 's_data', s.data_payload, 't_data', t.data_payload, 's_ephemeral', s.ephemeral_payload, 't_ephemeral', t.ephemeral_payload, 's_t_relation', sql_saga.get_allen_relation(s.valid_from, s.valid_until, t.t_valid_from, t.t_valid_until), 'seg_new_ent', seg.new_ent, 'seg_stable_identity_columns_are_null', seg.stable_identity_columns_are_null, 'seg_natural_identity_column_values_are_null', seg.natural_identity_column_values_are_null, 'identity_columns', %2$s, 'natural_identity_columns', %3$s, 'canonical_corr_ent', seg.corr_ent, 'direct_source_corr_ent', s.corr_ent) as trace$$,
                (SELECT string_agg(format('%L, seg.%I', col, col), ',') FROM unnest(v_lookup_columns) col),
                v_identity_cols_trace_expr,
                v_natural_identity_cols_trace_expr
            );
        ELSE
            v_trace_seed_expr := 'NULL::jsonb as trace';
        END IF;

        -- This trace step is inside the coalescing logic. It duplicates the CASE
        -- statement for `is_new_segment` to show exactly what is being compared.
        v_trace_select_list := format($$
            trace || jsonb_build_object(
                'cte', 'coalesce_check',
                'corr_ent', corr_ent,
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
            RAISE NOTICE '(%) v_entity_partition_key_cols (%%37$s): %', v_log_id, v_entity_partition_key_cols;
            RAISE NOTICE '(%) v_s_founding_join_condition (%%38$s): %', v_log_id, v_s_founding_join_condition;
            RAISE NOTICE '(%) v_tr_qualified_lookup_cols (%%39$s): %', v_log_id, v_tr_qualified_lookup_cols;
            RAISE NOTICE '(%) v_ident_corr_column_type (%%40$s): %', v_log_id, v_ident_corr_column_type;
            RAISE NOTICE '(%) v_non_temporal_lookup_cols_select_list (%%41$s): %', v_log_id, v_non_temporal_lookup_cols_select_list;
            RAISE NOTICE '(%) v_non_temporal_lookup_cols_select_list_no_alias (%%42$s): %', v_log_id, v_non_temporal_lookup_cols_select_list_no_alias;
            RAISE NOTICE '(%) v_non_temporal_tr_qualified_lookup_cols (%%43$s): %', v_log_id, v_non_temporal_tr_qualified_lookup_cols;
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
-- Example: A source table row `(row_id: 101, id: 1, name: 'A', ...)`
-- becomes a tuple with a structured `data_payload`, a correlation ID, and a boolean flag.
-- `(source_row_id: 101, corr_ent: 101, id: 1, ..., data_payload: {"name": "A"}, identity_column_values_are_null: false)`
--
-- Formulation: A simple SELECT that dynamically builds the `data_payload` JSONB object from all relevant
-- source columns. The correlation ID (`corr_ent`) provides a stable identifier for each source row.
--
-- Semantics of `stable_identity_columns_are_null`:
-- This is a boolean flag indicating if all columns of the stable entity identifier
-- (from the `identity_columns` parameter, NOT `natural_identity_columns`) are NULL for
-- a given source row. This is a crucial piece of information for the planner to distinguish
-- between operations on existing entities versus the "founding" of new ones.
--
-- - `false`: The source row has a non-NULL stable key. The planner will treat it as a
--   potential update or patch to an existing entity.
-- - `true`: The source row is "founding" a new entity. The planner knows that it must
--   rely on other identifiers (like `natural_identity_columns` or `founding_id_column`)
--   to group this row with other related source rows that constitute the same new entity.
--   It also signals to the executor that a new stable key (e.g., from a sequence)
--   may need to be generated.
source_initial AS (
    SELECT
        t.%18$I as source_row_id,
        %16$s as corr_ent,
        %64$s /* v_non_temporal_lookup_cols_select_list_prefix */
        t.%14$I as valid_from,
        t.%15$I as valid_until,
        %2$s AS data_payload,
        %56$s AS ephemeral_payload,
        %13$s as stable_identity_columns_are_null,
        %72$s as natural_identity_column_values_are_null
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
        NULL::%40$s as corr_ent, -- Target rows do not originate from a source row, so corr_ent is NULL. Type is introspected.
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
-- CTE 4: source_rows_with_new_flag
-- Purpose: Correctly determines if a source row represents a truly "new" entity.
-- Formulation: An entity is "new" if, and only if, it does not already exist in the target table.
-- The `target_entity_exists` flag is the single source of truth for this, determined by looking
-- up the entity's natural key. This is a critical declarative step that correctly identifies
-- the "founding" event for an entity's timeline.
source_rows_with_new_flag AS (
    SELECT
        *,
        NOT target_entity_exists as new_ent
    FROM source_rows
),
-- CTE 5: source_rows_with_unified_flags
-- Purpose: Establishes the canonical flag that controls partitioning logic.
-- An entity's timeline should only be partitioned by its correlation ID if it's a
-- truly new entity AND its stable identifier is NULL in the source (meaning it
-- needs to be generated or looked up later).
source_rows_with_unified_flags AS (
    SELECT
        *,
        stable_identity_columns_are_null AND new_ent as partition_by_corr_ent
    FROM source_rows_with_new_flag
),
-- CTE 6: active_source_rows
-- Purpose: Filters the source rows based on the requested `mode`.
-- Formulation: A simple `CASE` statement in the `WHERE` clause declaratively enforces the semantics of each mode.
-- For example, `INSERT_NEW_ENTITIES` will only consider rows where `target_entity_exists` is false. This is another
-- critical performance optimization.
active_source_rows AS (
    SELECT
        sr.*
    FROM source_rows_with_unified_flags sr
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
-- CTE 7: all_rows
-- Purpose: Creates the unified set of all relevant time periods (both source and target) for the entities being processed.
-- This is the foundational data set upon which the atomic timeline will be built.
--
-- Example: For an entity, this might produce:
-- `(id: 1, corr_ent: 101, valid_from: '2023-01-01', valid_until: '2023-06-01') -- from source`
-- `(id: 1, corr_ent: NULL, valid_from: '2022-01-01', valid_until: '2024-01-01') -- from target`
--
-- Formulation: A `UNION ALL` combines the active source rows with the relevant target rows. All rows for a
-- conceptual entity are processed together in downstream CTEs.
all_rows AS (
    SELECT %62$s corr_ent, valid_from, valid_until, new_ent, stable_identity_columns_are_null, natural_identity_column_values_are_null, partition_by_corr_ent FROM active_source_rows
    UNION ALL
    SELECT
        %65$s
        tr.corr_ent,
        tr.valid_from,
        tr.valid_until,
        false as new_ent,
        false as stable_identity_columns_are_null,
        false as natural_identity_column_values_are_null,
        false as partition_by_corr_ent
    FROM target_rows tr
),
-- CTE 8: time_points_raw
-- Purpose: Deconstructs all time periods from `all_rows` into a non-unique, ordered set of chronological points.
time_points_raw AS (
    SELECT %26$s, corr_ent, valid_from AS point, new_ent, stable_identity_columns_are_null, natural_identity_column_values_are_null, partition_by_corr_ent FROM all_rows
    UNION ALL
    SELECT %26$s, corr_ent, valid_until AS point, new_ent, stable_identity_columns_are_null, natural_identity_column_values_are_null, partition_by_corr_ent FROM all_rows
),
-- CTE 9: time_points_unified
-- Purpose: Establishes a single, authoritative correlation ID (`corr_ent`) for all time points that belong
-- to the same conceptual entity. This is critical for correctly partitioning the timeline in subsequent CTEs.
time_points_unified AS (
    SELECT
        *,
        FIRST_VALUE(corr_ent) OVER (PARTITION BY %61$s CASE WHEN partition_by_corr_ent THEN corr_ent ELSE NULL END ORDER BY corr_ent ASC NULLS LAST) as unified_corr_ent
    FROM time_points_raw
),
-- CTE 10: time_points
-- Purpose: De-duplicates the unified time points to get a distinct, ordered set for each entity.
time_points AS (
    SELECT DISTINCT ON (%61$s CASE WHEN partition_by_corr_ent THEN unified_corr_ent ELSE NULL END, point)
        %26$s,
        unified_corr_ent as corr_ent,
        point,
        new_ent,
        stable_identity_columns_are_null,
        natural_identity_column_values_are_null,
        partition_by_corr_ent
    FROM time_points_unified
    -- The ORDER BY here must match the DISTINCT ON to be valid and deterministic.
    -- We add `corr_ent` to the end to act as a deterministic tie-breaker.
    -- Source rows have a non-NULL `corr_ent`, target rows have NULL.
    -- `ASC NULLS LAST` ensures we prioritize flags from the source row when a time point is shared.
    ORDER BY %61$s CASE WHEN partition_by_corr_ent THEN unified_corr_ent ELSE NULL END, point, corr_ent ASC NULLS LAST
),
-- CTE 11: atomic_segments
-- Purpose: Reconstructs the timeline from the points into a set of atomic, non-overlapping, contiguous segments.
--
-- Example: The four points from the `time_points` example would be reconstructed into three atomic segments.
-- `(id: 1, corr_ent: ..., from: '2022-01-01', until: '2023-01-01')`
-- `(id: 1, corr_ent: ..., from: '2023-01-01', until: '2023-06-01')`
-- `(id: 1, corr_ent: ..., from: '2023-06-01', until: '2024-01-01')`
--
-- Formulation: The `LEAD()` window function is a stateless, declarative way to create the `[valid_from, valid_until)`
-- segments from the ordered list of time points. The partitioning is critical here to ensure timelines for
-- different entities are handled independently.
atomic_segments AS (
    SELECT %26$s, corr_ent, point as valid_from, next_point as valid_until, new_ent, stable_identity_columns_are_null, natural_identity_column_values_are_null, partition_by_corr_ent
    FROM (
        SELECT
            *,
            LEAD(point) OVER (PARTITION BY %61$s CASE WHEN partition_by_corr_ent THEN corr_ent ELSE NULL END ORDER BY point) as next_point
        FROM time_points
    ) with_lead
    WHERE point IS NOT NULL AND next_point IS NOT NULL AND point < next_point
),
-- CTE 12: resolved_atomic_segments_with_payloads
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
        FIRST_VALUE(with_base_payload.stable_pk_payload) OVER (PARTITION BY %61$s CASE WHEN with_base_payload.partition_by_corr_ent THEN with_base_payload.corr_ent ELSE NULL END ORDER BY with_base_payload.stable_pk_payload IS NULL, with_base_payload.valid_from) AS propagated_stable_pk_payload
    FROM (
        SELECT
            %36$s, -- v_partition_key_for_with_base_payload_expr
            seg.new_ent,
            seg.stable_identity_columns_are_null,
            seg.partition_by_corr_ent,
            seg.natural_identity_column_values_are_null,
            seg.valid_from,
            seg.valid_until,
            t.t_valid_from,
            t.t_valid_until,
            s.source_row_id,
            s.data_payload as s_data_payload,
            s.ephemeral_payload as s_ephemeral_payload,
            t.data_payload as t_data_payload,
            t.ephemeral_payload as t_ephemeral_payload,
            t.stable_pk_payload,
            -- Note: s.corr_ent and causal.corr_ent are selected here for tracing/debugging purposes only.
            -- The canonical entity correlation ID is seg.corr_ent, which is passed through via the
            -- partition key (`v_partition_key_for_with_base_payload_expr`).
            s.corr_ent as s_corr_ent,
            s.corr_ent as causal_corr_ent, -- This is now just for placeholder stability, the value is from the direct source.
            s.valid_from AS s_valid_from,
            s.valid_until AS s_valid_until,
            %47$s
        FROM atomic_segments seg
        -- Join to find the original target data for this time slice.
        -- Example: For an atomic segment (id:1, from:'2023-01-01', until:'2023-06-01'), this will find the
        -- original target row `(id:1, from:'2022-01-01', until:'2024-01-01', data:{...})` because the segment
        -- is contained by (`<@`) the target row's period.
        LEFT JOIN LATERAL (
            SELECT tr.data_payload, tr.ephemeral_payload, tr.valid_from as t_valid_from, tr.valid_until as t_valid_until, tr.stable_pk_payload
            FROM target_rows tr
            WHERE %29$s
              AND %19$I(seg.valid_from, seg.valid_until) <@ %19$I(tr.valid_from, tr.valid_until)
        ) t ON true
        -- Join to find the source data for this time slice.
        -- Example: For the same atomic segment, this will find the source row
        -- `(id:1, from:'2023-01-01', until:'2023-06-01', data:{...})` that directly covers it.
        -- If multiple source rows overlap (e.g., corrections), `ORDER BY ... LIMIT 1` ensures we deterministically pick the latest one.
        LEFT JOIN LATERAL (
            SELECT sr.source_row_id, sr.data_payload, sr.ephemeral_payload, sr.valid_from, sr.valid_until, sr.corr_ent
            FROM active_source_rows sr
            WHERE %28$s
              AND %19$I(seg.valid_from, seg.valid_until) <@ %19$I(sr.valid_from, sr.valid_until)
            -- In case of overlapping source rows, the one with the highest row_id (latest) wins.
            ORDER BY sr.source_row_id DESC
            LIMIT 1
        ) s ON true
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
),
%9$sresolved_atomic_segments_with_propagated_ids AS (
    SELECT
        *,
        -- This COALESCE implements the causal priority: look-behind is preferred over look-ahead.
        COALESCE(
            source_row_id,
            (max(source_row_id) OVER (PARTITION BY %61$s t_valid_from, CASE WHEN partition_by_corr_ent THEN corr_ent ELSE NULL END, look_behind_grp)),
            (max(source_row_id) OVER (PARTITION BY %61$s t_valid_from, CASE WHEN partition_by_corr_ent THEN corr_ent ELSE NULL END, look_ahead_grp))
        ) as propagated_source_row_id,
        COALESCE(
            s_valid_from,
            (max(s_valid_from) OVER (PARTITION BY %61$s t_valid_from, CASE WHEN partition_by_corr_ent THEN corr_ent ELSE NULL END, look_behind_grp)),
            (max(s_valid_from) OVER (PARTITION BY %61$s t_valid_from, CASE WHEN partition_by_corr_ent THEN corr_ent ELSE NULL END, look_ahead_grp))
        ) as propagated_s_valid_from,
        COALESCE(
            s_valid_until,
            (max(s_valid_until) OVER (PARTITION BY %61$s t_valid_from, CASE WHEN partition_by_corr_ent THEN corr_ent ELSE NULL END, look_behind_grp)),
            (max(s_valid_until) OVER (PARTITION BY %61$s t_valid_from, CASE WHEN partition_by_corr_ent THEN corr_ent ELSE NULL END, look_ahead_grp))
        ) as propagated_s_valid_until
    FROM (
        SELECT
            *,
            sum(CASE WHEN source_row_id IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY %61$s t_valid_from, CASE WHEN partition_by_corr_ent THEN corr_ent ELSE NULL END ORDER BY valid_from) AS look_behind_grp,
            sum(CASE WHEN source_row_id IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY %61$s t_valid_from, CASE WHEN partition_by_corr_ent THEN corr_ent ELSE NULL END ORDER BY valid_from DESC) AS look_ahead_grp
        FROM %10$s
    ) with_grp
),
-- CTE 11: resolved_atomic_segments
-- Purpose: Applies the high-level semantic logic for each mode (PATCH, REPLACE, etc.) to calculate
-- the final data payload for each atomic segment.
-- Formulation: The `v_final_data_payload_expr` is a dynamically generated SQL expression. For example,
-- for PATCH/UPSERT modes, it is `COALESCE(t_data_payload, '{}') || COALESCE(s_data_payload, '{}')`.
-- This is a stateless, declarative way to compute the final state for each segment independently.
resolved_atomic_segments AS (
    SELECT
        %66$sstable_identity_columns_are_null,
        natural_identity_column_values_are_null,
        new_ent,
        partition_by_corr_ent,
        valid_from,
        valid_until,
        t_valid_from,
        t_valid_until,
        propagated_s_valid_from,
        propagated_s_valid_until,
        propagated_source_row_id as source_row_id,
        corr_ent,
        propagated_stable_pk_payload AS stable_pk_payload,
        s_data_payload IS NULL AS unaffected_target_only_segment,
        s_data_payload,
        t_data_payload,
        sql_saga.get_allen_relation(propagated_s_valid_from, propagated_s_valid_until, t_valid_from, t_valid_until) AS s_t_relation,
        %8$s as data_payload,
        md5((%8$s - %5$L::text[])::text) as data_hash,
        COALESCE(t_ephemeral_payload, '{}'::jsonb) || COALESCE(s_ephemeral_payload, '{}'::jsonb) as ephemeral_payload,
        CASE WHEN %54$L::boolean
             THEN trace || jsonb_build_object(
                 'cte', 'ras',
                 'final_data_payload', %8$s,
                 'final_ephemeral_payload', COALESCE(t_ephemeral_payload, '{}'::jsonb) || COALESCE(s_ephemeral_payload, '{}'::jsonb)
             )
             ELSE NULL
        END as trace,
        CASE WHEN s_data_payload IS NOT NULL THEN 1 ELSE 2 END as priority
    FROM resolved_atomic_segments_with_propagated_ids
),
-- CTE 12: coalesced_final_segments
-- Purpose: Merges adjacent atomic segments that have identical data payloads (ignoring ephemeral columns).
-- This is a critical optimization to generate the minimal number of DML operations.
--
-- Example: If two adjacent segments have the same final data, e.g.:
-- `(... valid_from: '2022-01-01', valid_until: '2023-01-01', data: {"name":"A"})`
-- `(... valid_from: '2023-01-01', valid_until: '2024-01-01', data: {"name":"A"})`
-- They will be merged into a single segment:
-- `(... valid_from: '2022-01-01', valid_until: '2024-01-01', data: {"name":"A"})`
--
-- `(... from: '2023-01-01', until: '2024-01-01', data: {"name":"A"})`
-- They will be merged into a single segment: `(... from: '2022-01-01', until: '2024-01-01', data: {"name":"A"})`
--
-- Formulation: This is a classic "gaps-and-islands" problem, solved declaratively and efficiently
-- using window functions. The `island_group` CTE assigns a unique ID to each "island" of contiguous,
-- identical data. The final query then aggregates the segments within each group. The `PARTITION BY` and
-- `GROUP BY` are critical: they group by the stable key, but for new entities with a NULL stable key,
-- they add the `corr_ent` to keep them distinct.
coalesced_final_segments AS (
    WITH island_group AS (
        SELECT
            *,
            SUM(is_island_start) OVER (PARTITION BY %61$s CASE WHEN partition_by_corr_ent THEN corr_ent ELSE NULL END ORDER BY valid_from) as island_group_id
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
                    LAG(data_hash) OVER w as prev_data_hash
                FROM resolved_atomic_segments ras
                WHERE ras.data_payload IS NOT NULL
                WINDOW w AS (PARTITION BY %61$s CASE WHEN partition_by_corr_ent THEN corr_ent ELSE NULL END ORDER BY valid_from)
            ) s1
        ) s2
    )
    SELECT
        %37$s%60$s,
        sql_saga.first(corr_ent ORDER BY valid_from) as corr_ent,
        sql_saga.first(stable_identity_columns_are_null ORDER BY valid_from) as stable_identity_columns_are_null,
        sql_saga.first(natural_identity_column_values_are_null ORDER BY valid_from) as natural_identity_column_values_are_null,
        sql_saga.first(new_ent ORDER BY valid_from) as new_ent,
        sql_saga.first(partition_by_corr_ent ORDER BY valid_from) as partition_by_corr_ent,
        sql_saga.first(s_t_relation ORDER BY valid_from) as s_t_relation,
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
                 'island_group_id', island_group_id,
                 'final_payload', sql_saga.first(data_payload ORDER BY valid_from DESC),
                 'final_payload_sans_ephemeral', sql_saga.first(data_payload - %5$L::text[] ORDER BY valid_from DESC),
                 'atomic_traces', jsonb_agg((trace || jsonb_build_object('data_hash', data_hash, 'prev_data_hash', prev_data_hash)) ORDER BY valid_from)
             )
             ELSE NULL
        END as trace
    FROM island_group
    GROUP BY
        %61$s -- v_entity_partition_key_cols_prefix handles the optional comma
        island_group_id,
        -- The group key must match the partition key used to create the islands.
        CASE WHEN partition_by_corr_ent THEN corr_ent ELSE NULL END
),
-- CTE 14: diff
-- Purpose: Compares the final, coalesced state of the timeline with the original state from `target_rows`.
-- Formulation: A `FULL OUTER JOIN` is the standard, declarative way to compare two sets of data.
-- The result of this join provides all the necessary information to determine if a segment represents
-- an INSERT, UPDATE, DELETE, or an unchanged state.
diff AS (
    SELECT
        %30$s, -- v_diff_select_expr
        COALESCE(f.new_ent, false) as new_ent,
        COALESCE(f.stable_identity_columns_are_null, false) as stable_identity_columns_are_null,
        COALESCE(f.natural_identity_column_values_are_null, false) as natural_identity_column_values_are_null,
        COALESCE(f.partition_by_corr_ent, false) as partition_by_corr_ent,
        f.f_from, f.f_until, f.f_data, f.f_row_ids, f.stable_pk_payload, f.s_t_relation,
        CASE WHEN %54$L::boolean
             THEN f.trace || jsonb_build_object('cte', 'diff', 'f_corr_ent', f.corr_ent, 'final_payload_vs_target_payload', jsonb_build_object('f', f.f_data, 't', t.t_data))
             ELSE NULL
        END as trace,
        f.unaffected_target_only_segment,
        t.t_from, t.t_until, t.t_data,
        -- It calculates the original-to-final (before-and-after) relation.
        sql_saga.get_allen_relation(t.t_from, t.t_until, f.f_from, f.f_until) as b_a_relation
    FROM
    (
        SELECT
            %26$s,
            new_ent,
            stable_identity_columns_are_null,
            natural_identity_column_values_are_null,
            partition_by_corr_ent,
            s_t_relation,
            ancestor_valid_from,
            corr_ent,
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
            corr_ent,
            valid_from as t_from,
            valid_until as t_until,
            data_payload || ephemeral_payload as t_data
        FROM target_rows
    ) t
    ON %11$s -- Join on raw lookup columns and diff condition
),
-- CTE 14: diff_ranked
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
                        d.f_from,
                        d.f_until
                )
        END as update_rank
    FROM diff d
),
-- CTE 17: plan_with_op
-- Purpose: Assigns the final DML operation (`INSERT`, `UPDATE`, `DELETE`, `SKIP_IDENTICAL`) to each segment
-- based on the results of the `diff` and `diff_ranked` CTEs.
-- Formulation: A `CASE` statement declaratively translates the state of each segment into its corresponding DML action.
-- It also includes a `UNION ALL` to re-introduce source rows that were filtered out at the beginning, so they can
-- be reported with a `SKIPPED_NO_TARGET` status.
plan_with_op AS (
    (
        SELECT * FROM (
            SELECT
                d.f_row_ids as row_ids,
                d.s_t_relation,
                d.new_ent,
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
                d.corr_ent,
                d.t_from as old_valid_from,
                d.t_until as old_valid_until,
                d.f_from as new_valid_from,
                d.f_until as new_valid_until,
                d.f_data as data,
                d.b_a_relation,
                CASE WHEN %54$L::boolean THEN d.trace || jsonb_build_object('cte', 'plan_with_op', 'd_new_ent', d.new_ent, 'd_corr_ent', d.corr_ent) ELSE NULL END as trace
            FROM diff_ranked d
            WHERE d.f_row_ids IS NOT NULL OR d.t_data IS NOT NULL -- Exclude pure deletions of non-existent target data
        ) with_op
        WHERE with_op.operation IS NOT NULL
    )
    UNION ALL
    (
        -- This part of the plan handles source rows that were filtered out by the main logic,
        -- allowing the executor to provide accurate feedback.
        SELECT
            ARRAY[sr.source_row_id::BIGINT],
            NULL::sql_saga.allen_interval_relation,
            sr.new_ent,
            CASE
                WHEN %7$L::sql_saga.temporal_merge_mode = 'INSERT_NEW_ENTITIES' AND sr.target_entity_exists THEN 'SKIP_FILTERED'::sql_saga.temporal_merge_plan_action
                WHEN %7$L::sql_saga.temporal_merge_mode IN ('PATCH_FOR_PORTION_OF', 'REPLACE_FOR_PORTION_OF', 'DELETE_FOR_PORTION_OF', 'UPDATE_FOR_PORTION_OF') AND NOT sr.target_entity_exists THEN 'SKIP_NO_TARGET'::sql_saga.temporal_merge_plan_action
                ELSE 'ERROR'::sql_saga.temporal_merge_plan_action -- Should be unreachable, but acts as a fail-fast safeguard.
            END,
            %26$s,
            %32$s, -- v_skip_no_target_entity_id_json_build_expr
            sr.corr_ent,
            NULL, NULL, NULL, NULL, NULL, NULL,
            NULL::jsonb -- trace
        FROM source_rows_with_new_flag sr
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
-- CTE 18: plan
-- Purpose: Performs final calculations for the plan, such as determining the `update_effect` and constructing
-- the final `entity_ids` JSONB object for feedback and ID back-filling.
plan AS (
    SELECT
        p.*,
        p.entity_id_json as entity_ids,
        CASE
            WHEN p.operation <> 'UPDATE' THEN NULL::sql_saga.temporal_merge_update_effect
            WHEN p.new_valid_from = p.old_valid_from AND p.new_valid_until = p.old_valid_until THEN 'NONE'::sql_saga.temporal_merge_update_effect
            WHEN p.new_valid_from <= p.old_valid_from AND p.new_valid_until >= p.old_valid_until THEN 'GROW'::sql_saga.temporal_merge_update_effect
            WHEN p.new_valid_from >= p.old_valid_from AND p.new_valid_until <= p.old_valid_until THEN 'SHRINK'::sql_saga.temporal_merge_update_effect
            ELSE 'MOVE'::sql_saga.temporal_merge_update_effect
        END AS update_effect
    FROM plan_with_op p
    -- We must join back to source_rows to get the new_ent flag for the final entity_ids construction.
    -- This is safe because row_ids will contain at most one ID for new-entity INSERTs.
    LEFT JOIN source_rows_with_new_flag sr ON sr.source_row_id = p.row_ids[1]
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
    p.corr_ent::TEXT,
    p.new_ent,
    p.entity_ids,
    p.s_t_relation,
    p.b_a_relation,
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
            NULL,                           /* %1$s - v_entity_id_as_jsonb (OBSOLETE) */
            v_source_data_payload_expr,     /* %2$s - v_source_data_payload_expr */
            v_source_table_ident,           /* %3$s - v_source_table_ident */
            v_target_table_ident,           /* %4$s - v_target_table_ident */
            v_ephemeral_columns,            /* %5$L - v_ephemeral_columns */
            v_target_data_cols_jsonb_build, /* %6$s - v_target_data_cols_jsonb_build */
            temporal_merge_plan.mode,       /* %7$L - temporal_merge_plan.mode */
            v_final_data_payload_expr,      /* %8$s - v_final_data_payload_expr */
            v_resolver_ctes,                /* %9$s - v_resolver_ctes */
            v_resolver_from,                /* %10$s - v_resolver_from */
            v_diff_join_condition,          /* %11$s - v_diff_join_condition */
            '',                             /* %12$s - (OBSOLETE) */
            v_entity_id_check_is_null_expr, /* %13$s - v_entity_id_check_is_null_expr */
            v_valid_from_col,               /* %14$I - v_valid_from_col */
            v_valid_until_col,              /* %15$I - v_valid_until_col */
            v_ident_corr_select_expr,       /* %16$s - v_ident_corr_select_expr */
            NULL,                           /* %17$s - v_planner_entity_id_expr (OBSOLETE) */
            temporal_merge_plan.row_id_column, /* %18$I - temporal_merge_plan.row_id_column */
            v_range_constructor,            /* %19$I - v_range_constructor */
            v_target_rows_filter,           /* %20$s - v_target_rows_filter */
            v_stable_pk_cols_jsonb_build,   /* %21$s - v_stable_pk_cols_jsonb_build */
            v_all_id_cols,                  /* %22$L - v_all_id_cols */
            v_lookup_cols_select_list,      /* %23$s - v_lookup_cols_select_list */
            v_target_rows_lookup_cols_expr, /* %24$s - v_target_rows_lookup_cols_expr */
            v_source_rows_exists_join_expr, /* %25$s - v_source_rows_exists_join_expr */
            v_lookup_cols_select_list_no_alias, /* %26$s - v_lookup_cols_select_list_no_alias */
            v_atomic_segments_select_list_expr, /* %27$s - v_atomic_segments_select_list_expr */
            v_lateral_join_sr_to_seg,       /* %28$s - v_lateral_join_sr_to_seg */
            v_lateral_join_tr_to_seg,       /* %29$s - v_lateral_join_tr_to_seg */
            v_diff_select_expr,             /* %30$s - v_diff_select_expr */
            v_plan_with_op_entity_id_json_build_expr, /* %31$s - v_plan_with_op_entity_id_json_build_expr */
            v_skip_no_target_entity_id_json_build_expr, /* %32$s - v_skip_no_target_entity_id_json_build_expr */
            v_correlation_col,              /* %33$L - v_correlation_col */
            v_final_order_by_expr,          /* %34$s - v_final_order_by_expr */
            NULL,                           /* %35$s - v_partition_key_cols (OBSOLETE) */
            v_partition_key_for_with_base_payload_expr, /* %36$s - v_partition_key_for_with_base_payload_expr */
            v_entity_partition_key_cols,    /* %37$s - v_entity_partition_key_cols */
            v_s_founding_join_condition,    /* %38$s - v_s_founding_join_condition */
            v_tr_qualified_lookup_cols,     /* %39$s - v_tr_qualified_lookup_cols */
            v_ident_corr_column_type,       /* %40$s - v_ident_corr_column_type */
            v_non_temporal_lookup_cols_select_list, /* %41$s - v_non_temporal_lookup_cols_select_list */
            v_non_temporal_lookup_cols_select_list_no_alias, /* %42$s - v_non_temporal_lookup_cols_select_list_no_alias */
            v_non_temporal_tr_qualified_lookup_cols, /* %43$s - v_non_temporal_tr_qualified_lookup_cols */
            v_lookup_cols_sans_valid_from,  /* %44$s - v_lookup_cols_sans_valid_from */
            v_unqualified_id_cols_sans_vf,  /* %45$s - v_unqualified_id_cols_sans_vf */
            v_trace_select_list,            /* %46$s - v_trace_select_list */
            v_trace_seed_expr,              /* %47$s - v_trace_seed_expr */
            v_stable_id_aggregates_expr,    /* %48$s - v_stable_id_aggregates_expr */
            v_stable_id_projection_expr,    /* %49$s - v_stable_id_projection_expr */
            v_target_data_cols_jsonb_build_bare, /* %50$s - v_target_data_cols_jsonb_build_bare */
            v_stable_pk_cols_jsonb_build_bare, /* %51$s - v_stable_pk_cols_jsonb_build_bare */
            v_rcte_join_condition,          /* %52$s - v_rcte_join_condition */
            v_qualified_r_id_cols_sans_vf,  /* %53$s - v_qualified_r_id_cols_sans_vf */
            p_log_trace,                    /* %54$L - p_log_trace */
            v_entity_id_check_is_null_expr_no_alias, /* %55$s - v_entity_id_check_is_null_expr_no_alias */
            v_source_ephemeral_cols_jsonb_build, /* %56$s - v_source_ephemeral_cols_jsonb_build */
            v_target_ephemeral_cols_jsonb_build_bare, /* %57$s - v_target_ephemeral_cols_jsonb_build_bare */
            v_target_ephemeral_cols_jsonb_build, /* %58$s - v_target_ephemeral_cols_jsonb_build */
            v_coalesced_payload_expr,       /* %59$s - v_coalesced_payload_expr */
            v_stable_id_aggregates_expr_prefixed_with_comma, /* %60$s - v_stable_id_aggregates_expr_prefixed_with_comma */
            v_entity_partition_key_cols_prefix, /* %61$s - v_entity_partition_key_cols_prefix */
            v_non_temporal_lookup_cols_select_list_no_alias_prefix, /* %62$s - v_non_temporal_lookup_cols_select_list_no_alias_prefix */
            v_stable_id_projection_expr_prefix, /* %63$s - v_stable_id_projection_expr_prefix */
            v_non_temporal_lookup_cols_select_list_prefix, /* %64$s - v_non_temporal_lookup_cols_select_list_prefix */
            v_non_temporal_tr_qualified_lookup_cols_prefix, /* %65$s - v_non_temporal_tr_qualified_lookup_cols_prefix */
            v_lookup_cols_sans_valid_from_prefix, /* %66$s - v_lookup_cols_sans_valid_from_prefix */
            NULL,                                 /* %67$s - (not used) */
            v_lateral_join_entity_id_clause, /* %68$s - v_lateral_join_entity_id_clause */
            NULL,                                 /* %69$s - (placeholder) */
            v_identity_cols_trace_expr,     /* %70$s - v_identity_cols_trace_expr */
            v_natural_identity_cols_trace_expr, /* %71$s - v_natural_identity_cols_trace_expr */
            v_natural_identity_cols_are_null_expr, /* %72$s - v_natural_identity_cols_are_null_expr */
            NULL,                                 /* %73$s - (placeholder) */
            NULL,                                 /* %74$s - (placeholder) */
            NULL,                                 /* %75$s - (placeholder) */
            NULL,                                 /* %76$s - (placeholder) */
            NULL,                                 /* %77$s - (placeholder) */
            NULL,                                 /* %78$s - (placeholder) */
            NULL,                                 /* %79$s - (placeholder) */
            NULL                                  /* %80$s - (placeholder) */
        );

        -- Conditionally log the generated SQL for debugging.
        BEGIN
            IF p_log_sql THEN
                RAISE NOTICE '--- temporal_merge SQL for % ---', temporal_merge_plan.target_table;
                RAISE NOTICE '%', v_sql;
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
