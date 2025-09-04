-- Unified Planning Function
CREATE OR REPLACE FUNCTION sql_saga.temporal_merge_plan(
    p_target_table regclass,
    p_source_table regclass,
    p_id_columns TEXT[],
    p_ephemeral_columns TEXT[],
    p_mode sql_saga.temporal_merge_mode,
    p_era_name name,
    p_source_row_id_column name DEFAULT 'row_id',
    p_founding_id_column name DEFAULT NULL,
    p_delete_mode sql_saga.temporal_merge_delete_mode DEFAULT 'NONE'
) RETURNS SETOF sql_saga.temporal_plan_op
LANGUAGE plpgsql VOLATILE AS $temporal_merge_plan$
DECLARE
    v_plan_key_text TEXT;
    v_plan_ps_name TEXT;
    v_target_schema_name name;
    v_target_table_name_only name;
    v_range_constructor name;
    v_valid_from_col name;
    v_valid_until_col name;
BEGIN
    -- An entity must be identifiable. Fail fast if no ID columns are provided.
    IF p_id_columns IS NULL OR cardinality(p_id_columns) = 0 THEN
        RAISE EXCEPTION 'p_id_columns must be a non-empty array of column names that form the entity identifier.';
    END IF;

    -- Validate that provided column names exist.
    PERFORM 1 FROM pg_attribute WHERE attrelid = p_source_table AND attname = p_source_row_id_column AND NOT attisdropped AND attnum > 0;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'p_source_row_id_column "%" does not exist in source table %s', p_source_row_id_column, p_source_table::text;
    END IF;

    IF p_founding_id_column IS NOT NULL THEN
        PERFORM 1 FROM pg_attribute WHERE attrelid = p_source_table AND attname = p_founding_id_column AND NOT attisdropped AND attnum > 0;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'p_founding_id_column "%" does not exist in source table %s', p_founding_id_column, p_source_table::text;
        END IF;
    END IF;

    -- Introspect just enough to get the range constructor, which is part of the cache key.
    SELECT n.nspname, c.relname
    INTO v_target_schema_name, v_target_table_name_only
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = p_target_table;

    SELECT e.range_type::name, e.valid_from_column_name, e.valid_until_column_name
    INTO v_range_constructor, v_valid_from_col, v_valid_until_col
    FROM sql_saga.era e
    WHERE e.table_schema = v_target_schema_name
      AND e.table_name = v_target_table_name_only
      AND e.era_name = p_era_name;

    IF v_valid_from_col IS NULL THEN
        RAISE EXCEPTION 'No era named "%" found for table "%"', p_era_name, p_target_table;
    END IF;

    -- Generate the cache key first from all relevant arguments. This is fast.
    v_plan_key_text := format('%s:%s:%s:%s:%s:%s:%s:%s:%s:%s',
        p_target_table::oid,
        p_source_table::oid,
        p_id_columns,
        p_ephemeral_columns,
        p_mode,
        p_era_name,
        p_source_row_id_column,
        COALESCE(p_founding_id_column, ''),
        v_range_constructor,
        p_delete_mode
    );
    v_plan_ps_name := 'tm_plan_' || md5(v_plan_key_text);

    -- If the prepared statement already exists, execute it and exit immediately.
    IF EXISTS (SELECT 1 FROM pg_prepared_statements WHERE name = v_plan_ps_name) THEN
        RETURN QUERY EXECUTE format('EXECUTE %I', v_plan_ps_name);
        RETURN;
    END IF;

    -- On cache miss, enter a new block to declare variables and do the expensive work.
    DECLARE
        v_sql TEXT;
        v_source_data_cols_jsonb_build TEXT;
        v_target_data_cols_jsonb_build TEXT;
        v_entity_id_as_jsonb TEXT;
        v_source_schema_name TEXT;
        v_source_table_ident TEXT;
        v_target_table_ident TEXT;
        v_source_data_payload_expr TEXT;
        v_final_data_payload_expr TEXT;
        v_resolver_ctes TEXT;
        v_resolver_from TEXT;
        v_diff_join_condition TEXT;
        v_plan_source_row_ids_expr TEXT;
        v_entity_id_check_is_null_expr TEXT;
        v_founding_id_select_expr TEXT;
        v_planner_entity_id_expr TEXT;
        v_target_rows_filter TEXT;
    BEGIN
        -- On cache miss, proceed with the expensive introspection and query building.
        IF p_founding_id_column IS NOT NULL THEN
            IF p_founding_id_column = ANY(p_id_columns) THEN
                RAISE EXCEPTION 'p_founding_id_column (%) cannot be one of the p_id_columns (%)', p_founding_id_column, p_id_columns;
            END IF;

            v_founding_id_select_expr := format('t.%I as founding_id,', p_founding_id_column);
            v_planner_entity_id_expr := format($$
                CASE
                    WHEN si.is_new_entity
                    THEN si.entity_id || jsonb_build_object(%L, si.founding_id::text)
                    ELSE si.entity_id
                END
            $$, p_founding_id_column);
        ELSE
            v_founding_id_select_expr := format('t.%I as founding_id,', p_source_row_id_column);
            v_planner_entity_id_expr := format($$
                CASE
                    WHEN si.is_new_entity
                    THEN si.entity_id || jsonb_build_object(%L, si.founding_id::text)
                    ELSE si.entity_id
                END
            $$, COALESCE(p_founding_id_column, p_source_row_id_column));
        END IF;

        -- Resolve table identifiers to be correctly quoted and schema-qualified.
        v_target_table_ident := p_target_table::TEXT;

        SELECT n.nspname INTO v_source_schema_name
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.oid = p_source_table;

        IF v_source_schema_name = 'pg_temp' THEN
            v_source_table_ident := p_source_table::regclass::TEXT;
        ELSE
            v_source_table_ident := p_source_table::regclass::TEXT;
        END IF;

        -- Dynamically construct a jsonb object from the entity id columns to use as a single key for partitioning and joining.
        SELECT
            format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, t.%I', col, col), ', '), ''))
        INTO
            v_entity_id_as_jsonb
        FROM unnest(p_id_columns) AS col;

        -- 1. Dynamically get the list of common data columns from SOURCE and TARGET tables.
        WITH source_cols AS (
            SELECT pa.attname
            FROM pg_catalog.pg_attribute pa
            WHERE pa.attrelid = p_source_table
              AND pa.attnum > 0 AND NOT pa.attisdropped
        ),
        target_cols AS (
            SELECT pa.attname
            FROM pg_catalog.pg_attribute pa
            WHERE pa.attrelid = p_target_table
              AND pa.attnum > 0 AND NOT pa.attisdropped
        ),
        common_data_cols AS (
            SELECT s.attname
            FROM source_cols s JOIN target_cols t ON s.attname = t.attname
            WHERE s.attname NOT IN (p_source_row_id_column, v_valid_from_col, v_valid_until_col, 'era_id', 'era_name')
              AND s.attname <> ALL(p_id_columns)
        )
        SELECT
            format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, t.%I', attname, attname), ', '), ''))
        INTO
            v_source_data_cols_jsonb_build -- Re-use this variable for the common expression
        FROM
            common_data_cols;

        v_target_data_cols_jsonb_build := v_source_data_cols_jsonb_build; -- Both source and target use the same payload structure
        v_source_data_cols_jsonb_build := COALESCE(v_source_data_cols_jsonb_build, '''{}''::jsonb');
        v_target_data_cols_jsonb_build := COALESCE(v_target_data_cols_jsonb_build, '''{}''::jsonb');

        -- Construct an expression to reliably check if all entity_id columns for a source row are NULL.
        SELECT
            COALESCE(string_agg(format('t.%I IS NULL', col), ' AND '), 'true')
        INTO
            v_entity_id_check_is_null_expr
        FROM unnest(p_id_columns) AS col;

        -- Determine the scope of target entities to process based on the mode.
        -- By default, we optimize by only scanning target entities that are present in the source.
        v_target_rows_filter := format('WHERE (%s) IN (SELECT DISTINCT entity_id FROM source_initial)', v_entity_id_as_jsonb);
        -- For modes that might delete entities not in the source, we must scan the entire target table.
        IF p_mode IN ('MERGE_ENTITY_PATCH', 'MERGE_ENTITY_REPLACE') AND p_delete_mode IN ('DELETE_MISSING_ENTITIES', 'DELETE_MISSING_TIMELINE_AND_ENTITIES') THEN
            v_target_rows_filter := '';
        END IF;

        -- 2. Construct expressions and resolver CTEs based on the mode.
        -- This logic is structured to be orthogonal:
        -- 1. Payload Handling (_PATCH vs. _REPLACE) is determined first.
        -- 2. Timeline Handling (destructive vs. non-destructive) is determined by p_delete_mode.

        -- First, determine payload semantics based on the mode name.
        IF p_mode IN ('MERGE_ENTITY_PATCH', 'PATCH_FOR_PORTION_OF') THEN
            v_source_data_payload_expr := format('jsonb_strip_nulls(%s)', v_source_data_cols_jsonb_build);
        ELSIF p_mode = 'DELETE_FOR_PORTION_OF' THEN
            v_source_data_payload_expr := 'NULL::jsonb';
        ELSE -- MERGE_ENTITY_REPLACE, REPLACE_FOR_PORTION_OF, INSERT_NEW_ENTITIES
            v_source_data_payload_expr := v_source_data_cols_jsonb_build;
        END IF;

        -- Second, determine timeline semantics based *only* on p_delete_mode.
        IF p_delete_mode IN ('DELETE_MISSING_TIMELINE', 'DELETE_MISSING_TIMELINE_AND_ENTITIES') THEN
            -- Destructive mode: Source is truth. Non-overlapping target timeline is discarded.
            -- This uses a simple diff join, resulting in a robust DELETE and INSERT plan for changes.
            v_diff_join_condition := 'f.f_from = t.t_from';
            v_plan_source_row_ids_expr := 'd.f_source_row_ids';
            v_resolver_from := 'resolved_atomic_segments_with_payloads';
            v_resolver_ctes := '';
            -- In destructive mode, the final state is defined entirely by the source payload.
            -- The v_source_data_payload_expr already correctly handles PATCH vs REPLACE semantics.
            v_final_data_payload_expr := 's_data_payload';
        ELSE
            -- Non-destructive default mode: Merge timelines, preserving non-overlapping parts of the target.
            -- This requires a complex diff join and special handling for payload inheritance.
            v_diff_join_condition := $$
               f.f_from = t.t_from
               OR (
                   f.f_until = t.t_until AND f.f_data IS NOT DISTINCT FROM t.t_data
                   AND NOT EXISTS (
                       SELECT 1 FROM coalesced_final_segments f_inner
                       WHERE f_inner.entity_id = t.t_entity_id AND f_inner.valid_from = t.t_from
                   )
               )
            $$;
            v_plan_source_row_ids_expr := $$COALESCE(
                d.f_source_row_ids,
                MAX(d.f_source_row_ids) OVER (PARTITION BY d.entity_id, d.t_from)
            )$$;

            IF p_mode IN ('MERGE_ENTITY_PATCH', 'PATCH_FOR_PORTION_OF') THEN
                -- In PATCH modes, we inherit data from previous target segments to fill gaps.
                v_resolver_ctes := $$,
    segments_with_target_start AS (
        SELECT
            *,
            (t_valid_from = valid_from) as is_target_start_segment
        FROM resolved_atomic_segments_with_payloads
    ),
    payload_groups AS (
        SELECT
            *,
            SUM(CASE WHEN is_target_start_segment THEN 1 ELSE 0 END) OVER (PARTITION BY entity_id ORDER BY valid_from) as payload_group
        FROM segments_with_target_start
    ),
    resolved_atomic_segments_with_inherited_payload AS (
        SELECT
            *,
            FIRST_VALUE(t_data_payload) OVER (PARTITION BY entity_id, payload_group ORDER BY valid_from) as inherited_t_data_payload
        FROM payload_groups
    )
$$;
                v_final_data_payload_expr := $$(CASE
                    WHEN s_data_payload IS NOT NULL THEN (COALESCE(t_data_payload, inherited_t_data_payload, '{}'::jsonb) || s_data_payload)
                    ELSE COALESCE(t_data_payload, inherited_t_data_payload)
                END)$$;
                v_resolver_from := 'resolved_atomic_segments_with_inherited_payload';
            ELSE -- REPLACE and DELETE modes
                -- In REPLACE/DELETE modes, we do not inherit. Gaps remain gaps.
                v_resolver_ctes := '';
                v_final_data_payload_expr := $$COALESCE(s_data_payload, t_data_payload)$$;
                v_resolver_from := 'resolved_atomic_segments_with_payloads';
            END IF;
        END IF;

        -- Layer on optional destructive delete modes for missing entities.
        IF p_delete_mode IN ('DELETE_MISSING_ENTITIES', 'DELETE_MISSING_TIMELINE_AND_ENTITIES') THEN
            -- We need to add a flag to identify entities that are present in the source.
            -- This CTE is chained onto any previous resolver CTEs by using v_resolver_from.
            v_resolver_ctes := v_resolver_ctes || format($$,
    resolved_atomic_segments_with_flag AS (
        SELECT *,
            bool_or(s_data_payload IS NOT NULL) OVER (PARTITION BY entity_id) as entity_is_in_source
        FROM %s
    )
$$, v_resolver_from);
            v_resolver_from := 'resolved_atomic_segments_with_flag';

            -- If deleting missing entities, the final payload for those entities must be NULL.
            -- For entities present in the source, the existing v_final_data_payload_expr is correct.
            v_final_data_payload_expr := format($$CASE WHEN entity_is_in_source THEN (%s) ELSE NULL END$$, v_final_data_payload_expr);
        END IF;

        -- 4. Construct and execute the main query to generate the execution plan.
        v_sql := format($SQL$
WITH
source_initial AS (
    SELECT
        t.%18$I as source_row_id,
        %16$s
        %1$s as entity_id,
        t.%14$I as valid_from,
        t.%15$I as valid_until,
        %2$s AS data_payload,
        %13$s as is_new_entity
    FROM %3$s t
),
target_rows AS (
    SELECT
        %1$s as entity_id,
        t.%14$I as valid_from,
        t.%15$I as valid_until,
        %6$s AS data_payload
    FROM %4$s t
    %20$s -- v_target_rows_filter
),
source_rows AS (
    -- Filter the initial source rows based on the operation mode.
    SELECT
        si.source_row_id as source_row_id,
        -- If it's a new entity, synthesize a temporary unique ID by embedding the founding_id,
        -- so the planner can distinguish and group new entities.
        %17$s as entity_id,
        si.valid_from,
        si.valid_until,
        si.data_payload
    FROM source_initial si
    WHERE CASE %7$L::sql_saga.temporal_merge_mode
        -- MERGE_ENTITY modes process all source rows initially; they handle existing vs. new entities in the planner.
        WHEN 'MERGE_ENTITY_PATCH' THEN true
        WHEN 'MERGE_ENTITY_REPLACE' THEN true
        -- INSERT_NEW_ENTITIES is optimized to only consider rows for entities that are new to the target.
        WHEN 'INSERT_NEW_ENTITIES' THEN si.entity_id NOT IN (SELECT tr.entity_id FROM target_rows tr)
        -- ..._FOR_PORTION_OF modes are optimized to only consider rows for entities that already exist in the target.
        WHEN 'PATCH_FOR_PORTION_OF' THEN si.entity_id IN (SELECT tr.entity_id FROM target_rows tr)
        WHEN 'REPLACE_FOR_PORTION_OF' THEN si.entity_id IN (SELECT tr.entity_id FROM target_rows tr)
        WHEN 'DELETE_FOR_PORTION_OF' THEN si.entity_id IN (SELECT tr.entity_id FROM target_rows tr)
        ELSE false
    END
),
all_rows AS (
    SELECT entity_id, valid_from, valid_until FROM source_rows
    UNION ALL
    SELECT entity_id, valid_from, valid_until FROM target_rows
),
time_points AS (
    SELECT DISTINCT entity_id, point FROM (
        SELECT entity_id, valid_from AS point FROM all_rows
        UNION ALL
        SELECT entity_id, valid_until AS point FROM all_rows
    ) AS points
),
atomic_segments AS (
    SELECT entity_id, point as valid_from, LEAD(point) OVER (PARTITION BY entity_id ORDER BY point) as valid_until
    FROM time_points WHERE point IS NOT NULL
),
resolved_atomic_segments_with_payloads AS (
    SELECT
        seg.entity_id,
        seg.valid_from,
        seg.valid_until,
        t.t_valid_from,
        ( -- Find causal source row
            SELECT sr.source_row_id FROM source_rows sr
            WHERE sr.entity_id = seg.entity_id
              AND (
                  %19$I(sr.valid_from, sr.valid_until) && %19$I(seg.valid_from, seg.valid_until)
                  OR (
                      %19$I(sr.valid_from, sr.valid_until) -|- %19$I(seg.valid_from, seg.valid_until)
                      AND EXISTS (
                          SELECT 1 FROM target_rows tr
                          WHERE tr.entity_id = sr.entity_id
                            AND %19$I(sr.valid_from, sr.valid_until) && %19$I(tr.valid_from, tr.valid_until)
                      )
                  )
              )
            -- Prioritize the latest source row in case of overlaps
            ORDER BY sr.source_row_id DESC LIMIT 1
        ) as source_row_id,
        s.data_payload as s_data_payload,
        t.data_payload as t_data_payload
    FROM atomic_segments seg
    LEFT JOIN LATERAL (
        SELECT tr.data_payload, tr.valid_from as t_valid_from
        FROM target_rows tr
        WHERE tr.entity_id = seg.entity_id
          AND %19$I(seg.valid_from, seg.valid_until) <@ %19$I(tr.valid_from, tr.valid_until)
    ) t ON true
    LEFT JOIN LATERAL (
        SELECT sr.data_payload
        FROM source_rows sr
        WHERE sr.entity_id = seg.entity_id
          AND %19$I(seg.valid_from, seg.valid_until) <@ %19$I(sr.valid_from, sr.valid_until)
        -- In case of overlapping source rows, the one with the highest row_id (latest) wins.
        ORDER BY sr.source_row_id DESC
        LIMIT 1
    ) s ON true
    WHERE seg.valid_from < seg.valid_until
      AND (s.data_payload IS NOT NULL OR t.data_payload IS NOT NULL) -- Filter out gaps
)
%9$s,
resolved_atomic_segments AS (
    SELECT
        entity_id,
        valid_from,
        valid_until,
        t_valid_from,
        source_row_id,
        %8$s as data_payload,
        CASE WHEN s_data_payload IS NOT NULL THEN 1 ELSE 2 END as priority
    FROM %10$s
),
coalesced_final_segments AS (
    SELECT
        entity_id,
        MIN(valid_from) as valid_from,
        MAX(valid_until) as valid_until,
        data_payload,
        -- Aggregate the source_row_id from each atomic segment into a single array for the merged block.
        array_agg(DISTINCT source_row_id) FILTER (WHERE source_row_id IS NOT NULL) as source_row_ids
    FROM (
        SELECT
            *,
            -- This window function creates a grouping key (segment_group). A new group starts
            -- whenever there is a time gap or a change in the data payload.
            SUM(is_new_segment) OVER (PARTITION BY entity_id ORDER BY valid_from) as segment_group
        FROM (
            SELECT
                ras.*,
                CASE
                    -- A new segment starts if there is a gap between it and the previous one,
                    -- or if the data payload changes. For [) intervals,
                    -- contiguity is defined as the previous `valid_until` being equal to the current `valid_from`.
                    WHEN LAG(ras.valid_until) OVER (PARTITION BY ras.entity_id ORDER BY ras.valid_from) = ras.valid_from
                     AND LAG(ras.data_payload - %5$L::text[]) OVER (PARTITION BY ras.entity_id ORDER BY ras.valid_from) IS NOT DISTINCT FROM (ras.data_payload - %5$L::text[])
                    THEN 0 -- Not a new group (contiguous and same data)
                    ELSE 1 -- Is a new group (time gap or different data)
                END as is_new_segment
            FROM resolved_atomic_segments ras
        ) with_new_segment_flag
    ) with_segment_group
    GROUP BY
        entity_id,
        segment_group,
        data_payload
),

diff AS (
    SELECT
        -- Use COALESCE on entity_id to handle full additions/deletions
        COALESCE(f.f_entity_id, t.t_entity_id) as entity_id,
        f.f_from, f.f_until, f.f_data, f.f_source_row_ids,
        t.t_from, t.t_until, t.t_data,
        -- This function call determines the Allen Interval Relation between the final state and target state segments
        sql_saga.allen_get_relation(f.f_from, f.f_until, t.t_from, t.t_until) as relation
    FROM
    (
        SELECT
            entity_id AS f_entity_id,
            valid_from AS f_from,
            valid_until AS f_until,
            data_payload AS f_data,
            source_row_ids AS f_source_row_ids
        FROM coalesced_final_segments
        WHERE data_payload IS NOT NULL
    ) f
    FULL OUTER JOIN
    (
        SELECT
            entity_id as t_entity_id,
            valid_from as t_from,
            valid_until as t_until,
            data_payload as t_data
        FROM target_rows
    ) t
    ON f.f_entity_id = t.t_entity_id AND (%11$s)
),
plan AS (
    SELECT
        %12$s as source_row_ids,
        CASE
            WHEN d.f_data IS NULL THEN 'DELETE'::sql_saga.planner_action
            WHEN d.t_data IS NULL THEN 'INSERT'::sql_saga.planner_action
            WHEN d.f_data IS DISTINCT FROM d.t_data
              OR d.f_from IS DISTINCT FROM d.t_from
              OR d.f_until IS DISTINCT FROM d.t_until
            THEN 'UPDATE'::sql_saga.planner_action
            ELSE 'IDENTICAL'::sql_saga.planner_action
        END as operation,
        d.entity_id,
        d.t_from as old_valid_from,
        d.f_from as new_valid_from,
        d.f_until as new_valid_until,
        d.f_data as data,
        d.relation
    FROM diff d
)
SELECT
    row_number() OVER (ORDER BY p.entity_id, COALESCE(p.new_valid_from, p.old_valid_from), p.operation DESC, (p.source_row_ids[1]))::BIGINT as plan_op_seq,
    p.source_row_ids,
    p.operation,
    p.entity_id AS entity_ids,
    p.old_valid_from::TEXT,
    p.new_valid_from::TEXT,
    p.new_valid_until::TEXT,
    p.data,
    p.relation
FROM plan p
ORDER BY plan_op_seq;
$SQL$,
            v_entity_id_as_jsonb,           -- 1
            v_source_data_payload_expr,     -- 2
            v_source_table_ident,           -- 3
            v_target_table_ident,           -- 4
            p_ephemeral_columns,            -- 5
            v_target_data_cols_jsonb_build, -- 6
            p_mode,                         -- 7
            v_final_data_payload_expr,      -- 8
            v_resolver_ctes,                -- 9
            v_resolver_from,                -- 10
            v_diff_join_condition,          -- 11
            v_plan_source_row_ids_expr,     -- 12
            v_entity_id_check_is_null_expr, -- 13
            v_valid_from_col,               -- 14
            v_valid_until_col,              -- 15
            v_founding_id_select_expr,      -- 16
            v_planner_entity_id_expr,       -- 17
            p_source_row_id_column,         -- 18
            v_range_constructor,            -- 19
            v_target_rows_filter            -- 20
        );

        EXECUTE format('PREPARE %I AS %s', v_plan_ps_name, v_sql);

        RETURN QUERY EXECUTE format('EXECUTE %I', v_plan_ps_name);
    END;
END;
$temporal_merge_plan$;

COMMENT ON FUNCTION sql_saga.temporal_merge_plan(regclass, regclass, text[], text[], sql_saga.temporal_merge_mode, name, name, name, sql_saga.temporal_merge_delete_mode) IS
'Generates a set-based execution plan for a temporal merge. This function is marked VOLATILE because it uses PREPARE to cache its expensive planning query for the duration of the session, which is a side-effect not permitted in STABLE or IMMUTABLE functions.';


-- Unified Orchestrator Function
CREATE OR REPLACE PROCEDURE sql_saga.temporal_merge(
    p_target_table regclass,
    p_source_table regclass,
    p_id_columns TEXT[],
    p_ephemeral_columns TEXT[],
    p_mode sql_saga.temporal_merge_mode DEFAULT 'MERGE_ENTITY_PATCH',
    p_era_name name DEFAULT 'valid',
    p_source_row_id_column name DEFAULT 'row_id',
    p_founding_id_column name DEFAULT NULL,
    p_update_source_with_assigned_entity_ids BOOLEAN DEFAULT false,
    p_delete_mode sql_saga.temporal_merge_delete_mode DEFAULT 'NONE'
)
LANGUAGE plpgsql AS $temporal_merge$
DECLARE
    v_target_table_ident TEXT := p_target_table::TEXT;
    v_data_cols_ident TEXT;
    v_data_cols_select TEXT;
    v_update_set_clause TEXT;
    v_all_cols_ident TEXT;
    v_all_cols_select TEXT;
    v_entity_key_join_clause TEXT;
    v_all_source_row_ids INTEGER[];
    v_target_schema_name name;
    v_target_table_name_only name;
    v_valid_from_col name;
    v_valid_until_col name;
    v_valid_from_col_type regtype;
    v_valid_until_col_type regtype;
    v_insert_defaulted_columns TEXT[];
    v_all_cols_from_jsonb TEXT;
    v_internal_keys_to_remove TEXT[];
BEGIN
    v_internal_keys_to_remove := ARRAY[]::name[];

    IF p_founding_id_column IS NOT NULL THEN
        v_internal_keys_to_remove := v_internal_keys_to_remove || p_founding_id_column;
    ELSE
        v_internal_keys_to_remove := v_internal_keys_to_remove || p_source_row_id_column;
    END IF;

    -- If the feedback table exists from a previous call in the same transaction,
    -- truncate it. Otherwise, create it.
    IF to_regclass('pg_temp.temporal_merge_feedback') IS NULL THEN
        CREATE TEMP TABLE temporal_merge_feedback (LIKE sql_saga.temporal_merge_result) ON COMMIT DROP;
    ELSE
        TRUNCATE TABLE pg_temp.temporal_merge_feedback;
    END IF;

    -- Auto-detect columns that should be excluded from INSERT statements.
    -- This includes columns with defaults, identity columns, and generated columns.
    SELECT COALESCE(array_agg(a.attname), '{}')
    INTO v_insert_defaulted_columns
    FROM pg_catalog.pg_attribute a
    WHERE a.attrelid = p_target_table
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND (a.atthasdef OR a.attidentity IN ('a', 'd') OR a.attgenerated <> '');

    -- Introspect era information to get the correct column names
    SELECT n.nspname, c.relname
    INTO v_target_schema_name, v_target_table_name_only
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = p_target_table;

    SELECT e.valid_from_column_name, e.valid_until_column_name
    INTO v_valid_from_col, v_valid_until_col
    FROM sql_saga.era e
    WHERE e.table_schema = v_target_schema_name
      AND e.table_name = v_target_table_name_only
      AND e.era_name = p_era_name;

    IF v_valid_from_col IS NULL THEN
        RAISE EXCEPTION 'No era named "%" found for table "%"', p_era_name, p_target_table;
    END IF;

    SELECT atttypid::regtype INTO v_valid_from_col_type FROM pg_attribute WHERE attrelid = p_target_table AND attname = v_valid_from_col;
    SELECT atttypid::regtype INTO v_valid_until_col_type FROM pg_attribute WHERE attrelid = p_target_table AND attname = v_valid_until_col;

    EXECUTE format('SELECT array_agg(%I) FROM %s', p_source_row_id_column, p_source_table::TEXT) INTO v_all_source_row_ids;

        -- Dynamically construct join clause for composite entity key.
        SELECT
            string_agg(format('t.%I = jpr_entity.%I', col, col), ' AND ')
        INTO
            v_entity_key_join_clause
        FROM unnest(p_id_columns) AS col;

        v_entity_key_join_clause := COALESCE(v_entity_key_join_clause, 'true');

        IF to_regclass('pg_temp.temporal_merge_plan') IS NULL THEN
            CREATE TEMP TABLE temporal_merge_plan (LIKE sql_saga.temporal_plan_op, PRIMARY KEY (plan_op_seq)) ON COMMIT DROP;
        ELSE
            TRUNCATE TABLE pg_temp.temporal_merge_plan;
        END IF;

        INSERT INTO temporal_merge_plan
        SELECT * FROM sql_saga.temporal_merge_plan(
            p_target_table             => p_target_table,
            p_source_table             => p_source_table,
            p_id_columns               => p_id_columns,
            p_ephemeral_columns        => p_ephemeral_columns,
            p_mode                     => p_mode,
            p_era_name                 => p_era_name,
            p_source_row_id_column     => p_source_row_id_column,
            p_founding_id_column       => p_founding_id_column,
            p_delete_mode              => p_delete_mode
        ) p;

        -- Get dynamic column lists for DML, mimicking the planner's introspection logic for consistency.
        WITH source_cols AS (
            SELECT pa.attname
            FROM pg_catalog.pg_attribute pa
            WHERE pa.attrelid = p_source_table AND pa.attnum > 0 AND NOT pa.attisdropped
        ),
        target_cols AS (
            SELECT pa.attname, pa.atttypid
            FROM pg_catalog.pg_attribute pa
            WHERE pa.attrelid = p_target_table AND pa.attnum > 0 AND NOT pa.attisdropped
        ),
        common_data_cols AS (
            SELECT t.attname, t.atttypid
            FROM source_cols s JOIN target_cols t ON s.attname = t.attname
            WHERE s.attname NOT IN (p_source_row_id_column, v_valid_from_col, v_valid_until_col, 'era_id', 'era_name')
              AND s.attname <> ALL(p_id_columns)
        ),
        all_available_cols AS (
            SELECT c.attname, c.atttypid FROM common_data_cols c
            UNION ALL
            SELECT u.attname, t.atttypid
            FROM unnest(p_id_columns) u(attname)
            JOIN target_cols t ON u.attname = t.attname
        )
        SELECT
            string_agg(format('%I', attname), ', ') FILTER (WHERE attname <> ALL(p_id_columns)),
            string_agg(format('jpr_data.%I', attname), ', ') FILTER (WHERE attname <> ALL(p_id_columns)),
            string_agg(format('%I = jpr_data.%I', attname, attname), ', ') FILTER (WHERE attname <> ALL(p_id_columns)),
            string_agg(format('%I', attname), ', ') FILTER (WHERE attname <> ALL(v_insert_defaulted_columns)),
            string_agg(format('jpr_all.%I', attname), ', ') FILTER (WHERE attname <> ALL(v_insert_defaulted_columns)),
            string_agg(format('(s.full_data->>%L)::%s', attname, format_type(atttypid, -1)), ', ')
                FILTER (WHERE attname <> ALL(v_insert_defaulted_columns) AND attname NOT IN (v_valid_from_col, v_valid_until_col))
        INTO
            v_data_cols_ident,
            v_data_cols_select,
            v_update_set_clause,
            v_all_cols_ident,
            v_all_cols_select,
            v_all_cols_from_jsonb
        FROM all_available_cols;

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
                    SELECT col FROM unnest(p_id_columns) as col
                    UNION
                    SELECT 'id'
                    WHERE 'id' IN (SELECT attname FROM target_cols) AND 'id' <> ALL(p_id_columns)
                )
                SELECT
                    format('jsonb_build_object(%s)', string_agg(format('%L, ir.%I', col, col), ', '))
                INTO
                    v_entity_id_update_jsonb_build
                FROM feedback_id_cols;

                -- If founding_id_column is used, we need the multi-stage "Smart Merge" process
                IF p_founding_id_column IS NOT NULL AND
                   (SELECT TRUE FROM temporal_merge_plan WHERE entity_ids ? p_founding_id_column LIMIT 1)
                THEN
                    CREATE TEMP TABLE temporal_merge_entity_id_map (founding_id TEXT PRIMARY KEY, new_entity_ids JSONB) ON COMMIT DROP;

                    -- Stage 1: Insert just ONE row for each new conceptual entity to generate the ID.
                    -- Use a MERGE statement to get a direct mapping from the founding_id to the new key.
                    EXECUTE format($$
                        WITH founding_plan_ops AS (
                            SELECT DISTINCT ON (p.entity_ids->>%7$L)
                                p.plan_op_seq,
                                p.entity_ids->>%7$L as founding_id,
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
                            RETURNING t.*, s.founding_id
                        )
                        INSERT INTO temporal_merge_entity_id_map (founding_id, new_entity_ids)
                        SELECT
                            ir.founding_id,
                            %4$s -- v_entity_id_update_jsonb_build expression
                        FROM id_map_cte ir;
                    $$,
                        v_target_table_ident,           -- 1
                        v_all_cols_ident,               -- 2
                        v_all_cols_from_jsonb,          -- 3
                        v_entity_id_update_jsonb_build, -- 4
                        v_valid_from_col,               -- 5
                        v_valid_until_col,              -- 6
                        p_founding_id_column,           -- 7
                        v_valid_from_col_type,          -- 8
                        v_valid_until_col_type          -- 9
                    );

                    -- Stage 2: Back-fill the generated IDs into the plan for all dependent operations,
                    -- preserving the internal founding_id key for the next step.
                    EXECUTE format($$
                        UPDATE temporal_merge_plan p
                        SET entity_ids = m.new_entity_ids || jsonb_build_object(%1$L, p.entity_ids->>%1$L)
                        FROM temporal_merge_entity_id_map m
                        WHERE p.entity_ids->>%1$L = m.founding_id;
                    $$, p_founding_id_column);

                    -- Stage 3: Insert the remaining slices, which now have the correct foreign key.
                    DECLARE
                        v_stage3_all_cols_ident TEXT;
                        v_stage3_all_cols_select TEXT;
                    BEGIN
                        -- Re-calculate column lists for Stage 3 to INCLUDE the surrogate key columns.
                        -- We can now safely insert all non-generated columns.
                        WITH target_cols AS (
                            SELECT pa.attname
                            FROM pg_catalog.pg_attribute pa
                            WHERE pa.attrelid = p_target_table
                              AND pa.attnum > 0 AND NOT pa.attisdropped
                              AND pa.attgenerated = '' -- Exclude only generated columns
                              AND pa.attname NOT IN (v_valid_from_col, v_valid_until_col)
                        )
                        SELECT
                            string_agg(format('%I', attname), ', '),
                            string_agg(format('jpr_all.%I', attname), ', ')
                        INTO
                            v_stage3_all_cols_ident,
                            v_stage3_all_cols_select
                        FROM target_cols;

                        EXECUTE format($$
                            INSERT INTO %1$s (%2$s, %4$I, %5$I)
                            SELECT %3$s, p.new_valid_from::%7$s, p.new_valid_until::%8$s
                            FROM temporal_merge_plan p,
                                 LATERAL jsonb_populate_record(null::%1$s, p.entity_ids || p.data) as jpr_all
                            WHERE p.operation = 'INSERT'
                              AND NOT EXISTS ( -- Exclude the "founding" rows we already inserted
                                SELECT 1 FROM (
                                    SELECT DISTINCT ON (p_inner.entity_ids->>%6$L) plan_op_seq
                                    FROM temporal_merge_plan p_inner
                                    WHERE p_inner.operation = 'INSERT' AND p_inner.entity_ids ? %6$L
                                    ORDER BY p_inner.entity_ids->>%6$L, p_inner.plan_op_seq
                                ) AS founding_ops
                                WHERE founding_ops.plan_op_seq = p.plan_op_seq
                              );
                        $$,
                            v_target_table_ident,       -- 1
                            v_stage3_all_cols_ident,    -- 2
                            v_stage3_all_cols_select,   -- 3
                            v_valid_from_col,           -- 4
                            v_valid_until_col,          -- 5
                            p_founding_id_column,       -- 6
                            v_valid_from_col_type,      -- 7
                            v_valid_until_col_type      -- 8
                        );
                    END;

                    DROP TABLE temporal_merge_entity_id_map;
                ELSE
                    -- Standard case: No intra-step dependencies, or not using founding_id.
                    -- Use the robust MERGE pattern to handle all INSERTS in one go.
                    EXECUTE format($$
                        WITH
                        source_for_insert AS (
                            SELECT
                                p.plan_op_seq,
                                p.new_valid_from,
                                p.new_valid_until,
                                p.entity_ids || p.data as full_data
                            FROM temporal_merge_plan p
                            WHERE p.operation = 'INSERT'
                        ),
                        inserted_rows AS (
                            MERGE INTO %1$s t
                            USING source_for_insert s ON false
                            WHEN NOT MATCHED THEN
                                INSERT (%2$s, %5$I, %6$I)
                                VALUES (%3$s, s.new_valid_from::%7$s, s.new_valid_until::%8$s)
                            RETURNING t.*, s.plan_op_seq
                        )
                        UPDATE temporal_merge_plan p
                        SET entity_ids = %4$s
                        FROM inserted_rows ir
                        WHERE p.plan_op_seq = ir.plan_op_seq;
                    $$,
                        v_target_table_ident,           -- 1
                        v_all_cols_ident,               -- 2
                        v_all_cols_from_jsonb,          -- 3
                        v_entity_id_update_jsonb_build, -- 4
                        v_valid_from_col,               -- 5
                        v_valid_until_col,              -- 6
                        v_valid_from_col_type,          -- 7
                        v_valid_until_col_type          -- 8
                    );
                END IF;
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
                    SELECT unnest(p_id_columns) as col
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
                EXECUTE format($$
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
                    v_target_table_ident,           -- 1
                    v_entity_id_update_jsonb_build, -- 2
                    v_valid_from_col,               -- 3
                    v_valid_until_col,              -- 4
                    v_valid_from_col_type,          -- 5
                    v_valid_until_col_type          -- 6
                );
             END;
        END IF;

        -- Back-fill source table with generated IDs if requested.
        IF p_update_source_with_assigned_entity_ids THEN
            DECLARE
                v_source_update_set_clause TEXT;
            BEGIN
                -- Build a SET clause for all columns that are present in the entity_ids JSONB
                -- AND exist as columns on the source table. This is robust to surrogate keys
                -- that are not in p_id_columns.
                SELECT string_agg(
                    format('%I = (p.entity_ids->>%L)::%s', j.key, j.key, format_type(a.atttypid, a.atttypmod)),
                    ', '
                )
                INTO v_source_update_set_clause
                FROM (
                    SELECT key FROM jsonb_object_keys(
                        (SELECT entity_ids FROM temporal_merge_plan WHERE entity_ids IS NOT NULL and operation = 'INSERT' LIMIT 1)
                    ) as key
                ) j
                JOIN pg_attribute a ON a.attname = j.key
                WHERE a.attrelid = p_source_table AND NOT a.attisdropped AND a.attnum > 0;

                IF v_source_update_set_clause IS NOT NULL THEN
                    EXECUTE format($$
                        WITH map_row_to_entity AS (
                            SELECT DISTINCT ON (s.source_row_id)
                                s.source_row_id,
                                p.entity_ids
                            FROM (SELECT DISTINCT unnest(source_row_ids) AS source_row_id FROM temporal_merge_plan WHERE operation = 'INSERT') s
                            JOIN temporal_merge_plan p ON s.source_row_id = ANY(p.source_row_ids)
                            WHERE p.entity_ids IS NOT NULL
                            ORDER BY s.source_row_id, p.plan_op_seq
                        )
                        UPDATE %1$s s
                        SET %2$s
                        FROM map_row_to_entity p
                        WHERE s.%3$I = p.source_row_id;
                    $$, p_source_table::text, v_source_update_set_clause, p_source_row_id_column);
                END IF;
            END;
        END IF;

        -- 2. Execute UPDATE operations
        IF v_update_set_clause IS NOT NULL THEN
            EXECUTE format($$ UPDATE %1$s t SET %4$I = p.new_valid_from::%6$s, %5$I = p.new_valid_until::%7$s, %2$s
                FROM temporal_merge_plan p,
                     LATERAL jsonb_populate_record(null::%1$s, p.data) AS jpr_data,
                     LATERAL jsonb_populate_record(null::%1$s, p.entity_ids) AS jpr_entity
                WHERE p.operation = 'UPDATE' AND %3$s AND t.%4$I = p.old_valid_from::%6$s;
            $$, v_target_table_ident, v_update_set_clause, v_entity_key_join_clause, v_valid_from_col, v_valid_until_col, v_valid_from_col_type, v_valid_until_col_type);
        ELSIF v_all_cols_ident IS NOT NULL THEN
            EXECUTE format($$ UPDATE %1$s t SET %3$I = p.new_valid_from::%5$s, %4$I = p.new_valid_until::%6$s
                FROM temporal_merge_plan p, LATERAL jsonb_populate_record(null::%1$s, p.entity_ids) AS jpr_entity
                WHERE p.operation = 'UPDATE' AND %2$s AND t.%3$I = p.old_valid_from::%5$s;
            $$, v_target_table_ident, v_entity_key_join_clause, v_valid_from_col, v_valid_until_col, v_valid_from_col_type, v_valid_until_col_type);
        END IF;

        -- 3. Execute DELETE operations
        EXECUTE format($$ DELETE FROM %1$s t
            USING temporal_merge_plan p, LATERAL jsonb_populate_record(null::%1$s, p.entity_ids) AS jpr_entity
            WHERE p.operation = 'DELETE' AND %2$s AND t.%3$I = p.old_valid_from::%4$s;
        $$, v_target_table_ident, v_entity_key_join_clause, v_valid_from_col, v_valid_from_col_type);

        SET CONSTRAINTS ALL IMMEDIATE;

        -- 4. Generate and store feedback
        INSERT INTO temporal_merge_feedback
            SELECT
                s.source_row_id AS source_row_id,
                COALESCE(jsonb_agg(DISTINCT (p_unnested.entity_ids - v_internal_keys_to_remove)) FILTER (WHERE p_unnested.entity_ids IS NOT NULL AND p_unnested.operation <> 'IDENTICAL'), '[]'::jsonb) AS target_entity_ids,
                CASE
                    -- APPLIED: at least one non-IDENTICAL operation
                    WHEN bool_or(p_unnested.operation <> 'IDENTICAL') THEN 'APPLIED'::sql_saga.temporal_merge_status
                    -- TARGET_NOT_FOUND: no operations, and in a mode that requires a target
                    WHEN count(p_unnested.operation) = 0 AND p_mode IN ('PATCH_FOR_PORTION_OF', 'REPLACE_FOR_PORTION_OF', 'DELETE_FOR_PORTION_OF') THEN 'TARGET_NOT_FOUND'::sql_saga.temporal_merge_status
                    -- SKIPPED: all other cases (all IDENTICAL ops, or no ops for insert_only/merge_entity_*)
                    ELSE 'SKIPPED'::sql_saga.temporal_merge_status
                END AS status,
                NULL::TEXT AS error_message
            FROM
                (SELECT unnest(v_all_source_row_ids) as source_row_id) s
            LEFT JOIN (
                SELECT unnest(p.source_row_ids) as source_row_id, p.plan_op_seq, p.entity_ids, p.operation
                FROM temporal_merge_plan p
            ) p_unnested ON s.source_row_id = p_unnested.source_row_id
            GROUP BY
                s.source_row_id
            ORDER BY
                s.source_row_id;

END;
$temporal_merge$;

COMMENT ON PROCEDURE sql_saga.temporal_merge(regclass, regclass, TEXT[], TEXT[], sql_saga.temporal_merge_mode, name, name, name, boolean, sql_saga.temporal_merge_delete_mode) IS
'Orchestrates a set-based temporal merge operation. It generates a plan using temporal_merge_plan and then executes it.';

