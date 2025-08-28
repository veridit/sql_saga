CREATE OR REPLACE FUNCTION sql_saga.allen_get_relation(
    x_from anycompatible, x_until anycompatible,
    y_from anycompatible, y_until anycompatible
) RETURNS sql_saga.allen_interval_relation
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $$
BEGIN
    IF x_from IS NULL OR x_until IS NULL OR y_from IS NULL OR y_until IS NULL THEN
        RETURN NULL;
    END IF;

    IF x_until < y_from THEN RETURN 'precedes'; END IF;
    IF x_until = y_from THEN RETURN 'meets'; END IF;
    IF x_from < y_from AND y_from < x_until AND x_until < y_until THEN RETURN 'overlaps'; END IF;
    IF x_from = y_from AND x_until < y_until THEN RETURN 'starts'; END IF;
    IF x_from > y_from AND x_until < y_until THEN RETURN 'during'; END IF;
    IF x_from > y_from AND x_until = y_until THEN RETURN 'finishes'; END IF;
    IF x_from = y_from AND x_until = y_until THEN RETURN 'equals'; END IF;

    -- Inverse relations
    IF y_until < x_from THEN RETURN 'preceded by'; END IF;
    IF y_until = x_from THEN RETURN 'met by'; END IF;
    IF y_from < x_from AND x_from < y_until AND y_until < x_until THEN RETURN 'overlapped by'; END IF;
    IF x_from = y_from AND x_until > y_until THEN RETURN 'started by'; END IF;
    IF x_from < y_from AND x_until > y_until THEN RETURN 'contains'; END IF;
    IF x_from < y_from AND x_until = y_until THEN RETURN 'finished by'; END IF;

    RETURN NULL; -- Should be unreachable
END;
$$;


-- Unified Planning Function
CREATE OR REPLACE FUNCTION sql_saga.temporal_merge_plan(
    p_target_table regclass,
    p_source_table regclass,
    p_id_columns TEXT[],
    p_ephemeral_columns TEXT[],
    p_insert_defaulted_columns TEXT[],
    p_mode sql_saga.temporal_merge_mode,
    p_era_name name
) RETURNS SETOF sql_saga.temporal_plan_op
LANGUAGE plpgsql STABLE AS $temporal_merge_plan$
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
    v_target_schema_name name;
    v_target_table_name_only name;
    v_valid_from_col name;
    v_valid_until_col name;
BEGIN
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
        format('jsonb_build_object(%s)', string_agg(format('%L, t.%I', col, col), ', '))
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
        WHERE s.attname NOT IN ('row_id', v_valid_from_col, v_valid_until_col, 'era_id', 'era_name')
          AND s.attname <> ALL(p_id_columns)
    )
    SELECT
        format('jsonb_build_object(%s)', string_agg(format('%L, t.%I', attname, attname), ', '))
    INTO
        v_source_data_cols_jsonb_build -- Re-use this variable for the common expression
    FROM
        common_data_cols;

    v_target_data_cols_jsonb_build := v_source_data_cols_jsonb_build; -- Both source and target use the same payload structure
    v_source_data_cols_jsonb_build := COALESCE(v_source_data_cols_jsonb_build, '''{}''::jsonb');
    v_target_data_cols_jsonb_build := COALESCE(v_target_data_cols_jsonb_build, '''{}''::jsonb');

    -- Construct an expression to reliably check if all entity_id columns for a source row are NULL.
    SELECT
        string_agg(format('t.%I IS NULL', col), ' AND ')
    INTO
        v_entity_id_check_is_null_expr
    FROM unnest(p_id_columns) AS col;

    -- 2. Construct expressions and resolver CTEs based on the mode.
    IF p_mode IN ('upsert_patch', 'patch_only') THEN
        -- In 'patch' modes, data is merged and NULLs from the source are ignored.
        -- Historical data from preceding time slices is inherited into gaps.
        v_source_data_payload_expr := format('jsonb_strip_nulls(%s)', v_source_data_cols_jsonb_build);
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

    ELSE -- upsert_replace, replace_only
        -- In 'replace' modes, the source data payload overwrites the target. NULLs are meaningful.
        v_source_data_payload_expr := v_source_data_cols_jsonb_build;
        v_resolver_ctes := '';
        -- In `replace` mode, we prioritize the source data (`s_data_payload`). If there is no
        -- source data for a given time segment, we preserve the target data (`t_data_payload`).
        -- This correctly implements the "Temporal Patch" semantic.
        v_final_data_payload_expr := $$COALESCE(s_data_payload, t_data_payload)$$;
        v_resolver_from := 'resolved_atomic_segments_with_payloads';
    END IF;


    -- 3. Conditionally define planner logic based on mode
    IF p_mode IN ('upsert_patch', 'patch_only') THEN
        -- For `patch` mode, we use a complex join condition to find "remainder" slices of
        -- the original timeline and generate UPDATEs for them, avoiding DELETES.
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
        -- This expression ensures the source_row_ids from the causal source row are attributed
        -- to the "remainder" slice, which would otherwise have NULL for its source_row_ids.
        v_plan_source_row_ids_expr := $$COALESCE(
            d.f_source_row_ids,
            MAX(d.f_source_row_ids) OVER (PARTITION BY d.entity_id, d.t_from)
        )$$;
    ELSE -- replace modes
        -- For `replace` mode, a simple join is correct. This results in a robust
        -- DELETE and INSERT plan for any timeline splits.
        v_diff_join_condition := 'f.f_from = t.t_from';
        v_plan_source_row_ids_expr := 'd.f_source_row_ids';
    END IF;

    -- 4. Construct and execute the main query to generate the execution plan.
    v_sql := format($SQL$
WITH
source_initial AS (
    SELECT
        t.row_id,
        %1$s as entity_id,
        t.%14$I as valid_from,
        t.%15$I as valid_until,
        %2$s AS data_payload,
        %13$s as is_new_entity
    FROM %3$s t
    WHERE t.%14$I < t.%15$I
),
target_rows AS (
    SELECT
        %1$s as entity_id,
        t.%14$I as valid_from,
        t.%15$I as valid_until,
        %6$s AS data_payload
    FROM %4$s t
    WHERE (%1$s) IN (SELECT DISTINCT entity_id FROM source_initial)
),
source_rows AS (
    -- Filter the initial source rows based on the operation mode.
    SELECT
        si.row_id as source_row_id,
        -- If it's a new entity, synthesize a temporary unique ID by embedding the source_row_id,
        -- so the planner can distinguish it from other new entities in the same batch.
        CASE
            WHEN si.is_new_entity
            THEN si.entity_id || jsonb_build_object('_sql_saga_source_row_id_', si.row_id)
            ELSE si.entity_id
        END as entity_id,
        si.valid_from,
        si.valid_until,
        si.data_payload
    FROM source_initial si
    WHERE CASE %7$L::sql_saga.temporal_merge_mode
        -- For upsert modes, include all initial source rows.
        WHEN 'upsert_patch' THEN true
        WHEN 'upsert_replace' THEN true
        -- For _only modes, only include rows for entities that already exist in the target.
        WHEN 'patch_only' THEN si.entity_id IN (SELECT tr.entity_id FROM target_rows tr)
        WHEN 'replace_only' THEN si.entity_id IN (SELECT tr.entity_id FROM target_rows tr)
        -- For insert_only, only include rows for entities that DO NOT exist in the target.
        WHEN 'insert_only' THEN si.entity_id NOT IN (SELECT tr.entity_id FROM target_rows tr)
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
                  daterange(sr.valid_from, sr.valid_until) && daterange(seg.valid_from, seg.valid_until)
                  OR (
                      daterange(sr.valid_from, sr.valid_until) -|- daterange(seg.valid_from, seg.valid_until)
                      AND EXISTS (
                          SELECT 1 FROM target_rows tr
                          WHERE tr.entity_id = sr.entity_id
                            AND daterange(sr.valid_from, sr.valid_until) && daterange(tr.valid_from, tr.valid_until)
                      )
                  )
              )
            ORDER BY sr.source_row_id LIMIT 1
        ) as source_row_id,
        s.data_payload as s_data_payload,
        t.data_payload as t_data_payload
    FROM atomic_segments seg
    LEFT JOIN LATERAL (
        SELECT tr.data_payload, tr.valid_from as t_valid_from
        FROM target_rows tr
        WHERE tr.entity_id = seg.entity_id
          AND daterange(seg.valid_from, seg.valid_until) <@ daterange(tr.valid_from, tr.valid_until)
    ) t ON true
    LEFT JOIN LATERAL (
        SELECT sr.data_payload
        FROM source_rows sr
        WHERE sr.entity_id = seg.entity_id
          AND daterange(seg.valid_from, seg.valid_until) <@ daterange(sr.valid_from, sr.valid_until)
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
            WHEN d.f_data IS NULL THEN 'DELETE'::sql_saga.internal_plan_operation_type
            WHEN d.t_data IS NULL THEN 'INSERT'::sql_saga.internal_plan_operation_type
            ELSE 'UPDATE'::sql_saga.internal_plan_operation_type
        END as operation,
        d.entity_id,
        d.t_from as old_valid_from,
        d.f_from as new_valid_from,
        d.f_until as new_valid_until,
        d.f_data as data,
        d.relation
    FROM diff d
    WHERE d.f_data IS DISTINCT FROM d.t_data
       OR d.f_from IS DISTINCT FROM d.t_from
       OR d.f_until IS DISTINCT FROM d.t_until
)
SELECT
    row_number() OVER (ORDER BY (p.source_row_ids[1]), operation DESC, entity_id, COALESCE(new_valid_from, old_valid_from))::BIGINT as plan_op_seq,
    p.source_row_ids,
    p.operation::text::sql_saga.plan_operation_type,
    p.entity_id - '_sql_saga_source_row_id_' AS entity_ids, -- Remove the temporary planner ID before returning
    p.old_valid_from,
    p.new_valid_from,
    p.new_valid_until,
    p.data,
    p.relation
FROM plan p
WHERE p.operation::text <> 'NOOP'
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
        v_valid_until_col               -- 15
    );

    RETURN QUERY EXECUTE v_sql;
END;
$temporal_merge_plan$;


-- Unified Orchestrator Function
CREATE OR REPLACE FUNCTION sql_saga.temporal_merge(
    p_target_table regclass,
    p_source_table regclass,
    p_id_columns TEXT[],
    p_ephemeral_columns TEXT[],
    p_insert_defaulted_columns TEXT[] DEFAULT '{}',
    p_mode sql_saga.temporal_merge_mode DEFAULT 'upsert_patch',
    p_era_name name DEFAULT 'valid'
)
RETURNS SETOF sql_saga.temporal_merge_result
LANGUAGE plpgsql VOLATILE AS $temporal_merge$
DECLARE
    v_target_table_ident TEXT := p_target_table::TEXT;
    v_data_cols_ident TEXT;
    v_data_cols_select TEXT;
    v_update_set_clause TEXT;
    v_all_cols_ident TEXT;
    v_all_cols_select TEXT;
    v_entity_key_join_clause TEXT;
    v_all_source_row_ids INTEGER[];
    v_feedback sql_saga.temporal_merge_result;
    v_target_schema_name name;
    v_target_table_name_only name;
    v_valid_from_col name;
    v_valid_until_col name;
BEGIN
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

    EXECUTE format('SELECT array_agg(row_id) FROM %s', p_source_table::TEXT) INTO v_all_source_row_ids;

        -- Dynamically construct join clause for composite entity key.
        SELECT
            string_agg(format('t.%I = jpr_entity.%I', col, col), ' AND ')
        INTO
            v_entity_key_join_clause
        FROM unnest(p_id_columns) AS col;

        IF to_regclass('pg_temp.__temp_last_sql_saga_temporal_merge_plan') IS NOT NULL THEN
            DROP TABLE __temp_last_sql_saga_temporal_merge_plan;
        END IF;
        CREATE TEMP TABLE __temp_last_sql_saga_temporal_merge_plan (LIKE sql_saga.temporal_plan_op) ON COMMIT DROP;
        ALTER TABLE __temp_last_sql_saga_temporal_merge_plan ADD PRIMARY KEY (plan_op_seq);

        INSERT INTO __temp_last_sql_saga_temporal_merge_plan
        SELECT * FROM sql_saga.temporal_merge_plan(
            p_target_table             => p_target_table,
            p_source_table             => p_source_table,
            p_id_columns               => p_id_columns,
            p_ephemeral_columns        => p_ephemeral_columns,
            p_insert_defaulted_columns => p_insert_defaulted_columns,
            p_mode                     => p_mode,
            p_era_name                 => p_era_name
        );

        -- Get dynamic column lists for DML, mimicking the planner's introspection logic for consistency.
        WITH source_cols AS (
            SELECT pa.attname
            FROM pg_catalog.pg_attribute pa
            WHERE pa.attrelid = p_source_table AND pa.attnum > 0 AND NOT pa.attisdropped
        ),
        target_cols AS (
            SELECT pa.attname
            FROM pg_catalog.pg_attribute pa
            WHERE pa.attrelid = p_target_table AND pa.attnum > 0 AND NOT pa.attisdropped
        ),
        common_data_cols AS (
            SELECT s.attname
            FROM source_cols s JOIN target_cols t ON s.attname = t.attname
            WHERE s.attname NOT IN ('row_id', v_valid_from_col, v_valid_until_col, 'era_id', 'era_name')
              AND s.attname <> ALL(p_id_columns)
        ),
        all_available_cols AS (
            SELECT attname FROM common_data_cols
            UNION ALL
            SELECT unnest(p_id_columns)
        )
        SELECT
            string_agg(format('%I', attname), ', ') FILTER (WHERE attname <> ALL(p_id_columns)),
            string_agg(format('jpr_data.%I', attname), ', ') FILTER (WHERE attname <> ALL(p_id_columns)),
            string_agg(format('%I = jpr_data.%I', attname, attname), ', ') FILTER (WHERE attname <> ALL(p_id_columns)),
            string_agg(format('%I', attname), ', ') FILTER (WHERE attname <> ALL(p_insert_defaulted_columns)),
            string_agg(format('jpr_all.%I', attname), ', ') FILTER (WHERE attname <> ALL(p_insert_defaulted_columns))
        INTO
            v_data_cols_ident,
            v_data_cols_select,
            v_update_set_clause,
            v_all_cols_ident,
            v_all_cols_select
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

                EXECUTE format($$
                    WITH
                    inserts_with_rn AS (
                        SELECT
                            p.plan_op_seq,
                            p.new_valid_from,
                            p.new_valid_until,
                            p.entity_ids || p.data as full_data,
                            row_number() OVER (ORDER BY p.plan_op_seq) as rn
                        FROM __temp_last_sql_saga_temporal_merge_plan p
                        WHERE p.operation = 'INSERT'
                    ),
                    inserted_rows AS (
                        INSERT INTO %1$s (%2$s, %5$I, %6$I)
                        SELECT %3$s, i.new_valid_from, i.new_valid_until
                        FROM inserts_with_rn i, LATERAL jsonb_populate_record(null::%1$s, i.full_data) as jpr_all
                        ORDER BY i.rn
                        RETURNING *
                    ),
                    inserted_rows_with_rn AS (
                        SELECT *, row_number() OVER () as rn
                        FROM inserted_rows
                    )
                    UPDATE __temp_last_sql_saga_temporal_merge_plan p
                    SET entity_ids = %4$s
                    FROM inserts_with_rn i
                    JOIN inserted_rows_with_rn ir ON i.rn = ir.rn
                    WHERE p.plan_op_seq = i.plan_op_seq;
                $$,
                    v_target_table_ident,
                    v_all_cols_ident,
                    v_all_cols_select,
                    v_entity_id_update_jsonb_build,
                    v_valid_from_col,
                    v_valid_until_col
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

                EXECUTE format($$
                    WITH
                    inserts_with_rn AS (
                        SELECT
                            p.plan_op_seq,
                            p.new_valid_from,
                            p.new_valid_until,
                            row_number() OVER (ORDER BY p.plan_op_seq) as rn
                        FROM __temp_last_sql_saga_temporal_merge_plan p
                        WHERE p.operation = 'INSERT'
                    ),
                    inserted_rows AS (
                        INSERT INTO %1$s (%3$I, %4$I)
                        SELECT i.new_valid_from, i.new_valid_until
                        FROM inserts_with_rn i
                        ORDER BY i.rn
                        RETURNING *
                    ),
                    inserted_rows_with_rn AS (
                        SELECT *, row_number() OVER () as rn
                        FROM inserted_rows
                    )
                    UPDATE __temp_last_sql_saga_temporal_merge_plan p
                    SET entity_ids = %2$s
                    FROM inserts_with_rn i
                    JOIN inserted_rows_with_rn ir ON i.rn = ir.rn
                    WHERE p.plan_op_seq = i.plan_op_seq;
                $$,
                    v_target_table_ident,
                    v_entity_id_update_jsonb_build,
                    v_valid_from_col,
                    v_valid_until_col
                );
             END;
        END IF;

        -- 2. Execute UPDATE operations
        IF v_update_set_clause IS NOT NULL THEN
            EXECUTE format($$ UPDATE %1$s t SET %4$I = p.new_valid_from, %5$I = p.new_valid_until, %2$s
                FROM __temp_last_sql_saga_temporal_merge_plan p,
                     LATERAL jsonb_populate_record(null::%1$s, p.data) AS jpr_data,
                     LATERAL jsonb_populate_record(null::%1$s, p.entity_ids) AS jpr_entity
                WHERE p.operation = 'UPDATE' AND %3$s AND t.%4$I = p.old_valid_from;
            $$, v_target_table_ident, v_update_set_clause, v_entity_key_join_clause, v_valid_from_col, v_valid_until_col);
        ELSE
            EXECUTE format($$ UPDATE %1$s t SET %3$I = p.new_valid_from, %4$I = p.new_valid_until
                FROM __temp_last_sql_saga_temporal_merge_plan p, LATERAL jsonb_populate_record(null::%1$s, p.entity_ids) AS jpr_entity
                WHERE p.operation = 'UPDATE' AND %2$s AND t.%3$I = p.old_valid_from;
            $$, v_target_table_ident, v_entity_key_join_clause, v_valid_from_col, v_valid_until_col);
        END IF;

        -- 3. Execute DELETE operations
        EXECUTE format($$ DELETE FROM %1$s t
            USING __temp_last_sql_saga_temporal_merge_plan p, LATERAL jsonb_populate_record(null::%1$s, p.entity_ids) AS jpr_entity
            WHERE p.operation = 'DELETE' AND %2$s AND t.%3$I = p.old_valid_from;
        $$, v_target_table_ident, v_entity_key_join_clause, v_valid_from_col);

        SET CONSTRAINTS ALL IMMEDIATE;

        -- 4. Generate and return feedback
        RETURN QUERY
            SELECT
                s.row_id AS source_row_id,
                COALESCE(jsonb_agg(DISTINCT p_unnested.entity_ids) FILTER (WHERE p_unnested.entity_ids IS NOT NULL), '[]'::jsonb) AS target_entity_ids,
                CASE
                    WHEN bool_and(p_unnested.plan_op_seq IS NOT NULL) THEN 'SUCCESS'::sql_saga.set_result_status
                    ELSE 'MISSING_TARGET'::sql_saga.set_result_status
                END AS status,
                NULL::TEXT AS error_message
            FROM
                (SELECT unnest(v_all_source_row_ids) as row_id) s
            LEFT JOIN (
                SELECT unnest(p.source_row_ids) as source_row_id, p.plan_op_seq, p.entity_ids
                FROM __temp_last_sql_saga_temporal_merge_plan p
            ) p_unnested ON s.row_id = p_unnested.source_row_id
            GROUP BY
                s.row_id
            ORDER BY
                s.row_id;

END;
$temporal_merge$;

COMMENT ON FUNCTION sql_saga.temporal_merge IS
'Orchestrates a set-based temporal merge operation. It generates a plan using temporal_merge_plan and then executes it.';

