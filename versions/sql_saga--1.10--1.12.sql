\echo Use "ALTER EXTENSION sql_saga UPDATE TO '1.12'" to load this file. \quit
CREATE OR REPLACE FUNCTION sql_saga.native_library_version() RETURNS text LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT AS 'sql_saga_native', 'sql_saga_native_version_wrapper';
DO $native_guard_1_12$
BEGIN
    IF sql_saga.native_library_version() <> '1.12' THEN
        RAISE EXCEPTION 'sql_saga: native library version % does not match extension update version 1.12', sql_saga.native_library_version()
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Install the sql_saga 1.12 native library and restart PostgreSQL before retrying ALTER EXTENSION.';
    END IF;
END;
$native_guard_1_12$;
SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('sql_saga 1.12 upgrade'));

-- Executor for temporal_merge. This procedure contains the DML logic.
CREATE OR REPLACE PROCEDURE sql_saga.temporal_merge_execute(
    target_table regclass,
    source_table regclass,
    identity_columns TEXT[] DEFAULT NULL,
    mode sql_saga.temporal_merge_mode DEFAULT 'MERGE_ENTITY_PATCH',
    era_name name DEFAULT 'valid',
    row_id_column name DEFAULT 'row_id',
    founding_id_column name DEFAULT NULL,
    update_source_with_identity BOOLEAN DEFAULT false,
    lookup_columns TEXT[] DEFAULT NULL,
    delete_mode sql_saga.temporal_merge_delete_mode DEFAULT 'NONE',
    update_source_with_feedback BOOLEAN DEFAULT false,
    feedback_status_column name DEFAULT NULL,
    feedback_status_key name DEFAULT NULL,
    feedback_error_column name DEFAULT NULL,
    feedback_error_key name DEFAULT NULL,
    ephemeral_columns TEXT[] DEFAULT NULL,
    delay_constraints BOOLEAN DEFAULT true
)
LANGUAGE plpgsql AS $temporal_merge_execute$
DECLARE
    v_lookup_columns TEXT[];
    v_target_table_ident TEXT := temporal_merge_execute.target_table::TEXT;
    v_update_set_clause TEXT;
    v_all_cols_ident TEXT;
    v_all_cols_select TEXT;
    v_entity_key_join_clause TEXT;
    v_entity_key_select_list TEXT;
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
    v_log_feedback boolean;
    v_log_vars boolean;
    v_log_execute boolean;
    v_expected_idx_expr_with_bounds text;
    v_expected_idx_expr_default text;
    v_idx_rec record;
    v_source_rel_oid oid;
    v_source_rel_name_for_hint regclass;
    v_range_subtype regtype;
    v_valid_from_col_type regtype;
    v_valid_until_col_type regtype;
    v_insert_defaulted_columns TEXT[];
    v_founding_defaulted_columns TEXT[];
    v_all_cols_from_jsonb TEXT;
    v_founding_all_cols_ident TEXT;
    v_founding_all_cols_from_jsonb TEXT;
    v_pk_cols name[];
    v_feedback_set_clause TEXT;
    v_sql TEXT;
    v_causal_col name;
    v_log_id TEXT;
    v_summary_line TEXT;
    v_not_null_defaulted_cols TEXT[];
    v_update_statement_seqs INT[];
    v_update_statement_seq INT;
    v_source_col_names TEXT[];
    v_cache sql_saga.temporal_merge_executor_cache;
    v_use_native_executor boolean;
    v_log_step_timing boolean;
    v_t0 timestamptz;
    v_t1 timestamptz;
    v_t2 timestamptz;
    v_t3 timestamptz;
    v_t_delete timestamptz;
    v_t_update timestamptz;
    v_t_insert timestamptz;
    v_t_done timestamptz;
    v_partition_join_clauses TEXT[];
    v_partition_plan_filters TEXT[];
    v_partition_select_lists TEXT[];
    v_partition_count INT;
    v_part INT;
BEGIN
    v_log_trace := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.enable_trace', true), ''), 'false')::boolean;
    v_log_sql := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_sql', true), ''), 'false')::boolean;
    v_log_index_checks := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_index_checks', true), ''), 'false')::boolean;
    v_log_feedback := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_feedback', true), ''), 'false')::boolean;
    v_log_vars := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_vars', true), ''), 'false')::boolean;
    v_log_execute := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_execute', true), ''), 'false')::boolean;
    v_log_step_timing := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_step_timing', true), ''), 'false')::boolean;
    v_log_id := substr(md5(COALESCE(current_setting('sql_saga.temporal_merge.log_id_seed', true), random()::text)), 1, 3);
    IF v_log_step_timing THEN v_t0 := clock_timestamp(); END IF;

    -- Identity and lookup columns are assumed to have been discovered and validated by the main temporal_merge procedure.
    v_lookup_columns := COALESCE(temporal_merge_execute.lookup_columns, temporal_merge_execute.identity_columns);
    v_causal_col := COALESCE(temporal_merge_execute.founding_id_column, temporal_merge_execute.row_id_column);

    -- Use the native executor cache if the shared library is available.
    v_use_native_executor := to_regproc('sql_saga.temporal_merge_executor_introspect') IS NOT NULL;

    IF v_use_native_executor THEN
        -- Single function call replaces ~570 lines of introspection + CTE logic.
        -- On cache hit, returns immediately with zero SPI calls.
        SELECT * INTO v_cache FROM sql_saga.temporal_merge_executor_introspect(
            target_table => temporal_merge_execute.target_table::oid,
            source_table => temporal_merge_execute.source_table::oid,
            identity_columns => temporal_merge_execute.identity_columns,
            lookup_columns => v_lookup_columns,
            era_name => temporal_merge_execute.era_name,
            ephemeral_columns => temporal_merge_execute.ephemeral_columns,
            founding_id_column => temporal_merge_execute.founding_id_column,
            row_id_column => temporal_merge_execute.row_id_column
        );

        -- Unpack cached values into local variables
        v_range_col := v_cache.range_col;
        v_range_constructor := v_cache.range_constructor;
        v_range_subtype := v_cache.range_subtype::regtype;
        v_valid_from_col := v_cache.valid_from_col;
        v_valid_until_col := v_cache.valid_until_col;
        v_valid_to_col := v_cache.valid_to_col;
        v_valid_from_col_type := v_cache.valid_from_col_type::regtype;
        v_valid_until_col_type := v_cache.valid_until_col_type::regtype;
        v_pk_cols := v_cache.pk_cols::name[];
        v_not_null_defaulted_cols := v_cache.not_null_defaulted_cols;
        v_insert_defaulted_columns := v_cache.insert_defaulted_columns;
        v_founding_defaulted_columns := v_cache.founding_defaulted_columns;
        v_source_col_names := v_cache.source_col_names;
        v_update_set_clause := v_cache.update_set_clause;
        v_all_cols_ident := v_cache.all_cols_ident;
        v_all_cols_select := v_cache.all_cols_select;
        v_all_cols_from_jsonb := v_cache.all_cols_from_jsonb;
        v_founding_all_cols_ident := v_cache.founding_all_cols_ident;
        v_founding_all_cols_from_jsonb := v_cache.founding_all_cols_from_jsonb;
        v_entity_key_join_clause := v_cache.entity_key_join_clause;
        v_entity_key_select_list := v_cache.entity_key_select_list;
        v_partition_join_clauses := v_cache.partition_join_clauses;
        v_partition_plan_filters := v_cache.partition_plan_filters;
        v_partition_select_lists := v_cache.partition_select_lists;

        -- Still need schema name for constraint deferral later
        SELECT n.nspname INTO v_target_schema_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.oid = temporal_merge_execute.target_table;

    ELSE
        -- Fallback: PL/pgSQL introspection (original code path)

    -- Introspect source columns to determine which columns are actually provided.
    -- This allows us to handle partial source views correctly, especially for defaulted columns.
    SELECT COALESCE(array_agg(attname::text), '{}')
    INTO v_source_col_names
    FROM pg_attribute
    WHERE attrelid = temporal_merge_execute.source_table
      AND attnum > 0
      AND NOT attisdropped;

    -- Introspect columns that are NOT NULL and have a DEFAULT. For these columns,
    -- an incoming NULL in UPSERT mode should preserve the existing value rather
    -- than cause a NOT NULL violation.
    SELECT COALESCE(array_agg(a.attname), '{}')
    INTO v_not_null_defaulted_cols
    FROM pg_catalog.pg_attribute a
    WHERE a.attrelid = temporal_merge_execute.target_table
      AND a.attnum > 0 AND NOT a.attisdropped AND a.atthasdef AND a.attnotnull;

    -- Introspect era information to get the correct column names
    SELECT n.nspname, c.relname
    INTO v_target_schema_name, v_target_table_name_only
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = temporal_merge_execute.target_table;

    SELECT e.range_column_name, e.valid_from_column_name, e.valid_until_column_name, e.valid_to_column_name, e.range_type::name, e.range_subtype
    INTO v_range_col, v_valid_from_col, v_valid_until_col, v_valid_to_col, v_range_constructor, v_range_subtype
    FROM sql_saga.era e
    WHERE e.table_schema = v_target_schema_name
      AND e.table_name = v_target_table_name_only
      AND e.era_name = temporal_merge_execute.era_name;

    IF v_range_col IS NULL THEN
        RAISE EXCEPTION 'No era named "%" found for table "%"', temporal_merge_execute.era_name, sql_saga.__internal_display_name(temporal_merge_execute.target_table);
    END IF;

    -- Introspect the primary key columns, excluding temporal columns.
    -- They will be excluded from UPDATE SET clauses.
    -- Must happen after temporal columns are discovered.
    SELECT COALESCE(array_agg(a.attname ORDER BY array_position(c.conkey, a.attnum)), '{}'::name[])
    INTO v_pk_cols
    FROM pg_constraint c
    JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
    WHERE c.conrelid = temporal_merge_execute.target_table
      AND c.contype = 'p'
      AND a.attname NOT IN (v_range_col, v_valid_from_col, v_valid_until_col);

    END IF; -- v_use_native_executor (introspection phase 1)

    v_summary_line := format(
        'on %s: mode=>%s, delete_mode=>%s, identity_columns=>%L, lookup_columns=>%L, ephemeral_columns=>%L, founding_id_column=>%L, row_id_column=>%L',
        temporal_merge_execute.target_table,
        temporal_merge_execute.mode,
        temporal_merge_execute.delete_mode,
        temporal_merge_execute.identity_columns,
        temporal_merge_execute.lookup_columns,
        temporal_merge_execute.ephemeral_columns,
        temporal_merge_execute.founding_id_column,
        temporal_merge_execute.row_id_column
    );

    -- To ensure idempotency and avoid permission errors in complex transactions
    -- with role changes, we drop and recreate the feedback table for each call.
    IF to_regclass('pg_temp.temporal_merge_feedback') IS NOT NULL THEN
        DROP TABLE pg_temp.temporal_merge_feedback;
    END IF;
    CREATE TEMP TABLE temporal_merge_feedback (LIKE sql_saga.temporal_merge_feedback) ON COMMIT DROP;

    -- Create a unified, session-local cache for index checks to avoid redundant lookups.
    IF to_regclass('pg_temp.temporal_merge_index_cache') IS NULL THEN
        CREATE TEMP TABLE temporal_merge_index_cache (
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
        CREATE UNIQUE INDEX ON temporal_merge_index_cache (rel_oid) WHERE lookup_columns IS NULL;
        -- Unique index for BTREE checks
        CREATE UNIQUE INDEX ON temporal_merge_index_cache (rel_oid, lookup_columns) WHERE lookup_columns IS NOT NULL;
    END IF;

    IF v_log_step_timing THEN v_t1 := clock_timestamp(); END IF;

    -- Prepare expected normalized index expressions and logging flag for index checks
    IF v_valid_from_col IS NOT NULL AND v_valid_until_col IS NOT NULL THEN
        v_expected_idx_expr_with_bounds := format('%s(%s,%s,''[)'')', v_range_constructor, v_valid_from_col, v_valid_until_col);
        v_expected_idx_expr_default := format('%s(%s,%s)', v_range_constructor, v_valid_from_col, v_valid_until_col);
    ELSE
        v_expected_idx_expr_with_bounds := NULL;
        v_expected_idx_expr_default := NULL;
    END IF;

    -- Check cache for original source relation OID. This provides a fast path for repeated calls with the same view.
    SELECT has_index, hint_rel_name
    INTO v_has_gist_index, v_source_rel_name_for_hint
    FROM pg_temp.temporal_merge_index_cache AS tmc
    WHERE rel_oid = temporal_merge_execute.source_table AND tmc.lookup_columns IS NULL;

    IF NOT FOUND THEN
        -- On cache miss, resolve the relation if it's a view, perform the check, and populate the cache.
        DECLARE
            v_source_relkind char;
            v_is_view boolean;
        BEGIN
            SELECT c.relkind INTO v_source_relkind
            FROM pg_class c WHERE c.oid = temporal_merge_execute.source_table;
            v_is_view := (v_source_relkind = 'v');

            IF v_is_view THEN
                -- It's a view, find the underlying table. Assumes a simple view with one base table.
                SELECT d.refobjid INTO v_source_rel_oid
                FROM pg_rewrite r
                JOIN pg_depend d ON r.oid = d.objid
                JOIN pg_class c_dep ON c_dep.oid = d.refobjid
                WHERE r.ev_class = temporal_merge_execute.source_table
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
                RAISE WARNING 'Could not determine the base table for source relation "%" (it may be a complex view). Skipping GIST index performance check.', sql_saga.__internal_display_name(temporal_merge_execute.source_table);
                v_has_gist_index := true;
                v_source_rel_name_for_hint := temporal_merge_execute.source_table; -- Fallback
            ELSE
                -- We have a resolvable base table. Check cache for it.
                SELECT has_index
                INTO v_has_gist_index
                FROM pg_temp.temporal_merge_index_cache AS tmc
                WHERE rel_oid = v_source_rel_oid AND tmc.lookup_columns IS NULL;

                IF NOT FOUND THEN
                    -- Still a cache miss, so perform the actual check on the base table.
                    SELECT EXISTS (
                        -- Check for an index on the range column itself
                        SELECT 1
                        FROM pg_index ix
                        JOIN pg_class i ON i.oid = ix.indexrelid
                        JOIN pg_am am ON am.oid = i.relam
                        JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = ANY(ix.indkey)
                        WHERE ix.indrelid = v_source_rel_oid
                          AND am.amname = 'gist'
                          AND ix.indkey IS NOT NULL AND array_length(ix.indkey, 1) = 1
                          AND a.attname = v_range_col
                        UNION ALL
                        -- Check for an expression-based index on from/until columns (legacy/synchronized)
                        SELECT 1
                        FROM pg_index ix
                        JOIN pg_class i ON i.oid = ix.indexrelid
                        JOIN pg_am am ON am.oid = i.relam
                        WHERE ix.indrelid = v_source_rel_oid
                          AND am.amname = 'gist'
                          AND ix.indexprs IS NOT NULL
                          AND v_expected_idx_expr_with_bounds IS NOT NULL
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
                    INSERT INTO pg_temp.temporal_merge_index_cache (rel_oid, lookup_columns, has_index, hint_rel_name)
                    VALUES (v_source_rel_oid, NULL, v_has_gist_index, v_source_rel_name_for_hint);
                END IF;
            END IF;

            -- Now, populate the cache for the original source relation OID (which could be the view).
            -- This is the key optimization for subsequent calls with the same view.
            -- We only need to do this if the original source was different from the resolved one (i.e., it was a view).
            IF temporal_merge_execute.source_table <> v_source_rel_oid THEN
                INSERT INTO pg_temp.temporal_merge_index_cache (rel_oid, lookup_columns, has_index, hint_rel_name)
                VALUES (temporal_merge_execute.source_table, NULL, v_has_gist_index, v_source_rel_name_for_hint);
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
                RAISE WARNING 'Performance warning: The source relation % lacks a GIST index on its temporal columns.', temporal_merge_execute.source_table
                USING HINT = format('For better performance, consider creating an index, e.g., CREATE INDEX ON %s USING GIST (%I);',
                    v_source_rel_name_for_hint,
                    v_range_col
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
        -- Check for an index on the range column itself
        SELECT 1
        FROM pg_index ix
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am ON am.oid = i.relam
        JOIN pg_attribute a ON a.attrelid = ix.indrelid AND a.attnum = ANY(ix.indkey)
        WHERE ix.indrelid = v_target_table_ident::regclass
          AND am.amname = 'gist'
          AND ix.indkey IS NOT NULL AND array_length(ix.indkey, 1) = 1
          AND a.attname = v_range_col
        UNION ALL
        -- Check for an expression-based index on from/until columns (legacy/synchronized)
        SELECT 1
        FROM pg_index ix
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_am am ON am.oid = i.relam
        WHERE ix.indrelid = v_target_table_ident::regclass
          AND am.amname = 'gist'
          AND ix.indexprs IS NOT NULL
          AND v_expected_idx_expr_with_bounds IS NOT NULL
          AND (
              regexp_replace(pg_get_expr(ix.indexprs, ix.indrelid), '\s|::\w+', '', 'g') = v_expected_idx_expr_with_bounds
              OR regexp_replace(pg_get_expr(ix.indexprs, ix.indrelid), '\s|::\w+', '', 'g') = v_expected_idx_expr_default
          )
    )
    INTO v_has_target_gist_index;

    IF v_log_index_checks THEN
        RAISE NOTICE 'Index check (TARGET %) expected: "%", or "%"', temporal_merge_execute.target_table, v_expected_idx_expr_with_bounds, v_expected_idx_expr_default;
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
            SELECT c.reltuples INTO v_target_row_count FROM pg_class c WHERE c.oid = temporal_merge_execute.target_table;
            IF v_target_row_count >= 512 THEN
                RAISE WARNING 'Performance warning: The target relation % lacks a GIST index on its temporal columns.', temporal_merge_execute.target_table
                USING HINT = format('For better performance, consider creating an index, e.g., CREATE INDEX ON %s USING GIST (%I);',
                    v_target_table_ident,
                    v_range_col
                );
            END IF;
        END;
    END IF;

    -- Check for performance-critical BTREE index on target table's lookup columns.
    -- This check is cached per transaction to avoid redundant lookups.
    SELECT has_index
    INTO v_has_lookup_btree_index
    FROM pg_temp.temporal_merge_index_cache AS tmc
    WHERE rel_oid = temporal_merge_execute.target_table
      AND tmc.lookup_columns = v_lookup_columns;

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

        INSERT INTO pg_temp.temporal_merge_index_cache (rel_oid, lookup_columns, has_index, hint_rel_name)
        VALUES (temporal_merge_execute.target_table, v_lookup_columns, v_has_lookup_btree_index, NULL);
    END IF;

    IF NOT v_has_lookup_btree_index THEN
        DECLARE
            v_target_row_count REAL;
        BEGIN
            SELECT c.reltuples INTO v_target_row_count FROM pg_class c WHERE c.oid = temporal_merge_execute.target_table;
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

    IF v_log_step_timing THEN v_t2 := clock_timestamp(); END IF;

    IF NOT v_use_native_executor THEN
    -- Fallback: PL/pgSQL introspection for defaulted columns, column types, entity keys, and CTE

    -- Auto-detect columns that should be excluded from INSERT statements.
    -- We only exclude columns that cannot accept values (GENERATED ALWAYS).
    -- Columns with DEFAULTs (atthasdef) and IDENTITY BY DEFAULT (attidentity='d') are INCLUDED
    -- so that we can preserve values from history (payload).
    -- For these columns, we use COALESCE(payload, default) in the INSERT generation to handle missing values.
    SELECT COALESCE(array_agg(a.attname), '{}')
    INTO v_insert_defaulted_columns
    FROM pg_catalog.pg_attribute a
    WHERE a.attrelid = temporal_merge_execute.target_table
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND (a.attidentity = 'a' OR a.attgenerated <> '');

    -- For founding INSERTs of new entities:
    -- - Exclude IDENTITY BY DEFAULT ('d') so the database generates new IDs (sequences).
    -- - Exclude GENERATED ALWAYS ('a') and GENERATED columns.
    -- - INCLUDE columns with standard DEFAULTs (atthasdef) so we can COALESCE(payload, default).
    --   This allows explicit values from source (if provided) or falls back to default.
    SELECT COALESCE(array_agg(a.attname), '{}')
    INTO v_founding_defaulted_columns
    FROM pg_catalog.pg_attribute a
    WHERE a.attrelid = temporal_merge_execute.target_table
      AND a.attnum > 0
      AND NOT a.attisdropped
      AND (a.attidentity IN ('a', 'd') OR a.attgenerated <> '');

    -- Also exclude synchronized columns, as the trigger will populate them.
    IF v_valid_from_col IS NOT NULL THEN
        v_insert_defaulted_columns := v_insert_defaulted_columns || v_valid_from_col;
    END IF;
    IF v_valid_until_col IS NOT NULL THEN
        v_insert_defaulted_columns := v_insert_defaulted_columns || v_valid_until_col;
    END IF;
    IF v_valid_to_col IS NOT NULL THEN
        v_insert_defaulted_columns := v_insert_defaulted_columns || v_valid_to_col;
    END IF;
    IF v_range_col IS NOT NULL THEN
        v_insert_defaulted_columns := v_insert_defaulted_columns || v_range_col;
    END IF;

    IF v_valid_from_col IS NOT NULL THEN
        SELECT atttypid::regtype INTO v_valid_from_col_type FROM pg_attribute WHERE attrelid = temporal_merge_execute.target_table AND attname = v_valid_from_col;
    ELSE
        -- For range-only tables, use the range subtype for casting
        v_valid_from_col_type := v_range_subtype;
    END IF;
    IF v_valid_until_col IS NOT NULL THEN
        SELECT atttypid::regtype INTO v_valid_until_col_type FROM pg_attribute WHERE attrelid = temporal_merge_execute.target_table AND attname = v_valid_until_col;
    ELSE
        -- For range-only tables, use the range subtype for casting
        v_valid_until_col_type := v_range_subtype;
    END IF;

    -- Dynamically construct join clause for composite entity key.
    -- PERFORMANCE OPTIMIZATION: Pre-extract JSONB keys in the subquery SELECT list
    -- so PostgreSQL can use them directly in Hash/Merge join conditions.
    -- Without this, JSONB extraction in the WHERE clause forces a cross-join
    -- with post-filtering, causing O(N*M) comparisons instead of O(N+M).
    --
    -- For NOT NULL columns: use = for Hash/Merge join performance.
    -- For nullable columns (XOR pattern like legal_unit_id/establishment_id):
    -- use COALESCE sentinel to enable NULL=NULL matching while preserving Hash Join.
    -- IS NOT DISTINCT FROM was tested on PG18 and caused performance issues.
    SELECT
        string_agg(
            format(
                '(entity_keys->>%1$L)::%2$s AS %3$I',
                col,
                format_type(a.atttypid, -1),
                col || '_ek'  -- Suffix to avoid collision with other plan columns
            ),
            ', ' ORDER BY ic.ord
        ),
        string_agg(
            CASE WHEN a.attnotnull THEN
                format(
                    't.%1$I = p.%2$I',
                    col,
                    col || '_ek'
                )
            ELSE
                format(
                    'COALESCE(t.%1$I, (-1)::%3$s) = COALESCE(p.%2$I, (-1)::%3$s)',
                    col,
                    col || '_ek',
                    format_type(a.atttypid, -1)
                )
            END,
            ' AND ' ORDER BY ic.ord
        )
    INTO
        v_entity_key_select_list,
        v_entity_key_join_clause
    FROM unnest(identity_columns) WITH ORDINALITY AS ic(col, ord)
    JOIN pg_attribute a ON a.attrelid = temporal_merge_execute.target_table AND a.attname = ic.col AND NOT a.attisdropped;

    v_entity_key_join_clause := COALESCE(v_entity_key_join_clause, 'true');
    v_entity_key_select_list := COALESCE(v_entity_key_select_list, '');

    -- Partition arrays: single partition for PL/pgSQL fallback (no COALESCE elimination)
    v_partition_join_clauses := ARRAY[v_entity_key_join_clause];
    v_partition_plan_filters := ARRAY['true'];
    v_partition_select_lists := ARRAY[v_entity_key_select_list];

        -- Get dynamic column lists for DML. The data columns are defined as all columns
        -- in the target table, minus the identity and temporal boundary columns.
        -- This is intentionally different from the planner's introspection, as the executor
        -- must be able to handle a final payload that contains columns inherited from
        -- the target's history, which may not be present in the source.
        WITH target_cols AS (
            SELECT pa.attname, pa.atttypid, pa.attgenerated, pa.attidentity, pa.attnum, pa.atthasdef, pa.attnotnull
            FROM pg_catalog.pg_attribute pa
            WHERE pa.attrelid = temporal_merge_execute.target_table AND pa.attnum > 0 AND NOT pa.attisdropped
        ),
        common_data_cols AS (
            SELECT t.attname, t.atttypid, t.attnotnull, t.atthasdef, t.attnum
            FROM target_cols t
            LEFT JOIN pg_attrdef ad ON ad.adrelid = temporal_merge_execute.target_table AND ad.adnum = t.attnum
            WHERE (v_range_col IS NULL OR t.attname <> v_range_col)
              AND (v_valid_from_col IS NULL OR t.attname <> v_valid_from_col)
              AND (v_valid_until_col IS NULL OR t.attname <> v_valid_until_col)
              AND (v_valid_to_col IS NULL OR t.attname <> v_valid_to_col)
              AND t.attname <> ALL(COALESCE(temporal_merge_execute.identity_columns, '{}'))
              AND t.attname <> ALL(v_lookup_columns)
              AND t.attname <> ALL(COALESCE(v_pk_cols, '{}'))
              AND t.attidentity <> 'a' -- Exclude GENERATED ALWAYS AS IDENTITY
              AND t.attgenerated = '' -- Exclude GENERATED ... STORED
              AND COALESCE(pg_get_expr(ad.adbin, temporal_merge_execute.target_table), '') NOT ILIKE 'nextval(%'
        ),
        all_available_cols AS (
            SELECT c.attname, c.atttypid, c.attnotnull, c.atthasdef, c.attnum FROM common_data_cols c
            UNION
            SELECT u.attname, t.atttypid, t.attnotnull, t.atthasdef, t.attnum
            FROM unnest(v_lookup_columns) u(attname)
            JOIN target_cols t ON u.attname = t.attname
            UNION
            SELECT u.attname, t.atttypid, t.attnotnull, t.atthasdef, t.attnum
            FROM unnest(COALESCE(temporal_merge_execute.identity_columns, '{}')) u(attname)
            JOIN target_cols t ON u.attname = t.attname
            UNION
            SELECT pk.attname, t.atttypid, t.attnotnull, t.atthasdef, t.attnum
            FROM unnest(v_pk_cols) pk(attname)
            JOIN target_cols t ON pk.attname = t.attname
        ),
        cols_for_insert AS (
            -- All available columns that DON'T have a default...
            SELECT attname, atttypid, attnotnull, atthasdef, attnum FROM all_available_cols WHERE attname <> ALL(v_insert_defaulted_columns)
            UNION
            -- ...plus all identity and lookup columns and primary key columns, which must be provided for SCD-2 inserts.
            SELECT attname, atttypid, attnotnull, atthasdef, attnum FROM all_available_cols WHERE attname = ANY(COALESCE(temporal_merge_execute.identity_columns, '{}')) OR attname = ANY(v_lookup_columns) OR attname = ANY(v_pk_cols)
        ),
        cols_for_founding_insert AS (
            -- For "founding" INSERTs of new entities, we only include columns that do NOT have a default.
            -- This allows serial/identity columns to be generated by the database.
            SELECT attname, atttypid, attnotnull, atthasdef, attnum
            FROM all_available_cols
            WHERE attname <> ALL(v_founding_defaulted_columns)
        )
        SELECT
            (SELECT string_agg(
                format(
                    '%1$I = CASE WHEN p.data ? %2$L THEN %4$s ELSE t.%1$I END',
                    cdc.attname,
                    cdc.attname,
                    format_type(cdc.atttypid, -1),
                    -- For NOT NULL columns with a DEFAULT, COALESCE with the existing value to prevent NULL overwrites.
                    CASE
                        WHEN cdc.attname = ANY(v_not_null_defaulted_cols)
                        THEN format('COALESCE(%s, t.%I)', CASE WHEN (SELECT b.typcategory IN ('A', 'C') OR b.oid IN ('json'::regtype, 'jsonb'::regtype) FROM pg_catalog.pg_type AS t JOIN pg_catalog.pg_type AS b ON b.oid = CASE WHEN t.typtype = 'd' THEN t.typbasetype ELSE t.oid END WHERE t.oid = cdc.atttypid) THEN format('(pg_catalog.jsonb_populate_record(NULL::%s, pg_catalog.jsonb_build_object(%L, p.data->%L))).%I', v_target_table_ident, cdc.attname, cdc.attname, cdc.attname) ELSE format('(p.data->>%L)::%s', cdc.attname, format_type(cdc.atttypid, -1)) END, cdc.attname)
                        ELSE CASE WHEN (SELECT b.typcategory IN ('A', 'C') OR b.oid IN ('json'::regtype, 'jsonb'::regtype) FROM pg_catalog.pg_type AS t JOIN pg_catalog.pg_type AS b ON b.oid = CASE WHEN t.typtype = 'd' THEN t.typbasetype ELSE t.oid END WHERE t.oid = cdc.atttypid) THEN format('(pg_catalog.jsonb_populate_record(NULL::%s, pg_catalog.jsonb_build_object(%L, p.data->%L))).%I', v_target_table_ident, cdc.attname, cdc.attname, cdc.attname) ELSE format('(p.data->>%L)::%s', cdc.attname, format_type(cdc.atttypid, -1)) END
                    END
                ),
            ', ') FROM common_data_cols cdc),
            (SELECT string_agg(format('%I', cfi.attname), ', ' ORDER BY cfi.attnum) FROM cols_for_insert cfi WHERE cfi.attname IS DISTINCT FROM v_valid_from_col AND cfi.attname IS DISTINCT FROM v_valid_until_col),
            -- For Step 1.3 (historical slices for new entities via jsonb_populate_record):
            -- NOT NULL DEFAULT columns need COALESCE because jsonb_populate_record returns NULL for missing keys.
            (SELECT string_agg(
                CASE
                    WHEN cfi.attnotnull AND cfi.atthasdef THEN
                        format('COALESCE(jpr_all.%1$I, %2$s)',
                            cfi.attname,
                            pg_get_expr(ad.adbin, ad.adrelid)
                        )
                    ELSE
                        format('jpr_all.%I', cfi.attname)
                END,
                ', ' ORDER BY cfi.attnum
             )
             FROM cols_for_insert cfi
             LEFT JOIN pg_attrdef ad ON ad.adrelid = temporal_merge_execute.target_table
                                    AND ad.adnum = cfi.attnum
             WHERE cfi.attname IS DISTINCT FROM v_valid_from_col AND cfi.attname IS DISTINCT FROM v_valid_until_col),
            (SELECT string_agg(
                -- For NOT NULL DEFAULT columns, we must coalesce missing payload values with the DB default expression.
                -- This allows us to include the column in the INSERT (for payload preservation) without risking NULL violation.
                CASE
                    WHEN cfi.attnotnull AND cfi.atthasdef THEN
                        format('COALESCE(%s, %s)',
                            CASE WHEN (SELECT b.typcategory IN ('A', 'C') OR b.oid IN ('json'::regtype, 'jsonb'::regtype) FROM pg_catalog.pg_type AS t JOIN pg_catalog.pg_type AS b ON b.oid = CASE WHEN t.typtype = 'd' THEN t.typbasetype ELSE t.oid END WHERE t.oid = cfi.atttypid) THEN format('(pg_catalog.jsonb_populate_record(NULL::%s, pg_catalog.jsonb_build_object(%L, s.full_data->%L))).%I', v_target_table_ident, cfi.attname, cfi.attname, cfi.attname) ELSE format('(s.full_data->>%L)::%s', cfi.attname, format_type(cfi.atttypid, -1)) END,
                            pg_get_expr(ad.adbin, ad.adrelid)
                        )
                    ELSE
                        CASE WHEN (SELECT b.typcategory IN ('A', 'C') OR b.oid IN ('json'::regtype, 'jsonb'::regtype) FROM pg_catalog.pg_type AS t JOIN pg_catalog.pg_type AS b ON b.oid = CASE WHEN t.typtype = 'd' THEN t.typbasetype ELSE t.oid END WHERE t.oid = cfi.atttypid) THEN format('(pg_catalog.jsonb_populate_record(NULL::%s, pg_catalog.jsonb_build_object(%L, s.full_data->%L))).%I', v_target_table_ident, cfi.attname, cfi.attname, cfi.attname) ELSE format('(s.full_data->>%L)::%s', cfi.attname, format_type(cfi.atttypid, -1)) END
                END,
                ', ' ORDER BY cfi.attnum
             )
             FROM cols_for_insert cfi
             LEFT JOIN pg_attrdef ad ON ad.adrelid = temporal_merge_execute.target_table
                                    AND ad.adnum = cfi.attnum
             WHERE cfi.attname IS DISTINCT FROM v_valid_from_col AND cfi.attname IS DISTINCT FROM v_valid_until_col),
            (SELECT string_agg(format('%I', cffi.attname), ', ' ORDER BY cffi.attnum) FROM cols_for_founding_insert cffi WHERE cffi.attname IS DISTINCT FROM v_valid_from_col AND cffi.attname IS DISTINCT FROM v_valid_until_col),
            (SELECT string_agg(
                -- Same logic for founding inserts: Coalesce with default if NOT NULL and has default.
                CASE
                    WHEN cffi.attnotnull AND cffi.atthasdef THEN
                        format('COALESCE(%s, %s)',
                            CASE WHEN (SELECT b.typcategory IN ('A', 'C') OR b.oid IN ('json'::regtype, 'jsonb'::regtype) FROM pg_catalog.pg_type AS t JOIN pg_catalog.pg_type AS b ON b.oid = CASE WHEN t.typtype = 'd' THEN t.typbasetype ELSE t.oid END WHERE t.oid = cffi.atttypid) THEN format('(pg_catalog.jsonb_populate_record(NULL::%s, pg_catalog.jsonb_build_object(%L, s.full_data->%L))).%I', v_target_table_ident, cffi.attname, cffi.attname, cffi.attname) ELSE format('(s.full_data->>%L)::%s', cffi.attname, format_type(cffi.atttypid, -1)) END,
                            pg_get_expr(ad.adbin, ad.adrelid)
                        )
                    ELSE
                        CASE WHEN (SELECT b.typcategory IN ('A', 'C') OR b.oid IN ('json'::regtype, 'jsonb'::regtype) FROM pg_catalog.pg_type AS t JOIN pg_catalog.pg_type AS b ON b.oid = CASE WHEN t.typtype = 'd' THEN t.typbasetype ELSE t.oid END WHERE t.oid = cffi.atttypid) THEN format('(pg_catalog.jsonb_populate_record(NULL::%s, pg_catalog.jsonb_build_object(%L, s.full_data->%L))).%I', v_target_table_ident, cffi.attname, cffi.attname, cffi.attname) ELSE format('(s.full_data->>%L)::%s', cffi.attname, format_type(cffi.atttypid, -1)) END
                END,
                ', ' ORDER BY cffi.attnum
             )
             FROM cols_for_founding_insert cffi
             LEFT JOIN pg_attrdef ad ON ad.adrelid = temporal_merge_execute.target_table
                                    AND ad.adnum = cffi.attnum
             WHERE cffi.attname IS DISTINCT FROM v_valid_from_col AND cffi.attname IS DISTINCT FROM v_valid_until_col)
        INTO
            v_update_set_clause,
            v_all_cols_ident,
            v_all_cols_select,
            v_all_cols_from_jsonb,
            v_founding_all_cols_ident,
            v_founding_all_cols_from_jsonb;

    END IF; -- NOT v_use_native_executor (introspection phase 2)

        v_partition_count := COALESCE(array_length(v_partition_join_clauses, 1), 0);

        IF v_log_step_timing THEN
            v_t3 := clock_timestamp();
            RAISE NOTICE '(%) step_timing: introspection=%.1fms, index_checks=%.1fms, column_list_cte=%.1fms',
                v_log_id,
                extract(epoch from v_t1 - v_t0) * 1000,
                extract(epoch from v_t2 - v_t1) * 1000,
                extract(epoch from v_t3 - v_t2) * 1000;
        END IF;

        -- DELETE -> UPDATE -> INSERT order is critical for PostgreSQL 18 temporal FK compatibility.
        DECLARE
            v_deferrable_constraints text[];
            v_set_constraints_sql text;
            v_old_search_path TEXT;
        BEGIN
            /*
             * sql_saga's referenced-side regular foreign key triggers on the target are deferred whatever
             * delay_constraints says: a regular reference only needs the key to exist at some time, so a merge is
             * judged by the timeline it leaves, and a reshaped timeline (a DELETE then an INSERT) neither refuses
             * nor cascades (ON DELETE CASCADE). delay_constraints => false keeps the native constraints immediate.
             */
            IF delay_constraints OR EXISTS (
                SELECT FROM pg_catalog.pg_trigger AS t
                WHERE t.tgrelid = target_table AND t.tgconstraint <> 0
                  AND t.tgfoid IN ('sql_saga.uk_update_check_c()'::pg_catalog.regprocedure,
                                   'sql_saga.uk_delete_check_c()'::pg_catalog.regprocedure))
            THEN
                -- For DELETE-first timeline operations, we create temporary gaps that may violate
                -- foreign key constraints. We need to defer:
                -- 1. Unique/exclusion constraints ON the target table (prevents overlaps)
                -- 2. Foreign keys FROM other tables referencing the target table (tolerates gaps during DELETE)
                -- 3. sql_saga's own constraint triggers for regular foreign keys (contype 't'): the
                --    referenced-side triggers on the target, so a reshaped timeline is checked once the
                --    replacement rows are in, and the referencing-side trigger when the target is a child.
                -- With delay_constraints => false, only the referenced-side triggers of 3.
                DECLARE
                    v_intended oid[];
                    v_conflict record;
                BEGIN
                    SELECT array_agg(c.oid ORDER BY c.oid)
                    INTO v_intended
                    FROM pg_catalog.pg_constraint AS c
                    WHERE c.condeferrable
                      AND (
                          (delay_constraints AND c.conrelid = target_table AND c.contype IN ('u', 'x'))
                          OR
                          (delay_constraints AND c.confrelid = target_table AND c.contype = 'f')
                          OR
                          (c.conrelid = target_table AND c.contype = 't' AND EXISTS (
                              SELECT FROM pg_catalog.pg_trigger AS t
                              WHERE t.tgconstraint = c.oid
                                AND (t.tgfoid IN ('sql_saga.uk_update_check_c()'::pg_catalog.regprocedure,
                                                  'sql_saga.uk_delete_check_c()'::pg_catalog.regprocedure)
                                     OR (delay_constraints AND t.tgfoid = 'sql_saga.regular_fk_check_c()'::pg_catalog.regprocedure))))
                      );

                    /*
                     * SET CONSTRAINTS schema.name selects every constraint of that name in the schema, not one OID.
                     * Refuse, before any DML, when that would also select a constraint outside the intended set:
                     * deferring it (and later forcing it immediate) would change the checking moment of an object
                     * this merge has no business with.
                     */
                    SELECT pg_catalog.format('%I.%I', n.nspname, c.conname) AS deferred_name,
                           other.conname AS other_name,
                           pg_catalog.format('%I.%I', on_.nspname, oc.relname) AS other_table,
                           EXISTS (SELECT FROM pg_catalog.pg_trigger AS t
                                   WHERE t.tgconstraint = other.oid
                                     AND t.tgfoid IN ('sql_saga.uk_update_check_c()'::pg_catalog.regprocedure,
                                                      'sql_saga.uk_delete_check_c()'::pg_catalog.regprocedure,
                                                      'sql_saga.regular_fk_check_c()'::pg_catalog.regprocedure))
                           OR EXISTS (SELECT FROM sql_saga.unique_keys AS uk
                                      WHERE (uk.table_schema, uk.table_name) = (on_.nspname, oc.relname)
                                        AND other.conname IN (uk.unique_constraint, uk.exclude_constraint))
                           OR EXISTS (SELECT FROM sql_saga.foreign_keys AS fk
                                      WHERE (fk.table_schema, fk.table_name, fk.foreign_key_name)
                                          = (on_.nspname, oc.relname, other.conname)) AS other_managed
                    INTO v_conflict
                    FROM pg_catalog.pg_constraint AS c
                    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.connamespace
                    JOIN pg_catalog.pg_constraint AS other
                      ON (other.connamespace, other.conname) = (c.connamespace, c.conname)
                     AND other.oid <> ALL (v_intended)
                    JOIN pg_catalog.pg_class AS oc ON oc.oid = other.conrelid
                    JOIN pg_catalog.pg_namespace AS on_ ON on_.oid = oc.relnamespace
                    WHERE c.oid = ANY (v_intended)
                    ORDER BY c.oid, other.oid
                    LIMIT 1;

                    IF FOUND THEN
                        IF v_conflict.other_managed THEN
                            RAISE EXCEPTION 'sql_saga: temporal_merge on table % cannot defer constraint %: SET CONSTRAINTS by that name would also defer constraint % on table %, which belongs to another sql_saga object; rename one of them',
                                (SELECT pg_catalog.format('%I.%I', tn.nspname, tc.relname) FROM pg_catalog.pg_class AS tc JOIN pg_catalog.pg_namespace AS tn ON tn.oid = tc.relnamespace WHERE tc.oid = target_table),
                                v_conflict.deferred_name, pg_catalog.quote_ident(v_conflict.other_name), v_conflict.other_table
                                USING ERRCODE = 'duplicate_object';
                        END IF;
                        RAISE EXCEPTION 'sql_saga: temporal_merge on table % cannot defer constraint %: SET CONSTRAINTS by that name would also defer constraint % on table %, which sql_saga does not manage; rename that constraint',
                            (SELECT pg_catalog.format('%I.%I', tn.nspname, tc.relname) FROM pg_catalog.pg_class AS tc JOIN pg_catalog.pg_namespace AS tn ON tn.oid = tc.relnamespace WHERE tc.oid = target_table),
                            v_conflict.deferred_name, pg_catalog.quote_ident(v_conflict.other_name), v_conflict.other_table
                            USING ERRCODE = 'duplicate_object';
                    END IF;

                    -- Names are schema-qualified: SET CONSTRAINTS resolves a bare name through search_path.
                    SELECT array_agg(DISTINCT pg_catalog.format('%I.%I', n.nspname, c.conname))
                    INTO v_deferrable_constraints
                    FROM pg_catalog.pg_constraint AS c
                    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.connamespace
                    WHERE c.oid = ANY (v_intended);
                END;

                v_old_search_path := current_setting('search_path');
                EXECUTE format('SET search_path = %I, public', v_target_schema_name);

                IF v_deferrable_constraints IS NOT NULL THEN
                    v_set_constraints_sql := format('SET CONSTRAINTS %s DEFERRED',
                        array_to_string(v_deferrable_constraints, ', ')
                    );
                    IF v_log_execute THEN RAISE NOTICE '(%) Executing: %', v_log_id, v_set_constraints_sql; END IF;
                    EXECUTE v_set_constraints_sql;
                END IF;
            END IF;

            -- This block contains the DML. We wrap it to ensure constraints and search_path are reset.
            BEGIN
                -- PERFORMANCE: Analyze the plan table to give the optimizer accurate row estimates.
                -- Without this, PostgreSQL underestimates cardinality (often rows=1 for filtered enums)
                -- which causes it to choose Nested Loop joins instead of Hash Joins, resulting in
                -- 70x+ slower DML execution. This is critical for UPDATE operations that join
                -- temporal_merge_plan to the target table.
                ANALYZE temporal_merge_plan;

                -- Execute operations ordered by plan_op_seq: DELETE -> UPDATE -> INSERT
                -- The planner generates plan_op_seq to ensure DELETE operations come first,
                -- then UPDATEs (ordered by effect), then INSERTs. This prevents temporal overlaps.
                IF v_log_step_timing THEN v_t_delete := clock_timestamp(); END IF;
                IF v_log_execute THEN
                    RAISE NOTICE '(%) --- temporal_merge_execute: EXECUTING DELETES ---', v_log_id;
                END IF;
                -- 1. Execute DELETE operations (partitioned by nullable identity columns)
                -- Pre-extract entity_keys columns in subquery for efficient join.
                IF (SELECT TRUE FROM temporal_merge_plan WHERE operation = 'DELETE' LIMIT 1) THEN
                    FOR v_part IN 1..v_partition_count LOOP
                        v_sql := format($$ DELETE FROM %1$s t
                            USING (SELECT *, %5$s FROM temporal_merge_plan WHERE operation = 'DELETE' AND %6$s) p
                            WHERE %2$s AND t.%3$I = p.old_valid_range::%4$I;
                        $$, v_target_table_ident, v_partition_join_clauses[v_part], v_range_col, v_range_constructor, v_partition_select_lists[v_part], v_partition_plan_filters[v_part]);
                        IF v_log_sql THEN RAISE NOTICE '(%) DELETE partition %/% SQL: %', v_log_id, v_part, v_partition_count, v_sql; END IF;
                        EXECUTE v_sql;
                    END LOOP;
                END IF;

                IF v_log_step_timing THEN v_t_update := clock_timestamp(); END IF;
                IF v_log_execute THEN
                    RAISE NOTICE '(%) --- temporal_merge_execute: EXECUTING UPDATES ---', v_log_id;
                END IF;
                -- 2. Execute UPDATE operations (batched by statement_seq, partitioned by nullable identity columns)
                -- The planner assigns statement_seq to group operations that can safely execute together:
                -- - NONE/SHRINK operations share a statement_seq (they only contract ranges)
                -- - MOVE operations each get their own statement_seq (can create temporary overlaps)
                -- - GROW operations share a statement_seq (they only expand into gaps)
                -- This minimizes the number of SQL statements while preventing constraint violations.
                SELECT ARRAY_AGG(DISTINCT statement_seq ORDER BY statement_seq) INTO v_update_statement_seqs
                FROM temporal_merge_plan WHERE operation = 'UPDATE';

                IF v_update_statement_seqs IS NOT NULL THEN
                    FOREACH v_update_statement_seq IN ARRAY v_update_statement_seqs LOOP
                        FOR v_part IN 1..v_partition_count LOOP
                            IF v_update_set_clause IS NOT NULL THEN
                                -- Pre-extract entity_keys columns in subquery for efficient join.
                                -- This allows PostgreSQL to use Hash/Merge joins instead of cross-join with filter.
                                v_sql := format($$ UPDATE %1$s t SET %4$I = p.new_valid_range::%5$I, %2$s
                                    FROM (SELECT *, %9$s FROM temporal_merge_plan WHERE operation = 'UPDATE' AND statement_seq = %8$s AND %10$s ORDER BY plan_op_seq) p
                                    WHERE %3$s AND t.%4$I = p.old_valid_range::%5$I;
                                $$, v_target_table_ident, v_update_set_clause, v_partition_join_clauses[v_part], v_range_col, v_range_constructor, NULL, NULL, v_update_statement_seq, v_partition_select_lists[v_part], v_partition_plan_filters[v_part]);
                                IF v_log_sql THEN RAISE NOTICE '(%) UPDATE partition %/% seq=% SQL: %', v_log_id, v_part, v_partition_count, v_update_statement_seq, v_sql; END IF;
                                EXECUTE v_sql;
                            ELSIF v_all_cols_ident IS NOT NULL THEN
                                v_sql := format($$ UPDATE %1$s t SET %3$I = p.new_valid_range::%4$I
                                    FROM (SELECT *, %8$s FROM temporal_merge_plan WHERE operation = 'UPDATE' AND statement_seq = %7$s AND %9$s ORDER BY plan_op_seq) p
                                    WHERE %2$s AND t.%3$I = p.old_valid_range::%4$I;
                                $$, v_target_table_ident, v_partition_join_clauses[v_part], v_range_col, v_range_constructor, NULL, NULL, v_update_statement_seq, v_partition_select_lists[v_part], v_partition_plan_filters[v_part]);
                                IF v_log_sql THEN RAISE NOTICE '(%) UPDATE partition %/% seq=% SQL: %', v_log_id, v_part, v_partition_count, v_update_statement_seq, v_sql; END IF;
                                EXECUTE v_sql;
                            END IF;
                        END LOOP;
                    END LOOP;
                END IF;

                IF v_log_step_timing THEN v_t_insert := clock_timestamp(); END IF;
                IF v_log_execute THEN
                    RAISE NOTICE '(%) --- temporal_merge_execute: EXECUTING INSERTS ---', v_log_id;
                END IF;
                -- 3. Execute INSERT operations and capture generated IDs
                -- Provenance columns (doc/design/provenance.md): the planner gave each untouched piece its old values and
                -- each changed or new piece none; this transaction-local mark tells the managed trigger to keep the given
                -- values and fill only the missing ones from their DEFAULT. Cleared right after the INSERTs.
                PERFORM pg_catalog.set_config('sql_saga.provenance_carry', temporal_merge_execute.target_table::oid::text, true);
                IF v_all_cols_ident IS NOT NULL THEN
                     DECLARE
                        v_entity_id_update_jsonb_build TEXT;
                        v_needs_founding_insert BOOLEAN;
                     BEGIN
                        EXECUTE 'SELECT EXISTS(SELECT 1 FROM temporal_merge_plan WHERE operation = ''INSERT'' AND is_new_entity)'
                        INTO v_needs_founding_insert;

                        -- Build the expression to construct the entity_keys feedback JSONB.
                        WITH target_cols AS (
                            SELECT pa.attname
                            FROM pg_catalog.pg_attribute pa
                            WHERE pa.attrelid = v_target_table_ident::regclass
                              AND pa.attnum > 0 AND NOT pa.attisdropped
                        ),
                        feedback_id_cols AS (
                            SELECT col FROM unnest(COALESCE(temporal_merge_execute.identity_columns, '{}')) as col
                            UNION
                            SELECT pk_col FROM unnest(v_pk_cols) as pk_col WHERE pk_col IN (SELECT attname FROM target_cols) AND pk_col NOT IN (v_valid_from_col, v_valid_until_col)
                        )
                        SELECT
                            format('jsonb_build_object(%s)', string_agg(format('%L, ir.%I', col, col), ', '))
                        INTO
                            v_entity_id_update_jsonb_build
                        FROM feedback_id_cols;

                        -- Stage 1: Handle "founding" inserts for new entities that need generated keys.
                        IF v_needs_founding_insert THEN
                            CREATE TEMP TABLE temporal_merge_entity_id_map (grouping_key TEXT PRIMARY KEY, causal_id TEXT, new_entity_keys JSONB) ON COMMIT DROP;

                            -- Step 1.1: Insert just ONE "founding" row for each new conceptual entity to generate its ID.
                            EXECUTE format($$
                                WITH founding_plan_ops AS (
                                    SELECT DISTINCT ON (p.grouping_key)
                                        p.plan_op_seq,
                                        p.causal_id,
                                        p.grouping_key,
                                        p.new_valid_range,
                                        p.entity_keys || p.data as full_data
                                    FROM temporal_merge_plan p
                                    WHERE p.operation = 'INSERT' AND p.is_new_entity
                                    ORDER BY p.grouping_key, p.plan_op_seq
                                ),
                                id_map_cte AS (
                                    MERGE INTO %1$s t
                                    USING founding_plan_ops s ON false
                                    WHEN NOT MATCHED THEN
                                        INSERT (%2$s, %5$I)
                                        VALUES (%3$s, s.new_valid_range::%7$I)
                                    RETURNING t.*, s.causal_id, s.grouping_key
                                )
                                INSERT INTO temporal_merge_entity_id_map (grouping_key, causal_id, new_entity_keys)
                                SELECT
                                    ir.grouping_key,
                                    ir.causal_id,
                                    %4$s
                                FROM id_map_cte ir;
                            $$,
                                v_target_table_ident,           /* %1$s */
                                v_founding_all_cols_ident,      /* %2$s */
                                v_founding_all_cols_from_jsonb, /* %3$s */
                                v_entity_id_update_jsonb_build, /* %4$s */
                                v_range_col,                    /* %5$I */
                                NULL,                           /* %6$I (placeholder) */
                                v_range_constructor             /* %7$I */
                            );

                            -- Step 1.2: Back-fill the captured, generated IDs into the plan
                            EXECUTE format($$
                                UPDATE temporal_merge_plan p
                                SET entity_keys = p.entity_keys || m.new_entity_keys
                                FROM temporal_merge_entity_id_map m
                                WHERE p.grouping_key = m.grouping_key;
                            $$);

                            -- Step 1.3: Insert the remaining historical slices for the new entities
                            EXECUTE format($$
                                INSERT INTO %1$s (%2$s, %4$I)
                                SELECT %3$s, p.new_valid_range::%6$I
                                FROM temporal_merge_plan p,
                                     LATERAL jsonb_populate_record(null::%1$s, p.entity_keys || p.data) as jpr_all
                                WHERE p.operation = 'INSERT'
                                  AND p.is_new_entity
                                  AND p.causal_id IS NOT NULL
                                  AND NOT EXISTS (
                                    SELECT 1 FROM (
                                        SELECT DISTINCT ON (p_inner.grouping_key) plan_op_seq
                                        FROM temporal_merge_plan p_inner
                                        WHERE p_inner.operation = 'INSERT' AND p_inner.is_new_entity
                                        ORDER BY p_inner.grouping_key, p_inner.plan_op_seq
                                    ) AS founding_ops
                                    WHERE founding_ops.plan_op_seq = p.plan_op_seq
                                  )
                                ORDER BY p.plan_op_seq;
                            $$,
                                v_target_table_ident,       /* %1$s */
                                v_all_cols_ident,           /* %2$s */
                                v_all_cols_select,          /* %3$s */
                                v_range_col,                /* %4$I */
                                NULL,                       /* %5$I (placeholder) */
                                v_range_constructor         /* %6$I */
                            );

                            DROP TABLE temporal_merge_entity_id_map;
                        END IF;

                        -- Stage 2: Handle "non-founding" inserts
                        BEGIN
                            EXECUTE format($$
                                WITH
                                source_for_insert AS (
                                    SELECT
                                        p.plan_op_seq, p.new_valid_range,
                                        p.entity_keys || p.data as full_data
                                    FROM temporal_merge_plan p
                                    WHERE p.operation = 'INSERT' AND NOT p.is_new_entity
                                ),
                                inserted_rows AS (
                                    MERGE INTO %1$s t
                                    USING source_for_insert s ON false
                                    WHEN NOT MATCHED THEN
                                        INSERT (%2$s, %3$I)
                                        VALUES (%5$s, s.new_valid_range::%9$I)
                                    RETURNING t.*, s.plan_op_seq
                                )
                                UPDATE temporal_merge_plan p
                                SET entity_keys = p.entity_keys || %8$s
                                FROM inserted_rows ir
                                WHERE p.plan_op_seq = ir.plan_op_seq;
                            $$,
                                v_target_table_ident,               /* %1$s */
                                v_all_cols_ident,                   /* %2$s */
                                v_range_col,                        /* %3$I */
                                NULL,                               /* %4$I (placeholder) */
                                v_all_cols_from_jsonb,              /* %5$s */
                                NULL,                               /* %6$s (placeholder) */
                                NULL,                               /* %7$s (placeholder) */
                                v_entity_id_update_jsonb_build,     /* %8$s */
                                v_range_constructor                 /* %9$I */
                            );
                        END;
                     END;
                ELSE
                    -- This case handles tables with only temporal and defaulted ID columns.
                     DECLARE
                        v_entity_id_update_jsonb_build TEXT;
                     BEGIN
                        -- Build the expression to construct the entity_keys feedback JSONB.
                        WITH target_cols AS (
                            SELECT pa.attname
                            FROM pg_catalog.pg_attribute pa
                            WHERE pa.attrelid = v_target_table_ident::regclass
                              AND pa.attnum > 0 AND NOT pa.attisdropped
                        ),
                        feedback_id_cols AS (
                            SELECT col FROM unnest(COALESCE(temporal_merge_execute.identity_columns, '{}')) as col
                            UNION
                            SELECT pk_col FROM unnest(v_pk_cols) as pk_col WHERE pk_col IN (SELECT attname FROM target_cols) AND pk_col NOT IN (v_valid_from_col, v_valid_until_col)
                        )
                        SELECT
                            format('jsonb_build_object(%s)', string_agg(format('%L, ir.%I', col, col), ', '))
                        INTO
                            v_entity_id_update_jsonb_build
                        FROM feedback_id_cols;

                        v_sql := format($$
                            WITH
                            source_for_insert AS (
                                SELECT
                                    p.plan_op_seq,
                                    p.new_valid_range,
                                    p.entity_keys || p.data as full_data
                                FROM temporal_merge_plan p
                                WHERE p.operation = 'INSERT'
                            ),
                            inserted_rows AS (
                                MERGE INTO %1$s t
                                USING source_for_insert s ON false
                                WHEN NOT MATCHED THEN
                                    INSERT (%8$s, %3$I)
                                    VALUES (%9$s, s.new_valid_range::%7$I)
                                RETURNING t.*, s.plan_op_seq
                            )
                            UPDATE temporal_merge_plan p
                            SET entity_keys = p.entity_keys || %2$s
                            FROM inserted_rows ir
                            WHERE p.plan_op_seq = ir.plan_op_seq;
                        $$,
                            v_target_table_ident,           /* %1$s */
                            v_entity_id_update_jsonb_build, /* %2$s */
                            v_range_col,                    /* %3$I */
                            NULL,                           /* %4$I (placeholder) */
                            NULL,                           /* %5$s (placeholder) */
                            NULL,                           /* %6$s (placeholder) */
                            v_range_constructor,            /* %7$I */
                            v_all_cols_ident,               /* %8$s */
                            v_all_cols_from_jsonb           /* %9$s */
                        );
                        EXECUTE v_sql;
                     END;
                END IF;
                PERFORM pg_catalog.set_config('sql_saga.provenance_carry', '', true);

                IF v_log_step_timing THEN
                    v_t_done := clock_timestamp();
                    RAISE NOTICE '(%) executor_timing: delete=%.1fms, update=%.1fms, insert=%.1fms',
                        v_log_id,
                        extract(epoch from v_t_update - v_t_delete) * 1000,
                        extract(epoch from v_t_insert - v_t_update) * 1000,
                        extract(epoch from v_t_done - v_t_insert) * 1000;
                END IF;

                -- Back-fill source table with generated IDs if requested.
                IF temporal_merge_execute.update_source_with_identity THEN
                    DECLARE
                        v_source_update_set_clause TEXT;
                    BEGIN
                        SELECT string_agg(
                            format('%I = (p.entity_keys->>%L)::%s', j.key, j.key, format_type(a.atttypid, a.atttypmod)),
                            ', ' ORDER BY j.key
                        )
                        INTO v_source_update_set_clause
                        FROM (
                            SELECT key FROM jsonb_object_keys(
                                (SELECT entity_keys FROM temporal_merge_plan WHERE entity_keys IS NOT NULL and operation = 'INSERT' LIMIT 1)
                            ) as key
                            WHERE key = ANY(temporal_merge_execute.identity_columns)
                        ) j
                        JOIN pg_attribute a ON a.attname = j.key
                        WHERE a.attrelid = temporal_merge_execute.source_table AND NOT a.attisdropped AND a.attnum > 0;

                        IF v_source_update_set_clause IS NOT NULL THEN
                            -- PERFORMANCE OPTIMIZATION: The previous implementation used
                            -- `s.source_row_id = ANY(p.row_ids)` which caused O(n²) behavior
                            -- due to Nested Loop with array containment check on every row.
                            -- This rewrite unnests row_ids first and uses an equijoin, allowing
                            -- PostgreSQL to use Hash Join for O(n) performance.
                            v_sql := format($$
                                WITH plan_with_row_ids AS (
                                    SELECT p.plan_op_seq, p.entity_keys, unnest(p.row_ids) AS source_row_id
                                    FROM temporal_merge_plan p
                                    WHERE p.operation = 'INSERT' AND p.entity_keys IS NOT NULL
                                ),
                                map_row_to_entity AS (
                                    SELECT DISTINCT ON (pr.source_row_id)
                                        pr.source_row_id,
                                        pr.entity_keys
                                    FROM plan_with_row_ids pr
                                    ORDER BY pr.source_row_id, pr.plan_op_seq
                                )
                                UPDATE %1$s s
                                SET %2$s
                                FROM map_row_to_entity p
                                WHERE s.%3$I = p.source_row_id;
                            $$, temporal_merge_execute.source_table::text, v_source_update_set_clause, temporal_merge_execute.row_id_column);
                            EXECUTE v_sql;
                        END IF;
                    END;
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    PERFORM pg_catalog.set_config('sql_saga.provenance_carry', '', true);
                    -- If an error occurs, ensure constraints and search_path are reset before re-raising.
                    IF v_old_search_path IS NOT NULL THEN
                        IF v_deferrable_constraints IS NOT NULL THEN
                            v_set_constraints_sql := format('SET CONSTRAINTS %s IMMEDIATE',
                                array_to_string(v_deferrable_constraints, ', ')
                            );
                            IF v_log_execute THEN RAISE NOTICE '(%) Executing on error: %', v_log_id, v_set_constraints_sql; END IF;
                            EXECUTE v_set_constraints_sql;
                        END IF;
                        EXECUTE format('SET search_path = %s', v_old_search_path);
                    END IF;
                    RAISE;
            END;

            -- Restore constraints and search_path on successful completion.
            IF v_old_search_path IS NOT NULL THEN
                IF v_deferrable_constraints IS NOT NULL THEN
                    v_set_constraints_sql := format('SET CONSTRAINTS %s IMMEDIATE',
                        array_to_string(v_deferrable_constraints, ', ')
                    );
                    IF v_log_execute THEN RAISE NOTICE '(%) Executing: %', v_log_id, v_set_constraints_sql; END IF;
                    EXECUTE v_set_constraints_sql;
                END IF;
                EXECUTE format('SET search_path = %s', v_old_search_path);
            END IF;
        END;

        -- 4. Generate and store feedback
        v_sql := format($$
            WITH
            all_source_rows AS (
                SELECT t.%2$I AS source_row_id FROM %1$s t
            ),
            plan_unnested AS (
                SELECT unnest(p.row_ids) as source_row_id, p.plan_op_seq, p.entity_keys, p.operation, p.data, p.feedback
                FROM temporal_merge_plan p
            ),
            feedback_groups AS (
                SELECT
                    asr.source_row_id,
                    -- Aggregate all distinct operations for this source row.
                    array_agg(DISTINCT pu.operation) FILTER (WHERE pu.operation IS NOT NULL) as operations,
                    -- Aggregate all distinct entity IDs this source row touched.
                    COALESCE(jsonb_agg(DISTINCT pu.entity_keys) FILTER (WHERE pu.entity_keys IS NOT NULL), '[]'::jsonb) AS target_entity_keys,
                    -- Extract the specific error message from the plan's feedback payload if present.
                    (array_agg(pu.feedback->>'error') FILTER (WHERE pu.operation = 'ERROR' AND pu.feedback ? 'error'))[1] as error_message_from_plan
                FROM all_source_rows asr
                LEFT JOIN plan_unnested pu ON asr.source_row_id = pu.source_row_id
                GROUP BY asr.source_row_id
            )
            INSERT INTO temporal_merge_feedback
                SELECT
                    fg.source_row_id,
                    fg.target_entity_keys,
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
                        WHEN 'SKIP_ECLIPSED'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) THEN 'SKIPPED_ECLIPSED'
                        WHEN fg.operations = ARRAY['SKIP_IDENTICAL'::sql_saga.temporal_merge_plan_action] THEN 'SKIPPED_IDENTICAL'
                        -- If a source row resulted in no plan operations, it is an internal error.
                        WHEN fg.operations IS NULL THEN 'ERROR'
                        -- This is a safeguard. If the planner produces an unexpected combination of actions, we fail fast.
                        ELSE 'ERROR'
                    END::sql_saga.temporal_merge_feedback_status AS status,
                    CASE
                        WHEN 'ERROR'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) THEN COALESCE(fg.error_message_from_plan, 'Planner generated an ERROR action, indicating an internal logic error.')
                        WHEN fg.operations IS NULL THEN 'Planner failed to generate a plan for this source row.'
                        WHEN NOT (
                             'INSERT'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) OR
                             'UPDATE'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) OR
                             'DELETE'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) OR
                             'SKIP_NO_TARGET'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) OR
                             'SKIP_FILTERED'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) OR
                             'SKIP_ECLIPSED'::sql_saga.temporal_merge_plan_action = ANY(fg.operations) OR
                             fg.operations = ARRAY['SKIP_IDENTICAL'::sql_saga.temporal_merge_plan_action]
                        )
                        THEN 'Planner produced an unhandled combination of actions: ' || fg.operations::text
                        ELSE NULL::TEXT
                    END AS error_message
                FROM feedback_groups fg
                ORDER BY fg.source_row_id;
        $$,
            temporal_merge_execute.source_table::text,       -- %1$s
            temporal_merge_execute.row_id_column      -- %2$I
        );
        EXECUTE v_sql;

    -- Conditionally output the feedback for debugging, based on a session variable.
    DECLARE
        v_feedback_rec RECORD;
    BEGIN
        IF v_log_feedback THEN
            RAISE NOTICE 'temporal_merge feedback (%) %', v_log_id, v_summary_line;
            FOR v_feedback_rec IN SELECT * FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id LOOP
                RAISE NOTICE '(%) %', v_log_id, to_json(v_feedback_rec);
            END LOOP;
        END IF;
    END;

    IF temporal_merge_execute.update_source_with_feedback THEN
        IF temporal_merge_execute.feedback_status_column IS NULL AND temporal_merge_execute.feedback_error_column IS NULL THEN
            RAISE EXCEPTION 'When update_source_with_feedback is true, at least one feedback column (feedback_status_column or feedback_error_column) must be provided.';
        END IF;

        v_feedback_set_clause := '';

        -- If a status column is provided, build its part of the SET clause
        IF temporal_merge_execute.feedback_status_column IS NOT NULL THEN
            IF temporal_merge_execute.feedback_status_key IS NULL THEN
                RAISE EXCEPTION 'When feedback_status_column is provided, feedback_status_key must also be provided.';
            END IF;

            PERFORM 1 FROM pg_attribute WHERE attrelid = temporal_merge_execute.source_table AND attname = temporal_merge_execute.feedback_status_column AND atttypid = 'jsonb'::regtype AND NOT attisdropped AND attnum > 0;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'feedback_status_column "%" does not exist in source table %s or is not of type jsonb', temporal_merge_execute.feedback_status_column, temporal_merge_execute.source_table::text;
            END IF;

            v_feedback_set_clause := v_feedback_set_clause || format(
                '%I = COALESCE(s.%I, ''{}''::jsonb) || jsonb_build_object(%L, f.status)',
                temporal_merge_execute.feedback_status_column, temporal_merge_execute.feedback_status_column, temporal_merge_execute.feedback_status_key
            );
        END IF;

        -- If an error column is provided, build its part of the SET clause
        IF temporal_merge_execute.feedback_error_column IS NOT NULL THEN
            IF temporal_merge_execute.feedback_error_key IS NULL THEN
                RAISE EXCEPTION 'When feedback_error_column is provided, feedback_error_key must also be provided.';
            END IF;

            PERFORM 1 FROM pg_attribute WHERE attrelid = temporal_merge_execute.source_table AND attname = temporal_merge_execute.feedback_error_column AND atttypid = 'jsonb'::regtype AND NOT attisdropped AND attnum > 0;
            IF NOT FOUND THEN
                RAISE EXCEPTION 'feedback_error_column "%" does not exist in source table %s or is not of type jsonb', temporal_merge_execute.feedback_error_column, temporal_merge_execute.source_table::text;
            END IF;

            IF v_feedback_set_clause <> '' THEN
                v_feedback_set_clause := v_feedback_set_clause || ', ';
            END IF;

            v_feedback_set_clause := v_feedback_set_clause || format(
                '%I = CASE WHEN f.error_message IS NOT NULL THEN COALESCE(s.%I, ''{}''::jsonb) || jsonb_build_object(%L, f.error_message) ELSE COALESCE(s.%I, ''{}''::jsonb) - %L END',
                temporal_merge_execute.feedback_error_column, temporal_merge_execute.feedback_error_column, temporal_merge_execute.feedback_error_key, temporal_merge_execute.feedback_error_column, temporal_merge_execute.feedback_error_key
            );
        END IF;

        v_sql := format($$
            UPDATE %1$s s
            SET %2$s
            FROM pg_temp.temporal_merge_feedback f
            WHERE s.%3$I = f.source_row_id;
        $$, temporal_merge_execute.source_table::text, v_feedback_set_clause, temporal_merge_execute.row_id_column);
        EXECUTE v_sql;
    END IF;

END;
$temporal_merge_execute$;
/*
 * sql_saga 1.11 update: data repairs that need the 1.11 definitions. None: the typed decoder changes what later
 * temporal_merge calls write; rows written before are not inspected (a jsonb string written for a new entity's later
 * slice by the native planner cannot be told apart from a deliberate JSON string).
 */


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
    v_plan_sqls JSONB;
    v_source_table_ident TEXT;  -- Actual source table name
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
    v_multirange_type regtype;
    v_range_subtype regtype;
    v_range_subtype_category char(1);
    -- Provenance columns of the era (doc/design/provenance.md): stripped from changed pieces, compared when coalescing.
    v_provenance_columns text[];
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
    -- Column Classification Variables
    v_identity_columns_classified TEXT[];
    v_generated_columns TEXT[];
    v_not_null_default_columns TEXT[];
    v_nullable_default_columns TEXT[];
    v_always_exclude_columns TEXT[];
    v_not_null_no_default_columns TEXT[];
    -- Optimization: mutually_exclusive_columns metadata for performance
    v_mutually_exclusive_cols name[];
BEGIN
    v_log_vars := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_vars', true), ''), 'false')::boolean;
    v_log_id := substr(md5(COALESCE(current_setting('sql_saga.temporal_merge.log_id_seed', true), random()::text)), 1, 3);

    IF to_regclass('pg_temp.temporal_merge_plan_cache') IS NULL THEN
        CREATE TEMP TABLE temporal_merge_plan_cache (
            cache_key TEXT PRIMARY KEY,
            plan_sqls JSONB NOT NULL
        );
    END IF;

    v_causal_col := COALESCE(founding_id_column, row_id_column);
    
    -- Resolve source table identifier early (needed for placeholder substitution in execution phase)
    -- The regclass::TEXT conversion handles schema qualification correctly
    v_source_table_ident := source_table::regclass::TEXT;

    --
    -- Phase 1: Introspection and Column List Generation
    -- This block is the single source of truth for all column lists. All subsequent
    -- logic builds upon these foundational variables.
    --
    -- 1.1: Introspect temporal boundary columns. This must happen first.
    SELECT n.nspname, c.relname
    INTO v_target_schema_name, v_target_table_name_only
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = target_table;

    SELECT e.range_column_name, e.valid_from_column_name, e.valid_until_column_name, e.valid_to_column_name, e.range_type, e.multirange_type, e.range_subtype, e.range_subtype_category,
           COALESCE(e.provenance_columns::text[], '{}')
    INTO v_range_col, v_valid_from_col, v_valid_until_col, v_valid_to_col, v_range_constructor, v_multirange_type, v_range_subtype, v_range_subtype_category,
         v_provenance_columns
    FROM sql_saga.era e
    WHERE e.table_schema = v_target_schema_name AND e.table_name = v_target_table_name_only AND e.era_name = temporal_merge_plan.era_name;

    IF v_range_col IS NULL THEN RAISE EXCEPTION 'No era named "%" found for table "%"', era_name, sql_saga.__internal_display_name(target_table); END IF;

    -- 1.1.1: Query mutually_exclusive_columns metadata for join optimization
    -- We query the first applicable mutually_exclusive_columns array from unique_keys.
    -- This is used to generate optimized CASE/WHEN join conditions.
    SELECT uk.mutually_exclusive_columns INTO v_mutually_exclusive_cols
    FROM sql_saga.unique_keys uk
    WHERE uk.table_schema = v_target_schema_name 
      AND uk.table_name = v_target_table_name_only 
      AND uk.era_name = temporal_merge_plan.era_name
      AND uk.mutually_exclusive_columns IS NOT NULL
    LIMIT 1;

    v_temporal_cols := ARRAY[v_range_col];
    IF v_valid_from_col IS NOT NULL THEN v_temporal_cols := v_temporal_cols || v_valid_from_col; END IF;
    IF v_valid_to_col IS NOT NULL THEN v_temporal_cols := v_temporal_cols || v_valid_to_col; END IF;
    IF v_valid_until_col IS NOT NULL THEN v_temporal_cols := v_temporal_cols || v_valid_until_col; END IF;

    -- 1.2: Introspect the primary key (must happen after v_temporal_cols is defined).
    SELECT COALESCE(array_agg(a.attname ORDER BY array_position(c.conkey, a.attnum)), '{}'::name[]) INTO v_pk_cols
    FROM pg_constraint c JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
    WHERE c.conrelid = target_table AND c.contype = 'p' AND a.attname <> ALL(v_temporal_cols);

    -- 1.3: Introspect and sanitize all key types.
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

    -- 1.4: (Moved to cache-miss block - v_causal_column_type only used there)

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
    -- Normalize ephemeral columns for stable cache key and better reuse.
    SELECT ARRAY(SELECT DISTINCT e FROM unnest(v_ephemeral_columns) AS e ORDER BY e)
    INTO v_ephemeral_columns;

    -- 1.7: Build the source table's temporal column expression.
    -- This block determines how to project `valid_from` and `valid_until` from the source,
    -- handling `valid_to` conversion automatically. It also performs a consistency
    -- check if both `valid_to` and `valid_until` are present.
    DECLARE
        v_source_has_range_col BOOLEAN;
        v_source_has_valid_from BOOLEAN;
        v_source_has_valid_until BOOLEAN;
        v_source_has_valid_to BOOLEAN;
        v_from_expr TEXT;
        v_until_expr TEXT;
    BEGIN
        -- Introspect all possible temporal source columns.
        SELECT
            EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = source_table AND attname = v_range_col AND NOT attisdropped AND attnum > 0),
            EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = source_table AND attname = v_valid_from_col AND NOT attisdropped AND attnum > 0),
            EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = source_table AND attname = v_valid_until_col AND NOT attisdropped AND attnum > 0)
        INTO v_source_has_range_col, v_source_has_valid_from, v_source_has_valid_until;

        IF v_valid_to_col IS NOT NULL THEN
            SELECT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = source_table AND attname = v_valid_to_col AND NOT attisdropped AND attnum > 0)
            INTO v_source_has_valid_to;
        ELSE
            v_source_has_valid_to := false;
        END IF;

        -- Must have at least a `valid_from` or `range` column.
        IF NOT v_source_has_valid_from AND NOT v_source_has_range_col THEN
             RAISE EXCEPTION 'Source table "%" must have either the range column "%" or the component column "%".', sql_saga.__internal_display_name(source_table), v_range_col, v_valid_from_col;
        END IF;

        -- Build expressions for `_from` and `_until` components.
        IF v_source_has_valid_from THEN
            v_from_expr := format('source_table.%I', v_valid_from_col);
        END IF;

        IF v_source_has_valid_to THEN
            CASE v_range_subtype_category
                WHEN 'D' THEN v_interval := '''1 day''::interval';
                WHEN 'N' THEN v_interval := '1';
                ELSE RAISE EXCEPTION 'Unsupported range subtype for valid_to -> valid_until conversion: %', v_range_constructor::text;
            END CASE;
        END IF;

        -- Build v_until_expr with per-row logic
        IF v_source_has_valid_until AND v_source_has_valid_to THEN
             -- Both exist, so create a COALESCE expression to handle NULL valid_until.
             v_until_expr := format('COALESCE(source_table.%I, (source_table.%I + %s)::%s)', v_valid_until_col, v_valid_to_col, v_interval, v_range_subtype);
        ELSIF v_source_has_valid_until THEN
             v_until_expr := format('source_table.%I', v_valid_until_col);
        ELSIF v_source_has_valid_to THEN
             v_until_expr := format('(source_table.%I + %s)::%s', v_valid_to_col, v_interval, v_range_subtype);
        END IF;

        -- Must have a source for `_until` if there's no range column.
        IF NOT v_source_has_range_col AND v_until_expr IS NULL THEN
            RAISE EXCEPTION 'Source table "%" must have a "%", "%", or "%" column.', sql_saga.__internal_display_name(source_table), v_range_col, v_valid_until_col, v_valid_to_col;
        END IF;

        -- Combine expressions, prioritizing the range column but falling back to components.
        IF v_source_has_range_col THEN
            v_source_temporal_cols_expr := format('COALESCE(lower(source_table.%1$I), %2$s) as valid_from, COALESCE(upper(source_table.%1$I), %3$s) as valid_until',
                v_range_col, COALESCE(v_from_expr, 'NULL'), COALESCE(v_until_expr, 'NULL'));
        ELSE
            v_source_temporal_cols_expr := format('%s as valid_from, %s as valid_until', v_from_expr, v_until_expr);
        END IF;

        -- Build consistency check.
        IF v_source_has_valid_until AND v_source_has_valid_to THEN
            v_consistency_check_expr := format($$((source_table.%1$I IS NULL OR source_table.%2$I IS NULL) OR (((source_table.%1$I + %3$s)::%4$s) IS NOT DISTINCT FROM source_table.%2$I))$$, v_valid_to_col, v_valid_until_col, v_interval, v_range_subtype);
        ELSE
            v_consistency_check_expr := 'true';
        END IF;
    END;

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
    -- Phase 2: Session Cache Management & Validation
    --
    -- Generated plans are cached only per session (pg_temp). There is deliberately no cache shared
    -- between sessions: a shared cache of executable SQL lets one role make another role run SQL
    -- of its choosing (cache poisoning).
    -- The cache key includes source_table OID for within-session uniqueness.
    v_plan_key_text := format('%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s',
        target_table::oid,
        source_table::oid,
        identity_columns,
        v_ephemeral_columns,
        v_provenance_columns,
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

    SELECT plan_sqls INTO v_plan_sqls
    FROM pg_temp.temporal_merge_plan_cache
    WHERE cache_key = v_plan_key_text;
    
    IF v_plan_sqls IS NULL THEN -- Cache miss: PREPARE all sql's
        -- On cache miss, proceed with validation and planning.
        -- OPT-1: Consolidated column validation - single query instead of 6+ separate PERFORM queries.
        -- This batch validates all required columns exist in their respective tables at once.
        DECLARE
            v_missing RECORD;
            v_check_identity_in_source BOOLEAN;
        BEGIN
            -- Identity columns only need to exist in source if no lookup key is provided
            v_check_identity_in_source := (v_representative_lookup_key IS NULL OR cardinality(v_representative_lookup_key) = 0);
            
            FOR v_missing IN
                WITH required_columns AS (
                    -- row_id_column must exist in source
                    SELECT 'row_id_column' AS col_type, row_id_column AS col_name, source_table AS tbl
                    UNION ALL
                    -- founding_id_column must exist in source (if provided)
                    SELECT 'founding_id_column', founding_id_column, source_table
                    WHERE founding_id_column IS NOT NULL
                    UNION ALL
                    -- identity_columns must exist in target
                    SELECT 'identity_column (target)', unnest(v_identity_columns), target_table
                    WHERE v_identity_columns IS NOT NULL AND cardinality(v_identity_columns) > 0
                    UNION ALL
                    -- identity_columns must exist in source (only if no lookup key)
                    SELECT 'identity_column (source)', unnest(v_identity_columns), source_table
                    WHERE v_identity_columns IS NOT NULL AND cardinality(v_identity_columns) > 0
                      AND v_check_identity_in_source
                    UNION ALL
                    -- lookup_columns must exist in target
                    SELECT 'lookup_column (target)', unnest(v_all_lookup_cols), target_table
                    WHERE v_all_lookup_cols IS NOT NULL AND cardinality(v_all_lookup_cols) > 0
                    UNION ALL
                    -- lookup_columns must exist in source
                    SELECT 'lookup_column (source)', unnest(v_all_lookup_cols), source_table
                    WHERE v_all_lookup_cols IS NOT NULL AND cardinality(v_all_lookup_cols) > 0
                )
                SELECT rc.col_type, rc.col_name, rc.tbl::regclass::text AS tbl_name
                FROM required_columns rc
                LEFT JOIN pg_attribute a ON a.attrelid = rc.tbl 
                    AND a.attname = rc.col_name 
                    AND a.attnum > 0 
                    AND NOT a.attisdropped
                WHERE a.attname IS NULL
            LOOP
                -- Raise specific error message based on column type
                CASE v_missing.col_type
                    WHEN 'row_id_column' THEN
                        RAISE EXCEPTION 'row_id_column "%" does not exist in source table %s', v_missing.col_name, v_missing.tbl_name;
                    WHEN 'founding_id_column' THEN
                        RAISE EXCEPTION 'founding_id_column "%" does not exist in source table %s', v_missing.col_name, v_missing.tbl_name;
                    WHEN 'identity_column (target)' THEN
                        RAISE EXCEPTION 'identity_column % does not exist in target table %', quote_ident(v_missing.col_name), v_missing.tbl_name;
                    WHEN 'identity_column (source)' THEN
                        RAISE EXCEPTION 'identity_column % does not exist in source table % (and no lookup_columns were provided)', quote_ident(v_missing.col_name), v_missing.tbl_name;
                    WHEN 'lookup_column (target)' THEN
                        RAISE EXCEPTION 'lookup_column % does not exist in target table %', quote_ident(v_missing.col_name), v_missing.tbl_name;
                    WHEN 'lookup_column (source)' THEN
                        RAISE EXCEPTION 'lookup_column % does not exist in source table %', quote_ident(v_missing.col_name), v_missing.tbl_name;
                END CASE;
            END LOOP;
        END; -- END consolidated column validation
    
        IF v_causal_col IS NULL THEN
            RAISE EXCEPTION 'The causal identifier column cannot be NULL. Please provide a non-NULL value for either founding_id_column or row_id_column.';
        END IF;
        
        -- OPT-7a: Moved from pre-cache section - only used inside cache-miss block
        SELECT atttypid INTO v_causal_column_type
        FROM pg_attribute
        WHERE attrelid = source_table
          AND attname = v_causal_col
          AND NOT attisdropped AND attnum > 0;
          
        IF v_causal_column_type IS NULL THEN
            RAISE EXCEPTION 'Causal column "%" does not exist in source table %s', v_causal_col, source_table::text;
        END IF;
        IF v_causal_col = ANY(v_lookup_columns) THEN
            RAISE EXCEPTION 'The causal column (%) cannot be one of the natural identity columns (%)', v_causal_col, v_lookup_columns;
        END IF;
    
        IF v_range_constructor IS NULL THEN
            RAISE EXCEPTION 'Could not determine the range type for era "%" on table "%". Please ensure the era was created correctly, possibly by specifying the range_type parameter in add_era().', era_name, sql_saga.__internal_display_name(target_table);
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
            -- OPT-2: Cached nullability info for entity key columns (single query instead of 3)
            v_entity_key_cols_notnull name[];
            v_entity_key_cols_nullable name[];
            -- OPT-3: Cached metadata for lookup columns (nullability + type info)
            v_lookup_cols_meta JSONB;  -- {col_name: {notnull: bool, type: text}}
            v_propagated_id_cols_list TEXT;
            v_lookup_keys_as_jsonb_expr TEXT;
            v_lookup_keys_as_array_expr TEXT;
            v_keys_for_filtering JSONB;
            v_lateral_source_resolver_sql TEXT;
            v_lateral_source_resolver_sql_existing TEXT;
            v_lateral_source_resolver_sql_new TEXT;
            v_lateral_join_sr_to_seg_existing TEXT;
            v_lateral_join_sr_to_seg_new TEXT;
            v_unified_id_cols_projection TEXT;
            v_target_nk_json_expr TEXT;
            v_identity_keys_jsonb_build_expr_d TEXT;
            v_lookup_keys_jsonb_build_expr_d TEXT;
            v_identity_keys_jsonb_build_expr_sr TEXT;
            v_lookup_keys_jsonb_build_expr_sr TEXT;
        BEGIN
            v_plan_sqls := '[]'::jsonb;
            -- On cache miss, proceed with the expensive introspection and query building.
            
            -- OPT-2: Single query to get nullability info for all entity key columns.
            -- This replaces 3 separate pg_attribute queries in join condition builders.
            SELECT 
                COALESCE(array_agg(a.attname) FILTER (WHERE a.attnotnull), '{}'),
                COALESCE(array_agg(a.attname) FILTER (WHERE NOT a.attnotnull), '{}')
            INTO v_entity_key_cols_notnull, v_entity_key_cols_nullable
            FROM unnest(v_original_entity_key_cols) AS col
            JOIN pg_attribute a ON a.attrelid = target_table AND a.attname = col AND NOT a.attisdropped;
            
            -- OPT-3: Single query to get nullability + type info for all lookup columns.
            -- This replaces 4 separate pg_attribute queries in target_rows_filter loop.
            SELECT jsonb_object_agg(
                a.attname,
                jsonb_build_object('notnull', a.attnotnull, 'type', format_type(a.atttypid, a.atttypmod))
            )
            INTO v_lookup_cols_meta
            FROM unnest(v_lookup_columns) AS col
            JOIN pg_attribute a ON a.attrelid = target_table AND a.attname = col AND NOT a.attisdropped;
            
            v_lookup_cols_meta := COALESCE(v_lookup_cols_meta, '{}'::jsonb);
            
            v_unified_id_cols_projection := (
                SELECT string_agg(
                    CASE
                        WHEN col = ANY(COALESCE(v_all_lookup_cols, '{}'))
                        THEN format('(tpu.unified_canonical_nk_json->>%L)::%s AS %I', col, type, col)
                        ELSE format('tpu.%I', col)
                    END,
                    ', ' ORDER BY ord
                )
                FROM (
                    SELECT col, c.ord, format_type(a.atttypid, a.atttypmod) as type
                    FROM unnest(v_original_entity_segment_key_cols) WITH ORDINALITY AS c(col, ord)
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
                            ' AND ' ORDER BY c.ord
                        )
                        FROM unnest(COALESCE(v_all_lookup_cols, '{}')) WITH ORDINALITY AS c(col, ord)
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
            -- v_source_table_ident is set earlier in the function
    
            -- Construct reusable SQL fragments for entity key column lists and join conditions,
            -- since these are more index friendly than a jsonb object, that we previously had.
            -- The lateral joins from atomic segments to source/target rows must use ALL identity columns
            -- to ensure the join is on a unique entity. This logic correctly maps original column names
            -- to their internal, aliased names, which are used in all relevant CTEs (sr, tr, seg).
            --
            -- PERFORMANCE OPTIMIZATION: Use simple equality (col = col) for NOT NULL columns,
            -- which allows PostgreSQL to use a direct Index Scan instead of BitmapOr with two
            -- index scans, providing ~2x speedup. For NULLABLE columns, we must use the 
            -- NULL-safe pattern (col = col OR (col IS NULL AND col IS NULL)) to correctly
            -- handle NULL values that should match.
            -- OPT-2: Use cached nullability arrays instead of querying pg_attribute
            v_lateral_join_sr_to_seg_existing := (
                SELECT COALESCE(string_agg(
                    CASE
                        WHEN col = ANY(v_entity_key_cols_notnull) THEN
                            format('source_row.%1$I = seg.%1$I', col)
                        ELSE
                            format('(source_row.%1$I = seg.%1$I OR (source_row.%1$I IS NULL AND seg.%1$I IS NULL))', col)
                    END,
                    ' AND '
                ), 'true')
                FROM unnest(v_original_entity_key_cols) AS col
            );
            v_lateral_join_sr_to_seg_new := 'source_row.grouping_key = seg.grouping_key';

            v_lateral_join_sr_to_seg := format($$
                ((seg.is_new_entity AND %2$s)
                OR
                (NOT seg.is_new_entity AND %1$s))
            $$, v_lateral_join_sr_to_seg_existing, v_lateral_join_sr_to_seg_new);
            -- Build the join condition for existing_segments_with_target.
            -- Optimized: Use mutually_exclusive_columns for CASE/WHEN optimization when available.
            -- For other NULLABLE columns, use null-safe pattern (a = b OR (a IS NULL AND b IS NULL))
            -- For NOT NULL columns, use simple = for better query plan performance (Hash Joins).
            IF v_mutually_exclusive_cols IS NOT NULL 
               AND v_mutually_exclusive_cols <@ v_original_entity_key_cols::name[] THEN
                -- Generate optimized CASE/WHEN condition for mutually exclusive columns.
                -- This avoids expensive (col IS NULL AND col IS NULL) checks when columns
                -- are known to be mutually exclusive (only one can be non-NULL at a time).
                -- OPT-2: Use cached nullability arrays instead of querying pg_attribute
                v_lateral_join_tr_to_seg := (
                    WITH me_cols AS (
                        -- Mutually exclusive columns: generate WHEN clauses
                        SELECT string_agg(
                            format(
                                'WHEN target_row.%1$I IS NOT NULL THEN target_row.%1$I = seg.%1$I%2$s',
                                col,
                                -- Add IS NULL checks for other mutually exclusive cols
                                (SELECT COALESCE(' AND ' || string_agg(format('seg.%I IS NULL', other_col), ' AND '), '')
                                 FROM unnest(v_mutually_exclusive_cols) other_col WHERE other_col <> col)
                            ),
                            ' '
                        ) AS case_expr
                        FROM unnest(v_mutually_exclusive_cols) AS col
                    ),
                    non_me_cols AS (
                        -- Non-mutually-exclusive columns: use standard null-safe comparison
                        SELECT COALESCE(string_agg(
                            CASE
                                WHEN col = ANY(v_entity_key_cols_notnull) THEN
                                    format('target_row.%1$I = seg.%1$I', col)
                                ELSE
                                    format('(target_row.%1$I = seg.%1$I OR (target_row.%1$I IS NULL AND seg.%1$I IS NULL))', col)
                            END,
                            ' AND '
                        ), '') AS conditions
                        FROM unnest(v_original_entity_key_cols) AS col
                        WHERE col <> ALL(v_mutually_exclusive_cols)
                    )
                    SELECT 
                        CASE 
                            WHEN me_cols.case_expr IS NOT NULL AND non_me_cols.conditions <> '' THEN
                                format('(CASE %s ELSE FALSE END) AND %s', me_cols.case_expr, non_me_cols.conditions)
                            WHEN me_cols.case_expr IS NOT NULL THEN
                                format('CASE %s ELSE FALSE END', me_cols.case_expr)
                            ELSE 'true'
                        END
                    FROM me_cols, non_me_cols
                );
            ELSE
                -- OPT-2: Use cached nullability arrays instead of querying pg_attribute
                v_lateral_join_tr_to_seg := (
                    SELECT COALESCE(string_agg(
                        CASE
                            WHEN col = ANY(v_entity_key_cols_notnull) THEN
                                format('target_row.%1$I = seg.%1$I', col)
                            ELSE
                                format('(target_row.%1$I = seg.%1$I OR (target_row.%1$I IS NULL AND seg.%1$I IS NULL))', col)
                        END,
                        ' AND '
                    ), 'true')
                    FROM unnest(v_original_entity_key_cols) AS col
                );
            END IF;
    
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
                ', ' ORDER BY c.ord), '')
                FROM unnest(v_original_entity_segment_key_cols) WITH ORDINALITY AS c(col, ord)
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
                WHERE s.attname NOT IN (row_id_column, 'era_id', 'era_name')
                AND s.attname <> ALL(v_temporal_cols) -- Exclude all temporal columns (range, from, until, to)
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
                WHERE t.attname NOT IN ('era_id', 'era_name')
                AND t.attname <> ALL(v_temporal_cols) -- Exclude all temporal columns (range, from, until, to)
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
                -- Smart data payload construction using jsonb manipulation
                -- First build the full object, then we'll strip NULL DEFAULT columns in the query
                (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(
                    format('%L, source_table.%I', attname, attname), ', '
                ) FILTER (WHERE attname <> ALL(COALESCE(v_always_exclude_columns, '{}'))), '')) FROM source_data_cols),
                -- Smart ephemeral payload construction with same approach
                (SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(
                    format('%L, source_table.%I', attname, attname), ', '
                ) FILTER (WHERE attname <> ALL(COALESCE(v_always_exclude_columns, '{}'))), '')) FROM source_ephemeral_cols),
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
    
            -- Phase 3.5: Comprehensive Column Classification (MOVED BEFORE payload construction)
            -- Classify all target columns to enable intelligent payload construction
            WITH column_classifications AS (
                SELECT 
                    a.attname,
                    a.atthasdef,
                    a.attidentity,
                    a.attgenerated,
                    a.attnotnull,
                    CASE 
                        WHEN a.attidentity IN ('a', 'd') THEN 'IDENTITY'
                        WHEN a.attgenerated <> '' THEN 'GENERATED'  
                        WHEN a.attnotnull AND a.atthasdef THEN 'NOT_NULL_DEFAULT'
                        WHEN NOT a.attnotnull AND a.atthasdef THEN 'NULLABLE_DEFAULT'
                        WHEN a.attnotnull AND NOT a.atthasdef THEN 'NOT_NULL_NO_DEFAULT'
                        ELSE 'REGULAR'
                    END as column_class
                FROM pg_catalog.pg_attribute a
                WHERE a.attrelid = target_table 
                  AND a.attnum > 0 
                  AND NOT a.attisdropped
            )
            SELECT 
                COALESCE(array_agg(attname) FILTER (WHERE column_class = 'IDENTITY'), '{}'),
                COALESCE(array_agg(attname) FILTER (WHERE column_class = 'GENERATED'), '{}'),
                COALESCE(array_agg(attname) FILTER (WHERE column_class = 'NOT_NULL_DEFAULT'), '{}'),
                COALESCE(array_agg(attname) FILTER (WHERE column_class = 'NULLABLE_DEFAULT'), '{}'),
                COALESCE(array_agg(attname) FILTER (WHERE column_class IN ('IDENTITY', 'GENERATED')), '{}'),
                COALESCE(array_agg(attname) FILTER (WHERE column_class = 'NOT_NULL_NO_DEFAULT'), '{}')
            INTO 
                v_identity_columns_classified, v_generated_columns, 
                v_not_null_default_columns, v_nullable_default_columns,
                v_always_exclude_columns, v_not_null_no_default_columns
            FROM column_classifications;

            -- Build a jsonb object of the stable identity columns. These are the keys
            -- that must be preserved across an entity's timeline.
            SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, t.%I', col, col), ', '), ''))
            INTO v_stable_pk_cols_jsonb_build
            FROM unnest(v_identity_columns) col;
    
            SELECT format('jsonb_build_object(%s)', COALESCE(string_agg(format('%L, %I', col, col), ', '), ''))
            INTO v_stable_pk_cols_jsonb_build_bare
            FROM unnest(v_identity_columns) col;
    
            -- DEBUG: Temporary validation of column classifications
            IF cardinality(v_identity_columns_classified) > 0 THEN
                RAISE DEBUG 'Identity columns detected: %', v_identity_columns_classified;
            END IF;
            IF cardinality(v_generated_columns) > 0 THEN
                RAISE DEBUG 'Generated columns detected: %', v_generated_columns;
            END IF;
            IF cardinality(v_not_null_default_columns) > 0 THEN
                RAISE DEBUG 'NOT NULL + DEFAULT columns detected: %', v_not_null_default_columns;
            END IF;
    
            -- Build source-side stable PK payload. It must project NULL for columns not present in the source.
            -- Use WITH ORDINALITY to preserve array order and ensure deterministic output.
            SELECT COALESCE(format('jsonb_build_object(%s)', string_agg(
                CASE
                    WHEN sa.attname IS NOT NULL THEN format('%L, source_table.%I', c.col, c.col)
                    ELSE format('%L, NULL::%s', c.col, format_type(ta.atttypid, ta.atttypmod))
                END,
            ', ' ORDER BY c.ord)), '{}'::jsonb::text)
            INTO v_stable_pk_cols_jsonb_build_source
            FROM unnest(COALESCE(v_identity_columns, '{}')) WITH ORDINALITY AS c(col, ord)
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
                ' AND ' ORDER BY c.ord), 'true')
            INTO
                v_entity_id_check_is_null_expr
            FROM unnest(v_identity_columns) WITH ORDINALITY AS c(col, ord)
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
                    -- For range-only tables (no boundary columns), use the range column itself.
                    v_distinct_on_cols := v_identity_columns || COALESCE(v_valid_from_col, v_range_col);
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
                            -- OPT-3: Use cached metadata instead of querying pg_attribute
                            SELECT bool_or(NOT (v_lookup_cols_meta->col->>'notnull')::boolean)
                            INTO v_any_nullable_lookup_cols
                            FROM unnest(v_key_cols) AS col;
                            v_any_nullable_lookup_cols := COALESCE(v_any_nullable_lookup_cols, false);
    
                            -- This is the crucial fix. The subquery must project the columns for the
                            -- current key, and NULL for all columns belonging to OTHER natural keys
                            -- to ensure the UNION branches have the same structure.
                            -- OPT-3: Use cached metadata instead of querying pg_attribute
                            v_lookup_cols_si_alias_select_list := (
                                SELECT string_agg(
                                    CASE
                                        WHEN c.col = ANY(v_key_cols) THEN format('si.%I', c.col)
                                        ELSE format('NULL::%s', v_lookup_cols_meta->c.col->>'type')
                                    END,
                                ', ')
                                FROM unnest(v_lookup_columns) AS c(col)
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
                                -- Nullable columns in the key require special handling.
                                -- The naive NULL-safe join `(si.col = t.col OR (si.col IS NULL AND t.col IS NULL))`
                                -- causes O(n²) bitmap scans because PostgreSQL can't optimize the OR condition.
                                --
                                -- Solution: Partition by NULL pattern and use simple equality joins within each partition.
                                -- For each combination of {NULL, NOT NULL} for nullable columns, we generate a separate
                                -- subquery with explicit IS NULL / IS NOT NULL conditions, allowing index scans.
                                DECLARE
                                    v_nullable_cols name[];
                                    v_notnull_cols name[];
                                    v_partition_parts TEXT[];
                                    v_col name;
                                    v_null_condition TEXT;
                                    v_notnull_condition TEXT;
                                    v_join_cols TEXT;
                                BEGIN
                                    -- OPT-3: Use cached metadata instead of querying pg_attribute (2 queries -> 0)
                                    SELECT 
                                        COALESCE(array_agg(c) FILTER (WHERE NOT (v_lookup_cols_meta->c->>'notnull')::boolean), ARRAY[]::name[]),
                                        COALESCE(array_agg(c) FILTER (WHERE (v_lookup_cols_meta->c->>'notnull')::boolean), ARRAY[]::name[])
                                    INTO v_nullable_cols, v_notnull_cols
                                    FROM unnest(v_key_cols) AS c;
                                    
                                    v_partition_parts := ARRAY[]::TEXT[];
                                    
                                    -- For each nullable column, generate two partitions: one for NULL, one for NOT NULL
                                    -- This handles the common case of mutually exclusive nullable columns (XOR pattern)
                                    FOREACH v_col IN ARRAY v_nullable_cols LOOP
                                        -- Partition where this column IS NOT NULL (use equality join)
                                        v_notnull_condition := format('inner_t.%I IS NOT NULL', v_col);
                                        v_join_cols := (
                                            SELECT string_agg(format('si.%1$I = inner_t.%1$I', c), ' AND ')
                                            FROM unnest(v_notnull_cols || v_col) c
                                        );
                                        v_partition_parts := v_partition_parts || format($$(
                                            SELECT inner_t.*
                                            FROM %1$s inner_t
                                            WHERE %2$s
                                              AND (%3$s) IN (
                                                SELECT %4$s FROM source_initial si WHERE si.%5$I IS NOT NULL
                                              )
                                        )$$,
                                            v_target_table_ident,
                                            v_notnull_condition,
                                            (SELECT string_agg(format('inner_t.%I', c), ', ') FROM unnest(v_notnull_cols || v_col) c),
                                            (SELECT string_agg(format('si.%I', c), ', ') FROM unnest(v_notnull_cols || v_col) c),
                                            v_col
                                        );
                                    END LOOP;
                                    
                                    -- If there are no nullable columns, fall back to simple IN clause
                                    IF cardinality(v_nullable_cols) = 0 THEN
                                        v_partition_parts := ARRAY[format($$(
                                            SELECT * FROM %1$s inner_t
                                            WHERE (%2$s) IN (SELECT %3$s FROM source_initial si)
                                        )$$, 
                                            v_target_table_ident,
                                            (SELECT string_agg(format('inner_t.%I', col), ', ') FROM unnest(v_key_cols) col),
                                            (SELECT string_agg(format('si.%I', col), ', ') FROM unnest(v_key_cols) col)
                                        )];
                                    END IF;
                                    
                                    v_union_parts := v_union_parts || format($$(
                                        SELECT DISTINCT ON (%2$s) * FROM (
                                            %1$s
                                        ) partitioned
                                    )$$, 
                                        array_to_string(v_partition_parts, ' UNION ALL '),
                                        (SELECT string_agg(format('partitioned.%I', col), ', ') FROM unnest(v_distinct_on_cols) AS col)
                                    );
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
                -- PATCH modes: Strip NULLs from source payload. NULL values in source are ignored.
                v_source_data_payload_expr := format($sn$(CASE WHEN pg_catalog.jsonb_path_exists(%1$s, 'strict $.*.**{1 to last} ? (@ == null)') THEN (%1$s) - ARRAY(SELECT sn.k FROM pg_catalog.jsonb_each(%1$s) AS sn(k, v) WHERE sn.v = 'null'::jsonb) ELSE pg_catalog.jsonb_strip_nulls(%1$s) END)$sn$, v_source_data_cols_jsonb_build);
                v_source_ephemeral_payload_expr := format($sn$(CASE WHEN pg_catalog.jsonb_path_exists(%1$s, 'strict $.*.**{1 to last} ? (@ == null)') THEN (%1$s) - ARRAY(SELECT sn.k FROM pg_catalog.jsonb_each(%1$s) AS sn(k, v) WHERE sn.v = 'null'::jsonb) ELSE pg_catalog.jsonb_strip_nulls(%1$s) END)$sn$, v_source_ephemeral_cols_jsonb_build);
            ELSIF mode = 'DELETE_FOR_PORTION_OF' THEN
                v_source_data_payload_expr := '''"__DELETE__"''::jsonb';
                v_source_ephemeral_payload_expr := v_source_ephemeral_cols_jsonb_build;
            ELSE -- MERGE_ENTITY_REPLACE, MERGE_ENTITY_UPSERT, REPLACE_FOR_PORTION_OF, UPDATE_FOR_PORTION_OF, INSERT_NEW_ENTITIES
                -- UPDATE/REPLACE modes: Smart payload handling for DEFAULT and NOT NULL columns
                -- Build expressions that exclude NULL values for:
                -- 1. Columns with defaults (use the default)
                -- 2. NOT NULL columns without defaults (avoid constraint violation)
                DECLARE 
                    v_exclude_if_null_cols TEXT[];
                    v_source_columns TEXT[];
                    v_col TEXT;
                    v_data_strip_expr TEXT;
                    v_ephemeral_strip_expr TEXT;
                BEGIN
                    -- Get list of columns that exist in the source
                    SELECT array_agg(attname)
                    INTO v_source_columns
                    FROM pg_catalog.pg_attribute
                    WHERE attrelid = source_table
                      AND attnum > 0
                      AND NOT attisdropped;
                    
                    -- Filter to only columns that exist in both source and target
                    -- and should be excluded if NULL
                    v_exclude_if_null_cols := ARRAY(
                        SELECT unnest(v_not_null_default_columns || v_nullable_default_columns || v_not_null_no_default_columns)
                        INTERSECT
                        SELECT unnest(v_source_columns)
                    );
                    
                    IF cardinality(v_exclude_if_null_cols) > 0 THEN
                        -- Start with the base expression
                        v_data_strip_expr := v_source_data_cols_jsonb_build;
                        v_ephemeral_strip_expr := v_source_ephemeral_cols_jsonb_build;
                        
                        -- For each column that should be excluded if NULL, wrap with a CASE to remove it
                        FOREACH v_col IN ARRAY v_exclude_if_null_cols
                        LOOP
                            v_data_strip_expr := format(
                                $$(CASE WHEN source_table.%I IS NULL THEN (%s) - %L ELSE (%s) END)$$,
                                v_col, v_data_strip_expr, v_col, v_data_strip_expr
                            );
                            v_ephemeral_strip_expr := format(
                                $$(CASE WHEN source_table.%I IS NULL THEN (%s) - %L ELSE (%s) END)$$,
                                v_col, v_ephemeral_strip_expr, v_col, v_ephemeral_strip_expr
                            );
                        END LOOP;
                        
                        v_source_data_payload_expr := v_data_strip_expr;
                        v_source_ephemeral_payload_expr := v_ephemeral_strip_expr;
                    ELSE
                        -- No columns to exclude, use original behavior
                        v_source_data_payload_expr := v_source_data_cols_jsonb_build;
                        v_source_ephemeral_payload_expr := v_source_ephemeral_cols_jsonb_build;
                    END IF;
                END;
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
            -- Build the join expression for source_rows_with_matches.
            -- For NULLABLE columns, we MUST use the null-safe pattern (a = b OR (a IS NULL AND b IS NULL))
            -- because NULL = NULL evaluates to NULL (not TRUE), which would fail the join.
            -- For NOT NULL columns, we can use simple = for better query plan performance (Hash Joins).
            v_source_rows_exists_join_expr := (
                SELECT string_agg(
                    format('((%s))', (
                        SELECT string_agg(
                            CASE
                                WHEN a.attnotnull THEN
                                    -- NOT NULL column: safe to use simple =
                                    format('source_row.%1$I = target_row.%2$I', c, CASE WHEN c = v_valid_from_col THEN 'valid_from'::name WHEN c = v_valid_until_col THEN 'valid_until'::name ELSE c END)
                                ELSE
                                    -- NULLABLE column: must use null-safe comparison
                                    format('(source_row.%1$I = target_row.%2$I OR (source_row.%1$I IS NULL AND target_row.%2$I IS NULL))', c, CASE WHEN c = v_valid_from_col THEN 'valid_from'::name WHEN c = v_valid_until_col THEN 'valid_until'::name ELSE c END)
                            END,
                            ' AND ' ORDER BY k.ord
                        )
                        FROM jsonb_array_elements_text(v_key) WITH ORDINALITY AS k(c, ord)
                        LEFT JOIN pg_attribute a ON a.attrelid = target_table AND a.attname = k.c AND NOT a.attisdropped
                    )),
                ' OR ' ORDER BY lk.ord)
                FROM jsonb_array_elements(lookup_keys) WITH ORDINALITY AS lk(v_key, ord)
            );

            IF v_source_rows_exists_join_expr IS NULL THEN
                -- Fallback to identity columns when no lookup keys provided
                v_source_rows_exists_join_expr := (
                    SELECT string_agg(
                        CASE
                            WHEN a.attnotnull THEN
                                format('source_row.%1$I = target_row.%2$I', col, CASE WHEN col = v_valid_from_col THEN 'valid_from'::name WHEN col = v_valid_until_col THEN 'valid_until'::name ELSE col END)
                            ELSE
                                format('(source_row.%1$I = target_row.%2$I OR (source_row.%1$I IS NULL AND target_row.%2$I IS NULL))', col, CASE WHEN col = v_valid_from_col THEN 'valid_from'::name WHEN col = v_valid_until_col THEN 'valid_until'::name ELSE col END)
                        END,
                        ' AND ' ORDER BY ic.ord
                    )
                    FROM unnest(v_identity_columns) WITH ORDINALITY AS ic(col, ord)
                    LEFT JOIN pg_attribute a ON a.attrelid = target_table AND a.attname = ic.col AND NOT a.attisdropped
                );
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
                -- Trace seed captures the initial state of each atomic segment with clear naming:
                -- - lookup_key_values: the key columns used to match source to target (not the grouping_key string)
                -- - target_stable_pk: the stable PK from the matched target row (if any)
                -- - segment_stable_pk: the propagated stable PK for this segment
                -- - seg_valid_from/until: the segment's temporal boundaries
                -- - s_valid_from/until: the source row's original temporal boundaries
                -- - t_valid_from/until: the target row's temporal boundaries (if matched)
                -- Note: source_row_id removed as redundant with contributing_row_ids[1]
                v_trace_seed_expr := format($$jsonb_build_object('cte', 'resolved_atomic_segments_with_payloads', 'contributing_row_ids', source_payloads.contributing_row_ids, 'constellation', %4$L, 'lookup_key_values', jsonb_build_object(%1$s), 's_data', source_payloads.data_payload, 't_data', seg.t_data_payload, 's_ephemeral', source_payloads.ephemeral_payload, 't_ephemeral', seg.t_ephemeral_payload, 'seg_valid_from', seg.valid_from, 'seg_valid_until', seg.valid_until, 's_valid_from', source_payloads.valid_from, 's_valid_until', source_payloads.valid_until, 't_valid_from', seg.t_valid_from, 't_valid_until', seg.t_valid_until, 'orig_source_target_relation', sql_saga.get_allen_relation(source_payloads.valid_from, source_payloads.valid_until, seg.t_valid_from, seg.t_valid_until), 'target_stable_pk', seg.target_stable_pk_payload, 'segment_stable_pk', seg.stable_pk_payload, 'seg_is_new_entity', seg.is_new_entity, 'seg_stable_identity_columns_are_null', seg.stable_identity_columns_are_null, 'seg_lookup_columns_unavailable', seg.lookup_columns_unavailable, 'stable_identity_values', %2$s, 'natural_identity_values', %3$s, 'canonical_causal_id', seg.causal_id) as trace$$,
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
                    'segment_stable_pk', propagated_stable_pk_payload
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
                                    r.data_payload || (CASE WHEN pg_catalog.jsonb_path_exists(s.data_payload, 'strict $.*.**{1 to last} ? (@ == null)') THEN (s.data_payload) - ARRAY(SELECT sn.k FROM pg_catalog.jsonb_each(s.data_payload) AS sn(k, v) WHERE sn.v = 'null'::jsonb) ELSE pg_catalog.jsonb_strip_nulls(s.data_payload) END),
                                    r.ephemeral_payload || (CASE WHEN pg_catalog.jsonb_path_exists(s.ephemeral_payload, 'strict $.*.**{1 to last} ? (@ == null)') THEN (s.ephemeral_payload) - ARRAY(SELECT sn.k FROM pg_catalog.jsonb_each(s.ephemeral_payload) AS sn(k, v) WHERE sn.v = 'null'::jsonb) ELSE pg_catalog.jsonb_strip_nulls(s.ephemeral_payload) END),
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

                    v_lateral_source_resolver_sql_existing := format($$
                        LEFT JOIN LATERAL (
                            WITH RECURSIVE ordered_sources AS (
                                SELECT
                                    source_row.source_row_id, source_row.data_payload, source_row.ephemeral_payload,
                                    source_row.valid_from, source_row.valid_until, source_row.causal_id,
                                    row_number() OVER (ORDER BY source_row.source_row_id) as rn
                                FROM active_source_rows source_row
                                WHERE %1$s
                                AND %2$I(seg.valid_from, seg.valid_until) <@ %2$I(source_row.valid_from, source_row.valid_until)
                            ),
                            running_payload AS (
                                SELECT rn, source_row_id, data_payload, ephemeral_payload, valid_from, valid_until, causal_id, ARRAY[source_row_id::BIGINT] as contributing_row_ids
                                FROM ordered_sources WHERE rn = 1
                                UNION ALL
                                SELECT
                                    s.rn, s.source_row_id,
                                    r.data_payload || (CASE WHEN pg_catalog.jsonb_path_exists(s.data_payload, 'strict $.*.**{1 to last} ? (@ == null)') THEN (s.data_payload) - ARRAY(SELECT sn.k FROM pg_catalog.jsonb_each(s.data_payload) AS sn(k, v) WHERE sn.v = 'null'::jsonb) ELSE pg_catalog.jsonb_strip_nulls(s.data_payload) END),
                                    r.ephemeral_payload || (CASE WHEN pg_catalog.jsonb_path_exists(s.ephemeral_payload, 'strict $.*.**{1 to last} ? (@ == null)') THEN (s.ephemeral_payload) - ARRAY(SELECT sn.k FROM pg_catalog.jsonb_each(s.ephemeral_payload) AS sn(k, v) WHERE sn.v = 'null'::jsonb) ELSE pg_catalog.jsonb_strip_nulls(s.ephemeral_payload) END),
                                    s.valid_from, s.valid_until, s.causal_id,
                                    r.contributing_row_ids || s.source_row_id::BIGINT
                                FROM running_payload r JOIN ordered_sources s ON s.rn = r.rn + 1
                            )
                            SELECT source_row_id, data_payload, ephemeral_payload, valid_from, valid_until, causal_id, contributing_row_ids
                            FROM running_payload
                            ORDER BY rn DESC
                            LIMIT 1
                        ) source_payloads ON true
                    $$, v_lateral_join_sr_to_seg_existing, v_range_constructor);

                    v_lateral_source_resolver_sql_new := format($$
                        LEFT JOIN LATERAL (
                            WITH RECURSIVE ordered_sources AS (
                                SELECT
                                    source_row.source_row_id, source_row.data_payload, source_row.ephemeral_payload,
                                    source_row.valid_from, source_row.valid_until, source_row.causal_id,
                                    row_number() OVER (ORDER BY source_row.source_row_id) as rn
                                FROM active_source_rows source_row
                                WHERE %1$s
                                AND %2$I(seg.valid_from, seg.valid_until) <@ %2$I(source_row.valid_from, source_row.valid_until)
                            ),
                            running_payload AS (
                                SELECT rn, source_row_id, data_payload, ephemeral_payload, valid_from, valid_until, causal_id, ARRAY[source_row_id::BIGINT] as contributing_row_ids
                                FROM ordered_sources WHERE rn = 1
                                UNION ALL
                                SELECT
                                    s.rn, s.source_row_id,
                                    r.data_payload || (CASE WHEN pg_catalog.jsonb_path_exists(s.data_payload, 'strict $.*.**{1 to last} ? (@ == null)') THEN (s.data_payload) - ARRAY(SELECT sn.k FROM pg_catalog.jsonb_each(s.data_payload) AS sn(k, v) WHERE sn.v = 'null'::jsonb) ELSE pg_catalog.jsonb_strip_nulls(s.data_payload) END),
                                    r.ephemeral_payload || (CASE WHEN pg_catalog.jsonb_path_exists(s.ephemeral_payload, 'strict $.*.**{1 to last} ? (@ == null)') THEN (s.ephemeral_payload) - ARRAY(SELECT sn.k FROM pg_catalog.jsonb_each(s.ephemeral_payload) AS sn(k, v) WHERE sn.v = 'null'::jsonb) ELSE pg_catalog.jsonb_strip_nulls(s.ephemeral_payload) END),
                                    s.valid_from, s.valid_until, s.causal_id,
                                    r.contributing_row_ids || s.source_row_id::BIGINT
                                FROM running_payload r JOIN ordered_sources s ON s.rn = r.rn + 1
                            )
                            SELECT source_row_id, data_payload, ephemeral_payload, valid_from, valid_until, causal_id, contributing_row_ids
                            FROM running_payload
                            ORDER BY rn DESC
                            LIMIT 1
                        ) source_payloads ON true
                    $$, v_lateral_join_sr_to_seg_new, v_range_constructor);

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

                    v_lateral_source_resolver_sql_existing := format($$
                        LEFT JOIN LATERAL (
                            WITH RECURSIVE ordered_sources AS (
                                SELECT
                                    source_row.source_row_id, source_row.data_payload, source_row.ephemeral_payload,
                                    source_row.valid_from, source_row.valid_until, source_row.causal_id,
                                    row_number() OVER (ORDER BY source_row.source_row_id) as rn
                                FROM active_source_rows source_row
                                WHERE %1$s
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
                    $$, v_lateral_join_sr_to_seg_existing, v_range_constructor);

                    v_lateral_source_resolver_sql_new := format($$
                        LEFT JOIN LATERAL (
                            WITH RECURSIVE ordered_sources AS (
                                SELECT
                                    source_row.source_row_id, source_row.data_payload, source_row.ephemeral_payload,
                                    source_row.valid_from, source_row.valid_until, source_row.causal_id,
                                    row_number() OVER (ORDER BY source_row.source_row_id) as rn
                                FROM active_source_rows source_row
                                WHERE %1$s
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
                    $$, v_lateral_join_sr_to_seg_new, v_range_constructor);

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

                    v_lateral_source_resolver_sql_existing := format($$
                        LEFT JOIN LATERAL (
                            SELECT source_row.source_row_id, source_row.data_payload, source_row.ephemeral_payload, source_row.valid_from, source_row.valid_until, source_row.causal_id, ARRAY[source_row.source_row_id::BIGINT] as contributing_row_ids
                            FROM active_source_rows source_row
                            WHERE %1$s
                            AND %2$I(seg.valid_from, seg.valid_until) <@ %2$I(source_row.valid_from, source_row.valid_until)
                            ORDER BY source_row.source_row_id DESC
                            LIMIT 1
                        ) source_payloads ON true
                    $$, v_lateral_join_sr_to_seg_existing, v_range_constructor);

                    v_lateral_source_resolver_sql_new := format($$
                        LEFT JOIN LATERAL (
                            SELECT source_row.source_row_id, source_row.data_payload, source_row.ephemeral_payload, source_row.valid_from, source_row.valid_until, source_row.causal_id, ARRAY[source_row.source_row_id::BIGINT] as contributing_row_ids
                            FROM active_source_rows source_row
                            WHERE %1$s
                            AND %2$I(seg.valid_from, seg.valid_until) <@ %2$I(source_row.valid_from, source_row.valid_until)
                            ORDER BY source_row.source_row_id DESC
                            LIMIT 1
                        ) source_payloads ON true
                    $$, v_lateral_join_sr_to_seg_new, v_range_constructor);

                ELSE
                    RAISE EXCEPTION 'Unhandled temporal_merge_mode in planner: %', mode;
            END CASE;
    
            -- 4. Construct and execute the main query to generate the execution plan.
            --
            -- Phase 4.1: Materialize source_initial as a temporary table for performance and clarity.
            
            v_sql := format($SQL$
                CREATE TEMP TABLE source_initial ON COMMIT DROP AS
                WITH source_data AS (
                    SELECT
                        source_table.%1$I /* row_id_column */ as source_row_id,
                        %2$s /* v_causal_select_expr */ as causal_id,
                        %3$s -- v_non_temporal_lookup_cols_select_list_prefix
                        %4$s, /* v_source_temporal_cols_expr */
                        %5$s /* v_source_data_payload_expr */ AS data_payload,
                        %6$s /* v_source_ephemeral_payload_expr */ AS ephemeral_payload,
                        %7$s /* v_stable_pk_cols_jsonb_build_source */ as stable_pk_payload,
                        %8$s /* v_entity_id_check_is_null_expr */ as stable_identity_columns_are_null,
                        %9$s /* v_lookup_cols_are_null_expr */ as lookup_columns_unavailable,
                        %10$s /* v_is_identifiable_expr */ as is_identifiable,
                        %11$s /* v_consistency_check_expr */ as temporal_columns_are_consistent
                    FROM %12$s /* v_source_table_ident */ source_table
                )
                SELECT 
                    *,
                    -- OPTIMIZATION: Pre-compute range column to avoid repeated daterange() calls
                    -- This provides ~10-15%% performance improvement in eclipse detection
                    %13$I(valid_from, valid_until) as valid_range
                FROM source_data;
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
                v_source_table_ident,                                /* %12$s */
                v_range_constructor                                  /* %13$I */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            v_sql := 'CREATE INDEX ON source_initial (source_row_id);';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);

            IF v_lookup_columns IS NOT NULL AND cardinality(v_lookup_columns) > 0 THEN
                SELECT format('CREATE INDEX ON source_initial (%s);', string_agg(quote_ident(c), ', '))
                INTO v_sql
                FROM unnest(v_lookup_columns) AS c;
                v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);
                
                -- OPTIMIZATION: Add composite index for eclipse detection
                -- This enables index-only scans providing ~20-30 percent performance improvement
                SELECT format('CREATE INDEX ON source_initial (%s, source_row_id);', string_agg(quote_ident(c), ', '))
                INTO v_sql
                FROM unnest(v_lookup_columns) AS c;
                v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);
            END IF;

            v_sql := 'CREATE INDEX ON source_initial (causal_id);';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);

            v_sql := 'CREATE INDEX ON source_initial (causal_id, source_row_id);';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);

            v_sql := 'ANALYZE source_initial;';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);
    
            v_sql := format($SQL$
                CREATE TEMP TABLE source_with_eclipsed_flag ON COMMIT DROP AS
                SELECT * FROM (
                    -- Path 1: Existing Entities (Window Function on Lookup Keys)
                    SELECT
                        s1.*,
                        COALESCE(
                            range_agg(valid_range) OVER (
                                PARTITION BY %1$s
                                ORDER BY source_row_id DESC
                                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                            ) @> valid_range,
                            false
                        ) as is_eclipsed,
                        array_agg(source_row_id) OVER (
                            PARTITION BY %1$s
                            ORDER BY source_row_id DESC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                        ) as eclipsed_by
                    FROM source_initial s1
                    WHERE NOT s1.lookup_columns_unavailable

                    UNION ALL

                    -- Path 2: New Entities (Window Function on Causal ID)
                    SELECT
                        s1.*,
                        COALESCE(
                            range_agg(valid_range) OVER (
                                PARTITION BY causal_id
                                ORDER BY source_row_id DESC
                                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                            ) @> valid_range,
                            false
                        ) as is_eclipsed,
                        array_agg(source_row_id) OVER (
                            PARTITION BY causal_id
                            ORDER BY source_row_id DESC
                            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                        ) as eclipsed_by
                    FROM source_initial s1
                    WHERE s1.lookup_columns_unavailable
                ) t;
            $SQL$,
                (SELECT string_agg(format('%I', col), ', ') FROM unnest(v_lookup_columns) as col)  -- %1$s
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- Phase 4.2: Construct and execute the main query to generate the execution plan.
            -- Each CTE is materialized into a temporary table for performance analysis and debugging.
            
            -- CTE 3: target_rows
            v_sql := format($SQL$
                CREATE TEMP TABLE target_rows ON COMMIT DROP AS
                SELECT
                    %1$s /* v_non_temporal_lookup_cols_select_list_no_alias_prefix */ -- (non-temporal identity columns)
                    lower(t.%2$I) as valid_from, -- The temporal identity column (e.g., valid_from)
                    NULL::%3$s /* v_causal_column_type */ as causal_id, -- Target rows do not originate from a source row, so causal_id is NULL. Type is introspected.
                    %4$s /* v_stable_pk_cols_jsonb_build_bare */ as stable_pk_payload,
                    upper(t.%2$I) as valid_until,
                    %6$s /* v_target_data_cols_jsonb_build_bare */ AS data_payload,
                    %7$s /* v_target_ephemeral_cols_jsonb_build_bare */ AS ephemeral_payload,
                    %8$s /* v_target_nk_json_expr */ AS canonical_nk_json
                FROM %9$s /* v_target_rows_filter */
            $SQL$,
                v_non_temporal_lookup_cols_select_list_no_alias_prefix, /* %1$s */
                v_range_col,                                            /* %2$I */
                v_causal_column_type,                                   /* %3$s */
                v_stable_pk_cols_jsonb_build_bare,                      /* %4$s */
                NULL,                                                   /* %5$I -- placeholder */
                v_target_data_cols_jsonb_build_bare,                    /* %6$s */
                v_target_ephemeral_cols_jsonb_build_bare,               /* %7$s */
                v_target_nk_json_expr,                                  /* %8$s */
                v_target_rows_filter                                    /* %9$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);

            IF v_original_entity_key_cols IS NOT NULL AND cardinality(v_original_entity_key_cols) > 0 THEN
                v_sql := format('CREATE INDEX ON target_rows (%s);', (SELECT string_agg(quote_ident(c), ', ') FROM unnest(v_original_entity_key_cols) AS c));
                v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);
            END IF;

            v_sql := format('CREATE INDEX ON target_rows USING gist (%s(valid_from, valid_until));', v_range_constructor);
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);

            v_sql := 'ANALYZE target_rows;';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);
    
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
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- CTE 5: source_rows_with_aggregates
            v_sql := $SQL$
                CREATE TEMP TABLE source_rows_with_aggregates ON COMMIT DROP AS
                SELECT
                    source_row_id,
                    count(DISTINCT discovered_stable_pk_payload) as match_count,
                    -- Filter out NULL to avoid [null] result for new entities with no matches
                    COALESCE(jsonb_agg(DISTINCT discovered_stable_pk_payload) FILTER (WHERE discovered_stable_pk_payload IS NOT NULL), '[]'::jsonb) as conflicting_ids
                FROM source_rows_with_matches
                GROUP BY source_row_id
            $SQL$;
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
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
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- CTE 6: source_rows
            v_sql := format($SQL$
                CREATE TEMP TABLE source_rows ON COMMIT DROP AS
                SELECT DISTINCT ON (p.source_row_id)
                    p.source_row_id, p.causal_id, p.valid_from, p.valid_until, p.data_payload, p.ephemeral_payload,
                    (p.stable_identity_columns_are_null AND p.discovered_stable_pk_payload IS NULL) as stable_identity_columns_are_null,
                    p.lookup_columns_unavailable, p.is_identifiable,
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
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- CTE 8: source_rows_with_new_flag
            v_sql := $SQL$
                CREATE TEMP TABLE source_rows_with_new_flag ON COMMIT DROP AS
                SELECT *, NOT target_entity_exists as is_new_entity
                FROM source_rows
            $SQL$;
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
            
            -- CTE 9.1: source_rows_with_nk_json
            v_sql := format($SQL$
                CREATE TEMP TABLE source_rows_with_nk_json ON COMMIT DROP AS
                SELECT source_row.*, %1$s /* v_lookup_keys_as_jsonb_expr */ as nk_json, %2$s /* v_lookup_keys_as_array_expr */ as nk_non_null_keys_array
                FROM source_rows_with_new_flag source_row
            $SQL$,
                v_lookup_keys_as_jsonb_expr, /* %1$s */
                v_lookup_keys_as_array_expr  /* %2$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);

            v_sql := 'CREATE INDEX ON source_rows_with_nk_json USING GIN (nk_json);';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);

            v_sql := 'ANALYZE source_rows_with_nk_json;';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);
    
            -- CTE 9.2: source_rows_with_canonical_key
            -- Optimization: When all new entities have uniform NK specificity (same non-null keys),
            -- each row's canonical_nk_json is simply its own nk_json. This avoids an O(n²) LATERAL
            -- join that would otherwise check each row against all others.
            -- The LATERAL is only needed when rows have different specificities (e.g., one row has
            -- NK {a:null, b:1} and another has {a:5, b:1} where the second is "more specific").
            v_sql := format($SQL$
                CREATE TEMP TABLE source_rows_with_canonical_key ON COMMIT DROP AS
                WITH nk_specificity_check AS (
                    SELECT count(DISTINCT nk_non_null_keys_array::text) = 1 AS is_uniform
                    FROM source_rows_with_nk_json
                    WHERE is_new_entity
                )
                SELECT s.*, (%1$s /* v_grouping_key_expr */) as grouping_key
                FROM (
                    SELECT
                        s1.*,
                        CASE 
                            -- Fast path: uniform specificity means each row is its own canonical
                            WHEN (SELECT is_uniform FROM nk_specificity_check) 
                            THEN s1.nk_json
                            -- Slow path: varied specificity requires finding most specific match
                            ELSE (
                                SELECT s2_inner.nk_json
                                FROM source_rows_with_nk_json s2_inner
                                WHERE s1.is_new_entity AND s2_inner.is_new_entity AND s2_inner.nk_json @> s1.nk_json
                                ORDER BY array_length(s2_inner.nk_non_null_keys_array, 1) DESC, s2_inner.nk_non_null_keys_array::text DESC
                                LIMIT 1
                            )
                        END as canonical_nk_json
                    FROM source_rows_with_nk_json s1
                ) s
            $SQL$,
                v_grouping_key_expr /* %1$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
            
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
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
            
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
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);

            IF v_original_entity_key_cols IS NOT NULL AND cardinality(v_original_entity_key_cols) > 0 THEN
                v_sql := format('CREATE INDEX ON active_source_rows (%s);', (SELECT string_agg(quote_ident(c), ', ') FROM unnest(v_original_entity_key_cols) AS c));
                v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);
            END IF;

            v_sql := 'CREATE INDEX ON active_source_rows (grouping_key);';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);

            v_sql := format('CREATE INDEX ON active_source_rows USING gist (%s(valid_from, valid_until));', v_range_constructor);
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);

            v_sql := 'ANALYZE active_source_rows;';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);
    
            -- CTE 10: all_rows
            v_sql := format($SQL$
                CREATE TEMP TABLE all_rows ON COMMIT DROP AS
                SELECT %1$s /* v_non_temporal_lookup_cols_select_list_no_alias_prefix */ causal_id, valid_from, valid_until, is_new_entity, stable_pk_payload, stable_identity_columns_are_null, lookup_columns_unavailable, is_identifiable, is_ambiguous, conflicting_ids, canonical_nk_json FROM active_source_rows
                UNION ALL
                SELECT
                    %2$s /* v_non_temporal_tr_qualified_lookup_cols_prefix */
                    target_row.causal_id,
                    target_row.valid_from,
                    target_row.valid_until,
                    false as is_new_entity,
                    target_row.stable_pk_payload,
                    false as stable_identity_columns_are_null,
                    false as lookup_columns_unavailable,
                    true as is_identifiable,
                    false as is_ambiguous,
                    NULL::jsonb as conflicting_ids,
                    target_row.canonical_nk_json
                FROM target_rows target_row
            $SQL$,
                v_non_temporal_lookup_cols_select_list_no_alias_prefix, /* %1$s */
                v_non_temporal_tr_qualified_lookup_cols_prefix          /* %2$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- CTE 11: time_points_raw
            v_sql := format($SQL$
                CREATE TEMP TABLE time_points_raw ON COMMIT DROP AS
                SELECT %1$s, causal_id, valid_from AS point, is_new_entity, stable_pk_payload, stable_identity_columns_are_null, lookup_columns_unavailable, is_identifiable, is_ambiguous, conflicting_ids, canonical_nk_json FROM all_rows
                UNION ALL
                SELECT %1$s, causal_id, valid_until AS point, is_new_entity, stable_pk_payload, stable_identity_columns_are_null, lookup_columns_unavailable, is_identifiable, is_ambiguous, conflicting_ids, canonical_nk_json FROM all_rows
            $SQL$,
                v_lookup_cols_select_list_no_alias /* %1$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
            
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
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
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
                    tpu.lookup_columns_unavailable,
                    tpu.is_identifiable,
                    tpu.is_ambiguous,
                    tpu.conflicting_ids,
                    tpu.unified_canonical_nk_json as canonical_nk_json
                FROM time_points_unified tpu
            $SQL$,
                v_unified_id_cols_projection /* %1$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- CTE 13: time_points
            v_sql := $SQL$
                CREATE TEMP TABLE time_points ON COMMIT DROP AS
                SELECT DISTINCT ON (grouping_key, point) *
                FROM time_points_with_unified_ids
                ORDER BY grouping_key, point, causal_id DESC NULLS LAST
            $SQL$;
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- CTE 14: atomic_segments
            v_sql := format($SQL$
                CREATE TEMP TABLE atomic_segments ON COMMIT DROP AS
                SELECT grouping_key, %1$s, causal_id, point as valid_from, next_point as valid_until, is_new_entity, stable_pk_payload, stable_identity_columns_are_null, lookup_columns_unavailable, is_identifiable, is_ambiguous, conflicting_ids, canonical_nk_json
                FROM (
                    SELECT *, LEAD(point) OVER (PARTITION BY grouping_key ORDER BY point) as next_point
                    FROM time_points
                ) with_lead
                WHERE point IS NOT NULL AND next_point IS NOT NULL AND point < next_point
            $SQL$,
                v_lookup_cols_select_list_no_alias /* %1$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);

            v_sql := 'CREATE INDEX ON atomic_segments (grouping_key);';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);

            v_sql := 'ANALYZE atomic_segments;';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);
            
            -- CTE 15: resolved_atomic_segments_with_payloads (Split-path optimization)
            -- Path 1: Existing entities (use Hash Join for better performance)
            v_sql := format($SQL$
                CREATE TEMP TABLE existing_segments_with_target ON COMMIT DROP AS
                SELECT
                    seg.grouping_key, seg.canonical_nk_json, %1$s, seg.is_new_entity, seg.is_identifiable, seg.is_ambiguous, seg.conflicting_ids, seg.stable_identity_columns_are_null, seg.lookup_columns_unavailable, seg.valid_from, seg.valid_until,
                    target_row.valid_from as t_valid_from, target_row.valid_until as t_valid_until,
                    target_row.data_payload as t_data_payload, target_row.ephemeral_payload as t_ephemeral_payload, seg.stable_pk_payload, 
                    target_row.stable_pk_payload as target_stable_pk_payload,
                    source_payloads.source_row_id, source_payloads.contributing_row_ids,
                    source_payloads.data_payload as s_data_payload, source_payloads.ephemeral_payload as s_ephemeral_payload,
                    source_payloads.valid_from AS s_valid_from, source_payloads.valid_until AS s_valid_until,
                    %2$s
                FROM atomic_segments seg
                LEFT JOIN target_rows target_row 
                    ON %3$s 
                    AND %4$I(seg.valid_from, seg.valid_until) <@ %4$I(target_row.valid_from, target_row.valid_until)
                %5$s
                WHERE NOT seg.is_new_entity
            $SQL$,
                v_entity_key_for_with_base_payload_expr, /* %1$s */
                v_trace_seed_expr,                       /* %2$s */
                v_lateral_join_tr_to_seg,                /* %3$s */
                v_range_constructor,                     /* %4$I */
                v_lateral_source_resolver_sql_existing   /* %5$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);

            -- Analyze existing_segments_with_target after creation
            v_sql := 'ANALYZE existing_segments_with_target;';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);

            -- Path 2: New entities (no target join needed)
            v_sql := format($SQL$
                CREATE TEMP TABLE new_segments_no_target ON COMMIT DROP AS
                SELECT
                    seg.grouping_key, seg.canonical_nk_json, %1$s, seg.is_new_entity, seg.is_identifiable, seg.is_ambiguous, seg.conflicting_ids, seg.stable_identity_columns_are_null, seg.lookup_columns_unavailable, seg.valid_from, seg.valid_until,
                    NULL::%3$s as t_valid_from, NULL::%3$s as t_valid_until,
                    NULL::jsonb as t_data_payload, NULL::jsonb as t_ephemeral_payload, seg.stable_pk_payload,
                    NULL::jsonb as target_stable_pk_payload,
                    source_payloads.source_row_id, source_payloads.contributing_row_ids,
                    source_payloads.data_payload as s_data_payload, source_payloads.ephemeral_payload as s_ephemeral_payload,
                    source_payloads.valid_from AS s_valid_from, source_payloads.valid_until AS s_valid_until,
                    %2$s
                FROM atomic_segments seg
                %4$s
                WHERE seg.is_new_entity
            $SQL$,
                v_entity_key_for_with_base_payload_expr, /* %1$s */
                v_trace_seed_expr,                       /* %2$s */
                v_range_subtype,                         /* %3$s */
                v_lateral_source_resolver_sql_new        /* %4$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);

            -- Analyze new_segments_no_target after creation
            v_sql := 'ANALYZE new_segments_no_target;';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);

            -- Path 3: Union and apply source resolution
            v_sql := format($SQL$
                CREATE TEMP TABLE resolved_atomic_segments_with_payloads ON COMMIT DROP AS
                WITH all_segments AS (
                    SELECT * FROM existing_segments_with_target
                    UNION ALL
                    SELECT * FROM new_segments_no_target
                )
                SELECT
                    seg.*,
                    seg.stable_pk_payload as propagated_stable_pk_payload
                FROM all_segments seg
                WHERE (seg.s_data_payload IS NOT NULL OR seg.t_data_payload IS NOT NULL)
                AND CASE %1$L::sql_saga.temporal_merge_mode
                    WHEN 'PATCH_FOR_PORTION_OF' THEN seg.t_data_payload IS NOT NULL
                    WHEN 'REPLACE_FOR_PORTION_OF' THEN seg.t_data_payload IS NOT NULL
                    WHEN 'DELETE_FOR_PORTION_OF' THEN seg.t_data_payload IS NOT NULL
                    WHEN 'UPDATE_FOR_PORTION_OF' THEN seg.t_data_payload IS NOT NULL
                    ELSE true
                END
            $SQL$,
                mode                                     /* %1$L */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);

            -- PERFORMANCE: Analyze temp table for accurate cardinality estimates
            -- This prevents O(n²) behavior in lateral joins by providing proper statistics
            v_sql := 'ANALYZE resolved_atomic_segments_with_payloads;';
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);
    
            v_resolver_from := 'resolved_atomic_segments_with_payloads';
    
            IF delete_mode IN ('DELETE_MISSING_ENTITIES', 'DELETE_MISSING_TIMELINE_AND_ENTITIES') THEN
                v_sql := format($$
                    CREATE TEMP TABLE resolved_atomic_segments_with_flag ON COMMIT DROP AS
                    SELECT *, bool_or(s_data_payload IS NOT NULL) OVER (PARTITION BY grouping_key) as entity_is_in_source
                    FROM %s
                $$, v_resolver_from);
                v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);

                -- PERFORMANCE: Analyze temp table for accurate cardinality estimates
                v_sql := 'ANALYZE resolved_atomic_segments_with_flag;';
                v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'setup', 'sql', v_sql);

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
                    ) as causal_source_row_ids,
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
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- CTE 16: resolved_atomic_segments
            v_sql := format($SQL$
                CREATE TEMP TABLE resolved_atomic_segments ON COMMIT DROP AS
                SELECT
                    grouping_key, %1$s stable_identity_columns_are_null, lookup_columns_unavailable, is_new_entity, is_identifiable, is_ambiguous, conflicting_ids, unified_canonical_nk_json,
                    valid_from, valid_until, t_valid_from, t_valid_until, propagated_s_valid_from, propagated_s_valid_until, causal_source_row_ids, causal_id, propagated_stable_pk_payload as stable_pk_payload,
                    causal_source_row_ids IS NULL AS unaffected_target_only_segment,
                    s_data_payload, t_data_payload,
                    sql_saga.get_allen_relation(propagated_s_valid_from, propagated_s_valid_until, t_valid_from, t_valid_until) AS orig_source_target_relation,
                    %2$s as data_payload,
                    md5((CASE WHEN pg_catalog.jsonb_path_exists(%2$s, 'strict $.*.**{1 to last} ? (@ == null)') THEN (%2$s) - ARRAY(SELECT sn.k FROM pg_catalog.jsonb_each(%2$s) AS sn(k, v) WHERE sn.v = 'null'::jsonb) ELSE pg_catalog.jsonb_strip_nulls(%2$s) END)::text) as data_hash,
                    CASE %4$s
                        WHEN s_data_payload IS NULL THEN t_ephemeral_payload
                        ELSE COALESCE(t_ephemeral_payload, '{}'::jsonb) || COALESCE(s_ephemeral_payload, '{}'::jsonb)
                    END as ephemeral_payload,
                    CASE WHEN %3$L::boolean
                        THEN trace || jsonb_build_object( 'cte', 'ras', 'segment_stable_pk', propagated_stable_pk_payload, 'final_data_payload', %2$s, 'final_ephemeral_payload', CASE WHEN s_data_payload IS NULL THEN t_ephemeral_payload ELSE COALESCE(t_ephemeral_payload, '{}'::jsonb) || COALESCE(s_ephemeral_payload, '{}'::jsonb) END )
                        ELSE NULL
                    END as trace
                FROM resolved_atomic_segments_with_propagated_ids
            $SQL$,
                v_lookup_cols_sans_valid_from_prefix, /* %1$s */
                v_final_data_payload_expr,            /* %2$s */
                p_log_trace,                          /* %3$L */
                -- Provenance columns (doc/design/provenance.md): a new piece, or one whose data differs from the piece
                -- it replaces, carries no provenance value, so the managed trigger fills it from the DEFAULT; an
                -- untouched piece keeps the target's. Generated only for an era that has them.
                CASE WHEN cardinality(v_provenance_columns) > 0 THEN format(
                    $$WHEN s_data_payload IS NOT NULL
                         AND (t_data_payload IS NULL OR (CASE WHEN pg_catalog.jsonb_path_exists(%1$s, 'strict $.*.**{1 to last} ? (@ == null)') THEN (%1$s) - ARRAY(SELECT sn.k FROM pg_catalog.jsonb_each(%1$s) AS sn(k, v) WHERE sn.v = 'null'::jsonb) ELSE pg_catalog.jsonb_strip_nulls(%1$s) END) IS DISTINCT FROM (CASE WHEN pg_catalog.jsonb_path_exists(t_data_payload, 'strict $.*.**{1 to last} ? (@ == null)') THEN (t_data_payload) - ARRAY(SELECT sn.k FROM pg_catalog.jsonb_each(t_data_payload) AS sn(k, v) WHERE sn.v = 'null'::jsonb) ELSE pg_catalog.jsonb_strip_nulls(t_data_payload) END))
                        THEN (COALESCE(t_ephemeral_payload, '{}'::jsonb) || COALESCE(s_ephemeral_payload, '{}'::jsonb)) - %2$L::text[]$$,
                    v_final_data_payload_expr, v_provenance_columns)
                ELSE '' END                           /* %4$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);

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
                            OR (prev_data_hash IS DISTINCT FROM data_hash)%1$s
                            THEN 1
                            ELSE 0
                        END as is_island_start
                    FROM (
                        SELECT
                            *,
                            LAG(valid_until) OVER w as prev_valid_until,
                            LAG(data_hash) OVER w as prev_data_hash,%2$s
                            LAG(data_payload) OVER w as prev_data_payload
                        FROM resolved_atomic_segments ras
                        WHERE ras.data_payload IS NOT NULL
                        WINDOW w AS (PARTITION BY grouping_key ORDER BY valid_from)
                    ) s1
                ) s2
            $SQL$;
            -- Pieces with different provenance never coalesce (doc/design/provenance.md, decision 3); only for an era
            -- that has provenance columns, so other plans are unchanged.
            v_sql := format(v_sql,
                CASE WHEN cardinality(v_provenance_columns) > 0
                     THEN E'\n                            OR (prev_provenance IS DISTINCT FROM provenance)' ELSE '' END,
                CASE WHEN cardinality(v_provenance_columns) > 0
                     THEN format($$
                            LAG(COALESCE((SELECT jsonb_object_agg(k, v) FROM jsonb_each(ras.ephemeral_payload) AS e(k, v) WHERE k = ANY (%1$L::text[]) AND v <> 'null'::jsonb), '{}'::jsonb)) OVER w as prev_provenance,
                            COALESCE((SELECT jsonb_object_agg(k, v) FROM jsonb_each(ras.ephemeral_payload) AS e(k, v) WHERE k = ANY (%1$L::text[]) AND v <> 'null'::jsonb), '{}'::jsonb) as provenance,$$,
                                 v_provenance_columns)
                     ELSE '' END);
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- CTE 17.5: coalesced_final_segments
            v_sql := format($SQL$
                CREATE TEMP TABLE coalesced_final_segments ON COMMIT DROP AS
                SELECT
                    grouping_key, %1$s,
                    sql_saga.first(causal_id ORDER BY valid_from) as causal_id,
                    sql_saga.first(stable_identity_columns_are_null ORDER BY valid_from) as stable_identity_columns_are_null,
                    sql_saga.first(lookup_columns_unavailable ORDER BY valid_from) as lookup_columns_unavailable,
                    sql_saga.first(is_new_entity ORDER BY valid_from) as is_new_entity,
                    sql_saga.first(is_identifiable ORDER BY valid_from) as is_identifiable,
                    sql_saga.first(is_ambiguous ORDER BY valid_from) as is_ambiguous,
                    sql_saga.first(conflicting_ids ORDER BY valid_from) as conflicting_ids,
                    sql_saga.first(unified_canonical_nk_json ORDER BY valid_from) as canonical_nk_json,
                    sql_saga.first(orig_source_target_relation ORDER BY valid_from) as orig_source_target_relation,
                    sql_saga.first(t_valid_from ORDER BY valid_from) as ancestor_valid_from,
                    MIN(valid_from) as valid_from,
                    MAX(valid_until) as valid_until,
                    %2$s as final_payload,
                    sql_saga.first(stable_pk_payload ORDER BY valid_from DESC) as stable_pk_payload,
                    bool_and(unaffected_target_only_segment) as unaffected_target_only_segment,
                    (SELECT array_agg(DISTINCT e) FROM unnest(array_concat_agg(causal_source_row_ids)) e WHERE e IS NOT NULL) as row_ids,
                    CASE WHEN %3$L::boolean
                        THEN jsonb_build_object( 'cte', 'coalesced', 'island_group_id', island_group_id, 'coalesced_stable_pk', sql_saga.first(stable_pk_payload ORDER BY valid_from DESC), 'final_payload', sql_saga.first(data_payload ORDER BY valid_from DESC), 'final_payload_sans_ephemeral', sql_saga.first(data_payload - %4$L::text[] ORDER BY valid_from DESC), 'atomic_traces', jsonb_agg((trace || jsonb_build_object('data_hash', data_hash, 'prev_data_hash', prev_data_hash, 'prev_data_payload', prev_data_payload)) ORDER BY valid_from) )
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
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
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
                    COALESCE(final_seg.lookup_columns_unavailable, false) as lookup_columns_unavailable,
                    final_seg.valid_from AS f_from, final_seg.valid_until AS f_until, final_seg.final_payload AS f_payload, final_seg.row_ids AS f_row_ids, final_seg.stable_pk_payload, final_seg.orig_source_target_relation,
                    CASE WHEN %2$L::boolean
                        THEN final_seg.trace || jsonb_build_object('cte', 'diff', 'diff_stable_pk', final_seg.stable_pk_payload, 'final_seg_causal_id', final_seg.causal_id, 'final_payload_vs_target_payload', jsonb_build_object('f', final_seg.final_payload, 't', target_seg.data_payload))
                        ELSE NULL
                    END as trace,
                    final_seg.unaffected_target_only_segment,
                    target_seg.valid_from as t_from, target_seg.valid_until as t_until, (target_seg.data_payload || target_seg.ephemeral_payload) as t_payload,
                    sql_saga.get_allen_relation(target_seg.valid_from, target_seg.valid_until, final_seg.valid_from, final_seg.valid_until) as b_a_relation
                FROM coalesced_final_segments AS final_seg
                FULL OUTER JOIN target_rows AS target_seg ON %3$s
            $SQL$,
                v_diff_select_expr,    /* %1$s */
                p_log_trace,           /* %2$L */
                v_diff_join_condition  /* %3$s */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
            
            -- CTE 19: diff_ranked
            v_sql := format($SQL$
                CREATE TEMP TABLE diff_ranked ON COMMIT DROP AS
                SELECT
                    d.*,
                    CASE
                        WHEN d.t_from IS NULL OR d.f_from IS NULL THEN NULL
                        WHEN d.f_from = d.t_from AND d.f_until = d.t_until AND d.f_payload IS NOT DISTINCT FROM d.t_payload THEN NULL
                        ELSE
                            row_number() OVER (
                                PARTITION BY d.grouping_key, d.t_from
                                ORDER BY
                                    CASE WHEN d.f_from = d.t_from THEN 1 ELSE 2 END,
                                    CASE WHEN d.f_payload - %1$L::text[] IS NOT DISTINCT FROM d.t_payload - %1$L::text[] THEN 1 ELSE 2 END,
                                    d.f_from,
                                    d.f_until
                            )
                    END as update_rank
                FROM diff d
            $SQL$,
                v_ephemeral_columns /* %1$L */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
            
            -- CTE 20: plan_with_op
            v_sql := format($SQL$
                CREATE TEMP TABLE plan_with_op ON COMMIT DROP AS
                (
                    SELECT * FROM (
                        SELECT
                            d.f_row_ids as row_ids, d.orig_source_target_relation, d.is_new_entity,
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
                                ELSE d.f_payload
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
                        WHERE d.f_row_ids IS NOT NULL OR d.t_payload IS NOT NULL
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
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- CTE 21: plan
            v_sql := format($SQL$
                CREATE TEMP TABLE plan ON COMMIT DROP AS
                SELECT
                    p.row_ids, p.operation, p.causal_id, p.is_new_entity,
                    %1$s,
                    p.entity_keys_json as entity_keys,
                    p.identity_keys, p.lookup_keys,
                    p.orig_source_target_relation, p.b_a_relation, p.old_valid_from, p.old_valid_until,
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
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
    
            -- Final SELECT
            -- Execution strategy: with_temporary_temporal_gaps
            -- Operations are ordered DELETE→UPDATE→INSERT to avoid temporary overlaps (would violate NOT DEFERRABLE unique constraints)
            -- while tolerating temporary gaps (allowed by DEFERRABLE foreign key constraints). This enables native PostgreSQL 18
            -- temporal foreign keys. Within UPDATE operations, the update_effect enum order (NONE→SHRINK→MOVE→GROW) further
            -- minimizes gap duration by contracting timelines before expanding them.
            --
            -- CRITICAL: MOVE operations must be ordered by old_valid_from DESC (later ranges first) to avoid temporal conflicts.
            -- When multiple MOVE operations affect the same entity, processing later ranges first ensures that
            -- the target range doesn't conflict with existing rows that haven't been moved yet.
            --
            -- statement_seq groups operations that can safely execute in a single SQL statement:
            -- - DELETE: all in one statement (deletions don't conflict)
            -- - UPDATE NONE/SHRINK: all in one statement (only contract ranges)
            -- - UPDATE MOVE: each gets its own statement (can create temporary overlaps)
            -- - UPDATE GROW: all in one statement (only expand into gaps)
            -- - INSERT: all in one statement (new ranges don't overlap by design)
            v_sql := format($SQL$
                WITH RECURSIVE ordered_plan AS (
                    SELECT
                        row_number() OVER ( ORDER BY p.grouping_key, %1$s, CASE p.operation WHEN 'DELETE' THEN 1 WHEN 'UPDATE' THEN 2 WHEN 'INSERT' THEN 3 ELSE 4 END, p.update_effect NULLS FIRST, CASE WHEN p.update_effect = 'MOVE' THEN 1 ELSE 0 END, CASE WHEN p.update_effect = 'MOVE' THEN COALESCE(p.old_valid_from, p.new_valid_from) END DESC NULLS LAST, COALESCE(p.old_valid_from, p.new_valid_from), p.new_valid_from, (p.row_ids[1]) )::BIGINT as plan_op_seq,
                        p.*
                    FROM plan p
                ),
                -- For MOVE operations, group into the largest possible batches that can execute together.
                -- Within a single UPDATE statement, all rows see the pre-update state (MVCC snapshot).
                -- So if MOVE A's new_range overlaps MOVE B's old_range, they cannot be in the same statement.
                --
                -- Algorithm: Process MOVEs in plan_op_seq order (which is old_valid_from DESC).
                -- Track a multirange of old_ranges in the current batch. For each MOVE:
                -- - If new_range overlaps the batch's multirange, start a new batch
                -- - Add this MOVE's old_range to the batch's multirange
                --
                -- We use a recursive CTE to accumulate the multirange with reset-on-conflict logic.
                moves_ordered AS (
                    SELECT
                        o.plan_op_seq,
                        o.grouping_key,
                        o.old_valid_from,
                        o.old_valid_until,
                        o.new_valid_from,
                        o.new_valid_until,
                        row_number() OVER (PARTITION BY o.grouping_key ORDER BY o.plan_op_seq) AS move_seq
                    FROM ordered_plan o
                    WHERE o.operation = 'UPDATE' AND o.update_effect = 'MOVE'
                ),
                moves_with_batch AS (
                    -- Base case: first MOVE in each entity starts batch 0
                    SELECT 
                        plan_op_seq, grouping_key, old_valid_from, old_valid_until, 
                        new_valid_from, new_valid_until, move_seq,
                        0::bigint AS move_batch,
                        %2$s(%3$s(old_valid_from, old_valid_until, '[)')) AS batch_old_ranges
                    FROM moves_ordered
                    WHERE move_seq = 1
                    
                    UNION ALL
                    
                    -- Recursive case: check if new MOVE conflicts with batch's multirange
                    SELECT
                        m.plan_op_seq, m.grouping_key, m.old_valid_from, m.old_valid_until,
                        m.new_valid_from, m.new_valid_until, m.move_seq,
                        CASE 
                            WHEN %3$s(m.new_valid_from, m.new_valid_until, '[)') && prev.batch_old_ranges
                            THEN prev.move_batch + 1  -- Conflict: start new batch
                            ELSE prev.move_batch      -- No conflict: same batch
                        END AS move_batch,
                        CASE 
                            WHEN %3$s(m.new_valid_from, m.new_valid_until, '[)') && prev.batch_old_ranges
                            THEN %2$s(%3$s(m.old_valid_from, m.old_valid_until, '[)'))  -- Reset multirange
                            ELSE prev.batch_old_ranges + %2$s(%3$s(m.old_valid_from, m.old_valid_until, '[)'))  -- Add to multirange
                        END AS batch_old_ranges
                    FROM moves_ordered m
                    JOIN moves_with_batch prev 
                      ON prev.grouping_key = m.grouping_key 
                     AND prev.move_seq = m.move_seq - 1
                ),
                with_statement_seq AS (
                    SELECT
                        o.plan_op_seq,
                        CASE
                            WHEN o.operation = 'DELETE' THEN 1
                            WHEN o.operation = 'UPDATE' AND o.update_effect IN ('NONE', 'SHRINK') THEN 2
                            WHEN o.operation = 'UPDATE' AND o.update_effect = 'MOVE' THEN
                                -- Base of 3 for first MOVE batch, +1 for each subsequent batch
                                3 + COALESCE(mb.move_batch, 0)
                            WHEN o.operation = 'UPDATE' AND o.update_effect = 'GROW' THEN 1000000  -- Will be renumbered
                            WHEN o.operation = 'INSERT' THEN 2000000  -- Will be renumbered
                            ELSE 3000000
                        END::INT as raw_statement_seq,
                        o.row_ids, o.operation, o.update_effect, o.causal_id::TEXT, o.is_new_entity, o.entity_keys, o.identity_keys, o.lookup_keys, o.orig_source_target_relation, o.b_a_relation, o.old_valid_from::TEXT,
                        o.old_valid_until::TEXT, o.new_valid_from::TEXT, o.new_valid_until::TEXT, o.data, o.feedback, CASE WHEN o.trace IS NOT NULL THEN o.trace || jsonb_build_object('final_grouping_key', o.grouping_key) ELSE NULL END as trace, o.grouping_key
                    FROM ordered_plan o
                    LEFT JOIN moves_with_batch mb ON mb.plan_op_seq = o.plan_op_seq
                )
                SELECT
                    plan_op_seq,
                    dense_rank() OVER (ORDER BY raw_statement_seq)::INT as statement_seq,
                    row_ids, operation, update_effect, causal_id, is_new_entity, entity_keys, identity_keys, lookup_keys, orig_source_target_relation, b_a_relation, old_valid_from,
                    old_valid_until, new_valid_from, new_valid_until,
                    -- New range columns
                    CASE 
                        WHEN old_valid_from IS NOT NULL AND old_valid_until IS NOT NULL 
                        THEN %3$s(old_valid_from::%4$s, old_valid_until::%5$s, '[)')::TEXT
                        ELSE NULL 
                    END as old_valid_range,
                    CASE 
                        WHEN new_valid_from IS NOT NULL AND new_valid_until IS NOT NULL 
                        THEN %3$s(new_valid_from::%4$s, new_valid_until::%5$s, '[)')::TEXT
                        ELSE NULL 
                    END as new_valid_range,
                    data, feedback, trace, grouping_key
                FROM with_statement_seq
                ORDER BY plan_op_seq;
            $SQL$,
                v_final_order_by_expr, /* %1$s */
                v_multirange_type, /* %2$s - multirange type */
                v_range_constructor, /* %3$s - range type */
                v_range_subtype, /* %4$s - range subtype for from cast */
                v_range_subtype /* %5$s - range subtype for until cast */
            );
            v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'query', 'sql', v_sql);
    
            -- Conditionally log the generated SQL for debugging.
            BEGIN
                IF p_log_sql THEN
                    RAISE NOTICE '--- temporal_merge SQL for % ---', temporal_merge_plan.target_table;
                    RAISE NOTICE '%', v_sql;
                END IF;
            END;
    
            -- Store in the session cache with actual table names
            INSERT INTO pg_temp.temporal_merge_plan_cache (cache_key, plan_sqls)
            VALUES (v_plan_key_text, v_plan_sqls);
        END;
    END IF; -- IF v_plan_sqls IS NULL THEN -- Full cache miss

    -- Delete any temp tables that are created by the prepared statments.
    CALL sql_saga.temporal_merge_drop_temp_tables();
    -- Execute all statements in the plan.
    DECLARE
        v_step JSONB;
        v_step_sql TEXT;
        v_final_query TEXT;
        v_step_start TIMESTAMPTZ;
        v_step_num INT := 0;
        v_step_ms NUMERIC;
        v_log_timing BOOLEAN;
    BEGIN
        -- Check if timing logging is enabled via GUC
        v_log_timing := COALESCE(current_setting('sql_saga.log_step_timing', true), 'off') = 'on';
        
        FOR v_step IN SELECT * FROM jsonb_array_elements(v_plan_sqls)
        LOOP
            v_step_num := v_step_num + 1;
            v_step_start := clock_timestamp();
            
            -- Get the SQL for this step (L1 cache has fully-substituted SQL)
            v_step_sql := v_step->>'sql';
            
            IF v_step->>'type' = 'query' THEN
                v_final_query := v_step_sql;
            ELSE
                EXECUTE v_step_sql;
            END IF;
            
            IF v_log_timing THEN
                v_step_ms := EXTRACT(EPOCH FROM clock_timestamp() - v_step_start) * 1000;
                IF v_step_ms > 5 THEN  -- Only log steps taking > 5ms
                    RAISE NOTICE 'Step % (%ms): %', v_step_num, round(v_step_ms, 1), left(v_step_sql, 100);
                END IF;
            END IF;
        END LOOP;

        RETURN QUERY EXECUTE v_final_query;
    END;
END;
$temporal_merge_plan$;
/*
 * sql_saga 1.12 update: data repairs that need the 1.12 definitions. None: rows written by the PL/pgSQL planner under
 * MERGE_ENTITY_PATCH lost nulls nested inside jsonb values, which cannot be told apart from values written that way.
 */
SELECT sql_saga.check_integrity();
