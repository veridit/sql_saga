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
    v_valid_from_col_type regtype;
    v_valid_until_col_type regtype;
    v_insert_defaulted_columns TEXT[];
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
BEGIN
    v_log_trace := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.enable_trace', true), ''), 'false')::boolean;
    v_log_sql := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_sql', true), ''), 'false')::boolean;
    v_log_index_checks := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_index_checks', true), ''), 'false')::boolean;
    v_log_feedback := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_feedback', true), ''), 'false')::boolean;
    v_log_vars := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_vars', true), ''), 'false')::boolean;
    v_log_execute := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_execute', true), ''), 'false')::boolean;
    v_log_id := substr(md5(COALESCE(current_setting('sql_saga.temporal_merge.log_id_seed', true), random()::text)), 1, 3);

    -- Identity and lookup columns are assumed to have been discovered and validated by the main temporal_merge procedure.
    v_lookup_columns := COALESCE(temporal_merge_execute.lookup_columns, temporal_merge_execute.identity_columns);
    v_causal_col := COALESCE(temporal_merge_execute.founding_id_column, temporal_merge_execute.row_id_column);

    -- Introspect columns that are NOT NULL and have a DEFAULT. For these columns,
    -- an incoming NULL in UPSERT mode should preserve the existing value rather
    -- than cause a NOT NULL violation.
    SELECT COALESCE(array_agg(a.attname), '{}')
    INTO v_not_null_defaulted_cols
    FROM pg_catalog.pg_attribute a
    WHERE a.attrelid = temporal_merge_execute.target_table
      AND a.attnum > 0 AND NOT a.attisdropped AND a.atthasdef AND a.attnotnull;
    
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

    -- Introspect the primary key columns. They will be excluded from UPDATE SET clauses.
    SELECT COALESCE(array_agg(a.attname), '{}'::name[])
    INTO v_pk_cols
    FROM pg_constraint c
    JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey)
    WHERE c.conrelid = temporal_merge_execute.target_table AND c.contype = 'p';

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

    -- Introspect era information to get the correct column names
    SELECT n.nspname, c.relname
    INTO v_target_schema_name, v_target_table_name_only
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = temporal_merge_execute.target_table;

    SELECT e.valid_from_column_name, e.valid_until_column_name, e.synchronize_valid_to_column, e.synchronize_range_column, e.range_type::name
    INTO v_valid_from_col, v_valid_until_col, v_valid_to_col, v_range_col, v_range_constructor
    FROM sql_saga.era e
    WHERE e.table_schema = v_target_schema_name
      AND e.table_name = v_target_table_name_only
      AND e.era_name = temporal_merge_execute.era_name;

    IF v_valid_from_col IS NULL THEN
        RAISE EXCEPTION 'No era named "%" found for table "%"', temporal_merge_execute.era_name, temporal_merge_execute.target_table;
    END IF;

    -- Prepare expected normalized index expressions and logging flag for index checks
    v_expected_idx_expr_with_bounds := format('%s(%s,%s,''[)'')', v_range_constructor, v_valid_from_col, v_valid_until_col);
    v_expected_idx_expr_default := format('%s(%s,%s)', v_range_constructor, v_valid_from_col, v_valid_until_col);

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
                RAISE WARNING 'Could not determine the base table for source relation "%" (it may be a complex view). Skipping GIST index performance check.', temporal_merge_execute.source_table;
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

    -- Auto-detect columns that should be excluded from INSERT statements.
    -- This includes columns with defaults, identity columns, and generated columns.
    SELECT COALESCE(array_agg(a.attname), '{}')
    INTO v_insert_defaulted_columns
    FROM pg_catalog.pg_attribute a
    WHERE a.attrelid = temporal_merge_execute.target_table
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

    SELECT atttypid::regtype INTO v_valid_from_col_type FROM pg_attribute WHERE attrelid = temporal_merge_execute.target_table AND attname = v_valid_from_col;
    SELECT atttypid::regtype INTO v_valid_until_col_type FROM pg_attribute WHERE attrelid = temporal_merge_execute.target_table AND attname = v_valid_until_col;

    -- Dynamically construct join clause for composite entity key.
    -- This uses an index-friendly, null-safe pattern.
    SELECT
        string_agg(format('(t.%1$I = jpr_entity.%1$I OR (t.%1$I IS NULL AND jpr_entity.%1$I IS NULL))', col), ' AND ')
    INTO
        v_entity_key_join_clause
    FROM unnest(identity_columns) AS col;

    v_entity_key_join_clause := COALESCE(v_entity_key_join_clause, 'true');


        -- Get dynamic column lists for DML. The data columns are defined as all columns
        -- in the target table, minus the identity and temporal boundary columns.
        -- This is intentionally different from the planner's introspection, as the executor
        -- must be able to handle a final payload that contains columns inherited from
        -- the target's history, which may not be present in the source.
        WITH target_cols AS (
            SELECT pa.attname, pa.atttypid, pa.attgenerated, pa.attidentity, pa.attnum
            FROM pg_catalog.pg_attribute pa
            WHERE pa.attrelid = temporal_merge_execute.target_table AND pa.attnum > 0 AND NOT pa.attisdropped
        ),
        common_data_cols AS (
            SELECT t.attname, t.atttypid
            FROM target_cols t
            LEFT JOIN pg_attrdef ad ON ad.adrelid = temporal_merge_execute.target_table AND ad.adnum = t.attnum
            WHERE t.attname NOT IN (v_valid_from_col, v_valid_until_col)
              AND t.attname <> ALL(COALESCE(temporal_merge_execute.identity_columns, '{}'))
              AND t.attname <> ALL(v_lookup_columns)
              AND t.attname <> ALL(COALESCE(v_pk_cols, '{}'))
              AND t.attidentity <> 'a' -- Exclude GENERATED ALWAYS AS IDENTITY
              AND t.attgenerated = '' -- Exclude GENERATED ... STORED
              AND COALESCE(pg_get_expr(ad.adbin, temporal_merge_execute.target_table), '') NOT ILIKE 'nextval(%'
        ),
        all_available_cols AS (
            SELECT c.attname, c.atttypid FROM common_data_cols c
            UNION
            SELECT u.attname, t.atttypid
            FROM unnest(v_lookup_columns) u(attname)
            JOIN target_cols t ON u.attname = t.attname
            UNION
            SELECT u.attname, t.atttypid
            FROM unnest(COALESCE(temporal_merge_execute.identity_columns, '{}')) u(attname)
            JOIN target_cols t ON u.attname = t.attname
            UNION
            SELECT pk.attname, t.atttypid
            FROM unnest(v_pk_cols) pk(attname)
            JOIN target_cols t ON pk.attname = t.attname
        ),
        cols_for_insert AS (
            -- All available columns that DON'T have a default...
            SELECT attname, atttypid FROM all_available_cols WHERE attname <> ALL(v_insert_defaulted_columns)
            UNION
            -- ...plus all identity and lookup columns and primary key columns, which must be provided for SCD-2 inserts.
            SELECT attname, atttypid FROM all_available_cols WHERE attname = ANY(COALESCE(temporal_merge_execute.identity_columns, '{}')) OR attname = ANY(v_lookup_columns) OR attname = ANY(v_pk_cols)
        ),
        cols_for_founding_insert AS (
            -- For "founding" INSERTs of new entities, we only include columns that do NOT have a default.
            -- This allows serial/identity columns to be generated by the database.
            SELECT attname, atttypid
            FROM all_available_cols
            WHERE attname <> ALL(v_insert_defaulted_columns)
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
                        THEN format('COALESCE((p.data->>%1$L)::%2$s, t.%3$I)', cdc.attname, format_type(cdc.atttypid, -1), cdc.attname)
                        ELSE format('(p.data->>%1$L)::%2$s', cdc.attname, format_type(cdc.atttypid, -1))
                    END
                ),
            ', ') FROM common_data_cols cdc),
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

        -- INSERT -> UPDATE -> DELETE order is critical for sql_saga compatibility.
        DECLARE
            v_deferrable_constraints name[];
            v_set_constraints_sql text;
            v_old_search_path TEXT;
        BEGIN
            IF delay_constraints THEN
                -- For timeline-splitting operations, the INSERT->UPDATE DML order creates
                -- a temporary timeline overlap. To allow this, we must temporarily defer
                -- any deferrable unique or exclusion constraints on the target table.
                -- This requires managing the search_path to ensure constraints can be found.
                v_old_search_path := current_setting('search_path');
                EXECUTE format('SET search_path = %I, public', v_target_schema_name);

                SELECT array_agg(conname)
                INTO v_deferrable_constraints
                FROM pg_constraint
                WHERE conrelid = target_table
                  AND contype IN ('u', 'x') -- u = unique, x = exclusion
                  AND condeferrable;

                IF v_deferrable_constraints IS NOT NULL THEN
                    v_set_constraints_sql := format('SET CONSTRAINTS %s DEFERRED',
                        (SELECT string_agg(quote_ident(c), ', ') FROM unnest(v_deferrable_constraints) AS c)
                    );
                    IF v_log_execute THEN RAISE NOTICE '(%) Executing: %', v_log_id, v_set_constraints_sql; END IF;
                    EXECUTE v_set_constraints_sql;
                END IF;
            END IF;

            -- This block contains the DML. We wrap it to ensure constraints and search_path are reset.
            BEGIN
                IF v_log_execute THEN
                    RAISE NOTICE '(%) --- temporal_merge_execute: EXECUTING INSERTS ---', v_log_id;
                END IF;
                -- 1. Execute INSERT operations and capture generated IDs
                IF v_all_cols_ident IS NOT NULL THEN
                     DECLARE
                        v_entity_id_update_jsonb_build TEXT;
                        v_needs_founding_insert BOOLEAN;
                     BEGIN
                        EXECUTE 'SELECT EXISTS(SELECT 1 FROM temporal_merge_plan WHERE operation = ''INSERT'' AND is_new_entity)'
                        INTO v_needs_founding_insert;

                        -- Build the expression to construct the entity_keys feedback JSONB.
                        -- This should include the conceptual entity ID columns AND any surrogate key.
                        -- A simple and effective heuristic is to always include a column named 'id' if it exists on the target table.
                        WITH target_cols AS (
                            SELECT pa.attname
                            FROM pg_catalog.pg_attribute pa
                            WHERE pa.attrelid = v_target_table_ident::regclass
                              AND pa.attnum > 0 AND NOT pa.attisdropped
                        ),
                        feedback_id_cols AS (
                            -- For back-filling, we care about the IDENTITY columns AND any surrogate key.
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
                        -- This unified "Smart Merge" logic is a critical part of the executor. It handles
                        -- the complex case where new entities need to be created and have their database-generated
                        -- identifiers (e.g., from a SERIAL column) captured and back-filled into all
                        -- other plan operations for that same new entity, all within a single, set-based operation.
                        IF v_needs_founding_insert THEN
                            -- This temporary table acts as a map to store the generated ID for each new
                            -- conceptual entity, keyed by its grouping_key.
                            CREATE TEMP TABLE temporal_merge_entity_id_map (grouping_key TEXT PRIMARY KEY, causal_id TEXT, new_entity_keys JSONB) ON COMMIT DROP;

                            -- Step 1.1: Insert just ONE "founding" row for each new conceptual entity to generate its ID.
                            -- The `MERGE ... ON false` pattern is a robust way to perform a bulk INSERT that needs
                            -- to return columns from both the source data (`s`) and the newly inserted target
                            -- rows (`t`), which a standard `INSERT ... SELECT ... RETURNING` cannot do as easily.
                            EXECUTE format($$
                                WITH founding_plan_ops AS (
                                    SELECT DISTINCT ON (p.grouping_key)
                                        p.plan_op_seq,
                                        p.causal_id,
                                        p.grouping_key,
                                        p.new_valid_from,
                                        p.new_valid_until,
                                        p.entity_keys || p.data as full_data
                                    FROM temporal_merge_plan p
                                    WHERE p.operation = 'INSERT' AND p.is_new_entity
                                    ORDER BY p.grouping_key, p.plan_op_seq
                                ),
                                id_map_cte AS (
                                    MERGE INTO %1$s t
                                    USING founding_plan_ops s ON false
                                    WHEN NOT MATCHED THEN
                                        INSERT (%2$s, %5$I, %6$I)
                                        VALUES (%3$s, s.new_valid_from::%8$s, s.new_valid_until::%9$s)
                                    RETURNING t.*, s.causal_id, s.grouping_key
                                )
                                INSERT INTO temporal_merge_entity_id_map (grouping_key, causal_id, new_entity_keys)
                                SELECT
                                    ir.grouping_key,
                                    ir.causal_id,
                                    %4$s -- v_entity_id_update_jsonb_build expression
                                FROM id_map_cte ir;
                            $$,
                                v_target_table_ident,           /* %1$s */
                                v_founding_all_cols_ident,      /* %2$s */
                                v_founding_all_cols_from_jsonb, /* %3$s */
                                v_entity_id_update_jsonb_build, /* %4$s */
                                v_valid_from_col,               /* %5$I */
                                v_valid_until_col,              /* %6$I */
                                NULL,                           /* %7$L (placeholder) */
                                v_valid_from_col_type,          /* %8$s */
                                v_valid_until_col_type          /* %9$s */
                            );

                            -- Step 1.2: Back-fill the captured, generated IDs into the plan for all other
                            -- operations that belong to the same new entity.
                            EXECUTE format($$
                                UPDATE temporal_merge_plan p
                                SET entity_keys = p.entity_keys || m.new_entity_keys
                                FROM temporal_merge_entity_id_map m
                                WHERE p.grouping_key = m.grouping_key;
                            $$);

                            -- Step 1.3: Insert the remaining historical slices for the new entities. These
                            -- slices now have the correct, generated entity ID (e.g., foreign key),
                            -- which was back-filled into their `entity_keys` payload in the previous step.
                            EXECUTE format($$
                                INSERT INTO %1$s (%2$s, %4$I, %5$I)
                                SELECT %3$s, p.new_valid_from::%7$s, p.new_valid_until::%8$s
                                FROM temporal_merge_plan p,
                                     LATERAL jsonb_populate_record(null::%1$s, p.entity_keys || p.data) as jpr_all
                                WHERE p.operation = 'INSERT'
                                  AND p.is_new_entity -- Only founding inserts
                                  AND p.causal_id IS NOT NULL
                                  AND NOT EXISTS ( -- Exclude the "founding" rows we already inserted in Step 1.1
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
                                v_valid_from_col,           /* %4$I */
                                v_valid_until_col,          /* %5$I */
                                NULL,                       /* %6$L (placeholder) */
                                v_valid_from_col_type,      /* %7$s */
                                v_valid_until_col_type      /* %8$s */
                            );

                            DROP TABLE temporal_merge_entity_id_map;
                        END IF;

                        -- Stage 2: Handle "non-founding" inserts. These are typically for SCD-2 style
                        -- history, where a new historical record is inserted for an entity that already
                        -- exists in the target table. These operations already have the correct stable
                        -- identifier and do not need any keys to be generated.
                        BEGIN
                            EXECUTE format($$
                                WITH
                                source_for_insert AS (
                                    SELECT
                                        p.plan_op_seq, p.new_valid_from, p.new_valid_until,
                                        p.entity_keys || p.data as full_data
                                    FROM temporal_merge_plan p
                                    WHERE p.operation = 'INSERT' AND NOT p.is_new_entity
                                ),
                                inserted_rows AS (
                                    MERGE INTO %1$s t
                                    USING source_for_insert s ON false
                                    WHEN NOT MATCHED THEN
                                        INSERT (%2$s, %3$I, %4$I)
                                        VALUES (%5$s, s.new_valid_from::%6$s, s.new_valid_until::%7$s)
                                    RETURNING t.*, s.plan_op_seq
                                )
                                UPDATE temporal_merge_plan p
                                SET entity_keys = p.entity_keys || %8$s
                                FROM inserted_rows ir
                                WHERE p.plan_op_seq = ir.plan_op_seq;
                            $$,
                                v_target_table_ident,               /* %1$s */
                                v_all_cols_ident,                   /* %2$s */
                                v_valid_from_col,                   /* %3$I */
                                v_valid_until_col,                  /* %4$I */
                                v_all_cols_from_jsonb,              /* %5$s */
                                v_valid_from_col_type,              /* %6$s */
                                v_valid_until_col_type,             /* %7$s */
                                v_entity_id_update_jsonb_build      /* %8$s */
                            );
                        END;
                     END;
                ELSE
                    -- This case handles tables with only temporal and defaulted ID columns.
                     DECLARE
                        v_entity_id_update_jsonb_build TEXT;
                     BEGIN
                        -- Build the expression to construct the entity_keys feedback JSONB.
                        -- This should include the conceptual entity ID columns AND any surrogate key.
                        -- A simple and effective heuristic is to always include a column named 'id' if it exists on the target table.
                        WITH target_cols AS (
                            SELECT pa.attname
                            FROM pg_catalog.pg_attribute pa
                            WHERE pa.attrelid = v_target_table_ident::regclass
                              AND pa.attnum > 0 AND NOT pa.attisdropped
                        ),
                        feedback_id_cols AS (
                            -- For back-filling, we care about the IDENTITY columns AND any surrogate key.
                            SELECT col FROM unnest(COALESCE(temporal_merge_execute.identity_columns, '{}')) as col
                            UNION
                            SELECT pk_col FROM unnest(v_pk_cols) as pk_col WHERE pk_col IN (SELECT attname FROM target_cols) AND pk_col NOT IN (v_valid_from_col, v_valid_until_col)
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
                            SET entity_keys = p.entity_keys || %2$s
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
                IF temporal_merge_execute.update_source_with_identity THEN
                    DECLARE
                        v_source_update_set_clause TEXT;
                    BEGIN
                        -- Build a SET clause for the identity columns. This writes back
                        -- any generated surrogate keys to the source table, but correctly
                        -- excludes any lookup key columns.
                        SELECT string_agg(
                            format('%I = (p.entity_keys->>%L)::%s', j.key, j.key, format_type(a.atttypid, a.atttypmod)),
                            ', '
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
                            v_sql := format($$
                                WITH map_row_to_entity AS (
                                    SELECT DISTINCT ON (s.source_row_id)
                                        s.source_row_id,
                                        p.entity_keys
                                    FROM (SELECT DISTINCT unnest(row_ids) AS source_row_id FROM temporal_merge_plan WHERE operation = 'INSERT') s
                                    JOIN temporal_merge_plan p ON s.source_row_id = ANY(p.row_ids)
                                    WHERE p.entity_keys IS NOT NULL
                                    ORDER BY s.source_row_id, p.plan_op_seq
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

                IF v_log_execute THEN
                    RAISE NOTICE '(%) --- temporal_merge_execute: EXECUTING UPDATES ---', v_log_id;
                END IF;
                -- 2. Execute UPDATE operations.
                -- As proven by test 58, we can use a single, ordered UPDATE statement.
                -- The ORDER BY on the plan's sequence number ensures that "grow"
                -- operations are processed before "shrink" or "move" operations,
                -- preventing transient gaps that would violate foreign key constraints.
                IF v_update_set_clause IS NOT NULL THEN
                    v_sql := format($$ UPDATE %1$s t SET %4$I = p.new_valid_from::%6$s, %5$I = p.new_valid_until::%7$s, %2$s
                        FROM (SELECT * FROM temporal_merge_plan WHERE operation = 'UPDATE' ORDER BY plan_op_seq) p,
                             LATERAL jsonb_populate_record(null::%1$s, p.entity_keys) AS jpr_entity
                        WHERE %3$s AND t.%4$I = p.old_valid_from::%6$s;
                    $$, v_target_table_ident, v_update_set_clause, v_entity_key_join_clause, v_valid_from_col, v_valid_until_col, v_valid_from_col_type, v_valid_until_col_type);
                    EXECUTE v_sql;
                ELSIF v_all_cols_ident IS NOT NULL THEN
                    v_sql := format($$ UPDATE %1$s t SET %3$I = p.new_valid_from::%5$s, %4$I = p.new_valid_until::%6$s
                        FROM (SELECT * FROM temporal_merge_plan WHERE operation = 'UPDATE' ORDER BY plan_op_seq) p,
                             LATERAL jsonb_populate_record(null::%1$s, p.entity_keys) AS jpr_entity
                        WHERE %2$s AND t.%3$I = p.old_valid_from::%5$s;
                    $$, v_target_table_ident, v_entity_key_join_clause, v_valid_from_col, v_valid_until_col, v_valid_from_col_type, v_valid_until_col_type);
                    EXECUTE v_sql;
                END IF;

                IF v_log_execute THEN
                    RAISE NOTICE '(%) --- temporal_merge_execute: EXECUTING DELETES ---', v_log_id;
                END IF;
                -- 3. Execute DELETE operations
                IF (SELECT TRUE FROM temporal_merge_plan WHERE operation = 'DELETE' LIMIT 1) THEN
                    v_sql := format($$ DELETE FROM %1$s t
                        USING temporal_merge_plan p, LATERAL jsonb_populate_record(null::%1$s, p.entity_keys) AS jpr_entity
                        WHERE p.operation = 'DELETE' AND %2$s AND t.%3$I = p.old_valid_from::%4$s;
                    $$, v_target_table_ident, v_entity_key_join_clause, v_valid_from_col, v_valid_from_col_type);
                    EXECUTE v_sql;
                END IF;
            EXCEPTION
                WHEN OTHERS THEN
                    -- If an error occurs, ensure constraints and search_path are reset before re-raising.
                    IF delay_constraints THEN
                        IF v_deferrable_constraints IS NOT NULL THEN
                            v_set_constraints_sql := format('SET CONSTRAINTS %s IMMEDIATE',
                                (SELECT string_agg(quote_ident(c), ', ') FROM unnest(v_deferrable_constraints) AS c)
                            );
                            IF v_log_execute THEN RAISE NOTICE '(%) Executing on error: %', v_log_id, v_set_constraints_sql; END IF;
                            EXECUTE v_set_constraints_sql;
                        END IF;
                        EXECUTE format('SET search_path = %s', v_old_search_path);
                    END IF;
                    RAISE;
            END;

            -- Restore constraints and search_path on successful completion.
            IF delay_constraints THEN
                IF v_deferrable_constraints IS NOT NULL THEN
                    v_set_constraints_sql := format('SET CONSTRAINTS %s IMMEDIATE',
                        (SELECT string_agg(quote_ident(c), ', ') FROM unnest(v_deferrable_constraints) AS c)
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

COMMENT ON PROCEDURE sql_saga.temporal_merge_execute(regclass, regclass, TEXT[], sql_saga.temporal_merge_mode, name, name, name, boolean, text[], sql_saga.temporal_merge_delete_mode, boolean, name, name, name, name, text[], boolean) IS
'Executes a temporal merge plan that is assumed to exist in a temporary table `temporal_merge_plan`.';

