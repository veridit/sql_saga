\echo Use "ALTER EXTENSION sql_saga UPDATE TO '1.15'" to load this file. \quit
CREATE OR REPLACE FUNCTION sql_saga.native_library_version() RETURNS text LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT AS 'sql_saga_native', 'sql_saga_native_version_wrapper';
DO $native_guard_1_15$
BEGIN
    IF (SELECT pg_catalog.array_agg(x::int) FROM pg_catalog.unnest(pg_catalog.string_to_array(sql_saga.native_library_version(), '.')) AS x) < (SELECT pg_catalog.array_agg(x::int) FROM pg_catalog.unnest(pg_catalog.string_to_array('1.15', '.')) AS x) THEN
        RAISE EXCEPTION 'sql_saga: native library version % does not match extension update version 1.15', sql_saga.native_library_version()
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Install the sql_saga 1.15 native library and restart PostgreSQL before retrying ALTER EXTENSION.';
    END IF;
END;
$native_guard_1_15$;
SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('sql_saga 1.15 upgrade'));
CREATE OR REPLACE FUNCTION sql_saga.for_portion_of_trigger()
 RETURNS trigger
 LANGUAGE plpgsql
AS $for_portion_of_trigger$
#variable_conflict use_variable
DECLARE
    info record;
    jnew jsonb;
    jold jsonb;
    identifier_columns text[];
    temp_source_table_name text;
    delete_portion boolean := false;
    merge_mode sql_saga.temporal_merge_mode := 'UPDATE_FOR_PORTION_OF';
BEGIN
    -- This view only supports UPDATE operations
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'sql_saga: DELETE is not supported on a "for_portion_of" view.'
            USING ERRCODE = 'feature_not_supported',
                  HINT = 'Use UPDATE with an identifier column set to NULL and the requested temporal bounds. Omit bounds to remove the complete matched history.';
    ELSIF TG_OP <> 'UPDATE' THEN
        RAISE EXCEPTION 'sql_saga: The "for_portion_of" view only supports UPDATE operations. For INSERTs or DELETEs, please use the base table directly.';
    END IF;

    -- Get metadata about the view's underlying table.
    -- Resolution order for ephemeral_columns: view-level override takes precedence over era-level default.
    -- table_oid quotes each part: schema and table names may need quoting ("Odd ""Schema"""."Ta'ble").
    SELECT v.table_schema, v.table_name, v.era_name, v.generated_columns, pg_catalog.format('%I.%I', v.table_schema, v.table_name)::regclass AS table_oid,
           COALESCE(v.ephemeral_columns, e.ephemeral_columns) AS ephemeral_columns,
           e.range_column_name, e.valid_from_column_name, e.valid_until_column_name, e.valid_to_column_name,
           e.range_subtype_category, e.range_subtype, e.range_type
    INTO info
    FROM sql_saga.updatable_view AS v
    JOIN sql_saga.era AS e USING (table_schema, table_name, era_name)
    WHERE v.view_schema = TG_TABLE_SCHEMA AND v.view_name = TG_TABLE_NAME
      AND v.view_type = 'for_portion_of';

    IF NOT FOUND THEN
         RAISE EXCEPTION 'sql_saga: could not find metadata for view %.%', quote_ident(TG_TABLE_SCHEMA), quote_ident(TG_TABLE_NAME);
    END IF;

    jnew := to_jsonb(NEW);
    jold := to_jsonb(OLD);

    -- The conceptual entity identifier excludes temporal columns even when
    -- add_for_portion_of_view introspected a PRIMARY KEY containing them.
    identifier_columns := TG_ARGV::text[];
    IF info.range_column_name IS NOT NULL THEN identifier_columns := array_remove(identifier_columns, info.range_column_name); END IF;
    IF info.valid_from_column_name IS NOT NULL THEN identifier_columns := array_remove(identifier_columns, info.valid_from_column_name); END IF;
    IF info.valid_until_column_name IS NOT NULL THEN identifier_columns := array_remove(identifier_columns, info.valid_until_column_name); END IF;
    IF info.valid_to_column_name IS NOT NULL THEN identifier_columns := array_remove(identifier_columns, info.valid_to_column_name); END IF;

    -- A NULL assignment to an identifier requests removal, never a NULL-key
    -- update. Ignore identifiers already NULL in OLD, so a nullable key does
    -- not turn an unrelated payload correction into a deletion.
    SELECT EXISTS (
        SELECT FROM unnest(identifier_columns) AS ic(column_name)
        WHERE jnew -> ic.column_name = 'null'::jsonb
          AND jnew -> ic.column_name IS DISTINCT FROM jold -> ic.column_name
    ) INTO delete_portion;

    IF delete_portion THEN
        IF EXISTS (
            SELECT FROM jsonb_each(jnew) AS nc(column_name, value)
            WHERE nc.value IS DISTINCT FROM jold -> nc.column_name
              AND NOT (nc.column_name = ANY(identifier_columns) AND nc.value = 'null'::jsonb)
              AND nc.column_name IS DISTINCT FROM info.range_column_name
              AND nc.column_name IS DISTINCT FROM info.valid_from_column_name
              AND nc.column_name IS DISTINCT FROM info.valid_until_column_name
              AND nc.column_name IS DISTINCT FROM info.valid_to_column_name
        ) THEN
            RAISE EXCEPTION 'sql_saga: a portion deletion cannot also change non-temporal column values.'
                USING ERRCODE = 'invalid_parameter_value',
                      HINT = 'Set an identifier column to NULL and change only the requested temporal bounds.';
        END IF;

        -- A middle split can physically UPDATE and INSERT without DELETE.
        -- Require DELETE authority for the logical removal regardless of plan.
        -- All physical DML still runs as the invoker, with its normal RLS.
        IF NOT pg_catalog.has_table_privilege(info.table_oid, 'DELETE') THEN
            RAISE EXCEPTION 'permission denied for table %', info.table_name
                USING ERRCODE = 'insufficient_privilege';
        END IF;

        -- NULL is only the command signal. Every component of the identity
        -- supplied to temporal_merge must come from OLD, including composites.
        SELECT jnew || jsonb_object_agg(ic.column_name, jold -> ic.column_name)
        INTO jnew
        FROM unnest(identifier_columns) AS ic(column_name);
        merge_mode := 'DELETE_FOR_PORTION_OF';
    END IF;

    -- Strip GENERATED columns from jnew using direct array subtraction
    jnew := jnew - info.generated_columns;

    -- For UPDATE operations, we need to handle column filtering carefully to support two cases:
    -- 1. Columns not mentioned in UPDATE -> should preserve existing values across segments
    -- 2. Columns explicitly set (including to NULL) -> should apply the new value
    --
    -- Strategy: Build jnew to contain:
    --   - Identity columns (always from NEW)
    --   - Temporal columns (always from NEW)
    --   - Changed columns (from NEW, may be NULL if explicitly set to NULL)
    --   - Unchanged columns (from OLD, to preserve values across temporal segments)
    --
    -- With range intersection, each trigger sees only its own temporal segment in the target,
    -- so using OLD values for unchanged columns preserves the correct values per segment.
    IF TG_OP = 'UPDATE' THEN
        DECLARE
            filtered_jnew jsonb := '{}'::jsonb;
            key text;
            identity_cols text[] := TG_ARGV::text[];
        BEGIN
            -- Iterate through all keys in jnew
            FOR key IN SELECT jsonb_object_keys(jnew)
            LOOP
                IF key = ANY(identity_cols)
                   OR key = info.range_column_name
                   OR key = info.valid_from_column_name
                   OR key = info.valid_until_column_name
                   OR key = info.valid_to_column_name
                THEN
                    -- Always include identity and temporal columns with NEW value
                    filtered_jnew := filtered_jnew || jsonb_build_object(key, jnew->key);
                ELSIF (jnew->key) IS DISTINCT FROM (jold->key) THEN
                    -- Changed column: use NEW value (may be explicit NULL)
                    filtered_jnew := filtered_jnew || jsonb_build_object(key, jnew->key);
                ELSE
                    -- Unchanged column: use OLD value to preserve across segments
                    filtered_jnew := filtered_jnew || jsonb_build_object(key, jold->key);
                END IF;
            END LOOP;
            
            jnew := filtered_jnew;
        END;
    END IF;

    -- Writes are batched: this row trigger only prepares the row's source and appends it to a queue,
    -- pg_temp.__temp_for_portion_of_<view oid> (or ..._delete for removals). The view's statement-level
    -- trigger, sql_saga.for_portion_of_apply_trigger(), applies each queue with one temporal_merge.
    temp_source_table_name := format('__temp_for_portion_of_%s', TG_RELID);
    IF delete_portion THEN
        temp_source_table_name := temp_source_table_name || '_delete';
    END IF;

    -- A statement on this view issued while its queue is being applied (for example by a trigger on the
    -- base table) would add to, and apply, the queue being read. Refuse it.
    IF TG_RELID::text = ANY (pg_catalog.string_to_array(pg_catalog.current_setting('sql_saga.for_portion_of_applying', true), ',')) THEN
        RAISE EXCEPTION 'sql_saga: cannot update view %.% while a change through it is being applied',
            quote_ident(TG_TABLE_SCHEMA), quote_ident(TG_TABLE_NAME)
            USING ERRCODE = 'object_in_use',
                  HINT = 'Write to the base table directly from the trigger.';
    END IF;

    -- A queued row is applied only by the view's apply trigger. Without it (a view's triggers cannot be
    -- disabled, but one can be dropped despite drop protection), refuse rather than report a change that
    -- is never made. When this row trigger fires, the apply trigger fires too (same view, same role rules).
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_trigger AS t
        WHERE t.tgrelid = TG_RELID
          AND t.tgfoid = 'sql_saga.for_portion_of_apply_trigger()'::pg_catalog.regprocedure
          AND t.tgtype = 16) -- AFTER UPDATE FOR EACH STATEMENT
    THEN
        RAISE EXCEPTION 'sql_saga: view %.% has no apply trigger, so its changes would not be made',
            quote_ident(TG_TABLE_SCHEMA), quote_ident(TG_TABLE_NAME)
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Recreate the view with sql_saga.drop_for_portion_of_view and sql_saga.add_for_portion_of_view.';
    END IF;

    IF to_regclass('pg_temp.' || quote_ident(temp_source_table_name)) IS NULL THEN
        DECLARE
            v_cols_def text;
        BEGIN
            -- Build column definitions excluding the pre-computed generated_columns list
            -- This excludes GENERATED columns that would cause INSERT failures
            SELECT string_agg(
                format('%I %s', pa.attname, format_type(pa.atttypid, pa.atttypmod)),
                ', '
            )
            INTO v_cols_def
            FROM pg_attribute pa
            WHERE pa.attrelid = info.table_oid
              AND pa.attname <> ALL(info.generated_columns)
              AND pa.attnum > 0 AND NOT pa.attisdropped;

            EXECUTE format('CREATE TEMP TABLE %I (row_id bigserial, %s, merge_status jsonb, changed_columns name[]) ON COMMIT DROP',
                temp_source_table_name, v_cols_def);
        END;
    END IF;

    -- The `for_portion_of` view allows specifying a time slice using either the authoritative range
    -- column, or the convenient component columns (`valid_from`, `valid_until`, `valid_to`). This trigger must
    -- determine the definitive time slice for the operation and pass it to temporal_merge.
    DECLARE
        v_final_range_val jsonb;
        v_range_changed BOOLEAN := false;
        v_from_changed BOOLEAN := false;
        v_until_changed BOOLEAN := false;
        v_to_changed BOOLEAN := false;
        v_until_via_to text;
    BEGIN
        IF info.range_column_name IS NOT NULL THEN
            EXECUTE format('SELECT ($1).%I IS DISTINCT FROM ($2).%I', info.range_column_name, info.range_column_name) INTO v_range_changed USING NEW, OLD;
        END IF;
        IF info.valid_from_column_name IS NOT NULL THEN
            EXECUTE format('SELECT ($1).%I IS DISTINCT FROM ($2).%I', info.valid_from_column_name, info.valid_from_column_name) INTO v_from_changed USING NEW, OLD;
        END IF;
        IF info.valid_until_column_name IS NOT NULL THEN
            EXECUTE format('SELECT ($1).%I IS DISTINCT FROM ($2).%I', info.valid_until_column_name, info.valid_until_column_name) INTO v_until_changed USING NEW, OLD;
        END IF;
        IF info.valid_to_column_name IS NOT NULL THEN
            EXECUTE format('SELECT ($1).%I IS DISTINCT FROM ($2).%I', info.valid_to_column_name, info.valid_to_column_name) INTO v_to_changed USING NEW, OLD;
        END IF;

        -- If any temporal column was changed, derive the new range for the operation.
        IF v_range_changed OR v_from_changed OR v_until_changed OR v_to_changed THEN
            -- The user specified a temporal range for this update. We must intersect this
            -- requested range with the OLD row's existing timeline to ensure each trigger
            -- firing only affects its own temporal segment.
            DECLARE
                v_requested_range_val jsonb;
                v_old_range_val jsonb;
            BEGIN
                -- First, get the OLD row's range
                EXECUTE format('SELECT to_jsonb(($1).%I)', info.range_column_name) 
                INTO v_old_range_val USING OLD;

                -- If the user set the range column directly, use it as the requested range
                IF v_range_changed THEN
                    v_requested_range_val := jnew->info.range_column_name;
                -- Otherwise, derive the requested range from the component from/until/to columns
                ELSE
                    -- If valid_to was changed, convert it to valid_until (add 1 to make it exclusive)
                    IF v_to_changed THEN
                        DECLARE
                            v_new_to text;
                        BEGIN
                            v_new_to := (jnew ->> info.valid_to_column_name);
                            IF v_new_to IS NOT NULL THEN
                                EXECUTE format('SELECT ($1::%s + 1)::text', info.range_subtype)
                                INTO v_until_via_to USING v_new_to;
                            END IF;
                        END;
                    END IF;

                    -- Build the requested range using from and until (preferring converted valid_to if available)
                    EXECUTE format('SELECT to_jsonb( %I(($1->>%L)::%s, COALESCE($2, ($1->>%L))::%s, ''[)'') )',
                        info.range_type,
                        info.valid_from_column_name, info.range_subtype,
                        info.valid_until_column_name, info.range_subtype
                    ) INTO v_requested_range_val USING jnew, v_until_via_to;
                END IF;

                -- Compute the intersection of the requested range with the OLD row's range
                -- This ensures each trigger firing only affects its own temporal segment
                -- Extract the text value from jsonb first (using ->> operator)
                EXECUTE format('SELECT to_jsonb( (($1->>0)::%I) * (($2->>0)::%I) )',
                    info.range_type, info.range_type
                ) INTO v_final_range_val USING v_requested_range_val, v_old_range_val;
                
                -- If the intersection is empty, this row is outside the requested timespan
                -- Return NULL to signal that this trigger firing should be skipped
                IF v_final_range_val::text = '"empty"' THEN
                    RETURN NULL;
                END IF;
            END;
        ELSE
            -- No temporal columns were changed, so this is a data-only update. The portion
            -- to update is the original time slice from the OLD record.
            EXECUTE format('SELECT to_jsonb(($1).%I)', info.range_column_name) INTO v_final_range_val USING OLD;
        END IF;

        IF v_final_range_val IS NULL OR v_final_range_val = 'null'::jsonb THEN
            RAISE EXCEPTION 'sql_saga: could not determine the time portion for the UPDATE. Please SET either the range column (%) or both component columns (% and %).',
                quote_ident(info.range_column_name), quote_ident(info.valid_from_column_name), quote_ident(info.valid_until_column_name);
        END IF;

        jnew := jsonb_set(jnew, ARRAY[info.range_column_name], v_final_range_val);
    END;

    -- Clean up: always remove the component columns before passing to temporal_merge.
    -- The range column is the single source of truth for the period, and the sync
    -- trigger on the base table will re-populate them correctly upon write.
    IF info.valid_from_column_name IS NOT NULL THEN jnew := jnew - info.valid_from_column_name; END IF;
    IF info.valid_until_column_name IS NOT NULL THEN jnew := jnew - info.valid_until_column_name; END IF;
    IF info.valid_to_column_name IS NOT NULL THEN jnew := jnew - info.valid_to_column_name; END IF;

    -- Insert the cleaned-up JSON into the temp table using explicit column list
    -- This avoids issues with GENERATED columns that aren't in the temp table
    BEGIN
        DECLARE
            v_insert_cols text;
            v_insert_vals text;
            v_changed name[];
        BEGIN
            -- The data columns this row changes. The apply trigger merges only these: an unchanged column's OLD value
            -- was read before temporal_merge locks the entity, and writing it back would undo a concurrent change
            -- (doc/design/temporal-merge-concurrency.md). A removal changes none.
            SELECT COALESCE(array_agg(nc.key::name ORDER BY nc.key), '{}')
            INTO v_changed
            FROM jsonb_each(jnew) AS nc(key, value)
            WHERE NOT delete_portion
              AND nc.value IS DISTINCT FROM jold -> nc.key
              AND nc.key <> ALL (identifier_columns)
              AND nc.key IS DISTINCT FROM info.range_column_name;

            -- Build explicit column list and values from jnew
            SELECT 
                string_agg(quote_ident(key), ', '),
                string_agg(format('($1->>%L)::%s', key, format_type(pa.atttypid, pa.atttypmod)), ', ')
            INTO v_insert_cols, v_insert_vals
            FROM jsonb_object_keys(jnew) AS key
            JOIN pg_attribute pa ON pa.attrelid = info.table_oid
                                AND pa.attname = key
            WHERE pa.attnum > 0 AND NOT pa.attisdropped;

            EXECUTE format('INSERT INTO %I (%s, changed_columns) VALUES (%s, $2)',
                temp_source_table_name, v_insert_cols, v_insert_vals)
            USING jnew, v_changed;
        END;
    END;

    RETURN NEW;
END;
$for_portion_of_trigger$;
/*
 * The statement-level half of a for_portion_of view (1.6): AFTER UPDATE FOR EACH STATEMENT, created by
 * add_for_portion_of_view as the view's apply trigger (updatable_view.trigger_name || '_apply').
 *
 * The INSTEAD OF row trigger, sql_saga.for_portion_of_trigger(), only prepares each changed row's source
 * and queues it. This trigger applies the statement's queues with one temporal_merge each, removals first
 * (DELETE_FOR_PORTION_OF), then changes (UPDATE_FOR_PORTION_OF), and empties them. Each row's source covers
 * its OLD row's period intersected with the requested period, so the queued rows of one entity never
 * overlap, and the row trigger never saw an earlier row's effect: one merge gives the per-row result.
 *
 * It runs inside the statement as the caller (SECURITY INVOKER): an error aborts the statement, and the
 * caller's privileges and row-level security decide every physical change, as before. While a queue is
 * applied, sql_saga.for_portion_of_applying names the view, and the row trigger refuses a nested statement
 * on it (a trigger on the base table writing through the same view).
 */
CREATE OR REPLACE FUNCTION sql_saga.for_portion_of_apply_trigger()
 RETURNS trigger
 LANGUAGE plpgsql
AS $for_portion_of_apply_trigger$
#variable_conflict use_variable
DECLARE
    info record;
    identifier_columns text[];
    queue record;
    queue_oid regclass;
    column_set record;
    v_nonempty boolean;
    v_error_message text;
    v_applying text;
BEGIN
    IF TG_LEVEL <> 'STATEMENT' OR TG_WHEN <> 'AFTER' OR TG_OP <> 'UPDATE' THEN
        RAISE EXCEPTION 'sql_saga: for_portion_of_apply_trigger() must be an AFTER UPDATE FOR EACH STATEMENT trigger'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    FOR queue IN
        SELECT q.name, q.mode::sql_saga.temporal_merge_mode AS mode
        FROM (VALUES (1, '__temp_for_portion_of_' || TG_RELID || '_delete', 'DELETE_FOR_PORTION_OF'),
                     (2, '__temp_for_portion_of_' || TG_RELID, 'UPDATE_FOR_PORTION_OF')) AS q(ord, name, mode)
        ORDER BY q.ord
    LOOP
        queue_oid := pg_catalog.to_regclass(pg_catalog.format('pg_temp.%I', queue.name));
        CONTINUE WHEN queue_oid IS NULL;
        EXECUTE pg_catalog.format('SELECT EXISTS (SELECT FROM %s)', queue_oid) INTO v_nonempty;
        CONTINUE WHEN NOT v_nonempty;

        IF info IS NULL THEN
            SELECT v.era_name, v.trigger_name, pg_catalog.format('%I.%I', v.table_schema, v.table_name)::regclass AS table_oid,
                   COALESCE(v.ephemeral_columns, e.ephemeral_columns) AS ephemeral_columns,
                   e.range_column_name, e.valid_from_column_name, e.valid_until_column_name, e.valid_to_column_name
            INTO info
            FROM sql_saga.updatable_view AS v
            JOIN sql_saga.era AS e USING (table_schema, table_name, era_name)
            WHERE v.view_schema = TG_TABLE_SCHEMA AND v.view_name = TG_TABLE_NAME
              AND v.view_type = 'for_portion_of';
            IF NOT FOUND THEN
                RAISE EXCEPTION 'sql_saga: could not find metadata for view %.%', quote_ident(TG_TABLE_SCHEMA), quote_ident(TG_TABLE_NAME);
            END IF;

            -- The entity identifier: the row trigger's arguments without the temporal columns.
            SELECT pg_catalog.string_to_array(pg_catalog.rtrim(pg_catalog.encode(t.tgargs, 'escape'), E'\\000'), E'\\000')
            INTO identifier_columns
            FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (TG_RELID, info.trigger_name);
            IF identifier_columns IS NULL THEN
                RAISE EXCEPTION 'sql_saga: the row trigger "%" of view %.% is missing', info.trigger_name,
                    quote_ident(TG_TABLE_SCHEMA), quote_ident(TG_TABLE_NAME);
            END IF;
            identifier_columns := pg_catalog.array_remove(identifier_columns, info.range_column_name);
            identifier_columns := pg_catalog.array_remove(identifier_columns, info.valid_from_column_name);
            identifier_columns := pg_catalog.array_remove(identifier_columns, info.valid_until_column_name);
            identifier_columns := pg_catalog.array_remove(identifier_columns, info.valid_to_column_name);
        END IF;

        v_applying := pg_catalog.current_setting('sql_saga.for_portion_of_applying', true);
        PERFORM pg_catalog.set_config('sql_saga.for_portion_of_applying',
            pg_catalog.concat_ws(',', NULLIF(v_applying, ''), TG_RELID::text), true);

        -- Merge each set of rows that changed the same columns through a view of the queue with exactly the
        -- identity, the period and those columns: a column the source lacks keeps the target's value, read under
        -- temporal_merge's locks (doc/design/temporal-merge-concurrency.md). Sets never overlap in time (each row lies
        -- within its own OLD row), and are merged in queue order.
        FOR column_set IN EXECUTE pg_catalog.format(
            'SELECT q.changed_columns FROM %s AS q GROUP BY q.changed_columns ORDER BY min(q.row_id)', queue_oid)
        LOOP
            IF to_regclass('pg_temp.__for_portion_of_source') IS NOT NULL THEN
                DROP VIEW pg_temp.__for_portion_of_source;
            END IF;
            EXECUTE pg_catalog.format(
                'CREATE TEMP VIEW __for_portion_of_source AS SELECT q.row_id, %s, q.merge_status FROM %s AS q WHERE q.changed_columns = %L',
                (SELECT string_agg(pg_catalog.quote_ident(c), ', ')
                 FROM unnest(identifier_columns || info.range_column_name::text || column_set.changed_columns::text[]) AS c),
                queue_oid, column_set.changed_columns);

            CALL sql_saga.temporal_merge(
                target_table => info.table_oid,
                source_table => 'pg_temp.__for_portion_of_source'::regclass,
                primary_identity_columns => identifier_columns,
                mode => queue.mode,
                era_name => info.era_name,
                update_source_with_feedback => true,
                feedback_status_column => 'merge_status',
                feedback_status_key => 'temporal_merge',
                ephemeral_columns => info.ephemeral_columns
            );
        END LOOP;
        DROP VIEW IF EXISTS pg_temp.__for_portion_of_source;

        PERFORM pg_catalog.set_config('sql_saga.for_portion_of_applying', COALESCE(v_applying, ''), true);

        EXECUTE pg_catalog.format($SQL$
            SELECT q.merge_status->'temporal_merge'->>'error_message'
            FROM %s AS q
            WHERE (q.merge_status->'temporal_merge'->>'status')::sql_saga.temporal_merge_feedback_status = 'ERROR'
            ORDER BY q.row_id
            LIMIT 1
        $SQL$, queue_oid) INTO v_error_message;
        IF v_error_message IS NOT NULL THEN
            RAISE EXCEPTION 'sql_saga: applying the change for portion of time failed. First error: %', v_error_message;
        END IF;

        EXECUTE pg_catalog.format('TRUNCATE %s', queue_oid);
    END LOOP;

    RETURN NULL;
END;
$for_portion_of_apply_trigger$;
CREATE OR REPLACE FUNCTION sql_saga.__api_drop_unique_key_by_name(
    table_oid regclass,
    key_name name,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT',
    cleanup boolean DEFAULT true
 )
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, pg_temp
AS
$function$
#variable_conflict use_variable
DECLARE
    table_schema name;
    table_name name;
    foreign_key_row sql_saga.foreign_keys;
    unique_key_row sql_saga.unique_keys;
    unique_object_attributable boolean;
    exclude_object_attributable boolean;
    check_object_attributable boolean;
    range_column_name name;
    expected_attnums smallint[];
BEGIN
    PERFORM sql_saga.__internal_entry_guard();
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;
    PERFORM sql_saga.__internal_require_owner(table_oid, 'drop_unique_key_by_name');

    SELECT n.nspname, c.relname
    INTO table_schema, table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = table_oid;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    FOR unique_key_row IN
        SELECT uk.*
        FROM sql_saga.unique_keys AS uk
        WHERE (uk.table_schema, uk.table_name) = (table_schema, table_name)
          AND (uk.unique_key_name = key_name OR key_name IS NULL)
    LOOP
        RAISE DEBUG 'drop_unique_key_by_name: Processing key: %', row_to_json(unique_key_row);

        IF unique_key_row.key_type IN ('primary', 'natural')
           AND unique_key_row.exclude_constraint IS NULL
           AND unique_key_row.check_constraint IS NULL THEN
            SELECT e.range_column_name
            INTO range_column_name
            FROM sql_saga.era AS e
            WHERE (e.table_schema, e.table_name, e.era_name) =
                  (unique_key_row.table_schema, unique_key_row.table_name, unique_key_row.era_name);

            SELECT pg_catalog.array_agg(a.attnum::smallint ORDER BY u.n)
            INTO expected_attnums
            FROM pg_catalog.unnest(unique_key_row.column_names || range_column_name)
                 WITH ORDINALITY AS u(column_name, n)
            JOIN pg_catalog.pg_attribute AS a
              ON a.attrelid = table_oid AND a.attname = u.column_name
             AND a.attnum > 0 AND NOT a.attisdropped;

            SELECT uc.oid IS NOT NULL
                   AND pg_catalog.cardinality(expected_attnums) =
                       pg_catalog.cardinality(unique_key_row.column_names) + 1
                   AND uc.contype = CASE unique_key_row.key_type WHEN 'primary' THEN 'p' ELSE 'u' END
                   AND uc.conkey = expected_attnums AND uc.conperiod
                   AND NOT uc.condeferrable AND NOT uc.condeferred
                   AND ui.indrelid = table_oid AND ui.indisunique AND ui.indisvalid AND ui.indisready
                   AND ui.indnkeyatts = pg_catalog.cardinality(expected_attnums)
                   AND ui.indkey::text = pg_catalog.array_to_string(expected_attnums, ' ')
                   AND ui.indexprs IS NULL AND ui.indpred IS NULL AND am.amname = 'gist'
            INTO unique_object_attributable
            FROM pg_catalog.pg_constraint AS uc
            JOIN pg_catalog.pg_index AS ui ON ui.indexrelid = uc.conindid
            JOIN pg_catalog.pg_class AS idx ON idx.oid = ui.indexrelid
            JOIN pg_catalog.pg_am AS am ON am.oid = idx.relam
            WHERE uc.conrelid = table_oid AND uc.conname = unique_key_row.unique_constraint;

            unique_object_attributable := coalesce(unique_object_attributable, false);
            exclude_object_attributable := true;
            check_object_attributable := true;
        ELSE
            SELECT a.unique_attributable, a.exclude_attributable, a.check_attributable
            INTO unique_object_attributable, exclude_object_attributable, check_object_attributable
            FROM sql_saga.__internal_unique_key_attribution(unique_key_row.unique_key_name) AS a;
        END IF;
        /* Propagate the requested drop behavior to foreign keys, if desired */
        FOR foreign_key_row IN
            SELECT fk.*
            FROM sql_saga.foreign_keys AS fk
            WHERE fk.unique_key_name = unique_key_row.unique_key_name
        LOOP
            IF drop_behavior = 'RESTRICT' THEN
                RAISE EXCEPTION 'cannot drop unique key "%" because foreign key "%" on table "%" depends on it',
                    unique_key_row.unique_key_name, foreign_key_row.foreign_key_name,
                    sql_saga.__internal_display_name(format('%I.%I', foreign_key_row.table_schema, foreign_key_row.table_name)::regclass);
            END IF;

            /* As native DROP ... CASCADE: the key's owner (checked above) drops the dependent keys, whoever owns them. */
            PERFORM sql_saga.__internal_drop_foreign_key_by_name(NULL, foreign_key_row.foreign_key_name, drop_index => true, require_owner => false);
        END LOOP;

        DELETE FROM sql_saga.unique_keys AS uk
        WHERE uk.unique_key_name = unique_key_row.unique_key_name;

        /* If purging, drop the underlying constraints unless the table has been dropped */
        IF cleanup AND EXISTS (
            SELECT FROM pg_catalog.pg_class AS c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE (n.nspname, c.relname) = (unique_key_row.table_schema, unique_key_row.table_name))
        THEN
            IF unique_key_row.check_constraint IS NOT NULL AND check_object_attributable THEN
                PERFORM sql_saga.__internal_execute_as_caller(format('ALTER TABLE %I.%I DROP CONSTRAINT IF EXISTS %I',
                    unique_key_row.table_schema, unique_key_row.table_name, unique_key_row.check_constraint));
            ELSIF unique_key_row.check_constraint IS NOT NULL
                  AND EXISTS (SELECT FROM pg_catalog.pg_constraint AS c
                             WHERE c.conrelid = table_oid AND c.conname = unique_key_row.check_constraint) THEN
                RAISE NOTICE 'sql_saga: Left constraint "%" on table "%" in place because it is not the sql_saga-managed constraint',
                    unique_key_row.check_constraint, sql_saga.__internal_display_name(table_oid);
            END IF;
            -- An XOR key's NULLS NOT DISTINCT unique constraint is dropped as any natural key's (below).
            IF unique_key_row.key_type = 'predicated' THEN
                BEGIN
                    IF unique_object_attributable THEN
                        IF unique_key_row.unique_constraint IS NOT NULL THEN
                            PERFORM sql_saga.__internal_execute_as_caller(format('DROP INDEX %I.%I', unique_key_row.table_schema, unique_key_row.unique_constraint));
                        END IF;
                    ELSIF to_regclass(format('%I.%I', unique_key_row.table_schema, unique_key_row.unique_constraint)) IS NOT NULL THEN
                        RAISE NOTICE 'sql_saga: Left index "%" on table "%" in place because it is not the sql_saga-managed index',
                            unique_key_row.unique_constraint, sql_saga.__internal_display_name(table_oid);
                    END IF;
                    IF exclude_object_attributable AND unique_key_row.exclude_constraint IS NOT NULL THEN
                        PERFORM sql_saga.__internal_execute_as_caller(format('ALTER TABLE %I.%I DROP CONSTRAINT %I', unique_key_row.table_schema,
                            unique_key_row.table_name, unique_key_row.exclude_constraint));
                    ELSIF EXISTS (SELECT FROM pg_catalog.pg_constraint AS c WHERE c.conrelid = table_oid AND c.conname = unique_key_row.exclude_constraint) THEN
                        RAISE NOTICE 'sql_saga: Left constraint "%" on table "%" in place because it is not the sql_saga-managed constraint',
                            unique_key_row.exclude_constraint, sql_saga.__internal_display_name(table_oid);
                    END IF;
                END;
            ELSE -- 'primary' or 'natural'
                DECLARE
                    alter_parts text[] := '{}';
                BEGIN
                    -- This is a unique/primary key constraint, drop both constraints if they exist.
                    IF unique_key_row.unique_constraint IS NOT NULL AND unique_object_attributable THEN
                        alter_parts := alter_parts || format('DROP CONSTRAINT IF EXISTS %I', unique_key_row.unique_constraint);
                    ELSIF unique_key_row.unique_constraint IS NOT NULL
                          AND EXISTS (SELECT FROM pg_catalog.pg_constraint AS c
                                     WHERE c.conrelid = table_oid AND c.conname = unique_key_row.unique_constraint) THEN
                        RAISE NOTICE 'sql_saga: Left constraint "%" on table "%" in place because it is not the sql_saga-managed constraint',
                            unique_key_row.unique_constraint, sql_saga.__internal_display_name(table_oid);
                    ELSIF unique_key_row.unique_constraint IS NOT NULL
                          AND to_regclass(format('%I.%I', unique_key_row.table_schema, unique_key_row.unique_constraint)) IS NOT NULL THEN
                        RAISE NOTICE 'sql_saga: Left index "%" on table "%" in place because it is not the sql_saga-managed index',
                            unique_key_row.unique_constraint, sql_saga.__internal_display_name(table_oid);
                    END IF;
                    IF unique_key_row.exclude_constraint IS NOT NULL AND exclude_object_attributable THEN
                        alter_parts := alter_parts || format('DROP CONSTRAINT IF EXISTS %I', unique_key_row.exclude_constraint);
                    ELSIF unique_key_row.exclude_constraint IS NOT NULL
                          AND EXISTS (SELECT FROM pg_catalog.pg_constraint AS c WHERE c.conrelid = table_oid AND c.conname = unique_key_row.exclude_constraint) THEN
                        RAISE NOTICE 'sql_saga: Left constraint "%" on table "%" in place because it is not the sql_saga-managed constraint',
                            unique_key_row.exclude_constraint, sql_saga.__internal_display_name(table_oid);
                    END IF;

                    IF cardinality(alter_parts) > 0 THEN
                        PERFORM sql_saga.__internal_execute_as_caller(format('ALTER TABLE %I.%I %s',
                            unique_key_row.table_schema,
                            unique_key_row.table_name,
                            array_to_string(alter_parts, ', ')
                        ));
                    END IF;
                END;
            END IF;
        END IF;
    END LOOP;

END;
$function$;
/*
 * 1.15 (hug bjd8v7): the CHECK of a natural key with mutually_exclusive_columns, as PostgreSQL deparses it
 * (pg_get_constraintdef), so add_unique_key, check_integrity, rename following and the update step compare text with
 * text. At most one of the columns holds a value per row; all NULL is allowed (before the key starts).
 */
CREATE OR REPLACE FUNCTION sql_saga.__internal_xor_check_definition(mutually_exclusive_columns name[])
RETURNS text
LANGUAGE sql
IMMUTABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_xor_check_definition$
    SELECT pg_catalog.format('CHECK ((num_nonnulls(%s) <= 1))',
        (SELECT pg_catalog.string_agg(pg_catalog.quote_ident(c), ', ' ORDER BY n)
         FROM pg_catalog.unnest(mutually_exclusive_columns) WITH ORDINALITY AS u(c, n)));
$__internal_xor_check_definition$;
/*
 * The guards that bind a natural key to its entity, both ways.
 * (1) 1.14, hug mnntde: a value belongs to one entity. For each stable primary key column (the primary key's columns
 * without the range), EXCLUDE (natural key WITH =, that column WITH <>). Together they refuse a natural key value on
 * two rows whose primary key differs in any column, at any time. Not needed when the natural key itself contains every
 * stable primary key column (outside its mutually exclusive columns): then its own uniqueness already binds it.
 * (2) 1.15, hug 40nwvn: an entity holds one value. For each natural key column outside the primary key,
 * EXCLUDE (stable primary key WITH =, that column WITH <>).
 * There is no opt-out: a natural key that could move between entities, or change for one, identifies nothing.
 * Names (1): <key>_pk_consistency_excl for a single stable column (as before 1.14), <key>_<column>_pk_consistency_excl
 * for each column of a composite one; a mutually exclusive key adds the XOR column to the name, as before.
 * Names (2): <key>_nk_consistency_excl for a one-column natural key, <key>_<column>_nk_consistency_excl otherwise.
 */
CREATE OR REPLACE FUNCTION sql_saga.__internal_generate_pk_consistency_constraints(
    p_unique_key_name name,
    p_natural_key_columns name[],
    p_mutually_exclusive_columns name[],
    p_stable_pk_columns name[]
)
RETURNS jsonb -- { "cmds": text[], "names": name[] }
LANGUAGE plpgsql AS
$function$
#variable_conflict use_variable
DECLARE
    v_constraint_cmds text[] := '{}';
    v_constraint_names name[] := '{}';
    v_constraint_name name;
    v_constraint_sql text;
    v_withs text[];
    v_pk_column name;
    v_composite boolean := pg_catalog.array_length(p_stable_pk_columns, 1) > 1;
    non_xor_cols name[] := ARRAY(SELECT c FROM pg_catalog.unnest(p_natural_key_columns) AS c
                                 WHERE p_mutually_exclusive_columns IS NULL OR c <> ALL (p_mutually_exclusive_columns));
    xor_col name;
    where_clause_partial text;
BEGIN
    IF p_stable_pk_columns IS NULL OR pg_catalog.array_length(p_stable_pk_columns, 1) IS NULL THEN
        RAISE EXCEPTION 'sql_saga: a primary key without a stable column cannot bind natural key "%"', p_unique_key_name;
    END IF;
    -- The first half is implied by the natural key's own uniqueness when it holds every stable primary key column; the
    -- other half (one value per entity, below) may still be needed.
    FOREACH v_pk_column IN ARRAY CASE WHEN p_stable_pk_columns <@ non_xor_cols THEN '{}'::name[] ELSE p_stable_pk_columns END LOOP
        IF p_mutually_exclusive_columns IS NULL THEN
            -- A primary key column that is part of the natural key binds itself: (c WITH =, c WITH <>) never conflicts.
            CONTINUE WHEN v_pk_column = ANY (p_natural_key_columns);
            v_constraint_name := CASE WHEN v_composite
                THEN sql_saga.__internal_make_name(ARRAY[p_unique_key_name, v_pk_column, 'pk_consistency_excl'])
                ELSE p_unique_key_name || '_pk_consistency_excl' END;
            -- Truncated to 63 bytes, two long names can meet: the ordinal goes in the part that is never truncated.
            IF v_constraint_name = ANY (v_constraint_names) THEN
                v_constraint_name := sql_saga.__internal_make_name(ARRAY[p_unique_key_name, v_pk_column],
                    pg_catalog.cardinality(v_constraint_names) + 1 || '_pk_consistency_excl');
            END IF;
            SELECT array_agg(pg_catalog.format('%I WITH =', col)) INTO v_withs FROM pg_catalog.unnest(p_natural_key_columns) AS col;
            v_withs := v_withs || pg_catalog.format('%I WITH <>', v_pk_column);
            v_constraint_sql := pg_catalog.format('ADD CONSTRAINT %I EXCLUDE USING gist (%s)', v_constraint_name, pg_catalog.array_to_string(v_withs, ', '));
            v_constraint_cmds := pg_catalog.array_append(v_constraint_cmds, v_constraint_sql);
            v_constraint_names := pg_catalog.array_append(v_constraint_names, v_constraint_name);
        ELSE
            FOREACH xor_col IN ARRAY p_mutually_exclusive_columns LOOP
                CONTINUE WHEN v_pk_column = ANY (non_xor_cols || xor_col);
                v_constraint_name := sql_saga.__internal_make_name(
                    CASE WHEN v_composite THEN ARRAY[p_unique_key_name, xor_col, v_pk_column, 'pk_consistency_excl']
                         ELSE ARRAY[p_unique_key_name, xor_col, 'pk_consistency_excl'] END);
                IF v_constraint_name = ANY (v_constraint_names) THEN
                    v_constraint_name := sql_saga.__internal_make_name(ARRAY[p_unique_key_name, xor_col, v_pk_column],
                        pg_catalog.cardinality(v_constraint_names) + 1 || '_pk_consistency_excl');
                END IF;
                SELECT array_agg(pg_catalog.format('%I WITH =', col)) INTO v_withs FROM pg_catalog.unnest(non_xor_cols || xor_col) AS col;
                v_withs := v_withs || pg_catalog.format('%I WITH <>', v_pk_column);
                -- Written as PostgreSQL deparses it (pg_get_constraintdef), so check_integrity compares text with text.
                where_clause_partial := pg_catalog.format('WHERE ((%s))',
                    (SELECT string_agg(p, ' AND ' ORDER BY o) FROM (
                        SELECT 0 AS o, pg_catalog.format('(%I IS NOT NULL)', xor_col) AS p
                        UNION ALL
                        SELECT u.n, pg_catalog.format('(%I IS NULL)', u.other_col)
                        FROM pg_catalog.unnest(p_mutually_exclusive_columns) WITH ORDINALITY AS u(other_col, n)
                        WHERE u.other_col <> xor_col) AS parts));
                v_constraint_sql := pg_catalog.format('ADD CONSTRAINT %I EXCLUDE USING gist (%s) %s', v_constraint_name,
                    pg_catalog.array_to_string(v_withs, ', '), where_clause_partial);
                v_constraint_cmds := pg_catalog.array_append(v_constraint_cmds, v_constraint_sql);
                v_constraint_names := pg_catalog.array_append(v_constraint_names, v_constraint_name);
            END LOOP;
        END IF;
    END LOOP;

    -- The other half (1.15, hug 40nwvn): EXCLUDE (stable primary key WITH =, natural key column WITH <>) for each natural
    -- key column outside the primary key, so an entity holds at most one non-NULL value of it over its whole timeline
    -- (NULL before the key starts and after it is retired is allowed: an exclusion ignores NULLs). An identifier that
    -- changes is another natural key column; the entity moves to it by a row carrying both. A mutually exclusive
    -- column needs no WHERE: its NULL rows never conflict.
    FOREACH v_pk_column IN ARRAY p_natural_key_columns LOOP
        CONTINUE WHEN v_pk_column = ANY (p_stable_pk_columns);
        v_constraint_name := CASE WHEN pg_catalog.array_length(p_natural_key_columns, 1) > 1
            THEN sql_saga.__internal_make_name(ARRAY[p_unique_key_name, v_pk_column], 'nk_consistency_excl')
            ELSE sql_saga.__internal_make_name(ARRAY[p_unique_key_name], 'nk_consistency_excl') END;
        IF v_constraint_name = ANY (v_constraint_names) THEN
            v_constraint_name := sql_saga.__internal_make_name(ARRAY[p_unique_key_name, v_pk_column],
                pg_catalog.cardinality(v_constraint_names) + 1 || '_nk_consistency_excl');
        END IF;
        SELECT array_agg(pg_catalog.format('%I WITH =', col) ORDER BY o) INTO v_withs
        FROM pg_catalog.unnest(p_stable_pk_columns) WITH ORDINALITY AS u(col, o);
        v_withs := v_withs || pg_catalog.format('%I WITH <>', v_pk_column);
        v_constraint_sql := pg_catalog.format('ADD CONSTRAINT %I EXCLUDE USING gist (%s)', v_constraint_name, pg_catalog.array_to_string(v_withs, ', '));
        v_constraint_cmds := pg_catalog.array_append(v_constraint_cmds, v_constraint_sql);
        v_constraint_names := pg_catalog.array_append(v_constraint_names, v_constraint_name);
    END LOOP;

    RETURN jsonb_build_object('cmds', to_jsonb(v_constraint_cmds), 'names', to_jsonb(v_constraint_names));
END;
$function$;
CREATE OR REPLACE FUNCTION sql_saga.__api_add_unique_key(
        table_oid regclass,
        column_names name[],
        era_name name DEFAULT 'valid',
        key_type sql_saga.unique_key_type DEFAULT 'natural',
        unique_key_name name DEFAULT NULL,
        unique_constraint name DEFAULT NULL,
        exclude_constraint name DEFAULT NULL,
        predicate text DEFAULT NULL,
        mutually_exclusive_columns name[] DEFAULT NULL)
 RETURNS name
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, pg_temp
AS
$function$
#variable_conflict use_variable
DECLARE
    table_schema name;
    table_name name;
    era_row sql_saga.era;
    column_attnums smallint[];
    era_attnums smallint[];
    idx integer;
    constraint_record record;
    pass integer;
    sql text;
    alter_cmds text[];
    v_unique_constraint_found boolean := false;
    v_exclude_constraint_found boolean := false;
    v_check_constraint name;
    unique_sql text;
    exclude_sql text;
    where_clause text;

    v_pk_consistency_constraint_names name[];
    v_stable_cols_to_store name[];
BEGIN
    PERFORM sql_saga.__internal_entry_guard();
    PERFORM sql_saga.__internal_require_owner(table_oid, 'add_unique_key');
    RAISE DEBUG 'add_unique_key entry: table_oid=%, column_names=%, era_name=%, key_type=%', table_oid, column_names, era_name, key_type;

    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    SELECT n.nspname, c.relname
    INTO table_schema, table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = table_oid;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    SELECT p.*
    INTO era_row
    FROM sql_saga.era AS p
    WHERE (p.table_schema, p.table_name, p.era_name) = (table_schema, table_name, era_name);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'era "%" does not exist', era_name;
    END IF;

    -- For metadata storage, we use the exact column list provided by the user,
    -- as this is what symmetric drop functions will expect. The native unique
    -- constraint will include the temporal column, but our metadata only tracks
    -- the stable part of the key. An explicit check below ensures the user does
    -- not include the range column here.
    v_stable_cols_to_store := __api_add_unique_key.column_names;

    -- This is no longer used by native temporal keys, but is still needed for XOR key logic on predicated keys.
    IF era_row.valid_from_column_name IS NOT NULL THEN
        era_attnums := ARRAY[
            (SELECT a.attnum FROM pg_catalog.pg_attribute AS a WHERE (a.attrelid, a.attname) = (table_oid, era_row.valid_from_column_name)),
            (SELECT a.attnum FROM pg_catalog.pg_attribute AS a WHERE (a.attrelid, a.attname) = (table_oid, era_row.valid_until_column_name))
        ];
    END IF;

    /* Get attnums from column names */
    SELECT array_agg(a.attnum ORDER BY n.ordinality)
    INTO column_attnums
    FROM unnest(column_names) WITH ORDINALITY AS n (name, ordinality)
    LEFT JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (table_oid, n.name);

    /* System columns are not allowed */
    IF 0 > ANY (column_attnums) THEN
        RAISE EXCEPTION 'index creation on system columns is not supported';
    END IF;

    /* Report if any columns weren't found */
    idx := array_position(column_attnums, NULL);
    IF idx IS NOT NULL THEN
        RAISE EXCEPTION 'column "%" does not exist', column_names[idx];
    END IF;

    /* Make sure the range column isn't also in the normal columns */
    IF era_row.range_column_name = ANY (column_names) THEN
        RAISE EXCEPTION 'range column "%" cannot also be part of the key columns', era_row.range_column_name;
    END IF;

    /*
     * Columns belonging to a SYSTEM_TIME period are not allowed in a UNIQUE
     * key. SQL:2016 11.7 SR 5)b)
     */
    IF EXISTS (
        SELECT FROM sql_saga.era AS e
        WHERE (e.table_schema, e.table_name, e.era_name) = (era_row.table_schema, era_row.table_name, 'system_time')
          AND ARRAY[e.valid_from_column_name, e.valid_until_column_name] && column_names)
    THEN
        RAISE EXCEPTION 'columns in era for SYSTEM_TIME are not allowed in UNIQUE keys';
    END IF;

    -- Validate parameter combinations
    IF key_type = 'predicated' AND predicate IS NULL THEN
        RAISE EXCEPTION 'a predicate must be provided when key_type is ''predicated''';
    END IF;

    IF key_type IN ('primary', 'natural') AND predicate IS NOT NULL THEN
        RAISE EXCEPTION 'a predicate can only be provided when key_type is ''predicated''';
    END IF;

    IF mutually_exclusive_columns IS NOT NULL THEN
        IF NOT column_names @> mutually_exclusive_columns THEN
            RAISE EXCEPTION 'all mutually_exclusive_columns must be present in column_names';
        END IF;
        IF key_type <> 'natural' THEN
            RAISE EXCEPTION 'mutually_exclusive_columns can only be used with key_type = ''natural''';
        END IF;
    END IF;

    IF predicate IS NOT NULL THEN
        /* One expression only, resolved as the caller; store and use its canonical deparse (Q4). */
        predicate := sql_saga.__internal_predicate_for_table(table_oid, predicate);
    END IF;
    where_clause := CASE
        WHEN predicate IS NULL THEN ''
        WHEN left(predicate, 1) = '(' THEN format(' WHERE %s', predicate /* already parenthesized canonical deparse */)
        ELSE format(' WHERE (%s)', predicate /* %s */)
    END;

    IF key_type IN ('primary', 'natural') THEN
        -- When creating a primary key, validate that the table schema is compatible with SCD Type 2 history.
        IF key_type = 'primary' THEN
            DECLARE
                identity_column_name name;
            BEGIN
                -- Check for a GENERATED ALWAYS identity column first, as it's a more specific error.
                SELECT a.attname
                INTO identity_column_name
                FROM pg_catalog.pg_attribute a
                WHERE a.attrelid = table_oid
                AND a.attidentity = 'a'; -- 'a' = always, 'd' = by default

                IF identity_column_name IS NOT NULL THEN
                    RAISE EXCEPTION 'table "%" has a GENERATED ALWAYS AS IDENTITY column ("%"); this is incompatible with SCD Type 2 history as it prevents inserting new historical versions of an entity', sql_saga.__internal_display_name(table_oid), identity_column_name;
                END IF;

                -- Check for a simple primary key that does not include the temporal columns.
                DECLARE
                    pk_columns name[];
                BEGIN
                    SELECT
                        array_agg(a.attname ORDER BY u.ord)
                    INTO
                        pk_columns
                    FROM
                        pg_catalog.pg_constraint c
                        JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS u(attnum, ord) ON TRUE
                        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = u.attnum
                    WHERE
                        c.conrelid = table_oid AND c.contype = 'p';

                    IF pk_columns IS NOT NULL AND NOT (pk_columns @> ARRAY[era_row.range_column_name]) THEN
                        RAISE EXCEPTION 'table "%" has a simple PRIMARY KEY that does not include the temporal range column ("%"); this is incompatible with SCD Type 2 history', sql_saga.__internal_display_name(table_oid), era_row.range_column_name;
                    END IF;
                END;
            END;
        END IF;
        /* If we were given a unique constraint to use, look it up and make sure it matches */
        -- For primary and natural keys, we now use a native temporal constraint.
        -- These constraints must be NOT DEFERRABLE (the default) to be referenceable by
        -- native temporal foreign keys. The with_temporary_temporal_gaps execution strategy
        -- (DELETE->UPDATE->INSERT) avoids creating overlaps that would violate this constraint.
        SELECT format('%s (%s, %I WITHOUT OVERLAPS)',
            CASE WHEN key_type = 'primary' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END, /* %s */
            string_agg(quote_ident(u.column_name), ', ' ORDER BY u.ordinality), /* %s */
            era_row.range_column_name /* %I */
        )
        INTO unique_sql
        FROM unnest(column_names) WITH ORDINALITY AS u (column_name, ordinality);

        IF unique_constraint IS NULL AND key_type = 'primary' THEN
            -- If this is a primary key and no name was given, check if one already exists.
            SELECT c.conname INTO unique_constraint
            FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.contype) = (table_oid, 'p');
        END IF;

        IF unique_constraint IS NOT NULL THEN
            SELECT c.oid, c.contype, c.condeferrable, c.condeferred, c.conkey
            INTO constraint_record
            FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.conname) = (table_oid, unique_constraint);

            IF FOUND THEN
                v_unique_constraint_found := true;

                IF key_type = 'primary' AND constraint_record.contype <> 'p' THEN
                    RAISE EXCEPTION 'constraint "%" is not a PRIMARY KEY', unique_constraint;
                ELSIF key_type = 'natural' AND constraint_record.contype NOT IN ('p', 'u') THEN
                    RAISE EXCEPTION 'constraint "%" is not a PRIMARY KEY or UNIQUE KEY', unique_constraint;
                END IF;

                -- PostgreSQL 18 requirement: WITHOUT OVERLAPS constraints must be NOT DEFERRABLE
                -- to be referenceable by native temporal FKs (FOREIGN KEY ... PERIOD).
                -- Users migrating from trigger-based FKs may have DEFERRABLE PKs which must be changed.
                -- The with_temporary_temporal_gaps strategy (DELETE→UPDATE→INSERT) creates temporary
                -- gaps (tolerated by DEFERRABLE FKs) rather than overlaps (which would violate this constraint).
                DECLARE
                    unique_sql_nulls_not_distinct text;
                    actual_def text;
                BEGIN
                    -- Generate the NULLS NOT DISTINCT variant for comparison
                    SELECT format('%s (%s, %I WITHOUT OVERLAPS)',
                        CASE WHEN key_type = 'primary' THEN 'PRIMARY KEY NULLS NOT DISTINCT' ELSE 'UNIQUE NULLS NOT DISTINCT' END,
                        string_agg(quote_ident(u.column_name), ', ' ORDER BY u.ordinality),
                        era_row.range_column_name
                    )
                    INTO unique_sql_nulls_not_distinct
                    FROM unnest(column_names) WITH ORDINALITY AS u (column_name, ordinality);

                    actual_def := pg_catalog.pg_get_constraintdef(constraint_record.oid);

                    -- Check if constraint matches either traditional or NULLS NOT DISTINCT format
                    IF actual_def <> unique_sql AND actual_def <> unique_sql_nulls_not_distinct THEN
                        RAISE EXCEPTION E'constraint "%" does not match expected format.\nExpected (traditional): "%"\nExpected (NULLS NOT DISTINCT): "%"\nFound: "%"\n\nPostgreSQL 18 Temporal FK Requirement:\nUnique constraints with WITHOUT OVERLAPS must be NOT DEFERRABLE to be referenceable by native temporal foreign keys. The DELETE→UPDATE→INSERT execution strategy avoids overlaps while tolerating temporary gaps through DEFERRABLE FK checks.\n\nSee AGENTS.md section "PostgreSQL 18 Temporal Constraints and Foreign Keys" for details.',
                            unique_constraint,
                            unique_sql,
                            unique_sql_nulls_not_distinct,
                            actual_def;
                    END IF;
                END;

                /* Looks good, let's use it. */
            END IF;
        END IF;
    ELSIF key_type = 'predicated' THEN
        -- For predicated keys, we don't create a UNIQUE constraint/index.
        -- Uniqueness is enforced by the EXCLUDE constraint.
        -- We can clear unique_sql as it's not applicable.
        unique_sql := NULL;
    END IF;

    -- Native temporal constraints (WITHOUT OVERLAPS) replace exclusion constraints
    -- for primary and natural keys. Predicated keys still require an exclusion constraint.
    IF key_type = 'predicated' THEN
        DECLARE
            withs text[];
        BEGIN
            SELECT array_agg(format('%I WITH =', column_name /* %I */) ORDER BY n.ordinality)
            INTO withs
            FROM unnest(column_names) WITH ORDINALITY AS n (column_name, ordinality);

            withs := withs || format('%I WITH &&',
                era_row.range_column_name /* %I */
            );

            exclude_sql := format('EXCLUDE USING gist (%s)%s DEFERRABLE', array_to_string(withs, ', ') /* %s */, where_clause /* %s */);
        END;

        IF exclude_constraint IS NOT NULL THEN
            SELECT c.oid, c.contype, c.condeferrable, c.condeferred, pg_catalog.pg_get_constraintdef(c.oid) AS definition
            INTO constraint_record
            FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.conname) = (table_oid, exclude_constraint);

            IF FOUND THEN
                v_exclude_constraint_found := true;

                IF constraint_record.contype <> 'x' THEN
                    RAISE EXCEPTION 'constraint "%" is not an EXCLUDE constraint', exclude_constraint;
                END IF;

                IF NOT constraint_record.condeferrable THEN
                    RAISE EXCEPTION 'constraint "%" must be DEFERRABLE', exclude_constraint;
                END IF;

                IF constraint_record.condeferred THEN
                    RAISE EXCEPTION 'constraint "%" must be INITIALLY IMMEDIATE', exclude_constraint;
                END IF;

                IF constraint_record.definition <> exclude_sql THEN
                    RAISE EXCEPTION 'constraint "%" does not match. Expected definition: "%", but found definition: "%".',
                        exclude_constraint,
                        exclude_sql,
                        constraint_record.definition;
                END IF;

                /* Looks good, let's use it. */
            END IF;
        END IF;
    END IF;

    /*
     * Generate a name for the unique constraint.  We don't have to worry about
     * concurrency here because all period ddl commands lock the periods table.
     */
    IF unique_key_name IS NULL THEN
        unique_key_name := sql_saga.__internal_make_name(
            ARRAY[(SELECT c.relname FROM pg_catalog.pg_class AS c WHERE c.oid = table_oid)]
                || column_names
                || ARRAY[era_name]);
    END IF;
    pass := 0;
    WHILE EXISTS (
       SELECT FROM sql_saga.unique_keys AS uk
       WHERE uk.unique_key_name = unique_key_name || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END)
    LOOP
       pass := pass + 1;
    END LOOP;
    unique_key_name := unique_key_name || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END;

    /* Time to make the underlying constraints */
    IF key_type = 'primary' THEN
        SELECT array_agg(format('ALTER COLUMN %I SET NOT NULL', u.column_name))
        INTO alter_cmds
        FROM unnest(column_names) WITH ORDINALITY AS u (column_name, ordinality);
    ELSE
        alter_cmds := '{}';
    END IF;

    IF key_type IN ('primary', 'natural') THEN
        -- For XOR keys, we don't create a single overarching UNIQUE constraint.
        -- Uniqueness is handled by the partial indexes.
        IF mutually_exclusive_columns IS NULL THEN
            IF NOT v_unique_constraint_found THEN
                IF unique_constraint IS NULL THEN
                    -- If this is a primary key, Postgres will name it automatically.
                    -- For natural keys, we'll generate a name for consistency.
                    IF key_type = 'natural' THEN
                        unique_constraint := unique_key_name || '_uniq';
                        alter_cmds := alter_cmds || ('ADD CONSTRAINT ' || quote_ident(unique_constraint) || ' ' || unique_sql);
                    ELSE
                        alter_cmds := alter_cmds || ('ADD ' || unique_sql);
                    END IF;
                ELSE
                     alter_cmds := alter_cmds || ('ADD CONSTRAINT ' || quote_ident(unique_constraint) || ' ' || unique_sql);
                END IF;
            END IF;
        END IF;
    ELSIF key_type = 'predicated' THEN
        -- For predicated keys, uniqueness is handled by the EXCLUDE constraint, not a UNIQUE index.
        -- No action needed here for unique_sql.
    END IF;

    -- For XOR keys, we create partial exclusion constraints instead of one broad one.
    IF mutually_exclusive_columns IS NULL THEN
        -- Add the exclusion constraint only for predicated keys.
        IF key_type = 'predicated' AND NOT v_exclude_constraint_found THEN
            IF exclude_constraint IS NULL THEN
                exclude_constraint := unique_key_name || '_excl';
            END IF;
            alter_cmds := alter_cmds || ('ADD CONSTRAINT ' || quote_ident(exclude_constraint) || ' ' || exclude_sql);
        END IF;
    ELSE -- mutually_exclusive_columns IS NOT NULL (XOR keys)
        BEGIN
            -- XOR keys now use a single UNIQUE constraint with NULLS NOT DISTINCT
            -- This replaces the complex CHECK + partial indexes + partial EXCLUDE constraints

            -- For XOR keys, we use the unified unique constraint approach
            IF unique_constraint IS NULL THEN
                unique_constraint := unique_key_name;
            END IF;

            -- Generate UNIQUE constraint with NULLS NOT DISTINCT to handle XOR columns
            unique_sql := format('UNIQUE NULLS NOT DISTINCT (%s, %I WITHOUT OVERLAPS)',
                (SELECT string_agg(format('%I', column_name), ', ') FROM unnest(column_names) AS column_name),
                era_row.range_column_name
            );

            alter_cmds := alter_cmds || ('ADD CONSTRAINT ' || quote_ident(unique_constraint) || ' ' || unique_sql);

            -- 1.15 (hug bjd8v7): at most one mutually exclusive column holds a value per row; all NULL is allowed
            -- (before the key starts). A constraint of that name with exactly this definition is adopted.
            v_check_constraint := sql_saga.__internal_make_name(ARRAY[unique_key_name], 'xor_check');
            pass := 0;
            WHILE EXISTS (
                SELECT FROM pg_catalog.pg_constraint AS c
                WHERE c.conrelid = table_oid
                  AND c.conname = v_check_constraint || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END
                  AND pg_catalog.pg_get_constraintdef(c.oid) <> sql_saga.__internal_xor_check_definition(mutually_exclusive_columns))
            LOOP
                pass := pass + 1;
            END LOOP;
            v_check_constraint := v_check_constraint || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END;
            IF NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS c WHERE c.conrelid = table_oid AND c.conname = v_check_constraint) THEN
                alter_cmds := alter_cmds || pg_catalog.format('ADD CONSTRAINT %I %s', v_check_constraint,
                    sql_saga.__internal_xor_check_definition(mutually_exclusive_columns));
            END IF;
            exclude_constraint := NULL;
        END;
    END IF;

    -- For native temporal keys, there is no separate exclusion constraint.
    IF key_type IN ('primary', 'natural') THEN
        exclude_constraint := NULL;
    END IF;

    -- A natural key is bound to its entity both ways (1.14, 1.15): with a primary key the guards are built now; without
    -- one the state is pending ('{}') until add_unique_key adds the primary key. '{}' with a primary key means both
    -- guards are implied (the natural key is the primary key's columns).
    IF key_type = 'natural' THEN
        DECLARE
            pk_column_names name[];
            consistency_result jsonb;
        BEGIN
            SELECT uk.column_names INTO pk_column_names FROM sql_saga.unique_keys uk WHERE (uk.table_schema, uk.table_name) = (table_schema, table_name) AND uk.key_type = 'primary'::sql_saga.unique_key_type;
            IF NOT FOUND THEN
                SELECT array_agg(a.attname ORDER BY u.ord) INTO pk_column_names FROM pg_catalog.pg_constraint c JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS u(attnum, ord) ON TRUE JOIN pg_catalog.pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = u.attnum WHERE c.conrelid = table_oid AND c.contype = 'p';
            END IF;

            IF pk_column_names IS NULL THEN
                v_pk_consistency_constraint_names := '{}';
            ELSE
                consistency_result := sql_saga.__internal_generate_pk_consistency_constraints(
                    p_unique_key_name => unique_key_name,
                    p_natural_key_columns => column_names,
                    p_mutually_exclusive_columns => mutually_exclusive_columns,
                    p_stable_pk_columns => ARRAY(SELECT c FROM unnest(pk_column_names) AS c WHERE c <> era_row.range_column_name)
                );
                alter_cmds := alter_cmds || ARRAY(SELECT jsonb_array_elements_text(consistency_result->'cmds'));
                v_pk_consistency_constraint_names := ARRAY(SELECT jsonb_array_elements_text(consistency_result->'names'))::name[];
            END IF;
        END;
    END IF;

    IF alter_cmds <> '{}' THEN

        SELECT format('ALTER TABLE %I.%I %s', n.nspname /* %I */, c.relname /* %I */, array_to_string(alter_cmds, ', ') /* %s */)
        INTO sql
        FROM pg_catalog.pg_class AS c
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
        WHERE c.oid = table_oid;

        PERFORM sql_saga.__internal_execute_as_caller(sql);
        RAISE NOTICE 'sql_saga: Added constraints to table %: %', table_oid, array_to_string(alter_cmds, '; ');
    END IF;

    /* If we don't already have a unique_constraint, it must be the one with the highest oid */
    IF unique_constraint IS NULL AND key_type IN ('primary', 'natural') THEN
        SELECT c.conname
        INTO unique_constraint
        FROM pg_catalog.pg_constraint AS c
        WHERE c.conrelid = table_oid AND c.contype = (CASE WHEN key_type = 'primary' THEN 'p' ELSE 'u' END)
        ORDER BY oid DESC
        LIMIT 1;
    END IF;

    RAISE DEBUG 'add_unique_key: about to insert with v_stable_cols_to_store=%', v_stable_cols_to_store;

    RAISE DEBUG 'add_unique_key: Inserting into sql_saga.unique_keys: name=%, table=%.%, key_type=%, cols=%, era=%, u_constraint=%, ex_constraint=%, check_constraint=%, predicate=%, mut_ex_cols=%, pk_consistency_constraints=%',
        unique_key_name, table_schema, table_name, key_type, v_stable_cols_to_store, era_name, unique_constraint, exclude_constraint, v_check_constraint, predicate, mutually_exclusive_columns, v_pk_consistency_constraint_names;
    INSERT INTO sql_saga.unique_keys (unique_key_name, table_schema, table_name, key_type, column_names, era_name, unique_constraint, exclude_constraint, check_constraint, predicate, mutually_exclusive_columns, pk_consistency_constraint_names)
    VALUES (unique_key_name, table_schema, table_name, key_type, v_stable_cols_to_store, era_name, unique_constraint, exclude_constraint, v_check_constraint, predicate, mutually_exclusive_columns, v_pk_consistency_constraint_names);

    -- Create compound index for efficient exact match queries (needed by temporal_merge)
    -- This index helps UPDATE/DELETE operations that need to find rows by both
    -- identity columns AND exact range match
    IF era_name IS NOT NULL AND array_length(v_stable_cols_to_store, 1) > 0 THEN
        DECLARE
            compound_index_name name;
            compound_index_sql text;
            compound_columns text;
        BEGIN
            -- Build list of columns for the compound index
            compound_columns := (
                SELECT string_agg(quote_ident(c), ', ')
                FROM unnest(v_stable_cols_to_store) AS c
            );

            -- Add the range column at the end
            compound_columns := compound_columns || ', ' || quote_ident(era_row.range_column_name);

            compound_index_name := sql_saga.__internal_make_name(
                ARRAY[table_name] || v_stable_cols_to_store || ARRAY[era_row.range_column_name, 'idx']
            );

            -- Check if index already exists
            IF NOT EXISTS (
                SELECT 1 FROM pg_index i
                JOIN pg_class c ON c.oid = i.indexrelid
                WHERE i.indrelid = table_oid
                AND c.relname = compound_index_name
            ) THEN
                compound_index_sql := format(
                    'CREATE INDEX %I ON %I.%I (%s)',
                    compound_index_name,
                    table_schema,
                    table_name,
                    compound_columns
                );

                RAISE DEBUG 'add_unique_key: Creating compound index for temporal_merge performance: %', compound_index_sql;
                PERFORM sql_saga.__internal_execute_as_caller(compound_index_sql);
                RAISE DEBUG 'sql_saga: created compound index % on % for temporal_merge performance', compound_index_name, table_oid::regclass;
            END IF;
        END;
    END IF;

    -- When adding a primary key, bind every natural key that was waiting for one (state '{}').
    IF key_type = 'primary' THEN
        DECLARE
            nk_row sql_saga.unique_keys;
            consistency_result jsonb;
            consistency_cmds text[] := '{}';
        BEGIN
            FOR nk_row IN
                SELECT * FROM sql_saga.unique_keys uk
                WHERE (uk.table_schema, uk.table_name) = (table_schema, table_name)
                  AND uk.key_type = 'natural' AND uk.pk_consistency_constraint_names = '{}'
                ORDER BY uk.unique_key_name
            LOOP
                consistency_result := sql_saga.__internal_generate_pk_consistency_constraints(
                    p_unique_key_name => nk_row.unique_key_name,
                    p_natural_key_columns => nk_row.column_names,
                    p_mutually_exclusive_columns => nk_row.mutually_exclusive_columns,
                    p_stable_pk_columns => ARRAY(SELECT c FROM unnest(column_names) AS c WHERE c <> era_row.range_column_name)
                );
                consistency_cmds := consistency_cmds || ARRAY(SELECT jsonb_array_elements_text(consistency_result->'cmds'));
                -- Qualified: unique_key_name alone is this function's parameter (#variable_conflict use_variable), the
                -- primary key's name, and until 1.14 left the natural key's catalog row at '{}' with its guard built.
                UPDATE sql_saga.unique_keys AS uk
                SET pk_consistency_constraint_names = ARRAY(SELECT jsonb_array_elements_text(consistency_result->'names'))::name[]
                WHERE uk.unique_key_name = nk_row.unique_key_name;
            END LOOP;

            IF array_length(consistency_cmds, 1) > 0 THEN
                RAISE NOTICE 'sql_saga: Applied retroactive primary key consistency constraints to table %.%: %', table_schema, table_name, array_to_string(consistency_cmds, '; ');
                PERFORM sql_saga.__internal_execute_as_caller(format('ALTER TABLE %I.%I %s', table_schema, table_name, array_to_string(consistency_cmds, ', ')));
            END IF;
        END;
    END IF;

    -- Create a standard B-Tree index on the unique key columns to support
    -- fast lookups for foreign key checks (both temporal and regular).
    -- This is only created if a suitable index doesn't already exist.
    DECLARE
        index_name name;
        index_sql text;
        index_columns text;
        existing_index_oid oid;
    BEGIN
        -- Find an existing B-Tree index on exactly these columns in this order
        SELECT i.indexrelid INTO existing_index_oid
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
        JOIN pg_catalog.pg_am am ON am.oid = c.relam
        WHERE i.indrelid = table_oid
          AND i.indisvalid
          AND i.indnkeyatts = array_length(column_names, 1)
          AND i.indkey::text = array_to_string(column_attnums, ' ')
          AND am.amname = 'btree'
        LIMIT 1;

        IF existing_index_oid IS NULL THEN
            index_name := sql_saga.__internal_make_name(
                ARRAY[table_name] || column_names || ARRAY['idx']
            );
            SELECT string_agg(quote_ident(c), ', ') INTO index_columns FROM unnest(column_names) AS u(c);
            index_sql := format('CREATE INDEX %I ON %I.%I USING btree (%s)',
                index_name, /* %I */
                table_schema, /* %I */
                table_name, /* %I */
                index_columns /* %s */
            );
            RAISE DEBUG 'add_unique_key: Creating lookup index: %', index_sql;
            PERFORM sql_saga.__internal_execute_as_caller(index_sql);
        END IF;
    END;

    RETURN unique_key_name;
END;
$function$;
-- ---- end 1.3 small2 ----

CREATE OR REPLACE FUNCTION sql_saga.__internal_rename_following()
 RETURNS event_trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 -- Set a safe search path for this SECURITY DEFINER function
 SET search_path = sql_saga, pg_catalog, pg_temp
AS
$function$
#variable_conflict use_variable
DECLARE
    r record;
    sql text;
    v_is_alter_trigger boolean := false;
    is_alter_table boolean := false;
    cmd record;
    v_command_changes_shape boolean;
    v_shape_changed_table_oids oid[] := ARRAY[]::oid[];
    v_table_oid oid;
    v_current_columns name[];
    v_removed_columns name[];
    v_added_columns name[];
    v_missing_excluded name;
    v_unique_key_columns_before jsonb;
    v_altered_table_oids oid[];
    v_touched_relation_oids oid[];
BEGIN
    PERFORM sql_saga.__internal_entry_guard();
    /* DROP statements cannot rename or reshape surviving managed objects. */
    IF tg_tag LIKE 'DROP %' THEN
        RETURN;
    END IF;

    FOR cmd IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        IF cmd.command_tag = 'ALTER TABLE' THEN
            is_alter_table := true;
            IF cmd.classid = 'pg_catalog.pg_class'::regclass THEN
                v_command_changes_shape := cmd.objsubid > 0
                    OR sql_saga.__internal_alter_table_changes_column_shape(cmd.command);

                IF v_command_changes_shape THEN
                    IF EXISTS (
                        SELECT 1
                        FROM sql_saga.system_time_era AS ste
                        JOIN pg_catalog.pg_class AS c
                          ON c.oid = cmd.objid
                        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                        WHERE (ste.table_schema, ste.table_name) = (n.nspname, c.relname))
                       AND NOT cmd.objid = ANY (v_shape_changed_table_oids) THEN
                        v_shape_changed_table_oids := pg_catalog.array_append(v_shape_changed_table_oids, cmd.objid);
                    END IF;

                    -- PostgreSQL propagates column-shape changes through table inheritance. Avoid all
                    -- pg_inherits work for the overwhelmingly common no-children case.
                    IF (SELECT c.relhassubclass FROM pg_catalog.pg_class AS c WHERE c.oid = cmd.objid) THEN
                        FOR v_table_oid IN
                            WITH RECURSIVE descendants(table_oid) AS (
                                SELECT i.inhrelid
                                FROM pg_catalog.pg_inherits AS i
                                WHERE i.inhparent = cmd.objid
                                UNION
                                SELECT i.inhrelid
                                FROM descendants AS d
                                JOIN pg_catalog.pg_inherits AS i ON i.inhparent = d.table_oid
                            )
                            SELECT c.oid
                            FROM descendants AS d
                            JOIN pg_catalog.pg_class AS c ON c.oid = d.table_oid
                            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                            JOIN sql_saga.system_time_era AS ste
                              ON (ste.table_schema, ste.table_name) = (n.nspname, c.relname)
                            JOIN LATERAL (
                                SELECT pg_catalog.array_agg(a.attname ORDER BY a.attname COLLATE "C") AS column_names
                                FROM pg_catalog.pg_attribute AS a
                                WHERE a.attrelid = c.oid
                                  AND a.attnum > 0
                                  AND NOT a.attisdropped
                            ) AS live ON true
                            -- Set comparison: only added/removed columns are shape changes.
                            WHERE (SELECT pg_catalog.array_agg(s.attname ORDER BY s.attname COLLATE "C")
                                   FROM pg_catalog.unnest(ste.table_columns_snapshot) AS s(attname))
                                  IS DISTINCT FROM live.column_names
                        LOOP
                            IF NOT v_table_oid = ANY (v_shape_changed_table_oids) THEN
                                v_shape_changed_table_oids := pg_catalog.array_append(
                                    v_shape_changed_table_oids, v_table_oid);
                            END IF;
                        END LOOP;
                    END IF;
                END IF;
            END IF;
        END IF;
    END LOOP;

    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        IF r.command_tag = 'ALTER TRIGGER' THEN
            v_is_alter_trigger := true;
            exit;
        END IF;
    END LOOP;

    -- Exit early if the DDL command does not affect a managed object.
    -- We bypass this for ALTER TRIGGER because the object identity contains
    -- the *new* trigger name, which won't be in our metadata yet.
    IF NOT v_is_alter_trigger
       AND cardinality(v_shape_changed_table_oids) = 0
       AND NOT sql_saga.__internal_ddl_command_affects_managed_object() THEN
        RETURN;
    END IF;

    /*
     * The relations this command touched: tables, views and indexes it names directly, the tables of
     * the constraints, triggers, policies and indexes it names, and their inheritance descendants
     * (PostgreSQL propagates renames to them). Built once from the catalog only.
     *
     * The constraint renames below are found by comparing constraint definitions
     * (pg_get_constraintdef), and deparsing a CHECK constraint takes a lock on its table. They only
     * deparse constraints of these relations: a rename can only happen on a relation the command
     * touched, and a concurrent session (such as another pg_restore -j worker) may hold an exclusive
     * lock on any other table while it waits for one of ours, which would be a deadlock.
     */
    v_touched_relation_oids := ARRAY(
        WITH RECURSIVE touched(relation_oid) AS (
            SELECT CASE ev.classid
                       WHEN 'pg_catalog.pg_class'::regclass THEN
                           coalesce((SELECT i.indrelid FROM pg_catalog.pg_index AS i WHERE i.indexrelid = ev.objid), ev.objid)
                       WHEN 'pg_catalog.pg_constraint'::regclass THEN
                           (SELECT c.conrelid FROM pg_catalog.pg_constraint AS c WHERE c.oid = ev.objid)
                       WHEN 'pg_catalog.pg_trigger'::regclass THEN
                           (SELECT t.tgrelid FROM pg_catalog.pg_trigger AS t WHERE t.oid = ev.objid)
                       WHEN 'pg_catalog.pg_policy'::regclass THEN
                           (SELECT p.polrelid FROM pg_catalog.pg_policy AS p WHERE p.oid = ev.objid)
                   END
            FROM pg_catalog.pg_event_trigger_ddl_commands() AS ev
            UNION
            SELECT i.inhrelid
            FROM touched AS t
            JOIN pg_catalog.pg_inherits AS i ON i.inhparent = t.relation_oid
        )
        SELECT t.relation_oid FROM touched AS t WHERE t.relation_oid IS NOT NULL);

    /*
     * Anything that is stored by reg* type will auto-adjust, but anything we
     * store by name will need to be updated after a rename. One way to do this
     * is to recreate the constraints we have and pull new names out that way.
     * If we are unable to do something like that, we must raise an exception.
     */

    ---
    --- era
    ---

    /*
     * Follow renames of the era's temporal columns (range, valid_from, valid_until, valid_to) on the
     * relations touched by this ALTER TABLE, and regenerate what names them (the synchronization
     * function, the referenced-side triggers of regular foreign keys). Runs before the unique keys
     * are followed, which exclude the era's range column by name.
     */
    IF is_alter_table THEN
        PERFORM sql_saga.__internal_era_column_rename_follow(ARRAY(
            SELECT DISTINCT c.objid
            FROM pg_catalog.pg_event_trigger_ddl_commands() AS c
            WHERE c.command_tag = 'ALTER TABLE' AND c.classid = 'pg_catalog.pg_class'::regclass));
    END IF;

    -- ---- 1.3 small2: era.ephemeral_columns follow RENAME COLUMN, pk consistency names follow RENAME CONSTRAINT ----
    IF is_alter_table THEN
        PERFORM sql_saga.__internal_era_ephemeral_and_pk_consistency_rename_follow();
    END IF;
    -- ---- end 1.3 small2 ----

    -- Follow renames of the bounds check constraint (NOT isempty(range)).
    FOR sql IN
        SELECT format('UPDATE sql_saga.era SET bounds_check_constraint = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            c.conname,
            e.table_schema,
            e.table_name,
            e.era_name
        )
        FROM sql_saga.era AS e
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (e.table_name, (SELECT oid FROM pg_namespace WHERE nspname = e.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        WHERE e.bounds_check_constraint IS NOT NULL
          AND e.bounds_check_constraint <> c.conname
          AND pc.oid = ANY (v_touched_relation_oids)
          AND sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = format('CHECK ((NOT isempty(%I)))', e.range_column_name)
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, e.bounds_check_constraint))
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames of the boundary check constraint (from < until).
    FOR sql IN
        SELECT format('UPDATE sql_saga.era SET boundary_check_constraint = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            c.conname,
            e.table_schema,
            e.table_name,
            e.era_name
        )
        FROM sql_saga.era AS e
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (e.table_name, (SELECT oid FROM pg_namespace WHERE nspname = e.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        WHERE e.boundary_check_constraint IS NOT NULL
          AND e.boundary_check_constraint <> c.conname
          AND e.valid_from_column_name IS NOT NULL
          AND e.valid_until_column_name IS NOT NULL
          AND pc.oid = ANY (v_touched_relation_oids)
          AND sql_saga.__internal_era_boundary_check_matches(sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids), e.valid_from_column_name, e.valid_until_column_name)
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, e.boundary_check_constraint))
    LOOP
        EXECUTE sql;
    END LOOP;

    ---
    --- system_time_era
    ---

    -- Follow renames of the infinity_check_constraint.
    -- The constraint format is: CHECK ((upper(range_column_name) = 'infinity'::timestamp with time zone))
    FOR sql IN
        SELECT format('UPDATE sql_saga.system_time_era SET infinity_check_constraint = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            c.conname,
            ste.table_schema,
            ste.table_name,
            ste.era_name
        )
        FROM sql_saga.system_time_era AS ste
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (ste.table_schema, ste.table_name, ste.era_name)
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (ste.table_name, (SELECT oid FROM pg_namespace WHERE nspname = ste.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        WHERE ste.infinity_check_constraint <> c.conname
          AND pc.oid = ANY (v_touched_relation_oids)
          AND sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = format('CHECK ((upper(%I) = ''infinity''::timestamp with time zone))', e.range_column_name)
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, ste.infinity_check_constraint))
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames of the generated_always_trigger.
    FOR sql IN
        SELECT format('UPDATE sql_saga.system_time_era SET generated_always_trigger = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            t.tgname,
            ste.table_schema,
            ste.table_name,
            ste.era_name
        )
        FROM sql_saga.system_time_era AS ste
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (ste.table_name, (SELECT oid FROM pg_namespace WHERE nspname = ste.table_schema))
        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = pc.oid
        WHERE t.tgname <> ste.generated_always_trigger
          AND t.tgfoid = 'sql_saga.generated_always_as_row_start_end()'::regprocedure
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (pc.oid, ste.generated_always_trigger))
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames of the write_history_trigger.
    FOR sql IN
        SELECT format('UPDATE sql_saga.system_time_era SET write_history_trigger = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            t.tgname,
            ste.table_schema,
            ste.table_name,
            ste.era_name
        )
        FROM sql_saga.system_time_era AS ste
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (ste.table_name, (SELECT oid FROM pg_namespace WHERE nspname = ste.table_schema))
        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = pc.oid
        WHERE t.tgname <> ste.write_history_trigger
          AND t.tgfoid = 'sql_saga.write_history()'::regprocedure
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (pc.oid, ste.write_history_trigger))
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames of the truncate_trigger.
    FOR sql IN
        SELECT format('UPDATE sql_saga.system_time_era SET truncate_trigger = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            t.tgname,
            ste.table_schema,
            ste.table_name,
            ste.era_name
        )
        FROM sql_saga.system_time_era AS ste
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (ste.table_name, (SELECT oid FROM pg_namespace WHERE nspname = ste.table_schema))
        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = pc.oid
        WHERE t.tgname <> ste.truncate_trigger
          AND t.tgfoid = 'sql_saga.truncate_system_versioning()'::regprocedure
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (pc.oid, ste.truncate_trigger))
    LOOP
        EXECUTE sql;
    END LOOP;

    /*
     * Attribute base-column changes only to registered base relations present in this ALTER TABLE
     * invocation. The persisted snapshot supplies the pre-command names without storing OIDs.
     */
    FOREACH v_table_oid IN ARRAY v_shape_changed_table_oids
    LOOP
        SELECT pg_catalog.array_agg(a.attname ORDER BY a.attnum)
        INTO v_current_columns
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = v_table_oid
          AND a.attnum > 0
          AND NOT a.attisdropped;

        SELECT pg_catalog.array_agg(s.column_name ORDER BY s.ordinality)
        INTO v_removed_columns
        FROM sql_saga.system_time_era AS ste
        CROSS JOIN LATERAL pg_catalog.unnest(ste.table_columns_snapshot) WITH ORDINALITY AS s(column_name, ordinality)
        WHERE pg_catalog.to_regclass(pg_catalog.format('%I.%I', ste.table_schema, ste.table_name)) = v_table_oid
          AND NOT s.column_name = ANY (v_current_columns);

        SELECT pg_catalog.array_agg(c.column_name ORDER BY c.ordinality)
        INTO v_added_columns
        FROM pg_catalog.unnest(v_current_columns) WITH ORDINALITY AS c(column_name, ordinality)
        JOIN sql_saga.system_time_era AS ste
          ON pg_catalog.to_regclass(pg_catalog.format('%I.%I', ste.table_schema, ste.table_name)) = v_table_oid
        WHERE NOT c.column_name = ANY (ste.table_columns_snapshot);

        SELECT removed.column_name
        INTO v_missing_excluded
        FROM pg_catalog.unnest(coalesce(v_removed_columns, ARRAY[]::name[])) AS removed(column_name)
        JOIN sql_saga.system_time_era AS ste
          ON pg_catalog.to_regclass(pg_catalog.format('%I.%I', ste.table_schema, ste.table_name)) = v_table_oid
        WHERE removed.column_name = ANY (ste.excluded_column_names)
        ORDER BY removed.column_name
        LIMIT 1;

        IF v_missing_excluded IS NOT NULL THEN
            IF cardinality(v_removed_columns) = 1 AND cardinality(v_added_columns) = 1 THEN
                RAISE EXCEPTION 'cannot drop or rename column "%" on table "%" because it is excluded from system_time era',
                    v_missing_excluded, sql_saga.__internal_display_name(v_table_oid);
            END IF;
            RAISE EXCEPTION 'cannot determine whether excluded column "%" on table "%" was renamed because ALTER TABLE changed multiple columns; split column-shape changes into separate ALTER TABLE statements',
                v_missing_excluded, sql_saga.__internal_display_name(v_table_oid);
        END IF;
    END LOOP;

    ---
    --- updatable_view
    ---
    DECLARE
        v_view_row sql_saga.updatable_view;
        v_view_oid regclass;
        v_new_trigger_name name;
    BEGIN
        FOR v_view_row IN SELECT * FROM sql_saga.updatable_view
        LOOP
            v_view_oid := to_regclass(format('%I.%I', v_view_row.view_schema /* %I */, v_view_row.view_name /* %I */));

            -- Check if the registered trigger still exists on the view
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = v_view_oid AND tgname = v_view_row.trigger_name) THEN
                -- If it's stale, find the *new* row trigger on that view that points to our function, and keep
                -- the apply trigger (1.6 for_portion_of, 1.7 current), named after the row trigger, paired with it.
                SELECT tgname INTO v_new_trigger_name FROM pg_trigger
                WHERE tgrelid = v_view_oid
                  AND tgfoid = CASE v_view_row.view_type
                      WHEN 'for_portion_of' THEN 'sql_saga.for_portion_of_trigger()'::regprocedure
                      ELSE 'sql_saga.current_view_trigger()'::regprocedure END;

                IF FOUND THEN
                    sql := format('UPDATE sql_saga.updatable_view SET trigger_name = %L WHERE (view_schema, view_name) = (%L, %L)',
                        v_new_trigger_name, /* %L */
                        v_view_row.view_schema, /* %L */
                        v_view_row.view_name /* %L */
                    );
                    EXECUTE sql;
                    IF EXISTS (SELECT FROM pg_trigger
                               WHERE tgrelid = v_view_oid AND tgname = v_view_row.trigger_name || '_apply'
                                 AND tgfoid = CASE v_view_row.view_type
                                     WHEN 'for_portion_of' THEN 'sql_saga.for_portion_of_apply_trigger()'::regprocedure
                                     ELSE 'sql_saga.current_view_apply_trigger()'::regprocedure END)
                       AND NOT EXISTS (SELECT FROM pg_trigger WHERE tgrelid = v_view_oid AND tgname = v_new_trigger_name || '_apply') THEN
                        PERFORM sql_saga.__internal_execute_as_caller(format('ALTER TRIGGER %I ON %s RENAME TO %I',
                            v_view_row.trigger_name || '_apply', v_view_oid, v_new_trigger_name || '_apply'));
                        RAISE NOTICE 'sql_saga: Renamed trigger "%" on view "%" to "%" to follow the rename of trigger "%"',
                            v_view_row.trigger_name || '_apply', sql_saga.__internal_display_name(v_view_oid),
                            v_new_trigger_name || '_apply', v_view_row.trigger_name;
                    END IF;
                END IF;
            END IF;
        END LOOP;
    END;

    -- A renamed column of a unique key referenced by a regular foreign key: attribute the dependent
    -- helper functions and referenced-side triggers before anything changes, and remember the key
    -- columns, so that they can be regenerated below. Only relations touched by this ALTER TABLE
    -- are considered; any other ALTER TABLE skips this work.
    IF is_alter_table THEN
        v_altered_table_oids := ARRAY(
            SELECT DISTINCT c.objid
            FROM pg_catalog.pg_event_trigger_ddl_commands() AS c
            WHERE c.command_tag = 'ALTER TABLE' AND c.classid = 'pg_catalog.pg_class'::regclass);
        IF EXISTS (
            SELECT FROM pg_catalog.pg_class AS c
            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
            JOIN sql_saga.unique_keys AS uk ON (uk.table_schema, uk.table_name) = (n.nspname, c.relname)
            JOIN sql_saga.foreign_keys AS fk ON fk.unique_key_name = uk.unique_key_name AND fk.type = 'regular_to_temporal'
            WHERE c.oid = ANY (v_altered_table_oids)) THEN
            v_unique_key_columns_before := sql_saga.__internal_regular_fk_key_rename_prepare(v_altered_table_oids);
        END IF;
    END IF;

    ---
    --- unique_keys
    ---

    FOR sql IN
        -- Follow renames for columns in non-XOR unique keys
        SELECT format('UPDATE sql_saga.unique_keys SET column_names = %L WHERE unique_key_name = %L',
            a.column_names, /* %L */
            uk.unique_key_name /* %L */
        )
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT oid FROM pg_namespace WHERE nspname = uk.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON (c.conrelid, c.conname) = (pc.oid, uk.unique_constraint)
        JOIN LATERAL (
            SELECT array_agg(a.attname ORDER BY u.ordinality) AS column_names
            FROM unnest(c.conkey) WITH ORDINALITY AS u (attnum, ordinality)
            JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attnum) = (pc.oid, u.attnum)
            WHERE a.attname NOT IN (e.range_column_name) -- The native constraint will include the range column
            ) AS a ON true
        WHERE uk.mutually_exclusive_columns IS NULL
          AND uk.unique_constraint IS NOT NULL
          AND uk.column_names <> a.column_names
    LOOP
        EXECUTE sql;
    END LOOP;

    /*
     * Follow renames for columns in XOR unique keys by inspecting the unified constraint.
     * column_names is stored in the order given to add_unique_key (the constraint order) and is
     * rewritten only when the SET of key column names changed (a real rename), never when only the
     * order differs. A renamed column is replaced IN PLACE, in column_names and in
     * mutually_exclusive_columns, so both keep their stored order. The old name is mapped to the new
     * one by attnum: the constraint column (by attnum) whose current name is not catalogued is the
     * renamed one, and the catalogued name no longer present on the table is its old name.
     */
    DECLARE
        v_xor_uk record;
        v_missing_names name[];
        v_unclaimed_names name[];
        v_new_column_names name[];
        v_new_xor_columns name[];
    BEGIN
        FOR v_xor_uk IN
            SELECT uk.unique_key_name, uk.table_schema, uk.table_name, uk.column_names, uk.mutually_exclusive_columns,
                   pc.oid AS table_oid,
                   (SELECT array_agg(a.attname ORDER BY u.ord)
                    FROM unnest(c.conkey) WITH ORDINALITY AS u (attnum, ord)
                    JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attnum) = (pc.oid, u.attnum)
                    WHERE a.attname <> e.range_column_name -- The native constraint includes the range column
                   ) AS constraint_column_names
            FROM sql_saga.unique_keys AS uk
            JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
            JOIN pg_catalog.pg_class AS pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT n.oid FROM pg_catalog.pg_namespace AS n WHERE n.nspname = uk.table_schema))
            JOIN pg_catalog.pg_constraint AS c ON (c.conrelid, c.conname) = (pc.oid, uk.unique_constraint)
            WHERE uk.mutually_exclusive_columns IS NOT NULL
              AND uk.unique_constraint IS NOT NULL
            ORDER BY uk.unique_key_name COLLATE "C"
        LOOP
            -- Same set of names (possibly in another order): not a rename, keep the stored order.
            CONTINUE WHEN v_xor_uk.column_names @> v_xor_uk.constraint_column_names
                      AND v_xor_uk.constraint_column_names @> v_xor_uk.column_names;

            -- Catalogued names no longer present on the table: the old names.
            v_missing_names := ARRAY(
                SELECT col.name
                FROM unnest(v_xor_uk.column_names) WITH ORDINALITY AS col (name, n)
                WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a
                                  WHERE (a.attrelid, a.attname) = (v_xor_uk.table_oid, col.name)
                                    AND a.attnum > 0 AND NOT a.attisdropped)
                ORDER BY col.n);
            -- Constraint columns (identified by attnum) whose current name is not catalogued: the new names.
            v_unclaimed_names := ARRAY(
                SELECT col.name
                FROM unnest(v_xor_uk.constraint_column_names) AS col (name)
                WHERE NOT col.name = ANY (v_xor_uk.column_names));

            IF cardinality(v_missing_names) <> 1 OR cardinality(v_unclaimed_names) <> 1 THEN
                RAISE EXCEPTION 'sql_saga: cannot follow the column rename for XOR unique key "%" on table "%.%": catalogued columns (%) no longer match the columns of its constraint (%); rename one key column per ALTER TABLE statement',
                    v_xor_uk.unique_key_name, v_xor_uk.table_schema, v_xor_uk.table_name,
                    array_to_string(v_xor_uk.column_names, ', '), array_to_string(v_xor_uk.constraint_column_names, ', ');
            END IF;

            v_new_column_names := array_replace(v_xor_uk.column_names, v_missing_names[1], v_unclaimed_names[1]);
            v_new_xor_columns := array_replace(v_xor_uk.mutually_exclusive_columns, v_missing_names[1], v_unclaimed_names[1]);

            UPDATE sql_saga.unique_keys AS uk
            SET column_names = v_new_column_names,
                mutually_exclusive_columns = v_new_xor_columns
            WHERE uk.unique_key_name = v_xor_uk.unique_key_name;
        END LOOP;
    END;

    FOR sql IN
        -- Follow renames for unique_constraint (for native temporal keys)
        SELECT format('UPDATE sql_saga.unique_keys SET unique_constraint = %L WHERE unique_key_name = %L',
            c.conname, /* %L */
            uk.unique_key_name /* %L */
        )
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT oid FROM pg_namespace WHERE nspname = uk.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        WHERE uk.unique_constraint IS NOT NULL AND uk.unique_constraint <> c.conname
          AND NOT EXISTS (SELECT FROM pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, uk.unique_constraint))
          AND pc.oid = ANY (v_touched_relation_oids)
          AND (
            -- Match traditional constraint format
            sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = (
                SELECT format('%s (%s, %I WITHOUT OVERLAPS)',
                    CASE WHEN uk.key_type = 'primary' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END,
                    string_agg(quote_ident(u.name), ', ' ORDER BY u.ordinality),
                    e.range_column_name
                )
                FROM unnest(uk.column_names) WITH ORDINALITY AS u(name, ordinality)
            )
            OR
            -- Match NULLS NOT DISTINCT constraint format
            sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = (
                SELECT format('%s (%s, %I WITHOUT OVERLAPS)',
                    CASE WHEN uk.key_type = 'primary' THEN 'PRIMARY KEY NULLS NOT DISTINCT' ELSE 'UNIQUE NULLS NOT DISTINCT' END,
                    string_agg(quote_ident(u.name), ', ' ORDER BY u.ordinality),
                    e.range_column_name
                )
                FROM unnest(uk.column_names) WITH ORDINALITY AS u(name, ordinality)
            )
          )
    LOOP
        EXECUTE sql;
    END LOOP;

    FOR sql IN
        -- Follow renames for exclude_constraint (for predicated keys)
        SELECT format('UPDATE sql_saga.unique_keys SET exclude_constraint = %L WHERE unique_key_name = %L',
            c.conname, /* %L */
            uk.unique_key_name /* %L */
        )
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT oid FROM pg_namespace WHERE nspname = uk.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        WHERE uk.exclude_constraint IS NOT NULL AND uk.exclude_constraint <> c.conname
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, uk.exclude_constraint))
          AND pc.oid = ANY (v_touched_relation_oids)
          AND sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = (
            SELECT format('EXCLUDE USING gist (%s, %I WITH &&) WHERE %s DEFERRABLE',
                string_agg(quote_ident(u.name) || ' WITH =', ', ' ORDER BY u.ordinality),
                e.range_column_name,
                CASE WHEN left(uk.predicate, 1) = '(' THEN uk.predicate ELSE '(' || uk.predicate || ')' END
            )
            FROM unnest(uk.column_names) WITH ORDINALITY AS u(name, ordinality)
          )
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames for check_constraint (for XOR keys)
    FOR sql IN
        SELECT format('UPDATE sql_saga.unique_keys SET check_constraint = %L WHERE unique_key_name = %L',
            c.conname, uk.unique_key_name
        )
        FROM sql_saga.unique_keys uk
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT oid FROM pg_namespace WHERE nspname = uk.table_schema))
        JOIN pg_constraint c ON c.conrelid = pc.oid
        WHERE uk.check_constraint IS NOT NULL
        AND uk.check_constraint <> c.conname
        AND c.contype = 'c'
        AND NOT EXISTS (SELECT FROM pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, uk.check_constraint))
        AND pc.oid = ANY (v_touched_relation_oids)
        AND sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = sql_saga.__internal_xor_check_definition(uk.mutually_exclusive_columns)
    LOOP
        EXECUTE sql;
    END LOOP;

    -- A renamed referenced key column: regenerate the dependent regular foreign key objects.
    IF v_unique_key_columns_before IS DISTINCT FROM '{}'::jsonb AND v_unique_key_columns_before IS NOT NULL THEN
        PERFORM sql_saga.__internal_regular_fk_key_rename_apply(v_unique_key_columns_before);
    END IF;

    ---
    --- foreign_keys
    ---

    DECLARE
        fk_table RECORD;
        current_columns name[];
        removed_columns name[];
        added_columns name[];
        old_col_name name;
        new_col_name name;
        fk_table_oid oid;
    BEGIN
        FOR fk_table IN
            SELECT DISTINCT table_schema, table_name
            FROM sql_saga.foreign_keys
        LOOP
            fk_table_oid := to_regclass(format('%I.%I', fk_table.table_schema /* %I */, fk_table.table_name /* %I */));

            -- If table was dropped, skip it. drop_protection will handle FK violations.
            IF fk_table_oid IS NULL THEN
                CONTINUE;
            END IF;

            -- Get current columns for this table
            SELECT array_agg(a.attname ORDER BY a.attnum) INTO current_columns
            FROM pg_catalog.pg_attribute AS a
            WHERE a.attrelid = fk_table_oid
              AND a.attnum > 0 AND NOT a.attisdropped;

            -- Find if a rename occurred by comparing current columns with the last known snapshot for this table
            DECLARE
                snapshot name[];
            BEGIN
                -- Get a snapshot from any of the FKs on this table (they are all identical for a given table)
                SELECT fk_table_columns_snapshot INTO snapshot FROM sql_saga.foreign_keys
                WHERE (table_schema, table_name) = (fk_table.table_schema, fk_table.table_name)
                LIMIT 1;

                -- If columns are unchanged, we are done with this table
                -- Set comparison: a restore can recreate the table with another column order.
                IF (SELECT array_agg(u.c ORDER BY u.c COLLATE "C") FROM unnest(snapshot) AS u(c))
                   IS NOT DISTINCT FROM
                   (SELECT array_agg(u.c ORDER BY u.c COLLATE "C") FROM unnest(current_columns) AS u(c)) THEN
                    CONTINUE;
                END IF;

                -- Find columns that were in the snapshot but are not in the current table state
                SELECT array_agg(c) INTO removed_columns
                FROM unnest(snapshot) AS u(c)
                WHERE NOT (u.c = ANY(current_columns));

                -- Find columns that are in the current table state but were not in the snapshot
                SELECT array_agg(c) INTO added_columns
                FROM unnest(current_columns) AS u(c)
                WHERE NOT (u.c = ANY(snapshot));

                -- If exactly one column was removed and one was added, we have a rename.
                IF array_length(removed_columns, 1) = 1 AND array_length(added_columns, 1) = 1 THEN
                    old_col_name := removed_columns[1];
                    new_col_name := added_columns[1];

                    -- We have a rename. PostgreSQL renames neither constraints, indexes nor triggers when a
                    -- column is renamed, so every stored object name stays unchanged; only column_names and
                    -- the snapshot follow. A regular FK's referenced-side triggers carry the referencing
                    -- column list in their arguments, so they are re-created with the same names.
                    DECLARE
                        fk_rec RECORD;
                    BEGIN
                        -- Snapshot first: the trigger DDL below re-enters this event trigger, which must
                        -- then see no pending rename.
                        UPDATE sql_saga.foreign_keys
                        SET fk_table_columns_snapshot = current_columns
                        WHERE (table_schema, table_name) = (fk_table.table_schema, fk_table.table_name);

                        FOR fk_rec IN
                            SELECT * FROM sql_saga.foreign_keys
                            WHERE (table_schema, table_name) = (fk_table.table_schema, fk_table.table_name)
                              AND old_col_name = ANY(column_names)
                            ORDER BY foreign_key_name
                        LOOP
                            /*
                             * Before the catalog follows: the referencing-side trigger (1.3) must be exactly
                             * what sql_saga generated for the columns before the rename (its arguments still
                             * name the old column), or it is a user's object and is not overwritten.
                             */
                            IF fk_rec.type = 'regular_to_temporal' AND fk_rec.fk_helper_function IS NULL
                               AND NOT sql_saga.__internal_regular_fk_child_trigger_is_canonical(fk_rec.foreign_key_name) THEN
                                RAISE EXCEPTION 'sql_saga: cannot rename a column of foreign key % on table %: the referencing-side trigger % is not the canonical sql_saga trigger',
                                    pg_catalog.quote_ident(fk_rec.foreign_key_name),
                                    pg_catalog.format('%I.%I', fk_rec.table_schema, fk_rec.table_name),
                                    pg_catalog.quote_ident(fk_rec.fk_check_constraint)
                                    USING HINT = pg_catalog.format('Confirm the intended foreign key definition, then recreate it with sql_saga.drop_foreign_key_by_name(%L, %L) and sql_saga.add_regular_foreign_key.',
                                        pg_catalog.format('%I.%I', fk_rec.table_schema, fk_rec.table_name), fk_rec.foreign_key_name);
                            END IF;
                            UPDATE sql_saga.foreign_keys
                            SET column_names = array_replace(fk_rec.column_names, old_col_name, new_col_name)
                            WHERE foreign_key_name = fk_rec.foreign_key_name;

                            IF fk_rec.type = 'regular_to_temporal' THEN
                                PERFORM sql_saga.__internal_recreate_regular_fk_uk_triggers(fk_rec.foreign_key_name);
                            END IF;
                        END LOOP;
                    END;
                END IF;

                -- Regardless of the operation (RENAME, ADD, DROP), update the snapshot
                -- to reflect the new state of the table for all FKs on that table.
                -- For DROP, drop_protection will have already fired if the column was part of an FK.
                UPDATE sql_saga.foreign_keys
                SET fk_table_columns_snapshot = current_columns
                WHERE (table_schema, table_name) = (fk_table.table_schema, fk_table.table_name);
            END;
        END LOOP;
    END;

    /*
     * Protection logic: Prevent manual renaming or dropping of managed triggers.
     * This logic is disabled for ALTER TABLE commands because any trigger changes
     * are side-effects of a column rename, which is handled above. This avoids
     * false positives caused by MVCC visibility issues within the event trigger.
     */
    IF NOT is_alter_table THEN
        -- Only on explicit ALTER TRIGGER: DROP TRIGGER is refused by drop_protection, and on any other
        -- command (for instance the CREATE POLICY of a history-policy sync while pg_restore has loaded
        -- the catalog rows but not yet created the post-data triggers) a missing trigger is not a rename.
        IF v_is_alter_trigger THEN
            FOR r IN
                SELECT fk.foreign_key_name, to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) AS table_oid, fk.uk_update_trigger AS trigger_name
                FROM sql_saga.foreign_keys AS fk
                JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
                WHERE fk.uk_update_trigger IS NOT NULL
                  AND NOT EXISTS (
                    SELECT FROM pg_catalog.pg_trigger AS t
                    WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', uk.table_schema, uk.table_name)), fk.uk_update_trigger))
                UNION ALL
                SELECT fk.foreign_key_name, to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) AS table_oid, fk.uk_delete_trigger AS trigger_name
                FROM sql_saga.foreign_keys AS fk
                JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
                WHERE fk.uk_delete_trigger IS NOT NULL
                  AND NOT EXISTS (
                    SELECT FROM pg_catalog.pg_trigger AS t
                    WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', uk.table_schema, uk.table_name)), fk.uk_delete_trigger))
                UNION ALL
                /* The TRUNCATE trigger of a regular foreign key on the referenced table (1.4). */
                SELECT fk.foreign_key_name, to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) AS table_oid, fk.uk_truncate_trigger AS trigger_name
                FROM sql_saga.foreign_keys AS fk
                JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
                WHERE fk.uk_truncate_trigger IS NOT NULL
                  AND NOT EXISTS (
                    SELECT FROM pg_catalog.pg_trigger AS t
                    WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', uk.table_schema, uk.table_name)), fk.uk_truncate_trigger))
                UNION ALL
                /* The referencing-side constraint trigger of a regular foreign key (1.3). */
                SELECT fk.foreign_key_name, to_regclass(format('%I.%I', fk.table_schema, fk.table_name)) AS table_oid, fk.fk_check_constraint AS trigger_name
                FROM sql_saga.foreign_keys AS fk
                WHERE fk.type = 'regular_to_temporal'
                  AND fk.fk_check_constraint IS NOT NULL AND fk.fk_check_constraint <> ''
                  AND to_regclass(format('%I.%I', fk.table_schema, fk.table_name)) IS NOT NULL
                  /* A pre-1.3 CHECK referencing side is not a trigger. */
                  AND NOT EXISTS (
                    SELECT FROM pg_catalog.pg_constraint AS c
                    WHERE c.conrelid = to_regclass(format('%I.%I', fk.table_schema, fk.table_name))
                      AND c.conname = fk.fk_check_constraint AND c.contype = 'c')
                  AND NOT EXISTS (
                    SELECT FROM pg_catalog.pg_trigger AS t
                    WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', fk.table_schema, fk.table_name)), fk.fk_check_constraint))
            LOOP
                RAISE EXCEPTION 'cannot drop or rename trigger "%" on table "%" because it is used in an era foreign key "%"',
                    r.trigger_name, sql_saga.__internal_display_name(r.table_oid), r.foreign_key_name;
            END LOOP;
        END IF;

        -- Protect synchronize_temporal_columns_trigger from being renamed or dropped.
        -- This trigger is essential for maintaining consistency between range and boundary columns.
        -- Only check during explicit ALTER TRIGGER commands to avoid MVCC visibility issues
        -- during other operations like ALTER TABLE OWNER or GRANT/REVOKE.
        IF v_is_alter_trigger THEN
            FOR r IN
                SELECT e.era_name, to_regclass(format('%I.%I', e.table_schema, e.table_name)) AS table_oid,
                       e.sync_temporal_trg_name, e.sync_temporal_trg_function_name
                FROM sql_saga.era AS e
                WHERE e.sync_temporal_trg_name IS NOT NULL  -- Only eras with synchronization triggers
                  AND e.sync_temporal_trg_function_name IS NOT NULL
                  AND NOT EXISTS (
                      SELECT FROM pg_catalog.pg_trigger AS t
                      WHERE t.tgrelid = to_regclass(format('%I.%I', e.table_schema, e.table_name))
                        AND t.tgname = e.sync_temporal_trg_name
                        AND t.tgfoid = to_regprocedure(format('sql_saga.%I()', e.sync_temporal_trg_function_name)))
            LOOP
                RAISE EXCEPTION 'cannot drop or rename trigger "%" on table "%" because it is managed by era "%"',
                    r.sync_temporal_trg_name, sql_saga.__internal_display_name(r.table_oid), r.era_name;
            END LOOP;
        END IF;
    END IF;

    IF cardinality(v_shape_changed_table_oids) > 0 THEN
        UPDATE sql_saga.system_time_era AS ste
        SET table_columns_snapshot = live.column_names
        FROM (
            SELECT c.oid AS table_oid,
                   pg_catalog.array_agg(a.attname ORDER BY a.attnum) AS column_names
            FROM pg_catalog.pg_class AS c
            JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.oid
            WHERE c.oid = ANY (v_shape_changed_table_oids)
              AND a.attnum > 0
              AND NOT a.attisdropped
            GROUP BY c.oid
        ) AS live
        WHERE pg_catalog.to_regclass(pg_catalog.format('%I.%I', ste.table_schema, ste.table_name)) = live.table_oid;
    END IF;

    -- ---- 1.3 A3: managed updatable views follow ADD/DROP/RENAME COLUMN (src/2o_updatable_view_column_follow.sql) ----
    /*
     * Recreate the views that __internal_updatable_view_prepare_drop_column dropped at ddl_command_start,
     * then bring the managed views of every table (and inheritance descendant) whose columns this
     * ALTER TABLE added, dropped or renamed (objsubid > 0: RENAME COLUMN) up to the base's columns, and
     * refresh their generated_columns.
     */
    IF is_alter_table THEN
        DECLARE
            v_view_shape_table_oids oid[];
        BEGIN
            v_view_shape_table_oids := ARRAY(
                WITH RECURSIVE reshaped(table_oid) AS (
                    SELECT c.objid
                    FROM pg_catalog.pg_event_trigger_ddl_commands() AS c
                    WHERE c.command_tag = 'ALTER TABLE'
                      AND c.classid = 'pg_catalog.pg_class'::regclass
                      AND (c.objsubid > 0 OR sql_saga.__internal_alter_table_changes_column_shape(c.command))
                    UNION
                    SELECT i.inhrelid
                    FROM reshaped AS rs
                    JOIN pg_catalog.pg_inherits AS i ON i.inhparent = rs.table_oid
                )
                SELECT DISTINCT rs.table_oid FROM reshaped AS rs);
            -- rename_following runs as the extension owner, the only role that can read the pending table.
            IF cardinality(v_view_shape_table_oids) > 0
               OR EXISTS (SELECT FROM sql_saga.__internal_updatable_view_pending) THEN
                PERFORM sql_saga.__internal_updatable_view_follow_shape(v_view_shape_table_oids);
            END IF;
        END;
    END IF;
    -- ---- end 1.3 A3 ----

END;
$function$;
CREATE OR REPLACE FUNCTION sql_saga.__internal_unique_key_inventory(filter_unique_key_name name DEFAULT NULL)
RETURNS TABLE(
    unique_key_name name,
    table_oid oid,
    unique_attributable boolean,
    exclude_attributable boolean,
    check_attributable boolean,
    object_missing boolean)
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_unique_key_inventory$
    WITH keys AS (
        SELECT uk.*, e.range_column_name,
               pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.table_name))::oid AS table_oid
        FROM sql_saga.unique_keys AS uk
        LEFT JOIN sql_saga.era AS e USING (table_schema, table_name, era_name)
        WHERE filter_unique_key_name IS NULL OR uk.unique_key_name = filter_unique_key_name
    ), expected AS (
        SELECT k.*,
               x.expected_attnums,
               pg_catalog.cardinality(x.expected_attnums) = pg_catalog.cardinality(k.column_names) + 1 AS columns_valid
        FROM keys AS k
        LEFT JOIN LATERAL (
            SELECT pg_catalog.array_agg(a.attnum::smallint ORDER BY u.n) AS expected_attnums
            FROM pg_catalog.unnest(k.column_names || k.range_column_name) WITH ORDINALITY AS u(column_name, n)
            JOIN pg_catalog.pg_attribute AS a
              ON a.attrelid = k.table_oid AND a.attname = u.column_name
             AND a.attnum > 0 AND NOT a.attisdropped
        ) AS x ON true
    )
    SELECT k.unique_key_name, k.table_oid,
        CASE
            WHEN k.table_oid IS NULL OR NOT k.columns_valid THEN false
            WHEN uk.unique_constraint IS NULL THEN true
            WHEN uk.key_type = 'predicated' THEN EXISTS (
                SELECT 1 FROM pg_catalog.pg_index AS i
                JOIN pg_catalog.pg_class AS ic ON ic.oid = i.indexrelid
                JOIN pg_catalog.pg_am AS am ON am.oid = ic.relam
                WHERE i.indexrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.unique_constraint))
                  AND i.indrelid = k.table_oid AND i.indisunique AND i.indisvalid AND i.indisready
                  AND i.indnkeyatts = pg_catalog.cardinality(k.expected_attnums)
                  AND i.indkey::text = pg_catalog.array_to_string(k.expected_attnums, ' ')
                  AND i.indexprs IS NULL AND am.amname = 'btree'
                  /* Stored predicates are canonical deparses (1.4); compare them exactly. */
                  AND pg_catalog.pg_get_expr(i.indpred, i.indrelid) IS NOT DISTINCT FROM uk.predicate)
            ELSE EXISTS (
                SELECT 1 FROM pg_catalog.pg_constraint AS c
                JOIN pg_catalog.pg_index AS i ON i.indexrelid = c.conindid
                JOIN pg_catalog.pg_class AS ic ON ic.oid = i.indexrelid
                JOIN pg_catalog.pg_am AS am ON am.oid = ic.relam
                WHERE c.conrelid = k.table_oid AND c.conname = uk.unique_constraint
                  AND c.contype = CASE uk.key_type WHEN 'primary' THEN 'p' ELSE 'u' END
                  AND c.conkey = k.expected_attnums AND c.conperiod
                  AND NOT c.condeferrable AND NOT c.condeferred
                  AND i.indrelid = k.table_oid AND i.indisunique AND i.indisvalid AND i.indisready
                  AND i.indnkeyatts = pg_catalog.cardinality(k.expected_attnums)
                  AND i.indkey::text = pg_catalog.array_to_string(k.expected_attnums, ' ')
                  AND i.indexprs IS NULL AND i.indpred IS NULL AND am.amname = 'gist')
        END AS unique_attributable,
        CASE
            WHEN k.table_oid IS NULL OR NOT k.columns_valid THEN false
            WHEN uk.exclude_constraint IS NULL THEN true
            ELSE EXISTS (
                SELECT 1 FROM pg_catalog.pg_constraint AS c
                JOIN pg_catalog.pg_index AS i ON i.indexrelid = c.conindid
                JOIN pg_catalog.pg_class AS ic ON ic.oid = i.indexrelid
                JOIN pg_catalog.pg_am AS am ON am.oid = ic.relam
                WHERE c.conrelid = k.table_oid AND c.conname = uk.exclude_constraint
                  AND c.contype = 'x' AND c.conkey = k.expected_attnums
                  AND c.condeferrable AND NOT c.condeferred
                  AND i.indrelid = k.table_oid AND i.indisvalid AND i.indisready
                  AND i.indnkeyatts = pg_catalog.cardinality(k.expected_attnums)
                  AND i.indkey::text = pg_catalog.array_to_string(k.expected_attnums, ' ')
                  AND i.indexprs IS NULL AND am.amname = 'gist'
                  AND pg_catalog.pg_get_expr(i.indpred, i.indrelid) IS NOT DISTINCT FROM uk.predicate
                  AND pg_catalog.cardinality(c.conexclop) = pg_catalog.cardinality(k.expected_attnums)
                  AND NOT EXISTS (
                      SELECT 1
                      FROM pg_catalog.unnest(c.conexclop) WITH ORDINALITY AS x(oproid, n)
                      JOIN pg_catalog.pg_operator AS o ON o.oid = x.oproid
                      WHERE (x.n <= pg_catalog.cardinality(uk.column_names) AND o.oprname <> '=')
                         OR (x.n = pg_catalog.cardinality(k.expected_attnums) AND o.oprname <> '&&')))
        END AS exclude_attributable,
        uk.check_constraint IS NULL OR EXISTS (
            SELECT 1 FROM pg_catalog.pg_constraint AS c
            WHERE c.conrelid = k.table_oid AND c.conname = uk.check_constraint AND c.contype = 'c' AND c.convalidated
              AND uk.mutually_exclusive_columns IS NOT NULL
              AND pg_catalog.pg_get_constraintdef(c.oid) = sql_saga.__internal_xor_check_definition(uk.mutually_exclusive_columns)) AS check_attributable,
        k.table_oid IS NULL
        OR (uk.unique_constraint IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint AS c
                            WHERE c.conrelid = k.table_oid AND c.conname = uk.unique_constraint)
            AND pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.unique_constraint)) IS NULL)
        OR (uk.exclude_constraint IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint AS c
                            WHERE c.conrelid = k.table_oid AND c.conname = uk.exclude_constraint)) AS object_missing
    FROM expected AS k
    CROSS JOIN LATERAL (SELECT k.*) AS uk;
$__internal_unique_key_inventory$;
CREATE OR REPLACE FUNCTION sql_saga.__internal_unique_key_attribution(unique_key_name name)
RETURNS TABLE(unique_attributable boolean, exclude_attributable boolean, check_attributable boolean)
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_unique_key_attribution$
    WITH key AS (
        SELECT uk.*, e.range_column_name, t.oid AS table_oid, tn.oid AS table_namespace_oid
        FROM sql_saga.unique_keys AS uk
        LEFT JOIN sql_saga.era AS e USING (table_schema, table_name, era_name)
        LEFT JOIN pg_catalog.pg_namespace AS tn ON tn.nspname = uk.table_schema
        LEFT JOIN pg_catalog.pg_class AS t ON t.relnamespace = tn.oid AND t.relname = uk.table_name
        WHERE uk.unique_key_name = $1
    ), expected AS (
        SELECT k.*, x.expected_attnums,
               pg_catalog.cardinality(x.expected_attnums) = pg_catalog.cardinality(k.column_names) + 1 AS columns_valid
        FROM key AS k
        LEFT JOIN LATERAL (
            SELECT pg_catalog.array_agg(a.attnum::smallint ORDER BY u.n) AS expected_attnums
            FROM pg_catalog.unnest(k.column_names || k.range_column_name) WITH ORDINALITY AS u(column_name, n)
            JOIN pg_catalog.pg_attribute AS a
              ON a.attrelid = k.table_oid AND a.attname = u.column_name
             AND a.attnum > 0 AND NOT a.attisdropped
        ) AS x ON true
    )
    SELECT CASE
               WHEN k.table_oid IS NULL OR NOT k.columns_valid THEN false
               WHEN k.unique_constraint IS NULL THEN true
               WHEN k.key_type = 'predicated' THEN
                   ui.indexrelid IS NOT NULL AND ui.indrelid = k.table_oid
                   AND ui.indisunique AND ui.indisvalid AND ui.indisready
                   AND ui.indnkeyatts = pg_catalog.cardinality(k.expected_attnums)
                   AND ui.indkey::text = pg_catalog.array_to_string(k.expected_attnums, ' ')
                   AND ui.indexprs IS NULL AND uam.amname = 'btree'
                   AND pg_catalog.pg_get_expr(ui.indpred, ui.indrelid) IS NOT DISTINCT FROM k.predicate
               ELSE uc.oid IS NOT NULL
                   AND uc.contype = CASE k.key_type WHEN 'primary' THEN 'p' ELSE 'u' END
                   AND uc.conkey = k.expected_attnums AND uc.conperiod
                   AND NOT uc.condeferrable AND NOT uc.condeferred
                   AND ui.indrelid = k.table_oid AND ui.indisunique AND ui.indisvalid AND ui.indisready
                   AND ui.indnkeyatts = pg_catalog.cardinality(k.expected_attnums)
                   AND ui.indkey::text = pg_catalog.array_to_string(k.expected_attnums, ' ')
                   AND ui.indexprs IS NULL AND ui.indpred IS NULL AND uam.amname = 'gist'
           END,
           CASE
               WHEN k.exclude_constraint IS NULL THEN true
               WHEN k.table_oid IS NULL OR NOT k.columns_valid THEN false
               ELSE ec.oid IS NOT NULL AND ec.contype = 'x' AND ec.conkey = k.expected_attnums
                   AND ec.condeferrable AND NOT ec.condeferred
                   AND ei.indrelid = k.table_oid AND ei.indisvalid AND ei.indisready
                   AND ei.indnkeyatts = pg_catalog.cardinality(k.expected_attnums)
                   AND ei.indkey::text = pg_catalog.array_to_string(k.expected_attnums, ' ')
                   AND ei.indexprs IS NULL AND eam.amname = 'gist'
                   AND pg_catalog.pg_get_expr(ei.indpred, ei.indrelid) IS NOT DISTINCT FROM k.predicate
                   AND pg_catalog.cardinality(ec.conexclop) = pg_catalog.cardinality(k.expected_attnums)
                   AND NOT EXISTS (
                       SELECT 1
                       FROM pg_catalog.unnest(ec.conexclop) WITH ORDINALITY AS x(oproid, n)
                       JOIN pg_catalog.pg_operator AS o ON o.oid = x.oproid
                       WHERE (x.n <= pg_catalog.cardinality(k.column_names) AND o.oprname <> '=')
                          OR (x.n = pg_catalog.cardinality(k.expected_attnums) AND o.oprname <> '&&'))
           END,
           k.check_constraint IS NULL OR EXISTS (
               SELECT 1 FROM pg_catalog.pg_constraint AS c
               WHERE c.conrelid = k.table_oid AND c.conname = k.check_constraint AND c.contype = 'c' AND c.convalidated
                 AND k.mutually_exclusive_columns IS NOT NULL
                 AND pg_catalog.pg_get_constraintdef(c.oid) = sql_saga.__internal_xor_check_definition(k.mutually_exclusive_columns))
    FROM expected AS k
    LEFT JOIN pg_catalog.pg_constraint AS uc
      ON k.key_type <> 'predicated' AND uc.conrelid = k.table_oid AND uc.conname = k.unique_constraint
    LEFT JOIN pg_catalog.pg_class AS uic
      ON uic.relnamespace = k.table_namespace_oid AND uic.relname = k.unique_constraint
    LEFT JOIN pg_catalog.pg_index AS ui
      ON ui.indexrelid = CASE WHEN k.key_type = 'predicated' THEN uic.oid ELSE uc.conindid END
    LEFT JOIN pg_catalog.pg_class AS uidx ON uidx.oid = ui.indexrelid
    LEFT JOIN pg_catalog.pg_am AS uam ON uam.oid = uidx.relam
    LEFT JOIN pg_catalog.pg_constraint AS ec
      ON k.exclude_constraint IS NOT NULL AND ec.conrelid = k.table_oid AND ec.conname = k.exclude_constraint
    LEFT JOIN pg_catalog.pg_index AS ei ON ei.indexrelid = ec.conindid
    LEFT JOIN pg_catalog.pg_class AS eidx ON eidx.oid = ei.indexrelid
    LEFT JOIN pg_catalog.pg_am AS eam ON eam.oid = eidx.relam;
$__internal_unique_key_attribution$;
CREATE OR REPLACE FUNCTION sql_saga.__internal_unique_key_object_is_attributable(
    unique_key_name name,
    object_kind text)
RETURNS boolean
LANGUAGE plpgsql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_unique_key_object_is_attributable$
#variable_conflict use_variable
DECLARE
    uk sql_saga.unique_keys;
    table_oid oid;
    range_column name;
    expected_attnums smallint[];
    object_oid oid;
    attributable boolean;
BEGIN
    SELECT x.* INTO uk FROM sql_saga.unique_keys AS x
    WHERE x.unique_key_name = $1;
    IF NOT FOUND THEN RETURN false; END IF;

    IF object_kind = 'exclude' AND uk.exclude_constraint IS NULL THEN RETURN true; END IF;
    IF object_kind = 'check' THEN
        RETURN (SELECT a.check_attributable FROM sql_saga.__internal_unique_key_attribution(uk.unique_key_name) AS a);
    END IF;

    IF object_kind = 'unique' AND uk.key_type <> 'predicated' THEN
        table_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.table_name));
        IF table_oid IS NULL THEN RETURN false; END IF;
        SELECT e.range_column_name INTO range_column
        FROM sql_saga.era AS e
        WHERE (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name);
        IF range_column IS NULL THEN RETURN false; END IF;
        SELECT pg_catalog.array_agg(a.attnum::smallint ORDER BY u.n)
        INTO expected_attnums
        FROM pg_catalog.unnest(uk.column_names || range_column) WITH ORDINALITY AS u(column_name, n)
        JOIN pg_catalog.pg_attribute AS a
          ON a.attrelid = table_oid AND a.attname = u.column_name
         AND a.attnum > 0 AND NOT a.attisdropped;
        IF pg_catalog.cardinality(expected_attnums) IS DISTINCT FROM pg_catalog.cardinality(uk.column_names) + 1 THEN
            RETURN false;
        END IF;
        RETURN EXISTS (
            SELECT 1
            FROM pg_catalog.pg_constraint AS c
            JOIN pg_catalog.pg_index AS i ON i.indexrelid = c.conindid
            JOIN pg_catalog.pg_class AS ic ON ic.oid = i.indexrelid
            JOIN pg_catalog.pg_am AS am ON am.oid = ic.relam
            WHERE c.conrelid = table_oid AND c.conname = uk.unique_constraint
              AND c.contype = CASE uk.key_type WHEN 'primary' THEN 'p' ELSE 'u' END
              AND c.conkey = expected_attnums AND c.conperiod
              AND NOT c.condeferrable AND NOT c.condeferred
              AND i.indrelid = table_oid AND i.indisunique AND i.indisvalid AND i.indisready
              AND i.indnkeyatts = pg_catalog.cardinality(expected_attnums)
              AND i.indkey::text = pg_catalog.array_to_string(expected_attnums, ' ')
              AND i.indexprs IS NULL AND i.indpred IS NULL AND am.amname = 'gist');
    END IF;

    SELECT CASE object_kind
        WHEN 'unique' THEN a.unique_attributable
        WHEN 'exclude' THEN a.exclude_attributable
        WHEN 'check' THEN a.check_attributable
        ELSE false
    END
    INTO attributable
    FROM sql_saga.__internal_unique_key_attribution(unique_key_name) AS a;
    RETURN attributable;
END;
$__internal_unique_key_object_is_attributable$;
CREATE OR REPLACE FUNCTION sql_saga.check_integrity()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = sql_saga, pg_catalog, pg_temp
AS $check_integrity$
DECLARE
    r record;
    base_oid oid;
    child_oid oid;
    child_enabled "char";
    disabled_trigger name;
    history_oid oid;
    view_oid oid;
    expected_columns name[];
    function_oid oid;
    expected_return_type oid;
    missing_column name;
    mispairing record;
    user_policy record;
BEGIN
    /*
     * sql_saga's event triggers come first: a disabled, detached or dropped one (rename_following above all) lets
     * table-level damage through, so it is reported before any damage it may have caused. Name order, COLLATE "C".
     * ENABLE ALWAYS and ENABLE REPLICA are enabled. A missing trigger cannot be attached again, so its repair
     * recreates it with the definition from the extension script (src/2j_event_triggers.sql) and then attaches it.
     * An existing trigger must also have the extension script's shape (event, tag filter as a set, function): a
     * drifted one is detached, dropped, recreated and attached again. This list is the single copy of 2j that
     * check_integrity holds, so every check_integrity on a fresh install compares it with what 2j actually created.
     */
    FOR r IN
        SELECT d.evtname, d.evtevent, d.evttags, d.evtfoid_text, et.oid AS evt_oid, et.evtenabled,
               EXISTS (SELECT 1 FROM pg_catalog.pg_depend AS dep
                       JOIN pg_catalog.pg_extension AS x ON x.oid = dep.refobjid
                       WHERE dep.classid = 'pg_catalog.pg_event_trigger'::regclass AND dep.objid = et.oid
                         AND dep.refclassid = 'pg_catalog.pg_extension'::regclass AND dep.deptype = 'e'
                         AND x.extname = 'sql_saga') AS is_member,
               (et.evtevent IS NOT DISTINCT FROM d.evtevent
                AND et.evtfoid IS NOT DISTINCT FROM pg_catalog.to_regprocedure(d.evtfoid_text || '()')::oid
                AND ARRAY(SELECT t FROM unnest(et.evttags) AS t ORDER BY t COLLATE "C")
                    = ARRAY(SELECT t FROM unnest(d.evttags) AS t ORDER BY t COLLATE "C")) AS has_canonical_shape,
               pg_catalog.format('CREATE EVENT TRIGGER %I ON %s%s EXECUTE FUNCTION %s(); ALTER EXTENSION sql_saga ADD EVENT TRIGGER %I',
                   d.evtname, d.evtevent,
                   CASE WHEN d.evttags IS NULL THEN ''
                        ELSE pg_catalog.format(' WHEN TAG IN (%s)',
                                 (SELECT pg_catalog.string_agg(pg_catalog.quote_literal(t), ', ' ORDER BY o)
                                  FROM unnest(d.evttags) WITH ORDINALITY AS u (t, o))) END,
                   d.evtfoid_text, d.evtname) AS recreate_sql
        FROM (VALUES
            ('sql_saga_drop_protection'::name, 'sql_drop'::name, NULL::text[], 'sql_saga.drop_protection'::text),
            ('sql_saga_health_checks', 'ddl_command_end', NULL, 'sql_saga.health_checks'),
            ('sql_saga_record_alter_function_resolved', 'ddl_command_start', ARRAY['ALTER FUNCTION', 'ALTER ROUTINE'],
             'sql_saga.record_alter_function_resolved'),
            ('sql_saga_refuse_managed_relation_identity_change', 'ddl_command_start', ARRAY['ALTER TABLE', 'ALTER VIEW', 'ALTER SCHEMA'],
             'sql_saga.refuse_managed_relation_identity_change'),
            ('sql_saga_rename_following', 'ddl_command_end', NULL, 'sql_saga.rename_following')
        ) AS d (evtname, evtevent, evttags, evtfoid_text)
        LEFT JOIN pg_catalog.pg_event_trigger AS et ON et.evtname = d.evtname
        ORDER BY d.evtname COLLATE "C"
    LOOP
        IF r.evt_oid IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for event trigger "%": event trigger is missing; repair with %',
                r.evtname, r.recreate_sql;
        END IF;
        IF NOT r.is_member THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for event trigger "%": event trigger is not a member of extension sql_saga; repair with ALTER EXTENSION sql_saga ADD EVENT TRIGGER %',
                r.evtname, pg_catalog.quote_ident(r.evtname);
        END IF;
        IF NOT r.has_canonical_shape THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for event trigger "%": event trigger definition differs from the extension''s; repair with %',
                r.evtname,
                pg_catalog.format('ALTER EXTENSION sql_saga DROP EVENT TRIGGER %1$I; DROP EVENT TRIGGER %1$I; %2$s',
                    r.evtname, r.recreate_sql);
        END IF;
        IF r.evtenabled = 'D' THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for event trigger "%": event trigger is disabled; repair with ALTER EVENT TRIGGER % ENABLE',
                r.evtname, pg_catalog.quote_ident(r.evtname);
        END IF;
    END LOOP;

    /*
     * A managed view dropped at ddl_command_start of an ALTER TABLE column change (DROP COLUMN or ALTER COLUMN
     * ... TYPE) is recreated, and its
     * pending row consumed, at the ddl_command_end of the same statement (src/2o). A row seen here (other
     * sessions' in-flight rows are invisible) was left behind by a statement whose end trigger did not run.
     */
    FOR r IN
        SELECT p.*,
               pg_catalog.format('DELETE FROM sql_saga.__internal_updatable_view_pending WHERE (view_schema, view_name) = (%L, %L)',
                   p.view_schema, p.view_name)
               || CASE WHEN EXISTS (SELECT FROM sql_saga.updatable_view AS uv
                                    WHERE (uv.view_schema, uv.view_name) = (p.view_schema, p.view_name)) THEN ''
                       WHEN p.view_type = 'for_portion_of' THEN
                           pg_catalog.format('; SELECT sql_saga.add_for_portion_of_view(%L, era_name => %L%s)',
                               pg_catalog.format('%I.%I', p.table_schema, p.table_name), p.era_name,
                               CASE WHEN p.ephemeral_columns IS NOT NULL
                                    THEN pg_catalog.format(', ephemeral_columns => %L', p.ephemeral_columns::text)
                                    ELSE '' END)
                       ELSE
                           pg_catalog.format('; SELECT sql_saga.add_current_view(%L, era_name => %L, delete_mode => %L, current_func_name => %L)',
                               pg_catalog.format('%I.%I', p.table_schema, p.table_name), p.era_name,
                               COALESCE(p.view_delete_mode, 'delete_as_cutoff'), p.current_func) END AS repair_sql
        FROM sql_saga.__internal_updatable_view_pending AS p
        ORDER BY p.view_schema COLLATE "C", p.view_name COLLATE "C"
    LOOP
        RAISE EXCEPTION 'sql_saga: integrity violation for updatable view "%.%" of table "%.%": view was dropped by an ALTER TABLE column change and not recreated, because event trigger sql_saga_rename_following did not run; repair with %',
            r.view_schema, r.view_name, r.table_schema, r.table_name, r.repair_sql;
    END LOOP;

    /* Base relation and era invariants are the first dependency layer. */
    FOR r IN
        SELECT e.*,
               pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))::oid AS inventory_table_oid,
               CASE
                   WHEN pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name)) IS NULL THEN 1
                   WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_class AS c
                               WHERE c.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                                 AND (c.relkind <> 'r' OR c.relpersistence <> 'p')) THEN 2
                   WHEN (e.sync_temporal_trg_function_name IS NOT NULL
                         AND e.sync_temporal_trg_function_name <> sql_saga.__internal_make_name(
                             ARRAY[e.table_schema, e.table_name, e.era_name], 'template_sync'))
                     OR EXISTS (SELECT 1 FROM sql_saga.system_time_era AS ste
                                WHERE (ste.table_schema, ste.table_name) = (e.table_schema, e.table_name)
                                  AND NOT EXISTS (SELECT 1 FROM sql_saga.era AS x
                                                  WHERE (x.table_schema, x.table_name, x.era_name) =
                                                        (ste.table_schema, ste.table_name, ste.era_name)))
                     OR EXISTS (SELECT 1 FROM sql_saga.unique_keys AS uk
                                WHERE (uk.table_schema, uk.table_name) = (e.table_schema, e.table_name)
                                  AND NOT EXISTS (SELECT 1 FROM sql_saga.era AS x
                                                  WHERE (x.table_schema, x.table_name, x.era_name) =
                                                        (uk.table_schema, uk.table_name, uk.era_name))) THEN 3
                   WHEN NOT EXISTS (
                       SELECT 1 FROM pg_catalog.pg_attribute AS a
                       JOIN pg_catalog.pg_range AS pr ON pr.rngtypid = e.range_type
                       WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                         AND a.attname = e.range_column_name AND a.attnum > 0 AND NOT a.attisdropped
                         AND a.atttypid = e.range_type AND pr.rngmultitypid = e.multirange_type
                         AND pr.rngsubtype = e.range_subtype
                         AND (SELECT t.typcategory FROM pg_catalog.pg_type AS t WHERE t.oid = e.range_subtype) = e.range_subtype_category) THEN 4
                   WHEN (e.valid_from_column_name IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_attribute AS a
                            WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND a.attname = e.valid_from_column_name AND a.atttypid = e.range_subtype
                              AND a.attnum > 0 AND NOT a.attisdropped))
                     OR (e.valid_until_column_name IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_attribute AS a
                            WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND a.attname = e.valid_until_column_name AND a.atttypid = e.range_subtype
                              AND a.attnum > 0 AND NOT a.attisdropped))
                     OR (e.bounds_check_constraint IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_constraint AS c
                            WHERE c.conrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND c.conname = e.bounds_check_constraint AND c.contype = 'c'
                              AND pg_catalog.pg_get_constraintdef(c.oid, false) LIKE '%NOT isempty(%'))
                     OR (e.boundary_check_constraint IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_constraint AS c
                            WHERE c.conrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND c.conname = e.boundary_check_constraint AND c.contype = 'c'
                              AND pg_catalog.pg_get_constraintdef(c.oid, false) LIKE '% < %'))
                     OR (e.sync_temporal_trg_name IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_trigger AS t
                            WHERE t.tgrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND t.tgname = e.sync_temporal_trg_name AND NOT t.tgisinternal AND t.tgtype = 23
                              AND t.tgfoid = pg_catalog.to_regprocedure(
                                  pg_catalog.format('sql_saga.%I()', e.sync_temporal_trg_function_name)))) THEN 5
               END AS violation
        FROM sql_saga.era AS e
        ORDER BY e.table_schema COLLATE "C", e.table_name COLLATE "C", e.era_name COLLATE "C"
    LOOP
        base_oid := r.inventory_table_oid;
        IF r.violation = 1 THEN
            /*
             * The catalogue names a table that no longer resolves: most likely renamed or moved without sql_saga
             * following (sql_saga 1.0 did not). drop_era cannot even run then; renaming or moving it back is the repair.
             */
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%.%": catalogued base table is missing; if it was renamed or moved, rename or move it back to %',
                r.table_schema, r.table_name, pg_catalog.format('%I.%I', r.table_schema, r.table_name);
        END IF;
        IF r.violation = 2 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": catalogued base relation kind or persistence is noncanonical; repair with drop_era and add_era',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF r.violation = 3 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": catalogue relationships are incoherent; repair with drop_era and add_era',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF r.violation IN (4, 5) THEN
            /*
             * A catalogued era column that no longer exists under its name was most likely renamed without
             * sql_saga following. Renaming it back is the repair, not drop_era and add_era. A column that exists
             * with the wrong type keeps the noncanonical message below.
             */
            SELECT c.column_name INTO missing_column
            FROM unnest(ARRAY[r.range_column_name, r.valid_from_column_name, r.valid_until_column_name])
                 WITH ORDINALITY AS c (column_name, position)
            WHERE c.column_name IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute AS a
                              WHERE a.attrelid = base_oid AND a.attname = c.column_name
                                AND a.attnum > 0 AND NOT a.attisdropped)
            ORDER BY c.position
            LIMIT 1;
            IF missing_column IS NOT NULL THEN
                RAISE EXCEPTION 'sql_saga: integrity violation for era "%" on table "%": column "%" is missing; if it was renamed, rename it back with ALTER TABLE % RENAME COLUMN <current name> TO %',
                    r.era_name, sql_saga.__internal_display_name(base_oid::regclass), missing_column,
                    pg_catalog.format('%I.%I', r.table_schema, r.table_name), pg_catalog.quote_ident(missing_column);
            END IF;
        END IF;
        IF r.violation = 4 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for era "%" on table "%": column or range type metadata is noncanonical; repair with drop_era and add_era',
                r.era_name, sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF r.violation = 5 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for era "%" on table "%": named check or synchronization object is noncanonical; repair with drop_era and add_era',
                r.era_name, sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
    END LOOP;

    /* The era synchronization trigger is enabled (1.14, hug a5sdk3): disabled, valid_from and valid_until stop following
     * the range column. DISABLE TRIGGER stays allowed (it is not a drop, doc/design/drop-protection.md). ENABLE ALWAYS
     * and ENABLE REPLICA are a site's choice for replication (REPLICA fires only in replica sessions), as for the
     * foreign key and provenance triggers below. Its existence and shape are checked above. */
    FOR r IN
        SELECT e.table_schema, e.table_name, e.era_name, e.sync_temporal_trg_name
        FROM sql_saga.era AS e
        JOIN pg_catalog.pg_namespace AS n ON n.nspname = e.table_schema
        JOIN pg_catalog.pg_class AS c ON (c.relnamespace, c.relname) = (n.oid, e.table_name)
        JOIN pg_catalog.pg_trigger AS t ON (t.tgrelid, t.tgname) = (c.oid, e.sync_temporal_trg_name)
        WHERE t.tgenabled = 'D'
        ORDER BY e.table_schema COLLATE "C", e.table_name COLLATE "C", e.era_name COLLATE "C"
    LOOP
        RAISE EXCEPTION 'sql_saga: integrity violation for era "%" on table "%": synchronization trigger % is disabled; repair with ALTER TABLE % ENABLE TRIGGER %',
            r.era_name, pg_catalog.format('%I.%I', r.table_schema, r.table_name), pg_catalog.quote_ident(r.sync_temporal_trg_name),
            pg_catalog.format('%I.%I', r.table_schema, r.table_name), pg_catalog.quote_ident(r.sync_temporal_trg_name);
    END LOOP;

    /* Provenance columns (1.9, doc/design/provenance.md): the managed trigger exists and is enabled. */
    FOR r IN
        SELECT e.table_schema, e.table_name, e.era_name, t.tgenabled,
               pg_catalog.format('%s_provenance', e.era_name) AS trigger_name
        FROM sql_saga.era AS e
        JOIN pg_catalog.pg_namespace AS n ON n.nspname = e.table_schema
        JOIN pg_catalog.pg_class AS c ON (c.relnamespace, c.relname) = (n.oid, e.table_name)
        LEFT JOIN pg_catalog.pg_trigger AS t
               ON (t.tgrelid, t.tgname) = (c.oid, pg_catalog.format('%s_provenance', e.era_name))
              AND t.tgfoid = 'sql_saga.provenance_trigger()'::pg_catalog.regprocedure
        WHERE e.provenance_columns IS NOT NULL AND (t.oid IS NULL OR t.tgenabled = 'D')
        ORDER BY e.table_schema, e.table_name, e.era_name
    LOOP
        IF r.tgenabled IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for era "%" on table "%": provenance trigger % is missing; repair with set_era_provenance_columns',
                r.era_name, pg_catalog.format('%I.%I', r.table_schema, r.table_name), pg_catalog.quote_ident(r.trigger_name);
        END IF;
        RAISE EXCEPTION 'sql_saga: integrity violation for era "%" on table "%": provenance trigger % is disabled; repair with ALTER TABLE % ENABLE TRIGGER %',
            r.era_name, pg_catalog.format('%I.%I', r.table_schema, r.table_name), pg_catalog.quote_ident(r.trigger_name),
            pg_catalog.format('%I.%I', r.table_schema, r.table_name), pg_catalog.quote_ident(r.trigger_name);
    END LOOP;

    /* Each provenance column keeps the DEFAULT it was registered with; without one every write is refused. */
    FOR r IN
        SELECT e.table_schema, e.table_name, e.era_name, p.col
        FROM sql_saga.era AS e
        CROSS JOIN LATERAL pg_catalog.unnest(e.provenance_columns) AS p(col)
        JOIN pg_catalog.pg_namespace AS n ON n.nspname = e.table_schema
        JOIN pg_catalog.pg_class AS c ON (c.relnamespace, c.relname) = (n.oid, e.table_name)
        JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (c.oid, p.col) AND NOT a.attisdropped
        WHERE NOT a.atthasdef OR a.attgenerated <> ''
        ORDER BY e.table_schema, e.table_name, e.era_name, p.col
    LOOP
        RAISE EXCEPTION 'sql_saga: integrity violation for era "%" on table "%": provenance column % has no DEFAULT; repair with ALTER TABLE % ALTER COLUMN % SET DEFAULT ..., or set_era_provenance_columns',
            r.era_name, pg_catalog.format('%I.%I', r.table_schema, r.table_name), pg_catalog.quote_ident(r.col),
            pg_catalog.format('%I.%I', r.table_schema, r.table_name), pg_catalog.quote_ident(r.col);
    END LOOP;

    /* SYSTEM_TIME invariants have the highest managed-object priority. */
    FOR r IN
        SELECT ste.*, e.range_column_name
        FROM sql_saga.system_time_era AS ste
        JOIN sql_saga.era AS e USING (table_schema, table_name, era_name)
        ORDER BY ste.table_schema COLLATE "C", ste.table_name COLLATE "C"
    LOOP
        base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
        IF base_oid IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%.%": catalogued base table is missing', r.table_schema, r.table_name;
        END IF;

        IF NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.generated_always_trigger)
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.write_history_trigger)
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.truncate_trigger) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": SYSTEM_TIME trigger is missing; repair with drop_system_versioning and add_system_versioning',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;

        IF NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_constraint AS c
                WHERE c.conrelid = base_oid AND c.conname = r.infinity_check_constraint
                  AND c.contype = 'c'
                  AND pg_catalog.pg_get_constraintdef(c.oid, false) LIKE '%upper(%''infinity''%')
           OR NOT sql_saga.__internal_trigger_is_attributable(
                base_oid, r.generated_always_trigger,
                'sql_saga.generated_always_as_row_start_end()'::regprocedure, 23, r.range_column_name)
           OR NOT sql_saga.__internal_trigger_is_attributable(
                base_oid, r.write_history_trigger,
                'sql_saga.write_history()'::regprocedure, 25, r.range_column_name)
           OR NOT sql_saga.__internal_trigger_is_attributable(
                base_oid, r.truncate_trigger,
                'sql_saga.truncate_system_versioning()'::regprocedure, 32, NULL) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": SYSTEM_TIME trigger definition is noncanonical; repair with drop_system_versioning and add_system_versioning',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;

        SELECT x.attname INTO missing_column
        FROM pg_catalog.unnest(r.excluded_column_names) AS x(attname)
        WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = base_oid AND a.attname = x.attname AND a.attnum > 0 AND NOT a.attisdropped)
        ORDER BY x.attname COLLATE "C" LIMIT 1;
        IF FOUND THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": excluded SYSTEM_TIME column "%" is missing; repair with set_system_time_era_excluded_columns or drop_system_versioning',
                sql_saga.__internal_display_name(base_oid::regclass), missing_column;
        END IF;

        /* The snapshot detects added and removed columns only; compare it as a set, because a
         * restore recreates inherited children with inherited columns first. */
        SELECT pg_catalog.array_agg(a.attname ORDER BY a.attname COLLATE "C") INTO expected_columns
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = base_oid AND a.attnum > 0 AND NOT a.attisdropped;
        IF expected_columns IS DISTINCT FROM (
                SELECT pg_catalog.array_agg(s.attname ORDER BY s.attname COLLATE "C")
                FROM pg_catalog.unnest(r.table_columns_snapshot) AS s(attname)) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": SYSTEM_TIME column snapshot is stale; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
    END LOOP;

    FOR r IN
        SELECT uk.*, i.table_oid, i.object_missing,
               i.unique_attributable, i.exclude_attributable, i.check_attributable
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.__internal_unique_key_inventory() AS i USING (unique_key_name)
        ORDER BY uk.table_schema COLLATE "C", uk.table_name COLLATE "C", uk.unique_key_name COLLATE "C"
    LOOP
        base_oid := r.table_oid;
        IF r.object_missing THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for unique key "%" on table "%": managed constraint or index is missing; repair with drop_unique_key and add_unique_key',
                r.unique_key_name, sql_saga.__internal_display_name(base_oid::regclass);
        END IF;

        /* 1.15 (hug bjd8v7): a natural key with mutually exclusive columns limits a row to one of them. */
        IF r.mutually_exclusive_columns IS NOT NULL AND (r.check_constraint IS NULL OR NOT r.check_attributable) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for unique key "%" on table "%": the CHECK that allows at most one of its mutually exclusive columns % per row is missing or not as add_unique_key builds it; repair with drop_unique_key and add_unique_key',
                r.unique_key_name, sql_saga.__internal_display_name(base_oid::regclass),
                pg_catalog.array_to_string(r.mutually_exclusive_columns, ', ')
                USING HINT = pg_catalog.format('Or add it and record it: ALTER TABLE %s ADD CONSTRAINT %I %s; UPDATE sql_saga.unique_keys SET check_constraint = %L WHERE unique_key_name = %L;',
                    base_oid::regclass, sql_saga.__internal_make_name(ARRAY[r.unique_key_name], 'xor_check'),
                    sql_saga.__internal_xor_check_definition(r.mutually_exclusive_columns),
                    sql_saga.__internal_make_name(ARRAY[r.unique_key_name], 'xor_check'), r.unique_key_name);
        END IF;

        IF NOT r.unique_attributable OR NOT r.exclude_attributable OR NOT r.check_attributable THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for unique key "%" on table "%": managed constraint or index is noncanonical; repair with drop_unique_key and add_unique_key',
                r.unique_key_name, sql_saga.__internal_display_name(base_oid::regclass);
        END IF;

        /* 1.14 (hug mnntde) and 1.15 (hug 40nwvn): a natural key on a table with a primary key is bound to it both ways
         * by exactly the guards add_unique_key builds now. */
        IF r.key_type = 'natural' THEN
            DECLARE
                pk_row sql_saga.unique_keys;
                range_column name;
                expected jsonb;
            BEGIN
                SELECT uk.* INTO pk_row FROM sql_saga.unique_keys AS uk
                WHERE (uk.table_schema, uk.table_name, uk.key_type) = (r.table_schema, r.table_name, 'primary');
                IF FOUND THEN
                    SELECT e.range_column_name INTO range_column FROM sql_saga.era AS e
                    WHERE (e.table_schema, e.table_name, e.era_name) = (r.table_schema, r.table_name, r.era_name);
                    expected := sql_saga.__internal_generate_pk_consistency_constraints(
                        r.unique_key_name, r.column_names, r.mutually_exclusive_columns,
                        ARRAY(SELECT c FROM pg_catalog.unnest(pk_row.column_names) AS c WHERE c <> range_column));
                    /* Count and definitions, not names: a renamed guard is followed (rename_following). */
                    IF r.pk_consistency_constraint_names IS NULL
                       OR pg_catalog.cardinality(r.pk_consistency_constraint_names) <> pg_catalog.jsonb_array_length(expected->'cmds')
                       OR EXISTS (
                            SELECT FROM pg_catalog.jsonb_array_elements_text(expected->'cmds') WITH ORDINALITY AS x(cmd, n)
                            JOIN pg_catalog.jsonb_array_elements_text(expected->'names') WITH ORDINALITY AS en(conname, n) USING (n)
                            JOIN pg_catalog.unnest(r.pk_consistency_constraint_names) WITH ORDINALITY AS g(conname, n) USING (n)
                            LEFT JOIN pg_catalog.pg_constraint AS c ON (c.conrelid, c.conname, c.contype) = (base_oid, g.conname, 'x')
                            WHERE c.oid IS NULL
                               OR x.cmd <> pg_catalog.format('ADD CONSTRAINT %I %s', en.conname, pg_catalog.pg_get_constraintdef(c.oid))) THEN
                        RAISE EXCEPTION 'sql_saga: integrity violation for unique key "%" on table "%": natural key is not bound to the primary key; repair with drop_unique_key and add_unique_key',
                            r.unique_key_name, sql_saga.__internal_display_name(base_oid::regclass);
                    END IF;
                END IF;
            END;
        END IF;
    END LOOP;

    FOR r IN
        SELECT fk.*, uk.table_schema AS uk_schema, uk.table_name AS uk_table
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        ORDER BY fk.table_schema COLLATE "C", fk.table_name COLLATE "C", fk.foreign_key_name COLLATE "C"
    LOOP
        /*
         * A foreign key whose objects pair child columns with other referenced columns than the unique key's
         * constraint order (built by sql_saga 1.0/1.1 against a sorted XOR key catalog) is reported as such, with
         * the repair, before the generic attribution checks (which it also fails).
         */
        SELECT * INTO mispairing FROM sql_saga.__internal_foreign_key_mispairing_report(r.foreign_key_name);
        IF mispairing.message IS NOT NULL THEN
            RAISE EXCEPTION USING MESSAGE = mispairing.message, HINT = mispairing.hint;
        END IF;

        IF r.type = 'temporal_to_temporal' THEN
            base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
            IF base_oid IS NULL
               OR NOT sql_saga.__internal_temporal_foreign_key_is_attributable(r.foreign_key_name) THEN
                RAISE EXCEPTION 'sql_saga: integrity violation for foreign key on table "%.%": managed constraint or trigger is noncanonical; repair with drop_foreign_key and add_foreign_key',
                    r.table_schema, r.table_name;
            END IF;
        ELSE
            base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.uk_schema, r.uk_table));
            child_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
            /*
             * 1.3: the referencing side is a constraint trigger named fk_check_constraint. A pre-1.3 row
             * (helper function, CHECK constraint) is diagnosed by the historical checks below.
             */
            IF r.fk_helper_function IS NULL AND child_oid IS NOT NULL THEN
                SELECT t.tgenabled INTO child_enabled FROM pg_catalog.pg_trigger AS t
                WHERE t.tgrelid = child_oid AND t.tgname = r.fk_check_constraint;
                IF NOT FOUND THEN
                    RAISE EXCEPTION 'sql_saga: integrity violation for foreign key %: referencing-side trigger % on table % is missing; repair with %',
                        pg_catalog.quote_ident(r.foreign_key_name), pg_catalog.quote_ident(r.fk_check_constraint),
                        pg_catalog.format('%I.%I', r.table_schema, r.table_name),
                        sql_saga.__internal_foreign_key_repair_sql(r.foreign_key_name);
                END IF;
                IF child_enabled = 'D' THEN
                    RAISE EXCEPTION 'sql_saga: integrity violation for foreign key %: referencing-side trigger % on table % is disabled; repair with ALTER TABLE % ENABLE TRIGGER %',
                        pg_catalog.quote_ident(r.foreign_key_name), pg_catalog.quote_ident(r.fk_check_constraint),
                        pg_catalog.format('%I.%I', r.table_schema, r.table_name),
                        pg_catalog.format('%I.%I', r.table_schema, r.table_name), pg_catalog.quote_ident(r.fk_check_constraint);
                END IF;
            END IF;
            /* Referenced-side triggers disabled: same style. */
            IF base_oid IS NOT NULL THEN
                SELECT t.tgname INTO disabled_trigger FROM pg_catalog.pg_trigger AS t
                WHERE t.tgrelid = base_oid AND t.tgname IN (r.uk_update_trigger, r.uk_delete_trigger, r.uk_truncate_trigger) AND t.tgenabled = 'D'
                ORDER BY t.tgname = r.uk_delete_trigger, t.tgname COLLATE "C" LIMIT 1;
                IF FOUND THEN
                    RAISE EXCEPTION 'sql_saga: integrity violation for foreign key %: referenced-side trigger % on table % is disabled; repair with ALTER TABLE % ENABLE TRIGGER %',
                        pg_catalog.quote_ident(r.foreign_key_name), pg_catalog.quote_ident(disabled_trigger),
                        pg_catalog.format('%I.%I', r.uk_schema, r.uk_table),
                        pg_catalog.format('%I.%I', r.uk_schema, r.uk_table), pg_catalog.quote_ident(disabled_trigger);
                END IF;
            END IF;
            IF base_oid IS NULL
               OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.uk_update_trigger)
               OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.uk_delete_trigger)
               OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.uk_truncate_trigger) THEN
                RAISE EXCEPTION 'sql_saga: integrity violation for foreign key on table "%.%": referenced-side helper trigger is missing; repair with drop_foreign_key and add_regular_foreign_key',
                    r.table_schema, r.table_name;
            END IF;
            IF NOT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_trigger AS t
                    WHERE t.tgrelid = base_oid AND t.tgname = r.uk_update_trigger
                      AND t.tgfoid = 'sql_saga.uk_update_check_c()'::regprocedure)
               OR NOT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_trigger AS t
                    WHERE t.tgrelid = base_oid AND t.tgname = r.uk_delete_trigger
                      AND t.tgfoid = 'sql_saga.uk_delete_check_c()'::regprocedure)
               OR NOT EXISTS (
                    /* AFTER TRUNCATE FOR EACH STATEMENT (tgtype 32), the canonical arguments, no WHEN (1.4). */
                    SELECT 1 FROM pg_catalog.pg_trigger AS t
                    WHERE t.tgrelid = base_oid AND t.tgname = r.uk_truncate_trigger
                      AND t.tgfoid = 'sql_saga.uk_truncate_check_c()'::regprocedure
                      AND t.tgtype = 32 AND t.tgqual IS NULL AND NOT t.tgisinternal
                      AND t.tgargs = sql_saga.__internal_regular_fk_trigger_args(r.foreign_key_name))
               OR (r.fk_helper_function IS NOT NULL AND NOT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_constraint AS c
                    WHERE c.conrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name))
                      AND c.conname = r.fk_check_constraint AND c.contype = 'c'))
               OR NOT sql_saga.__internal_regular_foreign_key_is_attributable(r.foreign_key_name) THEN
                RAISE EXCEPTION 'sql_saga: integrity violation for foreign key on table "%.%": managed constraint or trigger is noncanonical; repair with drop_foreign_key and add_foreign_key',
                    r.table_schema, r.table_name;
            END IF;
        END IF;
    END LOOP;

    FOR r IN
        SELECT v.* FROM sql_saga.updatable_view AS v
        ORDER BY v.table_schema COLLATE "C", v.table_name COLLATE "C", v.view_type, v.view_schema COLLATE "C", v.view_name COLLATE "C"
    LOOP
        view_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.view_schema, r.view_name));
        IF view_oid IS NULL
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = view_oid AND t.tgname = r.trigger_name) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for updatable view "%.%" of table "%.%": identifier column or trigger is missing; repair with drop_%_view and add_%_view',
                r.view_schema, r.view_name, r.table_schema, r.table_name,
                CASE r.view_type WHEN 'for_portion_of' THEN 'for_portion_of' ELSE 'current' END,
                CASE r.view_type WHEN 'for_portion_of' THEN 'for_portion_of' ELSE 'current' END;
        END IF;
        IF EXISTS (
                SELECT 1
                FROM pg_catalog.pg_attribute AS a
                WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name))
                  AND a.attnum > 0 AND NOT a.attisdropped
                  AND NOT EXISTS (
                      SELECT 1 FROM pg_catalog.pg_attribute AS va
                      WHERE va.attrelid = view_oid AND va.attname = a.attname
                        AND va.atttypid = a.atttypid AND va.attnum > 0 AND NOT va.attisdropped))
           OR NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_trigger AS t
                WHERE t.tgrelid = view_oid AND t.tgname = r.trigger_name
                  AND NOT t.tgisinternal AND t.tgtype = 93
                  AND t.tgfoid = CASE r.view_type
                      WHEN 'for_portion_of' THEN 'sql_saga.for_portion_of_trigger()'::regprocedure
                      ELSE 'sql_saga.current_view_trigger()'::regprocedure END)
           -- Each updatable view applies its queued changes with exactly one statement trigger (1.6, current 1.7).
           OR (
                SELECT pg_catalog.count(*) FROM pg_catalog.pg_trigger AS t
                WHERE t.tgrelid = view_oid AND NOT t.tgisinternal AND t.tgqual IS NULL AND t.tgnargs = 0
                  AND t.tgtype = CASE r.view_type WHEN 'for_portion_of' THEN 16 ELSE 4 END
                  AND t.tgfoid = CASE r.view_type
                      WHEN 'for_portion_of' THEN 'sql_saga.for_portion_of_apply_trigger()'::regprocedure
                      ELSE 'sql_saga.current_view_apply_trigger()'::regprocedure END) <> 1 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for updatable view "%.%" of table "%.%": identifier column or trigger definition is noncanonical; repair with drop_%_view and add_%_view',
                r.view_schema, r.view_name, r.table_schema, r.table_name,
                CASE r.view_type WHEN 'for_portion_of' THEN 'for_portion_of' ELSE 'current' END,
                CASE r.view_type WHEN 'for_portion_of' THEN 'for_portion_of' ELSE 'current' END;
        END IF;
    END LOOP;

    FOR r IN
        SELECT sv.*, a.public_view_attributable, a.gated_view_attributable
        FROM sql_saga.system_versioning AS sv
        CROSS JOIN LATERAL sql_saga.__internal_system_versioning_member_attribution(
            pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name))::oid) AS a
        ORDER BY sv.table_schema COLLATE "C", sv.table_name COLLATE "C"
    LOOP
        base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
        history_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.history_schema_name, r.history_table_name));
        view_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.view_schema_name, r.view_table_name));
        IF view_oid IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history view "%.%" is missing; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass), r.view_schema_name, r.view_table_name;
        END IF;
        IF history_oid IS NULL OR NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_class AS h
                WHERE h.oid = history_oid AND h.relkind = 'r') THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history family definition is noncanonical; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_class AS h WHERE h.oid = history_oid AND h.relpersistence <> 'p') THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history table "%" is not persistent; repair with ALTER TABLE % SET LOGGED',
                sql_saga.__internal_display_name(base_oid::regclass), sql_saga.__internal_display_name(history_oid::regclass),
                history_oid::regclass;
        END IF;
        IF EXISTS (
                SELECT 1 FROM pg_catalog.pg_class AS b JOIN pg_catalog.pg_class AS h ON h.oid = history_oid
                WHERE b.oid = base_oid AND (NOT h.relrowsecurity OR h.relforcerowsecurity <> b.relforcerowsecurity)) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history family definition is noncanonical; repair with sync_history_policies or rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF EXISTS (
                SELECT 1 FROM pg_catalog.pg_class AS b JOIN pg_catalog.pg_class AS h ON h.oid = history_oid
                WHERE b.oid = base_oid AND h.relowner <> b.relowner) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history family definition is noncanonical; repair with drop_system_versioning and add_system_versioning',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF EXISTS (
                SELECT 1 FROM pg_catalog.pg_class AS h
                WHERE h.oid = history_oid AND (
                    EXISTS (SELECT 1 FROM pg_catalog.aclexplode(coalesce(h.relacl, pg_catalog.acldefault('r', h.relowner))) AS x
                            WHERE x.grantee NOT IN (0, h.relowner) OR (x.grantee = 0 AND x.privilege_type <> 'SELECT'))
                    OR NOT EXISTS (SELECT 1 FROM pg_catalog.aclexplode(coalesce(h.relacl, pg_catalog.acldefault('r', h.relowner))) AS x
                                   WHERE x.grantee = 0 AND x.privilege_type = 'SELECT')))
           OR NOT r.public_view_attributable
           OR NOT r.gated_view_attributable THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history family definition is noncanonical; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        /*
         * A policy of the user's own on the history table: sync_history_policies refuses to remove it, so the
         * generic repair below cannot work. Name it and give the manual repair. Never touch the user's policy.
         * The first one in name order (COLLATE "C") is reported.
         */
        IF history_oid IS NOT NULL THEN
            SELECT p.polname, bn.nspname AS base_schema, b.relname AS base_name,
                   hn.nspname AS history_schema, h.relname AS history_name
              INTO user_policy
              FROM pg_catalog.pg_policy AS p
              JOIN pg_catalog.pg_class AS h ON h.oid = p.polrelid
              JOIN pg_catalog.pg_namespace AS hn ON hn.oid = h.relnamespace
              JOIN pg_catalog.pg_class AS b ON b.oid = base_oid
              JOIN pg_catalog.pg_namespace AS bn ON bn.oid = b.relnamespace
             WHERE p.polrelid = history_oid
               AND NOT sql_saga.__internal_is_managed_history_policy(p.polname)
             ORDER BY p.polname COLLATE "C"
             LIMIT 1;
            IF FOUND THEN
                RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table %: history table % has policy % not managed by sql_saga',
                    pg_catalog.format('%I.%I', user_policy.base_schema, user_policy.base_name),
                    pg_catalog.format('%I.%I', user_policy.history_schema, user_policy.history_name),
                    pg_catalog.quote_ident(user_policy.polname)
                USING HINT = pg_catalog.format(
                    'Review the policy definition, then run DROP POLICY %1$I ON %2$I.%3$I; if it is still needed, create it on %4$I.%5$I; then run SELECT sql_saga.sync_history_policies(%6$L);',
                    user_policy.polname, user_policy.history_schema, user_policy.history_name,
                    user_policy.base_schema, user_policy.base_name,
                    pg_catalog.format('%I.%I', user_policy.base_schema, user_policy.base_name));
            END IF;
        END IF;
        BEGIN
            IF history_oid IS NULL OR NOT sql_saga.__internal_history_policies_synchronized(base_oid, history_oid) THEN
                RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'policy mismatch';
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": managed history policy set is inconsistent; repair with sync_history_policies',
                sql_saga.__internal_display_name(base_oid::regclass);
        END;
    END LOOP;

    FOR r IN
        SELECT sv.table_schema, sv.table_name, sv.view_schema_name, sv.view_table_name, q.*,
               CASE q.role
                   WHEN 'as_of' THEN a.as_of_attributable
                   WHEN 'between' THEN a.between_attributable
                   WHEN 'between_symmetric' THEN a.between_symmetric_attributable
                   ELSE a.from_to_attributable
               END AS definition_attributable
        FROM sql_saga.system_versioning AS sv
        CROSS JOIN LATERAL sql_saga.__internal_system_versioning_member_attribution(
            pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name))::oid) AS a
        CROSS JOIN LATERAL sql_saga.__internal_system_versioning_query_functions(
            pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name))::oid) AS q
        ORDER BY sv.table_schema COLLATE "C", sv.table_name COLLATE "C",
                 CASE q.role WHEN 'as_of' THEN 1 WHEN 'between' THEN 2 WHEN 'between_symmetric' THEN 3 ELSE 4 END
    LOOP
        base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
        function_oid := pg_catalog.to_regprocedure(r.identity);
        IF function_oid IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%.%": generated query function role "%" is missing; repair with rebuild_system_versioning_views',
                r.table_schema, r.table_name, r.role;
        END IF;
        SELECT c.reltype INTO expected_return_type FROM pg_catalog.pg_class AS c
        WHERE c.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.view_schema_name, r.view_table_name));
        IF (SELECT p.prorettype FROM pg_catalog.pg_proc AS p WHERE p.oid = function_oid) IS DISTINCT FROM expected_return_type
           OR 1 <> (
                SELECT pg_catalog.count(*)
                FROM pg_catalog.pg_depend AS d
                WHERE d.classid = 'pg_catalog.pg_proc'::regclass
                  AND d.objid = function_oid
                  AND d.refclassid = 'pg_catalog.pg_type'::regclass
                  AND d.deptype = 'n')
           OR NOT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_depend AS d
                JOIN pg_catalog.pg_type AS t ON t.oid = d.refobjid
                WHERE d.classid = 'pg_catalog.pg_proc'::regclass
                  AND d.objid = function_oid
                  AND d.refclassid = 'pg_catalog.pg_type'::regclass
                  AND d.refobjid = expected_return_type
                  AND d.deptype = 'n'
                  AND t.typrelid = pg_catalog.to_regclass(
                      pg_catalog.format('%I.%I', r.view_schema_name, r.view_table_name))) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%.%": generated query function role "%" has a noncanonical return-type dependency; repair with rebuild_system_versioning_views',
                r.table_schema, r.table_name, r.role;
        END IF;
        IF NOT r.definition_attributable THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%.%": generated query function role "%" definition is noncanonical; repair with rebuild_system_versioning_views',
                r.table_schema, r.table_name, r.role;
        END IF;
    END LOOP;
END;
$check_integrity$;
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
    -- Natural key columns outside the identity and primary key, carried per piece (hug 40nwvn).
    v_nk_data_cols TEXT[];

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
    -- 1.15 (hug bjd8v7): the target's natural keys with mutually exclusive columns, restricted to the columns the source
    -- carries: [{"name": key, "cols": [...]}]. A source row giving more than one of a set gets an ERROR of its own.
    v_xor_sets jsonb;
    v_xor_conflict_expr text;
BEGIN
    v_log_vars := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.log_vars', true), ''), 'false')::boolean;
    v_log_id := substr(md5(COALESCE(current_setting('sql_saga.temporal_merge.log_id_seed', true), random()::text)), 1, 3);

    -- A delete mode removes target time or entities the source does not name: it belongs to the MERGE_ENTITY_* modes,
    -- which state an entity's whole timeline. The other modes act on a portion or on new entities only (hug 2hxkwx).
    IF delete_mode <> 'NONE' AND mode NOT IN ('MERGE_ENTITY_UPSERT', 'MERGE_ENTITY_PATCH', 'MERGE_ENTITY_REPLACE') THEN
        RAISE EXCEPTION 'sql_saga: delete_mode % applies to the MERGE_ENTITY_* modes, not to %', delete_mode, mode
            USING ERRCODE = 'invalid_parameter_value',
                  HINT = 'Use delete_mode => ''NONE'', or a MERGE_ENTITY_* mode.';
    END IF;

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

    SELECT COALESCE(jsonb_agg(jsonb_build_object('name', x.unique_key_name, 'cols', x.cols) ORDER BY x.unique_key_name), '[]'::jsonb)
    INTO v_xor_sets
    FROM (
        SELECT uk.unique_key_name,
               ARRAY(SELECT u.c FROM unnest(uk.mutually_exclusive_columns) WITH ORDINALITY AS u(c, n)
                     WHERE EXISTS (SELECT FROM pg_attribute AS a WHERE a.attrelid = source_table AND a.attname = u.c
                                   AND a.attnum > 0 AND NOT a.attisdropped)
                     ORDER BY u.n) AS cols
        FROM sql_saga.unique_keys AS uk
        WHERE uk.table_schema = v_target_schema_name AND uk.table_name = v_target_table_name_only
          AND uk.mutually_exclusive_columns IS NOT NULL
    ) AS x
    WHERE cardinality(x.cols) > 1;

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

    -- Natural key columns outside the identity and primary key (hug 40nwvn). They name the entity, so they are matched as
    -- keys; each piece also carries its own value in its data payload, because an entity holds one value of each but may
    -- hold none (NULL) before the key starts or after it is retired.
    v_nk_data_cols := ARRAY(SELECT c FROM unnest(v_all_lookup_cols) AS c
                            WHERE c <> ALL (COALESCE(v_identity_columns, '{}')) AND c <> ALL (COALESCE(v_pk_cols, '{}'))
                              AND c <> ALL (v_temporal_cols)
                            ORDER BY c);

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

        -- 1.15 (hug bjd8v7): the message for a source row giving more than one column of a mutually exclusive set.
        SELECT COALESCE('CASE ' || string_agg(format(
                   $$WHEN num_nonnulls(%1$s) > 1 THEN 'Source row is inconsistent. Mutually exclusive columns ' || array_to_string(ARRAY[%2$s], ', ') || ' of natural key ' || %3$L || CASE WHEN num_nonnulls(%1$s) = 2 THEN ' both have values.' ELSE ' all have values.' END$$,
                   (SELECT string_agg(format('source_table.%I', c), ', ' ORDER BY n) FROM jsonb_array_elements_text(k->'cols') WITH ORDINALITY AS u(c, n)),
                   (SELECT string_agg(format('CASE WHEN source_table.%I IS NOT NULL THEN %L END', c, c), ', ' ORDER BY n) FROM jsonb_array_elements_text(k->'cols') WITH ORDINALITY AS u(c, n)),
                   k->>'name'), ' ' ORDER BY o) || ' END', 'NULL::text')
        INTO v_xor_conflict_expr
        FROM jsonb_array_elements(v_xor_sets) WITH ORDINALITY AS x(k, o);

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
    v_plan_key_text := format('%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s',
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
        v_xor_sets,
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
            -- An ambiguous row (its keys find several entities) names the first entity found, as in the native planner,
            -- but keeps its own natural key values: no entity's are its.
            SELECT string_agg(CASE WHEN col = ANY (v_identity_columns)
                                   THEN format('COALESCE(p.%1$I, p.discovered_id_%2$s) AS %1$I', col, ord)
                                   ELSE format('CASE WHEN p.is_ambiguous THEN p.%1$I ELSE COALESCE(p.%1$I, p.discovered_id_%2$s) END AS %1$I', col, ord)
                              END, ', ')
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
            v_source_rows_exists_join_branches TEXT[]; -- one equijoin per key (hug 40nwvn)
            v_source_nk_payload_expr TEXT; -- the natural key values a source row gives (hug 40nwvn)
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
            FROM (SELECT DISTINCT unnest(v_lookup_columns || COALESCE(v_identity_columns, '{}'))) AS c(col)
            JOIN pg_attribute a ON a.attrelid = target_table AND a.attname = c.col AND NOT a.attisdropped;
            
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
                -- Natural key columns outside the identity are per-piece data, not part of the entity's key (hug 40nwvn).
                FROM unnest(v_original_entity_key_cols) AS col
                WHERE col <> ALL (v_nk_data_cols)
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
               AND v_mutually_exclusive_cols <@ v_original_entity_key_cols::name[]
               AND NOT v_mutually_exclusive_cols && v_nk_data_cols::name[] THEN
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
                        WHERE col <> ALL(v_mutually_exclusive_cols) AND col <> ALL (v_nk_data_cols)
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
                    WHERE col <> ALL (v_nk_data_cols)
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

            -- Each piece carries its natural key values outside the identity in its data (hug 40nwvn): a target piece its
            -- own (also NULL), a source row only those it gives, in every mode (a merge never clears a natural key).
            IF cardinality(v_nk_data_cols) > 0 THEN
                v_target_data_cols_jsonb_build := format('(%s || jsonb_build_object(%s))', v_target_data_cols_jsonb_build,
                    (SELECT string_agg(format('%L, t.%I', c, c), ', ') FROM unnest(v_nk_data_cols) AS c));
                v_target_data_cols_jsonb_build_bare := format('(%s || jsonb_build_object(%s))', v_target_data_cols_jsonb_build_bare,
                    (SELECT string_agg(format('%L, %I', c, c), ', ') FROM unnest(v_nk_data_cols) AS c));
                v_source_nk_payload_expr := format('jsonb_strip_nulls(jsonb_build_object(%s))',
                    (SELECT string_agg(format('%L, source_table.%I', c, c), ', ') FROM unnest(v_nk_data_cols) AS c
                     WHERE EXISTS (SELECT FROM pg_attribute AS a WHERE a.attrelid = source_table AND a.attname = c
                                     AND a.attnum > 0 AND NOT a.attisdropped)));
            END IF;
    
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
            -- Determine the set of keys to use for filtering the target table: every key a source row may name its
            -- entity by, each natural key and the identity (hug mfky60).
            v_keys_for_filtering := COALESCE(lookup_keys, '[]'::jsonb);
            IF cardinality(v_identity_columns) > 0
               AND NOT EXISTS (SELECT FROM jsonb_array_elements(v_keys_for_filtering) AS k(key) WHERE k.key = to_jsonb(v_identity_columns)) THEN
                v_keys_for_filtering := v_keys_for_filtering || jsonb_build_array(to_jsonb(v_identity_columns));
            END IF;
    
            IF delete_mode IN ('DELETE_MISSING_ENTITIES', 'DELETE_MISSING_TIMELINE_AND_ENTITIES') THEN
                -- Deleting the entities the source does not name: scan the entire target (every MERGE_ENTITY_* mode).
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
                    -- With natural key columns outside the identity, the rows a key finds may be part of their entity only
                    -- (the key is NULL before it starts and after it is retired): read every row of each entity found, by
                    -- its identity, as the native planner does (hug 40nwvn).
                    IF cardinality(v_nk_data_cols) > 0 AND cardinality(v_identity_columns) > 0 THEN
                        v_target_rows_filter := format($$(
                            SELECT * FROM %1$s AS whole
                            WHERE (%2$s) IN (SELECT %3$s FROM %4$s)
                        ) AS t $$, v_target_table_ident,
                            (SELECT string_agg(format('whole.%I', c), ', ') FROM unnest(v_identity_columns) AS c),
                            (SELECT string_agg(format('t.%I', c), ', ') FROM unnest(v_identity_columns) AS c),
                            v_target_rows_filter);
                    END IF;
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

            -- The natural key values the source row gives, in every mode but DELETE_FOR_PORTION_OF (hug 40nwvn).
            IF v_source_nk_payload_expr IS NOT NULL AND mode <> 'DELETE_FOR_PORTION_OF' THEN
                v_source_data_payload_expr := format('((%s) || %s)', v_source_data_payload_expr, v_source_nk_payload_expr);
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
            -- Build the join expression for source_rows_with_matches: one branch per key a source row may name its entity
            -- by (each natural key and the identity, hug mfky60), ORed. A key names an entity only when at least one of
            -- its columns is not NULL in the source: an all-NULL natural key matched every target row without that key
            -- (NULL IS NULL on both sides), a quadratic join and false matches (hug 40nwvn). Among the key's columns, a
            -- NULLABLE one still compares null-safely (a mutually exclusive key: the NULL column must be NULL in both).
            v_source_rows_exists_join_branches := ARRAY(
                SELECT format('((%s) AND (%s))', (
                        SELECT string_agg(
                            CASE
                                WHEN a.attnotnull THEN
                                    format('source_row.%1$I = target_row.%2$I', c, CASE WHEN c = v_valid_from_col THEN 'valid_from'::name WHEN c = v_valid_until_col THEN 'valid_until'::name ELSE c END)
                                ELSE
                                    format('(source_row.%1$I = target_row.%2$I OR (source_row.%1$I IS NULL AND target_row.%2$I IS NULL))', c, CASE WHEN c = v_valid_from_col THEN 'valid_from'::name WHEN c = v_valid_until_col THEN 'valid_until'::name ELSE c END)
                            END,
                            ' AND ' ORDER BY k.ord)
                        FROM jsonb_array_elements_text(lk.v_key) WITH ORDINALITY AS k(c, ord)
                        LEFT JOIN pg_attribute a ON a.attrelid = target_table AND a.attname = k.c AND NOT a.attisdropped
                    ), (
                        SELECT string_agg(format('source_row.%I IS NOT NULL', k.c), ' OR ' ORDER BY k.ord)
                        FROM jsonb_array_elements_text(lk.v_key) WITH ORDINALITY AS k(c, ord)
                    ))
                FROM jsonb_array_elements(v_keys_for_filtering) WITH ORDINALITY AS lk(v_key, ord)
                ORDER BY lk.ord);
            v_source_rows_exists_join_expr := COALESCE(array_to_string(v_source_rows_exists_join_branches, ' OR '), 'false');
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
            v_plan_with_op_entity_id_json_build_expr := format('jsonb_build_object(%s) || CASE WHEN d.is_ambiguous IS NOT TRUE THEN COALESCE(d.entity_nk_json, ''{}''::jsonb) ELSE ''{}''::jsonb END || COALESCE(d.stable_pk_payload, ''{}''::jsonb)', v_plan_with_op_entity_id_json_build_expr_part_A);
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
                        %11$s /* v_consistency_check_expr */ as temporal_columns_are_consistent,
                        %14$s /* v_xor_conflict_expr */ as xor_conflict
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
                v_range_constructor,                                 /* %13$I */
                v_xor_conflict_expr                                  /* %14$s */
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
    
            -- CTE 4: source_rows_with_matches. One equijoin per key, combined by UNION: an OR of the keys in one join
            -- condition can only be a nested loop (1,000 source rows against 3,000 target rows took 28 s, hug 40nwvn); each
            -- branch alone is a hash join. A source row without any match keeps one row with NULL matches.
            v_sql := format($SQL$
                CREATE TEMP TABLE source_rows_with_matches ON COMMIT DROP AS
                WITH matched AS (%3$s)
                SELECT
                    source_row.*,
                    target_row.stable_pk_payload as discovered_stable_pk_payload,
                    %1$s /* v_propagated_id_cols_list */
                FROM source_with_eclipsed_flag source_row
                LEFT JOIN matched ON matched.source_ctid = source_row.ctid
                LEFT JOIN target_rows target_row ON target_row.ctid = matched.target_ctid
            $SQL$,
                v_propagated_id_cols_list,      /* %1$s */
                NULL,
                COALESCE((
                    SELECT string_agg(format(
                        'SELECT source_row.ctid AS source_ctid, target_row.ctid AS target_ctid FROM source_with_eclipsed_flag source_row JOIN target_rows target_row ON %s',
                        branch), ' UNION ')
                    FROM unnest(v_source_rows_exists_join_branches) AS branch),
                    'SELECT NULL::tid AS source_ctid, NULL::tid AS target_ctid WHERE false') /* %3$s */
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
                    p.temporal_columns_are_consistent, p.xor_conflict,
                    COALESCE(p.stable_pk_payload, p.discovered_stable_pk_payload) as stable_pk_payload,
                    -- The identity the row names (its non-NULL columns) must be the entity's its natural key found (hug
                    -- mfky60); values compare as text, as the native planner does.
                    CASE WHEN p.discovered_stable_pk_payload IS NOT NULL AND EXISTS (
                        SELECT FROM jsonb_each(jsonb_strip_nulls(p.stable_pk_payload)) AS n(key, value)
                        WHERE (p.discovered_stable_pk_payload ->> n.key) IS DISTINCT FROM (n.value #>> '{}'))
                    THEN 'Source row is inconsistent. Its natural key matches target entity ' || p.discovered_stable_pk_payload::text
                         || ', but its identity is ' || jsonb_strip_nulls(p.stable_pk_payload)::text || '.'
                    END AS identity_conflict,
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
                                WHERE s1.is_new_entity AND s2_inner.is_new_entity AND s2_inner.xor_conflict IS NULL AND s2_inner.nk_json @> s1.nk_json
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
            
            -- CTE 9.25: source_nk_conflicts (hug 40nwvn). An entity holds at most one non-NULL value of each natural key
            -- column outside its identity, over its whole timeline. A row giving another value than the entity holds, or
            -- than another row of the batch gives for it, is refused; a row giving the one value the entity holds is not.
            IF cardinality(v_nk_data_cols) > 0 THEN
                v_sql := format($SQL$
                    CREATE TEMP TABLE source_nk_conflicts ON COMMIT DROP AS
                    WITH cand AS (
                        SELECT s.source_row_id, s.grouping_key, s.stable_pk_payload, x.col, x.val
                        FROM source_rows_with_canonical_key AS s
                        CROSS JOIN LATERAL (VALUES %1$s) AS x(col, val)
                        WHERE s.xor_conflict IS NULL AND NOT s.is_ambiguous AND s.identity_conflict IS NULL AND (s.is_identifiable OR NOT s.is_new_entity)
                          AND s.temporal_columns_are_consistent AND x.val IS NOT NULL
                    ),
                    held AS (
                        SELECT DISTINCT 'existing_entity__' || %2$s AS grouping_key, x.col, x.val
                        FROM target_rows AS t
                        CROSS JOIN LATERAL (VALUES %3$s) AS x(col, val)
                        WHERE x.val IS NOT NULL
                    ),
                    per AS (
                        SELECT v.grouping_key, v.col,
                               count(*) FILTER (WHERE v.is_held) AS n_held,
                               min(v.val #>> '{}') FILTER (WHERE v.is_held) AS held_val,
                               jsonb_agg(v.val ORDER BY v.val #>> '{}' COLLATE "C") AS vals
                        FROM (SELECT grouping_key, col, val, bool_or(is_held) AS is_held
                              FROM (SELECT grouping_key, col, val, false AS is_held FROM cand
                                    UNION ALL
                                    SELECT h.grouping_key, h.col, h.val, true FROM held AS h
                                    WHERE h.grouping_key IN (SELECT grouping_key FROM cand)) AS u
                              GROUP BY grouping_key, col, val) AS v
                        GROUP BY v.grouping_key, v.col
                        HAVING count(*) > 1
                    )
                    SELECT DISTINCT ON (c.source_row_id) c.source_row_id,
                           'Source row is inconsistent. Natural key column ' || c.col || ' of target entity '
                           || c.stable_pk_payload::text || ' would hold more than one value: ' || p.vals::text || '.' AS message
                    FROM cand AS c
                    JOIN per AS p ON (p.grouping_key, p.col) = (c.grouping_key, c.col)
                    WHERE NOT (p.n_held = 1 AND c.val #>> '{}' = p.held_val)
                    ORDER BY c.source_row_id, c.col
                $SQL$,
                    (SELECT string_agg(format('(%L, to_jsonb(s.%I))', c, c), ', ' ORDER BY c) FROM unnest(v_nk_data_cols) AS c),
                    (SELECT string_agg(format('COALESCE(t.%I::text, ''_NULL_'')', col), ' || ''__'' || ') FROM unnest(v_identity_columns) AS col),
                    (SELECT string_agg(format('(%L, to_jsonb(t.%I))', c, c), ', ' ORDER BY c) FROM unnest(v_nk_data_cols) AS c)
                );
                v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
            END IF;

            -- CTE 9.3: source_rows_with_early_feedback
            v_sql := format($SQL$
                CREATE TEMP TABLE source_rows_with_early_feedback ON COMMIT DROP AS
                SELECT
                    s.*,
                    CASE
                        WHEN s.xor_conflict IS NOT NULL
                        THEN jsonb_build_object( 'operation', 'ERROR'::text, 'message', s.xor_conflict )
                        WHEN s.is_ambiguous
                        THEN jsonb_build_object( 'operation', 'ERROR'::text, 'message', 'Source row is ambiguous. It matches multiple distinct target entities: ' || s.conflicting_ids::text )
                        WHEN s.identity_conflict IS NOT NULL
                        THEN jsonb_build_object( 'operation', 'ERROR'::text, 'message', s.identity_conflict )
                        WHEN NOT s.is_identifiable AND s.is_new_entity
                        THEN jsonb_build_object( 'operation', 'ERROR'::text, 'message', 'Source row is unidentifiable. It has NULL for all stable identity columns ' || replace(%1$L::text, '"', '') || ' and all natural keys ' || replace(%2$L::text, '"', '') )
                        WHEN NOT s.temporal_columns_are_consistent
                        THEN jsonb_build_object( 'operation', 'ERROR'::text, 'message', 'Source row has inconsistent temporal columns. Column "' || %3$L || '" must be equal to column "' || %4$L || '" + ' || %5$L || '.' )%6$s
                        WHEN s.is_eclipsed
                        THEN jsonb_build_object( 'operation', 'SKIP_ECLIPSED'::text, 'message', 'Source row was eclipsed by row_ids=' || s.eclipsed_by::text || ' in the same batch.' )
                        ELSE NULL
                    END as early_feedback
                FROM source_rows_with_canonical_key s%7$s
            $SQL$,
                v_identity_columns, /* %1$L */
                lookup_keys,        /* %2$L */
                v_valid_until_col,  /* %3$L */
                v_valid_to_col,     /* %4$L */
                v_interval,         /* %5$L */
                CASE WHEN cardinality(v_nk_data_cols) > 0 THEN E'\n                        WHEN nk.message IS NOT NULL\n                        THEN jsonb_build_object( ''operation'', ''ERROR''::text, ''message'', nk.message )' ELSE '' END, /* %6$s */
                CASE WHEN cardinality(v_nk_data_cols) > 0 THEN E'\n                LEFT JOIN source_nk_conflicts AS nk ON nk.source_row_id = s.source_row_id' ELSE '' END /* %7$s */
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

            -- The entity's natural key values (hug 40nwvn): it holds at most one of each column outside its identity (rows
            -- giving another were refused), the one a source row gives, else the one a target piece holds. Time without a
            -- target piece carries them, so it coalesces with the pieces beside it, as in the native planner.
            IF cardinality(v_nk_data_cols) > 0 THEN
                v_sql := format($SQL$
                    CREATE TEMP TABLE resolved_atomic_segments_with_entity_nk ON COMMIT DROP AS
                    SELECT r.*, e.nk AS entity_nk_payload
                    FROM %1$s AS r
                    JOIN (SELECT grouping_key, jsonb_strip_nulls(jsonb_build_object(%2$s)) AS nk
                          FROM %1$s GROUP BY grouping_key) AS e USING (grouping_key)
                $SQL$, v_resolver_from,
                    (SELECT string_agg(format('%1$L, COALESCE(sql_saga.first(s_data_payload->%1$L ORDER BY valid_from) FILTER (WHERE s_data_payload->%1$L <> ''null''::jsonb), sql_saga.first(t_data_payload->%1$L ORDER BY valid_from) FILTER (WHERE t_data_payload->%1$L <> ''null''::jsonb))', c), ', ')
                     FROM unnest(v_nk_data_cols) AS c));
                v_plan_sqls := v_plan_sqls || jsonb_build_object('type', 'ddl', 'sql', v_sql);
                v_resolver_from := 'resolved_atomic_segments_with_entity_nk';
                -- New time (no target piece) takes the entity's values under the source's.
                v_final_data_payload_expr := format($$CASE WHEN t_data_payload IS NULL AND (%1$s) IS NOT NULL THEN entity_nk_payload || (%1$s) ELSE (%1$s) END$$, v_final_data_payload_expr);
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
                    END as update_rank%2$s
                FROM diff d
            $SQL$,
                v_ephemeral_columns /* %1$L */,
                -- The entity's natural key values (hug 40nwvn) for entity_keys: a piece before the key starts holds NULL,
                -- but names the same entity, as in the native planner. A refused (ambiguous) row neither takes them nor
                -- gives its own.
                CASE WHEN cardinality(v_nk_data_cols) > 0 THEN format(E',\n                    jsonb_strip_nulls(jsonb_build_object(%s)) AS entity_nk_json',
                    (SELECT string_agg(format('%1$L, sql_saga.first(d.%1$I) FILTER (WHERE d.%1$I IS NOT NULL AND d.is_ambiguous IS NOT TRUE) OVER (PARTITION BY d.grouping_key)', c), ', ')
                     FROM unnest(v_nk_data_cols) AS c))
                ELSE E',\n                    NULL::jsonb AS entity_nk_json' END /* %2$s */
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
                                -- Every key column, NULL where no row of the new entity gives it, as for existing entities
                                -- and in the native planner.
                                THEN jsonb_build_object(%8$s) || d.canonical_nk_json || COALESCE(d.stable_pk_payload, '{}'::jsonb)
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
    IF to_regclass('pg_temp.existing_segments_with_target') IS NOT NULL THEN DROP TABLE pg_temp.existing_segments_with_target; END IF;
    IF to_regclass('pg_temp.new_segments_no_target') IS NOT NULL THEN DROP TABLE pg_temp.new_segments_no_target; END IF;
    IF to_regclass('pg_temp.resolved_atomic_segments_with_payloads') IS NOT NULL THEN DROP TABLE pg_temp.resolved_atomic_segments_with_payloads; END IF;
    IF to_regclass('pg_temp.resolved_atomic_segments_with_flag') IS NOT NULL THEN DROP TABLE pg_temp.resolved_atomic_segments_with_flag; END IF;
    IF to_regclass('pg_temp.resolved_atomic_segments_with_entity_nk') IS NOT NULL THEN DROP TABLE pg_temp.resolved_atomic_segments_with_entity_nk; END IF;
    IF to_regclass('pg_temp.source_nk_conflicts') IS NOT NULL THEN DROP TABLE pg_temp.source_nk_conflicts; END IF;
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
    v_changed bigint;  -- rows an UPDATE or DELETE changed, checked against the plan (hug 7rzgv5)
    v_planned bigint;
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
              -- A natural key column outside the identity and primary key is set like data: each piece carries its own
              -- value, and a handover fills it on existing pieces (hug 40nwvn).
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
                        GET DIAGNOSTICS v_changed = ROW_COUNT;
                        EXECUTE format('SELECT count(*) FROM temporal_merge_plan WHERE operation = ''DELETE'' AND %s', v_partition_plan_filters[v_part]) INTO v_planned;
                        IF v_changed <> v_planned THEN
                            RAISE EXCEPTION 'sql_saga: temporal_merge planned to delete % rows of %, but % changed', v_planned, temporal_merge_execute.target_table, v_changed
                                USING ERRCODE = 'internal_error',
                                      HINT = 'The target changed under the plan. A BEFORE DELETE trigger that returns NULL, or a row-level security DELETE policy narrower than its SELECT policy, also does this (doc/design/temporal-merge-concurrency.md).';
                        END IF;
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
                                GET DIAGNOSTICS v_changed = ROW_COUNT;
                                EXECUTE format('SELECT count(*) FROM temporal_merge_plan WHERE operation = ''UPDATE'' AND statement_seq = %s AND %s', v_update_statement_seq, v_partition_plan_filters[v_part]) INTO v_planned;
                                IF v_changed <> v_planned THEN
                                    RAISE EXCEPTION 'sql_saga: temporal_merge planned to update % rows of %, but % changed', v_planned, temporal_merge_execute.target_table, v_changed
                                        USING ERRCODE = 'internal_error',
                                              HINT = 'The target changed under the plan. A BEFORE UPDATE trigger that returns NULL, or a row-level security UPDATE policy narrower than its SELECT policy, also does this (doc/design/temporal-merge-concurrency.md).';
                                END IF;
                            ELSIF v_all_cols_ident IS NOT NULL THEN
                                v_sql := format($$ UPDATE %1$s t SET %3$I = p.new_valid_range::%4$I
                                    FROM (SELECT *, %8$s FROM temporal_merge_plan WHERE operation = 'UPDATE' AND statement_seq = %7$s AND %9$s ORDER BY plan_op_seq) p
                                    WHERE %2$s AND t.%3$I = p.old_valid_range::%4$I;
                                $$, v_target_table_ident, v_partition_join_clauses[v_part], v_range_col, v_range_constructor, NULL, NULL, v_update_statement_seq, v_partition_select_lists[v_part], v_partition_plan_filters[v_part]);
                                IF v_log_sql THEN RAISE NOTICE '(%) UPDATE partition %/% seq=% SQL: %', v_log_id, v_part, v_partition_count, v_update_statement_seq, v_sql; END IF;
                                EXECUTE v_sql;
                                GET DIAGNOSTICS v_changed = ROW_COUNT;
                                EXECUTE format('SELECT count(*) FROM temporal_merge_plan WHERE operation = ''UPDATE'' AND statement_seq = %s AND %s', v_update_statement_seq, v_partition_plan_filters[v_part]) INTO v_planned;
                                IF v_changed <> v_planned THEN
                                    RAISE EXCEPTION 'sql_saga: temporal_merge planned to update % rows of %, but % changed', v_planned, temporal_merge_execute.target_table, v_changed
                                        USING ERRCODE = 'internal_error',
                                              HINT = 'The target changed under the plan. A BEFORE UPDATE trigger that returns NULL, or a row-level security UPDATE policy narrower than its SELECT policy, also does this (doc/design/temporal-merge-concurrency.md).';
                                END IF;
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
             RAISE EXCEPTION 'Could not discover an identity key (primary or natural) for table "%". Please register a key using sql_saga.add_unique_key() or explicitly provide primary_identity_columns.', sql_saga.__internal_display_name(target_table);
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
                    RAISE EXCEPTION 'When update_source_with_identity is true, identity_column "%" must exist in the source table "%" to be updated.', v_col, sql_saga.__internal_display_name(source_table);
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
        RAISE EXCEPTION 'Could not determine identity columns for table "%". No primary or natural keys are registered, or they were explicitly disabled with an empty array.', sql_saga.__internal_display_name(temporal_merge.target_table);
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

    -- Lock the entities the source names, then plan (doc/design/temporal-merge-concurrency.md, hug 7rzgv5).
    -- At READ COMMITTED the planner's later statements take a new snapshot, so the plan sees everything committed
    -- before these locks were granted, and nothing can change these entities until this transaction ends: the
    -- result is that of running the concurrent transactions one after the other. The locks and the planner call
    -- must stay separate statements.
    DECLARE
        v_keys text[];            -- one entry per key kind: the identity, then each natural key
        v_key_cols text[];
        v_key text;
        v_lock_parts text[] := '{}';
        v_row_parts text[] := '{}';
        v_present text;
        v_hash_args text;
        -- A lock is per key, named by its columns (not its position, which depends on the source's columns), so every
        -- writer hashes the same key alike.
        v_identity_key text := to_jsonb(COALESCE(v_identity_cols_discovered, '{}'))::text;
        v_id_hash_k text := (SELECT string_agg(pg_catalog.format('k.%I::%s', c, pg_catalog.format_type(a.atttypid, a.atttypmod)), ', ' ORDER BY o)
                             FROM unnest(v_identity_cols_discovered) WITH ORDINALITY AS u(c, o)
                             JOIN pg_catalog.pg_attribute AS a ON a.attrelid = temporal_merge.target_table AND a.attname = u.c);
        v_by_identity boolean;    -- this key is not the identity: also lock the identity of each target entity it finds
        v_whole_entity boolean;   -- lock whole entities for this key (a natural key outside the identity)
        v_id_t text := (SELECT string_agg(pg_catalog.format('t.%I', c), ', ') FROM unnest(v_identity_cols_discovered) AS c);
        v_id_k text := (SELECT string_agg(pg_catalog.format('k.%I', c), ', ') FROM unnest(v_identity_cols_discovered) AS c);
        v_range_col_name name := (SELECT e.range_column_name FROM sql_saga.era AS e
                                  JOIN pg_catalog.pg_class AS c ON c.relname = e.table_name
                                  JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace AND n.nspname = e.table_schema
                                  WHERE c.oid = temporal_merge.target_table AND e.era_name = temporal_merge.era_name);
    BEGIN
        SELECT array_agg(k ORDER BY o) INTO v_keys
        FROM (
            SELECT to_jsonb(v_identity_cols_discovered)::text AS k, 0 AS o
            WHERE cardinality(v_identity_cols_discovered) > 0
            UNION ALL
            SELECT nk.value::text, nk.ordinality
            FROM jsonb_array_elements(COALESCE(v_natural_identity_keys_discovered, '[]'::jsonb)) WITH ORDINALITY AS nk
        ) AS keys
        -- A key whose columns the source or the target lacks names no entity to lock; the planner reports a missing
        -- target column.
        WHERE NOT EXISTS (
            SELECT FROM jsonb_array_elements_text(k::jsonb) AS c(name)
            WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a
                              WHERE a.attrelid = temporal_merge.source_table AND a.attname = c.name
                                AND a.attnum > 0 AND NOT a.attisdropped)
               OR NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a
                              WHERE a.attrelid = temporal_merge.target_table AND a.attname = c.name
                                AND a.attnum > 0 AND NOT a.attisdropped));

        FOREACH v_key IN ARRAY COALESCE(v_keys, '{}') LOOP
            SELECT array_agg(c ORDER BY o) INTO v_key_cols FROM jsonb_array_elements_text(v_key::jsonb) WITH ORDINALITY AS u(c, o);
            v_by_identity := v_key <> v_identity_key AND cardinality(v_identity_cols_discovered) > 0
                -- An identity column the target lacks names no entity; the planner reports it.
                AND (SELECT count(*) FROM unnest(v_identity_cols_discovered) AS c
                     JOIN pg_catalog.pg_attribute AS a ON a.attrelid = temporal_merge.target_table AND a.attname = c
                                                    AND a.attnum > 0 AND NOT a.attisdropped) = cardinality(v_identity_cols_discovered);
            v_whole_entity := v_by_identity
                AND EXISTS (SELECT FROM unnest(v_key_cols) AS c WHERE c <> ALL (v_identity_cols_discovered));
            -- Each source value is cast to the target column's type, so the lock follows the target's equality.
            SELECT string_agg(pg_catalog.format('s.%I::%s', c, pg_catalog.format_type(a.atttypid, a.atttypmod)), ', ' ORDER BY o),
                   string_agg(pg_catalog.format('s.%I IS NOT NULL', c), ' OR ' ORDER BY o)
            INTO v_hash_args, v_present
            FROM unnest(v_key_cols) WITH ORDINALITY AS u(c, o)
            JOIN pg_catalog.pg_attribute AS a ON a.attrelid = temporal_merge.target_table AND a.attname = u.c;
            -- A key names an entity when any of its columns is not NULL (a natural key may hold mutually exclusive
            -- nullable columns); a NULL column hashes as NULL, so the lock covers exactly the columns present.
            v_lock_parts := v_lock_parts || pg_catalog.format(
                'SELECT pg_catalog.hash_record_extended(ROW(%L::text, %s), 0) & 65535 AS stripe FROM %s AS s WHERE %s',
                v_key, v_hash_args, temporal_merge.source_table, v_present);
            -- A key other than the identity also locks the identity of every target entity it finds, so two writers of
            -- one entity share a lock however each names it. Without it, a writer naming entity 1 by identity and 2 by
            -- natural key, and one naming them the other way round, took disjoint locks and then crossed on the row
            -- locks: a deadlock (review of 1.15). The lookup takes no lock; a natural key never moves to another entity
            -- (hug 40nwvn), and a writer that gives an entity a natural key value locks that value too.
            IF v_by_identity THEN
                v_lock_parts := v_lock_parts || (
                    SELECT array_agg(pg_catalog.format(
                        CASE WHEN cardinality(p.present) = 1
                             THEN 'SELECT pg_catalog.hash_record_extended(ROW(%1$L::text, %2$s), 0) & 65535 AS stripe FROM %3$s AS k WHERE %4$s = ANY (ARRAY(SELECT %5$s FROM %6$s AS s WHERE %7$s))'
                             ELSE 'SELECT pg_catalog.hash_record_extended(ROW(%1$L::text, %2$s), 0) & 65535 AS stripe FROM %3$s AS k WHERE (%4$s) IN (SELECT * FROM (SELECT DISTINCT %5$s FROM %6$s AS s WHERE %7$s OFFSET 0) AS keys)'
                        END,
                        v_identity_key,
                        v_id_hash_k,
                        temporal_merge.target_table,
                        (SELECT string_agg(pg_catalog.format('k.%I', c), ', ' ORDER BY o) FROM unnest(v_key_cols) WITH ORDINALITY AS u(c, o) WHERE c = ANY (p.present)),
                        (SELECT string_agg(pg_catalog.format('s.%I::%s', c, pg_catalog.format_type(a.atttypid, a.atttypmod)), ', ' ORDER BY o)
                         FROM unnest(v_key_cols) WITH ORDINALITY AS u(c, o)
                         JOIN pg_catalog.pg_attribute AS a ON a.attrelid = temporal_merge.target_table AND a.attname = u.c
                         WHERE c = ANY (p.present)),
                        temporal_merge.source_table,
                        (SELECT string_agg(pg_catalog.format('s.%I IS %s NULL', c, CASE WHEN c = ANY (p.present) THEN 'NOT' ELSE '' END), ' AND ')
                         FROM unnest(v_key_cols) AS c)))
                    FROM (SELECT ARRAY(SELECT c FROM unnest(v_key_cols) WITH ORDINALITY AS u(c, o)
                                       WHERE (m >> (o::integer - 1)) & 1 = 1) AS present
                          FROM generate_series(1, (1 << cardinality(v_key_cols)) - 1) AS m) AS p);
            END IF;
            -- The target rows of the key, by equality on the source's non-NULL columns, typed as the target's so the key's
            -- index applies; one statement per pattern of NULL columns (a natural key with mutually exclusive columns).
            -- A single column is matched with = ANY (ARRAY(...)), which the planner always serves from the index. Several
            -- columns are matched against the source's distinct keys, materialized first (OFFSET 0): on the source's own
            -- estimates, (a, b) IN (subquery) was planned as a nested loop over the index of one column, 48 s for 28,578
            -- keys (hug t8b97s), where a join on the materialized keys takes milliseconds.
            v_row_parts := v_row_parts || (
                SELECT array_agg(pg_catalog.format(
                    CASE WHEN cardinality(p.present) = 1
                         THEN 'SELECT count(*) FROM (SELECT FROM %1$s AS t WHERE %6$s%2$s = ANY (ARRAY(SELECT %3$s FROM %4$s AS s WHERE %5$s))%7$s ORDER BY %8$s FOR UPDATE OF t) AS locked'
                         ELSE 'SELECT count(*) FROM (SELECT FROM %1$s AS t WHERE %6$s(%2$s) IN (SELECT * FROM (SELECT DISTINCT %3$s FROM %4$s AS s WHERE %5$s OFFSET 0) AS keys)%7$s ORDER BY %8$s FOR UPDATE OF t) AS locked'
                    END,
                    temporal_merge.target_table,
                    (SELECT string_agg(pg_catalog.format('%s.%I', CASE WHEN v_whole_entity THEN 'k' ELSE 't' END, c), ', ' ORDER BY o) FROM unnest(v_key_cols) WITH ORDINALITY AS u(c, o) WHERE c = ANY (p.present)),
                    (SELECT string_agg(pg_catalog.format('s.%I::%s', c, pg_catalog.format_type(a.atttypid, a.atttypmod)), ', ' ORDER BY o)
                     FROM unnest(v_key_cols) WITH ORDINALITY AS u(c, o)
                     JOIN pg_catalog.pg_attribute AS a ON a.attrelid = temporal_merge.target_table AND a.attname = u.c
                     WHERE c = ANY (p.present)),
                    temporal_merge.source_table,
                    (SELECT string_agg(pg_catalog.format('s.%I IS %s NULL', c, CASE WHEN c = ANY (p.present) THEN 'NOT' ELSE '' END), ' AND ')
                     FROM unnest(v_key_cols) AS c),
                    -- A natural key outside the identity may be NULL on some pieces of its entity (before it starts, after
                    -- it is retired): lock every row of each entity it finds, by identity, as the planner reads them (hug
                    -- 40nwvn).
                    CASE WHEN v_whole_entity AND cardinality(v_identity_cols_discovered) = 1
                              THEN pg_catalog.format('%s = ANY (ARRAY(SELECT %s FROM %s AS k WHERE ', v_id_t, v_id_k, temporal_merge.target_table)
                         WHEN v_whole_entity THEN pg_catalog.format('(%s) IN (SELECT %s FROM %s AS k WHERE ', v_id_t, v_id_k, temporal_merge.target_table)
                         ELSE '' END,
                    CASE WHEN v_whole_entity AND cardinality(v_identity_cols_discovered) = 1 THEN '))'
                         WHEN v_whole_entity THEN ')' ELSE '' END,
                    CASE WHEN v_whole_entity THEN v_id_t || ', t.' || pg_catalog.quote_ident(v_range_col_name)
                         ELSE (SELECT string_agg(pg_catalog.format('t.%I', c), ', ' ORDER BY o) FROM unnest(v_key_cols) WITH ORDINALITY AS u(c, o) WHERE c = ANY (p.present)) END))
                FROM (SELECT ARRAY(SELECT c FROM unnest(v_key_cols) WITH ORDINALITY AS u(c, o)
                                   WHERE (m >> (o::integer - 1)) & 1 = 1) AS present
                      FROM generate_series(1, (1 << cardinality(v_key_cols)) - 1) AS m) AS p);
        END LOOP;

        IF cardinality(v_lock_parts) > 0 THEN
            EXECUTE pg_catalog.format(
                'SELECT count(pg_catalog.pg_advisory_xact_lock(%s, stripe::integer)) FROM (SELECT DISTINCT stripe FROM (%s) AS k ORDER BY stripe) AS ordered',
                temporal_merge.target_table::oid::bigint::bit(32)::integer, array_to_string(v_lock_parts, ' UNION ALL '));
            -- One statement per key and pattern of present columns, so each uses its key's index. Every row locked
            -- here belongs to an entity whose identity lock is held (above), so no other merge locks these rows
            -- concurrently, and the order across statements cannot deadlock between merges.
            FOREACH v_key IN ARRAY v_row_parts LOOP
                EXECUTE v_key;
            END LOOP;
        END IF;
    END;

    -- The plan table is created here and passed implicitly to the planner and executor.
    IF to_regclass('pg_temp.temporal_merge_plan') IS NOT NULL THEN
        DROP TABLE pg_temp.temporal_merge_plan;
    END IF;
    CREATE TEMP TABLE temporal_merge_plan (LIKE sql_saga.temporal_merge_plan, PRIMARY KEY (plan_op_seq)) ON COMMIT DROP;

    -- Step 1: Generate the plan.
    DECLARE
        v_plan_start TIMESTAMPTZ := clock_timestamp();
        v_plan_ms NUMERIC;
        v_exec_start TIMESTAMPTZ;
        v_exec_ms NUMERIC;
        v_log_timing BOOLEAN;
        v_use_plpgsql BOOLEAN;
    BEGIN
        v_log_timing := COALESCE(current_setting('sql_saga.log_step_timing', true), 'off') = 'on';
        v_use_plpgsql := COALESCE(NULLIF(current_setting('sql_saga.temporal_merge.use_plpgsql_planner', true), ''), 'false')::boolean;

        IF NOT v_use_plpgsql THEN
            -- Use native Rust sweep-line planner (default, 10-13x faster with O(1) scaling)
            PERFORM sql_saga.temporal_merge_plan_native(
                target_table => temporal_merge.target_table::oid,
                source_table => temporal_merge.source_table::oid,
                mode => temporal_merge.mode::text,
                era_name => temporal_merge.era_name::text,
                identity_columns => v_identity_cols_discovered,
                row_id_column => temporal_merge.row_id_column::text,
                founding_id_column => temporal_merge.founding_id_column::text,
                delete_mode => temporal_merge.delete_mode::text,
                lookup_keys => v_natural_identity_keys_discovered,
                ephemeral_columns => temporal_merge.ephemeral_columns,
                p_log_trace => v_log_trace,
                p_log_sql => v_log_sql
            );
        ELSE
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
        END IF;

        v_plan_ms := EXTRACT(EPOCH FROM clock_timestamp() - v_plan_start) * 1000;
        IF v_log_timing THEN
            RAISE NOTICE 'temporal_merge PLAN phase: %ms (%)', round(v_plan_ms, 1), CASE WHEN v_use_plpgsql THEN 'plpgsql' ELSE 'native' END;
        END IF;
    END;

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
    DECLARE
        v_exec_start TIMESTAMPTZ := clock_timestamp();
        v_exec_ms NUMERIC;
        v_log_timing BOOLEAN;
    BEGIN
        v_log_timing := COALESCE(current_setting('sql_saga.log_step_timing', true), 'off') = 'on';
        
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
        
        v_exec_ms := EXTRACT(EPOCH FROM clock_timestamp() - v_exec_start) * 1000;
        IF v_log_timing THEN
            RAISE NOTICE 'temporal_merge EXECUTE phase: %ms', round(v_exec_ms, 1);
        END IF;
    END;
END;
$temporal_merge$;
COMMENT ON FUNCTION sql_saga.__api_add_unique_key(regclass, name[], name, sql_saga.unique_key_type, name, name, name, text, name[]) IS
'Adds a temporal unique key to a table, ensuring uniqueness across time for a given set of columns within an era. Supports primary, natural, and predicated keys. A natural key is always bound to one primary key value, also when that key is composite, and an entity holds one value of each natural key column (NULL before the key starts and after it is retired).';
REVOKE ALL ON FUNCTION sql_saga.__internal_xor_check_definition(name[]) FROM PUBLIC;
DO $copy_api_comments$
DECLARE
    w record;
BEGIN
    FOR w IN
        SELECT pub.oid::pg_catalog.regprocedure AS public_routine, pub.prokind, d.description
        FROM pg_catalog.pg_proc AS wk
        JOIN pg_catalog.pg_description AS d
          ON d.objoid = wk.oid AND d.classoid = 'pg_catalog.pg_proc'::pg_catalog.regclass AND d.objsubid = 0
        JOIN pg_catalog.pg_proc AS pub
          ON pub.pronamespace = wk.pronamespace AND pub.proname = pg_catalog.substr(wk.proname, 7)
         AND pub.proargtypes = wk.proargtypes
        WHERE wk.pronamespace = 'sql_saga'::pg_catalog.regnamespace AND wk.proname LIKE E'\\_\\_api\\_%'
    LOOP
        EXECUTE pg_catalog.format('COMMENT ON %s %s IS %L',
            CASE WHEN w.prokind = 'p' THEN 'PROCEDURE' ELSE 'FUNCTION' END, w.public_routine, w.description);
    END LOOP;
END;
$copy_api_comments$;
/*
 * The hand-written part of the current release's update step (devops/update_step.py): catalog DDL and data repairs
 * that need the new definitions. It runs after every new and changed routine is in place, and the step's integrity
 * gate runs after it; a part between the lines "-- before routines" and "-- after routines" runs before the routines
 * (a type or table a new signature uses). Name each install-script change it makes in a line "-- handles: <key>"
 * (make prints the keys). Emptied after each release.
 */

/* 1.15: delete modes apply to the MERGE_ENTITY_* modes only; the other modes refuse them (hug 2hxkwx). */
-- handles: COMMENT ON TYPE sql_saga.temporal_merge_delete_mode
COMMENT ON TYPE sql_saga.temporal_merge_delete_mode IS
'Controls deletion for the MERGE_ENTITY_* modes; the other modes refuse any value but NONE.
- NONE (default): No deletions occur.
- DELETE_MISSING_TIMELINE: For entities in the source, any part of their target timeline not covered by the source is deleted.
- DELETE_MISSING_ENTITIES: Any entity in the target that is not present in the source is completely deleted.
- DELETE_MISSING_TIMELINE_AND_ENTITIES: A combination of both timeline and entity deletion.';

/* 1.15: the CHECK of a natural key with mutually exclusive columns is recorded in check_constraint (hug bjd8v7). */
-- handles: COMMENT ON COLUMN sql_saga.unique_keys.check_constraint
COMMENT ON COLUMN sql_saga.unique_keys.check_constraint IS 'For natural keys with mutually_exclusive_columns, the name of the CHECK (num_nonnulls(<columns>) <= 1) constraint: at most one of those columns holds a value per row (all NULL is allowed, before the key starts). NULL for other keys.';
-- handles: COMMENT ON COLUMN sql_saga.unique_keys.mutually_exclusive_columns
COMMENT ON COLUMN sql_saga.unique_keys.mutually_exclusive_columns IS 'For natural keys, the subset of columns that are mutually exclusive: at most one holds a value per row (enforced by check_constraint); uniqueness uses NULLS NOT DISTINCT.';

/*
 * 1.15: a natural key with mutually exclusive columns allows at most one of them per row (hug bjd8v7). Since 1.0 the
 * CHECK that said so was not built. Every such key gets the CHECK a fresh 1.15 add_unique_key builds, recorded in
 * check_constraint; one of that name and definition is adopted. A row giving two of the columns refuses the update,
 * shown by PostgreSQL's DETAIL with the repair; no row changes.
 */
DO $add_xor_checks$
DECLARE
    k record;
    def text;
    cname name;
    pass integer;
    conflict text;
    n integer := 0;
BEGIN
    FOR k IN
        SELECT uk.unique_key_name, uk.table_schema, uk.table_name, uk.mutually_exclusive_columns,
               pg_catalog.format('%I.%I', uk.table_schema, uk.table_name)::pg_catalog.regclass AS table_oid
        FROM sql_saga.unique_keys AS uk
        WHERE uk.mutually_exclusive_columns IS NOT NULL
        ORDER BY uk.table_schema, uk.table_name, uk.unique_key_name
    LOOP
        def := sql_saga.__internal_xor_check_definition(k.mutually_exclusive_columns);
        cname := sql_saga.__internal_make_name(ARRAY[k.unique_key_name], 'xor_check');
        pass := 0;
        WHILE EXISTS (
            SELECT FROM pg_catalog.pg_constraint AS c
            WHERE c.conrelid = k.table_oid AND c.conname = cname || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END
              AND pg_catalog.pg_get_constraintdef(c.oid) <> def)
        LOOP
            pass := pass + 1;
        END LOOP;
        cname := cname || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END;
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS c WHERE (c.conrelid, c.conname) = (k.table_oid, cname)) THEN
            BEGIN
                EXECUTE pg_catalog.format('ALTER TABLE %I.%I ADD CONSTRAINT %I %s', k.table_schema, k.table_name, cname, def);
            EXCEPTION WHEN check_violation THEN
                -- ALTER TABLE ADD CONSTRAINT names no row; DETAIL lists the first five, as the preflight does.
                EXECUTE pg_catalog.format(
                    'SELECT pg_catalog.string_agg(r, '', '') FROM (SELECT pg_catalog.format(''(%%s)'', concat_ws('', '', %1$s)) AS r FROM %2$I.%3$I WHERE num_nonnulls(%1$s) > 1 ORDER BY 1 LIMIT 5) AS x',
                    (SELECT pg_catalog.string_agg(pg_catalog.quote_ident(c), ', ') FROM pg_catalog.unnest(k.mutually_exclusive_columns) AS c),
                    k.table_schema, k.table_name)
                INTO conflict;
                conflict := pg_catalog.format('Rows giving (%s): %s.', pg_catalog.array_to_string(k.mutually_exclusive_columns, ', '), conflict);
                RAISE EXCEPTION 'sql_saga: natural key % on table %.% has rows giving more than one of its mutually exclusive columns %',
                    pg_catalog.quote_ident(k.unique_key_name), pg_catalog.quote_ident(k.table_schema), pg_catalog.quote_ident(k.table_name),
                    pg_catalog.array_to_string(k.mutually_exclusive_columns, ', ')
                    USING ERRCODE = 'check_violation', DETAIL = conflict,
                          HINT = pg_catalog.format('sql_saga 1.15 enforces at most one of them per row. Find the rows with SELECT * FROM %I.%I WHERE num_nonnulls(%s) > 1; correct them so that each gives at most one, then retry.',
                              k.table_schema, k.table_name,
                              (SELECT pg_catalog.string_agg(pg_catalog.quote_ident(c), ', ') FROM pg_catalog.unnest(k.mutually_exclusive_columns) AS c));
            END;
            n := n + 1;
        END IF;
        UPDATE sql_saga.unique_keys AS uk SET check_constraint = cname WHERE uk.unique_key_name = k.unique_key_name;
    END LOOP;
    IF n > 0 THEN
        RAISE NOTICE 'sql_saga: Added the mutually exclusive column CHECK to % natural keys', n;
    END IF;
END;
$add_xor_checks$;

/*
 * 1.15: a natural key holds one value per entity (hug 40nwvn). Every natural key on a table with a primary key gets the
 * guards a fresh 1.15 add_unique_key builds: the 1.14 guard (a value belongs to one entity) and, for each natural key
 * column outside the primary key, EXCLUDE (stable primary key WITH =, column WITH <>) (an entity holds one value;
 * NULL before the key starts and after it is retired stays allowed). Rows that break it refuse the update, listed by
 * PostgreSQL's DETAIL with the repair; no row changes. A guard that already exists exactly as built is kept.
 */
DO $bind_natural_keys_both_ways$
DECLARE
    k record;
    result jsonb;
    cmds text[];
    g record;
    existing text;
    conflict text;
    n integer := 0;
BEGIN
    FOR k IN
        SELECT nk.unique_key_name, nk.table_schema, nk.table_name, nk.column_names, nk.mutually_exclusive_columns,
               pg_catalog.format('%I.%I', nk.table_schema, nk.table_name)::pg_catalog.regclass AS table_oid,
               ARRAY(SELECT c FROM pg_catalog.unnest(pk.column_names) AS c WHERE c <> e.range_column_name) AS stable_pk
        FROM sql_saga.unique_keys AS nk
        JOIN sql_saga.unique_keys AS pk ON (pk.table_schema, pk.table_name, pk.key_type) = (nk.table_schema, nk.table_name, 'primary')
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (nk.table_schema, nk.table_name, nk.era_name)
        WHERE nk.key_type = 'natural'
        ORDER BY nk.table_schema, nk.table_name, nk.unique_key_name
    LOOP
        result := sql_saga.__internal_generate_pk_consistency_constraints(
            k.unique_key_name, k.column_names, k.mutually_exclusive_columns, k.stable_pk);
        cmds := '{}';
        FOR g IN
            SELECT x.cmd, en.conname
            FROM pg_catalog.jsonb_array_elements_text(result->'cmds') WITH ORDINALITY AS x(cmd, n)
            JOIN pg_catalog.jsonb_array_elements_text(result->'names') WITH ORDINALITY AS en(conname, n) USING (n)
        LOOP
            existing := NULL;
            SELECT pg_catalog.format('ADD CONSTRAINT %I %s', c.conname, pg_catalog.pg_get_constraintdef(c.oid)) INTO existing
            FROM pg_catalog.pg_constraint AS c WHERE (c.conrelid, c.conname) = (k.table_oid, g.conname);
            IF existing IS NULL THEN
                cmds := cmds || g.cmd;
            ELSIF existing <> g.cmd THEN
                RAISE EXCEPTION 'sql_saga: constraint % on table %.% is not the guard that binds natural key % to its primary key',
                    pg_catalog.quote_ident(g.conname), pg_catalog.quote_ident(k.table_schema), pg_catalog.quote_ident(k.table_name),
                    pg_catalog.quote_ident(k.unique_key_name)
                    USING ERRCODE = 'duplicate_object', DETAIL = pg_catalog.format('Found: %s. Expected: %s.', existing, g.cmd),
                          HINT = 'The update needs that name for the guard. Rename or drop that constraint, then retry.';
            END IF;
        END LOOP;
        IF pg_catalog.cardinality(cmds) > 0 THEN
            BEGIN
                EXECUTE pg_catalog.format('ALTER TABLE %I.%I %s', k.table_schema, k.table_name, pg_catalog.array_to_string(cmds, ', '));
            EXCEPTION WHEN exclusion_violation THEN
                -- PostgreSQL's DETAIL names the two rows: one entity with two values of the natural key.
                GET STACKED DIAGNOSTICS conflict = PG_EXCEPTION_DETAIL;
                RAISE EXCEPTION 'sql_saga: natural key % on table %.% holds more than one value for an entity',
                    pg_catalog.quote_ident(k.unique_key_name), pg_catalog.quote_ident(k.table_schema), pg_catalog.quote_ident(k.table_name)
                    USING ERRCODE = 'exclusion_violation', DETAIL = conflict,
                          HINT = 'sql_saga 1.15 lets an entity hold one value of each natural key (NULL before it starts and after it is retired). An identifier that changes belongs in another natural key column. Before updating, correct the rows so that each entity holds one value, then retry.';
            END;
            n := n + 1;
        END IF;
        UPDATE sql_saga.unique_keys AS uk
        SET pk_consistency_constraint_names = ARRAY(SELECT pg_catalog.jsonb_array_elements_text(result->'names'))::name[]
        WHERE uk.unique_key_name = k.unique_key_name;
    END LOOP;
    IF n > 0 THEN
        RAISE NOTICE 'sql_saga: Bound % natural keys to one value per entity', n;
    END IF;
END;
$bind_natural_keys_both_ways$;
DO $last_step_gate$
BEGIN
    IF (SELECT pg_catalog.array_agg(x::int) FROM pg_catalog.unnest(pg_catalog.string_to_array(sql_saga.native_library_version(), '.')) AS x)
       = (SELECT pg_catalog.array_agg(x::int) FROM pg_catalog.unnest(pg_catalog.string_to_array(
              (SELECT e.extversion FROM pg_catalog.pg_extension AS e WHERE e.extname = 'sql_saga'), '.')) AS x) THEN
        PERFORM sql_saga.check_integrity();
    END IF;
END;
$last_step_gate$;
