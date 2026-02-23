-- src/34_template_sync_triggers.sql
-- High-performance table/era-specific synchronization triggers.
-- Hardcoded logic eliminates JSONB and dynamic SQL overhead (8-9x faster per benchmarks).

CREATE OR REPLACE PROCEDURE sql_saga.drop_synchronize_temporal_columns_trigger(table_oid regclass, era_name name)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = sql_saga, pg_catalog, public
AS $drop_synchronize_temporal_columns_trigger$
DECLARE
    v_table_schema name;
    v_table_name name;

    v_func_name name;
    v_trg_name name;
    v_stored_func_name name;
    v_stored_trg_name name;
BEGIN
    SELECT n.nspname, c.relname
    INTO v_table_schema, v_table_name
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.oid = drop_synchronize_temporal_columns_trigger.table_oid;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table % does not exist', table_oid;
    END IF;

    -- Read stored names from sql_saga.era
    SELECT e.sync_temporal_trg_name, e.sync_temporal_trg_function_name
    INTO v_stored_trg_name, v_stored_func_name
    FROM sql_saga.era AS e
    WHERE (e.table_schema, e.table_name, e.era_name) = (v_table_schema, v_table_name, drop_synchronize_temporal_columns_trigger.era_name);

    -- Use stored names if available, otherwise compute them for backward compatibility
    IF v_stored_trg_name IS NOT NULL THEN
        v_trg_name := v_stored_trg_name;
    ELSE
        v_trg_name := sql_saga.__internal_make_name(ARRAY[v_table_name, era_name], 'sync_temporal_trg');
    END IF;

    IF v_stored_func_name IS NOT NULL THEN
        v_func_name := v_stored_func_name;
    ELSE
        v_func_name := sql_saga.__internal_make_name(ARRAY[v_table_schema, v_table_name, era_name], 'template_sync');
    END IF;

    -- Cleanup: earlier development iterations used different naming patterns.
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_trigger AS t
        WHERE t.tgrelid = drop_synchronize_temporal_columns_trigger.table_oid::oid
          AND t.tgname = sql_saga.__internal_make_name(ARRAY[v_table_name, era_name], 'template_sync')
    ) THEN
        EXECUTE format($$DROP TRIGGER %1$I ON %2$s$$,
            sql_saga.__internal_make_name(ARRAY[v_table_name, era_name], 'template_sync') /* %1$I */,
            table_oid /* %2$s */
        );
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_trigger AS t
        WHERE t.tgrelid = drop_synchronize_temporal_columns_trigger.table_oid::oid
          AND t.tgname = v_trg_name
    ) THEN
        EXECUTE format($$DROP TRIGGER %1$I ON %2$s$$,
            v_trg_name /* %1$I */,
            table_oid /* %2$s */
        );
    END IF;

    IF to_regprocedure(format('sql_saga.%I()', v_func_name)) IS NOT NULL THEN
        EXECUTE format($$DROP FUNCTION sql_saga.%1$I() CASCADE$$,
            v_func_name /* %1$I */
        );
    END IF;

    -- Clear the stored names after successful cleanup
    UPDATE sql_saga.era AS e
    SET sync_temporal_trg_name = NULL,
        sync_temporal_trg_function_name = NULL
    WHERE (e.table_schema, e.table_name, e.era_name) = (v_table_schema, v_table_name, drop_synchronize_temporal_columns_trigger.era_name);
END;
$drop_synchronize_temporal_columns_trigger$;

COMMENT ON PROCEDURE sql_saga.drop_synchronize_temporal_columns_trigger(regclass,name)
    IS 'Drops the table-specific sync trigger and its specialized trigger function.';


CREATE OR REPLACE PROCEDURE sql_saga.add_synchronize_temporal_columns_trigger(table_oid regclass, era_name name)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = sql_saga, pg_catalog, public
AS $add_synchronize_temporal_columns_trigger$
DECLARE
    v_table_schema name;
    v_table_name name;

    v_range_col name;
    v_from_col name;
    v_until_col name;
    v_to_col name;

    v_range_type regtype;
    v_range_subtype regtype;
    v_apply_defaults boolean;

    v_func_name name;
    v_trg_name name;

    v_update_of_cols name[];
    v_update_of_cols_sql text;

    v_range_col_ident text;
    v_from_col_ident text;
    v_until_col_ident text;
    v_to_col_ident text;

    v_range_col_lit text;
    v_from_col_lit text;
    v_until_col_lit text;
    v_to_col_lit text;

    v_range_type_sql text;
    v_range_subtype_sql text;

    v_to_decl text := '';
    v_to_changed_decl text := '';
    v_to_changed_detect text := '';
    v_to_extract text := '';
    v_to_collect_end text := '';
    v_to_error_end_src text := '';
    v_to_populate text := '';

    v_defaults_logic text := '';
    v_validation_error text;

    specialized_trigger_template text;
    specialized_trigger_sql text;
    specialized_trigger_create_trigger_sql text;
BEGIN
    SELECT n.nspname, c.relname
    INTO v_table_schema, v_table_name
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.oid = add_synchronize_temporal_columns_trigger.table_oid;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Table % does not exist', table_oid;
    END IF;

    SELECT
        e.range_column_name,
        e.valid_from_column_name,
        e.valid_until_column_name,
        e.valid_to_column_name,
        e.range_type,
        e.range_subtype,
        e.trigger_applies_defaults
    INTO
        v_range_col,
        v_from_col,
        v_until_col,
        v_to_col,
        v_range_type,
        v_range_subtype,
        v_apply_defaults
    FROM sql_saga.era AS e
    WHERE (e.table_schema, e.table_name, e.era_name) = (v_table_schema, v_table_name, add_synchronize_temporal_columns_trigger.era_name);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Era "%" not found on table %', era_name, table_oid;
    END IF;

    IF v_from_col IS NULL OR v_until_col IS NULL THEN
        RAISE EXCEPTION 'Template sync trigger requires valid_from_column_name and valid_until_column_name (era "%" on %)', era_name, table_oid;
    END IF;

    IF v_to_col IS NOT NULL AND v_from_col IS NULL THEN
        RAISE EXCEPTION 'valid_to_column_name requires valid_from_column_name and valid_until_column_name (era "%" on %)', era_name, table_oid;
    END IF;

    v_func_name := sql_saga.__internal_make_name(ARRAY[v_table_schema, v_table_name, era_name], 'template_sync');
    v_trg_name := sql_saga.__internal_make_name(ARRAY[v_table_name, era_name], 'sync_temporal_trg');

    v_range_col_ident := quote_ident(v_range_col);
    v_from_col_ident := quote_ident(v_from_col);
    v_until_col_ident := quote_ident(v_until_col);

    v_range_col_lit := quote_literal(v_range_col);
    v_from_col_lit := quote_literal(v_from_col);
    v_until_col_lit := quote_literal(v_until_col);

    v_range_type_sql := v_range_type::text;
    v_range_subtype_sql := v_range_subtype::text;

    v_update_of_cols := ARRAY[v_range_col];

    IF v_to_col IS NOT NULL THEN
        v_to_col_ident := quote_ident(v_to_col);
        v_to_col_lit := quote_literal(v_to_col);
        v_update_of_cols := v_update_of_cols || v_to_col;

        v_to_decl := $to_decl$
    until_via_to text;
$to_decl$;

        v_to_changed_decl := $to_changed_decl$
    is_to_changed boolean;
$to_changed_decl$;

        v_to_changed_detect := $to_changed_detect$
    is_to_changed := (TG_OP = 'INSERT' AND NEW.__TO_COL_IDENT__ IS NOT NULL)
        OR (TG_OP = 'UPDATE' AND NEW.__TO_COL_IDENT__ IS DISTINCT FROM OLD.__TO_COL_IDENT__);
$to_changed_detect$;

        v_to_extract := $to_extract$
    IF is_to_changed THEN
        until_via_to := (NEW.__TO_COL_IDENT__::__SUBTYPE_TYPE__ + 1)::text;
    END IF;
$to_extract$;

        v_to_collect_end := $to_collect_end$
    IF is_to_changed THEN
        IF final_from IS NULL THEN
            RAISE EXCEPTION 'When setting "%", the start of the period must also be provided via "%" or "%".', __TO_COL_LITERAL__, __FROM_COL_LITERAL__, __RANGE_COL_LITERAL__;
        END IF;
        until_sources := until_sources || until_via_to;
    END IF;
$to_collect_end$;

        v_to_error_end_src := $to_error_end_src$
                UNION ALL SELECT __TO_COL_LITERAL__ AS key, until_via_to AS val WHERE is_to_changed
$to_error_end_src$;

        v_to_populate := $to_populate$
    IF final_until = 'infinity' THEN
        NEW.__TO_COL_IDENT__ := 'infinity'::__SUBTYPE_TYPE__;
    ELSE
        NEW.__TO_COL_IDENT__ := (final_until::__SUBTYPE_TYPE__ - 1);
    END IF;
$to_populate$;

        v_validation_error := $validation_error$
        RAISE EXCEPTION 'The temporal period could not be determined. At least one of "%", "%" (with "%"), or the pair "%"/"%" must be provided.', __RANGE_COL_LITERAL__, __TO_COL_LITERAL__, __FROM_COL_LITERAL__, __FROM_COL_LITERAL__, __UNTIL_COL_LITERAL__;
$validation_error$;
    ELSE
        v_validation_error := $validation_error$
        RAISE EXCEPTION 'The temporal period could not be determined. At least one of "%" or the pair "%"/"%" must be provided.', __RANGE_COL_LITERAL__, __FROM_COL_LITERAL__, __UNTIL_COL_LITERAL__;
$validation_error$;
    END IF;

    v_update_of_cols := v_update_of_cols || v_from_col || v_until_col;

    IF v_apply_defaults THEN
        v_defaults_logic := $defaults$
    IF final_until IS NULL THEN
        final_until := 'infinity';
    END IF;
$defaults$;
    END IF;

    SELECT string_agg(quote_ident(c), ', ')
    INTO v_update_of_cols_sql
    FROM unnest(v_update_of_cols) AS c;

    specialized_trigger_template := $template$
CREATE OR REPLACE FUNCTION sql_saga.__FUNC_NAME__()
RETURNS trigger
LANGUAGE plpgsql AS $template_sync_trigger$
DECLARE
    final_from text;
    final_until text;

    from_via_bounds text;
    until_via_bounds text;
    from_via_range text;
    until_via_range text;

__TO_DECL__

    from_sources text[] := ARRAY[]::text[];
    until_sources text[] := ARRAY[]::text[];
    distinct_from text[];
    distinct_until text[];

    is_range_changed boolean;
    is_from_changed boolean;
    is_until_changed boolean;
    is_to_changed boolean := false;

    has_from_input boolean;
    has_until_input boolean;
BEGIN
    IF NEW.__RANGE_COL_IDENT__ = 'empty'::__RANGE_TYPE__ THEN
        RAISE EXCEPTION 'Cannot use an empty range for temporal column "%"', __RANGE_COL_LITERAL__;
    END IF;

    is_range_changed := (TG_OP = 'INSERT' AND NEW.__RANGE_COL_IDENT__ IS NOT NULL)
        OR (TG_OP = 'UPDATE' AND NEW.__RANGE_COL_IDENT__ IS DISTINCT FROM OLD.__RANGE_COL_IDENT__);

    is_from_changed := (TG_OP = 'INSERT' AND NEW.__FROM_COL_IDENT__ IS NOT NULL)
        OR (TG_OP = 'UPDATE' AND NEW.__FROM_COL_IDENT__ IS DISTINCT FROM OLD.__FROM_COL_IDENT__);

    is_until_changed := (TG_OP = 'INSERT' AND NEW.__UNTIL_COL_IDENT__ IS NOT NULL)
        OR (TG_OP = 'UPDATE' AND NEW.__UNTIL_COL_IDENT__ IS DISTINCT FROM OLD.__UNTIL_COL_IDENT__);

__TO_CHANGED_DETECT__

    has_from_input := is_range_changed OR is_from_changed;
    has_until_input := is_range_changed OR is_until_changed OR is_to_changed;

    IF is_range_changed THEN
        from_via_range := lower(NEW.__RANGE_COL_IDENT__)::text;
        until_via_range := upper(NEW.__RANGE_COL_IDENT__)::text;
    END IF;

    IF is_from_changed THEN
        from_via_bounds := NEW.__FROM_COL_IDENT__::text;
    END IF;

    IF is_until_changed THEN
        until_via_bounds := NEW.__UNTIL_COL_IDENT__::text;
    END IF;

__TO_EXTRACT__

    IF is_range_changed AND from_via_range IS NOT NULL THEN
        from_sources := from_sources || from_via_range;
    END IF;
    IF is_from_changed AND from_via_bounds IS NOT NULL THEN
        from_sources := from_sources || from_via_bounds;
    END IF;

    SELECT array_agg(DISTINCT v) INTO distinct_from FROM unnest(from_sources) AS v;

    IF cardinality(distinct_from) > 1 THEN
        RAISE EXCEPTION 'Inconsistent start of period provided. Sources: %',
            (SELECT jsonb_object_agg(key, val)
             FROM (
                SELECT __RANGE_COL_LITERAL__ AS key, from_via_range AS val WHERE is_range_changed AND from_via_range IS NOT NULL
                UNION ALL SELECT __FROM_COL_LITERAL__ AS key, from_via_bounds AS val WHERE is_from_changed AND from_via_bounds IS NOT NULL
             ) AS s);
    END IF;

    IF cardinality(distinct_from) = 1 THEN
        final_from := distinct_from[1];
    ELSIF TG_OP = 'UPDATE' AND NOT has_from_input THEN
        final_from := OLD.__FROM_COL_IDENT__::text;
    END IF;

    IF is_range_changed AND until_via_range IS NOT NULL THEN
        until_sources := until_sources || until_via_range;
    END IF;
    IF is_until_changed AND until_via_bounds IS NOT NULL THEN
        until_sources := until_sources || until_via_bounds;
    END IF;

__TO_COLLECT_END__

    SELECT array_agg(DISTINCT v) INTO distinct_until FROM unnest(until_sources) AS v;

    IF cardinality(distinct_until) > 1 THEN
        RAISE EXCEPTION 'Inconsistent end of period provided. Sources: %',
            (SELECT jsonb_object_agg(key, val)
             FROM (
                SELECT __RANGE_COL_LITERAL__ AS key, until_via_range AS val WHERE is_range_changed AND until_via_range IS NOT NULL
                UNION ALL SELECT __UNTIL_COL_LITERAL__ AS key, until_via_bounds AS val WHERE is_until_changed AND until_via_bounds IS NOT NULL
__TO_ERROR_END_SRC__
             ) AS s);
    END IF;

    IF cardinality(distinct_until) = 1 THEN
        final_until := distinct_until[1];
    ELSIF TG_OP = 'UPDATE' AND NOT has_until_input THEN
        final_until := OLD.__UNTIL_COL_IDENT__::text;
    END IF;

__DEFAULTS__

    IF final_from IS NULL OR final_until IS NULL THEN
__VALIDATION_ERROR__
    END IF;

    NEW.__FROM_COL_IDENT__ := final_from::__SUBTYPE_TYPE__;
    NEW.__UNTIL_COL_IDENT__ := final_until::__SUBTYPE_TYPE__;

__TO_POPULATE__

    BEGIN
        NEW.__RANGE_COL_IDENT__ := __RANGE_TYPE__(final_from::__SUBTYPE_TYPE__, final_until::__SUBTYPE_TYPE__, '[)');
    EXCEPTION WHEN data_exception THEN
        -- Let the table's CHECK constraint handle invalid bounds (e.g., from >= until).
    END;

    RETURN NEW;
END;
$template_sync_trigger$;
$template$;

    specialized_trigger_sql := specialized_trigger_template;

    -- First insert all optional blocks, then do identifier/type replacement.
    specialized_trigger_sql := replace(specialized_trigger_sql, '__TO_DECL__', v_to_decl);
    specialized_trigger_sql := replace(specialized_trigger_sql, '__TO_CHANGED_DETECT__', v_to_changed_detect);
    specialized_trigger_sql := replace(specialized_trigger_sql, '__TO_EXTRACT__', v_to_extract);
    specialized_trigger_sql := replace(specialized_trigger_sql, '__TO_COLLECT_END__', v_to_collect_end);
    specialized_trigger_sql := replace(specialized_trigger_sql, '__TO_ERROR_END_SRC__', v_to_error_end_src);
    specialized_trigger_sql := replace(specialized_trigger_sql, '__TO_POPULATE__', v_to_populate);

    specialized_trigger_sql := replace(specialized_trigger_sql, '__DEFAULTS__', v_defaults_logic);
    specialized_trigger_sql := replace(specialized_trigger_sql, '__VALIDATION_ERROR__', v_validation_error);

    IF v_to_col IS NOT NULL THEN
        specialized_trigger_sql := replace(specialized_trigger_sql, '__TO_COL_IDENT__', v_to_col_ident);
        specialized_trigger_sql := replace(specialized_trigger_sql, '__TO_COL_LITERAL__', v_to_col_lit);
    ELSE
        specialized_trigger_sql := replace(specialized_trigger_sql, '__TO_COL_IDENT__', 'NULL');
        specialized_trigger_sql := replace(specialized_trigger_sql, '__TO_COL_LITERAL__', 'NULL');
    END IF;

    specialized_trigger_sql := replace(specialized_trigger_sql, '__FUNC_NAME__', quote_ident(v_func_name));

    specialized_trigger_sql := replace(specialized_trigger_sql, '__RANGE_COL_IDENT__', v_range_col_ident);
    specialized_trigger_sql := replace(specialized_trigger_sql, '__FROM_COL_IDENT__', v_from_col_ident);
    specialized_trigger_sql := replace(specialized_trigger_sql, '__UNTIL_COL_IDENT__', v_until_col_ident);

    specialized_trigger_sql := replace(specialized_trigger_sql, '__RANGE_COL_LITERAL__', v_range_col_lit);
    specialized_trigger_sql := replace(specialized_trigger_sql, '__FROM_COL_LITERAL__', v_from_col_lit);
    specialized_trigger_sql := replace(specialized_trigger_sql, '__UNTIL_COL_LITERAL__', v_until_col_lit);

    specialized_trigger_sql := replace(specialized_trigger_sql, '__RANGE_TYPE__', v_range_type_sql);
    specialized_trigger_sql := replace(specialized_trigger_sql, '__SUBTYPE_TYPE__', v_range_subtype_sql);

    specialized_trigger_create_trigger_sql := format($$CREATE TRIGGER %1$I BEFORE INSERT OR UPDATE OF %2$s ON %3$s FOR EACH ROW EXECUTE FUNCTION sql_saga.%4$I()$$,
        v_trg_name /* %1$I */,
        v_update_of_cols_sql /* %2$s */,
        table_oid /* %3$s */,
        v_func_name /* %4$I */
    );

    RAISE DEBUG 'specialized_trigger_template: %', specialized_trigger_template;
    RAISE DEBUG 'specialized_trigger_sql: %', specialized_trigger_sql;
    RAISE DEBUG 'specialized_trigger_create_trigger_sql: %', specialized_trigger_create_trigger_sql;

    CALL sql_saga.drop_synchronize_temporal_columns_trigger(table_oid, era_name);

    EXECUTE specialized_trigger_sql;
    EXECUTE specialized_trigger_create_trigger_sql;

    -- Update sql_saga.era with the trigger and function names
    UPDATE sql_saga.era AS e
    SET sync_temporal_trg_name = v_trg_name,
        sync_temporal_trg_function_name = v_func_name
    WHERE (e.table_schema, e.table_name, e.era_name) = (v_table_schema, v_table_name, add_synchronize_temporal_columns_trigger.era_name);
END;
$add_synchronize_temporal_columns_trigger$;

COMMENT ON PROCEDURE sql_saga.add_synchronize_temporal_columns_trigger(regclass,name)
    IS 'Generator for high-performance table-specific temporal column sync triggers.';
