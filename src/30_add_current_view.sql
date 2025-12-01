CREATE FUNCTION sql_saga.add_current_view(table_oid regclass, era_name name DEFAULT 'valid', delete_mode name DEFAULT 'delete_as_cutoff', current_func_name text DEFAULT NULL)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    r record;
    view_name name;
    trigger_name name;
    target_schema_name name;
    target_table_name name;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'table_oid must be specified';
    END IF;

    IF delete_mode NOT IN ('delete_as_cutoff', 'delete_as_documented_ending') THEN
        RAISE EXCEPTION 'delete_mode must be one of ''delete_as_cutoff'' or ''delete_as_documented_ending''';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    SELECT n.nspname, c.relname INTO target_schema_name, target_table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = table_oid;

    FOR r IN
        SELECT p.table_schema AS schema_name, p.table_name AS table_name, c.relowner AS table_owner, p.era_name, c.oid AS table_oid, p.range_type, p.range_subtype, p.range_subtype_category, p.range_column_name
        FROM sql_saga.era AS p
        JOIN pg_catalog.pg_class AS c ON c.relname = p.table_name
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace AND n.nspname = p.table_schema
        WHERE (p.table_schema, p.table_name) = (target_schema_name, target_table_name)
          AND p.era_name = era_name
          AND p.era_name <> 'system_time'
          AND NOT EXISTS (
                SELECT FROM sql_saga.updatable_view AS v
                WHERE (v.table_schema, v.table_name, v.era_name) = (p.table_schema, p.table_name, p.era_name)
                  AND v.view_type = 'current')
    LOOP
        DECLARE
            identifier_columns name[];
            identifier_columns_quoted text;
            non_temporal_columns_quoted text;
            range_category char;
            now_function text;
            trigger_args text;
        BEGIN
            -- Verify the era is time-based by checking the cached category
            IF r.range_subtype_category <> 'D' THEN -- 'D' for all DateTime types
                 RAISE EXCEPTION 'Cannot create a "current" view for era "%" on table "%" because it is a range of %, and only date or timestamp is supported.',
                    r.era_name, table_oid, r.range_subtype::regtype;
            END IF;

            -- Determine now() function based on range subtype, but allow override for testing
            IF current_func_name IS NULL THEN
                SELECT CASE t.typname
                    WHEN 'date' THEN 'CURRENT_DATE'
                    ELSE 'now()'
                END INTO now_function
                FROM pg_type t
                WHERE t.oid = r.range_subtype;
            ELSE
                now_function := current_func_name;
            END IF;

            -- Validate the function name to prevent SQL injection.
            -- We allow CURRENT_DATE and now() as special keywords, otherwise it must be a valid function.
            IF lower(now_function) NOT IN ('current_date', 'now()') THEN
                BEGIN
                    PERFORM now_function::regprocedure;
                EXCEPTION
                    WHEN OTHERS THEN
                        RAISE EXCEPTION 'current_func_name must be "CURRENT_DATE", "now()", or a valid function signature, but is "%"', now_function;
                END;
            END IF;

            -- Find identifier columns (PK or single-column unique key)
            SELECT uk.column_names INTO identifier_columns
            FROM sql_saga.unique_keys uk
            WHERE (uk.table_schema, uk.table_name, uk.era_name) = (r.schema_name, r.table_name, r.era_name)
              AND array_length(uk.column_names, 1) = 1;

            IF identifier_columns IS NULL THEN
                SELECT array_agg(a.attname ORDER BY u.ordinality)
                INTO identifier_columns
                FROM pg_catalog.pg_constraint c
                JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS u(attnum, ordinality) ON true
                JOIN pg_catalog.pg_attribute a ON (a.attrelid, a.attnum) = (c.conrelid, u.attnum)
                WHERE (c.conrelid, c.contype) = (r.table_oid, 'p');
            END IF;

            IF identifier_columns IS NULL THEN
                RAISE EXCEPTION 'table "%" must have a primary key or a single-column temporal unique key for era "%" to support updatable views',
                    table_oid, r.era_name;
            END IF;

            view_name := sql_saga.__internal_make_updatable_view_name(r.table_name, r.era_name, 'current');
            trigger_name := 'current_' || r.era_name;

            -- The view includes all columns from the base table, but is filtered to only show the current records.
            -- This makes its schema consistent with the for_portion_of view, enabling parameter passing via SET.
            EXECUTE format('CREATE VIEW %1$I.%2$I WITH (security_barrier=true) AS SELECT * FROM %1$I.%3$I WHERE %4$I @> %5$s::%6$s',
                r.schema_name, view_name, r.table_name, r.range_column_name, now_function, r.range_subtype);
            EXECUTE format('ALTER VIEW %1$I.%2$I OWNER TO %s', r.schema_name, view_name, r.table_owner::regrole);

            -- Pass identifier columns, delete mode, and comment column to trigger
            SELECT string_agg(quote_literal(c), ', ')
            INTO identifier_columns_quoted
            FROM unnest(identifier_columns) AS u(c);

            trigger_args := concat_ws(', ',
                quote_literal(delete_mode),
                identifier_columns_quoted
            );

            EXECUTE format('CREATE TRIGGER %I INSTEAD OF INSERT OR UPDATE OR DELETE ON %I.%I FOR EACH ROW EXECUTE PROCEDURE sql_saga.current_view_trigger(%s)',
                trigger_name, r.schema_name, view_name, trigger_args);
            INSERT INTO sql_saga.updatable_view (view_schema, view_name, view_type, table_schema, table_name, era_name, trigger_name, current_func)
                VALUES (r.schema_name, view_name, 'current', r.schema_name, r.table_name, r.era_name, trigger_name, now_function);
        END;
    END LOOP;

    RETURN true;
END;
$function$;

COMMENT ON FUNCTION sql_saga.add_current_view(regclass, name, name, text) IS
'Creates a view that shows only the current state of data, making it ideal for ORMs and REST APIs. It provides a trigger for safe, explicit SCD Type 2 updates and soft-deletes.';

