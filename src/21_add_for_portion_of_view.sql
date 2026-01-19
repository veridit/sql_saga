CREATE FUNCTION sql_saga.add_for_portion_of_view(table_oid regclass, era_name name DEFAULT 'valid')
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
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'table_oid must not be NULL';
    END IF;
    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    DECLARE
        target_schema_name name;
        target_table_name name;
        era_exists boolean;
    BEGIN
        SELECT n.nspname, c.relname INTO target_schema_name, target_table_name
        FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.oid = table_oid;

        -- Validate era exists (fail-fast)
        SELECT EXISTS (
            SELECT 1 FROM sql_saga.era AS p
            WHERE (p.table_schema, p.table_name) = (target_schema_name, target_table_name)
              AND p.era_name = era_name
        ) INTO era_exists;

        IF NOT era_exists THEN
            RAISE EXCEPTION 'era "%" does not exist on table %', era_name, table_oid;
        END IF;

        FOR r IN
            SELECT p.table_schema AS schema_name, p.table_name AS table_name, c.relowner AS table_owner, p.era_name, c.oid AS table_oid
            FROM sql_saga.era AS p
            JOIN pg_catalog.pg_class AS c ON c.relname = p.table_name
            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace AND n.nspname = p.table_schema
            WHERE (p.table_schema, p.table_name) = (target_schema_name, target_table_name)
              AND (era_name IS NULL OR p.era_name = era_name)
              AND p.era_name <> 'system_time'
              AND NOT EXISTS (
                    SELECT FROM sql_saga.updatable_view AS v
                    WHERE (v.table_schema, v.table_name, v.era_name) = (p.table_schema, p.table_name, p.era_name)
                      AND v.view_type = 'for_portion_of')
        LOOP
            DECLARE
                identifier_columns name[];
                identifier_columns_quoted text;
            BEGIN
                -- Prefer a single-column temporal unique key as the identifier
                SELECT uk.column_names INTO identifier_columns
                FROM sql_saga.unique_keys uk
                WHERE (uk.table_schema, uk.table_name, uk.era_name) = (r.schema_name, r.table_name, r.era_name)
                  AND array_length(uk.column_names, 1) = 1;

                IF identifier_columns IS NULL THEN
                    -- Fallback to primary key
                    SELECT array_agg(a.attname ORDER BY u.ordinality)
                    INTO identifier_columns
                    FROM pg_catalog.pg_constraint c
                    JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS u(attnum, ordinality) ON true
                    JOIN pg_catalog.pg_attribute a ON (a.attrelid, a.attnum) = (c.conrelid, u.attnum)
                    WHERE (c.conrelid, c.contype) = (r.table_oid, 'p');
                END IF;

                IF identifier_columns IS NULL THEN
                    RAISE EXCEPTION 'table "%" must have a primary key or a single-column temporal unique key for era "%" to support updatable views',
                        format('%I.%I', r.schema_name /* %I */, r.table_name /* %I */)::regclass, r.era_name;
                END IF;

                SELECT string_agg(quote_literal(c), ', ') INTO identifier_columns_quoted FROM unnest(identifier_columns) AS u(c);

                view_name := sql_saga.__internal_make_updatable_view_name(r.table_name, r.era_name, 'for_portion_of');
                trigger_name := 'for_portion_of_' || r.era_name;
                EXECUTE format('CREATE VIEW %1$I.%2$I AS TABLE %1$I.%3$I', r.schema_name /* %1$I */, view_name /* %2$I */, r.table_name /* %3$I */);
                EXECUTE format('ALTER VIEW %1$I.%2$I OWNER TO %s', r.schema_name /* %1$I */, view_name /* %2$I */, r.table_owner::regrole /* %s */);
                EXECUTE format('CREATE TRIGGER %I INSTEAD OF INSERT OR UPDATE OR DELETE ON %I.%I FOR EACH ROW EXECUTE PROCEDURE sql_saga.for_portion_of_trigger(%s)',
                    trigger_name, /* %I */
                    r.schema_name, /* %I */
                    view_name, /* %I */
                    identifier_columns_quoted /* %s */
                );
                INSERT INTO sql_saga.updatable_view (view_schema, view_name, view_type, table_schema, table_name, era_name, trigger_name)
                    VALUES (r.schema_name, view_name, 'for_portion_of', r.schema_name, r.table_name, r.era_name, trigger_name);
            END;
        END LOOP;
    END;

    RETURN true;
END;
$function$;

COMMENT ON FUNCTION sql_saga.add_for_portion_of_view(regclass, name) IS
'Creates a specialized view that emulates the SQL:2011 `FOR PORTION OF` syntax, allowing a data change to be applied to a specific time slice of a record''s history.';

