CREATE FUNCTION sql_saga.__api_drop_unique_key_by_name(
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
                EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT IF EXISTS %I',
                    unique_key_row.table_schema, unique_key_row.table_name, unique_key_row.check_constraint);
            ELSIF unique_key_row.check_constraint IS NOT NULL
                  AND EXISTS (SELECT FROM pg_catalog.pg_constraint AS c
                             WHERE c.conrelid = table_oid AND c.conname = unique_key_row.check_constraint) THEN
                RAISE NOTICE 'sql_saga: Left constraint "%" on table "%" in place because it is not the sql_saga-managed constraint',
                    unique_key_row.check_constraint, sql_saga.__internal_display_name(table_oid);
            END IF;
            IF unique_key_row.mutually_exclusive_columns IS NOT NULL THEN
                DECLARE
                    alter_parts text[];
                    object_name name;
                BEGIN
                    -- Build a list of constraints to drop in a single ALTER TABLE command.
                    alter_parts := ARRAY[]::text[];

                    -- No partial constraints or indexes to drop for XOR keys using NULLS NOT DISTINCT.

                    -- 3. Execute the ALTER TABLE command for all constraints.
                    IF cardinality(alter_parts) > 0 THEN
                        EXECUTE format('ALTER TABLE %I.%I %s',
                            unique_key_row.table_schema,
                            unique_key_row.table_name,
                            array_to_string(alter_parts, ', ')
                        );
                    END IF;
                END;
            ELSIF unique_key_row.key_type = 'predicated' THEN
                BEGIN
                    IF unique_object_attributable THEN
                        IF unique_key_row.unique_constraint IS NOT NULL THEN
                            EXECUTE format('DROP INDEX %I.%I', unique_key_row.table_schema, unique_key_row.unique_constraint);
                        END IF;
                    ELSIF to_regclass(format('%I.%I', unique_key_row.table_schema, unique_key_row.unique_constraint)) IS NOT NULL THEN
                        RAISE NOTICE 'sql_saga: Left index "%" on table "%" in place because it is not the sql_saga-managed index',
                            unique_key_row.unique_constraint, sql_saga.__internal_display_name(table_oid);
                    END IF;
                    IF exclude_object_attributable AND unique_key_row.exclude_constraint IS NOT NULL THEN
                        EXECUTE format('ALTER TABLE %I.%I DROP CONSTRAINT %I', unique_key_row.table_schema,
                            unique_key_row.table_name, unique_key_row.exclude_constraint);
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
                        EXECUTE format('ALTER TABLE %I.%I %s',
                            unique_key_row.table_schema,
                            unique_key_row.table_name,
                            array_to_string(alter_parts, ', ')
                        );
                    END IF;
                END;
            END IF;
        END IF;
    END LOOP;

END;
$function$;

COMMENT ON FUNCTION sql_saga.__api_drop_unique_key_by_name(regclass, name, sql_saga.drop_behavior, boolean) IS
'Drops a temporal unique key identified by its unique name.';
