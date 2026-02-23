CREATE FUNCTION sql_saga.drop_unique_key(
        table_oid regclass,
        column_names name[],
        era_name name DEFAULT 'valid',
        drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT',
        cleanup boolean DEFAULT true
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = sql_saga, pg_catalog, public
AS
$function$
#variable_conflict use_variable
DECLARE
    table_schema name;
    table_name name;
    key_row sql_saga.unique_keys;
    constraint_name name;
    alter_sql text;
BEGIN
    SELECT n.nspname, c.relname
    INTO table_schema, table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = table_oid;

    SELECT uk.* INTO key_row
    FROM sql_saga.unique_keys AS uk
    WHERE (uk.table_schema, uk.table_name) = (table_schema, table_name)
      AND uk.column_names = column_names
      AND uk.era_name = era_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'unique key on table % for columns % with era % does not exist', table_oid, column_names, era_name;
    END IF;

    -- If dropping a primary key, we must first drop any consistency constraints
    -- on other natural keys that depend on it, and reset their state to "pending".
    IF key_row.key_type = 'primary' THEN
        DECLARE
            constraints_to_drop record;
            cleanup_cmds text[] := '{}';
        BEGIN
            -- First, collect all constraints to be dropped and update metadata in one go.
            FOR constraints_to_drop IN
                SELECT unique_key_name, pk_consistency_constraint_names
                FROM sql_saga.unique_keys uk
                WHERE (uk.table_schema, uk.table_name) = (table_schema, table_name)
                  AND uk.key_type = 'natural'
                  AND uk.pk_consistency_constraint_names IS NOT NULL AND uk.pk_consistency_constraint_names <> '{}'
            LOOP
                FOREACH constraint_name IN ARRAY constraints_to_drop.pk_consistency_constraint_names
                LOOP
                    cleanup_cmds := array_append(cleanup_cmds, format('DROP CONSTRAINT %I', constraint_name));
                END LOOP;

                -- Reset the natural key's state to "pending" (empty array).
                UPDATE sql_saga.unique_keys SET pk_consistency_constraint_names = '{}'
                WHERE unique_key_name = constraints_to_drop.unique_key_name;
            END LOOP;

            IF array_length(cleanup_cmds, 1) > 0 THEN
                alter_sql := format('ALTER TABLE %I.%I %s', table_schema, table_name, array_to_string(cleanup_cmds, ', '));
                RAISE NOTICE 'sql_saga: dropping dependent pk consistency constraints: %', alter_sql;
                EXECUTE alter_sql;
            END IF;
        END;
    END IF;

    -- Drop any associated consistency constraints on this key itself.
    IF key_row.pk_consistency_constraint_names IS NOT NULL AND key_row.pk_consistency_constraint_names <> '{}' THEN
        DECLARE
            original_constraints name[] := key_row.pk_consistency_constraint_names;
        BEGIN
            -- Temporarily clear the metadata to bypass the drop_protection trigger check.
            -- If the DROP fails, the transaction will roll back, reverting this change.
            UPDATE sql_saga.unique_keys SET pk_consistency_constraint_names = NULL WHERE unique_key_name = key_row.unique_key_name;

            FOREACH constraint_name IN ARRAY original_constraints
            LOOP
                alter_sql := format('ALTER TABLE %I.%I DROP CONSTRAINT %I', table_schema, table_name, constraint_name);
                RAISE NOTICE 'sql_saga: dropping constraint: %', alter_sql;
                EXECUTE alter_sql;
            END LOOP;
        END;
    END IF;

    RAISE DEBUG 'drop_unique_key: Found key name "%" to drop. Forwarding to drop_unique_key_by_name.', key_row.unique_key_name;
    PERFORM sql_saga.drop_unique_key_by_name(table_oid, key_row.unique_key_name, drop_behavior, cleanup);
END;
$function$;

COMMENT ON FUNCTION sql_saga.drop_unique_key(regclass, name[], name, sql_saga.drop_behavior, boolean) IS
'Drops a temporal unique key identified by its table, columns, and era.';
