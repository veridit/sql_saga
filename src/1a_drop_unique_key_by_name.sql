CREATE FUNCTION sql_saga.drop_unique_key_by_name(
    table_oid regclass,
    key_name name,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT',
    cleanup boolean DEFAULT true
 )
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    table_schema name;
    table_name name;
    foreign_key_row sql_saga.foreign_keys;
    unique_key_row sql_saga.unique_keys;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

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
        /* Cascade to foreign keys, if desired */
        FOR foreign_key_row IN
            SELECT fk.*
            FROM sql_saga.foreign_keys AS fk
            WHERE fk.unique_key_name = unique_key_row.unique_key_name
        LOOP
            IF drop_behavior = 'RESTRICT' THEN
                RAISE EXCEPTION 'cannot drop unique key "%" because foreign key "%" on table "%" depends on it',
                    unique_key_row.unique_key_name, foreign_key_row.foreign_key_name, format('%I.%I', foreign_key_row.table_schema, foreign_key_row.table_name)::regclass;
            END IF;

            PERFORM sql_saga.drop_foreign_key_by_name(NULL, foreign_key_row.foreign_key_name);
        END LOOP;

        DELETE FROM sql_saga.unique_keys AS uk
        WHERE uk.unique_key_name = unique_key_row.unique_key_name;

        /* If purging, drop the underlying constraints unless the table has been dropped */
        IF cleanup AND EXISTS (
            SELECT FROM pg_catalog.pg_class AS c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE (n.nspname, c.relname) = (unique_key_row.table_schema, unique_key_row.table_name))
        THEN
            IF unique_key_row.mutually_exclusive_columns IS NOT NULL THEN
                DECLARE
                    alter_parts text[];
                    object_name name;
                BEGIN
                    -- Build a list of constraints to drop in a single ALTER TABLE command.
                    alter_parts := ARRAY[]::text[];

                    -- 1. Add the CHECK constraint to the drop list.
                    IF unique_key_row.check_constraint IS NOT NULL THEN
                        alter_parts := alter_parts || format('DROP CONSTRAINT %I', unique_key_row.check_constraint);
                    END IF;

                    -- 2. No partial constraints or indexes to drop for XOR keys using NULLS NOT DISTINCT

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
                IF unique_key_row.check_constraint IS NOT NULL THEN
                    -- This is a unique index, drop it and the exclusion constraint
                    EXECUTE format('DROP INDEX %I.%I; ALTER TABLE %I.%I DROP CONSTRAINT %I;',
                        unique_key_row.table_schema, /* %I */
                        unique_key_row.unique_constraint, /* %I */
                        unique_key_row.table_schema, /* %I */
                        unique_key_row.table_name, /* %I */
                        unique_key_row.exclude_constraint /* %I */
                    );
                END IF;
            ELSE -- 'primary' or 'natural'
                DECLARE
                    alter_parts text[] := '{}';
                BEGIN
                    -- This is a unique/primary key constraint, drop both constraints if they exist.
                    IF unique_key_row.unique_constraint IS NOT NULL THEN
                        alter_parts := alter_parts || format('DROP CONSTRAINT %I', unique_key_row.unique_constraint);
                    END IF;
                    IF unique_key_row.exclude_constraint IS NOT NULL THEN
                        alter_parts := alter_parts || format('DROP CONSTRAINT %I', unique_key_row.exclude_constraint);
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

COMMENT ON FUNCTION sql_saga.drop_unique_key_by_name(regclass, name, sql_saga.drop_behavior, boolean) IS
'Drops a temporal unique key identified by its unique name.';
