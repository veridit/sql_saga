CREATE FUNCTION sql_saga.drop_foreign_key_by_name(
    table_oid regclass,
    key_name name)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    foreign_key_row sql_saga.foreign_keys;
    unique_table_oid regclass;
    fk_table_oid regclass;
    fk_schema_name name;
    fk_table_name name;
BEGIN
    IF table_oid IS NULL AND key_name IS NULL THEN
        RAISE EXCEPTION 'no table or key name specified';
    END IF;

    IF table_oid IS NOT NULL THEN
        SELECT n.nspname, c.relname
        INTO fk_schema_name, fk_table_name
        FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.oid = table_oid;
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    FOR foreign_key_row IN
        SELECT fk.*
        FROM sql_saga.foreign_keys AS fk
        WHERE (table_oid IS NULL OR (fk.table_schema, fk.table_name) = (fk_schema_name, fk_table_name))
          AND (fk.foreign_key_name = key_name OR key_name IS NULL)
    LOOP
        -- Delete the metadata record first, so the drop_protection trigger doesn't fire.
        DELETE FROM sql_saga.foreign_keys AS fk
        WHERE fk.foreign_key_name = foreign_key_row.foreign_key_name;

        /*
         * Now that the metadata is gone, we can safely drop the database objects.
         */
        fk_table_oid := format('%I.%I', foreign_key_row.table_schema, foreign_key_row.table_name)::regclass;

        CASE foreign_key_row.type
            WHEN 'temporal_to_temporal' THEN
                IF EXISTS (SELECT FROM pg_trigger WHERE tgrelid = fk_table_oid AND tgname = foreign_key_row.fk_insert_trigger) THEN
                    EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.fk_insert_trigger /* %I */, fk_table_oid /* %s */);
                END IF;
                IF EXISTS (SELECT FROM pg_trigger WHERE tgrelid = fk_table_oid AND tgname = foreign_key_row.fk_update_trigger) THEN
                    EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.fk_update_trigger /* %I */, fk_table_oid /* %s */);
                END IF;
            WHEN 'regular_to_temporal' THEN
                IF EXISTS (SELECT FROM pg_constraint WHERE conrelid = fk_table_oid AND conname = foreign_key_row.fk_check_constraint) THEN
                    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', fk_table_oid /* %s */, foreign_key_row.fk_check_constraint /* %I */);
                END IF;
                -- If no other foreign keys use the helper function, drop it.
                IF NOT EXISTS (
                    SELECT 1 FROM sql_saga.foreign_keys
                    WHERE fk_helper_function = foreign_key_row.fk_helper_function
                ) THEN
                    EXECUTE format('DROP FUNCTION %s', foreign_key_row.fk_helper_function /* %s */);
                END IF;
        END CASE;

        SELECT to_regclass(format('%I.%I', uk.table_schema /* %I */, uk.table_name /* %I */))
        INTO unique_table_oid
        FROM sql_saga.unique_keys AS uk
        WHERE uk.unique_key_name = foreign_key_row.unique_key_name;

        IF FOUND AND pg_catalog.to_regclass(unique_table_oid::text) IS NOT NULL THEN
            IF EXISTS (SELECT FROM pg_trigger WHERE tgrelid = unique_table_oid AND tgname = foreign_key_row.uk_update_trigger) THEN
                EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.uk_update_trigger /* %I */, unique_table_oid /* %s */);
            END IF;
            IF EXISTS (SELECT FROM pg_trigger WHERE tgrelid = unique_table_oid AND tgname = foreign_key_row.uk_delete_trigger) THEN
                EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.uk_delete_trigger /* %I */, unique_table_oid /* %s */);
            END IF;
        END IF;
    END LOOP;

    RETURN true;
END;
$function$;

