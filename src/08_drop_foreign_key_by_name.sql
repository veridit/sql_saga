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
        DELETE FROM sql_saga.foreign_keys AS fk
        WHERE fk.foreign_key_name = foreign_key_row.foreign_key_name;

        /*
         * Make sure the table hasn't been dropped and that the triggers exist
         * before doing these.  We could use the IF EXISTS clause but we don't
         * in order to avoid the NOTICE.
         */
        fk_table_oid := format('%I.%I', foreign_key_row.table_schema, foreign_key_row.table_name)::regclass;
        IF EXISTS (
                SELECT FROM pg_catalog.pg_class AS c
                WHERE c.oid = fk_table_oid)
            AND EXISTS (
                SELECT FROM pg_catalog.pg_trigger AS t
                WHERE t.tgrelid = fk_table_oid
                  AND t.tgname IN (foreign_key_row.fk_insert_trigger, foreign_key_row.fk_update_trigger))
        THEN
            EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.fk_insert_trigger, fk_table_oid);
            EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.fk_update_trigger, fk_table_oid);
        END IF;

        SELECT to_regclass(format('%I.%I', uk.table_schema, uk.table_name))
        INTO unique_table_oid
        FROM sql_saga.unique_keys AS uk
        WHERE uk.unique_key_name = foreign_key_row.unique_key_name;

        /* Ditto for the UNIQUE side. */
        IF FOUND
            AND EXISTS (
                SELECT FROM pg_catalog.pg_class AS c
                WHERE c.oid = unique_table_oid)
            AND EXISTS (
                SELECT FROM pg_catalog.pg_trigger AS t
                WHERE t.tgrelid = unique_table_oid
                  AND t.tgname IN (foreign_key_row.uk_update_trigger, foreign_key_row.uk_delete_trigger))
        THEN
            EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.uk_update_trigger, unique_table_oid);
            EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.uk_delete_trigger, unique_table_oid);
        END IF;
    END LOOP;

    RETURN true;
END;
$function$;
