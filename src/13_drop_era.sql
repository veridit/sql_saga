CREATE FUNCTION sql_saga.drop_era(table_oid regclass, era_name name DEFAULT 'valid', drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT false)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    era_row sql_saga.era;
    portion_view regclass;
    is_dropped boolean;
    table_schema name;
    table_name name;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    SELECT n.nspname, c.relname
    INTO table_schema, table_name
    FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = table_oid;

    IF era_name IS NULL THEN
        RAISE EXCEPTION 'no era name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    /*
     * Has the table been dropped already?  This could happen if the period is
     * being dropped by the drop_protection event trigger or through a DROP
     * CASCADE.
     */
    is_dropped := NOT EXISTS (SELECT FROM pg_catalog.pg_class AS c WHERE c.oid = table_oid);

    SELECT p.*
    INTO era_row
    FROM sql_saga.era AS p
    WHERE (p.table_schema, p.table_name, p.era_name) = (table_schema, table_name, era_name);

    IF NOT FOUND THEN
        RAISE DEBUG 'era % not found for table %', era_name, table_oid;
        RETURN false;
    END IF;

    /* Drop the "for portion" view if it hasn't been dropped already */
    PERFORM sql_saga.drop_api(table_oid, era_name, drop_behavior, cleanup);

    /* If this is a system_time era, get rid of the triggers */
    DECLARE
        system_time_era_row sql_saga.system_time_era;
    BEGIN
        DELETE FROM sql_saga.system_time_era AS ste
        WHERE (ste.table_schema, ste.table_name, ste.era_name) = (table_schema, table_name, era_name)
        RETURNING * INTO system_time_era_row;

        IF FOUND AND NOT is_dropped THEN
            EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', table_oid, system_time_era_row.infinity_check_constraint);
            EXECUTE format('DROP TRIGGER %I ON %s', system_time_era_row.generated_always_trigger, table_oid);
            EXECUTE format('DROP TRIGGER %I ON %s', system_time_era_row.write_history_trigger, table_oid);
            EXECUTE format('DROP TRIGGER %I ON %s', system_time_era_row.truncate_trigger, table_oid);
        END IF;
    END;

    IF drop_behavior = 'RESTRICT' THEN
        /* Check for UNIQUE or PRIMARY KEYs */
        IF EXISTS (
            SELECT FROM sql_saga.unique_keys AS uk
            WHERE (uk.table_schema, uk.table_name, uk.era_name) = (table_schema, table_name, era_name))
        THEN
            RAISE EXCEPTION 'era % is part of a UNIQUE or PRIMARY KEY', era_name;
        END IF;

        /* Check for FOREIGN KEYs */
        IF EXISTS (
            SELECT FROM sql_saga.foreign_keys AS fk
            WHERE (fk.table_schema, fk.table_name, fk.era_name) = (table_schema, table_name, era_name))
        THEN
            RAISE EXCEPTION 'era % is part of a FOREIGN KEY', era_name;
        END IF;

--        /* Check for SYSTEM VERSIONING */
--        IF EXISTS (
--            SELECT FROM sql_saga.system_versioning AS sv
--            WHERE (sv.table_oid, sv.era_name) = (table_oid, era_name))
--        THEN
--            RAISE EXCEPTION 'table % has SYSTEM VERSIONING', table_oid;
--        END IF;

        /* Remove from catalog */
        DELETE FROM sql_saga.era AS p
        WHERE (p.table_schema, p.table_name, p.era_name) = (table_schema, table_name, era_name);

        /* Delete bounds check constraint and columns if purging */
        IF NOT is_dropped AND cleanup THEN
            EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I, DROP COLUMN %I, DROP COLUMN %I',
                table_oid, era_row.bounds_check_constraint, era_row.valid_from_column_name, era_row.valid_until_column_name);
        END IF;

        RETURN true;
    END IF;

    /* We must be in CASCADE mode now */

    PERFORM sql_saga.drop_foreign_key_by_name(table_oid, fk.foreign_key_name)
    FROM sql_saga.foreign_keys AS fk
    WHERE (fk.table_schema, fk.table_name, fk.era_name) = (table_schema, table_name, era_name);

    PERFORM sql_saga.drop_unique_key_by_name(table_oid, uk.unique_key_name, drop_behavior, cleanup)
    FROM sql_saga.unique_keys AS uk
    WHERE (uk.table_schema, uk.table_name, uk.era_name) = (table_schema, table_name, era_name);

    /* Remove from catalog */
    DELETE FROM sql_saga.era AS p
    WHERE (p.table_schema, p.table_name, p.era_name) = (table_schema, table_name, era_name);

    /* Delete bounds check constraint and columns if purging */
    IF NOT is_dropped AND cleanup THEN
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I, DROP COLUMN %I, DROP COLUMN %I',
            table_oid, era_row.bounds_check_constraint, era_row.valid_from_column_name, era_row.valid_until_column_name);
    END IF;

    RETURN true;
END;
$function$;
