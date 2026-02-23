CREATE FUNCTION sql_saga.drop_era(table_oid regclass, era_name name DEFAULT 'valid', drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT false)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, public
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

    /* If this is a system_time era, get rid of the triggers */
    DECLARE
        system_time_era_row sql_saga.system_time_era;
    BEGIN
        DELETE FROM sql_saga.system_time_era AS ste
        WHERE (ste.table_schema, ste.table_name, ste.era_name) = (table_schema, table_name, era_name)
        RETURNING * INTO system_time_era_row;

        IF FOUND AND NOT is_dropped THEN
            EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', table_oid /* %s */, system_time_era_row.infinity_check_constraint /* %I */);
            EXECUTE format('DROP TRIGGER %I ON %s', system_time_era_row.generated_always_trigger /* %I */, table_oid /* %s */);
            EXECUTE format('DROP TRIGGER %I ON %s', system_time_era_row.write_history_trigger /* %I */, table_oid /* %s */);
            EXECUTE format('DROP TRIGGER %I ON %s', system_time_era_row.truncate_trigger /* %I */, table_oid /* %s */);
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
            WHERE (fk.table_schema, fk.table_name, fk.fk_era_name) = (table_schema, table_name, era_name))
        THEN
            RAISE EXCEPTION 'era % is part of a FOREIGN KEY', era_name;
        END IF;

        /* Check for updatable views */
        IF EXISTS (
            SELECT FROM sql_saga.updatable_view AS v
            WHERE (v.table_schema, v.table_name, v.era_name) = (table_schema, table_name, era_name))
        THEN
            RAISE EXCEPTION 'era % is used by an updatable view', era_name;
        END IF;

        /* Check for SYSTEM VERSIONING - must use drop_system_versioning() instead */
        IF EXISTS (
            SELECT FROM sql_saga.system_versioning AS sv
            WHERE (sv.table_schema, sv.table_name, sv.era_name) = (table_schema, table_name, era_name))
        THEN
            RAISE EXCEPTION 'era % has SYSTEM VERSIONING, use drop_system_versioning() instead', era_name;
        END IF;

        /* Drop synchronization trigger BEFORE removing from catalog */
        IF NOT is_dropped AND (era_row.valid_from_column_name IS NOT NULL OR era_row.valid_to_column_name IS NOT NULL) THEN
            CALL sql_saga.drop_synchronize_temporal_columns_trigger(table_oid, era_name);
        END IF;

        /* Remove from catalog */
        DELETE FROM sql_saga.era AS p
        WHERE (p.table_schema, p.table_name, p.era_name) = (table_schema, table_name, era_name);

        /* Delete bounds check constraint (NOT isempty(range)). */
        IF NOT is_dropped AND era_row.bounds_check_constraint IS NOT NULL THEN
            EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', table_oid /* %s */, era_row.bounds_check_constraint /* %I */);
        END IF;

        /* Delete boundary check constraint (from < until). */
        IF NOT is_dropped AND era_row.boundary_check_constraint IS NOT NULL THEN
            EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', table_oid /* %s */, era_row.boundary_check_constraint /* %I */);
        END IF;

        /* Drop column default if it was set */
        IF NOT is_dropped AND NOT era_row.trigger_applies_defaults AND era_row.valid_until_column_name IS NOT NULL THEN
            EXECUTE format('ALTER TABLE %s ALTER COLUMN %I DROP DEFAULT', table_oid, era_row.valid_until_column_name);
        END IF;

        /* Delete columns if purging */
        IF NOT is_dropped AND cleanup THEN
            DECLARE
                drop_clauses text[] := '{}';
            BEGIN
                -- The range column is always present for an era.
                drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.range_column_name);

                -- Drop from/until if they were part of the era.
                IF era_row.valid_from_column_name IS NOT NULL THEN
                    drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.valid_from_column_name);
                    drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.valid_until_column_name);
                END IF;

                -- Drop to if it was part of the era.
                IF era_row.valid_to_column_name IS NOT NULL THEN
                    drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.valid_to_column_name);
                END IF;

                IF array_length(drop_clauses, 1) > 0 THEN
                     EXECUTE format('ALTER TABLE %s %s', table_oid, array_to_string(drop_clauses, ', '));
                END IF;
            END;
        END IF;

        RETURN true;
    END IF;

    /* We must be in CASCADE mode now */

    PERFORM sql_saga.drop_for_portion_of_view(table_oid, era_name, drop_behavior, cleanup);

    PERFORM sql_saga.drop_foreign_key_by_name(table_oid, fk.foreign_key_name)
    FROM sql_saga.foreign_keys AS fk
    WHERE (fk.table_schema, fk.table_name, fk.fk_era_name) = (table_schema, table_name, era_name);

    PERFORM sql_saga.drop_unique_key_by_name(table_oid, uk.unique_key_name, drop_behavior, cleanup)
    FROM sql_saga.unique_keys AS uk
    WHERE (uk.table_schema, uk.table_name, uk.era_name) = (table_schema, table_name, era_name);

    /* Drop synchronization trigger BEFORE removing from catalog */
    IF NOT is_dropped AND (era_row.valid_from_column_name IS NOT NULL OR era_row.valid_to_column_name IS NOT NULL) THEN
        CALL sql_saga.drop_synchronize_temporal_columns_trigger(table_oid, era_name);
    END IF;

    /* Remove from catalog */
    DELETE FROM sql_saga.era AS p
    WHERE (p.table_schema, p.table_name, p.era_name) = (table_schema, table_name, era_name);

    /* Delete bounds check constraint (NOT isempty(range)). */
    IF NOT is_dropped AND era_row.bounds_check_constraint IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', table_oid /* %s */, era_row.bounds_check_constraint /* %I */);
    END IF;

    /* Delete boundary check constraint (from < until). */
    IF NOT is_dropped AND era_row.boundary_check_constraint IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', table_oid /* %s */, era_row.boundary_check_constraint /* %I */);
    END IF;

    /* Drop column default if it was set */
    IF NOT is_dropped AND NOT era_row.trigger_applies_defaults AND era_row.valid_until_column_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %s ALTER COLUMN %I DROP DEFAULT', table_oid, era_row.valid_until_column_name);
    END IF;

    /* Delete columns if purging */
    IF NOT is_dropped AND cleanup THEN
        DECLARE
            drop_clauses text[] := '{}';
        BEGIN
            -- The range column is always present for an era.
            drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.range_column_name);

            -- Drop from/until if they were part of the era.
            IF era_row.valid_from_column_name IS NOT NULL THEN
                drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.valid_from_column_name);
                drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.valid_until_column_name);
            END IF;

            -- Drop to if it was part of the era.
            IF era_row.valid_to_column_name IS NOT NULL THEN
                drop_clauses := drop_clauses || format('DROP COLUMN %I', era_row.valid_to_column_name);
            END IF;

            IF array_length(drop_clauses, 1) > 0 THEN
                 EXECUTE format('ALTER TABLE %s %s', table_oid, array_to_string(drop_clauses, ', '));
            END IF;
        END;
    END IF;

    RETURN true;
END;
$function$;

COMMENT ON FUNCTION sql_saga.drop_era(regclass, name, sql_saga.drop_behavior, boolean) IS
'Deregisters a temporal table, removing all associated constraints, triggers, and metadata.';
