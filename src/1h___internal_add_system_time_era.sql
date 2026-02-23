CREATE FUNCTION sql_saga.__internal_add_system_time_era(
    table_oid regclass,
    range_column_name name DEFAULT 'system_valid_range',
    infinity_check_constraint name DEFAULT NULL,
    generated_always_trigger name DEFAULT NULL,
    write_history_trigger name DEFAULT NULL,
    truncate_trigger name DEFAULT NULL,
    excluded_column_names name[] DEFAULT '{}')
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, public
AS
$function$
#variable_conflict use_variable
DECLARE
    era_name CONSTANT name := 'system_time';
    table_schema name;
    table_name name;
    kind "char";
    persistence "char";
    alter_commands text[] DEFAULT '{}';
    range_attnum smallint;
    range_type regtype;
    range_notnull boolean;
    excluded_column_name name;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    PERFORM sql_saga.__internal_serialize(table_oid);

    SELECT n.nspname, c.relname, c.relpersistence, c.relkind
    INTO table_schema, table_name, persistence, kind
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.oid = table_oid;

    IF kind <> 'r' THEN
        IF kind = 'p' THEN
            RAISE EXCEPTION 'partitioned tables are not supported yet';
        END IF;
        RAISE EXCEPTION 'relation % is not a table', table_oid;
    END IF;

    IF persistence <> 'p' THEN
        RAISE EXCEPTION 'table "%" must be persistent', table_oid;
    END IF;

    IF EXISTS (SELECT FROM sql_saga.era AS e WHERE (e.table_schema, e.table_name, e.era_name) = (table_schema, table_name, era_name)) THEN
        RAISE EXCEPTION 'era for SYSTEM_TIME already exists on table "%"', table_oid;
    END IF;

    IF EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE (a.attrelid, a.attname) = (table_oid, era_name)) THEN
        RAISE EXCEPTION 'a column named system_time already exists for table "%"', table_oid;
    END IF;

    SELECT a.attnum, a.atttypid, a.attnotnull
    INTO range_attnum, range_type, range_notnull
    FROM pg_catalog.pg_attribute AS a
    WHERE (a.attrelid, a.attname) = (table_oid, range_column_name);

    IF NOT FOUND THEN
        alter_commands := alter_commands || format('ADD COLUMN %I tstzrange NOT NULL DEFAULT tstzrange(''-infinity'', ''infinity'')', range_column_name);
        range_attnum := 0;
        range_type := 'tstzrange'::regtype;
        range_notnull := true;
    END IF;
    alter_commands := alter_commands || format('ALTER COLUMN %I SET DEFAULT tstzrange(transaction_timestamp(), ''infinity'')', range_column_name);

    IF range_attnum < 0 THEN
        RAISE EXCEPTION 'system columns cannot be used as a system time era range';
    END IF;

    IF range_type <> 'tstzrange'::regtype THEN
        RAISE EXCEPTION 'system time era range column must be of type tstzrange';
    END IF;

    IF NOT range_notnull THEN
        alter_commands := alter_commands || format('ALTER COLUMN %I SET NOT NULL', range_column_name);
    END IF;

    DECLARE
        condef CONSTANT text := format('CHECK ((upper(%I) = ''infinity''::timestamp with time zone))', range_column_name);
    BEGIN
        IF infinity_check_constraint IS NULL THEN
            infinity_check_constraint := sql_saga.__internal_make_name(ARRAY[table_name, range_column_name], 'infinity_check');
        END IF;
        alter_commands := alter_commands || format('ADD CONSTRAINT %I %s', infinity_check_constraint /* %I */, condef /* %s */);
    END;

    IF alter_commands <> '{}' THEN
        EXECUTE format('ALTER TABLE %I.%I %s', table_schema /* %I */, table_name /* %I */, array_to_string(alter_commands, ', ') /* %s */);
    END IF;

    generated_always_trigger := coalesce(generated_always_trigger, sql_saga.__internal_make_name(ARRAY[table_name], 'system_time_generated_always'));
    EXECUTE format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE ON %s FOR EACH ROW EXECUTE PROCEDURE sql_saga.generated_always_as_row_start_end(%L)', generated_always_trigger, table_oid, range_column_name);

    write_history_trigger := coalesce(write_history_trigger, sql_saga.__internal_make_name(ARRAY[table_name], 'system_time_write_history'));
    EXECUTE format('CREATE TRIGGER %I AFTER UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE PROCEDURE sql_saga.write_history(%L)', write_history_trigger, table_oid, range_column_name);

    truncate_trigger := coalesce(truncate_trigger, sql_saga.__internal_make_name(ARRAY[table_name], 'truncate'));
    EXECUTE format('CREATE TRIGGER %I AFTER TRUNCATE ON %s FOR EACH STATEMENT EXECUTE PROCEDURE sql_saga.truncate_system_versioning()', truncate_trigger /* %I */, table_oid /* %s */);

    INSERT INTO sql_saga.era (table_schema, table_name, era_name, range_column_name, valid_from_column_name, valid_until_column_name, range_type, multirange_type, range_subtype, range_subtype_category, bounds_check_constraint)
    VALUES (table_schema, table_name, era_name, range_column_name, NULL, NULL, 'tstzrange', 'tstzmultirange', 'timestamptz', 'D', NULL);

    INSERT INTO sql_saga.system_time_era (table_schema, table_name, era_name, infinity_check_constraint, generated_always_trigger, write_history_trigger, truncate_trigger, excluded_column_names)
    VALUES (table_schema, table_name, era_name, infinity_check_constraint, generated_always_trigger, write_history_trigger, truncate_trigger, excluded_column_names);

    RETURN true;
END;
$function$;
