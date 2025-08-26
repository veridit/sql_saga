CREATE FUNCTION sql_saga.__internal_add_system_time_era(
    table_oid regclass,
    valid_from_column_name name DEFAULT 'system_valid_from',
    valid_until_column_name name DEFAULT 'system_valid_until',
    bounds_check_constraint name DEFAULT NULL,
    infinity_check_constraint name DEFAULT NULL,
    generated_always_trigger name DEFAULT NULL,
    write_history_trigger name DEFAULT NULL,
    truncate_trigger name DEFAULT NULL,
    excluded_column_names name[] DEFAULT '{}')
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
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
    valid_from_attnum smallint;
    valid_from_type oid;
    valid_from_notnull boolean;
    valid_until_attnum smallint;
    valid_until_type oid;
    valid_until_notnull boolean;
    excluded_column_name name;
    DATE_OID CONSTANT integer := 1082;
    TIMESTAMP_OID CONSTANT integer := 1114;
    TIMESTAMPTZ_OID CONSTANT integer := 1184;
    range_type regtype;
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
    INTO valid_from_attnum, valid_from_type, valid_from_notnull
    FROM pg_catalog.pg_attribute AS a
    WHERE (a.attrelid, a.attname) = (table_oid, valid_from_column_name);

    IF NOT FOUND THEN
        alter_commands := alter_commands || format('ADD COLUMN %I timestamp with time zone NOT NULL DEFAULT ''-infinity''', valid_from_column_name);
        valid_from_attnum := 0;
        valid_from_type := 'timestamp with time zone'::regtype;
        valid_from_notnull := true;
    END IF;
    alter_commands := alter_commands || format('ALTER COLUMN %I SET DEFAULT transaction_timestamp()', valid_from_column_name);

    IF valid_from_attnum < 0 THEN
        RAISE EXCEPTION 'system columns cannot be used in an era';
    END IF;

    SELECT a.attnum, a.atttypid, a.attnotnull
    INTO valid_until_attnum, valid_until_type, valid_until_notnull
    FROM pg_catalog.pg_attribute AS a
    WHERE (a.attrelid, a.attname) = (table_oid, valid_until_column_name);

    IF NOT FOUND THEN
        alter_commands := alter_commands || format('ADD COLUMN %I timestamp with time zone NOT NULL DEFAULT ''infinity''', valid_until_column_name);
        valid_until_attnum := 0;
        valid_until_type := 'timestamp with time zone'::regtype;
        valid_until_notnull := true;
    ELSE
        alter_commands := alter_commands || format('ALTER COLUMN %I SET DEFAULT ''infinity''', valid_until_column_name);
    END IF;

    IF valid_until_attnum < 0 THEN
        RAISE EXCEPTION 'system columns cannot be used in an era';
    END IF;

    IF valid_from_type::regtype NOT IN ('date', 'timestamp without time zone', 'timestamp with time zone') THEN
        RAISE EXCEPTION 'SYSTEM_TIME eras must be of type "date", "timestamp without time zone", or "timestamp with time zone"';
    END IF;
    IF valid_from_type <> valid_until_type THEN
        RAISE EXCEPTION 'start and end columns must be of same type';
    END IF;

    CASE valid_from_type
        WHEN DATE_OID THEN range_type := 'daterange';
        WHEN TIMESTAMP_OID THEN range_type := 'tsrange';
        WHEN TIMESTAMPTZ_OID THEN range_type := 'tstzrange';
    ELSE
        RAISE EXCEPTION 'unexpected data type: "%"', valid_from_type::regtype;
    END CASE;

    IF NOT valid_from_notnull THEN
        alter_commands := alter_commands || format('ALTER COLUMN %I SET NOT NULL', valid_from_column_name);
    END IF;
    IF NOT valid_until_notnull THEN
        alter_commands := alter_commands || format('ALTER COLUMN %I SET NOT NULL', valid_until_column_name);
    END IF;

    DECLARE
        condef CONSTANT text := format('CHECK ((%I < %I))', valid_from_column_name, valid_until_column_name);
    BEGIN
        IF bounds_check_constraint IS NULL THEN
            bounds_check_constraint := sql_saga.__internal_make_name(ARRAY[table_name, era_name], 'check');
        END IF;
        alter_commands := alter_commands || format('ADD CONSTRAINT %I %s', bounds_check_constraint, condef);
    END;

    DECLARE
        condef CONSTANT text := format('CHECK ((%I = ''infinity''::timestamp with time zone))', valid_until_column_name);
    BEGIN
        IF infinity_check_constraint IS NULL THEN
            infinity_check_constraint := sql_saga.__internal_make_name(ARRAY[table_name, valid_until_column_name], 'infinity_check');
        END IF;
        alter_commands := alter_commands || format('ADD CONSTRAINT %I %s', infinity_check_constraint, condef);
    END;

    IF alter_commands <> '{}' THEN
        EXECUTE format('ALTER TABLE %I.%I %s', table_schema, table_name, array_to_string(alter_commands, ', '));
    END IF;

    generated_always_trigger := coalesce(generated_always_trigger, sql_saga.__internal_make_name(ARRAY[table_name], 'system_time_generated_always'));
    EXECUTE format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE ON %s FOR EACH ROW EXECUTE PROCEDURE sql_saga.generated_always_as_row_start_end()', generated_always_trigger, table_oid);

    write_history_trigger := coalesce(write_history_trigger, sql_saga.__internal_make_name(ARRAY[table_name], 'system_time_write_history'));
    EXECUTE format('CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE PROCEDURE sql_saga.write_history()', write_history_trigger, table_oid);

    truncate_trigger := coalesce(truncate_trigger, sql_saga.__internal_make_name(ARRAY[table_name], 'truncate'));
    EXECUTE format('CREATE TRIGGER %I AFTER TRUNCATE ON %s FOR EACH STATEMENT EXECUTE PROCEDURE sql_saga.truncate_system_versioning()', truncate_trigger, table_oid);

    INSERT INTO sql_saga.era (table_schema, table_name, era_name, valid_from_column_name, valid_until_column_name, range_type, bounds_check_constraint)
    VALUES (table_schema, table_name, era_name, valid_from_column_name, valid_until_column_name, range_type, bounds_check_constraint);

    INSERT INTO sql_saga.system_time_era (table_schema, table_name, era_name, infinity_check_constraint, generated_always_trigger, write_history_trigger, truncate_trigger, excluded_column_names)
    VALUES (table_schema, table_name, era_name, infinity_check_constraint, generated_always_trigger, write_history_trigger, truncate_trigger, excluded_column_names);

    RETURN true;
END;
$function$;
