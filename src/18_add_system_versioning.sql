CREATE FUNCTION sql_saga.add_system_versioning(
    table_oid regclass,
    history_table_name name DEFAULT NULL,
    view_name name DEFAULT NULL,
    function_as_of_name name DEFAULT NULL,
    function_between_name name DEFAULT NULL,
    function_between_symmetric_name name DEFAULT NULL,
    function_from_to_name name DEFAULT NULL)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    table_schema name;
    table_name name;
    table_owner regrole;
    persistence "char";
    kind "char";
    era_row sql_saga.era;
    history_table_id oid;
    sql text;
    grantees text;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    PERFORM sql_saga.__internal_serialize(table_oid);

    SELECT n.nspname, c.relname, c.relowner, c.relpersistence, c.relkind
    INTO table_schema, table_name, table_owner, persistence, kind
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.oid = table_oid;

    IF EXISTS (SELECT 1 FROM sql_saga.system_versioning sv WHERE (sv.table_schema, sv.table_name) = (table_schema, table_name)) THEN
        RAISE EXCEPTION 'table already has SYSTEM VERSIONING';
    END IF;

    IF kind <> 'r' THEN
        IF kind = 'p' THEN
            RAISE EXCEPTION 'partitioned tables are not supported yet';
        END IF;
        RAISE EXCEPTION 'relation % is not a table', table_oid;
    END IF;

    IF persistence <> 'p' THEN
        RAISE EXCEPTION 'table "%" must be persistent', table_oid;
    END IF;

    SELECT e.*
    INTO era_row
    FROM sql_saga.era AS e
    WHERE (e.table_schema, e.table_name, e.era_name) = (table_schema, table_name, 'system_time');

    IF NOT FOUND THEN
        PERFORM sql_saga.__internal_add_system_time_era(table_oid);
        -- Re-fetch era row
        SELECT e.* INTO era_row FROM sql_saga.era e WHERE (e.table_schema, e.table_name, e.era_name) = (table_schema, table_name, 'system_time');
    END IF;

    history_table_name := coalesce(history_table_name, sql_saga.__internal_make_name(ARRAY[table_name], 'history'));
    view_name := coalesce(view_name, sql_saga.__internal_make_name(ARRAY[table_name], 'with_history'));
    function_as_of_name := coalesce(function_as_of_name, sql_saga.__internal_make_name(ARRAY[table_name], '_as_of'));
    function_between_name := coalesce(function_between_name, sql_saga.__internal_make_name(ARRAY[table_name], '_between'));
    function_between_symmetric_name := coalesce(function_between_symmetric_name, sql_saga.__internal_make_name(ARRAY[table_name], '_between_symmetric'));
    function_from_to_name := coalesce(function_from_to_name, sql_saga.__internal_make_name(ARRAY[table_name], '_from_to'));

    SELECT c.oid INTO history_table_id FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace WHERE (n.nspname, c.relname) = (table_schema, history_table_name);

    IF FOUND THEN
        IF EXISTS (SELECT 1 FROM sql_saga.era p JOIN pg_class c ON p.table_name = c.relname JOIN pg_namespace n ON c.relnamespace = n.oid AND p.table_schema = n.nspname WHERE c.oid = history_table_id) THEN
            RAISE EXCEPTION 'history tables for SYSTEM VERSIONING cannot have eras';
        END IF;
        IF EXISTS (
            WITH
            L (attname, atttypid, atttypmod, attcollation) AS (SELECT a.attname, a.atttypid, a.atttypmod, a.attcollation FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = table_oid AND NOT a.attisdropped),
            R (attname, atttypid, atttypmod, attcollation) AS (SELECT a.attname, a.atttypid, a.atttypmod, a.attcollation FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = history_table_id AND NOT a.attisdropped)
            SELECT FROM L NATURAL FULL JOIN R WHERE L.attname IS NULL OR R.attname IS NULL)
        THEN
            RAISE EXCEPTION 'base table "%" and history table "%" are not compatible', table_oid, history_table_id::regclass;
        END IF;
        EXECUTE format('ALTER TABLE %s OWNER TO %I', history_table_id::regclass /* %s */, table_owner /* %I */);
    ELSE
        EXECUTE format('CREATE TABLE %1$I.%2$I (LIKE %1$I.%3$I)', table_schema /* %1$I */, history_table_name /* %2$I */, table_name /* %3$I */);
        history_table_id := format('%I.%I', table_schema /* %I */, history_table_name /* %I */)::regclass;
        EXECUTE format('ALTER TABLE %1$I.%2$I OWNER TO %3$I', table_schema /* %1$I */, history_table_name /* %2$I */, table_owner /* %3$I */);
        RAISE DEBUG 'history table "%" created for "%", be sure to index it properly', history_table_id::regclass, table_oid;
    END IF;

    EXECUTE format('CREATE VIEW %1$I.%2$I AS SELECT %5$s FROM %1$I.%3$I UNION ALL SELECT %5$s FROM %1$I.%4$I',
        table_schema, /* %1$I */
        view_name, /* %2$I */
        table_name, /* %3$I */
        history_table_name, /* %4$I */
        (SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum) FROM pg_attribute AS a WHERE a.attrelid = table_oid AND a.attnum > 0 AND NOT a.attisdropped) /* %5$s */
    );
    EXECUTE format('ALTER VIEW %1$I.%2$I OWNER TO %3$I', table_schema /* %1$I */, view_name /* %2$I */, table_owner /* %3$I */);

    EXECUTE format($$ CREATE FUNCTION %1$I.%2$I(timestamp with time zone) RETURNS SETOF %1$I.%3$I LANGUAGE sql STABLE AS 'SELECT * FROM %1$I.%3$I WHERE %4$I <= $1 AND %5$I > $1' $$, table_schema /* %1$I */, function_as_of_name /* %2$I */, view_name /* %3$I */, era_row.valid_from_column_name /* %4$I */, era_row.valid_until_column_name /* %5$I */);
    EXECUTE format('ALTER FUNCTION %1$I.%2$I(timestamp with time zone) OWNER TO %3$I', table_schema /* %1$I */, function_as_of_name /* %2$I */, table_owner /* %3$I */);
    EXECUTE format($$ CREATE FUNCTION %1$I.%2$I(timestamp with time zone, timestamp with time zone) RETURNS SETOF %1$I.%3$I LANGUAGE sql STABLE AS 'SELECT * FROM %1$I.%3$I WHERE $1 <= $2 AND %5$I > $1 AND %4$I <= $2' $$, table_schema /* %1$I */, function_between_name /* %2$I */, view_name /* %3$I */, era_row.valid_from_column_name /* %4$I */, era_row.valid_until_column_name /* %5$I */);
    EXECUTE format('ALTER FUNCTION %1$I.%2$I(timestamp with time zone, timestamp with time zone) OWNER TO %3$I', table_schema /* %1$I */, function_between_name /* %2$I */, table_owner /* %3$I */);
    EXECUTE format($$ CREATE FUNCTION %1$I.%2$I(timestamp with time zone, timestamp with time zone) RETURNS SETOF %1$I.%3$I LANGUAGE sql STABLE AS 'SELECT * FROM %1$I.%3$I WHERE %5$I > least($1, $2) AND %4$I <= greatest($1, $2)' $$, table_schema /* %1$I */, function_between_symmetric_name /* %2$I */, view_name /* %3$I */, era_row.valid_from_column_name /* %4$I */, era_row.valid_until_column_name /* %5$I */);
    EXECUTE format('ALTER FUNCTION %1$I.%2$I(timestamp with time zone, timestamp with time zone) OWNER TO %3$I', table_schema /* %1$I */, function_between_symmetric_name /* %2$I */, table_owner /* %3$I */);
    EXECUTE format($$ CREATE FUNCTION %1$I.%2$I(timestamp with time zone, timestamp with time zone) RETURNS SETOF %1$I.%3$I LANGUAGE sql STABLE AS 'SELECT * FROM %1$I.%3$I WHERE $1 < $2 AND %5$I > $1 AND %4$I < $2' $$, table_schema /* %1$I */, function_from_to_name /* %2$I */, view_name /* %3$I */, era_row.valid_from_column_name /* %4$I */, era_row.valid_until_column_name /* %5$I */);
    EXECUTE format('ALTER FUNCTION %1$I.%2$I(timestamp with time zone, timestamp with time zone) OWNER TO %3$I', table_schema /* %1$I */, function_from_to_name /* %2$I */, table_owner /* %3$I */);

    -- TODO: Set privileges on history objects

    INSERT INTO sql_saga.system_versioning (table_schema, table_name, era_name, history_schema_name, history_table_name, view_schema_name, view_table_name, func_as_of, func_between, func_between_symmetric, func_from_to)
    VALUES (
        table_schema, table_name, 'system_time',
        table_schema, history_table_name,
        table_schema, view_name,
        format('%I.%I(timestamp with time zone)', table_schema /* %I */, function_as_of_name /* %I */),
        format('%I.%I(timestamp with time zone,timestamp with time zone)', table_schema /* %I */, function_between_name /* %I */),
        format('%I.%I(timestamp with time zone,timestamp with time zone)', table_schema /* %I */, function_between_symmetric_name /* %I */),
        format('%I.%I(timestamp with time zone,timestamp with time zone)', table_schema /* %I */, function_from_to_name /* %I */)
    );
END;
$function$;
