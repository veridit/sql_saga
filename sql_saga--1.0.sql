-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION sql_saga" to load this file. \quit

/* This extension is non-relocatable */
CREATE SCHEMA sql_saga;
GRANT USAGE ON SCHEMA sql_saga TO PUBLIC;

CREATE TYPE sql_saga.drop_behavior AS ENUM ('CASCADE', 'RESTRICT');
CREATE TYPE sql_saga.fk_actions AS ENUM ('CASCADE', 'SET NULL', 'SET DEFAULT', 'RESTRICT', 'NO ACTION');
CREATE TYPE sql_saga.fk_match_types AS ENUM ('FULL', 'PARTIAL', 'SIMPLE');

/*
 * All referencing columns must be either name or regsomething in order for
 * pg_dump to work properly.  Plain OIDs are not allowed but attribute numbers
 * are, so that we don't have to track renames.
 *
 * Anything declared as regsomething and created for the period (such as the
 * "__as_of" function), should be UNIQUE.  If Postgres already verifies
 * uniqueness, such as constraint names on a table, then we don't need to do it
 * also.
 */

CREATE TABLE sql_saga.era (
    table_oid regclass NOT NULL,
    era_name name NOT NULL DEFAULT 'valid',
    start_after_column_name name NOT NULL,
    stop_on_column_name name NOT NULL,
    -- active_column_name name NOT NULL,
    range_type regtype NOT NULL,
    bounds_check_constraint name NOT NULL,
    -- infinity_check_constraint name NOT NULL,
    -- generated_always_trigger name NOT NULL,
    audit_table_oid regclass, -- NOT NULL
    -- audit_trigger name NOT NULL,
    -- delete_trigger name NOT NULL,
    --excluded_column_names name[] NOT NULL DEFAULT '{}',
    -- UNIQUE(...) for each trigger/function name.

    PRIMARY KEY (table_oid, era_name),

    CHECK (start_after_column_name <> stop_on_column_name),
    CHECK (era_name <> 'system_time')
);
COMMENT ON TABLE sql_saga.era IS 'The main catalog for sql_saga.  All "DDL" operations for periods must first take an exclusive lock on this table.';
GRANT SELECT ON TABLE sql_saga.era TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('sql_saga.era', '');

CREATE TABLE sql_saga.unique_keys (
    unique_key_name name NOT NULL,
    table_oid regclass NOT NULL,
    column_names name[] NOT NULL,
    era_name name NOT NULL,
    unique_constraint name NOT NULL,
    exclude_constraint name NOT NULL,

    PRIMARY KEY (unique_key_name),

    FOREIGN KEY (table_oid, era_name) REFERENCES sql_saga.era
);
GRANT SELECT ON TABLE sql_saga.unique_keys TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('sql_saga.unique_keys', '');

COMMENT ON TABLE sql_saga.unique_keys IS 'A registry of UNIQUE/PRIMARY keys using era WITHOUT OVERLAPS';

CREATE TABLE sql_saga.foreign_keys (
    foreign_key_name name NOT NULL,
    table_oid regclass NOT NULL,
    column_names name[] NOT NULL,
    era_name name NOT NULL,
    unique_key_name name NOT NULL,
    match_type sql_saga.fk_match_types NOT NULL DEFAULT 'SIMPLE',
    update_action sql_saga.fk_actions NOT NULL DEFAULT 'NO ACTION',
    delete_action sql_saga.fk_actions NOT NULL DEFAULT 'NO ACTION',
    fk_insert_trigger name NOT NULL,
    fk_update_trigger name NOT NULL,
    uk_update_trigger name NOT NULL,
    uk_delete_trigger name NOT NULL,

    PRIMARY KEY (foreign_key_name),

    FOREIGN KEY (table_oid, era_name) REFERENCES sql_saga.era,
    FOREIGN KEY (unique_key_name) REFERENCES sql_saga.unique_keys,

    CHECK (delete_action NOT IN ('CASCADE', 'SET NULL', 'SET DEFAULT')),
    CHECK (update_action NOT IN ('CASCADE', 'SET NULL', 'SET DEFAULT'))
);
GRANT SELECT ON TABLE sql_saga.foreign_keys TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('sql_saga.foreign_keys', '');

COMMENT ON TABLE sql_saga.foreign_keys IS 'A registry of foreign keys using era WITHOUT OVERLAPS';


CREATE VIEW sql_saga.information_schema__era AS
    SELECT current_catalog AS table_catalog,
           n.nspname AS table_schema,
           c.relname AS table_name,
           e.era_name,
           e.start_after_column_name,
           e.stop_on_column_name
    FROM sql_saga.era AS e
    JOIN pg_catalog.pg_class AS c ON c.oid = e.table_oid
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace;


CREATE TABLE sql_saga.api_view (
    table_oid regclass NOT NULL,
    era_name name NOT NULL,
    view_oid regclass NOT NULL,
    trigger_name name NOT NULL,
    -- truncate_trigger name NOT NULL,

    PRIMARY KEY (table_oid, era_name),

    FOREIGN KEY (table_oid, era_name) REFERENCES sql_saga.era,

    UNIQUE (view_oid)
);
GRANT SELECT ON TABLE sql_saga.api_view TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('sql_saga.api_view', '');

/*
 * C Helper functions
 */
CREATE OR REPLACE FUNCTION sql_saga.covers_without_gaps_transfn(internal, anyrange, anyrange)
RETURNS internal
AS 'sql_saga', 'covers_without_gaps_transfn'
LANGUAGE c;

CREATE OR REPLACE FUNCTION sql_saga.fk_insert_check_c()
RETURNS trigger
AS 'sql_saga', 'fk_insert_check_c'
LANGUAGE c;

CREATE OR REPLACE FUNCTION sql_saga.covers_without_gaps_finalfn(internal, anyrange, anyrange)
RETURNS boolean
AS 'sql_saga', 'covers_without_gaps_finalfn'
LANGUAGE c;

/*
 * covers_without_gaps(period anyrange, target anyrange) -
 * Returns true if the collected `period` ranges are contiguous (have no gaps)
 * and completely cover the fixed `target` range.
 */
CREATE AGGREGATE sql_saga.covers_without_gaps(anyrange, anyrange) (
  sfunc = sql_saga.covers_without_gaps_transfn,
  stype = internal,
  finalfunc = sql_saga.covers_without_gaps_finalfn,
  finalfunc_extra
);

/*
 * Generic trigger function to synchronize valid_from and valid_after columns.
 * Ensures valid_from = valid_after + 1 day.
 */
CREATE FUNCTION sql_saga.synchronize_valid_from_after()
RETURNS TRIGGER LANGUAGE plpgsql AS $synchronize_valid_from_after$
BEGIN
    -- For INSERT operations
    IF TG_OP = 'INSERT' THEN
        IF NEW.valid_from IS NOT NULL AND NEW.valid_after IS NULL THEN
            NEW.valid_after := NEW.valid_from - INTERVAL '1 day';
        ELSIF NEW.valid_after IS NOT NULL AND NEW.valid_from IS NULL THEN
            NEW.valid_from := NEW.valid_after + INTERVAL '1 day';
        ELSIF NEW.valid_from IS NOT NULL AND NEW.valid_after IS NOT NULL THEN
            IF NEW.valid_after != (NEW.valid_from - INTERVAL '1 day') THEN
                RAISE EXCEPTION 'On INSERT, valid_from and valid_after are inconsistent. Expected valid_after = valid_from - 1 day. Got valid_from=%, valid_after=%', NEW.valid_from, NEW.valid_after;
            END IF;
        ELSE -- Both are NULL, set a default validity period starting today
            NEW.valid_after := current_date - INTERVAL '1 day'; -- (exclusive start) yesterday
            NEW.valid_from  := current_date;                   -- (inclusive start) today
            RAISE DEBUG 'On INSERT for table %, both valid_from and valid_after were NULL. Defaulted: valid_from=%, valid_after=%', TG_TABLE_NAME, NEW.valid_from, NEW.valid_after;
        END IF;

    -- For UPDATE operations
    ELSIF TG_OP = 'UPDATE' THEN
        -- Case 1: Both valid_from and valid_after are being explicitly changed by the UPDATE statement
        IF NEW.valid_from IS DISTINCT FROM OLD.valid_from AND NEW.valid_after IS DISTINCT FROM OLD.valid_after THEN
            IF NEW.valid_from IS NULL OR NEW.valid_after IS NULL THEN
                RAISE EXCEPTION 'On UPDATE for table %, when changing both valid_from and valid_after, neither can be set to NULL. Attempted valid_from=%, valid_after=%', TG_TABLE_NAME, NEW.valid_from, NEW.valid_after;
            END IF;
            IF NEW.valid_after != (NEW.valid_from - INTERVAL '1 day') THEN
                RAISE EXCEPTION 'On UPDATE for table %, conflicting explicit values for valid_from and valid_after. With valid_from=%, expected valid_after=%. Got valid_after=%', 
                                 TG_TABLE_NAME, NEW.valid_from, NEW.valid_from - INTERVAL '1 day', NEW.valid_after;
            END IF;
        -- Case 2: Only valid_from is being explicitly changed (and valid_after was not, or its change was not distinct)
        ELSIF NEW.valid_from IS DISTINCT FROM OLD.valid_from THEN
            IF NEW.valid_from IS NULL THEN
                RAISE EXCEPTION 'On UPDATE for table %, valid_from cannot be set to NULL. Attempted valid_from=%, valid_after=%', TG_TABLE_NAME, NEW.valid_from, NEW.valid_after;
            END IF;
            NEW.valid_after := NEW.valid_from - INTERVAL '1 day';
        -- Case 3: Only valid_after is being explicitly changed (and valid_from was not, or its change was not distinct)
        ELSIF NEW.valid_after IS DISTINCT FROM OLD.valid_after THEN
            IF NEW.valid_after IS NULL THEN
                RAISE EXCEPTION 'On UPDATE for table %, valid_after cannot be set to NULL. Attempted valid_from=%, valid_after=%', TG_TABLE_NAME, NEW.valid_from, NEW.valid_after;
            END IF;
            NEW.valid_from := NEW.valid_after + INTERVAL '1 day';
        -- Case 4: Neither valid_from nor valid_after is being distinctly changed by the UPDATE statement's SET clause.
        ELSE
            IF NEW.valid_from IS NULL OR NEW.valid_after IS NULL THEN
                 RAISE EXCEPTION 'On UPDATE for table %, valid_from and valid_after cannot be NULL (and were not changed by SET clause). Got valid_from=%, valid_after=%', TG_TABLE_NAME, NEW.valid_from, NEW.valid_after;
            END IF;
            IF NEW.valid_after != (NEW.valid_from - INTERVAL '1 day') THEN
                 RAISE EXCEPTION 'On UPDATE for table %, existing valid_from and valid_after are inconsistent (and were not changed by SET clause). Got valid_from=%, valid_after=%', TG_TABLE_NAME, NEW.valid_from, NEW.valid_after;
            END IF;
        END IF;
        
        -- Final safeguard checks after all logic.
        IF NEW.valid_from IS NULL OR NEW.valid_after IS NULL THEN
            RAISE EXCEPTION 'On UPDATE for table %, valid_from and valid_after must result in non-NULL values. Got valid_from=%, valid_after=%', TG_TABLE_NAME, NEW.valid_from, NEW.valid_after;
        END IF;

        IF NEW.valid_after != (NEW.valid_from - INTERVAL '1 day') THEN
            RAISE EXCEPTION 'On UPDATE for table %, derived valid_from and valid_after are inconsistent after all processing. Got valid_from=%, valid_after=%', TG_TABLE_NAME, NEW.valid_from, NEW.valid_after;
        END IF;
    END IF;
    RETURN NEW;
END;
$synchronize_valid_from_after$;



/*
 * These function starting with "_" are private to the periods extension and
 * should not be called by outsiders.  When all the other functions have been
 * translated to C, they will be removed.
 */
CREATE FUNCTION sql_saga._serialize(table_name regclass)
 RETURNS void
 LANGUAGE sql
AS
$function$
/* XXX: Is this the best way to do locking? */
SELECT pg_catalog.pg_advisory_xact_lock('sql_saga.era'::regclass::oid::integer, table_name::oid::integer);
$function$;

CREATE FUNCTION sql_saga._make_name(resizable text[], fixed text DEFAULT NULL, separator text DEFAULT '_', extra integer DEFAULT 2)
 RETURNS name
 IMMUTABLE
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    max_length integer;
    result text;

    NAMEDATALEN CONSTANT integer := 64;
BEGIN
    /*
     * Reduce the resizable texts until they and the fixed text fit in
     * NAMEDATALEN.  This probably isn't very efficient but it's not on a hot
     * code path so we don't care.
     */

    SELECT max(length(t))
    INTO max_length
    FROM unnest(resizable) AS u (t);

    LOOP
        result := format('%s%s', array_to_string(resizable, separator), separator || fixed);
        IF octet_length(result) <= NAMEDATALEN-extra-1 THEN
            RETURN result;
        END IF;

        max_length := max_length - 1;
        resizable := ARRAY (
            SELECT left(t, max_length)
            FROM unnest(resizable) WITH ORDINALITY AS u (t, o)
            ORDER BY o
        );
    END LOOP;
END;
$function$;

CREATE FUNCTION sql_saga._make_api_view_name(table_name name, era_name name)
 RETURNS name
 IMMUTABLE
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    max_length integer;
    result text;

    NAMEDATALEN CONSTANT integer := 64;
BEGIN
    /*
     * Reduce the table and period names until they fit in NAMEDATALEN.  This
     * probably isn't very efficient but it's not on a hot code path so we
     * don't care.
     */

    max_length := greatest(length(table_name), length(era_name));

    LOOP
        result := format('%s__for_portion_of_%s', table_name, era_name);
        IF octet_length(result) <= NAMEDATALEN-1 THEN
            RETURN result;
        END IF;

        max_length := max_length - 1;
        table_name := left(table_name, max_length);
        era_name := left(era_name, max_length);
    END LOOP;
END;
$function$;


CREATE TYPE sql_saga.foreign_key_metadata AS (
    fk_table_oid regclass,
    fk_schema_name name,
    fk_table_name name,
    fk_column_names name[],
    fk_era_name name,
    fk_start_after_column_name name,
    fk_stop_on_column_name name,

    uk_table_oid regclass,
    uk_schema_name name,
    uk_table_name name,
    uk_column_names name[],
    uk_era_name name,
    uk_start_after_column_name name,
    uk_stop_on_column_name name,

    match_type sql_saga.fk_match_types,
    update_action sql_saga.fk_actions,
    delete_action sql_saga.fk_actions
);


CREATE FUNCTION sql_saga._get_foreign_key_metadata(foreign_key_name_param name)
RETURNS SETOF sql_saga.foreign_key_metadata
STABLE
LANGUAGE plpgsql
AS
$function$
BEGIN
    RETURN QUERY
    SELECT fc.oid::regclass     AS fk_table_oid,
           fn.nspname           AS fk_schema_name,
           fc.relname           AS fk_table_name,
           fk.column_names      AS fk_column_names,
           fe.era_name          AS fk_era_name,
           fe.start_after_column_name AS fk_start_after_column_name,
           fe.stop_on_column_name   AS fk_stop_on_column_name,

           uc.oid::regclass     AS uk_table_oid,
           un.nspname           AS uk_schema_name,
           uc.relname           AS uk_table_name,
           uk.column_names      AS uk_column_names,
           ue.era_name          AS uk_era_name,
           ue.start_after_column_name AS uk_start_after_column_name,
           ue.stop_on_column_name   AS uk_stop_on_column_name,

           fk.match_type        AS match_type,
           fk.update_action     AS update_action,
           fk.delete_action     AS delete_action
    FROM sql_saga.foreign_keys AS fk
    JOIN sql_saga.era AS fe ON (fe.table_oid, fe.era_name) = (fk.table_oid, fk.era_name)
    JOIN pg_catalog.pg_class AS fc ON fc.oid = fk.table_oid
    JOIN pg_catalog.pg_namespace AS fn ON fn.oid = fc.relnamespace
    JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
    JOIN sql_saga.era AS ue ON (ue.table_oid, ue.era_name) = (uk.table_oid, uk.era_name)
    JOIN pg_catalog.pg_class AS uc ON uc.oid = uk.table_oid
    JOIN pg_catalog.pg_namespace AS un ON un.oid = uc.relnamespace
    WHERE fk.foreign_key_name = foreign_key_name_param;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'foreign key "%" not found', foreign_key_name_param;
    END IF;
END;
$function$;


CREATE FUNCTION sql_saga.add_era(
    table_oid regclass,
    start_after_column_name name,
    stop_on_column_name name,
    era_name name DEFAULT 'valid',
    range_type regtype DEFAULT NULL,
    bounds_check_constraint name DEFAULT NULL)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    table_name name;
    kind "char";
    persistence "char";
    alter_commands text[] DEFAULT '{}';

    start_attnum smallint;
    start_type oid;
    start_collation oid;
    start_notnull boolean;

    end_attnum smallint;
    end_type oid;
    end_collation oid;
    end_notnull boolean;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    IF era_name IS NULL THEN
        RAISE EXCEPTION 'no era name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga._serialize(table_oid);

    /* Period names are limited to lowercase alphanumeric characters for now */
    era_name := lower(era_name);
    IF era_name !~ '^[a-z_][0-9a-z_]*$' THEN
        RAISE EXCEPTION 'only alphanumeric characters are currently allowed';
    END IF;

    /* Must be a regular persistent base table. SQL:2016 11.27 SR 2 */

    SELECT c.relpersistence, c.relkind
    INTO persistence, kind
    FROM pg_catalog.pg_class AS c
    WHERE c.oid = table_oid;

    IF kind <> 'r' THEN
        /*
         * The main reason partitioned tables aren't supported yet is simply
         * because I haven't put any thought into it.
         * Maybe it's trivial, maybe not.
         */
        IF kind = 'p' THEN
            RAISE EXCEPTION 'partitioned tables are not supported yet';
        END IF;

        RAISE EXCEPTION 'relation % is not a table', $1;
    END IF;

    IF persistence <> 'p' THEN
        /* We could probably accept unlogged tables but what's the point? */
        RAISE EXCEPTION 'table "%" must be persistent', table_oid;
    END IF;

    /*
     * Check if era already exists.  Actually no other application time
     * eras are allowed per spec, but we don't obey that.  We can have as
     * many application time eras as we want.
     *
     * SQL:2016 11.27 SR 5.b
     */
    IF EXISTS (SELECT FROM sql_saga.era AS p WHERE (p.table_oid, p.era_name) = (table_oid, era_name)) THEN
        RAISE EXCEPTION 'era for "%" already exists on table "%"', era_name, table_oid;
    END IF;

    /*
     * Although we are not creating a new object, the SQL standard says that
     * periods are in the same namespace as columns, so prevent that.
     *
     * SQL:2016 11.27 SR 5.c
     */
    IF EXISTS (
        SELECT FROM pg_catalog.pg_attribute AS a
        WHERE (a.attrelid, a.attname) = (table_oid, era_name))
    THEN
        RAISE EXCEPTION 'a column named "%" already exists for table "%"', era_name, table_oid;
    END IF;

    /*
     * Contrary to SYSTEM_TIME periods, the columns must exist already for
     * application time sql_saga.
     *
     * SQL:2016 11.27 SR 5.d
     */

    /* Get start column information */
    SELECT a.attnum, a.atttypid, a.attcollation, a.attnotnull
    INTO start_attnum, start_type, start_collation, start_notnull
    FROM pg_catalog.pg_attribute AS a
    WHERE (a.attrelid, a.attname) = (table_oid, start_after_column_name);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'column "%" not found in table "%"', start_after_column_name, table_oid;
    END IF;

    IF start_attnum < 0 THEN
        RAISE EXCEPTION 'system columns cannot be used in an era';
    END IF;

    /* Get end column information */
    SELECT a.attnum, a.atttypid, a.attcollation, a.attnotnull
    INTO end_attnum, end_type, end_collation, end_notnull
    FROM pg_catalog.pg_attribute AS a
    WHERE (a.attrelid, a.attname) = (table_oid, stop_on_column_name);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'column "%" not found in table "%"', stop_on_column_name, table_oid;
    END IF;

    IF end_attnum < 0 THEN
        RAISE EXCEPTION 'system columns cannot be used in an era';
    END IF;

    /*
     * Verify compatibility of start/end columns.  The standard says these must
     * be either date or timestamp, but we allow anything with a corresponding
     * range type because why not.
     *
     * SQL:2016 11.27 SR 5.g
     */
    IF start_type <> end_type THEN
        RAISE EXCEPTION 'start and end columns must be of same type';
    END IF;

    IF start_collation <> end_collation THEN
        RAISE EXCEPTION 'start and end columns must be of same collation';
    END IF;

    /* Get the range type that goes with these columns */
    IF range_type IS NOT NULL THEN
        IF NOT EXISTS (
            SELECT FROM pg_catalog.pg_range AS r
            WHERE (r.rngtypid, r.rngsubtype, r.rngcollation) = (range_type, start_type, start_collation))
        THEN
            RAISE EXCEPTION 'range "%" does not match data type "%"', range_type, start_type;
        END IF;
    ELSE
        SELECT r.rngtypid
        INTO range_type
        FROM pg_catalog.pg_range AS r
        JOIN pg_catalog.pg_opclass AS c ON c.oid = r.rngsubopc
        WHERE (r.rngsubtype, r.rngcollation) = (start_type, start_collation)
          AND c.opcdefault;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'no default range type for %', start_type::regtype;
        END IF;
    END IF;

    /*
     * Period columns must not be nullable.
     *
     * SQL:2016 11.27 SR 5.h
     */
    IF NOT start_notnull THEN
        alter_commands := alter_commands || format('ALTER COLUMN %I SET NOT NULL', start_after_column_name);
    END IF;
    IF NOT end_notnull THEN
        alter_commands := alter_commands || format('ALTER COLUMN %I SET NOT NULL', stop_on_column_name);
    END IF;

    /*
     * Find and appropriate a CHECK constraint to make sure that start < end.
     * Create one if necessary.
     *
     * SQL:2016 11.27 GR 2.b
     */
    DECLARE
        condef CONSTANT text := format('CHECK ((%I < %I))', start_after_column_name, stop_on_column_name);
        context text;
    BEGIN
        IF bounds_check_constraint IS NOT NULL THEN
            /* We were given a name, does it exist? */
            SELECT pg_catalog.pg_get_constraintdef(c.oid)
            INTO context
            FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.conname) = (table_oid, bounds_check_constraint)
              AND c.contype = 'c';

            IF FOUND THEN
                /* Does it match? */
                IF context <> condef THEN
                    RAISE EXCEPTION 'constraint "%" on table "%" does not match', bounds_check_constraint, table_oid;
                END IF;
            ELSE
                /* If it doesn't exist, we'll use the name for the one we create. */
                alter_commands := alter_commands || format('ADD CONSTRAINT %I %s', bounds_check_constraint, condef);
            END IF;
        ELSE
            /* No name given, can we appropriate one? */
            SELECT c.conname
            INTO bounds_check_constraint
            FROM pg_catalog.pg_constraint AS c
            WHERE c.conrelid = table_oid
              AND c.contype = 'c'
              AND pg_catalog.pg_get_constraintdef(c.oid) = condef;

            /* Make our own then */
            IF NOT FOUND THEN
                SELECT c.relname
                INTO table_name
                FROM pg_catalog.pg_class AS c
                WHERE c.oid = table_oid;

                bounds_check_constraint := sql_saga._make_name(ARRAY[table_name, era_name], 'check');
                alter_commands := alter_commands || format('ADD CONSTRAINT %I %s', bounds_check_constraint, condef);
            END IF;
        END IF;
    END;

-- TODO: Ensure that infinity is the default.
--    /*
--     * Find and appropriate a CHECK constraint to make sure that end = 'infinity'.
--     * Create one if necessary.
--     *
--     * SQL:2016 4.15.2.2
--     */
--    DECLARE
--        condef CONSTANT text := format('CHECK ((%I = ''infinity''::timestamp with time zone))', stop_on_column_name);
--        context text;
--    BEGIN
--        IF infinity_check_constraint IS NOT NULL THEN
--            /* We were given a name, does it exist? */
--            SELECT pg_catalog.pg_get_constraintdef(c.oid)
--            INTO context
--            FROM pg_catalog.pg_constraint AS c
--            WHERE (c.conrelid, c.conname) = (table_class, infinity_check_constraint)
--              AND c.contype = 'c';
--
--            IF FOUND THEN
--                /* Does it match? */
--                IF context <> condef THEN
--                    RAISE EXCEPTION 'constraint "%" on table "%" does not match', infinity_check_constraint, table_class;
--                END IF;
--            ELSE
--                /* If it doesn't exist, we'll use the name for the one we create. */
--                alter_commands := alter_commands || format('ADD CONSTRAINT %I %s', infinity_check_constraint, condef);
--            END IF;
--        ELSE
--            /* No name given, can we appropriate one? */
--            SELECT c.conname
--            INTO infinity_check_constraint
--            FROM pg_catalog.pg_constraint AS c
--            WHERE c.conrelid = table_class
--              AND c.contype = 'c'
--              AND pg_catalog.pg_get_constraintdef(c.oid) = condef;
--
--            /* Make our own then */
--            IF NOT FOUND THEN
--                SELECT c.relname
--                INTO table_name
--                FROM pg_catalog.pg_class AS c
--                WHERE c.oid = table_class;
--
--                infinity_check_constraint := sql_saga._make_name(ARRAY[table_name, stop_on_column_name], 'infinity_check');
--                alter_commands := alter_commands || format('ADD CONSTRAINT %I %s', infinity_check_constraint, condef);
--            END IF;
--        END IF;
--    END;


    /* If we've created any work for ourselves, do it now */
    IF alter_commands <> '{}' THEN
        EXECUTE format('ALTER TABLE %s %s', table_oid, array_to_string(alter_commands, ', '));
    END IF;

    INSERT INTO sql_saga.era (table_oid, era_name, start_after_column_name, stop_on_column_name, range_type, bounds_check_constraint)
    VALUES (table_oid, era_name, start_after_column_name, stop_on_column_name, range_type, bounds_check_constraint);

    -- Code for creation of triggers, when extending the era api
    --        /* Make sure all the excluded columns exist */
    --    FOR excluded_column_name IN
    --        SELECT u.name
    --        FROM unnest(excluded_column_names) AS u (name)
    --        WHERE NOT EXISTS (
    --            SELECT FROM pg_catalog.pg_attribute AS a
    --            WHERE (a.attrelid, a.attname) = (table_class, u.name))
    --    LOOP
    --        RAISE EXCEPTION 'column "%" does not exist', excluded_column_name;
    --    END LOOP;
    --
    --    /* Don't allow system columns to be excluded either */
    --    FOR excluded_column_name IN
    --        SELECT u.name
    --        FROM unnest(excluded_column_names) AS u (name)
    --        JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (table_class, u.name)
    --        WHERE a.attnum < 0
    --    LOOP
    --        RAISE EXCEPTION 'cannot exclude system column "%"', excluded_column_name;
    --    END LOOP;
    --
    --    generated_always_trigger := coalesce(
    --        generated_always_trigger,
    --        sql_saga._make_name(ARRAY[table_name], 'system_time_generated_always'));
    --    EXECUTE format('CREATE TRIGGER %I BEFORE INSERT OR UPDATE ON %s FOR EACH ROW EXECUTE PROCEDURE sql_saga.generated_always_as_row_start_end()', generated_always_trigger, table_class);
    --
    --    write_history_trigger := coalesce(
    --        write_history_trigger,
    --        sql_saga._make_name(ARRAY[table_name], 'system_time_write_history'));
    --    EXECUTE format('CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE PROCEDURE sql_saga.write_history()', write_history_trigger, table_class);
    --
    --    truncate_trigger := coalesce(
    --        truncate_trigger,
    --        sql_saga._make_name(ARRAY[table_name], 'truncate'));
    --    EXECUTE format('CREATE TRIGGER %I AFTER TRUNCATE ON %s FOR EACH STATEMENT EXECUTE PROCEDURE sql_saga.truncate_era()', truncate_trigger, table_class);


    RETURN true;
END;
$function$;

CREATE FUNCTION sql_saga.drop_era(table_oid regclass, era_name name DEFAULT 'valid', drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT true)
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
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    IF era_name IS NULL THEN
        RAISE EXCEPTION 'no era name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga._serialize(table_oid);

    /*
     * Has the table been dropped already?  This could happen if the period is
     * being dropped by the drop_protection event trigger or through a DROP
     * CASCADE.
     */
    is_dropped := NOT EXISTS (SELECT FROM pg_catalog.pg_class AS c WHERE c.oid = table_oid);

    SELECT p.*
    INTO era_row
    FROM sql_saga.era AS p
    WHERE (p.table_oid, p.era_name) = (table_oid, era_name);

    IF NOT FOUND THEN
        RAISE DEBUG 'era % not found for table %', era_name, table_oid;
        RETURN false;
    END IF;

    /* Drop the "for portion" view if it hasn't been dropped already */
    PERFORM sql_saga.drop_api(table_oid, era_name, drop_behavior, cleanup);

    /* If this is a system_time period, get rid of the triggers */
    --    DELETE FROM sql_saga.system_time_periods AS stp
    --    WHERE stp.table_oid = table_oid
    --    RETURNING stp.* INTO system_time_era_row;
    --
    --    IF FOUND AND NOT is_dropped THEN
    --        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', table_oid, system_time_era_row.infinity_check_constraint);
    --        EXECUTE format('DROP TRIGGER %I ON %s', system_time_era_row.generated_always_trigger, table_oid);
    --        EXECUTE format('DROP TRIGGER %I ON %s', system_time_era_row.write_history_trigger, table_oid);
    --        EXECUTE format('DROP TRIGGER %I ON %s', system_time_era_row.truncate_trigger, table_oid);
    --    END IF;

    IF drop_behavior = 'RESTRICT' THEN
        /* Check for UNIQUE or PRIMARY KEYs */
        IF EXISTS (
            SELECT FROM sql_saga.unique_keys AS uk
            WHERE (uk.table_oid, uk.era_name) = (table_oid, era_name))
        THEN
            RAISE EXCEPTION 'era % is part of a UNIQUE or PRIMARY KEY', era_name;
        END IF;

        /* Check for FOREIGN KEYs */
        IF EXISTS (
            SELECT FROM sql_saga.foreign_keys AS fk
            WHERE (fk.table_oid, fk.era_name) = (table_oid, era_name))
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

        /* Delete bounds check constraint if purging */
        IF NOT is_dropped AND cleanup THEN
            EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I',
                table_oid, era_row.bounds_check_constraint);
        END IF;

        /* Remove from catalog */
        DELETE FROM sql_saga.era AS p
        WHERE (p.table_oid, p.era_name) = (table_oid, era_name);

        RETURN true;
    END IF;

    /* We must be in CASCADE mode now */

    PERFORM sql_saga.drop_foreign_key(table_oid, fk.foreign_key_name)
    FROM sql_saga.foreign_keys AS fk
    WHERE (fk.table_oid, fk.era_name) = (table_oid, era_name);

    PERFORM sql_saga.drop_unique_key(table_oid, uk.unique_key_name, drop_behavior, cleanup)
    FROM sql_saga.unique_keys AS uk
    WHERE (uk.table_oid, uk.era_name) = (table_oid, era_name);

    /* Delete bounds check constraint if purging */
    IF NOT is_dropped AND cleanup THEN
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I',
            table_oid, era_row.bounds_check_constraint);
    END IF;

    /* Remove from catalog */
    DELETE FROM sql_saga.era AS p
    WHERE (p.table_oid, p.era_name) = (table_oid, era_name);

    RETURN true;
END;
$function$;


--CREATE FUNCTION sql_saga.set_era_excluded_columns(
--    table_name regclass,
--    excluded_column_names name[])
-- RETURNS void
-- LANGUAGE plpgsql
-- SECURITY DEFINER
--AS
--$function$
--#variable_conflict use_variable
--DECLARE
--    excluded_column_name name;
--BEGIN
--    /* Always serialize operations on our catalogs */
--    PERFORM sql_saga._serialize(table_name);
--
--    /* Make sure all the excluded columns exist */
--    FOR excluded_column_name IN
--        SELECT u.name
--        FROM unnest(excluded_column_names) AS u (name)
--        WHERE NOT EXISTS (
--            SELECT FROM pg_catalog.pg_attribute AS a
--            WHERE (a.attrelid, a.attname) = (table_name, u.name))
--    LOOP
--        RAISE EXCEPTION 'column "%" does not exist', excluded_column_name;
--    END LOOP;
--
--    /* Don't allow system columns to be excluded either */
--    FOR excluded_column_name IN
--        SELECT u.name
--        FROM unnest(excluded_column_names) AS u (name)
--        JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (table_name, u.name)
--        WHERE a.attnum < 0
--    LOOP
--        RAISE EXCEPTION 'cannot exclude system column "%"', excluded_column_name;
--    END LOOP;
--
--    /* Do it. */
--    UPDATE sql_saga.era AS stp SET
--        excluded_column_names = excluded_column_names
--    WHERE stp.table_name = table_name;
--END;
--$function$;


CREATE FUNCTION sql_saga.truncate_era()
 RETURNS trigger
 LANGUAGE plpgsql
 STRICT
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    audit_table_name name;
BEGIN
    SELECT sv.audit_table_name
    INTO audit_table_name
    FROM sql_saga.era AS sv
    WHERE sv.table_name = TG_RELID;

    IF FOUND THEN
        EXECUTE format('TRUNCATE %s', audit_table_name);
    END IF;

    RETURN NULL;
END;
$function$;

CREATE FUNCTION sql_saga.add_api(table_oid regclass DEFAULT NULL, era_name name DEFAULT 'valid')
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    r record;
    view_name name;
    trigger_name name;
BEGIN
    /*
     * If table_oid and era_name are specified, then just add the views for that.
     *
     * If no period is specified, add the views for all periods of the table.
     *
     * If no table is specified, add the views everywhere.
     *
     * If no table is specified but a period is, that doesn't make any sense.
     */
    IF table_oid IS NULL AND era_name IS NOT NULL THEN
        RAISE EXCEPTION 'cannot specify era name without table name';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga._serialize(table_oid);

    /*
     * We require the table to have a primary key, so check to see if there is
     * one.  This requires a lock on the table so no one removes it after we
     * check and before we commit.
     */
    EXECUTE format('LOCK TABLE %s IN ACCESS SHARE MODE', table_oid);

    /* Now check for the primary key */
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_constraint AS c
        WHERE (c.conrelid, c.contype) = (table_oid, 'p'))
    THEN
        RAISE EXCEPTION 'table "%" must have a primary key', table_oid;
    END IF;

    FOR r IN
        SELECT n.nspname AS schema_name, c.relname AS table_name, c.relowner AS table_owner, p.era_name
        FROM sql_saga.era AS p
        JOIN pg_catalog.pg_class AS c ON c.oid = p.table_oid
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
        WHERE (table_oid IS NULL OR p.table_oid = table_oid)
          AND (era_name IS NULL OR p.era_name = era_name)
          AND p.era_name <> 'system_time'
          AND NOT EXISTS (
                SELECT FROM sql_saga.api_view AS _fpv
                WHERE (_fpv.table_oid, _fpv.era_name) = (p.table_oid, p.era_name))
    LOOP
        view_name := sql_saga._make_api_view_name(r.table_name, r.era_name);
        trigger_name := 'for_portion_of_' || r.era_name;
        EXECUTE format('CREATE VIEW %1$I.%2$I AS TABLE %1$I.%3$I', r.schema_name, view_name, r.table_name);
        EXECUTE format('ALTER VIEW %1$I.%2$I OWNER TO %s', r.schema_name, view_name, r.table_owner::regrole);
        EXECUTE format('CREATE TRIGGER %I INSTEAD OF UPDATE ON %I.%I FOR EACH ROW EXECUTE PROCEDURE sql_saga.update_portion_of()',
            trigger_name, r.schema_name, view_name);
        INSERT INTO sql_saga.api_view (table_oid, era_name, view_oid, trigger_name)
            VALUES (format('%I.%I', r.schema_name, r.table_name), r.era_name, format('%I.%I', r.schema_name, view_name), trigger_name);
    END LOOP;

    RETURN true;
END;
$function$;

CREATE FUNCTION sql_saga.drop_api(table_oid regclass, era_name name, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT false)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    view_oid regclass;
    trigger_name name;
BEGIN
    /*
     * If table_oid and era_name are specified, then just drop the views for that.
     *
     * If no period is specified, drop the views for all periods of the table.
     *
     * If no table is specified, drop the views everywhere.
     *
     * If no table is specified but a period is, that doesn't make any sense.
     */
    IF table_oid IS NULL AND era_name IS NOT NULL THEN
        RAISE EXCEPTION 'cannot specify era name without table name';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga._serialize(table_oid);

    FOR view_oid, trigger_name IN
        DELETE FROM sql_saga.api_view AS fp
        WHERE (table_oid IS NULL OR fp.table_oid = table_oid)
          AND (era_name IS NULL OR fp.era_name = era_name)
        RETURNING fp.view_oid, fp.trigger_name
    LOOP
        EXECUTE format('DROP TRIGGER %I on %s', trigger_name, view_oid);
        EXECUTE format('DROP VIEW %s %s', view_oid, drop_behavior);
    END LOOP;

    RETURN true;
END;
$function$;

CREATE FUNCTION sql_saga.update_portion_of()
 RETURNS trigger
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    info record;
    test boolean;
    generated_columns_sql text;
    generated_columns text[];

    jnew jsonb;
    fromval jsonb;
    toval jsonb;

    jold jsonb;
    bstartval jsonb;
    bendval jsonb;

    pre_row jsonb;
    new_row jsonb;
    post_row jsonb;
    pre_assigned boolean;
    post_assigned boolean;

    SERVER_VERSION CONSTANT integer := current_setting('server_version_num')::integer;

    TEST_SQL CONSTANT text :=
        'VALUES (CAST(%2$L AS %1$s) < CAST(%3$L AS %1$s) AND '
        '        CAST(%3$L AS %1$s) < CAST(%4$L AS %1$s))';

    GENERATED_COLUMNS_SQL_PRE_10 CONSTANT text :=
        'SELECT array_agg(a.attname) '
        'FROM pg_catalog.pg_attribute AS a '
        'WHERE a.attrelid = $1 '
        '  AND a.attnum > 0 '
        '  AND NOT a.attisdropped '
        '  AND (pg_catalog.pg_get_serial_sequence(a.attrelid::regclass::text, a.attname) IS NOT NULL '
        '    OR EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c '
        '               WHERE _c.conrelid = a.attrelid '
        '                 AND _c.contype = ''p'' '
        '                 AND _c.conkey @> ARRAY[a.attnum]) '
        '    OR EXISTS (SELECT FROM sql_saga.era AS _p '
        '               WHERE (_p.table_oid, _p.era_name) = (a.attrelid, ''system_time'') '
        '                 AND a.attname IN (_p.start_after_column_name, _p.stop_on_column_name)))';

    GENERATED_COLUMNS_SQL_PRE_12 CONSTANT text :=
        'SELECT array_agg(a.attname) '
        'FROM pg_catalog.pg_attribute AS a '
        'WHERE a.attrelid = $1 '
        '  AND a.attnum > 0 '
        '  AND NOT a.attisdropped '
        '  AND (pg_catalog.pg_get_serial_sequence(a.attrelid::regclass::text, a.attname) IS NOT NULL '
        '    OR a.attidentity <> '''' '
        '    OR EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c '
        '               WHERE _c.conrelid = a.attrelid '
        '                 AND _c.contype = ''p'' '
        '                 AND _c.conkey @> ARRAY[a.attnum]) '
        '    OR EXISTS (SELECT FROM sql_saga.era AS _p '
        '               WHERE (_p.table_oid, _p.era_name) = (a.attrelid, ''system_time'') '
        '                 AND a.attname IN (_p.start_after_column_name, _p.stop_on_column_name)))';

    GENERATED_COLUMNS_SQL_CURRENT CONSTANT text :=
        'SELECT array_agg(a.attname) '
        'FROM pg_catalog.pg_attribute AS a '
        'WHERE a.attrelid = $1 '
        '  AND a.attnum > 0 '
        '  AND NOT a.attisdropped '
        '  AND (pg_catalog.pg_get_serial_sequence(a.attrelid::regclass::text, a.attname) IS NOT NULL '
        '    OR a.attidentity <> '''' '
        '    OR a.attgenerated <> '''' '
        '    OR EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c '
        '               WHERE _c.conrelid = a.attrelid '
        '                 AND _c.contype = ''p'' '
        '                 AND _c.conkey @> ARRAY[a.attnum]) '
        '    OR EXISTS (SELECT FROM sql_saga.era AS _p '
        '               WHERE (_p.table_oid, _p.era_name) = (a.attrelid, ''system_time'') '
        '                 AND a.attname IN (_p.start_after_column_name, _p.stop_on_column_name)))';

BEGIN
    /*
     * REFERENCES:
     *     SQL:2016 15.13 GR 10
     */

    /* Get the table information from this view */
    SELECT e.table_oid, e.era_name,
           e.start_after_column_name, e.stop_on_column_name,
           format_type(a.atttypid, a.atttypmod) AS datatype
    INTO info
    FROM sql_saga.api_view AS fpv
    JOIN sql_saga.era AS e ON (e.table_oid, e.era_name) = (fpv.table_oid, fpv.era_name)
    JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (e.table_oid, e.start_after_column_name)
    WHERE fpv.view_oid = TG_RELID;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'table and era information not found for view "%"', TG_RELID::regclass;
    END IF;

    jnew := to_jsonb(NEW);
    fromval := jnew->info.start_after_column_name;
    toval := jnew->info.stop_on_column_name;

    jold := to_jsonb(OLD);
    bstartval := jold->info.start_after_column_name;
    bendval := jold->info.stop_on_column_name;

    pre_row := jold;
    new_row := jnew;
    post_row := jold;

    /* Reset the period columns */
    new_row := jsonb_set(new_row, ARRAY[info.start_after_column_name], bstartval);
    new_row := jsonb_set(new_row, ARRAY[info.stop_on_column_name], bendval);

    /* If the period is the only thing changed, do nothing */
    IF new_row = jold THEN
        RETURN NULL;
    END IF;

    pre_assigned := false;
    EXECUTE format(TEST_SQL, info.datatype, bstartval, fromval, bendval) INTO test;
    IF test THEN
        pre_assigned := true;
        pre_row := jsonb_set(pre_row, ARRAY[info.stop_on_column_name], fromval);
        new_row := jsonb_set(new_row, ARRAY[info.start_after_column_name], fromval);
    END IF;

    post_assigned := false;
    EXECUTE format(TEST_SQL, info.datatype, bstartval, toval, bendval) INTO test;
    IF test THEN
        post_assigned := true;
        new_row := jsonb_set(new_row, ARRAY[info.stop_on_column_name], toval::jsonb);
        post_row := jsonb_set(post_row, ARRAY[info.start_after_column_name], toval::jsonb);
    END IF;

    IF pre_assigned OR post_assigned THEN
        /* Don't validate foreign keys until all this is done */
        SET CONSTRAINTS ALL DEFERRED;

        /*
         * Find and remove all generated columns from pre_row and post_row.
         * SQL:2016 15.13 GR 10)b)i)
         *
         * We also remove columns that own a sequence as those are a form of
         * generated column.  We do not, however, remove columns that default
         * to nextval() without owning the underlying sequence.
         *
         * Columns belonging to a SYSTEM_TIME period are also removed.
         *
         * In addition to what the standard calls for, we also remove any
         * columns belonging to primary keys.
         */
        IF SERVER_VERSION < 100000 THEN
            generated_columns_sql := GENERATED_COLUMNS_SQL_PRE_10;
        ELSIF SERVER_VERSION < 120000 THEN
            generated_columns_sql := GENERATED_COLUMNS_SQL_PRE_12;
        ELSE
            generated_columns_sql := GENERATED_COLUMNS_SQL_CURRENT;
        END IF;

        EXECUTE generated_columns_sql
        INTO generated_columns
        USING info.table_oid;

        /* There may not be any generated columns. */
        IF generated_columns IS NOT NULL THEN
            IF SERVER_VERSION < 100000 THEN
                SELECT jsonb_object_agg(e.key, e.value)
                INTO pre_row
                FROM jsonb_each(pre_row) AS e (key, value)
                WHERE e.key <> ALL (generated_columns);

                SELECT jsonb_object_agg(e.key, e.value)
                INTO post_row
                FROM jsonb_each(post_row) AS e (key, value)
                WHERE e.key <> ALL (generated_columns);
            ELSE
                pre_row := pre_row - generated_columns;
                post_row := post_row - generated_columns;
            END IF;
        END IF;
    END IF;

    IF pre_assigned THEN
        EXECUTE format('INSERT INTO %s (%s) VALUES (%s)',
            info.table_oid,
            (SELECT string_agg(quote_ident(key), ', ' ORDER BY key) FROM jsonb_each_text(pre_row)),
            (SELECT string_agg(quote_nullable(value), ', ' ORDER BY key) FROM jsonb_each_text(pre_row)));
    END IF;

    EXECUTE format('UPDATE %s SET %s WHERE %s AND %I > %L AND %I < %L',
                   info.table_oid,
                   (SELECT string_agg(format('%I = %L', j.key, j.value), ', ')
                    FROM (SELECT key, value FROM jsonb_each_text(new_row)
                          EXCEPT ALL
                          SELECT key, value FROM jsonb_each_text(jold)
                         ) AS j
                   ),
                   (SELECT string_agg(format('%I = %L', key, value), ' AND ')
                    FROM pg_catalog.jsonb_each_text(jold) AS j
                    JOIN pg_catalog.pg_attribute AS a ON a.attname = j.key
                    JOIN pg_catalog.pg_constraint AS c ON c.conkey @> ARRAY[a.attnum]
                    WHERE a.attrelid = info.table_oid
                      AND c.conrelid = info.table_oid
                   ),
                   info.stop_on_column_name,
                   fromval,
                   info.start_after_column_name,
                   toval
                  );

    IF post_assigned THEN
        EXECUTE format('INSERT INTO %s (%s) VALUES (%s)',
            info.table_oid,
            (SELECT string_agg(quote_ident(key), ', ' ORDER BY key) FROM jsonb_each_text(post_row)),
            (SELECT string_agg(quote_nullable(value), ', ' ORDER BY key) FROM jsonb_each_text(post_row)));
    END IF;

    RETURN NEW;
END;
$function$;


CREATE FUNCTION sql_saga.add_unique_key(
        table_oid regclass,
        column_names name[],
        era_name name DEFAULT 'valid',
        unique_key_name name DEFAULT NULL,
        unique_constraint name DEFAULT NULL,
        exclude_constraint name DEFAULT NULL)
 RETURNS name
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    era_row sql_saga.era;
    column_attnums smallint[];
    era_attnums smallint[];
    idx integer;
    constraint_record record;
    pass integer;
    sql text;
    alter_cmds text[];
    unique_index regclass;
    exclude_index regclass;
    unique_sql text;
    exclude_sql text;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga._serialize(table_oid);

    SELECT p.*
    INTO era_row
    FROM sql_saga.era AS p
    WHERE (p.table_oid, p.era_name) = (table_oid, era_name);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'era "%" does not exist', era_name;
    END IF;

    /* For convenience, put the period's attnums in an array */
    era_attnums := ARRAY[
        (SELECT a.attnum FROM pg_catalog.pg_attribute AS a WHERE (a.attrelid, a.attname) = (era_row.table_oid, era_row.start_after_column_name)),
        (SELECT a.attnum FROM pg_catalog.pg_attribute AS a WHERE (a.attrelid, a.attname) = (era_row.table_oid, era_row.stop_on_column_name))
    ];

    /* Get attnums from column names */
    SELECT array_agg(a.attnum ORDER BY n.ordinality)
    INTO column_attnums
    FROM unnest(column_names) WITH ORDINALITY AS n (name, ordinality)
    LEFT JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (table_oid, n.name);

    /* System columns are not allowed */
    IF 0 > ANY (column_attnums) THEN
        RAISE EXCEPTION 'index creation on system columns is not supported';
    END IF;

    /* Report if any columns weren't found */
    idx := array_position(column_attnums, NULL);
    IF idx IS NOT NULL THEN
        RAISE EXCEPTION 'column "%" does not exist', column_names[idx];
    END IF;

    /* Make sure the period columns aren't also in the normal columns */
    IF era_row.start_after_column_name = ANY (column_names) THEN
        RAISE EXCEPTION 'column "%" specified twice', era_row.start_after_column_name;
    END IF;
    IF era_row.stop_on_column_name = ANY (column_names) THEN
        RAISE EXCEPTION 'column "%" specified twice', era_row.stop_on_column_name;
    END IF;

    /*
     * Columns belonging to a SYSTEM_TIME period are not allowed in a UNIQUE
     * key. SQL:2016 11.7 SR 5)b)
     */
    IF EXISTS (
        SELECT FROM sql_saga.era AS e
        WHERE (e.table_oid, e.era_name) = (era_row.table_oid, 'system_time')
          AND ARRAY[e.start_after_column_name, e.stop_on_column_name] && column_names)
    THEN
        RAISE EXCEPTION 'columns in era for SYSTEM_TIME are not allowed in UNIQUE keys';
    END IF;

    /* If we were given a unique constraint to use, look it up and make sure it matches */
    SELECT format('UNIQUE (%s) DEFERRABLE', string_agg(quote_ident(u.column_name), ', ' ORDER BY u.ordinality))
    INTO unique_sql
    FROM unnest(column_names || era_row.start_after_column_name || era_row.stop_on_column_name) WITH ORDINALITY AS u (column_name, ordinality);

    IF unique_constraint IS NOT NULL THEN
        SELECT c.oid, c.contype, c.condeferrable, c.condeferred, c.conkey
        INTO constraint_record
        FROM pg_catalog.pg_constraint AS c
        WHERE (c.conrelid, c.conname) = (table_oid, unique_constraint);

        IF NOT FOUND THEN
            RAISE EXCEPTION 'constraint "%" does not exist', unique_constraint;
        END IF;

        IF constraint_record.contype NOT IN ('p', 'u') THEN
            RAISE EXCEPTION 'constraint "%" is not a PRIMARY KEY or UNIQUE KEY', unique_constraint;
        END IF;

        IF NOT constraint_record.condeferrable THEN
            /* For restore purposes, constraints may be deferred,
             * but everything must be valid at the end fo the transaction
             */
            RAISE EXCEPTION 'constraint "%" must be DEFERRABLE', unique_constraint;
        END IF;

        IF constraint_record.condeferred THEN
            /* By default constraints are NOT deferred,
             * and the user receives a timely validation error.
             */
            RAISE EXCEPTION 'constraint "%" must be INITIALLY IMMEDIATE', unique_constraint;
        END IF;

        IF NOT constraint_record.conkey = column_attnums || era_attnums THEN
            RAISE EXCEPTION 'constraint "%" does not match', unique_constraint;
        END IF;

        /* Looks good, let's use it. */
    END IF;

    /*
     * If we were given an exclude constraint to use, look it up and make sure
     * it matches.  We do that by generating the text that we expect
     * pg_get_constraintdef() to output and compare against that instead of
     * trying to deal with the internally stored components like we did for the
     * UNIQUE constraint.
     *
     * We will use this same text to create the constraint if it doesn't exist.
     */
    DECLARE
        withs text[];
    BEGIN
        SELECT array_agg(format('%I WITH =', column_name) ORDER BY n.ordinality)
        INTO withs
        FROM unnest(column_names) WITH ORDINALITY AS n (column_name, ordinality);

        withs := withs || format('%I(%I, %I, ''(]''::text) WITH &&',
            era_row.range_type, era_row.start_after_column_name, era_row.stop_on_column_name);

        exclude_sql := format('EXCLUDE USING gist (%s) DEFERRABLE', array_to_string(withs, ', '));
    END;

    IF exclude_constraint IS NOT NULL THEN
        SELECT c.oid, c.contype, c.condeferrable, c.condeferred, pg_catalog.pg_get_constraintdef(c.oid) AS definition
        INTO constraint_record
        FROM pg_catalog.pg_constraint AS c
        WHERE (c.conrelid, c.conname) = (table_oid, exclude_constraint);

        IF NOT FOUND THEN
            RAISE EXCEPTION 'constraint "%" does not exist', exclude_constraint;
        END IF;

        IF constraint_record.contype <> 'x' THEN
            RAISE EXCEPTION 'constraint "%" is not an EXCLUDE constraint', exclude_constraint;
        END IF;

        IF NOT constraint_record.condeferrable THEN
            /* For restore purposes, constraints may be deferred,
             * but everything must be valid at the end fo the transaction
             */
            RAISE EXCEPTION 'constraint "%" must be DEFERRABLE', exclude_constraint;
        END IF;

        IF constraint_record.condeferred THEN
            /* By default constraints are NOT deferred,
             * and the user receives a timely validation error.
             */
            RAISE EXCEPTION 'constraint "%" must be INITIALLY IMMEDIATE', exclude_constraint;
        END IF;

        IF constraint_record.definition <> exclude_sql THEN
            RAISE EXCEPTION 'constraint "%" does not match', exclude_constraint;
        END IF;

        /* Looks good, let's use it. */
    END IF;

    /*
     * Generate a name for the unique constraint.  We don't have to worry about
     * concurrency here because all period ddl commands lock the periods table.
     */
    IF unique_key_name IS NULL THEN
        unique_key_name := sql_saga._make_name(
            ARRAY[(SELECT c.relname FROM pg_catalog.pg_class AS c WHERE c.oid = table_oid)]
                || column_names
                || ARRAY[era_name]);
    END IF;
    pass := 0;
    WHILE EXISTS (
       SELECT FROM sql_saga.unique_keys AS uk
       WHERE uk.unique_key_name = unique_key_name || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END)
    LOOP
       pass := pass + 1;
    END LOOP;
    unique_key_name := unique_key_name || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END;

    /* Time to make the underlying constraints */
    alter_cmds := '{}';
    IF unique_constraint IS NULL THEN
        alter_cmds := alter_cmds || ('ADD ' || unique_sql);
    END IF;

    IF exclude_constraint IS NULL THEN
        alter_cmds := alter_cmds || ('ADD ' || exclude_sql);
    END IF;

    IF alter_cmds <> '{}' THEN
        SELECT format('ALTER TABLE %I.%I %s', n.nspname, c.relname, array_to_string(alter_cmds, ', '))
        INTO sql
        FROM pg_catalog.pg_class AS c
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
        WHERE c.oid = table_oid;

        EXECUTE sql;
    END IF;

    /* If we don't already have a unique_constraint, it must be the one with the highest oid */
    IF unique_constraint IS NULL THEN
        SELECT c.conname, c.conindid
        INTO unique_constraint, unique_index
        FROM pg_catalog.pg_constraint AS c
        WHERE (c.conrelid, c.contype) = (table_oid, 'u')
        ORDER BY oid DESC
        LIMIT 1;
    END IF;

    /* If we don't already have an exclude_constraint, it must be the one with the highest oid */
    IF exclude_constraint IS NULL THEN
        SELECT c.conname, c.conindid
        INTO exclude_constraint, exclude_index
        FROM pg_catalog.pg_constraint AS c
        WHERE (c.conrelid, c.contype) = (table_oid, 'x')
        ORDER BY oid DESC
        LIMIT 1;
    END IF;

    INSERT INTO sql_saga.unique_keys (unique_key_name, table_oid, column_names, era_name, unique_constraint, exclude_constraint)
    VALUES (unique_key_name, table_oid, column_names, era_name, unique_constraint, exclude_constraint);

    RETURN unique_key_name;
END;
$function$;

CREATE FUNCTION sql_saga.drop_unique_key(
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
    foreign_key_row sql_saga.foreign_keys;
    unique_key_row sql_saga.unique_keys;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga._serialize(table_oid);

    FOR unique_key_row IN
        SELECT uk.*
        FROM sql_saga.unique_keys AS uk
        WHERE uk.table_oid = table_oid
          AND (uk.unique_key_name = key_name OR key_name IS NULL)
    LOOP
        /* Cascade to foreign keys, if desired */
        FOR foreign_key_row IN
            SELECT fk.foreign_key_name
            FROM sql_saga.foreign_keys AS fk
            WHERE fk.unique_key_name = unique_key_row.unique_key_name
        LOOP
            IF drop_behavior = 'RESTRICT' THEN
                RAISE EXCEPTION 'cannot drop unique key "%" because foreign key "%" on table "%" depends on it',
                    unique_key_row.unique_key_name, foreign_key_row.foreign_key_name, foreign_key_row.table_oid;
            END IF;

            PERFORM sql_saga.drop_foreign_key(NULL, foreign_key_row.foreign_key_name);
        END LOOP;

        DELETE FROM sql_saga.unique_keys AS uk
        WHERE uk.unique_key_name = unique_key_row.unique_key_name;

        /* If purging, drop the underlying constraints unless the table has been dropped */
        IF cleanup AND EXISTS (
            SELECT FROM pg_catalog.pg_class AS c
            WHERE c.oid = unique_key_row.table_oid)
        THEN
            EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I, DROP CONSTRAINT %I',
                unique_key_row.table_oid, unique_key_row.unique_constraint, unique_key_row.exclude_constraint);
        END IF;
    END LOOP;

END;
$function$;

CREATE FUNCTION sql_saga.drop_unique_key(
        table_oid regclass,
        column_names name[],
        era_name name,
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
    key_name_found name;
BEGIN
    SELECT uk.unique_key_name INTO key_name_found
    FROM sql_saga.unique_keys AS uk
    WHERE uk.table_oid = table_oid
      AND uk.column_names = column_names
      AND uk.era_name = era_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'unique key on table % for columns % with era % does not exist', table_oid, column_names, era_name;
    END IF;

    PERFORM sql_saga.drop_unique_key(table_oid, key_name_found, drop_behavior, cleanup);
END;
$function$;

CREATE FUNCTION sql_saga.uk_update_check()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
#variable_conflict use_variable
DECLARE
    /* Use jsonb to look up values by parameterized names */
    jold jsonb := to_jsonb(OLD);

    foreign_key_name name              := TG_ARGV[0];
    fk_table_oid regclass              := TG_ARGV[1];
    fk_schema_name text                := TG_ARGV[2];
    fk_table_name text                 := TG_ARGV[3];
    fk_column_names text[]             := TG_ARGV[4];
    fk_era_name text                   := TG_ARGV[5];
    fk_start_after_column_name text          := TG_ARGV[6];
    fk_stop_on_column_name text            := TG_ARGV[7];
    uk_table_oid regclass              := TG_ARGV[8];
    uk_schema_name text                := TG_ARGV[9];
    uk_table_name text                 := TG_ARGV[10];
    uk_column_names text[]             := TG_ARGV[11];
    uk_era_name text                   := TG_ARGV[12];
    uk_start_after_column_name text          := TG_ARGV[13];
    uk_stop_on_column_name text            := TG_ARGV[14];
    match_type sql_saga.fk_match_types := TG_ARGV[15];
    update_action sql_saga.fk_actions  := TG_ARGV[16];
    delete_action sql_saga.fk_actions  := TG_ARGV[17];
BEGIN
    /*
     * This function is called when a table referenced by foreign keys with
     * periods is updated.  It checks to verify that the referenced table still
     * contains the proper data to satisfy the foreign key constraint.
     *
     * The first argument is the name of the foreign key in our custom
     * catalogs.
     *
     * If this is a NO ACTION constraint, we need to check if there is a new
     * row that still satisfies the constraint, in which case there is no
     * error.
     */

    /* Check the constraint */
    PERFORM sql_saga.validate_foreign_key_old_row(jold, true /* is_update */, foreign_key_name, fk_table_oid, fk_schema_name, fk_table_name, fk_column_names, fk_era_name, fk_start_after_column_name, fk_stop_on_column_name, uk_table_oid, uk_schema_name, uk_table_name, uk_column_names, uk_era_name, uk_start_after_column_name, uk_stop_on_column_name, match_type, update_action, delete_action);

    RETURN NULL;
END;
$function$;

CREATE FUNCTION sql_saga.drop_foreign_key(
    table_oid regclass,
    column_names name[],
    era_name name,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT'
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    key_name_found name;
BEGIN
    SELECT fk.foreign_key_name INTO key_name_found
    FROM sql_saga.foreign_keys AS fk
    WHERE fk.table_oid = table_oid
      AND fk.column_names = column_names
      AND fk.era_name = era_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'foreign key on table % for columns % with era % does not exist', table_oid, column_names, era_name;
    END IF;

    PERFORM sql_saga.drop_foreign_key(table_oid, key_name_found);
END;
$function$;

CREATE FUNCTION sql_saga.uk_delete_check()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
#variable_conflict use_variable
DECLARE
    /* Use jsonb to look up values by parameterized names */
    jold jsonb := to_jsonb(OLD);

    foreign_key_name name              := TG_ARGV[0];
    fk_table_oid regclass              := TG_ARGV[1];
    fk_schema_name text                := TG_ARGV[2];
    fk_table_name text                 := TG_ARGV[3];
    fk_column_names text[]             := TG_ARGV[4];
    fk_era_name text                   := TG_ARGV[5];
    fk_start_after_column_name text          := TG_ARGV[6];
    fk_stop_on_column_name text            := TG_ARGV[7];
    uk_table_oid regclass              := TG_ARGV[8];
    uk_schema_name text                := TG_ARGV[9];
    uk_table_name text                 := TG_ARGV[10];
    uk_column_names text[]             := TG_ARGV[11];
    uk_era_name text                   := TG_ARGV[12];
    uk_start_after_column_name text          := TG_ARGV[13];
    uk_stop_on_column_name text            := TG_ARGV[14];
    match_type sql_saga.fk_match_types := TG_ARGV[15];
    update_action sql_saga.fk_actions  := TG_ARGV[16];
    delete_action sql_saga.fk_actions  := TG_ARGV[17];
BEGIN
    /*
     * This function is called when a table referenced by foreign keys with
     * periods is deleted from.  It checks to verify that the referenced table
     * still contains the proper data to satisfy the foreign key constraint.
     *
     * The first argument is the name of the foreign key in our custom
     * catalogs.
     *
     * The only difference between NO ACTION and RESTRICT is when the check is
     * done, so this function is used for both.
     */

    /* Check the constraint */
    PERFORM sql_saga.validate_foreign_key_old_row(jold, false /* is_update */, foreign_key_name, fk_table_oid, fk_schema_name, fk_table_name, fk_column_names, fk_era_name, fk_start_after_column_name, fk_stop_on_column_name, uk_table_oid, uk_schema_name, uk_table_name, uk_column_names, uk_era_name, uk_start_after_column_name, uk_stop_on_column_name, match_type, update_action, delete_action);

    RETURN NULL;
END;
$function$;


CREATE FUNCTION sql_saga.add_foreign_key(
        fk_table_oid regclass,
        fk_column_names name[],
        -- TODO: Simplify API with the following
        -- foreign_table_name regclass,
        -- foreign_fk_column_names name[],
        fk_era_name name,
        unique_key_name name,
        match_type sql_saga.fk_match_types DEFAULT 'SIMPLE',
        update_action sql_saga.fk_actions DEFAULT 'NO ACTION',
        delete_action sql_saga.fk_actions DEFAULT 'NO ACTION',
        foreign_key_name name DEFAULT NULL,
        fk_insert_trigger name DEFAULT NULL,
        fk_update_trigger name DEFAULT NULL,
        uk_update_trigger name DEFAULT NULL,
        uk_delete_trigger name DEFAULT NULL)
 RETURNS name
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    fk_era_row sql_saga.era;
    uk_era_row sql_saga.era;
    uk_row sql_saga.unique_keys;
    fk_schema_name text;
    fk_table_name text;
    uk_schema_name text;
    uk_table_name text;
    column_attnums smallint[];
    idx integer;
    pass integer;
    upd_action text DEFAULT '';
    del_action text DEFAULT '';
    foreign_columns_with_era_columns text;
    unique_columns_with_era_columns text;
BEGIN
    IF fk_table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga._serialize(fk_table_oid);

    SELECT n.nspname, c.relname INTO fk_schema_name, fk_table_name
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = fk_table_oid;

    /* Get the period involved */
    SELECT e.*
    INTO fk_era_row
    FROM sql_saga.era AS e
    WHERE (e.table_oid, e.era_name) = (fk_table_oid, fk_era_name);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'era "%" does not exist', fk_era_name;
    END IF;

    /* Get column attnums from column names */
    SELECT array_agg(a.attnum ORDER BY n.ordinality)
    INTO column_attnums
    FROM unnest(fk_column_names) WITH ORDINALITY AS n (name, ordinality)
    LEFT JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (fk_table_oid, n.name);

    /* System columns are not allowed */
    IF 0 > ANY (column_attnums) THEN
        RAISE EXCEPTION 'index creation on system columns is not supported';
    END IF;

    /* Report if any columns weren't found */
    idx := array_position(column_attnums, NULL);
    IF idx IS NOT NULL THEN
        RAISE EXCEPTION 'column "%" does not exist', fk_column_names[idx];
    END IF;

    /* Make sure the period columns aren't also in the normal columns */
    IF fk_era_row.start_after_column_name = ANY (fk_column_names) THEN
        RAISE EXCEPTION 'column "%" specified twice', fk_era_row.start_after_column_name;
    END IF;
    IF fk_era_row.stop_on_column_name = ANY (fk_column_names) THEN
        RAISE EXCEPTION 'column "%" specified twice', fk_era_row.stop_on_column_name;
    END IF;

    /* Get the unique key we're linking to */
    SELECT uk.*
    INTO uk_row
    FROM sql_saga.unique_keys AS uk
    WHERE uk.unique_key_name = unique_key_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'unique key "%" does not exist', unique_key_name;
    END IF;

    /* Get the unique key's eroa */
    SELECT e.*
    INTO uk_era_row
    FROM sql_saga.era AS e
    WHERE (e.table_oid, e.era_name) = (uk_row.table_oid, uk_row.era_name);

    IF fk_era_row.range_type <> uk_era_row.range_type THEN
        RAISE EXCEPTION 'era types "%" and "%" are incompatible',
            fk_era_row.era_name, uk_era_row.era_name;
    END IF;

    SELECT n.nspname, c.relname INTO uk_schema_name, uk_table_name
    FROM pg_catalog.pg_class c
    JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
    WHERE c.oid = uk_row.table_oid;

    /* Check that all the columns match */
    IF EXISTS (
        SELECT FROM unnest(fk_column_names, uk_row.column_names) AS u (fk_attname, uk_attname)
        JOIN pg_catalog.pg_attribute AS fa ON (fa.attrelid, fa.attname) = (fk_table_oid, u.fk_attname)
        JOIN pg_catalog.pg_attribute AS ua ON (ua.attrelid, ua.attname) = (uk_row.table_oid, u.uk_attname)
        WHERE (fa.atttypid, fa.atttypmod, fa.attcollation) <> (ua.atttypid, ua.atttypmod, ua.attcollation))
    THEN
        RAISE EXCEPTION 'column types do not match';
    END IF;

    /* The range types must match, too */
    IF fk_era_row.range_type <> uk_era_row.range_type THEN
        RAISE EXCEPTION 'period types do not match';
    END IF;

    /*
     * Generate a name for the foreign constraint.  We don't have to worry about
     * concurrency here because all period ddl commands lock the periods table.
     */
    IF foreign_key_name IS NULL THEN
        foreign_key_name := sql_saga._make_name(
            ARRAY[(SELECT c.relname FROM pg_catalog.pg_class AS c WHERE c.oid = fk_table_oid)]
               || fk_column_names
               || ARRAY[fk_era_name]);
    END IF;
    pass := 0;
    WHILE EXISTS (
       SELECT FROM sql_saga.foreign_keys AS fk
       WHERE fk.foreign_key_name = foreign_key_name || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END)
    LOOP
       pass := pass + 1;
    END LOOP;
    foreign_key_name := foreign_key_name || CASE WHEN pass > 0 THEN '_' || pass::text ELSE '' END;

    -- TODO: Consider how update_action should be handled, it seems
    -- clear it should affect the timing of the trigger.
    /* See if we're deferring the constraints or not */
    -- IF update_action = 'NO ACTION' THEN
    --     upd_action := ' DEFERRABLE INITIALLY DEFERRED';
    -- END IF;
    -- IF delete_action = 'NO ACTION' THEN
    --     del_action := ' DEFERRABLE INITIALLY DEFERRED';
    -- END IF;

    /* Get the columns that require checking the constraint */
    SELECT string_agg(quote_ident(u.column_name), ', ' ORDER BY u.ordinality)
    INTO foreign_columns_with_era_columns
    FROM unnest(fk_column_names || fk_era_row.start_after_column_name || fk_era_row.stop_on_column_name) WITH ORDINALITY AS u (column_name, ordinality);

    SELECT string_agg(quote_ident(u.column_name), ', ' ORDER BY u.ordinality)
    INTO unique_columns_with_era_columns
    FROM unnest(uk_row.column_names || uk_era_row.start_after_column_name || uk_era_row.stop_on_column_name) WITH ORDINALITY AS u (column_name, ordinality);

    /* Add all the known variables for the triggers to avoid lookups when executing the triggers. */
    DECLARE
        fk_start_after_column_name text := fk_era_row.start_after_column_name;
        fk_stop_on_column_name text := fk_era_row.stop_on_column_name;

        fk_column_names_arr_str text := format('{%s}',fk_column_names);
        uk_column_names_arr_str text := format('{%s}',uk_row.column_names);

        uk_era_name text := uk_era_row.era_name;
        uk_table_oid regclass := uk_row.table_oid;

        uk_start_after_column_name text := uk_era_row.start_after_column_name;
        uk_stop_on_column_name text := uk_era_row.stop_on_column_name;
    BEGIN
        /* Time to make the underlying triggers */
        fk_insert_trigger := coalesce(fk_insert_trigger, sql_saga._make_name(ARRAY[foreign_key_name], 'fk_insert'));
        EXECUTE format($$
            CREATE CONSTRAINT TRIGGER %19$I AFTER INSERT ON %3$I.%4$I FROM %10$I.%11$I DEFERRABLE FOR EACH ROW EXECUTE PROCEDURE
            sql_saga.fk_insert_check_c(%1$L,%2$L,%3$L,%4$L,%5$L,%6$L,%7$L,%8$L,%9$L,%10$L,%11$L,%12$L,%13$L,%14$L,%15$L,%16$L,%17$L,%18$L);
            $$
            -- Parameters for the function call in the template
            , /* %1$  */ foreign_key_name
            , /* %2$  */ fk_table_oid
            , /* %3$  */ fk_schema_name
            , /* %4$  */ fk_table_name
            , /* %5$  */ fk_column_names_arr_str
            , /* %6$  */ fk_era_name
            , /* %7$  */ fk_start_after_column_name
            , /* %8$  */ fk_stop_on_column_name
            , /* %9$  */ uk_table_oid
            , /* %10$ */ uk_schema_name
            , /* %11$ */ uk_table_name
            , /* %12$ */ uk_column_names_arr_str
            , /* %13$ */ uk_era_name
            , /* %14$ */ uk_start_after_column_name
            , /* %15$ */ uk_stop_on_column_name
            , /* %16$ */ match_type
            , /* %17$ */ update_action
            , /* %18$ */ delete_action
            -- Other parameters
            , /* %19$  */ fk_insert_trigger
        );

        fk_update_trigger := coalesce(fk_update_trigger, sql_saga._make_name(ARRAY[foreign_key_name], 'fk_update'));
        EXECUTE format($$
            CREATE CONSTRAINT TRIGGER %19$I AFTER UPDATE OF %20$s ON %3$I.%4$I FROM %10$I.%11$I DEFERRABLE FOR EACH ROW EXECUTE PROCEDURE
            sql_saga.fk_update_check(%1$L,%2$L,%3$L,%4$L,%5$L,%6$L,%7$L,%8$L,%9$L,%10$L,%11$L,%12$L,%13$L,%14$L,%15$L,%16$L,%17$L,%18$L);
            $$
            -- Parameters for the function call in the template
            , /* %1$  */ foreign_key_name
            , /* %2$  */ fk_table_oid
            , /* %3$  */ fk_schema_name
            , /* %4$  */ fk_table_name
            , /* %5$  */ fk_column_names_arr_str
            , /* %6$  */ fk_era_name
            , /* %7$  */ fk_start_after_column_name
            , /* %8$  */ fk_stop_on_column_name
            , /* %9$  */ uk_table_oid
            , /* %10$ */ uk_schema_name
            , /* %11$ */ uk_table_name
            , /* %12$ */ uk_column_names_arr_str
            , /* %13$ */ uk_era_name
            , /* %14$ */ uk_start_after_column_name
            , /* %15$ */ uk_stop_on_column_name
            , /* %16$ */ match_type
            , /* %17$ */ update_action
            , /* %18$ */ delete_action
            -- Other parameters
            , /* %19$   */ fk_update_trigger
            , /* %20$   */ foreign_columns_with_era_columns
        );

        uk_update_trigger := coalesce(uk_update_trigger, sql_saga._make_name(ARRAY[foreign_key_name], 'uk_update'));
        EXECUTE format($$
            CREATE CONSTRAINT TRIGGER %19$I AFTER UPDATE OF %20$s ON %10$I.%11$I FROM %3$I.%4$I DEFERRABLE FOR EACH ROW EXECUTE PROCEDURE
            sql_saga.uk_update_check(%1$L,%2$L,%3$L,%4$L,%5$L,%6$L,%7$L,%8$L,%9$L,%10$L,%11$L,%12$L,%13$L,%14$L,%15$L,%16$L,%17$L,%18$L);
            $$
            -- Parameters for the function call in the template
            , /* %1$  */ foreign_key_name
            , /* %2$  */ fk_table_oid
            , /* %3$  */ fk_schema_name
            , /* %4$  */ fk_table_name
            , /* %5$  */ fk_column_names_arr_str
            , /* %6$  */ fk_era_name
            , /* %7$  */ fk_start_after_column_name
            , /* %8$  */ fk_stop_on_column_name
            , /* %9$  */ uk_table_oid
            , /* %10$ */ uk_schema_name
            , /* %11$ */ uk_table_name
            , /* %12$ */ uk_column_names_arr_str
            , /* %13$ */ uk_era_name
            , /* %14$ */ uk_start_after_column_name
            , /* %15$ */ uk_stop_on_column_name
            , /* %16$ */ match_type
            , /* %17$ */ update_action
            , /* %18$ */ delete_action
            -- Other parameters
            , /* %19$ */ uk_update_trigger
            , /* %20$ */ unique_columns_with_era_columns
        );

        uk_delete_trigger := coalesce(uk_delete_trigger, sql_saga._make_name(ARRAY[foreign_key_name], 'uk_delete'));
        EXECUTE format($$
            CREATE CONSTRAINT TRIGGER %19$I AFTER DELETE ON %10$I.%11$I FROM %3$I.%4$I DEFERRABLE FOR EACH ROW EXECUTE PROCEDURE
            sql_saga.uk_delete_check(%1$L,%2$L,%3$L,%4$L,%5$L,%6$L,%7$L,%8$L,%9$L,%10$L,%11$L,%12$L,%13$L,%14$L,%15$L,%16$L,%17$L,%18$L);
            $$
            -- Parameters for the function call in the template
            , /* %1$  */ foreign_key_name
            , /* %2$  */ fk_table_oid
            , /* %3$  */ fk_schema_name
            , /* %4$  */ fk_table_name
            , /* %5$  */ fk_column_names_arr_str
            , /* %6$  */ fk_era_name
            , /* %7$  */ fk_start_after_column_name
            , /* %8$  */ fk_stop_on_column_name
            , /* %9$  */ uk_table_oid
            , /* %10$ */ uk_schema_name
            , /* %11$ */ uk_table_name
            , /* %12$ */ uk_column_names_arr_str
            , /* %13$ */ uk_era_name
            , /* %14$ */ uk_start_after_column_name
            , /* %15$ */ uk_stop_on_column_name
            , /* %16$ */ match_type
            , /* %17$ */ update_action
            , /* %18$ */ delete_action
            -- Other parameters
            , /* %19$  */ uk_delete_trigger
        );

        INSERT INTO sql_saga.foreign_keys
            ( foreign_key_name
            , table_oid
            , column_names
            , era_name
            , unique_key_name
            , match_type
            , update_action
            , delete_action
            , fk_insert_trigger
            , fk_update_trigger
            , uk_update_trigger
            , uk_delete_trigger
            ) VALUES
            ( foreign_key_name
            , fk_table_oid
            , fk_column_names
            , fk_era_name
            , uk_row.unique_key_name
            , match_type
            , update_action
            , delete_action
            , fk_insert_trigger
            , fk_update_trigger
            , uk_update_trigger
            , uk_delete_trigger
            );

        /* Validate the constraint on existing data, iterating over each row. */
        EXECUTE format(
            'SELECT sql_saga.validate_foreign_key_new_row('||
            'to_jsonb(%4$I.*)'||
            ',%1$L,%2$L,%3$L,%4$L,%5$L,%6$L,%7$L,%8$L,%9$L,%10$L,%11$L,%12$L,%13$L,%14$L,%15$L,%16$L,%17$L,%18$L'||
            ') FROM %3$I.%4$I;'
            , /* %1$  */ foreign_key_name
            , /* %2$  */ fk_table_oid
            , /* %3$  */ fk_schema_name
            , /* %4$  */ fk_table_name
            , /* %5$  */ fk_column_names_arr_str
            , /* %6$  */ fk_era_name
            , /* %7$  */ fk_start_after_column_name
            , /* %8$  */ fk_stop_on_column_name
            , /* %9$  */ uk_table_oid
            , /* %10$ */ uk_schema_name
            , /* %11$ */ uk_table_name
            , /* %12$ */ uk_column_names_arr_str
            , /* %13$ */ uk_era_name
            , /* %14$ */ uk_start_after_column_name
            , /* %15$ */ uk_stop_on_column_name
            , /* %16$ */ match_type
            , /* %17$ */ update_action
            , /* %18$ */ delete_action
            );
    END;

    RETURN foreign_key_name;
END;
$function$;


CREATE FUNCTION sql_saga.drop_foreign_key(
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
BEGIN
    IF table_oid IS NULL AND key_name IS NULL THEN
        RAISE EXCEPTION 'no table or key name specified';
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga._serialize(table_oid);

    FOR foreign_key_row IN
        SELECT fk.*
        FROM sql_saga.foreign_keys AS fk
        WHERE (fk.table_oid = table_oid OR table_oid IS NULL)
          AND (fk.foreign_key_name = key_name OR key_name IS NULL)
    LOOP
        DELETE FROM sql_saga.foreign_keys AS fk
        WHERE fk.foreign_key_name = foreign_key_row.foreign_key_name;

        /*
         * Make sure the table hasn't been dropped and that the triggers exist
         * before doing these.  We could use the IF EXISTS clause but we don't
         * in order to avoid the NOTICE.
         */
        IF EXISTS (
                SELECT FROM pg_catalog.pg_class AS c
                WHERE c.oid = foreign_key_row.table_oid)
            AND EXISTS (
                SELECT FROM pg_catalog.pg_trigger AS t
                WHERE t.tgrelid = foreign_key_row.table_oid
                  AND t.tgname IN (foreign_key_row.fk_insert_trigger, foreign_key_row.fk_update_trigger))
        THEN
            EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.fk_insert_trigger, foreign_key_row.table_oid);
            EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.fk_update_trigger, foreign_key_row.table_oid);
        END IF;

        SELECT uk.table_oid
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

CREATE FUNCTION sql_saga.fk_insert_check()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
#variable_conflict use_variable
DECLARE
    /* Use jsonb to look up values by parameterized names */
    jnew jsonb := to_jsonb(NEW);
    foreign_key_name name              := TG_ARGV[0];
    fk_table_oid regclass              := TG_ARGV[1];
    fk_schema_name text                := TG_ARGV[2];
    fk_table_name text                 := TG_ARGV[3];
    fk_column_names text[]             := TG_ARGV[4];
    fk_era_name text                   := TG_ARGV[5];
    fk_start_after_column_name text          := TG_ARGV[6];
    fk_stop_on_column_name text            := TG_ARGV[7];
    uk_table_oid regclass              := TG_ARGV[8];
    uk_schema_name text                := TG_ARGV[9];
    uk_table_name text                 := TG_ARGV[10];
    uk_column_names text[]             := TG_ARGV[11];
    uk_era_name text                   := TG_ARGV[12];
    uk_start_after_column_name text          := TG_ARGV[13];
    uk_stop_on_column_name text            := TG_ARGV[14];
    match_type sql_saga.fk_match_types := TG_ARGV[15];
    update_action sql_saga.fk_actions  := TG_ARGV[16];
    delete_action sql_saga.fk_actions  := TG_ARGV[17];
BEGIN
    /*
     * This function is called when a new row is inserted into a table
     * containing foreign keys with sql_saga.  It checks to verify that the
     * referenced table contains the proper data to satisfy the foreign key
     * constraint.
     *
     * The first argument is the name of the foreign key in our custom
     * catalogs.
     */

    jnew := to_jsonb(NEW);
    PERFORM sql_saga.validate_foreign_key_new_row(jnew, foreign_key_name, fk_table_oid, fk_schema_name, fk_table_name, fk_column_names, fk_era_name, fk_start_after_column_name, fk_stop_on_column_name, uk_table_oid, uk_schema_name, uk_table_name, uk_column_names, uk_era_name, uk_start_after_column_name, uk_stop_on_column_name, match_type, update_action, delete_action);
    RETURN NULL;
END;
$function$;

CREATE FUNCTION sql_saga.fk_update_check()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
#variable_conflict use_variable
DECLARE
    /* Use jsonb to look up values by parameterized names */
    jnew jsonb := to_jsonb(NEW);

    foreign_key_name name              := TG_ARGV[0];
    fk_table_oid regclass              := TG_ARGV[1];
    fk_schema_name text                := TG_ARGV[2];
    fk_table_name text                 := TG_ARGV[3];
    fk_column_names text[]             := TG_ARGV[4];
    fk_era_name text                   := TG_ARGV[5];
    fk_start_after_column_name text          := TG_ARGV[6];
    fk_stop_on_column_name text            := TG_ARGV[7];
    uk_table_oid regclass              := TG_ARGV[8];
    uk_schema_name text                := TG_ARGV[9];
    uk_table_name text                 := TG_ARGV[10];
    uk_column_names text[]             := TG_ARGV[11];
    uk_era_name text                   := TG_ARGV[12];
    uk_start_after_column_name text          := TG_ARGV[13];
    uk_stop_on_column_name text            := TG_ARGV[14];
    match_type sql_saga.fk_match_types := TG_ARGV[15];
    update_action sql_saga.fk_actions  := TG_ARGV[16];
    delete_action sql_saga.fk_actions  := TG_ARGV[17];
BEGIN
    /*
     * This function is called when a table containing foreign keys with
     * periods is updated.  It checks to verify that the referenced table
     * contains the proper data to satisfy the foreign key constraint.
     *
     * The first argument is the name of the foreign key in our custom
     * catalogs.
     */

    /* Check the constraint */

    PERFORM sql_saga.validate_foreign_key_new_row(jnew, foreign_key_name, fk_table_oid, fk_schema_name, fk_table_name, fk_column_names, fk_era_name, fk_start_after_column_name, fk_stop_on_column_name, uk_table_oid, uk_schema_name, uk_table_name, uk_column_names, uk_era_name, uk_start_after_column_name, uk_stop_on_column_name, match_type, update_action, delete_action);
    RETURN NULL;
END;
$function$;

/*
 * This function is called by an AFTER UPDATE or AFTER DELETE trigger on a
 * referenced (unique key) table. It finds all rows in the referencing table
 * that pointed to the OLD row and validates that their validity periods are
 * still covered after the change.
 *
 * Performance characteristics:
 * This function uses a single, set-based query. It finds all affected foreign
 * key rows and, for each one, runs a correlated subquery with the
 * `covers_without_gaps` aggregate to validate its coverage. This is generally
 * more performant than a `LOOP` for bulk operations. The total complexity is
 * roughly O(N * M log M), where N is the number of referencing rows and M is
 * the number of unique key rows, but with lower per-row overhead than a
 * pl/pgsql loop.
 *
 * NOTE: Previous attempts at a set-based query failed due to subtle bugs in
 * the `covers_without_gaps` aggregate itself. Now that the aggregate is
 * robust, this more performant set-based approach is viable and correct.
 */
CREATE FUNCTION sql_saga.validate_foreign_key_old_row(
    row_data jsonb,
    is_update boolean,

    foreign_key_name name,
    fk_table_oid regclass,
    fk_schema_name text,
    fk_table_name text,
    fk_column_names text[],
    fk_era_name text,
    fk_start_after_column_name text,
    fk_stop_on_column_name text,
    uk_table_oid regclass,
    uk_schema_name text,
    uk_table_name text,
    uk_column_names text[],
    uk_era_name text,
    uk_start_after_column_name text,
    uk_stop_on_column_name text,
    match_type sql_saga.fk_match_types,
    delete_action sql_saga.fk_actions,
    update_action sql_saga.fk_actions
    )
 RETURNS boolean
 LANGUAGE plpgsql
AS
$function$
DECLARE
    column_name name;
    join_on text;
    where_clause text;
    uk_range_constructor regtype;
    fk_range_constructor regtype;
    violation boolean;

    QSQL_VALIDATE_FKS CONSTANT text :=
    'SELECT EXISTS ('
    '  SELECT 1'
    '  FROM %1$s AS fk'
    '  WHERE %4$s AND COALESCE(NOT ('
    '    SELECT sql_saga.covers_without_gaps('
    '      %7$I(uk.%8$I, uk.%9$I, ''(]''),'
    '      %10$I(fk.%5$I, fk.%6$I, ''(]'')'
    '      ORDER BY uk.%8$I'
    '    )'
    '    FROM %2$s AS uk'
    '    WHERE %3$s' -- join condition: fk.col = uk.col
    '  ), true)'
    ')';

BEGIN
    -- If any part of the key in the OLD row is NULL, it cannot be referenced.
    FOREACH column_name IN ARRAY uk_column_names LOOP
        IF row_data->>column_name IS NULL THEN
            RETURN true;
        END IF;
    END LOOP;

    -- Build JOIN clause for subquery: fk -> uk
    SELECT string_agg(format('fk.%I = uk.%I', u.fkc, u.ukc), ' AND ')
    INTO join_on
    FROM unnest(fk_column_names, uk_column_names) AS u(fkc, ukc);

    -- Build WHERE clause to find fk rows that reference the OLD uk row
    SELECT string_agg(format('fk.%I = %s', u.fkc, quote_literal(row_data->>u.ukc)), ' AND ')
    INTO where_clause
    FROM unnest(fk_column_names, uk_column_names) AS u(fkc, ukc);

    SELECT range_type INTO fk_range_constructor FROM sql_saga.era WHERE table_oid = fk_table_oid AND era_name = fk_era_name;
    SELECT range_type INTO uk_range_constructor FROM sql_saga.era WHERE table_oid = uk_table_oid AND era_name = uk_era_name;

    EXECUTE format(QSQL_VALIDATE_FKS,
        fk_table_oid,                                -- %1$s: fk table
        uk_table_oid,                                -- %2$s: uk table
        join_on,                                     -- %3$s: join condition
        where_clause,                                -- %4$s: where condition
        fk_start_after_column_name,                  -- %5$I: fk start col
        fk_stop_on_column_name,                      -- %6$I: fk end col
        uk_range_constructor,                        -- %7$I: uk range type constructor
        uk_start_after_column_name,                  -- %8$I: uk start col
        uk_stop_on_column_name,                      -- %9$I: uk end col
        fk_range_constructor                         -- %10$I: fk range type constructor
    )
    INTO violation;

    IF violation THEN
        RAISE EXCEPTION 'update or delete on table "%" violates foreign key constraint "%" on table "%"',
            uk_table_oid,
            foreign_key_name,
            fk_table_oid;
    END IF;

    RETURN true;
END;
$function$;

/*
 * This function validates a single new or updated row in a referencing
 * (foreign key) table.
 *
 * Performance characteristics:
 * This function executes a single set-based query against the referenced
 * table. The primary cost is the `covers_without_gaps` aggregate, which
 * operates on the set of unique key rows matching the foreign key.
 * Performance is roughly O(M log M) where M is the number of referenced key
 * rows, due to the sorting required by the aggregate.
 */
CREATE FUNCTION sql_saga.validate_foreign_key_new_row(row_data jsonb
    , foreign_key_name name
    , fk_table_oid regclass
    , fk_schema_name text
    , fk_table_name text
    , fk_column_names text[]
    , fk_era_name text
    , fk_start_after_column_name text
    , fk_stop_on_column_name text
    , uk_table_oid regclass
    , uk_schema_name text
    , uk_table_name text
    , uk_column_names text[]
    , uk_era_name text
    , uk_start_after_column_name text
    , uk_stop_on_column_name text
    , match_type sql_saga.fk_match_types
    , update_action sql_saga.fk_actions
    , delete_action sql_saga.fk_actions
    )
 RETURNS boolean
 LANGUAGE plpgsql
AS
$function$
#variable_conflict use_variable
DECLARE
    okay boolean;
    uk_where_clause text;
    fk_start_val text;
    fk_end_val text;
    uk_range_constructor regtype;
    fk_range_constructor regtype;

    QSQL_VALIDATE_NEW_ROW CONSTANT text :=
    'SELECT COALESCE(('
    '  SELECT sql_saga.covers_without_gaps('
    '    %1$I(uk.%2$I, uk.%3$I, ''(]''),'
    '    %4$I(%5$L, %6$L, ''(]'')'
    '    ORDER BY uk.%2$I'
    '  )'
    '  FROM %7$I.%8$I AS uk'
    '  WHERE %9$s'
    '), false)';

BEGIN
    IF row_data IS NULL THEN
        RAISE EXCEPTION 'row_data is not provided';
    END IF;

    /*
     * Check for NULLs in the foreign key columns and handle according to
     * MATCH type.
     */
    DECLARE
        column_name name;
        has_nulls boolean := false;
        all_nulls boolean := true;
    BEGIN
        FOREACH column_name IN ARRAY fk_column_names LOOP
            IF row_data->>column_name IS NULL THEN
                has_nulls := true;
            ELSE
                all_nulls := false;
            END IF;
        END LOOP;

        IF all_nulls THEN
            /*
             * If there are no values at all, all three types pass.
             *
             * Period columns are by definition NOT NULL so the FULL MATCH
             * type is only concerned with the non-period columns of the
             * constraint.  SQL:2016 4.23.3.3
             */
            RETURN true;
        END IF;

        IF has_nulls THEN
            CASE match_type
                WHEN 'SIMPLE' THEN
                    RETURN true;
                WHEN 'PARTIAL' THEN
                    RAISE EXCEPTION 'MATCH PARTIAL is not implemented';
                WHEN 'FULL' THEN
                    RAISE EXCEPTION 'insert or update on table "%" violates foreign key constraint "%" (MATCH FULL with NULLs)',
                        fk_table_oid,
                        foreign_key_name;
            END CASE;
        END IF;
    END;

    /*
     * Build and execute a query to check if the referenced unique key
     * completely covers the new row's validity period.
     */
    SELECT string_agg(format('uk.%I = %s', u.ukc, quote_literal(row_data->>u.fkc)), ' AND ')
    INTO uk_where_clause
    FROM unnest(fk_column_names, uk_column_names) AS u(fkc, ukc);

    fk_start_val := row_data->>fk_start_after_column_name;
    fk_end_val := row_data->>fk_stop_on_column_name;

    SELECT range_type INTO fk_range_constructor FROM sql_saga.era WHERE table_oid = fk_table_oid AND era_name = fk_era_name;
    SELECT range_type INTO uk_range_constructor FROM sql_saga.era WHERE table_oid = uk_table_oid AND era_name = uk_era_name;

    EXECUTE format(QSQL_VALIDATE_NEW_ROW,
        uk_range_constructor,           -- %1$I
        uk_start_after_column_name,     -- %2$I
        uk_stop_on_column_name,         -- %3$I
        fk_range_constructor,           -- %4$I
        fk_start_val,                   -- %5$L
        fk_end_val,                     -- %6$L
        uk_schema_name,                 -- %7$I
        uk_table_name,                  -- %8$I
        uk_where_clause                 -- %9$s
    ) INTO okay;

    IF NOT okay THEN
        RAISE EXCEPTION 'insert or update on table "%" violates foreign key constraint "%"',
            fk_table_oid,
            foreign_key_name;
    END IF;

    RETURN true;
END;
$function$;

--TODO: Pick relevant parts of creating functions for views
-- to make the API for the `era` table.
--
-- CREATE FUNCTION sql_saga.add_system_versioning(
--     table_class regclass,
--     audit_table_name name DEFAULT NULL,
--     view_oid name DEFAULT NULL,
--     function_as_of_name name DEFAULT NULL,
--     function_between_name name DEFAULT NULL,
--     function_between_symmetric_name name DEFAULT NULL,
--     function_from_to_name name DEFAULT NULL)
--  RETURNS void
--  LANGUAGE plpgsql
--  SECURITY DEFINER
-- AS
-- $function$
-- #variable_conflict use_variable
-- DECLARE
--     schema_name name;
--     table_name name;
--     table_owner regrole;
--     persistence "char";
--     kind "char";
--     era_row sql_saga.era;
--     history_table_id oid;
--     sql text;
--     grantees text;
-- BEGIN
--     IF table_class IS NULL THEN
--         RAISE EXCEPTION 'no table name specified';
--     END IF;
--
--     /* Always serialize operations on our catalogs */
--     PERFORM sql_saga._serialize(table_class);
--
--     /*
--      * REFERENCES:
--      *     SQL:2016 4.15.2.2
--      *     SQL:2016 11.3 SR 2.3
--      *     SQL:2016 11.3 GR 1.c
--      *     SQL:2016 11.29
--      */
--
--     /* Already registered? SQL:2016 11.29 SR 5 */
--     IF EXISTS (SELECT FROM sql_saga.system_versioning AS r WHERE r.table_oid = table_class) THEN
--         RAISE EXCEPTION 'table already has SYSTEM VERSIONING';
--     END IF;
--
--     /* Must be a regular persistent base table. SQL:2016 11.29 SR 2 */
--
--     SELECT n.nspname, c.relname, c.relowner, c.relpersistence, c.relkind
--     INTO schema_name, table_name, table_owner, persistence, kind
--     FROM pg_catalog.pg_class AS c
--     JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
--     WHERE c.oid = table_class;
--
--     IF kind <> 'r' THEN
--         /*
--          * The main reason partitioned tables aren't supported yet is simply
--          * because I haven't put any thought into it.
--          * Maybe it's trivial, maybe not.
--          */
--         IF kind = 'p' THEN
--             RAISE EXCEPTION 'partitioned tables are not supported yet';
--         END IF;
--
--         RAISE EXCEPTION 'relation % is not a table', $1;
--     END IF;
--
--     IF persistence <> 'p' THEN
--         /*
--          * We could probably accept unlogged tables if the history table is
--          * also unlogged, but what's the point?
--          */
--         RAISE EXCEPTION 'table "%" must be persistent', table_class;
--     END IF;
--
--     /* We need a SYSTEM_TIME period. SQL:2016 11.29 SR 4 */
--     SELECT p.*
--     INTO era_row
--     FROM sql_saga.era AS p
--     WHERE (p.table_name, p.era_name) = (table_class, 'system_time');
--
--     IF NOT FOUND THEN
--         RAISE EXCEPTION 'no period for SYSTEM_TIME found for table %', table_class;
--     END IF;
--
--     /* Get all of our "fake" infrastructure ready */
--     audit_table_name := coalesce(audit_table_name, sql_saga._make_name(ARRAY[table_name], 'history'));
--     view_oid := coalesce(view_oid, sql_saga._make_name(ARRAY[table_name], 'with_history'));
--     function_as_of_name := coalesce(function_as_of_name, sql_saga._make_name(ARRAY[table_name], '_as_of'));
--     function_between_name := coalesce(function_between_name, sql_saga._make_name(ARRAY[table_name], '_between'));
--     function_between_symmetric_name := coalesce(function_between_symmetric_name, sql_saga._make_name(ARRAY[table_name], '_between_symmetric'));
--     function_from_to_name := coalesce(function_from_to_name, sql_saga._make_name(ARRAY[table_name], '_from_to'));
--
--     /*
--      * Create the history table.  If it already exists we check that all the
--      * columns match but otherwise we trust the user.  Perhaps the history
--      * table was disconnected in order to change the schema (a case which is
--      * not defined by the SQL standard).  Or perhaps the user wanted to
--      * partition the history table.
--      *
--      * There shouldn't be any concurrency issues here because our main catalog
--      * is locked.
--      */
--     SELECT c.oid
--     INTO history_table_id
--     FROM pg_catalog.pg_class AS c
--     JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
--     WHERE (n.nspname, c.relname) = (schema_name, audit_table_name);
--
--     IF FOUND THEN
--         /* Don't allow any periods on the history table (this might be relaxed later) */
--         IF EXISTS (SELECT FROM sql_saga.era AS p WHERE p.table_name = history_table_id) THEN
--             RAISE EXCEPTION 'history tables for SYSTEM VERSIONING cannot have periods';
--         END IF;
--
--         /*
--          * The query to the attributes is harder than one would think because
--          * we need to account for dropped columns.  Basically what we're
--          * looking for is that all columns have the same name, type, and
--          * collation.
--          */
--         IF EXISTS (
--             WITH
--             L (attname, atttypid, atttypmod, attcollation) AS (
--                 SELECT a.attname, a.atttypid, a.atttypmod, a.attcollation
--                 FROM pg_catalog.pg_attribute AS a
--                 WHERE a.attrelid = table_class
--                   AND NOT a.attisdropped
--             ),
--             R (attname, atttypid, atttypmod, attcollation) AS (
--                 SELECT a.attname, a.atttypid, a.atttypmod, a.attcollation
--                 FROM pg_catalog.pg_attribute AS a
--                 WHERE a.attrelid = history_table_id
--                   AND NOT a.attisdropped
--             )
--             SELECT FROM L NATURAL FULL JOIN R
--             WHERE L.attname IS NULL OR R.attname IS NULL)
--         THEN
--             RAISE EXCEPTION 'base table "%" and history table "%" are not compatible',
--                 table_class, history_table_id::regclass;
--         END IF;
--
--         /* Make sure the owner is correct */
--         EXECUTE format('ALTER TABLE %s OWNER TO %I', history_table_id::regclass, table_owner);
--
--         /*
--          * Remove all privileges other than SELECT from everyone on the history
--          * table.  We do this without error because some privileges may have
--          * been added in order to do maintenance while we were disconnected.
--          *
--          * We start by doing the table owner because that will make sure we
--          * don't have NULL in pg_class.relacl.
--          */
--         --EXECUTE format('REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER ON TABLE %s FROM %I',
--             --history_table_id::regclass, table_owner);
--     ELSE
--         EXECUTE format('CREATE TABLE %1$I.%2$I (LIKE %1$I.%3$I)', schema_name, audit_table_name, table_name);
--         history_table_id := format('%I.%I', schema_name, audit_table_name)::regclass;
--
--         EXECUTE format('ALTER TABLE %1$I.%2$I OWNER TO %3$I', schema_name, audit_table_name, table_owner);
--
--         RAISE DEBUG 'history table "%" created for "%", be sure to index it properly',
--             history_table_id::regclass, table_class;
--     END IF;
--
--     /* Create the "with history" view.  This one we do want to error out on if it exists. */
--     EXECUTE format(
--         /*
--          * The query we really want here is
--          *
--          *     CREATE VIEW view_oid AS
--          *         TABLE table_name
--          *         UNION ALL CORRESPONDING
--          *         TABLE audit_table_name
--          *
--          * but PostgreSQL doesn't support that syntax (yet), so we have to do
--          * it manually.
--          */
--         'CREATE VIEW %1$I.%2$I AS SELECT %5$s FROM %1$I.%3$I UNION ALL SELECT %5$s FROM %1$I.%4$I',
--         schema_name, view_oid, table_name, audit_table_name,
--         (SELECT string_agg(quote_ident(a.attname), ', ' ORDER BY a.attnum)
--          FROM pg_attribute AS a
--          WHERE a.attrelid = table_class
--            AND a.attnum > 0
--            AND NOT a.attisdropped
--         ));
--     EXECUTE format('ALTER VIEW %1$I.%2$I OWNER TO %3$I', schema_name, view_oid, table_owner);
--
--     /*
--      * Create functions to simulate the system versioned grammar.  These must
--      * be inlinable for any kind of performance.
--      */
--     EXECUTE format(
--         $$
--         CREATE FUNCTION %1$I.%2$I(timestamp with time zone)
--          RETURNS SETOF %1$I.%3$I
--          LANGUAGE sql
--          STABLE
--         AS 'SELECT * FROM %1$I.%3$I WHERE %4$I <= $1 AND %5$I > $1'
--         $$, schema_name, function_as_of_name, view_oid, era_row.start_after_column_name, era_row.stop_on_column_name);
--     EXECUTE format('ALTER FUNCTION %1$I.%2$I(timestamp with time zone) OWNER TO %3$I',
--         schema_name, function_as_of_name, table_owner);
--
--     EXECUTE format(
--         $$
--         CREATE FUNCTION %1$I.%2$I(timestamp with time zone, timestamp with time zone)
--          RETURNS SETOF %1$I.%3$I
--          LANGUAGE sql
--          STABLE
--         AS 'SELECT * FROM %1$I.%3$I WHERE $1 <= $2 AND %5$I > $1 AND %4$I <= $2'
--         $$, schema_name, function_between_name, view_oid, era_row.start_after_column_name, era_row.stop_on_column_name);
--     EXECUTE format('ALTER FUNCTION %1$I.%2$I(timestamp with time zone, timestamp with time zone) OWNER TO %3$I',
--         schema_name, function_between_name, table_owner);
--
--     EXECUTE format(
--         $$
--         CREATE FUNCTION %1$I.%2$I(timestamp with time zone, timestamp with time zone)
--          RETURNS SETOF %1$I.%3$I
--          LANGUAGE sql
--          STABLE
--         AS 'SELECT * FROM %1$I.%3$I WHERE %5$I > least($1, $2) AND %4$I <= greatest($1, $2)'
--         $$, schema_name, function_between_symmetric_name, view_oid, era_row.start_after_column_name, era_row.stop_on_column_name);
--     EXECUTE format('ALTER FUNCTION %1$I.%2$I(timestamp with time zone, timestamp with time zone) OWNER TO %3$I',
--         schema_name, function_between_symmetric_name, table_owner);
--
--     EXECUTE format(
--         $$
--         CREATE FUNCTION %1$I.%2$I(timestamp with time zone, timestamp with time zone)
--          RETURNS SETOF %1$I.%3$I
--          LANGUAGE sql
--          STABLE
--         AS 'SELECT * FROM %1$I.%3$I WHERE $1 < $2 AND %5$I > $1 AND %4$I < $2'
--         $$, schema_name, function_from_to_name, view_oid, era_row.start_after_column_name, era_row.stop_on_column_name);
--     EXECUTE format('ALTER FUNCTION %1$I.%2$I(timestamp with time zone, timestamp with time zone) OWNER TO %3$I',
--         schema_name, function_from_to_name, table_owner);
--
--     /* Set privileges on history objects */
--     FOR sql IN
--         SELECT format('REVOKE ALL ON %s %s FROM %s',
--                       CASE object_type
--                           WHEN 'r' THEN 'TABLE'
--                           WHEN 'p' THEN 'TABLE'
--                           WHEN 'v' THEN 'TABLE'
--                           WHEN 'f' THEN 'FUNCTION'
--                       ELSE 'ERROR'
--                       END,
--                       string_agg(DISTINCT object_name, ', '),
--                       string_agg(DISTINCT quote_ident(COALESCE(a.rolname, 'public')), ', '))
--         FROM (
--             SELECT c.relkind AS object_type,
--                    c.oid::regclass::text AS object_name,
--                    acl.grantee AS grantee
--             FROM pg_class AS c
--             JOIN pg_namespace AS n ON n.oid = c.relnamespace
--             CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
--             WHERE n.nspname = schema_name
--               AND c.relname IN (audit_table_name, view_oid)
--
--             UNION ALL
--
--             SELECT 'f',
--                    p.oid::regprocedure::text,
--                    acl.grantee
--             FROM pg_proc AS p
--             CROSS JOIN LATERAL aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) AS acl
--             WHERE p.oid = ANY (ARRAY[
--                     format('%I.%I(timestamp with time zone)', schema_name, function_as_of_name)::regprocedure,
--                     format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_between_name)::regprocedure,
--                     format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_between_symmetric_name)::regprocedure,
--                     format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_from_to_name)::regprocedure
--                 ])
--         ) AS objects
--         LEFT JOIN pg_authid AS a ON a.oid = objects.grantee
--         GROUP BY objects.object_type
--     LOOP
--         EXECUTE sql;
--     END LOOP;
--
--     FOR grantees IN
--         SELECT string_agg(acl.grantee::regrole::text, ', ')
--         FROM pg_class AS c
--         CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
--         WHERE c.oid = table_class
--           AND acl.privilege_type = 'SELECT'
--     LOOP
--         EXECUTE format('GRANT SELECT ON TABLE %1$I.%2$I, %1$I.%3$I TO %4$s',
--                        schema_name, audit_table_name, view_oid, grantees);
--         EXECUTE format('GRANT EXECUTE ON FUNCTION %s, %s, %s, %s TO %s',
--                        format('%I.%I(timestamp with time zone)', schema_name, function_as_of_name)::regprocedure,
--                        format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_between_name)::regprocedure,
--                        format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_between_symmetric_name)::regprocedure,
--                        format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_from_to_name)::regprocedure,
--                        grantees);
--     END LOOP;
--
--     /* Register it */
--     INSERT INTO sql_saga.system_versioning (table_name, era_name, audit_table_name, view_oid,
--                                            func_as_of, func_between, func_between_symmetric, func_from_to)
--     VALUES (
--         table_class,
--         'system_time',
--         format('%I.%I', schema_name, audit_table_name),
--         format('%I.%I', schema_name, view_oid),
--         format('%I.%I(timestamp with time zone)', schema_name, function_as_of_name),
--         format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_between_name),
--         format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_between_symmetric_name),
--         format('%I.%I(timestamp with time zone,timestamp with time zone)', schema_name, function_from_to_name)
--     );
-- END;
-- $function$;
--
-- CREATE FUNCTION sql_saga.drop_system_versioning(table_name regclass, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT false)
--  RETURNS boolean
--  LANGUAGE plpgsql
--  SECURITY DEFINER
-- AS $function$
-- #variable_conflict use_variable
-- DECLARE
--     system_versioning_row sql_saga.system_versioning;
--     is_dropped boolean;
-- BEGIN
--     IF table_name IS NULL THEN
--         RAISE EXCEPTION 'no table name specified';
--     END IF;
--
--     /* Always serialize operations on our catalogs */
--     PERFORM sql_saga._serialize(table_name);
--
--     /*
--      * REFERENCES:
--      *     SQL:2016 4.15.2.2
--      *     SQL:2016 11.3 SR 2.3
--      *     SQL:2016 11.3 GR 1.c
--      *     SQL:2016 11.30
--      */
--
--     /*
--      * We need to delete our row first so that the DROP protection doesn't
--      * block us.
--      */
--     DELETE FROM sql_saga.system_versioning AS sv
--     WHERE sv.table_name = table_name
--     RETURNING * INTO system_versioning_row;
--
--     IF NOT FOUND THEN
--         RAISE DEBUG 'table % does not have SYSTEM VERSIONING', table_name;
--         RETURN false;
--     END IF;
--
--     /*
--      * Has the table been dropped?  If so, everything else is also dropped
--      * except for the history table.
--      */
--     is_dropped := NOT EXISTS (SELECT FROM pg_catalog.pg_class AS c WHERE c.oid = table_name);
--
--     IF NOT is_dropped THEN
--         /* Drop the functions. */
--         EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_as_of::regprocedure, drop_behavior);
--         EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_between::regprocedure, drop_behavior);
--         EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_between_symmetric::regprocedure, drop_behavior);
--         EXECUTE format('DROP FUNCTION %s %s', system_versioning_row.func_from_to::regprocedure, drop_behavior);
--
--         /* Drop the "with_history" view. */
--         EXECUTE format('DROP VIEW %s %s', system_versioning_row.view_oid, drop_behavior);
--     END IF;
--
--     /*
--      * SQL:2016 11.30 GR 2 says "Every row of T that corresponds to a
--      * historical system row is effectively deleted at the end of the SQL-
--      * statement." but we leave the history table intact in case the user
--      * merely wants to make some DDL changes and hook things back up again.
--      *
--      * The cleanup parameter tells us that the user really wants to get rid of it
--      * all.
--      */
--     IF NOT is_dropped AND cleanup THEN
--         PERFORM sql_saga.drop_era(table_name, 'system_time', drop_behavior, cleanup);
--         EXECUTE format('DROP TABLE %s %s', system_versioning_row.audit_table_name, drop_behavior);
--     END IF;
--
--     RETURN true;
-- END;
-- $function$;


CREATE FUNCTION sql_saga.drop_protection()
 RETURNS event_trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    r record;
    table_oid regclass;
    era_name name;
BEGIN
    /*
     * This function is called after the fact, so we have to just look to see
     * if anything is missing in the catalogs if we just store the name and not
     * a reg* type.
     */

    ---
    --- periods
    ---

    /* If one of our tables is being dropped, remove references to it */
    FOR table_oid, era_name IN
        SELECT p.table_oid, p.era_name
        FROM sql_saga.era AS p
        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
                ON dobj.objid = p.table_oid
        WHERE dobj.object_type = 'table'
        ORDER BY dobj.ordinality
    LOOP
        PERFORM sql_saga.drop_era(table_oid, era_name, 'CASCADE', true);
    END LOOP;

    /*
     * If a column belonging to one of our periods is dropped, we need to reject that.
     * SQL:2016 11.23 SR 6
     */
    FOR r IN
        SELECT dobj.object_identity, e.era_name
        FROM sql_saga.era AS e
        JOIN pg_catalog.pg_attribute AS sa ON (sa.attrelid, sa.attname) = (e.table_oid, e.start_after_column_name)
        JOIN pg_catalog.pg_attribute AS ea ON (ea.attrelid, ea.attname) = (e.table_oid, e.stop_on_column_name)
        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
                ON dobj.objid = e.table_oid AND dobj.objsubid IN (sa.attnum, ea.attnum)
        WHERE dobj.object_type = 'table column'
        ORDER BY dobj.ordinality
    LOOP
        RAISE EXCEPTION 'cannot drop column "%" because it is part of the period "%"',
            r.object_identity, r.era_name;
    END LOOP;

    /* Also reject dropping the rangetype */
    FOR r IN
        SELECT dobj.object_identity, e.table_oid, e.era_name
        FROM sql_saga.era AS e
        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
                ON dobj.objid = e.range_type
        ORDER BY dobj.ordinality
    LOOP
        RAISE EXCEPTION 'cannot drop rangetype "%" because it is used in period "%" on table "%"',
            r.object_identity, r.era_name, r.table_oid;
    END LOOP;

    --/* Complain if the infinity CHECK constraint is missing. */
    --FOR r IN
    --    SELECT p.table_name, p.infinity_check_constraint
    --    FROM sql_saga.system_time_periods AS p
    --    WHERE NOT EXISTS (
    --        SELECT FROM pg_catalog.pg_constraint AS c
    --        WHERE (c.conrelid, c.conname) = (p.table_name, p.infinity_check_constraint))
    --LOOP
    --    RAISE EXCEPTION 'cannot drop constraint "%" on table "%" because it is used in SYSTEM_TIME period',
    --        r.infinity_check_constraint, r.table_oid;
    --END LOOP;

    /* Complain if the GENERATED ALWAYS AS ROW START/END trigger is missing. */
    --FOR r IN
    --    SELECT p.table_name, p.generated_always_trigger
    --    FROM sql_saga.system_time_periods AS p
    --    WHERE NOT EXISTS (
    --        SELECT FROM pg_catalog.pg_trigger AS t
    --        WHERE (t.tgrelid, t.tgname) = (p.table_name, p.generated_always_trigger))
    --LOOP
    --    RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in SYSTEM_TIME period',
    --        r.generated_always_trigger, r.table_oid;
    --END LOOP;

    /* Complain if the write_history trigger is missing. */
    -- FOR r IN
    --     SELECT p.table_name, p.write_history_trigger
    --     FROM sql_saga.system_time_periods AS p
    --     WHERE NOT EXISTS (
    --         SELECT FROM pg_catalog.pg_trigger AS t
    --         WHERE (t.tgrelid, t.tgname) = (p.table_name, p.write_history_trigger))
    -- LOOP
    --     RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in SYSTEM_TIME period',
    --         r.write_history_trigger, r.table_oid;
    -- END LOOP;

    /* Complain if the TRUNCATE trigger is missing. */
    --FOR r IN
    --    SELECT p.table_name, p.truncate_trigger
    --    FROM sql_saga.system_time_periods AS p
    --    WHERE NOT EXISTS (
    --        SELECT FROM pg_catalog.pg_trigger AS t
    --        WHERE (t.tgrelid, t.tgname) = (p.table_name, p.truncate_trigger))
    --LOOP
    --    RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in SYSTEM_TIME period',
    --        r.truncate_trigger, r.table_oid;
    --END LOOP;

    /*
     * We can't reliably find out what a column was renamed to, so just error
     * out in this case.
     */
--    FOR r IN
--        SELECT stp.table_name, u.column_name
--        FROM sql_saga.era AS stp
--        CROSS JOIN LATERAL unnest(stp.excluded_column_names) AS u (column_name)
--        WHERE NOT EXISTS (
--            SELECT FROM pg_catalog.pg_attribute AS a
--            WHERE (a.attrelid, a.attname) = (stp.table_name, u.column_name))
--    LOOP
--        RAISE EXCEPTION 'cannot drop or rename column "%" on table "%" because it is excluded from an era',
--            r.column_name, r.table_oid;
--    END LOOP;

    ---
    --- api_view
    ---

    /* Reject dropping the FOR PORTION OF view. */
    FOR r IN
        SELECT dobj.object_identity
        FROM sql_saga.api_view AS fpv
        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
                ON dobj.objid = fpv.view_oid
        WHERE dobj.object_type = 'view'
        ORDER BY dobj.ordinality
    LOOP
        RAISE EXCEPTION 'cannot drop view "%", call "sql_saga.drop_api()" instead',
            r.object_identity;
    END LOOP;

    /* Complain if the FOR PORTION OF trigger is missing. */
    FOR r IN
        SELECT fpv.table_oid, fpv.era_name, fpv.view_oid, fpv.trigger_name
        FROM sql_saga.api_view AS fpv
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (fpv.view_oid, fpv.trigger_name))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on view "%" because it is used in FOR PORTION OF view for period "%" on table "%"',
            r.trigger_name, r.view_oid, r.era_name, r.table_oid;
    END LOOP;

    /* Complain if the table's primary key has been dropped. */
    FOR r IN
        SELECT fpv.table_oid, fpv.era_name
        FROM sql_saga.api_view AS fpv
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.contype) = (fpv.table_oid, 'p'))
    LOOP
        RAISE EXCEPTION 'cannot drop primary key on table "%" because it has a FOR PORTION OF view for period "%"',
            r.table_oid, r.era_name;
    END LOOP;

    ---
    --- unique_keys
    ---

    /*
     * We don't need to protect the individual columns as long as we protect
     * the indexes.  PostgreSQL will make sure they stick around.
     */

    /* Complain if the indexes implementing our unique indexes are missing. */
    FOR r IN
        SELECT uk.unique_key_name, uk.table_oid, uk.unique_constraint
        FROM sql_saga.unique_keys AS uk
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.conname) = (uk.table_oid, uk.unique_constraint))
    LOOP
        RAISE EXCEPTION 'cannot drop constraint "%" on table "%" because it is used in era unique key "%"',
            r.unique_constraint, r.table_oid, r.unique_key_name;
    END LOOP;

    FOR r IN
        SELECT uk.unique_key_name, uk.table_oid, uk.exclude_constraint
        FROM sql_saga.unique_keys AS uk
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.conname) = (uk.table_oid, uk.exclude_constraint))
    LOOP
        RAISE EXCEPTION 'cannot drop constraint "%" on table "%" because it is used in era unique key "%"',
            r.exclude_constraint, r.table_oid, r.unique_key_name;
    END LOOP;

    ---
    --- foreign_keys
    ---

    /* Complain if any of the triggers are missing */
    FOR r IN
        SELECT fk.foreign_key_name, fk.table_oid, fk.fk_insert_trigger
        FROM sql_saga.foreign_keys AS fk
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (fk.table_oid, fk.fk_insert_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in era foreign key "%"',
            r.fk_insert_trigger, r.table_oid, r.foreign_key_name;
    END LOOP;

    FOR r IN
        SELECT fk.foreign_key_name, fk.table_oid, fk.fk_update_trigger
        FROM sql_saga.foreign_keys AS fk
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (fk.table_oid, fk.fk_update_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in era foreign key "%"',
            r.fk_update_trigger, r.table_oid, r.foreign_key_name;
    END LOOP;

    FOR r IN
        SELECT fk.foreign_key_name, uk.table_oid, fk.uk_update_trigger
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (uk.table_oid, fk.uk_update_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in era foreign key "%"',
            r.uk_update_trigger, r.table_oid, r.foreign_key_name;
    END LOOP;

    FOR r IN
        SELECT fk.foreign_key_name, uk.table_oid, fk.uk_delete_trigger
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (uk.table_oid, fk.uk_delete_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in era foreign key "%"',
            r.uk_delete_trigger, r.table_oid, r.foreign_key_name;
    END LOOP;

    ---
    --- system_versioning
    ---

--    FOR r IN
--        SELECT dobj.object_identity, sv.table_name
--        FROM sql_saga.system_versioning AS sv
--        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
--                ON dobj.objid = sv.audit_table_name
--        WHERE dobj.object_type = 'table'
--        ORDER BY dobj.ordinality
--    LOOP
--        RAISE EXCEPTION 'cannot drop table "%" because it is used in SYSTEM VERSIONING for table "%"',
--            r.object_identity, r.table_oid;
--    END LOOP;
--
--    FOR r IN
--        SELECT dobj.object_identity, sv.table_name
--        FROM sql_saga.system_versioning AS sv
--        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
--                ON dobj.objid = sv.view_oid
--        WHERE dobj.object_type = 'view'
--        ORDER BY dobj.ordinality
--    LOOP
--        RAISE EXCEPTION 'cannot drop view "%" because it is used in SYSTEM VERSIONING for table "%"',
--            r.object_identity, r.table_oid;
--    END LOOP;
--
--    FOR r IN
--        SELECT dobj.object_identity, sv.table_name
--        FROM sql_saga.system_versioning AS sv
--        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
--                ON dobj.object_identity = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to])
--        WHERE dobj.object_type = 'function'
--        ORDER BY dobj.ordinality
--    LOOP
--        RAISE EXCEPTION 'cannot drop function "%" because it is used in SYSTEM VERSIONING for table "%"',
--            r.object_identity, r.table_oid;
--    END LOOP;
END;
$function$;

CREATE EVENT TRIGGER sql_saga_drop_protection ON sql_drop EXECUTE PROCEDURE sql_saga.drop_protection();

CREATE FUNCTION sql_saga.rename_following()
 RETURNS event_trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    r record;
    sql text;
BEGIN
    /*
     * Anything that is stored by reg* type will auto-adjust, but anything we
     * store by name will need to be updated after a rename. One way to do this
     * is to recreate the constraints we have and pull new names out that way.
     * If we are unable to do something like that, we must raise an exception.
     */

    ---
    --- era
    ---

    /*
     * Start and end columns of an era can be found by the bounds check
     * constraint.
     */
    FOR sql IN
        SELECT pg_catalog.format('UPDATE sql_saga.era SET start_after_column_name = %L, stop_on_column_name = %L WHERE (table_oid, era_name) = (%L::regclass, %L)',
            sa.attname, ea.attname, e.table_oid, e.era_name)
        FROM sql_saga.era AS e
        JOIN pg_catalog.pg_constraint AS c ON (c.conrelid, c.conname) = (e.table_oid, e.bounds_check_constraint)
        JOIN pg_catalog.pg_attribute AS sa ON sa.attrelid = e.table_oid
        JOIN pg_catalog.pg_attribute AS ea ON ea.attrelid = e.table_oid
        WHERE (e.start_after_column_name, e.stop_on_column_name) <> (sa.attname, ea.attname)
          AND pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((%I < %I))', sa.attname, ea.attname)
    LOOP
        EXECUTE sql;
    END LOOP;

    /*
     * Inversely, the bounds check constraint can be retrieved via the start
     * and end columns.
     */
    FOR sql IN
        SELECT pg_catalog.format('UPDATE sql_saga.era SET bounds_check_constraint = %L WHERE (table_oid, era_name) = (%L::regclass, %L)',
            c.conname, e.table_oid, e.era_name)
        FROM sql_saga.era AS e
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = e.table_oid
        JOIN pg_catalog.pg_attribute AS sa ON sa.attrelid = e.table_oid
        JOIN pg_catalog.pg_attribute AS ea ON ea.attrelid = e.table_oid
        WHERE e.bounds_check_constraint <> c.conname
          AND pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((%I < %I))', sa.attname, ea.attname)
          AND (e.start_after_column_name, e.stop_on_column_name) = (sa.attname, ea.attname)
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (e.table_oid, e.bounds_check_constraint))
    LOOP
        EXECUTE sql;
    END LOOP;

    --    FOR sql IN
    --        SELECT pg_catalog.format('UPDATE sql_saga.system_time_periods SET infinity_check_constraint = %L WHERE table_name = %L::regclass',
    --            c.conname, p.table_name)
    --        FROM sql_saga.era AS p
    --        JOIN sql_saga.system_time_periods AS stp ON (stp.table_name, stp.era_name) = (p.table_name, p.era_name)
    --        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = p.table_name
    --        JOIN pg_catalog.pg_attribute AS ea ON ea.attrelid = p.table_name
    --        WHERE stp.infinity_check_constraint <> c.conname
    --          AND pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((%I = ''infinity''::%s))', ea.attname, format_type(ea.atttypid, ea.atttypmod))
    --          AND p.stop_on_column_name = ea.attname
    --          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (stp.table_name, stp.infinity_check_constraint))
    --    LOOP
    --        EXECUTE sql;
    --    END LOOP;
    --
    --    FOR sql IN
    --        SELECT pg_catalog.format('UPDATE sql_saga.system_time_periods SET generated_always_trigger = %L WHERE table_name = %L::regclass',
    --            t.tgname, stp.table_name)
    --        FROM sql_saga.system_time_periods AS stp
    --        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = stp.table_name
    --        WHERE t.tgname <> stp.generated_always_trigger
    --          AND t.tgfoid = 'sql_saga.generated_always_as_row_start_end()'::regprocedure
    --          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (stp.table_name, stp.generated_always_trigger))
    --    LOOP
    --        EXECUTE sql;
    --    END LOOP;
    --
    --    FOR sql IN
    --        SELECT pg_catalog.format('UPDATE sql_saga.system_time_periods SET write_history_trigger = %L WHERE table_name = %L::regclass',
    --            t.tgname, stp.table_name)
    --        FROM sql_saga.system_time_periods AS stp
    --        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = stp.table_name
    --        WHERE t.tgname <> stp.write_history_trigger
    --          AND t.tgfoid = 'sql_saga.write_history()'::regprocedure
    --          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (stp.table_name, stp.write_history_trigger))
    --    LOOP
    --        EXECUTE sql;
    --    END LOOP;
    --
    --    FOR sql IN
    --        SELECT pg_catalog.format('UPDATE sql_saga.system_time_periods SET truncate_trigger = %L WHERE table_name = %L::regclass',
    --            t.tgname, stp.table_name)
    --        FROM sql_saga.system_time_periods AS stp
    --        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = stp.table_name
    --        WHERE t.tgname <> stp.truncate_trigger
    --          AND t.tgfoid = 'sql_saga.truncate_era()'::regprocedure
    --          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (stp.table_name, stp.truncate_trigger))
    --    LOOP
    --        EXECUTE sql;
    --    END LOOP;

    /*
     * We can't reliably find out what a column was renamed to, so just error
     * out in this case.
     */
--    FOR r IN
--        SELECT stp.table_name, u.column_name
--        FROM sql_saga.system_time_periods AS stp
--        CROSS JOIN LATERAL unnest(stp.excluded_column_names) AS u (column_name)
--        WHERE NOT EXISTS (
--            SELECT FROM pg_catalog.pg_attribute AS a
--            WHERE (a.attrelid, a.attname) = (stp.table_name, u.column_name))
--    LOOP
--        RAISE EXCEPTION 'cannot drop or rename column "%" on table "%" because it is excluded from era',
--            r.column_name, r.table_oid;
--    END LOOP;

    ---
    --- api_view
    ---

    FOR sql IN
        SELECT pg_catalog.format('UPDATE sql_saga.api_view SET trigger_name = %L WHERE (table_oid, era_name) = (%L::regclass, %L)',
            t.tgname, fpv.table_oid, fpv.era_name)
        FROM sql_saga.api_view AS fpv
        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = fpv.view_oid
        WHERE t.tgname <> fpv.trigger_name
          AND t.tgfoid = 'sql_saga.update_portion_of()'::regprocedure
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (fpv.table_oid, fpv.trigger_name))
    LOOP
        EXECUTE sql;
    END LOOP;

    ---
    --- unique_keys
    ---

    FOR sql IN
        SELECT format('UPDATE sql_saga.unique_keys SET column_names = %L WHERE unique_key_name = %L',
            a.column_names, uk.unique_key_name)
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.era AS e ON (e.table_oid, e.era_name) = (uk.table_oid, uk.era_name)
        JOIN pg_catalog.pg_constraint AS c ON (c.conrelid, c.conname) = (uk.table_oid, uk.unique_constraint)
        JOIN LATERAL (
            SELECT array_agg(a.attname ORDER BY u.ordinality) AS column_names
            FROM unnest(c.conkey) WITH ORDINALITY AS u (attnum, ordinality)
            JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attnum) = (uk.table_oid, u.attnum)
            WHERE a.attname NOT IN (e.start_after_column_name, e.stop_on_column_name)
            ) AS a ON true
        WHERE uk.column_names <> a.column_names
    LOOP
        --RAISE DEBUG 'unique_keys sql:%', sql;
        EXECUTE sql;
    END LOOP;

    FOR sql IN
        SELECT format('UPDATE sql_saga.unique_keys SET unique_constraint = %L WHERE unique_key_name = %L',
            c.conname, uk.unique_key_name)
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.era AS e ON (e.table_oid, e.era_name) = (uk.table_oid, uk.era_name)
        CROSS JOIN LATERAL unnest(uk.column_names || ARRAY[e.start_after_column_name, e.stop_on_column_name]) WITH ORDINALITY AS u (column_name, ordinality)
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = uk.table_oid
        WHERE NOT EXISTS (SELECT FROM pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (uk.table_oid, uk.unique_constraint))
        GROUP BY uk.unique_key_name, c.oid, c.conname
        HAVING format('UNIQUE (%s) DEFERRABLE', string_agg(quote_ident(u.column_name), ', ' ORDER BY u.ordinality)) = pg_catalog.pg_get_constraintdef(c.oid)
    LOOP
        --RAISE DEBUG 'unique_constraint sql:%', sql;
        EXECUTE sql;
    END LOOP;

    FOR sql IN
        SELECT format('UPDATE sql_saga.unique_keys SET exclude_constraint = %L WHERE unique_key_name = %L',
            c.conname, uk.unique_key_name)
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.era AS e ON (e.table_oid, e.era_name) = (uk.table_oid, uk.era_name)
        CROSS JOIN LATERAL unnest(uk.column_names) WITH ORDINALITY AS u (column_name, ordinality)
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = uk.table_oid
        WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (uk.table_oid, uk.exclude_constraint))
        GROUP BY uk.unique_key_name, c.oid, c.conname, e.range_type, e.start_after_column_name, e.stop_on_column_name
        HAVING format('EXCLUDE USING gist (%s, %I(%I, %I, ''(]''::text) WITH &&) DEFERRABLE',
                      string_agg(quote_ident(u.column_name) || ' WITH =', ', ' ORDER BY u.ordinality),
                      e.range_type,
                      e.start_after_column_name,
                      e.stop_on_column_name) = pg_catalog.pg_get_constraintdef(c.oid)
    LOOP
        --RAISE DEBUG 'exclude_constraint sql:%', sql;
        EXECUTE sql;
    END LOOP;

    ---
    --- foreign_keys
    ---

    /*
     * We can't reliably find out what a column was renamed to, so just error
     * out in this case.
     */
    FOR r IN
        SELECT fk.foreign_key_name, fk.table_oid, u.column_name
        FROM sql_saga.foreign_keys AS fk
        CROSS JOIN LATERAL unnest(fk.column_names) AS u (column_name)
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_attribute AS a
            WHERE (a.attrelid, a.attname) = (fk.table_oid, u.column_name))
    LOOP
        RAISE EXCEPTION 'cannot drop or rename column "%" on table "%" because it is used in era foreign key "%"',
            r.column_name, r.table_oid, r.foreign_key_name;
    END LOOP;

    /*
     * Since there can be multiple foreign keys, there is no reliable way to
     * know which trigger might belong to what, so just error out.
     */
    FOR r IN
        SELECT fk.foreign_key_name, fk.table_oid, fk.fk_insert_trigger AS trigger_name
        FROM sql_saga.foreign_keys AS fk
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (fk.table_oid, fk.fk_insert_trigger))
        UNION ALL
        SELECT fk.foreign_key_name, fk.table_oid, fk.fk_update_trigger AS trigger_name
        FROM sql_saga.foreign_keys AS fk
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (fk.table_oid, fk.fk_update_trigger))
        UNION ALL
        SELECT fk.foreign_key_name, uk.table_oid, fk.uk_update_trigger AS trigger_name
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (uk.table_oid, fk.uk_update_trigger))
        UNION ALL
        SELECT fk.foreign_key_name, uk.table_oid, fk.uk_delete_trigger AS trigger_name
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (uk.table_oid, fk.uk_delete_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop or rename trigger "%" on table "%" because it is used in an era foreign key "%"',
            r.trigger_name, r.table_oid, r.foreign_key_name;
    END LOOP;

END;
$function$;

CREATE EVENT TRIGGER sql_saga_rename_following ON ddl_command_end EXECUTE PROCEDURE sql_saga.rename_following();

CREATE OR REPLACE FUNCTION sql_saga.health_checks()
 RETURNS event_trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    cmd text;
    r record;
    save_search_path text;
BEGIN
    /* Make sure that all of our tables are still persistent */
    FOR r IN
        SELECT e.table_oid
        FROM sql_saga.era AS e
        JOIN pg_catalog.pg_class AS c ON c.oid = e.table_oid
        WHERE c.relpersistence <> 'p'
    LOOP
        RAISE EXCEPTION 'table "%" must remain persistent because it has an era',
            r.table_oid;
    END LOOP;

    /* And the history tables, too */
    FOR r IN
        SELECT e.table_oid
        FROM sql_saga.era AS e
        JOIN pg_catalog.pg_class AS c ON c.oid = e.audit_table_oid
        WHERE c.relpersistence <> 'p'
    LOOP
        RAISE EXCEPTION 'history table "%" must remain persistent because it has an era',
            r.table_oid;
    END LOOP;

    /* Check that our system versioning functions are still here */
    --    save_search_path := pg_catalog.current_setting('search_path');
    --    PERFORM pg_catalog.set_config('search_path', 'pg_catalog, pg_temp', true);
    --    FOR r IN
    --        SELECT *
    --        FROM sql_saga.era AS sv
    --        CROSS JOIN LATERAL UNNEST(ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]) AS u (fn)
    --        WHERE NOT EXISTS (
    --            SELECT FROM pg_catalog.pg_proc AS p
    --            WHERE p.oid::regprocedure::text = u.fn
    --        )
    --    LOOP
    --        RAISE EXCEPTION 'cannot drop or rename function "%" because it is used in SYSTEM VERSIONING for table "%"',
    --            r.fn, r.table_oid;
    --    END LOOP;
    --    PERFORM pg_catalog.set_config('search_path', save_search_path, true);

    /* Fix up history and for-portion objects ownership */
    FOR cmd IN
        --        SELECT format('ALTER %s %s OWNER TO %I',
        --            CASE ht.relkind
        --                WHEN 'p' THEN 'TABLE'
        --                WHEN 'r' THEN 'TABLE'
        --                WHEN 'v' THEN 'VIEW'
        --            END,
        --            ht.oid::regclass, t.relowner::regrole)
        --        FROM sql_saga.system_versioning AS sv
        --        JOIN pg_class AS t ON t.oid = sv.table_name
        --        JOIN pg_class AS ht ON ht.oid IN (sv.audit_table_name, sv.view_oid)
        --        WHERE t.relowner <> ht.relowner
        --
        --        UNION ALL

        SELECT format('ALTER VIEW %s OWNER TO %I', fpt.oid::regclass, t.relowner::regrole)
        FROM sql_saga.api_view AS fpv
        JOIN pg_class AS t ON t.oid = fpv.table_oid
        JOIN pg_class AS fpt ON fpt.oid = fpv.view_oid
        WHERE t.relowner <> fpt.relowner

        --        UNION ALL
        --
        --        SELECT format('ALTER FUNCTION %s OWNER TO %I', p.oid::regprocedure, t.relowner::regrole)
        --        FROM sql_saga.system_versioning AS sv
        --        JOIN pg_class AS t ON t.oid = sv.table_name
        --        JOIN pg_proc AS p ON p.oid = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]::regprocedure[])
        --        WHERE t.relowner <> p.proowner
    LOOP
        EXECUTE cmd;
    END LOOP;

    /* Check GRANTs */
    IF EXISTS (
        SELECT FROM pg_event_trigger_ddl_commands() AS ev_ddl
        WHERE ev_ddl.command_tag = 'GRANT')
    THEN
        FOR r IN
            SELECT *,
                   EXISTS (
                       SELECT
                       FROM pg_class AS _c
                       CROSS JOIN LATERAL aclexplode(COALESCE(_c.relacl, acldefault('r', _c.relowner))) AS _acl
                       WHERE _c.oid = objects.table_oid
                         AND _acl.grantee = objects.grantee
                         AND _acl.privilege_type = 'SELECT'
                   ) AS on_base_table
            FROM (
--                SELECT sv.table_oid,
--                       c.oid::regclass::text AS object_name,
--                       c.relkind AS object_type,
--                       acl.privilege_type,
--                       acl.privilege_type AS base_privilege_type,
--                       acl.grantee,
--                       'h' AS history_or_portion
--                FROM sql_saga.system_versioning AS sv
--                JOIN pg_class AS c ON c.oid IN (sv.audit_table_name, sv.view_oid)
--                CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
--
--                UNION ALL
--
                SELECT fpv.table_oid,
                       c.oid::regclass::text AS object_name,
                       c.relkind AS object_type,
                       acl.privilege_type,
                       acl.privilege_type AS base_privilege_type,
                       acl.grantee,
                       'p' AS history_or_portion
                FROM sql_saga.api_view AS fpv
                JOIN pg_class AS c ON c.oid = fpv.view_oid
                CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl

--                UNION ALL
--
--                SELECT sv.table_oid,
--                       p.oid::regprocedure::text,
--                       'f',
--                       acl.privilege_type,
--                       'SELECT',
--                       acl.grantee,
--                       'h'
--                FROM sql_saga.system_versioning AS sv
--                JOIN pg_proc AS p ON p.oid = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]::regprocedure[])
--                CROSS JOIN LATERAL aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) AS acl
            ) AS objects
            ORDER BY object_name, object_type, privilege_type
        LOOP
            IF
                r.history_or_portion = 'h' AND
                (r.object_type, r.privilege_type) NOT IN (('r', 'SELECT'), ('p', 'SELECT'), ('v', 'SELECT'), ('f', 'EXECUTE'))
            THEN
                RAISE EXCEPTION 'cannot grant % to "%"; history objects are read-only',
                    r.privilege_type, r.object_name;
            END IF;

            IF NOT r.on_base_table THEN
                RAISE EXCEPTION 'cannot grant % directly to "%"; grant % to "%" instead',
                    r.privilege_type, r.object_name, r.base_privilege_type, r.table_oid;
            END IF;
        END LOOP;

        /* Propagate GRANTs */
        FOR cmd IN
            SELECT format('GRANT %s ON %s %s TO %s',
                          string_agg(DISTINCT privilege_type, ', '),
                          object_type,
                          string_agg(DISTINCT object_name, ', '),
                          string_agg(DISTINCT COALESCE(a.rolname, 'public'), ', '))
            FROM (
--                SELECT 'TABLE' AS object_type,
--                       hc.oid::regclass::text AS object_name,
--                       'SELECT' AS privilege_type,
--                       acl.grantee
--                FROM sql_saga.system_versioning AS sv
--                JOIN pg_class AS c ON c.oid = sv.table_name
--                CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
--                JOIN pg_class AS hc ON hc.oid IN (sv.audit_table_name, sv.view_oid)
--                WHERE acl.privilege_type = 'SELECT'
--                  AND NOT has_table_privilege(acl.grantee, hc.oid, 'SELECT')
--
--                UNION ALL
--
                SELECT 'TABLE' AS object_type,
                       fpc.oid::regclass::text AS object_name,
                       acl.privilege_type AS privilege_type,
                       acl.grantee
                FROM sql_saga.api_view AS fpv
                JOIN pg_class AS c ON c.oid = fpv.table_oid
                CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
                JOIN pg_class AS fpc ON fpc.oid = fpv.view_oid
                WHERE NOT has_table_privilege(acl.grantee, fpc.oid, acl.privilege_type)

--                UNION ALL
--
--                SELECT 'FUNCTION',
--                       hp.oid::regprocedure::text,
--                       'EXECUTE',
--                       acl.grantee
--                FROM sql_saga.system_versioning AS sv
--                JOIN pg_class AS c ON c.oid = sv.table_name
--                CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
--                JOIN pg_proc AS hp ON hp.oid = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]::regprocedure[])
--                WHERE acl.privilege_type = 'SELECT'
--                  AND NOT has_function_privilege(acl.grantee, hp.oid, 'EXECUTE')
            ) AS objects
            LEFT JOIN pg_authid AS a ON a.oid = objects.grantee
            GROUP BY object_type
        LOOP
            EXECUTE cmd;
        END LOOP;
    END IF;

    /* Check REVOKEs */
    IF EXISTS (
        SELECT FROM pg_event_trigger_ddl_commands() AS ev_ddl
        WHERE ev_ddl.command_tag = 'REVOKE')
    THEN
        FOR r IN
--            SELECT sv.table_name,
--                   hc.oid::regclass::text AS object_name,
--                   acl.privilege_type,
--                   acl.privilege_type AS base_privilege_type
--            FROM sql_saga.system_versioning AS sv
--            JOIN pg_class AS c ON c.oid = sv.table_name
--            CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
--            JOIN pg_class AS hc ON hc.oid IN (sv.audit_table_name, sv.view_oid)
--            WHERE acl.privilege_type = 'SELECT'
--              AND NOT EXISTS (
--                SELECT
--                FROM aclexplode(COALESCE(hc.relacl, acldefault('r', hc.relowner))) AS _acl
--                WHERE _acl.privilege_type = 'SELECT'
--                  AND _acl.grantee = acl.grantee)
--
--            UNION ALL

            SELECT fpv.table_oid,
                   hc.oid::regclass::text AS object_name,
                   acl.privilege_type,
                   acl.privilege_type AS base_privilege_type
            FROM sql_saga.api_view AS fpv
            JOIN pg_class AS c ON c.oid = fpv.table_oid
            CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
            JOIN pg_class AS hc ON hc.oid = fpv.view_oid
            WHERE NOT EXISTS (
                SELECT
                FROM aclexplode(COALESCE(hc.relacl, acldefault('r', hc.relowner))) AS _acl
                WHERE _acl.privilege_type = acl.privilege_type
                  AND _acl.grantee = acl.grantee)

--            UNION ALL
--
--            SELECT sv.table_name,
--                   hp.oid::regprocedure::text,
--                   'EXECUTE',
--                   'SELECT'
--            FROM sql_saga.system_versioning AS sv
--            JOIN pg_class AS c ON c.oid = sv.table_name
--            CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
--            JOIN pg_proc AS hp ON hp.oid = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]::regprocedure[])
--            WHERE acl.privilege_type = 'SELECT'
--              AND NOT EXISTS (
--                SELECT
--                FROM aclexplode(COALESCE(hp.proacl, acldefault('f', hp.proowner))) AS _acl
--                WHERE _acl.privilege_type = 'EXECUTE'
--                  AND _acl.grantee = acl.grantee)
--
            ORDER BY table_oid, object_name
        LOOP
            RAISE EXCEPTION 'cannot revoke % directly from "%", revoke % from "%" instead',
                r.privilege_type, r.object_name, r.base_privilege_type, r.table_oid;
        END LOOP;

        /* Propagate REVOKEs */
        FOR cmd IN
            SELECT format('REVOKE %s ON %s %s FROM %s',
                          string_agg(DISTINCT privilege_type, ', '),
                          object_type,
                          string_agg(DISTINCT object_name, ', '),
                          string_agg(DISTINCT COALESCE(a.rolname, 'public'), ', '))
            FROM (
--                SELECT 'TABLE' AS object_type,
--                       hc.oid::regclass::text AS object_name,
--                       'SELECT' AS privilege_type,
--                       hacl.grantee
--                FROM sql_saga.system_versioning AS sv
--                JOIN pg_class AS hc ON hc.oid IN (sv.audit_table_name, sv.view_oid)
--                CROSS JOIN LATERAL aclexplode(COALESCE(hc.relacl, acldefault('r', hc.relowner))) AS hacl
--                WHERE hacl.privilege_type = 'SELECT'
--                  AND NOT has_table_privilege(hacl.grantee, sv.table_name, 'SELECT')
--
--                UNION ALL

                SELECT 'TABLE' AS object_type,
                       hc.oid::regclass::text AS object_name,
                       hacl.privilege_type,
                       hacl.grantee
                FROM sql_saga.api_view AS fpv
                JOIN pg_class AS hc ON hc.oid = fpv.view_oid
                CROSS JOIN LATERAL aclexplode(COALESCE(hc.relacl, acldefault('r', hc.relowner))) AS hacl
                WHERE NOT has_table_privilege(hacl.grantee, fpv.table_oid, hacl.privilege_type)

--                UNION ALL
--
--                SELECT 'FUNCTION' AS object_type,
--                       hp.oid::regprocedure::text AS object_name,
--                       'EXECUTE' AS privilege_type,
--                       hacl.grantee
--                FROM sql_saga.system_versioning AS sv
--                JOIN pg_proc AS hp ON hp.oid = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]::regprocedure[])
--                CROSS JOIN LATERAL aclexplode(COALESCE(hp.proacl, acldefault('f', hp.proowner))) AS hacl
--                WHERE hacl.privilege_type = 'EXECUTE'
--                  AND NOT has_table_privilege(hacl.grantee, sv.table_name, 'SELECT')
            ) AS objects
            LEFT JOIN pg_authid AS a ON a.oid = objects.grantee
            GROUP BY object_type
        LOOP
            EXECUTE cmd;
        END LOOP;
    END IF;
END;
$function$;

CREATE EVENT TRIGGER sql_saga_health_checks ON ddl_command_end EXECUTE PROCEDURE sql_saga.health_checks();

/* Predicates */

CREATE FUNCTION sql_saga.contains(sv1 anyelement, ev1 anyelement, ve anyelement)
 RETURNS boolean
 LANGUAGE sql
 IMMUTABLE
AS
$function$
    SELECT sv1 <= ve AND ev1 > ve;
$function$;

CREATE FUNCTION sql_saga.contains(sv1 anyelement, ev1 anyelement, sv2 anyelement, ev2 anyelement)
 RETURNS boolean
 LANGUAGE sql
 IMMUTABLE
AS
$function$
    SELECT sv1 <= sv2 AND ev1 >= ev2;
$function$;

CREATE FUNCTION sql_saga.equals(sv1 anyelement, ev1 anyelement, sv2 anyelement, ev2 anyelement)
 RETURNS boolean
 LANGUAGE sql
 IMMUTABLE
AS
$function$
    SELECT sv1 = sv2 AND ev1 = ev2;
$function$;

CREATE FUNCTION sql_saga.overlaps(sv1 anyelement, ev1 anyelement, sv2 anyelement, ev2 anyelement)
 RETURNS boolean
 LANGUAGE sql
 IMMUTABLE
AS
$function$
    SELECT sv1 < ev2 AND ev1 > sv2;
$function$;

CREATE FUNCTION sql_saga.precedes(sv1 anyelement, ev1 anyelement, sv2 anyelement, ev2 anyelement)
 RETURNS boolean
 LANGUAGE sql
 IMMUTABLE
AS
$function$
    SELECT ev1 <= sv2;
$function$;

CREATE FUNCTION sql_saga.succeeds(sv1 anyelement, ev1 anyelement, sv2 anyelement, ev2 anyelement)
 RETURNS boolean
 LANGUAGE sql
 IMMUTABLE
AS
$function$
    SELECT sv1 >= ev2;
$function$;

CREATE FUNCTION sql_saga.immediately_precedes(sv1 anyelement, ev1 anyelement, sv2 anyelement, ev2 anyelement)
 RETURNS boolean
 LANGUAGE sql
 IMMUTABLE
AS
$function$
    SELECT ev1 = sv2;
$function$;

CREATE FUNCTION sql_saga.immediately_succeeds(sv1 anyelement, ev1 anyelement, sv2 anyelement, ev2 anyelement)
 RETURNS boolean
 LANGUAGE sql
 IMMUTABLE
AS
$function$
    SELECT sv1 = ev2;
$function$;


