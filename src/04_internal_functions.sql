/*
 * These function starting with "_" are private to the periods extension and
 * should not be called by outsiders.  When all the other functions have been
 * translated to C, they will be removed.
 */
CREATE FUNCTION sql_saga.__internal_serialize(table_name regclass)
 RETURNS void
 LANGUAGE sql
AS
$function$
/* XXX: Is this the best way to do locking? */
SELECT pg_catalog.pg_advisory_xact_lock('sql_saga.era'::regclass::oid::integer, table_name::oid::integer);
$function$;

CREATE FUNCTION sql_saga.__internal_ddl_command_affects_managed_object()
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
-- A safe search_path is required
SET search_path = pg_catalog, public
AS
$function$
DECLARE
    tags text[];
BEGIN
    SELECT array_agg(command_tag) INTO tags FROM pg_event_trigger_ddl_commands();

    -- For GRANT, REVOKE, and ALTER TABLE, we must always proceed, because objid is unreliable.
    IF 'GRANT' = ANY(tags) OR 'REVOKE' = ANY(tags) OR 'ALTER TABLE' = ANY(tags) THEN
        RETURN true;
    END IF;

    -- For other commands (e.g., CREATE TABLE), we can safely use objid to
    -- check if the command affects a managed table.
    RETURN EXISTS (
        SELECT 1 FROM pg_event_trigger_ddl_commands() ddl
        JOIN pg_class c ON c.oid = ddl.objid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE EXISTS (SELECT 1 FROM sql_saga.era e WHERE (e.table_schema, e.table_name) = (n.nspname, c.relname))
           OR EXISTS (SELECT 1 FROM sql_saga.updatable_view v JOIN pg_class vc ON vc.relname = v.view_name JOIN pg_namespace vn ON vn.oid = vc.relnamespace AND vn.nspname = v.view_schema WHERE vc.oid = c.oid)
    );
END;
$function$;

CREATE FUNCTION sql_saga.__internal_make_name(resizable text[], fixed text DEFAULT NULL, separator text DEFAULT '_', extra integer DEFAULT 2)
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
        result := format('%s%s', array_to_string(resizable, separator) /* %s */, separator || fixed /* %s */);
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

CREATE FUNCTION sql_saga.__internal_make_updatable_view_name(table_name name, era_name name, view_type_suffix text)
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
     * Reduce the table and period names until they fit in NAMEDATALEN. This
     * is not very efficient but it's not on a hot code path.
     */
    max_length := greatest(length(table_name), length(era_name));

    LOOP
        result := format('%s__%s_%s', table_name /* %s */, view_type_suffix /* %s */, era_name /* %s */);
        IF octet_length(result) <= NAMEDATALEN-1 THEN
            RETURN result;
        END IF;

        max_length := max_length - 1;
        table_name := left(table_name, max_length);
        era_name := left(era_name, max_length);
    END LOOP;
END;
$function$;
