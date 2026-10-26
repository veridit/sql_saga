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

CREATE FUNCTION sql_saga.__internal_display_name(object_name regclass)
RETURNS text
LANGUAGE sql
STABLE
AS $__internal_display_name$
    SELECT array_to_string(parse_ident(object_name::text), '.');
$__internal_display_name$;

CREATE FUNCTION sql_saga.__internal_display_name(object_identity text)
RETURNS text
LANGUAGE sql
IMMUTABLE
SET search_path = pg_catalog
AS $__internal_display_name$
    SELECT array_to_string(parse_ident(object_identity), '.');
$__internal_display_name$;

CREATE FUNCTION sql_saga.__internal_display_name(schema_name name, object_name name)
RETURNS text
LANGUAGE sql
STABLE
AS $__internal_display_name$
    SELECT sql_saga.__internal_display_name(
        CASE WHEN schema_name = ANY (pg_catalog.current_schemas(true))
             THEN pg_catalog.quote_ident(object_name)
             ELSE pg_catalog.format('%I.%I', schema_name, object_name)
        END);
$__internal_display_name$;

CREATE FUNCTION sql_saga.__internal_display_function_name(function_identity text)
RETURNS text
LANGUAGE sql
IMMUTABLE
SET search_path = pg_catalog
AS $__internal_display_function_name$
    SELECT array_to_string(parse_ident(left(function_identity, strpos(function_identity, '(') - 1)), '.')
        || substring(function_identity FROM strpos(function_identity, '('));
$__internal_display_function_name$;

-- Only event workers use the text forms; the regclass form stays callable by PUBLIC because
-- security-invoker routines such as temporal_merge use it in their messages.
REVOKE EXECUTE ON FUNCTION sql_saga.__internal_display_name(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION sql_saga.__internal_display_function_name(text) FROM PUBLIC;

CREATE FUNCTION sql_saga.__internal_ddl_command_affects_managed_object()
RETURNS boolean
LANGUAGE plpgsql
SECURITY INVOKER
-- Catalog rows inspected here are public; the event payload is available only in an event trigger.
SET search_path = sql_saga, pg_catalog, pg_temp
AS
$function$
DECLARE
    tags text[];
BEGIN
    SELECT array_agg(command_tag) INTO tags FROM pg_event_trigger_ddl_commands();

    -- GRANT and REVOKE never need any action: privileges on derived objects are decided at access
    -- time by the caller's rights on the base table (tmp/design_final.md). Answer quickly.
    IF tags <@ ARRAY['GRANT', 'REVOKE', 'ALTER DEFAULT PRIVILEGES'] THEN
        RETURN false;
    END IF;

    -- For ALTER TABLE we must always proceed, because objid is unreliable.
    -- Policy DDL is examined by health_checks, which looks up the policy's table.
    IF 'ALTER TABLE' = ANY(tags) OR 'CREATE POLICY' = ANY(tags) OR 'ALTER POLICY' = ANY(tags) THEN
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
           -- Also check system_versioning objects (history table, view)
           OR EXISTS (SELECT 1 FROM sql_saga.system_versioning sv WHERE (sv.history_schema_name, sv.history_table_name) = (n.nspname, c.relname))
           OR EXISTS (SELECT 1 FROM sql_saga.system_versioning sv WHERE (sv.view_schema_name, sv.view_table_name) = (n.nspname, c.relname))
           OR EXISTS (SELECT 1 FROM sql_saga.system_versioning sv WHERE (sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name)) = (n.nspname, c.relname))
    );
END;
$function$;

/*
 * Is this statement part of the teardown phase of pg_restore --clean (tmp/design_restore_clean.md)?
 *
 * pg_restore --clean drops every archived object, dependents first and each base table last, before
 * it recreates them, so the objects of a sql_saga family disappear one at a time while their catalog
 * rows still exist. The event triggers let such a statement through only when all of these hold:
 *   - the recorded immediate caller is a superuser (ordinary roles cannot forge a restore exemption);
 *   - the session settings pg_dump and pg_restore always set: check_function_bodies and row_security off;
 *   - a pg_restore session: application_name 'pg_restore' (libpq's default for pg_restore), or the
 *     explicit opt-in sql_saga.clean_restore = on, for a restore script run through psql;
 *   - a top-level DROP statement (pg_restore sends each archive entry as its own query).
 * An accidental drop in any other session, even with the same settings, keeps every protection.
 * Not a security boundary: whoever may run these DROPs owns the objects and could disable the
 * triggers or the event triggers anyway. Called by the event workers; the read-only helper stays PRIVATE
 * because public event entry points use caller-recording trampolines.
 */
CREATE FUNCTION sql_saga.__internal_is_clean_teardown()
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = pg_catalog
AS $__internal_is_clean_teardown$
    SELECT sql_saga.__internal_caller_is_superuser()
       AND current_setting('check_function_bodies') = 'off'
       AND current_setting('row_security') = 'off'
       AND (current_setting('application_name') = 'pg_restore'
            -- '' (not NULL) once the session has seen the placeholder, so coalesce both
            OR coalesce(current_setting('sql_saga.clean_restore', true), '') = 'on')
       -- DROP ..., or ALTER TABLE [IF EXISTS] [ONLY] <relation> DROP CONSTRAINT ..., where <relation> is
       -- [schema.]table and each part is a plain identifier or a quoted one ("" escapes a quote,
       -- spaces and dots allowed), as pg_dump writes them.
       AND current_query() ~* '^\s*(DROP\s|ALTER\s+TABLE\s+(IF\s+EXISTS\s+)?(ONLY\s+)?(?:"(?:[^"]|"")*"|[^\s".]+)(?:\.(?:"(?:[^"]|"")*"|[^\s".]+))?\s+DROP\s+CONSTRAINT\s)';
$__internal_is_clean_teardown$;

REVOKE EXECUTE ON FUNCTION sql_saga.__internal_is_clean_teardown() FROM PUBLIC;

/*
 * Is this statement run by a parallel pg_restore (pg_restore -j N) that the caller has declared with
 * PGOPTIONS='-c sql_saga.parallel_restore=on' (README, "Backup and Restore")?
 *
 * Then health_checks defers the synchronization of history policies: each sync reads and alters both
 * tables of a family, while parallel workers already hold exclusive locks on one table each, so two
 * workers restoring the two tables of one family would deadlock. The documented final command,
 * sql_saga.sync_all_history_policies(), runs the deferred synchronization once the restore is done.
 * Recognized only in a pg_restore session (the settings pg_dump and pg_restore always set, and
 * application_name 'pg_restore'), and only with the explicit setting, which is meant for the -j
 * invocation alone. Serial and --single-transaction restores keep synchronizing as they go.
 * Not a security boundary: deferring only delays the copy of policies the restoring owner could
 * change anyway, and guarded history reads fail closed until the sync has run.
 * Called by the event workers; the read-only helper stays PRIVATE because public event entry points use
 * caller-recording trampolines.
 */
CREATE FUNCTION sql_saga.__internal_is_parallel_restore()
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = pg_catalog
AS $__internal_is_parallel_restore$
    SELECT current_setting('check_function_bodies') = 'off'
       AND current_setting('row_security') = 'off'
       AND current_setting('application_name') = 'pg_restore'
       -- '' (not NULL) once the session has seen the placeholder, so coalesce both
       AND coalesce(current_setting('sql_saga.parallel_restore', true), '') = 'on';
$__internal_is_parallel_restore$;

REVOKE EXECUTE ON FUNCTION sql_saga.__internal_is_parallel_restore() FROM PUBLIC;

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

/*
 * Names in the NOTICEs that announce what sql_saga created or dropped: quoted identifiers
 * (quote_ident / %I.%I), so names containing dots, quotes or parentheses stay unambiguous, with the
 * schema left out for public. Fixed rather than search_path dependent, because the announcing
 * functions run with their own search_path. The function form is read from the catalog by OID (so it
 * never parses identifier text) and must be computed while the function exists: schema.name and the
 * argument types, as regprocedure prints them.
 */
CREATE FUNCTION sql_saga.__internal_notice_name(schema_name name, object_name text)
RETURNS text
LANGUAGE sql
IMMUTABLE
SET search_path = pg_catalog
AS $__internal_notice_name$
    SELECT CASE WHEN schema_name = 'public' THEN quote_ident(object_name) ELSE format('%I.%I', schema_name, object_name) END;
$__internal_notice_name$;

CREATE FUNCTION sql_saga.__internal_notice_function_name(function_oid oid)
RETURNS text
LANGUAGE sql
STABLE
SET search_path = pg_catalog
AS $__internal_notice_function_name$
    SELECT CASE WHEN n.nspname = 'public' THEN quote_ident(p.proname) ELSE format('%I.%I', n.nspname, p.proname) END
        || '(' || coalesce((SELECT string_agg(format_type(a.type_oid, NULL), ',' ORDER BY a.ordinality)
                            FROM unnest(p.proargtypes::oid[]) WITH ORDINALITY AS a (type_oid, ordinality)), '') || ')'
    FROM pg_proc AS p
    JOIN pg_namespace AS n ON n.oid = p.pronamespace
    WHERE p.oid = function_oid;
$__internal_notice_function_name$;

REVOKE ALL ON FUNCTION sql_saga.__internal_notice_name(name, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_notice_function_name(oid) FROM PUBLIC;
