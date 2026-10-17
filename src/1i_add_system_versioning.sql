/*
 * The views and as-of functions of a system-versioned table (tmp/design_manual_schema_changes.md).
 *
 * One generator serves add_system_versioning and rebuild_system_versioning_views, so both create the
 * same objects. Columns correspond by NAME: the views show the base table's columns in its order,
 * then the history-only columns in the history's order (NULL for current rows). Every base column
 * must be in the history with the same type, typmod and collation, except those left out with
 * without_columns, which only shapes the views for one call and is stored nowhere.
 *
 * The objects keep the fixed access shape of tmp/design_final.md: owner = base owner, SELECT (views)
 * or EXECUTE (functions) granted to PUBLIC and to nobody else, B_with_history security_invoker,
 * B_history_gated owner-rights with its gate.
 */

-- A column type as written in DDL, with COLLATE when the collation is not the type's default.
CREATE FUNCTION sql_saga.__internal_column_type_sql(type_oid oid, typmod integer, collation_oid oid)
RETURNS text
LANGUAGE sql
STABLE
-- Only pg_catalog is visible, so format_type schema-qualifies every other type (sql_saga's included)
-- and the DDL in HINTs runs whatever the caller's search_path.
SET search_path = pg_catalog
AS $__internal_column_type_sql$
    SELECT pg_catalog.format_type(type_oid, typmod)
        || coalesce((SELECT ' COLLATE ' || pg_catalog.quote_ident(co.collname)
                     FROM pg_catalog.pg_collation AS co
                     WHERE co.oid = collation_oid
                       AND collation_oid IS DISTINCT FROM (SELECT t.typcollation FROM pg_catalog.pg_type AS t WHERE t.oid = type_oid)), '');
$__internal_column_type_sql$;

/*
 * Refuses a history table that cannot take every base column, with the same errors the history
 * writer raises (src/sql_saga.c, CheckHistoryTupleCompatibility). Columns in without_columns are not
 * checked: they are not in the views being generated (the writer still checks them).
 */
-- No fixed search_path, so relation names in the messages read as the caller would write them
-- (ms, not public.ms), as in every sql_saga message. Every reference is schema-qualified.
CREATE FUNCTION sql_saga.__internal_check_history_columns(table_oid oid, history_table_oid oid, without_columns name[] DEFAULT '{}')
RETURNS void
LANGUAGE plpgsql
STABLE
AS $__internal_check_history_columns$
DECLARE
    c record;
    rebuild_call text;
BEGIN
    FOR c IN
        SELECT ba.attname,
               sql_saga.__internal_column_type_sql(ba.atttypid, ba.atttypmod, ba.attcollation) AS base_type,
               ha.attname IS NOT NULL AS in_history,
               sql_saga.__internal_column_type_sql(ha.atttypid, ha.atttypmod, ha.attcollation) AS history_type
        FROM pg_catalog.pg_attribute AS ba
        LEFT JOIN pg_catalog.pg_attribute AS ha
               ON ha.attrelid = history_table_oid AND ha.attname = ba.attname AND ha.attnum > 0 AND NOT ha.attisdropped
        WHERE ba.attrelid = table_oid AND ba.attnum > 0 AND NOT ba.attisdropped
          AND ba.attname <> ALL (without_columns)
          AND (ha.attname IS NULL
               OR (ha.atttypid, ha.atttypmod, ha.attcollation) IS DISTINCT FROM (ba.atttypid, ba.atttypmod, ba.attcollation))
        ORDER BY ba.attnum
        LIMIT 1
    LOOP
        rebuild_call := pg_catalog.format('SELECT sql_saga.rebuild_system_versioning_views(%L);', sql_saga.__internal_qualified_name(table_oid));
        IF NOT c.in_history THEN
            RAISE EXCEPTION 'sql_saga: column % of base table % is missing from history table %',
                pg_catalog.quote_ident(c.attname), table_oid::pg_catalog.regclass, history_table_oid::pg_catalog.regclass
                USING ERRCODE = 'undefined_column',
                      HINT = pg_catalog.format('Run: ALTER TABLE %s ADD COLUMN %s %s; %s',
                                    sql_saga.__internal_qualified_name(history_table_oid), pg_catalog.quote_ident(c.attname), c.base_type, rebuild_call);
        END IF;
        RAISE EXCEPTION 'sql_saga: column % has type % in base table % but type % in history table %',
            pg_catalog.quote_ident(c.attname), c.base_type, table_oid::pg_catalog.regclass, c.history_type, history_table_oid::pg_catalog.regclass
            USING ERRCODE = 'datatype_mismatch',
                  HINT = pg_catalog.format('Run: ALTER TABLE %s ALTER COLUMN %s TYPE %s; %s',
                                sql_saga.__internal_qualified_name(history_table_oid), pg_catalog.quote_ident(c.attname), c.base_type, rebuild_call);
    END LOOP;
END;
$__internal_check_history_columns$;

/*
 * (Re)creates B_history_gated, B_with_history and the four as-of functions of a registered family
 * from the current columns. Callers check arguments and authorization and hold the family's lock.
 * Existing objects are dropped first (without cascading: a user object depending on them makes this
 * fail with PostgreSQL's dependency error). The drops are allowed by drop_protection and the new
 * definitions by health_checks only while sql_saga.rebuilding_system_versioning_views names this table.
 */
CREATE FUNCTION sql_saga.__internal_refresh_system_time_columns_snapshot(table_oid oid)
RETURNS void
LANGUAGE sql
SECURITY DEFINER
SET search_path = sql_saga, pg_catalog
AS $__internal_refresh_system_time_columns_snapshot$
    UPDATE sql_saga.system_time_era AS ste
    SET table_columns_snapshot = live.column_names
    FROM (
        SELECT pg_catalog.array_agg(a.attname ORDER BY a.attnum) AS column_names
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = table_oid
          AND a.attnum > 0
          AND NOT a.attisdropped
    ) AS live
    WHERE pg_catalog.to_regclass(pg_catalog.format('%I.%I', ste.table_schema, ste.table_name)) = table_oid;
$__internal_refresh_system_time_columns_snapshot$;

REVOKE ALL ON FUNCTION sql_saga.__internal_refresh_system_time_columns_snapshot(oid) FROM PUBLIC;

CREATE FUNCTION sql_saga.__internal_system_versioning_query_functions(table_oid oid)
RETURNS TABLE(role text, identity text, argument_types text, body text)
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_system_versioning_query_functions$
    SELECT q.role,
           q.identity,
           q.argument_types,
           pg_catalog.format(q.body_template, sv.view_schema_name, sv.view_table_name, e.range_column_name) AS body
    FROM sql_saga.system_versioning AS sv
    JOIN sql_saga.era AS e
      ON (e.table_schema, e.table_name, e.era_name) = (sv.table_schema, sv.table_name, sv.era_name)
    CROSS JOIN LATERAL (VALUES
        (1, 'as_of', sv.func_as_of, 'timestamp with time zone', 'SELECT * FROM %1$I.%2$I WHERE %3$I @> $1'),
        (2, 'between', sv.func_between, 'timestamp with time zone, timestamp with time zone', 'SELECT * FROM %1$I.%2$I WHERE %3$I && tstzrange($1, $2, ''[]'')'),
        (3, 'between_symmetric', sv.func_between_symmetric, 'timestamp with time zone, timestamp with time zone', 'SELECT * FROM %1$I.%2$I WHERE %3$I && tstzrange(least($1, $2), greatest($1, $2), ''[]'')'),
        (4, 'from_to', sv.func_from_to, 'timestamp with time zone, timestamp with time zone', 'SELECT * FROM %1$I.%2$I WHERE %3$I && tstzrange($1, $2)')
    ) AS q(ordinality, role, identity, argument_types, body_template)
    WHERE pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name))::oid = table_oid
    ORDER BY q.ordinality;
$__internal_system_versioning_query_functions$;

CREATE FUNCTION sql_saga.__internal_rebuild_system_versioning_views(table_oid oid, without_columns name[] DEFAULT '{}')
RETURNS void
LANGUAGE plpgsql
SET search_path = sql_saga, pg_catalog
AS $__internal_rebuild_system_versioning_views$
DECLARE
    sv sql_saga.system_versioning;
    history_table_oid oid;
    table_owner name;
    range_column name;
    gated_view_name name;
    base_projection text;
    history_projection text;
    fn record;
    previous_flag text;
    had_view boolean;
    had_gated boolean;
    had_functions text[];
    base_display text;
BEGIN
    SELECT s.* INTO sv
    FROM sql_saga.system_versioning AS s
    WHERE to_regclass(format('%I.%I', s.table_schema, s.table_name))::oid = table_oid;
    history_table_oid := to_regclass(format('%I.%I', sv.history_schema_name, sv.history_table_name))::oid;
    table_owner := (SELECT c.relowner::regrole::name FROM pg_catalog.pg_class AS c WHERE c.oid = table_oid);
    range_column := (SELECT e.range_column_name FROM sql_saga.era AS e
                     WHERE (e.table_schema, e.table_name, e.era_name) = (sv.table_schema, sv.table_name, sv.era_name));
    gated_view_name := sql_saga.__internal_history_gated_name(sv.history_table_name);

    PERFORM sql_saga.__internal_check_history_columns(table_oid, history_table_oid, without_columns);

    -- The union of both tables' columns, by name: base columns in base order, then history-only ones.
    WITH u AS (
        SELECT coalesce(ba.attname, ha.attname) AS attname, ba.attnum AS base_attnum, ha.attnum AS history_attnum,
               ha.atttypid, ha.atttypmod, ha.attcollation
        FROM (SELECT a.* FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = table_oid AND a.attnum > 0 AND NOT a.attisdropped) AS ba
        FULL JOIN (SELECT a.* FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = history_table_oid AND a.attnum > 0 AND NOT a.attisdropped) AS ha
               ON ha.attname = ba.attname
        WHERE coalesce(ba.attname, ha.attname) <> ALL (without_columns)
    )
    SELECT string_agg(CASE WHEN u.base_attnum IS NOT NULL THEN quote_ident(u.attname)
                           ELSE format('NULL::%s AS %I', sql_saga.__internal_column_type_sql(u.atttypid, u.atttypmod, u.attcollation), u.attname)
                      END, ', ' ORDER BY u.base_attnum NULLS LAST, u.history_attnum),
           string_agg(quote_ident(u.attname), ', ' ORDER BY u.base_attnum NULLS LAST, u.history_attnum)
    INTO base_projection, history_projection
    FROM u;

    previous_flag := current_setting('sql_saga.rebuilding_system_versioning_views', true);
    base_display := sql_saga.__internal_notice_name(sv.table_schema, sv.table_name);
    had_view := to_regclass(format('%I.%I', sv.view_schema_name, sv.view_table_name)) IS NOT NULL;
    had_gated := to_regclass(format('%I.%I', sv.history_schema_name, gated_view_name)) IS NOT NULL;
    had_functions := ARRAY(SELECT u.f FROM unnest(ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]) AS u (f)
                           WHERE to_regprocedure(u.f) IS NOT NULL);
    PERFORM set_config('sql_saga.rebuilding_system_versioning_views', table_oid::text, true);
    BEGIN
        FOR fn IN
            SELECT u.f FROM unnest(ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]) AS u (f)
            WHERE to_regprocedure(u.f) IS NOT NULL
        LOOP
            EXECUTE format('DROP ROUTINE %s', fn.f::regprocedure);
        END LOOP;
        -- add_system_versioning calls this before the views exist.
        IF to_regclass(format('%I.%I', sv.view_schema_name, sv.view_table_name)) IS NOT NULL THEN
            EXECUTE format('DROP VIEW %I.%I', sv.view_schema_name, sv.view_table_name);
        END IF;
        IF to_regclass(format('%I.%I', sv.history_schema_name, gated_view_name)) IS NOT NULL THEN
            EXECUTE format('DROP VIEW %I.%I', sv.history_schema_name, gated_view_name);
        END IF;

        EXECUTE format('CREATE VIEW %I.%I AS %s', sv.history_schema_name, gated_view_name,
            sql_saga.__internal_history_gated_view_sql(table_oid, history_table_oid, history_projection));
        EXECUTE format('ALTER VIEW %I.%I OWNER TO %I', sv.history_schema_name, gated_view_name, table_owner);
        EXECUTE format('GRANT SELECT ON TABLE %I.%I TO PUBLIC', sv.history_schema_name, gated_view_name);
        IF NOT had_gated THEN
            RAISE NOTICE 'sql_saga: Created view "%" for system-versioned table "%"',
                sql_saga.__internal_notice_name(sv.history_schema_name, gated_view_name), base_display;
        END IF;

        EXECUTE format($view$CREATE VIEW %1$I.%2$I WITH (security_invoker = on) AS
        SELECT %3$s FROM %4$s
        UNION ALL SELECT %5$s FROM %6$I.%7$I
        UNION ALL SELECT %5$s FROM %8$s
            WHERE pg_catalog.row_security_active(%9$L::regclass::oid)
              AND sql_saga.__internal_history_policies_synchronized(%9$L::regclass::oid, %10$L::regclass::oid)$view$,
            sv.view_schema_name, /* %1$I */
            sv.view_table_name, /* %2$I */
            base_projection, /* %3$s */
            sql_saga.__internal_qualified_name(table_oid), /* %4$s */
            history_projection, /* %5$s */
            sv.history_schema_name, /* %6$I */
            gated_view_name, /* %7$I */
            sql_saga.__internal_qualified_name(history_table_oid), /* %8$s */
            sql_saga.__internal_qualified_name(table_oid), /* %9$L */
            sql_saga.__internal_qualified_name(history_table_oid) /* %10$L */
        );
        EXECUTE format('ALTER VIEW %I.%I OWNER TO %I', sv.view_schema_name, sv.view_table_name, table_owner);
        EXECUTE format('GRANT SELECT ON TABLE %I.%I TO PUBLIC', sv.view_schema_name, sv.view_table_name);
        IF NOT had_view THEN
            RAISE NOTICE 'sql_saga: Created view "%" for system-versioned table "%"',
                sql_saga.__internal_notice_name(sv.view_schema_name, sv.view_table_name), base_display;
        END IF;

        -- Each registered signature names a function; its body is fixed by its role.
        FOR fn IN
            SELECT q.identity AS f, q.body
            FROM sql_saga.__internal_system_versioning_query_functions(table_oid) AS q
        LOOP
            EXECUTE format('CREATE FUNCTION %s RETURNS SETOF %I.%I LANGUAGE sql STABLE AS %L',
                fn.f, sv.view_schema_name, sv.view_table_name,
                fn.body);
            EXECUTE format('ALTER FUNCTION %s OWNER TO %I', fn.f::regprocedure, table_owner);
            -- PUBLIC EXECUTE is explicit, because hardened databases revoke the default.
            EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO PUBLIC', fn.f::regprocedure);
            IF fn.f <> ALL (had_functions) THEN
                RAISE NOTICE 'sql_saga: Created function "%" for system-versioned table "%"',
                    sql_saga.__internal_notice_function_name(fn.f::regprocedure), base_display;
            END IF;
        END LOOP;

        -- The fixed ACL is PUBLIC plus the owner. New objects can receive more through
        -- ALTER DEFAULT PRIVILEGES; take those away, so a rebuild always ends in the fixed state.
        FOR fn IN
            SELECT format('REVOKE ALL ON %s %s FROM %s',
                          CASE WHEN o.is_function THEN 'FUNCTION' ELSE 'TABLE' END, o.ident,
                          string_agg(DISTINCT quote_ident(x.grantee::regrole::text), ', ')) AS stmt
            FROM (SELECT false AS is_function, c.oid::regclass::text AS ident, c.relacl AS acl, c.relowner AS owner
                  FROM pg_catalog.pg_class AS c
                  WHERE c.oid IN (to_regclass(format('%I.%I', sv.view_schema_name, sv.view_table_name)),
                                  to_regclass(format('%I.%I', sv.history_schema_name, gated_view_name)))
                  UNION ALL
                  SELECT true, p.oid::regprocedure::text, p.proacl, p.proowner
                  FROM pg_catalog.pg_proc AS p
                  WHERE p.oid = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]::regprocedure[])
                 ) AS o
            CROSS JOIN LATERAL aclexplode(o.acl) AS x
            WHERE x.grantee NOT IN (0, o.owner)
            GROUP BY o.is_function, o.ident
        LOOP
            EXECUTE fn.stmt;
        END LOOP;
    EXCEPTION WHEN OTHERS THEN
        PERFORM set_config('sql_saga.rebuilding_system_versioning_views', coalesce(previous_flag, ''), true);
        RAISE;
    END;
    PERFORM set_config('sql_saga.rebuilding_system_versioning_views', coalesce(previous_flag, ''), true);

    RAISE DEBUG 'sql_saga: generated SYSTEM VERSIONING views and query functions for table %', table_oid::regclass;
END;
$__internal_rebuild_system_versioning_views$;

CREATE FUNCTION sql_saga.__internal_reconcile_system_versioning_family(
    table_oid regclass,
    without_columns name[] DEFAULT '{}')
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = sql_saga, pg_catalog
AS $__internal_reconcile_system_versioning_family$
DECLARE
    sv sql_saga.system_versioning;
    history_table_oid oid;
    table_owner name;
    acl record;
    needs_rebuild boolean;
    expected_columns integer;
    actual_columns integer;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'sql_saga: cannot reconcile a NULL table'
            USING ERRCODE = 'null_value_not_allowed',
                  HINT = 'Pass a registered base table to sql_saga.__internal_reconcile_system_versioning_family().';
    END IF;
    PERFORM sql_saga.__internal_serialize(table_oid);
    SELECT s.* INTO sv FROM sql_saga.system_versioning AS s
    WHERE pg_catalog.to_regclass(pg_catalog.format('%I.%I', s.table_schema, s.table_name)) OPERATOR(pg_catalog.=) table_oid;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'sql_saga: table % does not have SYSTEM VERSIONING', table_oid
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;
    IF current_setting('sql_saga.rebuilding_system_versioning_views', true) OPERATOR(pg_catalog.=) table_oid::oid::text THEN
        PERFORM sql_saga.__internal_sync_history_policies(table_oid);
        PERFORM sql_saga.__internal_refresh_system_time_columns_snapshot(table_oid);
        RETURN;
    END IF;
    history_table_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.history_schema_name, sv.history_table_name))::oid;
    IF history_table_oid IS NULL THEN
        RAISE EXCEPTION 'sql_saga: history table %.% registered for % does not exist',
            pg_catalog.quote_ident(sv.history_schema_name), pg_catalog.quote_ident(sv.history_table_name), table_oid
            USING ERRCODE = 'undefined_table', HINT = 'Restore the registered history table before retrying.';
    END IF;
    -- Preserve history-only columns outside canonical views. The public rebuild API calls the
    -- generator directly when an operator explicitly wants those columns included.
    SELECT coalesce(without_columns, '{}'::name[]) || coalesce(pg_catalog.array_agg(a.attname ORDER BY a.attnum), '{}'::name[])
    INTO without_columns
    FROM pg_catalog.pg_attribute AS a
    WHERE a.attrelid = history_table_oid AND a.attnum > 0 AND NOT a.attisdropped
      AND NOT EXISTS (
          SELECT FROM pg_catalog.pg_attribute AS b
          WHERE b.attrelid = table_oid AND b.attnum > 0 AND NOT b.attisdropped
            AND b.attname OPERATOR(pg_catalog.=) a.attname)
      AND a.attname <> ALL (coalesce(without_columns, '{}'::name[]));
    BEGIN
      EXECUTE pg_catalog.format('LOCK TABLE %s, %s IN ACCESS EXCLUSIVE MODE', table_oid, history_table_oid::regclass);
      table_owner := (SELECT c.relowner::regrole::name FROM pg_catalog.pg_class AS c WHERE c.oid = table_oid);
      IF (SELECT c.relowner::regrole::name FROM pg_catalog.pg_class AS c WHERE c.oid = history_table_oid) <> table_owner THEN
          EXECUTE pg_catalog.format('ALTER TABLE %s OWNER TO %I', history_table_oid::regclass, table_owner);
      END IF;
      IF NOT (SELECT c.relrowsecurity FROM pg_catalog.pg_class AS c WHERE c.oid = history_table_oid) THEN
          EXECUTE pg_catalog.format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', history_table_oid::regclass);
      END IF;
      FOR acl IN
        SELECT pg_catalog.format('REVOKE ALL ON TABLE %s FROM %s', history_table_oid::regclass,
                                 pg_catalog.string_agg(DISTINCT pg_catalog.quote_ident(x.grantee::regrole::text), ', ')) AS stmt
        FROM pg_catalog.pg_class AS c CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) AS x
        WHERE c.oid = history_table_oid AND x.grantee NOT IN (0, c.relowner) GROUP BY c.oid
      LOOP
          EXECUTE acl.stmt;
      END LOOP;
      IF NOT EXISTS (
          SELECT FROM pg_catalog.pg_class AS c
          CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) AS x
          WHERE c.oid = history_table_oid AND x.grantee = 0 AND x.privilege_type = 'SELECT') THEN
          EXECUTE pg_catalog.format('GRANT SELECT ON TABLE %s TO PUBLIC', history_table_oid::regclass);
      END IF;

      SELECT count(*) INTO expected_columns
      FROM (
          SELECT a.attname FROM pg_catalog.pg_attribute AS a
          WHERE a.attrelid = table_oid AND a.attnum > 0 AND NOT a.attisdropped
            AND a.attname <> ALL (coalesce(without_columns, '{}'))
          UNION
          SELECT a.attname FROM pg_catalog.pg_attribute AS a
          WHERE a.attrelid = history_table_oid AND a.attnum > 0 AND NOT a.attisdropped
            AND a.attname <> ALL (coalesce(without_columns, '{}'))
      ) AS expected;
      SELECT count(*) INTO actual_columns
      FROM pg_catalog.pg_attribute AS a
      WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.view_schema_name, sv.view_table_name))
        AND a.attnum > 0 AND NOT a.attisdropped;
      needs_rebuild := pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.history_schema_name,
                           sql_saga.__internal_history_gated_name(sv.history_table_name))) IS NULL
          OR pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.view_schema_name, sv.view_table_name)) IS NULL
          OR expected_columns IS DISTINCT FROM actual_columns
          OR NOT EXISTS (
              SELECT FROM pg_catalog.pg_class AS c
              WHERE c.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.view_schema_name, sv.view_table_name))
                AND 'security_invoker=on' = ANY (coalesce(c.reloptions, '{}')))
          OR EXISTS (
              SELECT FROM unnest(ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]) AS f(fn)
              WHERE pg_catalog.to_regprocedure(f.fn) IS NULL);
      IF needs_rebuild THEN
          PERFORM sql_saga.__internal_rebuild_system_versioning_views(table_oid, coalesce(without_columns, '{}'));
          PERFORM sql_saga.__internal_refresh_system_time_columns_snapshot(table_oid);
      END IF;
      PERFORM sql_saga.__internal_sync_history_policies(table_oid);
    END;
END;
$__internal_reconcile_system_versioning_family$;

CREATE FUNCTION sql_saga.add_system_versioning(
    table_oid regclass,
    history_table_name name DEFAULT NULL,
    view_name name DEFAULT NULL,
    function_as_of_name name DEFAULT NULL,
    function_between_name name DEFAULT NULL,
    function_between_symmetric_name name DEFAULT NULL,
    function_from_to_name name DEFAULT NULL,
    history_table_must_exist boolean DEFAULT false)
 RETURNS void
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, public
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
    history_row_count bigint;
    other_schema name;
    other_family record;
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
        RAISE EXCEPTION 'table "%" must be persistent', sql_saga.__internal_display_name(table_oid);
    END IF;

    /*
     * Validate an existing history table before anything is created, so a refusal leaves no trace:
     * missing (with history_table_must_exist), already another family's history, or carrying an era.
     */
    history_table_name := coalesce(history_table_name, sql_saga.__internal_make_name(ARRAY[table_name], 'history'));

    SELECT c.oid INTO history_table_id
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE (n.nspname, c.relname) = (table_schema, history_table_name);

    IF history_table_id IS NULL THEN
        IF history_table_must_exist THEN
            SELECT n.nspname INTO other_schema
            FROM pg_catalog.pg_class AS c
            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
            WHERE c.relname = history_table_name AND c.relkind IN ('r', 'p')
              AND n.nspname <> table_schema
              AND n.nspname NOT LIKE 'pg\_%' AND n.nspname <> 'information_schema'
            ORDER BY n.nspname
            LIMIT 1;
            IF other_schema IS NOT NULL THEN
                RAISE EXCEPTION 'history table "%" does not exist, and history_table_must_exist is true',
                    sql_saga.__internal_display_name(format('%I.%I', table_schema, history_table_name))
                USING ERRCODE = 'undefined_table',
                      HINT = format('Table "%s" has that name in another schema. Move it first: ALTER TABLE %I.%I SET SCHEMA %I;',
                          sql_saga.__internal_display_name(format('%I.%I', other_schema, history_table_name)),
                          other_schema, history_table_name, table_schema);
            END IF;
            RAISE EXCEPTION 'history table "%" does not exist, and history_table_must_exist is true',
                sql_saga.__internal_display_name(format('%I.%I', table_schema, history_table_name))
            USING ERRCODE = 'undefined_table';
        END IF;
    ELSE
        SELECT sv.table_schema, sv.table_name INTO other_family
        FROM sql_saga.system_versioning AS sv
        WHERE (sv.history_schema_name, sv.history_table_name) = (table_schema, history_table_name);
        IF FOUND THEN
            RAISE EXCEPTION 'table "%" is already the history table of system-versioned table "%"',
                sql_saga.__internal_display_name(history_table_id::regclass),
                sql_saga.__internal_display_name(format('%I.%I', other_family.table_schema, other_family.table_name))
            USING ERRCODE = 'object_in_use';
        END IF;
        IF EXISTS (SELECT 1 FROM sql_saga.era AS e WHERE (e.table_schema, e.table_name) = (table_schema, history_table_name)) THEN
            RAISE EXCEPTION 'history tables for SYSTEM VERSIONING cannot have eras';
        END IF;
        /*
         * Compare with the base table as it will be once versioned: the system time era adds
         * system_valid_range when the base lacks it. Checked here, before any change.
         */
        IF EXISTS (
            WITH
            L (attname, atttypid, atttypmod, attcollation) AS (
                SELECT a.attname, a.atttypid, a.atttypmod, a.attcollation FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = table_oid AND NOT a.attisdropped
                UNION ALL
                SELECT 'system_valid_range'::name, 'pg_catalog.tstzrange'::pg_catalog.regtype::oid, -1, 0::oid
                WHERE NOT EXISTS (SELECT FROM sql_saga.era AS e WHERE (e.table_schema, e.table_name, e.era_name) = (table_schema, table_name, 'system_time'))
                  AND NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE (a.attrelid, a.attname) = (table_oid, 'system_valid_range') AND NOT a.attisdropped)),
            R (attname, atttypid, atttypmod, attcollation) AS (SELECT a.attname, a.atttypid, a.atttypmod, a.attcollation FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = history_table_id AND NOT a.attisdropped)
            SELECT FROM L NATURAL FULL JOIN R WHERE L.attname IS NULL OR R.attname IS NULL)
        THEN
            RAISE EXCEPTION 'base table "%" and history table "%" are not compatible', sql_saga.__internal_display_name(table_oid), sql_saga.__internal_display_name(history_table_id::regclass);
        END IF;
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

    view_name := coalesce(view_name, sql_saga.__internal_make_name(ARRAY[table_name], 'with_history'));
    function_as_of_name := coalesce(function_as_of_name, sql_saga.__internal_make_name(ARRAY[table_name], '_as_of'));
    function_between_name := coalesce(function_between_name, sql_saga.__internal_make_name(ARRAY[table_name], '_between'));
    function_between_symmetric_name := coalesce(function_between_symmetric_name, sql_saga.__internal_make_name(ARRAY[table_name], '_between_symmetric'));
    function_from_to_name := coalesce(function_from_to_name, sql_saga.__internal_make_name(ARRAY[table_name], '_from_to'));

    IF history_table_id IS NOT NULL THEN
        EXECUTE format('ALTER TABLE %s OWNER TO %I', history_table_id::regclass /* %s */, table_owner /* %I */);
        EXECUTE format('SELECT count(*) FROM %s', history_table_id::regclass /* %s */) INTO history_row_count;
        RAISE NOTICE 'sql_saga: Adopted existing table "%" (% rows) as the history table of system-versioned table "%"',
            sql_saga.__internal_display_name(history_table_id::regclass), history_row_count,
            sql_saga.__internal_display_name(table_oid);
    ELSE
        EXECUTE format('CREATE TABLE %1$I.%2$I (LIKE %1$I.%3$I)', table_schema /* %1$I */, history_table_name /* %2$I */, table_name /* %3$I */);
        history_table_id := format('%I.%I', table_schema /* %I */, history_table_name /* %I */)::regclass;
        EXECUTE format('ALTER TABLE %1$I.%2$I OWNER TO %3$I', table_schema /* %1$I */, history_table_name /* %2$I */, table_owner /* %3$I */);
        RAISE NOTICE 'sql_saga: Created history table "%" for system-versioned table "%"',
            sql_saga.__internal_notice_name(table_schema, history_table_name), sql_saga.__internal_notice_name(table_schema, table_name);
        RAISE DEBUG 'history table "%" created for "%", be sure to index it properly', history_table_id::regclass, table_oid;
    END IF;

    /*
     * Access-time authority (tmp/design_final.md): the history objects are granted to PUBLIC and
     * hold no copy of the base table's ACL. Callers are checked against the base when they read:
     *   - B_history_gated (owner-rights) raises 42501 unless the caller may SELECT B, and serves
     *     callers that B's row-level security does not apply to;
     *   - B_history is read as the caller when B's RLS applies, filtered by the copies of B's
     *     policies (sql_saga.__internal_sync_history_policies), after checking they are in sync.
     */
    EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', history_table_id::regclass);
    EXECUTE format('GRANT SELECT ON TABLE %s TO PUBLIC', history_table_id::regclass);

    -- Registered first: the generator reads the family's names from the catalog.
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

    -- B_history_gated, B_with_history and the as-of functions: the same generator as
    -- rebuild_system_versioning_views, so a rebuild of an unchanged family reproduces them exactly.
    PERFORM sql_saga.__internal_reconcile_system_versioning_family(table_oid, '{}');
END;
$function$;

COMMENT ON FUNCTION sql_saga.add_system_versioning(regclass, name, name, name, name, name, name, boolean) IS
'Adds system versioning to a table, creating a history table and triggers to automatically track all data changes. An existing table named history_table_name in the table''s schema is adopted as the history (reported by a NOTICE); history_table_must_exist => true requires it, as when moving a system-versioned table.';

/*
 * The as-of functions of a versioned table are executable by PUBLIC (tmp/design_final.md section 5).
 * add_system_versioning grants it explicitly, because hardened databases revoke EXECUTE from PUBLIC by
 * default. pg_dump writes no GRANT for a function ACL equal to the default, so a pg_restore --clean into
 * a database where those default privileges already apply recreates the functions owner-only. The
 * catalog row is loaded after the functions exist, so granting when it is inserted keeps the rule after
 * any restore. Runs as the inserting role: add_system_versioning (the extension owner) or pg_restore.
 */
CREATE FUNCTION sql_saga.__internal_grant_query_functions()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = sql_saga, pg_catalog
AS $__internal_grant_query_functions$
DECLARE
    query_function regprocedure;
BEGIN
    FOR query_function IN
        SELECT to_regprocedure(u.signature)
        FROM unnest(ARRAY[NEW.func_as_of, NEW.func_between, NEW.func_between_symmetric, NEW.func_from_to]) AS u (signature)
        WHERE to_regprocedure(u.signature) IS NOT NULL
    LOOP
        EXECUTE format('GRANT EXECUTE ON FUNCTION %s TO PUBLIC', query_function /* %s */);
    END LOOP;
    EXECUTE format('GRANT SELECT ON TABLE %I.%I TO PUBLIC', NEW.history_schema_name, NEW.history_table_name);
    RETURN NULL;
END;
$__internal_grant_query_functions$;

CREATE TRIGGER system_versioning_grant_query_functions
AFTER INSERT ON sql_saga.system_versioning
FOR EACH ROW EXECUTE FUNCTION sql_saga.__internal_grant_query_functions();

/*
 * Manual schema changes (tmp/design_manual_schema_changes.md): after the owner changed the base and
 * history tables by hand, this recreates the views and as-of functions from the current columns.
 *
 * SECURITY DEFINER lets this checked public gateway use the private canonical generator. The
 * effective caller is recovered from role/session_user and must be a member of the base owner.
 * The fixed path deliberately includes public so regclass names retain the v1.1 caller-visible
 * qualification used by existing messages. Every relation and function reference in this body is
 * schema-qualified, so public is used only for regclass display.
 */
CREATE FUNCTION sql_saga.rebuild_system_versioning_views(
    table_oid regclass,
    without_columns name[] DEFAULT '{}')
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $rebuild_system_versioning_views$
DECLARE
    sv sql_saga.system_versioning;
    history_table_oid oid;
    range_column name;
    excluded name;
    previous_rebuild_flag text;
    caller_role name;
    authorization_table_name name;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    SELECT c.relname INTO authorization_table_name FROM pg_catalog.pg_class AS c
    WHERE c.oid OPERATOR(pg_catalog.=) table_oid;
    caller_role := CASE WHEN current_setting('role') = 'none' THEN session_user ELSE current_setting('role') END;
    IF NOT pg_catalog.pg_has_role(caller_role, (SELECT c.relowner FROM pg_catalog.pg_class AS c WHERE c.oid OPERATOR(pg_catalog.=) table_oid), 'USAGE') THEN
        RAISE EXCEPTION 'must be owner of table %', authorization_table_name USING ERRCODE = 'insufficient_privilege';
    END IF;

    PERFORM sql_saga.__internal_serialize(table_oid);

    SELECT s.* INTO sv
    FROM sql_saga.system_versioning AS s
    WHERE pg_catalog.to_regclass(pg_catalog.format('%I.%I', s.table_schema, s.table_name)) OPERATOR(pg_catalog.=) table_oid;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'table % does not have SYSTEM VERSIONING', table_oid;
    END IF;

    history_table_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.history_schema_name, sv.history_table_name))::pg_catalog.oid;
    range_column := (SELECT e.range_column_name FROM sql_saga.era AS e
                     WHERE (e.table_schema, e.table_name, e.era_name) OPERATOR(pg_catalog.=) (sv.table_schema, sv.table_name, sv.era_name));

    without_columns := coalesce(without_columns, '{}');
    IF pg_catalog.array_position(without_columns, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'sql_saga: without_columns must not contain NULL';
    END IF;
    FOR excluded IN
        SELECT u.c FROM pg_catalog.unnest(without_columns) WITH ORDINALITY AS u (c, n)
        GROUP BY u.c HAVING pg_catalog.count(*) OPERATOR(pg_catalog.>) 1 ORDER BY pg_catalog.min(u.n)
    LOOP
        RAISE EXCEPTION 'sql_saga: column % is listed more than once in without_columns', pg_catalog.quote_ident(excluded);
    END LOOP;
    FOR excluded IN
        SELECT u.c FROM pg_catalog.unnest(without_columns) WITH ORDINALITY AS u (c, n)
        WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a
                          WHERE a.attrelid IN (table_oid, history_table_oid) AND a.attname OPERATOR(pg_catalog.=) u.c
                            AND a.attnum OPERATOR(pg_catalog.>) 0 AND NOT a.attisdropped)
        ORDER BY u.n
    LOOP
        RAISE EXCEPTION 'sql_saga: column % does not exist in either % or %',
            pg_catalog.quote_ident(excluded), table_oid, history_table_oid::pg_catalog.regclass;
    END LOOP;
    IF range_column OPERATOR(pg_catalog.=) ANY (without_columns) THEN
        RAISE EXCEPTION 'sql_saga: cannot exclude system time column % from SYSTEM VERSIONING views for table %',
            pg_catalog.quote_ident(range_column), table_oid;
    END IF;

    -- Checked here, in the caller's search_path, so the error names relations as the writer's does.
    PERFORM sql_saga.__internal_check_history_columns(table_oid, history_table_oid, without_columns);

    PERFORM sql_saga.__internal_rebuild_system_versioning_views(table_oid, without_columns);

    -- Use the existing authorized reconciler as the private refresh gateway.
    previous_rebuild_flag := current_setting('sql_saga.rebuilding_system_versioning_views', true);
    PERFORM set_config('sql_saga.rebuilding_system_versioning_views', table_oid::oid::text, true);
    BEGIN
        PERFORM sql_saga.__internal_reconcile_system_versioning_family(table_oid, without_columns);
    EXCEPTION WHEN OTHERS THEN
        PERFORM set_config('sql_saga.rebuilding_system_versioning_views', coalesce(previous_rebuild_flag, ''), true);
        RAISE;
    END;
    PERFORM set_config('sql_saga.rebuilding_system_versioning_views', coalesce(previous_rebuild_flag, ''), true);

    RAISE NOTICE 'sql_saga: Rebuilt SYSTEM VERSIONING views and query functions for table %', table_oid;
END;
$rebuild_system_versioning_views$;

COMMENT ON FUNCTION sql_saga.rebuild_system_versioning_views(regclass, name[]) IS
'Rebuilds the views and as-of query functions of a system-versioned table from the current base and history columns. Columns are matched by name; history may have additional columns. without_columns temporarily omits named columns so dependent columns can be changed manually.';
