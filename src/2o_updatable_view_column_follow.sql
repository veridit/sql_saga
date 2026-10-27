/*
 * Managed updatable views follow the column shape of their base table (1.3, A3).
 *
 * add_for_portion_of_view and add_current_view create views whose column list is frozen at creation
 * (`AS TABLE t`, `SELECT * FROM t`). When the base table changes shape:
 *
 *   ADD COLUMN    (ddl_command_end, rename_following): the new columns are appended to the base, so the
 *                 view is extended in place with CREATE OR REPLACE VIEW, repeating its current view options
 *                 (security_barrier, check_option, ...). It keeps its OID, so grants, comments, rules,
 *                 triggers and user views built on it are untouched. Inside an extension script (CREATE
 *                 EXTENSION, ALTER EXTENSION ... UPDATE) PostgreSQL refuses CREATE OR REPLACE of a view
 *                 that extension does not own, so there the view is dropped and recreated as for DROP
 *                 COLUMN below: a new OID, with the same grants, comments, column defaults, triggers, view
 *                 options and extension membership. The same holds when follow_shape repairs a stale view
 *                 (an ADD COLUMN that was not followed), as the 1.3 update script does. A user view or rule
 *                 on the managed view then makes the ALTER TABLE fail, as for DROP COLUMN.
 *   DROP COLUMN   of a column the view shows and sql_saga does not manage: a view cannot lose a column in
 *                 place, and PostgreSQL refuses the DROP COLUMN while the view references it. So at
 *                 ddl_command_start (C hook in refuse_managed_relation_identity_change, run as the
 *                 extension owner) the facts of the managed view (grants, column grants, comments, column
 *                 defaults, view options, trigger definitions) are recorded in
 *                 sql_saga.__internal_updatable_view_pending and the view is dropped together with its
 *                 catalog row; at ddl_command_end it is recreated from those facts without the column, owned
 *                 by the table owner. A user view, rule or other object depending on the managed view would
 *                 be lost by that, so the DROP COLUMN is then refused before anything changes.
 *                 A DROP COLUMN of a column sql_saga manages is left alone here: the view's dependency on
 *                 it makes PostgreSQL (or, with CASCADE, drop_protection) refuse it as before.
 *   RENAME COLUMN the view columns are renamed in place, the identifier arguments of the INSTEAD OF
 *                 trigger and updatable_view.ephemeral_columns follow.
 *   ALTER COLUMN TYPE (ddl_command_start, C hook, __internal_updatable_view_prepare_alter_type): first the
 *                 refusals, before anything changes and whether or not a view exists. The type of an era's
 *                 range column or of its boundary columns (valid_from, valid_until, valid_to) is bound into
 *                 the era catalog, the synchronization trigger and the bounds constraints, so it is refused,
 *                 with the public migration as HINT. A column a sql_saga foreign key uses, on either side,
 *                 is refused, naming the foreign keys: the other table would have to change with it, and
 *                 sql_saga never alters it. Then the managed views that show a changed column (on the
 *                 table and the inheritance descendants the change recurses to) are recorded and dropped as
 *                 for DROP COLUMN and recreated at ddl_command_end with the new types. A user object on such
 *                 a view refuses the statement, as for DROP COLUMN. Any other column, including an
 *                 identifier no foreign key uses, simply follows. When the same statement also drops a
 *                 column sql_saga manages, the views are left in place so that PostgreSQL refuses it.
 *
 * The NOTICE of a recreated view names exactly what changed: the columns it no longer shows, the columns
 * that have a new type and the columns that are new.
 *
 * All paths recompute sql_saga.updatable_view.generated_columns from the base table.
 *
 * The pending state is a table, not a setting: only these SECURITY DEFINER functions can read or write
 * it, so nothing the session user controls is ever executed. It is transactional, so a failed or
 * rolled-back ALTER TABLE takes its rows with it, and every row is consumed by the ddl_command_end of the
 * statement that wrote it, or by the next statement in the same transaction that fires rename_following.
 * The two event triggers (sql_saga_refuse_managed_relation_identity_change and sql_saga_rename_following)
 * must be disabled together: with only the second one disabled, a DROP COLUMN leaves the view dropped
 * and its row behind, which check_integrity reports with the repair.
 *
 * A recreated view keeps exactly the extension membership the dropped one had: a DROP COLUMN run by
 * another extension's script (CREATE EXTENSION, ALTER EXTENSION ... UPDATE) would otherwise make the
 * user's view a member of that extension, and pg_dump would omit it.
 */

CREATE TABLE sql_saga.__internal_updatable_view_pending (
    transaction_id bigint NOT NULL DEFAULT pg_catalog.pg_current_xact_id()::text::bigint,
    view_schema name NOT NULL,
    view_name name NOT NULL,
    view_type sql_saga.updatable_view_type NOT NULL,
    table_schema name NOT NULL,
    table_name name NOT NULL,
    era_name name NOT NULL,
    trigger_name name NOT NULL,
    current_func text,
    ephemeral_columns name[],
    -- A current view's bound clock (sql_saga.__internal_current_view_clock), read from the view's rule before the drop.
    view_clock text,
    -- Facts of the dropped view, read from the catalogs by __internal_updatable_view_record_and_drop.
    view_acl aclitem[],
    view_comment text,
    view_reloptions text[],
    view_columns name[] NOT NULL,      -- The view's columns before the drop, in order.
    view_column_types text[] NOT NULL, -- Their types (format_type), in the same order, for the NOTICE.
    view_extension name,               -- The extension the view was a member of (NULL: none).
    view_delete_mode name,             -- A current view's delete mode (first trigger argument), for repair hints.
    column_facts jsonb NOT NULL,       -- [{name, acl (aclitem[] as text[]), comment, default (pg_get_expr)}]
    trigger_facts jsonb NOT NULL,      -- [{name, definition (pg_get_triggerdef), enabled, comment}]
    PRIMARY KEY (transaction_id, view_schema, view_name)
);
REVOKE ALL ON TABLE sql_saga.__internal_updatable_view_pending FROM PUBLIC;
COMMENT ON TABLE sql_saga.__internal_updatable_view_pending IS
'Internal: managed updatable views dropped at ddl_command_start of an ALTER TABLE ... DROP COLUMN, recreated and removed at its ddl_command_end. Only sql_saga''s own functions use it; never dumped.';

/*
 * The CREATE [OR REPLACE] VIEW statement of add_for_portion_of_view / add_current_view, with the given
 * view options (reloptions; NULL: the options those functions use). security_invoker = on is always kept.
 * A current view filters by clock: the bound clock of the view being rebuilt (sql_saga.__internal_current_view_clock),
 * never uv.current_func, the text as registered, which names the function only under add_current_view's search_path.
 */
CREATE FUNCTION sql_saga.__internal_updatable_view_create_sql(uv sql_saga.updatable_view, or_replace boolean, reloptions text[], clock text)
RETURNS text
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, pg_temp
AS $__internal_updatable_view_create_sql$
DECLARE
    v_era sql_saga.era;
    v_options text[];
    v_with text;
BEGIN
    SELECT e.* INTO v_era
    FROM sql_saga.era AS e
    WHERE (e.table_schema, e.table_name, e.era_name) = (uv.table_schema, uv.table_name, uv.era_name);

    v_options := COALESCE(reloptions,
        CASE WHEN uv.view_type = 'current'
             THEN ARRAY['security_barrier=true', 'security_invoker=on']
             ELSE ARRAY['security_invoker=on'] END);
    v_options := ARRAY(SELECT o.opt FROM pg_catalog.unnest(v_options) WITH ORDINALITY AS o(opt, n)
                       WHERE pg_catalog.split_part(o.opt, '=', 1) <> 'security_invoker'
                       ORDER BY o.n)
                 || ARRAY['security_invoker=on'];
    SELECT pg_catalog.string_agg(pg_catalog.format('%I = %L',
               pg_catalog.split_part(o.opt, '=', 1),
               pg_catalog.substr(o.opt, pg_catalog.strpos(o.opt, '=') + 1)), ', ' ORDER BY o.n)
    INTO v_with
    FROM pg_catalog.unnest(v_options) WITH ORDINALITY AS o(opt, n);

    -- The statements of add_for_portion_of_view and add_current_view.
    IF uv.view_type = 'current' AND clock IS NULL THEN
        RAISE EXCEPTION 'sql_saga: cannot rebuild current view "%.%" without its bound clock', uv.view_schema, uv.view_name;
    END IF;
    IF uv.view_type = 'for_portion_of' THEN
        RETURN pg_catalog.format('CREATE %4$sVIEW %1$I.%2$I WITH (%5$s) AS TABLE %1$I.%3$I',
            uv.view_schema /* %1$I */, uv.view_name /* %2$I */, uv.table_name /* %3$I */,
            CASE WHEN or_replace THEN 'OR REPLACE ' ELSE '' END /* %4$s */, v_with /* %5$s */);
    END IF;

    RETURN pg_catalog.format('CREATE %7$sVIEW %1$I.%2$I WITH (%8$s) AS SELECT * FROM %1$I.%3$I WHERE %4$I @> %5$s::%6$s',
        uv.view_schema /* %1$I */, uv.view_name /* %2$I */, uv.table_name /* %3$I */,
        v_era.range_column_name /* %4$I */, clock /* %5$s */, v_era.range_subtype /* %6$s */,
        CASE WHEN or_replace THEN 'OR REPLACE ' ELSE '' END /* %7$s */, v_with /* %8$s */);
END;
$__internal_updatable_view_create_sql$;

/* GENERATED ... STORED and GENERATED ALWAYS AS IDENTITY columns: never written through the views. */
CREATE FUNCTION sql_saga.__internal_updatable_view_generated_columns(table_oid oid)
RETURNS name[]
LANGUAGE sql
STABLE
SET search_path = pg_catalog, pg_temp
AS $__internal_updatable_view_generated_columns$
    SELECT COALESCE(pg_catalog.array_agg(pa.attname ORDER BY pa.attnum), '{}')
    FROM pg_catalog.pg_attribute AS pa
    WHERE pa.attrelid = table_oid
      AND pa.attnum > 0
      AND NOT pa.attisdropped
      AND (pa.attgenerated <> '' OR pa.attidentity = 'a');
$__internal_updatable_view_generated_columns$;

/*
 * The first object a drop and recreate of the view would lose, as a pg_describe_object text, or NULL:
 * an object depending on the view (a user view, a materialized view, a function using its row type, ...)
 * or a user rule on the view itself. The view's own _RETURN rule, triggers, column defaults and row types
 * are recreated with it and do not count.
 */
CREATE FUNCTION sql_saga.__internal_updatable_view_foreign_dependent(view_oid oid)
RETURNS text
LANGUAGE sql
STABLE
SET search_path = pg_catalog, pg_temp
AS $__internal_updatable_view_foreign_dependent$
    WITH own_type AS (
        SELECT t.oid FROM pg_catalog.pg_type AS t
        WHERE t.typrelid = view_oid
        UNION ALL
        SELECT ta.oid FROM pg_catalog.pg_type AS t
        JOIN pg_catalog.pg_type AS ta ON ta.typelem = t.oid AND ta.oid = t.typarray
        WHERE t.typrelid = view_oid
    )
    SELECT dep.dependent FROM (
        SELECT pg_catalog.pg_describe_object(
                   CASE WHEN d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass THEN 'pg_catalog.pg_class'::pg_catalog.regclass ELSE d.classid END,
                   CASE WHEN d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass THEN rw.ev_class ELSE d.objid END,
                   CASE WHEN d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass THEN 0 ELSE d.objsubid END) AS dependent
        FROM pg_catalog.pg_depend AS d
        LEFT JOIN pg_catalog.pg_rewrite AS rw
          ON d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass AND rw.oid = d.objid
        WHERE ((d.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.refobjid = view_oid)
               OR (d.refclassid = 'pg_catalog.pg_type'::pg_catalog.regclass AND d.refobjid IN (SELECT ot.oid FROM own_type AS ot)))
          AND d.deptype NOT IN ('i', 'a', 'e', 'x')
          -- Rules on the view itself: the user ones are listed below.
          AND NOT (d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass AND rw.ev_class = view_oid)
          AND NOT (d.classid = 'pg_catalog.pg_trigger'::pg_catalog.regclass
                   AND EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.oid = d.objid AND t.tgrelid = view_oid))
          AND NOT (d.classid = 'pg_catalog.pg_attrdef'::pg_catalog.regclass
                   AND EXISTS (SELECT FROM pg_catalog.pg_attrdef AS ad WHERE ad.oid = d.objid AND ad.adrelid = view_oid))
          AND NOT (d.classid = 'pg_catalog.pg_type'::pg_catalog.regclass AND d.objid IN (SELECT ot.oid FROM own_type AS ot))
          AND NOT (d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.objid = view_oid)
        UNION ALL
        -- A user rule on the view would be dropped with it.
        SELECT pg_catalog.pg_describe_object('pg_catalog.pg_rewrite'::pg_catalog.regclass, r.oid, 0)
        FROM pg_catalog.pg_rewrite AS r
        WHERE r.ev_class = view_oid AND r.rulename <> '_RETURN'
    ) AS dep
    ORDER BY dep.dependent COLLATE "C"
    LIMIT 1;
$__internal_updatable_view_foreign_dependent$;

/* The extension a view is a member of, or NULL. */
CREATE FUNCTION sql_saga.__internal_updatable_view_extension(view_oid oid)
RETURNS name
LANGUAGE sql
STABLE
SET search_path = pg_catalog, pg_temp
AS $__internal_updatable_view_extension$
    SELECT x.extname
    FROM pg_catalog.pg_depend AS d
    JOIN pg_catalog.pg_extension AS x ON x.oid = d.refobjid
    WHERE d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.objid = view_oid AND d.objsubid = 0
      AND d.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass AND d.deptype = 'e'
    LIMIT 1;
$__internal_updatable_view_extension$;

/* Applies the grants of a saved ACL to a relation or one of its columns (the owner's are implicit). */
CREATE FUNCTION sql_saga.__internal_updatable_view_apply_acl(view_oid oid, column_name name, acl aclitem[])
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $__internal_updatable_view_apply_acl$
DECLARE
    v_owner oid;
    x record;
BEGIN
    -- No grants (aclexplode rejects an empty array).
    IF acl IS NULL OR pg_catalog.cardinality(acl) = 0 THEN
        RETURN;
    END IF;
    SELECT c.relowner INTO v_owner FROM pg_catalog.pg_class AS c WHERE c.oid = view_oid;

    FOR x IN
        SELECT e.grantee, e.privilege_type, e.is_grantable
        FROM pg_catalog.aclexplode(acl) AS e
        WHERE e.grantee <> v_owner
          AND (e.grantee = 0 OR EXISTS (SELECT FROM pg_catalog.pg_roles AS r WHERE r.oid = e.grantee))
        ORDER BY e.grantee, e.privilege_type
    LOOP
        PERFORM sql_saga.__internal_execute_as_caller(pg_catalog.format('GRANT %s%s ON TABLE %s TO %s%s',
            x.privilege_type,
            CASE WHEN column_name IS NULL THEN '' ELSE pg_catalog.format(' (%I)', column_name) END,
            view_oid::pg_catalog.regclass,
            CASE WHEN x.grantee = 0 THEN 'PUBLIC' ELSE x.grantee::pg_catalog.regrole::text END,
            CASE WHEN x.is_grantable THEN ' WITH GRANT OPTION' ELSE '' END));
    END LOOP;
END;
$__internal_updatable_view_apply_acl$;

/*
 * Records the facts of a managed view about to be dropped in __internal_updatable_view_pending, then
 * drops it and its catalog row. Everything stored is read from the catalogs here.
 */
CREATE FUNCTION sql_saga.__internal_updatable_view_record_and_drop(uv sql_saga.updatable_view)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $__internal_updatable_view_record_and_drop$
DECLARE
    v_view_oid oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', uv.view_schema, uv.view_name));
    v_extension name;
BEGIN
    IF v_view_oid IS NULL THEN
        RAISE EXCEPTION 'sql_saga: managed view "%.%" does not exist', uv.view_schema, uv.view_name;
    END IF;

    INSERT INTO sql_saga.__internal_updatable_view_pending
        (view_schema, view_name, view_type, table_schema, table_name, era_name, trigger_name, current_func,
         ephemeral_columns, view_clock, view_acl, view_comment, view_reloptions, view_columns, view_column_types, view_extension, view_delete_mode,
         column_facts, trigger_facts)
    SELECT uv.view_schema, uv.view_name, uv.view_type, uv.table_schema, uv.table_name, uv.era_name,
           uv.trigger_name, uv.current_func, uv.ephemeral_columns,
           CASE WHEN uv.view_type = 'current' THEN sql_saga.__internal_current_view_clock(v_view_oid) END,
           c.relacl, pg_catalog.obj_description(v_view_oid, 'pg_class'), c.reloptions,
           ARRAY(SELECT a.attname FROM pg_catalog.pg_attribute AS a
                 WHERE a.attrelid = v_view_oid AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum),
           ARRAY(SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) FROM pg_catalog.pg_attribute AS a
                 WHERE a.attrelid = v_view_oid AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum),
           sql_saga.__internal_updatable_view_extension(v_view_oid),
           CASE WHEN uv.view_type = 'current' THEN
               (SELECT (pg_catalog.string_to_array(pg_catalog.rtrim(pg_catalog.encode(t.tgargs, 'escape'), E'\\000'), E'\\000'))[1]
                FROM pg_catalog.pg_trigger AS t
                WHERE (t.tgrelid, t.tgname) = (v_view_oid, uv.trigger_name)) END,
           (SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
                       'name', a.attname,
                       -- No column grant: an empty array, not JSON null.
                       'acl', COALESCE(pg_catalog.to_jsonb(a.attacl::text[]), '[]'::jsonb),
                       'comment', pg_catalog.col_description(v_view_oid, a.attnum),
                       'default', pg_catalog.pg_get_expr(ad.adbin, ad.adrelid)) ORDER BY a.attnum), '[]')
            FROM pg_catalog.pg_attribute AS a
            LEFT JOIN pg_catalog.pg_attrdef AS ad ON (ad.adrelid, ad.adnum) = (a.attrelid, a.attnum)
            WHERE a.attrelid = v_view_oid AND a.attnum > 0 AND NOT a.attisdropped
              AND (a.attacl IS NOT NULL OR ad.oid IS NOT NULL
                   OR pg_catalog.col_description(v_view_oid, a.attnum) IS NOT NULL)),
           (SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
                       'name', t.tgname,
                       'definition', pg_catalog.pg_get_triggerdef(t.oid),
                       'enabled', t.tgenabled::text,
                       'comment', pg_catalog.obj_description(t.oid, 'pg_trigger')) ORDER BY t.tgname COLLATE "C"), '[]')
            FROM pg_catalog.pg_trigger AS t
            WHERE t.tgrelid = v_view_oid AND NOT t.tgisinternal)
    FROM pg_catalog.pg_class AS c
    WHERE c.oid = v_view_oid;

    -- Catalog row first, so drop_protection treats the view as unmanaged (as drop_*_view does).
    DELETE FROM sql_saga.updatable_view AS u
    WHERE (u.view_schema, u.view_name) = (uv.view_schema, uv.view_name);
    -- An extension member cannot be dropped; restore adds the recreated view back to it.
    v_extension := sql_saga.__internal_updatable_view_extension(v_view_oid);
    IF v_extension IS NOT NULL THEN
        EXECUTE pg_catalog.format('ALTER EXTENSION %I DROP VIEW %s', v_extension, v_view_oid::pg_catalog.regclass);
    END IF;
    PERFORM sql_saga.__internal_execute_as_caller(pg_catalog.format('DROP VIEW %s', v_view_oid::pg_catalog.regclass));
END;
$__internal_updatable_view_record_and_drop$;

/* Recreates a managed view from its row in __internal_updatable_view_pending (already removed from it). */
CREATE FUNCTION sql_saga.__internal_updatable_view_restore(pending sql_saga.__internal_updatable_view_pending)
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $__internal_updatable_view_restore$
DECLARE
    v_row sql_saga.updatable_view;
    v_table_oid oid;
    v_view_oid oid;
    v_column jsonb;
    v_trigger jsonb;
    v_removed text;
    v_retyped text;
    v_added text;
    v_changes text[];
    v_extension name;
BEGIN
    v_table_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', pending.table_schema, pending.table_name));
    IF v_table_oid IS NULL THEN
        RAISE EXCEPTION 'sql_saga: cannot recreate managed view "%.%" because its table "%.%" no longer exists',
            pending.view_schema, pending.view_name, pending.table_schema, pending.table_name;
    END IF;
    /* Only a view this transaction dropped: the name is free and it is not catalogued. */
    IF pending.transaction_id IS DISTINCT FROM pg_catalog.pg_current_xact_id()::text::bigint
       OR pg_catalog.to_regclass(pg_catalog.format('%I.%I', pending.view_schema, pending.view_name)) IS NOT NULL
       OR EXISTS (SELECT FROM sql_saga.updatable_view AS u
                  WHERE (u.view_schema, u.view_name) = (pending.view_schema, pending.view_name))
       OR NOT EXISTS (SELECT FROM sql_saga.era AS e
                      WHERE (e.table_schema, e.table_name, e.era_name) = (pending.table_schema, pending.table_name, pending.era_name)) THEN
        RAISE EXCEPTION 'sql_saga: cannot recreate managed view "%.%": it was not dropped by this transaction''s ALTER TABLE',
            pending.view_schema, pending.view_name;
    END IF;

    v_row := ROW(pending.view_schema, pending.view_name, pending.view_type, pending.table_schema, pending.table_name,
                 pending.era_name, pending.trigger_name, pending.current_func,
                 sql_saga.__internal_updatable_view_generated_columns(v_table_oid), pending.ephemeral_columns)::sql_saga.updatable_view;

    PERFORM sql_saga.__internal_execute_as_caller(
        sql_saga.__internal_updatable_view_create_sql(v_row, false, pending.view_reloptions, pending.view_clock));
    v_view_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', v_row.view_schema, v_row.view_name));

    /*
     * Exactly the extension membership the dropped view had: CREATE VIEW inside another extension's
     * script made it a member of that extension (with its row types; ALTER EXTENSION ... DROP VIEW
     * takes them along).
     */
    v_extension := sql_saga.__internal_updatable_view_extension(v_view_oid);
    IF v_extension IS DISTINCT FROM pending.view_extension THEN
        IF v_extension IS NOT NULL THEN
            EXECUTE pg_catalog.format('ALTER EXTENSION %I DROP VIEW %s', v_extension, v_view_oid::pg_catalog.regclass);
        END IF;
        IF pending.view_extension IS NOT NULL THEN
            EXECUTE pg_catalog.format('ALTER EXTENSION %I ADD VIEW %s', pending.view_extension, v_view_oid::pg_catalog.regclass);
        END IF;
    END IF;
    -- As add_*_view: the view belongs to the table owner.
    PERFORM sql_saga.__internal_execute_as_caller(pg_catalog.format('ALTER VIEW %s OWNER TO %s', v_view_oid::pg_catalog.regclass,
        (SELECT c.relowner::pg_catalog.regrole::text FROM pg_catalog.pg_class AS c WHERE c.oid = v_table_oid)));

    PERFORM sql_saga.__internal_updatable_view_apply_acl(v_view_oid, NULL, pending.view_acl);
    IF pending.view_comment IS NOT NULL THEN
        PERFORM sql_saga.__internal_execute_as_caller(pg_catalog.format('COMMENT ON VIEW %s IS %L', v_view_oid::pg_catalog.regclass, pending.view_comment));
    END IF;

    FOR v_column IN SELECT c FROM pg_catalog.jsonb_array_elements(pending.column_facts) AS c
    LOOP
        CONTINUE WHEN NOT EXISTS (
            SELECT FROM pg_catalog.pg_attribute AS a
            WHERE (a.attrelid, a.attname) = (v_view_oid, v_column->>'name') AND a.attnum > 0 AND NOT a.attisdropped);
        IF pg_catalog.jsonb_typeof(v_column->'acl') = 'array' THEN
            PERFORM sql_saga.__internal_updatable_view_apply_acl(v_view_oid, (v_column->>'name')::name,
                ARRAY(SELECT pg_catalog.jsonb_array_elements_text(v_column->'acl'))::aclitem[]);
        END IF;
        IF v_column->>'comment' IS NOT NULL THEN
            PERFORM sql_saga.__internal_execute_as_caller(pg_catalog.format('COMMENT ON COLUMN %s.%I IS %L', v_view_oid::pg_catalog.regclass, v_column->>'name', v_column->>'comment'));
        END IF;
        IF v_column->>'default' IS NOT NULL THEN
            -- pg_get_expr text captured under this same search_path.
            PERFORM sql_saga.__internal_execute_as_caller(pg_catalog.format('ALTER VIEW %s ALTER COLUMN %I SET DEFAULT %s', v_view_oid::pg_catalog.regclass, v_column->>'name', v_column->>'default'));
        END IF;
    END LOOP;

    FOR v_trigger IN SELECT t FROM pg_catalog.jsonb_array_elements(pending.trigger_facts) AS t
    LOOP
        -- pg_get_triggerdef text captured by __internal_updatable_view_record_and_drop under this search_path.
        IF v_trigger->>'definition' !~ '^CREATE (CONSTRAINT )?TRIGGER ' THEN
            RAISE EXCEPTION 'sql_saga: unexpected trigger definition for view "%.%"', pending.view_schema, pending.view_name;
        END IF;
        PERFORM sql_saga.__internal_execute_as_caller(v_trigger->>'definition');
        IF v_trigger->>'enabled' IS DISTINCT FROM 'O' THEN
            PERFORM sql_saga.__internal_execute_as_caller(pg_catalog.format('ALTER TABLE %s %s TRIGGER %I', v_view_oid::pg_catalog.regclass,
                CASE v_trigger->>'enabled' WHEN 'D' THEN 'DISABLE' WHEN 'R' THEN 'ENABLE REPLICA' ELSE 'ENABLE ALWAYS' END,
                v_trigger->>'name'));
        END IF;
        IF v_trigger->>'comment' IS NOT NULL THEN
            PERFORM sql_saga.__internal_execute_as_caller(pg_catalog.format('COMMENT ON TRIGGER %I ON %s IS %L', v_trigger->>'name', v_view_oid::pg_catalog.regclass, v_trigger->>'comment'));
        END IF;
    END LOOP;

    -- The catalog row comes last: until then, the nested event triggers see an unmanaged view.
    INSERT INTO sql_saga.updatable_view SELECT v_row.*;

    -- What changed, from the view's columns and types before (pending) and after: the columns it no longer
    -- shows, the columns with a new type, and the new columns, each in column order.
    SELECT pg_catalog.string_agg(pg_catalog.quote_ident(old_column.name), ', ' ORDER BY old_column.n)
    INTO v_removed
    FROM pg_catalog.unnest(pending.view_columns) WITH ORDINALITY AS old_column(name, n)
    WHERE NOT EXISTS (
        SELECT FROM pg_catalog.pg_attribute AS a
        WHERE (a.attrelid, a.attname) = (v_view_oid, old_column.name) AND a.attnum > 0 AND NOT a.attisdropped);

    SELECT pg_catalog.string_agg(pg_catalog.quote_ident(a.attname), ', ' ORDER BY a.attnum)
    INTO v_retyped
    FROM pg_catalog.pg_attribute AS a
    JOIN ROWS FROM (pg_catalog.unnest(pending.view_columns), pg_catalog.unnest(pending.view_column_types)) AS old_column(name, type_name)
      ON old_column.name = a.attname
    WHERE a.attrelid = v_view_oid AND a.attnum > 0 AND NOT a.attisdropped
      AND pg_catalog.format_type(a.atttypid, a.atttypmod) IS DISTINCT FROM old_column.type_name;

    SELECT pg_catalog.string_agg(pg_catalog.quote_ident(a.attname), ', ' ORDER BY a.attnum)
    INTO v_added
    FROM pg_catalog.pg_attribute AS a
    WHERE a.attrelid = v_view_oid AND a.attnum > 0 AND NOT a.attisdropped
      AND NOT a.attname = ANY (pending.view_columns);

    v_changes := ARRAY[]::text[];
    IF v_removed IS NOT NULL THEN
        v_changes := v_changes || pg_catalog.format('without column(s) %s', v_removed);
    END IF;
    IF v_retyped IS NOT NULL THEN
        v_changes := v_changes || pg_catalog.format('with the new type of column(s) %s', v_retyped);
    END IF;
    IF v_added IS NOT NULL THEN
        v_changes := v_changes || pg_catalog.format('with new column(s) %s', v_added);
    END IF;

    RAISE NOTICE 'sql_saga: Dropped and recreated % view "%"%, owned by the table owner, restoring its grants, comments, column defaults, triggers and view options',
        v_row.view_type, sql_saga.__internal_display_name(v_view_oid::pg_catalog.regclass),
        CASE WHEN pg_catalog.cardinality(v_changes) = 0
             THEN pg_catalog.format(' to match the columns of table "%s"', sql_saga.__internal_display_name(v_table_oid::pg_catalog.regclass))
             ELSE pg_catalog.format(' %s of table "%s"',
                      -- "A", "A and B", "A, B and C"
                      CASE WHEN pg_catalog.cardinality(v_changes) = 1 THEN v_changes[1]
                           ELSE pg_catalog.array_to_string(v_changes[1:pg_catalog.cardinality(v_changes) - 1], ', ')
                                || ' and ' || v_changes[pg_catalog.cardinality(v_changes)] END,
                      sql_saga.__internal_display_name(v_table_oid::pg_catalog.regclass)) END;
END;
$__internal_updatable_view_restore$;

/*
 * Recreates (and removes from __internal_updatable_view_pending) every view this transaction recorded
 * and dropped. The rows are taken first: the DDL of the restore fires the event triggers again.
 */
CREATE FUNCTION sql_saga.__internal_updatable_view_restore_pending()
RETURNS void
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $__internal_updatable_view_restore_pending$
DECLARE
    v_pending sql_saga.__internal_updatable_view_pending[];
    v_one sql_saga.__internal_updatable_view_pending;
BEGIN
    WITH taken AS (
        DELETE FROM sql_saga.__internal_updatable_view_pending AS p
        WHERE p.transaction_id = pg_catalog.pg_current_xact_id()::text::bigint
        RETURNING p.*
    )
    SELECT pg_catalog.array_agg(t ORDER BY t.view_schema COLLATE "C", t.view_name COLLATE "C")
    INTO v_pending
    FROM taken AS t;

    IF v_pending IS NULL THEN
        RETURN;
    END IF;
    FOREACH v_one IN ARRAY v_pending
    LOOP
        PERFORM sql_saga.__internal_updatable_view_restore(v_one);
    END LOOP;
END;
$__internal_updatable_view_restore_pending$;

/*
 * The managed views of a table (and, when recurse, of the inheritance descendants that lose the column
 * with it) that show one of the given columns.
 */
CREATE FUNCTION sql_saga.__internal_updatable_view_showing_columns(table_oid oid, column_names text[], recurse boolean)
RETURNS TABLE (uv_row sql_saga.updatable_view, base_oid oid, view_oid oid)
LANGUAGE sql
STABLE
SET search_path = pg_catalog, pg_temp
AS $__internal_updatable_view_showing_columns$
    WITH RECURSIVE tables(relation_oid) AS (
        SELECT table_oid
        UNION
        SELECT i.inhrelid FROM tables AS t JOIN pg_catalog.pg_inherits AS i ON i.inhparent = t.relation_oid
        WHERE recurse
    ), managed AS (
        SELECT uv AS uv_row, t.relation_oid AS base_oid,
               pg_catalog.to_regclass(pg_catalog.format('%I.%I', uv.view_schema, uv.view_name))::oid AS view_oid
        FROM tables AS t
        JOIN pg_catalog.pg_class AS c ON c.oid = t.relation_oid
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
        JOIN sql_saga.updatable_view AS uv ON (uv.table_schema, uv.table_name) = (n.nspname, c.relname)
    )
    SELECT m.uv_row, m.base_oid, m.view_oid
    FROM managed AS m
    WHERE m.view_oid IS NOT NULL
      AND EXISTS (
          SELECT FROM pg_catalog.pg_attribute AS va
          WHERE va.attrelid = m.view_oid AND va.attnum > 0 AND NOT va.attisdropped
            AND va.attname = ANY (column_names)
            -- A descendant keeps a column it defines locally or also inherits from another parent.
            AND (m.base_oid = table_oid
                 OR EXISTS (SELECT FROM pg_catalog.pg_attribute AS ba
                            WHERE (ba.attrelid, ba.attname) = (m.base_oid, va.attname)
                              AND NOT ba.attislocal AND ba.attinhcount = 1)))
    ORDER BY (m.uv_row).view_schema COLLATE "C", (m.uv_row).view_name COLLATE "C";
$__internal_updatable_view_showing_columns$;

/*
 * Whether a DROP COLUMN of these columns drops one that sql_saga manages for a managed view of the table
 * (or of a descendant losing the column with it): an era, key, foreign key, view identifier, ephemeral or
 * excluded column. Such a drop is not ours to accommodate: the views stay in place, so that their
 * dependency makes PostgreSQL (or, with CASCADE, drop_protection) refuse it.
 */
CREATE FUNCTION sql_saga.__internal_updatable_view_drop_is_managed(table_oid oid, column_names text[], recurse boolean)
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = pg_catalog, pg_temp
AS $__internal_updatable_view_drop_is_managed$
    SELECT EXISTS (
        SELECT
        FROM sql_saga.__internal_updatable_view_showing_columns(table_oid, column_names, recurse) AS v
        CROSS JOIN pg_catalog.unnest(column_names) AS d(column_name)
        WHERE EXISTS (
                  SELECT FROM sql_saga.era AS e
                  WHERE (e.table_schema, e.table_name) = ((v.uv_row).table_schema, (v.uv_row).table_name)
                    AND (d.column_name IN (e.range_column_name, e.valid_from_column_name, e.valid_until_column_name, e.valid_to_column_name)
                         OR d.column_name = ANY (e.ephemeral_columns)))
           OR EXISTS (
                  SELECT FROM sql_saga.unique_keys AS uk
                  WHERE (uk.table_schema, uk.table_name) = ((v.uv_row).table_schema, (v.uv_row).table_name)
                    AND (d.column_name = ANY (uk.column_names) OR d.column_name = ANY (uk.mutually_exclusive_columns)))
           OR EXISTS (
                  SELECT FROM sql_saga.foreign_keys AS fk
                  WHERE (fk.table_schema, fk.table_name) = ((v.uv_row).table_schema, (v.uv_row).table_name)
                    AND d.column_name = ANY (fk.column_names))
           OR EXISTS (
                  SELECT FROM sql_saga.system_time_era AS ste
                  WHERE (ste.table_schema, ste.table_name) = ((v.uv_row).table_schema, (v.uv_row).table_name)
                    AND d.column_name = ANY (ste.excluded_column_names))
           OR d.column_name = ANY ((v.uv_row).ephemeral_columns)
           OR EXISTS (
                  SELECT FROM pg_catalog.pg_trigger AS t
                  WHERE (t.tgrelid, t.tgname) = (v.view_oid, (v.uv_row).trigger_name)
                    AND d.column_name = ANY (pg_catalog.string_to_array(
                        pg_catalog.rtrim(pg_catalog.encode(t.tgargs, 'escape'), E'\\000'), E'\\000'))));
$__internal_updatable_view_drop_is_managed$;

/*
 * ddl_command_start of ALTER TABLE ... DROP COLUMN (called from the C event trigger, as the extension
 * owner, with the dropped column names): record and drop every managed view of the table (and, unless
 * ONLY, of the inheritance descendants that lose the column with it) that shows one of the dropped
 * columns, for __internal_updatable_view_follow_shape to recreate at ddl_command_end.
 */
CREATE FUNCTION sql_saga.__internal_updatable_view_prepare_drop_column(table_oid oid, column_names text[], recurse boolean)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $__internal_updatable_view_prepare_drop_column$
#variable_conflict use_variable
DECLARE
    v_view record;
    v_dependent text;
BEGIN
    PERFORM sql_saga.__internal_entry_guard();
    IF table_oid IS NULL OR column_names IS NULL OR pg_catalog.cardinality(column_names) = 0 THEN
        RETURN;
    END IF;

    /* Fast exit: no managed view at all. */
    IF NOT EXISTS (SELECT FROM sql_saga.updatable_view) THEN
        RETURN;
    END IF;

    /*
     * A dropped column that sql_saga manages (era, key, foreign key, view identifier, ephemeral or
     * excluded column) is not ours to accommodate: leave the views in place, so their dependency makes
     * the DROP COLUMN fail as before (or, with CASCADE, drop_protection refuses it). The catalogs follow
     * renames (rename_following, __internal_updatable_view_follow_shape), so the current names match.
     */
    IF sql_saga.__internal_updatable_view_drop_is_managed(table_oid, column_names, recurse) THEN
        RETURN;
    END IF;

    /* Refuse before any change when the rebuild would take a user object (view, rule, ...) with it. */
    FOR v_view IN
        SELECT d.view_oid, d.base_oid, (d.uv_row).view_type AS view_type
        FROM sql_saga.__internal_updatable_view_showing_columns(table_oid, column_names, recurse) AS d
    LOOP
        v_dependent := sql_saga.__internal_updatable_view_foreign_dependent(v_view.view_oid);
        IF v_dependent IS NOT NULL THEN
            RAISE EXCEPTION 'cannot drop column(s) % of table "%" because sql_saga must rebuild its managed % view "%" without them, and % depends on that view',
                (SELECT pg_catalog.string_agg(pg_catalog.format('"%s"', d.c), ', ') FROM pg_catalog.unnest(column_names) AS d(c)),
                sql_saga.__internal_display_name(v_view.base_oid::pg_catalog.regclass),
                v_view.view_type,
                sql_saga.__internal_display_name(v_view.view_oid::pg_catalog.regclass),
                v_dependent
                USING ERRCODE = 'dependent_objects_still_exist',
                      HINT = 'Drop the dependent object, run the DROP COLUMN, then recreate the dependent object.';
        END IF;
    END LOOP;

    FOR v_view IN
        SELECT d.uv_row
        FROM sql_saga.__internal_updatable_view_showing_columns(table_oid, column_names, recurse) AS d
    LOOP
        PERFORM sql_saga.__internal_updatable_view_record_and_drop(v_view.uv_row);
    END LOOP;
END;
$__internal_updatable_view_prepare_drop_column$;

/*
 * ddl_command_start of ALTER TABLE ... ALTER COLUMN ... TYPE (called from the C event trigger, as the
 * extension owner, with the retyped columns and the columns the same statement drops). A type change
 * recurses to every inheritance descendant (PostgreSQL requires it), so all of them are considered.
 *
 * 1. Refused, whether or not a view exists:
 *    - an era's range column or boundary column (valid_from, valid_until, valid_to): its type is bound into
 *      the era catalog (range_type, range_subtype), the synchronization trigger and the bounds constraints;
 *    - a column a sql_saga foreign key uses, on either side: the referencing column, or a column of the
 *      unique key it references. The other side would have to change with it, and sql_saga never alters
 *      another table on the caller's behalf.
 * 2. When the statement also drops a column sql_saga manages, nothing is done: the views stay in place and
 *    PostgreSQL refuses the drop as before.
 * 3. The managed views that show a retyped (or dropped) column are recorded and dropped, to be recreated by
 *    __internal_updatable_view_follow_shape at ddl_command_end. A user object depending on one of them
 *    would be lost with it, so the statement is refused before any change.
 */
CREATE FUNCTION sql_saga.__internal_updatable_view_prepare_alter_type(table_oid oid, type_column_names text[], drop_column_names text[], recurse boolean)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $__internal_updatable_view_prepare_alter_type$
#variable_conflict use_variable
DECLARE
    v_bound record;
    v_fk record;
    v_views sql_saga.updatable_view[];
    v_row sql_saga.updatable_view;
    v_view_oid oid;
    v_dependent text;
    v_tables oid[];
BEGIN
    PERFORM sql_saga.__internal_entry_guard();
    IF table_oid IS NULL OR type_column_names IS NULL OR pg_catalog.cardinality(type_column_names) = 0 THEN
        RETURN;
    END IF;
    drop_column_names := COALESCE(drop_column_names, '{}');

    -- The table and the descendants the type change recurses to, parents first.
    WITH RECURSIVE tables(relation_oid, depth) AS (
        SELECT table_oid, 0
        UNION
        SELECT i.inhrelid, t.depth + 1 FROM tables AS t JOIN pg_catalog.pg_inherits AS i ON i.inhparent = t.relation_oid
        WHERE recurse
    )
    SELECT pg_catalog.array_agg(t.relation_oid ORDER BY t.depth, t.relation_oid)
    INTO v_tables
    FROM (SELECT tt.relation_oid, pg_catalog.min(tt.depth) AS depth FROM tables AS tt GROUP BY tt.relation_oid) AS t;

    /* 1a. Era range and boundary columns. */
    FOR v_bound IN
        SELECT tab.relation_oid, e.era_name, d.column_name,
               CASE d.column_name
                   WHEN e.range_column_name THEN 'range column'
                   WHEN e.valid_from_column_name THEN 'valid_from column'
                   WHEN e.valid_until_column_name THEN 'valid_until column'
                   ELSE 'valid_to column' END AS role
        FROM pg_catalog.unnest(v_tables) WITH ORDINALITY AS tab(relation_oid, n)
        JOIN pg_catalog.pg_class AS c ON c.oid = tab.relation_oid
        JOIN pg_catalog.pg_namespace AS ns ON ns.oid = c.relnamespace
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name) = (ns.nspname, c.relname)
        CROSS JOIN pg_catalog.unnest(type_column_names) WITH ORDINALITY AS d(column_name, m)
        WHERE d.column_name IN (e.range_column_name, e.valid_from_column_name, e.valid_until_column_name, e.valid_to_column_name)
        ORDER BY tab.n, d.m, e.era_name COLLATE "C"
        LIMIT 1
    LOOP
        RAISE EXCEPTION 'cannot alter type of column "%" of table "%" because it is the % of era "%" managed by sql_saga',
            v_bound.column_name, sql_saga.__internal_display_name(v_bound.relation_oid::pg_catalog.regclass),
            v_bound.role, v_bound.era_name
            USING ERRCODE = 'feature_not_supported',
                  HINT = 'Save and drop the era''s dependants with their drop functions (foreign keys with sql_saga.drop_foreign_key_by_name, unique keys with sql_saga.drop_unique_key, updatable views with sql_saga.drop_for_portion_of_view or sql_saga.drop_current_view), drop the era with sql_saga.drop_era(..., cleanup => false), change the range column and its boundary columns together, then add the era again with sql_saga.add_era and recreate the dependants.';
    END LOOP;

    /* 1b. Columns a sql_saga foreign key uses, as referencing column or as a column of the referenced key. */
    FOR v_fk IN
        SELECT tab.relation_oid, d.column_name,
               pg_catalog.string_agg(pg_catalog.format('"%s" on table "%s"', fk.foreign_key_name,
                   sql_saga.__internal_display_name(pg_catalog.format('%I.%I', fk.table_schema, fk.table_name)::pg_catalog.regclass)),
                   ', ' ORDER BY fk.foreign_key_name COLLATE "C") AS foreign_keys,
               pg_catalog.min(tab.n) AS n, pg_catalog.min(d.m) AS m
        FROM pg_catalog.unnest(v_tables) WITH ORDINALITY AS tab(relation_oid, n)
        JOIN pg_catalog.pg_class AS c ON c.oid = tab.relation_oid
        JOIN pg_catalog.pg_namespace AS ns ON ns.oid = c.relnamespace
        CROSS JOIN pg_catalog.unnest(type_column_names) WITH ORDINALITY AS d(column_name, m)
        JOIN sql_saga.foreign_keys AS fk
          ON ((fk.table_schema, fk.table_name) = (ns.nspname, c.relname) AND d.column_name = ANY (fk.column_names))
          OR EXISTS (SELECT FROM sql_saga.unique_keys AS uk
                     WHERE uk.unique_key_name = fk.unique_key_name
                       AND (uk.table_schema, uk.table_name) = (ns.nspname, c.relname)
                       AND d.column_name = ANY (uk.column_names))
        GROUP BY tab.relation_oid, d.column_name
        ORDER BY pg_catalog.min(tab.n), pg_catalog.min(d.m)
        LIMIT 1
    LOOP
        RAISE EXCEPTION 'cannot alter type of column "%" of table "%" because sql_saga foreign key(s) % use it',
            v_fk.column_name, sql_saga.__internal_display_name(v_fk.relation_oid::pg_catalog.regclass), v_fk.foreign_keys
            USING ERRCODE = 'feature_not_supported',
                  HINT = 'Drop the foreign key(s) with sql_saga.drop_foreign_key_by_name, change the type of the columns on both sides, then add the foreign key(s) again.';
    END LOOP;

    /* Fast exit: no managed view at all. */
    IF NOT EXISTS (SELECT FROM sql_saga.updatable_view) THEN
        RETURN;
    END IF;

    /* 2. A drop of a column sql_saga manages in the same statement: leave it to PostgreSQL to refuse. */
    IF pg_catalog.cardinality(drop_column_names) > 0
       AND sql_saga.__internal_updatable_view_drop_is_managed(table_oid, drop_column_names, recurse) THEN
        RETURN;
    END IF;

    /* 3. The managed views to rebuild: on any of the tables, showing a retyped column, or a dropped one. */
    SELECT pg_catalog.array_agg(v.uv_row ORDER BY (v.uv_row).view_schema COLLATE "C", (v.uv_row).view_name COLLATE "C")
    INTO v_views
    FROM (
        SELECT m.uv_row
        FROM (
            SELECT uv AS uv_row,
                   pg_catalog.to_regclass(pg_catalog.format('%I.%I', uv.view_schema, uv.view_name))::oid AS view_oid
            FROM pg_catalog.unnest(v_tables) AS tab(relation_oid)
            JOIN pg_catalog.pg_class AS c ON c.oid = tab.relation_oid
            JOIN pg_catalog.pg_namespace AS ns ON ns.oid = c.relnamespace
            JOIN sql_saga.updatable_view AS uv ON (uv.table_schema, uv.table_name) = (ns.nspname, c.relname)
        ) AS m
        WHERE m.view_oid IS NOT NULL
          AND EXISTS (SELECT FROM pg_catalog.pg_attribute AS va
                      WHERE va.attrelid = m.view_oid AND va.attnum > 0 AND NOT va.attisdropped
                        AND va.attname = ANY (type_column_names))
        UNION
        SELECT s.uv_row
        FROM sql_saga.__internal_updatable_view_showing_columns(table_oid, drop_column_names, recurse) AS s
        WHERE pg_catalog.cardinality(drop_column_names) > 0
    ) AS v;
    IF v_views IS NULL THEN
        RETURN;
    END IF;

    /* Refuse before any change when the rebuild would take a user object (view, rule, ...) with it. */
    FOREACH v_row IN ARRAY v_views
    LOOP
        v_view_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', v_row.view_schema, v_row.view_name));
        v_dependent := sql_saga.__internal_updatable_view_foreign_dependent(v_view_oid);
        IF v_dependent IS NOT NULL THEN
            RAISE EXCEPTION 'cannot alter type of column(s) % of table "%" because sql_saga must rebuild its managed % view "%" with the new type, and % depends on that view',
                (SELECT pg_catalog.string_agg(pg_catalog.format('"%s"', d.c), ', ' ORDER BY d.n)
                 FROM pg_catalog.unnest(type_column_names) WITH ORDINALITY AS d(c, n)),
                sql_saga.__internal_display_name(pg_catalog.format('%I.%I', v_row.table_schema, v_row.table_name)::pg_catalog.regclass),
                v_row.view_type,
                sql_saga.__internal_display_name(v_view_oid::pg_catalog.regclass),
                v_dependent
                USING ERRCODE = 'dependent_objects_still_exist',
                      HINT = 'Drop the dependent object, run the ALTER TABLE, then recreate the dependent object.';
        END IF;
    END LOOP;

    FOREACH v_row IN ARRAY v_views
    LOOP
        PERFORM sql_saga.__internal_updatable_view_record_and_drop(v_row);
    END LOOP;
END;
$__internal_updatable_view_prepare_alter_type$;

/*
 * ddl_command_end of ALTER TABLE (rename_following): recreate the views that
 * __internal_updatable_view_prepare_drop_column dropped, then bring every managed view of the reshaped
 * tables up to the base's columns and refresh generated_columns.
 */
CREATE FUNCTION sql_saga.__internal_updatable_view_follow_shape(table_oids oid[])
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $__internal_updatable_view_follow_shape$
#variable_conflict use_variable
DECLARE
    v_view record;
    v_row sql_saga.updatable_view;
    v_rename record;
    v_trigger_args text[];
    v_first_column_arg integer;
    v_base_columns text[];
    v_view_columns text[];
    v_added text;
    v_generated name[];
    v_dependent text;
    v_replaced boolean;
BEGIN
    PERFORM sql_saga.__internal_entry_guard();
    PERFORM sql_saga.__internal_updatable_view_restore_pending();

    IF table_oids IS NULL OR pg_catalog.cardinality(table_oids) = 0 THEN
        RETURN;
    END IF;

    FOR v_view IN
        SELECT uv.view_schema, uv.view_name, t.table_oid,
               pg_catalog.to_regclass(pg_catalog.format('%I.%I', uv.view_schema, uv.view_name))::oid AS view_oid
        FROM (SELECT DISTINCT u.table_oid FROM pg_catalog.unnest(table_oids) AS u(table_oid)) AS t
        JOIN pg_catalog.pg_class AS c ON c.oid = t.table_oid
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
        JOIN sql_saga.updatable_view AS uv ON (uv.table_schema, uv.table_name) = (n.nspname, c.relname)
        ORDER BY uv.view_schema COLLATE "C", uv.view_name COLLATE "C"
    LOOP
        CONTINUE WHEN v_view.view_oid IS NULL;
        SELECT uv.* INTO v_row FROM sql_saga.updatable_view AS uv
        WHERE (uv.view_schema, uv.view_name) = (v_view.view_schema, v_view.view_name);

        /*
         * for_portion_of_trigger keeps its single-row source in pg_temp.__temp_for_portion_of_<view oid>
         * until the end of the transaction. Created before this reshape, it has the old columns.
         */
        IF pg_catalog.to_regclass(pg_catalog.format('pg_temp.%I', '__temp_for_portion_of_' || v_view.view_oid)) IS NOT NULL THEN
            EXECUTE pg_catalog.format('DROP TABLE pg_temp.%I', '__temp_for_portion_of_' || v_view.view_oid);
        END IF;

        SELECT pg_catalog.array_agg(pg_catalog.format('%I %s', a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)) ORDER BY a.attnum)
        INTO v_base_columns
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = v_view.table_oid AND a.attnum > 0 AND NOT a.attisdropped;

        SELECT pg_catalog.array_agg(pg_catalog.format('%I %s', a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)) ORDER BY a.attnum)
        INTO v_view_columns
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = v_view.view_oid AND a.attnum > 0 AND NOT a.attisdropped;

        /*
         * Invariant: on entry the view's columns were the base's columns, or a prefix of them (every
         * ALTER TABLE is followed here, and DROP COLUMN rebuilt the view already). So after an ALTER
         * TABLE, equal types in equal positions under some different names can only be a RENAME COLUMN;
         * more base columns with the view's as prefix an ADD COLUMN; anything else (a view found stale,
         * e.g. after the event triggers were disabled) is repaired by a drop and recreate.
         */
        IF v_view_columns IS DISTINCT FROM v_base_columns
           AND (SELECT pg_catalog.array_agg(pg_catalog.format_type(a.atttypid, a.atttypmod) ORDER BY a.attnum) FROM pg_catalog.pg_attribute AS a
                WHERE a.attrelid = v_view.view_oid AND a.attnum > 0 AND NOT a.attisdropped)
             = (SELECT pg_catalog.array_agg(pg_catalog.format_type(a.atttypid, a.atttypmod) ORDER BY a.attnum) FROM pg_catalog.pg_attribute AS a
                WHERE a.attrelid = v_view.table_oid AND a.attnum > 0 AND NOT a.attisdropped) THEN
            /*
             * The base columns were renamed. Rename the view columns in place (same OID, grants,
             * comments), then the identifier columns in the INSTEAD OF trigger's arguments (CREATE OR
             * REPLACE TRIGGER, so the managed trigger is never dropped; its arguments are the delete mode
             * for a current view, then column names only) and the view's ephemeral_columns.
             */
            FOR v_rename IN
                SELECT va.attname AS old_name, ba.attname AS new_name
                FROM (SELECT a.attname, pg_catalog.row_number() OVER (ORDER BY a.attnum) AS n
                      FROM pg_catalog.pg_attribute AS a
                      WHERE a.attrelid = v_view.view_oid AND a.attnum > 0 AND NOT a.attisdropped) AS va
                JOIN (SELECT a.attname, pg_catalog.row_number() OVER (ORDER BY a.attnum) AS n
                      FROM pg_catalog.pg_attribute AS a
                      WHERE a.attrelid = v_view.table_oid AND a.attnum > 0 AND NOT a.attisdropped) AS ba
                  ON ba.n = va.n
                WHERE va.attname <> ba.attname
                ORDER BY va.n
            LOOP
                RAISE DEBUG 'sql_saga: follow rename of column "%" to "%" in view %', v_rename.old_name, v_rename.new_name, v_view.view_oid::pg_catalog.regclass;
                PERFORM sql_saga.__internal_execute_as_caller(pg_catalog.format('ALTER VIEW %s RENAME COLUMN %I TO %I',
                    v_view.view_oid::pg_catalog.regclass, v_rename.old_name, v_rename.new_name));
                RAISE NOTICE 'sql_saga: Renamed column "%" of % view "%" to "%" to follow the rename in table "%"',
                    v_rename.old_name, v_row.view_type, sql_saga.__internal_display_name(v_view.view_oid::pg_catalog.regclass),
                    v_rename.new_name, sql_saga.__internal_display_name(v_view.table_oid::pg_catalog.regclass);

                SELECT pg_catalog.string_to_array(pg_catalog.rtrim(pg_catalog.encode(t.tgargs, 'escape'), E'\\000'), E'\\000')
                INTO v_trigger_args
                FROM pg_catalog.pg_trigger AS t
                WHERE (t.tgrelid, t.tgname) = (v_view.view_oid, v_row.trigger_name);
                -- The current view's first argument is its delete mode, not a column.
                v_first_column_arg := CASE WHEN v_row.view_type = 'current' THEN 2 ELSE 1 END;
                IF v_trigger_args IS NOT NULL
                   AND v_rename.old_name = ANY (v_trigger_args[v_first_column_arg:]) THEN
                    v_trigger_args := v_trigger_args[1:v_first_column_arg - 1]
                                      || pg_catalog.array_replace(v_trigger_args[v_first_column_arg:], v_rename.old_name::text, v_rename.new_name::text);
                    PERFORM sql_saga.__internal_execute_as_caller(pg_catalog.format('CREATE OR REPLACE TRIGGER %I INSTEAD OF INSERT OR UPDATE OR DELETE ON %s FOR EACH ROW EXECUTE PROCEDURE sql_saga.%s(%s)',
                        v_row.trigger_name, v_view.view_oid::pg_catalog.regclass,
                        CASE v_row.view_type WHEN 'current' THEN 'current_view_trigger' ELSE 'for_portion_of_trigger' END,
                        (SELECT pg_catalog.string_agg(pg_catalog.quote_literal(a.arg), ', ' ORDER BY a.n)
                         FROM pg_catalog.unnest(v_trigger_args) WITH ORDINALITY AS a(arg, n))));
                    RAISE NOTICE 'sql_saga: Recreated trigger "%" on % view "%" (CREATE OR REPLACE TRIGGER) with identifier column "%" renamed to "%"',
                        v_row.trigger_name, v_row.view_type, sql_saga.__internal_display_name(v_view.view_oid::pg_catalog.regclass),
                        v_rename.old_name, v_rename.new_name;
                END IF;

                IF v_rename.old_name = ANY (v_row.ephemeral_columns) THEN
                    v_row.ephemeral_columns := pg_catalog.array_replace(v_row.ephemeral_columns, v_rename.old_name, v_rename.new_name);
                    UPDATE sql_saga.updatable_view AS uv
                    SET ephemeral_columns = v_row.ephemeral_columns
                    WHERE (uv.view_schema, uv.view_name) = (v_row.view_schema, v_row.view_name);
                END IF;
            END LOOP;
        ELSIF v_view_columns IS DISTINCT FROM v_base_columns THEN
            v_replaced := false;
            IF v_view_columns = v_base_columns[1:pg_catalog.cardinality(v_view_columns)] THEN
                -- Columns were appended: extend the view in place (same OID, grants, comments, rules,
                -- triggers), repeating its current view options, which CREATE OR REPLACE VIEW would reset.
                -- Inside another extension's script PostgreSQL refuses to replace a view that extension does not
                -- own (SQLSTATE 55000); the subtransaction is rolled back and the drop-and-recreate path below is
                -- taken instead. Any other 55000 from this statement is handled the same way: the fallback either
                -- rebuilds the view completely or refuses with its own message, so nothing is silently lost.
                BEGIN
                    PERFORM sql_saga.__internal_execute_as_caller(
                        sql_saga.__internal_updatable_view_create_sql(v_row, true,
                            (SELECT c.reloptions FROM pg_catalog.pg_class AS c WHERE c.oid = v_view.view_oid),
                            CASE WHEN v_row.view_type = 'current' THEN sql_saga.__internal_current_view_clock(v_view.view_oid) END));
                    v_replaced := true;
                EXCEPTION WHEN object_not_in_prerequisite_state THEN
                    /*
                     * Inside an extension script (CREATE EXTENSION, ALTER EXTENSION ... UPDATE) PostgreSQL
                     * refuses CREATE OR REPLACE of an object that extension does not own ("An extension is not
                     * allowed to replace an object that it does not own"). The drop and recreate below
                     * gives the same view (new OID) and keeps exactly its extension membership, so it is a
                     * correct fallback whatever raised this condition.
                     */
                    RAISE DEBUG 'sql_saga: CREATE OR REPLACE VIEW of % refused (%), recreating it instead', v_view.view_oid::pg_catalog.regclass, SQLERRM;
                END;
            END IF;
            IF v_replaced THEN
                SELECT pg_catalog.string_agg(pg_catalog.quote_ident(a.attname), ', ' ORDER BY a.attnum)
                INTO v_added
                FROM pg_catalog.pg_attribute AS a
                WHERE a.attrelid = v_view.table_oid AND a.attnum > 0 AND NOT a.attisdropped
                  -- By name and type: dropped-column holes make attnums no count of the view's columns.
                  AND NOT pg_catalog.format('%I %s', a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod)) = ANY (v_view_columns);
                RAISE NOTICE 'sql_saga: Replaced % view "%" (CREATE OR REPLACE VIEW, keeping its view options) to add column(s) % of table "%"',
                    v_row.view_type, sql_saga.__internal_display_name(v_view.view_oid::pg_catalog.regclass), v_added,
                    sql_saga.__internal_display_name(v_view.table_oid::pg_catalog.regclass);
            ELSE
                -- The view's columns are no prefix of the base's, or CREATE OR REPLACE was refused inside
                -- an extension script: drop and recreate it.
                v_dependent := sql_saga.__internal_updatable_view_foreign_dependent(v_view.view_oid);
                IF v_dependent IS NOT NULL THEN
                    RAISE EXCEPTION 'cannot change the columns of table "%" because sql_saga must rebuild its managed % view "%", and % depends on that view',
                        sql_saga.__internal_display_name(v_view.table_oid::pg_catalog.regclass), v_row.view_type,
                        sql_saga.__internal_display_name(v_view.view_oid::pg_catalog.regclass), v_dependent
                        USING ERRCODE = 'dependent_objects_still_exist',
                              HINT = 'Drop the dependent object, run the ALTER TABLE, then recreate the dependent object.';
                END IF;
                PERFORM sql_saga.__internal_updatable_view_record_and_drop(v_row);
                PERFORM sql_saga.__internal_updatable_view_restore_pending();
                CONTINUE; -- restore recomputed generated_columns
            END IF;
        END IF;

        v_generated := sql_saga.__internal_updatable_view_generated_columns(v_view.table_oid);
        IF v_row.generated_columns IS DISTINCT FROM v_generated THEN
            UPDATE sql_saga.updatable_view AS uv
            SET generated_columns = v_generated
            WHERE (uv.view_schema, uv.view_name) = (v_row.view_schema, v_row.view_name);
        END IF;
    END LOOP;
END;
$__internal_updatable_view_follow_shape$;

REVOKE ALL ON FUNCTION sql_saga.__internal_updatable_view_create_sql(sql_saga.updatable_view, boolean, text[], text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_updatable_view_generated_columns(oid) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_updatable_view_foreign_dependent(oid) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_updatable_view_extension(oid) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_updatable_view_apply_acl(oid, name, aclitem[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_updatable_view_record_and_drop(sql_saga.updatable_view) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_updatable_view_restore(sql_saga.__internal_updatable_view_pending) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_updatable_view_restore_pending() FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_updatable_view_showing_columns(oid, text[], boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_updatable_view_prepare_drop_column(oid, text[], boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_updatable_view_drop_is_managed(oid, text[], boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_updatable_view_prepare_alter_type(oid, text[], text[], boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_updatable_view_follow_shape(oid[]) FROM PUBLIC;
