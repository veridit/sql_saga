CREATE FUNCTION sql_saga.__internal_refuse_managed_schema_rename(schema_name text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = sql_saga, pg_catalog, public
AS $__internal_refuse_managed_schema_rename$
#variable_conflict use_variable
DECLARE
    managed record;
    object_display_name text;
    base_display_name text;
BEGIN
    SELECT candidate.managed_class, candidate.object_schema, candidate.object_name,
           candidate.base_schema, candidate.base_table, candidate.era_name, candidate.detail
    INTO managed
    FROM (
        SELECT 1 AS priority, 'system_versioning_base' AS managed_class,
               sv.table_schema AS object_schema, sv.table_name AS object_name,
               sv.table_schema AS base_schema, sv.table_name AS base_table,
               sv.era_name, NULL::text AS detail
        FROM sql_saga.system_versioning AS sv
        WHERE sv.table_schema = schema_name

        UNION ALL

        SELECT 2, 'era', e.table_schema, e.table_name,
               e.table_schema, e.table_name, e.era_name, NULL::text
        FROM sql_saga.era AS e
        WHERE e.table_schema = schema_name

        UNION ALL

        SELECT 3, 'foreign_key', fk.table_schema, fk.table_name,
               fk.table_schema, fk.table_name, fk.fk_era_name, fk.foreign_key_name::text
        FROM sql_saga.foreign_keys AS fk
        WHERE fk.table_schema = schema_name

        UNION ALL

        SELECT 4, 'history_table', sv.history_schema_name, sv.history_table_name,
               sv.table_schema, sv.table_name, sv.era_name, NULL::text
        FROM sql_saga.system_versioning AS sv
        WHERE sv.history_schema_name = schema_name

        UNION ALL

        SELECT 5, 'system_versioning_view', sv.view_schema_name, sv.view_table_name,
               sv.table_schema, sv.table_name, sv.era_name, NULL::text
        FROM sql_saga.system_versioning AS sv
        WHERE sv.view_schema_name = schema_name

        UNION ALL

        SELECT 5, 'system_versioning_view', sv.history_schema_name,
               sql_saga.__internal_history_gated_name(sv.history_table_name),
               sv.table_schema, sv.table_name, sv.era_name, NULL::text
        FROM sql_saga.system_versioning AS sv
        WHERE sv.history_schema_name = schema_name

        UNION ALL

        SELECT 6, 'system_versioning_function', n.nspname, p.proname,
               sv.table_schema, sv.table_name, sv.era_name, NULL::text
        FROM sql_saga.system_versioning AS sv
        CROSS JOIN LATERAL unnest(ARRAY[
            sv.func_as_of,
            sv.func_between,
            sv.func_between_symmetric,
            sv.func_from_to
        ]) AS f (signature)
        JOIN pg_catalog.pg_proc AS p ON p.oid = pg_catalog.to_regprocedure(f.signature)
        JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
        WHERE n.nspname = schema_name

        UNION ALL

        SELECT CASE uv.view_type WHEN 'current' THEN 7 ELSE 8 END,
               uv.view_type::text, uv.view_schema, uv.view_name,
               uv.table_schema, uv.table_name, uv.era_name, NULL::text
        FROM sql_saga.updatable_view AS uv
        WHERE uv.view_schema = schema_name
    ) AS candidate
    ORDER BY candidate.object_schema::text COLLATE "C",
             candidate.object_name::text COLLATE "C", candidate.priority,
             candidate.era_name::text COLLATE "C", candidate.detail COLLATE "C"
    LIMIT 1;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    object_display_name := sql_saga.__internal_display_name(
        pg_catalog.format('%I.%I', managed.object_schema, managed.object_name));
    base_display_name := sql_saga.__internal_display_name(
        pg_catalog.format('%I.%I', managed.base_schema, managed.base_table));

    IF managed.managed_class = 'system_versioning_base' THEN
        RAISE EXCEPTION 'cannot rename schema "%" because it contains managed system-versioned table "%"; drop system versioning first with sql_saga.drop_system_versioning(table_oid regclass, drop_behavior sql_saga.drop_behavior, cleanup boolean)', schema_name, object_display_name
            USING ERRCODE = 'object_in_use';
    ELSIF managed.managed_class = 'era' THEN
        RAISE EXCEPTION 'cannot rename schema "%" because it contains managed table "%" with era "%"; drop dependent keys and foreign keys, then drop the era first with sql_saga.drop_era(table_oid regclass, era_name name, drop_behavior sql_saga.drop_behavior, cleanup boolean)', schema_name, object_display_name, managed.era_name
            USING ERRCODE = 'object_in_use';
    ELSIF managed.managed_class = 'foreign_key' THEN
        RAISE EXCEPTION 'cannot rename schema "%" because it contains managed table "%" with foreign key "%"; drop it first with sql_saga.drop_foreign_key_by_name(table_oid regclass, key_name name, drop_index boolean)', schema_name, object_display_name, managed.detail
            USING ERRCODE = 'object_in_use';
    ELSIF managed.managed_class = 'history_table' THEN
        RAISE EXCEPTION 'cannot rename schema "%" because it contains managed history table "%" for table "%"; drop system versioning first with sql_saga.drop_system_versioning(table_oid regclass, drop_behavior sql_saga.drop_behavior, cleanup boolean)', schema_name, object_display_name, base_display_name
            USING ERRCODE = 'object_in_use';
    ELSIF managed.managed_class IN ('system_versioning_view', 'system_versioning_function') THEN
        RAISE EXCEPTION 'cannot rename schema "%" because it contains managed % "%" for table "%"; drop system versioning first with sql_saga.drop_system_versioning(table_oid regclass, drop_behavior sql_saga.drop_behavior, cleanup boolean)', schema_name,
            CASE managed.managed_class WHEN 'system_versioning_view' THEN 'view' ELSE 'query function' END,
            object_display_name, base_display_name
            USING ERRCODE = 'object_in_use';
    ELSIF managed.managed_class = 'current' THEN
        RAISE EXCEPTION 'cannot rename schema "%" because it contains managed view "%" for table "%"; drop it with sql_saga.drop_current_view(table_oid regclass, era_name name, drop_behavior sql_saga.drop_behavior) before renaming the schema', schema_name, object_display_name, base_display_name
            USING ERRCODE = 'object_in_use';
    ELSE
        RAISE EXCEPTION 'cannot rename schema "%" because it contains managed view "%" for table "%"; drop it with sql_saga.drop_for_portion_of_view(table_oid regclass, era_name name, drop_behavior sql_saga.drop_behavior, cleanup boolean) before renaming the schema', schema_name, object_display_name, base_display_name
            USING ERRCODE = 'object_in_use';
    END IF;
END;
$__internal_refuse_managed_schema_rename$;

REVOKE ALL ON FUNCTION sql_saga.__internal_refuse_managed_schema_rename(text) FROM PUBLIC;

/*
 * The supported way to move or rename a system-versioned table together with its history
 * (README, "Moving a system-versioned table"), named by the identity-change refusals.
 */
CREATE FUNCTION sql_saga.__internal_system_versioning_move_advice(
    base_schema name, base_name name, history_schema name, history_name name)
RETURNS text
LANGUAGE sql
IMMUTABLE
SET search_path = pg_catalog
AS $__internal_system_versioning_move_advice$
    SELECT pg_catalog.format(
        'run sql_saga.drop_system_versioning(%1$L, cleanup => false) and sql_saga.drop_system_time_era(%1$L, cleanup => false), '
        'move or rename table "%2$s" and history table "%3$s" together, '
        'then run sql_saga.add_system_versioning(table_oid, history_table_name => name, history_table_must_exist => true)',
        pg_catalog.format('%I.%I', base_schema, base_name),
        pg_catalog.array_to_string(pg_catalog.parse_ident(pg_catalog.format('%I.%I', base_schema, base_name)), '.'),
        pg_catalog.array_to_string(pg_catalog.parse_ident(pg_catalog.format('%I.%I', history_schema, history_name)), '.'));
$__internal_system_versioning_move_advice$;

REVOKE ALL ON FUNCTION sql_saga.__internal_system_versioning_move_advice(name, name, name, name) FROM PUBLIC;

CREATE FUNCTION sql_saga.__internal_refuse_managed_relation_identity_change(relation_oid oid)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = sql_saga, pg_catalog, public
AS $__internal_refuse_managed_relation_identity_change$
#variable_conflict use_variable
DECLARE
    relation_schema name;
    relation_name name;
    relation_display_name text;
    base_display_name text;
    history_schema name;
    history_name name;
    managed record;
BEGIN
    SELECT n.nspname, c.relname,
           sql_saga.__internal_display_name(pg_catalog.format('%I.%I', n.nspname, c.relname))
    INTO relation_schema, relation_name, relation_display_name
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.oid = relation_oid;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    SELECT candidate.managed_class, candidate.base_schema, candidate.base_table,
           candidate.era_name, candidate.detail
    INTO managed
    FROM (
        SELECT 1 AS priority, 'system_versioning_base' AS managed_class,
               sv.table_schema AS base_schema, sv.table_name AS base_table,
               sv.era_name, NULL::name AS detail
        FROM sql_saga.system_versioning AS sv
        WHERE (sv.table_schema, sv.table_name) = (relation_schema, relation_name)

        UNION ALL

        SELECT 2, 'era', e.table_schema, e.table_name, e.era_name, NULL::name
        FROM sql_saga.era AS e
        WHERE (e.table_schema, e.table_name) = (relation_schema, relation_name)

        UNION ALL

        SELECT 3, 'foreign_key', fk.table_schema, fk.table_name, fk.fk_era_name,
               fk.foreign_key_name
        FROM sql_saga.foreign_keys AS fk
        WHERE (fk.table_schema, fk.table_name) = (relation_schema, relation_name)

        UNION ALL

        SELECT 4, 'history_table', sv.table_schema, sv.table_name, sv.era_name, NULL::name
        FROM sql_saga.system_versioning AS sv
        WHERE (sv.history_schema_name, sv.history_table_name) = (relation_schema, relation_name)

        UNION ALL

        SELECT 5, 'system_versioning_view', sv.table_schema, sv.table_name, sv.era_name, NULL::name
        FROM sql_saga.system_versioning AS sv
        WHERE (sv.view_schema_name, sv.view_table_name) = (relation_schema, relation_name)
           OR (sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name)) =
              (relation_schema, relation_name)

        UNION ALL

        SELECT CASE uv.view_type WHEN 'current' THEN 6 ELSE 7 END,
               uv.view_type::text, uv.table_schema, uv.table_name, uv.era_name, NULL::name
        FROM sql_saga.updatable_view AS uv
        WHERE (uv.view_schema, uv.view_name) = (relation_schema, relation_name)
    ) AS candidate
    ORDER BY candidate.priority, candidate.era_name, candidate.detail
    LIMIT 1;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    IF managed.managed_class = 'system_versioning_base' THEN
        SELECT sv.history_schema_name, sv.history_table_name INTO history_schema, history_name
        FROM sql_saga.system_versioning AS sv
        WHERE (sv.table_schema, sv.table_name) = (managed.base_schema, managed.base_table);
        RAISE EXCEPTION 'cannot rename or move system-versioned table "%"; to move it with its history, %', relation_display_name,
            sql_saga.__internal_system_versioning_move_advice(managed.base_schema, managed.base_table, history_schema, history_name)
            USING ERRCODE = 'object_in_use';
    ELSIF managed.managed_class = 'era' THEN
        RAISE EXCEPTION 'cannot rename or move table "%" because it has era "%"; drop dependent keys and foreign keys, then drop the era first with sql_saga.drop_era(table_oid regclass, era_name name, drop_behavior sql_saga.drop_behavior, cleanup boolean)', relation_display_name, managed.era_name
            USING ERRCODE = 'object_in_use';
    ELSIF managed.managed_class = 'foreign_key' THEN
        RAISE EXCEPTION 'cannot rename or move table "%" because it has foreign key "%"; drop it first with sql_saga.drop_foreign_key_by_name(table_oid regclass, key_name name, drop_index boolean)', relation_display_name, managed.detail
            USING ERRCODE = 'object_in_use';
    END IF;

    base_display_name := sql_saga.__internal_display_name(
        pg_catalog.format('%I.%I', managed.base_schema, managed.base_table));

    IF managed.managed_class = 'history_table' THEN
        RAISE EXCEPTION 'history table "%" is managed by sql_saga for table "%"; to move it with its table, %', relation_display_name, base_display_name,
            sql_saga.__internal_system_versioning_move_advice(managed.base_schema, managed.base_table, relation_schema, relation_name)
            USING ERRCODE = 'object_in_use';
    ELSIF managed.managed_class = 'system_versioning_view' THEN
        RAISE EXCEPTION 'view "%" is managed by sql_saga for table "%"; it cannot be renamed or moved; drop system versioning first with sql_saga.drop_system_versioning(table_oid regclass, drop_behavior sql_saga.drop_behavior, cleanup boolean)', relation_display_name, base_display_name
            USING ERRCODE = 'object_in_use';
    ELSIF managed.managed_class = 'current' THEN
        RAISE EXCEPTION 'view "%" is managed by sql_saga for table "%"; drop it with sql_saga.drop_current_view(table_oid regclass, era_name name, drop_behavior sql_saga.drop_behavior) before renaming or moving it', relation_display_name, base_display_name
            USING ERRCODE = 'object_in_use';
    ELSE
        RAISE EXCEPTION 'view "%" is managed by sql_saga for table "%"; drop it with sql_saga.drop_for_portion_of_view(table_oid regclass, era_name name, drop_behavior sql_saga.drop_behavior, cleanup boolean) before renaming or moving it', relation_display_name, base_display_name
            USING ERRCODE = 'object_in_use';
    END IF;
END;
$__internal_refuse_managed_relation_identity_change$;

REVOKE ALL ON FUNCTION sql_saga.__internal_refuse_managed_relation_identity_change(oid) FROM PUBLIC;

/*
 * Re-create the referenced-side triggers of a regular foreign key, under their stored names, with
 * arguments taken from the current sql_saga.foreign_keys row (used after a referencing column rename).
 * drop_protection refuses dropping a trigger that sql_saga.foreign_keys names, so the names are
 * withdrawn from the row for the duration of the DROP and restored afterwards (the metadata-first
 * pattern of drop_foreign_key_by_name). Everything runs in the caller's transaction; no setting or
 * argument lets a caller skip the protection.
 */
CREATE FUNCTION sql_saga.__internal_recreate_regular_fk_uk_triggers(foreign_key_name name)
RETURNS void
LANGUAGE plpgsql
SET search_path = sql_saga, pg_catalog
AS $__internal_recreate_regular_fk_uk_triggers$
#variable_conflict use_variable
DECLARE
    fk sql_saga.foreign_keys;
    uk sql_saga.unique_keys;
    uk_era sql_saga.era;
    uk_table_oid regclass;
    update_columns text;
    update_enabled "char";
    delete_enabled "char";
BEGIN
    SELECT * INTO fk FROM sql_saga.foreign_keys AS f
    WHERE f.foreign_key_name = foreign_key_name AND f.type = 'regular_to_temporal';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'sql_saga: regular foreign key "%" not found', foreign_key_name;
    END IF;
    SELECT * INTO uk FROM sql_saga.unique_keys AS u WHERE u.unique_key_name = fk.unique_key_name;
    SELECT * INTO uk_era FROM sql_saga.era AS e
    WHERE (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name);
    uk_table_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.table_name));
    IF uk_table_oid IS NULL OR uk_era.era_name IS NULL THEN
        RAISE EXCEPTION 'sql_saga: referenced table of foreign key "%" not found', foreign_key_name;
    END IF;

    SELECT pg_catalog.string_agg(pg_catalog.quote_ident(u.column_name), ', ' ORDER BY u.ordinality)
    INTO update_columns
    FROM pg_catalog.unnest(uk.column_names || uk_era.valid_from_column_name || uk_era.valid_until_column_name)
         WITH ORDINALITY AS u (column_name, ordinality);

    /* The firing mode (DISABLE / ENABLE / ENABLE REPLICA / ENABLE ALWAYS) of each trigger survives the rebuild. */
    SELECT t.tgenabled INTO update_enabled FROM pg_catalog.pg_trigger AS t
    WHERE t.tgrelid = uk_table_oid AND t.tgname = fk.uk_update_trigger;
    SELECT t.tgenabled INTO delete_enabled FROM pg_catalog.pg_trigger AS t
    WHERE t.tgrelid = uk_table_oid AND t.tgname = fk.uk_delete_trigger;

    UPDATE sql_saga.foreign_keys AS f
    SET uk_update_trigger = NULL, uk_delete_trigger = NULL
    WHERE f.foreign_key_name = fk.foreign_key_name;

    IF EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = uk_table_oid AND t.tgname = fk.uk_update_trigger) THEN
        EXECUTE pg_catalog.format('DROP TRIGGER %I ON %s', fk.uk_update_trigger, uk_table_oid);
    END IF;
    IF EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = uk_table_oid AND t.tgname = fk.uk_delete_trigger) THEN
        EXECUTE pg_catalog.format('DROP TRIGGER %I ON %s', fk.uk_delete_trigger, uk_table_oid);
    END IF;

    /* Same definitions as add_regular_foreign_key. */
    EXECUTE pg_catalog.format($$
        CREATE CONSTRAINT TRIGGER %1$I AFTER UPDATE OF %2$s ON %3$I.%4$I FROM %5$I.%6$I DEFERRABLE FOR EACH ROW EXECUTE PROCEDURE
        sql_saga.uk_update_check_c(%7$L, %5$L, %6$L, %8$L, %9$L, %10$L, %11$L, %3$L, %4$L, %12$L, %13$L, %14$L, %15$L, %16$L, %17$L, %18$L, 'regular_to_temporal');
        $$
        , /* %1$I */ fk.uk_update_trigger
        , /* %2$s */ update_columns
        , /* %3$I */ uk.table_schema
        , /* %4$I */ uk.table_name
        , /* %5$I */ fk.table_schema
        , /* %6$I */ fk.table_name
        , /* %7$L */ fk.foreign_key_name
        , /* %8$L */ fk.column_names::text
        , /* %9$L */ ''
        , /* %10$L*/ ''
        , /* %11$L*/ ''
        , /* %12$L*/ uk.column_names::text
        , /* %13$L*/ uk_era.era_name
        , /* %14$L*/ uk_era.valid_from_column_name
        , /* %15$L*/ uk_era.valid_until_column_name
        , /* %16$L*/ fk.match_type
        , /* %17$L*/ fk.update_action
        , /* %18$L*/ fk.delete_action
    );
    EXECUTE pg_catalog.format($$
        CREATE CONSTRAINT TRIGGER %1$I AFTER DELETE ON %2$I.%3$I FROM %4$I.%5$I DEFERRABLE FOR EACH ROW EXECUTE PROCEDURE
        sql_saga.uk_delete_check_c(%6$L, %4$L, %5$L, %7$L, %8$L, %9$L, %10$L, %2$L, %3$L, %11$L, %12$L, %13$L, %14$L, %15$L, %16$L, %17$L, 'regular_to_temporal');
        $$
        , /* %1$I */ fk.uk_delete_trigger
        , /* %2$I */ uk.table_schema
        , /* %3$I */ uk.table_name
        , /* %4$I */ fk.table_schema
        , /* %5$I */ fk.table_name
        , /* %6$L */ fk.foreign_key_name
        , /* %7$L */ fk.column_names::text
        , /* %8$L */ ''
        , /* %9$L */ ''
        , /* %10$L*/ ''
        , /* %11$L*/ uk.column_names::text
        , /* %12$L*/ uk_era.era_name
        , /* %13$L*/ uk_era.valid_from_column_name
        , /* %14$L*/ uk_era.valid_until_column_name
        , /* %15$L*/ fk.match_type
        , /* %16$L*/ fk.update_action
        , /* %17$L*/ fk.delete_action
    );

    /* A new trigger fires in origin mode ('O'); reapply each trigger's previous mode. */
    IF update_enabled IS DISTINCT FROM 'O' AND update_enabled IS NOT NULL THEN
        EXECUTE pg_catalog.format('ALTER TABLE %s %s TRIGGER %I', uk_table_oid,
            CASE update_enabled WHEN 'D' THEN 'DISABLE' WHEN 'R' THEN 'ENABLE REPLICA' WHEN 'A' THEN 'ENABLE ALWAYS' END,
            fk.uk_update_trigger);
    END IF;
    IF delete_enabled IS DISTINCT FROM 'O' AND delete_enabled IS NOT NULL THEN
        EXECUTE pg_catalog.format('ALTER TABLE %s %s TRIGGER %I', uk_table_oid,
            CASE delete_enabled WHEN 'D' THEN 'DISABLE' WHEN 'R' THEN 'ENABLE REPLICA' WHEN 'A' THEN 'ENABLE ALWAYS' END,
            fk.uk_delete_trigger);
    END IF;

    UPDATE sql_saga.foreign_keys AS f
    SET uk_update_trigger = fk.uk_update_trigger, uk_delete_trigger = fk.uk_delete_trigger
    WHERE f.foreign_key_name = fk.foreign_key_name;
END;
$__internal_recreate_regular_fk_uk_triggers$;

REVOKE ALL ON FUNCTION sql_saga.__internal_recreate_regular_fk_uk_triggers(name) FROM PUBLIC;

/*
 * The canonical body of a regular foreign key's helper function, for the current columns of the
 * referenced unique key: exactly what add_regular_foreign_key generates. NULL when a key column is
 * missing or has no default btree equality operator.
 */
CREATE FUNCTION sql_saga.__internal_regular_fk_helper_body(foreign_key_name name)
RETURNS text
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_regular_fk_helper_body$
    SELECT CASE WHEN w.missing OR w.found_columns IS DISTINCT FROM w.stable_columns THEN NULL
           ELSE pg_catalog.format($body$
                SELECT EXISTS (SELECT 1 FROM %I.%I AS uk WHERE %s);
            $body$, uk.table_schema, uk.table_name, w.where_clause)
           END
    FROM sql_saga.foreign_keys AS fk
    JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
    JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
    CROSS JOIN LATERAL (
        SELECT pg_catalog.string_agg(pg_catalog.format('uk.%I OPERATOR(%I.=) $%s',
                                                       u.name, coalesce(eq.nspname, ''), u.ordinality),
                                     ' AND ' ORDER BY u.ordinality) AS where_clause,
               pg_catalog.bool_or(eq.oid IS NULL) AS missing,
               pg_catalog.count(*) AS found_columns,
               (SELECT pg_catalog.count(*) FROM pg_catalog.unnest(uk.column_names) AS c(name)
                WHERE c.name OPERATOR(pg_catalog.<>) e.range_column_name) AS stable_columns
        FROM pg_catalog.unnest(ARRAY(
                 SELECT c.name FROM pg_catalog.unnest(uk.column_names) WITH ORDINALITY AS c(name, n)
                 WHERE c.name OPERATOR(pg_catalog.<>) e.range_column_name ORDER BY c.n))
             WITH ORDINALITY AS u(name, ordinality)
        JOIN pg_catalog.pg_attribute AS ua
          ON ua.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.table_name))
         AND ua.attname = u.name AND ua.attnum > 0 AND NOT ua.attisdropped
        LEFT JOIN LATERAL (
            SELECT o.oid, n.nspname
            FROM pg_catalog.pg_opclass AS opc
            JOIN pg_catalog.pg_am AS am ON am.oid = opc.opcmethod AND am.amname = 'btree'
            JOIN pg_catalog.pg_amop AS amop ON amop.amopfamily = opc.opcfamily
                 AND amop.amoplefttype = opc.opcintype AND amop.amoprighttype = opc.opcintype
                 AND amop.amopstrategy = 3
            JOIN pg_catalog.pg_operator AS o ON o.oid = amop.amopopr
            JOIN pg_catalog.pg_namespace AS n ON n.oid = o.oprnamespace
            WHERE opc.opcdefault
              AND opc.opcintype = (SELECT coalesce(nullif(t.typbasetype, 0), t.oid)
                                   FROM pg_catalog.pg_type AS t WHERE t.oid = ua.atttypid)
            LIMIT 1
        ) AS eq ON true
    ) AS w
    WHERE fk.foreign_key_name = $1 AND fk.type = 'regular_to_temporal';
$__internal_regular_fk_helper_body$;

REVOKE ALL ON FUNCTION sql_saga.__internal_regular_fk_helper_body(name) FROM PUBLIC;

/*
 * The canonical arguments (pg_trigger.tgargs) of a regular foreign key's referenced-side triggers,
 * built from the catalog exactly as add_regular_foreign_key passes them.
 */
CREATE FUNCTION sql_saga.__internal_regular_fk_trigger_args(foreign_key_name name)
RETURNS bytea
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_regular_fk_trigger_args$
    SELECT pg_catalog.string_agg(
               pg_catalog.convert_to(a.arg, pg_catalog.getdatabaseencoding()) OPERATOR(pg_catalog.||) '\x00'::bytea,
               ''::bytea ORDER BY a.n)
    FROM sql_saga.foreign_keys AS fk
    JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
    JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
    CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[
        fk.foreign_key_name::text, fk.table_schema::text, fk.table_name::text, fk.column_names::text,
        '', '', '',
        uk.table_schema::text, uk.table_name::text, uk.column_names::text, e.era_name::text,
        coalesce(e.valid_from_column_name::text, 'null'), coalesce(e.valid_until_column_name::text, 'null'),
        fk.match_type::text, fk.update_action::text, fk.delete_action::text, 'regular_to_temporal'
    ]) WITH ORDINALITY AS a(arg, n)
    WHERE fk.foreign_key_name = $1 AND fk.type = 'regular_to_temporal';
$__internal_regular_fk_trigger_args$;

REVOKE ALL ON FUNCTION sql_saga.__internal_regular_fk_trigger_args(name) FROM PUBLIC;

/*
 * Whether a regular foreign key's helper function body and both referenced-side trigger argument
 * lists are the canonical ones for the current catalog (used by check_integrity).
 */
CREATE FUNCTION sql_saga.__internal_regular_foreign_key_is_attributable(foreign_key_name name)
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_regular_foreign_key_is_attributable$
    SELECT coalesce((
        SELECT pg_catalog.btrim(p.prosrc, E' \t\r\n')
               OPERATOR(pg_catalog.=) pg_catalog.btrim(sql_saga.__internal_regular_fk_helper_body(fk.foreign_key_name), E' \t\r\n')
        FROM pg_catalog.pg_proc AS p
        WHERE p.oid = pg_catalog.to_regprocedure(fk.fk_helper_function)
    ) AND NOT EXISTS (
        SELECT FROM pg_catalog.pg_trigger AS t
        WHERE t.tgrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.table_name))
          AND t.tgname IN (fk.uk_update_trigger, fk.uk_delete_trigger)
          AND t.tgargs IS DISTINCT FROM sql_saga.__internal_regular_fk_trigger_args(fk.foreign_key_name)
    ), false)
    FROM sql_saga.foreign_keys AS fk
    JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
    WHERE fk.foreign_key_name = $1 AND fk.type = 'regular_to_temporal';
$__internal_regular_foreign_key_is_attributable$;

REVOKE ALL ON FUNCTION sql_saga.__internal_regular_foreign_key_is_attributable(name) FROM PUBLIC;

/*
 * The canonical body of a regular foreign key helper function for the given referenced key columns
 * (stable columns, in key order). Each column is compared with the default btree equality operator
 * of the helper's argument type at the same position; add_regular_foreign_key requires equal
 * referencing and referenced column types, so this equals the body generated from the referenced
 * table, and it does not need the named columns to exist (it also describes the helper as it was
 * before a referenced column rename). NULL when an argument type has no such operator or the column
 * count differs from the argument count.
 */
CREATE FUNCTION sql_saga.__internal_regular_fk_helper_body_for(helper regprocedure, uk_schema name, uk_table name, uk_columns name[])
RETURNS text
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_regular_fk_helper_body_for$
    SELECT CASE WHEN w.missing OR w.found_columns IS DISTINCT FROM p.pronargs::bigint THEN NULL
           ELSE pg_catalog.format($body$
                SELECT EXISTS (SELECT 1 FROM %I.%I AS uk WHERE %s);
            $body$, uk_schema, uk_table, w.where_clause)
           END
    FROM pg_catalog.pg_proc AS p
    CROSS JOIN LATERAL (
        SELECT pg_catalog.string_agg(pg_catalog.format('uk.%I OPERATOR(%I.=) $%s',
                                                       u.name, coalesce(eq.nspname, ''), u.ordinality),
                                     ' AND ' ORDER BY u.ordinality) AS where_clause,
               pg_catalog.bool_or(eq.oid IS NULL) AS missing,
               pg_catalog.count(*) AS found_columns
        FROM pg_catalog.unnest(uk_columns) WITH ORDINALITY AS u(name, ordinality)
        LEFT JOIN LATERAL (
            SELECT o.oid, n.nspname
            FROM pg_catalog.pg_opclass AS opc
            JOIN pg_catalog.pg_am AS am ON am.oid = opc.opcmethod AND am.amname = 'btree'
            JOIN pg_catalog.pg_amop AS amop ON amop.amopfamily = opc.opcfamily
                 AND amop.amoplefttype = opc.opcintype AND amop.amoprighttype = opc.opcintype
                 AND amop.amopstrategy = 3
            JOIN pg_catalog.pg_operator AS o ON o.oid = amop.amopopr
            JOIN pg_catalog.pg_namespace AS n ON n.oid = o.oprnamespace
            WHERE opc.opcdefault
              AND opc.opcintype = (SELECT coalesce(nullif(t.typbasetype, 0), t.oid)
                                   FROM pg_catalog.pg_type AS t
                                   WHERE t.oid = p.proargtypes[(u.ordinality - 1)::integer])
            LIMIT 1
        ) AS eq ON true
    ) AS w
    WHERE p.oid = helper;
$__internal_regular_fk_helper_body_for$;

REVOKE ALL ON FUNCTION sql_saga.__internal_regular_fk_helper_body_for(regprocedure, name, name, name[]) FROM PUBLIC;

/*
 * Whether a function is exactly the helper add_regular_foreign_key generates for the given
 * referenced key columns: an SQL, STABLE STRICT, SECURITY DEFINER boolean function with only IN
 * arguments, the canonical body, and search_path = pg_catalog as its only search_path setting
 * (other per-function settings are the owner's choice and are kept).
 */
CREATE FUNCTION sql_saga.__internal_regular_fk_helper_is_canonical(helper regprocedure, uk_schema name, uk_table name, uk_columns name[])
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_regular_fk_helper_is_canonical$
    SELECT coalesce((
        SELECT p.prolang OPERATOR(pg_catalog.=) (SELECT l.oid FROM pg_catalog.pg_language AS l WHERE l.lanname OPERATOR(pg_catalog.=) 'sql')
           AND p.prorettype OPERATOR(pg_catalog.=) 'pg_catalog.bool'::pg_catalog.regtype
           AND p.prokind OPERATOR(pg_catalog.=) 'f' AND NOT p.proretset
           AND p.proisstrict AND p.provolatile OPERATOR(pg_catalog.=) 's'
           AND p.prosecdef
           AND p.proargmodes IS NULL
           AND 'search_path=pg_catalog' OPERATOR(pg_catalog.=) ANY (p.proconfig)
           AND NOT EXISTS (
               SELECT FROM pg_catalog.unnest(p.proconfig) AS c(setting)
               WHERE c.setting LIKE 'search\_path=%' AND c.setting OPERATOR(pg_catalog.<>) 'search_path=pg_catalog')
           AND pg_catalog.btrim(p.prosrc, E' \t\r\n') OPERATOR(pg_catalog.=)
               pg_catalog.btrim(sql_saga.__internal_regular_fk_helper_body_for(helper, uk_schema, uk_table, uk_columns), E' \t\r\n')
        FROM pg_catalog.pg_proc AS p
        WHERE p.oid OPERATOR(pg_catalog.=) helper
    ), false);
$__internal_regular_fk_helper_is_canonical$;

REVOKE ALL ON FUNCTION sql_saga.__internal_regular_fk_helper_is_canonical(regprocedure, name, name, name[]) FROM PUBLIC;

/*
 * Regenerate, in place, the helper function of a regular foreign key with the canonical body for
 * the current referenced key columns (after a referenced-column rename). Only the body changes:
 * the function is re-created from its own pg_get_functiondef header, so its identity, owner,
 * privileges, SECURITY DEFINER and every per-function setting (proconfig) are kept, and the CHECK
 * constraints that call it stay valid. The caller has attributed the helper beforehand.
 */
CREATE FUNCTION sql_saga.__internal_regenerate_regular_fk_helper(foreign_key_name name)
RETURNS void
LANGUAGE plpgsql
SET search_path = sql_saga, pg_catalog
AS $__internal_regenerate_regular_fk_helper$
#variable_conflict use_variable
DECLARE
    helper regprocedure;
    body text;
    definition text;
    old_source text;
    dollar_tag text := '$function';
    old_suffix text;
BEGIN
    SELECT pg_catalog.to_regprocedure(fk.fk_helper_function) INTO helper
    FROM sql_saga.foreign_keys AS fk
    WHERE fk.foreign_key_name = foreign_key_name AND fk.type = 'regular_to_temporal';
    body := sql_saga.__internal_regular_fk_helper_body(foreign_key_name);
    IF helper IS NULL OR body IS NULL THEN
        RAISE EXCEPTION 'sql_saga: cannot regenerate the helper function of foreign key "%"', foreign_key_name;
    END IF;
    definition := pg_catalog.pg_get_functiondef(helper);
    SELECT p.prosrc INTO old_source FROM pg_catalog.pg_proc AS p WHERE p.oid = helper;
    -- pg_get_functiondef quotes the body with "$function", extended by "x" while the body contains
    -- that prefix, then "$". Derive the same tag, so the expected suffix is exact for any body.
    WHILE pg_catalog.strpos(old_source, dollar_tag) > 0 LOOP
        dollar_tag := dollar_tag || 'x';
    END LOOP;
    dollar_tag := dollar_tag || '$';
    old_suffix := 'AS ' || dollar_tag || old_source || dollar_tag || E'\n';
    IF pg_catalog.left(definition, 27) IS DISTINCT FROM 'CREATE OR REPLACE FUNCTION '
       OR pg_catalog.right(definition, pg_catalog.length(old_suffix)) IS DISTINCT FROM old_suffix THEN
        RAISE EXCEPTION 'sql_saga: cannot regenerate the helper function of foreign key "%"', foreign_key_name;
    END IF;
    EXECUTE pg_catalog.left(definition, pg_catalog.length(definition) - pg_catalog.length(old_suffix))
        || pg_catalog.format('AS %L', body);
END;
$__internal_regenerate_regular_fk_helper$;

REVOKE ALL ON FUNCTION sql_saga.__internal_regenerate_regular_fk_helper(name) FROM PUBLIC;

/*
 * Before a referenced key column rename changes anything: for the unique keys on the given
 * relations (those touched by the ALTER TABLE) that are referenced by a regular foreign key and
 * have a catalogued column missing from the table (a renamed key column), require that every such
 * foreign key's helper function and referenced-side triggers are exactly what sql_saga generated for
 * the columns before the rename. Refuses otherwise, so nothing a user changed is overwritten.
 * Returns the key columns before the rename, by unique key name.
 */
CREATE FUNCTION sql_saga.__internal_regular_fk_key_rename_prepare(table_oids oid[])
RETURNS jsonb
LANGUAGE plpgsql
SET search_path = sql_saga, pg_catalog
AS $__internal_regular_fk_key_rename_prepare$
#variable_conflict use_variable
DECLARE
    uk_rec record;
    fk_rec record;
    stable_columns name[];
    columns_before jsonb := '{}'::jsonb;
BEGIN
    FOR uk_rec IN
        SELECT uk.unique_key_name, uk.table_schema, uk.table_name, uk.column_names, e.range_column_name, c.oid AS uk_table_oid
        FROM pg_catalog.pg_class AS c
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
        JOIN sql_saga.unique_keys AS uk ON (uk.table_schema, uk.table_name) = (n.nspname, c.relname)
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
        WHERE c.oid = ANY (table_oids)
          AND EXISTS (SELECT FROM sql_saga.foreign_keys AS fk
                      WHERE fk.unique_key_name = uk.unique_key_name AND fk.type = 'regular_to_temporal')
          AND EXISTS (SELECT FROM pg_catalog.unnest(uk.column_names) AS col(name)
                      WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a
                                        WHERE a.attrelid = c.oid AND a.attname = col.name
                                          AND a.attnum > 0 AND NOT a.attisdropped))
        ORDER BY uk.unique_key_name COLLATE "C"
    LOOP
        stable_columns := ARRAY(
            SELECT col.name FROM pg_catalog.unnest(uk_rec.column_names) WITH ORDINALITY AS col(name, n)
            WHERE col.name <> uk_rec.range_column_name ORDER BY col.n);
        FOR fk_rec IN
            SELECT fk.foreign_key_name, fk.fk_helper_function, fk.uk_update_trigger, fk.uk_delete_trigger
            FROM sql_saga.foreign_keys AS fk
            WHERE fk.unique_key_name = uk_rec.unique_key_name AND fk.type = 'regular_to_temporal'
            ORDER BY fk.foreign_key_name COLLATE "C"
        LOOP
            IF NOT sql_saga.__internal_regular_fk_helper_is_canonical(
                    pg_catalog.to_regprocedure(fk_rec.fk_helper_function),
                    uk_rec.table_schema, uk_rec.table_name, stable_columns) THEN
                RAISE EXCEPTION 'sql_saga: cannot rename a column of unique key "%" on table "%.%": helper function % of regular foreign key "%" is not the canonical sql_saga helper; restore it, or drop the foreign key with drop_foreign_key first',
                    uk_rec.unique_key_name, uk_rec.table_schema, uk_rec.table_name, fk_rec.fk_helper_function, fk_rec.foreign_key_name;
            END IF;
            IF EXISTS (
                SELECT FROM pg_catalog.pg_trigger AS t
                WHERE t.tgrelid = uk_rec.uk_table_oid
                  AND t.tgname IN (fk_rec.uk_update_trigger, fk_rec.uk_delete_trigger)
                  AND (t.tgfoid NOT IN ('sql_saga.uk_update_check_c()'::regprocedure, 'sql_saga.uk_delete_check_c()'::regprocedure)
                       OR t.tgargs IS DISTINCT FROM sql_saga.__internal_regular_fk_trigger_args(fk_rec.foreign_key_name))) THEN
                RAISE EXCEPTION 'sql_saga: cannot rename a column of unique key "%" on table "%.%": a referenced-side trigger of regular foreign key "%" is not the canonical sql_saga trigger; repair with drop_foreign_key and add_regular_foreign_key first',
                    uk_rec.unique_key_name, uk_rec.table_schema, uk_rec.table_name, fk_rec.foreign_key_name;
            END IF;
        END LOOP;
        columns_before := columns_before || pg_catalog.jsonb_build_object(uk_rec.unique_key_name, uk_rec.column_names);
    END LOOP;
    RETURN columns_before;
END;
$__internal_regular_fk_key_rename_prepare$;

REVOKE ALL ON FUNCTION sql_saga.__internal_regular_fk_key_rename_prepare(oid[]) FROM PUBLIC;

/*
 * After the unique key columns followed a rename: regenerate, in place, the helper function and the
 * referenced-side triggers of every regular foreign key on a key whose columns changed (same names,
 * owner, privileges, settings and firing modes; the helper body and the trigger arguments name the
 * current columns). columns_before comes from __internal_regular_fk_key_rename_prepare.
 */
CREATE FUNCTION sql_saga.__internal_regular_fk_key_rename_apply(columns_before jsonb)
RETURNS void
LANGUAGE plpgsql
SET search_path = sql_saga, pg_catalog
AS $__internal_regular_fk_key_rename_apply$
#variable_conflict use_variable
DECLARE
    fk_rec record;
BEGIN
    FOR fk_rec IN
        SELECT fk.foreign_key_name
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        WHERE fk.type = 'regular_to_temporal'
          AND columns_before ? uk.unique_key_name::text
          AND (columns_before -> uk.unique_key_name::text) IS DISTINCT FROM pg_catalog.to_jsonb(uk.column_names)
        ORDER BY fk.foreign_key_name COLLATE "C"
    LOOP
        PERFORM sql_saga.__internal_regenerate_regular_fk_helper(fk_rec.foreign_key_name);
        PERFORM sql_saga.__internal_recreate_regular_fk_uk_triggers(fk_rec.foreign_key_name);
    END LOOP;
END;
$__internal_regular_fk_key_rename_apply$;

REVOKE ALL ON FUNCTION sql_saga.__internal_regular_fk_key_rename_apply(jsonb) FROM PUBLIC;

/*
 * Whether a CHECK constraint definition (pg_get_constraintdef) is the boundary check add_era creates
 * for the given boundary columns: CHECK ((from < until)), or, for subtypes with infinity,
 * CHECK (((from < until) AND (from > '-infinity'::subtype))) (rendered per subtype, e.g.
 * '-infinity'::date, '-Infinity'::numeric, '-infinity'::timestamp with time zone).
 */
CREATE FUNCTION sql_saga.__internal_era_boundary_check_matches(definition text, valid_from_column_name name, valid_until_column_name name)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_era_boundary_check_matches$
    SELECT $1 OPERATOR(pg_catalog.=) pg_catalog.format('CHECK ((%I < %I))', $2, $3)
        OR (pg_catalog.starts_with($1, pg_catalog.format('CHECK (((%I < %I) AND (%I > ''-', $2, $3, $2))
            AND pg_catalog.substr($1, pg_catalog.length(pg_catalog.format('CHECK (((%I < %I) AND (%I > ''-', $2, $3, $2)) OPERATOR(pg_catalog.+) 1)
                OPERATOR(pg_catalog.~) '^[Ii]nfinity''::[a-z0-9 ]+\)\)\)$');
$__internal_era_boundary_check_matches$;

REVOKE ALL ON FUNCTION sql_saga.__internal_era_boundary_check_matches(text, name, name) FROM PUBLIC;

/*
 * pg_get_constraintdef of a constraint of one of the given relations, NULL for any other constraint
 * (so it matches no expected definition). rename_following compares definitions only for the
 * relations its DDL command touched: deparsing a CHECK constraint locks the constraint's table, which
 * a concurrent session may hold exclusively (see rename_following). The CASE, unlike a WHERE
 * condition next to the deparse, guarantees that the definition of another relation is never read,
 * whatever order the planner evaluates the conditions in.
 */
CREATE FUNCTION sql_saga.__internal_constraintdef_if_touched(constraint_oid oid, table_oid oid, touched_relation_oids oid[])
RETURNS text
LANGUAGE sql
STABLE
SET search_path = pg_catalog
AS $__internal_constraintdef_if_touched$
    SELECT CASE WHEN table_oid = ANY (touched_relation_oids) THEN pg_catalog.pg_get_constraintdef(constraint_oid) END;
$__internal_constraintdef_if_touched$;

REVOKE ALL ON FUNCTION sql_saga.__internal_constraintdef_if_touched(oid, oid, oid[]) FROM PUBLIC;

/*
 * Whether a regular foreign key's referenced-side trigger is exactly what add_regular_foreign_key
 * creates (see __internal_recreate_regular_fk_uk_triggers): a non-internal constraint trigger on the
 * referenced table FROM the referencing table, DEFERRABLE INITIALLY IMMEDIATE, row level, AFTER
 * UPDATE OF the key columns and bounds (update) or AFTER DELETE (delete), with the role's C
 * function, no WHEN qualifier, no transition tables, and the canonical arguments for the current
 * catalog. Only the firing mode (tgenabled) may differ. update_of_attnums: the expected UPDATE OF
 * columns of the update trigger (by attnum, so the caller can name them after a column rename).
 */
CREATE FUNCTION sql_saga.__internal_regular_fk_uk_trigger_is_canonical(foreign_key_name name, trigger_oid oid, is_update boolean, update_of_attnums smallint[])
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = sql_saga, pg_catalog
AS $__internal_regular_fk_uk_trigger_is_canonical$
    SELECT coalesce((
        SELECT NOT t.tgisinternal
           AND t.tgfoid OPERATOR(pg_catalog.=) CASE WHEN $3 THEN 'sql_saga.uk_update_check_c()'::regprocedure
                                                   ELSE 'sql_saga.uk_delete_check_c()'::regprocedure END
           -- ROW (1) + AFTER + UPDATE (16) or DELETE (8)
           AND t.tgtype OPERATOR(pg_catalog.=) CASE WHEN $3 THEN 17 ELSE 9 END
           AND t.tgconstraint OPERATOR(pg_catalog.<>) 0
           AND t.tgconstrrelid OPERATOR(pg_catalog.=) pg_catalog.to_regclass(pg_catalog.format('%I.%I', fk.table_schema, fk.table_name))
           AND t.tgconstrindid OPERATOR(pg_catalog.=) 0
           AND t.tgdeferrable AND NOT t.tginitdeferred
           AND t.tgqual IS NULL
           AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL
           AND t.tgparentid OPERATOR(pg_catalog.=) 0
           AND t.tgargs OPERATOR(pg_catalog.=) sql_saga.__internal_regular_fk_trigger_args(fk.foreign_key_name)
           -- tgattr (int2vector) is zero-based: compare the elements, not the array bounds.
           AND ARRAY(SELECT pg_catalog.unnest(t.tgattr::pg_catalog.int2[]))::pg_catalog.int2[] OPERATOR(pg_catalog.=)
               CASE WHEN $3 THEN $4 ELSE '{}'::pg_catalog.int2[] END
        FROM pg_catalog.pg_trigger AS t
        WHERE t.oid = $2
          AND t.tgrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', uk.table_schema, uk.table_name))
          AND t.tgname = CASE WHEN $3 THEN fk.uk_update_trigger ELSE fk.uk_delete_trigger END
    ), false)
    FROM sql_saga.foreign_keys AS fk
    JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
    WHERE fk.foreign_key_name = $1 AND fk.type = 'regular_to_temporal';
$__internal_regular_fk_uk_trigger_is_canonical$;

REVOKE ALL ON FUNCTION sql_saga.__internal_regular_fk_uk_trigger_is_canonical(name, oid, boolean, smallint[]) FROM PUBLIC;

/*
 * Follow renames of the temporal columns of application-time eras (range, valid_from, valid_until,
 * valid_to) on the given relations (those touched by the ALTER TABLE). A catalogued column that no
 * longer exists is identified by attnum through an object that follows renames by itself:
 *   - the synchronization trigger: its UPDATE OF list is range, [valid_to,] valid_from, valid_until;
 *   - without one, the bounds CHECK (NOT isempty(range)) names the range column and the boundary
 *     CHECK (from < until ...) names the boundary columns.
 * Before anything changes, everything that will be regenerated must be exactly what sql_saga
 * generated for the columns before the rename, and must not be used by anything else; otherwise the
 * rename is refused, so nothing a user created or changed is overwritten or deleted:
 *   - the synchronization function: definition (body and properties) as generated, and no
 *     dependent other than its own trigger;
 *   - the synchronization trigger: definition as generated (only the firing mode may differ);
 *   - the referenced-side triggers of every regular foreign key referencing a unique key of the era
 *     (when a boundary column changes): see __internal_regular_fk_uk_trigger_is_canonical.
 * Then, in place: the era row follows; the synchronization function is replaced (CREATE OR REPLACE:
 * same OID, owner and privileges, its trigger keeps its OID and firing mode); the referenced-side
 * triggers are re-created with the same names and firing modes.
 * The system_time era is not followed here.
 */
CREATE FUNCTION sql_saga.__internal_era_column_rename_follow(table_oids oid[])
RETURNS void
LANGUAGE plpgsql
SET search_path = sql_saga, pg_catalog
AS $__internal_era_column_rename_follow$
#variable_conflict use_variable
DECLARE
    era_rec record;
    fk_rec record;
    sync_trigger record;
    sync_columns name[];
    sync_function oid;
    generated record;
    canonical_definition text;
    dependent record;
    new_range name;
    new_from name;
    new_until name;
    new_to name;
    boundaries_changed boolean;
    era_display text;
    uk_update_attnums smallint[];
BEGIN
    FOR era_rec IN
        SELECT e.*, c.oid AS table_oid
        FROM pg_catalog.pg_class AS c
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name) = (n.nspname, c.relname)
        WHERE c.oid = ANY (table_oids)
          AND e.era_name <> 'system_time'
          AND EXISTS (
              SELECT FROM pg_catalog.unnest(ARRAY[e.range_column_name, e.valid_from_column_name,
                                                  e.valid_until_column_name, e.valid_to_column_name]) AS col(name)
              WHERE col.name IS NOT NULL
                AND NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a
                                WHERE a.attrelid = c.oid AND a.attname = col.name AND a.attnum > 0 AND NOT a.attisdropped))
        ORDER BY e.table_schema COLLATE "C", e.table_name COLLATE "C", e.era_name COLLATE "C"
    LOOP
        new_range := NULL;
        new_from := NULL;
        new_until := NULL;
        new_to := NULL;
        sync_function := NULL;
        era_display := sql_saga.__internal_display_name(era_rec.table_oid::regclass);

        SELECT t.oid, t.tgfoid INTO sync_trigger
        FROM pg_catalog.pg_trigger AS t
        WHERE t.tgrelid = era_rec.table_oid
          AND t.tgname = era_rec.sync_temporal_trg_name;
        IF FOUND THEN
            sync_function := pg_catalog.to_regprocedure(pg_catalog.format('sql_saga.%I()', era_rec.sync_temporal_trg_function_name));
            sync_columns := ARRAY(
                SELECT a.attname
                FROM pg_catalog.pg_trigger AS t
                CROSS JOIN LATERAL pg_catalog.unnest(t.tgattr::pg_catalog.int2[]) WITH ORDINALITY AS u(attnum, n)
                JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attnum) = (era_rec.table_oid, u.attnum)
                WHERE t.oid = sync_trigger.oid
                ORDER BY u.n);
            IF era_rec.valid_to_column_name IS NULL AND pg_catalog.cardinality(sync_columns) = 3 THEN
                new_range := sync_columns[1];
                new_from := sync_columns[2];
                new_until := sync_columns[3];
            ELSIF era_rec.valid_to_column_name IS NOT NULL AND pg_catalog.cardinality(sync_columns) = 4 THEN
                new_range := sync_columns[1];
                new_to := sync_columns[2];
                new_from := sync_columns[3];
                new_until := sync_columns[4];
            ELSE
                RAISE EXCEPTION 'sql_saga: cannot rename a column of era "%" on table "%": synchronization trigger "%" is not the canonical sql_saga trigger; restore it with sql_saga.add_synchronize_temporal_columns_trigger(table_oid regclass, era_name name) first',
                    era_rec.era_name, era_display, era_rec.sync_temporal_trg_name;
            END IF;
        ELSE
            SELECT a.attname INTO new_range
            FROM pg_catalog.pg_constraint AS c
            JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attnum) = (c.conrelid, c.conkey[1])
            WHERE (c.conrelid, c.conname) = (era_rec.table_oid, era_rec.bounds_check_constraint)
              AND c.contype = 'c' AND pg_catalog.cardinality(c.conkey) = 1
              AND pg_catalog.pg_get_constraintdef(c.oid) = pg_catalog.format('CHECK ((NOT isempty(%I)))', a.attname);
            SELECT sa.attname, ea.attname INTO new_from, new_until
            FROM pg_catalog.pg_constraint AS c
            JOIN pg_catalog.pg_attribute AS sa ON sa.attrelid = c.conrelid AND sa.attnum = ANY (c.conkey)
            JOIN pg_catalog.pg_attribute AS ea ON ea.attrelid = c.conrelid AND ea.attnum = ANY (c.conkey)
            WHERE (c.conrelid, c.conname) = (era_rec.table_oid, era_rec.boundary_check_constraint)
              AND c.contype = 'c' AND sa.attnum <> ea.attnum
              AND sql_saga.__internal_era_boundary_check_matches(pg_catalog.pg_get_constraintdef(c.oid), sa.attname, ea.attname);
        END IF;

        /* Only a catalogued column that is gone follows, and only to a column of the era's types. */
        IF new_range IS NULL OR EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = era_rec.table_oid AND a.attname = era_rec.range_column_name AND a.attnum > 0 AND NOT a.attisdropped)
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = era_rec.table_oid AND a.attname = new_range AND a.atttypid = era_rec.range_type) THEN
            new_range := era_rec.range_column_name;
        END IF;
        IF new_from IS NULL OR era_rec.valid_from_column_name IS NULL
           OR EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = era_rec.table_oid AND a.attname = era_rec.valid_from_column_name AND a.attnum > 0 AND NOT a.attisdropped)
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = era_rec.table_oid AND a.attname = new_from AND a.atttypid = era_rec.range_subtype) THEN
            new_from := era_rec.valid_from_column_name;
        END IF;
        IF new_until IS NULL OR era_rec.valid_until_column_name IS NULL
           OR EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = era_rec.table_oid AND a.attname = era_rec.valid_until_column_name AND a.attnum > 0 AND NOT a.attisdropped)
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = era_rec.table_oid AND a.attname = new_until AND a.atttypid = era_rec.range_subtype) THEN
            new_until := era_rec.valid_until_column_name;
        END IF;
        IF new_to IS NULL OR era_rec.valid_to_column_name IS NULL
           OR EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = era_rec.table_oid AND a.attname = era_rec.valid_to_column_name AND a.attnum > 0 AND NOT a.attisdropped)
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = era_rec.table_oid AND a.attname = new_to AND a.atttypid = era_rec.range_subtype) THEN
            new_to := era_rec.valid_to_column_name;
        END IF;

        IF (new_range, new_from, new_until, new_to) IS NOT DISTINCT FROM
           (era_rec.range_column_name, era_rec.valid_from_column_name, era_rec.valid_until_column_name, era_rec.valid_to_column_name) THEN
            CONTINUE;
        END IF;
        boundaries_changed := (new_from, new_until) IS DISTINCT FROM (era_rec.valid_from_column_name, era_rec.valid_until_column_name);

        /*
         * Attribute before anything changes. The synchronization objects are compared with what
         * sql_saga generates from the catalog as it was before the rename (era row not yet updated);
         * their column references are by name, so the generated text uses the old names.
         */
        IF sync_trigger.oid IS NOT NULL THEN
            generated := sql_saga.__internal_synchronize_temporal_columns_sql(era_rec.table_oid::regclass, era_rec.era_name);

            IF sync_function IS NULL OR sync_trigger.tgfoid IS DISTINCT FROM sync_function
               OR generated.function_name IS DISTINCT FROM era_rec.sync_temporal_trg_function_name THEN
                RAISE EXCEPTION 'sql_saga: cannot rename a column of era "%" on table "%": synchronization trigger "%" is not the canonical sql_saga trigger; restore it with sql_saga.add_synchronize_temporal_columns_trigger(table_oid regclass, era_name name) first',
                    era_rec.era_name, era_display, era_rec.sync_temporal_trg_name;
            END IF;

            FOR dependent IN
                SELECT t.tgname, t.tgrelid
                FROM pg_catalog.pg_trigger AS t
                WHERE t.tgfoid = sync_function AND t.oid <> sync_trigger.oid
                ORDER BY t.tgrelid, t.tgname COLLATE "C"
            LOOP
                RAISE EXCEPTION 'sql_saga: cannot rename a column of era "%" on table "%": synchronization function %() is also used by trigger "%" on table "%"; drop that trigger first',
                    era_rec.era_name, era_display, pg_catalog.format('sql_saga.%I', era_rec.sync_temporal_trg_function_name), dependent.tgname,
                    sql_saga.__internal_display_name(dependent.tgrelid::regclass);
            END LOOP;
            IF EXISTS (
                SELECT FROM pg_catalog.pg_depend AS d
                WHERE d.refclassid = 'pg_catalog.pg_proc'::regclass AND d.refobjid = sync_function
                  AND d.deptype IN ('n', 'a')
                  AND NOT (d.classid = 'pg_catalog.pg_trigger'::regclass AND d.objid = sync_trigger.oid)) THEN
                RAISE EXCEPTION 'sql_saga: cannot rename a column of era "%" on table "%": synchronization function %() has dependent objects other than its trigger; drop them first',
                    era_rec.era_name, era_display, pg_catalog.format('sql_saga.%I', era_rec.sync_temporal_trg_function_name);
            END IF;

            /*
             * The function: exactly the generated body (prosrc) and the properties CREATE FUNCTION
             * gives it there (plpgsql, RETURNS trigger, no arguments, VOLATILE, not STRICT, not
             * SECURITY DEFINER, not LEAKPROOF, PARALLEL UNSAFE, COST 100, no SET clauses).
             */
            canonical_definition := pg_catalog.substr(generated.function_sql,
                pg_catalog.strpos(generated.function_sql, '$template_sync_trigger$') + pg_catalog.length('$template_sync_trigger$'));
            canonical_definition := pg_catalog.substr(canonical_definition, 1,
                pg_catalog.strpos(canonical_definition, '$template_sync_trigger$') - 1);
            IF NOT EXISTS (
                SELECT FROM pg_catalog.pg_proc AS p
                WHERE p.oid = sync_function
                  AND p.prosrc = canonical_definition
                  AND p.prolang = (SELECT l.oid FROM pg_catalog.pg_language AS l WHERE l.lanname = 'plpgsql')
                  AND p.prorettype = 'pg_catalog.trigger'::regtype AND p.pronargs = 0 AND p.prokind = 'f'
                  AND p.provolatile = 'v' AND NOT p.proisstrict AND NOT p.prosecdef AND NOT p.proleakproof
                  AND p.proparallel = 'u' AND p.procost = 100 AND p.proconfig IS NULL AND p.probin IS NULL
                  AND p.prosqlbody IS NULL AND p.protrftypes IS NULL AND p.prosupport = 0) THEN
                RAISE EXCEPTION 'sql_saga: cannot rename a column of era "%" on table "%": synchronization function %() is not the canonical sql_saga function; restore it with sql_saga.add_synchronize_temporal_columns_trigger(table_oid regclass, era_name name) first',
                    era_rec.era_name, era_display, pg_catalog.format('sql_saga.%I', era_rec.sync_temporal_trg_function_name);
            END IF;

            /*
             * The trigger: BEFORE INSERT OR UPDATE OF range, [valid_to,] valid_from, valid_until (by
             * attnum: the columns as they are named now, after the rename), FOR EACH ROW, the
             * function, no WHEN, no arguments, not a constraint trigger. Only the firing mode may differ.
             */
            IF generated.trigger_name IS DISTINCT FROM era_rec.sync_temporal_trg_name
               OR NOT EXISTS (
                SELECT FROM pg_catalog.pg_trigger AS t
                WHERE t.oid = sync_trigger.oid
                  -- ROW (1) + BEFORE (2) + INSERT (4) + UPDATE (16)
                  AND t.tgtype = 23 AND NOT t.tgisinternal AND t.tgconstraint = 0 AND t.tgconstrrelid = 0
                  AND t.tgqual IS NULL AND t.tgnargs = 0 AND t.tgoldtable IS NULL AND t.tgnewtable IS NULL
                  AND t.tgparentid = 0
                  -- tgattr (int2vector) is zero-based: compare the elements, not the array bounds.
                  AND ARRAY(SELECT pg_catalog.unnest(t.tgattr::pg_catalog.int2[]))::pg_catalog.int2[] = ARRAY(
                      SELECT a.attnum
                      FROM pg_catalog.unnest(ARRAY[new_range, new_to, new_from, new_until])
                           WITH ORDINALITY AS u(column_name, n)
                      JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (era_rec.table_oid, u.column_name)
                      WHERE u.column_name IS NOT NULL
                      ORDER BY u.n)::pg_catalog.int2[]) THEN
                RAISE EXCEPTION 'sql_saga: cannot rename a column of era "%" on table "%": synchronization trigger "%" is not the canonical sql_saga trigger; restore it with sql_saga.add_synchronize_temporal_columns_trigger(table_oid regclass, era_name name) first',
                    era_rec.era_name, era_display, era_rec.sync_temporal_trg_name;
            END IF;
        END IF;

        IF boundaries_changed THEN
            FOR fk_rec IN
                SELECT fk.foreign_key_name, fk.uk_update_trigger, fk.uk_delete_trigger, uk.column_names AS uk_column_names
                FROM sql_saga.foreign_keys AS fk
                JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
                WHERE fk.type = 'regular_to_temporal'
                  AND (uk.table_schema, uk.table_name, uk.era_name) = (era_rec.table_schema, era_rec.table_name, era_rec.era_name)
                ORDER BY fk.foreign_key_name COLLATE "C"
            LOOP
                /* UPDATE OF key columns and bounds, by attnum: the bounds are named as they are now. */
                uk_update_attnums := ARRAY(
                    SELECT a.attnum
                    FROM pg_catalog.unnest(fk_rec.uk_column_names || new_from || new_until) WITH ORDINALITY AS u(column_name, n)
                    JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (era_rec.table_oid, u.column_name)
                    ORDER BY u.n);
                IF NOT coalesce(sql_saga.__internal_regular_fk_uk_trigger_is_canonical(fk_rec.foreign_key_name,
                           (SELECT t.oid FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = era_rec.table_oid AND t.tgname = fk_rec.uk_update_trigger), true, uk_update_attnums), false)
                   OR NOT coalesce(sql_saga.__internal_regular_fk_uk_trigger_is_canonical(fk_rec.foreign_key_name,
                           (SELECT t.oid FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = era_rec.table_oid AND t.tgname = fk_rec.uk_delete_trigger), false, NULL), false) THEN
                    RAISE EXCEPTION 'sql_saga: cannot rename a column of era "%" on table "%": a referenced-side trigger of regular foreign key "%" is not the canonical sql_saga trigger; repair with drop_foreign_key and add_regular_foreign_key first',
                        era_rec.era_name, era_display, fk_rec.foreign_key_name;
                END IF;
            END LOOP;
        END IF;

        UPDATE sql_saga.era AS e
        SET range_column_name = new_range,
            valid_from_column_name = new_from,
            valid_until_column_name = new_until,
            valid_to_column_name = new_to
        WHERE (e.table_schema, e.table_name, e.era_name) = (era_rec.table_schema, era_rec.table_name, era_rec.era_name);

        /*
         * The synchronization function names the columns: replace it in place (same OID, owner and
         * privileges). Its trigger references the columns by attnum and keeps its OID and firing mode.
         */
        IF sync_trigger.oid IS NOT NULL THEN
            generated := sql_saga.__internal_synchronize_temporal_columns_sql(era_rec.table_oid::regclass, era_rec.era_name);
            EXECUTE generated.function_sql;
        END IF;

        /* The referenced-side triggers of regular foreign keys carry the bounds in their arguments. */
        IF boundaries_changed THEN
            FOR fk_rec IN
                SELECT fk.foreign_key_name
                FROM sql_saga.foreign_keys AS fk
                JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
                WHERE fk.type = 'regular_to_temporal'
                  AND (uk.table_schema, uk.table_name, uk.era_name) = (era_rec.table_schema, era_rec.table_name, era_rec.era_name)
                ORDER BY fk.foreign_key_name COLLATE "C"
            LOOP
                PERFORM sql_saga.__internal_recreate_regular_fk_uk_triggers(fk_rec.foreign_key_name);
            END LOOP;
        END IF;
    END LOOP;
END;
$__internal_era_column_rename_follow$;

REVOKE ALL ON FUNCTION sql_saga.__internal_era_column_rename_follow(oid[]) FROM PUBLIC;

CREATE FUNCTION sql_saga.rename_following()
 RETURNS event_trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 -- Set a safe search path for this SECURITY DEFINER function
 SET search_path = sql_saga, pg_catalog, public
AS
$function$
#variable_conflict use_variable
DECLARE
    r record;
    sql text;
    v_is_alter_trigger boolean := false;
    is_alter_table boolean := false;
    cmd record;
    v_command_changes_shape boolean;
    v_shape_changed_table_oids oid[] := ARRAY[]::oid[];
    v_table_oid oid;
    v_current_columns name[];
    v_removed_columns name[];
    v_added_columns name[];
    v_missing_excluded name;
    v_unique_key_columns_before jsonb;
    v_altered_table_oids oid[];
    v_touched_relation_oids oid[];
BEGIN
    /* DROP statements cannot rename or reshape surviving managed objects. */
    IF tg_tag LIKE 'DROP %' THEN
        RETURN;
    END IF;

    FOR cmd IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        IF cmd.command_tag = 'ALTER TABLE' THEN
            is_alter_table := true;
            IF cmd.classid = 'pg_catalog.pg_class'::regclass THEN
                v_command_changes_shape := cmd.objsubid > 0
                    OR sql_saga.__internal_alter_table_changes_column_shape(cmd.command);

                IF v_command_changes_shape THEN
                    IF EXISTS (
                        SELECT 1
                        FROM sql_saga.system_time_era AS ste
                        JOIN pg_catalog.pg_class AS c
                          ON c.oid = cmd.objid
                        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                        WHERE (ste.table_schema, ste.table_name) = (n.nspname, c.relname))
                       AND NOT cmd.objid = ANY (v_shape_changed_table_oids) THEN
                        v_shape_changed_table_oids := pg_catalog.array_append(v_shape_changed_table_oids, cmd.objid);
                    END IF;

                    -- PostgreSQL propagates column-shape changes through table inheritance. Avoid all
                    -- pg_inherits work for the overwhelmingly common no-children case.
                    IF (SELECT c.relhassubclass FROM pg_catalog.pg_class AS c WHERE c.oid = cmd.objid) THEN
                        FOR v_table_oid IN
                            WITH RECURSIVE descendants(table_oid) AS (
                                SELECT i.inhrelid
                                FROM pg_catalog.pg_inherits AS i
                                WHERE i.inhparent = cmd.objid
                                UNION
                                SELECT i.inhrelid
                                FROM descendants AS d
                                JOIN pg_catalog.pg_inherits AS i ON i.inhparent = d.table_oid
                            )
                            SELECT c.oid
                            FROM descendants AS d
                            JOIN pg_catalog.pg_class AS c ON c.oid = d.table_oid
                            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                            JOIN sql_saga.system_time_era AS ste
                              ON (ste.table_schema, ste.table_name) = (n.nspname, c.relname)
                            JOIN LATERAL (
                                SELECT pg_catalog.array_agg(a.attname ORDER BY a.attname COLLATE "C") AS column_names
                                FROM pg_catalog.pg_attribute AS a
                                WHERE a.attrelid = c.oid
                                  AND a.attnum > 0
                                  AND NOT a.attisdropped
                            ) AS live ON true
                            -- Set comparison: only added/removed columns are shape changes.
                            WHERE (SELECT pg_catalog.array_agg(s.attname ORDER BY s.attname COLLATE "C")
                                   FROM pg_catalog.unnest(ste.table_columns_snapshot) AS s(attname))
                                  IS DISTINCT FROM live.column_names
                        LOOP
                            IF NOT v_table_oid = ANY (v_shape_changed_table_oids) THEN
                                v_shape_changed_table_oids := pg_catalog.array_append(
                                    v_shape_changed_table_oids, v_table_oid);
                            END IF;
                        END LOOP;
                    END IF;
                END IF;
            END IF;
        END IF;
    END LOOP;

    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        IF r.command_tag = 'ALTER TRIGGER' THEN
            v_is_alter_trigger := true;
            exit;
        END IF;
    END LOOP;

    -- Exit early if the DDL command does not affect a managed object.
    -- We bypass this for ALTER TRIGGER because the object identity contains
    -- the *new* trigger name, which won't be in our metadata yet.
    IF NOT v_is_alter_trigger
       AND cardinality(v_shape_changed_table_oids) = 0
       AND NOT sql_saga.__internal_ddl_command_affects_managed_object() THEN
        RETURN;
    END IF;

    /*
     * The relations this command touched: tables, views and indexes it names directly, the tables of
     * the constraints, triggers, policies and indexes it names, and their inheritance descendants
     * (PostgreSQL propagates renames to them). Built once from the catalog only.
     *
     * The constraint renames below are found by comparing constraint definitions
     * (pg_get_constraintdef), and deparsing a CHECK constraint takes a lock on its table. They only
     * deparse constraints of these relations: a rename can only happen on a relation the command
     * touched, and a concurrent session (such as another pg_restore -j worker) may hold an exclusive
     * lock on any other table while it waits for one of ours, which would be a deadlock.
     */
    v_touched_relation_oids := ARRAY(
        WITH RECURSIVE touched(relation_oid) AS (
            SELECT CASE ev.classid
                       WHEN 'pg_catalog.pg_class'::regclass THEN
                           coalesce((SELECT i.indrelid FROM pg_catalog.pg_index AS i WHERE i.indexrelid = ev.objid), ev.objid)
                       WHEN 'pg_catalog.pg_constraint'::regclass THEN
                           (SELECT c.conrelid FROM pg_catalog.pg_constraint AS c WHERE c.oid = ev.objid)
                       WHEN 'pg_catalog.pg_trigger'::regclass THEN
                           (SELECT t.tgrelid FROM pg_catalog.pg_trigger AS t WHERE t.oid = ev.objid)
                       WHEN 'pg_catalog.pg_policy'::regclass THEN
                           (SELECT p.polrelid FROM pg_catalog.pg_policy AS p WHERE p.oid = ev.objid)
                   END
            FROM pg_catalog.pg_event_trigger_ddl_commands() AS ev
            UNION
            SELECT i.inhrelid
            FROM touched AS t
            JOIN pg_catalog.pg_inherits AS i ON i.inhparent = t.relation_oid
        )
        SELECT t.relation_oid FROM touched AS t WHERE t.relation_oid IS NOT NULL);

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
     * Follow renames of the era's temporal columns (range, valid_from, valid_until, valid_to) on the
     * relations touched by this ALTER TABLE, and regenerate what names them (the synchronization
     * function, the referenced-side triggers of regular foreign keys). Runs before the unique keys
     * are followed, which exclude the era's range column by name.
     */
    IF is_alter_table THEN
        PERFORM sql_saga.__internal_era_column_rename_follow(ARRAY(
            SELECT DISTINCT c.objid
            FROM pg_catalog.pg_event_trigger_ddl_commands() AS c
            WHERE c.command_tag = 'ALTER TABLE' AND c.classid = 'pg_catalog.pg_class'::regclass));
    END IF;

    -- Follow renames of the bounds check constraint (NOT isempty(range)).
    FOR sql IN
        SELECT format('UPDATE sql_saga.era SET bounds_check_constraint = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            c.conname,
            e.table_schema,
            e.table_name,
            e.era_name
        )
        FROM sql_saga.era AS e
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (e.table_name, (SELECT oid FROM pg_namespace WHERE nspname = e.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        WHERE e.bounds_check_constraint IS NOT NULL
          AND e.bounds_check_constraint <> c.conname
          AND pc.oid = ANY (v_touched_relation_oids)
          AND sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = format('CHECK ((NOT isempty(%I)))', e.range_column_name)
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, e.bounds_check_constraint))
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames of the boundary check constraint (from < until).
    FOR sql IN
        SELECT format('UPDATE sql_saga.era SET boundary_check_constraint = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            c.conname,
            e.table_schema,
            e.table_name,
            e.era_name
        )
        FROM sql_saga.era AS e
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (e.table_name, (SELECT oid FROM pg_namespace WHERE nspname = e.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        WHERE e.boundary_check_constraint IS NOT NULL
          AND e.boundary_check_constraint <> c.conname
          AND e.valid_from_column_name IS NOT NULL
          AND e.valid_until_column_name IS NOT NULL
          AND pc.oid = ANY (v_touched_relation_oids)
          AND sql_saga.__internal_era_boundary_check_matches(sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids), e.valid_from_column_name, e.valid_until_column_name)
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, e.boundary_check_constraint))
    LOOP
        EXECUTE sql;
    END LOOP;

    ---
    --- system_time_era
    ---

    -- Follow renames of the infinity_check_constraint.
    -- The constraint format is: CHECK ((upper(range_column_name) = 'infinity'::timestamp with time zone))
    FOR sql IN
        SELECT format('UPDATE sql_saga.system_time_era SET infinity_check_constraint = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            c.conname,
            ste.table_schema,
            ste.table_name,
            ste.era_name
        )
        FROM sql_saga.system_time_era AS ste
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (ste.table_schema, ste.table_name, ste.era_name)
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (ste.table_name, (SELECT oid FROM pg_namespace WHERE nspname = ste.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        WHERE ste.infinity_check_constraint <> c.conname
          AND pc.oid = ANY (v_touched_relation_oids)
          AND sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = format('CHECK ((upper(%I) = ''infinity''::timestamp with time zone))', e.range_column_name)
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, ste.infinity_check_constraint))
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames of the generated_always_trigger.
    FOR sql IN
        SELECT format('UPDATE sql_saga.system_time_era SET generated_always_trigger = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            t.tgname,
            ste.table_schema,
            ste.table_name,
            ste.era_name
        )
        FROM sql_saga.system_time_era AS ste
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (ste.table_name, (SELECT oid FROM pg_namespace WHERE nspname = ste.table_schema))
        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = pc.oid
        WHERE t.tgname <> ste.generated_always_trigger
          AND t.tgfoid = 'sql_saga.generated_always_as_row_start_end()'::regprocedure
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (pc.oid, ste.generated_always_trigger))
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames of the write_history_trigger.
    FOR sql IN
        SELECT format('UPDATE sql_saga.system_time_era SET write_history_trigger = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            t.tgname,
            ste.table_schema,
            ste.table_name,
            ste.era_name
        )
        FROM sql_saga.system_time_era AS ste
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (ste.table_name, (SELECT oid FROM pg_namespace WHERE nspname = ste.table_schema))
        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = pc.oid
        WHERE t.tgname <> ste.write_history_trigger
          AND t.tgfoid = 'sql_saga.write_history()'::regprocedure
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (pc.oid, ste.write_history_trigger))
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames of the truncate_trigger.
    FOR sql IN
        SELECT format('UPDATE sql_saga.system_time_era SET truncate_trigger = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            t.tgname,
            ste.table_schema,
            ste.table_name,
            ste.era_name
        )
        FROM sql_saga.system_time_era AS ste
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (ste.table_name, (SELECT oid FROM pg_namespace WHERE nspname = ste.table_schema))
        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = pc.oid
        WHERE t.tgname <> ste.truncate_trigger
          AND t.tgfoid = 'sql_saga.truncate_system_versioning()'::regprocedure
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (pc.oid, ste.truncate_trigger))
    LOOP
        EXECUTE sql;
    END LOOP;

    /*
     * Attribute base-column changes only to registered base relations present in this ALTER TABLE
     * invocation. The persisted snapshot supplies the pre-command names without storing OIDs.
     */
    FOREACH v_table_oid IN ARRAY v_shape_changed_table_oids
    LOOP
        SELECT pg_catalog.array_agg(a.attname ORDER BY a.attnum)
        INTO v_current_columns
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = v_table_oid
          AND a.attnum > 0
          AND NOT a.attisdropped;

        SELECT pg_catalog.array_agg(s.column_name ORDER BY s.ordinality)
        INTO v_removed_columns
        FROM sql_saga.system_time_era AS ste
        CROSS JOIN LATERAL pg_catalog.unnest(ste.table_columns_snapshot) WITH ORDINALITY AS s(column_name, ordinality)
        WHERE pg_catalog.to_regclass(pg_catalog.format('%I.%I', ste.table_schema, ste.table_name)) = v_table_oid
          AND NOT s.column_name = ANY (v_current_columns);

        SELECT pg_catalog.array_agg(c.column_name ORDER BY c.ordinality)
        INTO v_added_columns
        FROM pg_catalog.unnest(v_current_columns) WITH ORDINALITY AS c(column_name, ordinality)
        JOIN sql_saga.system_time_era AS ste
          ON pg_catalog.to_regclass(pg_catalog.format('%I.%I', ste.table_schema, ste.table_name)) = v_table_oid
        WHERE NOT c.column_name = ANY (ste.table_columns_snapshot);

        SELECT removed.column_name
        INTO v_missing_excluded
        FROM pg_catalog.unnest(coalesce(v_removed_columns, ARRAY[]::name[])) AS removed(column_name)
        JOIN sql_saga.system_time_era AS ste
          ON pg_catalog.to_regclass(pg_catalog.format('%I.%I', ste.table_schema, ste.table_name)) = v_table_oid
        WHERE removed.column_name = ANY (ste.excluded_column_names)
        ORDER BY removed.column_name
        LIMIT 1;

        IF v_missing_excluded IS NOT NULL THEN
            IF cardinality(v_removed_columns) = 1 AND cardinality(v_added_columns) = 1 THEN
                RAISE EXCEPTION 'cannot drop or rename column "%" on table "%" because it is excluded from system_time era',
                    v_missing_excluded, sql_saga.__internal_display_name(v_table_oid);
            END IF;
            RAISE EXCEPTION 'cannot determine whether excluded column "%" on table "%" was renamed because ALTER TABLE changed multiple columns; split column-shape changes into separate ALTER TABLE statements',
                v_missing_excluded, sql_saga.__internal_display_name(v_table_oid);
        END IF;
    END LOOP;

    ---
    --- updatable_view
    ---
    DECLARE
        v_view_row sql_saga.updatable_view;
        v_view_oid regclass;
        v_new_trigger_name name;
    BEGIN
        FOR v_view_row IN SELECT * FROM sql_saga.updatable_view
        LOOP
            v_view_oid := to_regclass(format('%I.%I', v_view_row.view_schema /* %I */, v_view_row.view_name /* %I */));

            -- Check if the registered trigger still exists on the view
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = v_view_oid AND tgname = v_view_row.trigger_name) THEN
                -- If it's stale, find the *new* trigger on that view that points to our function.
                -- This currently only supports for_portion_of, but is extensible.
                IF v_view_row.view_type = 'for_portion_of' THEN
                    SELECT tgname INTO v_new_trigger_name FROM pg_trigger
                    WHERE tgrelid = v_view_oid AND tgfoid = 'sql_saga.for_portion_of_trigger()'::regprocedure;

                    IF FOUND THEN
                        sql := format('UPDATE sql_saga.updatable_view SET trigger_name = %L WHERE (view_schema, view_name) = (%L, %L)',
                            v_new_trigger_name, /* %L */
                            v_view_row.view_schema, /* %L */
                            v_view_row.view_name /* %L */
                        );
                        EXECUTE sql;
                    END IF;
                END IF;
            END IF;
        END LOOP;
    END;

    -- A renamed column of a unique key referenced by a regular foreign key: attribute the dependent
    -- helper functions and referenced-side triggers before anything changes, and remember the key
    -- columns, so that they can be regenerated below. Only relations touched by this ALTER TABLE
    -- are considered; any other ALTER TABLE skips this work.
    IF is_alter_table THEN
        v_altered_table_oids := ARRAY(
            SELECT DISTINCT c.objid
            FROM pg_catalog.pg_event_trigger_ddl_commands() AS c
            WHERE c.command_tag = 'ALTER TABLE' AND c.classid = 'pg_catalog.pg_class'::regclass);
        IF EXISTS (
            SELECT FROM pg_catalog.pg_class AS c
            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
            JOIN sql_saga.unique_keys AS uk ON (uk.table_schema, uk.table_name) = (n.nspname, c.relname)
            JOIN sql_saga.foreign_keys AS fk ON fk.unique_key_name = uk.unique_key_name AND fk.type = 'regular_to_temporal'
            WHERE c.oid = ANY (v_altered_table_oids)) THEN
            v_unique_key_columns_before := sql_saga.__internal_regular_fk_key_rename_prepare(v_altered_table_oids);
        END IF;
    END IF;

    ---
    --- unique_keys
    ---

    FOR sql IN
        -- Follow renames for columns in non-XOR unique keys
        SELECT format('UPDATE sql_saga.unique_keys SET column_names = %L WHERE unique_key_name = %L',
            a.column_names, /* %L */
            uk.unique_key_name /* %L */
        )
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT oid FROM pg_namespace WHERE nspname = uk.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON (c.conrelid, c.conname) = (pc.oid, uk.unique_constraint)
        JOIN LATERAL (
            SELECT array_agg(a.attname ORDER BY u.ordinality) AS column_names
            FROM unnest(c.conkey) WITH ORDINALITY AS u (attnum, ordinality)
            JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attnum) = (pc.oid, u.attnum)
            WHERE a.attname NOT IN (e.range_column_name) -- The native constraint will include the range column
            ) AS a ON true
        WHERE uk.mutually_exclusive_columns IS NULL
          AND uk.unique_constraint IS NOT NULL
          AND uk.column_names <> a.column_names
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames for columns in XOR unique keys by inspecting the unified constraint
    FOR sql IN
        WITH new_xor_cols AS (
            SELECT
                uk.unique_key_name,
                array_agg(DISTINCT a.attname ORDER BY a.attname) AS new_column_names
            FROM sql_saga.unique_keys uk
            JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
            JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT oid FROM pg_namespace WHERE nspname = uk.table_schema))
            JOIN pg_catalog.pg_constraint AS c ON (c.conrelid, c.conname) = (pc.oid, uk.unique_constraint)
            CROSS JOIN unnest(c.conkey) WITH ORDINALITY AS u(attnum, ord)
            JOIN pg_attribute a ON (a.attrelid, a.attnum) = (pc.oid, u.attnum)
            WHERE uk.mutually_exclusive_columns IS NOT NULL
              AND uk.unique_constraint IS NOT NULL
              AND a.attname NOT IN (e.range_column_name) -- Exclude temporal column
            GROUP BY uk.unique_key_name
        )
        SELECT format('UPDATE sql_saga.unique_keys SET column_names = %L WHERE unique_key_name = %L',
            nxc.new_column_names,
            uk.unique_key_name
        )
        FROM sql_saga.unique_keys uk
        JOIN new_xor_cols nxc ON uk.unique_key_name = nxc.unique_key_name
        WHERE uk.column_names <> nxc.new_column_names
    LOOP
        EXECUTE sql;
    END LOOP;

    FOR sql IN
        -- Follow renames for unique_constraint (for native temporal keys)
        SELECT format('UPDATE sql_saga.unique_keys SET unique_constraint = %L WHERE unique_key_name = %L',
            c.conname, /* %L */
            uk.unique_key_name /* %L */
        )
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT oid FROM pg_namespace WHERE nspname = uk.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        WHERE uk.unique_constraint IS NOT NULL AND uk.unique_constraint <> c.conname
          AND NOT EXISTS (SELECT FROM pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, uk.unique_constraint))
          AND pc.oid = ANY (v_touched_relation_oids)
          AND (
            -- Match traditional constraint format
            sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = (
                SELECT format('%s (%s, %I WITHOUT OVERLAPS)',
                    CASE WHEN uk.key_type = 'primary' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END,
                    string_agg(quote_ident(u.name), ', ' ORDER BY u.ordinality),
                    e.range_column_name
                )
                FROM unnest(uk.column_names) WITH ORDINALITY AS u(name, ordinality)
            )
            OR
            -- Match NULLS NOT DISTINCT constraint format
            sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = (
                SELECT format('%s (%s, %I WITHOUT OVERLAPS)',
                    CASE WHEN uk.key_type = 'primary' THEN 'PRIMARY KEY NULLS NOT DISTINCT' ELSE 'UNIQUE NULLS NOT DISTINCT' END,
                    string_agg(quote_ident(u.name), ', ' ORDER BY u.ordinality),
                    e.range_column_name
                )
                FROM unnest(uk.column_names) WITH ORDINALITY AS u(name, ordinality)
            )
          )
    LOOP
        EXECUTE sql;
    END LOOP;

    FOR sql IN
        -- Follow renames for exclude_constraint (for predicated keys)
        SELECT format('UPDATE sql_saga.unique_keys SET exclude_constraint = %L WHERE unique_key_name = %L',
            c.conname, /* %L */
            uk.unique_key_name /* %L */
        )
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT oid FROM pg_namespace WHERE nspname = uk.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        WHERE uk.exclude_constraint IS NOT NULL AND uk.exclude_constraint <> c.conname
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, uk.exclude_constraint))
          AND pc.oid = ANY (v_touched_relation_oids)
          AND sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = (
            SELECT format('EXCLUDE USING gist (%s, %I WITH &&) WHERE (%s) DEFERRABLE',
                string_agg(quote_ident(u.name) || ' WITH =', ', ' ORDER BY u.ordinality),
                e.range_column_name,
                uk.predicate
            )
            FROM unnest(uk.column_names) WITH ORDINALITY AS u(name, ordinality)
          )
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames for check_constraint (for XOR keys)
    FOR sql IN
        SELECT format('UPDATE sql_saga.unique_keys SET check_constraint = %L WHERE unique_key_name = %L',
            c.conname, uk.unique_key_name
        )
        FROM sql_saga.unique_keys uk
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT oid FROM pg_namespace WHERE nspname = uk.table_schema))
        JOIN pg_constraint c ON c.conrelid = pc.oid
        WHERE uk.check_constraint IS NOT NULL
        AND uk.check_constraint <> c.conname
        AND c.contype = 'c'
        AND NOT EXISTS (SELECT FROM pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, uk.check_constraint))
        AND pc.oid = ANY (v_touched_relation_oids)
        AND sql_saga.__internal_constraintdef_if_touched(c.oid, c.conrelid, v_touched_relation_oids) = format('CHECK (((%s) = 1))', format('num_nonnulls(%s)', (SELECT string_agg(format('%I', col), ', ') FROM unnest(uk.mutually_exclusive_columns) as col)))
    LOOP
        EXECUTE sql;
    END LOOP;

    -- A renamed referenced key column: regenerate the dependent regular foreign key objects.
    IF v_unique_key_columns_before IS DISTINCT FROM '{}'::jsonb AND v_unique_key_columns_before IS NOT NULL THEN
        PERFORM sql_saga.__internal_regular_fk_key_rename_apply(v_unique_key_columns_before);
    END IF;

    ---
    --- foreign_keys
    ---

    DECLARE
        fk_table RECORD;
        current_columns name[];
        removed_columns name[];
        added_columns name[];
        old_col_name name;
        new_col_name name;
        fk_table_oid oid;
    BEGIN
        FOR fk_table IN
            SELECT DISTINCT table_schema, table_name
            FROM sql_saga.foreign_keys
        LOOP
            fk_table_oid := to_regclass(format('%I.%I', fk_table.table_schema /* %I */, fk_table.table_name /* %I */));

            -- If table was dropped, skip it. drop_protection will handle FK violations.
            IF fk_table_oid IS NULL THEN
                CONTINUE;
            END IF;

            -- Get current columns for this table
            SELECT array_agg(a.attname ORDER BY a.attnum) INTO current_columns
            FROM pg_catalog.pg_attribute AS a
            WHERE a.attrelid = fk_table_oid
              AND a.attnum > 0 AND NOT a.attisdropped;

            -- Find if a rename occurred by comparing current columns with the last known snapshot for this table
            DECLARE
                snapshot name[];
            BEGIN
                -- Get a snapshot from any of the FKs on this table (they are all identical for a given table)
                SELECT fk_table_columns_snapshot INTO snapshot FROM sql_saga.foreign_keys
                WHERE (table_schema, table_name) = (fk_table.table_schema, fk_table.table_name)
                LIMIT 1;

                -- If columns are unchanged, we are done with this table
                -- Set comparison: a restore can recreate the table with another column order.
                IF (SELECT array_agg(u.c ORDER BY u.c COLLATE "C") FROM unnest(snapshot) AS u(c))
                   IS NOT DISTINCT FROM
                   (SELECT array_agg(u.c ORDER BY u.c COLLATE "C") FROM unnest(current_columns) AS u(c)) THEN
                    CONTINUE;
                END IF;

                -- Find columns that were in the snapshot but are not in the current table state
                SELECT array_agg(c) INTO removed_columns
                FROM unnest(snapshot) AS u(c)
                WHERE NOT (u.c = ANY(current_columns));

                -- Find columns that are in the current table state but were not in the snapshot
                SELECT array_agg(c) INTO added_columns
                FROM unnest(current_columns) AS u(c)
                WHERE NOT (u.c = ANY(snapshot));

                -- If exactly one column was removed and one was added, we have a rename.
                IF array_length(removed_columns, 1) = 1 AND array_length(added_columns, 1) = 1 THEN
                    old_col_name := removed_columns[1];
                    new_col_name := added_columns[1];

                    -- We have a rename. PostgreSQL renames neither constraints, indexes nor triggers when a
                    -- column is renamed, so every stored object name stays unchanged; only column_names and
                    -- the snapshot follow. A regular FK's referenced-side triggers carry the referencing
                    -- column list in their arguments, so they are re-created with the same names.
                    DECLARE
                        fk_rec RECORD;
                    BEGIN
                        -- Snapshot first: the trigger DDL below re-enters this event trigger, which must
                        -- then see no pending rename.
                        UPDATE sql_saga.foreign_keys
                        SET fk_table_columns_snapshot = current_columns
                        WHERE (table_schema, table_name) = (fk_table.table_schema, fk_table.table_name);

                        FOR fk_rec IN
                            SELECT * FROM sql_saga.foreign_keys
                            WHERE (table_schema, table_name) = (fk_table.table_schema, fk_table.table_name)
                              AND old_col_name = ANY(column_names)
                            ORDER BY foreign_key_name
                        LOOP
                            UPDATE sql_saga.foreign_keys
                            SET column_names = array_replace(fk_rec.column_names, old_col_name, new_col_name)
                            WHERE foreign_key_name = fk_rec.foreign_key_name;

                            IF fk_rec.type = 'regular_to_temporal' THEN
                                PERFORM sql_saga.__internal_recreate_regular_fk_uk_triggers(fk_rec.foreign_key_name);
                            END IF;
                        END LOOP;
                    END;
                END IF;

                -- Regardless of the operation (RENAME, ADD, DROP), update the snapshot
                -- to reflect the new state of the table for all FKs on that table.
                -- For DROP, drop_protection will have already fired if the column was part of an FK.
                UPDATE sql_saga.foreign_keys
                SET fk_table_columns_snapshot = current_columns
                WHERE (table_schema, table_name) = (fk_table.table_schema, fk_table.table_name);
            END;
        END LOOP;
    END;

    /*
     * Protection logic: Prevent manual renaming or dropping of managed triggers.
     * This logic is disabled for ALTER TABLE commands because any trigger changes
     * are side-effects of a column rename, which is handled above. This avoids
     * false positives caused by MVCC visibility issues within the event trigger.
     */
    IF NOT is_alter_table THEN
        -- Only on explicit ALTER TRIGGER: DROP TRIGGER is refused by drop_protection, and on any other
        -- command (for instance the CREATE POLICY of a history-policy sync while pg_restore has loaded
        -- the catalog rows but not yet created the post-data triggers) a missing trigger is not a rename.
        IF v_is_alter_trigger THEN
            FOR r IN
                SELECT fk.foreign_key_name, to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) AS table_oid, fk.uk_update_trigger AS trigger_name
                FROM sql_saga.foreign_keys AS fk
                JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
                WHERE fk.uk_update_trigger IS NOT NULL
                  AND NOT EXISTS (
                    SELECT FROM pg_catalog.pg_trigger AS t
                    WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', uk.table_schema, uk.table_name)), fk.uk_update_trigger))
                UNION ALL
                SELECT fk.foreign_key_name, to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) AS table_oid, fk.uk_delete_trigger AS trigger_name
                FROM sql_saga.foreign_keys AS fk
                JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
                WHERE fk.uk_delete_trigger IS NOT NULL
                  AND NOT EXISTS (
                    SELECT FROM pg_catalog.pg_trigger AS t
                    WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', uk.table_schema, uk.table_name)), fk.uk_delete_trigger))
            LOOP
                RAISE EXCEPTION 'cannot drop or rename trigger "%" on table "%" because it is used in an era foreign key "%"',
                    r.trigger_name, sql_saga.__internal_display_name(r.table_oid), r.foreign_key_name;
            END LOOP;
        END IF;

        -- Protect synchronize_temporal_columns_trigger from being renamed or dropped.
        -- This trigger is essential for maintaining consistency between range and boundary columns.
        -- Only check during explicit ALTER TRIGGER commands to avoid MVCC visibility issues
        -- during other operations like ALTER TABLE OWNER or GRANT/REVOKE.
        IF v_is_alter_trigger THEN
            FOR r IN
                SELECT e.era_name, to_regclass(format('%I.%I', e.table_schema, e.table_name)) AS table_oid,
                       e.sync_temporal_trg_name, e.sync_temporal_trg_function_name
                FROM sql_saga.era AS e
                WHERE e.sync_temporal_trg_name IS NOT NULL  -- Only eras with synchronization triggers
                  AND e.sync_temporal_trg_function_name IS NOT NULL
                  AND NOT EXISTS (
                      SELECT FROM pg_catalog.pg_trigger AS t
                      WHERE t.tgrelid = to_regclass(format('%I.%I', e.table_schema, e.table_name))
                        AND t.tgname = e.sync_temporal_trg_name
                        AND t.tgfoid = to_regprocedure(format('sql_saga.%I()', e.sync_temporal_trg_function_name)))
            LOOP
                RAISE EXCEPTION 'cannot drop or rename trigger "%" on table "%" because it is managed by era "%"',
                    r.sync_temporal_trg_name, sql_saga.__internal_display_name(r.table_oid), r.era_name;
            END LOOP;
        END IF;
    END IF;

    IF cardinality(v_shape_changed_table_oids) > 0 THEN
        UPDATE sql_saga.system_time_era AS ste
        SET table_columns_snapshot = live.column_names
        FROM (
            SELECT c.oid AS table_oid,
                   pg_catalog.array_agg(a.attname ORDER BY a.attnum) AS column_names
            FROM pg_catalog.pg_class AS c
            JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.oid
            WHERE c.oid = ANY (v_shape_changed_table_oids)
              AND a.attnum > 0
              AND NOT a.attisdropped
            GROUP BY c.oid
        ) AS live
        WHERE pg_catalog.to_regclass(pg_catalog.format('%I.%I', ste.table_schema, ste.table_name)) = live.table_oid;
    END IF;

END;
$function$;

COMMENT ON FUNCTION sql_saga.rename_following() IS
'An event trigger function that follows object renames and updates sql_saga''s metadata accordingly.';
