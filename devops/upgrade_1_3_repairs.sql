/*
 * sql_saga 1.3 update: repairs after the 1.3 definitions are loaded (repairs fragment of
 * devops/generate_update_scripts.py). The generator emits the final integrity gate right after this fragment.
 *
 * 1. Regular foreign keys: the referencing-side CHECK constraint calling a helper function (1.0 to 1.2) becomes
 *    the 1.3 constraint trigger of the same name, after every existing child row has been checked.
 * 2. Managed updatable views that lag their base table's columns (an ADD COLUMN 1.1 and 1.2 did not follow) are
 *    brought up to date, keeping their OID, grants and the user objects that depend on them.
 * 3. sql_saga_rename_following gets back exactly the state the preamble recorded.
 * Every statement runs inside the update's transaction: any refusal rolls the whole update back.
 */

/*
 * 1. Regular foreign keys whose referencing side is still the CHECK + helper form (fk_helper_function set).
 *
 * The CHECK and the helper must be exactly what sql_saga created (attributed here, before anything changes):
 * a validated CHECK named fk_check_constraint calling only the catalogued helper with the child columns in
 * foreign_keys.column_names order, and a canonical helper for the referenced key. A constraint trigger never
 * looks at existing rows, and a CHECK accepted rows only while they were written, so every child row is
 * checked with the add_regular_foreign_key semantics (MATCH SIMPLE: rows with every key column set; MATCH FULL:
 * rows with any key column set; default btree equality of each column type). Orphans refuse the update, with
 * their count and the scan that lists them. Then, metadata first (drop_protection protects the catalogued
 * name), the CHECK is dropped and the canonical 1.3 trigger is created under the same name by
 * __internal_create_regular_fk_child_trigger. The referenced-side triggers keep their 1.2 definition, which is
 * the 1.3 one (same arguments). A helper no foreign key uses any more is dropped, without CASCADE and only when
 * nothing else depends on it.
 */
DO $convert_regular_fk_checks$
DECLARE
    fk record;
    con pg_catalog.pg_constraint;
    helper oid;
    child_oid oid;
    parent_oid oid;
    stable_columns name[];
    child_attnums smallint[];
    check_attnums smallint[];
    not_null_clause text;
    scan_where_clause text;
    count_where_clause text;
    scan text;
    orphans bigint;
    helpers text[] := ARRAY[]::text[];
    h text;
    dependent text;
BEGIN
    FOR fk IN
        SELECT f.*, uk.table_schema AS uk_schema, uk.table_name AS uk_table, uk.column_names AS uk_columns,
               e.range_column_name AS uk_range_column
        FROM sql_saga.foreign_keys AS f
        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = f.unique_key_name
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
        WHERE f.type = 'regular_to_temporal' AND f.fk_helper_function IS NOT NULL
        ORDER BY f.table_schema COLLATE "C", f.table_name COLLATE "C", f.foreign_key_name COLLATE "C"
    LOOP
        child_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', fk.table_schema, fk.table_name));
        parent_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', fk.uk_schema, fk.uk_table));
        helper := pg_catalog.to_regprocedure(fk.fk_helper_function);
        stable_columns := ARRAY(SELECT c.name FROM pg_catalog.unnest(fk.uk_columns) WITH ORDINALITY AS c (name, n)
                                WHERE c.name OPERATOR(pg_catalog.<>) fk.uk_range_column ORDER BY c.n);
        child_attnums := ARRAY(SELECT a.attnum FROM pg_catalog.unnest(fk.column_names) WITH ORDINALITY AS c (name, n)
                               JOIN pg_catalog.pg_attribute AS a
                                 ON (a.attrelid, a.attname) = (child_oid, c.name) AND a.attnum > 0 AND NOT a.attisdropped
                               ORDER BY c.n);
        SELECT c.* INTO con FROM pg_catalog.pg_constraint AS c
        WHERE c.conrelid = child_oid AND c.conname = fk.fk_check_constraint AND c.contype = 'c';
        check_attnums := ARRAY(SELECT m[1]::smallint FROM pg_catalog.regexp_matches(con.conbin, ':varattno ([0-9]+)', 'g') AS m);

        IF child_oid IS NULL OR parent_oid IS NULL OR helper IS NULL OR con.oid IS NULL
           OR NOT con.convalidated
           OR pg_catalog.cardinality(child_attnums) <> pg_catalog.cardinality(fk.column_names)
           OR pg_catalog.cardinality(stable_columns) <> pg_catalog.cardinality(fk.column_names)
           OR check_attnums IS DISTINCT FROM child_attnums
           /*
            * The expression is exactly one call of the helper whose arguments are plain columns of this
            * table: the node tree is that FUNCEXPR (at the top) and one VAR per child column, nothing else.
            * Any wrapper (OR, NOT, a cast, a constant, another call) adds a node and is refused.
            */
           OR con.conbin NOT LIKE '{FUNCEXPR :funcid ' || helper::text || ' :funcresulttype 16 :funcretset false %'
           OR pg_catalog.length(con.conbin) - pg_catalog.length(pg_catalog.replace(con.conbin, '{', ''))
              <> 1 + pg_catalog.cardinality(child_attnums)
           OR (SELECT pg_catalog.count(*) FROM pg_catalog.regexp_matches(con.conbin, '\{VAR :varno 1 :varattno [0-9]+ ', 'g') AS m)
              <> pg_catalog.cardinality(child_attnums)
           OR (SELECT pg_catalog.count(*) FROM pg_catalog.regexp_matches(con.conbin, ':varlevelsup 0 ', 'g') AS m)
              <> pg_catalog.cardinality(child_attnums)
           OR NOT sql_saga.__internal_regular_fk_helper_is_canonical(helper::pg_catalog.regprocedure, fk.uk_schema, fk.uk_table, stable_columns) THEN
            RAISE EXCEPTION 'sql_saga: cannot convert the referencing side of foreign key % to a constraint trigger: CHECK constraint % on table % or its function % is not the one sql_saga created',
                pg_catalog.quote_ident(fk.foreign_key_name), pg_catalog.quote_ident(fk.fk_check_constraint),
                pg_catalog.format('%I.%I', fk.table_schema, fk.table_name), fk.fk_helper_function
                USING ERRCODE = 'object_not_in_prerequisite_state',
                      HINT = pg_catalog.format('Restore them as sql_saga created them, or drop the foreign key on the current version (SELECT sql_saga.drop_foreign_key_by_name(%L, %L)), run ALTER EXTENSION sql_saga UPDATE, then add it again.',
                          pg_catalog.format('%I.%I', fk.table_schema, fk.table_name), fk.foreign_key_name);
        END IF;

        /* The rows the new trigger would refuse, by the semantics of add_regular_foreign_key. */
        SELECT CASE WHEN fk.match_type = 'FULL'
                    THEN 'NOT (' || pg_catalog.string_agg(pg_catalog.format('fk.%I IS NULL', u.c), ' AND ' ORDER BY u.o) || ')'
                    ELSE pg_catalog.string_agg(pg_catalog.format('fk.%I IS NOT NULL', u.c), ' AND ' ORDER BY u.o) END
        INTO not_null_clause
        FROM pg_catalog.unnest(fk.column_names) WITH ORDINALITY AS u (c, o);
        SELECT pg_catalog.string_agg(pg_catalog.format('uk.%I = fk.%I', u.k, u.c), ' AND ' ORDER BY u.o),
               pg_catalog.string_agg(pg_catalog.format('uk.%I OPERATOR(%I.=) fk.%I', u.k, eq.nspname, u.c), ' AND ' ORDER BY u.o)
        INTO scan_where_clause, count_where_clause
        FROM ROWS FROM (pg_catalog.unnest(fk.column_names), pg_catalog.unnest(stable_columns)) WITH ORDINALITY AS u (c, k, o)
        JOIN pg_catalog.pg_attribute AS ua ON (ua.attrelid, ua.attname) = (parent_oid, u.k)
        CROSS JOIN LATERAL (
            SELECT n.nspname
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
        ) AS eq;
        IF count_where_clause IS NULL THEN
            RAISE EXCEPTION 'sql_saga: cannot convert the referencing side of foreign key %: a referenced column has no default btree equality operator',
                pg_catalog.quote_ident(fk.foreign_key_name)
                USING ERRCODE = 'undefined_function';
        END IF;
        EXECUTE pg_catalog.format('SELECT pg_catalog.count(*) FROM %I.%I AS fk WHERE %s AND NOT EXISTS (SELECT FROM %I.%I AS uk WHERE %s)',
            fk.table_schema, fk.table_name, not_null_clause, fk.uk_schema, fk.uk_table, count_where_clause)
        INTO orphans;
        IF orphans > 0 THEN
            scan := pg_catalog.format('SELECT fk.* FROM %I.%I AS fk WHERE %s AND NOT EXISTS (SELECT FROM %I.%I AS uk WHERE %s)',
                fk.table_schema, fk.table_name, not_null_clause, fk.uk_schema, fk.uk_table, scan_where_clause);
            RAISE EXCEPTION 'sql_saga: cannot convert the referencing side of foreign key % to a constraint trigger: % of % % a missing key of %; list them with %',
                pg_catalog.quote_ident(fk.foreign_key_name), CASE WHEN orphans = 1 THEN '1 row' ELSE orphans::text || ' rows' END,
                pg_catalog.format('%I.%I', fk.table_schema, fk.table_name), CASE WHEN orphans = 1 THEN 'references' ELSE 'reference' END,
                pg_catalog.format('%I.%I', fk.uk_schema, fk.uk_table), scan
                USING ERRCODE = 'foreign_key_violation',
                      HINT = 'Delete or correct those rows on the current version, then run ALTER EXTENSION sql_saga UPDATE again.';
        END IF;

        /* Metadata first: with the name withdrawn, drop_protection admits the DROP of the CHECK. */
        UPDATE sql_saga.foreign_keys AS f SET fk_check_constraint = ''::name, fk_helper_function = NULL
        WHERE f.foreign_key_name = fk.foreign_key_name;
        EXECUTE pg_catalog.format('ALTER TABLE %I.%I DROP CONSTRAINT %I', fk.table_schema, fk.table_name, fk.fk_check_constraint);
        UPDATE sql_saga.foreign_keys AS f SET fk_check_constraint = fk.fk_check_constraint
        WHERE f.foreign_key_name = fk.foreign_key_name;
        PERFORM sql_saga.__internal_create_regular_fk_child_trigger(fk.foreign_key_name);
        RAISE NOTICE 'sql_saga: Replaced CHECK constraint % on table % of foreign key % with constraint trigger %, after validating the existing rows under MATCH %',
            pg_catalog.quote_ident(fk.fk_check_constraint), pg_catalog.format('%I.%I', fk.table_schema, fk.table_name),
            pg_catalog.quote_ident(fk.foreign_key_name), pg_catalog.quote_ident(fk.fk_check_constraint), fk.match_type;

        IF NOT fk.fk_helper_function = ANY (helpers) THEN
            helpers := helpers || fk.fk_helper_function;
        END IF;
    END LOOP;

    /* Helpers no catalogued foreign key uses any more. */
    FOREACH h IN ARRAY helpers LOOP
        CONTINUE WHEN EXISTS (SELECT FROM sql_saga.foreign_keys AS f WHERE f.fk_helper_function = h);
        helper := pg_catalog.to_regprocedure(h);
        CONTINUE WHEN helper IS NULL;
        SELECT x.description INTO dependent
        FROM (SELECT pg_catalog.pg_describe_object(d.classid, d.objid, d.objsubid) AS description
              FROM pg_catalog.pg_depend AS d
              WHERE d.refclassid = 'pg_catalog.pg_proc'::pg_catalog.regclass AND d.refobjid = helper AND d.deptype IN ('n', 'a')) AS x
        ORDER BY x.description COLLATE "C"
        LIMIT 1;
        IF dependent IS NULL AND EXISTS (
               SELECT FROM pg_catalog.pg_depend AS d
               WHERE d.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass AND d.objid = helper AND d.deptype = 'e') THEN
            RAISE NOTICE 'sql_saga: Kept function %, no longer used by any foreign key, because it is a member of an extension', helper::pg_catalog.regprocedure;
            CONTINUE;
        END IF;
        IF dependent IS NOT NULL THEN
            RAISE NOTICE 'sql_saga: Kept function %, no longer used by any foreign key, because % depends on it', helper::pg_catalog.regprocedure, dependent;
            CONTINUE;
        END IF;
        EXECUTE pg_catalog.format('DROP FUNCTION %s', helper::pg_catalog.regprocedure);
        RAISE NOTICE 'sql_saga: Dropped function %, no longer used by any foreign key', h;
    END LOOP;
END;
$convert_regular_fk_checks$;

/*
 * 2. Managed views whose columns lag their base table. A view that belongs to no extension is made a member of
 * sql_saga for the repair, so that __internal_updatable_view_follow_shape may replace it in place
 * (CREATE OR REPLACE VIEW: same OID, grants, comment, rules and the user objects that depend on it; inside an
 * extension script PostgreSQL only replaces members of that extension), then detached again, which also
 * removes the pg_init_privs entries the membership recorded. A view of another extension keeps exactly that
 * membership (follow_shape recreates it then, or refuses naming a dependent object).
 */
DO $repair_stale_views$
DECLARE
    t record;
    v record;
    adopted oid[];
BEGIN
    FOR t IN
        SELECT DISTINCT b.oid AS table_oid, b.oid::pg_catalog.regclass::text COLLATE "C" AS sort_name
        FROM sql_saga.updatable_view AS uv
        JOIN pg_catalog.pg_class AS b ON b.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', uv.table_schema, uv.table_name))
        JOIN pg_catalog.pg_class AS vc ON vc.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', uv.view_schema, uv.view_name))
        WHERE ARRAY(SELECT pg_catalog.format('%I %s', a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod))
                    FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = vc.oid AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum)
              IS DISTINCT FROM
              ARRAY(SELECT pg_catalog.format('%I %s', a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod))
                    FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = b.oid AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum)
        ORDER BY 2
    LOOP
        adopted := ARRAY[]::oid[];
        FOR v IN
            SELECT vc.oid AS view_oid
            FROM sql_saga.updatable_view AS uv
            JOIN pg_catalog.pg_class AS vc ON vc.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', uv.view_schema, uv.view_name))
            WHERE pg_catalog.to_regclass(pg_catalog.format('%I.%I', uv.table_schema, uv.table_name)) = t.table_oid
              AND sql_saga.__internal_updatable_view_extension(vc.oid) IS NULL
            ORDER BY vc.oid::pg_catalog.regclass::text COLLATE "C"
        LOOP
            EXECUTE pg_catalog.format('ALTER EXTENSION sql_saga ADD VIEW %s', v.view_oid::pg_catalog.regclass);
            adopted := adopted || v.view_oid;
        END LOOP;

        PERFORM sql_saga.__internal_updatable_view_follow_shape(ARRAY[t.table_oid]);

        FOR v IN
            SELECT a.view_oid FROM pg_catalog.unnest(adopted) AS a (view_oid)
            WHERE sql_saga.__internal_updatable_view_extension(a.view_oid) = 'sql_saga'
        LOOP
            EXECUTE pg_catalog.format('ALTER EXTENSION sql_saga DROP VIEW %s', v.view_oid::pg_catalog.regclass);
        END LOOP;
    END LOOP;
END;
$repair_stale_views$;

/* 3. sql_saga_rename_following back in the state the preamble recorded (a disabled one stays disabled). */
DO $restore_rename_following$
DECLARE
    state text := pg_catalog.current_setting('sql_saga.upgrade_rename_following_state', true);
BEGIN
    CASE state
        WHEN 'O' THEN ALTER EVENT TRIGGER sql_saga_rename_following ENABLE;
        WHEN 'A' THEN ALTER EVENT TRIGGER sql_saga_rename_following ENABLE ALWAYS;
        WHEN 'R' THEN ALTER EVENT TRIGGER sql_saga_rename_following ENABLE REPLICA;
        WHEN 'D', 'missing' THEN NULL; -- the final integrity gate refuses it, with its repair
        ELSE
            RAISE EXCEPTION 'sql_saga: the update lost the recorded state of event trigger sql_saga_rename_following'
                USING ERRCODE = 'internal_error';
    END CASE;
END;
$restore_rename_following$;
