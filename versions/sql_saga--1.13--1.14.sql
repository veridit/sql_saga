\echo Use "ALTER EXTENSION sql_saga UPDATE TO '1.14'" to load this file. \quit
CREATE OR REPLACE FUNCTION sql_saga.native_library_version() RETURNS text LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT AS 'sql_saga_native', 'sql_saga_native_version_wrapper';
DO $native_guard_1_14$
BEGIN
    IF (SELECT pg_catalog.array_agg(x::int) FROM pg_catalog.unnest(pg_catalog.string_to_array(sql_saga.native_library_version(), '.')) AS x) < (SELECT pg_catalog.array_agg(x::int) FROM pg_catalog.unnest(pg_catalog.string_to_array('1.14', '.')) AS x) THEN
        RAISE EXCEPTION 'sql_saga: native library version % does not match extension update version 1.14', sql_saga.native_library_version()
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Install the sql_saga 1.14 native library and restart PostgreSQL before retrying ALTER EXTENSION.';
    END IF;
END;
$native_guard_1_14$;
SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('sql_saga 1.14 upgrade'));
DO $replace_add_unique_key$
DECLARE
    old_proc CONSTANT text := 'sql_saga.add_unique_key(regclass, name[], name, sql_saga.unique_key_type, boolean, name, name, name, text, name[])';
    new_proc CONSTANT text := 'sql_saga.add_unique_key(regclass, name[], name, sql_saga.unique_key_type, name, name, name, text, name[])';
    probe CONSTANT text := 'sql_saga.__sql_saga_1_14_upgrade_acl_probe()';
    probe_acl pg_catalog.aclitem[];
    probe_owner pg_catalog.oid;
    differs boolean;
    old_grantees text;
    new_grantees text;
    old_grants text;
    new_grants text;
    cascade_word CONSTANT text := 'CAS' || 'CADE';
BEGIN
    IF pg_catalog.to_regprocedure(old_proc) IS NULL THEN
        RETURN;
    END IF;
    IF pg_catalog.to_regprocedure(probe) IS NOT NULL THEN
        RAISE EXCEPTION 'sql_saga: function % already exists; the 1.14 update needs that name for a temporary probe', probe
            USING ERRCODE = 'duplicate_function',
                  HINT = 'Drop or rename that function, then retry the update.';
    END IF;
    EXECUTE 'CREATE FUNCTION ' || probe || ' RETURNS void LANGUAGE sql AS ''SELECT''';
    SELECT p.proacl, p.proowner INTO probe_acl, probe_owner
    FROM pg_catalog.pg_proc AS p
    WHERE p.oid = pg_catalog.to_regprocedure(probe);
    EXECUTE 'DROP FUNCTION ' || probe;

    WITH old_set AS (
        SELECT a.grantee, pg_catalog.bool_or(a.is_grantable) AS grantable
        FROM pg_catalog.pg_proc AS p
        CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(p.proacl, pg_catalog.acldefault('f', p.proowner))) AS a
        WHERE p.oid = pg_catalog.to_regprocedure(old_proc)
          AND a.privilege_type = 'EXECUTE'
          AND a.grantee OPERATOR(pg_catalog.<>) p.proowner
        GROUP BY a.grantee
    ), new_set AS (
        SELECT a.grantee, pg_catalog.bool_or(a.is_grantable) AS grantable
        FROM pg_catalog.aclexplode(coalesce(probe_acl, pg_catalog.acldefault('f', probe_owner))) AS a
        WHERE a.privilege_type = 'EXECUTE'
          AND a.grantee OPERATOR(pg_catalog.<>) probe_owner
          AND (a.grantee OPERATOR(pg_catalog.<>) 0 OR true)
        GROUP BY a.grantee
    ), both_sets AS (
        SELECT 'old' AS side, o.grantee, o.grantable FROM old_set AS o
        UNION ALL
        SELECT 'new', n.grantee, n.grantable FROM new_set AS n
    ), named AS (
        SELECT b.side, b.grantable, b.grantee = 0 AS is_public,
               CASE WHEN b.grantee = 0 THEN 'PUBLIC' ELSE b.grantee::pg_catalog.regrole::text END AS who
        FROM both_sets AS b
    )
    SELECT
        EXISTS (SELECT FROM old_set AS o FULL JOIN new_set AS n ON n.grantee = o.grantee
                WHERE o.grantable IS DISTINCT FROM n.grantable),
        pg_catalog.string_agg(nm.who, ', ' ORDER BY NOT nm.is_public, nm.who COLLATE "C") FILTER (WHERE nm.side = 'old'),
        pg_catalog.string_agg(nm.who, ', ' ORDER BY NOT nm.is_public, nm.who COLLATE "C") FILTER (WHERE nm.side = 'new'),
        pg_catalog.string_agg(pg_catalog.format('GRANT EXECUTE ON FUNCTION %s TO %s%s;', new_proc, nm.who,
                                                CASE WHEN nm.grantable THEN ' WITH GRANT OPTION' ELSE '' END),
                              ' ' ORDER BY NOT nm.is_public, nm.who COLLATE "C") FILTER (WHERE nm.side = 'old'),
        pg_catalog.string_agg(pg_catalog.format('GRANT EXECUTE ON FUNCTION %s TO %s%s;', old_proc, nm.who,
                                                CASE WHEN nm.grantable THEN ' WITH GRANT OPTION' ELSE '' END),
                              ' ' ORDER BY NOT nm.is_public, nm.who COLLATE "C") FILTER (WHERE nm.side = 'new')
    INTO differs, old_grantees, new_grantees, old_grants, new_grants
    FROM named AS nm;

    IF differs THEN
        RAISE EXCEPTION 'sql_saga: the privileges on % differ from those its 1.14 replacement gets; the update replaces this function, and cannot carry changed privileges over', old_proc
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = pg_catalog.concat_ws(' ',
                      'Reset them, update, and apply them to the new signature after the update. Run:',
                      CASE WHEN old_grantees IS NOT NULL THEN pg_catalog.format('REVOKE ALL ON FUNCTION %s FROM %s %s;', old_proc, old_grantees, cascade_word) END,
                      new_grants,
                      'ALTER EXTENSION sql_saga UPDATE;',
                      CASE WHEN new_grantees IS NOT NULL THEN pg_catalog.format('REVOKE ALL ON FUNCTION %s FROM %s %s;', new_proc, new_grantees, cascade_word) END,
                      old_grants);
    END IF;
    EXECUTE 'DROP ROUTINE ' || old_proc;
END;
$replace_add_unique_key$;
DO $replace__api_add_unique_key$
DECLARE
    old_proc CONSTANT text := 'sql_saga.__api_add_unique_key(regclass, name[], name, sql_saga.unique_key_type, boolean, name, name, name, text, name[])';
    new_proc CONSTANT text := 'sql_saga.__api_add_unique_key(regclass, name[], name, sql_saga.unique_key_type, name, name, name, text, name[])';
    probe CONSTANT text := 'sql_saga.__sql_saga_1_14_upgrade_acl_probe()';
    probe_acl pg_catalog.aclitem[];
    probe_owner pg_catalog.oid;
    differs boolean;
    old_grantees text;
    new_grantees text;
    old_grants text;
    new_grants text;
    cascade_word CONSTANT text := 'CAS' || 'CADE';
BEGIN
    IF pg_catalog.to_regprocedure(old_proc) IS NULL THEN
        RETURN;
    END IF;
    IF pg_catalog.to_regprocedure(probe) IS NOT NULL THEN
        RAISE EXCEPTION 'sql_saga: function % already exists; the 1.14 update needs that name for a temporary probe', probe
            USING ERRCODE = 'duplicate_function',
                  HINT = 'Drop or rename that function, then retry the update.';
    END IF;
    EXECUTE 'CREATE FUNCTION ' || probe || ' RETURNS void LANGUAGE sql AS ''SELECT''';
    SELECT p.proacl, p.proowner INTO probe_acl, probe_owner
    FROM pg_catalog.pg_proc AS p
    WHERE p.oid = pg_catalog.to_regprocedure(probe);
    EXECUTE 'DROP FUNCTION ' || probe;

    WITH old_set AS (
        SELECT a.grantee, pg_catalog.bool_or(a.is_grantable) AS grantable
        FROM pg_catalog.pg_proc AS p
        CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(p.proacl, pg_catalog.acldefault('f', p.proowner))) AS a
        WHERE p.oid = pg_catalog.to_regprocedure(old_proc)
          AND a.privilege_type = 'EXECUTE'
          AND a.grantee OPERATOR(pg_catalog.<>) p.proowner
        GROUP BY a.grantee
    ), new_set AS (
        SELECT a.grantee, pg_catalog.bool_or(a.is_grantable) AS grantable
        FROM pg_catalog.aclexplode(coalesce(probe_acl, pg_catalog.acldefault('f', probe_owner))) AS a
        WHERE a.privilege_type = 'EXECUTE'
          AND a.grantee OPERATOR(pg_catalog.<>) probe_owner
          AND (a.grantee OPERATOR(pg_catalog.<>) 0 OR false)
        GROUP BY a.grantee
    ), both_sets AS (
        SELECT 'old' AS side, o.grantee, o.grantable FROM old_set AS o
        UNION ALL
        SELECT 'new', n.grantee, n.grantable FROM new_set AS n
    ), named AS (
        SELECT b.side, b.grantable, b.grantee = 0 AS is_public,
               CASE WHEN b.grantee = 0 THEN 'PUBLIC' ELSE b.grantee::pg_catalog.regrole::text END AS who
        FROM both_sets AS b
    )
    SELECT
        EXISTS (SELECT FROM old_set AS o FULL JOIN new_set AS n ON n.grantee = o.grantee
                WHERE o.grantable IS DISTINCT FROM n.grantable),
        pg_catalog.string_agg(nm.who, ', ' ORDER BY NOT nm.is_public, nm.who COLLATE "C") FILTER (WHERE nm.side = 'old'),
        pg_catalog.string_agg(nm.who, ', ' ORDER BY NOT nm.is_public, nm.who COLLATE "C") FILTER (WHERE nm.side = 'new'),
        pg_catalog.string_agg(pg_catalog.format('GRANT EXECUTE ON FUNCTION %s TO %s%s;', new_proc, nm.who,
                                                CASE WHEN nm.grantable THEN ' WITH GRANT OPTION' ELSE '' END),
                              ' ' ORDER BY NOT nm.is_public, nm.who COLLATE "C") FILTER (WHERE nm.side = 'old'),
        pg_catalog.string_agg(pg_catalog.format('GRANT EXECUTE ON FUNCTION %s TO %s%s;', old_proc, nm.who,
                                                CASE WHEN nm.grantable THEN ' WITH GRANT OPTION' ELSE '' END),
                              ' ' ORDER BY NOT nm.is_public, nm.who COLLATE "C") FILTER (WHERE nm.side = 'new')
    INTO differs, old_grantees, new_grantees, old_grants, new_grants
    FROM named AS nm;

    IF differs THEN
        RAISE EXCEPTION 'sql_saga: the privileges on % differ from those its 1.14 replacement gets; the update replaces this function, and cannot carry changed privileges over', old_proc
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = pg_catalog.concat_ws(' ',
                      'Reset them, update, and apply them to the new signature after the update. Run:',
                      CASE WHEN old_grantees IS NOT NULL THEN pg_catalog.format('REVOKE ALL ON FUNCTION %s FROM %s %s;', old_proc, old_grantees, cascade_word) END,
                      new_grants,
                      'ALTER EXTENSION sql_saga UPDATE;',
                      CASE WHEN new_grantees IS NOT NULL THEN pg_catalog.format('REVOKE ALL ON FUNCTION %s FROM %s %s;', new_proc, new_grantees, cascade_word) END,
                      old_grants);
    END IF;
    EXECUTE 'DROP ROUTINE ' || old_proc;
END;
$replace__api_add_unique_key$;
DO $replace__internal_generate_pk_consistency_constraints$
DECLARE
    old_proc CONSTANT text := 'sql_saga.__internal_generate_pk_consistency_constraints(name, name[], name[], name)';
    new_proc CONSTANT text := 'sql_saga.__internal_generate_pk_consistency_constraints(name, name[], name[], name[])';
    probe CONSTANT text := 'sql_saga.__sql_saga_1_14_upgrade_acl_probe()';
    probe_acl pg_catalog.aclitem[];
    probe_owner pg_catalog.oid;
    differs boolean;
    old_grantees text;
    new_grantees text;
    old_grants text;
    new_grants text;
    cascade_word CONSTANT text := 'CAS' || 'CADE';
BEGIN
    IF pg_catalog.to_regprocedure(old_proc) IS NULL THEN
        RETURN;
    END IF;
    IF pg_catalog.to_regprocedure(probe) IS NOT NULL THEN
        RAISE EXCEPTION 'sql_saga: function % already exists; the 1.14 update needs that name for a temporary probe', probe
            USING ERRCODE = 'duplicate_function',
                  HINT = 'Drop or rename that function, then retry the update.';
    END IF;
    EXECUTE 'CREATE FUNCTION ' || probe || ' RETURNS void LANGUAGE sql AS ''SELECT''';
    SELECT p.proacl, p.proowner INTO probe_acl, probe_owner
    FROM pg_catalog.pg_proc AS p
    WHERE p.oid = pg_catalog.to_regprocedure(probe);
    EXECUTE 'DROP FUNCTION ' || probe;

    WITH old_set AS (
        SELECT a.grantee, pg_catalog.bool_or(a.is_grantable) AS grantable
        FROM pg_catalog.pg_proc AS p
        CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(p.proacl, pg_catalog.acldefault('f', p.proowner))) AS a
        WHERE p.oid = pg_catalog.to_regprocedure(old_proc)
          AND a.privilege_type = 'EXECUTE'
          AND a.grantee OPERATOR(pg_catalog.<>) p.proowner
        GROUP BY a.grantee
    ), new_set AS (
        SELECT a.grantee, pg_catalog.bool_or(a.is_grantable) AS grantable
        FROM pg_catalog.aclexplode(coalesce(probe_acl, pg_catalog.acldefault('f', probe_owner))) AS a
        WHERE a.privilege_type = 'EXECUTE'
          AND a.grantee OPERATOR(pg_catalog.<>) probe_owner
          AND (a.grantee OPERATOR(pg_catalog.<>) 0 OR true)
        GROUP BY a.grantee
    ), both_sets AS (
        SELECT 'old' AS side, o.grantee, o.grantable FROM old_set AS o
        UNION ALL
        SELECT 'new', n.grantee, n.grantable FROM new_set AS n
    ), named AS (
        SELECT b.side, b.grantable, b.grantee = 0 AS is_public,
               CASE WHEN b.grantee = 0 THEN 'PUBLIC' ELSE b.grantee::pg_catalog.regrole::text END AS who
        FROM both_sets AS b
    )
    SELECT
        EXISTS (SELECT FROM old_set AS o FULL JOIN new_set AS n ON n.grantee = o.grantee
                WHERE o.grantable IS DISTINCT FROM n.grantable),
        pg_catalog.string_agg(nm.who, ', ' ORDER BY NOT nm.is_public, nm.who COLLATE "C") FILTER (WHERE nm.side = 'old'),
        pg_catalog.string_agg(nm.who, ', ' ORDER BY NOT nm.is_public, nm.who COLLATE "C") FILTER (WHERE nm.side = 'new'),
        pg_catalog.string_agg(pg_catalog.format('GRANT EXECUTE ON FUNCTION %s TO %s%s;', new_proc, nm.who,
                                                CASE WHEN nm.grantable THEN ' WITH GRANT OPTION' ELSE '' END),
                              ' ' ORDER BY NOT nm.is_public, nm.who COLLATE "C") FILTER (WHERE nm.side = 'old'),
        pg_catalog.string_agg(pg_catalog.format('GRANT EXECUTE ON FUNCTION %s TO %s%s;', old_proc, nm.who,
                                                CASE WHEN nm.grantable THEN ' WITH GRANT OPTION' ELSE '' END),
                              ' ' ORDER BY NOT nm.is_public, nm.who COLLATE "C") FILTER (WHERE nm.side = 'new')
    INTO differs, old_grantees, new_grantees, old_grants, new_grants
    FROM named AS nm;

    IF differs THEN
        RAISE EXCEPTION 'sql_saga: the privileges on % differ from those its 1.14 replacement gets; the update replaces this function, and cannot carry changed privileges over', old_proc
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = pg_catalog.concat_ws(' ',
                      'Reset them, update, and apply them to the new signature after the update. Run:',
                      CASE WHEN old_grantees IS NOT NULL THEN pg_catalog.format('REVOKE ALL ON FUNCTION %s FROM %s %s;', old_proc, old_grantees, cascade_word) END,
                      new_grants,
                      'ALTER EXTENSION sql_saga UPDATE;',
                      CASE WHEN new_grantees IS NOT NULL THEN pg_catalog.format('REVOKE ALL ON FUNCTION %s FROM %s %s;', new_proc, new_grantees, cascade_word) END,
                      old_grants);
    END IF;
    EXECUTE 'DROP ROUTINE ' || old_proc;
END;
$replace__internal_generate_pk_consistency_constraints$;
/*
 * The guard that binds a natural key to its entity (1.14, hug mnntde): for each stable primary key column (the primary
 * key's columns without the range), EXCLUDE (natural key WITH =, that column WITH <>). Together they refuse a natural
 * key value on two rows whose primary key differs in any column, at any time. No guard is needed when the natural key
 * itself contains every stable primary key column (outside its mutually exclusive columns): then its own uniqueness
 * already binds it. There is no opt-out: a natural key that could move between entities identifies nothing.
 * Names: <key>_pk_consistency_excl for a single stable column (as before 1.14), <key>_<column>_pk_consistency_excl for
 * each column of a composite one; a mutually exclusive key adds the XOR column to the name, as before.
 */
CREATE OR REPLACE FUNCTION sql_saga.__internal_generate_pk_consistency_constraints(
    p_unique_key_name name,
    p_natural_key_columns name[],
    p_mutually_exclusive_columns name[],
    p_stable_pk_columns name[]
)
RETURNS jsonb -- { "cmds": text[], "names": name[] }
LANGUAGE plpgsql AS
$function$
#variable_conflict use_variable
DECLARE
    v_constraint_cmds text[] := '{}';
    v_constraint_names name[] := '{}';
    v_constraint_name name;
    v_constraint_sql text;
    v_withs text[];
    v_pk_column name;
    v_composite boolean := pg_catalog.array_length(p_stable_pk_columns, 1) > 1;
    non_xor_cols name[] := ARRAY(SELECT c FROM pg_catalog.unnest(p_natural_key_columns) AS c
                                 WHERE p_mutually_exclusive_columns IS NULL OR c <> ALL (p_mutually_exclusive_columns));
    xor_col name;
    where_clause_partial text;
BEGIN
    IF p_stable_pk_columns IS NULL OR pg_catalog.array_length(p_stable_pk_columns, 1) IS NULL THEN
        RAISE EXCEPTION 'sql_saga: a primary key without a stable column cannot bind natural key "%"', p_unique_key_name;
    END IF;
    -- Implied by the natural key's own uniqueness: it holds every stable primary key column.
    IF p_stable_pk_columns <@ non_xor_cols THEN
        RETURN jsonb_build_object('cmds', '[]'::jsonb, 'names', '[]'::jsonb);
    END IF;
    FOREACH v_pk_column IN ARRAY p_stable_pk_columns LOOP
        IF p_mutually_exclusive_columns IS NULL THEN
            -- A primary key column that is part of the natural key binds itself: (c WITH =, c WITH <>) never conflicts.
            CONTINUE WHEN v_pk_column = ANY (p_natural_key_columns);
            v_constraint_name := CASE WHEN v_composite
                THEN sql_saga.__internal_make_name(ARRAY[p_unique_key_name, v_pk_column, 'pk_consistency_excl'])
                ELSE p_unique_key_name || '_pk_consistency_excl' END;
            -- Truncated to 63 bytes, two long names can meet: the ordinal goes in the part that is never truncated.
            IF v_constraint_name = ANY (v_constraint_names) THEN
                v_constraint_name := sql_saga.__internal_make_name(ARRAY[p_unique_key_name, v_pk_column],
                    pg_catalog.cardinality(v_constraint_names) + 1 || '_pk_consistency_excl');
            END IF;
            SELECT array_agg(pg_catalog.format('%I WITH =', col)) INTO v_withs FROM pg_catalog.unnest(p_natural_key_columns) AS col;
            v_withs := v_withs || pg_catalog.format('%I WITH <>', v_pk_column);
            v_constraint_sql := pg_catalog.format('ADD CONSTRAINT %I EXCLUDE USING gist (%s)', v_constraint_name, pg_catalog.array_to_string(v_withs, ', '));
            v_constraint_cmds := pg_catalog.array_append(v_constraint_cmds, v_constraint_sql);
            v_constraint_names := pg_catalog.array_append(v_constraint_names, v_constraint_name);
        ELSE
            FOREACH xor_col IN ARRAY p_mutually_exclusive_columns LOOP
                CONTINUE WHEN v_pk_column = ANY (non_xor_cols || xor_col);
                v_constraint_name := sql_saga.__internal_make_name(
                    CASE WHEN v_composite THEN ARRAY[p_unique_key_name, xor_col, v_pk_column, 'pk_consistency_excl']
                         ELSE ARRAY[p_unique_key_name, xor_col, 'pk_consistency_excl'] END);
                IF v_constraint_name = ANY (v_constraint_names) THEN
                    v_constraint_name := sql_saga.__internal_make_name(ARRAY[p_unique_key_name, xor_col, v_pk_column],
                        pg_catalog.cardinality(v_constraint_names) + 1 || '_pk_consistency_excl');
                END IF;
                SELECT array_agg(pg_catalog.format('%I WITH =', col)) INTO v_withs FROM pg_catalog.unnest(non_xor_cols || xor_col) AS col;
                v_withs := v_withs || pg_catalog.format('%I WITH <>', v_pk_column);
                -- Written as PostgreSQL deparses it (pg_get_constraintdef), so check_integrity compares text with text.
                where_clause_partial := pg_catalog.format('WHERE ((%s))',
                    (SELECT string_agg(p, ' AND ' ORDER BY o) FROM (
                        SELECT 0 AS o, pg_catalog.format('(%I IS NOT NULL)', xor_col) AS p
                        UNION ALL
                        SELECT u.n, pg_catalog.format('(%I IS NULL)', u.other_col)
                        FROM pg_catalog.unnest(p_mutually_exclusive_columns) WITH ORDINALITY AS u(other_col, n)
                        WHERE u.other_col <> xor_col) AS parts));
                v_constraint_sql := pg_catalog.format('ADD CONSTRAINT %I EXCLUDE USING gist (%s) %s', v_constraint_name,
                    pg_catalog.array_to_string(v_withs, ', '), where_clause_partial);
                v_constraint_cmds := pg_catalog.array_append(v_constraint_cmds, v_constraint_sql);
                v_constraint_names := pg_catalog.array_append(v_constraint_names, v_constraint_name);
            END LOOP;
        END IF;
    END LOOP;

    RETURN jsonb_build_object('cmds', to_jsonb(v_constraint_cmds), 'names', to_jsonb(v_constraint_names));
END;
$function$;
CREATE OR REPLACE FUNCTION sql_saga.__api_add_unique_key(
        table_oid regclass,
        column_names name[],
        era_name name DEFAULT 'valid',
        key_type sql_saga.unique_key_type DEFAULT 'natural',
        unique_key_name name DEFAULT NULL,
        unique_constraint name DEFAULT NULL,
        exclude_constraint name DEFAULT NULL,
        predicate text DEFAULT NULL,
        mutually_exclusive_columns name[] DEFAULT NULL)
 RETURNS name
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, pg_temp
AS
$function$
#variable_conflict use_variable
DECLARE
    table_schema name;
    table_name name;
    era_row sql_saga.era;
    column_attnums smallint[];
    era_attnums smallint[];
    idx integer;
    constraint_record record;
    pass integer;
    sql text;
    alter_cmds text[];
    v_unique_constraint_found boolean := false;
    v_exclude_constraint_found boolean := false;
    v_check_constraint name;
    unique_sql text;
    exclude_sql text;
    where_clause text;

    v_pk_consistency_constraint_names name[];
    v_stable_cols_to_store name[];
BEGIN
    PERFORM sql_saga.__internal_entry_guard();
    PERFORM sql_saga.__internal_require_owner(table_oid, 'add_unique_key');
    RAISE DEBUG 'add_unique_key entry: table_oid=%, column_names=%, era_name=%, key_type=%', table_oid, column_names, era_name, key_type;

    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'no table name specified';
    END IF;

    SELECT n.nspname, c.relname
    INTO table_schema, table_name
    FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = table_oid;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    SELECT p.*
    INTO era_row
    FROM sql_saga.era AS p
    WHERE (p.table_schema, p.table_name, p.era_name) = (table_schema, table_name, era_name);

    IF NOT FOUND THEN
        RAISE EXCEPTION 'era "%" does not exist', era_name;
    END IF;

    -- For metadata storage, we use the exact column list provided by the user,
    -- as this is what symmetric drop functions will expect. The native unique
    -- constraint will include the temporal column, but our metadata only tracks
    -- the stable part of the key. An explicit check below ensures the user does
    -- not include the range column here.
    v_stable_cols_to_store := __api_add_unique_key.column_names;

    -- This is no longer used by native temporal keys, but is still needed for XOR key logic on predicated keys.
    IF era_row.valid_from_column_name IS NOT NULL THEN
        era_attnums := ARRAY[
            (SELECT a.attnum FROM pg_catalog.pg_attribute AS a WHERE (a.attrelid, a.attname) = (table_oid, era_row.valid_from_column_name)),
            (SELECT a.attnum FROM pg_catalog.pg_attribute AS a WHERE (a.attrelid, a.attname) = (table_oid, era_row.valid_until_column_name))
        ];
    END IF;

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

    /* Make sure the range column isn't also in the normal columns */
    IF era_row.range_column_name = ANY (column_names) THEN
        RAISE EXCEPTION 'range column "%" cannot also be part of the key columns', era_row.range_column_name;
    END IF;

    /*
     * Columns belonging to a SYSTEM_TIME period are not allowed in a UNIQUE
     * key. SQL:2016 11.7 SR 5)b)
     */
    IF EXISTS (
        SELECT FROM sql_saga.era AS e
        WHERE (e.table_schema, e.table_name, e.era_name) = (era_row.table_schema, era_row.table_name, 'system_time')
          AND ARRAY[e.valid_from_column_name, e.valid_until_column_name] && column_names)
    THEN
        RAISE EXCEPTION 'columns in era for SYSTEM_TIME are not allowed in UNIQUE keys';
    END IF;

    -- Validate parameter combinations
    IF key_type = 'predicated' AND predicate IS NULL THEN
        RAISE EXCEPTION 'a predicate must be provided when key_type is ''predicated''';
    END IF;

    IF key_type IN ('primary', 'natural') AND predicate IS NOT NULL THEN
        RAISE EXCEPTION 'a predicate can only be provided when key_type is ''predicated''';
    END IF;

    IF mutually_exclusive_columns IS NOT NULL THEN
        IF NOT column_names @> mutually_exclusive_columns THEN
            RAISE EXCEPTION 'all mutually_exclusive_columns must be present in column_names';
        END IF;
        IF key_type <> 'natural' THEN
            RAISE EXCEPTION 'mutually_exclusive_columns can only be used with key_type = ''natural''';
        END IF;
    END IF;

    IF predicate IS NOT NULL THEN
        /* One expression only, resolved as the caller; store and use its canonical deparse (Q4). */
        predicate := sql_saga.__internal_predicate_for_table(table_oid, predicate);
    END IF;
    where_clause := CASE
        WHEN predicate IS NULL THEN ''
        WHEN left(predicate, 1) = '(' THEN format(' WHERE %s', predicate /* already parenthesized canonical deparse */)
        ELSE format(' WHERE (%s)', predicate /* %s */)
    END;

    IF key_type IN ('primary', 'natural') THEN
        -- When creating a primary key, validate that the table schema is compatible with SCD Type 2 history.
        IF key_type = 'primary' THEN
            DECLARE
                identity_column_name name;
            BEGIN
                -- Check for a GENERATED ALWAYS identity column first, as it's a more specific error.
                SELECT a.attname
                INTO identity_column_name
                FROM pg_catalog.pg_attribute a
                WHERE a.attrelid = table_oid
                AND a.attidentity = 'a'; -- 'a' = always, 'd' = by default

                IF identity_column_name IS NOT NULL THEN
                    RAISE EXCEPTION 'table "%" has a GENERATED ALWAYS AS IDENTITY column ("%"); this is incompatible with SCD Type 2 history as it prevents inserting new historical versions of an entity', sql_saga.__internal_display_name(table_oid), identity_column_name;
                END IF;

                -- Check for a simple primary key that does not include the temporal columns.
                DECLARE
                    pk_columns name[];
                BEGIN
                    SELECT
                        array_agg(a.attname ORDER BY u.ord)
                    INTO
                        pk_columns
                    FROM
                        pg_catalog.pg_constraint c
                        JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS u(attnum, ord) ON TRUE
                        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = u.attnum
                    WHERE
                        c.conrelid = table_oid AND c.contype = 'p';

                    IF pk_columns IS NOT NULL AND NOT (pk_columns @> ARRAY[era_row.range_column_name]) THEN
                        RAISE EXCEPTION 'table "%" has a simple PRIMARY KEY that does not include the temporal range column ("%"); this is incompatible with SCD Type 2 history', sql_saga.__internal_display_name(table_oid), era_row.range_column_name;
                    END IF;
                END;
            END;
        END IF;
        /* If we were given a unique constraint to use, look it up and make sure it matches */
        -- For primary and natural keys, we now use a native temporal constraint.
        -- These constraints must be NOT DEFERRABLE (the default) to be referenceable by
        -- native temporal foreign keys. The with_temporary_temporal_gaps execution strategy
        -- (DELETE->UPDATE->INSERT) avoids creating overlaps that would violate this constraint.
        SELECT format('%s (%s, %I WITHOUT OVERLAPS)',
            CASE WHEN key_type = 'primary' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END, /* %s */
            string_agg(quote_ident(u.column_name), ', ' ORDER BY u.ordinality), /* %s */
            era_row.range_column_name /* %I */
        )
        INTO unique_sql
        FROM unnest(column_names) WITH ORDINALITY AS u (column_name, ordinality);

        IF unique_constraint IS NULL AND key_type = 'primary' THEN
            -- If this is a primary key and no name was given, check if one already exists.
            SELECT c.conname INTO unique_constraint
            FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.contype) = (table_oid, 'p');
        END IF;

        IF unique_constraint IS NOT NULL THEN
            SELECT c.oid, c.contype, c.condeferrable, c.condeferred, c.conkey
            INTO constraint_record
            FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.conname) = (table_oid, unique_constraint);

            IF FOUND THEN
                v_unique_constraint_found := true;

                IF key_type = 'primary' AND constraint_record.contype <> 'p' THEN
                    RAISE EXCEPTION 'constraint "%" is not a PRIMARY KEY', unique_constraint;
                ELSIF key_type = 'natural' AND constraint_record.contype NOT IN ('p', 'u') THEN
                    RAISE EXCEPTION 'constraint "%" is not a PRIMARY KEY or UNIQUE KEY', unique_constraint;
                END IF;

                -- PostgreSQL 18 requirement: WITHOUT OVERLAPS constraints must be NOT DEFERRABLE
                -- to be referenceable by native temporal FKs (FOREIGN KEY ... PERIOD).
                -- Users migrating from trigger-based FKs may have DEFERRABLE PKs which must be changed.
                -- The with_temporary_temporal_gaps strategy (DELETE→UPDATE→INSERT) creates temporary
                -- gaps (tolerated by DEFERRABLE FKs) rather than overlaps (which would violate this constraint).
                DECLARE
                    unique_sql_nulls_not_distinct text;
                    actual_def text;
                BEGIN
                    -- Generate the NULLS NOT DISTINCT variant for comparison
                    SELECT format('%s (%s, %I WITHOUT OVERLAPS)',
                        CASE WHEN key_type = 'primary' THEN 'PRIMARY KEY NULLS NOT DISTINCT' ELSE 'UNIQUE NULLS NOT DISTINCT' END,
                        string_agg(quote_ident(u.column_name), ', ' ORDER BY u.ordinality),
                        era_row.range_column_name
                    )
                    INTO unique_sql_nulls_not_distinct
                    FROM unnest(column_names) WITH ORDINALITY AS u (column_name, ordinality);

                    actual_def := pg_catalog.pg_get_constraintdef(constraint_record.oid);

                    -- Check if constraint matches either traditional or NULLS NOT DISTINCT format
                    IF actual_def <> unique_sql AND actual_def <> unique_sql_nulls_not_distinct THEN
                        RAISE EXCEPTION E'constraint "%" does not match expected format.\nExpected (traditional): "%"\nExpected (NULLS NOT DISTINCT): "%"\nFound: "%"\n\nPostgreSQL 18 Temporal FK Requirement:\nUnique constraints with WITHOUT OVERLAPS must be NOT DEFERRABLE to be referenceable by native temporal foreign keys. The DELETE→UPDATE→INSERT execution strategy avoids overlaps while tolerating temporary gaps through DEFERRABLE FK checks.\n\nSee AGENTS.md section "PostgreSQL 18 Temporal Constraints and Foreign Keys" for details.',
                            unique_constraint,
                            unique_sql,
                            unique_sql_nulls_not_distinct,
                            actual_def;
                    END IF;
                END;

                /* Looks good, let's use it. */
            END IF;
        END IF;
    ELSIF key_type = 'predicated' THEN
        -- For predicated keys, we don't create a UNIQUE constraint/index.
        -- Uniqueness is enforced by the EXCLUDE constraint.
        -- We can clear unique_sql as it's not applicable.
        unique_sql := NULL;
    END IF;

    -- Native temporal constraints (WITHOUT OVERLAPS) replace exclusion constraints
    -- for primary and natural keys. Predicated keys still require an exclusion constraint.
    IF key_type = 'predicated' THEN
        DECLARE
            withs text[];
        BEGIN
            SELECT array_agg(format('%I WITH =', column_name /* %I */) ORDER BY n.ordinality)
            INTO withs
            FROM unnest(column_names) WITH ORDINALITY AS n (column_name, ordinality);

            withs := withs || format('%I WITH &&',
                era_row.range_column_name /* %I */
            );

            exclude_sql := format('EXCLUDE USING gist (%s)%s DEFERRABLE', array_to_string(withs, ', ') /* %s */, where_clause /* %s */);
        END;

        IF exclude_constraint IS NOT NULL THEN
            SELECT c.oid, c.contype, c.condeferrable, c.condeferred, pg_catalog.pg_get_constraintdef(c.oid) AS definition
            INTO constraint_record
            FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.conname) = (table_oid, exclude_constraint);

            IF FOUND THEN
                v_exclude_constraint_found := true;

                IF constraint_record.contype <> 'x' THEN
                    RAISE EXCEPTION 'constraint "%" is not an EXCLUDE constraint', exclude_constraint;
                END IF;

                IF NOT constraint_record.condeferrable THEN
                    RAISE EXCEPTION 'constraint "%" must be DEFERRABLE', exclude_constraint;
                END IF;

                IF constraint_record.condeferred THEN
                    RAISE EXCEPTION 'constraint "%" must be INITIALLY IMMEDIATE', exclude_constraint;
                END IF;

                IF constraint_record.definition <> exclude_sql THEN
                    RAISE EXCEPTION 'constraint "%" does not match. Expected definition: "%", but found definition: "%".',
                        exclude_constraint,
                        exclude_sql,
                        constraint_record.definition;
                END IF;

                /* Looks good, let's use it. */
            END IF;
        END IF;
    END IF;

    /*
     * Generate a name for the unique constraint.  We don't have to worry about
     * concurrency here because all period ddl commands lock the periods table.
     */
    IF unique_key_name IS NULL THEN
        unique_key_name := sql_saga.__internal_make_name(
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
    IF key_type = 'primary' THEN
        SELECT array_agg(format('ALTER COLUMN %I SET NOT NULL', u.column_name))
        INTO alter_cmds
        FROM unnest(column_names) WITH ORDINALITY AS u (column_name, ordinality);
    ELSE
        alter_cmds := '{}';
    END IF;

    IF key_type IN ('primary', 'natural') THEN
        -- For XOR keys, we don't create a single overarching UNIQUE constraint.
        -- Uniqueness is handled by the partial indexes.
        IF mutually_exclusive_columns IS NULL THEN
            IF NOT v_unique_constraint_found THEN
                IF unique_constraint IS NULL THEN
                    -- If this is a primary key, Postgres will name it automatically.
                    -- For natural keys, we'll generate a name for consistency.
                    IF key_type = 'natural' THEN
                        unique_constraint := unique_key_name || '_uniq';
                        alter_cmds := alter_cmds || ('ADD CONSTRAINT ' || quote_ident(unique_constraint) || ' ' || unique_sql);
                    ELSE
                        alter_cmds := alter_cmds || ('ADD ' || unique_sql);
                    END IF;
                ELSE
                     alter_cmds := alter_cmds || ('ADD CONSTRAINT ' || quote_ident(unique_constraint) || ' ' || unique_sql);
                END IF;
            END IF;
        END IF;
    ELSIF key_type = 'predicated' THEN
        -- For predicated keys, uniqueness is handled by the EXCLUDE constraint, not a UNIQUE index.
        -- No action needed here for unique_sql.
    END IF;

    -- For XOR keys, we create partial exclusion constraints instead of one broad one.
    IF mutually_exclusive_columns IS NULL THEN
        -- Add the exclusion constraint only for predicated keys.
        IF key_type = 'predicated' AND NOT v_exclude_constraint_found THEN
            IF exclude_constraint IS NULL THEN
                exclude_constraint := unique_key_name || '_excl';
            END IF;
            alter_cmds := alter_cmds || ('ADD CONSTRAINT ' || quote_ident(exclude_constraint) || ' ' || exclude_sql);
        END IF;
    ELSE -- mutually_exclusive_columns IS NOT NULL (XOR keys)
        BEGIN
            -- XOR keys now use a single UNIQUE constraint with NULLS NOT DISTINCT
            -- This replaces the complex CHECK + partial indexes + partial EXCLUDE constraints

            -- For XOR keys, we use the unified unique constraint approach
            IF unique_constraint IS NULL THEN
                unique_constraint := unique_key_name;
            END IF;

            -- Generate UNIQUE constraint with NULLS NOT DISTINCT to handle XOR columns
            unique_sql := format('UNIQUE NULLS NOT DISTINCT (%s, %I WITHOUT OVERLAPS)',
                (SELECT string_agg(format('%I', column_name), ', ') FROM unnest(column_names) AS column_name),
                era_row.range_column_name
            );

            alter_cmds := alter_cmds || ('ADD CONSTRAINT ' || quote_ident(unique_constraint) || ' ' || unique_sql);

            -- Clear XOR-specific metadata (no longer using partial constraints)
            v_check_constraint := NULL;
            exclude_constraint := NULL;
        END;
    END IF;

    -- For native temporal keys, there is no separate exclusion constraint.
    IF key_type IN ('primary', 'natural') THEN
        exclude_constraint := NULL;
    END IF;

    -- A natural key is bound to its entity (1.14): with a primary key the guard is built now; without one the state is
    -- pending ('{}') until add_unique_key adds the primary key. '{}' with a primary key means the guard is implied.
    IF key_type = 'natural' THEN
        DECLARE
            pk_column_names name[];
            consistency_result jsonb;
        BEGIN
            SELECT uk.column_names INTO pk_column_names FROM sql_saga.unique_keys uk WHERE (uk.table_schema, uk.table_name) = (table_schema, table_name) AND uk.key_type = 'primary'::sql_saga.unique_key_type;
            IF NOT FOUND THEN
                SELECT array_agg(a.attname ORDER BY u.ord) INTO pk_column_names FROM pg_catalog.pg_constraint c JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS u(attnum, ord) ON TRUE JOIN pg_catalog.pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = u.attnum WHERE c.conrelid = table_oid AND c.contype = 'p';
            END IF;

            IF pk_column_names IS NULL THEN
                v_pk_consistency_constraint_names := '{}';
            ELSE
                consistency_result := sql_saga.__internal_generate_pk_consistency_constraints(
                    p_unique_key_name => unique_key_name,
                    p_natural_key_columns => column_names,
                    p_mutually_exclusive_columns => mutually_exclusive_columns,
                    p_stable_pk_columns => ARRAY(SELECT c FROM unnest(pk_column_names) AS c WHERE c <> era_row.range_column_name)
                );
                alter_cmds := alter_cmds || ARRAY(SELECT jsonb_array_elements_text(consistency_result->'cmds'));
                v_pk_consistency_constraint_names := ARRAY(SELECT jsonb_array_elements_text(consistency_result->'names'))::name[];
            END IF;
        END;
    END IF;

    IF alter_cmds <> '{}' THEN

        SELECT format('ALTER TABLE %I.%I %s', n.nspname /* %I */, c.relname /* %I */, array_to_string(alter_cmds, ', ') /* %s */)
        INTO sql
        FROM pg_catalog.pg_class AS c
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
        WHERE c.oid = table_oid;

        PERFORM sql_saga.__internal_execute_as_caller(sql);
        RAISE NOTICE 'sql_saga: Added constraints to table %: %', table_oid, array_to_string(alter_cmds, '; ');
    END IF;

    /* If we don't already have a unique_constraint, it must be the one with the highest oid */
    IF unique_constraint IS NULL AND key_type IN ('primary', 'natural') THEN
        SELECT c.conname
        INTO unique_constraint
        FROM pg_catalog.pg_constraint AS c
        WHERE c.conrelid = table_oid AND c.contype = (CASE WHEN key_type = 'primary' THEN 'p' ELSE 'u' END)
        ORDER BY oid DESC
        LIMIT 1;
    END IF;

    RAISE DEBUG 'add_unique_key: about to insert with v_stable_cols_to_store=%', v_stable_cols_to_store;

    RAISE DEBUG 'add_unique_key: Inserting into sql_saga.unique_keys: name=%, table=%.%, key_type=%, cols=%, era=%, u_constraint=%, ex_constraint=%, check_constraint=%, predicate=%, mut_ex_cols=%, pk_consistency_constraints=%',
        unique_key_name, table_schema, table_name, key_type, v_stable_cols_to_store, era_name, unique_constraint, exclude_constraint, v_check_constraint, predicate, mutually_exclusive_columns, v_pk_consistency_constraint_names;
    INSERT INTO sql_saga.unique_keys (unique_key_name, table_schema, table_name, key_type, column_names, era_name, unique_constraint, exclude_constraint, check_constraint, predicate, mutually_exclusive_columns, pk_consistency_constraint_names)
    VALUES (unique_key_name, table_schema, table_name, key_type, v_stable_cols_to_store, era_name, unique_constraint, exclude_constraint, v_check_constraint, predicate, mutually_exclusive_columns, v_pk_consistency_constraint_names);

    -- Create compound index for efficient exact match queries (needed by temporal_merge)
    -- This index helps UPDATE/DELETE operations that need to find rows by both
    -- identity columns AND exact range match
    IF era_name IS NOT NULL AND array_length(v_stable_cols_to_store, 1) > 0 THEN
        DECLARE
            compound_index_name name;
            compound_index_sql text;
            compound_columns text;
        BEGIN
            -- Build list of columns for the compound index
            compound_columns := (
                SELECT string_agg(quote_ident(c), ', ')
                FROM unnest(v_stable_cols_to_store) AS c
            );

            -- Add the range column at the end
            compound_columns := compound_columns || ', ' || quote_ident(era_row.range_column_name);

            compound_index_name := sql_saga.__internal_make_name(
                ARRAY[table_name] || v_stable_cols_to_store || ARRAY[era_row.range_column_name, 'idx']
            );

            -- Check if index already exists
            IF NOT EXISTS (
                SELECT 1 FROM pg_index i
                JOIN pg_class c ON c.oid = i.indexrelid
                WHERE i.indrelid = table_oid
                AND c.relname = compound_index_name
            ) THEN
                compound_index_sql := format(
                    'CREATE INDEX %I ON %I.%I (%s)',
                    compound_index_name,
                    table_schema,
                    table_name,
                    compound_columns
                );

                RAISE DEBUG 'add_unique_key: Creating compound index for temporal_merge performance: %', compound_index_sql;
                PERFORM sql_saga.__internal_execute_as_caller(compound_index_sql);
                RAISE DEBUG 'sql_saga: created compound index % on % for temporal_merge performance', compound_index_name, table_oid::regclass;
            END IF;
        END;
    END IF;

    -- When adding a primary key, bind every natural key that was waiting for one (state '{}').
    IF key_type = 'primary' THEN
        DECLARE
            nk_row sql_saga.unique_keys;
            consistency_result jsonb;
            consistency_cmds text[] := '{}';
        BEGIN
            FOR nk_row IN
                SELECT * FROM sql_saga.unique_keys uk
                WHERE (uk.table_schema, uk.table_name) = (table_schema, table_name)
                  AND uk.key_type = 'natural' AND uk.pk_consistency_constraint_names = '{}'
                ORDER BY uk.unique_key_name
            LOOP
                consistency_result := sql_saga.__internal_generate_pk_consistency_constraints(
                    p_unique_key_name => nk_row.unique_key_name,
                    p_natural_key_columns => nk_row.column_names,
                    p_mutually_exclusive_columns => nk_row.mutually_exclusive_columns,
                    p_stable_pk_columns => ARRAY(SELECT c FROM unnest(column_names) AS c WHERE c <> era_row.range_column_name)
                );
                consistency_cmds := consistency_cmds || ARRAY(SELECT jsonb_array_elements_text(consistency_result->'cmds'));
                -- Qualified: unique_key_name alone is this function's parameter (#variable_conflict use_variable), the
                -- primary key's name, and until 1.14 left the natural key's catalog row at '{}' with its guard built.
                UPDATE sql_saga.unique_keys AS uk
                SET pk_consistency_constraint_names = ARRAY(SELECT jsonb_array_elements_text(consistency_result->'names'))::name[]
                WHERE uk.unique_key_name = nk_row.unique_key_name;
            END LOOP;

            IF array_length(consistency_cmds, 1) > 0 THEN
                RAISE NOTICE 'sql_saga: Applied retroactive primary key consistency constraints to table %.%: %', table_schema, table_name, array_to_string(consistency_cmds, '; ');
                PERFORM sql_saga.__internal_execute_as_caller(format('ALTER TABLE %I.%I %s', table_schema, table_name, array_to_string(consistency_cmds, ', ')));
            END IF;
        END;
    END IF;

    -- Create a standard B-Tree index on the unique key columns to support
    -- fast lookups for foreign key checks (both temporal and regular).
    -- This is only created if a suitable index doesn't already exist.
    DECLARE
        index_name name;
        index_sql text;
        index_columns text;
        existing_index_oid oid;
    BEGIN
        -- Find an existing B-Tree index on exactly these columns in this order
        SELECT i.indexrelid INTO existing_index_oid
        FROM pg_catalog.pg_index i
        JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
        JOIN pg_catalog.pg_am am ON am.oid = c.relam
        WHERE i.indrelid = table_oid
          AND i.indisvalid
          AND i.indnkeyatts = array_length(column_names, 1)
          AND i.indkey::text = array_to_string(column_attnums, ' ')
          AND am.amname = 'btree'
        LIMIT 1;

        IF existing_index_oid IS NULL THEN
            index_name := sql_saga.__internal_make_name(
                ARRAY[table_name] || column_names || ARRAY['idx']
            );
            SELECT string_agg(quote_ident(c), ', ') INTO index_columns FROM unnest(column_names) AS u(c);
            index_sql := format('CREATE INDEX %I ON %I.%I USING btree (%s)',
                index_name, /* %I */
                table_schema, /* %I */
                table_name, /* %I */
                index_columns /* %s */
            );
            RAISE DEBUG 'add_unique_key: Creating lookup index: %', index_sql;
            PERFORM sql_saga.__internal_execute_as_caller(index_sql);
        END IF;
    END;

    RETURN unique_key_name;
END;
$function$;
CREATE OR REPLACE FUNCTION sql_saga.check_integrity()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = sql_saga, pg_catalog, pg_temp
AS $check_integrity$
DECLARE
    r record;
    base_oid oid;
    child_oid oid;
    child_enabled "char";
    disabled_trigger name;
    history_oid oid;
    view_oid oid;
    expected_columns name[];
    function_oid oid;
    expected_return_type oid;
    missing_column name;
    mispairing record;
    user_policy record;
BEGIN
    /*
     * sql_saga's event triggers come first: a disabled, detached or dropped one (rename_following above all) lets
     * table-level damage through, so it is reported before any damage it may have caused. Name order, COLLATE "C".
     * ENABLE ALWAYS and ENABLE REPLICA are enabled. A missing trigger cannot be attached again, so its repair
     * recreates it with the definition from the extension script (src/2j_event_triggers.sql) and then attaches it.
     * An existing trigger must also have the extension script's shape (event, tag filter as a set, function): a
     * drifted one is detached, dropped, recreated and attached again. This list is the single copy of 2j that
     * check_integrity holds, so every check_integrity on a fresh install compares it with what 2j actually created.
     */
    FOR r IN
        SELECT d.evtname, d.evtevent, d.evttags, d.evtfoid_text, et.oid AS evt_oid, et.evtenabled,
               EXISTS (SELECT 1 FROM pg_catalog.pg_depend AS dep
                       JOIN pg_catalog.pg_extension AS x ON x.oid = dep.refobjid
                       WHERE dep.classid = 'pg_catalog.pg_event_trigger'::regclass AND dep.objid = et.oid
                         AND dep.refclassid = 'pg_catalog.pg_extension'::regclass AND dep.deptype = 'e'
                         AND x.extname = 'sql_saga') AS is_member,
               (et.evtevent IS NOT DISTINCT FROM d.evtevent
                AND et.evtfoid IS NOT DISTINCT FROM pg_catalog.to_regprocedure(d.evtfoid_text || '()')::oid
                AND ARRAY(SELECT t FROM unnest(et.evttags) AS t ORDER BY t COLLATE "C")
                    = ARRAY(SELECT t FROM unnest(d.evttags) AS t ORDER BY t COLLATE "C")) AS has_canonical_shape,
               pg_catalog.format('CREATE EVENT TRIGGER %I ON %s%s EXECUTE FUNCTION %s(); ALTER EXTENSION sql_saga ADD EVENT TRIGGER %I',
                   d.evtname, d.evtevent,
                   CASE WHEN d.evttags IS NULL THEN ''
                        ELSE pg_catalog.format(' WHEN TAG IN (%s)',
                                 (SELECT pg_catalog.string_agg(pg_catalog.quote_literal(t), ', ' ORDER BY o)
                                  FROM unnest(d.evttags) WITH ORDINALITY AS u (t, o))) END,
                   d.evtfoid_text, d.evtname) AS recreate_sql
        FROM (VALUES
            ('sql_saga_drop_protection'::name, 'sql_drop'::name, NULL::text[], 'sql_saga.drop_protection'::text),
            ('sql_saga_health_checks', 'ddl_command_end', NULL, 'sql_saga.health_checks'),
            ('sql_saga_record_alter_function_resolved', 'ddl_command_start', ARRAY['ALTER FUNCTION', 'ALTER ROUTINE'],
             'sql_saga.record_alter_function_resolved'),
            ('sql_saga_refuse_managed_relation_identity_change', 'ddl_command_start', ARRAY['ALTER TABLE', 'ALTER VIEW', 'ALTER SCHEMA'],
             'sql_saga.refuse_managed_relation_identity_change'),
            ('sql_saga_rename_following', 'ddl_command_end', NULL, 'sql_saga.rename_following')
        ) AS d (evtname, evtevent, evttags, evtfoid_text)
        LEFT JOIN pg_catalog.pg_event_trigger AS et ON et.evtname = d.evtname
        ORDER BY d.evtname COLLATE "C"
    LOOP
        IF r.evt_oid IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for event trigger "%": event trigger is missing; repair with %',
                r.evtname, r.recreate_sql;
        END IF;
        IF NOT r.is_member THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for event trigger "%": event trigger is not a member of extension sql_saga; repair with ALTER EXTENSION sql_saga ADD EVENT TRIGGER %',
                r.evtname, pg_catalog.quote_ident(r.evtname);
        END IF;
        IF NOT r.has_canonical_shape THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for event trigger "%": event trigger definition differs from the extension''s; repair with %',
                r.evtname,
                pg_catalog.format('ALTER EXTENSION sql_saga DROP EVENT TRIGGER %1$I; DROP EVENT TRIGGER %1$I; %2$s',
                    r.evtname, r.recreate_sql);
        END IF;
        IF r.evtenabled = 'D' THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for event trigger "%": event trigger is disabled; repair with ALTER EVENT TRIGGER % ENABLE',
                r.evtname, pg_catalog.quote_ident(r.evtname);
        END IF;
    END LOOP;

    /*
     * A managed view dropped at ddl_command_start of an ALTER TABLE column change (DROP COLUMN or ALTER COLUMN
     * ... TYPE) is recreated, and its
     * pending row consumed, at the ddl_command_end of the same statement (src/2o). A row seen here (other
     * sessions' in-flight rows are invisible) was left behind by a statement whose end trigger did not run.
     */
    FOR r IN
        SELECT p.*,
               pg_catalog.format('DELETE FROM sql_saga.__internal_updatable_view_pending WHERE (view_schema, view_name) = (%L, %L)',
                   p.view_schema, p.view_name)
               || CASE WHEN EXISTS (SELECT FROM sql_saga.updatable_view AS uv
                                    WHERE (uv.view_schema, uv.view_name) = (p.view_schema, p.view_name)) THEN ''
                       WHEN p.view_type = 'for_portion_of' THEN
                           pg_catalog.format('; SELECT sql_saga.add_for_portion_of_view(%L, era_name => %L%s)',
                               pg_catalog.format('%I.%I', p.table_schema, p.table_name), p.era_name,
                               CASE WHEN p.ephemeral_columns IS NOT NULL
                                    THEN pg_catalog.format(', ephemeral_columns => %L', p.ephemeral_columns::text)
                                    ELSE '' END)
                       ELSE
                           pg_catalog.format('; SELECT sql_saga.add_current_view(%L, era_name => %L, delete_mode => %L, current_func_name => %L)',
                               pg_catalog.format('%I.%I', p.table_schema, p.table_name), p.era_name,
                               COALESCE(p.view_delete_mode, 'delete_as_cutoff'), p.current_func) END AS repair_sql
        FROM sql_saga.__internal_updatable_view_pending AS p
        ORDER BY p.view_schema COLLATE "C", p.view_name COLLATE "C"
    LOOP
        RAISE EXCEPTION 'sql_saga: integrity violation for updatable view "%.%" of table "%.%": view was dropped by an ALTER TABLE column change and not recreated, because event trigger sql_saga_rename_following did not run; repair with %',
            r.view_schema, r.view_name, r.table_schema, r.table_name, r.repair_sql;
    END LOOP;

    /* Base relation and era invariants are the first dependency layer. */
    FOR r IN
        SELECT e.*,
               pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))::oid AS inventory_table_oid,
               CASE
                   WHEN pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name)) IS NULL THEN 1
                   WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_class AS c
                               WHERE c.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                                 AND (c.relkind <> 'r' OR c.relpersistence <> 'p')) THEN 2
                   WHEN (e.sync_temporal_trg_function_name IS NOT NULL
                         AND e.sync_temporal_trg_function_name <> sql_saga.__internal_make_name(
                             ARRAY[e.table_schema, e.table_name, e.era_name], 'template_sync'))
                     OR EXISTS (SELECT 1 FROM sql_saga.system_time_era AS ste
                                WHERE (ste.table_schema, ste.table_name) = (e.table_schema, e.table_name)
                                  AND NOT EXISTS (SELECT 1 FROM sql_saga.era AS x
                                                  WHERE (x.table_schema, x.table_name, x.era_name) =
                                                        (ste.table_schema, ste.table_name, ste.era_name)))
                     OR EXISTS (SELECT 1 FROM sql_saga.unique_keys AS uk
                                WHERE (uk.table_schema, uk.table_name) = (e.table_schema, e.table_name)
                                  AND NOT EXISTS (SELECT 1 FROM sql_saga.era AS x
                                                  WHERE (x.table_schema, x.table_name, x.era_name) =
                                                        (uk.table_schema, uk.table_name, uk.era_name))) THEN 3
                   WHEN NOT EXISTS (
                       SELECT 1 FROM pg_catalog.pg_attribute AS a
                       JOIN pg_catalog.pg_range AS pr ON pr.rngtypid = e.range_type
                       WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                         AND a.attname = e.range_column_name AND a.attnum > 0 AND NOT a.attisdropped
                         AND a.atttypid = e.range_type AND pr.rngmultitypid = e.multirange_type
                         AND pr.rngsubtype = e.range_subtype
                         AND (SELECT t.typcategory FROM pg_catalog.pg_type AS t WHERE t.oid = e.range_subtype) = e.range_subtype_category) THEN 4
                   WHEN (e.valid_from_column_name IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_attribute AS a
                            WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND a.attname = e.valid_from_column_name AND a.atttypid = e.range_subtype
                              AND a.attnum > 0 AND NOT a.attisdropped))
                     OR (e.valid_until_column_name IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_attribute AS a
                            WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND a.attname = e.valid_until_column_name AND a.atttypid = e.range_subtype
                              AND a.attnum > 0 AND NOT a.attisdropped))
                     OR (e.bounds_check_constraint IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_constraint AS c
                            WHERE c.conrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND c.conname = e.bounds_check_constraint AND c.contype = 'c'
                              AND pg_catalog.pg_get_constraintdef(c.oid, false) LIKE '%NOT isempty(%'))
                     OR (e.boundary_check_constraint IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_constraint AS c
                            WHERE c.conrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND c.conname = e.boundary_check_constraint AND c.contype = 'c'
                              AND pg_catalog.pg_get_constraintdef(c.oid, false) LIKE '% < %'))
                     OR (e.sync_temporal_trg_name IS NOT NULL AND NOT EXISTS (
                            SELECT 1 FROM pg_catalog.pg_trigger AS t
                            WHERE t.tgrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', e.table_schema, e.table_name))
                              AND t.tgname = e.sync_temporal_trg_name AND NOT t.tgisinternal AND t.tgtype = 23
                              AND t.tgfoid = pg_catalog.to_regprocedure(
                                  pg_catalog.format('sql_saga.%I()', e.sync_temporal_trg_function_name)))) THEN 5
               END AS violation
        FROM sql_saga.era AS e
        ORDER BY e.table_schema COLLATE "C", e.table_name COLLATE "C", e.era_name COLLATE "C"
    LOOP
        base_oid := r.inventory_table_oid;
        IF r.violation = 1 THEN
            /*
             * The catalogue names a table that no longer resolves: most likely renamed or moved without sql_saga
             * following (sql_saga 1.0 did not). drop_era cannot even run then; renaming or moving it back is the repair.
             */
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%.%": catalogued base table is missing; if it was renamed or moved, rename or move it back to %',
                r.table_schema, r.table_name, pg_catalog.format('%I.%I', r.table_schema, r.table_name);
        END IF;
        IF r.violation = 2 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": catalogued base relation kind or persistence is noncanonical; repair with drop_era and add_era',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF r.violation = 3 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": catalogue relationships are incoherent; repair with drop_era and add_era',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF r.violation IN (4, 5) THEN
            /*
             * A catalogued era column that no longer exists under its name was most likely renamed without
             * sql_saga following. Renaming it back is the repair, not drop_era and add_era. A column that exists
             * with the wrong type keeps the noncanonical message below.
             */
            SELECT c.column_name INTO missing_column
            FROM unnest(ARRAY[r.range_column_name, r.valid_from_column_name, r.valid_until_column_name])
                 WITH ORDINALITY AS c (column_name, position)
            WHERE c.column_name IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_attribute AS a
                              WHERE a.attrelid = base_oid AND a.attname = c.column_name
                                AND a.attnum > 0 AND NOT a.attisdropped)
            ORDER BY c.position
            LIMIT 1;
            IF missing_column IS NOT NULL THEN
                RAISE EXCEPTION 'sql_saga: integrity violation for era "%" on table "%": column "%" is missing; if it was renamed, rename it back with ALTER TABLE % RENAME COLUMN <current name> TO %',
                    r.era_name, sql_saga.__internal_display_name(base_oid::regclass), missing_column,
                    pg_catalog.format('%I.%I', r.table_schema, r.table_name), pg_catalog.quote_ident(missing_column);
            END IF;
        END IF;
        IF r.violation = 4 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for era "%" on table "%": column or range type metadata is noncanonical; repair with drop_era and add_era',
                r.era_name, sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF r.violation = 5 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for era "%" on table "%": named check or synchronization object is noncanonical; repair with drop_era and add_era',
                r.era_name, sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
    END LOOP;

    /* The era synchronization trigger is enabled (1.14, hug a5sdk3): disabled, valid_from and valid_until stop following
     * the range column. DISABLE TRIGGER stays allowed (it is not a drop, doc/design/drop-protection.md). ENABLE ALWAYS
     * and ENABLE REPLICA are a site's choice for replication (REPLICA fires only in replica sessions), as for the
     * foreign key and provenance triggers below. Its existence and shape are checked above. */
    FOR r IN
        SELECT e.table_schema, e.table_name, e.era_name, e.sync_temporal_trg_name
        FROM sql_saga.era AS e
        JOIN pg_catalog.pg_namespace AS n ON n.nspname = e.table_schema
        JOIN pg_catalog.pg_class AS c ON (c.relnamespace, c.relname) = (n.oid, e.table_name)
        JOIN pg_catalog.pg_trigger AS t ON (t.tgrelid, t.tgname) = (c.oid, e.sync_temporal_trg_name)
        WHERE t.tgenabled = 'D'
        ORDER BY e.table_schema COLLATE "C", e.table_name COLLATE "C", e.era_name COLLATE "C"
    LOOP
        RAISE EXCEPTION 'sql_saga: integrity violation for era "%" on table "%": synchronization trigger % is disabled; repair with ALTER TABLE % ENABLE TRIGGER %',
            r.era_name, pg_catalog.format('%I.%I', r.table_schema, r.table_name), pg_catalog.quote_ident(r.sync_temporal_trg_name),
            pg_catalog.format('%I.%I', r.table_schema, r.table_name), pg_catalog.quote_ident(r.sync_temporal_trg_name);
    END LOOP;

    /* Provenance columns (1.9, doc/design/provenance.md): the managed trigger exists and is enabled. */
    FOR r IN
        SELECT e.table_schema, e.table_name, e.era_name, t.tgenabled,
               pg_catalog.format('%s_provenance', e.era_name) AS trigger_name
        FROM sql_saga.era AS e
        JOIN pg_catalog.pg_namespace AS n ON n.nspname = e.table_schema
        JOIN pg_catalog.pg_class AS c ON (c.relnamespace, c.relname) = (n.oid, e.table_name)
        LEFT JOIN pg_catalog.pg_trigger AS t
               ON (t.tgrelid, t.tgname) = (c.oid, pg_catalog.format('%s_provenance', e.era_name))
              AND t.tgfoid = 'sql_saga.provenance_trigger()'::pg_catalog.regprocedure
        WHERE e.provenance_columns IS NOT NULL AND (t.oid IS NULL OR t.tgenabled = 'D')
        ORDER BY e.table_schema, e.table_name, e.era_name
    LOOP
        IF r.tgenabled IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for era "%" on table "%": provenance trigger % is missing; repair with set_era_provenance_columns',
                r.era_name, pg_catalog.format('%I.%I', r.table_schema, r.table_name), pg_catalog.quote_ident(r.trigger_name);
        END IF;
        RAISE EXCEPTION 'sql_saga: integrity violation for era "%" on table "%": provenance trigger % is disabled; repair with ALTER TABLE % ENABLE TRIGGER %',
            r.era_name, pg_catalog.format('%I.%I', r.table_schema, r.table_name), pg_catalog.quote_ident(r.trigger_name),
            pg_catalog.format('%I.%I', r.table_schema, r.table_name), pg_catalog.quote_ident(r.trigger_name);
    END LOOP;

    /* Each provenance column keeps the DEFAULT it was registered with; without one every write is refused. */
    FOR r IN
        SELECT e.table_schema, e.table_name, e.era_name, p.col
        FROM sql_saga.era AS e
        CROSS JOIN LATERAL pg_catalog.unnest(e.provenance_columns) AS p(col)
        JOIN pg_catalog.pg_namespace AS n ON n.nspname = e.table_schema
        JOIN pg_catalog.pg_class AS c ON (c.relnamespace, c.relname) = (n.oid, e.table_name)
        JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (c.oid, p.col) AND NOT a.attisdropped
        WHERE NOT a.atthasdef OR a.attgenerated <> ''
        ORDER BY e.table_schema, e.table_name, e.era_name, p.col
    LOOP
        RAISE EXCEPTION 'sql_saga: integrity violation for era "%" on table "%": provenance column % has no DEFAULT; repair with ALTER TABLE % ALTER COLUMN % SET DEFAULT ..., or set_era_provenance_columns',
            r.era_name, pg_catalog.format('%I.%I', r.table_schema, r.table_name), pg_catalog.quote_ident(r.col),
            pg_catalog.format('%I.%I', r.table_schema, r.table_name), pg_catalog.quote_ident(r.col);
    END LOOP;

    /* SYSTEM_TIME invariants have the highest managed-object priority. */
    FOR r IN
        SELECT ste.*, e.range_column_name
        FROM sql_saga.system_time_era AS ste
        JOIN sql_saga.era AS e USING (table_schema, table_name, era_name)
        ORDER BY ste.table_schema COLLATE "C", ste.table_name COLLATE "C"
    LOOP
        base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
        IF base_oid IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%.%": catalogued base table is missing', r.table_schema, r.table_name;
        END IF;

        IF NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.generated_always_trigger)
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.write_history_trigger)
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.truncate_trigger) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": SYSTEM_TIME trigger is missing; repair with drop_system_versioning and add_system_versioning',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;

        IF NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_constraint AS c
                WHERE c.conrelid = base_oid AND c.conname = r.infinity_check_constraint
                  AND c.contype = 'c'
                  AND pg_catalog.pg_get_constraintdef(c.oid, false) LIKE '%upper(%''infinity''%')
           OR NOT sql_saga.__internal_trigger_is_attributable(
                base_oid, r.generated_always_trigger,
                'sql_saga.generated_always_as_row_start_end()'::regprocedure, 23, r.range_column_name)
           OR NOT sql_saga.__internal_trigger_is_attributable(
                base_oid, r.write_history_trigger,
                'sql_saga.write_history()'::regprocedure, 25, r.range_column_name)
           OR NOT sql_saga.__internal_trigger_is_attributable(
                base_oid, r.truncate_trigger,
                'sql_saga.truncate_system_versioning()'::regprocedure, 32, NULL) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": SYSTEM_TIME trigger definition is noncanonical; repair with drop_system_versioning and add_system_versioning',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;

        SELECT x.attname INTO missing_column
        FROM pg_catalog.unnest(r.excluded_column_names) AS x(attname)
        WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = base_oid AND a.attname = x.attname AND a.attnum > 0 AND NOT a.attisdropped)
        ORDER BY x.attname COLLATE "C" LIMIT 1;
        IF FOUND THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": excluded SYSTEM_TIME column "%" is missing; repair with set_system_time_era_excluded_columns or drop_system_versioning',
                sql_saga.__internal_display_name(base_oid::regclass), missing_column;
        END IF;

        /* The snapshot detects added and removed columns only; compare it as a set, because a
         * restore recreates inherited children with inherited columns first. */
        SELECT pg_catalog.array_agg(a.attname ORDER BY a.attname COLLATE "C") INTO expected_columns
        FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = base_oid AND a.attnum > 0 AND NOT a.attisdropped;
        IF expected_columns IS DISTINCT FROM (
                SELECT pg_catalog.array_agg(s.attname ORDER BY s.attname COLLATE "C")
                FROM pg_catalog.unnest(r.table_columns_snapshot) AS s(attname)) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for table "%": SYSTEM_TIME column snapshot is stale; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
    END LOOP;

    FOR r IN
        SELECT uk.*, i.table_oid, i.object_missing,
               i.unique_attributable, i.exclude_attributable, i.check_attributable
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.__internal_unique_key_inventory() AS i USING (unique_key_name)
        ORDER BY uk.table_schema COLLATE "C", uk.table_name COLLATE "C", uk.unique_key_name COLLATE "C"
    LOOP
        base_oid := r.table_oid;
        IF r.object_missing THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for unique key "%" on table "%": managed constraint or index is missing; repair with drop_unique_key and add_unique_key',
                r.unique_key_name, sql_saga.__internal_display_name(base_oid::regclass);
        END IF;

        IF NOT r.unique_attributable OR NOT r.exclude_attributable OR NOT r.check_attributable THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for unique key "%" on table "%": managed constraint or index is noncanonical; repair with drop_unique_key and add_unique_key',
                r.unique_key_name, sql_saga.__internal_display_name(base_oid::regclass);
        END IF;

        /* 1.14 (hug mnntde): a natural key on a table with a primary key is bound to it by exactly the guard
         * add_unique_key builds now (none when the natural key holds every stable primary key column). */
        IF r.key_type = 'natural' THEN
            DECLARE
                pk_row sql_saga.unique_keys;
                range_column name;
                expected jsonb;
            BEGIN
                SELECT uk.* INTO pk_row FROM sql_saga.unique_keys AS uk
                WHERE (uk.table_schema, uk.table_name, uk.key_type) = (r.table_schema, r.table_name, 'primary');
                IF FOUND THEN
                    SELECT e.range_column_name INTO range_column FROM sql_saga.era AS e
                    WHERE (e.table_schema, e.table_name, e.era_name) = (r.table_schema, r.table_name, r.era_name);
                    expected := sql_saga.__internal_generate_pk_consistency_constraints(
                        r.unique_key_name, r.column_names, r.mutually_exclusive_columns,
                        ARRAY(SELECT c FROM pg_catalog.unnest(pk_row.column_names) AS c WHERE c <> range_column));
                    /* Count and definitions, not names: a renamed guard is followed (rename_following). */
                    IF r.pk_consistency_constraint_names IS NULL
                       OR pg_catalog.cardinality(r.pk_consistency_constraint_names) <> pg_catalog.jsonb_array_length(expected->'cmds')
                       OR EXISTS (
                            SELECT FROM pg_catalog.jsonb_array_elements_text(expected->'cmds') WITH ORDINALITY AS x(cmd, n)
                            JOIN pg_catalog.jsonb_array_elements_text(expected->'names') WITH ORDINALITY AS en(conname, n) USING (n)
                            JOIN pg_catalog.unnest(r.pk_consistency_constraint_names) WITH ORDINALITY AS g(conname, n) USING (n)
                            LEFT JOIN pg_catalog.pg_constraint AS c ON (c.conrelid, c.conname, c.contype) = (base_oid, g.conname, 'x')
                            WHERE c.oid IS NULL
                               OR x.cmd <> pg_catalog.format('ADD CONSTRAINT %I %s', en.conname, pg_catalog.pg_get_constraintdef(c.oid))) THEN
                        RAISE EXCEPTION 'sql_saga: integrity violation for unique key "%" on table "%": natural key is not bound to the primary key; repair with drop_unique_key and add_unique_key',
                            r.unique_key_name, sql_saga.__internal_display_name(base_oid::regclass);
                    END IF;
                END IF;
            END;
        END IF;
    END LOOP;

    FOR r IN
        SELECT fk.*, uk.table_schema AS uk_schema, uk.table_name AS uk_table
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        ORDER BY fk.table_schema COLLATE "C", fk.table_name COLLATE "C", fk.foreign_key_name COLLATE "C"
    LOOP
        /*
         * A foreign key whose objects pair child columns with other referenced columns than the unique key's
         * constraint order (built by sql_saga 1.0/1.1 against a sorted XOR key catalog) is reported as such, with
         * the repair, before the generic attribution checks (which it also fails).
         */
        SELECT * INTO mispairing FROM sql_saga.__internal_foreign_key_mispairing_report(r.foreign_key_name);
        IF mispairing.message IS NOT NULL THEN
            RAISE EXCEPTION USING MESSAGE = mispairing.message, HINT = mispairing.hint;
        END IF;

        IF r.type = 'temporal_to_temporal' THEN
            base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
            IF base_oid IS NULL
               OR NOT sql_saga.__internal_temporal_foreign_key_is_attributable(r.foreign_key_name) THEN
                RAISE EXCEPTION 'sql_saga: integrity violation for foreign key on table "%.%": managed constraint or trigger is noncanonical; repair with drop_foreign_key and add_foreign_key',
                    r.table_schema, r.table_name;
            END IF;
        ELSE
            base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.uk_schema, r.uk_table));
            child_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
            /*
             * 1.3: the referencing side is a constraint trigger named fk_check_constraint. A pre-1.3 row
             * (helper function, CHECK constraint) is diagnosed by the historical checks below.
             */
            IF r.fk_helper_function IS NULL AND child_oid IS NOT NULL THEN
                SELECT t.tgenabled INTO child_enabled FROM pg_catalog.pg_trigger AS t
                WHERE t.tgrelid = child_oid AND t.tgname = r.fk_check_constraint;
                IF NOT FOUND THEN
                    RAISE EXCEPTION 'sql_saga: integrity violation for foreign key %: referencing-side trigger % on table % is missing; repair with %',
                        pg_catalog.quote_ident(r.foreign_key_name), pg_catalog.quote_ident(r.fk_check_constraint),
                        pg_catalog.format('%I.%I', r.table_schema, r.table_name),
                        sql_saga.__internal_foreign_key_repair_sql(r.foreign_key_name);
                END IF;
                IF child_enabled = 'D' THEN
                    RAISE EXCEPTION 'sql_saga: integrity violation for foreign key %: referencing-side trigger % on table % is disabled; repair with ALTER TABLE % ENABLE TRIGGER %',
                        pg_catalog.quote_ident(r.foreign_key_name), pg_catalog.quote_ident(r.fk_check_constraint),
                        pg_catalog.format('%I.%I', r.table_schema, r.table_name),
                        pg_catalog.format('%I.%I', r.table_schema, r.table_name), pg_catalog.quote_ident(r.fk_check_constraint);
                END IF;
            END IF;
            /* Referenced-side triggers disabled: same style. */
            IF base_oid IS NOT NULL THEN
                SELECT t.tgname INTO disabled_trigger FROM pg_catalog.pg_trigger AS t
                WHERE t.tgrelid = base_oid AND t.tgname IN (r.uk_update_trigger, r.uk_delete_trigger, r.uk_truncate_trigger) AND t.tgenabled = 'D'
                ORDER BY t.tgname = r.uk_delete_trigger, t.tgname COLLATE "C" LIMIT 1;
                IF FOUND THEN
                    RAISE EXCEPTION 'sql_saga: integrity violation for foreign key %: referenced-side trigger % on table % is disabled; repair with ALTER TABLE % ENABLE TRIGGER %',
                        pg_catalog.quote_ident(r.foreign_key_name), pg_catalog.quote_ident(disabled_trigger),
                        pg_catalog.format('%I.%I', r.uk_schema, r.uk_table),
                        pg_catalog.format('%I.%I', r.uk_schema, r.uk_table), pg_catalog.quote_ident(disabled_trigger);
                END IF;
            END IF;
            IF base_oid IS NULL
               OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.uk_update_trigger)
               OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.uk_delete_trigger)
               OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND t.tgname = r.uk_truncate_trigger) THEN
                RAISE EXCEPTION 'sql_saga: integrity violation for foreign key on table "%.%": referenced-side helper trigger is missing; repair with drop_foreign_key and add_regular_foreign_key',
                    r.table_schema, r.table_name;
            END IF;
            IF NOT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_trigger AS t
                    WHERE t.tgrelid = base_oid AND t.tgname = r.uk_update_trigger
                      AND t.tgfoid = 'sql_saga.uk_update_check_c()'::regprocedure)
               OR NOT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_trigger AS t
                    WHERE t.tgrelid = base_oid AND t.tgname = r.uk_delete_trigger
                      AND t.tgfoid = 'sql_saga.uk_delete_check_c()'::regprocedure)
               OR NOT EXISTS (
                    /* AFTER TRUNCATE FOR EACH STATEMENT (tgtype 32), the canonical arguments, no WHEN (1.4). */
                    SELECT 1 FROM pg_catalog.pg_trigger AS t
                    WHERE t.tgrelid = base_oid AND t.tgname = r.uk_truncate_trigger
                      AND t.tgfoid = 'sql_saga.uk_truncate_check_c()'::regprocedure
                      AND t.tgtype = 32 AND t.tgqual IS NULL AND NOT t.tgisinternal
                      AND t.tgargs = sql_saga.__internal_regular_fk_trigger_args(r.foreign_key_name))
               OR (r.fk_helper_function IS NOT NULL AND NOT EXISTS (
                    SELECT 1 FROM pg_catalog.pg_constraint AS c
                    WHERE c.conrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name))
                      AND c.conname = r.fk_check_constraint AND c.contype = 'c'))
               OR NOT sql_saga.__internal_regular_foreign_key_is_attributable(r.foreign_key_name) THEN
                RAISE EXCEPTION 'sql_saga: integrity violation for foreign key on table "%.%": managed constraint or trigger is noncanonical; repair with drop_foreign_key and add_foreign_key',
                    r.table_schema, r.table_name;
            END IF;
        END IF;
    END LOOP;

    FOR r IN
        SELECT v.* FROM sql_saga.updatable_view AS v
        ORDER BY v.table_schema COLLATE "C", v.table_name COLLATE "C", v.view_type, v.view_schema COLLATE "C", v.view_name COLLATE "C"
    LOOP
        view_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.view_schema, r.view_name));
        IF view_oid IS NULL
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = view_oid AND t.tgname = r.trigger_name) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for updatable view "%.%" of table "%.%": identifier column or trigger is missing; repair with drop_%_view and add_%_view',
                r.view_schema, r.view_name, r.table_schema, r.table_name,
                CASE r.view_type WHEN 'for_portion_of' THEN 'for_portion_of' ELSE 'current' END,
                CASE r.view_type WHEN 'for_portion_of' THEN 'for_portion_of' ELSE 'current' END;
        END IF;
        IF EXISTS (
                SELECT 1
                FROM pg_catalog.pg_attribute AS a
                WHERE a.attrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name))
                  AND a.attnum > 0 AND NOT a.attisdropped
                  AND NOT EXISTS (
                      SELECT 1 FROM pg_catalog.pg_attribute AS va
                      WHERE va.attrelid = view_oid AND va.attname = a.attname
                        AND va.atttypid = a.atttypid AND va.attnum > 0 AND NOT va.attisdropped))
           OR NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_trigger AS t
                WHERE t.tgrelid = view_oid AND t.tgname = r.trigger_name
                  AND NOT t.tgisinternal AND t.tgtype = 93
                  AND t.tgfoid = CASE r.view_type
                      WHEN 'for_portion_of' THEN 'sql_saga.for_portion_of_trigger()'::regprocedure
                      ELSE 'sql_saga.current_view_trigger()'::regprocedure END)
           -- Each updatable view applies its queued changes with exactly one statement trigger (1.6, current 1.7).
           OR (
                SELECT pg_catalog.count(*) FROM pg_catalog.pg_trigger AS t
                WHERE t.tgrelid = view_oid AND NOT t.tgisinternal AND t.tgqual IS NULL AND t.tgnargs = 0
                  AND t.tgtype = CASE r.view_type WHEN 'for_portion_of' THEN 16 ELSE 4 END
                  AND t.tgfoid = CASE r.view_type
                      WHEN 'for_portion_of' THEN 'sql_saga.for_portion_of_apply_trigger()'::regprocedure
                      ELSE 'sql_saga.current_view_apply_trigger()'::regprocedure END) <> 1 THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for updatable view "%.%" of table "%.%": identifier column or trigger definition is noncanonical; repair with drop_%_view and add_%_view',
                r.view_schema, r.view_name, r.table_schema, r.table_name,
                CASE r.view_type WHEN 'for_portion_of' THEN 'for_portion_of' ELSE 'current' END,
                CASE r.view_type WHEN 'for_portion_of' THEN 'for_portion_of' ELSE 'current' END;
        END IF;
    END LOOP;

    FOR r IN
        SELECT sv.*, a.public_view_attributable, a.gated_view_attributable
        FROM sql_saga.system_versioning AS sv
        CROSS JOIN LATERAL sql_saga.__internal_system_versioning_member_attribution(
            pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name))::oid) AS a
        ORDER BY sv.table_schema COLLATE "C", sv.table_name COLLATE "C"
    LOOP
        base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
        history_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.history_schema_name, r.history_table_name));
        view_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.view_schema_name, r.view_table_name));
        IF view_oid IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history view "%.%" is missing; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass), r.view_schema_name, r.view_table_name;
        END IF;
        IF history_oid IS NULL OR NOT EXISTS (
                SELECT 1 FROM pg_catalog.pg_class AS h
                WHERE h.oid = history_oid AND h.relkind = 'r') THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history family definition is noncanonical; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_class AS h WHERE h.oid = history_oid AND h.relpersistence <> 'p') THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history table "%" is not persistent; repair with ALTER TABLE % SET LOGGED',
                sql_saga.__internal_display_name(base_oid::regclass), sql_saga.__internal_display_name(history_oid::regclass),
                history_oid::regclass;
        END IF;
        IF EXISTS (
                SELECT 1 FROM pg_catalog.pg_class AS b JOIN pg_catalog.pg_class AS h ON h.oid = history_oid
                WHERE b.oid = base_oid AND (NOT h.relrowsecurity OR h.relforcerowsecurity <> b.relforcerowsecurity)) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history family definition is noncanonical; repair with sync_history_policies or rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF EXISTS (
                SELECT 1 FROM pg_catalog.pg_class AS b JOIN pg_catalog.pg_class AS h ON h.oid = history_oid
                WHERE b.oid = base_oid AND h.relowner <> b.relowner) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history family definition is noncanonical; repair with drop_system_versioning and add_system_versioning',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        IF EXISTS (
                SELECT 1 FROM pg_catalog.pg_class AS h
                WHERE h.oid = history_oid AND (
                    EXISTS (SELECT 1 FROM pg_catalog.aclexplode(coalesce(h.relacl, pg_catalog.acldefault('r', h.relowner))) AS x
                            WHERE x.grantee NOT IN (0, h.relowner) OR (x.grantee = 0 AND x.privilege_type <> 'SELECT'))
                    OR NOT EXISTS (SELECT 1 FROM pg_catalog.aclexplode(coalesce(h.relacl, pg_catalog.acldefault('r', h.relowner))) AS x
                                   WHERE x.grantee = 0 AND x.privilege_type = 'SELECT')))
           OR NOT r.public_view_attributable
           OR NOT r.gated_view_attributable THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": history family definition is noncanonical; repair with rebuild_system_versioning_views',
                sql_saga.__internal_display_name(base_oid::regclass);
        END IF;
        /*
         * A policy of the user's own on the history table: sync_history_policies refuses to remove it, so the
         * generic repair below cannot work. Name it and give the manual repair. Never touch the user's policy.
         * The first one in name order (COLLATE "C") is reported.
         */
        IF history_oid IS NOT NULL THEN
            SELECT p.polname, bn.nspname AS base_schema, b.relname AS base_name,
                   hn.nspname AS history_schema, h.relname AS history_name
              INTO user_policy
              FROM pg_catalog.pg_policy AS p
              JOIN pg_catalog.pg_class AS h ON h.oid = p.polrelid
              JOIN pg_catalog.pg_namespace AS hn ON hn.oid = h.relnamespace
              JOIN pg_catalog.pg_class AS b ON b.oid = base_oid
              JOIN pg_catalog.pg_namespace AS bn ON bn.oid = b.relnamespace
             WHERE p.polrelid = history_oid
               AND NOT sql_saga.__internal_is_managed_history_policy(p.polname)
             ORDER BY p.polname COLLATE "C"
             LIMIT 1;
            IF FOUND THEN
                RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table %: history table % has policy % not managed by sql_saga',
                    pg_catalog.format('%I.%I', user_policy.base_schema, user_policy.base_name),
                    pg_catalog.format('%I.%I', user_policy.history_schema, user_policy.history_name),
                    pg_catalog.quote_ident(user_policy.polname)
                USING HINT = pg_catalog.format(
                    'Review the policy definition, then run DROP POLICY %1$I ON %2$I.%3$I; if it is still needed, create it on %4$I.%5$I; then run SELECT sql_saga.sync_history_policies(%6$L);',
                    user_policy.polname, user_policy.history_schema, user_policy.history_name,
                    user_policy.base_schema, user_policy.base_name,
                    pg_catalog.format('%I.%I', user_policy.base_schema, user_policy.base_name));
            END IF;
        END IF;
        BEGIN
            IF history_oid IS NULL OR NOT sql_saga.__internal_history_policies_synchronized(base_oid, history_oid) THEN
                RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'policy mismatch';
            END IF;
        EXCEPTION WHEN OTHERS THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%": managed history policy set is inconsistent; repair with sync_history_policies',
                sql_saga.__internal_display_name(base_oid::regclass);
        END;
    END LOOP;

    FOR r IN
        SELECT sv.table_schema, sv.table_name, sv.view_schema_name, sv.view_table_name, q.*,
               CASE q.role
                   WHEN 'as_of' THEN a.as_of_attributable
                   WHEN 'between' THEN a.between_attributable
                   WHEN 'between_symmetric' THEN a.between_symmetric_attributable
                   ELSE a.from_to_attributable
               END AS definition_attributable
        FROM sql_saga.system_versioning AS sv
        CROSS JOIN LATERAL sql_saga.__internal_system_versioning_member_attribution(
            pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name))::oid) AS a
        CROSS JOIN LATERAL sql_saga.__internal_system_versioning_query_functions(
            pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.table_schema, sv.table_name))::oid) AS q
        ORDER BY sv.table_schema COLLATE "C", sv.table_name COLLATE "C",
                 CASE q.role WHEN 'as_of' THEN 1 WHEN 'between' THEN 2 WHEN 'between_symmetric' THEN 3 ELSE 4 END
    LOOP
        base_oid := pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.table_schema, r.table_name));
        function_oid := pg_catalog.to_regprocedure(r.identity);
        IF function_oid IS NULL THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%.%": generated query function role "%" is missing; repair with rebuild_system_versioning_views',
                r.table_schema, r.table_name, r.role;
        END IF;
        SELECT c.reltype INTO expected_return_type FROM pg_catalog.pg_class AS c
        WHERE c.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', r.view_schema_name, r.view_table_name));
        IF (SELECT p.prorettype FROM pg_catalog.pg_proc AS p WHERE p.oid = function_oid) IS DISTINCT FROM expected_return_type
           OR 1 <> (
                SELECT pg_catalog.count(*)
                FROM pg_catalog.pg_depend AS d
                WHERE d.classid = 'pg_catalog.pg_proc'::regclass
                  AND d.objid = function_oid
                  AND d.refclassid = 'pg_catalog.pg_type'::regclass
                  AND d.deptype = 'n')
           OR NOT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_depend AS d
                JOIN pg_catalog.pg_type AS t ON t.oid = d.refobjid
                WHERE d.classid = 'pg_catalog.pg_proc'::regclass
                  AND d.objid = function_oid
                  AND d.refclassid = 'pg_catalog.pg_type'::regclass
                  AND d.refobjid = expected_return_type
                  AND d.deptype = 'n'
                  AND t.typrelid = pg_catalog.to_regclass(
                      pg_catalog.format('%I.%I', r.view_schema_name, r.view_table_name))) THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%.%": generated query function role "%" has a noncanonical return-type dependency; repair with rebuild_system_versioning_views',
                r.table_schema, r.table_name, r.role;
        END IF;
        IF NOT r.definition_attributable THEN
            RAISE EXCEPTION 'sql_saga: integrity violation for system-versioned table "%.%": generated query function role "%" definition is noncanonical; repair with rebuild_system_versioning_views',
                r.table_schema, r.table_name, r.role;
        END IF;
    END LOOP;
END;
$check_integrity$;
CREATE OR REPLACE FUNCTION sql_saga.add_unique_key(
    table_oid regclass,
    column_names name[],
    era_name name DEFAULT 'valid'::name,
    key_type sql_saga.unique_key_type DEFAULT 'natural'::sql_saga.unique_key_type,
    unique_key_name name DEFAULT NULL::name,
    unique_constraint name DEFAULT NULL::name,
    exclude_constraint name DEFAULT NULL::name,
    predicate text DEFAULT NULL::text,
    mutually_exclusive_columns name[] DEFAULT NULL::name[])
RETURNS name
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';
COMMENT ON FUNCTION sql_saga.__api_add_unique_key(regclass, name[], name, sql_saga.unique_key_type, name, name, name, text, name[]) IS
'Adds a temporal unique key to a table, ensuring uniqueness across time for a given set of columns within an era. Supports primary, natural, and predicated keys. A natural key is always bound to one primary key value, also when that key is composite.';
REVOKE ALL ON FUNCTION sql_saga.__api_add_unique_key(regclass, name[], name, sql_saga.unique_key_type, name, name, name, text, name[]) FROM PUBLIC;
DO $copy_api_comments$
DECLARE
    w record;
BEGIN
    FOR w IN
        SELECT pub.oid::pg_catalog.regprocedure AS public_routine, pub.prokind, d.description
        FROM pg_catalog.pg_proc AS wk
        JOIN pg_catalog.pg_description AS d
          ON d.objoid = wk.oid AND d.classoid = 'pg_catalog.pg_proc'::pg_catalog.regclass AND d.objsubid = 0
        JOIN pg_catalog.pg_proc AS pub
          ON pub.pronamespace = wk.pronamespace AND pub.proname = pg_catalog.substr(wk.proname, 7)
         AND pub.proargtypes = wk.proargtypes
        WHERE wk.pronamespace = 'sql_saga'::pg_catalog.regnamespace AND wk.proname LIKE E'\\_\\_api\\_%'
    LOOP
        EXECUTE pg_catalog.format('COMMENT ON %s %s IS %L',
            CASE WHEN w.prokind = 'p' THEN 'PROCEDURE' ELSE 'FUNCTION' END, w.public_routine, w.description);
    END LOOP;
END;
$copy_api_comments$;
/*
 * The hand-written part of the current release's update step (devops/update_step.py): catalog DDL and data repairs
 * that need the new definitions. It runs after every new and changed routine is in place, and the step's integrity
 * gate runs after it; a part between the lines "-- before routines" and "-- after routines" runs before the routines
 * (a type or table a new signature uses). Name each install-script change it makes in a line "-- handles: <key>"
 * (make prints the keys). Emptied after each release.
 */

/*
 * 1.14: every natural key is bound to its table's primary key (hug mnntde). Natural keys that an earlier release left
 * unbound (an explicit enforce_consistency_with_primary_key => false, or a composite primary key) get the guard a fresh
 * 1.14 add_unique_key builds. Rows that break it refuse the update, listed with the repair; no row changes.
 * Through 1.13, a natural key added before its primary key got its guard but kept the catalog value '{}' (the update
 * of the catalog row matched the primary key's name); such a guard, exactly as add_unique_key builds it, is recorded
 * instead of built again.
 */
DO $bind_natural_keys$
DECLARE
    k record;
    result jsonb;
    cmds text[];
    g record;
    existing text;
    conflict text;
    n integer := 0;
BEGIN
    FOR k IN
        SELECT nk.unique_key_name, nk.table_schema, nk.table_name, nk.column_names, nk.mutually_exclusive_columns,
               nk.pk_consistency_constraint_names,
               pg_catalog.format('%I.%I', nk.table_schema, nk.table_name)::pg_catalog.regclass AS table_oid,
               ARRAY(SELECT c FROM pg_catalog.unnest(pk.column_names) AS c WHERE c <> e.range_column_name) AS stable_pk
        FROM sql_saga.unique_keys AS nk
        JOIN sql_saga.unique_keys AS pk ON (pk.table_schema, pk.table_name, pk.key_type) = (nk.table_schema, nk.table_name, 'primary')
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (nk.table_schema, nk.table_name, nk.era_name)
        WHERE nk.key_type = 'natural'
          AND (nk.pk_consistency_constraint_names IS NULL OR nk.pk_consistency_constraint_names = '{}')
        ORDER BY nk.table_schema, nk.table_name, nk.unique_key_name
    LOOP
        result := sql_saga.__internal_generate_pk_consistency_constraints(
            k.unique_key_name, k.column_names, k.mutually_exclusive_columns, k.stable_pk);
        cmds := '{}';
        FOR g IN
            SELECT x.cmd, en.conname
            FROM pg_catalog.jsonb_array_elements_text(result->'cmds') WITH ORDINALITY AS x(cmd, n)
            JOIN pg_catalog.jsonb_array_elements_text(result->'names') WITH ORDINALITY AS en(conname, n) USING (n)
        LOOP
            SELECT pg_catalog.format('ADD CONSTRAINT %I %s', c.conname, pg_catalog.pg_get_constraintdef(c.oid)) INTO existing
            FROM pg_catalog.pg_constraint AS c WHERE (c.conrelid, c.conname) = (k.table_oid, g.conname);
            IF existing IS NULL THEN
                cmds := cmds || g.cmd;
            ELSIF existing <> g.cmd THEN
                RAISE EXCEPTION 'sql_saga: constraint % on table %.% is not the guard that binds natural key % to its primary key',
                    pg_catalog.quote_ident(g.conname), pg_catalog.quote_ident(k.table_schema), pg_catalog.quote_ident(k.table_name),
                    pg_catalog.quote_ident(k.unique_key_name)
                    USING ERRCODE = 'duplicate_object', DETAIL = pg_catalog.format('Found: %s. Expected: %s.', existing, g.cmd),
                          HINT = 'The update needs that name for the guard. Rename or drop that constraint, then retry.';
            END IF;
        END LOOP;
        IF pg_catalog.cardinality(cmds) > 0 THEN
            BEGIN
                EXECUTE pg_catalog.format('ALTER TABLE %I.%I %s', k.table_schema, k.table_name, pg_catalog.array_to_string(cmds, ', '));
            EXCEPTION WHEN exclusion_violation THEN
                -- PostgreSQL's DETAIL names the two rows: one natural key value under two primary key values.
                GET STACKED DIAGNOSTICS conflict = PG_EXCEPTION_DETAIL;
                RAISE EXCEPTION 'sql_saga: natural key % on table %.% is not bound to its primary key: a natural key value belongs to more than one primary key value',
                    pg_catalog.quote_ident(k.unique_key_name), pg_catalog.quote_ident(k.table_schema), pg_catalog.quote_ident(k.table_name)
                    USING ERRCODE = 'exclusion_violation', DETAIL = conflict,
                          HINT = 'sql_saga 1.14 binds every natural key value to one primary key value. Before updating, correct the rows so that each natural key value belongs to one entity, then retry.';
            END;
            n := n + 1;
        END IF;
        UPDATE sql_saga.unique_keys AS uk
        SET pk_consistency_constraint_names = ARRAY(SELECT pg_catalog.jsonb_array_elements_text(result->'names'))::name[]
        WHERE uk.unique_key_name = k.unique_key_name;
    END LOOP;
    IF n > 0 THEN
        RAISE NOTICE 'sql_saga: Bound % natural keys to their primary keys', n;
    END IF;
END;
$bind_natural_keys$;

/*
 * 1.14: jsonb provenance values that 1.9 stored as a JSON string become the object they encode (hug k5zndr).
 * 1.9 evaluated each provenance DEFAULT as text, so a jsonb provenance column got "{\"by\": ...}" instead of
 * {"by": ...}. A provenance value comes only from the column's DEFAULT (the trigger overwrites a supplied one), so a
 * JSON string that parses to an object can only be that defect; anything else is left as it is. The rows of the
 * history table of a system-versioned table are repaired too (write_history copied the value).
 * The repair is a storage correction, not a change of data: no trigger of the table fires (the provenance trigger
 * would put the old value back, write_history would add a history row, generated_always would move system time).
 * Triggers are disabled only on a table that has such a value, and each gets its exact firing mode back.
 */
DO $repair_jsonb_provenance$
DECLARE
    rel record;
    trg record;
    fixable text;
    sets text;
    found_one boolean;
    n bigint;
    total bigint := 0;
    modes jsonb;
BEGIN
    FOR rel IN
        WITH prov AS (
            SELECT DISTINCT e.table_schema, e.table_name, p.col
            FROM sql_saga.era AS e
            CROSS JOIN LATERAL pg_catalog.unnest(e.provenance_columns) AS p(col)
        ), targets AS (
            SELECT c.oid AS rel_oid, pr.col FROM prov AS pr
            JOIN pg_catalog.pg_namespace AS ns ON ns.nspname = pr.table_schema
            JOIN pg_catalog.pg_class AS c ON (c.relnamespace, c.relname) = (ns.oid, pr.table_name)
            UNION
            SELECT h.oid, pr.col FROM prov AS pr
            JOIN sql_saga.system_versioning AS sv ON (sv.table_schema, sv.table_name) = (pr.table_schema, pr.table_name)
            JOIN pg_catalog.pg_namespace AS hn ON hn.nspname = sv.history_schema_name
            JOIN pg_catalog.pg_class AS h ON (h.relnamespace, h.relname) = (hn.oid, sv.history_table_name)
        )
        SELECT t.rel_oid, pg_catalog.array_agg(t.col ORDER BY t.col) AS cols
        FROM targets AS t
        JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (t.rel_oid, t.col) AND a.attnum > 0 AND NOT a.attisdropped
        WHERE 'pg_catalog.jsonb'::pg_catalog.regtype IN (
            WITH RECURSIVE base(oid) AS (
                SELECT a.atttypid
                UNION ALL
                SELECT ty.typbasetype FROM base JOIN pg_catalog.pg_type AS ty ON ty.oid = base.oid WHERE ty.typtype = 'd')
            SELECT base.oid FROM base)
        GROUP BY t.rel_oid
        ORDER BY t.rel_oid::pg_catalog.regclass::text
    LOOP
        -- A JSON string whose text is a JSON object (the parse is guarded, so other strings never raise).
        SELECT pg_catalog.string_agg(pg_catalog.format(
                   '(pg_catalog.jsonb_typeof(%1$I) = ''string'' AND CASE WHEN pg_catalog.pg_input_is_valid(%1$I #>> ''{}'', ''jsonb'') THEN pg_catalog.jsonb_typeof((%1$I #>> ''{}'')::pg_catalog.jsonb) = ''object'' ELSE false END)', c), ' OR '),
               pg_catalog.string_agg(pg_catalog.format(
                   '%1$I = CASE WHEN pg_catalog.jsonb_typeof(%1$I) = ''string'' AND CASE WHEN pg_catalog.pg_input_is_valid(%1$I #>> ''{}'', ''jsonb'') THEN pg_catalog.jsonb_typeof((%1$I #>> ''{}'')::pg_catalog.jsonb) = ''object'' ELSE false END THEN (%1$I #>> ''{}'')::pg_catalog.jsonb ELSE %1$I END', c), ', ')
        INTO fixable, sets
        FROM pg_catalog.unnest(rel.cols) AS c;
        -- ONLY: a table that inherits from it is not sql_saga's, and its triggers stay enabled.
        EXECUTE pg_catalog.format('SELECT EXISTS (SELECT FROM ONLY %s WHERE %s)', rel.rel_oid::pg_catalog.regclass, fixable) INTO STRICT found_one;
        IF NOT found_one THEN
            CONTINUE;
        END IF;
        SELECT coalesce(pg_catalog.jsonb_object_agg(t.tgname, t.tgenabled), '{}') INTO modes
        FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = rel.rel_oid AND NOT t.tgisinternal AND t.tgenabled <> 'D';
        FOR trg IN SELECT k AS tgname FROM pg_catalog.jsonb_object_keys(modes) AS k ORDER BY k LOOP
            EXECUTE pg_catalog.format('ALTER TABLE %s DISABLE TRIGGER %I', rel.rel_oid::pg_catalog.regclass, trg.tgname);
        END LOOP;
        EXECUTE pg_catalog.format('UPDATE ONLY %s SET %s WHERE %s', rel.rel_oid::pg_catalog.regclass, sets, fixable);
        GET DIAGNOSTICS n = ROW_COUNT;
        total := total + n;
        FOR trg IN SELECT m.key AS tgname, m.value #>> '{}' AS mode FROM pg_catalog.jsonb_each(modes) AS m ORDER BY m.key LOOP
            EXECUTE pg_catalog.format('ALTER TABLE %s %s TRIGGER %I', rel.rel_oid::pg_catalog.regclass,
                CASE trg.mode WHEN 'O' THEN 'ENABLE' WHEN 'A' THEN 'ENABLE ALWAYS' WHEN 'R' THEN 'ENABLE REPLICA' END, trg.tgname);
        END LOOP;
    END LOOP;
    IF total > 0 THEN
        RAISE NOTICE 'sql_saga: Converted the jsonb provenance values of % rows from the JSON strings sql_saga 1.9 stored into the objects they encode', total;
    END IF;
END;
$repair_jsonb_provenance$;
DO $last_step_gate$
BEGIN
    IF (SELECT pg_catalog.array_agg(x::int) FROM pg_catalog.unnest(pg_catalog.string_to_array(sql_saga.native_library_version(), '.')) AS x)
       = (SELECT pg_catalog.array_agg(x::int) FROM pg_catalog.unnest(pg_catalog.string_to_array(
              (SELECT e.extversion FROM pg_catalog.pg_extension AS e WHERE e.extname = 'sql_saga'), '.')) AS x) THEN
        PERFORM sql_saga.check_integrity();
    END IF;
END;
$last_step_gate$;
