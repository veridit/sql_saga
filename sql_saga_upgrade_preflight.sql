\set ON_ERROR_STOP on
\pset pager off

DO $preflight$
DECLARE
    installed_version text;
    policy_list text;
    probe_acl pg_catalog.aclitem[];
    probe_owner pg_catalog.oid;
BEGIN
    SELECT e.extversion INTO installed_version
    FROM pg_catalog.pg_extension AS e
    WHERE e.extname = 'sql_saga';

    IF installed_version = '1.0' THEN
        IF pg_catalog.to_regclass('sql_saga.temporal_merge_cache') IS NULL
           OR (SELECT pg_catalog.count(*) FROM pg_catalog.pg_proc AS p
               WHERE p.pronamespace = 'sql_saga'::pg_catalog.regnamespace
                 AND p.proname ~ '^temporal_merge_(cache|source_columns_hash|invalidate_cache)') <> 8 THEN
            RAISE EXCEPTION 'sql_saga: installed version 1.0 is not the supported v1.0 release shape'
                USING ERRCODE = 'object_not_in_prerequisite_state',
                      HINT = 'Restore the released v1.0 artifact before upgrading, or contact the sql_saga maintainer.';
        END IF;
    ELSIF installed_version = '1.1' THEN
        IF pg_catalog.to_regclass('sql_saga.installation') IS NULL
           OR pg_catalog.to_regclass('sql_saga.temporal_merge_cache') IS NOT NULL
           OR (SELECT pg_catalog.count(*) FROM pg_catalog.pg_event_trigger AS e
               WHERE e.evtfoid = 'sql_saga.drop_protection()'::pg_catalog.regprocedure
                 AND e.evtenabled <> 'D') <> 1 THEN
            RAISE EXCEPTION 'sql_saga: installed version 1.1 is not the supported v1.1 release shape'
                USING ERRCODE = 'object_not_in_prerequisite_state',
                      HINT = 'Restore the released v1.1 artifact before upgrading, or contact the sql_saga maintainer.';
        END IF;
    ELSIF installed_version = '1.2' THEN
        IF pg_catalog.to_regclass('sql_saga.installation') IS NULL
           OR pg_catalog.to_regclass('sql_saga.temporal_merge_cache') IS NOT NULL
           OR pg_catalog.to_regclass('sql_saga.history_policy_sync_guard') IS NULL
           OR (SELECT pg_catalog.count(*) FROM pg_catalog.pg_event_trigger AS e
               WHERE e.evtfoid = 'sql_saga.drop_protection()'::pg_catalog.regprocedure
                 AND e.evtenabled <> 'D') <> 1 THEN
            RAISE EXCEPTION 'sql_saga: installed version 1.2 is not the supported v1.2 release shape'
                USING ERRCODE = 'object_not_in_prerequisite_state',
                      HINT = 'Restore the released v1.2 artifact before upgrading, or contact the sql_saga maintainer.';
        END IF;
    ELSIF installed_version = '1.3' THEN
        IF pg_catalog.to_regclass('sql_saga.installation') IS NULL
           OR pg_catalog.to_regclass('sql_saga.history_policy_sync_guard') IS NULL
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a
                          WHERE a.attrelid = 'sql_saga.era'::pg_catalog.regclass AND a.attname = 'catalog_format' AND NOT a.attisdropped) THEN
            RAISE EXCEPTION 'sql_saga: installed version 1.3 is not the supported v1.3 release shape'
                USING ERRCODE = 'object_not_in_prerequisite_state',
                      HINT = 'Restore the released v1.3 artifact before upgrading, or contact the sql_saga maintainer.';
        END IF;
    ELSIF installed_version = '1.4' THEN
        IF pg_catalog.to_regclass('sql_saga.installation') IS NULL
           OR pg_catalog.to_regclass('sql_saga.history_policy_sync_guard') IS NULL
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a
                          WHERE a.attrelid = 'sql_saga.foreign_keys'::pg_catalog.regclass AND a.attname = 'uk_truncate_trigger' AND NOT a.attisdropped)
           OR pg_catalog.to_regprocedure('sql_saga.uk_truncate_check_c()') IS NULL THEN
            RAISE EXCEPTION 'sql_saga: installed version 1.4 is not the supported v1.4 release shape'
                USING ERRCODE = 'object_not_in_prerequisite_state',
                      HINT = 'Restore the released v1.4 artifact before upgrading, or contact the sql_saga maintainer.';
        END IF;
    ELSIF installed_version = '1.5' THEN
        IF pg_catalog.to_regclass('sql_saga.installation') IS NULL
           OR pg_catalog.to_regclass('sql_saga.history_policy_sync_guard') IS NULL
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a
                          WHERE a.attrelid = 'sql_saga.foreign_keys'::pg_catalog.regclass AND a.attname = 'uk_truncate_trigger' AND NOT a.attisdropped)
           OR pg_catalog.to_regprocedure('sql_saga.uk_truncate_check_c()') IS NULL
           OR pg_catalog.to_regprocedure('sql_saga.for_portion_of_apply_trigger()') IS NOT NULL
           OR pg_catalog.strpos(pg_catalog.pg_get_functiondef('sql_saga.for_portion_of_trigger()'::pg_catalog.regprocedure), 'DELETE_FOR_PORTION_OF') = 0 THEN
            RAISE EXCEPTION 'sql_saga: installed version 1.5 is not the supported v1.5 release shape'
                USING ERRCODE = 'object_not_in_prerequisite_state',
                      HINT = 'Restore the released v1.5 artifact before upgrading, or contact the sql_saga maintainer.';
        END IF;
    ELSIF installed_version = '1.6' THEN
        IF pg_catalog.to_regclass('sql_saga.installation') IS NULL
           OR pg_catalog.to_regclass('sql_saga.history_policy_sync_guard') IS NULL
           OR pg_catalog.to_regprocedure('sql_saga.uk_truncate_check_c()') IS NULL
           OR pg_catalog.to_regprocedure('sql_saga.for_portion_of_apply_trigger()') IS NULL
           OR pg_catalog.to_regprocedure('sql_saga.current_view_apply_trigger()') IS NOT NULL THEN
            RAISE EXCEPTION 'sql_saga: installed version 1.6 is not the supported v1.6 release shape'
                USING ERRCODE = 'object_not_in_prerequisite_state',
                      HINT = 'Restore the released v1.6 artifact before upgrading, or contact the sql_saga maintainer.';
        END IF;
    ELSIF installed_version = '1.7' THEN
        IF pg_catalog.to_regclass('sql_saga.installation') IS NULL
           OR pg_catalog.to_regprocedure('sql_saga.for_portion_of_apply_trigger()') IS NULL
           OR pg_catalog.to_regprocedure('sql_saga.current_view_apply_trigger()') IS NULL
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a
                          WHERE a.attrelid = 'sql_saga.era'::pg_catalog.regclass AND a.attname = 'audit_table_name' AND NOT a.attisdropped) THEN
            RAISE EXCEPTION 'sql_saga: installed version 1.7 is not the supported v1.7 release shape'
                USING ERRCODE = 'object_not_in_prerequisite_state',
                      HINT = 'Restore the released v1.7 artifact before upgrading, or contact the sql_saga maintainer.';
        END IF;
    ELSIF installed_version = '1.8' THEN
        IF pg_catalog.to_regclass('sql_saga.installation') IS NULL
           OR pg_catalog.to_regprocedure('sql_saga.current_view_apply_trigger()') IS NULL
           OR EXISTS (SELECT FROM pg_catalog.pg_attribute AS a
                      WHERE a.attrelid = 'sql_saga.era'::pg_catalog.regclass AND a.attname IN ('audit_table_name', 'provenance_columns') AND NOT a.attisdropped)
           OR pg_catalog.to_regprocedure('sql_saga.add_era(regclass, name, name, boolean, name, name, name, boolean, boolean, boolean, regtype, name, name[])') IS NULL THEN
            RAISE EXCEPTION 'sql_saga: installed version 1.8 is not the supported v1.8 release shape'
                USING ERRCODE = 'object_not_in_prerequisite_state',
                      HINT = 'Restore the released v1.8 artifact before upgrading, or contact the sql_saga maintainer.';
        END IF;
    ELSIF installed_version = '1.9' THEN
        IF pg_catalog.to_regclass('sql_saga.installation') IS NULL
           OR pg_catalog.to_regprocedure('sql_saga.provenance_trigger()') IS NULL
           OR NOT EXISTS (SELECT FROM pg_catalog.pg_attribute AS a
                          WHERE a.attrelid = 'sql_saga.era'::pg_catalog.regclass AND a.attname = 'provenance_columns' AND NOT a.attisdropped) THEN
            RAISE EXCEPTION 'sql_saga: installed version 1.9 is not the supported v1.9 release shape'
                USING ERRCODE = 'object_not_in_prerequisite_state',
                      HINT = 'Restore the released v1.9 artifact before upgrading, or contact the sql_saga maintainer.';
        END IF;
    ELSIF installed_version = '1.10' THEN
        IF pg_catalog.to_regclass('sql_saga.installation') IS NULL
           OR pg_catalog.to_regprocedure('sql_saga.provenance_trigger()') IS NULL
           OR pg_catalog.strpos(pg_catalog.pg_get_functiondef('sql_saga.provenance_trigger()'::pg_catalog.regprocedure), 'pg_catalog.to_jsonb(%s)') = 0 THEN
            RAISE EXCEPTION 'sql_saga: installed version 1.10 is not the supported v1.10 release shape'
                USING ERRCODE = 'object_not_in_prerequisite_state',
                      HINT = 'Restore the released v1.10 artifact before upgrading, or contact the sql_saga maintainer.';
        END IF;
    ELSIF installed_version = '1.11' THEN
        IF pg_catalog.to_regclass('sql_saga.installation') IS NULL
           OR pg_catalog.to_regprocedure('sql_saga.provenance_trigger()') IS NULL
           OR pg_catalog.strpos(pg_catalog.pg_get_functiondef('sql_saga.temporal_merge_execute(regclass, regclass, text[], sql_saga.temporal_merge_mode, name, name, name, boolean, text[], sql_saga.temporal_merge_delete_mode, boolean, name, name, name, name, text[], boolean)'::pg_catalog.regprocedure), 'jsonb_populate_record(NULL::%s') = 0 THEN
            RAISE EXCEPTION 'sql_saga: installed version 1.11 is not the supported v1.11 release shape'
                USING ERRCODE = 'object_not_in_prerequisite_state',
                      HINT = 'Restore the released v1.11 artifact before upgrading, or contact the sql_saga maintainer.';
        END IF;
    ELSIF installed_version = '1.12' THEN
        IF pg_catalog.to_regclass('sql_saga.installation') IS NULL
           OR pg_catalog.to_regprocedure('sql_saga.provenance_trigger()') IS NULL
           OR pg_catalog.strpos(pg_catalog.pg_get_functiondef('sql_saga.temporal_merge_plan(regclass, regclass, sql_saga.temporal_merge_mode, name, text[], name, name, sql_saga.temporal_merge_delete_mode, jsonb, text[], boolean, boolean)'::pg_catalog.regprocedure), 'strict $.*.**{1 to last} ? (@ == null)') = 0 THEN
            RAISE EXCEPTION 'sql_saga: installed version 1.12 is not the supported v1.12 release shape'
                USING ERRCODE = 'object_not_in_prerequisite_state',
                      HINT = 'Restore the released v1.12 artifact before upgrading, or contact the sql_saga maintainer.';
        END IF;
    ELSIF installed_version = '1.13' THEN
        IF pg_catalog.to_regclass('sql_saga.installation') IS NULL
           OR pg_catalog.to_regprocedure('sql_saga.provenance_trigger()') IS NULL
           OR pg_catalog.strpos(pg_catalog.pg_get_functiondef('sql_saga.__internal_drop_protection()'::pg_catalog.regprocedure), 'd.address_names[3] IN (e.bounds_check_constraint, e.boundary_check_constraint)') = 0 THEN
            RAISE EXCEPTION 'sql_saga: installed version 1.13 is not the supported v1.13 release shape'
                USING ERRCODE = 'object_not_in_prerequisite_state',
                      HINT = 'Restore the released v1.13 artifact before upgrading, or contact the sql_saga maintainer.';
        END IF;
    ELSE
        RAISE EXCEPTION 'sql_saga: upgrade preflight requires released sql_saga 1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 1.10, 1.11, 1.12 or 1.13, found %', coalesce(installed_version, 'not installed')
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Run this preflight only in a database containing the released v1.0, v1.1, v1.2, v1.3, v1.4, v1.5, v1.6, v1.7, v1.8, v1.9, v1.10, v1.11, v1.12 or v1.13 extension.';
    END IF;

    /* Regular foreign keys the 1.4 update refuses (a partitioned referencing table, a predicated referenced key). */
    SELECT pg_catalog.string_agg(pg_catalog.format('%I on %I.%I: %s', f.foreign_key_name, f.table_schema, f.table_name,
               CASE WHEN c.relkind = 'p' THEN 'the referencing table is partitioned' ELSE 'the referenced key is predicated' END),
               E'\n' ORDER BY f.foreign_key_name)
    INTO policy_list
    FROM sql_saga.foreign_keys AS f
    JOIN sql_saga.unique_keys AS u ON u.unique_key_name = f.unique_key_name
    JOIN pg_catalog.pg_namespace AS n ON n.nspname = f.table_schema
    JOIN pg_catalog.pg_class AS c ON (c.relnamespace, c.relname) = (n.oid, f.table_name)
    WHERE f.type = 'regular_to_temporal' AND (c.relkind = 'p' OR u.key_type = 'predicated');
    IF policy_list IS NOT NULL THEN
        RAISE EXCEPTION 'sql_saga: regular foreign keys that sql_saga 1.4 does not support prevent upgrade:%', E'\n' || policy_list
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Drop each with sql_saga.drop_foreign_key_by_name before updating; after the update add it to each partition, or against a primary or natural key (doc/upgrade.md).';
    END IF;

    /* For_portion_of views that already have a trigger named as the 1.6 apply trigger refuse the update. */
    SELECT pg_catalog.string_agg(pg_catalog.format('%I.%I trigger %I', uv.view_schema, uv.view_name, t.tgname), E'\n' ORDER BY uv.view_schema, uv.view_name)
    INTO policy_list
    FROM sql_saga.updatable_view AS uv
    JOIN pg_catalog.pg_trigger AS t
      ON t.tgrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', uv.view_schema, uv.view_name))
     AND t.tgname = uv.trigger_name || '_apply'
    WHERE uv.view_type = 'for_portion_of'
      AND t.tgfoid IS DISTINCT FROM pg_catalog.to_regprocedure('sql_saga.for_portion_of_apply_trigger()');
    IF policy_list IS NOT NULL THEN
        RAISE EXCEPTION 'sql_saga: triggers named as the 1.6 apply trigger of a for_portion_of view prevent upgrade:%', E'\n' || policy_list
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Rename or drop each trigger before updating (doc/upgrade.md).';
    END IF;

    /* Current views that already have a trigger named as the 1.7 apply trigger refuse the update. */
    SELECT pg_catalog.string_agg(pg_catalog.format('%I.%I trigger %I', uv.view_schema, uv.view_name, t.tgname), E'\n' ORDER BY uv.view_schema, uv.view_name)
    INTO policy_list
    FROM sql_saga.updatable_view AS uv
    JOIN pg_catalog.pg_trigger AS t
      ON t.tgrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', uv.view_schema, uv.view_name))
     AND t.tgname = uv.trigger_name || '_apply'
    WHERE uv.view_type = 'current'
      AND t.tgfoid IS DISTINCT FROM pg_catalog.to_regprocedure('sql_saga.current_view_apply_trigger()');
    IF policy_list IS NOT NULL THEN
        RAISE EXCEPTION 'sql_saga: triggers named as the 1.7 apply trigger of a current view prevent upgrade:%', E'\n' || policy_list
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Rename or drop each trigger before updating (doc/upgrade.md).';
    END IF;

    /* Values someone wrote by hand into the era audit columns, which the 1.8 update drops; no release writes them. */
    IF EXISTS (SELECT FROM pg_catalog.pg_attribute AS a
               WHERE a.attrelid = 'sql_saga.era'::pg_catalog.regclass AND a.attname = 'audit_table_name' AND NOT a.attisdropped) THEN
        EXECUTE $q$SELECT pg_catalog.string_agg(pg_catalog.format('%I.%I era %I', e.table_schema, e.table_name, e.era_name), E'\n'
                       ORDER BY e.table_schema, e.table_name, e.era_name)
                   FROM sql_saga.era AS e WHERE e.audit_schema_name IS NOT NULL OR e.audit_table_name IS NOT NULL$q$
        INTO policy_list;
        IF policy_list IS NOT NULL THEN
            RAISE EXCEPTION 'sql_saga: era rows with audit_schema_name or audit_table_name set prevent upgrade:%', E'\n' || policy_list
                USING ERRCODE = 'object_not_in_prerequisite_state',
                      HINT = 'Set both columns to NULL in those rows (no sql_saga release reads them), then update.';
        END IF;
    END IF;

    /* History tables of system-versioned tables that are not persistent: the 1.8 update refuses them. */
    SELECT pg_catalog.string_agg(pg_catalog.format('%I.%I', sv.history_schema_name, sv.history_table_name), E'\n'
               ORDER BY sv.history_schema_name, sv.history_table_name)
    INTO policy_list
    FROM sql_saga.system_versioning AS sv
    JOIN pg_catalog.pg_namespace AS hn ON hn.nspname = sv.history_schema_name
    JOIN pg_catalog.pg_class AS h ON h.relnamespace = hn.oid AND h.relname = sv.history_table_name
    WHERE h.relpersistence <> 'p';
    IF policy_list IS NOT NULL THEN
        RAISE EXCEPTION 'sql_saga: history tables that are not persistent prevent upgrade:%', E'\n' || policy_list
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Run ALTER TABLE <history table> SET LOGGED for each before updating.';
    END IF;

    /* EXECUTE privileges on add_era that differ from those its replacement gets: the 1.9 update replaces the
       signature and refuses them. The replacement gets the default privileges of a function created now, read from a
       probe that is rolled back, minus PUBLIC for the private worker (the rule of the update script). */
    BEGIN
        EXECUTE 'CREATE FUNCTION sql_saga.__sql_saga_preflight_acl_probe() RETURNS void LANGUAGE sql AS ''SELECT''';
        SELECT p.proacl, p.proowner INTO probe_acl, probe_owner
        FROM pg_catalog.pg_proc AS p
        WHERE p.oid = 'sql_saga.__sql_saga_preflight_acl_probe()'::pg_catalog.regprocedure;
        RAISE EXCEPTION USING ERRCODE = 'P0001', MESSAGE = 'sql_saga preflight probe rollback';
    EXCEPTION WHEN raise_exception THEN
        NULL;
    END;
    WITH old_set AS (
        SELECT p.oid AS proc, p.proname, a.grantee, pg_catalog.bool_or(a.is_grantable) AS grantable
        FROM pg_catalog.pg_proc AS p
        CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(p.proacl, pg_catalog.acldefault('f', p.proowner))) AS a
        WHERE p.oid IN (pg_catalog.to_regprocedure('sql_saga.add_era(regclass, name, name, boolean, name, name, name, boolean, boolean, boolean, regtype, name, name[])'),
                        pg_catalog.to_regprocedure('sql_saga.__api_add_era(regclass, name, name, boolean, name, name, name, boolean, boolean, boolean, regtype, name, name[])'))
          AND a.privilege_type = 'EXECUTE'
          AND a.grantee <> p.proowner
        GROUP BY p.oid, p.proname, a.grantee
    ), new_set AS (
        SELECT p.oid AS proc, p.proname, a.grantee, pg_catalog.bool_or(a.is_grantable) AS grantable
        FROM pg_catalog.pg_proc AS p
        CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(probe_acl, pg_catalog.acldefault('f', probe_owner))) AS a
        WHERE p.oid IN (pg_catalog.to_regprocedure('sql_saga.add_era(regclass, name, name, boolean, name, name, name, boolean, boolean, boolean, regtype, name, name[])'),
                        pg_catalog.to_regprocedure('sql_saga.__api_add_era(regclass, name, name, boolean, name, name, name, boolean, boolean, boolean, regtype, name, name[])'))
          AND a.privilege_type = 'EXECUTE'
          AND a.grantee <> probe_owner
          AND (a.grantee <> 0 OR p.proname = 'add_era')
        GROUP BY p.oid, p.proname, a.grantee
    )
    SELECT pg_catalog.string_agg(pg_catalog.format('%s: %s %s', coalesce(o.proc, n.proc)::pg_catalog.regprocedure,
               CASE WHEN coalesce(o.grantee, n.grantee) = 0 THEN 'PUBLIC' ELSE coalesce(o.grantee, n.grantee)::pg_catalog.regrole::text END,
               CASE WHEN o.grantee IS NULL THEN 'would gain EXECUTE'
                    WHEN n.grantee IS NULL THEN 'would lose EXECUTE'
                    ELSE 'would change grant option' END),
               E'\n' ORDER BY coalesce(o.proname, n.proname), coalesce(o.grantee, n.grantee))
    INTO policy_list
    FROM old_set AS o
    FULL JOIN new_set AS n ON (n.proc, n.grantee) = (o.proc, o.grantee)
    WHERE o.grantable IS DISTINCT FROM n.grantable;
    IF policy_list IS NOT NULL THEN
        RAISE EXCEPTION 'sql_saga: EXECUTE privileges on add_era that the 1.9 update cannot carry over to the new signature prevent upgrade:%', E'\n' || policy_list
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Reset them, update, and apply them to the new signature after the update; the update prints the exact statements (doc/upgrade.md).';
    END IF;

    SELECT pg_catalog.string_agg(pg_catalog.format('%I.%I policy %I', n.nspname, c.relname, p.polname), E'\n' ORDER BY n.nspname, c.relname, p.polname)
    INTO policy_list
    FROM sql_saga.system_versioning AS sv
    JOIN pg_catalog.pg_namespace AS n ON n.nspname = sv.history_schema_name
    JOIN pg_catalog.pg_class AS c ON c.relnamespace = n.oid AND c.relname = sv.history_table_name
    JOIN pg_catalog.pg_policy AS p ON p.polrelid = c.oid
    WHERE p.polname !~ '^sql_saga_';
    IF policy_list IS NOT NULL THEN
        RAISE EXCEPTION 'sql_saga: non-managed history policies prevent upgrade:%', E'\n' || policy_list
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Move each policy to its base table, drop it from history, and rerun preflight.';
    END IF;

    /* Natural keys the 1.14 update binds to their primary key (an earlier release left them unbound): a natural key
       value held by more than one primary key value refuses the update. The query mirrors the guard: equal values
       (a NULL is equal to nothing), one guard per column of a mutually exclusive key, on the rows where only that
       column is set. */
    DECLARE
        k record;
        stable_pk name[];
        variant record;
        nk_value text;
    BEGIN
        policy_list := NULL;
        FOR k IN
            SELECT nk.unique_key_name, nk.table_schema, nk.table_name, nk.column_names, nk.mutually_exclusive_columns,
                   pk.column_names AS pk_columns, e.range_column_name
            FROM sql_saga.unique_keys AS nk
            JOIN sql_saga.unique_keys AS pk ON (pk.table_schema, pk.table_name, pk.key_type) = (nk.table_schema, nk.table_name, 'primary')
            JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (nk.table_schema, nk.table_name, nk.era_name)
            WHERE nk.key_type = 'natural' AND (nk.pk_consistency_constraint_names IS NULL OR nk.pk_consistency_constraint_names = '{}')
            ORDER BY nk.table_schema, nk.table_name, nk.unique_key_name
        LOOP
            stable_pk := ARRAY(SELECT c FROM pg_catalog.unnest(k.pk_columns) AS c WHERE c <> k.range_column_name);
            IF stable_pk <@ ARRAY(SELECT c FROM pg_catalog.unnest(k.column_names) AS c
                                  WHERE k.mutually_exclusive_columns IS NULL OR c <> ALL (k.mutually_exclusive_columns)) THEN
                CONTINUE;
            END IF;
            FOR variant IN
                SELECT ARRAY(SELECT c FROM pg_catalog.unnest(k.column_names) AS c WHERE c <> ALL (k.mutually_exclusive_columns)) || x AS cols,
                       ARRAY(SELECT o FROM pg_catalog.unnest(k.mutually_exclusive_columns) AS o WHERE o <> x) AS nulls
                FROM pg_catalog.unnest(k.mutually_exclusive_columns) AS x
                WHERE k.mutually_exclusive_columns IS NOT NULL
                UNION ALL
                SELECT k.column_names, '{}'::name[] WHERE k.mutually_exclusive_columns IS NULL
            LOOP
                EXECUTE pg_catalog.format(
                    'SELECT pg_catalog.string_agg(v, '', '') FROM (SELECT pg_catalog.format(%L, %s) AS v FROM %I.%I WHERE %s GROUP BY %s HAVING pg_catalog.count(DISTINCT (%s)) > 1 ORDER BY 1 LIMIT 5) AS x',
                    '(' || pg_catalog.array_to_string(ARRAY(SELECT '%s' FROM pg_catalog.unnest(variant.cols)), ', ') || ')',
                    (SELECT pg_catalog.string_agg(pg_catalog.format('%I', c), ', ') FROM pg_catalog.unnest(variant.cols) AS c),
                    k.table_schema, k.table_name,
                    (SELECT pg_catalog.string_agg(f, ' AND ') FROM (
                        SELECT pg_catalog.format('%I IS NOT NULL', c) AS f FROM pg_catalog.unnest(variant.cols) AS c
                        UNION ALL
                        SELECT pg_catalog.format('%I IS NULL', c) FROM pg_catalog.unnest(variant.nulls) AS c) AS w),
                    (SELECT pg_catalog.string_agg(pg_catalog.format('%I', c), ', ') FROM pg_catalog.unnest(variant.cols) AS c),
                    (SELECT pg_catalog.string_agg(pg_catalog.format('%I', c), ', ') FROM pg_catalog.unnest(stable_pk) AS c))
                INTO nk_value;
                IF nk_value IS NOT NULL THEN
                    policy_list := pg_catalog.concat_ws(E'\n', policy_list,
                        pg_catalog.format('%I.%I natural key %I: %s', k.table_schema, k.table_name, k.unique_key_name, nk_value));
                END IF;
            END LOOP;
        END LOOP;
        IF policy_list IS NOT NULL THEN
            RAISE EXCEPTION 'sql_saga: natural key values held by more than one primary key value prevent upgrade:%', E'\n' || policy_list
                USING ERRCODE = 'object_not_in_prerequisite_state',
                      HINT = 'The 1.14 update binds every natural key value to one primary key value. Correct the rows so that each value belongs to one entity.';
        END IF;
    END;
END;
$preflight$;

SELECT 'family' AS report, pg_catalog.format('%I.%I', sv.table_schema, sv.table_name) AS object, NULL::text AS grantee, NULL::text AS privilege
FROM sql_saga.system_versioning AS sv
UNION ALL
SELECT 'derived grant', c.oid::pg_catalog.regclass::text, x.grantee::pg_catalog.regrole::text, x.privilege_type
FROM pg_catalog.pg_class AS c
CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) AS x
WHERE x.grantee <> 0
  AND x.grantee <> c.relowner
  AND (EXISTS (SELECT FROM sql_saga.system_versioning AS sv WHERE c.oid IN (
          pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.history_schema_name, sv.history_table_name)),
          pg_catalog.to_regclass(pg_catalog.format('%I.%I', sv.view_schema_name, sv.view_table_name))))
       OR EXISTS (SELECT FROM sql_saga.updatable_view AS uv WHERE c.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', uv.view_schema, uv.view_name))))
ORDER BY 1, 2, 3, 4;
