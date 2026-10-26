\set ON_ERROR_STOP on
\pset pager off

DO $preflight$
DECLARE
    installed_version text;
    policy_list text;
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
    ELSE
        RAISE EXCEPTION 'sql_saga: upgrade preflight requires released sql_saga 1.0, 1.1, 1.2 or 1.3, found %', coalesce(installed_version, 'not installed')
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Run this preflight only in a database containing the released v1.0, v1.1, v1.2 or v1.3 extension.';
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
