/*
 * sql_saga 1.3 update preamble (fragment P of devops/generate_update_scripts.py; tmp/v13_upgrade_integration_plan.md).
 *
 * The generator places this fragment right after the header and the trusted source-version line
 *   SELECT pg_catalog.set_config('sql_saga.upgrade_from', '<1.0|1.1|1.2>', true);
 * and before the frozen historical update (1.0 and 1.1 sources only). Inside ALTER EXTENSION ... UPDATE,
 * pg_extension.extversion already holds the target version, so nothing here branches on it.
 *
 * Nothing in this fragment changes the database before the precise refusals have run, except the
 * transaction-local settings, the transient disabling of sql_saga_rename_following (restored at the end of
 * the repairs fragment) and the bootstrap of the 1k pairing routines (members of the extension, replaced again
 * with the other routines later). A refusal rolls the whole update back.
 */

/* The source version the generator wrote into this script, checked against the catalog it is about to update. */
DO $upgrade_source$
DECLARE
    source text := pg_catalog.current_setting('sql_saga.upgrade_from', true);
    has_installation boolean := pg_catalog.to_regclass('sql_saga.installation') IS NOT NULL;
    has_sync_guard boolean := pg_catalog.to_regclass('sql_saga.history_policy_sync_guard') IS NOT NULL;
    has_catalog_format boolean := EXISTS (
        SELECT FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = 'sql_saga.era'::pg_catalog.regclass AND a.attname = 'catalog_format' AND NOT a.attisdropped);
    found text;
BEGIN
    found := CASE
        WHEN NOT has_installation AND NOT has_sync_guard AND NOT has_catalog_format THEN '1.0'
        WHEN has_installation AND NOT has_sync_guard AND NOT has_catalog_format THEN '1.1'
        WHEN has_installation AND has_sync_guard AND NOT has_catalog_format THEN '1.2'
    END;
    IF source IS NULL OR source NOT IN ('1.0', '1.1', '1.2') OR found IS DISTINCT FROM source THEN
        RAISE EXCEPTION 'sql_saga: this update script is for sql_saga %, but the installed catalog has the layout of %',
            coalesce(source, '(unknown)'), coalesce(found, 'no released version')
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Run ALTER EXTENSION sql_saga UPDATE without changing the installed extension scripts.';
    END IF;
END;
$upgrade_source$;

CREATE OR REPLACE FUNCTION sql_saga.native_library_version() RETURNS text LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT AS 'sql_saga_native', 'sql_saga_native_version_wrapper';
DO $native_guard_1_3$
BEGIN
    IF sql_saga.native_library_version() <> '1.3' THEN
        RAISE EXCEPTION 'sql_saga: native library version % does not match extension update version 1.3', sql_saga.native_library_version()
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Install the sql_saga 1.3 native library and restart PostgreSQL before retrying ALTER EXTENSION.';
    END IF;
END;
$native_guard_1_3$;

SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('sql_saga 1.3 upgrade'));

/* check_integrity and the mispairing report give the upgrade sequence in their hints (src/1k). */
SELECT pg_catalog.set_config('sql_saga.integrity_context', 'update', true);

/*
 * sql_saga 1.0, 1.1 and 1.2 rename_following rewrite the catalog list of every XOR unique key sorted by name
 * on any ALTER TABLE (1.0: also on GRANT and REVOKE), and the update scripts below issue such statements. So
 * it is disabled for the duration of the update, before any catalog repair, and its recorded state is put back
 * exactly at the end of the repairs fragment, after the 1.3 definition is in place. A disabled trigger stays
 * disabled, and the final integrity gate refuses it with its repair. The state is kept in a transaction-local
 * setting and only ever read back through a fixed list of codes.
 */
DO $record_rename_following$
DECLARE
    state "char";
BEGIN
    SELECT et.evtenabled INTO state FROM pg_catalog.pg_event_trigger AS et WHERE et.evtname = 'sql_saga_rename_following';
    PERFORM pg_catalog.set_config('sql_saga.upgrade_rename_following_state', coalesce(state::text, 'missing'), true);
    IF state IS NOT NULL AND state <> 'D' THEN
        ALTER EVENT TRIGGER sql_saga_rename_following DISABLE;
    END IF;
END;
$record_rename_following$;

/*
 * From 1.0 only: 1.0's event triggers act on every GRANT and REVOKE (its __internal_ddl_command_affects_managed_object
 * answers true for them), and health_checks then re-validates managed view privileges, which refuses a base table
 * granted before its views were created. The bootstrap below issues REVOKEs, so the 1.0 guard is replaced first with
 * exactly the definition the frozen 1.0 to 1.1 part installs as its own first step (versions/sql_saga--1.0--1.1.sql:105);
 * that part replaces it again with its real definition.
 */
DO $neutralize_1_0_event_triggers$
BEGIN
    IF pg_catalog.current_setting('sql_saga.upgrade_from', true) IS DISTINCT FROM '1.0' THEN
        RETURN;
    END IF;
    CREATE OR REPLACE FUNCTION sql_saga.__internal_ddl_command_affects_managed_object()
    RETURNS boolean LANGUAGE sql STABLE SET search_path = pg_catalog
    AS $upgrade_guard$ SELECT false $upgrade_guard$;
END;
$neutralize_1_0_event_triggers$;

-- sql_saga 1.3 generator: insert src/1k_foreign_key_pairing.sql routines here

/*
 * Foreign keys whose managed objects pair child columns with other referenced columns than the unique key's
 * constraint order (sql_saga 1.0/1.1 created them against a catalog list of an XOR key it had sorted, or a
 * child CHECK got its arguments in another order, W11) are refused here, while every object that shows the
 * pairing is untouched, with the exact repair and the upgrade sequence. The intended pairing cannot be known,
 * so nothing is re-paired automatically. Same order as check_integrity.
 */
DO $refuse_mispaired_foreign_keys$
DECLARE
    fk record;
    report record;
BEGIN
    FOR fk IN
        SELECT f.foreign_key_name
        FROM sql_saga.foreign_keys AS f
        ORDER BY f.table_schema COLLATE "C", f.table_name COLLATE "C", f.foreign_key_name COLLATE "C"
    LOOP
        SELECT * INTO report FROM sql_saga.__internal_foreign_key_mispairing_report(fk.foreign_key_name);
        IF report.message IS NOT NULL THEN
            RAISE EXCEPTION USING MESSAGE = report.message, HINT = report.hint, ERRCODE = 'object_not_in_prerequisite_state';
        END IF;
    END LOOP;
END;
$refuse_mispaired_foreign_keys$;

/*
 * The catalog list of an XOR unique key back in the order of its constraint, where an earlier rename_following
 * only re-sorted it (same set of names). A list whose names differ from the constraint's (a rename that was
 * not followed) is left alone: check_integrity reports it.
 */
DO $restore_xor_key_order$
DECLARE
    k record;
BEGIN
    FOR k IN
        SELECT uk.unique_key_name, uk.table_schema, uk.table_name, uk.column_names AS catalog_columns,
               sql_saga.__internal_unique_key_constraint_columns(uk.unique_key_name) AS constraint_columns
        FROM sql_saga.unique_keys AS uk
        WHERE uk.mutually_exclusive_columns IS NOT NULL
        ORDER BY uk.unique_key_name COLLATE "C"
    LOOP
        CONTINUE WHEN k.constraint_columns IS NULL OR k.catalog_columns = k.constraint_columns
                   OR pg_catalog.cardinality(k.catalog_columns) <> pg_catalog.cardinality(k.constraint_columns)
                   OR NOT (k.catalog_columns @> k.constraint_columns AND k.constraint_columns @> k.catalog_columns);
        UPDATE sql_saga.unique_keys AS uk
        SET column_names = k.constraint_columns
        WHERE uk.unique_key_name = k.unique_key_name;
        RAISE NOTICE 'sql_saga: Restored the column order of unique key % on table % to its constraint order (%), from (%)',
            pg_catalog.quote_ident(k.unique_key_name), pg_catalog.format('%I.%I', k.table_schema, k.table_name),
            pg_catalog.array_to_string(k.constraint_columns, ', '), pg_catalog.array_to_string(k.catalog_columns, ', ');
    END LOOP;
END;
$restore_xor_key_order$;

/*
 * From 1.0 only: the frozen 1.0 to 1.1 part drops and recreates every managed view ($reconcile_updatable_views$).
 * An object of the user's that depends on one (a view, a rule, a function using its row type) would be lost or
 * make that DROP fail obscurely, so the update refuses first, naming it. sql_saga never rebuilds user objects.
 */
DO $refuse_view_dependents_1_0$
DECLARE
    v record;
BEGIN
    IF pg_catalog.current_setting('sql_saga.upgrade_from', true) IS DISTINCT FROM '1.0' THEN
        RETURN;
    END IF;
    FOR v IN
        WITH managed AS (
            SELECT uv.view_type, pg_catalog.to_regclass(pg_catalog.format('%I.%I', uv.view_schema, uv.view_name))::oid AS view_oid
            FROM sql_saga.updatable_view AS uv
        ), own_type AS (
            SELECT m.view_oid, t.oid AS type_oid FROM managed AS m JOIN pg_catalog.pg_type AS t ON t.typrelid = m.view_oid
            UNION ALL
            SELECT m.view_oid, ta.oid FROM managed AS m
            JOIN pg_catalog.pg_type AS t ON t.typrelid = m.view_oid
            JOIN pg_catalog.pg_type AS ta ON ta.oid = t.typarray
        )
        SELECT m.view_type, m.view_oid, dep.dependent
        FROM managed AS m
        CROSS JOIN LATERAL (
            SELECT pg_catalog.pg_describe_object(
                       CASE WHEN d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass THEN 'pg_catalog.pg_class'::pg_catalog.regclass ELSE d.classid END,
                       CASE WHEN d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass THEN rw.ev_class ELSE d.objid END,
                       CASE WHEN d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass THEN 0 ELSE d.objsubid END) AS dependent
            FROM pg_catalog.pg_depend AS d
            LEFT JOIN pg_catalog.pg_rewrite AS rw ON d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass AND rw.oid = d.objid
            WHERE ((d.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.refobjid = m.view_oid)
                   OR (d.refclassid = 'pg_catalog.pg_type'::pg_catalog.regclass
                       AND d.refobjid IN (SELECT ot.type_oid FROM own_type AS ot WHERE ot.view_oid = m.view_oid)))
              AND d.deptype NOT IN ('i', 'a', 'e', 'x')
              AND NOT (d.classid = 'pg_catalog.pg_rewrite'::pg_catalog.regclass AND rw.ev_class = m.view_oid)
              AND NOT (d.classid = 'pg_catalog.pg_trigger'::pg_catalog.regclass
                       AND EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.oid = d.objid AND t.tgrelid = m.view_oid))
              AND NOT (d.classid = 'pg_catalog.pg_attrdef'::pg_catalog.regclass
                       AND EXISTS (SELECT FROM pg_catalog.pg_attrdef AS ad WHERE ad.oid = d.objid AND ad.adrelid = m.view_oid))
              AND NOT (d.classid = 'pg_catalog.pg_type'::pg_catalog.regclass
                       AND d.objid IN (SELECT ot.type_oid FROM own_type AS ot WHERE ot.view_oid = m.view_oid))
              AND NOT (d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass AND d.objid = m.view_oid)
            UNION ALL
            SELECT pg_catalog.pg_describe_object('pg_catalog.pg_rewrite'::pg_catalog.regclass, r.oid, 0)
            FROM pg_catalog.pg_rewrite AS r
            WHERE r.ev_class = m.view_oid AND r.rulename <> '_RETURN'
        ) AS dep
        WHERE m.view_oid IS NOT NULL
        ORDER BY m.view_oid::pg_catalog.regclass::text COLLATE "C", dep.dependent COLLATE "C"
        LIMIT 1
    LOOP
        RAISE EXCEPTION 'sql_saga: cannot update from 1.0: the update drops and recreates the managed % view %, and % depends on that view',
            v.view_type, v.view_oid::pg_catalog.regclass, v.dependent
            USING ERRCODE = 'dependent_objects_still_exist',
                  HINT = 'Drop the dependent object, run ALTER EXTENSION sql_saga UPDATE, then recreate the dependent object.';
    END LOOP;
END;
$refuse_view_dependents_1_0$;
