/*
 * sql_saga 1.4 update preamble (1.4 delta of devops/generate_update_scripts_1_4.py).
 *
 * Runs on a catalog with the 1.3 layout: directly from 1.3, or right after the embedded update to 1.3 (1.0, 1.1 and
 * 1.2 sources). The generator writes the trusted source version first:
 *   SELECT pg_catalog.set_config('sql_saga.upgrade_from_1_4', '<1.0|1.1|1.2|1.3>', true);
 * A refusal rolls the whole update back.
 */

/* The catalog must have the 1.3 layout now (and not yet the 1.4 one). */
DO $upgrade_source_1_4$
DECLARE
    source text := pg_catalog.current_setting('sql_saga.upgrade_from_1_4', true);
    has_catalog_format boolean := EXISTS (
        SELECT FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = 'sql_saga.era'::pg_catalog.regclass AND a.attname = 'catalog_format' AND NOT a.attisdropped);
    has_truncate_trigger boolean := EXISTS (
        SELECT FROM pg_catalog.pg_attribute AS a
        WHERE a.attrelid = 'sql_saga.foreign_keys'::pg_catalog.regclass AND a.attname = 'uk_truncate_trigger' AND NOT a.attisdropped);
BEGIN
    IF source IS NULL OR source NOT IN ('1.0', '1.1', '1.2', '1.3') OR NOT has_catalog_format OR has_truncate_trigger THEN
        RAISE EXCEPTION 'sql_saga: this update to 1.4 expects the sql_saga 1.3 catalog layout at this point, from source %',
            coalesce(source, '(unknown)')
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Run ALTER EXTENSION sql_saga UPDATE without changing the installed extension scripts.';
    END IF;
END;
$upgrade_source_1_4$;

CREATE OR REPLACE FUNCTION sql_saga.native_library_version() RETURNS text LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT AS 'sql_saga_native', 'sql_saga_native_version_wrapper';
DO $native_guard_1_4$
BEGIN
    IF sql_saga.native_library_version() <> '1.4' THEN
        RAISE EXCEPTION 'sql_saga: native library version % does not match extension update version 1.4', sql_saga.native_library_version()
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Install the sql_saga 1.4 native library and restart PostgreSQL before retrying ALTER EXTENSION.';
    END IF;
END;
$native_guard_1_4$;

SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('sql_saga 1.4 upgrade'));
SELECT pg_catalog.set_config('sql_saga.integrity_context', 'update', true);
