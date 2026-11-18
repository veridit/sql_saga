/*
 * sql_saga 1.8 update: catalog layout changes (schema fragment of devops/generate_update_scripts_1_8.py).
 * Runs on a catalog with the 1.4 layout (1.4 to 1.7): directly from 1.4 to 1.7, or after the embedded update from
 * 1.0 to 1.3. Kept in step with src/0b_schema.sql. The generator places it after the 1.8 routines: the DROP COLUMN
 * fires sql_saga's event triggers, whose 1.7 definitions still read the dropped columns.
 */

/*
 * era: audit_schema_name and audit_table_name came from the periods extension's system versioning, which sql_saga
 * replaced with its own (sql_saga.system_versioning). No release ever wrote them. They are dropped only when every
 * row is NULL; a value means someone wrote one by hand, and the update refuses rather than discard it.
 */
DO $era_audit_columns_1_8$
DECLARE
    n_columns integer;
    n_set bigint;
BEGIN
    SELECT pg_catalog.count(*) INTO n_columns
    FROM pg_catalog.pg_attribute AS a
    WHERE a.attrelid = 'sql_saga.era'::pg_catalog.regclass AND NOT a.attisdropped
      AND a.attname IN ('audit_schema_name', 'audit_table_name');
    IF n_columns <> 2 THEN
        RAISE EXCEPTION 'sql_saga: expected the columns audit_schema_name and audit_table_name of the 1.4 layout on sql_saga.era, found %', n_columns
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Restore the sql_saga catalog tables as the installed release created them, then retry the update.';
    END IF;
    EXECUTE 'SELECT pg_catalog.count(*) FROM sql_saga.era WHERE audit_schema_name IS NOT NULL OR audit_table_name IS NOT NULL'
        INTO n_set;
    IF n_set > 0 THEN
        RAISE EXCEPTION 'sql_saga: % rows of sql_saga.era have audit_schema_name or audit_table_name set; sql_saga never writes them', n_set
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Set both columns to NULL in those rows (no sql_saga release reads them), then retry the update.';
    END IF;
    ALTER TABLE sql_saga.era DROP COLUMN audit_schema_name, DROP COLUMN audit_table_name;
    RAISE NOTICE 'sql_saga: Dropped the unused columns audit_schema_name and audit_table_name of table sql_saga.era';
END;
$era_audit_columns_1_8$;

/*
 * era.catalog_format: the era layout changes, so a 1.4 to 1.7 dump restored directly into 1.8 must fail at its first
 * era row. The released constraint is identified by its exact definition.
 */
DO $catalog_format_1_8$
DECLARE
    released CONSTANT text := 'CHECK (((catalog_format IS NOT NULL) AND (catalog_format = ''1.4''::text)))';
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS c
                   WHERE c.conrelid = 'sql_saga.era'::pg_catalog.regclass
                     AND c.conname = 'restore old sql_saga dumps per doc/upgrade.md'
                     AND pg_catalog.pg_get_constraintdef(c.oid) = released) THEN
        RAISE EXCEPTION 'sql_saga: expected the released catalog_format constraint of sql_saga 1.4 to 1.7 on sql_saga.era'
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Restore the sql_saga catalog tables as the installed release created them, then retry the update.';
    END IF;
    ALTER TABLE sql_saga.era DROP CONSTRAINT "restore old sql_saga dumps per doc/upgrade.md";
    UPDATE sql_saga.era SET catalog_format = '1.8';
    ALTER TABLE sql_saga.era
        ADD CONSTRAINT "restore old sql_saga dumps per doc/upgrade.md" CHECK (catalog_format IS NOT NULL AND catalog_format = '1.8');
    RAISE NOTICE 'sql_saga: Set catalog_format to 1.8 on % era rows of table sql_saga.era', (SELECT pg_catalog.count(*) FROM sql_saga.era);
END;
$catalog_format_1_8$;
