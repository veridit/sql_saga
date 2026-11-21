/*
 * sql_saga 1.9 update: catalog layout changes (schema fragment of devops/generate_update_scripts_1_9.py).
 * Runs on a catalog with the 1.8 layout: directly from 1.8, or after the embedded update from an older release. Kept
 * in step with src/0b_schema.sql. The generator places it after the 1.9 routines.
 */

/*
 * era.provenance_columns (doc/design/provenance.md): NULL for every existing era. Appended, which is why src/0b_schema.sql
 * declares it after catalog_format: a fresh install and an updated one have the same column order.
 */
DO $era_provenance_columns_1_9$
BEGIN
    IF EXISTS (SELECT FROM pg_catalog.pg_attribute AS a
               WHERE a.attrelid = 'sql_saga.era'::pg_catalog.regclass AND a.attname = 'provenance_columns' AND NOT a.attisdropped) THEN
        RAISE EXCEPTION 'sql_saga: sql_saga.era already has a column provenance_columns, which the 1.9 update adds'
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Restore the sql_saga catalog tables as the installed release created them, then retry the update.';
    END IF;
    ALTER TABLE sql_saga.era ADD COLUMN provenance_columns name[] DEFAULT NULL;
    RAISE NOTICE 'sql_saga: Added the column provenance_columns to table sql_saga.era';
END;
$era_provenance_columns_1_9$;

/*
 * era.catalog_format: the era layout changes, so a 1.8 dump restored directly into 1.9 must fail at its first era
 * row. The released constraint is identified by its exact definition.
 */
DO $catalog_format_1_9$
DECLARE
    released CONSTANT text := 'CHECK (((catalog_format IS NOT NULL) AND (catalog_format = ''1.8''::text)))';
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS c
                   WHERE c.conrelid = 'sql_saga.era'::pg_catalog.regclass
                     AND c.conname = 'restore old sql_saga dumps per doc/upgrade.md'
                     AND pg_catalog.pg_get_constraintdef(c.oid) = released) THEN
        RAISE EXCEPTION 'sql_saga: expected the released catalog_format constraint of sql_saga 1.8 on sql_saga.era'
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Restore the sql_saga catalog tables as the installed release created them, then retry the update.';
    END IF;
    ALTER TABLE sql_saga.era DROP CONSTRAINT "restore old sql_saga dumps per doc/upgrade.md";
    UPDATE sql_saga.era SET catalog_format = '1.9';
    ALTER TABLE sql_saga.era
        ADD CONSTRAINT "restore old sql_saga dumps per doc/upgrade.md" CHECK (catalog_format IS NOT NULL AND catalog_format = '1.9');
    RAISE NOTICE 'sql_saga: Set catalog_format to 1.9 on % era rows of table sql_saga.era', (SELECT pg_catalog.count(*) FROM sql_saga.era);
END;
$catalog_format_1_9$;
