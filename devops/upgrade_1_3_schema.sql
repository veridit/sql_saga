/*
 * sql_saga 1.3 update: catalog layout changes (schema fragment of devops/generate_update_scripts.py). Runs after
 * the frozen historical update (1.0 and 1.1 sources) and before the generator emits the new pending table from
 * src/2o and the 1.3 routines. The same text serves all three sources: on every one of them the catalog has the
 * 1.2 layout by now. Kept in step with src/0b_schema.sql.
 */

/*
 * era.catalog_format: the release that introduced the current catalog layout. No default, on purpose (a dump of
 * another layout restored into 1.3 fails at its first era row), so the existing rows are set explicitly.
 */
ALTER TABLE sql_saga.era ADD COLUMN catalog_format text;
UPDATE sql_saga.era SET catalog_format = '1.3';
ALTER TABLE sql_saga.era
    ADD CONSTRAINT "restore old sql_saga dumps per doc/upgrade.md" CHECK (catalog_format IS NOT NULL AND catalog_format = '1.3');

/*
 * foreign_keys: a 1.3 regular foreign key has no helper function (its referencing side is a constraint trigger
 * named fk_check_constraint), so the per-type CHECK no longer requires fk_helper_function. The unnamed released
 * constraint is identified by its exact definition, never by guessing: anything else is refused.
 */
DO $foreign_keys_type_check$
DECLARE
    -- pg_get_constraintdef of the released constraint (PostgreSQL 18 deparses CASE over several lines).
    released CONSTANT text := pg_catalog.concat_ws(pg_catalog.chr(10),
        'CHECK (',
        'CASE type',
        '    WHEN ''temporal_to_temporal''::sql_saga.fg_type THEN ((fk_era_name IS NOT NULL) AND (fk_check_constraint IS NULL) AND (fk_helper_function IS NULL))',
        '    WHEN ''regular_to_temporal''::sql_saga.fg_type THEN ((fk_era_name IS NULL) AND (fk_check_constraint IS NOT NULL) AND (fk_helper_function IS NOT NULL))',
        '    ELSE NULL::boolean',
        'END)');
    found name[];
BEGIN
    SELECT pg_catalog.array_agg(c.conname ORDER BY c.conname COLLATE "C") INTO found
    FROM pg_catalog.pg_constraint AS c
    WHERE c.conrelid = 'sql_saga.foreign_keys'::pg_catalog.regclass AND c.contype = 'c'
      AND pg_catalog.pg_get_constraintdef(c.oid) = released;
    -- PostgreSQL named it foreign_keys_check in every release, as in a fresh 1.3 install.
    IF found IS DISTINCT FROM ARRAY['foreign_keys_check']::name[] THEN
        RAISE EXCEPTION 'sql_saga: expected exactly one released type CHECK constraint on sql_saga.foreign_keys, found %',
            coalesce(pg_catalog.array_to_string(found, ', '), 'none')
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Restore the sql_saga catalog tables as the installed release created them, then retry the update.';
    END IF;
    -- Same name, the definition of src/0b_schema.sql.
    EXECUTE pg_catalog.format($sql$
        ALTER TABLE sql_saga.foreign_keys DROP CONSTRAINT %1$I, ADD CONSTRAINT %1$I CHECK (
            CASE type
                WHEN 'temporal_to_temporal' THEN
                    fk_era_name IS NOT NULL
                    AND fk_check_constraint IS NULL AND fk_helper_function IS NULL
                WHEN 'regular_to_temporal' THEN
                    fk_era_name IS NULL
                    AND fk_check_constraint IS NOT NULL
            END)
        $sql$, found[1]);
    RAISE NOTICE 'sql_saga: Added column catalog_format (set to 1.3 on % era rows) and constraint "restore old sql_saga dumps per doc/upgrade.md" to table sql_saga.era, and replaced constraint foreign_keys_check of table sql_saga.foreign_keys so a regular foreign key needs no helper function',
        (SELECT pg_catalog.count(*) FROM sql_saga.era);
END;
$foreign_keys_type_check$;
