/*
 * sql_saga 1.4 update: catalog layout changes (schema fragment of devops/generate_update_scripts.py, 1.4 delta).
 * Runs on a catalog with the 1.3 layout: directly from 1.3, or after the embedded 1.3 update from 1.0, 1.1 and 1.2.
 * Kept in step with src/0b_schema.sql.
 */

/*
 * foreign_keys: ON DELETE CASCADE becomes valid for regular foreign keys, and each regular foreign key gets a
 * TRUNCATE trigger on its parent. The released CHECK that refuses CASCADE is identified by its exact definition,
 * never by guessing; anything else is refused. The new constraint is named, as in a fresh 1.4 installation.
 */
DO $foreign_keys_1_4$
DECLARE
    released CONSTANT text := 'CHECK ((delete_action <> ALL (ARRAY[''CASCADE''::sql_saga.fk_actions, ''SET NULL''::sql_saga.fk_actions, ''SET DEFAULT''::sql_saga.fk_actions])))';
    found name[];
BEGIN
    SELECT pg_catalog.array_agg(c.conname ORDER BY c.conname COLLATE "C") INTO found
    FROM pg_catalog.pg_constraint AS c
    WHERE c.conrelid = 'sql_saga.foreign_keys'::pg_catalog.regclass AND c.contype = 'c'
      AND pg_catalog.pg_get_constraintdef(c.oid) = released;
    IF found IS DISTINCT FROM ARRAY['foreign_keys_delete_action_check']::name[] THEN
        RAISE EXCEPTION 'sql_saga: expected exactly one released delete_action CHECK constraint on sql_saga.foreign_keys, found %',
            coalesce(pg_catalog.array_to_string(found, ', '), 'none')
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Restore the sql_saga catalog tables as the installed release created them, then retry the update.';
    END IF;
    ALTER TABLE sql_saga.foreign_keys
        DROP CONSTRAINT foreign_keys_delete_action_check,
        ADD CONSTRAINT foreign_keys_delete_action_check CHECK (delete_action NOT IN ('SET NULL', 'SET DEFAULT')),
        ADD CONSTRAINT foreign_keys_delete_action_regular_only CHECK (delete_action <> 'CASCADE' OR type = 'regular_to_temporal'),
        ADD COLUMN uk_truncate_trigger name;
    RAISE NOTICE 'sql_saga: Replaced constraint foreign_keys_delete_action_check and added constraint foreign_keys_delete_action_regular_only and column uk_truncate_trigger to table sql_saga.foreign_keys';
END;
$foreign_keys_1_4$;

/*
 * era.catalog_format: 1.4 changes the foreign_keys layout, so a 1.3 dump restored directly into 1.4 (without the
 * update, which creates every regular foreign key's TRUNCATE trigger) must fail at its first era row, as a 1.0 to
 * 1.2 dump does in 1.3. The released constraint is identified by its exact definition.
 */
DO $catalog_format_1_4$
DECLARE
    released CONSTANT text := 'CHECK (((catalog_format IS NOT NULL) AND (catalog_format = ''1.3''::text)))';
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS c
                   WHERE c.conrelid = 'sql_saga.era'::pg_catalog.regclass
                     AND c.conname = 'restore old sql_saga dumps per doc/upgrade.md'
                     AND pg_catalog.pg_get_constraintdef(c.oid) = released) THEN
        RAISE EXCEPTION 'sql_saga: expected the released catalog_format constraint of sql_saga 1.3 on sql_saga.era'
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Restore the sql_saga catalog tables as the installed release created them, then retry the update.';
    END IF;
    ALTER TABLE sql_saga.era DROP CONSTRAINT "restore old sql_saga dumps per doc/upgrade.md";
    UPDATE sql_saga.era SET catalog_format = '1.4';
    ALTER TABLE sql_saga.era
        ADD CONSTRAINT "restore old sql_saga dumps per doc/upgrade.md" CHECK (catalog_format IS NOT NULL AND catalog_format = '1.4');
    RAISE NOTICE 'sql_saga: Set catalog_format to 1.4 on % era rows of table sql_saga.era', (SELECT pg_catalog.count(*) FROM sql_saga.era);
END;
$catalog_format_1_4$;
