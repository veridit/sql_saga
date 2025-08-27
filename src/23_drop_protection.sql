CREATE FUNCTION sql_saga.drop_protection()
 RETURNS event_trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
AS
$function$
#variable_conflict use_variable
DECLARE
    r record;
    table_oid regclass;
    era_name name;
BEGIN
    /*
     * This function is called after the fact, so we have to just look to see
     * if anything is missing in the catalogs if we just store the name and not
     * a reg* type.
     */

    ---
    --- periods
    ---

    /* If one of our tables is being dropped, remove references to it */
    FOR r IN
        SELECT dobj.schema_name, dobj.object_name
        FROM pg_catalog.pg_event_trigger_dropped_objects() AS dobj
        WHERE dobj.object_type = 'table'
          AND EXISTS (SELECT 1 FROM sql_saga.era e WHERE (e.table_schema, e.table_name) = (dobj.schema_name, dobj.object_name))
    LOOP
        -- This table was dropped. Cascade-delete its metadata and any dependent triggers on other tables.
        -- This logic mirrors drop_era(..., 'CASCADE', true) for a dropped table.

        -- 1. For FKs that reference the dropped table, drop them completely.
        -- We can call drop_foreign_key because it can look up the FK table by name and drop its triggers.
        PERFORM sql_saga.drop_foreign_key_by_name(
            format('%I.%I', fk.table_schema, fk.table_name)::regclass,
            fk.foreign_key_name
        )
        FROM sql_saga.foreign_keys fk
        JOIN sql_saga.unique_keys uk ON fk.unique_key_name = uk.unique_key_name
        WHERE (uk.table_schema, uk.table_name) = (r.schema_name, r.object_name);

        -- 2. For FKs ON the dropped table, remove triggers from the referenced UK table.
        DECLARE
            fk_row record;
            uk_table_oid regclass;
        BEGIN
            FOR fk_row IN
                SELECT fk.*, uk.table_schema AS uk_schema, uk.table_name AS uk_table
                FROM sql_saga.foreign_keys fk JOIN sql_saga.unique_keys uk ON fk.unique_key_name = uk.unique_key_name
                WHERE (fk.table_schema, fk.table_name) = (r.schema_name, r.object_name)
            LOOP
                uk_table_oid := format('%I.%I', fk_row.uk_schema, fk_row.uk_table)::regclass;
                -- Use DROP IF EXISTS because the UK table might have been dropped in the same command.
                IF pg_catalog.to_regclass(uk_table_oid::text) IS NOT NULL THEN
                    IF EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = uk_table_oid AND tgname = fk_row.uk_update_trigger) THEN
                        EXECUTE format('DROP TRIGGER %I ON %s', fk_row.uk_update_trigger, uk_table_oid);
                    END IF;
                    IF EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = uk_table_oid AND tgname = fk_row.uk_delete_trigger) THEN
                        EXECUTE format('DROP TRIGGER %I ON %s', fk_row.uk_delete_trigger, uk_table_oid);
                    END IF;
                END IF;
            END LOOP;
        END;

        -- 3. Delete all metadata for the dropped table.
        DELETE FROM sql_saga.api_view WHERE (table_schema, table_name) = (r.schema_name, r.object_name);
        DELETE FROM sql_saga.foreign_keys WHERE (table_schema, table_name) = (r.schema_name, r.object_name);
        DELETE FROM sql_saga.unique_keys WHERE (table_schema, table_name) = (r.schema_name, r.object_name);
        DELETE FROM sql_saga.era WHERE (table_schema, table_name) = (r.schema_name, r.object_name);
    END LOOP;

    /*
     * If a column belonging to one of our periods is dropped, we need to reject that.
     * SQL:2016 11.23 SR 6
     */
    FOR r IN
        SELECT dobj.object_identity, e.era_name
        FROM sql_saga.era AS e
        JOIN pg_catalog.pg_event_trigger_dropped_objects() AS dobj
            ON dobj.object_type = 'table column'
            AND dobj.address_names[1] = e.table_schema
            AND dobj.address_names[2] = e.table_name
            AND dobj.address_names[3] IN (e.valid_from_column_name, e.valid_until_column_name)
    LOOP
        RAISE EXCEPTION 'cannot drop column "%" because it is part of the period "%"',
            r.object_identity, r.era_name;
    END LOOP;

    /* Also reject dropping the rangetype */
    FOR r IN
        SELECT dobj.object_identity, format('%I.%I', e.table_schema, e.table_name)::regclass AS table_oid, e.era_name
        FROM sql_saga.era AS e
        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
                ON dobj.objid = e.range_type
        ORDER BY dobj.ordinality
    LOOP
        RAISE EXCEPTION 'cannot drop rangetype "%" because it is used in period "%" on table "%"',
            r.object_identity, r.era_name, r.table_oid;
    END LOOP;

    --/* Complain if the infinity CHECK constraint is missing. */
    --FOR r IN
    --    SELECT p.table_name, p.infinity_check_constraint
    --    FROM sql_saga.system_time_periods AS p
    --    WHERE NOT EXISTS (
    --        SELECT FROM pg_catalog.pg_constraint AS c
    --        WHERE (c.conrelid, c.conname) = (p.table_name, p.infinity_check_constraint))
    --LOOP
    --    RAISE EXCEPTION 'cannot drop constraint "%" on table "%" because it is used in SYSTEM_TIME period',
    --        r.infinity_check_constraint, r.table_oid;
    --END LOOP;

    /* Complain if the GENERATED ALWAYS AS ROW START/END trigger is missing. */
    --FOR r IN
    --    SELECT p.table_name, p.generated_always_trigger
    --    FROM sql_saga.system_time_periods AS p
    --    WHERE NOT EXISTS (
    --        SELECT FROM pg_catalog.pg_trigger AS t
    --        WHERE (t.tgrelid, t.tgname) = (p.table_name, p.generated_always_trigger))
    --LOOP
    --    RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in SYSTEM_TIME period',
    --        r.generated_always_trigger, r.table_oid;
    --END LOOP;

    /* Complain if the write_history trigger is missing. */
    -- FOR r IN
    --     SELECT p.table_name, p.write_history_trigger
    --     FROM sql_saga.system_time_periods AS p
    --     WHERE NOT EXISTS (
    --         SELECT FROM pg_catalog.pg_trigger AS t
    --         WHERE (t.tgrelid, t.tgname) = (p.table_name, p.write_history_trigger))
    -- LOOP
    --     RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in SYSTEM_TIME period',
    --         r.write_history_trigger, r.table_oid;
    -- END LOOP;

    /* Complain if the TRUNCATE trigger is missing. */
    --FOR r IN
    --    SELECT p.table_name, p.truncate_trigger
    --    FROM sql_saga.system_time_periods AS p
    --    WHERE NOT EXISTS (
    --        SELECT FROM pg_catalog.pg_trigger AS t
    --        WHERE (t.tgrelid, t.tgname) = (p.table_name, p.truncate_trigger))
    --LOOP
    --    RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in SYSTEM_TIME period',
    --        r.truncate_trigger, r.table_oid;
    --END LOOP;

    /*
     * We can't reliably find out what a column was renamed to, so just error
     * out in this case.
     */
--    FOR r IN
--        SELECT stp.table_name, u.column_name
--        FROM sql_saga.era AS stp
--        CROSS JOIN LATERAL unnest(stp.excluded_column_names) AS u (column_name)
--        WHERE NOT EXISTS (
--            SELECT FROM pg_catalog.pg_attribute AS a
--            WHERE (a.attrelid, a.attname) = (stp.table_name, u.column_name))
--    LOOP
--        RAISE EXCEPTION 'cannot drop or rename column "%" on table "%" because it is excluded from an era',
--            r.column_name, r.table_oid;
--    END LOOP;

    ---
    --- api_view
    ---

    /* Reject dropping the FOR PORTION OF view. */
    FOR r IN
        SELECT dobj.object_identity
        FROM sql_saga.api_view AS fpv
        JOIN pg_catalog.pg_event_trigger_dropped_objects() AS dobj
            ON dobj.object_type = 'view'
            AND (dobj.address_names[1], dobj.address_names[2]) = (fpv.view_schema_name, fpv.view_table_name)
    LOOP
        RAISE EXCEPTION 'cannot drop view "%", call "sql_saga.drop_api()" instead',
            r.object_identity;
    END LOOP;

    /* Complain if the FOR PORTION OF trigger is missing. */
    FOR r IN
        SELECT format('%I.%I', fpv.table_schema, fpv.table_name)::regclass AS table_oid, fpv.era_name, format('%I.%I', fpv.view_schema_name, fpv.view_table_name)::regclass as view_oid, fpv.trigger_name
        FROM sql_saga.api_view AS fpv
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', fpv.view_schema_name, fpv.view_table_name)), fpv.trigger_name))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on view "%" because it is used in FOR PORTION OF view for period "%" on table "%"',
            r.trigger_name, r.view_oid, r.era_name, r.table_oid;
    END LOOP;

    /* Complain if the table's primary key has been dropped. */
    FOR r IN
        SELECT to_regclass(format('%I.%I', fpv.table_schema, fpv.table_name)) AS table_oid, fpv.era_name
        FROM sql_saga.api_view AS fpv
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.contype) = (to_regclass(format('%I.%I', fpv.table_schema, fpv.table_name)), 'p'))
    LOOP
        RAISE EXCEPTION 'cannot drop primary key on table "%" because it has a FOR PORTION OF view for period "%"',
            r.table_oid, r.era_name;
    END LOOP;

    ---
    --- unique_keys
    ---

    /*
     * We don't need to protect the individual columns as long as we protect
     * the indexes.  PostgreSQL will make sure they stick around.
     */

    /* Complain if the indexes implementing our unique indexes are missing. */
    FOR r IN
        SELECT uk.unique_key_name, to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) AS table_oid, uk.unique_constraint
        FROM sql_saga.unique_keys AS uk
        WHERE pg_catalog.to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) IS NOT NULL
          AND NOT EXISTS (
            SELECT FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.conname) = (to_regclass(format('%I.%I', uk.table_schema, uk.table_name)), uk.unique_constraint))
    LOOP
        RAISE EXCEPTION 'cannot drop constraint "%" on table "%" because it is used in era unique key "%"',
            r.unique_constraint, r.table_oid, r.unique_key_name;
    END LOOP;

    FOR r IN
        SELECT uk.unique_key_name, to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) AS table_oid, uk.exclude_constraint
        FROM sql_saga.unique_keys AS uk
        WHERE pg_catalog.to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) IS NOT NULL
          AND NOT EXISTS (
            SELECT FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.conname) = (to_regclass(format('%I.%I', uk.table_schema, uk.table_name)), uk.exclude_constraint))
    LOOP
        RAISE EXCEPTION 'cannot drop constraint "%" on table "%" because it is used in era unique key "%"',
            r.exclude_constraint, r.table_oid, r.unique_key_name;
    END LOOP;

    ---
    --- foreign_keys
    ---

    /* Complain if any of the triggers are missing */
    FOR r IN
        SELECT fk.foreign_key_name, to_regclass(format('%I.%I', fk.table_schema, fk.table_name)) AS table_oid, fk.fk_insert_trigger
        FROM sql_saga.foreign_keys AS fk
        WHERE fk.type = 'temporal_to_temporal' AND NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', fk.table_schema, fk.table_name)), fk.fk_insert_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in era foreign key "%"',
            r.fk_insert_trigger, r.table_oid, r.foreign_key_name;
    END LOOP;

    FOR r IN
        SELECT fk.foreign_key_name, to_regclass(format('%I.%I', fk.table_schema, fk.table_name)) AS table_oid, fk.fk_update_trigger
        FROM sql_saga.foreign_keys AS fk
        WHERE fk.type = 'temporal_to_temporal' AND NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', fk.table_schema, fk.table_name)), fk.fk_update_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in era foreign key "%"',
            r.fk_update_trigger, r.table_oid, r.foreign_key_name;
    END LOOP;

    FOR r IN
        SELECT fk.foreign_key_name, to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) AS table_oid, fk.uk_update_trigger
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', uk.table_schema, uk.table_name)), fk.uk_update_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in era foreign key "%"',
            r.uk_update_trigger, r.table_oid, r.foreign_key_name;
    END LOOP;

    FOR r IN
        SELECT fk.foreign_key_name, to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) AS table_oid, fk.uk_delete_trigger
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', uk.table_schema, uk.table_name)), fk.uk_delete_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in era foreign key "%"',
            r.uk_delete_trigger, r.table_oid, r.foreign_key_name;
    END LOOP;

    ---
    --- system_versioning
    ---

--    FOR r IN
--        SELECT dobj.object_identity, sv.table_name
--        FROM sql_saga.system_versioning AS sv
--        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
--                ON dobj.objid = sv.audit_table_name
--        WHERE dobj.object_type = 'table'
--        ORDER BY dobj.ordinality
--    LOOP
--        RAISE EXCEPTION 'cannot drop table "%" because it is used in SYSTEM VERSIONING for table "%"',
--            r.object_identity, r.table_oid;
--    END LOOP;
--
--    FOR r IN
--        SELECT dobj.object_identity, sv.table_name
--        FROM sql_saga.system_versioning AS sv
--        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
--                ON dobj.objid = sv.view_oid
--        WHERE dobj.object_type = 'view'
--        ORDER BY dobj.ordinality
--    LOOP
--        RAISE EXCEPTION 'cannot drop view "%" because it is used in SYSTEM VERSIONING for table "%"',
--            r.object_identity, r.table_oid;
--    END LOOP;
--
--    FOR r IN
--        SELECT dobj.object_identity, sv.table_name
--        FROM sql_saga.system_versioning AS sv
--        JOIN pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj
--                ON dobj.object_identity = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to])
--        WHERE dobj.object_type = 'function'
--        ORDER BY dobj.ordinality
--    LOOP
--        RAISE EXCEPTION 'cannot drop function "%" because it is used in SYSTEM VERSIONING for table "%"',
--            r.object_identity, r.table_oid;
--    END LOOP;
END;
$function$;
