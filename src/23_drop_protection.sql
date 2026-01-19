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

        -- 1. For FKs that reference the dropped table, drop their triggers.
        -- The metadata will be cleaned up via ON DELETE CASCADE from the unique key.
        DECLARE
            fk_row record;
            fk_table_oid regclass;
        BEGIN
            FOR fk_row IN
                SELECT fk.*
                FROM sql_saga.foreign_keys fk
                JOIN sql_saga.unique_keys uk ON fk.unique_key_name = uk.unique_key_name
                WHERE (uk.table_schema, uk.table_name) = (r.schema_name, r.object_name)
            LOOP
                fk_table_oid := pg_catalog.to_regclass(format('%I.%I', fk_row.table_schema /* %I */, fk_row.table_name /* %I */));
                -- The referencing table might have been dropped in the same CASCADE. If it still exists, drop its triggers/constraints.
                IF fk_table_oid IS NOT NULL THEN
                    IF fk_row.type = 'regular_to_temporal' THEN
                        IF fk_row.fk_check_constraint IS NOT NULL AND EXISTS (SELECT 1 FROM pg_catalog.pg_constraint WHERE conrelid = fk_table_oid AND conname = fk_row.fk_check_constraint) THEN
                            EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', fk_table_oid /* %s */, fk_row.fk_check_constraint /* %I */);
                        END IF;
                        IF fk_row.fk_helper_function IS NOT NULL THEN
                           EXECUTE format('DROP FUNCTION IF EXISTS %s', fk_row.fk_helper_function /* %s */);
                        END IF;
                    END IF;
                END IF;
            END LOOP;
        END;

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
                uk_table_oid := pg_catalog.to_regclass(format('%I.%I', fk_row.uk_schema /* %I */, fk_row.uk_table /* %I */));
                -- Use DROP IF EXISTS because the UK table might have been dropped in the same command.
                IF uk_table_oid IS NOT NULL THEN
                    IF EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = uk_table_oid AND tgname = fk_row.uk_update_trigger) THEN
                        EXECUTE format('DROP TRIGGER %I ON %s', fk_row.uk_update_trigger /* %I */, uk_table_oid /* %s */);
                    END IF;
                    IF EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = uk_table_oid AND tgname = fk_row.uk_delete_trigger) THEN
                        EXECUTE format('DROP TRIGGER %I ON %s', fk_row.uk_delete_trigger /* %I */, uk_table_oid /* %s */);
                    END IF;
                END IF;
            END LOOP;
        END;

        -- 3. Clean up template sync triggers/functions for each era on this table BEFORE deleting metadata
        DECLARE
            era_row record;
            func_name name;
        BEGIN
            -- Table has been dropped, but we still need to clean up the functions
            FOR era_row IN 
                SELECT e.era_name, e.sync_temporal_trg_function_name
                FROM sql_saga.era e 
                WHERE (e.table_schema, e.table_name) = (r.schema_name, r.object_name)
                  AND e.sync_temporal_trg_function_name IS NOT NULL  -- Only eras with sync triggers
            LOOP
                -- Drop the function directly (trigger already gone with table)
                func_name := era_row.sync_temporal_trg_function_name;
                IF to_regprocedure(format('sql_saga.%I()', func_name)) IS NOT NULL THEN
                    EXECUTE format($$DROP FUNCTION sql_saga.%I() CASCADE$$, func_name);
                END IF;
            END LOOP;
        END;
        
        -- 4. Delete all metadata for the dropped table.
        DELETE FROM sql_saga.updatable_view WHERE (table_schema, table_name) = (r.schema_name, r.object_name);
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
    --- updatable_view
    ---

    /* Reject dropping any of our updatable views directly. */
    FOR r IN
        SELECT dobj.object_identity, v.view_type
        FROM sql_saga.updatable_view AS v
        JOIN pg_catalog.pg_event_trigger_dropped_objects() AS dobj
            ON dobj.object_type = 'view'
            AND (dobj.address_names[1], dobj.address_names[2]) = (v.view_schema, v.view_name)
    LOOP
        RAISE EXCEPTION 'cannot drop view "%", call "sql_saga.drop_%_view()" instead',
            r.object_identity, r.view_type;
    END LOOP;

    /* Complain if the updatable view's trigger is missing. */
    FOR r IN
        SELECT format('%I.%I', v.table_schema, v.table_name)::regclass AS table_oid, v.era_name, format('%I.%I', v.view_schema, v.view_name)::regclass as view_oid, v.trigger_name
        FROM sql_saga.updatable_view AS v
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', v.view_schema, v.view_name)), v.trigger_name))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on view "%" because it is part of an updatable view for era "%" on table "%"',
            r.trigger_name, r.view_oid, r.era_name, r.table_oid;
    END LOOP;

    /* Complain if the table's identifier (PK or unique key) has been dropped. */
    FOR r IN
        SELECT to_regclass(format('%I.%I', v.table_schema, v.table_name)) AS table_oid, v.era_name
        FROM sql_saga.updatable_view AS v
        WHERE
            -- Check for a Primary Key
            NOT EXISTS (
                SELECT FROM pg_catalog.pg_constraint AS c
                WHERE (c.conrelid, c.contype) = (to_regclass(format('%I.%I', v.table_schema, v.table_name)), 'p')
            )
            -- Check for a single-column temporal unique key
            AND NOT EXISTS (
                SELECT FROM sql_saga.unique_keys uk
                WHERE (uk.table_schema, uk.table_name, uk.era_name) = (v.table_schema, v.table_name, v.era_name)
                  AND array_length(uk.column_names, 1) = 1
            )
    LOOP
        RAISE EXCEPTION 'table "%" must have a primary key or a single-column temporal unique key to support its updatable view for era "%"',
            r.table_oid, r.era_name;
    END LOOP;

    ---
    --- unique_keys
    ---

    /*
     * We don't need to protect the individual columns as long as we protect
     * the indexes.  PostgreSQL will make sure they stick around.
     */

    /* Complain if the objects implementing our unique keys are missing. */
    FOR r IN
        SELECT uk.*, to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) AS table_oid
        FROM sql_saga.unique_keys AS uk
        -- Only check tables that still exist.
        WHERE pg_catalog.to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) IS NOT NULL
    LOOP
        RAISE DEBUG 'drop_protection: checking unique_key %', row_to_json(r);
        -- If the table this key belongs to is being dropped, skip protection checks.
        IF EXISTS (
            SELECT 1 FROM pg_event_trigger_dropped_objects() dobj
            WHERE dobj.object_type = 'table'
              AND (dobj.schema_name, dobj.object_name) = (r.table_schema, r.table_name)
        ) THEN
            CONTINUE;
        END IF;

        -- Check unique constraint or unique index(es)
        IF r.mutually_exclusive_columns IS NOT NULL THEN
            -- For XOR keys, check_constraint is the CHECK constraint.
            IF r.check_constraint IS NOT NULL AND NOT EXISTS (
                SELECT FROM pg_catalog.pg_constraint AS c
                WHERE (c.conrelid, c.conname) = (r.table_oid, r.check_constraint)
            ) THEN
                RAISE EXCEPTION 'cannot drop constraint "%" on table "%" because it is used in era unique key "%"',
                    r.check_constraint, r.table_oid, r.unique_key_name;
            END IF;

            -- Mutually exclusive keys also have multiple partial indexes and constraints.
            IF r.partial_exclude_constraint_names IS NOT NULL THEN
                DECLARE
                    v_constraint_name name;
                BEGIN
                    FOREACH v_constraint_name IN ARRAY r.partial_exclude_constraint_names
                    LOOP
                        IF NOT EXISTS (
                            SELECT FROM pg_catalog.pg_constraint AS c
                            WHERE (c.conrelid, c.conname) = (r.table_oid, v_constraint_name)
                        ) THEN
                            RAISE EXCEPTION 'cannot drop constraint "%" on table "%" because it is used in era unique key "%"',
                                v_constraint_name, r.table_oid, r.unique_key_name;
                        END IF;
                    END LOOP;
                END;
            END IF;

            IF r.partial_index_names IS NOT NULL THEN
                DECLARE
                    v_index_name name;
                BEGIN
                    FOREACH v_index_name IN ARRAY r.partial_index_names
                    LOOP
                        IF pg_catalog.to_regclass(format('%I.%I', r.table_schema, v_index_name)) IS NULL THEN
                            RAISE EXCEPTION 'cannot drop index "%" on table "%" because it is used in era unique key "%"',
                                v_index_name, r.table_oid, r.unique_key_name;
                        END IF;
                    END LOOP;
                END;
            END IF;
        ELSIF r.predicate IS NOT NULL THEN
            IF r.unique_constraint IS NOT NULL THEN
                -- Predicated keys use a unique index, not a constraint.
                IF pg_catalog.to_regclass(format('%I.%I', r.table_schema, r.unique_constraint)) IS NULL THEN
                    RAISE EXCEPTION 'cannot drop index "%" on table "%" because it is used in era unique key "%"',
                        r.unique_constraint, r.table_oid, r.unique_key_name;
                END IF;
            END IF;
        ELSE
            -- Standard keys use a unique constraint.
            IF NOT EXISTS (
                SELECT FROM pg_catalog.pg_constraint AS c
                WHERE (c.conrelid, c.conname) = (r.table_oid, r.unique_constraint)
            ) THEN
                RAISE EXCEPTION 'cannot drop constraint "%" on table "%" because it is used in era unique key "%"',
                    r.unique_constraint, r.table_oid, r.unique_key_name;
            END IF;
        END IF;

        -- Check exclude constraint for non-XOR keys
        IF r.exclude_constraint IS NOT NULL AND NOT EXISTS (
            SELECT FROM pg_catalog.pg_constraint AS c
            WHERE (c.conrelid, c.conname) = (r.table_oid, r.exclude_constraint)
        ) THEN
            RAISE EXCEPTION 'cannot drop constraint "%" on table "%" because it is used in era unique key "%"',
                r.exclude_constraint, r.table_oid, r.unique_key_name;
        END IF;

        -- Check pk_consistency constraints
        IF r.pk_consistency_constraint_names IS NOT NULL AND r.pk_consistency_constraint_names <> '{}' THEN
            DECLARE
                v_constraint_name name;
            BEGIN
                FOREACH v_constraint_name IN ARRAY r.pk_consistency_constraint_names
                LOOP
                    IF NOT EXISTS (
                        SELECT FROM pg_catalog.pg_constraint AS c
                        WHERE (c.conrelid, c.conname) = (r.table_oid, v_constraint_name)
                    ) THEN
                        RAISE EXCEPTION 'cannot drop constraint "%" on table "%" because it is used in era unique key "%"',
                            v_constraint_name, r.table_oid, r.unique_key_name;
                    END IF;
                END LOOP;
            END;
        END IF;
    END LOOP;

    ---
    --- foreign_keys
    ---

    /* Complain if any of the uk triggers are missing */
    FOR r IN
        SELECT fk.foreign_key_name, to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) AS table_oid, fk.uk_update_trigger
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        WHERE to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) IS NOT NULL
        AND fk.uk_update_trigger IS NOT NULL  -- Skip native FKs (PG18+) which have no triggers
        AND NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_trigger AS t
            WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', uk.table_schema, uk.table_name)), fk.uk_update_trigger))
    LOOP
        RAISE EXCEPTION 'cannot drop trigger "%" on table "%" because it is used in era foreign key "%"',
            r.uk_update_trigger, r.table_oid, r.foreign_key_name;
    END LOOP;

    FOR r IN
        SELECT fk.foreign_key_name, to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) AS table_oid, fk.uk_delete_trigger
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        WHERE to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) IS NOT NULL
        AND fk.uk_delete_trigger IS NOT NULL  -- Skip native FKs (PG18+) which have no triggers
        AND NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_trigger AS t
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

COMMENT ON FUNCTION sql_saga.drop_protection() IS
'An event trigger function that prevents accidental dropping of sql_saga-managed objects.';
