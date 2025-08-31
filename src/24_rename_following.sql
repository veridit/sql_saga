CREATE FUNCTION sql_saga.rename_following()
 RETURNS event_trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 -- Set a safe search path for this SECURITY DEFINER function
 SET search_path = sql_saga, pg_catalog, public
AS
$function$
#variable_conflict use_variable
DECLARE
    r record;
    sql text;
    v_is_alter_trigger boolean := false;
    is_alter_table boolean := false;
    cmd record;
BEGIN
    FOR cmd IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        IF cmd.command_tag = 'ALTER TABLE' THEN
            is_alter_table := true;
            EXIT;
        END IF;
    END LOOP;

    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        IF r.command_tag = 'ALTER TRIGGER' THEN
            v_is_alter_trigger := true;
            exit;
        END IF;
    END LOOP;

    -- Exit early if the DDL command does not affect a managed object.
    -- We bypass this for ALTER TRIGGER because the object identity contains
    -- the *new* trigger name, which won't be in our metadata yet.
    IF NOT v_is_alter_trigger AND NOT sql_saga.__internal_ddl_command_affects_managed_object() THEN
        RETURN;
    END IF;
    /*
     * Anything that is stored by reg* type will auto-adjust, but anything we
     * store by name will need to be updated after a rename. One way to do this
     * is to recreate the constraints we have and pull new names out that way.
     * If we are unable to do something like that, we must raise an exception.
     */

    ---
    --- era
    ---

    /*
     * Start and end columns of an era can be found by the bounds check
     * constraint.
     */
    FOR sql IN
        SELECT pg_catalog.format('UPDATE sql_saga.era SET valid_from_column_name = %L, valid_until_column_name = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            sa.attname, ea.attname, e.table_schema, e.table_name, e.era_name)
        FROM sql_saga.era AS e
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (e.table_name, (SELECT oid FROM pg_namespace WHERE nspname = e.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON (c.conrelid, c.conname) = (pc.oid, e.bounds_check_constraint)
        JOIN pg_catalog.pg_attribute AS sa ON sa.attrelid = pc.oid
        JOIN pg_catalog.pg_attribute AS ea ON ea.attrelid = pc.oid
        WHERE (e.valid_from_column_name, e.valid_until_column_name) <> (sa.attname, ea.attname)
          AND pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((%I < %I))', sa.attname, ea.attname)
    LOOP
        EXECUTE sql;
    END LOOP;

    /*
     * Inversely, the bounds check constraint can be retrieved via the start
     * and end columns.
     */
    FOR sql IN
        SELECT pg_catalog.format('UPDATE sql_saga.era SET bounds_check_constraint = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            c.conname, e.table_schema, e.table_name, e.era_name)
        FROM sql_saga.era AS e
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (e.table_name, (SELECT oid FROM pg_namespace WHERE nspname = e.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        JOIN pg_catalog.pg_attribute AS sa ON sa.attrelid = pc.oid
        JOIN pg_catalog.pg_attribute AS ea ON ea.attrelid = pc.oid
        WHERE e.bounds_check_constraint <> c.conname
          AND pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((%I < %I))', sa.attname, ea.attname)
          AND (e.valid_from_column_name, e.valid_until_column_name) = (sa.attname, ea.attname)
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, e.bounds_check_constraint))
    LOOP
        EXECUTE sql;
    END LOOP;

    --    FOR sql IN
    --        SELECT pg_catalog.format('UPDATE sql_saga.system_time_periods SET infinity_check_constraint = %L WHERE table_name = %L::regclass',
    --            c.conname, p.table_name)
    --        FROM sql_saga.era AS p
    --        JOIN sql_saga.system_time_periods AS stp ON (stp.table_name, stp.era_name) = (p.table_name, p.era_name)
    --        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = p.table_name
    --        JOIN pg_catalog.pg_attribute AS ea ON ea.attrelid = p.table_name
    --        WHERE stp.infinity_check_constraint <> c.conname
    --          AND pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((%I = ''infinity''::%s))', ea.attname, format_type(ea.atttypid, ea.atttypmod))
    --          AND p.stop_on_column_name = ea.attname
    --          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (stp.table_name, stp.infinity_check_constraint))
    --    LOOP
    --        EXECUTE sql;
    --    END LOOP;
    --
    --    FOR sql IN
    --        SELECT pg_catalog.format('UPDATE sql_saga.system_time_periods SET generated_always_trigger = %L WHERE table_name = %L::regclass',
    --            t.tgname, stp.table_name)
    --        FROM sql_saga.system_time_periods AS stp
    --        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = stp.table_name
    --        WHERE t.tgname <> stp.generated_always_trigger
    --          AND t.tgfoid = 'sql_saga.generated_always_as_row_start_end()'::regprocedure
    --          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (stp.table_name, stp.generated_always_trigger))
    --    LOOP
    --        EXECUTE sql;
    --    END LOOP;
    --
    --    FOR sql IN
    --        SELECT pg_catalog.format('UPDATE sql_saga.system_time_periods SET write_history_trigger = %L WHERE table_name = %L::regclass',
    --            t.tgname, stp.table_name)
    --        FROM sql_saga.system_time_periods AS stp
    --        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = stp.table_name
    --        WHERE t.tgname <> stp.write_history_trigger
    --          AND t.tgfoid = 'sql_saga.write_history()'::regprocedure
    --          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (stp.table_name, stp.write_history_trigger))
    --    LOOP
    --        EXECUTE sql;
    --    END LOOP;
    --
    --    FOR sql IN
    --        SELECT pg_catalog.format('UPDATE sql_saga.system_time_periods SET truncate_trigger = %L WHERE table_name = %L::regclass',
    --            t.tgname, stp.table_name)
    --        FROM sql_saga.system_time_periods AS stp
    --        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = stp.table_name
    --        WHERE t.tgname <> stp.truncate_trigger
    --          AND t.tgfoid = 'sql_saga.truncate_era()'::regprocedure
    --          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (stp.table_name, stp.truncate_trigger))
    --    LOOP
    --        EXECUTE sql;
    --    END LOOP;

    /*
     * We can't reliably find out what a column was renamed to, so just error
     * out in this case.
     */
--    FOR r IN
--        SELECT stp.table_name, u.column_name
--        FROM sql_saga.system_time_periods AS stp
--        CROSS JOIN LATERAL unnest(stp.excluded_column_names) AS u (column_name)
--        WHERE NOT EXISTS (
--            SELECT FROM pg_catalog.pg_attribute AS a
--            WHERE (a.attrelid, a.attname) = (stp.table_name, u.column_name))
--    LOOP
--        RAISE EXCEPTION 'cannot drop or rename column "%" on table "%" because it is excluded from era',
--            r.column_name, r.table_oid;
--    END LOOP;

    ---
    --- updatable_view
    ---
    DECLARE
        v_view_row sql_saga.updatable_view;
        v_view_oid regclass;
        v_new_trigger_name name;
    BEGIN
        FOR v_view_row IN SELECT * FROM sql_saga.updatable_view
        LOOP
            v_view_oid := to_regclass(format('%I.%I', v_view_row.view_schema, v_view_row.view_name));

            -- Check if the registered trigger still exists on the view
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = v_view_oid AND tgname = v_view_row.trigger_name) THEN
                -- If it's stale, find the *new* trigger on that view that points to our function.
                -- This currently only supports for_portion_of, but is extensible.
                IF v_view_row.view_type = 'for_portion_of' THEN
                    SELECT tgname INTO v_new_trigger_name FROM pg_trigger
                    WHERE tgrelid = v_view_oid AND tgfoid = 'sql_saga.for_portion_of_trigger()'::regprocedure;

                    IF FOUND THEN
                        sql := format('UPDATE sql_saga.updatable_view SET trigger_name = %L WHERE (view_schema, view_name) = (%L, %L)',
                            v_new_trigger_name, v_view_row.view_schema, v_view_row.view_name);
                        EXECUTE sql;
                    END IF;
                END IF;
            END IF;
        END LOOP;
    END;

    ---
    --- unique_keys
    ---

    FOR sql IN
        SELECT format('UPDATE sql_saga.unique_keys SET column_names = %L WHERE unique_key_name = %L',
            a.column_names, uk.unique_key_name)
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT oid FROM pg_namespace WHERE nspname = uk.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON (c.conrelid, c.conname) = (pc.oid, uk.unique_constraint)
        JOIN LATERAL (
            SELECT array_agg(a.attname ORDER BY u.ordinality) AS column_names
            FROM unnest(c.conkey) WITH ORDINALITY AS u (attnum, ordinality)
            JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attnum) = (pc.oid, u.attnum)
            WHERE a.attname NOT IN (e.valid_from_column_name, e.valid_until_column_name)
            ) AS a ON true
        WHERE uk.column_names <> a.column_names
    LOOP
        --RAISE DEBUG 'unique_keys sql:%', sql;
        EXECUTE sql;
    END LOOP;

    FOR sql IN
        SELECT format('UPDATE sql_saga.unique_keys SET unique_constraint = %L WHERE unique_key_name = %L',
            c.conname, uk.unique_key_name)
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT oid FROM pg_namespace WHERE nspname = uk.table_schema))
        CROSS JOIN LATERAL unnest(uk.column_names || ARRAY[e.valid_from_column_name, e.valid_until_column_name]) WITH ORDINALITY AS u (column_name, ordinality)
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        WHERE NOT EXISTS (SELECT FROM pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, uk.unique_constraint))
        GROUP BY uk.unique_key_name, c.oid, c.conname
        HAVING format('UNIQUE (%s) DEFERRABLE', string_agg(quote_ident(u.column_name), ', ' ORDER BY u.ordinality)) = pg_catalog.pg_get_constraintdef(c.oid)
    LOOP
        --RAISE DEBUG 'unique_constraint sql:%', sql;
        EXECUTE sql;
    END LOOP;

    FOR sql IN
        SELECT format('UPDATE sql_saga.unique_keys SET exclude_constraint = %L WHERE unique_key_name = %L',
            c.conname, uk.unique_key_name)
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT oid FROM pg_namespace WHERE nspname = uk.table_schema))
        CROSS JOIN LATERAL unnest(uk.column_names) WITH ORDINALITY AS u (column_name, ordinality)
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, uk.exclude_constraint))
        GROUP BY uk.unique_key_name, c.oid, c.conname, e.range_type, e.valid_from_column_name, e.valid_until_column_name
        HAVING format('EXCLUDE USING gist (%s, %I(%I, %I) WITH &&) DEFERRABLE',
                      string_agg(quote_ident(u.column_name) || ' WITH =', ', ' ORDER BY u.ordinality),
                      e.range_type,
                      e.valid_from_column_name,
                      e.valid_until_column_name) = pg_catalog.pg_get_constraintdef(c.oid)
    LOOP
        --RAISE DEBUG 'exclude_constraint sql:%', sql;
        EXECUTE sql;
    END LOOP;

    ---
    --- foreign_keys
    ---

    DECLARE
        fk_table RECORD;
        current_columns name[];
        removed_columns name[];
        added_columns name[];
        old_col_name name;
        new_col_name name;
        fk_table_oid oid;
    BEGIN
        FOR fk_table IN
            SELECT DISTINCT table_schema, table_name
            FROM sql_saga.foreign_keys
        LOOP
            fk_table_oid := to_regclass(format('%I.%I', fk_table.table_schema, fk_table.table_name));

            -- If table was dropped, skip it. drop_protection will handle FK violations.
            IF fk_table_oid IS NULL THEN
                CONTINUE;
            END IF;

            -- Get current columns for this table
            SELECT array_agg(a.attname ORDER BY a.attnum) INTO current_columns
            FROM pg_catalog.pg_attribute AS a
            WHERE a.attrelid = fk_table_oid
              AND a.attnum > 0 AND NOT a.attisdropped;

            -- Find if a rename occurred by comparing current columns with the last known snapshot for this table
            DECLARE
                snapshot name[];
            BEGIN
                -- Get a snapshot from any of the FKs on this table (they are all identical for a given table)
                SELECT fk_table_columns_snapshot INTO snapshot FROM sql_saga.foreign_keys
                WHERE (table_schema, table_name) = (fk_table.table_schema, fk_table.table_name)
                LIMIT 1;

                -- If columns are unchanged, we are done with this table
                IF snapshot = current_columns THEN
                    CONTINUE;
                END IF;

                -- Find columns that were in the snapshot but are not in the current table state
                SELECT array_agg(c) INTO removed_columns
                FROM unnest(snapshot) AS u(c)
                WHERE NOT (u.c = ANY(current_columns));

                -- Find columns that are in the current table state but were not in the snapshot
                SELECT array_agg(c) INTO added_columns
                FROM unnest(current_columns) AS u(c)
                WHERE NOT (u.c = ANY(snapshot));

                -- If exactly one column was removed and one was added, we have a rename.
                IF array_length(removed_columns, 1) = 1 AND array_length(added_columns, 1) = 1 THEN
                    old_col_name := removed_columns[1];
                    new_col_name := added_columns[1];

                    -- We have a rename. Find all affected FKs, rename their triggers and update their metadata.
                    DECLARE
                        fk_rec RECORD;
                        uk_rec RECORD;
                        new_column_names name[];
                        new_foreign_key_name name;
                        new_fk_insert_trigger name;
                        new_fk_update_trigger name;
                        new_uk_update_trigger name;
                        new_uk_delete_trigger name;
                    BEGIN
                        FOR fk_rec IN
                            SELECT * FROM sql_saga.foreign_keys
                            WHERE (table_schema, table_name) = (fk_table.table_schema, fk_table.table_name)
                              AND old_col_name = ANY(column_names)
                        LOOP
                            -- Get UK info for uk-side trigger renames
                            SELECT * INTO uk_rec FROM sql_saga.unique_keys WHERE unique_key_name = fk_rec.unique_key_name;

                            -- 1. Calculate all the new names
                            DECLARE
                                old_base_foreign_key_name name;
                                new_base_foreign_key_name name;
                                name_suffix text;
                            BEGIN
                                new_column_names := array_replace(fk_rec.column_names, old_col_name, new_col_name);

                                -- To correctly rename, we must preserve any suffix that was added
                                -- to the original foreign key name to ensure uniqueness.
                                old_base_foreign_key_name := sql_saga.__internal_make_name(
                                    ARRAY[fk_rec.table_name] || fk_rec.column_names || ARRAY[fk_rec.fk_era_name]
                                );
                                name_suffix := regexp_replace(fk_rec.foreign_key_name, '^' || old_base_foreign_key_name, '');

                                new_base_foreign_key_name := sql_saga.__internal_make_name(
                                    ARRAY[fk_rec.table_name] || new_column_names || ARRAY[fk_rec.fk_era_name]
                                );
                                new_foreign_key_name := new_base_foreign_key_name || name_suffix;

                                -- Regenerate trigger names with the full new name
                                new_fk_insert_trigger := sql_saga.__internal_make_name(ARRAY[new_foreign_key_name], 'fk_insert');
                                new_fk_update_trigger := sql_saga.__internal_make_name(ARRAY[new_foreign_key_name], 'fk_update');
                                new_uk_update_trigger := sql_saga.__internal_make_name(ARRAY[new_foreign_key_name], 'uk_update');
                                new_uk_delete_trigger := sql_saga.__internal_make_name(ARRAY[new_foreign_key_name], 'uk_delete');
                            END;

                            -- It appears that Postgres automatically renames triggers when a column that is part
                            -- of the trigger's name is renamed. Therefore, we do not need to rename them
                            -- manually; we only need to update our metadata to reflect the new names.
                            UPDATE sql_saga.foreign_keys
                            SET
                                foreign_key_name = new_foreign_key_name,
                                column_names = new_column_names,
                                fk_insert_trigger = new_fk_insert_trigger,
                                fk_update_trigger = new_fk_update_trigger,
                                uk_update_trigger = new_uk_update_trigger,
                                uk_delete_trigger = new_uk_delete_trigger
                            WHERE foreign_key_name = fk_rec.foreign_key_name;
                        END LOOP;
                    END;
                END IF;

                -- Regardless of the operation (RENAME, ADD, DROP), update the snapshot
                -- to reflect the new state of the table for all FKs on that table.
                -- For DROP, drop_protection will have already fired if the column was part of an FK.
                UPDATE sql_saga.foreign_keys
                SET fk_table_columns_snapshot = current_columns
                WHERE (table_schema, table_name) = (fk_table.table_schema, fk_table.table_name);
            END;
        END LOOP;
    END;

    /*
     * Protection logic: Prevent manual renaming or dropping of managed triggers.
     * This logic is disabled for ALTER TABLE commands because any trigger changes
     * are side-effects of a column rename, which is handled above. This avoids
     * false positives caused by MVCC visibility issues within the event trigger.
     */
    IF NOT is_alter_table THEN
        FOR r IN
            SELECT fk.foreign_key_name, to_regclass(format('%I.%I', fk.table_schema, fk.table_name)) AS table_oid, fk.fk_insert_trigger AS trigger_name
            FROM sql_saga.foreign_keys AS fk
            WHERE fk.type = 'temporal_to_temporal' AND NOT EXISTS (
                SELECT FROM pg_catalog.pg_trigger AS t
                WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', fk.table_schema, fk.table_name)), fk.fk_insert_trigger))
            UNION ALL
            SELECT fk.foreign_key_name, to_regclass(format('%I.%I', fk.table_schema, fk.table_name)) AS table_oid, fk.fk_update_trigger AS trigger_name
            FROM sql_saga.foreign_keys AS fk
            WHERE fk.type = 'temporal_to_temporal' AND NOT EXISTS (
                SELECT FROM pg_catalog.pg_trigger AS t
                WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', fk.table_schema, fk.table_name)), fk.fk_update_trigger))
            UNION ALL
            SELECT fk.foreign_key_name, to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) AS table_oid, fk.uk_update_trigger AS trigger_name
            FROM sql_saga.foreign_keys AS fk
            JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
            WHERE NOT EXISTS (
                SELECT FROM pg_catalog.pg_trigger AS t
                WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', uk.table_schema, uk.table_name)), fk.uk_update_trigger))
            UNION ALL
            SELECT fk.foreign_key_name, to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) AS table_oid, fk.uk_delete_trigger AS trigger_name
            FROM sql_saga.foreign_keys AS fk
            JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
            WHERE NOT EXISTS (
                SELECT FROM pg_catalog.pg_trigger AS t
                WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', uk.table_schema, uk.table_name)), fk.uk_delete_trigger))
        LOOP
            RAISE EXCEPTION 'cannot drop or rename trigger "%" on table "%" because it is used in an era foreign key "%"',
                r.trigger_name, r.table_oid, r.foreign_key_name;
        END LOOP;
    END IF;

END;
$function$;
