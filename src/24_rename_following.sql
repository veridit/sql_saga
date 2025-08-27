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
BEGIN
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
    --- api_view
    ---
    DECLARE
        v_api_view_row sql_saga.api_view;
        v_view_oid regclass;
        v_new_trigger_name name;
    BEGIN
        FOR v_api_view_row IN SELECT * FROM sql_saga.api_view
        LOOP
            v_view_oid := to_regclass(format('%I.%I', v_api_view_row.view_schema_name, v_api_view_row.view_table_name));

            -- Check if the registered trigger still exists on the view
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = v_view_oid AND tgname = v_api_view_row.trigger_name) THEN
                -- If it's stale, find the *new* trigger on that view that points to our function
                SELECT tgname INTO v_new_trigger_name FROM pg_trigger
                WHERE tgrelid = v_view_oid AND tgfoid = 'sql_saga.update_portion_of()'::regprocedure;

                IF FOUND THEN
                    sql := format('UPDATE sql_saga.api_view SET trigger_name = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
                        v_new_trigger_name, v_api_view_row.table_schema, v_api_view_row.table_name, v_api_view_row.era_name);
                    EXECUTE sql;
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

    /*
     * We can't reliably find out what a column was renamed to, so just error
     * out in this case.
     */
    FOR r IN
        SELECT fk.foreign_key_name, to_regclass(format('%I.%I', fk.table_schema, fk.table_name)) as table_oid, u.column_name
        FROM sql_saga.foreign_keys AS fk
        CROSS JOIN LATERAL unnest(fk.column_names) AS u (column_name)
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_attribute AS a
            WHERE (a.attrelid, a.attname) = (to_regclass(format('%I.%I', fk.table_schema, fk.table_name)), u.column_name))
    LOOP
        RAISE EXCEPTION 'cannot drop or rename column "%" on table "%" because it is used in era foreign key "%"',
            r.column_name, r.table_oid, r.foreign_key_name;
    END LOOP;

    /*
     * Since there can be multiple foreign keys, there is no reliable way to
     * know which trigger might belong to what, so just error out.
     */
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

END;
$function$;
