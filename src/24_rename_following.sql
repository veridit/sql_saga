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
     * Follow renames for the era's temporal columns. This logic is broken into
     * three parts to handle each type of temporal column independently.
     *
     * 1. The primary `range_column_name` is identified by its data type via
     *    the synchronization trigger's metadata.
     * 2. The optional `valid_from_column_name` and `valid_until_column_name`
     *    are identified by inspecting the bounds check constraint.
     * 3. The optional `valid_to_column_name` is identified as the "other"
     *    column in the synchronization trigger's metadata.
     */

    -- Follow rename of the primary range_column_name for synchronized eras.
    FOR sql IN
        SELECT format('UPDATE sql_saga.era SET range_column_name = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            a.attname, e.table_schema, e.table_name, e.era_name
        )
        FROM sql_saga.era e
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (e.table_name, (SELECT oid FROM pg_namespace WHERE nspname = e.table_schema))
        JOIN pg_trigger t ON t.tgrelid = pc.oid
        JOIN LATERAL (
            -- Find the single column attached to the trigger that has the era's range type.
            SELECT att.attname
            FROM unnest(t.tgattr) AS tg(attnum)
            JOIN pg_attribute att ON (att.attrelid, att.attnum) = (pc.oid, tg.attnum)
            WHERE att.atttypid = e.range_type
            LIMIT 1
        ) AS a ON true
        WHERE e.trigger_applies_defaults -- This flag indicates a synchronization trigger exists.
          AND t.tgfoid = 'sql_saga.synchronize_temporal_columns'::regproc
          AND e.range_column_name <> a.attname
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames for valid_from and valid_until columns using the boundary check constraint.
    FOR sql IN
        SELECT format('UPDATE sql_saga.era SET valid_from_column_name = %L, valid_until_column_name = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            sa.attname, ea.attname,
            e.table_schema, e.table_name, e.era_name
        )
        FROM sql_saga.era AS e
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (e.table_name, (SELECT oid FROM pg_namespace WHERE nspname = e.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON (c.conrelid, c.conname) = (pc.oid, e.boundary_check_constraint)
        JOIN pg_catalog.pg_attribute AS sa ON sa.attrelid = pc.oid
        JOIN pg_catalog.pg_attribute AS ea ON ea.attrelid = pc.oid
        WHERE e.boundary_check_constraint IS NOT NULL
          AND (e.valid_from_column_name, e.valid_until_column_name) <> (sa.attname, ea.attname)
          AND pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((%I < %I))', sa.attname, ea.attname)
    LOOP
        RAISE DEBUG 'rename_following: Executing boundary column rename: %', sql;
        EXECUTE sql;
    END LOOP;

    -- Follow renames for the optional valid_to_column_name for synchronized eras.
    FOR sql IN
        WITH new_valid_to AS (
            SELECT
                e.table_schema, e.table_name, e.era_name,
                (
                    -- The valid_to column is the one left in the sync trigger's metadata after
                    -- excluding all other known temporal columns.
                    SELECT a.attname
                    FROM unnest(t.tgattr) AS att(num)
                    JOIN pg_attribute a ON (a.attrelid, a.attnum) = (t.tgrelid, att.num)
                    WHERE a.attname IS DISTINCT FROM e.range_column_name
                      AND a.attname IS DISTINCT FROM e.valid_from_column_name
                      AND a.attname IS DISTINCT FROM e.valid_until_column_name
                    LIMIT 1 -- There should be at most one left.
                ) AS candidate_name
            FROM sql_saga.era e
            JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (e.table_name, (SELECT oid FROM pg_namespace WHERE nspname = e.table_schema))
            JOIN pg_trigger t ON t.tgrelid = pc.oid
            WHERE e.trigger_applies_defaults -- Sync trigger exists.
              AND e.valid_to_column_name IS NOT NULL -- Only for eras that have a valid_to column.
              AND t.tgfoid = 'sql_saga.synchronize_temporal_columns'::regproc
        )
        SELECT format('UPDATE sql_saga.era SET valid_to_column_name = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            n.candidate_name,
            e.table_schema, e.table_name, e.era_name
        )
        FROM sql_saga.era e
        JOIN new_valid_to n ON (e.table_schema, e.table_name, e.era_name) = (n.table_schema, n.table_name, n.era_name)
        WHERE e.valid_to_column_name IS DISTINCT FROM n.candidate_name
          AND n.candidate_name IS NOT NULL
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames of the bounds check constraint (NOT isempty(range)).
    FOR sql IN
        SELECT format('UPDATE sql_saga.era SET bounds_check_constraint = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            c.conname,
            e.table_schema,
            e.table_name,
            e.era_name
        )
        FROM sql_saga.era AS e
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (e.table_name, (SELECT oid FROM pg_namespace WHERE nspname = e.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        WHERE e.bounds_check_constraint IS NOT NULL
          AND e.bounds_check_constraint <> c.conname
          AND pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((NOT isempty(%I)))', e.range_column_name)
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, e.bounds_check_constraint))
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames of the boundary check constraint (from < until).
    FOR sql IN
        SELECT format('UPDATE sql_saga.era SET boundary_check_constraint = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            c.conname,
            e.table_schema,
            e.table_name,
            e.era_name
        )
        FROM sql_saga.era AS e
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (e.table_name, (SELECT oid FROM pg_namespace WHERE nspname = e.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        WHERE e.boundary_check_constraint IS NOT NULL
          AND e.boundary_check_constraint <> c.conname
          AND e.valid_from_column_name IS NOT NULL
          AND e.valid_until_column_name IS NOT NULL
          AND (pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((%I < %I))', e.valid_from_column_name, e.valid_until_column_name)
               OR pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((%I < %I) AND (%I > ''-infinity''::integer))', e.valid_from_column_name, e.valid_until_column_name, e.valid_from_column_name))
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, e.boundary_check_constraint))
    LOOP
        EXECUTE sql;
    END LOOP;

    ---
    --- system_time_era
    ---

    -- Follow renames of the infinity_check_constraint.
    -- The constraint format is: CHECK ((upper(range_column_name) = 'infinity'::timestamp with time zone))
    FOR sql IN
        SELECT format('UPDATE sql_saga.system_time_era SET infinity_check_constraint = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            c.conname,
            ste.table_schema,
            ste.table_name,
            ste.era_name
        )
        FROM sql_saga.system_time_era AS ste
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (ste.table_schema, ste.table_name, ste.era_name)
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (ste.table_name, (SELECT oid FROM pg_namespace WHERE nspname = ste.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        WHERE ste.infinity_check_constraint <> c.conname
          AND pg_catalog.pg_get_constraintdef(c.oid) = format('CHECK ((upper(%I) = ''infinity''::timestamp with time zone))', e.range_column_name)
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, ste.infinity_check_constraint))
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames of the generated_always_trigger.
    FOR sql IN
        SELECT format('UPDATE sql_saga.system_time_era SET generated_always_trigger = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            t.tgname,
            ste.table_schema,
            ste.table_name,
            ste.era_name
        )
        FROM sql_saga.system_time_era AS ste
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (ste.table_name, (SELECT oid FROM pg_namespace WHERE nspname = ste.table_schema))
        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = pc.oid
        WHERE t.tgname <> ste.generated_always_trigger
          AND t.tgfoid = 'sql_saga.generated_always_as_row_start_end()'::regprocedure
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (pc.oid, ste.generated_always_trigger))
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames of the write_history_trigger.
    FOR sql IN
        SELECT format('UPDATE sql_saga.system_time_era SET write_history_trigger = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            t.tgname,
            ste.table_schema,
            ste.table_name,
            ste.era_name
        )
        FROM sql_saga.system_time_era AS ste
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (ste.table_name, (SELECT oid FROM pg_namespace WHERE nspname = ste.table_schema))
        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = pc.oid
        WHERE t.tgname <> ste.write_history_trigger
          AND t.tgfoid = 'sql_saga.write_history()'::regprocedure
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (pc.oid, ste.write_history_trigger))
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames of the truncate_trigger.
    FOR sql IN
        SELECT format('UPDATE sql_saga.system_time_era SET truncate_trigger = %L WHERE (table_schema, table_name, era_name) = (%L, %L, %L)',
            t.tgname,
            ste.table_schema,
            ste.table_name,
            ste.era_name
        )
        FROM sql_saga.system_time_era AS ste
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (ste.table_name, (SELECT oid FROM pg_namespace WHERE nspname = ste.table_schema))
        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = pc.oid
        WHERE t.tgname <> ste.truncate_trigger
          AND t.tgfoid = 'sql_saga.truncate_system_versioning()'::regprocedure
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger AS _t WHERE (_t.tgrelid, _t.tgname) = (pc.oid, ste.truncate_trigger))
    LOOP
        EXECUTE sql;
    END LOOP;

    /*
     * We can't reliably find out what a column was renamed to, so just error
     * out in this case. Note: This is also checked in drop_protection.sql.
     */
    FOR r IN
        SELECT format('%I.%I', ste.table_schema, ste.table_name)::regclass AS table_oid, u.column_name
        FROM sql_saga.system_time_era AS ste
        CROSS JOIN LATERAL unnest(ste.excluded_column_names) AS u (column_name)
        WHERE NOT EXISTS (
            SELECT FROM pg_catalog.pg_attribute AS a
            WHERE (a.attrelid, a.attname) = (to_regclass(format('%I.%I', ste.table_schema, ste.table_name)), u.column_name))
    LOOP
        RAISE EXCEPTION 'cannot drop or rename column "%" on table "%" because it is excluded from system_time era',
            r.column_name, r.table_oid;
    END LOOP;

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
            v_view_oid := to_regclass(format('%I.%I', v_view_row.view_schema /* %I */, v_view_row.view_name /* %I */));

            -- Check if the registered trigger still exists on the view
            IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgrelid = v_view_oid AND tgname = v_view_row.trigger_name) THEN
                -- If it's stale, find the *new* trigger on that view that points to our function.
                -- This currently only supports for_portion_of, but is extensible.
                IF v_view_row.view_type = 'for_portion_of' THEN
                    SELECT tgname INTO v_new_trigger_name FROM pg_trigger
                    WHERE tgrelid = v_view_oid AND tgfoid = 'sql_saga.for_portion_of_trigger()'::regprocedure;

                    IF FOUND THEN
                        sql := format('UPDATE sql_saga.updatable_view SET trigger_name = %L WHERE (view_schema, view_name) = (%L, %L)',
                            v_new_trigger_name, /* %L */
                            v_view_row.view_schema, /* %L */
                            v_view_row.view_name /* %L */
                        );
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
        -- Follow renames for columns in non-XOR unique keys
        SELECT format('UPDATE sql_saga.unique_keys SET column_names = %L WHERE unique_key_name = %L',
            a.column_names, /* %L */
            uk.unique_key_name /* %L */
        )
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT oid FROM pg_namespace WHERE nspname = uk.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON (c.conrelid, c.conname) = (pc.oid, uk.unique_constraint)
        JOIN LATERAL (
            SELECT array_agg(a.attname ORDER BY u.ordinality) AS column_names
            FROM unnest(c.conkey) WITH ORDINALITY AS u (attnum, ordinality)
            JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attnum) = (pc.oid, u.attnum)
            WHERE a.attname NOT IN (e.range_column_name) -- The native constraint will include the range column
            ) AS a ON true
        WHERE uk.mutually_exclusive_columns IS NULL
          AND uk.unique_constraint IS NOT NULL
          AND uk.column_names <> a.column_names
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames for columns in XOR unique keys by inspecting partial indexes
    FOR sql IN
        WITH new_xor_cols AS (
            SELECT
                uk.unique_key_name,
                array_agg(DISTINCT a.attname ORDER BY a.attname) AS new_column_names
            FROM sql_saga.unique_keys uk
            JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT oid FROM pg_namespace WHERE nspname = uk.table_schema))
            CROSS JOIN unnest(uk.partial_index_names) AS pin(name)
            JOIN pg_class i ON i.relname = pin.name AND i.relnamespace = pc.relnamespace
            JOIN pg_index pi ON pi.indexrelid = i.oid
            CROSS JOIN unnest(pi.indkey::smallint[]) WITH ORDINALITY AS u(attnum, ord)
            JOIN pg_attribute a ON (a.attrelid, a.attnum) = (pi.indrelid, u.attnum)
            WHERE uk.mutually_exclusive_columns IS NOT NULL
            GROUP BY uk.unique_key_name
        )
        SELECT format('UPDATE sql_saga.unique_keys SET column_names = %L WHERE unique_key_name = %L',
            nxc.new_column_names,
            uk.unique_key_name
        )
        FROM sql_saga.unique_keys uk
        JOIN new_xor_cols nxc ON uk.unique_key_name = nxc.unique_key_name
        WHERE uk.column_names <> nxc.new_column_names
    LOOP
        EXECUTE sql;
    END LOOP;

    FOR sql IN
        -- Follow renames for unique_constraint (for native temporal keys)
        SELECT format('UPDATE sql_saga.unique_keys SET unique_constraint = %L WHERE unique_key_name = %L',
            c.conname, /* %L */
            uk.unique_key_name /* %L */
        )
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT oid FROM pg_namespace WHERE nspname = uk.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        WHERE uk.unique_constraint IS NOT NULL AND uk.unique_constraint <> c.conname
          AND NOT EXISTS (SELECT FROM pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, uk.unique_constraint))
          AND pg_get_constraintdef(c.oid) = (
            SELECT format('%s (%s, %I WITHOUT OVERLAPS)',
                CASE WHEN uk.key_type = 'primary' THEN 'PRIMARY KEY' ELSE 'UNIQUE' END,
                string_agg(quote_ident(u.name), ', ' ORDER BY u.ordinality),
                e.range_column_name
            )
            FROM unnest(uk.column_names) WITH ORDINALITY AS u(name, ordinality)
          )
    LOOP
        EXECUTE sql;
    END LOOP;

    FOR sql IN
        -- Follow renames for exclude_constraint (for predicated keys)
        SELECT format('UPDATE sql_saga.unique_keys SET exclude_constraint = %L WHERE unique_key_name = %L',
            c.conname, /* %L */
            uk.unique_key_name /* %L */
        )
        FROM sql_saga.unique_keys AS uk
        JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT oid FROM pg_namespace WHERE nspname = uk.table_schema))
        JOIN pg_catalog.pg_constraint AS c ON c.conrelid = pc.oid
        WHERE uk.exclude_constraint IS NOT NULL AND uk.exclude_constraint <> c.conname
          AND NOT EXISTS (SELECT FROM pg_catalog.pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, uk.exclude_constraint))
          AND pg_get_constraintdef(c.oid) = (
            SELECT format('EXCLUDE USING gist (%s, %I WITH &&) WHERE (%s) DEFERRABLE',
                string_agg(quote_ident(u.name) || ' WITH =', ', ' ORDER BY u.ordinality),
                e.range_column_name,
                uk.predicate
            )
            FROM unnest(uk.column_names) WITH ORDINALITY AS u(name, ordinality)
          )
    LOOP
        EXECUTE sql;
    END LOOP;

    -- Follow renames for check_constraint (for XOR keys)
    FOR sql IN
        SELECT format('UPDATE sql_saga.unique_keys SET check_constraint = %L WHERE unique_key_name = %L',
            c.conname, uk.unique_key_name
        )
        FROM sql_saga.unique_keys uk
        JOIN pg_class pc ON (pc.relname, pc.relnamespace) = (uk.table_name, (SELECT oid FROM pg_namespace WHERE nspname = uk.table_schema))
        JOIN pg_constraint c ON c.conrelid = pc.oid
        WHERE uk.check_constraint IS NOT NULL
        AND uk.check_constraint <> c.conname
        AND c.contype = 'c'
        AND NOT EXISTS (SELECT FROM pg_constraint AS _c WHERE (_c.conrelid, _c.conname) = (pc.oid, uk.check_constraint))
        AND pg_get_constraintdef(c.oid) = format('CHECK (((%s) = 1))', format('num_nonnulls(%s)', (SELECT string_agg(format('%I', col), ', ') FROM unnest(uk.mutually_exclusive_columns) as col)))
    LOOP
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
            fk_table_oid := to_regclass(format('%I.%I', fk_table.table_schema /* %I */, fk_table.table_name /* %I */));

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
            SELECT fk.foreign_key_name, to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) AS table_oid, fk.uk_update_trigger AS trigger_name
            FROM sql_saga.foreign_keys AS fk
            JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
            WHERE fk.uk_update_trigger IS NOT NULL
              AND NOT EXISTS (
                SELECT FROM pg_catalog.pg_trigger AS t
                WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', uk.table_schema, uk.table_name)), fk.uk_update_trigger))
            UNION ALL
            SELECT fk.foreign_key_name, to_regclass(format('%I.%I', uk.table_schema, uk.table_name)) AS table_oid, fk.uk_delete_trigger AS trigger_name
            FROM sql_saga.foreign_keys AS fk
            JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
            WHERE fk.uk_delete_trigger IS NOT NULL
              AND NOT EXISTS (
                SELECT FROM pg_catalog.pg_trigger AS t
                WHERE (t.tgrelid, t.tgname) = (to_regclass(format('%I.%I', uk.table_schema, uk.table_name)), fk.uk_delete_trigger))
        LOOP
            RAISE EXCEPTION 'cannot drop or rename trigger "%" on table "%" because it is used in an era foreign key "%"',
                r.trigger_name, r.table_oid, r.foreign_key_name;
        END LOOP;

        -- Protect synchronize_temporal_columns_trigger from being renamed or dropped.
        -- This trigger is essential for maintaining consistency between range and boundary columns.
        -- Only check during explicit ALTER TRIGGER commands to avoid MVCC visibility issues
        -- during other operations like ALTER TABLE OWNER or GRANT/REVOKE.
        IF v_is_alter_trigger THEN
            FOR r IN
                SELECT e.era_name, to_regclass(format('%I.%I', e.table_schema, e.table_name)) AS table_oid,
                       e.sync_temporal_trg_name, e.sync_temporal_trg_function_name
                FROM sql_saga.era AS e
                WHERE e.sync_temporal_trg_name IS NOT NULL  -- Only eras with synchronization triggers
                  AND e.sync_temporal_trg_function_name IS NOT NULL
                  AND NOT EXISTS (
                      SELECT FROM pg_catalog.pg_trigger AS t
                      WHERE t.tgrelid = to_regclass(format('%I.%I', e.table_schema, e.table_name))
                        AND t.tgname = e.sync_temporal_trg_name
                        AND t.tgfoid = to_regprocedure(format('sql_saga.%I()', e.sync_temporal_trg_function_name)))
            LOOP
                RAISE EXCEPTION 'cannot drop or rename trigger "%" on table "%" because it is managed by era "%"',
                    r.sync_temporal_trg_name, r.table_oid, r.era_name;
            END LOOP;
        END IF;
    END IF;

END;
$function$;

COMMENT ON FUNCTION sql_saga.rename_following() IS
'An event trigger function that follows object renames and updates sql_saga''s metadata accordingly.';
