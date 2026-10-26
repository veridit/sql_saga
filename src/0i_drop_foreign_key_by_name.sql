/*
 * The worker behind drop_foreign_key_by_name. require_owner is false only where sql_saga drops a foreign key as the
 * dependant of something the caller has already been checked to own: drop_unique_key_by_name(..., 'CASCADE') removes
 * the foreign keys that reference the key, as native DROP ... CASCADE lets the referenced table's owner do, whoever
 * owns the referencing tables. Private.
 */
CREATE FUNCTION sql_saga.__internal_drop_foreign_key_by_name(
    table_oid regclass,
    key_name name,
    drop_index boolean,
    require_owner boolean)
 RETURNS boolean
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, pg_temp
AS
$function$
#variable_conflict use_variable
DECLARE
    foreign_key_row record;
    unique_table_oid regclass;
    fk_table_oid regclass;
    fk_schema_name name;
    fk_table_name name;
    native_constraint_attributable boolean;
BEGIN
    IF table_oid IS NULL AND key_name IS NULL THEN
        RAISE EXCEPTION 'no table or key name specified';
    END IF;

    IF table_oid IS NOT NULL THEN
        SELECT n.nspname, c.relname
        INTO fk_schema_name, fk_table_name
        FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
        WHERE c.oid = table_oid;
    END IF;

    /* Always serialize operations on our catalogs */
    PERFORM sql_saga.__internal_serialize(table_oid);

    FOR foreign_key_row IN
        SELECT fk.*,
               fkc.oid AS fk_table_oid,
               ukc.oid AS unique_table_oid,
               coalesce(
                   fk.type <> 'temporal_to_temporal'
                   OR (
                       c.oid IS NOT NULL
                       AND c.contype = 'f'
                       AND c.conrelid = fkc.oid
                       AND c.confrelid = ukc.oid
                       AND c.conkey = fk_cols.attnums
                       -- The canonical pairing (the key's constraint order), or the one historical pairing
                       -- sql_saga 1.0/1.1 created against an XOR key whose catalog list it had sorted: the
                       -- referenced columns sorted by name (dropping it is the first half of its repair).
                       AND (c.confkey = uk_cols.attnums
                            OR (uk.mutually_exclusive_columns IS NOT NULL
                                AND c.confkey = sql_saga.__internal_historical_xor_confkey(uk.unique_key_name)))
                       AND c.conperiod
                       AND c.condeferrable
                       AND NOT c.condeferred
                       AND c.convalidated
                       AND c.conenforced
                       AND c.confmatchtype = CASE fk.match_type WHEN 'FULL' THEN 'f' WHEN 'PARTIAL' THEN 'p' ELSE 's' END
                       AND c.confupdtype = CASE fk.update_action::text WHEN 'RESTRICT' THEN 'r' WHEN 'CAS' || 'CADE' THEN 'c' WHEN 'SET NULL' THEN 'n' WHEN 'SET DEFAULT' THEN 'd' ELSE 'a' END
                       AND c.confdeltype = CASE fk.delete_action::text WHEN 'RESTRICT' THEN 'r' WHEN 'CAS' || 'CADE' THEN 'c' WHEN 'SET NULL' THEN 'n' WHEN 'SET DEFAULT' THEN 'd' ELSE 'a' END
                   ),
                   false) AS native_constraint_attributable
        FROM sql_saga.foreign_keys AS fk
        LEFT JOIN pg_catalog.pg_namespace AS fkn ON fkn.nspname = fk.table_schema
        LEFT JOIN pg_catalog.pg_class AS fkc
          ON fkc.relnamespace = fkn.oid AND fkc.relname = fk.table_name
        LEFT JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        LEFT JOIN pg_catalog.pg_namespace AS ukn ON ukn.nspname = uk.table_schema
        LEFT JOIN pg_catalog.pg_class AS ukc
          ON ukc.relnamespace = ukn.oid AND ukc.relname = uk.table_name
        LEFT JOIN sql_saga.era AS fk_era
          ON (fk_era.table_schema, fk_era.table_name, fk_era.era_name)
           = (fk.table_schema, fk.table_name, fk.fk_era_name)
        LEFT JOIN sql_saga.era AS uk_era
          ON (uk_era.table_schema, uk_era.table_name, uk_era.era_name)
           = (uk.table_schema, uk.table_name, uk.era_name)
        LEFT JOIN LATERAL (
            SELECT pg_catalog.array_agg(a.attnum::smallint ORDER BY u.n) AS attnums
            FROM pg_catalog.unnest(fk.column_names || fk_era.range_column_name) WITH ORDINALITY AS u(column_name, n)
            JOIN pg_catalog.pg_attribute AS a
              ON a.attrelid = fkc.oid AND a.attname = u.column_name
             AND a.attnum > 0 AND NOT a.attisdropped
        ) AS fk_cols ON fk.type = 'temporal_to_temporal'
        LEFT JOIN LATERAL (
            SELECT pg_catalog.array_agg(a.attnum::smallint ORDER BY u.n) AS attnums
            FROM pg_catalog.unnest(uk.column_names || uk_era.range_column_name) WITH ORDINALITY AS u(column_name, n)
            JOIN pg_catalog.pg_attribute AS a
              ON a.attrelid = ukc.oid AND a.attname = u.column_name
             AND a.attnum > 0 AND NOT a.attisdropped
        ) AS uk_cols ON fk.type = 'temporal_to_temporal'
        LEFT JOIN pg_catalog.pg_constraint AS c
          ON fk.type = 'temporal_to_temporal'
         AND c.conrelid = fkc.oid
         AND c.conname = fk.foreign_key_name
        WHERE (table_oid IS NULL OR (fk.table_schema, fk.table_name) = (fk_schema_name, fk_table_name))
          AND (fk.foreign_key_name = key_name OR key_name IS NULL)
    LOOP
        native_constraint_attributable := foreign_key_row.native_constraint_attributable;

        /* As ALTER TABLE ... DROP CONSTRAINT: the caller must own the referencing table (also when found by name). */
        IF require_owner THEN
            IF foreign_key_row.fk_table_oid IS NOT NULL THEN
                PERFORM sql_saga.__internal_require_owner(foreign_key_row.fk_table_oid, 'drop_foreign_key');
            ELSIF foreign_key_row.unique_table_oid IS NOT NULL THEN
                /*
                 * A dangling row (the referencing table is gone): removing it drops triggers on the referenced
                 * table, so its owner may, as for DROP ... CASCADE there (R6).
                 */
                PERFORM sql_saga.__internal_require_owner(foreign_key_row.unique_table_oid, 'drop_foreign_key');
            ELSIF NOT (SELECT r.rolsuper FROM pg_catalog.pg_roles AS r WHERE r.oid = sql_saga.__internal_caller()) THEN
                RAISE EXCEPTION 'must be superuser to drop foreign key "%", whose tables no longer exist',
                    foreign_key_row.foreign_key_name USING ERRCODE = 'insufficient_privilege';
            END IF;
        END IF;

        -- Delete the metadata record first, so the drop_protection trigger doesn't fire on the main objects.
        DELETE FROM sql_saga.foreign_keys AS fk
        WHERE fk.foreign_key_name = foreign_key_row.foreign_key_name;

        /*
         * Now that the metadata is gone, we can safely drop the database objects.
         */

        -- Drop the auto-created index if it exists and requested.
        IF drop_index AND foreign_key_row.fk_index_name IS NOT NULL THEN
            RAISE NOTICE 'sql_saga: Dropping automatically created index "%" for foreign key "%"', foreign_key_row.fk_index_name, foreign_key_row.foreign_key_name;
            IF require_owner THEN
                PERFORM sql_saga.__internal_execute_as_caller(format('DROP INDEX IF EXISTS %I.%I', foreign_key_row.table_schema, foreign_key_row.fk_index_name));
            ELSE
                EXECUTE format('DROP INDEX IF EXISTS %I.%I', foreign_key_row.table_schema, foreign_key_row.fk_index_name);
            END IF;
        END IF;
        fk_table_oid := foreign_key_row.fk_table_oid;

        CASE foreign_key_row.type
            WHEN 'temporal_to_temporal' THEN
                -- For native temporal FKs, we drop the constraint directly.
                IF EXISTS (SELECT FROM pg_constraint WHERE conrelid = fk_table_oid AND conname = foreign_key_row.foreign_key_name) THEN
                    IF native_constraint_attributable THEN
                        IF require_owner THEN
                            PERFORM sql_saga.__internal_execute_as_caller(format('ALTER TABLE %s DROP CONSTRAINT %I', fk_table_oid /* %s */, foreign_key_row.foreign_key_name /* %I */));
                        ELSE
                            EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', fk_table_oid /* %s */, foreign_key_row.foreign_key_name /* %I */);
                        END IF;
                    ELSE
                        RAISE NOTICE 'sql_saga: Left constraint "%" on table "%" in place because it is not the sql_saga-managed constraint',
                            foreign_key_row.foreign_key_name, sql_saga.__internal_display_name(fk_table_oid);
                    END IF;
                END IF;
            WHEN 'regular_to_temporal' THEN
                /* 1.3: the referencing side is a constraint trigger named fk_check_constraint. */
                IF EXISTS (SELECT FROM pg_trigger WHERE tgrelid = fk_table_oid AND tgname = foreign_key_row.fk_check_constraint
                           AND tgfoid = pg_catalog.to_regprocedure('sql_saga.regular_fk_check_c()')) THEN
                    IF require_owner THEN
                        PERFORM sql_saga.__internal_execute_as_caller(format('DROP TRIGGER %I ON %s', foreign_key_row.fk_check_constraint /* %I */, fk_table_oid /* %s */));
                    ELSE
                        EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.fk_check_constraint /* %I */, fk_table_oid /* %s */);
                    END IF;
                /* Historical (pre-1.3) form: a CHECK constraint calling the helper function. */
                ELSIF foreign_key_row.fk_check_constraint IS NOT NULL AND foreign_key_row.fk_helper_function IS NOT NULL AND EXISTS (
                            /* Only the released CHECK: it depends on (calls) the catalogued helper function. */
                            SELECT 1 FROM pg_catalog.pg_constraint AS c
                            JOIN pg_catalog.pg_depend AS d
                              ON d.classid = 'pg_catalog.pg_constraint'::regclass AND d.objid = c.oid
                             AND d.refclassid = 'pg_catalog.pg_proc'::regclass
                             AND d.refobjid = pg_catalog.to_regprocedure(foreign_key_row.fk_helper_function)
                            WHERE c.conrelid = fk_table_oid AND c.conname = foreign_key_row.fk_check_constraint AND c.contype = 'c') THEN
                    IF require_owner THEN
                        PERFORM sql_saga.__internal_execute_as_caller(format('ALTER TABLE %s DROP CONSTRAINT %I', fk_table_oid /* %s */, foreign_key_row.fk_check_constraint /* %I */));
                    ELSE
                        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', fk_table_oid /* %s */, foreign_key_row.fk_check_constraint /* %I */);
                    END IF;
                ELSIF EXISTS (SELECT FROM pg_constraint WHERE conrelid = fk_table_oid AND conname = foreign_key_row.fk_check_constraint) THEN
                    RAISE NOTICE 'sql_saga: Left constraint "%" on table "%" in place because it is not the sql_saga-managed constraint',
                        foreign_key_row.fk_check_constraint, sql_saga.__internal_display_name(fk_table_oid);
                END IF;
                -- If no other foreign keys use the helper function, drop it (new 1.3 FKs have none).
                IF foreign_key_row.fk_helper_function IS NOT NULL AND NOT EXISTS (
                    SELECT 1 FROM sql_saga.foreign_keys
                    WHERE fk_helper_function = foreign_key_row.fk_helper_function
                ) THEN
                    IF to_regprocedure(foreign_key_row.fk_helper_function) IS NOT NULL THEN
                        EXECUTE format('DROP FUNCTION %s', foreign_key_row.fk_helper_function);
                    END IF;
                END IF;
        END CASE;

        unique_table_oid := foreign_key_row.unique_table_oid;

        IF unique_table_oid IS NOT NULL AND pg_catalog.to_regclass(unique_table_oid::text) IS NOT NULL THEN
            IF EXISTS (SELECT FROM pg_trigger WHERE tgrelid = unique_table_oid AND tgname = foreign_key_row.uk_update_trigger) THEN
                EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.uk_update_trigger /* %I */, unique_table_oid /* %s */);
            END IF;
            IF EXISTS (SELECT FROM pg_trigger WHERE tgrelid = unique_table_oid AND tgname = foreign_key_row.uk_delete_trigger) THEN
                EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.uk_delete_trigger /* %I */, unique_table_oid /* %s */);
            END IF;
            IF foreign_key_row.uk_truncate_trigger IS NOT NULL
               AND EXISTS (SELECT FROM pg_trigger WHERE tgrelid = unique_table_oid AND tgname = foreign_key_row.uk_truncate_trigger) THEN
                EXECUTE format('DROP TRIGGER %I ON %s', foreign_key_row.uk_truncate_trigger /* %I */, unique_table_oid /* %s */);
            END IF;
        END IF;
    END LOOP;

    RETURN true;
END;
$function$;
REVOKE ALL ON FUNCTION sql_saga.__internal_drop_foreign_key_by_name(regclass, name, boolean, boolean) FROM PUBLIC;

CREATE FUNCTION sql_saga.__api_drop_foreign_key_by_name(
    table_oid regclass,
    key_name name,
    drop_index boolean DEFAULT true)
 RETURNS boolean
 LANGUAGE sql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, pg_temp
AS
$function$
    /* As ALTER TABLE ... DROP CONSTRAINT: the caller must own the referencing table (also when found by name). */
    SELECT sql_saga.__internal_drop_foreign_key_by_name(table_oid, key_name, drop_index, require_owner => true);
$function$;
