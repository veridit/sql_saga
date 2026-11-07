CREATE OR REPLACE PROCEDURE sql_saga.manage_temporal_fk_triggers(p_action sql_saga.trigger_action, VARIADIC p_table_oids regclass[])
LANGUAGE plpgsql SECURITY INVOKER AS $procedure$
DECLARE
    v_trigger_rec RECORD;
    v_sql TEXT;
BEGIN
    v_sql := 'ALTER TABLE %s ' || p_action::text || ' TRIGGER %I';

    -- Handle all incoming FK triggers to the specified tables. The table is resolved through the catalog,
    -- not by casting "schema.table" text, so names that need quoting work.
    FOR v_trigger_rec IN
        SELECT pc.oid::regclass AS table_oid, trg.trigger_name, fk.foreign_key_name, trg.n
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON fk.unique_key_name = uk.unique_key_name
        JOIN pg_catalog.pg_namespace AS pn ON pn.nspname = uk.table_schema
        JOIN pg_catalog.pg_class AS pc ON (pc.relnamespace, pc.relname) = (pn.oid, uk.table_name)
        CROSS JOIN LATERAL (VALUES (1, fk.uk_update_trigger), (2, fk.uk_delete_trigger), (3, fk.uk_truncate_trigger)) AS trg(n, trigger_name)
        WHERE pc.oid = ANY (p_table_oids::oid[]) AND trg.trigger_name IS NOT NULL
        UNION ALL
        -- The referencing-side constraint trigger of a regular foreign key, when its table is listed.
        SELECT pc.oid::regclass, fk.fk_check_constraint, fk.foreign_key_name, 4
        FROM sql_saga.foreign_keys AS fk
        JOIN pg_catalog.pg_namespace AS pn ON pn.nspname = fk.table_schema
        JOIN pg_catalog.pg_class AS pc ON (pc.relnamespace, pc.relname) = (pn.oid, fk.table_name)
        JOIN pg_catalog.pg_trigger AS t ON (t.tgrelid, t.tgname) = (pc.oid, fk.fk_check_constraint)
        WHERE fk.type = 'regular_to_temporal' AND pc.oid = ANY (p_table_oids::oid[])
        ORDER BY 3, 4
    LOOP
        EXECUTE format(v_sql, v_trigger_rec.table_oid, v_trigger_rec.trigger_name);
    END LOOP;
END;
$procedure$;


CREATE OR REPLACE PROCEDURE sql_saga.disable_temporal_triggers(VARIADIC p_table_oids regclass[])
LANGUAGE plpgsql SECURITY INVOKER AS $procedure$
DECLARE
    v_native_constraints text;
BEGIN
    CALL sql_saga.manage_temporal_fk_triggers('disable', VARIADIC p_table_oids);

    -- Native temporal foreign keys (temporal_to_temporal) have no sql_saga triggers to disable: name them and
    -- the statement that defers them, since they are DEFERRABLE.
    SELECT string_agg(pg_catalog.format('%I.%I', fk_ns.nspname, c.conname), ', ' ORDER BY c.conname COLLATE "C", fk_ns.nspname COLLATE "C")
    INTO v_native_constraints
    FROM (
        SELECT DISTINCT fk_class.oid AS fk_table_oid, fk.foreign_key_name
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        JOIN pg_catalog.pg_namespace AS fk_n ON fk_n.nspname = fk.table_schema
        JOIN pg_catalog.pg_class AS fk_class ON (fk_class.relnamespace, fk_class.relname) = (fk_n.oid, fk.table_name)
        JOIN pg_catalog.pg_namespace AS uk_n ON uk_n.nspname = uk.table_schema
        JOIN pg_catalog.pg_class AS uk_class ON (uk_class.relnamespace, uk_class.relname) = (uk_n.oid, uk.table_name)
        WHERE fk.type = 'temporal_to_temporal'
          AND (fk_class.oid = ANY (p_table_oids::oid[]) OR uk_class.oid = ANY (p_table_oids::oid[]))
    ) AS native
    JOIN pg_catalog.pg_constraint AS c ON (c.conrelid, c.conname, c.contype) = (native.fk_table_oid, native.foreign_key_name, 'f')
    JOIN pg_catalog.pg_class AS fk_c ON fk_c.oid = c.conrelid
    JOIN pg_catalog.pg_namespace AS fk_ns ON fk_ns.oid = fk_c.relnamespace;

    IF v_native_constraints IS NOT NULL THEN
        RAISE NOTICE 'sql_saga: disable_temporal_triggers left native temporal foreign keys enabled (they have no sql_saga triggers); they are DEFERRABLE, defer them in this transaction with: SET CONSTRAINTS % DEFERRED',
            v_native_constraints;
    END IF;
END;
$procedure$;
COMMENT ON PROCEDURE sql_saga.disable_temporal_triggers(VARIADIC regclass[]) IS
'Disables all sql_saga-managed temporal foreign key triggers for the specified tables. This is a targeted alternative to `ALTER TABLE ... DISABLE TRIGGER USER`, intended for complex ETL batches where transient, inconsistent states are expected. It does not affect non-FK triggers, such as those for synchronized columns. Only regular-to-temporal foreign keys (add_regular_foreign_key) have such triggers. A temporal-to-temporal foreign key (add_foreign_key) is a native temporal foreign key constraint (FOREIGN KEY ... PERIOD) with no sql_saga triggers, and this procedure leaves it unchanged; it is DEFERRABLE, so defer it in the transaction with SET CONSTRAINTS <schema>.<foreign key name> DEFERRED (or SET CONSTRAINTS ALL DEFERRED). A NOTICE names every native temporal foreign key referencing or referenced by the specified tables, with that statement.';

CREATE OR REPLACE PROCEDURE sql_saga.enable_temporal_triggers(VARIADIC p_table_oids regclass[])
LANGUAGE plpgsql SECURITY INVOKER AS $procedure$
BEGIN
    CALL sql_saga.manage_temporal_fk_triggers('enable', VARIADIC p_table_oids);
END;
$procedure$;
COMMENT ON PROCEDURE sql_saga.enable_temporal_triggers(VARIADIC regclass[]) IS
'Re-enables all sql_saga-managed temporal foreign key triggers for the specified tables. This should be called at the end of an ETL batch transaction to restore integrity checks.';

/* STATBUS-591 prototype. Native constraint queues own all validation and error handling. */
CREATE PROCEDURE sql_saga.defer_temporal_checks(VARIADIC table_oids regclass[])
LANGUAGE plpgsql SECURITY INVOKER
SET search_path = pg_catalog, sql_saga, pg_temp
AS $defer_temporal_checks$
DECLARE
    v_intended oid[];
    v_registered oid[];
    v_names text;
    v_invalid text;
    v_conflict record;
BEGIN
    IF table_oids IS NULL OR pg_catalog.array_position(table_oids, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'sql_saga: defer_temporal_checks requires non-NULL table names'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF EXISTS (SELECT FROM pg_catalog.unnest(table_oids) AS requested(table_oid)
               WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_class AS c WHERE c.oid = requested.table_oid)) THEN
        RAISE EXCEPTION 'sql_saga: defer_temporal_checks names a table that no longer exists'
            USING ERRCODE = 'undefined_table';
    END IF;

    WITH related AS (
        SELECT fk.*, child.oid AS child_oid, parent.oid AS parent_oid
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
        JOIN pg_catalog.pg_namespace AS cn ON cn.nspname = fk.table_schema
        JOIN pg_catalog.pg_class AS child ON (child.relnamespace, child.relname) = (cn.oid, fk.table_name)
        JOIN pg_catalog.pg_namespace AS pn ON pn.nspname = uk.table_schema
        JOIN pg_catalog.pg_class AS parent ON (parent.relnamespace, parent.relname) = (pn.oid, uk.table_name)
        WHERE child.oid = ANY (table_oids::oid[]) OR parent.oid = ANY (table_oids::oid[])
    ), expected AS (
        SELECT r.child_oid AS table_oid, r.foreign_key_name AS constraint_name, 'f'::"char" AS kind,
               NULL::oid AS function_oid
        FROM related AS r WHERE r.type = 'temporal_to_temporal'
        UNION ALL
        SELECT e.table_oid, e.constraint_name, 't'::"char", e.function_oid
        FROM related AS r
        CROSS JOIN LATERAL (VALUES
            (r.child_oid, r.fk_check_constraint, 'sql_saga.regular_fk_check_c()'::regprocedure::oid),
            (r.parent_oid, r.uk_update_trigger, 'sql_saga.uk_update_check_c()'::regprocedure::oid),
            (r.parent_oid, r.uk_delete_trigger, 'sql_saga.uk_delete_check_c()'::regprocedure::oid)
        ) AS e(table_oid, constraint_name, function_oid)
        WHERE r.type = 'regular_to_temporal'
    )
    SELECT pg_catalog.array_agg(DISTINCT c.oid),
           pg_catalog.min(pg_catalog.format('%s constraint %I', e.table_oid::regclass, e.constraint_name))
               FILTER (WHERE c.oid IS NULL OR NOT c.condeferrable
                       OR (e.kind = 't' AND NOT EXISTS (
                           SELECT FROM pg_catalog.pg_trigger AS t
                           WHERE t.tgconstraint = c.oid AND t.tgrelid = e.table_oid
                             AND t.tgname = e.constraint_name AND t.tgfoid = e.function_oid
                             AND t.tgenabled IN ('O', 'A')))
                       OR (e.kind = 'f' AND EXISTS (
                           SELECT FROM pg_catalog.pg_trigger AS t
                           WHERE t.tgconstraint = c.oid AND t.tgenabled NOT IN ('O', 'A'))))
    INTO v_intended, v_invalid
    FROM expected AS e
    LEFT JOIN pg_catalog.pg_constraint AS c
      ON (c.conrelid, c.conname, c.contype) = (e.table_oid, e.constraint_name, e.kind);

    IF v_invalid IS NOT NULL THEN
        RAISE EXCEPTION 'sql_saga: cannot defer %: expected an enabled, deferrable foreign-key check', v_invalid
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Restore the managed constraint or trigger definition before deferring checks.';
    END IF;
    IF v_intended IS NULL THEN
        RETURN;
    END IF;

    -- PostgreSQL addresses constraint names within a schema, not individual OIDs. Never widen the selection.
    SELECT pg_catalog.format('%I.%I', n.nspname, c.conname) AS constraint_name,
           other.conrelid::regclass AS other_table
    INTO v_conflict
    FROM pg_catalog.pg_constraint AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.connamespace
    JOIN pg_catalog.pg_constraint AS other
      ON (other.connamespace, other.conname) = (c.connamespace, c.conname)
     AND other.oid <> ALL (v_intended)
    WHERE c.oid = ANY (v_intended)
    ORDER BY c.oid, other.oid LIMIT 1;
    IF FOUND THEN
        RAISE EXCEPTION 'sql_saga: cannot defer constraint %: that name also selects a constraint on unselected table %',
            v_conflict.constraint_name, v_conflict.other_table
            USING ERRCODE = 'duplicate_object', HINT = 'Rename one of the constraints to make its schema-qualified name unique.';
    END IF;

    SELECT pg_catalog.string_agg(DISTINCT pg_catalog.format('%I.%I', n.nspname, c.conname), ', ')
    INTO v_names
    FROM pg_catalog.pg_constraint AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.connamespace
    WHERE c.oid = ANY (v_intended);
    EXECUTE pg_catalog.format('SET CONSTRAINTS %s DEFERRED', v_names);

    -- SET LOCAL and native constraint modes both follow savepoints and transaction end. This registry is
    -- only an executor checking-moment preference, never an authorization token or a way to skip validation.
    v_registered := COALESCE(NULLIF(pg_catalog.current_setting('sql_saga.temporal_merge.commit_deferred_constraints', true), '')::oid[], '{}'::oid[]);
    SELECT pg_catalog.array_agg(DISTINCT u.constraint_oid ORDER BY u.constraint_oid)
    INTO v_registered
    FROM pg_catalog.unnest(v_registered || v_intended) AS u(constraint_oid);
    PERFORM pg_catalog.set_config('sql_saga.temporal_merge.commit_deferred_constraints', v_registered::text, true);
END;
$defer_temporal_checks$;
GRANT EXECUTE ON PROCEDURE sql_saga.defer_temporal_checks(VARIADIC regclass[]) TO PUBLIC;
COMMENT ON PROCEDURE sql_saga.defer_temporal_checks(VARIADIC regclass[]) IS
'EXPERIMENTAL: defer enabled sql_saga foreign-key checks touching the given tables to transaction end, including across temporal_merge calls. Call inside BEGIN/COMMIT. Native PostgreSQL still validates at COMMIT or explicit SET CONSTRAINTS IMMEDIATE. TRUNCATE protection and synchronized columns remain immediate. No shared-table DDL or elevation. No effect in other sessions. Not part of released 1.4.';
