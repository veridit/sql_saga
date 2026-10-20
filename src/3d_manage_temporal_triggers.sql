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
        CROSS JOIN LATERAL (VALUES (1, fk.uk_update_trigger), (2, fk.uk_delete_trigger)) AS trg(n, trigger_name)
        WHERE pc.oid = ANY (p_table_oids::oid[]) AND trg.trigger_name IS NOT NULL
        UNION ALL
        -- The referencing-side constraint trigger of a regular foreign key, when its table is listed.
        SELECT pc.oid::regclass, fk.fk_check_constraint, fk.foreign_key_name, 3
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
