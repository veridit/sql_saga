CREATE OR REPLACE PROCEDURE sql_saga.manage_temporal_fk_triggers(p_action sql_saga.trigger_action, VARIADIC p_table_oids regclass[])
LANGUAGE plpgsql SECURITY INVOKER AS $procedure$
DECLARE
    v_trigger_rec RECORD;
    v_sql TEXT;
BEGIN
    v_sql := 'ALTER TABLE %s ' || p_action::text || ' TRIGGER %I';

    -- Handle all incoming FK triggers to the specified tables
    FOR v_trigger_rec IN
        SELECT
            (uk.table_schema || '.' || uk.table_name)::regclass AS table_oid,
            fk.uk_update_trigger AS trigger_name
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON fk.unique_key_name = uk.unique_key_name
        WHERE (uk.table_schema || '.' || uk.table_name)::regclass = ANY(p_table_oids) AND fk.uk_update_trigger IS NOT NULL
        UNION ALL
        SELECT
            (uk.table_schema || '.' || uk.table_name)::regclass,
            fk.uk_delete_trigger
        FROM sql_saga.foreign_keys AS fk
        JOIN sql_saga.unique_keys AS uk ON fk.unique_key_name = uk.unique_key_name
        WHERE (uk.table_schema || '.' || uk.table_name)::regclass = ANY(p_table_oids) AND fk.uk_delete_trigger IS NOT NULL
    LOOP
        EXECUTE format(v_sql, v_trigger_rec.table_oid, v_trigger_rec.trigger_name);
    END LOOP;
END;
$procedure$;


CREATE OR REPLACE PROCEDURE sql_saga.disable_temporal_triggers(VARIADIC p_table_oids regclass[])
LANGUAGE plpgsql SECURITY INVOKER AS $procedure$
BEGIN
    CALL sql_saga.manage_temporal_fk_triggers('disable', VARIADIC p_table_oids);
END;
$procedure$;
COMMENT ON PROCEDURE sql_saga.disable_temporal_triggers(VARIADIC regclass[]) IS
'Disables all sql_saga-managed temporal foreign key triggers for the specified tables. This is a targeted alternative to `ALTER TABLE ... DISABLE TRIGGER USER`, intended for complex ETL batches where transient, inconsistent states are expected. It does not affect non-FK triggers, such as those for synchronized columns.';

CREATE OR REPLACE PROCEDURE sql_saga.enable_temporal_triggers(VARIADIC p_table_oids regclass[])
LANGUAGE plpgsql SECURITY INVOKER AS $procedure$
BEGIN
    CALL sql_saga.manage_temporal_fk_triggers('enable', VARIADIC p_table_oids);
END;
$procedure$;
COMMENT ON PROCEDURE sql_saga.enable_temporal_triggers(VARIADIC regclass[]) IS
'Re-enables all sql_saga-managed temporal foreign key triggers for the specified tables. This should be called at the end of an ETL batch transaction to restore integrity checks.';
