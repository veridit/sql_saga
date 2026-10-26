CREATE FUNCTION sql_saga.truncate_system_versioning()
 RETURNS trigger
 LANGUAGE plpgsql
 STRICT
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, pg_temp
AS
$function$
#variable_conflict use_variable
DECLARE
    history_schema name;
    history_table name;
    table_schema name;
    table_name name;
BEGIN
    SELECT n.nspname, c.relname
    INTO table_schema, table_name
    FROM pg_catalog.pg_class AS c
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.oid = TG_RELID;

    SELECT sv.history_schema_name, sv.history_table_name
    INTO history_schema, history_table
    FROM sql_saga.system_versioning AS sv
    WHERE (sv.table_schema, sv.table_name) = (table_schema, table_name);

    /*
     * Only as the trigger sql_saga created for the table's system time era (1.4, review round 3, Q2): a role with
     * TRIGGER on the table may create triggers calling this SECURITY DEFINER function.
     */
    IF NOT EXISTS (
        SELECT FROM sql_saga.system_time_era AS ste
        JOIN pg_catalog.pg_trigger AS t ON t.tgrelid OPERATOR(pg_catalog.=) TG_RELID AND t.tgname OPERATOR(pg_catalog.=) ste.truncate_trigger
        WHERE (ste.table_schema, ste.table_name) = (table_schema, table_name)
          AND ste.truncate_trigger OPERATOR(pg_catalog.=) TG_NAME
          AND NOT t.tgisinternal AND t.tgnargs OPERATOR(pg_catalog.=) 0
          AND t.tgfoid OPERATOR(pg_catalog.=) 'sql_saga.truncate_system_versioning()'::pg_catalog.regprocedure)
    THEN
        RAISE EXCEPTION 'trigger "%" on table "%" is not a sql_saga system versioning trigger', TG_NAME, TG_TABLE_NAME
            USING ERRCODE = 'insufficient_privilege',
                  DETAIL = 'truncate_system_versioning may only fire as the truncate_trigger that sql_saga created for the table''s system time era.';
    END IF;

    IF history_table IS NOT NULL THEN
        /*
         * As the history table's owner (1.4, review round 2), like the history INSERT in write_history: the base
         * table's writer needs no rights on the history, and the owner's code on the history (its TRUNCATE
         * triggers) never runs with the extension owner's rights.
         */
        PERFORM sql_saga.__internal_truncate_as_owner(pg_catalog.format('%I.%I', history_schema, history_table)::regclass);
    END IF;

    RETURN NULL;
END;
$function$;
