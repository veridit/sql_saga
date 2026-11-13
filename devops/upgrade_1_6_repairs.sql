/*
 * sql_saga 1.6 update: repairs after the 1.6 definitions are loaded (devops/generate_update_scripts_1_6.py).
 * The generator emits the final integrity gate right after this.
 *
 * Every existing for_portion_of view gets its statement-level apply trigger, exactly as a fresh 1.6
 * add_for_portion_of_view creates it: AFTER UPDATE FOR EACH STATEMENT, named after the view's row trigger with
 * the suffix _apply. The view, its OID, grants, comments and other triggers are unchanged. Like the TRUNCATE
 * triggers of the 1.4 update, the trigger belongs to its view, not to the extension. A view that already has a
 * trigger of that name refuses the update, naming it.
 */
DO $create_for_portion_of_apply_triggers$
DECLARE
    v record;
    n integer := 0;
BEGIN
    FOR v IN
        SELECT uv.view_schema, uv.view_name, uv.trigger_name,
               pg_catalog.to_regclass(pg_catalog.format('%I.%I', uv.view_schema, uv.view_name)) AS view_oid
        FROM sql_saga.updatable_view AS uv
        WHERE uv.view_type = 'for_portion_of'
        ORDER BY uv.view_schema COLLATE "C", uv.view_name COLLATE "C"
    LOOP
        CONTINUE WHEN v.view_oid IS NULL; /* check_integrity reports a missing view */
        IF EXISTS (SELECT FROM pg_catalog.pg_trigger AS t
                   WHERE t.tgrelid = v.view_oid AND t.tgname = v.trigger_name || '_apply') THEN
            RAISE EXCEPTION 'sql_saga: view %.% already has a trigger named %, which sql_saga 1.6 needs for its apply trigger',
                pg_catalog.quote_ident(v.view_schema), pg_catalog.quote_ident(v.view_name),
                pg_catalog.quote_ident(v.trigger_name || '_apply')
                USING ERRCODE = 'duplicate_object',
                      HINT = 'Before updating, rename or drop that trigger, then retry ALTER EXTENSION sql_saga UPDATE.';
        END IF;
        EXECUTE pg_catalog.format('CREATE TRIGGER %I AFTER UPDATE ON %s FOR EACH STATEMENT EXECUTE PROCEDURE sql_saga.for_portion_of_apply_trigger()',
            v.trigger_name || '_apply', v.view_oid::pg_catalog.regclass);
        n := n + 1;
    END LOOP;
    IF n > 0 THEN
        RAISE NOTICE 'sql_saga: Created the apply trigger of % for_portion_of views', n;
    END IF;
END;
$create_for_portion_of_apply_triggers$;
