/*
 * Hardened databases revoke EXECUTE from PUBLIC by default (ALTER DEFAULT PRIVILEGES ... REVOKE
 * EXECUTE ON FUNCTIONS FROM PUBLIC). The FOR PORTION OF and current views are open to PUBLIC and
 * call these routines as the caller, whose rights on the base table decide what happens; so the
 * routines themselves must stay executable by PUBLIC (tmp/design_final.md section 5).
 */
GRANT EXECUTE ON FUNCTION sql_saga.for_portion_of_trigger() TO PUBLIC;
GRANT EXECUTE ON FUNCTION sql_saga.current_view_trigger() TO PUBLIC;
DO $grant_temporal_merge$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT p.oid::regprocedure AS routine, p.prokind
        FROM pg_catalog.pg_proc AS p
        WHERE p.pronamespace = 'sql_saga'::regnamespace
          AND p.proname IN ('temporal_merge', 'temporal_merge_plan', 'temporal_merge_plan_native',
                            'temporal_merge_execute', 'temporal_merge_drop_temp_tables',
                            'temporal_merge_executor_introspect')
    LOOP
        EXECUTE format('GRANT EXECUTE ON %s %s TO PUBLIC',
            CASE WHEN r.prokind = 'p' THEN 'PROCEDURE' ELSE 'FUNCTION' END, r.routine);
    END LOOP;
END;
$grant_temporal_merge$;
