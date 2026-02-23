CREATE FUNCTION sql_saga.drop_system_time_era(table_oid regclass, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT true)
 RETURNS boolean
 LANGUAGE sql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, public
AS
$function$
SELECT sql_saga.drop_era(table_oid, 'system_time', drop_behavior, cleanup);
$function$;

COMMENT ON FUNCTION sql_saga.drop_system_time_era(regclass, sql_saga.drop_behavior, boolean) IS
'Drops the internal system time era metadata. This is a lower-level function typically called by `drop_system_versioning`.';
