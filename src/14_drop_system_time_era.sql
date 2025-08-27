CREATE FUNCTION sql_saga.drop_system_time_era(table_oid regclass, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT true)
 RETURNS boolean
 LANGUAGE sql
 SECURITY DEFINER
AS
$function$
SELECT sql_saga.drop_era(table_oid, 'system_time', drop_behavior, cleanup);
$function$;
