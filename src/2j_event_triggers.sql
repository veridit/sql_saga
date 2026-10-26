/* Event entry points must be replaced only after every private worker exists: on upgrade the old event
 * triggers remain active while the update runs, so an earlier trampoline would fail with "worker missing". */
CREATE OR REPLACE FUNCTION sql_saga.drop_protection()
RETURNS event_trigger
AS 'sql_saga', 'sql_saga_event_trampoline'
LANGUAGE c VOLATILE SECURITY INVOKER;
GRANT EXECUTE ON FUNCTION sql_saga.drop_protection() TO PUBLIC;

CREATE OR REPLACE FUNCTION sql_saga.rename_following()
RETURNS event_trigger
AS 'sql_saga', 'sql_saga_event_trampoline'
LANGUAGE c VOLATILE SECURITY INVOKER;
GRANT EXECUTE ON FUNCTION sql_saga.rename_following() TO PUBLIC;

CREATE OR REPLACE FUNCTION sql_saga.health_checks()
RETURNS event_trigger
AS 'sql_saga', 'sql_saga_event_trampoline'
LANGUAGE c VOLATILE SECURITY INVOKER;
GRANT EXECUTE ON FUNCTION sql_saga.health_checks() TO PUBLIC;

COMMENT ON FUNCTION sql_saga.drop_protection() IS
'An event trigger function that prevents accidental dropping of sql_saga-managed objects. During the teardown phase of pg_restore --clean (see sql_saga.__internal_is_clean_teardown) it lets the drops through and removes the managed history policies the dump cannot name.';
COMMENT ON FUNCTION sql_saga.rename_following() IS
'An event trigger function that follows object renames and updates sql_saga''s metadata accordingly.';
COMMENT ON FUNCTION sql_saga.health_checks() IS
'An event trigger function that runs after DDL commands to check for inconsistencies in sql_saga''s metadata catalogs.';

CREATE EVENT TRIGGER sql_saga_drop_protection ON sql_drop EXECUTE PROCEDURE sql_saga.drop_protection();

CREATE EVENT TRIGGER sql_saga_refuse_managed_relation_identity_change ON ddl_command_start
    WHEN TAG IN ('ALTER TABLE', 'ALTER VIEW', 'ALTER SCHEMA')
    EXECUTE PROCEDURE sql_saga.__internal_refuse_managed_relation_identity_change();

CREATE EVENT TRIGGER sql_saga_record_alter_function_resolved ON ddl_command_start
    WHEN TAG IN ('ALTER FUNCTION', 'ALTER ROUTINE')
    EXECUTE PROCEDURE sql_saga.__internal_record_alter_function_resolved();

CREATE EVENT TRIGGER sql_saga_rename_following ON ddl_command_end EXECUTE PROCEDURE sql_saga.rename_following();

CREATE EVENT TRIGGER sql_saga_health_checks ON ddl_command_end EXECUTE PROCEDURE sql_saga.health_checks();
