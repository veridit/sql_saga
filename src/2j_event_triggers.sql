CREATE EVENT TRIGGER sql_saga_drop_protection ON sql_drop EXECUTE PROCEDURE sql_saga.drop_protection();

CREATE EVENT TRIGGER sql_saga_refuse_managed_relation_identity_change ON ddl_command_start
    WHEN TAG IN ('ALTER TABLE', 'ALTER VIEW', 'ALTER SCHEMA')
    EXECUTE PROCEDURE sql_saga.__internal_refuse_managed_relation_identity_change();

CREATE EVENT TRIGGER sql_saga_rename_following ON ddl_command_end EXECUTE PROCEDURE sql_saga.rename_following();

CREATE EVENT TRIGGER sql_saga_health_checks ON ddl_command_end EXECUTE PROCEDURE sql_saga.health_checks();
