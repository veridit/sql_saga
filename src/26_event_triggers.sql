CREATE EVENT TRIGGER sql_saga_drop_protection ON sql_drop EXECUTE PROCEDURE sql_saga.drop_protection();

CREATE EVENT TRIGGER sql_saga_rename_following ON ddl_command_end EXECUTE PROCEDURE sql_saga.rename_following();

CREATE EVENT TRIGGER sql_saga_health_checks ON ddl_command_end EXECUTE PROCEDURE sql_saga.health_checks();

CREATE EVENT TRIGGER sql_saga_cache_invalidation ON ddl_command_end 
    WHEN TAG IN ('ALTER TABLE', 'DROP TABLE')
    EXECUTE PROCEDURE sql_saga.temporal_merge_cache_invalidation();
