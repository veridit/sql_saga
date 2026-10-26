/* Internal integrity and attribution helpers are implementation details. */
REVOKE ALL ON FUNCTION sql_saga.__internal_system_versioning_query_functions(oid) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_system_versioning_function_is_attributable(oid, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_system_versioning_member_attribution(oid) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_trigger_is_attributable(oid, name, regprocedure, integer, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_system_versioning_view_is_attributable(oid, oid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_unique_key_inventory(name) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_unique_key_attribution(name) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_temporal_foreign_key_is_attributable(name) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_unique_key_object_is_attributable(name, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_display_name(name, name) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.check_integrity() FROM PUBLIC;

/*
 * SECURITY DEFINER workers (1.4, review round 2, R1). Each runs with the extension owner's rights and, without a
 * management function above it, cannot tell who called it, so only sql_saga's own functions and event triggers may.
 * Test 144 lists every SECURITY DEFINER function with its PUBLIC EXECUTE, so a new one is reviewed.
 */
REVOKE ALL ON FUNCTION sql_saga.__internal_add_system_time_era(regclass, name, name, name, name, name, name[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_reconcile_system_versioning_family(regclass, name[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_sync_history_policies(oid, name[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_drop_protection() FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_rename_following() FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_health_checks() FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__internal_ddl_command_affects_managed_object() FROM PUBLIC;

/* The private SECURITY DEFINER workers behind the public management functions (src/3h_api_trampolines.sql). */
REVOKE ALL ON FUNCTION sql_saga.__api_add_current_view(regclass, name, name, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__api_add_era(regclass, name, name, boolean, name, name, name, boolean, boolean, boolean, regtype, name, name[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__api_add_for_portion_of_view(regclass, name, name[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__api_add_foreign_key(regclass, name[], regclass, name[], name, sql_saga.fk_match_types, sql_saga.fk_actions, sql_saga.fk_actions, name, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__api_add_regular_foreign_key(regclass, name[], name, sql_saga.fk_match_types, sql_saga.fk_actions, sql_saga.fk_actions, name, name, text, name, name, boolean) FROM PUBLIC;
REVOKE ALL ON PROCEDURE sql_saga.__api_add_synchronize_temporal_columns_trigger(table_oid regclass, era_name name) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__api_add_system_versioning(regclass, name, name, name, name, name, name, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__api_add_temporal_foreign_key(regclass, name[], name, name, sql_saga.fk_match_types, sql_saga.fk_actions, sql_saga.fk_actions, name, name, name, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__api_add_unique_key(regclass, name[], name, sql_saga.unique_key_type, boolean, name, name, name, text, name[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__api_drop_current_view(regclass, name, sql_saga.drop_behavior) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__api_drop_era(regclass, name, sql_saga.drop_behavior, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__api_drop_for_portion_of_view(regclass, name, sql_saga.drop_behavior, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__api_drop_foreign_key(regclass, name[], name, sql_saga.drop_behavior, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__api_drop_foreign_key_by_name(regclass, name, boolean) FROM PUBLIC;
REVOKE ALL ON PROCEDURE sql_saga.__api_drop_synchronize_temporal_columns_trigger(table_oid regclass, era_name name) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__api_drop_system_time_era(regclass, sql_saga.drop_behavior, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__api_drop_system_versioning(regclass, sql_saga.drop_behavior, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__api_drop_unique_key(regclass, name[], name, sql_saga.drop_behavior, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__api_drop_unique_key_by_name(regclass, name, sql_saga.drop_behavior, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__api_rebuild_system_versioning_views(regclass, name[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__api_set_era_ephemeral_columns(regclass, name, name[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__api_set_system_time_era_excluded_columns(regclass, name[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__api_sync_all_history_policies() FROM PUBLIC;
REVOKE ALL ON FUNCTION sql_saga.__api_sync_history_policies(regclass) FROM PUBLIC;
